# ======================================
# app.py - v4.5 (Admin 篩選 - 中文日誌)
# ======================================
from flask import Flask, request, jsonify, send_file, render_template
from flask_cors import CORS
from s3_store import S3Store
from pathlib import Path
import os, io, csv, uuid, jwt
from datetime import datetime, timezone, timedelta
from functools import wraps
import json
import logging 
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

APP_VERSION = "4.5-admin-filter" # <-- (新) 版本號

# ---------- 台灣時區設定 ----------
TAIWAN_TZ = timezone(timedelta(hours=8))

# ---------- Admin 設定 ----------
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "888888"
JWT_SECRET = os.environ.get('JWT_SECRET', 'your-secret-key-change-this-in-production')
ENABLE_ADMIN_AUTH = os.environ.get('ENABLE_ADMIN_AUTH', 'false').lower() == 'true'

# ---------- 基本路徑 ----------
PROJECT_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PROJECT_DIR / "templates"

app = Flask(__name__, template_folder=str(TEMPLATES_DIR), static_folder="static")
CORS(app)

# ---------- S3 儲存初始化 ----------
s3_store = S3Store()

MAX_PER_BOOKING = 3

# ---------- (新) 速率限制設定 ----------
def get_ip_address():
    if 'X-Forwarded-For' in request.headers:
        return request.headers['X-Forwarded-For'].split(',')[0].strip()
    else:
        return request.remote_addr

limiter = Limiter(
    app=app,
    key_func=get_ip_address,
    default_limits=[],
    storage_uri="memory://" 
)

# ---------- Token 驗證裝飾器 ----------
def require_admin_token(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not ENABLE_ADMIN_AUTH:
            return f(*args, **kwargs)
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'success': False, 'message': 'No token provided'}), 401
        try:
            token = token.replace('Bearer ', '')
            payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
            if payload.get('username') != ADMIN_USERNAME:
                return jsonify({'success': False, 'message': 'Invalid token'}), 401
        except jwt.ExpiredSignatureError:
            return jsonify({'success': False, 'message': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'success': False, 'message': 'Invalid token'}), 401
        return f(*args, **kwargs)
    return decorated

# ---------- 輔助函式 (保持不變) ----------
def _find_reservation_and_date(reservation_id):
    if not reservation_id:
        return None, None
    all_reservations = s3_store.get_all_reservations()
    reservation = next((r for r in all_reservations if r.get('id') == reservation_id), None)
    if not reservation:
        return None, None
    try:
        created_at_iso = reservation.get("created_at")
        created_dt = datetime.fromisoformat(created_at_iso)
        date_str = created_dt.strftime('%Y-%m-%d')
        return reservation, date_str
    except Exception as e:
        logger.error(f"無法從 {reservation_id} 解析日期: {e}")
        date_str = datetime.now(TAIWAN_TZ).strftime('%Y-%m-%d')
        return reservation, date_str

# ---------- 初始化桌位資料 (保持不變) ----------
def init_tables():
    try:
        existing_tables = s3_store.get_tables_data()
        if existing_tables:
            print("[INFO] S3 中已存在桌位資料")
            return
        tables_data = {}
        for i in range(1, 109):
            tables_data[str(i)] = {
                "id": i,
                "name": f"Table {i}",
                "total": 10,
                "seats_left": 10
            }
        if s3_store.save_tables_data(tables_data):
            print("[INFO] 已在 S3 中初始化桌位 (1..108)")
        else:
            print("[WARN] 在 S3 中初始化桌位失敗")
    except Exception as e:
        print(f"[WARN] init_tables 失敗: {e}")

try:
    init_tables()
except Exception as e:
    print(f"[WARN] init_tables 已跳過: {e}")

# ---------- (NEW) 版本檢查 API ----------
@app.route("/api/version")
def get_version():
    return jsonify(
        app_version=APP_VERSION, 
        s3_store_version=getattr(s3_store, 'VERSION', 'unknown')
    )

# ---------- 頁面路由 (保持不變) ----------
@app.route("/")
@app.route("/index.html")
def index_page():
    return render_template("index.html")

@app.route("/admin")
@app.route("/admin.html")
def admin_page():
    return render_template("admin.html")

@app.route("/login")
@app.route("/login.html")
def login_page():
    return render_template("login.html")

@app.route("/reports")
@app.route("/reports.html")
def reports_page():
    return render_template("reports.html")

# ---------- 健康檢查 ----------
@app.get("/health")
def health():
    return jsonify(ok=True)

# ---------- S3 連線測試 ----------
@app.route('/test-s3')
def test_s3():
    if s3_store.test_connection():
        return jsonify({ 'status': 'success', 'message': 'S3 Connection Successful!', 'bucket': s3_store.bucket_name })
    else:
        return jsonify({ 'status': 'error', 'message': 'S3 Connection Failed' }), 500

# ---------- Admin 登入 API (UI 英文) ----------
@app.post("/api/admin/login")
def admin_login():
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'message': 'No data provided'}), 400
    username = data.get('username')
    password = data.get('password')
    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        payload = { 'username': username, 'exp': datetime.now(TAIWAN_TZ) + timedelta(hours=24) }
        token = jwt.encode(payload, JWT_SECRET, algorithm='HS256')
        return jsonify({ 'success': True, 'token': token, 'message': 'Login successful' })
    else:
        return jsonify({ 'success': False, 'message': 'Invalid credentials' }), 401

# ---------- Token 驗證 API (UI 英文) ----------
@app.get("/api/admin/verify")
@require_admin_token
def admin_verify():
    return jsonify({'success': True, 'message': 'Token valid'})

# ---------- API：座位狀態 (!!! 已優化 !!!) ----------
@app.get("/api/status")
def api_status():
    tables_data = s3_store.get_tables_data()
    if not tables_data:
        return jsonify({"tables": []})
    all_reservations = s3_store.get_all_reservations()
    reservations_map = {}
    for r in all_reservations:
        table_id_str = str(r.get("table_id"))
        name = r.get("employee_name", "Unknown")
        seats = r.get("seats_taken", 1) 
        if table_id_str not in reservations_map:
            reservations_map[table_id_str] = []
        reservations_map[table_id_str].append({ "name": name, "seats": seats })
    tables_list = []
    for table_id_str in sorted(tables_data.keys(), key=int):
        table = tables_data[table_id_str]
        reservation_list = reservations_map.get(table_id_str, []) 
        tables_list.append({
            "table_id": table["id"],
            "seats_left": table["seats_left"],
            "reservations": reservation_list
        })
    return jsonify({"tables": tables_list})

# ---------- API：桌位清單 (保持不變) ----------
@app.get("/api/tables")
def api_tables():
    tables_data = s3_store.get_tables_data()
    if not tables_data:
        return jsonify([])
    tables_list = []
    for table_id in sorted(tables_data.keys(), key=int):
        table = tables_data[table_id]
        tables_list.append({
            "id": table["id"], "name": table["name"],
            "total": table["total"], "seats_left": table["seats_left"]
        })
    return jsonify(tables_list)

# ---------- API：可用性 (保持不變) ----------
@app.get("/api/reservations/availability")
def api_availability():
    tables_data = s3_store.get_tables_data()
    if not tables_data:
        return jsonify({"holds": [], "confirmed": []})
    confirmed = []
    for table_id, table in tables_data.items():
        if table["seats_left"] <= 0:
            confirmed.append({"table_id": table["id"]})
    return jsonify({"holds": [], "confirmed": confirmed})

# ---------- API：查詢預約 (!!! 已優化 & 公開 !!!) ----------
@app.get("/api/reservations")
# (!!!) 保持公開，
# 這樣 reports.html (公開) 和 admin.html (登入) 才能共用
def list_reservations():
    table_id = request.args.get("table_id", type=int)
    page = max(1, request.args.get("page", default=1, type=int))
    size = min(100, max(1, request.args.get("page_size", default=50, type=int)))
    try:
        all_reservations = s3_store.get_all_reservations()
        logger.debug(f"[DEBUG] 從快取/S3 找到 {len(all_reservations)} 筆預訂")
        if table_id:
            all_reservations = [r for r in all_reservations if r.get("table_id") == table_id]
        all_reservations.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        total = len(all_reservations)
        start = (page - 1) * size
        end = start + size
        data = all_reservations[start:end]
        return jsonify({
            "data": data, "total": total,
            "page": page, "page_size": size
        })
    except Exception as e:
        logger.error(f"[ERROR] list_reservations 失敗: {e}")
        return jsonify({ "data": [], "total": 0, "page": page, "page_size": size, "error": str(e) })

# ---------- API：建立預約 (!!! 已優化 - CAS + 速率限制 !!!) ----------
@app.post("/api/reserve")
@limiter.limit("1 per 2 seconds") # (優化) 每 IP 2 秒只能請求一次
def reserve():
    payload = request.get_json(force=True)
    try:
        table_id = int(payload["table_id"])
        table_id_str = str(table_id) 
    except Exception:
        return jsonify(success=False, message="table_id is required"), 400

    seats = payload.get("seats_to_take", 1)
    try: seats = int(seats)
    except Exception: seats = 1

    employee_name = str(payload.get("employee_name", "")).strip() or "Guest"
    login_id = (str(payload.get("login_id", "")).strip() or "guest").lower()

    if seats < 1 or seats > MAX_PER_BOOKING:
        return jsonify(success=False, message=f"Invalid seat count (1-{MAX_PER_BOOKING})"), 400

    idem_key = request.headers.get("Idempotency-Key") or str(uuid.uuid4())

    try:
        # 1. 檢查防重複提交
        existing_idem = s3_store.get_idempotency_key(idem_key)
        if existing_idem:
            return jsonify(success=True, message="Already processed",
                           reservation_id=existing_idem.get("reservation_id")), 200

        # 2. (優化) 檢查登入 ID
        if s3_store.check_login_id_exists(login_id):
            return jsonify(success=False, message="This login_id already has a reservation."), 409

        # 3. 取得 ETag
        tables_data, etag = s3_store.get_tables_data_with_etag()
        if not tables_data:
            return jsonify(success=False, message="Server error: Cannot read tables data"), 500
            
        # 4. 檢查座位
        if table_id_str not in tables_data:
             return jsonify(success=False, message="Table not found"), 404
        if tables_data[table_id_str]["seats_left"] < seats:
            return jsonify(success=False, message="This table is full or seats are not enough now."), 409

        # 5. 在記憶體中修改
        tables_data[table_id_str]["seats_left"] -= seats

        # 6. 嘗試原子性寫回 (CAS)
        try:
            s3_store.save_tables_data_cas(tables_data, etag)
        except ClientError as e:
            if e.response['Error']['Code'] == 'PreconditionFailed':
                logger.warning(f"CAS 409: S3 PreconditionFailed (幾乎同時提交) for Table {table_id_str}")
                return jsonify(success=False, message="You were too slow! That seat was just taken. Please select another."), 409 
            else:
                raise 
        
        # 7. 建立預約
        reservation_id = str(uuid.uuid4())
        reservation_data = {
            "id": reservation_id, "table_id": table_id, "seats_taken": seats,
            "employee_name": employee_name, "login_id": login_id,
            "created_at": datetime.now(TAIWAN_TZ).isoformat()
        }
        logger.debug(f"[DEBUG] 正在建立預訂: {reservation_data}")

        # 8. 儲存
        if s3_store.save_reservation(reservation_id, reservation_data, date_str=None):
            s3_store.save_idempotency_key(idem_key, {"reservation_id": reservation_id})
            logger.info(f"預訂成功建立: {reservation_id}")
            return jsonify(success=True, message="Reservation confirmed!",
                           reservation_id=reservation_id, table_id=table_id), 201
        else:
            # 復原 (Rollback)
            logger.warning(f"儲存預訂 {reservation_id} 失敗, 正在回復座位...")
            s3_store.release_seats_cas(table_id, seats) 
            return jsonify(success=False, message="Failed to save reservation"), 500

    except Exception as e:
        logger.error(f"[ERROR] 預訂失敗: {e}")
        try:
            if 'table_id' in locals():
                s3_store.release_seats_cas(table_id, seats)
                logger.info(f"因錯誤 {e} 回復桌位 {table_id}")
            else:
                logger.error(f"因錯誤 {e} 預訂失敗，但 table_id 未定義，無法回復座位")
        except Exception as rollback_e:
            logger.error(f"[CRITICAL] 回復失敗! {rollback_e}")
        return jsonify(success=False, message=f"Reservation failed: {str(e)}"), 500

# ---------- API：取消預約 (!!! 已優化 - CAS !!!) ----------
@app.post("/api/cancel")
@require_admin_token # Admin 才能取消
def cancel():
    payload = request.get_json(force=True)
    reservation_id = payload.get("reservation_id")
    if not reservation_id:
        return jsonify(success=False, message="reservation_id required"), 400
    reservation, date_str = _find_reservation_and_date(reservation_id)
    if not reservation:
        return jsonify(success=False, message="Reservation not found"), 404
    if s3_store.delete_reservation(reservation_id, date_str):
        if s3_store.release_seats_cas(reservation["table_id"], reservation["seats_taken"]):
            return jsonify(success=True)
        else:
             logger.warning(f"預訂 {reservation_id} 已刪除, 但 CAS 釋放座位失敗")
             return jsonify(success=True, message="Reservation deleted, but seat release failed. Please resync.")
    else:
        return jsonify(success=False, message="Failed to cancel reservation"), 500


# ---------- (!!! 已優化 - CAS !!!) API：更新訂位資訊 ----------
@app.post("/api/admin/update_reservation")
@require_admin_token # Admin 才能更新
def update_reservation_details():
    payload = request.get_json(force=True)
    reservation_id = payload.get("reservation_id")
    new_login_id = payload.get("login_id")
    new_name = payload.get("employee_name")
    try:
        new_seats = int(payload.get("seats_taken"))
    except (ValueError, TypeError):
        return jsonify(success=False, message="Invalid seats_taken value"), 400

    if not reservation_id or not new_login_id or not new_name:
        return jsonify(success=False, message="Missing required fields"), 400
    if new_seats < 1 or new_seats > MAX_PER_BOOKING:
        return jsonify(success=False, message=f"Seats must be between 1 and {MAX_PER_BOOKING}"), 400

    new_login_id = new_login_id.strip().lower()
    new_name = new_name.strip()
    if not new_login_id or not new_name:
        return jsonify(success=False, message="Fields cannot be empty"), 400

    seat_diff = 0 
    table_id = None
    try:
        reservation, date_str = _find_reservation_and_date(reservation_id)
        if not reservation:
            return jsonify(success=False, message="Reservation not found"), 404

        current_login_id = reservation.get("login_id")
        current_seats = reservation.get("seats_taken")
        table_id = reservation.get("table_id") 

        seat_diff = new_seats - current_seats
        if seat_diff > 0:
            if not s3_store.reserve_seats_cas(table_id, seat_diff):
                return jsonify(success=False, message=f"Not enough seats on Table {table_id} (or CAS conflict)"), 409
        elif seat_diff < 0:
            if not s3_store.release_seats_cas(table_id, abs(seat_diff)):
                 logger.warning(f"更新 {reservation_id} 時釋放座位失敗")

        if current_login_id != new_login_id:
            if s3_store.check_login_id_exists(new_login_id):
                if seat_diff > 0:
                    s3_store.release_seats_cas(table_id, seat_diff) 
                elif seat_diff < 0:
                    s3_store.reserve_seats_cas(table_id, abs(seat_diff)) 
                return jsonify(success=False, message=f"The new login_id '{new_login_id}' is already taken."), 409
        
        reservation["login_id"] = new_login_id
        reservation["employee_name"] = new_name
        reservation["seats_taken"] = new_seats
        reservation["updated_at"] = datetime.now(TAIWAN_TZ).isoformat()

        if s3_store.update_reservation(reservation_id, reservation, date_str):
            return jsonify(success=True, message="Reservation updated.")
        else:
            if seat_diff > 0:
                s3_store.release_seats_cas(table_id, seat_diff)
            elif seat_diff < 0:
                s3_store.reserve_seats_cas(table_id, abs(seat_diff))
            return jsonify(success=False, message="Failed to save update to S3."), 500

    except Exception as e:
        logger.error(f"[ERROR] 更新預訂失敗: {e}")
        if seat_diff > 0 and table_id:
            s3_store.release_seats_cas(table_id, seat_diff)
        elif seat_diff < 0 and table_id:
             s3_store.reserve_seats_cas(table_id, abs(seat_diff))
        return jsonify(success=False, message=str(e)), 500

# ---------- API：資料重新同步 (!!! 已優化 - CAS !!!) ----------
@app.post("/api/admin/resync")
@require_admin_token # Admin 才能同步
def admin_resync():
    try:
        logger.info("[INFO] 開始重新同步桌位資料...")
        all_reservations = s3_store.get_all_reservations()
        actual_seats_taken = {}
        for r in all_reservations:
            table_id_str = str(r.get("table_id"))
            seats = r.get("seats_taken", 1) 
            if table_id_str not in actual_seats_taken:
                actual_seats_taken[table_id_str] = 0
            actual_seats_taken[table_id_str] += seats

        tables_data, etag = s3_store.get_tables_data_with_etag()
        if not tables_data:
            return jsonify(success=False, message="No tables data found to resync."), 500

        updated_count = 0
        for table_id_str, table in tables_data.items():
            total_seats = table.get("total", 10) 
            taken_seats = actual_seats_taken.get(table_id_str, 0) 
            new_seats_left = total_seats - taken_seats
            if table["seats_left"] != new_seats_left:
                logger.info(f"[RESYNC] 桌號 {table_id_str}: 剩餘座位 {table['seats_left']}, 修正為 {new_seats_left}")
                table["seats_left"] = new_seats_left
                updated_count += 1
            
        try:
            s3_store.save_tables_data_cas(tables_data, etag)
            logger.info(f"[INFO] 重新同步完成。 {updated_count} 張桌子已修正。")
            return jsonify(success=True, message=f"Resync complete. {updated_count} table(s) corrected.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'PreconditionFailed':
                logger.warning("[RESYNC] 重新同步時發生 CAS 衝突。請重試。")
                return jsonify(success=False, message="Conflict during resync (someone was reserving). Please try again."), 409
            else:
                raise 

    except Exception as e:
        logger.error(f"[ERROR] 重新同步失敗: {e}")
        return jsonify(success=False, message=str(e)), 500


# ---------- 匯出 CSV (!!! 已優化 & 公開 !!!) ----------
@app.get("/api/reservations.csv")
# (!!!) 保持公開
def export_csv():
    # (新) 允許透過 query 篩選 table_id
    table_id = request.args.get("table_id", type=int)

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["id", "table_id", "seats_taken", "employee_name", "login_id", "created_at"])
    
    reservations = s3_store.get_all_reservations()

    # (新) 如果有 table_id，則篩選
    if table_id:
        reservations = [r for r in reservations if r.get("table_id") == table_id]
        
    reservations.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    
    for r in reservations:
        writer.writerow([
            r.get("id", ""), r.get("table_id", ""), r.get("seats_taken", ""),
            r.get("employee_name", ""), r.get("login_id", ""), r.get("created_at", "")
        ])
    mem = io.BytesIO(out.getvalue().encode("utf-8"))
    mem.seek(0)
    
    # (新) 檔名
    filename = f"reservations_table_{table_id}.csv" if table_id else "reservations_all.csv"
    
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name=filename)

# ---------- (新) 速率限制的錯誤處理 ----------
@app.errorhandler(429)
def ratelimit_handler(e):
    logger.warning(f"IP {get_ip_address()} 觸發速率限制: {e.description}")
    return jsonify(
        success=False,
        message=f"Too many requests. Please wait {e.description.split(' ')[-2]} seconds."
    ), 429

# ---------- 啟動 ----------
if __name__ == "__main__":
    try:
        init_tables()
    except Exception as e:
        print(f"[WARN] init_tables 已跳過: {e}")
    print(f"[INFO] Admin 驗證: {'啟用' if ENABLE_ADMIN_AUTH else '停用'}")
    port = int(os.getenv("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
