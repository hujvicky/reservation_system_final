# ======================================
# app.py - v4.3 (強制重部署 - 中文日誌)
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
# (新) 匯入速率限制
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
# (新) 處理 S3 客戶端錯誤 (用於 CAS)
from botocore.exceptions import ClientError

# (新) 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

APP_VERSION = "4.3-force-redeploy" # <-- (新) 版本號

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
    """ 考慮 App Runner 的 'X-Forwarded-For' 標頭 """
    if 'X-Forwarded-For' in request.headers:
        # X-Forwarded-For 可能包含多個 IP (client, proxy1, proxy2)，取第一個
        return request.headers['X-Forwarded-For'].split(',')[0].strip()
    else:
        return request.remote_addr

limiter = Limiter(
    app=app,
    key_func=get_ip_address,
    default_limits=[],
    storage_uri="memory://" # 每個 App Runner 執行個體各自的記憶體
)

# ---------- Token 驗證裝飾器 ----------
def require_admin_token(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # 如果驗證被停用，直接執行函數
        if not ENABLE_ADMIN_AUTH:
            return f(*args, **kwargs)

        token = request.headers.get('Authorization')
        if not token:
            # (UI 英文)
            return jsonify({'success': False, 'message': 'No token provided'}), 401

        try:
            token = token.replace('Bearer ', '')
            payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
            if payload.get('username') != ADMIN_USERNAME:
                # (UI 英文)
                return jsonify({'success': False, 'message': 'Invalid token'}), 401
        except jwt.ExpiredSignatureError:
            # (UI 英文)
            return jsonify({'success': False, 'message': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            # (UI 英文)
            return jsonify({'success': False, 'message': 'Invalid token'}), 401

        return f(*args, **kwargs)
    return decorated

# ---------- (!!! 修正 1 !!!) 輔助函式 (保持不變) ----------
def _find_reservation_and_date(reservation_id):
    """ (修復 Bug 用) 尋找訂單和它的日期 """
    if not reservation_id:
        return None, None
        
    # (優化) 這裡會自動使用 s3_store 中的快取，所以速度很快
    all_reservations = s3_store.get_all_reservations()
    reservation = next((r for r in all_reservations if r.get('id') == reservation_id), None)
    
    if not reservation:
        return None, None

    try:
        created_at_iso = reservation.get("created_at")
        # 從 ISO 格式字串解析出 datetime 物件
        created_dt = datetime.fromisoformat(created_at_iso)
        # 格式化成 s3_store.py 需要的 'YYYY-MM-DD'
        date_str = created_dt.strftime('%Y-%m-%d')
        return reservation, date_str
    except Exception as e:
        # (日誌 中文)
        logger.error(f"無法從 {reservation_id} 解析日期: {e}")
        # 降級：嘗試用今天的日期 (可能會失敗)
        date_str = datetime.now(TAIWAN_TZ).strftime('%Y-%m-%d')
        return reservation, date_str

# ---------- 初始化桌位資料 (保持不變) ----------
def init_tables():
    """初始化桌位資料到 S3"""
    try:
        # 檢查是否已有桌位資料
        existing_tables = s3_store.get_tables_data()
        if existing_tables:
            # (日誌 中文)
            print("[INFO] S3 中已存在桌位資料")
            return

        # 建立預設桌位資料
        tables_data = {}
        for i in range(1, 109):
            tables_data[str(i)] = {
                "id": i,
                "name": f"Table {i}",
                "total": 10,
                "seats_left": 10
            }

        if s3_store.save_tables_data(tables_data):
            # (日誌 中文)
            print("[INFO] 已在 S3 中初始化桌位 (1..108)")
        else:
            # (日誌 中文)
            print("[WARN] 在 S3 中初始化桌位失敗")

    except Exception as e:
        # (日誌 中文)
        print(f"[WARN] init_tables 失敗: {e}")

# 啟動時初始化
try:
    init_tables()
except Exception as e:
    # (日誌 中文)
    print(f"[WARN] init_tables 已跳過: {e}")

# ---------- (NEW) 版本檢查 API ----------
@app.route("/api/version")
def get_version():
    """檢查後端版本，用於除錯"""
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
        return jsonify({
            'status': 'success',
            'message': 'S3 Connection Successful!', # (UI 英文)
            'bucket': s3_store.bucket_name
        })
    else:
        return jsonify({
            'status': 'error',
            'message': 'S3 Connection Failed' # (UI 英文)
        }), 500

# ---------- Admin 登入 API (UI 英文) ----------
@app.post("/api/admin/login")
def admin_login():
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'message': 'No data provided'}), 400

    username = data.get('username')
    password = data.get('password')

    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        # 生成 JWT token (24小時有效)
        payload = {
            'username': username,
            'exp': datetime.now(TAIWAN_TZ) + timedelta(hours=24)
        }
        token = jwt.encode(payload, JWT_SECRET, algorithm='HS256')

        return jsonify({
            'success': True,
            'token': token,
            'message': 'Login successful' # (UI 英文)
        })
    else:
        return jsonify({
            'success': False,
            'message': 'Invalid credentials' # (UI 英文)
        }), 401

# ---------- Token 驗證 API (UI 英文) ----------
@app.get("/api/admin/verify")
@require_admin_token
def admin_verify():
    return jsonify({'success': True, 'message': 'Token valid'}) # (UI 英文)

# ---------- API：座位狀態 (!!! 已優化 !!!) ----------
@app.get("/api/status")
def api_status():
    """
    (優化) 此 API 現在非常快。
    get_tables_data() 是一個 S3 GET。
    get_all_reservations() 現在會從 5 秒記憶體快取中讀取。
    """
    tables_data = s3_store.get_tables_data()
    if not tables_data:
        return jsonify({"tables": []})

    # (優化) 這裡會從快取讀取，速度極快
    all_reservations = s3_store.get_all_reservations()
    
    # === (MODIFIED) START: 查詢所有訂位並建立查詢表 (包含座位數) ===
    # 建立一個 map (table_id_str -> [ {name:..., seats:...}, ... ])
    reservations_map = {}
    for r in all_reservations:
        table_id_str = str(r.get("table_id"))
        name = r.get("employee_name", "Unknown")
        # (NEW) 取得 'seats_taken'，如果沒有則預設為 1
        seats = r.get("seats_taken", 1) 
        
        if table_id_str not in reservations_map:
            reservations_map[table_id_str] = []
            
        # (MODIFIED) 加入一個物件，包含姓名和座位數
        reservations_map[table_id_str].append({
            "name": name,
            "seats": seats
        })
    # === (MODIFIED) END ===

    tables_list = []
    for table_id_str in sorted(tables_data.keys(), key=int):
        table = tables_data[table_id_str]
        
        # 從 map 中取得這張桌子的訂位清單 (物件陣列)
        reservation_list = reservations_map.get(table_id_str, []) 
        
        # (MODIFIED) 將 'reservations' 欄位加入回傳
        tables_list.append({
            "table_id": table["id"],
            "seats_left": table["seats_left"],
            "reservations": reservation_list  # <--- 這就是前端需要的新欄位
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
            "id": table["id"],
            "name": table["name"],
            "total": table["total"],
            "seats_left": table["seats_left"]
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

# ---------- API：查詢預約 (!!! 已優化 !!!) ----------
@app.get("/api/reservations")
@require_admin_token
def list_reservations():
    table_id = request.args.get("table_id", type=int)
    page = max(1, request.args.get("page", default=1, type=int))
    size = min(100, max(1, request.args.get("page_size", default=50, type=int)))

    try:
        # (優化) 獲取所有預約 (受益於快取)
        all_reservations = s3_store.get_all_reservations()
        # (日誌 中文)
        logger.debug(f"[DEBUG] 從快取/S3 找到 {len(all_reservations)} 筆預訂")

        # 過濾條件
        if table_id:
            all_reservations = [r for r in all_reservations if r.get("table_id") == table_id]

        # 排序（最新的在前）
        all_reservations.sort(key=lambda x: x.get("created_at", ""), reverse=True)

        # 分頁
        total = len(all_reservations)
        start = (page - 1) * size
        end = start + size
        data = all_reservations[start:end]

        return jsonify({
            "data": data,
            "total": total,
            "page": page,
            "page_size": size
        })
    except Exception as e:
        # (日誌 中文)
        logger.error(f"[ERROR] list_reservations 失敗: {e}")
        return jsonify({
            "data": [],
            "total": 0,
            "page": page,
            "page_size": size,
            "error": str(e)
        })

# ---------- API：建立預約 (!!! 已優化 - CAS + 速率限制 !!!) ----------
@app.post("/api/reserve")
@limiter.limit("1 per 2 seconds") # (優化) 每 IP 2 秒只能請求一次
def reserve():
    payload = request.get_json(force=True)

    try:
        table_id = int(payload["table_id"])
        table_id_str = str(table_id) # (新) 儲存字串版本以供查閱
    except Exception:
        # (UI 英文)
        return jsonify(success=False, message="table_id is required"), 400

    seats = payload.get("seats_to_take", 1)
    try:
        seats = int(seats)
    except Exception:
        seats = 1

    employee_name = str(payload.get("employee_name", "")).strip() or "Guest"
    login_id = (str(payload.get("login_id", "")).strip() or "guest").lower()

    if seats < 1 or seats > MAX_PER_BOOKING:
        # (UI 英文)
        return jsonify(success=False, message=f"Invalid seat count (1-{MAX_PER_BOOKING})"), 400

    idem_key = request.headers.get("Idempotency-Key") or str(uuid.uuid4())

    try:
        # 1. 檢查防重複提交 (Idempotency)
        existing_idem = s3_store.get_idempotency_key(idem_key)
        if existing_idem:
            # (UI 英文)
            return jsonify(success=True, message="Already processed",
                           reservation_id=existing_idem.get("reservation_id")), 200

        # 2. (優化) 檢查登入 ID，現在會從快取讀取，速度極快
        if s3_store.check_login_id_exists(login_id):
            # (UI 英文)
            return jsonify(success=False, message="This login_id already has a reservation."), 409

        # --- (優化) CAS 原子性更新 tables.json ---
        # 3. 取得目前的 tables.json 和 ETag
        tables_data, etag = s3_store.get_tables_data_with_etag()
        if not tables_data:
            # (UI 英文)
            return jsonify(success=False, message="Server error: Cannot read tables data"), 500
            
        # 4. 檢查座位
        if table_id_str not in tables_data:
             # (UI 英文)
             return jsonify(success=False, message="Table not found"), 404
        if tables_data[table_id_str]["seats_left"] < seats:
            # (UI 英文)
            return jsonify(success=False, message="This table is full or seats are not enough now."), 409 # 409 Conflict

        # 5. 在記憶體中修改
        tables_data[table_id_str]["seats_left"] -= seats

        # 6. 嘗試原子性寫回 (Compare-And-Swap)
        try:
            s3_store.save_tables_data_cas(tables_data, etag)
        except ClientError as e:
            if e.response['Error']['Code'] == 'PreconditionFailed':
                # --- 這就是 409 衝突！ ---
                # 意思是：在我們讀取和寫回之間，檔案被其他人改了 (ETag 不匹配)
                # (日誌 中文)
                logger.warning(f"CAS 409: S3 PreconditionFailed (幾乎同時提交) for Table {table_id_str}")
                # (UI 英文)
                return jsonify(success=False, message="You were too slow! That seat was just taken. Please select another."), 409 # 409 Conflict
            else:
                raise # 其他 S3 錯誤
        # --- CAS 結束，座位已成功扣除 ---

        # 7. 建立預約（使用台灣時區）
        reservation_id = str(uuid.uuid4())
        reservation_data = {
            "id": reservation_id,
            "table_id": table_id,
            "seats_taken": seats,
            "employee_name": employee_name,
            "login_id": login_id,
            "created_at": datetime.now(TAIWAN_TZ).isoformat()
        }
        # (日誌 中文)
        logger.debug(f"[DEBUG]  đang tạo đặt chỗ: {reservation_data}")

        # 8. 儲存預約和防重複鍵
        # (MODIFIED) 傳入 reservation_id 和 date_str=None (讓 s3_store 用今天日期)
        if s3_store.save_reservation(reservation_id, reservation_data, date_str=None):
            s3_store.save_idempotency_key(idem_key, {"reservation_id": reservation_id})
            # (日誌 中文)
            logger.info(f"預訂成功建立: {reservation_id}")
            # (UI 英文)
            return jsonify(success=True, message="Reservation confirmed!",
                           reservation_id=reservation_id, table_id=table_id), 201
        else:
            # 復原 (Rollback)
            # (日誌 中文)
            logger.warning(f"儲存預訂 {reservation_id} 失敗, 正在回復座位...")
            s3_store.release_seats_cas(table_id, seats) # (使用 CAS 版本的釋放)
            # (UI 英文)
            return jsonify(success=False, message="Failed to save reservation"), 500

    except Exception as e:
        # (日誌 中文)
        logger.error(f"[ERROR] 預訂失敗: {e}")
        # (重要) 嘗試回復座位
        try:
            # 檢查 table_id 是否已定義
            if 'table_id' in locals():
                s3_store.release_seats_cas(table_id, seats)
                # (日誌 中文)
                logger.info(f"因錯誤 {e} 回復桌位 {table_id}")
            else:
                # (日誌 中文)
                logger.error(f"因錯誤 {e} 預訂失敗，但 table_id 未定義，無法回復座位")
        except Exception as rollback_e:
            # (日誌 中文)
            logger.error(f"[CRITICAL] 回復失敗! {rollback_e}")
        # (UI 英文)
        return jsonify(success=False, message=f"Reservation failed: {str(e)}"), 500

# ---------- API：取消預約 (!!! 已優化 - CAS !!!) ----------
@app.post("/api/cancel")
@require_admin_token
def cancel():
    payload = request.get_json(force=True)
    reservation_id = payload.get("reservation_id")

    if not reservation_id:
        # (UI 英文)
        return jsonify(success=False, message="reservation_id required"), 400

    # (MODIFIED) 
    # 1. 尋找訂單和它的日期
    reservation, date_str = _find_reservation_and_date(reservation_id)
    
    if not reservation:
        # (UI 英文)
        return jsonify(success=False, message="Reservation not found"), 404

    # 2. 呼叫 delete_reservation 並傳入日期
    if s3_store.delete_reservation(reservation_id, date_str):
        # 3. (優化) 成功刪除後，使用 CAS 釋放座位
        if s3_store.release_seats_cas(reservation["table_id"], reservation["seats_taken"]):
            return jsonify(success=True)
        else:
             # (日誌 中文)
             logger.warning(f"預訂 {reservation_id} 已刪除, 但 CAS 釋放座位失敗")
             # 仍回傳成功，因為預訂確實被刪了
             # (UI 英文)
             return jsonify(success=True, message="Reservation deleted, but seat release failed. Please resync.")
    else:
        # (UI 英文)
        return jsonify(success=False, message="Failed to cancel reservation"), 500


# ---------- (!!! 已優化 - CAS !!!) API：更新訂位資訊 ----------
@app.post("/api/admin/update_reservation")
@require_admin_token
def update_reservation_details():
    payload = request.get_json(force=True)
    reservation_id = payload.get("reservation_id")
    new_login_id = payload.get("login_id")
    new_name = payload.get("employee_name")
    
    try:
        # (NEW) 取得新的座位數
        new_seats = int(payload.get("seats_taken"))
    except (ValueError, TypeError):
        # (UI 英文)
        return jsonify(success=False, message="Invalid seats_taken value"), 400

    if not reservation_id or not new_login_id or not new_name:
        # (UI 英文)
        return jsonify(success=False, message="Missing required fields"), 400
    
    if new_seats < 1 or new_seats > MAX_PER_BOOKING:
        # (UI 英文)
        return jsonify(success=False, message=f"Seats must be between 1 and {MAX_PER_BOOKING}"), 400

    # 格式化
    new_login_id = new_login_id.strip().lower()
    new_name = new_name.strip()
    
    if not new_login_id or not new_name:
        # (UI 英文)
        return jsonify(success=False, message="Fields cannot be empty"), 400

    seat_diff = 0 # 初始化座位差異
    table_id = None

    try:
        # 1. 取得現有訂位和日期
        reservation, date_str = _find_reservation_and_date(reservation_id)
        if not reservation:
            # (UI 英文)
            return jsonify(success=False, message="Reservation not found"), 404

        current_login_id = reservation.get("login_id")
        current_seats = reservation.get("seats_taken")
        table_id = reservation.get("table_id") # 取得桌號以供復原

        # 2. (NEW) 處理座位數變更 (使用 CAS)
        seat_diff = new_seats - current_seats
        
        if seat_diff > 0:
            # 嘗試增加座位
            if not s3_store.reserve_seats_cas(table_id, seat_diff):
                # (UI 英文)
                return jsonify(success=False, message=f"Not enough seats on Table {table_id} (or CAS conflict)"), 409
        elif seat_diff < 0:
            # 減少座位
            if not s3_store.release_seats_cas(table_id, abs(seat_diff)):
                 # (日誌 中文)
                 logger.warning(f"更新 {reservation_id} 時釋放座位失敗")
                 # 即使釋放失敗，我們還是可以繼續更新文字資訊

        # 3. 檢查 Login ID 是否被變更
        if current_login_id != new_login_id:
            # 4. 如果變G了，檢查新的 Login ID 是否已被他人使用 (受益於快取)
            if s3_store.check_login_id_exists(new_login_id):
                # (NEW) 復原座位變更！
                if seat_diff > 0:
                    s3_store.release_seats_cas(table_id, seat_diff) # 歸還剛才預訂的座位
                elif seat_diff < 0:
                    s3_store.reserve_seats_cas(table_id, abs(seat_diff)) # 拿回剛才釋放的座位
                # (UI 英文)
                return jsonify(success=False, message=f"The new login_id '{new_login_id}' is already taken."), 409
        
        # 5. 更新訂位物件
        reservation["login_id"] = new_login_id
        reservation["employee_name"] = new_name
        reservation["seats_taken"] = new_seats
        reservation["updated_at"] = datetime.now(TAIWAN_TZ).isoformat()

        # 6. 儲存回 S3 (save_reservation 會清除快取)
        if s3_store.update_reservation(reservation_id, reservation, date_str):
            # (UI 英文)
            return jsonify(success=True, message="Reservation updated.")
        else:
            # (NEW) 復原座位變更！
            if seat_diff > 0:
                s3_store.release_seats_cas(table_id, seat_diff)
            elif seat_diff < 0:
                s3_store.reserve_seats_cas(table_id, abs(seat_diff))
            # (UI 英文)
            return jsonify(success=False, message="Failed to save update to S3."), 500

    except Exception as e:
        # (日誌 中文)
        logger.error(f"[ERROR] 更新預訂失敗: {e}")
        # (NEW) 處理未知的錯誤，並嘗試復原 (更安全的 rollback)
        if seat_diff > 0 and table_id:
            s3_store.release_seats_cas(table_id, seat_diff)
        elif seat_diff < 0 and table_id:
             s3_store.reserve_seats_cas(table_id, abs(seat_diff))
        return jsonify(success=False, message=str(e)), 500

# ---------- API：資料重新同步 (!!! 已優化 - CAS !!!) ----------
@app.post("/api/admin/resync")
@require_admin_token # 確保只有 Admin 能執行
def admin_resync():
    """
    (優化) 重新計算所有桌子的 seats_left。
    以 'reservations/' 資料夾為準，去更新 'tables.json'。
    使用 CAS 寫回以避免覆蓋預訂。
    """
    try:
        # (日誌 中文)
        logger.info("[INFO] 開始重新同步桌位資料...")
        
        # 1. 取得「所有」訂單 (受益於快取)
        all_reservations = s3_store.get_all_reservations()

        # 2. 重新計算每張桌子「實際」被佔用的座位數
        actual_seats_taken = {} # 格式: { "table_id_str": count, ... }
        for r in all_reservations:
            table_id_str = str(r.get("table_id"))
            seats = r.get("seats_taken", 1) # 取得佔用座位，預設 1
            
            if table_id_str not in actual_seats_taken:
                actual_seats_taken[table_id_str] = 0
            actual_seats_taken[table_id_str] += seats

        # 3. (優化) 取得目前桌位資料和 ETag
        tables_data, etag = s3_store.get_tables_data_with_etag()
        if not tables_data:
            # (UI 英文)
            return jsonify(success=False, message="No tables data found to resync."), 500

        # 4. 迴圈檢查 'tables.json' 並修正 'seats_left'
        updated_count = 0
        for table_id_str, table in tables_data.items():
            total_seats = table.get("total", 10) # 取得總座位數
            taken_seats = actual_seats_taken.get(table_id_str, 0) # 取得實際佔用數
            
            new_seats_left = total_seats - taken_seats
            
            # 如果 S3 上的 'seats_left' 不等於我們剛算出的新數字，就更新它
            if table["seats_left"] != new_seats_left:
                # (日誌 中V)
                logger.info(f"[RESYNC] 桌號 {table_id_str}: 剩餘座位 {table['seats_left']}, 修正為 {new_seats_left}")
                table["seats_left"] = new_seats_left
                updated_count += 1
            
        # 5. (優化) 將修正後的 'tables_data' 使用 CAS 完整存回 S3
        try:
            s3_store.save_tables_data_cas(tables_data, etag)
            # (日誌 中文)
            logger.info(f"[INFO] 重新同步完成。 {updated_count} 張桌子已修正。")
            # (UI 英文)
            return jsonify(success=True, message=f"Resync complete. {updated_count} table(s) corrected.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'PreconditionFailed':
                # (日誌 中文)
                logger.warning("[RESYNC] 重新同步時發生 CAS 衝突。請重試。")
                # (UI 英文)
                return jsonify(success=False, message="Conflict during resync (someone was reserving). Please try again."), 409
            else:
                raise # 其他 S3 錯誤

    except Exception as e:
        # (日誌 中文)
        logger.error(f"[ERROR] 重新同步失敗: {e}")
        return jsonify(success=False, message=str(e)), 500


# ---------- 匯出 CSV (!!! 已優化 !!!) ----------
@app.get("/api/reservations.csv")
@require_admin_token
def export_csv():
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["id", "table_id", "seats_taken", "employee_name", "login_id", "created_at"])

    # (優化) 受益於快取
    reservations = s3_store.get_all_reservations()
    reservations.sort(key=lambda x: x.get("created_at", ""), reverse=True)

    for r in reservations:
        writer.writerow([
            r.get("id", ""),
            r.get("table_id", ""),
            r.get("seats_taken", ""),
            r.get("employee_name", ""),
            r.get("login_id", ""),
            r.get("created_at", "")
        ])

    mem = io.BytesIO(out.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name="reservations.csv")

# ---------- (新) 速率限制的錯誤處理 ----------
@app.errorhandler(429)
def ratelimit_handler(e):
    """自訂 429 (Too Many Requests) 的回應"""
    # (日誌 中文)
    logger.warning(f"IP {get_ip_address()} 觸發速率限制: {e.description}")
    return jsonify(
        success=False,
        # (UI 英文)
        # e.description 可能是 "1 per 2 second"，我們取 "2"
        message=f"Too many requests. Please wait {e.description.split(' ')[-2]} seconds."
    ), 429

# ---------- 啟動 ----------
if __name__ == "__main__":
    # Gunicorn 會忽略這一段，只有在本地執行 `python app.py` 時才會用到
    try:
        init_tables()
    except Exception as e:
        # (日誌 中文)
        print(f"[WARN] init_tables 已跳過: {e}")
    # (日誌 中文)
    print(f"[INFO] Admin 驗證: {'啟用' if ENABLE_ADMIN_AUTH else '停用'}")
    port = int(os.getenv("PORT", 8000))
    # (注意) 執行 gunicorn 時不會使用 debug=True
    app.run(host="0.0.0.0", port=port, debug=False)
