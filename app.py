# ======================================
# app.py - 最終修正版 v9
# ======================================

from flask import Flask, request, jsonify, send_file, render_template
from flask_cors import CORS
from s3_store import S3Store
from ddb_store import DynamoDBStore
from pathlib import Path
import os, io, csv, uuid, jwt
from datetime import datetime, timezone, timedelta
from functools import wraps
import json
import logging

# (NEW) 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

APP_VERSION = "3.0-fix-delete-bug" # <-- 版本號

# ---------- 台灣時區設定 ----------
TAIWAN_TZ = timezone(timedelta(hours=8))

# ---------- Admin 設定 ----------
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "888888"
JWT_SECRET = os.environ.get('JWT_SECRET', 'your-secret-key-change-this-in-production')

# 是否啟用 Admin 驗證（設為 False 可暫時關閉驗證）
ENABLE_ADMIN_AUTH = os.environ.get('ENABLE_ADMIN_AUTH', 'false').lower() == 'true'

# ---------- 基本路徑 ----------
PROJECT_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PROJECT_DIR / "templates"

app = Flask(__name__, template_folder=str(TEMPLATES_DIR), static_folder="static")
CORS(app)

# ---------- 儲存層初始化 ----------
# 根據環境變數選擇儲存後端 (預設使用DynamoDB)
USE_DYNAMODB = os.environ.get('USE_DYNAMODB', 'true').lower() == 'true'

if USE_DYNAMODB:
    print("[INFO] Using DynamoDB as storage backend")
    store = DynamoDBStore()
else:
    print("[INFO] Using S3 as storage backend")
    store = S3Store()

MAX_PER_BOOKING = 3

# ---------- Token 驗證裝飾器 ----------
def require_admin_token(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # 如果驗證被停用，直接執行函數
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

# ---------- (!!! 修正 1 !!!) 輔助函式：根據 ID 查找訂單並取得日期 ----------
def _find_reservation_and_date(reservation_id):
    """
    (修復 Bug 用)
    查找訂單並取得相關日期信息。
    """
    if not reservation_id:
        return None, None
        
    all_reservations = store.get_all_reservations()
    reservation = next((r for r in all_reservations if r.get('id') == reservation_id), None)
    
    if not reservation:
        return None, None

    try:
        created_at_iso = reservation.get("created_at")
        # 從 ISO 格式字串解析出 datetime 物件
        created_dt = datetime.fromisoformat(created_at_iso)
        # 格式化成需要的 'YYYY-MM-DD'
        date_str = created_dt.strftime('%Y-%m-%d')
        return reservation, date_str
    except Exception as e:
        logger.error(f"Could not parse date from reservation {reservation_id}: {e}")
        # 降級：嘗試用今天的日期 (可能會失敗)
        date_str = datetime.now(TAIWAN_TZ).strftime('%Y-%m-%d')
        return reservation, date_str

# ---------- 初始化桌位資料 ----------
def init_tables():
    """初始化桌位資料"""
    try:
        # 檢查是否已有桌位資料
        existing_tables = store.get_tables_data()
        if existing_tables:
            print("[INFO] Tables data already exists")
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

        if store.save_tables_data(tables_data):
            print("[INFO] Initialized tables (1..108).")
        else:
            print("[WARN] Failed to initialize tables.")

    except Exception as e:
        print(f"[WARN] init_tables failed: {e}")

# 啟動時初始化
try:
    init_tables()
except Exception as e:
    print(f"[WARN] init_tables skipped: {e}")

# ---------- (NEW) 版本檢查 API ----------
@app.route("/api/version")
def get_version():
    """檢查後端版本，用於除錯"""
    try:
        # 創建 S3Store 實例以檢查版本（向後相容）
        s3_store = S3Store() if not USE_DYNAMODB else None
        return jsonify(
            app_version=APP_VERSION, 
            store_type="DynamoDB" if USE_DYNAMODB else "S3",
            store_version=getattr(store, 'VERSION', 'unknown'),
            s3_store_version=getattr(s3_store, 'VERSION', 'unknown') if s3_store else 'N/A'
        )
    except Exception as e:
        return jsonify(
            app_version=APP_VERSION, 
            store_type="DynamoDB" if USE_DYNAMODB else "S3",
            store_version=getattr(store, 'VERSION', 'unknown'),
            error=str(e)
        )

# ---------- 頁面路由 ----------
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

# ---------- 儲存連線測試 ----------
@app.route('/test-connection')
def test_connection():
    if store.test_connection():
        store_type = "DynamoDB" if USE_DYNAMODB else "S3"
        message = f'{store_type} 連線成功！'
        response_data = {
            'status': 'success',
            'message': message,
            'store_type': store_type
        }
        
        # 如果是S3，添加bucket資訊
        if not USE_DYNAMODB:
            response_data['bucket'] = getattr(store, 'bucket_name', 'unknown')
            
        return jsonify(response_data)
    else:
        store_type = "DynamoDB" if USE_DYNAMODB else "S3"
        return jsonify({
            'status': 'error',
            'message': f'{store_type} 連線失敗',
            'store_type': store_type
        }), 500

# ---------- S3 連線測試 (向後相容) ----------
@app.route('/test-s3')
def test_s3():
    return test_connection()

# ---------- Admin 登入 API ----------
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
            'message': 'Login successful'
        })
    else:
        return jsonify({
            'success': False,
            'message': 'Invalid credentials'
        }), 401

# ---------- Token 驗證 API ----------
@app.get("/api/admin/verify")
@require_admin_token
def admin_verify():
    return jsonify({'success': True, 'message': 'Token valid'})

# ---------- API：座位狀態 (已修改版) ----------
@app.get("/api/status")
def api_status():
    tables_data = store.get_tables_data()
    if not tables_data:
        return jsonify({"tables": []})

    # === (MODIFIED) START: 查詢所有訂位並建立查詢表 (包含座位數) ===
    
    # 1. 取得所有訂位紀錄
    all_reservations = store.get_all_reservations()
    
    # 2. 建立一個 map (table_id_str -> [ {name:..., seats:...}, ... ])
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

# ---------- API：桌位清單 ----------
@app.get("/api/tables")
def api_tables():
    tables_data = store.get_tables_data()
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

# ---------- API：可用性 ----------
@app.get("/api/reservations/availability")
def api_availability():
    tables_data = store.get_tables_data()
    if not tables_data:
        return jsonify({"holds": [], "confirmed": []})

    confirmed = []
    for table_id, table in tables_data.items():
        if table["seats_left"] <= 0:
            confirmed.append({"table_id": table["id"]})

    return jsonify({"holds": [], "confirmed": confirmed})

# ---------- API：查詢預約 ----------
@app.get("/api/reservations")
@require_admin_token
def list_reservations():
    table_id = request.args.get("table_id", type=int)
    page = max(1, request.args.get("page", default=1, type=int))
    size = min(100, max(1, request.args.get("page_size", default=50, type=int)))

    try:
        if USE_DYNAMODB and hasattr(store, 'get_reservations_paginated') and table_id:
            # 使用優化的 GSI 查詢 (僅當有 table_id 篩選時)
            total = store.get_reservations_count(table_id_filter=table_id)
            print(f"[DEBUG] GSI Count for table_id {table_id}: {total}")
            
            # 對於有篩選的查詢，使用高效的 GSI 分頁
            skip_items = (page - 1) * size
            collected_items = []
            last_key = None
            
            # 收集足夠的數據以支援分頁
            items_collected = 0
            while items_collected < skip_items + size:
                fetch_size = min(size * 3, 100)
                result = store.get_reservations_paginated(
                    limit=fetch_size, 
                    last_key=last_key, 
                    table_id_filter=table_id
                )
                
                if not result['items']:
                    break
                    
                collected_items.extend(result['items'])
                items_collected += len(result['items'])
                last_key = result['last_key']
                
                if not result['has_more']:
                    break
            
            # 排序（最新的在前）
            collected_items.sort(key=lambda x: x.get("created_at", ""), reverse=True)
            
            # 取得所需頁面的數據
            start_idx = skip_items
            end_idx = start_idx + size
            data = collected_items[start_idx:end_idx]
            
            store_type = "DynamoDB (GSI Query)"
            print(f"[DEBUG] {store_type} returned {len(data)} items for page {page}")
            
        else:
            # 對於一般查詢（無篩選）或 S3，使用可靠的全量載入方式
            all_reservations = store.get_all_reservations()
            store_type = "S3" if not USE_DYNAMODB else "DynamoDB (Full Scan)"
            print(f"[DEBUG] {store_type} loaded {len(all_reservations)} total reservations")

            # 過濾條件（如果有的話）
            if table_id:
                all_reservations = [r for r in all_reservations if r.get("table_id") == table_id]
                print(f"[DEBUG] After filtering by table_id {table_id}: {len(all_reservations)} reservations")

            # 排序（最新的在前）
            all_reservations.sort(key=lambda x: x.get("created_at", ""), reverse=True)

            # 記憶體分頁（可靠且一致）
            total = len(all_reservations)
            start = (page - 1) * size
            end = start + size
            data = all_reservations[start:end]
            
            print(f"[DEBUG] Memory pagination: page {page}, showing items {start}-{end-1} of {total}")

        return jsonify({
            "data": data,
            "total": total,
            "page": page,
            "page_size": size,
            "store_type": store_type  # 用於除錯
        })
        
    except Exception as e:
        print(f"[ERROR] list_reservations failed: {e}")
        return jsonify({
            "data": [],
            "total": 0,
            "page": page,
            "page_size": size,
            "error": str(e)
        })

# ---------- API：建立預約 (!!! 修正 2 !!!) ----------
@app.post("/api/reserve")
def reserve():
    payload = request.get_json(force=True)

    try:
        table_id = int(payload["table_id"])
    except Exception:
        return jsonify(success=False, message="table_id is required"), 400

    seats = payload.get("seats_to_take", 1)
    try:
        seats = int(seats)
    except Exception:
        seats = 1

    employee_name = str(payload.get("employee_name", "")).strip() or "Guest"
    login_id = (str(payload.get("login_id", "")).strip() or "guest").lower()

    if seats < 1 or seats > MAX_PER_BOOKING:
        return jsonify(success=False, message=f"Invalid seat count (1-{MAX_PER_BOOKING})"), 400

    idem_key = request.headers.get("Idempotency-Key") or str(uuid.uuid4())

    try:
        # 檢查防重複提交
        existing_idem = store.get_idempotency_key(idem_key)
        if existing_idem:
            return jsonify(success=True, message="Already processed",
                           reservation_id=existing_idem.get("reservation_id")), 200

        # 檢查 login_id 是否已存在
        if store.check_login_id_exists(login_id):
            return jsonify(success=False, message="This login_id already has a reservation."), 409

        # 檢查座位是否足夠並扣除
        if not store.reserve_seats(table_id, seats):
            return jsonify(success=False, message="This table is full or seats are not enough now."), 409

        # 建立預約（使用台灣時區）
        reservation_id = str(uuid.uuid4())
        reservation_data = {
            "id": reservation_id,
            "table_id": table_id,
            "seats_taken": seats,
            "employee_name": employee_name,
            "login_id": login_id,
            "created_at": datetime.now(TAIWAN_TZ).isoformat()
        }

        print(f"[DEBUG] Creating reservation: {reservation_data}")

        # 儲存預約和防重複鍵
        if store.save_reservation(reservation_id, reservation_data, date_str=None):
            store.save_idempotency_key(idem_key, {"reservation_id": reservation_id})
            print(f"[INFO] Reservation created successfully: {reservation_id}")
            return jsonify(success=True, message="Reservation confirmed!",
                           reservation_id=reservation_id, table_id=table_id), 201
        else:
            # 如果儲存失敗，回復座位
            store.release_seats(table_id, seats)
            return jsonify(success=False, message="Failed to save reservation"), 500

    except Exception as e:
        print(f"[ERROR] Reservation failed: {e}")
        return jsonify(success=False, message=f"Reservation failed: {str(e)}"), 500

# ---------- API：取消預約 (!!! 修正 3 !!!) ----------
@app.post("/api/cancel")
@require_admin_token
def cancel():
    payload = request.get_json(force=True)
    reservation_id = payload.get("reservation_id")

    if not reservation_id:
        return jsonify(success=False, message="reservation_id required"), 400

    # (MODIFIED) 
    # 1. 尋找訂單和它的日期
    reservation, date_str = _find_reservation_and_date(reservation_id)
    
    if not reservation:
        return jsonify(success=False, message="Reservation not found"), 404

    # 2. 呼叫 delete_reservation 並傳入日期
    if store.delete_reservation(reservation_id, date_str):
        # 3. 成功刪除後，釋放座位
        store.release_seats(reservation["table_id"], reservation["seats_taken"])
        return jsonify(success=True)
    else:
        return jsonify(success=False, message="Failed to cancel reservation"), 500

# ---------- (REMOVED) API：減少座位 ----------
# ... (此函式已被移除) ...

# ---------- (!!! 修正 4 !!!) API：更新訂位資訊 ----------
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
        return jsonify(success=False, message="Invalid seats_taken value"), 400

    if not reservation_id or not new_login_id or not new_name:
        return jsonify(success=False, message="Missing required fields"), 400
    
    if new_seats < 1 or new_seats > MAX_PER_BOOKING:
        return jsonify(success=False, message=f"Seats must be between 1 and {MAX_PER_BOOKING}"), 400

    # 格式化
    new_login_id = new_login_id.strip().lower()
    new_name = new_name.strip()
    
    if not new_login_id or not new_name:
        return jsonify(success=False, message="Fields cannot be empty"), 400

    seat_diff = 0 # 初始化座位差異
    table_id = None

    try:
        # 1. 取得現有訂位和日期
        reservation, date_str = _find_reservation_and_date(reservation_id)
        if not reservation:
            return jsonify(success=False, message="Reservation not found"), 404

        current_login_id = reservation.get("login_id")
        current_seats = reservation.get("seats_taken")
        table_id = reservation.get("table_id") # 取得桌號以供復原

        # 2. (NEW) 處理座位數變更
        seat_diff = new_seats - current_seats
        
        if seat_diff > 0:
            # 嘗試增加座位
            if not store.reserve_seats(table_id, seat_diff):
                return jsonify(success=False, message=f"Not enough seats available on Table {table_id} to add {seat_diff} seat(s)."), 409
        elif seat_diff < 0:
            # 減少座位
            store.release_seats(table_id, abs(seat_diff))

        # 3. 檢查 Login ID 是否被變更
        if current_login_id != new_login_id:
            # 4. 如果變更了，檢查新的 Login ID 是否已被他人使用
            if store.check_login_id_exists(new_login_id):
                # (NEW) 復原座位變更！
                if seat_diff > 0:
                    store.release_seats(table_id, seat_diff) # 歸還剛才預訂的座位
                elif seat_diff < 0:
                    store.reserve_seats(table_id, abs(seat_diff)) # 拿回剛才釋放的座位
                return jsonify(success=False, message=f"The new login_id '{new_login_id}' is already taken."), 409
        
        # 5. 更新訂位物件
        reservation["login_id"] = new_login_id
        reservation["employee_name"] = new_name
        reservation["seats_taken"] = new_seats
        reservation["updated_at"] = datetime.now(TAIWAN_TZ).isoformat()

        # 6. 儲存更新
        if store.update_reservation(reservation_id, reservation, date_str):
            return jsonify(success=True, message="Reservation updated.")
        else:
            # (NEW) 復原座位變更！
            if seat_diff > 0:
                store.release_seats(table_id, seat_diff)
            elif seat_diff < 0:
                store.reserve_seats(table_id, abs(seat_diff))
            return jsonify(success=False, message="Failed to save update."), 500

    except Exception as e:
        logger.error(f"[ERROR] Update reservation failed: {e}")
        # (NEW) 處理未知的錯誤，並嘗試復原 (更安全的 rollback)
        if seat_diff > 0 and table_id:
            store.release_seats(table_id, seat_diff)
        elif seat_diff < 0 and table_id:
             store.reserve_seats(table_id, abs(seat_diff))
        return jsonify(success=False, message=str(e)), 500

# ---------- API：資料重新同步 ----------
@app.post("/api/admin/resync")
@require_admin_token # 確保只有 Admin 能執行
def admin_resync():
    """
    重新計算所有桌子的 seats_left。
    以訂位資料為準，去更新桌位狀態。
    """
    try:
        print("[INFO] Starting table data resynchronization...")
        
        # 1. 取得目前桌位資料 (我們需要 'total' 總容量)
        tables_data = store.get_tables_data()
        if not tables_data:
            return jsonify(success=False, message="No tables data found to resync."), 500

        # 2. 取得「所有」訂單 (這是我們唯一的「事實來源」)
        all_reservations = store.get_all_reservations()

        # 3. 重新計算每張桌子「實際」被佔用的座位數
        actual_seats_taken = {} # 格式: { "table_id_str": count, ... }
        for r in all_reservations:
            table_id_str = str(r.get("table_id"))
            seats = r.get("seats_taken", 1) # 取得佔用座位，預設 1
            
            if table_id_str not in actual_seats_taken:
                actual_seats_taken[table_id_str] = 0
            actual_seats_taken[table_id_str] += seats

        # 4. 迴圈檢查桌位資料並修正 'seats_left'
        updated_count = 0
        for table_id_str, table in tables_data.items():
            total_seats = table.get("total", 10) # 取得總座位數
            taken_seats = actual_seats_taken.get(table_id_str, 0) # 取得實際佔用數
            
            new_seats_left = total_seats - taken_seats
            
            # 如果現有的 'seats_left' 不等於我們剛算出的新數字，就更新它
            if table["seats_left"] != new_seats_left:
                print(f"[RESYNC] Table {table_id_str}: Seats left was {table['seats_left']}, correcting to {new_seats_left}")
                table["seats_left"] = new_seats_left
                updated_count += 1
            
        # 5. 將修正後的 'tables_data' 完整存回儲存系統
        if store.save_tables_data(tables_data):
            print(f"[INFO] Resync complete. {updated_count} table(s) were corrected.")
            return jsonify(success=True, message=f"Resync complete. {updated_count} table(s) corrected.")
        else:
            print("[ERROR] Resync failed: Could not save updated tables data.")
            return jsonify(success=False, message="Failed to save corrected data."), 500

    except Exception as e:
        print(f"[ERROR] Resync failed: {e}")
        return jsonify(success=False, message=str(e)), 500


# ---------- 匯出 CSV ----------
@app.get("/api/reservations.csv")
@require_admin_token
def export_csv():
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["id", "table_id", "seats_taken", "employee_name", "login_id", "created_at"])

    reservations = store.get_all_reservations()
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

# ---------- 啟動 ----------
if __name__ == "__main__":
    try:
        init_tables()
    except Exception as e:
        print(f"[WARN] init_tables skipped: {e}")

    print(f"[INFO] Admin authentication: {'ENABLED' if ENABLE_ADMIN_AUTH else 'DISABLED'}")
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=8080, debug=False)
