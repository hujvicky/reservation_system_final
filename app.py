# ======================================
# app.py - 最終修正版
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

# (NEW) 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# ---------- S3 儲存初始化 ----------
s3_store = S3Store()

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
    因為 s3_store 是用日期分資料夾，我們必須先找到訂單的日期。
    """
    if not reservation_id:
        return None, None
        
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
        logger.error(f"Could not parse date from reservation {reservation_id}: {e}")
        # 降級：嘗試用今天的日期 (可能會失敗)
        date_str = datetime.now(TAIWAN_TZ).strftime('%Y-%m-%d')
        return reservation, date_str

# ---------- 初始化桌位資料 ----------
def init_tables():
    """初始化桌位資料到 S3"""
    try:
        # 檢查是否已有桌位資料
        existing_tables = s3_store.get_tables_data()
        if existing_tables:
            print("[INFO] Tables data already exists in S3")
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
            print("[INFO] Initialized tables (1..108) in S3.")
        else:
            print("[WARN] Failed to initialize tables in S3.")

    except Exception as e:
        print(f"[WARN] init_tables failed: {e}")

# 啟動時初始化
try:
    init_tables()
except Exception as e:
    print(f"[WARN] init_tables skipped: {e}")

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

# ---------- S3 連線測試 ----------
@app.route('/test-s3')
def test_s3():
    if s3_store.test_connection():
        return jsonify({
            'status': 'success',
            'message': 'S3 連線成功！',
            'bucket': s3_store.bucket_name
        })
    else:
        return jsonify({
            'status': 'error',
            'message': 'S3 連線失敗'
        }), 500

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
    tables_data = s3_store.get_tables_data()
    if not tables_data:
        return jsonify({"tables": []})

    # === (MODIFIED) START: 查詢所有訂位並建立查詢表 (包含座位數) ===
    
    # 1. 取得所有訂位紀錄
    all_reservations = s3_store.get_all_reservations()
    
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

# ---------- API：可用性 ----------
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

# ---------- API：查詢預約 ----------
@app.get("/api/reservations")
@require_admin_token
def list_reservations():
    table_id = request.args.get("table_id", type=int)
    page = max(1, request.args.get("page", default=1, type=int))
    size = min(100, max(1, request.args.get("page_size", default=50, type=int)))

    try:
        # 獲取所有預約
        all_reservations = s3_store.get_all_reservations()
        print(f"[DEBUG] Found {len(all_reservations)} reservations in S3")

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
        existing_idem = s3_store.get_idempotency_key(idem_key)
        if existing_idem:
            return jsonify(success=True, message="Already processed",
                           reservation_id=existing_idem.get("reservation_id")), 200

        # 檢查 login_id 是否已存在
        if s3_store.check_login_id_exists(login_id):
            return jsonify(success=False, message="This login_id already has a reservation."), 409

        # 檢查座位是否足夠並扣除
        if not s3_store.reserve_seats(table_id, seats):
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
        # (MODIFIED) 傳入 reservation_id 和 date_str=None (讓 s3_store 用今天日期)
        # 這裡的 date_str=None 很重要，這樣 s3_store.py 才會自動抓今天日期
        if s3_store.save_reservation(reservation_id, reservation_data, date_str=None):
            s3_store.save_idempotency_key(idem_key, {"reservation_id": reservation_id})
            print(f"[INFO] Reservation created successfully: {reservation_id}")
            return jsonify(success=True, message="Reservation confirmed!",
                           reservation_id=reservation_id, table_id=table_id), 201
        else:
            # 如果儲存失敗，回復座位
            s3_store.release_seats(table_id, seats)
            return jsonify(success=False, message="Failed to save reservation"), 500

    except Exception
