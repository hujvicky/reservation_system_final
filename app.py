# ======================================
# Reservation System - AWS App Runner + S3 版
# 完整功能：CRUD + 防重投 + login_id 唯一(不分大小寫) + S3 儲存
# ======================================

from flask import Flask, request, jsonify, send_file, render_template
from flask_cors import CORS
from s3_store import S3Store
from pathlib import Path
import os, io, csv, uuid
from datetime import datetime
import json

# ---------- 基本路徑 ----------
PROJECT_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PROJECT_DIR / "templates"

app = Flask(__name__, template_folder=str(TEMPLATES_DIR), static_folder="static")
CORS(app)

# ---------- S3 儲存初始化 ----------
s3_store = S3Store()

MAX_PER_BOOKING = 3

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

# ---------- API：座位狀態 ----------
@app.get("/api/status")
def api_status():
    tables_data = s3_store.get_tables_data()
    if not tables_data:
        return jsonify({"tables": []})

    tables_list = []
    for table_id in sorted(tables_data.keys(), key=int):
        table = tables_data[table_id]
        tables_list.append({
            "table_id": table["id"],
            "seats_left": table["seats_left"]
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
def list_reservations():
    table_id = request.args.get("table_id", type=int)
    page = max(1, request.args.get("page", default=1, type=int))
    size = min(100, max(1, request.args.get("page_size", default=50, type=int)))

    # 獲取所有預約
    all_reservations = s3_store.get_all_reservations()

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

# ---------- API：建立預約 ----------
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

        # 建立預約
        reservation_id = str(uuid.uuid4())
        reservation_data = {
            "id": reservation_id,
            "table_id": table_id,
            "seats_taken": seats,
            "employee_name": employee_name,
            "login_id": login_id,
            "created_at": datetime.now().isoformat()
        }

        # 儲存預約和防重複鍵
        if s3_store.save_reservation(reservation_id, reservation_data):
            s3_store.save_idempotency_key(idem_key, {"reservation_id": reservation_id})
            return jsonify(success=True, message="Reservation confirmed!",
                         reservation_id=reservation_id, table_id=table_id), 201
        else:
            # 如果儲存失敗，回復座位
            s3_store.release_seats(table_id, seats)
            return jsonify(success=False, message="Failed to save reservation"), 500

    except Exception as e:
        return jsonify(success=False, message=f"Reservation failed: {str(e)}"), 500

# ---------- API：取消預約 ----------
@app.post("/api/cancel")
def cancel():
    payload = request.get_json(force=True)
    reservation_id = payload.get("reservation_id")

    if not reservation_id:
        return jsonify(success=False, message="reservation_id required"), 400

    reservation = s3_store.get_reservation(reservation_id)
    if not reservation:
        return jsonify(success=False, message="Reservation not found"), 404

    # 刪除預約並回復座位
    if s3_store.delete_reservation(reservation_id):
        s3_store.release_seats(reservation["table_id"], reservation["seats_taken"])
        return jsonify(success=True)
    else:
        return jsonify(success=False, message="Failed to cancel reservation"), 500

# ---------- API：減少座位 ----------
@app.post("/api/reduce")
def reduce_seats():
    payload = request.get_json(force=True)
    reservation_id = payload.get("reservation_id")
    reduce_by = payload.get("reduce_by", 0)

    try:
        reduce_by = int(reduce_by)
    except Exception:
        reduce_by = 0

    if not reservation_id or reduce_by <= 0:
        return jsonify(success=False, message="Invalid input"), 400

    reservation = s3_store.get_reservation(reservation_id)
    if not reservation:
        return jsonify(success=False, message="Reservation not found"), 404

    current_seats = reservation["seats_taken"]

    if reduce_by >= current_seats:
        # 完全取消預約
        if s3_store.delete_reservation(reservation_id):
            s3_store.release_seats(reservation["table_id"], current_seats)
            return jsonify(success=True, message=f"Reservation cancelled, returned {current_seats} seat(s).")
    else:
        # 減少座位數
        updated_reservation = reservation.copy()
        updated_reservation["seats_taken"] = current_seats - reduce_by
        updated_reservation["updated_at"] = datetime.now().isoformat()

        if s3_store.update_reservation(reservation_id, updated_reservation):
            s3_store.release_seats(reservation["table_id"], reduce_by)
            return jsonify(success=True, message=f"Reduced {reduce_by} seat(s).")

    return jsonify(success=False, message="Failed to reduce seats"), 500

# ---------- 匯出 CSV ----------
@app.get("/api/reservations.csv")
def export_csv():
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["id", "table_id", "seats_taken", "employee_name", "login_id", "created_at"])

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

# ---------- 啟動 ----------
if __name__ == "__main__":
    try:
        init_tables()
    except Exception as e:
        print(f"[WARN] init_tables skipped: {e}")
    port = int(os.getenv("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
