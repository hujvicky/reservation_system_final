# ======================================
# AWS App Runner 兼容版：保留原功能完整CRUD + 自動偵測可寫DB
# ======================================

from flask import Flask, request, jsonify, send_file, render_template
from flask_cors import CORS
from models import db, TableInventory, Reservation, IdempotencyKey
from pathlib import Path
import os, shutil, io, csv, datetime as dt

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
LOCAL_DB_PATH = DATA_DIR / "reservations.db"
TMP_DB_PATH = Path("/tmp/reservations.db")

app = Flask(
    __name__,
    template_folder=str(PROJECT_DIR / "templates"),
    static_folder=str(PROJECT_DIR / "static")
)
CORS(app)

# === 資料庫設定：自動偵測環境 ===
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    DATA_DIR.mkdir(exist_ok=True)
    # 🔹 App Runner 容器的根目錄是唯讀，複製一份 DB 到 /tmp
    if not TMP_DB_PATH.exists() and LOCAL_DB_PATH.exists():
        shutil.copy(LOCAL_DB_PATH, TMP_DB_PATH)
    elif not LOCAL_DB_PATH.exists():
        print("[⚠️ Warning] 未找到本地 reservations.db，系統會自動建立新資料庫。")
    DATABASE_URL = f"sqlite:///{TMP_DB_PATH}"

app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(app)

MAX_PER_BOOKING = 3


# === 初始化資料表 ===
def init_seed():
    with app.app_context():
        db.create_all()
        if TableInventory.query.count() == 0:
            for i in range(1, 109):
                t = TableInventory(id=i, name=f"Table {i}", total=10, seats_left=10)
                db.session.add(t)
            db.session.commit()
            print("[✅] Initialized TableInventory data.")


# === 狀態檢查 ===
@app.get("/api/status")
def api_status():
    return jsonify(status="ok", db=DATABASE_URL)


# === 查詢桌位 ===
@app.get("/api/tables")
def api_tables():
    with app.app_context():
        tables = TableInventory.query.all()
        return jsonify([
            {"id": t.id, "name": t.name, "total": t.total, "seats_left": t.seats_left}
            for t in tables
        ])


# === 查詢所有預約 ===
@app.get("/api/reservations")
def api_reservations():
    with app.app_context():
        reservations = Reservation.query.order_by(Reservation.id.desc()).all()
        return jsonify([
            {"id": r.id, "name": r.name, "table_id": r.table_id, "seats": r.seats, "created_at": r.created_at.isoformat()}
            for r in reservations
        ])


# === 新增預約 ===
@app.post("/api/reserve")
def api_reserve():
    data = request.get_json()
    name = data.get("name")
    table_id = data.get("table_id")
    seats = data.get("seats", 1)

    if not name or not table_id:
        return jsonify(error="Missing required fields"), 400
    if seats > MAX_PER_BOOKING:
        return jsonify(error=f"Cannot book more than {MAX_PER_BOOKING} seats"), 400

    with app.app_context():
        table = TableInventory.query.filter_by(id=table_id).first()
        if not table or table.seats_left < seats:
            return jsonify(error="Not enough seats"), 400

        reservation = Reservation(name=name, table_id=table_id, seats=seats)
        table.seats_left -= seats
        db.session.add(reservation)
        db.session.commit()

    return jsonify(message="Reservation successful", id=reservation.id)


# === 更新預約 ===
@app.put("/api/update/<int:reservation_id>")
def api_update(reservation_id):
    data = request.get_json()
    seats = data.get("seats")
    if seats is None:
        return jsonify(error="Missing seats"), 400

    with app.app_context():
        reservation = Reservation.query.get(reservation_id)
        if not reservation:
            return jsonify(error="Reservation not found"), 404

        table = TableInventory.query.get(reservation.table_id)
        if not table:
            return jsonify(error="Table not found"), 404

        diff = seats - reservation.seats
        if diff > table.seats_left:
            return jsonify(error="Not enough seats left"), 400

        table.seats_left -= diff
        reservation.seats = seats
        db.session.commit()

    return jsonify(message="Reservation updated successfully")


# === 刪除預約 ===
@app.delete("/api/delete/<int:reservation_id>")
def api_delete(reservation_id):
    with app.app_context():
        reservation = Reservation.query.get(reservation_id)
        if not reservation:
            return jsonify(error="Reservation not found"), 404

        table = TableInventory.query.get(reservation.table_id)
        if table:
            table.seats_left += reservation.seats

        db.session.delete(reservation)
        db.session.commit()

    return jsonify(message="Reservation deleted successfully")


# === 匯出所有預約為 CSV ===
@app.get("/api/export")
def api_export():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "name", "table_id", "seats", "created_at"])
    with app.app_context():
        for r in Reservation.query.all():
            writer.writerow([r.id, r.name, r.table_id, r.seats, r.created_at])
    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"reservations_{dt.date.today()}.csv",
    )


# === 前端頁面 ===
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/admin")
def admin_page():
    return render_template("admin.html")

@app.route("/login")
def login_page():
    return render_template("login.html")

@app.route("/reports")
def reports_page():
    return render_template("reports.html")


# === 主程式 ===
if __name__ == "__main__":
    try:
        init_seed()  # 🔹 自動建立資料表（第一次執行）
    except Exception as e:
        print(f"[⚠️ Warning] init_seed skipped due to error: {e}")
    app.run(host="0.0.0.0", port=8080)
