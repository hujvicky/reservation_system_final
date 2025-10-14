# ======================================
# Reservation System - AWS App Runner 版
# 完整功能：CRUD + 防重 + /tmp SQLite fallback + 前端相容路由
# ======================================

from flask import Flask, request, jsonify, send_file, render_template
from flask_cors import CORS
from sqlalchemy import text
from models import db  # models.py 已定義 tables / reservations / idempotency_keys
from pathlib import Path
import os, shutil, io, csv, datetime as dt, uuid

# ---------- 基本路徑 ----------
PROJECT_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PROJECT_DIR / "templates"
SEED_DB_PATH = PROJECT_DIR / "data" / "reservations.db"   # 若有，當作種子
TMP_DIR = Path("/tmp")
TMP_DB_PATH = TMP_DIR / "reservations.db"

app = Flask(__name__, template_folder=str(TEMPLATES_DIR), static_folder="static")
CORS(app)

# ---------- 資料庫設定（App Runner 可寫在 /tmp） ----------
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    if not TMP_DB_PATH.exists() and SEED_DB_PATH.exists():
        try:
            shutil.copy(SEED_DB_PATH, TMP_DB_PATH)
            print("[INFO] Copied seed DB to /tmp/reservations.db")
        except Exception as e:
            print(f"[WARN] Failed to copy seed DB: {e}")
    DATABASE_URL = f"sqlite:///{TMP_DB_PATH}"

app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(app)

MAX_PER_BOOKING = 3


# ---------- 初始化（建表 & 預設桌位） ----------
def init_seed():
    with app.app_context():
        db.create_all()
        try:
            existing = db.session.execute(text("SELECT COUNT(*) FROM tables")).scalar()
        except Exception:
            existing = 0
        if not existing:
            with db.session.begin():
                for i in range(1, 109):
                    db.session.execute(
                        text(
                            "INSERT INTO tables (id, name, total, seats_left) "
                            "VALUES (:id, :name, :total, :left)"
                        ),
                        {"id": i, "name": f"Table {i}", "total": 10, "left": 10},
                    )
            print("[INFO] Initialized tables (1..108).")


# ---------- 頁面（保持與前端連結相容） ----------
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


# ---------- API：座位狀態（index.html 會每 3 秒輪詢） ----------
@app.get("/api/status")
def api_status():
    rows = db.session.execute(
        text("SELECT id, seats_left FROM tables ORDER BY id")
    ).mappings().all()
    return jsonify({"tables": [{"table_id": r["id"], "seats_left": r["seats_left"]} for r in rows]})


# ---------- API：桌位清單（目前前端沒直接用，但保留） ----------
@app.get("/api/tables")
def api_tables():
    rows = db.session.execute(
        text("SELECT id, name, total, seats_left FROM tables ORDER BY id")
    ).mappings().all()
    return jsonify([dict(r) for r in rows])


# ---------- API：可用性（回傳滿座桌，index 有用到） ----------
@app.get("/api/reservations/availability")
def api_availability():
    rows = db.session.execute(
        text("SELECT id, seats_left FROM tables ORDER BY id")
    ).mappings().all()
    confirmed = [{"table_id": r["id"]} for r in rows if r["seats_left"] <= 0]
    return jsonify({"holds": [], "confirmed": confirmed})


# ---------- 工具：把 raw row 轉為可被前端 new Date() 的 ISO ----------
def _row_to_reservation_dict(r):
    created = r["created_at"]
    if hasattr(created, "isoformat"):
        created_iso = created.isoformat()
    else:
        # 有些方言會回傳字串，確保是 str
        created_iso = str(created)
    return {
        "id": r["id"],
        "table_id": r["table_id"],
        "seats_taken": r["seats_taken"],
        "employee_name": r["employee_name"],
        "login_id": r["login_id"],
        "created_at": created_iso,
    }


# ---------- API：查詢預約（admin/reports 兩頁都用） ----------
@app.get("/api/reservations")
def list_reservations():
    table_id = request.args.get("table_id", type=int)
    page = max(1, request.args.get("page", default=1, type=int))
    size = min(100, max(1, request.args.get("page_size", default=50, type=int)))
    off = (page - 1) * size

    where, params = [], {}
    if table_id:
        where.append("table_id = :tid")
        params["tid"] = table_id
    WHERE = ("WHERE " + " AND ".join(where)) if where else ""

    data = db.session.execute(
        text(f"""
            SELECT id, table_id, seats_taken, employee_name, login_id, created_at
            FROM reservations
            {WHERE}
            ORDER BY created_at DESC
            LIMIT :lim OFFSET :off
        """),
        dict(params, lim=size, off=off),
    ).mappings().all()

    total = db.session.execute(
        text(f"SELECT COUNT(*) FROM reservations {WHERE}"), params
    ).scalar()

    return jsonify({
        "data": [_row_to_reservation_dict(r) for r in data],
        "total": int(total),
        "page": page,
        "page_size": size
    })


# ---------- API：建立預約（index.html 的 Confirm） ----------
@app.post("/api/reserve")
def reserve():
    payload = request.get_json(force=True)

    # 前端傳入欄位（index.html）：table_id, seats_to_take, employee_name, login_id
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
    login_id = str(payload.get("login_id", "")).strip() or "guest"

    if seats < 1 or seats > MAX_PER_BOOKING:
        return jsonify(success=False, message=f"Invalid seat count (1-{MAX_PER_BOOKING})"), 400

    idem_key = request.headers.get("Idempotency-Key")
    if not idem_key:
        # 前端目前會傳，但為避免手動測試遺漏，這裡自動給一個
        idem_key = str(uuid.uuid4())

    with db.session.begin():
        # 防止重複提交（相同 Idempotency-Key）
        row = db.session.execute(
            text("SELECT result_reservation_id FROM idempotency_keys WHERE key=:k"),
            {"k": idem_key},
        ).first()
        if row and row[0]:
            return jsonify(success=True, message="Already processed", reservation_id=row[0]), 200
        elif row is None:
            db.session.execute(
                text("INSERT INTO idempotency_keys(key) VALUES (:k)"),
                {"k": idem_key},
            )

        # 扣座位（確保不會變負數）
        updated = db.session.execute(
            text("""
                UPDATE tables
                SET seats_left = seats_left - :n
                WHERE id = :tid AND seats_left >= :n
            """),
            {"n": seats, "tid": table_id},
        )
        if updated.rowcount == 0:
            db.session.rollback()
            return jsonify(success=False, message="This table is full or seats are not enough now."), 409

        # 建立預約
        rid = str(uuid.uuid4())
        db.session.execute(
            text("""
                INSERT INTO reservations(id, table_id, seats_taken, employee_name, login_id)
                VALUES(:id, :tid, :n, :emp, :login)
            """),
            {"id": rid, "tid": table_id, "n": seats, "emp": employee_name, "login": login_id},
        )
        db.session.execute(
            text("UPDATE idempotency_keys SET result_reservation_id=:rid WHERE key=:k"),
            {"rid": rid, "k": idem_key},
        )

    return jsonify(success=True, message="Reservation confirmed!", reservation_id=rid), 201


# ---------- API：取消預約（admin.html 的 Cancel） ----------
@app.post("/api/cancel")
def cancel():
    payload = request.get_json(force=True)
    rid = payload.get("reservation_id")
    if not rid:
        return jsonify(success=False, message="reservation_id required"), 400

    with db.session.begin():
        row = db.session.execute(
            text("SELECT table_id, seats_taken FROM reservations WHERE id=:id"),
            {"id": rid},
        ).first()
        if not row:
            return jsonify(success=False, message="Reservation not found"), 404

        db.session.execute(text("DELETE FROM reservations WHERE id=:id"), {"id": rid})
        db.session.execute(
            text("UPDATE tables SET seats_left = seats_left + :n WHERE id=:tid"),
            {"n": row[1], "tid": row[0]},
        )
    return jsonify(success=True)


# ---------- API：減少座位（admin.html 的 Reduce） ----------
@app.post("/api/reduce")
def reduce_seats():
    payload = request.get_json(force=True)
    rid = payload.get("reservation_id")
    reduce_by = payload.get("reduce_by", 0)
    try:
        reduce_by = int(reduce_by)
    except Exception:
        reduce_by = 0

    if not rid or reduce_by <= 0:
        return jsonify(success=False, message="Invalid input"), 400

    with db.session.begin():
        row = db.session.execute(
            text("SELECT table_id, seats_taken FROM reservations WHERE id=:id"),
            {"id": rid},
        ).first()
        if not row:
            return jsonify(success=False, message="Reservation not found"), 404

        current = int(row[1])
        if reduce_by >= current:
            # 直接刪除並全數回補
            db.session.execute(text("DELETE FROM reservations WHERE id=:id"), {"id": rid})
            to_return = current
        else:
            # 更新預約數量
            db.session.execute(
                text("UPDATE reservations SET seats_taken = seats_taken - :reduce WHERE id=:id"),
                {"reduce": reduce_by, "id": rid},
            )
            to_return = reduce_by

        # 回補座位
        db.session.execute(
            text("UPDATE tables SET seats_left = seats_left + :n WHERE id=:tid"),
            {"n": to_return, "tid": row[0]},
        )

    return jsonify(success=True, message=f"Reduced {to_return} seat(s).")


# ---------- 匯出 CSV（admin/reports 會用到） ----------
@app.get("/api/reservations.csv")
def export_csv():
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["id", "table_id", "seats_taken", "employee_name", "login_id", "created_at"])
    rows = db.session.execute(
        text("""
            SELECT id, table_id, seats_taken, employee_name, login_id, created_at
            FROM reservations
            ORDER BY created_at DESC
        """)
    ).mappings().all()
    for r in rows:
        created = r["created_at"]
        created_iso = created.isoformat() if hasattr(created, "isoformat") else str(created)
        writer.writerow([r["id"], r["table_id"], r["seats_taken"], r["employee_name"], r["login_id"], created_iso])

    mem = io.BytesIO(out.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name="reservations.csv")


# ---------- 啟動 ----------
if __name__ == "__main__":
    try:
        init_seed()
    except Exception as e:
        print(f"[WARN] init_seed skipped: {e}")
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
