from flask import Flask, request, jsonify, send_file, send_from_directory
from flask_cors import CORS
from sqlalchemy import text
from models import db, TableInventory, Reservation, IdempotencyKey
from pathlib import Path
import os, uuid, io, csv, datetime as dt

PROJECT_DIR = Path(__file__).resolve().parent
app = Flask(__name__, static_folder=str(PROJECT_DIR), static_url_path="")
CORS(app)

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    db_file = PROJECT_DIR / "reservations.db"
    DATABASE_URL = f"sqlite:///{db_file.as_posix()}"

app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(app)

MAX_PER_BOOKING = 3

def init_seed():
    with app.app_context():
        db.create_all()
        if TableInventory.query.count() == 0:
            for i in range(1, 109):
                t = TableInventory(id=i, name=f"Table {i}", total=10, seats_left=10)
                db.session.add(t)
            db.session.commit()

@app.get("/api/status")
def api_status_compat():
    rows = db.session.execute(text("SELECT id, seats_left FROM tables ORDER BY id ASC")).mappings().all()
    return jsonify({"tables": [{"table_id": r["id"], "seats_left": r["seats_left"]} for r in rows]})

@app.get("/")
def root():
    return app.send_static_file("index.html")

@app.get("/api/reservations/availability")
def availability():
    rows = db.session.execute(text("SELECT id, seats_left FROM tables ORDER BY id ASC")).mappings().all()
    confirmed = [{"table_id": r["id"]} for r in rows if r["seats_left"] <= 0]
    return jsonify({"holds": [], "confirmed": confirmed})

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
    data = db.session.execute(text(f"""
        SELECT id, table_id, seats_taken, employee_name, login_id, created_at
          FROM reservations
          {WHERE}
         ORDER BY created_at DESC
         LIMIT :lim OFFSET :off
    """), dict(params, lim=size, off=off)).mappings().all()
    total = db.session.execute(text(f"SELECT COUNT(*) AS c FROM reservations {WHERE}"), params).scalar()
    return jsonify({"data": [dict(r) for r in data], "total": int(total), "page": page, "page_size": size})

@app.get("/api/reservations.csv")
def export_csv():
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["id","table_id","seats_taken","employee_name","login_id","created_at"])
    rows = db.session.execute(text("""
        SELECT id, table_id, seats_taken, employee_name, login_id, created_at
          FROM reservations ORDER BY created_at DESC
    """ )).mappings().all()
    for r in rows:
        writer.writerow([r["id"], r["table_id"], r["seats_taken"], r["employee_name"], r["login_id"], r["created_at"]])
    mem = io.BytesIO(out.getvalue().encode("utf-8")); mem.seek(0)
    return send_file(mem, mimetype="text/csv", download_name="reservations.csv", as_attachment=True)

@app.post("/api/reserve")
def reserve():
    payload = request.get_json(force=True)
    table_id = int(payload["table_id"])
    seats = int(payload.get("seats_to_take", 1))
    employee_name = str(payload.get("employee_name","")).strip() or "Guest"
    login_id = str(payload.get("login_id","")).strip() or "guest"
    if seats < 1 or seats > MAX_PER_BOOKING:
        return jsonify(success=False, message=f"Invalid seat count (1-{MAX_PER_BOOKING})."), 400
    idem_key = request.headers.get("Idempotency-Key")
    if not idem_key:
        return jsonify(success=False, message="Missing Idempotency-Key header."), 400
    with db.session.begin():
        row = db.session.execute(text("SELECT result_reservation_id FROM idempotency_keys WHERE key=:k"), {"k": idem_key}).first()
        if row and row[0]:
            return jsonify(success=True, message="Already processed.", reservation_id=row[0]), 200
        elif row is None:
            db.session.execute(text("INSERT INTO idempotency_keys(key) VALUES (:k)"), {"k": idem_key})
        updated = db.session.execute(text("""
            UPDATE tables
               SET seats_left = seats_left - :n
             WHERE id = :tid AND seats_left >= :n
        """), {"n": seats, "tid": table_id})
        if updated.rowcount == 0:
            db.session.rollback()
            return jsonify(success=False, message="This table is full or seats are not enough now."), 409
        rid = str(uuid.uuid4())
        db.session.execute(text("""
            INSERT INTO reservations(id, table_id, seats_taken, employee_name, login_id)
            VALUES(:id, :tid, :n, :emp, :login)
        """), {"id": rid, "tid": table_id, "n": seats, "emp": employee_name, "login": login_id})
        db.session.execute(text("UPDATE idempotency_keys SET result_reservation_id=:rid WHERE key=:k"),
                           {"rid": rid, "k": idem_key})
    return jsonify(success=True, message="Reservation confirmed!", reservation_id=rid), 201

@app.post("/api/cancel")
def cancel():
    payload = request.get_json(force=True)
    rid = payload.get("reservation_id")
    if not rid:
        return jsonify(success=False, message="reservation_id required"), 400
    with db.session.begin():
        row = db.session.execute(text("SELECT table_id, seats_taken FROM reservations WHERE id=:id"), {"id": rid}).first()
        if not row:
            return jsonify(success=False, message="Reservation not found"), 404
        db.session.execute(text("DELETE FROM reservations WHERE id=:id"), {"id": rid})
        db.session.execute(text("UPDATE tables SET seats_left = seats_left + :n WHERE id=:tid"),
                           {"n": row[1], "tid": row[0]})
    return jsonify(success=True)

@app.post("/api/reduce")
def reduce_seats():
    payload = request.get_json(force=True)
    rid = payload.get("reservation_id")
    reduce_by = int(payload.get("reduce_by", 0))
    if not rid or reduce_by <= 0:
        return jsonify(success=False, message="Invalid input"), 400
    with db.session.begin():
        row = db.session.execute(text("SELECT table_id, seats_taken FROM reservations WHERE id=:id"), {"id": rid}).first()
        if not row:
            return jsonify(success=False, message="Reservation not found"), 404
        current = int(row[1])
        if reduce_by >= current:
            db.session.execute(text("DELETE FROM reservations WHERE id=:id"), {"id": rid})
            to_return = current
        else:
            db.session.execute(text("""
                UPDATE reservations
                   SET seats_taken = seats_taken - :reduce
                 WHERE id=:id
            """), {"reduce": reduce_by, "id": rid})
            to_return = reduce_by
        db.session.execute(text("""
            UPDATE tables
               SET seats_left = seats_left + :n
             WHERE id=:tid
        """), {"n": to_return, "tid": row[0]})
    return jsonify(success=True, message=f"Reduced {to_return} seat(s).")

if __name__ == "__main__":
    with app.app_context():
        init_seed()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
