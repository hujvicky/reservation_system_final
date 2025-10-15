# ======================================
# Reservation System - AWS App Runner 版
# 加強版：防重登入ID + CRUD + /tmp SQLite fallback
# ======================================

from flask import Flask, request, jsonify, send_file, render_template
from flask_cors import CORS
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from models import db, TableInventory, Reservation, IdempotencyKey
from pathlib import Path
import os, shutil, io, csv, uuid

# ---------- 基本路徑 ----------
PROJECT_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PROJECT_DIR / "templates"
SEED_DB_PATH = PROJECT_DIR / "data" / "reservations.db"
TMP_DIR = Path("/tmp")
TMP_DB_PATH = TMP_DIR / "reservations.db"

app = Flask(__name__, template_folder=str(TEMPLATES_DIR), static_folder="static")
CORS(app)

# ---------- 資料庫設定 ----------
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    if not TMP_DB_PATH.exists() and SEED_DB_PATH.exists():
        try:
            shutil.copy(SEED_DB_PATH, TMP_DB_PATH)
            print("[INFO] Copied seed DB to /tmp/reservations.db")
        except Exception as e:
            print(f"[WARN] Failed to copy seed DB: {e}")
    DATABASE_URL = f"sqlite:///{TMP_DB_PATH}?check_same_thread=false"

app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(app)

MAX_PER_BOOKING = 3


# ---------- 初始化（建表 & 預設桌位） ----------
def init_seed():
    with app.app_context():
        db.create_all()  # 建出 models 內三表

        # login_id 唯一索引（避免併發重複）
        db.session.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_reservations_login
            ON reservations(login_id)
        """))
        db.session.commit()

        cnt = db.session.execute(text("SELECT COUNT(*) FROM tables")).scalar() or 0
        if cnt == 0:
            rows = [
                {"id": i, "name": f"Table {i}", "total": 10, "seats_left": 10}
                for i in range(1, 109)
            ]
            db.session.execute(
                text(
                    "INSERT OR IGNORE INTO tables (id, name, total, seats_left) "
                    "VALUES (:id, :name, :total, :seats_left)"
                ),
                rows,
            )
            db.session.commit()
            print("[INFO] Initialized tables (1..108).")

        names = db.session.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        ).scalars().all()
        print("[INFO] Tables present:", names)


# gunicorn / apprunner 啟動時先執行
try:
    init_seed()
except Exception as e:
    print(f"[WARN] init_seed skipped: {e}")


# ---------- 頁面 ----------
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


# ---------- API：座位狀態 ----------
@app.get("/api/status")
def api_status():
    rows = db.session.execute(
        text("SELECT id, seats_left FROM tables ORDER BY id")
    ).mappings().all()
    return jsonify({"tables": [{"table_id": r["id"]_]()
