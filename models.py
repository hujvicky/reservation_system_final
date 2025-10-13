from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import CheckConstraint, func
from sqlalchemy.dialects.postgresql import UUID
import uuid

db = SQLAlchemy()

class TableInventory(db.Model):
    __tablename__ = "tables"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), nullable=True)
    total = db.Column(db.Integer, nullable=False, default=10)
    seats_left = db.Column(db.Integer, nullable=False, default=10)
    __table_args__ = (
        CheckConstraint('seats_left >= 0', name='ck_seats_non_negative'),
    )

class Reservation(db.Model):
    __tablename__ = "reservations"
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    table_id = db.Column(db.Integer, db.ForeignKey("tables.id"), nullable=False, index=True)
    seats_taken = db.Column(db.Integer, nullable=False, default=1)
    employee_name = db.Column(db.String(128), nullable=False)
    login_id = db.Column(db.String(128), nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), server_default=func.now())

class IdempotencyKey(db.Model):
    __tablename__ = "idempotency_keys"
    key = db.Column(db.String(64), primary_key=True)
    result_reservation_id = db.Column(db.String(36), nullable=True)
    created_at = db.Column(db.DateTime(timezone=True), server_default=func.now())
