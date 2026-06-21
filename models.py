import sqlite3
from datetime import datetime
from typing import Optional, Set, Tuple
from contextlib import contextmanager

from pydantic import BaseModel, Field, field_validator

DB_PATH = "gateway.db"


class SensorData(BaseModel):
    sensor_id: str = Field(min_length=1, max_length=64)
    temperature: float = Field(ge=-40.0, le=85.0)
    humidity: float = Field(ge=0.0, le=100.0)
    timestamp: Optional[float] = None

    @field_validator("sensor_id")
    @classmethod
    def sensor_id_must_be_valid(cls, v: str) -> str:
        if not v.replace("-", "").replace("_", "").isalnum():
            raise ValueError("sensor_id must contain only alphanumeric chars, hyphens, underscores")
        return v


class AlertRule(BaseModel):
    temp_high: Optional[float] = None
    temp_low: Optional[float] = None
    humidity_high: Optional[float] = None
    humidity_low: Optional[float] = None

    @field_validator("temp_high", "temp_low")
    @classmethod
    def temp_range_check(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and (v < -40.0 or v > 85.0):
            raise ValueError("temperature threshold out of range [-40, 85]")
        return v

    @field_validator("humidity_high", "humidity_low")
    @classmethod
    def humidity_range_check(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and (v < 0.0 or v > 100.0):
            raise ValueError("humidity threshold out of range [0, 100]")
        return v


class AlertRecord(BaseModel):
    id: Optional[int] = None
    sensor_id: str
    alert_type: str
    value: float
    threshold: float
    window_start: float
    window_end: float
    created_at: Optional[float] = None


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT NOT NULL,
                temperature REAL NOT NULL,
                humidity REAL NOT NULL,
                timestamp REAL NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                value REAL NOT NULL,
                threshold REAL NOT NULL,
                window_start REAL NOT NULL,
                window_end REAL NOT NULL,
                created_at REAL NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rules (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                temp_high REAL,
                temp_low REAL,
                humidity_high REAL,
                humidity_low REAL,
                updated_at REAL NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alert_state (
                sensor_id TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                triggered_at REAL NOT NULL,
                last_value REAL NOT NULL,
                last_threshold REAL NOT NULL,
                PRIMARY KEY (sensor_id, alert_type)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS aggregation_checkpoints (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_window_end REAL NOT NULL
            )
        """)
        cursor = conn.execute("SELECT COUNT(*) as cnt FROM rules WHERE id = 1")
        row = cursor.fetchone()
        if row["cnt"] == 0:
            conn.execute(
                "INSERT INTO rules (id, temp_high, temp_low, humidity_high, humidity_low, updated_at) "
                "VALUES (1, ?, ?, ?, ?, ?)",
                (35.0, 0.0, 90.0, 10.0, datetime.now().timestamp()),
            )

        cursor = conn.execute("SELECT COUNT(*) as cnt FROM aggregation_checkpoints WHERE id = 1")
        row = cursor.fetchone()
        if row["cnt"] == 0:
            conn.execute(
                "INSERT INTO aggregation_checkpoints (id, last_window_end) VALUES (1, ?)",
                (datetime.now().timestamp(),),
            )

        conn.execute("CREATE INDEX IF NOT EXISTS idx_readings_ts ON sensor_readings(timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_readings_sensor ON sensor_readings(sensor_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_sensor ON alerts(sensor_id)")


def load_rules() -> AlertRule:
    with get_db() as conn:
        cursor = conn.execute("SELECT * FROM rules WHERE id = 1")
        row = cursor.fetchone()
        return AlertRule(
            temp_high=row["temp_high"],
            temp_low=row["temp_low"],
            humidity_high=row["humidity_high"],
            humidity_low=row["humidity_low"],
        )


def save_rules(rule: AlertRule):
    with get_db() as conn:
        conn.execute(
            "UPDATE rules SET temp_high=?, temp_low=?, humidity_high=?, humidity_low=?, updated_at=? WHERE id = 1",
            (
                rule.temp_high,
                rule.temp_low,
                rule.humidity_high,
                rule.humidity_low,
                datetime.now().timestamp(),
            ),
        )


def load_alert_state() -> Set[Tuple[str, str]]:
    with get_db() as conn:
        cursor = conn.execute("SELECT sensor_id, alert_type FROM alert_state")
        return {(row["sensor_id"], row["alert_type"]) for row in cursor.fetchall()}


def upsert_alert_state(sensor_id: str, alert_type: str, value: float, threshold: float, triggered_at: float):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO alert_state (sensor_id, alert_type, triggered_at, last_value, last_threshold)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(sensor_id, alert_type) DO UPDATE SET
                triggered_at = excluded.triggered_at,
                last_value = excluded.last_value,
                last_threshold = excluded.last_threshold
            """,
            (sensor_id, alert_type, triggered_at, value, threshold),
        )


def remove_alert_state(sensor_id: str, alert_type: str):
    with get_db() as conn:
        conn.execute(
            "DELETE FROM alert_state WHERE sensor_id = ? AND alert_type = ?",
            (sensor_id, alert_type),
        )


def load_checkpoint() -> float:
    with get_db() as conn:
        cursor = conn.execute("SELECT last_window_end FROM aggregation_checkpoints WHERE id = 1")
        row = cursor.fetchone()
        return row["last_window_end"] if row else datetime.now().timestamp()


def save_checkpoint(window_end: float):
    with get_db() as conn:
        conn.execute(
            "UPDATE aggregation_checkpoints SET last_window_end = ? WHERE id = 1",
            (window_end,),
        )
