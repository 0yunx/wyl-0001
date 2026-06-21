import math
import sqlite3
import time
from datetime import datetime
from typing import List, Optional, Set, Tuple
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


class AnomalyRecord(BaseModel):
    id: Optional[int] = None
    sensor_id: str
    temperature: float
    humidity: float
    timestamp: float
    received_at: float
    mean_temp: Optional[float] = None
    std_temp: Optional[float] = None
    mean_humidity: Optional[float] = None
    std_humidity: Optional[float] = None


WINDOW_SIZE = 10
SIGMA_THRESHOLD = 3
MIN_STD_TEMP = 1.0
MIN_STD_HUMIDITY = 5.0
SCHEMA_VERSION = 3

DEFAULT_TEMP_HIGH = 32.0
DEFAULT_TEMP_LOW = 18.0
DEFAULT_HUMIDITY_HIGH = 75.0
DEFAULT_HUMIDITY_LOW = 35.0


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _ensure_schema_version_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            version INTEGER NOT NULL,
            applied_at REAL NOT NULL
        )
    """)
    cursor = conn.execute("SELECT COUNT(*) as cnt FROM schema_version WHERE id = 1")
    row = cursor.fetchone()
    if row["cnt"] == 0:
        conn.execute(
            "INSERT INTO schema_version (id, version, applied_at) VALUES (1, 0, ?)",
            (datetime.now().timestamp(),),
        )


def _get_schema_version(conn) -> int:
    cursor = conn.execute("SELECT version FROM schema_version WHERE id = 1")
    row = cursor.fetchone()
    return row["version"] if row else 0


def _set_schema_version(conn, version: int):
    conn.execute(
        "UPDATE schema_version SET version = ?, applied_at = ? WHERE id = 1",
        (version, datetime.now().timestamp()),
    )


def _migrate_v0_to_v1(conn):
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
    conn.execute(
        "INSERT OR IGNORE INTO rules (id, temp_high, temp_low, humidity_high, humidity_low, updated_at) "
        "VALUES (1, ?, ?, ?, ?, ?)",
        (
            DEFAULT_TEMP_HIGH,
            DEFAULT_TEMP_LOW,
            DEFAULT_HUMIDITY_HIGH,
            DEFAULT_HUMIDITY_LOW,
            datetime.now().timestamp(),
        ),
    )
    conn.execute(
        "INSERT OR IGNORE INTO aggregation_checkpoints (id, last_window_end) VALUES (1, ?)",
        (0.0,),
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_readings_ts ON sensor_readings(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_readings_sensor ON sensor_readings(sensor_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_sensor ON alerts(sensor_id)")


def _migrate_v1_to_v2(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS anomalies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id TEXT NOT NULL,
            temperature REAL NOT NULL,
            humidity REAL NOT NULL,
            timestamp REAL NOT NULL,
            received_at REAL NOT NULL,
            mean_temp REAL,
            std_temp REAL,
            mean_humidity REAL,
            std_humidity REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_anomalies_sensor ON anomalies(sensor_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_anomalies_received_at ON anomalies(received_at)")


def _migrate_v2_to_v3(conn):
    """
    Schema v3:
    - Tune default thresholds based on simulated data distribution
    - Reset checkpoint to 0.0 so no historical readings are skipped
    - Clean alert_state of stale entries (threshold mismatch with rules, dead sensors)
    - Add rule_version column to rules table to detect hot-reload conflicts
    """
    try:
        conn.execute("ALTER TABLE rules ADD COLUMN rule_version INTEGER NOT NULL DEFAULT 1")
    except sqlite3.OperationalError:
        pass

    conn.execute(
        """
        UPDATE rules
        SET temp_high = ?,
            temp_low = ?,
            humidity_high = ?,
            humidity_low = ?,
            updated_at = ?,
            rule_version = rule_version + 1
        WHERE id = 1
          AND (
              temp_high IS NULL OR temp_high != ?
              OR temp_low IS NULL OR temp_low != ?
              OR humidity_high IS NULL OR humidity_high != ?
              OR humidity_low IS NULL OR humidity_low != ?
          )
        """,
        (
            DEFAULT_TEMP_HIGH,
            DEFAULT_TEMP_LOW,
            DEFAULT_HUMIDITY_HIGH,
            DEFAULT_HUMIDITY_LOW,
            datetime.now().timestamp(),
            DEFAULT_TEMP_HIGH,
            DEFAULT_TEMP_LOW,
            DEFAULT_HUMIDITY_HIGH,
            DEFAULT_HUMIDITY_LOW,
        ),
    )

    conn.execute(
        "UPDATE aggregation_checkpoints SET last_window_end = 0.0 WHERE id = 1",
    )

    cursor = conn.execute(
        "SELECT temp_high, temp_low, humidity_high, humidity_low FROM rules WHERE id = 1"
    )
    rule = cursor.fetchone()
    if rule is not None:
        stale_keys = []
        cursor2 = conn.execute(
            "SELECT sensor_id, alert_type, last_threshold FROM alert_state"
        )
        for row in cursor2.fetchall():
            expected = None
            at = row["alert_type"]
            if at == "temp_high":
                expected = rule["temp_high"]
            elif at == "temp_low":
                expected = rule["temp_low"]
            elif at == "humidity_high":
                expected = rule["humidity_high"]
            elif at == "humidity_low":
                expected = rule["humidity_low"]
            if expected is None or abs(row["last_threshold"] - expected) > 1e-9:
                stale_keys.append((row["sensor_id"], at))
        for sid, at in stale_keys:
            conn.execute(
                "DELETE FROM alert_state WHERE sensor_id = ? AND alert_type = ?",
                (sid, at),
            )


_MIGRATIONS = [
    _migrate_v0_to_v1,
    _migrate_v1_to_v2,
    _migrate_v2_to_v3,
]


def init_db():
    with get_db() as conn:
        _ensure_schema_version_table(conn)
        current = _get_schema_version(conn)
        target = SCHEMA_VERSION
        if current >= target:
            return
        for version in range(current, target):
            migration = _MIGRATIONS[version]
            migration(conn)
            _set_schema_version(conn, version + 1)


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
            """
            UPDATE rules
            SET temp_high = ?,
                temp_low = ?,
                humidity_high = ?,
                humidity_low = ?,
                updated_at = ?,
                rule_version = COALESCE(rule_version, 1) + 1
            WHERE id = 1
            """,
            (
                rule.temp_high,
                rule.temp_low,
                rule.humidity_high,
                rule.humidity_low,
                datetime.now().timestamp(),
            ),
        )
        stale_pairs = [
            ("temp_high", rule.temp_high),
            ("temp_low", rule.temp_low),
            ("humidity_high", rule.humidity_high),
            ("humidity_low", rule.humidity_low),
        ]
        for alert_type, expected in stale_pairs:
            if expected is None:
                conn.execute(
                    "DELETE FROM alert_state WHERE alert_type = ?",
                    (alert_type,),
                )
            else:
                conn.execute(
                    """
                    DELETE FROM alert_state
                    WHERE alert_type = ?
                      AND ABS(last_threshold - ?) > 1e-9
                    """,
                    (alert_type, expected),
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


def _get_recent_readings(conn, sensor_id: str, limit: int) -> List[sqlite3.Row]:
    cursor = conn.execute(
        """
        SELECT temperature, humidity, timestamp
        FROM sensor_readings
        WHERE sensor_id = ?
        ORDER BY timestamp DESC, id DESC
        LIMIT ?
        """,
        (sensor_id, limit),
    )
    return cursor.fetchall()


def _mean_std(values: List[float]) -> Tuple[Optional[float], Optional[float]]:
    n = len(values)
    if n < 2:
        return None, None
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / (n - 1)
    std = math.sqrt(variance)
    return mean, std


def clean_and_ingest(data: SensorData) -> dict:
    """
    Data cleaning pipeline: sliding window outlier detection.
    If the new reading deviates more than SIGMA_THRESHOLD standard deviations
    from the mean of the last WINDOW_SIZE readings, it goes to anomalies table
    instead of sensor_readings.
    Returns a dict with status info.
    """
    ts = data.timestamp if data.timestamp is not None else time.time()
    received_at = time.time()

    with get_db() as conn:
        recent = _get_recent_readings(conn, data.sensor_id, WINDOW_SIZE)

        if len(recent) >= 2:
            temps = [row["temperature"] for row in recent]
            hums = [row["humidity"] for row in recent]

            mean_temp, std_temp = _mean_std(temps)
            mean_hum, std_hum = _mean_std(hums)

            eff_std_temp = max(std_temp, MIN_STD_TEMP) if std_temp is not None else None
            eff_std_hum = max(std_hum, MIN_STD_HUMIDITY) if std_hum is not None else None

            temp_outlier = (
                mean_temp is not None
                and eff_std_temp is not None
                and abs(data.temperature - mean_temp) > SIGMA_THRESHOLD * eff_std_temp
            )
            humidity_outlier = (
                mean_hum is not None
                and eff_std_hum is not None
                and abs(data.humidity - mean_hum) > SIGMA_THRESHOLD * eff_std_hum
            )

            if temp_outlier or humidity_outlier:
                conn.execute(
                    """
                    INSERT INTO anomalies
                        (sensor_id, temperature, humidity, timestamp, received_at,
                         mean_temp, std_temp, mean_humidity, std_humidity)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        data.sensor_id,
                        data.temperature,
                        data.humidity,
                        ts,
                        received_at,
                        mean_temp,
                        std_temp,
                        mean_hum,
                        std_hum,
                    ),
                )
                return {
                    "status": "anomaly",
                    "received_at": received_at,
                    "reason": "outlier_detected",
                    "mean_temp": mean_temp,
                    "std_temp": std_temp,
                    "mean_humidity": mean_hum,
                    "std_humidity": std_hum,
                }

        conn.execute(
            "INSERT INTO sensor_readings (sensor_id, temperature, humidity, timestamp) VALUES (?, ?, ?, ?)",
            (data.sensor_id, data.temperature, data.humidity, ts),
        )
        return {
            "status": "ok",
            "received_at": received_at,
        }
