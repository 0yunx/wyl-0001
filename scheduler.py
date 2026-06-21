import asyncio
import math
import time
from typing import List, Set, Tuple

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from models import (
    get_db,
    load_rules,
    load_alert_state,
    upsert_alert_state,
    remove_alert_state,
    load_checkpoint,
    save_checkpoint,
    AlertRecord,
)

WINDOW_SECONDS = 30
STALE_SENSOR_THRESHOLD_SECONDS = 300

_client_queues: List[asyncio.Queue] = []
_lock = asyncio.Lock()


async def register_client() -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue()
    async with _lock:
        _client_queues.append(q)
    return q


async def unregister_client(q: asyncio.Queue):
    async with _lock:
        if q in _client_queues:
            _client_queues.remove(q)


async def _broadcast(alert_dict: dict):
    async with _lock:
        dead = []
        for q in _client_queues:
            try:
                q.put_nowait(alert_dict)
            except asyncio.QueueFull:
                dead.append(q)
        for q in dead:
            _client_queues.remove(q)


def _check_threshold(
    rules,
    max_temp: float,
    min_temp: float,
    max_humidity: float,
    min_humidity: float,
) -> List[Tuple[str, float, float]]:
    violations: List[Tuple[str, float, float]] = []
    if rules.temp_high is not None and max_temp > rules.temp_high:
        violations.append(("temp_high", max_temp, rules.temp_high))
    if rules.temp_low is not None and min_temp < rules.temp_low:
        violations.append(("temp_low", min_temp, rules.temp_low))
    if rules.humidity_high is not None and max_humidity > rules.humidity_high:
        violations.append(("humidity_high", max_humidity, rules.humidity_high))
    if rules.humidity_low is not None and min_humidity < rules.humidity_low:
        violations.append(("humidity_low", min_humidity, rules.humidity_low))
    return violations


def _aggregate_window(window_start: float, window_end: float) -> List[AlertRecord]:
    rules = load_rules()
    active_keys: Set[Tuple[str, str]] = load_alert_state()

    new_alerts: List[AlertRecord] = []

    with get_db() as conn:
        cursor = conn.execute(
            """
            SELECT sensor_id,
                   MAX(temperature) as max_temp,
                   MIN(temperature) as min_temp,
                   MAX(humidity) as max_humidity,
                   MIN(humidity) as min_humidity
            FROM sensor_readings
            WHERE timestamp >= ? AND timestamp < ?
            GROUP BY sensor_id
            """,
            (window_start, window_end),
        )
        rows = cursor.fetchall()

        current_triggered: Set[Tuple[str, str]] = set()
        sensors_in_window: Set[str] = set()

        for row in rows:
            sensor_id = row["sensor_id"]
            sensors_in_window.add(sensor_id)
            violations = _check_threshold(
                rules,
                row["max_temp"],
                row["min_temp"],
                row["max_humidity"],
                row["min_humidity"],
            )

            for alert_type, value, threshold in violations:
                key = (sensor_id, alert_type)
                current_triggered.add(key)

                if key not in active_keys:
                    alert = AlertRecord(
                        sensor_id=sensor_id,
                        alert_type=alert_type,
                        value=value,
                        threshold=threshold,
                        window_start=window_start,
                        window_end=window_end,
                        created_at=window_end,
                    )
                    new_alerts.append(alert)

                    conn.execute(
                        """
                        INSERT INTO alerts (sensor_id, alert_type, value, threshold, window_start, window_end, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            alert.sensor_id,
                            alert.alert_type,
                            alert.value,
                            alert.threshold,
                            alert.window_start,
                            alert.window_end,
                            alert.created_at,
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO alert_state (sensor_id, alert_type, triggered_at, last_value, last_threshold)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(sensor_id, alert_type) DO UPDATE SET
                            triggered_at = excluded.triggered_at,
                            last_value = excluded.last_value,
                            last_threshold = excluded.last_threshold
                        """,
                        (sensor_id, alert_type, window_end, value, threshold),
                    )

        cleared = set()
        for key in active_keys - current_triggered:
            cleared.add(key)

        if sensors_in_window:
            cursor2 = conn.execute(
                "SELECT MAX(timestamp) as overall_max FROM sensor_readings WHERE timestamp < ?",
                (window_end,),
            )
            row_max = cursor2.fetchone()
            overall_max_ts = row_max["overall_max"] if row_max else None
        else:
            overall_max_ts = None

        if overall_max_ts is not None:
            staleness_cutoff = overall_max_ts - STALE_SENSOR_THRESHOLD_SECONDS
            cursor3 = conn.execute(
                """
                SELECT DISTINCT a.sensor_id, a.alert_type
                FROM alert_state a
                LEFT JOIN (
                    SELECT sensor_id, MAX(timestamp) as last_ts
                    FROM sensor_readings
                    WHERE timestamp < ?
                    GROUP BY sensor_id
                ) s ON a.sensor_id = s.sensor_id
                WHERE s.sensor_id IS NULL OR s.last_ts < ?
                """,
                (window_end, staleness_cutoff),
            )
            for row in cursor3.fetchall():
                key = (row["sensor_id"], row["alert_type"])
                cleared.add(key)

        for key in cleared:
            sensor_id, alert_type = key
            conn.execute(
                "DELETE FROM alert_state WHERE sensor_id = ? AND alert_type = ?",
                (sensor_id, alert_type),
            )

    return new_alerts


def _catch_up_aggregate() -> List[AlertRecord]:
    checkpoint = load_checkpoint()

    with get_db() as conn:
        cursor = conn.execute(
            "SELECT MAX(timestamp) as max_ts FROM sensor_readings"
        )
        row = cursor.fetchone()
        max_ts = row["max_ts"] if row and row["max_ts"] is not None else None

    if max_ts is None:
        return []

    all_new_alerts: List[AlertRecord] = []

    window_start = math.floor(checkpoint / WINDOW_SECONDS) * WINDOW_SECONDS

    while window_start + WINDOW_SECONDS <= max_ts:
        window_end = window_start + WINDOW_SECONDS
        alerts = _aggregate_window(window_start, window_end)
        all_new_alerts.extend(alerts)
        save_checkpoint(window_end)
        window_start = window_end

    return all_new_alerts


async def _aggregation_job():
    alerts = _catch_up_aggregate()
    for alert in alerts:
        await _broadcast(alert.model_dump())


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(
        _aggregation_job,
        trigger=IntervalTrigger(seconds=WINDOW_SECONDS),
        id="aggregation_job",
        replace_existing=True,
    )
    return scheduler
