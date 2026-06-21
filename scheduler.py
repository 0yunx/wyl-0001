import asyncio
from datetime import datetime
from typing import List

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from models import get_db, load_rules, AlertRecord


alert_queue: asyncio.Queue = asyncio.Queue()


def _aggregate_and_check(window_seconds: int = 30) -> List[AlertRecord]:
    now = datetime.now().timestamp()
    window_start = now - window_seconds

    rules = load_rules()
    alerts: List[AlertRecord] = []

    with get_db() as conn:
        cursor = conn.execute(
            """
            SELECT sensor_id,
                   AVG(temperature) as avg_temp,
                   MAX(temperature) as max_temp,
                   MIN(temperature) as min_temp,
                   AVG(humidity) as avg_humidity,
                   MAX(humidity) as max_humidity,
                   MIN(humidity) as min_humidity
            FROM sensor_readings
            WHERE timestamp >= ?
            GROUP BY sensor_id
            """,
            (window_start,),
        )
        rows = cursor.fetchall()

        for row in rows:
            sensor_id = row["sensor_id"]

            if rules.temp_high is not None and row["max_temp"] > rules.temp_high:
                alerts.append(
                    AlertRecord(
                        sensor_id=sensor_id,
                        alert_type="temp_high",
                        value=row["max_temp"],
                        threshold=rules.temp_high,
                        window_start=window_start,
                        window_end=now,
                        created_at=now,
                    )
                )

            if rules.temp_low is not None and row["min_temp"] < rules.temp_low:
                alerts.append(
                    AlertRecord(
                        sensor_id=sensor_id,
                        alert_type="temp_low",
                        value=row["min_temp"],
                        threshold=rules.temp_low,
                        window_start=window_start,
                        window_end=now,
                        created_at=now,
                    )
                )

            if rules.humidity_high is not None and row["max_humidity"] > rules.humidity_high:
                alerts.append(
                    AlertRecord(
                        sensor_id=sensor_id,
                        alert_type="humidity_high",
                        value=row["max_humidity"],
                        threshold=rules.humidity_high,
                        window_start=window_start,
                        window_end=now,
                        created_at=now,
                    )
                )

            if rules.humidity_low is not None and row["min_humidity"] < rules.humidity_low:
                alerts.append(
                    AlertRecord(
                        sensor_id=sensor_id,
                        alert_type="humidity_low",
                        value=row["min_humidity"],
                        threshold=rules.humidity_low,
                        window_start=window_start,
                        window_end=now,
                        created_at=now,
                    )
                )

        for alert in alerts:
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

    return alerts


async def _aggregation_job():
    alerts = _aggregate_and_check()
    for alert in alerts:
        await alert_queue.put(alert.model_dump())


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(
        _aggregation_job,
        trigger=IntervalTrigger(seconds=30),
        id="aggregation_job",
        replace_existing=True,
    )
    return scheduler
