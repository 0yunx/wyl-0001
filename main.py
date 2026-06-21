import asyncio
import json
import random
import time
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager

from models import (
    SensorData,
    AlertRule,
    EventCreate,
    init_db,
    get_db,
    load_rules,
    save_rules,
    clean_and_ingest,
)
from scheduler import create_scheduler, register_client, unregister_client

NUM_SENSORS = 10
sensor_tasks: List[asyncio.Task] = []


async def sensor_worker(sensor_id: str):
    base_temp = random.uniform(20.0, 30.0)
    base_humidity = random.uniform(40.0, 70.0)

    while True:
        try:
            temp = base_temp + random.uniform(-5.0, 10.0)
            humidity = base_humidity + random.uniform(-10.0, 15.0)

            temp = max(-40.0, min(85.0, temp))
            humidity = max(0.0, min(100.0, humidity))

            data = SensorData(
                sensor_id=sensor_id,
                temperature=round(temp, 2),
                humidity=round(humidity, 2),
                timestamp=time.time(),
            )

            result = clean_and_ingest(data)

        except Exception as e:
            print(f"[sensor {sensor_id}] error: {e}")

        await asyncio.sleep(1)


async def sse_client_stream(q: asyncio.Queue):
    try:
        while True:
            alert = await q.get()
            yield f"data: {json.dumps(alert, ensure_ascii=False)}\n\n"
    except asyncio.CancelledError:
        pass
    finally:
        await unregister_client(q)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    scheduler = create_scheduler()
    scheduler.start()
    app.state.scheduler = scheduler

    for i in range(NUM_SENSORS):
        sensor_id = f"sensor-{i+1:02d}"
        task = asyncio.create_task(sensor_worker(sensor_id))
        sensor_tasks.append(task)

    print(f"[startup] {NUM_SENSORS} sensor simulators started")
    print("[startup] aggregation scheduler started (every 30s)")

    yield

    scheduler.shutdown()
    for task in sensor_tasks:
        task.cancel()
    await asyncio.gather(*sensor_tasks, return_exceptions=True)
    sensor_tasks.clear()
    print("[shutdown] clean up done")


app = FastAPI(title="Edge IoT Thermo Gateway Simulator", lifespan=lifespan)


@app.post("/ingest", status_code=201)
async def ingest_data(data: SensorData):
    result = clean_and_ingest(data)
    return result


@app.get("/rules")
async def get_rules():
    rules = load_rules()
    return rules.model_dump()


@app.put("/rules")
async def update_rules(rule: AlertRule):
    save_rules(rule)
    return {"status": "ok", "message": "rules updated (hot-reload)"}


@app.get("/stream")
async def stream_alerts():
    q = await register_client()
    return StreamingResponse(
        sse_client_stream(q),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/alerts")
async def list_alerts(limit: int = 50):
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")
    with get_db() as conn:
        cursor = conn.execute(
            "SELECT * FROM alerts ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


@app.post("/events", status_code=201)
async def create_event(event: EventCreate):
    created_at = time.time()
    payload_json = json.dumps(event.payload, ensure_ascii=False)
    with get_db() as conn:
        cursor = conn.execute(
            """
            INSERT INTO events (sensor_id, event_type, payload, source, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (event.sensor_id, event.event_type, payload_json, event.source, created_at),
        )
        event_id = cursor.lastrowid
        return {
            "id": event_id,
            "sensor_id": event.sensor_id,
            "event_type": event.event_type,
            "payload": event.payload,
            "source": event.source,
            "created_at": created_at,
        }


@app.get("/events")
async def list_events(
    sensor_id: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    before: Optional[int] = Query(None),
):
    conditions = []
    params = []
    if sensor_id is not None:
        conditions.append("sensor_id = ?")
        params.append(sensor_id)
    if event_type is not None:
        conditions.append("event_type = ?")
        params.append(event_type)
    if source is not None:
        conditions.append("source = ?")
        params.append(source)
    if before is not None:
        conditions.append("id < ?")
        params.append(before)
    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)
    with get_db() as conn:
        cursor = conn.execute(
            f"SELECT * FROM events {where} ORDER BY id DESC LIMIT ?",
            (*params, limit),
        )
        rows = cursor.fetchall()
        results = []
        for row in rows:
            d = dict(row)
            d["payload"] = json.loads(d["payload"]) if d["payload"] else {}
            results.append(d)
        return results


@app.delete("/events/{event_id}")
async def delete_event(event_id: int):
    with get_db() as conn:
        cursor = conn.execute("SELECT id FROM events WHERE id = ?", (event_id,))
        row = cursor.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="event not found")
        conn.execute("DELETE FROM events WHERE id = ?", (event_id,))
        return {"status": "ok", "deleted": event_id}


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
