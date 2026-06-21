import asyncio
import json
import random
import time
from datetime import datetime
from typing import List

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager

from models import (
    SensorData,
    AlertRule,
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


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
