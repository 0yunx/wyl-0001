# Edge IoT 温控网关模拟器

基于 FastAPI 的边缘 IoT 温控网关模拟器，支持温湿度数据采集、阈值告警去重、SSE 广播推送、断点续跑补齐。

## 功能特性

- **10 个协程模拟传感器**：每秒推送温湿度数据
- **/ingest 数据接入**：FastAPI + Pydantic 校验，写入 SQLite
- **/rules 阈值配置**：支持热更新，无需重启服务
- **APScheduler 定长窗口聚合**：每 30 秒一个窗口，检测超阈值
- **告警去重**：同一 (sensor_id, alert_type) 只在状态转换时写入，不会每周期重复刷同一条
- **/stream SSE 广播推送**：每个客户端独立 Queue，所有客户端收到相同告警
- **断点续跑补齐**：checkpoint 持久化，重启后从上次断点补齐所有遗漏窗口

## 项目结构

```
.
├── main.py        # FastAPI 主程序，传感器模拟，API 端点
├── models.py      # Pydantic 模型，SQLite 数据库操作，alert_state / checkpoint 管理
├── scheduler.py   # APScheduler 聚合任务，告警去重，广播分发，checkpoint 补齐
└── gateway.db     # SQLite 数据库文件（运行时自动创建）
```

## 安装依赖

```bash
pip install fastapi uvicorn pydantic apscheduler
```

## 启动服务

```bash
python main.py
```

服务默认监听 `http://0.0.0.0:8000`。

## API 接口

### POST /ingest
上报传感器数据。

**请求体**：
```json
{
  "sensor_id": "sensor-01",
  "temperature": 25.5,
  "humidity": 60.2,
  "timestamp": 1234567890.0
}
```

**校验规则**：
- `sensor_id`: 1-64 字符，仅允许字母数字、连字符、下划线
- `temperature`: -40.0 ~ 85.0
- `humidity`: 0.0 ~ 100.0
- `timestamp`: 可选，缺省用服务器时间

### GET /rules
查询当前告警阈值。

### PUT /rules
更新告警阈值（热更新，立即生效）。

**请求体**：
```json
{
  "temp_high": 35.0,
  "temp_low": 0.0,
  "humidity_high": 90.0,
  "humidity_low": 10.0
}
```

所有字段可选，留 null 表示不启用对应告警。

### GET /stream
SSE 实时告警广播流。多个客户端同时连接，每个客户端都收到相同的告警消息。

```bash
curl -N http://localhost:8000/stream
```

### GET /alerts
查询历史告警记录。

**参数**：
- `limit`: 返回条数，默认 50，最大 500

### POST /events
写入一条自由格式事件日志。

**请求体**：
```json
{
  "sensor_id": "sensor-01",
  "event_type": "calibration",
  "payload": {"calibrated_by": "admin", "offset": -0.5},
  "source": "user"
}
```

**字段说明**：
- `sensor_id`: 1-64 字符，仅允许字母数字、连字符、下划线
- `event_type`: 1-128 字符的事件类型，如 `"calibration"`、`"firmware_update"`、`"manual_override"`
- `payload`: 任意 JSON 对象，默认 `{}`
- `source`: 事件来源，仅允许 `"api"`、`"system"`、`"user"` 三值之一

**curl 示例**：
```bash
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"sensor_id":"sensor-01","event_type":"calibration","payload":{"offset":-0.5},"source":"user"}'
```

### GET /events
查询事件日志，支持多条件筛选和游标翻页。

**参数**：
- `sensor_id`: 按传感器 ID 过滤（可选）
- `event_type`: 按事件类型过滤（可选）
- `source`: 按来源过滤，可选值 `"api"`、`"system"`、`"user"`（可选）
- `limit`: 返回条数，默认 50，最大 500
- `before`: 游标，返回 id 小于此值的事件，用于翻页（可选）

结果按 `id DESC` 排列，即最新事件在前。翻页时取返回结果中最小的 `id` 作为下一页的 `before` 值。

**curl 示例**：
```bash
# 按 sensor_id 过滤
curl "http://localhost:8000/events?sensor_id=sensor-01"

# 按 event_type 和 source 组合过滤
curl "http://localhost:8000/events?event_type=calibration&source=user"

# 游标翻页：第一页取 limit=2，然后用返回最小 id 作为 before
curl "http://localhost:8000/events?limit=2"
curl "http://localhost:8000/events?limit=2&before=5"
```

### DELETE /events/{id}
按 ID 单条删除事件。

**curl 示例**：
```bash
curl -X DELETE http://localhost:8000/events/1
```

删除成功返回 `{"status":"ok","deleted":1}`，ID 不存在时返回 404。

### GET /health
健康检查。

## 验证步骤

### 1. 启动服务

```bash
python main.py
```

### 2. Pydantic 入参校验：灌脏数据应被拒

```bash
# 温度超范围 - 应返回 422
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"sensor_id":"sensor-01","temperature":100.0,"humidity":50.0}'

# sensor_id 含特殊字符 - 应返回 422
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"sensor_id":"sensor@01","temperature":25.0,"humidity":50.0}'

# 湿度为负 - 应返回 422
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"sensor_id":"sensor-01","temperature":25.0,"humidity":-10.0}'
```

### 3. 调低阈值后新告警从 /stream 冒出

**终端 1**：监听 SSE 流
```bash
curl -N http://localhost:8000/stream
```

**终端 2**：同样监听（验证广播，两个终端都应收到相同消息）
```bash
curl -N http://localhost:8000/stream
```

**终端 3**：调低阈值触发告警
```bash
curl -X PUT http://localhost:8000/rules \
  -H "Content-Type: application/json" \
  -d '{"temp_high":20.0,"temp_low":null,"humidity_high":null,"humidity_low":null}'
```

等待最多 30 秒（聚合周期），终端 1 和终端 2 应该同时收到相同的告警事件。

### 4. 杀进程重启 alerts 不清零，且停机期间的读数被补聚合

```bash
# 查看当前告警数
curl http://localhost:8000/alerts?limit=5

# 杀掉服务（Ctrl+C 或 taskkill）

# 重新启动
python main.py

# 再次查看告警 - 历史数据仍然存在，停机期间的外部数据（通过 /ingest 写入的）也会被补聚合
curl http://localhost:8000/alerts?limit=5
```

## 设计说明

### 定长窗口聚合 + Checkpoint 补齐
聚合窗口为定长 30 秒，对齐到整分钟（如 00:00~00:30, 00:30~01:00）。每次聚合完成后将 `window_end` 写入 `aggregation_checkpoints` 表。重启时从 checkpoint 读取上次处理到哪个窗口，补齐所有遗漏窗口。这保证了：即使进程停了 5 分钟再起，这 5 分钟内通过 /ingest 写入的读数也会被正确聚合，告警判定不会遗漏。

### 告警去重（状态转换模型）
`alert_state` 表持久化当前活跃的 (sensor_id, alert_type) 对。聚合逻辑只在 **状态转换** 时写入 alerts：
- 正常 → 超阈值：插入 alert + 更新 alert_state
- 超阈值 → 超阈值（持续）：不重复写入
- 超阈值 → 恢复正常：从 alert_state 删除，下次再超时可重新告警

阈值热更新也会触发去重逻辑：调低阈值后，之前未超阈的传感器变为超阈，因为 alert_state 中没有对应 key，会正确生成新告警。

### SSE 广播
每个 /stream 客户端连接时创建独立的 asyncio.Queue，注册到广播列表。告警产生时遍历所有客户端 Queue 推送，实现真正的广播——多个客户端同时连接都收到相同消息。客户端断开时自动从广播列表移除。

### 阈值热更新
规则存储在 SQLite 的 `rules` 表中，每次聚合任务运行时都从数据库读取最新规则，因此 PUT /rules 更新后无需重启服务，下一个聚合周期立即生效。热更新后的新阈值会与 alert_state 中的旧状态做对比，产生正确的状态转换。
