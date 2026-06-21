# Edge IoT 温控网关模拟器

一个基于 FastAPI 的边缘 IoT 温控网关模拟器，支持温湿度数据采集、阈值告警、SSE 实时推送、断点续跑。

## 功能特性

- **10 个协程模拟传感器**：每秒推送温湿度数据
- **/ingest 数据接入**：FastAPI + Pydantic 校验，写入 SQLite
- **/rules 阈值配置**：支持热更新，无需重启服务
- **APScheduler 滚动聚合**：每 30 秒聚合一次，检测超阈值
- **/stream SSE 实时推送**：告警实时推送到客户端
- **断点续跑**：SQLite 持久化存储，重启不丢数据

## 项目结构

```
.
├── main.py        # FastAPI 主程序，传感器模拟，API 端点
├── models.py      # Pydantic 模型，SQLite 数据库操作
├── scheduler.py   # APScheduler 聚合任务，告警检测
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

所有字段可选，留空表示不启用对应告警。

### GET /stream
SSE 实时告警流。

```bash
curl -N http://localhost:8000/stream
```

### GET /alerts
查询历史告警记录。

**参数**：
- `limit`: 返回条数，默认 50，最大 500

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

### 3. 调高阈值后新告警立即从 /stream 冒出

**终端 1**：监听 SSE 流
```bash
curl -N http://localhost:8000/stream
```

**终端 2**：调低阈值触发告警（例如把 temp_high 设到 20 度，很容易触发）
```bash
curl -X PUT http://localhost:8000/rules \
  -H "Content-Type: application/json" \
  -d '{"temp_high":20.0,"temp_low":null,"humidity_high":null,"humidity_low":null}'
```

等待最多 30 秒（聚合周期），终端 1 应该会收到告警事件。

### 4. 杀进程重启 alerts 不清零

```bash
# 查看当前告警数
curl http://localhost:8000/alerts?limit=5

# 杀掉服务（Ctrl+C 或 taskkill）

# 重新启动
python main.py

# 再次查看告警 - 历史数据仍然存在
curl http://localhost:8000/alerts?limit=5
```

## 设计说明

### 滚动聚合
APScheduler 每 30 秒执行一次，查询过去 30 秒内每个传感器的最大/最小/平均值，与阈值对比，产生的告警写入 alerts 表并推入 SSE 队列。

### 阈值热更新
规则存储在 SQLite 的 `rules` 表中，每次聚合任务运行时都从数据库读取最新规则，因此 PUT /rules 更新后无需重启服务，下一个聚合周期立即生效。

### 断点续跑
所有数据（传感器读数、告警、规则）都持久化到 SQLite，服务重启后自动恢复。传感器模拟协程在启动时重新创建，继续产生数据。

### SSE 推送
告警通过 asyncio.Queue 传递，`/stream` 端点从队列消费并以 SSE 格式推送。多个客户端各自独立消费队列中的消息（当前实现为广播式，新客户端会从连接时刻起接收新告警）。
