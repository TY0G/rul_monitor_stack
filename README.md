# Flask + Kafka + Spark Structured Streaming 发动机 RUL 监控系统

此项目基于CMAPSS-FD001数据集训练，包含：

- Flask 登录注册页面
- 邮箱验证码注册 / 登录 / 忘记密码
- 首页 ECharts 剩余寿命监控图
- 发动机 1 - 100 切换
- 启动 / 暂停按钮
- 打印周期速度设置
- Kafka Producer 读取 `test_FD001.txt` 推流
- Spark Structured Streaming 消费 Kafka 并落盘
- Flask 读取 Spark 输出并调用 `best_rul_model.pkl` / `scaler.pkl` 预测 RUL
- 剩余寿命低于40自动邮件报警
- Docker Compose 一键启动

## 目录结构

```text
.
├─ app.py
├─ docker-compose.yml
├─ Dockerfile
├─ requirements.txt
├─ data/
│  ├─ train_FD001.txt
│  ├─ test_FD001.txt
│  └─ RUL_FD001.txt
├─ models/
│  ├─ best_rul_model.pkl
│  └─ scaler.pkl
├─ producer/
│  ├─ Dockerfile
│  ├─ producer.py
│  └─ requirements.txt
├─ spark/
│  └─ stream_job.py
├─ static/
├─ templates/
└─ runtime/
```


```text
http://127.0.0.1:5000
```

## Docker 启动整套链路

项目根目录执行：

```bash
docker compose up --build
```

启动后：

- Web：`http://localhost:5000`
- Kafka：`localhost:9092`
- Spark Structured Streaming：容器内运行

## 使用方式

1. 先打开网页并注册 / 登录。
2. 进入主页后点击“启动”。
3. Flask 会创建 `runtime/control/producer.enabled` 文件。
4. Producer 检测到该文件后开始向 Kafka 推送 `test_FD001.txt` 数据。
5. Spark 从 Kafka 读取流并写入 `runtime/stream_output/parsed`。
6. Flask 自动读取 Spark 输出并刷新图表。

再次点击“暂停”会关闭 producer 控制标记，推流暂停。

## 说明

### 1. 模型文件

当前项目已经自带示例模型：

- `models/best_rul_model.pkl`
- `models/scaler.pkl`

如果你想换成你自己训练出来的模型，直接覆盖同名文件即可。
模型训练文件参阅notebook文件夹

### 2. 数据源优先级

主页优先读取：

1. Spark Structured Streaming 的落盘结果
2. 如果当前还没有流式数据，则回退到离线 `FD001` 测试集回放

### 3. 重训模型

如果你想重新生成模型文件：

```bash
python scripts/train_fd001_model.py
```

### 4. 建议选择引擎20作为样例


### 5. 纯模拟传感器推流（不依赖 test_FD001）

如果你想直接模拟“传感器 -> Kafka”链路，可运行：

```bash
python producer/sensor_simulator.py
```

可选环境变量：

- `KAFKA_BOOTSTRAP_SERVERS`（默认 `localhost:9092`）
- `KAFKA_TOPIC`（默认 `engine-rul-topic`）
- `SIM_ENGINE_COUNT`（默认 `10`）
- `SIM_INTERVAL_SECONDS`（默认 `0.2`）
- `SIM_START_RUL`（默认 `180`）
- `SIM_LOOP_FOREVER`（默认 `true`）

示例：

```bash
SIM_ENGINE_COUNT=5 SIM_INTERVAL_SECONDS=0.1 SIM_LOOP_FOREVER=false python producer/sensor_simulator.py
```

## 常见问题

### PowerShell 运行脚本被拦截

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\run.ps1
```

### 数据库文件查看

项目使用 SQLite，数据库文件是：

```text
site.db
```

可以用 DB Browser for SQLite 打开。

## 适合后续继续扩展的方向

- 把 Spark 输出改成 Parquet / Delta
- 把 Flask 改成 WebSocket 实时推送
- 把 Producer 改成从真实设备流读取
- 把模型替换成成神经网络模型
