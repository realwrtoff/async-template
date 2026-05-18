# async-template

[English](README.en.md)

基于 **Python 3.12+** 与 **aio-pika** 的异步 RabbitMQ 消费者模板。提供连接池、延迟重试、死信队列，以及协程 / 线程 / 进程三种并发消费模式，便于快速搭建可观测的异步任务服务。

## 特性

- **异步 RabbitMQ 客户端**：连接池 + Channel 池，支持 `push` / `pop` / `ack` / `nack`
- **重试与死信**：失败任务进入延迟队列重试，超过次数后进入死信队列（DLQ）
- **三种消费者**：`CoroutineConsumer`（纯协程）、`ThreadConsumer`（线程）、`ProcessConsumer`（多进程）
- **任务注册表**：在 `main.py` 中集中注册任务 ID 与 Processor 映射
- **按任务配置**：通过环境变量前缀为每个任务单独设置队列名、并发数等
- **结构化日志**：控制台 + 按小时滚动的文件日志（`logs/app.log`）

## 项目结构

```
async-template/
├── main.py                 # 入口：注册任务、选择消费者、启动与优雅关闭
├── app/
│   └── demo_processor.py   # 示例 Processor，可复制扩展
├── core/
│   ├── rabbitmq.py         # RabbitMQ 封装（池化、重试、DLQ）
│   ├── processor.py        # TaskProcessor 抽象基类
│   └── consumer/           # Coroutine / Thread / Process 三种消费者
├── utils/
│   ├── env_loader.py       # 环境变量加载（支持任务级前缀）
│   └── logger.py           # 日志初始化
├── pyproject.toml
└── uv.lock
```

## 环境要求

- Python >= 3.12
- RabbitMQ（AMQP）
- 推荐使用 [uv](https://github.com/astral-sh/uv) 管理依赖

## 快速开始

### 1. 安装依赖

```bash
uv sync
```

或使用 pip：

```bash
pip install -e .
```

### 2. 配置环境变量

在项目根目录创建 `.env` 文件：

```env
# RabbitMQ 连接（必填）
RABBITMQ_URL=amqp://guest:guest@localhost:5672/

# 要启动的任务 ID，逗号分隔，需与 TASK_REGISTRY 中的 key 一致
TASK_IDS=demo_task

# 以下为 demo_task 的专属配置（前缀为任务 ID 大写 + 下划线）
DEMO_TASK_QUEUE_NAME=demo_task
DEMO_TASK_CONCURRENCY=2
```

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `RABBITMQ_URL` | AMQP 连接地址 | — |
| `TASK_IDS` | 启用的任务 ID 列表 | — |
| `{TASK_ID}_QUEUE_NAME` | 监听的队列名 | 任务 ID |
| `{TASK_ID}_CONCURRENCY` | 并发消费者数量 | `1` |

### 3. 运行

```bash
uv run python main.py
```

或：

```bash
python main.py
```

## 选择消费者模式

在 `main.py` 中修改 import 即可切换（默认使用协程消费者）：

```python
from core.consumer import CoroutineConsumer as Consumer
# from core.consumer import ThreadConsumer as Consumer
# from core.consumer import ProcessConsumer as Consumer
```

| 模式 | 适用场景 |
|------|----------|
| `CoroutineConsumer` | I/O 密集、纯 async 业务，资源占用低 |
| `ThreadConsumer` | 需在独立线程中跑事件循环，或部分阻塞调用 |
| `ProcessConsumer` | CPU 密集、需绕过 GIL 的计算任务 |

## 新增业务任务

### 1. 实现 Processor

在 `app/` 下新建文件，继承 `TaskProcessor`：

```python
from core.processor import TaskProcessor

class MyProcessor(TaskProcessor):
    async def process(self, task):
        # 业务逻辑；返回 False 或 falsy 值会触发重试
        return {"status": "ok"}

    async def callback(self, task, result):
        # 处理成功后的回调（统计、通知等）
        pass
```

### 2. 注册任务

在 `main.py` 的 `TASK_REGISTRY` 中注册：

```python
from app.my_processor import MyProcessor

TASK_REGISTRY: dict[str, Type[TaskProcessor]] = {
    "demo_task": DemoProcessor,
    "my_task": MyProcessor,
}
```

### 3. 配置并启动

在 `.env` 中增加 `TASK_IDS=my_task` 及 `MY_TASK_*` 相关变量后重新运行即可。

## 消息处理与重试

1. 从队列 `pop` 消息，解析 JSON 为 `task`
2. 调用 `processor.process(task)`
3. 无论成功或失败，先 `ack` 原消息
4. 若 `process` 返回 **真值**：执行 `callback`，记为成功
5. 若返回 **假值**：按 `count` 头递增重试次数，写入延迟队列；超过 `max_retry`（默认 3 次）则记为失败
6. 延迟时间默认 30 秒；超过最大重试后消息进入 `{queue_name}_dlq` 死信队列

向队列投递测试消息示例（需已声明对应队列）：

```python
import asyncio
from core.rabbitmq import RabbitMQ

async def main():
    rmq = RabbitMQ("amqp://guest:guest@localhost:5672/")
    await rmq.init()
    await rmq.push("demo_task", {"hello": "world"})
    await rmq.close()

asyncio.run(main())
```

## 日志

- 目录：`logs/`
- 文件：`app.log`，按小时切割，默认保留约 7 天
- 关键事件：`task_received`、`task_ack`、`task_success`、`task_retry`、`task_failed`、`task_exception`

## 依赖

- [aio-pika](https://github.com/mosquito/aio-pika) — 异步 AMQP 客户端
- [python-dotenv](https://github.com/theskumar/python-dotenv) — `.env` 加载

## License

本项目采用 [MIT License](LICENSE)。
