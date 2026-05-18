# async-template

[中文文档](README.md)

An async RabbitMQ consumer template built on **Python 3.12+** and **aio-pika**. It provides connection pooling, delayed retries, dead-letter queues, and three concurrency modes (coroutine / thread / process) so you can stand up observable async task workers quickly.

## Features

- **Async RabbitMQ client**: connection pool + channel pool with `push` / `pop` / `ack` / `nack`
- **Retry & dead letter**: failed tasks go to a delay queue for retry; after max attempts they land in a DLQ
- **Three consumer types**: `CoroutineConsumer` (async), `ThreadConsumer` (threads), `ProcessConsumer` (multiprocessing)
- **Task registry**: central mapping of task IDs to processors in `main.py`
- **Per-task config**: env vars prefixed by task ID for queue name, concurrency, and more
- **Structured logging**: console + hourly-rotating file logs (`logs/app.log`)

## Project layout

```
async-template/
├── main.py                 # Entry: register tasks, pick consumer, start & graceful shutdown
├── app/
│   └── demo_processor.py   # Example processor — copy and extend
├── core/
│   ├── rabbitmq.py         # RabbitMQ wrapper (pooling, retry, DLQ)
│   ├── processor.py        # TaskProcessor abstract base class
│   └── consumer/           # Coroutine / Thread / Process consumers
├── utils/
│   ├── env_loader.py       # Env loading (per-task prefix support)
│   └── logger.py           # Logger setup
├── pyproject.toml
└── uv.lock
```

## Requirements

- Python >= 3.12
- RabbitMQ (AMQP)
- [uv](https://github.com/astral-sh/uv) recommended for dependencies

## Quick start

### 1. Install dependencies

```bash
uv sync
```

Or with pip:

```bash
pip install -e .
```

### 2. Configure environment

Create a `.env` file in the project root:

```env
# RabbitMQ connection (required)
RABBITMQ_URL=amqp://guest:guest@localhost:5672/

# Task IDs to run (comma-separated); keys must exist in TASK_REGISTRY
TASK_IDS=demo_task

# Per-task settings for demo_task (prefix: UPPERCASE task id + underscore)
DEMO_TASK_QUEUE_NAME=demo_task
DEMO_TASK_CONCURRENCY=2
```

| Variable | Description | Default |
|----------|-------------|---------|
| `RABBITMQ_URL` | AMQP connection URL | — |
| `TASK_IDS` | Enabled task ID list | — |
| `{TASK_ID}_QUEUE_NAME` | Queue to consume | task ID |
| `{TASK_ID}_CONCURRENCY` | Number of concurrent workers | `1` |

### 3. Run

```bash
uv run python main.py
```

Or:

```bash
python main.py
```

## Choosing a consumer mode

Change the import in `main.py` (coroutine consumer is the default):

```python
from core.consumer import CoroutineConsumer as Consumer
# from core.consumer import ThreadConsumer as Consumer
# from core.consumer import ProcessConsumer as Consumer
```

| Mode | When to use |
|------|-------------|
| `CoroutineConsumer` | I/O-bound, fully async workloads; low overhead |
| `ThreadConsumer` | Separate event loop per thread, or some blocking calls |
| `ProcessConsumer` | CPU-bound work that benefits from bypassing the GIL |

## Adding a new task

### 1. Implement a processor

Create a file under `app/` and subclass `TaskProcessor`:

```python
from core.processor import TaskProcessor

class MyProcessor(TaskProcessor):
    async def process(self, task):
        # Business logic; return False or any falsy value to trigger retry
        return {"status": "ok"}

    async def callback(self, task, result):
        # Post-success hook (metrics, notifications, etc.)
        pass
```

### 2. Register the task

Add it to `TASK_REGISTRY` in `main.py`:

```python
from app.my_processor import MyProcessor

TASK_REGISTRY: dict[str, Type[TaskProcessor]] = {
    "demo_task": DemoProcessor,
    "my_task": MyProcessor,
}
```

### 3. Configure and run

Add `TASK_IDS=my_task` and `MY_TASK_*` variables to `.env`, then start the service again.

## Message handling & retries

1. `pop` a message from the queue and parse JSON into `task`
2. Call `processor.process(task)`
3. Always `ack` the original message first (success or failure)
4. If `process` returns a **truthy** value: run `callback` and treat as success
5. If **falsy**: increment retry count from the `count` header and republish to a delay queue; after `max_retry` (default 3) the task is marked failed
6. Default delay is 30 seconds; after max retries messages go to `{queue_name}_dlq`

Example: publish a test message (queue must already exist):

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

## Logging

- Directory: `logs/`
- File: `app.log`, rotated hourly, ~7 days retention by default
- Key events: `task_received`, `task_ack`, `task_success`, `task_retry`, `task_failed`, `task_exception`

## Dependencies

- [aio-pika](https://github.com/mosquito/aio-pika) — async AMQP client
- [python-dotenv](https://github.com/theskumar/python-dotenv) — `.env` loading

## License

This project is licensed under the [MIT License](LICENSE).
