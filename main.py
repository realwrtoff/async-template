import asyncio
import logging
import signal
from typing import List, Type

from app.demo_processor import DemoProcessor
# -------------- 只需要改这一行 --------------
from core.consumer import CoroutineConsumer as Consumer
# from core.consumer import ThreadConsumer as Consumer
# from core.consumer import ProcessConsumer as Consumer
# -------------------------------------------
from core.processor import TaskProcessor
from utils.env_loader import EnvLoader
from utils.logger import setup_logger

setup_logger()
logger = logging.getLogger(__name__)

TASK_REGISTRY: dict[str, Type[TaskProcessor]] = {
    "demo_task": DemoProcessor,
}

consumers: List[Consumer] = []


async def run_all_consumers(amqp_url: str, task_ids: List[str]):
    tasks = []
    for task_id in task_ids:
        if task_id not in TASK_REGISTRY:
            continue

        processor_cls = TASK_REGISTRY[task_id]
        processor = processor_cls(task_id=task_id)

        consumer = Consumer(
            amqp_url=amqp_url,
            queue_name=processor.queue_name,
            processor=processor,
            concurrency=processor.concurrency,
        )
        consumers.append(consumer)
        tasks.append(consumer.start())

        logger.info(f"🚀 启动任务 [{task_id}] | queue={processor.queue_name} | 并发={processor.concurrency}")

    await asyncio.gather(*tasks)


async def shutdown_all():
    logger.info("🛑 优雅关闭所有消费者...")
    for consumer in consumers:
        await consumer.stop()
    logger.info("✅ 所有消费者已关闭")


def handle_exit_signal(*args, **kwargs):
    asyncio.create_task(shutdown_all())


async def main():
    env = EnvLoader()
    amqp_url = env.get("RABBITMQ_URL")
    task_ids_str = env.get("TASK_IDS", "").strip()

    if not task_ids_str:
        logger.warning("⚠️ 未配置 TASK_IDS")
        return

    task_ids = [t.strip() for t in task_ids_str.split(",") if t.strip()]
    valid_tasks = [t for t in task_ids if t in TASK_REGISTRY]

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_exit_signal)

    await run_all_consumers(amqp_url, valid_tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 服务已安全退出")