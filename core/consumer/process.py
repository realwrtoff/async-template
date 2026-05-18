import asyncio
import json
import logging
import multiprocessing

from core.consumer.base import BaseConsumer
from core.processor import TaskProcessor
from core.rabbitmq import RabbitMQ

logger = logging.getLogger(__name__)


class ProcessConsumer(BaseConsumer):
    def __init__(
        self,
        amqp_url: str,
        queue_name: str,
        processor: TaskProcessor,
        concurrency: int = 1,
        max_retry: int = 3,
        retry_delay: int = 30,
    ):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.processor = processor
        self.concurrency = concurrency
        self.max_retry = max_retry
        self.retry_delay = retry_delay

        self._stop_event = multiprocessing.Event()

    async def start(self):
        logger.info(f"[ProcessConsumer] start | queue={self.queue_name} | concurrency={self.concurrency}")

        for _ in range(self.concurrency):
            p = multiprocessing.Process(target=self._process_worker, daemon=True)
            p.start()

        while not self._stop_event.is_set():
            await asyncio.sleep(1)

    def _process_worker(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # 每个进程自己的 RMQ 客户端
        rmq = RabbitMQ(self.amqp_url)
        loop.run_until_complete(rmq.init())

        try:
            loop.run_until_complete(self._process_consume(rmq))
        finally:
            loop.run_until_complete(rmq.close())
            loop.close()

    async def _process_consume(self, rmq):
        while not self._stop_event.is_set():
            try:
                msg = await rmq.pop(self.queue_name)
                if not msg:
                    continue

                logger.info({"event": "task_received", "queue": self.queue_name})
                task = json.loads(msg.body)
                retry = int(msg.headers.get("count", 0))

                result = await self.processor.process(task)
                await rmq.ack(msg)

                logger.info({"event": "task_ack", "queue": self.queue_name})

                if result:
                    await self.processor.callback(task, result)
                    logger.info({"event": "task_success", "queue": self.queue_name})
                else:
                    await self._retry(rmq, task, retry)

            except Exception as e:
                logger.error(f"consume error: {e}", exc_info=True)
                logger.error({"event": "task_exception", "queue": self.queue_name,"error": str(e)})
                await asyncio.sleep(0.5)

    async def _retry(self, rmq, task, current_retry):
        next_retry = current_retry + 1

        # 超过最大重试次数 → 最终失败
        if next_retry > self.max_retry:
            logger.error({"event": "task_failed", "queue": self.queue_name})
            return

        # 重试入队
        await rmq.push(
            self.queue_name,
            task,
            count=next_retry,
            seconds=self.retry_delay,
            max_retry=self.max_retry
        )

        # 重试日志
        logger.warning(
            {"event": "task_retry", "queue": self.queue_name, "current_retry": current_retry,"next_retry": next_retry}
        )

    async def stop(self):
        self._stop_event.set()
        logger.info(f"[ProcessConsumer] stopped | queue={self.queue_name}")