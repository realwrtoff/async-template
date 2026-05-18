import asyncio
import json
import logging

from core.consumer.base import BaseConsumer
from core.processor import TaskProcessor
from core.rabbitmq import RabbitMQ

logger = logging.getLogger(__name__)


class CoroutineConsumer(BaseConsumer):
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

        self._rmq = RabbitMQ(amqp_url)
        self._stop_event = asyncio.Event()

    async def start(self):
        await self._rmq.init()
        logger.info(f"[CoroutineConsumer] start | queue={self.queue_name} | concurrency={self.concurrency}")

        tasks = [asyncio.create_task(self._consume()) for _ in range(self.concurrency)]
        await asyncio.gather(*tasks)

    async def _consume(self):
        while not self._stop_event.is_set():
            try:
                msg = await self._rmq.pop(self.queue_name)
                if not msg:
                    continue

                logger.info({"event": "task_received", "queue": self.queue_name})
                task = json.loads(msg.body)
                count_val = msg.headers.get("count", 0)
                if isinstance(count_val, bytes):
                    retry = int(count_val.decode())
                elif isinstance(count_val, (int, str)):
                    retry = int(count_val)
                else:
                    retry = 0

                result = await self.processor.process(task)
                await self._rmq.ack(msg)

                logger.info({"event": "task_ack", "queue": self.queue_name})

                if result:
                    await self.processor.callback(task, result)
                    logger.info({"event": "task_success", "queue": self.queue_name})
                else:
                    await self._retry(task, retry)

            except Exception as e:
                logger.error(f"consume error: {e}", exc_info=True)
                logger.error({"event": "task_exception", "queue": self.queue_name,"error": str(e)})
                await asyncio.sleep(0.5)

    async def _retry(self, task, current_retry):
        next_retry = current_retry + 1

        # 超过最大重试次数 → 失败
        if next_retry > self.max_retry:
            logger.error({"event": "task_failed", "queue": self.queue_name})
            return

        # 重试入队
        await self._rmq.push(
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
        await self._rmq.close()
        logger.info(f"[CoroutineConsumer] stopped | queue={self.queue_name}")