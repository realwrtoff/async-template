import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Optional

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message
from aio_pika.exceptions import (
    QueueEmpty,
    ConnectionClosed,
    ChannelClosed,
    AMQPError
)
from aio_pika.pool import Pool

logger = logging.getLogger(__name__)


class BasicQueue(ABC):
    @abstractmethod
    async def init(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def push(self, queue_name: str, data: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    async def pop(self, queue_name: str,** kwargs: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    async def ack(self, message: Any) -> None:
        raise NotImplementedError

    @abstractmethod
    async def nack(self, message: Any, requeue: bool = False) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


class RabbitMQ(BasicQueue):
    DEFAULT_POOL_SIZE = 2
    DEFAULT_CHANNEL_SIZE = 10
    DEFAULT_MAX_RETRY_COUNT = 3
    DEFAULT_DEAD_LETTER_DAYS = 7
    DEFAULT_DELAY_SECONDS = 30
    DEFAULT_POP_TIMEOUT = 1.0

    def __init__(
        self,
        rabbit_url: str,
        pool_size: int = DEFAULT_POOL_SIZE,
        channel_size: int = DEFAULT_CHANNEL_SIZE,
        heartbeat: int = 60,
        max_retry_count: int = DEFAULT_MAX_RETRY_COUNT,
    ):
        self.rabbit_url = rabbit_url
        self.pool_size = pool_size
        self.channel_size = channel_size
        self.heartbeat = heartbeat
        self.max_retry_count = max_retry_count

        self._connection_pool: Optional[Pool] = None
        self._channel_pool: Optional[Pool] = None
        self._init_lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._closed = False

    async def init(self) -> None:
        if self._ready.is_set() or self._closed:
            return

        async with self._init_lock:
            if self._ready.is_set() or self._closed:
                return

            try:
                self._connection_pool = Pool(
                    self._get_connection, max_size=self.pool_size
                )
                self._channel_pool = Pool(
                    self._get_channel, max_size=self.channel_size
                )
                self._ready.set()
                logger.info(f"[RabbitMQ] 初始化成功 pool={self.pool_size} channel={self.channel_size}")
            except Exception as e:
                logger.error(f"[RabbitMQ] 初始化失败: {e}", exc_info=True)
                raise

    async def _get_connection(self) -> aio_pika.abc.AbstractRobustConnection:
        try:
            return await aio_pika.connect_robust(
                self.rabbit_url, heartbeat=self.heartbeat
            )
        except AMQPError as e:
            logger.error(f"[RabbitMQ] 连接失败: {e}")
            raise

    async def _get_channel(self) -> aio_pika.abc.AbstractChannel:
        if self._closed:
            raise RuntimeError("client closed")

        async with self._connection_pool.acquire() as conn:
            ch = await conn.channel()
            await ch.set_qos(prefetch_count=1)  # 只设置一次！
            return ch

    async def push(self, queue_name: str, data: Any,** kwargs: Any) -> Any:
        if self._closed:
            raise RuntimeError("client closed")

        await self.init()
        count = int(kwargs.get("count", 0))
        durable = kwargs.get("durable", True)

        async with self._channel_pool.acquire() as channel:
            if 0 < count < self.max_retry_count:
                rk = await self._setup_delay_queue(channel, queue_name,** kwargs)
            elif count >= self.max_retry_count:
                rk = await self._setup_dead_letter_queue(channel, queue_name, **kwargs)
            else:
                rk = await self._setup_normal_queue(channel, queue_name, durable)

            msg = self._create_message(data, count)
            await channel.default_exchange.publish(msg, routing_key=rk)
            logger.debug(f"[PUSH] queue={queue_name} count={count}")
            return msg

    async def pop(self, queue_name: str,** kwargs: Any) -> Optional[aio_pika.abc.AbstractIncomingMessage]:
        if self._closed:
            return None

        await self.init()
        durable = kwargs.get("durable", True)

        try:
            async with self._channel_pool.acquire() as channel:
                queue = await channel.declare_queue(queue_name, durable=durable)
                return await asyncio.wait_for(
                    queue.get(), timeout=self.DEFAULT_POP_TIMEOUT
                )
        except (QueueEmpty, asyncio.TimeoutError):
            return None
        except (ConnectionClosed, ChannelClosed):
            logger.warning("[POP] 连接断开，准备重连")
            self._ready.clear()
            return None
        except Exception as e:
            logger.error(f"[POP] 异常 queue={queue_name}: {e}", exc_info=True)
            return None

    async def ack(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        try:
            if not message.processed:
                await message.ack()
        except Exception as e:
            logger.warning(f"[ACK] 失败: {e}")

    async def nack(self, message: aio_pika.abc.AbstractIncomingMessage, requeue: bool = False) -> None:
        try:
            if not message.processed:
                await message.nack(requeue=requeue)
        except Exception as e:
            logger.warning(f"[NACK] 失败: {e}")

    async def _setup_delay_queue(self, channel: aio_pika.abc.AbstractChannel, queue_name: str,** kwargs: Any) -> str:
        durable = kwargs.get("durable", True)
        ex_name = kwargs.get("exchange", "letter-exchange")
        delay = int(kwargs.get("seconds", self.DEFAULT_DELAY_SECONDS))
        ttl = delay * 1000

        ex = await channel.declare_exchange(ex_name, ExchangeType.DIRECT, durable=durable)
        queue = await channel.declare_queue(queue_name, durable=durable)
        trans_rk = f"{queue_name}_trans_router"
        await queue.bind(ex, routing_key=trans_rk)

        retry_rk = f"{queue_name}_retry"
        await channel.declare_queue(
            retry_rk,
            durable=durable,
            arguments={
                "x-message-ttl": ttl,
                "x-dead-letter-exchange": ex_name,
                "x-dead-letter-routing-key": trans_rk,
            }
        )
        return retry_rk

    async def _setup_dead_letter_queue(self, channel: aio_pika.abc.AbstractChannel, queue_name: str,** kwargs: Any) -> str:
        days = int(kwargs.get("days", self.DEFAULT_DEAD_LETTER_DAYS))
        dlq_rk = f"{queue_name}_dlq"
        await channel.declare_queue(
            dlq_rk,
            durable=kwargs.get("durable", True),
            arguments={"x-expires": days * 24 * 3600 * 1000}
        )
        return dlq_rk

    async def _setup_normal_queue(self, channel: aio_pika.abc.AbstractChannel, queue_name: str, durable: bool) -> str:
        await channel.declare_queue(queue_name, durable=durable)
        return queue_name

    @staticmethod
    def _create_message(data: Any, count: int) -> Message:
        return Message(
            body=json.dumps(data, ensure_ascii=False).encode(),
            headers={"count": count},
            delivery_mode=DeliveryMode.PERSISTENT
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._ready.clear()

        try:
            if self._channel_pool:
                await self._channel_pool.close()
            if self._connection_pool:
                await self._connection_pool.close()
            logger.info("[RabbitMQ] 已关闭")
        except Exception as e:
            logger.error(f"[关闭失败] {e}")