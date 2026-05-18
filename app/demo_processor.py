import logging
from core.processor import TaskProcessor

logger = logging.getLogger(__name__)


class DemoProcessor(TaskProcessor):
    async def process(self, task):
        """
        核心业务逻辑
        :param task: 从 MQ 消费出来的 JSON 对象
        :return: 任意结果（False = 触发重试）
        """
        logger.info({
            "event": "process_start",
            "queue": self.queue_name,
            "task": task
        })

        # ======================
        # 在这里写你的业务逻辑
        # ======================
        # 示例：发送邮件 / 下载文件 / 解析数据 / 调用接口

        # 模拟业务成功
        return {"status": "ok", "data": {}}

    async def callback(self, task, result):
        """
        业务执行完成后的回调
        :param task: 原始任务
        :param result: process() 返回的结果
        """
        # 结构化可观测日志
        logger.info({
            "event": "process_done",
            "queue": self.queue_name,
            "result": result,
        })

        # 你可以在这里：统计、监控、发通知、写库等