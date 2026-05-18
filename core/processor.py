from abc import ABC, abstractmethod
from typing import Any, Dict

from utils.env_loader import EnvLoader


class TaskProcessor(ABC):
    def __init__(self, task_id: str):
        self.task_id = task_id
        self.env = EnvLoader(task_id)
        self.queue_name = self.env.get('queue_name', self.task_id)
        self.concurrency = self.env.get_int('concurrency', 1)

    @abstractmethod
    async def process(self, task: Dict[str, Any]) -> Any:
        pass

    async def callback(self, task: Dict[str, Any], result: Any) -> None:
        return