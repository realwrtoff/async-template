import os
from dotenv import load_dotenv
load_dotenv()

class EnvLoader:
    def __init__(self, task_id=None):
        self.prefix = f"{task_id}_".upper() if task_id else ""

    def get(self, key, default=None):
        return os.getenv(f"{self.prefix}{key}", default)

    def get_int(self, key, default=1):
        try:
            return int(os.getenv(f"{self.prefix}{key}", default))
        except:
            return default