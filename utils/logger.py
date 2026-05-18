import logging
import os
from logging.handlers import TimedRotatingFileHandler

def setup_logger(
    name: str = "app",
    log_dir: str = "logs",
    backup_count: int = 7 * 24  # 7天 * 24小时
):
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    # 按小时切割，保留7天
    file_handler = TimedRotatingFileHandler(
        when="H",
        interval=1,
        backupCount=backup_count,
        encoding="utf-8",
        filename=f"{log_dir}/app.log"
    )

    # 可观测格式：结构化纯文本（对接日志平台最舒服）
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )
    file_handler.setFormatter(formatter)

    # 控制台输出
    console = logging.StreamHandler()
    console.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console)
    return logger