import logging
import logging.config
from enum import Enum

try:
    from pydantic import BaseSettings  # type: ignore
except ImportError:  # Pydantic v2
    from pydantic_settings import BaseSettings  # type: ignore


class StorageMode(Enum):
    DEBUG = "DEBUG"
    SQS = "SQS"
    CSV = "CSV"


class Settings(BaseSettings):
    STORAGE_MODE: StorageMode = StorageMode.CSV
    SCRAPED_EVENT_SQS_URL: str = ""
    SCRAPED_EVENT_CSV_PATH: str = "data/places.csv"
    PROXY_LIST: str = ""
    PROXY_FILE: str = ""
    
    # 渐进式线程调度配置
    THREAD_STARTUP_DELAY: float = 1.0  # 线程启动间隔（秒）
    THREAD_BATCH_SIZE: int = 2  # 每批启动的线程数
    THREAD_BATCH_DELAY: float = 3.0  # 批次间延迟（秒）
    # 写入线程进度日志频率（每写入 N 条打印一次）
    WRITER_PROGRESS_EVERY: int = 10

    class Config:
        env_file = ".env"


def init_logging(level: str = "INFO") -> None:
    """Use project's logger.py to configure logging by import side-effect."""
    try:
        import logger  # noqa: F401
    except Exception:
        # Fallback minimal setup
        root = logging.getLogger()
        if getattr(root, "_gmaps_inited", False):
            return
        handler = logging.StreamHandler()
        handler.setLevel(level)
        fmt = logging.Formatter("[%(levelname)s] | %(filename)s:%(lineno)d | %(message)s")
        handler.setFormatter(fmt)
        root.handlers[:] = []
        root.addHandler(handler)
        root.setLevel(level)
        setattr(root, "_gmaps_inited", True)


settings = Settings()
