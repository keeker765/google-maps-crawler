import logging
from logging.handlers import RotatingFileHandler
from rich.logging import RichHandler
from rich.console import Console
import os

# 日志目录与文件
LOG_DIR = "logs"
LOG_FILE = "app.log"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, LOG_FILE)

# Rich 控制台配置
console = Console(force_terminal=True)  # 强制彩色输出（即使非 TTY 环境）

# 格式模板
LOG_FORMAT = "[%(levelname)s] | %(filename)s:%(lineno)d | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# 文件日志 Handler（带轮换）
file_handler = RotatingFileHandler(
    LOG_PATH,
    maxBytes=5 * 1024 * 1024,  # 每个文件最大 5MB
    backupCount=5,             # 最多保留 5 个历史日志
    encoding="utf-8"
)

# Rich 控制台日志 Handler（关闭 file://）
rich_handler = RichHandler(
    show_time=True,
    show_level=True,
    show_path=False,  # ✅ 关键：关闭 file:// 路径
    console=console
)

# 统一配置
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt=DATE_FORMAT,
    handlers=[file_handler, rich_handler]
)

# 获取主 logger
main_thread_logger = logging.getLogger("main")
crawler_thread_logger = logging.getLogger("crawler")
writer_thread_logger = logging.getLogger("writer")
