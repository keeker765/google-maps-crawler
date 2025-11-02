from __future__ import annotations

import signal
import threading
from typing import Callable, Optional
from logger import main_thread_logger as logger


STOP_EVENT = threading.Event()


def _make_handler() -> Callable[[int, Optional[object]], None]:
    def handler(signum, frame):  # type: ignore[override]
        STOP_EVENT.set()
    return handler


def install_signal_handlers() -> None:
    """Install Ctrl+C (SIGINT) handler to set a global stop flag.

    Call this once in CLI entry before starting long-running work.
    """
    try:
        signal.signal(signal.SIGINT, _make_handler())
    except Exception as e:
        logger.warning("install_signal_handlers failed: %s", e)
