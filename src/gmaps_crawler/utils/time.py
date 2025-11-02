from datetime import datetime, timezone
from contextlib import contextmanager
import time
from typing import Iterator, Optional


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def log_duration(logger, label: str, *, level: str = "info", enabled: bool = True) -> Iterator[None]:
    """Context manager to log a block's elapsed time in milliseconds.

    Usage:
        with log_duration(logger, "open_search_tab"):
            tab = browser.new_tab(url=url)
    """
    t0 = time.monotonic()
    try:
        yield
    finally:
        if enabled:
            elapsed = int((time.monotonic() - t0) * 1000)
            log = getattr(logger, level, logger.info)
            log("%s elapsed=%dms", label, elapsed)
