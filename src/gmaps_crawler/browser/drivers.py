from __future__ import annotations

from logger import crawler_thread_logger as logger
import time
import socket
import tempfile
from pathlib import Path
from typing import Optional, Tuple

from DrissionPage import ChromiumOptions
from DrissionPage._base.chromium import Chromium

IMPLICT_WAIT = 5
# use named crawler logger


def _ensure_download_manager(options: ChromiumOptions) -> None:
    """Ensure the underlying Chromium instance has a download manager."""
    try:
        browser = Chromium(addr_or_opts=options)
        if hasattr(browser, "_dl_mgr"):
            return
        from DrissionPage._units.downloader import DownloadManager  # type: ignore
        browser._dl_mgr = DownloadManager(browser)  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - defensive path
        logger.warning("Failed to initialise download manager fallback: %s", exc)


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def _ensure_userdata_dir(dir_path: Optional[str]) -> str:
    if dir_path:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        return dir_path
    # Default to a unique, ephemeral user data dir per browser instance
    base = Path("data/tmp/userdata")
    base.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix="dp_ud_", dir=str(base)))
    return str(tmp)


def create_browser(
    headless: bool = False,
    window_width: Optional[int] = None,
    window_height: Optional[int] = None,
    proxy: Optional[str] = None,
    retries: int = 3,
    retry_interval: float = 2.0,
    devtools_port: Optional[int] = None,
    user_data_dir: Optional[str] = None,
) -> Chromium:
    options = ChromiumOptions()
    # Always set window-size when provided, regardless of headless, to enforce a stable viewport.
    options.set_argument('--window-size', "1920,1080")
    # Force English UI to stabilize selectors and texts
    try:
        options.set_argument('--lang=en-US')
    except Exception as e:
        logger.warning("Failed to set --lang=en-US: %s", e)
    try:
        # Some versions support setting Chromium preferences directly
        options.set_pref('intl.accept_languages', 'en-US,en')  # type: ignore[attr-defined]
    except Exception as e:
        logger.debug("set_pref intl.accept_languages not supported: %s", e)
    if headless:
        options.headless(True)
        
    # options.no_imgs(True)                     不行
    # options.set_argument('--disable-gpu')         # 别禁用 GPU
    # options.set_user_agent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ...')
    options.set_argument('--disable-blink-features=AutomationControlled')

    # Assign a dedicated DevTools port & user data dir to avoid collisions
    port = devtools_port or _pick_free_port()
    udir = _ensure_userdata_dir(user_data_dir)
    options.set_argument(f"--remote-debugging-port={port}")
    # options.set_argument(f"--user-data-dir={udir}")

    if proxy:
        options.set_argument(f"--proxy-server={proxy}")

    options.set_timeouts(IMPLICT_WAIT)

    logger.debug("Chromium launch with devtools_port=%s user_data_dir=%s", port, udir)
    return Chromium(options)
  
