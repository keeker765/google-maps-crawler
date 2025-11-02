from __future__ import annotations

import time
from typing import Optional

from DrissionPage import Chromium
from DrissionPage._pages.chromium_tab import ChromiumTab

from gmaps_crawler.browser.drivers import create_browser
from gmaps_crawler.pipeline.utils import _dismiss_consent, local_search_click


class BrowserSession:
    def open_browser(self, *, headless: bool, window_width: Optional[int], window_height: Optional[int], proxy: Optional[str]) -> Chromium:
        return create_browser(headless=headless, window_width=window_width, window_height=window_height, proxy=proxy)

    def open_search_tab(self, browser: Chromium, url: str) -> ChromiumTab:
        return browser.new_tab(url=url, background=False)

    def ensure_consent(self, tab: ChromiumTab, attempts: int = 3) -> None:
        for _ in range(max(1, attempts)):
            if _dismiss_consent(tab):
                return
            time.sleep(0.2)
        raise RuntimeError("consent dismiss failed")

    def ensure_local_search(self, tab: ChromiumTab, attempts: int = 5) -> None:
        for _ in range(max(1, attempts)):
            if local_search_click(tab):
                return
        raise RuntimeError(f"local search click failed: url={tab.url}")

