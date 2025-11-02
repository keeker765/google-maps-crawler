from __future__ import annotations

import time
from typing import List, Optional
from logger import crawler_thread_logger as logger

from DrissionPage import ChromiumPage
from DrissionPage._elements.chromium_element import ChromiumElement
from gmaps_crawler.utils.errors import ScrollEndNotReachedError
from gmaps_crawler.pipeline.exec.stop import STOP_EVENT
from gmaps_crawler.pipeline.utils import get_places_wrapper

alias =  {
    "coffee":["coffee", "cafe", "expresso bar"]
}

class GMapsNavigator:
    SCROLL_PIXEL_STEP = 600

    def __init__(self, tab: ChromiumPage) -> None:
        self.tab = tab
        self.logger = logger

    def _get_places_wrapper(self, query_text) -> List[ChromiumElement]:
        return get_places_wrapper(self.tab, query_text=query_text)

    def _get_scroll_container(self) -> Optional[ChromiumElement]:
        selectors = [
            "css:div.section-scrollbox",
            "xpath://*[@role='feed' and descendant::a[contains(@href,'/maps/place/')]]",
            "xpath://div[contains(@class,'scroll') and descendant::a[contains(@href,'/maps/place/')]]",
        ]
        for sel in selectors:
            try:
                containers = self.tab.eles(sel) or []
            except Exception as e:
                self.logger.debug("_get_scroll_container selector fail %s: %s", sel, e)
                containers = []
            if containers:
                return containers[0]
        return None
    
    def _scroll_until_start(self, name):
        scroll_selector = f"@aria-label=Results for {name}"
        scroll_container = self.tab.ele(scroll_selector)
        scroll_container.scroll.to_top()

    def _scroll_until_end(self, name: str) -> None:
        scroll_selector = f"@aria-label=Results for {name}"
        scroll_container = self.tab.ele(scroll_selector)
        end_selector = "@text():You've reached the end of the list."
        max_steps = 120
        import random
        reach_ele = None
        # warm up a few scrolls to trigger initial population
        for _ in range(3):
            try:
                scroll_container.scroll(600)
                time.sleep(0.2)
            except Exception as e:
                self.logger.debug("warm-up scroll failed: %s", e)
                break
        for _ in range(max_steps):
            if STOP_EVENT.is_set():
                raise KeyboardInterrupt
            scroll_container.scroll(900)
            time.sleep(random.uniform(0.2, 0.5))
            try:
                reach_ele = scroll_container.ele(end_selector, timeout=0.5)
                if reach_ele:
                    while not reach_ele.states.is_whole_in_viewport:
                        scroll_container.scroll(700)
                        time.sleep(random.uniform(0.2, 0.4))
                    break
            except Exception as e:
                self.logger.debug("end-of-list probe failed: %s", e)
                continue
        if not reach_ele:
            raise ScrollEndNotReachedError("End-of-list not reached within max scroll attempts.")
