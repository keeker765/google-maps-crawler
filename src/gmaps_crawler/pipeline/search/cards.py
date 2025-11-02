from __future__ import annotations

from typing import Dict, List
from DrissionPage import Chromium
from DrissionPage._pages.chromium_tab import ChromiumTab

from gmaps_crawler.pipeline.search.places_crawler import GMapsPlacesCrawler


def collect_cards(browser: Chromium, search_tab: ChromiumTab, query: str) -> List[Dict]:
    crawler = GMapsPlacesCrawler(browser=browser, search_tab=search_tab, proxy_pool=None)
    return crawler._gather_all_cards(query_text=query)

