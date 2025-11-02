from __future__ import annotations

from logger import crawler_thread_logger as logger
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from DrissionPage import Chromium

from gmaps_crawler.config import settings
from gmaps_crawler.network.proxy import ProxyPool, parse_proxy_sources
from gmaps_crawler.pipeline.city.context import RunContext, TileContext
from gmaps_crawler.pipeline.search.cards import collect_cards
from gmaps_crawler.pipeline.tile.session import BrowserSession
from gmaps_crawler.pipeline.tile.tab_pool import TabPool  # legacy only
from gmaps_crawler.pipeline.tasks.build import build_tasks
from gmaps_crawler.pipeline.tasks.worker import make_extract_worker  # legacy only
from gmaps_crawler.pipeline.exec.streaming import run_streaming  # legacy only
from gmaps_crawler.pipeline.exec.scheduler import ProgressiveTaskScheduler  # legacy only
from gmaps_crawler.pipeline.exec.stop import STOP_EVENT
from gmaps_crawler.pipeline.search.urls import build_search_url
from gmaps_crawler.utils.time import log_duration
from gmaps_crawler.pipeline.exec.simple_pool import TabWorkerPool


# use named crawler logger


class TileRunner:
    def __init__(
        self,
        *,
        query: str,
        latitude: float,
        longitude: float,
        zoom: int,
        language: str,
        headless: bool,
        window_width: Optional[int],
        window_height: Optional[int],
        print_coverage: bool,
        coverage_wait: float,
        coverage_attempts: int,
        coverage_interval: float,
        proxy: Optional[str],
        proxy_list: Optional[str],
        proxy_file: Optional[str],
        proxy_sources: Optional[Sequence[str]],
        proxy_strategy: str,
        verbose: bool,
        run_ctx: RunContext,
        tile_ctx: TileContext,
        workers: int = 1,
        db_path: Optional[Path] = None,
        executor: Optional[object] = None,
        thread_startup_delay: Optional[float] = None,
        thread_batch_size: Optional[int] = None,
        thread_batch_delay: Optional[float] = None,
        use_simple_executor: bool = True,
    ) -> None:
        self.query = query
        self.latitude = latitude
        self.longitude = longitude
        self.zoom = zoom
        self.language = language
        self.headless = headless
        self.window_width = window_width
        self.window_height = window_height
        self.print_coverage = print_coverage
        self.coverage_wait = coverage_wait
        self.coverage_attempts = coverage_attempts
        self.coverage_interval = coverage_interval
        self.proxy = proxy
        self.proxy_list = proxy_list
        self.proxy_file = proxy_file
        self.proxy_sources = proxy_sources
        self.proxy_strategy = proxy_strategy
        self.verbose = verbose
        self.run_ctx = run_ctx
        self.tile_ctx = tile_ctx
        self.workers = workers
        self.db_path = db_path
        self.executor = executor
        self.thread_startup_delay = thread_startup_delay
        self.thread_batch_size = thread_batch_size
        self.thread_batch_delay = thread_batch_delay
        self.use_simple_executor = use_simple_executor
        self.browser: Optional[Chromium] = None
        self.seen = 0

    def _build_proxy_pool(self) -> Optional[ProxyPool]:
        seen_sources = set()
        sources: List[str] = []

        def add_source(value: Optional[str]) -> None:
            if not value or value in seen_sources:
                return
            seen_sources.add(value)
            sources.append(value)

        if self.proxy_sources:
            for item in self.proxy_sources:
                add_source(item)

        parsed_sources = parse_proxy_sources(
            proxy_string=self.proxy_list if self.proxy_list is not None else settings.PROXY_LIST,
            file_path=self.proxy_file if self.proxy_file is not None else (settings.PROXY_FILE or None),
        )
        for item in parsed_sources:
            add_source(item)

        add_source(self.proxy)
        return ProxyPool(sources, strategy=self.proxy_strategy) if sources else None

    def run(self) -> Tuple[int, int, int]:
        logger.info("[bold yellow]\n============== * Running Gmaps Crawler =============[/]", extra={"markup": True})
        payload = settings.model_dump() if hasattr(settings, "model_dump") else settings.dict()
        # logger.info("[yellow]Settings:[/yellow] %s", payload, extra={"markup": True})
        logger.info(
            "Search query: %s \nlang=%s long=%s",
            self.query, self.latitude, self.longitude
        )

        proxy_pool = self._build_proxy_pool()
        selected_proxy = proxy_pool.next_proxy() if proxy_pool else None
        if selected_proxy:
            logger.info("Using proxy: %s", selected_proxy)

        session = BrowserSession()
        with log_duration(logger, "create_browser"):
            self.browser = session.open_browser(
                headless=self.headless,
                window_width=self.window_width,
                window_height=self.window_height,
                proxy=selected_proxy,
            )
        assert self.browser is not None

        search_url = getattr(self.tile_ctx, "tile_url", None) or build_search_url(self.query, self.latitude, self.longitude, self.zoom, self.language)
        logger.info(f"Navigating to search URL, tile idx is {self.tile_ctx.index}\nURL: {search_url}")
        with log_duration(logger, "open_search_tab"):
            search_tab = session.open_search_tab(self.browser, search_url)
        # if self.print_coverage:
        #     logger.info("Coverage already measured at run start; reuse cached.")

        # consent
        session.ensure_consent(search_tab, attempts=3)
        # search this area
        session.ensure_local_search(search_tab, attempts=5)
        search_tab.wait(2)
        
        if search_tab.wait.eles_loaded("@text():No results found"):
            return 0, 0, 0

        with log_duration(logger, "scroll_and_collect"):
            cards = collect_cards(self.browser, search_tab, self.query)
        self.seen = len(cards)
        logger.info("Collect cards done, count=%d", self.seen)

        tasks = build_tasks(cards, city=self.run_ctx.city, query=self.query, db=self.run_ctx.db)
        total = len(tasks)
        # Always use simple tab worker pool that writes directly to DB
        logger.info("Using tab worker pool: tasks=%d cards_total=%d workers=%d", total, len(cards), self.workers)
        pool = TabWorkerPool(
            browser=self.browser,
            run_ctx=self.run_ctx,
            tile_ctx=self.tile_ctx,
            db_path=self.db_path,
            workers=self.workers,
        )
        pool.submit_tasks(tasks)
        pool.start()
        pool.join()
        inserted, failed_count = pool.stats()
        thread_done = inserted + failed_count

        logger.info(
            "[tile %d] summary: scheduled=%d thread_done=%d inserted=%d failed=%d",
            self.tile_ctx.index,
            total,
            thread_done,
            inserted,
            failed_count,
        )

        # teardown
        # simple pool creates and closes tabs in workers; nothing to close here
        self.browser.quit(timeout=5, force=True, del_data=False)
        return self.seen, inserted, failed_count
