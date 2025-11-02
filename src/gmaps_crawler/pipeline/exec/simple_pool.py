from __future__ import annotations

import threading
import queue
from typing import List, Dict, Optional, Tuple

from logger import crawler_thread_logger as logger
from DrissionPage import Chromium
from DrissionPage._pages.chromium_tab import ChromiumTab

from gmaps_crawler.pipeline.utils import _dismiss_consent
from gmaps_crawler.pipeline.extractors import extract_pipeline
from gmaps_crawler.utils.geo_id import parse_lat_lng_from_href, make_place_id_from_latlng
from gmaps_crawler.pipeline.tasks.payloads import (
    build_base_payload,
    build_success_payload,
    build_failure_payload,
)
from gmaps_crawler.storage.db import DB
from gmaps_crawler.pipeline.exec.stop import STOP_EVENT


class TabWorker(threading.Thread):
    def __init__(
        self,
        *,
        browser: Chromium,
        task_queue: "queue.Queue[Optional[Dict]]",
        run_ctx,
        tile_ctx,
        db_path,
        query: str,
        wait_title_seconds: int = 8,
    ) -> None:
        super().__init__(daemon=True)
        self.browser = browser
        self.task_queue = task_queue
        self.run_ctx = run_ctx
        self.tile_ctx = tile_ctx
        self.query = query
        self.wait_title_seconds = wait_title_seconds
        self._inserted = 0
        self._failed = 0
        self.db_path = db_path

    def stats(self) -> Tuple[int, int]:
        return self._inserted, self._failed

    def run(self) -> None:
        db = DB(self.db_path)

        tab: ChromiumTab = self.browser.new_tab(background=True)
        try:
            while not STOP_EVENT.is_set():
                try:
                    item = self.task_queue.get(timeout=0.2)
                except Exception as e:
                    logger.exception("Failed to get task from queue: %s", str(e))
                    continue
                if item is None:
                    self.task_queue.task_done()
                    break
                info: Dict = item
                try:
                    name = (info.get("name") or "").strip()
                    href = (info.get("href") or "").strip()
                    pid = str(info.get("_pid") or "")
                    if not href:
                        base = build_base_payload(run_ctx=self.run_ctx, tile_ctx=self.tile_ctx, query=self.query, name=name, href=href, pid=pid, lat=0.0, lng=0.0)
                        payload = build_failure_payload(base, run_ctx=self.run_ctx, last_error="empty href")
                        db.upsert_place_failure(**payload)
                        self._failed += 1
                        continue
                    _dismiss_consent(tab)
                    tab.get(href)
                    data = extract_pipeline(tab, self.browser, city_name=self.run_ctx.city, place_id=pid or info.get("_pid"))
                    
                    address = (data.get("address") or "").strip()

                    # ensure lat/lng/pid
                    lat = float(info.get("_lat")) if info.get("_lat") is not None else None
                    lng = float(info.get("_lng")) if info.get("_lng") is not None else None
                    
                    if lat is None or lng is None:
                        lat, lng = parse_lat_lng_from_href(href)
                    
                    if not lat or not lng:
                        raise ValueError("missing lat/lng")
                        
                    pid_final = pid or (make_place_id_from_latlng(lat, lng) if (lat and lng) else "")
                    if not pid_final:
                        raise ValueError("missing pid_final")
                    
                    base = build_base_payload(run_ctx=self.run_ctx, tile_ctx=self.tile_ctx, query=self.query, name=name, href=href, pid=pid_final, lat=lat or 0.0, lng=lng or 0.0)
                    if not address:
                        raise ValueError("missing address")
                    else:
                        payload = build_success_payload({**base}, data)
                        db.upsert_place_struct(**payload, extracted_at=None, run_id=self.run_ctx.run_id)
                        # logger.info("Successfully processed place: %s", pid)
                        self._inserted += 1
                        
                except Exception as e:
                    nm = (info.get("name") or "").strip()
                    href = (info.get("href") or "").strip()
                    pid = str(info.get("_pid") or "")
                    base = build_base_payload(run_ctx=self.run_ctx, tile_ctx=self.tile_ctx, query=self.query, name=nm, href=href, pid=pid, lat=0.0, lng=0.0)
                    payload = build_failure_payload(base, run_ctx=self.run_ctx, last_error=getattr(e, "__class__", type(e)).__name__, warnings_json="[]")
                    db.upsert_place_failure(**payload)
                    logger.exception("Failed to process place: %s", pid)
                    self._failed += 1
                finally:
                    # logger.info("Successfully processed place: %s %s %s", self._inserted, self._failed, pid)
                    self.task_queue.task_done()
        finally:
            tab.close()


class TabWorkerPool:
    def __init__(self, *, 
                 browser: Chromium, 
                 run_ctx, 
                 tile_ctx, 
                 db_path, 
                 workers: int = 2):
        self.browser = browser
        self.run_ctx = run_ctx
        self.tile_ctx = tile_ctx
        self.db_path = db_path
        self.workers = max(1, int(workers))
        self._queue: "queue.Queue[Optional[Dict]]" = queue.Queue()
        self._threads: List[TabWorker] = []

    def submit_tasks(self, tasks: List[Dict]) -> None:
        for info in tasks:
            self._queue.put(info)
        for _ in range(self.workers):
            self._queue.put(None)

    def start(self) -> None:
        if self._threads:
            return
        for _ in range(self.workers):
            t = TabWorker(browser=self.browser, 
                          task_queue=self._queue, 
                          run_ctx=self.run_ctx, 
                          tile_ctx=self.tile_ctx, 
                          db_path=self.db_path, 
                          query=self.run_ctx.query if hasattr(self.run_ctx, 'query') else "")
            t.start()
            self._threads.append(t)

    def join(self) -> None:
        for t in self._threads:
            t.join()

    def stats(self) -> Tuple[int, int]:
        inserted = 0
        failed = 0
        for t in self._threads:
            ins, fail = t.stats()
            inserted += ins
            failed += fail
        return inserted, failed
