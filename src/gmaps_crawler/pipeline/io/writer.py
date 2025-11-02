from __future__ import annotations

import queue
import threading
from typing import Dict, Optional

from logger import writer_thread_logger as logger
from gmaps_crawler.config import settings



class ResultWriter:
    def __init__(self, *, db_path, run_ctx, tile_ctx, every: Optional[int] = None) -> None:
        self.db_path = db_path
        self.run_ctx = run_ctx
        self.tile_ctx = tile_ctx
        self.every = 10
        self._q: "queue.Queue[dict]" = queue.Queue()
        self._t: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._inserted = 0
        self._failed = 0

    def start(self) -> None:
        if self._t is not None:
            return
        self._t = threading.Thread(target=self._loop, name=f"WriterTile{self.tile_ctx.index}", daemon=True)
        self._t.start()
        logger.info("[writer][tile %d] started (every=%d)", self.tile_ctx.index, self.every)

    def feed(self, item: dict) -> None:
        self._q.put(item)

    def stop(self) -> None:
        self._q.put(None)  # type: ignore[arg-type]
        self._q.join()
        if self._t:
            self._t.join(timeout=5.0)
        logger.info("[writer][tile %d] stopped inserted=%d failed=%d", self.tile_ctx.index, self._inserted, self._failed)

    def stats(self) -> tuple[int, int]:
        with self._lock:
            return self._inserted, self._failed

    def _loop(self) -> None:
        from gmaps_crawler.storage.db import DB as _DB
        db = _DB(self.db_path)
        while True:
            item = self._q.get()
            if item is None:
                self._q.task_done()
                break
            if not isinstance(item, dict):
                self._q.task_done()
                continue
            status = item.get("status")
            payload = item.get("payload") or {}
            try:
                if status == "success":
                    db.upsert_place_struct(**payload, extracted_at=None, run_id=self.run_ctx.run_id)
                    with self._lock:
                        self._inserted += 1
                        print(f"➕1: {self._inserted}")
                        if self._inserted % self.every == 0:
                            logger.info("[writer][tile %d] inserted=%d", self.tile_ctx.index, self._inserted)
                elif status == "failed":
                    db.upsert_place_failure(**payload)
                    with self._lock:
                        self._failed += 1
            finally:
                self._q.task_done()
