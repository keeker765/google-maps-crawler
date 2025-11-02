from __future__ import annotations

import json as _json
import logging
import threading
from typing import Dict, Callable, Tuple

from DrissionPage._pages.chromium_tab import ChromiumTab

from gmaps_crawler.pipeline.utils import _dismiss_consent
from gmaps_crawler.pipeline.extractors import extract_pipeline
from gmaps_crawler.utils.geo_id import parse_lat_lng_from_href, make_place_id_from_latlng
from gmaps_crawler.pipeline.tasks.payloads import (
    build_base_payload,
    build_success_payload,
    build_failure_payload,
)

logger = logging.getLogger(__name__)


class ExtractWorker:
    """Detail page extraction worker.

    Instances are callable and compatible with the original make_extract_worker contract.
    """

    WAIT_TITLE_SECONDS = 8

    def __init__(self, *, browser, tab_pool, run_ctx, tile_ctx, query: str) -> None:
        self.browser = browser
        self.tab_pool = tab_pool
        self.run_ctx = run_ctx
        self.tile_ctx = tile_ctx
        self.query = query

    def __call__(self, info: Dict) -> Dict:
        """Entry point for scheduler. Returns a dict with status/payload."""
        if self.run_ctx is None or self.tile_ctx is None:
            raise RuntimeError("context missing")

        tab: ChromiumTab = self._acquire_tab()
        try:
            name, href, pid = self._safe_name_href_pid(info)
            if not href:
                return {"status": "failed", "payload": {"place_id": pid, "last_error": "empty href"}}

            # Try to dismiss consent first (pre-navigation URL read is a common failure point)
            self._ensure_consent(tab)

            # Navigate to detail page
            self._navigate(tab, href)

            # Best-effort title wait (non-fatal)
            self._wait_title(tab)

            # Run extractor
            data = extract_pipeline(tab, self.browser, city_name=self.run_ctx.city, place_id=pid or info.get("_pid"))
            address = (data.get("address") or "").strip()
            warnings_json = _json.dumps(data.get("warnings") or [], ensure_ascii=False)

            # Ensure coordinates and pid
            lat, lng, pid_final = self._ensure_lat_lng_pid(info, href)

            base = build_base_payload(
                run_ctx=self.run_ctx,
                tile_ctx=self.tile_ctx,
                query=self.query,
                name=name,
                href=href,
                pid=pid_final,
                lat=lat,
                lng=lng,
            )

            if not address:
                logger.warning("[extract][failed] tile=%d pid=%s reason=%s", self.tile_ctx.index, pid_final, "missing address")
                payload = build_failure_payload(base, run_ctx=self.run_ctx, last_error="missing address", warnings_json=warnings_json)
                return {"status": "failed", "payload": payload}

            # Success
            payload = build_success_payload({**base}, data)
            return {"status": "success", "payload": payload}

        except Exception as e:
            # Normalize error to class name
            err = getattr(e, "__class__", type(e)).__name__
            try:
                lat = float(info.get("_lat") or 0.0)
                lng = float(info.get("_lng") or 0.0)
                pid_fallback = str(info.get("_pid") or "")
                name = (info.get("name") or "").strip()
                href = (info.get("href") or "").strip()
            except Exception:
                lat = 0.0
                lng = 0.0
                pid_fallback = ""
                name = str(info.get("name") or "")
                href = str(info.get("href") or "")
            base = build_base_payload(
                run_ctx=self.run_ctx,
                tile_ctx=self.tile_ctx,
                query=self.query,
                name=name,
                href=href,
                pid=pid_fallback,
                lat=lat,
                lng=lng,
            )
            logger.warning(
                "[extract][failed] tile=%d pid=%s reason=%s",
                self.tile_ctx.index,
                base.get("place_id", ""),
                err,
            )
            payload = build_failure_payload(base, run_ctx=self.run_ctx, last_error=err, warnings_json="[]")
            return {"status": "failed", "payload": payload}
        finally:
            self._release_tab(tab)

    # ---- helpers ----
    def _acquire_tab(self) -> ChromiumTab:
        tab = self.tab_pool.acquire()
        return tab

    def _release_tab(self, tab: ChromiumTab) -> None:
        try:
            self.tab_pool.release(tab)
        except Exception as e:
            logger.error("tab_pool.release error: %s", e)

    def _ensure_consent(self, tab: ChromiumTab) -> bool:
        thread_name = threading.current_thread().name
        try:
            # logger.info("[consent][pre-url] thread=%s tab=%r", thread_name, tab)
            _ = tab.url 
        except Exception as e:
            logger.error("[consent][url-read-failed] thread=%s exc=%s", thread_name, getattr(e, "__class__", type(e)).__name__)
            # propagate to let caller handle failed tab
            raise
        try:
            return bool(_dismiss_consent(tab))
        except Exception as e:
            logger.error("dismiss consent failed on detail tab: %s", e)
            return False

    def _navigate(self, tab: ChromiumTab, href: str) -> None:
        tab.get(href)

    def _wait_title(self, tab: ChromiumTab) -> None:
        try:
            tab.wait.ele_displayed('xpath://h1[text() != ""]', timeout=self.WAIT_TITLE_SECONDS)
        except Exception as e:
            logger.error("title wait failed: %s", e)

    def _safe_name_href_pid(self, info: Dict) -> Tuple[str, str, str]:
        name = (info.get("name") or "").strip()
        href = (info.get("href") or "").strip()
        pid = str(info.get("_pid") or "")
        return name, href, pid

    def _ensure_lat_lng_pid(self, info: Dict, href: str) -> Tuple[float, float, str]:
        try:
            lat = float(info.get("_lat")) if info.get("_lat") is not None else None
            lng = float(info.get("_lng")) if info.get("_lng") is not None else None
        except Exception:
            lat = None
            lng = None
        if lat is None or lng is None:
            try:
                lat, lng = parse_lat_lng_from_href(href)
            except Exception:
                lat, lng = 0.0, 0.0
        pid = str(info.get("_pid") or "")
        if not pid and lat is not None and lng is not None:
            pid = make_place_id_from_latlng(lat, lng)
        return float(lat or 0.0), float(lng or 0.0), pid


def make_extract_worker(*, browser, tab_pool, run_ctx, tile_ctx, query) -> Callable[[Dict], Dict]:
    """Factory returning a scheduler-compatible worker callable.

    Keeps the original public API intact while delegating to ExtractWorker.
    """
    worker = ExtractWorker(browser=browser, tab_pool=tab_pool, run_ctx=run_ctx, tile_ctx=tile_ctx, query=query)
    return worker
