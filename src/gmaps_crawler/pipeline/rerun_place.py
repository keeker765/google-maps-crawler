from __future__ import annotations

import json
from typing import List, Any
import time
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from DrissionPage._pages.chromium_tab import ChromiumTab

from logger import crawler_thread_logger as logger
from gmaps_crawler.browser.drivers import create_browser
from gmaps_crawler.pipeline.utils import _dismiss_consent
from gmaps_crawler.pipeline.extractors import extract_pipeline
from gmaps_crawler.storage.db import DB


# use named crawler logger


@dataclass
class RerunResult:
    place_id: str
    status: str
    tile_index: Optional[int]
    warnings: list
    last_error: str
    timings_ms: Dict[str, int]


def rerun_place(
    place_id: str,
    *,
    db_path: Path = Path("data/db/gmaps.sqlite"),
    headless: bool = True,
) -> Dict[str, Any]:
    """Re-extract a single place by place_id and update DB and tile counts.

    Fixed params by request:
    - window size forced by drivers to 1920x1080
    - language forced to English implicitly (we do not add non-en params)
    - no proxy usage
    - headless default True
    - no tile recalculation (TODO optional)
    """
    db = DB(db_path)
    row = db.get_place_by_id(place_id)
    if not row:
        raise ValueError(f"place not found for place_id={place_id}")

    href = (row.get("href") or "").strip()
    if not href:
        raise ValueError(f"href missing for place_id={place_id}")

    city = str(row.get("city") or "").strip()
    query = str(row.get("query") or "").strip()
    tile_index = int(row.get("tile_index") or 0)
    name = str(row.get("name") or "").strip()
    lat = float(row.get("lat") or 0.0)
    lng = float(row.get("lng") or 0.0)
    run_id = str(row.get("run_id") or "manual_rerun")

    timings: Dict[str, int] = {}

    t0 = time.monotonic()
    browser = create_browser(headless=headless)
    timings["create_browser_ms"] = int((time.monotonic() - t0) * 1000)

    try:
        t1 = time.monotonic()
        tab: ChromiumTab = browser.new_tab(url=href, background=False)
        try:
            _dismiss_consent(tab)
        except Exception as e:
            # Align with main pipeline: consent issues are non-fatal
            logger.warning("Consent dismiss error (non-fatal): %s", e)
        timings["open_tab_ms"] = int((time.monotonic() - t1) * 1000)

        # Wait title briefly; extractor will fail loudly on missing address
        try:
            tab.wait.ele_displayed('xpath://h1[text() != ""]', timeout=8)
        except Exception as e:
            # Align with main pipeline: missing title is non-fatal; extractor will validate
            logger.warning("Title not visible within timeout (non-fatal): %s", e)

        t2 = time.monotonic()
        data = extract_pipeline(tab, browser, city_name=city, place_id=place_id)
        address = (data.get("address") or "").strip()
        # Only use 'warnings' per unified contract
        warnings_json = json.dumps(data.get("warnings") or [], ensure_ascii=False)
        timings["extract_ms"] = int((time.monotonic() - t2) * 1000)

        if not address:
            # write failed row
            db.upsert_place_failure(
                place_id=place_id,
                city=city,
                query=query,
                tile_index=tile_index,
                name=name,
                href=href,
                lat=lat,
                lng=lng,
                last_error="missing address",
                warnings=warnings_json,
                extracted_at=None,
                run_id=run_id,
            )
            status = "failed"
            last_error = "missing address"
        else:
            payload = {
                "place_id": place_id,
                "city": city,
                "query": query,
                "tile_index": tile_index,
                "name": name,
                "href": href,
                "lat": lat,
                "lng": lng,
                "address": address,
                "location": str(data.get("location") or ""),
                "phone": str(data.get("phone") or ""),
                "plus_code": str(data.get("plus_code") or ""),
                "website": str(data.get("website") or ""),
                "social_media_urls": json.dumps(data.get("social_media_urls") or [], ensure_ascii=False),
                "open_time": str(data.get("open_time") or ""),
                "emails_phones_socials": json.dumps(data.get("emails_phones_socials") or {}, ensure_ascii=False),
                "warnings": warnings_json,
            }
            t3 = time.monotonic()
            db.upsert_place_struct(**payload, extracted_at=None, run_id=run_id)
            timings["upsert_ms"] = int((time.monotonic() - t3) * 1000)
            status = "success"
            last_error = ""

        # refresh tile counts (no tile recalculation)
        t4 = time.monotonic()
        db.update_tile_counts(city, query, tile_index)
        timings["update_tile_ms"] = int((time.monotonic() - t4) * 1000)

        res = RerunResult(
            place_id=place_id,
            status=status,
            tile_index=tile_index,
            warnings=json.loads(warnings_json) if warnings_json else [],
            last_error=last_error,
            timings_ms=timings,
        )
        return {
            "place_id": res.place_id,
            "status": res.status,
            "tile_index": res.tile_index,
            "warnings": res.warnings,
            "last_error": res.last_error,
            "timings_ms": res.timings_ms,
        }
    finally:
        browser.quit(timeout=5, force=True, del_data=False)

def _select_failed_place_ids(db_path: Path, 
                             *, 
                             city: Optional[str] = None, 
                             query: Optional[str] = None, 
                             limit: Optional[int] = None, 
                             only_errors: Optional[List[str]] = None) -> List[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        sql = (
            "SELECT place_id FROM places WHERE status='failed'"
        )
        params: List[Any] = []
        if city:
            sql += " AND city=?"; params.append(city)
        if query:
            sql += " AND query=?"; params.append(query)
        if only_errors:
            ph = ",".join(["?"] * len(only_errors))
            sql += f" AND last_error IN ({ph})"; params.extend(list(only_errors))
        sql += " ORDER BY extracted_at DESC"
        if limit and limit > 0:
            sql += f" LIMIT {int(limit)}"
        rows = conn.execute(sql, params).fetchall()
        return [str(r[0]) for r in rows]
    finally:
        conn.close()


def rerun_failed_places(
    *,
    db_path: Path = Path("data/db/gmaps.sqlite"),
    headless: bool = True,
    workers: int = 2,
    city: Optional[str] = "France",
    query: Optional[str] = "Coffee Store",
    limit: Optional[int] = None,
    only_errors: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Rerun all failed places (optionally filtered) with a thread pool.

    Returns summary dict: {selected, attempted, succeeded, failed}
    """
    pids = _select_failed_place_ids(db_path, city=city, query=query, limit=limit, only_errors=only_errors)
    selected = len(pids)
    if selected == 0:
        return {"selected": 0, "attempted": 0, "succeeded": 0, "failed": 0}

    attempted = 0
    succeeded = 0
    failed = 0
    results: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max(1, int(workers or 1))) as ex:
        fut_map = {ex.submit(rerun_place, pid, db_path=db_path, headless=headless): pid for pid in pids}
        for fut in as_completed(fut_map):
            pid = fut_map[fut]
            try:
                out = fut.result()
                attempted += 1
                if (out.get("status") or "") == "success":
                    succeeded += 1
                else:
                    failed += 1
                results.append(out)
            except Exception as e:
                attempted += 1
                failed += 1
                results.append({"place_id": pid, "status": "failed", "last_error": str(e)})

    return {"selected": selected, "attempted": attempted, "succeeded": succeeded, "failed": failed, "results": results}


if __name__ == "__main__":
    # out = rerun_place("2d80530d-100d-5eca-96e5-046169ab9273")
    # print(out)
    rerun_failed_places()




