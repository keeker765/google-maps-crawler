from __future__ import annotations

import uuid
import traceback
from pathlib import Path
from typing import Optional, Sequence

from logger import main_thread_logger as logger
from gmaps_crawler.geo.bbox import fetch_bounding_box
from gmaps_crawler.pipeline.city.grid import generate_grid_points, GridPoint
from gmaps_crawler.pipeline.city.context import RunContext, TileContext
from gmaps_crawler.pipeline.tile.runner import TileRunner
from gmaps_crawler.pipeline.exec.stop import STOP_EVENT
from gmaps_crawler.pipeline.search.urls import build_search_url
from gmaps_crawler.storage.db import DB
from gmaps_crawler.browser.drivers import create_browser
from gmaps_crawler.browser.coverage import measure_map_coverage


def crawl_city(
    city: str,
    query: str,
    *,
    country: Optional[str] = None,
    language: str = "en",
    zoom: int = 16,
    headless: bool = False,
    print_coverage: bool = True,
    verbose: bool = False,
    sleep: float = 5.0,
    proxy: Optional[str] = None,
    proxy_list: Optional[str] = None,
    proxy_file: Optional[str] = None,
    proxy_sources: Optional[Sequence[str]] = None,
    proxy_strategy: str = "round_robin",
    cell_width_km: Optional[float] = None,
    cell_height_km: Optional[float] = None,
    workers: int = 1,
    # 渐进式调度器参数
    thread_startup_delay: Optional[float] = None,
    thread_batch_size: Optional[int] = None,
    thread_batch_delay: Optional[float] = None,
    db_path: Path = Path("data/db/gmaps.sqlite"),
    csv_path: Path = Path("data/places.csv"),
    html_root: Path = Path("data/html"),
    # 爬取完成后是否自动重试失败的 place
    retry_failed: bool = True,
    retry_workers: int = 2,
    retry_max_total: Optional[int] = None,
    retry_only_errors: Optional[Sequence[str]] = None,
) -> None:
    # Enforce fixed resolution regardless of external args
    window_width = 1920
    window_height = 1080
    # Hard-code coverage probe parameters
    coverage_wait = 3.0
    coverage_attempts = 5
    coverage_interval = 0.5
    # Prepare DB and run context
    db = DB(db_path)
    run_id = uuid.uuid4().hex
    db.start_run(run_id, city=city, country=country, query=query, zoom=zoom, language=language)

    # Prepare viewport defaults and try reusing existing tiles first
    vp_w_px = float(window_width)
    vp_h_px = float(window_height)
    points_with_url: list[tuple[GridPoint, str]] = []
    tiles_rows = db.list_tiles(city, query)
    if tiles_rows:
        # Reuse existing tiles (skip coverage & grid generation); backfill missing URLs
        db.reset_in_progress(city, query)
        for row in tiles_rows:
            gp = GridPoint(
                index=int(row["tile_index"]),
                latitude=float(row["tile_center_lat"]),
                longitude=float(row["tile_center_lng"]),
                row=int(row["tile_row"]),
                col=int(row["tile_col"]),
            )
            url = row.get("tile_url") or ""
            if not url:
                url = build_search_url(query, gp.latitude, gp.longitude, zoom, language)
                try:
                    db.update_tile_url(city, query, gp.index, url)
                except Exception:
                    pass
            points_with_url.append((gp, url))
        # Try to reuse recorded viewport px from first tile
        try:
            r0 = tiles_rows[0]
            if r0.get("viewport_width_px") and r0.get("viewport_height_px"):
                vp_w_px = float(r0.get("viewport_width_px") or vp_w_px)
                vp_h_px = float(r0.get("viewport_height_px") or vp_h_px)
        except Exception:
            pass
    else:
        # bbox + dynamic cell size (from actual viewport coverage if not provided)
        bbox = fetch_bounding_box(city, country=country)
        used_cell_w = cell_width_km
        used_cell_h = cell_height_km
        if used_cell_w is None or used_cell_h is None:
            # Measure coverage at bbox center with current zoom + window to derive cell size
            center_lat = (bbox.min_lat + bbox.max_lat) / 2.0
            center_lng = (bbox.min_lon + bbox.max_lon) / 2.0
            browser = create_browser(headless=headless, window_width=window_width, window_height=window_height, proxy=proxy)
            try:
                url = build_search_url(query, center_lat, center_lng, zoom, language)
                tab = browser.new_tab(url=url, background=False)
                cov = measure_map_coverage(tab, attempts=max(1, coverage_attempts), interval=max(0.2, coverage_interval), wait_before=max(0.0, coverage_wait))
                if cov:
                    # Derive cell size from actual viewport coverage with a small safety factor
                    used_cell_w = (cov.viewport_width_m / 1000.0) * 0.9
                    used_cell_h = (cov.viewport_height_m / 1000.0) * 0.9
                    vp_w_px = float(cov.viewport_width_px)
                    vp_h_px = float(cov.viewport_height_px)
                    mpp = float(cov.meters_per_pixel)
                else:
                    # Fallback to approximate formula if coverage failed
                    import math
                    mpp = 156543.03392 * math.cos(math.radians(center_lat)) / (2 ** zoom)
                    used_cell_w = (mpp * float(window_width) / 1000.0) * 0.9
                    used_cell_h = (mpp * float(window_height) / 1000.0) * 0.9
            finally:
                try:
                    browser.quit(timeout=5, force=True, del_data=False)
                except Exception as e:
                    logger.warning("browser quit failed after coverage probe: %s", e)

        # Safety factor to reduce edge artifacts; overlap still applied below
        used_cell_w = float(used_cell_w) if used_cell_w is not None else 3.0
        used_cell_h = float(used_cell_h) if used_cell_h is not None else 1.8
        points = generate_grid_points(bbox, cell_width_km=used_cell_w, cell_height_km=used_cell_h, overlap_ratio=0.25)
        # Precompute tile URLs once per point
        points_with_url = [
            (p, build_search_url(query, float(p.latitude), float(p.longitude), zoom, language))
            for p in points
        ]

        # init tiles and reset in_progress
        db.reset_in_progress(city, query)
        db.init_tiles(
            city,
            query,
            [
                (
                    int(p.index), int(p.row), int(p.col),
                    float(p.latitude), float(p.longitude), url,
                    int(window_width), int(window_height), float(vp_w_px), float(vp_h_px)
                )
                for (p, url) in points_with_url
            ],
        )
        # persist run meta
        try:
            db.update_run_meta(
                run_id=run_id,
                window_width_px=window_width,
                window_height_px=window_height,
                viewport_width_px=vp_w_px,
                viewport_height_px=vp_h_px,
                mpp=float(mpp),
                cell_width_km=used_cell_w,
                cell_height_km=used_cell_h,
                overlap_ratio=0.25,
            )
        except Exception as e:
            logger.warning("update_run_meta failed: %s", e)

    # City-level tiles summary (before iterating)
    try:
        tiles_rows_summary = db.list_tiles(city, query)
        _counts = {"pending": 0, "in_progress": 0, "failed": 0, "completed": 0, "other": 0}
        for _r in tiles_rows_summary:
            _st = str(_r.get("status") or "")
            if _st in _counts:
                _counts[_st] += 1
            else:
                _counts["other"] += 1
        _total = len(tiles_rows_summary)
        _to_run = sum(1 for _r in tiles_rows_summary if (_r.get("status") or "") != "completed")
        logger.info(
            "[city] tiles total=%d pending=%d in_progress=%d failed=%d completed=%d to_run=%d",
            _total,
            _counts["pending"],
            _counts["in_progress"],
            _counts["failed"],
            _counts["completed"],
            _to_run,
        )
    except Exception as e:
        logger.warning("[city] tiles summary failed: %s", e)

    # Build explicit run context for the crawler
    run_ctx = RunContext(
        city=city,
        query=query,
        country=country,
        zoom=zoom,
        language=language,
        run_id=run_id,
        csv_path=csv_path,
        html_root=html_root,
        db=db,
    )

    processed = 0
    # Prepare a persistent process pool for cross-tile reuse when workers>1
    executor = None
    if workers and workers > 1:
        from concurrent.futures import ProcessPoolExecutor
        executor = ProcessPoolExecutor(max_workers=workers)
    for (p, tile_url) in points_with_url:
        if STOP_EVENT.is_set():
            break
        # Skip completed tiles
        status = db.get_tile_status(city, query, int(p.index))
        # logger.info(
        #     "[tile %d] status=%s center=(%.6f,%.6f)",
        #     int(p.index), status or "", float(p.latitude), float(p.longitude)
        # )
        if status == "completed":
            processed += 1
            continue

        # Mark in progress
        db.set_tile_in_progress(
            city,
            query,
            tile_index=int(p.index),
            tile_row=int(p.row),
            tile_col=int(p.col),
            lat=float(p.latitude),
            lng=float(p.longitude),
        )

        # Build tile context
        tile_ctx = TileContext(
            index=int(p.index),
            row=int(p.row),
            col=int(p.col),
            center_lat=float(p.latitude),
            center_lng=float(p.longitude),
            tile_url=tile_url,
        )

        try:
            runner = TileRunner(
                query=query,
                latitude=float(p.latitude),
                longitude=float(p.longitude),
                zoom=zoom,
                language=language,
                headless=headless,
                window_width=window_width,
                window_height=window_height,
                print_coverage=print_coverage,
                coverage_wait=coverage_wait,
                coverage_attempts=coverage_attempts,
                coverage_interval=coverage_interval,
                proxy=proxy,
                proxy_list=proxy_list,
                proxy_file=proxy_file,
                proxy_sources=proxy_sources,
                proxy_strategy=proxy_strategy,
                verbose=verbose,
                run_ctx=run_ctx,
                tile_ctx=tile_ctx,
                workers=workers,
                db_path=db_path,
                executor=executor,
                thread_startup_delay=thread_startup_delay,
                thread_batch_size=thread_batch_size,
                thread_batch_delay=thread_batch_delay,
            )
            seen_count, new_count, failed_count = runner.run()
            db.set_tile_completed(city, query, int(p.index), result_count=seen_count or 0, processed_count=new_count or 0, failed_count=failed_count or 0)
        except KeyboardInterrupt:
            STOP_EVENT.set()
            break
        except Exception as exc:
            traceback.print_exc()
            db.set_tile_failed(city, query, int(p.index), f"tile failed: {exc}")
            # continue to next tile instead of stopping the whole run
            continue
    # shutdown persistent pool
    if executor is not None:
        try:
            executor.shutdown(wait=True, cancel_futures=True)
        except Exception as e:
            logger.warning("executor shutdown failed: %s", e)

    # 可选：在城市级抓取完成后，自动重试失败的 place 记录
    if retry_failed and not STOP_EVENT.is_set():
        try:
            # 延迟导入以避免循环依赖
            from gmaps_crawler.api import retry_failed_places  # type: ignore

            summary = retry_failed_places(
                city,
                query,
                db_path=db_path,
                headless=headless,
                workers=max(1, int(retry_workers or 1)),
                max_total=retry_max_total,
                only_errors=tuple(retry_only_errors) if retry_only_errors else None,
            )
            logger.info(
                "[auto-retry] city=%s query=%s selected=%s attempted=%s succeeded=%s failed=%s",
                city,
                query,
                summary.get("selected", 0),
                summary.get("attempted", 0),
                summary.get("succeeded", 0),
                summary.get("failed", 0),
            )
        except Exception as e:
            logger.warning("[auto-retry] failed to run: %s", e)


if __name__ == "__main__":  # simple smoke test
    print("[crawl_city] Running smoke test for Paris coffee shops…")
    try:
        crawl_city(
            city="Budapest",
            query="Coffee Store",
            country="",
            language="en",
            zoom=15,
            headless=False,
            print_coverage=True,
            verbose=True,
            sleep=3.0,
            workers=10,
        )
        print("[crawl_city] Smoke test finished.")
    except Exception as e:
        import traceback
        print("[crawl_city] Smoke test failed:", e)
        traceback.print_exc()
