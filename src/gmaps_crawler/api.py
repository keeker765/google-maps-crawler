from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence, Iterable, Dict, Any, List, Tuple
import sqlite3
from logger import main_thread_logger as logger  # use project named logger

from gmaps_crawler.pipeline.city.crawl_city import crawl_city
from gmaps_crawler.pipeline.rerun_place import rerun_place as _rerun_place; from gmaps_crawler.pipeline.rerun_place import rerun_failed_places as _rerun_failed_places
from gmaps_crawler.pipeline.exec.scheduler import ProgressiveTaskScheduler, ProgressiveTaskManager
from gmaps_crawler.pipeline.exec.stop import STOP_EVENT


def run_city(
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
    limit: Optional[int] = None,
    proxy: Optional[str] = None,
    proxy_list: Optional[str] = None,
    proxy_file: Optional[str] = None,
    proxy_sources: Optional[Sequence[str]] = None,
    proxy_strategy: str = "round_robin",
    db_path: Path = Path("data/db/gmaps.sqlite"),
    csv_path: Path = Path("data/places.csv"),
    html_root: Path = Path("data/html"),
    ) -> None:
    """Programmatic entry point for a DB-backed Google Maps city crawl.

    This mirrors the CLI behavior but is intended for direct import.

    Example:
        from gmaps_crawler.api import run_city
        run_city("Paris", "coffee shops in Paris", headless=True)
    """
    # Hard-code coverage probe parameters internally; no need to pass
    crawl_city(
        city,
        query,
        country=country,
        language=language,
        zoom=zoom,
        headless=headless,
        print_coverage=print_coverage,
        verbose=verbose,
        sleep=sleep,
        limit=limit,
        proxy=proxy,
        proxy_list=proxy_list,
        proxy_file=proxy_file,
        proxy_sources=proxy_sources,
        proxy_strategy=proxy_strategy,
        db_path=db_path,
        csv_path=csv_path,
        html_root=html_root,
    )


def rerun_place(place_id: str, *, db_path: Path = Path("data/db/gmaps.sqlite"), headless: bool = True) -> dict:
    """Programmatic entry to re-extract a single place by place_id.

    Fixed parameters: 1920x1080 window, language=en, no proxy. Headless default True.
    """
    return _rerun_place(place_id, db_path=db_path, headless=headless)


# def retry_failed_places(
#     city: str,
#     query: str,
#     *,
#     db_path: Path = Path("data/db/gmaps.sqlite"),
#     headless: bool = True,
#     workers: int = 2,
#     max_total: Optional[int] = None,
#     only_errors: Optional[Sequence[str]] = None,
# ) -> Dict[str, Any]:
#     """
#     閲嶈瘯鍩庡競鍐呭け璐ョ殑 place 璁板綍锛坰tatus='failed'锛夈€?
#     - 澶嶇敤鐜版湁鐨勫崟鏉￠噸璺戦€昏緫锛坧ipeline.rerun_place.rerun_place锛夈€?    - 浣跨敤娓愯繘寮忚皟搴﹀櫒杩涜灏忓苟鍙戦噸璇曪紝閬靛惊 STOP_EVENT銆?    - 姣忔潯閲嶈窇鍚庯紝rerun_place 鍐呴儴浼氭洿鏂板搴?tile 鐨勭粺璁°€?
#     杩斿洖锛歿"attempted": int, "succeeded": int, "failed": int, "selected": int}
#     """
#     # use named logger for API layer

#     conn = sqlite3.connect(str(db_path))
#     try:
#         sql = (
#             "SELECT place_id, tile_index, last_error FROM places "
#             "WHERE city=? AND query=? AND status='failed'"
#         )
#         params: List[Any] = [city, query]
#         if only_errors:
#             placeholders = ",".join(["?"] * len(only_errors))
#             sql += f" AND last_error IN ({placeholders})"
#             params.extend(list(only_errors))
#         sql += " ORDER BY extracted_at DESC"
#         if max_total and max_total > 0:
#             sql += f" LIMIT {int(max_total)}"
#         cur = conn.execute(sql, params)
#         rows = cur.fetchall()
#         place_ids: List[str] = [str(r[0]) for r in rows]
#     finally:
#         conn.close()

#     selected = len(place_ids)
#     if selected == 0:
#         return {"attempted": 0, "succeeded": 0, "failed": 0, "selected": 0}

#     # 鍑嗗浠诲姟
#     def _do_rerun(pid: str) -> Tuple[str, str]:
#         if STOP_EVENT.is_set():
#             return pid, "stopped"
#         try:
#             res = _rerun_place(pid, db_path=db_path, headless=headless)
#             return pid, str(res.get("status") or "unknown")
#         except Exception:
#             return pid, "failed"

#     tasks = [(_do_rerun, (pid,)) for pid in place_ids]

#     scheduler = ProgressiveTaskScheduler(max_workers=max(1, int(workers)))
#     manager = ProgressiveTaskManager(scheduler)
#     results = manager.execute_tasks(tasks, timeout=None)

#     attempted = 0
#     succeeded = 0
#     failed = 0
#     for item in results:
#         if not item:
#             continue
#         attempted += 1
#         _pid, status = item
#         if status == "success":
#             succeeded += 1
#         elif status == "stopped":
#             # 涓嶈鍏ュけ璐?            pass
#         else:
#             failed += 1

#     return {"attempted": attempted, "succeeded": succeeded, "failed": failed, "selected": selected}


if __name__ == "__main__":
    import argparse
    # 浣跨敤椤圭洰鑷甫 logger 閰嶇疆锛堝鍏ュ嵆鐢熸晥锛?    import logger  # noqa: F401

    parser = argparse.ArgumentParser(description="gmaps_crawler.api smoke entry")
    parser.add_argument("mode", choices=["crawl", "retry"], nargs="?", default="retry")
    parser.add_argument("--city", default="Paris")
    parser.add_argument("--query", default="Coffee Store")
    parser.add_argument("--headless", action="store_true", help="Run headless (default: True)")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--max-total", type=int)
    args = parser.parse_args()

    city = args.city
    query = args.query
    headless = True if not hasattr(args, "headless") else (args.headless or True)


    if args.mode == "crawl":
        print(f"[api] Running crawl smoke: city={city} query={query}")
        run_city(
            city,
            query,
            language="en",
            zoom=15,
            headless=False,
            print_coverage=True,
            verbose=True,
        )
    else:
        print(f"[api] Running retry_failed smoke: city={city} query={query}")
        summary = retry_failed_places(
            city,
            query,
            db_path=Path("data/db/gmaps.sqlite"),
            headless=False,
            workers=max(1, int(args.workers or 10)),
            max_total=args.max_total,
        )
        print("summary:", summary)


# Thin wrapper to delegate retry to pipeline.rerun_place (batch mode)
from pathlib import Path as _PathAlias
from typing import Optional as _OptAlias, Sequence as _SeqAlias, Dict as _DictAlias, Any as _AnyAlias

def retry_failed_places(
    city: str,
    query: str,
    *,
    db_path: _PathAlias = _PathAlias("data/db/gmaps.sqlite"),
    headless: bool = True,
    workers: int = 2,
    max_total: _OptAlias[int] = None,
    only_errors: _OptAlias[_SeqAlias[str]] = None,
) -> _DictAlias[str, _AnyAlias]:
    from gmaps_crawler.pipeline.rerun_place import rerun_failed_places as _rfp
    return _rfp(
        db_path=db_path,
        headless=headless,
        workers=workers,
        city=city,
        query=query,
        limit=max_total,
        only_errors=list(only_errors) if only_errors else None,
    )
