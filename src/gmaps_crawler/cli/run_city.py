from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Optional, Sequence

from gmaps_crawler.pipeline.city.crawl_city import crawl_city
from gmaps_crawler.pipeline.exec.stop import install_signal_handlers


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="DB-backed Google Maps city crawl (endpoint resume + dedupe)."
    )
    parser.add_argument("city", help="City name, e.g. 'Paris'.")
    parser.add_argument("query", help="Full query phrase, e.g. 'coffee shops in Paris'.")
    parser.add_argument("--country", help="Optional country filter for Overpass.")
    parser.add_argument("--language", default="en", help="UI language (default: %(default)s).")
    parser.add_argument("--zoom", type=int, default=16, help="Map zoom (default: %(default)s).")
    parser.add_argument("--headless", action="store_true", help="Run Chromium headless.")
    parser.add_argument("--print-coverage", action="store_true", help="Measure viewport coverage (fixed params).")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--sleep", type=float, default=5.0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--proxy")
    parser.add_argument("--proxy-list")
    parser.add_argument("--proxy-file")
    parser.add_argument("--proxy-source", action="append")
    parser.add_argument("--proxy-strategy", choices=["round_robin", "random"], default="round_robin")
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel workers for detail extraction (default: %(default)s).")
    parser.add_argument("--db-path", type=Path, default=Path("data/db/gmaps.sqlite"))
    parser.add_argument("--csv-path", type=Path, default=Path("data/places.csv"))
    parser.add_argument("--html-root", type=Path, default=Path("data/html"))
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    # 使用自定义 logger（导入即全局生效）
    import logger  # noqa: F401
    args = parse_args(argv)
    # Enable Ctrl+C graceful stop
    install_signal_handlers()
    crawl_city(
        args.city,
        args.query,
        country=args.country,
        language=args.language,
        zoom=args.zoom,
        headless=args.headless,
        print_coverage=args.print_coverage,
        verbose=args.verbose,
        sleep=args.sleep,
        limit=args.limit,
        proxy=args.proxy,
        proxy_list=args.proxy_list,
        proxy_file=args.proxy_file,
        proxy_sources=args.proxy_source,
        proxy_strategy=args.proxy_strategy,
        workers=args.workers,
        db_path=args.db_path,
        csv_path=args.csv_path,
        html_root=args.html_root,
    )


if __name__ == "__main__":
    main()

