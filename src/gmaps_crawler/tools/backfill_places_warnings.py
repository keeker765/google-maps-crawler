from __future__ import annotations

import sys
import sqlite3
from pathlib import Path
from typing import Iterable


DEFAULT_CANDIDATES = [
    Path("data/db/gmaps.sqlite"),
    Path("src/data/db/gmaps.sqlite"),
]


def ensure_warnings(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ALTER TABLE places ADD COLUMN warnings TEXT")
    except Exception:
        pass
    conn.commit()


def backfill(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute("UPDATE places SET warnings='[]' WHERE warnings IS NULL OR TRIM(warnings)='' ")
    n = cur.rowcount if cur.rowcount is not None else 0
    conn.commit()
    return n


def run(paths: Iterable[Path]) -> None:
    any_done = False
    for p in paths:
        if not p.exists():
            continue
        try:
            conn = sqlite3.connect(str(p))
        except Exception as e:
            print(f"[backfill-warnings] Skip {p} (open error: {e})")
            continue
        try:
            ensure_warnings(conn)
            n = backfill(conn)
            print(f"[backfill-warnings] {p} -> warnings backfilled: {n}")
            any_done = True
        finally:
            try:
                conn.close()
            except Exception:
                pass
    if not any_done:
        print("[backfill-warnings] No candidate DB found. Specify: python -m gmaps_crawler.tools.backfill_places_warnings <db_path>")


if __name__ == "__main__":
    args = [Path(a) for a in sys.argv[1:]]
    paths = args if args else DEFAULT_CANDIDATES
    run(paths)

