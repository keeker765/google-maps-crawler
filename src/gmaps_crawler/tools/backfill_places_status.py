from __future__ import annotations

import sys
import sqlite3
from pathlib import Path
from typing import Iterable


DEFAULT_CANDIDATES = [
    Path("data/db/gmaps.sqlite"),
    Path("src/data/db/gmaps.sqlite"),
]


def ensure_columns(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ALTER TABLE places ADD COLUMN status TEXT")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE places ADD COLUMN last_error TEXT")
    except Exception:
        pass
    conn.commit()


def backfill(conn: sqlite3.Connection) -> tuple[int, int]:
    cur = conn.cursor()
    cur.execute("UPDATE places SET status='success' WHERE status IS NULL OR TRIM(status)='' ")
    n1 = cur.rowcount if cur.rowcount is not None else 0
    cur.execute("UPDATE places SET last_error='' WHERE last_error IS NULL")
    n2 = cur.rowcount if cur.rowcount is not None else 0
    conn.commit()
    return n1, n2


def run(paths: Iterable[Path]) -> None:
    any_done = False
    for p in paths:
        if not p.exists():
            continue
        try:
            conn = sqlite3.connect(str(p))
        except Exception as e:
            print(f"[backfill] Skip {p} (open error: {e})")
            continue
        try:
            ensure_columns(conn)
            n1, n2 = backfill(conn)
            print(f"[backfill] {p} -> status updated: {n1}, last_error updated: {n2}")
            any_done = True
        finally:
            try:
                conn.close()
            except Exception:
                pass
    if not any_done:
        print("[backfill] No candidate DB found. Specify path explicitly: python -m gmaps_crawler.tools.backfill_places_status <db_path>")


if __name__ == "__main__":
    args = [Path(a) for a in sys.argv[1:]]
    paths = args if args else DEFAULT_CANDIDATES
    run(paths)

