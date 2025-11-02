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
        conn.execute("ALTER TABLE tiles ADD COLUMN failed_count INTEGER DEFAULT 0")
    except Exception:
        pass
    conn.commit()


def backfill(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    # processed_count = success count from places
    cur.execute(
        """
        UPDATE tiles SET
            processed_count = (
                SELECT COUNT(1) FROM places p
                WHERE p.city=tiles.city AND p.query=tiles.query AND p.tile_index=tiles.tile_index
                  AND (p.status IS NULL OR p.status='' OR p.status='success')
            ),
            failed_count = (
                SELECT COUNT(1) FROM places p
                WHERE p.city=tiles.city AND p.query=tiles.query AND p.tile_index=tiles.tile_index
                  AND p.status='failed'
            )
        """
    )
    conn.commit()


def run(paths: Iterable[Path]) -> None:
    any_done = False
    for p in paths:
        if not p.exists():
            continue
        try:
            conn = sqlite3.connect(str(p))
        except Exception as e:
            print(f"[backfill-tiles] Skip {p} (open error: {e})")
            continue
        try:
            ensure_columns(conn)
            backfill(conn)
            print(f"[backfill-tiles] Updated tiles counts from places for {p}")
            any_done = True
        finally:
            try:
                conn.close()
            except Exception:
                pass
    if not any_done:
        print("[backfill-tiles] No candidate DB found. Specify path: python src/gmaps_crawler/tools/backfill_tiles_counts.py <db_path>")


if __name__ == "__main__":
    args = [Path(a) for a in sys.argv[1:]]
    paths = args if args else DEFAULT_CANDIDATES
    run(paths)

