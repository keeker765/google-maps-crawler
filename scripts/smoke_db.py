"""
Lightweight smoke test for DB writes using named parameters.

Usage:
  python scripts/smoke_db.py

Requires only the Python standard library; no browser or pandas.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from gmaps_crawler.storage.db import DB, update_tile_counts  # noqa: E402


def main() -> None:
    db_path = Path("data/db/test_smoke.sqlite")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    db = DB(db_path)

    run_id = "smoke_run"
    city = "Paris"
    country = "France"
    query = "Coffee"

    db.start_run(run_id, city=city, country=country, query=query, zoom=15, language="en")
    db.update_run_meta(
        run_id=run_id,
        window_width_px=1920,
        window_height_px=1080,
        viewport_width_px=1280.0,
        viewport_height_px=720.0,
        mpp=1.23,
        cell_width_km=2.0,
        cell_height_km=1.0,
        overlap_ratio=0.25,
    )

    # init tiles
    db.reset_in_progress(city, query)
    points = []
    for i in range(3):
        points.append((i, 0, i, 48.85, 2.35 + i * 0.01, f"https://maps.example/{i}", 1920, 1080, 1280.0, 720.0))
    db.init_tiles(city, query, points)

    # progress lifecycle
    db.set_tile_in_progress(city, query, tile_index=0, tile_row=0, tile_col=0, lat=48.85, lng=2.35)

    payload_ok = dict(
        place_id="pid-1",
        city=city,
        query=query,
        tile_index=0,
        name="Cafe A",
        href="https://gmaps/a",
        lat=48.851,
        lng=2.351,
        address="addr",
        location="Paris,Ile-de-France,France",
        phone="+33 1 23 45 67 89",
        plus_code="XXXX+XX",
        website="https://a.example",
        social_media_urls='["https://ig.com/a"]',
        open_time="Mon-Fri 8-20",
        emails_phones_socials='{"emails":[{"email":"info@a.fr","source_url":"https://a.example"}]}',
        warnings="",
        extracted_at=None,
        run_id=run_id,
    )
    db.upsert_place_struct(**payload_ok)

    payload_fail = dict(
        place_id="pid-2",
        city=city,
        query=query,
        tile_index=0,
        name="Cafe B",
        href="https://gmaps/b",
        lat=48.852,
        lng=2.352,
        last_error="no address",
        warnings="",
        extracted_at=None,
        run_id=run_id,
    )
    db.upsert_place_failure(**payload_fail)

    # finalize tile
    update_tile_counts(db.conn, city, query, 0)
    db.set_tile_completed(city, query, 0, result_count=2, processed_count=1, failed_count=1)

    # read-back
    print("place pid-1:", db.get_place_by_id("pid-1"))
    print("OK: smoke test finished =>", db_path)


if __name__ == "__main__":
    main()

