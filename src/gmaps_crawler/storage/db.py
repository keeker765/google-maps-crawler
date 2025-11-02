import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from typing import Iterable, Optional


def get_connection(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            city TEXT NOT NULL,
            country TEXT,
            query TEXT NOT NULL,
            zoom INTEGER NOT NULL,
            language TEXT NOT NULL,
            window_width_px INTEGER,
            window_height_px INTEGER,
            viewport_width_px REAL,
            viewport_height_px REAL,
            mpp REAL,
            cell_width_km REAL,
            cell_height_km REAL,
            overlap_ratio REAL
        );

        CREATE TABLE IF NOT EXISTS tiles (
            city TEXT NOT NULL,
            query TEXT NOT NULL,
            tile_index INTEGER NOT NULL,
            tile_row INTEGER NOT NULL,
            tile_col INTEGER NOT NULL,
            tile_center_lat REAL NOT NULL,
            tile_center_lng REAL NOT NULL,
            tile_url TEXT,
            result_count INTEGER DEFAULT 0,
            window_width_px INTEGER,
            window_height_px INTEGER,
            viewport_width_px REAL,
            viewport_height_px REAL,
            processed_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            status TEXT NOT NULL CHECK(status in ('pending','in_progress','completed','failed')) DEFAULT 'pending',
            updated_at TEXT,
            last_error TEXT,
            PRIMARY KEY (city, query, tile_index)
        );
        CREATE INDEX IF NOT EXISTS tiles_q ON tiles(city, query, status);

        CREATE TABLE IF NOT EXISTS places (\n            place_id TEXT NOT NULL,\n            city TEXT NOT NULL,\n            query TEXT NOT NULL,\n            tile_index INTEGER NOT NULL,\n            name TEXT NOT NULL,\n            href TEXT NOT NULL,\n            lat REAL NOT NULL,\n            lng REAL NOT NULL,\n            address TEXT,\n            location TEXT,\n            phone TEXT,\n            plus_code TEXT,\n            website TEXT,\n            social_media_urls TEXT,\n            open_time TEXT,\n            emails_phones_socials TEXT,\n            status TEXT NOT NULL CHECK(status in ('success','failed')) DEFAULT 'success',\n            last_error TEXT,\n            warnings TEXT,\n            extracted_at TEXT NOT NULL,\n            run_id TEXT NOT NULL,\n            PRIMARY KEY (city, query, place_id)\n        );
        CREATE INDEX IF NOT EXISTS places_tile ON places(city, query, tile_index);
        CREATE UNIQUE INDEX IF NOT EXISTS places_place_id_unique ON places(place_id);
        """
    )
    # best-effort migrations for existing DBs (ignore if columns already exist)
    for sql in (
        "ALTER TABLE runs ADD COLUMN window_width_px INTEGER",
        "ALTER TABLE runs ADD COLUMN window_height_px INTEGER",
        "ALTER TABLE runs ADD COLUMN viewport_width_px REAL",
        "ALTER TABLE runs ADD COLUMN viewport_height_px REAL",
        "ALTER TABLE runs ADD COLUMN mpp REAL",
        "ALTER TABLE runs ADD COLUMN cell_width_km REAL",
        "ALTER TABLE runs ADD COLUMN cell_height_km REAL",
        "ALTER TABLE runs ADD COLUMN overlap_ratio REAL",
    ):
        try:
            conn.execute(sql)
        except Exception:
            pass
    try:
        conn.execute("ALTER TABLE tiles ADD COLUMN tile_url TEXT")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE tiles ADD COLUMN result_count INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE tiles ADD COLUMN window_width_px INTEGER")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE tiles ADD COLUMN window_height_px INTEGER")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE tiles ADD COLUMN viewport_width_px REAL")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE tiles ADD COLUMN viewport_height_px REAL")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE tiles ADD COLUMN processed_count INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE tiles ADD COLUMN failed_count INTEGER DEFAULT 0")
    except Exception:
        pass
    for sql in (
        "ALTER TABLE places ADD COLUMN address TEXT",
        "ALTER TABLE places ADD COLUMN location TEXT",
        "ALTER TABLE places ADD COLUMN phone TEXT",
        "ALTER TABLE places ADD COLUMN plus_code TEXT",
        "ALTER TABLE places ADD COLUMN website TEXT",
        "ALTER TABLE places ADD COLUMN social_media_urls TEXT",
        "ALTER TABLE places ADD COLUMN open_time TEXT",
        "ALTER TABLE places ADD COLUMN emails_phones_socials TEXT",
        "ALTER TABLE places ADD COLUMN status TEXT",
        "ALTER TABLE places ADD COLUMN last_error TEXT",
        "ALTER TABLE places ADD COLUMN warnings TEXT",
    ):
        try:
            conn.execute(sql)
        except Exception:
            pass
    conn.commit()


def start_run(conn: sqlite3.Connection, run_id: str, *, city: str, country: Optional[str], query: str, zoom: int, language: str) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO runs(run_id, started_at, city, country, query, zoom, language)
        VALUES (:run_id, :started_at, :city, :country, :query, :zoom, :language)
        """,
        {
            "run_id": run_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "city": city,
            "country": (country or ""),
            "query": query,
            "zoom": int(zoom),
            "language": language,
        },
    )
    conn.commit()


def update_run_meta(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    window_width_px: int,
    window_height_px: int,
    viewport_width_px: float,
    viewport_height_px: float,
    mpp: float,
    cell_width_km: float,
    cell_height_km: float,
    overlap_ratio: float,
) -> None:
    conn.execute(
        """
        UPDATE runs SET
            window_width_px=:window_width_px, window_height_px=:window_height_px,
            viewport_width_px=:viewport_width_px, viewport_height_px=:viewport_height_px,
            mpp=:mpp, cell_width_km=:cell_width_km, cell_height_km=:cell_height_km, overlap_ratio=:overlap_ratio
        WHERE run_id=:run_id
        """,
        {
            "window_width_px": int(window_width_px),
            "window_height_px": int(window_height_px),
            "viewport_width_px": float(viewport_width_px),
            "viewport_height_px": float(viewport_height_px),
            "mpp": float(mpp),
            "cell_width_km": float(cell_width_km),
            "cell_height_km": float(cell_height_km),
            "overlap_ratio": float(overlap_ratio),
            "run_id": run_id,
        },
    )
    conn.commit()


def reset_in_progress(conn: sqlite3.Connection, city: str, query: str) -> None:
    conn.execute(
        "UPDATE tiles SET status='pending' WHERE city=:city AND query=:query AND status='in_progress'",
        {"city": city, "query": query},
    )
    conn.commit()


def init_tiles(
    conn: sqlite3.Connection,
    city: str,
    query: str,
    points: Iterable[tuple[int, int, int, float, float, str, int, int, float, float]],
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        """
        INSERT INTO tiles(
            city, query, tile_index, tile_row, tile_col,
            tile_center_lat, tile_center_lng, tile_url,
            window_width_px, window_height_px, viewport_width_px, viewport_height_px,
            status, updated_at
        )
        VALUES (:city, :query, :tile_index, :tile_row, :tile_col,
                :tile_center_lat, :tile_center_lng, :tile_url,
                :window_width_px, :window_height_px, :viewport_width_px, :viewport_height_px,
                'pending', :updated_at)
        ON CONFLICT(city, query, tile_index) DO UPDATE SET
            tile_row=excluded.tile_row,
            tile_col=excluded.tile_col,
            tile_center_lat=excluded.tile_center_lat,
            tile_center_lng=excluded.tile_center_lng,
            tile_url=excluded.tile_url,
            window_width_px=excluded.window_width_px,
            window_height_px=excluded.window_height_px,
            viewport_width_px=excluded.viewport_width_px,
            viewport_height_px=excluded.viewport_height_px,
            updated_at=excluded.updated_at
        """,
        [
            {
                "city": city,
                "query": query,
                "tile_index": idx,
                "tile_row": r,
                "tile_col": c,
                "tile_center_lat": float(lat),
                "tile_center_lng": float(lng),
                "tile_url": url,
                "window_width_px": int(win_w),
                "window_height_px": int(win_h),
                "viewport_width_px": float(vp_w),
                "viewport_height_px": float(vp_h),
                "updated_at": now,
            }
            for (idx, r, c, lat, lng, url, win_w, win_h, vp_w, vp_h) in points
        ],
    )
    conn.commit()


def get_tile_status(conn: sqlite3.Connection, city: str, query: str, tile_index: int) -> Optional[str]:
    cur = conn.execute(
        "SELECT status FROM tiles WHERE city=:city AND query=:query AND tile_index=:tile_index",
        {"city": city, "query": query, "tile_index": int(tile_index)},
    )
    row = cur.fetchone()
    return row[0] if row else None


def set_tile_in_progress(
    conn: sqlite3.Connection,
    city: str,
    query: str,
    *,
    tile_index: int,
    tile_row: int,
    tile_col: int,
    lat: float,
    lng: float,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO tiles(city, query, tile_index, tile_row, tile_col, tile_center_lat, tile_center_lng, result_count, status, updated_at, last_error)
        VALUES (:city, :query, :tile_index, :tile_row, :tile_col, :tile_center_lat, :tile_center_lng, 0, 'in_progress', :updated_at, NULL)
        ON CONFLICT(city, query, tile_index) DO UPDATE SET
            status='in_progress', tile_row=excluded.tile_row, tile_col=excluded.tile_col,
            tile_center_lat=excluded.tile_center_lat, tile_center_lng=excluded.tile_center_lng,
            result_count=0,
            updated_at=excluded.updated_at, last_error=NULL
        """,
        {
            "city": city,
            "query": query,
            "tile_index": int(tile_index),
            "tile_row": int(tile_row),
            "tile_col": int(tile_col),
            "tile_center_lat": float(lat),
            "tile_center_lng": float(lng),
            "updated_at": now,
        },
    )
    conn.commit()


def set_tile_completed(conn: sqlite3.Connection, city: str, query: str, tile_index: int, *, result_count: int, processed_count: int = 0, failed_count: int = 0) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE tiles SET status='completed', result_count=:result_count, processed_count=:processed_count, failed_count=:failed_count, updated_at=:updated_at WHERE city=:city AND query=:query AND tile_index=:tile_index",
        {
            "result_count": int(result_count),
            "processed_count": int(processed_count),
            "failed_count": int(failed_count),
            "updated_at": now,
            "city": city,
            "query": query,
            "tile_index": int(tile_index),
        },
    )
    conn.commit()


def set_tile_failed(conn: sqlite3.Connection, city: str, query: str, tile_index: int, error_text: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE tiles SET status='failed', updated_at=:updated_at, last_error=:last_error WHERE city=:city AND query=:query AND tile_index=:tile_index",
        {"updated_at": now, "last_error": error_text, "city": city, "query": query, "tile_index": int(tile_index)},
    )
    conn.commit()


def set_tile_note(conn: sqlite3.Connection, city: str, query: str, tile_index: int, error_text: str) -> None:
    """Attach non-fatal notes/errors to a completed/in_progress tile without changing status."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE tiles SET updated_at=:updated_at, last_error=:last_error WHERE city=:city AND query=:query AND tile_index=:tile_index",
        {"updated_at": now, "last_error": error_text, "city": city, "query": query, "tile_index": int(tile_index)},
    )
    conn.commit()


def list_tiles(conn: sqlite3.Connection, city: str, query: str) -> list[dict]:
    cur = conn.execute(
        """
        SELECT tile_index, tile_row, tile_col,
               tile_center_lat, tile_center_lng,
               tile_url, window_width_px, window_height_px,
               viewport_width_px, viewport_height_px,
               status
        FROM tiles
        WHERE city=:city AND query=:query
        ORDER BY tile_index ASC
        """,
        {"city": city, "query": query},
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def update_tile_url(conn: sqlite3.Connection, city: str, query: str, tile_index: int, tile_url: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE tiles SET tile_url=:tile_url, updated_at=:updated_at WHERE city=:city AND query=:query AND tile_index=:tile_index",
        {
            "tile_url": tile_url,
            "updated_at": now,
            "city": city,
            "query": query,
            "tile_index": int(tile_index),
        },
    )
    conn.commit()


def place_exists(conn: sqlite3.Connection, city: str, query: str, place_id: str) -> bool:
    # Deduplicate globally by place_id (UUID), ignore city/query
    cur = conn.execute(
        "SELECT 1 FROM places WHERE place_id=:place_id AND (status IS NULL OR status = '' OR status = 'success')",
        {"place_id": place_id},
    )
    return cur.fetchone() is not None


def upsert_place(conn: sqlite3.Connection, *, place_id: str, city: str, query: str, tile_index: int, name: str, href: str, lat: float, lng: float, extracted_at: Optional[str], run_id: str) -> None:
    ts = extracted_at or datetime.now(timezone.utc).isoformat()
    params = {
        "place_id": place_id,
        "city": city,
        "query": query,
        "tile_index": int(tile_index),
        "name": name,
        "href": href,
        "lat": float(lat),
        "lng": float(lng),
        "extracted_at": ts,
        "run_id": run_id,
    }
    conn.execute(
        """
        INSERT INTO places(place_id, city, query, tile_index, name, href, lat, lng, extracted_at, run_id)
        VALUES (:place_id, :city, :query, :tile_index, :name, :href, :lat, :lng, :extracted_at, :run_id)
        ON CONFLICT(place_id) DO UPDATE SET
            city=excluded.city,
            query=excluded.query,
            tile_index=excluded.tile_index,
            name=excluded.name,
            href=excluded.href,
            lat=excluded.lat,
            lng=excluded.lng,
            extracted_at=excluded.extracted_at,
            run_id=excluded.run_id
        """,
        params,
    )
    conn.commit()


def upsert_place_struct(
    conn: sqlite3.Connection,
    *,
    place_id: str,
    city: str,
    query: str,
    tile_index: int,
    name: str,
    href: str,
    lat: float,
    lng: float,
    address: str,
    location: str,
    phone: str,
    plus_code: str,
    website: str,
    social_media_urls: str,
    open_time: str,
    emails_phones_socials: str,
    warnings: str = "",
    extracted_at: Optional[str] = None,
    run_id: str = "",
) -> None:
    ts = extracted_at or datetime.now(timezone.utc).isoformat()
    params = {
        "place_id": place_id,
        "city": city,
        "query": query,
        "tile_index": int(tile_index),
        "name": name,
        "href": href,
        "lat": float(lat),
        "lng": float(lng),
        "address": address,
        "location": location,
        "phone": phone,
        "plus_code": plus_code,
        "website": website,
        "social_media_urls": social_media_urls,
        "open_time": open_time,
        "emails_phones_socials": emails_phones_socials,
        "warnings": warnings,
        "extracted_at": ts,
        "run_id": run_id,
    }
    conn.execute(
        """
        INSERT INTO places(
            place_id, city, query, tile_index, name, href, lat, lng,
            address, location, phone, plus_code, website, social_media_urls, open_time, emails_phones_socials,
            status, last_error, warnings, extracted_at, run_id
        ) VALUES (:place_id, :city, :query, :tile_index, :name, :href, :lat, :lng,
                  :address, :location, :phone, :plus_code, :website, :social_media_urls, :open_time, :emails_phones_socials,
                  'success', '', :warnings, :extracted_at, :run_id)
        ON CONFLICT(place_id) DO UPDATE SET
            city=excluded.city,
            query=excluded.query,
            tile_index=excluded.tile_index,
            name=excluded.name,
            href=excluded.href,
            lat=excluded.lat,
            lng=excluded.lng,
            address=excluded.address,
            location=excluded.location,
            phone=excluded.phone,
            plus_code=excluded.plus_code,
            website=excluded.website,
            social_media_urls=excluded.social_media_urls,
            open_time=excluded.open_time,
            emails_phones_socials=excluded.emails_phones_socials,
            status='success', last_error='', warnings=excluded.warnings,
            extracted_at=excluded.extracted_at,
            run_id=excluded.run_id
        """,
        params,
    )
    conn.commit()



def upsert_place_failure(
    conn: sqlite3.Connection,
    *,
    place_id: str,
    city: str,
    query: str,
    tile_index: int,
    name: str,
    href: str,
    lat: float,
    lng: float,
    last_error: str,
    warnings: str = "",
    extracted_at: Optional[str] = None,
    run_id: str = "",
) -> None:
    ts = extracted_at or datetime.now(timezone.utc).isoformat()
    params = {
        "place_id": place_id,
        "city": city,
        "query": query,
        "tile_index": int(tile_index),
        "name": name,
        "href": href,
        "lat": float(lat),
        "lng": float(lng),
        "last_error": str(last_error or ''),
        "warnings": warnings,
        "extracted_at": ts,
        "run_id": run_id,
    }
    conn.execute(
        """
        INSERT INTO places(
            place_id, city, query, tile_index, name, href, lat, lng,
            status, last_error, warnings, extracted_at, run_id
        ) VALUES (:place_id, :city, :query, :tile_index, :name, :href, :lat, :lng,
                  'failed', :last_error, :warnings, :extracted_at, :run_id)
        ON CONFLICT(place_id) DO UPDATE SET
            city=excluded.city,
            query=excluded.query,
            tile_index=excluded.tile_index,
            name=excluded.name,
            href=excluded.href,
            lat=excluded.lat,
            lng=excluded.lng,
            status='failed', last_error=excluded.last_error, warnings=excluded.warnings,
            extracted_at=excluded.extracted_at,
            run_id=excluded.run_id
        """,
        params,
    )
    conn.commit()


def get_place_by_id(conn: sqlite3.Connection, place_id: str) -> Optional[dict]:
    cur = conn.execute(
        """
        SELECT place_id, city, query, tile_index, name, href, lat, lng, run_id
        FROM places WHERE place_id=:place_id
        """,
        {"place_id": place_id},
    )
    row = cur.fetchone()
    if not row:
        return None
    cols = [c[0] for c in cur.description]
    return {k: v for k, v in zip(cols, row)}


def update_tile_counts(conn: sqlite3.Connection, city: str, query: str, tile_index: int) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
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
            ),
            updated_at = :updated_at
        WHERE city=:city AND query=:query AND tile_index=:tile_index
        """,
        {"updated_at": now, "city": city, "query": query, "tile_index": int(tile_index)},
    )
    conn.commit()


class DB:
    def __init__(self, db_path: Path) -> None:
        self.conn = get_connection(db_path)
        init_schema(self.conn)

    def start_run(self, run_id: str, *, city: str, country: Optional[str], query: str, zoom: int, language: str) -> None:
        start_run(self.conn, run_id, city=city, country=country, query=query, zoom=zoom, language=language)

    def reset_in_progress(self, city: str, query: str) -> None:
        reset_in_progress(self.conn, city, query)

    def init_tiles(self, city: str, query: str, points: Iterable[tuple[int, int, int, float, float, str, int, int, float, float]]) -> None:
        init_tiles(self.conn, city, query, points)

    def get_tile_status(self, city: str, query: str, tile_index: int) -> Optional[str]:
        return get_tile_status(self.conn, city, query, tile_index)

    def set_tile_in_progress(self, city: str, query: str, *, tile_index: int, tile_row: int, tile_col: int, lat: float, lng: float) -> None:
        set_tile_in_progress(self.conn, city, query, tile_index=tile_index, tile_row=tile_row, tile_col=tile_col, lat=lat, lng=lng)

    def set_tile_completed(self, city: str, query: str, tile_index: int, *, result_count: int, processed_count: int = 0, failed_count: int = 0) -> None:
        set_tile_completed(self.conn, city, query, tile_index, result_count=result_count, processed_count=processed_count, failed_count=failed_count)

    def set_tile_failed(self, city: str, query: str, tile_index: int, error_text: str) -> None:
        set_tile_failed(self.conn, city, query, tile_index, error_text)

    def set_tile_note(self, city: str, query: str, tile_index: int, error_text: str) -> None:
        set_tile_note(self.conn, city, query, tile_index, error_text)

    def list_tiles(self, city: str, query: str) -> list[dict]:
        return list_tiles(self.conn, city, query)

    def update_tile_url(self, city: str, query: str, tile_index: int, tile_url: str) -> None:
        update_tile_url(self.conn, city, query, tile_index, tile_url)

    def place_exists(self, city: str, query: str, place_id: str) -> bool:
        return place_exists(self.conn, city, query, place_id)

    def upsert_place(self, *, place_id: str, city: str, query: str, tile_index: int, name: str, href: str, lat: float, lng: float, extracted_at: Optional[str], run_id: str) -> None:
        upsert_place(self.conn, place_id=place_id, city=city, query=query, tile_index=tile_index, name=name, href=href, lat=lat, lng=lng, extracted_at=extracted_at, run_id=run_id)

    def upsert_place_struct(self, **payload) -> None:
        upsert_place_struct(self.conn, **payload)

    def upsert_place_failure(self, **payload) -> None:
        upsert_place_failure(self.conn, **payload)

    def update_run_meta(self, **kwargs) -> None:
        update_run_meta(self.conn, **kwargs)

    def get_place_by_id(self, place_id: str) -> Optional[dict]:
        return get_place_by_id(self.conn, place_id)

    def update_tile_counts(self, city: str, query: str, tile_index: int) -> None:
        update_tile_counts(self.conn, city, query, tile_index)














