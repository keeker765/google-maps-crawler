# Database Schema (SQLite)

This document describes the SQLite schema used by the crawler. The database file defaults to `data/db/gmaps.sqlite`.

## Overview
- Tables: `runs`, `tiles`, `places`
- Primary keys and indexes ensure resumability (tiles), de-duplication (places), and basic analytics.
- The legacy column `file_html` has been removed from code and schema (existing DBs may still have it; it is ignored).

---

## Table: runs
Tracks a single execution of a city/query crawl and its viewport/coverage parameters.

Columns:
- `run_id` TEXT PRIMARY KEY — unique identifier for the run.
- `started_at` TEXT NOT NULL — ISO-8601 UTC timestamp.
- `city` TEXT NOT NULL — target city.
- `country` TEXT — optional country.
- `query` TEXT NOT NULL — search phrase.
- `zoom` INTEGER NOT NULL — map zoom level.
- `language` TEXT NOT NULL — UI language code.
- `window_width_px` INTEGER — window width in pixels.
- `window_height_px` INTEGER — window height in pixels.
- `viewport_width_px` REAL — measured canvas width.
- `viewport_height_px` REAL — measured canvas height.
- `mpp` REAL — meters-per-pixel.
- `cell_width_km` REAL — grid cell width (km).
- `cell_height_km` REAL — grid cell height (km).
- `overlap_ratio` REAL — grid overlap ratio.

---

## Table: tiles
Represents each grid cell to be crawled (resumeable and auditable).

Composite key and indexes:
- PRIMARY KEY (`city`, `query`, `tile_index`)
- INDEX on (`city`, `query`, `status`)

Columns:
- `city` TEXT NOT NULL
- `query` TEXT NOT NULL
- `tile_index` INTEGER NOT NULL — stable index of the cell.
- `tile_row` INTEGER NOT NULL, `tile_col` INTEGER NOT NULL — grid position.
- `tile_center_lat` REAL NOT NULL, `tile_center_lng` REAL NOT NULL — cell center.
- `tile_url` TEXT — precomputed Google Maps search URL.
- `result_count` INTEGER DEFAULT 0 — total cards seen.
- `window_width_px` INTEGER, `window_height_px` INTEGER — window size at crawl.
- `viewport_width_px` REAL, `viewport_height_px` REAL — canvas size at crawl.
- `processed_count` INTEGER DEFAULT 0 — successful inserts.
- `failed_count` INTEGER DEFAULT 0 — per-place failures.
- `status` TEXT NOT NULL CHECK in ('pending','in_progress','completed','failed') DEFAULT 'pending'
- `updated_at` TEXT — ISO timestamp of last status change.
- `last_error` TEXT — non-fatal aggregated notes or fatal error text.

---

## Table: places
Stores deduplicated place details and contact data. De-duplicated globally by `place_id`.

Keys and indexes:
- PRIMARY KEY (`city`, `query`, `place_id`)
- UNIQUE INDEX on `place_id`

Columns:
- `place_id` TEXT NOT NULL — deterministic UUID from (lat,lng).
- `city` TEXT NOT NULL — city for this record.
- `query` TEXT NOT NULL — search phrase.
- `tile_index` INTEGER NOT NULL — originating tile index.
- `name` TEXT NOT NULL — business name.
- `href` TEXT NOT NULL — Google Maps place link.
- `lat` REAL NOT NULL, `lng` REAL NOT NULL — parsed coordinates.
- `address` TEXT — street address (cleaned).
- `location` TEXT — serialized city/state/country string.
- `phone` TEXT — primary phone.
- `plus_code` TEXT — Google Plus Code if available.
- `website` TEXT — official website URL.
- `social_media_urls` TEXT — JSON list of raw social links.
- `open_time` TEXT — text/serialized opening hours.
- `emails_phones_socials` TEXT — JSON with emails/phones/socials details.
- `status` TEXT NOT NULL CHECK in ('success','failed') DEFAULT 'success' — place extraction status.
- `last_error` TEXT — last error message for failed extraction.
- `warnings` TEXT — warning summary (e.g., missing optional fields).
- `extracted_at` TEXT NOT NULL — ISO-8601 UTC timestamp.
- `run_id` TEXT NOT NULL — foreign key to `runs.run_id` (not enforced).

Note: The historical `file_html` column is no longer created or written.

---

## Typical Queries
- Pending tiles: `SELECT tile_index FROM tiles WHERE city=? AND query=? AND status='pending'`
- Progress overview: `SELECT status, COUNT(*) FROM tiles WHERE city=? AND query=? GROUP BY status`
- Places by city/query: `SELECT COUNT(*) FROM places WHERE city=? AND query=?`
- De-duplication check: `SELECT COUNT(DISTINCT place_id) FROM places`

---

## Compatibility Notes
- Existing databases that already have a `file_html` column remain readable; the application no longer writes or requires it.
- Schema migrations are best-effort and tolerant of missing/extra columns.

