from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from gmaps_crawler.storage.db import DB


@dataclass
class RunContext:
    city: str
    query: str
    country: Optional[str]
    zoom: int
    language: str
    run_id: str
    csv_path: Path
    html_root: Path
    db: DB


@dataclass
class TileContext:
    index: int
    row: int
    col: int
    center_lat: float
    center_lng: float
    tile_url: str
