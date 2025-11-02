from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, List

from gmaps_crawler.geo.bbox import BoundingBox

EARTH_RADIUS_KM = 6371.0088
MEAN_LAT_KM = 110.574  # average kilometres per degree latitude


@dataclass(frozen=True)
class GridPoint:
    index: int
    latitude: float
    longitude: float
    row: int
    col: int


def km_to_lat_deg(kilometres: float) -> float:
    return kilometres / MEAN_LAT_KM


def km_to_lon_deg(kilometres: float, latitude: float) -> float:
    cos_lat = math.cos(math.radians(latitude))
    if cos_lat == 0:
        raise ValueError("Longitude degree size undefined at the poles.")
    km_per_degree = 111.320 * cos_lat
    return kilometres / km_per_degree


def _frange(start: float, stop: float, step: float) -> Iterable[float]:
    value = start
    epsilon = step / 10.0
    while value <= stop + epsilon:
        yield value
        value += step


def generate_grid_points(
    bbox: BoundingBox,
    *,
    cell_width_km: float,
    cell_height_km: float,
    overlap_ratio: float = 0.1,
) -> List[GridPoint]:
    if cell_width_km <= 0 or cell_height_km <= 0:
        raise ValueError("cell dimensions must be positive")
    if not (0 <= overlap_ratio < 1):
        raise ValueError("overlap_ratio must be between 0 (inclusive) and 1 (exclusive)")

    row_idx = 0
    index = 0
    points: List[GridPoint] = []

    lat_step_km = cell_height_km * (1 - overlap_ratio)
    lat_step_deg = km_to_lat_deg(lat_step_km)
    lat_cell_deg = km_to_lat_deg(cell_height_km)

    for lat_center in _frange(bbox.min_lat + lat_cell_deg / 2, bbox.max_lat, lat_step_deg):
        lon_step_km = cell_width_km * (1 - overlap_ratio)
        lon_step_deg = km_to_lon_deg(lon_step_km, lat_center)
        lon_cell_deg = km_to_lon_deg(cell_width_km, lat_center)

        col_idx = 0
        for lon_center in _frange(bbox.min_lon + lon_cell_deg / 2, bbox.max_lon, lon_step_deg):
            points.append(
                GridPoint(
                    index=index,
                    latitude=lat_center,
                    longitude=lon_center,
                    row=row_idx,
                    col=col_idx,
                )
            )
            index += 1
            col_idx += 1
        row_idx += 1

    return points
