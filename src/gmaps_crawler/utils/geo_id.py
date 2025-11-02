import re
import uuid
from typing import Tuple


_COORD_RE = re.compile(r"/@(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?),")
_COORD_D_RE = re.compile(r"!3d(-?\d+(?:\.\d+)?)!4d(-?\d+(?:\.\d+)?)")


def parse_lat_lng_from_href(href: str) -> Tuple[float, float]:
    m = _COORD_RE.search(href)
    if m:
        return float(m.group(1)), float(m.group(2))
    m2 = _COORD_D_RE.search(href)
    if m2:
        return float(m2.group(1)), float(m2.group(2))
    raise ValueError(f"href has no supported lat/lng segment: {href}")


def make_place_id_from_latlng(lat: float, lng: float, ndigits: int = 7) -> str:
    key = f"{lat:.{ndigits}f},{lng:.{ndigits}f}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))

