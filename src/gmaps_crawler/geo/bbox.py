from logger import crawler_thread_logger as logger
import os
import time
from dataclasses import dataclass
from typing import Iterable, Optional, Tuple, Dict
from threading import Lock
from pathlib import Path
import json

import requests

DEFAULT_TIMEOUT = 30
REQUEST_TIMEOUT = (10, DEFAULT_TIMEOUT)
ENV_ENDPOINTS = "GMAPS_CRAWLER_OVERPASS_ENDPOINTS"
DEFAULT_OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.osm.ch/api/interpreter",
    "https://overpass.openstreetmap.ru/cgi/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter",
]

# use named crawler logger

# In-memory + disk cache to avoid repeated Overpass calls in multi-thread runs
_BBOX_CACHE_MEM: Dict[Tuple[str, str], Tuple[float, float, float, float]] = {}
_BBOX_CACHE_LOCK = Lock()
_BBOX_CACHE_PATH = Path("data/cache/bbox_cache.json")


def _load_bbox_cache_disk() -> None:
    if _BBOX_CACHE_PATH.exists():
        try:
            data = json.loads(_BBOX_CACHE_PATH.read_text(encoding="utf-8"))
            for k, v in data.items():
                if isinstance(v, (list, tuple)) and len(v) == 4 and isinstance(k, str) and "|" in k:
                    city, country = k.split("|", 1)
                    _BBOX_CACHE_MEM[(city, country)] = (float(v[0]), float(v[1]), float(v[2]), float(v[3]))
        except Exception as e:
            logger.debug("bbox cache load failed: %s", e)


def _save_bbox_cache_disk() -> None:
    try:
        _BBOX_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {f"{k[0]}|{k[1]}": list(v) for k, v in _BBOX_CACHE_MEM.items()}
        _BBOX_CACHE_PATH.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.debug("bbox cache save failed: %s", e)


@dataclass(frozen=True)
class BoundingBox:
    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float

    def as_tuple(self) -> tuple[float, float, float, float]:
        return self.min_lat, self.min_lon, self.max_lat, self.max_lon


# ----------------------------
# 错误类型定义
# ----------------------------
class OverpassError(RuntimeError):
    """Fetch bounding box via Overpass."""

class OverpassConnectionError(OverpassError):
    """Fetch bounding box via Overpass."""

class OverpassResponseError(OverpassError):
    """Fetch bounding box via Overpass."""

class OverpassDataError(OverpassError):
    """Fetch bounding box via Overpass."""


def _iter_overpass_endpoints() -> Iterable[str]:
    override = os.getenv(ENV_ENDPOINTS)
    if override:
        endpoints = [item.strip() for item in override.split(",") if item.strip()]
        if endpoints:
            logger.debug("Using custom Overpass endpoints: %s", endpoints)
            return endpoints
    return DEFAULT_OVERPASS_ENDPOINTS


def _build_query(city: str, country: Optional[str] = None, admin_level: Optional[int] = None) -> str:
    """Fetch bounding box via Overpass."""
    city_filter = f'["name"="{city}"]'
    if admin_level is not None:
        city_filter += f'["admin_level"="{admin_level}"]'

    if country:
        return (
            f"[out:json][timeout:{DEFAULT_TIMEOUT}];\n"
            f"area[\"name\"=\"{country}\"][\"boundary\"=\"administrative\"][\"admin_level\"=\"2\"]->.searchArea;\n"
            f"relation{city_filter}[\"boundary\"=\"administrative\"](area.searchArea);\n"
            "out bb;"
        )
    return (
        f"[out:json][timeout:{DEFAULT_TIMEOUT}];\n"
        f"relation{city_filter}[\"boundary\"=\"administrative\"];\n"
        "out bb;"
    )


def fetch_bounding_box(
    city: str,
    *,
    country: Optional[str] = None,
    admin_level: Optional[int] = None,
    session: Optional[requests.Session] = None,
) -> BoundingBox:
    """Fetch bounding box via Overpass."""
    if not city:
        raise ValueError("city 参数不能为空")

    key = (city.strip(), (country or "").strip())
    with _BBOX_CACHE_LOCK:
        if not _BBOX_CACHE_MEM:
            _load_bbox_cache_disk()
        if key in _BBOX_CACHE_MEM:
            min_lat, min_lon, max_lat, max_lon = _BBOX_CACHE_MEM[key]
            logger.info("Using cached bbox for city=%s country=%s: (%s,%s,%s,%s)", city, country or "", min_lat, min_lon, max_lat, max_lon)
            return BoundingBox(min_lat=min_lat, min_lon=min_lon, max_lat=max_lat, max_lon=max_lon)

    query = _build_query(city, country=country, admin_level=admin_level)
    own_session = False
    if session is None:
        session = requests.Session()
        own_session = True

    endpoints = list(_iter_overpass_endpoints())
    logger.info("Fetching Overpass bounding box for city=%s country=%s endpoints=%d",
                city, country or "N/A", len(endpoints))

    last_error: Optional[Exception] = None

    try:
        for index, endpoint in enumerate(endpoints):
            t0 = time.monotonic()
            logger.debug("[overpass] trying endpoint #%d: %s", index + 1, endpoint)

            try:
                response = session.post(endpoint, data={"data": query}, timeout=REQUEST_TIMEOUT)
                elapsed = int((time.monotonic() - t0) * 1000)

                if response.status_code != 200:
                    raise OverpassResponseError(
                        f"HTTP {response.status_code} from {endpoint} after {elapsed}ms: {response.text[:200]}"
                    )

                payload = response.json()
                elements = payload.get("elements")
                if not elements:
                    raise OverpassDataError(
                        f"No bounding box found for city='{city}', country='{country}', admin_level={admin_level}"
                    )

                bounds = elements[0].get("bounds")
                if not bounds:
                    raise OverpassDataError(f"Missing 'bounds' field in Overpass response for {city}")

                bbox = BoundingBox(
                    min_lat=float(bounds["minlat"]),
                    min_lon=float(bounds["minlon"]),
                    max_lat=float(bounds["maxlat"]),
                    max_lon=float(bounds["maxlon"]),
                )

                # cache and return
                with _BBOX_CACHE_LOCK:
                    _BBOX_CACHE_MEM[key] = bbox.as_tuple()
                    _save_bbox_cache_disk()

                logger.info("[overpass] success: city=%s country=%s endpoint=%s elapsed=%dms bbox=%s",
                            city, country, endpoint, elapsed, bbox.as_tuple())
                return bbox

            except requests.Timeout as e:
                last_error = OverpassConnectionError(f"Timeout contacting {endpoint}: {e}")
            except requests.ConnectionError as e:
                last_error = OverpassConnectionError(f"Connection error to {endpoint}: {e}")
            except OverpassError as e:
                last_error = e
            except Exception as e:
                last_error = OverpassError(f"Unexpected error from {endpoint}: {e}")

            # 打印详细失败日志
            elapsed = int((time.monotonic() - t0) * 1000)
            logger.warning("[overpass] failed endpoint=%s elapsed=%dms reason=%s", endpoint, elapsed, last_error)

            if index + 1 < len(endpoints):
                time.sleep(1)  # 暂停以避免被限流

    finally:
        if own_session:
            session.close()

    raise OverpassError(f"All Overpass endpoints failed for '{city}'. Last error: {last_error}")
# if __name__ == "__main__":
#     import sys

#     # 设置日志格式和级别
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s | %(levelname)-8s | %(message)s",
#         datefmt="%H:%M:%S",
#     )

#     # 从命令行读取参数，例如：
#     # python overpass_utils.py Paris France
#     city = sys.argv[1] if len(sys.argv) > 1 else "Paris"
#     country = sys.argv[2] if len(sys.argv) > 2 else None

#     print(f"Fetching bounding box for city='{city}' country='{country}'...")
#     try:
#         bbox = fetch_bounding_box(city, country=country)
#         print("\n✅ Bounding box retrieved successfully:")
#         print(f"   min_lat={bbox.min_lat}")
#         print(f"   min_lon={bbox.min_lon}")
#         print(f"   max_lat={bbox.max_lat}")
#         print(f"   max_lon={bbox.max_lon}")
#         print(f"   as_tuple={bbox.as_tuple()}")
#     except Exception as e:
#         print("\n❌ Failed to fetch bounding box:")
#         print(f"   {type(e).__name__}: {e}")

