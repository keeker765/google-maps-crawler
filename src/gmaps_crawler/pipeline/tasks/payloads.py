from __future__ import annotations

import json as _json
from typing import Dict


def build_base_payload(
    *,
    run_ctx,
    tile_ctx,
    query: str,
    name: str,
    href: str,
    pid: str,
    lat: float,
    lng: float,
) -> Dict:
    """Build the common payload fields shared by success/failed results."""
    return {
        "place_id": pid,
        "city": run_ctx.city,
        "query": query,
        "tile_index": tile_ctx.index,
        "name": name,
        "href": href,
        "lat": float(lat),
        "lng": float(lng),
    }


def build_success_payload(base: Dict, data: Dict) -> Dict:
    """Extend base payload with extracted data fields for a success record."""
    return {
        **base,
        "address": str(data.get("address") or ""),
        "location": str(data.get("location") or ""),
        "phone": str(data.get("phone") or ""),
        "plus_code": str(data.get("plus_code") or ""),
        "website": str(data.get("website") or ""),
        "social_media_urls": _json.dumps(data.get("social_media_urls") or [], ensure_ascii=False),
        "open_time": str(data.get("open_time") or ""),
        "emails_phones_socials": _json.dumps(data.get("emails_phones_socials") or {}, ensure_ascii=False),
        "warnings": _json.dumps(data.get("warnings") or [], ensure_ascii=False),
    }


def build_failure_payload(base: Dict, *, run_ctx, last_error: str, warnings_json: str = "[]") -> Dict:
    """Extend base payload to represent a failed extraction row."""
    return {
        **base,
        "last_error": str(last_error or ""),
        "warnings": warnings_json,
        "extracted_at": None,
        "run_id": run_ctx.run_id,
    }
