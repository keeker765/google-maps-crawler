from __future__ import annotations

from typing import Dict, List, Optional

from gmaps_crawler.utils.geo_id import make_place_id_from_latlng, parse_lat_lng_from_href


def build_tasks(cards: List[Dict], *, city: str, query: str, db) -> List[Dict]:
    tasks: List[Dict] = []
    for info in cards:
        href = (info.get("href") or "").strip()
        if not href:
            continue
        try:
            lat_sk, lng_sk = parse_lat_lng_from_href(href)
            pid = make_place_id_from_latlng(lat_sk, lng_sk)
        except Exception:
            continue
        if db and db.place_exists(city, query, pid):
            continue
        inf = dict(info)
        inf["_pid"] = pid
        inf["_lat"] = float(lat_sk)
        inf["_lng"] = float(lng_sk)
        tasks.append(inf)
    return tasks

