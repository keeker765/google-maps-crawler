from __future__ import annotations

import argparse
import ast
import json
import re
from typing import Union, List
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from logger import writer_thread_logger as logger

import pandas as pd
from gmaps_crawler.config import settings  # noqa: F401 (import to initialize logging)


# Lightweight email pattern; normalize to lowercase
EMAIL_RE = re.compile(r"(?i)([a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})")

from urllib.parse import urlparse


def normalize_email(text: str) -> List[str]:
    if not text:
        return []
    found = EMAIL_RE.findall(text)
    cleaned: List[str] = []
    for item in found:
        e = item.strip().lower()
        if e and e not in cleaned:
            cleaned.append(e)
    return cleaned


def extract_instagram_handle(raw_urls: List[str]) -> str:
    for raw_url in raw_urls:
        if "instagram" in raw_url:
            return raw_url
    return ""


def json_load_maybe(text: str, default):
    if text is None:
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


def split_location_fields(location_text: str, city_fallback: str = "") -> Tuple[str, str, str]:
    """Split stored location text into (city, state_province, country).

    Handles:
    - "Paris,Ile-de-France,France"
    - "('Paris', 'Ile-de-France', 'France')"
    - plain city string (falls back to city only)
    """
    if not location_text:
        return city_fallback, "", ""

    txt = str(location_text).strip()
    try:
        obj = ast.literal_eval(txt)
        if isinstance(obj, (tuple, list)):
            vals = [str(x).strip() for x in obj]
            if len(vals) >= 3:
                return vals[0], vals[1], vals[2]
            if len(vals) == 2:
                return vals[0], vals[1], ""
            if len(vals) == 1:
                return vals[0], "", ""
    except Exception:
        pass

    parts = [p.strip() for p in txt.split(",") if p.strip()]
    if len(parts) >= 3:
        return parts[0], parts[1], parts[-1]
    if len(parts) == 2:
        return parts[0], parts[1], ""
    if len(parts) == 1:
        return parts[0], "", ""
    return city_fallback, "", ""

def website_filter(website_url: Union[str, List[str]]):
    common_urls = [
        "youtube.com", "bilibili.com", "tiktok.com", "vimeo.com",
        "facebook.com", "twitter.com", "instagram.com", "weibo.com",
        "reddit.com", "linkedin.com", "pinterest.com", "threads.net",
        "google.com", "baidu.com", "bing.com", "yahoo.com", "duckduckgo.com",
        "amazon.com", "taobao.com", "tmall.com", "jd.com", "ebay.com",
        "cnn.com", "bbc.com", "nytimes.com", "foxnews.com", "reuters.com",
        "spotify.com", "soundcloud.com", "apple.com/music", "netflix.com",
        "disneyplus.com", "hulu.com",
        "gmail.com", "outlook.com", "yahoo.com/mail", "163.com", "qq.com",
        "quora.com", "zhihu.com", "douban.com", "stackoverflow.com",
        "wikipedia.org", "canva.com", "medium.com", "notion.so",
        "figma.com", "github.com", "gitlab.com",
    ]
    def _website_filter(website: str):
        website = (website
                    .replace(" ", "")
                    .replace("https://", "")
                    .replace("http://", "")
                    .replace("www.", ""))
        website = website[:-1] if website.endswith("/") else website
        return "" if website in common_urls else website
    
    if isinstance(website_url, List):
        return [ _website_filter(w) for w in website_url if w]
    
    return _website_filter(website_url)
        

def build_rows(df: pd.DataFrame) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for _, r in df.iterrows():
        name = str(r.get("name") or "")
        href = str(r.get("href") or "")
        city_db = str(r.get("city") or "")
        address = str(r.get("address") or "")
        location_txt = str(r.get("location") or "")
        phone = str(r.get("phone") or "")
        website = website_filter(str(r.get("website") or ""))
        social_media_urls = json_load_maybe(r.get("social_media_urls"), [])
        open_time = str(r.get("open_time") or "")
        eps = json_load_maybe(r.get("emails_phones_socials"), {})

        # city/state/country
        city_out, state_province, country = split_location_fields(location_txt, city_fallback=city_db)

        # socials dict
        socials = eps.get("socials") if isinstance(eps, dict) else {}
        socials = socials if isinstance(socials, dict) else {}
        facebook_url = str(socials.get("facebook") or "")
        twitter_url = str(socials.get("twitter") or "")
        yelp_url = str(socials.get("yelp") or "")

        # instagram handle
        ig_handle = extract_instagram_handle(social_media_urls)

        # additional phones (distinct, excluding primary)
        add_phones_list: List[str] = []
        phs = eps.get("phones") if isinstance(eps, dict) else []
        PHONE_VALID_RE = re.compile(r"^(?:\+(?:[1-9]\d{0,2})(?:[-\s]?\d{6,})?)$")

        if isinstance(phs, list):
            for p in phs:
                pv = str(p.get("phone") if isinstance(p, dict) else p or "").strip()
                # 过滤掉不符合正常手机号格式的项（如 +2336-237 这种）
                if not PHONE_VALID_RE.match(pv):
                    continue
                if pv and pv.replace("+", "").replace(" ", "") != phone.replace("+", "").replace(" ", "") and pv not in add_phones_list:
                    add_phones_list.append(pv)

        add_phones_list = list(set(add_phones_list))
        if phone in add_phones_list:
            add_phones_list.remove(phone)
        additional_phones = ",".join(add_phones_list)


        # social medias raw list (combine raw list + summarized socials values)
        raw_urls_list: List[str] = []
        if isinstance(social_media_urls, list):
            raw_urls_list.extend([str(u) for u in (social_media_urls or []) if u])
        raw_urls_list.extend([str(v) for v in socials.values() if v])
        raw_urls_list = list(dict.fromkeys([u for u in raw_urls_list if u]))
        social_medias_raw = ",".join(raw_urls_list)

        # emails list (one row per email)
        emails_items = eps.get("emails") if isinstance(eps, dict) else []
        emitted = False
        if isinstance(emails_items, list) and emails_items:
            for item in emails_items:
                if not isinstance(item, dict):
                    continue
                src_text = str(item.get("email") or "")
                owner = str(item.get("owner") or "")  # optional
                source_url = str(item.get("source_url") or "")
                for em in normalize_email(src_text):
                    rows.append(
                        {
                            "business_name": name,
                            "instagram_handle": ig_handle,
                            "city": city_out,
                            "state_province": state_province,
                            "country": country,
                            "full_address": address,
                            "phone": phone,
                            "additional_phones": additional_phones,
                            "google_maps_url": href,
                            "website_url": website,
                            "opening_hours": open_time,
                            "google_knowledge_url": "",
                            "social_medias_raw": social_medias_raw,
                            "facebook_url": facebook_url,
                            "twitter_url": twitter_url,
                            "yelp_url": yelp_url,
                            "email": em,
                            "email_owner_name": owner,
                            "source_url": source_url,
                            "scrape_notes": "",
                        }
                    )
                    emitted = True
        if not emitted:
            # one email per row; skip if not found
            continue
    return rows


def export_emails_csv(db_path: Path, out_csv: Path, *, city: Optional[str] = None, query: Optional[str] = None) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        base_sql = (
            "SELECT name, href, city, address, location, phone, website, social_media_urls, open_time, emails_phones_socials "
            "FROM places"
        )
        conds: List[str] = []
        params: List[str] = []
        if city:
            conds.append("city = ?")
            params.append(city)
        if query:
            conds.append("query = ?")
            params.append(query)
        if conds:
            base_sql += " WHERE " + " AND ".join(conds)
        df = pd.read_sql(base_sql, conn, params=params)
        rows = build_rows(df)

        # Desired columns & order (per client doc)
        cols = [
            "business_name",
            "instagram_handle",
            "city",
            "state_province",
            "country",
            "full_address",
            "phone",
            "additional_phones",
            "google_maps_url",
            "website_url",
            "opening_hours",
            "google_knowledge_url",
            "social_medias_raw",
            "facebook_url",
            "twitter_url",
            "yelp_url",
            "email",
            "email_owner_name",
            "source_url",
            "scrape_notes",
        ]
        if not rows:
            pd.DataFrame(columns=cols).to_csv(out_csv, index=False, encoding="utf-8")
            return 0
        out_df = pd.DataFrame(rows)[cols]
        out_df.to_csv(out_csv, index=False, encoding="utf-8")
        return len(out_df)
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export per-email CSV from places table (one email per row).")
    p.add_argument("--db", type=Path, default=Path("data/db/gmaps.sqlite"), help="Path to SQLite DB.")
    p.add_argument("--out", type=Path, default=Path("data/export_emails.csv"), help="Output CSV path.")
    p.add_argument("--city", help="Optional city filter.")
    p.add_argument("--query", help="Optional query phrase filter.")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    count = export_emails_csv(args.db, args.out, city=args.city, query=args.query)
    print(f"Exported {count} email rows to {args.out}")
