from __future__ import annotations

from urllib.parse import quote_plus

from DrissionPage._pages.chromium_tab import ChromiumTab


def build_search_url(query: str, latitude: float, longitude: float, zoom: int, language: str) -> str:
    """Construct a Google Maps search URL for a query centered at lat/lng.

    Keeps params minimal to reduce automatic reframe by Google.
    """
    encoded_query = quote_plus(query)
    return f"https://www.google.com/maps/search/{encoded_query}/@{latitude},{longitude},{zoom}z?hl={language}"

