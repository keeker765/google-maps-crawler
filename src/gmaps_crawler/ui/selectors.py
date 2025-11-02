from DrissionPage import ChromiumPage
from DrissionPage._elements.chromium_element import ChromiumElement


def extract_card_info(card_ele: ChromiumElement) -> dict:
    """Extract basic info from a results list card element.

    Returns a dict with:
    - name: aria-label of the card (str)
    - href: link to the place (str)
    - card_html: raw HTML of the card (str)
    """
    name = (card_ele.attr('aria-label') or '').strip()
    href = (card_ele.attr('href') or '').strip()
    card_html = card_ele.html
    return {"name": name, "href": href, "card_html": card_html}


def extract_place_html(tab: ChromiumPage, place_name: str) -> str:
    """Strictly extract the place container via photo aria-label parent chain.

    Rule: @aria-label=Photo of {place_name} → parent() → parent() → parent().
    No fallback. If any step fails, raise to surface the issue for debugging.
    """
    photo_ele = tab.ele(f"@aria-label=Photo of {place_name}", timeout=5)
    if not photo_ele:
        raise RuntimeError(f"Photo element not found via aria-label for: {place_name}, url: {tab.url}")
    parent1 = photo_ele.parent()
    if not parent1:
        raise RuntimeError("Photo parent() missing at level 1")
    parent2 = parent1.parent()
    if not parent2:
        raise RuntimeError("Photo parent() missing at level 2")
    parent3 = parent2.parent()
    if not parent3:
        raise RuntimeError("Photo parent() missing at level 3")
    return parent3.html
