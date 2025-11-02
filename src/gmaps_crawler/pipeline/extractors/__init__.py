import logging

from DrissionPage import Chromium
from DrissionPage._pages.chromium_tab import ChromiumTab
from DrissionPage.errors import ElementNotFoundError

from gmaps_crawler.pipeline.extractors.page_extractors.address import extract_address
from gmaps_crawler.pipeline.extractors.page_extractors.open_time import extract_open_time
from gmaps_crawler.pipeline.extractors.page_extractors.phone import extract_phone
from gmaps_crawler.pipeline.extractors.page_extractors.plus_code import extract_plus_code
from gmaps_crawler.pipeline.extractors.page_extractors.social_media_url import extract_social_media_urls
from gmaps_crawler.pipeline.extractors.page_extractors.website import extract_website
from gmaps_crawler.pipeline.extractors.web_extractors.email_phone_social import extract_emails_phones_socials
import logging
from typing import Any, Callable, Dict, Optional

# NOTE: online geocoding removed; location uses provided city_name text


def extract_pipeline(page: ChromiumTab, browser: Chromium, city_name: str, *, place_id: Optional[str] = None) -> dict:
    """
    Extracts structured information (address, contact, socials, etc.) from a page.

    Args:
        page (ChromiumTab): The browser tab to extract data from.
        browser (Chromium): Browser instance for additional requests.
        city_name (str): Name of the city for location details.

    Returns:
        dict: A dictionary containing extracted business data.
    """
    
    warnings_list: list[dict] = []

    def safe_extract(extractor: Callable[..., Any], *args, default: Any = "", field_name: str = "") -> Any:
        """Safely runs an extraction function with consistent error handling.
        No warning logs are emitted for extractor fields per requirement.
        """
        try:
            return extractor(*args)
        except Exception as e:
            # Abbreviate ElementNotFoundError -> ENFE; otherwise use class name
            err_code = "ENFE" if isinstance(e, ElementNotFoundError) else e.__class__.__name__
            # store field + error only (no console logging)
            warnings_list.append({"field": field_name, "error": err_code})
            return default

    data: Dict[str, Any] = {
        "address": extract_address(page),  # required field, should raise if missing
        "location": city_name or "",
        "phone": safe_extract(extract_phone, page, default="", field_name="phone"),
        "plus_code": safe_extract(extract_plus_code, page, default="", field_name="plus code"),
        "website": safe_extract(extract_website, page, default="", field_name="website"),
        "social_media_urls": safe_extract(extract_social_media_urls, page, default=[], field_name="social media urls"),
        "open_time": safe_extract(extract_open_time, page, default="", field_name="open time"),
    }

    # Dependent extraction (needs website + socials)
    all_urls = [data.get("website", "")] + data.get("social_media_urls", [])
    all_urls = [u for u in all_urls if u]
    data["emails_phones_socials"] = safe_extract(
        extract_emails_phones_socials, browser, all_urls, default={}, field_name="emails/phones/socials"
    )

    # attach warnings list for per-place persistence
    data["warnings"] = warnings_list
    return data


if __name__ == "__main__":
    from DrissionPage import ChromiumPage
    from DrissionPage import ChromiumOptions

    co = (
        ChromiumOptions()
        .set_argument("--window-size=1920,1080")
        .set_local_port(64421)
        # .headless()
    )
    cp = ChromiumPage(co)
    cp.clear_cache()

    url2 = "https://www.google.com/maps/place/Caf%C3%A9+d%E2%80%99Auteur+-+Specialty+Coffee+shop+%26+roaster/@48.8244431,2.2406341,13z/data=!4m10!1m2!2m1!1scoffee!3m6!1s0x47e6716b341278c9:0x42925abdb9dd5f03!8m2!3d48.854221!4d2.338259!15sCgZjb2ZmZWVaCCIGY29mZmVlkgELY29mZmVlX3Nob3CaASRDaGREU1VoTk1HOW5TMFZKUTBGblNVTm9jaTExT0hsQlJSQUKqAUYKCS9tLzAydnFmbRABKgoiBmNvZmZlZSgAMh8QASIbBU737Hm4ft6hodqExuINT07eu8VxtxwQX3kcMgoQAiIGY29mZmVl4AEA-gEECAAQPw!16s%2Fg%2F11m_dd_szg?authuser=0&hl=en&entry=ttu&g_ep=EgoyMDI1MTAxNC4wIKXMDSoASAFQAw%3D%3D"
    cp.get("https://www.google.com/maps/place/Emilie+and+the+Cool+Kids/data=!4m7!3m6!1s0x47e67badd056802d:0x6541e257467da4ab!8m2!3d48.8300641!4d2.2453215!16s%2Fg%2F11td9h96mk!19sChIJLYBW0K175kcRq6R9RlfiQWU?authuser=0&hl=en&rclk=1")

    if "consent" in cp.url:
        more_ele = cp.ele("@text()=More options")
    print(more_ele.attrs)
    accept_ele = more_ele.parent(2).prev(2).children()[1]
    print(accept_ele.attrs)
    accept_ele.click()
        
    data = extract_pipeline(cp, cp, city_name="pairs")
    print("Extracted Data:", data)
    #Extracted Data: {'address': '337 Bleecker St, New York, NY 10014, United States', 'location': ('New York', 'New York', 'United States'), 'phone': '', 'plus_code': 'PXMW+F8 New York, USA', 'website': 'donotfeedalligators.com', 'social_media_urls': ['https://www.instagram.com/donotfeedalligators/', 'https://www.instagram.com/reel/DChn0euyrVi/', 'https://donotfeedalligators.com/', 'https://donotfeedalligators.com/?srsltid\\u003dAfmBOoqhS0MUAr_aTaEX-2E8lHrCgXVe9vGHqaFLw-NzkFoz5-LuqKev'], 'open_time': 'Monday8am–7pm\nTuesday8am–7pm\nWednesday8am–7pm\nThursday8am–11pm\nFriday8am–11pm\nSaturday8am–11pm\nSunday8am–7pm', 'emails_phones_socials': {'emails': [{'email': 'shop@dnfa.nyc', 'source_url': 'http://donotfeedalligators.com'}, {'email': 'events@dnfa.nyc', 'source_url': 'http://donotfeedalligators.com'}, {'email': 'jobs@dnfa.nyc', 'source_url': 'https://www.instagram.com/reel/DChn0euyrVi/'}], 'phones': [], 'socials': {'facebook': 'https://www.facebook.com/donotfeedalligators', 'instagram': 'https://www.instagram.com/reel/DChn0euyrVi/', 'twitter': 'https://twitter.com/dnfanyc', 'linkedin': '', 'youtube': '', 'tiktok': 'https://www.tiktok.com/@donotfeedalligators', 'whatsapp': '', 'telegram': '', 'yelp': ''}}}
    
    
    


