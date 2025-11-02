from typing import List

from DrissionPage._pages.chromium_tab import ChromiumTab

from gmaps_crawler.pipeline.extractors.utils import extract_https_in_quotes


def remove_contained_urls(urls: List[str]) -> List[str]:
    result: List[str] = []
    for url in urls:
        if not any(url != other and url in other for other in urls):
            result.append(url)
    return result


def extract_social_media_urls(page: ChromiumTab) -> List[str]:
    page.listen.start('google.com/search?q=local+guide+program', method='GET')

    address_copy = page.ele("@data-value=Copy address")
    parent6 = address_copy.parent(6)
    for _ in range(5):
        page.actions.scroll(delta_x=0, delta_y=1000 * (_ + 1), on_ele=parent6)
        packet = page.listen.wait(timeout=1)
        if packet:
            break

    if packet:
        html_content = packet.response.body
        page.listen.stop()
    else:
        raise RuntimeError("No response received for the monitored request.")

    result = extract_https_in_quotes(html_content)
    if not result:
        return []
    return remove_contained_urls(result)

