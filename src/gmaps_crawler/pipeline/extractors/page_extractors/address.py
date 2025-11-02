import re
from typing import Union

from DrissionPage._elements.chromium_element import ChromiumElement
from DrissionPage._pages.chromium_tab import ChromiumTab

from gmaps_crawler.pipeline.extractors.utils import clean_strange_chars


def extract_address(ele: Union[ChromiumTab, ChromiumElement]) -> str:
    """从地点元素中提取地址信息。

    Args:
        ele (ChromiumElement): 地点的 ChromiumElement 元素。

    Returns:
        str: 提取的地址字符串，如果未找到则返回空字符串。
    """
    address_copy = ele.ele("@aria-label=Copy address")
    parent4_text = address_copy.parent(4).text
    return clean_strange_chars(parent4_text) if parent4_text is not None else ""



if __name__ == "__main__":
    from DrissionPage import ChromiumPage

    cp = ChromiumPage()
    cp.get("https://www.google.com/maps/place/Caf%C3%A9+Ch%C3%A9rie+-+Brasserie+Bar+%C3%A0+Cocktail/@48.8332283,2.0910944,12z/data=!4m10!1m2!2m1!1zQ2Fmw6kgQ2jDqXJpZSA!3m6!1s0x47e67ae8cdd23175:0xb4d4489b8a325248!8m2!3d48.8332283!4d2.2435297!15sCg1DYWbDqSBDaMOpcmllWg8iDWNhZsOpIGNow6lyaWWSARRyZXN0YXVyYW50X2JyYXNzZXJpZZoBJENoZERTVWhOTUc5blMwVkpRMEZuU1VSMU1YSklSeTFCUlJBQqoBQgoLL2cvMXYyNmxmNHQQATIeEAEiGtyioYrpSntlgG9qrYrkbEGZZ19-dTtSlb8NMhEQAiINY2Fmw6kgY2jDqXJpZeABAPoBBAgAEEA!16s%2Fg%2F1v26lf4t?authuser=0&hl=en&entry=ttu&g_ep=EgoyMDI1MTAxNC4wIKXMDSoASAFQAw%3D%3D")

    address = extract_address(cp)
    print("Extracted Address:", address)
