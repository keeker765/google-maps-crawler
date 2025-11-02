from typing import Union

from DrissionPage._elements.chromium_element import ChromiumElement
from DrissionPage.errors import ElementNotFoundError, ElementLostError
from DrissionPage import ChromiumPage

from gmaps_crawler.pipeline.extractors.utils import clean_strange_chars
from gmaps_crawler.utils.errors import OpenTimeFormatError


def extract_open_time(ele: Union[ChromiumElement, ChromiumElement]) -> str:
    """从地点元素中提取营业时间信息。
    Args:
        ele (ChromiumElement): 地点的 ChromiumElement 元素。
    Returns:
        str: 提取的营业时间字符串，如果未找到则返回空字符串。
    """
    address_copy = ele.ele("@aria-label=Copy address")
    parent4 = address_copy.parent(4)
    parent4_next = parent4.next()
    parent4_next.click()
    ele.wait(0.4)
    opentime_eles = ele.ele("@tag()=tbody").children()
    if len(opentime_eles) != 7:
        raise OpenTimeFormatError(f"open time elements length is not 7, but {len(opentime_eles)}")
    opentime_list = [clean_strange_chars(opentime_eles[i].text.replace(" ", "")) for i in range(7)]
    return "\n".join(opentime_list)

        

if __name__ == "__main__":
    from DrissionPage import ChromiumPage

    cp = ChromiumPage()
    url2 = "https://www.google.com/maps/place/Caf%C3%A9+d%E2%80%99Auteur+-+Specialty+Coffee+shop+%26+roaster/@48.8244431,2.2406341,13z/data=!4m10!1m2!2m1!1scoffee!3m6!1s0x47e6716b341278c9:0x42925abdb9dd5f03!8m2!3d48.854221!4d2.338259!15sCgZjb2ZmZWVaCCIGY29mZmVlkgELY29mZmVlX3Nob3CaASRDaGREU1VoTk1HOW5TMFZKUTBGblNVTm9jaTExT0hsQlJSQUKqAUYKCS9tLzAydnFmbRABKgoiBmNvZmZlZSgAMh8QASIbBU737Hm4ft6hodqExuINT07eu8VxtxwQX3kcMgoQAiIGY29mZmVl4AEA-gEECAAQPw!16s%2Fg%2F11m_dd_szg?authuser=0&hl=en&entry=ttu&g_ep=EgoyMDI1MTAxNC4wIKXMDSoASAFQAw%3D%3D"
    cp.get("https://www.google.com/maps/place/Caf%C3%A9+Ch%C3%A9rie+-+Brasserie+Bar+%C3%A0+Cocktail/@48.8332283,2.0910944,12z/data=!4m10!1m2!2m1!1zQ2Fmw6kgQ2jDqXJpZSA!3m6!1s0x47e67ae8cdd23175:0xb4d4489b8a325248!8m2!3d48.8332283!4d2.2435297!15sCg1DYWbDqSBDaMOpcmllWg8iDWNhZsOpIGNow6lyaWWSARRyZXN0YXVyYW50X2JyYXNzZXJpZZoBJENoZERTVWhOTUc5blMwVkpRMEZuU1VSMU1YSklSeTFCUlJBQqoBQgoLL2cvMXYyNmxmNHQQATIeEAEiGtyioYrpSntlgG9qrYrkbEGZZ19-dTtSlb8NMhEQAiINY2Fmw6kgY2jDqXJpZeABAPoBBAgAEEA!16s%2Fg%2F1v26lf4t?authuser=0&hl=en&entry=ttu&g_ep=EgoyMDI1MTAxNC4wIKXMDSoASAFQAw%3D%3D")

    address = extract_open_time(cp)
    print("Extracted Opentime:", address)
