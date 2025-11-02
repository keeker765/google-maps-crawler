import random
from logger import crawler_thread_logger as logger

from DrissionPage._pages.chromium_tab import ChromiumTab


def _dismiss_consent(tab: ChromiumTab) -> bool:
    if "consent" in tab.url:
        try:
            accept_eles = tab.eles("@aria-label=Accept all")
            [accept_ele.click() for accept_ele in accept_eles]
        except Exception as e:
            logger.error("dismiss consent primary failed: %s", e)
            try:
                more_ele = tab.ele("@text()=More options")
                accept_ele = more_ele.parent(2).prev(2).children()[1]
                accept_ele.click()
                return True if tab.wait.url_change("consent", exclude=False, timeout=0.3, raise_err=True) else False
            except Exception as e2:
                logger.error("dismiss consent fallback failed: %s", e2)
                return False
        
    return True


def local_search_click(tab: ChromiumTab) -> bool:
    def _local_search_click(tab: ChromiumTab) -> bool:
        map_ele = tab.ele("@class=id-content-container")
        x = 500
        map_ele.drag(0, x//2, random.uniform(0.2, 1))
        map_ele.drag(0, x//2, random.uniform(0.2, 1))
        map_ele.drag(0, -x//2, random.uniform(0.2, 1))
        map_ele.drag(0, -x//2, random.uniform(0.2, 1))
        tab.wait(0.3)
        tab.listen.start("https://www.google.com/search?")
        if tab.wait.eles_loaded("@aria-label=Search this area", timeout=2, raise_err=False):
            for _ in range(5):
                try:
                    loca_search_ele = tab.ele("@aria-label=Search this area")
                    loca_search_ele.click()
                    loca_search_ele.wait.disabled_or_deleted()
                    return True
                except Exception as e:
                    logger.debug("local_search_click attempt failed: %s", e)
        else:
            return False

    res = _local_search_click(tab)
    import time
    time.sleep(5)
    return True if tab.listen.wait(timeout=3) else False
        
        
# def _get_places_wrapper2(tab):
#     xpath = "//a[contains(@href, 'https://www.google.com/maps/place/') and @jsaction]"
#     links = tab.eles(f"xpath:{xpath}") or []
#     return links

def get_places_wrapper(tab, query_text):
    alias =  {
    "coffee":["coffee", "cafe", "expresso bar"]
}
    coffee = query_text.split(" ")[0].lower()
    coffees = alias[coffee]
    xpath = "//a[contains(@href, 'https://www.google.com/maps/place/') and @jsaction]"
    links = tab.eles(f"xpath:{xpath}") or []
    filter_links = []
    # use shared crawler logger imported at module level
    if links:
        for _, l_ele in enumerate(links):
            for coffee in coffees:
                if coffee in l_ele.next(2).text.lower():
                    logger.debug("filter match text=%s", l_ele.next(2).text.lower())
                    filter_links.append(l_ele)
                    break
             
    return filter_links

def _scroll_until_end(tab, name: str) -> None:
    import time
    scroll_selector = f"@aria-label=Results for {name}"
    scroll_container = tab.ele(scroll_selector)
    end_selector = "@text():You've reached the end of the list."
    max_steps = 120
    import random
    reach_ele = None
    # warm up a few scrolls to trigger initial population
    for _ in range(3):
        try:
            scroll_container.scroll(600)
            time.sleep(0.2)
        except Exception:
            break
    for _ in range(max_steps):
        scroll_container.scroll(900)
        time.sleep(random.uniform(0.2, 0.5))
        try:
            reach_ele = scroll_container.ele(end_selector, timeout=0.5)
            if reach_ele:
                while not reach_ele.states.is_whole_in_viewport:
                    scroll_container.scroll(700)
                    time.sleep(random.uniform(0.2, 0.4))
                break
        except Exception:
            continue
    if not reach_ele:
        raise ValueError("End-of-list not reached within max scroll attempts.")
    
def _scroll_until_start(tab, name):
    scroll_selector = f"@aria-label=Results for {name}"
    scroll_container = tab.ele(scroll_selector)
    scroll_container.scroll.to_top()
        
if __name__ == "__main__":
    from DrissionPage import ChromiumPage, ChromiumOptions
    co = ChromiumOptions()                       
    # co.set_argument('--disable-gpu')         # 禁用 GPU
    # co.set_user_agent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ...')
    # co.set_argument('--disable-blink-features=AutomationControlled')
    co.headless(False)
    co.set_local_port(48694)
    co.set_argument('--window-size', "1920,1080")
    cp = ChromiumPage(co)
    url2 = "https://www.google.com/maps/place/Caf%C3%A9+d%E2%80%99Auteur+-+Specialty+Coffee+shop+%26+roaster/@48.8244431,2.2406341,13z/data=!4m10!1m2!2m1!1scoffee!3m6!1s0x47e6716b341278c9:0x42925abdb9dd5f03!8m2!3d48.854221!4d2.338259!15sCgZjb2ZmZWVaCCIGY29mZmVlkgELY29mZmVlX3Nob3CaASRDaGREU1VoTk1HOW5TMFZKUTBGblNVTm9jaTExT0hsQlJSQUKqAUYKCS9tLzAydnFmbRABKgoiBmNvZmZlZSgAMh8QASIbBU737Hm4ft6hodqExuINT07eu8VxtxwQX3kcMgoQAiIGY29mZmVl4AEA-gEECAAQPw!16s%2Fg%2F11m_dd_szg?authuser=0&hl=en&entry=ttu&g_ep=EgoyMDI1MTAxNC4wIKXMDSoASAFQAw%3D%3D"
    cp.get("https://www.google.com/maps/search/Coffee+Store/@48.82768585966042,2.250507220347744,15z?hl=en")
    # _dismiss_consent(cp)
    local_search_click(cp)
    
    q = "Coffee Store" 
    _scroll_until_end(cp, q)
    print(len(get_places_wrapper(cp, q)))

    # print(_scroll_until_start(cp, "coffee shop in Paris"))
    print(local_search_click(cp))
    
    # cp.quit()

    
