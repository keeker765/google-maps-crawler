from __future__ import annotations

from logger import crawler_thread_logger as logger
import time
import json
from typing import List, Optional

from DrissionPage import ChromiumPage
from DrissionPage._elements.chromium_element import ChromiumElement
from DrissionPage._pages.chromium_tab import ChromiumTab
from DrissionPage.errors import ContextLostError, WaitTimeoutError

from gmaps_crawler.pipeline.city.context import RunContext, TileContext
from gmaps_crawler.pipeline.search.navigator import GMapsNavigator
from gmaps_crawler.ui.selectors import extract_card_info
from gmaps_crawler.pipeline.extractors import extract_pipeline
from gmaps_crawler.utils.geo_id import parse_lat_lng_from_href, make_place_id_from_latlng
from gmaps_crawler.utils.time import now_iso
from gmaps_crawler.utils.errors import PlaceMissingAddress
from gmaps_crawler.pipeline.exec.stop import STOP_EVENT

# use named crawler logger


class GMapsPlacesCrawler:
    WAIT_SECONDS_RESTAURANT_TITLE = 10

    def __init__(self, *, browser, search_tab: ChromiumPage, proxy_pool: Optional[object] = None) -> None:
        self.browser = browser
        self.search_tab = search_tab
        self.navigator = GMapsNavigator(search_tab)
        self.proxy_pool = proxy_pool

    def _safe_scroll_into_view(self, element: Optional[ChromiumElement]) -> None:
        if not element:
            return
        try:
            if not element.states.is_displayed:
                element.scroll.to_see(center=True)
                time.sleep(0.2)
            else:
                element.scroll.to_see(center=True)
                time.sleep(0.1)
        except ContextLostError:
            raise
        except Exception as e:
            logger.error("scroll_into_view failed: %s", e)
            raise

    def wait_restaurant_title_show(self, tab: ChromiumTab) -> None:
        try:
            tab.wait.ele_displayed('xpath://h1[text() != ""]', timeout=self.WAIT_SECONDS_RESTAURANT_TITLE)
        except WaitTimeoutError:
            logger.warning("Restaurant title did not appear within %s seconds.", self.WAIT_SECONDS_RESTAURANT_TITLE)

    def get_place_details(
        self,
        tab: ChromiumTab,
        place_name: str,
        card_html: str,
        href: str,
        run_ctx: RunContext,
        tile_ctx: TileContext,
        query_text: str,
    ) -> None:
        self.wait_restaurant_title_show(tab)

        header = tab.ele(f"@aria-label={place_name}")
        restaurant_name = header.attr("aria-label") if header else ""
        if not restaurant_name:
            logger.error("Restaurant name not found on the page.")
            raise RuntimeError("Restaurant name not found; page structure may have changed.")

        lat, lng = parse_lat_lng_from_href(href)
        place_id = make_place_id_from_latlng(lat, lng)

        data = extract_pipeline(tab, browser=self.browser, city_name=run_ctx.city, place_id=place_id)
        if not (data.get("address") or "").strip():
            raise PlaceMissingAddress(f"Missing address for place '{restaurant_name}' href={href}")
        address = data.get("address", "") or ""
        loc = data.get("location") or {}
        if isinstance(loc, dict):
            location_text = ",".join(str(loc.get(k, "")) for k in ("city", "state", "country")).strip(",")
        else:
            location_text = str(loc)
        phone = data.get("phone", "") or ""
        plus_code = data.get("plus_code", "") or ""
        website = data.get("website", "") or ""
        sm_urls = data.get("social_media_urls") or []
        sm_urls_text = json.dumps(sm_urls, ensure_ascii=False)
        open_time_text = str(data.get("open_time", "") or "")
        eps = data.get("emails_phones_socials") or {}
        eps_text = json.dumps(eps, ensure_ascii=False)
        warnings_list = data.get("warnings") or []
        warnings_text = json.dumps(warnings_list, ensure_ascii=False)

        extracted_at = now_iso()
        if run_ctx.db:
            run_ctx.db.upsert_place_struct(
                place_id=place_id,
                city=run_ctx.city,
                query=query_text,
                tile_index=tile_ctx.index,
                name=restaurant_name,
                href=href,
                lat=float(lat),
                lng=float(lng),
                address=address,
                location=location_text,
                phone=phone,
                plus_code=plus_code,
                website=website,
                social_media_urls=sm_urls_text,
                open_time=open_time_text,
                emails_phones_socials=eps_text,
                warnings=warnings_text,
                extracted_at=extracted_at,
                run_id=run_ctx.run_id,
            )

    def _gather_all_cards(self, query_text: str) -> list[dict]:
        """Scroll the results list to the end and collect visible cards (name, href, card_html)."""
        gathered: list[dict] = []
        self.navigator._scroll_until_end(query_text)
        items = self.navigator._get_places_wrapper(query_text)
        logger.info("gather_cards count=%s", len(items))
        for el in items or []:
            try:
                info = extract_card_info(el)
            except Exception as e:
                logger.warning("extract_card_info failed: %s", e)
                continue
            if info and info.get("href"):
                gathered.append(info)
        return gathered

    def get_places(self, run_ctx: RunContext, tile_ctx: TileContext, query_text: str) -> tuple[int, int, str]:
        cards = self._gather_all_cards(query_text=query_text)
        seen = len(cards)
        inserted = 0
        errors: list[str] = []
        # reuse a single detail tab
        detail_tab: ChromiumTab = self.browser.new_tab(background=False)
        for info in cards:
            if STOP_EVENT.is_set():
                break
            place_name = info.get("name") or ""
            href = info.get("href") or ""
            card_html = info.get("card_html") or ""
            if not href:
                errors.append(f"Empty href for place '{place_name}'")
                continue
            try:
                lat_sk, lng_sk = parse_lat_lng_from_href(href)
            except Exception:
                errors.append(f"Href parse failed for place '{place_name}': {href}")
                continue
            place_id_sk = make_place_id_from_latlng(lat_sk, lng_sk)
            if run_ctx.db and run_ctx.db.place_exists(run_ctx.city, query_text, place_id_sk):
                continue
            try:
                detail_tab.get(href)
                self.get_place_details(detail_tab, place_name, card_html, href, run_ctx, tile_ctx, query_text)
                inserted += 1
            except PlaceMissingAddress as exc:
                errors.append(str(exc))
                continue
            except Exception as exc:
                errors.append(f"extract failed: {exc}")
                continue
        try:
            detail_tab.close()
        except Exception as e:
            logger.error("detail_tab.close failed: %s", e)
            raise
        return seen, inserted, ("; ".join(errors[:10]) if errors else "")
