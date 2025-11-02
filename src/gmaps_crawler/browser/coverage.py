from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence, Tuple

from DrissionPage import ChromiumPage
from logger import crawler_thread_logger as logger
from DrissionPage.errors import ElementLostError, NoRectError

from gmaps_crawler.browser.drivers import IMPLICT_WAIT, create_browser
from gmaps_crawler.pipeline.utils import _dismiss_consent


UNIT_TO_METERS: Dict[str, float] = {
    "m": 1.0,
    "meter": 1.0,
    "metre": 1.0,
    "米": 1.0,
    "km": 1000.0,
    "kilometer": 1000.0,
    "kilometre": 1000.0,
    "公里": 1000.0,
    "千米": 1000.0,
    "mi": 1609.344,
    "mile": 1609.344,
    "英里": 1609.344,
    "ft": 0.3048,
    "feet": 0.3048,
    "英尺": 0.3048,
    "yd": 0.9144,
    "yard": 0.9144,
    "码": 0.9144,
}

SCALE_PATTERN = re.compile(r"([\d.,]+)\s*(m|km|mi|ft|yd|米|公里|千米|英里|码)", re.IGNORECASE)
SCALE_TEXT_XPATH = (
    "xpath://div[@role='contentinfo']//*[self::span or self::label or self::div]"
    "[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), ' m') "
    " or contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'km') "
    " or contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'mi') "
    " or contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'ft') "
    " or contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'yd') "
    " or contains(text(),'米') or contains(text(),'公里') or contains(text(),'千米') "
    " or contains(text(),'英里') or contains(text(),'码')]"
)


@dataclass(frozen=True)
class CoverageInfo:
    label_text: str
    meters: float
    bar_width_px: float
    meters_per_pixel: float
    viewport_width_px: float
    viewport_height_px: float
    viewport_width_m: float
    viewport_height_m: float
    window_width_px: float
    window_height_px: float
    device_pixel_ratio: float


def _wait_for_footer(page: ChromiumPage, timeout: float = 10.0) -> None:
    try:
        page.wait.ele_displayed("xpath://div[@role='contentinfo']", timeout=timeout)
        return
    except Exception as e:
        logger.debug("footer contentinfo not found: %s", e)
    try:
        page.wait.ele_displayed(
            "xpath://div[contains(@class,'scene-footer') or contains(@jsaction,'footer')]",
            timeout=max(2.0, timeout / 2),
        )
    except Exception as e:
        logger.debug("footer scene-footer not found: %s", e)


def _wait_for_scale_text(page: ChromiumPage, timeout: float = 10.0) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            ele = page.ele(SCALE_TEXT_XPATH, timeout=0.5)
            if ele and ele.text:
                return
        except Exception as e:
            logger.debug("scale text lookup failed: %s", e)
        time.sleep(0.5)


def _unit_multiplier(unit: str) -> Optional[float]:
    key = unit.strip().lower()
    if key in UNIT_TO_METERS:
        return UNIT_TO_METERS[key]
    return UNIT_TO_METERS.get(unit.strip(), None)


def _largest_canvas_dimensions(page: ChromiumPage) -> Tuple[float, float]:
    try:
        canvases = page.eles("tag:canvas")
    except Exception as e:
        logger.debug("canvas enumeration failed: %s", e)
        canvases = []

    max_area = 0.0
    best_width = 0.0
    best_height = 0.0
    for canvas in canvases or []:
        try:
            width, height = canvas.rect.size
        except (NoRectError, ElementLostError):
            continue
        if width <= 0 or height <= 0:
            continue
        area = width * height
        if area > max_area:
            max_area = area
            best_width = float(width)
            best_height = float(height)
    return best_width, best_height


def _run_scale_js(page: ChromiumPage) -> Optional[Dict[str, Any]]:
    container = None
    label_element = None
    selectors: Sequence[str] = ("css:#scale", "xpath://div[@id='scale']", SCALE_TEXT_XPATH)
    for selector in selectors:
        try:
            element = page.ele(selector, timeout=0.5)
        except Exception:
            element = None
        if not element:
            continue
        if selector == SCALE_TEXT_XPATH:
            label_element = element
            try:
                container = element.ele("xpath:..")
            except Exception:
                container = element
        else:
            container = element
            try:
                label_element = container.ele("tag:label", timeout=0.2)
            except Exception:
                label_element = None
        if container:
            break

    if not container:
        return None

    label_text = ""
    if label_element and label_element.text:
        label_text = label_element.text.strip()
    if not label_text:
        try:
            label_text = (container.attr("aria-label") or container.text or "").strip()
        except Exception as e:
            logger.debug("label fallback read failed: %s", e)
            label_text = ""
    if not label_text:
        try:
            fallback_label = page.ele(SCALE_TEXT_XPATH, timeout=0.2)
            if fallback_label and fallback_label.text:
                label_text = fallback_label.text.strip()
        except Exception as e:
            logger.debug("label_text ultimate fallback failed: %s", e)

    if not label_text:
        return None

    match = SCALE_PATTERN.search(label_text)
    if not match:
        return None

    try:
        numeric_value = float(match.group(1).replace(",", "."))
    except ValueError:
        return None

    multiplier = _unit_multiplier(match.group(2))
    if multiplier is None:
        return None
    meters = numeric_value * multiplier

    bar = None
    bar_selectors = (
        "css:#scale div.Ty7QWe",
        "xpath:.//div[contains(@class,'Ty7QWe')]",
        "xpath:.//*[contains(@style,'width') and contains(@style,'px')]",
    )
    for selector in bar_selectors:
        try:
            bar = container.ele(selector, timeout=0.2)
        except Exception as e:
            logger.debug("bar selector fail on %s: %s", selector, e)
            bar = None
        if bar:
            break
    if not bar:
        try:
            bar = page.ele("css:div.Ty7QWe", timeout=0.2)
        except Exception as e:
            logger.debug("bar page-level fallback failed: %s", e)
            bar = None
    if not bar:
        return None

    try:
        bar_width_px, bar_height_px = bar.rect.size
    except (NoRectError, ElementLostError):
        return None
    if bar_width_px <= 0:
        return None

    viewport_width_px, viewport_height_px = _largest_canvas_dimensions(page)
    if viewport_width_px <= 0 or viewport_height_px <= 0:
        try:
            viewport_width_px, viewport_height_px = page.rect.viewport_size
        except Exception as e:
            logger.debug("viewport_size read failed: %s", e)
            viewport_width_px = viewport_height_px = 0.0

    try:
        inner_width, inner_height = page.rect.viewport_size_with_scrollbar
    except Exception as e:
        logger.debug("viewport_size_with_scrollbar failed: %s", e)
        inner_width = viewport_width_px
        inner_height = viewport_height_px

    try:
        device_pixel_ratio = float(page.run_js("return window.devicePixelRatio;"))
    except Exception as e:
        logger.debug("devicePixelRatio js failed: %s", e)
        device_pixel_ratio = 1.0

    return {
        "labelText": label_text,
        "meters": meters,
        "barWidthPx": float(bar_width_px),
        "barHeightPx": float(bar_height_px),
        "metersPerPixel": meters / bar_width_px if bar_width_px else 0.0,
        "canvasRect": {"width": float(viewport_width_px), "height": float(viewport_height_px)},
        "window": {
            "innerWidth": float(inner_width),
            "innerHeight": float(inner_height),
            "devicePixelRatio": device_pixel_ratio,
        },
    }


def measure_map_coverage(
    page: ChromiumPage,
    *,
    attempts: int = 10,
    interval: float = 1.0,
    wait_before: float = 0.0,
    dismiss_dialog: bool = True,
) -> Optional[CoverageInfo]:
    if wait_before > 0:
        time.sleep(wait_before)

    if dismiss_dialog:
        _dismiss_consent(page)

    _wait_for_footer(page, max(5.0, wait_before))
    _wait_for_scale_text(page, timeout=max(5.0, wait_before + IMPLICT_WAIT))

    for _ in range(max(1, attempts)):
        data: Optional[Dict[str, Any]] = _run_scale_js(page)
        if not data:
            time.sleep(interval)
            continue
        
        left_bar_widh = (page.ele("@text()=Saved").parent().rect.size[0] + 
                         page.ele("@aria-label:Results for").parent().rect.size[0])
        # print(left_bar_widh)

        canvas = data.get("canvasRect") or {}
        window_info = data.get("window") or {}
        viewport_width_px = float(canvas.get("width", 0.0)) - left_bar_widh
        viewport_height_px = float(canvas.get("height", 0.0))
        meters_per_pixel = float(data.get("metersPerPixel", 0.0))
        viewport_width_m = viewport_width_px * meters_per_pixel
        viewport_height_m = viewport_height_px * meters_per_pixel

        return CoverageInfo( 
            label_text=str(data.get("labelText", "")),
            meters=float(data.get("meters", 0.0)),
            bar_width_px=float(data.get("barWidthPx", 0.0)),
            meters_per_pixel=meters_per_pixel,
            viewport_width_px=viewport_width_px,
            viewport_height_px=viewport_height_px,
            viewport_width_m=viewport_width_m,
            viewport_height_m=viewport_height_m,
            window_width_px=float(window_info.get("innerWidth", 0.0)),
            window_height_px=float(window_info.get("innerHeight", 0.0)),
            device_pixel_ratio=float(window_info.get("devicePixelRatio", 1.0)),
        )

    return None


if __name__ == "__main__":  # pragma: no cover - manual smoke test
    from dataclasses import asdict
    print("[coverage] Starting smoke test (Chromium + tab).")
    browser = create_browser(headless=False)
    try:
        search_url = "https://www.google.com/maps/search/Coffee+Store/@48.86421425611068,2.2559637842382134,15z?hl=en"
        tab = browser.new_tab(url=search_url, background=False)
        coverage = measure_map_coverage(tab, attempts=5, interval=1.0, wait_before=3.0)
        if coverage:
            print("[coverage] Measurement succeeded:")
            for key, value in asdict(coverage).items():
                print(f"  {key}: {value}")
        else:
            print("[coverage] Measurement failed (no scale information).")
    finally:
        try:
            browser.quit(timeout=5, force=True, del_data=False)
        except Exception:
            pass
