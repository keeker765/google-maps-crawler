import threading
import time
from pathlib import Path
import sys

# Add project src to sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'src'))

from gmaps_crawler.browser.drivers import create_browser
from gmaps_crawler.pipeline.utils import _dismiss_consent


def worker_use_tab(tab):
    print(f"[{threading.current_thread().name}] worker starting")
    try:
        print("  read url:", tab.url)
    except Exception as e:
        print("  read url FAILED:", type(e).__name__, str(e))
    try:
        r = _dismiss_consent(tab)
        print("  dismiss_consent:", r)
    except Exception as e:
        print("  dismiss_consent FAILED:", type(e).__name__, str(e))
    try:
        tab.get('https://www.google.com/maps')
        print("  get maps OK")
    except Exception as e:
        print("  get maps FAILED:", type(e).__name__, str(e))


def main():
    br = create_browser(headless=True, window_width=1280, window_height=720)
    try:
        # mimic TabPool background tab
        tab = br.new_tab(background=True)
        print(f"[MainThread] created background tab: {tab}")
        th = threading.Thread(target=worker_use_tab, args=(tab,), name='WorkerRepo')
        th.start(); th.join()
    finally:
        try:
            tab.close()
        except Exception:
            pass
        br.quit(timeout=3, force=True, del_data=True)


if __name__ == '__main__':
    main()
