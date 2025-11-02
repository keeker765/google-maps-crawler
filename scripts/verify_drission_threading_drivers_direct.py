import importlib.util
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
mod_path = ROOT / 'src' / 'gmaps_crawler' / 'browser' / 'drivers.py'
spec = importlib.util.spec_from_file_location('drivers_local', str(mod_path))
mod = importlib.util.module_from_spec(spec)
sys.modules['drivers_local'] = mod
assert spec and spec.loader
spec.loader.exec_module(mod)  # type: ignore

create_browser = mod.create_browser  # type: ignore


def worker(tab):
    print(f"[{threading.current_thread().name}] start")
    try:
        print('  url:', tab.url)
    except Exception as e:
        print('  read url FAILED:', type(e).__name__, e)
    try:
        tab.get('https://www.google.com/maps')
        print('  get maps OK')
    except Exception as e:
        print('  get maps FAILED:', type(e).__name__, e)


def main():
    br = create_browser(headless=True, window_width=1280, window_height=720)
    try:
        tab = br.new_tab(background=True)
        t = threading.Thread(target=worker, args=(tab,), name='WorkerDriversDirect')
        t.start(); t.join()
    finally:
        try: tab.close()
        except Exception: pass
        br.quit(timeout=3, force=True, del_data=True)

if __name__ == '__main__':
    main()
