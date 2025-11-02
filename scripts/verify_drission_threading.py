import threading
import time

try:
    from DrissionPage import Chromium, ChromiumOptions
except Exception:
    from DrissionPage import ChromiumOptions  # type: ignore
    from DrissionPage._base.chromium import Chromium  # type: ignore


def cross_thread_use_loaded(tab):
    print(f"[{threading.current_thread().name}] cross-thread (loaded tab)")
    for i in range(3):
        try:
            print(f"  try {i}: tab.url -> {tab.url}")
            _ = tab.run_js("return document.title;")
            time.sleep(0.2)
        except Exception as e:
            print("  cross-thread failure:", type(e).__name__, str(e))
            break


def cross_thread_use_blank_bg(tab):
    print(f"[{threading.current_thread().name}] cross-thread (blank background tab)")
    steps = [
        ("read_url_0", lambda: print("    url:", tab.url)),
        ("eles_probe", lambda: tab.eles("@aria-label=Accept all")),
        ("run_js_0", lambda: tab.run_js("return 1;")),
        ("get_blank", lambda: tab.get("about:blank")),
        ("read_url_1", lambda: print("    url:", tab.url)),
        ("get_google", lambda: tab.get("https://www.google.com")),
        ("run_js_1", lambda: tab.run_js("return document.title;")),
    ]
    for name, fn in steps:
        try:
            r = fn()
            if r is not None:
                print("   ", name, "->", r if isinstance(r, (str,int,float,bool)) else type(r).__name__)
        except Exception as e:
            print("   ", name, "FAILED:", type(e).__name__, str(e))
            break


def thread_local_ok():
    print(f"[{threading.current_thread().name}] creating & using in SAME thread")
    co = ChromiumOptions(); co.headless(True); co.set_argument('--window-size', '1280,720')
    br = Chromium(co)
    try:
        t = br.new_tab(url='https://www.google.com', background=False)
        print("    url:", t.url)
        _ = t.run_js("return document.title;")
        print("    thread-local OK")
        t.close()
    finally:
        br.quit(timeout=3, force=True, del_data=True)


def main():
    print(f"[{threading.current_thread().name}] MAIN: create browser & tabs")
    co = ChromiumOptions(); co.headless(True); co.set_argument('--window-size', '1280,720')
    browser = Chromium(co)
    try:
        # Loaded tab
        t1 = browser.new_tab(url='https://www.google.com', background=False)
        th1 = threading.Thread(target=cross_thread_use_loaded, args=(t1,), name='Worker1')
        th1.start(); th1.join()

        print("----")
        # Background blank tab
        t2 = browser.new_tab(background=True)
        th2 = threading.Thread(target=cross_thread_use_blank_bg, args=(t2,), name='Worker2')
        th2.start(); th2.join()

        print("----")
        # Thread-local success control
        th3 = threading.Thread(target=thread_local_ok, name='WorkerLocal')
        th3.start(); th3.join()
    finally:
        try:
            t1.close()
        except Exception:
            pass
        try:
            t2.close()
        except Exception:
            pass
        browser.quit(timeout=3, force=True, del_data=True)


if __name__ == '__main__':
    main()
