from __future__ import annotations

from typing import List, Optional
from DrissionPage import Chromium
from DrissionPage._pages.chromium_tab import ChromiumTab


class TabPool:
    def __init__(self, browser: Chromium, size: int, window_width: Optional[int], window_height: Optional[int]):
        import queue
        self._q: "queue.Queue" = queue.Queue()
        for _ in range(max(1, size)):
            tab = browser.new_tab(background=True)
            self._q.put(tab)

    def acquire(self) -> ChromiumTab:
        return self._q.get()

    def release(self, tab: ChromiumTab) -> None:
        self._q.put(tab)

    def close_all(self) -> None:
        errors: List[str] = []
        while True:
            try:
                tab = self._q.get_nowait()
            except Exception:
                break
            try:
                tab.close()
            except Exception as e:
                errors.append(str(e))
        if errors:
            raise RuntimeError("TabPool.close_all encountered errors: " + "; ".join(errors))

