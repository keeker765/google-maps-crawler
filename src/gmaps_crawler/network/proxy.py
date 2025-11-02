from __future__ import annotations

import itertools
import random
from collections import deque
from typing import Iterable, Optional


class ProxyPool:
    """Simple proxy pool with round-robin or random selection."""

    def __init__(self, proxies: Iterable[str], strategy: str = "round_robin") -> None:
        cleaned = [proxy.strip() for proxy in proxies if proxy and proxy.strip()]
        self._proxies = deque(cleaned)
        self._strategy = strategy
        self._random_iter = itertools.repeat(0)

    def has_proxies(self) -> bool:
        return bool(self._proxies)

    def next_proxy(self) -> Optional[str]:
        if not self._proxies:
            return None

        if self._strategy == "random":
            return random.choice(list(self._proxies))

        proxy = self._proxies[0]
        self._proxies.rotate(-1)
        return proxy


def parse_proxy_sources(proxy_string: Optional[str] = None, file_path: Optional[str] = None) -> list[str]:
    """Parse proxies from comma-separated string and/or file."""
    proxies: list[str] = []
    if proxy_string:
        proxies.extend([item.strip() for item in proxy_string.split(",") if item.strip()])

    if file_path:
        with open(file_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                proxies.append(line)

    return proxies

