"""gmaps_crawler package public API.

Avoid importing heavy submodules at package import time to prevent side effects
(e.g., requiring DrissionPage when only configuration is needed). Use lazy
attribute access to import on first use.
"""

__all__ = ["run_city", "rerun_place", "retry_failed_places"]


def __getattr__(name):  # lazy re-export
    if name in __all__:
        from . import api as _api

        return getattr(_api, name)
    raise AttributeError(name)
