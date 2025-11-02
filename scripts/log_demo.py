"""
Emit a few log lines to verify console logging format.

Usage:
  python scripts/log_demo.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

# Import config to install our logging config
import logger  # importing applies basicConfig from your logger.py
import logging


logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("VS Code clickable log demo from log_demo.py")
    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("Example exception with path/line")


if __name__ == "__main__":
    main()
