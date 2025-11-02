# Google Maps Crawler (DrissionPage Edition)

This project crawls Google Maps search results using [DrissionPage](https://github.com/g1879/DrissionPage). It scrolls through listings, opens each place, and stores structured details (name, address, business hours, traits, reviews, photo link, plus code) via the selected storage backend. By default, results append to a CSV so downstream analysis in pandas is straightforward.

---

## Quick Start

1. **Create a Conda environment**
   ```bash
   conda create -n gmaps python=3.9
   conda activate gmaps
   ```
2. **Install dependencies (editable mode recommended)**
   ```bash
   pip install -e .[dev]
   # or runtime-only:
   pip install -r requirements.txt
   ```
3. **Run the crawler (CLI)**
   ```bash
   python -m gmaps_crawler.cli.run_city "Paris" "coffee shops in Paris" --country France --headless --limit 1
   ```
   Records append to `data/places.csv`. Override the output with:
   ```bash
   $Env:SCRAPED_EVENT_CSV_PATH = "exports/my_run.csv"   # PowerShell
   export SCRAPED_EVENT_CSV_PATH=exports/my_run.csv     # bash/zsh
   ```
   Or use the Python API directly:
   ```python
   from gmaps_crawler import run_city
   run_city("Paris", "coffee shops in Paris", headless=True, limit=1)
   ```
   Append `--headless` to run Chromium without a visible window.
4. **Switch storage backends (optional)**
   - Debug printout: `export STORAGE_MODE=DEBUG`
   - AWS SQS: `export STORAGE_MODE=SQS` and set `SCRAPED_EVENT_SQS_URL`

---

## CSV Output Format

Each row contains:

| Column            | Description                                                       |
|-------------------|-------------------------------------------------------------------|
| `name`            | Place name                                                        |
| `address`         | Street address                                                    |
| `business_hours`  | JSON string of opening hours                   |
| `photo_link`      | URL of the header image                                           |
| `rate` / `reviews`| Rating text and review count                                      |
| `extra_attrs`     | JSON string of misc attributes                  |
| `traits`          | JSON-encoded categories (service options, amenities, etc.)        |
| `identifier`      | Plus code (if available)                                          |

Use pandas to load the file:

```python
import pandas as pd
import json
df = pd.read_csv("data/places.csv")
df["business_hours"] = df["business_hours"].apply(lambda x: json.loads(x))
```

---

## Defaults

- Resolution: 1920 x 1080 (fixed)
- Coverage probe (measured once per city and reused):
  - coverage_wait = 3.0 s
  - coverage_attempts = 5
  - coverage_interval = 0.5 s

These are hard-coded in api.run_city() and pipeline/city/crawl_city.py ！ flags with other values are ignored.

## Pipeline Layout

Code lives under src/gmaps_crawler/ with clear layers:

- pipeline/city
  - crawl_city.py ！ city orchestrator (bbox ★ grid ★ tiles)
  - context.py ！ RunContext/TileContext
  - grid.py ！ grid generation
- pipeline/tile
  - runner.py ！ single-tile execution (compose modules)
  - session.py ！ browser session (open/search/consent)
  - tab_pool.py ！ detail tab pool
- pipeline/search
  - cards.py ！ collect result cards
  - navigator.py ！ list scrolling / end-of-list detection
  - places_crawler.py ！ UI helpers
  - urls.py ！ search URL builder
- pipeline/tasks
  - build.py ！ build task list (href ★ place_id, DB dedupe)
  - worker.py ！ per-place extract worker (returns success/failed payload)
- pipeline/exec
  - scheduler.py ！ progressive thread scheduler
  - streaming.py ！ streaming runner (feed writer as results complete)
  - stop.py ！ STOP_EVENT and signal install
- pipeline/io
  - writer.py ！ single-writer thread (DB upserts, progress logs)

## Development Notes

- Formatting: black src tests and isort src tests
- Tests: pytest -q
- Contributor guide: docs_en/AGENTS.md

Keep a separate .env for secrets (custom CSV paths, proxies). DrissionPage uses a fresh user-data-dir unless customized in drivers.create_browser().
