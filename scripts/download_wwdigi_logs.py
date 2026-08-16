#!/usr/bin/env python3
"""
Download the full WW DIGI public log archive.

Directory layout:
    WWDIGI/<year>/<callsign>.log

The script scrapes https://ww-digi.com/publiclogs/ to discover year pages,
extracts all .log links, and downloads them with a thread pool.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import re
import sys
import threading
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Iterable, Tuple

from archive_storage import archive_log_exists, atomic_write_bytes
from task_ledger import TASK_LEDGER_PATH, TaskLedger, task_mark_complete, task_should_skip


BASE_URL = "https://ww-digi.com/publiclogs/"
BASE_DIR = Path("WWDIGI")
WORKERS = 20
REQUEST_TIMEOUT = 30
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# Shared lock for clean console output.
PRINT_LOCK = threading.Lock()
TASK_LEDGER: "TaskLedger | None" = None


def destination_log_exists(path: Path) -> bool:
    try:
        return archive_log_exists(path)
    except ValueError:
        return path.exists()


def fetch_text(url: str) -> str:
    """Fetch a URL and return decoded text."""
    req = urllib.request.Request(url, headers=HTTP_HEADERS)
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="ignore")


def discover_year_pages() -> Iterable[Tuple[str, str]]:
    """Yield (year, url) tuples for every year listing page."""
    html = fetch_text(BASE_URL)
    seen: set[str] = set()
    for match in re.finditer(r"href=['\"](?P<year>(19|20)\d{2})/['\"]", html, flags=re.IGNORECASE):
        year = match.group("year")
        if year in seen:
            continue
        seen.add(year)
        full_url = urllib.parse.urljoin(BASE_URL, f"{year}/")
        yield year, full_url


def discover_logs(year: str, page_url: str) -> Iterable[Tuple[str, str]]:
    """Yield (year, log_url) tuples for every .log on the given listing page."""
    html = fetch_text(page_url)
    for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html, flags=re.IGNORECASE):
        log_url = urllib.parse.urljoin(page_url, href)
        yield year, log_url


def download_log(year: str, log_url: str) -> dict[str, int]:
    """Download a single log file into WWDIGI/<year>/."""
    filename = Path(urllib.parse.urlparse(log_url).path).name
    dest_dir = BASE_DIR / year
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filename

    if destination_log_exists(dest_path):
        with PRINT_LOCK:
            print(f"skip (exists): {dest_path}")
        return {"skip": 1}

    try:
        req = urllib.request.Request(log_url, headers=HTTP_HEADERS)
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            content = resp.read()
        atomic_write_bytes(dest_path, content)
        with PRINT_LOCK:
            print(f"ok   {dest_path}")
        return {"ok": 1}
    except Exception as exc:  # pylint: disable=broad-except
        with PRINT_LOCK:
            print(f"fail {log_url}: {exc}")
        return {"error": 1}


def main() -> int:
    parser = argparse.ArgumentParser(description="Download WW DIGI public logs.")
    parser.add_argument(
        "--last",
        type=int,
        default=None,
        help="Limit to the most recent N years (default: all years).",
    )
    parser.add_argument(
        "--task-ledger",
        type=Path,
        default=TASK_LEDGER_PATH,
        help="SQLite task ledger (default: state/downloads/tasks.sqlite).",
    )
    parser.add_argument(
        "--no-task-ledger",
        action="store_true",
        help="Disable task ledger usage.",
    )
    args = parser.parse_args()
    global TASK_LEDGER
    TASK_LEDGER = None if args.no_task_ledger else TaskLedger(args.task_ledger)

    pages = list(discover_year_pages())
    if not pages:
        print("No year pages discovered. Is the site reachable?", file=sys.stderr)
        return 1

    # Process newest years first.
    pages.sort(key=lambda itm: int(itm[0]), reverse=True)

    if args.last:
        pages = pages[: args.last]

    print(f"Discovered {len(pages)} year pages (newest first)")
    total_logs = 0
    for year, page_url in pages:
        print(f"Scanning {year} -> {page_url}")
        logs = list(discover_logs(year, page_url))
        print(f"  found {len(logs)} logs")
        if not logs:
            continue
        total_logs += len(logs)
        task_key = f"{Path(__file__).stem}:{year}"
        skip, list_hash, item_count = task_should_skip(TASK_LEDGER, task_key, [url for _y, url in logs])
        if skip:
            print(f"  skip (task ledger): {year} items={item_count}")
            continue
        counts = {"ok": 0, "skip": 0, "error": 0}
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = [executor.submit(download_log, year, url) for year, url in logs]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                for key, value in result.items():
                    counts[key] = counts.get(key, 0) + value
        if counts.get("error", 0) == 0:
            task_mark_complete(TASK_LEDGER, task_key, list_hash, item_count)

    if total_logs == 0:
        print("No log links found.", file=sys.stderr)
        return 1

    print("Done.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        if TASK_LEDGER is not None:
            TASK_LEDGER.close()
