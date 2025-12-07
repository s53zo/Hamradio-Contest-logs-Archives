#!/usr/bin/env python3
"""
Download the full CQ WW RTTY public log archive.

Directory layout:
    CQWWRTTY/
        <year>/<callsign>.log

The script scrapes https://cqwwrtty.com/publiclogs/ to discover year pages,
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


BASE_URL = "https://cqwwrtty.com/publiclogs/"
BASE_DIR = Path("CQWWRTTY")
WORKERS = 20
REQUEST_TIMEOUT = 30

# Shared lock for clean console output.
PRINT_LOCK = threading.Lock()


def fetch_text(url: str) -> str:
    """Fetch a URL and return decoded text."""
    with urllib.request.urlopen(url, timeout=REQUEST_TIMEOUT) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="ignore")


def discover_year_pages() -> Iterable[Tuple[str, str]]:
    """
    Yield (year, url) tuples for every year listing page.
    """
    html = fetch_text(BASE_URL)
    pattern = re.compile(r"href=['\"](?P<year>(19|20)\d{2})/['\"]")
    seen = set()
    for match in pattern.finditer(html):
        year = match.group("year")
        if year in seen:
            continue
        seen.add(year)
        full_url = urllib.parse.urljoin(BASE_URL, f"{year}/")
        yield year, full_url


def discover_logs(year: str, page_url: str) -> Iterable[Tuple[str, str]]:
    """
    Yield (year, log_url) tuples for every .log on the given listing page.
    """
    html = fetch_text(page_url)
    for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html, flags=re.IGNORECASE):
        log_url = urllib.parse.urljoin(page_url, href)
        yield year, log_url


def download_log(year: str, log_url: str) -> None:
    """Download a single log file into CQWWRTTY/<year>/."""
    filename = Path(urllib.parse.urlparse(log_url).path).name
    dest_dir = BASE_DIR / year
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filename

    if dest_path.exists():
        with PRINT_LOCK:
            print(f"skip (exists): {dest_path}")
        return

    try:
        with urllib.request.urlopen(log_url, timeout=REQUEST_TIMEOUT) as resp, open(dest_path, "wb") as fh:
            fh.write(resp.read())
        with PRINT_LOCK:
            print(f"ok   {dest_path}")
    except Exception as exc:  # pylint: disable=broad-except
        with PRINT_LOCK:
            print(f"fail {log_url}: {exc}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Download CQ WW RTTY public logs.")
    parser.add_argument(
        "--last",
        type=int,
        default=None,
        help="Limit to the most recent N years (default: all years).",
    )
    args = parser.parse_args()

    pages = list(discover_year_pages())
    if not pages:
        print("No year pages discovered. Is the site reachable?", file=sys.stderr)
        return 1

    # Process newest years first.
    pages.sort(key=lambda itm: int(itm[0]), reverse=True)

    if args.last:
        pages = pages[: args.last]

    print(f"Discovered {len(pages)} year pages (newest first)")
    log_tasks = []
    for year, page_url in pages:
        print(f"Scanning {year} -> {page_url}")
        logs = list(discover_logs(year, page_url))
        print(f"  found {len(logs)} logs")
        log_tasks.extend(logs)

    if not log_tasks:
        print("No log links found.", file=sys.stderr)
        return 1

    print(f"Found {len(log_tasks)} logs to download")

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [executor.submit(download_log, year, url) for year, url in log_tasks]
        for future in concurrent.futures.as_completed(futures):
            future.result()

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
