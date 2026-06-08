#!/usr/bin/env python3
"""
Downloader for Istra Open Contest public logs.

Public logs are published as direct Cabrillo links in year-specific folders:
  https://ioc.9a1p.com/public_logs_2026/

Output layout:
  Istra_Open_Contest/<year>/<CALL>.log
"""

from __future__ import annotations

import argparse
import html
import random
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from task_ledger import TASK_LEDGER_PATH, TaskLedger, task_mark_complete, task_should_skip


def pick_user_agent() -> str:
    chrome_major = random.randint(120, 126)
    chrome_build = random.randint(0, 9999)
    chrome_patch = random.randint(0, 199)
    firefox_major = random.randint(120, 126)
    safari_major = random.choice([16, 17])
    safari_minor = random.randint(0, 6)
    safari_patch = random.randint(0, 9)
    ua_pool = [
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{chrome_major}.0.{chrome_build}.{chrome_patch} Safari/537.36"
        ),
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{chrome_major}.0.{chrome_build}.{chrome_patch} Safari/537.36"
        ),
        (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{chrome_major}.0.{chrome_build}.{chrome_patch} Safari/537.36"
        ),
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            f"Version/{safari_major}.{safari_minor}.{safari_patch} Safari/605.1.15"
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:"
            f"{firefox_major}.0) Gecko/20100101 Firefox/{firefox_major}.0"
        ),
    ]
    return random.choice(ua_pool)


USER_AGENT = pick_user_agent()
REQUEST_TIMEOUT = 30
OUTPUT_ROOT = Path("Istra_Open_Contest")
PUBLIC_LOG_URLS = ("https://ioc.9a1p.com/public_logs_2026/",)
LOG_EXTS = (".log", ".cbr", ".txt")
TASK_LEDGER: "TaskLedger | None" = None


def fetch_text(url: str, headers: Dict[str, str] | None = None, retries: int = 3, delay: float = 1.0) -> str:
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req_headers = {"User-Agent": USER_AGENT}
            if headers:
                req_headers.update(headers)
            req = urllib.request.Request(url, headers=req_headers)
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                return resp.read().decode(charset, errors="ignore")
        except Exception as exc:  # pylint: disable=broad-except
            last_exc = exc
            if attempt + 1 < retries:
                time.sleep(delay * (2 ** attempt))
            else:
                raise
    raise last_exc  # type: ignore[misc]


def fetch_bytes(url: str, headers: Dict[str, str] | None = None, retries: int = 3, delay: float = 1.0) -> bytes:
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req_headers = {"User-Agent": USER_AGENT}
            if headers:
                req_headers.update(headers)
            req = urllib.request.Request(url, headers=req_headers)
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                return resp.read()
        except Exception as exc:  # pylint: disable=broad-except
            last_exc = exc
            if attempt + 1 < retries:
                time.sleep(delay * (2 ** attempt))
            else:
                raise
    raise last_exc  # type: ignore[misc]


def year_from_public_logs_url(url: str) -> int:
    match = re.search(r"public_logs_((?:19|20)\d{2})/?", url)
    if not match:
        raise ValueError(f"Unable to derive year from public logs URL: {url}")
    return int(match.group(1))


def normalize_call_from_filename(path_name: str) -> str:
    name = urllib.parse.unquote(Path(path_name).name)
    lower = name.lower()
    for ext in LOG_EXTS:
        if lower.endswith(ext):
            name = name[: -len(ext)]
            break
    return name.replace("/", "_").upper()


def discover_year_urls() -> List[Tuple[int, str]]:
    return sorted(
        ((year_from_public_logs_url(url), url) for url in PUBLIC_LOG_URLS),
        reverse=True,
    )


def discover_log_urls(year: int, public_logs_url: str | None = None) -> List[Tuple[str, str]]:
    if public_logs_url is None:
        matches = [url for known_year, url in discover_year_urls() if known_year == year]
        if not matches:
            return []
        public_logs_url = matches[0]

    html_text = fetch_text(public_logs_url)
    links = re.findall(r'href=[\"\']([^\"\']+)[\"\']', html_text, flags=re.IGNORECASE)
    seen_calls: set[str] = set()
    seen_urls: set[str] = set()
    results: List[Tuple[str, str]] = []
    for link in links:
        url = urllib.parse.urljoin(public_logs_url, html.unescape(link))
        parsed = urllib.parse.urlparse(url)
        if not parsed.path.lower().endswith(LOG_EXTS):
            continue
        if url in seen_urls:
            continue
        call = normalize_call_from_filename(parsed.path)
        if not call or call in seen_calls:
            continue
        seen_calls.add(call)
        seen_urls.add(url)
        results.append((call, url))
    return sorted(results)


def fetch_log(url: str) -> bytes:
    return fetch_bytes(url)


def write_log(year: int, call: str, content: bytes) -> Path:
    dest = OUTPUT_ROOT / str(year) / f"{call.replace('/', '_').upper()}.log"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        return dest
    dest.write_bytes(content)
    return dest


def iter_years(from_year: int | None, to_year: int | None) -> Iterable[Tuple[int, str]]:
    years = discover_year_urls()
    if from_year is None and to_year is None:
        return years
    if not years:
        return []
    min_year = min(year for year, _url in years)
    max_year = max(year for year, _url in years)
    start = from_year if from_year is not None else min_year
    end = to_year if to_year is not None else max_year
    return [(year, url) for year, url in years if start <= year <= end]


def main() -> int:
    parser = argparse.ArgumentParser(description="Download Istra Open Contest public logs.")
    parser.add_argument("--from-year", type=int, default=None, help="Start year (default: earliest available)")
    parser.add_argument("--to-year", type=int, default=None, help="End year (default: latest available)")
    parser.add_argument(
        "--task-ledger",
        type=Path,
        default=TASK_LEDGER_PATH,
        help="SQLite task ledger (default: scripts/download_tasks_ledger.sqlite).",
    )
    parser.add_argument(
        "--no-task-ledger",
        action="store_true",
        help="Disable task ledger usage.",
    )
    args = parser.parse_args()

    global TASK_LEDGER
    TASK_LEDGER = None if args.no_task_ledger else TaskLedger(args.task_ledger)

    total_ok = total_skip = total_err = 0
    total_logs = 0
    for year, public_logs_url in iter_years(args.from_year, args.to_year):
        logs = discover_log_urls(year, public_logs_url)
        if not logs:
            print(f"{year}: no logs found")
            continue
        total_logs += len(logs)
        task_key = f"{Path(__file__).stem}:{year}"
        skip, list_hash, item_count = task_should_skip(
            TASK_LEDGER, task_key, [url for _call, url in logs]
        )
        if skip:
            print(f"{year}: skip (task ledger) items={item_count}")
            continue
        errors = 0
        for call, url in logs:
            dest = OUTPUT_ROOT / str(year) / f"{call.replace('/', '_').upper()}.log"
            if dest.exists():
                total_skip += 1
                continue
            try:
                payload = fetch_log(url)
            except Exception as exc:  # pylint: disable=broad-except
                print(f"fail {call} {year}: {exc}")
                total_err += 1
                errors += 1
                continue
            if not payload:
                total_skip += 1
                continue
            write_log(year, call, payload)
            total_ok += 1
        if errors == 0:
            task_mark_complete(TASK_LEDGER, task_key, list_hash, item_count)
    if total_logs == 0:
        print("No logs found.")
        return 0
    print(f"done ok={total_ok} skip={total_skip} err={total_err}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
