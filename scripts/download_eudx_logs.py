#!/usr/bin/env python3
"""
Downloader for EUDX Contest public logs.

The public logs page provides year filters, paginated result tables, and
tokenized download links:
  https://www.eudx-contest.com/public-logs/

Output layout:
  EUDX_contest/<year>/<CALL>.log
"""

from __future__ import annotations

import argparse
import html
import random
import re
import time
import urllib.parse
import urllib.request
from html.parser import HTMLParser
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
OUTPUT_ROOT = Path("EUDX_contest")
TASK_LEDGER: "TaskLedger | None" = None
PUBLIC_LOGS_ROOT = "https://www.eudx-contest.com/public-logs/"
LOG_EXTS = (".txt", ".log", ".cbr", ".adi", ".gz", ".zip")




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


def public_logs_url(year: int, page: int = 1) -> str:
    params = {"logs_year": str(year)}
    if page > 1:
        params["logs_page"] = str(page)
    return f"{PUBLIC_LOGS_ROOT}?{urllib.parse.urlencode(params)}"


def _legacy_public_logs_url(year: int) -> str:
    return f"https://www.eudx-contest.com/public-logs-{year}/"


class PublicLogsPageParser(HTMLParser):
    """Parse the database-backed EUDX public-log table introduced in 2026."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.years: set[int] = set()
        self.logs: List[Tuple[str, str]] = []
        self.page_numbers: set[int] = {1}
        self._in_year_select = False
        self._in_row = False
        self._in_callsign = False
        self._callsign_parts: List[str] = []
        self._download_url: str | None = None

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str | None]]) -> None:
        attr = {key.lower(): value for key, value in attrs}
        tag = tag.lower()
        if tag == "select" and (
            attr.get("id") == "logs_year" or attr.get("name") == "logs_year"
        ):
            self._in_year_select = True
        elif tag == "option" and self._in_year_select:
            value = attr.get("value") or ""
            if re.fullmatch(r"\d{4}", value):
                self.years.add(int(value))
        elif tag == "tr":
            self._in_row = True
            self._callsign_parts = []
            self._download_url = None
        elif (
            tag == "td"
            and self._in_row
            and (attr.get("data-label") or "").lower() == "callsign"
        ):
            self._in_callsign = True
        elif tag == "a":
            href = attr.get("href") or ""
            classes = (attr.get("class") or "").split()
            if self._in_row and "eudx-download" in classes:
                self._download_url = href
            if href:
                query = urllib.parse.parse_qs(urllib.parse.urlparse(html.unescape(href)).query)
                for value in query.get("logs_page", []):
                    if value.isdigit():
                        self.page_numbers.add(int(value))

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == "select":
            self._in_year_select = False
        elif tag == "td":
            self._in_callsign = False
        elif tag == "tr":
            call = "".join(self._callsign_parts).strip().upper()
            if self._in_row and call and self._download_url:
                self.logs.append(
                    (call, urllib.parse.urljoin(PUBLIC_LOGS_ROOT, html.unescape(self._download_url)))
                )
            self._in_row = False
            self._in_callsign = False

    def handle_data(self, data: str) -> None:
        if self._in_callsign:
            self._callsign_parts.append(data)


def _parse_public_logs_page(html_text: str) -> PublicLogsPageParser:
    parser = PublicLogsPageParser()
    parser.feed(html_text)
    return parser


def discover_years() -> List[int]:
    html_text = fetch_text(PUBLIC_LOGS_ROOT)
    parser = _parse_public_logs_page(html_text)
    years = sorted(parser.years)
    if not years:
        # Compatibility with the year-specific pages used before August 2026.
        years = sorted({int(m.group(1)) for m in re.finditer(r"public-logs-(\d{4})", html_text)})
    return years


def _normalize_filename(path_name: str) -> str:
    name = urllib.parse.unquote(path_name)
    lower = name.lower()
    if lower.endswith(".cbr.gz") or lower.endswith(".log.gz") or lower.endswith(".txt.gz"):
        name = name[: -len(".gz")]
        lower = name.lower()
    for ext in [".txt", ".log", ".cbr", ".adi", ".zip", ".gz"]:
        if lower.endswith(ext):
            name = name[: -len(ext)]
            break
    return name


def discover_log_urls(year: int) -> List[Tuple[str, str]]:
    first_html = fetch_text(public_logs_url(year))
    first_page = _parse_public_logs_page(first_html)

    if first_page.logs:
        page_count = max(first_page.page_numbers)
        page_logs = list(first_page.logs)
        for page in range(2, page_count + 1):
            parsed_page = _parse_public_logs_page(fetch_text(public_logs_url(year, page)))
            page_logs.extend(parsed_page.logs)

        seen_calls: set[str] = set()
        seen_urls: set[str] = set()
        results: List[Tuple[str, str]] = []
        for call, link in page_logs:
            if call in seen_calls or link in seen_urls:
                continue
            seen_calls.add(call)
            seen_urls.add(link)
            results.append((call, link))
        return results

    # Compatibility with the old pages containing direct upload links.
    html_text = fetch_text(_legacy_public_logs_url(year))
    links = re.findall(r'href=\"([^\"]+)\"', html_text)
    seen_calls: set[str] = set()
    seen_urls: set[str] = set()
    results: List[Tuple[str, str]] = []
    for link in links:
        if "wp-content/uploads" not in link:
            continue
        link = urllib.parse.urljoin(PUBLIC_LOGS_ROOT, link)
        parsed = urllib.parse.urlparse(link)
        if not parsed.path:
            continue
        if not parsed.path.lower().endswith(LOG_EXTS):
            continue
        if link in seen_urls:
            continue
        filename = Path(parsed.path).name
        call = _normalize_filename(filename).upper()
        if not call:
            continue
        if call in seen_calls:
            continue
        seen_urls.add(link)
        seen_calls.add(call)
        results.append((call, link))
    return results


def fetch_log(url: str) -> bytes:
    return fetch_bytes(url)


def write_log(year: int, call: str, content: bytes) -> Path:
    safe_call = call.replace("/", "_")
    dest = OUTPUT_ROOT / str(year) / f"{safe_call}.log"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        return dest
    dest.write_bytes(content)
    return dest


def iter_years(from_year: int | None, to_year: int | None) -> Iterable[int]:
    years = discover_years()
    if not years:
        return []
    if from_year is None and to_year is None:
        return years
    min_year = min(years)
    max_year = max(years)
    start = from_year if from_year is not None else min_year
    end = to_year if to_year is not None else max_year
    return [year for year in years if start <= year <= end]


def main() -> int:
    parser = argparse.ArgumentParser(description="Download EUDX Contest public logs.")
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
    for year in iter_years(args.from_year, args.to_year):
        logs = discover_log_urls(year)
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
            dest = OUTPUT_ROOT / str(year) / f"{call.replace('/', '_')}.log"
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
