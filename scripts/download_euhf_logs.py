#!/usr/bin/env python3
"""
Downloader for EUHFC open logs reconstructed from UBN reports.

Flow:
1) Read https://euhf.s5cc.eu/official-results/ to find yearly result pages (/offical-results-YYYY/).
2) On each yearly page, extract category slugs from the hidden `ananas` form inputs (year=YYYY&category=...).
3) For each year/category, POST to https://euhf.s5cc.eu/results/ and collect UBN links (../results/YYYY/ubn/CALL.txt).
4) Download each UBN, extract QSO lines that belong to the log owner, and rebuild a Cabrillo file.

Output: EUHFC/<year>/<CALL>.log (calls sanitized: "/" -> "_").
"""

from __future__ import annotations

import argparse
import concurrent.futures
import html
import random
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple

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
DEFAULT_WORKERS = 10
OUTPUT_ROOT = Path("EUHFC")
RESULTS_ROOT = "https://euhf.s5cc.eu"
TASK_LEDGER: "TaskLedger | None" = None


def fetch_text(url: str, data: bytes | None = None, retries: int = 3, delay: float = 1.0) -> str:
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers={"User-Agent": USER_AGENT})
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


def discover_years() -> List[int]:
    html_text = fetch_text(f"{RESULTS_ROOT}/official-results/")
    years: Set[int] = set()
    for match in re.finditer(r"/offical-results-(\d{4})/", html_text):
        years.add(int(match.group(1)))
    # Fallback: probe known pattern across a reasonable span in case links are missing from index.
    current_year = datetime.now(timezone.utc).year
    for year in range(2010, current_year + 1):
        if year in years:
            continue
        try:
            fetch_text(f"{RESULTS_ROOT}/offical-results-{year}/")
            years.add(year)
        except Exception:
            continue
    return sorted(years, reverse=True)


def discover_categories(year: int) -> List[str]:
    html_text = fetch_text(f"{RESULTS_ROOT}/offical-results-{year}/")
    slugs: Set[str] = set()
    for match in re.finditer(r'name="ananas"\s+value="year=\d{4}&amp;category=([a-z0-9]+)"', html_text):
        slugs.add(match.group(1))
    return sorted(slugs)


def discover_ubn_links(year: int, category: str) -> List[Tuple[str, str]]:
    """Return list of (call, ubn_url)."""
    data = urllib.parse.urlencode({"ananas": f"year={year}&category={category}"}).encode()
    html_text = fetch_text(f"{RESULTS_ROOT}/results/", data=data)
    links: List[Tuple[str, str]] = []
    for match in re.finditer(r'href="([^"]*?/results/(\d{4})/ubn/([A-Z0-9/]+)\.txt)"', html_text, flags=re.IGNORECASE):
        url = urllib.parse.urljoin(RESULTS_ROOT + "/", html.unescape(match.group(1)))
        call = match.group(3).upper()
        links.append((call, url))
    return links


def parse_owner(html_text: str, default_call: str) -> str:
    m = re.search(r"Callsign used:\s*([A-Z0-9/]+)", html_text, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return default_call.upper()


def parse_category(html_text: str) -> str:
    m = re.search(r"CATEGORY:\s*(.+)", html_text)
    if m:
        return m.group(1).strip()
    return ""


def extract_qsos(html_text: str, owner: str) -> List[Tuple[int, str, str, str, str, str, str, str]]:
    """
    Return list of (freq, mode, date, time, mycall, s_rst, s_exch, their, r_rst, r_exch)
    but we will output later without mycall.
    """
    qsos: List[Tuple[int, str, str, str, str, str, str, str]] = []
    for line in html_text.splitlines():
        line = line.strip()
        if not line.startswith("QSO:"):
            continue
        parts = line.split()
        if len(parts) < 10:
            continue
        _, freq, mode, date, time_str, mycall, s_rst, s_exch, their_call, r_rst, r_exch = parts[:11]
        if mycall.upper() != owner.upper():
            continue
        try:
            qsos.append(
                (
                    int(freq),
                    mode.upper(),
                    date,
                    time_str,
                    mycall.upper(),
                    s_rst,
                    s_exch,
                    their_call.upper(),
                    r_rst,
                    r_exch,
                )
            )
        except ValueError:
            continue
    return qsos


def build_cabrillo(call: str, category: str, qsos: Sequence[Tuple[int, str, str, str, str, str, str, str, str]]) -> str:
    lines = [
        "START-OF-LOG: 3.0",
        "CREATED-BY: euhfc-ubn-downloader",
        "CONTEST: EUHFC",
        f"CALLSIGN: {call}",
        f"CATEGORY: {category}",
    ]
    for freq, mode, date, time_str, mycall, s_rst, s_exch, their, r_rst, r_exch in qsos:
        lines.append(
            f"QSO: {freq:>5} {mode:<3} {date} {time_str:>4} {mycall:<13} {s_rst:<3} {s_exch:<6} {their:<13} {r_rst:<3} {r_exch:<6}"
        )
    lines.append("END-OF-LOG:")
    return "\n".join(lines) + "\n"


def write_log(year: int, call: str, content: str) -> Path:
    safe_call = call.replace("/", "_")
    dest = OUTPUT_ROOT / str(year) / f"{safe_call}.log"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        return dest
    dest.write_text(content, encoding="utf-8")
    return dest


def main() -> int:
    parser = argparse.ArgumentParser(description="Download EUHFC Cabrillo logs reconstructed from UBN.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Max concurrent downloads.")
    parser.add_argument(
        "--years",
        type=str,
        default="all",
        help="Comma years or 'all'.",
    )
    parser.add_argument(
        "--max-per-year",
        type=int,
        default=None,
        help="Optional cap on logs per year (for testing).",
    )
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

    years = discover_years()
    if args.years.lower() != "all":
        wanted = {int(y) for y in args.years.split(",") if y.strip().isdigit()}
        years = [y for y in years if y in wanted]
    if not years:
        print("No years found.")
        return 1

    def worker(year: int, call: str, url: str) -> dict[str, int]:
        safe_call = call.replace("/", "_")
        dest = OUTPUT_ROOT / str(year) / f"{safe_call}.log"
        if dest.exists():
            print(f"skip (exists): {dest}")
            return {"skip": 1}
        try:
            text = fetch_text(url)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Failed to fetch UBN for {call} {year} ({url}): {exc}", file=sys.stderr)
            return {"error": 1}
        owner = parse_owner(text, call)
        category = parse_category(text)
        qsos = extract_qsos(text, owner)
        if not qsos:
            print(f"No QSOs for {owner} {year}", file=sys.stderr)
            return {"skip": 1}
        cab = build_cabrillo(owner, category, qsos)
        dest = write_log(year, owner, cab)
        print(f"ok   {dest}")
        return {"ok": 1}

    total_logs = 0
    for year in years:
        categories = discover_categories(year)
        if not categories:
            print(f"{year}: no categories found; skipping (likely no published results).")
            continue
        year_links: List[Tuple[int, str, str]] = []
        for cat in categories:
            links = discover_ubn_links(year, cat)
            if args.max_per_year:
                links = links[: args.max_per_year]
            for call, url in links:
                year_links.append((year, call, url))
        if not year_links:
            print(f"{year}: no UBN links found (older years often have none published).")
            continue
        print(f"{year}: queued {len(year_links)} logs")
        total_logs += len(year_links)
        task_key = f"{Path(__file__).stem}:{year}"
        skip, list_hash, item_count = task_should_skip(
            TASK_LEDGER, task_key, [url for _y, _c, url in year_links]
        )
        if skip:
            print(f"{year}: skip (task ledger) items={item_count}")
            continue
        counts = {"ok": 0, "skip": 0, "error": 0}
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [executor.submit(worker, year, call, url) for year, call, url in year_links]
            for fut in concurrent.futures.as_completed(futures):
                result = fut.result()
                for key, value in result.items():
                    counts[key] = counts.get(key, 0) + value
        if counts.get("error", 0) == 0:
            task_mark_complete(TASK_LEDGER, task_key, list_hash, item_count)

    if total_logs == 0:
        print("No UBN links found.")
        return 1

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
