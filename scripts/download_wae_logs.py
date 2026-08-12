#!/usr/bin/env python3
"""
Downloader for WAE (Worked All Europe) open logs (CW/SSB/RTTY) from dxhf2.darc.de.

Sources:
  - Current results:   https://dxhf2.darc.de/~waecwlog/user.cgi?fc=loglist&form=referat&lang=en
  - Archive results:   https://dxhf2.darc.de/~waecwlog/arch_res.cgi (POST year, type)
  - Open logs request: https://dxhf2.darc.de/~waecwlog/user.cgi?fc=req_olog&form=referat&lang=en&call=<CALL>&jahr=<YEAR>&status=show

We pull calls per mode/year from the current results (latest year) and archive results
(years in the <select> dropdown), then download the Cabrillo text for each call/year.

Output layout: WAE/<mode>/<year>/<CALL>.log
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
OUTPUT_ROOT = Path("WAE")
TASK_LEDGER: "TaskLedger | None" = None


MODES = {
    "cw": "waecwlog",
    "ssb": "waessblog",
    "rtty": "waerttylog",
}


def fetch_text(url: str, data: bytes | None = None, retries: int = 3, delay: float = 1.0) -> str:
    """Fetch a URL and return decoded text."""
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


def loglist_contest_year(html_text: str) -> int | None:
    """Extract the contest year without considering framework timestamps."""
    text = " ".join(html.unescape(re.sub(r"<[^>]+>", " ", html_text)).split())
    match = re.search(
        r"Worked All Europe DX Contest.*?\b(20\d{2})\s+"
        r"(?:Final scores|Classified logs)\b",
        text,
        flags=re.IGNORECASE,
    )
    return int(match.group(1)) if match else None


def discover_loglist_year(mode_base: str) -> int | None:
    url = f"https://dxhf2.darc.de/~{mode_base}/user.cgi?fc=loglist&form=referat&lang=en"
    return loglist_contest_year(fetch_text(url))


def discover_years(mode_base: str) -> List[int]:
    """Parse available years from the Open Log form and current loglist page."""
    url = f"https://dxhf2.darc.de/~{mode_base}/user.cgi?fc=req_olog&form=referat&lang=en"
    html_text = fetch_text(url)
    years: Set[int] = set()
    for match in re.finditer(r'<option[^>]*value="(\d{4})"', html_text):
        years.add(int(match.group(1)))
    loglist_year = discover_loglist_year(mode_base)
    if loglist_year:
        # The Open Log form may expose the current calendar year before that
        # mode has published results or logs (for example RTTY 2026 in August).
        years = {year for year in years if year <= loglist_year}
        years.add(loglist_year)
    return sorted(years, reverse=True)


def calls_from_table(html_text: str) -> List[str]:
    """Extract calls from result tables by scanning each cell."""
    calls: List[str] = []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html_text, flags=re.DOTALL | re.IGNORECASE):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.DOTALL | re.IGNORECASE)
        if not cells:
            continue
        for cell in cells:
            call_raw = clean_cell(cell)
            if is_callsign(call_raw):
                calls.append(call_raw)
                break
    return calls


def is_callsign(text: str) -> bool:
    """Heuristic: allow only call-like tokens and require a letter after a digit."""
    if not re.fullmatch(r"[A-Z0-9/]+", text):
        return False
    if not re.search(r"\d", text) or not re.search(r"[A-Z]", text):
        return False
    return bool(re.search(r"\d+[A-Z]", text))


def clean_cell(cell: str) -> str:
    return " ".join(html.unescape(re.sub(r"<[^>]+>", "", cell)).split()).upper()


def discover_calls_for_year(mode_base: str, year: int, latest_year: int) -> List[str]:
    """
    Get call list for a given year.
    Latest year: use loglist (final scores).
    Archive years: use arch_res.cgi with POST year.
    """
    html_blobs: List[str] = []
    if year == latest_year:
        url = f"https://dxhf2.darc.de/~{mode_base}/user.cgi?fc=loglist&form=referat&lang=en"
        html_blobs.append(fetch_text(url))
    # Always hit archive (often more complete / localized)
    data = urllib.parse.urlencode({"form": "referat", "lang": "en", "year": str(year), "type": "EU/NonEU"}).encode()
    html_blobs.append(fetch_text(f"https://dxhf2.darc.de/~{mode_base}/arch_res.cgi", data=data))
    # If English returned nothing meaningful, try German.
    data_de = urllib.parse.urlencode({"form": "referat", "lang": "de", "year": str(year), "type": "EU/NonEU"}).encode()
    html_blobs.append(fetch_text(f"https://dxhf2.darc.de/~{mode_base}/arch_res.cgi", data=data_de))

    calls: List[str] = []
    for blob in html_blobs:
        calls.extend(calls_from_table(blob))
    # remove duplicates, preserve order
    seen: Set[str] = set()
    uniq: List[str] = []
    for call in calls:
        if call in seen:
            continue
        seen.add(call)
        uniq.append(call)
    return uniq


def extract_cabrillo(html_text: str) -> str | None:
    """Extract Cabrillo content from the open log page."""
    match = re.search(r"(START-OF-LOG:.*?END-OF-LOG:)", html_text, flags=re.DOTALL | re.IGNORECASE)
    if not match:
        return None
    cab = match.group(1)
    # Normalize line endings to \n
    return "\n".join(line.rstrip() for line in cab.splitlines()) + "\n"


def fetch_log(mode_base: str, call: str, year: int) -> str | None:
    params = {
        "fc": "req_olog",
        "form": "referat",
        "lang": "en",
        "call": call,
        "jahr": str(year),
        "status": "show",
    }
    url = f"https://dxhf2.darc.de/~{mode_base}/user.cgi?{urllib.parse.urlencode(params)}"
    html_text = fetch_text(url)
    return extract_cabrillo(html_text)


def write_log(mode: str, year: int, call: str, content: str) -> Path:
    safe_call = call.replace("/", "_")
    dest = OUTPUT_ROOT / mode.upper() / str(year) / f"{safe_call}.log"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        return dest
    dest.write_text(content, encoding="utf-8")
    return dest


def main() -> int:
    parser = argparse.ArgumentParser(description="Download WAE open logs (CW/SSB/RTTY).")
    parser.add_argument("--modes", type=str, default="cw,ssb,rtty", help="Comma list of modes to fetch (cw,ssb,rtty).")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Max concurrent downloads (default: 10).")
    parser.add_argument(
        "--max-per-year",
        type=int,
        default=None,
        help="Optional limit of logs per year (for testing).",
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

    selected_modes = [m.strip().lower() for m in args.modes.split(",") if m.strip().lower() in MODES]
    if not selected_modes:
        print("No valid modes selected.", file=sys.stderr)
        return 1

    def worker(mode: str, year: int, base: str, call: str) -> dict[str, int]:
        safe_call = call.replace("/", "_")
        dest = OUTPUT_ROOT / mode.upper() / str(year) / f"{safe_call}.log"
        if dest.exists():
            print(f"skip (exists): {dest}")
            return {"skip": 1}
        cab = fetch_log(base, call, year)
        if not cab:
            print(f"Missing cabrillo for {call} {year} ({mode})", file=sys.stderr)
            return {"skip": 1}
        dest = write_log(mode, year, call, cab)
        print(f"ok   {dest}")
        return {"ok": 1}

    total_tasks = 0
    for mode in selected_modes:
        base = MODES[mode]
        years = discover_years(base)
        if not years:
            print(f"No years found for {mode}.")
            continue
        latest = years[0]
        for year in years:
            calls = discover_calls_for_year(base, year, latest)
            if args.max_per_year:
                calls = calls[: args.max_per_year]
            print(f"{mode.upper()} {year}: {len(calls)} calls")
            if not calls:
                continue
            total_tasks += len(calls)
            task_key = f"{Path(__file__).stem}:{mode}:{year}"
            skip, list_hash, item_count = task_should_skip(
                TASK_LEDGER, task_key, calls, upper=True
            )
            if skip:
                print(f"skip (task ledger): {mode.upper()} {year} items={item_count}")
                continue
            counts = {"ok": 0, "skip": 0, "error": 0}
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [executor.submit(worker, mode, year, base, call) for call in calls]
                for fut in concurrent.futures.as_completed(futures):
                    result = fut.result()
                    for key, value in result.items():
                        counts[key] = counts.get(key, 0) + value
            if counts.get("error", 0) == 0:
                task_mark_complete(TASK_LEDGER, task_key, list_hash, item_count)

    if total_tasks == 0:
        print("No logs to fetch.")
        return 0

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
