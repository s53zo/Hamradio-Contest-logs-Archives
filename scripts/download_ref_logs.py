#!/usr/bin/env python3
"""
Downloader for French HF Championship (Coupe du REF) logs (CW/SSB).

Sources:
  CW:  https://concours.r-e-f.org/logrecus/claimed_hf.php?periode=YYYY&contest=cdfcw
  SSB: https://concours.r-e-f.org/logrecus/claimed_hf.php?periode=YYYY&contest=cdfssb

Each list page links to viewlogHF.php entries that return Cabrillo wrapped
inside a <pre>...</pre> block. The log page requires a Referer to the list page.

Output layout:
  REF/<year>/CW/<CALL>.log
  REF/<year>/SSB/<CALL>.log
"""

from __future__ import annotations

import argparse
import random
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, List

from archive_storage import archive_log_exists, atomic_write_text
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
OUTPUT_ROOT = Path("REF")
TASK_LEDGER: "TaskLedger | None" = None

MODES = {
    "cdfcw": "CW",
    "cdfssb": "SSB",
}


def destination_log_exists(path: Path) -> bool:
    try:
        return archive_log_exists(path)
    except ValueError:
        return path.exists()




def list_url(year: int, contest: str) -> str:
    return f"https://concours.r-e-f.org/logrecus/claimed_hf.php?periode={year}&contest={contest}"


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


def discover_calls(year: int, contest: str) -> List[str]:
    url = list_url(year, contest)
    html_text = fetch_text(url)
    pattern = re.compile(r"viewlogHF\.php\?indicatif=([^&]+)&periode=\d{4}&concours=" + re.escape(contest), re.IGNORECASE)
    calls: List[str] = []
    for match in pattern.finditer(html_text):
        call_raw = urllib.parse.unquote(match.group(1))
        if call_raw:
            calls.append(call_raw.upper())
    # de-dup, preserve order
    seen = set()
    uniq: List[str] = []
    for call in calls:
        if call in seen:
            continue
        seen.add(call)
        uniq.append(call)
    return uniq


def normalize_log(text: str) -> str:
    # Remove <pre> wrappers but keep all spacing and headers intact.
    text = re.sub(r"</?pre[^>]*>", "", text, flags=re.IGNORECASE)
    return text


def fetch_log(year: int, contest: str, call: str) -> str | None:
    url = (
        "https://concours.r-e-f.org/logrecus/viewlogHF.php?"
        + urllib.parse.urlencode({"indicatif": call, "periode": str(year), "concours": contest})
    )
    referer = list_url(year, contest)
    html_text = fetch_text(url, headers={"Referer": referer})
    if "Impossible de traiter la demande" in html_text:
        return None
    return normalize_log(html_text)


def write_log(year: int, mode_label: str, call: str, content: str) -> Path:
    safe_call = call.replace("/", "_")
    dest = OUTPUT_ROOT / str(year) / mode_label / f"{safe_call}.log"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if destination_log_exists(dest):
        return dest
    atomic_write_text(dest, content)
    return dest


def main() -> int:
    parser = argparse.ArgumentParser(description="Download REF (Coupe du REF) HF logs (CW/SSB).")
    parser.add_argument("--from-year", type=int, default=2010, help="Start year (default: 2010)")
    parser.add_argument("--to-year", type=int, default=time.gmtime().tm_year, help="End year (default: current year)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Max concurrent downloads")
    parser.add_argument("--mode", choices=["cw", "ssb", "all"], default="all")
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

    modes: Dict[str, str]
    if args.mode == "all":
        modes = MODES
    elif args.mode == "cw":
        modes = {"cdfcw": "CW"}
    else:
        modes = {"cdfssb": "SSB"}

    total_ok = total_skip = total_err = 0
    for year in range(args.from_year, args.to_year + 1):
        for contest, mode_label in modes.items():
            calls = discover_calls(year, contest)
            if not calls:
                print(f"{year} {mode_label}: no calls found")
                continue
            task_key = f"{Path(__file__).stem}:{year}:{mode_label}"
            skip, list_hash, item_count = task_should_skip(
                TASK_LEDGER, task_key, calls, upper=True
            )
            if skip:
                print(f"{year} {mode_label}: skip (task ledger) items={item_count}")
                continue
            errors = 0
            for call in calls:
                dest = OUTPUT_ROOT / str(year) / mode_label / f"{call.replace('/', '_')}.log"
                key = dest.as_posix()
                if destination_log_exists(dest):
                    total_skip += 1
                    continue
                try:
                    log_text = fetch_log(year, contest, call)
                except Exception as exc:  # pylint: disable=broad-except
                    print(f"fail {call} {year} {mode_label}: {exc}")
                    total_err += 1
                    errors += 1
                    continue
                if not log_text:
                    total_skip += 1
                    continue
                final_dest = write_log(year, mode_label, call, log_text)
                print(f"ok   {final_dest}")
                total_ok += 1
            if errors == 0:
                task_mark_complete(TASK_LEDGER, task_key, list_hash, item_count)
    print(f"done: ok {total_ok} skip {total_skip} err {total_err}")
    return 0 if total_err == 0 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        if TASK_LEDGER is not None:
            TASK_LEDGER.close()
