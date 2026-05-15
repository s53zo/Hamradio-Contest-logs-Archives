#!/usr/bin/env python3
"""
Download ARRL contest public logs.

Directory layout:
    ARRL/<contest_slug>/<year>/<callsign>.log

The script scrapes https://contests.arrl.org/publiclogs.php to discover contests,
then per-contest year pages, and finally each Cabrillo log via showpubliclog.php.
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
from typing import Iterable, List, Tuple

from task_ledger import TASK_LEDGER_PATH, TaskLedger, task_mark_complete, task_should_skip


BASE_URL = "https://contests.arrl.org/publiclogs.php"
LOG_URL = "https://contests.arrl.org/showpubliclog.php"
BASE_DIR = Path("ARRL")
WORKERS = 10
REQUEST_TIMEOUT = 30


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

# Shared lock for clean console output.
PRINT_LOCK = threading.Lock()
TASK_LEDGER: "TaskLedger | None" = None


def fetch_text(url: str, retries: int = 3, delay: float = 1.0) -> str:
    """Fetch a URL and return decoded text with simple retries."""
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
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


def discover_contests() -> List[Tuple[str, str]]:
    """
    Return a list of (eid, name) contests from the dropdown on publiclogs.php.
    """
    html_text = fetch_text(BASE_URL)
    contests = []
    for match in re.finditer(r'<option value=([0-9]+)>([^<]+)</option>', html_text, flags=re.IGNORECASE):
        eid = match.group(1)
        name = html.unescape(match.group(2)).strip()
        if eid == "0":
            continue
        contests.append((eid, name))
    return contests


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def discover_years(eid: str) -> List[Tuple[str, str]]:
    """
    Given a contest eid, return a list of (year, iid) pairs.
    The year pages are linked as publiclogs.php?eid=<eid>&iid=<iid>.
    """
    html_text = fetch_text(f"{BASE_URL}?eid={eid}")
    years: List[Tuple[str, str]] = []
    pattern = re.compile(
        rf'href="publiclogs\.php\?eid={re.escape(eid)}&iid=(\d+)">((?:19|20)\d{{2}})<',
        flags=re.IGNORECASE,
    )
    for iid, year in pattern.findall(html_text):
        years.append((year, iid))
    # Newest first
    years.sort(key=lambda tup: tup[0], reverse=True)
    return years


def discover_logs(eid: str, iid: str) -> Iterable[Tuple[str, str, str]]:
    """
    Yield (callsign, year, log_url) for one contest/year page.
    """
    html_text = fetch_text(f"{BASE_URL}?eid={eid}&iid={iid}")
    # The log links look like <a href="showpubliclog.php?q=TOKEN" ...>CALL</a>
    for match in re.finditer(r'href="showpubliclog\.php\?q=([^"]+)".*?>([^<]+)</a>', html_text):
        token = match.group(1)
        call = html.unescape(match.group(2)).strip().upper()
        log_url = f"{LOG_URL}?q={token}"
        yield call, log_url


def extract_preformatted(text: str) -> str | None:
    match = re.search(r"<pre[^>]*>(.*?)</pre>", text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    body = match.group(1)
    body = re.sub(r"<br\\s*/?>", "\n", body, flags=re.IGNORECASE)
    return html.unescape(body)


def download_log(dest_dir: Path, call: str, log_url: str, retries: int = 3, delay: float = 1.0) -> dict[str, int]:
    """Download a single log file into dest_dir."""
    safe_call = call.replace("/", "-")
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / f"{safe_call}.log"

    if dest_path.exists():
        with PRINT_LOCK:
            print(f"skip (exists): {dest_path}")
        return {"skip": 1}

    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(log_url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp, open(dest_path, "wb") as fh:
                raw = resp.read()
                content = raw
                raw_lower = raw.lower()
                if b"<pre" in raw_lower or resp.headers.get_content_type() == "text/html":
                    charset = resp.headers.get_content_charset() or "utf-8"
                    text = raw.decode(charset, errors="ignore")
                    extracted = extract_preformatted(text)
                    if extracted:
                        if not extracted.endswith("\n"):
                            extracted += "\n"
                        content = extracted.encode("utf-8")
                fh.write(content)
            with PRINT_LOCK:
                print(f"ok   {dest_path}")
            return {"ok": 1}
        except Exception as exc:  # pylint: disable=broad-except
            last_exc = exc
            if attempt + 1 < retries:
                time.sleep(delay * (2 ** attempt))
            else:
                with PRINT_LOCK:
                    print(f"fail {log_url}: {exc}")
                return {"error": 1}
    return {"error": 1}


def select_contests(all_contests: List[Tuple[str, str]], filters: List[str]) -> List[Tuple[str, str]]:
    """
    Filter contests by numeric eid or substring match on name.
    """
    if not filters:
        return all_contests
    selected: List[Tuple[str, str]] = []
    lowered = [f.lower() for f in filters]
    for eid, name in all_contests:
        for flt in lowered:
            if flt == eid or flt in name.lower():
                selected.append((eid, name))
                break
    return selected


def main() -> int:
    parser = argparse.ArgumentParser(description="Download ARRL public logs.")
    parser.add_argument(
        "--contest",
        action="append",
        default=[],
        help="Contest filter: numeric eid or substring of contest name. Repeatable. Default: all.",
    )
    parser.add_argument(
        "--last",
        type=int,
        default=None,
        help="Limit to the most recent N years per contest (default: all years).",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=BASE_DIR,
        help="Destination base directory (default: ARRL).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=WORKERS,
        help="Thread pool size for downloads (default: 20).",
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

    contests = discover_contests()
    contests = select_contests(contests, args.contest)
    if not contests:
        print("No contests matched.", file=sys.stderr)
        return 1

    total_logs = 0
    for eid, name in contests:
        try:
            years = discover_years(eid)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Failed to fetch years for {name} ({eid}): {exc}", file=sys.stderr)
            continue
        if args.last:
            years = years[: args.last]
        if not years:
            print(f"No years found for {name} ({eid}).")
            continue
        contest_slug = slugify(name)
        print(f"{name} ({eid}): {len(years)} year(s)")
        for year, iid in years:
            print(f"  Scanning {year}")
            dest_dir = args.base_dir / contest_slug / year
            try:
                logs = list(discover_logs(eid, iid))
            except Exception as exc:  # pylint: disable=broad-except
                print(f"  Failed to fetch logs for {name} {year}: {exc}", file=sys.stderr)
                continue
            if not logs:
                continue
            total_logs += len(logs)
            task_key = f"{Path(__file__).stem}:{contest_slug}:{year}"
            skip, list_hash, item_count = task_should_skip(
                TASK_LEDGER, task_key, [url for _call, url in logs]
            )
            if skip:
                print(f"  skip (task ledger): {year} items={item_count}")
                continue
            counts = {"ok": 0, "skip": 0, "error": 0}
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [executor.submit(download_log, dest_dir, call, url) for call, url in logs]
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
    sys.exit(main())
