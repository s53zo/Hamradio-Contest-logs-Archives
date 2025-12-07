#!/usr/bin/env python3
"""
Public contest logs downloader with interactive menu.

Supports:
  1) CQWW (PH/CW)
  2) CQWPX (PH/CW)
  3) CQWW RTTY
  4) CQ 160 (PH/CW)
  5) CQWPX RTTY
  6) ARRL contests (all contests from publiclogs.php)

Directory layout roots:
  CQWW/, CQWPX/, CQWWRTTY/, CQ160/, CQWPXRTTY/, ARRL/<contest_slug>/

Usage: run the script, pick contests (or all), then choose how many years (number or 'all').
"""

from __future__ import annotations

import argparse
import concurrent.futures
import html
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Tuple


USER_AGENT = "Mozilla/5.0 (compatible; public-logs-downloader/1.0)"
REQUEST_TIMEOUT = 30
DEFAULT_WORKERS = 20

PRINT_LOCK = threading.Lock()


def fetch_text(url: str, retries: int = 3, delay: float = 1.0) -> str:
    """Fetch a URL and return decoded text with simple retries and UA."""
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


def download_file(dest_path: Path, url: str, retries: int = 3, delay: float = 1.0) -> None:
    """Download a URL to dest_path with retries; skip if exists."""
    if dest_path.exists():
        with PRINT_LOCK:
            print(f"skip (exists): {dest_path}")
        return
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp, open(dest_path, "wb") as fh:
                fh.write(resp.read())
            with PRINT_LOCK:
                print(f"ok   {dest_path}")
            return
        except Exception as exc:  # pylint: disable=broad-except
            last_exc = exc
            if attempt + 1 < retries:
                time.sleep(delay * (2 ** attempt))
            else:
                with PRINT_LOCK:
                    print(f"fail {url}: {exc}")
    if last_exc:
        raise last_exc


class AdaptiveLimiter:
    """
    Adaptive concurrency limiter that adjusts available permits based on error rate.

    Decreasing concurrency is applied via a "debt" that withholds releases until
    the permit count matches the new limit.
    """

    def __init__(
        self,
        initial: int,
        min_limit: int,
        max_limit: int,
        window: int = 50,
        up_threshold: float = 0.01,
        down_threshold: float = 0.05,
    ) -> None:
        self._sema = threading.Semaphore(initial)
        self._limit = initial
        self._min = min_limit
        self._max = max_limit
        self._window = window
        self._up_threshold = up_threshold
        self._down_threshold = down_threshold
        self._succ = 0
        self._fail = 0
        self._debt = 0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        self._sema.acquire()

    def release(self, success: bool) -> None:
        with self._lock:
            if success:
                self._succ += 1
            else:
                self._fail += 1
            total = self._succ + self._fail
            # Apply debt if we reduced limit while all permits were busy.
            if self._debt > 0:
                self._debt -= 1
                # don't release a permit; we're shrinking capacity
                return
            self._sema.release()

            if total >= self._window:
                fail_rate = self._fail / total
                adjusted = False
                if fail_rate > self._down_threshold and self._limit > self._min:
                    self._limit -= 1
                    self._debt += 1
                    adjusted = True
                elif fail_rate < self._up_threshold and self._limit < self._max:
                    self._limit += 1
                    self._sema.release()
                    adjusted = True

                if adjusted:
                    with PRINT_LOCK:
                        print(
                            f"adaptive: limit {self._limit} (fail_rate={fail_rate:.3f}, window={total})"
                        )
                self._succ = 0
                self._fail = 0


# ----- CQWW -----
def tasks_cqww(last: int | None) -> List[Tuple[Path, str]]:
    base_url = "https://cqww.com/publiclogs/"
    base_dir = Path("CQWW")
    html_text = fetch_text(base_url)
    pages = []
    for match in re.finditer(r"href=['\"](?P<path>(?P<year>\d{4})(?P<mode>ph|cw)/)['\"]", html_text, flags=re.IGNORECASE):
        year = match.group("year")
        mode = match.group("mode").lower()
        path = match.group("path")
        pages.append((year, mode, urllib.parse.urljoin(base_url, path)))
    pages.sort(key=lambda itm: int(itm[0]), reverse=True)
    if last:
        pages = pages[: last * 2]
    tasks: List[Tuple[Path, str]] = []
    for year, mode, url in pages:
        html_page = fetch_text(url)
        for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE):
            log_url = urllib.parse.urljoin(url, href)
            filename = Path(urllib.parse.urlparse(log_url).path).name
            dest = base_dir / mode / year / filename
            tasks.append((dest, log_url))
    return tasks


# ----- CQWPX -----
def tasks_cqwpx(last: int | None) -> List[Tuple[Path, str]]:
    base_url = "https://cqwpx.com/publiclogs/"
    base_dir = Path("CQWPX")
    html_text = fetch_text(base_url)
    pages = []
    for match in re.finditer(r"href=['\"](?P<path>(?P<year>\d{4})(?P<mode>ph|cw)/)['\"]", html_text, flags=re.IGNORECASE):
        year = match.group("year")
        mode = match.group("mode").lower()
        path = match.group("path")
        pages.append((year, mode, urllib.parse.urljoin(base_url, path)))
    pages.sort(key=lambda itm: int(itm[0]), reverse=True)
    if last:
        pages = pages[: last * 2]
    tasks: List[Tuple[Path, str]] = []
    for year, mode, url in pages:
        html_page = fetch_text(url)
        for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE):
            log_url = urllib.parse.urljoin(url, href)
            filename = Path(urllib.parse.urlparse(log_url).path).name
            dest = base_dir / mode / year / filename
            tasks.append((dest, log_url))
    return tasks


# ----- CQWW RTTY -----
def tasks_cqwwrtty(last: int | None) -> List[Tuple[Path, str]]:
    base_url = "https://cqwwrtty.com/publiclogs/"
    base_dir = Path("CQWWRTTY")
    html_text = fetch_text(base_url)
    years = []
    for match in re.finditer(r"href=['\"](?P<year>(19|20)\d{2})/['\"]", html_text):
        year = match.group("year")
        years.append((year, urllib.parse.urljoin(base_url, f"{year}/")))
    years.sort(key=lambda itm: int(itm[0]), reverse=True)
    if last:
        years = years[: last]
    tasks: List[Tuple[Path, str]] = []
    for year, url in years:
        html_page = fetch_text(url)
        for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE):
            log_url = urllib.parse.urljoin(url, href)
            filename = Path(urllib.parse.urlparse(log_url).path).name
            dest = base_dir / year / filename
            tasks.append((dest, log_url))
    return tasks


# ----- CQ 160 -----
def tasks_cq160(last: int | None) -> List[Tuple[Path, str]]:
    base_url = "https://cq160.com/publiclogs/"
    base_dir = Path("CQ160")
    html_text = fetch_text(base_url)
    pages = []
    for match in re.finditer(r"href=['\"](?P<path>(?P<year>\d{4})(?P<mode>ph|cw)/)['\"]", html_text, flags=re.IGNORECASE):
        year = match.group("year")
        mode = match.group("mode").lower()
        path = match.group("path")
        pages.append((year, mode, urllib.parse.urljoin(base_url, path)))
    pages.sort(key=lambda itm: int(itm[0]), reverse=True)
    if last:
        pages = pages[: last * 2]
    tasks: List[Tuple[Path, str]] = []
    for year, mode, url in pages:
        html_page = fetch_text(url)
        for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE):
            log_url = urllib.parse.urljoin(url, href)
            filename = Path(urllib.parse.urlparse(log_url).path).name
            dest = base_dir / mode / year / filename
            tasks.append((dest, log_url))
    return tasks


# ----- CQWPX RTTY -----
def tasks_cqwpxrtty(last: int | None) -> List[Tuple[Path, str]]:
    base_url = "https://cqwpxrtty.com/publiclogs/"
    base_dir = Path("CQWPXRTTY")
    html_text = fetch_text(base_url)
    years = []
    for match in re.finditer(r"href=['\"](?P<year>(19|20)\d{2})/['\"]", html_text):
        year = match.group("year")
        years.append((year, urllib.parse.urljoin(base_url, f"{year}/")))
    years.sort(key=lambda itm: int(itm[0]), reverse=True)
    if last:
        years = years[: last]
    tasks: List[Tuple[Path, str]] = []
    for year, url in years:
        html_page = fetch_text(url)
        for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE):
            log_url = urllib.parse.urljoin(url, href)
            filename = Path(urllib.parse.urlparse(log_url).path).name
            dest = base_dir / year / filename
            tasks.append((dest, log_url))
    return tasks


# ----- ARRL -----
def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def arrl_discover_contests() -> List[Tuple[str, str]]:
    html_text = fetch_text("https://contests.arrl.org/publiclogs.php")
    contests: List[Tuple[str, str]] = []
    for match in re.finditer(r'<option value=([0-9]+)>([^<]+)</option>', html_text, flags=re.IGNORECASE):
        eid = match.group(1)
        name = html.unescape(match.group(2)).strip()
        if eid == "0":
            continue
        contests.append((eid, name))
    return contests


def arrl_discover_years(eid: str) -> List[Tuple[str, str]]:
    html_text = fetch_text(f"https://contests.arrl.org/publiclogs.php?eid={eid}")
    years: List[Tuple[str, str]] = []
    pattern = re.compile(
        rf'href="publiclogs\.php\?eid={re.escape(eid)}&iid=(\d+)">((?:19|20)\d{{2}})<',
        flags=re.IGNORECASE,
    )
    for iid, year in pattern.findall(html_text):
        years.append((year, iid))
    years.sort(key=lambda tup: tup[0], reverse=True)
    return years


def arrl_discover_logs(eid: str, iid: str) -> Iterable[Tuple[str, str]]:
    html_text = fetch_text(f"https://contests.arrl.org/publiclogs.php?eid={eid}&iid={iid}")
    for match in re.finditer(r'href="showpubliclog\.php\?q=([^"]+)".*?>([^<]+)</a>', html_text):
        token = match.group(1)
        call = html.unescape(match.group(2)).strip().upper()
        log_url = f"https://contests.arrl.org/showpubliclog.php?q={token}"
        yield call, log_url


def tasks_arrl(last: int | None) -> List[Tuple[Path, str]]:
    contests = arrl_discover_contests()
    tasks: List[Tuple[Path, str]] = []
    for eid, name in contests:
        try:
            years = arrl_discover_years(eid)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Failed to fetch years for {name} ({eid}): {exc}", file=sys.stderr)
            continue
        if last:
            years = years[: last]
        if not years:
            print(f"No years found for {name} ({eid}).")
            continue
        contest_slug = slugify(name)
        for year, iid in years:
            try:
                for call, log_url in arrl_discover_logs(eid, iid):
                    safe_call = call.replace("/", "-")
                    dest = Path("ARRL") / contest_slug / year / f"{safe_call}.log"
                    tasks.append((dest, log_url))
            except Exception as exc:  # pylint: disable=broad-except
                print(f"Failed to fetch logs for {name} {year}: {exc}", file=sys.stderr)
                continue
    return tasks


# ----- Menu / main -----
ProviderFn = Callable[[int | None], List[Tuple[Path, str]]]

PROVIDERS: Dict[int, Tuple[str, ProviderFn]] = {
    1: ("CQWW (PH/CW)", tasks_cqww),
    2: ("CQWPX (PH/CW)", tasks_cqwpx),
    3: ("CQWW RTTY", tasks_cqwwrtty),
    4: ("CQ 160 (PH/CW)", tasks_cq160),
    5: ("CQWPX RTTY", tasks_cqwpxrtty),
    6: ("ARRL contests (all)", tasks_arrl),
}


def prompt_selection() -> List[int]:
    print("Select contests to download (comma-separated numbers or 'all'):")
    for num, (name, _) in PROVIDERS.items():
        print(f"  {num}) {name}")
    while True:
        choice = input("> ").strip().lower()
        if choice in {"all", "a"}:
            return list(PROVIDERS.keys())
        try:
            parts = [int(p.strip()) for p in choice.split(",") if p.strip()]
            valid = [p for p in parts if p in PROVIDERS]
            if valid:
                return valid
        except ValueError:
            pass
        print("Invalid selection, try again (e.g., 1,3 or all).")


def prompt_last_years() -> int | None:
    while True:
        choice = input("How many recent years? (number or 'all'): ").strip().lower()
        if choice in {"all", "a"}:
            return None
        try:
            val = int(choice)
            if val > 0:
                return val
        except ValueError:
            pass
        print("Please enter a positive integer or 'all'.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Public contest logs downloader with menu.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Thread pool size / max concurrency (default: 20).")
    parser.add_argument(
        "--no-adaptive",
        action="store_true",
        help="Disable adaptive concurrency (adaptive is on by default).",
    )
    parser.add_argument(
        "--min-workers",
        type=int,
        default=4,
        help="Minimum concurrent downloads when adaptive is enabled (default: 4).",
    )
    parser.add_argument("--non-interactive", action="store_true", help="Skip menu; use --contests and --last.")
    parser.add_argument(
        "--contests",
        type=str,
        default="all",
        help="Comma numbers from menu or 'all' (used when --non-interactive).",
    )
    parser.add_argument(
        "--last",
        type=str,
        default="all",
        help="How many recent years (number or 'all') (used when --non-interactive).",
    )
    args = parser.parse_args()

    adaptive_enabled = not args.no_adaptive

    if args.non_interactive:
        if args.contests.lower() in {"all", "a"}:
            selections = list(PROVIDERS.keys())
        else:
            try:
                selections = [int(p.strip()) for p in args.contests.split(",") if p.strip()]
                selections = [s for s in selections if s in PROVIDERS]
            except ValueError:
                print("Invalid --contests value.", file=sys.stderr)
                return 1
        last_val: int | None
        if args.last.lower() in {"all", "a"}:
            last_val = None
        else:
            try:
                last_val = int(args.last)
            except ValueError:
                print("Invalid --last value.", file=sys.stderr)
                return 1
    else:
        selections = prompt_selection()
        last_val = prompt_last_years()

    limiter: AdaptiveLimiter | None = None
    if adaptive_enabled:
        max_limit = max(1, args.workers)
        min_limit = max(1, min(args.min_workers, max_limit))
        limiter = AdaptiveLimiter(
            initial=max_limit,
            min_limit=min_limit,
            max_limit=max_limit,
        )
        print(f"Adaptive concurrency enabled: min={min_limit}, max={max_limit}")

    all_tasks: List[Tuple[Path, str]] = []
    for sel in selections:
        name, fn = PROVIDERS[sel]
        print(f"\n=== {name} ===")
        tasks = fn(last_val)
        print(f"  queued {len(tasks)} downloads")
        all_tasks.extend(tasks)

    if not all_tasks:
        print("No log links found.")
        return 1

    print(f"\nTotal files to download: {len(all_tasks)} using up to {args.workers} workers")

    def wrapped_task(dest: Path, url: str) -> None:
        if limiter:
            limiter.acquire()
        success = False
        try:
            download_file(dest, url)
            success = True
        finally:
            if limiter:
                limiter.release(success)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(wrapped_task, dest, url) for dest, url in all_tasks]
        for future in concurrent.futures.as_completed(futures):
            future.result()

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
