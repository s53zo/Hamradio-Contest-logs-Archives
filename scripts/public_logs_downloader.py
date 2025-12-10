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
  7) ZRS KVP (pomlad/jesen on vhfmanager.net)
  8) EUHFC (reconstructed from UBN reports)
  9) WAE (CW/SSB/RTTY open logs)
 10) VHFManager contests (official/unofficial)

Directory layout roots:
  CQWW/, CQWPX/, CQWWRTTY/, CQ160/, CQWPXRTTY/, ARRL/<contest_slug>/,
  ZRS_KVP/<year>/<season>/, EUHFC/<year>/, WAE/<mode>/<year>/,
  VHF_MANAGER/<contest_slug>_<ContestID>/

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
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Tuple
import subprocess


USER_AGENT = "Mozilla/5.0 (compatible; public-logs-downloader/1.0)"
REQUEST_TIMEOUT = 30
DEFAULT_WORKERS = 20

PRINT_LOCK = threading.Lock()


@dataclass
class DownloadTask:
    dest: Path
    host: str  # hostname (before DNS resolution)
    source: str  # provider label
    action: Callable[[], None]


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


def make_http_task(dest_path: Path, url: str, source: str) -> DownloadTask:
    """Wrap a simple file download into a DownloadTask."""
    host = urllib.parse.urlparse(url).hostname or "unknown"
    return DownloadTask(dest=dest_path, host=host, source=source, action=lambda dest=dest_path, link=url: download_file(dest, link))


def resolve_hosts(hosts: Iterable[str]) -> Dict[str, List[str]]:
    """Resolve hosts with dig for logging; best-effort."""
    resolved: Dict[str, List[str]] = {}
    try:
        subprocess.run(["dig", "+short", "localhost"], capture_output=True, text=True, check=False)
    except FileNotFoundError:
        return resolved

    for host in hosts:
        if not host or host == "unknown":
            resolved[host] = []
            continue
        try:
            res = subprocess.run(["dig", "+short", host], capture_output=True, text=True, check=False)
            ips = [line.strip() for line in res.stdout.splitlines() if line.strip()]
            resolved[host] = ips
            with PRINT_LOCK:
                if ips:
                    print(f"dig {host}: {' '.join(ips)}")
                else:
                    print(f"dig {host}: no answers")
        except Exception as exc:  # pylint: disable=broad-except
            resolved[host] = []
            with PRINT_LOCK:
                print(f"dig {host} failed: {exc}")
    return resolved


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
def tasks_cqww(last: int | None) -> List[DownloadTask]:
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
    tasks: List[DownloadTask] = []
    for year, mode, url in pages:
        html_page = fetch_text(url)
        for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE):
            log_url = urllib.parse.urljoin(url, href)
            filename = Path(urllib.parse.urlparse(log_url).path).name
            dest = base_dir / mode / year / filename
            tasks.append(make_http_task(dest, log_url, source="CQWW"))
    return tasks


# ----- CQWPX -----
def tasks_cqwpx(last: int | None) -> List[DownloadTask]:
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
    tasks: List[DownloadTask] = []
    for year, mode, url in pages:
        html_page = fetch_text(url)
        for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE):
            log_url = urllib.parse.urljoin(url, href)
            filename = Path(urllib.parse.urlparse(log_url).path).name
            dest = base_dir / mode / year / filename
            tasks.append(make_http_task(dest, log_url, source="CQWPX"))
    return tasks


# ----- CQWW RTTY -----
def tasks_cqwwrtty(last: int | None) -> List[DownloadTask]:
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
    tasks: List[DownloadTask] = []
    for year, url in years:
        html_page = fetch_text(url)
        for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE):
            log_url = urllib.parse.urljoin(url, href)
            filename = Path(urllib.parse.urlparse(log_url).path).name
            dest = base_dir / year / filename
            tasks.append(make_http_task(dest, log_url, source="CQWW RTTY"))
    return tasks


# ----- CQ 160 -----
def tasks_cq160(last: int | None) -> List[DownloadTask]:
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
    tasks: List[DownloadTask] = []
    for year, mode, url in pages:
        html_page = fetch_text(url)
        for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE):
            log_url = urllib.parse.urljoin(url, href)
            filename = Path(urllib.parse.urlparse(log_url).path).name
            dest = base_dir / mode / year / filename
            tasks.append(make_http_task(dest, log_url, source="CQ 160"))
    return tasks


# ----- CQWPX RTTY -----
def tasks_cqwpxrtty(last: int | None) -> List[DownloadTask]:
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
    tasks: List[DownloadTask] = []
    for year, url in years:
        html_page = fetch_text(url)
        for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE):
            log_url = urllib.parse.urljoin(url, href)
            filename = Path(urllib.parse.urlparse(log_url).path).name
            dest = base_dir / year / filename
            tasks.append(make_http_task(dest, log_url, source="CQWPX RTTY"))
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


def tasks_arrl(last: int | None) -> List[DownloadTask]:
    contests = arrl_discover_contests()
    tasks: List[DownloadTask] = []
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
                    tasks.append(make_http_task(dest, log_url, source="ARRL"))
            except Exception as exc:  # pylint: disable=broad-except
                print(f"Failed to fetch logs for {name} {year}: {exc}", file=sys.stderr)
                continue
    return tasks


# ----- ZRS KVP -----
def tasks_zrs_kvp(last: int | None) -> List[DownloadTask]:
    import download_zrs_kvp_logs as zrs  # type: ignore

    seasons = zrs.discover_seasons(last)
    tasks: List[DownloadTask] = []
    for season in seasons:
        log_links = zrs.discover_logs(season)
        for url in log_links:
            host = urllib.parse.urlparse(url).hostname or "unknown"
            query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
            log_id = query.get("logID", ["unknown"])[0]
            placeholder = Path(zrs.OUTPUT_ROOT) / str(season.year) / season.season / f"log-{log_id}.log"

            def action(season=season, url=url) -> None:
                result = zrs.download_log(url, season)
                if not result:
                    return
                call, cbr = result
                dest_path = Path(zrs.OUTPUT_ROOT) / str(season.year) / season.season / f"{call}.log"
                if dest_path.exists():
                    with PRINT_LOCK:
                        print(f"skip (exists): {dest_path}")
                    return
                final_dest = zrs.write_log(zrs.OUTPUT_ROOT, season, call, cbr)
                with PRINT_LOCK:
                    print(f"ok   {final_dest}")

            tasks.append(DownloadTask(dest=placeholder, host=host, source="ZRS KVP", action=action))
    return tasks


# ----- EUHFC (UBN reconstructed) -----
def tasks_euhfc(last: int | None) -> List[DownloadTask]:
    import download_euhf_logs as euhf  # type: ignore

    years = euhf.discover_years()
    if last:
        years = years[:last]
    tasks: List[DownloadTask] = []
    for year in years:
        categories = euhf.discover_categories(year)
        if not categories:
            continue
        for cat in categories:
            for call, url in euhf.discover_ubn_links(year, cat):
                host = urllib.parse.urlparse(url).hostname or "unknown"
                safe_call = call.replace("/", "_")
                placeholder = euhf.OUTPUT_ROOT / str(year) / f"{safe_call}.log"

                def action(year=year, call=call, url=url) -> None:
                    dest = euhf.OUTPUT_ROOT / str(year) / f"{call.replace('/', '_')}.log"
                    if dest.exists():
                        with PRINT_LOCK:
                            print(f"skip (exists): {dest}")
                        return
                    try:
                        text = euhf.fetch_text(url)
                    except Exception as exc:  # pylint: disable=broad-except
                        with PRINT_LOCK:
                            print(f"fail {url}: {exc}")
                        return
                    owner = euhf.parse_owner(text, call)
                    category = euhf.parse_category(text)
                    qsos = euhf.extract_qsos(text, owner)
                    if not qsos:
                        with PRINT_LOCK:
                            print(f"skip (no qsos): {owner} {year}")
                        return
                    cab = euhf.build_cabrillo(owner, category, qsos)
                    final_dest = euhf.write_log(year, owner, cab)
                    with PRINT_LOCK:
                        print(f"ok   {final_dest}")

                tasks.append(DownloadTask(dest=placeholder, host=host, source="EUHFC", action=action))
    return tasks


# ----- VHFManager -----
def tasks_vhfmanager(last: int | None) -> List[DownloadTask]:
    import download_vhfmanager_logs as vhf  # type: ignore

    contests = vhf.discover_contests(last)
    tasks: List[DownloadTask] = []
    for contest in contests:
        try:
            contest, links = vhf.discover_logs(contest)
        except Exception as exc:  # pylint: disable=broad-except
            with PRINT_LOCK:
                print(f"Failed to fetch contest {contest.cid}: {exc}")
            continue
        for link in links:
            host = urllib.parse.urlparse(link.url).hostname or "unknown"
            safe_hint = (link.call_hint or f"log_{link.url.split('=')[-1]}").replace("/", "_")
            placeholder = vhf.OUTPUT_ROOT / f"{vhf.slugify(contest.name)}_{contest.cid}" / f"{safe_hint}.log"

            def action(contest=contest, link=link) -> None:
                try:
                    page = vhf.fetch_text(link.url)
                except Exception as exc:  # pylint: disable=broad-except
                    with PRINT_LOCK:
                        print(f"fail {link.url}: {exc}")
                    return
                call, category, locator = vhf.parse_log_header(page)
                if not call:
                    call = link.call_hint or f"log_{hash(link.url) & 0xFFFF}"
                qsos = vhf.parse_qsos(page, call, category, locator)
                if not qsos:
                    with PRINT_LOCK:
                        print(f"skip (no qsos): {call} ({contest.cid})")
                    return
                contest_dir = vhf.derive_contest_dir(contest, qsos)
                band_label = vhf.band_label_from_qsos(qsos)
                cab = vhf.build_cabrillo(contest, call, category, qsos)
                dest = vhf.write_log(contest_dir, band_label, call, cab)
                with PRINT_LOCK:
                    print(f"ok   {dest}")

            tasks.append(DownloadTask(dest=placeholder, host=host, source="VHFManager", action=action))
    return tasks


# ----- WAE -----
def tasks_wae(last: int | None) -> List[DownloadTask]:
    import download_wae_logs as wae  # type: ignore

    tasks: List[DownloadTask] = []
    for mode, base in wae.MODES.items():
        years = wae.discover_years(base)
        if last:
            years = years[:last]
        if not years:
            continue
        latest = years[0]
        for year in years:
            calls = wae.discover_calls_for_year(base, year, latest)
            for call in calls:
                safe_call = call.replace("/", "_")
                placeholder = wae.OUTPUT_ROOT / mode.upper() / str(year) / f"{safe_call}.log"
                host = "dxhf2.darc.de"

                def action(mode=mode, base=base, call=call, year=year) -> None:
                    dest = wae.OUTPUT_ROOT / mode.upper() / str(year) / f"{call.replace('/', '_')}.log"
                    if dest.exists():
                        with PRINT_LOCK:
                            print(f"skip (exists): {dest}")
                        return
                    try:
                        cab = wae.fetch_log(base, call, year)
                    except Exception as exc:  # pylint: disable=broad-except
                        with PRINT_LOCK:
                            print(f"fail {call} {year} ({mode}): {exc}")
                        return
                    if not cab:
                        with PRINT_LOCK:
                            print(f"skip (no cabrillo): {call} {year} ({mode})")
                        return
                    final_dest = wae.write_log(mode, year, call, cab)
                    with PRINT_LOCK:
                        print(f"ok   {final_dest}")

                tasks.append(DownloadTask(dest=placeholder, host=host, source="WAE", action=action))
    return tasks


# ----- Menu / main -----
ProviderFn = Callable[[int | None], List[DownloadTask]]

PROVIDERS: Dict[int, Tuple[str, ProviderFn]] = {
    1: ("CQWW (PH/CW)", tasks_cqww),
    2: ("CQWPX (PH/CW)", tasks_cqwpx),
    3: ("CQWW RTTY", tasks_cqwwrtty),
    4: ("CQ 160 (PH/CW)", tasks_cq160),
    5: ("CQWPX RTTY", tasks_cqwpxrtty),
    6: ("ARRL contests (all)", tasks_arrl),
    7: ("ZRS KVP (pomlad/jesen)", tasks_zrs_kvp),
    8: ("EUHFC (reconstructed from UBN)", tasks_euhfc),
    9: ("WAE (CW/SSB/RTTY)", tasks_wae),
    10: ("VHFManager contests", tasks_vhfmanager),
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
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print extra diagnostics (host/IP buckets, per-host task counts).",
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

    all_tasks: List[DownloadTask] = []
    print("\nStarting provider discovery in parallel...")

    def run_provider(sel: int) -> Tuple[int, List[DownloadTask]]:
        name, fn = PROVIDERS[sel]
        tasks = fn(last_val)
        return sel, tasks

    discovery_results: Dict[int, Tuple[str, int]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(len(selections), args.workers))) as executor:
        futures = {executor.submit(run_provider, sel): sel for sel in selections}
        for fut in concurrent.futures.as_completed(futures):
            sel, tasks = fut.result()
            name, _ = PROVIDERS[sel]
            discovery_results[sel] = (name, len(tasks))
            all_tasks.extend(tasks)

    print("\nDiscovery results (stable order):")
    for sel in sorted(selections):
        name, count = discovery_results.get(sel, (PROVIDERS[sel][0], 0))
        print(f"  {sel}) {name:<40} queued {count:>6} downloads")

    if not all_tasks:
        print("No log links found.")
        return 1

    print(f"\nTotal files to download: {len(all_tasks)} using up to {args.workers} workers per server")

    hostnames = {task.host for task in all_tasks}
    if args.debug:
        print("\nResolving hosts with dig (best-effort)...")
    resolved = resolve_hosts(hostnames)

    server_buckets: Dict[Tuple[str, str], List[DownloadTask]] = {}
    for task in all_tasks:
        ips = resolved.get(task.host, [])
        ip = ips[0] if ips else "unresolved"
        key = (task.host, ip)
        server_buckets.setdefault(key, []).append(task)

    if args.debug:
        print("\nDEBUG: host -> IP mapping:")
        for host in sorted(hostnames):
            ips = resolved.get(host, [])
            print(f"  {host}: {', '.join(ips) if ips else 'unresolved'}")

        print("\nDEBUG: bucket breakdown (host/ip -> count by source):")
        for (host, ip), tasks in server_buckets.items():
            by_source: Dict[str, int] = {}
            for task in tasks:
                by_source[task.source] = by_source.get(task.source, 0) + 1
            source_str = ", ".join(f"{src}:{cnt}" for src, cnt in sorted(by_source.items()))
            print(f"  {host} ({ip}): {len(tasks)} tasks [{source_str}]")

    def build_limiter() -> AdaptiveLimiter | None:
        if not adaptive_enabled:
            return None
        max_limit = max(1, args.workers)
        min_limit = max(1, min(args.min_workers, max_limit))
        print(f"Adaptive concurrency enabled for host: min={min_limit}, max={max_limit}")
        return AdaptiveLimiter(initial=max_limit, min_limit=min_limit, max_limit=max_limit)

    def process_host(host_label: str, tasks: List[DownloadTask]) -> None:
        limiter = build_limiter()

        def wrapped_task(task: DownloadTask) -> None:
            if limiter:
                limiter.acquire()
            success = False
            try:
                task.action()
                success = True
            finally:
                if limiter:
                    limiter.release(success)

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [executor.submit(wrapped_task, task) for task in tasks]
            for future in concurrent.futures.as_completed(futures):
                future.result()

    threads = []
    for (host, ip), tasks in server_buckets.items():
        label = f"{host} ({ip})"
        print(f"\nServer {label}: {len(tasks)} tasks")
        t = threading.Thread(target=process_host, args=(label, tasks))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
