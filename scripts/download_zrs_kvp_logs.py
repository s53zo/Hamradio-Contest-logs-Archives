#!/usr/bin/env python3
"""
Downloader/parser for ZRS KVP public logs on vhfmanager.net.

Workflow:
1) Read http://kvp.hamradio.si/rezultati.html to find yearly pomlad/jesen pages.
2) Each season page embeds an iframe to results.php?ContestID=... (on vhfmanager.net).
3) From each results page, collect display_log.php links (one per submitted log).
4) For every log page, parse the QSO table and emit a Cabrillo file.

Output layout:
  ZRS_KVP/<year>/<season>/<CALL>.log   (Cabrillo content)

Notes:
- Contest is 80 m only; older public logs omit exact frequency, so those rows are
  mapped to 3500 kHz for CW and 3700 kHz for SSB. Newer rows publish exact kHz.
- Exchanges ("Let.") are numeric license-year abbreviations; asterisks are stripped.
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
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

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
BASE_RESULTS = "http://kvp.hamradio.si/rezultati.html"
OUTPUT_ROOT = Path("ZRS_KVP")
TASK_LEDGER: "TaskLedger | None" = None
Qso = Tuple[int, str, str, str, str, str, str, str, str]


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


def clean_html_cell(text: str) -> str:
    """Strip tags/whitespace and unescape."""
    no_tags = re.sub(r"<[^>]+>", "", text)
    unescaped = html.unescape(no_tags)
    return " ".join(unescaped.split())


def to_year(two_digit: str, default_year: int) -> int:
    """Convert 2-digit year to 4-digit using 1900/2000 heuristic."""
    try:
        yy = int(two_digit)
    except ValueError:
        return default_year
    return 1900 + yy if yy >= 90 else 2000 + yy


def parse_date(date_text: str, fallback_year: int) -> str:
    """
    Convert dates like '16.11.25' into ISO '2025-11-16'.
    If parsing fails, fall back to the season year.
    """
    parts = re.split(r"[.\-/]", date_text.strip())
    if len(parts) == 3:
        dd, mm, yy = parts
        try:
            year = to_year(yy, fallback_year)
            return f"{int(year):04d}-{int(mm):02d}-{int(dd):02d}"
        except ValueError:
            pass
    return f"{fallback_year:04d}"


def parse_time(time_text: str) -> str:
    """Return HHMM from '08:03'."""
    digits = re.sub(r"\D", "", time_text)
    return digits.zfill(4)[:4]


def freq_for_mode(mode: str) -> int:
    """Map CW/SSB to representative 80 m frequencies."""
    mode = mode.upper()
    if mode.startswith("CW"):
        return 3500
    return 3700


def cabrillo_mode(mode: str) -> str:
    """Normalize VHFManager mode labels to Cabrillo HF mode labels."""
    mode = mode.upper()
    if mode.startswith("CW"):
        return "CW"
    if mode in {"SSB", "PH", "PHONE", "FM", "AM"}:
        return "PH"
    return mode[:2] or "PH"


def parse_frequency(freq_text: str, mode: str) -> int:
    """Parse exact frequency from VHFManager text, falling back for legacy rows."""
    cleaned = clean_html_cell(freq_text).lower().replace(",", ".")
    match = re.search(r"(\d+(?:\.\d+)?)\s*(mhz|khz)?", cleaned)
    if not match:
        return freq_for_mode(mode)
    value = float(match.group(1))
    unit = match.group(2) or "khz"
    if unit == "mhz":
        value *= 1000
    return int(round(value))


def is_date_cell(text: str) -> bool:
    return re.fullmatch(r"\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4}", clean_html_cell(text)) is not None


def is_frequency_cell(text: str) -> bool:
    cleaned = clean_html_cell(text).lower()
    return "hz" in cleaned or (re.fullmatch(r"\d+(?:[.,]\d+)?", cleaned) is not None)


def sanitize_exchange(text: str) -> str:
    """Keep digits only (strip '*' or other marks)."""
    digits = re.sub(r"\D", "", text)
    return digits or "00"


def sanitize_call(call: str) -> str:
    return call.upper().replace(" ", "").replace("/", "-")


@dataclass
class Season:
    year: int
    season: str  # 'pomlad' or 'jesen'
    contest_id: str
    results_url: str


def discover_seasons(limit_years: int | None) -> List[Season]:
    """
    Scrape the results index page for pomlad/jesen entries and resolve their ContestIDs.
    """
    index_html = fetch_text(BASE_RESULTS)
    entries: Dict[Tuple[int, str], Season] = {}
    link_re = re.compile(
        r'href="(?P<url>https?://kvp\.hamradio\.si/(?P<year>\d{4})/'
        r'(?P<file>[^"]*?(pomlad|jesen)\.html))"',
        flags=re.IGNORECASE,
    )
    for match in link_re.finditer(index_html):
        year = int(match.group("year"))
        season = "pomlad" if "pomlad" in match.group("file").lower() else "jesen"
        season_url = match.group("url")
        if ("skupno" in season_url.lower()) or ((year, season) in entries):
            continue
        try:
            season_html = fetch_text(season_url)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Failed to fetch season page {season_url}: {exc}", file=sys.stderr)
            continue
        iframe_match = re.search(
            r'iframe[^>]+src="([^"]*results\.php[^"]*ContestID=(\d+)[^"]*)"',
            season_html,
            flags=re.IGNORECASE,
        )
        if not iframe_match:
            continue
        iframe_src = html.unescape(iframe_match.group(1))
        contest_id = iframe_match.group(2)
        results_url = urllib.parse.urljoin(season_url, iframe_src)
        entries[(year, season)] = Season(year, season, contest_id, results_url)

    seasons = sorted(entries.values(), key=lambda s: (s.year, s.season), reverse=True)
    if limit_years:
        years_seen: List[int] = []
        limited: List[Season] = []
        for season in seasons:
            if season.year not in years_seen:
                years_seen.append(season.year)
            if len(years_seen) > limit_years:
                continue
            limited.append(season)
        seasons = limited
    return seasons


def discover_logs(season: Season) -> List[str]:
    """Extract display_log.php links for a given season results page."""
    html_text = fetch_text(season.results_url)
    href_re = re.compile(
        r'href="(?P<href>display_log\.php\?[^"]*ContestID=\d+[^"]*logID=\d+[^"]*)"',
        flags=re.IGNORECASE,
    )
    links = []
    for match in href_re.finditer(html_text):
        href = html.unescape(match.group("href"))
        abs_url = urllib.parse.urljoin(season.results_url, href)
        links.append(abs_url)
    # remove duplicates while preserving order
    seen = set()
    deduped = []
    for url in links:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def extract_log_summary(html_text: str) -> Tuple[str | None, str | None]:
    """Return (call, category) from the summary definition list."""
    summary_match = re.search(r'<dl class="log_summary">(.*?)</dl>', html_text, flags=re.DOTALL | re.IGNORECASE)
    if not summary_match:
        return None, None
    block = summary_match.group(1)
    call_match = re.search(r"<dt>\s*Znak:\s*</dt>\s*<dd>(.*?)</dd>", block, flags=re.IGNORECASE | re.DOTALL)
    cat_match = re.search(r"<dt>\s*Kategorija:\s*</dt>\s*<dd>(.*?)</dd>", block, flags=re.IGNORECASE | re.DOTALL)
    call = clean_html_cell(call_match.group(1)) if call_match else None
    category = clean_html_cell(cat_match.group(1)) if cat_match else None
    return call or None, category or None


def parse_qsos(html_text: str, season_year: int) -> List[Qso]:
    """
    Parse QSO rows and return tuples:
    (freq, mode, date, time, their_call, rst_sent, exch_sent, rst_recv, exch_recv)
    """
    table_match = re.search(r'<table[^>]*class="display_log[^"]*"[^>]*>(.*?)</table>', html_text, flags=re.DOTALL | re.IGNORECASE)
    if not table_match:
        return []
    table_html = table_match.group(1)
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, flags=re.DOTALL | re.IGNORECASE)
    qsos = []
    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.DOTALL | re.IGNORECASE)
        if len(cells) < 8:
            continue  # skip headers or category rows

        has_frequency = len(cells) >= 9 and is_frequency_cell(cells[0]) and is_date_cell(cells[1])
        if has_frequency:
            (
                freq_cell,
                date_cell,
                time_cell,
                call_cell,
                mode_cell,
                rst_s_cell,
                exch_s_cell,
                rst_r_cell,
                exch_r_cell,
            ) = cells[:9]
        else:
            freq_cell = ""
            date_cell, time_cell, call_cell, mode_cell, rst_s_cell, exch_s_cell, rst_r_cell, exch_r_cell = cells[:8]

        their_call = clean_html_cell(call_cell)
        if not their_call:
            continue
        mode = cabrillo_mode(clean_html_cell(mode_cell))
        freq = parse_frequency(freq_cell, mode) if freq_cell else freq_for_mode(mode)
        date = parse_date(clean_html_cell(date_cell), season_year)
        time_val = parse_time(clean_html_cell(time_cell))
        rst_s = clean_html_cell(rst_s_cell) or "59"
        rst_r = clean_html_cell(rst_r_cell) or "59"
        exch_s = sanitize_exchange(clean_html_cell(exch_s_cell))
        exch_r = sanitize_exchange(clean_html_cell(exch_r_cell))
        qsos.append((freq, mode, date, time_val, their_call.upper(), rst_s, exch_s, rst_r, exch_r))
    return qsos


def build_cabrillo(call: str, category: str | None, qsos: Sequence[Qso], season: Season) -> str:
    cat_info = categorize(category)
    lines = [
        "START-OF-LOG: 3.0",
        "CREATED-BY: zrs-kvp-downloader",
        "CONTEST: ZRS-KVP",
        f"CALLSIGN: {call}",
        f"LOCATION: S5",
        f"CATEGORY: {category or ''}",
        f"CATEGORY-OPERATOR: {cat_info['operator']}",
        f"CATEGORY-BAND: {cat_info['band']}",
        f"CATEGORY-MODE: {cat_info['mode']}",
        f"CATEGORY-POWER: {cat_info['power']}",
        f"CATEGORY-ASSISTED: {cat_info['assisted']}",
        f"CATEGORY-TRANSMITTER: {cat_info['transmitter']}",
        f"CATEGORY-STATION: {cat_info['station']}",
        f"CATEGORY-OVERLAY: ",
        f"OPERATORS: {call}",
        "CLAIMED-SCORE: ",
        "CLUB: ",
        "NAME: ",
    ]
    for freq, mode, date, time_val, their_call, rst_s, exch_s, rst_r, exch_r in qsos:
        lines.append(
            f"QSO: {freq:>5} {mode:<2} {date} {time_val:>4} "
            f"{call:<13} {rst_s:<3} {exch_s:<6} {their_call:<13} {rst_r:<3} {exch_r:<6}"
        )
    lines.append("END-OF-LOG:")
    return "\n".join(lines) + "\n"


def download_log(url: str, season: Season, max_errors: int = 1) -> Tuple[str, str] | None:
    try:
        html_text = fetch_text(url)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Failed to fetch {season.year} {season.season} log: {exc}", file=sys.stderr)
        return None
    call, category = extract_log_summary(html_text)
    if not call:
        print(f"Missing callsign in {season.year} {season.season} log", file=sys.stderr)
        return None
    qsos = parse_qsos(html_text, season.year)
    if not qsos:
        print(f"No QSOs parsed for {call} ({season.year} {season.season})", file=sys.stderr)
    cbr = build_cabrillo(sanitize_call(call), category, qsos, season)
    return sanitize_call(call), cbr


def categorize(category: str | None) -> Dict[str, str]:
    """Derive standard Cabrillo category fields from the KVP category string."""
    base = (category or "").upper()
    power = "HIGH" if "VELIKA" in base else "LOW" if "MALA" in base else "QRP" if "QRP" in base else ""
    if "CW/SSB" in base or "MIXED" in base:
        mode = "MIXED"
    elif "CW" in base:
        mode = "CW"
    elif "SSB" in base or "PH" in base:
        mode = "SSB"
    else:
        mode = ""
    return {
        "operator": "SINGLE-OP",
        "band": "80M",
        "mode": mode,
        "power": power,
        "assisted": "NON-ASSISTED",
        "transmitter": "ONE",
        "station": "FIXED",
    }


def write_log(dest_root: Path, season: Season, call: str, content: str) -> Path:
    dest = dest_root / str(season.year) / season.season / f"{call}.log"
    dest.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(dest, content)
    return dest


def main() -> int:
    parser = argparse.ArgumentParser(description="Download/parse ZRS KVP public logs into Cabrillo.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Max concurrent downloads (default: 10).")
    parser.add_argument(
        "--last",
        type=str,
        default="1",
        help="How many recent years to fetch (number or 'all'). Default: 1.",
    )
    parser.add_argument(
        "--max-per-season",
        type=int,
        default=None,
        help="Optional cap on number of logs per season (for testing).",
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

    last_val: int | None
    if args.last.lower() in {"all", "a"}:
        last_val = None
    else:
        try:
            last_val = int(args.last)
        except ValueError:
            print("Invalid --last value.", file=sys.stderr)
            return 1

    seasons = discover_seasons(last_val)
    if not seasons:
        print("No seasons discovered.")
        return 1

    def worker(season: Season, url: str) -> dict[str, int]:
        result = download_log(url, season)
        if not result:
            return {"error": 1}
        call, cbr = result
        dest = OUTPUT_ROOT / str(season.year) / season.season / f"{call}.log"
        if archive_log_exists(dest):
            print(f"skip (exists): {dest}")
            return {"skip": 1}
        dest = write_log(OUTPUT_ROOT, season, call, cbr)
        print(f"ok   {dest}")
        return {"ok": 1}

    total_logs = 0
    for season in seasons:
        log_links = discover_logs(season)
        if args.max_per_season:
            log_links = log_links[: args.max_per_season]
        print(f"{season.year} {season.season}: {len(log_links)} logs")
        if not log_links:
            continue
        total_logs += len(log_links)
        task_key = f"{Path(__file__).stem}:{season.year}:{season.season}"
        skip, list_hash, item_count = task_should_skip(TASK_LEDGER, task_key, log_links)
        if skip:
            print(f"skip (task ledger): {season.year} {season.season} items={item_count}")
            continue
        counts = {"ok": 0, "skip": 0, "error": 0}
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [executor.submit(worker, season, url) for url in log_links]
            for fut in concurrent.futures.as_completed(futures):
                result = fut.result()
                for key, value in result.items():
                    counts[key] = counts.get(key, 0) + value
        if counts.get("error", 0) == 0:
            task_mark_complete(TASK_LEDGER, task_key, list_hash, item_count)

    if total_logs == 0:
        print("No logs found to download.")
        return 1

    print("Done.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        if TASK_LEDGER is not None:
            TASK_LEDGER.close()
