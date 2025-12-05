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
- Contest is 80 m only; frequency is mapped to 3500 kHz for CW and 3700 kHz for SSB.
- Exchanges ("Let.") are numeric license-year abbreviations; asterisks are stripped.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import html
import re
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


USER_AGENT = "Mozilla/5.0 (compatible; zrs-kvp-downloader/1.0)"
REQUEST_TIMEOUT = 30
DEFAULT_WORKERS = 10
BASE_RESULTS = "http://kvp.hamradio.si/rezultati.html"
OUTPUT_ROOT = Path("ZRS_KVP")


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


def parse_qsos(html_text: str, season_year: int) -> List[Tuple[int, str, str, str, str, str, str, str]]:
    """
    Parse QSO rows and return tuples:
    (freq, date, time, their_call, rst_sent, exch_sent, rst_recv, exch_recv)
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
        date_cell, time_cell, call_cell, mode_cell, rst_s_cell, exch_s_cell, rst_r_cell, exch_r_cell = cells[:8]
        their_call = clean_html_cell(call_cell)
        if not their_call:
            continue
        mode = clean_html_cell(mode_cell).upper()
        freq = freq_for_mode(mode)
        date = parse_date(clean_html_cell(date_cell), season_year)
        time_val = parse_time(clean_html_cell(time_cell))
        rst_s = clean_html_cell(rst_s_cell) or "59"
        rst_r = clean_html_cell(rst_r_cell) or "59"
        exch_s = sanitize_exchange(clean_html_cell(exch_s_cell))
        exch_r = sanitize_exchange(clean_html_cell(exch_r_cell))
        qsos.append((freq, date, time_val, their_call.upper(), rst_s, exch_s, rst_r, exch_r))
    return qsos


def build_cabrillo(call: str, category: str | None, qsos: Sequence[Tuple[int, str, str, str, str, str, str, str]], season: Season) -> str:
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
    for freq, date, time_val, their_call, rst_s, exch_s, rst_r, exch_r in qsos:
        mode = "CW" if freq == 3500 else "PH"
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
        print(f"Failed to fetch log {url}: {exc}", file=sys.stderr)
        return None
    call, category = extract_log_summary(html_text)
    if not call:
        print(f"Missing callsign in log {url}", file=sys.stderr)
        return None
    qsos = parse_qsos(html_text, season.year)
    if not qsos:
        print(f"No QSOs parsed for {call} ({url})", file=sys.stderr)
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
    dest.write_text(content, encoding="utf-8")
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
    args = parser.parse_args()

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

    tasks: List[Tuple[Season, str]] = []
    for season in seasons:
        log_links = discover_logs(season)
        if args.max_per_season:
            log_links = log_links[: args.max_per_season]
        print(f"{season.year} {season.season}: {len(log_links)} logs")
        for url in log_links:
            tasks.append((season, url))

    if not tasks:
        print("No logs found to download.")
        return 1

    print(f"Total logs to fetch: {len(tasks)}")

    def worker(season: Season, url: str) -> None:
        result = download_log(url, season)
        if not result:
            return
        call, cbr = result
        dest = write_log(OUTPUT_ROOT, season, call, cbr)
        print(f"ok   {dest}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(worker, season, url) for season, url in tasks]
        for fut in concurrent.futures.as_completed(futures):
            fut.result()

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
