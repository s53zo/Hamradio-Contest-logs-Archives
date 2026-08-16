#!/usr/bin/env python3
"""
Downloader/parser for OK-OM DX Contest public logs on okomdx.crk.cz.

Year pages (examples):
  http://okomdx.crk.cz/index.php?page=2012-2
  http://okomdx.crk.cz/index.php?page=2023-3

Each year page embeds an iframe with a results page:
  http://okomdx.crk.cz/eval/index.php?str=results&id_round=149

The results page lists log pages (not always as hrefs):
  http://okomdx.crk.cz/eval/index.php?str=log&id_round=149&callsign=OK5Z

We rebuild Cabrillo logs from the QSO table and skip rejected rows.

Output layout:
  OK_Contest/<year>/<mode>/<CALL>.log
  OK_OM_DX_Contest/<year>/<mode>/<CALL>.log
  OK_DX_RTTY_contest/<year>/<CALL>.log
"""

from __future__ import annotations

import argparse
import html
import random
import re
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

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
BASE_URL = "http://okomdx.crk.cz"
RESULTS_INDEX = f"{BASE_URL}/index.php?page=results"
OUTPUT_ROOT_OKOM = Path("OK_OM_DX_Contest")
OUTPUT_ROOT_OK = Path("OK_Contest")
OUTPUT_ROOT_RTTY = Path("OK_DX_RTTY_contest")
TASK_LEDGER: "TaskLedger | None" = None


def fetch_text(url: str, retries: int = 3, delay: float = 1.0) -> str:
    """Fetch a URL and return decoded text with simple retries."""
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                charset = resp.headers.get_content_charset()
                raw = resp.read()
                if charset:
                    return raw.decode(charset, errors="ignore")
                # best-effort fallback for older pages
                try:
                    return raw.decode("utf-8")
                except UnicodeDecodeError:
                    return raw.decode("windows-1250", errors="ignore")
        except Exception as exc:  # pylint: disable=broad-except
            last_exc = exc
            if attempt + 1 < retries:
                time.sleep(delay * (2 ** attempt))
            else:
                raise
    raise last_exc  # type: ignore[misc]


def discover_year_pages() -> List[Tuple[int, str]]:
    """Return list of (year, page_slug) like ('2012-2')."""
    html_text = fetch_text(RESULTS_INDEX)
    pages = set(re.findall(r"page=(\d{4}-\d+)", html_text))
    results: List[Tuple[int, str]] = []
    for slug in pages:
        year_str = slug.split("-", 1)[0]
        if year_str.isdigit():
            results.append((int(year_str), slug))
    results.sort()
    return results


def discover_rounds(page_slug: str) -> List[int]:
    url = f"{BASE_URL}/index.php?page={page_slug}"
    html_text = fetch_text(url)
    rounds = sorted({int(m.group(1)) for m in re.finditer(r"id_round=(\d+)", html_text)})
    return rounds


def discover_calls(id_round: int) -> List[str]:
    url = f"{BASE_URL}/eval/index.php?str=results&id_round={id_round}"
    html_text = fetch_text(url)
    region_raw = re.findall(r"region=([^&\"'>\\s]+)", html_text, flags=re.IGNORECASE)
    regions = []
    for region in region_raw:
        region = html.unescape(region)
        region = urllib.parse.unquote(region)
        if region and region not in regions:
            regions.append(region)
    result_urls = [url]
    for region in regions:
        region_param = urllib.parse.quote(region)
        result_urls.append(f"{BASE_URL}/eval/index.php?str=results&id_round={id_round}&region={region_param}")

    pattern = re.compile(r"index\.php\?str=log&id_round=" + re.escape(str(id_round)) + r"&callsign=([A-Z0-9/]+)")
    seen: set[str] = set()
    uniq: List[str] = []
    for result_url in result_urls:
        page_html = fetch_text(result_url)
        for match in pattern.finditer(page_html):
            call = match.group(1).upper()
            if call in seen:
                continue
            seen.add(call)
            uniq.append(call)
    return uniq


def fetch_log_html(id_round: int, call: str) -> str:
    url = f"{BASE_URL}/eval/index.php?str=log&id_round={id_round}&callsign={urllib.parse.quote(call)}"
    return fetch_text(url)


def _clean(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    return " ".join(text.split()).strip()


@dataclass
class LogMeta:
    contest_title: str
    contest_root: Path
    year: int
    mode_label: str
    call: str
    operators: str
    category: str


def _normalize_mode_label(token: str) -> str:
    upper = token.upper()
    if upper in {"CW"}:
        return "CW"
    if upper in {"PH", "SSB", "PHONE"}:
        return "SSB"
    if upper in {"RTTY", "RY"}:
        return "RTTY"
    return ""


def parse_header_meta(html_text: str, fallback_year: int, fallback_call: str) -> Optional[LogMeta]:
    title_match = re.search(r"<h2>([^<]+)</h2>", html_text, flags=re.IGNORECASE)
    if not title_match:
        return None
    contest_title = _clean(title_match.group(1))
    title_upper = contest_title.upper()

    year_match = re.search(r"(19|20)\d{2}", contest_title)
    year = int(year_match.group(0)) if year_match else fallback_year

    if "OK DX RTTY" in title_upper:
        contest_root = OUTPUT_ROOT_RTTY
        mode_label = "RTTY"
    elif "OK-OM DX" in title_upper:
        contest_root = OUTPUT_ROOT_OKOM
        mode_label = _normalize_mode_label(title_upper)
    elif re.search(r"\bOK\s+CW\s+CONTEST\b", title_upper):
        contest_root = OUTPUT_ROOT_OK
        mode_label = "CW"
    elif re.search(r"\bOK\s+SSB\s+CONTEST\b", title_upper):
        contest_root = OUTPUT_ROOT_OK
        mode_label = "SSB"
    else:
        return None

    # Parse the small header table
    call = fallback_call
    operators = fallback_call
    category = ""
    table_match = re.search(r"<table[^>]*class='vypis'[^>]*>(.*?)</table>", html_text, flags=re.DOTALL | re.IGNORECASE)
    if table_match:
        tds = re.findall(r"<td[^>]*>(.*?)</td>", table_match.group(1), flags=re.DOTALL | re.IGNORECASE)
        if len(tds) >= 1:
            call = _clean(tds[0]).upper() or call
        if len(tds) >= 2:
            operators = _clean(tds[1]) or operators
        if len(tds) >= 3:
            category = _clean(tds[2])

    return LogMeta(
        contest_title=contest_title,
        contest_root=contest_root,
        year=year,
        mode_label=mode_label,
        call=call,
        operators=operators,
        category=category,
    )


def parse_datetime(value: str) -> Tuple[str, str]:
    # expected: "dd.mm.yyyy hh:mm"
    date_match = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", value)
    time_match = re.search(r"(\d{1,2}):(\d{2})", value)
    if not date_match or not time_match:
        return ("0000-00-00", "0000")
    day, month, year = date_match.group(1), date_match.group(2), date_match.group(3)
    hour, minute = time_match.group(1), time_match.group(2)
    date_out = f"{year}-{int(month):02d}-{int(day):02d}"
    time_out = f"{int(hour):02d}{int(minute):02d}"
    return date_out, time_out


def qso_mode_from_label(mode_label: str) -> str:
    upper = mode_label.upper()
    if upper == "CW":
        return "CW"
    if upper == "RTTY":
        return "RY"
    return "PH"


def parse_qsos(
    html_text: str, mode_label: str
) -> Tuple[List[Tuple[int, str, str, str, str, str, str, str, str, str]], str]:
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html_text, flags=re.DOTALL | re.IGNORECASE)
    qsos: List[Tuple[int, str, str, str, str, str, str, str, str, str]] = []
    detected_label = ""
    for row in rows:
        if "<td" not in row:
            continue
        tds = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.DOTALL | re.IGNORECASE)
        if len(tds) < 12:
            continue
        tds = [_clean(td) for td in tds]
        if not tds[0].isdigit():
            continue
        freq = tds[1]
        if not freq.isdigit():
            continue
        row_mode = _normalize_mode_label(tds[2])
        if row_mode and not detected_label:
            detected_label = row_mode
        date_val = tds[3]
        mycall = tds[4].upper()
        rst_s = tds[5] or ""
        exch_s = tds[6] or ""
        their_call = tds[7].upper()
        rst_r = tds[8] or ""
        exch_r = tds[9] or ""
        problem = tds[-1]
        if problem:
            continue
        date_out, time_out = parse_datetime(date_val)
        mode_out = qso_mode_from_label(row_mode or mode_label)
        qsos.append(
            (
                int(freq),
                date_out,
                time_out,
                mycall,
                rst_s,
                exch_s,
                their_call,
                rst_r,
                exch_r,
                mode_out,
            )
        )
    return qsos, (detected_label or _normalize_mode_label(mode_label))


def derive_category_fields(category: str, mode_label: str) -> Dict[str, str]:
    upper = category.upper()
    operator = "SINGLE-OP"
    if "MULTI" in upper:
        operator = "MULTI-OP"
    elif "CHECKLOG" in upper:
        operator = "CHECKLOG"
    elif "SWL" in upper:
        operator = "SWL"
    band = "ALL"
    for token in ["160M", "80M", "40M", "20M", "15M", "10M", "ALL"]:
        if token in upper:
            band = token
            break
    power = ""
    if "HIGH" in upper:
        power = "HIGH"
    elif "LOW" in upper:
        power = "LOW"
    elif "QRP" in upper:
        power = "QRP"
    transmitter = "ONE"
    if "MULTI-TWO" in upper:
        transmitter = "TWO"
    return {
        "operator": operator,
        "band": band,
        "mode": mode_label,
        "power": power,
        "assisted": "NON-ASSISTED",
        "transmitter": transmitter,
        "station": "FIXED",
    }


def build_cabrillo(meta: LogMeta, qsos: Sequence[Tuple[int, str, str, str, str, str, str, str, str, str]]) -> str:
    cat_info = derive_category_fields(meta.category, meta.mode_label)
    lines = [
        "START-OF-LOG: 3.0",
        "CREATED-BY: okomdx-downloader",
        f"CONTEST: {meta.contest_title}",
        f"CALLSIGN: {meta.call}",
        f"OPERATORS: {meta.operators or meta.call}",
        f"CATEGORY: {meta.category}",
        f"CATEGORY-OPERATOR: {cat_info['operator']}",
        f"CATEGORY-BAND: {cat_info['band']}",
        f"CATEGORY-MODE: {cat_info['mode']}",
        f"CATEGORY-POWER: {cat_info['power']}",
        f"CATEGORY-ASSISTED: {cat_info['assisted']}",
        f"CATEGORY-TRANSMITTER: {cat_info['transmitter']}",
        f"CATEGORY-STATION: {cat_info['station']}",
    ]
    for freq, date_out, time_out, mycall, rst_s, exch_s, their_call, rst_r, exch_r, mode_out in qsos:
        lines.append(
            f"QSO: {freq:>5} {mode_out:<2} {date_out} {time_out:>4} "
            f"{mycall:<13} {rst_s:<3} {exch_s:<10} {their_call:<13} {rst_r:<3} {exch_r:<10}"
        )
    lines.append("END-OF-LOG:")
    return "\n".join(lines) + "\n"


def dest_path(meta: LogMeta, call: str) -> Path:
    safe_call = call.replace("/", "_")
    if meta.contest_root == OUTPUT_ROOT_RTTY:
        return meta.contest_root / str(meta.year) / f"{safe_call}.log"
    return meta.contest_root / str(meta.year) / meta.mode_label / f"{safe_call}.log"


def write_log(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if archive_log_exists(path):
        return
    atomic_write_text(path, content)


def classify_round(id_round: int, fallback_year: int, calls: List[str]) -> Optional[LogMeta]:
    if not calls:
        return None
    sample_call = calls[0]
    html_text = fetch_log_html(id_round, sample_call)
    meta = parse_header_meta(html_text, fallback_year, sample_call)
    if meta:
        _qsos, detected = parse_qsos(html_text, meta.mode_label)
        if detected:
            meta.mode_label = detected
    return meta


def iter_year_pages(last: int | None) -> List[Tuple[int, str]]:
    pages = discover_year_pages()
    pages.sort(reverse=True)
    if last:
        years_seen: set[int] = set()
        limited: List[Tuple[int, str]] = []
        for year, slug in pages:
            if year in years_seen:
                limited.append((year, slug))
                continue
            years_seen.add(year)
            if len(years_seen) > last:
                continue
            limited.append((year, slug))
        pages = limited
    return pages


def main() -> int:
    parser = argparse.ArgumentParser(description="Download OK-OM DX Contest logs (CW/SSB/RTTY).")
    parser.add_argument("--last", type=int, default=None, help="Only latest N years")
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

    total_ok = total_skip = total_err = 0
    pages = iter_year_pages(args.last)
    for year, slug in pages:
        rounds = discover_rounds(slug)
        for round_id in rounds:
            calls = discover_calls(round_id)
            if not calls:
                continue
            task_key = f"{Path(__file__).stem}:{year}:{round_id}"
            skip, list_hash, item_count = task_should_skip(
                TASK_LEDGER, task_key, calls, upper=True
            )
            if skip:
                print(f"skip (task ledger): {year} {round_id} items={item_count}")
                continue
            meta = classify_round(round_id, year, calls)
            if not meta:
                print(f"unknown contest header for round {round_id} (year {year})")
                continue
            errors = 0
            for call in calls:
                html_text = fetch_log_html(round_id, call)
                parsed = parse_header_meta(html_text, meta.year, call)
                if not parsed:
                    total_err += 1
                    errors += 1
                    continue
                qsos, detected = parse_qsos(html_text, parsed.mode_label)
                if detected:
                    parsed.mode_label = detected
                cab = build_cabrillo(parsed, qsos)
                dest = dest_path(parsed, parsed.call)
                if archive_log_exists(dest):
                    total_skip += 1
                    continue
                write_log(dest, cab)
                total_ok += 1
            if errors == 0:
                task_mark_complete(TASK_LEDGER, task_key, list_hash, item_count)
    print(f"done ok={total_ok} skip={total_skip} err={total_err}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        if TASK_LEDGER is not None:
            TASK_LEDGER.close()
