#!/usr/bin/env python3
"""
Downloader for SP DX Contest logs reconstructed from public result JSON.

Source pages:
  https://spdxcontest.pzk.org.pl/<year>/claimed_logs.php
  https://spdxcontest.pzk.org.pl/<year>/results_json_files/<CALL>.json

Output layout:
  SPDX_contest/<year>/<CALL>.log
"""

from __future__ import annotations

import argparse
import html
import json
import random
import re
import time
import urllib.parse
import urllib.request
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
OUTPUT_ROOT = Path("SPDX_contest")
BASE_URL = "https://spdxcontest.pzk.org.pl"
MIN_YEAR = 2019
SP_PREFIXES = ("3Z", "HF", "SN", "SO", "SP", "SQ")
TASK_LEDGER: "TaskLedger | None" = None


def fetch_text(url: str, retries: int = 3, delay: float = 1.0) -> str:
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


def claimed_logs_url(year: int) -> str:
    return f"{BASE_URL}/{year}/claimed_logs.php"


def results_json_url(year: int, call: str) -> str:
    safe_call = call.replace("/", "_")
    quoted = urllib.parse.quote(safe_call)
    return f"{BASE_URL}/{year}/results_json_files/{quoted}.json"


def legacy_results_json_url(year: int, call: str) -> str:
    safe_call = call.replace("/", "_")
    quoted = urllib.parse.quote(safe_call)
    return f"{BASE_URL}/{year}/results/{quoted}.json"


def discover_years() -> List[int]:
    archive_url = f"{BASE_URL}/"
    html_text = fetch_text(archive_url)
    years = sorted(
        {
            int(match.group(1))
            for match in re.finditer(r'href="(\d{4})/"', html_text)
            if int(match.group(1)) >= MIN_YEAR
        }
    )
    if years:
        return years

    current_year = time.gmtime().tm_year
    discovered: List[int] = []
    for year in range(MIN_YEAR, current_year + 1):
        try:
            year_text = fetch_text(claimed_logs_url(year))
        except Exception:
            continue
        if "viewresult.php?call=" in year_text:
            discovered.append(year)
    return discovered


def discover_calls(year: int) -> List[str]:
    html_text = fetch_text(claimed_logs_url(year))
    seen: set[str] = set()
    calls: List[str] = []

    def add_call(raw: str) -> None:
        call = urllib.parse.unquote(raw).strip()
        if not call or call in seen:
            return
        if "STATION_CALLSIGN" in call or "+stations" in call:
            return
        seen.add(call)
        calls.append(call)

    for var_name in ("stationsSP", "stationsDX"):
        match = re.search(rf"var {var_name} = JSON\.parse\('(.+?)'\);", html_text, re.DOTALL)
        if not match:
            continue
        raw_json = html.unescape(match.group(1))
        raw_json = raw_json.replace("\\'", "'")
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, list):
            continue
        for item in parsed:
            if not isinstance(item, dict):
                continue
            raw_call = item.get("STATION_CALLSIGN")
            if raw_call is None:
                continue
            add_call(str(raw_call))

    if calls:
        return calls

    matches = re.findall(r'href="viewresult\.php\?call=([^"#]+)"', html_text)
    for raw in matches:
        add_call(raw)
    return calls


def fetch_log_data(year: int, call: str) -> Dict[str, object]:
    urls = [results_json_url(year, call), legacy_results_json_url(year, call)]
    last_exc: Exception | None = None
    for url in urls:
        try:
            text = fetch_text(url)
            return json.loads(text)
        except Exception as exc:  # pylint: disable=broad-except
            last_exc = exc
            continue
    raise last_exc if last_exc is not None else RuntimeError("failed to fetch SPDX log data")


def clean_field(value: object) -> str:
    text = html.unescape("" if value is None else str(value))
    text = re.sub(r"<br\s*/?>.*", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", "", text)
    return " ".join(text.strip().split())


def clean_mode(value: str) -> str:
    upper = clean_field(value).upper()
    if upper in {"SSB", "PHONE"}:
        return "PH"
    return upper


def band_to_freq(value: str) -> str:
    band = clean_field(value).lower()
    mapping = {
        "160m": "1800",
        "80m": "3500",
        "40m": "7000",
        "20m": "14000",
        "15m": "21000",
        "10m": "28000",
    }
    return mapping.get(band, band)


def qso_report(mode: str) -> str:
    return "59" if mode == "PH" else "599"


def is_sp_callsign(call: str) -> bool:
    upper = call.upper()
    return any(upper.startswith(prefix) for prefix in SP_PREFIXES)


def normalize_category(summary: Dict[str, object]) -> str:
    parts = [clean_field(summary.get("CLASS")), clean_field(summary.get("SUBCLASS"))]
    text = " ".join(part for part in parts if part)
    text = text.upper().replace("(", "").replace(")", "")
    return " ".join(text.split())


def build_regular_qso(record: Dict[str, object]) -> str | None:
    datetime_text = clean_field(record.get("LOGSP_DATETIME"))
    if not datetime_text:
        return None
    date_part, _, time_part = datetime_text.partition(" ")
    hhmm = time_part.replace(":", "")[:4]
    mode = clean_mode(str(record.get("MODE", "")))
    station = clean_field(record.get("STATION_CALLSIGN"))
    worked = clean_field(record.get("CALL"))
    sent = clean_field(record.get("STX_STRING"))
    received = clean_field(record.get("SRX_STRING"))
    if not all([date_part, hhmm, mode, station, worked]):
        return None
    report = qso_report(mode)
    return (
        f"QSO: {band_to_freq(str(record.get('BAND', '')))} {mode} {date_part} {hhmm} "
        f"{station} {report} {sent} {worked} {report} {received}"
    ).rstrip()


def build_swl_qso(record: Dict[str, object]) -> str | None:
    datetime_text = clean_field(record.get("LOGSP_DATETIME"))
    if not datetime_text:
        return None
    date_part, _, time_part = datetime_text.partition(" ")
    hhmm = time_part.replace(":", "")[:4]
    mode = clean_mode(str(record.get("MODE", "")))
    call_field = clean_field(record.get("CALL"))
    heard_a, sep, heard_b = call_field.partition(" - ")
    heard_a = heard_a.strip()
    heard_b = heard_b.strip()
    if not all([date_part, hhmm, mode, heard_a, sep, heard_b]):
        return None
    station = clean_field(record.get("STATION_CALLSIGN")) or heard_a
    report = qso_report(mode)
    exchange = clean_field(record.get("SRX_STRING"))
    return (
        f"QSO: {band_to_freq(str(record.get('BAND', '')))} {mode} {date_part} {hhmm} "
        f"{station} {report} {heard_a} {heard_b} {report} {exchange}"
    ).rstrip()


def build_cabrillo(year: int, call: str, payload: Dict[str, object]) -> str:
    summary = payload.get("summary", {})
    log = payload.get("log", [])
    if not isinstance(summary, dict) or not isinstance(log, list):
        raise ValueError("unexpected SPDX payload structure")

    station_call = clean_field(summary.get("STATION_CALLSIGN")) or call
    category = normalize_category(summary)
    score = clean_field(summary.get("RESULTS")) or clean_field(summary.get("POINTS"))
    operators = clean_field(summary.get("OPERATORS"))
    is_swl = category.startswith("SWL ")

    lines = [
        "START-OF-LOG: 2.0",
        f"CALLSIGN: {station_call}",
        "CONTEST: SPDX",
    ]
    if category:
        lines.append(f"CATEGORY: {category}")
    if score:
        lines.append(f"CLAIMED-SCORE: {score}")
    if operators:
        lines.append(f"OPERATORS: {operators}")
    lines.append("CREATED-BY: HMRA public SPDX downloader")
    lines.append(
        f"SOAPBOX: Recreated from {results_json_url(year, station_call)}"
    )

    for item in log:
        if not isinstance(item, dict):
            continue
        qso_line = build_swl_qso(item) if is_swl else build_regular_qso(item)
        if qso_line:
            lines.append(qso_line)

    lines.append("END-OF-LOG:")
    return "\n".join(lines) + "\n"


def dest_path(year: int, call: str) -> Path:
    safe_call = call.replace("/", "_")
    return OUTPUT_ROOT / str(year) / f"{safe_call}.log"


def write_log(dest: Path, content: str) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        dest.write_text(content, encoding="utf-8")
    return dest


def iter_years(from_year: int | None, to_year: int | None) -> Iterable[int]:
    years = discover_years()
    if from_year is None and to_year is None:
        return years
    start = from_year if from_year is not None else MIN_YEAR
    end = to_year if to_year is not None else max(years, default=start)
    return [year for year in years if start <= year <= end]


def main() -> int:
    parser = argparse.ArgumentParser(description="Download SPDX Contest public logs.")
    parser.add_argument("--from-year", type=int, default=None, help="Start year.")
    parser.add_argument("--to-year", type=int, default=None, help="End year.")
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
    for year in iter_years(args.from_year, args.to_year):
        try:
            calls = discover_calls(year)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"{year}: list failed: {exc}")
            total_err += 1
            continue
        if not calls:
            continue
        task_key = f"{Path(__file__).stem}:{year}"
        skip, list_hash, item_count = task_should_skip(TASK_LEDGER, calls, upper=True)
        if skip:
            print(f"{year}: skip (task ledger) items={item_count}")
            continue
        year_errors = 0
        for call in calls:
            dest = dest_path(year, call)
            if dest.exists():
                total_skip += 1
                continue
            try:
                payload = fetch_log_data(year, call)
                cabrillo = build_cabrillo(year, call, payload)
                write_log(dest, cabrillo)
            except Exception as exc:  # pylint: disable=broad-except
                print(f"fail {call} {year}: {exc}")
                total_err += 1
                year_errors += 1
                continue
            total_ok += 1
        if year_errors == 0:
            task_mark_complete(TASK_LEDGER, task_key, list_hash, item_count)
    print(f"done ok={total_ok} skip={total_skip} err={total_err}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
