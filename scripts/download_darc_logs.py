#!/usr/bin/env python3
"""
Downloader for DARC open logs from dxhf2.darc.de.

Sources (per contest base):
  - Current results:   https://dxhf2.darc.de/~<base>/user.cgi?fc=loglist&form=referat&lang=en
  - Archive results:   https://dxhf2.darc.de/~<base>/arch_res.cgi (POST year, type)
  - Open logs request: https://dxhf2.darc.de/~<base>/user.cgi?fc=req_olog&form=referat&lang=en&call=<CALL>&jahr=<YEAR>&status=show

Output layout:
  - DARC/Fieldday/CW/<year>/<CALL>.log
  - DARC/Fieldday/SSB/<year>/<CALL>.log
  - DARC/WAG/<year>/<CALL>.log
  - DARC/Ausbildungscontest/<year>/<CALL>.log
  - DARC/Ausbildungscontest_CW/<year>/<edition>/<CALL>.log
  - DARC/RTTY_Kurzcontest/<year>/<edition>/<CALL>.log
  - DARC/FT4/<year>/<edition>/<CALL>.log
  - DARC/Easter/<year>/<CALL>.log
  - DARC/XMAS/<year>/<CALL>.log
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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple

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


@dataclass(frozen=True)
class ContestSpec:
    key: str
    base: str
    output_root: Path
    label: str
    editions: Tuple[str, ...] = ()
    request_params: Dict[str, str] = field(default_factory=dict)


CONTESTS: Dict[str, ContestSpec] = {
    "fieldday_cw": ContestSpec(
        key="fieldday_cw",
        base="fdcwlog",
        output_root=Path("DARC") / "Fieldday" / "CW",
        label="DARC Fieldday CW",
    ),
    "fieldday_ssb": ContestSpec(
        key="fieldday_ssb",
        base="fdssblog",
        output_root=Path("DARC") / "Fieldday" / "SSB",
        label="DARC Fieldday SSB",
    ),
    "wag": ContestSpec(
        key="wag",
        base="waglog",
        output_root=Path("DARC") / "WAG",
        label="DARC WAG",
    ),
    "ausbildungscontest": ContestSpec(
        key="ausbildungscontest",
        base="aclog",
        output_root=Path("DARC") / "Ausbildungscontest",
        label="DARC Ausbildungscontest",
    ),
    "ausbildungscontest_cw": ContestSpec(
        key="ausbildungscontest_cw",
        base="accwlog",
        output_root=Path("DARC") / "Ausbildungscontest_CW",
        label="DARC Ausbildungscontest CW",
        editions=("mar", "jun", "sep", "dec"),
    ),
    "rtty_kurzcontest": ContestSpec(
        key="rtty_kurzcontest",
        base="shortrylog",
        output_root=Path("DARC") / "RTTY_Kurzcontest",
        label="DARC RTTY-Kurzcontest",
        editions=("jan", "apr", "jul", "oct"),
    ),
    "ft4": ContestSpec(
        key="ft4",
        base="ft4log",
        output_root=Path("DARC") / "FT4",
        label="DARC FT4 Contest",
        editions=("feb", "mai", "aug", "nov"),
    ),
    "easter": ContestSpec(
        key="easter",
        base="easterlog",
        output_root=Path("DARC") / "Easter",
        label="DARC Easter",
    ),
    "xmas": ContestSpec(
        key="xmas",
        base="xmaslog",
        output_root=Path("DARC") / "XMAS",
        label="DARC XMAS",
    ),
}


TASK_LEDGER: "TaskLedger | None" = None


def destination_log_exists(path: Path) -> bool:
    try:
        return archive_log_exists(path)
    except ValueError:
        return path.exists()


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


def clean_cell(cell: str) -> str:
    return " ".join(html.unescape(re.sub(r"<[^>]+>", "", cell)).split()).upper()


def is_callsign(text: str) -> bool:
    if not re.fullmatch(r"[A-Z0-9/]+", text):
        return False
    if not re.search(r"\d", text) or not re.search(r"[A-Z]", text):
        return False
    return bool(re.search(r"\d+[A-Z]", text))


def calls_from_table(html_text: str) -> List[str]:
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


def current_period_from_loglist(html_text: str) -> Tuple[int | None, str | None]:
    edition_match = re.search(r"(20\d{2})\s*/\s*([a-z]{3})", html_text, flags=re.IGNORECASE)
    if edition_match:
        return int(edition_match.group(1)), edition_match.group(2).lower()
    years = [int(y) for y in re.findall(r"(20\d{2})", html_text)]
    return (max(years), None) if years else (None, None)


def discover_loglist(spec: ContestSpec) -> Tuple[int | None, str | None, List[str]]:
    url = f"https://dxhf2.darc.de/~{spec.base}/user.cgi?fc=loglist&form=referat&lang=en"
    html_text = fetch_text(url)
    year, edition = current_period_from_loglist(html_text)
    return year, edition, calls_from_table(html_text)


def discover_years(spec: ContestSpec, loglist_year: int | None) -> List[int]:
    years: Set[int] = set()
    # Open log page year options
    url = f"https://dxhf2.darc.de/~{spec.base}/user.cgi?fc=req_olog&form=referat&lang=en"
    html_text = fetch_text(url)
    for match in re.finditer(r'<option[^>]*value=\"(\d{4})\"', html_text):
        years.add(int(match.group(1)))
    if spec.editions:
        for match in re.finditer(r'<option[^>]*value=\"([a-z]{3})\"', html_text, flags=re.IGNORECASE):
            value = match.group(1).lower()
            if value in spec.editions:
                years.add(loglist_year or 0)
    # Archive page year options
    arch_html = fetch_text(f"https://dxhf2.darc.de/~{spec.base}/arch_res.cgi?form=referat&lang=en")
    for match in re.finditer(r'<option[^>]*value=\"(\d{4})\"', arch_html):
        years.add(int(match.group(1)))
    if loglist_year:
        years.add(loglist_year)
    years.discard(0)
    return sorted(years, reverse=True)


def archive_html(spec: ContestSpec, year: int, edition: str | None = None, lang: str = "en") -> str:
    params = {
        "form": "referat",
        "lang": lang,
        "year": str(year),
        "type": "Category",
    }
    if edition:
        params["edition"] = edition
    data = urllib.parse.urlencode(params).encode()
    return fetch_text(f"https://dxhf2.darc.de/~{spec.base}/arch_res.cgi", data=data)


def discover_calls_for_period(
    spec: ContestSpec,
    year: int,
    edition: str | None,
    loglist_year: int | None,
    loglist_edition: str | None,
    loglist_calls: Sequence[str],
) -> List[str]:
    calls: List[str] = []
    if loglist_year and loglist_year == year and loglist_edition == edition and loglist_calls:
        calls.extend(loglist_calls)
    html_en = archive_html(spec, year, edition=edition, lang="en")
    calls.extend(calls_from_table(html_en))
    if not calls:
        html_de = archive_html(spec, year, edition=edition, lang="de")
        calls.extend(calls_from_table(html_de))
    seen: Set[str] = set()
    uniq: List[str] = []
    for call in calls:
        if call in seen:
            continue
        seen.add(call)
        uniq.append(call)
    return uniq


def discover_periods(spec: ContestSpec, last: int | None = None) -> List[Tuple[int, str | None]]:
    loglist_year, loglist_edition, _ = discover_loglist(spec)
    years = discover_years(spec, loglist_year)
    if last:
        years = years[:last]
    periods: List[Tuple[int, str | None]] = []
    if not spec.editions:
        return [(year, None) for year in years]
    for year in years:
        year_editions = list(spec.editions)
        if loglist_year == year and loglist_edition and loglist_edition in year_editions:
            year_editions = [loglist_edition] + [ed for ed in year_editions if ed != loglist_edition]
        periods.extend((year, edition) for edition in year_editions)
    return periods


def period_label(year: int, edition: str | None) -> str:
    return f"{year}/{edition}" if edition else str(year)


def extract_cabrillo(html_text: str) -> str | None:
    match = re.search(r"(START-OF-LOG:.*?END-OF-LOG:)", html_text, flags=re.DOTALL | re.IGNORECASE)
    if not match:
        return None
    cab = match.group(1)
    return "\n".join(line.rstrip() for line in cab.splitlines()) + "\n"


def fetch_log(spec: ContestSpec, call: str, year: int, edition: str | None = None) -> str | None:
    params = {
        "fc": "req_olog",
        "form": "referat",
        "lang": "en",
        "call": call,
        "jahr": str(year),
        "status": "show",
    }
    params.update(spec.request_params)
    if edition:
        params["edition"] = edition
    url = f"https://dxhf2.darc.de/~{spec.base}/user.cgi?{urllib.parse.urlencode(params)}"
    html_text = fetch_text(url)
    return extract_cabrillo(html_text)


def period_has_public_logs(
    spec: ContestSpec,
    year: int,
    edition: str | None,
    calls: Sequence[str],
    sample_size: int = 3,
) -> bool:
    for call in calls[:sample_size]:
        cab = fetch_log(spec, call, year, edition=edition)
        if cab:
            return True
    return False


def write_log(spec: ContestSpec, year: int, call: str, content: str, edition: str | None = None) -> Path:
    safe_call = call.replace("/", "_")
    dest = spec.output_root / str(year)
    if edition:
        dest = dest / edition
    dest = dest / f"{safe_call}.log"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if destination_log_exists(dest):
        return dest
    atomic_write_text(dest, content)
    return dest


def parse_contests(text: str) -> List[str]:
    if not text:
        return list(CONTESTS.keys())
    lowered = [part.strip().lower() for part in text.split(",") if part.strip()]
    if any(val in {"all", "a"} for val in lowered):
        return list(CONTESTS.keys())
    selected = []
    for key in lowered:
        if key in CONTESTS:
            selected.append(key)
    return selected


def main() -> int:
    parser = argparse.ArgumentParser(description="Download DARC open logs (Fieldday CW/SSB, WAG).")
    parser.add_argument(
        "--contests",
        type=str,
        default="all",
        help="Comma list of contest keys (default: all).",
    )
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
        help="SQLite task ledger (default: state/downloads/tasks.sqlite).",
    )
    parser.add_argument("--no-task-ledger", action="store_true", help="Disable task ledger usage.")
    args = parser.parse_args()

    selected = parse_contests(args.contests)
    if not selected:
        print("No contests selected.", file=sys.stderr)
        return 1

    global TASK_LEDGER
    TASK_LEDGER = None if args.no_task_ledger else TaskLedger(args.task_ledger)

    def worker(spec: ContestSpec, year: int, edition: str | None, call: str) -> dict[str, int]:
        dest = spec.output_root / str(year)
        if edition:
            dest = dest / edition
        dest = dest / f"{call.replace('/', '_')}.log"
        if destination_log_exists(dest):
            print(f"skip (exists): {dest}")
            return {"skip": 1}
        try:
            cab = fetch_log(spec, call, year, edition=edition)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"fail {call} {period_label(year, edition)} ({spec.label}): {exc}")
            return {"error": 1}
        if not cab:
            print(f"skip (no cabrillo): {call} {period_label(year, edition)} ({spec.label})")
            return {"skip": 1}
        final_dest = write_log(spec, year, call, cab, edition=edition)
        print(f"ok   {final_dest}")
        return {"ok": 1}

    total_calls = 0
    for key in selected:
        spec = CONTESTS[key]
        try:
            loglist_year, loglist_edition, loglist_calls = discover_loglist(spec)
            periods = discover_periods(spec)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Failed to discover periods for {spec.label}: {exc}", file=sys.stderr)
            continue
        if not periods:
            continue
        accepted_periods = 0
        for year, edition in periods:
            try:
                calls = discover_calls_for_period(
                    spec,
                    year,
                    edition,
                    loglist_year,
                    loglist_edition,
                    loglist_calls,
                )
            except Exception as exc:  # pylint: disable=broad-except
                print(f"Failed to discover calls {spec.label} {period_label(year, edition)}: {exc}", file=sys.stderr)
                continue
            if args.max_per_year is not None:
                calls = calls[: args.max_per_year]
            if not calls:
                continue
            try:
                has_public_logs = period_has_public_logs(spec, year, edition, calls)
            except Exception as exc:  # pylint: disable=broad-except
                print(f"Failed to probe logs {spec.label} {period_label(year, edition)}: {exc}", file=sys.stderr)
                continue
            if not has_public_logs:
                print(f"skip (no public logs yet): {spec.label} {period_label(year, edition)}")
                continue
            if last is not None and accepted_periods >= last:
                break
            accepted_periods += 1
            total_calls += len(calls)
            period_key = period_label(year, edition)
            task_key = f"{Path(__file__).stem}:{spec.key}:{period_key}"
            skip, list_hash, item_count = task_should_skip(TASK_LEDGER, task_key, calls, upper=True)
            if skip:
                print(f"skip (task ledger): {spec.label} {period_key} items={item_count}")
                continue

            counts = {"ok": 0, "skip": 0, "error": 0}
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [executor.submit(worker, spec, year, edition, call) for call in calls]
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    for key, value in result.items():
                        counts[key] = counts.get(key, 0) + value
            if counts.get("error", 0) == 0:
                task_mark_complete(TASK_LEDGER, task_key, list_hash, item_count)

    if total_calls == 0:
        print("No logs to download.", file=sys.stderr)
        return 1

    print("Done.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        if TASK_LEDGER is not None:
            TASK_LEDGER.close()
