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
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple

USER_AGENT = "Mozilla/5.0 (compatible; wae-olog-downloader/1.0)"
REQUEST_TIMEOUT = 30
DEFAULT_WORKERS = 10
OUTPUT_ROOT = Path("WAE")

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


def discover_years(mode_base: str) -> List[int]:
    """Parse available years from the Open Log form (req_olog)."""
    url = f"https://dxhf2.darc.de/~{mode_base}/user.cgi?fc=req_olog&form=referat&lang=en"
    html_text = fetch_text(url)
    years: Set[int] = set()
    for match in re.finditer(r'<option[^>]*value="(\d{4})"', html_text):
        years.add(int(match.group(1)))
    return sorted(years, reverse=True)


def calls_from_table(html_text: str) -> List[str]:
    """Extract calls from the first callsign column in result tables."""
    calls: List[str] = []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html_text, flags=re.DOTALL | re.IGNORECASE):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.DOTALL | re.IGNORECASE)
        if len(cells) < 2:
            continue
        call_raw = clean_cell(cells[1])
        if is_callsign(call_raw):
            calls.append(call_raw)
    return calls


def is_callsign(text: str) -> bool:
    """Simple callsign heuristic: contains a digit and a letter."""
    return bool(re.search(r"[A-Z]", text)) and bool(re.search(r"\d", text))


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
    args = parser.parse_args()

    selected_modes = [m.strip().lower() for m in args.modes.split(",") if m.strip().lower() in MODES]
    if not selected_modes:
        print("No valid modes selected.", file=sys.stderr)
        return 1

    tasks: List[Tuple[str, int, str, str]] = []
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
            for call in calls:
                tasks.append((mode, year, base, call))

    if not tasks:
        print("No logs to fetch.")
        return 0

    print(f"Total logs to fetch: {len(tasks)}")

    def worker(mode: str, year: int, base: str, call: str) -> None:
        # Skip fetching if the file is already present
        safe_call = call.replace("/", "_")
        dest = OUTPUT_ROOT / mode.upper() / str(year) / f"{safe_call}.log"
        if dest.exists():
            print(f"skip (exists): {dest}")
            return
        cab = fetch_log(base, call, year)
        if not cab:
            print(f"Missing cabrillo for {call} {year} ({mode})", file=sys.stderr)
            return
        dest = write_log(mode, year, call, cab)
        print(f"ok   {dest}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(worker, mode, year, base, call) for mode, year, base, call in tasks]
        for fut in concurrent.futures.as_completed(futures):
            fut.result()

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
