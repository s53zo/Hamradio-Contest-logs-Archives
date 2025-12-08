#!/usr/bin/env python3
"""
Downloader for VHFManager contest logs (official/unofficial results).

Flow:
1) Discover contests by scanning VHFManager pages for results.php?ContestID=...
2) For each contest, collect display_log.php links on the results page.
3) Fetch every log page, extract Station/Category header and QSO table, and rebuild Cabrillo.

Output: VHF_MANAGER/<contest_slug>_<ContestID>/<CALL>.log
"""

from __future__ import annotations

import argparse
import concurrent.futures
import html
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

USER_AGENT = "Mozilla/5.0 (compatible; vhfmanager-downloader/1.0)"
REQUEST_TIMEOUT = 30
DEFAULT_WORKERS = 10
BASE_URL = "https://vhfmanager.net"
OUTPUT_ROOT = Path("VHF_MANAGER")


def fetch_text(url: str, retries: int = 3, delay: float = 1.0) -> str:
    """Fetch a URL and return decoded text."""
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


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_") or "contest"


@dataclass
class Contest:
    cid: int
    name: str
    results_url: str


@dataclass
class LogLink:
    url: str
    call_hint: Optional[str]
    category_hint: Optional[str]


def clean(text: str) -> str:
    no_tags = re.sub(r"<[^>]+>", "", text)
    unescaped = html.unescape(no_tags)
    return " ".join(unescaped.split())


def discover_contests(limit: int | None) -> List[Contest]:
    pages = [
        f"{BASE_URL}/",
        f"{BASE_URL}/?language=G",
        f"{BASE_URL}/modules/competitions.php?language=G",
    ]
    contests: dict[int, Contest] = {}
    for page in pages:
        try:
            html_text = fetch_text(page)
        except Exception:
            continue
        for match in re.finditer(r"results\\.php\\?ContestID=(\\d+)", html_text, flags=re.IGNORECASE):
            cid = int(match.group(1))
            if cid in contests:
                continue
            results_url = urllib.parse.urljoin(page, f"modules/results.php?ContestID={cid}&language=G")
            contests[cid] = Contest(cid=cid, name=f"Contest_{cid}", results_url=results_url)
            if limit and len(contests) >= limit:
                break
        if limit and len(contests) >= limit:
            break
    return sorted(contests.values(), key=lambda c: c.cid, reverse=True)


def parse_contest_name(html_text: str, cid: int) -> str:
    m = re.search(r"<title[^>]*>([^<]+)</title>", html_text, flags=re.IGNORECASE)
    if m:
        title = clean(m.group(1))
        if title:
            return title
    m = re.search(r"<h[12][^>]*>([^<]+)</h[12]>", html_text, flags=re.IGNORECASE)
    if m:
        heading = clean(m.group(1))
        if heading:
            return heading
    return f"Contest_{cid}"


def discover_logs(contest: Contest) -> Tuple[Contest, List[LogLink]]:
    html_text = fetch_text(contest.results_url)
    contest = Contest(cid=contest.cid, name=parse_contest_name(html_text, contest.cid), results_url=contest.results_url)
    links: List[LogLink] = []
    row_re = re.compile(r"<tr[^>]*>(.*?)</tr>", flags=re.IGNORECASE | re.DOTALL)
    for row_match in row_re.finditer(html_text):
        row_html = row_match.group(1)
        if "display_log" not in row_html.lower():
            continue
        href_match = re.search(
            r'href="([^"]*display_log[^"]*ContestID=\\d+[^"]*logID=\\d+[^"]*)"',
            row_html,
            flags=re.IGNORECASE,
        )
        if not href_match:
            continue
        href = html.unescape(href_match.group(1))
        abs_url = urllib.parse.urljoin(contest.results_url, href)
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, flags=re.IGNORECASE | re.DOTALL)
        call_hint = None
        for cell in cells:
            text = clean(cell).upper()
            if re.match(r"^[A-Z0-9/]{3,}$", text):
                call_hint = text
                break
        links.append(LogLink(url=abs_url, call_hint=call_hint, category_hint=None))
    return contest, links


def parse_log_header(html_text: str) -> Tuple[Optional[str], Optional[str]]:
    call = None
    category = None
    summary_match = re.search(r'<dl[^>]*class="log_summary"[^>]*>(.*?)</dl>', html_text, flags=re.IGNORECASE | re.DOTALL)
    if summary_match:
        block = summary_match.group(1)
        call_match = re.search(r"<dt>\\s*Station:.*?</dt>\\s*<dd>(.*?)</dd>", block, flags=re.IGNORECASE | re.DOTALL)
        if call_match:
            call = clean(call_match.group(1)).upper()
        cat_match = re.search(r"<dt>\\s*Category:.*?</dt>\\s*<dd>(.*?)</dd>", block, flags=re.IGNORECASE | re.DOTALL)
        if cat_match:
            category = clean(cat_match.group(1))
    if not call:
        title_match = re.search(r"<title[^>]*>([^<]+)</title>", html_text, flags=re.IGNORECASE)
        if title_match:
            text = clean(title_match.group(1))
            m = re.search(r"([A-Z0-9/]{3,})", text)
            if m:
                call = m.group(1).upper()
    return call, category


def parse_date(date_text: str) -> str:
    parts = re.split(r"[./-]", date_text.strip())
    if len(parts) == 3:
        dd, mm, yy = parts
        try:
            yy_int = int(yy)
            year = 2000 + yy_int if yy_int < 80 else 1900 + yy_int if yy_int < 100 else yy_int
            return f"{year:04d}-{int(mm):02d}-{int(dd):02d}"
        except ValueError:
            pass
    return date_text.strip() or ""


def parse_time_val(time_text: str) -> str:
    digits = re.sub(r"\\D", "", time_text)
    return digits.zfill(4)[:4] if digits else "0000"


def extract_band_khz(category: str | None) -> int:
    if not category:
        return 0
    m = re.search(r"(\\d+(?:\\.\\d+)?)\\s*mhz", category, flags=re.IGNORECASE)
    if m:
        try:
            mhz = float(m.group(1))
            return int(mhz * 1000)
        except ValueError:
            return 0
    m = re.search(r"(\\d+)\\s*ghz", category, flags=re.IGNORECASE)
    if m:
        try:
            ghz = float(m.group(1))
            return int(ghz * 1_000_000)
        except ValueError:
            return 0
    return 0


def parse_qsos(html_text: str, mycall: str, category: str | None) -> List[Tuple[int, str, str, str, str, str, str, str]]:
    table_match = re.search(r"<table>\\s*<thead>.*?</thead>\\s*<tbody>(.*?)</tbody>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not table_match:
        return []
    body = table_match.group(1)
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", body, flags=re.IGNORECASE | re.DOTALL)
    qsos: List[Tuple[int, str, str, str, str, str, str, str]] = []
    band_hint = extract_band_khz(category)
    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.IGNORECASE | re.DOTALL)
        if len(cells) < 9:
            continue
        fields = [clean(c) for c in cells]
        date_val, time_val, their_call, mode, rst_s, nr_s, rst_r, nr_r, wwl = (fields + [""] * 9)[:9]
        if not their_call:
            continue
        freq = band_hint if band_hint else 144000
        mode_out = "CW" if mode.upper().startswith("CW") else "PH"
        exch_s = f"{nr_s} {wwl}".strip()
        exch_r = f"{nr_r} {wwl}".strip()
        qsos.append(
            (
                freq,
                parse_date(date_val),
                parse_time_val(time_val),
                their_call.upper(),
                rst_s or "59",
                exch_s or "00",
                rst_r or "59",
                exch_r or "00",
            )
        )
    return qsos


def build_cabrillo(contest: Contest, call: str, category: str | None, qsos: Sequence[Tuple[int, str, str, str, str, str, str, str]]) -> str:
    lines = [
        "START-OF-LOG: 3.0",
        "CREATED-BY: vhfmanager-downloader",
        f"CONTEST: {contest.name}",
        f"CALLSIGN: {call}",
        f"CATEGORY: {category or ''}",
        "CATEGORY-OPERATOR: SINGLE-OP",
        "CATEGORY-ASSISTED: NON-ASSISTED",
        "CATEGORY-TRANSMITTER: ONE",
        "CATEGORY-STATION: FIXED",
    ]
    for freq, date, time_val, their_call, rst_s, exch_s, rst_r, exch_r in qsos:
        mode = "PH"
        lines.append(
            f"QSO: {freq:>5} {mode:<2} {date} {time_val:>4} "
            f"{call:<13} {rst_s:<3} {exch_s:<10} {their_call:<13} {rst_r:<3} {exch_r:<10}"
        )
    lines.append("END-OF-LOG:")
    return "\n".join(lines) + "\n"


def write_log(contest: Contest, call: str, cab: str) -> Path:
    safe_call = call.replace("/", "_")
    dest = OUTPUT_ROOT / f"{slugify(contest.name)}_{contest.cid}" / f"{safe_call}.log"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        return dest
    dest.write_text(cab, encoding="utf-8")
    return dest


def main() -> int:
    parser = argparse.ArgumentParser(description="Download VHFManager contest logs.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Max concurrent downloads.")
    parser.add_argument("--last-contests", type=int, default=None, help="Limit to most recent N contests (by ID).")
    parser.add_argument("--max-logs", type=int, default=None, help="Optional cap on logs per contest (testing).")
    args = parser.parse_args()

    contests = discover_contests(args.last_contests)
    if not contests:
        print("No contests found.")
        return 1

    all_links: List[Tuple[Contest, LogLink]] = []
    for contest in contests:
        try:
            contest, links = discover_logs(contest)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Failed to fetch contest {contest.cid}: {exc}")
            continue
        if args.max_logs:
            links = links[: args.max_logs]
        print(f"{contest.name} ({contest.cid}): {len(links)} logs")
        for link in links:
            all_links.append((contest, link))

    if not all_links:
        print("No logs to download.")
        return 1

    print(f"Total logs to fetch: {len(all_links)}")

    def worker(contest: Contest, link: LogLink) -> None:
        try:
            page = fetch_text(link.url)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Failed to fetch log {link.url}: {exc}")
            return
        call, category = parse_log_header(page)
        if not call:
            call = link.call_hint or f"log_{hash(link.url) & 0xFFFF}"
        qsos = parse_qsos(page, call, category)
        if not qsos:
            print(f"skip (no qsos): {call} ({contest.name})")
            return
        cab = build_cabrillo(contest, call, category, qsos)
        dest = write_log(contest, call, cab)
        print(f"ok   {dest}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(worker, contest, link) for contest, link in all_links]
        for fut in concurrent.futures.as_completed(futures):
            fut.result()

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
