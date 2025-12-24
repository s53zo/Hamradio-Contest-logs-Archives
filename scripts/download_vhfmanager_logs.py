#!/usr/bin/env python3
"""
Downloader for VHFManager contest logs (official/unofficial results).

Flow:
1) Discover contests by scanning VHFManager pages for results.php?ContestID=...
2) For each contest, collect display_log.php links on the results page.
3) Fetch every log page, extract Station/Category header and QSO table, and rebuild Cabrillo.
4) Optionally discover check logs by following per-QSO display_log links.

Output: VHF_MANAGER/<contest_folder>/<band>/<CALL>.log
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
    """
    Probe a descending range of ContestID values and pick those that contain log links.
    limit = number of most recent contests to return (by ID).
    """
    max_probe = 700  # generous upper bound for probing
    found: List[Contest] = []
    for cid in range(max_probe, 0, -1):
        url = f"{BASE_URL}/modules/results.php?ContestID={cid}&language=G"
        try:
            html_text = fetch_text(url)
        except Exception:
            continue
        if "display_log" not in html_text.lower():
            continue
        name = parse_contest_name(html_text, cid)
        if name.startswith("Contest_"):
            lower = html_text.lower()
            if "pmc contest" in lower:
                name = "PMC contest"
            elif "50 mhz" in lower:
                name = "ZRS 50 MHz tekmovanje"
            elif "maraton" in lower:
                name = "ZRS maraton 12 termin"
        found.append(Contest(cid=cid, name=name, results_url=url))
        if limit and len(found) >= limit:
            break
    return found


def parse_contest_name(html_text: str, cid: int) -> str:
    candidates: List[str] = []
    for pat in [r"<title[^>]*>([^<]+)</title>", r"<h[1-4][^>]*>([^<]+)</h[1-4]>"]:
        for m in re.finditer(pat, html_text, flags=re.IGNORECASE):
            text = clean(m.group(1))
            if text:
                candidates.append(text)
    if candidates:
        longest = max(candidates, key=len)
        # Avoid meaningless "Official results" titles
        if longest.lower().strip().startswith("official results"):
            candidates = [c for c in candidates if "results" not in c.lower()]
            if candidates:
                longest = max(candidates, key=len)
        return longest
    lower = html_text.lower()
    if "pmc contest" in lower:
        return "PMC contest"
    if "50 mhz" in lower:
        return "ZRS 50 MHz tekmovanje"
    if "70 mhz" in lower:
        return "ZRS 70 MHz tekmovanje"
    if "maraton" in lower:
        return "ZRS maraton 12 termin"
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
            r'href="([^"]*display_log[^"]*ContestID=\d+[^"]*logID=\d+[^"]*)"',
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
    locator = None
    summary_match = re.search(r'<dl[^>]*class="log_summary"[^>]*>(.*?)</dl>', html_text, flags=re.IGNORECASE | re.DOTALL)
    if summary_match:
        block = summary_match.group(1)
        call_match = re.search(r"<dt>\s*Station:.*?</dt>\s*<dd>(.*?)</dd>", block, flags=re.IGNORECASE | re.DOTALL)
        if call_match:
            call = clean(call_match.group(1)).upper()
        cat_match = re.search(r"<dt>\s*Category:.*?</dt>\s*<dd>(.*?)</dd>", block, flags=re.IGNORECASE | re.DOTALL)
        if cat_match:
            category = clean(cat_match.group(1))
        loc_match = re.search(r"<dt>\s*Locator:.*?</dt>\s*<dd>(.*?)</dd>", block, flags=re.IGNORECASE | re.DOTALL)
        if loc_match:
            locator = clean(loc_match.group(1)).upper()
    if not call:
        title_match = re.search(r"<title[^>]*>([^<]+)</title>", html_text, flags=re.IGNORECASE)
        if title_match:
            text = clean(title_match.group(1))
            m = re.search(r"([A-Z0-9/]{3,})", text)
            if m:
                call = m.group(1).upper()
    return call, category, locator


def normalize_log_url(url: str, base_url: str) -> str:
    abs_url = urllib.parse.urljoin(base_url, html.unescape(url))
    parsed = urllib.parse.urlsplit(abs_url)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))


def contest_id_from_url(url: str) -> Optional[int]:
    parsed = urllib.parse.urlsplit(url)
    q = urllib.parse.parse_qs(parsed.query)
    cid = (q.get("ContestID") or [None])[0]
    return int(cid) if cid and str(cid).isdigit() else None


def extract_checklog_links(html_text: str, base_url: str) -> List[LogLink]:
    table_match = re.search(
        r"<table>\s*<thead>.*?</thead>\s*<tbody>(.*?)</tbody>",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not table_match:
        return []
    body = table_match.group(1)
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", body, flags=re.IGNORECASE | re.DOTALL)
    found: dict[str, LogLink] = {}
    anchor_re = re.compile(
        r'<a[^>]+href=["\']([^"\']*display_log[^"\']*)["\'][^>]*>(.*?)</a>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for row in rows:
        match = anchor_re.search(row)
        if not match:
            continue
        href, anchor_text = match.groups()
        call_hint = clean(anchor_text).upper()
        if not re.match(r"^[A-Z0-9/]{3,}$", call_hint):
            continue
        if call_hint in found:
            continue
        url = normalize_log_url(href, base_url)
        found[call_hint] = LogLink(url=url, call_hint=call_hint, category_hint=None)
    return list(found.values())


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
    digits = re.sub(r"\D", "", time_text)
    return digits.zfill(4)[:4] if digits else "0000"


def extract_band_khz(category: str | None) -> int:
    if not category:
        return 0
    m = re.search(r"(\d+(?:\.\d+)?)\s*mhz", category, flags=re.IGNORECASE)
    if m:
        try:
            mhz = float(m.group(1))
            return int(mhz * 1000)
        except ValueError:
            return 0
    m = re.search(r"(\d+)\s*ghz", category, flags=re.IGNORECASE)
    if m:
        try:
            ghz = float(m.group(1))
            return int(ghz * 1_000_000)
        except ValueError:
            return 0
    return 0


def parse_qsos(
    html_text: str, mycall: str, category: str | None, station_locator: Optional[str]
) -> List[Tuple[int, str, str, str, str, str, str, str, int]]:
    table_match = re.search(r"<table>\s*<thead>.*?</thead>\s*<tbody>(.*?)</tbody>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not table_match:
        return []
    body = table_match.group(1)
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", body, flags=re.IGNORECASE | re.DOTALL)
    qsos: List[Tuple[int, str, str, str, str, str, str, str, int]] = []
    band_hint = extract_band_khz(category)
    if band_hint == 145000:
        band_hint = 144000
    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.IGNORECASE | re.DOTALL)
        if len(cells) < 9:
            continue
        fields = [clean(c) for c in cells]
        date_val, time_val, their_call, mode, rst_s, nr_s, rst_r, nr_r, wwl = (fields + [""] * 9)[:9]
        if not their_call:
            continue
        band_for_freq = band_hint if band_hint else 144000
        mode_upper = mode.upper()
        mode_out = "CW" if mode_upper.startswith("CW") else "PH"
        if band_for_freq == 144000:
            freq = 144100 if mode_out == "CW" else 144300
        else:
            freq = band_for_freq
        exch_s = f"{nr_s} {station_locator or ''}".strip()
        exch_r = f"{nr_r} {wwl}".strip() if wwl else (nr_r or "").strip()
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
                band_for_freq,
            )
        )
    return qsos


def build_cabrillo(
    contest: Contest, call: str, category: str | None, qsos: Sequence[Tuple[int, str, str, str, str, str, str, str, int]]
) -> str:
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
    for freq, date, time_val, their_call, rst_s, exch_s, rst_r, exch_r, _band in qsos:
        mode = "CW" if freq == 144100 else "PH"
        lines.append(
            f"QSO: {freq:>5} {mode:<2} {date} {time_val:>4} "
            f"{call:<13} {rst_s:<3} {exch_s:<10} {their_call:<13} {rst_r:<3} {exch_r:<10}"
        )
    lines.append("END-OF-LOG:")
    return "\n".join(lines) + "\n"


def derive_contest_dir(contest: Contest, qsos: Sequence[Tuple[int, str, str, str, str, str, str, str, int]]) -> str:
    month_map = {
        "januar": "January",
        "februar": "February",
        "marec": "March",
        "marcev": "March",
        "marčev": "March",
        "april": "April",
        "maj": "May",
        "junij": "June",
        "julij": "July",
        "avgust": "August",
        "september": "September",
        "oktober": "October",
        "november": "November",
        "december": "December",
        "oktobrsko": "October",
        "novembrsko": "November",
        "septembrsko": "September",
        "julijsko": "July",
    }
    name_lower = contest.name.lower()
    # strip boilerplate words
    for drop in ["official results", "unofficial results", "vhfmanager", "official", "unofficial", "results", " - "]:
        name_lower = name_lower.replace(drop, " ")
    base_name = " ".join(name_lower.split())
    month = None
    for key, eng in month_map.items():
        if key in base_name:
            month = eng
            break
    year = None
    for _, date_val, *_rest in qsos:
        if len(date_val) >= 4 and date_val[:4].isdigit():
            year = date_val[:4]
            break
    if not year:
        m = re.search(r"(20\\d{2}|19\\d{2})", contest.name)
        if m:
            year = m.group(1)
    if not year:
        # try any 2-digit year in dates to infer century (assume >= 90 => 1900s)
        for _, date_val, *_rest in qsos:
            m2 = re.search(r"(\\d{2})", date_val)
            if m2:
                yy = int(m2.group(1))
                year = str(1900 + yy if yy >= 90 else 2000 + yy)
                break
    # Maraton special case: group by year and termin if present
    if "maraton" in base_name:
        if not year:
            year = "unknown"
        termin = None
        m_term = re.search(r"(\d+)\s*\.?\s*termin", base_name)
        if m_term:
            termin = m_term.group(1)
        if termin:
            return f"ZRS_Maraton/{year}/Termin_{termin}"
        return f"ZRS_Maraton/{year}"
    # PMC contest label
    if "pmc contest" in base_name:
        if not year:
            year = "unknown"
        return f"PMC_{year}"
    # ZRS 50/70 MHz tekmovanje -> keep name, add year
    if "50 mhz" in base_name or "70 mhz" in base_name:
        if not year:
            year = "unknown"
        band_prefix = "50_MHz" if "50 mhz" in base_name else "70_MHz"
        return f"ZRS_{band_prefix}_tekmovanje_{year}"
    if month and year:
        return f"ZRS_{month}_{year}"
    return f"{slugify(base_name) or 'contest'}_{contest.cid}"


def band_label_from_qsos(qsos: Sequence[Tuple[int, str, str, str, str, str, str, str, int]]) -> str:
    bands = {band for *_rest, band in qsos if band}
    if not bands:
        return "unknown_band"
    band = sorted(bands)[0]
    return f"{int(round(band / 1000))}MHz"


def write_log(contest_dir: str, band_label: str, call: str, cab: str) -> Path:
    safe_call = call.replace("/", "_")
    dest = OUTPUT_ROOT / contest_dir / band_label / f"{safe_call}.log"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        return dest
    dest.write_text(cab, encoding="utf-8")
    return dest


def download_contest_logs(
    contest: Contest,
    seed_links: Sequence[LogLink],
    workers: int,
    max_logs: Optional[int],
    include_checklogs: bool = True,
) -> int:
    seen: set[str] = set()
    pending: List[LogLink] = []
    for link in seed_links:
        url = normalize_log_url(link.url, contest.results_url)
        if url in seen:
            continue
        if contest_id_from_url(url) != contest.cid:
            continue
        seen.add(url)
        pending.append(LogLink(url=url, call_hint=link.call_hint, category_hint=link.category_hint))

    def worker(link: LogLink) -> List[LogLink]:
        try:
            page = fetch_text(link.url)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Failed to fetch log {link.url}: {exc}")
            return []
        call, category, locator = parse_log_header(page)
        if not call:
            call = link.call_hint or f"log_{hash(link.url) & 0xFFFF}"
        qsos = parse_qsos(page, call, category, locator)
        if not qsos:
            print(f"skip (no qsos): {call} ({contest.name})")
            return []
        contest_dir = derive_contest_dir(contest, qsos)
        band_label = band_label_from_qsos(qsos)
        cab = build_cabrillo(contest, call, category, qsos)
        dest = write_log(contest_dir, band_label, call, cab)
        print(f"ok   {dest}")
        if not include_checklogs:
            return []
        new_links = extract_checklog_links(page, link.url)
        if call:
            new_links = [lnk for lnk in new_links if lnk.call_hint != call]
        return new_links

    if max_logs:
        pending = pending[:max_logs]
        seen = set(link.url for link in pending)

    downloaded = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures: dict[concurrent.futures.Future[List[LogLink]], LogLink] = {}
        while pending or futures:
            while pending and len(futures) < workers:
                link = pending.pop()
                futures[executor.submit(worker, link)] = link
            if not futures:
                break
            done, _ = concurrent.futures.wait(
                futures.keys(), return_when=concurrent.futures.FIRST_COMPLETED
            )
            for fut in done:
                _link = futures.pop(fut)
                try:
                    new_links = fut.result()
                except Exception as exc:  # pylint: disable=broad-except
                    print(f"Failed processing log {_link.url}: {exc}")
                    new_links = []
                downloaded += 1
                if not include_checklogs:
                    continue
                for new_link in new_links:
                    if max_logs and len(seen) >= max_logs:
                        break
                    url = normalize_log_url(new_link.url, contest.results_url)
                    if url in seen:
                        continue
                    if contest_id_from_url(url) != contest.cid:
                        continue
                    seen.add(url)
                    pending.append(LogLink(url=url, call_hint=new_link.call_hint, category_hint=None))
    return downloaded


def main() -> int:
    parser = argparse.ArgumentParser(description="Download VHFManager contest logs.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Max concurrent downloads.")
    parser.add_argument("--last-contests", type=int, default=None, help="Limit to most recent N contests (by ID).")
    parser.add_argument("--max-logs", type=int, default=None, help="Optional cap on logs per contest (testing).")
    parser.add_argument(
        "--no-checklogs",
        action="store_true",
        help="Skip discovery of check logs referenced from QSO rows.",
    )
    args = parser.parse_args()

    contests = discover_contests(args.last_contests)
    if not contests:
        print("No contests found.")
        return 1

    any_downloaded = False
    for contest in contests:
        try:
            contest, links = discover_logs(contest)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Failed to fetch contest {contest.cid}: {exc}")
            continue
        if not links:
            continue
        if args.max_logs:
            links = links[: args.max_logs]
        print(f"{contest.name} ({contest.cid}): {len(links)} seed logs")
        downloaded = download_contest_logs(
            contest,
            links,
            workers=args.workers,
            max_logs=args.max_logs,
            include_checklogs=not args.no_checklogs,
        )
        if downloaded:
            any_downloaded = True

    if not any_downloaded:
        print("No logs to download.")
        return 1

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
