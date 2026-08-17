#!/usr/bin/env python3
"""
Downloader for VHFManager contest logs (official/unofficial results).

Flow:
1) Discover contests by scanning VHFManager pages for results.php?ContestID=...
2) For each contest, collect display_log.php links on the results page.
3) Fetch every log page, extract Station/Category header and QSO table, and rebuild Cabrillo.
4) Optionally discover check logs by following per-QSO display_log links.

Output: EU_VHF_CONTESTS/<contest_folder>/<band>/<CALL>.log
PMC Output: WW_PMC/<year>/<CALL>.log
"""

from __future__ import annotations

import argparse
import concurrent.futures
import html
import os
import random
import re
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from archive_storage import ArchiveInventory, archive_log_exists, atomic_write_text
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
MAX_CONSECUTIVE_DISCOVERY_ERRORS = 3
MAX_CONTEST_ID = 700
BASE_URL = "https://vhfmanager.net"
OUTPUT_ROOT = Path("EU_VHF_CONTESTS")
CHECKLOG_STATE_ROOT = Path("state") / "providers" / "vhfmanager" / "checklogs"
TASK_LEDGER: "TaskLedger | None" = None
CHECKLOG_MARKER_LOCK = threading.RLock()
ARCHIVE_INVENTORY = ArchiveInventory()


HOST_COOLDOWN: dict[str, float] = {}
HOST_COOLDOWN_LOCK = threading.Lock()
WINDOWS_RESERVED_FILENAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}


def safe_filename_component(value: str) -> str:
    """Return a filename component that is portable to Windows."""
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", value).rstrip(" .")
    if not safe:
        return "_"
    if safe.split(".", 1)[0].upper() in WINDOWS_RESERVED_FILENAMES:
        safe = f"_{safe}"
    return safe


def is_dns_error(exc: Exception) -> bool:
    if isinstance(exc, socket.gaierror):
        return True
    if isinstance(exc, urllib.error.URLError) and isinstance(exc.reason, socket.gaierror):
        return True
    message = str(exc).lower()
    return "nodename nor servname" in message or "name or service not known" in message


def wait_for_host(host: str) -> None:
    if not host:
        return
    with HOST_COOLDOWN_LOCK:
        until = HOST_COOLDOWN.get(host, 0.0)
    now = time.time()
    if until > now:
        time.sleep(until - now)


def bump_host_cooldown(host: str, delay: float) -> None:
    if not host:
        return
    until = time.time() + delay
    with HOST_COOLDOWN_LOCK:
        current = HOST_COOLDOWN.get(host, 0.0)
        if until > current:
            HOST_COOLDOWN[host] = until


def empty_counts() -> Dict[str, int]:
    return {"ok": 0, "skip": 0, "error": 0}


def add_counts(target: Dict[str, int], delta: Dict[str, int]) -> None:
    for key, value in delta.items():
        target[key] = target.get(key, 0) + value


def request_headers(url: str) -> dict[str, str]:
    parsed = urllib.parse.urlsplit(url)
    q = urllib.parse.parse_qs(parsed.query)
    cid = (q.get("ContestID") or [None])[0]
    if cid and str(cid).isdigit():
        referer = f"{BASE_URL}/modules/results.php?ContestID={cid}&language=G"
    else:
        referer = BASE_URL
    return {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer,
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def fetch_text(url: str, retries: int = 3, delay: float = 1.0) -> str:
    """Fetch a URL and return decoded text."""
    last_exc: Exception | None = None
    host = urllib.parse.urlparse(url).hostname or ""
    for attempt in range(retries):
        wait_for_host(host)
        try:
            req = urllib.request.Request(url, headers=request_headers(url))
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                return resp.read().decode(charset, errors="ignore")
        except Exception as exc:  # pylint: disable=broad-except
            last_exc = exc
            if is_dns_error(exc):
                bump_host_cooldown(host, max(5.0, delay * (2 ** attempt) * 5.0))
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


def discover_contests(
    limit: int | None,
    recent_years: int | None = None,
) -> List[Contest]:
    """
    Probe a descending range of ContestID values and pick those that contain log links.
    limit = number of most recent contests to return (by ID).
    recent_years = include every contest from the newest N contest years.
    """
    found: List[Contest] = []
    newest_year: int | None = None
    consecutive_errors = 0
    probed = 0
    for cid in range(MAX_CONTEST_ID, 0, -1):
        url = f"{BASE_URL}/modules/results.php?ContestID={cid}&language=G"
        try:
            # A probe does not need the normal per-page retry policy. Repeated
            # transport failures indicate that the provider itself is down.
            html_text = fetch_text(url, retries=1)
        except Exception as exc:
            consecutive_errors += 1
            if consecutive_errors >= MAX_CONSECUTIVE_DISCOVERY_ERRORS:
                raise RuntimeError(
                    "VHFManager unavailable after "
                    f"{consecutive_errors} consecutive discovery requests: {exc}"
                ) from exc
            continue
        consecutive_errors = 0
        probed += 1
        if probed % 50 == 0:
            print(
                f"VHFManager discovery: checked {probed} contest IDs; "
                f"found {len(found)}"
            )
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
        contest = Contest(cid=cid, name=name, results_url=url)
        if recent_years:
            links = parse_log_links(contest, html_text)
            contest_year = contest_year_from_links(contest, links)
            if contest_year is not None:
                if newest_year is None:
                    newest_year = contest_year
                cutoff_year = newest_year - recent_years + 1
                if contest_year < cutoff_year:
                    break
        found.append(contest)
        if recent_years is None and limit and len(found) >= limit:
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
        lower = html_text.lower()
        if "pmc" in lower and "pmc" not in longest.lower():
            m = re.search(r"(WW\\s*PMC\\s*\\d{4})", html_text, flags=re.IGNORECASE)
            if m:
                return clean(m.group(1))
            return "WW PMC"
        return longest
    lower = html_text.lower()
    if "pmc contest" in lower:
        return "WW PMC"
    if "50 mhz" in lower:
        return "ZRS 50 MHz tekmovanje"
    if "70 mhz" in lower:
        return "ZRS 70 MHz tekmovanje"
    if "maraton" in lower:
        return "ZRS maraton 12 termin"
    return f"Contest_{cid}"


def parse_log_links(contest: Contest, html_text: str) -> List[LogLink]:
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
    return links


def discover_logs(contest: Contest) -> Tuple[Contest, List[LogLink]]:
    html_text = fetch_text(contest.results_url)
    contest = Contest(cid=contest.cid, name=parse_contest_name(html_text, contest.cid), results_url=contest.results_url)
    links = parse_log_links(contest, html_text)
    return contest, links


def parse_log_header(html_text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    call = None
    category = None
    locator = None
    pmc_designation = None
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
        pmc_match = re.search(r"<dt>\s*PMC-Designation:.*?</dt>\s*<dd>(.*?)</dd>", block, flags=re.IGNORECASE | re.DOTALL)
        if pmc_match:
            pmc_designation = clean(pmc_match.group(1)).upper()
    if not call:
        title_match = re.search(r"<title[^>]*>([^<]+)</title>", html_text, flags=re.IGNORECASE)
        if title_match:
            text = clean(title_match.group(1))
            m = re.search(r"([A-Z0-9/]{3,})", text)
            if m:
                call = m.group(1).upper()
    return call, category, locator, pmc_designation


def normalize_log_url(url: str, base_url: str) -> str:
    abs_url = urllib.parse.urljoin(base_url, html.unescape(url))
    parsed = urllib.parse.urlsplit(abs_url)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))


def contest_id_from_url(url: str) -> Optional[int]:
    parsed = urllib.parse.urlsplit(url)
    q = urllib.parse.parse_qs(parsed.query)
    cid = (q.get("ContestID") or [None])[0]
    return int(cid) if cid and str(cid).isdigit() else None


def log_id_from_url(url: str) -> Optional[int]:
    parsed = urllib.parse.urlsplit(url)
    q = urllib.parse.parse_qs(parsed.query)
    lid = (q.get("logID") or [None])[0]
    return int(lid) if lid and str(lid).isdigit() else None


def is_qso_view_url(url: str) -> bool:
    parsed = urllib.parse.urlsplit(url)
    q = urllib.parse.parse_qs(parsed.query)
    qso_id = (q.get("QSOID") or [None])[0]
    return bool(qso_id and str(qso_id).isdigit())


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
    normalized = category.replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)\s*mhz", normalized, flags=re.IGNORECASE)
    if m:
        try:
            mhz = float(m.group(1))
            return int(mhz * 1000)
        except ValueError:
            return 0
    m = re.search(r"(\d+(?:\.\d+)?)\s*ghz", normalized, flags=re.IGNORECASE)
    if m:
        try:
            ghz = float(m.group(1))
            return int(ghz * 1_000_000)
        except ValueError:
            return 0
    return 0


def parse_qsos_vhf(
    html_text: str, mycall: str, category: str | None, station_locator: Optional[str]
) -> List[Tuple[int, str, str, str, str, str, str, str, str, int]]:
    table_match = re.search(r"<table>\s*<thead>.*?</thead>\s*<tbody>(.*?)</tbody>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not table_match:
        return []
    body = table_match.group(1)
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", body, flags=re.IGNORECASE | re.DOTALL)
    qsos: List[Tuple[int, str, str, str, str, str, str, str, str, int]] = []
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
                mode_out,
                band_for_freq,
            )
        )
    return qsos


def parse_tables(html_text: str) -> List[Tuple[List[str], List[List[str]]]]:
    tables: List[Tuple[List[str], List[List[str]]]] = []
    for table_html in re.findall(r"<table[^>]*>(.*?)</table>", html_text, flags=re.IGNORECASE | re.DOTALL):
        head_match = re.search(r"<thead>(.*?)</thead>", table_html, flags=re.IGNORECASE | re.DOTALL)
        body_match = re.search(r"<tbody>(.*?)</tbody>", table_html, flags=re.IGNORECASE | re.DOTALL)
        header_cells: List[str] = []
        if head_match:
            head_rows = re.findall(r"<tr[^>]*>(.*?)</tr>", head_match.group(1), flags=re.IGNORECASE | re.DOTALL)
            if head_rows:
                header_cells = re.findall(r"<th[^>]*>(.*?)</th>", head_rows[-1], flags=re.IGNORECASE | re.DOTALL)
        if not header_cells:
            first_row = re.search(r"<tr[^>]*>(.*?)</tr>", table_html, flags=re.IGNORECASE | re.DOTALL)
            if first_row:
                header_cells = re.findall(r"<th[^>]*>(.*?)</th>", first_row.group(1), flags=re.IGNORECASE | re.DOTALL)
        headers = [clean(h) for h in header_cells] if header_cells else []
        rows: List[List[str]] = []
        if body_match:
            row_htmls = re.findall(r"<tr[^>]*>(.*?)</tr>", body_match.group(1), flags=re.IGNORECASE | re.DOTALL)
        else:
            row_htmls = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, flags=re.IGNORECASE | re.DOTALL)
            if header_cells and row_htmls:
                row_htmls = row_htmls[1:]
        for row_html in row_htmls:
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.IGNORECASE | re.DOTALL)
            if not cells:
                continue
            rows.append([clean(c) for c in cells])
        if headers or rows:
            tables.append((headers, rows))
    return tables


def parse_pmc_operators(html_text: str) -> Optional[str]:
    for headers, rows in parse_tables(html_text):
        if not headers:
            continue
        headers_lower = [h.lower() for h in headers]
        if "operators" not in headers_lower or "log callsign" not in headers_lower:
            continue
        op_i = headers_lower.index("operators")
        operators: List[str] = []
        for row in rows:
            if op_i < len(row) and row[op_i]:
                operators.append(row[op_i])
        if not operators:
            return None
        flat = ",".join(operators)
        parts = [p.strip() for p in re.split(r"[;,]+", flat) if p.strip()]
        seen: set[str] = set()
        unique: List[str] = []
        for part in parts:
            if part not in seen:
                seen.add(part)
                unique.append(part)
        return ", ".join(unique)
    return None


def parse_pmc_table(html_text: str) -> Tuple[List[str], List[List[str]]]:
    for headers, rows in parse_tables(html_text):
        if not headers:
            continue
        headers_lower = {h.lower() for h in headers}
        if {"date", "gmt", "callsign", "band", "mode"}.issubset(headers_lower):
            return headers, rows
    return [], []


def parse_qsos_pmc(
    html_text: str, category: str | None, pmc_designation: Optional[str]
) -> List[Tuple[int, str, str, str, str, str, str, str, str, int]]:
    headers, rows = parse_pmc_table(html_text)
    if not headers or not rows:
        return []

    def idx(name: str) -> Optional[int]:
        name_lower = name.lower()
        for i, header in enumerate(headers):
            if header.lower() == name_lower:
                return i
        return None

    date_i = idx("Date")
    time_i = idx("GMT")
    call_i = idx("Callsign")
    band_i = idx("Band")
    mode_i = idx("Mode")
    rsts_i = idx("RSTs")
    nrs_i = idx("NRs")
    rstr_i = idx("RSTr")
    nrr_i = idx("NRr")
    pmc_i = idx("PMC")
    if None in (date_i, time_i, call_i, band_i, mode_i, rsts_i, rstr_i):
        return []
    qsos: List[Tuple[int, str, str, str, str, str, str, str, str, int]] = []
    for row in rows:
        if len(row) <= max(date_i, time_i, call_i, band_i, mode_i, rsts_i, rstr_i):
            continue
        date_val = row[date_i]
        time_val = row[time_i]
        their_call = row[call_i].upper()
        band_text = row[band_i]
        mode = row[mode_i]
        rst_s = row[rsts_i] if rsts_i is not None and rsts_i < len(row) else ""
        nr_s = row[nrs_i] if nrs_i is not None and nrs_i < len(row) else ""
        rst_r = row[rstr_i] if rstr_i is not None and rstr_i < len(row) else ""
        nr_r = row[nrr_i] if nrr_i is not None and nrr_i < len(row) else ""
        pmc_r = row[pmc_i] if pmc_i is not None and pmc_i < len(row) else ""
        if not their_call:
            continue
        band_for_freq = extract_band_khz(band_text or category)
        if not band_for_freq:
            continue
        mode_upper = mode.upper()
        mode_out = "CW" if mode_upper.startswith("CW") else "PH"
        freq = band_for_freq
        exch_s = " ".join(part for part in [nr_s, pmc_designation] if part).strip()
        exch_r = " ".join(part for part in [nr_r, pmc_r] if part).strip()
        qsos.append(
            (
                freq,
                parse_date(date_val),
                parse_time_val(time_val),
                their_call,
                rst_s or "59",
                exch_s or "00",
                rst_r or "59",
                exch_r or "00",
                mode_out,
                band_for_freq,
            )
        )
    return qsos


def parse_qsos(
    html_text: str,
    contest: Contest,
    mycall: str,
    category: str | None,
    station_locator: Optional[str],
    pmc_designation: Optional[str],
) -> List[Tuple[int, str, str, str, str, str, str, str, str, int]]:
    if pmc_designation or "pmc" in contest.name.lower():
        return parse_qsos_pmc(html_text, category, pmc_designation)
    return parse_qsos_vhf(html_text, mycall, category, station_locator)


def contest_year_from_links(
    contest: Contest,
    links: Sequence[LogLink],
    attempts: int = 3,
) -> Optional[int]:
    for link in links[:attempts]:
        try:
            page = fetch_text(link.url)
        except Exception:  # pylint: disable=broad-except
            continue
        call, category, locator, pmc_designation = parse_log_header(page)
        qsos = parse_qsos(
            page,
            contest,
            call or link.call_hint or "",
            category,
            locator,
            pmc_designation,
        )
        for _freq, date_value, *_rest in qsos:
            if re.fullmatch(r"(?:19|20)\d{2}-\d{2}-\d{2}", date_value):
                return int(date_value[:4])
    return None


def cabrillo_contest_name(contest: Contest, force_pmc: bool) -> str:
    return "WW_PMC" if force_pmc or "pmc" in contest.name.lower() else contest.name


def build_cabrillo(
    contest: Contest,
    call: str,
    category: str | None,
    qsos: Sequence[Tuple[int, str, str, str, str, str, str, str, str, int]],
    force_pmc: bool,
    operators: Optional[str],
) -> str:
    lines = [
        "START-OF-LOG: 3.0",
        "CREATED-BY: vhfmanager-downloader",
        f"CONTEST: {cabrillo_contest_name(contest, force_pmc)}",
        f"CALLSIGN: {call}",
    ]
    if operators:
        lines.append(f"OPERATORS: {operators}")
    lines += [
        f"CATEGORY: {category or ''}",
        "CATEGORY-OPERATOR: SINGLE-OP",
        "CATEGORY-ASSISTED: NON-ASSISTED",
        "CATEGORY-TRANSMITTER: ONE",
        "CATEGORY-STATION: FIXED",
    ]
    for freq, date, time_val, their_call, rst_s, exch_s, rst_r, exch_r, mode, _band in qsos:
        lines.append(
            f"QSO: {freq:>5} {mode:<2} {date} {time_val:>4} "
            f"{call:<13} {rst_s:<3} {exch_s:<10} {their_call:<13} {rst_r:<3} {exch_r:<10}"
        )
    lines.append("END-OF-LOG:")
    return "\n".join(lines) + "\n"


def derive_contest_dir(
    contest: Contest,
    qsos: Sequence[Tuple[int, str, str, str, str, str, str, str, str, int]],
    force_pmc: bool,
) -> str:
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
    if force_pmc:
        if not year:
            year = "unknown"
        return f"{year}"
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
    if "pmc" in base_name:
        if not year:
            year = "unknown"
        return f"{year}"
    # ZRS 50/70 MHz tekmovanje -> keep name, add year
    if "50 mhz" in base_name or "70 mhz" in base_name:
        if not year:
            year = "unknown"
        band_prefix = "50_MHz" if "50 mhz" in base_name else "70_MHz"
        return f"ZRS_{band_prefix}_tekmovanje_{year}"
    if month and year:
        return f"ZRS_{month}_{year}"
    return f"{slugify(base_name) or 'contest'}_{contest.cid}"


def band_label_from_qsos(qsos: Sequence[Tuple[int, str, str, str, str, str, str, str, str, int]]) -> str:
    bands = {band for *_rest, band in qsos if band}
    if not bands:
        return "unknown_band"
    band = sorted(bands)[0]
    return f"{int(round(band / 1000))}MHz"


def is_pmc_contest(contest: Contest) -> bool:
    return "pmc" in contest.name.lower()


def contest_output_root(is_pmc: bool) -> Path:
    return Path("WW_PMC") if is_pmc else OUTPUT_ROOT


def pmc_file_tag(category: str | None) -> Optional[str]:
    if not category:
        return None
    lower = category.lower()
    if "all bands" in lower or lower.startswith("all -"):
        return "ALL"
    band = extract_band_khz(category)
    if band:
        return f"{int(round(band / 1000))}MHz"
    return None


def write_log(
    output_root: Path,
    contest_dir: str,
    band_label: Optional[str],
    call: str,
    cab: str,
    file_tag: Optional[str],
) -> Tuple[Path, str]:
    safe_call = safe_filename_component(call)
    safe_tag = re.sub(r"[^A-Za-z0-9_.-]+", "_", file_tag or "").strip("._")
    file_stem = f"{safe_call}_{safe_tag}" if safe_tag else safe_call
    if band_label:
        dest = output_root / contest_dir / band_label / f"{file_stem}.log"
    else:
        dest = output_root / contest_dir / f"{file_stem}.log"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if archive_log_exists(dest):
        return dest, "skip"
    atomic_write_text(dest, cab)
    return dest, "ok"


def legacy_checklog_marker_path(contest: Contest, log_id: int) -> Path:
    return OUTPUT_ROOT / ".checklogs" / str(contest.cid) / f"{log_id}.done"


def checklog_marker_path(contest: Contest, log_id: int) -> Path:
    return CHECKLOG_STATE_ROOT / str(contest.cid) / f"{log_id}.done"


def migrate_legacy_checklog_markers(repo_root: Path | None = None) -> int:
    root = Path.cwd() if repo_root is None else repo_root
    legacy_root = root / OUTPUT_ROOT / ".checklogs"
    state_root = root / CHECKLOG_STATE_ROOT
    legacy_entries: dict[Path, Path | None] = {}
    if legacy_root.is_dir():
        for path in legacy_root.glob("*/*.done"):
            try:
                relative = path.relative_to(root)
            except ValueError:
                relative = None
            legacy_entries[path] = relative
    git_repo = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=root,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    ).stdout.strip() == "true"
    tracked_paths: set[Path] = set()
    try:
        tracked_prefix = legacy_root.relative_to(root)
    except ValueError:
        tracked_prefix = None
    if git_repo and tracked_prefix is not None:
        inventory = ArchiveInventory(root)
        tracked_paths = set(
            inventory.git_paths(tracked_prefix, log_only=False)
        )
        for relative in tracked_paths:
            legacy_entries.setdefault(root / relative, relative)
    if not legacy_entries:
        return 0
    migrated = 0
    tracked_to_unskip: List[Path] = []
    with CHECKLOG_MARKER_LOCK:
        for legacy, relative in sorted(
            legacy_entries.items(), key=lambda item: item[0].as_posix()
        ):
            try:
                contest_id = int(legacy.parent.name)
                log_id = int(legacy.stem)
            except ValueError:
                continue
            target = state_root / str(contest_id) / f"{log_id}.done"
            if not target.exists():
                atomic_write_text(target, "ok\n")
            if legacy.exists():
                legacy.unlink()
            if relative is not None and relative in tracked_paths:
                tracked_to_unskip.append(relative)
            migrated += 1
        if tracked_to_unskip:
            path_input = b"\0".join(
                path.as_posix().encode("utf-8", errors="surrogateescape")
                for path in tracked_to_unskip
            ) + b"\0"
            subprocess.run(
                ["git", "update-index", "--no-skip-worktree", "-z", "--stdin"],
                cwd=root,
                check=True,
                input=path_input,
                stdout=subprocess.DEVNULL,
            )
        for directory in sorted(
            (path for path in legacy_root.rglob("*") if path.is_dir()),
            key=lambda path: len(path.parts),
            reverse=True,
        ):
            try:
                directory.rmdir()
            except OSError:
                pass
        try:
            legacy_root.rmdir()
        except OSError:
            pass
    if migrated:
        print(f"VHFManager: migrated {migrated} checklog markers to {state_root}")
    return migrated


def checklog_marker_exists(contest: Contest, log_id: Optional[int]) -> bool:
    if log_id is None:
        return False
    with CHECKLOG_MARKER_LOCK:
        marker = checklog_marker_path(contest, log_id)
        if marker.exists():
            return True
        legacy = legacy_checklog_marker_path(contest, log_id)
        if not legacy.exists():
            return False
        atomic_write_text(marker, "ok\n")
        legacy.unlink()
        return True


def write_checklog_marker(contest: Contest, log_id: Optional[int]) -> None:
    if log_id is None:
        return
    with CHECKLOG_MARKER_LOCK:
        marker = checklog_marker_path(contest, log_id)
        if not marker.exists():
            atomic_write_text(marker, "ok\n")
        legacy = legacy_checklog_marker_path(contest, log_id)
        if legacy.exists():
            legacy.unlink()


def download_contest_logs(
    contest: Contest,
    seed_links: Sequence[LogLink],
    workers: int,
    max_logs: Optional[int],
    include_checklogs: bool = True,
) -> Dict[str, int]:
    seen: set[str] = set()
    pending: List[LogLink] = []
    seed_urls: set[str] = set()
    skipped_seed = 0
    for link in seed_links:
        url = normalize_log_url(link.url, contest.results_url)
        log_id = log_id_from_url(url)
        if checklog_marker_exists(contest, log_id):
            skipped_seed += 1
            continue
        if url in seen:
            continue
        if contest_id_from_url(url) != contest.cid:
            continue
        seen.add(url)
        seed_urls.add(url)
        pending.append(LogLink(url=url, call_hint=link.call_hint, category_hint=link.category_hint))

    def worker(link: LogLink) -> Tuple[Dict[str, int], List[LogLink]]:
        is_seed = link.url in seed_urls
        log_id = log_id_from_url(link.url)
        if is_pmc_contest(contest) and is_qso_view_url(link.url):
            print(f"skip (pmc qso view): log_id={log_id}")
            write_checklog_marker(contest, log_id)
            return {"skip": 1}, []
        try:
            page = fetch_text(link.url)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Failed to fetch log_id={log_id}: {exc}")
            return {"error": 1}, []
        call, category, locator, pmc_designation = parse_log_header(page)
        if not call:
            call = link.call_hint or f"log_{hash(link.url) & 0xFFFF}"
        qsos = parse_qsos(page, contest, call, category, locator, pmc_designation)
        if not qsos:
            print(f"skip (no qsos): {call} ({contest.name})")
            write_checklog_marker(contest, log_id)
            return {"skip": 1}, []
        is_pmc = is_pmc_contest(contest) or bool(pmc_designation)
        operators = parse_pmc_operators(page) if is_pmc else None
        contest_dir = derive_contest_dir(contest, qsos, is_pmc)
        band_label = None if is_pmc else band_label_from_qsos(qsos)
        cab = build_cabrillo(contest, call, category, qsos, is_pmc, operators)
        output_root = contest_output_root(is_pmc)
        file_tag = pmc_file_tag(category) if is_pmc else None
        dest, status = write_log(output_root, contest_dir, band_label, call, cab, file_tag)
        print(f"{'ok  ' if status == 'ok' else 'skip'} {dest}")
        write_checklog_marker(contest, log_id)
        if not include_checklogs:
            return {status: 1}, []
        new_links = extract_checklog_links(page, link.url)
        if call:
            new_links = [lnk for lnk in new_links if lnk.call_hint != call]
        return {status: 1}, new_links

    if max_logs:
        pending = pending[:max_logs]
        seen = set(link.url for link in pending)

    totals = empty_counts()
    totals["skip"] += skipped_seed
    totals["skip_seed"] = skipped_seed
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures: dict[concurrent.futures.Future[Tuple[Dict[str, int], List[LogLink]]], LogLink] = {}
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
                    counts, new_links = fut.result()
                except Exception as exc:  # pylint: disable=broad-except
                    print(f"Failed processing log_id={log_id_from_url(_link.url)}: {exc}")
                    counts = {"error": 1}
                    new_links = []
                add_counts(totals, counts)
                if not include_checklogs:
                    continue
                for new_link in new_links:
                    if max_logs and len(seen) >= max_logs:
                        break
                    url = normalize_log_url(new_link.url, contest.results_url)
                    log_id = log_id_from_url(url)
                    if checklog_marker_exists(contest, log_id):
                        continue
                    if url in seen:
                        continue
                    if contest_id_from_url(url) != contest.cid:
                        continue
                    seen.add(url)
                    pending.append(LogLink(url=url, call_hint=new_link.call_hint, category_hint=None))
    return totals


def main() -> int:
    parser = argparse.ArgumentParser(description="Download VHFManager contest logs.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Max concurrent downloads.")
    parser.add_argument("--last-contests", type=int, default=None, help="Limit to most recent N contests (by ID).")
    parser.add_argument("--max-logs", type=int, default=None, help="Optional cap on logs per contest (testing).")
    parser.add_argument("--only-pmc", action="store_true", help="Only download PMC contests.")
    parser.add_argument(
        "--no-checklogs",
        action="store_true",
        help="Skip discovery of check logs referenced from QSO rows.",
    )
    parser.add_argument(
        "--seed-sweep",
        action="store_true",
        help="Mark seed log_ids as processed when output files already exist (fast sweep).",
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

    migrate_legacy_checklog_markers()
    contests = discover_contests(args.last_contests)
    if not contests:
        print("No contests found.")
        return 1

    any_downloaded = False
    total_skipped_seed = 0
    for contest in contests:
        try:
            contest, links = discover_logs(contest)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Failed to fetch contest {contest.cid}: {exc}")
            continue
        if args.only_pmc and not is_pmc_contest(contest):
            continue
        if not links:
            continue
        if args.max_logs:
            links = links[: args.max_logs]
        print(f"{contest.name} ({contest.cid}): {len(links)} seed logs")

        if args.seed_sweep:
            sweep_marked = 0
            for link in links:
                url = normalize_log_url(link.url, contest.results_url)
                log_id = log_id_from_url(url)
                if log_id is None:
                    continue
                if checklog_marker_exists(contest, log_id):
                    continue
                if link.call_hint:
                    safe_call = safe_filename_component(link.call_hint)
                    if is_pmc_contest(contest):
                        years = sorted({int(y) for y in re.findall(r"(?:19|20)\\d{2}", contest.name)})
                        if not years:
                            continue
                        dest = None
                        for year in years:
                            base = contest_output_root(True) / str(year)
                            matches = ARCHIVE_INVENTORY.logs_for_callsign(safe_call, base)
                            if matches:
                                dest = ARCHIVE_INVENTORY.repo_root / matches[0]
                                break
                    else:
                        contest_dir = derive_contest_dir(contest, [], False)
                        base = OUTPUT_ROOT / contest_dir
                        matches = ARCHIVE_INVENTORY.logs_for_callsign(safe_call, base)
                        dest = ARCHIVE_INVENTORY.repo_root / matches[0] if matches else None
                        if dest is None:
                            continue
                    write_checklog_marker(contest, log_id)
                    sweep_marked += 1
            if sweep_marked:
                print(f"  seed sweep marked {sweep_marked} log_ids")
            # Continue to normal download flow after sweep

        include_checklogs = (not args.no_checklogs) and (not is_pmc_contest(contest))
        task_key = f"{Path(__file__).stem}:{contest.cid}:checklogs={int(include_checklogs)}"
        normalized_links = [normalize_log_url(lnk.url, contest.results_url) for lnk in links]
        skip, list_hash, item_count = task_should_skip(
            TASK_LEDGER, task_key, normalized_links
        )
        if skip:
            print(f"skip (task ledger): {contest.name} ({contest.cid}) items={item_count}")
            continue
        totals = download_contest_logs(
            contest,
            links,
            workers=args.workers,
            max_logs=args.max_logs,
            include_checklogs=include_checklogs,
        )
        if totals.get("error", 0) == 0:
            task_mark_complete(TASK_LEDGER, task_key, list_hash, item_count)
        if totals.get("ok", 0):
            any_downloaded = True
        total_skipped_seed += max(0, totals.get("skip_seed", 0))

    if not any_downloaded:
        print("No logs to download.")
        return 1
    if total_skipped_seed:
        print(f"INFO: {total_skipped_seed} seed logs skipped (checklogs already processed)")

    print("Done.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        if TASK_LEDGER is not None:
            TASK_LEDGER.close()
