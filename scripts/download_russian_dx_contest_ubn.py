#!/usr/bin/env python3
"""
Download UA9QCQ Russian DX Contest UBN logs for selected dates.

Writes all logs into RussianDXContest/<year>/.
Requires UA9QCQ_COOKIE env var for authenticated UBN access.
"""

from __future__ import annotations

import argparse
import getpass
import os
import re
import socket
import sys
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Dict, List, Sequence, Tuple

from archive_storage import archive_log_exists, atomic_write_text
from task_ledger import TASK_LEDGER_PATH, TaskLedger, task_mark_complete, task_should_skip

BASE_URL = "https://ua9qcq.com"
RESULTS_URL = f"{BASE_URL}/results_new.php"
UBN_URL = f"{BASE_URL}/ubnlog.php"
MORE_INFO_URL = f"{BASE_URL}/more_stn_info.php"

TEST_ID = "25"  # Russian DX Contest
REQUEST_TIMEOUT = 30
MAX_RESPONSE_SECONDS = 120
READ_TIMEOUT_SECONDS = 5
READ_CHUNK_SIZE = 65536
HTML_END_MARKER = b"</html>"
HTML_TAIL_BYTES = 16384
DEFAULT_TERR_ID = "76"  # World Wide
TASK_LEDGER: "TaskLedger | None" = None
PROGRESS_LABEL = "Russian DX Contest"
DEFAULT_MODE = "MIXED"
DEFAULT_BAND = "ALL"
DEFAULT_MAX_DATE_SECONDS = 900
DEFAULT_MAX_CONSECUTIVE_ERRORS = 50


def set_response_socket_timeout(resp: urllib.request.addinfourl, timeout: float) -> None:
    sock = None
    raw = getattr(getattr(resp, "fp", None), "raw", None)
    if raw is not None:
        sock = getattr(raw, "_sock", None) or getattr(raw, "sock", None)
    if sock is None:
        sock = getattr(getattr(resp, "fp", None), "_sock", None)
    if sock is not None:
        sock.settimeout(timeout)


def read_response_text(resp: urllib.request.addinfourl) -> str:
    start = time.time()
    chunks: List[bytes] = []
    tail = b""
    set_response_socket_timeout(resp, READ_TIMEOUT_SECONDS)
    while True:
        if time.time() - start > MAX_RESPONSE_SECONDS:
            raise TimeoutError("UA9QCQ response read timed out")
        try:
            chunk = resp.read(READ_CHUNK_SIZE)
        except socket.timeout:
            continue
        if not chunk:
            break
        chunks.append(chunk)
        tail = (tail + chunk)[-HTML_TAIL_BYTES:]
        if HTML_END_MARKER in tail.lower():
            break
    charset = resp.headers.get_content_charset() or "utf-8"
    return b"".join(chunks).decode(charset, errors="ignore")


def fetch_text(url: str, data: Dict[str, str] | None = None) -> str:
    payload = None
    if data is not None:
        payload = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, data=payload)
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return read_response_text(resp)


def fetch_text_with_cookie(url: str, data: Dict[str, str], cookie: str) -> str:
    payload = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Cookie": cookie})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return read_response_text(resp)


class SelectParser(HTMLParser):
    def __init__(self, target_name: str) -> None:
        super().__init__()
        self.target_name = target_name
        self.in_select = False
        self.options: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str | None]]) -> None:
        if tag.lower() == "select":
            attrs_dict = dict(attrs)
            if attrs_dict.get("name") == self.target_name:
                self.in_select = True
        if tag.lower() == "option" and self.in_select:
            attrs_dict = dict(attrs)
            value = attrs_dict.get("value")
            if value:
                self.options.append(value)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "select" and self.in_select:
            self.in_select = False


class MoreInfoFormParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_form = False
        self.form_action = ""
        self.inputs: Dict[str, str] = {}
        self.entries: List[Dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str | None]]) -> None:
        attrs_dict = dict(attrs)
        if tag.lower() == "form":
            self.in_form = True
            self.form_action = attrs_dict.get("action", "") or ""
            self.inputs = {}
        if self.in_form and tag.lower() == "input":
            name = attrs_dict.get("name")
            value = attrs_dict.get("value")
            if name and value is not None:
                self.inputs[name] = value

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "form" and self.in_form:
            if "more_stn_info.php" in self.form_action:
                self.entries.append(self.inputs)
            self.in_form = False
            self.form_action = ""
            self.inputs = {}


class RowCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.table_depth = 0
        self.row_stack: List[Dict[str, object]] = []
        self.rows: List[Tuple[int | None, List[str]]] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str | None]]) -> None:
        if tag.lower() == "table":
            self.table_depth += 1
        if tag.lower() == "tr":
            self.row_stack.append(
                {
                    "depth": self.table_depth,
                    "cells": [],
                    "in_cell": False,
                    "cell": [],
                }
            )
        if tag.lower() in ("td", "th") and self.row_stack:
            self.row_stack[-1]["in_cell"] = True
            self.row_stack[-1]["cell"] = []

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "table":
            self.table_depth -= 1
        if tag.lower() in ("td", "th") and self.row_stack:
            if self.row_stack[-1].get("in_cell"):
                cell_text = self.row_stack[-1]["cell"]
                if not isinstance(cell_text, list):
                    cell_text = []
                text = unescape("".join(cell_text))
                text = " ".join(text.split())
                cells = self.row_stack[-1]["cells"]
                if isinstance(cells, list):
                    cells.append(text)
                self.row_stack[-1]["in_cell"] = False
                self.row_stack[-1]["cell"] = []
        if tag.lower() == "tr" and self.row_stack:
            row = self.row_stack.pop()
            depth = row.get("depth")
            cells = row.get("cells")
            if isinstance(cells, list):
                self.rows.append((int(depth) if depth is not None else None, cells))

    def handle_data(self, data: str) -> None:
        for row in self.row_stack:
            if row.get("in_cell"):
                cell_text = row.get("cell")
                if isinstance(cell_text, list):
                    cell_text.append(data)


class TextCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: List[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)



def normalized_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text.lower())


INFO_KEY_MAP = {
    "callsign": "callsign",
    "category": "category",
    "station": "station",
    "operator": "operators",
    "operators": "operators",
    "club": "club",
    "name": "name",
    "power": "power",
    "city": "city",
    "qth": "qth",
    "location": "location",
    "district": "district",
    "region": "region",
    "oblast": "region",
    "province": "region",
    "state": "region",
    "country": "country",
    "locator": "grid",
    "grid": "grid",
    "rda": "rda",
    "email": "email",
    "address": "address",
    "address1": "address",
    "address2": "address2",
    "claimedscore": "claimed_score",
    "totalscore": "claimed_score",
    "score": "claimed_score",
    "band": "band",
    "mode": "mode",
    "assisted": "assisted",
    "transmitter": "transmitter",
    "cqzone": "cq_zone",
    "ituzone": "itu_zone",
    "arrlsection": "arrl_section",
    "dxcc": "dxcc",
    "continent": "continent",
    "overlay": "overlay",
}


def normalize_info_key(text: str) -> str | None:
    key = normalized_key(text.rstrip(":"))
    return INFO_KEY_MAP.get(key)


def normalize_mode_text(text: str, default_mode: str) -> str:
    upper = text.upper()
    if 'MIXED' in upper:
        return 'MIXED'
    if 'CW' in upper and ('SSB' in upper or 'PHONE' in upper):
        return 'MIXED'
    if 'SSB' in upper or 'PHONE' in upper:
        return 'SSB'
    if 'RTTY' in upper or 'DIGI' in upper or 'DIGITAL' in upper:
        return 'DIGITAL'
    if 'CW' in upper:
        return 'CW'
    return default_mode


def normalize_band_text(text: str) -> str:
    upper = text.upper()
    if "ALL" in upper:
        return "ALL"
    for band in ("160", "80", "40", "20", "15", "10", "6", "2"):
        if band in upper:
            return f"{band}M"
    return ""


def normalize_assisted_text(text: str) -> str:
    upper = text.upper()
    if 'UNASSISTED' in upper or 'NON-ASSISTED' in upper or 'NONASSISTED' in upper:
        return 'NON-ASSISTED'
    if 'ASSISTED' in upper:
        return 'ASSISTED'
    return ''

def parse_more_info_fields(html_text: str) -> Dict[str, str]:
    if "Login:" in html_text:
        return {}
    collector = RowCollector()
    collector.feed(html_text)
    meta: Dict[str, str] = {}
    for _, cells in collector.rows:
        if len(cells) < 2:
            continue
        label = cells[0].strip()
        value = cells[1].strip()
        if not label or not value:
            continue
        key = normalize_info_key(label)
        if not key:
            continue
        if key in meta and meta[key] != value:
            meta[key] = f"{meta[key]} / {value}"
        else:
            meta[key] = value
    return meta


def split_exchange(text: str, default_rst: str = "599", default_exch: str = "0000") -> Tuple[str, str]:
    tokens = text.split()
    if not tokens:
        return default_rst, default_exch
    rst = tokens[0]
    exch = " ".join(tokens[1:]) if len(tokens) > 1 else default_exch
    return rst, exch


def confirmed_qsos(
    html_text: str, start_time: int, include_errors: bool
) -> List[Tuple[int, str, str, str, str, str, str, str, str]]:
    collector = RowCollector()
    collector.feed(html_text)

    required = [
        "freq",
        "mode",
        "date",
        "time",
        "calltx",
        "exchangetx",
        "callrx",
        "exchangerx",
        "error",
    ]
    header_depth: int | None = None
    header: List[str] | None = None
    col: Dict[str, int] | None = None
    for depth, cells in collector.rows:
        if not cells or not any(cell.lower() == "freq" for cell in cells):
            continue
        candidate = {normalized_key(name): i for i, name in enumerate(cells)}
        if all(key in candidate for key in required):
            header_depth = depth
            header = cells
            col = candidate
            break

    if header_depth is None or header is None or col is None:
        return []

    qsos: List[Tuple[int, str, str, str, str, str, str, str, str]] = []
    for depth, cells in collector.rows:
        if depth != header_depth:
            continue
        if not cells:
            continue
        freq_val = re.sub(r"[^0-9]", "", cells[col["freq"]])
        if len(freq_val) not in (4, 5):
            continue
        if len(cells) < len(header):
            cells = cells + [""] * (len(header) - len(cells))
        time_val = re.sub(r"[^0-9]", "", cells[col["time"]])
        if len(time_val) != 4:
            continue
        if int(time_val) < start_time:
            continue
        freq = freq_val
        mode = cells[col["mode"]].upper()
        date = cells[col["date"]]
        exch_tx = cells[col["exchangetx"]]
        call_rx = cells[col["callrx"]].upper()
        exch_rx = cells[col["exchangerx"]]
        rst_s, exch_s = split_exchange(exch_tx)
        rst_r, exch_r = split_exchange(exch_rx)
        qsos.append(
            (
                int(freq),
                mode,
                date,
                time_val,
                call_rx,
                rst_s,
                exch_s,
                rst_r,
                exch_r,
            )
        )
    return qsos


def parse_category_text(html_text: str) -> str | None:
    collector = TextCollector()
    collector.feed(html_text)
    tokens = [unescape(t).strip() for t in collector.parts]
    tokens = [t for t in tokens if t]
    for idx, token in enumerate(tokens):
        if token.rstrip(":").lower() == "category" and idx + 1 < len(tokens):
            return tokens[idx + 1]
    return None


def parse_operator_text(html_text: str) -> str | None:
    collector = TextCollector()
    collector.feed(html_text)
    tokens = [unescape(t).strip() for t in collector.parts]
    tokens = [t for t in tokens if t]
    for idx, token in enumerate(tokens):
        token_key = token.rstrip(":").lower()
        if token_key.startswith("operator") and idx + 1 < len(tokens):
            return tokens[idx + 1]
    return None


def derive_category_fields(
    category_text: str | None, meta: Dict[str, str] | None = None
) -> Dict[str, str]:
    category = (category_text or '').upper()
    operator = 'SINGLE-OP'
    transmitter = 'ONE'
    mode = DEFAULT_MODE
    assisted = ''
    power = ''
    band = DEFAULT_BAND

    if meta:
        if meta.get('power'):
            power = meta['power'].strip().upper()
        if meta.get('mode'):
            mode = normalize_mode_text(meta['mode'], mode)
        if meta.get('band'):
            band_hint = normalize_band_text(meta['band'])
            if band_hint:
                band = band_hint
        if meta.get('assisted'):
            assisted = normalize_assisted_text(meta['assisted'])
        if meta.get('transmitter'):
            transmitter = meta['transmitter'].strip().upper()

    if 'CHECKLOG' in category:
        operator = 'CHECKLOG'
    elif 'MULTI' in category:
        operator = 'MULTI-OP'
        if 'MULTI-ONE' in category or 'MULTI ONE' in category:
            transmitter = 'ONE'
        elif 'MULTI-TWO' in category or 'MULTI TWO' in category:
            transmitter = 'TWO'
        elif 'MULTI-MULTI' in category or 'MULTI MULTI' in category:
            transmitter = 'MULTI'
    else:
        operator = 'SINGLE-OP'

    if not assisted:
        if 'UNASSISTED' in category or 'NON-ASSISTED' in category:
            assisted = 'NON-ASSISTED'
        elif 'ASSISTED' in category:
            assisted = 'ASSISTED'

    if 'MIXED' in category:
        mode = 'MIXED'
    elif 'SSB' in category or 'PHONE' in category:
        mode = 'SSB'
    elif 'RTTY' in category or 'DIGI' in category or 'DIGITAL' in category:
        mode = 'DIGITAL'
    elif 'CW' in category:
        mode = 'CW'

    if not power:
        if 'QRP' in category:
            power = 'QRP'
        elif 'LOW' in category:
            power = 'LOW'
        elif 'HIGH' in category:
            power = 'HIGH'

    band_hint = normalize_band_text(category)
    if band_hint:
        band = band_hint

    return {
        'operator': operator,
        'transmitter': transmitter,
        'mode': mode,
        'assisted': assisted,
        'power': power,
        'band': band,
    }


def build_cabrillo(
    call: str,
    qsos: Sequence[Tuple[int, str, str, str, str, str, str, str, str]],
    category: Dict[str, str],
    operators: str | None,
    meta: Dict[str, str],
) -> str:
    def append_if(lines: List[str], label: str, value: str | None) -> None:
        if value:
            lines.append(f"{label}: {value}")

    def build_location(meta: Dict[str, str]) -> str:
        if meta.get("location"):
            return meta["location"]
        parts: List[str] = []
        for key in ("qth", "city", "region", "district", "country", "grid", "rda"):
            value = meta.get(key)
            if value and value not in parts:
                parts.append(value)
        return ", ".join(parts)

    claimed_score = meta.get("claimed_score", "")
    location = build_location(meta)
    lines = [
        "START-OF-LOG: 3.0",
        "CREATED-BY: ua9qcq-russian-dx-ubn-downloader",
        "CONTEST: RUSSIAN-DX",
        f"CALLSIGN: {call}",
        f"CATEGORY-OPERATOR: {category.get('operator', '')}",
        f"CATEGORY-BAND: {category.get('band', DEFAULT_BAND)}",
        f"CATEGORY-MODE: {category.get('mode', '')}",
        f"CATEGORY-ASSISTED: {category.get('assisted', '')}",
        f"CATEGORY-TRANSMITTER: {category.get('transmitter', '')}",
        f"CATEGORY-STATION: {meta.get('station', '')}",
        f"CATEGORY-POWER: {category.get('power', '')}",
    ]
    append_if(lines, "CATEGORY-OVERLAY", meta.get("overlay"))
    lines.append(f"OPERATORS: {operators or call}")
    append_if(lines, "CLAIMED-SCORE", claimed_score)
    append_if(lines, "CLUB", meta.get("club"))
    append_if(lines, "NAME", meta.get("name"))
    append_if(lines, "CQ-ZONE", meta.get("cq_zone"))
    append_if(lines, "ITU-ZONE", meta.get("itu_zone"))
    append_if(lines, "ARRL-SECTION", meta.get("arrl_section"))
    append_if(lines, "DXCC", meta.get("dxcc"))
    append_if(lines, "CONTINENT", meta.get("continent"))
    append_if(lines, "LOCATION", location)
    append_if(lines, "ADDRESS", meta.get("address"))
    append_if(lines, "ADDRESS", meta.get("address2"))
    append_if(lines, "ADDRESS-CITY", meta.get("city"))
    append_if(lines, "ADDRESS-STATE-PROVINCE", meta.get("region"))
    append_if(lines, "ADDRESS-COUNTRY", meta.get("country"))
    append_if(lines, "EMAIL", meta.get("email"))
    append_if(lines, "GRID-LOCATOR", meta.get("grid"))
    for freq, mode, date, time_val, their_call, rst_s, exch_s, rst_r, exch_r in qsos:
        lines.append(
            f"QSO: {freq:>5} {mode:<2} {date} {time_val:>4} "
            f"{call:<13} {rst_s:<3} {exch_s:<6} {their_call:<13} {rst_r:<3} {exch_r:<6}"
        )
    lines.append("END-OF-LOG:")
    return "\n".join(lines) + "\n"


def parse_year_options(html_text: str) -> List[str]:
    parser = SelectParser("db_yr")
    parser.feed(html_text)
    return parser.options


def parse_date_options(html_text: str) -> List[str]:
    parser = SelectParser("db_dt")
    parser.feed(html_text)
    return parser.options


def parse_more_info_entries(html_text: str) -> List[Dict[str, str]]:
    parser = MoreInfoFormParser()
    parser.feed(html_text)
    return parser.entries


def fetch_category(cookie: str, entry: Dict[str, str]) -> Dict[str, str]:
    try:
        html_text = fetch_text_with_cookie(MORE_INFO_URL, entry, cookie)
    except Exception:
        return derive_category_fields(None)
    category_text = parse_category_text(html_text)
    return derive_category_fields(category_text)


def fetch_station_meta(
    cookie: str, entry: Dict[str, str]
) -> Tuple[Dict[str, str], str | None, Dict[str, str]]:
    payload = dict(entry)
    payload.setdefault("lang", "en")
    try:
        html_text = fetch_text_with_cookie(MORE_INFO_URL, payload, cookie)
    except Exception:
        return derive_category_fields(None), None, {}
    meta = parse_more_info_fields(html_text)
    category_text = meta.get("category") or parse_category_text(html_text)
    operator_text = meta.get("operators") or parse_operator_text(html_text)
    return derive_category_fields(category_text, meta), operator_text, meta


def results_post_data(year: str, date_value: str) -> Dict[str, str]:
    return {
        "lang": "en",
        "testid": TEST_ID,
        "db_yr": year,
        "db_dt": date_value,
        "ctg_id": "all",
        "ov_id": "0",
        "terr_id": DEFAULT_TERR_ID,
        "obl_id": "0",
        "country_id": "0",
        "r150s_id": "0",
        "admin": "0",
    }


def pick_dates(options: List[str], count: int) -> List[str]:
    if not options or count <= 0:
        return []
    if len(options) <= count:
        return options
    return options[-count:]


@dataclass
class FetchStats:
    total_calls: int = 0
    saved_logs: int = 0
    skipped_empty: int = 0
    skipped_existing: int = 0
    errors: int = 0
    aborted: bool = False
    abort_reason: str = ""


def fetch_for_date(
    cookie: str,
    year: str,
    contest_date: str,
    output_dir: Path,
    sleep_s: float,
    start_time: int,
    include_errors: bool,
    limit_saved: int | None,
    progress_every: int | None,
    max_runtime_seconds: int | None = None,
    max_consecutive_errors: int | None = None,
    should_abort: Callable[[], bool] | None = None,
    max_idle_seconds: int | None = None,
) -> FetchStats:
    stats = FetchStats()
    started_at = time.monotonic()
    max_runtime = max_runtime_seconds if max_runtime_seconds and max_runtime_seconds > 0 else None
    max_idle = max_idle_seconds if max_idle_seconds and max_idle_seconds > 0 else None
    last_progress_at = started_at
    max_errors = max_consecutive_errors if max_consecutive_errors and max_consecutive_errors > 0 else None
    consecutive_errors = 0

    def abort(reason: str, count_error: bool = True) -> None:
        stats.aborted = True
        stats.abort_reason = reason
        if count_error and stats.errors == 0:
            stats.errors = 1

    results_html = fetch_text(RESULTS_URL, results_post_data(year, contest_date))
    entries = parse_more_info_entries(results_html)
    print(f"[{PROGRESS_LABEL}] progress {year} {contest_date}: discovered={len(entries)} station entries")
    calls: List[str] = []
    for entry in entries:
        call = entry.get("callsign")
        if call:
            calls.append(call.strip().upper())
    task_key = f"{Path(__file__).stem}:{year}:{contest_date}"
    skip, list_hash, item_count = task_should_skip(TASK_LEDGER, task_key, calls, upper=True)
    if skip:
        print(f"[{PROGRESS_LABEL}] skip (task ledger): {year} {contest_date} items={item_count}")
        return stats
    if not entries:
        task_mark_complete(TASK_LEDGER, task_key, list_hash, item_count)
        return stats

    for entry in entries:
        if should_abort and should_abort():
            abort("download interrupted")
            break
        if max_runtime is not None and time.monotonic() - started_at >= max_runtime:
            abort(f"date timeout after {max_runtime}s")
            break
        if max_idle is not None and time.monotonic() - last_progress_at >= max_idle:
            abort(f"idle timeout after {max_idle}s without progress", count_error=False)
            break
        if limit_saved is not None and stats.saved_logs >= limit_saved:
            break
        callsign = entry.get("callsign")
        id_res = entry.get("id_res")
        if not callsign or not id_res:
            continue
        call_norm = callsign.strip().upper()
        safe_call = call_norm.replace("/", "_")
        stats.total_calls += 1
        dest_dir = output_dir / year
        dest_path = dest_dir / f"{safe_call}.log"
        if archive_log_exists(dest_path):
            stats.skipped_existing += 1
            consecutive_errors = 0
            last_progress_at = time.monotonic()
            if progress_every and stats.total_calls % progress_every == 0:
                print(
                    f"[{PROGRESS_LABEL}] progress {year} {contest_date}: processed={stats.total_calls} "
                    f"saved={stats.saved_logs} empty={stats.skipped_empty} "
                    f"existing={stats.skipped_existing} errors={stats.errors}"
                )
            continue
        post_data = {
            "lang": "en",
            "admin": "0",
            "testid": TEST_ID,
            "db_yr": year,
            "db_dt": contest_date,
            "callsign": callsign,
            "id_res": id_res,
        }
        try:
            category, operators, meta = fetch_station_meta(cookie, entry)
            html_text = fetch_text_with_cookie(UBN_URL, post_data, cookie)
            qsos = confirmed_qsos(html_text, start_time, include_errors)
            if not qsos:
                stats.skipped_empty += 1
            else:
                dest_dir.mkdir(parents=True, exist_ok=True)
                content = build_cabrillo(call_norm, qsos, category, operators, meta)
                atomic_write_text(dest_path, content)
                stats.saved_logs += 1
            consecutive_errors = 0
        except Exception:
            stats.errors += 1
            consecutive_errors += 1
            if max_errors is not None and consecutive_errors >= max_errors:
                abort(f"{consecutive_errors} consecutive call errors")
                break
        last_progress_at = time.monotonic()
        if sleep_s > 0:
            time.sleep(sleep_s)
        if progress_every and stats.total_calls % progress_every == 0:
            print(
                f"[{PROGRESS_LABEL}] progress {year} {contest_date}: processed={stats.total_calls} "
                f"saved={stats.saved_logs} empty={stats.skipped_empty} existing={stats.skipped_existing} errors={stats.errors}"
            )
    if stats.errors == 0 and not stats.aborted:
        task_mark_complete(TASK_LEDGER, task_key, list_hash, item_count)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch UA9QCQ UBN logs for Russian DX Contest."
    )
    parser.add_argument(
        "--year",
        help="Contest year to query (default: latest available on site).",
    )
    parser.add_argument(
        "--dates",
        nargs="+",
        help="Contest dates (YYYY-MM-DD). If omitted, prompts for selection.",
    )
    parser.add_argument(
        "--all-dates",
        action="store_true",
        help="Use all available dates for the selected year (skips prompt).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="How many oldest dates to use when --dates is omitted.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("RussianDXContest"),
        help="Output directory root.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Delay between UBN requests in seconds.",
    )
    parser.add_argument(
        "--start-time",
        type=int,
        default=0,
        help="Earliest QSO time (HHMM) to include (default: 0000).",
    )
    parser.add_argument(
        "--include-errors",
        action="store_true",
        help="Include QSO rows with non-empty Error column (default: included).",
    )
    parser.add_argument(
        "--limit-saved",
        type=int,
        default=None,
        help="Stop after saving this many logs (useful for test runs).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=0,
        help="Print progress every N calls (0 disables).",
    )
    parser.add_argument(
        "--max-date-seconds",
        type=int,
        default=DEFAULT_MAX_DATE_SECONDS,
        help="Abort one date after N seconds (0 disables, default: 900).",
    )
    parser.add_argument(
        "--max-consecutive-errors",
        type=int,
        default=DEFAULT_MAX_CONSECUTIVE_ERRORS,
        help="Abort one date after N consecutive call failures (0 disables, default: 50).",
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
    args.include_errors = True

    cookie = os.environ.get("UA9QCQ_COOKIE", "")
    if not cookie:
        cookie = getpass.getpass(
            "UA9QCQ session cookie (UA9QCQ_COOKIE, input hidden): "
        ).strip()
    if not cookie:
        raise SystemExit("UA9QCQ_COOKIE is required to fetch UBN logs.")

    landing = fetch_text(RESULTS_URL)
    years = parse_year_options(landing)
    if not years:
        raise SystemExit("Unable to find year options on results page.")
    year = args.year or max(years)

    landing_for_dates = fetch_text(RESULTS_URL, results_post_data(year, "0"))
    date_options = parse_date_options(landing_for_dates)
    if year and year != "0":
        date_options = [date for date in date_options if date.startswith(f"{year}-")]
    dates_from_site = bool(date_options)
    if not date_options:
        date_options = ["0"]

    if args.dates:
        dates = args.dates
    elif args.all_dates:
        dates = date_options
    else:
        if not dates_from_site:
            dates = ["0"]
        elif not sys.stdin.isatty():
            dates = pick_dates(date_options, args.count)
        else:
            recent_preview = ", ".join(date_options[:10])
            oldest_preview = ", ".join(date_options[-10:])
            print("Available dates are listed newest to oldest on UA9QCQ.")
            print(f"Newest 10: {recent_preview}")
            print(f"Oldest 10: {oldest_preview}")
            prompt = (
                "Enter dates (YYYY-MM-DD, space-separated), or press Enter for oldest "
                f"{args.count}: "
            )
            entry = input(prompt).strip()
            if entry:
                dates = entry.split()
            else:
                dates = pick_dates(date_options, args.count)

    if not dates:
        raise SystemExit("No dates selected.")

    out_dir = args.out
    out_dir.mkdir(parents=True, exist_ok=True)

    global TASK_LEDGER
    TASK_LEDGER = None if args.no_task_ledger else TaskLedger(args.task_ledger)

    overall = FetchStats()
    for contest_date in dates:
        remaining = None
        if args.limit_saved is not None:
            remaining = max(args.limit_saved - overall.saved_logs, 0)
            if remaining == 0:
                break
        stats = fetch_for_date(
            cookie,
            year,
            contest_date,
            out_dir,
            args.sleep,
            args.start_time,
            args.include_errors,
            remaining,
            args.progress_every or None,
            args.max_date_seconds,
            args.max_consecutive_errors,
        )
        overall.total_calls += stats.total_calls
        overall.saved_logs += stats.saved_logs
        overall.skipped_empty += stats.skipped_empty
        overall.skipped_existing += stats.skipped_existing
        overall.errors += stats.errors

    summary = (
        f"calls={overall.total_calls} saved={overall.saved_logs} "
        f"empty={overall.skipped_empty} skipped_existing={overall.skipped_existing} "
        f"errors={overall.errors}"
    )
    print(summary)


if __name__ == "__main__":
    try:
        main()
    finally:
        if TASK_LEDGER is not None:
            TASK_LEDGER.close()
