#!/usr/bin/env python3
"""
Downloader for TTC-SPCWC public checked logs.

The TTC-SPCWC site publishes per-round ranking pages and public checked-log
views, not raw Cabrillo files. This downloader converts those public QSO tables
back into Cabrillo-like logs for archival use.

Output layout:
  TTC-SPCWC/<YYYY-MM-DD>/<CALL>.log
"""

from __future__ import annotations

import argparse
import html
import http.client
import random
import re
import subprocess
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

from task_ledger import TASK_LEDGER_PATH, TaskLedger, task_mark_complete, task_should_skip

BASE_URL = "https://spcwc.pl"
RANKINGS_URL = f"{BASE_URL}/ttc/rankings?lang=en"
OUTPUT_ROOT = Path("TTC-SPCWC")
REQUEST_TIMEOUT = 30
READ_CHUNK_SIZE = 65536
TASK_LEDGER: "TaskLedger | None" = None

CALL_RE = re.compile(r"^[A-Z0-9]+(?:/[A-Z0-9]+)?$")
DATE_RE = re.compile(r"\b((?:19|20)\d{2}-\d{2}-\d{2})\b")
ROUND_RANKING_RE = re.compile(r"^/(\d+)/ranking(?:\?lang=en)?$")
CATEGORY_RE = re.compile(r"^(SO(?:40|80|AB)-(?:QRP|LP|HP))$")


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


@dataclass(frozen=True)
class Round:
    round_id: str
    date: str
    ranking_url: str


@dataclass(frozen=True)
class StationLog:
    round_id: str
    date: str
    call: str
    category: str
    url: str


@dataclass(frozen=True)
class Qso:
    date: str
    time_utc: str
    freq: int
    band: str
    mode: str
    station: str
    rst_sent: str
    exch_sent: str
    correspondent: str
    rst_recv: str
    exch_recv: str
    status: str
    errors: str


def fetch_text(
    url: str,
    headers: Dict[str, str] | None = None,
    retries: int = 3,
    delay: float = 1.0,
    allow_incomplete: bool = False,
) -> str:
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req_headers = {"User-Agent": USER_AGENT}
            if headers:
                req_headers.update(headers)
            req = urllib.request.Request(url, headers=req_headers)
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                chunks: List[bytes] = []
                while True:
                    try:
                        chunk = resp.read(READ_CHUNK_SIZE)
                    except http.client.IncompleteRead as exc:
                        if allow_incomplete and exc.partial and attempt + 1 >= retries:
                            chunks.append(exc.partial)
                            break
                        raise
                    if not chunk:
                        break
                    chunks.append(chunk)
                return b"".join(chunks).decode(charset, errors="replace")
        except Exception as exc:  # pylint: disable=broad-except
            last_exc = exc
            if attempt + 1 < retries:
                time.sleep(delay * (2 ** attempt))
            else:
                raise
    raise last_exc  # type: ignore[misc]


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def clean_call(text: str) -> str:
    for token in re.findall(r"[A-Z0-9/]+", text.upper()):
        if CALL_RE.match(token):
            return token
    return text.strip().upper()


def category_band(category: str) -> str:
    if category.startswith("SO40-"):
        return "40M"
    if category.startswith("SO80-"):
        return "80M"
    return "ALL"


def category_power(category: str) -> str:
    suffix = category.rsplit("-", 1)[-1]
    if suffix == "QRP":
        return "QRP"
    if suffix == "LP":
        return "LOW"
    if suffix == "HP":
        return "HIGH"
    return ""


class RowLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_row = False
        self.in_cell = False
        self.current_cell = ""
        self.cells: List[str] = []
        self.links: List[str] = []
        self.rows: List[Tuple[List[str], List[str]]] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str | None]]) -> None:
        attrs_dict = dict(attrs)
        if tag.lower() == "tr":
            self.in_row = True
            self.cells = []
            self.links = []
        elif self.in_row and tag.lower() in {"td", "th"}:
            self.in_cell = True
            self.current_cell = ""
        elif self.in_row and tag.lower() == "a":
            href = attrs_dict.get("href")
            if href:
                self.links.append(html.unescape(href))

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.current_cell += data

    def handle_entityref(self, name: str) -> None:
        if self.in_cell:
            self.current_cell += html.unescape(f"&{name};")

    def handle_charref(self, name: str) -> None:
        if self.in_cell:
            self.current_cell += html.unescape(f"&#{name};")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self.in_row and self.in_cell and tag in {"td", "th"}:
            self.cells.append(normalize_text(self.current_cell))
            self.current_cell = ""
            self.in_cell = False
        elif self.in_row and tag == "tr":
            self.rows.append((self.cells, self.links))
            self.in_row = False


class HrefParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.hrefs.append(html.unescape(href))


class QsoTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_row = False
        self.row_attrs: Dict[str, str] = {}
        self.in_cell = False
        self.cell_text = ""
        self.cell_attrs: Dict[str, str] = {}
        self.cells: List[Tuple[str, Dict[str, str]]] = []
        self.qsos: List[Qso] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str | None]]) -> None:
        attrs_dict = {key: value or "" for key, value in attrs}
        if tag.lower() == "tr":
            self.in_row = True
            self.row_attrs = attrs_dict
            self.cells = []
        elif self.in_row and tag.lower() in {"td", "th"}:
            self.in_cell = True
            self.cell_text = ""
            self.cell_attrs = attrs_dict
        elif self.in_cell and tag.lower() == "br":
            self.cell_text += " "

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.cell_text += data

    def handle_entityref(self, name: str) -> None:
        if self.in_cell:
            self.cell_text += html.unescape(f"&{name};")

    def handle_charref(self, name: str) -> None:
        if self.in_cell:
            self.cell_text += html.unescape(f"&#{name};")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self.in_row and self.in_cell and tag in {"td", "th"}:
            self.cells.append((normalize_text(self.cell_text), dict(self.cell_attrs)))
            self.in_cell = False
            self.cell_text = ""
            self.cell_attrs = {}
        elif self.in_row and tag == "tr":
            self._finish_row()
            self.in_row = False
            self.row_attrs = {}
            self.cells = []

    def _finish_row(self) -> None:
        texts = [text for text, _attrs in self.cells]
        if len(texts) < 11 or not DATE_RE.fullmatch(texts[0]):
            return
        band_attrs = self.cells[2][1]
        band = band_attrs.get("data-band") or texts[2]
        freq_text = band_attrs.get("data-freq") or ("7020" if band == "40M" else "3520")
        try:
            freq = int(freq_text)
        except ValueError:
            freq = 7020 if band == "40M" else 3520
        self.qsos.append(
            Qso(
                date=texts[0],
                time_utc=texts[1],
                freq=freq,
                band=band,
                mode=texts[3],
                station=clean_call(texts[4]),
                rst_sent=texts[5],
                exch_sent=texts[6],
                correspondent=clean_call(texts[7]),
                rst_recv=texts[8],
                exch_recv=texts[9],
                status=texts[10],
                errors=self.row_attrs.get("data-errors", ""),
            )
        )


def parse_rounds(html_text: str) -> List[Round]:
    parser = RowLinkParser()
    parser.feed(html_text)
    seen: set[str] = set()
    rounds: List[Round] = []
    for cells, links in parser.rows:
        date_match = DATE_RE.search(" ".join(cells))
        if not date_match:
            continue
        date_text = date_match.group(1)
        for href in links:
            path = urllib.parse.urlparse(href).path
            match = ROUND_RANKING_RE.match(path)
            if not match:
                continue
            round_id = match.group(1)
            if round_id in seen:
                continue
            seen.add(round_id)
            rounds.append(
                Round(
                    round_id=round_id,
                    date=date_text,
                    ranking_url=urllib.parse.urljoin(BASE_URL, f"/{round_id}/ranking?lang=en"),
                )
            )
    return sorted(rounds, key=lambda item: item.date, reverse=True)


def discover_rounds() -> List[Round]:
    return parse_rounds(fetch_text(RANKINGS_URL))


def parse_category_urls(html_text: str, round_id: str) -> List[Tuple[str, str]]:
    parser = HrefParser()
    parser.feed(html_text)
    seen: set[str] = set()
    categories: List[Tuple[str, str]] = []
    prefix = f"/{round_id}/ranking/"
    for href in parser.hrefs:
        path = urllib.parse.urlparse(href).path
        if not path.startswith(prefix):
            continue
        category = path[len(prefix) :]
        if not CATEGORY_RE.fullmatch(category) or category in seen:
            continue
        seen.add(category)
        categories.append((category, urllib.parse.urljoin(BASE_URL, f"{path}?lang=en")))
    return sorted(categories)


def parse_station_links(html_text: str, round_info: Round, category: str) -> List[StationLog]:
    parser = HrefParser()
    parser.feed(html_text)
    seen: set[str] = set()
    stations: List[StationLog] = []
    prefix = f"/{round_info.round_id}/log/"
    for href in parser.hrefs:
        path = urllib.parse.urlparse(href).path
        if not path.startswith(prefix):
            continue
        call = clean_call(urllib.parse.unquote(path[len(prefix) :]))
        if not call or call in seen:
            continue
        seen.add(call)
        stations.append(
            StationLog(
                round_id=round_info.round_id,
                date=round_info.date,
                call=call,
                category=category,
                url=urllib.parse.urljoin(BASE_URL, f"{path}?lang=en"),
            )
        )
    return sorted(stations, key=lambda item: item.call)


def parse_expected_station_count(html_text: str) -> int | None:
    text = normalize_text(re.sub(r"<[^>]+>", " ", html_text))
    match = re.search(r"\b(\d+)\s+stations\b", text, flags=re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1))


def parse_expected_qso_count(html_text: str) -> int | None:
    text = normalize_text(re.sub(r"<[^>]+>", " ", html_text))
    match = re.search(
        r"\bRound\s+\d{4}-\d{2}-\d{2}\s*[·-]\s*(\d+)\s+QSOs?\b",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return int(match.group(1))


def discover_station_logs(round_info: Round) -> List[StationLog]:
    ranking_html = fetch_text(round_info.ranking_url)
    category_urls = parse_category_urls(ranking_html, round_info.round_id)
    stations_by_call: Dict[str, StationLog] = {}
    for category, url in category_urls:
        category_stations: List[StationLog] = []
        expected: int | None = None
        for attempt in range(3):
            category_html = fetch_text(url)
            expected = parse_expected_station_count(category_html)
            category_stations = parse_station_links(category_html, round_info, category)
            if expected is None or len(category_stations) >= expected:
                break
            if attempt < 2:
                time.sleep(0.5 * (2 ** attempt))
        if expected is not None and len(category_stations) < expected:
            raise ValueError(
                f"{round_info.date} {category}: parsed {len(category_stations)} station links, expected {expected}"
            )
        for station in category_stations:
            stations_by_call.setdefault(station.call, station)
    return sorted(stations_by_call.values(), key=lambda item: (item.category, item.call))


def parse_qsos(html_text: str) -> List[Qso]:
    parser = QsoTableParser()
    parser.feed(html_text)
    return parser.qsos


def build_cabrillo(station: StationLog, qsos: Sequence[Qso], source_url: str | None = None) -> str:
    if not qsos:
        raise ValueError(f"No QSO rows for {station.call} {station.date}")
    call = station.call.replace("/", "_").upper()
    errors = sum(1 for qso in qsos if qso.status.upper() != "OK" or qso.errors)
    lines = [
        "START-OF-LOG: 3.0",
        "CREATED-BY: spcwc-ttc-public-log-downloader",
        "CONTEST: TTC-SPCWC",
        f"CALLSIGN: {station.call}",
        "CATEGORY-OPERATOR: SINGLE-OP",
        f"CATEGORY-BAND: {category_band(station.category)}",
        "CATEGORY-MODE: CW",
        f"CATEGORY-POWER: {category_power(station.category)}",
        f"CATEGORY: {station.category}",
        f"CLAIMED-SCORE: {max(len(qsos) - errors, 0)}",
        f"OPERATORS: {station.call}",
        f"SOAPBOX: Generated from public checked-log view {source_url or station.url}",
        "SOAPBOX: QSO status/error details from the public page are not represented in Cabrillo QSO lines.",
    ]
    if errors:
        lines.append(f"SOAPBOX: Public checked-log view reported {errors} QSO rows with non-OK status.")
    for qso in qsos:
        time_text = qso.time_utc.replace(":", "").zfill(4)
        lines.append(
            f"QSO: {qso.freq:>5} {qso.mode:<2} {qso.date} {time_text:>4} "
            f"{call:<13} {qso.rst_sent:<3} {qso.exch_sent:<6} "
            f"{qso.correspondent:<13} {qso.rst_recv:<3} {qso.exch_recv:<6}"
        )
    lines.append("END-OF-LOG:")
    return "\n".join(lines) + "\n"


def destination_for(station: StationLog, output_root: Path | None = None) -> Path:
    if output_root is None:
        output_root = OUTPUT_ROOT
    return output_root / station.date / f"{station.call.replace('/', '_').upper()}.log"


def valid_existing_log(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    try:
        if path.stat().st_size <= 0:
            return False
        head = path.read_bytes()[:4096]
    except OSError:
        return False
    stripped = head.lstrip().lower()
    if stripped.startswith((b"<!doctype html", b"<html")):
        return False
    if b"start-of-log" not in stripped[:2048]:
        return False
    return True


def remove_invalid_existing(path: Path) -> bool:
    if not path.exists() or valid_existing_log(path):
        return False
    try:
        path.unlink()
        return True
    except OSError:
        return False


def fetch_log(station: StationLog) -> str:
    last_shortfall: str | None = None
    for attempt in range(6):
        html_text = fetch_text(station.url, allow_incomplete=True)
        qsos = parse_qsos(html_text)
        expected = parse_expected_qso_count(html_text)
        if expected is None or len(qsos) >= expected:
            return build_cabrillo(station, qsos)
        last_shortfall = f"parsed {len(qsos)} QSO rows, expected {expected}"
        if attempt < 5:
            time.sleep(0.5 * (2 ** attempt))
    try:
        proc = subprocess.run(
            ["curl", "-L", "--fail", "--silent", "--show-error", "--retry", "5", "--retry-delay", "1", station.url],
            capture_output=True,
            check=False,
            timeout=90,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError(f"{station.date} {station.call}: {last_shortfall}; curl fallback failed: {exc}") from exc
    if proc.returncode == 0 and proc.stdout:
        html_text = proc.stdout.decode("utf-8", errors="replace")
        qsos = parse_qsos(html_text)
        expected = parse_expected_qso_count(html_text)
        if expected is None or len(qsos) >= expected:
            return build_cabrillo(station, qsos)
        last_shortfall = f"curl parsed {len(qsos)} QSO rows, expected {expected}"
    raise ValueError(f"{station.date} {station.call}: {last_shortfall}")


def write_log(station: StationLog, output_root: Path | None = None) -> Path:
    dest = destination_for(station, output_root)
    if valid_existing_log(dest):
        return dest
    remove_invalid_existing(dest)
    payload = fetch_log(station)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(payload, encoding="utf-8")
    return dest


def iter_rounds(last: int | None = None) -> Iterable[Round]:
    rounds = discover_rounds()
    if last:
        return rounds[:last]
    return rounds


def main() -> int:
    parser = argparse.ArgumentParser(description="Download TTC-SPCWC public checked logs.")
    parser.add_argument("--last", type=int, default=None, help="How many recent published rounds to download.")
    parser.add_argument("--round-id", nargs="+", help="Specific published round ID(s), e.g. 25.")
    parser.add_argument("--out", type=Path, default=OUTPUT_ROOT, help="Output directory root.")
    parser.add_argument(
        "--task-ledger",
        type=Path,
        default=TASK_LEDGER_PATH,
        help="SQLite task ledger (default: scripts/download_tasks_ledger.sqlite).",
    )
    parser.add_argument("--no-task-ledger", action="store_true", help="Disable task ledger usage.")
    args = parser.parse_args()

    global TASK_LEDGER
    TASK_LEDGER = None if args.no_task_ledger else TaskLedger(args.task_ledger)

    rounds = list(iter_rounds(args.last))
    if args.round_id:
        requested = set(args.round_id)
        rounds = [round_info for round_info in rounds if round_info.round_id in requested]

    total_ok = total_skip = total_err = 0
    for round_info in rounds:
        try:
            stations = discover_station_logs(round_info)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"fail list {round_info.date}: {exc}")
            total_err += 1
            continue
        task_key = f"TTC-SPCWC/{round_info.date}"
        skip, list_hash, count = task_should_skip(TASK_LEDGER, task_key, [s.url for s in stations])
        if skip:
            print(f"{round_info.date}: skip (task ledger) items={count}")
            continue
        errors = 0
        for station in stations:
            dest = destination_for(station, args.out)
            if valid_existing_log(dest):
                total_skip += 1
                continue
            remove_invalid_existing(dest)
            try:
                write_log(station, args.out)
                total_ok += 1
            except Exception as exc:  # pylint: disable=broad-except
                print(f"fail {station.date} {station.call}: {exc}")
                errors += 1
                total_err += 1
        if errors == 0:
            task_mark_complete(TASK_LEDGER, task_key, list_hash, count)
    print(f"done ok={total_ok} skip={total_skip} err={total_err}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
