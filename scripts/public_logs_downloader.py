#!/usr/bin/env python3
"""
Public contest logs downloader with interactive menu.

Supports:
  1) CQWW (PH/CW)
  2) CQWPX (PH/CW)
  3) CQWW RTTY
  4) CQ 160 (PH/CW)
  5) CQWPX RTTY
  6) ARRL contests (all contests from publiclogs.php)
  7) ZRS KVP (pomlad/jesen on vhfmanager.net)
  8) EUHFC (reconstructed from UBN reports)
  9) WAE (CW/SSB/RTTY open logs)
 10) VHFManager contests (official/unofficial)
 11) Wednesday Mini-Test 40m (UA9QCQ UBN)
 12) Russian DX Contest (UA9QCQ UBN)
 13) Wednesday Mini-Test 80m (UA9QCQ UBN)
 14) RF Championship CW (UA9QCQ UBN)
 15) Ham Spirit Contest (UA9QCQ UBN)
 17) RCC Cup (UA9QCQ UBN)
  18) RDA Contest (UA9QCQ UBN)
 19) Russian Radio Team Championship (UA9QCQ UBN)
 20) Yuri Gagarin International DX Contest (UA9QCQ UBN)
 21) Coupe du REF (French HF Championship, CW/SSB)
  22) EUDX Contest (public logs)
  23) OK Contest (CW/SSB) + OK-OM DX Contest (CW/SSB) + OK DX RTTY Contest
  24) DARC contests (Fieldday/WAG/Ausbildungscontest/CW/RTTY/FT4/Easter/XMAS)
  26) WW DIGI (public logs)
  27) SP DX Contest (recreated from public result JSON)
  28) OK1WC Memorial (preliminary/final reference tables)
  29) YU DX Contest (recreated from public result JSON)
  30) SAC Scandinavian Activity Contest (public Cabrillo logs)
  31) URE public logs (EAPSK63/EARTTY/King of Spain/CNCW/CME)
  32) 9A HRS contests (HF Robot public QSO tables)
  33) Istra Open Contest (public Cabrillo logs)
  34) TTC-SPCWC (public checked-log tables)

Directory layout roots:
  CQWW/, CQWPX/, CQWWRTTY/, CQ160/, CQWPXRTTY/, ARRL/<contest_slug>/,
  WWDIGI/<year>/,
  ZRS_KVP/<year>/<season>/, EUHFC/<year>/, WAE/<mode>/<year>/,
  EU_VHF_CONTESTS/<contest_slug>_<ContestID>/, WednesdayMiniTest40m/<date>/,
  RussianDXContest/<year>/, WednesdayMiniTest80m/<date>/,
  RFChampionshipCW/<year>/, HamSpiritContest/<year>/, RCCCup/<year>/,
  RDAContest/<year>/, RussianRadioTeamChampionship/<year>/,
  YuriGagarinDXContest/<year>/, REF/<year>/<mode>/, EUDX_contest/<year>/,
  OK_Contest/<year>/<mode>/, OK_OM_DX_Contest/<year>/<mode>/, OK_DX_RTTY_contest/<year>/,
  SPDX_contest/<year>/, OK1WC_Memorial/<date>/, YU_DX_Contest/<year>/,
  SAC/<mode>/<year>/, URE/<contest>/<year>/, 9A_HRS_Contest/<contest>/<year>/,
  Istra_Open_Contest/<year>/, TTC-SPCWC/<date>/,
  DARC/Fieldday/<mode>/<year>/, DARC/WAG/<year>/,
  DARC/Ausbildungscontest/<year>/, DARC/Ausbildungscontest_CW/<year>/<edition>/,
  DARC/RTTY_Kurzcontest/<year>/<edition>/, DARC/FT4/<year>/<edition>/,
  DARC/Easter/<year>/, DARC/XMAS/<year>/

Usage: run the script, pick contests (or all), then choose how many years (number or 'all').
"""

from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import html
import hashlib
import http.client
import http.cookiejar
import json
import os
import random
import re
import sys
import threading
import time
import unicodedata
import urllib.parse
import urllib.request
from datetime import date
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Tuple


# Silence noisy HTTPResponse close errors on Python 3.14 shutdown.
if not getattr(http.client, "_hmra_safe_close", False):
    _orig_close = http.client.HTTPResponse.close

    def _safe_close(self) -> None:
        try:
            _orig_close(self)
        except ValueError as exc:
            if "closed file" in str(exc).lower():
                return
            raise

    http.client.HTTPResponse.close = _safe_close  # type: ignore[assignment]
    http.client._hmra_safe_close = True
import subprocess
import sqlite3
import zlib
from collections import Counter, OrderedDict, defaultdict


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
DEFAULT_WORKERS = 20
HOST_WORKER_CAPS = {
    "ua9qcq.com": 1,
    "memorial-ok1wc.cz": 1,
    "www.sactest.net": 1,
    "concursos.ure.es": 4,
    "www.hamradio.hr": 4,
    "ioc.9a1p.com": 4,
    "spcwc.pl": 2,
}

PRINT_LOCK = threading.Lock()
UA9QCQ_COOKIE_LOCK = threading.Lock()
UA9QCQ_COOKIE: str | None = None
DOWNLOAD_CANCEL_EVENT = threading.Event()
TASK_LEDGER_PATH = Path("scripts") / "download_tasks_ledger.sqlite"
TASK_LEDGER: "TaskLedger | None" = None

MANIFEST_ROOTS = {
    "ARRL",
    "CQ160",
    "CQWPX",
    "CQWPXRTTY",
    "CQWW",
    "CQWWRTTY",
    "EUHFC",
    "EUDX_contest",
    "EU_VHF_CONTESTS",
    "HamSpiritContest",
    "Istra_Open_Contest",
    "OK_Contest",
    "OK1WC_Memorial",
    "OK_OM_DX_Contest",
    "OK_DX_RTTY_contest",
    "RCCCup",
    "RDAContest",
    "REF",
    "RFChampionshipCW",
    "RussianDXContest",
    "RussianRadioTeamChampionship",
    "SAC",
    "WAE",
    "WednesdayMiniTest40m",
    "WednesdayMiniTest80m",
    "DARC",
    "WWDIGI",
    "WW_PMC",
    "YuriGagarinDXContest",
    "YU_DX_Contest",
    "9A_HRS_Contest",
    "ZRS_KVP",
    "SPDX_contest",
    "TTC-SPCWC",
    "URE",
    "RECONSTRUCTED_LOGS",
}

MODE_MAP = {
    "cw": "CW",
    "ph": "PH",
    "phone": "PH",
    "ssb": "SSB",
    "rtty": "RTTY",
    "rt": "RTTY",
    "digital": "DIGI",
    "digi": "DIGI",
}

LOG_MODE_MAP = {
    "CW": "CW",
    "PH": "PH",
    "PHONE": "PH",
    "SSB": "PH",
    "FM": "PH",
    "AM": "PH",
    "RT": "RTTY",
    "RY": "RTTY",
    "RTTY": "RTTY",
    "DIG": "DIGI",
    "DG": "DIGI",
    "DIGI": "DIGI",
    "DIGITAL": "DIGI",
    "FT4": "DIGI",
    "FT8": "DIGI",
    "MIX": "MIXED",
    "MIXED": "MIXED",
    "ALL": "MIXED",
}

SEASON_MAP = {
    "spring": "Spring",
    "summer": "Summer",
    "fall": "Fall",
    "autumn": "Fall",
    "winter": "Winter",
    "pomlad": "Spring",
    "jesen": "Fall",
}

MONTH_MAP = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "mai": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}

LOG_EXTS = {".log", ".adi", ".cbr"}
REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_MODE_SCAN_BYTES = 1024 * 1024
MANIFEST_MODE_QSO_LIMIT = 200
MANIFEST_DETAIL_TOKENS = set(MODE_MAP) | set(SEASON_MAP) | set(MONTH_MAP)


@dataclass
class DownloadTask:
    dest: Path
    host: str  # hostname (before DNS resolution)
    source: str  # provider label
    action: Callable[[], Dict[str, int]]
    task_key: str | None = None
    task_hash: str | None = None
    task_count: int | None = None
    output_roots: Tuple[str, ...] = ()


def empty_counts() -> Dict[str, int]:
    return {"ok": 0, "skip": 0, "error": 0, "cancel": 0}


def add_counts(target: Dict[str, int], delta: Dict[str, int]) -> None:
    for key, value in delta.items():
        target[key] = target.get(key, 0) + value


def get_ua9qcq_cookie() -> str:
    global UA9QCQ_COOKIE
    if UA9QCQ_COOKIE is not None:
        return UA9QCQ_COOKIE
    with UA9QCQ_COOKIE_LOCK:
        if UA9QCQ_COOKIE is not None:
            return UA9QCQ_COOKIE
        cookie = os.environ.get("UA9QCQ_COOKIE", "").strip()
        if not cookie and sys.stdin.isatty():
            cookie = input("UA9QCQ session cookie (UA9QCQ_COOKIE): ").strip()
        if cookie:
            os.environ["UA9QCQ_COOKIE"] = cookie
        UA9QCQ_COOKIE = cookie
    return UA9QCQ_COOKIE


def fetch_text(url: str, retries: int = 3, delay: float = 1.0) -> str:
    """Fetch a URL and return decoded text with simple retries and UA."""
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


def ledger_key(dest_path: Path) -> str:
    return dest_path.as_posix()


class TaskLedger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                task_key TEXT PRIMARY KEY,
                list_hash TEXT NOT NULL,
                item_count INTEGER,
                last_checked INTEGER
            )
            """
        )
        self._conn.commit()

    def has_hash(self, task_key: str, list_hash: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "SELECT list_hash FROM tasks WHERE task_key = ?",
                (task_key,),
            )
            row = cur.fetchone()
            return bool(row and row[0] == list_hash)

    def set_hash(self, task_key: str, list_hash: str, item_count: int) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO tasks (task_key, list_hash, item_count, last_checked)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(task_key) DO UPDATE SET
                    list_hash=excluded.list_hash,
                    item_count=excluded.item_count,
                    last_checked=excluded.last_checked
                """,
                (task_key, list_hash, item_count, int(time.time())),
            )
            self._conn.commit()


def normalize_items(items: Iterable[str], upper: bool = False) -> List[str]:
    uniq: set[str] = set()
    for item in items:
        text = str(item).strip()
        if not text:
            continue
        if upper:
            text = text.upper()
        uniq.add(text)
    return sorted(uniq)


def hash_items(items: Iterable[str], upper: bool = False) -> Tuple[str, int]:
    normalized = normalize_items(items, upper=upper)
    blob = "\n".join(normalized).encode("utf-8")
    return hashlib.sha256(blob).hexdigest(), len(normalized)


def task_should_skip(task_key: str, items: Iterable[str], upper: bool = False) -> Tuple[bool, str, int]:
    list_hash, count = hash_items(items, upper=upper)
    if TASK_LEDGER and TASK_LEDGER.has_hash(task_key, list_hash):
        return True, list_hash, count
    return False, list_hash, count


def task_mark_complete(task_key: str, list_hash: str, item_count: int) -> None:
    if TASK_LEDGER:
        TASK_LEDGER.set_hash(task_key, list_hash, item_count)


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
    if b"<html" in stripped[:512] and b"start-of-log" not in stripped[:2048]:
        return False
    return True


def remove_invalid_existing(path: Path) -> bool:
    if not path.exists():
        return False
    if valid_existing_log(path):
        return False
    try:
        path.unlink()
    except OSError:
        return False
    with PRINT_LOCK:
        print(f"retry (invalid existing): {path}")
    return True


def task_should_skip_known_outputs(
    task_key: str,
    items: Iterable[str],
    expected_paths: Iterable[Path],
    upper: bool = False,
    label: str | None = None,
) -> Tuple[bool, str, int]:
    skip, list_hash, count = task_should_skip(task_key, items, upper=upper)
    if not skip or count == 0:
        return skip, list_hash, count
    expected = list(expected_paths)
    valid_count = sum(1 for path in expected if valid_existing_log(path))
    if valid_count < count:
        with PRINT_LOCK:
            prefix = f"{label}: " if label else ""
            print(
                f"{prefix}stale task ledger for {task_key} "
                f"(valid {valid_count}/{count} files), re-queueing missing logs"
            )
        skip = False
    return skip, list_hash, count


def season_from_month(month: int | None) -> str | None:
    if month is None:
        return None
    if month in {12, 1, 2}:
        return "Winter"
    if month in {3, 4, 5}:
        return "Spring"
    if month in {6, 7, 8}:
        return "Summer"
    if month in {9, 10, 11}:
        return "Fall"
    return None


def normalize_log_mode(value: object) -> str | None:
    raw = str(value or "").strip().upper()
    if not raw:
        return None
    for token in re.split(r"[^A-Z0-9]+", raw):
        if token in LOG_MODE_MAP:
            return LOG_MODE_MAP[token]
    return LOG_MODE_MAP.get(raw)


def infer_log_mode_from_content(path: Path) -> str | None:
    qso_modes: set[str] = set()
    qso_seen = 0
    scanned = 0
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                scanned += len(line)
                if ":" not in line:
                    if scanned >= MANIFEST_MODE_SCAN_BYTES:
                        break
                    continue
                key, value = line.split(":", 1)
                key = key.strip().upper()
                if key == "CATEGORY-MODE":
                    mode = normalize_log_mode(value)
                    if mode:
                        return mode
                if key == "QSO":
                    fields = value.strip().split()
                    if len(fields) >= 2:
                        mode = normalize_log_mode(fields[1])
                        if mode:
                            qso_modes.add(mode)
                            qso_seen += 1
                    if qso_seen >= MANIFEST_MODE_QSO_LIMIT:
                        break
                if scanned >= MANIFEST_MODE_SCAN_BYTES:
                    break
    except OSError:
        return None

    if "MIXED" in qso_modes or len(qso_modes) > 1:
        return "MIXED"
    if qso_modes:
        return next(iter(qso_modes))
    return None


def manifest_segment_kind(segment: str) -> str:
    lower = segment.lower()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", segment):
        return "date"
    if re.fullmatch(r"(19|20)\d{2}", segment):
        return "year"
    if re.fullmatch(r"\d+(?:mhz|ghz)", lower):
        return "detail"
    tokens = [tok for tok in re.split(r"[^a-z0-9]+", lower) if tok]
    if tokens and all(tok in MANIFEST_DETAIL_TOKENS or re.fullmatch(r"\d+(?:mhz|ghz)", tok) for tok in tokens):
        return "detail"
    return "subcontest"


def manifest_path_hierarchy(parse_parts: Tuple[str, ...]) -> Tuple[str | None, str | None]:
    if len(parse_parts) < 3:
        return None, None
    middle = list(parse_parts[1:-1])
    split_idx: int | None = None
    include_split_segment = False
    for idx, segment in enumerate(middle):
        if manifest_segment_kind(segment) in {"year", "date"}:
            split_idx = idx
            break
    if split_idx is None:
        for idx, segment in enumerate(middle):
            tokens = [tok for tok in re.split(r"[^a-z0-9]+", segment.lower()) if tok]
            if any(re.fullmatch(r"(19|20)\d{2}", token) for token in tokens):
                split_idx = idx
                include_split_segment = True
                break
    if split_idx is None:
        return None, None

    before_time = middle[: split_idx + 1] if include_split_segment else middle[:split_idx]
    after_time = middle[split_idx + 1 :]
    subcontest_parts = [segment for segment in before_time if manifest_segment_kind(segment) == "subcontest"]
    detail_parts = [segment for segment in before_time if manifest_segment_kind(segment) == "detail"]
    detail_parts.extend(after_time)
    subcontest = "/".join(subcontest_parts) if subcontest_parts else None
    detail = "/".join(detail_parts) if detail_parts else None
    return subcontest, detail


def manifest_record_from_path(rel_path: Path, full_path: Path | None = None) -> Dict[str, object]:
    parts = rel_path.parts
    parse_parts = parts
    if parts and parts[0] == "RECONSTRUCTED_LOGS":
        parse_parts = parts[1:]
    contest = parse_parts[0] if parse_parts else ""
    callsign = rel_path.stem
    record: Dict[str, object] = {
        "path": rel_path.as_posix(),
        "callsign": callsign,
        "contest": contest,
    }
    subcontest, detail = manifest_path_hierarchy(parse_parts)
    if subcontest:
        record["subcontest"] = subcontest
        record["contest_slug"] = subcontest
    if detail:
        record["detail"] = detail

    year: int | None = None
    month: int | None = None
    week: int | None = None
    mode: str | None = None
    season: str | None = None
    band: str | None = None
    date_str: str | None = None

    for segment in parse_parts[:-1]:
        lower = segment.lower()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", segment):
            date_str = segment
            dt = date.fromisoformat(segment)
            year = dt.year
            month = dt.month
            week = dt.isocalendar().week
            season = season_from_month(month)
        if re.fullmatch(r"\d{4}", segment):
            year = int(segment)
        if re.fullmatch(r"\d+mhz", lower):
            band = segment
        if lower in SEASON_MAP:
            season = SEASON_MAP[lower]

        tokens = [tok for tok in re.split(r"[^a-z0-9]+", lower) if tok]
        for token in tokens:
            if token in MODE_MAP:
                mode = MODE_MAP[token]
            if token in MONTH_MAP:
                month = MONTH_MAP[token]
            if re.fullmatch(r"(19|20)\d{2}", token):
                year = int(token)

    if month is not None and season is None:
        season = season_from_month(month)

    if year is not None:
        record["year"] = year
    if mode is None and full_path is not None:
        mode = infer_log_mode_from_content(full_path)
    if mode is not None:
        record["mode"] = mode
    if season is not None:
        record["season"] = season
    if month is not None:
        record["month"] = month
    if week is not None:
        record["week"] = week
    if date_str is not None:
        record["date"] = date_str
    if band is not None:
        record["band"] = band
    return record


def callsign_bucket(callsign: str) -> int:
    if not callsign:
        return 0
    return zlib.crc32(callsign.upper().encode("ascii", errors="ignore")) & 0xFF


def build_sqlite_shards(repo_root: Path, shard_dir: Path, progress_every: int = 50000) -> int:
    shard_dir.mkdir(parents=True, exist_ok=True)
    for path in shard_dir.glob("logs_*.sqlite"):
        path.unlink()

    entries = 0
    max_open = 32
    connections: "OrderedDict[int, sqlite3.Connection]" = OrderedDict()
    batches: Dict[int, List[Tuple[object, ...]]] = {}

    def ensure_conn(bucket: int) -> sqlite3.Connection:
        conn = connections.get(bucket)
        if conn is not None:
            connections.move_to_end(bucket)
            return conn
        if len(connections) >= max_open:
            old_bucket, old_conn = connections.popitem(last=False)
            old_batch = batches.get(old_bucket, [])
            if old_batch:
                old_conn.executemany(
                    """
                    INSERT INTO logs (path, callsign, contest, year, mode, season, subcontest, detail)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    old_batch,
                )
                old_conn.commit()
                old_batch.clear()
            old_conn.close()
        db_path = shard_dir / f"logs_{bucket:02x}.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                path TEXT,
                callsign TEXT,
                contest TEXT,
                year INTEGER,
                mode TEXT,
                season TEXT,
                subcontest TEXT,
                detail TEXT
            )
            """
        )
        connections[bucket] = conn
        batches.setdefault(bucket, [])
        return conn

    for root in sorted(MANIFEST_ROOTS):
        base = repo_root / root
        if not base.exists():
            continue
        print(f"Scanning {base}...")
        for path in base.rglob("*"):
            if not path.is_file():
                continue
            if path.name == ".DS_Store":
                continue
            if path.suffix.lower() not in LOG_EXTS:
                continue
            rel = path.relative_to(repo_root)
            record = manifest_record_from_path(rel, path)
            callsign = str(record.get("callsign") or "")
            callsign = callsign.upper()
            bucket = callsign_bucket(callsign)
            conn = ensure_conn(bucket)
            batch = batches[bucket]
            batch.append(
                (
                    record.get("path"),
                    callsign,
                    record.get("contest"),
                    record.get("year"),
                    record.get("mode"),
                    record.get("season"),
                    record.get("subcontest"),
                    record.get("detail"),
                )
            )
            entries += 1
            if progress_every > 0 and entries % progress_every == 0:
                print(f"  indexed {entries} files...")
            if len(batch) >= 2000:
                conn.executemany(
                    """
                    INSERT INTO logs (path, callsign, contest, year, mode, season, subcontest, detail)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch,
                )
                conn.commit()
                batch.clear()

    for bucket, conn in list(connections.items()):
        batch = batches.get(bucket, [])
        if batch:
            conn.executemany(
                """
                INSERT INTO logs (path, callsign, contest, year, mode, season, subcontest, detail)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch,
            )
            conn.commit()
        conn.close()

    for db_path in shard_dir.glob("logs_*.sqlite"):
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_callsign ON logs(callsign)")
        conn.commit()
        conn.execute("VACUUM")
        conn.close()

    return entries


README_STATS_START = "<!-- STATS:START -->"
README_STATS_END = "<!-- STATS:END -->"
README_YEARS_START = "<!-- YEARS:START -->"
README_YEARS_END = "<!-- YEARS:END -->"

README_CONTEST_LABELS = {
    "9A_HRS_Contest": "9A HRS Contest",
    "EUDX_contest": "EUDX contest",
    "EU_VHF_CONTESTS": "EU VHF CONTESTS",
    "Istra_Open_Contest": "Istra Open Contest",
    "OK1WC_Memorial": "OK1WC Memorial",
    "OK_OM_DX_Contest": "OK OM DX Contest",
    "SPDX_contest": "SPDX contest",
    "WW_PMC": "WW PMC",
    "YU_DX_Contest": "YU DX Contest",
    "ZRS_KVP": "ZRS KVP",
}


@dataclass
class ReadmeStats:
    shard_count: int
    total_logs: int = 0
    source_logs: int = 0
    reconstructed_logs: int = 0
    source_callsigns: set[str] = field(default_factory=set)
    contest_roots: set[str] = field(default_factory=set)
    source_contest_counts: Counter[str] = field(default_factory=Counter)
    source_contest_years: Dict[str, set[int]] = field(default_factory=lambda: defaultdict(set))

    @property
    def source_callsign_count(self) -> int:
        return len(self.source_callsigns)

    @property
    def contest_root_count(self) -> int:
        return len(self.contest_roots)


def format_count(value: int) -> str:
    return f"{value:,}"


def readme_contest_label(contest: str) -> str:
    return README_CONTEST_LABELS.get(contest, contest)


def collect_readme_stats(shard_dir: Path) -> ReadmeStats:
    shard_paths = sorted(shard_dir.glob("logs_*.sqlite"))
    stats = ReadmeStats(shard_count=len(shard_paths))

    for shard_path in shard_paths:
        conn = sqlite3.connect(shard_path)
        try:
            stats.total_logs += int(conn.execute("SELECT count(*) FROM logs").fetchone()[0])
            stats.reconstructed_logs += int(
                conn.execute(
                    "SELECT count(*) FROM logs WHERE path LIKE 'RECONSTRUCTED_LOGS/%'"
                ).fetchone()[0]
            )
            stats.source_logs += int(
                conn.execute(
                    "SELECT count(*) FROM logs WHERE path NOT LIKE 'RECONSTRUCTED_LOGS/%'"
                ).fetchone()[0]
            )
            stats.contest_roots.update(
                str(row[0])
                for row in conn.execute("SELECT DISTINCT contest FROM logs WHERE contest IS NOT NULL AND contest != ''")
            )
            stats.source_callsigns.update(
                str(row[0])
                for row in conn.execute(
                    """
                    SELECT DISTINCT callsign
                    FROM logs
                    WHERE path NOT LIKE 'RECONSTRUCTED_LOGS/%'
                      AND callsign IS NOT NULL
                      AND callsign != ''
                    """
                )
            )
            stats.source_contest_counts.update(
                {
                    str(contest): int(count)
                    for contest, count in conn.execute(
                        """
                        SELECT contest, count(*)
                        FROM logs
                        WHERE path NOT LIKE 'RECONSTRUCTED_LOGS/%'
                          AND contest IS NOT NULL
                          AND contest != ''
                        GROUP BY contest
                        """
                    )
                }
            )
            for contest, year in conn.execute(
                """
                SELECT DISTINCT contest, year
                FROM logs
                WHERE path NOT LIKE 'RECONSTRUCTED_LOGS/%'
                  AND contest IS NOT NULL
                  AND contest != ''
                  AND typeof(year) = 'integer'
                """
            ):
                stats.source_contest_years[str(contest)].add(int(year))
        finally:
            conn.close()
    return stats


def render_readme_stats(stats: ReadmeStats, today: date | None = None) -> str:
    today = today or date.today()
    return "\n".join(
        [
            README_STATS_START,
            f"SH6-indexed snapshot counted on {today.isoformat()}:",
            "",
            f"- total indexed log files: {format_count(stats.total_logs)}",
            f"- source/public indexed log files: {format_count(stats.source_logs)}",
            (
                "- reconstructed mock log files in `RECONSTRUCTED_LOGS/`: "
                f"{format_count(stats.reconstructed_logs)}"
            ),
            (
                "- unique source/public callsigns in the SH6 index: "
                f"{format_count(stats.source_callsign_count)}"
            ),
            f"- contest roots in the SH6 index: {format_count(stats.contest_root_count)}",
            f"- SQLite shard files in `SH6/`: {format_count(stats.shard_count)}",
            README_STATS_END,
        ]
    )


def render_readme_years_table(stats: ReadmeStats) -> str:
    lines = [
        README_YEARS_START,
        "Years are collected from SH6 index metadata derived from archive paths.",
        "`RECONSTRUCTED_LOGS` and repo/tooling directories are excluded from this",
        "source/public table.",
        "",
        "| Top-level directory | Available years | Indexed source/public logs |",
        "|---|---|---:|",
    ]
    for contest in sorted(stats.source_contest_counts, key=lambda value: (readme_contest_label(value), value)):
        years = ", ".join(str(year) for year in sorted(stats.source_contest_years.get(contest, set())))
        lines.append(
            f"| {readme_contest_label(contest)} | {years} | "
            f"{format_count(stats.source_contest_counts[contest])} |"
        )
    lines.append(README_YEARS_END)
    return "\n".join(lines)


def replace_marked_section(text: str, start_marker: str, end_marker: str, replacement: str) -> str:
    if text.count(start_marker) != 1 or text.count(end_marker) != 1:
        raise ValueError(f"Expected exactly one README marker pair: {start_marker} / {end_marker}")
    start = text.find(start_marker)
    end = text.find(end_marker)
    if start == -1 or end == -1 or end < start:
        raise ValueError(f"Missing README markers: {start_marker} / {end_marker}")
    end += len(end_marker)
    return text[:start] + replacement + text[end:]


def update_readme_from_shards(repo_root: Path, shard_dir: Path, today: date | None = None) -> None:
    readme_path = repo_root / "README.md"
    stats = collect_readme_stats(shard_dir)
    text = readme_path.read_text(encoding="utf-8")
    text = replace_marked_section(
        text,
        README_STATS_START,
        README_STATS_END,
        render_readme_stats(stats, today=today),
    )
    text = replace_marked_section(
        text,
        README_YEARS_START,
        README_YEARS_END,
        render_readme_years_table(stats),
    )
    readme_path.write_text(text, encoding="utf-8")


class DownloadLedger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._entries: set[str] = set()
        self._loaded = False

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        if not self.path.exists():
            return
        try:
            with open(self.path, "r", encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    key = line.strip().split("\t", 1)[0]
                    if key:
                        self._entries.add(key)
        except Exception:  # pylint: disable=broad-except
            return

    def contains(self, key: str) -> bool:
        with self._lock:
            self._load()
            return key in self._entries

    def add(self, key: str, meta: str | None = None) -> bool:
        with self._lock:
            self._load()
            if key in self._entries:
                return False
            self.path.parent.mkdir(parents=True, exist_ok=True)
            line = key if meta is None else f"{key}\t{meta}"
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
            self._entries.add(key)
            return True

    def should_queue(self, dest_path: Path) -> bool:
        key = ledger_key(dest_path)
        if self.contains(key) and valid_existing_log(dest_path):
            return False
        if valid_existing_log(dest_path):
            self.add(key, "exists")
            return False
        remove_invalid_existing(dest_path)
        return True


class GzipLedger:
    def __init__(self, path: Path) -> None:
        self.path_txt = path
        suffix = path.suffix + ".gz" if path.suffix else ".gz"
        self.path_gz = path.with_suffix(suffix)
        self._lock = threading.Lock()
        self._entries: set[str] = set()
        self._loaded = False

    def _read_lines(self, path: Path, gz: bool) -> None:
        if not path.exists():
            return
        try:
            opener = gzip.open if gz else open
            mode = "rt" if gz else "r"
            with opener(path, mode, encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    key = line.strip().split("\t", 1)[0]
                    if key:
                        self._entries.add(key)
        except Exception:  # pylint: disable=broad-except
            return

    def _migrate_to_gz(self) -> None:
        if not self.path_txt.exists() and self.path_gz.exists():
            return
        self.path_gz.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(self.path_gz, "wt", encoding="utf-8") as fh:
            for key in sorted(self._entries):
                fh.write(key + "\n")
        if self.path_txt.exists():
            try:
                self.path_txt.unlink()
            except OSError:
                pass

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        self._read_lines(self.path_gz, gz=True)
        if self.path_txt.exists():
            self._read_lines(self.path_txt, gz=False)
            self._migrate_to_gz()

    def contains(self, key: str) -> bool:
        with self._lock:
            self._load()
            return key in self._entries

    def add(self, key: str, meta: str | None = None) -> bool:
        with self._lock:
            self._load()
            if key in self._entries:
                return False
            self.path_gz.parent.mkdir(parents=True, exist_ok=True)
            line = key if meta is None else f"{key}\t{meta}"
            with gzip.open(self.path_gz, "ab") as fh:
                fh.write((line + "\n").encode("utf-8"))
            self._entries.add(key)
            return True

    def should_queue(self, dest_path: Path) -> bool:
        key = ledger_key(dest_path)
        if self.contains(key) and valid_existing_log(dest_path):
            return False
        if valid_existing_log(dest_path):
            self.add(key, "exists")
            return False
        remove_invalid_existing(dest_path)
        return True


def ledger_for(text_path: Path) -> DownloadLedger | GzipLedger:
    return GzipLedger(text_path)


def download_file(
    dest_path: Path,
    url: str,
    ledger: DownloadLedger | GzipLedger | None = None,
    retries: int = 3,
    delay: float = 1.0,
) -> Dict[str, int]:
    """Download a URL to dest_path with retries; skip if exists."""
    key = ledger_key(dest_path)
    if ledger and ledger.contains(key) and valid_existing_log(dest_path):
        with PRINT_LOCK:
            print(f"skip (ledger): {dest_path}")
        return {"skip": 1}
    if valid_existing_log(dest_path):
        if ledger:
            ledger.add(key, "exists")
        with PRINT_LOCK:
            print(f"skip (exists): {dest_path}")
        return {"skip": 1}
    remove_invalid_existing(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp, open(dest_path, "wb") as fh:
                fh.write(resp.read())
            if ledger:
                ledger.add(key, url)
            with PRINT_LOCK:
                print(f"ok   {dest_path}")
            return {"ok": 1}
        except Exception as exc:  # pylint: disable=broad-except
            if attempt + 1 < retries:
                time.sleep(delay * (2 ** attempt))
            else:
                with PRINT_LOCK:
                    print(f"fail {url}: {exc}")
    return {"error": 1}


def extract_preformatted(text: str) -> str | None:
    match = re.search(r"<pre[^>]*>(.*?)</pre>", text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    body = match.group(1)
    body = re.sub(r"<br\\s*/?>", "\n", body, flags=re.IGNORECASE)
    return html.unescape(body)


def download_arrl_log(
    dest_path: Path,
    url: str,
    ledger: DownloadLedger | GzipLedger | None = None,
    retries: int = 3,
    delay: float = 1.0,
) -> Dict[str, int]:
    """Download an ARRL log, extracting Cabrillo text from HTML if needed."""
    key = ledger_key(dest_path)
    if ledger and ledger.contains(key) and valid_existing_log(dest_path):
        with PRINT_LOCK:
            print(f"skip (ledger): {dest_path}")
        return {"skip": 1}
    if valid_existing_log(dest_path):
        if ledger:
            ledger.add(key, "exists")
        with PRINT_LOCK:
            print(f"skip (exists): {dest_path}")
        return {"skip": 1}
    remove_invalid_existing(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp, open(dest_path, "wb") as fh:
                raw = resp.read()
                content = raw
                raw_lower = raw.lower()
                if b"<pre" in raw_lower or resp.headers.get_content_type() == "text/html":
                    charset = resp.headers.get_content_charset() or "utf-8"
                    text = raw.decode(charset, errors="ignore")
                    extracted = extract_preformatted(text)
                    if extracted:
                        if not extracted.endswith("\n"):
                            extracted += "\n"
                        content = extracted.encode("utf-8")
                fh.write(content)
            if ledger:
                ledger.add(key, url)
            with PRINT_LOCK:
                print(f"ok   {dest_path}")
            return {"ok": 1}
        except Exception as exc:  # pylint: disable=broad-except
            if attempt + 1 < retries:
                time.sleep(delay * (2 ** attempt))
            else:
                with PRINT_LOCK:
                    print(f"fail {url}: {exc}")
    return {"error": 1}


def make_http_task(
    dest_path: Path,
    url: str,
    source: str,
    ledger: DownloadLedger | None = None,
    retries: int = 3,
    delay: float = 1.0,
    task_key: str | None = None,
    task_hash: str | None = None,
    task_count: int | None = None,
    output_roots: Tuple[str, ...] = (),
) -> DownloadTask:
    """Wrap a simple file download into a DownloadTask."""
    host = urllib.parse.urlparse(url).hostname or "unknown"
    return DownloadTask(
        dest=dest_path,
        host=host,
        source=source,
        action=lambda dest=dest_path, link=url: download_file(
            dest,
            link,
            ledger=ledger,
            retries=retries,
            delay=delay,
        ),
        task_key=task_key,
        task_hash=task_hash,
        task_count=task_count,
        output_roots=output_roots,
    )


def resolve_hosts(hosts: Iterable[str]) -> Dict[str, List[str]]:
    """Resolve hosts with dig for logging; best-effort."""
    resolved: Dict[str, List[str]] = {}
    try:
        subprocess.run(["dig", "+short", "localhost"], capture_output=True, text=True, check=False)
    except FileNotFoundError:
        return resolved

    for host in hosts:
        if not host or host == "unknown":
            resolved[host] = []
            continue
        try:
            res = subprocess.run(["dig", "+short", host], capture_output=True, text=True, check=False)
            ips = [line.strip() for line in res.stdout.splitlines() if line.strip()]
            resolved[host] = ips
            with PRINT_LOCK:
                if ips:
                    print(f"dig {host}: {' '.join(ips)}")
                else:
                    print(f"dig {host}: no answers")
        except Exception as exc:  # pylint: disable=broad-except
            resolved[host] = []
            with PRINT_LOCK:
                print(f"dig {host} failed: {exc}")
    return resolved


class AdaptiveLimiter:
    """
    Adaptive concurrency limiter that adjusts available permits based on error rate.

    Decreasing concurrency is applied via a "debt" that withholds releases until
    the permit count matches the new limit.
    """

    def __init__(
        self,
        initial: int,
        min_limit: int,
        max_limit: int,
        window: int = 50,
        up_threshold: float = 0.01,
        down_threshold: float = 0.05,
    ) -> None:
        self._sema = threading.Semaphore(initial)
        self._limit = initial
        self._min = min_limit
        self._max = max_limit
        self._window = window
        self._up_threshold = up_threshold
        self._down_threshold = down_threshold
        self._succ = 0
        self._fail = 0
        self._debt = 0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        self._sema.acquire()

    def release(self, success: bool) -> None:
        with self._lock:
            if success:
                self._succ += 1
            else:
                self._fail += 1
            total = self._succ + self._fail
            # Apply debt if we reduced limit while all permits were busy.
            if self._debt > 0:
                self._debt -= 1
                # don't release a permit; we're shrinking capacity
                return
            self._sema.release()

            if total >= self._window:
                fail_rate = self._fail / total
                adjusted = False
                if fail_rate > self._down_threshold and self._limit > self._min:
                    self._limit -= 1
                    self._debt += 1
                    adjusted = True
                elif fail_rate < self._up_threshold and self._limit < self._max:
                    self._limit += 1
                    self._sema.release()
                    adjusted = True

                if adjusted:
                    with PRINT_LOCK:
                        print(
                            f"adaptive: limit {self._limit} (fail_rate={fail_rate:.3f}, window={total})"
                        )
                self._succ = 0
                self._fail = 0


# ----- CQWW -----
def tasks_cqww(last: int | None) -> List[DownloadTask]:
    base_url = "https://cqww.com/publiclogs/"
    base_dir = Path("CQWW")
    html_text = fetch_text(base_url)
    pages = []
    for match in re.finditer(r"href=['\"](?P<path>(?P<year>\d{4})(?P<mode>ph|cw)/)['\"]", html_text, flags=re.IGNORECASE):
        year = match.group("year")
        mode = match.group("mode").lower()
        path = match.group("path")
        pages.append((year, mode, urllib.parse.urljoin(base_url, path)))
    pages.sort(key=lambda itm: int(itm[0]), reverse=True)
    if last:
        pages = pages[: last * 2]
    tasks: List[DownloadTask] = []
    for year, mode, url in pages:
        html_page = fetch_text(url)
        log_urls = [
            urllib.parse.urljoin(url, href)
            for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE)
        ]
        task_key = f"CQWW/{mode.upper()}/{year}"
        dests = [
            base_dir / mode / year / Path(urllib.parse.urlparse(log_url).path).name
            for log_url in log_urls
        ]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, log_urls, dests, label="CQWW"
        )
        if skip:
            continue
        created = 0
        for log_url, dest in zip(log_urls, dests):
            if valid_existing_log(dest):
                continue
            remove_invalid_existing(dest)
            tasks.append(
                make_http_task(
                    dest,
                    log_url,
                    source="CQWW",
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- CQWPX -----
def tasks_cqwpx(last: int | None) -> List[DownloadTask]:
    base_url = "https://cqwpx.com/publiclogs/"
    base_dir = Path("CQWPX")
    html_text = fetch_text(base_url)
    pages = []
    for match in re.finditer(r"href=['\"](?P<path>(?P<year>\d{4})(?P<mode>ph|cw)/)['\"]", html_text, flags=re.IGNORECASE):
        year = match.group("year")
        mode = match.group("mode").lower()
        path = match.group("path")
        pages.append((year, mode, urllib.parse.urljoin(base_url, path)))
    pages.sort(key=lambda itm: int(itm[0]), reverse=True)
    if last:
        pages = pages[: last * 2]
    tasks: List[DownloadTask] = []
    for year, mode, url in pages:
        html_page = fetch_text(url)
        log_urls = [
            urllib.parse.urljoin(url, href)
            for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE)
        ]
        task_key = f"CQWPX/{mode.upper()}/{year}"
        dests = [
            base_dir / mode / year / Path(urllib.parse.urlparse(log_url).path).name
            for log_url in log_urls
        ]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, log_urls, dests, label="CQWPX"
        )
        if skip:
            continue
        created = 0
        for log_url, dest in zip(log_urls, dests):
            if valid_existing_log(dest):
                continue
            remove_invalid_existing(dest)
            tasks.append(
                make_http_task(
                    dest,
                    log_url,
                    source="CQWPX",
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- CQWW RTTY -----
def tasks_cqwwrtty(last: int | None) -> List[DownloadTask]:
    base_url = "https://cqwwrtty.com/publiclogs/"
    base_dir = Path("CQWWRTTY")
    html_text = fetch_text(base_url)
    years = []
    for match in re.finditer(r"href=['\"](?P<year>(19|20)\d{2})/['\"]", html_text):
        year = match.group("year")
        years.append((year, urllib.parse.urljoin(base_url, f"{year}/")))
    years.sort(key=lambda itm: int(itm[0]), reverse=True)
    if last:
        years = years[: last]
    tasks: List[DownloadTask] = []
    for year, url in years:
        html_page = fetch_text(url)
        log_urls = [
            urllib.parse.urljoin(url, href)
            for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE)
        ]
        task_key = f"CQWWRTTY/{year}"
        dests = [
            base_dir / year / Path(urllib.parse.urlparse(log_url).path).name
            for log_url in log_urls
        ]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, log_urls, dests, label="CQWW RTTY"
        )
        if skip:
            continue
        created = 0
        for log_url, dest in zip(log_urls, dests):
            if valid_existing_log(dest):
                continue
            remove_invalid_existing(dest)
            tasks.append(
                make_http_task(
                    dest,
                    log_url,
                    source="CQWW RTTY",
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- CQ 160 -----
def tasks_cq160(last: int | None) -> List[DownloadTask]:
    base_url = "https://cq160.com/publiclogs/"
    base_dir = Path("CQ160")
    html_text = fetch_text(base_url)
    pages = []
    for match in re.finditer(r"href=['\"](?P<path>(?P<year>\d{4})(?P<mode>ph|cw)/)['\"]", html_text, flags=re.IGNORECASE):
        year = match.group("year")
        mode = match.group("mode").lower()
        path = match.group("path")
        pages.append((year, mode, urllib.parse.urljoin(base_url, path)))
    pages.sort(key=lambda itm: int(itm[0]), reverse=True)
    if last:
        pages = pages[: last * 2]
    tasks: List[DownloadTask] = []
    for year, mode, url in pages:
        html_page = fetch_text(url)
        log_urls = [
            urllib.parse.urljoin(url, href)
            for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE)
        ]
        task_key = f"CQ160/{mode.upper()}/{year}"
        dests = [
            base_dir / mode / year / Path(urllib.parse.urlparse(log_url).path).name
            for log_url in log_urls
        ]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, log_urls, dests, label="CQ160"
        )
        if skip:
            continue
        created = 0
        for log_url, dest in zip(log_urls, dests):
            if valid_existing_log(dest):
                continue
            remove_invalid_existing(dest)
            tasks.append(
                make_http_task(
                    dest,
                    log_url,
                    source="CQ 160",
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- CQWPX RTTY -----
def tasks_cqwpxrtty(last: int | None) -> List[DownloadTask]:
    base_url = "https://cqwpxrtty.com/publiclogs/"
    base_dir = Path("CQWPXRTTY")
    html_text = fetch_text(base_url)
    years = []
    for match in re.finditer(r"href=['\"](?P<year>(19|20)\d{2})/['\"]", html_text):
        year = match.group("year")
        years.append((year, urllib.parse.urljoin(base_url, f"{year}/")))
    years.sort(key=lambda itm: int(itm[0]), reverse=True)
    if last:
        years = years[: last]
    tasks: List[DownloadTask] = []
    for year, url in years:
        html_page = fetch_text(url)
        log_urls = [
            urllib.parse.urljoin(url, href)
            for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE)
        ]
        task_key = f"CQWPXRTTY/{year}"
        dests = [
            base_dir / year / Path(urllib.parse.urlparse(log_url).path).name
            for log_url in log_urls
        ]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, log_urls, dests, label="CQWPX RTTY"
        )
        if skip:
            continue
        created = 0
        for log_url, dest in zip(log_urls, dests):
            if valid_existing_log(dest):
                continue
            remove_invalid_existing(dest)
            tasks.append(
                make_http_task(
                    dest,
                    log_url,
                    source="CQWPX RTTY",
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- WW DIGI -----
def tasks_wwdigi(last: int | None) -> List[DownloadTask]:
    base_url = "https://ww-digi.com/publiclogs/"
    base_dir = Path("WWDIGI")
    html_text = fetch_text(base_url)
    years: List[Tuple[str, str]] = []
    for match in re.finditer(r"href=['\"](?P<year>(19|20)\d{2})/['\"]", html_text, flags=re.IGNORECASE):
        year = match.group("year")
        years.append((year, urllib.parse.urljoin(base_url, f"{year}/")))
    years.sort(key=lambda itm: int(itm[0]), reverse=True)
    if last:
        years = years[: last]
    tasks: List[DownloadTask] = []
    for year, url in years:
        html_page = fetch_text(url)
        log_urls = [
            urllib.parse.urljoin(url, href)
            for href in re.findall(r"href=['\"]([^'\"<>]+\.log)['\"]", html_page, flags=re.IGNORECASE)
        ]
        task_key = f"WWDIGI/{year}"
        dests = [
            base_dir / year / Path(urllib.parse.urlparse(log_url).path).name
            for log_url in log_urls
        ]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, log_urls, dests, label="WW DIGI"
        )
        if skip:
            continue
        created = 0
        for log_url, dest in zip(log_urls, dests):
            if valid_existing_log(dest):
                continue
            remove_invalid_existing(dest)
            tasks.append(
                make_http_task(
                    dest,
                    log_url,
                    source="WW DIGI",
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- ARRL -----
def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def arrl_discover_contests() -> List[Tuple[str, str]]:
    html_text = fetch_text("https://contests.arrl.org/publiclogs.php")
    contests: List[Tuple[str, str]] = []
    for match in re.finditer(r'<option value=([0-9]+)>([^<]+)</option>', html_text, flags=re.IGNORECASE):
        eid = match.group(1)
        name = html.unescape(match.group(2)).strip()
        if eid == "0":
            continue
        contests.append((eid, name))
    return contests


def arrl_discover_years(eid: str) -> List[Tuple[str, str]]:
    html_text = fetch_text(f"https://contests.arrl.org/publiclogs.php?eid={eid}")
    years: List[Tuple[str, str]] = []
    pattern = re.compile(
        rf'href="publiclogs\.php\?eid={re.escape(eid)}&iid=(\d+)">((?:19|20)\d{{2}})<',
        flags=re.IGNORECASE,
    )
    for iid, year in pattern.findall(html_text):
        years.append((year, iid))
    years.sort(key=lambda tup: tup[0], reverse=True)
    return years


def arrl_discover_logs(eid: str, iid: str) -> Iterable[Tuple[str, str]]:
    html_text = fetch_text(f"https://contests.arrl.org/publiclogs.php?eid={eid}&iid={iid}")
    for match in re.finditer(r'href="(showpubliclog\.php\?[^"]+)".*?>([^<]+)</a>', html_text):
        link = html.unescape(match.group(1))
        call = html.unescape(match.group(2)).strip().upper()
        log_url = urllib.parse.urljoin("https://contests.arrl.org/", link)
        yield call, log_url


def tasks_arrl(last: int | None) -> List[DownloadTask]:
    contests = arrl_discover_contests()
    tasks: List[DownloadTask] = []
    for eid, name in contests:
        try:
            years = arrl_discover_years(eid)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Failed to fetch years for {name} ({eid}): {exc}", file=sys.stderr)
            continue
        if last:
            years = years[: last]
        if not years:
            print(f"No years found for {name} ({eid}).")
            continue
        contest_slug = slugify(name)
        for year, iid in years:
            try:
                logs = list(arrl_discover_logs(eid, iid))
            except Exception as exc:  # pylint: disable=broad-except
                print(f"Failed to fetch logs for {name} {year}: {exc}", file=sys.stderr)
                continue
            task_key = f"ARRL/{contest_slug}/{year}"
            log_urls = [url for _call, url in logs]
            dests = [
                Path("ARRL") / contest_slug / year / f"{call.replace('/', '-')}.log"
                for call, _url in logs
            ]
            skip, list_hash, count = task_should_skip_known_outputs(
                task_key, log_urls, dests, label="ARRL"
            )
            if skip:
                continue
            created = 0
            for (_call, log_url), dest in zip(logs, dests):
                if valid_existing_log(dest):
                    continue

                def action(dest=dest, url=log_url) -> Dict[str, int]:
                    return download_arrl_log(dest, url, ledger=None)

                tasks.append(
                    DownloadTask(
                        dest=dest,
                        host="contests.arrl.org",
                        source="ARRL",
                        action=action,
                        task_key=task_key,
                        task_hash=list_hash,
                        task_count=count,
                    )
                )
                created += 1
            if created == 0:
                task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- ZRS KVP -----
def tasks_zrs_kvp(last: int | None) -> List[DownloadTask]:
    import download_zrs_kvp_logs as zrs  # type: ignore

    seasons = zrs.discover_seasons(last)
    tasks: List[DownloadTask] = []
    for season in seasons:
        log_links = zrs.discover_logs(season)
        task_key = f"ZRS_KVP/{season.year}/{season.season}"
        skip, list_hash, count = task_should_skip(task_key, log_links)
        if skip:
            continue
        created = 0
        for url in log_links:
            host = urllib.parse.urlparse(url).hostname or "unknown"
            placeholder = Path(zrs.OUTPUT_ROOT) / str(season.year) / season.season / f"log-{abs(hash(url)) & 0xFFFF}.log"

            def action(season=season, url=url) -> Dict[str, int]:
                try:
                    result = zrs.download_log(url, season)
                except Exception as exc:  # pylint: disable=broad-except
                    with PRINT_LOCK:
                        print(f"fail {url}: {exc}")
                    return {"error": 1}
                if not result:
                    return {"skip": 1}
                call, cbr = result
                dest_path = Path(zrs.OUTPUT_ROOT) / str(season.year) / season.season / f"{call}.log"
                if valid_existing_log(dest_path):
                    with PRINT_LOCK:
                        print(f"skip (exists): {dest_path}")
                    return {"skip": 1}
                remove_invalid_existing(dest_path)
                final_dest = zrs.write_log(zrs.OUTPUT_ROOT, season, call, cbr)
                with PRINT_LOCK:
                    print(f"ok   {final_dest}")
                return {"ok": 1}

            tasks.append(
                DownloadTask(
                    dest=placeholder,
                    host=host,
                    source="ZRS KVP",
                    action=action,
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- EUHFC (UBN reconstructed) -----
def tasks_euhfc(last: int | None) -> List[DownloadTask]:
    import download_euhf_logs as euhf  # type: ignore

    years = euhf.discover_years()
    if last:
        years = years[:last]
    tasks: List[DownloadTask] = []
    for year in years:
        year_links: List[Tuple[str, str]] = []
        categories = euhf.discover_categories(year)
        if not categories:
            continue
        for cat in categories:
            for call, url in euhf.discover_ubn_links(year, cat):
                year_links.append((call, url))

        task_key = f"EUHFC/{year}"
        urls = [url for _c, url in year_links]
        dests = [
            euhf.OUTPUT_ROOT / str(year) / f"{call.replace('/', '_')}.log"
            for call, _url in year_links
        ]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, urls, dests, label="EUHFC"
        )
        if skip:
            continue
        created = 0
        for (call, url), placeholder in zip(year_links, dests):
            host = urllib.parse.urlparse(url).hostname or "unknown"
            if valid_existing_log(placeholder):
                continue
            remove_invalid_existing(placeholder)

            def action(year=year, call=call, url=url) -> Dict[str, int]:
                dest = euhf.OUTPUT_ROOT / str(year) / f"{call.replace('/', '_')}.log"
                if valid_existing_log(dest):
                    with PRINT_LOCK:
                        print(f"skip (exists): {dest}")
                    return {"skip": 1}
                remove_invalid_existing(dest)
                try:
                    text = euhf.fetch_text(url)
                except Exception as exc:  # pylint: disable=broad-except
                    with PRINT_LOCK:
                        print(f"fail {url}: {exc}")
                    return {"error": 1}
                owner = euhf.parse_owner(text, call)
                category = euhf.parse_category(text)
                qsos = euhf.extract_qsos(text, owner)
                if not qsos:
                    with PRINT_LOCK:
                        print(f"skip (no qsos): {owner} {year}")
                    return {"skip": 1}
                cab = euhf.build_cabrillo(owner, category, qsos)
                final_dest = euhf.write_log(year, owner, cab)
                with PRINT_LOCK:
                    print(f"ok   {final_dest}")
                return {"ok": 1}

            tasks.append(
                DownloadTask(
                    dest=placeholder,
                    host=host,
                    source="EUHFC",
                    action=action,
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- VHFManager -----
def tasks_vhfmanager(last: int | None) -> List[DownloadTask]:
    import download_vhfmanager_logs as vhf  # type: ignore

    contests = vhf.discover_contests(last)
    tasks: List[DownloadTask] = []
    for contest in contests:
        host = urllib.parse.urlparse(vhf.BASE_URL).hostname or "unknown"
        try:
            contest, links = vhf.discover_logs(contest)
        except Exception as exc:  # pylint: disable=broad-except
            with PRINT_LOCK:
                print(f"Failed to fetch contest {contest.cid}: {exc}")
            continue
        if not links:
            continue
        output_root = "WW_PMC" if vhf.is_pmc_contest(contest) else "EU_VHF_CONTESTS"
        task_key = f"{output_root}/{contest.cid}"
        skip, list_hash, count = task_should_skip(task_key, [lnk.url for lnk in links])
        if skip:
            continue
        placeholder = Path(output_root) / f"{vhf.slugify(contest.name)}_{contest.cid}" / "contest.log"

        def action(contest=contest, links=links) -> Dict[str, int]:
            counts = vhf.download_contest_logs(
                contest,
                links,
                workers=vhf.DEFAULT_WORKERS,
                max_logs=None,
                include_checklogs=not vhf.is_pmc_contest(contest),
            )
            return counts

        tasks.append(
            DownloadTask(
                dest=placeholder,
                host=host,
                source="VHFManager",
                action=action,
                task_key=task_key,
                task_hash=list_hash,
                task_count=count,
                output_roots=(output_root,),
            )
        )
    return tasks


# ----- WAE -----
def tasks_wae(last: int | None) -> List[DownloadTask]:
    import download_wae_logs as wae  # type: ignore

    tasks: List[DownloadTask] = []
    for mode, base in wae.MODES.items():
        years = wae.discover_years(base)
        if last:
            years = years[:last]
        if not years:
            continue
        latest = years[0]
        for year in years:
            calls = wae.discover_calls_for_year(base, year, latest)
            task_key = f"WAE/{mode.upper()}/{year}"
            dests = [
                wae.OUTPUT_ROOT / mode.upper() / str(year) / f"{call.replace('/', '_')}.log"
                for call in calls
            ]
            skip, list_hash, count = task_should_skip_known_outputs(
                task_key, calls, dests, upper=True, label="WAE"
            )
            if skip:
                continue
            created = 0
            for call, placeholder in zip(calls, dests):
                host = "dxhf2.darc.de"
                if valid_existing_log(placeholder):
                    continue
                remove_invalid_existing(placeholder)

                def action(mode=mode, base=base, call=call, year=year) -> Dict[str, int]:
                    dest = wae.OUTPUT_ROOT / mode.upper() / str(year) / f"{call.replace('/', '_')}.log"
                    if valid_existing_log(dest):
                        with PRINT_LOCK:
                            print(f"skip (exists): {dest}")
                        return {"skip": 1}
                    remove_invalid_existing(dest)
                    try:
                        cab = wae.fetch_log(base, call, year)
                    except Exception as exc:  # pylint: disable=broad-except
                        with PRINT_LOCK:
                            print(f"fail {call} {year} ({mode}): {exc}")
                        return {"error": 1}
                    if not cab:
                        with PRINT_LOCK:
                            print(f"skip (no cabrillo): {call} {year} ({mode})")
                        return {"skip": 1}
                    final_dest = wae.write_log(mode, year, call, cab)
                    with PRINT_LOCK:
                        print(f"ok   {final_dest}")
                    return {"ok": 1}

                tasks.append(
                    DownloadTask(
                        dest=placeholder,
                        host=host,
                        source="WAE",
                        action=action,
                        task_key=task_key,
                        task_hash=list_hash,
                        task_count=count,
                    )
                )
                created += 1
            if created == 0:
                task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- DARC contests -----
def _build_darc_tasks(keys: Tuple[str, ...], source: str, last: int | None) -> List[DownloadTask]:
    import download_darc_logs as darc  # type: ignore

    tasks: List[DownloadTask] = []
    host = "dxhf2.darc.de"
    for key in keys:
        spec = darc.CONTESTS[key]
        try:
            loglist_year, loglist_edition, loglist_calls = darc.discover_loglist(spec)
            periods = darc.discover_periods(spec, last=last)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Failed to discover periods for {spec.label}: {exc}", file=sys.stderr)
            continue
        accepted_periods = 0
        for year, edition in periods:
            try:
                calls = darc.discover_calls_for_period(
                    spec,
                    year,
                    edition,
                    loglist_year,
                    loglist_edition,
                    loglist_calls,
                )
            except Exception as exc:  # pylint: disable=broad-except
                print(f"Failed to discover calls for {spec.label} {darc.period_label(year, edition)}: {exc}", file=sys.stderr)
                continue
            if not calls:
                continue
            try:
                has_public_logs = darc.period_has_public_logs(spec, year, edition, calls)
            except Exception as exc:  # pylint: disable=broad-except
                print(f"Failed to probe logs for {spec.label} {darc.period_label(year, edition)}: {exc}", file=sys.stderr)
                continue
            if not has_public_logs:
                print(f"DARC skip (no public logs yet): {spec.label} {darc.period_label(year, edition)}")
                continue
            if last is not None and accepted_periods >= last:
                break
            accepted_periods += 1
            period_key = darc.period_label(year, edition)
            task_key = f"{spec.output_root.as_posix()}/{period_key}"
            dests = []
            for call in calls:
                dest = spec.output_root / str(year)
                if edition:
                    dest = dest / edition
                dests.append(dest / f"{call.replace('/', '_')}.log")
            skip, list_hash, count = task_should_skip_known_outputs(
                task_key, calls, dests, upper=True, label=source
            )
            if skip:
                continue
            created = 0
            for call, placeholder in zip(calls, dests):
                if valid_existing_log(placeholder):
                    continue
                remove_invalid_existing(placeholder)

                def action(spec=spec, call=call, year=year, edition=edition) -> Dict[str, int]:
                    dest = spec.output_root / str(year)
                    if edition:
                        dest = dest / edition
                    dest = dest / f"{call.replace('/', '_')}.log"
                    if valid_existing_log(dest):
                        with PRINT_LOCK:
                            print(f"skip (exists): {dest}")
                        return {"skip": 1}
                    remove_invalid_existing(dest)
                    try:
                        cab = darc.fetch_log(spec, call, year, edition=edition)
                    except Exception as exc:  # pylint: disable=broad-except
                        with PRINT_LOCK:
                            print(f"fail {call} {darc.period_label(year, edition)} ({spec.label}): {exc}")
                        return {"error": 1}
                    if not cab:
                        with PRINT_LOCK:
                            print(f"skip (no cabrillo): {call} {darc.period_label(year, edition)} ({spec.label})")
                        return {"skip": 1}
                    final_dest = darc.write_log(spec, year, call, cab, edition=edition)
                    with PRINT_LOCK:
                        print(f"ok   {final_dest}")
                    return {"ok": 1}

                tasks.append(
                    DownloadTask(
                        dest=placeholder,
                        host=host,
                        source=source,
                        action=action,
                        task_key=task_key,
                        task_hash=list_hash,
                        task_count=count,
                    )
                )
                created += 1
            if created == 0:
                task_mark_complete(task_key, list_hash, count)
    return tasks


def tasks_darc_all(last: int | None) -> List[DownloadTask]:
    return _build_darc_tasks(
        (
            "fieldday_cw",
            "fieldday_ssb",
            "wag",
            "ausbildungscontest",
            "ausbildungscontest_cw",
            "rtty_kurzcontest",
            "ft4",
            "easter",
            "xmas",
        ),
        "DARC",
        last,
    )


# ----- Wednesday Mini-Test 40m (UA9QCQ UBN) -----
def tasks_wed_minitest_40m(last: int | None) -> List[DownloadTask]:
    import download_wednesday_minitest_40m_ubn as wed  # type: ignore

    configure_ua9qcq_module(wed)
    cookie = get_ua9qcq_cookie()
    if not cookie:
        print("UA9QCQ_COOKIE is required for Wednesday Mini-Test 40m; skipping.")
        return []

    landing = wed.fetch_text_with_cookie(wed.RESULTS_URL, {"lang": "en"}, cookie)
    years = [y for y in wed.parse_year_options(landing) if y.isdigit()]
    years = sorted(set(years), reverse=True)
    landing_all_dates = wed.fetch_text_with_cookie(
        wed.RESULTS_URL, wed.results_post_data("0", "0"), cookie
    )
    dates_all = [d for d in wed.parse_date_options(landing_all_dates) if re.match(r"^\d{4}-\d{2}-\d{2}$", d)]
    if dates_all:
        years_from_dates = sorted({d[:4] for d in dates_all}, reverse=True)
        if not years or set(years).issubset(set(years_from_dates)):
            years = years_from_dates
    if last:
        years = years[:last]

    tasks: List[DownloadTask] = []
    host = urllib.parse.urlparse(wed.RESULTS_URL).hostname or "ua9qcq.com"
    output_root = Path("WednesdayMiniTest40m")
    for year in years:
        if dates_all:
            dates = [d for d in dates_all if d.startswith(f"{year}-")]
        else:
            landing_for_dates = wed.fetch_text_with_cookie(
                wed.RESULTS_URL, wed.results_post_data(year, "0"), cookie
            )
            dates = [d for d in wed.parse_date_options(landing_for_dates) if d.startswith(f"{year}-")]
        task_key = f"WednesdayMiniTest40m/{year}"
        skip, list_hash, count = task_should_skip(task_key, dates)
        if skip:
            continue
        for contest_date in dates:
            placeholder = output_root / contest_date / "contest.log"

            def action(
                year=year,
                contest_date=contest_date,
                cookie=cookie,
            ) -> Dict[str, int]:
                with PRINT_LOCK:
                    print(f"UA9QCQ start Wednesday Mini-Test 40m: year={year} date={contest_date}")
                stats = wed.fetch_for_date(
                    cookie,
                    year,
                    contest_date,
                    output_root,
                    sleep_s=0.0,
                    start_time=1700,
                    include_errors=False,
                    max_runtime_seconds=UA9QCQ_DATE_TIMEOUT,
                    max_consecutive_errors=UA9QCQ_MAX_CONSECUTIVE_ERRORS,
                    should_abort=DOWNLOAD_CANCEL_EVENT.is_set,
                )
                if stats.errors:
                    with PRINT_LOCK:
                        reason = getattr(stats, "abort_reason", "")
                        suffix = f" abort={reason}" if reason else ""
                        print(
                            f"fail {contest_date}: saved={stats.saved_logs} "
                            f"empty={stats.skipped_empty} existing={stats.skipped_existing} "
                            f"errors={stats.errors}{suffix}"
                        )
                    return {"error": 1}
                if stats.saved_logs:
                    with PRINT_LOCK:
                        print(
                            f"ok   {contest_date}: saved={stats.saved_logs} "
                            f"empty={stats.skipped_empty} existing={stats.skipped_existing}"
                        )
                    return {"ok": 1}
                with PRINT_LOCK:
                    print(f"skip (no logs): {contest_date}")
                return {"skip": 1}

            tasks.append(
                DownloadTask(
                    dest=placeholder,
                    host=host,
                    source="Wednesday Mini-Test 40m",
                    action=action,
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                    output_roots=(output_root.as_posix(),),
                )
            )
        if not dates:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- Wednesday Mini-Test 80m (UA9QCQ UBN) -----
def tasks_wed_minitest_80m(last: int | None) -> List[DownloadTask]:
    import download_wednesday_minitest_80m_ubn as wed  # type: ignore

    configure_ua9qcq_module(wed)
    cookie = get_ua9qcq_cookie()
    if not cookie:
        print("UA9QCQ_COOKIE is required for Wednesday Mini-Test 80m; skipping.")
        return []

    landing = wed.fetch_text_with_cookie(wed.RESULTS_URL, {"lang": "en"}, cookie)
    years = [y for y in wed.parse_year_options(landing) if y.isdigit()]
    years = sorted(set(years), reverse=True)
    landing_all_dates = wed.fetch_text_with_cookie(
        wed.RESULTS_URL, wed.results_post_data("0", "0"), cookie
    )
    dates_all = [
        d
        for d in wed.parse_date_options(landing_all_dates)
        if re.match(r"^\d{4}-\d{2}-\d{2}$", d)
    ]
    if dates_all:
        years_from_dates = sorted({d[:4] for d in dates_all}, reverse=True)
        if not years or set(years).issubset(set(years_from_dates)):
            years = years_from_dates
    if last:
        years = years[:last]

    tasks: List[DownloadTask] = []
    host = urllib.parse.urlparse(wed.RESULTS_URL).hostname or "ua9qcq.com"
    output_root = Path("WednesdayMiniTest80m")
    for year in years:
        if dates_all:
            dates = [d for d in dates_all if d.startswith(f"{year}-")]
        else:
            landing_for_dates = wed.fetch_text_with_cookie(
                wed.RESULTS_URL, wed.results_post_data(year, "0"), cookie
            )
            dates = [d for d in wed.parse_date_options(landing_for_dates) if d.startswith(f"{year}-")]
        task_key = f"WednesdayMiniTest80m/{year}"
        skip, list_hash, count = task_should_skip(task_key, dates)
        if skip:
            continue
        for contest_date in dates:
            placeholder = output_root / contest_date / "contest.log"

            def action(
                year=year,
                contest_date=contest_date,
                cookie=cookie,
            ) -> Dict[str, int]:
                with PRINT_LOCK:
                    print(f"UA9QCQ start Wednesday Mini-Test 80m: year={year} date={contest_date}")
                stats = wed.fetch_for_date(
                    cookie,
                    year,
                    contest_date,
                    output_root,
                    sleep_s=0.0,
                    start_time=1700,
                    include_errors=False,
                    max_runtime_seconds=UA9QCQ_DATE_TIMEOUT,
                    max_consecutive_errors=UA9QCQ_MAX_CONSECUTIVE_ERRORS,
                    should_abort=DOWNLOAD_CANCEL_EVENT.is_set,
                )
                if stats.errors:
                    with PRINT_LOCK:
                        reason = getattr(stats, "abort_reason", "")
                        suffix = f" abort={reason}" if reason else ""
                        print(
                            f"fail {contest_date}: saved={stats.saved_logs} "
                            f"empty={stats.skipped_empty} existing={stats.skipped_existing} "
                            f"errors={stats.errors}{suffix}"
                        )
                    return {"error": 1}
                if stats.saved_logs:
                    with PRINT_LOCK:
                        print(
                            f"ok   {contest_date}: saved={stats.saved_logs} "
                            f"empty={stats.skipped_empty} existing={stats.skipped_existing}"
                        )
                    return {"ok": 1}
                with PRINT_LOCK:
                    print(f"skip (no logs): {contest_date}")
                return {"skip": 1}

            tasks.append(
                DownloadTask(
                    dest=placeholder,
                    host=host,
                    source="Wednesday Mini-Test 80m",
                    action=action,
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                    output_roots=(output_root.as_posix(),),
                )
            )
        if not dates:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- UA9QCQ yearly contests (UBN) -----
def tasks_ua9qcq_yearly(
    last: int | None,
    module,
    source_label: str,
    output_root: Path,
    start_time: int = 0,
    include_errors: bool = True,
) -> List[DownloadTask]:
    configure_ua9qcq_module(module)
    cookie = get_ua9qcq_cookie()
    if not cookie:
        print(f"UA9QCQ_COOKIE is required for {source_label}; skipping.")
        return []

    def fetch_with_retry(data: Dict[str, str]) -> str:
        last_exc: Exception | None = None
        for attempt in range(2):
            try:
                return module.fetch_text_with_cookie(module.RESULTS_URL, data, cookie)
            except Exception as exc:  # pylint: disable=broad-except
                last_exc = exc
                time.sleep(1)
        raise last_exc or RuntimeError("UA9QCQ request failed")

    landing_all = fetch_with_retry(module.results_post_data("0", "0"))
    years = [y for y in module.parse_year_options(landing_all) if y.isdigit()]
    years = sorted(set(years), reverse=True)
    dates_all = [
        d
        for d in module.parse_date_options(landing_all)
        if re.match(r"^\d{4}-\d{2}-\d{2}$", d)
    ]
    if dates_all:
        years_from_dates = sorted({d[:4] for d in dates_all}, reverse=True)
        if not years or set(years).issubset(set(years_from_dates)):
            years = years_from_dates
    if last:
        years = years[:last]

    tasks: List[DownloadTask] = []
    host = urllib.parse.urlparse(module.RESULTS_URL).hostname or "ua9qcq.com"
    for year in years:
        if dates_all:
            dates = [d for d in dates_all if d.startswith(f"{year}-")]
        else:
            landing_for_dates = fetch_with_retry(module.results_post_data(year, "0"))
            dates = [
                d
                for d in module.parse_date_options(landing_for_dates)
                if re.match(r"^\d{4}-\d{2}-\d{2}$", d)
            ]
        if not dates:
            dates = ["0"]
        task_key = f"{output_root.as_posix()}/{year}"
        skip, list_hash, count = task_should_skip(task_key, dates)
        if skip:
            continue
        for contest_date in dates:
            placeholder = output_root / year / "contest.log"

            def action(
                year=year,
                contest_date=contest_date,
                cookie=cookie,
            ) -> Dict[str, int]:
                with PRINT_LOCK:
                    print(f"UA9QCQ start {source_label}: year={year} date={contest_date}")
                stats = module.fetch_for_date(
                    cookie,
                    year,
                    contest_date,
                    output_root,
                    sleep_s=0.0,
                    start_time=start_time,
                    include_errors=include_errors,
                    limit_saved=None,
                    progress_every=UA9QCQ_PROGRESS_EVERY,
                    max_runtime_seconds=UA9QCQ_DATE_TIMEOUT,
                    max_consecutive_errors=UA9QCQ_MAX_CONSECUTIVE_ERRORS,
                    should_abort=DOWNLOAD_CANCEL_EVENT.is_set,
                )
                if stats.errors:
                    with PRINT_LOCK:
                        reason = getattr(stats, "abort_reason", "")
                        suffix = f" abort={reason}" if reason else ""
                        print(
                            f"fail {year} {contest_date}: saved={stats.saved_logs} "
                            f"empty={stats.skipped_empty} existing={stats.skipped_existing} "
                            f"errors={stats.errors}{suffix}"
                        )
                    return {"error": 1}
                if stats.saved_logs:
                    with PRINT_LOCK:
                        print(
                            f"ok   {year} {contest_date}: saved={stats.saved_logs} "
                            f"empty={stats.skipped_empty} existing={stats.skipped_existing}"
                        )
                    return {"ok": 1}
                with PRINT_LOCK:
                    print(f"skip (no logs): {year} {contest_date}")
                return {"skip": 1}

            tasks.append(
                DownloadTask(
                    dest=placeholder,
                    host=host,
                    source=source_label,
                    action=action,
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                    output_roots=(output_root.as_posix(),),
                )
            )
        if not dates:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- Russian DX Contest (UA9QCQ UBN) -----
def tasks_rdxc(last: int | None) -> List[DownloadTask]:
    import download_russian_dx_contest_ubn as rdxc  # type: ignore

    return tasks_ua9qcq_yearly(
        last,
        rdxc,
        "Russian DX Contest",
        Path("RussianDXContest"),
        start_time=0,
        include_errors=True,
    )


# ----- RF Championship CW (UA9QCQ UBN) -----
def tasks_rf_championship_cw(last: int | None) -> List[DownloadTask]:
    import download_rf_championship_cw_ubn as rf_cw  # type: ignore

    return tasks_ua9qcq_yearly(
        last,
        rf_cw,
        "RF Championship CW",
        Path("RFChampionshipCW"),
        start_time=0,
        include_errors=True,
    )


# ----- Ham Spirit Contest (UA9QCQ UBN) -----
def tasks_ham_spirit(last: int | None) -> List[DownloadTask]:
    import download_ham_spirit_contest_ubn as ham_spirit  # type: ignore

    return tasks_ua9qcq_yearly(
        last,
        ham_spirit,
        "Ham Spirit Contest",
        Path("HamSpiritContest"),
        start_time=0,
        include_errors=True,
    )


# ----- RCC Cup (UA9QCQ UBN) -----
def tasks_rcc_cup(last: int | None) -> List[DownloadTask]:
    import download_rcc_cup_ubn as rcc_cup  # type: ignore

    return tasks_ua9qcq_yearly(
        last,
        rcc_cup,
        "RCC Cup",
        Path("RCCCup"),
        start_time=0,
        include_errors=True,
    )


# ----- RDA Contest (UA9QCQ UBN) -----
def tasks_rda(last: int | None) -> List[DownloadTask]:
    import download_rda_contest_ubn as rda  # type: ignore

    return tasks_ua9qcq_yearly(
        last,
        rda,
        "RDA Contest",
        Path("RDAContest"),
        start_time=0,
        include_errors=True,
    )


# ----- Russian Radio Team Championship (UA9QCQ UBN) -----
def tasks_rrtc(last: int | None) -> List[DownloadTask]:
    import download_russian_radio_team_championship_ubn as rrtc  # type: ignore

    return tasks_ua9qcq_yearly(
        last,
        rrtc,
        "Russian Radio Team Championship",
        Path("RussianRadioTeamChampionship"),
        start_time=0,
        include_errors=True,
    )


# ----- Yuri Gagarin International DX Contest (UA9QCQ UBN) -----
def tasks_yuri_gagarin(last: int | None) -> List[DownloadTask]:
    import download_yuri_gagarin_dx_contest_ubn as yuri  # type: ignore

    return tasks_ua9qcq_yearly(
        last,
        yuri,
        "Yuri Gagarin International DX Contest",
        Path("YuriGagarinDXContest"),
        start_time=0,
        include_errors=True,
    )


# ----- Coupe du REF (French HF Championship) -----
def tasks_ref(last: int | None) -> List[DownloadTask]:
    import download_ref_logs as ref  # type: ignore

    tasks: List[DownloadTask] = []
    current_year = date.today().year
    years = list(range(2010, current_year + 1))
    years.sort(reverse=True)
    if last:
        years = years[:last]
    host = "concours.r-e-f.org"
    for year in years:
        for contest, mode_label in ref.MODES.items():
            try:
                calls = ref.discover_calls(year, contest)
            except Exception as exc:  # pylint: disable=broad-except
                with PRINT_LOCK:
                    print(f"REF list failed {year} {mode_label}: {exc}")
                continue
            if not calls:
                continue
            task_key = f"REF/{year}/{contest}"
            dests = [
                ref.OUTPUT_ROOT / str(year) / mode_label / f"{call.replace('/', '_')}.log"
                for call in calls
            ]
            skip, list_hash, count = task_should_skip_known_outputs(
                task_key, calls, dests, upper=True, label="REF"
            )
            if skip:
                continue
            created = 0
            for call, placeholder in zip(calls, dests):
                if valid_existing_log(placeholder):
                    continue
                remove_invalid_existing(placeholder)

                def action(
                    year=year,
                    contest=contest,
                    mode_label=mode_label,
                    call=call,
                ) -> Dict[str, int]:
                    dest = ref.OUTPUT_ROOT / str(year) / mode_label / f"{call.replace('/', '_')}.log"
                    if valid_existing_log(dest):
                        with PRINT_LOCK:
                            print(f"skip (exists): {dest}")
                        return {"skip": 1}
                    remove_invalid_existing(dest)
                    try:
                        cab = ref.fetch_log(year, contest, call)
                    except Exception as exc:  # pylint: disable=broad-except
                        with PRINT_LOCK:
                            print(f"fail {call} {year} ({mode_label}): {exc}")
                        return {"error": 1}
                    if not cab:
                        with PRINT_LOCK:
                            print(f"skip (no log): {call} {year} ({mode_label})")
                        return {"skip": 1}
                    final_dest = ref.write_log(year, mode_label, call, cab)
                    with PRINT_LOCK:
                        print(f"ok   {final_dest}")
                    return {"ok": 1}

                tasks.append(
                    DownloadTask(
                        dest=placeholder,
                        host=host,
                        source="REF",
                        action=action,
                        task_key=task_key,
                        task_hash=list_hash,
                        task_count=count,
                    )
                )
                created += 1
            if created == 0:
                task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- EUDX Contest -----
def tasks_eudx(last: int | None) -> List[DownloadTask]:
    import download_eudx_logs as eudx  # type: ignore

    tasks: List[DownloadTask] = []
    years = eudx.discover_years()
    years.sort(reverse=True)
    if last:
        years = years[:last]
    for year in years:
        try:
            logs = eudx.discover_log_urls(year)
        except Exception as exc:  # pylint: disable=broad-except
            with PRINT_LOCK:
                print(f"EUDX list failed {year}: {exc}")
            continue
        if not logs:
            continue
        task_key = f"EUDX_contest/{year}"
        urls = [url for _c, url in logs]
        dests = [
            eudx.OUTPUT_ROOT / str(year) / f"{call.replace('/', '_')}.log"
            for call, _url in logs
        ]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, urls, dests, label="EUDX"
        )
        if skip:
            continue
        created = 0
        for (_call, url), dest in zip(logs, dests):
            if valid_existing_log(dest):
                continue
            remove_invalid_existing(dest)
            tasks.append(
                make_http_task(
                    dest,
                    url,
                    "EUDX",
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- Istra Open Contest -----
def tasks_istra_open(last: int | None) -> List[DownloadTask]:
    import download_istra_open_logs as ioc  # type: ignore

    tasks: List[DownloadTask] = []
    years = ioc.discover_year_urls()
    if last:
        years = years[:last]
    for year, public_logs_url in years:
        try:
            logs = ioc.discover_log_urls(year, public_logs_url)
        except Exception as exc:  # pylint: disable=broad-except
            with PRINT_LOCK:
                print(f"Istra Open list failed {year}: {exc}")
            continue
        if not logs:
            continue
        task_key = f"Istra_Open_Contest/{year}"
        urls = [url for _call, url in logs]
        dests = [
            ioc.OUTPUT_ROOT / str(year) / f"{call.replace('/', '_').upper()}.log"
            for call, _url in logs
        ]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, urls, dests, label="Istra Open"
        )
        if skip:
            continue
        created = 0
        for (_call, url), dest in zip(logs, dests):
            if valid_existing_log(dest):
                continue
            remove_invalid_existing(dest)
            tasks.append(
                make_http_task(
                    dest,
                    url,
                    "Istra Open",
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                    output_roots=(ioc.OUTPUT_ROOT.as_posix(),),
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- TTC-SPCWC -----
def tasks_ttc_spcwc(last: int | None) -> List[DownloadTask]:
    import download_ttc_spcwc_logs as ttc  # type: ignore

    tasks: List[DownloadTask] = []
    rounds = list(ttc.iter_rounds(last))
    for round_info in rounds:
        try:
            stations = ttc.discover_station_logs(round_info)
        except Exception as exc:  # pylint: disable=broad-except
            with PRINT_LOCK:
                print(f"TTC-SPCWC list failed {round_info.date}: {exc}")
            continue
        if not stations:
            continue
        task_key = f"TTC-SPCWC/{round_info.date}"
        urls = [station.url for station in stations]
        dests = [ttc.destination_for(station) for station in stations]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, urls, dests, label="TTC-SPCWC"
        )
        if skip:
            continue
        created = 0
        for station, dest in zip(stations, dests):
            if valid_existing_log(dest):
                continue
            remove_invalid_existing(dest)

            def action(station=station, dest=dest) -> Dict[str, int]:
                try:
                    payload = ttc.fetch_log(station)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_text(payload, encoding="utf-8")
                    with PRINT_LOCK:
                        print(f"ok   {dest}")
                    return {"ok": 1}
                except Exception as exc:  # pylint: disable=broad-except
                    with PRINT_LOCK:
                        print(f"fail {station.url}: {exc}")
                    return {"error": 1}

            tasks.append(
                DownloadTask(
                    dest=dest,
                    host=urllib.parse.urlparse(station.url).hostname or "unknown",
                    source="TTC-SPCWC",
                    action=action,
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                    output_roots=(ttc.OUTPUT_ROOT.as_posix(),),
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- OK-OM DX Contest -----
def tasks_okomdx(last: int | None) -> List[DownloadTask]:
    import download_okomdx_logs as okom  # type: ignore

    tasks: List[DownloadTask] = []
    pages = okom.iter_year_pages(last)
    for year, slug in pages:
        try:
            rounds = okom.discover_rounds(slug)
        except Exception as exc:  # pylint: disable=broad-except
            with PRINT_LOCK:
                print(f"OKOMDX rounds failed {year} {slug}: {exc}")
            continue
        for round_id in rounds:
            try:
                calls = okom.discover_calls(round_id)
            except Exception as exc:  # pylint: disable=broad-except
                with PRINT_LOCK:
                    print(f"OKOMDX list failed round {round_id}: {exc}")
                continue
            if not calls:
                continue
            meta = okom.classify_round(round_id, year, calls)
            if not meta:
                with PRINT_LOCK:
                    print(f"OKOMDX unknown contest header for round {round_id} ({year})")
                continue
            task_key = f"OKOMDX/{year}/{round_id}"
            dests = [okom.dest_path(meta, call) for call in calls]
            skip, list_hash, count = task_should_skip_known_outputs(
                task_key, calls, dests, upper=True, label="OKOMDX"
            )
            if skip:
                continue
            created = 0
            for call, dest in zip(calls, dests):
                if valid_existing_log(dest):
                    continue
                remove_invalid_existing(dest)

                def action(
                    round_id=round_id,
                    year=year,
                    call=call,
                ) -> Dict[str, int]:
                    try:
                        html_text = okom.fetch_log_html(round_id, call)
                    except Exception as exc:  # pylint: disable=broad-except
                        with PRINT_LOCK:
                            print(f"OKOMDX fail fetch {call} ({round_id}): {exc}")
                        return {"error": 1}
                    parsed = okom.parse_header_meta(html_text, year, call)
                    if not parsed:
                        with PRINT_LOCK:
                            print(f"OKOMDX unknown contest header {call} ({round_id})")
                        return {"error": 1}
                    qsos, detected = okom.parse_qsos(html_text, parsed.mode_label)
                    if detected:
                        parsed.mode_label = detected
                    cab = okom.build_cabrillo(parsed, qsos)
                    final_dest = okom.dest_path(parsed, parsed.call)
                    if valid_existing_log(final_dest):
                        with PRINT_LOCK:
                            print(f"skip (exists): {final_dest}")
                        return {"skip": 1}
                    remove_invalid_existing(final_dest)
                    okom.write_log(final_dest, cab)
                    with PRINT_LOCK:
                        print(f"ok   {final_dest}")
                    return {"ok": 1}

                tasks.append(
                    DownloadTask(
                        dest=dest,
                        host="okomdx.crk.cz",
                        source="OKOMDX",
                        action=action,
                        task_key=task_key,
                        task_hash=list_hash,
                        task_count=count,
                    )
                )
                created += 1
            if created == 0:
                task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- SP DX Contest -----
def tasks_spdx(last: int | None) -> List[DownloadTask]:
    import download_spdx_logs as spdx  # type: ignore

    tasks: List[DownloadTask] = []
    years = spdx.discover_years()
    years.sort(reverse=True)
    if last:
        years = years[:last]
    for year in years:
        try:
            calls = spdx.discover_calls(year)
        except Exception as exc:  # pylint: disable=broad-except
            with PRINT_LOCK:
                print(f"SPDX list failed {year}: {exc}")
            continue
        if not calls:
            continue
        task_key = f"SPDX_contest/{year}"
        dests = [spdx.dest_path(year, call) for call in calls]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, calls, dests, upper=True, label="SPDX"
        )
        if skip:
            continue
        created = 0
        for call, dest in zip(calls, dests):
            if valid_existing_log(dest):
                continue
            remove_invalid_existing(dest)

            def action(year=year, call=call, dest=dest) -> Dict[str, int]:
                try:
                    payload = spdx.fetch_log_data(year, call)
                    cab = spdx.build_cabrillo(year, call, payload)
                    if valid_existing_log(dest):
                        with PRINT_LOCK:
                            print(f"skip (exists): {dest}")
                        return {"skip": 1}
                    remove_invalid_existing(dest)
                    spdx.write_log(dest, cab)
                except Exception as exc:  # pylint: disable=broad-except
                    with PRINT_LOCK:
                        print(f"SPDX fail {call} {year}: {exc}")
                    return {"error": 1}
                with PRINT_LOCK:
                    print(f"ok   {dest}")
                return {"ok": 1}

            tasks.append(
                DownloadTask(
                    dest=dest,
                    host="spdxcontest.pzk.org.pl",
                    source="SPDX",
                    action=action,
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    return tasks


# ----- OK1WC Memorial -----
OK1WC_BASE_URL = "https://memorial-ok1wc.cz/"
OK1WC_ROUNDS_URL = urllib.parse.urljoin(OK1WC_BASE_URL, "index.php?page=roundsall")
OK1WC_TABLE_URL = urllib.parse.urljoin(
    OK1WC_BASE_URL,
    "index.php?page=eval3/table_out_vypis",
)
OK1WC_OUTPUT_ROOT = Path("OK1WC_Memorial")
OK1WC_LOG_PUB_LEVELS = {"3", "4"}
OK1WC_REQUEST_TIMEOUT = 12
OK1WC_CALL_DISCOVERY_WORKERS = 2
OK1WC_ROUND_CONTEXT_ERRORS = (
    "Pro vybrané podmínky není co zobrazit",
    "není možné před ukončením příjmu deníků",
)
OK1WC_BAND_FREQ = {
    "160M": "1800",
    "80M": "3500",
    "40M": "7000",
    "20M": "14000",
    "15M": "21000",
    "10M": "28000",
    "6M": "50000",
    "4M": "70000",
    "2M": "144000",
}
OK1WC_MODE_MAP = {
    "CW": "CW",
    "PH": "PH",
    "PHONE": "PH",
    "SSB": "PH",
    "FM": "FM",
    "RTTY": "RY",
    "RY": "RY",
    "DIGI": "DG",
    "DIGITAL": "DG",
}


@dataclass
class OK1WCRound:
    kolo: str
    rocnik: str
    tyden: str
    jobdate: str
    pub_level: str
    date_iso: str
    url: str


@dataclass
class OK1WCQSO:
    index: str
    category: str
    freq: str
    mode: str
    date: str
    time: str
    own_call: str
    sent_rst: str
    sent_exchange: str
    received_rst: str
    received_exchange: str
    worked_call: str


class OK1WCSession:
    def __init__(self) -> None:
        self.cookie_jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookie_jar)
        )
        self.initialized = False

    def fetch_text(
        self,
        url: str,
        data: Dict[str, str] | None = None,
        retries: int = 3,
        delay: float = 1.0,
    ) -> str:
        payload = None
        if data is not None:
            payload = urllib.parse.urlencode(data).encode("utf-8")
        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                req = urllib.request.Request(
                    url,
                    data=payload,
                    headers={
                        "User-Agent": USER_AGENT,
                        "Connection": "close",
                        "Referer": OK1WC_ROUNDS_URL,
                    },
                )
                if payload is not None:
                    req.add_header(
                        "Content-Type",
                        "application/x-www-form-urlencoded",
                    )
                with self.opener.open(req, timeout=OK1WC_REQUEST_TIMEOUT) as resp:
                    charset = resp.headers.get_content_charset() or "utf-8"
                    return resp.read().decode(charset, errors="ignore")
            except Exception as exc:  # pylint: disable=broad-except
                last_exc = exc
                if attempt + 1 < retries:
                    time.sleep(delay * (2 ** attempt))
                else:
                    raise
        raise last_exc  # type: ignore[misc]

    def ensure_initialized(self) -> None:
        if not self.initialized:
            self.fetch_text(OK1WC_ROUNDS_URL)
            self.initialized = True

    def fetch_round_page(self, round_info: OK1WCRound) -> str:
        self.ensure_initialized()
        return self.fetch_text(round_info.url)


def ok1wc_jobdate_to_iso(jobdate: str) -> str:
    if not re.fullmatch(r"\d{6}", jobdate):
        return "0000-00-00"
    yy = int(jobdate[:2])
    year = 2000 + yy if yy < 80 else 1900 + yy
    return f"{year:04d}-{int(jobdate[2:4]):02d}-{int(jobdate[4:6]):02d}"


def ok1wc_round_from_href(href: str) -> OK1WCRound | None:
    href = html.unescape(href).strip()
    full_url = urllib.parse.urljoin(OK1WC_BASE_URL, href)
    parsed = urllib.parse.urlparse(full_url)
    params_raw = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

    def param(name: str) -> str:
        return (params_raw.get(name, [""])[0] or "").strip()

    kolo = param("kolo")
    rocnik = param("rocnik")
    tyden = param("tyden")
    jobdate = param("jobdate")
    pub_level = param("pub_level")
    if not all([kolo, rocnik, tyden, jobdate]):
        return None
    if pub_level not in OK1WC_LOG_PUB_LEVELS:
        return None
    return OK1WCRound(
        kolo=kolo,
        rocnik=rocnik,
        tyden=tyden,
        jobdate=jobdate,
        pub_level=pub_level,
        date_iso=ok1wc_jobdate_to_iso(jobdate),
        url=full_url,
    )


def ok1wc_discover_rounds(last: int | None) -> List[OK1WCRound]:
    session = OK1WCSession()
    html_text = session.fetch_text(OK1WC_ROUNDS_URL)
    rounds_by_kolo: Dict[str, OK1WCRound] = {}
    for match in re.finditer(
        r"href=['\"](?P<href>index\.php\?page=eval3/a_eval3[^'\"]+)['\"]",
        html_text,
        flags=re.IGNORECASE,
    ):
        round_info = ok1wc_round_from_href(match.group("href"))
        if round_info:
            rounds_by_kolo.setdefault(round_info.kolo, round_info)
    rounds = sorted(
        rounds_by_kolo.values(),
        key=lambda item: (item.date_iso, int(item.kolo) if item.kolo.isdigit() else 0),
        reverse=True,
    )
    if not last:
        return rounds

    years_seen: set[str] = set()
    limited: List[OK1WCRound] = []
    for round_info in rounds:
        year = round_info.date_iso[:4]
        if year not in years_seen:
            if len(years_seen) >= last:
                continue
            years_seen.add(year)
        limited.append(round_info)
    return limited


def ok1wc_clean_cell(value: str) -> str:
    value = re.sub(r"<br\s*/?>", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"<[^>]+>", "", value)
    value = html.unescape(value).replace("\xa0", " ")
    return " ".join(value.split()).strip()


def ok1wc_parse_calls(html_text: str) -> List[str]:
    form_match = re.search(
        r"<form\b[^>]*table_out_vypis.*?</form>",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not form_match:
        return []
    seen: set[str] = set()
    calls: List[str] = []
    for raw in re.findall(
        r"<option\b[^>]*value=['\"]([^'\"]+)['\"]",
        form_match.group(0),
        flags=re.IGNORECASE,
    ):
        call = html.unescape(raw).strip()
        if call in {"", "no_calls", "all_calls"} or call in seen:
            continue
        seen.add(call)
        calls.append(call)
    return calls


def ok1wc_parse_band_mode(value: str) -> Tuple[str, str]:
    tokens = value.upper().replace("/", " ").split()
    band = tokens[0] if tokens else ""
    mode_token = tokens[1] if len(tokens) > 1 else ""
    freq = OK1WC_BAND_FREQ.get(band, "")
    if not freq:
        mhz_match = re.match(r"^(\d+(?:\.\d+)?)\s*MHZ$", band)
        if mhz_match:
            freq = str(int(float(mhz_match.group(1)) * 1000))
        else:
            freq = re.sub(r"\D", "", band) or "0"
    return freq, OK1WC_MODE_MAP.get(mode_token, mode_token or "CW")


def ok1wc_parse_datetime(value: str) -> Tuple[str, str]:
    match = re.search(
        r"(\d{1,2})/(\d{1,2})/(\d{2,4})\s+(\d{1,2}):(\d{2})",
        value,
    )
    if not match:
        return "0000-00-00", "0000"
    day, month, year_s, hour, minute = match.groups()
    year = int(year_s)
    if year < 100:
        year = 2000 + year if year < 80 else 1900 + year
    return (
        f"{year:04d}-{int(month):02d}-{int(day):02d}",
        f"{int(hour):02d}{int(minute):02d}",
    )


def ok1wc_split_report_exchange(value: str, mode: str) -> Tuple[str, str]:
    parts = value.split(None, 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) == 1:
        return parts[0], ""
    return ("59" if mode == "PH" else "599"), ""


def ok1wc_parse_qsos(html_text: str) -> List[OK1WCQSO]:
    qsos: List[OK1WCQSO] = []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html_text, flags=re.IGNORECASE | re.DOTALL):
        tds = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.IGNORECASE | re.DOTALL)
        if len(tds) < 16:
            continue
        cells = [ok1wc_clean_cell(td) for td in tds]
        if not cells[0].isdigit():
            continue
        freq, mode = ok1wc_parse_band_mode(cells[2])
        date_out, time_out = ok1wc_parse_datetime(cells[3])
        sent_rst, sent_exchange = ok1wc_split_report_exchange(cells[5], mode)
        rcvd_rst, rcvd_exchange = ok1wc_split_report_exchange(cells[6], mode)
        own_call = cells[4].upper()
        worked_call = cells[7].upper()
        if not all([own_call, worked_call, date_out, time_out]):
            continue
        qsos.append(
            OK1WCQSO(
                index=cells[0],
                category=cells[1],
                freq=freq,
                mode=mode,
                date=date_out,
                time=time_out,
                own_call=own_call,
                sent_rst=sent_rst,
                sent_exchange=sent_exchange,
                received_rst=rcvd_rst,
                received_exchange=rcvd_exchange,
                worked_call=worked_call,
            )
        )
    return qsos


def ok1wc_fetch_log_html(
    session: OK1WCSession,
    round_info: OK1WCRound,
    call: str,
) -> str:
    data = {
        "CallS": call,
        "kolo": round_info.kolo,
        "DispAll": "Y",
        "submit": "Odeslat",
    }
    html_text = session.fetch_text(OK1WC_TABLE_URL, data=data)
    if any(message in html_text for message in OK1WC_ROUND_CONTEXT_ERRORS):
        session.fetch_round_page(round_info)
        html_text = session.fetch_text(OK1WC_TABLE_URL, data=data)
    return html_text


def ok1wc_fetch_round_calls(
    round_info: OK1WCRound,
    attempts: int = 2,
) -> Tuple[OK1WCSession, List[str], str]:
    last_html = ""
    for attempt in range(1, attempts + 1):
        session = OK1WCSession()
        round_html = session.fetch_round_page(round_info)
        calls = ok1wc_parse_calls(round_html)
        if calls:
            return session, calls, round_html
        last_html = round_html
        if attempt < attempts:
            with PRINT_LOCK:
                print(
                    f"OK1WC retry calls {round_info.jobdate}: "
                    f"no reference calls on attempt {attempt}/{attempts}"
                )
            time.sleep(2 * attempt)
    return session, [], last_html


def ok1wc_make_round_session_getter(round_info: OK1WCRound) -> Callable[[], OK1WCSession]:
    session: OK1WCSession | None = None
    session_lock = threading.Lock()

    def get_session() -> OK1WCSession:
        nonlocal session
        with session_lock:
            if session is None:
                session = OK1WCSession()
                session.fetch_round_page(round_info)
            return session

    return get_session


def ok1wc_derive_category_fields(category: str, fallback_mode: str) -> Dict[str, str]:
    upper = category.upper()
    tokens = [token for token in re.split(r"[^A-Z0-9]+", upper) if token]
    operator = "SINGLE-OP"
    if "CHECKLOG" in tokens:
        operator = "CHECKLOG"
    elif "MULTI" in tokens:
        operator = "MULTI-OP"
    elif "SWL" in tokens:
        operator = "SWL"
    band = "ALL"
    for token in ("160M", "80M", "40M", "20M", "15M", "10M", "6M", "4M", "2M", "ALL"):
        if token in tokens:
            band = token
            break
    mode = fallback_mode
    for token in ("CW", "SSB", "PH", "RTTY", "MIXED"):
        if token in tokens:
            mode = "PH" if token == "SSB" else token
            break
    power = ""
    for token in ("QRP", "LOW", "HIGH"):
        if token in tokens:
            power = token
            break
    return {
        "operator": operator,
        "band": band,
        "mode": mode,
        "power": power,
    }


def ok1wc_build_cabrillo(
    round_info: OK1WCRound,
    call: str,
    qsos: List[OK1WCQSO],
) -> str:
    category = next((qso.category for qso in qsos if qso.category), "")
    fallback_mode = next((qso.mode for qso in qsos if qso.mode), "CW")
    category_fields = ok1wc_derive_category_fields(category, fallback_mode)
    station_call = qsos[0].own_call if qsos else call.upper()
    lines = [
        "START-OF-LOG: 3.0",
        "CREATED-BY: HMRA public OK1WC downloader",
        "CONTEST: OK1WC-MEMORIAL",
        f"CALLSIGN: {station_call}",
        f"OPERATORS: {station_call}",
    ]
    if category:
        lines.append(f"CATEGORY: {category}")
    lines.extend(
        [
            f"CATEGORY-OPERATOR: {category_fields['operator']}",
            f"CATEGORY-BAND: {category_fields['band']}",
            f"CATEGORY-MODE: {category_fields['mode']}",
        ]
    )
    if category_fields["power"]:
        lines.append(f"CATEGORY-POWER: {category_fields['power']}")
    lines.extend(
        [
            f"SOAPBOX: Recreated from OK1WC Memorial reference table MWC{round_info.jobdate}.",
            f"SOAPBOX: Source publication level: {round_info.pub_level}.",
            f"SOAPBOX: Source: {round_info.url}",
        ]
    )
    for qso in qsos:
        lines.append(
            f"QSO: {qso.freq:>5} {qso.mode:<2} {qso.date} {qso.time:>4} "
            f"{qso.own_call:<13} {qso.sent_rst:<3} {qso.sent_exchange:<10} "
            f"{qso.worked_call:<13} {qso.received_rst:<3} {qso.received_exchange:<10}"
        )
    lines.append("END-OF-LOG:")
    return "\n".join(lines) + "\n"


def ok1wc_dest_path(round_info: OK1WCRound, call: str) -> Path:
    safe_call = call.replace("/", "_")
    return OK1WC_OUTPUT_ROOT / round_info.date_iso / f"{safe_call}.log"


def ok1wc_round_marker_path(round_info: OK1WCRound) -> Path:
    return OK1WC_OUTPUT_ROOT / round_info.date_iso / f".pub_level_{round_info.pub_level}.complete"


def ok1wc_should_write_log(dest: Path, round_info: OK1WCRound) -> bool:
    if not dest.exists() or not valid_existing_log(dest):
        return True
    if round_info.pub_level != "4":
        return False
    try:
        existing = dest.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return False
    return "Source publication level: 3." in existing


def ok1wc_write_round_marker(marker: Path, round_info: OK1WCRound, calls: List[str]) -> None:
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(
        "\n".join(
            [
                f"jobdate={round_info.jobdate}",
                f"kolo={round_info.kolo}",
                f"pub_level={round_info.pub_level}",
                f"calls={len(calls)}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def ok1wc_write_log(dest: Path, content: str, overwrite: bool = False) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if overwrite or not dest.exists():
        dest.write_text(content, encoding="utf-8")
    return dest


def tasks_ok1wc(last: int | None) -> List[DownloadTask]:
    tasks: List[DownloadTask] = []
    with PRINT_LOCK:
        print("OK1WC: discovering preliminary/final rounds...")
    rounds = ok1wc_discover_rounds(last)
    with PRINT_LOCK:
        print(f"OK1WC: discovered {len(rounds)} rounds")
    pending_rounds = [
        (round_info, ok1wc_round_marker_path(round_info))
        for round_info in rounds
        if not ok1wc_round_marker_path(round_info).exists()
    ]

    def discover_calls(
        round_info: OK1WCRound,
        marker: Path,
    ) -> Tuple[OK1WCRound, Path, List[str], str, Exception | None]:
        try:
            _session, calls, round_html = ok1wc_fetch_round_calls(round_info)
            return round_info, marker, calls, round_html, None
        except Exception as exc:  # pylint: disable=broad-except
            return round_info, marker, [], "", exc

    round_results: List[Tuple[OK1WCRound, Path, List[str], str, Exception | None]] = []
    if pending_rounds:
        max_workers = min(OK1WC_CALL_DISCOVERY_WORKERS, len(pending_rounds))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(discover_calls, round_info, marker)
                for round_info, marker in pending_rounds
            ]
            for future in concurrent.futures.as_completed(futures):
                round_results.append(future.result())
    round_results.sort(
        key=lambda item: (
            item[0].date_iso,
            int(item[0].kolo) if item[0].kolo.isdigit() else 0,
        ),
        reverse=True,
    )

    for round_info, marker, calls, round_html, exc in round_results:
        if exc is not None:
            with PRINT_LOCK:
                print(f"OK1WC round failed {round_info.jobdate}: {exc}")
            continue
        if not calls:
            has_reference_form = "table_out_vypis" in round_html
            page_bytes = len(round_html.encode("utf-8", errors="ignore"))
            with PRINT_LOCK:
                print(
                    f"OK1WC fail (no calls): {round_info.jobdate} "
                    f"bytes={page_bytes} reference_form={has_reference_form}"
                )
            continue

        with PRINT_LOCK:
            print(
                f"OK1WC {round_info.jobdate} pub_level={round_info.pub_level}: "
                f"{len(calls)} calls"
            )
        task_key = f"OK1WC_Memorial/{round_info.date_iso}/{round_info.pub_level}"
        dests = [ok1wc_dest_path(round_info, call) for call in calls]
        task_items = [f"{call}\t{round_info.pub_level}" for call in calls]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, task_items, dests, upper=True, label="OK1WC"
        )
        if skip:
            ok1wc_write_round_marker(marker, round_info, calls)
            continue
        get_log_session = ok1wc_make_round_session_getter(round_info)
        created = 0
        for call, dest in zip(calls, dests):
            if not ok1wc_should_write_log(dest, round_info):
                continue
            remove_invalid_existing(dest)

            def action(
                round_info=round_info,
                call=call,
                dest=dest,
                get_log_session=get_log_session,
            ) -> Dict[str, int]:
                try:
                    session = get_log_session()
                    log_html = ok1wc_fetch_log_html(session, round_info, call)
                    qsos = ok1wc_parse_qsos(log_html)
                    if not qsos:
                        with PRINT_LOCK:
                            print(f"OK1WC skip (no qsos): {call} {round_info.jobdate}")
                        return {"skip": 1}
                    cab = ok1wc_build_cabrillo(round_info, call, qsos)
                    ok1wc_write_log(dest, cab, overwrite=dest.exists())
                except Exception as exc:  # pylint: disable=broad-except
                    with PRINT_LOCK:
                        print(f"OK1WC fail {call} {round_info.jobdate}: {exc}")
                    return {"error": 1}
                with PRINT_LOCK:
                    print(f"ok   {dest}")
                return {"ok": 1}

            tasks.append(
                DownloadTask(
                    dest=dest,
                    host="memorial-ok1wc.cz",
                    source="OK1WC Memorial",
                    action=action,
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                    output_roots=(OK1WC_OUTPUT_ROOT.as_posix(),),
                )
            )
            created += 1
        if created == 0:
            ok1wc_write_round_marker(marker, round_info, calls)
            task_mark_complete(task_key, list_hash, count)
    with PRINT_LOCK:
        print(f"OK1WC: queued {len(tasks)} call tasks")
    return tasks


# ----- YU DX Contest -----
YUDX_API_BASE = "https://yudx.yu1srs.org.rs"
YUDX_RESULTS_PAGE = urllib.parse.urljoin(YUDX_API_BASE, "/results")
YUDX_OUTPUT_ROOT = Path("YU_DX_Contest")
YUDX_BAND_FREQ = {
    "160M": "1800",
    "80M": "3500",
    "40M": "7000",
    "20M": "14000",
    "15M": "21000",
    "10M": "28000",
}
YUDX_MODE_MAP = {
    "CW": "CW",
    "SSB": "PH",
    "PH": "PH",
    "PHONE": "PH",
    "FM": "FM",
    "RTTY": "RY",
    "RY": "RY",
    "DIGI": "DG",
}


@dataclass
class YUDXResultMeta:
    year: int
    call: str
    result_id: int
    category_code: str
    category_header: str
    check_log: bool
    claimed_qso: int
    total_score: int


@dataclass
class YUDXQSO:
    date: str
    time: str
    freq: str
    mode: str
    own_call: str
    sent_rst: str
    sent_exchange: str
    worked_call: str
    received_rst: str
    received_exchange: str


def yudx_api_json(
    path: str,
    params: Dict[str, object] | None = None,
) -> Dict[str, object]:
    query = ""
    if params:
        query = "?" + urllib.parse.urlencode(params)
    url = urllib.parse.urljoin(YUDX_API_BASE, path) + query
    payload = json.loads(fetch_text(url))
    if not isinstance(payload, dict):
        raise ValueError(f"YU DX API returned non-object payload for {url}")
    if not payload.get("success"):
        raise ValueError(f"YU DX API failed for {url}: {payload}")
    return payload


def yudx_discover_years(last: int | None) -> List[int]:
    payload = yudx_api_json("/api/results/years")
    data = payload.get("data")
    if not isinstance(data, list):
        return []
    years: List[int] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        status_id = int(item.get("status_id") or 0)
        if status_id < 5:
            continue
        match = re.match(r"\s*((?:19|20)\d{2})\b", str(item.get("naziv") or ""))
        if match:
            years.append(int(match.group(1)))
    years = sorted(set(years), reverse=True)
    if last:
        years = years[:last]
    return years


def yudx_int(value: object) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def yudx_clean_value(value: object) -> str:
    text = str(value or "").replace("\xa0", " ")
    return " ".join(text.split()).strip()


def yudx_category_map(results_by_category: object) -> Dict[int, str]:
    category_by_id: Dict[int, str] = {}
    if not isinstance(results_by_category, dict):
        return category_by_id
    for header, rows in results_by_category.items():
        if not isinstance(rows, list):
            continue
        header_text = yudx_clean_value(header)
        for row in rows:
            if isinstance(row, dict):
                result_id = yudx_int(row.get("id"))
                if result_id:
                    category_by_id[result_id] = header_text
    return category_by_id


def yudx_category_lookup(categories: object) -> Dict[Tuple[str, bool], str]:
    lookup: Dict[Tuple[str, bool], str] = {}
    if not isinstance(categories, list):
        return lookup
    for category in categories:
        header = yudx_clean_value(category)
        match = re.match(r"^([A-Z])(\.)?(?=\s|\(|$)", header)
        if not match:
            continue
        lookup[(match.group(1), bool(match.group(2)))] = header
    return lookup


def yudx_category_from_row(
    row: Dict[str, object],
    category_by_id: Dict[int, str],
    category_lookup: Dict[Tuple[str, bool], str],
) -> str:
    result_id = yudx_int(row.get("id"))
    if result_id in category_by_id:
        return category_by_id[result_id]
    category_code = yudx_clean_value(row.get("category")).upper().rstrip(".")
    call = yudx_clean_value(row.get("callsign")).upper()
    prefer_out_of_serbia = not (call.startswith("YU") or call.startswith("YT"))
    return (
        category_lookup.get((category_code, prefer_out_of_serbia))
        or category_lookup.get((category_code, False))
        or category_lookup.get((category_code, True))
        or category_code
    )


def yudx_result_metas(year: int) -> List[YUDXResultMeta]:
    payload = yudx_api_json(f"/api/results/{year}")
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    category_by_id = yudx_category_map(data.get("results_by_category"))
    category_lookup = yudx_category_lookup(data.get("categories"))
    rows: List[object] = []
    for key in ("normal_results", "check_log_results"):
        raw_rows = data.get(key)
        if isinstance(raw_rows, list):
            rows.extend(raw_rows)
    metas: List[YUDXResultMeta] = []
    seen_calls: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        call = yudx_clean_value(row.get("callsign")).upper()
        if not call or call in seen_calls:
            continue
        seen_calls.add(call)
        result_id = yudx_int(row.get("id"))
        category_code = yudx_clean_value(row.get("category"))
        category_header = yudx_category_from_row(row, category_by_id, category_lookup)
        metas.append(
            YUDXResultMeta(
                year=year,
                call=call,
                result_id=result_id,
                category_code=category_code,
                category_header=category_header,
                check_log=yudx_clean_value(row.get("check_log")).upper() == "Y",
                claimed_qso=yudx_int(row.get("claimed_qso")),
                total_score=yudx_int(row.get("total_score")),
            )
        )
    return metas


def yudx_fetch_qso_rows(year: int, call: str) -> List[Dict[str, object]]:
    payload = yudx_api_json(
        "/api/sveveze",
        {"calluces": call, "takmicenje_id": year},
    )
    data = payload.get("data")
    if not isinstance(data, list):
        return []
    return [row for row in data if isinstance(row, dict)]


def yudx_year_has_qso_rows(year: int, metas: List[YUDXResultMeta]) -> bool:
    for meta in metas[:12]:
        try:
            if yudx_fetch_qso_rows(year, meta.call):
                return True
        except Exception:  # pylint: disable=broad-except
            return True
    return False


def yudx_band_to_freq(band: str) -> str:
    normalized = yudx_clean_value(band).upper().replace(" ", "")
    normalized = normalized.replace("MHZ", "M")
    normalized = normalized.replace("3,5M", "80M").replace("3.5M", "80M")
    if normalized in YUDX_BAND_FREQ:
        return YUDX_BAND_FREQ[normalized]
    match = re.match(r"^(\d+(?:[.,]\d+)?)M$", normalized)
    if match:
        mhz = float(match.group(1).replace(",", "."))
        return str(int(mhz * 1000))
    return re.sub(r"\D", "", normalized) or "0"


def yudx_time_to_hhmm(value: object) -> str:
    match = re.search(r"(\d{1,2}):(\d{2})", str(value or ""))
    if not match:
        return "0000"
    return f"{int(match.group(1)):02d}{int(match.group(2)):02d}"


def yudx_parse_qsos(rows: List[Dict[str, object]]) -> List[YUDXQSO]:
    qsos: List[YUDXQSO] = []
    for row in rows:
        own_call = yudx_clean_value(row.get("calluces") or row.get("znak")).upper()
        worked_call = yudx_clean_value(row.get("callradjen")).upper()
        date_text = yudx_clean_value(row.get("datum"))
        mode_text = yudx_clean_value(row.get("mode")).upper()
        if not own_call or not worked_call or not date_text:
            continue
        qsos.append(
            YUDXQSO(
                date=date_text,
                time=yudx_time_to_hhmm(row.get("vreme")),
                freq=yudx_band_to_freq(yudx_clean_value(row.get("band"))),
                mode=YUDX_MODE_MAP.get(mode_text, mode_text or "CW"),
                own_call=own_call,
                sent_rst=yudx_clean_value(row.get("snd_rprt")) or "599",
                sent_exchange=yudx_clean_value(row.get("snd_rbr")),
                worked_call=worked_call,
                received_rst=yudx_clean_value(row.get("rec_rprt")) or "599",
                received_exchange=yudx_clean_value(row.get("rec_rbr")),
            )
        )
    return qsos


def yudx_derive_category_fields(meta: YUDXResultMeta) -> Dict[str, str]:
    header = meta.category_header.upper().replace("\xa0", " ")
    operator = "CHECKLOG" if meta.check_log else "SINGLE-OP"
    if not meta.check_log and ("MOST" in header or "MULTI" in header):
        operator = "MULTI-OP"
    band = "ALL"
    band_match = re.search(r"(\d+(?:[.,]\d+)?)\s*MHZ", header)
    if band_match:
        mhz = float(band_match.group(1).replace(",", "."))
        band = {
            3.5: "80M",
            7.0: "40M",
            14.0: "20M",
            21.0: "15M",
            28.0: "10M",
        }.get(mhz, band)
    mode = "MIXED"
    if "CW" in header and "SSB" not in header and "MIXED" not in header:
        mode = "CW"
    elif "SSB" in header and "CW" not in header and "MIXED" not in header:
        mode = "PH"
    power = ""
    if "QRP" in header:
        power = "QRP"
    elif "-LP" in header or "LOW" in header or "<= 100" in header:
        power = "LOW"
    elif "-HP" in header or "HIGH" in header or "<= 1500" in header:
        power = "HIGH"
    return {
        "operator": operator,
        "band": band,
        "mode": mode,
        "power": power,
    }


def yudx_build_cabrillo(meta: YUDXResultMeta, qsos: List[YUDXQSO]) -> str:
    station_call = qsos[0].own_call if qsos else meta.call
    category_fields = yudx_derive_category_fields(meta)
    source_url = f"{YUDX_RESULTS_PAGE}?year={meta.year}"
    lines = [
        "START-OF-LOG: 3.0",
        "CREATED-BY: HMRA public YU DX downloader",
        "CONTEST: YU-DX-CONTEST",
        f"CALLSIGN: {station_call}",
        f"OPERATORS: {station_call}",
        f"CATEGORY: {meta.category_header}",
        f"CATEGORY-OPERATOR: {category_fields['operator']}",
        f"CATEGORY-BAND: {category_fields['band']}",
        f"CATEGORY-MODE: {category_fields['mode']}",
    ]
    if category_fields["power"]:
        lines.append(f"CATEGORY-POWER: {category_fields['power']}")
    if meta.check_log:
        lines.append("CHECKLOG: YES")
    lines.extend(
        [
            f"SOAPBOX: Recreated from YU DX public QSO table for {meta.year}.",
            "SOAPBOX: Public Error Summary rows are ignored; QSO rows include adjudication status.",
            f"SOAPBOX: Source: {source_url}",
        ]
    )
    if meta.total_score:
        lines.append(f"SOAPBOX: Published final score: {meta.total_score}.")
    for qso in qsos:
        lines.append(
            f"QSO: {qso.freq:>5} {qso.mode:<2} {qso.date} {qso.time:>4} "
            f"{qso.own_call:<13} {qso.sent_rst:<3} {qso.sent_exchange:<10} "
            f"{qso.worked_call:<13} {qso.received_rst:<3} {qso.received_exchange:<10}"
        )
    lines.append("END-OF-LOG:")
    return "\n".join(lines) + "\n"


def yudx_dest_path(year: int, call: str) -> Path:
    safe_call = re.sub(r"[^A-Za-z0-9_.-]+", "_", call)
    return YUDX_OUTPUT_ROOT / str(year) / f"{safe_call}.log"


def yudx_write_log(dest: Path, content: str) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(content, encoding="utf-8")


def tasks_yudx(last: int | None) -> List[DownloadTask]:
    tasks: List[DownloadTask] = []
    with PRINT_LOCK:
        print("YUDX: discovering public result years...")
    years = yudx_discover_years(last)
    for year in years:
        try:
            metas = yudx_result_metas(year)
        except Exception as exc:  # pylint: disable=broad-except
            with PRINT_LOCK:
                print(f"YUDX list failed {year}: {exc}")
            continue
        if not metas:
            continue
        if not yudx_year_has_qso_rows(year, metas):
            with PRINT_LOCK:
                print(
                    f"YUDX skip {year}: result tables exist, "
                    "but public QSO API returns no rows"
                )
            continue
        task_key = f"YU_DX_Contest/{year}"
        task_items = [
            f"{meta.call}\t{meta.result_id}\t{meta.category_header}\t{meta.claimed_qso}"
            for meta in metas
        ]
        dests = [yudx_dest_path(year, meta.call) for meta in metas]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, task_items, dests, upper=True, label="YUDX"
        )
        if skip:
            continue
        created = 0
        for meta, dest in zip(metas, dests):
            if valid_existing_log(dest):
                continue
            remove_invalid_existing(dest)

            def action(meta=meta, dest=dest) -> Dict[str, int]:
                try:
                    rows = yudx_fetch_qso_rows(meta.year, meta.call)
                    qsos = yudx_parse_qsos(rows)
                    if not qsos:
                        with PRINT_LOCK:
                            print(f"YUDX skip (no qsos): {meta.call} {meta.year}")
                        return {"skip": 1}
                    cab = yudx_build_cabrillo(meta, qsos)
                    if valid_existing_log(dest):
                        with PRINT_LOCK:
                            print(f"skip (exists): {dest}")
                        return {"skip": 1}
                    remove_invalid_existing(dest)
                    yudx_write_log(dest, cab)
                except Exception as exc:  # pylint: disable=broad-except
                    with PRINT_LOCK:
                        print(f"YUDX fail {meta.call} {meta.year}: {exc}")
                    return {"error": 1}
                with PRINT_LOCK:
                    print(f"ok   {dest}")
                return {"ok": 1}

            tasks.append(
                DownloadTask(
                    dest=dest,
                    host="yudx.yu1srs.org.rs",
                    source="YU DX Contest",
                    action=action,
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    with PRINT_LOCK:
        print(f"YUDX: queued {len(tasks)} call logs")
    return tasks


# ----- 9A HRS contests (HF Robot) -----
HRS_HF_BASE_URL = "https://www.hamradio.hr/hfrobot/"
HRS_HF_ARCHIVE_URL = urllib.parse.urljoin(HRS_HF_BASE_URL, "index.php?k=a")
HRS_HF_OUTPUT_ROOT = Path("9A_HRS_Contest")


@dataclass(frozen=True)
class HRSRound:
    cid: str
    name: str
    slug: str
    year: int
    status: str
    logs: int
    uploaded_qsos: int
    check_logs: int


@dataclass(frozen=True)
class HRSLogMeta:
    call: str
    contest: str
    category: str
    category_label: str
    claimed_qsos: int
    claimed_score: int
    checklog: bool


@dataclass(frozen=True)
class HRSCallInfo:
    call: str
    category_label: str
    log_view: str
    checklog: bool


@dataclass(frozen=True)
class HRSQSO:
    freq: str
    mode: str
    date: str
    time: str
    own_call: str
    sent_rst: str
    sent_exchange: str
    worked_call: str
    received_rst: str
    received_exchange: str


def hrs_clean_html_text(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text).replace("\xa0", " ")
    return " ".join(text.split()).strip()


def hrs_ascii_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", value)
    text = text.encode("ascii", "ignore").decode("ascii")
    return " ".join(text.split()).strip()


def hrs_clean_slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    normalized = normalized.encode("ascii", "ignore").decode("ascii").lower()
    if "croatian dx contest" in normalized:
        return "Croatian_DX_Contest"
    if "croatian cw contest" in normalized:
        return "Croatian_CW_Contest"
    if "hrvatski radioamaterski kup" in normalized:
        return "Hrvatski_Radioamaterski_Kup"
    if "zimski kv kup" in normalized:
        return "Zimski_KV_Kup"
    if "kup jadrana" in normalized:
        return "Kup_Jadrana"
    text = re.sub(r"\b(?:19|20)\d{2}\b", "", value)
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_")
    return text or "Unknown"


def hrs_safe_call(call: str) -> str:
    safe = call.upper().replace("/", "_")
    safe = re.sub(r"[^A-Z0-9_.-]+", "_", safe).strip("._")
    return safe or "UNKNOWN"


def hrs_int(value: str) -> int:
    match = re.search(r"\d+", value.replace(".", "").replace(",", ""))
    return int(match.group(0)) if match else 0


def hrs_is_final_status(status: str) -> bool:
    normalized = unicodedata.normalize("NFKD", status)
    normalized = normalized.encode("ascii", "ignore").decode("ascii").lower()
    return "sluzbeni" in normalized or "final" in normalized or "official" in normalized


def hrs_looks_like_call(value: str) -> bool:
    text = value.strip().upper()
    if not text or " " in text:
        return False
    return bool(re.search(r"[A-Z]", text) and re.search(r"\d", text))


def hrs_mode(value: str) -> str:
    raw = value.strip().upper()
    mapped = YUDX_MODE_MAP.get(raw) or normalize_log_mode(raw)
    if mapped and mapped != "MIXED":
        return mapped
    safe = re.sub(r"[^A-Z0-9]", "", raw)
    return safe[:4] or "CW"


def hrs_header_token(value: str, fallback: str) -> str:
    text = hrs_clean_html_text(value).upper()
    text = re.sub(r"[^A-Z0-9_-]+", "-", text).strip("-")
    return text or fallback


def hrs_discover_rounds(last: int | None) -> List[HRSRound]:
    page = fetch_text(HRS_HF_ARCHIVE_URL, retries=5, delay=2.0)
    rounds: Dict[str, HRSRound] = {}
    for row in re.findall(r"<TR[^>]*>(.*?)</TR>", page, flags=re.IGNORECASE | re.DOTALL):
        row_unescaped = html.unescape(row)
        link_match = re.search(
            r'href=["\']index\.php\?What=p&CID=([^"\']+)["\'][^>]*>(.*?)</a>',
            row_unescaped,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not link_match:
            continue
        cid = link_match.group(1).strip()
        if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", cid):
            continue
        cells = [
            hrs_clean_html_text(cell)
            for cell in re.findall(r"<TD[^>]*>(.*?)</TD>", row, flags=re.IGNORECASE | re.DOTALL)
        ]
        if len(cells) < 10:
            continue
        name = hrs_clean_html_text(link_match.group(2))
        status = cells[4]
        if not hrs_is_final_status(status):
            continue
        year = int(cid[:4])
        rounds[cid] = HRSRound(
            cid=cid,
            name=name,
            slug=hrs_clean_slug(name),
            year=year,
            status=status,
            logs=hrs_int(cells[7]),
            uploaded_qsos=hrs_int(cells[8]),
            check_logs=hrs_int(cells[9]),
        )
    selected = sorted(rounds.values(), key=lambda item: item.cid, reverse=True)
    if last:
        years = sorted({round.year for round in selected}, reverse=True)[:last]
        selected_years = set(years)
        selected = [round for round in selected if round.year in selected_years]
    return selected


def hrs_round_url(round_info: HRSRound) -> str:
    return urllib.parse.urljoin(
        HRS_HF_BASE_URL,
        "index.php?" + urllib.parse.urlencode({"What": "p", "CID": round_info.cid}),
    )


def hrs_log_url(cid: str, call: str, log_view: str = "vloghtm") -> str:
    return urllib.parse.urljoin(
        HRS_HF_BASE_URL,
        "hfcc_log.php?" + urllib.parse.urlencode({"What": log_view, "CID": cid, "ID": call}),
    )


def hrs_discover_calls(round_info: HRSRound) -> List[str]:
    return [info.call for info in hrs_discover_call_infos(round_info)]


def hrs_extract_log_links(segment: str, cid: str, category_label: str) -> Dict[str, HRSCallInfo]:
    calls: Dict[str, HRSCallInfo] = {}
    for href in re.findall(
        r'href=["\']([^"\']*hfcc_log\.php\?[^"\']*What=vloghtm(?:oc)?[^"\']*)',
        segment,
        flags=re.IGNORECASE,
    ):
        parsed = urllib.parse.urlparse(html.unescape(href))
        params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        if (params.get("CID") or [""])[0] != cid:
            continue
        call = (params.get("ID") or [""])[0].strip().upper()
        log_view = (params.get("What") or ["vloghtm"])[0].strip() or "vloghtm"
        if call:
            calls[hrs_safe_call(call)] = HRSCallInfo(
                call=call,
                category_label=category_label,
                log_view=log_view,
                checklog=log_view.lower() != "vloghtm",
            )
    return calls


def hrs_check_logs_url(round_info: HRSRound) -> str:
    return urllib.parse.urljoin(
        HRS_HF_BASE_URL,
        "view_hf_check.php?" + urllib.parse.urlencode({"CID": round_info.cid}),
    )


def hrs_discover_call_infos(round_info: HRSRound) -> List[HRSCallInfo]:
    page = html.unescape(fetch_text(hrs_round_url(round_info), retries=5, delay=2.0))
    calls: Dict[str, HRSCallInfo] = {}
    sections = list(
        re.finditer(
            r"<P\b[^>]*>\s*<b>(.*?)</b>\s*</P>",
            page,
            flags=re.IGNORECASE | re.DOTALL,
        )
    )
    if not sections:
        calls.update(hrs_extract_log_links(page, round_info.cid, ""))
    else:
        for index, match in enumerate(sections):
            category_label = hrs_clean_html_text(match.group(1))
            next_start = sections[index + 1].start() if index + 1 < len(sections) else len(page)
            segment = page[match.end() : next_start]
            calls.update(hrs_extract_log_links(segment, round_info.cid, category_label))

    if not calls:
        calls.update(hrs_extract_log_links(page, round_info.cid, ""))
    if round_info.check_logs:
        try:
            check_page = html.unescape(fetch_text(hrs_check_logs_url(round_info), retries=5, delay=2.0))
            calls.update(hrs_extract_log_links(check_page, round_info.cid, "CHECKLOG"))
        except Exception as exc:  # pylint: disable=broad-except
            with PRINT_LOCK:
                print(f"9A HRS checklog list failed {round_info.cid}: {exc}")
    return [calls[key] for key in sorted(calls)]


def hrs_parse_log_meta(page: str, fallback_call: str) -> HRSLogMeta:
    values: Dict[str, str] = {}
    for row in re.findall(r"<TR[^>]*>(.*?)</TR>", page, flags=re.IGNORECASE | re.DOTALL):
        cells = [
            hrs_clean_html_text(cell)
            for cell in re.findall(r"<TD[^>]*>(.*?)</TD>", row, flags=re.IGNORECASE | re.DOTALL)
        ]
        if len(cells) == 2:
            values[cells[0].strip().upper()] = cells[1].strip()
    return HRSLogMeta(
        call=values.get("CALL", fallback_call).upper(),
        contest=values.get("CONTEST", "9A-HRS"),
        category=values.get("CATEGORY", "UNKNOWN"),
        category_label="",
        claimed_qsos=hrs_int(values.get("QSOS", "")),
        claimed_score=hrs_int(values.get("SCOR", "") or values.get("SCORE", "")),
        checklog=False,
    )


def hrs_parse_qsos(page: str) -> List[HRSQSO]:
    qsos: List[HRSQSO] = []
    for row in re.findall(r"<TR[^>]*>(.*?)</TR>", page, flags=re.IGNORECASE | re.DOTALL):
        cells = [
            hrs_clean_html_text(cell)
            for cell in re.findall(r"<TD[^>]*>(.*?)</TD>", row, flags=re.IGNORECASE | re.DOTALL)
        ]
        if len(cells) < 12 or not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", cells[2]):
            continue
        offset = (
            1
            if len(cells) >= 13
            and not hrs_looks_like_call(cells[4])
            and hrs_looks_like_call(cells[5])
            else 0
        )
        own_call = cells[4 + offset].upper()
        worked_call = cells[8 + offset].upper()
        if not own_call or not worked_call:
            continue
        sent_exchange = " ".join(part for part in (cells[6 + offset], cells[7 + offset]) if part)
        received_exchange = " ".join(part for part in (cells[10 + offset], cells[11 + offset]) if part)
        qsos.append(
            HRSQSO(
                freq=re.sub(r"\D", "", cells[0]) or "0",
                mode=hrs_mode(cells[1]),
                date=cells[2],
                time=re.sub(r"\D", "", cells[3]).zfill(4)[-4:],
                own_call=own_call,
                sent_rst=cells[5 + offset] or "599",
                sent_exchange=sent_exchange,
                worked_call=worked_call,
                received_rst=cells[9 + offset] or "599",
                received_exchange=received_exchange,
            )
        )
    return qsos


def hrs_freq_band(freq: str) -> str | None:
    try:
        khz = int(freq)
    except ValueError:
        return None
    if 1800 <= khz <= 2000:
        return "160M"
    if 3500 <= khz <= 4000:
        return "80M"
    if 7000 <= khz <= 7300:
        return "40M"
    if 14000 <= khz <= 14350:
        return "20M"
    if 21000 <= khz <= 21450:
        return "15M"
    if 28000 <= khz <= 29700:
        return "10M"
    return None


def hrs_category_fields(meta: HRSLogMeta, qsos: List[HRSQSO]) -> Dict[str, str]:
    category_text = f"{meta.category} {meta.category_label}".upper()
    normalized = unicodedata.normalize("NFKD", category_text)
    normalized = normalized.encode("ascii", "ignore").decode("ascii").upper()
    operator = "SINGLE-OP"
    if meta.checklog or "CHECK" in normalized:
        operator = "CHECKLOG"
    elif "MULTI" in normalized or "VISE OPERATORA" in normalized or "VISEOPERATORA" in normalized:
        operator = "MULTI-OP"

    modes = {normalize_log_mode(qso.mode) or qso.mode for qso in qsos if qso.mode}
    if "MIXED" in modes or len(modes) > 1:
        mode = "MIXED"
    elif modes:
        mode = next(iter(modes))
    else:
        mode = "MIXED"
    bands = {band for qso in qsos for band in [hrs_freq_band(qso.freq)] if band}
    band = next(iter(bands)) if len(bands) == 1 else "ALL"
    power = ""
    if "QRP" in normalized:
        power = "QRP"
    elif "LOW POWER" in normalized or "MALA SNAGA" in normalized:
        power = "LOW"
    elif "HIGH POWER" in normalized or "VELIKA SNAGA" in normalized:
        power = "HIGH"
    return {"operator": operator, "band": band, "mode": mode, "power": power}


def hrs_build_cabrillo(
    round_info: HRSRound,
    meta: HRSLogMeta,
    qsos: List[HRSQSO],
    log_view: str = "vloghtm",
) -> str:
    station_call = qsos[0].own_call if qsos else meta.call
    fields = hrs_category_fields(meta, qsos)
    contest = hrs_header_token(meta.contest, "9A-HRS")
    category = hrs_ascii_text(meta.category or "UNKNOWN")
    lines = [
        "START-OF-LOG: 3.0",
        "CREATED-BY: HMRA public 9A HRS HF Robot downloader",
        f"CONTEST: {contest}",
        f"CALLSIGN: {station_call}",
        f"OPERATORS: {station_call}",
        f"CATEGORY: {category}",
        f"CATEGORY-OPERATOR: {fields['operator']}",
        f"CATEGORY-BAND: {fields['band']}",
        f"CATEGORY-MODE: {fields['mode']}",
    ]
    if fields["power"]:
        lines.append(f"CATEGORY-POWER: {fields['power']}")
    if fields["operator"] == "CHECKLOG":
        lines.append("CHECKLOG: YES")
    if meta.claimed_score:
        lines.append(f"CLAIMED-SCORE: {meta.claimed_score}")
    lines.extend(
        [
            f"SOAPBOX: Recreated from 9A HRS HF Robot public QSO table for {round_info.name}.",
            "SOAPBOX: Public error-summary rows are ignored.",
            f"SOAPBOX: Source: {hrs_log_url(round_info.cid, meta.call, log_view)}",
        ]
    )
    if meta.category_label:
        lines.append(f"SOAPBOX: Source category label: {hrs_ascii_text(meta.category_label)}.")
    if meta.claimed_qsos:
        lines.append(f"SOAPBOX: Published QSO count: {meta.claimed_qsos}.")
        if len(qsos) != meta.claimed_qsos:
            lines.append(f"SOAPBOX: Parsed QSO count: {len(qsos)}.")
    for qso in qsos:
        lines.append(
            f"QSO: {qso.freq:>5} {qso.mode:<2} {qso.date} {qso.time:>4} "
            f"{qso.own_call:<13} {qso.sent_rst:<3} {qso.sent_exchange:<10} "
            f"{qso.worked_call:<13} {qso.received_rst:<3} {qso.received_exchange:<10}"
        )
    lines.append("END-OF-LOG:")
    return "\n".join(lines) + "\n"


def hrs_dest_path(round_info: HRSRound, call: str) -> Path:
    return HRS_HF_OUTPUT_ROOT / round_info.slug / str(round_info.year) / f"{hrs_safe_call(call)}.log"


def tasks_hrs_hf(last: int | None) -> List[DownloadTask]:
    tasks: List[DownloadTask] = []
    try:
        rounds = hrs_discover_rounds(last)
    except Exception as exc:  # pylint: disable=broad-except
        with PRINT_LOCK:
            print(f"9A HRS discovery failed: {exc}")
        return tasks
    with PRINT_LOCK:
        print(f"9A HRS: discovered {len(rounds)} official rounds")
    for round_info in rounds:
        try:
            call_infos = hrs_discover_call_infos(round_info)
        except Exception as exc:  # pylint: disable=broad-except
            with PRINT_LOCK:
                print(f"9A HRS list failed {round_info.cid}: {exc}")
            continue
        if not call_infos:
            with PRINT_LOCK:
                print(f"9A HRS skip (no calls): {round_info.cid}")
            continue
        with PRINT_LOCK:
            print(
                f"9A HRS {round_info.cid}: {len(call_infos)} public logs "
                f"(archive logs={round_info.logs}, check={round_info.check_logs})"
            )
        task_key = f"9A_HRS_Contest/{round_info.slug}/{round_info.cid}"
        task_items = [
            f"{info.call}\t{info.category_label}\t{info.log_view}\t{hrs_log_url(round_info.cid, info.call, info.log_view)}"
            for info in call_infos
        ]
        dests = [hrs_dest_path(round_info, info.call) for info in call_infos]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, task_items, dests, upper=True, label="9A HRS"
        )
        if skip:
            continue
        created = 0
        for info, dest in zip(call_infos, dests):
            if valid_existing_log(dest):
                continue
            remove_invalid_existing(dest)

            def action(round_info=round_info, info=info, dest=dest) -> Dict[str, int]:
                try:
                    page = fetch_text(hrs_log_url(round_info.cid, info.call, info.log_view), retries=6, delay=2.0)
                    parsed_meta = hrs_parse_log_meta(page, info.call)
                    meta = HRSLogMeta(
                        call=parsed_meta.call,
                        contest=parsed_meta.contest,
                        category=parsed_meta.category,
                        category_label=info.category_label or parsed_meta.category_label,
                        claimed_qsos=parsed_meta.claimed_qsos,
                        claimed_score=parsed_meta.claimed_score,
                        checklog=info.checklog or parsed_meta.checklog,
                    )
                    qsos = hrs_parse_qsos(page)
                    if not qsos:
                        with PRINT_LOCK:
                            print(f"9A HRS skip (no qsos): {info.call} {round_info.cid}")
                        return {"skip": 1}
                    if meta.claimed_qsos and len(qsos) != meta.claimed_qsos:
                        with PRINT_LOCK:
                            print(
                                f"9A HRS warn {info.call} {round_info.cid}: "
                                f"parsed {len(qsos)} qsos, header says {meta.claimed_qsos}"
                            )
                    cab = hrs_build_cabrillo(round_info, meta, qsos, info.log_view)
                    if valid_existing_log(dest):
                        with PRINT_LOCK:
                            print(f"skip (exists): {dest}")
                        return {"skip": 1}
                    remove_invalid_existing(dest)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_text(cab, encoding="utf-8")
                except Exception as exc:  # pylint: disable=broad-except
                    with PRINT_LOCK:
                        print(f"9A HRS fail {info.call} {round_info.cid}: {exc}")
                    return {"error": 1}
                with PRINT_LOCK:
                    print(f"ok   {dest}")
                return {"ok": 1}

            tasks.append(
                DownloadTask(
                    dest=dest,
                    host="www.hamradio.hr",
                    source="9A HRS Contest",
                    action=action,
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                    output_roots=(HRS_HF_OUTPUT_ROOT.as_posix(),),
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    with PRINT_LOCK:
        print(f"9A HRS: queued {len(tasks)} reconstructed logs")
    return tasks


# ----- SAC Scandinavian Activity Contest -----
SAC_PUBLIC_LOGS_URL = "https://www.sactest.net/publiclogs/"
SAC_OUTPUT_ROOT = Path("SAC")
SAC_MODES = {"cw": "CW", "ssb": "SSB"}


@dataclass(frozen=True)
class SACLogLink:
    year: int
    mode: str
    call: str
    url: str


def sac_public_log_index_url(year: int, mode: str) -> str:
    return f"{SAC_PUBLIC_LOGS_URL}index.php/?year={year}&mode={mode.lower()}"


def sac_discover_year_modes(last: int | None) -> List[Tuple[int, str]]:
    page = fetch_text(SAC_PUBLIC_LOGS_URL)
    pairs: set[Tuple[int, str]] = set()
    for year_text, mode_text in re.findall(
        r"year=(20\d{2})(?:&|&amp;|&#0?38;)mode=([a-z]+)",
        page,
        flags=re.IGNORECASE,
    ):
        year = int(year_text)
        mode = mode_text.lower()
        if mode in SAC_MODES:
            pairs.add((year, mode))
    years = sorted({year for year, _mode in pairs}, reverse=True)
    if last:
        years = years[:last]
    selected_years = set(years)
    return sorted(
        ((year, mode) for year, mode in pairs if year in selected_years),
        key=lambda item: (-item[0], item[1]),
    )


def sac_safe_call(call: str) -> str:
    safe = call.upper().replace("/", "_")
    safe = re.sub(r"[^A-Z0-9_.-]+", "_", safe).strip("._")
    return safe or "UNKNOWN"


def sac_discover_logs(year: int, mode: str) -> List[SACLogLink]:
    page_url = sac_public_log_index_url(year, mode)
    page = fetch_text(page_url)
    links: Dict[str, SACLogLink] = {}
    for href_double, href_single, href_plain, label in re.findall(
        r"<a\s+href=(?:\"([^\"]+?\.log)\"|'([^']+?\.log)'|([^ >]+?\.log))\s*>(.*?)</a>",
        page,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        raw_href = href_double or href_single or href_plain
        call = re.sub(r"<[^>]+>", "", label)
        call = html.unescape(call).strip().upper()
        if not call:
            call = Path(urllib.parse.urlparse(raw_href).path).stem.upper()
        url = urllib.parse.urljoin(page_url, html.unescape(raw_href))
        safe_call = sac_safe_call(call)
        links[safe_call] = SACLogLink(year=year, mode=mode.lower(), call=call, url=url)
    return [links[key] for key in sorted(links)]


def tasks_sac(last: int | None) -> List[DownloadTask]:
    tasks: List[DownloadTask] = []
    try:
        year_modes = sac_discover_year_modes(last)
    except Exception as exc:  # pylint: disable=broad-except
        with PRINT_LOCK:
            print(f"SAC discovery failed: {exc}")
        return tasks
    for year, mode in year_modes:
        try:
            logs = sac_discover_logs(year, mode)
        except Exception as exc:  # pylint: disable=broad-except
            with PRINT_LOCK:
                print(f"SAC list failed {year} {mode.upper()}: {exc}")
            continue
        if not logs:
            with PRINT_LOCK:
                print(f"SAC skip (no logs): {year} {mode.upper()}")
            continue
        task_key = f"SAC/{SAC_MODES[mode]}/{year}"
        task_items = [f"{log.call}\t{log.url}" for log in logs]
        dests = [
            SAC_OUTPUT_ROOT / SAC_MODES[mode] / str(year) / f"{sac_safe_call(log.call)}.log"
            for log in logs
        ]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, task_items, dests, upper=True, label="SAC"
        )
        if skip:
            continue
        created = 0
        for log, dest in zip(logs, dests):
            if valid_existing_log(dest):
                continue
            remove_invalid_existing(dest)
            tasks.append(
                make_http_task(
                    dest,
                    log.url,
                    source="SAC",
                    retries=8,
                    delay=3.0,
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    with PRINT_LOCK:
        print(f"SAC: queued {len(tasks)} public Cabrillo logs")
    return tasks


# ----- URE public logs -----
URE_PUBLIC_LOGS_URL = "https://concursos.ure.es/en/logs-publicos/"
URE_OUTPUT_ROOT = Path("URE")
URE_CONTEST_NAMES = {
    "eapsk": "EAPSK63",
    "eartty": "EARTTY",
    "smrcw": "SMRCW",
    "smrssb": "SMRSSB",
    "cncw": "CNCW",
    "cme": "CME",
}


@dataclass(frozen=True)
class UREContestLink:
    code: str
    year: int
    label: str


@dataclass(frozen=True)
class URELogLink:
    year: int
    contest: str
    call: str
    url: str


def ure_public_log_page_url(code: str, year: int) -> str:
    return f"{URE_PUBLIC_LOGS_URL}?c={code}-{year}"


def ure_safe_call(call: str) -> str:
    safe = call.upper().replace("/", "_")
    safe = re.sub(r"[^A-Z0-9_.-]+", "_", safe).strip("._")
    return safe or "UNKNOWN"


def ure_discover_contests(last: int | None) -> List[UREContestLink]:
    page = fetch_text(URE_PUBLIC_LOGS_URL)
    contests: Dict[Tuple[str, int], UREContestLink] = {}
    for code, year_text, label in re.findall(
        r"<a\s+href=[\"']\?c=([a-z0-9]+)-(20\d{2})[\"'][^>]*>(.*?)</a>",
        page,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        code = code.lower()
        if code not in URE_CONTEST_NAMES:
            continue
        year = int(year_text)
        clean_label = html.unescape(re.sub(r"<[^>]+>", "", label)).strip()
        contests[(code, year)] = UREContestLink(code=code, year=year, label=clean_label)
    years = sorted({year for _code, year in contests}, reverse=True)
    if last:
        years = years[:last]
    selected_years = set(years)
    return [
        contests[key]
        for key in sorted(contests, key=lambda item: (-item[1], item[0]))
        if key[1] in selected_years
    ]


def ure_discover_logs(contest: UREContestLink) -> List[URELogLink]:
    page_url = ure_public_log_page_url(contest.code, contest.year)
    page = fetch_text(page_url)
    logs: Dict[Tuple[str, str], URELogLink] = {}
    for href_double, href_single, href_plain, label in re.findall(
        r"<a\s+[^>]*href=(?:\"([^\"]*?/public-logs/[^\"]+?\.log)\"|'([^']*?/public-logs/[^']+?\.log)'|([^ >]*?/public-logs/[^ >]+?\.log))[^>]*>(.*?)</a>",
        page,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        raw_href = href_double or href_single or href_plain
        url = urllib.parse.urljoin(page_url, html.unescape(raw_href))
        path_parts = [part for part in urllib.parse.urlparse(url).path.split("/") if part]
        if len(path_parts) < 4:
            continue
        try:
            year = int(path_parts[-3])
        except ValueError:
            year = contest.year
        contest_dir = path_parts[-2].upper()
        call = html.unescape(re.sub(r"<[^>]+>", "", label)).strip().upper()
        if not call:
            call = Path(path_parts[-1]).stem.upper()
        logs[(contest_dir, ure_safe_call(call))] = URELogLink(
            year=year,
            contest=contest_dir,
            call=call,
            url=url,
        )
    return [logs[key] for key in sorted(logs)]


def tasks_ure(last: int | None) -> List[DownloadTask]:
    tasks: List[DownloadTask] = []
    try:
        contests = ure_discover_contests(last)
    except Exception as exc:  # pylint: disable=broad-except
        with PRINT_LOCK:
            print(f"URE discovery failed: {exc}")
        return tasks
    for contest in contests:
        try:
            logs = ure_discover_logs(contest)
        except Exception as exc:  # pylint: disable=broad-except
            with PRINT_LOCK:
                print(f"URE list failed {contest.code}-{contest.year}: {exc}")
            continue
        if not logs:
            with PRINT_LOCK:
                print(f"URE skip (no logs): {contest.code}-{contest.year}")
            continue
        task_key = f"URE/{contest.code.upper()}/{contest.year}"
        task_items = [f"{log.contest}\t{log.call}\t{log.url}" for log in logs]
        dests = [
            URE_OUTPUT_ROOT / log.contest / str(log.year) / f"{ure_safe_call(log.call)}.log"
            for log in logs
        ]
        skip, list_hash, count = task_should_skip_known_outputs(
            task_key, task_items, dests, upper=True, label="URE"
        )
        if skip:
            continue
        created = 0
        for log, dest in zip(logs, dests):
            if valid_existing_log(dest):
                continue
            remove_invalid_existing(dest)
            tasks.append(
                make_http_task(
                    dest,
                    log.url,
                    source="URE",
                    retries=6,
                    delay=2.0,
                    task_key=task_key,
                    task_hash=list_hash,
                    task_count=count,
                )
            )
            created += 1
        if created == 0:
            task_mark_complete(task_key, list_hash, count)
    with PRINT_LOCK:
        print(f"URE: queued {len(tasks)} public Cabrillo logs")
    return tasks


# ----- Menu / main -----
ProviderFn = Callable[[int | None], List[DownloadTask]]

PROVIDERS: Dict[int, Tuple[str, ProviderFn]] = {
    1: ("CQWW (PH/CW)", tasks_cqww),
    2: ("CQWPX (PH/CW)", tasks_cqwpx),
    3: ("CQWW RTTY", tasks_cqwwrtty),
    4: ("CQ 160 (PH/CW)", tasks_cq160),
    5: ("CQWPX RTTY", tasks_cqwpxrtty),
    6: ("ARRL contests (all)", tasks_arrl),
    7: ("ZRS KVP (pomlad/jesen)", tasks_zrs_kvp),
    8: ("EUHFC (reconstructed from UBN)", tasks_euhfc),
    9: ("WAE (CW/SSB/RTTY)", tasks_wae),
    10: ("VHFManager contests", tasks_vhfmanager),
    11: ("Wednesday Mini-Test 40m (UA9QCQ UBN)", tasks_wed_minitest_40m),
    12: ("Russian DX Contest (UA9QCQ UBN)", tasks_rdxc),
    13: ("Wednesday Mini-Test 80m (UA9QCQ UBN)", tasks_wed_minitest_80m),
    14: ("RF Championship CW (UA9QCQ UBN)", tasks_rf_championship_cw),
    15: ("Ham Spirit Contest (UA9QCQ UBN)", tasks_ham_spirit),
    17: ("RCC Cup (UA9QCQ UBN)", tasks_rcc_cup),
    18: ("RDA Contest (UA9QCQ UBN)", tasks_rda),
    19: ("Russian Radio Team Championship (UA9QCQ UBN)", tasks_rrtc),
    20: ("Yuri Gagarin International DX Contest (UA9QCQ UBN)", tasks_yuri_gagarin),
    21: ("Coupe du REF (French HF Championship, CW/SSB)", tasks_ref),
    22: ("EUDX Contest (public logs)", tasks_eudx),
    23: ("OK Contest (CW/SSB) + OK-OM DX Contest (CW/SSB) + OK DX RTTY Contest", tasks_okomdx),
    24: ("DARC contests (Fieldday/WAG/Ausbildungscontest/CW/RTTY/FT4/Easter/XMAS)", tasks_darc_all),
    26: ("WW DIGI (public logs)", tasks_wwdigi),
    27: ("SP DX Contest (recreated from public result JSON)", tasks_spdx),
    28: ("OK1WC Memorial (preliminary/final reference tables)", tasks_ok1wc),
    29: ("YU DX Contest (public result QSO tables)", tasks_yudx),
    30: ("SAC Scandinavian Activity Contest (public Cabrillo logs)", tasks_sac),
    31: ("URE public logs (EAPSK63/EARTTY/King of Spain/CNCW/CME)", tasks_ure),
    32: ("9A HRS contests (HF Robot public QSO tables)", tasks_hrs_hf),
    33: ("Istra Open Contest (public Cabrillo logs)", tasks_istra_open),
    34: ("TTC-SPCWC (public checked-log tables)", tasks_ttc_spcwc),
}
UA9QCQ_PROVIDER_IDS = {11, 12, 13, 14, 15, 17, 18, 19, 20}
UA9QCQ_PROGRESS_EVERY = 100
DEFAULT_UA9QCQ_DATE_TIMEOUT = 900
DEFAULT_UA9QCQ_MAX_CONSECUTIVE_ERRORS = 50
DEFAULT_UA9QCQ_REQUEST_TIMEOUT = 12
UA9QCQ_DATE_TIMEOUT: int | None = DEFAULT_UA9QCQ_DATE_TIMEOUT
UA9QCQ_MAX_CONSECUTIVE_ERRORS: int | None = DEFAULT_UA9QCQ_MAX_CONSECUTIVE_ERRORS
UA9QCQ_REQUEST_TIMEOUT = DEFAULT_UA9QCQ_REQUEST_TIMEOUT


def configure_ua9qcq_module(module) -> None:
    timeout = UA9QCQ_REQUEST_TIMEOUT
    if timeout > 0 and hasattr(module, "REQUEST_TIMEOUT"):
        module.REQUEST_TIMEOUT = min(int(getattr(module, "REQUEST_TIMEOUT", timeout)), timeout)


def prompt_selection() -> List[int]:
    print("Select contests to download (comma-separated numbers or 'all'):")
    for num, (name, _) in PROVIDERS.items():
        print(f"  {num}) {name}")
    while True:
        choice = input("> ").strip().lower()
        selection = parse_selection(choice, PROVIDERS.keys())
        if selection:
            return selection
        print("Invalid selection, try again (e.g., 1,3 or all).")


def prompt_last_years() -> int | None:
    while True:
        choice = input("How many recent years? (number or 'all'): ").strip().lower()
        if choice in {"all", "a"}:
            return None
        try:
            val = int(choice)
            if val > 0:
                return val
        except ValueError:
            pass
        print("Please enter a positive integer or 'all'.")


def parse_selection(text: str, valid_ids: Iterable[int]) -> List[int] | None:
    if text in {"all", "a"}:
        return list(valid_ids)
    valid_set = set(valid_ids)
    selections: List[int] = []
    seen: set[int] = set()
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if not parts:
        return None
    for part in parts:
        if "-" in part:
            if part.count("-") != 1:
                return None
            start_s, end_s = [p.strip() for p in part.split("-", 1)]
            if not start_s.isdigit() or not end_s.isdigit():
                return None
            start = int(start_s)
            end = int(end_s)
            if start > end:
                start, end = end, start
            for value in range(start, end + 1):
                if value in valid_set and value not in seen:
                    selections.append(value)
                    seen.add(value)
        else:
            if not part.isdigit():
                return None
            value = int(part)
            if value in valid_set and value not in seen:
                selections.append(value)
                seen.add(value)
    return selections or None


def prompt_git_push() -> None:
    def run_command(argv: list[str], label: str, cwd: Path | None = None, env: dict[str, str] | None = None) -> bool:
        print(f"Running {label}...")
        proc = subprocess.run(argv, check=False, cwd=cwd, env=env)
        if proc.returncode != 0:
            print(f"{label} failed (exit {proc.returncode}).", file=sys.stderr)
            return False
        return True

    try:
        resp = input(
            '\nRun `git status -sb && git add -A && git commit -m "New logs" && git push -u origin main`? [y/N]: '
        ).strip().lower()
    except EOFError:
        resp = ""
    if resp not in {"y", "yes"}:
        return
    commands = [
        (["git", "status", "-sb"], "git status -sb"),
        (["git", "add", "-A"], "git add -A"),
        (["git", "commit", "-m", "New logs"], 'git commit -m "New logs"'),
        (["git", "push", "-u", "origin", "main"], "git push -u origin main"),
    ]
    for argv, label in commands:
        if not run_command(argv, label, cwd=REPO_ROOT):
            return


def main() -> int:
    start_time = time.time()
    parser = argparse.ArgumentParser(description="Public contest logs downloader with menu.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Thread pool size / max concurrency (default: 20).")
    parser.add_argument(
        "--no-adaptive",
        action="store_true",
        help="Disable adaptive concurrency (adaptive is on by default).",
    )
    parser.add_argument(
        "--min-workers",
        type=int,
        default=4,
        help="Minimum concurrent downloads when adaptive is enabled (default: 4).",
    )
    parser.add_argument("--non-interactive", action="store_true", help="Skip menu; use --contests and --last.")
    parser.add_argument(
        "--contests",
        type=str,
        default="all",
        help="Comma numbers from menu or 'all' (used when --non-interactive).",
    )
    parser.add_argument(
        "--last",
        type=str,
        default="all",
        help="How many recent years (number or 'all') (used when --non-interactive).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print extra diagnostics (host/IP buckets, per-host task counts).",
    )
    parser.add_argument(
        "--heartbeat",
        type=int,
        default=60,
        help="Heartbeat interval in seconds (0 disables).",
    )
    parser.add_argument(
        "--ua9qcq-date-timeout",
        type=int,
        default=DEFAULT_UA9QCQ_DATE_TIMEOUT,
        help="Abort one UA9QCQ date after N seconds (0 disables, default: 900).",
    )
    parser.add_argument(
        "--ua9qcq-max-consecutive-errors",
        type=int,
        default=DEFAULT_UA9QCQ_MAX_CONSECUTIVE_ERRORS,
        help="Abort one UA9QCQ date after N consecutive call failures (0 disables, default: 50).",
    )
    parser.add_argument(
        "--ua9qcq-request-timeout",
        type=int,
        default=DEFAULT_UA9QCQ_REQUEST_TIMEOUT,
        help="Per-request timeout for UA9QCQ helper modules in seconds (0 keeps helper default, default: 12).",
    )
    parser.add_argument(
        "--rebuild-shards",
        action="store_true",
        help="Delete and rebuild SH6 SQLite shard indexes after downloads.",
    )
    parser.add_argument(
        "--no-update-readme",
        action="store_true",
        help="Do not refresh README.md stats after rebuilding SH6 shards.",
    )
    parser.add_argument(
        "--no-task-ledger",
        action="store_true",
        help="Disable task hash ledger (always rediscover/download lists).",
    )
    args = parser.parse_args()

    global TASK_LEDGER, UA9QCQ_DATE_TIMEOUT, UA9QCQ_MAX_CONSECUTIVE_ERRORS, UA9QCQ_REQUEST_TIMEOUT
    UA9QCQ_DATE_TIMEOUT = args.ua9qcq_date_timeout if args.ua9qcq_date_timeout > 0 else None
    UA9QCQ_MAX_CONSECUTIVE_ERRORS = (
        args.ua9qcq_max_consecutive_errors
        if args.ua9qcq_max_consecutive_errors > 0
        else None
    )
    UA9QCQ_REQUEST_TIMEOUT = args.ua9qcq_request_timeout

    adaptive_enabled = not args.no_adaptive
    if args.rebuild_shards:
        shard_dir = Path("SH6")
        print(f"Rebuilding SQLite shards in: {shard_dir}")
        try:
            shard_count = build_sqlite_shards(Path("."), shard_dir)
            print(f"Shard entries: {shard_count}")
        except Exception as exc:  # pylint: disable=broad-except
            print(f"SQLite shard rebuild failed: {exc}", file=sys.stderr)
            return 1
        if not args.no_update_readme:
            try:
                update_readme_from_shards(Path("."), shard_dir)
                print("Updated README.md stats.")
            except Exception as exc:  # pylint: disable=broad-except
                print(f"README.md stats update failed: {exc}", file=sys.stderr)
                return 1
        print("Done.")
        return 0

    if not args.no_task_ledger:
        TASK_LEDGER = TaskLedger(TASK_LEDGER_PATH)

    if args.non_interactive:
        selections = parse_selection(args.contests.lower(), PROVIDERS.keys())
        if not selections:
            print("Invalid --contests value.", file=sys.stderr)
            return 1
        last_val: int | None
        if args.last.lower() in {"all", "a"}:
            last_val = None
        else:
            try:
                last_val = int(args.last)
            except ValueError:
                print("Invalid --last value.", file=sys.stderr)
                return 1
    else:
        selections = prompt_selection()
        last_val = prompt_last_years()

    if any(sel in UA9QCQ_PROVIDER_IDS for sel in selections):
        get_ua9qcq_cookie()

    total_tasks = 0
    print("\nStarting provider discovery in parallel...")

    def run_provider(sel: int) -> Tuple[int, List[DownloadTask]]:
        name, fn = PROVIDERS[sel]
        try:
            tasks = fn(last_val)
            if tasks:
                filtered = [task for task in tasks if not valid_existing_log(task.dest)]
                if len(filtered) != len(tasks):
                    with PRINT_LOCK:
                        print(
                            f"Provider {sel}) {name}: filtered {len(tasks) - len(filtered)} existing files"
                        )
                tasks = filtered
        except Exception as exc:  # pylint: disable=broad-except
            with PRINT_LOCK:
                print(f"Provider {sel}) {name} failed during discovery: {exc}")
            return sel, []
        return sel, tasks

    discovery_results: Dict[int, Tuple[str, int]] = {}
    download_threads: List[threading.Thread] = []
    provider_stats: Dict[int, Dict[str, int]] = {}
    reconstruct_roots: set[str] = set()
    stats_lock = threading.Lock()
    download_cancel_event = DOWNLOAD_CANCEL_EVENT
    download_cancel_event.clear()

    def build_limiter(max_workers: int) -> AdaptiveLimiter | None:
        if not adaptive_enabled:
            return None
        max_limit = max(1, max_workers)
        min_limit = max(1, min(args.min_workers, max_limit))
        print(f"Adaptive concurrency enabled for host: min={min_limit}, max={max_limit}")
        return AdaptiveLimiter(initial=max_limit, min_limit=min_limit, max_limit=max_limit)

    def process_host(
        host_label: str,
        tasks: List[DownloadTask],
        provider_counts: Dict[str, int],
        counts_lock: threading.Lock,
        max_workers: int,
        task_totals: Dict[str, int],
        task_done: Dict[str, int],
        task_errors: Dict[str, int],
        task_counts: Dict[str, int],
        task_lock: threading.Lock,
        cancel_event: threading.Event,
    ) -> None:
        limiter = build_limiter(max_workers)

        def wrapped_task(task: DownloadTask) -> Dict[str, int]:
            if cancel_event.is_set():
                return {"cancel": 1}
            if limiter:
                limiter.acquire()
            success = False
            try:
                if cancel_event.is_set():
                    return {"cancel": 1}
                counts = task.action()
                success = counts.get("error", 0) == 0
                if task.task_key and task.task_hash:
                    with task_lock:
                        task_done[task.task_key] = task_done.get(task.task_key, 0) + 1
                        if counts.get("error", 0) > 0:
                            task_errors[task.task_key] = task_errors.get(task.task_key, 0) + counts.get("error", 0)
                        if (
                            not cancel_event.is_set()
                            and task_done[task.task_key] == task_totals.get(task.task_key, 0)
                        ):
                            if task_errors.get(task.task_key, 0) == 0:
                                task_mark_complete(
                                    task.task_key,
                                    task.task_hash,
                                    task_counts.get(task.task_key, 0),
                                )
                return counts
            except Exception as exc:  # pylint: disable=broad-except
                with PRINT_LOCK:
                    print(f"fail task {task.dest}: {exc}")
                success = False
                return {"error": 1}
            finally:
                if limiter:
                    limiter.release(success)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(wrapped_task, task) for task in tasks]
            pending = set(futures)
            try:
                while pending:
                    if cancel_event.is_set():
                        for future in pending:
                            future.cancel()
                        break
                    done, pending = concurrent.futures.wait(
                        pending,
                        timeout=1.0,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    for future in done:
                        if future.cancelled():
                            counts = {"cancel": 1}
                        else:
                            counts = future.result()
                        with counts_lock:
                            add_counts(provider_counts, counts)
            finally:
                if cancel_event.is_set():
                    executor.shutdown(wait=True, cancel_futures=True)
                    for future in pending:
                        if future.cancelled():
                            counts = {"cancel": 1}
                        else:
                            try:
                                counts = future.result()
                            except Exception as exc:  # pylint: disable=broad-except
                                with PRINT_LOCK:
                                    print(f"fail cancelled task cleanup: {exc}")
                                counts = {"error": 1}
                        with counts_lock:
                            add_counts(provider_counts, counts)

    def download_provider(sel: int, tasks: List[DownloadTask]) -> None:
        name, _ = PROVIDERS[sel]
        if not tasks:
            return
        provider_start = time.time()
        provider_counts = empty_counts()
        provider_lock = threading.Lock()
        task_totals: Dict[str, int] = {}
        task_done: Dict[str, int] = {}
        task_errors: Dict[str, int] = {}
        task_counts: Dict[str, int] = {}
        task_lock = threading.Lock()
        for task in tasks:
            if not task.task_key or not task.task_hash:
                continue
            task_totals[task.task_key] = task_totals.get(task.task_key, 0) + 1
            task_counts.setdefault(task.task_key, task.task_count or 0)
        with PRINT_LOCK:
            print(f"\nProvider {sel}) {name}: starting {len(tasks)} downloads")
        stop_event = threading.Event()

        def heartbeat() -> None:
            if args.heartbeat <= 0:
                return
            while not stop_event.wait(args.heartbeat):
                if download_cancel_event.is_set():
                    return
                with provider_lock:
                    counts = dict(provider_counts)
                elapsed = int(time.time() - provider_start)
                with PRINT_LOCK:
                    print(
                        f"heartbeat {sel}) {name}: ok {counts.get('ok', 0)} "
                        f"skip {counts.get('skip', 0)} err {counts.get('error', 0)} "
                        f"elapsed {elapsed}s"
                    )

        hb_thread = threading.Thread(target=heartbeat)
        hb_thread.start()
        hostnames = {task.host for task in tasks}
        if args.debug:
            with PRINT_LOCK:
                print("\nResolving hosts with dig (best-effort)...")
        resolved = resolve_hosts(hostnames)

        server_buckets: Dict[Tuple[str, str], List[DownloadTask]] = {}
        for task in tasks:
            ips = resolved.get(task.host, [])
            ip = ips[0] if ips else "unresolved"
            key = (task.host, ip)
            server_buckets.setdefault(key, []).append(task)

        if args.debug:
            with PRINT_LOCK:
                print("\nDEBUG: host -> IP mapping:")
                for host in sorted(hostnames):
                    ips = resolved.get(host, [])
                    print(f"  {host}: {', '.join(ips) if ips else 'unresolved'}")

                print("\nDEBUG: bucket breakdown (host/ip -> count by source):")
                for (host, ip), tasks_for_host in server_buckets.items():
                    by_source: Dict[str, int] = {}
                    for task in tasks_for_host:
                        by_source[task.source] = by_source.get(task.source, 0) + 1
                    source_str = ", ".join(
                        f"{src}:{cnt}" for src, cnt in sorted(by_source.items())
                    )
                    print(f"  {host} ({ip}): {len(tasks_for_host)} tasks [{source_str}]")

        threads = []
        for (host, ip), tasks_for_host in server_buckets.items():
            label = f"{host} ({ip})"
            with PRINT_LOCK:
                print(f"\nServer {label}: {len(tasks_for_host)} tasks")
            max_workers = min(args.workers, HOST_WORKER_CAPS.get(host, args.workers))
            t = threading.Thread(
                target=process_host,
                args=(
                    label,
                    tasks_for_host,
                    provider_counts,
                    provider_lock,
                    max_workers,
                    task_totals,
                    task_done,
                    task_errors,
                    task_counts,
                    task_lock,
                    download_cancel_event,
                ),
            )
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        stop_event.set()
        hb_thread.join()
        elapsed = time.time() - provider_start
        provider_counts["elapsed"] = int(round(elapsed))
        with stats_lock:
            provider_stats[sel] = provider_counts
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(len(selections), args.workers))) as executor:
        futures = {executor.submit(run_provider, sel): sel for sel in selections}
        for fut in concurrent.futures.as_completed(futures):
            sel, tasks = fut.result()
            name, _ = PROVIDERS[sel]
            discovery_results[sel] = (name, len(tasks))
            total_tasks += len(tasks)
            for task in tasks:
                roots = task.output_roots or (
                    (task.dest.parts[0],) if task.dest.parts else ()
                )
                reconstruct_roots.update(root for root in roots if root in MANIFEST_ROOTS)
            t = threading.Thread(target=download_provider, args=(sel, tasks))
            t.start()
            download_threads.append(t)

    print("\nDiscovery results (stable order):")
    for sel in sorted(selections):
        name, count = discovery_results.get(sel, (PROVIDERS[sel][0], 0))
        print(f"  {sel}) {name:<40} queued {count:>6} downloads")

    if total_tasks == 0:
        print("No new downloads queued.")
        return 0

    print(f"\nTotal files to download: {total_tasks} using up to {args.workers} workers per server")

    download_interrupted = False
    try:
        for t in download_threads:
            while t.is_alive():
                t.join(timeout=0.5)
    except KeyboardInterrupt:
        download_interrupted = True
        download_cancel_event.set()
        print("\nDownload interrupt received. Stopping queued downloads...")
        for t in download_threads:
            while t.is_alive():
                t.join(timeout=0.5)
        print("Download workers stopped.")

    def print_download_summary() -> None:
        total_elapsed = time.time() - start_time
        total_counts = empty_counts()
        for stats in provider_stats.values():
            add_counts(total_counts, stats)
        print("\nSummary:")
        print(f"  providers: {len(selections)}")
        print(f"  total queued: {total_tasks}")
        print(f"  downloaded: {total_counts.get('ok', 0)}")
        print(f"  skipped: {total_counts.get('skip', 0)}")
        print(f"  errors: {total_counts.get('error', 0)}")
        if total_counts.get("cancel", 0):
            print(f"  canceled: {total_counts.get('cancel', 0)}")
        print(f"  elapsed: {total_elapsed:.1f}s")
        print("\nPer-provider stats:")
        for sel in sorted(selections):
            name, _ = PROVIDERS[sel]
            stats = provider_stats.get(sel, empty_counts())
            queued = discovery_results.get(sel, (name, 0))[1]
            provider_elapsed = stats.get("elapsed", 0)
            cancel_part = (
                f"cancel {stats.get('cancel', 0):>6} "
                if stats.get("cancel", 0)
                else ""
            )
            print(
                f"  {sel}) {name:<40} ok {stats.get('ok', 0):>6} "
                f"skip {stats.get('skip', 0):>6} err {stats.get('error', 0):>4} "
                f"{cancel_part}queued {queued:>6} time {provider_elapsed:>5}s"
            )

    if download_interrupted:
        print_download_summary()
        try:
            resp = input(
                "\nContinue with post-download steps as if downloads had finished "
                "(reconstruction prompt, shard rebuild, git prompt)? [y/N]: "
            ).strip().lower()
        except EOFError:
            resp = ""
        if resp not in {"y", "yes"}:
            print("Post-download steps skipped after interrupt.")
            return 130

    if not download_interrupted:
        print_download_summary()
    if args.non_interactive:
        resp = ""
    else:
        try:
            resp = input("\nAdd reconstructed logs from submitted logs? [y/N]: ").strip().lower()
        except EOFError:
            resp = ""
    if resp in {"y", "yes"}:
        script_path = Path("scripts") / "reconstruct_missing_logs.py"
        if not script_path.exists():
            print(f"Reconstruction script not found: {script_path}", file=sys.stderr)
        elif not reconstruct_roots:
            print("No contest roots from this run were queued, skipping reconstruction.")
        else:
            roots = sorted(reconstruct_roots)
            print(
                "Running reconstruction for current-run contests only: "
                + ", ".join(roots)
            )
            proc = subprocess.run(
                [
                    sys.executable,
                    str(script_path),
                    "--no-rebuild-shards",
                    *[arg for root in roots for arg in ("--contest", root)],
                ],
                check=False,
            )
            if proc.returncode != 0:
                print(f"Reconstruction failed (exit {proc.returncode}).", file=sys.stderr)
    shard_dir = Path("SH6")
    print(f"\nRebuilding SQLite shards in: {shard_dir}")
    shards_rebuilt = False
    try:
        shard_count = build_sqlite_shards(Path("."), shard_dir)
        print(f"Shard entries: {shard_count}")
        shards_rebuilt = True
    except Exception as exc:  # pylint: disable=broad-except
        print(f"SQLite shard rebuild failed: {exc}", file=sys.stderr)
    if shards_rebuilt and not args.no_update_readme:
        try:
            update_readme_from_shards(Path("."), shard_dir)
            print("Updated README.md stats.")
        except Exception as exc:  # pylint: disable=broad-except
            print(f"README.md stats update failed: {exc}", file=sys.stderr)
            return 1
    if not args.non_interactive:
        prompt_git_push()
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
