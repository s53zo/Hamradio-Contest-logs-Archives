#!/usr/bin/env python3
"""Download public YOTA Contest evaluated QSO data as Cabrillo logs."""

from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

BASE_URL = "https://contest.ham-yota.com"
EVENTS_URL = f"{BASE_URL}/nest/events/list?site=contest.ham-yota.com"
OUTPUT_ROOT = Path("YOTA_Contest")
REQUEST_TIMEOUT = 30


@dataclass(frozen=True)
class YOTAEvent:
    event_id: str
    year: int
    round_code: str
    round_name: str


@dataclass(frozen=True)
class YOTAEntry:
    event: YOTAEvent
    callsign: str
    category: str
    round_code: str


def fetch_json(url: str) -> object:
    request = urllib.request.Request(url, headers={"User-Agent": "Hamradio-Contest-logs-Archives"})
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        return json.load(response)


def discover_events(last: int | None = None) -> list[YOTAEvent]:
    payload = fetch_json(EVENTS_URL)
    if not isinstance(payload, list):
        raise ValueError("YOTA event API returned a non-list response")
    events = []
    for item in payload:
        if not isinstance(item, dict) or not item.get("isClaimed"):
            continue
        name = str(item.get("name", ""))
        match = re.fullmatch(r"YOTA Contest (\d+)(?:st|nd|rd|th) Round - (\d{4})", name)
        if not match:
            continue
        events.append(
            YOTAEvent(
                event_id=str(item["_id"]),
                year=int(match.group(2)),
                round_code=f"YOTA-{match.group(2)}-{match.group(1)}",
                round_name=f"Round_{match.group(1)}",
            )
        )
    events.sort(key=lambda event: (event.year, event.round_code), reverse=True)
    if last:
        years = sorted({event.year for event in events}, reverse=True)[:last]
        events = [event for event in events if event.year in years]
    return events


def claimed_url(event_id: str) -> str:
    return f"{BASE_URL}/nest/claimed?{urllib.parse.urlencode({'eventId': event_id})}"


def qso_url(entry: YOTAEntry) -> str:
    return f"{BASE_URL}/nest/qso?" + urllib.parse.urlencode(
        {
            "eventId": entry.event.event_id,
            "callsign": entry.callsign,
            "roundCode": entry.round_code,
            "isClaimed": "true",
        }
    )


def discover_entries(event: YOTAEvent) -> list[YOTAEntry]:
    payload = fetch_json(claimed_url(event.event_id))
    if not isinstance(payload, list):
        raise ValueError("YOTA claimed-results API returned a non-list response")
    discovered_round_code = next(
        (
            str(rounds[0].get("code", ""))
            for group in payload
            if isinstance(group, dict)
            for rounds in [((group.get("evalCategory") or {}).get("rounds") or [])]
            if rounds and rounds[0].get("code")
        ),
        event.round_code,
    )
    entries: dict[str, YOTAEntry] = {}
    for group in payload:
        if not isinstance(group, dict):
            continue
        eval_category = group.get("evalCategory") or {}
        category = str(eval_category.get("name", "CHECKLOG"))
        rounds = eval_category.get("rounds") or []
        round_code = str(rounds[0].get("code", "")) if rounds else discovered_round_code
        for log in group.get("logs", []):
            callsign = str((log.get("_id") or {}).get("callsign", "")).strip().upper()
            if callsign:
                entries.setdefault(callsign, YOTAEntry(event, callsign, category, round_code))
    return sorted(entries.values(), key=lambda entry: entry.callsign)


def safe_call(callsign: str) -> str:
    return re.sub(r"[^A-Z0-9_.-]+", "_", callsign.upper()).strip("._") or "UNKNOWN"


def destination(entry: YOTAEntry, root: Path = OUTPUT_ROOT) -> Path:
    return root / str(entry.event.year) / entry.event.round_name / f"{safe_call(entry.callsign)}.log"


def category_fields(name: str) -> tuple[str, str]:
    operator = "MULTI-OP" if "multi" in name.lower() else "SINGLE-OP"
    band = "ALL"
    match = re.search(r"\b(10|15|20|40|80)\s*m\b", name, re.IGNORECASE)
    if match:
        band = f"{match.group(1)}M"
    return operator, band


def _exchange(qso: dict, prefix: str) -> str:
    number = qso.get(f"{prefix}Num")
    text = qso.get(f"{prefix}Exch")
    return str(number if number is not None else text or "0")


def build_cabrillo(entry: YOTAEntry, payload: dict) -> str:
    operator, band = category_fields(entry.category)
    lines = [
        "START-OF-LOG: 3.0",
        "CREATED-BY: yota-public-qso-downloader",
        "CONTEST: YOTA-CONTEST",
        f"CALLSIGN: {entry.callsign}",
        f"CATEGORY-OPERATOR: {operator}",
        f"CATEGORY-BAND: {band}",
        "CATEGORY-MODE: MIXED",
        f"CATEGORY: {entry.category}",
        "CLAIMED-SCORE: 0",
    ]
    for qso in payload.get("qsos", []):
        stamp = datetime.fromisoformat(str(qso["dateTime"]).replace("Z", "+00:00")).astimezone(timezone.utc)
        freq = int(qso.get("freq") or 0)
        mode = str(qso.get("mode") or "CW").upper()
        mode = "PH" if mode in {"SSB", "PHONE"} else mode[:2]
        lines.append(
            "QSO: "
            f"{freq:5d} {mode:<2} {stamp:%Y-%m-%d %H%M} "
            f"{entry.callsign:<13} {str(qso.get('sRst') or '59'):<3} {_exchange(qso, 's'):<6} "
            f"{str(qso.get('callsign') or 'UNKNOWN').upper():<13} "
            f"{str(qso.get('rRst') or '59'):<3} {_exchange(qso, 'r'):<6}"
        )
    lines.append("END-OF-LOG:")
    return "\n".join(lines) + "\n"


def fetch_log(entry: YOTAEntry) -> str:
    payload = fetch_json(qso_url(entry))
    if not isinstance(payload, dict):
        raise ValueError("YOTA QSO API returned a non-object response")
    return build_cabrillo(entry, payload)


def iter_entries(last: int | None = None) -> Iterable[YOTAEntry]:
    for event in discover_events(last):
        yield from discover_entries(event)
