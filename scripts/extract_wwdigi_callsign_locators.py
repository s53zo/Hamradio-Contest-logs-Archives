#!/usr/bin/env python3
"""
Extract most common station locator per callsign from WWDIGI Cabrillo logs.

Rules implemented:
1) Use all callsigns seen in logs (submitted station + QSO partners).
2) For submitted logs, include GRID-LOCATOR when present.
3) For QSO lines, map MYCALL->MYGRID and HISCALL->HISGRID.
4) Normalize to 4-digit grid locators and pick the most common per callsign.

Outputs:
- analysis/wwdigi_callsign_locator_counts.csv
- analysis/wwdigi_callsign_locator_best.csv
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple


CALL_RE = re.compile(r"[A-Z0-9/]+")
GRID4_RE = re.compile(r"^[A-Z]{2}[0-9]{2}$")


@dataclass
class LocatorVotes:
    total: int = 0
    header: int = 0
    my_qso: int = 0
    his_qso: int = 0
    last_year: int = 0
    sample_file: str = ""


def normalize_callsign(text: str) -> Optional[str]:
    if not text:
        return None
    candidate = text.strip().upper()
    match = CALL_RE.search(candidate)
    if not match:
        return None
    call = match.group(0)
    return call if call else None


def normalize_locator4(text: str) -> Optional[str]:
    if not text:
        return None
    alnum = re.sub(r"[^A-Z0-9]", "", text.upper())
    if len(alnum) < 4:
        return None
    grid4 = alnum[:4]
    if not GRID4_RE.match(grid4):
        return None
    return grid4


def extract_year_from_path(path: Path) -> int:
    # Expected path format: WWDIGI/<year>/<file>.log
    for part in path.parts:
        if re.fullmatch(r"(19|20)\d{2}", part):
            return int(part)
    return 0


def parse_qso_tokens(line: str) -> Optional[Tuple[str, str, str, str]]:
    """
    Return (my_call, my_grid, his_call, his_grid) or None.

    Handles both:
      QSO: 14092 DG ...
      QSO:14092 DG ...
    """
    raw = line.strip()
    if not raw.startswith("QSO:"):
        return None

    if raw.startswith("QSO: ") or raw == "QSO:":
        tokens = raw.split()
    else:
        # e.g. QSO:14092 ...
        tokens = ["QSO:", raw[4:]]
        tail = raw[4:].strip()
        if tail:
            tokens = ["QSO:"] + tail.split()

    if len(tokens) < 9 or tokens[0] != "QSO:":
        return None

    my_call = tokens[5]
    my_grid = tokens[6]
    his_call = tokens[7]
    his_grid = tokens[8]
    return my_call, my_grid, his_call, his_grid


def add_vote(
    table: Dict[Tuple[str, str], LocatorVotes],
    call: Optional[str],
    loc4: Optional[str],
    source: str,
    year: int,
    sample_file: str,
) -> None:
    if call is None or loc4 is None:
        return
    key = (call, loc4)
    slot = table.get(key)
    if slot is None:
        slot = LocatorVotes(sample_file=sample_file)
        table[key] = slot
    slot.total += 1
    if source == "header":
        slot.header += 1
    elif source == "my_qso":
        slot.my_qso += 1
    elif source == "his_qso":
        slot.his_qso += 1
    if year > slot.last_year:
        slot.last_year = year
        slot.sample_file = sample_file


def iter_logs(root: Path) -> Iterable[Path]:
    yield from root.rglob("*.log")


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract WWDIGI callsign -> most common locator mapping.")
    parser.add_argument("--root", type=Path, default=Path("WWDIGI"), help="WWDIGI root directory.")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("analysis"),
        help="Output directory for CSV files.",
    )
    args = parser.parse_args()

    if not args.root.exists():
        raise SystemExit(f"Input root not found: {args.root}")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    counts_csv = args.out_dir / "wwdigi_callsign_locator_counts.csv"
    best_csv = args.out_dir / "wwdigi_callsign_locator_best.csv"

    votes: Dict[Tuple[str, str], LocatorVotes] = {}
    calls_seen: set[str] = set()
    files_scanned = 0
    malformed_qso = 0

    for path in iter_logs(args.root):
        files_scanned += 1
        year = extract_year_from_path(path)
        sample_file = path.as_posix()

        header_call: Optional[str] = None
        header_grid4: Optional[str] = None

        with path.open("r", encoding="utf-8", errors="ignore") as fh:
            for raw_line in fh:
                line = raw_line.rstrip("\n")

                if line.startswith("CALLSIGN:"):
                    header_call = normalize_callsign(line.split(":", 1)[1].strip())
                    if header_call:
                        calls_seen.add(header_call)
                    continue

                if line.startswith("GRID-LOCATOR:"):
                    header_grid4 = normalize_locator4(line.split(":", 1)[1].strip())
                    continue

                if not line.startswith("QSO:"):
                    continue

                parsed = parse_qso_tokens(line)
                if parsed is None:
                    malformed_qso += 1
                    continue

                my_raw, my_grid_raw, his_raw, his_grid_raw = parsed
                my_call = normalize_callsign(my_raw)
                his_call = normalize_callsign(his_raw)
                my_grid4 = normalize_locator4(my_grid_raw)
                his_grid4 = normalize_locator4(his_grid_raw)

                if my_call:
                    calls_seen.add(my_call)
                    add_vote(votes, my_call, my_grid4, "my_qso", year, sample_file)
                if his_call:
                    calls_seen.add(his_call)
                    add_vote(votes, his_call, his_grid4, "his_qso", year, sample_file)

        # Header evidence for submitted station.
        add_vote(votes, header_call, header_grid4, "header", year, sample_file)

    # Aggregate per callsign.
    per_call: Dict[str, Dict[str, LocatorVotes]] = defaultdict(dict)
    for (call, loc4), stats in votes.items():
        per_call[call][loc4] = stats

    # Write counts table (one row per callsign+locator).
    with counts_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "callsign",
                "locator4",
                "total_votes",
                "header_votes",
                "my_qso_votes",
                "his_qso_votes",
                "last_year",
                "sample_file",
            ]
        )
        for call in sorted(per_call):
            for loc4, stats in sorted(per_call[call].items(), key=lambda it: (-it[1].total, it[0])):
                writer.writerow(
                    [
                        call,
                        loc4,
                        stats.total,
                        stats.header,
                        stats.my_qso,
                        stats.his_qso,
                        stats.last_year,
                        stats.sample_file,
                    ]
                )

    # Write best locator per callsign using "most common" rule.
    with best_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "callsign",
                "best_locator4",
                "best_votes",
                "total_votes",
                "confidence",
                "tied_best_count",
                "header_votes_for_best",
                "my_qso_votes_for_best",
                "his_qso_votes_for_best",
                "last_year_for_best",
                "sample_file_for_best",
            ]
        )
        for call in sorted(calls_seen):
            loc_map = per_call.get(call)
            if not loc_map:
                writer.writerow([call, "", 0, 0, 0.0, 0, 0, 0, 0, 0, ""])
                continue

            total_votes = sum(item.total for item in loc_map.values())
            max_votes = max(item.total for item in loc_map.values())
            top = [(loc, st) for loc, st in loc_map.items() if st.total == max_votes]
            # Deterministic tie-breaker: newer evidence, then more header votes, then locator text.
            top.sort(key=lambda it: (-it[1].last_year, -it[1].header, it[0]))
            best_loc, best_stats = top[0]
            confidence = (best_stats.total / total_votes) if total_votes else 0.0
            writer.writerow(
                [
                    call,
                    best_loc,
                    best_stats.total,
                    total_votes,
                    f"{confidence:.6f}",
                    len(top),
                    best_stats.header,
                    best_stats.my_qso,
                    best_stats.his_qso,
                    best_stats.last_year,
                    best_stats.sample_file,
                ]
            )

    resolved_calls = len(per_call)
    unresolved_calls = len(calls_seen - set(per_call.keys()))
    print(f"Scanned files: {files_scanned}")
    print(f"Malformed QSO lines skipped: {malformed_qso}")
    print(f"Unique callsigns seen: {len(calls_seen)}")
    print(f"Callsigns with locator evidence: {resolved_calls}")
    print(f"Callsigns without locator evidence: {unresolved_calls}")
    print(f"Wrote: {counts_csv}")
    print(f"Wrote: {best_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
