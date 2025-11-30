#!/usr/bin/env python3
"""
Find stations that likely called CQ for a short time and then left the band.

Heuristics:
    - A station is considered to be "running" (calling CQ) on a band if
      most of the observed QSOs for that station occur on (roughly) the
      same frequency.
    - If the total observed time span on that band is below a threshold,
      we treat it as a "short appearance".

Data source:
    Cabrillo logs in CQWW/<mode>/<year>/*.log (default: CQWW/ph/2024).
    We only need the QSO lines; each QSO line contributes an observation
    for the *worked* station (the second callsign in the line).
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import multiprocessing
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


BANDS: List[Tuple[int, int, str]] = [
    (1800, 2000, "160m"),
    (3500, 4000, "80m"),
    (5250, 5450, "60m"),
    (7000, 7300, "40m"),
    (10100, 10150, "30m"),
    (14000, 14350, "20m"),
    (18068, 18168, "17m"),
    (21000, 21450, "15m"),
    (24890, 24990, "12m"),
    (28000, 29700, "10m"),
    (50000, 54000, "6m"),
]

LOGS_BASE = Path("CQWW")


class Observation:
    __slots__ = ("ts", "freq", "logger")

    def __init__(self, ts: dt.datetime, freq: int, logger: str) -> None:
        self.ts = ts
        self.freq = freq
        self.logger = logger


def freq_to_band(freq_khz: int) -> str | None:
    for low, high, name in BANDS:
        if low <= freq_khz <= high:
            return name
    return None


def parse_qso_line(line: str) -> Tuple[int, str, dt.datetime, str, str] | None:
    """
    Parse a Cabrillo QSO line.
    Returns (freq_khz, mode, timestamp, logger_call, worked_call).
    """
    if not line.startswith("QSO:"):
        return None
    parts = line.split()
    if len(parts) < 11:
        return None
    try:
        freq_khz = int(parts[1])
        mode = parts[2]
        date_str = parts[3]
        time_str = parts[4]
        logger_call = parts[5].upper()
        worked_call = parts[8].upper()
        ts = dt.datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H%M")
    except (ValueError, IndexError):
        return None
    return freq_khz, mode, ts, logger_call, worked_call


def best_freq_cluster(freqs: List[int], window_khz: int) -> Tuple[int, int]:
    """
    Find the densest frequency cluster within +/- window_khz.
    Returns (count_in_cluster, representative_freq).
    """
    if not freqs:
        return 0, 0
    freqs.sort()
    left = 0
    best_count = 1
    best_freq = freqs[0]
    for right, freq in enumerate(freqs):
        while freq - freqs[left] > window_khz:
            left += 1
        count = right - left + 1
        if count > best_count:
            best_count = count
            slice_freqs = freqs[left : right + 1]
            best_freq = int(sum(slice_freqs) / len(slice_freqs))
    return best_count, best_freq


def _parse_file(path: Path, mode_filter: str | None) -> List[Tuple[Tuple[str, str], Observation]]:
    """
    Parse one Cabrillo log and emit observations keyed by (worked call, band).
    """
    out: List[Tuple[Tuple[str, str], Observation]] = []
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError as exc:
        print(f"warn: cannot read {path}: {exc}")
        return out

    for line in text.splitlines():
        parsed = parse_qso_line(line)
        if not parsed:
            continue
        freq_khz, mode, ts, logger_call, worked_call = parsed
        if mode_filter and mode.upper() != mode_filter.upper():
            continue
        band = freq_to_band(freq_khz)
        if not band:
            continue
        out.append(((worked_call, band), Observation(ts, freq_khz, logger_call)))
    return out


def _parse_file_star(args: Tuple[Path, str | None]) -> List[Tuple[Tuple[str, str], Observation]]:
    path, mode_filter = args
    return _parse_file(path, mode_filter)


def collect_observations(
    log_dir: Path, mode_filter: str | None, workers: int, executor_kind: str
) -> Dict[Tuple[str, str], List[Observation]]:
    """
    Walk all .log files under log_dir and collect observations keyed by (call, band).
    Uses a thread or process pool to speed up parsing across many logs.
    """
    observations: Dict[Tuple[str, str], List[Observation]] = defaultdict(list)
    paths = sorted(log_dir.glob("*.log"))
    if not paths:
        print(f"warn: no .log files found in {log_dir}")
        return observations

    # Auto-select executor: use processes for larger/CPU-heavier logs; threads otherwise.
    executor_resolved = executor_kind
    if executor_kind == "auto":
        total_size = 0
        for p in paths:
            try:
                total_size += p.stat().st_size
            except OSError:
                continue
        avg_size = total_size / max(1, len(paths))
        # Heuristic: if average log is >50 KB and log count is modest, processes help.
        if avg_size > 50_000 and len(paths) < 2000 and (os.cpu_count() or 1) > 1:
            executor_resolved = "process"
        else:
            executor_resolved = "thread"

    executor_cls = concurrent.futures.ThreadPoolExecutor
    if executor_resolved == "process":
        executor_cls = concurrent.futures.ProcessPoolExecutor

    tasks = [(p, mode_filter) for p in paths]

    with executor_cls(max_workers=workers) as executor:
        mapper = _parse_file_star if executor_resolved == "process" else (lambda arg: _parse_file(*arg))
        for obs_list in executor.map(mapper, tasks, chunksize=20):
            for key, obs in obs_list:
                observations[key].append(obs)
    return observations


def infer_year_label(path: Path) -> str:
    """
    Try to extract a year label from the directory name; fallback to the
    directory basename. Include parent (mode) if available to keep labels distinct.
    """
    stem = path.name
    match = re.search(r"(20\d{2}|19\d{2})", stem)
    if match:
        year = match.group(1)
        mode = path.parent.name if path.parent else ""
        return f"{mode}-{year}" if mode else year
    return stem


def discover_year_dirs(base: Path, last: int | None = None) -> List[Path]:
    """
    Find year subdirectories under base (e.g., CQWW/*/<year>) sorted newest first.
    If --last is provided, keep only the most recent N distinct years (all modes for those years).
    """
    by_year: dict[str, List[Path]] = {}
    if not base.exists():
        return []
    for mode_dir in base.iterdir():
        if not mode_dir.is_dir():
            continue
        for child in mode_dir.iterdir():
            if child.is_dir() and re.fullmatch(r"(19|20)\d{2}", child.name):
                by_year.setdefault(child.name, []).append(child)
    years = sorted(by_year.keys(), key=int, reverse=True)
    if last:
        years = years[:last]
    ordered: List[Path] = []
    for year in years:
        ordered.extend(sorted(by_year[year]))
    return ordered


def analyze(
    observations: Dict[Tuple[str, str], List[Observation]],
    max_span_minutes: float,
    min_span_minutes: float,
    freq_window_khz: int,
    min_cluster_qsos: int,
    min_cluster_ratio: float,
) -> List[dict]:
    """
    Derive short-appearance CQ stations from observations.
    """
    results: List[dict] = []
    for (call, band), obs_list in observations.items():
        if len(obs_list) < min_cluster_qsos:
            continue
        obs_list.sort(key=lambda o: o.ts)
        span_min = (obs_list[-1].ts - obs_list[0].ts).total_seconds() / 60.0
        if span_min > max_span_minutes or span_min < min_span_minutes:
            continue
        freqs = [obs.freq for obs in obs_list]
        cluster_count, cluster_freq = best_freq_cluster(freqs, freq_window_khz)
        ratio = cluster_count / len(obs_list)
        if cluster_count < min_cluster_qsos or ratio < min_cluster_ratio:
            continue
        loggers = {o.logger for o in obs_list}
        results.append(
            {
                "call": call,
                "band": band,
                "qsos": len(obs_list),
                "logs": len(loggers),
                "span_min": span_min,
                "freq_khz": cluster_freq,
                "cluster_qsos": cluster_count,
                "cluster_ratio": ratio,
                "first": obs_list[0].ts,
                "last": obs_list[-1].ts,
            }
        )
    results.sort(key=lambda r: (r["span_min"], -r["qsos"]))
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Find short CQ appearances in CQWW logs.")
    parser.add_argument(
        "--log-dir",
        action="append",
        type=Path,
        help="Directory with .log files. Repeatable. Default: CQWW/ph/2024.",
    )
    parser.add_argument(
        "--last",
        type=int,
        default=None,
        help="Use the most recent N year directories under CQWW/ (ignored if --log-dir is used).",
    )
    parser.add_argument(
        "--mode",
        default="",
        help="Mode filter (e.g. PH, CW, RY). Use '' to include all modes (default: '').",
    )
    parser.add_argument(
        "--max-span-minutes",
        type=float,
        default=60.0,
        help="Maximum observed time span to consider a short appearance (default: 60).",
    )
    parser.add_argument(
        "--min-span-minutes",
        type=float,
        default=0.0,
        help="Minimum observed time span to consider (default: 0).",
    )
    parser.add_argument(
        "--freq-window-khz",
        type=int,
        default=3,
        help="Frequency window in kHz to treat as 'same frequency' (default: 3).",
    )
    parser.add_argument(
        "--min-cluster-qsos",
        type=int,
        default=10,
        help="Minimum QSOs in the main frequency cluster (default: 10).",
    )
    parser.add_argument(
        "--min-cluster-ratio",
        type=float,
        default=0.6,
        help="Minimum ratio of QSOs inside the main frequency cluster (default: 0.6).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Limit output rows per year (default: 50).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(32, (os.cpu_count() or 1) + 4),
        help="Number of threads for parsing logs (default: ~CPU count).",
    )
    parser.add_argument(
        "--executor",
        choices=["thread", "process", "auto"],
        default="auto",
        help="thread=I/O bound, process=CPU bound, auto=choose based on file size/count (default).",
    )
    args = parser.parse_args()

    mode_filter = args.mode if args.mode else None
    if args.log_dir:
        log_dirs = args.log_dir
    else:
        base = LOGS_BASE
        log_dirs = discover_year_dirs(base, last=args.last)
        if not log_dirs:
            fallback = LOGS_BASE / "ph" / "2024"
            print(f"warn: no year directories found under {base}, fallback to {fallback}")
            log_dirs = [fallback]

    all_year_results: List[Tuple[str, List[dict]]] = []

    for log_dir in log_dirs:
        year_label = infer_year_label(log_dir)
        observations = collect_observations(log_dir, mode_filter, args.workers, args.executor)
        results = analyze(
            observations=observations,
            max_span_minutes=args.max_span_minutes,
            min_span_minutes=args.min_span_minutes,
            freq_window_khz=args.freq_window_khz,
            min_cluster_qsos=args.min_cluster_qsos,
            min_cluster_ratio=args.min_cluster_ratio,
        )
        for row in results:
            row["year"] = year_label
        all_year_results.append((year_label, results))

    for year_label, results in all_year_results:
        print(
            f"\n=== {year_label} ===\n{'CALL':<12} {'BAND':<4} {'QSO':>4} {'LOGS':>4} {'SPAN(min)':>10} {'FREQ(kHz)':>9} {'CLUST':>7} {'FIRST UTC':>16} {'LAST UTC':>16}"
        )
        for row in results[: args.limit]:
            print(
                f"{row['call']:<12} {row['band']:<4} {row['qsos']:>4} {row['logs']:>4} "
                f"{row['span_min']:>10.1f} {row['freq_khz']:>9} "
                f"{row['cluster_qsos']:>3}/{row['qsos']:<3} "
                f"{row['first']:%m-%d %H:%M} {row['last']:%m-%d %H:%M}"
            )
        print(f"Found {len(results)} short-run candidates in {year_label}.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
