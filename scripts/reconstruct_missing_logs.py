#!/usr/bin/env python3
"""Reconstruct mockup Cabrillo logs for missing submissions."""

from __future__ import annotations

import argparse
import concurrent.futures
import os
from collections import defaultdict
from dataclasses import dataclass
import json
from pathlib import Path
import re
import tempfile
import gzip
import threading
import urllib.request
from typing import Dict, Iterable, List, Optional, Set, Tuple


@dataclass
class Qso:
    freq: str
    mode: str
    date: str
    time: str
    mycall: str
    sent_exch: List[str]
    theircall: str
    recv_exch: List[str]


class ReconstructLedger:
    def __init__(self, path: Path) -> None:
        self.path_txt = path
        suffix = path.suffix + ".gz" if path.suffix else ".gz"
        self.path_gz = path.with_suffix(suffix)
        self._lock = threading.Lock()
        self._loaded = False
        self._entries: Set[str] = set()

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
        except Exception:
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


def ledger_path_for(
    out_dir: Path,
    repo_root: Path,
    ledger_root: Optional[Path],
    ledger_name: str,
) -> Path:
    if ledger_root is None:
        return out_dir / ledger_name
    try:
        rel = out_dir.resolve().relative_to(repo_root.resolve())
    except Exception:
        rel = Path(out_dir.name)
    return ledger_root / rel / ledger_name


def state_path_for(out_dir: Path, repo_root: Path, ledger_root: Optional[Path]) -> Path:
    if ledger_root is None:
        return out_dir / ".reconstruct_state.json"
    try:
        rel = out_dir.resolve().relative_to(repo_root.resolve())
    except Exception:
        rel = Path(out_dir.name)
    return ledger_root / rel / ".reconstruct_state.json"


def collect_contest_stats(contest_dir: Path) -> Dict[str, int]:
    log_count = 0
    total_size = 0
    max_mtime_ns = 0
    for path in iter_logs(contest_dir):
        try:
            stat = path.stat()
        except OSError:
            continue
        log_count += 1
        total_size += stat.st_size
        if stat.st_mtime_ns > max_mtime_ns:
            max_mtime_ns = stat.st_mtime_ns
    return {
        "log_count": log_count,
        "total_size": total_size,
        "max_mtime_ns": max_mtime_ns,
    }


def load_state(path: Path) -> Optional[Dict[str, int]]:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    return data  # type: ignore[return-value]


def save_state(path: Path, data: Dict[str, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, sort_keys=True) + "\n", encoding="utf-8")


def normalize_call(call: str) -> str:
    return call.replace("_", "/").replace("-", "/").upper().strip()


PORTABLE_SUFFIXES = {"P", "M", "MM", "AM", "QRP"}
GRID_RE = re.compile(r"^[A-R]{2}[0-9]{2}([A-X]{2}([0-9]{2})?)?$")
RST_RE = re.compile(r"^[1-5][0-9N]{2}$")


def base_call(call: str) -> str:
    parts = call.split("/")
    if len(parts) == 1:
        return call
    suffix = parts[-1]
    if suffix in PORTABLE_SUFFIXES:
        return parts[0]
    return max(parts, key=len)


def load_master_calls(path: Path) -> Set[str]:
    data = path.read_bytes()
    # Scan binary for call-like tokens.
    tokens = re.findall(rb"[A-Z0-9/]{3,12}", data)
    calls: Set[str] = set()
    for t in tokens:
        if not any(48 <= b <= 57 for b in t):
            continue
        try:
            call = t.decode("ascii")
        except Exception:
            continue
        calls.add(call)
        calls.add(base_call(call))
    return calls


def download_master_dta(url: str) -> Path:
    tmp = tempfile.NamedTemporaryFile(prefix="master_", suffix=".dta", delete=False)
    tmp_path = Path(tmp.name)
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            tmp.write(resp.read())
    finally:
        tmp.close()
    return tmp_path


def looks_like_callsign(token: str) -> bool:
    token = normalize_call(token)
    if not token:
        return False
    if len(token) < 3 or len(token) > 15:
        return False
    if not re.fullmatch(r"[A-Z0-9/]+", token):
        return False
    if token.startswith("/") or token.endswith("/"):
        return False
    if not re.search(r"[A-Z]", token) or not re.search(r"[0-9]", token):
        return False
    if "/" not in token:
        first_digit = next((idx for idx, ch in enumerate(token) if ch.isdigit()), -1)
        if first_digit == -1:
            return False
        if not any(ch.isalpha() for ch in token[first_digit + 1 :]):
            return False
    if "/" not in token and GRID_RE.match(token):
        return False
    if RST_RE.match(token):
        return False
    return True


def find_theircall_index(tokens: List[str]) -> Optional[int]:
    for idx, token in enumerate(tokens):
        if looks_like_callsign(token):
            return idx
    return None


def trim_adjudication_status(tokens: List[str]) -> List[str]:
    trimmed = list(tokens)
    while len(trimmed) > 1 and trimmed[-1] in {"0", "1"}:
        trimmed.pop()
    return trimmed


def parse_qso_line(line: str) -> Optional[Qso]:
    parts = line.strip().split()
    if not parts or parts[0] != "QSO:":
        return None
    if len(parts) < 7:
        return None
    # QSO: freq mode date time mycall sent-exch theircall recv-exch
    tail = parts[6:]
    their_idx = find_theircall_index(tail)
    if their_idx is None:
        return None
    sent_exch = trim_adjudication_status(tail[:their_idx])
    theircall = normalize_call(tail[their_idx])
    recv_exch = trim_adjudication_status(tail[their_idx + 1 :])
    if not sent_exch or not recv_exch:
        return None
    return Qso(
        freq=parts[1],
        mode=parts[2],
        date=parts[3],
        time=parts[4],
        mycall=normalize_call(parts[5]),
        sent_exch=sent_exch,
        theircall=theircall,
        recv_exch=recv_exch,
    )


def iter_logs(contest_dir: Path) -> Iterable[Path]:
    for path in contest_dir.rglob("*.log"):
        if path.is_file():
            yield path


def load_submitted_calls(contest_dir: Path) -> Dict[str, Path]:
    calls: Dict[str, Path] = {}
    for log in iter_logs(contest_dir):
        call = normalize_call(log.stem)
        if call:
            calls[call] = log
    return calls


def build_reconstructed_log(
    call: str,
    qsos: List[Qso],
    contest_name: str,
    season_label: str,
    created_by: str,
) -> str:
    lines = [
        "START-OF-LOG: 3.0",
        f"CREATED-BY: {created_by}",
        f"CONTEST: {contest_name}",
        f"CALLSIGN: {call}",
        "LOCATION: ",
        "CATEGORY: CHECKLOG",
        "CATEGORY-OPERATOR: SINGLE-OP",
        "CATEGORY-BAND: ALL",
        "CATEGORY-MODE: MIXED",
        "CATEGORY-POWER: UNKNOWN",
        "CATEGORY-ASSISTED: NON-ASSISTED",
        "CATEGORY-TRANSMITTER: ONE",
        "CATEGORY-STATION: UNKNOWN",
        "CATEGORY-OVERLAY: ",
        f"OPERATORS: {call}",
        "CLAIMED-SCORE: ",
        "CLUB: ",
        "NAME: ",
        "SOAPBOX: RECONSTRUCTED MOCKUP LOG - NOT AN OFFICIAL SUBMISSION.",
        f"SOAPBOX: Derived from submitted logs for {contest_name} {season_label}.",
    ]

    for q in qsos:
        sent = " ".join(q.recv_exch)
        recv = " ".join(q.sent_exch)
        lines.append(
            f"QSO: {q.freq:>5} {q.mode:<2} {q.date} {q.time:>4} "
            f"{call:<13} {sent} {q.mycall:<13} {recv}"
        )

    lines.append("END-OF-LOG:")
    return "\n".join(lines) + "\n"


def detect_contest_name(log_path: Path, fallback: str) -> str:
    try:
        path = log_path
        if path.is_dir():
            for candidate in path.rglob("*.log"):
                path = candidate
                break
        with path.open("r", errors="ignore") as f:
            for line in f:
                if line.startswith("CONTEST:"):
                    return line.split(":", 1)[1].strip() or fallback
    except Exception:
        pass
    return fallback


def detect_season_label(contest_dir: Path, repo_root: Path) -> str:
    try:
        rel = contest_dir.resolve().relative_to(repo_root.resolve())
        return rel.as_posix()
    except Exception:
        return contest_dir.name


def find_contest_dirs(repo_root: Path, include: Optional[Set[str]]) -> List[Path]:
    exclude = {".git", "analysis", "scripts", "SH6", "RECONSTRUCTED_LOGS"}
    contest_dirs: Set[Path] = set()
    for dirpath, dirnames, filenames in os.walk(repo_root):
        dirnames[:] = [d for d in dirnames if d not in exclude]
        rel = Path(dirpath).resolve()
        try:
            rel_parts = rel.relative_to(repo_root.resolve()).parts
        except Exception:
            rel_parts = ()
        if include and rel_parts:
            if rel_parts[0] not in include:
                continue
        if any(name.lower().endswith(".log") for name in filenames):
            contest_dirs.add(Path(dirpath))
    return sorted(contest_dirs)


def reconstruct_contest(
    contest_dir: Path,
    out_dir: Path,
    master_calls: Set[str],
    min_qsos: int,
    created_by: str,
    contest_name: Optional[str],
    season_label: Optional[str],
    dry_run: bool,
    limit: Optional[int],
    repo_root: Path,
    ledger_root: Optional[Path],
    ledger_name: str,
    use_ledger: bool,
    skip_unchanged: bool,
) -> Tuple[int, int, int, int, int]:
    submitted = load_submitted_calls(contest_dir)
    recon: Dict[str, List[Qso]] = defaultdict(list)
    total_qsos = 0
    skipped_existing = 0
    skipped_unchanged = 0
    ledger = None
    if use_ledger:
        ledger_path = ledger_path_for(out_dir, repo_root, ledger_root, ledger_name)
        ledger = ReconstructLedger(ledger_path)

    stats = collect_contest_stats(contest_dir)
    state_path = state_path_for(out_dir, repo_root, ledger_root)
    if skip_unchanged:
        prior = load_state(state_path)
        if prior:
            if (
                prior.get("log_count") == stats["log_count"]
                and prior.get("total_size") == stats["total_size"]
                and prior.get("max_mtime_ns") == stats["max_mtime_ns"]
            ):
                skipped_unchanged = 1
                return (
                    int(prior.get("submitted_logs", stats["log_count"])),
                    int(prior.get("parsed_qsos", 0)),
                    int(prior.get("reconstructed_logs", 0)),
                    int(prior.get("skipped_existing", 0)),
                    skipped_unchanged,
                )

    for log in iter_logs(contest_dir):
        with log.open("r", errors="ignore") as f:
            for line in f:
                qso = parse_qso_line(line)
                if not qso:
                    continue
                total_qsos += 1
                other = qso.theircall
                if other in submitted:
                    continue
                if other not in master_calls and base_call(other) not in master_calls:
                    continue
                recon[other].append(qso)

    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    contest_name = contest_name or detect_contest_name(contest_dir, contest_dir.name)
    season_label = season_label or detect_season_label(contest_dir, Path(__file__).resolve().parents[1])

    written = 0
    for call, qsos in sorted(recon.items()):
        if limit is not None and written >= limit:
            break
        if len(qsos) < min_qsos:
            continue
        qsos.sort(key=lambda q: (q.date, q.time, q.freq, q.mode, q.mycall))
        dest_path = out_dir / f"{call.replace('/', '_')}.log"
        try:
            key = dest_path.relative_to(repo_root).as_posix()
        except Exception:
            key = dest_path.as_posix()
        if dest_path.exists() or (ledger and ledger.contains(key)):
            skipped_existing += 1
            if ledger and dest_path.exists():
                ledger.add(key, "exists")
            continue
        if dry_run:
            written += 1
            continue
        content = build_reconstructed_log(
            call,
            qsos,
            contest_name,
            season_label,
            created_by,
        )
        dest_path.write_text(content, encoding="utf-8")
        if ledger:
            ledger.add(key, f"{contest_name} {season_label}")
        written += 1

    if not dry_run:
        state = {
            "log_count": stats["log_count"],
            "total_size": stats["total_size"],
            "max_mtime_ns": stats["max_mtime_ns"],
            "submitted_logs": len(submitted),
            "parsed_qsos": total_qsos,
            "reconstructed_logs": written,
            "skipped_existing": skipped_existing,
        }
        save_state(state_path, state)

    return len(submitted), total_qsos, written, skipped_existing, skipped_unchanged


def main() -> int:
    ap = argparse.ArgumentParser(description="Reconstruct mockup Cabrillo logs.")
    ap.add_argument(
        "--contest-dir",
        default=None,
        help="Contest directory containing submitted logs (omit to process all contests)",
    )
    ap.add_argument(
        "--contest",
        action="append",
        default=None,
        help="Top-level contest name to include (can be repeated)",
    )
    ap.add_argument(
        "--out-dir",
        default=None,
        help="Output directory for reconstructed logs (defaults to RECONSTRUCTED_LOGS mirror)",
    )
    ap.add_argument(
        "--out-root",
        default=None,
        help="Root output directory for reconstructed logs",
    )
    ap.add_argument("--contest-name", default=None)
    ap.add_argument("--season-label", default=None)
    ap.add_argument("--created-by", default="reconstructed-log-builder")
    ap.add_argument("--min-qsos", type=int, default=10)
    ap.add_argument(
        "--master-url",
        default="https://www.supercheckpartial.com/MASTER.DTA",
        help="URL to fetch MASTER.DTA (downloaded fresh each run)",
    )
    ap.add_argument("--limit", type=int, default=None, help="Max reconstructed logs per contest")
    ap.add_argument("--dry-run", action="store_true", help="Analyze without writing logs")
    ap.add_argument(
        "--ledger-root",
        type=Path,
        default=Path("scripts") / ".reconstructed_ledgers",
        help="Root directory for per-contest reconstruction ledgers",
    )
    ap.add_argument(
        "--ledger",
        type=Path,
        default=None,
        help="(deprecated) Use --ledger-root to control per-contest ledger placement",
    )
    ap.add_argument("--no-ledger", action="store_true", help="Disable ledger usage")
    ap.add_argument(
        "--skip-unchanged",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip reconstruction when input logs are unchanged (default: true).",
    )
    ap.add_argument(
        "--no-rebuild-shards",
        action="store_true",
        help="Skip rebuilding SH6 SQLite shard indexes after reconstruction.",
    )
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    out_root = Path(args.out_root) if args.out_root else (repo_root / "RECONSTRUCTED_LOGS")
    master_path = download_master_dta(args.master_url)
    try:
        master_calls = load_master_calls(master_path)
    finally:
        try:
            master_path.unlink()
        except Exception:
            pass

    if args.contest_dir:
        contest_dirs = [Path(args.contest_dir)]
    else:
        include = None
        if args.contest:
            include = {c.strip() for entry in args.contest for c in entry.split(",") if c.strip()}
        contest_dirs = find_contest_dirs(repo_root, include)

    if not contest_dirs:
        print("No contest directories found.")
        return 1

    total_submitted = 0
    total_qsos = 0
    total_recon = 0
    total_skipped = 0
    total_skipped_unchanged = 0
    processed = 0

    worker_count = max(1, (os.cpu_count() or 2) - 2)
    print(f"Using {worker_count} worker threads.")

    def resolve_out_dir(contest_dir: Path) -> Path:
        if args.out_dir:
            return Path(args.out_dir)
        try:
            rel = contest_dir.resolve().relative_to(repo_root.resolve())
        except Exception:
            rel = Path(contest_dir.name)
        return out_root / rel

    ledger_root = args.ledger_root
    ledger_name = ".reconstructed_ledger.txt"
    if args.ledger:
        if args.ledger.suffix:
            ledger_root = args.ledger.parent
            ledger_name = args.ledger.name
        else:
            ledger_root = args.ledger
    use_ledger = not args.no_ledger

    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {}
        for contest_dir in contest_dirs:
            out_dir = resolve_out_dir(contest_dir)
            fut = executor.submit(
                reconstruct_contest,
                contest_dir=contest_dir,
                out_dir=out_dir,
                master_calls=master_calls,
                min_qsos=args.min_qsos,
                created_by=args.created_by,
                contest_name=args.contest_name,
                season_label=args.season_label,
                dry_run=args.dry_run,
                limit=args.limit,
                repo_root=repo_root,
                ledger_root=ledger_root,
                ledger_name=ledger_name,
                use_ledger=use_ledger,
                skip_unchanged=args.skip_unchanged,
            )
            future_map[fut] = (contest_dir, out_dir)

        for fut in concurrent.futures.as_completed(future_map):
            contest_dir, out_dir = future_map[fut]
            submitted_count, qsos_count, recon_count, skipped_existing, skipped_unchanged = fut.result()
            contest_name = args.contest_name or detect_contest_name(contest_dir, contest_dir.name)
            season_label = args.season_label or detect_season_label(contest_dir, repo_root)
            processed += 1
            total_submitted += submitted_count
            total_qsos += qsos_count
            total_recon += recon_count
            total_skipped += skipped_existing
            total_skipped_unchanged += skipped_unchanged
            print(
                f"[{contest_dir}] submitted_logs={submitted_count} parsed_qsos={qsos_count} "
                f"reconstructed_logs={recon_count} skipped_existing={skipped_existing} "
                f"skipped_unchanged={skipped_unchanged}"
            )

    print(
        f"total_contests={processed} submitted_logs={total_submitted} "
        f"parsed_qsos={total_qsos} reconstructed_logs={total_recon} "
        f"skipped_existing={total_skipped} skipped_unchanged={total_skipped_unchanged}"
    )
    if args.dry_run:
        print("dry_run=true (no files written)")
    if args.dry_run or args.no_rebuild_shards:
        return 0

    try:
        from public_logs_downloader import build_sqlite_shards  # type: ignore
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Unable to rebuild SH6 shards: {exc}")
        return 1

    shard_dir = repo_root / "SH6"
    print(f"Rebuilding SQLite shards in: {shard_dir}")
    try:
        shard_count = build_sqlite_shards(repo_root, shard_dir)
        print(f"Shard entries: {shard_count}")
        print("Shard rebuild complete.")
    except Exception as exc:  # pylint: disable=broad-except
        print(f"SQLite shard rebuild failed: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
