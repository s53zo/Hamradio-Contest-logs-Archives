#!/usr/bin/env python3
"""Incremental SH6 maintenance and Git-tree path auditing."""

from __future__ import annotations

import argparse
import sqlite3
import subprocess
import tempfile
from collections import defaultdict
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from archive_storage import LOG_EXTENSIONS, callsign_bucket, valid_local_log


LOG_COLUMNS = ("path", "callsign", "contest", "year", "mode", "season", "subcontest", "detail")


@dataclass
class DeltaResult:
    upserted: int = 0
    deleted: int = 0
    unchanged: int = 0
    changed_shards: set[Path] = field(default_factory=set)


@dataclass
class AuditResult:
    expected: int
    indexed: int
    missing: list[str]
    extra: list[str]

    @property
    def clean(self) -> bool:
        return not self.missing and not self.extra and self.expected == self.indexed


@dataclass
class WorktreeDelta:
    added_or_modified: list[Path]
    deleted: list[Path]


def normalize_path(repo_root: Path, value: str | Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        path = path.resolve().relative_to(repo_root.resolve())
    if not path.parts or ".." in path.parts:
        raise ValueError(f"unsafe archive path: {value}")
    return Path(*path.parts)


def shard_path_for(shard_root: Path, rel_path: Path) -> Path:
    return shard_root / f"logs_{callsign_bucket(rel_path.stem.upper()):02x}.sqlite"


def create_logs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS logs (
            path TEXT NOT NULL,
            callsign TEXT NOT NULL,
            contest TEXT,
            year INTEGER,
            mode TEXT,
            season TEXT,
            subcontest TEXT,
            detail TEXT
        )
        """
    )


def ensure_schema(conn: sqlite3.Connection) -> None:
    create_logs_table(conn)
    conn.execute(
        "DELETE FROM logs WHERE rowid NOT IN (SELECT MIN(rowid) FROM logs GROUP BY path)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_callsign ON logs(callsign)")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_path ON logs(path)")


def ensure_path_index(shard_path: Path) -> None:
    with closing(sqlite3.connect(shard_path)) as conn:
        with conn:
            ensure_schema(conn)


def migrate_path_indexes(shard_root: Path) -> int:
    migrated = 0
    for shard in sorted(shard_root.glob("logs_*.sqlite")):
        ensure_path_index(shard)
        migrated += 1
    return migrated


def _manifest_record(repo_root: Path, rel_path: Path) -> tuple[object, ...]:
    from public_logs_downloader import manifest_record_from_path

    full_path = repo_root / rel_path
    if not valid_local_log(full_path):
        raise ValueError(f"missing or invalid local log for SH6 update: {rel_path}")
    record = manifest_record_from_path(rel_path, full_path)
    callsign = str(record.get("callsign") or "").upper()
    return (
        record.get("path"),
        callsign,
        record.get("contest"),
        record.get("year"),
        record.get("mode"),
        record.get("season"),
        record.get("subcontest"),
        record.get("detail"),
    )


def apply_path_delta(
    repo_root: Path,
    added_or_modified: Iterable[str | Path],
    *,
    deleted: Iterable[str | Path] = (),
    shard_root: Path | None = None,
) -> DeltaResult:
    repo_root = repo_root.resolve()
    shard_root = shard_root or (repo_root / "SH6")
    shard_root.mkdir(parents=True, exist_ok=True)
    additions: dict[Path, list[Path]] = defaultdict(list)
    deletions: dict[Path, list[Path]] = defaultdict(list)

    for value in added_or_modified:
        rel = normalize_path(repo_root, value)
        if rel.suffix.lower() in LOG_EXTENSIONS:
            additions[shard_path_for(shard_root, rel)].append(rel)
    for value in deleted:
        rel = normalize_path(repo_root, value)
        if rel.suffix.lower() in LOG_EXTENSIONS:
            deletions[shard_path_for(shard_root, rel)].append(rel)

    result = DeltaResult()
    for shard in sorted(set(additions) | set(deletions)):
        before_changes = 0
        with closing(sqlite3.connect(shard)) as conn:
            with conn:
                ensure_schema(conn)
                before_changes = conn.total_changes
                for rel in deletions.get(shard, []):
                    cursor = conn.execute("DELETE FROM logs WHERE path = ?", (rel.as_posix(),))
                    result.deleted += max(0, cursor.rowcount)
                for rel in additions.get(shard, []):
                    values = _manifest_record(repo_root, rel)
                    cursor = conn.execute(
                        """
                        INSERT INTO logs (path, callsign, contest, year, mode, season, subcontest, detail)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(path) DO UPDATE SET
                            callsign=excluded.callsign,
                            contest=excluded.contest,
                            year=excluded.year,
                            mode=excluded.mode,
                            season=excluded.season,
                            subcontest=excluded.subcontest,
                            detail=excluded.detail
                        WHERE logs.callsign IS NOT excluded.callsign
                           OR logs.contest IS NOT excluded.contest
                           OR logs.year IS NOT excluded.year
                           OR logs.mode IS NOT excluded.mode
                           OR logs.season IS NOT excluded.season
                           OR logs.subcontest IS NOT excluded.subcontest
                           OR logs.detail IS NOT excluded.detail
                        """,
                        values,
                    )
                    if cursor.rowcount:
                        result.upserted += 1
                    else:
                        result.unchanged += 1
                if conn.total_changes > before_changes:
                    result.changed_shards.add(shard)
    return result


def worktree_log_delta(repo_root: Path, revision: str = "HEAD") -> WorktreeDelta:
    repo_root = repo_root.resolve()
    proc = subprocess.run(
        ["git", "diff", "--no-renames", "--name-status", "-z", revision],
        cwd=repo_root,
        check=True,
        stdout=subprocess.PIPE,
    )
    fields = [field for field in proc.stdout.split(b"\0") if field]
    added_or_modified: set[Path] = set()
    deleted: set[Path] = set()
    index = 0
    while index + 1 < len(fields):
        status = fields[index].decode("ascii", errors="replace")
        path = Path(fields[index + 1].decode("utf-8", errors="surrogateescape"))
        index += 2
        if path.suffix.lower() not in LOG_EXTENSIONS:
            continue
        if status.startswith("D"):
            deleted.add(path)
        else:
            added_or_modified.add(path)

    untracked = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"],
        cwd=repo_root,
        check=True,
        stdout=subprocess.PIPE,
    )
    for raw in untracked.stdout.split(b"\0"):
        if not raw:
            continue
        path = Path(raw.decode("utf-8", errors="surrogateescape"))
        if path.suffix.lower() in LOG_EXTENSIONS:
            added_or_modified.add(path)
    return WorktreeDelta(
        added_or_modified=sorted(added_or_modified),
        deleted=sorted(deleted),
    )


def _iter_git_paths(repo_root: Path, revision: str) -> Iterable[str]:
    proc = subprocess.Popen(
        ["git", "ls-tree", "-r", "-z", "--name-only", revision],
        cwd=repo_root,
        stdout=subprocess.PIPE,
    )
    assert proc.stdout is not None
    pending = b""
    with proc.stdout:
        while chunk := proc.stdout.read(1024 * 1024):
            pending += chunk
            parts = pending.split(b"\0")
            pending = parts.pop()
            for raw in parts:
                if raw:
                    yield raw.decode("utf-8", errors="surrogateescape")
    if pending:
        yield pending.decode("utf-8", errors="surrogateescape")
    if proc.wait() != 0:
        raise subprocess.CalledProcessError(proc.returncode, proc.args)


def audit_git_tree(
    repo_root: Path,
    *,
    revision: str = "HEAD",
    shard_root: Path | None = None,
    sample_limit: int = 100,
) -> AuditResult:
    repo_root = repo_root.resolve()
    shard_root = shard_root or (repo_root / "SH6")
    with tempfile.TemporaryDirectory(prefix="hcla-shard-audit-") as temp:
        database = Path(temp) / "audit.sqlite"
        with closing(sqlite3.connect(database)) as conn:
            conn.execute("CREATE TABLE expected(path TEXT PRIMARY KEY) WITHOUT ROWID")
            conn.execute("CREATE TABLE indexed(path TEXT PRIMARY KEY) WITHOUT ROWID")
            batch: list[tuple[str]] = []
            for path in _iter_git_paths(repo_root, revision):
                if Path(path).suffix.lower() not in LOG_EXTENSIONS:
                    continue
                batch.append((path,))
                if len(batch) >= 10000:
                    conn.executemany("INSERT OR IGNORE INTO expected(path) VALUES (?)", batch)
                    batch.clear()
            if batch:
                conn.executemany("INSERT OR IGNORE INTO expected(path) VALUES (?)", batch)

            for shard in sorted(shard_root.glob("logs_*.sqlite")):
                with closing(sqlite3.connect(f"{shard.resolve().as_uri()}?mode=ro", uri=True)) as source:
                    rows = source.execute("SELECT path FROM logs")
                    while values := rows.fetchmany(10000):
                        conn.executemany("INSERT OR IGNORE INTO indexed(path) VALUES (?)", values)
            conn.commit()
            expected = int(conn.execute("SELECT count(*) FROM expected").fetchone()[0])
            indexed = int(conn.execute("SELECT count(*) FROM indexed").fetchone()[0])
            missing = [
                row[0]
                for row in conn.execute(
                    "SELECT path FROM expected EXCEPT SELECT path FROM indexed LIMIT ?",
                    (sample_limit,),
                )
            ]
            extra = [
                row[0]
                for row in conn.execute(
                    "SELECT path FROM indexed EXCEPT SELECT path FROM expected LIMIT ?",
                    (sample_limit,),
                )
            ]
    return AuditResult(expected=expected, indexed=indexed, missing=missing, extra=extra)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("migrate-indexes")
    audit_parser = subparsers.add_parser("audit")
    audit_parser.add_argument("--revision", default="HEAD")
    args = parser.parse_args()

    if args.command == "migrate-indexes":
        count = migrate_path_indexes(args.repo / "SH6")
        print(f"SH6 path indexes migrated: {count}")
        return 0
    result = audit_git_tree(args.repo, revision=args.revision)
    print(
        f"SH6 path audit: expected={result.expected} indexed={result.indexed} "
        f"missing={len(result.missing)} extra={len(result.extra)}"
    )
    for path in result.missing:
        print(f"missing: {path}")
    for path in result.extra:
        print(f"extra: {path}")
    return 0 if result.clean else 1


if __name__ == "__main__":
    raise SystemExit(main())
