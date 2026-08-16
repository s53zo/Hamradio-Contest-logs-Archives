#!/usr/bin/env python3
"""Remote-aware archive inventory helpers for sparse updater clones."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import threading
import zlib
from contextlib import closing
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
LOG_EXTENSIONS = {".log", ".adi", ".cbr"}
ARCHIVE_ROOTS = {
    "9A_HRS_Contest",
    "ARRL",
    "CQ160",
    "CQWPX",
    "CQWPXRTTY",
    "CQWW",
    "CQWWRTTY",
    "DARC",
    "EUDX_contest",
    "EUHFC",
    "EU_VHF_CONTESTS",
    "HamSpiritContest",
    "Istra_Open_Contest",
    "OK_Contest",
    "OK1WC_Memorial",
    "OK_DX_RTTY_contest",
    "OK_OM_DX_Contest",
    "RCCCup",
    "RDAContest",
    "RECONSTRUCTED_LOGS",
    "REF",
    "RFChampionshipCW",
    "RussianDXContest",
    "RussianRadioTeamChampionship",
    "SAC",
    "SPDX_contest",
    "TTC-SPCWC",
    "URE",
    "WAE",
    "WRTC",
    "WWDIGI",
    "WW_PMC",
    "WednesdayMiniTest40m",
    "WednesdayMiniTest80m",
    "YOTA_Contest",
    "YU_DX_Contest",
    "YuriGagarinDXContest",
    "ZRS_KVP",
}


def atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.part")
    try:
        temporary.write_bytes(content)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    atomic_write_bytes(path, content.encode(encoding))


def callsign_bucket(callsign: str) -> int:
    if not callsign:
        return 0
    return zlib.crc32(callsign.upper().encode("ascii", errors="ignore")) & 0xFF


def valid_local_log(path: Path) -> bool:
    if not path.is_file():
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


class ArchiveInventory:
    """Resolve local and remote-only archive paths without fetching log blobs."""

    def __init__(self, repo_root: Path = REPO_ROOT, revision: str = "HEAD") -> None:
        self.repo_root = repo_root.resolve()
        self.shard_root = self.repo_root / "SH6"
        self.revision = revision
        self._remote_cache: dict[str, bool] = {}
        self._cache_lock = threading.Lock()

    def normalize(self, value: str | Path) -> Path:
        path = Path(value)
        if path.is_absolute():
            try:
                path = path.resolve().relative_to(self.repo_root)
            except ValueError as exc:
                raise ValueError(f"archive path is outside repository: {value}") from exc
        if not path.parts or ".." in path.parts:
            raise ValueError(f"unsafe archive path: {value}")
        return Path(*path.parts)

    def local_path(self, value: str | Path) -> Path:
        return self.repo_root / self.normalize(value)

    def shard_path(self, value: str | Path) -> Path:
        rel = self.normalize(value)
        bucket = callsign_bucket(rel.stem.upper())
        return self.shard_root / f"logs_{bucket:02x}.sqlite"

    def indexed(self, value: str | Path) -> bool:
        rel = self.normalize(value)
        key = rel.as_posix()
        with self._cache_lock:
            cached = self._remote_cache.get(key)
        if cached is not None:
            return cached

        shard = self.shard_path(rel)
        found = False
        if shard.is_file():
            uri = f"{shard.resolve().as_uri()}?mode=ro"
            try:
                with closing(sqlite3.connect(uri, uri=True)) as conn:
                    row = conn.execute(
                        "SELECT 1 FROM logs WHERE path = ? LIMIT 1",
                        (key,),
                    ).fetchone()
                found = row is not None
            except sqlite3.Error:
                found = False
        with self._cache_lock:
            self._remote_cache[key] = found
        return found

    def log_exists(self, value: str | Path) -> bool:
        rel = self.normalize(value)
        local = self.repo_root / rel
        if local.exists():
            return valid_local_log(local)
        if rel.suffix.lower() not in LOG_EXTENSIONS:
            return False
        return self.indexed(rel)

    def git_paths(
        self,
        prefix: str | Path | None = None,
        *,
        log_only: bool = True,
    ) -> list[Path]:
        command = ["git", "ls-tree", "-r", "-z", "--name-only", self.revision]
        if prefix is not None:
            command.extend(["--", self.normalize(prefix).as_posix()])
        proc = subprocess.run(
            command,
            cwd=self.repo_root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        paths = [Path(raw.decode("utf-8", errors="surrogateescape")) for raw in proc.stdout.split(b"\0") if raw]
        if log_only:
            paths = [path for path in paths if path.suffix.lower() in LOG_EXTENSIONS]
        return paths

    def read_git_blob(self, value: str | Path) -> bytes:
        rel = self.normalize(value)
        proc = subprocess.run(
            ["git", "show", f"{self.revision}:{rel.as_posix()}"],
            cwd=self.repo_root,
            check=True,
            stdout=subprocess.PIPE,
        )
        return proc.stdout

    def materialize(self, paths: Iterable[str | Path], destination: Path) -> list[Path]:
        written: list[Path] = []
        destination.mkdir(parents=True, exist_ok=True)
        for value in paths:
            rel = self.normalize(value)
            target = destination / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(self.read_git_blob(rel))
            written.append(target)
        return written

    def materialize_prefix(self, prefix: str | Path, destination: Path) -> list[Path]:
        rel_prefix = self.normalize(prefix)
        destination = destination.resolve()
        destination.mkdir(parents=True, exist_ok=True)

        proc = subprocess.run(
            ["git", "ls-tree", "-r", "-z", self.revision, "--", rel_prefix.as_posix()],
            cwd=self.repo_root,
            check=True,
            stdout=subprocess.PIPE,
        )
        entries: list[tuple[bytes, Path]] = []
        for raw in proc.stdout.split(b"\0"):
            if not raw:
                continue
            metadata, raw_path = raw.split(b"\t", 1)
            _mode, object_type, object_id = metadata.split(b" ", 2)
            if object_type != b"blob":
                continue
            path = self.normalize(raw_path.decode("utf-8", errors="surrogateescape"))
            entries.append((object_id, path))

        if not entries:
            return []

        object_ids = list(dict.fromkeys(object_id for object_id, _path in entries))
        missing = self._missing_git_objects(object_ids)
        if missing:
            promisor = subprocess.run(
                ["git", "config", "--bool", "remote.origin.promisor"],
                cwd=self.repo_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            if promisor.returncode != 0 or promisor.stdout.strip() != "true":
                raise RuntimeError("cannot materialize missing Git blobs without promisor origin")
            subprocess.run(
                [
                    "git",
                    "-c",
                    "fetch.negotiationAlgorithm=noop",
                    "fetch",
                    "origin",
                    "--no-tags",
                    "--no-write-fetch-head",
                    "--recurse-submodules=no",
                    "--filter=blob:none",
                    "--stdin",
                ],
                cwd=self.repo_root,
                check=True,
                input=b"".join(object_id + b"\n" for object_id in missing),
                stdout=subprocess.DEVNULL,
            )
            remaining = self._missing_git_objects(missing)
            if remaining:
                raise RuntimeError(f"promisor remote did not provide {len(remaining)} required blobs")

        written: list[Path] = []
        for object_id, rel in entries:
            target = (destination / rel).resolve()
            if destination not in target.parents:
                raise ValueError(f"unsafe materialization path: {rel}")
            blob = subprocess.run(
                ["git", "cat-file", "blob", object_id.decode("ascii")],
                cwd=self.repo_root,
                check=True,
                stdout=subprocess.PIPE,
            ).stdout
            atomic_write_bytes(target, blob)
            written.append(target)
        return written

    def _missing_git_objects(self, object_ids: Iterable[bytes]) -> list[bytes]:
        unique_ids = list(dict.fromkeys(object_ids))
        if not unique_ids:
            return []
        env = os.environ.copy()
        env["GIT_NO_LAZY_FETCH"] = "1"
        proc = subprocess.run(
            ["git", "cat-file", "--batch-check=%(objectname) %(objecttype)"],
            cwd=self.repo_root,
            check=True,
            input=b"".join(object_id + b"\n" for object_id in unique_ids),
            stdout=subprocess.PIPE,
            env=env,
        )
        results = proc.stdout.splitlines()
        if len(results) != len(unique_ids):
            raise RuntimeError("unexpected response while checking Git blob availability")
        return [
            object_id
            for object_id, result in zip(unique_ids, results)
            if result.endswith(b" missing")
        ]


_DEFAULT_INVENTORY = ArchiveInventory()


def archive_log_exists(value: str | Path) -> bool:
    try:
        return _DEFAULT_INVENTORY.log_exists(value)
    except ValueError:
        path = Path(value)
        return valid_local_log(path) if path.is_absolute() else False
