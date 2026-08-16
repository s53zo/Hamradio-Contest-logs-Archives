#!/usr/bin/env python3
"""Remote-aware archive inventory helpers for sparse updater clones."""

from __future__ import annotations

import sqlite3
import subprocess
import tarfile
import threading
import zlib
import os
from contextlib import closing
from pathlib import Path
from shutil import copyfileobj
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
        proc = subprocess.Popen(
            ["git", "archive", "--format=tar", self.revision, "--", rel_prefix.as_posix()],
            cwd=self.repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert proc.stdout is not None
        written: list[Path] = []
        try:
            with tarfile.open(fileobj=proc.stdout, mode="r|*") as archive:
                for member in archive:
                    if not member.isfile():
                        continue
                    rel = self.normalize(member.name)
                    target = (destination / rel).resolve()
                    if destination not in target.parents:
                        raise ValueError(f"unsafe archive member: {member.name}")
                    source = archive.extractfile(member)
                    if source is None:
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with source, target.open("wb") as output:
                        copyfileobj(source, output)
                    written.append(target)
        finally:
            proc.stdout.close()
        stderr = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
        if proc.stderr:
            proc.stderr.close()
        returncode = proc.wait()
        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, proc.args, stderr=stderr)
        return written


_DEFAULT_INVENTORY = ArchiveInventory()


def archive_log_exists(value: str | Path) -> bool:
    try:
        return _DEFAULT_INVENTORY.log_exists(value)
    except ValueError:
        path = Path(value)
        return valid_local_log(path) if path.is_absolute() else False
