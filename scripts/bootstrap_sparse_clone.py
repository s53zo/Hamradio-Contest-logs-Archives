#!/usr/bin/env python3
"""Bootstrap a depth-1 blobless sparse clone for archive maintenance."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


DEFAULT_REMOTE = "https://github.com/s53zo/Hamradio-Contest-logs-Archives.git"
DEFAULT_BRANCH = "main"
SPARSE_PATHS = (".github", "scripts", "tests", "state", "SH6")
LOG_EXTENSIONS = {".log", ".adi", ".cbr"}


class BootstrapError(RuntimeError):
    """Raised when the bootstrap clone cannot be completed safely."""


def run_git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def default_destination_name(remote: str) -> str:
    candidate = remote.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1]
    if candidate.endswith(".git"):
        candidate = candidate[:-4]
    if not candidate:
        raise BootstrapError(f"cannot derive destination name from remote {remote!r}")
    return candidate


def ensure_destination_absent(destination: Path) -> None:
    if destination.exists() or destination.is_symlink():
        raise BootstrapError(f"destination already exists: {destination}")


def bootstrap_sparse_clone(
    destination: Path,
    *,
    remote: str = DEFAULT_REMOTE,
    branch: str = DEFAULT_BRANCH,
) -> tuple[Path, int]:
    destination = destination.expanduser()
    ensure_destination_absent(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    run_git(
        destination.parent,
        "clone",
        "--depth",
        "1",
        "--filter=blob:none",
        "--sparse",
        "--single-branch",
        "--branch",
        branch,
        remote,
        destination.name,
    )
    # Cone-mode sparse checkout keeps root files while limiting checked-out directories.
    run_git(destination, "sparse-checkout", "set", "--cone", "--sparse-index", *SPARSE_PATHS)
    remote_logs = verify_sparse_clone(destination)
    return destination, remote_logs


def count_remote_logs(repo: Path) -> int:
    proc = subprocess.Popen(
        ["git", "ls-tree", "-r", "-z", "--name-only", "HEAD"],
        cwd=repo,
        stdout=subprocess.PIPE,
    )
    assert proc.stdout is not None
    count = 0
    pending = b""
    with proc.stdout:
        while chunk := proc.stdout.read(1024 * 1024):
            pending += chunk
            fields = pending.split(b"\0")
            pending = fields.pop()
            count += sum(
                1
                for raw in fields
                if raw and Path(raw.decode("utf-8", errors="surrogateescape")).suffix.lower() in LOG_EXTENSIONS
            )
    if pending and Path(pending.decode("utf-8", errors="surrogateescape")).suffix.lower() in LOG_EXTENSIONS:
        count += 1
    if proc.wait() != 0:
        raise BootstrapError("unable to enumerate remote archive paths")
    return count


def verify_sparse_clone(repo: Path) -> int:
    sparse_paths = set(run_git(repo, "sparse-checkout", "list").stdout.splitlines())
    if sparse_paths != set(SPARSE_PATHS):
        raise BootstrapError(f"unexpected sparse paths: {sorted(sparse_paths)}")
    for required in SPARSE_PATHS:
        if not (repo / required).is_dir():
            raise BootstrapError(f"required sparse directory is absent: {required}")
    top_level = set(run_git(repo, "ls-tree", "-d", "--name-only", "HEAD").stdout.splitlines())
    materialized_archive_dirs = sorted(
        name for name in top_level - set(SPARSE_PATHS) if (repo / name).exists()
    )
    if materialized_archive_dirs:
        raise BootstrapError(
            "non-updater directories were materialized: " + ", ".join(materialized_archive_dirs)
        )
    if run_git(repo, "config", "--get", "remote.origin.partialclonefilter").stdout.strip() != "blob:none":
        raise BootstrapError("clone is not configured with the blob:none filter")
    if run_git(repo, "config", "--bool", "remote.origin.promisor").stdout.strip() != "true":
        raise BootstrapError("origin is not configured as a promisor remote")
    if run_git(repo, "rev-parse", "--is-shallow-repository").stdout.strip() != "true":
        raise BootstrapError("clone is not shallow")
    return count_remote_logs(repo)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", nargs="?", type=Path)
    parser.add_argument("--remote", default=DEFAULT_REMOTE)
    parser.add_argument("--branch", default=DEFAULT_BRANCH)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    destination = args.destination or Path.cwd() / default_destination_name(args.remote)
    try:
        repo, remote_logs = bootstrap_sparse_clone(destination, remote=args.remote, branch=args.branch)
    except (BootstrapError, subprocess.CalledProcessError) as exc:
        message = exc.stderr.strip() if isinstance(exc, subprocess.CalledProcessError) else str(exc)
        print(f"bootstrap failed: {message}", file=sys.stderr)
        return 1
    print(f"Bootstrapped sparse clone at {repo}")
    print(f"Verified remote archive paths: {remote_logs}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
