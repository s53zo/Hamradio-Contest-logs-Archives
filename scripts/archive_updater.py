#!/usr/bin/env python3
"""Run and publish a sparse archive update as one recoverable transaction."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from archive_storage import ARCHIVE_ROOTS, LOG_EXTENSIONS, valid_local_log
from shard_index import apply_path_delta, worktree_log_delta


REPO_ROOT = Path(__file__).resolve().parents[1]
ALLOWED_TRACKED_ROOTS = {"SH6", "state", "RECONSTRUCTED_LOGS"}
ALLOWED_TRACKED_FILES = {"README.md"}
FORBIDDEN_SUFFIXES = {".part", ".tmp", ".sqlite-shm", ".sqlite-wal", ".sqlite-journal"}


class UpdateError(RuntimeError):
    pass


class ConcurrentUpdateError(UpdateError):
    pass


@dataclass
class Transaction:
    schema_version: int
    base_sha: str
    branch: str
    remote: str
    phase: str
    commit_sha: str | None = None


def run_git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def git_dir(repo: Path) -> Path:
    value = run_git(repo, "rev-parse", "--git-dir").stdout.strip()
    path = Path(value)
    return path if path.is_absolute() else (repo / path).resolve()


def journal_path(repo: Path) -> Path:
    return git_dir(repo) / "hcla" / "transaction.json"


def write_transaction(repo: Path, transaction: Transaction) -> None:
    path = journal_path(repo)
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(asdict(transaction), indent=2, sort_keys=True) + "\n"
    temporary = path.with_suffix(".tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def read_transaction(repo: Path) -> Transaction | None:
    path = journal_path(repo)
    if not path.is_file():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    return Transaction(**data)


def clear_transaction(repo: Path) -> None:
    path = journal_path(repo)
    if path.exists():
        path.unlink()


def current_branch(repo: Path) -> str:
    branch = run_git(repo, "branch", "--show-current").stdout.strip()
    if not branch:
        raise UpdateError("archive updater requires a named branch")
    return branch


def current_sha(repo: Path) -> str:
    return run_git(repo, "rev-parse", "HEAD").stdout.strip()


def remote_sha(repo: Path, remote: str, branch: str) -> str:
    run_git(repo, "fetch", "--prune", remote, branch)
    return run_git(repo, "rev-parse", f"{remote}/{branch}").stdout.strip()


def porcelain_paths(repo: Path) -> list[tuple[str, Path]]:
    proc = subprocess.run(
        ["git", "status", "--porcelain=v1", "-z", "--untracked-files=all"],
        cwd=repo,
        check=True,
        stdout=subprocess.PIPE,
    )
    fields = [field for field in proc.stdout.split(b"\0") if field]
    result: list[tuple[str, Path]] = []
    index = 0
    while index < len(fields):
        field = fields[index]
        status = field[:2].decode("ascii", errors="replace")
        path = Path(field[3:].decode("utf-8", errors="surrogateescape"))
        result.append((status, path))
        index += 2 if "R" in status or "C" in status else 1
    return result


def assert_clean_start(repo: Path) -> None:
    changes = porcelain_paths(repo)
    if changes:
        sample = ", ".join(f"{status} {path}" for status, path in changes[:10])
        raise UpdateError(f"working tree is not clean; resume or resolve it first: {sample}")


def assert_current(repo: Path, remote: str, branch: str) -> str:
    local = current_sha(repo)
    upstream = remote_sha(repo, remote, branch)
    if local != upstream:
        raise UpdateError(
            f"local {branch} is not current (local={local[:12]} remote={upstream[:12]}); "
            f"run git pull --ff-only {remote} {branch}"
        )
    return local


def adopt_fast_forwarded_transaction_head(repo: Path, transaction: Transaction) -> bool:
    local = current_sha(repo)
    if local == transaction.base_sha:
        return False
    if transaction.commit_sha is not None or transaction.phase in {"committed", "publishing"}:
        raise UpdateError("transaction journal does not match HEAD; inspect .git/hcla/transaction.json")
    if run_git(
        repo,
        "merge-base",
        "--is-ancestor",
        transaction.base_sha,
        local,
        check=False,
    ).returncode != 0:
        raise UpdateError("transaction base is not an ancestor of HEAD; inspect .git/hcla/transaction.json")
    upstream = remote_sha(repo, transaction.remote, transaction.branch)
    if upstream != local:
        raise UpdateError(
            "interrupted transaction can adopt only the current remote head; "
            f"run git pull --ff-only {transaction.remote} {transaction.branch}"
        )
    previous = transaction.base_sha
    transaction.base_sha = local
    write_transaction(repo, transaction)
    print(
        "Adopted remote fast-forward for interrupted transaction: "
        f"{previous[:12]} -> {local[:12]}"
    )
    return True


def run_child(repo: Path, argv: list[str], label: str) -> None:
    print(f"\n[{label}] {' '.join(argv)}")
    process = subprocess.Popen(argv, cwd=repo, start_new_session=True)
    try:
        returncode = process.wait()
    except KeyboardInterrupt:
        print(f"\nInterrupting {label}...", file=sys.stderr)
        stop_process_group(process)
        raise
    if returncode != 0:
        raise UpdateError(f"{label} failed with exit {returncode}")


def stop_process_group(
    process: subprocess.Popen,
    graceful_timeout: float = 15,
    terminate_timeout: float = 5,
) -> int:
    if process.poll() is not None:
        return int(process.returncode)
    os.killpg(process.pid, signal.SIGINT)
    try:
        return process.wait(timeout=graceful_timeout)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGTERM)
    try:
        return process.wait(timeout=terminate_timeout)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        return process.wait()


def source_log_changes(repo: Path) -> list[Path]:
    delta = worktree_log_delta(repo)
    if delta.deleted:
        sample = ", ".join(path.as_posix() for path in delta.deleted[:10])
        raise UpdateError(f"archive update refuses implicit log deletions: {sample}")
    return [
        path
        for path in delta.added_or_modified
        if path.parts and path.parts[0] != "RECONSTRUCTED_LOGS"
    ]


def update_shards(repo: Path) -> set[Path]:
    delta = worktree_log_delta(repo)
    if delta.deleted:
        sample = ", ".join(path.as_posix() for path in delta.deleted[:10])
        raise UpdateError(f"archive update refuses implicit log deletions: {sample}")
    result = apply_path_delta(repo, delta.added_or_modified)
    print(
        f"SH6 incremental update: upserted={result.upserted} unchanged={result.unchanged} "
        f"changed_shards={len(result.changed_shards)}"
    )
    return result.changed_shards


def update_readme(repo: Path) -> None:
    from public_logs_downloader import update_readme_from_shards

    update_readme_from_shards(repo, repo / "SH6")


def validate_changes(repo: Path) -> None:
    delta = worktree_log_delta(repo)
    if delta.deleted:
        raise UpdateError("validation rejected deleted archive logs")
    invalid = [path for path in delta.added_or_modified if not valid_local_log(repo / path)]
    if invalid:
        raise UpdateError("invalid generated logs: " + ", ".join(path.as_posix() for path in invalid[:10]))
    for status, path in porcelain_paths(repo):
        if any(path.name.endswith(suffix) for suffix in FORBIDDEN_SUFFIXES):
            raise UpdateError(f"transient file must not be published: {path}")
        if path.name.startswith(".env") or "cookie" in path.name.lower():
            raise UpdateError(f"possible credential file must not be published: {path}")
        if "D" in status and path.suffix.lower() in LOG_EXTENSIONS:
            raise UpdateError(f"log deletion must be explicitly reviewed: {path}")
    check = run_git(repo, "diff", "--check", check=False)
    if check.returncode != 0:
        raise UpdateError(check.stdout + check.stderr)


def is_transient_path(path: Path) -> bool:
    return any(path.name.endswith(suffix) for suffix in FORBIDDEN_SUFFIXES)


def cleanup_orphaned_transients(repo: Path) -> list[Path]:
    removed: list[Path] = []
    generated_roots = ARCHIVE_ROOTS | ALLOWED_TRACKED_ROOTS
    for status, path in porcelain_paths(repo):
        if status != "??" or not path.parts or path.parts[0] not in generated_roots:
            continue
        if not is_transient_path(path):
            continue
        candidate = repo / path
        if candidate.is_file() or candidate.is_symlink():
            candidate.unlink()
            removed.append(path)
    if removed:
        print(f"Removed {len(removed)} orphaned updater temporary files.")
    return removed


def migrate_legacy_provider_state(repo: Path) -> None:
    from download_vhfmanager_logs import migrate_legacy_checklog_markers

    migrate_legacy_checklog_markers(repo)


def allowed_generated_path(path: Path) -> bool:
    if path in {Path(value) for value in ALLOWED_TRACKED_FILES}:
        return True
    if not path.parts:
        return False
    if path.parts[0] in ALLOWED_TRACKED_ROOTS:
        return True
    return path.parts[0] in ARCHIVE_ROOTS and path.suffix.lower() in LOG_EXTENSIONS


def stage_generated_changes(repo: Path) -> list[Path]:
    paths = [path for _status, path in porcelain_paths(repo)]
    disallowed = [path for path in paths if not allowed_generated_path(path)]
    if disallowed:
        raise UpdateError(
            "refusing to stage unrelated paths: " + ", ".join(path.as_posix() for path in disallowed[:10])
        )
    if paths:
        run_git(repo, "add", "--sparse", "--", *[path.as_posix() for path in paths])
    return paths


def commit_update(repo: Path, title: str) -> str | None:
    staged = run_git(repo, "diff", "--cached", "--quiet", check=False)
    if staged.returncode == 0:
        return None
    body = (
        "Publish newly available contest logs with matching durable state and SH6 indexes.\n\n"
        "Constraint: Existing contest log paths are immutable raw GitHub interfaces\n"
        "Confidence: high\n"
        "Scope-risk: moderate\n"
        "Directive: Keep logs, state, and SH6 updates in the same publication transaction\n"
        "Tested: Archive validation and repository unit tests"
    )
    run_git(repo, "commit", "-m", title, "-m", body)
    return current_sha(repo)


def reconcile_and_push(repo: Path, transaction: Transaction) -> str:
    upstream = remote_sha(repo, transaction.remote, transaction.branch)
    local = current_sha(repo)
    if upstream != transaction.base_sha and not run_git(
        repo, "merge-base", "--is-ancestor", upstream, local, check=False
    ).returncode == 0:
        rebase = run_git(repo, "rebase", f"{transaction.remote}/{transaction.branch}", check=False)
        if rebase.returncode != 0:
            run_git(repo, "rebase", "--abort", check=False)
            raise ConcurrentUpdateError(
                "remote advanced and the update conflicts; local rebase was aborted. "
                f"Fetch and inspect {transaction.remote}/{transaction.branch}, then rerun the updater."
            )
        local = current_sha(repo)
        transaction.commit_sha = local
        write_transaction(repo, transaction)
    push = run_git(
        repo,
        "push",
        transaction.remote,
        f"HEAD:{transaction.branch}",
        check=False,
    )
    if push.returncode != 0:
        raise ConcurrentUpdateError(
            "push was rejected without overwriting the remote; rerun to fetch and reconcile.\n"
            + push.stderr
        )
    remote_after = remote_sha(repo, transaction.remote, transaction.branch)
    if remote_after != current_sha(repo):
        raise ConcurrentUpdateError("remote changed again after push verification")
    return remote_after


def sparse_cleanup(repo: Path) -> None:
    sparse = run_git(repo, "config", "--bool", "core.sparseCheckout", check=False)
    if sparse.returncode == 0 and sparse.stdout.strip() == "true":
        run_git(repo, "sparse-checkout", "reapply", "--sparse-index")


def run_tests(repo: Path) -> None:
    run_child(
        repo,
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py"],
        "tests",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=REPO_ROOT)
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--branch", default=None)
    parser.add_argument("--phase", choices=("all", "download", "reconstruct", "shards"), default="all")
    parser.add_argument("--contests", default="all")
    parser.add_argument("--last", default="1")
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--publish", action="store_true")
    parser.add_argument(
        "--resume-existing",
        action="store_true",
        help="Adopt valid uncommitted downloader output created before the transaction journal existed.",
    )
    parser.add_argument("--skip-tests", action="store_true")
    parser.add_argument("--commit-title", default="Keep the public contest archive current")
    args = parser.parse_args()

    repo = args.repo.resolve()
    branch = args.branch or current_branch(repo)
    migrate_legacy_provider_state(repo)
    transaction = read_transaction(repo)
    if transaction and transaction.phase in {"committed", "publishing"}:
        if not transaction.commit_sha or transaction.commit_sha != current_sha(repo):
            raise UpdateError(
                "post-commit transaction does not match HEAD; inspect "
                ".git/hcla/transaction.json"
            )
        if not args.publish:
            print(f"Pending local update commit: {transaction.commit_sha}")
            print("Rerun with --publish to reconcile and push it.")
            return 0
        transaction.phase = "publishing"
        write_transaction(repo, transaction)
        published = reconcile_and_push(repo, transaction)
        clear_transaction(repo)
        sparse_cleanup(repo)
        print(f"Published and verified: {published}")
        return 0

    if transaction is not None:
        cleanup_orphaned_transients(repo)

    if transaction is None:
        if args.resume_existing:
            if not porcelain_paths(repo):
                raise UpdateError("--resume-existing was requested but the working tree is clean")
            validate_changes(repo)
        else:
            assert_clean_start(repo)
        base = assert_current(repo, args.remote, branch)
        transaction = Transaction(1, base, branch, args.remote, "prepared")
        if args.dry_run:
            print(f"Dry run: base={base} branch={branch} contests={args.contests} last={args.last}")
            print("No downloader, state, SH6, commit, or push changes were made.")
            return 0
        write_transaction(repo, transaction)
    else:
        adopt_fast_forwarded_transaction_head(repo, transaction)

    try:
        if args.phase in {"all", "download"}:
            transaction.phase = "downloading"
            write_transaction(repo, transaction)
            run_child(
                repo,
                [
                    sys.executable,
                    "scripts/public_logs_downloader.py",
                    "--non-interactive",
                    "--contests",
                    args.contests,
                    "--last",
                    args.last,
                    "--workers",
                    str(args.workers),
                    "--no-post-download-shards",
                ],
                "download",
            )
            transaction.phase = "downloaded"
            write_transaction(repo, transaction)
            if args.phase == "download":
                print("Download phase complete. Rerun with --phase reconstruct to continue.")
                return 0

        if args.phase in {"all", "reconstruct"}:
            transaction.phase = "reconstructing"
            write_transaction(repo, transaction)
            if source_log_changes(repo):
                run_child(
                    repo,
                    [sys.executable, "scripts/reconstruct_missing_logs.py", "--changed-only", "--no-rebuild-shards"],
                    "reconstruction",
                )
            else:
                print("No changed source logs require reconstruction.")
            transaction.phase = "reconstructed"
            write_transaction(repo, transaction)
            if args.phase == "reconstruct":
                print("Reconstruction phase complete. Rerun with --phase shards to continue.")
                return 0

        if args.phase in {"all", "shards"}:
            transaction.phase = "indexing"
            write_transaction(repo, transaction)
            update_shards(repo)
            update_readme(repo)
            transaction.phase = "indexed"
            write_transaction(repo, transaction)

        if not porcelain_paths(repo):
            clear_transaction(repo)
            sparse_cleanup(repo)
            print("Archive is already current; no tracked files changed.")
            return 0
        validate_changes(repo)
        if not args.skip_tests:
            run_tests(repo)
        stage_generated_changes(repo)
        commit_sha = commit_update(repo, args.commit_title)
        if commit_sha is None:
            clear_transaction(repo)
            sparse_cleanup(repo)
            print("Archive is already current; no commit was created.")
            return 0
        transaction.phase = "committed"
        transaction.commit_sha = commit_sha
        write_transaction(repo, transaction)
        print(f"Created update commit: {commit_sha}")
        if not args.publish:
            print("Validated commit is local. Rerun with --publish to publish it.")
            return 0
        transaction.phase = "publishing"
        write_transaction(repo, transaction)
        published = reconcile_and_push(repo, transaction)
        clear_transaction(repo)
        sparse_cleanup(repo)
        print(f"Published and verified: {published}")
        return 0
    except KeyboardInterrupt:
        transaction.phase = "interrupted"
        write_transaction(repo, transaction)
        print("Update interrupted. Rerun the same command to resume from durable outputs.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except UpdateError as exc:
        print(f"archive updater: {exc}", file=sys.stderr)
        raise SystemExit(1)
