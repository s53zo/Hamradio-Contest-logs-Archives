#!/usr/bin/env python3
"""Run and publish a sparse archive update as one recoverable transaction."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from archive_storage import ARCHIVE_ROOTS, LOG_EXTENSIONS, ArchiveInventory, atomic_write_bytes, atomic_write_text, valid_local_log
from shard_index import apply_path_delta, worktree_log_delta


REPO_ROOT = Path(__file__).resolve().parents[1]
ALLOWED_TRACKED_ROOTS = {"SH6", "state", "RECONSTRUCTED_LOGS"}
ALLOWED_TRACKED_FILES = {"README.md"}
FORBIDDEN_SUFFIXES = {".part", ".tmp", ".sqlite-shm", ".sqlite-wal", ".sqlite-journal"}
COLLAPSED_STAGE_ROOTS = (Path("state/providers/vhfmanager/checklogs"),)


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
    repair_contests: str | None = None
    recovery_stage: str | None = None
    recovery_remove_invalid: bool = False
    recovery_deleted: list[str] | None = None
    recovery_backup: str | None = None


@contextmanager
def updater_lock(repo: Path):
    path = git_dir(repo) / "hcla/updater.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        if os.name == "nt":
            import msvcrt
            if path.stat().st_size == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
        else:
            import fcntl
        try:
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise UpdateError("Another updater owns this checkout; wait for it to finish") from exc
        try:
            yield
        finally:
            if os.name == "nt":
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle, fcntl.LOCK_UN)


def running_archive_updaters() -> list[int]:
    try:
        result = subprocess.run(["ps", "-axo", "pid=,command="], check=True,
                                text=True, stdout=subprocess.PIPE)
    except FileNotFoundError as exc:
        raise UpdateError("Full recovery requires the POSIX ps command (macOS/Linux)") from exc
    programs = {"archive_updater.py", "public_logs_downloader.py", "reconstruct_missing_logs.py",
                "download_vhfmanager_logs.py"}
    found = []
    for line in result.stdout.splitlines():
        fields = line.strip().split(None, 1)
        if len(fields) != 2 or not fields[0].isdigit() or int(fields[0]) == os.getpid():
            continue
        try:
            tokens = shlex.split(fields[1])
        except ValueError:
            continue
        if any(Path(token).name in programs for token in tokens):
            found.append(int(fields[0]))
    return found


def quarantine_recovery_artifacts(repo: Path) -> None:
    from download_vhfmanager_logs import kvp_contest_ids, quarantine_untracked_vhf
    kvp_ids = kvp_contest_ids()
    state_path = repo / "state/providers/vhfmanager/hf_parser.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    hf_ids = {int(cid) for cid in state.get("scopes", {}) if cid.isdigit()}
    count, backup = quarantine_untracked_vhf(repo, kvp_ids, hf_ids)
    print(f"Recovery quarantine: {count} misplaced untracked logs" +
          (f"; preserved in {backup}" if backup else ""), flush=True)


def known_hf_artifacts(repo: Path, paths: Iterable[Path]) -> list[Path]:
    state_path = repo / "state/providers/vhfmanager/hf_parser.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    ids = {int(cid) for cid in state.get("scopes", {}) if cid.isdigit()}
    found = []
    for path in paths:
        if len(path.parts) < 4 or path.parts[0] != "EU_VHF_CONTESTS" or path.suffix.lower() != ".log":
            continue
        match = re.search(r"_(\d+)$", path.parts[1])
        if match and int(match.group(1)) in ids and path.parts[2] != "HF":
            found.append(path)
    return found


def committed_hf_artifacts(repo: Path) -> list[Path]:
    tree = run_git(repo, "ls-tree", "--name-only", "HEAD", "--", "EU_VHF_CONTESTS").stdout.strip()
    if not tree:
        return []
    folders = run_git(repo, "ls-tree", "--name-only", "HEAD:EU_VHF_CONTESTS").stdout.splitlines()
    inventory = ArchiveInventory(repo)
    candidates = []
    for folder in folders:
        prefix = Path("EU_VHF_CONTESTS") / folder
        if known_hf_artifacts(repo, [prefix / "144MHz/example.log"]):
            candidates.extend(known_hf_artifacts(repo, inventory.git_paths(prefix)))
    return candidates


def recover_committed_hf_artifacts(repo: Path, transaction: Transaction, allow_removal: bool) -> None:
    candidates = committed_hf_artifacts(repo)
    inventory = ArchiveInventory(repo)
    if not candidates:
        return
    if not allow_removal:
        raise UpdateError(f"{len(candidates)} committed non-HF files exist for known HF contests; "
                          "review them or explicitly use --remove-invalid-generated to back up and remove confirmed malformed files")
    for path in candidates:
        content = inventory.read_git_blob(path)
        text = content.decode("utf-8", errors="replace")
        rows = [line.split() for line in text.splitlines() if line.startswith("QSO:")]
        if "CREATED-BY: vhfmanager-downloader" not in text or not rows or not all(
            len(row) > 6 and row[6] in {"CW", "SSB", "PH"} for row in rows
        ):
            raise UpdateError(f"Refusing automatic removal: artifact proof failed for {path}")
        backup_and_remove_recovery_file(repo, transaction, path, content)
    for relative in sorted({path.parent for path in candidates}):
        prefix = Path("state/reconstruction/ledgers/RECONSTRUCTED_LOGS") / relative
        for path in inventory.git_paths(prefix, log_only=False):
            if path.name in {".reconstruct_state.json", ".reconstructed_ledger.txt.gz", ".reconstructed_ledger.txt"}:
                backup_and_remove_recovery_file(repo, transaction, path, inventory.read_git_blob(path))
    backup = git_dir(repo) / transaction.recovery_backup
    print(f"Recovery backed up and removed {len(candidates)} proven malformed HF artifacts and their stale round ledgers; backup={backup}", flush=True)


def backup_and_remove_recovery_file(repo: Path, transaction: Transaction, path: Path, content: bytes) -> None:
    local = repo / path
    if local.exists() and local.read_bytes() != content:
        raise UpdateError(f"Refusing to overwrite modified artifact: {path}")
    if transaction.recovery_backup is None:
        backup_root = git_dir(repo) / "hcla/quarantine"
        backup_root.mkdir(parents=True, exist_ok=True)
        backup = Path(tempfile.mkdtemp(prefix="committed-hf-", dir=backup_root))
        transaction.recovery_backup = backup.relative_to(git_dir(repo)).as_posix()
    backup = git_dir(repo) / transaction.recovery_backup
    atomic_write_bytes(backup / path, content)
    approved = set(transaction.recovery_deleted or [])
    approved.add(path.as_posix())
    transaction.recovery_deleted = sorted(approved)
    atomic_write_text(backup / "manifest.json", json.dumps(transaction.recovery_deleted, indent=2) + "\n")
    write_transaction(repo, transaction)
    if local.exists():
        local.unlink()
    run_git(repo, "update-index", "--force-remove", "--", path.as_posix())


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


def run_child_captured(repo: Path, argv: list[str], label: str) -> str:
    """Run a child quietly, replaying its combined output only on failure."""
    print(f"\n[{label}] running...")
    process = subprocess.Popen(
        argv,
        cwd=repo,
        start_new_session=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        output, _ = process.communicate()
    except KeyboardInterrupt:
        print(f"\nInterrupting {label}...", file=sys.stderr)
        stop_process_group(process)
        raise
    if process.returncode != 0:
        if output:
            print(f"\n[{label}] captured output:", file=sys.stderr)
            print(output.rstrip(), file=sys.stderr)
        raise UpdateError(f"{label} failed with exit {process.returncode}")
    return output


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


def update_shards(repo: Path, approved_deletions: Iterable[str] = ()) -> set[Path]:
    delta = worktree_log_delta(repo)
    allowed_deleted = {Path(path) for path in approved_deletions}
    if set(delta.deleted) - allowed_deleted:
        sample = ", ".join(path.as_posix() for path in delta.deleted[:10])
        raise UpdateError(f"archive update refuses implicit log deletions: {sample}")
    result = apply_path_delta(repo, delta.added_or_modified, deleted=delta.deleted)
    print(
        f"SH6 incremental update: upserted={result.upserted} unchanged={result.unchanged} "
        f"changed_shards={len(result.changed_shards)} removed={len(delta.deleted)}"
    )
    return result.changed_shards


def update_readme(repo: Path) -> None:
    from public_logs_downloader import update_readme_from_shards

    update_readme_from_shards(repo, repo / "SH6")


def validate_changes(repo: Path, approved_deletions: Iterable[str] = (), check_artifacts: bool = True) -> None:
    delta = worktree_log_delta(repo)
    allowed_deleted = {Path(path) for path in approved_deletions}
    if set(delta.deleted) - allowed_deleted:
        raise UpdateError("validation rejected deleted archive logs")
    artifacts = known_hf_artifacts(repo, delta.added_or_modified) if check_artifacts else []
    if artifacts:
        raise UpdateError("Known HF contests contain non-HF generated files; run full recovery before publishing: " +
                          ", ".join(path.as_posix() for path in artifacts[:5]))
    invalid = [path for path in delta.added_or_modified if not valid_local_log(repo / path)]
    if invalid:
        raise UpdateError("invalid generated logs: " + ", ".join(path.as_posix() for path in invalid[:10]))
    for status, path in porcelain_paths(repo):
        if any(path.name.endswith(suffix) for suffix in FORBIDDEN_SUFFIXES):
            raise UpdateError(f"transient file must not be published: {path}")
        if path.name.startswith(".env") or "cookie" in path.name.lower():
            raise UpdateError(f"possible credential file must not be published: {path}")
        if "D" in status and path.suffix.lower() in LOG_EXTENSIONS and path not in allowed_deleted:
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
    if (
        len(path.parts) >= 3
        and path.parts[0] == "EU_VHF_CONTESTS"
        and path.parts[1] == ".checklogs"
        and path.suffix == ".done"
    ):
        return True
    if path.parts[0] in ALLOWED_TRACKED_ROOTS:
        return True
    return path.parts[0] in ARCHIVE_ROOTS and path.suffix.lower() in LOG_EXTENSIONS


def collapse_stage_paths(paths: Iterable[Path]) -> list[Path]:
    remaining = set(paths)
    collapsed: set[Path] = set()
    for root in COLLAPSED_STAGE_ROOTS:
        matches = {path for path in remaining if path.is_relative_to(root)}
        if matches:
            remaining.difference_update(matches)
            collapsed.add(root)
    return sorted(remaining | collapsed, key=lambda path: path.as_posix())


def stage_generated_changes(repo: Path) -> list[Path]:
    changes = porcelain_paths(repo)
    paths = [path for _status, path in changes]
    disallowed = [path for path in paths if not allowed_generated_path(path)]
    if disallowed:
        raise UpdateError(
            "refusing to stage unrelated paths: " + ", ".join(path.as_posix() for path in disallowed[:10])
        )
    deleted = [path for status, path in changes if "D" in status]
    retained = collapse_stage_paths(
        path for status, path in changes if "D" not in status
    )
    if deleted:
        pathspec = "\0".join(path.as_posix() for path in deleted) + "\0"
        subprocess.run(
            ["git", "update-index", "--force-remove", "-z", "--stdin"],
            cwd=repo,
            check=True,
            text=True,
            input=pathspec,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    if retained:
        pathspec = "\0".join(path.as_posix() for path in retained) + "\0"
        subprocess.run(
            [
                "git",
                "add",
                "--sparse",
                "--pathspec-from-file=-",
                "--pathspec-file-nul",
            ],
            cwd=repo,
            check=True,
            text=True,
            input=pathspec,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
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
    output = run_child_captured(
        repo,
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py"],
        "tests",
    )
    match = re.search(r"\bRan (\d+) tests? in\b", output)
    count = f" {match.group(1)}" if match else ""
    print(f"All{count} tests passed; ready for commit/publish.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=REPO_ROOT)
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--branch", default=None)
    parser.add_argument("--phase", choices=("all", "download", "reconstruct", "shards"), default="all")
    parser.add_argument("--contests", default="all")
    parser.add_argument("--last", default="1")
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--repair-reconstruction", action="store_true",
                        help="Repair committed historical reconstruction without running downloaders.")
    parser.add_argument("--repair-contests", default="all",
                        help="Comma-separated archive root names for reconstruction repair (default: all).")
    parser.add_argument("--recover-history", action="store_true",
                        help="Quarantine known untracked artifacts, download all providers/all years, and repair all historical reconstruction.")
    parser.add_argument("--remove-invalid-generated", action="store_true",
                        help="With full recovery, explicitly permit backed-up removal of proven malformed committed HF artifacts.")
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
    if args.repair_reconstruction and args.phase == "download":
        parser.error("Reconstruction repair cannot use --phase download")

    repo = args.repo.resolve()
    if args.dry_run:
        return run_archive_update(args, repo)
    with updater_lock(repo):
        return run_archive_update(args, repo)


def run_archive_update(args, repo: Path) -> int:
    branch = args.branch or current_branch(repo)
    transaction = read_transaction(repo)
    if args.dry_run and transaction and transaction.recovery_stage is None and not args.recover_history:
        print(f"Dry run: existing transaction phase={transaction.phase}; no updater changes made.")
        return 0
    if transaction and transaction.recovery_stage is not None:
        args.recover_history = True
        args.remove_invalid_generated = args.remove_invalid_generated or transaction.recovery_remove_invalid
    if args.remove_invalid_generated and not args.recover_history:
        raise UpdateError("--remove-invalid-generated requires --recover-history")
    if args.recover_history:
        if args.repair_reconstruction or (transaction and transaction.repair_contests is not None):
            raise UpdateError("Finish a standalone reconstruction repair before full recovery")
        if args.phase != "all":
            raise UpdateError("Full recovery uses --phase all; rerun the same recovery command to resume")
        if args.dry_run:
            print("Recovery preview: quarantine untracked KVP/non-HF artifacts; download all providers/all years; "
                  "repair all committed reconstruction; incrementally update SH6; test and publish if requested.")
            print(f"Resume stage: {(transaction.recovery_stage if transaction else None) or 'cleanup'}; no recovery changes made.")
            artifacts = committed_hf_artifacts(repo)
            if artifacts:
                print(f"{len(artifacts)} committed non-HF artifacts need review or explicit --remove-invalid-generated consent.")
            return 0
        active = running_archive_updaters()
        if active:
            raise UpdateError(f"Stop the active archive updater before recovery (PID(s): {', '.join(map(str, active))})")
        args.contests, args.last, args.repair_contests = "all", "all", "all"
    if transaction and transaction.repair_contests is not None:
        if args.repair_contests not in {"all", transaction.repair_contests}:
            raise UpdateError("Repair contest selection differs from the interrupted transaction")
        args.repair_reconstruction = True
        args.repair_contests = transaction.repair_contests
    elif transaction and args.repair_reconstruction:
        raise UpdateError("Finish the existing normal update before starting historical repair")
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
        if args.recover_history and transaction.recovery_stage is None:
            return run_archive_update(args, repo)
        return 0

    if transaction is not None:
        cleanup_orphaned_transients(repo)

    if transaction is None:
        if args.resume_existing or (args.recover_history and porcelain_paths(repo)):
            if not porcelain_paths(repo):
                raise UpdateError("--resume-existing was requested but the working tree is clean")
            validate_changes(repo, check_artifacts=not args.recover_history)
        else:
            assert_clean_start(repo)
        base = assert_current(repo, args.remote, branch)
        transaction = Transaction(1, base, branch, args.remote, "prepared")
        if args.repair_reconstruction:
            transaction.repair_contests = args.repair_contests
        if args.recover_history:
            transaction.recovery_stage = "cleanup"
            transaction.recovery_remove_invalid = args.remove_invalid_generated
        if args.dry_run:
            print(f"Dry run: base={base} branch={branch} contests={args.contests} last={args.last}")
            if args.repair_reconstruction:
                print(f"Historical reconstruction repair: roots={args.repair_contests}; no downloader")
            print("No downloader, state, SH6, commit, or push changes were made.")
            return 0
        write_transaction(repo, transaction)
    else:
        adopt_fast_forwarded_transaction_head(repo, transaction)
        if args.recover_history and transaction.recovery_stage is None:
            transaction.recovery_stage = "cleanup"
            write_transaction(repo, transaction)
    if args.recover_history and args.remove_invalid_generated:
        transaction.recovery_remove_invalid = True
        write_transaction(repo, transaction)

    migrate_legacy_provider_state(repo)

    try:
        if args.recover_history and transaction.recovery_stage == "cleanup":
            recover_committed_hf_artifacts(repo, transaction, args.remove_invalid_generated)
            quarantine_recovery_artifacts(repo)
            transaction.recovery_stage = "download"
            write_transaction(repo, transaction)
        if (args.phase in {"all", "download"} and not args.repair_reconstruction
                and (not args.recover_history or transaction.recovery_stage == "download")):
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
            if args.recover_history:
                transaction.recovery_stage = "reconstruct"
            write_transaction(repo, transaction)
            if args.phase == "download":
                print("Download phase complete. Rerun with --phase reconstruct to continue.")
                return 0

        if (args.phase in {"all", "reconstruct"}
                and (not args.recover_history or transaction.recovery_stage == "reconstruct")):
            transaction.phase = "reconstructing"
            write_transaction(repo, transaction)
            if args.repair_reconstruction or args.recover_history:
                command = [sys.executable, "scripts/reconstruct_missing_logs.py", "--historical", "--no-rebuild-shards"]
                if args.recover_history:
                    command.append("--include-worktree")
                    excluded = sorted({Path(path).parent.as_posix() for path in transaction.recovery_deleted or []
                                       if Path(path).suffix.lower() in LOG_EXTENSIONS})
                    for relative in excluded:
                        command.extend(["--exclude-source-round", relative])
                if args.repair_contests != "all":
                    command.extend(["--contest", args.repair_contests])
                run_child(repo, command, "historical reconstruction")
            elif source_log_changes(repo):
                run_child(
                    repo,
                    [sys.executable, "scripts/reconstruct_missing_logs.py", "--changed-only", "--no-rebuild-shards"],
                    "reconstruction",
                )
            else:
                print("No changed source logs require reconstruction.")
            transaction.phase = "reconstructed"
            if args.recover_history:
                transaction.recovery_stage = "shards"
            write_transaction(repo, transaction)
            if args.phase == "reconstruct":
                print("Reconstruction phase complete. Rerun with --phase shards to continue.")
                return 0

        if args.phase in {"all", "shards"}:
            transaction.phase = "indexing"
            write_transaction(repo, transaction)
            update_shards(repo, transaction.recovery_deleted or [])
            update_readme(repo)
            transaction.phase = "indexed"
            write_transaction(repo, transaction)

        if not porcelain_paths(repo):
            clear_transaction(repo)
            sparse_cleanup(repo)
            print("Archive is already current; no tracked files changed.")
            return 0
        validate_changes(repo, transaction.recovery_deleted or [])
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
