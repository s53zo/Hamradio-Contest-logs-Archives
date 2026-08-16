import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import archive_updater as updater  # noqa: E402


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, text=True, stdout=subprocess.PIPE
    ).stdout.strip()


class ArchiveUpdaterTests(unittest.TestCase):
    def make_remote_and_clones(self, root: Path) -> tuple[Path, Path, Path]:
        seed = root / "seed"
        remote = root / "remote.git"
        clone_a = root / "a"
        clone_b = root / "b"
        seed.mkdir()
        git(seed, "init", "-q", "-b", "main")
        git(seed, "config", "user.email", "test@example.invalid")
        git(seed, "config", "user.name", "Test")
        (seed / "README.md").write_text("seed\n", encoding="ascii")
        git(seed, "add", "README.md")
        git(seed, "commit", "-qm", "seed")
        git(root, "clone", "-q", "--bare", str(seed), str(remote))
        git(root, "clone", "-q", str(remote), str(clone_a))
        git(root, "clone", "-q", str(remote), str(clone_b))
        for clone in (clone_a, clone_b):
            git(clone, "config", "user.email", "test@example.invalid")
            git(clone, "config", "user.name", "Test")
        return remote, clone_a, clone_b

    def transaction(self, repo: Path) -> updater.Transaction:
        return updater.Transaction(
            schema_version=1,
            base_sha=git(repo, "rev-parse", "HEAD"),
            branch="main",
            remote="origin",
            phase="committed",
            commit_sha=git(repo, "rev-parse", "HEAD"),
        )

    def test_disjoint_remote_advance_rebases_and_pushes_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _remote, clone_a, clone_b = self.make_remote_and_clones(Path(temp))
            transaction = self.transaction(clone_b)
            (clone_a / "a.txt").write_text("a\n", encoding="ascii")
            git(clone_a, "add", "a.txt")
            git(clone_a, "commit", "-qm", "a")
            git(clone_a, "push", "-q", "origin", "main")
            (clone_b / "b.txt").write_text("b\n", encoding="ascii")
            git(clone_b, "add", "b.txt")
            git(clone_b, "commit", "-qm", "b")
            transaction.commit_sha = git(clone_b, "rev-parse", "HEAD")

            published = updater.reconcile_and_push(clone_b, transaction)

            self.assertEqual(published, git(clone_b, "rev-parse", "HEAD"))
            self.assertTrue((clone_b / "a.txt").is_file())
            self.assertTrue((clone_b / "b.txt").is_file())

    def test_divergent_same_path_advance_aborts_rebase_and_preserves_remote(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _remote, clone_a, clone_b = self.make_remote_and_clones(Path(temp))
            transaction = self.transaction(clone_b)
            (clone_a / "README.md").write_text("from a\n", encoding="ascii")
            git(clone_a, "commit", "-qam", "a")
            git(clone_a, "push", "-q", "origin", "main")
            remote_sha = git(clone_a, "rev-parse", "HEAD")
            (clone_b / "README.md").write_text("from b\n", encoding="ascii")
            git(clone_b, "commit", "-qam", "b")
            transaction.commit_sha = git(clone_b, "rev-parse", "HEAD")

            with self.assertRaises(updater.ConcurrentUpdateError):
                updater.reconcile_and_push(clone_b, transaction)

            self.assertEqual(git(clone_a, "ls-remote", "origin", "refs/heads/main").split()[0], remote_sha)
            self.assertFalse((clone_b / ".git/rebase-merge").exists())

    def test_process_group_shutdown_is_bounded_when_child_ignores_signals(self) -> None:
        child = subprocess.Popen(
            [
                sys.executable,
                "-c",
                (
                    "import signal,time; "
                    "signal.signal(signal.SIGINT, signal.SIG_IGN); "
                    "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                    "time.sleep(60)"
                ),
            ],
            start_new_session=True,
        )
        time.sleep(0.1)
        started = time.monotonic()

        returncode = updater.stop_process_group(child, graceful_timeout=0.1, terminate_timeout=0.1)

        self.assertLess(returncode, 0)
        self.assertLess(time.monotonic() - started, 2)

    def test_dry_run_leaves_checkout_and_transaction_state_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _remote, clone, _other = self.make_remote_and_clones(Path(temp))
            before = git(clone, "status", "--porcelain=v1")

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPTS / "archive_updater.py"),
                    "--repo",
                    str(clone),
                    "--dry-run",
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("No downloader, state, SH6, commit, or push changes", result.stdout)
            self.assertEqual(git(clone, "status", "--porcelain=v1"), before)
            self.assertFalse((clone / ".git/hcla").exists())

    def test_post_commit_transaction_can_resume_publication(self) -> None:
        for phase in ("committed", "publishing"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as temp:
                _remote, clone, observer = self.make_remote_and_clones(Path(temp))
                base_sha = git(clone, "rev-parse", "HEAD")
                (clone / "state").mkdir()
                (clone / "state/resume.json").write_text("{}\n", encoding="ascii")
                git(clone, "add", "state/resume.json")
                git(clone, "commit", "-qm", "resume fixture")
                commit_sha = git(clone, "rev-parse", "HEAD")
                updater.write_transaction(
                    clone,
                    updater.Transaction(
                        schema_version=1,
                        base_sha=base_sha,
                        branch="main",
                        remote="origin",
                        phase=phase,
                        commit_sha=commit_sha,
                    ),
                )

                result = subprocess.run(
                    [
                        sys.executable,
                        str(SCRIPTS / "archive_updater.py"),
                        "--repo",
                        str(clone),
                        "--publish",
                    ],
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("Published and verified", result.stdout)
                self.assertFalse(updater.journal_path(clone).exists())
                self.assertEqual(
                    git(observer, "ls-remote", "origin", "refs/heads/main").split()[0],
                    commit_sha,
                )

    def test_interrupted_resume_removes_only_generated_transients(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp)
            git(repo, "init", "-q", "-b", "main")
            archive_temp = repo / "YOTA_Contest/2026/Round_1/.S53ZO.log.123.1.part"
            unrelated = repo / "tests/notes.tmp"
            archive_temp.parent.mkdir(parents=True)
            unrelated.parent.mkdir(parents=True)
            archive_temp.write_text("partial", encoding="ascii")
            unrelated.write_text("keep", encoding="ascii")

            removed = updater.cleanup_orphaned_transients(repo)

            self.assertEqual(
                removed,
                [Path("YOTA_Contest/2026/Round_1/.S53ZO.log.123.1.part")],
            )
            self.assertFalse(archive_temp.exists())
            self.assertTrue(unrelated.exists())

    def test_interrupted_transaction_adopts_remote_fast_forward(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _remote, clone, other = self.make_remote_and_clones(Path(temp))
            base_sha = git(clone, "rev-parse", "HEAD")
            generated = clone / "YOTA_Contest/2026/Round_1/S53ZO.log"
            generated.parent.mkdir(parents=True)
            generated.write_text("START-OF-LOG: 3.0\nEND-OF-LOG:\n", encoding="ascii")
            transaction = updater.Transaction(
                schema_version=1,
                base_sha=base_sha,
                branch="main",
                remote="origin",
                phase="interrupted",
            )
            updater.write_transaction(clone, transaction)

            (other / "scripts").mkdir()
            (other / "scripts/fix.py").write_text("# fix\n", encoding="ascii")
            git(other, "add", "scripts/fix.py")
            git(other, "commit", "-qm", "fix")
            git(other, "push", "-q", "origin", "main")
            git(clone, "pull", "--ff-only")

            self.assertTrue(updater.adopt_fast_forwarded_transaction_head(clone, transaction))
            self.assertEqual(transaction.base_sha, git(clone, "rev-parse", "HEAD"))
            self.assertTrue(generated.is_file())
            self.assertEqual(updater.read_transaction(clone).base_sha, transaction.base_sha)

    def test_staging_rejects_log_like_files_outside_archive_roots(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp)
            git(repo, "init", "-q", "-b", "main")
            debug_log = repo / "tests/debug.log"
            debug_log.parent.mkdir(parents=True)
            debug_log.write_text("debug\n", encoding="ascii")

            with self.assertRaisesRegex(updater.UpdateError, "unrelated paths"):
                updater.stage_generated_changes(repo)

    def test_sparse_cleanup_preserves_remote_only_log_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp)
            git(repo, "init", "-q", "-b", "main")
            git(repo, "config", "user.email", "test@example.invalid")
            git(repo, "config", "user.name", "Test")
            files = {
                "scripts/placeholder.py": "print('ok')\n",
                "SH6/placeholder.txt": "index\n",
                "YOTA_Contest/2026/Round_1/S53ZO.log": (
                    "START-OF-LOG: 3.0\nEND-OF-LOG:\n"
                ),
            }
            for rel, content in files.items():
                path = repo / rel
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="ascii")
            git(repo, "add", ".")
            git(repo, "commit", "-qm", "fixture")
            git(repo, "sparse-checkout", "init", "--cone")
            git(repo, "sparse-checkout", "set", "scripts", "SH6")
            log_path = Path("YOTA_Contest/2026/Round_1/S53ZO.log")
            self.assertFalse((repo / log_path).exists())

            updater.sparse_cleanup(repo)

            self.assertEqual(git(repo, "status", "--porcelain=v1"), "")
            self.assertEqual(git(repo, "ls-tree", "--name-only", "HEAD", "--", str(log_path)), str(log_path))
            self.assertFalse((repo / log_path).exists())

    def test_log_rename_is_rejected_as_public_path_removal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp)
            git(repo, "init", "-q", "-b", "main")
            git(repo, "config", "user.email", "test@example.invalid")
            git(repo, "config", "user.name", "Test")
            original = repo / "YOTA_Contest/2026/Round_1/S53ZO.log"
            original.parent.mkdir(parents=True)
            original.write_text("START-OF-LOG: 3.0\nEND-OF-LOG:\n", encoding="ascii")
            git(repo, "add", ".")
            git(repo, "commit", "-qm", "fixture")
            renamed = original.with_name("S53ZO-RENAMED.log")
            os.replace(original, renamed)

            with self.assertRaisesRegex(updater.UpdateError, "refuses implicit log deletions"):
                updater.source_log_changes(repo)


if __name__ == "__main__":
    unittest.main()
