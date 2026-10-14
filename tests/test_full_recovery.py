import os
import json
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import archive_updater as updater
import download_vhfmanager_logs as vhf
import reconstruct_missing_logs as reconstruct
import public_logs_downloader as public
from shard_index import apply_path_delta, audit_git_tree
import test_archive_updater as fixtures


class FullRecoveryTests(unittest.TestCase):
    def test_checkout_lock_rejects_overlapping_updates_and_releases(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = self.fixture(Path(temporary))
            with updater.updater_lock(repo):
                with self.assertRaisesRegex(updater.UpdateError, "Another updater owns"):
                    with updater.updater_lock(repo):
                        self.fail("overlapping lock succeeded")
            with updater.updater_lock(repo):
                pass
    def prepare_hf_fixture(self, repo):
        (repo / "README.md").write_text("\n".join(["Fixture", public.README_STATS_START, public.README_STATS_END,
                                                   public.README_YEARS_START, public.README_YEARS_END]) + "\n")
        good = Path("EU_VHF_CONTESTS/ar_memorial_2025_450/HF/K1ABC.log")
        bad = Path("EU_VHF_CONTESTS/ar_memorial_2026_478/144MHz/K3ABC.log")
        (repo / good).parent.mkdir(parents=True)
        (repo / good).write_text(self.source_log("K1ABC", 2025))
        (repo / bad).parent.mkdir(parents=True)
        content = "START-OF-LOG: 3.0\nCREATED-BY: vhfmanager-downloader\nQSO: 144300 PH 2026-05-01 1200 K3ABC CW 599 K3ABC 0 K1ABC\nEND-OF-LOG:\n"
        (repo / bad).write_text(content)
        state = repo / "state/providers/vhfmanager/hf_parser.json"
        state.parent.mkdir(parents=True)
        state.write_text(json.dumps({"scopes": {"478": {"version": 2}}}))
        stale = repo / "state/reconstruction/ledgers/RECONSTRUCTED_LOGS" / bad.parent / ".reconstruct_state.json"
        stale.parent.mkdir(parents=True)
        stale.write_text('{"source_log_count": 164}')
        apply_path_delta(repo, [good, bad])
        fixtures.git(repo, "add", ".")
        fixtures.git(repo, "commit", "-qm", "HF fixture")
        fixtures.git(repo, "push", "-q", "origin", "main")
        return good, bad, content

    def source_log(self, call, year):
        return "START-OF-LOG: 3.0\nCONTEST: HF-TEST\n" + ''.join(
            f"QSO: 7000 CW {year}-05-01 12{number:02d} {call} 599 {number+1} DL9GRE 599 {number+1}\n"
            for number in range(10)
        ) + "END-OF-LOG:\n"

    def test_committed_artifact_cleanup_requires_opt_in_and_exact_deletion_whitelist(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = self.fixture(Path(temporary))
            good, bad, content = self.prepare_hf_fixture(repo)
            transaction = updater.Transaction(1, fixtures.git(repo, "rev-parse", "HEAD"), "main", "origin", "prepared")
            with self.assertRaisesRegex(updater.UpdateError, "explicitly use --remove-invalid-generated"):
                updater.recover_committed_hf_artifacts(repo, transaction, False)
            self.assertTrue((repo / bad).exists())
            updater.recover_committed_hf_artifacts(repo, transaction, True)
            backup = updater.git_dir(repo) / transaction.recovery_backup
            self.assertEqual((backup / bad).read_text(), content)
            stale = Path("state/reconstruction/ledgers/RECONSTRUCTED_LOGS") / bad.parent / ".reconstruct_state.json"
            self.assertTrue((backup / stale).exists())
            self.assertFalse((repo / stale).exists())
            self.assertFalse((repo / bad).exists())
            updater.validate_changes(repo, transaction.recovery_deleted)
            (repo / good).unlink()
            with self.assertRaisesRegex(updater.UpdateError, "deleted archive logs"):
                updater.validate_changes(repo, transaction.recovery_deleted)

    def test_publication_guard_rejects_new_non_hf_files_in_known_hf_contest(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = self.fixture(Path(temporary))
            self.prepare_hf_fixture(repo)
            new = repo / "EU_VHF_CONTESTS/ar_memorial_2026_478/144MHz/K4ABC.log"
            new.write_text("START-OF-LOG: 3.0\nEND-OF-LOG:\n")
            with self.assertRaisesRegex(updater.UpdateError, "Known HF contests"):
                updater.validate_changes(repo)

    def test_committed_cleanup_refuses_modified_or_unproven_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = self.fixture(Path(temporary))
            _good, bad, content = self.prepare_hf_fixture(repo)
            transaction = updater.Transaction(1, fixtures.git(repo, "rev-parse", "HEAD"), "main", "origin", "prepared")
            (repo / bad).write_text(content + "user edit\n")
            with self.assertRaisesRegex(updater.UpdateError, "modified artifact"):
                updater.recover_committed_hf_artifacts(repo, transaction, True)
            self.assertIn("user edit", (repo / bad).read_text())
            (repo / bad).write_text(content.replace("K3ABC CW 599", "K3ABC 599 001"))
            fixtures.git(repo, "add", ".")
            fixtures.git(repo, "commit", "-qm", "valid report fixture")
            with self.assertRaisesRegex(updater.UpdateError, "artifact proof failed"):
                updater.recover_committed_hf_artifacts(repo, transaction, True)
            self.assertTrue((repo / bad).exists())

    def test_pending_normal_publication_continues_into_full_recovery(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = self.fixture(Path(temporary))
            head = fixtures.git(repo, "rev-parse", "HEAD")
            updater.write_transaction(repo, updater.Transaction(1, head, "main", "origin", "committed", head))
            events = []
            with self.patches(repo, lambda repo, command, label: events.append(label), ["--publish"]):
                with (
                    mock.patch.object(updater, "quarantine_recovery_artifacts"),
                    mock.patch.object(updater, "reconcile_and_push", return_value=head),
                ):
                    self.assertEqual(updater.main(), 0)
            self.assertEqual(events, ["download", "historical reconstruction"])

    def test_complete_sparse_recovery_publishes_valid_logs_and_indexes_without_bad_artifact(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = self.fixture(root)
            good, bad, content = self.prepare_hf_fixture(repo)
            fixtures.git(repo, "sparse-checkout", "set", "--cone", "scripts", "tests", "state", "SH6")
            self.assertFalse((repo / good).exists())
            new = Path("EU_VHF_CONTESTS/ar_memorial_2026_478/HF/K3ABC.log")
            events = []
            def run_child(repo, command, label):
                events.append(label)
                if label == "download":
                    (repo / new).parent.mkdir(parents=True, exist_ok=True)
                    (repo / new).write_text(self.source_log("K3ABC", 2026))
                else:
                    master = root / "master.dta"
                    master.write_bytes(b"DL9GRE\x00")
                    with (
                        mock.patch.object(reconstruct, "__file__", str(repo / "scripts/reconstruct_missing_logs.py")),
                        mock.patch.object(reconstruct, "download_master_dta", return_value=master),
                        mock.patch.object(sys, "argv", command[1:]),
                    ):
                        self.assertEqual(reconstruct._main(), 0)
            with (
                mock.patch.object(sys, "argv", ["archive_updater.py", "--repo", str(repo), "--recover-history", "--remove-invalid-generated", "--publish"]),
                mock.patch.object(updater, "running_archive_updaters", return_value=[]),
                mock.patch.object(updater, "migrate_legacy_provider_state"),
                mock.patch.object(updater, "run_child", side_effect=run_child),
                mock.patch.object(updater, "run_tests") as tests,
                mock.patch.object(vhf, "kvp_contest_ids", return_value={476}),
            ):
                self.assertEqual(updater.main(), 0)
                tests.assert_called_once()
            self.assertEqual(events, ["download", "historical reconstruction"])
            audit = audit_git_tree(repo)
            self.assertEqual((audit.expected, audit.indexed, len(audit.missing), len(audit.extra)), (4, 4, 0, 0))
            self.assertEqual(fixtures.git(repo, "status", "--short"), "")
            self.assertIsNone(updater.read_transaction(repo))
            self.assertEqual(fixtures.git(repo, "rev-parse", "HEAD"), fixtures.git(repo, "ls-remote", "origin", "refs/heads/main").split()[0])
            self.assertEqual(len(list((updater.git_dir(repo) / "hcla/quarantine").rglob(bad.name))), 1)

    def fixture(self, root):
        return fixtures.ArchiveUpdaterTests().make_remote_and_clones(root)[1]

    def patches(self, repo, child, extra=()):
        stack = ExitStack()
        stack.enter_context(mock.patch.object(sys, "argv", ["archive_updater.py", "--repo", str(repo), "--recover-history", *extra]))
        stack.enter_context(mock.patch.object(updater, "running_archive_updaters", return_value=[]))
        stack.enter_context(mock.patch.object(updater, "migrate_legacy_provider_state"))
        stack.enter_context(mock.patch.object(updater, "run_child", side_effect=child))
        stack.enter_context(mock.patch.object(updater, "update_shards"))
        stack.enter_context(mock.patch.object(updater, "update_readme"))
        stack.enter_context(mock.patch.object(updater, "source_log_changes", side_effect=AssertionError("must include unchanged history")))
        return stack

    def test_full_recovery_quarantines_then_downloads_all_and_reconstructs_history(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = self.fixture(Path(temporary))
            events = []
            with self.patches(repo, lambda repo, command, label: events.append((label, command))):
                with mock.patch.object(updater, "quarantine_recovery_artifacts", side_effect=lambda repo: events.append(("cleanup", []))):
                    self.assertEqual(updater.main(), 0)
            self.assertEqual([event[0] for event in events], ["cleanup", "download", "historical reconstruction"])
            download = events[1][1]
            self.assertEqual(download[download.index("--contests") + 1], "all")
            self.assertEqual(download[download.index("--last") + 1], "all")
            self.assertIn("--historical", events[2][1])
            self.assertIn("--include-worktree", events[2][1])
            self.assertIsNone(updater.read_transaction(repo))

    def test_failed_reconstruction_resumes_without_redownloading_or_requarantining(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = self.fixture(Path(temporary))
            def fail_reconstruction(repo, command, label):
                if label == "historical reconstruction":
                    raise updater.UpdateError("test interruption")
            with self.patches(repo, fail_reconstruction), mock.patch.object(updater, "quarantine_recovery_artifacts"):
                with self.assertRaisesRegex(updater.UpdateError, "test interruption"):
                    updater.main()
            self.assertEqual(updater.read_transaction(repo).recovery_stage, "reconstruct")
            events = []
            with self.patches(repo, lambda repo, command, label: events.append(label)):
                with mock.patch.object(updater, "quarantine_recovery_artifacts") as quarantine:
                    self.assertEqual(updater.main(), 0)
                    quarantine.assert_not_called()
            self.assertEqual(events, ["historical reconstruction"])

    def test_active_process_blocks_recovery_before_any_state_change(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = self.fixture(Path(temporary))
            with self.patches(repo, lambda *args: None):
                with mock.patch.object(updater, "running_archive_updaters", return_value=[12345]):
                    with self.assertRaisesRegex(updater.UpdateError, "Stop the active archive updater"):
                        updater.main()
            self.assertIsNone(updater.read_transaction(repo))

    def test_preview_preserves_existing_transaction(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = self.fixture(Path(temporary))
            transaction = updater.Transaction(1, fixtures.git(repo, "rev-parse", "HEAD"), "main", "origin", "interrupted")
            updater.write_transaction(repo, transaction)
            before = updater.journal_path(repo).read_bytes()
            with self.patches(repo, lambda *args: self.fail("preview executed a child"), ["--dry-run"]):
                self.assertEqual(updater.main(), 0)
            self.assertEqual(updater.journal_path(repo).read_bytes(), before)

    def test_process_detection_ignores_self_and_other_python_commands(self):
        worker = os.getpid() + 1000
        output = f"{os.getpid()} python scripts/archive_updater.py\n{worker} python scripts/public_logs_downloader.py --contests all\n{worker+1} python -m unittest\n"
        with mock.patch.object(updater.subprocess, "run", return_value=mock.Mock(stdout=output)):
            self.assertEqual(updater.running_archive_updaters(), [worker])


if __name__ == "__main__":
    unittest.main()
