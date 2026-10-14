import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import archive_updater as updater
import reconstruct_missing_logs as reconstruct
from archive_storage import ArchiveInventory
import test_archive_updater as fixtures

git = fixtures.git


class HistoricalRepairTests(unittest.TestCase):
    def test_remote_rounds_are_selected_and_released_one_at_a_time(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp)
            git(repo, "init", "-q")
            git(repo, "config", "user.name", "Test")
            git(repo, "config", "user.email", "test@example.invalid")
            for relative in ("CQWW/cw/2025/K1ABC.log", "CQWW/cw/2024/K2ABC.log", "SAC/CW/2025/K3ABC.log"):
                path = repo / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("START-OF-LOG: 3.0\nEND-OF-LOG:\n")
            git(repo, "add", ".")
            git(repo, "commit", "-qm", "fixture")
            for path in repo.rglob("*.log"):
                path.unlink()
            rounds = reconstruct.historical_contest_dirs(ArchiveInventory(repo), {"CQWW"})
            first, relative = next(rounds)
            self.assertEqual(relative, Path("CQWW/cw/2024"))
            self.assertTrue((first / "K2ABC.log").exists())
            second, relative = next(rounds)
            self.assertFalse(first.exists())
            self.assertTrue((second / "K1ABC.log").exists())
            with self.assertRaises(StopIteration):
                next(rounds)
            self.assertFalse(second.exists())
            self.assertFalse((repo / "CQWW/cw/2025/K1ABC.log").exists())

    def test_unknown_root_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unknown contest roots"):
            next(reconstruct.historical_contest_dirs(mock.Mock(), {"scripts"}))

    def test_recovery_includes_new_rounds_and_modified_local_sources(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            git(root, "init", "-q")
            git(root, "config", "user.name", "Test")
            git(root, "config", "user.email", "test@example.invalid")
            existing = root / "CQWW/cw/2025/K1ABC.log"
            existing.parent.mkdir(parents=True)
            existing.write_text("old")
            git(root, "add", ".")
            git(root, "commit", "-qm", "fixture")
            existing.write_text("corrected source")
            new = root / "CQWW/cw/2026/K2ABC.log"
            new.parent.mkdir(parents=True)
            new.write_text("newly downloaded source")
            contents = []
            for path, relative in reconstruct.historical_contest_dirs(ArchiveInventory(root), {"CQWW"}, include_worktree=True):
                contents.append((relative, next(path.glob("*.log")).read_text()))
            self.assertEqual(contents, [(Path("CQWW/cw/2025"), "corrected source"),
                                        (Path("CQWW/cw/2026"), "newly downloaded source")])

    def test_cli_replaces_remote_reconstruction_without_source_changes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            repo = root / "repo"
            repo.mkdir()
            git(repo, "init", "-q")
            git(repo, "config", "user.name", "Test")
            git(repo, "config", "user.email", "test@example.invalid")
            source = repo / "EU_VHF_CONTESTS/example/2025/10000MHz/OE5VRL.log"
            source.parent.mkdir(parents=True)
            source.write_text("START-OF-LOG: 3.0\nCONTEST: TEST\n"
                              "QSO: 10000000 CW 2025-06-21 1100 OE5VRL 59S 12 JN78DK DL9GRE 59S 8 JO61FH\nEND-OF-LOG:\n")
            target = repo / "RECONSTRUCTED_LOGS/EU_VHF_CONTESTS/example/2025/10000MHz/DL9GRE.log"
            target.parent.mkdir(parents=True)
            target.write_text("START-OF-LOG: 3.0\nSOAPBOX: stale\nEND-OF-LOG:\n")
            git(repo, "add", ".")
            git(repo, "commit", "-qm", "fixture")
            source.unlink()
            target.unlink()
            master = root / "master.dta"
            master.write_bytes(b"DL9GRE\x00")
            with (
                mock.patch.object(reconstruct, "__file__", str(repo / "scripts/reconstruct_missing_logs.py")),
                mock.patch.object(reconstruct, "download_master_dta", return_value=master),
                mock.patch.object(sys, "argv", ["reconstruct_missing_logs.py", "--historical", "--contest", "EU_VHF_CONTESTS", "--min-qsos", "1", "--no-rebuild-shards"]),
            ):
                self.assertEqual(reconstruct._main(), 0)
            self.assertFalse(source.exists())
            self.assertIn("59S", target.read_text())
            self.assertNotIn("SOAPBOX: stale", target.read_text())
            self.assertTrue((repo / "state/reconstruction/ledgers").exists())

    def test_updater_repairs_unchanged_history_and_remembers_selection(self):
        with tempfile.TemporaryDirectory() as temp:
            _remote, repo, _other = fixtures.ArchiveUpdaterTests().make_remote_and_clones(Path(temp))
            args = ["archive_updater.py", "--repo", str(repo), "--phase", "reconstruct",
                    "--repair-reconstruction", "--repair-contests", "CQWW,EU_VHF_CONTESTS"]
            with (
                mock.patch.object(sys, "argv", args),
                mock.patch.object(updater, "migrate_legacy_provider_state"),
                mock.patch.object(updater, "run_child") as child,
                mock.patch.object(updater, "source_log_changes", side_effect=AssertionError("must repair unchanged history")),
            ):
                self.assertEqual(updater.main(), 0)
                command = child.call_args.args[1]
                self.assertIn("--historical", command)
                self.assertIn("CQWW,EU_VHF_CONTESTS", command)
                self.assertNotIn("--changed-only", command)
                self.assertEqual(child.call_count, 1)
            transaction = updater.read_transaction(repo)
            self.assertEqual(transaction.repair_contests, "CQWW,EU_VHF_CONTESTS")
            with (
                mock.patch.object(sys, "argv", ["archive_updater.py", "--repo", str(repo), "--phase", "reconstruct"]),
                mock.patch.object(updater, "migrate_legacy_provider_state"),
                mock.patch.object(updater, "run_child") as child,
            ):
                self.assertEqual(updater.main(), 0)
                self.assertIn("CQWW,EU_VHF_CONTESTS", child.call_args.args[1])


if __name__ == "__main__":
    unittest.main()
