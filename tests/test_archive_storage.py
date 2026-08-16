import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch


SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import archive_storage as storage  # noqa: E402


class ArchiveStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        (self.root / "SH6").mkdir()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def add_index_row(self, rel_path: str) -> None:
        rel = Path(rel_path)
        bucket = storage.callsign_bucket(rel.stem)
        shard = self.root / "SH6" / f"logs_{bucket:02x}.sqlite"
        with closing(sqlite3.connect(shard)) as conn:
            with conn:
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
                conn.execute(
                    "INSERT INTO logs(path, callsign) VALUES (?, ?)",
                    (rel.as_posix(), rel.stem.upper()),
                )

    def test_remote_indexed_log_exists_without_worktree_file(self) -> None:
        rel = "YOTA_Contest/2026/Round_1/S53ZO.log"
        self.add_index_row(rel)

        inventory = storage.ArchiveInventory(self.root)

        self.assertTrue(inventory.log_exists(rel))
        self.assertFalse((self.root / rel).exists())

    def test_invalid_local_file_is_not_hidden_by_remote_index(self) -> None:
        rel = "YOTA_Contest/2026/Round_1/S53ZO.log"
        self.add_index_row(rel)
        path = self.root / rel
        path.parent.mkdir(parents=True)
        path.write_text("<html>rate limited</html>\n", encoding="utf-8")

        inventory = storage.ArchiveInventory(self.root)

        self.assertFalse(inventory.log_exists(rel))

    def test_valid_new_local_log_exists_before_it_is_indexed(self) -> None:
        rel = "YOTA_Contest/2026/Round_1/S53ZO.log"
        path = self.root / rel
        path.parent.mkdir(parents=True)
        path.write_text("START-OF-LOG: 3.0\nEND-OF-LOG:\n", encoding="ascii")

        inventory = storage.ArchiveInventory(self.root)

        self.assertTrue(inventory.log_exists(rel))

    def test_callsign_lookup_combines_indexed_and_new_local_logs(self) -> None:
        indexed = "EU_VHF_CONTESTS/Test/2025/144MHz/S53ZO.log"
        local = "EU_VHF_CONTESTS/Test/2026/432MHz/S53ZO.log"
        self.add_index_row(indexed)
        local_path = self.root / local
        local_path.parent.mkdir(parents=True)
        local_path.write_text("START-OF-LOG: 3.0\nEND-OF-LOG:\n", encoding="ascii")

        inventory = storage.ArchiveInventory(self.root)

        self.assertEqual(
            inventory.logs_for_callsign("S53ZO", "EU_VHF_CONTESTS/Test"),
            [Path(indexed), Path(local)],
        )

    def test_git_tree_paths_and_blob_materialization_do_not_need_source_checkout(self) -> None:
        rel = Path("YOTA_Contest/2026/Round_1/S53ZO.log")
        source = self.root / rel
        source.parent.mkdir(parents=True)
        source.write_text("START-OF-LOG: 3.0\nEND-OF-LOG:\n", encoding="ascii")
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=self.root, check=True)
        subprocess.run(["git", "add", rel.as_posix()], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-qm", "fixture"], cwd=self.root, check=True)
        source.unlink()

        inventory = storage.ArchiveInventory(self.root)

        self.assertEqual(inventory.git_paths(rel.parent), [rel])
        self.assertEqual(
            inventory.read_git_blob(rel),
            b"START-OF-LOG: 3.0\nEND-OF-LOG:\n",
        )
        materialized = self.root / "materialized"
        self.assertEqual(
            inventory.materialize_prefix(rel.parent, materialized),
            [(materialized / rel).resolve()],
        )
        self.assertEqual((materialized / rel).read_bytes(), inventory.read_git_blob(rel))
        self.assertFalse(source.exists())

    def test_missing_prefix_materialization_does_not_fetch_repository_blobs(self) -> None:
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=self.root, check=True)
        (self.root / "README.md").write_text("fixture\n", encoding="ascii")
        subprocess.run(["git", "add", "README.md"], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-qm", "fixture"], cwd=self.root, check=True)
        inventory = storage.ArchiveInventory(self.root)

        self.assertEqual(inventory.materialize_prefix("Contest/2026", self.root / "output"), [])

    def test_partial_clone_materialization_batch_fetches_only_prefix_blobs(self) -> None:
        remote = self.root / "remote.git"
        source_repo = self.root / "source"
        clone = self.root / "clone"
        subprocess.run(["git", "init", "-q", "--bare", remote], check=True)
        subprocess.run(["git", "config", "uploadpack.allowFilter", "true"], cwd=remote, check=True)
        subprocess.run(["git", "config", "uploadpack.allowAnySHA1InWant", "true"], cwd=remote, check=True)
        subprocess.run(["git", "init", "-q", "-b", "main", source_repo], check=True)
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=source_repo, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=source_repo, check=True)

        target_dir = source_repo / "Contest" / "2026"
        target_dir.mkdir(parents=True)
        for callsign in ("S53M", "S54X"):
            (target_dir / f"{callsign}.log").write_text(
                f"START-OF-LOG: 3.0\nCALLSIGN: {callsign}\nEND-OF-LOG:\n",
                encoding="ascii",
            )
        unrelated_dir = source_repo / "Unrelated" / "2026"
        unrelated_dir.mkdir(parents=True)
        for number in range(20):
            (unrelated_dir / f"TEST{number}.log").write_text(
                f"START-OF-LOG: 3.0\nCALLSIGN: TEST{number}\nEND-OF-LOG:\n",
                encoding="ascii",
            )
        subprocess.run(["git", "add", "."], cwd=source_repo, check=True)
        subprocess.run(["git", "commit", "-qm", "fixture"], cwd=source_repo, check=True)
        subprocess.run(["git", "remote", "add", "origin", remote], cwd=source_repo, check=True)
        subprocess.run(["git", "push", "-q", "origin", "main"], cwd=source_repo, check=True)
        subprocess.run(["git", "symbolic-ref", "HEAD", "refs/heads/main"], cwd=remote, check=True)
        subprocess.run(
            ["git", "clone", "-q", "--filter=blob:none", "--no-checkout", remote.as_uri(), clone],
            check=True,
        )

        trace = self.root / "git-trace.log"
        inventory = storage.ArchiveInventory(clone)
        output = self.root / "materialized-partial"
        with patch.dict(os.environ, {"GIT_TRACE": str(trace)}):
            written = inventory.materialize_prefix("Contest/2026", output)

        self.assertEqual(
            written,
            [
                (output / "Contest/2026/S53M.log").resolve(),
                (output / "Contest/2026/S54X.log").resolve(),
            ],
        )
        trace_text = trace.read_text(encoding="utf-8")
        self.assertEqual(trace_text.count("built-in: git fetch origin"), 1)
        unrelated_missing = subprocess.run(
            ["git", "rev-list", "--objects", "--missing=print", "HEAD", "--", "Unrelated"],
            cwd=clone,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        self.assertTrue(any(line.startswith("?") for line in unrelated_missing))
        self.assertFalse((output / "Unrelated").exists())

    def test_normalize_rejects_paths_outside_repository(self) -> None:
        inventory = storage.ArchiveInventory(self.root)

        with self.assertRaises(ValueError):
            inventory.normalize("../outside.log")


if __name__ == "__main__":
    unittest.main()
