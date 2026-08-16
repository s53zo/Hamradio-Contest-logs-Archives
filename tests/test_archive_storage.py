import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path


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

    def test_normalize_rejects_paths_outside_repository(self) -> None:
        inventory = storage.ArchiveInventory(self.root)

        with self.assertRaises(ValueError):
            inventory.normalize("../outside.log")


if __name__ == "__main__":
    unittest.main()
