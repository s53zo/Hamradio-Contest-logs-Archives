import hashlib
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

import shard_index  # noqa: E402
import public_logs_downloader as public  # noqa: E402


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ShardIndexTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        (self.root / "SH6").mkdir()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def write_log(self, rel: str, mode: str = "CW") -> Path:
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "\n".join(
                [
                    "START-OF-LOG: 3.0",
                    f"CATEGORY-MODE: {mode}",
                    "END-OF-LOG:",
                    "",
                ]
            ),
            encoding="ascii",
        )
        return path

    def row_for(self, rel: str):
        shard = shard_index.shard_path_for(self.root / "SH6", Path(rel))
        with closing(sqlite3.connect(shard)) as conn:
            return conn.execute(
                "SELECT path, callsign, contest, year, mode FROM logs WHERE path = ?",
                (rel,),
            ).fetchone()

    @staticmethod
    def all_rows(shard_root: Path) -> list[tuple[object, ...]]:
        rows: list[tuple[object, ...]] = []
        for shard in sorted(shard_root.glob("logs_*.sqlite")):
            with closing(sqlite3.connect(shard)) as conn:
                rows.extend(
                    conn.execute(
                        """
                        SELECT path, callsign, contest, year, mode, season,
                               subcontest, detail
                        FROM logs
                        """
                    )
                )
        return sorted(rows, key=lambda row: str(row[0]))

    def test_incremental_insert_replace_and_noop_are_deterministic(self) -> None:
        rel = "YOTA_Contest/2026/Round_1/S53ZO.log"
        self.write_log(rel, "CW")

        first = shard_index.apply_path_delta(self.root, [Path(rel)])
        shard = next(iter(first.changed_shards))
        first_hash = sha256(shard)
        self.assertEqual(self.row_for(rel), (rel, "S53ZO", "YOTA_Contest", 2026, "CW"))

        second = shard_index.apply_path_delta(self.root, [Path(rel)])
        self.assertEqual(second.upserted, 0)
        self.assertEqual(second.changed_shards, set())
        self.assertEqual(sha256(shard), first_hash)

        self.write_log(rel, "RTTY")
        third = shard_index.apply_path_delta(self.root, [Path(rel)])
        self.assertEqual(third.upserted, 1)
        self.assertEqual(self.row_for(rel)[4], "RTTY")

    def test_incremental_delete_is_explicit(self) -> None:
        rel = "YOTA_Contest/2026/Round_1/S53ZO.log"
        path = self.write_log(rel)
        shard_index.apply_path_delta(self.root, [Path(rel)])
        path.unlink()

        result = shard_index.apply_path_delta(self.root, [], deleted=[Path(rel)])

        self.assertEqual(result.deleted, 1)
        self.assertIsNone(self.row_for(rel))

    def test_incremental_rows_match_reference_full_build(self) -> None:
        paths = [
            "YOTA_Contest/2026/Round_1/S53ZO.log",
            "DARC/WAG/2025/DL1ABC.log",
            "ZRS_KVP/2024/pomlad/S51A.log",
        ]
        self.write_log(paths[0], "RTTY")
        self.write_log(paths[1], "SSB")
        self.write_log(paths[2], "CW")
        incremental = self.root / "SH6-incremental"
        reference = self.root / "SH6-reference"

        shard_index.apply_path_delta(
            self.root,
            [Path(path) for path in paths],
            shard_root=incremental,
        )
        public.build_sqlite_shards(self.root, reference, progress_every=0)

        self.assertEqual(self.all_rows(incremental), self.all_rows(reference))

    def test_schema_migration_deduplicates_paths_and_adds_unique_index(self) -> None:
        rel = Path("YOTA_Contest/2026/Round_1/S53ZO.log")
        shard = shard_index.shard_path_for(self.root / "SH6", rel)
        with closing(sqlite3.connect(shard)) as conn:
            with conn:
                shard_index.create_logs_table(conn)
                values = (rel.as_posix(), "S53ZO", "YOTA_Contest", 2026, "CW", None, None, None)
                conn.execute("INSERT INTO logs VALUES (?, ?, ?, ?, ?, ?, ?, ?)", values)
                conn.execute("INSERT INTO logs VALUES (?, ?, ?, ?, ?, ?, ?, ?)", values)

        shard_index.ensure_path_index(shard)

        with closing(sqlite3.connect(shard)) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM logs").fetchone()[0], 1)
            indexes = {row[1] for row in conn.execute("PRAGMA index_list(logs)")}
        self.assertIn("idx_path", indexes)

    def test_git_tree_audit_finds_missing_and_extra_paths_without_log_checkout(self) -> None:
        tracked = "YOTA_Contest/2026/Round_1/S53ZO.log"
        extra = "YOTA_Contest/2026/Round_1/EXTRA.log"
        source = self.write_log(tracked)
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=self.root, check=True)
        subprocess.run(["git", "add", tracked], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-qm", "fixture"], cwd=self.root, check=True)
        source.unlink()
        self.write_log(extra)
        shard_index.apply_path_delta(self.root, [Path(extra)])

        audit = shard_index.audit_git_tree(self.root)

        self.assertEqual(audit.expected, 1)
        self.assertEqual(audit.indexed, 1)
        self.assertEqual(audit.missing, [tracked])
        self.assertEqual(audit.extra, [extra])

    def test_worktree_delta_includes_modified_untracked_and_deleted_logs(self) -> None:
        modified = "YOTA_Contest/2026/Round_1/MOD.log"
        deleted = "YOTA_Contest/2026/Round_1/DEL.log"
        untracked = "YOTA_Contest/2026/Round_1/NEW.log"
        self.write_log(modified)
        self.write_log(deleted)
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=self.root, check=True)
        subprocess.run(["git", "add", "YOTA_Contest"], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-qm", "fixture"], cwd=self.root, check=True)
        self.write_log(modified, "RTTY")
        (self.root / deleted).unlink()
        self.write_log(untracked)

        delta = shard_index.worktree_log_delta(self.root)

        self.assertEqual(delta.added_or_modified, [Path(modified), Path(untracked)])
        self.assertEqual(delta.deleted, [Path(deleted)])


if __name__ == "__main__":
    unittest.main()
