import hashlib
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from task_ledger import TaskLedger  # noqa: E402
import public_logs_downloader as public  # noqa: E402


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class TaskLedgerTests(unittest.TestCase):
    def test_repeating_identical_state_is_byte_stable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "state" / "downloads" / "tasks.sqlite"
            with TaskLedger(path) as ledger:
                ledger.set_hash("provider/year", "abc123", 42)
            before = file_hash(path)

            with TaskLedger(path) as ledger:
                ledger.set_hash("provider/year", "abc123", 42)
            after = file_hash(path)

            self.assertEqual(after, before)

    def test_parent_state_directory_is_created(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "nested" / "tasks.sqlite"

            with TaskLedger(path):
                pass

            self.assertTrue(path.is_file())

    def test_completed_inventory_persists_empty_results(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "tasks.sqlite"

            with TaskLedger(path) as ledger:
                ledger.set_hash(
                    "provider/year",
                    "inventory-hash",
                    10,
                    output_count=7,
                )
                record = ledger.get("provider/year")

            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.output_count, 7)
            self.assertEqual(record.empty_count, 3)

    def test_legacy_schema_is_migrated_without_losing_records(self) -> None:
        import sqlite3

        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "tasks.sqlite"
            conn = sqlite3.connect(path)
            conn.execute(
                """
                CREATE TABLE tasks (
                    task_key TEXT PRIMARY KEY,
                    list_hash TEXT NOT NULL,
                    item_count INTEGER,
                    last_checked INTEGER
                )
                """
            )
            conn.execute(
                "INSERT INTO tasks VALUES (?, ?, ?, ?)",
                ("provider/year", "abc123", 42, 1),
            )
            conn.commit()
            conn.close()

            with TaskLedger(path) as ledger:
                record = ledger.get("provider/year")

            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.list_hash, "abc123")
            self.assertEqual(record.item_count, 42)
            self.assertIsNone(record.output_count)
            self.assertIsNone(record.empty_count)

    def test_empty_results_do_not_make_valid_inventory_look_stale(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "tasks.sqlite"
            items = [f"station-{number}" for number in range(10)]
            list_hash, _ = public.hash_items(items)
            expected = [Path(f"station-{number}.log") for number in range(10)]

            with TaskLedger(path) as ledger:
                ledger.set_hash(
                    "provider/year",
                    list_hash,
                    len(items),
                    output_count=7,
                )
                previous = public.TASK_LEDGER
                public.TASK_LEDGER = ledger
                try:
                    with mock.patch.object(
                        public,
                        "valid_existing_log",
                        side_effect=lambda candidate: int(candidate.stem.split("-")[1]) < 7,
                    ):
                        skip, _, _ = public.task_should_skip_known_outputs(
                            "provider/year", items, expected
                        )
                    self.assertTrue(skip)

                    with mock.patch.object(
                        public,
                        "valid_existing_log",
                        side_effect=lambda candidate: int(candidate.stem.split("-")[1]) < 6,
                    ):
                        skip, _, _ = public.task_should_skip_known_outputs(
                            "provider/year", items, expected
                        )
                    self.assertFalse(skip)
                finally:
                    public.TASK_LEDGER = previous

    def test_public_downloader_closes_ledger_on_early_return(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            ledger = TaskLedger(Path(temp) / "tasks.sqlite")
            previous = public.TASK_LEDGER
            public.TASK_LEDGER = ledger
            try:
                with mock.patch.object(public, "_main", return_value=0):
                    self.assertEqual(public.main(), 0)
                self.assertIsNone(public.TASK_LEDGER)
                with self.assertRaisesRegex(RuntimeError, "closed"):
                    ledger.get("provider/year")
            finally:
                if public.TASK_LEDGER is not None:
                    public.TASK_LEDGER.close()
                public.TASK_LEDGER = previous


if __name__ == "__main__":
    unittest.main()
