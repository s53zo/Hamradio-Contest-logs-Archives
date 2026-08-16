import hashlib
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from migrate_updater_state import migrate_ok1wc_markers  # noqa: E402
from provider_state import ProviderState  # noqa: E402


class ProviderStateTests(unittest.TestCase):
    def test_identical_scope_update_is_byte_stable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "state/providers/provider.json"
            state = ProviderState(path)
            values = {"inventory_hash": "abc", "inventory_count": 42}
            self.assertTrue(state.update_scope("2026", values))
            before = hashlib.sha256(path.read_bytes()).hexdigest()

            self.assertFalse(state.update_scope("2026", values))

            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), before)

    def test_ok1wc_marker_migration_verifies_and_removes_legacy(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            marker = root / "OK1WC_Memorial/2026-08-03/.pub_level_4.complete"
            marker.parent.mkdir(parents=True)
            marker.write_text(
                "jobdate=260803\nkolo=726\npub_level=4\ncalls=128\n",
                encoding="ascii",
            )

            count, changed = migrate_ok1wc_markers(root, remove_legacy=True)

            self.assertEqual(count, 1)
            self.assertTrue(changed)
            self.assertFalse(marker.exists())
            state = ProviderState(root / "state/providers/ok1wc.json")
            self.assertEqual(
                state.get_scope("2026-08-03"),
                {"calls": 128, "jobdate": "260803", "kolo": "726", "pub_level": "4"},
            )


if __name__ == "__main__":
    unittest.main()
