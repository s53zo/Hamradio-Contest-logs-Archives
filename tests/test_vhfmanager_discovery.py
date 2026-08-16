import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import download_vhfmanager_logs as vhf  # noqa: E402


class VhfManagerDiscoveryTests(unittest.TestCase):
    def test_legacy_checklog_markers_migrate_to_central_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_root = root / "EU_VHF_CONTESTS"
            state_root = root / "state/providers/vhfmanager/checklogs"
            legacy = output_root / ".checklogs/488/299849.done"
            legacy.parent.mkdir(parents=True)
            legacy.write_text("ok\n", encoding="ascii")
            contest = vhf.Contest(488, "Fixture", "https://example.invalid")

            with mock.patch.object(vhf, "OUTPUT_ROOT", output_root), mock.patch.object(
                vhf, "CHECKLOG_STATE_ROOT", state_root
            ):
                self.assertEqual(vhf.migrate_legacy_checklog_markers(), 1)
                self.assertTrue(vhf.checklog_marker_exists(contest, 299849))
                vhf.write_checklog_marker(contest, 299850)

            self.assertFalse((output_root / ".checklogs").exists())
            self.assertEqual((state_root / "488/299849.done").read_text(), "ok\n")
            self.assertEqual((state_root / "488/299850.done").read_text(), "ok\n")

    def test_discovery_stops_when_provider_is_unavailable(self) -> None:
        calls = []

        def unavailable(url, retries=3, delay=1.0):
            calls.append((url, retries))
            raise ConnectionRefusedError("provider unavailable")

        with mock.patch.object(vhf, "fetch_text", side_effect=unavailable):
            with self.assertRaisesRegex(RuntimeError, "3 consecutive discovery requests"):
                vhf.discover_contests(1)

        self.assertEqual(len(calls), 3)
        self.assertTrue(all(retries == 1 for _, retries in calls))

    def test_missing_ids_do_not_trigger_transport_circuit_breaker(self) -> None:
        def available_without_logs(url, retries=3, delay=1.0):
            return "<html><title>No results</title></html>"

        with mock.patch.object(
            vhf, "fetch_text", side_effect=available_without_logs
        ):
            self.assertEqual(vhf.discover_contests(1), [])


if __name__ == "__main__":
    unittest.main()
