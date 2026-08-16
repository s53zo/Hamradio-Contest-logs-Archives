import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import download_vhfmanager_logs as vhf  # noqa: E402


class VhfManagerDiscoveryTests(unittest.TestCase):
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
