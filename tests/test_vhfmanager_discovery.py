import sys
import subprocess
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

    def test_remote_only_legacy_markers_migrate_in_sparse_checkout(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.name", "Test"], cwd=root, check=True)
            subprocess.run(
                ["git", "config", "user.email", "test@example.invalid"],
                cwd=root,
                check=True,
            )
            legacy = root / "EU_VHF_CONTESTS/.checklogs/481/294968.done"
            legacy.parent.mkdir(parents=True)
            legacy.write_text("ok\n", encoding="ascii")
            subprocess.run(["git", "add", "."], cwd=root, check=True)
            subprocess.run(["git", "commit", "-qm", "fixture"], cwd=root, check=True)
            legacy.unlink()
            legacy.parent.rmdir()
            legacy.parent.parent.rmdir()

            self.assertEqual(vhf.migrate_legacy_checklog_markers(root), 1)

            marker = root / "state/providers/vhfmanager/checklogs/481/294968.done"
            self.assertEqual(marker.read_text(), "ok\n")
            status = subprocess.run(
                ["git", "status", "--short"],
                cwd=root,
                check=True,
                text=True,
                stdout=subprocess.PIPE,
            ).stdout
            self.assertIn("EU_VHF_CONTESTS/.checklogs/481/294968.done", status)

            subprocess.run(
                ["git", "update-index", "--force-remove", "--", str(legacy.relative_to(root))],
                cwd=root,
                check=True,
            )
            self.assertEqual(vhf.migrate_legacy_checklog_markers(root), 1)

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

    def test_recent_year_discovery_continues_past_newest_contest(self) -> None:
        years = {500: 2026, 499: 2026, 498: 2025}

        def result_page(url, retries=3, delay=1.0):
            contest_id = int(url.split("ContestID=", 1)[1].split("&", 1)[0])
            if contest_id not in years:
                return "<html><title>No results</title></html>"
            return f'<html><title>Contest {contest_id}</title>display_log</html>'

        with (
            mock.patch.object(vhf, "MAX_CONTEST_ID", 500),
            mock.patch.object(vhf, "fetch_text", side_effect=result_page),
            mock.patch.object(vhf, "parse_log_links", return_value=[]),
            mock.patch.object(
                vhf,
                "contest_year_from_links",
                side_effect=lambda contest, _links: years[contest.cid],
            ),
        ):
            contests = vhf.discover_contests(None, recent_years=1)

        self.assertEqual([contest.cid for contest in contests], [500, 499])


if __name__ == "__main__":
    unittest.main()
