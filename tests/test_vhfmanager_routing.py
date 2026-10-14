import sys
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import download_vhfmanager_logs as vhf
import download_zrs_kvp_logs as kvp
import public_logs_downloader as public


class VhfRoutingTests(unittest.TestCase):
    def test_incomplete_contest_replays_marked_seeds_without_overwriting_valid_logs(self):
        contest = vhf.Contest(1, "VHF", "https://example.invalid/results.php?ContestID=1")
        link = vhf.LogLink("https://example.invalid/display_log.php?ContestID=1&logID=10", "K1ABC", "144MHz")
        with (
            mock.patch.object(vhf, "migrate_legacy_checklog_markers"),
            mock.patch.object(vhf, "discover_contests", return_value=[contest]),
            mock.patch.object(vhf, "kvp_contest_ids", return_value=set()),
            mock.patch.object(vhf, "discover_logs", return_value=(contest, [link])),
            mock.patch.object(public, "task_should_skip", return_value=(False, "hash", 1)) as cache,
            mock.patch.object(vhf, "download_contest_logs", return_value={"skip": 1}) as download,
        ):
            task = public.tasks_vhfmanager(None)[0]
            self.assertTrue(cache.call_args.args[1][0].startswith("reference-walk-v2:"))
            task.action()
            self.assertTrue(download.call_args.kwargs["recheck_processed"])
            self.assertFalse(download.call_args.kwargs["overwrite_existing"])
    def test_seed_band_hint_prevents_microwave_log_from_defaulting_to_144mhz(self):
        contest = vhf.Contest(1, "Microwave", "https://example.invalid/results.php?ContestID=1")
        seed = vhf.LogLink("https://example.invalid/display_log.php?ContestID=1&logID=10", "K1ABC", "5700MHz")
        cells = ["01.06.26", "12:00", "K2ABC", "CW", "599", "1", "599", "2", "JN23AA"]
        page = '<table><thead><tr><th>Date</th></tr></thead><tbody><tr>' + ''.join(f'<td>{cell}</td>' for cell in cells) + '</tr></tbody></table>'
        with (
            mock.patch.object(vhf, "fetch_text", return_value=page),
            mock.patch.object(vhf, "write_checklog_marker"),
            mock.patch.object(vhf, "checklog_marker_exists", return_value=False),
            mock.patch.object(vhf, "parse_log_header", return_value=("K1ABC", None, "JN22AA", None)),
            mock.patch.object(vhf, "write_log", return_value=(Path("example.log"), "skip")) as write,
        ):
            vhf.download_contest_logs(contest, [seed], workers=1, max_logs=None, include_checklogs=False)
            self.assertEqual(write.call_args.args[2], "5700MHz")
    def test_recovery_quarantines_non_hf_files_for_known_hf_contest(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            wrong = root / "EU_VHF_CONTESTS/ar_memorial_2026_478/144MHz/S53M.log"
            correct = root / "EU_VHF_CONTESTS/ar_memorial_2026_478/HF/S53M.log"
            for path in (wrong, correct):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("preserved")
            count, backup = vhf.quarantine_untracked_vhf(root, set(), {478})
            self.assertEqual(count, 1)
            self.assertFalse(wrong.exists())
            self.assertTrue(correct.exists())
            self.assertTrue((backup / wrong.relative_to(root)).exists())
    def test_explicit_parser_recovery_replaces_stale_log_but_skips_identical_content(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / "contest/HF/K1ABC.log"
            target.parent.mkdir(parents=True)
            target.write_text("old content")
            with mock.patch.object(vhf, "archive_log_exists", return_value=True):
                self.assertEqual(vhf.write_log(root, "contest", "HF", "K1ABC", "new content", None)[1], "skip")
                self.assertEqual(target.read_text(), "old content")
                self.assertEqual(vhf.write_log(root, "contest", "HF", "K1ABC", "new content", None, overwrite=True)[1], "ok")
                self.assertEqual(target.read_text(), "new content")
                self.assertEqual(vhf.write_log(root, "contest", "HF", "K1ABC", "new content", None, overwrite=True)[1], "skip")
    def test_quarantine_preserves_tracked_and_other_contest_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            def git(*args):
                subprocess.run(["git", *args], cwd=root, check=True, stdout=subprocess.PIPE)
            git("init", "-q")
            git("config", "user.name", "Test")
            git("config", "user.email", "test@example.invalid")
            tracked = root / "EU_VHF_CONTESTS/un_2026_476/HF/S53M.log"
            tracked.parent.mkdir(parents=True)
            tracked.write_text("committed log")
            git("add", ".")
            git("commit", "-qm", "fixture")
            misplaced = root / "EU_VHF_CONTESTS/un_2026_476/144MHz/S57NRK.log"
            other = root / "EU_VHF_CONTESTS/ar_memorial_2026_478/HF/S53M.log"
            canonical = root / "ZRS_KVP/2026/pomlad/S53M.log"
            for path in (misplaced, other, canonical):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("preserved bytes")
            count, backup = vhf.quarantine_untracked_kvp(root, {476}, dry_run=True)
            self.assertEqual((count, backup), (1, None))
            self.assertTrue(misplaced.exists())
            count, backup = vhf.quarantine_untracked_kvp(root, {476})
            self.assertEqual(count, 1)
            self.assertFalse(misplaced.exists())
            self.assertEqual((backup / misplaced.relative_to(root)).read_text(), "preserved bytes")
            self.assertTrue((backup / "manifest.json").exists())
            self.assertTrue(all(path.exists() for path in (tracked, other, canonical)))

    def test_kvp_mapping_covers_both_seasons_and_all_years(self):
        seasons = [SimpleNamespace(contest_id=str(cid)) for cid in (21, 200, 476)]
        with mock.patch.object(kvp, "discover_seasons", return_value=seasons) as discovery:
            self.assertEqual(vhf.kvp_contest_ids(), {21, 200, 476})
            discovery.assert_called_once_with(None)

    def test_generic_provider_does_not_discover_kvp_logs(self):
        contests = [vhf.Contest(cid, "KVP", "https://example.invalid") for cid in (21, 476)]
        with (
            mock.patch.object(vhf, "migrate_legacy_checklog_markers"),
            mock.patch.object(vhf, "discover_contests", return_value=contests),
            mock.patch.object(vhf, "kvp_contest_ids", return_value={21, 476}),
            mock.patch.object(vhf, "discover_logs") as logs,
        ):
            self.assertEqual(public.tasks_vhfmanager(None), [])
            logs.assert_not_called()

    def test_hf_row_cannot_fall_back_to_fabricated_144mhz(self):
        page = '<table><thead><tr><th>unknown</th></tr></thead><tbody><tr>'
        cells = ["3698 kHz", "19.04.26", "08:50", "S57NRK", "SSB", "59", "95", "59", "76"]
        page += ''.join(f'<td>{cell}</td>' for cell in cells) + '</tr></tbody></table>'
        self.assertEqual(vhf.parse_qsos_vhf(page, "S57NRK", None, None), [])

    def test_cyclic_url_variants_fetch_each_log_id_only_once(self):
        contest = vhf.Contest(1, "VHF", "https://vhfmanager.net/modules/results.php?ContestID=1")
        seed = vhf.LogLink("https://vhfmanager.net/modules/display_log.php?ContestID=1&logID=10&language=G", "K1ABC", "144MHz")
        second_seed = vhf.LogLink("https://vhfmanager.net/modules/display_log.php?ContestID=1&logID=20&language=G", "K2ABC", "144MHz")
        links = [
            vhf.LogLink("https://www.vhfmanager.net/modules/display_log.php?language=S5&QSOID=200&logID=10&ContestID=1", "K2ABC", None),
            vhf.LogLink("https://vhfmanager.net/modules/display_log.php?logID=10&QSOID=300&ContestID=1&language=S5", "K1ABC", None),
        ]
        qso = (144300, "2026-06-01", "1200", "K2ABC", "59", "1 FN42", "59", "1 FN43", "PH", 144000)
        with (
            mock.patch.object(vhf, "fetch_text", return_value="complete log") as fetch,
            mock.patch.object(vhf, "checklog_marker_exists", return_value=False),
            mock.patch.object(vhf, "write_checklog_marker"),
            mock.patch.object(vhf, "parse_log_header", return_value=("K0ABC", None, None, None)),
            mock.patch.object(vhf, "parse_qsos", return_value=[qso]),
            mock.patch.object(vhf, "build_cabrillo", return_value="log"),
            mock.patch.object(vhf, "write_log", return_value=(Path("example.log"), "skip")),
            mock.patch.object(vhf, "extract_checklog_links", return_value=links),
        ):
            counts = vhf.download_contest_logs(contest, [seed, second_seed], workers=2, max_logs=None, recheck_processed=True)
            self.assertEqual(counts["skip"], 2)
            self.assertEqual(fetch.call_count, 2)
            urls = [call.args[0] for call in fetch.call_args_list]
            self.assertTrue(all("QSOID" not in url for url in urls))
            self.assertEqual({vhf.log_id_from_url(url) for url in urls}, {10, 20})

    def test_referenced_only_station_keeps_qso_view_and_does_not_mark_origin(self):
        contest = vhf.Contest(1, "VHF", "https://vhfmanager.net/modules/results.php?ContestID=1")
        seed = vhf.LogLink("https://vhfmanager.net/modules/display_log.php?ContestID=1&logID=10", "K1ABC", "144MHz")
        references = [
            vhf.LogLink(f"https://vhfmanager.net/modules/display_log.php?ContestID=1&logID={origin}&QSOID={qso}", "K3ABC", None)
            for origin, qso in ((10, 100), (20, 200))
        ]
        qso = (144300, "2026-06-01", "1200", "K2ABC", "59", "1 FN42", "59", "1 FN43", "PH", 144000)
        with (
            mock.patch.object(vhf, "fetch_text", return_value="complete log") as fetch,
            mock.patch.object(vhf, "write_checklog_marker") as marker,
            mock.patch.object(vhf, "parse_log_header", return_value=("K0ABC", None, None, None)),
            mock.patch.object(vhf, "parse_qsos", return_value=[qso]),
            mock.patch.object(vhf, "build_cabrillo", return_value="log"),
            mock.patch.object(vhf, "write_log", return_value=(Path("example.log"), "skip")),
            mock.patch.object(vhf, "extract_checklog_links", return_value=references),
        ):
            vhf.download_contest_logs(contest, [seed], workers=1, max_logs=None, recheck_processed=True)
            self.assertEqual(fetch.call_count, 2)
            self.assertIn("QSOID=100", fetch.call_args_list[1].args[0])
            self.assertEqual([call.args[1] for call in marker.call_args_list], [10, None])

    def test_reference_to_completed_seed_uses_target_marker_not_origin_id(self):
        contest = vhf.Contest(1, "VHF", "https://example.invalid/results.php?ContestID=1")
        seeds = [vhf.LogLink(f"https://example.invalid/display_log.php?ContestID=1&logID={lid}", call, "144MHz")
                 for lid, call in ((10, "K1ABC"), (20, "K2ABC"))]
        reference = vhf.LogLink("https://example.invalid/display_log.php?ContestID=1&logID=10&QSOID=100", "K2ABC", None)
        qso = (144300, "2026-06-01", "1200", "K2ABC", "59", "1 FN42", "59", "1 FN43", "PH", 144000)
        with (
            mock.patch.object(vhf, "checklog_marker_exists", side_effect=lambda contest, lid: lid == 20),
            mock.patch.object(vhf, "fetch_text", return_value="complete log") as fetch,
            mock.patch.object(vhf, "write_checklog_marker"),
            mock.patch.object(vhf, "parse_log_header", return_value=("K1ABC", None, None, None)),
            mock.patch.object(vhf, "parse_qsos", return_value=[qso]),
            mock.patch.object(vhf, "build_cabrillo", return_value="log"),
            mock.patch.object(vhf, "write_log", return_value=(Path("example.log"), "skip")),
            mock.patch.object(vhf, "extract_checklog_links", return_value=[reference]),
        ):
            vhf.download_contest_logs(contest, seeds, workers=1, max_logs=None)
            self.assertEqual(fetch.call_count, 1)


if __name__ == "__main__":
    unittest.main()
