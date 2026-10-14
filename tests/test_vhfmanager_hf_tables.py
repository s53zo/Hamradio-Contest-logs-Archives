import sys
import unittest
import tempfile
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import download_vhfmanager_logs as vhf
import public_logs_downloader as public


class VhfManagerHFTablesTests(unittest.TestCase):
    def test_reference_links_survive_hf_table_attributes(self):
        page = '''<table class="table-log table-striped"><thead><tr><th>Znak</th></tr></thead>
        <tbody class="rows"><tr><td><a href="display_log.php?ContestID=478&amp;logID=123">S51DX</a></td></tr>
        <tr><td><a href="display_log.php?ContestID=478&amp;logID=123">S51DX</a></td></tr></tbody></table>'''
        links = vhf.extract_checklog_links(page, "https://vhfmanager.net/modules/display_log.php")
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].call_hint, "S51DX")
        self.assertEqual(vhf.log_id_from_url(links[0].url), 123)

    def parse(self, headings, cells):
        page = '<table><thead><tr>' + ''.join(f'<th>{x}</th>' for x in headings)
        page += '</tr></thead><tbody><tr>' + ''.join(f'<td>{x}</td>' for x in cells)
        page += '</tr></tbody></table>'
        contest = vhf.Contest(1, "HF Contest", "https://example.invalid")
        return vhf.parse_qsos(page, contest, "S53ZO", None, None, None)

    def test_ar_memorial_combined_datetime_preserves_both_exchanges(self):
        qsos = self.parse(
            ["Čas", "Znak", "Band", "Mode", "RST", "NRs", "I", "RST", "NRr"],
            ["01.05.26 13:04", "S51DX", "7 MHz", "CW", "599", "1", "A", "599", "4"],
        )
        self.assertEqual(qsos, [(7000, "2026-05-01", "1304", "S51DX",
                                "599", "1", "599", "4", "CW", 7000)])
        self.assertEqual(vhf.band_label_from_qsos(qsos), "HF")

    def test_kvp_frequency_layout_retains_exact_frequency(self):
        qsos = self.parse(
            ["Frekvenca", "Datum", "GMT", "Znak", "Mode", "RST", "Let.", "RST", "Let."],
            ["3698 kHz", "19.04.26", "07:00", "S53M", "SSB", "59", "83", "59", "50*"],
        )
        self.assertEqual(qsos, [(3698, "2026-04-19", "0700", "S53M",
                                "59", "83", "59", "50", "PH", 3698)])

    def test_hf_recovery_overrides_old_completed_task_and_markers_once(self):
        contest = vhf.Contest(478, "AR Memorial 2026", "https://example.invalid/results")
        link = vhf.LogLink("https://example.invalid/display_log.php?ContestID=478&logID=1",
                           "S53M", "Single operator")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with (
                mock.patch.object(public, "REPO_ROOT", root),
                mock.patch.object(vhf, "migrate_legacy_checklog_markers"),
                mock.patch.object(vhf, "discover_contests", return_value=[contest]),
                mock.patch.object(vhf, "discover_logs", return_value=(contest, [link])),
                mock.patch.object(vhf, "fetch_text", return_value="HF page"),
                mock.patch.object(vhf, "parse_qsos_hf", return_value=[]),
                mock.patch.object(public, "task_should_skip", return_value=(True, "hash", 1)),
                mock.patch.object(vhf, "download_contest_logs", return_value={"ok": 1}) as download,
            ):
                tasks = public.tasks_vhfmanager(1)
                self.assertEqual(len(tasks), 1)
                tasks[0].action()
                self.assertTrue(download.call_args.kwargs["recheck_processed"])
                self.assertEqual(public.tasks_vhfmanager(1), [])


if __name__ == "__main__":
    unittest.main()
