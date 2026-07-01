import importlib.util
import http.client
import sys
import tempfile
import unittest
from pathlib import Path


def load_module(name: str, path: Path):
    scripts_dir = path.parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


REPO_ROOT = Path(__file__).resolve().parents[1]
ttc = load_module("download_ttc_spcwc_logs", REPO_ROOT / "scripts" / "download_ttc_spcwc_logs.py")
pld = load_module("public_logs_downloader_ttc_test", REPO_ROOT / "scripts" / "public_logs_downloader.py")


class TtcSpcwcProviderTests(unittest.TestCase):
    def test_parse_rounds_ignores_unpublished_rows(self):
        html = """
        <table>
          <tr><td>05</td><td>2026-06-30</td><td>72 / 5310</td></tr>
          <tr><td>04</td><td><a href="/25/ranking">2026-06-23</a></td></tr>
          <tr><td>03</td><td><a href="/23/ranking">2026-06-16</a></td></tr>
        </table>
        """

        rounds = ttc.parse_rounds(html)

        self.assertEqual([round_info.round_id for round_info in rounds], ["25", "23"])
        self.assertEqual(rounds[0].date, "2026-06-23")
        self.assertEqual(rounds[0].ranking_url, "https://spcwc.pl/25/ranking?lang=en")

    def test_parse_station_links_cleans_category_and_calls(self):
        html = """
        <a href="/25/log/SM0OEK">SM0OEK</a>
        <a href="/25/log/DL4ME">DL4ME ★</a>
        <a href="/25/log/SM0OEK">duplicate</a>
        <a href="/ttc/rules/en">rules</a>
        """
        round_info = ttc.Round("25", "2026-06-23", "https://spcwc.pl/25/ranking?lang=en")

        stations = ttc.parse_station_links(html, round_info, "SO40-LP")

        self.assertEqual(
            stations,
            [
                ttc.StationLog("25", "2026-06-23", "DL4ME", "SO40-LP", "https://spcwc.pl/25/log/DL4ME?lang=en"),
                ttc.StationLog("25", "2026-06-23", "SM0OEK", "SO40-LP", "https://spcwc.pl/25/log/SM0OEK?lang=en"),
            ],
        )

    def test_parse_qso_rows_and_build_cabrillo(self):
        html = """
        <table><tbody>
        <tr class="qso-ok" data-errors="">
          <td>2026-06-23</td><td>19:30</td><td data-band="40M" data-freq="7026">40M</td><td>CW</td>
          <td>SM0OEK</td><td>599</td><td>001</td><td>SN1T</td><td>599</td><td>001</td><td>OK</td>
        </tr>
        <tr class="qso-err" data-errors="INVALID_QSO">
          <td>2026-06-23</td><td>20:29</td><td data-band="40M" data-freq="7027">40M</td><td>CW</td>
          <td>SM0OEK</td><td>599</td><td>127</td><td>DL4ME ★</td><td>599</td><td>127</td><td>INVALID_QSO</td>
        </tr>
        </tbody></table>
        """
        station = ttc.StationLog("25", "2026-06-23", "SM0OEK", "SO40-LP", "https://spcwc.pl/25/log/SM0OEK?lang=en")

        qsos = ttc.parse_qsos(html)
        cabrillo = ttc.build_cabrillo(station, qsos)

        self.assertEqual(len(qsos), 2)
        self.assertEqual(qsos[1].correspondent, "DL4ME")
        self.assertIn("CATEGORY: SO40-LP", cabrillo)
        self.assertIn("CLAIMED-SCORE: 1", cabrillo)
        self.assertIn("QSO:  7027 CW 2026-06-23 2029 SM0OEK", cabrillo)
        self.assertNotIn("★", cabrillo)

    def test_parse_expected_station_count(self):
        html = '<h1>Ranking — SO40-LP</h1><span>2026-06-23 &nbsp;·&nbsp; 38 stations</span>'

        self.assertEqual(ttc.parse_expected_station_count(html), 38)

    def test_parse_expected_qso_count_uses_largest_qso_count(self):
        html = """
        <button>6 QSOs with errors</button>
        <div>Round 2026-06-23 · 127 QSOs</div>
        """

        self.assertEqual(ttc.parse_expected_qso_count(html), 127)

    def test_tasks_ttc_spcwc_uses_expected_archive_paths(self):
        original_rounds = ttc.iter_rounds
        original_stations = ttc.discover_station_logs
        original_fetch_log = ttc.fetch_log
        original_output_root = ttc.OUTPUT_ROOT
        original_should_skip = pld.task_should_skip_known_outputs
        original_valid_existing = pld.valid_existing_log
        original_remove_invalid = pld.remove_invalid_existing

        with tempfile.TemporaryDirectory() as tmp:
            expected_output_root = Path(tmp) / "TTC-SPCWC"
            ttc.OUTPUT_ROOT = expected_output_root
            round_info = ttc.Round("25", "2026-06-23", "https://spcwc.pl/25/ranking?lang=en")
            station = ttc.StationLog("25", "2026-06-23", "SM0OEK", "SO40-LP", "https://spcwc.pl/25/log/SM0OEK?lang=en")
            ttc.iter_rounds = lambda _last=None: [round_info]
            ttc.discover_station_logs = lambda _round: [station]
            ttc.fetch_log = lambda _station: "START-OF-LOG: 3.0\nEND-OF-LOG:\n"
            pld.task_should_skip_known_outputs = (
                lambda _task_key, _items, dests, **_kwargs: (False, "hash", len(list(dests)))
            )
            pld.valid_existing_log = lambda _path: False
            pld.remove_invalid_existing = lambda _path: False

            try:
                tasks = pld.tasks_ttc_spcwc(last=1)
            finally:
                ttc.iter_rounds = original_rounds
                ttc.discover_station_logs = original_stations
                ttc.fetch_log = original_fetch_log
                ttc.OUTPUT_ROOT = original_output_root
                pld.task_should_skip_known_outputs = original_should_skip
                pld.valid_existing_log = original_valid_existing
                pld.remove_invalid_existing = original_remove_invalid

        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].dest.name, "SM0OEK.log")
        self.assertEqual(tasks[0].dest.parent.name, "2026-06-23")
        self.assertEqual(tasks[0].dest.parents[1], expected_output_root)
        self.assertEqual(tasks[0].source, "TTC-SPCWC")
        self.assertEqual(tasks[0].task_key, "TTC-SPCWC/2026-06-23")
        self.assertEqual(Path(tasks[0].output_roots[0]).name, "TTC-SPCWC")

    def test_fetch_log_retries_short_qso_page(self):
        short_html = """
        <div>Round 2026-06-23 · 2 QSOs</div>
        <table><tbody>
        <tr data-errors=""><td>2026-06-23</td><td>19:30</td><td data-band="40M" data-freq="7026">40M</td><td>CW</td><td>SM0OEK</td><td>599</td><td>001</td><td>SN1T</td><td>599</td><td>001</td><td>OK</td></tr>
        </tbody></table>
        """
        full_html = """
        <div>Round 2026-06-23 · 2 QSOs</div>
        <table><tbody>
        <tr data-errors=""><td>2026-06-23</td><td>19:30</td><td data-band="40M" data-freq="7026">40M</td><td>CW</td><td>SM0OEK</td><td>599</td><td>001</td><td>SN1T</td><td>599</td><td>001</td><td>OK</td></tr>
        <tr data-errors=""><td>2026-06-23</td><td>19:31</td><td data-band="40M" data-freq="7027">40M</td><td>CW</td><td>SM0OEK</td><td>599</td><td>002</td><td>DL4ME</td><td>599</td><td>002</td><td>OK</td></tr>
        </tbody></table>
        """
        calls = []
        original_fetch_text = ttc.fetch_text
        station = ttc.StationLog("25", "2026-06-23", "SM0OEK", "SO40-LP", "https://spcwc.pl/25/log/SM0OEK?lang=en")
        try:
            def fake_fetch(*args, **kwargs):
                calls.append(1)
                return short_html if len(calls) == 1 else full_html

            ttc.fetch_text = fake_fetch
            cabrillo = ttc.fetch_log(station)
        finally:
            ttc.fetch_text = original_fetch_text

        self.assertEqual(len(calls), 2)
        self.assertEqual(cabrillo.count("\nQSO:"), 2)

    def test_fetch_text_retries_incomplete_reads_by_default(self):
        attempts = 0

        class PartialResponse:
            headers = type("Headers", (), {"get_content_charset": lambda self: "utf-8"})()

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _size):
                raise http.client.IncompleteRead(b"partial html")

        def fail_urlopen(*_args, **_kwargs):
            nonlocal attempts
            attempts += 1
            return PartialResponse()

        original_urlopen = ttc.urllib.request.urlopen
        ttc.urllib.request.urlopen = fail_urlopen
        try:
            with self.assertRaises(http.client.IncompleteRead):
                ttc.fetch_text("https://spcwc.pl/25/ranking", retries=2, delay=0)
        finally:
            ttc.urllib.request.urlopen = original_urlopen
        self.assertEqual(attempts, 2)

    def test_fetch_text_can_keep_partial_log_payload(self):
        class PartialResponse:
            headers = type("Headers", (), {"get_content_charset": lambda self: "utf-8"})()

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _size):
                raise http.client.IncompleteRead(b"partial html")

        original_urlopen = ttc.urllib.request.urlopen
        ttc.urllib.request.urlopen = lambda *_args, **_kwargs: PartialResponse()
        try:
            self.assertEqual(
                ttc.fetch_text("https://spcwc.pl/25/log/SM0OEK", retries=1, allow_incomplete=True),
                "partial html",
            )
        finally:
            ttc.urllib.request.urlopen = original_urlopen


if __name__ == "__main__":
    unittest.main()
