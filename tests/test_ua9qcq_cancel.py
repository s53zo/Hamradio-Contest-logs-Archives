import importlib.util
import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path


def load_helper_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "download_russian_dx_contest_ubn.py"
    scripts_dir = module_path.parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    spec = importlib.util.spec_from_file_location("rdxc_ubn_cancel_test", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


rdxc = load_helper_module()


class UA9QCQCancelTests(unittest.TestCase):
    def test_fetch_for_date_aborts_before_processing_calls(self):
        original_fetch_text = rdxc.fetch_text
        original_parse_more_info_entries = rdxc.parse_more_info_entries
        original_fetch_station_meta = rdxc.fetch_station_meta
        original_fetch_text_with_cookie = rdxc.fetch_text_with_cookie

        rdxc.fetch_text = lambda *_args, **_kwargs: "<html></html>"
        rdxc.parse_more_info_entries = lambda _html: [
            {"callsign": "S53M", "id_res": "1"},
            {"callsign": "K1ABC", "id_res": "2"},
        ]
        rdxc.fetch_station_meta = self.fail
        rdxc.fetch_text_with_cookie = self.fail

        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = io.StringIO()
                with redirect_stdout(output):
                    stats = rdxc.fetch_for_date(
                        cookie="cookie",
                        year="2026",
                        contest_date="0",
                        output_dir=Path(tmp),
                        sleep_s=0.0,
                        start_time=0,
                        include_errors=True,
                        limit_saved=None,
                        progress_every=None,
                        should_abort=lambda: True,
                    )
        finally:
            rdxc.fetch_text = original_fetch_text
            rdxc.parse_more_info_entries = original_parse_more_info_entries
            rdxc.fetch_station_meta = original_fetch_station_meta
            rdxc.fetch_text_with_cookie = original_fetch_text_with_cookie

        self.assertTrue(stats.aborted)
        self.assertEqual(stats.abort_reason, "download interrupted")
        self.assertEqual(stats.total_calls, 0)
        self.assertEqual(stats.saved_logs, 0)
        self.assertEqual(stats.errors, 1)
        self.assertIn("[Russian DX Contest] progress 2026 0:", output.getvalue())

    def test_idle_timeout_is_retryable_and_not_counted_as_download_error(self):
        original_fetch_text = rdxc.fetch_text
        original_parse_more_info_entries = rdxc.parse_more_info_entries
        original_monotonic = rdxc.time.monotonic

        rdxc.fetch_text = lambda *_args, **_kwargs: "<html></html>"
        rdxc.parse_more_info_entries = lambda _html: [
            {"callsign": "S53M", "id_res": "1"},
        ]
        times = iter([0.0, 901.0])
        rdxc.time.monotonic = lambda: next(times)

        try:
            with tempfile.TemporaryDirectory() as tmp:
                stats = rdxc.fetch_for_date(
                    cookie="cookie",
                    year="2026",
                    contest_date="0",
                    output_dir=Path(tmp),
                    sleep_s=0.0,
                    start_time=0,
                    include_errors=True,
                    limit_saved=None,
                    progress_every=None,
                    max_idle_seconds=900,
                )
        finally:
            rdxc.fetch_text = original_fetch_text
            rdxc.parse_more_info_entries = original_parse_more_info_entries
            rdxc.time.monotonic = original_monotonic

        self.assertTrue(stats.aborted)
        self.assertEqual(stats.abort_reason, "idle timeout after 900s without progress")
        self.assertEqual(stats.errors, 0)
        self.assertEqual(stats.total_calls, 0)


if __name__ == "__main__":
    unittest.main()
