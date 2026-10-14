import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock


def load_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "download_darc_logs.py"
    scripts_dir = module_path.parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    spec = importlib.util.spec_from_file_location("download_darc_logs_test", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


darc = load_module()


class DARCProviderTests(unittest.TestCase):
    def test_latest_year_retains_all_published_editions(self):
        import download_darc_logs as module
        import public_logs_downloader as public
        spec = module.ContestSpec('fixture', 'fixture', Path('DARC/Fixture'), 'Fixture', ('jan', 'apr'))
        with (
            mock.patch.dict(module.CONTESTS, {'fixture': spec}),
            mock.patch.object(module, 'discover_loglist', return_value=(2026, 'apr', ['S53ZO'])),
            mock.patch.object(module, 'discover_periods', return_value=[(2026, 'jan'), (2026, 'apr')]),
            mock.patch.object(module, 'discover_calls_for_period', return_value=['S53ZO']),
            mock.patch.object(module, 'period_has_public_logs', return_value=True),
            mock.patch.object(public, 'task_should_skip_known_outputs', return_value=(False, 'hash', 1)),
            mock.patch.object(public, 'valid_existing_log', return_value=False),
            mock.patch.object(public, 'remove_invalid_existing'),
        ):
            tasks = public._build_darc_tasks(('fixture',), 'Fixture', 1)
        self.assertEqual([task.dest.parent.name for task in tasks], ['jan', 'apr'])

    def test_result_period_ignores_later_framework_timestamp(self):
        page = '<h2>DARC Weihnachtscontest (XMAS) 2025 Final scores</h2><footer>Generated 2026</footer>'
        self.assertEqual(darc.current_period_from_loglist(page), (2025, None))

    def test_result_period_recognizes_quarterly_edition(self):
        page = '<h2>DARC RTTY-Kurzcontest 2026/oct Classified logs</h2>'
        self.assertEqual(darc.current_period_from_loglist(page), (2026, 'oct'))

    def test_fetch_log_includes_custom_request_parameters(self):
        calls = []
        original_fetch_text = darc.fetch_text
        spec = darc.ContestSpec(
            key="custom",
            base="customlog",
            output_root=Path("DARC") / "Custom",
            label="DARC Custom",
            request_params={"contest": "custom"},
        )

        def fake_fetch_text(url, *args, **kwargs):
            calls.append(url)
            return "START-OF-LOG: 3.0\nEND-OF-LOG:"

        darc.fetch_text = fake_fetch_text
        try:
            darc.fetch_log(spec, "S53M", 2026, edition="jan")
        finally:
            darc.fetch_text = original_fetch_text

        self.assertEqual(len(calls), 1)
        url = calls[0]
        self.assertIn("contest=custom", url)
        self.assertIn("call=S53M", url)
        self.assertIn("jahr=2026", url)
        self.assertIn("edition=jan", url)

    def test_fetch_log_without_contest_param_for_fieldday(self):
        calls = []
        original_fetch_text = darc.fetch_text

        def fake_fetch_text(url, *args, **kwargs):
            calls.append(url)
            return "START-OF-LOG: 3.0\nEND-OF-LOG:"

        darc.fetch_text = fake_fetch_text
        try:
            darc.fetch_log(darc.CONTESTS["fieldday_cw"], "S53M", 2026)
        finally:
            darc.fetch_text = original_fetch_text

        self.assertEqual(len(calls), 1)
        url = calls[0]
        self.assertNotIn("contest=", url)


if __name__ == "__main__":
    unittest.main()
