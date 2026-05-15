import importlib.util
import sys
import unittest
from pathlib import Path


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
