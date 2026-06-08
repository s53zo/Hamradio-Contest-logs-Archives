import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


def load_public_downloader_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "public_logs_downloader.py"
    scripts_dir = module_path.parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    spec = importlib.util.spec_from_file_location("public_logs_downloader_istra_test", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_istra_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "download_istra_open_logs.py"
    scripts_dir = module_path.parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    spec = importlib.util.spec_from_file_location("download_istra_open_logs", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ioc = load_istra_module()
pld = load_public_downloader_module()


class IstraOpenProviderTests(unittest.TestCase):
    def test_discover_log_urls_extracts_unique_log_links(self):
        html = """
        <a href="9a1bm.log">9a1bm.log</a>
        <a href="S53M.log">S53M.log</a>
        <a href="S53M.log">duplicate</a>
        <a href="rules.html">rules</a>
        """
        original_fetch_text = ioc.fetch_text
        ioc.fetch_text = lambda *_args, **_kwargs: html
        try:
            logs = ioc.discover_log_urls(2026, "https://ioc.9a1p.com/public_logs_2026/")
        finally:
            ioc.fetch_text = original_fetch_text

        self.assertEqual(
            logs,
            [
                ("9A1BM", "https://ioc.9a1p.com/public_logs_2026/9a1bm.log"),
                ("S53M", "https://ioc.9a1p.com/public_logs_2026/S53M.log"),
            ],
        )

    def test_tasks_istra_open_uses_expected_archive_paths(self):
        original_output_root = ioc.OUTPUT_ROOT
        original_years = ioc.discover_year_urls
        original_logs = ioc.discover_log_urls
        original_should_skip = pld.task_should_skip_known_outputs
        original_valid_existing = pld.valid_existing_log
        original_remove_invalid = pld.remove_invalid_existing

        with tempfile.TemporaryDirectory() as tmp:
            ioc.OUTPUT_ROOT = Path(tmp) / "Istra_Open_Contest"
            ioc.discover_year_urls = lambda: [(2026, "https://ioc.9a1p.com/public_logs_2026/")]
            ioc.discover_log_urls = lambda _year, _url=None: [
                ("S53M", "https://ioc.9a1p.com/public_logs_2026/S53M.log"),
            ]
            pld.task_should_skip_known_outputs = (
                lambda _task_key, _items, dests, **_kwargs: (False, "hash", len(list(dests)))
            )
            pld.valid_existing_log = lambda _path: False
            pld.remove_invalid_existing = lambda _path: False

            try:
                tasks = pld.tasks_istra_open(last=1)
            finally:
                ioc.OUTPUT_ROOT = original_output_root
                ioc.discover_year_urls = original_years
                ioc.discover_log_urls = original_logs
                pld.task_should_skip_known_outputs = original_should_skip
                pld.valid_existing_log = original_valid_existing
                pld.remove_invalid_existing = original_remove_invalid

        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].dest.name, "S53M.log")
        self.assertEqual(tasks[0].dest.parent.name, "2026")
        self.assertEqual(tasks[0].source, "Istra Open")
        self.assertEqual(tasks[0].task_key, "Istra_Open_Contest/2026")
        self.assertEqual(Path(tasks[0].output_roots[0]).name, "Istra_Open_Contest")


if __name__ == "__main__":
    unittest.main()
