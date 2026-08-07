import importlib.util
import sys
import unittest
from pathlib import Path


def load_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "download_eudx_logs.py"
    scripts_dir = module_path.parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    spec = importlib.util.spec_from_file_location("download_eudx_logs_test", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


eudx = load_module()


class EudxProviderTests(unittest.TestCase):
    def test_discovers_years_from_filter(self):
        page = """
        <select id="logs_year" name="logs_year">
          <option value="2026" selected>2026</option>
          <option value="2025">2025</option>
        </select>
        """
        original_fetch_text = eudx.fetch_text
        eudx.fetch_text = lambda *_args, **_kwargs: page
        try:
            self.assertEqual(eudx.discover_years(), [2025, 2026])
        finally:
            eudx.fetch_text = original_fetch_text

    def test_discovers_paginated_tokenized_downloads(self):
        page_one = """
        <table><tbody>
          <tr>
            <td data-label="Callsign"><strong>S53M</strong></td>
            <td class="eudx-download-cell"><a class="eudx-download"
              href="/wp-admin/admin-post.php?action=eudx_download_log&amp;log_id=1&amp;token=abc">Download</a></td>
          </tr>
        </tbody></table>
        <a href="/public-logs/?logs_year=2026&amp;logs_page=2">2</a>
        """
        page_two = """
        <table><tbody>
          <tr>
            <td data-label="Callsign"><strong>9A1A</strong></td>
            <td class="eudx-download-cell"><a class="eudx-download"
              href="/wp-admin/admin-post.php?action=eudx_download_log&amp;log_id=2&amp;token=def">Download</a></td>
          </tr>
        </tbody></table>
        """
        requested = []
        original_fetch_text = eudx.fetch_text

        def fake_fetch_text(url, *_args, **_kwargs):
            requested.append(url)
            return page_two if "logs_page=2" in url else page_one

        eudx.fetch_text = fake_fetch_text
        try:
            logs = eudx.discover_log_urls(2026)
        finally:
            eudx.fetch_text = original_fetch_text

        self.assertEqual(len(requested), 2)
        self.assertEqual(
            logs,
            [
                (
                    "S53M",
                    "https://www.eudx-contest.com/wp-admin/admin-post.php?action=eudx_download_log&log_id=1&token=abc",
                ),
                (
                    "9A1A",
                    "https://www.eudx-contest.com/wp-admin/admin-post.php?action=eudx_download_log&log_id=2&token=def",
                ),
            ],
        )


if __name__ == "__main__":
    unittest.main()
