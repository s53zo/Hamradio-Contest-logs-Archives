import unittest
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import download_wae_logs as wae  # noqa: E402


class WaeProviderTests(unittest.TestCase):
    def test_loglist_contest_year_ignores_newer_framework_timestamp(self):
        page = """
        <h1>Worked All Europe DX Contest (WAEDC) RTTY 2025 Final scores</h1>
        <footer>framework generated: Thu May 7 07:01:00 2026</footer>
        """

        assert wae.loglist_contest_year(page) == 2025


    def test_loglist_contest_year_accepts_classified_logs(self):
        page = "<h1>Worked All Europe DX Contest (WAEDC) CW 2026 Classified logs</h1>"

        assert wae.loglist_contest_year(page) == 2026


    def test_discover_years_excludes_unpublished_placeholder_year(self):
        open_log_form = """
        <select>
          <option value="2026">2026</option>
          <option value="2025">2025</option>
          <option value="2024">2024</option>
        </select>
        """
        results = """
        <h1>Worked All Europe DX Contest (WAEDC) RTTY 2025 Final scores</h1>
        <footer>framework generated in 2026</footer>
        """
        original_fetch_text = wae.fetch_text
        wae.fetch_text = lambda url, **_kwargs: results if "fc=loglist" in url else open_log_form
        try:
            assert wae.discover_years("waerttylog") == [2025, 2024]
        finally:
            wae.fetch_text = original_fetch_text


    def test_future_loglist_does_not_add_year_missing_from_public_log_form(self):
        form = '<select><option value="2025">2025</option><option value="2024">2024</option></select>'
        placeholder = '<h1>Worked All Europe DX Contest (WAEDC) RTTY 2026 Classified logs</h1>'
        original = wae.fetch_text
        wae.fetch_text = lambda url, **kwargs: placeholder if 'fc=loglist' in url else form
        try:
            assert wae.discover_years('waerttylog') == [2025, 2024]
        finally:
            wae.fetch_text = original
