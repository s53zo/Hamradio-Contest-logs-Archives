import sys
from pathlib import Path
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import download_okomdx_logs as okom


class OKOMDXPeriodSelectionTests(unittest.TestCase):
    def test_shared_rounds_include_ok_and_rtty_without_accepting_future_placeholders(self):
        titles = {10: 'OK DX RTTY Contest 2026', 9: 'OK SSB contest 2026',
                  8: 'OK CW contest 2026', 7: 'OK-OM DX Contest 2026',
                  6: 'OK-OM DX Contest 2026', 5: 'OK DX RTTY Contest 2025',
                  4: 'OK-OM DX Contest 2025', 3: '1970', 2: 'OK CW contest 2024'}

        def fetch(url):
            if 'id_round=' in url:
                rid = int(url.rsplit('id_round=', 1)[1])
                return '<h2>' + titles[rid] + '</h2>' + ('str=log' if rid not in (10, 7, 3) else '')
            if 'page=2026-2' in url:
                return '<iframe src="eval/index.php?str=results&amp;id_round=10"></iframe>'
            return '<a href="index.php?page=2026-2">2026</a>'

        with mock.patch.object(okom, 'fetch_text', side_effect=fetch):
            rounds = okom.discover_shared_rounds(1)
        self.assertEqual([item.id_round for item in rounds], [9, 8, 6, 5, 4])

    def test_latest_year_keeps_all_its_pages_and_excludes_every_older_page(self):
        pages = [(2025, '2025-1'), (2026, '2026-1'), (2025, '2025-2'),
                 (2026, '2026-2'), (2024, '2024-3'), (2024, '2024-1')]
        with mock.patch.object(okom, 'discover_year_pages', return_value=pages):
            self.assertEqual(okom.iter_year_pages(1), [(2026, '2026-2'), (2026, '2026-1')])


if __name__ == '__main__':
    unittest.main()
