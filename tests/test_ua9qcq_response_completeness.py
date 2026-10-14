import importlib
import socket
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))

MODULES = (
    'download_ham_spirit_contest_ubn',
    'download_rcc_cup_ubn',
    'download_rda_contest_ubn',
    'download_rf_championship_cw_ubn',
    'download_russian_dx_contest_ubn',
    'download_russian_radio_team_championship_ubn',
    'download_wednesday_minitest_40m_ubn',
    'download_wednesday_minitest_80m_ubn',
    'download_yuri_gagarin_dx_contest_ubn',
)


class UA9QCQResponseCompletenessTests(unittest.TestCase):
    def test_worldwide_filter_matches_current_result_form(self):
        for name in MODULES:
            with self.subTest(provider=name):
                module = importlib.import_module(name)
                self.assertEqual(module.results_post_data('2026', '0')['terr_id'], '3')

    def test_streamed_html_end_marker_across_chunks(self):
        for name in MODULES:
            with self.subTest(provider=name):
                module = importlib.import_module(name)
                response = mock.Mock()
                response.read1.side_effect = [b'<html>results</ht', b'ml>']
                response.headers.get_content_charset.return_value = 'utf-8'
                with mock.patch.object(module, 'set_response_socket_timeout'):
                    self.assertEqual(module.read_response_text(response), '<html>results</html>')

    def test_eof_without_html_end_is_rejected(self):
        for name in MODULES:
            with self.subTest(provider=name):
                module = importlib.import_module(name)
                response = mock.Mock()
                response.read1.side_effect = [b'<html>partial results', b'']
                with mock.patch.object(module, 'set_response_socket_timeout'):
                    with self.assertRaisesRegex(ValueError, 'incomplete HTML'):
                        module.read_response_text(response)

    def test_timeout_does_not_retry_poisoned_response(self):
        for name in MODULES:
            with self.subTest(provider=name):
                module = importlib.import_module(name)
                response = mock.Mock()
                response.read1.side_effect = socket.timeout()
                with mock.patch.object(module, 'set_response_socket_timeout'):
                    with self.assertRaisesRegex(TimeoutError, 'stalled'):
                        module.read_response_text(response)
                response.read1.assert_called_once()


if __name__ == '__main__':
    unittest.main()
