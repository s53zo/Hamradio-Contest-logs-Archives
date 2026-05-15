import importlib.util
import sys
import unittest
from pathlib import Path


def load_reconstruct_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "reconstruct_missing_logs.py"
    scripts_dir = module_path.parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    spec = importlib.util.spec_from_file_location("reconstruct_missing_logs_test", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


reconstruct = load_reconstruct_module()


class ReconstructMissingLogsTests(unittest.TestCase):
    def test_parse_qso_line_drops_trailing_cqww_status_flag(self):
        qso = reconstruct.parse_qso_line(
            "QSO:  3548 CW 2008-11-30 0708 LN3Z          599 14     KD0S          599 04     0"
        )

        self.assertIsNotNone(qso)
        self.assertEqual(qso.mycall, "LN3Z")
        self.assertEqual(qso.sent_exch, ["599", "14"])
        self.assertEqual(qso.theircall, "KD0S")
        self.assertEqual(qso.recv_exch, ["599", "04"])

    def test_parse_qso_line_keeps_normal_two_token_exchange(self):
        qso = reconstruct.parse_qso_line(
            "QSO: 14000 PH 2026-01-01 1200 S53M 59 15 K1ABC 59 05"
        )

        self.assertIsNotNone(qso)
        self.assertEqual(qso.recv_exch, ["59", "05"])

    def test_parse_qso_line_drops_trailing_status_from_sent_exchange(self):
        qso = reconstruct.parse_qso_line(
            "QSO:  7025 RY 2009-09-26 0107 2E0ZWW        599 14 DX  RX4HZ         599 16 DX  0"
        )

        self.assertIsNotNone(qso)
        self.assertEqual(qso.sent_exch, ["599", "14", "DX"])
        self.assertEqual(qso.recv_exch, ["599", "16", "DX"])

    def test_parse_qso_line_drops_multiple_trailing_status_tokens(self):
        qso = reconstruct.parse_qso_line(
            "QSO:  3500 CW 2013-05-25 1129 K5CAO         599 078    JK2EIJ/0      599 0215 0 0"
        )

        self.assertIsNotNone(qso)
        self.assertEqual(qso.sent_exch, ["599", "078"])
        self.assertEqual(qso.recv_exch, ["599", "0215"])

    def test_parse_qso_line_rejects_exchange_token_as_callsign_in_darc_style_line(self):
        qso = reconstruct.parse_qso_line(
            "QSO: 14015 CW 2018-10-21 0636 DK1A          599 Y07 R4LR          599 471"
        )

        self.assertIsNotNone(qso)
        self.assertEqual(qso.theircall, "R4LR")
        self.assertEqual(qso.sent_exch, ["599", "Y07"])
        self.assertEqual(qso.recv_exch, ["599", "471"])

    def test_parse_qso_line_rejects_suffixless_exchange_tokens(self):
        qso = reconstruct.parse_qso_line(
            "QSO: 28022 CW 2025-02-01 1747 EA8/OH2KW     599 ES09 W2XL          599 8"
        )

        self.assertIsNotNone(qso)
        self.assertEqual(qso.theircall, "W2XL")
        self.assertEqual(qso.sent_exch, ["599", "ES09"])
        self.assertEqual(qso.recv_exch, ["599", "8"])

    def test_looks_like_callsign_rejects_exchange_like_tokens_without_suffix_letters(self):
        self.assertFalse(reconstruct.looks_like_callsign("Y07"))
        self.assertFalse(reconstruct.looks_like_callsign("ES09"))
        self.assertFalse(reconstruct.looks_like_callsign("UL14"))
        self.assertFalse(reconstruct.looks_like_callsign("RCC175"))
        self.assertTrue(reconstruct.looks_like_callsign("R4LR"))
        self.assertTrue(reconstruct.looks_like_callsign("D1M"))
        self.assertTrue(reconstruct.looks_like_callsign("K1A"))
        self.assertTrue(reconstruct.looks_like_callsign("WX8C/0"))


if __name__ == "__main__":
    unittest.main()
