import importlib.util
import sys
import unittest
from pathlib import Path


def load_downloader_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "public_logs_downloader.py"
    spec = importlib.util.spec_from_file_location("public_logs_downloader_test", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


pld = load_downloader_module()


class HrsHfProviderTests(unittest.TestCase):
    def test_parse_qsos_supports_legacy_12_column_rows(self):
        html = """
        <TABLE>
          <TR>
            <TD>28479</TD><TD>PH</TD><TD>2024-12-21</TD><TD>1400</TD>
            <TD>S53M</TD><TD>59</TD><TD>0001</TD><TD>28</TD>
            <TD>K2GSJ</TD><TD>59</TD><TD>0000</TD><TD>08</TD>
          </TR>
        </TABLE>
        """

        qsos = pld.hrs_parse_qsos(html)

        self.assertEqual(len(qsos), 1)
        self.assertEqual(qsos[0].own_call, "S53M")
        self.assertEqual(qsos[0].sent_exchange, "0001 28")
        self.assertEqual(qsos[0].worked_call, "K2GSJ")
        self.assertEqual(qsos[0].received_exchange, "0000 08")

    def test_parse_qsos_supports_13_column_rows_with_prefix_column(self):
        html = """
        <TABLE>
          <TR>
            <TD>03559</TD><TD>CW</TD><TD>2026-01-10</TD><TD>1302</TD>
            <TD>1</TD><TD>9A0KG</TD><TD>599</TD><TD>0001</TD><TD>SD</TD>
            <TD>9A2ZH</TD><TD>599</TD><TD>0001</TD><TD>ZD</TD>
          </TR>
        </TABLE>
        """

        qsos = pld.hrs_parse_qsos(html)

        self.assertEqual(len(qsos), 1)
        self.assertEqual(qsos[0].own_call, "9A0KG")
        self.assertEqual(qsos[0].sent_rst, "599")
        self.assertEqual(qsos[0].sent_exchange, "0001 SD")
        self.assertEqual(qsos[0].worked_call, "9A2ZH")
        self.assertEqual(qsos[0].received_exchange, "0001 ZD")

    def test_parse_qsos_skips_rows_without_worked_call(self):
        html = """
        <TABLE>
          <TR>
            <TD>21236</TD><TD>PH</TD><TD>2024-12-22</TD><TD>0920</TD>
            <TD>DO1DPL</TD><TD>599</TD><TD>0016</TD><TD>28</TD>
            <TD></TD><TD></TD><TD>0000</TD><TD>00</TD>
          </TR>
        </TABLE>
        """

        self.assertEqual(pld.hrs_parse_qsos(html), [])

    def test_discover_call_infos_preserves_category_sections(self):
        html = """
        <P><b>A10 - vise operatora, Mixed</b></P>
        <TABLE><TR><TD>
          <A HREF="hfcc_log.php?What=vloghtm&amp;CID=2025-12-20&amp;ID=9A7V">htm</A>
        </TD></TR></TABLE>
        <P><b>B01 - Single Operator, All Bands, Mixed, High Power</b></P>
        <TABLE><TR><TD>
          <A HREF="hfcc_log.php?What=vloghtm&amp;CID=2025-12-20&amp;ID=S53M">htm</A>
        </TD></TR></TABLE>
        """
        round_info = pld.HRSRound(
            cid="2025-12-20",
            name="Croatian DX Contest 2025",
            slug="Croatian_DX_Contest",
            year=2025,
            status="Rezultati su sluzbeni",
            logs=2,
            uploaded_qsos=0,
            check_logs=0,
        )
        original_fetch_text = pld.fetch_text
        pld.fetch_text = lambda *_args, **_kwargs: html
        try:
            infos = pld.hrs_discover_call_infos(round_info)
        finally:
            pld.fetch_text = original_fetch_text

        by_call = {info.call: info.category_label for info in infos}
        self.assertEqual(by_call["9A7V"], "A10 - vise operatora, Mixed")
        self.assertEqual(by_call["S53M"], "B01 - Single Operator, All Bands, Mixed, High Power")
        self.assertFalse(next(info for info in infos if info.call == "S53M").checklog)

    def test_discover_call_infos_merges_checklog_view_links(self):
        round_html = """
        <P><b>E - vise operatora, CW i SSB</b></P>
        <A HREF="hfcc_log.php?What=vloghtm&amp;CID=2026-01-10&amp;ID=9A0KG">htm</A>
        """
        check_html = """
        <TABLE><TR><TD>
          <A HREF="hfcc_log.php?What=vloghtmoc&amp;CID=2026-01-10&amp;ID=9A1CBM">view</A>
        </TD></TR></TABLE>
        """
        round_info = pld.HRSRound(
            cid="2026-01-10",
            name="ZIMSKI KV KUP - 9A5K Memorijal 2026",
            slug="Zimski_KV_Kup",
            year=2026,
            status="Rezultati su sluzbeni",
            logs=1,
            uploaded_qsos=0,
            check_logs=1,
        )
        original_fetch_text = pld.fetch_text
        pld.fetch_text = lambda url, *_args, **_kwargs: check_html if "view_hf_check.php" in url else round_html
        try:
            infos = pld.hrs_discover_call_infos(round_info)
        finally:
            pld.fetch_text = original_fetch_text

        by_call = {info.call: info for info in infos}
        self.assertEqual(by_call["9A0KG"].log_view, "vloghtm")
        self.assertFalse(by_call["9A0KG"].checklog)
        self.assertEqual(by_call["9A1CBM"].log_view, "vloghtmoc")
        self.assertTrue(by_call["9A1CBM"].checklog)

    def test_category_fields_derive_operator_band_mode_and_power(self):
        meta = pld.HRSLogMeta(
            call="S53M",
            contest="9A-DX",
            category="B01",
            category_label="B01 - Single Operator, All Bands, Mixed, High Power",
            claimed_qsos=2,
            claimed_score=100,
            checklog=False,
        )
        qsos = [
            pld.HRSQSO("28032", "CW", "2025-12-20", "1402", "S53M", "599", "0001 28", "EA8BW", "599", "0000 36"),
            pld.HRSQSO("7008", "PH", "2025-12-20", "1410", "S53M", "59", "0002 28", "9A1A", "59", "0002 28"),
        ]

        fields = pld.hrs_category_fields(meta, qsos)

        self.assertEqual(fields["operator"], "SINGLE-OP")
        self.assertEqual(fields["band"], "ALL")
        self.assertEqual(fields["mode"], "MIXED")
        self.assertEqual(fields["power"], "HIGH")

    def test_build_cabrillo_uses_category_code_and_keeps_label_as_soapbox(self):
        round_info = pld.HRSRound(
            cid="2026-01-10",
            name="ZIMSKI KV KUP - 9A5K Memorijal 2026",
            slug="Zimski_KV_Kup",
            year=2026,
            status="Rezultati su sluzbeni",
            logs=1,
            uploaded_qsos=1,
            check_logs=0,
        )
        meta = pld.HRSLogMeta(
            call="9A0KG",
            contest="HRV-ZIMSKI-KUP",
            category="E",
            category_label="E - više operatora, CW i SSB",
            claimed_qsos=1,
            claimed_score=22685,
            checklog=False,
        )
        qso = pld.HRSQSO("03559", "CW", "2026-01-10", "1302", "9A0KG", "599", "0001 SD", "9A2ZH", "599", "0001 ZD")

        cabrillo = pld.hrs_build_cabrillo(round_info, meta, [qso])

        self.assertIn("CONTEST: HRV-ZIMSKI-KUP", cabrillo)
        self.assertIn("CALLSIGN: 9A0KG", cabrillo)
        self.assertIn("CATEGORY: E", cabrillo)
        self.assertIn("CATEGORY-OPERATOR: MULTI-OP", cabrillo)
        self.assertIn("CATEGORY-BAND: 80M", cabrillo)
        self.assertIn("CATEGORY-MODE: CW", cabrillo)
        self.assertIn("CLAIMED-SCORE: 22685", cabrillo)
        self.assertIn("SOAPBOX: Source category label: E - vise operatora, CW i SSB.", cabrillo)
        self.assertNotIn("Parsed QSO count", cabrillo)
        self.assertIn("QSO: 03559 CW 2026-01-10 1302 9A0KG", cabrillo)

    def test_build_cabrillo_records_count_mismatch_as_soapbox(self):
        round_info = pld.HRSRound(
            cid="2024-12-21",
            name="Hrvatski Radioamaterski Kup 2024",
            slug="Hrvatski_Radioamaterski_Kup",
            year=2024,
            status="Rezultati su sluzbeni",
            logs=1,
            uploaded_qsos=16,
            check_logs=0,
        )
        meta = pld.HRSLogMeta(
            call="DO1DPL",
            contest="HRV-KUP",
            category="B",
            category_label="",
            claimed_qsos=16,
            claimed_score=0,
            checklog=False,
        )
        qso = pld.HRSQSO("21230", "PH", "2024-12-22", "0748", "DO1DPL", "599", "0001 28", "UP0L", "599", "0000 30")

        cabrillo = pld.hrs_build_cabrillo(round_info, meta, [qso])

        self.assertIn("SOAPBOX: Published QSO count: 16.", cabrillo)
        self.assertIn("SOAPBOX: Parsed QSO count: 1.", cabrillo)


if __name__ == "__main__":
    unittest.main()
