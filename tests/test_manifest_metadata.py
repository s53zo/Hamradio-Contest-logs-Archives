import importlib.util
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


def load_downloader_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "public_logs_downloader.py"
    scripts_dir = module_path.parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    spec = importlib.util.spec_from_file_location("public_logs_downloader_manifest_test", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


pld = load_downloader_module()


class ManifestMetadataTests(unittest.TestCase):
    def record(self, path):
        return pld.manifest_record_from_path(Path(path))

    def test_subcontest_roots_are_inferred_before_year(self):
        record = self.record("9A_HRS_Contest/Zimski_KV_Kup/2026/9A5M.log")

        self.assertEqual(record["contest"], "9A_HRS_Contest")
        self.assertEqual(record["subcontest"], "Zimski_KV_Kup")
        self.assertEqual(record["contest_slug"], "Zimski_KV_Kup")
        self.assertEqual(record["year"], 2026)
        self.assertNotIn("detail", record)

    def test_mode_before_year_is_detail_not_subcontest(self):
        record = self.record("CQWW/cw/2025/S53M.log")

        self.assertEqual(record["contest"], "CQWW")
        self.assertEqual(record["detail"], "cw")
        self.assertEqual(record["mode"], "CW")
        self.assertNotIn("subcontest", record)

    def test_darc_monthly_edition_preserves_subcontest_and_detail(self):
        record = self.record("DARC/RTTY_Kurzcontest/2026/jan/S53M.log")

        self.assertEqual(record["contest"], "DARC")
        self.assertEqual(record["subcontest"], "RTTY_Kurzcontest")
        self.assertEqual(record["detail"], "jan")
        self.assertEqual(record["month"], 1)

    def test_darc_fieldday_preserves_mode_folder_as_detail(self):
        record = self.record("DARC/Fieldday/CW/2024/S53M.log")

        self.assertEqual(record["subcontest"], "Fieldday")
        self.assertEqual(record["detail"], "CW")
        self.assertEqual(record["mode"], "CW")

    def test_reconstructed_prefix_is_ignored_for_hierarchy(self):
        record = self.record("RECONSTRUCTED_LOGS/ARRL/arrl_10_meter_contest/2025/S53M.log")

        self.assertEqual(record["contest"], "ARRL")
        self.assertEqual(record["subcontest"], "arrl_10_meter_contest")
        self.assertEqual(record["year"], 2025)

    def test_event_folder_with_embedded_year_is_subcontest(self):
        record = self.record("EU_VHF_CONTESTS/ZRS_September_2025/144MHz/S53M.log")

        self.assertEqual(record["contest"], "EU_VHF_CONTESTS")
        self.assertEqual(record["subcontest"], "ZRS_September_2025")
        self.assertEqual(record["detail"], "144MHz")
        self.assertEqual(record["year"], 2025)
        self.assertEqual(record["band"], "144MHz")

    def test_sqlite_shards_store_subcontest_and_detail_columns(self):
        original_roots = pld.MANIFEST_ROOTS
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            log_path = repo / "DARC" / "RTTY_Kurzcontest" / "2026" / "jan" / "S53M.log"
            log_path.parent.mkdir(parents=True)
            log_path.write_text("START-OF-LOG: 3.0\nCALLSIGN: S53M\nEND-OF-LOG:\n", encoding="utf-8")
            pld.MANIFEST_ROOTS = {"DARC"}
            try:
                count = pld.build_sqlite_shards(repo, repo / "SH6", progress_every=0)
            finally:
                pld.MANIFEST_ROOTS = original_roots

            self.assertEqual(count, 1)
            shard = repo / "SH6" / f"logs_{pld.callsign_bucket('S53M'):02x}.sqlite"
            conn = sqlite3.connect(shard)
            try:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(logs)")}
                row = conn.execute(
                    "SELECT path, contest, year, mode, season, subcontest, detail FROM logs WHERE callsign = ?",
                    ("S53M",),
                ).fetchone()
            finally:
                conn.close()

            self.assertIn("subcontest", columns)
            self.assertIn("detail", columns)
            self.assertEqual(row, ("DARC/RTTY_Kurzcontest/2026/jan/S53M.log", "DARC", 2026, "RTTY", "Winter", "RTTY_Kurzcontest", "jan"))


if __name__ == "__main__":
    unittest.main()
