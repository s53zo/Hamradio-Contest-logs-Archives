import importlib.util
import sqlite3
import sys
import tempfile
import unittest
from datetime import date
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

    def test_replace_marked_section_preserves_surrounding_content(self):
        text = "before\n<!-- X:START -->\nold\n<!-- X:END -->\nafter\n"

        updated = pld.replace_marked_section(
            text,
            "<!-- X:START -->",
            "<!-- X:END -->",
            "<!-- X:START -->\nnew\n<!-- X:END -->",
        )

        self.assertEqual(updated, "before\n<!-- X:START -->\nnew\n<!-- X:END -->\nafter\n")

    def test_replace_marked_section_rejects_duplicate_markers(self):
        text = "<!-- X:START -->\none\n<!-- X:END -->\n<!-- X:START -->\ntwo\n<!-- X:END -->\n"

        with self.assertRaises(ValueError):
            pld.replace_marked_section(
                text,
                "<!-- X:START -->",
                "<!-- X:END -->",
                "<!-- X:START -->\nnew\n<!-- X:END -->",
            )

    def test_readme_stats_aggregate_source_and_reconstructed_logs(self):
        with tempfile.TemporaryDirectory() as tmp:
            shard_dir = Path(tmp) / "SH6"
            shard_dir.mkdir()
            shard = shard_dir / "logs_00.sqlite"
            conn = sqlite3.connect(shard)
            try:
                conn.execute(
                    """
                    CREATE TABLE logs (
                        path TEXT,
                        callsign TEXT,
                        contest TEXT,
                        year INTEGER,
                        mode TEXT,
                        season TEXT,
                        subcontest TEXT,
                        detail TEXT
                    )
                    """
                )
                conn.executemany(
                    """
                    INSERT INTO logs (path, callsign, contest, year, mode, season, subcontest, detail)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        ("CQWW/cw/2025/S53M.log", "S53M", "CQWW", 2025, "CW", "", "", "cw"),
                        ("CQWW/ssb/2024/S53M.log", "S53M", "CQWW", 2024, "PH", "", "", "ssb"),
                        ("TTC-SPCWC/2026-06-23/SM0OEK.log", "SM0OEK", "TTC-SPCWC", 2026, "CW", "", "", ""),
                        (
                            "RECONSTRUCTED_LOGS/CQWW/cw/2025/TEST.log",
                            "TEST",
                            "CQWW",
                            2025,
                            "CW",
                            "",
                            "",
                            "cw",
                        ),
                    ],
                )
                conn.commit()
            finally:
                conn.close()

            stats = pld.collect_readme_stats(shard_dir)
            stats_text = pld.render_readme_stats(stats, today=date(2026, 7, 1))
            years_text = pld.render_readme_years_table(stats)

        self.assertEqual(stats.total_logs, 4)
        self.assertEqual(stats.source_logs, 3)
        self.assertEqual(stats.reconstructed_logs, 1)
        self.assertEqual(stats.source_callsign_count, 2)
        self.assertEqual(stats.contest_root_count, 2)
        self.assertIn("- source/public indexed log files: 3", stats_text)
        self.assertIn("- reconstructed mock log files in `RECONSTRUCTED_LOGS/`: 1", stats_text)
        self.assertIn("| CQWW | 2024, 2025 | 3 |", years_text)
        self.assertIn("| TTC-SPCWC | 2026 | 1 |", years_text)

    def test_readme_stats_aggregate_multiple_shards_and_ignore_non_integer_years(self):
        with tempfile.TemporaryDirectory() as tmp:
            shard_dir = Path(tmp) / "SH6"
            shard_dir.mkdir()
            for idx, rows in enumerate(
                [
                    [
                        ("CQWW/cw/2025/S53M.log", "S53M", "CQWW", 2025, "CW", "", "", "cw"),
                        ("CQWW/cw/bad/S54M.log", "S54M", "CQWW", "bad", "CW", "", "", "cw"),
                    ],
                    [
                        ("ARRL/arrl_10_meter_contest/2026/K1ABC.log", "K1ABC", "ARRL", 2026, "MIXED", "", "arrl_10_meter_contest", ""),
                        ("RECONSTRUCTED_LOGS/ARRL/arrl_10_meter_contest/2026/K2ABC.log", "K2ABC", "ARRL", 2026, "MIXED", "", "arrl_10_meter_contest", ""),
                    ],
                ]
            ):
                conn = sqlite3.connect(shard_dir / f"logs_{idx:02x}.sqlite")
                try:
                    conn.execute(
                        """
                        CREATE TABLE logs (
                            path TEXT,
                            callsign TEXT,
                            contest TEXT,
                            year,
                            mode TEXT,
                            season TEXT,
                            subcontest TEXT,
                            detail TEXT
                        )
                        """
                    )
                    conn.executemany(
                        """
                        INSERT INTO logs (path, callsign, contest, year, mode, season, subcontest, detail)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows,
                    )
                    conn.commit()
                finally:
                    conn.close()

            stats = pld.collect_readme_stats(shard_dir)
            years_text = pld.render_readme_years_table(stats)

        self.assertEqual(stats.shard_count, 2)
        self.assertEqual(stats.total_logs, 4)
        self.assertEqual(stats.source_logs, 3)
        self.assertEqual(stats.reconstructed_logs, 1)
        self.assertEqual(stats.source_callsign_count, 3)
        self.assertIn("| ARRL | 2026 | 2 |", years_text)
        self.assertIn("| CQWW | 2025 | 2 |", years_text)

    def test_readme_years_table_includes_reconstructed_only_years(self):
        with tempfile.TemporaryDirectory() as tmp:
            shard_dir = Path(tmp) / "SH6"
            shard_dir.mkdir()
            conn = sqlite3.connect(shard_dir / "logs_00.sqlite")
            try:
                conn.execute(
                    """
                    CREATE TABLE logs (
                        path TEXT,
                        callsign TEXT,
                        contest TEXT,
                        year INTEGER,
                        mode TEXT,
                        season TEXT,
                        subcontest TEXT,
                        detail TEXT
                    )
                    """
                )
                conn.executemany(
                    """
                    INSERT INTO logs (path, callsign, contest, year, mode, season, subcontest, detail)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        ("RussianDXContest/2026/RA1ABC.log", "RA1ABC", "RussianDXContest", 2026, "MIXED", "", "", ""),
                        (
                            "RECONSTRUCTED_LOGS/RussianDXContest/2020/RW1ABC.log",
                            "RW1ABC",
                            "RussianDXContest",
                            2020,
                            "MIXED",
                            "",
                            "",
                            "",
                        ),
                        (
                            "RECONSTRUCTED_LOGS/RussianDXContest/2025/RW2ABC.log",
                            "RW2ABC",
                            "RussianDXContest",
                            2025,
                            "MIXED",
                            "",
                            "",
                            "",
                        ),
                    ],
                )
                conn.commit()
            finally:
                conn.close()

            stats = pld.collect_readme_stats(shard_dir)
            years_text = pld.render_readme_years_table(stats)

        self.assertIn("| RussianDXContest | 2020, 2025, 2026 | 3 |", years_text)

    def test_update_readme_from_shards_rewrites_only_marked_sections(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            shard_dir = repo / "SH6"
            shard_dir.mkdir()
            (repo / "README.md").write_text(
                "\n".join(
                    [
                        "top",
                        pld.README_STATS_START,
                        "old stats",
                        pld.README_STATS_END,
                        "middle",
                        pld.README_YEARS_START,
                        "old years",
                        pld.README_YEARS_END,
                        "bottom",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            conn = sqlite3.connect(shard_dir / "logs_00.sqlite")
            try:
                conn.execute(
                    """
                    CREATE TABLE logs (
                        path TEXT,
                        callsign TEXT,
                        contest TEXT,
                        year INTEGER,
                        mode TEXT,
                        season TEXT,
                        subcontest TEXT,
                        detail TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO logs (path, callsign, contest, year, mode, season, subcontest, detail)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("ARRL/arrl_10_meter_contest/2026/S53M.log", "S53M", "ARRL", 2026, "MIXED", "", "arrl_10_meter_contest", ""),
                )
                conn.commit()
            finally:
                conn.close()

            pld.update_readme_from_shards(repo, shard_dir, today=date(2026, 7, 1))
            text = (repo / "README.md").read_text(encoding="utf-8")

        self.assertTrue(text.startswith("top\n"))
        self.assertIn("middle\n", text)
        self.assertTrue(text.endswith("bottom\n"))
        self.assertIn("- total indexed log files: 1", text)
        self.assertIn("| ARRL | 2026 | 1 |", text)


if __name__ == "__main__":
    unittest.main()
