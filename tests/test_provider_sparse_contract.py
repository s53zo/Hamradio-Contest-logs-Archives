import ast
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import public_logs_downloader as public  # noqa: E402


STANDALONE_PROVIDER_MODULES = {
    "download_arrl_logs.py",
    "download_cq160_logs.py",
    "download_cqwpx_logs.py",
    "download_cqwpxrtty_logs.py",
    "download_cqww_logs.py",
    "download_cqwwrtty_logs.py",
    "download_darc_logs.py",
    "download_eudx_logs.py",
    "download_euhf_logs.py",
    "download_ham_spirit_contest_ubn.py",
    "download_istra_open_logs.py",
    "download_okomdx_logs.py",
    "download_rcc_cup_ubn.py",
    "download_rda_contest_ubn.py",
    "download_ref_logs.py",
    "download_rf_championship_cw_ubn.py",
    "download_russian_dx_contest_ubn.py",
    "download_russian_radio_team_championship_ubn.py",
    "download_spdx_logs.py",
    "download_ttc_spcwc_logs.py",
    "download_vhfmanager_logs.py",
    "download_wae_logs.py",
    "download_wednesday_minitest_40m_ubn.py",
    "download_wednesday_minitest_80m_ubn.py",
    "download_wwdigi_logs.py",
    "download_yuri_gagarin_dx_contest_ubn.py",
    "download_zrs_kvp_logs.py",
}
INTEGRATED_PROVIDER_ADAPTERS = {"download_yota_contest_logs.py"}
UA9QCQ_PROVIDER_MODULES = {
    "download_ham_spirit_contest_ubn.py",
    "download_rcc_cup_ubn.py",
    "download_rda_contest_ubn.py",
    "download_rf_championship_cw_ubn.py",
    "download_russian_dx_contest_ubn.py",
    "download_russian_radio_team_championship_ubn.py",
    "download_wednesday_minitest_40m_ubn.py",
    "download_wednesday_minitest_80m_ubn.py",
    "download_yuri_gagarin_dx_contest_ubn.py",
}


class ProviderSparseContractTests(unittest.TestCase):
    def test_every_download_module_has_an_explicit_runtime_contract(self) -> None:
        discovered = {path.name for path in SCRIPTS.glob("download_*.py")}
        self.assertEqual(
            discovered,
            STANDALONE_PROVIDER_MODULES | INTEGRATED_PROVIDER_ADAPTERS,
        )

    def test_every_standalone_provider_uses_remote_inventory_and_atomic_writes(self) -> None:
        discovered = {
            path.name
            for path in SCRIPTS.glob("download_*.py")
            if "archive_storage" in path.read_text(encoding="utf-8")
        }
        self.assertEqual(discovered, STANDALONE_PROVIDER_MODULES)

        for name in sorted(STANDALONE_PROVIDER_MODULES):
            tree = ast.parse((SCRIPTS / name).read_text(encoding="utf-8"), filename=name)
            calls = {
                node.func.id
                for node in ast.walk(tree)
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            }
            with self.subTest(provider=name):
                self.assertIn("archive_log_exists", calls)
                self.assertTrue(
                    {"atomic_write_text", "atomic_write_bytes"} & calls,
                    "provider must not stream directly into a final log path",
                )

    def test_integrated_yota_adapter_writes_only_through_public_downloader(self) -> None:
        adapter = (SCRIPTS / "download_yota_contest_logs.py").read_text(encoding="utf-8")
        public_source = (SCRIPTS / "public_logs_downloader.py").read_text(encoding="utf-8")
        self.assertNotIn("if __name__ ==", adapter)
        self.assertIn("import download_yota_contest_logs as yota", public_source)
        self.assertIn("atomic_write_text(dest, cabrillo)", public_source)

    def test_every_ua9qcq_cookie_prompt_is_hidden(self) -> None:
        for name in sorted(UA9QCQ_PROVIDER_MODULES):
            source = (SCRIPTS / name).read_text(encoding="utf-8")
            with self.subTest(provider=name):
                self.assertIn("getpass.getpass", source)
                self.assertNotIn('input("UA9QCQ session cookie', source)

    def test_registry_filter_keeps_remote_only_logs_out_of_download_queue(self) -> None:
        task = public.DownloadTask(
            dest=Path("Contest/2026/S53ZO.log"),
            host="example.invalid",
            source="fixture",
            action=lambda: {"ok": 1},
        )

        with mock.patch.object(public, "valid_existing_log", return_value=True):
            missing, existing = public.filter_missing_tasks([task])

        self.assertEqual(missing, [])
        self.assertEqual(existing, 1)

    def test_all_registered_providers_use_the_shared_filter_contract(self) -> None:
        expected_ids = set(range(1, 16)) | set(range(17, 25)) | set(range(26, 36))
        self.assertEqual(set(public.PROVIDERS), expected_ids)
        for provider_id, (name, adapter) in public.PROVIDERS.items():
            with self.subTest(provider=provider_id):
                self.assertTrue(name)
                self.assertTrue(callable(adapter))


if __name__ == "__main__":
    unittest.main()
