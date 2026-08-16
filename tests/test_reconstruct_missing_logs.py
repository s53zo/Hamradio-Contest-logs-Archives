import importlib.util
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
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
    def write_source_log(self, contest_dir, qso_count=12):
        contest_dir.mkdir(parents=True)
        lines = ["START-OF-LOG: 3.0", "CALLSIGN: S53M", "CONTEST: TEST"]
        for idx in range(qso_count):
            lines.append(
                f"QSO: 14000 CW 2026-01-01 12{idx:02d} "
                f"S53M 599 001 K1ABC 599 {idx + 1:03d}"
            )
        lines.append("END-OF-LOG:")
        (contest_dir / "S53M.log").write_text("\n".join(lines) + "\n", encoding="utf-8")

    def cache_key(self, contest_dir, **overrides):
        values = {
            "stats": reconstruct.collect_contest_stats(contest_dir),
            "master_hash": "same-master",
            "min_qsos": 10,
            "limit": None,
            "use_ledger": False,
            "created_by": "test",
            "contest_name": "TEST",
            "season_label": "Contest/2026",
        }
        values.update(overrides)
        return reconstruct.reconstruction_cache_key(**values)

    def init_git_repo(self, repo):
        subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=repo, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)

    def test_download_master_dta_retries_timeout_and_cleans_up(self):
        payload = b"K1ABC\nS53M\n"
        calls = []
        original_urlopen = reconstruct.urllib.request.urlopen

        class FakeResponse:
            def __init__(self):
                self.remaining = payload

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _size):
                chunk, self.remaining = self.remaining, b""
                return chunk

        def fake_urlopen(request, timeout):
            calls.append((request, timeout))
            if len(calls) == 1:
                raise TimeoutError("slow origin")
            return FakeResponse()

        reconstruct.urllib.request.urlopen = fake_urlopen
        try:
            path = reconstruct.download_master_dta(
                "https://example.test/MASTER.DTA",
                retries=2,
                timeout=45,
                delay=0,
            )
            try:
                self.assertEqual(path.read_bytes(), payload)
            finally:
                path.unlink()
        finally:
            reconstruct.urllib.request.urlopen = original_urlopen

        self.assertEqual(len(calls), 2)
        self.assertEqual([timeout for _request, timeout in calls], [45, 45])
        self.assertEqual(
            calls[0][0].get_header("User-agent"),
            "Hamradio-Contest-logs-Archives/1.0",
        )

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

    def test_reconstruct_cache_includes_master_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            contest_dir = repo / "Contest" / "2026"
            out_dir = repo / "RECONSTRUCTED_LOGS" / "Contest" / "2026"
            ledger_root = repo / "ledgers"
            self.write_source_log(contest_dir)
            stale_key = self.cache_key(
                contest_dir,
                master_hash="old-master",
            )
            state_path = reconstruct.state_path_for(out_dir, repo, ledger_root)
            reconstruct.save_state(
                state_path,
                {
                    "schema_version": reconstruct.STATE_SCHEMA_VERSION,
                    "cache_key": stale_key,
                    "submitted_logs": 1,
                    "parsed_qsos": 12,
                    "reconstructed_logs": 0,
                    "skipped_existing": 0,
                    "output_logs": 0,
                },
            )

            result = reconstruct.reconstruct_contest(
                contest_dir=contest_dir,
                out_dir=out_dir,
                master_calls={"K1ABC"},
                master_hash="new-master",
                min_qsos=10,
                created_by="test",
                contest_name=None,
                season_label=None,
                dry_run=True,
                limit=None,
                repo_root=repo,
                ledger_root=ledger_root,
                ledger_name=".reconstructed_ledger.txt",
                use_ledger=False,
                skip_unchanged=True,
            )

        self.assertEqual(result.skipped_unchanged, 0)
        self.assertEqual(result.reconstructed_logs, 1)
        self.assertEqual(result.cached_reconstructed_logs, 0)

    def test_source_fingerprint_does_not_depend_on_materialization_mtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            contest_dir = Path(tmp) / "Contest" / "2026"
            self.write_source_log(contest_dir)
            before = reconstruct.collect_contest_stats(contest_dir)
            source = contest_dir / "S53M.log"
            os.utime(source, (source.stat().st_atime + 100, source.stat().st_mtime + 100))

            after = reconstruct.collect_contest_stats(contest_dir)

        self.assertEqual(after, before)

    def test_remote_reconstructed_output_is_skipped_when_absent_from_worktree(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            contest_dir = repo / "Contest" / "2026"
            out_dir = repo / "RECONSTRUCTED_LOGS" / "Contest" / "2026"
            ledger_root = repo / "state" / "reconstruction" / "ledgers"
            self.write_source_log(contest_dir)
            out_dir.mkdir(parents=True)
            output = out_dir / "K1ABC.log"
            output.write_text("START-OF-LOG: 3.0\nEND-OF-LOG:\n", encoding="ascii")
            self.init_git_repo(repo)
            subprocess.run(["git", "add", "Contest", "RECONSTRUCTED_LOGS"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-qm", "fixture"], cwd=repo, check=True)

            rel_output = output.relative_to(repo)
            bucket = reconstruct.ArchiveInventory(repo).shard_path(rel_output)
            bucket.parent.mkdir()
            with closing(sqlite3.connect(bucket)) as conn:
                with conn:
                    conn.execute(
                        "CREATE TABLE logs(path TEXT, callsign TEXT, contest TEXT, year INTEGER, mode TEXT, season TEXT, subcontest TEXT, detail TEXT)"
                    )
                    conn.execute(
                        "INSERT INTO logs(path, callsign) VALUES (?, ?)",
                        (rel_output.as_posix(), "K1ABC"),
                    )
            subprocess.run(
                ["git", "update-index", "--skip-worktree", rel_output.as_posix()],
                cwd=repo,
                check=True,
            )
            output.unlink()

            result = reconstruct.reconstruct_contest(
                contest_dir=contest_dir,
                out_dir=out_dir,
                master_calls={"K1ABC"},
                master_hash="master",
                min_qsos=10,
                created_by="test",
                contest_name=None,
                season_label=None,
                dry_run=False,
                limit=None,
                repo_root=repo,
                ledger_root=ledger_root,
                ledger_name=".reconstructed_ledger.txt",
                use_ledger=False,
                skip_unchanged=False,
            )

        self.assertEqual(result.reconstructed_logs, 0)
        self.assertEqual(result.skipped_existing, 1)
        self.assertEqual(result.output_logs, 1)

    def test_changed_only_materializes_complete_remote_source_round(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            contest_dir = repo / "Contest" / "2026"
            self.write_source_log(contest_dir)
            second = contest_dir / "S54X.log"
            second.write_text((contest_dir / "S53M.log").read_text(encoding="utf-8"), encoding="utf-8")
            self.init_git_repo(repo)
            subprocess.run(["git", "add", "Contest"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-qm", "fixture"], cwd=repo, check=True)
            rel_second = second.relative_to(repo)
            subprocess.run(
                ["git", "update-index", "--skip-worktree", rel_second.as_posix()],
                cwd=repo,
                check=True,
            )
            second.unlink()
            first = contest_dir / "S53M.log"
            first.write_text(first.read_text(encoding="utf-8") + "SOAPBOX: changed\n", encoding="utf-8")
            workspace = repo / "workspace"

            prepared = reconstruct.prepare_changed_contest_dirs(
                repo,
                workspace,
                reconstruct.ArchiveInventory(repo),
            )

            self.assertEqual(prepared, [(workspace / "Contest" / "2026", Path("Contest/2026"))])
            self.assertTrue((workspace / rel_second).is_file())
            self.assertIn("SOAPBOX: changed", (workspace / "Contest/2026/S53M.log").read_text())

    def test_reconstruct_cache_includes_output_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            contest_dir = repo / "Contest" / "2026"
            out_dir = repo / "RECONSTRUCTED_LOGS" / "Contest" / "2026"
            ledger_root = repo / "ledgers"
            self.write_source_log(contest_dir)
            state_path = reconstruct.state_path_for(out_dir, repo, ledger_root)
            reconstruct.save_state(
                state_path,
                {
                    "schema_version": reconstruct.STATE_SCHEMA_VERSION,
                    "cache_key": self.cache_key(contest_dir, created_by="old-tool"),
                    "submitted_logs": 1,
                    "parsed_qsos": 12,
                    "reconstructed_logs": 0,
                    "skipped_existing": 0,
                    "output_logs": 0,
                },
            )

            result = reconstruct.reconstruct_contest(
                contest_dir=contest_dir,
                out_dir=out_dir,
                master_calls={"K1ABC"},
                master_hash="same-master",
                min_qsos=10,
                created_by="test",
                contest_name=None,
                season_label=None,
                dry_run=True,
                limit=None,
                repo_root=repo,
                ledger_root=ledger_root,
                ledger_name=".reconstructed_ledger.txt",
                use_ledger=False,
                skip_unchanged=True,
            )

        self.assertEqual(result.skipped_unchanged, 0)
        self.assertEqual(result.reconstructed_logs, 1)

    def test_reconstruct_cache_includes_min_qsos(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            contest_dir = repo / "Contest" / "2026"
            out_dir = repo / "RECONSTRUCTED_LOGS" / "Contest" / "2026"
            ledger_root = repo / "ledgers"
            self.write_source_log(contest_dir)
            state_path = reconstruct.state_path_for(out_dir, repo, ledger_root)
            reconstruct.save_state(
                state_path,
                {
                    "schema_version": reconstruct.STATE_SCHEMA_VERSION,
                    "cache_key": self.cache_key(contest_dir, min_qsos=11),
                    "submitted_logs": 1,
                    "parsed_qsos": 12,
                    "reconstructed_logs": 0,
                    "skipped_existing": 0,
                    "output_logs": 0,
                },
            )

            result = reconstruct.reconstruct_contest(
                contest_dir=contest_dir,
                out_dir=out_dir,
                master_calls={"K1ABC"},
                master_hash="same-master",
                min_qsos=10,
                created_by="test",
                contest_name=None,
                season_label=None,
                dry_run=True,
                limit=None,
                repo_root=repo,
                ledger_root=ledger_root,
                ledger_name=".reconstructed_ledger.txt",
                use_ledger=False,
                skip_unchanged=True,
            )

        self.assertEqual(result.skipped_unchanged, 0)
        self.assertEqual(result.reconstructed_logs, 1)

    def test_reconstruct_cache_hit_reports_cached_outputs_not_new_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            contest_dir = repo / "Contest" / "2026"
            out_dir = repo / "RECONSTRUCTED_LOGS" / "Contest" / "2026"
            ledger_root = repo / "ledgers"
            self.write_source_log(contest_dir)
            out_dir.mkdir(parents=True)
            (out_dir / "K1ABC.log").write_text("START-OF-LOG: 3.0\nEND-OF-LOG:\n", encoding="utf-8")
            cache_key = self.cache_key(contest_dir)
            state_path = reconstruct.state_path_for(out_dir, repo, ledger_root)
            reconstruct.save_state(
                state_path,
                {
                    "schema_version": reconstruct.STATE_SCHEMA_VERSION,
                    "cache_key": cache_key,
                    "submitted_logs": 1,
                    "parsed_qsos": 12,
                    "reconstructed_logs": 1,
                    "skipped_existing": 0,
                    "output_logs": 1,
                },
            )

            result = reconstruct.reconstruct_contest(
                contest_dir=contest_dir,
                out_dir=out_dir,
                master_calls={"K1ABC"},
                master_hash="same-master",
                min_qsos=10,
                created_by="test",
                contest_name=None,
                season_label=None,
                dry_run=True,
                limit=None,
                repo_root=repo,
                ledger_root=ledger_root,
                ledger_name=".reconstructed_ledger.txt",
                use_ledger=False,
                skip_unchanged=True,
            )

        self.assertEqual(result.skipped_unchanged, 1)
        self.assertEqual(result.reconstructed_logs, 0)
        self.assertEqual(result.cached_reconstructed_logs, 1)
        self.assertEqual(result.output_logs, 1)
        self.assertEqual(result.skipped_existing, 0)

    def test_reconstruct_cache_hit_rebuilds_when_outputs_are_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            contest_dir = repo / "Contest" / "2026"
            out_dir = repo / "RECONSTRUCTED_LOGS" / "Contest" / "2026"
            ledger_root = repo / "ledgers"
            self.write_source_log(contest_dir)
            state_path = reconstruct.state_path_for(out_dir, repo, ledger_root)
            reconstruct.save_state(
                state_path,
                {
                    "schema_version": reconstruct.STATE_SCHEMA_VERSION,
                    "cache_key": self.cache_key(contest_dir),
                    "submitted_logs": 1,
                    "parsed_qsos": 12,
                    "reconstructed_logs": 1,
                    "skipped_existing": 0,
                    "output_logs": 1,
                },
            )

            result = reconstruct.reconstruct_contest(
                contest_dir=contest_dir,
                out_dir=out_dir,
                master_calls={"K1ABC"},
                master_hash="same-master",
                min_qsos=10,
                created_by="test",
                contest_name=None,
                season_label=None,
                dry_run=True,
                limit=None,
                repo_root=repo,
                ledger_root=ledger_root,
                ledger_name=".reconstructed_ledger.txt",
                use_ledger=False,
                skip_unchanged=True,
            )

        self.assertEqual(result.skipped_unchanged, 0)
        self.assertEqual(result.reconstructed_logs, 1)
        self.assertEqual(result.cached_reconstructed_logs, 0)
        self.assertEqual(result.output_logs, 0)

    def test_stale_ledger_entry_does_not_suppress_missing_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            contest_dir = repo / "Contest" / "2026"
            out_dir = repo / "RECONSTRUCTED_LOGS" / "Contest" / "2026"
            ledger_root = repo / "ledgers"
            self.write_source_log(contest_dir)
            dest = out_dir / "K1ABC.log"
            ledger_path = reconstruct.ledger_path_for(
                out_dir,
                repo,
                ledger_root,
                ".reconstructed_ledger.txt",
            )
            key = dest.relative_to(repo).as_posix()
            reconstruct.ReconstructLedger(ledger_path).add(key, "stale")

            result = reconstruct.reconstruct_contest(
                contest_dir=contest_dir,
                out_dir=out_dir,
                master_calls={"K1ABC"},
                master_hash="same-master",
                min_qsos=10,
                created_by="test",
                contest_name=None,
                season_label=None,
                dry_run=True,
                limit=None,
                repo_root=repo,
                ledger_root=ledger_root,
                ledger_name=".reconstructed_ledger.txt",
                use_ledger=True,
                skip_unchanged=False,
            )

        self.assertEqual(result.reconstructed_logs, 1)
        self.assertEqual(result.skipped_existing, 0)

    def test_stale_ledger_entry_does_not_suppress_missing_output_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            contest_dir = repo / "Contest" / "2026"
            out_dir = repo / "RECONSTRUCTED_LOGS" / "Contest" / "2026"
            ledger_root = repo / "ledgers"
            self.write_source_log(contest_dir)
            dest = out_dir / "K1ABC.log"
            ledger_path = reconstruct.ledger_path_for(
                out_dir,
                repo,
                ledger_root,
                ".reconstructed_ledger.txt",
            )
            key = dest.relative_to(repo).as_posix()
            reconstruct.ReconstructLedger(ledger_path).add(key, "stale")

            result = reconstruct.reconstruct_contest(
                contest_dir=contest_dir,
                out_dir=out_dir,
                master_calls={"K1ABC"},
                master_hash="same-master",
                min_qsos=10,
                created_by="test",
                contest_name=None,
                season_label=None,
                dry_run=False,
                limit=None,
                repo_root=repo,
                ledger_root=ledger_root,
                ledger_name=".reconstructed_ledger.txt",
                use_ledger=True,
                skip_unchanged=False,
            )

            self.assertTrue(dest.exists())

        self.assertEqual(result.reconstructed_logs, 1)
        self.assertEqual(result.output_logs, 1)

    def test_duplicate_source_qsos_are_written_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            contest_dir = repo / "Contest" / "2026"
            out_dir = repo / "RECONSTRUCTED_LOGS" / "Contest" / "2026"
            ledger_root = repo / "ledgers"
            contest_dir.mkdir(parents=True)
            qso = "QSO: 14000 CW 2026-01-01 1200 S53M 599 001 K1ABC 599 002"
            (contest_dir / "S53M.log").write_text(
                "\n".join(
                    [
                        "START-OF-LOG: 3.0",
                        "CALLSIGN: S53M",
                        "CONTEST: TEST",
                        qso,
                        qso,
                        "END-OF-LOG:",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            result = reconstruct.reconstruct_contest(
                contest_dir=contest_dir,
                out_dir=out_dir,
                master_calls={"K1ABC"},
                master_hash="same-master",
                min_qsos=1,
                created_by="test",
                contest_name=None,
                season_label=None,
                dry_run=False,
                limit=None,
                repo_root=repo,
                ledger_root=ledger_root,
                ledger_name=".reconstructed_ledger.txt",
                use_ledger=False,
                skip_unchanged=False,
            )

            lines = (out_dir / "K1ABC.log").read_text(encoding="utf-8").splitlines()
            reconstructed_qsos = [line for line in lines if line.startswith("QSO:")]

        self.assertEqual(result.parsed_qsos, 2)
        self.assertEqual(result.reconstructed_logs, 1)
        self.assertEqual(len(reconstructed_qsos), 1)

    def test_changed_round_replaces_affected_existing_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            contest_dir = repo / "Contest" / "2026"
            out_dir = repo / "RECONSTRUCTED_LOGS" / "Contest" / "2026"
            self.write_source_log(contest_dir)
            out_dir.mkdir(parents=True)
            destination = out_dir / "K1ABC.log"
            destination.write_text(
                "START-OF-LOG: 3.0\nSOAPBOX: stale output\nEND-OF-LOG:\n",
                encoding="utf-8",
            )

            result = reconstruct.reconstruct_contest(
                contest_dir=contest_dir,
                out_dir=out_dir,
                master_calls={"K1ABC"},
                master_hash="same-master",
                min_qsos=10,
                created_by="test",
                contest_name=None,
                season_label=None,
                dry_run=False,
                limit=None,
                repo_root=repo,
                ledger_root=repo / "ledgers",
                ledger_name=".reconstructed_ledger.txt",
                use_ledger=False,
                skip_unchanged=False,
                replace_existing=True,
            )

            content = destination.read_text(encoding="utf-8")

        self.assertEqual(result.reconstructed_logs, 1)
        self.assertNotIn("stale output", content)
        self.assertIn("QSO:", content)

    def test_dry_run_does_not_record_existing_outputs_in_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            contest_dir = repo / "Contest" / "2026"
            out_dir = repo / "RECONSTRUCTED_LOGS" / "Contest" / "2026"
            ledger_root = repo / "ledgers"
            self.write_source_log(contest_dir)
            out_dir.mkdir(parents=True)
            (out_dir / "K1ABC.log").write_text("START-OF-LOG: 3.0\nEND-OF-LOG:\n", encoding="utf-8")
            ledger_path = reconstruct.ledger_path_for(
                out_dir,
                repo,
                ledger_root,
                ".reconstructed_ledger.txt",
            )

            result = reconstruct.reconstruct_contest(
                contest_dir=contest_dir,
                out_dir=out_dir,
                master_calls={"K1ABC"},
                master_hash="same-master",
                min_qsos=10,
                created_by="test",
                contest_name=None,
                season_label=None,
                dry_run=True,
                limit=None,
                repo_root=repo,
                ledger_root=ledger_root,
                ledger_name=".reconstructed_ledger.txt",
                use_ledger=True,
                skip_unchanged=False,
            )

        self.assertEqual(result.reconstructed_logs, 0)
        self.assertEqual(result.skipped_existing, 1)
        self.assertFalse(ledger_path.exists())

    def test_reconstruct_result_supports_legacy_tuple_unpack(self):
        result = reconstruct.ReconstructResult(
            submitted_logs=1,
            parsed_qsos=2,
            reconstructed_logs=3,
            skipped_existing=4,
            skipped_unchanged=5,
            cached_reconstructed_logs=6,
            output_logs=7,
        )

        submitted, parsed, written, skipped, unchanged = result

        self.assertEqual((submitted, parsed, written, skipped, unchanged), (1, 2, 3, 4, 5))


if __name__ == "__main__":
    unittest.main()
