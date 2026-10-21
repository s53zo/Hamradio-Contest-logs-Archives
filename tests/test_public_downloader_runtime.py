import concurrent.futures
import io
import os
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock


SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import public_logs_downloader as public  # noqa: E402


class PublicDownloaderRuntimeTests(unittest.TestCase):
    def test_download_starts_before_other_provider_finishes_discovery(self):
        downloaded = threading.Event()
        slow_observed_download = []
        download_calls = []

        def download():
            download_calls.append(1)
            downloaded.set()
            return {"ok": 1}

        def slow(_last):
            slow_observed_download.append(downloaded.wait(timeout=3))
            return []

        task = public.DownloadTask(
            dest=Path("TTC-SPCWC/2099-01-01/EARLY.log"),
            host="example.invalid", source="fixture", action=download,
        )
        with (
            mock.patch.object(public, "PROVIDERS", {
                1: ("Fast", lambda _last: [task]), 2: ("Slow", slow),
            }),
            mock.patch.object(public, "valid_existing_log", return_value=False),
            mock.patch.object(public, "resolve_hosts", return_value={"example.invalid": []}),
            mock.patch.object(sys, "argv", [
                "downloader", "--non-interactive", "--contests", "all",
                "--workers", "2", "--heartbeat", "0",
                "--no-task-ledger", "--no-post-download-shards",
            ]),
            redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(public._main(), 0)
        self.assertEqual(slow_observed_download, [True])
        self.assertEqual(download_calls, [1])

    def test_waiting_ua9_providers_do_not_occupy_general_discovery_workers(self):
        general_started = threading.Event()
        first_finished = threading.Event()
        observations = []

        def first(_last):
            observations.append(("general", general_started.wait(timeout=3)))
            first_finished.set()
            return []

        def second(_last):
            observations.append(("serialized", first_finished.is_set()))
            return []

        def general(_last):
            general_started.set()
            return []

        with (
            mock.patch.object(public, "PROVIDERS", {
                11: ("UA9 first", first), 12: ("UA9 second", second),
                31: ("Independent", general),
            }),
            mock.patch.object(public, "get_ua9qcq_cookie", return_value=""),
            mock.patch.object(sys, "argv", [
                "downloader", "--non-interactive", "--contests", "all",
                "--workers", "1", "--heartbeat", "0",
                "--no-task-ledger", "--no-post-download-shards",
            ]),
            redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(public._main(), 0)
        self.assertEqual(observations, [("general", True), ("serialized", True)])

    def test_vhf_contest_batches_do_not_multiply_inner_worker_pools(self):
        active = 0
        maximum_active = 0
        lock = threading.Lock()
        def action():
            nonlocal active, maximum_active
            with lock:
                active += 1
                maximum_active = max(maximum_active, active)
            time.sleep(0.02)
            with lock:
                active -= 1
            return {"ok": 1}
        tasks = [public.DownloadTask(dest=Path(f"EU_VHF_CONTESTS/fixture/K{number}ABC.log"), host="vhfmanager.net", source="VHFManager", action=action)
                 for number in range(4)]
        argv = ["public_logs_downloader.py", "--non-interactive", "--contests", "all", "--last", "1",
                "--workers", "4", "--heartbeat", "0", "--no-task-ledger", "--no-post-download-shards"]
        with (
            mock.patch.object(public, "PROVIDERS", {10: ("Fixture VHF", lambda last: tasks)}),
            mock.patch.object(public, "valid_existing_log", return_value=False),
            mock.patch.object(public, "resolve_hosts", return_value={"vhfmanager.net": []}),
            mock.patch.object(sys, "argv", argv),
            redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(public._main(), 0)
        self.assertEqual(maximum_active, 1)
        self.assertEqual(public.VHF_LOG_WORKERS, 4)
    def test_weekly_ua9_cache_fingerprints_each_round(self):
        import download_wednesday_minitest_40m_ubn as forty
        import download_wednesday_minitest_80m_ubn as eighty
        for provider, module, root in (
            (public.tasks_wed_minitest_40m, forty, "WednesdayMiniTest40m"),
            (public.tasks_wed_minitest_80m, eighty, "WednesdayMiniTest80m"),
        ):
            with (
                self.subTest(root=root),
                mock.patch.object(public, "configure_ua9qcq_module"),
                mock.patch.object(public, "get_ua9qcq_cookie", return_value="test"),
                mock.patch.object(module, "fetch_text_with_cookie", return_value="complete"),
                mock.patch.object(module, "parse_year_options", return_value=["2026"]),
                mock.patch.object(module, "parse_date_options", return_value=["2026-10-14"]),
                mock.patch.object(module, "parse_more_info_entries", return_value=[{"callsign": "S53ZO/P", "id_res": "123"}]),
                mock.patch.object(public, "task_should_skip_known_outputs", return_value=(False, "hash", 1)) as cache,
            ):
                tasks = provider(1)
                self.assertEqual(len(tasks), 1)
                self.assertEqual(tasks[0].task_key, root + "/2026-10-14")
                self.assertEqual(cache.call_args.args[1], ["123:S53ZO/P"])
                self.assertEqual(cache.call_args.args[2], [Path(root + "/2026-10-14/S53ZO_P.log")])

    def test_ua9_batch_counts_include_existing_and_empty_stations(self):
        stats = mock.Mock(saved_logs=35, skipped_existing=1573, skipped_empty=75)
        counts = public.ua9_batch_counts(stats, "ok")
        self.assertEqual(counts, {"ok": 1, "files_written": 35, "output_count": 1608, "empty_count": 75})
        stats.saved_logs = 0
        self.assertEqual(public.ua9_batch_counts(stats, "skip")["output_count"], 1573)

    def test_annual_ua9_cache_tracks_stations_and_missing_outputs(self):
        module = mock.Mock()
        module.RESULTS_URL = "https://ua9qcq.com/results.php"
        module.results_post_data.side_effect = lambda year, date: {"year": year, "date": date}
        module.fetch_text_with_cookie.return_value = "complete results"
        module.parse_year_options.return_value = ["2026"]
        module.parse_date_options.return_value = []
        module.parse_more_info_entries.return_value = [
            {"callsign": "S53ZO/P", "id_res": "123"},
        ]
        with (
            mock.patch.object(public, "configure_ua9qcq_module"),
            mock.patch.object(public, "get_ua9qcq_cookie", return_value="test"),
            mock.patch.object(public, "task_should_skip", return_value=(True, "hash", 1)) as cached,
            mock.patch.object(public, "valid_existing_log", return_value=False) as exists,
        ):
            tasks = public.tasks_ua9qcq_yearly(1, module, "Russian DX", Path("RussianDXContest"))
            self.assertEqual(len(tasks), 1)
            self.assertEqual(cached.call_args.args[1], ["0:123:S53ZO/P"])
            exists.assert_called_once_with(Path("RussianDXContest/2026/S53ZO_P.log"))
            exists.return_value = True
            self.assertEqual(public.tasks_ua9qcq_yearly(1, module, "Russian DX", Path("RussianDXContest")), [])

    def test_batch_task_and_file_counts_use_separate_units(self):
        counts = public.empty_counts()
        public.record_task_result(counts, {"ok": 1, "files_written": 40})
        self.assertEqual(counts["tasks_completed"], 1)
        self.assertEqual(counts["written_files"], 40)
        public.record_task_result(counts, {"ok": 83})
        self.assertEqual(counts["tasks_completed"], 2)
        self.assertEqual(counts["written_files"], 123)

    def setUp(self) -> None:
        public.UA9QCQ_COOKIE = None
        public.UA9QCQ_DISCOVERY_OUTAGE = None

    def tearDown(self) -> None:
        public.UA9QCQ_COOKIE = None
        public.UA9QCQ_DISCOVERY_OUTAGE = None

    def test_cookie_prompt_hides_input(self) -> None:
        fake_stdin = mock.Mock()
        fake_stdin.isatty.return_value = True
        with (
            mock.patch.dict(os.environ, {}, clear=False),
            mock.patch.object(public.sys, "stdin", fake_stdin),
            mock.patch.object(public.getpass, "getpass", return_value="secret") as prompt,
        ):
            os.environ.pop("UA9QCQ_COOKIE", None)
            self.assertEqual(public.get_ua9qcq_cookie(), "PHPSESSID=secret")
            prompt.assert_called_once()
            self.assertEqual(os.environ["UA9QCQ_COOKIE"], "PHPSESSID=secret")
            os.environ.pop("UA9QCQ_COOKIE", None)

    def test_provider_discovery_errors_are_not_empty_successes(self) -> None:
        cases = (
            (public.tasks_sac, "sac_discover_year_modes"),
            (public.tasks_ure, "ure_discover_contests"),
            (public.tasks_hrs_hf, "hrs_discover_rounds"),
        )
        for provider, discovery in cases:
            with self.subTest(provider=provider.__name__):
                with (
                    mock.patch.object(public, discovery, side_effect=TimeoutError("offline")),
                    redirect_stdout(io.StringIO()),
                ):
                    with self.assertRaisesRegex(RuntimeError, "discovery failed"):
                        provider(1)
    def test_full_cookie_header_is_preserved(self) -> None:
        self.assertEqual(
            public.normalize_ua9qcq_cookie("PHPSESSID=secret; lang=en"),
            "PHPSESSID=secret; lang=en",
        )
        self.assertEqual(
            public.normalize_ua9qcq_cookie("Cookie: PHPSESSID=secret"),
            "PHPSESSID=secret",
        )

    def test_ua9qcq_discovery_is_serialized(self) -> None:
        active = 0
        maximum_active = 0
        state_lock = threading.Lock()

        def provider(_last_years):
            nonlocal active, maximum_active
            with state_lock:
                active += 1
                maximum_active = max(maximum_active, active)
            time.sleep(0.05)
            with state_lock:
                active -= 1
            return []

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(public.discover_provider_tasks, provider_id, "UA9", provider, 1)
                for provider_id in (11, 12)
            ]
            for future in futures:
                self.assertEqual(future.result(), [])

        self.assertEqual(maximum_active, 1)

    def test_ua9qcq_discovery_retries_transient_failure(self) -> None:
        calls = 0

        def provider(_last_years):
            nonlocal calls
            calls += 1
            if calls < 3:
                raise TimeoutError("temporary")
            return []

        with mock.patch.object(public.time, "sleep"):
            self.assertEqual(
                public.discover_provider_tasks(12, "Russian DX", provider, 1),
                [],
            )
        self.assertEqual(calls, 3)

    def test_ua9qcq_transport_failure_opens_shared_circuit(self) -> None:
        blocked_provider = mock.Mock(return_value=[])
        with mock.patch.object(public, "UA9QCQ_DISCOVERY_ATTEMPTS", 1):
            with self.assertRaisesRegex(ConnectionError, "VPN/WARP"):
                public.discover_provider_tasks(
                    11,
                    "Wednesday Mini-Test 40m",
                    lambda _last: (_ for _ in ()).throw(
                        OSError("cannot read from timed out object")
                    ),
                    1,
                )
            with self.assertRaisesRegex(ConnectionError, "VPN/WARP"):
                public.discover_provider_tasks(
                    12,
                    "Russian DX",
                    blocked_provider,
                    1,
                )
        blocked_provider.assert_not_called()

    def test_sinkhole_dns_uses_public_resolver_address(self) -> None:
        sinkhole = [
            (
                public.socket.AF_INET,
                public.socket.SOCK_STREAM,
                6,
                "",
                ("0.0.0.0", 443),
            )
        ]
        recovered = [
            (
                public.socket.AF_INET,
                public.socket.SOCK_STREAM,
                6,
                "",
                ("78.47.8.237", 443),
            )
        ]

        def getaddrinfo(host, *_args):
            return sinkhole if host == "vhfmanager.net" else recovered

        with (
            mock.patch.object(public, "ORIGINAL_GETADDRINFO", side_effect=getaddrinfo),
            mock.patch.object(
                public,
                "fallback_dns_addresses",
                return_value=("78.47.8.237",),
            ),
            mock.patch.object(public, "DNS_FALLBACK_LOGGED", set()),
            redirect_stdout(io.StringIO()),
        ):
            result = public.sinkhole_aware_getaddrinfo(
                "vhfmanager.net",
                443,
                public.socket.AF_UNSPEC,
                public.socket.SOCK_STREAM,
            )

        self.assertEqual(result, recovered)

    def test_real_dns_answer_does_not_use_fallback(self) -> None:
        resolved = [
            (
                public.socket.AF_INET,
                public.socket.SOCK_STREAM,
                6,
                "",
                ("62.109.19.112", 443),
            )
        ]
        with (
            mock.patch.object(public, "ORIGINAL_GETADDRINFO", return_value=resolved),
            mock.patch.object(public, "fallback_dns_addresses") as fallback,
        ):
            result = public.sinkhole_aware_getaddrinfo("ua9qcq.com", 443)

        self.assertEqual(result, resolved)
        fallback.assert_not_called()

    def test_discovery_failure_returns_nonzero(self) -> None:
        def fail(_last_years):
            raise RuntimeError("provider unavailable")

        argv = [
            "public_logs_downloader.py",
            "--non-interactive",
            "--contests",
            "1",
            "--last",
            "1",
            "--no-task-ledger",
            "--no-post-download-shards",
        ]
        output = io.StringIO()
        with (
            mock.patch.object(public, "PROVIDERS", {1: ("Fixture", fail)}),
            mock.patch.object(sys, "argv", argv),
            redirect_stdout(output),
            redirect_stderr(output),
        ):
            self.assertEqual(public._main(), 1)
        self.assertIn("FAILED: provider unavailable", output.getvalue())
        self.assertIn("Run incomplete", output.getvalue())

    def test_connection_discovery_warning_allows_no_download_resume(self) -> None:
        def fail(_last_years):
            try:
                raise ConnectionResetError("server offline")
            except ConnectionResetError as exc:
                raise RuntimeError("URE discovery failed") from exc

        output = io.StringIO()
        with (
            mock.patch.object(public, "PROVIDERS", {1: ("Fixture", fail)}),
            mock.patch.object(sys, "argv", [
                "downloader", "--non-interactive", "--contests", "1",
                "--no-task-ledger", "--no-post-download-shards",
            ]),
            redirect_stdout(output), redirect_stderr(output),
        ):
            self.assertEqual(public._main(), 0)
        self.assertIn("WARNING: URE discovery failed", output.getvalue())
        self.assertIn("Continuing with available files", output.getvalue())

    def test_connection_failures_allow_successful_downloads_to_proceed(self) -> None:
        def offline(_last):
            raise TimeoutError("offline provider")

        def reset():
            raise ConnectionResetError("offline download")

        tasks = [
            public.DownloadTask(
                dest=Path(f"TTC-SPCWC/2099-01-01/TEST{n}.log"),
                host="example.invalid", source="fixture", action=action,
                task_key=f"fixture/{n}", task_hash="hash", task_count=1,
            )
            for n, action in enumerate((lambda: {"ok": 1}, reset))
        ]
        output = io.StringIO()
        with (
            mock.patch.object(public, "PROVIDERS", {
                1: ("Online", lambda _last: tasks), 2: ("Offline", offline),
            }),
            mock.patch.object(public, "valid_existing_log", return_value=False),
            mock.patch.object(public, "resolve_hosts", return_value={"example.invalid": []}),
            mock.patch.object(public, "task_mark_complete") as complete,
            mock.patch.object(sys, "argv", [
                "downloader", "--non-interactive", "--contests", "1,2",
                "--workers", "1", "--heartbeat", "0",
                "--no-task-ledger", "--no-post-download-shards",
            ]),
            redirect_stdout(output), redirect_stderr(output),
        ):
            self.assertEqual(public._main(), 0)
        self.assertIn("Skipping post-download SH6 rebuild", output.getvalue())
        self.assertIn("1 download connection failure(s)", output.getvalue())
        self.assertEqual(complete.call_count, 1)
        self.assertEqual(complete.call_args.args[0], "fixture/0")

    def test_download_error_returns_nonzero_before_post_processing(self) -> None:
        task = public.DownloadTask(
            dest=Path("TTC-SPCWC/2099-01-01/TEST.log"),
            host="example.invalid",
            source="fixture",
            action=lambda: {"error": 1},
        )
        argv = [
            "public_logs_downloader.py",
            "--non-interactive",
            "--contests",
            "1",
            "--last",
            "1",
            "--workers",
            "1",
            "--heartbeat",
            "0",
            "--no-task-ledger",
            "--no-post-download-shards",
        ]
        output = io.StringIO()
        with (
            mock.patch.object(public, "PROVIDERS", {1: ("Fixture", lambda _last: [task])}),
            mock.patch.object(public, "valid_existing_log", return_value=False),
            mock.patch.object(public, "resolve_hosts", return_value={"example.invalid": []}),
            mock.patch.object(sys, "argv", argv),
            redirect_stdout(output),
            redirect_stderr(output),
        ):
            self.assertEqual(public._main(), 1)
        self.assertIn("1 download error(s)", output.getvalue())
        self.assertNotIn("Skipping post-download SH6 rebuild", output.getvalue())

    def test_host_cap_is_shared_across_providers(self) -> None:
        active = 0
        maximum_active = 0
        state_lock = threading.Lock()

        def action():
            nonlocal active, maximum_active
            with state_lock:
                active += 1
                maximum_active = max(maximum_active, active)
            time.sleep(0.05)
            with state_lock:
                active -= 1
            return {"ok": 1}

        def provider(number):
            return lambda _last: [
                public.DownloadTask(
                    dest=Path(f"TTC-SPCWC/2099-01-01/TEST{number}.log"),
                    host="ua9qcq.com",
                    source=f"fixture-{number}",
                    action=action,
                )
            ]

        argv = [
            "public_logs_downloader.py",
            "--non-interactive",
            "--contests",
            "all",
            "--last",
            "1",
            "--workers",
            "2",
            "--heartbeat",
            "0",
            "--no-task-ledger",
            "--no-post-download-shards",
        ]
        with (
            mock.patch.object(
                public,
                "PROVIDERS",
                {1: ("Fixture 1", provider(1)), 2: ("Fixture 2", provider(2))},
            ),
            mock.patch.object(public, "valid_existing_log", return_value=False),
            mock.patch.object(public, "resolve_hosts", return_value={"ua9qcq.com": []}),
            mock.patch.object(sys, "argv", argv),
            redirect_stdout(io.StringIO()),
            redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(public._main(), 0)

        self.assertEqual(maximum_active, 1)

    def test_download_failure_does_not_print_signed_url(self) -> None:
        signed_url = "https://example.invalid/log?action=get&token=top-secret"
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as tempdir:
            destination = Path(tempdir) / "TEST.log"
            with (
                mock.patch.object(
                    public.urllib.request,
                    "urlopen",
                    side_effect=TimeoutError("timed out"),
                ),
                redirect_stdout(output),
            ):
                self.assertEqual(
                    public.download_file(destination, signed_url, retries=1),
                    {"error": 1, "connection_warning": 1},
                )

        self.assertNotIn("top-secret", output.getvalue())
        self.assertIn("TEST.log", output.getvalue())


if __name__ == "__main__":
    unittest.main()
