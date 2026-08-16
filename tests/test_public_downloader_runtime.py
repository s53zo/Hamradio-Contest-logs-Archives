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
            with self.assertRaisesRegex(RuntimeError, "site recovers"):
                public.discover_provider_tasks(
                    11,
                    "Wednesday Mini-Test 40m",
                    lambda _last: (_ for _ in ()).throw(
                        OSError("cannot read from timed out object")
                    ),
                    1,
                )
            with self.assertRaisesRegex(RuntimeError, "site recovers"):
                public.discover_provider_tasks(
                    12,
                    "Russian DX",
                    blocked_provider,
                    1,
                )
        blocked_provider.assert_not_called()

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
                    {"error": 1},
                )

        self.assertNotIn("top-secret", output.getvalue())
        self.assertIn("TEST.log", output.getvalue())


if __name__ == "__main__":
    unittest.main()
