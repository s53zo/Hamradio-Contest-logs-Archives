import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


def load_downloader_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "public_logs_downloader.py"
    scripts_dir = module_path.parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    spec = importlib.util.spec_from_file_location("public_logs_downloader_ok1wc_test", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


pld = load_downloader_module()


class FakeOK1WCSession:
    def __init__(self):
        self.round_fetches = 0
        self.log_fetches = []

    def fetch_round_page(self, _round_info):
        self.round_fetches += 1
        return "<form action='table_out_vypis'><option value='DH0GHU'></option></form>"


class OK1WCProviderTests(unittest.TestCase):
    def round_info(self):
        return pld.OK1WCRound(
            kolo="709",
            rocnik="18",
            tyden="14",
            jobdate="260330",
            pub_level="4",
            date_iso="2026-03-30",
            url=(
                "https://memorial-ok1wc.cz/index.php?page=eval3/a_eval3&"
                "kolo=709&rocnik=18&tyden=14&jobdate=260330&pub_level=4"
            ),
        )

    def test_fetch_log_reloads_round_context_after_guard_page(self):
        round_info = self.round_info()

        class RetrySession:
            def __init__(self, testcase):
                self.testcase = testcase
                self.fetches = 0
                self.round_fetches = 0

            def fetch_text(self, _url, data=None):
                self.fetches += 1
                self.testcase.assertEqual(data["CallS"], "DH0GHU")
                if self.fetches == 1:
                    return "Zobrazení referenční tabulky není možné před ukončením příjmu deníků."
                return "<table><tr><td>1</td></tr></table>"

            def fetch_round_page(self, _round_info):
                self.round_fetches += 1
                return "<form action='table_out_vypis'></form>"

        session = RetrySession(self)

        html = pld.ok1wc_fetch_log_html(session, round_info, "DH0GHU")

        self.assertEqual(html, "<table><tr><td>1</td></tr></table>")
        self.assertEqual(session.fetches, 2)
        self.assertEqual(session.round_fetches, 1)

    def test_call_tasks_reuse_initialized_round_session(self):
        round_info = self.round_info()
        session = FakeOK1WCSession()
        qso = pld.OK1WCQSO(
            index="1",
            category="SO-40M-CW-LOW",
            freq="7000",
            mode="CW",
            date="2026-03-30",
            time="1700",
            own_call="DH0GHU",
            sent_rst="599",
            sent_exchange="001",
            received_rst="599",
            received_exchange="001",
            worked_call="S53M",
        )

        original_output_root = pld.OK1WC_OUTPUT_ROOT
        original_session_class = pld.OK1WCSession
        original_discover_rounds = pld.ok1wc_discover_rounds
        original_fetch_round_calls = pld.ok1wc_fetch_round_calls
        original_fetch_log_html = pld.ok1wc_fetch_log_html
        original_parse_qsos = pld.ok1wc_parse_qsos
        original_should_write_log = pld.ok1wc_should_write_log
        original_remove_invalid_existing = pld.remove_invalid_existing
        original_task_should_skip = pld.task_should_skip_known_outputs

        with tempfile.TemporaryDirectory() as tmp:
            pld.OK1WC_OUTPUT_ROOT = Path(tmp) / "OK1WC_Memorial"
            pld.OK1WCSession = lambda: session
            pld.ok1wc_discover_rounds = lambda _last: [round_info]

            def fake_fetch_round_calls(_round_info):
                return session, ["DH0GHU", "S53M"], "<form action='table_out_vypis'></form>"

            def fake_fetch_log_html(fetch_session, _round_info, call):
                self.assertIs(fetch_session, session)
                session.log_fetches.append(call)
                return "<table></table>"

            pld.ok1wc_fetch_round_calls = fake_fetch_round_calls
            pld.ok1wc_fetch_log_html = fake_fetch_log_html
            pld.ok1wc_parse_qsos = lambda _html: [qso]
            pld.ok1wc_should_write_log = lambda _dest, _round_info: True
            pld.remove_invalid_existing = lambda _dest: None
            pld.task_should_skip_known_outputs = (
                lambda _task_key, _task_items, dests, **_kwargs: (False, "hash", len(dests))
            )

            try:
                tasks = pld.tasks_ok1wc(last=1)
                counts = [task.action() for task in tasks]
            finally:
                pld.OK1WC_OUTPUT_ROOT = original_output_root
                pld.OK1WCSession = original_session_class
                pld.ok1wc_discover_rounds = original_discover_rounds
                pld.ok1wc_fetch_round_calls = original_fetch_round_calls
                pld.ok1wc_fetch_log_html = original_fetch_log_html
                pld.ok1wc_parse_qsos = original_parse_qsos
                pld.ok1wc_should_write_log = original_should_write_log
                pld.remove_invalid_existing = original_remove_invalid_existing
                pld.task_should_skip_known_outputs = original_task_should_skip

        self.assertEqual(len(tasks), 2)
        self.assertEqual(counts, [{"ok": 1}, {"ok": 1}])
        self.assertEqual(session.round_fetches, 1)
        self.assertEqual(session.log_fetches, ["DH0GHU", "S53M"])


if __name__ == "__main__":
    unittest.main()
