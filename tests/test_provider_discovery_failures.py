import importlib
import io
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import public_logs_downloader as public


class ProviderDiscoveryFailuresTests(unittest.TestCase):
    def test_sac_and_ure_unrecognized_indexes_fail_explicitly(self):
        for function, discovery in ((public.tasks_sac, 'sac_discover_year_modes'),
                                    (public.tasks_ure, 'ure_discover_contests')):
            with self.subTest(provider=function.__name__):
                with mock.patch.object(public, discovery, return_value=[]):
                    with self.assertRaisesRegex(RuntimeError, 'no recognized'):
                        function(1)

    def test_public_log_index_login_or_unknown_html_is_not_empty_success(self):
        for provider_id in (1, 2, 3, 4, 5, 26):
            with self.subTest(provider=provider_id):
                with mock.patch.object(public, 'fetch_text', return_value='<html>Login required</html>'):
                    with self.assertRaisesRegex(RuntimeError, 'format not recognized'):
                        public.PROVIDERS[provider_id][1](1)

    def test_failed_period_inventory_never_returns_a_successful_empty_queue(self):
        cases = (
            ('tasks_arrl', None, [('arrl_discover_contests', [('Example', 'eid')])], 'arrl_discover_years'),
            ('tasks_vhfmanager', 'download_vhfmanager_logs',
             [('migrate_legacy_checklog_markers', None),
              ('discover_contests', [SimpleNamespace(cid=1)])], 'discover_logs'),
            ('tasks_darc_all', 'download_darc_logs', [], 'discover_loglist'),
            ('tasks_ref', 'download_ref_logs', [], 'discover_calls'),
            ('tasks_eudx', 'download_eudx_logs', [('discover_years', [2026])], 'discover_log_urls'),
            ('tasks_istra_open', 'download_istra_open_logs',
             [('discover_year_urls', [(2026, 'https://example.invalid')])], 'discover_log_urls'),
            ('tasks_ttc_spcwc', 'download_ttc_spcwc_logs',
             [('iter_rounds', [SimpleNamespace(date='2026-10-06')])], 'discover_station_logs'),
            ('tasks_okomdx', 'download_okomdx_logs', [], 'discover_shared_rounds'),
            ('tasks_spdx', 'download_spdx_logs', [('discover_years', [2026])], 'discover_calls'),
            ('tasks_yudx', None, [('yudx_discover_years', [2026])], 'yudx_result_metas'),
            ('tasks_hrs_hf', None, [('hrs_discover_rounds', [SimpleNamespace(cid='round')])], 'hrs_discover_call_infos'),
            ('tasks_sac', None, [('sac_discover_year_modes', [(2026, 'cw')])], 'sac_discover_logs'),
            ('tasks_ure', None, [('ure_discover_contests', [SimpleNamespace(code='cme', year=2026)])], 'ure_discover_logs'),
        )
        for adapter, module_name, setup, failing in cases:
            with self.subTest(provider=adapter), ExitStack() as stack:
                module = importlib.import_module(module_name) if module_name else public
                for name, value in setup:
                    stack.enter_context(mock.patch.object(module, name, return_value=value))
                stack.enter_context(mock.patch.object(module, failing, side_effect=TimeoutError('offline')))
                stack.enter_context(redirect_stdout(io.StringIO()))
                stack.enter_context(redirect_stderr(io.StringIO()))
                with self.assertRaises((RuntimeError, TimeoutError)):
                    getattr(public, adapter)(1)


if __name__ == '__main__':
    unittest.main()
