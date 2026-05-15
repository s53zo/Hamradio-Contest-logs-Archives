import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import download_zrs_kvp_logs as kvp  # noqa: E402


def test_parse_legacy_kvp_log_without_frequency_column():
    html = """
    <table class="display_log">
      <tbody>
        <tr>
          <td>20.04.25</td><td>07:00</td><td>S56B</td><td>SSB</td>
          <td>59</td><td>50</td><td>59</td><td>87*</td>
          <td></td><td></td><td></td><td>1</td>
        </tr>
      </tbody>
    </table>
    """

    assert kvp.parse_qsos(html, 2025) == [
        (3700, "PH", "2025-04-20", "0700", "S56B", "59", "50", "59", "87")
    ]


def test_parse_current_kvp_log_with_exact_frequency_column():
    html = """
    <table class="display_log">
      <tbody>
        <tr>
          <td>3698 kHz</td><td>19.04.26</td><td>07:00</td><td>S53M</td><td>SSB</td>
          <td>59</td><td>83</td><td>59</td><td>50*</td>
          <td></td><td></td><td></td><td>1</td>
        </tr>
      </tbody>
    </table>
    """

    assert kvp.parse_qsos(html, 2026) == [
        (3698, "PH", "2026-04-19", "0700", "S53M", "59", "83", "59", "50")
    ]

