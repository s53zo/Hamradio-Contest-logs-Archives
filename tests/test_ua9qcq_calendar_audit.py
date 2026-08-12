import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import audit_ua9qcq_calendar as audit  # noqa: E402


def test_parse_external_rules_event():
    page = """
    <table><tr><td>Sat, 01 Aug, 2026 00:01</td><td>02 Aug, 2026 23:58</td>
    <td>Phone</td><td>Example QSO Party</td><td>&nbsp;</td>
    <td><a href="https://example.test/rules">Go to ...</a></td></tr></table>
    """

    assert audit.parse_month(page) == [
        {
            "start": "Sat, 01 Aug, 2026 00:01",
            "finish": "02 Aug, 2026 23:58",
            "modes": "Phone",
            "contest": "Example QSO Party",
            "results_url": "",
            "rules_url": "https://example.test/rules",
        }
    ]


def test_parse_ua9qcq_results_event():
    page = """
    <table><tr><td>Wed, 05 Aug, 2026 17:00</td><td>05 Aug, 2026 17:59</td>
    <td>CW</td><td>Wednesday mini-contest 40m</td>
    <td><form><input name="testid" value="242"></form></td>
    <td><form><input name="t_id" value="242"></form></td></tr></table>
    """

    event = audit.parse_month(page)[0]
    assert event["results_url"] == "https://ua9qcq.com/results_new.php?testid=242"
    assert event["contest"] == "Wednesday mini-contest 40m"


def test_month_range_crosses_year_boundary():
    assert list(audit.months(2025, 11, 2026, 2)) == [
        (2025, 11),
        (2025, 12),
        (2026, 1),
        (2026, 2),
    ]
