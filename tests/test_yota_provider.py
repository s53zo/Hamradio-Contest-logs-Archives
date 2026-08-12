import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import download_yota_contest_logs as yota  # noqa: E402


def event():
    return yota.YOTAEvent("event-id", 2025, "YOTA-2025-3", "Round_3")


def test_discover_events_filters_overall_and_nonpublic(monkeypatch):
    monkeypatch.setattr(
        yota,
        "fetch_json",
        lambda _url: [
            {"_id": "one", "name": "YOTA Contest 3rd Round - 2025", "isClaimed": True},
            {"_id": "overall", "name": "YOTA Contest 2025 Overall", "isClaimed": False},
            {"_id": "future", "name": "YOTA Contest 1st Round - 2026", "isClaimed": False},
        ],
    )
    assert yota.discover_events() == [yota.YOTAEvent("one", 2025, "YOTA-2025-3", "Round_3")]


def test_discover_entries_deduplicates_calls(monkeypatch):
    monkeypatch.setattr(
        yota,
        "fetch_json",
        lambda _url: [
            {
                "evalCategory": {
                    "name": "Single Operator",
                    "rounds": [{"code": "YOTA-2025-3-"}],
                },
                "logs": [{"_id": {"callsign": "f4hvv"}}],
            },
            {"evalCategory": {"name": "Checklog"}, "logs": [{"_id": {"callsign": "F4HVV"}}]},
        ],
    )
    assert yota.discover_entries(event()) == [
        yota.YOTAEntry(event(), "F4HVV", "Single Operator", "YOTA-2025-3-")
    ]


def test_discover_entries_uses_scored_round_code_for_checklogs(monkeypatch):
    monkeypatch.setattr(
        yota,
        "fetch_json",
        lambda _url: [
            {
                "evalCategory": {"name": "Single Operator", "rounds": [{"code": "YOTA-2025-3-"}]},
                "logs": [],
            },
            {"evalCategory": {"name": "Checklog"}, "logs": [{"_id": {"callsign": "HA8A"}}]},
        ],
    )
    assert yota.discover_entries(event()) == [
        yota.YOTAEntry(event(), "HA8A", "Checklog", "YOTA-2025-3-")
    ]


def test_build_cabrillo_uses_public_qso_fields():
    entry = yota.YOTAEntry(event(), "F4HVV", "Single Operator All band", "YOTA-2025-3-")
    cabrillo = yota.build_cabrillo(
        entry,
        {
            "qsos": [
                {
                    "dateTime": "2025-12-30T10:11:00.000Z",
                    "freq": 14000,
                    "mode": "PH",
                    "sRst": "59",
                    "sNum": 24,
                    "callsign": "HA8TA",
                    "rRst": "59",
                    "rNum": 15,
                }
            ]
        },
    )
    assert "CONTEST: YOTA-CONTEST" in cabrillo
    assert "QSO: 14000 PH 2025-12-30 1011 F4HVV" in cabrillo
    assert "HA8TA" in cabrillo
    assert cabrillo.endswith("END-OF-LOG:\n")


def test_destination_separates_rounds(tmp_path):
    entry = yota.YOTAEntry(event(), "HB0/F4HVV", "Single Operator", "YOTA-2025-3-")
    assert yota.destination(entry, tmp_path) == tmp_path / "2025" / "Round_3" / "HB0_F4HVV.log"
