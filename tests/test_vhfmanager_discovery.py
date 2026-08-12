import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import download_vhfmanager_logs as vhf  # noqa: E402


def test_discovery_stops_when_provider_is_unavailable(monkeypatch):
    calls = []

    def unavailable(url, retries=3, delay=1.0):
        calls.append((url, retries))
        raise ConnectionRefusedError("provider unavailable")

    monkeypatch.setattr(vhf, "fetch_text", unavailable)

    with pytest.raises(RuntimeError, match="3 consecutive discovery requests"):
        vhf.discover_contests(1)

    assert len(calls) == 3
    assert all(retries == 1 for _, retries in calls)


def test_missing_ids_do_not_trigger_transport_circuit_breaker(monkeypatch):
    def available_without_logs(url, retries=3, delay=1.0):
        return "<html><title>No results</title></html>"

    monkeypatch.setattr(vhf, "fetch_text", available_without_logs)

    assert vhf.discover_contests(1) == []
