#!/usr/bin/env python3
"""Migrate legacy updater markers into the canonical state hierarchy."""

from __future__ import annotations

import argparse
from pathlib import Path

from provider_state import ProviderState


def parse_marker(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    required = {"jobdate", "kolo", "pub_level", "calls"}
    if not required.issubset(values):
        raise RuntimeError(f"incomplete OK1WC marker: {path}")
    return values


def migrate_ok1wc_markers(repo_root: Path, remove_legacy: bool = False) -> tuple[int, bool]:
    markers = sorted((repo_root / "OK1WC_Memorial").glob("*/.pub_level_*.complete"))
    provider = ProviderState(repo_root / "state" / "providers" / "ok1wc.json")
    scopes = provider.scopes()
    for marker in markers:
        values = parse_marker(marker)
        scope = marker.parent.name
        prior = scopes.get(scope)
        if prior and int(str(prior.get("pub_level", 0))) > int(values["pub_level"]):
            continue
        scopes[scope] = {
            "calls": int(values["calls"]),
            "jobdate": values["jobdate"],
            "kolo": values["kolo"],
            "pub_level": values["pub_level"],
        }
    changed = provider.replace_scopes(scopes)
    migrated = provider.scopes()
    for marker in markers:
        values = parse_marker(marker)
        stored = migrated.get(marker.parent.name)
        if stored is None or int(str(stored["pub_level"])) < int(values["pub_level"]):
            raise RuntimeError(f"OK1WC marker migration verification failed: {marker}")
    if remove_legacy:
        for marker in markers:
            marker.unlink()
    return len(markers), changed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--remove-legacy", action="store_true")
    args = parser.parse_args()
    count, changed = migrate_ok1wc_markers(args.repo.resolve(), args.remove_legacy)
    print(
        f"OK1WC state: markers={count} state_changed={str(changed).lower()} "
        f"legacy_removed={str(args.remove_legacy).lower()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
