#!/usr/bin/env python3
"""Deterministic tracked state for downloader providers."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any


class ProviderState:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()

    def _load_unlocked(self) -> dict[str, Any]:
        if not self.path.is_file():
            return {"schema_version": 1, "scopes": {}}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"invalid provider state: {self.path}: {exc}") from exc
        if data.get("schema_version") != 1 or not isinstance(data.get("scopes"), dict):
            raise RuntimeError(f"unsupported provider state schema: {self.path}")
        return data

    def scopes(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            data = self._load_unlocked()
            return {key: dict(value) for key, value in data["scopes"].items()}

    def get_scope(self, scope: str) -> dict[str, Any] | None:
        with self._lock:
            value = self._load_unlocked()["scopes"].get(scope)
            return dict(value) if isinstance(value, dict) else None

    def update_scope(self, scope: str, values: dict[str, Any]) -> bool:
        with self._lock:
            data = self._load_unlocked()
            normalized = dict(sorted(values.items()))
            if data["scopes"].get(scope) == normalized:
                return False
            data["scopes"][scope] = normalized
            self._write_unlocked(data)
            return True

    def replace_scopes(self, scopes: dict[str, dict[str, Any]]) -> bool:
        normalized = {
            scope: dict(sorted(values.items()))
            for scope, values in sorted(scopes.items())
        }
        with self._lock:
            data = {"schema_version": 1, "scopes": normalized}
            content = json.dumps(data, indent=2, sort_keys=True) + "\n"
            if self.path.is_file() and self.path.read_text(encoding="utf-8") == content:
                return False
            self._write_content_unlocked(content)
            return True

    def _write_unlocked(self, data: dict[str, Any]) -> None:
        content = json.dumps(data, indent=2, sort_keys=True) + "\n"
        self._write_content_unlocked(content)

    def _write_content_unlocked(self, content: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(content, encoding="utf-8")
        os.replace(temporary, self.path)
