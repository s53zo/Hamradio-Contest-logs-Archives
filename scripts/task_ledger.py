#!/usr/bin/env python3
"""
Shared task ledger utilities for download scripts.
"""
from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
from pathlib import Path
from typing import Iterable, List, Tuple

TASK_LEDGER_PATH = Path("scripts") / "download_tasks_ledger.sqlite"


class TaskLedger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                task_key TEXT PRIMARY KEY,
                list_hash TEXT NOT NULL,
                item_count INTEGER,
                last_checked INTEGER
            )
            """
        )
        self._conn.commit()

    def has_hash(self, task_key: str, list_hash: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "SELECT list_hash FROM tasks WHERE task_key = ?",
                (task_key,),
            )
            row = cur.fetchone()
            return bool(row and row[0] == list_hash)

    def set_hash(self, task_key: str, list_hash: str, item_count: int) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO tasks (task_key, list_hash, item_count, last_checked)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(task_key) DO UPDATE SET
                    list_hash=excluded.list_hash,
                    item_count=excluded.item_count,
                    last_checked=excluded.last_checked
                """,
                (task_key, list_hash, item_count, int(time.time())),
            )
            self._conn.commit()


def normalize_items(items: Iterable[str], upper: bool = False) -> List[str]:
    uniq: set[str] = set()
    for item in items:
        text = str(item).strip()
        if not text:
            continue
        if upper:
            text = text.upper()
        uniq.add(text)
    return sorted(uniq)


def hash_items(items: Iterable[str], upper: bool = False) -> Tuple[str, int]:
    normalized = normalize_items(items, upper=upper)
    blob = "\n".join(normalized).encode("utf-8")
    return hashlib.sha256(blob).hexdigest(), len(normalized)


def task_should_skip(
    ledger: TaskLedger | None,
    task_key: str,
    items: Iterable[str],
    upper: bool = False,
) -> Tuple[bool, str, int]:
    list_hash, count = hash_items(items, upper=upper)
    if ledger and ledger.has_hash(task_key, list_hash):
        return True, list_hash, count
    return False, list_hash, count


def task_mark_complete(
    ledger: TaskLedger | None,
    task_key: str,
    list_hash: str,
    item_count: int,
) -> None:
    if ledger:
        ledger.set_hash(task_key, list_hash, item_count)
