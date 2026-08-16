#!/usr/bin/env python3
"""
Shared task ledger utilities for download scripts.
"""
from __future__ import annotations

import hashlib
import atexit
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

TASK_LEDGER_PATH = Path("state") / "downloads" / "tasks.sqlite"


@dataclass(frozen=True)
class TaskRecord:
    list_hash: str
    item_count: int | None
    output_count: int | None
    empty_count: int | None


class TaskLedger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=DELETE")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                task_key TEXT PRIMARY KEY,
                list_hash TEXT NOT NULL,
                item_count INTEGER,
                output_count INTEGER,
                empty_count INTEGER,
                last_checked INTEGER
            )
            """
        )
        columns = {
            str(row[1]) for row in self._conn.execute("PRAGMA table_info(tasks)")
        }
        if "output_count" not in columns:
            self._conn.execute("ALTER TABLE tasks ADD COLUMN output_count INTEGER")
        if "empty_count" not in columns:
            self._conn.execute("ALTER TABLE tasks ADD COLUMN empty_count INTEGER")
        self._conn.commit()
        atexit.register(self.close)

    def get(self, task_key: str) -> TaskRecord | None:
        with self._lock:
            if self._conn is None:
                raise RuntimeError("task ledger is closed")
            row = self._conn.execute(
                """
                SELECT list_hash, item_count, output_count, empty_count
                FROM tasks
                WHERE task_key = ?
                """,
                (task_key,),
            ).fetchone()
            if row is None:
                return None
            return TaskRecord(
                list_hash=str(row[0]),
                item_count=row[1],
                output_count=row[2],
                empty_count=row[3],
            )

    def has_hash(self, task_key: str, list_hash: str) -> bool:
        record = self.get(task_key)
        return bool(record and record.list_hash == list_hash)

    def set_hash(
        self,
        task_key: str,
        list_hash: str,
        item_count: int,
        *,
        output_count: int | None = None,
        empty_count: int | None = None,
    ) -> None:
        if output_count is not None and empty_count is None:
            empty_count = max(0, item_count - output_count)
        with self._lock:
            if self._conn is None:
                raise RuntimeError("task ledger is closed")
            self._conn.execute(
                """
                INSERT INTO tasks (
                    task_key, list_hash, item_count, output_count, empty_count,
                    last_checked
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_key) DO UPDATE SET
                    list_hash=excluded.list_hash,
                    item_count=excluded.item_count,
                    output_count=excluded.output_count,
                    empty_count=excluded.empty_count,
                    last_checked=excluded.last_checked
                WHERE tasks.list_hash <> excluded.list_hash
                   OR tasks.item_count IS NOT excluded.item_count
                   OR tasks.output_count IS NOT excluded.output_count
                   OR tasks.empty_count IS NOT excluded.empty_count
                """,
                (
                    task_key,
                    list_hash,
                    item_count,
                    output_count,
                    empty_count,
                    int(time.time()),
                ),
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def __enter__(self) -> "TaskLedger":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


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
    *,
    output_count: int | None = None,
    empty_count: int | None = None,
) -> None:
    if ledger:
        ledger.set_hash(
            task_key,
            list_hash,
            item_count,
            output_count=output_count,
            empty_count=empty_count,
        )
