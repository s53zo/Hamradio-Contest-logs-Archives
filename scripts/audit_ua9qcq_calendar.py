#!/usr/bin/env python3
"""Export normalized contest events from the UA9QCQ monthly calendar."""

from __future__ import annotations

import argparse
import csv
import html
import re
import urllib.parse
import urllib.request
from datetime import date
from html.parser import HTMLParser
from typing import Iterable

CALENDAR_URL = "https://ua9qcq.com/calendar_new.php"
ROW_RE = re.compile(r"<tr(?:\s[^>]*)?>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
CELL_RE = re.compile(r"<td(?:\s[^>]*)?>(.*?)</td>", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []
        self.inputs: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if tag == "a" and values.get("href"):
            self.links.append(values["href"] or "")
        if tag == "input" and values.get("name"):
            self.inputs[values["name"] or ""] = values.get("value") or ""


def clean(fragment: str) -> str:
    return " ".join(html.unescape(TAG_RE.sub(" ", fragment)).replace("\ufeff", "").split())


def fetch_month(year: int, month: int) -> str:
    body = urllib.parse.urlencode(
        {
            "lang": "en",
            "mo_calend": "1",
            "cyc_calend": "0",
            "only_we": "0",
            "cldr_month": str(month),
            "cldr_year": str(year),
        }
    ).encode()
    request = urllib.request.Request(
        CALENDAR_URL,
        data=body,
        headers={"User-Agent": "Hamradio-Contest-logs-Archives calendar audit"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8-sig", errors="replace")


def parse_month(page: str) -> list[dict[str, str]]:
    events = []
    for row in ROW_RE.findall(page):
        cells = CELL_RE.findall(row)
        if len(cells) < 6:
            continue
        start, finish, modes, contest = (clean(cell) for cell in cells[:4])
        if not re.search(r"\b\d{4}\b", start) or not contest or contest == "Contest":
            continue
        results_parser = LinkParser()
        results_parser.feed(cells[4])
        rules_parser = LinkParser()
        rules_parser.feed(cells[5])
        test_id = results_parser.inputs.get("testid", "")
        results_url = (
            f"https://ua9qcq.com/results_new.php?testid={test_id}"
            if test_id
            else (results_parser.links[0] if results_parser.links else "")
        )
        events.append(
            {
                "start": start,
                "finish": finish,
                "modes": modes,
                "contest": contest,
                "results_url": results_url,
                "rules_url": rules_parser.links[0] if rules_parser.links else "",
            }
        )
    return events


def months(start_year: int, start_month: int, end_year: int, end_month: int) -> Iterable[tuple[int, int]]:
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        yield year, month
        month += 1
        if month == 13:
            year, month = year + 1, 1


def main() -> int:
    today = date.today()
    default_start_year = today.year - 1
    default_start_month = today.month
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=f"{default_start_year:04d}-{default_start_month:02d}")
    parser.add_argument("--end", default=f"{today.year:04d}-{today.month:02d}")
    args = parser.parse_args()
    start_year, start_month = map(int, args.start.split("-"))
    end_year, end_month = map(int, args.end.split("-"))
    writer = csv.DictWriter(
        __import__("sys").stdout,
        fieldnames=("start", "finish", "modes", "contest", "results_url", "rules_url"),
        dialect="excel-tab",
        lineterminator="\n",
    )
    writer.writeheader()
    for year, month in months(start_year, start_month, end_year, end_month):
        writer.writerows(parse_month(fetch_month(year, month)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
