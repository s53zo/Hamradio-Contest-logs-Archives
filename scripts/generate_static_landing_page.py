#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

from archive_storage import atomic_write_text


GITHUB_URL = "https://github.com/s53zo/Hamradio-Contest-logs-Archives"


def build_page() -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Hamradio Contest Logs Archives</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f4ec;
      --fg: #1f2520;
      --muted: #6a736c;
      --card: #fffdf7;
      --line: #d7d2c5;
      --link: #005a3c;
    }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
      background: linear-gradient(180deg, #fcfbf7 0%, var(--bg) 100%);
      color: var(--fg);
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }}
    main {{
      max-width: 760px;
      width: 100%;
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 32px 28px;
      box-shadow: 0 10px 28px rgba(0, 0, 0, 0.05);
    }}
    h1 {{
      margin: 0 0 12px;
      font-size: 1.4rem;
      line-height: 1.3;
    }}
    p {{
      margin: 0 0 18px;
      color: var(--muted);
      line-height: 1.6;
    }}
    a {{
      color: var(--link);
      text-decoration: none;
      word-break: break-all;
    }}
    a:hover {{
      text-decoration: underline;
    }}
  </style>
</head>
<body>
  <main>
    <h1>Hamradio Contest Logs Archives</h1>
    <p>This Azure mirror serves the archive files directly. The project home is on GitHub:</p>
    <p><a href="{GITHUB_URL}">{GITHUB_URL}</a></p>
  </main>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir", type=Path)
    args = parser.parse_args()

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_text(output_dir / "index.html", build_page())


if __name__ == "__main__":
    main()
