Public logs gathered in github repo.

## Data stats
There are about 2,2mio logs in this repo and more than 500 mio QSOs. The overall volume is impressive and keeps growing as more contests are added.
Unique callsigns count: 188.506.

## Why this archive exists
I publish these logs because I believe contest data should be collected in a manageable, accessible way for everyone who wants to analyze, learn, or build tools on top of it.

## How to use the data
Logs are organized by contest, then mode or year when relevant. Most files are in plain text and follow common contest log formats (e.g. Cabrillo-style `QSO:` lines), so they can be parsed with standard tools or simple scripts.

## Reconstructed logs (mock submissions)
For contests where not all stations submitted logs, the repo can generate **reconstructed mock logs**. These are built by inferring QSOs for missing stations from logs that were submitted. They are **not** official submissions and are stored separately under `RECONSTRUCTED_LOGS/` with the same contest/year structure as the original logs.

Key points:
- Only callsigns present in `MASTER.DTA` are eligible for reconstruction.
- A minimum QSO threshold is enforced (default: 10).
- Logs are marked as checklogs and include clear SOAPBOX warnings that they are reconstructed.
- Reconstructed logs are included in the SH6 shard index when shards are rebuilt.

## Contributing logs
If you know of additional public log sources or missing contests, please open an issue or send a link. I will do my best to add them and keep the archive consistent.

## Deploying GitHub Pages in chunks

Use `Restore one top-level folder to gh-pages` in Actions to deploy one folder at a time.
Run it manually with `top_folder` set to a contest directory (for example `ARRL`) and optional `source_ref`.
This updates only that top-level folder in `gh-pages`, then commits and pushes, so you can wait for one folder to finish before starting the next.

You can also run it with GitHub CLI from the repo root:

```sh
./scripts/restore-gh-pages-folders.sh [--source-ref main] [--max-bytes 950000000] [--reset-branch true] [ARRL CQWW ...]
```

If no folders are provided, the script discovers top-level directories automatically and skips:
`.git`, `.github`, `scripts`, `.reconstructed_ledgers`.

Each folder is dispatched separately, the workflow is watched to completion, and folders larger than `--max-bytes` are split automatically using chunked restore.

## Cloning a subset
If you don't need all of them you can git clone just part of the repo with (example)

```sh
git clone --filter=blob:none --sparse https://github.com/s53zo/Hamradio-Contest-logs-Archives.git

cd Hamradio-Contest-logs-Archives

git sparse-checkout set WAE/CW (or some other folder)
```

## Serving the Archive

This archive is served directly from GitHub now. There is no Azure or Bunny mirror workflow in this repo anymore.

For updates, push the repository:

```sh
git status -sb
git add -A
git commit -m "New logs"
git push -u origin main
```

## Web Branch Publish

Pushes to `main` trigger [`.github/workflows/sync-web-branch.yml`](/Users/simon/Hamradio-Contest-logs-Archives/.github/workflows/sync-web-branch.yml).

That workflow keeps a reduced publish branch named `Web` in sync with:
- all tracked files at the repository root
- the top-level `SH6/` directory

Other top-level directories remain only on `main`.

For local inspection, you can print the exact selection with:

```sh
bash scripts/sync-web-branch.sh --print-paths
```

73
