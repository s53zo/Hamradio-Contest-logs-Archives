# Hamradio Contest Logs Archives

This repository collects publicly available amateur radio contest logs in one
place. The archive is intended for analysis, search, education, tooling, and
long-term preservation of contest activity data.

## Current Snapshot

<!-- STATS:START -->
SH6-indexed snapshot counted on 2026-09-01:

- total indexed log files: 2,219,870
- source/public indexed log files: 1,771,351
- reconstructed mock log files in `RECONSTRUCTED_LOGS/`: 448,519
- unique source/public callsigns in the SH6 index: 177,994
- contest roots in the SH6 index: 35
- SQLite shard files in `SH6/`: 256
<!-- STATS:END -->

These numbers change as new public sources are added, newer contest years are
published, reconstructed logs are regenerated, and SH6 shards are rebuilt.

## What Is Included

Most logs are stored as plain text Cabrillo-style files. Some contests publish
original submitted Cabrillo logs. Others publish public UBN reports, result
JSON, reference tables, or evaluated QSO tables; for those sources the scripts
recreate Cabrillo-like logs from the public data.

Current downloader coverage includes:

- CQ contests: `CQWW`, `CQWPX`, `CQWWRTTY`, `CQ160`, `CQWPXRTTY`
- `ARRL` public logs
- `ZRS_KVP`
- `EUHFC`
- `WAE`
- `EU_VHF_CONTESTS` and `WW_PMC` from VHFManager
- UA9QCQ UBN sources: Wednesday Mini-Test 40m/80m, Russian DX Contest, RF Championship CW, Ham Spirit, RCC Cup, RDA, Russian Radio Team Championship, Yuri Gagarin DX Contest
- `REF`
- `EUDX_contest`
- `Istra_Open_Contest`
- `TTC-SPCWC`
- OK contest family: `OK_Contest`, `OK_OM_DX_Contest`, `OK_DX_RTTY_contest`
- `DARC` contests: Fieldday, WAG, Ausbildungscontest, Ausbildungscontest CW, RTTY Kurzcontest, FT4, Easter, XMAS
- `WWDIGI`
- `SPDX_contest`
- `OK1WC_Memorial`
- `YU_DX_Contest`
- `SAC`
- `URE`
- `9A_HRS_Contest`
- `YOTA_Contest` evaluated public QSO tables
- One-time import: `WRTC` 2026 logs

## Directory Layout

Logs are organized by contest, then by mode, year, round, or contest-specific
subfolder where appropriate. Examples:

```text
CQWW/cw/2024/K1ABC.log
ARRL/arrl_10_meter_contest/2024/K1ABC.log
DARC/WAG/2024/K1ABC.log
DARC/Fieldday/CW/2024/K1ABC.log
SAC/CW/2024/K1ABC.log
9A_HRS_Contest/Zimski_KV_Kup/2026/K1ABC.log
TTC-SPCWC/2026-06-23/K1ABC.log
OK1WC_Memorial/2026-03-30/OK1ABC.log
YOTA_Contest/2026/Round_2/K1ABC.log
RECONSTRUCTED_LOGS/CQWW/cw/2024/K1ABC.log
SH6/logs_00.sqlite
```

`WAE` intentionally remains a top-level contest folder. Other DARC-run contests
are grouped under `DARC/`.

Contest directories and filenames are immutable public interfaces. Other
applications access individual logs through GitHub and `raw.githubusercontent.com`,
so existing log paths must never be moved, renamed, compressed, or replaced by
Git LFS pointers.

## Available Years By Top-Level Directory

<!-- YEARS:START -->
Years are collected from SH6 index metadata derived from archive paths.
Source/public logs and reconstructed logs are included; repo/tooling
directories are not indexed.

| Top-level directory | Available years | Indexed logs |
|---|---|---:|
| 9A HRS Contest | 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 21,628 |
| ARRL | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 331,419 |
| CQ160 | 2022, 2023, 2024, 2025, 2026 | 17,838 |
| CQWPX | 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 272,647 |
| CQWPXRTTY | 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 62,174 |
| CQWW | 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 413,732 |
| CQWWRTTY | 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 68,465 |
| DARC | 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 49,584 |
| EU VHF CONTESTS | 1980, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2039, 2056, 2057, 2065 | 582,677 |
| EUDX contest | 2023, 2024, 2025, 2026 | 9,326 |
| EUHFC | 2001, 2002, 2003, 2004, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2023, 2024, 2025 | 27,342 |
| HamSpiritContest | 2024, 2025 | 1,783 |
| Istra Open Contest | 2026 | 107 |
| OK OM DX Contest | 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 25,238 |
| OK1WC Memorial | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 48,549 |
| RCCCup | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 3,377 |
| RDAContest | 2018, 2019, 2020, 2021, 2022, 2023 | 8,115 |
| REF | 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 41,552 |
| RFChampionshipCW | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 2,488 |
| RussianDXContest | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 10,755 |
| RussianRadioTeamChampionship | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 3,883 |
| SAC | 2021, 2023, 2024, 2025 | 11,330 |
| SPDX contest | 2019, 2020, 2021, 2023, 2024, 2025, 2026 | 18,880 |
| TTC-SPCWC | 2026 | 1,216 |
| URE | 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 58,424 |
| WAE | 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 48,767 |
| WRTC | 2018, 2026 | 3,216 |
| WW PMC | 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 8,450 |
| WWDIGI | 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 13,397 |
| WednesdayMiniTest40m | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 4,408 |
| WednesdayMiniTest80m | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 13,956 |
| YOTA_Contest | 2021, 2022, 2023, 2024, 2025, 2026 | 14,102 |
| YU DX Contest | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 5,165 |
| YuriGagarinDXContest | 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 11,858 |
| ZRS KVP | 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 4,022 |
<!-- YEARS:END -->

## Data Quality

The archive mirrors public contest data, so quality depends on the original
source. A file may be:

- an original public Cabrillo log
- a Cabrillo-like reconstruction from UBN data
- a Cabrillo-like reconstruction from public result JSON
- a Cabrillo-like reconstruction from public reference or evaluation tables
- a reconstructed mock log for a station that did not submit a public log

Recreated and reconstructed logs are useful for analysis, but they are not
official contest submissions. Consumers should inspect the `CREATED-BY`,
`CONTEST`, `CATEGORY`, and `SOAPBOX` headers when source provenance matters.

## Updater Architecture

The archive uses one permanent public repository. Each updater computer has a
blobless sparse clone of that same repository containing only `.github/`,
`scripts/`, `tests/`, `state/`, `SH6/`, and root files. Contest logs remain on
GitHub and are recognized through SH6 even when their blobs are absent locally.

An update downloads only missing logs, temporarily materializes complete source
rounds needed for reconstruction, incrementally updates affected SH6 shards,
commits logs plus state plus indexes together, and then removes archive folders
from the sparse working tree. See [UPDATER.md](UPDATER.md) for the complete
operating and recovery guide.

## Choose A Checkout

Use the smallest checkout that matches the job:

| Purpose | Recommended checkout |
| --- | --- |
| Download and publish new logs | Blobless sparse updater clone |
| Query or audit SH6 | Sparse clone containing `SH6/` |
| Read one contest, mode, or year | Sparse clone of only that directory |
| Keep every current log locally | Shallow full clone |
| Inspect or preserve complete Git history | Complete full clone |
| Fetch one known log | Raw GitHub URL; no clone required |

## Create A Partial Updater Clone

Python 3.10 or newer, Git 2.34 or newer, and authenticated GitHub push access
are required. On each updater computer, create one blobless sparse checkout:

```sh
git clone --depth 1 --filter=blob:none --sparse --single-branch --branch main \
  https://github.com/s53zo/Hamradio-Contest-logs-Archives.git
cd Hamradio-Contest-logs-Archives
git sparse-checkout set --cone --sparse-index .github scripts tests state SH6
git status --short
git sparse-checkout list
python3 scripts/shard_index.py audit
```

This is the only clone needed on that computer. Contest directories and
`RECONSTRUCTED_LOGS/` remain on GitHub and are absent locally until an update
temporarily materializes changed rounds. See [UPDATER.md](UPDATER.md) when
replacing an older full checkout.

`git status --short` should print nothing. The sparse list should contain
`.github`, `scripts`, `tests`, `state`, and `SH6`; root files are included by
Git cone mode. A healthy SH6 audit ends with `missing=0 extra=0`.

## Create A Full Clone

To keep all current contest logs locally without downloading old Git history:

```sh
git clone --depth 1 --single-branch --branch main \
  https://github.com/s53zo/Hamradio-Contest-logs-Archives.git \
  Hamradio-Contest-logs-Archives-full
```

To clone both the current archive and its complete history:

```sh
git clone https://github.com/s53zo/Hamradio-Contest-logs-Archives.git \
  Hamradio-Contest-logs-Archives-history
```

The archive contains more than two million files, and a history clone is much
larger than a shallow current-state clone. A full clone is useful for offline
analysis or a recovery SH6 rebuild, but it is not required for routine updates.

## Update Contest Logs And Publish

Always begin by receiving the newest scripts, durable state, and SH6 shards:

```sh
./scripts/update_last_year_and_push.sh
```

This convenience command fast-forwards the checkout, checks every provider's
most recent contest year, reconstructs affected rounds, updates SH6, runs tests,
commits, and pushes. Optional updater arguments are passed through, for example:

```sh
./scripts/update_last_year_and_push.sh --workers 8
```

The equivalent explicit commands are:

```sh
git pull --ff-only
python3 scripts/archive_updater.py --dry-run --contests all --last 1
python3 scripts/archive_updater.py --contests all --last 1 --publish
```

For selected providers or lower concurrency:

```sh
python3 scripts/archive_updater.py --contests 28,30,31,32 --last 1 --workers 8 --publish
python3 scripts/archive_updater.py --contests 30 --last all --publish
```

`--last 1` checks the most recent contest year; `--last all` checks every year
published by the selected provider. Run the interactive downloader shown below
to display the current numbered provider menu.

The updater performs provider discovery, downloads, changed-round
reconstruction, incremental SH6 updates, README statistics, tests, scoped
staging, commit creation, remote reconciliation, push verification, and sparse
cleanup. A run with no new material reports that the archive is already current
and creates no commit.

UA9QCQ providers read the session cookie from `UA9QCQ_COOKIE`. Interactive runs
prompt for it without displaying the value. For unattended automation, inject
the variable through the machine's secret manager rather than putting the value
in a command, shell history, or tracked file. Cookies, credentials, `.env`
files, PID files, active transaction journals, and SQLite sidecars must not be
committed.

The low-level interactive downloader remains available for diagnostics:

```sh
python3 scripts/public_logs_downloader.py
```

## Recovery And Phases

`archive_updater.py` stores its untracked transaction journal under `.git/hcla/`.
After Ctrl-C, rerun the same command; complete logs and durable state are reused.
The wrapper interrupts the child process, then terminates it if it does not stop
within the bounded shutdown period.

To run the workflow in explicit phases:

```sh
python3 scripts/archive_updater.py --phase download --contests all --last 1
python3 scripts/archive_updater.py --phase reconstruct
python3 scripts/archive_updater.py --phase shards --publish
```

To adopt valid logs downloaded before the journal existed:

```sh
python3 scripts/archive_updater.py --resume-existing --phase reconstruct
python3 scripts/archive_updater.py --phase shards --publish
```

If another computer advances `main`, publication fetches again and rebases only
when Git can reconcile the commits normally. A divergent edit to the same log,
state file, README section, or shard stops without force-pushing or overwriting
the remote. Pull the newer commit and rerun the updater.

## Durable State

Tracked cross-computer state is under `state/`:

- `state/downloads/tasks.sqlite` stores completed provider inventory hashes,
  produced-output counts, and legitimate empty-result counts.
- `state/reconstruction/ledgers/` stores reconstruction cache and output state.
- `state/providers/ok1wc.json` stores OK1WC publication levels.
- `state/providers/vhfmanager/checklogs/<contest_id>/<log_id>.done` records
  processed VHFManager seed and referenced check logs.
- `state/schema.json` records canonical state locations.

No-op runs leave these files byte-for-byte unchanged. Local transaction and
temporary state stays under `.git/hcla/` or uses ignored sidecar suffixes.

## SH6 Maintenance

`SH6/` contains 256 SQLite shards used by https://s53m.com/SH6/. Routine updates
upsert only paths changed in the working tree and refuse implicit log deletion.
Audit the complete Git tree without fetching contest blobs:

```sh
python3 scripts/shard_index.py audit
```

The full recovery rebuild is intentionally blocked in sparse clones because an
incomplete working tree would produce incomplete shards. Run it only from a
full clone; it builds a replacement directory before swapping it into place:

```sh
python3 scripts/public_logs_downloader.py --rebuild-shards
```

The updater stages generated archive paths outside the sparse cone with Git's
sparse-aware staging mode. Temporary "sparse index is expanding" hints can
appear while those paths are materialized; successful publication cleans them
up automatically. To clean up manually after diagnostics, run:

```sh
git sparse-checkout reapply --sparse-index
```

Sparse cleanup removes working files but not locally created blobs reachable
from `HEAD`. When `.git` becomes too large, use the bootstrap command to create
a fresh updater clone at a new path, verify it, and then remove the old clone
manually.

## Clone One Contest, Mode, Or Year

Create a blobless sparse clone, then select only the directory you need:

```sh
git clone --depth 1 --filter=blob:none --sparse --single-branch --branch main \
  https://github.com/s53zo/Hamradio-Contest-logs-Archives.git
cd Hamradio-Contest-logs-Archives
git sparse-checkout set --cone WAE
```

Selections can be narrowed to a mode or year:

```sh
git sparse-checkout set --cone WAE/CW/2025
git sparse-checkout set --cone YOTA_Contest/2026/Round_2
```

To retrieve both submitted and reconstructed logs for the same round:

```sh
git sparse-checkout set --cone \
  CQWW/cw/2024 \
  RECONSTRUCTED_LOGS/CQWW/cw/2024
```

`git sparse-checkout set` replaces the current selection. Use
`git sparse-checkout add <directory>` to add another directory and
`git pull --ff-only` to receive later updates. Add `SH6` only when the local
SQLite search index is also needed.

## Download One Log Without Cloning

Every log remains directly accessible through GitHub. For example:

```text
https://github.com/s53zo/Hamradio-Contest-logs-Archives/blob/main/CQWW/cw/2024/2e0cvn.log
https://raw.githubusercontent.com/s53zo/Hamradio-Contest-logs-Archives/main/CQWW/cw/2024/2e0cvn.log
```

Replace the example path with any other log path. Archive log paths are kept
stable because applications use these raw URLs as a public interface.

## Verify And Maintain A Checkout

Useful checks for an updater clone are:

```sh
git status --short
git sparse-checkout list
python3 scripts/shard_index.py audit
du -sh .git SH6 state
```

A ready updater has a clean status, the expected sparse directories, and an SH6
audit ending in `missing=0 extra=0`. Consumer clones normally need only
`git pull --ff-only`. If an updater's `.git` directory grows too large after
many publications, create and verify a fresh sparse clone before retiring the
old checkout; [UPDATER.md](UPDATER.md) documents that migration.

## Contributing Sources

If you know of additional public log sources or missing contests, open an issue
or send a link. Good candidates are sources that publish original logs, UBN
reports, evaluated QSO tables, result JSON, or other public data detailed enough
to recreate Cabrillo-style QSO lines.
