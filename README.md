# Hamradio Contest Logs Archives

This repository collects publicly available amateur radio contest logs in one
place. The archive is intended for analysis, search, education, tooling, and
long-term preservation of contest activity data.

## Current Snapshot

Local snapshot counted on 2026-05-14:

- total log files: 2,161,302
- source/public log files: 1,737,018
- reconstructed mock log files in `RECONSTRUCTED_LOGS/`: 424,284
- parsed `QSO:` lines across all logs: 499,709,606
- parsed `QSO:` lines in source/public logs only: 462,271,062
- unique source/public log callsigns, counted from log filenames: 176,729

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
- OK contest family: `OK_Contest`, `OK_OM_DX_Contest`, `OK_DX_RTTY_contest`
- `DARC` contests: Fieldday, WAG, Ausbildungscontest, Ausbildungscontest CW, RTTY Kurzcontest, FT4, Easter, XMAS
- `WWDIGI`
- `SPDX_contest`
- `OK1WC_Memorial`
- `YU_DX_Contest`
- `SAC`
- `URE`
- `9A_HRS_Contest`

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
OK1WC_Memorial/2026-03-30/OK1ABC.log
RECONSTRUCTED_LOGS/CQWW/cw/2024/K1ABC.log
SH6/logs_00.sqlite
```

`WAE` intentionally remains a top-level contest folder. Other DARC-run contests
are grouped under `DARC/`.

## Available Years By Top-Level Directory

Years are collected recursively from archive directory names under each top-level directory. `RECONSTRUCTED_LOGS` and repo/tooling directories are excluded.

| Top-level directory | Available years | Log files |
|---|---|---:|
| 9A HRS Contest | 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 16,286 |
| ARRL | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 236,363 |
| CQ160 | 2022, 2023, 2024, 2025, 2026 | 13,534 |
| CQWPX | 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 188,235 |
| CQWPXRTTY | 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 45,154 |
| CQWW | 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 298,806 |
| CQWWRTTY | 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 52,519 |
| DARC | 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 36,319 |
| EU VHF CONTESTS | 1980, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2039, 2056, 2057, 2065 | 572,880 |
| EUDX contest | 2023, 2024, 2025 | 5,822 |
| EUHFC | 2001, 2002, 2003, 2004, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2023, 2024, 2025 | 20,860 |
| HamSpiritContest | 2024, 2025 | 1,783 |
| OK1WC Memorial | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 39,017 |
| OK OM DX Contest | 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 18,923 |
| RCCCup | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 949 |
| RDAContest | 2018, 2019, 2020, 2021, 2022, 2023 | 4,123 |
| REF | 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 33,304 |
| RFChampionshipCW | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 2,153 |
| RussianRadioTeamChampionship | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 1,140 |
| SAC | 2021, 2023, 2024 | 6,351 |
| SPDX contest | 2019, 2020, 2021, 2023, 2024, 2025, 2026 | 16,169 |
| URE | 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 49,981 |
| WAE | 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 33,323 |
| WednesdayMiniTest40m | 2026 | 824 |
| WednesdayMiniTest80m | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 11,152 |
| WW PMC | 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 6,170 |
| WWDIGI | 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 10,328 |
| YU DX Contest | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 4,328 |
| YuriGagarinDXContest | 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 7,215 |
| ZRS KVP | 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 3,711 |

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

## Downloading Logs

The main entry point is:

```sh
python3 scripts/public_logs_downloader.py
```

Interactive mode asks which contests to download and how many recent years to
include.

For unattended runs, use `--non-interactive`:

```sh
python3 scripts/public_logs_downloader.py --non-interactive --contests all --last 1
```

Useful examples:

```sh
# Download selected menu items for the most recent year.
python3 scripts/public_logs_downloader.py --non-interactive --contests 28,30,31,32 --last 1

# Download everything with the default adaptive concurrency.
python3 scripts/public_logs_downloader.py --non-interactive --contests all --last all

# Lower concurrency for fragile public servers.
python3 scripts/public_logs_downloader.py --non-interactive --contests all --last 1 --workers 8 --min-workers 2

# Force list rediscovery instead of trusting the task ledger.
python3 scripts/public_logs_downloader.py --non-interactive --contests all --last 1 --no-task-ledger

# Rebuild only the SH6 SQLite shard index.
python3 scripts/public_logs_downloader.py --rebuild-shards
```

The downloader uses a task ledger in `scripts/download_tasks_ledger.sqlite` to
avoid repeating completed source lists. It also validates existing files before
skipping them, so missing, empty, or obvious HTML/error files are retried.

## Reconstructed Logs

For contests where not all stations submitted public logs, the repository can
generate reconstructed mock logs under `RECONSTRUCTED_LOGS/`. These logs infer
QSOs for missing stations from submitted logs.

Important constraints:

- only callsigns present in `MASTER.DTA` are eligible
- a minimum QSO threshold is enforced
- generated files are marked as checklogs
- generated files include SOAPBOX warnings that they are reconstructed mock logs
- reconstructed logs are included in SH6 shards when shards are rebuilt

Run reconstruction with:

```sh
python3 scripts/reconstruct_missing_logs.py
```

## SH6 SQLite Shards

`SH6/` contains SQLite shard indexes used by the SH6 web client:

https://s53m.com/SH6/

Each `logs_XX.sqlite` file is keyed by a stable callsign hash so the browser can
download only the relevant shard. Rebuild shards after significant download or
reconstruction runs:

```sh
python3 scripts/public_logs_downloader.py --rebuild-shards
```

## Cloning A Subset

The full repository is large. Use sparse checkout if you only need part of it:

```sh
git clone --filter=blob:none --sparse https://github.com/s53zo/Hamradio-Contest-logs-Archives.git
cd Hamradio-Contest-logs-Archives
git sparse-checkout set WAE/CW
```

Replace `WAE/CW` with any contest folder or subfolder you need.

## Publishing

This archive is served directly from GitHub.

## Contributing Sources

If you know of additional public log sources or missing contests, open an issue
or send a link. Good candidates are sources that publish original logs, UBN
reports, evaluated QSO tables, result JSON, or other public data detailed enough
to recreate Cabrillo-style QSO lines.
