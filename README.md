# Hamradio Contest Logs Archive

Public amateur-radio contest logs and searchable SQLite indexes.
[Browse contest folders](https://github.com/s53zo/Hamradio-Contest-logs-Archives/tree/main)
or visit the [archive website](https://s53zo.github.io/Hamradio-Contest-logs-Archives/).

## Contents

| Location | Contents |
| --- | --- |
| Contest folders | Provider-published logs and provider-derived recreations, organized by contest, year, mode or round. Existing paths are preserved. |
| `RECONSTRUCTED_LOGS/` | Derived station logs reconstructed from contacts in other logs, not original submissions. |
| `SH6/` | SQLite metadata indexes linking callsigns to individual archive log paths. |

Formats include Cabrillo-like `.log`/`.cbr` and ADIF `.adi` files. Coverage
and exchange fields depend on each source. Reconstructed and recreated logs
can be incomplete or inconsistent; they are not proof of an original submission
or an independently verified contact.

## Download

Download individual files using GitHub's Raw link. Existing raw URLs retain:

```text
https://raw.githubusercontent.com/s53zo/Hamradio-Contest-logs-Archives/main/<archive-path>
```

For indexes only:

```sh
git clone --filter=blob:none --sparse --single-branch --branch main \
  https://github.com/s53zo/Hamradio-Contest-logs-Archives.git
cd Hamradio-Contest-logs-Archives
git sparse-checkout set --cone SH6
```

Add one contest/year with `git sparse-checkout add CQWW/cw/2025`.
For a full working copy, use normal `git clone` without `--sparse` or
`--filter`. Millions of files require substantial space and download time.
Update a clean checkout with `git pull --ff-only`.

## SH6 Format

The 256 shards are `SH6/logs_00.sqlite` through `SH6/logs_ff.sqlite`.
They contain metadata, not embedded log contents. Each `path` is relative
to this repository's root and identifies the log to download.

The `logs` table contains:

```text
path TEXT, callsign TEXT, contest TEXT, year INTEGER,
mode TEXT, season TEXT, subcontest TEXT, detail TEXT
```

Paths are unique; callsigns are indexed. The shard bucket is
`CRC32(uppercase ASCII callsign) & 255`, formatted as two hexadecimal digits.
For example, `S53M` belongs to `logs_36.sqlite`:

```sh
sqlite3 SH6/logs_36.sqlite \
  "SELECT path, contest, year, mode FROM logs WHERE callsign = 'S53M';"
```

Shard filenames are also available on the archive website. This public
repository contains data and documentation only. Scraping, reconstruction,
operational state and publishing automation are maintained separately.
Earlier public tooling remains in Git history; history has not been rewritten.

## Provenance And Reuse

Source providers, log authors and contest organizers may retain rights in
their material. Availability here does not grant a blanket reuse license.
Respect source terms and attribution requirements. Report errors or removal
requests through this repository's Issues.

## Archive Snapshot

<!-- STATS:START -->
SH6-indexed snapshot counted on 2026-11-05:

- total indexed log files: 2,494,380
- source/public indexed log files: 2,020,811
- reconstructed mock log files in `RECONSTRUCTED_LOGS/`: 473,569
- unique source/public callsigns in the SH6 index: 184,137
- contest roots in the SH6 index: 39
- SQLite shard files in `SH6/`: 256
<!-- STATS:END -->

## Coverage By Year

<!-- YEARS:START -->
Years are collected from SH6 index metadata derived from archive paths.
Source/public logs and reconstructed logs are included; repo/tooling
directories are not indexed.

| Top-level directory | Available years | Indexed logs |
|---|---|---:|
| 9A HRS Contest | 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 21,687 |
| ARRL | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 331,419 |
| CQ160 | 2022, 2023, 2024, 2025, 2026 | 17,838 |
| CQWPX | 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 272,647 |
| CQWPXRTTY | 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 62,174 |
| CQWW | 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 413,732 |
| CQWWRTTY | 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 68,465 |
| DARC | 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 50,469 |
| EU VHF CONTESTS | 1980, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2039, 2056, 2057, 2065 | 619,828 |
| EUDX contest | 2023, 2024, 2025, 2026 | 9,326 |
| EUHFC | 2001, 2002, 2003, 2004, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2023, 2024, 2025 | 27,342 |
| HamSpiritContest | 2024, 2025 | 1,783 |
| Istra Open Contest | 2026 | 107 |
| OK OM DX Contest | 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 25,238 |
| OK1WC Memorial | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 50,071 |
| OK_Contest | 2026 | 61 |
| OK_DX_RTTY_contest | 2025 | 1,043 |
| RCCCup | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 3,377 |
| RDAContest | 2018, 2019, 2020, 2021, 2022, 2023 | 8,115 |
| REF | 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 41,552 |
| RFChampionshipCW | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 2,488 |
| RSGB_HF | 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024, 2025, 2026 | 82,052 |
| RSGB_VHF | 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024, 2025, 2026 | 145,868 |
| RussianDXContest | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 10,755 |
| RussianRadioTeamChampionship | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 3,883 |
| SAC | 2021, 2023, 2024, 2025 | 11,330 |
| SPDX contest | 2019, 2020, 2021, 2023, 2024, 2025, 2026 | 18,880 |
| TTC-SPCWC | 2026 | 2,217 |
| URE | 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 58,424 |
| WAE | 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 50,708 |
| WRTC | 2018, 2026 | 3,216 |
| WW PMC | 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 8,818 |
| WWDIGI | 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 15,190 |
| WednesdayMiniTest40m | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 4,911 |
| WednesdayMiniTest80m | 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 14,219 |
| YOTA_Contest | 2021, 2022, 2023, 2024, 2025, 2026 | 14,102 |
| YU DX Contest | 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025 | 5,165 |
| YuriGagarinDXContest | 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 11,858 |
| ZRS KVP | 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026 | 4,022 |
<!-- YEARS:END -->
