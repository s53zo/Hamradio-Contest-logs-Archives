# SH6 SQLite Shards

This folder contains the SQLite shard indexes used by the SH6 client.
Each `logs_XX.sqlite` file is a shard keyed by a stable hash of the callsign,
so the browser can download only one small shard via HTTP Range requests and
run fast lookups locally.

Callsigns are stored in uppercase. Clients should normalize user input to
uppercase before computing the shard bucket and querying.

The data is used by the SH6 web app:
https://s53zo.github.io/SH6/index.html

## Current Data

Generated from the checked-in shard files on 2026-07-01.

| Metric | Value |
| --- | ---: |
| SQLite shard files | 256 |
| Indexed log entries | 2,183,576 |
| Unique indexed callsigns | 182,959 |
| Contest roots | 33 |
| Total shard size | 216,043,520 bytes |
| Parsed year range | 1980-2065 |

The parsed year range comes from repository path metadata. A small number of
EU VHF Manager paths include future-looking IDs such as `ZRS_March_2065`;
those values are preserved as source metadata, not normalized here.

## Largest Contest Roots

| Contest root | Indexed logs |
| --- | ---: |
| EU_VHF_CONTESTS | 581,238 |
| CQWW | 413,732 |
| ARRL | 328,540 |
| CQWPX | 272,636 |
| CQWWRTTY | 68,465 |
| CQWPXRTTY | 58,598 |
| URE | 58,424 |
| WAE | 48,747 |
| DARC | 48,631 |
| OK1WC_Memorial | 47,100 |

## Latest Indexed Data By Contest

| Contest root | Latest year | Logs in latest year |
| --- | ---: | ---: |
| 9A_HRS_Contest | 2026 | 181 |
| ARRL | 2026 | 14,883 |
| CQ160 | 2026 | 3,772 |
| CQWPX | 2026 | 16,809 |
| CQWPXRTTY | 2025 | 3,860 |
| CQWW | 2025 | 23,540 |
| CQWWRTTY | 2025 | 3,941 |
| DARC | 2026 | 1,173 |
| EUDX_contest | 2025 | 2,349 |
| EUHFC | 2025 | 1,884 |
| EU_VHF_CONTESTS | 2065 | 2 |
| HamSpiritContest | 2025 | 804 |
| Istra_Open_Contest | 2026 | 107 |
| OK1WC_Memorial | 2026 | 4,382 |
| OK_OM_DX_Contest | 2026 | 383 |
| RCCCup | 2026 | 241 |
| RDAContest | 2023 | 1,124 |
| REF | 2026 | 2,511 |
| RFChampionshipCW | 2026 | 269 |
| RussianDXContest | 2025 | 1,318 |
| RussianRadioTeamChampionship | 2025 | 357 |
| SAC | 2025 | 2,549 |
| SPDX_contest | 2026 | 2,130 |
| TTC-SPCWC | 2026 | 353 |
| URE | 2025 | 4,645 |
| WAE | 2025 | 5,212 |
| WWDIGI | 2025 | 2,082 |
| WW_PMC | 2026 | 659 |
| WednesdayMiniTest40m | 2026 | 1,238 |
| WednesdayMiniTest80m | 2026 | 771 |
| YU_DX_Contest | 2025 | 936 |
| YuriGagarinDXContest | 2025 | 1,198 |
| ZRS_KVP | 2026 | 171 |
