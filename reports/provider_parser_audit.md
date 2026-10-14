# Provider Parser Audit

Live parser audit completed on 2026-10-14. Publication and GitHub workflow verification are the remaining release gates.

Repository baseline: `71de311a400a564f1b5cdedd6e92e996ed1eba07`.
Evidence workspace: `/tmp/hcla-provider-audit.IHmJNR`.
Production archive and durable ledgers have not been modified by the probes.

The default Python 3.15 fails to import SSL because its OpenSSL 4 shared library is missing. The update shell entrypoint now selects a working versioned interpreter when the default cannot initialize HTTPS; an explicitly selected broken interpreter fails before pulling. Live probes use installed Python 3.13.16 with OpenSSL 3.6.5 and normal certificate verification. Provider credentials are inherited from the environment and excluded from evidence output.

All nine UA9 helpers now reject truncated HTML and terminate stalled buffered reads without reusing a poisoned response. Tests cover complete streamed HTML, premature EOF, and socket timeout for every helper.

| Provider | Discovery/source URL | Parser function | Detected periods | Candidate tasks | Smoke download | Reconstruction | Ledger/state | SH6 | Status | Caveat |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 CQWW (PH/CW) | https://cqww.com/publiclogs/ | `tasks_cqww` | CQWW/ph/2025; CQWW/cw/2025 | 18341 | 2/2 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 42 root ledger records; sparse/state regression tests | 2/2 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 2 CQWPX (PH/CW) | https://cqwpx.com/publiclogs/ | `tasks_cqwpx` | CQWPX/ph/2026; CQWPX/cw/2026 | 13152 | 2/2 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 38 root ledger records; sparse/state regression tests | 2/2 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 3 CQWW RTTY | https://cqwwrtty.com/publiclogs/ | `tasks_cqwwrtty` | CQWWRTTY/2025 | 3250 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 17 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 4 CQ 160 (PH/CW) | https://cq160.com/publiclogs/ | `tasks_cq160` | CQ160/cw/2026; CQ160/ph/2026 | 2858 | 2/2 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 9 root ledger records; sparse/state regression tests | 2/2 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 5 CQWPX RTTY | https://cqwpxrtty.com/publiclogs/ | `tasks_cqwpxrtty` | CQWPXRTTY/2026 | 3016 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 15 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 6 ARRL contests (all) | https://contests.arrl.org/publiclogs.php | `tasks_arrl` | 15 rounds/subcontests; ARRL/arrl_10_ghz_and_up_contest/2025 through ARRL/iaru_hf_world_championship/2025 | 28884 | 15/15 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 119 root ledger records; sparse/state regression tests | 15/15 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 7 ZRS KVP (pomlad/jesen) | http://kvp.hamradio.si/rezultati.html | `tasks_zrs_kvp` | ZRS_KVP/2026/pomlad | 165 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 28 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | PASS | Latest published source periods sampled; not a full historical inventory audit |
| 8 EUHFC (reconstructed from UBN) | https://euhf.s5cc.eu/official-results/ | `tasks_euhfc` | EUHFC/2025 | 2856 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 29 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 9 WAE (CW/SSB/RTTY) | https://dxhf2.darc.de/~waecwlog/user.cgi?fc=req_olog&form=referat&lang=en | `tasks_wae` | WAE/CW/2026; WAE/SSB/2025; WAE/RTTY/2025 | 4649 queued tasks | 3/3 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 30 root ledger records; sparse/state regression tests | 3/3 samples indexed; global missing=0 extra=0 | FIXED | Public-log selector excludes placeholder years; RTTY 2025 restored |
| 10 VHFManager contests | https://vhfmanager.net/modules/results.php?ContestID=556&language=G | `tasks_vhfmanager` | 20 rounds/subcontests; EU_VHF_CONTESTS/zrs_septembrsko_ukv_tekmovanje_unofficial_results_vhfmanager_492 through WW_PMC/ww_pmc_2026_471 | 20 | 100/100 band samples across 20 contests | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 449 root ledger records; sparse/state regression tests | 82/100 samples indexed; global missing=0 extra=0 | FIXED | HF layouts at IDs 476/478 restored; finalized and unofficial results tested |
| 11 Wednesday Mini-Test 40m (UA9QCQ UBN) | https://ua9qcq.com/results_new.php | `tasks_wed_minitest_40m` | 41 rounds/subcontests; WednesdayMiniTest40m/2026-10-14 through WednesdayMiniTest40m/2026-01-07 | 41 | 41/41 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 7 root ledger records; sparse/state regression tests | 40/41 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 12 Russian DX Contest (UA9QCQ UBN) | https://ua9qcq.com/results_new.php | `tasks_rdxc` | RussianDXContest/2026 | 1 queued tasks | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 7 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Complete World Wide response; P3N sample has 2873 parsed QSOs |
| 13 Wednesday Mini-Test 80m (UA9QCQ UBN) | https://ua9qcq.com/results_new.php | `tasks_wed_minitest_80m` | 41 rounds: WednesdayMiniTest80m/2026-10-14 through WednesdayMiniTest80m/2026-01-07 | 41 | 41/41 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 7 root ledger records; sparse/state regression tests | 40/41 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 14 RF Championship CW (UA9QCQ UBN) | https://ua9qcq.com/results_new.php | `tasks_rf_championship_cw` | RFChampionshipCW/2026 | 1 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 9 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 15 Ham Spirit Contest (UA9QCQ UBN) | https://ua9qcq.com/results_new.php | `tasks_ham_spirit` | HamSpiritContest/2025 | 1 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 2 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 17 RCC Cup (UA9QCQ UBN) | https://ua9qcq.com/results_new.php | `tasks_rcc_cup` | RCCCup/2026 | 1 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 9 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 18 RDA Contest (UA9QCQ UBN) | https://ua9qcq.com/results_new.php | `tasks_rda` | RDAContest/2023 | 1 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 6 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 19 Russian Radio Team Championship (UA9QCQ UBN) | https://ua9qcq.com/results_new.php | `tasks_rrtc` | RussianRadioTeamChampionship/2026 | 1 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 9 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 20 Yuri Gagarin International DX Contest (UA9QCQ UBN) | https://ua9qcq.com/results_new.php | `tasks_yuri_gagarin` | YuriGagarinDXContest/2026 | 1 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 8 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 21 Coupe du REF (French HF Championship, CW/SSB) | https://concours.r-e-f.org/logrecus/claimed_hf.php?periode=2026&contest=cdfcw | `tasks_ref` | REF/2026/CW; REF/2026/SSB | 2149 | 2/2 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 34 root ledger records; sparse/state regression tests | 2/2 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 22 EUDX Contest (public logs) | https://www.eudx-contest.com/public-logs/ | `tasks_eudx` | EUDX_contest/2026 | 1975 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 3 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 23 OK Contest (CW/SSB) + OK-OM DX Contest (CW/SSB) + OK DX RTTY Contest | http://okomdx.crk.cz/index.php?page=results | `tasks_okomdx` | OK_Contest/2026/SSB; OK_Contest/2026/CW; OK_OM_DX_Contest/2026/SSB; OK_DX_RTTY_contest/2025; OK_OM_DX_Contest/2025/CW | 2071 queued tasks | 5/5 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 0 root ledger records; sparse/state regression tests | 2/5 samples indexed; global missing=0 extra=0 | FIXED | All five advertised contest/mode families discovered; prior samples excluded from queued count |
| 24 DARC contests (Fieldday/WAG/Ausbildungscontest/CW/RTTY/FT4/Easter/XMAS) | https://dxhf2.darc.de/~fdcwlog/user.cgi?fc=loglist&form=referat&lang=en | `tasks_darc_all` | DARC/Fieldday/CW/2026; DARC/Fieldday/SSB/2026; DARC/WAG/2025; DARC/Ausbildungscontest/2025; DARC/Ausbildungscontest_CW/2026/sep; DARC/Ausbildungscontest_CW/2026/mar; DARC/Ausbildungscontest_CW/2026/jun; DARC/RTTY_Kurzcontest/2026/jan; DARC/RTTY_Kurzcontest/2026/apr; DARC/RTTY_Kurzcontest/2026/jul; DARC/FT4/2026/aug; DARC/FT4/2026/feb; DARC/FT4/2026/mai; DARC/Easter/2026; DARC/XMAS/2025 | 5054 queued tasks | 15/15 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 94 root ledger records; sparse/state regression tests | 15/15 samples indexed; global missing=0 extra=0 | FIXED | 15 published periods, including all quarterly/monthly editions; prior samples excluded from queued count |
| 26 WW DIGI (public logs) | https://ww-digi.com/publiclogs/ | `tasks_wwdigi` | WWDIGI/2026 | 1438 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 8 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 27 SP DX Contest (recreated from public result JSON) | https://spdxcontest.pzk.org.pl/ | `tasks_spdx` | SPDX_contest/2026 | 1927 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 6 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 28 OK1WC Memorial (preliminary/final reference tables) | https://memorial-ok1wc.cz/ | `tasks_ok1wc` | 40 published rounds, 2026-01-05 through 2026-10-05 | 40 rounds (call inventories checked) | 40/40 validated round samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 88 root ledger records; sparse/state regression tests | 40/40 samples indexed; global missing=0 extra=0 | FIXED | Sequential shared-session audit avoids repeated index initialization |
| 29 YU DX Contest (public result QSO tables) | https://yudx.yu1srs.org.rs/api/results/years | `tasks_yudx` | YU_DX_Contest/2025 | 805 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 8 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 30 SAC Scandinavian Activity Contest (public Cabrillo logs) | https://www.sactest.net/publiclogs/ | `tasks_sac` | SAC/CW/2025; SAC/SSB/2025 | 1893 | 2/2 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 8 root ledger records; sparse/state regression tests | 2/2 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 31 URE public logs (EAPSK63/EARTTY/King of Spain/CNCW/CME) | https://concursos.ure.es/en/logs-publicos/ | `tasks_ure` | 6 rounds/subcontests; URE/CME/2025 through URE/SMRSSB/2025 | 3982 | 6/6 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 70 root ledger records; sparse/state regression tests | 6/6 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 32 9A HRS contests (HF Robot public QSO tables) | https://www.hamradio.hr/hfrobot/index.php?k=a | `tasks_hrs_hf` | 9A_HRS_Contest/Kup_Jadrana/2026; 9A_HRS_Contest/Hrvatski_Radioamaterski_Kup/2026; 9A_HRS_Contest/Zimski_KV_Kup/2026 | 232 | 3/3 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 51 root ledger records; sparse/state regression tests | 3/3 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 33 Istra Open Contest (public Cabrillo logs) | https://ioc.9a1p.com/public_logs_2026/ | `tasks_istra_open` | Istra_Open_Contest/2026 | 90 | 1/1 validated samples | 90 source + 18 inverted logs in full fixture | Read-only: 1 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 34 TTC-SPCWC (public checked-log tables) | https://spcwc.pl/ttc/rankings?lang=en | `tasks_ttc_spcwc` | TTC-SPCWC/2026-10-06 | 114 | 1/1 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 12 root ledger records; sparse/state regression tests | 1/1 samples indexed; global missing=0 extra=0 | FIXED | Latest published source periods sampled; not a full historical inventory audit |
| 35 YOTA Contest (public evaluated QSO tables) | https://contest.ham-yota.com/nest/events/list?site=contest.ham-yota.com | `tasks_yota` | YOTA_Contest/2026/Round_2; YOTA_Contest/2026/Round_1 | 926 | 2/2 validated samples | Sample QSO parsing verified; no full provider reconstruction claim | Read-only: 17 root ledger records; sparse/state regression tests | 2/2 samples indexed; global missing=0 extra=0 | PASS | Latest published source periods sampled; not a full historical inventory audit |


Production SH6 audit returned `expected=2227017 indexed=2227017 missing=0 extra=0`. The current full suite passes 181 tests; shell syntax, schema JSON, and `git diff --check` pass.

All 33 registered providers now have successful live samples; every QSO in the 298 stored samples parses with the current reconstruction code. Counts represent isolated queued tasks, not necessarily total station inventory; batch tasks can write multiple files. Representative samples do not prove historical archive completeness.

Confirmed fixes cover complete UA9 responses and territory selection, annual station-inventory cache invalidation, explicit batch output accounting, VHF HF tables and legacy marker recovery, WAE public-year selection, all published DARC editions, all five OK/OKOM/RTTY families, OK1WC session reuse, discovery-error propagation, reconstruction of special callsigns, and previously undiscovered unittest cases.

Production source logs, reconstruction files, SH6 shards, and task state remain unchanged. Publication is pending the remaining audit gates.

## Latest Lifecycle Verification

The current scripts completed the full wrapper lifecycle in an isolated checkout,
publishing only to a local bare test remote:

- Download: 90 Istra Open 2026 source logs, exit 0.
- Reconstruction: 18 generated logs, exit 0.
- Incremental SH6 update and publication: exit 0.
- Fixture path audit: `expected=108 indexed=108 missing=0 extra=0`.
- Latest local and remote test commit: `063b1462a09075ece9f62a33db45f8c62b9e4748`.
- Checkout clean; no interrupted transaction remained.
- Updater dry-run: exit 0; no downloader, state, SH6, commit, or push changes.

This proves the complete lifecycle for the tested Istra batch, not full download
completion for every provider. Other formats have live sample validation and
regression tests; their sampled production paths and root-level ledger records are compared in the companion evidence.

## Verification Gates

- 33/33 provider rows match the registered mapping.
- 298 representative stored logs have all QSO rows parsed; three additional referenced VHFManager logs passed live checks.
- 40/40 OK1WC published rounds and 100 band samples across 20 VHFManager contests passed.
- Latest UA9 40m, 80m, and Russian DX probes passed after station-cache changes.
- Complete unittest suite: 181 tests passed, including sparse remote-only behavior, missing IDs, marker migration, transaction recovery, source failures, and parser regressions.
- Production SH6: `expected=2227017 indexed=2227017 missing=0 extra=0`.
- Shell syntax, schema JSON, compilation, and whitespace validation pass.
- Isolated end-to-end wrapper lifecycle and updater dry-run pass.
- No active upstream blocker remains in the sampled source periods.
- Production audit downloads will not be committed. Only scripts, tests, documentation, schema, and sanitized evidence are proposed for publication.
- GitHub workflow verification follows the release push.

## Known Limits

The audit samples every discovered latest contest/year/mode or round, including all advertised OK families and published DARC editions. It does not download every historical year or prove every live station log is already archived. Provider task counts can represent batches rather than station counts; repeated probes exclude previously saved temporary samples. The RDA selector currently exposes 2023 as its latest available year. Future events without public logs are not treated as parser failures. Missing closing HTML tags in the companion HTTP summary include non-HTML Cabrillo and JSON responses and are not, alone, an error classification.

Annual UA9 cache keys now fingerprint station identities. Mini-Test keys are per round, replacing year-only suppression without deleting legacy ledger rows. Known empty stations use explicit output/empty counts. VHFManager HF recovery is tracked centrally and revisits old markers once. Existing raw GitHub archive paths were not moved.

## Sample Inventory Comparison

The credential-free companion `reports/provider_parser_evidence.json` records all 298 sample paths, QSO counts, discovery scopes, HTTP status summaries, and read-only SH6/ledger observations for 33 providers. Production is globally consistent with its committed tree, but this is not a claim that every live source log is already archived. Unindexed samples are evidence of new or previously unrecognized paths and must be picked up by the normal updater. Counts of ledger records identify persisted state; they do not independently prove all task hashes are fresh.

## Referenced VHFManager Logs

A supplemental live check found that HF QSO tables use CSS classes on their
`table` element. The reference-link extractor previously required a bare table
tag and therefore silently missed these check logs. It now accepts table,
thead, and tbody attributes, with a duplicate-link regression test.

Verified after the fix:

- ContestID 476: 154 referenced calls; S53M sample parsed 198 QSOs.
- ContestID 478: 68 referenced calls; S51DX sample parsed 160 QSOs.
- ContestID 486: 695 referenced calls; IV3VFP sample parsed 38 QSOs.

All three fetched reference pages had complete closing HTML. These supplemental
samples are separate from the 298-sample inventory comparison above.
