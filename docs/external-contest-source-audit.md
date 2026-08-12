# External contest source audit

Audit date: 2026-08-12  
Calendar window: 2025-08-01 through 2026-08-31

The source calendar was the UA9QCQ monthly calendar, but this audit deliberately
looked for log data on organizer and third-party sites outside the UA9QCQ result
database. The raw calendar export is stored in
`reports/ua9qcq-calendar-2025-08_to_2026-08.tsv`.

## Result

The calendar export contains 1,632 event instances. After grouping recurring
events and comparing them with the downloader's existing providers, one new
external source was confirmed to expose complete public QSO records.

| Contest | External source | Public history | Decision |
| --- | --- | --- | --- |
| YOTA Contest | `contest.ham-yota.com` evaluated-results API | 2021-present, three rounds per year | Implemented as provider 35; downloads every public round and converts evaluated QSO JSON to Cabrillo |

## Sources inspected but not imported

These sites were checked because their contests occur in the calendar window.
They do not currently provide an enumerable public collection of complete logs.

| Contest family | Site inspected | Public material found | Decision |
| --- | --- | --- | --- |
| Makrothen RTTY | `pl259.org` | Callsigns received and per-call LCRs; LCRs contain summaries plus rejected QSOs only | Reject: incomplete QSO data |
| NAQP and NA Sprint | `ncjweb.com` | Logs-received callsigns and results | Reject: no public log bodies |
| NCCC Sprint, NCCC FT4 Sprint, CWT, K1USN SST | Organizer pages and `3830scores.com` | Rules and self-reported scores | Reject: scores are not logs |
| RSGB HF contests | `rsgbcc.org` | Results and adjudication material | Reject: no public complete-log archive found |
| UBA, PACC and Helvetia | Organizer/result sites | Results, scoreboards, or selected QSO views | Reject: complete submitted logs could not be enumerated |
| Oceania DX and commonwealth contests | Organizer/result sites | Results and reports | Reject: no public complete-log archive found |
| JARTS, JIDX and KCJ | Organizer sites | Received-log lists, results, and some check reports | Reject: no complete public QSO collection found |
| CQ-M, UN DX, Russian WW Digital | Organizer/result sites | Results and check-report pages | Reject: complete logs could not be verified outside UA9QCQ |
| State and regional QSO parties | Organizer sites | Results and received-log lists | Reject: no consistent public raw-log source found |
| BARTG, SARTG and other RTTY series | Organizer sites | Results, certificates, or check reports | Reject: no enumerable complete-log archive found |

## Acceptance rule

A provider is added only when the external source exposes all QSO rows for each
public station log and the station set can be enumerated by contest and year.
Calls-received lists, claimed scores, scoreboards, certificates, aggregate
results, and LCRs containing only bad QSOs are intentionally excluded.

The audit can be repeated with:

```sh
python3 scripts/audit_ua9qcq_calendar.py \
  --start 2025-08 --end 2026-08 > reports/ua9qcq-calendar-2025-08_to_2026-08.tsv
```
