# Updater state

This directory contains durable, non-secret state shared by sparse updater
clones. It is committed with the logs and SH6 changes that depend on it.

- `downloads/tasks.sqlite` records deterministic provider task-list hashes,
  produced-output counts, and legitimate empty-result counts.
  UA9 annual scopes fingerprint station IDs and callsigns, not just dates.
  Mini-Test scopes use `WednesdayMiniTest40m/<date>` and
  `WednesdayMiniTest80m/<date>`; legacy year-only records are retained but no
  longer suppress round discovery. Existing remote-only logs remain reusable.
- `reconstruction/ledgers/` records completed reconstruction output and cache
  keys.
- `providers/ok1wc.json` stores OK1WC publication levels by round.
- `providers/vhfmanager/checklogs/<contest_id>/<log_id>.done` records
  VHFManager seed and referenced check logs that have been processed.
- `providers/vhfmanager/hf_parser.json` records completed HF table-parser
  recovery by contest ID, allowing older empty-result markers to be revisited.
- `schema.json` identifies canonical state locations.

Cookies, credentials, PID files, active transaction journals, SQLite sidecar
files, and machine-specific paths do not belong here. Local transaction state
is stored under `.git/hcla/`.
