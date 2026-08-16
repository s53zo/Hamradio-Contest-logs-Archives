# Updater state

This directory contains durable, non-secret state shared by sparse updater
clones. It is committed with the logs and SH6 changes that depend on it.

- `downloads/tasks.sqlite` records deterministic provider task-list hashes,
  produced-output counts, and legitimate empty-result counts.
- `reconstruction/ledgers/` records completed reconstruction output and cache
  keys.
- `providers/` stores deterministic provider-specific state.
- `schema.json` identifies canonical state locations.

Cookies, credentials, PID files, active transaction journals, SQLite sidecar
files, and machine-specific paths do not belong here. Local transaction state
is stored under `.git/hcla/`.
