# Archive updater operations

## Repository model

There is one permanent repository: `s53zo/Hamradio-Contest-logs-Archives`.
Every updater computer uses its own blobless sparse clone of that repository.
The clone keeps scripts, tests, durable state, SH6, workflows, and root files
locally. Source and reconstructed contest logs remain individually accessible
on GitHub but normally have no working-tree copy.

Existing contest log paths are immutable public interfaces. Do not rename,
move, compress, delete, or replace them with pointers.
Explicitly approved recovery may remove proven malformed generated artifacts;
valid canonical contest logs remain unchanged.

## Prerequisites

- Python 3.10 or newer
- Git 2.34 or newer
- GitHub authentication with push access to `main`
- Provider credentials supplied through environment variables, never files
- `UA9QCQ_COOKIE` for UA9QCQ-backed providers

## Fresh computer

Create the computer's only local checkout directly as a blobless sparse clone:

```sh
git clone --depth 1 --filter=blob:none --sparse --single-branch --branch main \
  https://github.com/s53zo/Hamradio-Contest-logs-Archives.git
cd Hamradio-Contest-logs-Archives
git sparse-checkout set --cone --sparse-index .github scripts tests state SH6
git status --short
git sparse-checkout list
python3 scripts/shard_index.py audit
```

The resulting sparse cone is `.github`, `scripts`, `tests`, `state`, and `SH6`.
Root files are included by Git cone mode. This one clone is both the updater
and the local repository; no separate log repository or second permanent clone
is needed.

### Replacing an old full checkout

A full clone already contains the historical log blobs in `.git`, so merely
enabling sparse checkout does not recover that disk space. To migrate, create a
temporary replacement beside the clean, fully pushed old checkout:

```sh
python3 scripts/bootstrap_sparse_clone.py ../Hamradio-Contest-updater \
  --remote https://github.com/s53zo/Hamradio-Contest-logs-Archives.git
cd ../Hamradio-Contest-updater
python3 scripts/shard_index.py audit
```

After verifying the replacement and any computer-specific credentials, retire
the old full checkout. The overlap is only for migration; ongoing operation
uses the new sparse clone alone.

## Normal update

Run the complete latest-year update with one command:

```sh
./scripts/update_last_year_and_push.sh
```

The script runs `git pull --ff-only` and then publishes all providers with
`--last 1`. Additional updater options are passed through, such as
`--workers 8` or `--skip-tests`.

Before pulling, the script verifies that its Python interpreter can load SSL
and create a certificate-verifying HTTPS context. When the default `python3`
has broken SSL dependencies, it tries installed versioned interpreters from
Python 3.15 through 3.10. An explicit `PYTHON_BIN` is always honored and validated.
To select an interpreter directly:

```sh
PYTHON_BIN=/path/to/python3 ./scripts/update_last_year_and_push.sh
```

Its explicit equivalent is:

```sh
git pull --ff-only
python3 scripts/archive_updater.py --dry-run --contests all --last 1
python3 scripts/archive_updater.py --contests all --last 1 --publish
```

Interactive UA9QCQ runs request the session cookie without displaying it. For
unattended runs, inject `UA9QCQ_COOKIE` through the machine's secret manager.
Do not place the value in a command, shell history, or tracked file.

`--dry-run` performs Git/preflight checks and prints the intended scope. It does
not run providers or modify tracked state. The full command performs download,
changed-round reconstruction, incremental SH6 maintenance, README statistics,
tests, scoped staging, a Lore-format commit, a final remote fetch, normal rebase
when conflict-free, a non-force push, remote verification, and sparse cleanup.

Selected providers and history depth:

```sh
python3 scripts/archive_updater.py --contests 12,20,28 --last 1 --workers 8 --publish
python3 scripts/archive_updater.py --contests 30 --last all --publish
```

To inspect the validated commit before publishing, omit `--publish`. The journal
records the local commit; rerun with `--publish` to finish publication.

## Full Historical Recovery

After installing parser fixes, use this one-time command to recover missed
source logs and repair historical reconstruction:

```sh
./scripts/fix_archive_errors_and_push.sh --dry-run
./scripts/fix_archive_errors_and_push.sh --workers 8
```

It pulls the latest code, refuses to overlap another running updater, backs up
misplaced untracked KVP logs and non-HF outputs of known HF contests, checks
all providers across all available years, reconstructs both historical and newly
downloaded rounds, updates SH6 incrementally, tests, commits, and pushes. An
interrupted normal update can be adopted after its processes have stopped.
Recovery journals retain the current stage; rerun the same command after an
interruption without repeating completed stages.
The recovery shell command targets macOS/Linux. Updated archive updaters also
hold an OS-backed checkout lock, released automatically on exit or interruption.

If malformed generated files were already published, recovery stops by default.
After reviewing them, explicitly permit their removal:

```sh
./scripts/fix_archive_errors_and_push.sh --remove-invalid-generated --workers 8
```

This option requires a known HF contest, a misplaced non-HF path, the original
VHFManager generator header, and proof that every QSO has a shifted mode in its
sent-RST field. Files are backed up under `.git/hcla/quarantine/` before their
exact paths are removed from Git and SH6. Their incorrect raw URLs disappear;
valid canonical log paths remain unchanged. Modified or unproven files are
refused. Other source deletions remain prohibited. Backup manifests and consent
are retained in the local transaction journal for interruption recovery.
Stale reconstruction ledgers for those malformed round folders are also backed
up and removed; the discarded sources are excluded from historical processing.

Full history can take hours or days and fetch substantial Git blob data. Temporary source
rounds are released after processing, but Git caches fetched objects. This is a
one-time repair, not the daily update command. Upstream failures stop publication
and retain successful work; recovery cannot manufacture unavailable public logs.
Existing reconstructed files that no longer qualify are not automatically pruned.
VHF reference hashes carry a traversal revision. Changed or incomplete contests
replay marked seeds without rewriting existing valid files; completed current
hashes remain skippable. One VHF contest batch runs at a time, with its station
pool capped by `--workers`, rather than multiplying nested pools.

## KVP Duplicate Recovery

All published ZRS KVP spring/autumn ContestIDs are resolved from the official
season index and owned by the KVP provider at `ZRS_KVP/<year>/<season>/`.
The generic VHFManager provider skips them. Reference links with `QSOID` can
identify another station while retaining the origin's `logID`; traversal now
resolves known seeds and deduplicates referenced-only stations by callsign and
band instead of treating every contact URL as a new log. Invalid HF rows cannot
fall back to fabricated 144 MHz logs.
HF recovery revision 2 revisits previous HF markers once and refreshes stale
generated content at its existing path; byte-identical files are skipped.

If an older run created untracked KVP copies under `EU_VHF_CONTESTS/*_<id>/`,
stop that downloader before cleanup. Preview and then quarantine those files
using an HTTPS-capable Python interpreter:

```sh
python3 scripts/download_vhfmanager_logs.py --quarantine-kvp --dry-run
python3 scripts/download_vhfmanager_logs.py --quarantine-kvp
```

Only untracked misplaced KVP `.log` files are moved. Committed public paths,
canonical KVP logs, and other contests remain untouched. The printed backup
directory under `.git/hcla/quarantine/` preserves files and a manifest locally;
it is not pushed. Resume the normal updater after cleanup.

## Historical reconstruction repair

Normal updates reconstruct only rounds with changed source logs. To apply newer
reconstruction fixes to unchanged historical rounds, use archive root names
(not downloader provider IDs):

```sh
./scripts/repair_historical_reconstruction_and_push.sh \
  --repair-contests CQWW,CQWPX,EU_VHF_CONTESTS
```

Omit `--repair-contests` to cover all committed contest roots. This command pulls
first, skips downloaders, materializes one committed round at a time, replaces
generated reconstructed logs at their existing paths, updates SH6 incrementally,
runs tests, and publishes through the normal transaction workflow. Source logs
and download ledgers are not rewritten. Old reconstructed files that no longer
qualify are retained; this is not a deletion or archive-pruning command.

Temporary source files are removed between rounds, but fetched blobs stay in
Git's object cache. An all-history repair can therefore require substantial
network traffic and disk space; run selected contest roots in batches. The
reconstruction cache skips rounds already repaired with the same source data,
algorithm, and MASTER.DTA. Interrupted repair transactions retain their root
selection; rerun the same command to resume.

For explicit phases without immediate publication:

```sh
python3 scripts/archive_updater.py --repair-reconstruction \
  --repair-contests EU_VHF_CONTESTS --phase reconstruct
python3 scripts/archive_updater.py --phase shards --publish
```

The journal restores repair mode when resuming. Finish an existing normal update
before starting a repair. Historical repair and historical missing-log downloads
are separate operations; use `--last all` with the normal downloader workflow to
recover previously missed source logs first.

## Explicit phases

```sh
python3 scripts/archive_updater.py --phase download --contests all --last 1
python3 scripts/archive_updater.py --phase reconstruct
python3 scripts/archive_updater.py --phase shards --publish
```

The reconstruction phase enumerates exact source paths from the Git tree for
rounds containing changed logs. In a partial clone it batch-fetches only missing
blobs, materializes complete rounds in a temporary directory, writes new
outputs to their unchanged `RECONSTRUCTED_LOGS/...` paths, and removes the
temporary sources automatically.

Direct diagnostic commands are still available:

```sh
python3 scripts/public_logs_downloader.py
python3 scripts/reconstruct_missing_logs.py --changed-only --no-rebuild-shards
python3 scripts/shard_index.py audit
```

### UA9QCQ responses stall after about 16 KB

If every UA9QCQ provider reports an unfinished HTML response while the site
appears to open in a browser, test a large static file:

```sh
curl --max-time 10 --output /dev/null --write-out '%{http_code} %{size_download}\n' \
  https://ua9qcq.com/leaflet.js
```

A successful response is HTTP `200` with `223823` bytes. A timeout near 16 KB
indicates a VPN or tunnel path problem, not an expired UA9QCQ session. With the
consumer Cloudflare WARP client, bypass that host and rerun the test:

```sh
warp-cli tunnel host add ua9qcq.com
```

The exclusion keeps HTTPS encryption but routes this host outside WARP. Remove
it later with `warp-cli tunnel host remove ua9qcq.com` if Cloudflare or UA9QCQ
resolves the transport incompatibility.

If a provider immediately fails with `Connection refused`, check its DNS answer:

```sh
dig +short provider.example
```

Filtering resolvers may return the sinkhole address `0.0.0.0` for a legitimate
contest site. The downloader detects that answer, queries `1.1.1.1` and then
`8.8.8.8` with `dig`, caches the recovered IP, and keeps the original hostname
for HTTPS certificate validation. It reports this as `DNS fallback ...` and
does not require changes to system DNS or `/etc/hosts`.

## Interrupt recovery

On Ctrl-C, wait for `archive_updater.py` to report that the child stopped. The
transaction remains in `.git/hcla/transaction.json`; rerun the same phase or the
normal all-phase command. Atomic `.part` writes prevent partial logs from being
accepted. A resumed transaction removes orphaned updater temp files only from
declared archive/state roots, reuses complete local logs, and updates provider
or reconstruction state only after completed work.

If logs were downloaded by the low-level script before a transaction journal
existed, adopt them explicitly:

```sh
python3 scripts/archive_updater.py --resume-existing --phase reconstruct
python3 scripts/archive_updater.py --phase shards --publish
```

The adoption command rejects invalid logs, deletions, credentials, transient
files, and unrelated working-tree changes.

Generated contest and reconstruction paths normally sit outside the updater's
sparse cone. Publication intentionally stages them with `git add --sparse`;
removing that mode causes Git to reject a valid sparse update after SH6 work has
already completed.

## Concurrent computers

Only start from a clean checkout equal to `origin/main`. Publication fetches the
remote again after the local commit. Disjoint changes are rebased with normal
Git behavior; same-path or binary shard conflicts abort the rebase and leave the
remote untouched. Never use force-push to bypass that stop. Pull and rerun so
the second update is reconstructed and indexed against the newer archive.

## State and secrets

Durable state committed with updates:

```text
state/downloads/tasks.sqlite
state/reconstruction/ledgers/
state/providers/ok1wc.json
state/providers/vhfmanager/checklogs/<contest_id>/<log_id>.done
state/providers/vhfmanager/hf_parser.json
state/schema.json
```

Never commit:

```text
cookies or API credentials
.env files
*.part or *.tmp
*.sqlite-wal, *.sqlite-shm, or *.sqlite-journal
PID/lock files
.git/hcla/
machine-specific absolute paths
```

No-op task/provider updates do not rewrite timestamps or database rows.

## SH6 recovery

Routine maintenance is incremental. Verify all tracked archive paths against
SH6 without downloading log content:

```sh
python3 scripts/shard_index.py audit
```

A full rebuild needs a full clone and is blocked when `core.sparseCheckout` is
enabled:

```sh
python3 scripts/public_logs_downloader.py --rebuild-shards
```

The rebuild creates `.SH6.next`, swaps it only after successful completion, and
restores the previous directory if the swap fails.

## Disk maintenance

After a successful push, the updater runs:

```sh
git sparse-checkout reapply --sparse-index
```

This removes contest folders from the working tree without deleting them from
Git. Git may temporarily report that the sparse index is expanding while
out-of-cone paths are staged; the final cleanup restores the normal sparse
working tree. Locally created blobs remain reachable in `.git`, so periodically
check:

```sh
du -sh .git SH6 state
```

When growth is no longer practical, bootstrap a fresh clone into a new path,
run `python3 scripts/archive_updater.py --dry-run`, and remove the old clone only
after the new one is verified.
