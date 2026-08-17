# Archive updater operations

## Repository model

There is one permanent repository: `s53zo/Hamradio-Contest-logs-Archives`.
Every updater computer uses its own blobless sparse clone of that repository.
The clone keeps scripts, tests, durable state, SH6, workflows, and root files
locally. Source and reconstructed contest logs remain individually accessible
on GitHub but normally have no working-tree copy.

Existing contest log paths are immutable public interfaces. Do not rename,
move, compress, delete, or replace them with pointers.

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
