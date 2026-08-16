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

Run the bootstrap from any existing checkout containing the script:

```sh
python3 scripts/bootstrap_sparse_clone.py ../Hamradio-Contest-updater \
  --remote https://github.com/s53zo/Hamradio-Contest-logs-Archives.git
cd ../Hamradio-Contest-updater
```

The resulting sparse cone is `.github`, `scripts`, `tests`, `state`, and `SH6`.
Root files are included by Git cone mode. The bootstrap fails rather than
touching an existing destination and reports the number of remote log paths it
can enumerate without checking out their blobs.

## Normal update

```sh
git pull --ff-only
export UA9QCQ_COOKIE='session-cookie-when-needed'
python3 scripts/archive_updater.py --dry-run --contests all --last 1
python3 scripts/archive_updater.py --contests all --last 1 --publish
```

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

The reconstruction phase uses `git archive` to materialize only complete source
rounds containing changed logs in a temporary directory. It writes new outputs
to their unchanged `RECONSTRUCTED_LOGS/...` paths and removes the temporary
sources automatically.

Direct diagnostic commands are still available:

```sh
python3 scripts/public_logs_downloader.py
python3 scripts/reconstruct_missing_logs.py --changed-only --no-rebuild-shards
python3 scripts/shard_index.py audit
```

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
Git. Locally created blobs remain reachable in `.git`, so periodically check:

```sh
du -sh .git SH6 state
```

When growth is no longer practical, bootstrap a fresh clone into a new path,
run `python3 scripts/archive_updater.py --dry-run`, and remove the old clone only
after the new one is verified.
