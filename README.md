# codex-archive-subagent-threads

A small Rust CLI for finding completed Codex subagent threads, archiving them,
and verifying the resulting state.

The tool selects only child threads recorded as `closed`, still unarchived, and
associated with a named subagent. By default it limits the operation to one
parent thread; `--all-closed-subagents` opts into scanning every parent.

## Install

Requirements:

- Rust 1.85 or newer
- the Codex CLI on `PATH`
- a Codex state database at `$CODEX_HOME/state_5.sqlite` (normally
  `~/.codex/state_5.sqlite`)

```sh
cargo install --git https://github.com/azakharau/codex-archive-subagent-threads
```

## Usage

Preview the threads under the current Codex parent thread:

```sh
CODEX_THREAD_ID=<parent-thread-id> codex-archive-subagent-threads --dry-run
```

Pass the parent explicitly or inspect all completed subagents:

```sh
codex-archive-subagent-threads --parent-thread-id <thread-id> --dry-run
codex-archive-subagent-threads --all-closed-subagents --dry-run --json
```

Remove `--dry-run` to archive the selected threads. Run with `--help` for all
options.

## How it works

1. Opens the local Codex SQLite database read-only and selects candidates.
2. Requests `thread/archive` through `codex app-server`.
3. Reads the database again to verify every selected thread is archived.
4. If app-server fails or leaves a candidate unchanged, updates the matching
   SQLite rows directly in one transaction and verifies once more.

The output reports whether the SQLite fallback was used. `--direct-sqlite-only`
skips app-server entirely.

## Safety

Direct SQLite writes are a compatibility fallback, not a public Codex storage
API. The schema may change between Codex releases. Before a non-dry run:

- close or pause other processes that may write the Codex state database;
- back up `$CODEX_HOME/state_5.sqlite` and its `-wal`/`-shm` files together;
- run `--dry-run` and confirm the exact parent and child thread IDs;
- avoid `--all-closed-subagents` unless cross-parent cleanup is intended.

The fallback only changes `threads.archived` and `threads.archived_at` for the
already selected IDs, inside a transaction. It does not delete thread records.
Restore the database backup if a Codex upgrade makes the schema incompatible.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

## License

MIT
