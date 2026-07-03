# Daily restart database tools

These tools are intended for the Daily Restart & Resume flow. They are Bash scripts so cron/systemd callers do not need a Node or TypeScript runtime.

Both tools export this full PATH before doing any work:

```bash
/home/linuxbrew/.linuxbrew/bin:/home/adam/.local/bin:/usr/local/bin:/usr/bin:/bin
```

Both tools also set `umask 077` before creating SQLite outputs or temporary files.

## t3-db-snapshot

```bash
scripts/ops/daily-restart/t3-db-snapshot \
  --db /home/adam/.t3-vps/userdata/state.sqlite \
  --out-dir /home/adam/backups/t3-daily \
  --keep 7
```

Defaults:

- `--db` defaults to `T3DR_DB`, then `/home/adam/.t3-vps/userdata/state.sqlite`.
- `--out-dir` defaults to `T3DR_SNAPSHOT_DIR`, then `/home/adam/backups/t3-daily`.
- `--keep` defaults to `7`.

The snapshot uses `sqlite3`'s online `.backup` command, then runs `PRAGMA integrity_check` against the snapshot. On success it prints the absolute snapshot path to stdout. Diagnostics go to stderr. Any failure exits non-zero.

Snapshots are named `t3-state-<UTC yyyymmdd-HHMMSS>.sqlite`. Retention keeps the newest `--keep` matching snapshots in the output directory.

## t3-db-restore

```bash
scripts/ops/daily-restart/t3-db-restore \
  --snapshot /home/adam/backups/t3-daily/t3-state-20260703-120000.sqlite \
  --db /home/adam/.t3-vps/userdata/state.sqlite
```

Defaults:

- `--db` defaults to `T3DR_DB`, then `/home/adam/.t3-vps/userdata/state.sqlite`.
- `--snapshot` is required.

Precondition: the caller must stop the T3 service before restore. The script warns if WAL or SHM files are present, but it does not try to stop the service.

The restore verifies the snapshot with `PRAGMA integrity_check`, materializes it with SQLite's `.backup` API, moves the current database to `<db>.before-restore.<UTC yyyymmdd-HHMMSS>`, preserves any current WAL/SHM files as matching SQLite companions `<db>.before-restore.<UTC yyyymmdd-HHMMSS>-wal` and `<db>.before-restore.<UTC yyyymmdd-HHMMSS>-shm`, installs the materialized snapshot at `--db` with `0600` permissions, removes stale `<db>-wal` and `<db>-shm` from the active database path, and exits non-zero on any failure. On success it prints the absolute restored database path to stdout.
