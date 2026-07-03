# Daily restart database tools

Cron-safe Bash tools for the Daily Restart & Resume flow. Both export `/home/linuxbrew/.linuxbrew/bin:/home/adam/.local/bin:/usr/local/bin:/usr/bin:/bin`, set `umask 077`, and use locks for overlapping snapshot/restore runs.
Snapshot: `scripts/ops/daily-restart/t3-db-snapshot [--db PATH] [--out-dir DIR] [--keep N]`
Defaults: `--db` uses `T3DR_DB` or `/home/adam/.t3-vps/userdata/state.sqlite`; `--out-dir` uses `T3DR_SNAPSHOT_DIR` or `/home/adam/backups/t3-daily`; `--keep` defaults to `7`.

Uses sqlite3 `.backup`, runs `PRAGMA integrity_check`, prints the absolute `t3-state-<UTC yyyymmdd-HHMMSS>.sqlite` path, and exits non-zero on any failure. Retention always keeps the just-created snapshot.
Restore: `scripts/ops/daily-restart/t3-db-restore --snapshot FILE [--db PATH]`
Default: `--db` uses `T3DR_DB` or `/home/adam/.t3-vps/userdata/state.sqlite`; `--snapshot` is required. Caller must stop T3 first; WAL/SHM presence emits a warning only. Restore verifies/materializes with `.backup`, moves the current DB to `<db>.before-restore.<UTC ts>` with matching `-wal`/`-shm` companions, chmods backups/restored DB to `0600`, removes stale active WAL/SHM paths, prints the restored DB path, and exits non-zero on any failure.
