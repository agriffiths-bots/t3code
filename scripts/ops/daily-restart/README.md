```bash
node scripts/ops/daily-restart/capture-active-threads.ts \
  --db "${T3DR_DB:-/home/adam/.t3-vps/userdata/state.sqlite}" \
  --out "$T3DR_LEDGER/$(date -u +%F)/resume-manifest.json"
```

`--exclude THREAD_ID` may be repeated to omit known control/orchestrator threads. `--db` defaults to `T3DR_DB`, then `/home/adam/.t3-vps/userdata/state.sqlite`; `--out` is required. The tool opens the SQLite state DB read-only with `mode=ro` and `PRAGMA query_only = ON`, writes the manifest with a temp-file-plus-rename, prints the active thread count, and exits `0` for an empty capture. Captured active sessions include `running`, `starting`, and rows with an `active_turn_id`; `waiting` sessions are included for observability.

Manifest `pre_sha` and `db_snapshot` are intentionally left empty for the restart orchestrator to fill.

# Daily restart database tools: Bash/cron-safe `t3-db-snapshot [--db PATH] [--out-dir DIR] [--keep N]` and `t3-db-restore --snapshot FILE [--db PATH]`; defaults are `T3DR_DB` or `/home/adam/.t3-vps/userdata/state.sqlite`, `T3DR_SNAPSHOT_DIR` or `/home/adam/backups/t3-daily`, and `--keep 7`; both export the full cron PATH, set `umask 077`, use sqlite3 `.backup` plus `PRAGMA integrity_check`, print only the result path to stdout, and fail non-zero; restore caller must stop T3 first and WAL/SHM presence is warning-only.
