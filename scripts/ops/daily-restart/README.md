```bash
node scripts/ops/daily-restart/capture-active-threads.ts \
  --db "${T3DR_DB:-/home/adam/.t3-vps/userdata/state.sqlite}" \
  --out "$T3DR_LEDGER/$(date -u +%F)/resume-manifest.json"
```

`--exclude THREAD_ID` may be repeated to omit known control/orchestrator threads. `--db` defaults to `T3DR_DB`, then `/home/adam/.t3-vps/userdata/state.sqlite`; `--out` is required. The tool opens the SQLite state DB read-only with `mode=ro` and `PRAGMA query_only = ON`, writes the manifest with a temp-file-plus-rename, prints the active thread count, and exits `0` for an empty capture. Captured active sessions include `running`, `starting`, and rows with an `active_turn_id`; `waiting` sessions are included for observability.

Manifest `pre_sha` and `db_snapshot` are intentionally left empty for the restart orchestrator to fill.
