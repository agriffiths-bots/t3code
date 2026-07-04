# Daily Restart & Resume

Utilities in this directory support the daily T3 restart flow. They are meant
to run from cron or test harnesses outside the T3 service process tree.

## `health-probe`

```bash
scripts/ops/daily-restart/health-probe \
  --origin http://127.0.0.1:3773 \
  --service t3code.service \
  --timeout 120
```

The probe prints one machine-readable line per check:

```text
CHECK systemd PASS active
CHECK http PASS 200
CHECK spawn_wake PASS completed thread=...
```

It exits zero only when all checks pass. `T3_TOKEN` may be supplied by tests or
ephemeral harnesses. Otherwise the probe mints a short-lived local session from
`T3DR_CHECKOUT` and `T3DR_DB`/`T3CODE_HOME`, stores it only in a private temp
file for the smoke child, and revokes the session after cleanup.

## `capture-active-threads.ts`

```bash
node scripts/ops/daily-restart/capture-active-threads.ts \
  --db "${T3DR_DB:-/home/adam/.t3-vps/userdata/state.sqlite}" \
  --out "$T3DR_LEDGER/$(date -u +%F)/resume-manifest.json"
```

`--exclude THREAD_ID` may be repeated to omit known control/orchestrator threads. `--db` defaults to `T3DR_DB`, then `/home/adam/.t3-vps/userdata/state.sqlite`; `--out` is required. The tool opens the SQLite state DB read-only with `mode=ro` and `PRAGMA query_only = ON`, writes the manifest with a temp-file-plus-rename, prints the active thread count, and exits `0` for an empty capture. Captured active sessions include `running`, `starting`, and rows with an `active_turn_id`; `waiting` sessions are included for observability.

Manifest `pre_sha` and `db_snapshot` are intentionally left empty for the restart orchestrator to fill.
