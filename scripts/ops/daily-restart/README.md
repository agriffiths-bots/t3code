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

## `t3-daily-restart`

`t3-daily-restart` is the cron-safe restart orchestrator. It must run outside
the T3 service process tree.

The manager uses these defaults, all overridable by matching flags:

```text
T3DR_DB=/home/adam/.t3-vps/userdata/state.sqlite
T3DR_CHECKOUT=/home/adam/t3code
T3DR_SERVICE=t3code.service
T3DR_ORIGIN=http://127.0.0.1:3773
T3DR_SNAPSHOT_DIR=/home/adam/backups/t3-daily
T3DR_LEDGER=/home/adam/.openclaw/daily-restart
```

The snapshot is a hard gate before shutdown. On post-start health failure the
manager stops the service, checks out the pre-restart SHA, restores the DB
snapshot, starts the service, and probes again. Result and full logs are written
under `$T3DR_LEDGER/<UTC date>/`.

The database snapshot/restore, active-thread capture, and resume-injection
helpers are built in sibling slices. Until those implementations land, this
directory includes contract shims that fail before shutdown unless pointed at
real helpers with `T3DR_REAL_T3_DB_SNAPSHOT`, `T3DR_REAL_T3_DB_RESTORE`,
`T3DR_REAL_CAPTURE_ACTIVE_THREADS`, and `T3DR_REAL_INJECT_RESUME`.
