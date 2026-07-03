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
