# Daily restart operations

This directory contains the daily-restart helpers for capturing active work before
shutdown, injecting resume prompts after the server is available again, and
publishing the latest verified client update pointer.

## Capture active threads

`scripts/ops/daily-restart/capture-active-threads.ts` writes a restart manifest
from the T3 SQLite projection state:

```bash
node scripts/ops/daily-restart/capture-active-threads.ts \
  --db "${T3DR_DB:-/home/adam/.t3-vps/userdata/state.sqlite}" \
  --out "$T3DR_LEDGER/$(date -u +%F)/resume-manifest.json"
```

`--exclude THREAD_ID` may be repeated to omit known control/orchestrator threads.
`--db` defaults to `T3DR_DB`, then `/home/adam/.t3-vps/userdata/state.sqlite`;
`--out` is required. The tool opens the SQLite state DB read-only with `mode=ro`
and `PRAGMA query_only = ON`, writes the manifest with a temp-file-plus-rename,
prints the active thread count, and exits `0` for an empty capture. Captured active
sessions include `running`, `starting`, and rows with an `active_turn_id`;
`waiting` sessions are included for observability.

Manifest `pre_sha` and `db_snapshot` are intentionally left empty for the restart
orchestrator to fill.

## Resume injection

`scripts/ops/daily-restart/inject-resume` posts exactly one pinned resume user
message to each manifest thread captured as `role:"active"` with
`injected_at:null`.

```bash
T3DR_TOKEN="$T3DR_TOKEN" scripts/ops/daily-restart/inject-resume --manifest resume-manifest.json --origin "${T3DR_ORIGIN:-http://127.0.0.1:3773}"
```

`--origin` defaults to `T3DR_ORIGIN`, then `http://127.0.0.1:3773`. `--token`
defaults to `T3DR_TOKEN`, then `T3_TOKEN`; avoid passing live bearer tokens via
argv. The flag wins for ephemeral tests. `--dry-run` reports without posting or
mutating.

The script calls `POST /api/orchestration/dispatch` with `orchestration:operate`,
first sending `thread.interaction-mode.set` to force default mode, then
`thread.turn.start`. It retries transient dispatch failures per command (network
errors, hung attempts, 5xx, and 429 with bounded `Retry-After`) for up to five
attempts with about one minute of total retry sleep; non-transient 4xx failures
fail immediately. After each successful resume turn, it writes `injected_at`
using temp-file plus rename; retries reuse stable command/message IDs derived
from the manifest capture and thread so server command dedupe can catch a
lost-response retry.

## Token source

Ephemeral mints a scoped admin bearer by running
`env -u VITE_DEV_SERVER_URL T3CODE_HOME="$T3_HOME" node "$ENTRY" auth session issue --token-only`.

For live localhost, use the same mechanism against the live server state dir from
the orchestrator host, without direct DB access:

```bash
env -u VITE_DEV_SERVER_URL T3CODE_HOME="$T3CODE_HOME" node apps/server/dist/bin.mjs auth session issue --token-only
```

Pass the token as `--token` or `T3DR_TOKEN`. If live runs from source, replace
the entry with `apps/server/src/bin.ts`; the invariant is shared `T3CODE_HOME`.

## Database snapshots

`scripts/ops/daily-restart/t3-db-snapshot [--db PATH] [--out-dir DIR] [--keep N]`
creates an online-safe SQLite snapshot. `--db` defaults to `T3DR_DB`, then
`/home/adam/.t3-vps/userdata/state.sqlite`; `--out-dir` defaults to
`T3DR_SNAPSHOT_DIR`, then `/home/adam/backups/t3-daily`; `--keep` defaults to
`7`.

`scripts/ops/daily-restart/t3-db-restore --snapshot FILE [--db PATH]` restores a
verified snapshot. The caller must stop T3 first; active WAL/SHM files emit a
warning only. Both tools export a cron PATH with trusted system directories first,
set `umask 077`, use sqlite3
`.backup` plus `PRAGMA integrity_check`, print only the result path to stdout,
and exit non-zero on failure. Both tools must run as the existing database owner;
root callers should drop privileges to that uid before invoking them. Snapshot
staging stays inside the snapshot directory so the published file is installed
with a same-filesystem rename.

## Integration smoke

Fast unit coverage mocks HTTP. To exercise a disposable real server manually:
`.claude/skills/t3-test-server/scripts/t3-ephemeral.sh --boot-timeout 240 -- bash -c 'tmp="$(mktemp -d)"; node scripts/ops/daily-restart/inject-resume.integration.mjs "$tmp"'`.
This stays out of the default gate because source boot can take minutes.

## Verified client pointer

Client update agents poll branch `client-verified-latest` / file
`client-verified-latest.json`.

```bash
node scripts/ops/daily-restart/update-verified-pointer.ts --sha "$SHA"
```

Omit `--sha` to fetch and verify `origin/main`; non-hex `--sha` values are
resolved locally with `git rev-parse`. Pass `--dry-run` or `--out FILE`. Final stdout:
`VERIFIED <sha>` or `NOT-VERIFIED <reason>`. Failures exit non-zero and never
overwrite the previous pointer.

Gate: `Windows Launch Smoke` must conclude `success`, and a product release for
the SHA must contain Windows installer/blockmap/manifest assets. The pointer
uses a dedicated branch instead of release asset replacement so failed writes
leave the previous file commit in place.

Schema: `{"version":1,"sha","verified_at","desktop":{"ready":true,"artifacts":{"release","windows":{"installer","blockmap","manifest"}},"launch_smoke":"success","checks":{...},"linux":{"ready":"unknown","launch_smoke":"unknown","artifact":"unknown","reason"}},"mobile":{"ready":"unknown","ota":"unknown","apk"?:url,"reason"}}`.

Linux and mobile OTA are `unknown`: current workflows publish Windows/APK assets
but no main-queryable Linux release artifact or Expo OTA update.

Poll:

```bash
gh api 'repos/agriffiths-bots/t3code/contents/client-verified-latest.json?ref=client-verified-latest' --jq .content | base64 -d
```
