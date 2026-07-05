# Daily restart operations

This directory contains the daily-restart helpers for capturing active work before
shutdown, injecting resume prompts after the server is available again,
publishing the latest verified client update pointer, and probing post-restart
service health.

## Health probe

`scripts/ops/daily-restart/health-probe` validates the service and starts a
smoke thread to prove provider wake after restart.

```bash
scripts/ops/daily-restart/health-probe \
  --origin http://127.0.0.1:3773 \
  --service t3code.service \
  --instance claudeAgent \
  --model claude-sonnet-5 \
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

Set `--instance`/`--model` (or `T3DR_SMOKE_INSTANCE`/`T3DR_SMOKE_MODEL`) to a
provider and model that are configured on the target service.

## Daily restart orchestrator

`scripts/ops/daily-restart/t3-daily-restart` is the cron-safe restart manager.
It must run outside the T3 service process tree.
Shutdown waits for `systemctl --user stop` with a hard timeout, escalates to
SIGTERM and then SIGKILL for the service cgroup if needed, and does not update
the checkout unless `systemctl --user is-active` reports `inactive` or `failed`.

The manager uses these defaults, all overridable by matching flags:

```text
T3DR_DB=/home/adam/.t3-vps/userdata/state.sqlite
T3DR_ATTACHMENTS_DIR=$(dirname "$T3DR_DB")/attachments
T3DR_CHECKOUT=/home/adam/t3code
T3DR_SERVICE=t3code.service
T3DR_ORIGIN=http://127.0.0.1:3773
T3DR_SNAPSHOT_DIR=/home/adam/backups/t3-daily
T3DR_LEDGER=/home/adam/.openclaw/daily-restart
T3DR_PROBE_TIMEOUT=180
T3DR_SMOKE_INSTANCE=(required)
T3DR_SMOKE_MODEL=(required)
```

The snapshot is a hard gate before shutdown. The manager also validates an
active-thread capture before shutdown, then refreshes that capture again after
the service is stopped and before the quiesced DB snapshot. The refresh is
captured to a candidate file and only replaces the pre-stop manifest after JSON
validation. The post-stop manifest is authoritative: live pending rows come from
the quiesced capture, and shutdown-interrupted active turns are retained by the
capture query only when the quiesced `projection_turns` row is still running.
This prevents a pending request that was picked up while shutdown was in
progress from being replayed as a new turn, and also avoids replaying work that
completed before shutdown finished. If the post-stop refresh fails, the
unchanged service is restarted and the validated pre-stop manifest is injected
instead.
Rollbacks before the updated service can accept writes restore the preflight DB
snapshot after checking out the pre-restart SHA. The manager pins the pre-update
`health-probe` under `$T3DR_LEDGER/<UTC date>/pinned-tools/` before merging the
target SHA, then uses that pinned probe for post-update health checks and
rollback re-probes. If post-start health fails after the updated service was
started, the manager always restores the cycle-start DB snapshot as part of
rollback; the current DB is moved aside by `t3-db-restore`. Result and full logs
are written under `$T3DR_LEDGER/<UTC date>/`.
Set `T3DR_SMOKE_INSTANCE` and `T3DR_SMOKE_MODEL` to the provider/model pair the
health probe should wake. For one-off operator runs, `--smoke-instance` and
`--smoke-model` override those environment defaults.

`--prebuilt-target --rollback-sha SHA --target-sha SHA` is used by the nightly
cycle after it has built the target SHA in a detached staging worktree while the
service was still running. The live checkout stays on the rollback SHA until
shutdown; after the quiesced DB snapshot, the manager fast-forwards the checkout
and promotes the staged web and server dist artifacts. In this mode the manager skips rebuild
work during downtime, but still records the rollback SHA and uses the same
snapshot, stop/start, health, DB restore, and resume-injection rollback path if
the restarted service is not healthy.

## Nightly cycle

`scripts/ops/daily-restart/t3-nightly-cycle` is the 03:00 cron entrypoint. It
runs:

1. VPS backup (`T3DR_BACKUP_CMD`, default `~/.openclaw/bin/t3-vps-backup`).
2. Upstream sync (`T3DR_UPSTREAM_SYNC_CMD`, default `~/.openclaw/bin/t3-upstream-sync`).
3. Build `origin/main` in a detached staging worktree and stage the web dist
   plus server dist payload without mutating the live checkout.
   Targets that are not fast-forwards from the live checkout, or that change
   pnpm install inputs (`package.json`, `pnpm-lock.yaml`, `pnpm-workspace.yaml`,
   `.npmrc`, `pnpmfile.cjs`, or `patches/**`), are rejected before staging so
   new dist code is never started against rollback dependencies.
4. Optional desktop artifact hook. Until T1 lands, this is a safe no-op. The
   one-line integration is `T3DR_DESKTOP_ARTIFACT=1`, which runs
   `dist:desktop:artifact` in the staging worktree with
   `T3CODE_DESKTOP_OUTPUT_DIR=$T3DR_LEDGER/<date>/desktop-artifact`.
5. `t3-daily-restart`, passing the prebuilt target metadata and staged asset
   payload when step 3 built a target. Same-SHA no-op restarts are also pinned
   to the target SHA resolved before staging.
6. Deadline assertion, default `07:00 Europe/London`.

Machine-readable cycle events are appended to
`$T3DR_LEDGER/<UTC date>/t3-nightly-cycle.jsonl`; operator-readable logs and
alerts live in the same run directory.

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
work includes `running`, `starting`, rows with an `active_turn_id`, and live
pending turn-start projections, including starting sessions whose session row was
written after the pending turn-start. Pending-start entries carry the original
user message payload and turn-start metadata so resume injection can replay
unsent work; `waiting` sessions are included for observability.

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
argv. `--attachments-dir` defaults to `T3DR_ATTACHMENTS_DIR` when supplied by the
restart manager. The flag wins for ephemeral tests. `--dry-run` reports without
posting or mutating.

The script calls `POST /api/orchestration/dispatch` with `orchestration:operate`,
first sending `thread.interaction-mode.set` to force the captured mode, then
`thread.turn.start`. Running/interrupted work receives the pinned resume prompt;
captured pending-start work reuses the original pending user message id, text,
attachments, model selection, title seed, and source-plan reference because that
turn has not reached the provider yet. It retries transient dispatch failures
per command (network errors, hung attempts, 5xx, and 429 with bounded
`Retry-After`) for up to five attempts with about one minute of total retry
sleep; non-transient 4xx failures fail immediately. After each successful resume
turn, it writes `injected_at` using temp-file plus rename; retries reuse stable
command IDs derived from the manifest capture and thread so server command
dedupe can catch a lost-response retry.

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

The restart wake gap has an opt-in live-provider harness:

```bash
exports="$(.claude/skills/t3-test-server/scripts/t3-up.sh --name waiting-resume --entry apps/server/src/bin.ts --boot-timeout 240)"
eval "$exports"
T3DR_E2E_LIVE_PROVIDER=1 node scripts/ops/daily-restart/waiting-thread-resume.e2e.mjs \
  --transcript "$T3_HOME/waiting-thread-resume.log"
.claude/skills/t3-test-server/scripts/t3-down.sh waiting-resume
```

Without `T3DR_E2E_LIVE_PROVIDER=1`, the harness exits `75` and writes a
`BLOCKED_ON_LIVE_PROVIDER` transcript line rather than pretending a fixture
verified the live provider path.

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
