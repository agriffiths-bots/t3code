# Daily restart resume injection

`scripts/ops/daily-restart/inject-resume` posts exactly one pinned resume user message to each manifest thread captured as `role:"active"` with `injected_at:null`.

```bash
scripts/ops/daily-restart/inject-resume --manifest resume-manifest.json --origin "${T3DR_ORIGIN:-http://127.0.0.1:3773}" --token "$T3DR_TOKEN"
```

`--origin` defaults to `T3DR_ORIGIN`, then `http://127.0.0.1:3773`. `--token` defaults to `T3DR_TOKEN`, then `T3_TOKEN`; the flag wins for ephemeral tests. `--dry-run` reports without posting or mutating.

The script calls `POST /api/orchestration/dispatch` with `orchestration:operate`, first sending `thread.interaction-mode.set` to force default mode, then `thread.turn.start`. It retries transient dispatch failures per command (network errors, hung attempts, 5xx, and 429 with bounded `Retry-After`) for up to five attempts with about one minute of total retry sleep; non-transient 4xx failures fail immediately. After each successful resume turn, it writes `injected_at` using temp-file plus rename; retries reuse stable command/message IDs derived from the manifest capture and thread so server command dedupe can catch a lost-response retry.

## Token source

Ephemeral mints a scoped admin bearer by running `env -u VITE_DEV_SERVER_URL T3CODE_HOME="$T3_HOME" node "$ENTRY" auth session issue --token-only`.

For live localhost, use the same mechanism against the live server state dir from the orchestrator host, without direct DB access:

```bash
env -u VITE_DEV_SERVER_URL T3CODE_HOME="$T3CODE_HOME" node apps/server/dist/bin.mjs auth session issue --token-only
```

Pass the token as `--token` or `T3DR_TOKEN`. If live runs from source, replace the entry with `apps/server/src/bin.ts`; the invariant is shared `T3CODE_HOME`.

## Integration smoke

Fast unit coverage mocks HTTP. To exercise a disposable real server manually: `.claude/skills/t3-test-server/scripts/t3-ephemeral.sh --boot-timeout 240 -- bash -c 'tmp="$(mktemp -d)"; node scripts/ops/daily-restart/inject-resume.integration.mjs "$tmp"'`. This stays out of the default gate because source boot can take minutes.
