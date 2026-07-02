---
name: t3-test-server
description: Boot a throwaway/ephemeral T3 server with an isolated state DB, drive it over the Environment HTTP API, assert against its SQLite projections, then tear it down. Use to validate T3 harness changes (schedule/spawn/thread routing, wait/resume, scheduler) WITHOUT touching the live server or the unreliable live MCP tools.
---

# T3 ephemeral test server

Use this whenever you need to verify a change to the T3 server/harness (MCP tools,
scheduler, provider routing, session lifecycle) with **real** behaviour, but must
NOT use the live server or the live `t3_*` MCP tools (they are unreliable). It
boots a disposable server on a loopback port with its own temp `T3CODE_HOME`
(SQLite DB), runs your command against it, and always tears it down.

## One-command lifecycle

`scripts/t3-ephemeral.sh` boots the server, mints a scoped admin bearer, exports
connection env vars, runs your command, and cleans up on exit (kills the server +
`rm -rf` the temp home — even on error, via a trap).

```bash
scripts/t3-ephemeral.sh -- <command...>
```

Your command sees:

| env         | meaning                                                                  |
| ----------- | ------------------------------------------------------------------------ |
| `T3_ORIGIN` | `http://127.0.0.1:<port>` (Environment HTTP API)                         |
| `T3_TOKEN`  | bearer with `orchestration:operate`                                      |
| `T3_DB`     | the ephemeral server's state SQLite path, discovered at boot (read-only) |
| `T3_HOME`   | the temp `T3CODE_HOME`                                                   |
| `T3_PORT`   | the chosen loopback port                                                 |

Flags: `--entry PATH` (default `apps/server/dist/bin.mjs`; use
`apps/server/src/bin.ts` to run from source), `--boot-timeout SECS` (default 45;
use ~180 for the source entry, which type-strips on boot and is slow).

## Persistent lifecycle (multi-step / multi-agent testing)

When the server must outlive a single command (e.g. a repro subthread driving
the web UI with Playwright — see the `t3-e2e-testing` skill), use the up/down
pair instead:

```bash
# From the repo root:
exports="$(.claude/skills/t3-test-server/scripts/t3-up.sh --name MY-TASK)" \
  || exit 1                                             # errors stay on stderr
eval "$exports"                                         # exports the T3_* vars
# ... any number of commands/agents; later shells can reload the env with:
#     . ~/.cache/t3-ephemeral/instances/MY-TASK/instance.env
.claude/skills/t3-test-server/scripts/t3-down.sh MY-TASK  # ALWAYS tear down
.claude/skills/t3-test-server/scripts/t3-down.sh --all    # sweep leaked instances
```

Same isolation guarantees (fresh temp home, loopback-only, ports 13910–13940).
`instance.env` additionally records `T3_PID` and `T3_ENTRY`. The web UI is
served at `$T3_ORIGIN`; authenticate a browser via `e2e/ui.mjs` (`openT3()`),
which mints a one-time pairing token and lands on the authed app.

## Testing LOCAL source changes

`dist/bin.mjs` is a build artifact — rebuild it so the ephemeral server runs your
edits, then use the default entry:

```bash
vp run build:desktop        # rebuilds the server bundle (among others)
scripts/t3-ephemeral.sh -- <command...>
```

or run from source directly (slower): `--entry apps/server/src/bin.ts --boot-timeout 180`.

## Drivers & assertions (reuse the repo `e2e/` harness)

- `e2e/drive.mjs` — create a project + thread bound to a chosen provider instance
  and model, dispatch a user turn, then poll projections until it settles (auth
  via `T3_TOKEN`). Example:
  ```bash
  # bash -c so $T3_ORIGIN/$T3_DB expand in the child shell (they are exported
  # INSIDE the wrapper). drive.mjs needs its --workspace to exist, so create it.
  scripts/t3-ephemeral.sh -- bash -c 'mkdir -p /tmp/t3-eph-proj && node e2e/drive.mjs \
    --origin "$T3_ORIGIN" --db "$T3_DB" --workspace /tmp/t3-eph-proj \
    --instance claudeAgent --model claude-sonnet-4-6 --prompt "reply READY"'
  ```
- `e2e/assert.mjs` — read-only SQLite helpers (`openState`, `turnCountForThread`,
  `childrenOf`, `scheduledTask`, `listScheduledTasks`, `threadShell`,
  `assistantMessages`) over `T3_DB`.
- `e2e/poll.mjs` / `e2e/poll-wake.mjs` / `e2e/fib-sleep.sh` — polling + a
  long-running child for wait/resume scenarios.

### Validation modes

1. **Persisted-state (no live model needed)** — dispatch `project.create` /
   `thread.create` / session commands (`ClientOrchestrationCommand`) over
   `POST /api/orchestration/dispatch`, then assert the persisted rows from
   `T3_DB` (`projection_threads`, `scheduled_tasks`, …). Note this stores the
   `modelSelection` you send **verbatim** — it does not run provider-routing
   inference, and the `t3_*` MCP tools are invoked over MCP, not this endpoint.
2. **Provider-routing inference (Fix 1/2)** — the model→provider inference lives
   in the shared resolver + MCP tool handlers, so unit-test the resolver directly
   (`vp run test`) and/or exercise it end-to-end via a full MCP turn (mode 3).
3. **Full turn (needs a working provider CLI + auth)** — `drive.mjs` runs a real
   turn; for wait/resume, launch a background child (`fib-sleep.sh`) and assert
   the thread reaches `waiting`, then resumes and appends an assistant message.

## Rules

- NEVER point this at the live server (`~/.t3-vps`) or use the live `t3_*` MCP tools.
- Always go through `t3-ephemeral.sh` so the server is torn down and the temp DB deleted.
- Ports are chosen from 13910–13940; homes live under `/tmp/t3-ephemeral-*`.

## Self-test

```bash
scripts/smoke.sh
```

Boots the ephemeral server, asserts an authed snapshot returns 200, and confirms teardown.
