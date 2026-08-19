# t3 e2e harness assets

Manual, non-CI end-to-end harnesses. Neither runs in CI, neither drives a
browser, and neither points at live T3 state.

## Matrix bridge

`matrix-bridge.mjs` owns a disposable loopback conduwuit homeserver, two
registered Matrix accounts, an encrypted JSON-lines Matrix client
(`matrix-cli.mjs`), and an ephemeral T3 server on its own home. It has two
modes:

```
node e2e/matrix-bridge.mjs            # release gate: the full bridge flow
node e2e/matrix-bridge.mjs --smoke    # self-check: the harness itself
```

The release gate asserts pairing (rejection then acceptance), final-output-only
outbound, inbound turns, mid-turn steering, owner moves, homeserver outage
recovery, and unbridge silence. It needs a server that advertises
`environment.capabilities.matrixBridge` and an authenticated provider; without
the capability it stops immediately and says so.

The self-check skips the bridge entirely: the two accounts create, join, and
talk in an encrypted invite-only room by themselves. It proves the homeserver,
registration, E2EE client, and T3 boot all work, so a red release gate can be
read as a bridge failure rather than a harness failure.

Both modes own every temp path, loopback port, and spawned PID, and both tear
all of it down on every exit path. The pinned conduwuit `v0.5.0-rc4` static
musl binary is checksummed and cached under `~/.cache/t3-matrix-e2e`, then run
from a copy in the temp root. Progress: `/tmp/t3-matrix-e2e.progress`. A
failure keeps redacted logs at `/tmp/t3-matrix-e2e-failure.log` and still
deletes the temp root, because it holds access tokens and a crypto store.

See [docs/internals/matrix-bridge.md](../docs/internals/matrix-bridge.md) and
[the rollout runbook](../docs/operations/matrix-bridge-rollout.md).

## Sub-agent and scheduler

Manual (non-CI) end-to-end verification that a real Claude agent running INSIDE
a t3 thread drives the sub-agent + scheduler MCP tools (migrations 033/034,
`ChildThreadCoordinator` + `ScheduledTasksReactor`, the
`t3_spawn_subagent` / `t3_steer_subagent` / `t3_subagents` /
`t3_schedule_create|list|update|delete` tools). Full scenario design lives in
**`/tmp/t3-design/e2ePlan.md`** — this
README maps that plan onto the assets in this directory.

All assertions come from observable persisted state: the SQLite projection
tables + `scheduled_tasks`, plus MCP tool returns captured in a `claude -p`
transcript and measured wall-clock. The state DB is opened **read-only**
(`mode=ro`) at `<T3CODE_HOME>/userdata/state.sqlite`.

### Boot recipe (verified)

```
cd /tmp/t3code-inspect/apps/server && \
  T3CODE_HOME=<HOME> T3CODE_NO_BROWSER=1 \
  node src/bin.ts serve --port <PORT> --host 127.0.0.1
```

Use ports 13910–13920, temp homes under `/tmp`. Exit 124 under `timeout` means
healthy. **Always kill any server you start when done.**

### Assets

| File                     | Purpose                                                                                                                                                                                                                                                                                                                                                              |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fib-sleep.sh`           | Deterministic long-running child. Prints Fibonacci `1 1 2 3 5 8 13 21`, sleeping `n * FIB_SCALE` seconds after each (default `FIB_SCALE=60` => minutes, cumulative **54 min**). Keeps a process — and thus the t3 turn that launched it — alive for the whole budget. `FIB_SCALE=1` => 54s dry-run; `FIB_SCALE=0.05` => ~2.7s smoke. Timestamped heartbeat per step. |
| `assert.mjs`             | Read-only SQLite reader helpers (`node:sqlite`, `mode=ro`): `openState`, `turnCountForThread`, `turnTimestamps`, `childrenOf`, `scheduledTask`, `listScheduledTasks`, `threadShell`, `assistantMessages`.                                                                                                                                                            |
| `drive.mjs` / `drive.sh` | (pre-existing) Programmatically create a project/thread and dispatch a user turn over the Environment HTTP API, then poll projections until the turn settles. Used for bring-up + pre-flight.                                                                                                                                                                        |

#### `assert.mjs` helpers vs. schema

- `turnCountForThread` / `turnTimestamps` → `projection_turns` (migration 005).
- `childrenOf` → `projection_threads WHERE parent_thread_id = ?` (migration 034).
- `scheduledTask` / `listScheduledTasks` → `scheduled_tasks` (migration 034).
- `threadShell` → latest `projection_turns` row + `projection_thread_sessions`.
- `assistantMessages` → `projection_thread_messages` (pass `role:"user"` to find
  the coordinator wake injection `[sub-agent <id> completed]`).

### Scenario → asset/assertion map

#### (a) Same-thread schedule fires repeatedly — `e2ePlan.md` §(a)

- Agent calls `t3_schedule_create({threadId:root, intervalSeconds:60})`.
- Poll with `turnCountForThread(db, root)` — increases ~1/60s; `scheduledTask`
  shows `next_run_at` advancing, `last_run_at` updating, `last_status='dispatched'`.
- `childrenOf` / `COUNT(projection_threads)` stays stable (no new thread/run).
- Restart-persistence + no-double-turn-after-`kill -9`: `turnTimestamps(db, root)`
  shows no duplicate within one interval. Busy/skip: `scheduledTask` shows
  `last_status='skipped'`, `skipped_count++`, `next_run_at` still advances.

#### (b) Cross-provider spawn (claude+codex+cursor) — `e2ePlan.md` §(b)

- After `t3_spawn_subagent` ×3 (one required model/title pair per provider):
  `childrenOf(db, root)` returns 3 rows with `parent_thread_id=root`; the
  `model` column verifies per-provider routing.
- `threadShell(db, child).latestTurn.state` goes `running` → `completed`.
- `t3_subagents()` lists all three children and `t3_subagents({childThreadId})`
  reports each child's latest text after it settles.
- Detached WAKE / consolidation: `assistantMessages(db, root, "user")` contains
  the `[sub-agent <id> completed]` injection(s) — one turn carrying both for the
  two-child consolidation case.

#### (c) Long-running child wakes its parent (opt-in `E2E_ENABLE_1H=1`) — `e2ePlan.md` §(c)

- Child prompt runs `fib-sleep.sh` (default `FIB_SCALE=60`, 54 min cumulative),
  keeping its turn alive script-driven (reliable, not model-driven).
- The parent continues independently after spawn; no polling/wait tool is used.
- Assert with `threadShell` that the child turn does not settle early; measured
  wall-clock spawn→settle in [50,62] min; cross-check
  `turnTimestamps(db, child)` `completed_at - requested_at` ≈ duration. After
  completion, the parent receives exactly one coordinator wake injection.
- For local iteration, set the child prompt to use `FIB_SCALE=1` (54s) to
  exercise the same wake-on-completion path without the 1h hold.

#### (d) Killed child → parent receives failure wake — `e2ePlan.md` §(d)

- Spawn a ~10 min child (`FIB_SCALE` tuned), then let the parent continue.
- Kill via (i) `thread.delete`, (ii) `kill -9` the provider process,
  (iii) `session.stop`. Assert the parent wake reports `killed`/`failed`;
  `threadShell(db, child).session.last_error` is populated and
  `latestTurn.state` is `failed`/`interrupted`.
- ORPHAN: kill the parent after a detached spawn; assert a WARN is logged and no
  crash (documented limitation).
