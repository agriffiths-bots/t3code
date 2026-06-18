# t3 sub-agent + scheduler — e2e harness assets

Manual (non-CI) end-to-end verification that a real Claude agent running INSIDE
a t3 thread drives the sub-agent + scheduler MCP tools (migrations 033/034,
`ChildThreadCoordinator` + `ScheduledTasksReactor`, the
`t3_spawn_subagent` / `t3_steer_subagent` / `t3_check_subagent` /
`t3_wait_subagent` / `t3_list_subagent` / `t3_schedule_create|list|update|delete`
tools). Full scenario design lives in **`/tmp/t3-design/e2ePlan.md`** — this
README maps that plan onto the assets in this directory.

All assertions come from observable persisted state: the SQLite projection
tables + `scheduled_tasks`, plus MCP tool returns captured in a `claude -p`
transcript and measured wall-clock. The state DB is opened **read-only**
(`mode=ro`) at `<T3CODE_HOME>/userdata/state.sqlite`.

## Boot recipe (verified)

```
cd /tmp/t3code-inspect/apps/server && \
  T3CODE_HOME=<HOME> T3CODE_NO_BROWSER=1 \
  node src/bin.ts serve --port <PORT> --host 127.0.0.1
```

Use ports 13910–13920, temp homes under `/tmp`. Exit 124 under `timeout` means
healthy. **Always kill any server you start when done.**

## Assets

| File | Purpose |
|------|---------|
| `fib-sleep.sh` | Deterministic long-running child. Prints Fibonacci `1 1 2 3 5 8 13 21`, sleeping `n * FIB_SCALE` seconds after each (default `FIB_SCALE=60` => minutes, cumulative **54 min**). Keeps a process — and thus the t3 turn that launched it — alive for the whole budget. `FIB_SCALE=1` => 54s dry-run; `FIB_SCALE=0.05` => ~2.7s smoke. Timestamped heartbeat per step. |
| `assert.mjs` | Read-only SQLite reader helpers (`node:sqlite`, `mode=ro`): `openState`, `turnCountForThread`, `turnTimestamps`, `childrenOf`, `scheduledTask`, `listScheduledTasks`, `threadShell`, `assistantMessages`. |
| `drive.mjs` / `drive.sh` | (pre-existing) Programmatically create a project/thread and dispatch a user turn over the Environment HTTP API, then poll projections until the turn settles. Used for bring-up + pre-flight. |

### `assert.mjs` helpers vs. schema

- `turnCountForThread` / `turnTimestamps` → `projection_turns` (migration 005).
- `childrenOf` → `projection_threads WHERE parent_thread_id = ?` (migration 034).
- `scheduledTask` / `listScheduledTasks` → `scheduled_tasks` (migration 034).
- `threadShell` → latest `projection_turns` row + `projection_thread_sessions`.
- `assistantMessages` → `projection_thread_messages` (pass `role:"user"` to find
  the coordinator wake injection `[sub-agent <id> completed]`).

## Scenario → asset/assertion map

### (a) Same-thread schedule fires repeatedly — `e2ePlan.md` §(a)
- Agent calls `t3_schedule_create({threadId:root, intervalSeconds:60})`.
- Poll with `turnCountForThread(db, root)` — increases ~1/60s; `scheduledTask`
  shows `next_run_at` advancing, `last_run_at` updating, `last_status='dispatched'`.
- `childrenOf` / `COUNT(projection_threads)` stays stable (no new thread/run).
- Restart-persistence + no-double-turn-after-`kill -9`: `turnTimestamps(db, root)`
  shows no duplicate within one interval. Busy/skip: `scheduledTask` shows
  `last_status='skipped'`, `skipped_count++`, `next_run_at` still advances.

### (b) Cross-provider spawn (claude+codex+cursor) — `e2ePlan.md` §(b)
- After `t3_spawn_subagent` ×3 (one per provider, `detached:true`):
  `childrenOf(db, root)` returns 3 rows with `parent_thread_id=root`; the
  `model` column verifies per-provider routing.
- `threadShell(db, child).latestTurn.state` goes `running` → `completed`.
- Fan-out + single `t3_wait_subagent(mode:"all")`: assert each child settled and
  has non-null `assistantMessages(db, child)`.
- Detached WAKE / consolidation: `assistantMessages(db, root, "user")` contains
  the `[sub-agent <id> completed]` injection(s) — one turn carrying both for the
  two-child consolidation case.

### (c) Long wait ~1 hour (opt-in `E2E_ENABLE_1H=1`) — `e2ePlan.md` §(c)
- Child prompt runs `fib-sleep.sh` (default `FIB_SCALE=60`, 54 min cumulative),
  keeping its turn alive script-driven (reliable, not model-driven).
- Driver loops `t3_wait_subagent(timeoutSeconds:3900)` across ~20s slices.
- Assert with `threadShell` that the child turn does not settle early; measured
  wall-clock spawn→settle in [50,62] min; cross-check
  `turnTimestamps(db, child)` `completed_at - requested_at` ≈ duration. Each
  intermediate slice returns `status:"pending"` + resumeToken, HTTP <30s.
- For local iteration, set the child prompt to use `FIB_SCALE=1` (54s) to
  exercise the slice/resumeToken loop without the 1h hold.

### (d) Killed child → wait returns failure, not hang — `e2ePlan.md` §(d)
- Spawn a ~10 min child (`FIB_SCALE` tuned), start the wait loop.
- Kill via (i) `thread.delete`, (ii) `kill -9` the provider process,
  (iii) `session.stop`. Assert wait reports `killed`/`failed` within one slice;
  `threadShell(db, child).session.last_error` is populated and
  `latestTurn.state` is `failed`/`interrupted`.
- CONTROL: `t3_wait_subagent(timeoutSeconds:5)` on a live child → `timeout`.
- ORPHAN: kill the parent after a detached spawn; assert a WARN is logged and no
  crash (documented preview limitation).
