#!/usr/bin/env bash
# e2e/drive.sh — drive a REAL sub-agent-linkage spawn for the t3-update.sh GATE,
# exercising the genuine OrchestrationEngine -> decider -> ProjectionPipeline
# parent_thread_id projection on a RESTORED PROD DB. Per finalDesign.md §2e.
#
# Usage:
#   drive.sh <T3CODE_HOME> <ORIGIN> <PROJ_DIR> <PROVIDER> <MODEL> "<prompt>"
# Env (set by the gate):
#   GATE_SRV_PID  — pid of the booted gate `serve` process. drive.sh STOPS it before
#                   the offline linkage step so the in-process engine owns the WAL.
#
# Contract: exit 0 iff a child thread was created AND linked under a parent THROUGH the
# real engine (parent_thread_id projected). On ANY failure exit non-zero — FAIL-CLOSED.
#
# WHY THIS SHAPE (review fix — "gate exercises real spawn, not just typecheck"):
#   The full t3_spawn_subagent agent loop needs a working AI provider credential +
#   provider-instance config to run a turn that calls the MCP tool. The prod gate home
#   has neither (only state.sqlite is copied in), so that loop cannot run hermetically.
#   A raw `INSERT INTO projection_threads` would prove only the column exists and let a
#   broken-spawn build deploy. Instead we exercise the REAL engine:
#     1. thread.create over HTTP (client-allowed) against the booted server  -> real
#        decider (requireProject / requireThreadAbsent) + ProjectionPipeline create.
#     2. STOP the server, then `t3 cos link-parent <child> <parent>` OFFLINE — dispatches
#        thread.parent.set through the SAME OrchestrationEngineService.dispatch the spawn
#        handler's dispatchParentSet() uses -> decider (requireThread on BOTH child and
#        parent) -> ProjectionPipeline writes projection_threads.parent_thread_id.
#   A broken decider, projector, parent_thread_id migration, auth, or dispatch endpoint
#   all cause a non-zero exit. No raw SQL writes the linkage; assert.mjs re-reads what
#   the engine produced. (thread.parent.set is NOT in ClientOrchestrationCommand, so it
#   is intentionally driven via the in-process engine, not the HTTP client endpoint.)
set -euo pipefail

T3HOME="${1:?usage: drive.sh HOME ORIGIN PROJ PROVIDER MODEL PROMPT}"
ORIGIN="${2:?origin}"
PROJ="${3:?proj dir}"
PROVIDER="${4:-claudeAgent}"
MODEL="${5:-claude-sonnet-4-6}"
PROMPT="${6:-Use t3_spawn_subagent to start one detached child that replies READY, then stop.}"

SQLITE="${SQLITE:-/home/linuxbrew/.linuxbrew/bin/sqlite3}"
DB="$T3HOME/userdata/state.sqlite"
BIN="${T3_BIN:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/dist/bin.mjs}"
log(){ echo "[e2e-drive] $*" >&2; }

[ -f "$DB" ] || { log "no state.sqlite at $DB"; exit 2; }
command -v node >/dev/null 2>&1 || { log "node not on PATH"; exit 2; }
command -v curl >/dev/null 2>&1 || { log "curl not on PATH"; exit 2; }

roq(){ "$SQLITE" "file:$DB?mode=ro" "$1" 2>/dev/null; }

# ── pick a real root thread to parent the child under + reuse its model/modes ──
ROOT="$(roq "SELECT thread_id FROM projection_threads WHERE parent_thread_id IS NULL AND deleted_at IS NULL ORDER BY rowid DESC LIMIT 1")"
[ -n "$ROOT" ] || { log "no root (parent-less, live) thread in restored DB to parent under"; exit 1; }
PROJ_ID="$(roq "SELECT project_id FROM projection_threads WHERE thread_id='$ROOT' LIMIT 1")"
[ -n "$PROJ_ID" ] || { log "root thread $ROOT has no project_id"; exit 1; }
RUNTIME_MODE="$(roq "SELECT COALESCE(runtime_mode,'full-access') FROM projection_threads WHERE thread_id='$ROOT' LIMIT 1")"
INTERACTION_MODE="$(roq "SELECT COALESCE(interaction_mode,'default') FROM projection_threads WHERE thread_id='$ROOT' LIMIT 1")"
MODEL_JSON="$(roq "SELECT model_selection_json FROM projection_threads WHERE model_selection_json IS NOT NULL AND model_selection_json != '' ORDER BY rowid DESC LIMIT 1")"
[ -n "$MODEL_JSON" ] || MODEL_JSON='{"instanceId":"codex","model":"gpt-5-codex"}'
[ -n "$RUNTIME_MODE" ] || RUNTIME_MODE="full-access"
[ -n "$INTERACTION_MODE" ] || INTERACTION_MODE="default"

CHILD="e2e-child-$(date +%s)-$$"
NOW="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"

# ── mint a bearer for the booted server ───────────────────────────────────────
TOKEN="$(T3CODE_HOME="$T3HOME" T3CODE_NO_BROWSER=1 timeout 30 \
  node "$BIN" auth session issue --token-only --base-dir "$T3HOME" 2>/dev/null \
  | tr -d '[:space:]')" || { log "failed to mint bearer"; exit 1; }
[ -n "$TOKEN" ] || { log "empty bearer token"; exit 1; }
log "minted bearer (len=${#TOKEN}); root=$ROOT project=$PROJ_ID child=$CHILD"

# ── STEP 1: thread.create over HTTP (real decider + projector) ────────────────
CREATE_CMD="$(MODEL_JSON="$MODEL_JSON" CHILD="$CHILD" PROJ_ID="$PROJ_ID" NOW="$NOW" \
  RUNTIME_MODE="$RUNTIME_MODE" INTERACTION_MODE="$INTERACTION_MODE" node -e '
    const o={ type:"thread.create",
      commandId:"e2e:create:"+process.env.CHILD,
      threadId:process.env.CHILD,
      projectId:process.env.PROJ_ID,
      title:"e2e gate child",
      modelSelection:JSON.parse(process.env.MODEL_JSON),
      runtimeMode:process.env.RUNTIME_MODE,
      interactionMode:process.env.INTERACTION_MODE,
      branch:null, worktreePath:null,
      createdAt:process.env.NOW };
    process.stdout.write(JSON.stringify(o));')"
code="$(curl -fsS -X POST "$ORIGIN/api/orchestration/dispatch" \
  -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d "$CREATE_CMD" -w '%{http_code}' -o /dev/null 2>/dev/null)" || { log "thread.create transport failed"; exit 1; }
case "$code" in 2??) : ;; *) log "thread.create dispatch HTTP $code (engine/decider/projector or schema)"; exit 1 ;; esac
# confirm the create projected
created=0
for _ in $(seq 1 15); do
  [ "$(roq "SELECT COUNT(*) FROM projection_threads WHERE thread_id='$CHILD'")" -ge 1 ] 2>/dev/null && { created=1; break; }
  sleep 1
done
[ "$created" = "1" ] || { log "thread.create did not project child $CHILD"; exit 1; }
log "child $CHILD created + projected via real engine (HTTP)"

# ── STEP 2: stop the gate server, then link-parent OFFLINE (in-process engine) ─
# thread.parent.set is a server-internal command (not client-dispatchable over HTTP);
# `cos link-parent` dispatches it through OrchestrationEngineService.dispatch exactly as
# the spawn handler's dispatchParentSet() does. Stop the live server first so the offline
# engine owns the SQLite WAL (no two-writer race).
if [ -n "${GATE_SRV_PID:-}" ]; then
  log "stopping gate server pid=$GATE_SRV_PID for offline linkage"
  kill "$GATE_SRV_PID" 2>/dev/null || true
  for _ in $(seq 1 20); do kill -0 "$GATE_SRV_PID" 2>/dev/null || break; sleep 1; done
  kill -9 "$GATE_SRV_PID" 2>/dev/null || true
  wait "$GATE_SRV_PID" 2>/dev/null || true
fi

T3CODE_HOME="$T3HOME" T3CODE_NO_BROWSER=1 timeout 60 \
  node "$BIN" cos link-parent "$CHILD" "$ROOT" --base-dir "$T3HOME" >&2 \
  || { log "cos link-parent failed (linkage decider/projector or missing migration)"; exit 1; }

# ── STEP 3: assert the engine projected parent_thread_id (not a raw SQL write) ─
linked=0
for _ in $(seq 1 15); do
  n="$(roq "SELECT COUNT(*) FROM projection_threads WHERE thread_id='$CHILD' AND parent_thread_id='$ROOT' AND deleted_at IS NULL")"
  [ "${n:-0}" -ge 1 ] && { linked=1; break; }
  sleep 1
done
[ "$linked" = "1" ] || { log "engine did NOT project parent_thread_id for child $CHILD under $ROOT"; exit 1; }
log "GATE spawn linkage projected THROUGH the real engine (child=$CHILD parent=$ROOT)"
# Emit the exact {parent,child} ids the gate should assert childrenOf() on (the latest
# parent-less thread by rowid is no longer reliable once the child is created/linked).
printf '%s\t%s\n' "$ROOT" "$CHILD" > "$T3HOME/.gate-linkage" 2>/dev/null || true
exit 0
