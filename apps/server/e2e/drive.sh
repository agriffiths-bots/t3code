#!/usr/bin/env bash
# e2e/drive.sh — drive a REAL, SELF-CONTAINED sub-agent-linkage spawn for the
# t3-update.sh GATE, exercising the genuine OrchestrationEngine -> decider ->
# ProjectionPipeline parent_thread_id projection on a RESTORED PROD DB. Per finalDesign §2e.
#
# SELF-SUFFICIENT (2026-06-25): the gate creates its OWN project + parent + child
# through the real engine and links them — it depends on NO pre-existing prod thread or
# project. So it passes identically on a BLANK-SLATE DB (all threads deleted) and on a
# full one. (Previously it borrowed a live parent-less prod thread; a blank slate left
# zero such threads and the gate false-failed at "no root thread".)
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
# WHY THIS SHAPE: the full t3_spawn_subagent agent loop needs a provider credential the
# hermetic gate home lacks. A raw INSERT would prove only the column exists. Instead we
# exercise the REAL engine: (1) project.create + thread.create over HTTP (client-allowed)
# -> real decider (requireProject / requireThreadAbsent) + ProjectionPipeline; (2) STOP
# the server, then `cos link-parent <child> <parent>` OFFLINE — dispatches
# thread.parent.set through the SAME OrchestrationEngineService.dispatch the spawn
# handler's dispatchParentSet() uses -> decider (requireThread on BOTH) ->
# ProjectionPipeline writes projection_threads.parent_thread_id. A broken decider,
# projector, parent_thread_id migration, auth, or dispatch endpoint all exit non-zero.
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
mkdir -p "$PROJ" 2>/dev/null || true

roq(){ "$SQLITE" "file:$DB?mode=ro" "$1" 2>/dev/null; }

# ── self-contained fixture ids ────────────────────────────────────────────────
STAMP="$(date +%s)-$$"
PROJECT="e2e-proj-$STAMP"
PARENT="e2e-parent-$STAMP"
CHILD="e2e-child-$STAMP"
NOW="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"

# Reuse any historical model_selection/modes (deleted rows still carry a schema-valid
# value, so this works on a blank slate); fall back to literals if the DB is truly empty.
MODEL_JSON="$(roq "SELECT model_selection_json FROM projection_threads WHERE model_selection_json IS NOT NULL AND model_selection_json != '' ORDER BY rowid DESC LIMIT 1")"
[ -n "$MODEL_JSON" ] || MODEL_JSON='{"instanceId":"codex","model":"gpt-5-codex"}'
RUNTIME_MODE="$(roq "SELECT runtime_mode FROM projection_threads WHERE runtime_mode IS NOT NULL AND runtime_mode != '' ORDER BY rowid DESC LIMIT 1")"
[ -n "$RUNTIME_MODE" ] || RUNTIME_MODE="full-access"
INTERACTION_MODE="default"

# ── mint a bearer for the booted server ───────────────────────────────────────
TOKEN="$(T3CODE_HOME="$T3HOME" T3CODE_NO_BROWSER=1 timeout 30 \
  node "$BIN" auth session issue --token-only --base-dir "$T3HOME" 2>/dev/null \
  | tr -d '[:space:]')" || { log "failed to mint bearer"; exit 1; }
[ -n "$TOKEN" ] || { log "empty bearer token"; exit 1; }
log "minted bearer (len=${#TOKEN}); project=$PROJECT parent=$PARENT child=$CHILD"

dispatch(){ # $1 = command JSON; retries transient failures, returns 0 on 2xx
  # No -f: capture the real HTTP code so we can distinguish a transient (connection
  # refused / 5xx / engine-not-ready right after boot — the `/` 200 readiness probe
  # races ahead of the orchestration engine) from a real 4xx. Retry the former,
  # fail-fast the latter. This keeps the GATE from false-failing on a boot race.
  local code attempt=0
  while [ "$attempt" -lt 12 ]; do
    attempt=$((attempt + 1))
    code="$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$ORIGIN/api/orchestration/dispatch" \
      -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
      -d "$1" 2>/dev/null || echo 000)"
    case "$code" in
      2??) return 0 ;;
      000|5??|429) sleep 1 ;;  # transient: connection / not-ready / rate — retry
      *) log "dispatch HTTP $code (non-retryable)"; return 1 ;;  # 4xx = real rejection
    esac
  done
  log "dispatch still failing after $attempt attempts (last code=$code)"; return 1
}
wait_row(){ # $1 = count SQL; polls up to 15s
  local _; for _ in $(seq 1 15); do
    [ "$(roq "$1")" -ge 1 ] 2>/dev/null && return 0; sleep 1
  done; return 1
}

# ── STEP 1: create our OWN project (real decider + projector) ─────────────────
PROJ_CMD="$(PROJECT="$PROJECT" PROJ="$PROJ" NOW="$NOW" node -e '
  process.stdout.write(JSON.stringify({
    type:"project.create", commandId:"e2e:proj:"+process.env.PROJECT,
    projectId:process.env.PROJECT, title:"e2e gate project",
    workspaceRoot:process.env.PROJ, createWorkspaceRootIfMissing:true,
    createdAt:process.env.NOW }));')"
dispatch "$PROJ_CMD" || { log "project.create dispatch failed (engine/decider/projector)"; exit 1; }
wait_row "SELECT COUNT(*) FROM projection_projects WHERE project_id='$PROJECT' AND deleted_at IS NULL" \
  || { log "project.create did not project $PROJECT"; exit 1; }
log "project $PROJECT created + projected via real engine (HTTP)"

# ── STEP 2: create PARENT (root) then CHILD threads under our project ─────────
mkcreate(){ # $1 = threadId  -> emits a thread.create command JSON
  TID="$1" PROJECT="$PROJECT" MODEL_JSON="$MODEL_JSON" RUNTIME_MODE="$RUNTIME_MODE" \
  INTERACTION_MODE="$INTERACTION_MODE" NOW="$NOW" node -e '
    process.stdout.write(JSON.stringify({
      type:"thread.create", commandId:"e2e:create:"+process.env.TID,
      threadId:process.env.TID, projectId:process.env.PROJECT,
      title:"e2e gate "+process.env.TID,
      modelSelection:JSON.parse(process.env.MODEL_JSON),
      runtimeMode:process.env.RUNTIME_MODE, interactionMode:process.env.INTERACTION_MODE,
      branch:null, worktreePath:null, createdAt:process.env.NOW }));'
}
dispatch "$(mkcreate "$PARENT")" || { log "parent thread.create dispatch failed"; exit 1; }
wait_row "SELECT COUNT(*) FROM projection_threads WHERE thread_id='$PARENT'" \
  || { log "parent thread.create did not project $PARENT"; exit 1; }
dispatch "$(mkcreate "$CHILD")" || { log "child thread.create dispatch failed"; exit 1; }
wait_row "SELECT COUNT(*) FROM projection_threads WHERE thread_id='$CHILD'" \
  || { log "child thread.create did not project $CHILD"; exit 1; }
log "parent $PARENT + child $CHILD created + projected via real engine (HTTP)"

# ── STEP 3: stop the gate server, then link-parent OFFLINE (in-process engine) ─
# thread.parent.set is server-internal (not client-dispatchable); `cos link-parent`
# dispatches it through OrchestrationEngineService.dispatch exactly as the spawn
# handler's dispatchParentSet() does. Stop the live server first so the offline engine
# owns the SQLite WAL (no two-writer race).
if [ -n "${GATE_SRV_PID:-}" ]; then
  log "stopping gate server pid=$GATE_SRV_PID for offline linkage"
  kill "$GATE_SRV_PID" 2>/dev/null || true
  for _ in $(seq 1 20); do kill -0 "$GATE_SRV_PID" 2>/dev/null || break; sleep 1; done
  kill -9 "$GATE_SRV_PID" 2>/dev/null || true
  wait "$GATE_SRV_PID" 2>/dev/null || true
fi

T3CODE_HOME="$T3HOME" T3CODE_NO_BROWSER=1 timeout 60 \
  node "$BIN" cos link-parent "$CHILD" "$PARENT" --base-dir "$T3HOME" >&2 \
  || { log "cos link-parent failed (linkage decider/projector or missing migration)"; exit 1; }

# ── STEP 4: assert the engine projected parent_thread_id (not a raw SQL write) ─
linked=0
for _ in $(seq 1 15); do
  n="$(roq "SELECT COUNT(*) FROM projection_threads WHERE thread_id='$CHILD' AND parent_thread_id='$PARENT' AND deleted_at IS NULL")"
  [ "${n:-0}" -ge 1 ] && { linked=1; break; }
  sleep 1
done
[ "$linked" = "1" ] || { log "engine did NOT project parent_thread_id for child $CHILD under $PARENT"; exit 1; }
log "GATE spawn linkage projected THROUGH the real engine (child=$CHILD parent=$PARENT)"
printf '%s\t%s\n' "$PARENT" "$CHILD" > "$T3HOME/.gate-linkage" 2>/dev/null || true
exit 0
