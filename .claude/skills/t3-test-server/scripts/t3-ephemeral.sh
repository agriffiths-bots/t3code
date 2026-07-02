#!/usr/bin/env bash
# t3-ephemeral.sh — boot a THROWAWAY T3 server with an isolated state DB, run a
# command against it, then tear everything down. Never touches the live server.
#
# The command (everything after `--`) runs with these env vars exported:
#   T3_ORIGIN  http://127.0.0.1:<port>       (Environment HTTP API base)
#   T3_TOKEN   scoped admin bearer            (carries orchestration:operate)
#   T3_DB      the ephemeral state.sqlite     (open read-only for assertions)
#   T3_HOME    the temp T3CODE_HOME (also exported as T3CODE_HOME)
#   T3_PORT    the chosen port
#
# The env vars are exported INSIDE the wrapper, so any command that references
# them must be quoted so the CHILD shell expands them (use `bash -c '...'`), not
# the caller's shell.
#
# Usage:
#   t3-ephemeral.sh [--entry PATH] [--boot-timeout SECS] -- <command...>
#
# Examples:
#   # Drive a real turn (needs a working provider CLI + a writable workspace):
#   t3-ephemeral.sh -- bash -c 'mkdir -p /tmp/t3-eph-proj && node e2e/drive.mjs \
#       --origin "$T3_ORIGIN" --db "$T3_DB" --workspace /tmp/t3-eph-proj \
#       --instance claudeAgent --model claude-sonnet-4-6 --prompt "say READY"'
#
#   # Assert routing without running a model (Fix 1/2 style):
#   t3-ephemeral.sh -- bash -c 'curl -s -H "authorization: Bearer $T3_TOKEN" \
#       "$T3_ORIGIN/api/orchestration/snapshot"'
#
# To test LOCAL source changes, rebuild the server bundle first
# (`vp run build:desktop` or the server package build), or pass
# `--entry apps/server/src/bin.ts --boot-timeout 240` to run from source (slow boot).
set -euo pipefail

ENTRY="apps/server/dist/bin.mjs"
BOOT_TIMEOUT=45
while [[ $# -gt 0 ]]; do
  case "$1" in
    --entry) ENTRY="$2"; shift 2 ;;
    --boot-timeout) BOOT_TIMEOUT="$2"; shift 2 ;;
    --) shift; break ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
if [[ $# -eq 0 ]]; then
  echo "usage: t3-ephemeral.sh [--entry PATH] [--boot-timeout SECS] -- <command...>" >&2
  exit 2
fi

REPO_ROOT="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
cd "$REPO_ROOT"

# The default entry is the built bundle, but apps/*/dist is a gitignored build
# artifact — so on a clean checkout fall back to the (always-present) source
# entry, which boots slower because Node type-strips it on the fly.
if [[ "$ENTRY" == "apps/server/dist/bin.mjs" && ! -f "$ENTRY" ]]; then
  echo "[t3-ephemeral] $ENTRY not built; falling back to source entry (slower boot)." >&2
  ENTRY="apps/server/src/bin.ts"
  [[ "$BOOT_TIMEOUT" -lt 240 ]] && BOOT_TIMEOUT=240
fi

T3_HOME="$(mktemp -d "/tmp/t3-ephemeral-XXXXXX")"
SRV_PID=""

cleanup() {
  trap - EXIT INT TERM   # single-shot: don't re-run for a second signal
  local pid="$SRV_PID"
  SRV_PID=""
  if [[ -n "$pid" ]]; then
    kill "$pid" 2>/dev/null || true
    # Bounded teardown: wait up to ~5s for a clean exit, then SIGKILL.
    for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf "$T3_HOME" 2>/dev/null || true
}
# On a signal, clean up and then terminate (128 + signo) instead of continuing.
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

# Boot the server. Rather than pre-probe a free port (which races with concurrent
# runs), just try to bind; if a port is already taken, move to the next one.
# Exposure env (Tailscale serve) is cleared so the throwaway server stays
# loopback-only regardless of the caller's shell.
PORT=""
code=000
ready=0
for candidate in $(seq 13910 13940); do
  : > "$T3_HOME/server.log"
  # Clear exposure + trace env so the throwaway server stays loopback-only and
  # doesn't inherit the caller's OTLP/trace sinks, and pin the log level to info
  # so the "Listening on ..." readiness line is always emitted.
  env -u T3CODE_TAILSCALE_SERVE -u T3CODE_TAILSCALE_SERVE_PORT \
    -u T3CODE_TRACE_FILE -u T3CODE_OTLP_TRACES_URL -u T3CODE_OTLP_METRICS_URL \
    -u VITE_DEV_SERVER_URL \
    T3CODE_HOME="$T3_HOME" T3CODE_NO_BROWSER=1 T3CODE_LOG_LEVEL=Info \
    node "$ENTRY" serve --port "$candidate" --host 127.0.0.1 > "$T3_HOME/server.log" 2>&1 &
  SRV_PID=$!
  for ((i = 1; i <= BOOT_TIMEOUT; i++)); do
    kill -0 "$SRV_PID" 2>/dev/null || break
    # "Listening on ..." confirms OUR server bound this exact port. An occupied
    # port makes our server EADDRINUSE and die instead, so we never false-ready
    # against a pre-existing listener on the same port.
    if grep -q "Listening on http://127.0.0.1:$candidate" "$T3_HOME/server.log"; then
      code="$(curl -s -o /dev/null -m 2 -w "%{http_code}" "http://127.0.0.1:$candidate/api/orchestration/snapshot" || true)"
      ready=1
      break
    fi
    sleep 1
  done
  if [[ "$ready" == "1" ]]; then PORT="$candidate"; break; fi
  # Not ready. Reap this attempt, then decide whether to try the next port.
  kill "$SRV_PID" 2>/dev/null || true; wait "$SRV_PID" 2>/dev/null || true; SRV_PID=""
  if grep -qiE "EADDRINUSE|address already in use|port .* in use" "$T3_HOME/server.log"; then
    continue
  fi
  echo "[t3-ephemeral] server failed to start on port $candidate:" >&2
  tail -20 "$T3_HOME/server.log" >&2
  exit 1
done
if [[ "$ready" != "1" ]]; then
  echo "[t3-ephemeral] no usable port in 13910-13940 (boot timeout ${BOOT_TIMEOUT}s)" >&2
  exit 1
fi
echo "[t3-ephemeral] ready home=$T3_HOME port=$PORT entry=$ENTRY (http=$code)" >&2

# `|| true` so a non-zero mint doesn't trip `set -e` before we can print diagnostics.
T3_TOKEN="$(env -u VITE_DEV_SERVER_URL T3CODE_HOME="$T3_HOME" node "$ENTRY" auth session issue --token-only 2>>"$T3_HOME/server.log")" || true
if [[ -z "$T3_TOKEN" ]]; then
  echo "[t3-ephemeral] failed to mint token; recent server.log:" >&2
  tail -10 "$T3_HOME/server.log" >&2
  exit 1
fi

export T3_ORIGIN="http://127.0.0.1:$PORT"
export T3_TOKEN T3_HOME T3_PORT="$PORT"
# Also export T3CODE_HOME so child commands that shell out to the T3 CLI target
# this ephemeral home by default instead of the caller's live one.
export T3CODE_HOME="$T3_HOME"
# The server chooses its own state dir (userdata/ or dev/) from its config, so
# locate the actual SQLite DB rather than assuming a fixed subdirectory. It is
# written during boot; retry briefly, but DON'T abort the caller's command if it
# is still missing — let assertions decide how to handle an empty T3_DB.
T3_DB=""
for _ in $(seq 1 20); do
  T3_DB="$(find "$T3_HOME" -maxdepth 2 -name state.sqlite 2>/dev/null | head -1)"
  [[ -n "$T3_DB" ]] && break
  sleep 0.2
done
export T3_DB
[[ -n "$T3_DB" ]] || echo "[t3-ephemeral] warning: state.sqlite not found under $T3_HOME yet; T3_DB is empty" >&2

# HTTP listening != command-ready: wait for an AUTHED snapshot (200), which proves
# the orchestration engine is initialized, before handing off to the command.
ac=""
for _ in $(seq 1 40); do
  ac="$(curl -s -o /dev/null -m 3 -w "%{http_code}" -H "authorization: Bearer $T3_TOKEN" "$T3_ORIGIN/api/orchestration/snapshot" || true)"
  [[ "$ac" == "200" ]] && break
  sleep 0.5
done
if [[ "$ac" != "200" ]]; then
  echo "[t3-ephemeral] server never became command-ready (authed snapshot ${ac:-none}); aborting:" >&2
  tail -20 "$T3_HOME/server.log" >&2
  exit 1
fi

# The wrapped command inherits the ephemeral T3CODE_HOME; also drop any
# inherited dev-server URL so CLI calls it makes hit the same state dir.
unset VITE_DEV_SERVER_URL
set +e
"$@"
rc=$?
set -e
echo "[t3-ephemeral] command exited rc=$rc; tearing down" >&2
exit "$rc"
