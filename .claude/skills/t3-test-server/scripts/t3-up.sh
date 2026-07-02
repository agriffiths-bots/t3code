#!/usr/bin/env bash
# t3-up.sh — boot a PERSISTENT throwaway T3 server (isolated state DB) that
# outlives this script, for multi-step / multi-agent testing (e.g. a repro
# subthread driving the web UI with Playwright). Pair with t3-down.sh.
#
# Never touches the live server: fresh temp T3CODE_HOME, loopback-only,
# ports 13910-13940, exposure/trace env cleared.
#
# LINUX-ONLY tooling (the factory VPS): relies on setsid, /proc and GNU
# readlink -f. Do not expect it to run on stock macOS/BSD.
#
# Usage:
#   t3-up.sh [--name NAME] [--entry PATH] [--boot-timeout SECS]
#
# stdout (eval-able):   export T3_ORIGIN=... T3_TOKEN=... T3_DB=... T3_HOME=...
#                       export T3_PORT=... T3_PID=... T3_ENTRY=... T3_NAME=...
# Everything else goes to stderr. The same vars persist in the PRIVATE
# per-user registry ~/.cache/t3-ephemeral/instances/<NAME>/instance.env
# for later shells/agents:
#
#   eval "$(.claude/skills/t3-test-server/scripts/t3-up.sh --name repro)"
#   # ... later, any shell:
#   . ~/.cache/t3-ephemeral/instances/repro/instance.env
#   .claude/skills/t3-test-server/scripts/t3-down.sh repro
set -euo pipefail

NAME="default"
ENTRY="apps/server/dist/bin.mjs"
BOOT_TIMEOUT=45
while [[ $# -gt 0 ]]; do
  case "$1" in
    --name) NAME="$2"; shift 2 ;;
    --entry) ENTRY="$2"; shift 2 ;;
    --boot-timeout) BOOT_TIMEOUT="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
[[ "$NAME" =~ ^[A-Za-z0-9_-]+$ ]] || { echo "t3-up: --name must be [A-Za-z0-9_-]+" >&2; exit 2; }

REPO_ROOT="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
cd "$REPO_ROOT"

# Registry lives in a PRIVATE per-user dir (never world-writable /tmp: a
# predictable shared path would let a local co-tenant plant instance.env).
# Ownership + mode are enforced every run; values are PARSED, never sourced,
# by these scripts.
REG_ROOT="${T3_EPHEMERAL_REGISTRY:-$HOME/.cache/t3-ephemeral/instances}"
# Strip trailing slashes: a slash-qualified path makes bash FOLLOW a symlink
# in the -L test below, defeating the check.
while [[ "$REG_ROOT" == */ && "$REG_ROOT" != "/" ]]; do REG_ROOT="${REG_ROOT%/}"; done
# The registry marker is what t3-down trusts before sweeping — so it may only
# be stamped on a directory we created (or one already marked), never on an
# arbitrary pre-existing dir a mis-set T3_EPHEMERAL_REGISTRY points at.
if [[ -d "$REG_ROOT" && ! -f "$REG_ROOT/.t3-ephemeral-registry" ]]; then
  # Emptiness must be PROVEN: a listing failure (e.g. unreadable dir) must
  # fail closed, not read as "empty".
  entries="$(ls -A "$REG_ROOT" 2>/dev/null)"; ls_rc=$?
  if [[ $ls_rc -ne 0 || -n "$entries" ]]; then
    echo "t3-up: $REG_ROOT exists and is not a provably-empty t3 registry (no marker); refusing — point T3_EPHEMERAL_REGISTRY at a fresh directory" >&2
    exit 1
  fi
fi
mkdir -p "$REG_ROOT"
# Symlink/ownership check BEFORE chmod: chmod on a directory symlink would
# silently change the target's mode.
[[ -O "$REG_ROOT" && ! -L "$REG_ROOT" ]] || { echo "t3-up: refusing registry $REG_ROOT (not owned by us, or a symlink)" >&2; exit 1; }
chmod 700 "$REG_ROOT"
touch "$REG_ROOT/.t3-ephemeral-registry"
read_instance_var() { sed -n -e "s/^export $2='\(.*\)'\$/\1/p" -e "s/^export $2=\"\(.*\)\"\$/\1/p" "$1" 2>/dev/null | head -1; }
# True iff pid is alive AND was started for this instance — guards against
# PID reuse (see t3-down.sh, which applies the same check before kill).
# Prefers /proc environ; falls back to matching the recorded launch args
# where /proc is unavailable (macOS/BSD).
pid_is_instance() {
  local pid="$1" home="$2" entry="${3:-}" port="${4:-}" args
  [[ "$pid" =~ ^[0-9]+$ && -n "$home" ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  if [[ -r "/proc/$pid/environ" ]]; then
    tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | grep -qxF "T3CODE_HOME=$home"
  else
    args="$(ps -p "$pid" -o args= 2>/dev/null)" || return 1
    [[ -n "$entry" && -n "$port" && "$args" == *"$entry"* && "$args" == *"--port $port"* ]]
  fi
}
# True iff the recorded home is, after canonicalization, a non-symlink direct
# child of /tmp matching the mktemp pattern — the only thing we ever rm -rf.
safe_ephemeral_home() {
  local h="$1" r
  [[ -n "$h" && ! -L "$h" ]] || return 1
  r="$(readlink -f "$h" 2>/dev/null)" || return 1
  [[ "$r" == "$h" && "$r" =~ ^/tmp/t3-ephemeral-[A-Za-z0-9._-]+$ ]]
}
REG="$REG_ROOT/$NAME"
if [[ -e "$REG/instance.env" ]]; then
  old_pid="$(read_instance_var "$REG/instance.env" T3_PID)"
  old_home="$(read_instance_var "$REG/instance.env" T3_HOME)"
  old_entry="$(read_instance_var "$REG/instance.env" T3_ENTRY)"
  old_port="$(read_instance_var "$REG/instance.env" T3_PORT)"
  if pid_is_instance "$old_pid" "$old_home" "$old_entry" "$old_port"; then
    echo "t3-up: instance '$NAME' already running (pid $old_pid). Use it, or t3-down.sh $NAME first." >&2
    exit 1
  fi
  echo "t3-up: cleaning stale instance '$NAME'" >&2
  # Atomic reap-claim: rename the stale entry first (only one racer's mv can
  # succeed), so a slower racer can never rm -rf a winner's fresh registration.
  reap="$REG_ROOT/.reap-$NAME-$$"
  if mv "$REG" "$reap" 2>/dev/null; then
    # The dead leader's group members (provider CLIs, etc.) may still be
    # running against the old home — reap each one we can PROVE is ours
    # before deleting that home (same identity check as t3-down).
    if [[ "$old_pid" =~ ^[0-9]+$ && -n "$old_home" ]]; then
      for p in $(pgrep -g "$old_pid" 2>/dev/null); do
        if tr '\0' '\n' < "/proc/$p/environ" 2>/dev/null | grep -qxF "T3CODE_HOME=$old_home"; then
          kill -9 "$p" 2>/dev/null || true
        fi
      done
    fi
    safe_ephemeral_home "$old_home" && rm -rf "$old_home"
    rm -rf "$reap"
  else
    echo "t3-up: another t3-up is already reaping stale '$NAME'; retry in a moment" >&2
    exit 1
  fi
elif [[ -e "$REG" ]]; then
  # Either junk we didn't create or another t3-up mid-boot: never adopt it.
  echo "t3-up: $REG exists but has no instance.env (another t3-up in flight, or junk); remove it yourself or pick another --name" >&2
  exit 1
fi

if [[ "$ENTRY" == "apps/server/dist/bin.mjs" && ! -f "$ENTRY" ]]; then
  echo "t3-up: $ENTRY not built; falling back to source entry (slower boot)." >&2
  ENTRY="apps/server/src/bin.ts"
  [[ "$BOOT_TIMEOUT" -lt 240 ]] && BOOT_TIMEOUT=240
fi

T3_HOME="$(mktemp -d "/tmp/t3-ephemeral-XXXXXX")"
SRV_PID=""
REGISTERED=0
CLAIMED=0
# Until instance.env is registered, t3-down.sh cannot discover this instance —
# so any early exit (error, INT/TERM, caller timeout) must reap the server and
# temp home here. Disarmed once registration succeeds.
cleanup_on_abort() {
  [[ "$REGISTERED" == "1" ]] && return 0
  if [[ -n "$SRV_PID" ]]; then
    kill -- "-$SRV_PID" 2>/dev/null || kill "$SRV_PID" 2>/dev/null || true
    wait "$SRV_PID" 2>/dev/null || true
  fi
  rm -rf "$T3_HOME"
  [[ "$CLAIMED" == "1" ]] && rm -rf "$REG"
}
trap cleanup_on_abort EXIT
trap 'cleanup_on_abort; exit 130' INT
trap 'cleanup_on_abort; exit 143' TERM
fail() { exit 1; }

# Atomically CLAIM the name before booting (mkdir fails if it exists), so two
# concurrent `t3-up --name X` can't both boot and leak the loser's server.
# Claimed under the abort trap: any early exit releases the claim.
mkdir "$REG" 2>/dev/null || { echo "t3-up: lost the claim race for '$NAME' (another t3-up just took it)" >&2; exit 1; }
CLAIMED=1

PORT=""
ready=0
for candidate in $(seq 13910 13940); do
  : > "$T3_HOME/server.log"
  # setsid: survive this script's exit AND the caller's shell; loopback-only,
  # no exposure/trace inheritance, log level pinned so readiness is detectable.
  env -u T3CODE_TAILSCALE_SERVE -u T3CODE_TAILSCALE_SERVE_PORT \
    -u T3CODE_TRACE_FILE -u T3CODE_OTLP_TRACES_URL -u T3CODE_OTLP_METRICS_URL \
    -u VITE_DEV_SERVER_URL \
    T3CODE_HOME="$T3_HOME" T3CODE_NO_BROWSER=1 T3CODE_LOG_LEVEL=Info \
    setsid node "$ENTRY" serve --port "$candidate" --host 127.0.0.1 \
    > "$T3_HOME/server.log" 2>&1 < /dev/null &
  SRV_PID=$!
  for ((i = 1; i <= BOOT_TIMEOUT; i++)); do
    kill -0 "$SRV_PID" 2>/dev/null || break
    if grep -q "Listening on http://127.0.0.1:$candidate" "$T3_HOME/server.log"; then
      ready=1
      break
    fi
    sleep 1
  done
  if [[ "$ready" == "1" ]]; then PORT="$candidate"; break; fi
  kill "$SRV_PID" 2>/dev/null || true; wait "$SRV_PID" 2>/dev/null || true; SRV_PID=""
  if grep -qiE "EADDRINUSE|address already in use|port .* in use" "$T3_HOME/server.log"; then
    continue
  fi
  echo "t3-up: server failed to start on port $candidate:" >&2
  tail -20 "$T3_HOME/server.log" >&2
  fail
done
[[ "$ready" == "1" ]] || { echo "t3-up: no usable port in 13910-13940 (timeout ${BOOT_TIMEOUT}s)" >&2; fail; }

# Same env hygiene as the server launch: an inherited VITE_DEV_SERVER_URL
# would flip the CLI to the `dev` state dir and mint a token the server
# (running on `userdata`) never sees.
T3_TOKEN="$(env -u VITE_DEV_SERVER_URL T3CODE_HOME="$T3_HOME" node "$ENTRY" auth session issue --token-only 2>>"$T3_HOME/server.log")" || true
[[ -n "$T3_TOKEN" ]] || { echo "t3-up: failed to mint bearer; recent log:" >&2; tail -10 "$T3_HOME/server.log" >&2; fail; }

T3_ORIGIN="http://127.0.0.1:$PORT"
# Command-ready = authed snapshot 200 (HTTP listening alone is not enough).
ac=""
for _ in $(seq 1 40); do
  ac="$(curl -s -o /dev/null -m 3 -w "%{http_code}" -H "authorization: Bearer $T3_TOKEN" "$T3_ORIGIN/api/orchestration/snapshot" || true)"
  [[ "$ac" == "200" ]] && break
  sleep 0.5
done
[[ "$ac" == "200" ]] || { echo "t3-up: never command-ready (authed snapshot ${ac:-none}):" >&2; tail -20 "$T3_HOME/server.log" >&2; fail; }

T3_DB=""
for _ in $(seq 1 20); do
  T3_DB="$(find "$T3_HOME" -maxdepth 2 -name state.sqlite 2>/dev/null | head -1)"
  [[ -n "$T3_DB" ]] && break
  sleep 0.2
done

# ($REG was atomically claimed above)
# Values are SINGLE-quoted with embedded quotes escaped: instance.env is
# documented for sourcing/eval, so nothing in it may expand or execute
# (e.g. an --entry path containing $ or backticks).
shq() { printf "%s" "$1" | sed "s/'/'\\\\''/g"; }
{
  echo "# Consumers shell out to the T3 CLI against this instance: an inherited dev"
  echo "# URL would flip the CLI to a different state dir than the server's."
  echo "unset VITE_DEV_SERVER_URL 2>/dev/null || true"
  printf "export %s='%s'\n" \
    T3_NAME "$(shq "$NAME")" \
    T3_ORIGIN "$(shq "$T3_ORIGIN")" \
    T3_TOKEN "$(shq "$T3_TOKEN")" \
    T3_DB "$(shq "$T3_DB")" \
    T3_HOME "$(shq "$T3_HOME")" \
    T3CODE_HOME "$(shq "$T3_HOME")" \
    T3_PORT "$(shq "$PORT")" \
    T3_PID "$(shq "$SRV_PID")" \
    T3_ENTRY "$(shq "$ENTRY")"
} > "$REG/instance.env"
ln -sfn "$T3_HOME" "$REG/home"
# Registered: t3-down.sh owns the lifecycle from here on.
REGISTERED=1
trap - EXIT INT TERM

echo "t3-up: ready name=$NAME pid=$SRV_PID port=$PORT home=$T3_HOME" >&2
echo "t3-up: web UI at $T3_ORIGIN (pair via e2e/ui.mjs or auth pairing create)" >&2
echo "t3-up: tear down with: $(dirname "${BASH_SOURCE[0]}")/t3-down.sh $NAME" >&2
cat "$REG/instance.env"
