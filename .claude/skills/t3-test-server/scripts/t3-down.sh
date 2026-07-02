#!/usr/bin/env bash
# t3-down.sh — tear down persistent ephemeral T3 instances started by t3-up.sh.
#
#   t3-down.sh [NAME]     # default: "default"
#   t3-down.sh --all      # every registered instance (incl. stale ones)
#
# LINUX-ONLY tooling (the factory VPS) — see t3-up.sh.
set -uo pipefail

# Same private registry as t3-up.sh: per-user, ownership-checked, and PARSED
# (never sourced) — see t3-up.sh for the rationale.
REG_ROOT="${T3_EPHEMERAL_REGISTRY:-$HOME/.cache/t3-ephemeral/instances}"
# Strip trailing slashes: a slash-qualified path makes bash FOLLOW a symlink
# in the -L test below, defeating the check.
while [[ "$REG_ROOT" == */ && "$REG_ROOT" != "/" ]]; do REG_ROOT="${REG_ROOT%/}"; done
if [[ -e "$REG_ROOT" ]]; then
  [[ -O "$REG_ROOT" && ! -L "$REG_ROOT" ]] || { echo "t3-down: refusing registry $REG_ROOT (not owned by us, or a symlink)" >&2; exit 1; }
  # Fail closed if this isn't a t3 registry (marker is written by t3-up):
  # never sweep an arbitrary directory tree, e.g. a mis-set T3_EPHEMERAL_REGISTRY.
  [[ -f "$REG_ROOT/.t3-ephemeral-registry" ]] || { echo "t3-down: $REG_ROOT has no .t3-ephemeral-registry marker; refusing" >&2; exit 1; }
fi
read_instance_var() { sed -n "s/^export $2=\"\(.*\)\"\$/\1/p" "$1" 2>/dev/null | head -1; }

# True iff pid is alive AND was started for this instance — guards against
# PID reuse (a crashed instance's recorded pid may now belong to an unrelated
# process; never signal it). Prefers /proc environ; falls back to matching the
# recorded launch args where /proc is unavailable (macOS/BSD).
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

down_one() {
  local reg="$1" name pid home entry port
  name="$(basename "$reg")"
  if [[ -L "$reg" ]]; then
    # A symlinked entry is never something t3-up created: remove only the
    # link itself (touching "$reg/..." would mutate the symlink's TARGET).
    rm -f "$reg"
    echo "t3-down: '$name' was a symlink, not an instance; removed the link only" >&2
    return 0
  fi
  if [[ ! -f "$reg/instance.env" ]]; then
    # Unknown entry: never rm -rf what we didn't create. Drop our own 'home'
    # symlink if present (only ever a link), then the dir only if empty.
    [[ -L "$reg/home" ]] && rm -f "$reg/home"
    if rmdir "$reg" 2>/dev/null; then
      echo "t3-down: '$name' had no instance.env; removed empty registry entry" >&2
    else
      echo "t3-down: '$name' has no instance.env and is not empty; SKIPPING (inspect $reg yourself)" >&2
    fi
    return 0
  fi
  pid="$(read_instance_var "$reg/instance.env" T3_PID)"
  home="$(read_instance_var "$reg/instance.env" T3_HOME)"
  entry="$(read_instance_var "$reg/instance.env" T3_ENTRY)"
  port="$(read_instance_var "$reg/instance.env" T3_PORT)"
  if ! pid_is_instance "$pid" "$home" "$entry" "$port"; then
    echo "t3-down: '$name' process ${pid:-?} is gone (or the pid was reused); cleaning state only" >&2
    pid=""
  fi
  if [[ -n "$pid" ]]; then
    # t3-up starts the server with setsid, so the pid is a session/group leader:
    # negative pid kills the whole group (server + children).
    kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -0 "$pid" 2>/dev/null && { kill -9 -- "-$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null || true; }
    # The leader can exit while group children (provider CLIs, etc.) linger —
    # SIGKILL the group if ANY member is still alive before removing state.
    kill -0 -- "-$pid" 2>/dev/null && { kill -9 -- "-$pid" 2>/dev/null || true; }
  fi
  if safe_ephemeral_home "$home"; then
    rm -rf "$home"
  elif [[ -n "$home" ]]; then
    echo "t3-down: '$name' home '$home' is not a canonical /tmp/t3-ephemeral-* dir; NOT deleting it" >&2
  fi
  rm -rf "$reg"
  echo "t3-down: '$name' torn down (pid=${pid:-?} home=${home:-?})" >&2
}

if [[ "${1:-}" == "--all" ]]; then
  found=0
  for reg in "$REG_ROOT"/*/; do
    [[ -d "$reg" ]] || continue
    found=1
    down_one "${reg%/}"
  done
  [[ "$found" == "1" ]] || echo "t3-down: no instances registered" >&2
  exit 0
fi

NAME="${1:-default}"
# Same constraint as t3-up.sh: keeps $REG_ROOT/$NAME a direct child of the
# registry so the rm -rf below can never traverse outside it.
[[ "$NAME" =~ ^[A-Za-z0-9_-]+$ ]] || { echo "t3-down: NAME must be [A-Za-z0-9_-]+" >&2; exit 2; }
[[ -d "$REG_ROOT/$NAME" ]] || { echo "t3-down: no instance named '$NAME'" >&2; exit 1; }
down_one "$REG_ROOT/$NAME"
