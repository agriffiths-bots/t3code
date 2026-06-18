#!/usr/bin/env bash
# drive.sh — one-shot wrapper: mint an admin bearer against a running t3
# instance's home, then drive a create-project + create-thread + send-turn +
# read-output cycle through drive.mjs.
#
# Prereqs: a t3 server already running, e.g.
#   cd apps/server && T3CODE_HOME=/tmp/t3-e2e-drive T3CODE_NO_BROWSER=1 \
#     T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD=1 \
#     node src/bin.ts serve --port 13910 --host 127.0.0.1 /tmp/t3-e2e-proj
#
# Usage:
#   ./drive.sh [HOME] [ORIGIN] [WORKSPACE] [INSTANCE] [MODEL] [PROMPT]
set -euo pipefail

HOME_DIR="${1:-/tmp/t3-e2e-drive}"
ORIGIN="${2:-http://127.0.0.1:13910}"
WORKSPACE="${3:-/tmp/t3-e2e-proj}"
INSTANCE="${4:-claudeAgent}"
MODEL="${5:-claude-sonnet-4-6}"
PROMPT="${6:-reply with the single word READY and nothing else}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[drive.sh] minting admin bearer token (home=$HOME_DIR)"
TOKEN="$(cd "$REPO_ROOT/apps/server" && T3CODE_HOME="$HOME_DIR" \
  node src/bin.ts auth session issue --token-only --label "e2e-drive" 2>/dev/null)"

if [[ -z "$TOKEN" ]]; then
  echo "[drive.sh] FATAL: failed to mint token" >&2
  exit 2
fi

T3_TOKEN="$TOKEN" node "$REPO_ROOT/e2e/drive.mjs" \
  --origin "$ORIGIN" \
  --db "$HOME_DIR/userdata/state.sqlite" \
  --workspace "$WORKSPACE" \
  --instance "$INSTANCE" \
  --model "$MODEL" \
  --prompt "$PROMPT"
