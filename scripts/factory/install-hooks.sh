#!/usr/bin/env bash
# One-time per-clone setup for the software-factory commit gate.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
command -v jq >/dev/null || { echo "install-hooks: jq is required" >&2; exit 1; }
chmod +x "$ROOT/.githooks/"* "$ROOT/scripts/factory/"*.sh
git -C "$ROOT" config core.hooksPath .githooks
echo "install-hooks: core.hooksPath=.githooks — the factory pre-commit gate is active."
echo "Pre-warm the gate before committing with: scripts/factory/precommit-gate.sh --prepare"
