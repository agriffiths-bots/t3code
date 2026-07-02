#!/usr/bin/env bash
# One-time per-clone setup for the software-factory commit gate.
#
# The hook shims are COPIED OUT OF THE WORKTREE (to ~/.openclaw/factory-hooks/
# <repo>) and core.hooksPath points there — so a staged edit to .githooks/*
# can never replace the hook that judges the very commit carrying it. The
# shims themselves prefer the HEAD version of the gate for the same reason.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
command -v jq >/dev/null || { echo "install-hooks: jq is required" >&2; exit 1; }
chmod +x "$ROOT/.githooks/"* "$ROOT/scripts/factory/"*.sh

HOOKS_DIR="${FACTORY_HOOKS_DIR:-$HOME/.openclaw/factory-hooks/$(basename "$ROOT")}"
mkdir -p "$HOOKS_DIR"
cp "$ROOT/.githooks/pre-commit" "$ROOT/.githooks/pre-merge-commit" "$HOOKS_DIR/"
chmod +x "$HOOKS_DIR/"*
git -C "$ROOT" config core.hooksPath "$HOOKS_DIR"
echo "install-hooks: core.hooksPath=$HOOKS_DIR (out-of-tree) — the factory gate is active."
echo "Pre-warm the gate before committing with: scripts/factory/precommit-gate.sh --prepare"
