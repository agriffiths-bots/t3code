#!/usr/bin/env bash
# Enforce the provider persistent-consumer lifetime contract from the staged tree.
set -euo pipefail

MODE="${1:---staged}"
case "$MODE" in
  --staged|--all) ;;
  *) echo "usage: provider-consumer-contract-guard.sh [--staged|--all]" >&2; exit 2 ;;
esac

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

LAYERS_DIR="apps/server/src/provider/Layers"
HELPER="apps/server/src/provider/testUtils/providerConsumerLifetimeContract.ts"

if [ "$MODE" = "--staged" ]; then
  relevant=0
  while IFS= read -r path; do
    case "$path" in
      "$LAYERS_DIR"/*Adapter.ts|"$LAYERS_DIR"/*Adapter.test.ts|"$HELPER"|scripts/factory/provider-consumer-contract-guard.sh)
        relevant=1
        break
        ;;
    esac
  done < <(git diff --cached --name-only --no-renames)
  [ "$relevant" -eq 1 ] || exit 0
fi

if ! git cat-file -e ":$HELPER" 2>/dev/null; then
  echo "provider-consumer-contract-guard: missing staged contract helper: $HELPER" >&2
  exit 1
fi

helper_source="$(git show ":$HELPER")"
if ! grep -Fq "export const providerConsumerLifetimeContract" <<<"$helper_source"; then
  echo "provider-consumer-contract-guard: staged helper does not export providerConsumerLifetimeContract" >&2
  exit 1
fi

adapters=()
missing=()
while IFS= read -r source; do
  adapter_name="$(basename "$source" .ts)"
  test_file="${source%.ts}.test.ts"
  adapters+=("$adapter_name")

  if ! git cat-file -e ":$test_file" 2>/dev/null; then
    missing+=("$adapter_name: paired test file is missing ($test_file)")
    continue
  fi

  test_text="$(git show ":$test_file")"
  if ! grep -Fq "providerConsumerLifetimeContract({" <<<"$test_text" \
    || ! grep -Fq "adapterName: \"$adapter_name\"" <<<"$test_text"; then
    missing+=("$adapter_name: $test_file must invoke providerConsumerLifetimeContract with adapterName: \"$adapter_name\"")
  fi
done < <(git ls-files "$LAYERS_DIR/*Adapter.ts" | sort)

if [ "${#missing[@]}" -gt 0 ]; then
  echo "provider-consumer-contract-guard: persistent-consumer adapters without contract coverage:" >&2
  printf '  - %s\n' "${missing[@]}" >&2
  exit 1
fi

if [ "${#adapters[@]}" -eq 0 ]; then
  echo "provider-consumer-contract-guard: no persistent-consumer adapters derived; refusing" >&2
  exit 1
fi

echo "provider-consumer-contract-guard: PASS (${#adapters[@]} adapters: ${adapters[*]})"
