#!/usr/bin/env bash
# smoke.sh — self-test for the t3-test-server skill: boot an ephemeral server,
# assert an authenticated snapshot returns 200, and confirm THIS run's temp home
# was torn down (scoped so concurrent/stale runs don't cause false failures).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

marker="$(mktemp)"
trap 'rm -f "$marker"' EXIT

# t3-ephemeral.sh defaults to the built bundle but auto-falls back to the source
# entry when apps/server/dist is unbuilt, so this runs from a clean checkout.
"$HERE/t3-ephemeral.sh" -- bash -c '
  set -euo pipefail
  printf "%s" "$T3_HOME" > "'"$marker"'"   # record this run'\''s home for the leak check
  code=$(curl -s -o /dev/null -w "%{http_code}" -H "authorization: Bearer $T3_TOKEN" "$T3_ORIGIN/api/orchestration/snapshot")
  echo "authed snapshot http=$code"
  [ -f "$T3_DB" ] || { echo "state DB missing at $T3_DB"; exit 1; }
  [ "$code" = "200" ] || { echo "expected 200, got $code"; exit 1; }
  echo "smoke: OK"
'

home="$(cat "$marker" 2>/dev/null || true)"
if [ -n "$home" ] && [ -d "$home" ]; then
  echo "FAIL: this run's temp home $home was not cleaned up (teardown leaked)" >&2
  exit 1
fi
echo "smoke.sh: PASS"
