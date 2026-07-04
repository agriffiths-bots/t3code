#!/usr/bin/env bash
set -u

ROOT="$(git rev-parse --show-toplevel)"
SCRIPT="$ROOT/scripts/ops/daily-restart/health-probe"

pass_count=0
fail_count=0

fail() {
  echo "not ok - $1" >&2
  fail_count=$((fail_count + 1))
}

pass() {
  echo "ok - $1"
  pass_count=$((pass_count + 1))
}

make_fake_bin() {
  local dir="$1"
  mkdir -p "$dir"
  cat > "$dir/timeout" <<'SH'
#!/usr/bin/env bash
if [[ -n "${FAKE_NODE_LOG:-}" ]]; then
  printf 'timeout args=%s\n' "$*" >> "$FAKE_NODE_LOG"
fi
while [[ "${1:-}" == --* ]]; do
  case "$1" in
    --kill-after)
      shift 2
      ;;
    --kill-after=*)
      shift
      ;;
    *)
      shift
      ;;
  esac
done
shift
exec "$@"
SH
  cat > "$dir/systemctl" <<'SH'
#!/usr/bin/env bash
if [[ -n "${FAKE_SYSTEMD_SEQUENCE_FILE:-}" && -f "$FAKE_SYSTEMD_SEQUENCE_FILE" ]]; then
  IFS= read -r sequence < "$FAKE_SYSTEMD_SEQUENCE_FILE"
  current="${sequence%%,*}"
  if [[ "$sequence" == *,* ]]; then
    printf '%s\n' "${sequence#*,}" > "$FAKE_SYSTEMD_SEQUENCE_FILE"
  else
    : > "$FAKE_SYSTEMD_SEQUENCE_FILE"
  fi
  echo "$current"
  [[ "$current" == "active" ]] && exit 0
  exit "${FAKE_SYSTEMD_RC:-3}"
fi
echo "${FAKE_SYSTEMD:-active}"
[[ "${FAKE_SYSTEMD_RC:-0}" == "0" ]] || exit "$FAKE_SYSTEMD_RC"
SH
  cat > "$dir/curl" <<'SH'
#!/usr/bin/env bash
origin="${@: -1}"
if [[ -n "${FAKE_NODE_LOG:-}" ]]; then
  printf 'curl origin=%s\n' "$origin" >> "$FAKE_NODE_LOG"
fi
if [[ -n "${FAKE_HTTP_SEQUENCE_FILE:-}" && -f "$FAKE_HTTP_SEQUENCE_FILE" ]]; then
  IFS= read -r sequence < "$FAKE_HTTP_SEQUENCE_FILE"
  current="${sequence%%,*}"
  if [[ "$sequence" == *,* ]]; then
    printf '%s\n' "${sequence#*,}" > "$FAKE_HTTP_SEQUENCE_FILE"
  else
    : > "$FAKE_HTTP_SEQUENCE_FILE"
  fi
  printf '%s' "$current"
  [[ "$current" == "200" ]] && exit 0
  exit "${FAKE_CURL_RC:-22}"
fi
printf '%s' "${FAKE_HTTP_CODE:-200}"
[[ "${FAKE_CURL_RC:-0}" == "0" ]] || exit "$FAKE_CURL_RC"
SH
  cat > "$dir/node" <<'SH'
#!/usr/bin/env bash
if [[ -n "${FAKE_NODE_LOG:-}" ]]; then
  printf 'node args=%s\n' "$*" >> "$FAKE_NODE_LOG"
  printf 'node dev_url=%s args=%s\n' "${VITE_DEV_SERVER_URL-unset}" "$*" >> "$FAKE_NODE_LOG"
fi
if [[ "$1" == "-" ]]; then
  cat >/dev/null
  echo "${FAKE_NODE_OUTPUT:-completed thread=fake}"
  [[ "${FAKE_NODE_RC:-0}" == "0" ]] || exit "$FAKE_NODE_RC"
  exit 0
fi
if [[ "$1" == "-e" ]]; then
  cat >/dev/null
  printf '%s\n%s\n' "${FAKE_TOKEN:-token}" "${FAKE_SESSION_ID:-session}"
  exit 0
fi
if [[ "$*" == *"auth session issue"* ]]; then
  printf '{"token":"%s","sessionId":"%s"}\n' "${FAKE_TOKEN:-token}" "${FAKE_SESSION_ID:-session}"
  exit 0
fi
if [[ "$*" == *"auth session revoke"* ]]; then
  exit 0
fi
echo "${FAKE_TOKEN:-token}"
SH
  chmod +x "$dir"/timeout "$dir"/systemctl "$dir"/curl "$dir"/node
}

run_probe() {
  local name="$1"
  shift
  local tmp out rc
  tmp="$(mktemp -d)"
  make_fake_bin "$tmp/bin"
  FAKE_NODE_LOG="$tmp/node.log" T3DR_TEST_PATH_PREFIX="$tmp/bin" T3_TOKEN=fake-token "$SCRIPT" --origin http://127.0.0.1:1 --service fake.service --instance fakeAgent --model fake-model "$@" >"$tmp/out" 2>"$tmp/err"
  rc=$?
  out="$(cat "$tmp/out")"
  printf '%s\n%s\n%s\n' "$rc" "$out" "$(cat "$tmp/err")" > "$tmp/result"
  echo "$tmp/result"
}

run_probe_without_supplied_token() {
  local tmp out rc
  tmp="$(mktemp -d)"
  make_fake_bin "$tmp/bin"
  FAKE_NODE_LOG="$tmp/node.log" T3DR_TEST_PATH_PREFIX="$tmp/bin" T3DR_CHECKOUT="$tmp/checkout" T3CODE_HOME="$tmp/state" "$SCRIPT" --origin http://127.0.0.1:1 --service fake.service --instance fakeAgent --model fake-model "$@" >"$tmp/out" 2>"$tmp/err"
  rc=$?
  out="$(cat "$tmp/out")"
  printf '%s\n%s\n%s\n' "$rc" "$out" "$(cat "$tmp/err")" > "$tmp/result"
  echo "$tmp/result"
}

run_probe_exact() {
  local tmp out rc
  tmp="$(mktemp -d)"
  make_fake_bin "$tmp/bin"
  FAKE_NODE_LOG="$tmp/node.log" T3DR_TEST_PATH_PREFIX="$tmp/bin" T3_TOKEN=fake-token "$SCRIPT" "$@" >"$tmp/out" 2>"$tmp/err"
  rc=$?
  out="$(cat "$tmp/out")"
  printf '%s\n%s\n%s\n' "$rc" "$out" "$(cat "$tmp/err")" > "$tmp/result"
  echo "$tmp/result"
}

assert_contains() {
  local file="$1" needle="$2" label="$3"
  if grep -Fq -- "$needle" "$file"; then
    pass "$label"
  else
    fail "$label: missing $needle in $(cat "$file")"
  fi
}

result="$(run_probe "all pass")"
if [[ "$(sed -n '1p' "$result")" == "0" ]]; then pass "all checks pass exits zero"; else fail "all checks pass exits zero"; fi
assert_contains "$result" "CHECK systemd PASS active" "systemd pass line"
assert_contains "$result" "CHECK http PASS 200" "http pass line"
assert_contains "$result" "CHECK spawn_wake PASS completed thread=fake" "spawn pass line"

for flag in --origin --service --timeout --instance --model; do
  result="$(run_probe_exact "$flag")"
  if [[ "$(sed -n '1p' "$result")" == "2" ]]; then pass "$flag missing value exits usage"; else fail "$flag missing value exits usage"; fi
  assert_contains "$result" "health-probe: $flag requires a value" "$flag missing value message"
done

result="$(run_probe_exact --origin http://127.0.0.1:1 --service fake.service)"
if [[ "$(sed -n '1p' "$result")" == "2" ]]; then pass "smoke instance is required"; else fail "smoke instance is required"; fi
assert_contains "$result" "health-probe: --instance or T3DR_SMOKE_INSTANCE is required" "missing smoke instance message"

result="$(run_probe_exact --origin http://127.0.0.1:1 --service fake.service --instance fakeAgent)"
if [[ "$(sed -n '1p' "$result")" == "2" ]]; then pass "smoke model is required"; else fail "smoke model is required"; fi
assert_contains "$result" "health-probe: --model or T3DR_SMOKE_MODEL is required" "missing smoke model message"

result="$(run_probe "trailing slash origin" --origin http://127.0.0.1:1/)"
node_log="$(dirname "$result")/node.log"
assert_contains "$node_log" "node args=- http://127.0.0.1:1 120" "origin normalized before smoke request construction"
assert_contains "$node_log" "fakeAgent fake-model" "explicit smoke provider is passed to smoke child"
assert_contains "$node_log" "timeout args=--kill-after=10 150 node -" "smoke child has process-level timeout"
assert_contains "$node_log" "curl origin=http://127.0.0.1:1" "origin normalized before http readiness check"

result="$(run_probe "base-ten timeout" --timeout 08)"
node_log="$(dirname "$result")/node.log"
if [[ "$(sed -n '1p' "$result")" == "0" ]]; then pass "zero-padded timeout parses as base ten"; else fail "zero-padded timeout parses as base ten"; fi
assert_contains "$node_log" "node args=- http://127.0.0.1:1 8" "zero-padded timeout normalized before smoke child"
assert_contains "$node_log" "timeout args=--kill-after=10 38 node -" "zero-padded timeout normalized before process timeout"

export VITE_DEV_SERVER_URL=http://dev.invalid
result="$(run_probe_without_supplied_token --timeout 301)"
unset VITE_DEV_SERVER_URL
node_log="$(dirname "$result")/node.log"
assert_contains "$node_log" "--ttl 391s" "default minted token ttl scales with timeout"
assert_contains "$node_log" "node dev_url=unset args=" "token issue/revoke clear dev server URL"
assert_contains "$result" "CHECK spawn_wake PASS completed thread=fake" "minted token smoke pass line"

sequence_dir="$(mktemp -d)"
printf 'activating,active\n' > "$sequence_dir/systemd"
printf '000,200\n' > "$sequence_dir/http"
export FAKE_SYSTEMD_SEQUENCE_FILE="$sequence_dir/systemd" FAKE_HTTP_SEQUENCE_FILE="$sequence_dir/http"
result="$(run_probe "readiness retries" --timeout 3)"
if [[ "$(sed -n '1p' "$result")" == "0" ]]; then pass "transient readiness failures are retried"; else fail "transient readiness failures are retried"; fi
assert_contains "$result" "CHECK systemd PASS active" "systemd retry pass line"
assert_contains "$result" "CHECK http PASS 200" "http retry pass line"
unset FAKE_SYSTEMD_SEQUENCE_FILE FAKE_HTTP_SEQUENCE_FILE

export FAKE_SYSTEMD=$'active\nwarning: ignored diagnostic' FAKE_SYSTEMD_RC=0
result="$(run_probe "systemd active with diagnostics")"
if [[ "$(sed -n '1p' "$result")" == "0" ]]; then pass "systemd active exit status passes despite diagnostics"; else fail "systemd active exit status passes despite diagnostics"; fi
assert_contains "$result" "CHECK systemd PASS active" "systemd diagnostic pass line"
unset FAKE_SYSTEMD FAKE_SYSTEMD_RC

export FAKE_SYSTEMD=inactive FAKE_SYSTEMD_RC=3
result="$(run_probe "systemd fails" --timeout 1)"
if [[ "$(sed -n '1p' "$result")" != "0" ]]; then pass "systemd failure exits nonzero"; else fail "systemd failure exits nonzero"; fi
assert_contains "$result" "CHECK systemd FAIL inactive" "systemd fail line"
unset FAKE_SYSTEMD FAKE_SYSTEMD_RC

export FAKE_HTTP_CODE=500
result="$(run_probe "http fails" --timeout 1)"
if [[ "$(sed -n '1p' "$result")" != "0" ]]; then pass "http failure exits nonzero"; else fail "http failure exits nonzero"; fi
assert_contains "$result" "CHECK http FAIL 500" "http fail line"
unset FAKE_HTTP_CODE

export FAKE_NODE_RC=7 FAKE_NODE_OUTPUT="turn error"
result="$(run_probe "smoke fails")"
if [[ "$(sed -n '1p' "$result")" != "0" ]]; then pass "spawn failure exits nonzero"; else fail "spawn failure exits nonzero"; fi
assert_contains "$result" "CHECK spawn_wake FAIL turn error" "spawn fail line"
unset FAKE_NODE_RC FAKE_NODE_OUTPUT

echo "$pass_count passed, $fail_count failed"
[[ "$fail_count" -eq 0 ]]
