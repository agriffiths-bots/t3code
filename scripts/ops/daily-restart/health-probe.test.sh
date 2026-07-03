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
shift
exec "$@"
SH
  cat > "$dir/systemctl" <<'SH'
#!/usr/bin/env bash
echo "${FAKE_SYSTEMD:-active}"
[[ "${FAKE_SYSTEMD_RC:-0}" == "0" ]] || exit "$FAKE_SYSTEMD_RC"
SH
  cat > "$dir/curl" <<'SH'
#!/usr/bin/env bash
printf '%s' "${FAKE_HTTP_CODE:-200}"
[[ "${FAKE_CURL_RC:-0}" == "0" ]] || exit "$FAKE_CURL_RC"
SH
  cat > "$dir/node" <<'SH'
#!/usr/bin/env bash
if [[ "$1" == "-" ]]; then
  cat >/dev/null
  echo "${FAKE_NODE_OUTPUT:-completed thread=fake}"
  [[ "${FAKE_NODE_RC:-0}" == "0" ]] || exit "$FAKE_NODE_RC"
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
  T3DR_TEST_PATH_PREFIX="$tmp/bin" T3_TOKEN=fake-token "$SCRIPT" --origin http://127.0.0.1:1 --service fake.service "$@" >"$tmp/out" 2>"$tmp/err"
  rc=$?
  out="$(cat "$tmp/out")"
  printf '%s\n%s\n%s\n' "$rc" "$out" "$(cat "$tmp/err")" > "$tmp/result"
  echo "$tmp/result"
}

assert_contains() {
  local file="$1" needle="$2" label="$3"
  if grep -Fq "$needle" "$file"; then
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

export FAKE_SYSTEMD=inactive FAKE_SYSTEMD_RC=3
result="$(run_probe "systemd fails")"
if [[ "$(sed -n '1p' "$result")" != "0" ]]; then pass "systemd failure exits nonzero"; else fail "systemd failure exits nonzero"; fi
assert_contains "$result" "CHECK systemd FAIL inactive" "systemd fail line"
unset FAKE_SYSTEMD FAKE_SYSTEMD_RC

export FAKE_HTTP_CODE=500
result="$(run_probe "http fails")"
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
