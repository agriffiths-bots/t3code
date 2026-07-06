#!/usr/bin/env bash
set -u

ROOT="$(git rev-parse --show-toplevel)"
SCRIPT="$ROOT/scripts/ops/daily-restart/verify-restart"
fail_count=0
pass() { echo "ok - $1"; }
fail() { echo "not ok - $1" >&2; fail_count=$((fail_count + 1)); }

make_fake_bin() {
  local dir="$1"
  mkdir -p "$dir"
  cat >"$dir/systemctl" <<'SH'
#!/usr/bin/env bash
case "$*" in
  --user\ is-active\ *) [[ "${FAKE_SYSTEMCTL_ACTIVE:-1}" == "1" ]] && exit 0; exit 3 ;;
  --user\ show\ *) echo "${FAKE_CURRENT_PID:-222}"; exit 0 ;;
esac
exit 1
SH
  cat >"$dir/git" <<'SH'
#!/usr/bin/env bash
case "$*" in
  *"rev-parse --verify"*) echo "${FAKE_RESOLVED_SHA:-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}"; exit "${FAKE_GIT_RC:-0}" ;;
esac
exit 1
SH
  cat >"$dir/curl" <<'SH'
#!/usr/bin/env bash
printf '{"serverBuildSha":"%s"}\n' "${FAKE_DESCRIPTOR_SHA:-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}"
exit "${FAKE_CURL_RC:-0}"
SH
  chmod +x "$dir"/*
}

run_verify() {
  local tmp="$1"
  shift
  mkdir -p "$tmp/bin" "$tmp/checkout"
  make_fake_bin "$tmp/bin"
  env "$@" T_LOG="$tmp/calls.log" T3DR_TEST_PATH_PREFIX="$tmp/bin" "$SCRIPT" \
    --service fake.service \
    --origin http://127.0.0.1:1 \
    --checkout "$tmp/checkout" \
    --expected-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
    --previous-pid 111 \
    --timeout 1 >"$tmp/stdout" 2>"$tmp/stderr"
  echo $? >"$tmp/rc"
}

assert_case() {
  local name="$1" want_rc="$2" needle="$3" tmp rc
  shift 3
  tmp="$(mktemp -d)"
  run_verify "$tmp" "$@"
  rc="$(cat "$tmp/rc")"
  if [[ ( "$want_rc" == "0" && "$rc" == "0" ) || ( "$want_rc" != "0" && "$rc" != "0" ) ]]; then
    pass "$name exits as expected"
  else
    fail "$name exits as expected"
  fi
  grep -Fq "$needle" "$tmp/stdout" && pass "$name reports cause" || fail "$name reports cause"
}

assert_case "matching new pid and build sha" 0 "CHECK server_build_sha PASS expected=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb actual=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
assert_case "unchanged pid" 1 "MainPID did not change" FAKE_CURRENT_PID=111
assert_case "wrong build sha" 1 "serverBuildSha mismatch" FAKE_DESCRIPTOR_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

[[ "$fail_count" -eq 0 ]] || { echo "$fail_count failure(s)" >&2; exit 1; }
