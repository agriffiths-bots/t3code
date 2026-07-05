#!/usr/bin/env bash
set -u

ROOT="$(git rev-parse --show-toplevel)"
SCRIPT="$ROOT/scripts/ops/daily-restart/t3-daily-restart"

pass_count=0
fail_count=0

pass() { echo "ok - $1"; pass_count=$((pass_count + 1)); }
fail() { echo "not ok - $1" >&2; fail_count=$((fail_count + 1)); }

make_fake_bin() {
  local dir="$1"
  mkdir -p "$dir"
  cat >"$dir/git" <<'SH'
#!/usr/bin/env bash
echo "git $*" >>"$T_LOG"
case "$*" in
  *"rev-parse HEAD"*) echo "${FAKE_PRE_SHA:-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}" ;;
  *"rev-parse origin/main"*) echo "${FAKE_TARGET_SHA:-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}" ;;
  *"rev-parse --verify"*)
    value="${*: -1}"
    echo "${value%%^*}"
    ;;
  *"merge --ff-only"*)
    if [[ "${FAKE_TARGET_WEAKENS_HEALTH_PROBE:-0}" == "1" ]]; then
      cat >"$T_TMP/bin/health-probe" <<'WEAK'
#!/usr/bin/env bash
echo "health-weakened $*" >>"$T_LOG"
echo "CHECK systemd PASS active"
echo "CHECK http PASS 200"
echo "CHECK spawn_wake PASS completed"
WEAK
      chmod +x "$T_TMP/bin/health-probe"
    fi
    exit "${FAKE_GIT_RC:-0}"
    ;;
  *"diff --quiet"*) exit "${FAKE_MIGRATION_DIFF_RC:-0}" ;;
  *) exit "${FAKE_GIT_RC:-0}" ;;
esac
SH
  cat >"$dir/systemctl" <<'SH'
#!/usr/bin/env bash
echo "systemctl $*" >>"$T_LOG"
state_file="$T_TMP/systemctl-state"
[[ -f "$state_file" ]] || echo active >"$state_file"
case "$*" in
  --user\ is-active\ *)
    state="$(cat "$state_file")"
    echo "$state"
    [[ "$state" == "active" ]] && exit 0
    exit 3
    ;;
  --user\ stop\ *)
    stop_count_file="$T_TMP/systemctl-stop-count"
    stop_count=0
    [[ -f "$stop_count_file" ]] && stop_count="$(cat "$stop_count_file")"
    stop_count=$((stop_count + 1))
    echo "$stop_count" >"$stop_count_file"
    if [[ "${FAKE_SYSTEMCTL_HANG_STOP:-0}" == "1" || ( -n "${FAKE_SYSTEMCTL_HANG_STOP_N:-}" && "$stop_count" == "$FAKE_SYSTEMCTL_HANG_STOP_N" ) ]]; then
      sleep "${FAKE_SYSTEMCTL_HANG_SECS:-5}"
      exit 124
    fi
    if [[ -n "${FAKE_SYSTEMCTL_FAIL_STOP_N:-}" && "$stop_count" == "$FAKE_SYSTEMCTL_FAIL_STOP_N" ]]; then
      exit "${FAKE_SYSTEMCTL_FAIL_RC:-9}"
    fi
    echo inactive >"$state_file"
    exit "${FAKE_SYSTEMCTL_RC:-0}"
    ;;
  --user\ start\ *)
    echo active >"$state_file"
    exit "${FAKE_SYSTEMCTL_RC:-0}"
    ;;
  --user\ kill\ *|--user\ kill)
    if [[ "${FAKE_SYSTEMCTL_KILL_FAIL:-0}" == "1" ]]; then
      exit "${FAKE_SYSTEMCTL_KILL_RC:-9}"
    fi
    echo inactive >"$state_file"
    exit "${FAKE_SYSTEMCTL_RC:-0}"
    ;;
esac
exit "${FAKE_SYSTEMCTL_RC:-0}"
SH
  cat >"$dir/t3-db-snapshot" <<'SH'
#!/usr/bin/env bash
echo "snapshot $*" >>"$T_LOG"
count_file="$T_TMP/snapshot-count"
count=0
[[ -f "$count_file" ]] && count="$(cat "$count_file")"
count=$((count + 1))
echo "$count" >"$count_file"
if [[ -n "${FAKE_SNAPSHOT_FAIL_N:-}" && "$count" == "$FAKE_SNAPSHOT_FAIL_N" ]]; then
  exit "${FAKE_SNAPSHOT_RC:-9}"
fi
[[ "${FAKE_SNAPSHOT_RC:-0}" == "0" ]] || exit "$FAKE_SNAPSHOT_RC"
echo "$T_TMP/snapshot.sqlite"
SH
  cat >"$dir/t3-db-restore" <<'SH'
#!/usr/bin/env bash
echo "restore $*" >>"$T_LOG"
exit "${FAKE_RESTORE_RC:-0}"
SH
  cat >"$dir/capture-active-threads" <<'SH'
#!/usr/bin/env bash
echo "capture $*" >>"$T_LOG"
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--out" ]]; then
    if [[ "${FAKE_CAPTURE_BAD_JSON:-0}" == "1" ]]; then
      printf '{bad json\n' >"$2"
    else
      printf '{"threads":[]}\n' >"$2"
    fi
    break
  fi
  shift
done
SH
  cat >"$dir/health-probe" <<'SH'
#!/usr/bin/env bash
echo "health $*" >>"$T_LOG"
if [[ "${FAKE_HEALTH_PARTIAL:-0}" == "1" ]]; then
  echo "CHECK systemd PASS active"
  echo "CHECK http PASS 200"
  exit 124
fi
if [[ "${FAKE_HEALTH_FAIL_ONCE:-0}" == "1" && ! -f "$T_TMP/health-failed" ]]; then
  : >"$T_TMP/health-failed"
  echo "CHECK systemd PASS active"
  echo "CHECK http FAIL 500"
  echo "CHECK spawn_wake PASS completed"
  exit 1
fi
echo "CHECK systemd PASS active"
echo "CHECK http PASS 200"
echo "CHECK spawn_wake PASS completed"
SH
  cat >"$dir/inject-resume" <<'SH'
#!/usr/bin/env bash
echo "inject $*" >>"$T_LOG"
SH
  cat >"$dir/pnpm" <<'SH'
#!/usr/bin/env bash
echo "pnpm $*" >>"$T_LOG"
if [[ "${FAKE_PNPM_FAIL_ONCE:-0}" == "1" && ! -f "$T_TMP/pnpm-failed" ]]; then
  : >"$T_TMP/pnpm-failed"
  exit 8
fi
exit "${FAKE_PNPM_RC:-0}"
SH
  cat >"$dir/claude" <<'SH'
#!/usr/bin/env bash
echo "claude $*" >>"$T_LOG"
echo HEALTHY
SH
  chmod +x "$dir"/*
}

run_manager() {
  local tmp="$1"
  shift
  mkdir -p "$tmp/checkout" "$tmp/bin" "$tmp/ledger" "$tmp/snaps"
  make_fake_bin "$tmp/bin"
  T_TMP="$tmp" T_LOG="$tmp/calls.log" T3DR_TEST_PATH_PREFIX="$tmp/bin" T3DR_STOP_TIMEOUT="${T3DR_STOP_TIMEOUT:-1}" T3DR_KILL_TIMEOUT="${T3DR_KILL_TIMEOUT:-1}" "$SCRIPT" \
    --db "$tmp/state.sqlite" \
    --checkout "$tmp/checkout" \
    --service fake.service \
    --origin http://127.0.0.1:1 \
    --snapshot-dir "$tmp/snaps" \
    --ledger "$tmp/ledger" \
    --probe-timeout 1 \
    --smoke-instance fakeAgent \
    --smoke-model fake-model \
    "$@" >"$tmp/stdout" 2>"$tmp/stderr"
  echo $? >"$tmp/rc"
}

assert_order() {
  local file="$1"
  shift
  local last=0 needle line
  for needle in "$@"; do
    line="$(awk -v last="$last" -v needle="$needle" 'index($0, needle) && NR > last { print NR; exit }' "$file")"
    if [[ -z "$line" || "$line" -le "$last" ]]; then
      fail "order contains $needle after line $last"
      return
    fi
    last="$line"
  done
  pass "order $*"
}

tmp="$(mktemp -d)"
run_manager "$tmp"
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "happy path exits zero" || fail "happy path exits zero"
assert_order "$tmp/calls.log" "git -C" "snapshot" "capture" "systemctl --user stop" "snapshot" "git -C" "pnpm -C" "systemctl --user start" "health" "inject"
assert_order "$tmp/calls.log" "snapshot --db" "capture --db" "systemctl --user stop" "snapshot --db"
assert_order "$tmp/calls.log" "git -C" "pnpm -C $tmp/checkout/apps/web run build" "pnpm -C $tmp/checkout run build:desktop"
if awk 'f && /pnpm/ { found=1 } /health/ { f=1 } END { exit found ? 0 : 1 }' "$tmp/calls.log"; then
  fail "happy path built after health"
else
  pass "happy path builds before health"
fi
grep -Fq '"pre_sha"' "$tmp/ledger/"*/resume-manifest.json && pass "manifest filled" || fail "manifest filled"
grep -Fq "snapshot --db $tmp/state.sqlite --out-dir $tmp/snaps" "$tmp/calls.log" && pass "snapshot helper receives current flags" || fail "snapshot helper receives current flags"
grep -Fq "capture --db $tmp/state.sqlite --out $tmp/ledger/" "$tmp/calls.log" && pass "capture helper receives current flags" || fail "capture helper receives current flags"
grep -Fq "health --origin http://127.0.0.1:1 --service fake.service --instance fakeAgent --model fake-model --timeout 1" "$tmp/calls.log" && pass "health probe receives smoke provider" || fail "health probe receives smoke provider"

tmp="$(mktemp -d)"
mkdir -p "$tmp/checkout" "$tmp/bin" "$tmp/ledger" "$tmp/snaps"
make_fake_bin "$tmp/bin"
T_TMP="$tmp" T_LOG="$tmp/calls.log" T3DR_TEST_PATH_PREFIX="$tmp/bin" "$SCRIPT" \
  --db "$tmp/state.sqlite" \
  --checkout "$tmp/checkout" \
  --service fake.service \
  --origin http://127.0.0.1:1 \
  --snapshot-dir "$tmp/snaps" \
  --ledger "$tmp/ledger" \
  --probe-timeout 1 \
  >"$tmp/stdout" 2>"$tmp/stderr"
echo $? >"$tmp/rc"
[[ "$(cat "$tmp/rc")" == "2" ]] && pass "smoke provider config is required" || fail "smoke provider config is required"
grep -Fq "t3-daily-restart: --smoke-instance or T3DR_SMOKE_INSTANCE is required" "$tmp/stderr" && pass "missing smoke provider message" || fail "missing smoke provider message"

tmp="$(mktemp -d)"
export FAKE_SNAPSHOT_RC=9
run_manager "$tmp"
unset FAKE_SNAPSHOT_RC
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "snapshot failure exits nonzero" || fail "snapshot failure exits nonzero"
if grep -Fq "systemctl --user stop" "$tmp/calls.log" 2>/dev/null; then fail "snapshot failure stopped service"; else pass "snapshot failure aborts before stop"; fi

tmp="$(mktemp -d)"
mkdir -p "$tmp/ledger/$(date -u +%F)"
: >"$tmp/ledger/$(date -u +%F)/pinned-tools"
run_manager "$tmp"
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "health probe pin failure exits nonzero" || fail "health probe pin failure exits nonzero"
grep -Fq "RESULT FAILED" "$tmp/ledger/"*/t3-daily-restart.result && pass "health probe pin failure recorded" || fail "health probe pin failure recorded"
if grep -Fq "systemctl --user stop" "$tmp/calls.log"; then
  fail "health probe pin failure stopped service"
else
  pass "health probe pin failure aborts before stop"
fi

tmp="$(mktemp -d)"
export FAKE_SYSTEMCTL_HANG_STOP_N=1
run_manager "$tmp"
unset FAKE_SYSTEMCTL_HANG_STOP_N
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "hung shutdown stop escalates and proceeds" || fail "hung shutdown stop escalates and proceeds"
assert_order "$tmp/calls.log" "systemctl --user stop fake.service" "systemctl --user kill fake.service" "systemctl --user is-active fake.service" "snapshot --db" "git -C $tmp/checkout merge --ff-only"
grep -Fq "RESULT OK" "$tmp/ledger/"*/t3-daily-restart.result && pass "hung shutdown stop records ok after escalation" || fail "hung shutdown stop records ok after escalation"

tmp="$(mktemp -d)"
export FAKE_SYSTEMCTL_HANG_STOP_N=1
export FAKE_SYSTEMCTL_KILL_FAIL=1
run_manager "$tmp"
unset FAKE_SYSTEMCTL_HANG_STOP_N FAKE_SYSTEMCTL_KILL_FAIL
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "hung shutdown aborts when kill cannot stop service" || fail "hung shutdown aborts when kill cannot stop service"
grep -Fq "RESULT STOP-FAILED" "$tmp/ledger/"*/t3-daily-restart.result && pass "hung shutdown kill failure recorded loudly" || fail "hung shutdown kill failure recorded loudly"
if grep -Fq "merge --ff-only" "$tmp/calls.log"; then
  fail "hung shutdown kill failure updated code"
else
  pass "hung shutdown kill failure skips code update"
fi

tmp="$(mktemp -d)"
export FAKE_HEALTH_FAIL_ONCE=1
run_manager "$tmp"
unset FAKE_HEALTH_FAIL_ONCE
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "health rollback exits nonzero" || fail "health rollback exits nonzero"
assert_order "$tmp/calls.log" "health" "systemctl --user stop" "git -C" "restore" "pnpm -C $tmp/checkout/apps/web run build" "pnpm -C $tmp/checkout run build:desktop" "systemctl --user start" "health"
grep -Fq "RESULT ROLLBACK-OK" "$tmp/ledger/"*/t3-daily-restart.result && pass "health rollback result recorded" || fail "health rollback result recorded"
if grep -Fq "diff --quiet" "$tmp/calls.log"; then
  fail "health rollback consulted migration diff"
else
  pass "health rollback skips migration diff"
fi
if awk 'f && /inject/ { found=1 } /pnpm -C .* run build:desktop/ { if (++n == 2) f=1 } END { exit found ? 0 : 1 }' "$tmp/calls.log"; then
  pass "health rollback injects resume"
else
  fail "health rollback injects resume"
fi

tmp="$(mktemp -d)"
export FAKE_HEALTH_FAIL_ONCE=1
export FAKE_TARGET_WEAKENS_HEALTH_PROBE=1
run_manager "$tmp"
unset FAKE_HEALTH_FAIL_ONCE FAKE_TARGET_WEAKENS_HEALTH_PROBE
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "pinned health probe rejects weakened target probe" || fail "pinned health probe rejects weakened target probe"
grep -Fq "RESULT ROLLBACK-OK" "$tmp/ledger/"*/t3-daily-restart.result && pass "pinned health probe forced rollback" || fail "pinned health probe forced rollback"
if grep -Fq "health-weakened" "$tmp/calls.log"; then
  fail "target health probe was used after update"
else
  pass "pinned pre-update health probe used after update"
fi

tmp="$(mktemp -d)"
export FAKE_HEALTH_FAIL_ONCE=1
export FAKE_SYSTEMCTL_FAIL_STOP_N=2
export FAKE_SYSTEMCTL_KILL_FAIL=1
run_manager "$tmp"
unset FAKE_HEALTH_FAIL_ONCE FAKE_SYSTEMCTL_FAIL_STOP_N FAKE_SYSTEMCTL_KILL_FAIL
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "rollback stop escalation failure exits nonzero" || fail "rollback stop escalation failure exits nonzero"
grep -Fq "RESULT ROLLBACK-FAILED" "$tmp/ledger/"*/t3-daily-restart.result && pass "rollback stop escalation failure recorded" || fail "rollback stop escalation failure recorded"
if grep -Fq "restore" "$tmp/calls.log"; then
  fail "rollback stop escalation failure restored db"
else
  pass "rollback stop escalation failure skips db restore"
fi

tmp="$(mktemp -d)"
export FAKE_PNPM_FAIL_ONCE=1
export FAKE_SYSTEMCTL_FAIL_STOP_N=2
run_manager "$tmp"
unset FAKE_PNPM_FAIL_ONCE FAKE_SYSTEMCTL_FAIL_STOP_N
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "code rollback exits nonzero after build failure" || fail "code rollback exits nonzero after build failure"
grep -Fq "RESULT CODE-ROLLBACK-OK" "$tmp/ledger/"*/t3-daily-restart.result && pass "code rollback stop failure proceeds when already inactive" || fail "code rollback stop failure proceeds when already inactive"
if awk '/systemctl --user stop/ { stops++; if (stops == 2) after=1; next } after && /git -C/ { found=1 } END { exit found ? 0 : 1 }' "$tmp/calls.log"; then
  pass "code rollback stop failure verifies inactive before checkout"
else
  fail "code rollback stop failure verifies inactive before checkout"
fi

tmp="$(mktemp -d)"
export FAKE_PNPM_FAIL_ONCE=1
run_manager "$tmp"
unset FAKE_PNPM_FAIL_ONCE
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "web build failure exits nonzero" || fail "web build failure exits nonzero"
assert_order "$tmp/calls.log" "pnpm -C $tmp/checkout/apps/web run build" "systemctl --user stop" "git -C" "pnpm -C $tmp/checkout/apps/web run build" "pnpm -C $tmp/checkout run build:desktop" "systemctl --user start" "health"
if awk 'f && /restore/ { found=1 } /pnpm/ { f=1 } END { exit found ? 0 : 1 }' "$tmp/calls.log"; then
  fail "web build rollback restored db"
else
  pass "web build rollback avoids db restore"
fi
grep -Fq "RESULT CODE-ROLLBACK-OK" "$tmp/ledger/"*/t3-daily-restart.result && pass "web build rollback recorded" || fail "web build rollback recorded"
if awk 'f && /inject/ { found=1 } /pnpm/ { if (++n == 2) f=1 } END { exit found ? 0 : 1 }' "$tmp/calls.log"; then
  pass "code rollback injects resume"
else
  fail "code rollback injects resume"
fi

tmp="$(mktemp -d)"
export FAKE_CAPTURE_BAD_JSON=1
run_manager "$tmp"
unset FAKE_CAPTURE_BAD_JSON
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "bad manifest exits nonzero" || fail "bad manifest exits nonzero"
if grep -Fq "systemctl --user stop" "$tmp/calls.log"; then fail "bad manifest stopped service"; else pass "bad manifest aborts before stop"; fi

tmp="$(mktemp -d)"
export FAKE_HEALTH_PARTIAL=1
run_manager "$tmp"
unset FAKE_HEALTH_PARTIAL
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "partial health exits nonzero" || fail "partial health exits nonzero"
grep -Fq "RESULT ROLLBACK-FAILED" "$tmp/ledger/"*/t3-daily-restart.result && pass "partial health rolled back loudly" || fail "partial health rolled back loudly"

tmp="$(mktemp -d)"
mkdir -p "$tmp/ledger"
exec 8>"$tmp/ledger/t3-daily-restart.lock"
flock -n 8
run_manager "$tmp"
[[ "$(cat "$tmp/rc")" == "75" ]] && pass "lock exits 75" || fail "lock exits 75"
flock -u 8

tmp="$(mktemp -d)"
export FAKE_PRE_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
export FAKE_TARGET_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
run_manager "$tmp"
unset FAKE_PRE_SHA FAKE_TARGET_SHA
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "same-sha rerun exits zero" || fail "same-sha rerun exits zero"
if grep -Fq "pnpm" "$tmp/calls.log"; then fail "same-sha rerun rebuilt web"; else pass "same-sha rerun skips web rebuild"; fi

tmp="$(mktemp -d)"
export FAKE_PRE_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
run_manager "$tmp" --prebuilt-target \
  --rollback-sha aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --target-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
unset FAKE_PRE_SHA
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "prebuilt target exits zero" || fail "prebuilt target exits zero"
if grep -Fq "merge --ff-only" "$tmp/calls.log"; then
  fail "prebuilt target merged during downtime"
else
  pass "prebuilt target skips merge during downtime"
fi
if grep -Fq "pnpm" "$tmp/calls.log"; then
  fail "prebuilt target rebuilt during downtime"
else
  pass "prebuilt target skips rebuild during downtime"
fi

tmp="$(mktemp -d)"
export FAKE_PRE_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
export FAKE_HEALTH_FAIL_ONCE=1
run_manager "$tmp" --prebuilt-target \
  --rollback-sha aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --target-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
unset FAKE_PRE_SHA FAKE_HEALTH_FAIL_ONCE
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "prebuilt health failure exits nonzero" || fail "prebuilt health failure exits nonzero"
grep -Fq "git -C $tmp/checkout checkout aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "$tmp/calls.log" && pass "prebuilt rollback checks out rollback sha" || fail "prebuilt rollback checks out rollback sha"

tmp="$(mktemp -d)"
export FAKE_PRE_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
export FAKE_SNAPSHOT_FAIL_N=2
run_manager "$tmp" --prebuilt-target \
  --rollback-sha aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --target-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
unset FAKE_PRE_SHA FAKE_SNAPSHOT_FAIL_N
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "prebuilt quiesced snapshot failure exits nonzero" || fail "prebuilt quiesced snapshot failure exits nonzero"
assert_order "$tmp/calls.log" "systemctl --user stop fake.service" "snapshot --db" "systemctl --user stop fake.service" "git -C $tmp/checkout checkout aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "pnpm -C $tmp/checkout/apps/web run build" "systemctl --user start fake.service" "health"
grep -Fq "RESULT SNAPSHOT-FAILED" "$tmp/ledger/"*/t3-daily-restart.result && pass "prebuilt snapshot failure recorded" || fail "prebuilt snapshot failure recorded"
grep -Fq "RESULT CODE-ROLLBACK-OK" "$tmp/ledger/"*/t3-daily-restart.result && pass "prebuilt snapshot failure code rollback recorded" || fail "prebuilt snapshot failure code rollback recorded"

echo "$pass_count passed, $fail_count failed"
[[ "$fail_count" -eq 0 ]]
