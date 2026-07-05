#!/usr/bin/env bash
set -u

ROOT="$(git rev-parse --show-toplevel)"
SCRIPT="$ROOT/scripts/ops/daily-restart/t3-nightly-cycle"

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
  *"rev-parse HEAD"*) echo "${FAKE_HEAD_SHA:-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}" ;;
  *"rev-parse origin/main"*) echo "${FAKE_TARGET_SHA:-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}" ;;
  *"merge --ff-only"*) exit "${FAKE_GIT_MERGE_RC:-0}" ;;
esac
exit "${FAKE_GIT_RC:-0}"
SH
  cat >"$dir/pnpm" <<'SH'
#!/usr/bin/env bash
echo "pnpm $*" >>"$T_LOG"
exit "${FAKE_PNPM_RC:-0}"
SH
  cat >"$dir/fake-backup" <<'SH'
#!/usr/bin/env bash
echo "backup" >>"$T_LOG"
exit "${FAKE_BACKUP_RC:-0}"
SH
  cat >"$dir/fake-sync" <<'SH'
#!/usr/bin/env bash
echo "sync repo=${T3_REPO_DIR:-}" >>"$T_LOG"
exit "${FAKE_SYNC_RC:-0}"
SH
  cat >"$dir/fake-restart" <<'SH'
#!/usr/bin/env bash
echo "restart prebuilt=${T3DR_PREBUILT_TARGET:-0} checkout=${T3DR_CHECKOUT:-} ledger=${T3DR_LEDGER:-} rollback=${T3DR_ROLLBACK_SHA:-} target=${T3DR_TARGET_SHA:-}" >>"$T_LOG"
exit "${FAKE_RESTART_RC:-0}"
SH
  chmod +x "$dir"/*
}

run_cycle() {
  local tmp="$1"
  shift
  mkdir -p "$tmp/bin" "$tmp/checkout" "$tmp/ledger"
  make_fake_bin "$tmp/bin"
  T_TMP="$tmp" T_LOG="$tmp/calls.log" T3DR_TEST_PATH_PREFIX="$tmp/bin" \
    T3DR_CHECKOUT="$tmp/checkout" \
    T3DR_LEDGER="$tmp/ledger" \
    T3DR_BACKUP_CMD="fake-backup" \
    T3DR_UPSTREAM_SYNC_CMD="fake-sync" \
    T3DR_RESTART_CMD="$tmp/bin/fake-restart" \
    T3DR_DEADLINE_LOCAL="23:59" \
    T3DR_DESKTOP_ARTIFACT="${T3DR_DESKTOP_ARTIFACT:-0}" \
    "$SCRIPT" "$@" >"$tmp/stdout" 2>"$tmp/stderr"
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
run_cycle "$tmp"
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "happy path exits zero" || fail "happy path exits zero"
assert_order "$tmp/calls.log" "backup" "sync" "git -C" "git -C $tmp/checkout merge --ff-only" "pnpm -C $tmp/checkout/apps/web run build" "pnpm -C $tmp/checkout run build:desktop" "restart prebuilt=1"
grep -Fq "sync repo=$tmp/checkout" "$tmp/calls.log" && pass "sync receives checkout repo" || fail "sync receives checkout repo"
grep -Fq "checkout=$tmp/checkout ledger=$tmp/ledger" "$tmp/calls.log" && pass "restart receives checkout and ledger" || fail "restart receives checkout and ledger"
grep -Fq "rollback=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa target=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/calls.log" && pass "restart receives prebuilt rollback target" || fail "restart receives prebuilt rollback target"
if grep -Fq "dist:desktop:artifact" "$tmp/calls.log"; then
  fail "desktop artifact blocks by default"
else
  pass "desktop artifact no-ops by default"
fi
grep -Fq "RESULT OK" "$tmp/ledger/"*/t3-nightly-cycle.result && pass "result ok recorded" || fail "result ok recorded"
test -s "$tmp/ledger/"*/t3-nightly-cycle.jsonl && pass "json ledger written" || fail "json ledger written"

tmp="$(mktemp -d)"
export FAKE_HEAD_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
export FAKE_TARGET_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
run_cycle "$tmp"
unset FAKE_HEAD_SHA FAKE_TARGET_SHA
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "same sha exits zero" || fail "same sha exits zero"
if grep -Fq "pnpm" "$tmp/calls.log"; then
  fail "same sha skips build"
else
  pass "same sha skips build"
fi
grep -Fq "restart prebuilt=0" "$tmp/calls.log" && pass "same sha restart is non-prebuilt" || fail "same sha restart is non-prebuilt"

tmp="$(mktemp -d)"
export FAKE_BACKUP_RC=9
run_cycle "$tmp"
unset FAKE_BACKUP_RC
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "backup failure exits nonzero" || fail "backup failure exits nonzero"
if grep -Fq "sync" "$tmp/calls.log"; then
  fail "backup failure aborts before sync"
else
  pass "backup failure aborts before sync"
fi
grep -Fq "FAILURE step=backup" "$tmp/ledger/"*/t3-nightly-cycle.alert && pass "backup failure alerts" || fail "backup failure alerts"

tmp="$(mktemp -d)"
export FAKE_PNPM_RC=8
run_cycle "$tmp"
unset FAKE_PNPM_RC
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "build failure exits nonzero" || fail "build failure exits nonzero"
assert_order "$tmp/calls.log" "git -C $tmp/checkout merge --ff-only" "pnpm -C $tmp/checkout/apps/web run build" "git -C $tmp/checkout checkout aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
if grep -Fq "restart" "$tmp/calls.log"; then
  fail "build failure aborts before restart"
else
  pass "build failure aborts before restart"
fi

tmp="$(mktemp -d)"
T3DR_DESKTOP_ARTIFACT=1 run_cycle "$tmp"
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "desktop artifact enabled exits zero" || fail "desktop artifact enabled exits zero"
assert_order "$tmp/calls.log" "pnpm -C $tmp/checkout/apps/web run build" "pnpm -C $tmp/checkout run build:desktop" "pnpm -C $tmp/checkout run dist:desktop:artifact" "restart prebuilt=1"

echo "$pass_count passed, $fail_count failed"
[[ "$fail_count" -eq 0 ]]
