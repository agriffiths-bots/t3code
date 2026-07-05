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
if [[ "${FAKE_LOG_PATH:-0}" == "1" ]]; then
  echo "path=$PATH" >>"$T_LOG"
fi
echo "git $*" >>"$T_LOG"
case "$*" in
  *"rev-parse HEAD"*) echo "${FAKE_HEAD_SHA:-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}" ;;
  *"rev-parse origin/main"*) echo "${FAKE_TARGET_SHA:-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}" ;;
  *"diff --name-only"*) printf '%s\n' "${FAKE_CHANGED_FILES:-}" ;;
  *"merge-base --is-ancestor"*) exit "${FAKE_ANCESTOR_RC:-0}" ;;
  *"worktree prune"*) exit "${FAKE_GIT_WORKTREE_PRUNE_RC:-0}" ;;
  *"worktree add --detach"*)
    mkdir -p "${*: -2:1}"
    exit "${FAKE_GIT_WORKTREE_RC:-0}"
    ;;
  *"worktree remove --force"*)
    rm -rf -- "${*: -1}"
    exit "${FAKE_GIT_WORKTREE_REMOVE_RC:-0}"
    ;;
  *"merge --ff-only"*) exit "${FAKE_GIT_MERGE_RC:-0}" ;;
esac
exit "${FAKE_GIT_RC:-0}"
SH
  cat >"$dir/pnpm" <<'SH'
#!/usr/bin/env bash
echo "pnpm $*" >>"$T_LOG"
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
  if [[ "${args[$i]}" == "-C" && $((i + 1)) -lt ${#args[@]} ]]; then
    cwd="${args[$((i + 1))]}"
    case "$*" in
      *" run build")
        mkdir -p "$cwd/dist"
        printf 'web\n' >"$cwd/dist/index.html"
        ;;
      *" run build:desktop"|*" run dist:desktop:artifact")
        mkdir -p "$cwd/apps/server/dist/client"
        printf 'server-client\n' >"$cwd/apps/server/dist/client/index.html"
        printf 'server-bin\n' >"$cwd/apps/server/dist/bin.mjs"
        if [[ "$*" == *" run build:desktop" && "${FAKE_PREBUILT_ENV_AS_DIR_AFTER_BUILD:-0}" == "1" ]]; then
          mkdir -p "$T3DR_LEDGER/$(date -u +%F)/prebuilt-target.env"
        fi
        if [[ "$*" == *" run dist:desktop:artifact" && -n "${T3CODE_DESKTOP_OUTPUT_DIR:-}" ]]; then
          mkdir -p "$T3CODE_DESKTOP_OUTPUT_DIR"
          printf 'desktop\n' >"$T3CODE_DESKTOP_OUTPUT_DIR/artifact.txt"
        fi
        ;;
    esac
  fi
done
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
echo "restart prebuilt=${T3DR_PREBUILT_TARGET:-0} checkout=${T3DR_CHECKOUT:-} ledger=${T3DR_LEDGER:-} rollback=${T3DR_ROLLBACK_SHA:-} target=${T3DR_TARGET_SHA:-} assets=${T3DR_PREBUILT_ASSETS_DIR:-} probe=${T3DR_PINNED_HEALTH_PROBE:-}" >>"$T_LOG"
exit "${FAKE_RESTART_RC:-0}"
SH
  chmod +x "$dir"/*
}

run_cycle() {
  local tmp="$1"
  shift
  mkdir -p "$tmp/bin" "$tmp/checkout" "$tmp/ledger"
  mkdir -p "$tmp/checkout/scripts/ops/daily-restart"
  printf '#!/usr/bin/env bash\nexit 0\n' >"$tmp/checkout/scripts/ops/daily-restart/health-probe"
  chmod +x "$tmp/checkout/scripts/ops/daily-restart/health-probe"
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
stage="$tmp/ledger/$(date -u +%F)/prebuilt-stage/checkout"
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "happy path exits zero" || fail "happy path exits zero"
assert_order "$tmp/calls.log" "backup" "sync" "git -C" "git -C $tmp/checkout worktree prune" "git -C $tmp/checkout worktree add --detach $stage" "pnpm -C $stage install --frozen-lockfile --prefer-offline" "pnpm -C $stage/apps/web run build" "pnpm -C $stage run build:desktop" "restart prebuilt=1"
if grep -Fq "git -C $tmp/checkout merge --ff-only" "$tmp/calls.log" || grep -Fq "pnpm -C $tmp/checkout/apps/web run build" "$tmp/calls.log"; then
  fail "happy path leaves live checkout untouched before restart"
else
  pass "happy path leaves live checkout untouched before restart"
fi
grep -Fq "sync repo=$tmp/checkout" "$tmp/calls.log" && pass "sync receives checkout repo" || fail "sync receives checkout repo"
grep -Fq "checkout=$tmp/checkout ledger=$tmp/ledger" "$tmp/calls.log" && pass "restart receives checkout and ledger" || fail "restart receives checkout and ledger"
grep -Fq "rollback=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa target=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/calls.log" && pass "restart receives prebuilt rollback target" || fail "restart receives prebuilt rollback target"
grep -Fq "assets=$tmp/ledger/$(date -u +%F)/prebuilt-stage/payload" "$tmp/calls.log" && pass "restart receives staged payload" || fail "restart receives staged payload"
grep -Fq "probe=$tmp/ledger/$(date -u +%F)/pinned-tools/health-probe.rollback" "$tmp/calls.log" && pass "restart receives rollback probe" || fail "restart receives rollback probe"
grep -Fq "server-bin" "$tmp/ledger/$(date -u +%F)/prebuilt-stage/payload/apps/server/dist/bin.mjs" && pass "payload includes server executable" || fail "payload includes server executable"
if grep -Fq "dist:desktop:artifact" "$tmp/calls.log"; then
  fail "desktop artifact blocks by default"
else
  pass "desktop artifact no-ops by default"
fi
grep -Fq "RESULT OK" "$tmp/ledger/"*/t3-nightly-cycle.result && pass "result ok recorded" || fail "result ok recorded"
test -s "$tmp/ledger/"*/t3-nightly-cycle.jsonl && pass "json ledger written" || fail "json ledger written"

tmp="$(mktemp -d)"
caller_path="$tmp/nonstandard-node"
mkdir -p "$caller_path"
old_path="$PATH"
export FAKE_LOG_PATH=1
PATH="$caller_path:$old_path" run_cycle "$tmp"
unset FAKE_LOG_PATH
PATH="$old_path"
grep -Fq "$caller_path" "$tmp/calls.log" && pass "caller PATH is preserved after nightly trusted prefixes" || fail "caller PATH is preserved after nightly trusted prefixes"

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
grep -Fq "target=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/calls.log" && pass "same sha restart receives pinned target" || fail "same sha restart receives pinned target"

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
stage="$tmp/ledger/$(date -u +%F)/prebuilt-stage/checkout"
assert_order "$tmp/calls.log" "git -C $tmp/checkout worktree prune" "git -C $tmp/checkout worktree add --detach $stage" "pnpm -C $stage install --frozen-lockfile --prefer-offline"
if grep -Fq "git -C $tmp/checkout merge --ff-only" "$tmp/calls.log" || grep -Fq "git -C $tmp/checkout checkout aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "$tmp/calls.log"; then
  fail "prebuild failure mutated live checkout"
else
  pass "prebuild failure leaves live checkout untouched"
fi
if grep -Fq "restart" "$tmp/calls.log"; then
  fail "build failure aborts before restart"
else
  pass "build failure aborts before restart"
fi

tmp="$(mktemp -d)"
export FAKE_PREBUILT_ENV_AS_DIR_AFTER_BUILD=1
run_cycle "$tmp"
unset FAKE_PREBUILT_ENV_AS_DIR_AFTER_BUILD
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "metadata write failure exits nonzero" || fail "metadata write failure exits nonzero"
grep -Fq "failed to write restart metadata" "$tmp/ledger/"*/build-release-artifacts.log && pass "metadata write failure logged" || fail "metadata write failure logged"
if grep -Fq "restart" "$tmp/calls.log"; then
  fail "metadata write failure aborts before restart"
else
  pass "metadata write failure aborts before restart"
fi

tmp="$(mktemp -d)"
export FAKE_CHANGED_FILES=$'patches/foo.patch\napps/web/src/App.tsx'
run_cycle "$tmp"
unset FAKE_CHANGED_FILES
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "dependency install input change exits nonzero" || fail "dependency install input change exits nonzero"
grep -Fq "dependency manifests changed" "$tmp/ledger/"*/build-release-artifacts.log && pass "dependency install input change logged" || fail "dependency install input change logged"
if grep -Fq "worktree add" "$tmp/calls.log" || grep -Fq "restart" "$tmp/calls.log"; then
  fail "dependency install input change staged or restarted"
else
  pass "dependency install input change aborts before staging"
fi

tmp="$(mktemp -d)"
export FAKE_ANCESTOR_RC=1
run_cycle "$tmp"
unset FAKE_ANCESTOR_RC
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "non-fast-forward target exits nonzero" || fail "non-fast-forward target exits nonzero"
grep -Fq "target is not a fast-forward" "$tmp/ledger/"*/build-release-artifacts.log && pass "non-fast-forward target logged" || fail "non-fast-forward target logged"
if grep -Fq "worktree add" "$tmp/calls.log" || grep -Fq "restart" "$tmp/calls.log"; then
  fail "non-fast-forward target staged or restarted"
else
  pass "non-fast-forward target aborts before staging"
fi

tmp="$(mktemp -d)"
T3DR_DESKTOP_ARTIFACT=1 run_cycle "$tmp"
stage="$tmp/ledger/$(date -u +%F)/prebuilt-stage/checkout"
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "desktop artifact enabled exits zero" || fail "desktop artifact enabled exits zero"
assert_order "$tmp/calls.log" "pnpm -C $stage/apps/web run build" "pnpm -C $stage run build:desktop" "pnpm -C $stage run dist:desktop:artifact" "restart prebuilt=1"
grep -Fq "desktop" "$tmp/ledger/$(date -u +%F)/desktop-artifact/artifact.txt" && pass "desktop artifact persists outside stage checkout" || fail "desktop artifact persists outside stage checkout"

echo "$pass_count passed, $fail_count failed"
[[ "$fail_count" -eq 0 ]]
