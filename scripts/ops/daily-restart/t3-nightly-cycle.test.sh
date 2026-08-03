#!/usr/bin/env bash
# Assertion chains are safe here because pass and fail always return success.
# shellcheck disable=SC2015
set -u

ROOT="$(git rev-parse --show-toplevel)"
SCRIPT="$ROOT/scripts/ops/daily-restart/t3-nightly-cycle"
REAL_NODE="$(command -v node)"

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
if [[ "${1:-}" == "-C" && "${3:-}" == "checkout" ]]; then
  echo "${4:-}" >"$T_TMP/fake-head"
  exit "${FAKE_GIT_CHECKOUT_RC:-0}"
fi
case "$*" in
  *"rev-parse HEAD"*)
    if [[ -f "$T_TMP/fake-head" ]]; then
      cat "$T_TMP/fake-head"
    else
      echo "${FAKE_HEAD_SHA:-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}"
    fi
    ;;
  *"rev-parse origin/main"*) echo "${FAKE_TARGET_SHA:-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}" ;;
  *"rev-parse --verify"*)
    value="${*: -1}"
    echo "${value%%^*}"
    ;;
  *"diff --name-only"*) printf '%s\n' "${FAKE_CHANGED_FILES:-}" ;;
  *"status --porcelain"*) printf '%s\n' "${FAKE_GIT_STATUS:-}" ;;
  *"merge-base --is-ancestor"*)
    ancestor_from="${*: -2:1}"
    if [[ -n "${FAKE_ANCESTOR_FROM_RC:-}" && "$ancestor_from" == "${FAKE_ANCESTOR_FROM_SHA:-}" ]]; then
      exit "$FAKE_ANCESTOR_FROM_RC"
    fi
    exit "${FAKE_ANCESTOR_RC:-0}"
    ;;
  *"reflog show --format=%H --max-count=64 HEAD"*)
    if [[ -n "${FAKE_REFLOG_SHAS:-}" ]]; then
      printf '%s\n' "$FAKE_REFLOG_SHAS"
    else
      printf '%s\n' bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    fi
    ;;
  *"worktree prune"*) exit "${FAKE_GIT_WORKTREE_PRUNE_RC:-0}" ;;
  *"worktree add --detach"*)
    mkdir -p "${*: -2:1}"
    exit "${FAKE_GIT_WORKTREE_RC:-0}"
    ;;
  *"worktree remove --force"*)
    rm -rf -- "${*: -1}"
    exit "${FAKE_GIT_WORKTREE_REMOVE_RC:-0}"
    ;;
  *"merge --ff-only"*)
    if [[ "${FAKE_GIT_MERGE_RC:-0}" == "0" && "${FAKE_MERGE_LEAVES_HEAD:-0}" != "1" ]]; then
      echo "${*: -1}" >"$T_TMP/fake-head"
    fi
    if [[ "${FAKE_TARGET_REPLACES_RESTART:-0}" == "1" ]]; then
      cat >"$T_TMP/checkout/scripts/ops/daily-restart/t3-daily-restart" <<'TARGET'
#!/usr/bin/env bash
echo "target restart source=$0" >>"$T_LOG"
exit 97
TARGET
      chmod +x "$T_TMP/checkout/scripts/ops/daily-restart/t3-daily-restart"
    fi
    if [[ "${FAKE_LIVE_METADATA_AS_DIR:-0}" == "1" ]]; then
      mkdir -p "$T3DR_LEDGER/$(date -u +%F)/prebuilt-target.env"
    fi
    exit "${FAKE_GIT_MERGE_RC:-0}"
    ;;
esac
exit "${FAKE_GIT_RC:-0}"
SH
  cat >"$dir/mv" <<'SH'
#!/usr/bin/env bash
echo "mv $*" >>"$T_LOG"
if [[ "${FAKE_MV_NO_EXCHANGE:-0}" == "1" ]]; then
  for arg in "$@"; do
    if [[ "$arg" == "--exchange" ]]; then
      exit 1
    fi
  done
fi
exec /usr/bin/mv "$@"
SH
  cat >"$dir/curl" <<'SH'
#!/usr/bin/env bash
echo "curl $*" >>"$T_LOG"
printf '{"serverBuildSha":"%s"}\n' "${FAKE_DEPLOYED_SHA:-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}"
SH
  cat >"$dir/node" <<'SH'
#!/usr/bin/env bash
echo "node $*" >>"$T_LOG"
case "$*" in
  *"auth session issue"*)
    echo "mint-live-resume-token entry=${2:-missing}" >>"$T_LOG"
    printf '{"token":"%s","sessionId":"%s"}\n' "${FAKE_LIVE_RESUME_TOKEN:-minted-live-token}" "${FAKE_LIVE_RESUME_SESSION_ID:-minted-live-session}"
    exit "${FAKE_LIVE_RESUME_MINT_RC:-0}"
    ;;
  *"auth session revoke"*)
    echo "revoke-live-resume-token ${*: -1}" >>"$T_LOG"
    exit "${FAKE_LIVE_RESUME_REVOKE_RC:-0}"
    ;;
esac
exec "$REAL_NODE" "$@"
SH
  cat >"$dir/pnpm" <<'SH'
#!/usr/bin/env bash
[[ -n "${T3CODE_BUILD_SHA:-}" ]] && echo "pnpm-env T3CODE_BUILD_SHA=$T3CODE_BUILD_SHA cmd=$*" >>"$T_LOG"
[[ -n "${T3CODE_HOSTED_BUILD:-}" ]] && echo "pnpm-env T3CODE_HOSTED_BUILD=$T3CODE_HOSTED_BUILD cmd=$*" >>"$T_LOG"
echo "pnpm-backend-env HTTP=${VITE_HTTP_URL-unset} WS=${VITE_WS_URL-unset} DEV=${VITE_DEV_SERVER_URL-unset} cmd=$*" >>"$T_LOG"
echo "pnpm $*" >>"$T_LOG"
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
  if [[ "${args[$i]}" == "-C" && $((i + 1)) -lt ${#args[@]} ]]; then
    cwd="${args[$((i + 1))]}"
    case "$*" in
      *" run build:hosted")
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
if [[ "$*" == *" install "* ]]; then
  if [[ "$*" == *"-C $T_TMP/checkout install "* && -n "${FAKE_LIVE_INSTALL_RC:-}" &&
    "$(cat "$T_TMP/fake-head" 2>/dev/null)" == "${FAKE_TARGET_SHA:-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}" ]]; then
    exit "$FAKE_LIVE_INSTALL_RC"
  fi
  exit "${FAKE_PNPM_INSTALL_RC:-${FAKE_PNPM_RC:-0}}"
fi
if [[ "$*" == *" run build:hosted" ]]; then
  exit "${FAKE_PNPM_BUILD_RC:-${FAKE_PNPM_RC:-0}}"
fi
exit "${FAKE_PNPM_RC:-0}"
SH
  cat >"$dir/fake-backup" <<'SH'
#!/usr/bin/env bash
echo "backup" >>"$T_LOG"
if [[ "${FAKE_BACKUP_SIGNAL:-}" =~ ^(TERM|INT|HUP)$ ]]; then
  parent="$PPID"
  for _ in 1 2 3 4; do
    args="$(/usr/bin/ps -o args= -p "$parent" 2>/dev/null)"
    if [[ "$args" == *"t3-nightly-cycle"* ]]; then
      kill -s "$FAKE_BACKUP_SIGNAL" "$parent"
      break
    fi
    parent="$(/usr/bin/ps -o ppid= -p "$parent" 2>/dev/null | tr -d '[:space:]')"
    [[ "$parent" =~ ^[0-9]+$ ]] || break
  done
fi
exit "${FAKE_BACKUP_RC:-0}"
SH
  cat >"$dir/fake-sync" <<'SH'
#!/usr/bin/env bash
echo "sync repo=${T3_REPO_DIR:-}" >>"$T_LOG"
exit "${FAKE_SYNC_RC:-0}"
SH
  cat >"$dir/fake-restart" <<'SH'
#!/usr/bin/env bash
echo "restart prebuilt=${T3DR_PREBUILT_TARGET:-0} checkout=${T3DR_CHECKOUT:-} ledger=${T3DR_LEDGER:-} db=${T3DR_DB:-} service=${T3DR_SERVICE:-} origin=${T3DR_ORIGIN:-} snapshot=${T3DR_SNAPSHOT_DIR:-} static_dir=${T3DR_STATIC_DIR:-} probe_timeout=${T3DR_PROBE_TIMEOUT:-} smoke_instance=${T3DR_SMOKE_INSTANCE:-} smoke_model=${T3DR_SMOKE_MODEL:-} rollback=${T3DR_ROLLBACK_SHA:-} target=${T3DR_TARGET_SHA:-} assets=${T3DR_PREBUILT_ASSETS_DIR:-} probe=${T3DR_PINNED_HEALTH_PROBE:-} token=${T3DR_TOKEN:+set} token_session=${T3DR_TOKEN_SESSION_ID:-}" >>"$T_LOG"
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
  T_TMP="$tmp" T_LOG="$tmp/calls.log" REAL_NODE="$REAL_NODE" T3DR_TEST_PATH_PREFIX="$tmp/bin" \
    T3DR_CHECKOUT="$tmp/checkout" \
    T3DR_LEDGER="$tmp/ledger" \
    T3DR_DB="$tmp/state.sqlite" \
    T3DR_SERVICE="fake.service" \
    T3DR_ORIGIN="http://127.0.0.1:1" \
    T3DR_SNAPSHOT_DIR="$tmp/snaps" \
    T3DR_STATIC_DIR="${T3DR_STATIC_DIR:-}" \
    T3DR_PROBE_TIMEOUT="7" \
    T3DR_SMOKE_INSTANCE="fakeAgent" \
    T3DR_SMOKE_MODEL="fake-model" \
    T3DR_BACKUP_CMD="fake-backup" \
    T3DR_UPSTREAM_SYNC_CMD="fake-sync" \
    T3DR_RESTART_CMD="${FAKE_RESTART_CMD:-$tmp/bin/fake-restart}" \
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
export VITE_HTTP_URL=//localhost:3773 VITE_WS_URL=//127.0.0.2:3773 VITE_DEV_SERVER_URL=http://localhost:5733
run_cycle "$tmp"
unset VITE_HTTP_URL VITE_WS_URL VITE_DEV_SERVER_URL
stage="$tmp/ledger/$(date -u +%F)/prebuilt-stage/checkout"
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "happy path exits zero" || fail "happy path exits zero"
assert_order "$tmp/calls.log" "backup" "sync" "git -C" "git -C $tmp/checkout worktree prune" "git -C $tmp/checkout worktree add --detach $stage" "pnpm -C $stage install --frozen-lockfile --prefer-offline" "pnpm -C $stage/apps/web run build:hosted" "pnpm -C $stage run build:desktop" "assert-web-no-dev-endpoints.ts --force $stage/apps/web/dist" "restart prebuilt=1"
grep -Fq "pnpm-env T3CODE_BUILD_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb cmd=-C $stage/apps/web run build:hosted" "$tmp/calls.log" \
  && pass "happy path stamps standalone web build" \
  || fail "happy path stamps standalone web build"
grep -Fq "pnpm-backend-env HTTP=unset WS=unset DEV=unset cmd=-C $stage/apps/web run build:hosted" "$tmp/calls.log" && pass "staged hosted build scrubs inherited backend env" || fail "staged hosted build scrubs inherited backend env"
grep -Fq "pnpm-env T3CODE_BUILD_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb cmd=-C $stage run build:desktop" "$tmp/calls.log" \
  && pass "happy path stamps desktop/server build" \
  || fail "happy path stamps desktop/server build"
grep -Fq "pnpm-env T3CODE_HOSTED_BUILD=1 cmd=-C $stage run build:desktop" "$tmp/calls.log" && pass "desktop/server build preserves hosted mode" || fail "desktop/server build preserves hosted mode"
grep -Fq "pnpm-backend-env HTTP=unset WS=unset DEV=unset cmd=-C $stage run build:desktop" "$tmp/calls.log" && pass "desktop/server build scrubs inherited backend env" || fail "desktop/server build scrubs inherited backend env"
if grep -Fq "git -C $tmp/checkout merge --ff-only" "$tmp/calls.log" || grep -Fq "pnpm -C $tmp/checkout/apps/web run build:hosted" "$tmp/calls.log"; then
  fail "happy path leaves live checkout untouched before restart"
else
  pass "happy path leaves live checkout untouched before restart"
fi
grep -Fq "sync repo=$tmp/checkout" "$tmp/calls.log" && pass "sync receives checkout repo" || fail "sync receives checkout repo"
grep -Fq "checkout=$tmp/checkout ledger=$tmp/ledger" "$tmp/calls.log" && pass "restart receives checkout and ledger" || fail "restart receives checkout and ledger"
grep -Fq "db=$tmp/state.sqlite service=fake.service origin=http://127.0.0.1:1 snapshot=$tmp/snaps static_dir= probe_timeout=7 smoke_instance=fakeAgent smoke_model=fake-model" "$tmp/calls.log" && pass "restart receives explicit runtime env" || fail "restart receives explicit runtime env"
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
mkdir -p "$tmp/releases/web/current"
printf 'release web\n' >"$tmp/releases/web/current/index.html"
T3DR_STATIC_DIR="$tmp/releases/web/current" run_cycle "$tmp"
grep -Fq "static_dir=$tmp/releases/web/current" "$tmp/calls.log" \
  && pass "nightly cycle forwards configured static release independently of checkout" \
  || fail "nightly cycle forwards configured static release independently of checkout"

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
grep -Fq "RESULT FAILED step=backup rc=9" "$tmp/ledger/"*/t3-nightly-cycle.result && pass "backup failure records non-ok summary" || fail "backup failure records non-ok summary"
grep -Fq "END FAILED" "$tmp/ledger/"*/t3-nightly-cycle.log && pass "backup failure records failed cycle end" || fail "backup failure records failed cycle end"

tmp="$(mktemp -d)"
export FAKE_BACKUP_SIGNAL=TERM
run_cycle "$tmp"
unset FAKE_BACKUP_SIGNAL
[[ "$(cat "$tmp/rc")" == "143" ]] && pass "SIGTERM exits with 143" || fail "SIGTERM exits with 143"
grep -Fq "FAILURE step=signal-TERM rc=143" "$tmp/ledger/"*/t3-nightly-cycle.alert \
  && grep -Fq "RESULT FAILED step=signal-TERM rc=143" "$tmp/ledger/"*/t3-nightly-cycle.result \
  && pass "SIGTERM records an alert and failed result" \
  || fail "SIGTERM records an alert and failed result"

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
if grep -Fq "restart prebuilt=" "$tmp/calls.log"; then
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
if grep -Fq "restart prebuilt=" "$tmp/calls.log"; then
  fail "metadata write failure aborts before restart"
else
  pass "metadata write failure aborts before restart"
fi

tmp="$(mktemp -d)"
export FAKE_CHANGED_FILES=$'patches/foo.patch\napps/web/src/App.tsx'
export FAKE_MV_NO_EXCHANGE=1
export VITE_HTTP_URL=//localhost:3773 VITE_WS_URL=//127.0.0.2:3773 VITE_DEV_SERVER_URL=http://localhost:5733
mkdir -p "$tmp/checkout/apps/web/dist"
printf 'old-web\n' >"$tmp/checkout/apps/web/dist/index.html"
run_cycle "$tmp"
unset FAKE_CHANGED_FILES FAKE_MV_NO_EXCHANGE VITE_HTTP_URL VITE_WS_URL VITE_DEV_SERVER_URL
stage="$tmp/ledger/$(date -u +%F)/prebuilt-stage/checkout"
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "dependency install input change falls back successfully" || fail "dependency install input change falls back successfully"
grep -Fq "dependency manifests changed" "$tmp/ledger/"*/build-release-artifacts.log && pass "dependency install input change logged" || fail "dependency install input change logged"
assert_order "$tmp/calls.log" \
  "git -C $tmp/checkout diff --name-only" \
  "git -C $tmp/checkout worktree add --detach $stage" \
  "pnpm -C $stage install --frozen-lockfile --prefer-offline" \
  "pnpm -C $stage/apps/web run build:hosted" \
  "assert-web-no-dev-endpoints.ts --force $stage/apps/web/dist" \
  "mint-live-resume-token" \
  "git -C $tmp/checkout merge --ff-only bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" \
  "pnpm -C $tmp/checkout install --frozen-lockfile --prefer-offline" \
  "restart prebuilt=0"
grep -Fq "restart prebuilt=0" "$tmp/calls.log" \
  && grep -Fq "rollback=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa target=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/calls.log" \
  && pass "live fallback forces target rebuild from the pre-cycle rollback sha" \
  || fail "live fallback forces target rebuild from the pre-cycle rollback sha"
grep -Fq "token=set token_session=minted-live-session" "$tmp/calls.log" \
  && pass "live fallback hands the pre-mutation token to the restart manager" \
  || fail "live fallback hands the pre-mutation token to the restart manager"
grep -Fq "pnpm-env T3CODE_BUILD_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb cmd=-C $stage/apps/web run build:hosted" "$tmp/calls.log" \
  && pass "live fallback stamps web build" \
  || fail "live fallback stamps web build"
grep -Fq "pnpm-backend-env HTTP=unset WS=unset DEV=unset cmd=-C $stage/apps/web run build:hosted" "$tmp/calls.log" && pass "live fallback hosted build scrubs inherited backend env" || fail "live fallback hosted build scrubs inherited backend env"
if grep -Fq "pnpm -C $tmp/checkout run build:desktop" "$tmp/calls.log"; then
  fail "live fallback overwrites server artifact before guarded restart"
else
  pass "live fallback leaves server artifact to the guarded restart"
fi
grep -Fq "old-web" "$tmp/checkout/apps/web/dist/index.html" \
  && pass "live fallback leaves active web publication unchanged until the guarded restart" \
  || fail "live fallback leaves active web publication unchanged until the guarded restart"
[[ "$(cat "$tmp/rc")" == "0" ]] \
  && ! grep -Fq -- "--exchange" "$tmp/calls.log" \
  && pass "live fallback succeeds without nonportable mv exchange" \
  || fail "live fallback succeeds without nonportable mv exchange"
grep -Fq '"step":"build-release-artifacts","status":"fallback","rc":66' "$tmp/ledger/"*/t3-nightly-cycle.jsonl \
  && pass "rc=66 fallback is machine readable" \
  || fail "rc=66 fallback is machine readable"
[[ ! -e "$tmp/ledger/$(date -u +%F)/t3-nightly-cycle.alert" ]] \
  && pass "recovered rc=66 does not leave a failure alert" \
  || fail "recovered rc=66 does not leave a failure alert"
grep -Fq "RESULT OK" "$tmp/ledger/"*/t3-nightly-cycle.result && pass "live fallback cycle proceeds to ok" || fail "live fallback cycle proceeds to ok"
[[ ! -e "$tmp/ledger/t3-nightly-cycle.pending-live-deploy" ]] \
  && pass "successful live fallback clears pending marker" \
  || fail "successful live fallback clears pending marker"
grep -Fq "T3DR_TARGET_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/ledger/"*/live-deploy.completed \
  && pass "successful live fallback archives completion" \
  || fail "successful live fallback archives completion"
grep -Fq "T3DR_ROLLBACK_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "$tmp/ledger/"*/live-deploy.completed \
  && pass "successful live fallback archives rollback base" \
  || fail "successful live fallback archives rollback base"

tmp="$(mktemp -d)"
mkdir -p "$tmp/ledger"
printf 'T3DR_TARGET_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n' >"$tmp/ledger/t3-nightly-cycle.pending-live-deploy"
run_cycle "$tmp"
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "pre-mutation legacy pending marker migrates" || fail "pre-mutation legacy pending marker migrates"
grep -Fq "migrating legacy pending live deploy rollback_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "$tmp/ledger/"*/build-release-artifacts.log \
  && grep -Fq "T3DR_ROLLBACK_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "$tmp/ledger/"*/live-deploy.completed \
  && pass "legacy marker migration uses the safe current checkout" \
  || fail "legacy marker migration uses the safe current checkout"

tmp="$(mktemp -d)"
mkdir -p "$tmp/ledger/2026-07-16"
printf '2026-07-16T03:30:00Z RESULT OK pre_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa target_sha=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb snapshot=x manifest=y\n' \
  >"$tmp/ledger/2026-07-16/t3-daily-restart.result"
printf 'T3DR_TARGET_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n' >"$tmp/ledger/t3-nightly-cycle.pending-live-deploy"
printf 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n' >"$tmp/fake-head"
export FAKE_HEAD_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
export FAKE_TARGET_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
export FAKE_DEPLOYED_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
run_cycle "$tmp"
unset FAKE_HEAD_SHA FAKE_TARGET_SHA FAKE_DEPLOYED_SHA
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "successful legacy pending marker reconciles" || fail "successful legacy pending marker reconciles"
grep -Fq "reconciled pending live deploy already completed target_sha=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb format=legacy" "$tmp/ledger/"*/build-release-artifacts.log \
  && grep -Fq "T3DR_TARGET_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/ledger/"*/live-deploy.completed \
  && [[ ! -e "$tmp/ledger/t3-nightly-cycle.pending-live-deploy" ]] \
  && pass "successful legacy marker is archived without retry" \
  || fail "successful legacy marker is archived without retry"

tmp="$(mktemp -d)"
mkdir -p "$tmp/ledger/2026-07-16" "$tmp/checkout/scripts/ops/daily-restart"
printf '2026-07-16T03:30:00Z RESULT OK pre_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa target_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa snapshot=x manifest=y\n' >"$tmp/ledger/2026-07-16/t3-daily-restart.result"
printf 'T3DR_TARGET_SHA=cccccccccccccccccccccccccccccccccccccccc\n' >"$tmp/ledger/t3-nightly-cycle.pending-live-deploy"
printf 'cccccccccccccccccccccccccccccccccccccccc\n' >"$tmp/fake-head"
cat >"$tmp/checkout/scripts/ops/daily-restart/t3-daily-restart" <<'SH'
#!/usr/bin/env bash
# capability: live-rollback-v1
echo "legacy-compatible restart source=$0 rollback=${T3DR_ROLLBACK_SHA:-} target=${T3DR_TARGET_SHA:-}" >>"$T_LOG"
exit 0
SH
chmod +x "$tmp/checkout/scripts/ops/daily-restart/t3-daily-restart"
export FAKE_RESTART_CMD="$tmp/checkout/scripts/ops/daily-restart/t3-daily-restart"
export FAKE_TARGET_SHA=cccccccccccccccccccccccccccccccccccccccc
export FAKE_REFLOG_SHAS=$'cccccccccccccccccccccccccccccccccccccccc\nbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
run_cycle "$tmp"
unset FAKE_RESTART_CMD FAKE_TARGET_SHA FAKE_REFLOG_SHAS
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "post-mutation legacy pending marker migrates" || fail "post-mutation legacy pending marker migrates"
pinned_restart="$tmp/ledger/t3-nightly-cycle.restart-managers/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/t3-daily-restart"
grep -Fq "git -C $tmp/checkout reflog show --format=%H --max-count=64 HEAD" "$tmp/calls.log" \
  && grep -Fq "legacy-compatible restart source=$pinned_restart rollback=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa target=cccccccccccccccccccccccccccccccccccccccc" "$tmp/calls.log" \
  && grep -Fq "cccccccccccccccccccccccccccccccccccccccc" "$tmp/ledger/t3-nightly-cycle.restart-managers/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/.manager-source-sha" \
  && grep -Fq "rollback source=last-successful-restart sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "$tmp/ledger/"*/build-release-artifacts.log \
  && pass "legacy marker migration uses the last successful SHA instead of an intermediate failed retry" \
  || fail "legacy marker migration uses the last successful SHA instead of an intermediate failed retry"

tmp="$(mktemp -d)"
mkdir -p "$tmp/checkout/scripts/ops/daily-restart"
cat >"$tmp/checkout/scripts/ops/daily-restart/t3-daily-restart" <<'SH'
#!/usr/bin/env bash
# capability: live-rollback-v1
echo "restart source=$0 rollback=${T3DR_ROLLBACK_SHA:-} target=${T3DR_TARGET_SHA:-}" >>"$T_LOG"
exit 0
SH
chmod +x "$tmp/checkout/scripts/ops/daily-restart/t3-daily-restart"
export FAKE_CHANGED_FILES="pnpm-lock.yaml"
export FAKE_RESTART_CMD="$tmp/checkout/scripts/ops/daily-restart/t3-daily-restart"
export FAKE_TARGET_REPLACES_RESTART=1
run_cycle "$tmp"
unset FAKE_CHANGED_FILES FAKE_RESTART_CMD FAKE_TARGET_REPLACES_RESTART
pinned_restart="$tmp/ledger/t3-nightly-cycle.restart-managers/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/t3-daily-restart"
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "checkout-hosted restart orchestrator remains stable across live update" || fail "checkout-hosted restart orchestrator remains stable across live update"
grep -Fq "restart source=$pinned_restart rollback=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa target=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/calls.log" \
  && pass "live fallback invokes the pinned pre-update restart orchestrator" \
  || fail "live fallback invokes the pinned pre-update restart orchestrator"
if grep -Fq "target restart source=" "$tmp/calls.log"; then
  fail "live fallback invoked target restart code"
else
  pass "live fallback never invokes target restart code"
fi

tmp="$(mktemp -d)"
mkdir -p "$tmp/checkout/scripts/ops/daily-restart"
cat >"$tmp/checkout/scripts/ops/daily-restart/t3-daily-restart" <<'SH'
#!/usr/bin/env bash
# capability: live-rollback-v1
echo "rollback restart source=$0 rollback=${T3DR_ROLLBACK_SHA:-} target=${T3DR_TARGET_SHA:-}" >>"$T_LOG"
exit 0
SH
chmod +x "$tmp/checkout/scripts/ops/daily-restart/t3-daily-restart"
export FAKE_CHANGED_FILES="pnpm-lock.yaml"
export FAKE_RESTART_CMD="$tmp/checkout/scripts/ops/daily-restart/t3-daily-restart"
export FAKE_TARGET_REPLACES_RESTART=1
export FAKE_LIVE_INSTALL_RC=8
run_cycle "$tmp"
unset FAKE_LIVE_INSTALL_RC
pinned_restart="$tmp/ledger/t3-nightly-cycle.restart-managers/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/t3-daily-restart"
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "failed first live attempt exits nonzero before restart" || fail "failed first live attempt exits nonzero before restart"
[[ -x "$pinned_restart" ]] && pass "failed live attempt retains rollback-bound restart manager" || fail "failed live attempt retains rollback-bound restart manager"
run_cycle "$tmp"
unset FAKE_CHANGED_FILES FAKE_RESTART_CMD FAKE_TARGET_REPLACES_RESTART
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "pending live retry exits zero after failed handoff restore" || fail "pending live retry exits zero after failed handoff restore"
grep -Fq "rollback restart source=$pinned_restart rollback=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa target=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/calls.log" \
  && pass "pending live retry reuses rollback-bound restart manager" \
  || fail "pending live retry reuses rollback-bound restart manager"
if grep -Fq "target restart source=" "$tmp/calls.log"; then
  fail "pending live retry invoked target restart code"
else
  pass "pending live retry never invokes target restart code"
fi

tmp="$(mktemp -d)"
mkdir -p "$tmp/ledger"
cat >"$tmp/ledger/t3-nightly-cycle.pending-live-deploy" <<'EOF'
T3DR_ROLLBACK_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
T3DR_TARGET_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
T3DR_RESTART_MANAGER_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
EOF
export FAKE_TARGET_SHA=cccccccccccccccccccccccccccccccccccccccc
run_cycle "$tmp"
unset FAKE_TARGET_SHA
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "advanced origin pending retry exits zero" || fail "advanced origin pending retry exits zero"
grep -Fq "git -C $tmp/checkout merge --ff-only bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/calls.log" \
  && grep -Fq "restart prebuilt=0" "$tmp/calls.log" \
  && grep -Fq "target=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/calls.log" \
  && pass "pending retry deploys the stored target" \
  || fail "pending retry deploys the stored target"
if grep -Fq "git -C $tmp/checkout merge --ff-only cccccccccccccccccccccccccccccccccccccccc" "$tmp/calls.log" ||
  grep -Fq "target=cccccccccccccccccccccccccccccccccccccccc" "$tmp/calls.log"; then
  fail "pending retry silently deployed advanced origin"
else
  pass "pending retry does not silently deploy advanced origin"
fi

tmp="$(mktemp -d)"
mkdir -p "$tmp/ledger"
cat >"$tmp/ledger/t3-nightly-cycle.pending-live-deploy" <<'EOF'
T3DR_ROLLBACK_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
T3DR_TARGET_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
T3DR_RESTART_MANAGER_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
EOF
export FAKE_TARGET_SHA=cccccccccccccccccccccccccccccccccccccccc
export FAKE_ANCESTOR_FROM_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
export FAKE_ANCESTOR_FROM_RC=1
run_cycle "$tmp"
unset FAKE_TARGET_SHA FAKE_ANCESTOR_FROM_SHA FAKE_ANCESTOR_FROM_RC
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "unreachable stored target exits nonzero" || fail "unreachable stored target exits nonzero"
grep -Fq "stored pending target is no longer reachable from origin/main" "$tmp/ledger/"*/build-release-artifacts.log \
  && grep -Fq "FAILURE step=build-release-artifacts rc=70" "$tmp/ledger/"*/t3-nightly-cycle.alert \
  && pass "unreachable stored target fails closed with rc 70" \
  || fail "unreachable stored target fails closed with rc 70"
[[ -e "$tmp/ledger/t3-nightly-cycle.pending-live-deploy" ]] \
  && pass "unreachable stored target retains its pending marker" \
  || fail "unreachable stored target retains its pending marker"
if grep -Fq "merge --ff-only" "$tmp/calls.log" || grep -Fq "restart prebuilt=" "$tmp/calls.log"; then
  fail "unreachable stored target mutated or restarted"
else
  pass "unreachable stored target aborts before mutation"
fi

tmp="$(mktemp -d)"
mkdir -p "$tmp/checkout/apps/web/dist"
printf 'old-web\n' >"$tmp/checkout/apps/web/dist/index.html"
export FAKE_CHANGED_FILES="pnpm-lock.yaml"
export FAKE_LIVE_METADATA_AS_DIR=1
run_cycle "$tmp"
unset FAKE_CHANGED_FILES FAKE_LIVE_METADATA_AS_DIR
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "live metadata write failure exits nonzero" || fail "live metadata write failure exits nonzero"
grep -Fq "old-web" "$tmp/checkout/apps/web/dist/index.html" \
  && pass "live metadata write failure preserves the active web bundle" \
  || fail "live metadata write failure preserves the active web bundle"
[[ "$(cat "$tmp/fake-head")" == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ]] \
  && pass "live metadata write failure restores rollback source and dependencies" \
  || fail "live metadata write failure restores rollback source and dependencies"
if grep -Fq "restart prebuilt=" "$tmp/calls.log"; then
  fail "live metadata write failure restarted"
else
  pass "live metadata write failure aborts before restart"
fi

tmp="$(mktemp -d)"
export FAKE_CHANGED_FILES="pnpm-lock.yaml"
T3DR_DESKTOP_ARTIFACT=1 run_cycle "$tmp"
unset FAKE_CHANGED_FILES
stage="$tmp/ledger/$(date -u +%F)/prebuilt-stage/checkout"
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "artifact-enabled live fallback exits zero" || fail "artifact-enabled live fallback exits zero"
assert_order "$tmp/calls.log" \
  "pnpm -C $stage/apps/web run build:hosted" \
  "pnpm -C $stage run dist:desktop:artifact" \
  "pnpm -C $tmp/checkout install --frozen-lockfile --prefer-offline" \
  "restart prebuilt=0"
grep -Fq "desktop" "$tmp/ledger/$(date -u +%F)/desktop-artifact/artifact.txt" \
  && pass "live fallback persists desktop artifact" \
  || fail "live fallback persists desktop artifact"

tmp="$(mktemp -d)"
mkdir -p "$tmp/checkout/apps/web/dist"
printf 'old-web\n' >"$tmp/checkout/apps/web/dist/index.html"
export FAKE_CHANGED_FILES="pnpm-lock.yaml"
export FAKE_PNPM_BUILD_RC=8
run_cycle "$tmp"
unset FAKE_CHANGED_FILES FAKE_PNPM_BUILD_RC
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "isolated live web build failure exits nonzero" || fail "isolated live web build failure exits nonzero"
grep -Fq "old-web" "$tmp/checkout/apps/web/dist/index.html" \
  && pass "failed isolated build preserves active web dist" \
  || fail "failed isolated build preserves active web dist"
if grep -Fq "git -C $tmp/checkout merge --ff-only" "$tmp/calls.log" || grep -Fq "restart prebuilt=" "$tmp/calls.log"; then
  fail "failed isolated build mutates checkout or restarts"
else
  pass "failed isolated build aborts before live mutation"
fi

tmp="$(mktemp -d)"
export FAKE_CHANGED_FILES="pnpm-lock.yaml"
export FAKE_GIT_STATUS=" M apps/web/src/App.tsx"
run_cycle "$tmp"
unset FAKE_CHANGED_FILES FAKE_GIT_STATUS
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "dirty live fallback exits nonzero" || fail "dirty live fallback exits nonzero"
grep -Fq "FAILURE step=live-deploy rc=69" "$tmp/ledger/"*/t3-nightly-cycle.alert && pass "dirty live fallback alerts" || fail "dirty live fallback alerts"
test ! -e "$tmp/ledger/t3-nightly-cycle.pending-live-deploy" && pass "dirty live fallback does not persist pending state before validation" || fail "dirty live fallback does not persist pending state before validation"
if grep -Fq "merge --ff-only" "$tmp/calls.log" || grep -Fq "restart prebuilt=" "$tmp/calls.log"; then
  fail "dirty live fallback mutates or restarts"
else
  pass "dirty live fallback aborts before mutation"
fi

tmp="$(mktemp -d)"
export FAKE_CHANGED_FILES="pnpm-lock.yaml"
export FAKE_MERGE_LEAVES_HEAD=1
run_cycle "$tmp"
unset FAKE_CHANGED_FILES FAKE_MERGE_LEAVES_HEAD
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "wrong post-merge head exits nonzero" || fail "wrong post-merge head exits nonzero"
grep -Fq "did not reach pinned target" "$tmp/ledger/"*/live-deploy.log && pass "wrong post-merge head is logged" || fail "wrong post-merge head is logged"
if grep -Fq "restart prebuilt=" "$tmp/calls.log"; then
  fail "wrong post-merge head restarts"
else
  pass "wrong post-merge head restores rollback dependencies without restarting"
fi

tmp="$(mktemp -d)"
export FAKE_CHANGED_FILES="pnpm-lock.yaml"
export FAKE_LIVE_INSTALL_RC=8
run_cycle "$tmp"
unset FAKE_CHANGED_FILES FAKE_LIVE_INSTALL_RC
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "failed live install exits nonzero" || fail "failed live install exits nonzero"
grep -Fq "T3DR_TARGET_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/ledger/t3-nightly-cycle.pending-live-deploy" \
  && pass "failed live install retains target marker" \
  || fail "failed live install retains target marker"
[[ "$(cat "$tmp/fake-head")" == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ]] \
  && pass "failed live install restores rollback checkout" \
  || fail "failed live install restores rollback checkout"
run_cycle "$tmp"
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "restored-checkout pending deploy retry exits zero" || fail "restored-checkout pending deploy retry exits zero"
[[ "$(grep -Fc "pnpm -C $tmp/checkout install --frozen-lockfile --prefer-offline" "$tmp/calls.log")" == "3" ]] \
  && pass "failed install restores dependencies before retrying target install" \
  || fail "failed install restores dependencies before retrying target install"
[[ ! -e "$tmp/ledger/t3-nightly-cycle.pending-live-deploy" ]] \
  && pass "same-sha retry clears pending marker after success" \
  || fail "same-sha retry clears pending marker after success"

tmp="$(mktemp -d)"
mkdir -p "$tmp/ledger"
cat >"$tmp/ledger/t3-nightly-cycle.pending-live-deploy" <<'EOF'
T3DR_ROLLBACK_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
T3DR_TARGET_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
T3DR_RESTART_MANAGER_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
EOF
printf 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n' >"$tmp/fake-head"
export FAKE_LIVE_INSTALL_RC=8
run_cycle "$tmp"
unset FAKE_LIVE_INSTALL_RC
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "already-deployed pending retry failure exits nonzero" || fail "already-deployed pending retry failure exits nonzero"
[[ "$(cat "$tmp/fake-head")" == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ]] \
  && pass "already-deployed pending retry restores the proven running rollback" \
  || fail "already-deployed pending retry restores the proven running rollback"

tmp="$(mktemp -d)"
mkdir -p "$tmp/ledger/$(date -u +%F)"
cat >"$tmp/ledger/t3-nightly-cycle.pending-live-deploy" <<'EOF'
T3DR_ROLLBACK_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
T3DR_TARGET_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
T3DR_RESTART_MANAGER_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
EOF
printf '2026-07-17T03:30:00Z RESULT OK pre_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa target_sha=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb snapshot=x manifest=y\n' \
  >"$tmp/ledger/$(date -u +%F)/t3-daily-restart.result"
printf 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n' >"$tmp/fake-head"
export FAKE_DEPLOYED_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
run_cycle "$tmp"
unset FAKE_DEPLOYED_SHA
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "successfully deployed pending marker reconciles" || fail "successfully deployed pending marker reconciles"
grep -Fq "reconciled pending live deploy already completed target_sha=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/ledger/"*/build-release-artifacts.log \
  && grep -Fq "T3DR_TARGET_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/ledger/"*/live-deploy.completed \
  && pass "successful restart evidence archives stale pending rollback" \
  || fail "successful restart evidence archives stale pending rollback"
if grep -Fq "rollback=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa target=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/calls.log"; then
  fail "reconciled deployment reused stale rollback"
else
  pass "reconciled deployment never reuses stale rollback"
fi

tmp="$(mktemp -d)"
export FAKE_ANCESTOR_RC=1
export FAKE_CHANGED_FILES="pnpm-lock.yaml"
run_cycle "$tmp"
unset FAKE_ANCESTOR_RC FAKE_CHANGED_FILES
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "non-fast-forward target exits nonzero" || fail "non-fast-forward target exits nonzero"
grep -Fq "target is not a fast-forward" "$tmp/ledger/"*/build-release-artifacts.log && pass "non-fast-forward target logged" || fail "non-fast-forward target logged"
grep -Fq "FAILURE step=build-release-artifacts rc=67" "$tmp/ledger/"*/t3-nightly-cycle.alert && pass "non-66 build failure alerts" || fail "non-66 build failure alerts"
[[ ! -e "$tmp/ledger/t3-nightly-cycle.pending-live-deploy" ]] \
  && pass "non-fast-forward dependency target never persists a live marker" \
  || fail "non-fast-forward dependency target never persists a live marker"
if grep -Fq "worktree add" "$tmp/calls.log" || grep -Fq "restart" "$tmp/calls.log"; then
  fail "non-fast-forward target staged or restarted"
else
  pass "non-fast-forward target aborts before staging"
fi

tmp="$(mktemp -d)"
mkdir -p "$tmp/ledger"
exec 8>"$tmp/ledger/t3-nightly-cycle.lock"
flock -n 8
run_cycle "$tmp"
flock -u 8
exec 8>&-
[[ "$(cat "$tmp/rc")" == "75" ]] && pass "lock contention exits 75" || fail "lock contention exits 75"
grep -Fq "FAILURE step=lock rc=75" "$tmp/ledger/"*/t3-nightly-cycle.alert && pass "lock contention alerts" || fail "lock contention alerts"
grep -Fq "RESULT FAILED step=lock rc=75" "$tmp/ledger/"*/t3-nightly-cycle.result && pass "lock contention records non-ok summary" || fail "lock contention records non-ok summary"

tmp="$(mktemp -d)"
T3DR_BUILD_ALWAYS=invalid run_cycle "$tmp"
[[ "$(cat "$tmp/rc")" == "2" ]] && pass "invalid configuration exits 2" || fail "invalid configuration exits 2"
grep -Fq "FAILURE step=configuration rc=2" "$tmp/ledger/"*/t3-nightly-cycle.alert && pass "invalid configuration alerts" || fail "invalid configuration alerts"

tmp="$(mktemp -d)"
export VITE_HTTP_URL=//localhost:3773 VITE_WS_URL=//127.0.0.2:3773 VITE_DEV_SERVER_URL=http://localhost:5733
T3DR_DESKTOP_ARTIFACT=1 run_cycle "$tmp"
unset VITE_HTTP_URL VITE_WS_URL VITE_DEV_SERVER_URL
stage="$tmp/ledger/$(date -u +%F)/prebuilt-stage/checkout"
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "desktop artifact enabled exits zero" || fail "desktop artifact enabled exits zero"
assert_order "$tmp/calls.log" "pnpm -C $stage/apps/web run build:hosted" "pnpm -C $stage run build:desktop" "pnpm -C $stage run dist:desktop:artifact" "assert-web-no-dev-endpoints.ts --force $stage/apps/web/dist" "restart prebuilt=1"
grep -Fq "pnpm-env T3CODE_HOSTED_BUILD=1 cmd=-C $stage run build:desktop" "$tmp/calls.log" && pass "desktop artifact build preserves hosted mode" || fail "desktop artifact build preserves hosted mode"
grep -Fq "pnpm-backend-env HTTP=unset WS=unset DEV=unset cmd=-C $stage run build:desktop" "$tmp/calls.log" && pass "desktop artifact build scrubs inherited backend env" || fail "desktop artifact build scrubs inherited backend env"
grep -Fq "pnpm-env T3CODE_HOSTED_BUILD=1 cmd=-C $stage run dist:desktop:artifact" "$tmp/calls.log" && pass "desktop artifact packaging preserves hosted mode" || fail "desktop artifact packaging preserves hosted mode"
grep -Fq "pnpm-backend-env HTTP=unset WS=unset DEV=unset cmd=-C $stage run dist:desktop:artifact" "$tmp/calls.log" && pass "desktop artifact packaging scrubs inherited backend env" || fail "desktop artifact packaging scrubs inherited backend env"
grep -Fq "desktop" "$tmp/ledger/$(date -u +%F)/desktop-artifact/artifact.txt" && pass "desktop artifact persists outside stage checkout" || fail "desktop artifact persists outside stage checkout"

echo "$pass_count passed, $fail_count failed"
[[ "$fail_count" -eq 0 ]]
