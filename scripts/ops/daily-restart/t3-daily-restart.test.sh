#!/usr/bin/env bash
# Assertion chains are safe here because pass and fail always return success.
# shellcheck disable=SC2015
set -u

ROOT="$(git rev-parse --show-toplevel)"
SCRIPT="$ROOT/scripts/ops/daily-restart/t3-daily-restart"
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
    if [[ "${FAKE_TARGET_WEAKENS_INJECT_RESUME:-0}" == "1" ]]; then
      cat >"$T_TMP/bin/inject-resume" <<'WEAK'
#!/usr/bin/env bash
echo "inject-weakened token=${T3DR_TOKEN:-missing} $*" >>"$T_LOG"
WEAK
      chmod +x "$T_TMP/bin/inject-resume"
    fi
    if [[ "${FAKE_TARGET_WEAKENS_VERIFY_RESTART:-0}" == "1" ]]; then
      cat >"$T_TMP/bin/verify-restart" <<'WEAK'
#!/usr/bin/env bash
echo "verify-weakened $*" >>"$T_LOG"
WEAK
      chmod +x "$T_TMP/bin/verify-restart"
    fi
    if [[ "${FAKE_TARGET_WEAKENS_DB_RESTORE:-0}" == "1" ]]; then
      cat >"$T_TMP/bin/t3-db-restore" <<'WEAK'
#!/usr/bin/env bash
echo "restore-target $*" >>"$T_LOG"
exit 97
WEAK
      chmod +x "$T_TMP/bin/t3-db-restore"
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
  --user\ show\ *)
    state="$(cat "$state_file")"
    pid_file="$T_TMP/systemctl-pid"
    [[ -f "$pid_file" ]] || echo 111 >"$pid_file"
    if [[ "$state" == "active" ]]; then
      cat "$pid_file"
    else
      echo 0
    fi
    exit 0
    ;;
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
    pid_file="$T_TMP/systemctl-pid"
    pid=111
    [[ -f "$pid_file" ]] && pid="$(cat "$pid_file")"
    pid=$((pid + 1))
    echo "$pid" >"$pid_file"
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
echo "restore-original $*" >>"$T_LOG"
exit "${FAKE_RESTORE_RC:-0}"
SH
  cat >"$dir/capture-active-threads" <<'SH'
#!/usr/bin/env bash
echo "capture $*" >>"$T_LOG"
count_file="$T_TMP/capture-count"
count=0
[[ -f "$count_file" ]] && count="$(cat "$count_file")"
count=$((count + 1))
echo "$count" >"$count_file"
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--out" ]]; then
    if [[ "${FAKE_CAPTURE_BAD_JSON:-0}" == "1" || ( -n "${FAKE_CAPTURE_BAD_JSON_N:-}" && "$count" == "$FAKE_CAPTURE_BAD_JSON_N" ) ]]; then
      printf '{bad json\n' >"$2"
    elif [[ -n "${FAKE_CAPTURE_QUEUED_COUNT:-}" ]]; then
      count="$FAKE_CAPTURE_QUEUED_COUNT"
      printf '{"threads":[' >"$2"
      for i in $(seq 1 "$count"); do
        [[ "$i" == "1" ]] || printf ',' >>"$2"
        printf '{"thread_id":"queued-%s","role":"active","active_turn_id":"turn-%s","pending_message":{"message_id":"message-%s","role":"user","text":"queued","attachments":[]},"injected_at":null}' "$i" "$i" "$i" >>"$2"
      done
      printf ']}\n' >>"$2"
    elif [[ "${FAKE_CAPTURE_PHASE_THREADS:-0}" == "1" && "$2" == *.pre-stop.* ]]; then
      printf '{"threads":[{"thread_id":"pre-active","role":"active","active_turn_id":"turn-old"},{"thread_id":"pre-pending","role":"active","pending_message":{"message_id":"message-pre","role":"user","text":"pending","attachments":[]}}]}\n' >"$2"
    elif [[ "${FAKE_CAPTURE_PHASE_THREADS:-0}" == "1" && "$2" == *.post-stop.* ]]; then
      printf '{"threads":[{"thread_id":"post-active","active_turn_id":"turn-new"}]}\n' >"$2"
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
  cat >"$dir/verify-restart" <<'SH'
#!/usr/bin/env bash
echo "verify-restart $*" >>"$T_LOG"
if [[ "${FAKE_VERIFY_RESTART_RC:-0}" != "0" ]]; then
  echo "CHECK restart FAIL forced failure"
  exit "$FAKE_VERIFY_RESTART_RC"
fi
if [[ "${FAKE_VERIFY_RESTART_LEGACY_SHA_ONCE:-0}" == "1" && ! -f "$T_TMP/verify-restart-legacy-sha-failed" ]]; then
  : >"$T_TMP/verify-restart-legacy-sha-failed"
  echo "CHECK restart FAIL environment descriptor did not include serverBuildSha previous_pid=111 current_pid=112 expected_sha=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb actual_sha=unknown"
  exit 1
fi
echo "CHECK systemd PASS active"
echo "CHECK service_pid PASS"
echo "CHECK server_build_sha PASS"
SH
  cat >"$dir/inject-resume" <<'SH'
#!/usr/bin/env bash
echo "inject token=${T3DR_TOKEN:-missing} $*" >>"$T_LOG"
SH
  cat >"$dir/node" <<'SH'
#!/usr/bin/env bash
echo "node $*" >>"$T_LOG"
case "$*" in
  *"auth session issue"*)
    echo "mint-resume-token" >>"$T_LOG"
    printf '{"token":"%s","sessionId":"%s"}\n' "${FAKE_RESUME_TOKEN:-minted-token}" "${FAKE_RESUME_SESSION_ID:-minted-session}"
    exit "${FAKE_RESUME_TOKEN_MINT_RC:-0}"
    ;;
  *"auth session revoke"*)
    echo "revoke-resume-token ${*: -1}" >>"$T_LOG"
    exit "${FAKE_RESUME_TOKEN_REVOKE_RC:-0}"
    ;;
esac
if [[ "$1" == "-" && "${2:-}" == http* ]]; then
  echo "validate-resume-token origin=$2 token_file=$3" >>"$T_LOG"
  validate_count_file="$T_TMP/validate-resume-token-count"
  validate_count=0
  [[ -f "$validate_count_file" ]] && validate_count="$(cat "$validate_count_file")"
  validate_count=$((validate_count + 1))
  echo "$validate_count" >"$validate_count_file"
  if [[ "${FAKE_RESUME_TOKEN_VALIDATE_UNREACHABLE_ONCE:-0}" == "1" && "$validate_count" == "1" ]]; then
    exit 2
  fi
  if [[ -n "${FAKE_RESUME_TOKEN_VALIDATE_HTTP_STATUS:-}" && "$validate_count" == "1" ]]; then
    case "$FAKE_RESUME_TOKEN_VALIDATE_HTTP_STATUS" in
      502|503|504) exit 2 ;;
      *) exit 9 ;;
    esac
  fi
  scopes=" ${FAKE_RESUME_TOKEN_SCOPES:-orchestration:operate orchestration:read} "
  if [[ "$scopes" != *" orchestration:operate "* || "$scopes" != *" orchestration:read "* ]]; then
    exit 9
  fi
  exit "${FAKE_RESUME_TOKEN_VALIDATE_RC:-0}"
fi
exec "$REAL_NODE" "$@"
SH
  cat >"$dir/pnpm" <<'SH'
#!/usr/bin/env bash
echo "pnpm $*" >>"$T_LOG"
[[ -n "${T3CODE_BUILD_SHA:-}" ]] && echo "pnpm-env T3CODE_BUILD_SHA=$T3CODE_BUILD_SHA cmd=$*" >>"$T_LOG"
[[ -n "${T3CODE_HOSTED_BUILD:-}" ]] && echo "pnpm-env T3CODE_HOSTED_BUILD=$T3CODE_HOSTED_BUILD cmd=$*" >>"$T_LOG"
echo "pnpm-backend-env HTTP=${VITE_HTTP_URL-unset} WS=${VITE_WS_URL-unset} DEV=${VITE_DEV_SERVER_URL-unset} cmd=$*" >>"$T_LOG"
if [[ "${FAKE_PNPM_FAIL_ONCE:-0}" == "1" && ! -f "$T_TMP/pnpm-failed" ]]; then
  : >"$T_TMP/pnpm-failed"
  exit 8
fi
if [[ "$*" == *" install "* && -n "${FAKE_PNPM_INSTALL_RC:-}" ]]; then
  exit "$FAKE_PNPM_INSTALL_RC"
fi
[[ "${FAKE_PNPM_RC:-0}" == "0" ]] || exit "$FAKE_PNPM_RC"
cwd="${2:-}"
if [[ "$cwd" == */apps/web && "$*" == *" run build"* ]]; then
  mkdir -p "$cwd/dist"
  printf 'web\n' >"$cwd/dist/index.html"
  printf '{"buildSha":"%s"}\n' "${T3CODE_BUILD_SHA:-}" >"$cwd/dist/build-identity.json"
  [[ "${FAKE_WEB_DEV_ENDPOINT:-0}" == "1" ]] && printf 'http://user:p@ss@localhost:3773/api\n' >"$cwd/dist/bad.js"
fi
exit 0
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
  mkdir -p "$tmp/checkout/apps/web/src/components/settings"
  cp "$ROOT/apps/web/src/components/settings/SettingsFontPreviews.tsx" \
    "$tmp/checkout/apps/web/src/components/settings/SettingsFontPreviews.tsx"
  cp "$ROOT/apps/web/src/components/projectScriptEditor.tsx" \
    "$tmp/checkout/apps/web/src/components/projectScriptEditor.tsx"
  mkdir -p "$tmp/prebuilt-assets/apps/web/dist" "$tmp/prebuilt-assets/apps/server/dist/client"
  printf 'web\n' >"$tmp/prebuilt-assets/apps/web/dist/index.html"
  printf '{"buildSha":"%s"}\n' "${FAKE_TARGET_SHA:-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}" >"$tmp/prebuilt-assets/apps/web/dist/build-identity.json"
  printf 'server-client\n' >"$tmp/prebuilt-assets/apps/server/dist/client/index.html"
  printf 'server-bin\n' >"$tmp/prebuilt-assets/apps/server/dist/bin.mjs"
  make_fake_bin "$tmp/bin"
  T_TMP="$tmp" T_LOG="$tmp/calls.log" REAL_NODE="$REAL_NODE" T3DR_TEST_PATH_PREFIX="$tmp/bin" \
    T3DR_STOP_TIMEOUT="${T3DR_STOP_TIMEOUT:-1}" T3DR_KILL_TIMEOUT="${T3DR_KILL_TIMEOUT:-1}" \
    T3DR_STATIC_DIR="${T3DR_STATIC_DIR:-}" T3DR_SERVICE_CONFIG_DIR="${T3DR_SERVICE_CONFIG_DIR:-$tmp/systemd}" \
    T3DR_PREBUILT_ASSETS_DIR="$tmp/prebuilt-assets" "$SCRIPT" \
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

prepare_static_release() {
  local tmp="$1" sha="$2" release="$1/releases/web/${2:0:12}-previous/dist"
  mkdir -p "$release"
  printf 'previous web\n' >"$release/index.html"
  printf '{"buildSha":"%s"}\n' "$sha" >"$release/build-identity.json"
  ln -s "$release" "$tmp/releases/web/current"
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
run_manager "$tmp"
unset VITE_HTTP_URL VITE_WS_URL VITE_DEV_SERVER_URL
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "happy path exits zero" || fail "happy path exits zero"
[[ -f "$tmp/ledger/$(date -u +%F)/pinned-tools/web-scanner/hosted-display-url-allowlist.ts" ]] &&
  pass "pinned scanner includes display URL provenance manifest" ||
  fail "pinned scanner includes display URL provenance manifest"
grep -Fq "assert-web-no-dev-endpoints.ts --force $tmp/checkout/apps/web/dist --repo-root $tmp/checkout" "$tmp/calls.log" &&
  pass "pinned scanner verifies the checkout that produced the build" ||
  fail "pinned scanner verifies the checkout that produced the build"
assert_order "$tmp/calls.log" "git -C" "snapshot" "capture" "systemctl --user stop" "capture" "snapshot" "git -C" "pnpm -C" "systemctl --user start" "verify-restart" "health" "inject"
assert_order "$tmp/calls.log" "mint-resume-token" "validate-resume-token" "systemctl --user stop"
grep -Fq "inject token=minted-token" "$tmp/calls.log" && pass "resume injection receives preflight token" || fail "resume injection receives preflight token"
grep -Fq "revoke-resume-token minted-session" "$tmp/calls.log" && pass "minted resume token is revoked" || fail "minted resume token is revoked"
assert_order "$tmp/calls.log" "snapshot --db" "capture --db" "systemctl --user stop" "capture --db" "snapshot --db"
assert_order "$tmp/calls.log" "git -C" "pnpm -C $tmp/checkout/apps/web run build:hosted" "pnpm -C $tmp/checkout run build:desktop" "assert-web-no-dev-endpoints.ts --force $tmp/checkout/apps/web/dist" "systemctl --user start fake.service"
grep -Fq "pnpm-env T3CODE_BUILD_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb cmd=-C $tmp/checkout/apps/web run build:hosted" "$tmp/calls.log" \
  && pass "happy path stamps standalone web build" \
  || fail "happy path stamps standalone web build"
grep -Fq "pnpm-backend-env HTTP=unset WS=unset DEV=unset cmd=-C $tmp/checkout/apps/web run build:hosted" "$tmp/calls.log" && pass "hosted target build scrubs inherited backend env" || fail "hosted target build scrubs inherited backend env"
grep -Fq "pnpm-env T3CODE_BUILD_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb cmd=-C $tmp/checkout run build:desktop" "$tmp/calls.log" \
  && pass "happy path stamps desktop/server build" \
  || fail "happy path stamps desktop/server build"
grep -Fq "pnpm-env T3CODE_HOSTED_BUILD=1 cmd=-C $tmp/checkout run build:desktop" "$tmp/calls.log" && pass "desktop/server build preserves hosted mode" || fail "desktop/server build preserves hosted mode"
grep -Fq "pnpm-backend-env HTTP=unset WS=unset DEV=unset cmd=-C $tmp/checkout run build:desktop" "$tmp/calls.log" && pass "desktop/server build scrubs inherited backend env" || fail "desktop/server build scrubs inherited backend env"
if awk 'f && /pnpm/ { found=1 } /health/ { f=1 } END { exit found ? 0 : 1 }' "$tmp/calls.log"; then
  fail "happy path built after health"
else
  pass "happy path builds before health"
fi
grep -Fq '"pre_sha"' "$tmp/ledger/"*/resume-manifest.json && pass "manifest filled" || fail "manifest filled"
grep -Fq "snapshot --db $tmp/state.sqlite --out-dir $tmp/snaps" "$tmp/calls.log" && pass "snapshot helper receives current flags" || fail "snapshot helper receives current flags"
grep -Fq "capture --db $tmp/state.sqlite --out $tmp/ledger/" "$tmp/calls.log" && pass "capture helper receives current flags" || fail "capture helper receives current flags"
grep -Eq "capture --db $tmp/state.sqlite --out $tmp/ledger/.*/resume-manifest\\.json\\.post-stop\\.[0-9]+ --stopped-since [0-9]{4}-[0-9]{2}-[0-9]{2}T.* --pending-since [0-9]{4}-[0-9]{2}-[0-9]{2}T" "$tmp/calls.log" && pass "post-stop capture receives shutdown boundary" || fail "post-stop capture receives shutdown boundary"
grep -Fq "health --origin http://127.0.0.1:1 --service fake.service --instance fakeAgent --model fake-model --timeout 1" "$tmp/calls.log" && pass "health probe receives smoke provider" || fail "health probe receives smoke provider"
grep -Fq "verify-restart --service fake.service --origin http://127.0.0.1:1 --checkout $tmp/checkout --expected-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb --previous-pid 111 --timeout 1" "$tmp/calls.log" && pass "verify-restart receives pid and target" || fail "verify-restart receives pid and target"
[[ ! -e "$tmp/systemd/fake.service.d/20-t3dr-static-release.conf" ]] && pass "unconfigured restart preserves checkout-relative static behavior" || fail "unconfigured restart preserves checkout-relative static behavior"

tmp="$(mktemp -d)"
mkdir -p "$tmp/releases/web/.t3dr-stale/dist"
prepare_static_release "$tmp" aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
previous_static="$(readlink -f "$tmp/releases/web/current")"
T3DR_STATIC_DIR="$tmp/releases/web/current" run_manager "$tmp" --prebuilt-target --rollback-sha aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa --target-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
dropin="$tmp/systemd/fake.service.d/20-t3dr-static-release.conf"
published_static="$(readlink -f "$tmp/releases/web/current")"
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "configured static release exits zero" || fail "configured static release exits zero"
grep -Fxq "[Service]" "$dropin" && grep -Fxq "Environment=T3CODE_STATIC_DIR=$tmp/releases/web/current" "$dropin" && pass "configured release is persisted independently of checkout" || fail "configured release is persisted independently of checkout"
assert_order "$tmp/calls.log" "systemctl --user daemon-reload" "systemctl --user stop fake.service" "systemctl --user start fake.service"
[[ "$published_static" != "$previous_static" ]] && grep -Fq 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' "$published_static/build-identity.json" && [[ "$(cat "$tmp/releases/web/previous")" == "$previous_static" ]] && pass "target static release is published atomically with rollback metadata" || fail "target static release is published atomically with rollback metadata"
assert_order "$tmp/calls.log" "systemctl --user stop fake.service" "assert-web-no-dev-endpoints.ts --force $published_static" "systemctl --user start fake.service"
[[ ! -e "$tmp/releases/web/.t3dr-stale" ]] && pass "superseded static releases are pruned" || fail "superseded static releases are pruned"

tmp="$(mktemp -d)"
prepare_static_release "$tmp" aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
previous_static="$(readlink -f "$tmp/releases/web/current")"
FAKE_HEALTH_FAIL_ONCE=1 T3DR_STATIC_DIR="$tmp/releases/web/current" run_manager "$tmp"
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "configured static health failure exits nonzero" || fail "configured static health failure exits nonzero"
[[ "$(readlink -f "$tmp/releases/web/current")" == "$previous_static" ]] && pass "rollback restores the previous static release pointer" || fail "rollback restores the previous static release pointer"

tmp="$(mktemp -d)"
prepare_static_release "$tmp" aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
FAKE_WEB_DEV_ENDPOINT=1 T3DR_STATIC_DIR="$tmp/releases/web/current" run_manager "$tmp"
[[ "$(cat "$tmp/rc")" != "0" && -z "$(find "$tmp/releases/web" -maxdepth 1 -type d -name '.t3dr-*' -print -quit)" ]] && pass "failed static candidates are removed" || fail "failed static candidates are removed"

tmp="$(mktemp -d)"
prepare_static_release "$tmp" aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
T3DR_STATIC_DIR="$tmp/releases/web/current" run_manager "$tmp" --service fake
[[ "$(cat "$tmp/rc")" == "2" ]] && pass "static release rejects a shorthand service unit" || fail "static release rejects a shorthand service unit"
[[ ! -e "$tmp/systemd/fake.d/20-t3dr-static-release.conf" ]] && pass "shorthand service cannot create an unused drop-in" || fail "shorthand service cannot create an unused drop-in"

tmp="$(mktemp -d)"
T3DR_TOKEN=pre-minted-token T3DR_TOKEN_SESSION_ID=pre-minted-session run_manager "$tmp"
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "pre-minted resume token exits zero" || fail "pre-minted resume token exits zero"
if grep -Fq "mint-resume-token" "$tmp/calls.log"; then
  fail "pre-minted resume token was minted again"
else
  pass "pre-minted resume token skips checkout-hosted mint"
fi
grep -Fq "revoke-resume-token pre-minted-session" "$tmp/calls.log" \
  && pass "pre-minted resume token session is revoked" \
  || fail "pre-minted resume token session is revoked"

tmp="$(mktemp -d)"
export FAKE_PRE_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
run_manager "$tmp" \
  --rollback-sha aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --target-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
unset FAKE_PRE_SHA
[[ "$(cat "$tmp/rc")" == "2" ]] && pass "live fallback without pre-minted token exits two" || fail "live fallback without pre-minted token exits two"
grep -Fq "live fallback requires a resume token minted before checkout dependency mutation" "$tmp/stderr" \
  && pass "live fallback without pre-minted token fails closed" \
  || fail "live fallback without pre-minted token fails closed"

tmp="$(mktemp -d)"
export FAKE_PRE_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
T3DR_TOKEN=pre-minted-token T3DR_TOKEN_SESSION_ID=deferred-session \
  T3DR_DEFER_RESUME_TOKEN_REVOKE=1 run_manager "$tmp" \
  --rollback-sha aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --target-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
unset FAKE_PRE_SHA
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "deferred live token handoff exits zero" || fail "deferred live token handoff exits zero"
grep -Fq "revoke-resume-token deferred-session" "$tmp/calls.log" \
  && pass "deferred live token revokes after a consistent server build" \
  || fail "deferred live token revokes after a consistent server build"

tmp="$(mktemp -d)"
export FAKE_PRE_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
export FAKE_SNAPSHOT_FAIL_N=1
T3DR_TOKEN=pre-minted-token T3DR_TOKEN_SESSION_ID=deferred-session \
  T3DR_DEFER_RESUME_TOKEN_REVOKE=1 run_manager "$tmp" \
  --rollback-sha aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --target-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
unset FAKE_PRE_SHA FAKE_SNAPSHOT_FAIL_N
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "deferred live token pre-build failure exits nonzero" || fail "deferred live token pre-build failure exits nonzero"
if grep -Fq "revoke-resume-token deferred-session" "$tmp/calls.log"; then
  fail "deferred live token used an inconsistent server artifact for revocation"
else
  pass "deferred live token avoids inconsistent server artifact revocation"
fi

tmp="$(mktemp -d)"
export FAKE_VERIFY_RESTART_RC=9
export VITE_HTTP_URL=//localhost:3773 VITE_WS_URL=//127.0.0.2:3773 VITE_DEV_SERVER_URL=http://localhost:5733
run_manager "$tmp"
unset FAKE_VERIFY_RESTART_RC VITE_HTTP_URL VITE_WS_URL VITE_DEV_SERVER_URL
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "verify-restart failure exits nonzero" || fail "verify-restart failure exits nonzero"
assert_order "$tmp/calls.log" "systemctl --user start fake.service" "verify-restart" "systemctl --user stop fake.service" "restore" "git -C $tmp/checkout checkout aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "pnpm -C $tmp/checkout install --frozen-lockfile --prefer-offline" "pnpm -C $tmp/checkout/apps/web run build" "assert-web-no-dev-endpoints.ts --force $tmp/checkout/apps/web/dist" "pnpm -C $tmp/checkout run build:desktop" "assert-web-no-dev-endpoints.ts --force $tmp/checkout/apps/web/dist" "systemctl --user start fake.service" "health"
grep -Fq "pnpm-env T3CODE_HOSTED_BUILD=1 cmd=-C $tmp/checkout/apps/web run build" "$tmp/calls.log" && pass "legacy rollback build preserves hosted mode before the forced pinned scan" || fail "legacy rollback build preserves hosted mode before the forced pinned scan"
grep -Fq "pnpm-backend-env HTTP=unset WS=unset DEV=unset cmd=-C $tmp/checkout/apps/web run build" "$tmp/calls.log" && pass "legacy rollback build scrubs inherited backend env" || fail "legacy rollback build scrubs inherited backend env"
grep -Fq "pnpm-env T3CODE_HOSTED_BUILD=1 cmd=-C $tmp/checkout run build:desktop" "$tmp/calls.log" && pass "legacy rollback desktop build preserves hosted mode" || fail "legacy rollback desktop build preserves hosted mode"
grep -Fq "pnpm-backend-env HTTP=unset WS=unset DEV=unset cmd=-C $tmp/checkout run build:desktop" "$tmp/calls.log" && pass "legacy rollback desktop build scrubs inherited backend env" || fail "legacy rollback desktop build scrubs inherited backend env"
grep -Fq "RESULT ROLLBACK-OK" "$tmp/ledger/"*/t3-daily-restart.result && pass "verify-restart failure rolls back loudly" || fail "verify-restart failure rolls back loudly"


tmp="$(mktemp -d)"
export FAKE_VERIFY_RESTART_RC=9
export FAKE_PNPM_INSTALL_RC=8
run_manager "$tmp"
unset FAKE_VERIFY_RESTART_RC FAKE_PNPM_INSTALL_RC
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "rollback dependency install failure exits nonzero" || fail "rollback dependency install failure exits nonzero"
assert_order "$tmp/calls.log" "verify-restart" "systemctl --user stop fake.service" "restore" "git -C $tmp/checkout checkout aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "pnpm -C $tmp/checkout install --frozen-lockfile --prefer-offline"
grep -Fq "ROLLBACK dependency restore failed after db restore" "$tmp/ledger/"*/t3-daily-restart.log \
  && grep -Fq "RESULT ROLLBACK-FAILED" "$tmp/ledger/"*/t3-daily-restart.result \
  && pass "rollback install failure restores quiesced database first" \
  || fail "rollback install failure restores quiesced database first"

tmp="$(mktemp -d)"
caller_path="$tmp/nonstandard-node"
mkdir -p "$caller_path"
old_path="$PATH"
export FAKE_LOG_PATH=1
PATH="$caller_path:$old_path" run_manager "$tmp"
unset FAKE_LOG_PATH
PATH="$old_path"
grep -Fq "$caller_path" "$tmp/calls.log" && pass "caller PATH is preserved after trusted prefixes" || fail "caller PATH is preserved after trusted prefixes"

tmp="$(mktemp -d)"
export FAKE_CAPTURE_PHASE_THREADS=1
run_manager "$tmp"
unset FAKE_CAPTURE_PHASE_THREADS
manifest_path="$(echo "$tmp/ledger/"*/resume-manifest.json)"
grep -Fq -- "--include-pending-message-id message-pre" "$tmp/calls.log" && pass "post-stop capture receives pre-stop pending ids" || fail "post-stop capture receives pre-stop pending ids"
grep -Fq -- "--include-active-thread-id pre-active" "$tmp/calls.log" && pass "post-stop capture receives pre-stop active ids" || fail "post-stop capture receives pre-stop active ids"
if grep -Fq -- "--include-active-thread-id pre-pending" "$tmp/calls.log"; then fail "post-stop capture skips pending-only active ids"; else pass "post-stop capture skips pending-only active ids"; fi
if node - "$manifest_path" <<'NODE'
const fs = require("node:fs");
const manifest = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const ids = new Set((manifest.threads ?? []).map((thread) => thread.thread_id));
process.exit(ids.has("post-active") && !ids.has("pre-pending") && !ids.has("pre-active") ? 0 : 1);
NODE
then
  pass "post-stop manifest stays authoritative after pending-id handoff"
else
  fail "post-stop manifest stays authoritative after pending-id handoff"
fi

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
export FAKE_RESUME_TOKEN_VALIDATE_RC=7
run_manager "$tmp"
unset FAKE_RESUME_TOKEN_VALIDATE_RC
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "resume token validation failure exits nonzero" || fail "resume token validation failure exits nonzero"
if grep -Fq "systemctl --user stop" "$tmp/calls.log"; then
  fail "resume token validation failure stopped service"
else
  pass "resume token validation failure aborts before stop"
fi
grep -Fq "RESULT FAILED" "$tmp/ledger/"*/t3-daily-restart.result && pass "resume token validation failure recorded" || fail "resume token validation failure recorded"

tmp="$(mktemp -d)"
export FAKE_RESUME_TOKEN_SCOPES="orchestration:operate"
run_manager "$tmp"
unset FAKE_RESUME_TOKEN_SCOPES
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "operate-only resume token exits nonzero" || fail "operate-only resume token exits nonzero"
if grep -Fq "systemctl --user stop" "$tmp/calls.log"; then
  fail "operate-only resume token stopped service"
else
  pass "operate-only resume token aborts before stop"
fi

tmp="$(mktemp -d)"
export FAKE_RESUME_TOKEN_SCOPES="orchestration:read"
run_manager "$tmp"
unset FAKE_RESUME_TOKEN_SCOPES
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "read-only resume token exits nonzero" || fail "read-only resume token exits nonzero"
if grep -Fq "systemctl --user stop" "$tmp/calls.log"; then
  fail "read-only resume token stopped service"
else
  pass "read-only resume token aborts before stop"
fi

tmp="$(mktemp -d)"
export FAKE_RESUME_TOKEN_VALIDATE_UNREACHABLE_ONCE=1
run_manager "$tmp"
unset FAKE_RESUME_TOKEN_VALIDATE_UNREACHABLE_ONCE
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "unreachable origin during token validation recovers" || fail "unreachable origin during token validation recovers"
assert_order "$tmp/calls.log" "validate-resume-token" "systemctl --user stop fake.service" "systemctl --user start fake.service" "validate-resume-token" "inject"
grep -Fq "RESULT OK" "$tmp/ledger/"*/t3-daily-restart.result && pass "deferred token validation records ok after recovery" || fail "deferred token validation records ok after recovery"

tmp="$(mktemp -d)"
export FAKE_RESUME_TOKEN_VALIDATE_HTTP_STATUS=503
run_manager "$tmp"
unset FAKE_RESUME_TOKEN_VALIDATE_HTTP_STATUS
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "service-unavailable token validation response recovers" || fail "service-unavailable token validation response recovers"
assert_order "$tmp/calls.log" "validate-resume-token" "systemctl --user stop fake.service" "systemctl --user start fake.service" "validate-resume-token" "inject"

tmp="$(mktemp -d)"
export FAKE_RESUME_TOKEN_VALIDATE_HTTP_STATUS=401
run_manager "$tmp"
unset FAKE_RESUME_TOKEN_VALIDATE_HTTP_STATUS
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "auth HTTP token validation response exits nonzero" || fail "auth HTTP token validation response exits nonzero"
if grep -Fq "systemctl --user stop" "$tmp/calls.log"; then
  fail "auth HTTP token validation response stopped service"
else
  pass "auth HTTP token validation response aborts before stop"
fi

tmp="$(mktemp -d)"
run_manager "$tmp" --origin not-a-url
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "malformed origin exits nonzero" || fail "malformed origin exits nonzero"
if grep -Fq "systemctl --user stop" "$tmp/calls.log"; then
  fail "malformed origin stopped service"
else
  pass "malformed origin aborts before stop"
fi

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
assert_order "$tmp/calls.log" "systemctl --user stop fake.service" "systemctl --user kill fake.service" "systemctl --user is-active fake.service" "capture --db" "snapshot --db" "git -C $tmp/checkout merge --ff-only"
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
mkdir -p "$tmp/checkout/apps/web"
printf '{"scripts":{"build":"vp build","build:hosted":"vp build"}}\n' >"$tmp/checkout/apps/web/package.json"
export FAKE_HEALTH_FAIL_ONCE=1
run_manager "$tmp"
unset FAKE_HEALTH_FAIL_ONCE
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "health rollback exits nonzero" || fail "health rollback exits nonzero"
assert_order "$tmp/calls.log" "health" "systemctl --user stop" "restore" "git -C" "pnpm -C $tmp/checkout install --frozen-lockfile --prefer-offline" "pnpm -C $tmp/checkout/apps/web run build:hosted" "pnpm -C $tmp/checkout run build:desktop" "systemctl --user start" "health"
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
export FAKE_TARGET_WEAKENS_DB_RESTORE=1
run_manager "$tmp"
unset FAKE_HEALTH_FAIL_ONCE FAKE_TARGET_WEAKENS_DB_RESTORE
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "target-tainted db restore rollback exits nonzero" || fail "target-tainted db restore rollback exits nonzero"
grep -Fq "restore-original --db $tmp/state.sqlite" "$tmp/calls.log" \
  && pass "rollback uses the pinned pre-update db restore" \
  || fail "rollback uses the pinned pre-update db restore"
if grep -Fq "restore-target" "$tmp/calls.log"; then
  fail "rollback used the target-tainted db restore"
else
  pass "rollback ignores the target-tainted db restore"
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
export FAKE_TARGET_WEAKENS_INJECT_RESUME=1
run_manager "$tmp"
unset FAKE_TARGET_WEAKENS_INJECT_RESUME
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "weakened target inject-resume still exits zero" || fail "weakened target inject-resume still exits zero"
if grep -Fq "inject-weakened" "$tmp/calls.log"; then
  fail "target inject-resume was used after update"
else
  pass "pinned pre-update inject-resume used after update"
fi

tmp="$(mktemp -d)"
export FAKE_TARGET_WEAKENS_VERIFY_RESTART=1
run_manager "$tmp"
unset FAKE_TARGET_WEAKENS_VERIFY_RESTART
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "weakened target verify-restart still exits zero" || fail "weakened target verify-restart still exits zero"
if grep -Fq "verify-weakened" "$tmp/calls.log"; then
  fail "target verify-restart was used after update"
else
  pass "pinned pre-update verify-restart used after update"
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
assert_order "$tmp/calls.log" "pnpm -C $tmp/checkout/apps/web run build:hosted" "systemctl --user stop" "git -C" "pnpm -C $tmp/checkout install --frozen-lockfile --prefer-offline" "pnpm -C $tmp/checkout/apps/web run build" "pnpm -C $tmp/checkout run build:desktop" "systemctl --user start" "health"
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
export FAKE_CAPTURE_QUEUED_COUNT=3
run_manager "$tmp"
unset FAKE_CAPTURE_QUEUED_COUNT
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "queued manifest exits zero" || fail "queued manifest exits zero"
grep -Fq "RUN timeout=960 " "$tmp/ledger/"*/t3-daily-restart.log && pass "inject timeout scales with queued replay waits" || fail "inject timeout scales with queued replay waits"

tmp="$(mktemp -d)"
export FAKE_CAPTURE_BAD_JSON=1
run_manager "$tmp"
unset FAKE_CAPTURE_BAD_JSON
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "bad manifest exits nonzero" || fail "bad manifest exits nonzero"
if grep -Fq "systemctl --user stop" "$tmp/calls.log"; then fail "bad manifest stopped service"; else pass "bad manifest aborts before stop"; fi

tmp="$(mktemp -d)"
export FAKE_CAPTURE_BAD_JSON_N=2
run_manager "$tmp"
unset FAKE_CAPTURE_BAD_JSON_N
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "post-stop bad manifest exits nonzero" || fail "post-stop bad manifest exits nonzero"
assert_order "$tmp/calls.log" "systemctl --user stop fake.service" "capture --db" "systemctl --user start fake.service" "inject"
grep -Fq "inject token=minted-token --origin http://127.0.0.1:1 --manifest $tmp/ledger/" "$tmp/calls.log" && pass "post-stop bad manifest injects sanitized fallback manifest" || fail "post-stop bad manifest injects sanitized fallback manifest"

tmp="$(mktemp -d)"
export FAKE_CAPTURE_PHASE_THREADS=1
export FAKE_CAPTURE_BAD_JSON_N=2
run_manager "$tmp"
unset FAKE_CAPTURE_PHASE_THREADS FAKE_CAPTURE_BAD_JSON_N
manifest_path="$(echo "$tmp/ledger/"*/resume-manifest.json)"
if node - "$manifest_path" <<'NODE'
const fs = require("node:fs");
const manifest = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const threads = manifest.threads ?? [];
process.exit(
  threads.length === 1 &&
    threads[0]?.thread_id === "pre-active" &&
    threads[0]?.pending_message === undefined
    ? 0
    : 1,
);
NODE
then
  pass "post-stop capture failure strips pending fallback replay"
else
  fail "post-stop capture failure strips pending fallback replay"
fi

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
export FAKE_TARGET_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
export FAKE_VERIFY_RESTART_LEGACY_SHA_ONCE=1
run_manager "$tmp"
unset FAKE_PRE_SHA FAKE_TARGET_SHA FAKE_VERIFY_RESTART_LEGACY_SHA_ONCE
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "same-sha legacy unstamped rebuild exits zero" || fail "same-sha legacy unstamped rebuild exits zero"
assert_order "$tmp/calls.log" "systemctl --user start fake.service" "verify-restart" "systemctl --user stop fake.service" "pnpm -C $tmp/checkout/apps/web run build" "assert-web-no-dev-endpoints.ts --force $tmp/checkout/apps/web/dist" "pnpm -C $tmp/checkout run build:desktop" "systemctl --user start fake.service" "verify-restart" "health"
grep -Fq "pnpm-env T3CODE_BUILD_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb cmd=-C $tmp/checkout/apps/web run build" "$tmp/calls.log" \
  && pass "same-sha legacy unstamped rebuild stamps web target sha" \
  || fail "same-sha legacy unstamped rebuild stamps web target sha"
grep -Fq "pnpm-env T3CODE_BUILD_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb cmd=-C $tmp/checkout run build:desktop" "$tmp/calls.log" \
  && pass "same-sha legacy unstamped rebuild stamps deploy target sha" \
  || fail "same-sha legacy unstamped rebuild stamps deploy target sha"

tmp="$(mktemp -d)"
export FAKE_PRE_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
run_manager "$tmp" --target-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
unset FAKE_PRE_SHA
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "pinned target restart exits zero" || fail "pinned target restart exits zero"
if grep -Fq "fetch origin" "$tmp/calls.log"; then
  fail "pinned target restart fetched origin"
else
  pass "pinned target restart skips fetch"
fi

tmp="$(mktemp -d)"
export FAKE_PRE_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
T3DR_TOKEN=live-fallback-token run_manager "$tmp" \
  --rollback-sha aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --target-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
unset FAKE_PRE_SHA
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "live fallback restart exits zero" || fail "live fallback restart exits zero"
grep -Fq "pnpm-env T3CODE_BUILD_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb cmd=-C $tmp/checkout/apps/web run build:hosted" "$tmp/calls.log" \
  && grep -Fq "pnpm-env T3CODE_BUILD_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb cmd=-C $tmp/checkout run build:desktop" "$tmp/calls.log" \
  && pass "live fallback rollback metadata forces web and server rebuilds" \
  || fail "live fallback rollback metadata forces web and server rebuilds"
grep -Fq '"pre_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"' "$tmp/ledger/"*/resume-manifest.json \
  && pass "live fallback manifest records the pre-cycle rollback sha" \
  || fail "live fallback manifest records the pre-cycle rollback sha"

tmp="$(mktemp -d)"
export FAKE_PRE_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
export FAKE_VERIFY_RESTART_RC=9
T3DR_TOKEN=live-fallback-token run_manager "$tmp" \
  --rollback-sha aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --target-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
unset FAKE_PRE_SHA FAKE_VERIFY_RESTART_RC
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "live fallback verification failure exits nonzero" || fail "live fallback verification failure exits nonzero"
grep -Fq "git -C $tmp/checkout checkout aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "$tmp/calls.log" \
  && grep -Fq "pnpm -C $tmp/checkout install --frozen-lockfile --prefer-offline" "$tmp/calls.log" \
  && pass "live fallback verification failure restores pre-cycle source and dependencies" \
  || fail "live fallback verification failure restores pre-cycle source and dependencies"

tmp="$(mktemp -d)"
run_manager "$tmp" --prebuilt-target \
  --rollback-sha aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --target-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
[[ "$(cat "$tmp/rc")" == "0" ]] && pass "prebuilt target exits zero" || fail "prebuilt target exits zero"
if grep -Fq "fetch origin" "$tmp/calls.log"; then
  fail "prebuilt target fetched origin"
else
  pass "prebuilt target skips fetch"
fi
if grep -Fq "git -C $tmp/checkout merge --ff-only bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "$tmp/calls.log"; then
  pass "prebuilt target merges only after shutdown"
else
  fail "prebuilt target merges only after shutdown"
fi
if grep -Fq "pnpm" "$tmp/calls.log"; then
  fail "prebuilt target rebuilt during downtime"
else
  pass "prebuilt target skips rebuild during downtime"
fi
grep -Fq "server-client" "$tmp/checkout/apps/server/dist/client/index.html" && pass "prebuilt target promotes server client assets" || fail "prebuilt target promotes server client assets"
grep -Fq "server-bin" "$tmp/checkout/apps/server/dist/bin.mjs" && pass "prebuilt target promotes server executable" || fail "prebuilt target promotes server executable"
grep -Fq "web" "$tmp/checkout/apps/web/dist/index.html" && pass "prebuilt target promotes web assets" || fail "prebuilt target promotes web assets"

tmp="$(mktemp -d)"
export FAKE_HEALTH_FAIL_ONCE=1
run_manager "$tmp" --prebuilt-target \
  --rollback-sha aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --target-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
unset FAKE_HEALTH_FAIL_ONCE
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "prebuilt health failure exits nonzero" || fail "prebuilt health failure exits nonzero"
grep -Fq "git -C $tmp/checkout checkout aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "$tmp/calls.log" \
  && grep -Fq "pnpm -C $tmp/checkout install --frozen-lockfile --prefer-offline" "$tmp/calls.log" \
  && pass "prebuilt rollback restores rollback source and dependencies" \
  || fail "prebuilt rollback restores rollback source and dependencies"

tmp="$(mktemp -d)"
export FAKE_SNAPSHOT_FAIL_N=2
run_manager "$tmp" --prebuilt-target \
  --rollback-sha aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --target-sha bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
unset FAKE_SNAPSHOT_FAIL_N
[[ "$(cat "$tmp/rc")" != "0" ]] && pass "prebuilt quiesced snapshot failure exits nonzero" || fail "prebuilt quiesced snapshot failure exits nonzero"
assert_order "$tmp/calls.log" "systemctl --user stop fake.service" "capture --db" "snapshot --db" "systemctl --user start fake.service" "inject"
if grep -Fq "git -C $tmp/checkout checkout aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "$tmp/calls.log" || grep -Fq "pnpm -C $tmp/checkout/apps/web run build:hosted" "$tmp/calls.log"; then
  fail "prebuilt snapshot failure ran code rollback before update"
else
  pass "prebuilt snapshot failure restarts unchanged checkout"
fi
grep -Fq "RESULT SNAPSHOT-FAILED" "$tmp/ledger/"*/t3-daily-restart.result && pass "prebuilt snapshot failure recorded" || fail "prebuilt snapshot failure recorded"

echo "$pass_count passed, $fail_count failed"
[[ "$fail_count" -eq 0 ]]
