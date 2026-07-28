#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/t3-verified-nightly-alert"
test_root="$(mktemp -d)"
trap 'rm -rf -- "$test_root"' EXIT

mkdir -p "$test_root/fixtures"
fake_gh="$test_root/gh"
fake_alert="$test_root/wizzo-alert"

cat > "$fake_gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
fixture_dir="${VERIFIED_NIGHTLY_TEST_FIXTURES:?}"
case "$1 $2" in
  "run list")
    cp "$fixture_dir/runs.json" /dev/stdout
    ;;
  "run view")
    cp "$fixture_dir/jobs-$3.json" /dev/stdout
    ;;
  "release view")
    cp "$fixture_dir/release-$3.json" /dev/stdout
    ;;
  *)
    echo "Unexpected fake gh invocation: $*" >&2
    exit 2
    ;;
esac
EOF

cat > "$fake_alert" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${VERIFIED_NIGHTLY_TEST_ALERT_FAIL:-0}" == "1" ]]; then
  exit 9
fi
printf '%s\n' "$*" >> "${VERIFIED_NIGHTLY_TEST_ALERT_LOG:?}"
EOF
chmod +x "$fake_gh" "$fake_alert" "$SCRIPT"

cat > "$test_root/fixtures/runs.json" <<'EOF'
[
  {"databaseId":102,"conclusion":"failure","url":"https://example.test/runs/102","updatedAt":"2026-07-21T09:00:00Z"}
]
EOF
cat > "$test_root/fixtures/jobs-101.json" <<'EOF'
{"jobs":[{"name":"Publish v0.0.32 verified stable","conclusion":"success","steps":[]}]}
EOF
cat > "$test_root/fixtures/jobs-102.json" <<'EOF'
{"jobs":[{"name":"Provider E2E Gate","conclusion":"failure","steps":[{"name":"Run provider verification suite","conclusion":"failure"}]}]}
EOF
cat > "$test_root/fixtures/release-v0.0.32.json" <<'EOF'
{"isDraft":false,"isPrerelease":false,"isLatest":false,"targetCommitish":"abc123","url":"https://example.test/releases/v0.0.32"}
EOF

state_file="$test_root/state/last-run"
alert_log="$test_root/alerts.log"
run_monitor() {
  VERIFIED_NIGHTLY_GH_BIN="$fake_gh" \
  VERIFIED_NIGHTLY_ALERT_BIN="$fake_alert" \
  VERIFIED_NIGHTLY_STATE_FILE="$state_file" \
  VERIFIED_NIGHTLY_TEST_FIXTURES="$test_root/fixtures" \
  VERIFIED_NIGHTLY_TEST_ALERT_LOG="$alert_log" \
    "$SCRIPT"
}

run_monitor
grep -Fxq '102' "$state_file"
[[ "$(wc -l < "$alert_log")" == "1" ]]
grep -Fq 'Nightly promotion refused: Provider E2E Gate' "$alert_log"
grep -Fq 'Run provider verification suite' "$alert_log"

cat > "$test_root/fixtures/runs.json" <<'EOF'
[
  {"databaseId":102,"conclusion":"failure","url":"https://example.test/runs/102","updatedAt":"2026-07-21T09:00:00Z"},
  {"databaseId":101,"conclusion":"success","url":"https://example.test/runs/101","updatedAt":"2026-07-21T10:00:00Z"}
]
EOF
run_monitor
[[ "$(wc -l < "$alert_log")" == "2" ]]
grep -Fxq '101' "$state_file"
grep -Fxq '102' "$state_file"
grep -Fq 'v0.0.32 verified nightly promotion' "$alert_log"

run_monitor
[[ "$(wc -l < "$alert_log")" == "2" ]]

printf '0\n' > "$state_file"
: > "$alert_log"
if VERIFIED_NIGHTLY_TEST_ALERT_FAIL=1 run_monitor; then
  echo "Expected alert delivery failure." >&2
  exit 1
fi
grep -Fxq '0' "$state_file"
[[ ! -s "$alert_log" ]]

echo "verified-nightly alert tests passed"
