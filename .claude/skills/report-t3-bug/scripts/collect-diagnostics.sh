#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
usage: collect-diagnostics.sh [--tool NAME] [--args-file PATH] [--trace-file PATH] [--out PATH]

Emit a redacted Markdown diagnostics bundle for filing T3 Code failures in Linear.
USAGE
}

tool=""
args_file=""
trace_file="${T3CODE_TRACE_FILE:-}"
out=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --tool)
      tool="${2:-}"
      shift 2
      ;;
    --args-file)
      args_file="${2:-}"
      shift 2
      ;;
    --trace-file)
      trace_file="${2:-}"
      shift 2
      ;;
    --out)
      out="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

redact_stream() {
  sed -E \
    -e 's/(Authorization:[[:space:]]*Bearer[[:space:]]+)[^[:space:]]+/\1[REDACTED]/Ig' \
    -e 's/(CF-Access-Client-(Id|Secret):[[:space:]]*)[^[:space:]]+/\1[REDACTED]/Ig' \
    -e 's/("authorization"[[:space:]]*:[[:space:]]*"Bearer[[:space:]]+)[^"]+/\1[REDACTED]/Ig' \
    -e 's/((authorization)[[:space:]]*[=:][[:space:]]*Bearer[[:space:]]+)[^[:space:]",}]+/\1[REDACTED]/Ig' \
    -e 's/("([A-Za-z0-9_-]*(token|secret|password)[A-Za-z0-9_-]*|[A-Za-z0-9_-]*api(Key|[_-]?key)[A-Za-z0-9_-]*|cf-access-client-(id|secret)|cloudflareAccess(ClientId|ClientSecret|Token)|cloudflare_access_(client_id|client_secret|token))"[[:space:]]*:[[:space:]]*")[^"]+/\1[REDACTED]/Ig' \
    -e 's/((authorization|[A-Za-z0-9_-]*(token|secret|password)[A-Za-z0-9_-]*|[A-Za-z0-9_-]*api(Key|[_-]?key)[A-Za-z0-9_-]*|cf-access-client-(id|secret)|cloudflareAccess(ClientId|ClientSecret|Token)|cloudflare_access_(client_id|client_secret|token))[[:space:]]*[=:][[:space:]]*)[^[:space:]",}]+/\1[REDACTED]/Ig' \
    -e 's/((OPENAI|ANTHROPIC|GITHUB|GH|LINEAR|CLOUDFLARE)[_A-Z0-9-]*(TOKEN|KEY|SECRET)[=:][[:space:]]*)[^[:space:]]+/\1[REDACTED]/Ig'
}

emit() {
  {
    echo "## Environment"
    echo "- Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- Working directory: $(pwd)"
    echo "- Tool: ${tool:-unknown}"
    if git rev-parse --show-toplevel >/dev/null 2>&1; then
      echo "- Repo root: $(git rev-parse --show-toplevel)"
      echo "- Branch: $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
      echo "- HEAD: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
    fi
    echo "- Node: $(node --version 2>/dev/null || echo unavailable)"
    echo "- Bun: $(bun --version 2>/dev/null || echo unavailable)"
    echo

    if git rev-parse --show-toplevel >/dev/null 2>&1; then
      echo "## Git Status"
      echo '```'
      git status --short 2>/dev/null | redact_stream || true
      echo '```'
      echo
      echo "## Git Diff Stat"
      echo '```'
      git diff --stat 2>/dev/null | redact_stream || true
      echo '```'
      echo
    fi

    if [ -n "$args_file" ] && [ -f "$args_file" ]; then
      echo "## Tool Arguments"
      echo '```json'
      redact_stream < "$args_file"
      echo '```'
      echo
    fi

    if [ -n "$trace_file" ] && [ -f "$trace_file" ]; then
      echo "## Trace Tail"
      echo "- Source: $trace_file"
      echo '```'
      tail -n 200 "$trace_file" | redact_stream
      echo '```'
      echo
    fi
  } | redact_stream
}

if [ -n "$out" ]; then
  mkdir -p "$(dirname "$out")"
  emit > "$out"
  echo "$out"
else
  emit
fi
