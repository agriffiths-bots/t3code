#!/usr/bin/env bash
# precommit-gate.sh — the software-factory commit gate.
#
# Guarantees every commit (1) was built from a fully-staged tree, (2) passes
# the repo's static checks, and (3) is clean on autoreview
# — or carries an explicit, audited dismissal for every finding.
#
# Modes:
#   precommit-gate.sh hook       # invoked by .githooks/pre-commit
#   precommit-gate.sh --prepare  # run the same gate ahead of `git commit`;
#                                # on PASS it records a marker so the commit
#                                # itself is instant (recommended: run this in
#                                # the background while you write the message)
#   precommit-gate.sh --status   # show marker / dismissal / last-findings state
#
# Scope correctness: instead of stash tricks, the gate REQUIRES the index to
# match the working tree (and no untracked, non-ignored files). Then whole-tree
# static checks and a `--mode local` review validate exactly the tree being
# committed, no matter how the caller staged it (git add -A, commit -a, ...).
#
# A PASS is cached against the exact (HEAD, staged-tree) pair in
# .git/factory/gate-ok; any change to the staged content invalidates it.
#
# Disagreeing with findings — write .git/factory/dismissals.json (JSON array);
# every CURRENT finding must be covered by a valid entry that identifies it
# EXACTLY (file + line + title, copied verbatim from
# .git/factory/last-review.json — a dismissal never covers other findings,
# even in the same file):
#   [{"file":"apps/x.ts","line":42,"title":"<exact finding title>",
#     "reason":"upstream-origin","note":"optional"},
#    {"file":"apps/y.ts","line":7,"title":"<exact finding title>",
#     "reason":"false-positive","justification":"<why the finding is wrong,
#      verified against the real code — min 120 chars>"}]
#   * upstream-origin — accepted ONLY if the staged blob is byte-identical to
#     FACTORY_UPSTREAM_REF:<file> (the code verbatim is upstream's, merged
#     deliberately; upstream's design choices are not this commit's bugs).
#   * false-positive — accepted only with a >=120-char justification naming
#     the code-level reason. The PR merge gate (CI + Codex bound to HEAD) is
#     untouched and remains the hard backstop for anything dismissed here.
# Dismissals are single-use (consumed on success) and appended to the audit log.
#
# Escape hatches (all audited, all EXPLICIT — there is no ambient bypass;
# merge/cherry-pick/rebase continuations are gated like any other commit):
#   FACTORY_SKIP=1 FACTORY_SKIP_REASON="..."   skip the whole gate (upstream-
#     sync driver only: its PRs merge via the CI-only policy instead, and it
#     must set this for every commit/continuation it performs)
#   FACTORY_ALLOW_UNTRACKED=1                  permit untracked files to remain
#     (they are NOT in the commit; only set this when that is intentional)
set -uo pipefail

MODE="${1:-hook}"
case "$MODE" in hook|--prepare|--status) ;; *) echo "usage: precommit-gate.sh [hook|--prepare|--status]" >&2; exit 2;; esac

REPO_ROOT="$(git rev-parse --show-toplevel)" || exit 2
cd "$REPO_ROOT"
GIT_DIR="$(git rev-parse --git-dir)"
STATE_DIR="$GIT_DIR/factory"
mkdir -p "$STATE_DIR"

# Serialize gate runs per repo: a background --prepare racing a direct commit
# would otherwise interleave last-review.json / review-for and pair one tree's
# gate id with another tree's findings. Held (fd 9) for the whole run.
if [ "$MODE" != "--status" ]; then
  exec 9>"$STATE_DIR/gate.lock"
  flock 9 || { echo "factory-gate: cannot acquire gate lock" >&2; exit 2; }
fi
MARKER="$STATE_DIR/gate-ok"
REVIEW_JSON="$STATE_DIR/last-review.json"
REVIEW_FOR="$STATE_DIR/review-for"
DISMISSALS="$STATE_DIR/dismissals.json"

# FACTORY_GATE_FROM_HEAD may only be trusted when THIS process really is a
# HEAD materialization (running from the state dir) — an inherited env var
# must not let the worktree copy skip the HEAD re-exec for gate-file edits.
case "${BASH_SOURCE[0]}" in
  "$STATE_DIR"/*) ;;
  *) FACTORY_GATE_FROM_HEAD="" ;;
esac

# Gate objects must be REGULAR files wherever we materialize-and-execute (or
# source) them: `git show` of a symlink blob prints its target text, which
# would then run as shell.
head_is_regular() { # head_is_regular <path>
  local m
  m="$(git ls-tree HEAD -- "$1" 2>/dev/null | awk '{print $1}')"
  [ "$m" = "100644" ] || [ "$m" = "100755" ]
}

# ---- self-protection: a commit must not be judged by gate code/config it
# modifies itself. When gate files are staged-changed, re-exec the HEAD
# version of this script (the last landed, already-reviewed gate). ----
if [ -z "${FACTORY_GATE_FROM_HEAD:-}" ] && [ "$MODE" != "--status" ] \
   && git cat-file -e HEAD:scripts/factory/precommit-gate.sh 2>/dev/null \
   && ! git diff --cached --quiet -- scripts/factory .githooks 2>/dev/null; then
  head_is_regular scripts/factory/precommit-gate.sh \
    || { echo "factory-gate: HEAD gate is not a regular file; refusing" >&2; exit 2; }
  echo "factory-gate: this commit modifies gate files — evaluating with the HEAD gate" >&2
  git show HEAD:scripts/factory/precommit-gate.sh > "$STATE_DIR/gate-head.sh" \
    || { echo "factory-gate: cannot materialize the HEAD gate; refusing" >&2; exit 2; }
  FACTORY_GATE_FROM_HEAD=1 exec bash "$STATE_DIR/gate-head.sh" "$MODE"
fi

# ---- config (defaults + factory.conf) — loaded from HEAD, never from the
# tree being committed, so a staged config edit can't weaken this run.
# Bootstrap fallback to the worktree copy only while the gate isn't in HEAD
# yet (first installation commit). ----
FACTORY_STATIC_CHECKS=()
FACTORY_STATIC_CHECKS_PARALLEL=0
FACTORY_REVIEW_ARGS=()
FACTORY_AUTOREVIEW_BIN="$HOME/.claude/skills/autoreview/scripts/autoreview"
FACTORY_UPSTREAM_REF="upstream/main"
FACTORY_AUDIT_LOG="$HOME/.openclaw/audit/factory-precommit.jsonl"
# Config problems are RECORDED here and enforced after the --status /
# FACTORY_SKIP handling below: a broken landed config must refuse normal
# commits (never degrade to review-only) while the audited skip hatch stays
# usable to commit the repair itself.
CONF="$REPO_ROOT/scripts/factory/factory.conf"
CONF_ERR=""
if git cat-file -e HEAD:scripts/factory/factory.conf 2>/dev/null; then
  if ! head_is_regular scripts/factory/factory.conf; then
    CONF_ERR="HEAD factory.conf is not a regular file"
  elif ! git show HEAD:scripts/factory/factory.conf > "$STATE_DIR/factory.conf.head"; then
    CONF_ERR="cannot materialize HEAD factory.conf"
  # shellcheck source=factory.conf
  elif ! . "$STATE_DIR/factory.conf.head"; then
    CONF_ERR="failed to source HEAD factory.conf"
  fi
elif [ -f "$CONF" ]; then
  # shellcheck source=factory.conf
  . "$CONF" || CONF_ERR="failed to source $CONF"
fi
# An empty check list means a broken/neutered config — never a silent pass.
if [ -z "$CONF_ERR" ] && [ "${#FACTORY_STATIC_CHECKS[@]}" -eq 0 ]; then
  CONF_ERR="no static checks configured (bad or missing factory.conf)"
fi

command -v jq >/dev/null || { echo "factory-gate: jq is required" >&2; exit 2; }

now_ms() {
  local ns
  ns="$(date +%s%N 2>/dev/null || true)"
  if [[ "$ns" =~ ^[0-9]+$ ]]; then
    echo "$((ns / 1000000))"
    return 0
  fi
  if command -v node >/dev/null 2>&1; then
    node -e 'process.stdout.write(String(Date.now()))'
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    python3 -c 'import time; print(int(time.time() * 1000))'
    return 0
  fi
  echo "$(($(date +%s) * 1000))"
}

GATE_START_MS="$(now_ms)"
SCOPE_DONE_MS=""
STATIC_START_MS=""
STATIC_END_MS=""
REVIEW_START_MS=""
REVIEW_END_MS=""
STATIC_CHECKS_JSON="[]"

timings_json() {
  local end_ms scope_end static_start static_end review_start review_end
  end_ms="${1:-$(now_ms)}"
  scope_end="${SCOPE_DONE_MS:-$end_ms}"
  static_start="${STATIC_START_MS:-$scope_end}"
  static_end="${STATIC_END_MS:-$static_start}"
  review_start="${REVIEW_START_MS:-$static_end}"
  review_end="${REVIEW_END_MS:-$review_start}"
  jq -cn \
    --argjson total_ms "$((end_ms - GATE_START_MS))" \
    --argjson scope_ms "$((scope_end - GATE_START_MS))" \
    --argjson static_ms "$((static_end - static_start))" \
    --argjson review_ms "$((review_end - review_start))" \
    --argjson checks "$STATIC_CHECKS_JSON" \
    '{
      total_secs: ($total_ms / 1000),
      scope_secs: ($scope_ms / 1000),
      static_secs: ($static_ms / 1000),
      review_secs: ($review_ms / 1000),
      static_checks: $checks
    }'
}

audit() { # audit <verdict> <detail-json-object> — the gate's guarantees are
  # audit-backed, so an unwritable audit log FAILS the gate (never fail-open).
  local end_ms timings detail
  end_ms="$(now_ms)"
  timings="$(timings_json "$end_ms")" || {
    echo "factory-gate: FATAL — cannot build audit timing payload; refusing" >&2
    exit 2
  }
  detail="$(jq -cn --argjson detail "$2" --argjson timings "$timings" \
    '$detail + {timings:$timings}')" || {
      echo "factory-gate: FATAL — cannot build audit detail payload; refusing" >&2
      exit 2
    }
  mkdir -p "$(dirname "$FACTORY_AUDIT_LOG")" 2>/dev/null
  jq -cn --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg repo "$REPO_ROOT" \
    --arg branch "$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo '?')" \
    --arg mode "$MODE" --arg verdict "$1" --argjson detail "$detail" \
    '{ts:$ts,kind:"factory_precommit",repo:$repo,branch:$branch,mode:$mode,verdict:$verdict}+$detail' \
    >> "$FACTORY_AUDIT_LOG" || {
      echo "factory-gate: FATAL — cannot append to audit log $FACTORY_AUDIT_LOG; refusing" >&2
      exit 2
    }
}

apply_reviewer_override() {
  local override_file override_args default_args status
  override_file="$HOME/.openclaw/factory-reviewer-override.conf"
  [ -s "$override_file" ] || return 0
  override_args="$(<"$override_file")"
  [ -n "$override_args" ] || return 0
  default_args="${FACTORY_REVIEW_ARGS[*]}"
  status="rejected"
  if [[ "$override_args" =~ ^--reviewers\ [-A-Za-z0-9=.\ ]+$ ]] \
     && [ "$override_args" = "--reviewers claude --model claude=opus-4.8 --thinking claude=high" ]; then
    read -r -a FACTORY_REVIEW_ARGS <<< "$override_args"
    status="used"
  else
    echo "factory-gate: reviewer override rejected; using pinned default" >&2
  fi
  mkdir -p "$(dirname "$FACTORY_AUDIT_LOG")" 2>/dev/null
  jq -cn --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg repo "$REPO_ROOT" \
    --arg status "$status" --arg args "${FACTORY_REVIEW_ARGS[*]}" \
    --arg requested "$override_args" --arg default_args "$default_args" \
    '{ts:$ts,kind:"reviewer-override",repo:$repo,status:$status,args:$args,requested:$requested,default_args:$default_args}' \
    >> "$FACTORY_AUDIT_LOG" || {
      echo "factory-gate: FATAL — cannot audit reviewer override; refusing" >&2
      exit 2
    }
}

refuse() { # refuse <short> <verdict> <detail-json>
  echo "" >&2
  echo "factory-gate: COMMIT REFUSED — $1" >&2
  audit "${2:-refused}" "${3:-{\}}"
  exit 1
}

head_sha() { git rev-parse -q --verify HEAD 2>/dev/null || git hash-object -t tree /dev/null; }

# Self-heal the out-of-tree hook installation after an installed hook reaches
# the gate: updated shims in HEAD propagate to the installed dir. This cannot
# bootstrap a brand-new hook for a sequencer commit that runs before any
# existing installed hook fires; upgraded clones must run install-hooks.sh once
# for that first-use case.
hooks_path="$(git config core.hooksPath 2>/dev/null || true)"
if [[ "$MODE" != "--status" && -n "$hooks_path" && -d "$hooks_path" && "$hooks_path" = /* && "$hooks_path" != "$REPO_ROOT"/* ]]; then
  for h in pre-commit pre-merge-commit prepare-commit-msg; do
    if git cat-file -e "HEAD:.githooks/$h" 2>/dev/null && head_is_regular ".githooks/$h"; then
      if ! git show "HEAD:.githooks/$h" 2>/dev/null | cmp -s - "$hooks_path/$h" 2>/dev/null; then
        # Atomic + fail-closed, like the shims' own HEAD materialization: a
        # truncated live hook or a silently-failed refresh must never persist.
        tmp_hook="$hooks_path/.$h.$$"
        if git show "HEAD:.githooks/$h" > "$tmp_hook" 2>/dev/null && [ -s "$tmp_hook" ] \
           && chmod +x "$tmp_hook" && mv -f "$tmp_hook" "$hooks_path/$h"; then
          echo "factory-gate: refreshed installed hook '$h' from HEAD" >&2
        else
          rm -f "$tmp_hook"
          echo "factory-gate: FAILED to refresh installed hook '$h'; refusing (stale installed hooks must not persist silently)" >&2
          exit 2
        fi
      fi
    fi
  done
fi

# Dismissals are single-use and bound to the review that prompted them: any
# leftover file on a pass/skip path is stale and must not survive to cover a
# future finding that happens to share file/line/title.
discard_stale_dismissals() {
  [ -f "$DISMISSALS" ] || return 0
  mv "$DISMISSALS" "$STATE_DIR/dismissals-stale-$(date +%s).json"
  echo "factory-gate: discarded stale dismissals (this pass/skip did not consume them)" >&2
}

# ---- status mode ----
if [ "$MODE" = "--status" ]; then
  echo "marker:      $( [ -f "$MARKER" ] && cat "$MARKER" || echo '(none)')"
  echo "current:     $(head_sha) $(git write-tree 2>/dev/null || echo '(unmerged index)')"
  echo "dismissals:  $( [ -f "$DISMISSALS" ] && jq -c . "$DISMISSALS" || echo '(none)')"
  echo "last review: $( [ -f "$REVIEW_JSON" ] && jq -c '{findings:(.findings|length),overall_correctness}' "$REVIEW_JSON" || echo '(none)')"
  exit 0
fi

# ---- audited escape hatches ----
if [ "${FACTORY_SKIP:-0}" = "1" ]; then
  reason="${FACTORY_SKIP_REASON:-}"
  [ -n "$reason" ] || refuse "FACTORY_SKIP=1 requires FACTORY_SKIP_REASON" skip-missing-reason '{}'
  discard_stale_dismissals
  echo "factory-gate: SKIPPED — $reason" >&2
  audit skipped "$(jq -cn --arg r "$reason" '{skip_reason:$r}')"
  exit 0
fi

# Broken config refuses everything past this point (the skip hatch above
# stays usable to land the config repair, audited).
[ -z "$CONF_ERR" ] || refuse "gate config unusable: $CONF_ERR.
  Fix scripts/factory/factory.conf; if the broken config is already in HEAD,
  commit the repair with the audited escape hatch:
    FACTORY_SKIP=1 FACTORY_SKIP_REASON=\"config repair: <what broke>\" git commit ..." \
  config-broken "$(jq -cn --arg e "$CONF_ERR" '{conf_error:$e}')"
# ---- scope guard: the checked tree must BE the committed tree ----
staged="$(git diff --cached --name-only)"
if [ -z "$staged" ]; then
  # Nothing staged: let git itself decide (it refuses empty commits unless
  # --allow-empty, which is a deliberate caller choice).
  audit pass "$(jq -cn '{note:"empty staged diff"}')"
  exit 0
fi
unstaged="$(git diff --name-only)"
if [ -n "$unstaged" ]; then
  refuse "the working tree differs from the index for:
$(printf '%s\n' "$unstaged" | sed 's/^/    /')
  The gate validates exactly what will be committed. Stage everything
  (git add -A) or stash the unrelated edits, then retry." scope-unstaged \
  "$(jq -cn --arg f "$unstaged" '{unstaged:($f|split("\n"))}')"
fi
untracked="$(git ls-files --others --exclude-standard)"
if [ -n "$untracked" ] && [ "${FACTORY_ALLOW_UNTRACKED:-0}" != "1" ]; then
  refuse "untracked (non-ignored) files exist:
$(printf '%s\n' "$untracked" | sed 's/^/    /')
  They would be silently missing from the commit (classic forgotten-git-add
  breakage). git add them, .gitignore them, or delete them. If leaving them
  out is intentional, retry with FACTORY_ALLOW_UNTRACKED=1 (audited)." \
  scope-untracked "$(jq -cn --arg f "$untracked" '{untracked:($f|split("\n"))}')"
fi
[ -n "$untracked" ] && audit allow-untracked "$(jq -cn --arg f "$untracked" '{untracked:($f|split("\n"))}')"

# A commit may MODIFY gate files (it is then judged by the HEAD gate), but it
# may never REMOVE them: a tree without the gate would land judged by the old
# gate and leave the factory hookless. Applies to every load-bearing file.
for gf in scripts/factory/precommit-gate.sh scripts/factory/factory.conf \
          scripts/factory/install-hooks.sh .githooks/pre-commit \
          .githooks/pre-merge-commit .githooks/prepare-commit-msg; do
  git cat-file -e "HEAD:$gf" 2>/dev/null || continue
  if ! git rev-parse -q --verify ":$gf" >/dev/null 2>&1; then
    refuse "this commit removes the gate file '$gf'.
  The factory gate cannot be removed by a gated commit; restore the file
  (git checkout HEAD -- $gf) or change the gate deliberately in a reviewed,
  non-removing commit." gate-file-removed "$(jq -cn --arg f "$gf" '{removed:$f}')"
  fi
  smode="$(git ls-files --stage -- "$gf" 2>/dev/null | awk '{print $1}')"
  case "$smode" in
    100644|100755) ;;
    *) refuse "gate file '$gf' is staged as a non-regular object (mode ${smode:-none}).
  Gate files must stay regular files (a symlink here would later be executed
  as its target text)." gate-file-mode "$(jq -cn --arg f "$gf" --arg m "${smode:-none}" '{file:$f,mode:$m}')";;
  esac
done

HEAD_SHA="$(head_sha)"
TREE_SHA="$(git write-tree)" || refuse "git write-tree failed (unmerged index?)" scope-write-tree '{}'
GATE_ID="$HEAD_SHA $TREE_SHA"
SCOPE_DONE_MS="$(now_ms)"

append_static_check_result() { # append_static_check_result <cmd> <rc> <elapsed-ms>
  STATIC_CHECKS_JSON="$(jq -c --arg cmd "$1" --argjson rc "$2" --argjson elapsed_ms "$3" \
    '. + [{cmd:$cmd, rc:$rc, secs:($elapsed_ms / 1000)}]' <<<"$STATIC_CHECKS_JSON")"
}

run_static_checks_sequential() {
  local cmd check_start check_end check_rc
  for cmd in "${FACTORY_STATIC_CHECKS[@]}"; do
    echo "factory-gate: static check: $cmd" >&2
    check_start="$(now_ms)"
    if bash -c "$cmd" 1>&2; then
      check_rc=0
    else
      check_rc=$?
    fi
    check_end="$(now_ms)"
    append_static_check_result "$cmd" "$check_rc" "$((check_end - check_start))"
    if [ "$check_rc" -ne 0 ]; then
      STATIC_END_MS="$(now_ms)"
      refuse "static check failed: $cmd
  Fix the errors (for formatting, run the repo formatter's --fix mode), then
  re-stage with git add -A and retry." static-failed \
      "$(jq -cn --arg c "$cmd" '{failed_check:$c}')"
    fi
  done
}

run_static_check_capture() { # run_static_check_capture <cmd> <log> <meta>
  local cmd log meta check_start check_end check_rc
  cmd="$1"
  log="$2"
  meta="$3"
  check_start="$(now_ms)"
  if bash -c "$cmd" >"$log" 2>&1; then
    check_rc=0
  else
    check_rc=$?
  fi
  check_end="$(now_ms)"
  jq -cn --arg cmd "$cmd" --argjson rc "$check_rc" --argjson elapsed_ms "$((check_end - check_start))" \
    '{cmd:$cmd, rc:$rc, secs:($elapsed_ms / 1000)}' > "$meta"
  exit "$check_rc"
}

run_static_checks_parallel() {
  local run_dir failed_cmd failed_rc i cmd log meta rc
  local -a pids
  run_dir="$STATE_DIR/static-checks.$$"
  rm -rf "$run_dir"
  mkdir -p "$run_dir"
  echo "factory-gate: static checks: running ${#FACTORY_STATIC_CHECKS[@]} checks in parallel" >&2
  for i in "${!FACTORY_STATIC_CHECKS[@]}"; do
    cmd="${FACTORY_STATIC_CHECKS[$i]}"
    echo "factory-gate: static check: $cmd" >&2
    run_static_check_capture "$cmd" "$run_dir/$i.log" "$run_dir/$i.json" &
    pids[$i]=$!
  done

  failed_cmd=""
  failed_rc=0
  for i in "${!pids[@]}"; do
    wait "${pids[$i]}"
    rc=$?
    if [ "$rc" -ne 0 ] && [ -z "$failed_cmd" ]; then
      failed_cmd="${FACTORY_STATIC_CHECKS[$i]}"
      failed_rc="$rc"
    fi
  done

  for i in "${!FACTORY_STATIC_CHECKS[@]}"; do
    log="$run_dir/$i.log"
    meta="$run_dir/$i.json"
    [ ! -s "$log" ] || cat "$log" >&2
    if [ -s "$meta" ]; then
      STATIC_CHECKS_JSON="$(jq -cs '.[0] + [.[1]]' <(printf '%s\n' "$STATIC_CHECKS_JSON") "$meta")"
    else
      append_static_check_result "${FACTORY_STATIC_CHECKS[$i]}" 127 0
      [ -n "$failed_cmd" ] || failed_cmd="${FACTORY_STATIC_CHECKS[$i]}"
      [ "$failed_rc" -ne 0 ] || failed_rc=127
    fi
  done
  rm -rf "$run_dir"

  if [ -n "$failed_cmd" ]; then
    STATIC_END_MS="$(now_ms)"
    refuse "static check failed: $failed_cmd
  Fix the errors (for formatting, run the repo formatter's --fix mode), then
  re-stage with git add -A and retry." static-failed \
      "$(jq -cn --arg c "$failed_cmd" --argjson rc "$failed_rc" '{failed_check:$c,rc:$rc}')"
  fi
}

# ---- cached pass ----
if [ -f "$MARKER" ] && [ "$(cat "$MARKER")" = "$GATE_ID" ]; then
  discard_stale_dismissals
  echo "factory-gate: PASS (cached for this exact staged tree)" >&2
  audit pass "$(jq -cn --arg g "$GATE_ID" '{gate_id:$g,cached:true}')"
  exit 0
fi
rm -f "$MARKER"

# ---- cached review RESULT (same staged tree, e.g. a retry with dismissals):
# the findings set is frozen per (HEAD, staged-tree), so dismissals validate
# against exactly what the model saw, and no re-review is paid. ----
if [ -f "$REVIEW_FOR" ] && [ "$(cat "$REVIEW_FOR" 2>/dev/null)" = "$GATE_ID" ] \
   && jq -e '.findings | length > 0' "$REVIEW_JSON" >/dev/null 2>&1; then
  echo "factory-gate: reusing the review result for this exact staged tree (static checks already passed for it)" >&2
else
  # ---- static checks ----
  STATIC_START_MS="$(now_ms)"
  if [ "${FACTORY_STATIC_CHECKS_PARALLEL:-0}" = "1" ] && [ "${#FACTORY_STATIC_CHECKS[@]}" -gt 1 ]; then
    run_static_checks_parallel
  else
    run_static_checks_sequential
  fi
  STATIC_END_MS="$(now_ms)"

  # ---- autoreview over exactly the staged diff ----
  [ -x "$FACTORY_AUTOREVIEW_BIN" ] || refuse "autoreview helper not found at $FACTORY_AUTOREVIEW_BIN" review-infra '{}'
  apply_reviewer_override
  echo "factory-gate: autoreview (this can take several minutes; pre-warm next time with scripts/factory/precommit-gate.sh --prepare)" >&2
  rm -f "$REVIEW_JSON" "$REVIEW_FOR"
  REVIEW_START_MS="$(now_ms)"
  "$FACTORY_AUTOREVIEW_BIN" --mode local "${FACTORY_REVIEW_ARGS[@]}" --json-output "$REVIEW_JSON" 1>&2
  review_rc=$?
  REVIEW_END_MS="$(now_ms)"

  if [ "$review_rc" -eq 0 ]; then
    # Never cache a PASS on exit code alone: require a parseable report whose
    # findings are an ACTUAL empty array with a clean verdict (jq's
    # `null|length` is 0, so `{}`-style output must not slip through).
    if ! jq -e '(.findings | type == "array") and (.findings | length == 0) and (.overall_correctness == "patch is correct")' "$REVIEW_JSON" >/dev/null 2>&1; then
      refuse "review exited 0 but $REVIEW_JSON is missing, unparseable, or non-empty.
  Check FACTORY_AUTOREVIEW_BIN — refusing to cache a PASS without a clean report." review-infra \
      "$(jq -cn '{review_rc:0,report:"invalid"}')"
    fi
    discard_stale_dismissals
    echo "$GATE_ID" > "$MARKER"
    echo "factory-gate: PASS (static checks + clean review)" >&2
    audit pass "$(jq -cn --arg g "$GATE_ID" '{gate_id:$g,review:"clean"}')"
    exit 0
  fi

  # Review returned findings (or died). Findings without a parsable report are
  # an infra failure — never a bypass.
  if ! jq -e '.findings' "$REVIEW_JSON" >/dev/null 2>&1; then
    refuse "autoreview did not produce a findings report (engine failure?).
  Retry the commit; if it persists, run the helper by hand:
    $FACTORY_AUTOREVIEW_BIN --mode local ${FACTORY_REVIEW_ARGS[*]}" review-infra \
    "$(jq -cn --arg rc "$review_rc" '{review_rc:($rc|tonumber)}')"
  fi
  if [ "$(jq '.findings | length' "$REVIEW_JSON")" -eq 0 ]; then
    refuse "review exited $review_rc with zero findings (overall verdict: $(jq -r '.overall_correctness // "?"' "$REVIEW_JSON")).
  Inspect $REVIEW_JSON and re-run." review-infra "$(jq -cn --arg rc "$review_rc" '{review_rc:($rc|tonumber),findings:0}')"
  fi
  echo "$GATE_ID" > "$REVIEW_FOR"
fi

# The helper's validated schema is {file_path, line}; the alternate
# {absolute_file_path, line_range.start} shape is accepted defensively (it
# cannot pass the helper's validator today, but parsing it costs nothing and
# an unknown shape still fails closed as null:null → unmatchable → refused).
findings="$(jq -c --arg root "$REPO_ROOT/" \
  '[.findings[] | {file:((.code_location.file_path // .code_location.absolute_file_path) | ltrimstr($root) | ltrimstr("./")), line:(.code_location.line // .code_location.line_range.start), title, priority, category}]' "$REVIEW_JSON")"
n_findings="$(jq 'length' <<<"$findings")"

# ---- dismissals: refuse-by-default, dismiss-by-exception ----
valid_dismissals="[]"
if [ -f "$DISMISSALS" ] && jq -e 'type=="array"' "$DISMISSALS" >/dev/null 2>&1; then
  while IFS= read -r d; do
    file="$(jq -r '.file // ""' <<<"$d")"
    reason="$(jq -r '.reason // ""' <<<"$d")"
    file="${file#"$REPO_ROOT"/}"; file="${file#./}"
    ok=0; why=""
    case "$reason" in
      upstream-origin)
        staged_blob="$(git rev-parse -q --verify ":$file" 2>/dev/null || true)"
        upstream_blob="$(git rev-parse -q --verify "$FACTORY_UPSTREAM_REF:$file" 2>/dev/null || true)"
        if [ -n "$staged_blob" ] && [ "$staged_blob" = "$upstream_blob" ]; then ok=1
        else why="staged $file is NOT byte-identical to $FACTORY_UPSTREAM_REF:$file — the code is (at least partly) ours, so the finding must be fixed or dismissed as false-positive with a justification"; fi
        ;;
      false-positive)
        jlen="$(jq -r '(.justification // "") | length' <<<"$d")"
        if [ "$jlen" -ge 120 ]; then ok=1
        else why="false-positive dismissal for $file needs a justification of >=120 chars (has $jlen) naming the code-level reason the finding is wrong"; fi
        ;;
      *) why="unknown dismissal reason '$reason' for $file (use upstream-origin | false-positive)";;
    esac
    if [ "$ok" -eq 1 ]; then
      valid_dismissals="$(jq -c --argjson d "$d" --arg f "$file" \
        '. + [($d + {file:$f, line:(($d.line // -1) | (tonumber? // -1))})]' <<<"$valid_dismissals")"
    else
      echo "factory-gate: dismissal REJECTED: $why" >&2
    fi
  done < <(jq -c '.[]' "$DISMISSALS")
fi

# A dismissal covers exactly ONE finding: file + line + title must all match
# (so dismissing one finding never hides another in the same file).
uncovered="$(jq -c --argjson dis "$valid_dismissals" \
  '[.[] | . as $f | select(([$dis[] | select(.file == $f.file and .line == $f.line and .title == $f.title)] | length) == 0)]' <<<"$findings")"

if [ "$(jq 'length' <<<"$uncovered")" -eq 0 ]; then
  audit pass-with-dismissals "$(jq -cn --arg g "$GATE_ID" --argjson f "$findings" --argjson d "$valid_dismissals" \
    '{gate_id:$g,findings:$f,dismissals:$d}')"
  mv "$DISMISSALS" "$STATE_DIR/dismissals-used-$(date +%s).json"
  echo "$GATE_ID" > "$MARKER"
  echo "factory-gate: PASS — all $n_findings finding(s) covered by valid, audited dismissals" >&2
  exit 0
fi

echo "" >&2
echo "factory-gate: $n_findings review finding(s); $(jq 'length' <<<"$uncovered") not covered by a valid dismissal:" >&2
jq -r '.[] | "  [\(.priority)] \(.file):\(.line) — \(.title)"' <<<"$uncovered" >&2
cat >&2 <<EOF

  Next steps (in order of preference):
  1. FIX the findings, git add -A, and retry (full report: $REVIEW_JSON).
  2. If a finding is in code that is verbatim upstream's ($FACTORY_UPSTREAM_REF)
     or is demonstrably wrong, write $DISMISSALS —
     schema in the header of scripts/factory/precommit-gate.sh. Each entry must
     copy the finding's exact file, line, and title from the report above.
     Dismissals are validated, single-use, and audit-logged; the PR merge gate
     (CI + Codex on HEAD) still applies regardless.
EOF
refuse "review findings outstanding" review-findings \
  "$(jq -cn --arg g "$GATE_ID" --argjson f "$findings" --argjson d "$valid_dismissals" '{gate_id:$g,findings:$f,valid_dismissals:$d}')"
