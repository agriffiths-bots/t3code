---
name: linear-issue-fix
description: The set factory workflow for taking a Linear issue (team "Adam", keys ADA-*) from report to merged fix in agriffiths-bots/t3code — pick up, reproduce with a gpt-5.4-mini T3 subthread driving Playwright on an ephemeral server, fix, verify with a fresh subthread, commit through the factory gate, PR, Codex review + CI babysit, wizzo-approve merge, close the issue. Use when asked to handle/fix/pick up a Linear issue or run the issue loop.
---

# Linear issue → merged fix (the factory loop)

Linear workspace `adamfg`, team **Adam** (issues `ADA-*`), tools
`mcp__claude_ai_Linear__*`. Statuses: Todo → In Progress → In Review → Done.

Precondition for ANY GitHub write: `gh auth status -h github.com` must show
`wizzoapp[bot]`. If it doesn't, STOP.

## 1. Pick up

- `get_issue` (full description; `extract_images` for screenshots;
  `list_comments` for context).
- `save_issue` → state "In Progress", and `save_comment`: one line on what you
  are about to do. Keep the issue as the running log — every phase below ends
  with a short comment.
- Branch off `main`: `wizzo/ada-<n>-<short-slug>`.

## 2. Reproduce FIRST (never fix an unreproduced bug)

Spawn a repro agent — a T3 subthread on **gpt-5.4-mini** with
`t3_spawn_subagent` (plain `model: "gpt-5.4-mini"`; routing is handled by the
registry). If T3 subthreads are unavailable or misbehave, run the same steps
yourself or via a local subagent instead — the procedure is identical.

Give the agent this brief, verbatim structure:

> Follow the repo skill `.claude/skills/t3-e2e-testing/SKILL.md`.
> Instance name: `ada-<n>`. Reproduce this report against the ephemeral T3
> web UI with Playwright, following the steps a user would take:
> <issue title, description, expected vs actual, screenshots' content>
> Reply with REPRODUCED or NOT REPRODUCED, the exact steps you drove,
> screenshot paths, and any console/server-log errors. Tear the instance down.

- REPRODUCED → comment the evidence on the issue, continue.
- NOT REPRODUCED → comment the evidence + what info is missing, move the
  issue back to "Todo", and stop (do not guess-fix).

## 3. Fix

Minimal, in-scope change; match surrounding style. Add/adjust unit tests where
the bug class allows. `vp run typecheck`, `vp check --fix`,
`vp test run <affected>` locally.

## 4. Verify the fix like a user

Rebuild so the ephemeral server runs your edits: `vp run build:desktop`.
Spawn a FRESH gpt-5.4-mini subthread (same brief, instance `ada-<n>-verify`,
plus one regression check of the surrounding surface). Require FIXED with
evidence. STILL BROKEN → back to step 3.

## 5. Commit through the factory gate

```bash
git add -A
scripts/factory/precommit-gate.sh --prepare   # static checks + review panel; cached on PASS
git commit -m "fix(...): ... (ADA-<n>)"        # instant when pre-warmed
```

On findings: fix them, or use the audited dismissal mechanism
(`scripts/factory/README.md`) — never `--no-verify`.

## 6. PR → review → merge

```bash
git push -u origin wizzo/ada-<n>-<slug>
gh api repos/agriffiths-bots/t3code/pulls -X POST -f title="fix: ... (ADA-<n>)" \
  -f head=wizzo/ada-<n>-<slug> -f base=main -f body="Fixes ADA-<n>. <summary + repro/verify evidence>"
gh pr comment <pr#> --repo agriffiths-bots/t3code --body "@codex review"
```

Do NOT arm `gh pr merge --auto` — merging is done exclusively by
`wizzo-approve --apply` after its gate passes.

- Move the issue to "In Review" with the PR link.
- Babysit to green (the machine-level `babysit-pr` skill if available;
  otherwise inline): poll `gh pr checks <pr#>` and the review threads with
  bounded foreground `sleep` loops — background waits are unreliable here.
  Address every Codex finding: fix, or reply + resolve the thread with a
  code-verified rationale (branch protection requires resolved conversations).
- Land via the gate: `wizzo-approve agriffiths-bots/t3code <pr#> --apply`
  (never `gh pr review`/`merge` by hand). `approved+merge-failed` is usually
  an auto-merge race — check `gh pr view <pr#> --json state`.

## 7. Close the loop

`save_issue` → "Done", final `save_comment`: merged PR link, one-paragraph
summary, verification evidence. If anything was dismissed at the commit gate,
say so explicitly.

## Failure discipline

- Blocked on something only Adam can do (auth, product decision) → comment on
  the issue with exactly what is needed, leave it "In Progress", stop.
- Never weaken CI, the commit gate, or wizzo-approve to force a merge.
- Ephemeral instances are per-issue (`ada-<n>`); sweep strays with
  `.claude/skills/t3-test-server/scripts/t3-down.sh --all` when done.
