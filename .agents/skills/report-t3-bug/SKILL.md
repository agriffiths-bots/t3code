---
name: report-t3-bug
description: File or update Linear issues for T3 Code harness failures and vague T3 complaints. Use when a T3 MCP/browser/preview/scheduled-task/subagent/tool call fails, when T3 automation behaves incorrectly, or when the user asks to report or "moan" about a T3 bug.
---

# Report T3 Bug

Use this skill to turn T3 Code failures into actionable Linear issues in the
`adamfg` workspace, team **Adam** (`ADA-*`). Prefer a useful issue over a long
chat diagnosis; include enough evidence for the issue loop to reproduce it.

## Workflow

1. Capture the symptom:
   - failing T3 surface: MCP tool, preview/browser, scheduled task, subagent,
     thread resume, WebSocket, or UI flow
   - exact action and arguments, with secrets redacted
   - expected result, actual result, error text, and whether it is reproducible
   - relevant thread/session IDs, branch, commit, and local command output
2. Run `scripts/collect-diagnostics.sh` from this skill when local diagnostics
   are useful. Pass `--tool <name>` and, if present, `--trace-file <path>`.
3. Deduplicate before filing:
   - Use the available Linear issue listing/search tool for team `Adam`.
   - Search by the failing tool name, thread/session ID, and a concise symptom.
   - If an open issue already covers it, append a comment with the new evidence
     instead of creating another issue.
4. File or update the Linear issue:
   - Team: `Adam`
   - State: `Todo` unless the user explicitly asked you to begin fixing it
   - Priority: `2` when it blocks automation or loses work, otherwise `3`
   - Labels: use existing `bug`, `t3`, or `automation` labels when available;
     do not fail if labels are absent
   - Title: `[T3] <specific symptom>`
5. After filing, report the issue key and one-line next action. If the user
   asked to continue fixing, use the repo `linear-issue-fix` skill next.

## Issue Body

Use this structure:

```markdown
## Summary

<one paragraph>

## Trigger

- Surface:
- Tool/flow:
- Arguments or user action:
- Thread/session IDs:

## Expected

<what should have happened>

## Actual

<what happened, including exact error text>

## Diagnostics

<redacted output from collect-diagnostics.sh or equivalent>

## Reproduction

<steps if known, otherwise "Unknown; captured from live failure">
```

For vague complaints ("T3 is broken", "moan about this"), infer the likely
issue from current context and file it. Ask at most one clarifying question only
when there is no failing surface, no observable symptom, and no reasonable
diagnostic source.

## If Linear Is Unavailable

Create the same Markdown report under `/tmp/report-t3-bug-<timestamp>.md`,
tell the user exactly what blocked Linear filing, and leave the task in a state
where the issue can be created by rerunning this skill once Linear is connected.
