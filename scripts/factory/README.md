# Software-factory commit gate

Fork-local (agriffiths-bots) commit-time quality gate. The goal: an agent
should think about the _problem_, not the _process_ — `git commit` either
passes because the work is verifiably clean, or it refuses with exact
remediation steps.

## What runs on every commit

`.githooks/pre-commit` → `scripts/factory/precommit-gate.sh`:

1. **Scope guard** — the index must equal the working tree, and no untracked
   non-ignored files may exist. This makes "what we check" identical to "what
   gets committed" for every staging style (`git add -A`, `commit -a`, partial
   staging is rejected with instructions). Pathspec/`--only` commits that
   would leave other staged files out of the commit are also refused: Git
   presents the hook with a temporary commit index, so the excluded staged
   files show up as unstaged differences and trip the same scope guard.
2. **Static checks** — `vp run --cache typecheck` + `vp check` (from
   `factory.conf`; they never modify files). The repo marks these checks as
   parallel-safe, so the gate overlaps them and replays logs in configured
   order.
3. **Autoreview** — Codex `gpt-5.6-sol` (high) over exactly the staged diff
   (`--mode local`). Clean review ⇒ commit passes. Findings ⇒ commit refused.

A PASS is cached against the `(HEAD, staged-tree)` pair, so retries and the
`--prepare` pre-warm are instant. Any staged change invalidates the cache.

For a provider outage, the overseer may place
`~/.openclaw/factory-reviewer-override.conf` on Adam's instruction (and remove
it afterward). For example, Codex weekly exhaustion can use
`--reviewers claude --model claude=opus-4.8 --thinking claude=high`. The gate
whitelist-validates the file and writes every use or rejection to the audit
JSONL as `kind:"reviewer-override"`; malformed values fall back to the pinned
default. The verified Claude tuple above is the only accepted override.

## Recommended agent flow

```bash
git add -A
scripts/factory/precommit-gate.sh --prepare   # run in background if you like
# ...when it reports PASS:
git commit -m "..."                            # instant (cached gate result)
```

Running `git commit` directly also works — the hook just runs the whole gate
inline (typecheck + review can take several minutes; give the command a
generous timeout).

## Disagreeing with findings

Write `.git/factory/dismissals.json` (schema in the header of
`precommit-gate.sh`). Two reasons exist, both audited and single-use:

- `upstream-origin` — verified **structurally**: the staged file must be
  byte-identical to `upstream/main`'s copy. Use for pingdotgg code we merge
  deliberately; upstream's design decisions are not this commit's bugs.
- `false-positive` — requires a ≥120-char justification naming the code-level
  reason the finding is wrong. Use sparingly: the PR merge gate (CI + Codex
  bound to HEAD via `wizzo-approve`) still applies to everything dismissed
  here, so a wrong dismissal only moves the failure later.

## Escape hatches (audited, never silent)

- `FACTORY_SKIP=1 FACTORY_SKIP_REASON="..."` — skips the gate; used ONLY by
  the nightly upstream-sync driver (whose PRs merge via the CI-only policy
  dir), which must set it for every commit and sequencer continuation it
  performs. There is no ambient bypass in the gate itself; merge commits are
  covered via `pre-merge-commit`. New installs also include a
  `prepare-commit-msg` shim for cherry-pick/revert commits (the only hook git
  runs for them). Existing clones installed before that shim existed must run
  `scripts/factory/install-hooks.sh` once after upgrading; until then, their
  first sequencer-created commit can only be caught by the PR-level gate
  (CI + Codex via wizzo-approve). Rebase continuations remain a git-design
  gap covered by the same PR-level gate.

Audit trail: `~/.openclaw/audit/factory-precommit.jsonl` (every pass, refusal,
dismissal, and skip, with finding details and phase timings).

## Setup (once per clone)

```bash
scripts/factory/install-hooks.sh
```

This copies the hook shims OUT of the worktree (to
`~/.openclaw/factory-hooks/<repo>`) and points `core.hooksPath` there; the
shims themselves run the HEAD version of the gate. So neither the hook nor
the gate/config judging a commit can be altered by that same commit.

Linux-only tooling (flock, /proc, setsid) — the factory runs on the Linux VPS.
