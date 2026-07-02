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
   staging is rejected with instructions).
2. **Static checks** — `vp run typecheck` + `vp check` (from
   `factory.conf`; they never modify files).
3. **Autoreview panel** — Codex `gpt-5.5` (high) + Claude `claude-opus-4-8`
   (max) over exactly the staged diff (`--mode local`). Clean panel ⇒ commit
   passes. Findings ⇒ commit refused.

A PASS is cached against the `(HEAD, staged-tree)` pair, so retries and the
`--prepare` pre-warm are instant. Any staged change invalidates the cache.

## Recommended agent flow

```bash
git add -A
scripts/factory/precommit-gate.sh --prepare   # run in background if you like
# ...when it reports PASS:
git commit -m "..."                            # instant (cached gate result)
```

Running `git commit` directly also works — the hook just runs the whole gate
inline (typecheck + panel review can take several minutes; give the command a
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
  covered via `.githooks/pre-merge-commit`. Note one git-design limit:
  cherry-pick/rebase sequencer commits run no pre-commit hook at all — those
  flows are covered by the PR-level gate (CI + Codex via wizzo-approve).

Audit trail: `~/.openclaw/audit/factory-precommit.jsonl` (every pass, refusal,
dismissal, and skip, with finding details).

## Setup (once per clone)

```bash
scripts/factory/install-hooks.sh
```

This copies the hook shims OUT of the worktree (to
`~/.openclaw/factory-hooks/<repo>`) and points `core.hooksPath` there; the
shims themselves run the HEAD version of the gate. So neither the hook nor
the gate/config judging a commit can be altered by that same commit.

Linux-only tooling (flock, /proc, setsid) — the factory runs on the Linux VPS.
