# dseg slice 7 round 9 — peer remote-parent regression

## Intent

`commandAudienceGuard` rejects every authenticated peer remote spawn: the peer path builds a
`session`/`factory` authority, and `requireRemoteParentAllowed` unconditionally refuses that pair.
Permit exactly the case the handler has already proven safe — a peer token whose
`sourceEnvironmentId` equals the command's `parentEnvironmentId` — without relaxing the factory
rejection for anything else.

## Confirmed trace (regression, not a gap)

1. `subagent/handlers.ts:986` — `parentEnvironmentId = invocation.sourceEnvironmentId` (always defined here).
2. `:992-999` — handler proves `remoteParentEnvironmentId` matches the authenticated caller backend.
3. `:1002-1005` — `sessionDispatchAuthority({ subject: "mcp-peer:…", audienceCeiling: "factory" })` (hardcoded ceiling).
4. `dispatchParentSet` → `thread.parent.set` with `parentEnvironmentId` set.
5. `commandAudienceGuard.ts:861-865` — `parentEnvironmentId !== undefined` → `requireRemoteParentAllowed`.
6. `:478-484` — rejects `session` + `factory` ceiling. Deterministic, 100%.

On `origin/main` the same handler branch dispatched with **no** authority argument and no engine-side
guard; `commandAudienceGuard.ts` does not exist there. Hence: this diff introduced the breakage.

## Approach (chosen) and the rejected alternative

**Chosen:** add an optional `peerSourceEnvironmentId` to the existing `session` authority variant,
plus a `peerSessionDispatchAuthority` constructor used only by the peer spawn path. The guard permits
a remote parent iff that field is present **and equal to** `command.parentEnvironmentId`.

**Rejected:** a new `kind: "peer"` union member. Every site that switches on `authority.kind`
(`canAccessAudience`, `createdProjectAudience`, `requireThreadAudience`, the engine, the normalizer)
would need a new arm, materially growing a round-8 diff and creating fresh fail-open surface in
branches unrelated to this bug. An optional field leaves every one of those sites treating the peer
authority exactly as today's factory session — the behaviour change is confined to the single call
site that is actually wrong.

## Blast radius

- `commandAudienceGuard.ts` — authority union (additive optional field), new constructor,
  `requireRemoteParentAllowed` signature gains `parentEnvironmentId`. All other guard branches
  unchanged; a session without the field behaves exactly as before.
- `subagent/handlers.ts:1002` — peer spawn authority constructor swap. The same authority is reused by
  the `onError` child-delete cleanup, which routes through `canAccessAudience` (unaffected by an
  optional field) and must keep authorizing the delete.
- No persistence, projection, event-schema, migration, or client change: authority is dispatch-time
  only and is never serialized into an event.
- Non-peer callers of `sessionDispatchAuthority` (`ws.ts`, `http.ts`, `thread/handlers.ts`) are
  untouched and keep the strict rejection.

## Edge-case matrix → named red-first test

All at the **guard** layer in `OrchestrationCommandAudienceGuard.test.ts`. Handler-level tests cannot
substitute: `handlers.test.ts:693-698` stubs the engine, drops the authority argument, and never runs
the guard — which is exactly what hid this for eight rounds.

| case                                             | expectation                                                   | test                                                                          |
| ------------------------------------------------ | ------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| peer authority, env id matches (see round 11)    | permitted **iff ceiling is not `factory`**                    | `permits a remote parent for a peer session whose source environment matches` |
| peer authority, env id mismatched                | rejected                                                      | `rejects a remote parent whose environment does not match the peer session`   |
| plain factory session, `parentEnvironmentId` set | rejected (unchanged)                                          | `rejects a remote parent for a non-peer factory session`                      |
| `audience-bound-system` + factory audience       | rejected (unchanged)                                          | `rejects a remote parent for a factory audience-bound system authority`       |
| peer authority, **no** `parentEnvironmentId`     | falls through to the local same-audience path, not the permit | `does not let a peer session bypass the local parent audience check`          |

Ordering/replay/crash/version-skew are not in play: authorization is a pure function of
(command, authority, read model) with no persisted state and no cross-command ordering.

### Closing the handler→guard wiring hole

Guard-layer tests alone would all still pass if `subagent/handlers.ts` kept dispatching the plain
`sessionDispatchAuthority` — they construct the peer authority themselves, so the constructor swap
would be untested and production would still reject every peer spawn. So the stub engine in
`handlers.test.ts` is changed to **record the authority argument it currently discards**, and
`peer-scoped receiver spawn resolves child authority before dispatch` asserts the recorded authority
for `thread.parent.set` carries `peerSourceEnvironmentId === sourceEnvironmentId`. Reverting the
call-site swap turns that assertion red, so both halves of the fix are pinned.

## Smallest-change argument

The defect is one predicate at `commandAudienceGuard.ts:478-484` that lacks the information needed to
distinguish a legitimate peer from an arbitrary factory session. The minimum fix is to supply that
information at the one site that has it and consume it in that one predicate. Everything else in the
diff stays as reviewed through round 8.

---

# Round 11 — the permit bypasses the audience ceiling (gate P1)

## The defect in round 9's own fix

`requireRemoteParentAllowed` (`commandAudienceGuard.ts:511-517`) returns `Effect.void` on an
environment-identity match, which short-circuits the audience-ceiling refusal immediately below it
(`:518-526`). **Environment identity is not audience identity**: threads of different data audiences
live in the same environment, so proving the parent lives in the environment the peer token
authenticated as proves nothing about whether the caller may reach that parent's data. A peer whose
`audienceCeiling === "factory"` therefore links a remote parent the ceiling check would refuse.

This is reached only on the remote-parent branch (`:897` `else`), which is taken precisely when the
parent is absent from the local read model — which is why the parent's `dataAudience` is not readable
locally and the ceiling check is the only remaining segregation.

## Decision: fail closed, do not plumb (binding)

Two fixes exist.

**(a) Plumb the remote parent's `dataAudience`** through `SubagentPeerRegistry`, the peer token mint,
`McpInvocationContext` and the source-side spawn path, then compare it against the ceiling. Correct
and complete, but 4+ files on top of an already round-9 diff — exactly the growing-diff review loop
the policy exists to prevent. **Not done here.** Tracked as ADA-184, retargeted from "hardening" to
"restore factory peer remote-parent capability".

**(b) Fail closed (chosen).** If we cannot authenticate the remote parent's audience, a
factory-ceiling caller is refused. Add the ceiling condition to the permit so factory-ceiling
authorities fall through to the existing failure branch:

```ts
input.authority.kind === "session" &&
  input.authority.audienceCeiling !== "factory" &&
  input.authority.peerSourceEnvironmentId !== undefined &&
  input.authority.peerSourceEnvironmentId === input.parentEnvironmentId;
```

This is strictly more conservative than the round-9 branch: it removes permits, never adds one. It
preserves peer spawns for every non-factory caller.

**Accepted, deliberate consequence:** a factory-ceiling peer can no longer link a remote parent at
all. If that breaks a legitimate factory peer-spawn flow, that failure is the signal that (a) is
needed — and it is far better to learn it from a red test than to ship a guard that does not
segregate.

## Measured consequence: the narrowed permit is now decision-dead

`AuthAudienceCeiling = DataAudience = "private" | "factory"`
(`packages/contracts/src/auth.ts:9` → `packages/contracts/src/orchestration.ts:128`), so
`audienceCeiling !== "factory"` is exactly `=== "private"`. Enumerating
`requireRemoteParentAllowed` after the narrowing:

| authority                                  | with permit block                          | with permit block deleted                                                                             |
| ------------------------------------------ | ------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `session`, `private` ceiling, env matches  | permit → `Effect.void`                     | refusal needs `session && factory` (false) / `audience-bound && factory` (false) → tail `Effect.void` |
| `session`, `factory` ceiling               | block skipped                              | —                                                                                                     |
| `trusted-system` / `audience-bound-system` | block skipped (`kind === "session"` fails) | —                                                                                                     |

Every row is identical. **The permit block can no longer change any outcome**, and
`requireRemoteParentAllowed` is now behaviourally identical to its pre-round-9 form. By extension
`peerSourceEnvironmentId` and `peerSessionDispatchAuthority` are consulted by no live decision.

This was measured, not reasoned: forcing the permit to never fire (inserting `false &&` into the
predicate) left the guard suite at **41/41 passing** — no test distinguishes the two programs,
because no behavioural difference exists to distinguish.

The narrowing is still implemented exactly as specified above, because the security property it
establishes is correct and the wording was binding. But the resulting dead branch is a real code
defect and the manager must choose the end-state (see the memo): delete the permit block only, or
also unwind round 9's now-inert peer authority plumbing.

## Blast radius (round 11)

- `commandAudienceGuard.ts:511-517` only — one added conjunct. No signature, type, constructor, call
  site, event, projection or client change. `subagent/handlers.ts` is untouched by this round.
- The peer spawn path (`handlers.ts:1002`) mints a **hardcoded `factory`** ceiling, so in production
  this narrowing refuses today's peer remote-parent link. That is the accepted consequence above,
  pinned by an updated test rather than left to be discovered at runtime.

## Edge-case matrix → named red-first test (round 11)

| case                                                | expectation                        | test                                                                                              |
| --------------------------------------------------- | ---------------------------------- | ------------------------------------------------------------------------------------------------- |
| peer authority, **factory** ceiling, env id matches | **rejected** (was permitted in r9) | `refuses a remote parent for a factory-ceiling peer session whose source environment matches`     |
| peer authority, **private** ceiling, env id matches | permitted                          | `permits a remote parent for a non-factory-ceiling peer session whose source environment matches` |

The first replaces r9's `permits a remote parent for a peer session whose source environment
matches`, whose authority was `peerAuthority` (`audienceCeiling: "factory"`, test file `:172-176`).
Its expectation is now wrong for the reason above and is inverted. The second is its companion: it
proves the narrowing is a ceiling narrowing, not "refuse every peer", so the fix cannot regress into
disabling the peer path wholesale.

Honest limit of the companion test: it pins the **behaviour** (a private-ceiling peer keeps its
remote parent) but not the **permit branch**, since that behaviour now also holds via the tail
`Effect.void` — see the decision-dead section above. It stays because the behaviour is worth
pinning against a future over-narrowing of the refusal branch; it must not be read as coverage of
the permit block.

## Class sweep — early `return Effect.void` permits that short-circuit a later audience check

Every `return Effect.void` / bare `return` in this file, with the mechanism checked rather than
assumed:

| site                                         | verdict                                                                                                                                                                                                                                                                                                                                                   |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `:516` `requireRemoteParentAllowed` permit   | **The defect.** Introduced by this diff (`git diff --cached HEAD` shows it as added). Fixed this round.                                                                                                                                                                                                                                                   |
| `:499` `requireSameThreadAudience`           | Not a short-circuit: the equality it returns on **is** the audience check, and nothing follows it.                                                                                                                                                                                                                                                        |
| `:527` `requireRemoteParentAllowed` tail     | Terminal permit after the ceiling check has run. Correct by construction.                                                                                                                                                                                                                                                                                 |
| `:552` `requireSameBootstrapProjectAsThread` | Same shape as `:499` — the comparison is the check.                                                                                                                                                                                                                                                                                                       |
| `:567` `bootstrap === undefined`             | No bootstrap payload exists, so there are no side effects to authorize. Not audience-related.                                                                                                                                                                                                                                                             |
| `:570` `!isFactorySessionAuthority(...)`     | Skips the bootstrap worktree-path checks for non-factory authorities. **Not a bypass:** those checks exist to stop a _factory_ session creating a worktree overlapping a hidden private project. A private-ceiling session passes `canAccessAudience` for every audience anyway (`:403-407`), so skipping them grants it nothing it did not already hold. |
| `:671` `prepareWorktree === undefined`       | No worktree preparation requested; nothing to check.                                                                                                                                                                                                                                                                                                      |

`commandAudienceGuard.ts` **does not exist on `origin/main`** (`git show
origin/main:apps/server/src/orchestration/commandAudienceGuard.ts` → `fatal: path ... exists on
disk, but not in 'origin/main'`), so there is no pre-existing-on-main instance of this class to file.
`:567`/`:570`/`:671` are pre-existing at branch `HEAD` (`e85caa5b3`), not added by the staged diff.

## Smallest-change argument (round 11)

One added conjunct in one predicate, plus the two tests that pin both directions of it. The fix
removes an unsound permit without touching any of the plumbing that would be required to restore the
capability soundly — which is deliberately deferred to ADA-184.
