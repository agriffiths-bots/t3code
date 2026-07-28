# ADA-149 r5 — finish the bounded-read sweep at `getThreadDetailBounded`

## Intent

Round 4 introduced `BoundedProjectionRead` and swept the shell reads, but
`getThreadDetailBounded` still collapses timeout **and** defect into a bare
`Option.none()` that is indistinguishable from "row genuinely missing". Migrate
that read to the tagged three-state form and make every consumer that acts
TERMINALLY on absence take a non-conclusive branch on `Unavailable`.

## The class

A failed or unavailable READ is authoritative about nothing. Unavailability must
never produce a terminal outcome, and must never be recorded as a legitimate
negative (missing row / empty result).

## Defect being fixed

`ChildThreadCoordinator.ts:1206-1211`:

```ts
Effect.timeoutOption(...),
Effect.map(Option.flatten),      // <-- flatten BEFORE catchCause
Effect.catchCause(() => Effect.succeed(Option.none())),
```

`Option.flatten` runs first, so the outer (timeout) `None` and the inner
(missing row) `None` are already merged before `catchCause` adds a third
indistinguishable `None` for defects. Compare `getThreadShellForDrain`
(`:1200-1204`), where `catchCause` sits before any flatten and a defect
therefore lands on the outer/retry branch. The `catchCause` line here is NEW in
this PR, so defect-induced false negatives are newly reachable.

## Blast radius — every `getThreadDetailBounded` consumer

| line | consumer                                       | on absence today                                                         | terminal?                                            | action               |
| ---- | ---------------------------------------------- | ------------------------------------------------------------------------ | ---------------------------------------------------- | -------------------- |
| 1433 | `settleChild(bounded=true)`                    | `finalAssistantText = null`                                              | no — status comes from the caller, only text is lost | LEAVE (see below)    |
| 2058 | `handleThreadArchived` (live)                  | `projectedOutcome === null` → `completeChild(killed, "thread archived")` | **YES**                                              | FIX                  |
| 2072 | `handleThreadArchived` terminal-detail re-read | `finalAssistantText = null` on a shell-derived outcome                   | **YES** when status is `completed`                   | FIX                  |
| 2449 | drain, shell absent                            | `Option.isSome` gate → plain `return`, no settle                         | no                                                   | conservative already |
| 2465 | drain, shell archived                          | `Option.isSome` gate → falls through                                     | no                                                   | conservative already |
| 2530 | drain, completed outcome                       | `Option.isNone` → `return`, no settle                                    | no                                                   | conservative already |
| 3250 | replay `thread.archived`                       | `markLifecycleTerminal(killed, "thread archived")`                       | **YES**                                              | FIX                  |

`1433` is yes/yes/**no** on the class check (c): the terminal status is supplied
by the caller from independent evidence, and every `bounded=true` call site
(`1854, 1965, 2359, 2422, 2496, 2552, 4046, 4074, 4107`) passes `killed` or
`failed`, for which `finalAssistantText` is null on every path anyway. Only
partial text can be lost, never a misclassification. Recorded as a P3 follow-up
rather than fixed here — the named `terminalTextReadUnavailableChildIds` set is
boot-scoped (`:3614`) and cannot be extended to a live settle path without a new
design, which this brief forbids.

## Change

1. `getThreadDetailReadBounded` = `boundedProjectionRead(getThreadDetailById)` —
   tagged, correct ordering (catchCause inside `boundedProjectionRead`, before
   any flatten). `getThreadDetailForBoot` becomes an alias; `getThreadDetailBounded`
   becomes the Option **compatibility view** over it, documented like the
   existing `getThreadShellBounded` (`:1176-1179`), for the three call sites
   where Missing and Unavailable already take the same conservative direction.
2. `:3250` replay — `Unavailable` → `activeArchiveByReplayedChild.add(threadId)`
   and return. Non-terminal; live reconciliation settles it (`:3927-3931`).
3. `:2058` live — `Unavailable` → keep the independent shell fallback (real
   evidence), but if that yields nothing, add to `archivedActiveChildIds` and
   return WITHOUT completing, instead of killing.
4. `:2072` — if the terminal-detail re-read is `Unavailable` and the shell
   outcome is `completed`, stay pending rather than delivering an empty
   completed result.
5. `assertParent` (`:2612-2620`) — add the missing revert-sensitive test.

## Edge-case matrix → named red-first test

| edge case                                                    | test                                                                                                                             |
| ------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| replay archive, detail read times out                        | `replayed archive keeps a child active when the detail read is unavailable`                                                      |
| replay archive, detail row genuinely missing                 | `still settles a replayed archive whose detail row is genuinely missing` (pins Missing ≠ Unavailable)                            |
| live archive, detail unavailable, no shell evidence          | `live archive leaves a child pending when the detail read is unavailable`                                                        |
| live archive, detail unavailable, shell IS terminal          | shell evidence still settles (existing coverage at `:1324`)                                                                      |
| live archive, shell says completed, text re-read unavailable | `live archive does not deliver an empty completed result when the text re-read is unavailable`                                   |
| assertParent, shell read unavailable                         | `assertParent reports a retryable failure when the shell read is unavailable` — asserts the message, not merely `Exit.isFailure` |
| assertParent, shell read Missing                             | existing "not a child of" behaviour unchanged                                                                                    |

Harness note: `detailUnavailableIds` returns `Option.none()`, i.e. **Missing**.
The real `Unavailable` lever is `slowThreadDetailIds` / `slowThreadShellIds`
(timeout). New tests use the slow levers.

Correction after implementation: the Missing companion was planned to assert
`killed` / "thread archived". It actually settles `failed` — boot session
reconciliation reaches a running session with no detail row before the replay
archive branch does. Verified pre-existing on HEAD (same result with the fix
reverted), so the test asserts the real value. The contrast it pins is
unchanged and is the point: **Missing settles, Unavailable stays pending.**

## Revert-mutation proof

Each new test must be RED with the guard reverted. Recorded per test in the
commit message; `assertParent` specifically must fail on the message assertion,
since the old code also produced `Exit.isFailure` and the existing test therefore
passes identically before and after.

## Smallest-change argument

`BoundedProjectionRead` already exists and is the right shape. This adds one
tagged reader, one documented compatibility view, and three consumer branches.
No new type, no parallel mechanism, no change to `ThreadDeletionReactor`.

---

## Follow-on: orphan settlement live/replay symmetry (rounds 6-7)

### Intent

Every state transition and activity kind this branch's orphan machinery
introduces must land identically whether it arrives live or is reconstructed by
`reconcileFromLog`. Four defects on this PR were the same class: an orphan
settlement treated as permanent on one path and revocable on the other.

### The class

`reconcileFromLog` is a second, hand-written implementation of the live
handlers. Any state the live path can _revoke_ must be revocable on replay too,
in the same order, or a restart silently changes a child's outcome.

### Round 7 defect (gate P1, `ChildThreadCoordinator.ts:3610`, conf 0.89)

Round 6 memoized every durable orphan settlement into the ancestry memo before
the boot DFS. That made the walk kill **all** descendant cleanup candidates
regardless of ordering. Live processing survives it because
`markOrphanSettledChild` made the inferred settlement revocable by a later
start; replay did not compare order, so after a restart the history
_ancestor orphan settlement -> descendant replacement start_ settled the
descendant `killed`, discarding a valid replacement turn and leaking its lease.

### Change

`reconcileFromLog` records a local monotonic `replayOrdinal` per event and two
maps: `durableOrphanSettlementOrdinalByChild` (set with the settlement, cleared
by an accepted start, same lifetime as `authoritativeDurableOrphanSettlementByChild`)
and `acceptedStartOrdinalByChild`. The ancestry memo now holds
`{reason, settledAtOrdinal}` instead of a bare reason, so an inherited reason
carries the log position of the settlement it came from. A boot child skips
propagation only when it has an accepted start strictly newer than that
settlement, and is then memoized live so its own descendants are not killed
either. Reasons derived from the live shell (archived/missing ancestor) carry
no ordinal and are never superseded — propagation is preserved, not disabled.

A local ordinal is used rather than `event.sequence` because `sequence` is
optional on the event shape and only intra-replay order matters.

### Live vs replay symmetry table

| state                                         | live write                            | replay write            | revoke (both paths)                  |
| --------------------------------------------- | ------------------------------------- | ----------------------- | ------------------------------------ |
| `ORPHAN_SETTLED_ACTIVITY_KIND`                | emit `:1290`, consume `:2397`         | consume `:3104`         | same `orphanSettlementReason` helper |
| `orphanSettledChildIds`                       | `:2399`                               | `:3561`, `:3743`        | `clearOrphanSettledChild` `:2378`    |
| `suppressParentWakeChildIds`                  | `:578` (via `markOrphanSettledChild`) | same                    | `:582`; sole reader `:1434`          |
| `authoritativeDurableOrphanSettlementByChild` | n/a (replay-local)                    | `:3111`                 | `:3085` on accepted start            |
| `durableOrphanSettlementOrdinalByChild`       | n/a (replay-local)                    | `:3112`                 | `:3086` on accepted start            |
| `lifecycleTerminatedByChild`                  | n/a (replay-local)                    | `markLifecycleTerminal` | `:3060-3065` orphan escape hatch     |

`lifecycleTerminatedByChild` / `markLifecycleTerminal` pre-exist on `origin/main`
(7 / 6 hits); every other row has **zero** hits there and is in-diff.

### Round 7 second defect (gate P1, `:3739`, conf 0.95)

The first cut memoized a superseding child as unconditionally live, which
masked that child's own later shell state: with the order
_ancestor settlement -> child start -> child archived_, descendants read the
blanket-live memo instead of the archived shell and survived boot cleanup
holding their leases. Supersession now stops inheritance only — the child is
re-classified from its **own** shell (`replacementShellClassification`, no
ancestor recursion, same Unavailable/Missing/archived semantics as the walk),
so a replacement archived or deleted afterwards still orphans what is below it.

### Round 7 third defect (gate P1, `:3766`, conf 0.96)

A superseding child may be durably settled again _after_ the start that
superseded its ancestor's settlement. With the order
_ancestor settlement -> child start -> child settlement_, replacing the child's
pre-seeded memo with the shell classification kept the child itself killed but
stopped its descendants inheriting, leaving them alive on leases. The shell
classification is now consulted only when the child has no durable settlement
of its own; its own settlement is by construction the newer durable fact and
stays authoritative.

The six round-7 defects are one sub-class: **supersession bounds inheritance,
it does not assert liveness.** Each fix narrows what the supersession branch is
allowed to claim about the child.

### Round 7 fourth defect (gate P1, `:3780`, conf 0.97)

The same overwrite on the non-superseding branch. With nested settlements
(_ancestor settled -> descendant start -> intermediate child settled_), the
intermediate child inherits the ancestor's older classification and its own
newer memo is replaced, so the descendant compares its start against the
ancestor's ordinal, reads as a replacement, and keeps its lease. Both branches
now write through one `memoizeInheritedClassification` helper that refuses to
overwrite a child's own durable settlement — the invariant stated once rather
than re-checked per branch.

### Round 7 fifth defect (gate P1, `:3751`, conf 0.97)

The helper asserted that a child's own settlement is always the newer fact.
Both orderings occur: with _intermediate settled -> descendant start ->
ancestor settled_ the intermediate's own classification is the OLDER one, and
keeping it let the descendant read as a replacement and retain its lease. The
helper now states the real invariant — **a thread is classified by the newest
durable orphan fact at or above it** — and compares log ordinals in both
directions. A shell-derived reason has no ordinal because it describes current
state, which is newer than any logged event. The supersession branch is also
skipped for a child holding its own settlement: an accepted start clears that
settlement during replay, so one that survived is necessarily newer than the
child's last start.

Rounds 7.2-7.5 were all the same mistake — asserting a fixed precedence
(live / own / closest) instead of comparing recency. Ordinal comparison is the
fix; the earlier guards were special cases of it.

### Round 7 sixth defect (gate P1, `:3087`, conf 0.93)

`acceptedStartOrdinalByChild` stamped every `thread.turn-start-requested`,
but a request arriving while a turn is still active is a **steer** into that
turn (`recordPendingTurnStart` records it via `pendingSameTurnStarts`), not a
new lifecycle. A steer delivered after an ancestor settlement therefore looked
like a replacement and cancelled inherited cleanup for a descendant whose turn
began before the settlement. Only requests that begin a new turn are stamped.

### Edge-case matrix → named red-first test

| edge case                                       | test                                                                                  |
| ----------------------------------------------- | ------------------------------------------------------------------------------------- |
| descendant start newer than ancestor settlement | `preserves a descendant replacement start newer than the ancestor settlement`         |
| replacement child archived after its start      | `still settles descendants of a replacement child archived after its start`           |
| descendant start older than ancestor settlement | `still settles a descendant whose replacement start predates the ancestor settlement` |
| descendant with no start at all                 | `propagates a durable orphan settlement to a descendant of the settled child`         |
| suppression lifted on supersession              | `re-enables the parent wake when a replacement turn supersedes an orphan settlement`  |
| replacement child settled again after its start | `propagates a replacement child's own settlement newer than the inherited one`        |

### Revert-mutation proof

Eight mutations, each isolating one guard; counts in the commit message.

### Smallest-change argument

No new mechanism: the memo already existed and already flowed the reason down
the ancestry. This widens its value by one field and adds one comparison at the
single propagation site. Ordinals are local to `reconcileFromLog`, so no
persisted shape changes and no client/migration blast radius.
