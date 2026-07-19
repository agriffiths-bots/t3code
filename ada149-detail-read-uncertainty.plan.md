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
| replay archive, detail row genuinely missing                 | `replayed archive still kills a child whose detail row is missing` (pins Missing ≠ Unavailable)                                  |
| live archive, detail unavailable, no shell evidence          | `live archive leaves a child pending when the detail read is unavailable`                                                        |
| live archive, detail unavailable, shell IS terminal          | shell evidence still settles (existing coverage at `:1324`)                                                                      |
| live archive, shell says completed, text re-read unavailable | `live archive does not deliver an empty completed result when the text re-read is unavailable`                                   |
| assertParent, shell read unavailable                         | `assertParent reports a retryable failure when the shell read is unavailable` — asserts the message, not merely `Exit.isFailure` |
| assertParent, shell read Missing                             | existing "not a child of" behaviour unchanged                                                                                    |

Harness note: `detailUnavailableIds` returns `Option.none()`, i.e. **Missing**.
The real `Unavailable` lever is `slowThreadDetailIds` / `slowThreadShellIds`
(timeout). New tests use the slow levers.

## Revert-mutation proof

Each new test must be RED with the guard reverted. Recorded per test in the
commit message; `assertParent` specifically must fail on the message assertion,
since the old code also produced `Exit.isFailure` and the existing test therefore
passes identically before and after.

## Smallest-change argument

`BoundedProjectionRead` already exists and is the right shape. This adds one
tagged reader, one documented compatibility view, and three consumer branches.
No new type, no parallel mechanism, no change to `ThreadDeletionReactor`.
