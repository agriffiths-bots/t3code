# PR 211 — terminal outcome preservation and orphan retry order fencing

## Intent

Preserve an already-durable child terminal whenever an archive detail read or a replacement turn start is still uncertain. Fence boot orphan-cleanup retries at hot-stream observation order so a queued replacement request protects the replacement before the coordinator worker reaches its handler.

## Blast radius

- `apps/server/src/orchestration/Layers/ChildThreadCoordinator.ts`: live archive markers, live/replay orphan supersession, replay archive cleanup, and boot retry fencing.
- `apps/server/src/orchestration/Layers/ChildThreadCoordinator.test.ts`: event-order regression coverage and a small harness hook for enqueue-without-drain concurrency.
- Consumers that could break: `register`/`waitSlice` one-shot checks, maintenance sweeps, live diff/session handlers, boot log replay, descendant orphan classification, dispatch-lease release/reseed, parent wake suppression, and forked orphan settlement/provider-stop/session-recording retries.
- No contract, projection schema, migration, client, or persisted event-shape change. Restart behavior is covered through `reconcileFromLog`; mixed-version logs remain readable because fencing state is process-local and failure correlation uses the existing optional request id.

## Edge-case matrix and red-first tests

| Edge case                                                                           | Expected invariant                                                                                                               | Named red-first test                                                                                |
| ----------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| orphan terminal -> replacement request -> correlated start failure (live)           | original killed result and wake suppression are restored; no lease remains                                                       | `restores a live orphan settlement when its correlated replacement start fails`                     |
| same ordering reconstructed on restart                                              | failed request is not accepted replacement evidence                                                                              | `restores a replayed orphan settlement when its correlated replacement start fails`                 |
| archive detail unavailable -> `waitSlice`/one-shot check                            | child remains pending; lease bookkeeping cannot become kill authority                                                            | `keeps an uncertain live archive pending through a one-shot wait check`                             |
| completed event -> archive detail unavailable -> cleanup session event              | completed result/text survives; cleanup cannot replace it with archive-killed                                                    | `preserves replayed completion across unavailable archive detail and cleanup session`               |
| replacement start observed while worker is blocked and stale settlement retry wakes | stream observation fences the retry before handler state catches up                                                              | `fences orphan settlement retries when a replacement start is queued but unprocessed`               |
| request failure and settlement in the opposite order                                | a newer orphan settlement remains authoritative; failure cannot resurrect older state                                            | covered by request-id/current-terminal guards plus the live/replay failure tests                    |
| duplicate/overlapping requests and stale failures                                   | only the failure matching the newest pending request may restore the saved terminal                                              | request-id correlation in both live and replay paths; existing overlapping-start tests remain green |
| crash between request, failure, and restoration                                     | immutable-log replay derives the same terminal as live handling                                                                  | replay failure test                                                                                 |
| equal timestamps                                                                    | event order, never timestamps, decides supersession and retry fencing                                                            | replay ordinal and hot-stream enqueue order; no timestamp comparison added                          |
| concurrent retry versus queued request                                              | request observed first wins even if handler runs later; an already-started external call remains bounded/indeterminate as before | queued-replacement retry test plus existing settlement/physical-stop retry tests                    |
| version-skew legacy failure without request id                                      | retain the existing unambiguous-only fallback; ambiguous legacy failures do not restore the wrong lifecycle                      | existing legacy failure tests remain green                                                          |

## Change

1. Save the orphan terminal and replacement request id when live/replay supersession reopens it. A correlated `provider.turn.start.failed` restores that terminal; a confirmed active session, a newer orphan settlement, or a newer request clears/retargets the saved restoration state.
2. On unavailable live archive detail, clear `archivedChildIds` before returning and treat `archivedActiveChildIds` only as lease/unarchive bookkeeping. Diff/session terminal guards consult the authoritative archive marker only.
3. On unavailable replay archive detail with an existing terminal, call the same lifecycle-terminal preservation path used after a successful detail read instead of exposing the result to later active-archive cleanup.
4. Advance the orphan cleanup generation in the hot-stream subscriber before enqueueing `thread.turn-start-requested`; keep every existing forked retry step behind the common generation check.

## Revert-mutation proof

Each numbered change gets its named test run against a local revert of that guard. After restoration, re-run the three prior uncertainty mutations: replay archive unavailable stays pending, live archive unavailable stays pending, and unavailable completed-text reread does not deliver an empty result. Re-run the existing settlement-append and physical-stop supersession tests to prove all retry steps still share the fence.

## Smallest-change argument

The coordinator already owns orphan terminal identity, pending request correlation, archive authority, replay ordinals, and retry generations. Extending those local maps/guards and the existing test harness is smaller and safer than adding a persisted state machine, changing event schemas, or introducing another cleanup subsystem.
