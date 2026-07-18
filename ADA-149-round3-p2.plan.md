# ADA-149 Round 3 P2 Plan

Intent: fix the two confirmed PR #205 P2 regressions in `ChildThreadCoordinator` without changing provider-death semantics outside terminal child settlement. Stopped session-set events must not destroy a pending replacement start, and projection-restored terminal children must surface provider death `lastError` text instead of generic turn/session wording.

Blast radius: `apps/server/src/orchestration/Layers/ChildThreadCoordinator.ts` live session-set handling, replay session-set handling, projected lifecycle reconciliation, one-shot projection settlement, archive projection settlement, wait/wake result text, dispatch-lease release, `subagent_wait_deliveries`/promoted fallback behavior, restart replay from persisted orchestration events, and `apps/server/src/orchestration/Layers/ChildThreadCoordinator.test.ts`.

Edge-case matrix and red-first tests:

- Ordering both ways: stale stopped before replacement session-set must remain pending; stale stopped after replacement running is already covered. Red test: `does not settle stale stopped while replacement start is pending`.
- Duplicates/replays: persisted stale stopped with replay pending maps must not settle if a replacement start is replayed later/earlier; add replay only if cheap, otherwise document live coverage and handler symmetry.
- Crash-between-steps: projection-restored stopped/error after terminal projection must preserve `session.lastError` through wait/wake. Red test: `surfaces provider-death lastError from non-running projected lifecycle`.
- Same-timestamp ties: keep existing timestamp ownership tests unchanged; the new pending-start guard is independent of timestamp ordering.
- Version skew/concurrency: keep `error` session-set behavior unchanged and only skip ambiguous `stopped` settlement while pending starts exist.

Smallest-change argument: add a stopped-only pending replacement-start guard at the two event-handler settlement points and upgrade terminal error text through local `lastError` preference helpers. A same-turn pending marker identifies replacement-start races while preserving existing initial placeholder recovery. Do not add new settlement machinery; later replacement running/failure and existing wait reconciliation remain responsible for eventual settlement.
