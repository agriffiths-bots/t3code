# ADA-149 session projection marker plan

Intent: Fix the accepted PR #205 review defect where a stopped/error provider session already reflected in the projection can be converted through an interrupted/error latest turn without `fromSessionProjection`. The fix keeps stale post-unarchive projection-only provider-death failures pending on boot instead of waking the parent from pre-unarchive state.

Blast radius: `apps/server/src/orchestration/Layers/ChildThreadCoordinator.ts` projection lifecycle reconciliation, boot replay projection guards, one-shot register/wait reconciliation, live session/turn handlers, pending wake pruning, dispatch lease seeding, and `apps/server/src/orchestration/Layers/ChildThreadCoordinator.test.ts` restart/unarchive tests. Consumers that could break are parent wake injection persistence, foreground waits, detached child settlement, provider-death lease release, archived/unarchived child replay, and projection snapshot readers after restart.

Edge-case matrix:

- Ordering archived -> unarchived -> no new turn with stopped session + interrupted latest turn: red-first test `keeps inactive unarchived stopped interrupted projections pending on boot`.
- Ordering archived -> unarchived -> no new turn with error session + error latest turn: red-first coverage in the same test table.
- Live turn-derived interrupted/error settlement after registration: existing tests `settles error turn-diff as failed`, `settles session-set ready with a completed projected turn as completed`, and provider-death live tests must remain unmarked behaviorally.
- Duplicate/replayed boot events: existing replay reconciliation tests around unarchive terminal pruning and provider-death replay must remain idempotent.
- Crash between wake persistence and restart: existing pending dispatch restart/pruning tests must remain unchanged because marker only affects projection-only skipped settlement.
- Same-timestamp ties: no timestamp ordering changes; existing post-unarchive wake retention tests remain the guard.
- Version skew with older logs lacking terminal turn events: marker applies only to current projection-derived session failures, preserving old live/replay event outcomes.
- Concurrency/replacement start: existing pending replacement-start tests must still prevent stale stopped sessions from settling.

Smallest-change argument: leave `turnTerminalOutcome` live-turn semantics unchanged and add a narrow projection-lifecycle wrapper/branch for terminal session projections. This avoids marking live turn-diff/session-set results while giving boot unarchive guards the structural signal they already expect.
