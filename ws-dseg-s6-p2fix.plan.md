# ws-dseg-s6-p2fix plan

Intent: fix the two confirmed Codex P2 findings on PR #206 without changing the dseg contract. Factory-audience sessions should be able to complete structurally scoped notification ACKs they are allowed to observe, and idle snapshot-required recovery should back off like other unchanged reconciliation checks.

Blast radius: `apps/server/src/ws.ts` RPC scope enforcement; `apps/server/src/auth/audienceScopePolicy.ts` factory read RPC allowlist; their tests in `apps/server/src/server.test.ts` and `apps/server/src/auth/audienceScopePolicy.test.ts`; `apps/server/src/notifications/DeviceNotifications.ts` structural ACK guard and existing tests; client notification wrappers in `packages/client-runtime/src/state/server.ts` and `apps/web/src/pwa/pwaRuntime.tsx`; scheduled-task stream/mutation wrappers in `packages/client-runtime/src/state/schedules.ts`; `packages/client-runtime/src/state/threads.ts` reconciliation scheduling, cache/restart paths, live-event reset paths, and `packages/client-runtime/src/state/threads-sync.test.ts`.

Edge-case matrix and red-first tests:

- Factory show then ACK: `server.test.ts` RED test "allows factory read sessions to ACK delivered factory notifications" asserts a factory read WS session receives a show event, calls `serverAckNotification`, and gets dismiss fan-out.
- Factory ACK private/owner notification: existing `DeviceNotifications.test.ts` "delivers factory-audience notifications only to factory-attributed devices" already rejects factory ACKs for private notification IDs; cite instead of duplicating.
- Duplicate/replayed ACKs: same WS test will assert the mocked ACK is invoked once and dismissal is scoped; existing `ackNotification` state removal makes later ACKs return accepted false.
- Snapshot-required recovered loop: `threads-sync.test.ts` RED test "backs off repeated snapshot-required recoveries on idle threads" drives recovered snapshots at repeated checks and expects revision call times `2s, 4s, 8s` instead of `2s, 4s, 6s`.
- Real change ordering both ways: keep live-event reset behavior unchanged; `markThreadRecentlyActive` remains the only reset path, and recovered snapshot-required will intentionally count as unchanged after acknowledgement.
- Crash/restart and version skew: no persistence schema/protocol shape changes; ACK authorization remains per-call `audienceCeiling`, and snapshot metadata handling continues through existing normalization/acknowledgement paths.
- Same-timestamp ties/concurrency: no timestamp ordering changes; `applyLock` and `SynchronizedRef.modify` remain the concurrency boundaries.

Smallest-change argument: change `serverAckNotification` from operate to read scope and add it to the existing factory read RPC allowlist so the handler's current `audienceCeiling` guard is reachable; leave `serverRegisterNotificationDevice` operate-scoped. In `checkThreadRevision`, call `recordUnchangedRevision` in the `snapshot-required` recovered branch with the sibling branch's null revision/zero-byte accounting before returning.
