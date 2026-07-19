# PR 206 main merge plan

Intent: merge `origin/main` at `1c4afe019` into `factory/10-dseg-s6-event-stream-guard` without rebasing, so PR #206 reviews only slice 6. Keep main's finalized slice-5 read-query/redaction state and preserve the branch's slice-6 event, stream, notification, snapshot-required, ACK-scope, and reconciliation-backoff guards.

Blast radius: conflicted server auth policy, projection snapshot query/read consumers, orchestration HTTP revision/snapshot endpoints, websocket RPC authorization and event streams, server tests, client HTTP snapshot/revision loaders, thread state reconciliation, and thread sync tests. Restart/version-skew paths can break if factory clients see overbroad data, lose the `snapshot-required` recovery path, or if private sessions lose existing full access.

Edge-case matrix and red-first guards:

- Ordering both ways: main-before-branch and branch-before-main resolutions must preserve `serverAckNotification` at read scope plus per-call audience ceilings; guarded by `audienceScopePolicy.test.ts`, `DeviceNotifications.test.ts`, and `server.test.ts`.
- Duplicates/replays: replayed events and notification streams must filter by aggregate audience without leaking duplicates across scopes; guarded by `scheduledTaskAudienceStream.test.ts` and affected websocket replay tests.
- Crash between steps: after merge commit, no half-s5 conflict markers or add/add duplicates may remain; guarded by `vp check`, `vp run typecheck`, and conflict-free `git diff origin/main...HEAD --name-only`.
- Same-timestamp ties: main's finalized projection snapshot ordering/redaction and the kept-deleted-terminal behavior must remain authoritative; guarded by `ProjectionSnapshotQuery.test.ts` and `threads-sync.test.ts`.
- Version skew: old clients without snapshot-required machinery must still get not-found/unavailable semantics, while new clients back off and recover snapshots; guarded by `threadSnapshotHttp.test.ts` and `threads-sync.test.ts`.
- Concurrency: concurrent websocket authorization, notification ACKs, scheduled-task refreshes, and thread reconciliation must keep audience checks at each boundary; guarded by affected server websocket/HTTP tests and client-runtime sync tests.

Smallest-change argument: this is a mechanical merge conflict resolution, not a feature change. Use `origin/main` verbatim for s5-owned files, keep branch-only s6 hunks where main is empty, and compose only same-line overlap needed to keep both guards enforceable.
