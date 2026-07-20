# dseg-s7-r12 — remove the decision-dead remote-parent permit

## Intent

`requireRemoteParentAllowed` carries a permit branch that cannot change any outcome:
`AuthAudienceCeiling` is `"private" | "factory"`, so `audienceCeiling !== "factory"` implies
`=== "private"`, and a private-ceiling session already reaches the tail `Effect.void`. Delete
the dead branch so the guard stops appearing to establish the #207 property twice, then unwind
only what measurement proves is left with zero consumers.

## Blast radius

- `apps/server/src/orchestration/commandAudienceGuard.ts:508-525` — the branch itself.
- `parentEnvironmentId` parameter of `requireRemoteParentAllowed` — becomes body-unreferenced;
  check `vp check`/`tsgo` before deciding.
- `peerSourceEnvironmentId` (type field :33), `peerSessionDispatchAuthority` (:62),
  `handlers.ts:1003`, `handlers.test.ts:1776` — reference-count each, remove only at zero.
- No projection, migration, schema, client, or restart path is touched: this is a pure
  refusal-path predicate with no persisted representation.

## Edge-case matrix (all via the guard test, real `authorizeOrchestrationCommandMutation`)

| case                               | authority                     | env match | expected after deletion       |
| ---------------------------------- | ----------------------------- | --------- | ----------------------------- |
| factory-ceiling peer, env matches  | `peerAuthority`               | yes       | REFUSE (tail refusal)         |
| private-ceiling peer, env matches  | `privateCeilingPeerAuthority` | yes       | PERMIT (tail `Effect.void`)   |
| factory-ceiling peer, env differs  | `peerAuthority`               | no        | REFUSE                        |
| non-peer factory session           | `factoryAuthority`            | n/a       | REFUSE                        |
| factory audience-bound system      | audience-bound                | n/a       | REFUSE                        |
| peer session, LOCAL private parent | `peerAuthority`               | n/a       | REFUSE (local audience check) |

Every row already has a named test in
`apps/server/src/orchestration/Layers/OrchestrationCommandAudienceGuard.test.ts:1720-1830`.
No new test is needed and none may be deleted or weakened; the deletion must leave all six green.

## Smallest-change argument

The property (#207: a factory-audience surface cannot pivot to private data) is carried by the
tail refusal at `commandAudienceGuard.ts:526-534`, not by the permit. Deleting the permit is
therefore behaviour-preserving by construction, and is the whole fix. Any further removal is
justified only by a reference count of zero plus green `tsgo --noEmit` / `vp check` / suite.

## Revert-mutation note

The factory-ceiling refusal test can no longer be reverted by restoring a permit line, because
after this change no such line exists. The property is re-pinned against the tail refusal's
session clause (`input.authority.kind === "session" && input.authority.audienceCeiling ===
"factory"`), which is the line that actually carries it.

MEASURED: deleting that clause fails 3 of 41 in
`OrchestrationCommandAudienceGuard.test.ts` — `refuses a remote parent for a factory-ceiling
peer session whose source environment matches`, `rejects a remote parent whose environment does
not match the peer session`, and `rejects a remote parent for a non-peer factory session`.
Restored: 41/41.
