# Dseg slice 6: event/stream guard plan

Intent: make notification delivery and schedule reads honor the authenticated session's `audienceCeiling`, while leaving unrestricted (`private`) sessions unchanged. Restricted replay and shell/thread streams must refuse loudly until the approved follow-up adds durable audience-scoped cursors; content filtering over global sequences would leak private activity through gaps and high-water marks.

Blast radius: `apps/server/src/ws.ts` replay/shell/thread refusal plus notification and schedule subscriptions; `ProjectionSnapshotQuery` aggregate visibility including deleted rows; `DeviceNotifications` v1-to-v2 device-store migration, push/stream/ACK audience attribution; notify and schedule MCP handlers; server/notification/projection/stream tests and auth scope fixtures. Consumers at risk are web/mobile durable stream recovery, existing offline web-push registrations, projection restart/backfill readers, schedule repository refreshes, and older clients/providers. Public event/cursor schemas, event persistence, command authorization, and client reducers remain untouched for the dedicated cursor slice.

| Edge case                                            | Red-first guardrail                                                                          |
| ---------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Private and factory events ordered both ways         | `factory event cursor surfaces refuse before replaying either ordering`                      |
| Duplicate/replayed events and catch-up overlap       | `restricted replay and catch-up share the same loud refusal`                                 |
| Crash/restart with missing projection classification | `aggregate visibility lookup fails closed when projection classification is absent`          |
| Same-timestamp private/factory events                | `restricted replay refusal exposes neither event nor ordering metadata`                      |
| Version skew: v1 devices lack audience               | `migrates version-one notification devices to the private audience`                          |
| Version skew: malformed v2 device lacks audience     | `rejects unattributed version-two notification devices`                                      |
| Concurrent live publish during catch-up              | `restricted shell/thread streams refuse before attaching live delivery`                      |
| Deleted/archived aggregates and guessed IDs          | `private, factory, and nonexistent thread subscriptions share the restricted-cursor refusal` |
| Hidden schedule update                               | `suppresses schedule refreshes and rebases the audience-visible sequence densely`            |
| Project promotion without schedule mutation          | `refreshes factory schedules on audience promotion without a task mutation`                  |
| Notification show/dismiss and push fan-out           | `notification delivery table scopes websocket events and registered devices by audience`     |
| Unrestricted compatibility                           | `private replay, streams, notifications, schedules, and v1 devices remain complete`          |

Smallest change: keep canonical stores, public RPC contracts, and client reducers unchanged; add one shared projection-backed aggregate visibility query for safe schedule/notification reads and one common factory cursor refusal used by replay, shell, and thread streams. The approved cursor-namespace follow-up owns event persistence and recovery semantics; this slice suppresses unchanged restricted schedule snapshots, rebases their session-local sequence densely, and migrates v1 devices to required-at-rest v2 audience metadata while preserving explicit ceilings.
