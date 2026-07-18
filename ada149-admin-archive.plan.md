# ADA-149 supported admin archive lever plan

## Intent

Add `t3_archive_thread`, an MCP administration tool that lets an owner archive a non-terminal strict descendant by dispatching the existing `thread.archive` orchestration command. The tool must bind the target's ancestry to the invoking private root and fail closed for self-target, child, factory-audience, peer, cross-root, broken-ancestry, and cyclic-ancestry callers, while preserving the coordinator's existing `thread.archived` handling that settles the child and releases its dispatch lease.

## Diagnosis evidence

- Read-only production SQLite inspection found the Jul 9-10 child rows archived together at `2026-07-15T21:37:33.595Z`; several roots had no projected terminal turn before the archive cascade.
- The persisted log contains one existing `thread.archive` command followed by `thread.archived` events for the root and descendants, confirming the supported command path used by the manual mitigation.
- `ChildThreadCoordinator.handleThreadArchived` calls `SubagentDispatchLimiter.releaseForChild`, and `releases live archive leases even when projection is still running` already proves six held leases become available after those events.

## Blast radius

- `apps/server/src/mcp/toolkits/thread/tools.ts`: public MCP input/output/error schema, destructive tool metadata, and toolkit membership.
- `apps/server/src/mcp/toolkits/thread/handlers.ts`: owner authorization, bounded fail-closed target-ancestry validation, target preflight, coordinator terminal-state check, command-id generation, and dispatch through `OrchestrationEngineService`.
- `apps/server/src/mcp/toolkits/thread/handlers.test.ts`: red-first behavior, authorization matrix, target errors, and command-shape tests; its mocks must cover the coordinator/query/engine dependencies.
- `apps/server/src/mcp/McpOfficialClientConformance.test.ts` and `apps/server/src/mcp/toolSchemas.test.ts`: advertised tool inventory/input schema guards.
- Automatic consumers: Codex/Claude/Cursor/OpenCode/Grok provider sessions discover the new tool through the existing MCP `tools/list` endpoint. No client-runtime, WebSocket, contract command/event, projector, SQLite schema, migration, or live configuration changes are needed.
- Restart/replay remains owned by `ChildThreadCoordinator`: persisted `thread.archived` events are replayed before non-terminal children are re-seeded, so archived ghosts do not regain leases after reboot.

## Red-first edge-case matrix

| Case                                                                       | Expected behavior                                                                                      | Named proof                                                                                              |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------- |
| Private root owner archives tracked active child                           | Dispatch exactly one existing `thread.archive` command and return its sequence                         | `archives an active child through the existing orchestration command` (new, red before tool exists)      |
| Caller is private child, factory root, factory child, or peer              | Reject before target lookup/dispatch                                                                   | `authorizes archive only for a private root provider context` (new table-driven, red)                    |
| Private root targets itself                                                | Reject before ancestry traversal/dispatch so the caller cannot cascade-archive its active tree         | `rejects owner self-archive` (new, red)                                                                  |
| Private root targets another root's child                                  | Reject before terminal check/dispatch                                                                  | `rejects a private-root non-owner targeting another root's child` (new, red)                             |
| Target ancestry has a missing parent or cycle                              | Fail closed without dispatch or unbounded traversal                                                    | `rejects broken or cyclic target ancestry` (new table-driven, red)                                       |
| Target is already coordinator-terminal                                     | Reject clearly; do not dispatch                                                                        | `rejects an already-terminal child without dispatching` (new, red)                                       |
| Target does not exist                                                      | Reject clearly; do not crash or dispatch                                                               | `rejects a nonexistent archive target without dispatching` (new, red)                                    |
| Target was already archived / retry after crash-between-dispatch-and-reply | Reject clearly through archived-inclusive preflight; the first persisted archive remains authoritative | `rejects an already-archived retry without dispatching` (new, red)                                       |
| Archive event arrives before projected stop/turn terminal                  | Release the child lease immediately, settle as killed, and restore capacity                            | existing `releases live archive leases even when projection is still running`                            |
| Terminal event arrives before archive                                      | Preserve the established terminal result and keep release idempotent                                   | existing `preserves replayed completion when a child is archived later` plus new terminal preflight test |
| Duplicate archive calls / concurrent calls                                 | Orchestration serialization permits at most one archive; later preflight or invariant failure is clear | new already-archived retry test plus existing command receipt/invariant coverage                         |
| Duplicate/replayed `thread.archived` event                                 | `releaseForChild` is idempotent and reboot replay does not reseed the archived child                   | existing limiter duplicate-release and coordinator archived-ghost reconciliation tests                   |
| Crash after persistence but before MCP response                            | Retry sees the archived projection or the engine invariant; no parallel state mutation exists          | new already-archived retry test                                                                          |
| Same-timestamp archive/terminal events                                     | Existing event sequence, not wall-clock comparison in the MCP tool, determines coordinator ordering    | existing coordinator replay ordering tests                                                               |
| Older MCP client / newer server                                            | Extra tool is additive; all tool inputs remain top-level JSON objects                                  | updated official-client inventory and `every MCP tool advertises a top-level object input schema`        |

## Smallest-change argument

The thread toolkit already owns thread-management MCP tools and the invocation carries the source thread id, so it is the narrowest layer that can enforce root/private ownership. The handler will first reject equality, then walk persisted `parentThreadId` links with a visited set and depth bound until it reaches the invoking root, query existing coordinator state, and dispatch `thread.archive`; reusing the established event, projector, archive cascade, reactor, replay, and limiter release paths avoids a new admin event, persistence mutation, lease API, CLI protocol, or configuration surface.
