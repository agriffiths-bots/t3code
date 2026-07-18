# DSEG Slice 7 Command Guard P2 Fix Plan

## Intent

Fix two accepted PR #207 review defects in the audience-scoped mutation guard: bootstrap worktree preparation must not use a hidden/private checkout from an existing-thread shortcut, and project metadata updates must not leak hidden workspace-root collisions through downstream invariants. The guard remains the fail-closed boundary for hostile factory-scoped callers; private resources must look nonexistent.

## Blast Radius

- `apps/server/src/orchestration/commandAudienceGuard.ts`: target mutation authorization, not-found-shaped masking, bootstrap field binding.
- `apps/server/src/orchestration/Layers/OrchestrationCommandAudienceGuard.test.ts`: regression coverage for factory caller/private fixtures and class-sweep cases.
- Downstream consumers that could otherwise leak or side-effect after guard pass: `decider.ts` (`project.create`, `project.meta.update`, `thread.create`, `thread.turn.start`, `thread.parent.set` invariants), `commandInvariants.ts` workspace/id messages, `BootstrapTurnStartDispatcher.ts` Git fetch/create-worktree/setup-script before final turn, and `WorktreeLifecycleCoordinator.ts` project/worktree lifecycle triggers.
- Client/RPC callers: HTTP/WS dispatch, MCP thread/subagent tools, CLI trusted-system dispatch. No persistence schema, projection shape, migration, or restart behavior changes are intended.

## Edge-Case Matrix

| Edge case                                                                                                                | Red-first test                                                                                                                                                  |
| ------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Existing factory thread + bootstrap `prepareWorktree.projectCwd` set to a private project root                           | `rejects prepareWorktree bootstraps on existing threads before filesystem side effects`                                                                         |
| New bootstrapped thread with authorized factory `createThread.projectId` + matching factory `prepareWorktree.projectCwd` | `allows prepareWorktree when createThread authorizes the same accessible project root`                                                                          |
| New bootstrapped thread with authorized factory `createThread.projectId` + hidden/private `prepareWorktree.projectCwd`   | `rejects prepareWorktree whose cwd belongs to a hidden project even when createThread is factory`                                                               |
| Prepare-only bootstrap for missing thread, with no authorized `createThread` anchor                                      | covered by `rejects prepareWorktree bootstraps on existing threads before filesystem side effects` plus existing missing-thread branch; guard stays fail-closed |
| Factory project meta update moves onto hidden private root before decider collision                                      | `masks hidden project root collisions on project meta update`                                                                                                   |
| Hidden root collision with trailing slash/path normalization                                                             | same meta-update collision test uses normalized private root variant                                                                                            |
| Existing downstream referential checks (`projectId`, `threadId`, parent/source-plan ids)                                 | existing guard suite stays green; add sweep assertions only if another unmasked site appears                                                                    |
| Duplicate/replay and same-timestamp ties                                                                                 | guard decisions are pure read-model lookups; command receipts/timestamps stay in engine/decider and are unchanged                                               |
| Crash between authorization and side effect                                                                              | rejected prepareWorktree never enters dispatcher side effects; authorized paths keep existing cleanup semantics                                                 |
| Version skew/new command field                                                                                           | existing default fail-closed command case plus targeted bootstrap side-effect check remains the guardrail                                                       |

## Class Sweep Notes

Side-effect-bearing command fields found: `project.create.workspaceRoot/createWorkspaceRootIfMissing` and `project.meta.update.workspaceRoot` trigger worktree lifecycle only after guard/decider; `thread.create.worktreePath/worktreeRemovalPath` and `thread.meta.update.worktree*` are metadata/lifecycle fields after thread authorization; `thread.turn.start.bootstrap.prepareWorktree.*` drives Git fetch/create-worktree/setup-script inside `BootstrapTurnStartDispatcher` before the final stripped turn and needs explicit project-root authorization; `bootstrap.runSetupScript` is process side effect but only meaningful with an already authorized target worktree/project after the same bootstrap authorization; `project.data-audience.set.expectedWorkspaceRoot` is trusted local admin only and still requires target project access.

Error-shape leak sites found: hidden project/thread ids are already masked by `requireProjectAudience`, `requireThreadAudience`, hidden create-id checks, and parent/source-plan checks; `project.create` already masks hidden workspace-root collisions; `project.meta.update` is missing that precheck and will be fixed. Other decider invariants mention only authorized target ids/threads or user-supplied references after guard resolution, so they do not reveal hidden project ids/roots.

## Smallest Change

Keep the fix local to the guard: add a helper that resolves `prepareWorktree.projectCwd` to an active project and requires it to be the same authorized project as `bootstrap.createThread`, rejecting prepare-only/existing-thread session bootstraps with the same not-found-shaped thread error. Mirror the existing hidden-root collision masking in the `project.meta.update` branch before returning the command, with no contract or persistence changes.
