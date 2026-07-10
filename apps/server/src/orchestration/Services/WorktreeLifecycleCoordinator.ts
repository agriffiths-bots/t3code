import type { OrchestrationCommand, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Semaphore from "effect/Semaphore";

export interface WorktreeLifecycleCoordinatorShape {
  readonly withPermit: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  readonly markTeardownPending: (threadId: ThreadId) => Effect.Effect<void>;
  readonly clearTeardownPending: (threadId: ThreadId) => Effect.Effect<void>;
  readonly isTeardownPending: (threadId: ThreadId) => Effect.Effect<boolean>;
}

export class WorktreeLifecycleCoordinator extends Context.Service<
  WorktreeLifecycleCoordinator,
  WorktreeLifecycleCoordinatorShape
>()("t3/orchestration/Services/WorktreeLifecycleCoordinator") {}

export function commandRequiresWorktreeLifecycle(command: OrchestrationCommand): boolean {
  switch (command.type) {
    case "project.create":
    case "project.delete":
    case "thread.archive":
    case "thread.create":
    case "thread.delete":
    case "thread.turn.start":
    case "thread.unarchive":
      return true;
    case "project.meta.update":
      return command.workspaceRoot !== undefined;
    case "thread.meta.update":
      return (
        command.branch !== undefined ||
        command.worktreePath !== undefined ||
        command.worktreeRemovable !== undefined ||
        command.worktreeRemovalPath !== undefined
      );
    default:
      return false;
  }
}

export const WorktreeLifecycleCoordinatorLive = Layer.effect(
  WorktreeLifecycleCoordinator,
  Semaphore.make(1).pipe(
    Effect.map((semaphore) => {
      const pendingTeardowns = new Set<ThreadId>();
      return WorktreeLifecycleCoordinator.of({
        withPermit: (effect) => semaphore.withPermit(effect),
        markTeardownPending: (threadId) =>
          Effect.sync(() => {
            pendingTeardowns.add(threadId);
          }),
        clearTeardownPending: (threadId) =>
          Effect.sync(() => {
            pendingTeardowns.delete(threadId);
          }),
        isTeardownPending: (threadId) => Effect.sync(() => pendingTeardowns.has(threadId)),
      });
    }),
  ),
);
