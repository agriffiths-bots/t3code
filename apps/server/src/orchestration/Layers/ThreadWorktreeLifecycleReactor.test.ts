/* oxlint-disable t3code/no-manual-effect-runtime-in-tests -- The lifecycle worker owns a scoped event subscription that must be drained before assertions. */
import {
  CommandId,
  EventId,
  ProjectId,
  ThreadId,
  type OrchestrationEvent,
  type OrchestrationReadModel,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { describe, expect, it, vi } from "vite-plus/test";

import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import { ProviderService } from "../../provider/Services/ProviderService.ts";
import * as TerminalManager from "../../terminal/Manager.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { ThreadDeletionReactor } from "../Services/ThreadDeletionReactor.ts";
import { WorktreeLifecycleCoordinatorLive } from "../Services/WorktreeLifecycleCoordinator.ts";
import { ThreadDeletionReactorLive } from "./ThreadDeletionReactor.ts";

const NOW = "2026-07-10T18:00:00.000Z";
const PROJECT_ID = ProjectId.make("project-worktree-lifecycle");
const THREAD_ID = ThreadId.make("thread-worktree-owner");
const PROJECT_ROOT = "/repo";
const WORKTREE_ROOT = "/worktrees/owned";

const snapshot = (input: {
  readonly archived?: boolean;
  readonly deleted?: boolean;
  readonly shared?: boolean;
}): OrchestrationReadModel =>
  ({
    snapshotSequence: 2,
    projects: [{ id: PROJECT_ID, workspaceRoot: PROJECT_ROOT, deletedAt: null }],
    threads: [
      {
        id: THREAD_ID,
        projectId: PROJECT_ID,
        worktreePath: `${WORKTREE_ROOT}/packages/app`,
        worktreeRemovable: true,
        worktreeRemovalPath: WORKTREE_ROOT,
        archivedAt: input.archived ? NOW : null,
        deletedAt: input.deleted ? NOW : null,
        session: null,
      },
      ...(input.shared
        ? [
            {
              id: ThreadId.make("thread-worktree-consumer"),
              projectId: PROJECT_ID,
              worktreePath: `${WORKTREE_ROOT}/packages/web`,
              worktreeRemovable: false,
              worktreeRemovalPath: WORKTREE_ROOT,
              archivedAt: NOW,
              deletedAt: null,
              session: null,
            },
          ]
        : []),
    ],
    updatedAt: NOW,
  }) as unknown as OrchestrationReadModel;

const lifecycleEvent = (type: "thread.deleted" | "thread.archived"): OrchestrationEvent =>
  type === "thread.deleted"
    ? {
        sequence: 1,
        eventId: EventId.make("event-owned-worktree-delete"),
        aggregateKind: "thread",
        aggregateId: THREAD_ID,
        type,
        occurredAt: NOW,
        commandId: CommandId.make("command-owned-worktree-delete"),
        causationEventId: null,
        correlationId: CommandId.make("command-owned-worktree-delete"),
        metadata: {},
        payload: { threadId: THREAD_ID, deletedAt: NOW },
      }
    : {
        sequence: 2,
        eventId: EventId.make("event-owned-worktree-archive"),
        aggregateKind: "thread",
        aggregateId: THREAD_ID,
        type,
        occurredAt: NOW,
        commandId: CommandId.make("command-owned-worktree-archive"),
        causationEventId: null,
        correlationId: CommandId.make("command-owned-worktree-archive"),
        metadata: {},
        payload: { threadId: THREAD_ID, archivedAt: NOW, updatedAt: NOW },
      };

async function createHarness(input: {
  readonly snapshot: OrchestrationReadModel;
  readonly pathExists: boolean;
  readonly closeFails?: boolean;
  readonly dirty?: boolean;
  readonly persistedEvents?: ReadonlyArray<OrchestrationEvent>;
  readonly pruneFailuresBeforeSuccess?: number;
}) {
  const events = Effect.runSync(PubSub.unbounded<OrchestrationEvent>());
  const operations: string[] = [];
  const removeWorktree = vi.fn(() =>
    Effect.sync(() => {
      operations.push("remove");
    }),
  );
  let pruneAttempts = 0;
  const pruneWorktrees = vi.fn(() => {
    pruneAttempts += 1;
    return pruneAttempts <= (input.pruneFailuresBeforeSuccess ?? 0)
      ? Effect.fail("worktree prune failed")
      : Effect.sync(() => {
          operations.push("prune");
        });
  });
  const dispatch = vi.fn(() =>
    Effect.sync(() => {
      operations.push("metadata");
      return { sequence: 3 };
    }),
  );
  const closeTerminal = vi.fn(() =>
    input.closeFails === true
      ? Effect.fail("terminal close failed")
      : Effect.sync(() => {
          operations.push("close");
        }),
  );
  const invalidateLocalStatus = vi.fn(() =>
    Effect.sync(() => {
      operations.push("invalidate-status");
    }),
  );
  const runtime = ManagedRuntime.make(
    ThreadDeletionReactorLive.pipe(
      Layer.provideMerge(
        Layer.succeed(OrchestrationEngineService, {
          dispatch,
          dispatchCoordinated: dispatch,
          readEvents: () => Stream.fromIterable(input.persistedEvents ?? []),
          streamDomainEvents: Stream.fromPubSub(events),
        } as never),
      ),
      Layer.provideMerge(
        Layer.succeed(ProjectionSnapshotQuery, {
          getSnapshot: () => Effect.succeed(input.snapshot),
          getThreadShellByIdIncludingArchived: () =>
            Effect.succeed(
              Option.some({
                id: THREAD_ID,
                archivedAt: input.snapshot.threads[0]?.archivedAt ?? null,
                session: null,
              } as never),
            ),
        } as never),
      ),
      Layer.provideMerge(
        Layer.succeed(ProviderService, {
          listSessions: () => Effect.succeed([]),
          stopSession: () => Effect.void,
        } as never),
      ),
      Layer.provideMerge(
        Layer.succeed(TerminalManager.TerminalManager, { close: closeTerminal } as never),
      ),
      Layer.provideMerge(
        Layer.succeed(GitWorkflow.GitWorkflowService, {
          invalidateLocalStatus,
          localStatus: () =>
            Effect.sync(() => {
              operations.push("status");
              return {
                isRepo: true,
                hasWorkingTreeChanges: input.dirty === true,
              } as never;
            }),
          removeWorktree,
          pruneWorktrees,
        } as never),
      ),
      Layer.provideMerge(
        Layer.succeed(FileSystem.FileSystem, {
          exists: () => Effect.succeed(input.pathExists),
        } as never),
      ),
      Layer.provideMerge(WorktreeLifecycleCoordinatorLive),
    ),
  );
  const scope = Effect.runSync(Scope.make("sequential"));
  const reactor = await runtime.runPromise(ThreadDeletionReactor);
  await runtime.runPromise(reactor.start().pipe(Scope.provide(scope)));
  await runtime.runPromise(Effect.yieldNow);
  await runtime.runPromise(Effect.yieldNow);

  return {
    dispatch,
    invalidateLocalStatus,
    operations,
    pruneWorktrees,
    removeWorktree,
    drain: () => runtime.runPromise(reactor.drain),
    sleep: (durationMs: number) => runtime.runPromise(Effect.sleep(durationMs)),
    async dispose() {
      await Effect.runPromise(Scope.close(scope, Exit.void));
      await runtime.dispose();
    },
    async run(event: OrchestrationEvent) {
      await Effect.runPromise(PubSub.publish(events, event));
      await runtime.runPromise(Effect.yieldNow);
      await runtime.runPromise(reactor.drain);
    },
  };
}

describe("ThreadDeletionReactor owned worktree lifecycle", () => {
  it("removes an explicit owned worktree on delete and clears metadata last", async () => {
    const harness = await createHarness({
      snapshot: snapshot({ deleted: true }),
      pathExists: true,
    });
    try {
      await harness.run(lifecycleEvent("thread.deleted"));

      expect(harness.removeWorktree).toHaveBeenCalledWith({
        cwd: PROJECT_ROOT,
        path: WORKTREE_ROOT,
        force: true,
      });
      expect(harness.pruneWorktrees).toHaveBeenCalledWith(PROJECT_ROOT);
      expect(harness.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "thread.meta.update",
          threadId: THREAD_ID,
          worktreePath: null,
          worktreeRemovable: false,
          worktreeRemovalPath: null,
        }),
      );
      expect(harness.operations).toEqual(["close", "remove", "prune", "metadata"]);
    } finally {
      await harness.dispose();
    }
  });

  it("retains a root referenced by any other thread", async () => {
    const harness = await createHarness({
      snapshot: snapshot({ deleted: true, shared: true }),
      pathExists: true,
    });
    try {
      await harness.run(lifecycleEvent("thread.deleted"));

      expect(harness.removeWorktree).not.toHaveBeenCalled();
      expect(harness.pruneWorktrees).not.toHaveBeenCalled();
      expect(harness.dispatch).not.toHaveBeenCalled();
    } finally {
      await harness.dispose();
    }
  });

  it("prunes and clears metadata when the owned root is already absent", async () => {
    const harness = await createHarness({
      snapshot: snapshot({ deleted: true }),
      pathExists: false,
    });
    try {
      await harness.run(lifecycleEvent("thread.deleted"));

      expect(harness.removeWorktree).not.toHaveBeenCalled();
      expect(harness.pruneWorktrees).toHaveBeenCalledWith(PROJECT_ROOT);
      expect(harness.operations.at(-1)).toBe("metadata");
    } finally {
      await harness.dispose();
    }
  });

  it("removes a clean archived owned root without forcing it", async () => {
    const harness = await createHarness({
      snapshot: snapshot({ archived: true }),
      pathExists: true,
    });
    try {
      await harness.run(lifecycleEvent("thread.archived"));

      expect(harness.removeWorktree).toHaveBeenCalledWith({
        cwd: PROJECT_ROOT,
        path: WORKTREE_ROOT,
      });
      expect(harness.invalidateLocalStatus).toHaveBeenCalledWith(WORKTREE_ROOT);
      expect(harness.operations).toEqual([
        "close",
        "invalidate-status",
        "status",
        "remove",
        "prune",
        "metadata",
      ]);
      expect(harness.operations.at(-1)).toBe("metadata");
    } finally {
      await harness.dispose();
    }
  });

  it("retains a dirty archived owned root and its metadata", async () => {
    const harness = await createHarness({
      snapshot: snapshot({ archived: true }),
      pathExists: true,
      dirty: true,
    });
    try {
      await harness.run(lifecycleEvent("thread.archived"));

      expect(harness.removeWorktree).not.toHaveBeenCalled();
      expect(harness.pruneWorktrees).not.toHaveBeenCalled();
      expect(harness.dispatch).not.toHaveBeenCalled();
    } finally {
      await harness.dispose();
    }
  });

  it("does not touch the filesystem when terminal close fails", async () => {
    const harness = await createHarness({
      snapshot: snapshot({ deleted: true }),
      pathExists: true,
      closeFails: true,
    });
    try {
      await harness.run(lifecycleEvent("thread.deleted"));

      expect(harness.removeWorktree).not.toHaveBeenCalled();
      expect(harness.pruneWorktrees).not.toHaveBeenCalled();
      expect(harness.dispatch).not.toHaveBeenCalled();
    } finally {
      await harness.dispose();
    }
  });

  it("replays a committed delete after restart", async () => {
    const deleteEvent = lifecycleEvent("thread.deleted");
    const harness = await createHarness({
      snapshot: snapshot({ deleted: true }),
      pathExists: true,
      persistedEvents: [deleteEvent],
    });
    try {
      await harness.drain();

      expect(harness.removeWorktree).toHaveBeenCalledOnce();
      expect(harness.pruneWorktrees).toHaveBeenCalledOnce();
      expect(harness.dispatch).toHaveBeenCalledOnce();
    } finally {
      await harness.dispose();
    }
  });

  it("retries pruning before clearing ownership metadata", async () => {
    const harness = await createHarness({
      snapshot: snapshot({ deleted: true }),
      pathExists: false,
      pruneFailuresBeforeSuccess: 1,
    });
    try {
      await harness.run(lifecycleEvent("thread.deleted"));
      expect(harness.dispatch).not.toHaveBeenCalled();

      await harness.sleep(400);
      await harness.drain();
      expect(harness.pruneWorktrees).toHaveBeenCalledTimes(2);
      expect(harness.dispatch).toHaveBeenCalledOnce();
      expect(harness.operations.at(-1)).toBe("metadata");
    } finally {
      await harness.dispose();
    }
  });
});
