/* oxlint-disable t3code/no-manual-effect-runtime-in-tests -- The lifecycle worker owns a scoped event subscription that must be drained before assertions. */
import {
  CommandId,
  EventId,
  ProjectId,
  ThreadId,
  type OrchestrationEvent,
  type OrchestrationReadModel,
  type OrchestrationSession,
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
import {
  WorktreeLifecycleCoordinator,
  WorktreeLifecycleCoordinatorLive,
} from "../Services/WorktreeLifecycleCoordinator.ts";
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
  readonly ancestorShared?: boolean;
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
      ...(input.ancestorShared
        ? [
            {
              id: ThreadId.make("thread-worktree-ancestor-consumer"),
              projectId: PROJECT_ID,
              worktreePath: "/worktrees",
              worktreeRemovable: false,
              worktreeRemovalPath: null,
              archivedAt: null,
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

const stoppedSessionEvent = (): OrchestrationEvent =>
  ({
    sequence: 3,
    eventId: EventId.make("event-owned-worktree-stopped"),
    aggregateKind: "thread",
    aggregateId: THREAD_ID,
    type: "thread.session-set",
    occurredAt: NOW,
    commandId: CommandId.make("command-owned-worktree-stopped"),
    causationEventId: null,
    correlationId: CommandId.make("command-owned-worktree-stopped"),
    metadata: {},
    payload: {
      threadId: THREAD_ID,
      session: {
        threadId: THREAD_ID,
        status: "stopped",
        providerName: null,
        runtimeMode: "full-access",
        activeTurnId: null,
        lastError: null,
        updatedAt: NOW,
      },
    },
  }) as OrchestrationEvent;

async function createHarness(input: {
  readonly snapshot: OrchestrationReadModel;
  readonly pathExists: boolean;
  readonly closeFails?: boolean;
  readonly dirty?: boolean;
  readonly ignored?: boolean;
  readonly metadataFailuresBeforeSuccess?: number;
  readonly persistedEvents?: ReadonlyArray<OrchestrationEvent>;
  readonly projectedSessionStatus?: OrchestrationSession["status"];
  readonly projectIsRepo?: boolean;
  readonly projectRootExists?: boolean;
  readonly pruneFailuresBeforeSuccess?: number;
  readonly removeFails?: boolean;
}) {
  const events = Effect.runSync(PubSub.unbounded<OrchestrationEvent>());
  const operations: string[] = [];
  const removeWorktree = vi.fn(() =>
    input.removeFails === true
      ? Effect.fail("worktree removal failed")
      : Effect.sync(() => {
          operations.push("remove");
        }),
  );
  let pruneAttempts = 0;
  const pruneWorktrees = vi.fn(() => {
    pruneAttempts += 1;
    if (input.projectRootExists === false || input.projectIsRepo === false) {
      return Effect.fail("owning project repository is unavailable");
    }
    return pruneAttempts <= (input.pruneFailuresBeforeSuccess ?? 0)
      ? Effect.fail("worktree prune failed")
      : Effect.sync(() => {
          operations.push("prune");
        });
  });
  let metadataAttempts = 0;
  const dispatch = vi.fn((command: { readonly type: string; readonly commandId?: string }) => {
    if (command.type === "thread.meta.update") {
      metadataAttempts += 1;
      if (metadataAttempts <= (input.metadataFailuresBeforeSuccess ?? 0)) {
        return Effect.fail("metadata clear failed");
      }
    }
    return Effect.sync(() => {
      operations.push(command.type === "thread.meta.update" ? "metadata" : command.type);
      return { sequence: 3 };
    });
  });
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
                session: input.projectedSessionStatus
                  ? {
                      threadId: THREAD_ID,
                      status: input.projectedSessionStatus,
                      providerName: "codex",
                      runtimeMode: "full-access",
                      activeTurnId: null,
                      lastError: null,
                      updatedAt: NOW,
                    }
                  : null,
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
          hasIgnoredFiles: () => Effect.succeed(input.ignored === true),
          localStatus: ({ cwd }: { readonly cwd: string }) =>
            Effect.sync(() => {
              operations.push("status");
              return {
                isRepo: cwd === PROJECT_ROOT ? input.projectIsRepo !== false : true,
                hasWorkingTreeChanges: cwd === WORKTREE_ROOT && input.dirty === true,
              } as never;
            }),
          removeWorktree,
          pruneWorktrees,
        } as never),
      ),
      Layer.provideMerge(
        Layer.succeed(FileSystem.FileSystem, {
          exists: (path: string) =>
            Effect.succeed(
              path === WORKTREE_ROOT ? input.pathExists : input.projectRootExists !== false,
            ),
        } as never),
      ),
      Layer.provideMerge(WorktreeLifecycleCoordinatorLive),
    ),
  );
  const scope = Effect.runSync(Scope.make("sequential"));
  const reactor = await runtime.runPromise(ThreadDeletionReactor);
  const worktreeLifecycle = await runtime.runPromise(WorktreeLifecycleCoordinator);
  await runtime.runPromise(reactor.start().pipe(Scope.provide(scope)));
  await runtime.runPromise(Effect.yieldNow);
  await runtime.runPromise(Effect.yieldNow);

  return {
    closeTerminal,
    dispatch,
    invalidateLocalStatus,
    isTeardownPending: () => runtime.runPromise(worktreeLifecycle.isTeardownPending(THREAD_ID)),
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
        expect.objectContaining({
          kind: "trusted-system",
          reason: "ThreadDeletionReactor",
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

  it("retains an owned root nested inside another thread workspace", async () => {
    const harness = await createHarness({
      snapshot: snapshot({ deleted: true, ancestorShared: true }),
      pathExists: true,
    });
    try {
      await harness.run(lifecycleEvent("thread.deleted"));

      expect(harness.removeWorktree).not.toHaveBeenCalled();
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

  it("clears ownership when the worktree and owning repository are both gone", async () => {
    for (const projectState of [
      { projectRootExists: false },
      { projectRootExists: true, projectIsRepo: false },
    ]) {
      const harness = await createHarness({
        snapshot: snapshot({ deleted: true }),
        pathExists: false,
        ...projectState,
      });
      try {
        await harness.run(lifecycleEvent("thread.deleted"));

        expect(harness.pruneWorktrees).not.toHaveBeenCalled();
        expect(harness.dispatch).toHaveBeenCalledOnce();
        expect(harness.operations.at(-1)).toBe("metadata");
      } finally {
        await harness.dispose();
      }
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

  it("waits for a projected live archive session to stop before removal", async () => {
    const harness = await createHarness({
      snapshot: snapshot({ archived: true }),
      pathExists: true,
      projectedSessionStatus: "ready",
    });
    try {
      await harness.run(lifecycleEvent("thread.archived"));

      expect(harness.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({ type: "thread.session.stop", threadId: THREAD_ID }),
        expect.objectContaining({
          kind: "trusted-system",
          reason: "ThreadDeletionReactor",
        }),
      );
      expect(harness.removeWorktree).not.toHaveBeenCalled();

      await harness.sleep(400);
      await harness.drain();
      expect(harness.dispatch.mock.calls).toEqual(
        expect.arrayContaining([
          [
            expect.objectContaining({ type: "thread.session.stop", threadId: THREAD_ID }),
            expect.objectContaining({
              kind: "trusted-system",
              reason: "ThreadDeletionReactor",
            }),
          ],
          [
            expect.objectContaining({ type: "thread.session.stop", threadId: THREAD_ID }),
            expect.objectContaining({
              kind: "trusted-system",
              reason: "ThreadDeletionReactor",
            }),
          ],
        ]),
      );
      const stopCommandIds = harness.dispatch.mock.calls
        .map(([command]) => command)
        .filter((command) => command.type === "thread.session.stop")
        .map((command) => command.commandId);
      expect(new Set(stopCommandIds).size).toBe(stopCommandIds.length);
      expect(harness.removeWorktree).not.toHaveBeenCalled();
    } finally {
      await harness.dispose();
    }
  });

  it("removes an archived owned root whose projected session is already errored", async () => {
    const harness = await createHarness({
      snapshot: snapshot({ archived: true }),
      pathExists: true,
      projectedSessionStatus: "error",
    });
    try {
      await harness.run(lifecycleEvent("thread.archived"));

      expect(harness.dispatch).not.toHaveBeenCalledWith(
        expect.objectContaining({ type: "thread.session.stop" }),
      );
      expect(harness.removeWorktree).toHaveBeenCalledOnce();
    } finally {
      await harness.dispose();
    }
  });

  it("deletes terminal history when a stopped event finishes deleted-thread teardown", async () => {
    const harness = await createHarness({
      snapshot: snapshot({ deleted: true }),
      pathExists: false,
    });
    try {
      await harness.run(stoppedSessionEvent());

      expect(harness.closeTerminal).toHaveBeenCalledWith({
        threadId: THREAD_ID,
        deleteHistory: true,
      });
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

  it("retains an archived owned root containing only ignored files", async () => {
    const harness = await createHarness({
      snapshot: snapshot({ archived: true }),
      pathExists: true,
      ignored: true,
    });
    try {
      await harness.run(lifecycleEvent("thread.archived"));

      expect(harness.removeWorktree).not.toHaveBeenCalled();
      expect(harness.dispatch).not.toHaveBeenCalled();
      expect(await harness.isTeardownPending()).toBe(false);
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

  it("keeps archived teardown pending until ownership metadata clears", async () => {
    const harness = await createHarness({
      snapshot: snapshot({ archived: true }),
      pathExists: true,
      metadataFailuresBeforeSuccess: 1,
    });
    try {
      await harness.run(lifecycleEvent("thread.archived"));
      expect(await harness.isTeardownPending()).toBe(true);

      await harness.sleep(400);
      await harness.drain();
      expect(await harness.isTeardownPending()).toBe(false);
    } finally {
      await harness.dispose();
    }
  });

  it("clears archive teardown pending when removal fails and the root remains", async () => {
    const harness = await createHarness({
      snapshot: snapshot({ archived: true }),
      pathExists: true,
      removeFails: true,
    });
    try {
      await harness.run(lifecycleEvent("thread.archived"));

      expect(harness.removeWorktree).toHaveBeenCalledOnce();
      expect(await harness.isTeardownPending()).toBe(false);
    } finally {
      await harness.dispose();
    }
  });
});
