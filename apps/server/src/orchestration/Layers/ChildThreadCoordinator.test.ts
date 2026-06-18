import {
  CommandId,
  EventId,
  MessageId,
  ProviderInstanceId,
  ProjectId,
  ThreadId,
  TurnId,
  type ModelSelection,
  type OrchestrationCommand,
  type OrchestrationEvent,
  type OrchestrationLatestTurn,
  type OrchestrationSession,
  type OrchestrationThread,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import { afterEach, describe, expect, it } from "vite-plus/test";

import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { PendingDispatchRepositoryLive } from "../../persistence/Layers/PendingDispatches.ts";
import {
  PendingDispatchId,
  PendingDispatchRepository,
  type PendingDispatch,
} from "../../persistence/Services/PendingDispatches.ts";
import { ProviderInstanceRegistry } from "../../provider/Services/ProviderInstanceRegistry.ts";
import {
  ActiveBootstrapTurnStartDispatcherLive,
  BootstrapTurnStartDispatcher,
} from "../Services/BootstrapTurnStartDispatcher.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  ChildThreadCoordinator,
  MAX_DEPTH,
  WAIT_SLICE_SECONDS,
} from "../Services/ChildThreadCoordinator.ts";
import { ChildThreadCoordinatorLive } from "./ChildThreadCoordinator.ts";

const now = "2026-06-17T10:00:00.000Z";
const projectId = ProjectId.make("project-coordinator-test");
const codexModel: ModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5-codex",
};

const unsupported = () => Effect.die(new Error("Unsupported projection call in test")) as never;

// Logical wait budget deadlines (epoch ms) compared against the coordinator's
// Clock. A far-future deadline keeps the budget open; a past deadline forces
// pending children to come back as `timeout`.
const FAR_FUTURE_MS = 4_000_000_000_000;
const PAST_MS = 0;

interface ThreadState {
  readonly shell: OrchestrationThreadShell;
  readonly detail: OrchestrationThread;
}

const makeLatestTurn = (
  state: OrchestrationLatestTurn["state"],
): OrchestrationLatestTurn => ({
  turnId: TurnId.make("turn-1"),
  state,
  requestedAt: now,
  startedAt: now,
  completedAt: state === "completed" ? now : null,
  assistantMessageId: null,
});

const makeSession = (
  threadId: ThreadId,
  status: OrchestrationSession["status"],
  activeTurnId: TurnId | null = null,
): OrchestrationSession => ({
  threadId,
  status,
  providerName: "codex",
  runtimeMode: "full-access",
  activeTurnId,
  lastError: null,
  updatedAt: now,
});

const makeThreadState = (input: {
  readonly threadId: ThreadId;
  readonly parentThreadId?: ThreadId | null;
  readonly latestTurn?: OrchestrationLatestTurn | null;
  readonly session?: OrchestrationSession | null;
  readonly assistantText?: string | null;
}): ThreadState => {
  const latestTurn = input.latestTurn ?? null;
  const session = input.session ?? null;
  const shell: OrchestrationThreadShell = {
    id: input.threadId,
    projectId,
    title: `Thread ${input.threadId}`,
    modelSelection: codexModel,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    latestTurn,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    session,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    parentThreadId: input.parentThreadId ?? null,
  };
  const messages =
    input.assistantText != null
      ? [
          {
            id: MessageId.make(`msg-${input.threadId}`),
            role: "assistant" as const,
            text: input.assistantText,
            turnId: latestTurn?.turnId ?? null,
            streaming: false,
            createdAt: now,
            updatedAt: now,
          },
        ]
      : [];
  const detail: OrchestrationThread = {
    id: input.threadId,
    projectId,
    title: `Thread ${input.threadId}`,
    modelSelection: codexModel,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    latestTurn,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    deletedAt: null,
    messages,
    proposedPlans: [],
    activities: [],
    checkpoints: [],
    session,
  };
  return { shell, detail };
};

const turnDiffEvent = (
  threadId: ThreadId,
  status: "ready" | "missing" | "error",
): OrchestrationEvent =>
  ({
    eventId: EventId.make(`evt-diff-${threadId}-${status}`),
    type: "thread.turn-diff-completed",
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: now,
    payload: {
      threadId,
      turnId: TurnId.make("turn-1"),
      checkpointTurnCount: 1,
      checkpointRef: `thread:${threadId}:turn:1`,
      status,
      files: [],
      assistantMessageId: null,
      completedAt: now,
    },
  }) as unknown as OrchestrationEvent;

const sessionSetEvent = (
  threadId: ThreadId,
  status: OrchestrationSession["status"],
): OrchestrationEvent =>
  ({
    eventId: EventId.make(`evt-session-${threadId}-${status}`),
    type: "thread.session-set",
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: now,
    payload: { threadId, session: makeSession(threadId, status) },
  }) as unknown as OrchestrationEvent;

const threadDeletedEvent = (threadId: ThreadId): OrchestrationEvent =>
  ({
    eventId: EventId.make(`evt-deleted-${threadId}`),
    type: "thread.deleted",
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: now,
    payload: { threadId, deletedAt: now },
  }) as unknown as OrchestrationEvent;

describe("ChildThreadCoordinator", () => {
  let runtime: ManagedRuntime.ManagedRuntime<
    ChildThreadCoordinator | SqlClient | PendingDispatchRepository,
    unknown
  > | null = null;
  let scope: Scope.Closeable | null = null;

  afterEach(async () => {
    if (scope) {
      await Effect.runPromise(Scope.close(scope, Exit.void));
      scope = null;
    }
    if (runtime) {
      await runtime.dispose();
      runtime = null;
    }
  });

  async function createHarness(input?: {
    readonly threads?: ReadonlyArray<ThreadState>;
    readonly persistedEvents?: ReadonlyArray<OrchestrationEvent>;
    readonly knownInstances?: ReadonlyArray<string>;
    readonly seedChildRows?: ReadonlyArray<{
      readonly threadId: ThreadId;
      readonly parentThreadId: ThreadId;
    }>;
    /** Pending_dispatches rows inserted BEFORE start() (simulated restart). */
    readonly seedPendingDispatches?: ReadonlyArray<PendingDispatch>;
    /** Thread ids whose `getThreadShellById` read sleeps far longer than a slice. */
    readonly slowThreadShellIds?: ReadonlyArray<ThreadId>;
    /**
     * Invoked synchronously whenever a `thread.turn.start` command is dispatched.
     * Used by the re-entrancy test to enqueue the parent's terminal signal while
     * the parent-wake lock is still held, proving the lock is not deadlocked.
     */
    readonly onTurnStartDispatch?: (
      command: Extract<OrchestrationCommand, { readonly type: "thread.turn.start" }>,
      enqueue: (event: OrchestrationEvent) => void,
    ) => void;
  }) {
    const threadStates = new Map<ThreadId, ThreadState>();
    for (const state of input?.threads ?? []) {
      threadStates.set(state.shell.id, state);
    }
    const dispatched: Array<OrchestrationCommand> = [];
    const knownInstances = new Set(input?.knownInstances ?? ["codex"]);
    const slowShellIds = new Set((input?.slowThreadShellIds ?? []).map((id) => String(id)));

    const eventQueue = Effect.runSync(Queue.unbounded<OrchestrationEvent>());
    const enqueueSync = (event: OrchestrationEvent) =>
      Effect.runSync(Queue.offer(eventQueue, event));

    const recordDispatch = (command: OrchestrationCommand) => {
      dispatched.push(command);
      if (command.type === "thread.turn.start") {
        input?.onTurnStartDispatch?.(command, enqueueSync);
      }
      return { sequence: dispatched.length };
    };

    const engineLayer = Layer.succeed(
      OrchestrationEngineService,
      OrchestrationEngineService.of({
        readEvents: () => Stream.fromIterable(input?.persistedEvents ?? []),
        streamDomainEvents: Stream.fromQueue(eventQueue),
        dispatch: (command) => Effect.sync(() => recordDispatch(command)),
      }),
    );

    const projectionLayer = Layer.succeed(ProjectionSnapshotQuery, {
      getCommandReadModel: () => unsupported(),
      getSnapshot: () => unsupported(),
      getShellSnapshot: () => unsupported(),
      getArchivedShellSnapshot: () => unsupported(),
      getSnapshotSequence: () => unsupported(),
      getCounts: () => unsupported(),
      getActiveProjectByWorkspaceRoot: () => unsupported(),
      getProjectShellById: () => unsupported(),
      getFirstActiveThreadIdByProjectId: () => unsupported(),
      getThreadCheckpointContext: () => unsupported(),
      getFullThreadDiffContext: () => unsupported(),
      getThreadShellById: (threadId) =>
        Effect.gen(function* () {
          if (slowShellIds.has(String(threadId))) {
            // Simulate a stalled projection read far longer than a wait slice.
            yield* Effect.sleep(`${WAIT_SLICE_SECONDS * 3} seconds`);
          }
          const state = threadStates.get(threadId);
          return state ? Option.some(state.shell) : Option.none();
        }),
      getThreadDetailById: (threadId) =>
        Effect.sync(() => {
          const state = threadStates.get(threadId);
          return state ? Option.some(state.detail) : Option.none();
        }),
    });

    const registryLayer = Layer.succeed(ProviderInstanceRegistry, {
      getInstance: (instanceId) =>
        Effect.succeed(
          knownInstances.has(String(instanceId))
            ? ({ instanceId } as never)
            : undefined,
        ),
      listInstances: Effect.succeed([]),
      listUnavailable: Effect.succeed([]),
      streamChanges: Stream.empty,
      subscribeChanges: unsupported(),
    });

    // Fake bootstrap dispatcher + global-capture so `dispatchActive` resolves
    // and records turn-start commands the same way the real server does.
    const dispatcherLayer = Layer.succeed(BootstrapTurnStartDispatcher, {
      dispatch: (command) => Effect.sync(() => recordDispatch(command)),
    });
    const activeDispatcherLayer = ActiveBootstrapTurnStartDispatcherLive.pipe(
      Layer.provide(dispatcherLayer),
    );

    const layer = ChildThreadCoordinatorLive.pipe(
      Layer.provideMerge(PendingDispatchRepositoryLive),
      Layer.provideMerge(engineLayer),
      Layer.provideMerge(projectionLayer),
      Layer.provideMerge(registryLayer),
      Layer.provideMerge(activeDispatcherLayer),
      Layer.provideMerge(SqlitePersistenceMemory),
      Layer.provideMerge(NodeServices.layer),
    );

    runtime = ManagedRuntime.make(layer);

    const activeRuntime = runtime;
    if (input?.seedChildRows) {
      for (const row of input.seedChildRows) {
        await activeRuntime.runPromise(
          Effect.flatMap(
            Effect.service(SqlClient),
            (sql) =>
              sql`
                INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode, created_at, updated_at, parent_thread_id)
                VALUES (${row.threadId}, ${projectId}, ${"seed"}, ${"{}"}, ${"full-access"}, ${"default"}, ${now}, ${now}, ${row.parentThreadId})
              `,
          ),
        );
      }
    }

    // Pre-populate pending_dispatches BEFORE start() so reconciliation reloads
    // them (simulated restart).
    if (input?.seedPendingDispatches) {
      for (const row of input.seedPendingDispatches) {
        await activeRuntime.runPromise(
          Effect.flatMap(Effect.service(PendingDispatchRepository), (repo) => repo.insert(row)),
        );
      }
    }

    const coordinator = await activeRuntime.runPromise(Effect.service(ChildThreadCoordinator));
    scope = await Effect.runPromise(Scope.make("sequential"));
    await Effect.runPromise(coordinator.start().pipe(Scope.provide(scope)));

    const setThread = (state: ThreadState) => threadStates.set(state.shell.id, state);

    // Simulate a deleted thread: subsequent getThreadShellById reads return none.
    const removeThread = (threadId: ThreadId) => threadStates.delete(threadId);

    const listPendingDispatches = () =>
      activeRuntime.runPromise(
        Effect.flatMap(Effect.service(PendingDispatchRepository), (repo) => repo.listAll()),
      );

    const insertPendingDispatch = (row: PendingDispatch) =>
      activeRuntime.runPromise(
        Effect.flatMap(Effect.service(PendingDispatchRepository), (repo) => repo.insert(row)),
      );

    // Offer an event, then wait until the coordinator has finished processing it.
    const feed = async (event: OrchestrationEvent) => {
      await Effect.runPromise(Queue.offer(eventQueue, event));
      // Let the forked hot-stream fiber pull and enqueue, then wait for the worker.
      for (let i = 0; i < 50; i += 1) {
        await Effect.runPromise(Effect.yieldNow);
      }
      await Effect.runPromise(coordinator.drain);
    };

    const register = (registerInput: Parameters<typeof coordinator.register>[0]) =>
      Effect.runPromise(coordinator.register(registerInput));

    return {
      coordinator,
      dispatched,
      setThread,
      removeThread,
      feed,
      register,
      listPendingDispatches,
      insertPendingDispatch,
    };
  }

  it("settles ready turn-diff as completed and captures final assistant text", async () => {
    const child = ThreadId.make("child-completed");
    const parent = ThreadId.make("parent-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "all done",
        }),
      ],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    await harness.feed(turnDiffEvent(child, "ready"));
    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("all done");
  });

  it("settles error turn-diff as failed", async () => {
    const child = ThreadId.make("child-error");
    const parent = ThreadId.make("parent-2");
    const harness = await createHarness({
      threads: [makeThreadState({ threadId: child, parentThreadId: parent })],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    await harness.feed(turnDiffEvent(child, "error"));
    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("failed");
  });

  it("settles missing turn-diff as failed", async () => {
    const child = ThreadId.make("child-missing");
    const parent = ThreadId.make("parent-3");
    const harness = await createHarness({
      threads: [makeThreadState({ threadId: child, parentThreadId: parent })],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    await harness.feed(turnDiffEvent(child, "missing"));
    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("failed");
  });

  it("settles session-set stopped (idle) as failed", async () => {
    const child = ThreadId.make("child-stopped");
    const parent = ThreadId.make("parent-4");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("interrupted"),
          session: makeSession(child, "stopped"),
        }),
      ],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    await harness.feed(sessionSetEvent(child, "stopped"));
    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("failed");
  });

  it("does NOT settle session-set stopped while the latest turn is still running", async () => {
    const child = ThreadId.make("child-stopped-running");
    const parent = ThreadId.make("parent-4b");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "stopped"),
        }),
      ],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    await harness.feed(sessionSetEvent(child, "stopped"));
    const slice = await runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
    expect(slice.results[0]!.status).toBe("pending");
  });

  it("settles thread.deleted as killed", async () => {
    const child = ThreadId.make("child-deleted");
    const parent = ThreadId.make("parent-5");
    const harness = await createHarness({
      threads: [makeThreadState({ threadId: child, parentThreadId: parent })],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    await harness.feed(threadDeletedEvent(child));
    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("killed");
  });

  it("settles synchronously on register when the projection already shows the child terminal (hot-subscribe race)", async () => {
    const child = ThreadId.make("child-already-done");
    const parent = ThreadId.make("parent-6");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "finished before subscribe",
        }),
      ],
    });
    // No event is ever fed; register must settle from the synchronous one-shot check.
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("finished before subscribe");
  });

  it("waitSlice never exceeds the slice duration for a running child", async () => {
    const child = ThreadId.make("child-running");
    const parent = ThreadId.make("parent-7");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
        }),
      ],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    const startedMs = await Effect.runPromise(Clock.currentTimeMillis);
    const slice = await runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
    const elapsedMs = (await Effect.runPromise(Clock.currentTimeMillis)) - startedMs;
    expect(slice.results[0]!.status).toBe("pending");
    expect(slice.pending).toBe(true);
    expect(elapsedMs).toBeLessThan((WAIT_SLICE_SECONDS + 5) * 1_000);
  });

  it("waitSlice returns a terminal error row for an unknown childId (never hangs)", async () => {
    const harness = await createHarness({ threads: [] });
    const unknown = ThreadId.make("never-registered");
    const slice = await runtimeWaitSlice(harness, [unknown], FAR_FUTURE_MS);
    expect(slice.results).toHaveLength(1);
    expect(slice.results[0]!.status).toBe("failed");
    expect(slice.results[0]!.error).toContain("never registered");
  });

  it("never dispatches a resume to a turnCount-0 (fresh) detached parent (bug #2336)", async () => {
    const child = ThreadId.make("child-fresh-parent");
    const parent = ThreadId.make("parent-fresh");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "done",
        }),
        makeThreadState({ threadId: parent, latestTurn: null, session: null }),
      ],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: true,
      model: codexModel,
      spawnedAtMs: 0,
    });
    await harness.feed(turnDiffEvent(child, "ready"));
    const turnStarts = harness.dispatched.filter((command) => command.type === "thread.turn.start");
    expect(turnStarts).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(true);
  });

  it("wakes an IDLE detached parent with a consolidated turn dispatch", async () => {
    const child = ThreadId.make("child-wake");
    const parent = ThreadId.make("parent-idle");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "child result",
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(parent, "ready"),
        }),
      ],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: true,
      model: codexModel,
      spawnedAtMs: 0,
    });
    await harness.feed(turnDiffEvent(child, "ready"));
    const turnStarts = harness.dispatched.filter((command) => command.type === "thread.turn.start");
    expect(turnStarts).toHaveLength(1);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("rejects spawn beyond the depth cap", async () => {
    const harness = await createHarness({ threads: [] });
    let parent = ThreadId.make("depth-root");
    // Build a chain root -> c1 -> ... up to MAX_DEPTH-1 (all accepted).
    for (let depth = 1; depth < MAX_DEPTH; depth += 1) {
      const childThreadId = ThreadId.make(`depth-child-${depth}`);
      await harness.register({
        parentThreadId: parent,
        childThreadId,
        detached: false,
        model: codexModel,
        spawnedAtMs: 0,
      });
      parent = childThreadId;
    }
    // The next spawn would be at depth MAX_DEPTH -> rejected.
    const exit = await Effect.runPromiseExit(
      harness.coordinator.register({
        parentThreadId: parent,
        childThreadId: ThreadId.make("depth-too-deep"),
        detached: false,
        model: codexModel,
        spawnedAtMs: 0,
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
  });

  it("rejects a spawn that would create an ancestry cycle", async () => {
    const harness = await createHarness({ threads: [] });
    const a = ThreadId.make("cycle-a");
    const b = ThreadId.make("cycle-b");
    await harness.register({
      parentThreadId: a,
      childThreadId: b,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    // Now try to make `a` a child of `b` -> cycle.
    const exit = await Effect.runPromiseExit(
      harness.coordinator.register({
        parentThreadId: b,
        childThreadId: a,
        detached: false,
        model: codexModel,
        spawnedAtMs: 0,
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
  });

  it("fans out: one child never finishes (pending/timeout) while others settle", async () => {
    const parent = ThreadId.make("parent-fanout");
    const settled = ThreadId.make("fanout-settled");
    const running = ThreadId.make("fanout-running");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: settled,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "ok",
        }),
        makeThreadState({
          threadId: running,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(running, "running", TurnId.make("turn-1")),
        }),
      ],
    });
    for (const childThreadId of [settled, running]) {
      await harness.register({
        parentThreadId: parent,
        childThreadId,
        detached: false,
        model: codexModel,
        spawnedAtMs: 0,
      });
    }
    await harness.feed(turnDiffEvent(settled, "ready"));
    // Budget already exhausted -> the running child should come back as timeout.
    const slice = await runtimeWaitSlice(harness, [settled, running], PAST_MS);
    const byId = new Map(slice.results.map((result) => [result.childThreadId, result] as const));
    expect(byId.get(settled)!.status).toBe("completed");
    expect(byId.get(running)!.status).toBe("timeout");
    expect(slice.settledCount).toBe(1);
    expect(slice.timedOutCount).toBe(1);
  });

  it("is idempotent under a double terminal signal", async () => {
    const child = ThreadId.make("child-double");
    const parent = ThreadId.make("parent-double");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "first",
        }),
      ],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    await harness.feed(turnDiffEvent(child, "ready"));
    // A second, conflicting signal must not change the already-settled result.
    await harness.feed(turnDiffEvent(child, "error"));
    await harness.feed(threadDeletedEvent(child));
    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("first");
  });

  it("reconciles terminal-ness from the persisted log before forking the hot stream", async () => {
    const child = ThreadId.make("recon-child");
    const parent = ThreadId.make("recon-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "reconciled",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [turnDiffEvent(child, "ready")],
    });
    // The child was reconciled at start(); waitSlice must return it terminal.
    const slice = await runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
    expect(slice.results[0]!.status).toBe("completed");
    expect(slice.results[0]!.finalAssistantText).toBe("reconciled");
  });

  it("settles a reconciled non-terminal child as killed when its provider instance is gone", async () => {
    const child = ThreadId.make("recon-orphan");
    const parent = ThreadId.make("recon-orphan-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [],
      // No persisted terminal signal -> the child reconciles as non-terminal,
      // and with no available provider instance it is settled killed.
      knownInstances: [],
    });
    const slice = await runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
    expect(slice.results[0]!.status).toBe("killed");
  });

  it("waitSlice returns within the slice bound even when the projection read is slow (never-hang)", async () => {
    const child = ThreadId.make("child-slow-projection");
    const parent = ThreadId.make("parent-slow-projection");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
        }),
      ],
      // The one-shot terminal check inside waitSlice must not block on this.
      slowThreadShellIds: [child],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    const startedMs = await Effect.runPromise(Clock.currentTimeMillis);
    const slice = await runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
    const elapsedMs = (await Effect.runPromise(Clock.currentTimeMillis)) - startedMs;
    expect(slice.results[0]!.status).toBe("pending");
    // A stalled projection (3x slice) must not extend the wait past the slice.
    expect(elapsedMs).toBeLessThan((WAIT_SLICE_SECONDS + 5) * 1_000);
  });

  it("does not deadlock when a synchronous parent dispatch re-enters the wake lock", async () => {
    const child = ThreadId.make("child-sync-dispatch");
    const parent = ThreadId.make("parent-sync-dispatch");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "child output",
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(parent, "ready"),
        }),
      ],
      // While wakeParent holds the per-parent lock and dispatches the parent's
      // turn, synchronously publish the parent's own turn-diff-completed event.
      // If the lock were re-entrant/blocking on the same fiber this would
      // deadlock; the worker fiber drains it only after the lock is released.
      onTurnStartDispatch: (command, enqueue) => {
        if (command.threadId === parent) {
          enqueue(turnDiffEvent(parent, "ready"));
        }
      },
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: true,
      model: codexModel,
      spawnedAtMs: 0,
    });
    // Must complete (not hang) within the test runner timeout.
    await harness.feed(turnDiffEvent(child, "ready"));
    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start",
    );
    expect(turnStarts.length).toBeGreaterThanOrEqual(1);
    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
  });

  it("does NOT kill a reconciled child that has no detail row (projection lag)", async () => {
    const child = ThreadId.make("recon-no-detail");
    const parent = ThreadId.make("recon-no-detail-parent");
    const harness = await createHarness({
      // The child row is seeded in projection_threads, but NO thread detail/shell
      // state is registered -> getThreadDetailById returns None at reconcile time.
      threads: [],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [],
      knownInstances: [],
    });
    // The child was skipped during reconciliation (not tracked, not killed); a
    // wait for it reports the unknown-thread terminal error, never "killed".
    const slice = await runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
    expect(slice.results[0]!.status).toBe("failed");
    expect(slice.results[0]!.error).toContain("never registered");
  });

  it("R-A: a promoted child whose waiter stopped wakes the parent on completion", async () => {
    const child = ThreadId.make("promote-child");
    const parent = ThreadId.make("promote-parent");
    const harness = await createHarness({
      threads: [
        // Child starts running; it only completes after the wait was abandoned.
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
        }),
        // Idle parent so the wake dispatches a turn (not just enqueues).
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(parent, "ready"),
        }),
      ],
    });
    // Foreground (non-detached) child: completion would normally only resolve the
    // waiter, NOT wake the parent.
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    // The wait budget elapsed -> the waiter stopped -> promote to wake.
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));
    // Now the child completes; the promotion must wake the parent.
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        assistantText: "promoted result",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready"));
    const turnStarts = harness.dispatched.filter((command) => command.type === "thread.turn.start");
    expect(turnStarts).toHaveLength(1);
    // The durable wake row was deleted on dispatch (idle parent path).
    expect(await harness.listPendingDispatches()).toHaveLength(0);
  });

  it("R-B: a wake enqueued mid-turn persists a durable row, then drains on parent idle and deletes it", async () => {
    const child = ThreadId.make("durable-child");
    const parent = ThreadId.make("durable-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "durable result",
        }),
        // Parent is MID-TURN -> the wake must enqueue (and persist a durable row),
        // not dispatch.
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(parent, "running", TurnId.make("turn-9")),
        }),
      ],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: true,
      model: codexModel,
      spawnedAtMs: 0,
    });
    await harness.feed(turnDiffEvent(child, "ready"));
    // Mid-turn -> no dispatch yet, but a durable parent_injection row exists.
    expect(harness.dispatched.filter((c) => c.type === "thread.turn.start")).toHaveLength(0);
    const persisted = await harness.listPendingDispatches();
    expect(persisted).toHaveLength(1);
    expect(persisted[0]!.kind).toBe("parent_injection");
    expect(persisted[0]!.targetThreadId).toBe(parent);
    expect(persisted[0]!.sourceChildId).toBe(child);

    // The parent goes idle and completes a turn -> drain dispatches once and
    // deletes the row.
    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("completed"),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready"));
    expect(harness.dispatched.filter((c) => c.type === "thread.turn.start")).toHaveLength(1);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
  });

  it("R-B: a pre-populated durable row is reloaded on restart and drained exactly once on parent idle", async () => {
    const child = ThreadId.make("restart-child");
    const parent = ThreadId.make("restart-parent");
    const dispatchId = PendingDispatchId.make("pd-restart-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(parent, "ready"),
        }),
      ],
      // Simulated restart: a parent_injection row was persisted before the
      // process died, and must be reloaded by start().
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "reloaded child result",
          error: null,
          status: "completed",
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });
    // The reload made the parent's injection pending.
    expect(await runtimeHasPending(harness, parent)).toBe(true);
    // Parent completes a turn (idle) -> the reloaded row drains exactly once.
    await harness.feed(turnDiffEvent(parent, "ready"));
    const turnStarts = harness.dispatched.filter((c) => c.type === "thread.turn.start");
    expect(turnStarts).toHaveLength(1);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    // A second parent turn-completion must NOT re-fire the already-drained row.
    await harness.feed(turnDiffEvent(parent, "ready"));
    expect(harness.dispatched.filter((c) => c.type === "thread.turn.start")).toHaveLength(1);
  });

  it("R-B: a durable injection for a deleted parent is dropped (no orphaned row, no double-fire)", async () => {
    const child = ThreadId.make("orphan-child");
    const parent = ThreadId.make("orphan-parent");
    const dispatchId = PendingDispatchId.make("pd-orphan-1");
    // Restart with a durable parent_injection row whose parent thread no longer
    // exists (it was deleted before the crash). It must NOT linger forever.
    const harness = await createHarness({
      threads: [],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "result for a parent that is gone",
          error: null,
          status: "completed",
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });
    // The reload made the orphaned injection pending in memory.
    expect(await runtimeHasPending(harness, parent)).toBe(true);
    // A drain attempt for the missing parent must delete the orphaned row (so a
    // restart never re-loads it) and dispatch nothing.
    await harness.feed(turnDiffEvent(parent, "ready"));
    expect(harness.dispatched.filter((c) => c.type === "thread.turn.start")).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
  });

  it("R-C: a persisted child_steer drains exactly once when the child goes idle", async () => {
    const child = ThreadId.make("steer-child");
    const parent = ThreadId.make("steer-parent");
    const harness = await createHarness({
      threads: [
        // Child is idle (a turn just completed) so the deferred steer can fire.
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(child, "ready"),
        }),
        // Idle parent so the child's own completion-wake dispatches-and-deletes
        // its row, leaving the pending_dispatches table clean for the steer assert.
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(parent, "ready"),
        }),
      ],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: true,
      model: codexModel,
      spawnedAtMs: 0,
    });
    // A provider-deferred steer (I3 enqueues this; here we persist it directly).
    await harness.insertPendingDispatch({
      id: PendingDispatchId.make("pd-steer-1"),
      kind: "child_steer",
      targetThreadId: child,
      sourceChildId: null,
      text: "do the next thing",
      error: null,
      status: null,
      createdAt: now as unknown as PendingDispatch["createdAt"],
    });
    // The child going idle (turn-diff-completed) drains the steer once.
    await harness.feed(turnDiffEvent(child, "ready"));
    const steerStarts = harness.dispatched.filter(
      (c) => c.type === "thread.turn.start" && c.threadId === child,
    );
    expect(steerStarts).toHaveLength(1);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    // A second idle transition must NOT re-dispatch the already-drained steer.
    await harness.feed(turnDiffEvent(child, "ready"));
    expect(
      harness.dispatched.filter((c) => c.type === "thread.turn.start" && c.threadId === child),
    ).toHaveLength(1);
  });
});

// Helpers that run coordinator effects on the harness runtime.
async function runtimeRun(
  harness: { readonly coordinator: import("../Services/ChildThreadCoordinator.ts").ChildThreadCoordinatorShape },
  child: ThreadId,
) {
  const slice = await runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
  return slice.results[0]!;
}

async function runtimeWaitSlice(
  harness: { readonly coordinator: import("../Services/ChildThreadCoordinator.ts").ChildThreadCoordinatorShape },
  childThreadIds: ReadonlyArray<ThreadId>,
  budgetDeadlineMs: number,
) {
  return Effect.runPromise(
    harness.coordinator.waitSlice({ childThreadIds, mode: "all", budgetDeadlineMs }),
  );
}

async function runtimeHasPending(
  harness: { readonly coordinator: import("../Services/ChildThreadCoordinator.ts").ChildThreadCoordinatorShape },
  parent: ThreadId,
) {
  return Effect.runPromise(harness.coordinator.hasPendingInjections(parent));
}
