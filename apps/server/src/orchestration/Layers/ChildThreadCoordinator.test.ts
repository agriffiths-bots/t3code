/* oxlint-disable t3code/no-manual-effect-runtime-in-tests -- These concurrency tests intentionally manage a long-lived runtime, queues, and scopes across helper boundaries. */
import {
  EnvironmentId,
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
import * as SubagentDispatchLimiter from "../../mcp/toolkits/subagent/SubagentDispatchLimiter.ts";
import { ChildThreadCoordinatorLive } from "./ChildThreadCoordinator.ts";

const now = "2026-06-17T10:00:00.000Z";
const afterWaitDelivery = "2099-01-01T00:00:00.000Z";
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
  turnId: TurnId = TurnId.make("turn-1"),
  completedAt?: string | null,
): OrchestrationLatestTurn => ({
  turnId,
  state,
  requestedAt: now,
  startedAt: now,
  completedAt: completedAt === undefined ? (state === "completed" ? now : null) : completedAt,
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
  readonly latestUserMessageAt?: string | null;
  readonly latestSystemWakeAt?: string | null;
  readonly latestSystemWakeText?: string | null;
  readonly session?: OrchestrationSession | null;
  readonly assistantText?: string | null;
  readonly archivedAt?: string | null;
  readonly deletedAt?: string | null;
}): ThreadState => {
  const latestTurn = input.latestTurn ?? null;
  const session = input.session ?? null;
  const archivedAt = input.archivedAt ?? null;
  const latestUserMessageAt =
    input.latestUserMessageAt != null && input.latestSystemWakeAt != null
      ? input.latestUserMessageAt > input.latestSystemWakeAt
        ? input.latestUserMessageAt
        : input.latestSystemWakeAt
      : (input.latestUserMessageAt ?? input.latestSystemWakeAt ?? null);
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
    archivedAt,
    session,
    latestUserMessageAt,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    parentThreadId: input.parentThreadId ?? null,
  };
  const messages = [
    ...(input.latestUserMessageAt != null
      ? [
          {
            id: MessageId.make(`user-msg-${input.threadId}`),
            role: "user" as const,
            text: "latest user message",
            turnId: null,
            streaming: false,
            createdAt: input.latestUserMessageAt,
            updatedAt: input.latestUserMessageAt,
          },
        ]
      : []),
    ...(input.latestSystemWakeAt != null
      ? [
          {
            id: MessageId.make(`system-wake-msg-${input.threadId}`),
            role: "system" as const,
            text:
              input.latestSystemWakeText ?? `[sub-agent ${input.threadId} completed] newer wake`,
            turnId: null,
            streaming: false,
            createdAt: input.latestSystemWakeAt,
            updatedAt: input.latestSystemWakeAt,
          },
        ]
      : []),
    ...(input.assistantText != null
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
      : []),
  ];
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
    archivedAt,
    deletedAt: input.deletedAt ?? null,
    messages,
    turns: [],
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
  turnId: TurnId = TurnId.make("turn-1"),
): OrchestrationEvent =>
  ({
    eventId: EventId.make(`evt-diff-${threadId}-${turnId}-${status}`),
    type: "thread.turn-diff-completed",
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: now,
    payload: {
      threadId,
      turnId,
      checkpointTurnCount: 1,
      checkpointRef: `thread:${threadId}:turn:1`,
      status,
      files: [],
      assistantMessageId: null,
      completedAt: now,
    },
  }) as unknown as OrchestrationEvent;

const turnStartRequestedEvent = (threadId: ThreadId): OrchestrationEvent =>
  ({
    eventId: EventId.make(`evt-turn-start-${threadId}`),
    type: "thread.turn-start-requested",
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: now,
    payload: {
      threadId,
      messageId: MessageId.make(`message-${threadId}`),
      runtimeMode: "full-access",
      interactionMode: "default",
      createdAt: now,
    },
  }) as unknown as OrchestrationEvent;

const sessionSetEvent = (
  threadId: ThreadId,
  status: OrchestrationSession["status"],
  activeTurnId: TurnId | null = null,
): OrchestrationEvent =>
  ({
    eventId: EventId.make(`evt-session-${threadId}-${status}`),
    type: "thread.session-set",
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: now,
    payload: { threadId, session: makeSession(threadId, status, activeTurnId) },
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

const threadArchivedEvent = (threadId: ThreadId): OrchestrationEvent =>
  ({
    eventId: EventId.make(`evt-archived-${threadId}`),
    type: "thread.archived",
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: now,
    payload: { threadId, archivedAt: now, updatedAt: now },
  }) as unknown as OrchestrationEvent;

const threadUnarchivedEvent = (threadId: ThreadId): OrchestrationEvent =>
  ({
    eventId: EventId.make(`evt-unarchived-${threadId}`),
    type: "thread.unarchived",
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: now,
    payload: { threadId, updatedAt: now },
  }) as unknown as OrchestrationEvent;

describe("ChildThreadCoordinator", () => {
  let runtime: ManagedRuntime.ManagedRuntime<
    | ChildThreadCoordinator
    | SqlClient
    | PendingDispatchRepository
    | SubagentDispatchLimiter.SubagentDispatchLimiter,
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
      readonly parentEnvironmentId?: EnvironmentId;
    }>;
    /** Pending_dispatches rows inserted BEFORE start() (simulated restart). */
    readonly seedPendingDispatches?: ReadonlyArray<PendingDispatch>;
    /** subagent_wait_deliveries rows inserted BEFORE start() (simulated restart). */
    readonly seedWaitDeliveries?: ReadonlyArray<{
      readonly childThreadId: ThreadId;
      readonly parentThreadId: ThreadId;
      readonly deliveredAt?: string;
      readonly parentTurnIdAtDelivery?: TurnId;
    }>;
    /** subagent_promoted_children rows inserted BEFORE start() (simulated restart). */
    readonly seedPromotedChildren?: ReadonlyArray<{
      readonly childThreadId: ThreadId;
      readonly parentThreadId: ThreadId;
      readonly promotedAt?: string;
    }>;
    /**
     * Command ids that already have an `accepted` receipt at boot — i.e. their
     * turn LANDED before a crash. The fake engine dedups a re-fire of these the
     * same way the real engine's transactional receipt does (exactly-once).
     */
    readonly seedAcceptedCommandIds?: ReadonlyArray<string>;
    /** Thread ids whose `getThreadShellById` read sleeps far longer than a slice. */
    readonly slowThreadShellIds?: ReadonlyArray<ThreadId>;
    /** Thread ids whose next N `getThreadShellById` reads sleep far longer than a slice. */
    readonly slowThreadShellReadCounts?: ReadonlyArray<{
      readonly threadId: ThreadId;
      readonly count: number;
    }>;
    /** Thread ids whose `getThreadDetailById` read sleeps far longer than a slice. */
    readonly slowThreadDetailIds?: ReadonlyArray<ThreadId>;
    /** 1-based `getThreadDetailById` read numbers that sleep far longer than a slice. */
    readonly slowThreadDetailReadNumbers?: ReadonlyArray<{
      readonly threadId: ThreadId;
      readonly readNumbers: ReadonlyArray<number>;
    }>;
    /** Thread ids whose shell exists but whose detail read returns none. */
    readonly detailUnavailableIds?: ReadonlyArray<ThreadId>;
    /** Per-thread shell snapshots returned in order, then the last one thereafter. */
    readonly shellReadSequences?: ReadonlyArray<{
      readonly threadId: ThreadId;
      readonly states: ReadonlyArray<ThreadState>;
    }>;
    /**
     * Invoked synchronously whenever a `thread.turn.start` command is dispatched.
     * Used by the re-entrancy test to enqueue the parent's terminal signal while
     * the parent-wake lock is still held, proving the lock is not deadlocked.
     */
    readonly onTurnStartDispatch?: (
      command: Extract<OrchestrationCommand, { readonly type: "thread.turn.start" }>,
      enqueue: (event: OrchestrationEvent) => void,
      setThread: (state: ThreadState) => void,
    ) => void;
    /** Simulate the production hot stream: events emitted before subscription are lost. */
    readonly dropEventsBeforeStreamSubscription?: boolean;
  }) {
    const threadStates = new Map<ThreadId, ThreadState>();
    for (const state of input?.threads ?? []) {
      threadStates.set(state.shell.id, state);
    }
    const dispatched: Array<OrchestrationCommand> = [];
    const knownInstances = new Set(input?.knownInstances ?? ["codex"]);
    const slowShellIds = new Set((input?.slowThreadShellIds ?? []).map((id) => String(id)));
    const slowShellReadCounts = new Map<string, number>();
    for (const slowReadCount of input?.slowThreadShellReadCounts ?? []) {
      slowShellReadCounts.set(String(slowReadCount.threadId), slowReadCount.count);
    }
    const slowDetailIds = new Set((input?.slowThreadDetailIds ?? []).map((id) => String(id)));
    const slowDetailReadNumbers = new Map<string, ReadonlySet<number>>();
    const detailReadCounts = new Map<string, number>();
    for (const slowReadNumbers of input?.slowThreadDetailReadNumbers ?? []) {
      slowDetailReadNumbers.set(
        String(slowReadNumbers.threadId),
        new Set(slowReadNumbers.readNumbers),
      );
    }
    const detailUnavailableIds = new Set(
      (input?.detailUnavailableIds ?? []).map((id) => String(id)),
    );
    const shellReadSequences = new Map<string, ReadonlyArray<ThreadState>>();
    const shellReadCounts = new Map<string, number>();
    for (const sequence of input?.shellReadSequences ?? []) {
      shellReadSequences.set(String(sequence.threadId), sequence.states);
    }

    const eventQueue = Effect.runSync(Queue.unbounded<OrchestrationEvent>());
    let streamSubscribed = false;
    const enqueueSync = (event: OrchestrationEvent) => {
      if (input?.dropEventsBeforeStreamSubscription && !streamSubscribed) return;
      Effect.runSync(Queue.offer(eventQueue, event));
    };

    // Mirror the real OrchestrationEngine's transactional commandId dedup: a
    // command whose commandId already has an `accepted` receipt is NOT appended
    // again; the existing sequence is returned. This is the property the
    // exactly-once wake/steer delivery relies on across a crash.
    const acceptedByCommandId = new Map<string, number>();
    for (const commandId of input?.seedAcceptedCommandIds ?? []) {
      // Pre-landed turn (committed before the simulated crash): a re-fire under
      // this commandId returns the existing sequence without re-appending.
      acceptedByCommandId.set(commandId, acceptedByCommandId.size + 1);
    }
    const recordDispatch = (command: OrchestrationCommand) => {
      const existing = acceptedByCommandId.get(command.commandId);
      if (existing !== undefined) {
        return { sequence: existing };
      }
      dispatched.push(command);
      const sequence = dispatched.length;
      acceptedByCommandId.set(command.commandId, sequence);
      if (command.type === "thread.turn.start") {
        input?.onTurnStartDispatch?.(
          command,
          enqueueSync,
          (state) => void threadStates.set(state.shell.id, state),
        );
      }
      return { sequence };
    };

    const engineLayer = Layer.succeed(
      OrchestrationEngineService,
      OrchestrationEngineService.of({
        readEvents: () => Stream.fromIterable(input?.persistedEvents ?? []),
        streamDomainEvents: Stream.unwrap(
          Effect.sync(() => {
            streamSubscribed = true;
            return Stream.fromQueue(eventQueue);
          }),
        ),
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
          const slowReadCount = slowShellReadCounts.get(String(threadId)) ?? 0;
          if (slowShellIds.has(String(threadId)) || slowReadCount > 0) {
            if (slowReadCount > 0) {
              slowShellReadCounts.set(String(threadId), slowReadCount - 1);
            }
            // Simulate a stalled projection read far longer than a wait slice.
            yield* Effect.sleep(`${WAIT_SLICE_SECONDS * 3} seconds`);
          }
          const sequence = shellReadSequences.get(String(threadId));
          if (sequence !== undefined && sequence.length > 0) {
            const readCount = shellReadCounts.get(String(threadId)) ?? 0;
            shellReadCounts.set(String(threadId), readCount + 1);
            const shell = sequence[Math.min(readCount, sequence.length - 1)]!.shell;
            return shell.archivedAt === null ? Option.some(shell) : Option.none();
          }
          const state = threadStates.get(threadId);
          return state && state.shell.archivedAt === null
            ? Option.some(state.shell)
            : Option.none();
        }),
      getThreadDetailById: (threadId) =>
        Effect.gen(function* () {
          const key = String(threadId);
          const readCount = (detailReadCounts.get(key) ?? 0) + 1;
          detailReadCounts.set(key, readCount);
          if (slowDetailIds.has(key) || (slowDetailReadNumbers.get(key)?.has(readCount) ?? false)) {
            yield* Effect.sleep(`${WAIT_SLICE_SECONDS * 3} seconds`);
          }
          if (detailUnavailableIds.has(key)) {
            return Option.none();
          }
          const state = threadStates.get(threadId);
          return state ? Option.some(state.detail) : Option.none();
        }),
      getThreadDetailSnapshot: () => Effect.succeed(Option.none()),
    });

    const registryLayer = Layer.succeed(ProviderInstanceRegistry, {
      getInstance: (instanceId) =>
        Effect.succeed(
          knownInstances.has(String(instanceId)) ? ({ instanceId } as never) : undefined,
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
      Layer.provideMerge(SubagentDispatchLimiter.layerTest()),
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
                INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode, created_at, updated_at, parent_thread_id, parent_environment_id)
                VALUES (${row.threadId}, ${projectId}, ${"seed"}, ${"{}"}, ${"full-access"}, ${"default"}, ${now}, ${now}, ${row.parentThreadId}, ${row.parentEnvironmentId ?? null})
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
    if (input?.seedWaitDeliveries) {
      for (const row of input.seedWaitDeliveries) {
        await activeRuntime.runPromise(
          Effect.flatMap(
            Effect.service(SqlClient),
            (sql) =>
              sql`
                INSERT INTO subagent_wait_deliveries (
                  child_thread_id,
                  parent_thread_id,
                  delivered_at,
                  parent_turn_id_at_delivery
                )
                VALUES (
                  ${row.childThreadId},
                  ${row.parentThreadId},
                  ${row.deliveredAt ?? now},
                  ${row.parentTurnIdAtDelivery ?? null}
                )
                ON CONFLICT (child_thread_id) DO UPDATE SET
                  parent_thread_id = excluded.parent_thread_id,
                  delivered_at = excluded.delivered_at,
                  parent_turn_id_at_delivery = excluded.parent_turn_id_at_delivery
              `,
          ),
        );
      }
    }
    if (input?.seedPromotedChildren) {
      for (const row of input.seedPromotedChildren) {
        await activeRuntime.runPromise(
          Effect.flatMap(
            Effect.service(SqlClient),
            (sql) =>
              sql`
                INSERT INTO subagent_promoted_children (
                  child_thread_id,
                  parent_thread_id,
                  promoted_at
                )
                VALUES (
                  ${row.childThreadId},
                  ${row.parentThreadId},
                  ${row.promotedAt ?? now}
                )
                ON CONFLICT (child_thread_id) DO UPDATE SET
                  parent_thread_id = excluded.parent_thread_id,
                  promoted_at = excluded.promoted_at
              `,
          ),
        );
      }
    }

    const coordinator = await activeRuntime.runPromise(Effect.service(ChildThreadCoordinator));
    scope = await Effect.runPromise(Scope.make("sequential"));
    await Effect.runPromise(coordinator.start().pipe(Scope.provide(scope)));

    const setThread = (state: ThreadState) => threadStates.set(state.shell.id, state);
    const setShellSlow = (threadId: ThreadId, slow: boolean) => {
      if (slow) {
        slowShellIds.add(String(threadId));
      } else {
        slowShellIds.delete(String(threadId));
      }
    };
    const setDetailSlow = (threadId: ThreadId, slow: boolean) => {
      if (slow) {
        slowDetailIds.add(String(threadId));
      } else {
        slowDetailIds.delete(String(threadId));
      }
    };

    // Simulate a deleted thread: subsequent getThreadShellById reads return none.
    const removeThread = (threadId: ThreadId) => threadStates.delete(threadId);

    const listPendingDispatches = () =>
      activeRuntime.runPromise(
        Effect.flatMap(Effect.service(PendingDispatchRepository), (repo) => repo.listAll()),
      );

    const listWaitDeliveries = () =>
      activeRuntime.runPromise(
        Effect.flatMap(
          Effect.service(SqlClient),
          (sql) =>
            sql<{ readonly childThreadId: string; readonly parentThreadId: string }>`
              SELECT
                child_thread_id AS "childThreadId",
                parent_thread_id AS "parentThreadId"
              FROM subagent_wait_deliveries
              ORDER BY child_thread_id
            `,
        ),
      );

    const insertPendingDispatch = (row: PendingDispatch) =>
      activeRuntime.runPromise(
        Effect.flatMap(Effect.service(PendingDispatchRepository), (repo) => repo.insert(row)),
      );

    const holdSqlWriteLock = async () => {
      let release!: () => void;
      let acquired!: () => void;
      const releasePromise = new Promise<void>((resolve) => {
        release = resolve;
      });
      const acquiredPromise = new Promise<void>((resolve) => {
        acquired = resolve;
      });
      const lockPromise = activeRuntime.runPromise(
        Effect.flatMap(Effect.service(SqlClient), (sql) =>
          sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`
                INSERT INTO subagent_promoted_children (
                  child_thread_id,
                  parent_thread_id,
                  promoted_at
                )
                VALUES (
                  ${"test-lock-child"},
                  ${"test-lock-parent"},
                  ${now}
                )
                ON CONFLICT (child_thread_id) DO UPDATE SET
                  parent_thread_id = excluded.parent_thread_id,
                  promoted_at = excluded.promoted_at
              `;
              yield* Effect.promise(() => {
                acquired();
                return releasePromise;
              });
              yield* sql`
                DELETE FROM subagent_promoted_children
                WHERE child_thread_id = ${"test-lock-child"}
              `;
            }),
          ),
        ),
      );
      await acquiredPromise;
      return {
        release: async () => {
          release();
          await lockPromise;
        },
      };
    };

    const failPromotedChildInserts = () =>
      activeRuntime.runPromise(
        Effect.flatMap(
          Effect.service(SqlClient),
          (sql) => sql`
            CREATE TEMP TRIGGER test_fail_promoted_child_insert
            BEFORE INSERT ON subagent_promoted_children
            BEGIN
              SELECT RAISE(ABORT, 'test promoted child insert failure');
            END
          `,
        ),
      );

    const allowPromotedChildInserts = () =>
      activeRuntime.runPromise(
        Effect.flatMap(
          Effect.service(SqlClient),
          (sql) => sql`DROP TRIGGER IF EXISTS test_fail_promoted_child_insert`,
        ),
      );

    const listPromotedChildren = () =>
      activeRuntime.runPromise(
        Effect.flatMap(
          Effect.service(SqlClient),
          (sql) =>
            sql<{ readonly childThreadId: string }>`
              SELECT child_thread_id AS "childThreadId"
              FROM subagent_promoted_children
              ORDER BY child_thread_id
            `,
        ),
      );

    const canAcquireDispatchLease = () =>
      activeRuntime.runPromise(
        Effect.gen(function* () {
          const limiter = yield* SubagentDispatchLimiter.SubagentDispatchLimiter;
          const leaseOption = yield* limiter.acquire.pipe(Effect.timeoutOption("10 millis"));
          if (Option.isNone(leaseOption)) return false;
          yield* limiter.release(leaseOption.value);
          return true;
        }),
      );

    const seedDispatchLease = (childThreadId: ThreadId) =>
      activeRuntime.runPromise(
        Effect.gen(function* () {
          const limiter = yield* SubagentDispatchLimiter.SubagentDispatchLimiter;
          yield* limiter.seedChild(childThreadId);
        }),
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
      setShellSlow,
      setDetailSlow,
      removeThread,
      feed,
      register,
      listPendingDispatches,
      listWaitDeliveries,
      insertPendingDispatch,
      holdSqlWriteLock,
      failPromotedChildInserts,
      allowPromotedChildInserts,
      listPromotedChildren,
      canAcquireDispatchLease,
      seedDispatchLease,
    };
  }

  it("restart reconciliation ignores child rows whose parent belongs to a remote environment", async () => {
    const child = ThreadId.make("restart-remote-parent-child");
    const parent = ThreadId.make("restart-remote-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "remote parent should poll this",
        }),
      ],
      seedChildRows: [
        {
          threadId: child,
          parentThreadId: parent,
          parentEnvironmentId: EnvironmentId.make("environment-remote-parent"),
        },
      ],
    });

    await Effect.runPromise(harness.coordinator.drain);

    expect(harness.dispatched).toEqual([]);
    expect(await harness.listPendingDispatches()).toEqual([]);
  });

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

  it("settles error turn-diff as failed even when projection shows completed", async () => {
    const child = ThreadId.make("child-error-projection-completed");
    const parent = ThreadId.make("parent-error-projection-completed");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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
    await harness.feed(sessionSetEvent(child, "running", turn1));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "ready"),
        assistantText: "stale completed text",
      }),
    );
    await harness.feed(turnDiffEvent(child, "error", turn1));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("failed");
    expect(result.error).toBe("turn diff error");
  });

  it("does NOT settle missing turn-diff without matching projection evidence", async () => {
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
    const slice = await runtimeWaitSlice(harness, [child], PAST_MS);
    expect(slice.results[0]!.status).toBe("timeout");
  });

  it("does NOT settle missing turn-diff while the projection still shows the child running", async () => {
    const child = ThreadId.make("child-missing-running");
    const parent = ThreadId.make("parent-missing-running");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
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
    await harness.feed(turnDiffEvent(child, "missing"));
    const slice = await runtimeWaitSlice(harness, [child], PAST_MS);
    expect(slice.results[0]!.status).toBe("timeout");
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
  });

  it("does NOT settle missing turn-diff when the bounded projection read times out", async () => {
    const child = ThreadId.make("child-missing-slow-projection");
    const parent = ThreadId.make("parent-missing-slow-projection");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(parent, "ready"),
        }),
      ],
      slowThreadShellIds: [child],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: true,
      model: codexModel,
      spawnedAtMs: 0,
    });
    await harness.feed(turnDiffEvent(child, "missing"));
    const slice = await runtimeWaitSlice(harness, [child], PAST_MS);
    expect(slice.results[0]!.status).toBe("timeout");
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
  });

  it("does NOT settle missing turn-diff from a stale completed projection for another turn", async () => {
    const child = ThreadId.make("child-missing-stale-completed");
    const parent = ThreadId.make("parent-missing-stale-completed");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("previous-turn")),
          session: makeSession(child, "running", TurnId.make("turn-1")),
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
    await harness.feed(turnDiffEvent(child, "missing"));
    const slice = await runtimeWaitSlice(harness, [child], PAST_MS);
    expect(slice.results[0]!.status).toBe("timeout");
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
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

  it("treats session-set stopped after a completed projected turn as completed", async () => {
    const child = ThreadId.make("child-stopped-after-completed");
    const parent = ThreadId.make("parent-stopped-after-completed");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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
    await harness.feed(sessionSetEvent(child, "running", turn1));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "stopped"),
        assistantText: "final memo",
      }),
    );

    await harness.feed(sessionSetEvent(child, "stopped"));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("final memo");
    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start" && command.threadId === parent,
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]!.message.text).toContain(`[sub-agent ${child} completed] final memo`);
    expect(turnStarts[0]!.message.text).not.toContain("session stopped");
  });

  it("settles stopped completed projection even when detail is unavailable", async () => {
    const child = ThreadId.make("child-stopped-completed-no-detail");
    const parent = ThreadId.make("parent-stopped-completed-no-detail");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
        }),
      ],
      detailUnavailableIds: [child],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    await harness.feed(sessionSetEvent(child, "running", turn1));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "stopped"),
        assistantText: "detail is lagging",
      }),
    );

    await harness.feed(sessionSetEvent(child, "stopped"));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.error).toBe(null);
    expect(result.finalAssistantText).toBe(null);
  });

  it("does NOT treat session error after a completed projected turn as completed", async () => {
    const child = ThreadId.make("child-error-after-completed");
    const parent = ThreadId.make("parent-error-after-completed");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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
    await harness.feed(sessionSetEvent(child, "running", turn1));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "error"),
        assistantText: "stale successful text",
      }),
    );

    await harness.feed(sessionSetEvent(child, "error"));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("failed");
    expect(result.error).toBe("session error");
    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start" && command.threadId === parent,
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]!.message.text).toContain(`[sub-agent ${child} failed] session error`);
  });

  it("settles an errored session as failed during one-shot register", async () => {
    const child = ThreadId.make("child-error-register-completed");
    const parent = ThreadId.make("parent-error-register-completed");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "error"),
          assistantText: "stale successful text",
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

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("failed");
    expect(result.error).toBe("session error");
    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start" && command.threadId === parent,
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]!.message.text).toContain(`[sub-agent ${child} failed] session error`);
  });

  it("settles session-set ready with a completed projected turn as completed", async () => {
    const child = ThreadId.make("child-ready-completed");
    const parent = ThreadId.make("parent-ready-completed");
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
    await harness.feed(sessionSetEvent(child, "running", TurnId.make("turn-1")));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        session: makeSession(child, "ready"),
        assistantText: "ready result",
      }),
    );
    await harness.feed(sessionSetEvent(child, "ready"));
    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("ready result");
  });

  it("settles session-set ready after registering against an already-running projected turn", async () => {
    const child = ThreadId.make("child-ready-projected-running");
    const parent = ThreadId.make("parent-ready-projected-running");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "ready"),
        assistantText: "projected running completed",
      }),
    );
    await harness.feed(sessionSetEvent(child, "ready"));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("projected running completed");
  });

  it("drains deferred child steers when session-set ready observes a completed projected turn", async () => {
    const child = ThreadId.make("child-ready-drain-steer");
    const parent = ThreadId.make("parent-ready-drain-steer");
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
    await harness.insertPendingDispatch({
      id: PendingDispatchId.make("pd-ready-steer-1"),
      kind: "child_steer",
      targetThreadId: child,
      sourceChildId: null,
      text: "continue after ready",
      error: null,
      status: null,
      commandId: null,
      deliveredByWait: false,
      waitCancellable: false,
      createdAt: now as unknown as PendingDispatch["createdAt"],
    });

    await harness.feed(sessionSetEvent(child, "running", TurnId.make("turn-1")));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        session: makeSession(child, "ready"),
        assistantText: "ready result",
      }),
    );
    await harness.feed(sessionSetEvent(child, "ready"));

    const steerStarts = harness.dispatched.filter(
      (c) => c.type === "thread.turn.start" && c.threadId === child,
    );
    expect(steerStarts).toHaveLength(1);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
  });

  it("drains deferred child steers when one-shot projection settlement observes idle", async () => {
    const child = ThreadId.make("child-oneshot-drain-steer");
    const parent = ThreadId.make("parent-oneshot-drain-steer");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(child, "ready"),
          assistantText: "ready result",
        }),
      ],
    });
    await harness.insertPendingDispatch({
      id: PendingDispatchId.make("pd-oneshot-steer-1"),
      kind: "child_steer",
      targetThreadId: child,
      sourceChildId: null,
      text: "continue after one-shot",
      error: null,
      status: null,
      commandId: null,
      deliveredByWait: false,
      waitCancellable: false,
      createdAt: now as unknown as PendingDispatch["createdAt"],
    });

    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });

    const steerStarts = harness.dispatched.filter(
      (c) => c.type === "thread.turn.start" && c.threadId === child,
    );
    expect(steerStarts).toHaveLength(1);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
  });

  it("does NOT settle session-set ready from a stale completed projection for another turn", async () => {
    const child = ThreadId.make("child-ready-stale-completed");
    const parent = ThreadId.make("parent-ready-stale-completed");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
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
    await harness.feed(sessionSetEvent(child, "running", TurnId.make("turn-1")));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", TurnId.make("previous-turn")),
        session: makeSession(child, "ready"),
      }),
    );

    await harness.feed(sessionSetEvent(child, "ready"));

    const slice = await runtimeWaitSlice(harness, [child], PAST_MS);
    expect(slice.results[0]!.status).toBe("timeout");
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
  });

  it("does NOT settle stale ready turn-diff after a newer child turn starts", async () => {
    const child = ThreadId.make("child-ready-stale-diff");
    const parent = ThreadId.make("parent-ready-stale-diff");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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
    await harness.feed(sessionSetEvent(child, "running", turn1));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("running", turn2),
        session: makeSession(child, "running", turn2),
      }),
    );
    await harness.feed(sessionSetEvent(child, "running", turn2));

    await harness.feed(turnDiffEvent(child, "ready", turn1));

    const entriesAfterStaleDiff = await runtimeListChildren(harness, parent);
    expect(entriesAfterStaleDiff.find((entry) => entry.childThreadId === child)?.settled).toBe(
      false,
    );

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn2),
        session: makeSession(child, "ready"),
        assistantText: "turn 2 done",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", turn2));

    const entriesAfterCurrentDiff = await runtimeListChildren(harness, parent);
    expect(entriesAfterCurrentDiff.find((entry) => entry.childThreadId === child)?.settled).toBe(
      true,
    );
  });

  it("does NOT settle stale ready turn-diff after turn-start before the new session-set", async () => {
    const child = ThreadId.make("child-ready-stale-diff-before-session");
    const parent = ThreadId.make("parent-ready-stale-diff-before-session");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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
    await harness.feed(sessionSetEvent(child, "running", turn1));
    await harness.feed(turnStartRequestedEvent(child));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "running", turn1),
        assistantText: "old turn done",
      }),
    );

    await harness.feed(turnDiffEvent(child, "ready", turn1));

    const entriesAfterStaleDiff = await runtimeListChildren(harness, parent);
    expect(entriesAfterStaleDiff.find((entry) => entry.childThreadId === child)?.settled).toBe(
      false,
    );

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("running", turn2),
        session: makeSession(child, "running", turn2),
      }),
    );
    await harness.feed(sessionSetEvent(child, "running", turn2));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn2),
        session: makeSession(child, "ready"),
        assistantText: "turn 2 done",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", turn2));

    const entriesAfterCurrentDiff = await runtimeListChildren(harness, parent);
    expect(entriesAfterCurrentDiff.find((entry) => entry.childThreadId === child)?.settled).toBe(
      true,
    );
  });

  it("does NOT replay a stale idle ready diff as the requested new turn", async () => {
    const child = ThreadId.make("replay-stale-idle-ready-diff");
    const parent = ThreadId.make("replay-stale-idle-ready-diff-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "ready"),
          assistantText: "old turn done",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [turnStartRequestedEvent(child), turnDiffEvent(child, "ready", turn1)],
    });

    const slice = await runtimeWaitSlice(harness, [child], PAST_MS);
    expect(slice.results[0]!.status).toBe("timeout");
  });

  it("settles same-turn ready diff after a mid-turn steer", async () => {
    const child = ThreadId.make("child-mid-turn-steer-ready-diff");
    const parent = ThreadId.make("parent-mid-turn-steer-ready-diff");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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
    await harness.feed(sessionSetEvent(child, "running", turn1));
    await harness.feed(turnStartRequestedEvent(child));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "ready"),
        assistantText: "same turn done",
      }),
    );

    await harness.feed(turnDiffEvent(child, "ready", turn1));

    const entriesAfterReadyDiff = await runtimeListChildren(harness, parent);
    expect(entriesAfterReadyDiff.find((entry) => entry.childThreadId === child)?.settled).toBe(
      true,
    );
  });

  it("settles stopped completion after a same-turn mid-turn steer", async () => {
    const child = ThreadId.make("child-mid-turn-steer-stopped-complete");
    const parent = ThreadId.make("parent-mid-turn-steer-stopped-complete");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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
    await harness.feed(sessionSetEvent(child, "running", turn1));
    await harness.feed(turnStartRequestedEvent(child));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("running", turn1),
        session: makeSession(child, "running", turn1),
      }),
    );
    await harness.feed(turnDiffEvent(child, "missing", turn1));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "stopped"),
        assistantText: "same turn stopped complete",
      }),
    );

    await harness.feed(sessionSetEvent(child, "stopped"));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("same turn stopped complete");
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

  it("settles stopped after a stale completed projection with a newer user message as failed", async () => {
    const child = ThreadId.make("child-stopped-stale-completed-newer-user");
    const parent = ThreadId.make("parent-stopped-stale-completed-newer-user");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          latestUserMessageAt: "2026-06-17T10:02:00.000Z",
          session: makeSession(child, "stopped"),
          assistantText: "stale completed text",
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
    expect(result.error).toBe("session stopped");
  });

  it("settles stopped after a missing turn-diff while projection remains running", async () => {
    const child = ThreadId.make("child-missing-running-stopped");
    const parent = ThreadId.make("parent-missing-running-stopped");
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

    await harness.feed(turnDiffEvent(child, "missing"));
    await harness.feed(sessionSetEvent(child, "stopped"));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("failed");
    expect(result.error).toBe("session stopped");
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

  it("does NOT settle a completed projection until detail is available", async () => {
    const child = ThreadId.make("child-completed-detail-lag");
    const parent = ThreadId.make("parent-completed-detail-lag");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "detail arrived later",
        }),
      ],
      slowThreadDetailIds: [child],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });

    const beforeDetail = await runtimeWaitSlice(harness, [child], PAST_MS);
    expect(beforeDetail.results[0]!.status).toBe("timeout");

    harness.setDetailSlow(child, false);
    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("detail arrived later");
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
    const preflightExit = await Effect.runPromiseExit(
      harness.coordinator.validateSpawn({
        parentThreadId: parent,
        model: codexModel,
      }),
    );
    expect(Exit.isFailure(preflightExit)).toBe(true);

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

  it("does NOT reconcile a running placeholder missing turn-diff as terminal", async () => {
    const child = ThreadId.make("recon-running-missing");
    const parent = ThreadId.make("recon-running-missing-parent");
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
      persistedEvents: [
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running"),
        turnDiffEvent(child, "missing"),
      ],
    });
    const slice = await runtimeWaitSlice(harness, [child], PAST_MS);
    expect(slice.results[0]!.status).toBe("timeout");
  });

  it("does NOT reconcile a stale diff after a replayed turn-start before session-set", async () => {
    const child = ThreadId.make("replay-turn-start-stale-diff");
    const parent = ThreadId.make("replay-turn-start-stale-diff-parent");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "running", turn1),
          assistantText: "old replayed turn",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        sessionSetEvent(child, "running", turn1),
        turnStartRequestedEvent(child),
        turnDiffEvent(child, "ready", turn1),
      ],
    });

    const beforeSession = await runtimeWaitSlice(harness, [child], PAST_MS);
    expect(beforeSession.results[0]!.status).toBe("timeout");

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("running", turn2),
        session: makeSession(child, "running", turn2),
      }),
    );
    await harness.feed(sessionSetEvent(child, "running", turn2));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn2),
        session: makeSession(child, "ready"),
        assistantText: "turn 2 done",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", turn2));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("turn 2 done");
  });

  it("reconciles same-turn ready diff after a replayed mid-turn steer", async () => {
    const child = ThreadId.make("replay-mid-turn-steer-ready-diff");
    const parent = ThreadId.make("replay-mid-turn-steer-ready-diff-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "ready"),
          assistantText: "same turn replayed",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        sessionSetEvent(child, "running", turn1),
        turnStartRequestedEvent(child),
        turnDiffEvent(child, "ready", turn1),
      ],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("same turn replayed");
  });

  it("reconciles stopped completion after a replayed same-turn mid-turn steer", async () => {
    const child = ThreadId.make("replay-mid-turn-steer-stopped-complete");
    const parent = ThreadId.make("replay-mid-turn-steer-stopped-complete-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "stopped"),
          assistantText: "same turn stopped replayed",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        sessionSetEvent(child, "running", turn1),
        turnStartRequestedEvent(child),
        turnDiffEvent(child, "missing", turn1),
        sessionSetEvent(child, "stopped"),
      ],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("same turn stopped replayed");
  });

  it("reconciles stopped after a running placeholder missing turn-diff as failed", async () => {
    const child = ThreadId.make("recon-running-missing-stopped");
    const parent = ThreadId.make("recon-running-missing-stopped-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "stopped"),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running"),
        turnDiffEvent(child, "missing"),
        sessionSetEvent(child, "stopped"),
      ],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("failed");
    expect(result.error).toBe("session stopped");
  });

  it("reconciles stopped after a missing turn-diff as completed when projection completed", async () => {
    const child = ThreadId.make("recon-missing-stopped-completed");
    const parent = ThreadId.make("recon-missing-stopped-completed-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "stopped"),
          assistantText: "completed before stopped replay",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn1),
        turnDiffEvent(child, "missing", turn1),
        sessionSetEvent(child, "stopped"),
      ],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.error).toBe(null);
    expect(result.finalAssistantText).toBe("completed before stopped replay");
  });

  it("does NOT let stopped replay completion override an authoritative error diff", async () => {
    const child = ThreadId.make("recon-error-stopped-completed-child");
    const parent = ThreadId.make("recon-error-stopped-completed-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "stopped"),
          assistantText: "stale completed text",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn1),
        turnDiffEvent(child, "error", turn1),
        sessionSetEvent(child, "stopped"),
      ],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("failed");
    expect(result.error).toBe("turn diff error");
  });

  it("does NOT let session-error replay completion override a missing diff", async () => {
    const child = ThreadId.make("recon-missing-error-completed-child");
    const parent = ThreadId.make("recon-missing-error-completed-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "error"),
          assistantText: "stale completed text",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn1),
        turnDiffEvent(child, "missing", turn1),
        sessionSetEvent(child, "error"),
      ],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("failed");
    expect(result.error).toBe("session error");
  });

  it("does NOT let session-error replay override a completed turn-diff", async () => {
    const child = ThreadId.make("recon-ready-error-completed-child");
    const parent = ThreadId.make("recon-ready-error-completed-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "error"),
          assistantText: "stale completed text",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn1),
        turnDiffEvent(child, "ready", turn1),
        sessionSetEvent(child, "error"),
      ],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("failed");
    expect(result.error).toBe("session error");
  });

  it("settles live stopped after replaying a running placeholder missing turn-diff", async () => {
    const child = ThreadId.make("recon-running-missing-live-stopped");
    const parent = ThreadId.make("recon-running-missing-live-stopped-parent");
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
      persistedEvents: [
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", TurnId.make("turn-1")),
        turnDiffEvent(child, "missing"),
      ],
    });

    await harness.feed(sessionSetEvent(child, "stopped"));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("failed");
    expect(result.error).toBe("session stopped");
  });

  it("does NOT reconcile a startup ready session as terminal while a turn is unresolved", async () => {
    const child = ThreadId.make("recon-startup-ready-running-child");
    const parent = ThreadId.make("recon-startup-ready-running-parent");
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
      persistedEvents: [
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "ready"),
        sessionSetEvent(child, "running"),
      ],
    });
    const slice = await runtimeWaitSlice(harness, [child], PAST_MS);
    expect(slice.results[0]!.status).toBe("timeout");
  });

  it("does NOT reconcile stale ready turn-diff after a newer child turn starts", async () => {
    const child = ThreadId.make("recon-stale-ready-diff-child");
    const parent = ThreadId.make("recon-stale-ready-diff-parent");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn2),
          session: makeSession(child, "running", turn2),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn1),
        sessionSetEvent(child, "running", turn2),
        turnDiffEvent(child, "ready", turn1),
      ],
    });
    const entries = await runtimeListChildren(harness, parent);
    expect(entries.find((entry) => entry.childThreadId === child)?.settled).toBe(false);
  });

  it("preserves completed session-set during replay when a later missing turn diff exists", async () => {
    const child = ThreadId.make("recon-ready-missing-child");
    const parent = ThreadId.make("recon-ready-missing-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "ready"),
          assistantText: "completed before missing diff",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn1),
        sessionSetEvent(child, "ready"),
        turnDiffEvent(child, "missing", turn1),
      ],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("completed before missing diff");
  });

  it("does NOT replay stale ready session-set after a pending new turn-start", async () => {
    const child = ThreadId.make("recon-stale-ready-session-pending-child");
    const parent = ThreadId.make("recon-stale-ready-session-pending-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "ready"),
          assistantText: "old turn done",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        sessionSetEvent(child, "running", turn1),
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "ready"),
      ],
    });

    const slice = await runtimeWaitSlice(harness, [child], PAST_MS);
    expect(slice.results[0]!.status).toBe("timeout");
  });

  it("reconciles completed projection before killing a missing-diff child whose provider is gone", async () => {
    const child = ThreadId.make("recon-missing-completed-provider-gone-child");
    const parent = ThreadId.make("recon-missing-completed-provider-gone-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(child, "ready"),
          assistantText: "completed despite missing diff",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", TurnId.make("turn-1")),
        turnDiffEvent(child, "missing"),
      ],
      knownInstances: [],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("completed despite missing diff");
  });

  it("does not seed dispatch leases for boot-reconciled archived stopped ghosts", async () => {
    const parent = ThreadId.make("ghost-capacity-parent");
    const ghostIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`ghost-capacity-child-${index + 1}`),
    );
    const archivedAt = "2026-07-07T20:29:30.000Z";
    const harness = await createHarness({
      threads: ghostIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: null,
          session: makeSession(threadId, "stopped"),
          archivedAt,
        }),
      ),
      seedChildRows: ghostIds.map((threadId) => ({ threadId, parentThreadId: parent })),
      persistedEvents: [],
    });

    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => entry.settled)).toBe(true);
    expect(await harness.canAcquireDispatchLease()).toBe(true);
  });

  it("does not seed dispatch leases for log-replayed archived ghosts", async () => {
    const parent = ThreadId.make("ghost-log-archive-parent");
    const ghostIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`ghost-log-archive-child-${index + 1}`),
    );
    const harness = await createHarness({
      threads: ghostIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: null,
          session: makeSession(threadId, "stopped"),
        }),
      ),
      seedChildRows: ghostIds.map((threadId) => ({ threadId, parentThreadId: parent })),
      persistedEvents: ghostIds.map(threadArchivedEvent),
    });

    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => entry.settled)).toBe(true);
    expect(await harness.canAcquireDispatchLease()).toBe(true);
  });

  it("preserves replayed completion when a child is archived later", async () => {
    const child = ThreadId.make("recon-completed-then-archived-child");
    const parent = ThreadId.make("recon-completed-then-archived-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "stopped"),
          archivedAt: now,
          assistantText: "completed before archive",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [turnDiffEvent(child, "ready", turn1), threadArchivedEvent(child)],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("completed before archive");
  });

  it("overrides replayed completion when a child is deleted later", async () => {
    const child = ThreadId.make("recon-completed-then-deleted-child");
    const parent = ThreadId.make("recon-completed-then-deleted-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "stopped"),
          deletedAt: now,
          assistantText: "completed before delete",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [turnDiffEvent(child, "ready", turn1), threadDeletedEvent(child)],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("killed");
    expect(result.error).toBe("thread deleted");
  });

  it("keeps replayed archive terminal sticky against late turn diffs", async () => {
    const child = ThreadId.make("recon-archived-then-late-diff-child");
    const parent = ThreadId.make("recon-archived-then-late-diff-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: null,
          session: makeSession(child, "ready"),
          archivedAt: now,
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [threadArchivedEvent(child), turnDiffEvent(child, "ready", turn1)],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("killed");
    expect(result.error).toBe("thread archived");
  });

  it("keeps replayed archive sticky when archive detail read times out", async () => {
    const child = ThreadId.make("recon-archive-timeout-then-late-diff-child");
    const parent = ThreadId.make("recon-archive-timeout-then-late-diff-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [threadArchivedEvent(child), turnDiffEvent(child, "ready", turn1)],
      slowThreadDetailReadNumbers: [{ threadId: child, readNumbers: [2] }],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("killed");
    expect(result.error).toBe("thread archived");
    expect(await harness.canAcquireDispatchLease()).toBe(true);
  });

  it("keeps archive terminal when projection completes after the archive", async () => {
    const child = ThreadId.make("live-archive-before-late-complete-child");
    const parent = ThreadId.make("live-archive-before-late-complete-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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

    await harness.feed(threadArchivedEvent(child));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1, "2026-06-17T10:00:01.000Z"),
        session: makeSession(child, "stopped"),
        archivedAt: now,
        assistantText: "late completion after archive",
      }),
    );

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("killed");
    expect(result.error).toBe("thread archived");
  });

  it("keeps live archive sticky when a ready diff beats archived projection", async () => {
    const child = ThreadId.make("live-archive-marker-before-projection-child");
    const parent = ThreadId.make("live-archive-marker-before-projection-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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

    await harness.feed(threadArchivedEvent(child));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "stopped"),
        assistantText: "projection has not archived yet",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", turn1));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("killed");
    expect(result.error).toBe("thread archived");
  });

  it("records live archive marker even when projection detail is missing", async () => {
    const child = ThreadId.make("live-archive-marker-missing-detail-child");
    const parent = ThreadId.make("live-archive-marker-missing-detail-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness();
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });

    await harness.feed(threadArchivedEvent(child));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "stopped"),
        assistantText: "projection appeared after archive",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", turn1));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("killed");
    expect(result.error).toBe("thread archived");
  });

  it("preserves live completed projection when archive arrives after completion", async () => {
    const child = ThreadId.make("live-archive-after-completed-child");
    const parent = ThreadId.make("live-archive-after-completed-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "stopped"),
        assistantText: "completed before archive",
      }),
    );

    await harness.feed(threadArchivedEvent(child));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("completed before archive");
  });

  it("preserves live completed shell when archive detail read times out", async () => {
    const child = ThreadId.make("live-archive-completed-slow-detail-child");
    const parent = ThreadId.make("live-archive-completed-slow-detail-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
        }),
      ],
      slowThreadDetailReadNumbers: [{ threadId: child, readNumbers: [1] }],
    });
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "stopped"),
        assistantText: "completed before slow archive detail",
      }),
    );

    await harness.feed(threadArchivedEvent(child));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.error).toBe(null);
  });

  it("preserves pre-registration completed projection when archive arrives first", async () => {
    const child = ThreadId.make("live-archive-before-register-completed-child");
    const parent = ThreadId.make("live-archive-before-register-completed-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "stopped"),
          assistantText: "completed before registration",
        }),
      ],
    });

    await harness.feed(threadArchivedEvent(child));
    await harness.register({
      parentThreadId: parent,
      childThreadId: child,
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("completed before registration");
  });

  it("archive marker overrides pending-start ready diffs", async () => {
    const child = ThreadId.make("live-archive-marker-pending-ready-child");
    const parent = ThreadId.make("live-archive-marker-pending-ready-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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

    await harness.feed(turnStartRequestedEvent(child));
    await harness.feed(threadArchivedEvent(child));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "stopped"),
        assistantText: "pending diff after archive",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", turn1));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("killed");
    expect(result.error).toBe("thread archived");
  });

  it("archive marker overrides one-shot projection completion", async () => {
    const child = ThreadId.make("live-archive-marker-oneshot-child");
    const parent = ThreadId.make("live-archive-marker-oneshot-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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

    await harness.feed(threadArchivedEvent(child));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "stopped"),
        assistantText: "one-shot projection after archive",
      }),
    );

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("killed");
    expect(result.error).toBe("thread archived");
  });

  it("archive marker overrides later stopped sessions", async () => {
    const child = ThreadId.make("live-archive-marker-session-stopped-child");
    const parent = ThreadId.make("live-archive-marker-session-stopped-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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

    await harness.feed(turnStartRequestedEvent(child));
    await harness.feed(threadArchivedEvent(child));
    await harness.feed(sessionSetEvent(child, "stopped"));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("killed");
    expect(result.error).toBe("thread archived");
  });

  it("does not settle replayed archived children whose session is still running", async () => {
    const parent = ThreadId.make("recon-archive-running-parent");
    const childIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`recon-archive-running-child-${index + 1}`),
    );
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: childIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: null,
          session: makeSession(threadId, "running", turn1),
          archivedAt: now,
        }),
      ),
      seedChildRows: childIds.map((threadId) => ({ threadId, parentThreadId: parent })),
      persistedEvents: childIds.map(threadArchivedEvent),
    });

    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => !entry.settled)).toBe(true);
    expect(await harness.canAcquireDispatchLease()).toBe(false);
  });

  it("does not seed leases for archived stopped children with stale running turns", async () => {
    const parent = ThreadId.make("recon-archive-stale-running-parent");
    const childIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`recon-archive-stale-running-child-${index + 1}`),
    );
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: childIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(threadId, "stopped"),
          archivedAt: now,
        }),
      ),
      seedChildRows: childIds.map((threadId) => ({ threadId, parentThreadId: parent })),
    });

    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => entry.settled)).toBe(true);
    expect(await harness.canAcquireDispatchLease()).toBe(true);
  });

  it("clears replayed archive terminal when a child is unarchived before a new turn", async () => {
    const parent = ThreadId.make("recon-archive-unarchive-pending-parent");
    const childIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`recon-archive-unarchive-pending-child-${index + 1}`),
    );
    const turn1 = TurnId.make("turn-1");
    const staleArchivedWakeId = PendingDispatchId.make("pd-recon-archive-unarchive-stale");
    const harness = await createHarness({
      threads: childIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: null,
          session: makeSession(threadId, "running", turn1),
        }),
      ),
      seedChildRows: childIds.map((threadId) => ({ threadId, parentThreadId: parent })),
      persistedEvents: childIds.flatMap((threadId) => [
        threadArchivedEvent(threadId),
        threadUnarchivedEvent(threadId),
        turnStartRequestedEvent(threadId),
      ]),
      seedPendingDispatches: [
        {
          id: staleArchivedWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: childIds[0]!,
          text: null,
          error: "thread archived",
          status: "killed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => !entry.settled)).toBe(true);
    expect(entries.find((entry) => entry.childThreadId === childIds[0])?.detached).toBe(true);
    expect(await harness.canAcquireDispatchLease()).toBe(false);
    expect(await harness.listPendingDispatches()).toEqual([]);
  });

  it("does not re-mark fully pruned archived wakes as queued on boot", async () => {
    const child = ThreadId.make("recon-prune-archive-synthesize-child");
    const parent = ThreadId.make("recon-prune-archive-synthesize-parent");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const staleArchivedWakeId = PendingDispatchId.make("pd-recon-prune-archive-synthesize-stale");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "stopped"),
          assistantText: "completed after stale archive wake",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
      ],
      seedPendingDispatches: [
        {
          id: staleArchivedWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: null,
          error: "thread archived",
          status: "killed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).not.toBe(staleArchivedWakeId);
    expect(pendingRows[0]!.text).toBe("completed after stale archive wake");
  });

  it("keeps queued wake dedupe when pruning one stale archived wake leaves another row", async () => {
    const child = ThreadId.make("recon-prune-stale-archive-keep-wake-child");
    const parent = ThreadId.make("recon-prune-stale-archive-keep-wake-parent");
    const staleArchivedWakeId = PendingDispatchId.make("pd-recon-prune-stale-archive");
    const remainingWakeId = PendingDispatchId.make("pd-recon-prune-remaining-wake");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(child, "stopped"),
          assistantText: "already has durable wake",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      seedPromotedChildren: [{ childThreadId: child, parentThreadId: parent }],
      persistedEvents: [threadArchivedEvent(child), threadUnarchivedEvent(child)],
      seedPendingDispatches: [
        {
          id: staleArchivedWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: null,
          error: "thread archived",
          status: "killed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
        {
          id: remainingWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "already has durable wake",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).toBe(remainingWakeId);
  });

  it("clears promoted markers when pruning the only wait-cancellable archived wake", async () => {
    const child = ThreadId.make("recon-prune-promoted-archive-wake-child");
    const parent = ThreadId.make("recon-prune-promoted-archive-wake-parent");
    const staleArchivedWakeId = PendingDispatchId.make("pd-recon-prune-promoted-archive");
    const remainingWakeId = PendingDispatchId.make("pd-recon-prune-promoted-remaining");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(child, "stopped"),
          assistantText: "remaining durable wake",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      seedPromotedChildren: [{ childThreadId: child, parentThreadId: parent }],
      persistedEvents: [threadArchivedEvent(child), threadUnarchivedEvent(child)],
      seedPendingDispatches: [
        {
          id: staleArchivedWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: null,
          error: "thread archived",
          status: "killed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
        {
          id: remainingWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "remaining durable wake",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).toBe(remainingWakeId);
    expect(await harness.listPromotedChildren()).toEqual([]);
  });

  it("clears wait-delivery state when pruning a delivered stale archived wake", async () => {
    const child = ThreadId.make("recon-prune-delivered-archive-wake-child");
    const parent = ThreadId.make("recon-prune-delivered-archive-wake-parent");
    const turn1 = TurnId.make("turn-1");
    const staleArchivedWakeId = PendingDispatchId.make("pd-recon-prune-delivered-archive");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      seedPromotedChildren: [{ childThreadId: child, parentThreadId: parent }],
      seedWaitDeliveries: [{ childThreadId: child, parentThreadId: parent }],
      persistedEvents: [
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
        turnStartRequestedEvent(child),
      ],
      seedPendingDispatches: [
        {
          id: staleArchivedWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: null,
          error: "thread archived",
          status: "killed",
          commandId: null,
          deliveredByWait: true,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    expect(await harness.listPendingDispatches()).toEqual([]);
    expect(await harness.listWaitDeliveries()).toEqual([]);
    await harness.feed(sessionSetEvent(child, "running", turn1));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "stopped"),
        assistantText: "completion after stale archive wake",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", turn1));

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.status).toBe("completed");
    expect(pendingRows[0]!.deliveredByWait).toBe(false);
  });

  it("keeps current archived wake when a child is archived again after unarchive", async () => {
    const parent = ThreadId.make("recon-archive-unarchive-rearchive-parent");
    const child = ThreadId.make("recon-archive-unarchive-rearchive-child");
    const currentArchivedWakeId = PendingDispatchId.make("pd-recon-rearchive-current");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: null,
          session: makeSession(child, "ready"),
          archivedAt: now,
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
        threadArchivedEvent(child),
      ],
      seedPendingDispatches: [
        {
          id: currentArchivedWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: null,
          error: "thread archived",
          status: "killed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("killed");
    expect(result.error).toBe("thread archived");
    expect(await harness.listPendingDispatches()).toHaveLength(1);
  });

  it("keeps current archived wake when active re-archive detail cannot settle", async () => {
    const parent = ThreadId.make("recon-active-rearchive-parent");
    const child = ThreadId.make("recon-active-rearchive-child");
    const currentArchivedWakeId = PendingDispatchId.make("pd-recon-active-rearchive-current");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
        threadArchivedEvent(child),
      ],
      seedPendingDispatches: [
        {
          id: currentArchivedWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: null,
          error: "thread archived",
          status: "killed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).toBe(currentArchivedWakeId);
  });

  it("prunes prior completed wake but keeps active re-archive wake after replacement start", async () => {
    const parent = ThreadId.make("recon-started-active-rearchive-parent");
    const child = ThreadId.make("recon-started-active-rearchive-child");
    const oldCompletedWakeId = PendingDispatchId.make("pd-recon-started-rearchive-old");
    const currentArchivedWakeId = PendingDispatchId.make("pd-recon-started-rearchive-current");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn2),
          session: makeSession(child, "running", turn2),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn2),
        threadArchivedEvent(child),
      ],
      seedPendingDispatches: [
        {
          id: oldCompletedWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "old completed wake",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
        {
          id: currentArchivedWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: null,
          error: "thread archived",
          status: "killed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).toBe(currentArchivedWakeId);
  });

  it("releases live archive leases even when projection is still running", async () => {
    const parent = ThreadId.make("live-archive-running-parent");
    const childIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`live-archive-running-child-${index + 1}`),
    );
    const harness = await createHarness({
      threads: childIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: null,
          session: makeSession(threadId, "running", TurnId.make("turn-1")),
        }),
      ),
    });
    for (const childThreadId of childIds) {
      await harness.register({
        parentThreadId: parent,
        childThreadId,
        detached: false,
        model: codexModel,
        spawnedAtMs: 0,
      });
    }
    for (const childThreadId of childIds) {
      await harness.seedDispatchLease(childThreadId);
    }
    expect(await harness.canAcquireDispatchLease()).toBe(false);

    for (const childThreadId of childIds) {
      await harness.feed(threadArchivedEvent(childThreadId));
    }

    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => entry.settled)).toBe(true);
    expect(await harness.canAcquireDispatchLease()).toBe(true);
  });

  it("reacquires live archive leases when still-running children are unarchived", async () => {
    const parent = ThreadId.make("live-unarchive-running-parent");
    const childIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`live-unarchive-running-child-${index + 1}`),
    );
    const harness = await createHarness({
      threads: childIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: null,
          session: makeSession(threadId, "running", TurnId.make("turn-1")),
        }),
      ),
    });
    for (const childThreadId of childIds) {
      await harness.register({
        parentThreadId: parent,
        childThreadId,
        detached: false,
        model: codexModel,
        spawnedAtMs: 0,
      });
      await harness.seedDispatchLease(childThreadId);
      await harness.feed(threadArchivedEvent(childThreadId));
    }
    expect(await harness.canAcquireDispatchLease()).toBe(true);

    for (const childThreadId of childIds) {
      await harness.feed(threadUnarchivedEvent(childThreadId));
    }

    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => !entry.settled)).toBe(true);
    expect(await harness.canAcquireDispatchLease()).toBe(false);
  });

  it("reacquires live archive leases on unarchive when projection is missing", async () => {
    const parent = ThreadId.make("live-unarchive-running-missing-projection-parent");
    const childIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`live-unarchive-running-missing-projection-child-${index + 1}`),
    );
    const harness = await createHarness({
      threads: childIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: null,
          session: makeSession(threadId, "running", TurnId.make("turn-1")),
        }),
      ),
    });
    for (const childThreadId of childIds) {
      await harness.register({
        parentThreadId: parent,
        childThreadId,
        detached: false,
        model: codexModel,
        spawnedAtMs: 0,
      });
      await harness.seedDispatchLease(childThreadId);
      await harness.feed(threadArchivedEvent(childThreadId));
      harness.removeThread(childThreadId);
    }
    expect(await harness.canAcquireDispatchLease()).toBe(true);

    for (const childThreadId of childIds) {
      await harness.feed(threadUnarchivedEvent(childThreadId));
    }

    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => !entry.settled)).toBe(true);
    expect(await harness.canAcquireDispatchLease()).toBe(false);
  });

  it("does not reacquire live archive leases on unarchive when projection is inactive", async () => {
    const parent = ThreadId.make("live-unarchive-inactive-parent");
    const childIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`live-unarchive-inactive-child-${index + 1}`),
    );
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: childIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(threadId, "running", turn1),
        }),
      ),
    });
    for (const childThreadId of childIds) {
      await harness.register({
        parentThreadId: parent,
        childThreadId,
        detached: false,
        model: codexModel,
        spawnedAtMs: 0,
      });
      await harness.seedDispatchLease(childThreadId);
      await harness.feed(threadArchivedEvent(childThreadId));
      harness.setThread(
        makeThreadState({
          threadId: childThreadId,
          parentThreadId: parent,
          latestTurn: null,
          session: makeSession(childThreadId, "stopped"),
        }),
      );
    }
    expect(await harness.canAcquireDispatchLease()).toBe(true);

    for (const childThreadId of childIds) {
      await harness.feed(threadUnarchivedEvent(childThreadId));
    }

    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => !entry.settled)).toBe(true);
    expect(await harness.canAcquireDispatchLease()).toBe(true);
  });

  it("drains deferred steers when unarchiving idle children", async () => {
    const parent = ThreadId.make("live-unarchive-idle-steer-parent");
    const child = ThreadId.make("live-unarchive-idle-steer-child");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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
    await harness.feed(threadArchivedEvent(child));
    await harness.insertPendingDispatch({
      id: PendingDispatchId.make("pd-live-unarchive-idle-steer"),
      kind: "child_steer",
      targetThreadId: child,
      sourceChildId: null,
      text: "continue after unarchive",
      error: null,
      status: null,
      commandId: null,
      deliveredByWait: false,
      waitCancellable: false,
      createdAt: now as unknown as PendingDispatch["createdAt"],
    });
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: null,
        session: makeSession(child, "stopped"),
      }),
    );

    await harness.feed(threadUnarchivedEvent(child));

    const steerStarts = harness.dispatched.filter(
      (command): command is Extract<OrchestrationCommand, { readonly type: "thread.turn.start" }> =>
        command.type === "thread.turn.start" && command.threadId === child,
    );
    expect(steerStarts).toHaveLength(1);
    expect(steerStarts[0]?.message.text).toBe("continue after unarchive");
    expect(await harness.listPendingDispatches()).toEqual([]);
  });

  it("clears live wait-delivery state when unarchiving a delivered archived wake", async () => {
    const parent = ThreadId.make("live-unarchive-delivered-archive-wake-parent");
    const child = ThreadId.make("live-unarchive-delivered-archive-wake-child");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    await harness.feed(threadArchivedEvent(child));
    const archivedResult = await runtimeRun(harness, child);
    expect(archivedResult.status).toBe("killed");
    expect(archivedResult.error).toBe("thread archived");
    await Effect.runPromise(harness.coordinator.markWaitDelivered([archivedResult]));
    expect(await harness.listWaitDeliveries()).toHaveLength(1);
    expect((await harness.listPendingDispatches())[0]?.deliveredByWait).toBe(true);

    await harness.feed(threadUnarchivedEvent(child));
    expect(await harness.listPendingDispatches()).toEqual([]);
    expect(await harness.listWaitDeliveries()).toEqual([]);

    await harness.feed(turnStartRequestedEvent(child));
    await harness.feed(sessionSetEvent(child, "running", turn2));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn2),
        session: makeSession(child, "stopped"),
        assistantText: "completion after delivered archive wake",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", turn2));

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.deliveredByWait).toBe(false);
    expect(pendingRows[0]!.text).toBe("completion after delivered archive wake");
  });

  it("releases and settles terminal archives before child registration", async () => {
    const parent = ThreadId.make("live-archive-pre-register-parent");
    const childIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`live-archive-pre-register-child-${index + 1}`),
    );
    const harness = await createHarness({
      threads: childIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: null,
          session: makeSession(threadId, "stopped"),
          archivedAt: now,
        }),
      ),
    });
    for (const childThreadId of childIds) {
      await harness.seedDispatchLease(childThreadId);
    }
    expect(await harness.canAcquireDispatchLease()).toBe(false);

    for (const childThreadId of childIds) {
      await harness.feed(threadArchivedEvent(childThreadId));
    }

    expect(await harness.canAcquireDispatchLease()).toBe(true);

    for (const childThreadId of childIds) {
      await harness.register({
        parentThreadId: parent,
        childThreadId,
        detached: false,
        model: codexModel,
        spawnedAtMs: 0,
      });
    }
    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => entry.settled)).toBe(true);
  });

  it("reopens live archived children when they are unarchived before a new turn", async () => {
    const parent = ThreadId.make("live-archive-unarchive-pending-parent");
    const childIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`live-archive-unarchive-pending-child-${index + 1}`),
    );
    const harness = await createHarness({
      threads: childIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: null,
          session: makeSession(threadId, "stopped"),
        }),
      ),
    });
    for (const childThreadId of childIds) {
      await harness.register({
        parentThreadId: parent,
        childThreadId,
        detached: false,
        model: codexModel,
        spawnedAtMs: 0,
      });
      await harness.feed(threadArchivedEvent(childThreadId));
      await harness.feed(threadUnarchivedEvent(childThreadId));
      await harness.feed(turnStartRequestedEvent(childThreadId));
    }

    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => !entry.settled)).toBe(true);
    expect(await harness.canAcquireDispatchLease()).toBe(false);
  });

  it("reopens live archived completions on a later turn start after unarchive", async () => {
    const parent = ThreadId.make("live-archive-completed-unarchive-turn-parent");
    const childIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`live-archive-completed-unarchive-turn-child-${index + 1}`),
    );
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: childIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(threadId, "stopped"),
          archivedAt: now,
          assistantText: "completed before unarchive",
        }),
      ),
    });
    for (const childThreadId of childIds) {
      await harness.register({
        parentThreadId: parent,
        childThreadId,
        detached: false,
        model: codexModel,
        spawnedAtMs: 0,
      });
      await harness.feed(threadUnarchivedEvent(childThreadId));
      await harness.feed(turnStartRequestedEvent(childThreadId));
    }

    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => !entry.settled)).toBe(true);
    expect(await harness.canAcquireDispatchLease()).toBe(false);
  });

  it("drops stale queued wakes when an unarchived terminal child starts a new turn", async () => {
    const child = ThreadId.make("live-unarchive-terminal-drop-stale-wake-child");
    const parent = ThreadId.make("live-unarchive-terminal-drop-stale-wake-parent");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "stopped"),
          archivedAt: now,
          assistantText: "old result",
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
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    await harness.feed(threadUnarchivedEvent(child));
    expect(await harness.listPendingDispatches()).toHaveLength(1);
    await harness.feed(turnStartRequestedEvent(child));
    expect(await harness.listPendingDispatches()).toEqual([]);
    await harness.feed(sessionSetEvent(child, "running", turn2));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn2),
        session: makeSession(child, "stopped"),
        assistantText: "new result",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", turn2));

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.text).toBe("new result");
  });

  it("keeps replayed terminal wakes until unarchived children start a new turn", async () => {
    const child = ThreadId.make("recon-unarchive-terminal-prune-stale-wake-child");
    const parent = ThreadId.make("recon-unarchive-terminal-prune-stale-wake-parent");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const staleWakeId = PendingDispatchId.make("pd-recon-unarchive-terminal-stale");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "stopped"),
          assistantText: "old replayed result",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
      ],
      seedPendingDispatches: [
        {
          id: staleWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "old replayed result",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    expect(await harness.listPendingDispatches()).toHaveLength(1);
    await harness.feed(turnStartRequestedEvent(child));
    expect(await harness.listPendingDispatches()).toEqual([]);
    await harness.feed(sessionSetEvent(child, "running", turn2));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn2),
        session: makeSession(child, "stopped"),
        assistantText: "new replayed result",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", turn2));

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.text).toBe("new replayed result");
  });

  it("prunes replayed terminal wakes when a newer unarchived turn already started", async () => {
    const child = ThreadId.make("recon-unarchive-terminal-started-prune-wake-child");
    const parent = ThreadId.make("recon-unarchive-terminal-started-prune-wake-parent");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const staleWakeId = PendingDispatchId.make("pd-recon-unarchive-terminal-started-stale");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", TurnId.make("turn-2")),
          session: makeSession(child, "running", TurnId.make("turn-2")),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
        turnStartRequestedEvent(child),
      ],
      seedPendingDispatches: [
        {
          id: staleWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "old replayed result",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    expect(await harness.listPendingDispatches()).toEqual([]);
    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(1);
    expect(entries[0]!.settled).toBe(false);
    expect(entries[0]!.detached).toBe(true);
  });

  it("replaces replayed stale terminal wakes after a replacement turn completes", async () => {
    const child = ThreadId.make("recon-unarchive-terminal-replace-stale-wake-child");
    const parent = ThreadId.make("recon-unarchive-terminal-replace-stale-wake-parent");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const staleWakeId = PendingDispatchId.make("pd-recon-unarchive-terminal-replace-stale");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn2),
          session: makeSession(child, "stopped"),
          assistantText: "replacement result",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn2),
        turnDiffEvent(child, "ready", turn2),
      ],
      seedPendingDispatches: [
        {
          id: staleWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "old result",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).not.toBe(staleWakeId);
    expect(pendingRows[0]!.text).toBe("replacement result");
  });

  it("replaces stale wakes after archive-timeout unarchive and replacement turn replay", async () => {
    const child = ThreadId.make("recon-unarchive-timeout-replace-stale-wake-child");
    const parent = ThreadId.make("recon-unarchive-timeout-replace-stale-wake-parent");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const staleWakeId = PendingDispatchId.make("pd-recon-unarchive-timeout-replace-stale");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn2),
          session: makeSession(child, "stopped"),
          assistantText: "replacement after archive timeout",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn2),
        turnDiffEvent(child, "ready", turn2),
      ],
      seedPendingDispatches: [
        {
          id: staleWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "old result before archive timeout",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
      slowThreadDetailReadNumbers: [{ threadId: child, readNumbers: [2] }],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).not.toBe(staleWakeId);
    expect(pendingRows[0]!.text).toBe("replacement after archive timeout");
  });

  it("clears wait-delivery tombstones when an unarchived terminal child starts a new turn", async () => {
    const child = ThreadId.make("recon-unarchive-terminal-tombstone-new-turn-child");
    const parent = ThreadId.make("recon-unarchive-terminal-tombstone-new-turn-parent");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "stopped"),
          assistantText: "old delivered result",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      seedWaitDeliveries: [{ childThreadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
      ],
    });

    expect(await harness.listPendingDispatches()).toEqual([]);
    expect(await harness.listWaitDeliveries()).toHaveLength(1);
    await harness.feed(turnStartRequestedEvent(child));
    expect(await harness.listWaitDeliveries()).toEqual([]);
    await harness.feed(sessionSetEvent(child, "running", turn2));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn2),
        session: makeSession(child, "stopped"),
        assistantText: "new result after tombstone",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", turn2));

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.text).toBe("new result after tombstone");
  });

  it("preserves claimed terminal wakes when replay text detail is unavailable", async () => {
    const child = ThreadId.make("recon-unarchive-terminal-claimed-detail-lag-child");
    const parent = ThreadId.make("recon-unarchive-terminal-claimed-detail-lag-parent");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const claimedWakeId = PendingDispatchId.make("pd-recon-unarchive-terminal-claimed");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn2),
          session: makeSession(child, "stopped"),
          assistantText: "claimed replacement result",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadUnarchivedEvent(child),
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn2),
        turnDiffEvent(child, "ready", turn2),
      ],
      seedPendingDispatches: [
        {
          id: claimedWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "claimed replacement result",
          error: null,
          status: "completed",
          commandId: "claimed-restart-command",
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
      slowThreadDetailReadNumbers: [{ threadId: child, readNumbers: [2] }],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).toBe(claimedWakeId);
    expect(pendingRows[0]!.commandId).toBe("claimed-restart-command");
  });

  it("preserves claimed terminal wakes when assistant messages lag the terminal projection", async () => {
    const child = ThreadId.make("recon-unarchive-terminal-claimed-message-lag-child");
    const parent = ThreadId.make("recon-unarchive-terminal-claimed-message-lag-parent");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const laggedAssistantTurn = {
      ...makeLatestTurn("completed", turn2),
      assistantMessageId: MessageId.make("assistant-message-lag"),
    };
    const claimedWakeId = PendingDispatchId.make("pd-recon-unarchive-terminal-claimed-message");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: laggedAssistantTurn,
          session: makeSession(child, "stopped"),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadUnarchivedEvent(child),
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn2),
        turnDiffEvent(child, "ready", turn2),
      ],
      seedPendingDispatches: [
        {
          id: claimedWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "claimed result before message projection caught up",
          error: null,
          status: "completed",
          commandId: "claimed-message-lag-command",
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).toBe(claimedWakeId);
    expect(pendingRows[0]!.commandId).toBe("claimed-message-lag-command");
  });

  it("replaces claimed terminal wakes when completed projection has a real null result", async () => {
    const child = ThreadId.make("recon-unarchive-terminal-claimed-null-result-child");
    const parent = ThreadId.make("recon-unarchive-terminal-claimed-null-result-parent");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const staleWakeId = PendingDispatchId.make("pd-recon-unarchive-terminal-claimed-null");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn2),
          session: makeSession(child, "stopped"),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadUnarchivedEvent(child),
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn2),
        turnDiffEvent(child, "ready", turn2),
      ],
      seedPendingDispatches: [
        {
          id: staleWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "old claimed result",
          error: null,
          status: "completed",
          commandId: "claimed-null-result-command",
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).not.toBe(staleWakeId);
    expect(pendingRows[0]!.text).toBe(null);
  });

  it("replaces unclaimed terminal wakes when replay text detail is unavailable", async () => {
    const child = ThreadId.make("recon-unarchive-terminal-unclaimed-detail-lag-child");
    const parent = ThreadId.make("recon-unarchive-terminal-unclaimed-detail-lag-parent");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const staleWakeId = PendingDispatchId.make("pd-recon-unarchive-terminal-unclaimed");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn2),
          session: makeSession(child, "stopped"),
          assistantText: "unclaimed replacement result",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadUnarchivedEvent(child),
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn2),
        turnDiffEvent(child, "ready", turn2),
      ],
      seedPendingDispatches: [
        {
          id: staleWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "old unclaimed result",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
      slowThreadDetailReadNumbers: [{ threadId: child, readNumbers: [2] }],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).not.toBe(staleWakeId);
    expect(pendingRows[0]!.text).toBe("unclaimed replacement result");
  });

  it("reopens projection-only unarchived terminal children on replacement turn start", async () => {
    const child = ThreadId.make("recon-unarchive-projection-terminal-reopen-child");
    const parent = ThreadId.make("recon-unarchive-projection-terminal-reopen-parent");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "stopped"),
          assistantText: "old projection-only result",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [threadArchivedEvent(child), threadUnarchivedEvent(child)],
      slowThreadDetailReadNumbers: [{ threadId: child, readNumbers: [2] }],
    });

    expect((await harness.listPendingDispatches())[0]?.text).toBe("old projection-only result");
    await harness.feed(turnDiffEvent(child, "ready", turn1));
    expect((await harness.listPendingDispatches())[0]?.text).toBe("old projection-only result");
    await harness.feed(turnStartRequestedEvent(child));
    expect(await harness.listPendingDispatches()).toEqual([]);
    await harness.feed(sessionSetEvent(child, "running", turn2));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn2),
        session: makeSession(child, "stopped"),
        assistantText: "new projection-only result",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", turn2));

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("new projection-only result");
    expect((await harness.listPendingDispatches())[0]?.text).toBe("new projection-only result");
  });

  it("keeps post-unarchive completion wake when the same turn later starts another turn", async () => {
    const child = ThreadId.make("recon-unarchive-same-turn-complete-child");
    const parent = ThreadId.make("recon-unarchive-same-turn-complete-parent");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn1),
          session: makeSession(child, "running", turn1),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [threadArchivedEvent(child), threadUnarchivedEvent(child)],
      slowThreadDetailReadNumbers: [{ threadId: child, readNumbers: [2] }],
    });

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", turn1),
        session: makeSession(child, "stopped"),
        assistantText: "post-unarchive same-turn result",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", turn1));
    expect((await harness.listPendingDispatches())[0]?.text).toBe(
      "post-unarchive same-turn result",
    );

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("running", turn2),
        session: makeSession(child, "running", turn2),
      }),
    );
    await harness.feed(turnStartRequestedEvent(child));

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.text).toBe("post-unarchive same-turn result");
  });

  it("keeps replayed post-unarchive completion wake before a later replacement start", async () => {
    const child = ThreadId.make("recon-unarchive-replayed-same-turn-complete-child");
    const parent = ThreadId.make("recon-unarchive-replayed-same-turn-complete-parent");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const validWakeId = PendingDispatchId.make("pd-recon-unarchive-replayed-same-turn-complete");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn2),
          session: makeSession(child, "running", turn2),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
        turnDiffEvent(child, "ready", turn1),
        turnStartRequestedEvent(child),
      ],
      seedPendingDispatches: [
        {
          id: validWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "post-unarchive replayed result",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
      slowThreadDetailReadNumbers: [{ threadId: child, readNumbers: [2] }],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).toBe(validWakeId);
    expect(pendingRows[0]!.text).toBe("post-unarchive replayed result");
  });

  it("synthesizes missing replayed terminal wakes after unarchive", async () => {
    const child = ThreadId.make("recon-unarchive-terminal-missing-wake-child");
    const parent = ThreadId.make("recon-unarchive-terminal-missing-wake-parent");
    const parentTurn = TurnId.make("parent-turn");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "stopped"),
          assistantText: "replayed result without durable wake",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
      ],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.text).toBe("replayed result without durable wake");
  });

  it("keeps replayed post-unarchive wakes after a later child turn starts", async () => {
    const child = ThreadId.make("recon-unarchive-terminal-keep-new-wake-child");
    const parent = ThreadId.make("recon-unarchive-terminal-keep-new-wake-parent");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const newWakeId = PendingDispatchId.make("pd-recon-unarchive-terminal-new-wake");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn2),
          session: makeSession(child, "stopped"),
          assistantText: "new replayed result",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn2),
        turnDiffEvent(child, "ready", turn2),
      ],
      seedPendingDispatches: [
        {
          id: newWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "new replayed result",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).toBe(newWakeId);
    expect(pendingRows[0]!.text).toBe("new replayed result");
  });

  it("keeps replayed post-unarchive wakes after a subsequent child turn starts", async () => {
    const child = ThreadId.make("recon-unarchive-terminal-keep-after-later-start-child");
    const parent = ThreadId.make("recon-unarchive-terminal-keep-after-later-start-parent");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const turn3 = TurnId.make("turn-3");
    const newWakeId = PendingDispatchId.make(
      "pd-recon-unarchive-terminal-new-wake-after-later-start",
    );
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn3),
          session: makeSession(child, "running", turn3),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn2),
        turnDiffEvent(child, "ready", turn2),
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn3),
      ],
      seedPendingDispatches: [
        {
          id: newWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "post-unarchive result before later start",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).toBe(newWakeId);
    expect(pendingRows[0]!.text).toBe("post-unarchive result before later start");
  });

  it("prunes stale post-unarchive wake siblings after a subsequent child turn starts", async () => {
    const child = ThreadId.make("recon-unarchive-terminal-prune-sibling-after-later-start-child");
    const parent = ThreadId.make("recon-unarchive-terminal-prune-sibling-after-later-start-parent");
    const turn1 = TurnId.make("turn-1");
    const turn2 = TurnId.make("turn-2");
    const turn3 = TurnId.make("turn-3");
    const staleWakeId = PendingDispatchId.make(
      "pd-recon-unarchive-terminal-stale-sibling-after-later-start",
    );
    const newWakeId = PendingDispatchId.make(
      "pd-recon-unarchive-terminal-current-sibling-after-later-start",
    );
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", turn3),
          session: makeSession(child, "running", turn3),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnDiffEvent(child, "ready", turn1),
        threadArchivedEvent(child),
        threadUnarchivedEvent(child),
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn2),
        turnDiffEvent(child, "ready", turn2),
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn3),
      ],
      seedPendingDispatches: [
        {
          id: staleWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "old pre-unarchive result",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: "2026-06-17T09:59:59.000Z" as unknown as PendingDispatch["createdAt"],
        },
        {
          id: newWakeId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "post-unarchive result before later start",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]!.id).toBe(newWakeId);
    expect(pendingRows[0]!.text).toBe("post-unarchive result before later start");
  });

  it("preserves projected completion when shell reconciliation times out", async () => {
    const child = ThreadId.make("projected-completed-stopped-slow-shell-child");
    const parent = ThreadId.make("projected-completed-stopped-slow-shell-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "stopped"),
          archivedAt: now,
          assistantText: "projected completion survived",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [],
      slowThreadShellIds: [child],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("projected completion survived");
  });

  it("does not restore stale completed projection after a newer user message", async () => {
    const child = ThreadId.make("projected-stale-completed-newer-user-child");
    const parent = ThreadId.make("projected-stale-completed-newer-user-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          latestUserMessageAt: "2026-06-17T10:00:01.000Z",
          session: makeSession(child, "stopped"),
          assistantText: "stale completed output",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("failed");
    expect(result.error).toBe("session stopped");
  });

  it("does not restore stale completed projection after a newer indented sub-agent wake", async () => {
    const child = ThreadId.make("projected-stale-completed-system-wake-child");
    const parent = ThreadId.make("projected-stale-completed-system-wake-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          latestSystemWakeAt: "2026-06-17T10:00:01.000Z",
          latestSystemWakeText: `  [sub-agent ${child} completed] newer wake`,
          session: makeSession(child, "stopped"),
          assistantText: "stale completed output",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("failed");
    expect(result.error).toBe("session stopped");
  });

  it("keeps boot-reconciled stopped running children pending", async () => {
    const parent = ThreadId.make("boot-stopped-running-parent");
    const childIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`boot-stopped-running-child-${index + 1}`),
    );
    const harness = await createHarness({
      threads: childIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(threadId, "stopped"),
        }),
      ),
      seedChildRows: childIds.map((threadId) => ({ threadId, parentThreadId: parent })),
      persistedEvents: [],
    });

    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => !entry.settled)).toBe(true);
    expect(await harness.canAcquireDispatchLease()).toBe(false);
  });

  it("does not let stale projection terminal override replayed pending starts", async () => {
    const parent = ThreadId.make("boot-stale-stopped-pending-parent");
    const childIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`boot-stale-stopped-pending-child-${index + 1}`),
    );
    const harness = await createHarness({
      threads: childIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: null,
          session: makeSession(threadId, "stopped"),
        }),
      ),
      seedChildRows: childIds.map((threadId) => ({ threadId, parentThreadId: parent })),
      persistedEvents: childIds.flatMap((threadId) => [
        sessionSetEvent(threadId, "stopped"),
        turnStartRequestedEvent(threadId),
      ]),
    });

    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => !entry.settled)).toBe(true);
    expect(await harness.canAcquireDispatchLease()).toBe(false);
  });

  it("does not let archived detail fallback override replayed pending starts", async () => {
    const parent = ThreadId.make("boot-stale-archived-pending-parent");
    const childIds = Array.from({ length: 6 }, (_, index) =>
      ThreadId.make(`boot-stale-archived-pending-child-${index + 1}`),
    );
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: childIds.map((threadId) =>
        makeThreadState({
          threadId,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(threadId, "stopped"),
          archivedAt: now,
          assistantText: "stale archived completion",
        }),
      ),
      seedChildRows: childIds.map((threadId) => ({ threadId, parentThreadId: parent })),
      persistedEvents: childIds.map(turnStartRequestedEvent),
    });

    const entries = await runtimeListChildren(harness, parent);
    expect(entries).toHaveLength(6);
    expect(entries.every((entry) => !entry.settled)).toBe(true);
    expect(await harness.canAcquireDispatchLease()).toBe(false);
  });

  it("preserves replayed completion when a later stopped session cannot read shell", async () => {
    const child = ThreadId.make("recon-completed-stopped-slow-shell-child");
    const parent = ThreadId.make("recon-completed-stopped-slow-shell-parent");
    const turn1 = TurnId.make("turn-1");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed", turn1),
          session: makeSession(child, "stopped"),
          assistantText: "completed before shell lag",
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      persistedEvents: [
        turnStartRequestedEvent(child),
        sessionSetEvent(child, "running", turn1),
        turnDiffEvent(child, "ready", turn1),
        sessionSetEvent(child, "stopped"),
      ],
      slowThreadShellIds: [child],
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.error).toBe(null);
    expect(result.finalAssistantText).toBe("completed before shell lag");
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

  it("assertParent uses a bounded projection read for untracked children", async () => {
    const child = ThreadId.make("child-slow-assert-parent");
    const parent = ThreadId.make("parent-slow-assert-parent");
    const harness = await createHarness({
      slowThreadShellIds: [child],
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
        }),
      ],
    });

    const completed = await Effect.runPromise(
      harness.coordinator
        .assertParent(parent, child)
        .pipe(Effect.exit, Effect.timeoutOption("2 seconds")),
    );

    expect(Option.isSome(completed)).toBe(true);
    if (Option.isNone(completed)) {
      throw new Error("assertParent did not return within the bounded timeout");
    }
    expect(Exit.isFailure(completed.value)).toBe(true);
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
    const turnStarts = harness.dispatched.filter((command) => command.type === "thread.turn.start");
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

  it("R-A: a late wait after a dispatched promoted wake does not recreate a fallback", async () => {
    const child = ThreadId.make("promote-dispatched-late-wait-child");
    const parent = ThreadId.make("promote-dispatched-late-wait-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
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
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        assistantText: "already notified parent",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready"));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(1);
    expect(await harness.listPendingDispatches()).toHaveLength(0);

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("already notified parent");
    await Effect.runPromise(harness.coordinator.markWaitDelivered([result]));

    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(1);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: promotion wakes a foreground child that completed before promotion landed", async () => {
    const child = ThreadId.make("promote-race-child");
    const parent = ThreadId.make("promote-race-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
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
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        assistantText: "completed before promote",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready"));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);

    await Effect.runPromise(harness.coordinator.promoteToWake([child]));
    const turnStarts = harness.dispatched.filter((command) => command.type === "thread.turn.start");
    expect(turnStarts).toHaveLength(1);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
  });

  it("R-A: promotion wakes a foreground child that completes while promotion persistence is blocked", async () => {
    const child = ThreadId.make("promote-persist-race-child");
    const parent = ThreadId.make("promote-persist-race-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
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
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });

    const sqlLock = await harness.holdSqlWriteLock();
    const promotePromise = Effect.runPromise(harness.coordinator.promoteToWake([child]));
    await Effect.runPromise(Effect.sleep("10 millis"));

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        assistantText: "completed during promotion persist",
      }),
    );
    const feedPromise = harness.feed(turnDiffEvent(child, "ready"));

    await sqlLock.release();
    await Promise.all([promotePromise, feedPromise]);

    const turnStarts = harness.dispatched.filter((command) => command.type === "thread.turn.start");
    expect(turnStarts).toHaveLength(1);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
  });

  it("R-A: concurrent promotion waits for the durable marker write", async () => {
    const child = ThreadId.make("promote-concurrent-persist-child");
    const parent = ThreadId.make("promote-concurrent-persist-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
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
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });

    const sqlLock = await harness.holdSqlWriteLock();
    const firstPromotion = Effect.runPromise(harness.coordinator.promoteToWake([child]));
    await Effect.runPromise(Effect.sleep("10 millis"));

    let secondSettled = false;
    const secondPromotion = Effect.runPromise(harness.coordinator.promoteToWake([child])).then(
      () => {
        secondSettled = true;
      },
    );
    await Effect.runPromise(Effect.sleep("10 millis"));
    expect(secondSettled).toBe(false);

    await sqlLock.release();
    await Promise.all([firstPromotion, secondPromotion]);
    expect(secondSettled).toBe(true);
    expect(await harness.listPromotedChildren()).toEqual([{ childThreadId: String(child) }]);
  });

  it("R-A: failed promotion persistence rolls back so a retry writes the marker", async () => {
    const child = ThreadId.make("promote-persist-fail-retry-child");
    const parent = ThreadId.make("promote-persist-fail-retry-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
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
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });

    await harness.failPromotedChildInserts();
    const failed = await Effect.runPromiseExit(harness.coordinator.promoteToWake([child]));
    expect(Exit.isFailure(failed)).toBe(true);
    await harness.allowPromotedChildInserts();

    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    expect(await harness.listPromotedChildren()).toEqual([{ childThreadId: String(child) }]);
  });

  it("R-A: failed terminal promotion persistence can be retried to wake the parent", async () => {
    const child = ThreadId.make("promote-terminal-persist-fail-child");
    const parent = ThreadId.make("promote-terminal-persist-fail-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
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
      detached: false,
      model: codexModel,
      spawnedAtMs: 0,
    });
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        assistantText: "completed before promotion retry",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready"));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);

    await harness.failPromotedChildInserts();
    const failed = await Effect.runPromiseExit(harness.coordinator.promoteToWake([child]));
    expect(Exit.isFailure(failed)).toBe(true);
    await harness.allowPromotedChildInserts();

    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    const turnStarts = harness.dispatched.filter((command) => command.type === "thread.turn.start");
    expect(turnStarts).toHaveLength(1);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
  });

  it("R-A: terminal wait delivery cancels a queued promoted wake", async () => {
    const child = ThreadId.make("promote-wait-delivered-child");
    const parent = ThreadId.make("promote-wait-delivered-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
        }),
        // Parent is mid-turn, so a promoted completion queues a durable wake
        // until the active waiter receives the same terminal result.
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", TurnId.make("turn-parent")),
          session: makeSession(parent, "running", TurnId.make("turn-parent")),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        assistantText: "delivered by wait",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready"));
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("delivered by wait");
    await Effect.runPromise(harness.coordinator.markWaitDelivered([result]));
    // The durable fallback stays until the parent turn commits, so a crash
    // after the wait result but before parent completion still wakes the parent
    // on restart instead of losing the child result.
    const pendingAfterWait = await harness.listPendingDispatches();
    expect(pendingAfterWait).toHaveLength(1);
    expect(pendingAfterWait[0]?.deliveredByWait).toBe(true);
    expect(pendingAfterWait[0]?.waitCancellable).toBe(true);
    expect(await runtimeHasPending(harness, parent)).toBe(true);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent"), afterWaitDelivery),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready", TurnId.make("turn-parent")));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await harness.listWaitDeliveries()).toEqual([
      { childThreadId: String(child), parentThreadId: String(parent) },
    ]);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: parent failure before wait delivery keeps the fallback dispatchable", async () => {
    const child = ThreadId.make("promote-wait-parent-failed-child");
    const parent = ThreadId.make("promote-wait-parent-failed-parent");
    const parentTurn = TurnId.make("turn-parent");
    const childTurn = TurnId.make("turn-child");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", childTurn),
          session: makeSession(child, "running", childTurn),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", childTurn),
        session: makeSession(child, "ready"),
        assistantText: "deliver after parent failure",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", childTurn));
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    const result = await runtimeRun(harness, child);
    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("interrupted", parentTurn, now),
        session: makeSession(parent, "ready"),
      }),
    );
    await Effect.runPromise(harness.coordinator.markWaitDelivered([result]));

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(0);
    expect(await harness.listWaitDeliveries()).toEqual([]);
    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start" && command.threadId === parent,
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]?.message.text).toContain("deliver after parent failure");
  });

  it("R-A: projection-enriched wait delivery persists a fallback before child settlement", async () => {
    const child = ThreadId.make("promote-wait-enriched-no-row-child");
    const parent = ThreadId.make("promote-wait-enriched-no-row-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-child")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", TurnId.make("turn-parent")),
          session: makeSession(parent, "running", TurnId.make("turn-parent")),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    await Effect.runPromise(
      harness.coordinator.markWaitDelivered([
        {
          childThreadId: child,
          status: "completed",
          finalAssistantText: "projection-enriched result",
          error: null,
        },
      ]),
    );

    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]).toMatchObject({
      sourceChildId: child,
      targetThreadId: parent,
      text: "projection-enriched result",
      deliveredByWait: true,
      waitCancellable: true,
    });
    expect(await harness.listWaitDeliveries()).toEqual([
      { childThreadId: String(child), parentThreadId: String(parent) },
    ]);
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
  });

  it("R-A: interrupted promoted waits clear their active marker", async () => {
    const child = ThreadId.make("promote-wait-interrupted-marker-child");
    const parent = ThreadId.make("promote-wait-interrupted-marker-parent");
    const childTurn = TurnId.make("turn-child");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", childTurn),
          session: makeSession(child, "running", childTurn),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent")),
          session: makeSession(parent, "ready"),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", childTurn),
        session: makeSession(child, "ready"),
        assistantText: "completed after interrupted wait",
      }),
    );
    harness.setDetailSlow(child, true);
    const interrupted = await Effect.runPromise(
      harness.coordinator
        .waitSlice({ childThreadIds: [child], mode: "all", budgetDeadlineMs: FAR_FUTURE_MS })
        .pipe(Effect.timeoutOption("100 millis")),
    );
    expect(Option.isNone(interrupted)).toBe(true);

    harness.setDetailSlow(child, false);
    await harness.feed(turnDiffEvent(child, "ready", childTurn));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(1);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
  });

  it("R-A: wait delivery bounds the parent snapshot read", async () => {
    const child = ThreadId.make("promote-wait-delivery-bounded-parent-child");
    const parent = ThreadId.make("promote-wait-delivery-bounded-parent");
    const harness = await createHarness({
      slowThreadShellIds: [parent],
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-child")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", TurnId.make("turn-parent")),
          session: makeSession(parent, "running", TurnId.make("turn-parent")),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    const completed = await Effect.runPromise(
      harness.coordinator
        .markWaitDelivered([
          {
            childThreadId: child,
            status: "completed",
            finalAssistantText: "bounded parent snapshot",
            error: null,
          },
        ])
        .pipe(Effect.as(true), Effect.timeoutOption("2 seconds")),
    );

    expect(Option.isSome(completed)).toBe(true);
    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]).toMatchObject({
      sourceChildId: child,
      targetThreadId: parent,
      deliveredByWait: true,
      waitCancellable: true,
    });
    expect(await harness.listWaitDeliveries()).toEqual([
      { childThreadId: String(child), parentThreadId: String(parent) },
    ]);
  });

  it("R-A: interrupted wait delivery leaves a new fallback non-delivered", async () => {
    const child = ThreadId.make("promote-wait-delivery-interrupt-child");
    const parent = ThreadId.make("promote-wait-delivery-interrupt-parent");
    const harness = await createHarness({
      slowThreadShellIds: [parent],
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-child")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", TurnId.make("turn-parent")),
          session: makeSession(parent, "running", TurnId.make("turn-parent")),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    const completed = await Effect.runPromise(
      harness.coordinator
        .markWaitDelivered([
          {
            childThreadId: child,
            status: "completed",
            finalAssistantText: "interrupted delivery",
            error: null,
          },
        ])
        .pipe(Effect.as(true), Effect.timeoutOption("100 millis")),
    );

    expect(Option.isNone(completed)).toBe(true);
    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]).toMatchObject({
      sourceChildId: child,
      targetThreadId: parent,
      text: "interrupted delivery",
      deliveredByWait: false,
      waitCancellable: true,
    });
    expect(await harness.listWaitDeliveries()).toEqual([]);
    expect(await runtimeHasPending(harness, parent)).toBe(true);
  });

  it("R-A: interrupted wait delivery does not mark an existing fallback delivered in memory", async () => {
    const child = ThreadId.make("promote-wait-delivery-existing-interrupt-child");
    const parent = ThreadId.make("promote-wait-delivery-existing-interrupt-parent");
    const parentTurn = TurnId.make("turn-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-child")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", TurnId.make("turn-child")),
        session: makeSession(child, "ready"),
        assistantText: "existing interrupted delivery",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", TurnId.make("turn-child")));
    let pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]).toMatchObject({
      deliveredByWait: false,
      waitCancellable: true,
    });

    const result = await runtimeRun(harness, child);
    harness.setShellSlow(parent, true);
    const completed = await Effect.runPromise(
      harness.coordinator
        .markWaitDelivered([result])
        .pipe(Effect.as(true), Effect.timeoutOption("100 millis")),
    );

    expect(Option.isNone(completed)).toBe(true);
    pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.deliveredByWait).toBe(false);
    expect(await harness.listWaitDeliveries()).toEqual([]);

    harness.setShellSlow(parent, false);
    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("completed", parentTurn, afterWaitDelivery),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready", parentTurn));
    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start",
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]?.message.text).toContain("existing interrupted delivery");
    expect(await harness.listPendingDispatches()).toHaveLength(0);
  });

  it("R-A: one-shot terminal wait delivery survives a stale idle parent snapshot", async () => {
    const child = ThreadId.make("promote-wait-oneshot-idle-child");
    const parent = ThreadId.make("promote-wait-oneshot-idle-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent")),
          session: makeSession(parent, "ready"),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        assistantText: "delivered by one-shot wait",
      }),
    );
    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("delivered by one-shot wait");
    await Effect.runPromise(harness.coordinator.markWaitDelivered([result]));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(1);
    expect(await runtimeHasPending(harness, parent)).toBe(true);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent"), afterWaitDelivery),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready", TurnId.make("turn-parent")));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: active wait marker covers the one-shot projection check", async () => {
    const child = ThreadId.make("promote-wait-oneshot-race-child");
    const parent = ThreadId.make("promote-wait-oneshot-race-parent");
    const harness = await createHarness({
      slowThreadShellIds: [child],
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent")),
          session: makeSession(parent, "ready"),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    const waitPromise = runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
    await Effect.runPromise(Effect.sleep("10 millis"));
    harness.setShellSlow(child, false);
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        assistantText: "settled during one-shot check",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready"));

    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]).toMatchObject({
      deliveredByWait: false,
      waitCancellable: true,
    });

    const slice = await waitPromise;
    expect(slice.results[0]).toMatchObject({
      childThreadId: child,
      status: "completed",
      finalAssistantText: "settled during one-shot check",
    });
  });

  it("R-A: live settlement during a foreground wait queues instead of double-waking", async () => {
    const child = ThreadId.make("promote-wait-live-settle-child");
    const parent = ThreadId.make("promote-wait-live-settle-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent")),
          session: makeSession(parent, "ready"),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    const waitPromise = runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
    await Effect.runPromise(Effect.sleep("10 millis"));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        assistantText: "settled during active wait",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready"));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    let pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.deliveredByWait).toBe(false);
    expect(pendingRows[0]?.waitCancellable).toBe(true);

    const slice = await waitPromise;
    const result = slice.results[0]!;
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("settled during active wait");
    await Effect.runPromise(harness.coordinator.markWaitDelivered([result]));
    pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.deliveredByWait).toBe(true);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent-2"), afterWaitDelivery),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready", TurnId.make("turn-parent-2")));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: overlapping foreground waits keep the promoted wake cancellable", async () => {
    const child = ThreadId.make("promote-wait-overlap-child");
    const parent = ThreadId.make("promote-wait-overlap-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent")),
          session: makeSession(parent, "ready"),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    const firstWait = runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
    await Effect.runPromise(Effect.sleep("1 second"));
    const secondWait = runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);

    const firstSlice = await firstWait;
    expect(firstSlice.results[0]?.status).toBe("pending");

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        assistantText: "overlap delivered by wait",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready"));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    let pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.deliveredByWait).toBe(false);
    expect(pendingRows[0]?.waitCancellable).toBe(true);

    const secondSlice = await secondWait;
    const result = secondSlice.results[0]!;
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("overlap delivered by wait");
    await Effect.runPromise(harness.coordinator.markWaitDelivered([result]));
    pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.deliveredByWait).toBe(true);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent-2"), afterWaitDelivery),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready", TurnId.make("turn-parent-2")));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: abandoning one overlapping wait keeps another active waiter protected", async () => {
    const child = ThreadId.make("promote-wait-overlap-abandon-child");
    const parent = ThreadId.make("promote-wait-overlap-abandon-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent")),
          session: makeSession(parent, "ready"),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    const firstWait = runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
    await Effect.runPromise(Effect.sleep("10 millis"));
    const secondWait = runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
    await Effect.runPromise(Effect.sleep("10 millis"));

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        assistantText: "still protected by the second wait",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready"));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    let pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.deliveredByWait).toBe(false);
    expect(pendingRows[0]?.waitCancellable).toBe(true);

    const [firstSlice, secondSlice] = await Promise.all([firstWait, secondWait]);
    const firstResult = firstSlice.results[0]!;
    const secondResult = secondSlice.results[0]!;
    expect(firstResult.status).toBe("completed");
    expect(secondResult.status).toBe("completed");

    await Effect.runPromise(harness.coordinator.abandonWaitDelivery([child]));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.deliveredByWait).toBe(false);

    await Effect.runPromise(harness.coordinator.markWaitDelivered([secondResult]));
    pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.deliveredByWait).toBe(true);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent-2"), afterWaitDelivery),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready", TurnId.make("turn-parent-2")));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
  });

  it("R-A: abandoning the final promoted wait keeps the fallback queued while the parent is active", async () => {
    const child = ThreadId.make("promote-wait-abandon-active-parent-child");
    const parent = ThreadId.make("promote-wait-abandon-active-parent-parent");
    const parentTurn = TurnId.make("turn-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", TurnId.make("turn-child")),
          session: makeSession(child, "running", TurnId.make("turn-child")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    const wait = runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
    await Effect.runPromise(Effect.sleep("10 millis"));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", TurnId.make("turn-child")),
        session: makeSession(child, "ready"),
        assistantText: "queued after abandon",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", TurnId.make("turn-child")));

    const slice = await wait;
    expect(slice.results[0]?.status).toBe("completed");
    await Effect.runPromise(harness.coordinator.abandonWaitDelivery([child]));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    let pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.deliveredByWait).toBe(false);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("completed", parentTurn),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready", parentTurn));
    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start" && command.threadId === parent,
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]?.message.text).toContain("queued after abandon");
    pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(0);
  });

  it("R-A: wait delivery refuses to credit a newer parent turn", async () => {
    const child = ThreadId.make("promote-wait-parent-advanced-child");
    const parent = ThreadId.make("promote-wait-parent-advanced-parent");
    const waitingParentTurn = TurnId.make("turn-parent-waiting");
    const newerParentTurn = TurnId.make("turn-parent-newer");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", waitingParentTurn),
          session: makeSession(parent, "running", waitingParentTurn),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    const waitPromise = runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
    await Effect.runPromise(Effect.sleep("10 millis"));
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        assistantText: "not delivered to the newer parent turn",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready"));
    const slice = await waitPromise;
    const result = slice.results[0]!;
    expect(result.status).toBe("completed");
    expect(result.parentTurnIdAtWait).toBe(waitingParentTurn);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("running", newerParentTurn),
        session: makeSession(parent, "running", newerParentTurn),
      }),
    );
    await Effect.runPromise(harness.coordinator.markWaitDelivered([result]));

    let pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.deliveredByWait).toBe(false);
    expect(await harness.listWaitDeliveries()).toEqual([]);
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("completed", newerParentTurn, afterWaitDelivery),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready", newerParentTurn));

    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start" && command.threadId === parent,
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]?.message.text).toContain("not delivered to the newer parent turn");
    pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(0);
  });

  it("R-A: timeout rows preserve the waiting parent turn guard", async () => {
    const child = ThreadId.make("promote-wait-timeout-parent-turn-child");
    const parent = ThreadId.make("promote-wait-timeout-parent-turn-parent");
    const waitingParentTurn = TurnId.make("turn-parent-waiting");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", waitingParentTurn),
          session: makeSession(parent, "running", waitingParentTurn),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    const slice = await runtimeWaitSlice(harness, [child], PAST_MS);
    expect(slice.results[0]).toMatchObject({
      childThreadId: child,
      status: "timeout",
      parentTurnIdAtWait: waitingParentTurn,
    });
  });

  it("R-A: one-shot wait delivery dispatches the fallback after a failed parent turn", async () => {
    const child = ThreadId.make("promote-wait-oneshot-failed-parent-child");
    const parent = ThreadId.make("promote-wait-oneshot-failed-parent");
    const parentTurn = TurnId.make("turn-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
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
    await Effect.runPromise(harness.coordinator.promoteToWake([child]));

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        assistantText: "wait result after parent failure",
      }),
    );
    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("wait result after parent failure");
    await Effect.runPromise(harness.coordinator.markWaitDelivered([result]));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("error", parentTurn, afterWaitDelivery),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "error", parentTurn));
    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start" && command.threadId === parent,
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]?.message.text).toContain("wait result after parent failure");
    expect(await harness.listPendingDispatches()).toHaveLength(0);
  });

  it("R-A: same-millisecond failed parent dispatches a wait-delivered fallback", async () => {
    const child = ThreadId.make("promote-wait-same-ms-failed-parent-child");
    const parent = ThreadId.make("promote-wait-same-ms-failed-parent");
    const parentTurn = TurnId.make("turn-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-delivered-same-ms-failed");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
      ],
      seedWaitDeliveries: [{ childThreadId: child, parentThreadId: parent, deliveredAt: now }],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "wait result after same-ms parent failure",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: true,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("error", parentTurn, now),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "error", parentTurn));
    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start" && command.threadId === parent,
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]?.message.text).toContain("wait result after same-ms parent failure");
    expect(await harness.listPendingDispatches()).toHaveLength(0);
  });

  it("R-A: aged drains retain wait-delivered promoted wakes until the parent is idle", async () => {
    const child = ThreadId.make("promote-wait-delivered-aged-child");
    const parent = ThreadId.make("promote-wait-delivered-aged-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-delivered-aged");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", TurnId.make("turn-parent")),
          session: makeSession(parent, "running", TurnId.make("turn-parent")),
        }),
      ],
      seedWaitDeliveries: [{ childThreadId: child, parentThreadId: parent, deliveredAt: now }],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "already delivered by wait",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: true,
          waitCancellable: true,
          createdAt: "2000-01-01T00:00:00.000Z" as unknown as PendingDispatch["createdAt"],
        },
      ],
    });
    expect(await runtimeHasPending(harness, parent)).toBe(true);

    await harness.feed(turnDiffEvent(parent, "ready", TurnId.make("turn-parent")));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    const pendingAfterAgedDrain = await harness.listPendingDispatches();
    expect(pendingAfterAgedDrain).toHaveLength(1);
    expect(pendingAfterAgedDrain[0]?.deliveredByWait).toBe(true);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent"), afterWaitDelivery),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready", TurnId.make("turn-parent")));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: wait-delivered queued wake dispatches after a failed parent turn", async () => {
    const child = ThreadId.make("promote-wait-delivered-failed-parent-child");
    const parent = ThreadId.make("promote-wait-delivered-failed-parent");
    const parentTurn = TurnId.make("turn-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-delivered-failed-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
      ],
      seedWaitDeliveries: [{ childThreadId: child, parentThreadId: parent, deliveredAt: now }],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "wait-delivered result survived failed parent",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: true,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });
    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("interrupted", parentTurn, afterWaitDelivery),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready", parentTurn));

    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start" && command.threadId === parent,
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]?.message.text).toContain("wait-delivered result survived failed parent");
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: restart reload prunes a wait-delivered promoted wake on parent idle", async () => {
    const child = ThreadId.make("promote-wait-delivered-restart-child");
    const parent = ThreadId.make("promote-wait-delivered-restart-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-delivered-restart");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent"), afterWaitDelivery),
          session: makeSession(parent, "ready"),
        }),
      ],
      seedWaitDeliveries: [{ childThreadId: child, parentThreadId: parent, deliveredAt: now }],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "already delivered by wait",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: true,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: restart wait-delivered drain bounds a stalled parent shell read", async () => {
    const child = ThreadId.make("promote-wait-delivered-slow-drain-child");
    const parent = ThreadId.make("promote-wait-delivered-slow-drain-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-delivered-slow-drain");
    const harness = await createHarness({
      slowThreadShellReadCounts: [{ threadId: parent, count: 1 }],
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent"), afterWaitDelivery),
          session: makeSession(parent, "ready"),
        }),
      ],
      seedWaitDeliveries: [{ childThreadId: child, parentThreadId: parent, deliveredAt: now }],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "wait-delivered row waits for bounded restart drain retry",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: true,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.id).toBe(dispatchId);
    expect(await runtimeHasPending(harness, parent)).toBe(true);
    await Effect.runPromise(Effect.sleep("2600 millis"));
    expect(await harness.listPendingDispatches()).toEqual([]);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
  });

  it("R-A: restart drain subscribes before dispatching a nested parent wake", async () => {
    const child = ThreadId.make("promote-wait-restart-hot-child");
    const parent = ThreadId.make("promote-wait-restart-hot-parent");
    const grandparent = ThreadId.make("promote-wait-restart-hot-grandparent");
    const parentTurn = TurnId.make("turn-parent");
    const parentRecoveryTurn = TurnId.make("turn-parent-recovery");
    const dispatchId = PendingDispatchId.make("pd-wait-delivered-hot-subscribe");
    const parentRunning = makeThreadState({
      threadId: parent,
      parentThreadId: grandparent,
      latestTurn: makeLatestTurn("running", parentTurn),
      session: makeSession(parent, "running", parentTurn),
    });
    const parentFailedForDrain = makeThreadState({
      threadId: parent,
      parentThreadId: grandparent,
      latestTurn: makeLatestTurn("error", parentTurn, afterWaitDelivery),
      session: makeSession(parent, "ready"),
    });
    const parentCompletedAfterDrain = makeThreadState({
      threadId: parent,
      parentThreadId: grandparent,
      latestTurn: makeLatestTurn("completed", parentRecoveryTurn, afterWaitDelivery),
      session: makeSession(parent, "ready"),
      assistantText: "nested parent observed drain",
    });
    const harness = await createHarness({
      dropEventsBeforeStreamSubscription: true,
      threads: [
        parentRunning,
        makeThreadState({
          threadId: grandparent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-grandparent")),
          session: makeSession(grandparent, "ready"),
        }),
      ],
      shellReadSequences: [
        {
          threadId: parent,
          states: [parentRunning, parentFailedForDrain, parentCompletedAfterDrain],
        },
      ],
      seedChildRows: [{ threadId: parent, parentThreadId: grandparent }],
      seedWaitDeliveries: [{ childThreadId: child, parentThreadId: parent, deliveredAt: now }],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "wait-delivered child from restart",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: true,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
      onTurnStartDispatch: (command, enqueue, setThread) => {
        if (command.threadId !== parent) return;
        setThread(parentCompletedAfterDrain);
        enqueue(turnStartRequestedEvent(parent));
        enqueue(sessionSetEvent(parent, "running", parentRecoveryTurn));
        enqueue(turnDiffEvent(parent, "ready", parentRecoveryTurn));
      },
    });
    for (let i = 0; i < 50; i += 1) {
      await Effect.runPromise(Effect.yieldNow);
    }
    await Effect.runPromise(harness.coordinator.drain);

    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start",
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(turnStarts.map((command) => command.threadId)).toEqual([parent, grandparent]);
    expect(turnStarts[1]?.message.text).toContain("nested parent observed drain");
  });

  it("R-A: same-millisecond completed parent prunes only the recorded wait-calling turn", async () => {
    const child = ThreadId.make("promote-wait-same-ms-recorded-child");
    const parent = ThreadId.make("promote-wait-same-ms-recorded-parent");
    const parentTurn = TurnId.make("turn-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-delivered-same-ms-recorded");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", parentTurn, now),
          session: makeSession(parent, "ready"),
        }),
      ],
      seedWaitDeliveries: [
        {
          childThreadId: child,
          parentThreadId: parent,
          deliveredAt: now,
          parentTurnIdAtDelivery: parentTurn,
        },
      ],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "same millisecond recorded turn",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: true,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: restart retains a wait-delivered fallback on same-millisecond parent completion", async () => {
    const child = ThreadId.make("promote-wait-same-ms-restart-child");
    const parent = ThreadId.make("promote-wait-same-ms-restart-parent");
    const parentTurn = TurnId.make("turn-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-delivered-same-ms");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", parentTurn, now),
          session: makeSession(parent, "ready"),
        }),
      ],
      seedWaitDeliveries: [{ childThreadId: child, parentThreadId: parent, deliveredAt: now }],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "same millisecond wait delivery",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: true,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(1);
    expect(await runtimeHasPending(harness, parent)).toBe(true);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent-2"), afterWaitDelivery),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready", TurnId.make("turn-parent-2")));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: restart repairs a wait-delivered row missing its delivery tombstone", async () => {
    const child = ThreadId.make("promote-wait-delivered-missing-tombstone-child");
    const parent = ThreadId.make("promote-wait-delivered-missing-tombstone-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-delivered-missing-tombstone");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent"), afterWaitDelivery),
          session: makeSession(parent, "ready"),
        }),
      ],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "delivered row survived without tombstone",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: true,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await harness.listWaitDeliveries()).toEqual([
      { childThreadId: String(child), parentThreadId: String(parent) },
    ]);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: restart reconciliation does not duplicate a wait-delivered wake row", async () => {
    const child = ThreadId.make("promote-wait-delivered-restart-projected-child");
    const parent = ThreadId.make("promote-wait-delivered-restart-projected-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-delivered-restart-projected");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "already delivered before restart",
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent"), afterWaitDelivery),
          session: makeSession(parent, "ready"),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      seedWaitDeliveries: [{ childThreadId: child, parentThreadId: parent, deliveredAt: now }],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "already delivered before restart",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: true,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: restart drains crash-recovered wait-cancellable wake rows for an idle parent", async () => {
    const child = ThreadId.make("promote-wait-cancellable-restart-child");
    const parent = ThreadId.make("promote-wait-cancellable-restart-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-cancellable-restart");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent")),
          session: makeSession(parent, "ready"),
        }),
      ],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "wait result survived waiter crash",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start" && command.threadId === parent,
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]?.message.text).toContain("wait result survived waiter crash");
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: restart defers restored-child wait-cancellable wake rows before idle drain", async () => {
    const child = ThreadId.make("promote-wait-cancellable-restart-known-child");
    const parent = ThreadId.make("promote-wait-cancellable-restart-known-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-cancellable-restart-known");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "known child wait result survived waiter crash",
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent")),
          session: makeSession(parent, "ready"),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "known child wait result survived waiter crash",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(1);
    await Effect.runPromise(Effect.sleep("2600 millis"));
    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start" && command.threadId === parent,
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]?.message.text).toContain("known child wait result survived waiter crash");
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: restart retries idle-only wait-cancellable drains after a parent shell timeout", async () => {
    const child = ThreadId.make("promote-wait-cancellable-restart-retry-child");
    const parent = ThreadId.make("promote-wait-cancellable-restart-retry-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-cancellable-restart-retry");
    const harness = await createHarness({
      slowThreadShellReadCounts: [{ threadId: parent, count: 1 }],
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent")),
          session: makeSession(parent, "ready"),
        }),
      ],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "wait result survived first restart timeout",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(1);
    await Effect.runPromise(Effect.sleep("2600 millis"));
    const turnStarts = harness.dispatched.filter(
      (command) => command.type === "thread.turn.start" && command.threadId === parent,
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]?.message.text).toContain("wait result survived first restart timeout");
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: restart keeps crash-recovered wait-cancellable wake rows queued for an active parent", async () => {
    const child = ThreadId.make("promote-wait-cancellable-restart-active-child");
    const parent = ThreadId.make("promote-wait-cancellable-restart-active-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-cancellable-restart-active");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", TurnId.make("turn-parent")),
          session: makeSession(parent, "running", TurnId.make("turn-parent")),
        }),
      ],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "wait result must wait for parent idle",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });

    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    const pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]).toMatchObject({
      deliveredByWait: false,
      waitCancellable: true,
    });
    expect(await runtimeHasPending(harness, parent)).toBe(true);
  });

  it("R-A: wait-delivery tombstone suppresses restart replay after fallback prune", async () => {
    const child = ThreadId.make("promote-wait-tombstone-child");
    const parent = ThreadId.make("promote-wait-tombstone-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "already committed through wait",
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(parent, "ready"),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      seedWaitDeliveries: [{ childThreadId: child, parentThreadId: parent }],
    });
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await harness.listWaitDeliveries()).toEqual([
      { childThreadId: String(child), parentThreadId: String(parent) },
    ]);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: stale promoted marker with wait-delivery tombstone is pruned on restart", async () => {
    const child = ThreadId.make("promote-wait-stale-marker-child");
    const parent = ThreadId.make("promote-wait-stale-marker-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "already delivered through wait before crash",
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(parent, "ready"),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      seedWaitDeliveries: [{ childThreadId: child, parentThreadId: parent }],
      seedPromotedChildren: [{ childThreadId: child, parentThreadId: parent }],
    });
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await harness.listPromotedChildren()).toEqual([]);
    expect(await harness.listWaitDeliveries()).toEqual([
      { childThreadId: String(child), parentThreadId: String(parent) },
    ]);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: restart-restored promoted wake can be marked delivered by a later wait", async () => {
    const child = ThreadId.make("promote-wait-restart-later-wait-child");
    const parent = ThreadId.make("promote-wait-restart-later-wait-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-restart-later-wait");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "completed before restart wait",
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", TurnId.make("turn-parent")),
          session: makeSession(parent, "running", TurnId.make("turn-parent")),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "completed before restart wait",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    let pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.deliveredByWait).toBe(false);

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("completed before restart wait");
    await Effect.runPromise(harness.coordinator.markWaitDelivered([result]));
    pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.deliveredByWait).toBe(true);
    expect(pendingRows[0]?.waitCancellable).toBe(true);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent"), afterWaitDelivery),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready", TurnId.make("turn-parent")));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: restart-restored promoted child without a wake row keeps later wait cancellable", async () => {
    const child = ThreadId.make("promote-wait-restart-no-row-child");
    const parent = ThreadId.make("promote-wait-restart-no-row-parent");
    const parentTurn = TurnId.make("turn-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running", TurnId.make("turn-child")),
          session: makeSession(child, "running", TurnId.make("turn-child")),
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("running", parentTurn),
          session: makeSession(parent, "running", parentTurn),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      seedPromotedChildren: [{ childThreadId: child, parentThreadId: parent }],
    });
    const children = await runtimeListChildren(harness, parent);
    expect(children).toHaveLength(1);
    expect(children[0]?.detached).toBe(false);

    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed", TurnId.make("turn-child")),
        session: makeSession(child, "ready"),
        assistantText: "completed after promoted restart",
      }),
    );
    await harness.feed(turnDiffEvent(child, "ready", TurnId.make("turn-child")));

    let pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]).toMatchObject({
      sourceChildId: child,
      targetThreadId: parent,
      text: "completed after promoted restart",
      deliveredByWait: false,
      waitCancellable: true,
    });

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("completed after promoted restart");
    await Effect.runPromise(harness.coordinator.markWaitDelivered([result]));

    pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.deliveredByWait).toBe(true);
    expect(pendingRows[0]?.waitCancellable).toBe(true);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("completed", parentTurn, afterWaitDelivery),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready", parentTurn));

    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-A: later wait keeps a restored promoted wake through a stale committed parent snapshot", async () => {
    const child = ThreadId.make("promote-wait-restart-committed-parent-child");
    const parent = ThreadId.make("promote-wait-restart-committed-parent");
    const dispatchId = PendingDispatchId.make("pd-wait-restart-committed-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("completed"),
          assistantText: "completed before committed parent wait",
        }),
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent")),
          session: makeSession(parent, "ready"),
        }),
      ],
      seedChildRows: [{ threadId: child, parentThreadId: parent }],
      seedPendingDispatches: [
        {
          id: dispatchId,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: child,
          text: "completed before committed parent wait",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: true,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
    });
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    const result = await runtimeRun(harness, child);
    expect(result.status).toBe("completed");
    expect(result.finalAssistantText).toBe("completed before committed parent wait");
    await Effect.runPromise(harness.coordinator.markWaitDelivered([result]));
    expect(
      harness.dispatched.filter((command) => command.type === "thread.turn.start"),
    ).toHaveLength(0);
    let pendingRows = await harness.listPendingDispatches();
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]?.deliveredByWait).toBe(true);
    expect(await runtimeHasPending(harness, parent)).toBe(true);

    harness.setThread(
      makeThreadState({
        threadId: parent,
        latestTurn: makeLatestTurn("completed", TurnId.make("turn-parent-2"), afterWaitDelivery),
        session: makeSession(parent, "ready"),
      }),
    );
    await harness.feed(turnDiffEvent(parent, "ready", TurnId.make("turn-parent-2")));
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("treats missing turn diffs as completed child turns when projection is completed", async () => {
    const child = ThreadId.make("missing-diff-child");
    const parent = ThreadId.make("missing-diff-parent");
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: child,
          parentThreadId: parent,
          latestTurn: makeLatestTurn("running"),
          session: makeSession(child, "running", TurnId.make("turn-1")),
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
    harness.setThread(
      makeThreadState({
        threadId: child,
        parentThreadId: parent,
        latestTurn: makeLatestTurn("completed"),
        assistantText: "missing diff result",
      }),
    );

    await harness.feed(turnDiffEvent(child, "missing"));

    const turnStarts = harness.dispatched.filter((command) => command.type === "thread.turn.start");
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]?.message.role).toBe("system");
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
    expect(harness.dispatched.find((c) => c.type === "thread.turn.start")?.message.role).toBe(
      "system",
    );
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
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
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

  it("R-B exactly-once: a crash between dispatch and delete neither re-fires a landed wake nor loses an un-landed one", async () => {
    // Simulated crash window: the durable rows survived (delete never ran), and
    // for one of them the wake turn HAD committed before the crash (its
    // commandId already has an accepted receipt). On recovery + parent idle:
    //   - the LANDED wake must NOT re-fire (no duplicate "[sub-agent ...]" turn),
    //     but its orphaned row must still be cleaned up; and
    //   - the UN-LANDED wake MUST be delivered (no loss).
    const landedParent = ThreadId.make("xo-landed-parent");
    const landedChild = ThreadId.make("xo-landed-child");
    const lostParent = ThreadId.make("xo-lost-parent");
    const lostChild = ThreadId.make("xo-lost-child");
    const landedId = PendingDispatchId.make("pd-xo-landed");
    const lostId = PendingDispatchId.make("pd-xo-lost");
    // The coordinator derives the wake commandId deterministically from the row
    // id: batchCommandIdFor("subagent-wake", [id]) === `server:subagent-wake:${id}`.
    const landedCommandId = `server:subagent-wake:${landedId}`;
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: landedParent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(landedParent, "ready"),
        }),
        makeThreadState({
          threadId: lostParent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(lostParent, "ready"),
        }),
      ],
      seedPendingDispatches: [
        {
          id: landedId,
          kind: "parent_injection",
          targetThreadId: landedParent,
          sourceChildId: landedChild,
          text: "landed result",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
        {
          id: lostId,
          kind: "parent_injection",
          targetThreadId: lostParent,
          sourceChildId: lostChild,
          text: "un-landed result",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
      // The landed wake's turn committed before the crash.
      seedAcceptedCommandIds: [landedCommandId],
    });

    // Both rows reloaded as pending.
    expect(await runtimeHasPending(harness, landedParent)).toBe(true);
    expect(await runtimeHasPending(harness, lostParent)).toBe(true);

    // Recovery: each parent goes idle and drains.
    await harness.feed(turnDiffEvent(landedParent, "ready"));
    await harness.feed(turnDiffEvent(lostParent, "ready"));

    // No duplicate: the landed wake re-fired under the same commandId, which the
    // engine deduped, so NO new turn landed for the landed parent.
    const landedStarts = harness.dispatched.filter(
      (c) => c.type === "thread.turn.start" && c.threadId === landedParent,
    );
    expect(landedStarts).toHaveLength(0);
    // No loss: the un-landed wake was delivered as a fresh turn.
    const lostStarts = harness.dispatched.filter(
      (c) => c.type === "thread.turn.start" && c.threadId === lostParent,
    );
    expect(lostStarts).toHaveLength(1);

    // Both durable rows are cleaned up (the landed row even though it re-fired a
    // no-op) and neither parent has pending work left to re-load on next restart.
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, landedParent)).toBe(false);
    expect(await runtimeHasPending(harness, lostParent)).toBe(false);
  });

  it("R-B exactly-once: a keyed external wake retry is not re-delivered after its row was deleted", async () => {
    const parent = ThreadId.make("xo-keyed-parent");
    const child = ThreadId.make("xo-keyed-child");
    const dedupeKey = "remote-subagent-wake:xo-keyed-parent:peer-env:xo-keyed-child";
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(parent, "ready"),
        }),
      ],
    });

    await Effect.runPromise(
      harness.coordinator.enqueueParentInjection({
        parentThreadId: parent,
        childThreadId: child,
        status: "completed",
        finalAssistantText: "first remote result",
        error: null,
        dedupeKey,
      }),
    );

    const firstStarts = harness.dispatched.filter(
      (c) => c.type === "thread.turn.start" && c.threadId === parent,
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(firstStarts).toHaveLength(1);
    expect(firstStarts[0]?.commandId).toBe(`server:subagent-wake:${dedupeKey}`);
    expect(await harness.listPendingDispatches()).toHaveLength(0);

    await Effect.runPromise(
      harness.coordinator.enqueueParentInjection({
        parentThreadId: parent,
        childThreadId: child,
        status: "completed",
        finalAssistantText: "duplicate remote result",
        error: null,
        dedupeKey,
      }),
    );

    const startsAfterRetry = harness.dispatched.filter(
      (c) => c.type === "thread.turn.start" && c.threadId === parent,
    );
    expect(startsAfterRetry).toHaveLength(1);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
  });

  it("R-B exactly-once: a landed-but-undeleted batch is NOT re-delivered when a new row re-batches it", async () => {
    // The hard case the deterministic-batch-id alone could not cover: rows [X,Y]
    // were claimed+dispatched as ONE consolidated turn under a fixed commandId and
    // the turn LANDED, but the delete never ran (crash). Before restart a NEW
    // child Z completes for the same parent (durable row Z, unclaimed). On restart
    // + parent idle, the batch composition changes to {X,Y,Z}. A naive fresh batch
    // id would defeat engine dedup and re-deliver X,Y. The claim makes X,Y re-fire
    // under their ORIGINAL id (deduped no-op) while only Z is delivered fresh.
    const parent = ThreadId.make("xo-rebatch-parent");
    const childX = ThreadId.make("xo-rebatch-x");
    const childY = ThreadId.make("xo-rebatch-y");
    const childZ = ThreadId.make("xo-rebatch-z");
    const idX = PendingDispatchId.make("pd-xo-x");
    const idY = PendingDispatchId.make("pd-xo-y");
    const idZ = PendingDispatchId.make("pd-xo-z");
    // The id [X,Y] were dispatched under before the crash (batchCommandIdFor sorts
    // the ids), claimed durably onto both rows.
    const landedBatchCommandId = `server:subagent-wake:${[idX, idY].sort().join(",")}`;
    const harness = await createHarness({
      threads: [
        makeThreadState({
          threadId: parent,
          latestTurn: makeLatestTurn("completed"),
          session: makeSession(parent, "ready"),
        }),
      ],
      seedPendingDispatches: [
        {
          id: idX,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: childX,
          text: "x result",
          error: null,
          status: "completed",
          // Claimed under the landed batch id before the crash.
          commandId: landedBatchCommandId,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
        {
          id: idY,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: childY,
          text: "y result",
          error: null,
          status: "completed",
          commandId: landedBatchCommandId,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
        {
          // New child completion that arrived after the crash, before restart:
          // unclaimed, free to consolidate under a fresh id.
          id: idZ,
          kind: "parent_injection",
          targetThreadId: parent,
          sourceChildId: childZ,
          text: "z result",
          error: null,
          status: "completed",
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now as unknown as PendingDispatch["createdAt"],
        },
      ],
      // The [X,Y] turn committed before the crash.
      seedAcceptedCommandIds: [landedBatchCommandId],
    });

    // All three rows reloaded as pending for the parent.
    expect(await runtimeHasPending(harness, parent)).toBe(true);

    // Parent goes idle and drains.
    await harness.feed(turnDiffEvent(parent, "ready"));

    // Exactly ONE new turn lands: the X,Y re-fire dedups (no duplicate of x/y),
    // only Z is delivered fresh. The bug would have produced a turn containing
    // x/y/z together (X,Y duplicated) under a new id.
    const starts = harness.dispatched.filter(
      (c) => c.type === "thread.turn.start" && c.threadId === parent,
    ) as Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>;
    expect(starts).toHaveLength(1);
    const text = starts[0]?.message.text ?? "";
    expect(text).toContain("z result");
    expect(text).not.toContain("x result");
    expect(text).not.toContain("y result");

    // Every durable row is cleaned up; nothing re-loads on a further restart.
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(await runtimeHasPending(harness, parent)).toBe(false);
    await harness.feed(turnDiffEvent(parent, "ready"));
    expect(
      harness.dispatched.filter((c) => c.type === "thread.turn.start" && c.threadId === parent),
    ).toHaveLength(1);
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
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
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
      commandId: null,
      deliveredByWait: false,
      waitCancellable: false,
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
  harness: {
    readonly coordinator: import("../Services/ChildThreadCoordinator.ts").ChildThreadCoordinatorShape;
  },
  child: ThreadId,
) {
  const slice = await runtimeWaitSlice(harness, [child], FAR_FUTURE_MS);
  return slice.results[0]!;
}

async function runtimeWaitSlice(
  harness: {
    readonly coordinator: import("../Services/ChildThreadCoordinator.ts").ChildThreadCoordinatorShape;
  },
  childThreadIds: ReadonlyArray<ThreadId>,
  budgetDeadlineMs: number,
) {
  return Effect.runPromise(
    harness.coordinator.waitSlice({ childThreadIds, mode: "all", budgetDeadlineMs }),
  );
}

async function runtimeHasPending(
  harness: {
    readonly coordinator: import("../Services/ChildThreadCoordinator.ts").ChildThreadCoordinatorShape;
  },
  parent: ThreadId,
) {
  return Effect.runPromise(harness.coordinator.hasPendingInjections(parent));
}

async function runtimeListChildren(
  harness: {
    readonly coordinator: import("../Services/ChildThreadCoordinator.ts").ChildThreadCoordinatorShape;
  },
  parent: ThreadId,
) {
  return Effect.runPromise(harness.coordinator.listChildren(parent));
}
