import {
  EnvironmentId,
  IsoDateTime,
  NonNegativeInt,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type ModelSelection,
  type OrchestrationThread,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import { McpSchema, McpServer } from "effect/unstable/ai";
import { describe, expect, it } from "@effect/vitest";

import {
  ChildThreadCoordinator,
  type ChildThreadCoordinatorShape,
  type WaitSliceResult,
} from "../../../orchestration/Services/ChildThreadCoordinator.ts";
import { ActiveChildThreadCoordinatorLive } from "../../../orchestration/Layers/ChildThreadCoordinator.ts";
import {
  ActiveBootstrapTurnStartDispatcherLive,
  BootstrapTurnStartDispatcher,
} from "../../../orchestration/Services/BootstrapTurnStartDispatcher.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import {
  PendingDispatchRepository,
  type PendingDispatch,
} from "../../../persistence/Services/PendingDispatches.ts";
import {
  ScheduledTaskRepository,
  ScheduledTaskId,
  type ScheduledTask,
} from "../../../persistence/Services/ScheduledTasks.ts";
import { ProviderInstanceRegistry } from "../../../provider/Services/ProviderInstanceRegistry.ts";
import { SubagentToolkitRegistrationLive } from "../../McpHttpServer.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { SubagentRuntimeLive } from "./handlers.ts";

const environmentId = EnvironmentId.make("environment-subagent-test");
const projectId = ProjectId.make("project-subagent-test");
const parentThreadId = ThreadId.make("thread-subagent-parent");
const childThreadId = ThreadId.make("thread-subagent-child");
const codexModel: ModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5-codex",
};

const invocation = {
  environmentId,
  threadId: parentThreadId,
  providerSessionId: "provider-session-subagent-test",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(["thread-management"] as const),
  issuedAt: 1,
  expiresAt: Number.MAX_SAFE_INTEGER,
};

const client = McpSchema.McpServerClient.of({
  clientId: 1,
  initializePayload: {
    protocolVersion: "2025-03-26",
    capabilities: {},
    clientInfo: { name: "subagent-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

const unsupported = () => Effect.die(new Error("Unsupported call in subagent test")) as never;

let childDetailTurnState: "completed" | "running" | "error" | "interrupted" = "completed";
let childDetailLatestTurnId = "turn-1" as never;
let childDetailLatestTurnRequestedAt = "2026-06-17T10:00:00.000Z";
let childDetailLatestTurnCompletedAt = "2026-06-17T10:01:00.000Z";
let childDetailMessages: OrchestrationThread["messages"] | null = null;
let childDetailSession: OrchestrationThread["session"] | null = null;
let childDetailCheckpoints: OrchestrationThread["checkpoints"] | null = null;

const makeChildDetail = (): OrchestrationThread => ({
  id: childThreadId,
  projectId,
  title: "Child",
  modelSelection: codexModel,
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  latestTurn: {
    turnId: childDetailLatestTurnId,
    state: childDetailTurnState,
    requestedAt: childDetailLatestTurnRequestedAt,
    startedAt: childDetailLatestTurnRequestedAt,
    completedAt: childDetailTurnState === "running" ? null : childDetailLatestTurnCompletedAt,
    assistantMessageId: null,
  },
  createdAt: "2026-06-17T09:00:00.000Z",
  updatedAt: "2026-06-17T10:01:00.000Z",
  archivedAt: null,
  deletedAt: null,
  messages: childDetailMessages ?? [
    {
      id: "msg-1" as never,
      role: "assistant",
      text: "child done",
      turnId: "turn-1" as never,
      streaming: false,
      createdAt: "2026-06-17T10:01:00.000Z",
      updatedAt: "2026-06-17T10:01:00.000Z",
    },
  ],
  proposedPlans: [],
  activities: [],
  checkpoints: childDetailCheckpoints ?? [],
  session: childDetailSession,
});

// A child shell with a configurable turn state. Idle (latestTurn != running)
// drives the R-C steer "now" path; "running" drives the mid-turn branch (defer
// for codex/unknown, dispatch for a proven mid-turn driver).
const makeChildShell = (turnState: "completed" | "running" = "completed") => ({
  id: childThreadId,
  projectId,
  title: "Child",
  modelSelection: codexModel,
  runtimeMode: "full-access" as const,
  interactionMode: "default" as const,
  branch: null,
  worktreePath: null,
  latestTurn: {
    turnId: "turn-1" as never,
    state: turnState,
    requestedAt: "2026-06-17T10:00:00.000Z",
    startedAt: "2026-06-17T10:00:00.000Z",
    completedAt: turnState === "running" ? null : "2026-06-17T10:01:00.000Z",
    assistantMessageId: null,
  },
  createdAt: "2026-06-17T09:00:00.000Z",
  updatedAt: "2026-06-17T10:01:00.000Z",
  archivedAt: null,
  session: null,
  latestUserMessageAt: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
  parentThreadId,
});

// Test seams: mutable holders the per-test layers reconfigure before driving a
// tool call. Each test sets the slice result / records the side effects it
// asserts on.
let waitSliceResult: WaitSliceResult | null = null;
let childDetailDelayMs = 0;
let childDetailUnavailable = false;
// R-C seams: the child's turn state and its provider driver kind drive the
// idle/mid-turn + defer/dispatch decision. Defaults: idle child, unknown driver.
let childTurnState: "completed" | "running" = "completed";
let childDriverKind: string | undefined = undefined;
const promotedCalls: Array<ReadonlyArray<ThreadId>> = [];
const dispatchedTurns: Array<ThreadId> = [];
const insertedDispatches: Array<PendingDispatch> = [];
// Fix 1 seams: the enabled provider instances (with their live model lists) the
// schedule handlers resolve a plain `model` against, and the tasks they persist.
let modelInstances: ReadonlyArray<unknown> = [];
const insertedTasks: Array<{ readonly modelSelection: ModelSelection | null }> = [];
// Existing scheduled tasks visible to t3_schedule_update (via listAll), and the
// updated rows it writes back — so a test can assert a model re-route / un-pin.
let existingTasks: ReadonlyArray<ScheduledTask> = [];
const updatedTasks: Array<ScheduledTask> = [];

const scheduledTaskId = ScheduledTaskId.make("sched-fix1");
const makeScheduledTask = (modelSelection: ModelSelection | null): ScheduledTask => ({
  taskId: scheduledTaskId,
  threadId: parentThreadId,
  prompt: "nightly summary",
  scheduleKind: "interval",
  intervalSeconds: 3_600,
  cronExpr: null,
  timezoneName: "UTC",
  enabled: NonNegativeInt.make(1),
  busyPolicy: "skip",
  nextRunAt: IsoDateTime.make("2026-06-17T10:00:00.000Z"),
  lastRunAt: null,
  lastStatus: null,
  lastError: null,
  skippedCount: NonNegativeInt.make(0),
  retryCount: NonNegativeInt.make(0),
  queuedCount: NonNegativeInt.make(0),
  modelSelection,
  createdAt: IsoDateTime.make("2026-06-17T09:00:00.000Z"),
});

type ModelFixture =
  | string
  | {
      readonly slug: string;
      readonly optionId: string;
      readonly value: string;
    };

// A minimal provider instance exposing just what buildModelSources reads: an id,
// a driver kind, enabled=true, and a snapshot whose models list the given slugs.
const makeModelInstance = (
  instanceId: string,
  driverKind: string,
  models: ReadonlyArray<ModelFixture>,
) => ({
  instanceId: ProviderInstanceId.make(instanceId),
  driverKind,
  enabled: true,
  snapshot: {
    getSnapshot: Effect.succeed({
      status: "ready",
      models: models.map((entry) =>
        typeof entry === "string"
          ? { slug: entry, capabilities: null }
          : {
              slug: entry.slug,
              capabilities: {
                optionDescriptors: [
                  {
                    id: entry.optionId,
                    label: "Reasoning",
                    type: "select" as const,
                    options: ["low", "medium", "high", "xhigh", "max"].map((value) => ({
                      id: value,
                      label: value,
                      ...(value === entry.value ? { isDefault: true } : {}),
                    })),
                    currentValue: entry.value,
                  },
                ],
              },
            },
      ),
    }),
  },
});

// Source thread shell for the calling (parent) thread, needed by t3_schedule_*
// to validate the thread exists and to prefer its instance on model ties.
const makeParentShell = () => ({
  ...makeChildShell("completed"),
  id: parentThreadId,
  parentThreadId: null,
});

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
    Effect.succeed(
      threadId === childThreadId
        ? Option.some(makeChildShell(childTurnState))
        : threadId === parentThreadId
          ? Option.some(makeParentShell())
          : Option.none(),
    ),
  getThreadDetailById: (threadId) =>
    Effect.suspend(() => {
      const detail =
        threadId === childThreadId && !childDetailUnavailable
          ? Option.some(makeChildDetail())
          : Option.none();
      return childDetailDelayMs > 0
        ? Effect.sleep(`${childDetailDelayMs} millis`).pipe(Effect.as(detail))
        : Effect.succeed(detail);
    }),
  getThreadDetailSnapshot: () => Effect.succeed(Option.none()),
});

const coordinatorLayer = Layer.succeed(ChildThreadCoordinator, {
  register: () => Effect.void,
  waitSlice: () => (waitSliceResult ? Effect.succeed(waitSliceResult) : unsupported()),
  assertParent: () => Effect.void,
  promoteToWake: (ids) => Effect.sync(() => void promotedCalls.push(ids)),
  hasPendingInjections: () => Effect.succeed(false),
  listChildren: (parent) =>
    Effect.succeed(
      parent === parentThreadId
        ? [
            {
              childThreadId,
              parentThreadId,
              detached: true,
              model: codexModel,
              spawnedAtMs: 1,
              depth: 1,
              settled: true,
            },
          ]
        : [],
    ),
  start: () => Effect.void,
  drain: Effect.void,
} satisfies ChildThreadCoordinatorShape);

const engineLayer = Layer.succeed(OrchestrationEngineService, {
  readEvents: () => unsupported(),
  dispatch: () => unsupported(),
  streamDomainEvents: unsupported(),
});

const providerRegistryLayer = Layer.succeed(ProviderInstanceRegistry, {
  getInstance: () =>
    Effect.succeed(childDriverKind === undefined ? undefined : { driverKind: childDriverKind }),
  listInstances: Effect.suspend(() => Effect.succeed(modelInstances)),
  listUnavailable: Effect.succeed([]),
  changes: unsupported(),
} as never);

const scheduledTasksLayer = Layer.succeed(ScheduledTaskRepository, {
  listDue: () => Effect.succeed([]),
  insert: (task) => Effect.sync(() => void insertedTasks.push(task)),
  update: (task) => Effect.sync(() => void updatedTasks.push(task)),
  delete: () => Effect.void,
  markRun: () => Effect.void,
  listAll: () => Effect.suspend(() => Effect.succeed(existingTasks)),
  listByThread: () => Effect.succeed([]),
  revisionChanges: Stream.empty,
});

const pendingDispatchesLayer = Layer.succeed(PendingDispatchRepository, {
  insert: (row) => Effect.sync(() => void insertedDispatches.push(row)),
  listByTarget: () => Effect.succeed([]),
  listAll: () => Effect.succeed([]),
  claim: () => Effect.void,
  deleteByIds: () => Effect.void,
});

// Recording bootstrap dispatcher: the R-C "now"/"queued" steer path goes through
// dispatchActive -> this dispatcher, so the test can assert a turn was started.
const dispatcherLayer = Layer.succeed(BootstrapTurnStartDispatcher, {
  dispatch: (command) =>
    Effect.sync(() => {
      dispatchedTurns.push(command.threadId);
      return { sequence: dispatchedTurns.length };
    }),
});

const RuntimeActivationLive = Layer.mergeAll(
  SubagentRuntimeLive,
  ActiveChildThreadCoordinatorLive,
  ActiveBootstrapTurnStartDispatcherLive,
).pipe(
  Layer.provideMerge(coordinatorLayer),
  Layer.provideMerge(dispatcherLayer),
  Layer.provideMerge(projectionLayer),
  Layer.provideMerge(engineLayer),
  Layer.provideMerge(providerRegistryLayer),
  Layer.provideMerge(scheduledTasksLayer),
  Layer.provideMerge(pendingDispatchesLayer),
  Layer.provideMerge(NodeServices.layer),
);

const TestLayer = SubagentToolkitRegistrationLive.pipe(
  Layer.provideMerge(McpServer.McpServer.layer),
  Layer.provideMerge(RuntimeActivationLive),
);

describe("SubagentToolkit", () => {
  it.effect("gates on thread-management and lists a parent's sub-agents", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;

        const listTool = server.tools.find(({ tool }) => tool.name === "t3_list_subagents");
        expect(listTool?.tool.annotations?.readOnlyHint).toBe(true);

        const result = yield* server
          .callTool({ name: "t3_list_subagents", arguments: {} })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          parentThreadId,
          children: [
            {
              childThreadId,
              parentThreadId,
              detached: true,
              status: "completed",
              turnCount: 1,
            },
          ],
        });
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect(
    "checks a current completed stopped child as completed with a checkpointless turn count",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const server = yield* McpServer.McpServer;
          childDetailTurnState = "completed";
          childDetailSession = {
            threadId: childThreadId,
            status: "stopped",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: "2026-06-17T10:02:00.000Z",
          };

          const result = yield* server
            .callTool({
              name: "t3_check_subagent",
              arguments: { childThreadId },
            })
            .pipe(
              Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
              Effect.provideService(McpSchema.McpServerClient, client),
            );

          expect(result.isError).toBe(false);
          expect(result.structuredContent).toMatchObject({
            threadId: childThreadId,
            status: "completed",
            turnCount: 1,
            latestAssistantText: "child done",
          });
        }),
      ).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            childDetailSession = null;
          }),
        ),
        Effect.provide(TestLayer),
      ),
  );

  it.effect("reports a stopped child with only a stale completed latest turn as failed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        childDetailTurnState = "completed";
        childDetailSession = {
          threadId: childThreadId,
          status: "stopped",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-06-17T10:02:00.000Z",
        };
        childDetailMessages = [
          {
            id: "msg-1" as never,
            role: "assistant",
            text: "child done",
            turnId: "turn-1" as never,
            streaming: false,
            createdAt: "2026-06-17T10:01:00.000Z",
            updatedAt: "2026-06-17T10:01:00.000Z",
          },
          {
            id: "msg-2" as never,
            role: "user",
            text: "new attempted turn",
            turnId: null,
            streaming: false,
            createdAt: "2026-06-17T10:02:00.000Z",
            updatedAt: "2026-06-17T10:02:00.000Z",
          },
        ];

        const result = yield* server
          .callTool({
            name: "t3_check_subagent",
            arguments: { childThreadId },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          threadId: childThreadId,
          status: "failed",
          turnCount: 1,
        });
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailMessages = null;
          childDetailSession = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("reports a stopped interrupted child as failed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        childDetailTurnState = "interrupted";
        childDetailSession = {
          threadId: childThreadId,
          status: "stopped",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-06-17T10:02:00.000Z",
        };

        const checkResult = yield* server
          .callTool({
            name: "t3_check_subagent",
            arguments: { childThreadId },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );
        const listResult = yield* server
          .callTool({ name: "t3_list_subagents", arguments: {} })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(checkResult.isError).toBe(false);
        expect(checkResult.structuredContent).toMatchObject({
          threadId: childThreadId,
          status: "failed",
          turnCount: 1,
        });
        expect(listResult.isError).toBe(false);
        expect(listResult.structuredContent).toMatchObject({
          children: [{ childThreadId, status: "failed", turnCount: 1 }],
        });
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailTurnState = "completed";
          childDetailSession = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("does not count the active running turn from checkpointless messages", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        childDetailTurnState = "running";

        const result = yield* server
          .callTool({
            name: "t3_check_subagent",
            arguments: { childThreadId },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          threadId: childThreadId,
          status: "running",
          turnCount: 0,
        });
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailTurnState = "completed";
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("increments turn count for a checkpointless latest turn after checkpoints", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        childDetailCheckpoints = [
          {
            turnId: "turn-0" as never,
            checkpointTurnCount: NonNegativeInt.make(1),
            checkpointRef: "checkpoint-turn-0" as never,
            status: "ready",
            files: [],
            assistantMessageId: null,
            completedAt: "2026-06-17T09:59:00.000Z",
          },
        ];

        const result = yield* server
          .callTool({
            name: "t3_check_subagent",
            arguments: { childThreadId },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          threadId: childThreadId,
          status: "completed",
          turnCount: 2,
        });
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailCheckpoints = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("does not recount retained messages from capped checkpoint history", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        childDetailLatestTurnId = "turn-600" as never;
        childDetailLatestTurnRequestedAt = "2026-06-17T10:00:00.000Z";
        childDetailLatestTurnCompletedAt = "2026-06-17T10:00:30.000Z";
        childDetailCheckpoints = [
          {
            turnId: "turn-600" as never,
            checkpointTurnCount: NonNegativeInt.make(600),
            checkpointRef: "checkpoint-turn-600" as never,
            status: "ready",
            files: [],
            assistantMessageId: null,
            completedAt: "2026-06-17T10:00:30.000Z",
          },
        ];
        childDetailMessages = [
          {
            id: "msg-old" as never,
            role: "assistant",
            text: "old retained message",
            turnId: "turn-1" as never,
            streaming: false,
            createdAt: "2026-06-17T09:00:00.000Z",
            updatedAt: "2026-06-17T09:00:00.000Z",
          },
          {
            id: "msg-latest" as never,
            role: "assistant",
            text: "latest checkpointed message",
            turnId: "turn-600" as never,
            streaming: false,
            createdAt: "2026-06-17T10:00:30.000Z",
            updatedAt: "2026-06-17T10:00:30.000Z",
          },
        ];

        const result = yield* server
          .callTool({
            name: "t3_check_subagent",
            arguments: { childThreadId },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          threadId: childThreadId,
          status: "completed",
          turnCount: 600,
        });
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailLatestTurnId = "turn-1" as never;
          childDetailLatestTurnRequestedAt = "2026-06-17T10:00:00.000Z";
          childDetailLatestTurnCompletedAt = "2026-06-17T10:01:00.000Z";
          childDetailMessages = null;
          childDetailCheckpoints = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("reports a completed latest turn with an errored session as failed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        childDetailTurnState = "completed";
        childDetailSession = {
          threadId: childThreadId,
          status: "error",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: "provider failed",
          updatedAt: "2026-06-17T10:02:00.000Z",
        };

        const result = yield* server
          .callTool({
            name: "t3_check_subagent",
            arguments: { childThreadId },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          threadId: childThreadId,
          status: "failed",
          turnCount: 1,
        });
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailSession = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("rejects a credential without the thread-management capability", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        const result = yield* server.callTool({ name: "t3_list_subagents", arguments: {} }).pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, {
            ...invocation,
            capabilities: new Set(["preview"] as const),
          }),
          Effect.provideService(McpSchema.McpServerClient, client),
        );
        expect(result.isError).toBe(true);
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("R-A: wait auto-promotes a still-running child once the budget elapses", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        promotedCalls.length = 0;
        childDetailTurnState = "running";
        // The coordinator slice reports the child still pending.
        waitSliceResult = {
          results: [{ childThreadId, status: "pending", finalAssistantText: null, error: null }],
          settledCount: 0,
          timedOutCount: 0,
          pending: true,
          resumeToken: "coordinator-token",
        };

        // A resumeToken whose wait-start marker is well in the past puts the
        // 90s auto-promote deadline before "now" (the test clock starts at 0),
        // so this re-call promotes deterministically without a real 90s wait.
        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: {
              childThreadIds: [childThreadId],
              resumeToken: "-100000:coordinator-token",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          promoted: true,
          pending: false,
          results: [{ childThreadId, status: "running" }],
        });
        const row = (result.structuredContent as { results: ReadonlyArray<{ note?: string }> })
          .results[0];
        expect(row?.note).toContain("NOTIFIED");
        expect(promotedCalls).toEqual([[childThreadId]]);
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("R-A: wait auto-promotes timeout rows when the auto-promote deadline was active", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        promotedCalls.length = 0;
        childDetailTurnState = "running";
        // waitSlice converts pending children to "timeout" when the supplied
        // budgetDeadlineMs has elapsed. If that deadline was the 90s
        // auto-promote cap, the child is still running and must be promoted.
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "timeout",
              finalAssistantText: null,
              error: "wait exceeded budget",
            },
          ],
          settledCount: 0,
          timedOutCount: 1,
          pending: false,
          resumeToken: "coordinator-token",
        };

        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: {
              childThreadIds: [childThreadId],
              resumeToken: "-100000:coordinator-token",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          promoted: true,
          pending: false,
          timedOutCount: 0,
          results: [{ childThreadId, status: "running", error: null }],
        });
        const row = (result.structuredContent as { results: ReadonlyArray<{ note?: string }> })
          .results[0];
        expect(row?.note).toContain("NOTIFIED");
        expect(promotedCalls).toEqual([[childThreadId]]);
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("R-A: wait auto-promotes timeout rows when projection enrichment is unavailable", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        promotedCalls.length = 0;
        childDetailTurnState = "running";
        childDetailUnavailable = true;
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "timeout",
              finalAssistantText: null,
              error: "wait exceeded budget",
            },
          ],
          settledCount: 0,
          timedOutCount: 1,
          pending: false,
          resumeToken: "coordinator-token",
        };

        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: {
              childThreadIds: [childThreadId],
              resumeToken: "-100000:coordinator-token",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          promoted: true,
          pending: false,
          timedOutCount: 0,
          results: [{ childThreadId, status: "running", turnCount: 0, error: null }],
        });
        expect(promotedCalls).toEqual([[childThreadId]]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailUnavailable = false;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect(
    "keeps any-mode waits pending when rows are timeout plus pending and none settled",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const server = yield* McpServer.McpServer;
          const stillPendingChildId = ThreadId.make("thread-subagent-still-pending");
          waitSliceResult = {
            results: [
              {
                childThreadId,
                status: "timeout",
                finalAssistantText: null,
                error: "wait exceeded budget",
              },
              {
                childThreadId: stillPendingChildId,
                status: "pending",
                finalAssistantText: null,
                error: null,
              },
            ],
            settledCount: 0,
            timedOutCount: 1,
            pending: true,
            resumeToken: "coordinator-token",
          };

          const result = yield* server
            .callTool({
              name: "t3_wait_subagent",
              arguments: {
                childThreadIds: [childThreadId, stillPendingChildId],
                mode: "any",
                timeoutSeconds: 1,
              },
            })
            .pipe(
              Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
              Effect.provideService(McpSchema.McpServerClient, client),
            );

          expect(result.isError).toBe(false);
          expect(result.structuredContent).toMatchObject({
            pending: true,
            settledCount: 0,
            timedOutCount: 1,
            results: [
              { childThreadId, status: "timeout" },
              { childThreadId: stillPendingChildId, status: "pending" },
            ],
          });
          expect(result.structuredContent).not.toHaveProperty("promoted");
        }),
      ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("R-A: wait returns projection-terminal children instead of stale promotion", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        promotedCalls.length = 0;
        childDetailTurnState = "completed";
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "timeout",
              finalAssistantText: null,
              error: "wait exceeded budget",
            },
          ],
          settledCount: 0,
          timedOutCount: 1,
          pending: false,
          resumeToken: "coordinator-token",
        };

        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: {
              childThreadIds: [childThreadId],
              resumeToken: "-100000:coordinator-token",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          pending: false,
          settledCount: 1,
          timedOutCount: 0,
          results: [
            {
              childThreadId,
              status: "completed",
              finalAssistantText: "child done",
              error: null,
            },
          ],
        });
        expect(result.structuredContent).not.toHaveProperty("promoted");
        expect(promotedCalls).toEqual([]);
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("R-A: stale completed projection with a newer active turn still auto-promotes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        promotedCalls.length = 0;
        childDetailTurnState = "completed";
        childDetailSession = {
          threadId: childThreadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: "turn-2" as never,
          lastError: null,
          updatedAt: "2026-06-17T10:02:00.000Z",
        };
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "timeout",
              finalAssistantText: null,
              error: "wait exceeded budget",
            },
          ],
          settledCount: 0,
          timedOutCount: 1,
          pending: false,
          resumeToken: "coordinator-token",
        };

        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: {
              childThreadIds: [childThreadId],
              resumeToken: "-100000:coordinator-token",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          promoted: true,
          pending: false,
          timedOutCount: 0,
          results: [{ childThreadId, status: "running", error: null }],
        });
        expect(promotedCalls).toEqual([[childThreadId]]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailSession = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("returns completed projection waits when the completed child session has stopped", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        promotedCalls.length = 0;
        childDetailTurnState = "completed";
        childDetailSession = {
          threadId: childThreadId,
          status: "stopped",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-06-17T10:02:00.000Z",
        };
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "timeout",
              finalAssistantText: null,
              error: "wait exceeded budget",
            },
          ],
          settledCount: 0,
          timedOutCount: 1,
          pending: false,
          resumeToken: "coordinator-token",
        };

        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: {
              childThreadIds: [childThreadId],
              resumeToken: "-100000:coordinator-token",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          pending: false,
          settledCount: 1,
          timedOutCount: 0,
          results: [{ childThreadId, status: "completed", error: null }],
        });
        expect(result.structuredContent).not.toHaveProperty("promoted");
        expect(promotedCalls).toEqual([]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailSession = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("returns failed projection waits when the completed child session has errored", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        promotedCalls.length = 0;
        childDetailTurnState = "completed";
        childDetailSession = {
          threadId: childThreadId,
          status: "error",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: "provider failed",
          updatedAt: "2026-06-17T10:02:00.000Z",
        };
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "timeout",
              finalAssistantText: null,
              error: "wait exceeded budget",
            },
          ],
          settledCount: 0,
          timedOutCount: 1,
          pending: false,
          resumeToken: "coordinator-token",
        };

        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: {
              childThreadIds: [childThreadId],
              resumeToken: "-100000:coordinator-token",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          pending: false,
          settledCount: 1,
          timedOutCount: 0,
          results: [
            {
              childThreadId,
              status: "failed",
              error: "Child thread ended with status failed.",
            },
          ],
        });
        expect(result.structuredContent).not.toHaveProperty("promoted");
        expect(promotedCalls).toEqual([]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailSession = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("does not enrich completed waits with stale prior-turn assistant text", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        childDetailTurnState = "completed";
        childDetailMessages = [
          {
            id: "msg-prior" as never,
            role: "assistant",
            text: "stale prior answer",
            turnId: "turn-prior" as never,
            streaming: false,
            createdAt: "2026-06-17T09:59:00.000Z",
            updatedAt: "2026-06-17T09:59:00.000Z",
          },
        ];
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "timeout",
              finalAssistantText: null,
              error: "wait exceeded budget",
            },
          ],
          settledCount: 0,
          timedOutCount: 1,
          pending: false,
          resumeToken: "coordinator-token",
        };

        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: {
              childThreadIds: [childThreadId],
              resumeToken: "-100000:coordinator-token",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          pending: false,
          settledCount: 1,
          timedOutCount: 0,
          results: [
            {
              childThreadId,
              status: "completed",
              finalAssistantText: null,
              error: null,
            },
          ],
        });
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailMessages = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("does not enrich failed waits with stale prior-turn assistant text", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        childDetailTurnState = "error";
        childDetailMessages = [
          {
            id: "msg-prior" as never,
            role: "assistant",
            text: "stale prior answer",
            turnId: "turn-prior" as never,
            streaming: false,
            createdAt: "2026-06-17T09:59:00.000Z",
            updatedAt: "2026-06-17T09:59:00.000Z",
          },
        ];
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "timeout",
              finalAssistantText: null,
              error: "wait exceeded budget",
            },
          ],
          settledCount: 0,
          timedOutCount: 1,
          pending: false,
          resumeToken: "coordinator-token",
        };

        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: {
              childThreadIds: [childThreadId],
              resumeToken: "-100000:coordinator-token",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          pending: false,
          settledCount: 1,
          timedOutCount: 0,
          results: [
            {
              childThreadId,
              status: "failed",
              finalAssistantText: null,
              error: "Child thread ended with status failed.",
            },
          ],
        });
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailMessages = null;
          childDetailTurnState = "completed";
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("R-C: steer dispatches now for an idle child", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        dispatchedTurns.length = 0;
        insertedDispatches.length = 0;
        childTurnState = "completed";
        childDriverKind = "codex";

        const result = yield* server
          .callTool({
            name: "t3_steer_subagent",
            arguments: { childThreadId, message: "keep going" },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          childThreadId,
          accepted: true,
          applied: "now",
        });
        // Dispatched a turn immediately; nothing deferred to the durable table.
        expect(dispatchedTurns).toEqual([childThreadId]);
        expect(insertedDispatches).toHaveLength(0);
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("R-C: mid-turn unknown-driver steer is deferred to a durable child_steer row", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        dispatchedTurns.length = 0;
        insertedDispatches.length = 0;
        childTurnState = "running";
        childDriverKind = "futuredriver";

        const result = yield* server
          .callTool({
            name: "t3_steer_subagent",
            arguments: { childThreadId, message: "fix the failing test" },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          childThreadId,
          accepted: true,
          applied: "deferred-until-idle",
        });
        // No mid-turn inject reached the unknown driver; the steer was persisted instead.
        expect(dispatchedTurns).toHaveLength(0);
        expect(insertedDispatches).toHaveLength(1);
        expect(insertedDispatches[0]).toMatchObject({
          kind: "child_steer",
          targetThreadId: childThreadId,
          text: "fix the failing test",
        });
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("R-C: mid-turn codex steer dispatches now (codex supports steering)", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        dispatchedTurns.length = 0;
        insertedDispatches.length = 0;
        childTurnState = "running";
        childDriverKind = "codex";

        const result = yield* server
          .callTool({
            name: "t3_steer_subagent",
            arguments: { childThreadId, message: "switch to the other file" },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          childThreadId,
          accepted: true,
          applied: "queued-midturn",
        });
        // Codex steers mid-turn now; nothing deferred to the durable table.
        expect(dispatchedTurns).toEqual([childThreadId]);
        expect(insertedDispatches).toHaveLength(0);
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("R-C: mid-turn cursor steer dispatches now (proven mid-turn driver)", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        dispatchedTurns.length = 0;
        insertedDispatches.length = 0;
        childTurnState = "running";
        childDriverKind = "cursor";

        const result = yield* server
          .callTool({
            name: "t3_steer_subagent",
            arguments: { childThreadId, message: "use the other approach" },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          childThreadId,
          accepted: true,
          applied: "queued-midturn",
        });
        // A proven mid-turn driver folds the steer into the running turn now;
        // nothing is deferred to the durable table.
        expect(dispatchedTurns).toEqual([childThreadId]);
        expect(insertedDispatches).toHaveLength(0);
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("Fix 1: t3_schedule_create routes a plain model to its official harness", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        insertedTasks.length = 0;
        // Both Claude (native) and the Cursor aggregator list opus-4.8; native
        // priority must win (claudeAgent, not cursor) with no hardcoded names.
        modelInstances = [
          makeModelInstance("cursor", "cursor", ["claude-opus-4-8", "auto"]),
          makeModelInstance("claudeAgent", "claudeAgent", [
            { slug: "claude-opus-4-8", optionId: "effort", value: "high" },
            "claude-sonnet-4-6",
          ]),
        ];

        const result = yield* server
          .callTool({
            name: "t3_schedule_create",
            arguments: {
              prompt: "nightly summary",
              intervalSeconds: 3_600,
              model: "claude-opus-4-8",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        // The returned entry confirms the routed harness to the caller...
        expect(result.structuredContent).toMatchObject({
          modelSelection: {
            instanceId: "claudeAgent",
            model: "claude-opus-4-8",
            options: [{ id: "effort", value: "xhigh" }],
          },
        });
        // ...and the persisted task carries the same resolved selection.
        expect(insertedTasks).toHaveLength(1);
        expect(insertedTasks[0]!.modelSelection).toMatchObject({
          instanceId: "claudeAgent",
          model: "claude-opus-4-8",
          options: [{ id: "effort", value: "xhigh" }],
        });
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("t3_schedule_create applies directive default efforts for plain models", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        insertedTasks.length = 0;
        modelInstances = [
          makeModelInstance("codex", "codex", [
            { slug: "gpt-5.5", optionId: "reasoningEffort", value: "medium" },
          ]),
          makeModelInstance("claudeAgent", "claudeAgent", [
            { slug: "claude-opus-4-8", optionId: "effort", value: "high" },
            { slug: "claude-sonnet-5", optionId: "effort", value: "medium" },
            { slug: "claude-fable-5", optionId: "effort", value: "xhigh" },
          ]),
        ];

        const cases = [
          { model: "gpt-5.5", optionId: "reasoningEffort", value: "xhigh" },
          { model: "claude-opus-4-8", optionId: "effort", value: "xhigh" },
          { model: "claude-sonnet-5", optionId: "effort", value: "xhigh" },
          { model: "claude-fable-5", optionId: "effort", value: "high" },
        ] as const;

        for (const row of cases) {
          const result = yield* server
            .callTool({
              name: "t3_schedule_create",
              arguments: {
                prompt: `scheduled ${row.model}`,
                intervalSeconds: 3_600,
                model: row.model,
              },
            })
            .pipe(
              Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
              Effect.provideService(McpSchema.McpServerClient, client),
            );

          expect(result.isError).toBe(false);
        }

        expect(insertedTasks.map((task) => task.modelSelection)).toEqual([
          {
            instanceId: "codex",
            model: "gpt-5.5",
            options: [{ id: "reasoningEffort", value: "xhigh" }],
          },
          {
            instanceId: "claudeAgent",
            model: "claude-opus-4-8",
            options: [{ id: "effort", value: "xhigh" }],
          },
          {
            instanceId: "claudeAgent",
            model: "claude-sonnet-5",
            options: [{ id: "effort", value: "xhigh" }],
          },
          {
            instanceId: "claudeAgent",
            model: "claude-fable-5",
            options: [{ id: "effort", value: "high" }],
          },
        ]);
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("Fix 1: t3_schedule_create fails loudly when no provider serves the model", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        insertedTasks.length = 0;
        modelInstances = [makeModelInstance("codex", "codex", ["gpt-5.4"])];

        const result = yield* server
          .callTool({
            name: "t3_schedule_create",
            arguments: {
              prompt: "nightly summary",
              intervalSeconds: 3_600,
              model: "claude-opus-4-8",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        // An unroutable model errors rather than silently inheriting; nothing persisted.
        expect(result.isError).toBe(true);
        expect(insertedTasks).toHaveLength(0);
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("Fix 1: t3_schedule_update with model:null un-pins the model", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        updatedTasks.length = 0;
        // A task currently pinned to Claude.
        existingTasks = [
          makeScheduledTask({
            instanceId: ProviderInstanceId.make("claudeAgent"),
            model: "claude-opus-4-8",
          }),
        ];

        const result = yield* server
          .callTool({
            name: "t3_schedule_update",
            arguments: { taskId: scheduledTaskId, model: null },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        // The pin is cleared on both the returned entry and the persisted row.
        expect(result.structuredContent).toMatchObject({ modelSelection: null });
        expect(updatedTasks).toHaveLength(1);
        expect(updatedTasks[0]!.modelSelection).toBeNull();
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("Fix 1: t3_schedule_update re-routes to a new plain model", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        updatedTasks.length = 0;
        existingTasks = [makeScheduledTask(null)];
        modelInstances = [
          makeModelInstance("codex", "codex", ["gpt-5.4", "gpt-5.4-mini"]),
          makeModelInstance("claudeAgent", "claudeAgent", ["claude-opus-4-8"]),
        ];

        const result = yield* server
          .callTool({
            name: "t3_schedule_update",
            arguments: { taskId: scheduledTaskId, model: "gpt-5.4" },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(updatedTasks[0]!.modelSelection).toMatchObject({
          instanceId: "codex",
          model: "gpt-5.4",
        });
      }),
    ).pipe(Effect.provide(TestLayer)),
  );
});
