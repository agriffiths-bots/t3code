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
    turnId: "turn-1" as never,
    state: "completed",
    requestedAt: "2026-06-17T10:00:00.000Z",
    startedAt: "2026-06-17T10:00:00.000Z",
    completedAt: "2026-06-17T10:01:00.000Z",
    assistantMessageId: null,
  },
  createdAt: "2026-06-17T09:00:00.000Z",
  updatedAt: "2026-06-17T10:01:00.000Z",
  archivedAt: null,
  deletedAt: null,
  messages: [
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
  checkpoints: [],
  session: null,
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

// A minimal provider instance exposing just what buildModelSources reads: an id,
// a driver kind, enabled=true, and a snapshot whose models list the given slugs.
const makeModelInstance = (
  instanceId: string,
  driverKind: string,
  slugs: ReadonlyArray<string>,
) => ({
  instanceId: ProviderInstanceId.make(instanceId),
  driverKind,
  enabled: true,
  snapshot: {
    getSnapshot: Effect.succeed({
      models: slugs.map((slug) => ({ slug, capabilities: null })),
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
    Effect.succeed(threadId === childThreadId ? Option.some(makeChildDetail()) : Option.none()),
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
              turnCount: 0,
            },
          ],
        });
      }),
    ).pipe(Effect.provide(TestLayer)),
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
          makeModelInstance("claudeAgent", "claudeAgent", ["claude-opus-4-8", "claude-sonnet-4-6"]),
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
          modelSelection: { instanceId: "claudeAgent", model: "claude-opus-4-8" },
        });
        // ...and the persisted task carries the same resolved selection.
        expect(insertedTasks).toHaveLength(1);
        expect(insertedTasks[0]!.modelSelection).toMatchObject({
          instanceId: "claudeAgent",
          model: "claude-opus-4-8",
        });
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
