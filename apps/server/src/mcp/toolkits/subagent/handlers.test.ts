import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type ModelSelection,
  type OrchestrationThread,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { McpSchema, McpServer } from "effect/unstable/ai";
import { describe, expect, it } from "@effect/vitest";

import { ChildThreadCoordinator } from "../../../orchestration/Services/ChildThreadCoordinator.ts";
import { ActiveChildThreadCoordinatorLive } from "../../../orchestration/Layers/ChildThreadCoordinator.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ScheduledTaskRepository } from "../../../persistence/Services/ScheduledTasks.ts";
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
  getThreadShellById: () => unsupported(),
  getThreadDetailById: (threadId) =>
    Effect.succeed(threadId === childThreadId ? Option.some(makeChildDetail()) : Option.none()),
});

const coordinatorLayer = Layer.succeed(ChildThreadCoordinator, {
  register: () => Effect.void,
  waitSlice: () => unsupported(),
  assertParent: () => Effect.void,
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
});

const engineLayer = Layer.succeed(OrchestrationEngineService, {
  readEvents: () => unsupported(),
  dispatch: () => unsupported(),
  streamDomainEvents: unsupported(),
});

const providerRegistryLayer = Layer.succeed(ProviderInstanceRegistry, {
  getInstance: () => Effect.succeed(undefined),
  listInstances: Effect.succeed([]),
  listUnavailable: Effect.succeed([]),
  changes: unsupported(),
} as never);

const scheduledTasksLayer = Layer.succeed(ScheduledTaskRepository, {
  listDue: () => Effect.succeed([]),
  insert: () => Effect.void,
  update: () => Effect.void,
  delete: () => Effect.void,
  markRun: () => Effect.void,
  listAll: () => Effect.succeed([]),
  listByThread: () => Effect.succeed([]),
});

const RuntimeActivationLive = Layer.mergeAll(
  SubagentRuntimeLive,
  ActiveChildThreadCoordinatorLive,
).pipe(
  Layer.provideMerge(coordinatorLayer),
  Layer.provideMerge(projectionLayer),
  Layer.provideMerge(engineLayer),
  Layer.provideMerge(providerRegistryLayer),
  Layer.provideMerge(scheduledTasksLayer),
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
        const result = yield* server
          .callTool({ name: "t3_list_subagents", arguments: {} })
          .pipe(
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
});
