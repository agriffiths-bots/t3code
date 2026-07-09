import {
  EnvironmentId,
  IsoDateTime,
  NonNegativeInt,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type ModelSelection,
  type OrchestrationCommand,
  type OrchestrationProjectShell,
  type OrchestrationThread,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import { McpSchema, McpServer } from "effect/unstable/ai";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";
import { describe, expect, it } from "@effect/vitest";

import {
  ChildThreadCoordinator,
  type ChildThreadCoordinatorShape,
  type WaitDeliveredMark,
  type WaitSliceInput,
  type WaitSliceResult,
} from "../../../orchestration/Services/ChildThreadCoordinator.ts";
import { GitWorkflowService } from "../../../git/GitWorkflowService.ts";
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
  RemoteChildRepository,
  type RemoteChild,
} from "../../../persistence/Services/RemoteChildren.ts";
import { PersistenceSqlError } from "../../../persistence/Errors.ts";
import {
  ScheduledTaskRepository,
  ScheduledTaskId,
  type ScheduledTask,
} from "../../../persistence/Services/ScheduledTasks.ts";
import { ProviderInstanceRegistry } from "../../../provider/Services/ProviderInstanceRegistry.ts";
import * as VcsDriverRegistry from "../../../vcs/VcsDriverRegistry.ts";
import { SubagentToolkitRegistrationLive } from "../../McpHttpServer.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as SubagentPeerRegistry from "../../../subagents/SubagentPeerRegistry.ts";
import { ThreadStartRuntimeLive } from "../thread/handlers.ts";
import { ThreadStartToolError } from "../thread/tools.ts";
import * as SubagentDispatchLimiter from "./SubagentDispatchLimiter.ts";
import { SubagentRuntimeLive } from "./handlers.ts";

const environmentId = EnvironmentId.make("environment-subagent-test");
const projectId = ProjectId.make("project-subagent-test");
const parentThreadId = ThreadId.make("thread-subagent-parent");
const childThreadId = ThreadId.make("thread-subagent-child");
const codexModel: ModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5-codex",
};
const parentProject: OrchestrationProjectShell = {
  id: projectId,
  title: "Project",
  workspaceRoot: "/repo",
  repositoryIdentity: {
    canonicalKey: "git-local:/repo",
    locator: {
      source: "git-local",
      rootPath: "/repo",
    },
    rootPath: "/repo",
  },
  defaultModelSelection: codexModel,
  scripts: [],
  createdAt: "2026-06-17T09:00:00.000Z",
  updatedAt: "2026-06-17T09:00:00.000Z",
};
let activeProjectShell: OrchestrationProjectShell = parentProject;

const invocation = {
  credentialKind: "provider-session" as const,
  environmentId,
  threadId: parentThreadId,
  providerSessionId: "provider-session-subagent-test",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(["thread-management"] as const),
  issuedAt: 1,
  expiresAt: Number.MAX_SAFE_INTEGER,
};
const peerInvocation: McpInvocationContext.PeerMcpInvocationScope = {
  credentialKind: "peer",
  environmentId,
  peerTokenId: "peer-subagent-test",
  capabilities: new Set(["subagent:spawn", "subagent:check", "subagent:wait", "subagent:list"]),
  allowedParentThreadIds: new Set(),
  allowedChildThreadIds: new Set(),
  issuedAt: 1,
  expiresAt: null,
};
const unrestrictedPeerInvocation: McpInvocationContext.PeerMcpInvocationScope = {
  credentialKind: "peer",
  environmentId,
  peerTokenId: "peer-subagent-unrestricted-test",
  capabilities: new Set(["subagent:spawn", "subagent:check", "subagent:wait", "subagent:list"]),
  issuedAt: 1,
  expiresAt: null,
};
const entitledPeerInvocation: McpInvocationContext.PeerMcpInvocationScope = {
  ...peerInvocation,
  peerTokenId: "peer-subagent-entitled-test",
  allowedParentThreadIds: new Set([parentThreadId]),
  allowedChildThreadIds: new Set([childThreadId]),
};
const childOnlyPeerInvocation: McpInvocationContext.PeerMcpInvocationScope = {
  ...peerInvocation,
  peerTokenId: "peer-subagent-child-only-test",
  allowedChildThreadIds: new Set([childThreadId]),
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

const TestCryptoLive = Layer.sync(Crypto.Crypto, () => {
  let nextByte = 0;
  return Crypto.make({
    randomBytes: (size) =>
      Uint8Array.from({ length: size }, () => {
        nextByte = (nextByte + 1) % 256;
        return nextByte;
      }),
    digest: (_algorithm, data) => Effect.succeed(data),
  });
});

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
let failCoordinatorRegister = false;
let waitSliceEffect: ((input: WaitSliceInput) => Effect.Effect<WaitSliceResult>) | null = null;
const waitSliceCalls: WaitSliceInput[] = [];
const registeredChildren: Array<{
  readonly childThreadId: ThreadId;
  readonly parentThreadId: ThreadId;
}> = [];
const engineCommands: OrchestrationCommand[] = [];
let childDetailDelayMs = 0;
let childDetailUnavailable = false;
let childDetailFailOnCall: number | null = null;
let childDetailCallCount = 0;
// R-C seams: the child's turn state and its provider driver kind drive the
// idle/mid-turn + defer/dispatch decision. Defaults: idle child, unknown driver.
let childTurnState: "completed" | "running" = "completed";
let childDriverKind: string | undefined = undefined;
const promotedCalls: Array<ReadonlyArray<ThreadId>> = [];
let promoteToWakeDefect: unknown | null = null;
const markWaitDeliveredCalls: Array<ReadonlyArray<WaitDeliveredMark>> = [];
const abandonWaitDeliveryCalls: Array<ReadonlyArray<ThreadId>> = [];
const enqueuedParentInjections: Array<{
  readonly parentThreadId: ThreadId;
  readonly childThreadId: ThreadId;
  readonly status: string;
  readonly finalAssistantText: string | null;
  readonly error: string | null;
}> = [];
const assertParentCalls: Array<{
  readonly parentThreadId: ThreadId;
  readonly childThreadId: ThreadId;
}> = [];
let assertParentFailureChild: ThreadId | null = null;
const dispatchedTurns: Array<ThreadId> = [];
const dispatchedTurnCommands: Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>> =
  [];
const insertedDispatches: Array<PendingDispatch> = [];
const insertedRemoteChildren: Array<RemoteChild> = [];
let remoteChildRows: ReadonlyArray<RemoteChild> = [];
const updatedRemoteChildren: Array<{
  readonly parentThreadId: ThreadId;
  readonly childEnvironmentId: EnvironmentId;
  readonly childThreadId: ThreadId;
  readonly status: string;
}> = [];
let peerRegistryPeers: ReadonlyArray<SubagentPeerRegistry.SubagentPeer> = [];
const peerHttpRequests: Array<{ readonly method: string; readonly url: string }> = [];
let peerHttpHandler:
  | ((
      request: HttpClientRequest.HttpClientRequest,
    ) => Effect.Effect<HttpClientResponse.HttpClientResponse>)
  | null = null;
// Fix 1 seams: the enabled provider instances (with their live model lists) the
// schedule handlers resolve a plain `model` against, and the tasks they persist.
let modelInstances: ReadonlyArray<unknown> = [];
const insertedTasks: Array<{ readonly modelSelection: ModelSelection | null }> = [];
// Existing scheduled tasks visible to t3_schedule_update (via listAll), and the
// updated rows it writes back — so a test can assert a model re-route / un-pin.
let existingTasks: ReadonlyArray<ScheduledTask> = [];
const updatedTasks: Array<ScheduledTask> = [];

const peerEnvironmentId = EnvironmentId.make("environment-peer-b");
const remoteChildThreadId = ThreadId.make("thread-remote-child");
const remoteProjectId = ProjectId.make("project-remote-child");
const bearerPeer = (): SubagentPeerRegistry.SubagentPeer => ({
  alias: "peer-b",
  environmentId: peerEnvironmentId,
  httpBaseUrl: "https://peer.example/",
  mcpEndpoint: "https://peer.example/mcp",
  credential: new SubagentPeerRegistry.SubagentPeerBearerCredential({
    token: "peer-token",
  }),
  pairedAt: "2026-07-08T09:00:00.000Z",
  lastSeenAt: "2026-07-08T09:01:00.000Z",
});

const remoteChildRow = (status: RemoteChild["status"] = "running"): RemoteChild => ({
  parentThreadId,
  childEnvironmentId: peerEnvironmentId,
  childThreadId: remoteChildThreadId,
  alias: "peer-b",
  spawnParams: { prompt: "remote", directory: "/remote/repo", detached: true },
  status,
  lastPolledAt: null,
  createdAt: IsoDateTime.make("2026-07-08T09:02:00.000Z"),
  updatedAt: IsoDateTime.make("2026-07-08T09:02:00.000Z"),
});

const jsonRpcResponse = (id: number, result: unknown) => ({
  jsonrpc: "2.0",
  id,
  result,
});

type RemoteCheckFixture = {
  readonly threadId?: ThreadId;
  readonly status: string;
  readonly turnCount: number;
  readonly latestAssistantText: string | null;
};

const decodeHttpClientRequestJson = (request: HttpClientRequest.HttpClientRequest) => {
  const rawBody = (request.body as { readonly body?: Uint8Array }).body;
  if (rawBody === undefined) throw new Error("Expected request body.");
  return JSON.parse(new TextDecoder().decode(rawBody)) as {
    readonly id?: number;
    readonly method: string;
    readonly params?: { readonly arguments?: Record<string, unknown> };
  };
};

const remoteCheckPeerHandlerWith =
  (resolveCheck: (body: ReturnType<typeof decodeHttpClientRequestJson>) => RemoteCheckFixture) =>
  (request: HttpClientRequest.HttpClientRequest) => {
    if (request.url === "https://peer.example/mcp" && request.method === "DELETE") {
      return Effect.succeed(
        HttpClientResponse.fromWeb(request, new Response(null, { status: 204 })),
      );
    }
    if (request.url === "https://peer.example/mcp" && request.method === "POST") {
      return Effect.sync(() => {
        const body = decodeHttpClientRequestJson(request);
        if (body.method === "initialize") {
          return HttpClientResponse.fromWeb(
            request,
            Response.json(
              jsonRpcResponse(body.id ?? 1, {
                protocolVersion: "2025-06-18",
                capabilities: { tools: {} },
                serverInfo: { name: "peer", version: "0.0.0-test" },
              }),
              {
                headers: {
                  "mcp-session-id": "session-remote-check",
                  "mcp-protocol-version": "2025-06-18",
                },
              },
            ),
          );
        }
        if (body.method === "notifications/initialized") {
          return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
        }
        if (body.method === "tools/call") {
          const check = resolveCheck(body);
          return HttpClientResponse.fromWeb(
            request,
            Response.json(
              jsonRpcResponse(body.id ?? 2, {
                content: [{ type: "text", text: "checked" }],
                structuredContent: {
                  threadId: check.threadId ?? remoteChildThreadId,
                  status: check.status,
                  turnCount: check.turnCount,
                  latestAssistantText: check.latestAssistantText,
                },
                isError: false,
              }),
            ),
          );
        }
        return HttpClientResponse.fromWeb(
          request,
          Response.json({ error: "unexpected method" }, { status: 500 }),
        );
      });
    }
    return Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        Response.json({ error: "unexpected request" }, { status: 404 }),
      ),
    );
  };

const remoteCheckPeerHandler = (check: RemoteCheckFixture) =>
  remoteCheckPeerHandlerWith(() => check);

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
  getShellSnapshot: () =>
    Effect.succeed({
      snapshotSequence: 1,
      projects: [activeProjectShell],
      threads: [makeParentShell(), makeChildShell(childTurnState)],
      updatedAt: "2026-06-17T09:00:00.000Z",
    }),
  getArchivedShellSnapshot: () => unsupported(),
  getSnapshotSequence: () => unsupported(),
  getCounts: () => unsupported(),
  getActiveProjectByWorkspaceRoot: () => unsupported(),
  getProjectShellById: (id) =>
    Effect.succeed(id === activeProjectShell.id ? Option.some(activeProjectShell) : Option.none()),
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
      if (threadId === childThreadId) {
        childDetailCallCount += 1;
        if (childDetailFailOnCall === childDetailCallCount) {
          return Effect.fail(
            new PersistenceSqlError({
              operation: "ProjectionSnapshotQuery.getThreadDetailById:test",
              detail: "detail read failed",
            }),
          );
        }
      }
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
  validateSpawn: () => Effect.succeed({ depth: 1 }),
  register: (input) =>
    failCoordinatorRegister
      ? Effect.fail(new ThreadStartToolError({ message: "register failed" }))
      : Effect.sync(() => {
          registeredChildren.push({
            childThreadId: input.childThreadId,
            parentThreadId: input.parentThreadId,
          });
        }),
  waitSlice: (input) =>
    Effect.sync(() => {
      waitSliceCalls.push(input);
    }).pipe(
      Effect.flatMap(() =>
        waitSliceEffect !== null
          ? waitSliceEffect(input)
          : waitSliceResult
            ? Effect.succeed(waitSliceResult)
            : unsupported(),
      ),
    ),
  assertParent: (parent, child) =>
    Effect.sync(() => {
      assertParentCalls.push({ parentThreadId: parent, childThreadId: child });
    }).pipe(
      Effect.flatMap(() =>
        assertParentFailureChild !== null && child === assertParentFailureChild
          ? Effect.fail(new ThreadStartToolError({ message: "child is not owned by this parent" }))
          : Effect.void,
      ),
    ),
  promoteToWake: (ids) =>
    Effect.sync(() => void promotedCalls.push(ids)).pipe(
      Effect.flatMap(() =>
        promoteToWakeDefect === null ? Effect.void : Effect.die(promoteToWakeDefect),
      ),
    ),
  markWaitDelivered: (marks) => Effect.sync(() => void markWaitDeliveredCalls.push(marks)),
  abandonWaitDelivery: (ids) => Effect.sync(() => void abandonWaitDeliveryCalls.push(ids)),
  hasPendingInjections: () => Effect.succeed(false),
  enqueueParentInjection: (input) =>
    Effect.sync(() => {
      if (
        enqueuedParentInjections.some(
          (row) =>
            row.parentThreadId === input.parentThreadId &&
            row.childThreadId === input.childThreadId,
        )
      ) {
        return;
      }
      enqueuedParentInjections.push(input);
    }),
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
  dispatch: (command) =>
    Effect.sync(() => {
      engineCommands.push(command);
      return { sequence: engineCommands.length };
    }),
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
  markWaitDelivered: () => Effect.void,
  deleteByIds: () => Effect.void,
});

const remoteChildrenLayer = Layer.succeed(RemoteChildRepository, {
  upsert: (row) => Effect.sync(() => void insertedRemoteChildren.push(row)),
  getByChild: () => Effect.succeed(Option.none()),
  listByParent: ({ parentThreadId }) =>
    Effect.succeed(remoteChildRows.filter((row) => row.parentThreadId === parentThreadId)),
  listAll: () => Effect.succeed(remoteChildRows),
  updateStatus: (input) =>
    Effect.sync(() => {
      updatedRemoteChildren.push(input);
    }),
});

const peerRegistryLayer = Layer.succeed(SubagentPeerRegistry.SubagentPeerRegistry, {
  add: () => unsupported(),
  list: Effect.suspend(() => Effect.succeed(peerRegistryPeers)),
  remove: () => unsupported(),
  getByAlias: (alias) =>
    Effect.suspend(() =>
      Effect.succeed(
        Option.fromUndefinedOr(peerRegistryPeers.find((peer) => peer.alias === alias)),
      ),
    ),
  resolveTarget: (target) =>
    Effect.suspend(() => {
      const peer =
        peerRegistryPeers.find((candidate) => candidate.alias === target) ??
        peerRegistryPeers.find((candidate) => candidate.environmentId === target);
      return peer
        ? Effect.succeed(peer)
        : Effect.fail(
            new SubagentPeerRegistry.SubagentPeerTargetNotFoundError({
              target,
              knownAliases: peerRegistryPeers.map((candidate) => candidate.alias),
            }),
          );
    }),
  updateLastSeen: (alias) =>
    Effect.suspend(() =>
      Effect.succeed(
        Option.fromUndefinedOr(peerRegistryPeers.find((peer) => peer.alias === alias)),
      ),
    ),
});

const peerHttpClientLayer = Layer.succeed(
  HttpClient.HttpClient,
  HttpClient.make((request) => {
    peerHttpRequests.push({ method: request.method, url: request.url });
    return peerHttpHandler ? peerHttpHandler(request) : Effect.die("unexpected peer HTTP call");
  }),
);

const gitWorkflowLayer = Layer.mock(GitWorkflowService)({
  listRefs: () =>
    Effect.succeed({
      refs: [
        {
          name: "main",
          current: false,
          isDefault: true,
          isRemote: false,
          worktreePath: null,
        },
      ],
      isRepo: true,
      hasPrimaryRemote: true,
      nextCursor: null,
      totalCount: 1,
    }),
  status: () =>
    Effect.succeed({
      isRepo: true,
      hasPrimaryRemote: true,
      isDefaultRef: false,
      refName: "main",
      hasWorkingTreeChanges: false,
      workingTree: {
        files: [],
        insertions: 0,
        deletions: 0,
      },
      hasUpstream: true,
      aheadCount: 0,
      behindCount: 0,
      aheadOfDefaultCount: 0,
      pr: null,
    }),
});

const vcsFreshness = {
  source: "live-local" as const,
  observedAt: DateTime.makeUnsafe("1970-01-01T00:00:00.000Z"),
  expiresAt: Option.none(),
};

const vcsDriverRegistryLayer = Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
  detect: () =>
    Effect.succeed({
      kind: "git",
      repository: {
        kind: "git",
        rootPath: "/repo",
        metadataPath: "/repo/.git",
        freshness: vcsFreshness,
      },
      driver: {} as VcsDriverRegistry.VcsDriverHandle["driver"],
    } satisfies VcsDriverRegistry.VcsDriverHandle),
});

// Recording bootstrap dispatcher: the R-C "now"/"queued" steer path goes through
// dispatchActive -> this dispatcher, so the test can assert a turn was started.
const dispatcherLayer = Layer.succeed(BootstrapTurnStartDispatcher, {
  dispatch: (command) =>
    Effect.sync(() => {
      dispatchedTurns.push(command.threadId);
      dispatchedTurnCommands.push(command);
      return { sequence: dispatchedTurns.length };
    }),
});

const RuntimeActivationLive = Layer.mergeAll(
  SubagentRuntimeLive,
  ThreadStartRuntimeLive,
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
  Layer.provideMerge(remoteChildrenLayer),
  Layer.provideMerge(peerRegistryLayer),
  Layer.provideMerge(peerHttpClientLayer),
  Layer.provideMerge(gitWorkflowLayer),
  Layer.provideMerge(vcsDriverRegistryLayer),
  Layer.provideMerge(SubagentDispatchLimiter.layerTest(1)),
  Layer.provideMerge(TestCryptoLive),
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

  it.effect("spawns a detached child as a plain thread turn start", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        childDriverKind = "codex";
        dispatchedTurns.length = 0;
        dispatchedTurnCommands.length = 0;
        registeredChildren.length = 0;
        peerHttpRequests.length = 0;
        insertedRemoteChildren.length = 0;

        const result = yield* server
          .callTool({
            name: "t3_spawn_subagent",
            arguments: {
              prompt: "run as a normal thread",
              title: "plain child",
              mode: "current_checkout",
              detached: true,
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(dispatchedTurnCommands).toHaveLength(1);
        const command = dispatchedTurnCommands[0];
        if (!command) throw new Error("Expected spawn to dispatch a child turn start.");
        expect(command.type).toBe("thread.turn.start");
        expect("providerSessionDetached" in command).toBe(false);
        expect(registeredChildren).toEqual([{ childThreadId: command.threadId, parentThreadId }]);
        expect(peerHttpRequests).toEqual([]);
        expect(insertedRemoteChildren).toEqual([]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDriverKind = undefined;
          dispatchedTurns.length = 0;
          dispatchedTurnCommands.length = 0;
          registeredChildren.length = 0;
          peerHttpRequests.length = 0;
          insertedRemoteChildren.length = 0;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("spawns a remote child through a resolved peer target and records it", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        peerRegistryPeers = [bearerPeer()];
        peerHttpRequests.length = 0;
        insertedRemoteChildren.length = 0;
        registeredChildren.length = 0;
        let capturedRemoteArguments: Record<string, unknown> | undefined;
        peerHttpHandler = (request) => {
          if (request.url === "https://peer.example/.well-known/t3/environment") {
            return Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                Response.json({
                  environmentId: peerEnvironmentId,
                  label: "Peer B",
                  platform: { os: "linux", arch: "x64" },
                  serverVersion: "0.0.0-test",
                  capabilities: { repositoryIdentity: true },
                }),
              ),
            );
          }
          if (request.url === "https://peer.example/mcp" && request.method === "DELETE") {
            return Effect.succeed(
              HttpClientResponse.fromWeb(request, new Response(null, { status: 204 })),
            );
          }
          if (request.url === "https://peer.example/mcp" && request.method === "POST") {
            return Effect.sync(() => {
              const body = decodeHttpClientRequestJson(request);
              if (body.method === "initialize") {
                return HttpClientResponse.fromWeb(
                  request,
                  Response.json(
                    jsonRpcResponse(body.id ?? 1, {
                      protocolVersion: "2025-06-18",
                      capabilities: { tools: {} },
                      serverInfo: { name: "peer", version: "0.0.0-test" },
                    }),
                    {
                      headers: {
                        "mcp-session-id": "session-remote-spawn",
                        "mcp-protocol-version": "2025-06-18",
                      },
                    },
                  ),
                );
              }
              if (body.method === "notifications/initialized") {
                return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
              }
              if (body.method === "tools/call") {
                capturedRemoteArguments = body.params?.arguments;
                return HttpClientResponse.fromWeb(
                  request,
                  Response.json(
                    jsonRpcResponse(body.id ?? 2, {
                      content: [{ type: "text", text: "spawned" }],
                      structuredContent: {
                        childThreadId: remoteChildThreadId,
                        projectId: remoteProjectId,
                        mode: "current_checkout",
                        branch: null,
                        worktreePath: "/remote/repo",
                        parentThreadId,
                      },
                      isError: false,
                    }),
                  ),
                );
              }
              return HttpClientResponse.fromWeb(
                request,
                Response.json({ error: "unexpected method" }, { status: 500 }),
              );
            });
          }
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              Response.json({ error: "unexpected request" }, { status: 404 }),
            ),
          );
        };

        const result = yield* server
          .callTool({
            name: "t3_spawn_subagent",
            arguments: {
              prompt: "run elsewhere",
              target: "peer-b",
              directory: "/remote/repo",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          childThreadId: remoteChildThreadId,
          parentThreadId,
        });
        expect(capturedRemoteArguments).toMatchObject({
          prompt: "run elsewhere",
          directory: "/remote/repo",
          detached: true,
          remoteParentThreadId: parentThreadId,
          remoteParentEnvironmentId: environmentId,
        });
        expect(insertedRemoteChildren).toHaveLength(1);
        expect(insertedRemoteChildren[0]).toMatchObject({
          parentThreadId,
          childEnvironmentId: peerEnvironmentId,
          childThreadId: remoteChildThreadId,
          alias: "peer-b",
          status: "running",
        });
        expect(registeredChildren).toEqual([]);
        expect(peerHttpRequests.map((request) => request.url)).toEqual([
          "https://peer.example/.well-known/t3/environment",
          "https://peer.example/mcp",
          "https://peer.example/mcp",
          "https://peer.example/mcp",
          "https://peer.example/mcp",
        ]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          peerRegistryPeers = [];
          peerHttpHandler = null;
          peerHttpRequests.length = 0;
          insertedRemoteChildren.length = 0;
          registeredChildren.length = 0;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("does not timeout a side-effecting remote spawn tools call", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        const toolCallStarted = yield* Deferred.make<void>();
        peerRegistryPeers = [bearerPeer()];
        insertedRemoteChildren.length = 0;
        peerHttpHandler = (request) => {
          if (request.url === "https://peer.example/.well-known/t3/environment") {
            return Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                Response.json({
                  environmentId: peerEnvironmentId,
                  label: "Peer B",
                  platform: { os: "linux", arch: "x64" },
                  serverVersion: "0.0.0-test",
                  capabilities: { repositoryIdentity: true },
                }),
              ),
            );
          }
          if (request.url === "https://peer.example/mcp" && request.method === "DELETE") {
            return Effect.succeed(
              HttpClientResponse.fromWeb(request, new Response(null, { status: 204 })),
            );
          }
          if (request.url === "https://peer.example/mcp" && request.method === "POST") {
            return Effect.gen(function* () {
              const body = decodeHttpClientRequestJson(request);
              if (body.method === "initialize") {
                return HttpClientResponse.fromWeb(
                  request,
                  Response.json(
                    jsonRpcResponse(body.id ?? 1, {
                      protocolVersion: "2025-06-18",
                      capabilities: { tools: {} },
                      serverInfo: { name: "peer", version: "0.0.0-test" },
                    }),
                    {
                      headers: {
                        "mcp-session-id": "session-remote-spawn-slow",
                        "mcp-protocol-version": "2025-06-18",
                      },
                    },
                  ),
                );
              }
              if (body.method === "notifications/initialized") {
                return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
              }
              if (body.method === "tools/call") {
                yield* Deferred.succeed(toolCallStarted, void 0);
                yield* TestClock.adjust(Duration.seconds(6));
                return HttpClientResponse.fromWeb(
                  request,
                  Response.json(
                    jsonRpcResponse(body.id ?? 2, {
                      content: [{ type: "text", text: "spawned slowly" }],
                      structuredContent: {
                        childThreadId: remoteChildThreadId,
                        projectId: remoteProjectId,
                        mode: "current_checkout",
                        branch: null,
                        worktreePath: "/remote/repo",
                        parentThreadId,
                      },
                      isError: false,
                    }),
                  ),
                );
              }
              return HttpClientResponse.fromWeb(
                request,
                Response.json({ error: "unexpected method" }, { status: 500 }),
              );
            });
          }
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              Response.json({ error: "unexpected request" }, { status: 404 }),
            ),
          );
        };

        const fiber = yield* server
          .callTool({
            name: "t3_spawn_subagent",
            arguments: {
              prompt: "run slowly elsewhere",
              target: "peer-b",
              directory: "/remote/repo",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
            Effect.forkScoped,
          );
        yield* Deferred.await(toolCallStarted);
        const result = yield* Fiber.join(fiber);

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          childThreadId: remoteChildThreadId,
          parentThreadId,
        });
        expect(insertedRemoteChildren).toHaveLength(1);
        expect(insertedRemoteChildren[0]).toMatchObject({
          parentThreadId,
          childEnvironmentId: peerEnvironmentId,
          childThreadId: remoteChildThreadId,
          status: "running",
        });
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          peerRegistryPeers = [];
          peerHttpHandler = null;
          insertedRemoteChildren.length = 0;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("records a remote spawn when MCP session cleanup stalls", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        const deleteStarted = yield* Deferred.make<void>();
        peerRegistryPeers = [bearerPeer()];
        insertedRemoteChildren.length = 0;
        peerHttpHandler = (request) => {
          if (request.url === "https://peer.example/.well-known/t3/environment") {
            return Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                Response.json({
                  environmentId: peerEnvironmentId,
                  label: "Peer B",
                  platform: { os: "linux", arch: "x64" },
                  serverVersion: "0.0.0-test",
                  capabilities: { repositoryIdentity: true },
                }),
              ),
            );
          }
          if (request.url === "https://peer.example/mcp" && request.method === "DELETE") {
            return Effect.gen(function* () {
              yield* Deferred.succeed(deleteStarted, void 0);
              return yield* Effect.never;
            });
          }
          if (request.url === "https://peer.example/mcp" && request.method === "POST") {
            return Effect.sync(() => {
              const body = decodeHttpClientRequestJson(request);
              if (body.method === "initialize") {
                return HttpClientResponse.fromWeb(
                  request,
                  Response.json(
                    jsonRpcResponse(body.id ?? 1, {
                      protocolVersion: "2025-06-18",
                      capabilities: { tools: {} },
                      serverInfo: { name: "peer", version: "0.0.0-test" },
                    }),
                    {
                      headers: {
                        "mcp-session-id": "session-remote-spawn-cleanup-hangs",
                        "mcp-protocol-version": "2025-06-18",
                      },
                    },
                  ),
                );
              }
              if (body.method === "notifications/initialized") {
                return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
              }
              if (body.method === "tools/call") {
                return HttpClientResponse.fromWeb(
                  request,
                  Response.json(
                    jsonRpcResponse(body.id ?? 2, {
                      content: [{ type: "text", text: "spawned before cleanup stalled" }],
                      structuredContent: {
                        childThreadId: remoteChildThreadId,
                        projectId: remoteProjectId,
                        mode: "current_checkout",
                        branch: null,
                        worktreePath: "/remote/repo",
                        parentThreadId,
                      },
                      isError: false,
                    }),
                  ),
                );
              }
              return HttpClientResponse.fromWeb(
                request,
                Response.json({ error: "unexpected method" }, { status: 500 }),
              );
            });
          }
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              Response.json({ error: "unexpected request" }, { status: 404 }),
            ),
          );
        };

        const fiber = yield* server
          .callTool({
            name: "t3_spawn_subagent",
            arguments: {
              prompt: "run elsewhere before cleanup stalls",
              target: "peer-b",
              directory: "/remote/repo",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
            Effect.forkScoped,
          );
        yield* Deferred.await(deleteStarted);
        yield* TestClock.adjust(Duration.seconds(3));
        const result = yield* Fiber.join(fiber);

        expect(result.isError).toBe(false);
        expect(insertedRemoteChildren).toHaveLength(1);
        expect(insertedRemoteChildren[0]).toMatchObject({
          parentThreadId,
          childEnvironmentId: peerEnvironmentId,
          childThreadId: remoteChildThreadId,
          status: "running",
        });
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          peerRegistryPeers = [];
          peerHttpHandler = null;
          insertedRemoteChildren.length = 0;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("fails remote spawn before probing when target is unknown", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        peerRegistryPeers = [bearerPeer()];
        peerHttpRequests.length = 0;

        const result = yield* server
          .callTool({
            name: "t3_spawn_subagent",
            arguments: {
              prompt: "run nowhere",
              target: "missing",
              directory: "/remote/repo",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(true);
        const content = result.content?.[0];
        expect(content?.type).toBe("text");
        if (content?.type === "text") {
          expect(content.text).toContain("Subagent peer target 'missing' is not registered");
          expect(content.text).toContain("peer-b");
        }
        expect(peerHttpRequests).toEqual([]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          peerRegistryPeers = [];
          peerHttpRequests.length = 0;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("checks a remote child through its recorded peer and enqueues terminal wake", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        peerRegistryPeers = [bearerPeer()];
        remoteChildRows = [remoteChildRow("running")];
        peerHttpRequests.length = 0;
        updatedRemoteChildren.length = 0;
        enqueuedParentInjections.length = 0;
        peerHttpHandler = remoteCheckPeerHandler({
          status: "completed",
          turnCount: 3,
          latestAssistantText: "remote done",
        });

        const result = yield* server
          .callTool({
            name: "t3_check_subagent",
            arguments: { childThreadId: remoteChildThreadId },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          threadId: remoteChildThreadId,
          status: "completed",
          turnCount: 3,
          latestAssistantText: "remote done",
        });
        expect(enqueuedParentInjections).toEqual([
          {
            parentThreadId,
            childThreadId: remoteChildThreadId,
            status: "completed",
            finalAssistantText: "remote done",
            error: null,
          },
        ]);
        expect(updatedRemoteChildren).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              parentThreadId,
              childEnvironmentId: peerEnvironmentId,
              childThreadId: remoteChildThreadId,
              status: "completed",
            }),
          ]),
        );
        expect(peerHttpRequests).toEqual(
          expect.arrayContaining([{ method: "DELETE", url: "https://peer.example/mcp" }]),
        );
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          peerRegistryPeers = [];
          remoteChildRows = [];
          peerHttpRequests.length = 0;
          updatedRemoteChildren.length = 0;
          enqueuedParentInjections.length = 0;
          peerHttpHandler = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("bounds remote child checks when the peer MCP call does not answer", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        const requestStarted = yield* Deferred.make<void>();
        peerRegistryPeers = [bearerPeer()];
        remoteChildRows = [remoteChildRow("running")];
        peerHttpHandler = (request) => {
          if (request.url === "https://peer.example/mcp" && request.method === "POST") {
            return Effect.gen(function* () {
              yield* Deferred.succeed(requestStarted, void 0);
              return yield* Effect.never;
            });
          }
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              Response.json({ error: "unexpected request" }, { status: 404 }),
            ),
          );
        };

        const fiber = yield* server
          .callTool({
            name: "t3_check_subagent",
            arguments: { childThreadId: remoteChildThreadId },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
            Effect.forkScoped,
          );
        yield* Deferred.await(requestStarted);
        yield* TestClock.adjust(Duration.seconds(6));
        const result = yield* Fiber.join(fiber);

        expect(result.isError).toBe(true);
        const content = result.content?.[0];
        expect(content?.type).toBe("text");
        if (content?.type === "text") {
          expect(content.text).toContain("did not respond within 5 seconds");
        }
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          peerRegistryPeers = [];
          remoteChildRows = [];
          peerHttpHandler = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("rejects remote check results for a different child thread", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        const otherThreadId = ThreadId.make("thread-remote-other");
        peerRegistryPeers = [bearerPeer()];
        remoteChildRows = [remoteChildRow("running")];
        updatedRemoteChildren.length = 0;
        enqueuedParentInjections.length = 0;
        peerHttpHandler = remoteCheckPeerHandler({
          threadId: otherThreadId,
          status: "completed",
          turnCount: 1,
          latestAssistantText: "wrong child",
        });

        const result = yield* server
          .callTool({
            name: "t3_check_subagent",
            arguments: { childThreadId: remoteChildThreadId },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(true);
        const content = result.content?.[0];
        expect(content?.type).toBe("text");
        if (content?.type === "text") {
          expect(content.text).toContain(`expected ${remoteChildThreadId}`);
        }
        expect(updatedRemoteChildren).toEqual([]);
        expect(enqueuedParentInjections).toEqual([]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          peerRegistryPeers = [];
          remoteChildRows = [];
          peerHttpRequests.length = 0;
          updatedRemoteChildren.length = 0;
          enqueuedParentInjections.length = 0;
          peerHttpHandler = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("waits for a remote child through the peer check proxy", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        peerRegistryPeers = [bearerPeer()];
        remoteChildRows = [remoteChildRow("running")];
        enqueuedParentInjections.length = 0;
        peerHttpHandler = remoteCheckPeerHandler({
          status: "completed",
          turnCount: 2,
          latestAssistantText: "wait saw remote done",
        });

        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: { childThreadIds: [remoteChildThreadId], timeoutSeconds: 1 },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          results: [
            {
              childThreadId: remoteChildThreadId,
              status: "completed",
              turnCount: 2,
              finalAssistantText: "wait saw remote done",
              error: null,
            },
          ],
          settledCount: 1,
          timedOutCount: 0,
          pending: false,
        });
        expect(enqueuedParentInjections).toHaveLength(1);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          peerRegistryPeers = [];
          remoteChildRows = [];
          enqueuedParentInjections.length = 0;
          peerHttpHandler = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("treats interrupted remote children as settled for wait any", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        const runningChildThreadId = ThreadId.make("thread-remote-child-running");
        peerRegistryPeers = [bearerPeer()];
        remoteChildRows = [
          remoteChildRow("running"),
          {
            ...remoteChildRow("running"),
            childThreadId: runningChildThreadId,
            spawnParams: { prompt: "remote running", directory: "/remote/repo", detached: true },
          },
        ];
        enqueuedParentInjections.length = 0;
        peerHttpHandler = remoteCheckPeerHandlerWith((body) => {
          const requestedThreadId = body.params?.arguments?.childThreadId;
          return requestedThreadId === runningChildThreadId
            ? {
                threadId: runningChildThreadId,
                status: "running",
                turnCount: 1,
                latestAssistantText: null,
              }
            : {
                threadId: remoteChildThreadId,
                status: "interrupted",
                turnCount: 2,
                latestAssistantText: "interrupted final text",
              };
        });

        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: {
              childThreadIds: [remoteChildThreadId, runningChildThreadId],
              timeoutSeconds: 1,
              mode: "any",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          settledCount: 1,
          timedOutCount: 0,
          pending: false,
        });
        const results = (result.structuredContent as { readonly results?: ReadonlyArray<unknown> })
          .results;
        expect(results).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              childThreadId: remoteChildThreadId,
              status: "interrupted",
              turnCount: 2,
              finalAssistantText: "interrupted final text",
            }),
            expect.objectContaining({
              childThreadId: runningChildThreadId,
              status: "pending",
              turnCount: 1,
            }),
          ]),
        );
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          peerRegistryPeers = [];
          remoteChildRows = [];
          enqueuedParentInjections.length = 0;
          peerHttpHandler = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("lists remote children alongside local children", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        peerRegistryPeers = [bearerPeer()];
        remoteChildRows = [remoteChildRow("running")];
        peerHttpHandler = remoteCheckPeerHandler({
          status: "running",
          turnCount: 1,
          latestAssistantText: null,
        });

        const result = yield* server
          .callTool({
            name: "t3_list_subagents",
            arguments: { parentThreadId },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        const children = (
          result.structuredContent as { readonly children?: ReadonlyArray<unknown> }
        ).children;
        expect(children).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              childThreadId: remoteChildThreadId,
              parentThreadId,
              detached: true,
              status: "running",
              turnCount: 1,
            }),
          ]),
        );
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          peerRegistryPeers = [];
          remoteChildRows = [];
          peerHttpHandler = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("peer-scoped receiver spawn requires directory and records remote parent", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        const fileSystem = yield* FileSystem.FileSystem;
        const targetDirectory = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-peer-spawn-target-",
        });
        activeProjectShell = {
          ...parentProject,
          workspaceRoot: targetDirectory,
          repositoryIdentity: {
            canonicalKey: `git-local:${targetDirectory}`,
            locator: {
              source: "git-local",
              rootPath: targetDirectory,
            },
            rootPath: targetDirectory,
          },
        };
        engineCommands.length = 0;
        dispatchedTurnCommands.length = 0;
        registeredChildren.length = 0;
        const sourceEnvironmentId = EnvironmentId.make("environment-source-a");

        const result = yield* server
          .callTool({
            name: "t3_spawn_subagent",
            arguments: {
              prompt: "run on receiver",
              directory: targetDirectory,
              detached: true,
              remoteParentThreadId: parentThreadId,
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, {
              ...unrestrictedPeerInvocation,
              sourceEnvironmentId,
            }),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(dispatchedTurnCommands).toHaveLength(1);
        expect(registeredChildren).toEqual([]);
        const parentSetCommand = engineCommands.find(
          (command): command is Extract<OrchestrationCommand, { type: "thread.parent.set" }> =>
            command.type === "thread.parent.set",
        );
        expect(parentSetCommand).toMatchObject({
          type: "thread.parent.set",
          parentThreadId,
          parentEnvironmentId: sourceEnvironmentId,
        });
        expect(result.structuredContent).toMatchObject({
          parentThreadId,
        });
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          activeProjectShell = parentProject;
          engineCommands.length = 0;
          dispatchedTurnCommands.length = 0;
          registeredChildren.length = 0;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("rejects peer-scoped receiver spawn with a spoofed parent environment id", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        const fileSystem = yield* FileSystem.FileSystem;
        const targetDirectory = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-peer-spawn-target-",
        });
        activeProjectShell = {
          ...parentProject,
          workspaceRoot: targetDirectory,
          repositoryIdentity: {
            canonicalKey: `git-local:${targetDirectory}`,
            locator: {
              source: "git-local",
              rootPath: targetDirectory,
            },
            rootPath: targetDirectory,
          },
        };
        engineCommands.length = 0;
        dispatchedTurnCommands.length = 0;
        const sourceEnvironmentId = EnvironmentId.make("environment-source-a");
        const spoofedEnvironmentId = EnvironmentId.make("environment-spoofed");

        const result = yield* server
          .callTool({
            name: "t3_spawn_subagent",
            arguments: {
              prompt: "run on receiver",
              directory: targetDirectory,
              detached: true,
              remoteParentThreadId: parentThreadId,
              remoteParentEnvironmentId: spoofedEnvironmentId,
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, {
              ...unrestrictedPeerInvocation,
              sourceEnvironmentId,
            }),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(true);
        const content = result.content?.[0];
        expect(content?.type).toBe("text");
        if (content?.type === "text") {
          expect(content.text).toContain("does not match the authenticated caller backend");
        }
        expect(dispatchedTurnCommands).toEqual([]);
        expect(engineCommands).toEqual([]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          activeProjectShell = parentProject;
          engineCommands.length = 0;
          dispatchedTurnCommands.length = 0;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("rejects peer-scoped receiver spawn through symlink outside target project", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        const fileSystem = yield* FileSystem.FileSystem;
        const projectDirectory = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-peer-spawn-project-",
        });
        const outsideDirectory = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-peer-spawn-outside-",
        });
        const linkedDirectory = `${projectDirectory}/linked-outside`;
        yield* fileSystem.symlink(outsideDirectory, linkedDirectory);
        activeProjectShell = {
          ...parentProject,
          workspaceRoot: projectDirectory,
          repositoryIdentity: {
            canonicalKey: `git-local:${projectDirectory}`,
            locator: {
              source: "git-local",
              rootPath: projectDirectory,
            },
            rootPath: projectDirectory,
          },
        };
        engineCommands.length = 0;
        dispatchedTurnCommands.length = 0;

        const result = yield* server
          .callTool({
            name: "t3_spawn_subagent",
            arguments: {
              prompt: "run outside through a symlink",
              directory: linkedDirectory,
              detached: true,
              remoteParentThreadId: parentThreadId,
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, {
              ...unrestrictedPeerInvocation,
              sourceEnvironmentId: EnvironmentId.make("environment-source-a"),
            }),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(true);
        const content = result.content?.[0];
        expect(content?.type).toBe("text");
        if (content?.type === "text") {
          expect(content.text).toContain("not inside an active target project");
        }
        expect(dispatchedTurnCommands).toEqual([]);
        expect(engineCommands).toEqual([]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          activeProjectShell = parentProject;
          engineCommands.length = 0;
          dispatchedTurnCommands.length = 0;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("rejects peer-scoped receiver spawn for unauthorized remote parent ids", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        dispatchedTurnCommands.length = 0;

        const result = yield* server
          .callTool({
            name: "t3_spawn_subagent",
            arguments: {
              prompt: "run on receiver",
              directory: "/not-read-before-authz",
              remoteParentThreadId: parentThreadId,
              remoteParentEnvironmentId: EnvironmentId.make("environment-source-a"),
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, {
              ...peerInvocation,
              sourceEnvironmentId: EnvironmentId.make("environment-source-a"),
            }),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(true);
        const content = result.content?.[0];
        expect(content?.type).toBe("text");
        if (content?.type === "text") {
          expect(content.text).toContain("not authorized for parent thread");
        }
        expect(dispatchedTurnCommands).toEqual([]);
      }),
    ).pipe(
      Effect.ensuring(Effect.sync(() => void (dispatchedTurnCommands.length = 0))),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("allows peer-scoped list when the parent thread is explicit", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;

        const result = yield* server
          .callTool({
            name: "t3_list_subagents",
            arguments: { parentThreadId },
          })
          .pipe(
            Effect.provideService(
              McpInvocationContext.McpInvocationContext,
              entitledPeerInvocation,
            ),
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
            },
          ],
        });
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("allows unrestricted peer-scoped list when the parent thread is explicit", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;

        const result = yield* server
          .callTool({
            name: "t3_list_subagents",
            arguments: { parentThreadId },
          })
          .pipe(
            Effect.provideService(
              McpInvocationContext.McpInvocationContext,
              unrestrictedPeerInvocation,
            ),
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
            },
          ],
        });
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("rejects peer-scoped list without an explicit parent thread", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;

        const result = yield* server
          .callTool({ name: "t3_list_subagents", arguments: {} })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, peerInvocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(true);
        const content = result.content?.[0];
        expect(content?.type).toBe("text");
        if (content?.type !== "text") throw new Error("Expected text error content.");
        expect(content.text).toContain(
          "parentThreadId is required when listing with a peer-scoped credential",
        );
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("rejects peer-scoped list for an unauthorized parent thread", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;

        const result = yield* server
          .callTool({
            name: "t3_list_subagents",
            arguments: { parentThreadId },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, peerInvocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(true);
        const content = result.content?.[0];
        expect(content?.type).toBe("text");
        if (content?.type !== "text") throw new Error("Expected text error content.");
        expect(content.text).toContain(
          `Peer-scoped sub-agent credential is not authorized for parent thread ${parentThreadId}`,
        );
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

  it.effect("allows peer-scoped check for an authorized child thread", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;

        const result = yield* server
          .callTool({
            name: "t3_check_subagent",
            arguments: { childThreadId },
          })
          .pipe(
            Effect.provideService(
              McpInvocationContext.McpInvocationContext,
              entitledPeerInvocation,
            ),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          threadId: childThreadId,
          latestAssistantText: "child done",
        });
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("allows unrestricted peer-scoped check for any child thread", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;

        const result = yield* server
          .callTool({
            name: "t3_check_subagent",
            arguments: { childThreadId },
          })
          .pipe(
            Effect.provideService(
              McpInvocationContext.McpInvocationContext,
              unrestrictedPeerInvocation,
            ),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          threadId: childThreadId,
          latestAssistantText: "child done",
        });
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("rejects peer-scoped check for an unauthorized child thread", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;

        const result = yield* server
          .callTool({
            name: "t3_check_subagent",
            arguments: { childThreadId },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, peerInvocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(true);
        const content = result.content?.[0];
        expect(content?.type).toBe("text");
        if (content?.type !== "text") throw new Error("Expected text error content.");
        expect(content.text).toContain(
          `Peer-scoped sub-agent credential is not authorized for child thread ${childThreadId}`,
        );
        expect(content.text).not.toContain("child done");
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("rejects peer-scoped wait for an unauthorized child thread", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;

        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: { childThreadIds: [childThreadId] },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, peerInvocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(true);
        const content = result.content?.[0];
        expect(content?.type).toBe("text");
        if (content?.type !== "text") throw new Error("Expected text error content.");
        expect(content.text).toContain(
          `Peer-scoped sub-agent credential is not authorized for child thread ${childThreadId}`,
        );
        expect(content.text).not.toContain("child done");
      }),
    ).pipe(Effect.provide(TestLayer)),
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

  it.effect("releases the dispatch lease when cleanup delete succeeds after register failure", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        const limiter = yield* SubagentDispatchLimiter.SubagentDispatchLimiter;
        childDriverKind = "codex";
        failCoordinatorRegister = true;
        engineCommands.length = 0;
        dispatchedTurns.length = 0;

        const result = yield* server
          .callTool({
            name: "t3_spawn_subagent",
            arguments: {
              prompt: "cleanup after failed registration",
              title: "cleanup after failed registration",
              mode: "current_checkout",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(true);
        expect(dispatchedTurns.length).toBe(1);
        expect(engineCommands.map((command) => command.type)).toEqual([
          "thread.parent.set",
          "thread.delete",
        ]);

        const acquired = yield* Deferred.make<SubagentDispatchLimiter.SubagentDispatchLease>();
        const acquireFiber = yield* limiter.acquire.pipe(
          Effect.flatMap((lease) => Deferred.succeed(acquired, lease)),
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        const acquiredLease = yield* Deferred.poll(acquired);
        if (Option.isSome(acquiredLease)) {
          const lease = yield* acquiredLease.value;
          yield* limiter.release(lease);
          yield* Fiber.join(acquireFiber);
        } else {
          yield* Fiber.interrupt(acquireFiber);
        }
        expect(Option.isSome(acquiredLease)).toBe(true);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDriverKind = undefined;
          failCoordinatorRegister = false;
          engineCommands.length = 0;
          dispatchedTurns.length = 0;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect(
    "foreground spawn returns launch metadata after one pending slice instead of blocking",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const server = yield* McpServer.McpServer;
          childDriverKind = "codex";
          promotedCalls.length = 0;
          registeredChildren.length = 0;
          waitSliceCalls.length = 0;
          engineCommands.length = 0;
          waitSliceEffect = (input) =>
            Effect.sync(() => {
              if (waitSliceCalls.length > 1) {
                throw new Error("foreground spawn waited more than one coordinator slice");
              }
              return {
                results: [
                  {
                    childThreadId: input.childThreadIds[0]!,
                    status: "pending",
                    finalAssistantText: null,
                    error: null,
                  },
                ],
                settledCount: 0,
                timedOutCount: 0,
                pending: true,
                resumeToken: "spawn-slice",
              };
            });

          const result = yield* server
            .callTool({
              name: "t3_spawn_subagent",
              arguments: {
                prompt: "Long verification",
                title: "Long verification",
                mode: "current_checkout",
                detached: false,
                waitTimeoutSeconds: 900,
              },
            })
            .pipe(
              Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
              Effect.provideService(McpSchema.McpServerClient, client),
            );

          expect(result.isError).toBe(false);
          const content = result.structuredContent as {
            readonly childThreadId: ThreadId;
            readonly parentThreadId: ThreadId;
            readonly status?: string;
            readonly warning?: string;
            readonly finalAssistantText?: string | null;
          };
          expect(content.parentThreadId).toBe(parentThreadId);
          expect(content.status).toBe("running");
          expect(content.finalAssistantText).toBeNull();
          expect(content.warning).toContain("still running");
          expect(content.warning).toContain("t3_wait_subagent");
          expect(content.warning).not.toContain("t3_check_subagent");
          expect(waitSliceCalls.length).toBe(1);
          expect(registeredChildren).toEqual([
            { childThreadId: content.childThreadId, parentThreadId },
          ]);
          expect(promotedCalls).toEqual([[content.childThreadId]]);
          expect(engineCommands).toEqual([
            expect.objectContaining({
              type: "thread.parent.set",
              threadId: content.childThreadId,
              parentThreadId,
            }),
          ]);
        }),
      ).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            waitSliceEffect = null;
            waitSliceResult = null;
            waitSliceCalls.length = 0;
            registeredChildren.length = 0;
            engineCommands.length = 0;
            promotedCalls.length = 0;
            dispatchedTurns.length = 0;
            childDriverKind = undefined;
          }),
        ),
        Effect.provide(TestLayer),
      ),
  );

  it.effect("cleans up foreground child when promotion persistence fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        const limiter = yield* SubagentDispatchLimiter.SubagentDispatchLimiter;
        childDriverKind = "codex";
        promoteToWakeDefect = new Error("promotion failed");
        promotedCalls.length = 0;
        registeredChildren.length = 0;
        waitSliceCalls.length = 0;
        engineCommands.length = 0;
        dispatchedTurns.length = 0;
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "pending",
              finalAssistantText: null,
              error: null,
            },
          ],
          settledCount: 0,
          timedOutCount: 0,
          pending: true,
          resumeToken: "spawn-slice",
        };

        const result = yield* server
          .callTool({
            name: "t3_spawn_subagent",
            arguments: {
              prompt: "promotion failure cleanup",
              title: "promotion failure cleanup",
              mode: "current_checkout",
              detached: false,
              waitTimeoutSeconds: 900,
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(true);
        expect(promotedCalls).toHaveLength(1);
        const promotedChildId = promotedCalls[0]?.[0];
        expect(promotedChildId).toBeDefined();
        expect(registeredChildren).toEqual([{ childThreadId: promotedChildId, parentThreadId }]);
        expect(engineCommands.map((command) => command.type)).toEqual([
          "thread.parent.set",
          "thread.delete",
        ]);
        expect(engineCommands[0]).toMatchObject({
          type: "thread.parent.set",
          threadId: promotedChildId,
          parentThreadId,
        });
        expect(engineCommands[1]).toMatchObject({
          type: "thread.delete",
          threadId: promotedChildId,
        });

        const acquired = yield* Deferred.make<SubagentDispatchLimiter.SubagentDispatchLease>();
        const acquireFiber = yield* limiter.acquire.pipe(
          Effect.flatMap((lease) => Deferred.succeed(acquired, lease)),
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        const acquiredLease = yield* Deferred.poll(acquired);
        if (Option.isSome(acquiredLease)) {
          const lease = yield* acquiredLease.value;
          yield* limiter.release(lease);
          yield* Fiber.join(acquireFiber);
        } else {
          yield* Fiber.interrupt(acquireFiber);
        }
        expect(Option.isSome(acquiredLease)).toBe(true);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDriverKind = undefined;
          promoteToWakeDefect = null;
          waitSliceResult = null;
          waitSliceCalls.length = 0;
          registeredChildren.length = 0;
          engineCommands.length = 0;
          promotedCalls.length = 0;
          dispatchedTurns.length = 0;
        }),
      ),
      Effect.provide(TestLayer),
    ),
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

  it.effect("R-A: wait accepts sessionless projection-terminal children", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        promotedCalls.length = 0;
        markWaitDeliveredCalls.length = 0;
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
        expect(markWaitDeliveredCalls).toEqual([
          [
            {
              childThreadId,
              status: "completed",
              finalAssistantText: "child done",
              error: null,
            },
          ],
        ]);
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("R-A: wait rejects stale sessionless projection-terminal children", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        promotedCalls.length = 0;
        markWaitDeliveredCalls.length = 0;
        childDetailTurnState = "completed";
        childDetailMessages = [
          {
            id: "msg-1" as never,
            role: "assistant",
            text: "old child result",
            turnId: "turn-1" as never,
            streaming: false,
            createdAt: "2026-06-17T10:01:00.000Z",
            updatedAt: "2026-06-17T10:01:00.000Z",
          },
          {
            id: "msg-2" as never,
            role: "user",
            text: "newer queued child work",
            turnId: "turn-2" as never,
            streaming: false,
            createdAt: "2026-06-17T10:02:00.000Z",
            updatedAt: "2026-06-17T10:02:00.000Z",
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
          promoted: true,
          pending: false,
          settledCount: 0,
          timedOutCount: 0,
          results: [{ childThreadId, status: "running", error: null }],
        });
        expect(promotedCalls).toEqual([[childThreadId]]);
        expect(markWaitDeliveredCalls).toEqual([]);
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

  it.effect("R-A: child-only peer waits do not mark the parent wake delivered", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        markWaitDeliveredCalls.length = 0;
        abandonWaitDeliveryCalls.length = 0;
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "completed",
              finalAssistantText: "peer visible result",
              error: null,
            },
          ],
          settledCount: 1,
          timedOutCount: 0,
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
            Effect.provideService(
              McpInvocationContext.McpInvocationContext,
              childOnlyPeerInvocation,
            ),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          pending: false,
          settledCount: 1,
          results: [{ childThreadId, status: "completed", error: null }],
        });
        expect(markWaitDeliveredCalls).toEqual([]);
        expect(abandonWaitDeliveryCalls).toEqual([[childThreadId]]);
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("R-A: parent-authorized peer waits mark the parent wake delivered", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        assertParentCalls.length = 0;
        markWaitDeliveredCalls.length = 0;
        abandonWaitDeliveryCalls.length = 0;
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "completed",
              finalAssistantText: "parent-authorized peer result",
              error: null,
            },
          ],
          settledCount: 1,
          timedOutCount: 0,
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
            Effect.provideService(
              McpInvocationContext.McpInvocationContext,
              entitledPeerInvocation,
            ),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          pending: false,
          settledCount: 1,
          results: [{ childThreadId, status: "completed", error: null }],
        });
        expect(assertParentCalls).toEqual([{ parentThreadId, childThreadId }]);
        expect(markWaitDeliveredCalls).toEqual([[waitSliceResult.results[0]!]]);
        expect(abandonWaitDeliveryCalls).toEqual([]);
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("R-A: mixed-authority peer waits mark only parent-authorized rows", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        const otherChildThreadId = ThreadId.make("thread-subagent-other-parent-child");
        const mixedPeerInvocation: McpInvocationContext.PeerMcpInvocationScope = {
          ...peerInvocation,
          peerTokenId: "peer-subagent-mixed-authority-test",
          allowedParentThreadIds: new Set([parentThreadId]),
          allowedChildThreadIds: new Set([childThreadId, otherChildThreadId]),
        };
        assertParentCalls.length = 0;
        markWaitDeliveredCalls.length = 0;
        abandonWaitDeliveryCalls.length = 0;
        assertParentFailureChild = otherChildThreadId;
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "completed",
              finalAssistantText: "authorized parent result",
              error: null,
            },
            {
              childThreadId: otherChildThreadId,
              status: "completed",
              finalAssistantText: "child-only result",
              error: null,
            },
          ],
          settledCount: 2,
          timedOutCount: 0,
          pending: false,
          resumeToken: "coordinator-token",
        };

        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: {
              childThreadIds: [childThreadId, otherChildThreadId],
              resumeToken: "-100000:coordinator-token",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, mixedPeerInvocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(assertParentCalls.map((call) => call.childThreadId).sort()).toEqual(
          [childThreadId, otherChildThreadId].sort(),
        );
        expect(markWaitDeliveredCalls).toEqual([[waitSliceResult.results[0]!]]);
        expect(abandonWaitDeliveryCalls).toEqual([[otherChildThreadId]]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          assertParentFailureChild = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("R-A: wait accepts current ready/idle projection-terminal children", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        promotedCalls.length = 0;
        markWaitDeliveredCalls.length = 0;
        childDetailTurnState = "completed";
        childDetailSession = {
          threadId: childThreadId,
          status: "ready",
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
        expect(markWaitDeliveredCalls).toEqual([
          [
            {
              childThreadId,
              status: "completed",
              finalAssistantText: "child done",
              error: null,
            },
          ],
        ]);
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

  it.effect("R-A: wait does not mark delivered when final response enrichment fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        markWaitDeliveredCalls.length = 0;
        abandonWaitDeliveryCalls.length = 0;
        childDetailCallCount = 0;
        childDetailFailOnCall = 1;
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "completed",
              finalAssistantText: "coordinator result",
              error: null,
            },
          ],
          settledCount: 1,
          timedOutCount: 0,
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

        expect(result.isError).toBe(true);
        expect(markWaitDeliveredCalls).toEqual([]);
        expect(abandonWaitDeliveryCalls).toEqual([[childThreadId]]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailFailOnCall = null;
          childDetailCallCount = 0;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("R-A: wait abandons coordinator terminals when earlier enrichment fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        const terminalChildId = ThreadId.make("thread-subagent-terminal-before-enrichment");
        markWaitDeliveredCalls.length = 0;
        abandonWaitDeliveryCalls.length = 0;
        childDetailCallCount = 0;
        childDetailFailOnCall = 1;
        waitSliceResult = {
          results: [
            {
              childThreadId: terminalChildId,
              status: "completed",
              finalAssistantText: "coordinator terminal",
              error: null,
            },
            {
              childThreadId,
              status: "pending",
              finalAssistantText: null,
              error: null,
            },
          ],
          settledCount: 1,
          timedOutCount: 0,
          pending: true,
          resumeToken: "coordinator-token",
        };

        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: {
              childThreadIds: [terminalChildId, childThreadId],
              resumeToken: "-100000:coordinator-token",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(true);
        expect(markWaitDeliveredCalls).toEqual([]);
        expect(abandonWaitDeliveryCalls).toEqual([[terminalChildId]]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailFailOnCall = null;
          childDetailCallCount = 0;
        }),
      ),
      Effect.provide(TestLayer),
    ),
  );

  it.effect("R-A: wait dedupes duplicate child ids before coordinator wait", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        assertParentCalls.length = 0;
        waitSliceCalls.length = 0;
        markWaitDeliveredCalls.length = 0;
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "completed",
              finalAssistantText: "deduped result",
              error: null,
            },
          ],
          settledCount: 1,
          timedOutCount: 0,
          pending: false,
          resumeToken: "coordinator-token",
        };

        const result = yield* server
          .callTool({
            name: "t3_wait_subagent",
            arguments: {
              childThreadIds: [childThreadId, childThreadId],
              resumeToken: "-100000:coordinator-token",
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, client),
          );

        expect(result.isError).toBe(false);
        expect(assertParentCalls).toEqual([{ parentThreadId, childThreadId }]);
        expect(waitSliceCalls.map((call) => call.childThreadIds)).toEqual([[childThreadId]]);
        expect(markWaitDeliveredCalls).toEqual([[waitSliceResult.results[0]!]]);
      }),
    ).pipe(Effect.provide(TestLayer)),
  );

  it.effect("R-A: wait refuses to deliver a child owned by another parent", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        assertParentCalls.length = 0;
        waitSliceCalls.length = 0;
        markWaitDeliveredCalls.length = 0;
        assertParentFailureChild = childThreadId;
        waitSliceResult = {
          results: [
            {
              childThreadId,
              status: "completed",
              finalAssistantText: "wrong parent result",
              error: null,
            },
          ],
          settledCount: 1,
          timedOutCount: 0,
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

        expect(result.isError).toBe(true);
        expect(assertParentCalls).toEqual([{ parentThreadId, childThreadId }]);
        expect(waitSliceCalls).toEqual([]);
        expect(markWaitDeliveredCalls).toEqual([]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          assertParentFailureChild = null;
        }),
      ),
      Effect.provide(TestLayer),
    ),
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
        markWaitDeliveredCalls.length = 0;
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
        expect(markWaitDeliveredCalls).toEqual([
          [
            {
              childThreadId,
              status: "completed",
              finalAssistantText: "child done",
              error: null,
            },
          ],
        ]);
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
        markWaitDeliveredCalls.length = 0;
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
        expect(markWaitDeliveredCalls).toEqual([
          [
            {
              childThreadId,
              status: "failed",
              finalAssistantText: "child done",
              error: "Child thread ended with status failed.",
            },
          ],
        ]);
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

  it.effect("returns failed projection waits when the child session is interrupted", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        promotedCalls.length = 0;
        markWaitDeliveredCalls.length = 0;
        childDetailTurnState = "interrupted";
        childDetailSession = {
          threadId: childThreadId,
          status: "interrupted",
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
        expect(markWaitDeliveredCalls).toEqual([
          [
            {
              childThreadId,
              status: "failed",
              finalAssistantText: "child done",
              error: "Child thread ended with status failed.",
            },
          ],
        ]);
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          childDetailSession = null;
          childDetailTurnState = "completed";
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
          childDetailSession = null;
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
        childDetailSession = {
          threadId: childThreadId,
          status: "error",
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: "provider failed",
          updatedAt: "2026-06-17T10:02:00.000Z",
        };
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
          childDetailSession = null;
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
