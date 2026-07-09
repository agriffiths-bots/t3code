/**
 * Sub-agent + scheduler MCP toolkit handlers (finalPlan §6/§7).
 *
 * Mirrors the thread toolkit: a module-level active runtime (captured by
 * `SubagentRuntimeLive`, cloned from `ThreadStartRuntimeLive`) holds the live
 * services so the toolkit handlers can reach them without threading them
 * through the toolkit `Context`. The handlers reuse pr3107's
 * `activeThreadStartRuntime` for spawning, the live `ChildThreadCoordinator`
 * for the never-hang wait/registration, and `dispatchActive` for steering.
 *
 * @module subagent/handlers
 */
import {
  CommandId,
  EnvironmentId,
  ExecutionEnvironmentDescriptor,
  isProviderAvailable,
  IsoDateTime,
  MessageId,
  NonNegativeInt,
  ThreadId,
  type ModelSelection,
  type OrchestrationThread,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import {
  buildProviderOptionSelectionsFromDescriptors,
  pickModelSelectionFromInstances,
} from "@t3tools/shared/model";
import { Cron } from "croner";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import {
  coordinatorActive,
  finalAssistantTextFromThread,
} from "../../../orchestration/Layers/ChildThreadCoordinator.ts";
import type {
  ChildTerminalStatus,
  ChildThreadCoordinatorShape,
  WaitSliceResult,
} from "../../../orchestration/Services/ChildThreadCoordinator.ts";
import { WAIT_SLICE_SECONDS } from "../../../orchestration/Services/ChildThreadCoordinator.ts";
import { dispatchActive } from "../../../orchestration/Services/BootstrapTurnStartDispatcher.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import {
  ScheduledTaskRepository,
  toScheduleEntry,
  type ScheduledTask,
  type ScheduledTaskId,
  type ScheduledTaskRepositoryShape,
} from "../../../persistence/Services/ScheduledTasks.ts";
import {
  PendingDispatchRepository,
  type PendingDispatch,
  type PendingDispatchId,
  type PendingDispatchRepositoryShape,
} from "../../../persistence/Services/PendingDispatches.ts";
import {
  RemoteChildRepository,
  type RemoteChild,
  type RemoteChildRepositoryShape,
  type RemoteChildStatus,
} from "../../../persistence/Services/RemoteChildren.ts";
import { ProviderInstanceRegistry } from "../../../provider/Services/ProviderInstanceRegistry.ts";
import * as McpPeerClient from "../../../subagents/McpPeerClient.ts";
import * as SubagentPeerRegistry from "../../../subagents/SubagentPeerRegistry.ts";
import { cloudflareAccessHeaders, environmentUrl } from "../../../subagents/SubagentPeerHttp.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { activeThreadStartRuntimeOf, type ActiveThreadStartRuntime } from "../thread/handlers.ts";
import { ThreadStartToolError } from "../thread/tools.ts";
import { SubagentDispatchLimiter } from "./SubagentDispatchLimiter.ts";
import {
  SubagentToolkit,
  WAIT_AUTO_PROMOTE_SECONDS,
  WAIT_TIMEOUT_DEFAULT_SECONDS,
  WAIT_TIMEOUT_MAX_SECONDS,
  WAIT_TIMEOUT_MIN_SECONDS,
  type CheckSubagentInput,
  CheckSubagentOutput,
  type CheckSubagentOutput as CheckSubagentOutputType,
  type ListSubagentsInput,
  type ScheduleCreateInput,
  type ScheduleDeleteInput,
  type ScheduleListInput,
  type ScheduleUpdateInput,
  type SpawnSubagentInput,
  SpawnSubagentOutput,
  type SpawnSubagentOutput as SpawnSubagentOutputType,
  type SteerSubagentInput,
  type WaitSubagentInput,
  type WaitSubagentOutput as WaitSubagentOutputType,
} from "./tools.ts";

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
const isThreadStartToolError = Schema.is(ThreadStartToolError);
const isSubagentPeerTargetNotFoundError = Schema.is(
  SubagentPeerRegistry.SubagentPeerTargetNotFoundError,
);
const WAIT_PROJECTION_ENRICHMENT_TIMEOUT_MS = 250;
const PEER_SPAWN_PROBE_TIMEOUT = Duration.seconds(5);
const PEER_TOOL_CALL_TIMEOUT_SECONDS = 5;
const PEER_TOOL_CALL_TIMEOUT = Duration.seconds(PEER_TOOL_CALL_TIMEOUT_SECONDS);
const PEER_SESSION_CLOSE_TIMEOUT = Duration.seconds(2);
const LOCAL_SPAWN_DISPATCH_LEASE_TIMEOUT_SECONDS = 5;
const LOCAL_SPAWN_DISPATCH_LEASE_TIMEOUT = Duration.seconds(
  LOCAL_SPAWN_DISPATCH_LEASE_TIMEOUT_SECONDS,
);
const REMOTE_TERMINAL_DELIVERY_CLAIM_TTL_MS = 5 * 60 * 1_000;
const UNTRACKED_PROJECTION_CHILD_ERROR =
  "Sub-agent thread exists in the projection but is not tracked by this server instance.";
const FOREGROUND_SPAWN_PENDING_WARNING =
  "Sub-agent is still running after the initial foreground wait; returning launch metadata now. Use t3_wait_subagent to wait for terminal delivery, or stop polling and let the parent wake automatically when it completes.";

const fail = (message: string) => new ThreadStartToolError({ message });
const decodeSpawnSubagentOutput = Schema.decodeUnknownEffect(SpawnSubagentOutput);
const decodeCheckSubagentOutput = Schema.decodeUnknownEffect(CheckSubagentOutput);

const toToolError = (error: unknown, fallback: string): ThreadStartToolError =>
  isThreadStartToolError(error) ? error : fail(error instanceof Error ? error.message : fallback);

const requireCoordinator = (): Effect.Effect<ChildThreadCoordinatorShape, ThreadStartToolError> => {
  const coordinator = coordinatorActive();
  return coordinator
    ? Effect.succeed(coordinator)
    : Effect.fail(fail("Sub-agent coordinator is not available."));
};

const requireSpawnRuntime = (): Effect.Effect<ActiveThreadStartRuntime, ThreadStartToolError> => {
  const runtime = activeThreadStartRuntimeOf();
  return runtime
    ? Effect.succeed(runtime)
    : Effect.fail(fail("Thread start runtime is not available."));
};

const clamp = (value: number, min: number, max: number): number =>
  value < min ? min : value > max ? max : value;

const appendWarning = (existing: string | undefined, warning: string): string =>
  existing === undefined ? warning : `${existing} ${warning}`;

const acquireLocalDispatchLease = Effect.fn("SubagentToolkit.acquireLocalDispatchLease")(function* (
  runtime: SubagentRuntime,
  parentThreadId: ThreadId,
) {
  const lease = yield* runtime.dispatchLimiter.acquire.pipe(
    Effect.timeoutOption(LOCAL_SPAWN_DISPATCH_LEASE_TIMEOUT),
  );
  if (Option.isSome(lease)) return lease.value;

  yield* Effect.logWarning("sub-agent spawn dispatch capacity timed out", {
    parentThreadId,
    maxConcurrentDispatches: runtime.dispatchLimiter.maxConcurrentDispatches,
    timeoutSeconds: LOCAL_SPAWN_DISPATCH_LEASE_TIMEOUT_SECONDS,
  });
  return yield* fail(
    `Sub-agent dispatch capacity is saturated after ${LOCAL_SPAWN_DISPATCH_LEASE_TIMEOUT_SECONDS}s (limit ${runtime.dispatchLimiter.maxConcurrentDispatches}); no child was created. Try again after running sub-agents finish.`,
  );
});

/**
 * Derive a turn count from a thread detail: the highest checkpoint turn count,
 * or 0 when the thread has not completed a turn yet.
 */
const turnCountOf = (thread: OrchestrationThread): number => {
  let checkpointCount = 0;
  const checkpointTurnIds = new Set<string>();
  let latestCheckpointCompletedAt: string | null = null;
  for (const checkpoint of thread.checkpoints) {
    checkpointTurnIds.add(checkpoint.turnId);
    if (
      checkpoint.checkpointTurnCount > checkpointCount ||
      (checkpoint.checkpointTurnCount === checkpointCount &&
        (latestCheckpointCompletedAt === null ||
          checkpoint.completedAt > latestCheckpointCompletedAt))
    ) {
      checkpointCount = checkpoint.checkpointTurnCount;
      latestCheckpointCompletedAt = checkpoint.completedAt;
    }
  }
  const isAfterLatestCheckpoint = (timestamp: string | null): boolean =>
    timestamp !== null &&
    (latestCheckpointCompletedAt === null || timestamp > latestCheckpointCompletedAt);
  const runningTurnId =
    thread.latestTurn !== null && thread.latestTurn.state === "running"
      ? thread.latestTurn.turnId
      : null;
  const projectedTurnIds = new Set<string>();
  for (const message of thread.messages) {
    if (
      message.turnId !== null &&
      !message.streaming &&
      message.turnId !== runningTurnId &&
      !checkpointTurnIds.has(message.turnId) &&
      isAfterLatestCheckpoint(message.createdAt)
    ) {
      projectedTurnIds.add(message.turnId);
    }
  }
  if (
    thread.latestTurn !== null &&
    thread.latestTurn.state !== "running" &&
    !checkpointTurnIds.has(thread.latestTurn.turnId) &&
    isAfterLatestCheckpoint(
      thread.latestTurn.completedAt ?? thread.latestTurn.startedAt ?? thread.latestTurn.requestedAt,
    )
  ) {
    projectedTurnIds.add(thread.latestTurn.turnId);
  }
  return checkpointCount + projectedTurnIds.size;
};

/** Latest assistant message text on a thread, or null. */
const latestAssistantTextOf = (thread: OrchestrationThread): string | null => {
  let chosen: string | null = null;
  for (const message of thread.messages) {
    if (message.role === "assistant") chosen = message.text;
  }
  return chosen;
};

/**
 * Map a thread shell's turn/session state to a coarse readonly status used by
 * `t3_check_subagent` / `t3_list_subagents` (matches the coordinator's terminal
 * vocabulary where it overlaps).
 */
const statusOf = (
  thread: Pick<OrchestrationThread, "latestTurn" | "messages" | "session">,
): string => {
  if (thread.latestTurn?.state === "running") return "running";
  const session = thread.session;
  if (session?.status === "error") {
    return "failed";
  }
  if (hasCurrentProjectedTerminalTurn(thread)) {
    if (session?.status === "stopped" && thread.latestTurn?.state !== "completed") {
      return "failed";
    }
    switch (thread.latestTurn?.state) {
      case "completed":
        return "completed";
      case "error":
        return "failed";
      case "interrupted":
        return "interrupted";
    }
  }
  if (session?.status === "stopped") return "failed";
  switch (thread.latestTurn?.state) {
    case "error":
      return "failed";
    case "interrupted":
      return "interrupted";
    default:
      return session === null ? "idle" : "running";
  }
};

const hasCurrentProjectedTerminalTurn = (
  thread: Pick<OrchestrationThread, "latestTurn" | "messages" | "session">,
): boolean => {
  const latestTurn = thread.latestTurn;
  if (latestTurn === null || latestTurn.state === "running") return false;
  const session = thread.session;
  if (session?.activeTurnId != null) return latestTurn.turnId === session.activeTurnId;
  if (session?.status === "stopped") {
    const terminalAt = latestTurn.completedAt ?? latestTurn.startedAt ?? latestTurn.requestedAt;
    return !thread.messages.some(
      (message) =>
        !message.streaming &&
        message.turnId !== latestTurn.turnId &&
        message.createdAt > terminalAt,
    );
  }
  if (session?.status === "error") {
    return latestTurn.state !== "completed";
  }
  return (
    session === null ||
    session.status === "idle" ||
    session.status === "ready" ||
    session.status === "interrupted"
  );
};

const waitTerminalStatusOf = (
  thread: Pick<OrchestrationThread, "latestTurn" | "messages" | "session">,
): "completed" | "failed" | null => {
  if (thread.session?.status === "error") return "failed";
  if (!hasCurrentProjectedTerminalTurn(thread)) return null;
  const state = thread.latestTurn?.state;
  if (state === "completed") return "completed";
  if (state === undefined || state === "running") return null;
  return "failed";
};

const hasNoNewerMessageAfterTerminalTurn = (
  thread: Pick<OrchestrationThread, "latestTurn" | "messages">,
): boolean => {
  const latestTurn = thread.latestTurn;
  if (latestTurn === null) return false;
  const terminalAt = latestTurn.completedAt ?? latestTurn.startedAt ?? latestTurn.requestedAt;
  return !thread.messages.some(
    (message) =>
      !message.streaming && message.turnId !== latestTurn.turnId && message.createdAt > terminalAt,
  );
};

const reliableWaitTerminalStatusOf = (
  thread: Pick<OrchestrationThread, "latestTurn" | "messages" | "session">,
): "completed" | "failed" | null => {
  const projectedStatus = waitTerminalStatusOf(thread);
  if (projectedStatus === null) return null;
  const session = thread.session;
  if (session?.status === "error") return projectedStatus;
  if (session === null) {
    return hasNoNewerMessageAfterTerminalTurn(thread) ? projectedStatus : null;
  }
  if (
    session?.status === "stopped" ||
    session?.status === "idle" ||
    session?.status === "ready" ||
    session?.status === "interrupted"
  ) {
    return hasNoNewerMessageAfterTerminalTurn(thread) ? projectedStatus : null;
  }
  if (session?.activeTurnId != null && thread.latestTurn?.turnId === session.activeTurnId) {
    return projectedStatus;
  }
  return null;
};

const isWaitTerminal = (status: string): boolean =>
  status === "completed" || status === "failed" || status === "interrupted" || status === "killed";

const pendingForMode = (
  rows: ReadonlyArray<{ readonly status: string }>,
  mode: "all" | "any",
): boolean => {
  const settledCount = rows.filter((row) => isWaitTerminal(row.status)).length;
  const pendingCount = rows.filter((row) => row.status === "pending").length;
  return mode === "all" ? pendingCount > 0 : settledCount === 0 && pendingCount > 0;
};

interface SubagentRuntime {
  readonly crypto: Crypto.Crypto;
  readonly orchestrationEngine: typeof OrchestrationEngineService.Service;
  readonly projectionSnapshotQuery: typeof ProjectionSnapshotQuery.Service;
  readonly providerInstanceRegistry: typeof ProviderInstanceRegistry.Service;
  readonly scheduledTasks: ScheduledTaskRepositoryShape;
  readonly pendingDispatches: PendingDispatchRepositoryShape;
  readonly remoteChildren: RemoteChildRepositoryShape;
  readonly peerRegistry: typeof SubagentPeerRegistry.SubagentPeerRegistry.Service;
  readonly httpClient: HttpClient.HttpClient;
  readonly dispatchLimiter: typeof SubagentDispatchLimiter.Service;
}

let activeRuntime: SubagentRuntime | null = null;

const requireRuntime = (): Effect.Effect<SubagentRuntime, ThreadStartToolError> =>
  activeRuntime
    ? Effect.succeed(activeRuntime)
    : Effect.fail(fail("Sub-agent runtime is not available."));

const requireProviderInvocation = McpInvocationContext.requireProviderMcpCapability(
  "thread-management",
).pipe(Effect.mapError((error) => fail(error.message)));

const requireSubagentCapability = (capability: McpInvocationContext.McpCapability) =>
  McpInvocationContext.requireAnyMcpCapability(["thread-management", capability]).pipe(
    Effect.mapError((error) => fail(error.message)),
  );

const peerSetAllows = (set: ReadonlySet<ThreadId> | undefined, threadId: ThreadId): boolean =>
  set === undefined || set.has(threadId);

const requirePeerParentAccess = (
  invocation: McpInvocationContext.McpInvocationScope,
  parentThreadId: ThreadId,
): Effect.Effect<void, ThreadStartToolError> =>
  McpInvocationContext.isProviderInvocationScope(invocation) ||
  peerSetAllows(invocation.allowedParentThreadIds, parentThreadId)
    ? Effect.void
    : Effect.fail(
        fail(
          `Peer-scoped sub-agent credential is not authorized for parent thread ${parentThreadId}.`,
        ),
      );

const markableWaitDeliveredRows = (
  invocation: McpInvocationContext.McpInvocationScope,
  coordinator: ChildThreadCoordinatorShape,
  rows: ReadonlyArray<WaitSliceResult["results"][number]>,
): Effect.Effect<ReadonlyArray<WaitSliceResult["results"][number]>> => {
  if (McpInvocationContext.isProviderInvocationScope(invocation)) return Effect.succeed(rows);
  const allowedParents = [...(invocation.allowedParentThreadIds ?? [])];
  if (allowedParents.length === 0) return Effect.succeed([]);
  return Effect.forEach(
    rows,
    (row) =>
      Effect.forEach(
        allowedParents,
        (parentThreadId) =>
          coordinator.assertParent(parentThreadId, row.childThreadId).pipe(
            Effect.as(true),
            Effect.orElseSucceed(() => false),
          ),
        { concurrency: "unbounded" },
      ).pipe(Effect.map((matches) => ({ row, markable: matches.some(Boolean) }))),
    { concurrency: "unbounded" },
  ).pipe(
    Effect.map((results) =>
      results.filter((result) => result.markable).map((result) => result.row),
    ),
  );
};

const loadThreadShell = (runtime: SubagentRuntime, threadId: ThreadId) =>
  runtime.projectionSnapshotQuery
    .getThreadShellById(threadId)
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to load thread.")));

const requirePeerChildAccess = (
  runtime: SubagentRuntime,
  invocation: McpInvocationContext.McpInvocationScope,
  childThreadIds: ReadonlyArray<ThreadId>,
): Effect.Effect<void, ThreadStartToolError> => {
  if (McpInvocationContext.isProviderInvocationScope(invocation)) return Effect.void;
  const explicitAllowedChildren = invocation.allowedChildThreadIds;
  if (explicitAllowedChildren !== undefined) {
    const unauthorized = childThreadIds.find(
      (childThreadId) => !explicitAllowedChildren.has(childThreadId),
    );
    return unauthorized === undefined
      ? Effect.void
      : Effect.fail(
          fail(
            `Peer-scoped sub-agent credential is not authorized for child thread ${unauthorized}.`,
          ),
        );
  }
  const sourceEnvironmentId = invocation.sourceEnvironmentId;
  if (sourceEnvironmentId === undefined) {
    return Effect.fail(
      fail("Peer-scoped sub-agent credential is not authorized for child thread reads."),
    );
  }
  return Effect.forEach(
    childThreadIds,
    (childThreadId) =>
      loadThreadShell(runtime, childThreadId).pipe(
        Effect.flatMap((thread) =>
          Option.match(thread, {
            onNone: () =>
              Effect.fail(
                fail(
                  `Peer-scoped sub-agent credential is not authorized for child thread ${childThreadId}.`,
                ),
              ),
            onSome: (shell) =>
              shell.parentEnvironmentId === sourceEnvironmentId
                ? Effect.void
                : Effect.fail(
                    fail(
                      `Peer-scoped sub-agent credential is not authorized for child thread ${childThreadId}.`,
                    ),
                  ),
          }),
        ),
      ),
    { discard: true, concurrency: "unbounded" },
  );
};

const loadThreadDetail = (runtime: SubagentRuntime, threadId: ThreadId) =>
  runtime.projectionSnapshotQuery
    .getThreadDetailById(threadId)
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to load thread detail.")));

/**
 * Snapshot every USABLE provider instance's live model list into the shape
 * `pickModelSelectionFromInstances` consumes (routing id + driver kind + the
 * models it currently serves, each with default options). This is the same
 * upstream-maintained registry the model picker reads, so newly-shipped models
 * are matched without any code change here. Instances are filtered to those the
 * picker would expose: enabled, available, and not in an error state — an errored
 * or unavailable snapshot can still carry built-in models the runtime refuses to
 * run, so resolving against them would pin a schedule that only ever fails.
 */
const buildModelSources = (runtime: SubagentRuntime) =>
  runtime.providerInstanceRegistry.listInstances.pipe(
    Effect.flatMap((providerInstances) =>
      Effect.forEach(
        providerInstances.filter((providerInstance) => providerInstance.enabled),
        (providerInstance) =>
          Effect.map(providerInstance.snapshot.getSnapshot, (snapshot) => ({
            providerInstance,
            snapshot,
          })),
      ),
    ),
    Effect.map((pairs) =>
      pairs
        .filter(({ snapshot }) => isProviderAvailable(snapshot) && snapshot.status !== "error")
        .map(({ providerInstance, snapshot }) => ({
          instanceId: providerInstance.instanceId,
          driverKind: providerInstance.driverKind,
          models: snapshot.models.map((providerModel) => ({
            slug: providerModel.slug,
            optionDescriptors: providerModel.capabilities?.optionDescriptors,
            defaultOptions: buildProviderOptionSelectionsFromDescriptors(
              providerModel.capabilities?.optionDescriptors,
            ),
          })),
        })),
    ),
  );

/**
 * Resolve an explicit plain model name to a `ModelSelection` against the live
 * provider lists, preferring `preferInstanceId` (usually the target thread's
 * instance) on ties. Fails loudly when no configured provider serves the model
 * rather than silently falling back, so a schedule/sub-agent never runs on a
 * different model than the caller named.
 */
const resolveExplicitModelSelection = (
  runtime: SubagentRuntime,
  model: string,
  preferInstanceId: ProviderInstanceId | undefined,
): Effect.Effect<ModelSelection, ThreadStartToolError> =>
  buildModelSources(runtime).pipe(
    Effect.flatMap((modelSources) => {
      const resolved = pickModelSelectionFromInstances(model, modelSources, preferInstanceId);
      return resolved === null
        ? Effect.fail(
            fail(
              `Model "${model}" is not served by any configured provider. Pass a model shown in the model picker, or omit "model" to keep the thread's current model.`,
            ),
          )
        : Effect.succeed(resolved);
    }),
  );

const probePeerDescriptor = Effect.fn("SubagentToolkit.probePeerDescriptor")(function* (
  httpClient: HttpClient.HttpClient,
  peer: SubagentPeerRegistry.SubagentPeer,
) {
  const request = HttpClientRequest.get(
    environmentUrl(peer.httpBaseUrl, "/.well-known/t3/environment"),
  ).pipe(HttpClientRequest.setHeaders(cloudflareAccessHeaders(peer.cfAccess)));
  const response = yield* httpClient.execute(request).pipe(
    Effect.flatMap(HttpClientResponse.filterStatusOk),
    Effect.flatMap(HttpClientResponse.schemaBodyJson(ExecutionEnvironmentDescriptor)),
    Effect.timeout(PEER_SPAWN_PROBE_TIMEOUT),
    Effect.mapError(() =>
      fail(
        `Sub-agent target '${peer.alias}' is offline or did not answer its environment descriptor${peer.lastSeenAt ? ` (last seen ${peer.lastSeenAt})` : ""}.`,
      ),
    ),
  );
  if (response.environmentId !== peer.environmentId) {
    return yield* fail(
      `Sub-agent target '${peer.alias}' resolved to environment ${response.environmentId}, expected ${peer.environmentId}.`,
    );
  }
  return response;
});

const textContentOfToolResult = (result: {
  readonly content?: ReadonlyArray<unknown> | undefined;
}): string => {
  const text = result.content
    ?.map((item) =>
      typeof item === "object" &&
      item !== null &&
      "type" in item &&
      item.type === "text" &&
      "text" in item &&
      typeof item.text === "string"
        ? item.text
        : "",
    )
    .filter((value) => value.length > 0)
    .join("\n");
  return text && text.length > 0 ? text : "Remote sub-agent spawn failed.";
};

const decodeRemoteSpawnResult = (result: {
  readonly isError?: boolean | undefined;
  readonly structuredContent?: unknown;
  readonly content?: ReadonlyArray<unknown> | undefined;
}): Effect.Effect<SpawnSubagentOutputType, ThreadStartToolError> => {
  if (result.isError === true) {
    return Effect.fail(fail(textContentOfToolResult(result)));
  }
  return decodeSpawnSubagentOutput(result.structuredContent).pipe(
    Effect.mapError((error) =>
      fail(
        error instanceof Error
          ? error.message
          : "Remote sub-agent spawn returned an invalid response.",
      ),
    ),
  );
};

const decodeRemoteCheckResult = (result: {
  readonly isError?: boolean | undefined;
  readonly structuredContent?: unknown;
  readonly content?: ReadonlyArray<unknown> | undefined;
}): Effect.Effect<CheckSubagentOutputType, ThreadStartToolError> => {
  if (result.isError === true) {
    return Effect.fail(fail(textContentOfToolResult(result)));
  }
  return decodeCheckSubagentOutput(result.structuredContent).pipe(
    Effect.mapError((error) =>
      fail(
        error instanceof Error
          ? error.message
          : "Remote sub-agent check returned an invalid response.",
      ),
    ),
  );
};

const isRemoteTerminalStatus = (status: string): boolean =>
  status === "completed" || status === "failed" || status === "killed" || status === "interrupted";

const remoteChildStatusFromToolStatus = (status: string): RemoteChildStatus => {
  switch (status) {
    case "completed":
    case "failed":
    case "interrupted":
    case "killed":
      return status;
    case "running":
    case "pending":
    case "timeout":
    case "idle":
      return "running";
    default:
      return "unknown";
  }
};

const childTerminalStatusFromRemoteStatus = (status: string): ChildTerminalStatus =>
  status === "completed" ? "completed" : status === "killed" ? "killed" : "failed";

const remoteTerminalWakeDedupeKey = (child: RemoteChild): string =>
  `remote-subagent-wake:${child.parentThreadId}:${child.childEnvironmentId}:${child.childThreadId}`;

const resolveRemoteChildPeer = (runtime: SubagentRuntime, child: RemoteChild) =>
  Effect.gen(function* () {
    const resolveOption = (target: string) =>
      runtime.peerRegistry.resolveTarget(target).pipe(
        Effect.map(Option.some),
        Effect.catch((error) =>
          isSubagentPeerTargetNotFoundError(error)
            ? Effect.succeed(Option.none())
            : Effect.fail(fail(error.message)),
        ),
      );
    const aliasPeer = yield* resolveOption(child.alias);
    if (Option.isSome(aliasPeer) && aliasPeer.value.environmentId === child.childEnvironmentId) {
      return aliasPeer.value;
    }
    const environmentPeer = yield* resolveOption(child.childEnvironmentId);
    if (
      Option.isSome(environmentPeer) &&
      environmentPeer.value.environmentId === child.childEnvironmentId
    ) {
      return environmentPeer.value;
    }
    if (Option.isSome(environmentPeer)) {
      return yield* fail(
        `Remote child ${child.childThreadId} belongs to environment ${child.childEnvironmentId}, but environment-id lookup resolved alias '${environmentPeer.value.alias}' at ${environmentPeer.value.environmentId}.`,
      );
    }
    if (Option.isSome(aliasPeer)) {
      return yield* fail(
        `Remote child ${child.childThreadId} belongs to environment ${child.childEnvironmentId}, but peer alias '${child.alias}' now points at ${aliasPeer.value.environmentId}.`,
      );
    }
    return yield* fail(
      `Remote child ${child.childThreadId} belongs to environment ${child.childEnvironmentId}, but peer alias '${child.alias}' is not registered.`,
    );
  });

const withPeerToolTimeout = <A>(
  effect: Effect.Effect<A, ThreadStartToolError>,
  timeoutContext: string,
): Effect.Effect<A, ThreadStartToolError> =>
  Effect.gen(function* () {
    const fiber = yield* effect.pipe(Effect.forkDetach);
    const exit = yield* Fiber.await(fiber).pipe(
      Effect.raceFirst(
        Effect.sleep(PEER_TOOL_CALL_TIMEOUT).pipe(
          Effect.flatMap(() =>
            Fiber.interrupt(fiber).pipe(
              Effect.forkDetach,
              Effect.as(
                fail(
                  `${timeoutContext} did not respond within ${PEER_TOOL_CALL_TIMEOUT_SECONDS} seconds.`,
                ),
              ),
            ),
          ),
          Effect.flatMap((error) => Effect.fail(error)),
        ),
      ),
    );
    if (Exit.isSuccess(exit)) return exit.value;
    return yield* Effect.failCause(exit.cause);
  });

const callPeerTool = (
  runtime: SubagentRuntime,
  peer: SubagentPeerRegistry.SubagentPeer,
  input: McpPeerClient.McpPeerCallToolInput,
  timeoutContext: string,
) =>
  withPeerToolTimeout(
    Effect.acquireUseRelease(
      McpPeerClient.connect(peer).pipe(
        Effect.provideService(HttpClient.HttpClient, runtime.httpClient),
      ),
      (session) =>
        Effect.gen(function* () {
          yield* runtime.peerRegistry.updateLastSeen(peer.alias);
          return yield* session.callTool(input);
        }),
      (session) => closePeerSession(session, peer),
    ).pipe(
      Effect.mapError((error) =>
        isThreadStartToolError(error) ? error : fail(error.message ?? "Remote peer call failed."),
      ),
    ),
    timeoutContext,
  );

const closePeerSession = (
  session: McpPeerClient.McpPeerClientSession,
  peer: SubagentPeerRegistry.SubagentPeer,
) =>
  Effect.gen(function* () {
    const fiber = yield* session.close().pipe(Effect.forkDetach);
    const closeExit = yield* Fiber.await(fiber).pipe(
      Effect.timeoutOption(PEER_SESSION_CLOSE_TIMEOUT),
    );
    if (Option.isNone(closeExit)) {
      yield* Fiber.interrupt(fiber).pipe(Effect.forkDetach);
      yield* Effect.logWarning("timed out closing MCP peer session", {
        peerAlias: peer.alias,
        peerEnvironmentId: peer.environmentId,
      });
      return;
    }
    if (Exit.isFailure(closeExit.value)) {
      yield* Effect.logWarning("failed to close MCP peer session", {
        peerAlias: peer.alias,
        peerEnvironmentId: peer.environmentId,
        cause: Cause.pretty(closeExit.value.cause),
      });
    }
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("failed to close MCP peer session", {
        peerAlias: peer.alias,
        peerEnvironmentId: peer.environmentId,
        cause: Cause.pretty(cause),
      }),
    ),
  );

const connectPeerWithTimeout = (
  runtime: SubagentRuntime,
  peer: SubagentPeerRegistry.SubagentPeer,
  timeoutContext: string,
) =>
  withPeerToolTimeout(
    McpPeerClient.connect(peer).pipe(
      Effect.provideService(HttpClient.HttpClient, runtime.httpClient),
      Effect.mapError((error) =>
        isThreadStartToolError(error)
          ? error
          : fail(error.message ?? "Remote peer connect failed."),
      ),
    ),
    timeoutContext,
  );

const callRemoteChildTool = (
  runtime: SubagentRuntime,
  child: RemoteChild,
  input: McpPeerClient.McpPeerCallToolInput,
) =>
  Effect.gen(function* () {
    const peer = yield* resolveRemoteChildPeer(runtime, child);
    return yield* callPeerTool(runtime, peer, input, `Remote peer '${peer.alias}'`);
  });

const remoteChildrenByIdForParent = (
  runtime: SubagentRuntime,
  parentThreadId: ThreadId,
  childThreadIds: ReadonlyArray<ThreadId>,
) =>
  runtime.remoteChildren.listByParent({ parentThreadId }).pipe(
    Effect.mapError((error) => toToolError(error, "Failed to load remote children.")),
    Effect.map((children) => {
      const requested = new Set(childThreadIds.map(String));
      return new Map(
        children
          .filter((child) => requested.has(String(child.childThreadId)))
          .map((child) => [String(child.childThreadId), child] as const),
      );
    }),
  );

const deliverRemoteCompletion = (
  runtime: SubagentRuntime,
  child: RemoteChild,
  check: CheckSubagentOutputType,
) =>
  Effect.gen(function* () {
    if (!isRemoteTerminalStatus(check.status)) return;
    const terminalStatus = childTerminalStatusFromRemoteStatus(check.status);
    const error =
      terminalStatus === "completed" ? null : `Remote sub-agent ended with status ${check.status}.`;
    const coordinator = yield* requireCoordinator();
    yield* coordinator.enqueueParentInjection({
      parentThreadId: child.parentThreadId,
      childThreadId: child.childThreadId,
      status: terminalStatus,
      finalAssistantText: check.latestAssistantText,
      error,
      dedupeKey: remoteTerminalWakeDedupeKey(child),
    });
  });

const releaseRemoteCompletionClaim = (
  runtime: SubagentRuntime,
  input: {
    readonly child: RemoteChild;
    readonly claimId: string;
    readonly updatedAt: IsoDateTime;
  },
) =>
  runtime.remoteChildren
    .releaseTerminalDeliveryClaim({
      parentThreadId: input.child.parentThreadId,
      childEnvironmentId: input.child.childEnvironmentId,
      childThreadId: input.child.childThreadId,
      claimId: input.claimId,
      updatedAt: input.updatedAt,
    })
    .pipe(
      Effect.catch((error) =>
        Effect.logWarning("failed to release remote terminal delivery claim", {
          childThreadId: input.child.childThreadId,
          childEnvironmentId: input.child.childEnvironmentId,
          cause: error.message,
        }),
      ),
    );

type RemoteTerminalStatusUpdate = {
  readonly parentThreadId: ThreadId;
  readonly childEnvironmentId: EnvironmentId;
  readonly childThreadId: ThreadId;
  readonly status: RemoteChildStatus;
  readonly lastPolledAt: IsoDateTime;
  readonly updatedAt: IsoDateTime;
};

type SuppressedRemoteTerminalWake = {
  readonly child: RemoteChild;
  readonly check: CheckSubagentOutputType;
  readonly claimId: string;
  readonly update: RemoteTerminalStatusUpdate;
};

const markRemoteTerminalClaim = (runtime: SubagentRuntime, input: SuppressedRemoteTerminalWake) =>
  runtime.remoteChildren.markTerminalStatus({ ...input.update, claimId: input.claimId }).pipe(
    Effect.mapError((error) => toToolError(error, "Failed to update remote child.")),
    Effect.flatMap((marked) =>
      Option.isSome(marked)
        ? Effect.void
        : Effect.fail(fail("Remote child completion claim was lost before status update.")),
    ),
  );

const releaseSuppressedRemoteTerminalWake = (
  runtime: SubagentRuntime,
  input: SuppressedRemoteTerminalWake,
) =>
  releaseRemoteCompletionClaim(runtime, {
    child: input.child,
    claimId: input.claimId,
    updatedAt: input.update.updatedAt,
  });

const markSuppressedRemoteTerminalWake = (
  runtime: SubagentRuntime,
  input: SuppressedRemoteTerminalWake,
) => markRemoteTerminalClaim(runtime, input);

const restoreSuppressedRemoteTerminalWake = (
  runtime: SubagentRuntime,
  input: SuppressedRemoteTerminalWake,
) =>
  Effect.uninterruptible(
    deliverRemoteCompletion(runtime, input.child, input.check).pipe(
      Effect.catch((error) =>
        releaseSuppressedRemoteTerminalWake(runtime, input).pipe(
          Effect.andThen(Effect.fail(error)),
        ),
      ),
      Effect.andThen(
        markRemoteTerminalClaim(runtime, input).pipe(
          Effect.catch((error) =>
            releaseSuppressedRemoteTerminalWake(runtime, input).pipe(
              Effect.andThen(Effect.fail(error)),
            ),
          ),
        ),
      ),
    ),
  );

const pollRemoteChildWithDeliveryResult = Effect.fn(
  "SubagentToolkit.pollRemoteChildWithDeliveryResult",
)(function* (
  runtime: SubagentRuntime,
  child: RemoteChild,
  options?: {
    readonly deliverTerminalWake?: boolean;
    readonly onSuppressedTerminalWake?: (
      suppressed: SuppressedRemoteTerminalWake,
    ) => Effect.Effect<void>;
  },
) {
  const callResult = yield* callRemoteChildTool(runtime, child, {
    name: "t3_check_subagent",
    arguments: { childThreadId: child.childThreadId },
  });
  const check = yield* decodeRemoteCheckResult(callResult);
  if (check.threadId !== child.childThreadId) {
    return yield* fail(
      `Remote peer returned check result for ${check.threadId}, expected ${child.childThreadId}.`,
    );
  }
  const now = yield* DateTime.now;
  const updatedAt = DateTime.formatIso(now);
  const claimStaleBefore = DateTime.formatIso(
    DateTime.makeUnsafe(now.epochMilliseconds - REMOTE_TERMINAL_DELIVERY_CLAIM_TTL_MS),
  );
  const nextStatus = remoteChildStatusFromToolStatus(check.status);
  const update = {
    parentThreadId: child.parentThreadId,
    childEnvironmentId: child.childEnvironmentId,
    childThreadId: child.childThreadId,
    status: nextStatus,
    lastPolledAt: IsoDateTime.make(updatedAt),
    updatedAt: IsoDateTime.make(updatedAt),
  } as const;
  if (isRemoteTerminalStatus(nextStatus)) {
    if (!isRemoteTerminalStatus(child.status)) {
      const claimId = yield* runtime.crypto.randomUUIDv4.pipe(Effect.orDie);
      const suppressed = { child, check, claimId, update } satisfies SuppressedRemoteTerminalWake;
      const deliverTerminalWake = options?.deliverTerminalWake ?? true;
      const claimEffect = runtime.remoteChildren
        .claimTerminalDelivery({
          parentThreadId: child.parentThreadId,
          childEnvironmentId: child.childEnvironmentId,
          childThreadId: child.childThreadId,
          claimId,
          claimedAt: IsoDateTime.make(updatedAt),
          claimStaleBefore: IsoDateTime.make(claimStaleBefore),
          lastPolledAt: IsoDateTime.make(updatedAt),
          updatedAt: IsoDateTime.make(updatedAt),
        })
        .pipe(
          Effect.mapError((error) =>
            toToolError(error, "Failed to claim remote child completion delivery."),
          ),
        );
      const claimed = yield* deliverTerminalWake
        ? claimEffect
        : Effect.uninterruptible(
            claimEffect.pipe(
              Effect.tap((claimed) =>
                Option.isSome(claimed) && options?.onSuppressedTerminalWake !== undefined
                  ? options.onSuppressedTerminalWake(suppressed)
                  : Effect.void,
              ),
            ),
          );
      if (Option.isSome(claimed)) {
        if (!deliverTerminalWake) {
          return {
            check,
            suppressedTerminalWake: suppressed,
          };
        }
        yield* Effect.uninterruptible(
          deliverRemoteCompletion(runtime, child, check).pipe(
            Effect.catch((error) =>
              releaseSuppressedRemoteTerminalWake(runtime, suppressed).pipe(
                Effect.andThen(Effect.fail(error)),
              ),
            ),
            Effect.andThen(markRemoteTerminalClaim(runtime, suppressed)),
          ),
        );
        return {
          check,
          suppressedTerminalWake: null,
        };
      }
    }
    return { check, suppressedTerminalWake: null };
  }
  yield* runtime.remoteChildren
    .updateStatus(update)
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to update remote child.")));
  return { check, suppressedTerminalWake: null };
});

const pollRemoteChild = Effect.fn("SubagentToolkit.pollRemoteChild")(function* (
  runtime: SubagentRuntime,
  child: RemoteChild,
) {
  const result = yield* pollRemoteChildWithDeliveryResult(runtime, child);
  return result.check;
});

const callRemoteSpawnTool = (
  runtime: SubagentRuntime,
  peer: SubagentPeerRegistry.SubagentPeer,
  session: McpPeerClient.McpPeerClientSession,
  arguments_: SpawnSubagentInput,
) =>
  Effect.gen(function* () {
    yield* runtime.peerRegistry
      .updateLastSeen(peer.alias)
      .pipe(Effect.mapError((error) => fail(error.message)));
    return yield* session
      .callTool({
        name: "t3_spawn_subagent",
        arguments: arguments_,
      })
      .pipe(Effect.mapError((error) => fail(error.message)));
  }).pipe(Effect.ensuring(closePeerSession(session, peer)));

const remoteWaitRow = (input: {
  readonly child: RemoteChild;
  readonly check: CheckSubagentOutputType;
  readonly status: string;
  readonly promoted: boolean;
}): WaitSubagentOutputType["results"][number] => {
  const terminal = isRemoteTerminalStatus(input.status);
  const failed = terminal && input.status !== "completed";
  return {
    childThreadId: input.child.childThreadId,
    status: input.status,
    turnCount: input.check.turnCount,
    finalAssistantText: terminal ? input.check.latestAssistantText : null,
    error: failed ? `Remote sub-agent ended with status ${input.check.status}.` : null,
    ...(input.promoted
      ? {
          note: "still running — you will be NOTIFIED when it completes; stop calling wait and do other work",
        }
      : {}),
  };
};

const waitRemoteSubagents = Effect.fn("SubagentToolkit.waitRemoteSubagents")(function* (input: {
  readonly runtime: SubagentRuntime;
  readonly children: ReadonlyArray<RemoteChild>;
  readonly mode: "all" | "any";
  readonly waitStartMs: number;
  readonly callerDeadlineMs: number;
  readonly autoPromoteDeadlineMs: number;
  readonly resumeToken: string | undefined;
}) {
  const suppressedTerminalWakes = new Map<string, SuppressedRemoteTerminalWake>();

  const restoreAllSuppressedTerminalWakes = (reason: string) =>
    Effect.gen(function* () {
      const pending = Array.from(suppressedTerminalWakes.values());
      if (pending.length === 0) return;
      const restoreExits = yield* Effect.forEach(
        pending,
        (suppressed) =>
          restoreSuppressedRemoteTerminalWake(input.runtime, suppressed).pipe(Effect.exit),
        { concurrency: "unbounded" },
      );
      suppressedTerminalWakes.clear();
      const restoreFailure = restoreExits.find(Exit.isFailure);
      if (restoreFailure !== undefined) {
        yield* Effect.logWarning("failed to restore remote terminal wake after wait exit", {
          reason,
          cause: Cause.pretty(restoreFailure.cause),
        });
      }
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("failed to clean up suppressed remote terminal wakes", {
          reason,
          cause: Cause.pretty(cause),
        }),
      ),
    );

  return yield* Effect.gen(function* () {
    const sliceStartMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
    const autoPromoteDeadlineWasActive = input.autoPromoteDeadlineMs <= input.callerDeadlineMs;
    const sliceDeadlineMs = Math.min(
      input.callerDeadlineMs,
      input.autoPromoteDeadlineMs,
      sliceStartMs + WAIT_SLICE_SECONDS * 1_000,
    );
    let rows: WaitSubagentOutputType["results"] = [];
    let promoted = false;

    while (true) {
      const checkExits = yield* Effect.forEach(
        input.children,
        (child) =>
          pollRemoteChildWithDeliveryResult(input.runtime, child, {
            deliverTerminalWake: false,
            onSuppressedTerminalWake: (suppressed) =>
              Effect.sync(() => {
                suppressedTerminalWakes.set(String(suppressed.child.childThreadId), suppressed);
              }),
          }).pipe(
            Effect.map((result) => ({ child, ...result })),
            Effect.exit,
          ),
        { concurrency: "unbounded" },
      );
      const successes = checkExits.flatMap((exit) => (Exit.isSuccess(exit) ? [exit.value] : []));
      for (const success of successes) {
        if (success.suppressedTerminalWake !== null) {
          suppressedTerminalWakes.set(
            String(success.suppressedTerminalWake.child.childThreadId),
            success.suppressedTerminalWake,
          );
        }
      }
      const failed = checkExits.find(Exit.isFailure);
      if (failed !== undefined) {
        yield* restoreAllSuppressedTerminalWakes("wait poll failure");
        return yield* Effect.failCause(failed.cause);
      }
      const checks = successes;
      const afterPollMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
      promoted =
        autoPromoteDeadlineWasActive &&
        afterPollMs >= input.autoPromoteDeadlineMs &&
        checks.some(({ check }) => !isRemoteTerminalStatus(check.status));
      rows = checks.map(({ child, check }) => {
        const terminal = isRemoteTerminalStatus(check.status);
        const status = terminal
          ? check.status
          : promoted
            ? "running"
            : afterPollMs >= input.callerDeadlineMs
              ? "timeout"
              : "pending";
        return remoteWaitRow({ child, check, status, promoted: promoted && !terminal });
      });
      const pending = promoted ? false : pendingForMode(rows, input.mode);
      if (!pending || afterPollMs >= sliceDeadlineMs) {
        const markResults = yield* Effect.forEach(
          Array.from(suppressedTerminalWakes.values()),
          (suppressed) =>
            markSuppressedRemoteTerminalWake(input.runtime, suppressed).pipe(
              Effect.exit,
              Effect.map((exit) => ({ suppressed, exit })),
            ),
          { concurrency: "unbounded" },
        );
        let markFailureCause: Cause.Cause<ThreadStartToolError> | null = null;
        for (const result of markResults) {
          if (Exit.isFailure(result.exit)) {
            markFailureCause ??= result.exit.cause;
          }
        }
        if (markFailureCause !== null) {
          yield* Effect.logWarning("failed to mark remote wait completion delivered", {
            cause: Cause.pretty(markFailureCause),
          });
          return yield* Effect.failCause(markFailureCause);
        }
        suppressedTerminalWakes.clear();
        const settledCount = rows.filter((row) => isWaitTerminal(row.status)).length;
        const timedOutCount = promoted ? 0 : rows.filter((row) => row.status === "timeout").length;
        return {
          results: rows,
          settledCount,
          timedOutCount,
          pending: promoted ? false : pending,
          resumeToken: `${input.waitStartMs}:${input.resumeToken ?? "remote"}`,
          ...(promoted ? { promoted: true } : {}),
        } satisfies WaitSubagentOutputType;
      }
      const sleepMs = Math.max(1, Math.min(1_000, sliceDeadlineMs - afterPollMs));
      yield* Effect.sleep(Duration.millis(sleepMs));
    }
  }).pipe(
    Effect.onExit((exit) =>
      Exit.isSuccess(exit) ? Effect.void : restoreAllSuppressedTerminalWakes("wait interrupted"),
    ),
  );
});

const isUntrackedProjectionWaitRow = (row: WaitSliceResult["results"][number]): boolean =>
  row.status === "failed" && row.error === UNTRACKED_PROJECTION_CHILD_ERROR;

const peerProjectionWaitRow = (
  runtime: SubagentRuntime,
  row: WaitSliceResult["results"][number],
  callerDeadlineMs: number,
  observedAtMs: number,
): Effect.Effect<WaitSliceResult["results"][number], ThreadStartToolError> =>
  loadThreadDetail(runtime, row.childThreadId).pipe(
    Effect.map((thread) =>
      Option.match(thread, {
        onNone: () => row,
        onSome: (detail): WaitSliceResult["results"][number] => {
          const terminalStatus = reliableWaitTerminalStatusOf(detail);
          if (terminalStatus === "completed") {
            return {
              childThreadId: row.childThreadId,
              status: "completed",
              finalAssistantText: finalAssistantTextFromThread(detail),
              error: null,
            };
          }
          if (terminalStatus === "failed") {
            return {
              childThreadId: row.childThreadId,
              status: "failed",
              finalAssistantText: finalAssistantTextFromThread(detail),
              error: "Child thread ended with status failed.",
            };
          }
          const timedOut = observedAtMs >= callerDeadlineMs;
          return {
            childThreadId: row.childThreadId,
            status: timedOut ? "timeout" : "pending",
            finalAssistantText: null,
            error: timedOut ? "wait exceeded budget" : null,
          };
        },
      }),
    ),
  );

const applyPeerProjectionWaitFallback = Effect.fn(
  "SubagentToolkit.applyPeerProjectionWaitFallback",
)(function* (input: {
  readonly invocation: McpInvocationContext.McpInvocationScope;
  readonly runtime: SubagentRuntime;
  readonly slice: WaitSliceResult;
  readonly mode: "all" | "any";
  readonly callerDeadlineMs: number;
  readonly observedAtMs: number;
}) {
  if (McpInvocationContext.isProviderInvocationScope(input.invocation)) return input.slice;
  if (!input.slice.results.some(isUntrackedProjectionWaitRow)) return input.slice;
  const results = yield* Effect.forEach(
    input.slice.results,
    (row) =>
      isUntrackedProjectionWaitRow(row)
        ? peerProjectionWaitRow(input.runtime, row, input.callerDeadlineMs, input.observedAtMs)
        : Effect.succeed(row),
    { concurrency: "unbounded" },
  );
  return {
    results,
    settledCount: results.filter((row) => isWaitTerminal(row.status)).length,
    timedOutCount: results.filter((row) => row.status === "timeout").length,
    pending: pendingForMode(results, input.mode),
    resumeToken: input.slice.resumeToken,
  } satisfies WaitSliceResult;
});

const pollRemoteChildrenOnce = Effect.fn("SubagentToolkit.pollRemoteChildrenOnce")(function* (
  runtime: SubagentRuntime,
) {
  const rows = yield* runtime.remoteChildren
    .listAll()
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to list remote children.")));
  const running = rows.filter((row) => !isRemoteTerminalStatus(row.status));
  yield* Effect.forEach(
    running,
    (row) =>
      pollRemoteChild(runtime, row).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("remote sub-agent poll failed", {
            parentThreadId: row.parentThreadId,
            childEnvironmentId: row.childEnvironmentId,
            childThreadId: row.childThreadId,
            alias: row.alias,
            cause: Cause.pretty(cause),
          }),
        ),
      ),
    { discard: true, concurrency: 4 },
  );
});

const remoteChildPoller = (runtime: SubagentRuntime) =>
  pollRemoteChildrenOnce(runtime).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("remote sub-agent poll sweep failed", { cause: Cause.pretty(cause) }),
    ),
    Effect.repeat(Schedule.spaced(Duration.seconds(2))),
  );

const spawnSubagent = Effect.fn("SubagentToolkit.spawn")(function* (input: SpawnSubagentInput) {
  const invocation = yield* requireSubagentCapability("subagent:spawn");
  const runtime = yield* requireRuntime();
  const spawnRuntime = yield* requireSpawnRuntime();

  const {
    detached: detachedInput,
    waitTimeoutSeconds,
    target,
    remoteParentThreadId,
    remoteParentEnvironmentId,
    ...threadStartInput
  } = input;

  if (target !== undefined) {
    if (!McpInvocationContext.isProviderInvocationScope(invocation)) {
      return yield* fail("target is only supported by a provider-scoped parent thread.");
    }
    if (detachedInput === false) {
      return yield* fail(
        "Foreground remote sub-agent spawn is not available yet; omit detached or set detached=true.",
      );
    }
    if (threadStartInput.directory === undefined) {
      return yield* fail(
        "Remote sub-agent spawn requires directory so the target backend can choose a local project.",
      );
    }
    const peer = yield* runtime.peerRegistry
      .resolveTarget(target)
      .pipe(Effect.mapError((error) => fail(error.message)));
    const descriptor = yield* probePeerDescriptor(runtime.httpClient, peer);
    const remoteArguments = {
      ...threadStartInput,
      detached: true,
      remoteParentThreadId: invocation.threadId,
      remoteParentEnvironmentId: invocation.environmentId,
    } satisfies SpawnSubagentInput;
    const session = yield* connectPeerWithTimeout(runtime, peer, `Remote peer '${peer.alias}'`);
    const callResult = yield* callRemoteSpawnTool(runtime, peer, session, remoteArguments);
    const started = yield* decodeRemoteSpawnResult(callResult);
    const createdAt = yield* nowIso;
    yield* runtime.remoteChildren
      .upsert({
        parentThreadId: invocation.threadId,
        childEnvironmentId: descriptor.environmentId,
        childThreadId: started.childThreadId,
        alias: peer.alias,
        spawnParams: remoteArguments,
        status: "running",
        lastPolledAt: null,
        createdAt,
        updatedAt: createdAt,
      })
      .pipe(Effect.mapError((error) => toToolError(error, "Failed to record remote child.")));
    return {
      ...started,
      parentThreadId: invocation.threadId,
    };
  }

  if (!McpInvocationContext.isProviderInvocationScope(invocation)) {
    if (remoteParentThreadId === undefined) {
      return yield* fail(
        "Peer-scoped sub-agent spawn requires remoteParentThreadId from the caller backend.",
      );
    }
    if (detachedInput === false) {
      return yield* fail("Peer-scoped remote sub-agent spawn must be detached.");
    }
    const parentEnvironmentId = invocation.sourceEnvironmentId;
    if (parentEnvironmentId === undefined) {
      return yield* fail(
        "Peer-scoped sub-agent spawn requires a peer token with sourceEnvironmentId from the caller backend.",
      );
    }
    if (
      remoteParentEnvironmentId !== undefined &&
      remoteParentEnvironmentId !== parentEnvironmentId
    ) {
      return yield* fail(
        "Peer-scoped sub-agent spawn remoteParentEnvironmentId does not match the authenticated caller backend.",
      );
    }
    yield* requirePeerParentAccess(invocation, remoteParentThreadId);
    const started = yield* spawnRuntime(threadStartInput, invocation);
    yield* dispatchParentSet(
      runtime,
      started.threadId,
      remoteParentThreadId,
      parentEnvironmentId,
    ).pipe(
      Effect.mapError((error) => toToolError(error, "Failed to link remote sub-agent parent.")),
      Effect.onError(() =>
        dispatchStartedChildDelete(runtime, started.threadId).pipe(Effect.catch(() => Effect.void)),
      ),
    );
    return {
      childThreadId: started.threadId,
      projectId: started.projectId,
      mode: started.mode,
      branch: started.branch,
      worktreePath: started.worktreePath,
      parentThreadId: remoteParentThreadId,
      ...(started.warning ? { warning: started.warning } : {}),
    } satisfies SpawnSubagentOutputType;
  }

  const providerInvocation = invocation;
  const coordinator = yield* requireCoordinator();
  const detached = detachedInput ?? true;

  // Fail-fast: refuse to spawn against a provider instance that no longer exists.
  const source = yield* loadThreadShell(runtime, providerInvocation.threadId).pipe(
    Effect.flatMap((shell) =>
      Option.match(shell, {
        onNone: () =>
          Effect.fail(fail(`Source thread ${providerInvocation.threadId} was not found.`)),
        onSome: Effect.succeed,
      }),
    ),
  );
  // An explicit bare `model` resolves against the live provider model lists; a
  // named model no provider serves fails loudly instead of silently spawning on
  // a different (inherited) model. Prefer the source thread's instance on ties.
  const modelSelection: ModelSelection =
    threadStartInput.model !== undefined
      ? yield* resolveExplicitModelSelection(
          runtime,
          threadStartInput.model,
          source.modelSelection.instanceId,
        )
      : (threadStartInput.modelSelection ?? source.modelSelection);
  const instance = yield* runtime.providerInstanceRegistry.getInstance(modelSelection.instanceId);
  if (instance === undefined) {
    return yield* fail(`Provider instance ${modelSelection.instanceId} is not available.`);
  }
  yield* coordinator.validateSpawn({
    parentThreadId: providerInvocation.threadId,
    model: modelSelection,
  });

  // Spawn with the ALREADY-resolved selection (drop `model`) so the thread
  // runtime does not re-resolve against a possibly-different registry snapshot —
  // the coordinator record and the started thread then share one selection.
  const { model: _resolvedModel, ...threadStartInputWithSelection } = threadStartInput;
  const { started, spawnedAtMs, cleanupAndReleaseFromDelete } = yield* Effect.uninterruptibleMask(
    (restore) =>
      Effect.gen(function* () {
        const dispatchLease = yield* restore(
          acquireLocalDispatchLease(runtime, providerInvocation.threadId),
        );
        const releaseDispatchLease: Effect.Effect<void, never, never> =
          runtime.dispatchLimiter.release(dispatchLease);
        const started = yield* spawnRuntime(
          { ...threadStartInputWithSelection, modelSelection },
          providerInvocation,
        ).pipe(Effect.onError(() => releaseDispatchLease));
        yield* runtime.dispatchLimiter.bindChild(dispatchLease, started.threadId);
        const cleanupAndReleaseFromDelete = (reason: string) =>
          cleanupStartedChild(runtime, started.threadId, reason, releaseDispatchLease);
        const spawnedAtMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);

        // Persist the parent linkage before registration relies on the in-memory
        // limiter binding; restart reconciliation seeds running children from
        // this durable link.
        yield* dispatchParentSet(runtime, started.threadId, providerInvocation.threadId).pipe(
          Effect.mapError((error) => toToolError(error, "Failed to link sub-agent to parent.")),
          Effect.onError(() => cleanupAndReleaseFromDelete("parent link failed")),
        );
        yield* coordinator
          .register({
            parentThreadId: providerInvocation.threadId,
            childThreadId: started.threadId,
            detached,
            model: modelSelection,
            spawnedAtMs,
          })
          .pipe(Effect.onError(() => cleanupAndReleaseFromDelete("coordinator register failed")));
        return { started, spawnedAtMs, cleanupAndReleaseFromDelete };
      }),
  );

  const base: SpawnSubagentOutputType = {
    childThreadId: started.threadId,
    projectId: started.projectId,
    mode: started.mode,
    branch: started.branch,
    worktreePath: started.worktreePath,
    parentThreadId: providerInvocation.threadId,
    ...(started.warning ? { warning: started.warning } : {}),
  };

  if (detached) return base;

  const budgetSeconds = clamp(
    waitTimeoutSeconds ?? WAIT_TIMEOUT_DEFAULT_SECONDS,
    WAIT_TIMEOUT_MIN_SECONDS,
    WAIT_TIMEOUT_MAX_SECONDS,
  );
  const budgetDeadlineMs = spawnedAtMs + budgetSeconds * 1_000;
  const initialSlice = yield* coordinator.waitSlice({
    childThreadIds: [started.threadId],
    mode: "all",
    budgetDeadlineMs,
  });
  const row = initialSlice.results[0];
  if (row !== undefined && isWaitTerminal(row.status)) {
    return {
      ...base,
      status: row.status,
      finalAssistantText: row.finalAssistantText ?? null,
    };
  }

  yield* coordinator
    .promoteToWake([started.threadId])
    .pipe(Effect.onError(() => cleanupAndReleaseFromDelete("foreground promotion failed")));
  return {
    ...base,
    warning: appendWarning(base.warning, FOREGROUND_SPAWN_PENDING_WARNING),
    status: "running",
    finalAssistantText: null,
  };
});

const dispatchParentSet = Effect.fn("SubagentToolkit.dispatchParentSet")(function* (
  runtime: SubagentRuntime,
  childThreadId: ThreadId,
  parentThreadId: ThreadId,
  parentEnvironmentId?: EnvironmentId,
) {
  const uuid = yield* runtime.crypto.randomUUIDv4.pipe(Effect.orDie);
  const createdAt = yield* nowIso;
  yield* runtime.orchestrationEngine.dispatch({
    type: "thread.parent.set",
    commandId: CommandId.make(`server:subagent-link:${uuid}`),
    threadId: childThreadId,
    parentThreadId,
    ...(parentEnvironmentId !== undefined ? { parentEnvironmentId } : {}),
    createdAt,
  });
});

const dispatchStartedChildDelete = Effect.fn("SubagentToolkit.dispatchStartedChildDelete")(
  function* (runtime: SubagentRuntime, childThreadId: ThreadId) {
    const uuid = yield* runtime.crypto.randomUUIDv4.pipe(Effect.orDie);
    yield* runtime.orchestrationEngine.dispatch({
      type: "thread.delete",
      commandId: CommandId.make(`server:subagent-cleanup:${uuid}`),
      threadId: childThreadId,
    });
  },
);

const cleanupStartedChild = (
  runtime: SubagentRuntime,
  childThreadId: ThreadId,
  reason: string,
  releaseDispatchLease: Effect.Effect<void, never, never>,
): Effect.Effect<void, never, never> =>
  dispatchStartedChildDelete(runtime, childThreadId).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("failed to delete sub-agent after spawn registration failure", {
        childThreadId,
        reason,
        cause: Cause.pretty(cause),
      }),
    ),
    Effect.ensuring(releaseDispatchLease),
  );

const steerSubagent = Effect.fn("SubagentToolkit.steer")(function* (input: SteerSubagentInput) {
  const invocation = yield* requireProviderInvocation;
  const runtime = yield* requireRuntime();
  const coordinator = yield* requireCoordinator();

  yield* coordinator.assertParent(invocation.threadId, input.childThreadId);

  const child = yield* loadThreadShell(runtime, input.childThreadId).pipe(
    Effect.flatMap((shell) =>
      Option.match(shell, {
        onNone: () => Effect.fail(fail(`Sub-agent ${input.childThreadId} was not found.`)),
        onSome: Effect.succeed,
      }),
    ),
  );

  // R-C: pick a provider-safe mechanism from the child's turn state + driver.
  // An idle child is steered now; a mid-turn child dispatches now for every known
  // driver (claudeAgent/codex/cursor/grok/opencode — all support mid-turn steer).
  // Only an unknown/future driver is deferred to a durable 'child_steer' row that
  // the coordinator drains when the child idles, since its mid-turn semantics are
  // unverified.
  const midTurn = child.latestTurn?.state === "running";
  const instance = yield* runtime.providerInstanceRegistry.getInstance(
    child.modelSelection.instanceId,
  );
  const driverKind = instance?.driverKind;
  const MIDTURN_STEER_DRIVERS = ["claudeAgent", "codex", "cursor", "grok", "opencode"];
  const deferMidTurn = midTurn && !MIDTURN_STEER_DRIVERS.includes(driverKind ?? "");

  if (deferMidTurn) {
    const id = (yield* runtime.crypto.randomUUIDv4.pipe(Effect.orDie)) as PendingDispatchId;
    const createdAt = yield* nowIso;
    const row: PendingDispatch = {
      id,
      kind: "child_steer",
      targetThreadId: input.childThreadId,
      sourceChildId: null,
      text: input.message,
      error: null,
      status: null,
      commandId: null,
      deliveredByWait: false,
      waitCancellable: false,
      createdAt: IsoDateTime.make(createdAt),
    };
    yield* runtime.pendingDispatches
      .insert(row)
      .pipe(Effect.mapError((error) => toToolError(error, "Failed to defer steer.")));
    return {
      childThreadId: input.childThreadId,
      accepted: true,
      applied: "deferred-until-idle" as const,
    };
  }

  const uuid = yield* runtime.crypto.randomUUIDv4.pipe(Effect.orDie);
  const messageId = MessageId.make(yield* runtime.crypto.randomUUIDv4.pipe(Effect.orDie));
  const createdAt = yield* nowIso;
  yield* dispatchActive({
    type: "thread.turn.start",
    commandId: CommandId.make(`server:subagent-steer:${uuid}`),
    threadId: input.childThreadId,
    message: { messageId, role: "user", text: input.message, attachments: [] },
    runtimeMode: child.runtimeMode,
    interactionMode: child.interactionMode,
    bootstrap: undefined,
    createdAt,
  }).pipe(Effect.mapError((error) => toToolError(error, "Failed to steer sub-agent.")));

  return {
    childThreadId: input.childThreadId,
    accepted: true,
    applied: midTurn ? ("queued-midturn" as const) : ("now" as const),
  };
});

const checkSubagent = Effect.fn("SubagentToolkit.check")(function* (input: CheckSubagentInput) {
  const invocation = yield* requireSubagentCapability("subagent:check");
  const runtime = yield* requireRuntime();
  yield* requirePeerChildAccess(runtime, invocation, [input.childThreadId]);

  if (McpInvocationContext.isProviderInvocationScope(invocation)) {
    const remoteChildren = yield* remoteChildrenByIdForParent(runtime, invocation.threadId, [
      input.childThreadId,
    ]);
    const remoteChild = remoteChildren.get(String(input.childThreadId));
    if (remoteChild !== undefined) {
      return yield* pollRemoteChild(runtime, remoteChild);
    }
  }

  const detail = yield* loadThreadDetail(runtime, input.childThreadId).pipe(
    Effect.flatMap((thread) =>
      Option.match(thread, {
        onNone: () => Effect.fail(fail(`Thread ${input.childThreadId} was not found.`)),
        onSome: Effect.succeed,
      }),
    ),
  );

  return {
    threadId: input.childThreadId,
    status: statusOf(detail),
    turnCount: turnCountOf(detail),
    latestAssistantText: latestAssistantTextOf(detail),
  };
});

// R-A: the cumulative wait-start epoch ms is carried across resumeToken
// re-calls by prefixing it onto the coordinator's opaque slice token
// ("<waitStartMs>:<coordinatorToken>"). The first call (no resumeToken) marks
// now; subsequent calls recover the marker so the 90s budget spans the whole
// wait regardless of how many slices the model re-issued.
const parseWaitStartMs = (resumeToken: string | undefined, nowMs: number): number => {
  if (resumeToken === undefined) return nowMs;
  const separator = resumeToken.indexOf(":");
  if (separator <= 0) return nowMs;
  const parsed = Number(resumeToken.slice(0, separator));
  return Number.isFinite(parsed) ? parsed : nowMs;
};

const waitSubagent = Effect.fn("SubagentToolkit.wait")(function* (input: WaitSubagentInput) {
  const invocation = yield* requireSubagentCapability("subagent:wait");
  const runtime = yield* requireRuntime();
  const coordinator = yield* requireCoordinator();
  const childThreadIds = Array.from(new Set(input.childThreadIds));

  yield* requirePeerChildAccess(runtime, invocation, childThreadIds);
  const remoteRowsById = McpInvocationContext.isProviderInvocationScope(invocation)
    ? yield* remoteChildrenByIdForParent(runtime, invocation.threadId, childThreadIds)
    : new Map<string, RemoteChild>();
  if (remoteRowsById.size > 0) {
    if (remoteRowsById.size !== childThreadIds.length) {
      return yield* fail(
        "t3_wait_subagent cannot mix local and remote sub-agents in one call yet; wait for each backend group separately.",
      );
    }
    const budgetSeconds = clamp(
      input.timeoutSeconds ?? WAIT_TIMEOUT_DEFAULT_SECONDS,
      WAIT_TIMEOUT_MIN_SECONDS,
      WAIT_TIMEOUT_MAX_SECONDS,
    );
    const nowMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
    const waitStartMs = parseWaitStartMs(input.resumeToken, nowMs);
    return yield* waitRemoteSubagents({
      runtime,
      children: childThreadIds.map((childThreadId) => remoteRowsById.get(String(childThreadId))!),
      mode: input.mode ?? "all",
      waitStartMs,
      callerDeadlineMs: nowMs + budgetSeconds * 1_000,
      autoPromoteDeadlineMs: waitStartMs + WAIT_AUTO_PROMOTE_SECONDS * 1_000,
      resumeToken: input.resumeToken,
    });
  }
  if (McpInvocationContext.isProviderInvocationScope(invocation)) {
    yield* Effect.forEach(
      childThreadIds,
      (childThreadId) => coordinator.assertParent(invocation.threadId, childThreadId),
      { discard: true },
    );
  }

  const budgetSeconds = clamp(
    input.timeoutSeconds ?? WAIT_TIMEOUT_DEFAULT_SECONDS,
    WAIT_TIMEOUT_MIN_SECONDS,
    WAIT_TIMEOUT_MAX_SECONDS,
  );
  const nowMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
  const waitStartMs = parseWaitStartMs(input.resumeToken, nowMs);
  // Hand the coordinator only its own opaque token (the part after the marker).
  const coordinatorToken =
    input.resumeToken !== undefined && input.resumeToken.indexOf(":") > 0
      ? input.resumeToken.slice(input.resumeToken.indexOf(":") + 1)
      : input.resumeToken;
  // Cap the cumulative blocking budget at WAIT_AUTO_PROMOTE_SECONDS regardless
  // of the caller's timeoutSeconds; the earlier of the two deadlines bounds the
  // slice so the slice itself never blocks past the auto-promote horizon.
  const autoPromoteDeadlineMs = waitStartMs + WAIT_AUTO_PROMOTE_SECONDS * 1_000;
  const callerDeadlineMs = nowMs + budgetSeconds * 1_000;
  const budgetDeadlineMs = Math.min(callerDeadlineMs, autoPromoteDeadlineMs);
  const terminalWaitChildIds = new Set<ThreadId>();
  const abandonTerminalWaits = () =>
    terminalWaitChildIds.size === 0
      ? Effect.void
      : coordinator.abandonWaitDelivery([...terminalWaitChildIds]);

  return yield* Effect.gen(function* () {
    // One bounded slice per invocation — the agent re-calls with the returned
    // resumeToken while `pending` is true (never one long HTTP hold).
    const rawSlice = yield* coordinator.waitSlice({
      childThreadIds,
      mode: input.mode ?? "all",
      budgetDeadlineMs,
      ...(coordinatorToken !== undefined ? { resumeToken: coordinatorToken } : {}),
    });
    const afterSliceMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
    const slice = yield* applyPeerProjectionWaitFallback({
      invocation,
      runtime,
      slice: rawSlice,
      mode: input.mode ?? "all",
      callerDeadlineMs,
      observedAtMs: afterSliceMs,
    });
    terminalWaitChildIds.clear();
    for (const row of slice.results) {
      if (isWaitTerminal(row.status)) {
        terminalWaitChildIds.add(row.childThreadId);
      }
    }

    const enrichedRows = yield* Effect.forEach(
      slice.results,
      (
        row,
      ): Effect.Effect<
        {
          readonly row: WaitSliceResult["results"][number];
          readonly projectionTerminal: boolean;
        },
        ThreadStartToolError,
        never
      > => {
        if (row.status !== "pending" && row.status !== "timeout") {
          return Effect.succeed({ row, projectionTerminal: false });
        }
        return loadThreadDetail(runtime, row.childThreadId).pipe(
          Effect.timeoutOption(`${WAIT_PROJECTION_ENRICHMENT_TIMEOUT_MS} millis`),
          Effect.map((thread) =>
            Option.match(thread, {
              onNone: () => ({ row, projectionTerminal: false }),
              onSome: (
                detailOption,
              ): {
                readonly row: WaitSliceResult["results"][number];
                readonly projectionTerminal: boolean;
              } =>
                Option.match(detailOption, {
                  onNone: () => ({ row, projectionTerminal: false }),
                  onSome: (
                    detail,
                  ): {
                    readonly row: WaitSliceResult["results"][number];
                    readonly projectionTerminal: boolean;
                  } => {
                    const projectedStatus = reliableWaitTerminalStatusOf(detail);
                    if (projectedStatus === null) return { row, projectionTerminal: false };
                    if (projectedStatus === "completed") {
                      return {
                        row: {
                          childThreadId: row.childThreadId,
                          status: "completed",
                          finalAssistantText:
                            row.finalAssistantText ?? finalAssistantTextFromThread(detail),
                          error: null,
                          ...(row.parentTurnIdAtWait === undefined
                            ? {}
                            : { parentTurnIdAtWait: row.parentTurnIdAtWait }),
                        },
                        projectionTerminal: true,
                      };
                    }
                    if (projectedStatus === "failed") {
                      return {
                        row: {
                          childThreadId: row.childThreadId,
                          status: "failed",
                          finalAssistantText:
                            row.finalAssistantText ?? finalAssistantTextFromThread(detail),
                          error: `Child thread ended with status ${projectedStatus}.`,
                          ...(row.parentTurnIdAtWait === undefined
                            ? {}
                            : { parentTurnIdAtWait: row.parentTurnIdAtWait }),
                        },
                        projectionTerminal: true,
                      };
                    }
                    return { row, projectionTerminal: false };
                  },
                }),
            }),
          ),
        );
      },
      { concurrency: "unbounded" },
    );
    const effectiveRows: ReadonlyArray<WaitSliceResult["results"][number]> = enrichedRows.map(
      (entry) => entry.row,
    );
    const effectiveSlice: WaitSliceResult = {
      results: effectiveRows,
      settledCount: effectiveRows.filter((row) => isWaitTerminal(row.status)).length,
      timedOutCount: effectiveRows.filter((row) => row.status === "timeout").length,
      pending: pendingForMode(effectiveRows, input.mode ?? "all"),
      resumeToken: slice.resumeToken,
    };
    const untrackedProjectionChildIds = new Set(
      rawSlice.results.filter(isUntrackedProjectionWaitRow).map((row) => String(row.childThreadId)),
    );
    const coordinatorTerminalIds = new Set(
      rawSlice.results
        .filter((row) => isWaitTerminal(row.status) && !isUntrackedProjectionWaitRow(row))
        .map((row) => String(row.childThreadId)),
    );
    const projectionTerminalIds = new Set(
      enrichedRows
        .filter((entry) => entry.projectionTerminal)
        .map((entry) => String(entry.row.childThreadId)),
    );
    const waitDeliveredRows = effectiveSlice.results.filter(
      (row) =>
        isWaitTerminal(row.status) &&
        (coordinatorTerminalIds.has(String(row.childThreadId)) ||
          projectionTerminalIds.has(String(row.childThreadId))),
    );
    for (const row of waitDeliveredRows) {
      terminalWaitChildIds.add(row.childThreadId);
    }
    // Auto-promote (R-A): the 90s budget elapsed and one+ children are still
    // running. The coordinator maps "pending when the supplied deadline elapsed"
    // to status "timeout"; when the supplied deadline was our auto-promote cap
    // rather than the caller's requested timeout, those timeout rows are still
    // running children that must be promoted to wake-on-completion.
    const autoPromoteDeadlineWasActive = autoPromoteDeadlineMs <= callerDeadlineMs;
    const stillRunningIds =
      autoPromoteDeadlineWasActive && afterSliceMs >= autoPromoteDeadlineMs
        ? effectiveSlice.results
            .filter(
              (row) =>
                (row.status === "pending" || row.status === "timeout") &&
                !untrackedProjectionChildIds.has(String(row.childThreadId)),
            )
            .map((row) => row.childThreadId)
        : [];
    const autoPromote = stillRunningIds.length > 0;
    const autoPromotedChildIds = new Set(
      stillRunningIds.map((childThreadId) => String(childThreadId)),
    );
    if (autoPromote) {
      yield* coordinator.promoteToWake(stillRunningIds);
    }

    // Enrich each row with a turn count from the projection (the coordinator's
    // terminal result intentionally does not track it). On auto-promote, a still
    // "pending"/auto-promote "timeout" child is reported as "running" with a note
    // telling the model to stop waiting.
    const results: WaitSubagentOutputType["results"] = yield* Effect.forEach(
      effectiveSlice.results,
      (row) =>
        loadThreadDetail(runtime, row.childThreadId).pipe(
          Effect.timeoutOption(`${WAIT_PROJECTION_ENRICHMENT_TIMEOUT_MS} millis`),
          Effect.map((thread) => {
            const promotedRunning =
              autoPromotedChildIds.has(String(row.childThreadId)) &&
              (row.status === "pending" || row.status === "timeout");
            return {
              childThreadId: row.childThreadId,
              status: promotedRunning ? "running" : row.status,
              turnCount: Option.match(thread, {
                onNone: () => 0,
                onSome: (detailOption) =>
                  Option.match(detailOption, { onNone: () => 0, onSome: turnCountOf }),
              }),
              finalAssistantText: row.finalAssistantText,
              error: promotedRunning ? null : row.error,
              ...(promotedRunning
                ? {
                    note: "still running — you will be NOTIFIED when it completes; stop calling wait and do other work",
                  }
                : {}),
            };
          }),
        ),
      { concurrency: "unbounded" },
    );
    const reportedTimedOutCount = autoPromote
      ? effectiveSlice.results.filter(
          (row) => row.status === "timeout" && !autoPromotedChildIds.has(String(row.childThreadId)),
        ).length
      : effectiveSlice.timedOutCount;
    const reportedPending = autoPromote
      ? pendingForMode(results, input.mode ?? "all")
      : effectiveSlice.pending;

    if (waitDeliveredRows.length > 0) {
      const markableRows = yield* markableWaitDeliveredRows(
        invocation,
        coordinator,
        waitDeliveredRows,
      );
      if (markableRows.length > 0) {
        yield* coordinator.markWaitDelivered(markableRows);
      }
      const markableChildIds = new Set(markableRows.map((row) => String(row.childThreadId)));
      const abandonedChildIds = waitDeliveredRows
        .filter((row) => !markableChildIds.has(String(row.childThreadId)))
        .map((row) => row.childThreadId);
      if (abandonedChildIds.length > 0) {
        yield* coordinator.abandonWaitDelivery(abandonedChildIds);
      }
      terminalWaitChildIds.clear();
    }

    return {
      results,
      settledCount: effectiveSlice.settledCount,
      timedOutCount: reportedTimedOutCount,
      // Auto-promoted rows have a wake path and are reported as running/notified;
      // projection-only peer rows keep their polling status.
      pending: reportedPending,
      resumeToken: `${waitStartMs}:${effectiveSlice.resumeToken}`,
      ...(autoPromote ? { promoted: true } : {}),
    };
  }).pipe(Effect.onExit((exit) => (Exit.isSuccess(exit) ? Effect.void : abandonTerminalWaits())));
});

const listSubagents = Effect.fn("SubagentToolkit.list")(function* (input: ListSubagentsInput) {
  const invocation = yield* requireSubagentCapability("subagent:list");
  const runtime = yield* requireRuntime();
  const coordinator = yield* requireCoordinator();

  const parentThreadId =
    input.parentThreadId ??
    (McpInvocationContext.isProviderInvocationScope(invocation)
      ? invocation.threadId
      : yield* fail("parentThreadId is required when listing with a peer-scoped credential."));
  yield* requirePeerParentAccess(invocation, parentThreadId);
  const registered = yield* coordinator.listChildren(parentThreadId);

  const children = yield* Effect.forEach(registered, (entry) =>
    loadThreadDetail(runtime, entry.childThreadId).pipe(
      Effect.map((thread) =>
        Option.match(thread, {
          onNone: () => ({
            childThreadId: entry.childThreadId,
            parentThreadId: entry.parentThreadId,
            detached: entry.detached,
            depth: entry.depth,
            spawnedAtMs: entry.spawnedAtMs,
            settled: entry.settled,
            status: "unknown",
            turnCount: 0,
          }),
          onSome: (detail) => ({
            childThreadId: entry.childThreadId,
            parentThreadId: entry.parentThreadId,
            detached: entry.detached,
            depth: entry.depth,
            spawnedAtMs: entry.spawnedAtMs,
            settled: entry.settled,
            status: statusOf(detail),
            turnCount: turnCountOf(detail),
          }),
        }),
      ),
    ),
  );

  const registeredIds = new Set(registered.map((entry) => String(entry.childThreadId)));
  const projectionPeerChildren =
    McpInvocationContext.isProviderInvocationScope(invocation) ||
    invocation.sourceEnvironmentId === undefined
      ? []
      : yield* runtime.projectionSnapshotQuery.getShellSnapshot().pipe(
          Effect.mapError((error) => toToolError(error, "Failed to load thread list.")),
          Effect.map((snapshot) =>
            snapshot.threads.filter(
              (thread) =>
                thread.parentThreadId === parentThreadId &&
                thread.parentEnvironmentId === invocation.sourceEnvironmentId &&
                !registeredIds.has(String(thread.id)),
            ),
          ),
          Effect.flatMap((threads) =>
            Effect.forEach(
              threads,
              (thread) =>
                loadThreadDetail(runtime, thread.id).pipe(
                  Effect.map((detail) =>
                    Option.match(detail, {
                      onNone: () => {
                        const status = statusOf({
                          latestTurn: thread.latestTurn,
                          messages: [],
                          session: thread.session,
                        });
                        return {
                          childThreadId: thread.id,
                          parentThreadId,
                          detached: true,
                          depth: 1,
                          spawnedAtMs: Date.parse(thread.createdAt),
                          settled: isWaitTerminal(status),
                          status,
                          turnCount: thread.latestTurn === null ? 0 : 1,
                        };
                      },
                      onSome: (detailThread) => {
                        const status = statusOf(detailThread);
                        return {
                          childThreadId: thread.id,
                          parentThreadId,
                          detached: true,
                          depth: 1,
                          spawnedAtMs: Date.parse(thread.createdAt),
                          settled: isWaitTerminal(status),
                          status,
                          turnCount: turnCountOf(detailThread),
                        };
                      },
                    }),
                  ),
                ),
              { concurrency: "unbounded" },
            ),
          ),
        );

  const remoteRows = yield* runtime.remoteChildren
    .listByParent({ parentThreadId })
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to list remote children.")));
  const remoteChildren = yield* Effect.forEach(
    remoteRows,
    (entry) =>
      pollRemoteChild(runtime, entry).pipe(
        Effect.map((check) => {
          const status = remoteChildStatusFromToolStatus(check.status);
          return {
            childThreadId: entry.childThreadId,
            parentThreadId: entry.parentThreadId,
            detached: true,
            depth: 1,
            spawnedAtMs: Date.parse(entry.createdAt),
            settled: isRemoteTerminalStatus(status),
            status,
            turnCount: check.turnCount,
          };
        }),
        Effect.orElseSucceed(() => ({
          childThreadId: entry.childThreadId,
          parentThreadId: entry.parentThreadId,
          detached: true,
          depth: 1,
          spawnedAtMs: Date.parse(entry.createdAt),
          settled: isRemoteTerminalStatus(entry.status),
          status: entry.status,
          turnCount: 0,
        })),
      ),
    { concurrency: "unbounded" },
  );

  return { parentThreadId, children: [...children, ...projectionPeerChildren, ...remoteChildren] };
});

const validateCron = (
  cronExpr: string,
  timezone: string,
): Effect.Effect<void, ThreadStartToolError> =>
  Effect.try({
    try: () => {
      const _cron = new Cron(cronExpr, { timezone });
    },
    catch: () => fail(`Invalid cron expression: ${cronExpr}`),
  });

const computeNextRunIso = (
  scheduleKind: "interval" | "cron",
  intervalSeconds: number | null,
  cronExpr: string | null,
  timezone: string,
  nowMs: number,
): string | null => {
  if (scheduleKind === "interval") {
    if (intervalSeconds === null || intervalSeconds <= 0) return null;
    return DateTime.formatIso(Option.getOrThrow(DateTime.make(nowMs + intervalSeconds * 1_000)));
  }
  if (cronExpr === null) return null;
  const next = new Cron(cronExpr, { timezone }).nextRun(
    DateTime.toDateUtc(Option.getOrThrow(DateTime.make(nowMs))),
  );
  return next === null
    ? null
    : DateTime.formatIso(Option.getOrThrow(DateTime.make(next.getTime())));
};

const scheduleCreate = Effect.fn("SubagentToolkit.scheduleCreate")(function* (
  input: ScheduleCreateInput,
) {
  const invocation = yield* requireProviderInvocation;
  const runtime = yield* requireRuntime();

  const threadId = input.threadId ?? invocation.threadId;
  const hasInterval = input.intervalSeconds !== undefined;
  const hasCron = input.cronExpr !== undefined;
  if (hasInterval === hasCron) {
    return yield* fail("Provide exactly one of intervalSeconds or cronExpr.");
  }
  if (hasInterval && (input.intervalSeconds as number) <= 0) {
    return yield* fail("intervalSeconds must be positive.");
  }

  const timezone = input.timezone ?? "UTC";
  const scheduleKind: "interval" | "cron" = hasCron ? "cron" : "interval";
  const cronExpr = input.cronExpr ?? null;
  const intervalSeconds = input.intervalSeconds ?? null;
  if (cronExpr !== null) yield* validateCron(cronExpr, timezone);

  const shell = yield* loadThreadShell(runtime, threadId).pipe(
    Effect.flatMap((shellOption) =>
      Option.match(shellOption, {
        onNone: () => Effect.fail(fail(`Thread ${threadId} was not found.`)),
        onSome: Effect.succeed,
      }),
    ),
  );

  // Resolve an explicit plain `model` to a full selection (provider/harness
  // inferred), preferring the thread's current instance on ties; null inherits
  // the thread's model on each run.
  const modelSelection =
    input.model !== undefined
      ? yield* resolveExplicitModelSelection(runtime, input.model, shell.modelSelection.instanceId)
      : null;

  const uuid = yield* runtime.crypto.randomUUIDv4.pipe(Effect.orDie);
  const nowMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
  const createdAt = yield* nowIso;
  const nextRunAt = computeNextRunIso(scheduleKind, intervalSeconds, cronExpr, timezone, nowMs);

  const task: ScheduledTask = {
    taskId: uuid as ScheduledTaskId,
    threadId,
    prompt: input.prompt,
    scheduleKind,
    intervalSeconds,
    cronExpr,
    timezoneName: timezone,
    enabled: NonNegativeInt.make(1),
    busyPolicy: input.busyPolicy ?? "skip",
    nextRunAt: nextRunAt === null ? null : IsoDateTime.make(nextRunAt),
    lastRunAt: null,
    lastStatus: null,
    lastError: null,
    skippedCount: NonNegativeInt.make(0),
    retryCount: NonNegativeInt.make(0),
    queuedCount: NonNegativeInt.make(0),
    modelSelection,
    createdAt: IsoDateTime.make(createdAt),
  };

  yield* runtime.scheduledTasks
    .insert(task)
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to create scheduled task.")));

  return toScheduleEntry(task);
});

const scheduleList = Effect.fn("SubagentToolkit.scheduleList")(function* (
  input: ScheduleListInput,
) {
  yield* requireProviderInvocation;
  const runtime = yield* requireRuntime();

  const tasks = yield* (
    input.threadId !== undefined
      ? runtime.scheduledTasks.listByThread({ threadId: input.threadId })
      : runtime.scheduledTasks.listAll()
  ).pipe(Effect.mapError((error) => toToolError(error, "Failed to list scheduled tasks.")));

  return { tasks: tasks.map(toScheduleEntry) };
});

const loadTaskById = (
  runtime: SubagentRuntime,
  taskId: ScheduledTaskId,
): Effect.Effect<ScheduledTask, ThreadStartToolError> =>
  runtime.scheduledTasks.listAll().pipe(
    Effect.mapError((error) => toToolError(error, "Failed to load scheduled task.")),
    Effect.flatMap((tasks) => {
      const found = tasks.find((task) => task.taskId === taskId);
      return found
        ? Effect.succeed(found)
        : Effect.fail(fail(`Scheduled task ${taskId} was not found.`));
    }),
  );

const scheduleUpdate = Effect.fn("SubagentToolkit.scheduleUpdate")(function* (
  input: ScheduleUpdateInput,
) {
  yield* requireProviderInvocation;
  const runtime = yield* requireRuntime();

  const existing = yield* loadTaskById(runtime, input.taskId);

  const scheduleKind: "interval" | "cron" =
    input.cronExpr !== undefined
      ? "cron"
      : input.intervalSeconds !== undefined
        ? "interval"
        : existing.scheduleKind;
  const cronExpr =
    input.cronExpr !== undefined
      ? input.cronExpr
      : scheduleKind === "cron"
        ? existing.cronExpr
        : null;
  const intervalSeconds =
    input.intervalSeconds !== undefined
      ? input.intervalSeconds
      : scheduleKind === "interval"
        ? existing.intervalSeconds
        : null;
  if (cronExpr !== null) yield* validateCron(cronExpr, existing.timezoneName);
  if (scheduleKind === "interval" && intervalSeconds !== null && intervalSeconds <= 0) {
    return yield* fail("intervalSeconds must be positive.");
  }

  // Recompute next_run_at whenever the cadence changed so the reactor honours it.
  const cadenceChanged = input.cronExpr !== undefined || input.intervalSeconds !== undefined;
  let nextRunAt = existing.nextRunAt;
  if (cadenceChanged) {
    const nowMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
    const computed = computeNextRunIso(
      scheduleKind,
      intervalSeconds,
      cronExpr,
      existing.timezoneName,
      nowMs,
    );
    nextRunAt = computed === null ? null : IsoDateTime.make(computed);
  }

  // Three-way `model` handling: omit (undefined) keeps the current selection;
  // an explicit null un-pins it (runs inherit the thread's model again); a plain
  // name re-routes. On a re-route, prefer the schedule's current instance, or —
  // when it is still inheriting — the target thread's instance, so a
  // multi-instance setup keeps continuity (matches create).
  let modelSelection: ModelSelection | null;
  if (input.model === undefined) {
    modelSelection = existing.modelSelection;
  } else if (input.model === null) {
    modelSelection = null;
  } else {
    const preferInstanceId =
      existing.modelSelection?.instanceId ??
      (yield* loadThreadShell(runtime, existing.threadId).pipe(
        Effect.map((shellOption) =>
          Option.match(shellOption, {
            onNone: () => undefined,
            onSome: (shell) => shell.modelSelection.instanceId,
          }),
        ),
      ));
    modelSelection = yield* resolveExplicitModelSelection(runtime, input.model, preferInstanceId);
  }

  const updated: ScheduledTask = {
    ...existing,
    enabled:
      input.enabled === undefined ? existing.enabled : NonNegativeInt.make(input.enabled ? 1 : 0),
    busyPolicy: input.busyPolicy ?? existing.busyPolicy,
    scheduleKind,
    intervalSeconds,
    cronExpr,
    nextRunAt,
    modelSelection,
  };

  yield* runtime.scheduledTasks
    .update(updated)
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to update scheduled task.")));

  return toScheduleEntry(updated);
});

const scheduleDelete = Effect.fn("SubagentToolkit.scheduleDelete")(function* (
  input: ScheduleDeleteInput,
) {
  yield* requireProviderInvocation;
  const runtime = yield* requireRuntime();

  yield* runtime.scheduledTasks
    .delete({ taskId: input.taskId })
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to delete scheduled task.")));

  return { taskId: input.taskId, deleted: true };
});

const handlers = {
  t3_spawn_subagent: spawnSubagent,
  t3_steer_subagent: steerSubagent,
  t3_check_subagent: checkSubagent,
  t3_wait_subagent: waitSubagent,
  t3_list_subagents: listSubagents,
  t3_schedule_create: scheduleCreate,
  t3_schedule_list: scheduleList,
  t3_schedule_update: scheduleUpdate,
  t3_schedule_delete: scheduleDelete,
} satisfies Parameters<typeof SubagentToolkit.toLayer>[0];

export const SubagentToolkitHandlersLive = SubagentToolkit.toLayer(handlers);

const makeSubagentRuntime = Effect.fn("SubagentToolkit.makeActiveRuntime")(function* () {
  const crypto = yield* Crypto.Crypto;
  const orchestrationEngine = yield* OrchestrationEngineService;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const providerInstanceRegistry = yield* ProviderInstanceRegistry;
  const scheduledTasks = yield* ScheduledTaskRepository;
  const pendingDispatches = yield* PendingDispatchRepository;
  const remoteChildren = yield* RemoteChildRepository;
  const peerRegistry = yield* SubagentPeerRegistry.SubagentPeerRegistry;
  const httpClient = yield* HttpClient.HttpClient;
  const dispatchLimiter = yield* SubagentDispatchLimiter;
  return {
    crypto,
    orchestrationEngine,
    projectionSnapshotQuery,
    providerInstanceRegistry,
    scheduledTasks,
    pendingDispatches,
    remoteChildren,
    peerRegistry,
    httpClient,
    dispatchLimiter,
  };
});

export const SubagentRuntimeLive = Layer.effectDiscard(
  Effect.acquireRelease(
    makeSubagentRuntime().pipe(
      Effect.tap((runtime) =>
        Effect.sync(() => {
          activeRuntime = runtime;
        }).pipe(Effect.andThen(Effect.forkScoped(remoteChildPoller(runtime)))),
      ),
    ),
    (runtime) =>
      Effect.sync(() => {
        if (activeRuntime === runtime) activeRuntime = null;
      }),
  ),
);
