/**
 * Sub-agent + scheduler MCP toolkit handlers (finalPlan §6/§7).
 *
 * Mirrors the thread toolkit: a module-level active runtime (captured by
 * `SubagentRuntimeLive`, cloned from `ThreadStartRuntimeLive`) holds the live
 * services so the toolkit handlers can reach them without threading them
 * through the toolkit `Context`. The handlers reuse pr3107's
 * `activeThreadStartRuntime` for spawning, the live `ChildThreadCoordinator`
 * for registration/wake delivery, and `dispatchActive` for steering.
 *
 * @module subagent/handlers
 */
import {
  CommandId,
  EnvironmentId,
  isProviderAvailable,
  IsoDateTime,
  MessageId,
  NonNegativeInt,
  ThreadId,
  type ModelSelection,
  type OrchestrationThread,
  type OrchestrationThreadShell,
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
import { McpSchema, McpServer } from "effect/unstable/ai";
import { HttpClient } from "effect/unstable/http";

import { coordinatorActive } from "../../../orchestration/Layers/ChildThreadCoordinator.ts";
import type {
  ChildTerminalStatus,
  ChildThreadCoordinatorShape,
} from "../../../orchestration/Services/ChildThreadCoordinator.ts";
import { dispatchActive } from "../../../orchestration/Services/BootstrapTurnStartDispatcher.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { canReadDataAudience } from "../../../auth/audienceDataPolicy.ts";
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
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { activeThreadStartRuntimeOf, type ActiveThreadStartRuntime } from "../thread/handlers.ts";
import { applyMcpReasoningEffort } from "../thread/reasoningEffort.ts";
import { ThreadStartToolError } from "../thread/tools.ts";
import { SubagentDispatchLimiter } from "./SubagentDispatchLimiter.ts";
import {
  SubagentToolkit,
  LegacyCheckSubagentInput,
  type LegacyCheckSubagentInput as LegacyCheckSubagentInputType,
  SubagentDetailOutput,
  type SubagentDetailOutput as SubagentDetailOutputType,
  type ScheduleCreateInput,
  type ScheduleDeleteInput,
  type ScheduleListInput,
  type ScheduleUpdateInput,
  SpawnSubagentInternalInput,
  type SpawnSubagentInternalInput as SpawnSubagentInternalInputType,
  SpawnSubagentOutput,
  type SpawnSubagentOutput as SpawnSubagentOutputType,
  type SteerSubagentInput,
  type SubagentsInput,
} from "./tools.ts";

import {
  sessionDispatchAuthority,
  type OrchestrationCommandDispatchAuthority,
} from "../../../orchestration/commandAudienceGuard.ts";
const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
const isThreadStartToolError = Schema.is(ThreadStartToolError);
const isSubagentPeerTargetNotFoundError = Schema.is(
  SubagentPeerRegistry.SubagentPeerTargetNotFoundError,
);
const PEER_TOOL_CALL_TIMEOUT_SECONDS = 5;
const PEER_TOOL_CALL_TIMEOUT = Duration.seconds(PEER_TOOL_CALL_TIMEOUT_SECONDS);
const PEER_SESSION_CLOSE_TIMEOUT = Duration.seconds(2);
const LOCAL_SPAWN_DISPATCH_LEASE_TIMEOUT_SECONDS = 5;
const LOCAL_SPAWN_DISPATCH_LEASE_TIMEOUT = Duration.seconds(
  LOCAL_SPAWN_DISPATCH_LEASE_TIMEOUT_SECONDS,
);
const REMOTE_TERMINAL_DELIVERY_CLAIM_TTL_MS = 5 * 60 * 1_000;
const fail = (message: string) => new ThreadStartToolError({ message });
const decodeLegacyCheckSubagentInput = Schema.decodeUnknownEffect(LegacyCheckSubagentInput);
const decodeLegacySpawnSubagentInput = Schema.decodeUnknownEffect(SpawnSubagentInternalInput);
const decodeSubagentDetailOutput = Schema.decodeUnknownEffect(SubagentDetailOutput);
const encodeSubagentDetailOutput = Schema.encodeUnknownEffect(SubagentDetailOutput);
const encodeSpawnSubagentOutput = Schema.encodeUnknownEffect(SpawnSubagentOutput);

const toToolError = (error: unknown, fallback: string): ThreadStartToolError =>
  isThreadStartToolError(error) ? error : fail(error instanceof Error ? error.message : fallback);

const providerThreadAuthority = (
  thread: Pick<OrchestrationThread, "id" | "dataAudience">,
): OrchestrationCommandDispatchAuthority =>
  sessionDispatchAuthority({
    subject: `provider-thread:${thread.id}`,
    audienceCeiling: thread.dataAudience,
  });

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

const acquireLocalDispatchLease = Effect.fn("SubagentToolkit.acquireLocalDispatchLease")(function* (
  runtime: SubagentRuntime,
  parentThreadId: ThreadId,
) {
  const lease = yield* runtime.dispatchLimiter.acquire.pipe(
    Effect.timeoutOption(LOCAL_SPAWN_DISPATCH_LEASE_TIMEOUT),
  );
  if (Option.isSome(lease)) return lease.value;

  const holderChildThreadIds = yield* runtime.dispatchLimiter.leasedChildThreadIds;
  yield* Effect.logWarning("sub-agent spawn dispatch capacity timed out", {
    parentThreadId,
    maxConcurrentDispatches: runtime.dispatchLimiter.maxConcurrentDispatches,
    timeoutSeconds: LOCAL_SPAWN_DISPATCH_LEASE_TIMEOUT_SECONDS,
    holderChildThreadIds,
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
 * `t3_subagents` (matches the coordinator's terminal
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

const isWaitTerminal = (status: string): boolean =>
  status === "completed" || status === "failed" || status === "interrupted" || status === "killed";

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

const loadThreadShell = (runtime: SubagentRuntime, threadId: ThreadId) =>
  runtime.projectionSnapshotQuery
    .getThreadShellById(threadId)
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to load thread.")));

const loadThreadShellIncludingArchived = (runtime: SubagentRuntime, threadId: ThreadId) =>
  runtime.projectionSnapshotQuery
    .getThreadShellByIdIncludingArchived(threadId)
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to load thread.")));

const loadProviderSourceThread = Effect.fn("SubagentToolkit.loadProviderSourceThread")(function* (
  runtime: SubagentRuntime,
  invocation: McpInvocationContext.ProviderMcpInvocationScope,
) {
  return yield* loadThreadShellIncludingArchived(runtime, invocation.threadId).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.fail(fail(`Thread ${invocation.threadId} was not found.`)),
        onSome: Effect.succeed,
      }),
    ),
  );
});

const loadAudienceVisibleTargetThread = Effect.fn(
  "SubagentToolkit.loadAudienceVisibleTargetThread",
)(function* (
  runtime: SubagentRuntime,
  sourceThread: OrchestrationThreadShell,
  targetThreadId: ThreadId,
  notFoundMessage: string,
  options?: { readonly includingArchived?: boolean },
) {
  const targetThread = yield* options?.includingArchived === true
    ? loadThreadShellIncludingArchived(runtime, targetThreadId)
    : loadThreadShell(runtime, targetThreadId);
  if (
    Option.isNone(targetThread) ||
    !canReadDataAudience(sourceThread.dataAudience, targetThread.value.dataAudience)
  ) {
    return yield* fail(notFoundMessage);
  }
  return targetThread.value;
});

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
  reasoningEffort?: string,
): Effect.Effect<ModelSelection, ThreadStartToolError> =>
  buildModelSources(runtime).pipe(
    Effect.flatMap((modelSources) => {
      const resolved = pickModelSelectionFromInstances(model, modelSources, preferInstanceId);
      if (resolved === null) {
        return Effect.fail(
          fail(
            `Model "${model}" is not served by any configured provider. Pass a model shown in the model picker, or omit "model" to keep the thread's current model.`,
          ),
        );
      }
      const effort = applyMcpReasoningEffort(resolved, modelSources, reasoningEffort);
      return effort.error === undefined
        ? Effect.succeed(effort.selection)
        : Effect.fail(fail(effort.error));
    }),
  );

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
  return text && text.length > 0 ? text : "Remote sub-agent status request failed.";
};

const decodeRemoteCheckResult = (result: {
  readonly isError?: boolean | undefined;
  readonly structuredContent?: unknown;
  readonly content?: ReadonlyArray<unknown> | undefined;
}): Effect.Effect<SubagentDetailOutputType, ThreadStartToolError> => {
  if (result.isError === true) {
    return Effect.fail(fail(textContentOfToolResult(result)));
  }
  return decodeSubagentDetailOutput(result.structuredContent).pipe(
    Effect.mapError((error) =>
      fail(
        error instanceof Error
          ? error.message
          : "Remote sub-agent check returned an invalid response.",
      ),
    ),
  );
};

const isMissingPeerToolResult = (
  result: {
    readonly isError?: boolean | undefined;
    readonly content?: ReadonlyArray<unknown> | undefined;
  },
  toolName: string,
): boolean =>
  result.isError === true && textContentOfToolResult(result).includes(`'${toolName}' not found`);

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

const withPeerToolTimeout = <A, E>(
  effect: Effect.Effect<A, E>,
  timeoutContext: string,
): Effect.Effect<A, E | ThreadStartToolError> =>
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

const callRemoteChildTool = (
  runtime: SubagentRuntime,
  child: RemoteChild,
  input: McpPeerClient.McpPeerCallToolInput,
) =>
  Effect.gen(function* () {
    const peer = yield* resolveRemoteChildPeer(runtime, child);
    return yield* callPeerTool(runtime, peer, input, `Remote peer '${peer.alias}'`);
  });

const isMcpPeerClientError = Schema.is(McpPeerClient.McpPeerClientError);

const isMissingPeerToolError = (error: unknown, toolName: string): boolean => {
  if (!isMcpPeerClientError(error)) return false;
  if (error.operation !== "json-rpc" || error.method !== "tools/call") return false;
  const detail = error.detail.toLowerCase();
  return (
    /invalid[ _-]?(params|parameters)/u.test(detail) ||
    detail.includes("unknown tool") ||
    (detail.includes(toolName.toLowerCase()) && detail.includes("not found"))
  );
};

const toPeerToolError = (error: unknown): ThreadStartToolError =>
  isThreadStartToolError(error)
    ? error
    : fail(error instanceof Error ? error.message : "Remote peer call failed.");

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
  check: SubagentDetailOutputType,
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
  readonly check: SubagentDetailOutputType;
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

const pollRemoteChild = Effect.fn("SubagentToolkit.pollRemoteChild")(function* (
  runtime: SubagentRuntime,
  child: RemoteChild,
) {
  const subagentsResult = yield* callRemoteChildTool(runtime, child, {
    name: "t3_subagents",
    arguments: { childThreadId: child.childThreadId },
  }).pipe(
    Effect.catch((error) =>
      isMissingPeerToolError(error, "t3_subagents")
        ? callRemoteChildTool(runtime, child, {
            name: "t3_check_subagent",
            arguments: { childThreadId: child.childThreadId },
          })
        : Effect.fail(error),
    ),
    Effect.mapError(toPeerToolError),
  );
  const callResult = isMissingPeerToolResult(subagentsResult, "t3_subagents")
    ? yield* callRemoteChildTool(runtime, child, {
        name: "t3_check_subagent",
        arguments: { childThreadId: child.childThreadId },
      }).pipe(Effect.mapError(toPeerToolError))
    : subagentsResult;
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
      const claimed = yield* runtime.remoteChildren
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
      if (Option.isSome(claimed)) {
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
        return check;
      }
    }
    return check;
  }
  yield* runtime.remoteChildren
    .updateStatus(update)
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to update remote child.")));
  return check;
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

const PUBLIC_SPAWN_KEYS = new Set([
  "prompt",
  "model",
  "title",
  "directory",
  "branch",
  "reasoningEffort",
]);

const spawnSubagent = Effect.fn("SubagentToolkit.spawn")(function* (
  input: SpawnSubagentInternalInputType,
) {
  const invocation = yield* requireSubagentCapability("subagent:spawn");
  const runtime = yield* requireRuntime();
  const spawnRuntime = yield* requireSpawnRuntime();

  const {
    detached: _detached,
    target: _target,
    remoteParentThreadId,
    remoteParentEnvironmentId,
    waitTimeoutSeconds: _waitTimeoutSeconds,
    ...threadStartInput
  } = input;

  if (!McpInvocationContext.isProviderInvocationScope(invocation)) {
    if (remoteParentThreadId === undefined) {
      return yield* fail(
        "Peer-scoped sub-agent spawn requires remoteParentThreadId from the caller backend.",
      );
    }
    if (input.detached === false) {
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
    const { output: started, targetProject } = yield* spawnRuntime(threadStartInput, invocation);
    const startedThreadAuthority = sessionDispatchAuthority({
      subject: `provider-thread:${started.threadId}`,
      audienceCeiling: targetProject.dataAudience,
    });
    yield* dispatchParentSet(
      runtime,
      started.threadId,
      remoteParentThreadId,
      startedThreadAuthority,
      parentEnvironmentId,
    ).pipe(
      Effect.mapError((error) => toToolError(error, "Failed to link remote sub-agent parent.")),
      Effect.onError(() =>
        dispatchStartedChildDelete(runtime, started.threadId, startedThreadAuthority).pipe(
          Effect.catch(() => Effect.void),
        ),
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
  const unexpectedKeys = Object.keys(input).filter((key) => !PUBLIC_SPAWN_KEYS.has(key));
  if (unexpectedKeys.length > 0) {
    return yield* fail(
      `Unsupported t3_spawn_subagent argument${unexpectedKeys.length === 1 ? "" : "s"}: ${unexpectedKeys.join(", ")}. Valid arguments: prompt, model, title, directory, branch, reasoningEffort.`,
    );
  }
  if (input.model === undefined || input.title === undefined) {
    const missing = [
      ...(input.model === undefined ? ["model"] : []),
      ...(input.title === undefined ? ["title"] : []),
    ];
    return yield* fail(`Missing required t3_spawn_subagent argument(s): ${missing.join(", ")}.`);
  }

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
  const modelSelection: ModelSelection = yield* resolveExplicitModelSelection(
    runtime,
    input.model,
    source.modelSelection.instanceId,
    input.reasoningEffort,
  );
  const instance = yield* runtime.providerInstanceRegistry.getInstance(modelSelection.instanceId);
  if (instance === undefined) {
    return yield* fail(`Provider instance ${modelSelection.instanceId} is not available.`);
  }
  yield* coordinator.validateSpawn({
    parentThreadId: providerInvocation.threadId,
    model: modelSelection,
  });
  const sourceAuthority = providerThreadAuthority(source);

  // Spawn with the ALREADY-resolved selection (drop `model`) so the thread
  // runtime does not re-resolve against a possibly-different registry snapshot —
  // the coordinator record and the started thread then share one selection.
  const {
    model: _resolvedModel,
    reasoningEffort: _reasoningEffort,
    ...threadStartInputBase
  } = threadStartInput;
  const { started } = yield* Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const dispatchLease = yield* restore(
        acquireLocalDispatchLease(runtime, providerInvocation.threadId),
      );
      const releaseDispatchLease: Effect.Effect<void, never, never> =
        runtime.dispatchLimiter.release(dispatchLease);
      const { output: started } = yield* spawnRuntime(
        { ...threadStartInputBase, modelSelection },
        providerInvocation,
        { providerSessionDetached: true },
      ).pipe(Effect.onError(() => releaseDispatchLease));
      yield* runtime.dispatchLimiter.bindChild(dispatchLease, started.threadId);
      const cleanupAndReleaseFromDelete = (reason: string) =>
        cleanupStartedChild(
          runtime,
          started.threadId,
          sourceAuthority,
          reason,
          releaseDispatchLease,
        );
      const spawnedAtMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);

      // Persist the parent linkage before registration relies on the in-memory
      // limiter binding; restart reconciliation seeds running children from
      // this durable link.
      yield* dispatchParentSet(
        runtime,
        started.threadId,
        providerInvocation.threadId,
        sourceAuthority,
      ).pipe(
        Effect.mapError((error) => toToolError(error, "Failed to link sub-agent to parent.")),
        Effect.onError(() => cleanupAndReleaseFromDelete("parent link failed")),
      );
      yield* coordinator
        .register({
          parentThreadId: providerInvocation.threadId,
          childThreadId: started.threadId,
          detached: true,
          model: modelSelection,
          spawnedAtMs,
        })
        .pipe(Effect.onError(() => cleanupAndReleaseFromDelete("coordinator register failed")));
      return { started };
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

  return base;
});

const dispatchParentSet = Effect.fn("SubagentToolkit.dispatchParentSet")(function* (
  runtime: SubagentRuntime,
  childThreadId: ThreadId,
  parentThreadId: ThreadId,
  authority: OrchestrationCommandDispatchAuthority,
  parentEnvironmentId?: EnvironmentId,
) {
  const uuid = yield* runtime.crypto.randomUUIDv4.pipe(Effect.orDie);
  const createdAt = yield* nowIso;
  yield* runtime.orchestrationEngine.dispatch(
    {
      type: "thread.parent.set",
      commandId: CommandId.make(`server:subagent-link:${uuid}`),
      threadId: childThreadId,
      parentThreadId,
      ...(parentEnvironmentId !== undefined ? { parentEnvironmentId } : {}),
      createdAt,
    },
    authority,
  );
});

const dispatchStartedChildDelete = Effect.fn("SubagentToolkit.dispatchStartedChildDelete")(
  function* (
    runtime: SubagentRuntime,
    childThreadId: ThreadId,
    authority: OrchestrationCommandDispatchAuthority,
  ) {
    const uuid = yield* runtime.crypto.randomUUIDv4.pipe(Effect.orDie);
    yield* runtime.orchestrationEngine.dispatch(
      {
        type: "thread.delete",
        commandId: CommandId.make(`server:subagent-cleanup:${uuid}`),
        threadId: childThreadId,
      },
      authority,
    );
  },
);

const cleanupStartedChild = (
  runtime: SubagentRuntime,
  childThreadId: ThreadId,
  authority: OrchestrationCommandDispatchAuthority,
  reason: string,
  releaseDispatchLease: Effect.Effect<void, never, never>,
): Effect.Effect<void, never, never> =>
  dispatchStartedChildDelete(runtime, childThreadId, authority).pipe(
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

  const sourceThread = yield* loadProviderSourceThread(runtime, invocation);
  const child = yield* loadAudienceVisibleTargetThread(
    runtime,
    sourceThread,
    input.childThreadId,
    `Sub-agent ${input.childThreadId} was not found.`,
  );

  yield* coordinator.assertParent(invocation.threadId, input.childThreadId);

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
  yield* dispatchActive(
    {
      type: "thread.turn.start",
      commandId: CommandId.make(`server:subagent-steer:${uuid}`),
      threadId: input.childThreadId,
      message: { messageId, role: "user", text: input.message, attachments: [] },
      runtimeMode: child.runtimeMode,
      interactionMode: child.interactionMode,
      bootstrap: undefined,
      createdAt,
    },
    providerThreadAuthority(sourceThread),
  ).pipe(Effect.mapError((error) => toToolError(error, "Failed to steer sub-agent.")));

  return {
    childThreadId: input.childThreadId,
    accepted: true,
    applied: midTurn ? ("queued-midturn" as const) : ("now" as const),
  };
});

const checkSubagent = Effect.fn("SubagentToolkit.check")(function* (
  input: LegacyCheckSubagentInput,
) {
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
    const coordinator = yield* requireCoordinator();
    yield* coordinator.assertParent(invocation.threadId, input.childThreadId);
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
const listSubagents = Effect.fn("SubagentToolkit.list")(function* (input: {
  readonly parentThreadId?: ThreadId;
}) {
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

const subagents = Effect.fn("SubagentToolkit.subagents")(function* (input: SubagentsInput) {
  return input.childThreadId === undefined
    ? yield* listSubagents({})
    : yield* checkSubagent({ childThreadId: input.childThreadId });
});

const legacyPeerCheckSubagent = Effect.fn("SubagentToolkit.legacyPeerCheck")(function* (
  input: LegacyCheckSubagentInputType,
) {
  const invocation = yield* McpInvocationContext.McpInvocationContext;
  if (McpInvocationContext.isProviderInvocationScope(invocation)) {
    return yield* fail("t3_check_subagent is available only to authenticated peer backends.");
  }
  return yield* checkSubagent(input);
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

  const sourceThread = yield* loadProviderSourceThread(runtime, invocation);
  const shell = yield* loadAudienceVisibleTargetThread(
    runtime,
    sourceThread,
    threadId,
    `Thread ${threadId} was not found.`,
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
    busyPolicy: "skip",
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
  const invocation = yield* requireProviderInvocation;
  const runtime = yield* requireRuntime();

  const sourceThread = yield* loadProviderSourceThread(runtime, invocation);

  const tasks = yield* (
    input.threadId !== undefined
      ? runtime.scheduledTasks.listByThread({ threadId: input.threadId })
      : runtime.scheduledTasks.listAll()
  ).pipe(Effect.mapError((error) => toToolError(error, "Failed to list scheduled tasks.")));

  const visibility = yield* Effect.forEach(
    tasks,
    (task) =>
      runtime.projectionSnapshotQuery.getThreadShellByIdIncludingArchived(task.threadId).pipe(
        Effect.mapError((error) => toToolError(error, "Failed to filter scheduled tasks.")),
        Effect.map(
          Option.exists((targetThread) =>
            canReadDataAudience(sourceThread.dataAudience, targetThread.dataAudience),
          ),
        ),
      ),
    { concurrency: 8 },
  );

  return {
    tasks: tasks.filter((_, index) => visibility[index] === true).map(toScheduleEntry),
  };
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
  const invocation = yield* requireProviderInvocation;
  const runtime = yield* requireRuntime();

  const existing = yield* loadTaskById(runtime, input.taskId);
  const sourceThread = yield* loadProviderSourceThread(runtime, invocation);
  yield* loadAudienceVisibleTargetThread(
    runtime,
    sourceThread,
    existing.threadId,
    `Scheduled task ${input.taskId} was not found.`,
    { includingArchived: true },
  );

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

  // Omitted `model` keeps the current selection; a plain name re-routes. On a
  // re-route, prefer the schedule's current instance, or —
  // when it is still inheriting — the target thread's instance, so a
  // multi-instance setup keeps continuity (matches create).
  let modelSelection: ModelSelection | null;
  if (input.model === undefined) {
    modelSelection = existing.modelSelection;
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
    busyPolicy: existing.busyPolicy,
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
  const invocation = yield* requireProviderInvocation;
  const runtime = yield* requireRuntime();

  const existing = yield* loadTaskById(runtime, input.taskId);
  const sourceThread = yield* loadProviderSourceThread(runtime, invocation);
  yield* loadAudienceVisibleTargetThread(
    runtime,
    sourceThread,
    existing.threadId,
    `Scheduled task ${input.taskId} was not found.`,
    { includingArchived: true },
  );

  yield* runtime.scheduledTasks
    .delete({ taskId: input.taskId })
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to delete scheduled task.")));

  return { taskId: input.taskId, deleted: true };
});

const handlers = {
  t3_spawn_subagent: spawnSubagent,
  t3_steer_subagent: steerSubagent,
  t3_subagents: subagents,
  t3_schedule_create: scheduleCreate,
  t3_schedule_list: scheduleList,
  t3_schedule_update: scheduleUpdate,
  t3_schedule_delete: scheduleDelete,
} satisfies Parameters<typeof SubagentToolkit.toLayer>[0];

export const SubagentToolkitHandlersLive = SubagentToolkit.toLayer(handlers);

const compatibilityFailure = (cause: Cause.Cause<unknown>) =>
  new McpSchema.CallToolResult({
    isError: true,
    content: [{ type: "text", text: Cause.pretty(cause) }],
  });

const compatibilitySuccess = (encodedResult: unknown) =>
  new McpSchema.CallToolResult({
    isError: false,
    structuredContent:
      typeof encodedResult === "object" && encodedResult !== null ? encodedResult : undefined,
    content: [{ type: "text", text: JSON.stringify(encodedResult) }],
  });

const peerCompatibilityInstalled = new WeakSet<object>();

/** Install mixed-version peer calls without registering legacy public tools. */
export const installPeerSubagentCompatibility = Effect.gen(function* () {
  const server = yield* McpServer.McpServer;
  if (peerCompatibilityInstalled.has(server)) return;
  peerCompatibilityInstalled.add(server);
  const callPublicTool = server.callTool;

  const callTool = ((request: Parameters<typeof callPublicTool>[0]) =>
    Effect.gen(function* () {
      const invocation = yield* McpInvocationContext.McpInvocationContext;
      if (McpInvocationContext.isProviderInvocationScope(invocation)) {
        return yield* callPublicTool(request);
      }
      if (request.name === "t3_check_subagent") {
        return yield* decodeLegacyCheckSubagentInput(request.arguments).pipe(
          Effect.flatMap(legacyPeerCheckSubagent),
          Effect.flatMap(encodeSubagentDetailOutput),
          Effect.matchCause({
            onFailure: compatibilityFailure,
            onSuccess: compatibilitySuccess,
          }),
        );
      }
      if (request.name === "t3_spawn_subagent") {
        return yield* decodeLegacySpawnSubagentInput(request.arguments).pipe(
          Effect.flatMap(spawnSubagent),
          Effect.flatMap(encodeSpawnSubagentOutput),
          Effect.matchCause({
            onFailure: compatibilityFailure,
            onSuccess: compatibilitySuccess,
          }),
        );
      }
      return yield* callPublicTool(request);
    })) as typeof callPublicTool;

  Object.assign(server, { callTool });
});

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
