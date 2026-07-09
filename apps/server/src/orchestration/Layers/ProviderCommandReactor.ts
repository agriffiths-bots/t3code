import {
  type ChatAttachment,
  CommandId,
  EventId,
  IsoDateTime,
  MessageId,
  ModelSelection as ModelSelectionSchema,
  type ModelSelection,
  type OrchestrationEvent,
  type OrchestrationThread,
  ProviderInteractionMode,
  ProviderDriverKind,
  type ProjectId,
  type OrchestrationSession,
  ThreadId,
  type ProviderSession,
  RuntimeMode as RuntimeModeSchema,
  type RuntimeMode,
  type TurnId,
} from "@t3tools/contracts";
import { isTemporaryWorktreeBranch, WORKTREE_BRANCH_PREFIX } from "@t3tools/shared/git";
import * as Cache from "effect/Cache";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";

import { resolveThreadWorkspaceCwd } from "../../checkpointing/Utils.ts";
import { increment, orchestrationEventsProcessedTotal } from "../../observability/Metrics.ts";
import { ProviderAdapterRequestError } from "../../provider/Errors.ts";
import type { ProviderServiceError } from "../../provider/Errors.ts";
import { TextGeneration } from "../../textGeneration/TextGeneration.ts";
import { ProviderService } from "../../provider/Services/ProviderService.ts";
import { ProviderRegistry } from "../../provider/Services/ProviderRegistry.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  ProviderCommandReactor,
  type ProviderCommandReactorShape,
} from "../Services/ProviderCommandReactor.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { VcsStatusBroadcaster } from "../../vcs/VcsStatusBroadcaster.ts";
import { GitWorkflowService } from "../../git/GitWorkflowService.ts";
import {
  PendingDispatchId,
  PendingDispatchRepository,
  type PendingDispatch,
} from "../../persistence/Services/PendingDispatches.ts";
import { ProjectionTurnRepository } from "../../persistence/Services/ProjectionTurns.ts";
import { ProjectionTurnRepositoryLive } from "../../persistence/Layers/ProjectionTurns.ts";
const isProviderAdapterRequestError = Schema.is(ProviderAdapterRequestError);
const isProviderDriverKind = Schema.is(ProviderDriverKind);

type ProviderCommandReactorEvent = Extract<
  OrchestrationEvent,
  {
    type:
      | "thread.runtime-mode-set"
      | "thread.turn-start-requested"
      | "thread.turn-interrupt-requested"
      | "thread.approval-response-requested"
      | "thread.user-input-response-requested"
      | "thread.session-stop-requested"
      | "thread.session-set"
      | "thread.turn-diff-completed";
  }
>;
type ProviderIntentEvent = Extract<
  ProviderCommandReactorEvent,
  {
    type:
      | "thread.runtime-mode-set"
      | "thread.turn-start-requested"
      | "thread.turn-interrupt-requested"
      | "thread.approval-response-requested"
      | "thread.user-input-response-requested"
      | "thread.session-stop-requested";
  }
>;
type TurnStartRequestedEvent = Extract<
  ProviderCommandReactorEvent,
  { type: "thread.turn-start-requested" }
>;
type SessionStopRequestedEvent = Extract<
  ProviderCommandReactorEvent,
  { type: "thread.session-stop-requested" }
>;

const QueuedThreadTurnPayload = Schema.Struct({
  messageId: MessageId,
  modelSelection: Schema.optional(ModelSelectionSchema),
  runtimeMode: Schema.optional(RuntimeModeSchema),
  interactionMode: Schema.optional(ProviderInteractionMode),
  createdAt: IsoDateTime,
  sourceSequence: Schema.optional(Schema.Number),
  providerSafeSubagentSteer: Schema.optional(Schema.Boolean),
  waitForEarlierPromptBinding: Schema.optional(Schema.Boolean),
  waitForEarlierPromptMessageIds: Schema.optional(Schema.Array(MessageId)),
});
type QueuedThreadTurnPayload = typeof QueuedThreadTurnPayload.Type;
const QueuedThreadTurnPayloadJson = Schema.fromJsonString(QueuedThreadTurnPayload);
const encodeQueuedThreadTurnPayloadJson = Schema.encodeSync(QueuedThreadTurnPayloadJson);
const decodeQueuedThreadTurnPayloadJson = Schema.decodeUnknownOption(QueuedThreadTurnPayloadJson);

interface StopRequestMarker {
  readonly sequence: number;
  readonly createdAt: string;
}

interface HistoricalThreadSessionState {
  readonly status: OrchestrationSession["status"];
  readonly activeTurnId: TurnId | null;
}

const stopRequestSupersedesSequence = (
  stopRequest: StopRequestMarker,
  sourceSequence: number | undefined,
): boolean => sourceSequence === undefined || stopRequest.sequence > sourceSequence;

function historicalSessionBlocksImmediateSend(
  session: HistoricalThreadSessionState | undefined,
): boolean {
  if (session === undefined) {
    return false;
  }
  return (
    session.activeTurnId !== null || session.status === "starting" || session.status === "running"
  );
}

function toNonEmptyProviderInput(value: string | undefined): string | undefined {
  const normalized = value?.trim();
  return normalized && normalized.length > 0 ? normalized : undefined;
}

function mapProviderSessionStatusToOrchestrationStatus(
  status: "connecting" | "ready" | "running" | "waiting" | "error" | "closed",
): OrchestrationSession["status"] {
  switch (status) {
    case "connecting":
      return "starting";
    case "running":
      return "running";
    case "waiting":
      return "waiting";
    case "error":
      return "error";
    case "closed":
      return "stopped";
    case "ready":
    default:
      return "ready";
  }
}

const turnStartKeyForEvent = (event: TurnStartRequestedEvent): string =>
  event.commandId !== null ? `command:${event.commandId}` : `event:${event.eventId}`;

const HANDLED_TURN_START_KEY_MAX = 10_000;
const HANDLED_TURN_START_KEY_TTL = Duration.minutes(30);
const DEFAULT_RUNTIME_MODE: RuntimeMode = "full-access";
const DEFAULT_THREAD_TITLE = "New thread";
const THREAD_TURN_PENDING_KIND = "thread_turn" as const;
const PENDING_SAME_TURN_STEER_MARKER_PREFIX = "pending-same-turn-steer:";

const pendingThreadTurnIdForMessage = (messageId: MessageId): PendingDispatchId =>
  PendingDispatchId.make(`thread-turn:${messageId}`);

const isProviderSafeSubagentSteerTurnStart = (event: TurnStartRequestedEvent): boolean =>
  event.commandId !== null &&
  String(event.commandId).startsWith("server:subagent-steer-immediate:");

const pendingSameTurnSteerMarkerMessageId = (turnId: TurnId): MessageId | null => {
  const rawTurnId = String(turnId);
  return rawTurnId.startsWith(PENDING_SAME_TURN_STEER_MARKER_PREFIX)
    ? MessageId.make(rawTurnId.slice(PENDING_SAME_TURN_STEER_MARKER_PREFIX.length))
    : null;
};

const isPendingSameTurnSteerMarkerTurnId = (turnId: TurnId): boolean =>
  pendingSameTurnSteerMarkerMessageId(turnId) !== null;

const pendingThreadTurnRowForEvent = (
  event: TurnStartRequestedEvent,
  options?: {
    readonly waitForEarlierPromptBinding?: boolean;
    readonly waitForEarlierPromptMessageIds?: ReadonlyArray<MessageId>;
  },
): PendingDispatch => {
  const payload = event.payload;
  return {
    id: pendingThreadTurnIdForMessage(payload.messageId),
    kind: THREAD_TURN_PENDING_KIND,
    targetThreadId: payload.threadId,
    sourceChildId: null,
    text: encodeQueuedThreadTurnPayloadJson({
      messageId: payload.messageId,
      ...(payload.modelSelection !== undefined ? { modelSelection: payload.modelSelection } : {}),
      runtimeMode: payload.runtimeMode,
      interactionMode: payload.interactionMode,
      createdAt: payload.createdAt,
      sourceSequence: event.sequence,
      ...(isProviderSafeSubagentSteerTurnStart(event) ? { providerSafeSubagentSteer: true } : {}),
      ...(options?.waitForEarlierPromptBinding === true
        ? { waitForEarlierPromptBinding: true }
        : {}),
      ...(options?.waitForEarlierPromptMessageIds !== undefined &&
      options.waitForEarlierPromptMessageIds.length > 0
        ? { waitForEarlierPromptMessageIds: [...options.waitForEarlierPromptMessageIds] }
        : {}),
    }),
    error: null,
    status: null,
    commandId: null,
    deliveredByWait: false,
    waitCancellable: false,
    createdAt: IsoDateTime.make(payload.createdAt),
  };
};

function parseQueuedThreadTurnPayload(row: PendingDispatch): QueuedThreadTurnPayload | null {
  if (row.text === null) {
    return null;
  }
  return Option.getOrNull(decodeQueuedThreadTurnPayloadJson(row.text));
}

function stopRequestSupersedesQueuedRow(
  stopRequest: StopRequestMarker,
  row: PendingDispatch,
): boolean {
  const payload = parseQueuedThreadTurnPayload(row);
  return payload === null || stopRequestSupersedesSequence(stopRequest, payload.sourceSequence);
}

function failedProviderTurnStartMessageIds(thread: OrchestrationThread): ReadonlySet<string> {
  const ids = new Set<string>();
  const pendingPromptMessages = thread.messages.filter(
    (message) => message.turnId === null && (message.role === "user" || message.role === "system"),
  );
  const failedActivities = thread.activities
    .filter((activity) => activity.kind === "provider.turn.start.failed")
    .toSorted(
      (left, right) =>
        left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
    );

  for (const activity of failedActivities) {
    if (activity.kind !== "provider.turn.start.failed") {
      continue;
    }
    const payload =
      typeof activity.payload === "object" && activity.payload !== null
        ? (activity.payload as Record<string, unknown>)
        : null;
    if (typeof payload?.messageId === "string") {
      ids.add(payload.messageId);
      continue;
    }
    const legacyPrompt = pendingPromptMessages.find(
      (message) => !ids.has(message.id) && message.createdAt <= activity.createdAt,
    );
    if (legacyPrompt !== undefined) {
      ids.add(legacyPrompt.id);
    }
  }
  return ids;
}

function queuedThreadTurnRowsInStartOrder(rows: ReadonlyArray<PendingDispatch>): ReadonlyArray<{
  readonly row: PendingDispatch;
  readonly payload: QueuedThreadTurnPayload;
  readonly rowOrder: number;
}> {
  return rows
    .flatMap((row, rowOrder) => {
      const payload = parseQueuedThreadTurnPayload(row);
      return payload === null ? [] : [{ row, payload, rowOrder }];
    })
    .toSorted((left, right) => {
      if (
        left.payload.sourceSequence !== undefined &&
        right.payload.sourceSequence !== undefined &&
        left.payload.sourceSequence !== right.payload.sourceSequence
      ) {
        return left.payload.sourceSequence - right.payload.sourceSequence;
      }
      return left.rowOrder - right.rowOrder;
    });
}

function hasUnresolvedPromptMessages(
  thread: OrchestrationThread,
  messageIds: ReadonlyArray<MessageId>,
): boolean {
  const failedMessageIds = failedProviderTurnStartMessageIds(thread);
  for (const messageId of messageIds) {
    const message = thread.messages.find((candidate) => candidate.id === messageId);
    if (
      message !== undefined &&
      message.turnId === null &&
      (message.role === "user" || message.role === "system") &&
      !failedMessageIds.has(message.id)
    ) {
      return true;
    }
  }
  return false;
}

function hasEarlierUnresolvedQueuedThreadTurn(input: {
  readonly thread: OrchestrationThread;
  readonly rows: ReadonlyArray<PendingDispatch>;
  readonly rowId: PendingDispatchId;
  readonly waitForEarlierPromptMessageIds?: ReadonlyArray<MessageId>;
}): boolean {
  if (
    input.waitForEarlierPromptMessageIds !== undefined &&
    input.waitForEarlierPromptMessageIds.length > 0
  ) {
    return hasUnresolvedPromptMessages(input.thread, input.waitForEarlierPromptMessageIds);
  }

  const failedMessageIds = failedProviderTurnStartMessageIds(input.thread);
  for (const entry of queuedThreadTurnRowsInStartOrder(input.rows)) {
    if (entry.row.id === input.rowId) {
      return false;
    }
    const message = input.thread.messages.find(
      (candidate) => candidate.id === entry.payload.messageId,
    );
    if (
      message !== undefined &&
      message.turnId === null &&
      (message.role === "user" || message.role === "system") &&
      !failedMessageIds.has(message.id)
    ) {
      return true;
    }
  }
  return false;
}

function isThreadProviderIdle(thread: OrchestrationThread): boolean {
  if (thread.latestTurn?.state === "running") {
    return false;
  }
  const session = thread.session;
  if (session === null) {
    return true;
  }
  return (
    (session.status === "ready" ||
      session.status === "waiting" ||
      session.status === "stopped" ||
      session.status === "error") &&
    session.activeTurnId === null
  );
}

function canAutoDrainQueuedThreadTurns(thread: OrchestrationThread): boolean {
  if (thread.session?.status === "stopped") {
    return false;
  }
  return isThreadProviderIdle(thread);
}

function normalizeWorktreeIdentityPath(value: string | null | undefined): string | null {
  const trimmed = value?.trim();
  if (!trimmed) return null;
  const normalized = trimmed.replace(/[\\/]+$/, "");
  return normalized.length > 0 ? normalized : trimmed;
}

function worktreeIdentityPath(input: {
  readonly worktreePath: string | null | undefined;
  readonly worktreeRemovalPath?: string | null | undefined;
}): string | null {
  return normalizeWorktreeIdentityPath(input.worktreeRemovalPath ?? input.worktreePath);
}

export function providerErrorLabel(value: string | undefined): string {
  const normalized = value?.trim();
  return normalized && normalized.length > 0 ? normalized : "unknown";
}

export function providerErrorLabelFromInstanceHint(input: {
  readonly instanceId?: string | undefined;
  readonly modelSelectionInstanceId?: string | undefined;
  readonly sessionProvider?: string | undefined;
}): string {
  return providerErrorLabel(
    input.instanceId ?? input.modelSelectionInstanceId ?? input.sessionProvider,
  );
}

function canReplaceThreadTitle(currentTitle: string, titleSeed?: string): boolean {
  const trimmedCurrentTitle = currentTitle.trim();
  if (trimmedCurrentTitle === DEFAULT_THREAD_TITLE) {
    return true;
  }

  const trimmedTitleSeed = titleSeed?.trim();
  return trimmedTitleSeed !== undefined && trimmedTitleSeed.length > 0
    ? trimmedCurrentTitle === trimmedTitleSeed
    : false;
}

function findProviderAdapterRequestError(
  cause: Cause.Cause<ProviderServiceError>,
): ProviderAdapterRequestError | undefined {
  const failReason = cause.reasons.find(Cause.isFailReason);
  return isProviderAdapterRequestError(failReason?.error) ? failReason.error : undefined;
}

function isUnknownPendingApprovalRequestError(cause: Cause.Cause<ProviderServiceError>): boolean {
  const error = findProviderAdapterRequestError(cause);
  if (error) {
    const detail = error.detail.toLowerCase();
    return (
      detail.includes("unknown pending approval request") ||
      detail.includes("unknown pending permission request")
    );
  }
  const message = Cause.pretty(cause);
  return (
    message.includes("unknown pending approval request") ||
    message.includes("unknown pending permission request")
  );
}

function isUnknownPendingUserInputRequestError(cause: Cause.Cause<ProviderServiceError>): boolean {
  const error = findProviderAdapterRequestError(cause);
  if (error) {
    const detail = error.detail.toLowerCase();
    return (
      detail.includes("unknown pending user-input request") ||
      detail.includes("unknown pending user input request") ||
      detail.includes("unknown pending codex user input request")
    );
  }
  const message = Cause.pretty(cause).toLowerCase();
  return (
    message.includes("unknown pending user-input request") ||
    message.includes("unknown pending user input request") ||
    message.includes("unknown pending codex user input request")
  );
}

function stalePendingRequestDetail(
  requestKind: "approval" | "user-input",
  requestId: string,
): string {
  return `Stale pending ${requestKind} request: ${requestId}. Provider callback state does not survive app restarts or recovered sessions. Restart the turn to continue.`;
}

function buildGeneratedWorktreeBranchName(raw: string): string {
  const normalized = raw
    .trim()
    .toLowerCase()
    .replace(/^refs\/heads\//, "")
    .replace(/['"`]/g, "");

  const withoutPrefix = normalized.startsWith(`${WORKTREE_BRANCH_PREFIX}/`)
    ? normalized.slice(`${WORKTREE_BRANCH_PREFIX}/`.length)
    : normalized;

  const branchFragment = withoutPrefix
    .replace(/[^a-z0-9/_-]+/g, "-")
    .replace(/\/+/g, "/")
    .replace(/-+/g, "-")
    .replace(/^[./_-]+|[./_-]+$/g, "")
    .slice(0, 64)
    .replace(/[./_-]+$/g, "");

  const safeFragment = branchFragment.length > 0 ? branchFragment : "update";
  return `${WORKTREE_BRANCH_PREFIX}/${safeFragment}`;
}

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const orchestrationEngine = yield* OrchestrationEngineService;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const providerService = yield* ProviderService;
  const providerRegistry = yield* ProviderRegistry;
  const gitWorkflow = yield* GitWorkflowService;
  const vcsStatusBroadcaster = yield* VcsStatusBroadcaster;
  const textGeneration = yield* TextGeneration;
  const serverSettingsService = yield* ServerSettingsService;
  const pendingDispatches = yield* PendingDispatchRepository;
  const projectionTurns = yield* ProjectionTurnRepository;
  const serverCommandId = (tag: string) =>
    crypto.randomUUIDv4.pipe(Effect.map((uuid) => CommandId.make(`server:${tag}:${uuid}`)));
  const serverEventId = () => crypto.randomUUIDv4.pipe(Effect.map(EventId.make));
  const handledTurnStartKeys = yield* Cache.make<string, true>({
    capacity: HANDLED_TURN_START_KEY_MAX,
    timeToLive: HANDLED_TURN_START_KEY_TTL,
    lookup: () => Effect.succeed(true),
  });

  const wasTurnStartHandledRecently = (key: string) =>
    Cache.getOption(handledTurnStartKeys, key).pipe(Effect.map(Option.isSome));
  const markTurnStartHandledRecently = (key: string) => Cache.set(handledTurnStartKeys, key, true);
  const claimTurnStartHandling = (key: string) =>
    wasTurnStartHandledRecently(key).pipe(
      Effect.flatMap((handled) =>
        handled ? Effect.succeed(true) : markTurnStartHandledRecently(key).pipe(Effect.as(false)),
      ),
    );
  const providerDriverForInstance = (instanceId: ModelSelection["instanceId"]) =>
    providerService.getInstanceInfo(instanceId).pipe(
      Effect.map((info) => (isProviderDriverKind(info.driverKind) ? info.driverKind : null)),
      Effect.catchCause(() => Effect.succeed(null)),
    );

  const threadModelSelections = new Map<string, ModelSelection>();
  const inFlightThreadTurnDispatchIds = new Set<string>();
  const canceledThreadTurnDispatchIds = new Set<string>();
  const inFlightThreadTurnFibers = new Map<string, Fiber.Fiber<void, never>>();
  const inFlightImmediateThreadTurnThreadIds = new Set<string>();
  const stopRequestByThreadId = new Map<string, StopRequestMarker>();

  const claimThreadTurnDispatch = (rowId: PendingDispatchId) =>
    Effect.sync(() => {
      if (inFlightThreadTurnDispatchIds.has(rowId)) {
        return false;
      }
      inFlightThreadTurnDispatchIds.add(rowId);
      return true;
    });

  const releaseThreadTurnDispatchClaim = (rowId: PendingDispatchId) =>
    Effect.sync(() => {
      inFlightThreadTurnDispatchIds.delete(rowId);
      inFlightThreadTurnFibers.delete(rowId);
    });

  const appendProviderFailureActivity = (input: {
    readonly threadId: ThreadId;
    readonly kind:
      | "provider.turn.start.failed"
      | "provider.turn.interrupt.failed"
      | "provider.approval.respond.failed"
      | "provider.user-input.respond.failed"
      | "provider.session.stop.failed";
    readonly summary: string;
    readonly detail: string;
    readonly turnId: TurnId | null;
    readonly createdAt: string;
    readonly requestId?: string;
    readonly messageId?: MessageId;
  }) =>
    Effect.all({
      commandId: serverCommandId("provider-failure-activity"),
      eventId: serverEventId(),
    }).pipe(
      Effect.flatMap(({ commandId, eventId }) =>
        orchestrationEngine.dispatch({
          type: "thread.activity.append",
          commandId,
          threadId: input.threadId,
          activity: {
            id: eventId,
            tone: "error",
            kind: input.kind,
            summary: input.summary,
            payload: {
              detail: input.detail,
              ...(input.requestId ? { requestId: input.requestId } : {}),
              ...(input.messageId !== undefined ? { messageId: input.messageId } : {}),
            },
            turnId: input.turnId,
            createdAt: input.createdAt,
          },
          createdAt: input.createdAt,
        }),
      ),
    );

  const appendQueuedTurnCanceledActivity = (input: {
    readonly threadId: ThreadId;
    readonly messageId: MessageId;
    readonly createdAt: string;
  }) =>
    Effect.all({
      commandId: serverCommandId("queued-turn-canceled"),
      eventId: serverEventId(),
    }).pipe(
      Effect.flatMap(({ commandId, eventId }) =>
        orchestrationEngine.dispatch({
          type: "thread.activity.append",
          commandId,
          threadId: input.threadId,
          activity: {
            id: eventId,
            tone: "info",
            kind: "provider.turn.start.failed",
            summary: "Queued turn canceled",
            payload: {
              detail:
                "Queued prompt was canceled because the session was stopped before it could run.",
              messageId: input.messageId,
              canceled: true,
            },
            turnId: null,
            createdAt: input.createdAt,
          },
          createdAt: input.createdAt,
        }),
      ),
      Effect.asVoid,
    );

  const formatFailureDetail = (cause: Cause.Cause<unknown>): string => {
    const failReason = cause.reasons.find(Cause.isFailReason);
    const providerError = isProviderAdapterRequestError(failReason?.error)
      ? failReason.error
      : undefined;
    if (providerError) {
      return providerError.detail;
    }
    return Cause.pretty(cause);
  };

  const setThreadSession = (input: {
    readonly threadId: ThreadId;
    readonly session: OrchestrationSession;
    readonly createdAt: string;
  }) =>
    serverCommandId("provider-session-set").pipe(
      Effect.flatMap((commandId) =>
        orchestrationEngine.dispatch({
          type: "thread.session.set",
          commandId,
          threadId: input.threadId,
          session: input.session,
          createdAt: input.createdAt,
        }),
      ),
    );

  const setThreadSessionErrorOnTurnStartFailure = Effect.fnUntraced(function* (input: {
    readonly threadId: ThreadId;
    readonly detail: string;
    readonly modelSelection?: ModelSelection;
    readonly runtimeMode?: RuntimeMode;
    readonly messageId?: MessageId;
    readonly createdAt: string;
  }) {
    const thread = yield* resolveThread(input.threadId);
    const session = thread?.session;
    const failureModelSelection = input.modelSelection ?? thread?.modelSelection;
    const failureProviderName = failureModelSelection
      ? yield* providerDriverForInstance(failureModelSelection.instanceId)
      : null;
    if (!session) {
      if (!thread) {
        return;
      }
      const modelSelection = failureModelSelection ?? thread.modelSelection;
      const providerName =
        failureModelSelection === undefined
          ? yield* providerDriverForInstance(modelSelection.instanceId)
          : failureProviderName;
      yield* setThreadSession({
        threadId: input.threadId,
        session: {
          threadId: input.threadId,
          status: "error",
          providerName,
          providerInstanceId: modelSelection.instanceId,
          runtimeMode: input.runtimeMode ?? thread.runtimeMode,
          activeTurnId: null,
          lastError: input.detail,
          updatedAt: input.createdAt,
        },
        createdAt: input.createdAt,
      });
      return;
    }
    const isUnstartedSyntheticSession = isUnstartedSyntheticErrorSession(thread);
    yield* setThreadSession({
      threadId: input.threadId,
      session: {
        ...session,
        status:
          session.status === "stopped" && !isUnstartedSyntheticSession
            ? "stopped"
            : isUnstartedSyntheticSession
              ? "error"
              : "ready",
        ...(isUnstartedSyntheticSession
          ? {
              providerName: failureProviderName,
              ...(input.modelSelection !== undefined
                ? { providerInstanceId: input.modelSelection.instanceId }
                : {}),
              ...(input.runtimeMode !== undefined ? { runtimeMode: input.runtimeMode } : {}),
            }
          : {}),
        activeTurnId: null,
        lastError:
          session.status === "stopped" && !isUnstartedSyntheticSession
            ? session.lastError
            : input.detail,
        updatedAt: input.createdAt,
      },
      createdAt: input.createdAt,
    });
  });

  const resolveProject = Effect.fnUntraced(function* (projectId: ProjectId) {
    return yield* projectionSnapshotQuery
      .getProjectShellById(projectId)
      .pipe(Effect.map(Option.getOrUndefined));
  });

  const resolveThread = Effect.fnUntraced(function* (threadId: ThreadId) {
    return yield* projectionSnapshotQuery
      .getThreadDetailById(threadId)
      .pipe(Effect.map(Option.getOrUndefined));
  });

  const isUnstartedSyntheticErrorSession = (thread: {
    readonly latestTurn?: { readonly startedAt: string | null } | null;
    readonly session: { readonly lastError?: string | null; readonly status: string } | null;
  }) =>
    !thread.latestTurn?.startedAt &&
    (thread.session?.status === "error" ||
      (thread.session?.status === "stopped" &&
        typeof thread.session.lastError === "string" &&
        thread.session.lastError.length > 0));

  const resolveActiveSession = (threadId: ThreadId) =>
    providerService
      .listSessions()
      .pipe(Effect.map((sessions) => sessions.find((session) => session.threadId === threadId)));

  const rejectStartedThreadModelChangeIfRequired = Effect.fnUntraced(function* (input: {
    readonly threadId: ThreadId;
    readonly currentModelSelection: ModelSelection;
    readonly requestedModelSelection: ModelSelection | undefined;
  }) {
    const requestedModelSelection = input.requestedModelSelection;
    if (
      requestedModelSelection === undefined ||
      (input.currentModelSelection.instanceId === requestedModelSelection.instanceId &&
        input.currentModelSelection.model === requestedModelSelection.model)
    ) {
      return;
    }
    const providers = yield* providerRegistry.getProviders;
    const requiresNewThread =
      providers.find((snapshot) => snapshot.instanceId === input.currentModelSelection.instanceId)
        ?.requiresNewThreadForModelChange === true ||
      providers.find((snapshot) => snapshot.instanceId === requestedModelSelection.instanceId)
        ?.requiresNewThreadForModelChange === true;
    if (!requiresNewThread) {
      return;
    }
    return yield* new ProviderAdapterRequestError({
      provider: providerErrorLabelFromInstanceHint({
        instanceId: String(requestedModelSelection.instanceId),
        modelSelectionInstanceId: String(input.currentModelSelection.instanceId),
      }),
      method: "thread.turn.start",
      detail: `Thread '${input.threadId}' cannot switch models after the conversation has started. Start a new thread to use '${requestedModelSelection.model}'.`,
    });
  });

  const ensureSessionForThread = Effect.fn("ensureSessionForThread")(function* (
    threadId: ThreadId,
    createdAt: string,
    options?: {
      readonly modelSelection?: ModelSelection;
      readonly runtimeMode?: RuntimeMode;
    },
  ) {
    const thread = yield* resolveThread(threadId);
    if (!thread) {
      return yield* Effect.die(new Error(`Thread '${threadId}' was not found in read model.`));
    }

    const desiredRuntimeMode = options?.runtimeMode ?? thread.runtimeMode;
    const requestedModelSelection = options?.modelSelection;

    const activeSession = yield* resolveActiveSession(threadId);
    const activeThreadSession =
      thread.session !== null && thread.session.status !== "stopped" && activeSession
        ? thread.session
        : null;
    const hasEstablishedProviderBinding =
      thread.session !== null &&
      (activeSession !== undefined ||
        Boolean(thread.latestTurn?.startedAt) ||
        (thread.session.status === "stopped" && !isUnstartedSyntheticErrorSession(thread)));
    if (
      activeThreadSession !== null &&
      activeSession !== undefined &&
      (activeThreadSession.providerInstanceId === undefined ||
        activeSession.providerInstanceId === undefined)
    ) {
      return yield* new ProviderAdapterRequestError({
        provider: providerErrorLabel(activeThreadSession.providerName ?? undefined),
        method: "thread.turn.start",
        detail: `Thread '${threadId}' has an active provider session without a provider instance id.`,
      });
    }
    const currentInstanceId =
      activeThreadSession !== null &&
      activeSession !== undefined &&
      activeSession.providerInstanceId !== undefined
        ? activeSession.providerInstanceId
        : thread.modelSelection.instanceId;
    const desiredModelSelection = requestedModelSelection ?? thread.modelSelection;
    const desiredInstanceId = desiredModelSelection.instanceId;
    const currentInfo = yield* providerService.getInstanceInfo(currentInstanceId).pipe(
      Effect.mapError(
        () =>
          new ProviderAdapterRequestError({
            provider: providerErrorLabelFromInstanceHint({
              instanceId: String(currentInstanceId),
              modelSelectionInstanceId: String(thread.modelSelection.instanceId),
              sessionProvider: thread.session?.providerName ?? undefined,
            }),
            method: "thread.turn.start",
            detail: `Thread '${threadId}' references unknown provider instance '${currentInstanceId}'. The instance is not configured in this build.`,
          }),
      ),
    );
    const desiredInfo = yield* providerService.getInstanceInfo(desiredInstanceId).pipe(
      Effect.mapError(
        () =>
          new ProviderAdapterRequestError({
            provider: providerErrorLabelFromInstanceHint({
              instanceId: String(desiredModelSelection.instanceId),
            }),
            method: "thread.turn.start",
            detail: `Requested provider instance '${desiredInstanceId}' is not configured in this build.`,
          }),
      ),
    );
    const desiredDriverKind = desiredInfo.driverKind;
    if (!isProviderDriverKind(desiredDriverKind)) {
      return yield* new ProviderAdapterRequestError({
        provider: providerErrorLabel(String(desiredDriverKind)),
        method: "thread.turn.start",
        detail: `Requested provider instance '${desiredInstanceId}' uses unknown provider driver '${desiredDriverKind}'. The driver is not installed in this build.`,
      });
    }
    const preferredProvider: ProviderDriverKind = desiredDriverKind;
    if (hasEstablishedProviderBinding) {
      yield* rejectStartedThreadModelChangeIfRequired({
        threadId,
        currentModelSelection:
          activeSession?.model !== undefined
            ? {
                ...thread.modelSelection,
                instanceId: currentInstanceId,
                model: activeSession.model,
              }
            : thread.modelSelection,
        requestedModelSelection,
      });
    }
    if (
      hasEstablishedProviderBinding &&
      requestedModelSelection !== undefined &&
      requestedModelSelection.instanceId !== currentInstanceId
    ) {
      if (currentInfo.driverKind !== desiredInfo.driverKind) {
        return yield* new ProviderAdapterRequestError({
          provider: preferredProvider,
          method: "thread.turn.start",
          detail: `Thread '${threadId}' is bound to driver '${currentInfo.driverKind}' and cannot switch to '${desiredInfo.driverKind}'.`,
        });
      }
      if (
        currentInfo.continuationIdentity.continuationKey !==
        desiredInfo.continuationIdentity.continuationKey
      ) {
        return yield* new ProviderAdapterRequestError({
          provider: preferredProvider,
          method: "thread.turn.start",
          detail: `Thread '${threadId}' cannot switch from instance '${currentInstanceId}' to '${desiredInstanceId}' because their provider resume state is incompatible.`,
        });
      }
    }
    const project = yield* resolveProject(thread.projectId);
    const effectiveCwd = resolveThreadWorkspaceCwd({
      thread,
      projects: project ? [project] : [],
    });

    const startProviderSession = (input?: {
      readonly resumeCursor?: unknown;
      readonly activeTurnId?: TurnId;
      readonly provider?: ProviderDriverKind;
    }) =>
      providerService.startSession(threadId, {
        threadId,
        ...(preferredProvider ? { provider: preferredProvider } : {}),
        providerInstanceId: desiredInstanceId,
        ...(effectiveCwd ? { cwd: effectiveCwd } : {}),
        modelSelection: desiredModelSelection,
        ...(input?.resumeCursor !== undefined ? { resumeCursor: input.resumeCursor } : {}),
        ...(input?.activeTurnId !== undefined ? { activeTurnId: input.activeTurnId } : {}),
        runtimeMode: desiredRuntimeMode,
      });

    const bindSessionToThread = (session: ProviderSession) =>
      Effect.gen(function* () {
        if (session.providerInstanceId === undefined) {
          return yield* new ProviderAdapterRequestError({
            provider: providerErrorLabel(session.provider),
            method: "thread.turn.start",
            detail: `Provider session '${session.threadId}' started without a provider instance id.`,
          });
        }
        yield* setThreadSession({
          threadId,
          session: {
            threadId,
            status: mapProviderSessionStatusToOrchestrationStatus(session.status),
            providerName: session.provider,
            providerInstanceId: session.providerInstanceId,
            runtimeMode: desiredRuntimeMode,
            // Preserve only active orchestration turn ids carried by provider sessions.
            activeTurnId:
              session.status === "running" && session.activeTurnId !== undefined
                ? session.activeTurnId
                : null,
            lastError: session.lastError ?? null,
            updatedAt: session.updatedAt,
          },
          createdAt,
        });
      });

    const existingSessionThreadId =
      thread.session && thread.session.status !== "stopped" && activeSession ? thread.id : null;
    if (existingSessionThreadId) {
      const runtimeModeChanged = desiredRuntimeMode !== thread.session?.runtimeMode;
      const cwdChanged = effectiveCwd !== activeSession?.cwd;
      const sessionModelSwitch = (yield* providerService.getCapabilities(desiredInstanceId))
        .sessionModelSwitch;
      const modelChanged =
        requestedModelSelection !== undefined &&
        requestedModelSelection.model !== activeSession?.model;
      const instanceChanged =
        requestedModelSelection !== undefined &&
        activeSession?.providerInstanceId !== requestedModelSelection.instanceId;
      const shouldRestartForModelChange = modelChanged && sessionModelSwitch === "unsupported";
      const previousModelSelection = threadModelSelections.get(threadId);
      const shouldRestartForModelSelectionChange =
        preferredProvider === "claudeAgent" &&
        requestedModelSelection !== undefined &&
        !Equal.equals(previousModelSelection, requestedModelSelection);
      const shouldRestartWaitingSession =
        activeSession?.status === "waiting" && activeSession.activeTurnId === undefined;

      if (
        !runtimeModeChanged &&
        !cwdChanged &&
        !instanceChanged &&
        !shouldRestartForModelChange &&
        !shouldRestartForModelSelectionChange &&
        !shouldRestartWaitingSession
      ) {
        return existingSessionThreadId;
      }

      const resumeCursor = shouldRestartForModelChange
        ? undefined
        : (activeSession?.resumeCursor ?? undefined);
      const restartActiveTurnId =
        activeSession?.status === "running" ? activeSession.activeTurnId : undefined;
      yield* Effect.logInfo("provider command reactor restarting provider session", {
        threadId,
        existingSessionThreadId,
        currentProvider: activeSession?.provider,
        currentInstanceId,
        desiredInstanceId,
        desiredProvider: desiredModelSelection.instanceId,
        currentRuntimeMode: thread.session?.runtimeMode,
        desiredRuntimeMode,
        runtimeModeChanged,
        previousCwd: activeSession?.cwd,
        desiredCwd: effectiveCwd,
        cwdChanged,
        modelChanged,
        instanceChanged,
        shouldRestartForModelChange,
        shouldRestartForModelSelectionChange,
        hasResumeCursor: resumeCursor !== undefined,
        hasActiveTurn: restartActiveTurnId !== undefined,
      });
      const restartedSession = yield* startProviderSession(
        resumeCursor !== undefined
          ? {
              resumeCursor,
              ...(restartActiveTurnId !== undefined ? { activeTurnId: restartActiveTurnId } : {}),
            }
          : undefined,
      );
      yield* Effect.logInfo("provider command reactor restarted provider session", {
        threadId,
        previousSessionId: existingSessionThreadId,
        restartedSessionThreadId: restartedSession.threadId,
        provider: restartedSession.provider,
        runtimeMode: restartedSession.runtimeMode,
        cwd: restartedSession.cwd,
      });
      yield* bindSessionToThread(restartedSession);
      return restartedSession.threadId;
    }

    const startedSession = yield* startProviderSession(undefined);
    yield* bindSessionToThread(startedSession);
    return startedSession.threadId;
  });

  const buildSendTurnRequestForThread = Effect.fnUntraced(function* (input: {
    readonly threadId: ThreadId;
    readonly messageText: string;
    readonly attachments?: ReadonlyArray<ChatAttachment>;
    readonly modelSelection?: ModelSelection;
    readonly runtimeMode?: RuntimeMode;
    readonly interactionMode?: "default" | "plan";
    readonly createdAt: string;
  }) {
    const thread = yield* resolveThread(input.threadId);
    if (!thread) {
      return yield* Effect.die(
        new Error(`Thread '${input.threadId}' was not found in read model.`),
      );
    }
    yield* ensureSessionForThread(input.threadId, input.createdAt, {
      ...(input.modelSelection !== undefined ? { modelSelection: input.modelSelection } : {}),
      ...(input.runtimeMode !== undefined ? { runtimeMode: input.runtimeMode } : {}),
    });
    if (input.modelSelection !== undefined) {
      threadModelSelections.set(input.threadId, input.modelSelection);
    }
    const normalizedInput = toNonEmptyProviderInput(input.messageText);
    const normalizedAttachments = input.attachments ?? [];
    const activeSession = yield* providerService
      .listSessions()
      .pipe(
        Effect.map((sessions) => sessions.find((session) => session.threadId === input.threadId)),
      );
    const sessionModelSwitch =
      activeSession === undefined
        ? "in-session"
        : activeSession.providerInstanceId === undefined
          ? yield* new ProviderAdapterRequestError({
              provider: providerErrorLabel(activeSession.provider),
              method: "thread.turn.start",
              detail: `Active provider session '${activeSession.threadId}' is missing a provider instance id.`,
            })
          : (yield* providerService.getCapabilities(activeSession.providerInstanceId))
              .sessionModelSwitch;
    const requestedModelSelection =
      input.modelSelection ?? threadModelSelections.get(input.threadId) ?? thread.modelSelection;
    const modelForTurn =
      sessionModelSwitch === "unsupported" && input.modelSelection === undefined
        ? activeSession?.model !== undefined
          ? {
              ...requestedModelSelection,
              model: activeSession.model,
            }
          : requestedModelSelection
        : input.modelSelection;

    return {
      threadId: input.threadId,
      ...(normalizedInput ? { input: normalizedInput } : {}),
      ...(normalizedAttachments.length > 0 ? { attachments: normalizedAttachments } : {}),
      ...(modelForTurn !== undefined ? { modelSelection: modelForTurn } : {}),
      ...(input.interactionMode !== undefined ? { interactionMode: input.interactionMode } : {}),
    };
  });

  const maybeGenerateAndRenameWorktreeBranchForFirstTurn = Effect.fn(
    "maybeGenerateAndRenameWorktreeBranchForFirstTurn",
  )(function* (input: {
    readonly threadId: ThreadId;
    readonly branch: string | null;
    readonly worktreePath: string | null;
    readonly messageText: string;
    readonly attachments?: ReadonlyArray<ChatAttachment>;
  }) {
    if (!input.branch || !input.worktreePath) {
      return;
    }
    if (!isTemporaryWorktreeBranch(input.branch)) {
      return;
    }

    const oldBranch = input.branch;
    const cwd = input.worktreePath;
    const attachments = input.attachments ?? [];
    yield* Effect.gen(function* () {
      const snapshot = yield* projectionSnapshotQuery.getSnapshot();
      const currentThread = snapshot.threads.find((thread) => thread.id === input.threadId);
      const currentWorktreeIdentity = worktreeIdentityPath({
        worktreePath: input.worktreePath,
        worktreeRemovalPath: currentThread?.worktreeRemovalPath,
      });
      if (
        snapshot.threads.some(
          (thread) =>
            thread.id !== input.threadId &&
            currentWorktreeIdentity !== null &&
            worktreeIdentityPath(thread) === currentWorktreeIdentity,
        )
      ) {
        return;
      }

      const { textGenerationModelSelection: modelSelection } =
        yield* serverSettingsService.getSettings;

      const generated = yield* textGeneration.generateBranchName({
        cwd,
        message: input.messageText,
        ...(attachments.length > 0 ? { attachments } : {}),
        modelSelection,
      });
      if (!generated) return;

      const targetBranch = buildGeneratedWorktreeBranchName(generated.branch);
      if (targetBranch === oldBranch) return;

      const renamed = yield* gitWorkflow.renameBranch({ cwd, oldBranch, newBranch: targetBranch });
      yield* orchestrationEngine.dispatch({
        type: "thread.meta.update",
        commandId: yield* serverCommandId("worktree-branch-rename"),
        threadId: input.threadId,
        branch: renamed.branch,
        worktreePath: cwd,
      });
      yield* vcsStatusBroadcaster.refreshStatus(cwd).pipe(Effect.ignoreCause({ log: true }));
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("provider command reactor failed to generate or rename worktree branch", {
          threadId: input.threadId,
          cwd,
          oldBranch,
          cause: Cause.pretty(cause),
        }),
      ),
    );
  });

  const maybeGenerateThreadTitleForFirstTurn = Effect.fn("maybeGenerateThreadTitleForFirstTurn")(
    function* (input: {
      readonly threadId: ThreadId;
      readonly cwd: string;
      readonly messageText: string;
      readonly attachments?: ReadonlyArray<ChatAttachment>;
      readonly titleSeed?: string;
    }) {
      const attachments = input.attachments ?? [];
      yield* Effect.gen(function* () {
        const { textGenerationModelSelection: modelSelection } =
          yield* serverSettingsService.getSettings;

        const generated = yield* textGeneration.generateThreadTitle({
          cwd: input.cwd,
          message: input.messageText,
          ...(attachments.length > 0 ? { attachments } : {}),
          modelSelection,
        });
        if (!generated) return;

        const thread = yield* resolveThread(input.threadId);
        if (!thread) return;
        if (!canReplaceThreadTitle(thread.title, input.titleSeed)) {
          return;
        }

        yield* orchestrationEngine.dispatch({
          type: "thread.meta.update",
          commandId: yield* serverCommandId("thread-title-rename"),
          threadId: input.threadId,
          title: generated.title,
        });
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("provider command reactor failed to generate or rename thread title", {
            threadId: input.threadId,
            cwd: input.cwd,
            cause: Cause.pretty(cause),
          }),
        ),
      );
    },
  );

  const startFirstUserTurnSideEffects = Effect.fn("startFirstUserTurnSideEffects")(
    function* (input: {
      readonly event: TurnStartRequestedEvent;
      readonly thread: OrchestrationThread;
      readonly message: OrchestrationThread["messages"][number];
    }) {
      const { event, thread, message } = input;
      const isFirstUserMessageTurn =
        message.role === "user" &&
        thread.messages.filter((entry) => entry.role === "user").length === 1;
      if (!isFirstUserMessageTurn) {
        return;
      }

      const project = yield* resolveProject(thread.projectId);
      const generationCwd =
        resolveThreadWorkspaceCwd({
          thread,
          projects: project ? [project] : [],
        }) ?? process.cwd();
      const generationInput = {
        messageText: message.text,
        ...(message.attachments !== undefined ? { attachments: message.attachments } : {}),
        ...(event.payload.titleSeed !== undefined ? { titleSeed: event.payload.titleSeed } : {}),
      };

      yield* maybeGenerateAndRenameWorktreeBranchForFirstTurn({
        threadId: event.payload.threadId,
        branch: thread.branch,
        worktreePath: thread.worktreePath,
        ...generationInput,
      }).pipe(Effect.forkScoped);

      if (canReplaceThreadTitle(thread.title, event.payload.titleSeed)) {
        yield* maybeGenerateThreadTitleForFirstTurn({
          threadId: event.payload.threadId,
          cwd: generationCwd,
          ...generationInput,
        }).pipe(Effect.forkScoped);
      }
    },
  );

  const handleTurnStartFailure = (input: {
    readonly threadId: ThreadId;
    readonly cause: Cause.Cause<unknown>;
    readonly modelSelection?: ModelSelection;
    readonly runtimeMode?: RuntimeMode;
    readonly messageId?: MessageId;
    readonly createdAt: string;
  }) => {
    if (Cause.hasInterruptsOnly(input.cause)) {
      return Effect.void;
    }
    const detail = formatFailureDetail(input.cause);
    return setThreadSessionErrorOnTurnStartFailure({
      threadId: input.threadId,
      detail,
      ...(input.modelSelection !== undefined ? { modelSelection: input.modelSelection } : {}),
      ...(input.runtimeMode !== undefined ? { runtimeMode: input.runtimeMode } : {}),
      createdAt: input.createdAt,
    }).pipe(
      Effect.flatMap(() =>
        appendProviderFailureActivity({
          threadId: input.threadId,
          kind: "provider.turn.start.failed",
          summary: "Provider turn start failed",
          detail,
          turnId: null,
          createdAt: input.createdAt,
          ...(input.messageId !== undefined ? { messageId: input.messageId } : {}),
        }),
      ),
      Effect.asVoid,
    );
  };

  const recoverQueuedTurnStartFailure = (input: {
    readonly threadId: ThreadId;
    readonly rowId: PendingDispatchId;
    readonly cause: Cause.Cause<unknown>;
    readonly modelSelection?: ModelSelection;
    readonly runtimeMode?: RuntimeMode;
    readonly messageId?: MessageId;
    readonly createdAt: string;
  }): Effect.Effect<void, never, Scope.Scope> => {
    if (Cause.hasInterruptsOnly(input.cause)) {
      return Effect.void;
    }
    return handleTurnStartFailure(input).pipe(
      Effect.catchCause((recoveryCause) =>
        Effect.logWarning("provider command reactor failed to recover queued turn start failure", {
          threadId: input.threadId,
          dispatchId: input.rowId,
          cause: Cause.pretty(recoveryCause),
          originalCause: Cause.pretty(input.cause),
        }),
      ),
      Effect.andThen(deletePendingThreadTurnRows([input.rowId])),
      Effect.andThen(drainPendingThreadTurns(input.threadId)),
    );
  };

  const enqueueThreadTurnUntilIdle = Effect.fnUntraced(function* (input: {
    readonly event: TurnStartRequestedEvent;
  }) {
    const row = pendingThreadTurnRowForEvent(input.event);
    yield* Effect.sync(() => {
      canceledThreadTurnDispatchIds.delete(row.id);
    });
    yield* pendingDispatches.insert(row).pipe(Effect.orDie);
  });

  const deletePendingThreadTurnRows = (ids: ReadonlyArray<PendingDispatchId>) =>
    pendingDispatches.deleteByIds(ids).pipe(Effect.orDie);

  const resetAndReleaseThreadTurnDispatchClaim = (rowId: PendingDispatchId) =>
    pendingDispatches
      .resetClaims({ ids: [rowId] })
      .pipe(Effect.orDie, Effect.ensuring(releaseThreadTurnDispatchClaim(rowId)));

  const cancelPendingThreadTurnRows = (input: {
    readonly threadId: ThreadId;
    readonly rows: ReadonlyArray<PendingDispatch>;
    readonly createdAt: string;
  }) =>
    Effect.gen(function* () {
      yield* Effect.sync(() => {
        for (const row of input.rows) {
          canceledThreadTurnDispatchIds.add(row.id);
        }
      });
      yield* Effect.forEach(
        input.rows,
        (row) => {
          const fiber = inFlightThreadTurnFibers.get(row.id);
          return fiber === undefined ? Effect.void : Fiber.interrupt(fiber).pipe(Effect.ignore);
        },
        { concurrency: 1, discard: true },
      );
      yield* Effect.forEach(
        input.rows,
        (row) => {
          const payload = parseQueuedThreadTurnPayload(row);
          return payload === null
            ? Effect.void
            : appendQueuedTurnCanceledActivity({
                threadId: input.threadId,
                messageId: payload.messageId,
                createdAt: input.createdAt,
              });
        },
        { concurrency: 1, discard: true },
      );
      yield* deletePendingThreadTurnRows(input.rows.map((row) => row.id));
    }).pipe(Effect.orDie);

  const partitionStoppedThreadTurnRows = (input: {
    readonly thread: OrchestrationThread | undefined;
    readonly rows: ReadonlyArray<PendingDispatch>;
    readonly stopRequest: StopRequestMarker | undefined;
  }): {
    readonly rowsToCancel: ReadonlyArray<PendingDispatch>;
    readonly rowsToDelete: ReadonlyArray<PendingDispatch>;
    readonly rowsToKeep: ReadonlyArray<PendingDispatch>;
  } => {
    const rowsToCancel: PendingDispatch[] = [];
    const rowsToDelete: PendingDispatch[] = [];
    const rowsToKeep: PendingDispatch[] = [];
    for (const row of input.rows) {
      if (
        input.stopRequest !== undefined &&
        !stopRequestSupersedesQueuedRow(input.stopRequest, row)
      ) {
        rowsToKeep.push(row);
        continue;
      }
      const payload = parseQueuedThreadTurnPayload(row);
      const message =
        payload === null
          ? undefined
          : input.thread?.messages.find((entry) => entry.id === payload.messageId);
      if (message !== undefined && message.turnId !== null) {
        rowsToDelete.push(row);
      } else {
        rowsToCancel.push(row);
      }
    }
    return { rowsToCancel, rowsToDelete, rowsToKeep };
  };

  const claimedThreadTurnCanSend = (input: {
    readonly threadId: ThreadId;
    readonly rowId: PendingDispatchId;
    readonly waitForEarlierPromptBinding?: boolean;
    readonly waitForEarlierPromptMessageIds?: ReadonlyArray<MessageId>;
    readonly sourceSequence?: number;
    readonly allowActiveProviderTurn?: boolean;
    readonly requiredActiveTurnId?: TurnId;
  }) =>
    Effect.gen(function* () {
      const stopRequest = stopRequestByThreadId.get(input.threadId);
      if (
        stopRequest !== undefined &&
        stopRequestSupersedesSequence(stopRequest, input.sourceSequence)
      ) {
        return false;
      }
      if (canceledThreadTurnDispatchIds.has(input.rowId)) {
        return false;
      }
      const thread = yield* resolveThread(input.threadId);
      if (!thread) {
        return false;
      }
      const canSendForThread =
        input.allowActiveProviderTurn === true ||
        canAutoDrainQueuedThreadTurns(thread) ||
        (thread.session?.status === "stopped" &&
          stopRequest !== undefined &&
          !stopRequestSupersedesSequence(stopRequest, input.sourceSequence));
      if (!canSendForThread) {
        return false;
      }
      const activeProviderSession = yield* resolveActiveSession(input.threadId);
      if (
        input.allowActiveProviderTurn === true &&
        input.requiredActiveTurnId !== undefined &&
        activeProviderSession?.activeTurnId !== input.requiredActiveTurnId
      ) {
        return false;
      }
      if (
        input.allowActiveProviderTurn !== true &&
        (activeProviderSession?.status === "running" ||
          activeProviderSession?.activeTurnId !== undefined)
      ) {
        return false;
      }
      const latestThread = yield* resolveThread(input.threadId);
      if (!latestThread) {
        return false;
      }
      const rows = yield* pendingDispatches
        .listByTarget({ kind: THREAD_TURN_PENDING_KIND, targetThreadId: input.threadId })
        .pipe(Effect.orDie);
      if (
        input.waitForEarlierPromptBinding === true &&
        hasEarlierUnresolvedQueuedThreadTurn({
          thread: latestThread,
          rows,
          rowId: input.rowId,
          ...(input.waitForEarlierPromptMessageIds !== undefined
            ? { waitForEarlierPromptMessageIds: input.waitForEarlierPromptMessageIds }
            : {}),
        })
      ) {
        return false;
      }
      const canSendForLatestThread =
        input.allowActiveProviderTurn === true ||
        canAutoDrainQueuedThreadTurns(latestThread) ||
        (latestThread.session?.status === "stopped" &&
          stopRequest !== undefined &&
          !stopRequestSupersedesSequence(stopRequest, input.sourceSequence));
      if (!canSendForLatestThread) {
        return false;
      }
      return (
        rows.some((row) => row.id === input.rowId) &&
        !canceledThreadTurnDispatchIds.has(input.rowId)
      );
    });

  const drainPendingThreadTurns = (threadId: ThreadId): Effect.Effect<void, never, Scope.Scope> =>
    Effect.gen(function* () {
      let rows = yield* pendingDispatches
        .listByTarget({ kind: THREAD_TURN_PENDING_KIND, targetThreadId: threadId })
        .pipe(Effect.orDie);
      if (rows.length === 0) {
        return;
      }

      const thread = yield* resolveThread(threadId);
      if (!thread) {
        yield* deletePendingThreadTurnRows(rows.map((row) => row.id));
        return;
      }
      if (thread.session?.status === "stopped") {
        const stopRequest = stopRequestByThreadId.get(threadId);
        const { rowsToCancel, rowsToDelete, rowsToKeep } = partitionStoppedThreadTurnRows({
          thread,
          rows,
          stopRequest,
        });
        if (rowsToDelete.length > 0) {
          yield* deletePendingThreadTurnRows(rowsToDelete.map((row) => row.id));
        }
        if (rowsToCancel.length > 0) {
          yield* cancelPendingThreadTurnRows({
            threadId,
            rows: rowsToCancel,
            createdAt: thread.session.updatedAt,
          });
        }
        if (rowsToKeep.length === 0) {
          return;
        }
        rows = rowsToKeep;
      }
      const claimedRows = rows.filter(
        (row) => row.commandId !== null && !inFlightThreadTurnDispatchIds.has(row.id),
      );
      if (claimedRows.length > 0) {
        const rowsToDelete: PendingDispatch[] = [];
        const rowsToRetry: PendingDispatch[] = [];
        for (const row of claimedRows) {
          const payload = parseQueuedThreadTurnPayload(row);
          if (payload === null) {
            rowsToDelete.push(row);
            continue;
          }
          const message = thread.messages.find((entry) => entry.id === payload.messageId);
          if (message !== undefined && message.turnId !== null) {
            if (payload.providerSafeSubagentSteer === true) {
              const isMarkerBound = isPendingSameTurnSteerMarkerTurnId(message.turnId);
              const activeProviderSession = isMarkerBound
                ? undefined
                : yield* resolveActiveSession(threadId);
              if (isMarkerBound || activeProviderSession?.activeTurnId === message.turnId) {
                rowsToRetry.push(row);
              } else {
                rowsToDelete.push(row);
              }
            } else {
              rowsToDelete.push(row);
            }
          } else {
            rowsToRetry.push(row);
          }
        }
        if (rowsToDelete.length > 0) {
          yield* deletePendingThreadTurnRows(rowsToDelete.map((row) => row.id));
        }
        if (rowsToRetry.length > 0) {
          yield* pendingDispatches
            .resetClaims({ ids: rowsToRetry.map((row) => row.id) })
            .pipe(Effect.orDie);
        }
        const retryIds = new Set(rowsToRetry.map((row) => row.id));
        rows = rows.filter(
          (row) =>
            row.commandId === null ||
            inFlightThreadTurnDispatchIds.has(row.id) ||
            retryIds.has(row.id),
        );
        rows = rows.map((row) =>
          retryIds.has(row.id) ? ({ ...row, commandId: null } satisfies PendingDispatch) : row,
        );
        if (rows.length === 0) {
          return;
        }
      }
      if (inFlightImmediateThreadTurnThreadIds.has(threadId)) {
        return;
      }
      const activeProviderSession = yield* resolveActiveSession(threadId);
      const hasActiveProviderTurn =
        activeProviderSession?.status === "running" ||
        activeProviderSession?.activeTurnId !== undefined;
      if (hasActiveProviderTurn) {
        rows = rows.filter(
          (row) => parseQueuedThreadTurnPayload(row)?.providerSafeSubagentSteer === true,
        );
        if (rows.length === 0) {
          return;
        }
      }
      const stopRequest = stopRequestByThreadId.get(threadId);
      const hasPostStopQueuedRow =
        thread.session?.status === "stopped" &&
        stopRequest !== undefined &&
        rows.some((row) => !stopRequestSupersedesQueuedRow(stopRequest, row));
      const canDrainProviderSafeSubagentSteer =
        hasActiveProviderTurn &&
        rows.some((row) => parseQueuedThreadTurnPayload(row)?.providerSafeSubagentSteer === true);
      if (
        !canAutoDrainQueuedThreadTurns(thread) &&
        !hasPostStopQueuedRow &&
        !canDrainProviderSafeSubagentSteer
      ) {
        return;
      }

      for (const row of rows) {
        const claimed = yield* claimThreadTurnDispatch(row.id);
        if (!claimed) {
          return;
        }

        const payload = parseQueuedThreadTurnPayload(row);
        if (payload === null) {
          yield* Effect.logWarning("dropping invalid queued provider turn payload", {
            threadId,
            dispatchId: row.id,
          });
          yield* deletePendingThreadTurnRows([row.id]).pipe(
            Effect.ensuring(releaseThreadTurnDispatchClaim(row.id)),
          );
          continue;
        }

        const message = thread.messages.find((entry) => entry.id === payload.messageId);
        if (!message || (message.role !== "user" && message.role !== "system")) {
          yield* Effect.logWarning("dropping queued provider turn for missing prompt message", {
            threadId,
            dispatchId: row.id,
            messageId: payload.messageId,
          });
          yield* deletePendingThreadTurnRows([row.id]).pipe(
            Effect.ensuring(releaseThreadTurnDispatchClaim(row.id)),
          );
          continue;
        }
        if (
          payload.providerSafeSubagentSteer === true &&
          message.turnId !== null &&
          isPendingSameTurnSteerMarkerTurnId(message.turnId)
        ) {
          yield* releaseThreadTurnDispatchClaim(row.id);
          return;
        }
        const requiredActiveTurnId =
          payload.providerSafeSubagentSteer === true && message.turnId !== null
            ? message.turnId
            : undefined;
        if (requiredActiveTurnId !== undefined) {
          const activeProviderSession = yield* resolveActiveSession(threadId);
          if (activeProviderSession?.activeTurnId !== requiredActiveTurnId) {
            yield* deletePendingThreadTurnRows([row.id]).pipe(
              Effect.ensuring(releaseThreadTurnDispatchClaim(row.id)),
            );
            continue;
          }
        }
        if (message.turnId !== null && payload.providerSafeSubagentSteer !== true) {
          yield* deletePendingThreadTurnRows([row.id]).pipe(
            Effect.ensuring(releaseThreadTurnDispatchClaim(row.id)),
          );
          continue;
        }
        const latestQueuedRows = yield* pendingDispatches
          .listByTarget({ kind: THREAD_TURN_PENDING_KIND, targetThreadId: threadId })
          .pipe(Effect.orDie);
        if (
          payload.waitForEarlierPromptBinding === true &&
          hasEarlierUnresolvedQueuedThreadTurn({
            thread,
            rows: latestQueuedRows,
            rowId: row.id,
            ...(payload.waitForEarlierPromptMessageIds !== undefined
              ? { waitForEarlierPromptMessageIds: payload.waitForEarlierPromptMessageIds }
              : {}),
          })
        ) {
          yield* releaseThreadTurnDispatchClaim(row.id);
          return;
        }

        const sendTurnRequestExit = yield* buildSendTurnRequestForThread({
          threadId,
          messageText: message.text,
          ...(message.attachments !== undefined ? { attachments: message.attachments } : {}),
          ...(payload.modelSelection !== undefined
            ? { modelSelection: payload.modelSelection }
            : {}),
          ...(payload.runtimeMode !== undefined ? { runtimeMode: payload.runtimeMode } : {}),
          ...(payload.interactionMode !== undefined
            ? { interactionMode: payload.interactionMode }
            : {}),
          createdAt: payload.createdAt,
        }).pipe(
          Effect.map(Option.some),
          Effect.catchCause((cause) =>
            recoverQueuedTurnStartFailure({
              threadId,
              rowId: row.id,
              cause,
              ...(payload.modelSelection !== undefined
                ? { modelSelection: payload.modelSelection }
                : {}),
              ...(payload.runtimeMode !== undefined ? { runtimeMode: payload.runtimeMode } : {}),
              messageId: payload.messageId,
              createdAt: payload.createdAt,
            }).pipe(Effect.as(Option.none())),
          ),
          Effect.exit,
        );

        if (Exit.isFailure(sendTurnRequestExit)) {
          yield* releaseThreadTurnDispatchClaim(row.id);
          return yield* Effect.failCause(sendTurnRequestExit.cause);
        }

        const sendTurnRequest = sendTurnRequestExit.value;
        if (Option.isNone(sendTurnRequest)) {
          yield* releaseThreadTurnDispatchClaim(row.id);
          return;
        }

        const canSend = yield* claimedThreadTurnCanSend({
          threadId,
          rowId: row.id,
          ...(payload.waitForEarlierPromptBinding === true
            ? { waitForEarlierPromptBinding: true }
            : {}),
          ...(payload.waitForEarlierPromptMessageIds !== undefined
            ? { waitForEarlierPromptMessageIds: payload.waitForEarlierPromptMessageIds }
            : {}),
          ...(payload.sourceSequence !== undefined
            ? { sourceSequence: payload.sourceSequence }
            : {}),
          ...(payload.providerSafeSubagentSteer === true ? { allowActiveProviderTurn: true } : {}),
          ...(requiredActiveTurnId !== undefined ? { requiredActiveTurnId } : {}),
        });
        if (!canSend) {
          yield* releaseThreadTurnDispatchClaim(row.id);
          return;
        }

        const claimCommandId = yield* serverCommandId("queued-turn-send");
        yield* pendingDispatches
          .claim({ ids: [row.id], commandId: claimCommandId })
          .pipe(Effect.orDie);
        const canSendAfterClaim = yield* claimedThreadTurnCanSend({
          threadId,
          rowId: row.id,
          ...(payload.waitForEarlierPromptBinding === true
            ? { waitForEarlierPromptBinding: true }
            : {}),
          ...(payload.waitForEarlierPromptMessageIds !== undefined
            ? { waitForEarlierPromptMessageIds: payload.waitForEarlierPromptMessageIds }
            : {}),
          ...(payload.sourceSequence !== undefined
            ? { sourceSequence: payload.sourceSequence }
            : {}),
          ...(payload.providerSafeSubagentSteer === true ? { allowActiveProviderTurn: true } : {}),
          ...(requiredActiveTurnId !== undefined ? { requiredActiveTurnId } : {}),
        });
        if (!canSendAfterClaim) {
          yield* resetAndReleaseThreadTurnDispatchClaim(row.id);
          return;
        }

        const startQueuedSend = yield* Deferred.make<void>();
        const sendFiber = yield* Deferred.await(startQueuedSend).pipe(
          Effect.andThen(providerService.sendTurn(sendTurnRequest.value)),
          Effect.andThen(deletePendingThreadTurnRows([row.id])),
          Effect.catchCause((cause) =>
            recoverQueuedTurnStartFailure({
              threadId,
              rowId: row.id,
              cause,
              ...(payload.modelSelection !== undefined
                ? { modelSelection: payload.modelSelection }
                : {}),
              ...(payload.runtimeMode !== undefined ? { runtimeMode: payload.runtimeMode } : {}),
              messageId: payload.messageId,
              createdAt: payload.createdAt,
            }),
          ),
          Effect.ensuring(releaseThreadTurnDispatchClaim(row.id)),
          Effect.andThen(
            payload.providerSafeSubagentSteer === true
              ? drainPendingThreadTurns(threadId)
              : Effect.void,
          ),
          Effect.forkScoped,
        );
        const stillClaimed = yield* Effect.sync(() => {
          inFlightThreadTurnFibers.set(row.id, sendFiber);
          const claimed =
            inFlightThreadTurnDispatchIds.has(row.id) && !canceledThreadTurnDispatchIds.has(row.id);
          if (!claimed) {
            inFlightThreadTurnFibers.delete(row.id);
          }
          return claimed;
        });
        const canReleaseQueuedSend =
          stillClaimed &&
          (yield* claimedThreadTurnCanSend({
            threadId,
            rowId: row.id,
            ...(payload.waitForEarlierPromptBinding === true
              ? { waitForEarlierPromptBinding: true }
              : {}),
            ...(payload.waitForEarlierPromptMessageIds !== undefined
              ? { waitForEarlierPromptMessageIds: payload.waitForEarlierPromptMessageIds }
              : {}),
            ...(payload.sourceSequence !== undefined
              ? { sourceSequence: payload.sourceSequence }
              : {}),
            ...(payload.providerSafeSubagentSteer === true
              ? { allowActiveProviderTurn: true }
              : {}),
            ...(requiredActiveTurnId !== undefined ? { requiredActiveTurnId } : {}),
          }));
        if (!canReleaseQueuedSend) {
          yield* Fiber.interrupt(sendFiber).pipe(Effect.ignore);
          yield* resetAndReleaseThreadTurnDispatchClaim(row.id);
          return;
        }
        yield* Deferred.succeed(startQueuedSend, undefined);
        return;
      }
    }).pipe(Effect.orDie);

  const drainAllPendingThreadTurns = Effect.fnUntraced(function* () {
    const rows = yield* pendingDispatches.listAll().pipe(Effect.orDie);
    const threadIds = new Set(
      rows
        .filter((row) => row.kind === THREAD_TURN_PENDING_KIND)
        .map((row) => String(row.targetThreadId)),
    );
    for (const rawThreadId of threadIds) {
      yield* drainPendingThreadTurns(ThreadId.make(rawThreadId));
    }
  });

  const recoverMissingQueuedThreadTurnsFromEventLog = Effect.fnUntraced(function* () {
    const threadCache = new Map<string, OrchestrationThread | undefined>();
    const latestStopRequestByThread = new Map<
      string,
      { readonly sequence: number; readonly createdAt: string }
    >();
    const effectiveStopRequestByThread = new Map<string, StopRequestMarker>();
    const effectiveStopEventByThread = new Map<string, SessionStopRequestedEvent>();
    const resolveThreadCached = (threadId: ThreadId) =>
      Effect.gen(function* () {
        const threadCacheKey = String(threadId);
        let thread = threadCache.get(threadCacheKey);
        if (!threadCache.has(threadCacheKey)) {
          thread = yield* resolveThread(threadId);
          threadCache.set(threadCacheKey, thread);
        }
        return thread;
      });

    yield* orchestrationEngine.readEvents(0, Number.MAX_SAFE_INTEGER).pipe(
      Stream.runForEach((event) =>
        Effect.sync(() => {
          if (event.type === "thread.session-stop-requested") {
            const key = String(event.payload.threadId);
            const previous = latestStopRequestByThread.get(key);
            const stopRequest = {
              sequence: event.sequence,
              createdAt: event.payload.createdAt,
            };
            if (previous === undefined || event.sequence > previous.sequence) {
              latestStopRequestByThread.set(key, stopRequest);
            }
            effectiveStopRequestByThread.set(key, stopRequest);
            effectiveStopEventByThread.set(key, event);
            return;
          }
          if (event.type === "thread.session-set" && event.payload.session.status !== "stopped") {
            const key = String(event.payload.threadId);
            const stopRequest = effectiveStopRequestByThread.get(key);
            if (
              stopRequest !== undefined &&
              event.sequence > stopRequest.sequence &&
              event.payload.session.updatedAt >= stopRequest.createdAt
            ) {
              effectiveStopRequestByThread.delete(key);
              effectiveStopEventByThread.delete(key);
            }
          }
        }),
      ),
      Effect.orDie,
    );
    yield* Effect.sync(() => {
      for (const [threadId, stopRequest] of effectiveStopRequestByThread) {
        stopRequestByThreadId.set(threadId, stopRequest);
      }
    });
    yield* Effect.forEach(
      effectiveStopEventByThread.values(),
      (event) =>
        Effect.gen(function* () {
          const thread = yield* resolveThreadCached(event.payload.threadId);
          if (
            thread === undefined ||
            (thread.session?.status === "stopped" &&
              thread.session.updatedAt >= event.payload.createdAt)
          ) {
            return;
          }
          yield* processSessionStopRequested(event);
          threadCache.delete(String(event.payload.threadId));
        }),
      { concurrency: 1, discard: true },
    );

    const existingRows = yield* pendingDispatches.listAll().pipe(Effect.orDie);
    const knownThreadTurnRows = new Map(
      existingRows
        .filter((row) => row.kind === THREAD_TURN_PENDING_KIND)
        .map((row) => [row.id, row]),
    );
    const staleKnownRows: PendingDispatch[] = [];
    for (const row of knownThreadTurnRows.values()) {
      const payload = parseQueuedThreadTurnPayload(row);
      if (payload === null) {
        staleKnownRows.push(row);
        continue;
      }
      const thread = yield* resolveThreadCached(row.targetThreadId);
      const message = thread?.messages.find((entry) => entry.id === payload.messageId);
      const providerSafeSubagentSteer = payload.providerSafeSubagentSteer === true;
      const markerBoundProviderSafeSubagentSteer =
        providerSafeSubagentSteer &&
        message?.turnId !== null &&
        message?.turnId !== undefined &&
        isPendingSameTurnSteerMarkerTurnId(message.turnId);
      const activeProviderSession =
        providerSafeSubagentSteer &&
        !markerBoundProviderSafeSubagentSteer &&
        message !== undefined &&
        message.turnId !== null
          ? yield* resolveActiveSession(row.targetThreadId)
          : undefined;
      if (
        thread === undefined ||
        message === undefined ||
        (message.role !== "user" && message.role !== "system") ||
        (message.turnId !== null &&
          !markerBoundProviderSafeSubagentSteer &&
          (!providerSafeSubagentSteer || activeProviderSession?.activeTurnId !== message.turnId)) ||
        failedProviderTurnStartMessageIds(thread).has(payload.messageId)
      ) {
        staleKnownRows.push(row);
      }
    }
    if (staleKnownRows.length > 0) {
      yield* deletePendingThreadTurnRows(staleKnownRows.map((row) => row.id));
      for (const row of staleKnownRows) {
        knownThreadTurnRows.delete(row.id);
      }
    }
    const queuedRowCountByThread = new Map<string, number>();
    const queuedRowEvidenceIds = new Set<PendingDispatchId>();
    const incrementQueuedRowCount = (threadId: ThreadId) => {
      const key = String(threadId);
      queuedRowCountByThread.set(key, (queuedRowCountByThread.get(key) ?? 0) + 1);
    };
    const markQueuedRowEvidence = (row: PendingDispatch) => {
      if (queuedRowEvidenceIds.has(row.id)) {
        return;
      }
      queuedRowEvidenceIds.add(row.id);
      incrementQueuedRowCount(row.targetThreadId);
    };
    const decrementQueuedRowCount = (threadId: ThreadId) => {
      const key = String(threadId);
      const nextCount = (queuedRowCountByThread.get(key) ?? 0) - 1;
      if (nextCount > 0) {
        queuedRowCountByThread.set(key, nextCount);
      } else {
        queuedRowCountByThread.delete(key);
      }
    };
    const unmarkQueuedRowEvidence = (row: PendingDispatch) => {
      if (!queuedRowEvidenceIds.delete(row.id)) {
        return;
      }
      decrementQueuedRowCount(row.targetThreadId);
    };
    const historicalSessionByThread = new Map<string, HistoricalThreadSessionState>();
    const unsettledStopRequestByThread = new Map<string, StopRequestMarker>();
    const priorOpenProjectedPendingStartMessageIdsByThread = new Map<string, Set<string>>();
    const projectedPendingStartMessageIdsByThread = new Map<string, Set<string>>();
    const projectedPendingStartMessageIdsForThread = (threadId: ThreadId) =>
      Effect.gen(function* () {
        const key = String(threadId);
        const cached = projectedPendingStartMessageIdsByThread.get(key);
        if (cached !== undefined) {
          return cached;
        }
        const rows = yield* projectionTurns.listByThreadId({ threadId }).pipe(Effect.orDie);
        const pendingMessageIds = new Set<string>();
        for (const row of rows) {
          if (
            row.turnId === null &&
            row.state === "pending" &&
            row.pendingMessageId !== null &&
            row.checkpointTurnCount === null
          ) {
            pendingMessageIds.add(String(row.pendingMessageId));
          }
        }
        projectedPendingStartMessageIdsByThread.set(key, pendingMessageIds);
        return pendingMessageIds;
      });
    const dropProjectedPendingStartMessageId = (threadId: ThreadId, messageId: MessageId) => {
      const key = String(threadId);
      const rawMessageId = String(messageId);
      const pendingMessageIds = projectedPendingStartMessageIdsByThread.get(key);
      pendingMessageIds?.delete(String(messageId));
      if (pendingMessageIds?.size === 0) {
        projectedPendingStartMessageIdsByThread.delete(key);
      }
      const priorMessageIds = priorOpenProjectedPendingStartMessageIdsByThread.get(key);
      priorMessageIds?.delete(rawMessageId);
      if (priorMessageIds?.size === 0) {
        priorOpenProjectedPendingStartMessageIdsByThread.delete(key);
      }
    };
    const rememberOpenProjectedPendingStart = (threadId: ThreadId, messageId: MessageId) => {
      const key = String(threadId);
      const existing = priorOpenProjectedPendingStartMessageIdsByThread.get(key);
      if (existing !== undefined) {
        existing.add(String(messageId));
        return;
      }
      priorOpenProjectedPendingStartMessageIdsByThread.set(key, new Set([String(messageId)]));
    };

    yield* orchestrationEngine.readEvents(0, Number.MAX_SAFE_INTEGER).pipe(
      Stream.runForEach((event) =>
        Effect.gen(function* () {
          if (event.type === "thread.session-stop-requested") {
            unsettledStopRequestByThread.set(String(event.payload.threadId), {
              sequence: event.sequence,
              createdAt: event.payload.createdAt,
            });
            return;
          }
          if (event.type === "thread.session-set") {
            const key = String(event.payload.threadId);
            const historicalSession = {
              status: event.payload.session.status,
              activeTurnId: event.payload.session.activeTurnId,
            } satisfies HistoricalThreadSessionState;
            historicalSessionByThread.set(key, historicalSession);
            const stopRequest = unsettledStopRequestByThread.get(key);
            if (
              stopRequest !== undefined &&
              event.sequence > stopRequest.sequence &&
              (event.payload.session.status === "stopped" ||
                event.payload.session.updatedAt >= stopRequest.createdAt)
            ) {
              unsettledStopRequestByThread.delete(key);
            }
            return;
          }
          if (event.type !== "thread.turn-start-requested") {
            return;
          }

          const rowId = pendingThreadTurnIdForMessage(event.payload.messageId);
          const key = turnStartKeyForEvent(event);
          const existingKnownRow = knownThreadTurnRows.get(rowId);
          if (existingKnownRow !== undefined) {
            const payload = parseQueuedThreadTurnPayload(existingKnownRow);
            if (payload !== null && payload.sourceSequence === undefined) {
              const backfilledRow = {
                ...existingKnownRow,
                text: encodeQueuedThreadTurnPayloadJson({
                  messageId: payload.messageId,
                  ...(payload.modelSelection !== undefined
                    ? { modelSelection: payload.modelSelection }
                    : {}),
                  ...(payload.runtimeMode !== undefined
                    ? { runtimeMode: payload.runtimeMode }
                    : {}),
                  ...(payload.interactionMode !== undefined
                    ? { interactionMode: payload.interactionMode }
                    : {}),
                  createdAt: payload.createdAt,
                  sourceSequence: event.sequence,
                  ...(payload.providerSafeSubagentSteer === true
                    ? { providerSafeSubagentSteer: true }
                    : {}),
                  ...(payload.waitForEarlierPromptBinding === true
                    ? { waitForEarlierPromptBinding: true }
                    : {}),
                  ...(payload.waitForEarlierPromptMessageIds !== undefined
                    ? { waitForEarlierPromptMessageIds: payload.waitForEarlierPromptMessageIds }
                    : {}),
                }),
              } satisfies PendingDispatch;
              yield* pendingDispatches.insert(backfilledRow).pipe(Effect.orDie);
              knownThreadTurnRows.set(rowId, backfilledRow);
            }
          }
          const stopRequest = latestStopRequestByThread.get(String(event.payload.threadId));
          if (stopRequest !== undefined && stopRequest.sequence > event.sequence) {
            const existingRow = knownThreadTurnRows.get(rowId);
            const thread = yield* resolveThreadCached(event.payload.threadId);
            const message = thread?.messages.find((entry) => entry.id === event.payload.messageId);
            if (existingRow !== undefined) {
              if (yield* claimTurnStartHandling(key)) {
                return;
              }
              if (message !== undefined && message.turnId !== null) {
                yield* deletePendingThreadTurnRows([existingRow.id]);
                dropProjectedPendingStartMessageId(event.payload.threadId, event.payload.messageId);
              } else {
                yield* cancelPendingThreadTurnRows({
                  threadId: event.payload.threadId,
                  rows: [existingRow],
                  createdAt: stopRequest.createdAt,
                });
                dropProjectedPendingStartMessageId(event.payload.threadId, event.payload.messageId);
              }
              knownThreadTurnRows.delete(rowId);
              unmarkQueuedRowEvidence(existingRow);
            } else {
              if (
                thread !== undefined &&
                message !== undefined &&
                (message.role === "user" || message.role === "system") &&
                message.turnId === null &&
                !failedProviderTurnStartMessageIds(thread).has(event.payload.messageId)
              ) {
                if (yield* claimTurnStartHandling(key)) {
                  return;
                }
                yield* appendQueuedTurnCanceledActivity({
                  threadId: event.payload.threadId,
                  messageId: event.payload.messageId,
                  createdAt: stopRequest.createdAt,
                });
                dropProjectedPendingStartMessageId(event.payload.threadId, event.payload.messageId);
              }
            }
            return;
          }

          if (knownThreadTurnRows.has(rowId)) {
            const row = knownThreadTurnRows.get(rowId);
            if (row !== undefined) {
              markQueuedRowEvidence(row);
              yield* claimTurnStartHandling(key);
            }
            return;
          }

          const thread = yield* resolveThreadCached(event.payload.threadId);
          if (!thread) {
            return;
          }

          const message = thread.messages.find((entry) => entry.id === event.payload.messageId);
          if (!message || (message.role !== "user" && message.role !== "system")) {
            return;
          }
          const markerAnchorMessageId =
            message.turnId !== null && isProviderSafeSubagentSteerTurnStart(event)
              ? pendingSameTurnSteerMarkerMessageId(message.turnId)
              : null;
          if (message.turnId !== null && markerAnchorMessageId === null) {
            return;
          }
          if (failedProviderTurnStartMessageIds(thread).has(event.payload.messageId)) {
            return;
          }

          const threadKey = String(event.payload.threadId);
          const projectedPendingStartMessageIds = yield* projectedPendingStartMessageIdsForThread(
            event.payload.threadId,
          );
          const hasOpenProjectedPendingStart = projectedPendingStartMessageIds.has(
            String(event.payload.messageId),
          );
          const hasProviderSafeProjectedPendingStartEvidence =
            isProviderSafeSubagentSteerTurnStart(event) && hasOpenProjectedPendingStart;
          const earlierOpenProjectedPendingStartMessageIds = [
            ...(priorOpenProjectedPendingStartMessageIdsByThread.get(threadKey) ?? []),
          ].map((messageId) => MessageId.make(messageId));
          const hasProjectedPendingStartOrderingEvidence =
            hasOpenProjectedPendingStart && earlierOpenProjectedPendingStartMessageIds.length > 0;
          const hasQueuedRecoveryEvidence =
            (queuedRowCountByThread.get(threadKey) ?? 0) > 0 ||
            historicalSessionBlocksImmediateSend(historicalSessionByThread.get(threadKey)) ||
            unsettledStopRequestByThread.has(threadKey) ||
            hasProjectedPendingStartOrderingEvidence ||
            markerAnchorMessageId !== null ||
            hasProviderSafeProjectedPendingStartEvidence;
          if (!hasQueuedRecoveryEvidence) {
            if (hasOpenProjectedPendingStart) {
              rememberOpenProjectedPendingStart(event.payload.threadId, event.payload.messageId);
            }
            return;
          }

          if (yield* claimTurnStartHandling(key)) {
            return;
          }
          if (hasOpenProjectedPendingStart) {
            rememberOpenProjectedPendingStart(event.payload.threadId, event.payload.messageId);
          }
          yield* startFirstUserTurnSideEffects({ event, thread, message });
          const waitForEarlierPromptMessageIds =
            markerAnchorMessageId !== null
              ? [markerAnchorMessageId]
              : hasProjectedPendingStartOrderingEvidence
                ? earlierOpenProjectedPendingStartMessageIds
                : undefined;
          const row = pendingThreadTurnRowForEvent(
            event,
            waitForEarlierPromptMessageIds !== undefined
              ? {
                  waitForEarlierPromptBinding: true,
                  waitForEarlierPromptMessageIds,
                }
              : undefined,
          );
          yield* pendingDispatches.insert(row).pipe(Effect.orDie);
          knownThreadTurnRows.set(row.id, row);
          markQueuedRowEvidence(row);
        }),
      ),
      Effect.orDie,
    );
  });

  const processTurnStartRequested = Effect.fn("processTurnStartRequested")(function* (
    event: TurnStartRequestedEvent,
  ) {
    const key = turnStartKeyForEvent(event);
    if (yield* claimTurnStartHandling(key)) {
      return;
    }

    const thread = yield* resolveThread(event.payload.threadId);
    if (!thread) {
      return;
    }

    const message = thread.messages.find((entry) => entry.id === event.payload.messageId);
    if (!message || (message.role !== "user" && message.role !== "system")) {
      yield* appendProviderFailureActivity({
        threadId: event.payload.threadId,
        kind: "provider.turn.start.failed",
        summary: "Provider turn start failed",
        detail: `Turn start message '${event.payload.messageId}' was not found for turn start request.`,
        turnId: null,
        createdAt: event.payload.createdAt,
        messageId: event.payload.messageId,
      });
      return;
    }

    const stopRequest = stopRequestByThreadId.get(event.payload.threadId);
    if (stopRequest !== undefined && stopRequestSupersedesSequence(stopRequest, event.sequence)) {
      yield* appendQueuedTurnCanceledActivity({
        threadId: event.payload.threadId,
        messageId: event.payload.messageId,
        createdAt: stopRequest.createdAt,
      });
      return;
    }

    yield* startFirstUserTurnSideEffects({ event, thread, message });

    let pendingThreadTurns = yield* pendingDispatches
      .listByTarget({ kind: THREAD_TURN_PENDING_KIND, targetThreadId: event.payload.threadId })
      .pipe(Effect.orDie);
    if (thread.session?.status === "stopped" && pendingThreadTurns.length > 0) {
      const stopRequest = stopRequestByThreadId.get(event.payload.threadId);
      const rowsToCancel =
        stopRequest === undefined
          ? pendingThreadTurns
          : pendingThreadTurns.filter((row) => stopRequestSupersedesQueuedRow(stopRequest, row));
      const rowsToKeep =
        stopRequest === undefined
          ? []
          : pendingThreadTurns.filter((row) => !stopRequestSupersedesQueuedRow(stopRequest, row));
      if (rowsToCancel.length > 0) {
        yield* cancelPendingThreadTurnRows({
          threadId: event.payload.threadId,
          rows: rowsToCancel,
          createdAt: event.payload.createdAt,
        });
      }
      pendingThreadTurns = rowsToKeep;
    }
    const activeProviderSession = yield* resolveActiveSession(event.payload.threadId);
    const hasLiveProviderTurn =
      activeProviderSession?.status === "running" ||
      activeProviderSession?.activeTurnId !== undefined;
    const hasImmediateSendInFlight = inFlightImmediateThreadTurnThreadIds.has(
      event.payload.threadId,
    );
    const isProviderSafeSubagentSteer = isProviderSafeSubagentSteerTurnStart(event);
    const hasQueuedProviderSafeSubagentSteer = pendingThreadTurns.some(
      (row) => parseQueuedThreadTurnPayload(row)?.providerSafeSubagentSteer === true,
    );
    const shouldQueueProviderSafeSubagentSteer =
      hasImmediateSendInFlight ||
      hasQueuedProviderSafeSubagentSteer ||
      (!hasLiveProviderTurn && (!isThreadProviderIdle(thread) || pendingThreadTurns.length > 0));
    const shouldQueueRegularTurn =
      !isThreadProviderIdle(thread) ||
      hasLiveProviderTurn ||
      hasImmediateSendInFlight ||
      pendingThreadTurns.length > 0;
    if (
      isProviderSafeSubagentSteer ? shouldQueueProviderSafeSubagentSteer : shouldQueueRegularTurn
    ) {
      yield* enqueueThreadTurnUntilIdle({ event });
      if (canAutoDrainQueuedThreadTurns(thread)) {
        yield* drainPendingThreadTurns(event.payload.threadId);
      }
      return;
    }

    const recoverTurnStartFailure = (cause: Cause.Cause<unknown>) =>
      handleTurnStartFailure({
        threadId: event.payload.threadId,
        cause,
        ...(event.payload.modelSelection !== undefined
          ? { modelSelection: event.payload.modelSelection }
          : {}),
        runtimeMode: event.payload.runtimeMode,
        messageId: event.payload.messageId,
        createdAt: event.payload.createdAt,
      }).pipe(
        Effect.catchCause((recoveryCause) =>
          Effect.logWarning("provider command reactor failed to recover turn start failure", {
            eventType: event.type,
            threadId: event.payload.threadId,
            cause: Cause.pretty(recoveryCause),
            originalCause: Cause.pretty(cause),
          }),
        ),
      );

    const sendTurnRequest = yield* buildSendTurnRequestForThread({
      threadId: event.payload.threadId,
      messageText: message.text,
      ...(message.attachments !== undefined ? { attachments: message.attachments } : {}),
      ...(event.payload.modelSelection !== undefined
        ? { modelSelection: event.payload.modelSelection }
        : {}),
      runtimeMode: event.payload.runtimeMode,
      interactionMode: event.payload.interactionMode,
      createdAt: event.payload.createdAt,
    }).pipe(
      Effect.map(Option.some),
      Effect.catchCause((cause) =>
        handleTurnStartFailure({
          threadId: event.payload.threadId,
          cause,
          ...(event.payload.modelSelection !== undefined
            ? { modelSelection: event.payload.modelSelection }
            : {}),
          runtimeMode: event.payload.runtimeMode,
          messageId: event.payload.messageId,
          createdAt: event.payload.createdAt,
        }).pipe(Effect.as(Option.none())),
      ),
    );

    if (Option.isNone(sendTurnRequest)) {
      return;
    }

    yield* Effect.sync(() => {
      inFlightImmediateThreadTurnThreadIds.add(event.payload.threadId);
    });
    yield* providerService.sendTurn(sendTurnRequest.value).pipe(
      Effect.catchCause(recoverTurnStartFailure),
      Effect.ensuring(
        Effect.sync(() => {
          inFlightImmediateThreadTurnThreadIds.delete(event.payload.threadId);
        }),
      ),
      Effect.andThen(drainPendingThreadTurns(event.payload.threadId)),
      Effect.forkScoped,
    );
  });

  const processTurnInterruptRequested = Effect.fn("processTurnInterruptRequested")(function* (
    event: Extract<ProviderIntentEvent, { type: "thread.turn-interrupt-requested" }>,
  ) {
    const thread = yield* resolveThread(event.payload.threadId);
    if (!thread) {
      return;
    }
    const activeSession = yield* resolveActiveSession(event.payload.threadId);
    const hasSession =
      thread.session &&
      thread.session.status !== "stopped" &&
      !(activeSession === undefined && isUnstartedSyntheticErrorSession(thread));
    if (!hasSession) {
      return yield* appendProviderFailureActivity({
        threadId: event.payload.threadId,
        kind: "provider.turn.interrupt.failed",
        summary: "Provider turn interrupt failed",
        detail: "No active provider session is bound to this thread.",
        turnId: event.payload.turnId ?? null,
        createdAt: event.payload.createdAt,
      });
    }

    // Orchestration turn ids are not provider turn ids, so interrupt by session.
    yield* providerService.interruptTurn({ threadId: event.payload.threadId });
  });

  const processApprovalResponseRequested = Effect.fn("processApprovalResponseRequested")(function* (
    event: Extract<ProviderIntentEvent, { type: "thread.approval-response-requested" }>,
  ) {
    const thread = yield* resolveThread(event.payload.threadId);
    if (!thread) {
      return;
    }
    const activeSession = yield* resolveActiveSession(event.payload.threadId);
    const hasSession =
      thread.session &&
      thread.session.status !== "stopped" &&
      !(activeSession === undefined && isUnstartedSyntheticErrorSession(thread));
    if (!hasSession) {
      return yield* appendProviderFailureActivity({
        threadId: event.payload.threadId,
        kind: "provider.approval.respond.failed",
        summary: "Provider approval response failed",
        detail: "No active provider session is bound to this thread.",
        turnId: null,
        createdAt: event.payload.createdAt,
        requestId: event.payload.requestId,
      });
    }

    yield* providerService
      .respondToRequest({
        threadId: event.payload.threadId,
        requestId: event.payload.requestId,
        decision: event.payload.decision,
      })
      .pipe(
        Effect.catchCause((cause) =>
          appendProviderFailureActivity({
            threadId: event.payload.threadId,
            kind: "provider.approval.respond.failed",
            summary: "Provider approval response failed",
            detail: isUnknownPendingApprovalRequestError(cause)
              ? stalePendingRequestDetail("approval", event.payload.requestId)
              : Cause.pretty(cause),
            turnId: null,
            createdAt: event.payload.createdAt,
            requestId: event.payload.requestId,
          }),
        ),
      );
  });

  const processUserInputResponseRequested = Effect.fn("processUserInputResponseRequested")(
    function* (
      event: Extract<ProviderIntentEvent, { type: "thread.user-input-response-requested" }>,
    ) {
      const thread = yield* resolveThread(event.payload.threadId);
      if (!thread) {
        return;
      }
      const activeSession = yield* resolveActiveSession(event.payload.threadId);
      const hasSession =
        thread.session &&
        thread.session.status !== "stopped" &&
        !(activeSession === undefined && isUnstartedSyntheticErrorSession(thread));
      if (!hasSession) {
        return yield* appendProviderFailureActivity({
          threadId: event.payload.threadId,
          kind: "provider.user-input.respond.failed",
          summary: "Provider user input response failed",
          detail: "No active provider session is bound to this thread.",
          turnId: null,
          createdAt: event.payload.createdAt,
          requestId: event.payload.requestId,
        });
      }

      yield* providerService
        .respondToUserInput({
          threadId: event.payload.threadId,
          requestId: event.payload.requestId,
          answers: event.payload.answers,
        })
        .pipe(
          Effect.catchCause((cause) =>
            appendProviderFailureActivity({
              threadId: event.payload.threadId,
              kind: "provider.user-input.respond.failed",
              summary: "Provider user input response failed",
              detail: isUnknownPendingUserInputRequestError(cause)
                ? stalePendingRequestDetail("user-input", event.payload.requestId)
                : Cause.pretty(cause),
              turnId: null,
              createdAt: event.payload.createdAt,
              requestId: event.payload.requestId,
            }),
          ),
        );
    },
  );

  const processSessionStopRequested = Effect.fn("processSessionStopRequested")(function* (
    event: Extract<ProviderIntentEvent, { type: "thread.session-stop-requested" }>,
  ) {
    yield* Effect.sync(() => {
      stopRequestByThreadId.set(event.payload.threadId, {
        sequence: event.sequence,
        createdAt: event.payload.createdAt,
      });
    });
    const thread = yield* resolveThread(event.payload.threadId);
    if (!thread) {
      return;
    }

    const now = event.payload.createdAt;
    const rows = yield* pendingDispatches
      .listByTarget({ kind: THREAD_TURN_PENDING_KIND, targetThreadId: thread.id })
      .pipe(Effect.orDie);
    const { rowsToCancel, rowsToDelete } = partitionStoppedThreadTurnRows({
      thread,
      rows,
      stopRequest: { sequence: event.sequence, createdAt: event.payload.createdAt },
    });
    if (rowsToDelete.length > 0) {
      yield* deletePendingThreadTurnRows(rowsToDelete.map((row) => row.id));
    }
    if (rowsToCancel.length > 0) {
      yield* cancelPendingThreadTurnRows({
        threadId: thread.id,
        rows: rowsToCancel,
        createdAt: now,
      });
    }

    const activeSession = yield* resolveActiveSession(thread.id);
    if (
      thread.session &&
      thread.session.status !== "stopped" &&
      !(activeSession === undefined && isUnstartedSyntheticErrorSession(thread))
    ) {
      yield* providerService.stopSession({ threadId: thread.id });
    }

    yield* setThreadSession({
      threadId: thread.id,
      session: {
        threadId: thread.id,
        status: "stopped",
        providerName: thread.session?.providerName ?? null,
        ...(thread.session?.providerInstanceId !== undefined
          ? { providerInstanceId: thread.session.providerInstanceId }
          : {}),
        runtimeMode: thread.session?.runtimeMode ?? DEFAULT_RUNTIME_MODE,
        activeTurnId: null,
        lastError: thread.session?.lastError ?? null,
        updatedAt: now,
      },
      createdAt: now,
    });
  });

  const processDomainEvent = Effect.fn("processDomainEvent")(function* (
    event: ProviderCommandReactorEvent,
  ) {
    yield* Effect.annotateCurrentSpan({
      "orchestration.event_type": event.type,
      "orchestration.thread_id": event.payload.threadId,
      ...(event.commandId ? { "orchestration.command_id": event.commandId } : {}),
    });
    yield* increment(orchestrationEventsProcessedTotal, {
      eventType: event.type,
    });
    switch (event.type) {
      case "thread.runtime-mode-set": {
        const thread = yield* resolveThread(event.payload.threadId);
        const activeSession = thread
          ? yield* resolveActiveSession(event.payload.threadId)
          : undefined;
        if (
          !thread?.session ||
          thread.session.status === "stopped" ||
          (activeSession === undefined && isUnstartedSyntheticErrorSession(thread))
        ) {
          return;
        }
        const cachedModelSelection = threadModelSelections.get(event.payload.threadId);
        yield* ensureSessionForThread(event.payload.threadId, event.occurredAt, {
          ...(cachedModelSelection !== undefined ? { modelSelection: cachedModelSelection } : {}),
          runtimeMode: event.payload.runtimeMode,
        });
        return;
      }
      case "thread.turn-start-requested":
        yield* processTurnStartRequested(event);
        return;
      case "thread.turn-interrupt-requested":
        yield* processTurnInterruptRequested(event);
        return;
      case "thread.approval-response-requested":
        yield* processApprovalResponseRequested(event);
        return;
      case "thread.user-input-response-requested":
        yield* processUserInputResponseRequested(event);
        return;
      case "thread.session-stop-requested":
        yield* processSessionStopRequested(event);
        return;
      case "thread.session-set":
        if (event.payload.session.status === "stopped") {
          const rows = yield* pendingDispatches
            .listByTarget({
              kind: THREAD_TURN_PENDING_KIND,
              targetThreadId: event.payload.threadId,
            })
            .pipe(Effect.orDie);
          const stopRequest = stopRequestByThreadId.get(event.payload.threadId);
          const thread = yield* resolveThread(event.payload.threadId);
          const { rowsToCancel, rowsToDelete, rowsToKeep } = partitionStoppedThreadTurnRows({
            thread,
            rows,
            stopRequest,
          });
          if (rowsToDelete.length > 0) {
            yield* deletePendingThreadTurnRows(rowsToDelete.map((row) => row.id));
          }
          if (rowsToCancel.length > 0) {
            yield* cancelPendingThreadTurnRows({
              threadId: event.payload.threadId,
              rows: rowsToCancel,
              createdAt: event.payload.session.updatedAt,
            });
          }
          if (rowsToKeep.length > 0) {
            yield* drainPendingThreadTurns(event.payload.threadId);
            return;
          }
          return;
        }
        const stopRequest = stopRequestByThreadId.get(event.payload.threadId);
        if (
          stopRequest !== undefined &&
          event.sequence > stopRequest.sequence &&
          event.payload.session.updatedAt >= stopRequest.createdAt
        ) {
          yield* Effect.sync(() => {
            stopRequestByThreadId.delete(event.payload.threadId);
          });
        }
        yield* drainPendingThreadTurns(event.payload.threadId);
        return;
      case "thread.turn-diff-completed":
        yield* drainPendingThreadTurns(event.payload.threadId);
        return;
    }
  });

  const processDomainEventSafely = (event: ProviderCommandReactorEvent) =>
    processDomainEvent(event).pipe(
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Effect.failCause(cause);
        }
        return Effect.logWarning("provider command reactor failed to process event", {
          eventType: event.type,
          cause: Cause.pretty(cause),
        });
      }),
    );

  const worker = yield* makeDrainableWorker(processDomainEventSafely);

  const start: ProviderCommandReactorShape["start"] = Effect.fn("start")(function* () {
    const processEvent = Effect.fn("processEvent")(function* (event: OrchestrationEvent) {
      if (event.type === "thread.session-stop-requested") {
        yield* Effect.sync(() => {
          stopRequestByThreadId.set(event.payload.threadId, {
            sequence: event.sequence,
            createdAt: event.payload.createdAt,
          });
        });
      }
      if (
        event.type === "thread.runtime-mode-set" ||
        event.type === "thread.turn-start-requested" ||
        event.type === "thread.turn-interrupt-requested" ||
        event.type === "thread.approval-response-requested" ||
        event.type === "thread.user-input-response-requested" ||
        event.type === "thread.session-stop-requested" ||
        event.type === "thread.session-set" ||
        event.type === "thread.turn-diff-completed"
      ) {
        return yield* worker.enqueue(event);
      }
    });

    yield* Effect.forkScoped(
      Stream.runForEach(orchestrationEngine.streamDomainEvents, processEvent),
    );
    yield* Effect.yieldNow;
    yield* recoverMissingQueuedThreadTurnsFromEventLog().pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("provider command reactor failed to recover queued turns on start", {
          cause: Cause.pretty(cause),
        }),
      ),
    );
    yield* drainAllPendingThreadTurns().pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("provider command reactor failed to drain queued turns on start", {
          cause: Cause.pretty(cause),
        }),
      ),
    );
  });

  return {
    start,
    drain: worker.drain,
  } satisfies ProviderCommandReactorShape;
});

export const ProviderCommandReactorLive = Layer.effect(ProviderCommandReactor, make).pipe(
  Layer.provide(ProjectionTurnRepositoryLive),
);
