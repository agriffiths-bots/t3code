import {
  type ChatAttachment,
  CommandId,
  EventId,
  type MessageId,
  type ModelSelection,
  OrchestrationDispatchCommandError,
  type OrchestrationEvent,
  ProviderDriverKind,
  type ProjectId,
  type OrchestrationSession,
  ThreadId,
  type ProviderSession,
  type RuntimeMode,
  TurnId,
} from "@t3tools/contracts";
import { isTemporaryWorktreeBranch, WORKTREE_BRANCH_PREFIX } from "@t3tools/shared/git";
import * as Cache from "effect/Cache";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";

import { resolveThreadWorkspaceCwd } from "../../checkpointing/Utils.ts";
import { increment, orchestrationEventsProcessedTotal } from "../../observability/Metrics.ts";
import {
  ProviderAdapterRequestError,
  ProviderSendTurnFailedError,
  ProviderSessionStartTimeoutError,
} from "../../provider/Errors.ts";
import type { ProviderServiceError } from "../../provider/Errors.ts";
import { TextGeneration } from "../../textGeneration/TextGeneration.ts";
import { ProviderService } from "../../provider/Services/ProviderService.ts";
import { ProviderRegistry } from "../../provider/Services/ProviderRegistry.ts";
import {
  dispatchAlreadyCoordinated,
  OrchestrationEngineService,
} from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  ProviderCommandReactor,
  type ProviderCommandReactorShape,
} from "../Services/ProviderCommandReactor.ts";
import { forkParked, ServerActivation } from "../../serverActivation.ts";
import { canReplaceThreadTitle, DEFAULT_THREAD_TITLE } from "../threadTitles.ts";
import {
  resolveSourceControlWriterModelSelection,
  ServerSettingsService,
} from "../../serverSettings.ts";
import { VcsStatusBroadcaster } from "../../vcs/VcsStatusBroadcaster.ts";
import { GitWorkflowService } from "../../git/GitWorkflowService.ts";
import { WorktreeLifecycleCoordinator } from "../Services/WorktreeLifecycleCoordinator.ts";
import { threadAudienceSystemDispatchAuthority } from "../commandAudienceGuard.ts";
const isProviderAdapterRequestError = Schema.is(ProviderAdapterRequestError);
const isProviderSendTurnFailedError = Schema.is(ProviderSendTurnFailedError);
const isProviderSessionStartTimeoutError = Schema.is(ProviderSessionStartTimeoutError);
const isProviderDriverKind = Schema.is(ProviderDriverKind);

type ProviderIntentEvent = Extract<
  OrchestrationEvent,
  {
    type:
      | "thread.meta-updated"
      | "thread.runtime-mode-set"
      | "thread.turn-start-requested"
      | "thread.turn-interrupt-requested"
      | "thread.approval-response-requested"
      | "thread.user-input-response-requested"
      | "thread.session-stop-requested";
  }
>;

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

const turnStartKeyForEvent = (event: ProviderIntentEvent): string =>
  event.commandId !== null ? `command:${event.commandId}` : `event:${event.eventId}`;

const HANDLED_TURN_START_KEY_MAX = 10_000;
const HANDLED_TURN_START_KEY_TTL = Duration.minutes(30);
const FAILED_TURN_STOP_TIMEOUT = Duration.seconds(10);
const FAILED_TURN_SESSION_LOOKUP_TIMEOUT = Duration.seconds(1);
const DEFAULT_RUNTIME_MODE: RuntimeMode = "full-access";
const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

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

const MAX_REGENERATION_ATTACHMENTS = 4;
const MAX_THREAD_TITLE_CONTEXT_CHARS = 8_000;
const MAX_FIRST_USER_TITLE_CONTEXT_CHARS = 2_000;
const THREAD_TITLE_CONTEXT_TRUNCATION_MARKER = "[Earlier content truncated]\n\n";
const FIRST_USER_CONTEXT_TRUNCATION_MARKER = "\n[First user message truncated]";

type ThreadTitleMessage = {
  readonly role: "user" | "assistant" | "system";
  readonly text: string;
  readonly attachments?: ReadonlyArray<ChatAttachment> | undefined;
};

function formatThreadTitleSection(message: ThreadTitleMessage): string | undefined {
  if (message.role === "system") {
    return undefined;
  }
  const text = message.text.trim();
  const attachmentSummary = (message.attachments ?? [])
    .map((attachment) => attachment.name)
    .join(", ");
  const contents = [
    ...(text.length > 0 ? [text] : []),
    ...(attachmentSummary.length > 0 ? [`[Attachments: ${attachmentSummary}]`] : []),
  ].join("\n");
  return contents.length > 0 ? `${message.role.toUpperCase()}:\n${contents}` : undefined;
}

function limitFirstUserSection(section: string): string {
  if (section.length <= MAX_FIRST_USER_TITLE_CONTEXT_CHARS) {
    return section;
  }
  return `${section.slice(
    0,
    MAX_FIRST_USER_TITLE_CONTEXT_CHARS - FIRST_USER_CONTEXT_TRUNCATION_MARKER.length,
  )}${FIRST_USER_CONTEXT_TRUNCATION_MARKER}`;
}

function collectRecentThreadTitleContext(
  messages: ReadonlyArray<ThreadTitleMessage>,
  maxChars: number,
): {
  readonly context: string;
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly truncated: boolean;
} {
  let context = "";
  let truncated = false;
  const retainedAttachments: Array<ChatAttachment> = [];

  for (const message of messages.toReversed()) {
    const section = formatThreadTitleSection(message);
    if (section === undefined) {
      continue;
    }

    const separator = context.length > 0 ? "\n\n" : "";
    const available = maxChars - context.length - separator.length;
    if (section.length > available) {
      if (available > 0) {
        context = `${section.slice(-available)}${separator}${context}`;
        retainedAttachments.unshift(...(message.attachments ?? []));
      }
      truncated = true;
      break;
    }
    context = `${section}${separator}${context}`;
    retainedAttachments.unshift(...(message.attachments ?? []));
  }

  return { context, attachments: retainedAttachments, truncated };
}

function formatThreadTitleContext(messages: ReadonlyArray<ThreadTitleMessage>): {
  readonly message: string;
  readonly attachments: ReadonlyArray<ChatAttachment>;
} {
  const recent = collectRecentThreadTitleContext(messages, MAX_THREAD_TITLE_CONTEXT_CHARS);
  if (!recent.truncated) {
    return {
      message: recent.context,
      attachments: recent.attachments.slice(-MAX_REGENERATION_ATTACHMENTS),
    };
  }

  const firstUserMessage = messages.find(
    (message) => message.role === "user" && formatThreadTitleSection(message),
  );
  const firstUserSection = firstUserMessage
    ? formatThreadTitleSection(firstUserMessage)
    : undefined;
  if (!firstUserMessage || !firstUserSection) {
    return {
      message: `${THREAD_TITLE_CONTEXT_TRUNCATION_MARKER}${recent.context}`,
      attachments: recent.attachments.slice(-MAX_REGENERATION_ATTACHMENTS),
    };
  }

  const pinnedSection = limitFirstUserSection(firstUserSection);
  const recentContextBudget =
    MAX_THREAD_TITLE_CONTEXT_CHARS -
    pinnedSection.length -
    "\n\n".length -
    THREAD_TITLE_CONTEXT_TRUNCATION_MARKER.length;
  const retainedRecent = collectRecentThreadTitleContext(messages, recentContextBudget);
  const pinnedAttachment = firstUserMessage.attachments?.[0];
  const recentAttachments = retainedRecent.attachments.filter(
    (attachment) => attachment.id !== pinnedAttachment?.id,
  );

  return {
    message: `${pinnedSection}\n\n${THREAD_TITLE_CONTEXT_TRUNCATION_MARKER}${retainedRecent.context}`,
    attachments: [
      ...(pinnedAttachment ? [pinnedAttachment] : []),
      ...recentAttachments.slice(
        -(MAX_REGENERATION_ATTACHMENTS - (pinnedAttachment === undefined ? 0 : 1)),
      ),
    ],
  };
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

function findProviderAdapterRequestError(
  cause: Cause.Cause<ProviderServiceError>,
): ProviderAdapterRequestError | undefined {
  const failReason = cause.reasons.find(Cause.isFailReason);
  return isProviderAdapterRequestError(failReason?.error) ? failReason.error : undefined;
}

function findProviderSendTurnFailedError(
  cause: Cause.Cause<unknown>,
): ProviderSendTurnFailedError | undefined {
  const failReason = cause.reasons.find(Cause.isFailReason);
  return isProviderSendTurnFailedError(failReason?.error) ? failReason.error : undefined;
}

function findProviderSessionStartTimeoutError(
  cause: Cause.Cause<unknown>,
): ProviderSessionStartTimeoutError | undefined {
  const failReason = cause.reasons.find(Cause.isFailReason);
  return isProviderSessionStartTimeoutError(failReason?.error) ? failReason.error : undefined;
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

const DEFAULT_TURN_START_LIFECYCLE_GRACE_MS = 15_000;

export interface ProviderCommandReactorLiveOptions {
  /** How long a turn start may keep the worktree-lifecycle permit while its
   * provider sendTurn is in flight. Long enough for every adapter to register
   * the turn against the live worktree; short enough that a hung provider
   * cannot wedge the shared lifecycle slot. */
  readonly turnStartLifecycleGraceMs?: number;
}

const makeReactor = (options?: ProviderCommandReactorLiveOptions) =>
  Effect.gen(function* () {
    const turnStartLifecycleGraceMs = Math.max(
      1,
      options?.turnStartLifecycleGraceMs ?? DEFAULT_TURN_START_LIFECYCLE_GRACE_MS,
    );
    const crypto = yield* Crypto.Crypto;
    const orchestrationEngine = yield* OrchestrationEngineService;
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const providerService = yield* ProviderService;
    const providerRegistry = yield* ProviderRegistry;
    const gitWorkflow = yield* GitWorkflowService;
    const vcsStatusBroadcaster = yield* VcsStatusBroadcaster;
    const textGeneration = yield* TextGeneration;
    const serverSettingsService = yield* ServerSettingsService;
    const worktreeLifecycle = yield* WorktreeLifecycleCoordinator;
    const serverCommandId = (tag: string) =>
      crypto.randomUUIDv4.pipe(Effect.map((uuid) => CommandId.make(`server:${tag}:${uuid}`)));
    const serverEventId = () => crypto.randomUUIDv4.pipe(Effect.map(EventId.make));
    const handledTurnStartKeys = yield* Cache.make<string, true>({
      capacity: HANDLED_TURN_START_KEY_MAX,
      timeToLive: HANDLED_TURN_START_KEY_TTL,
      lookup: () => Effect.succeed(true),
    });

    const hasHandledTurnStartRecently = (key: string) =>
      Cache.getOption(handledTurnStartKeys, key).pipe(
        Effect.flatMap((cached) =>
          Cache.set(handledTurnStartKeys, key, true).pipe(Effect.as(Option.isSome(cached))),
        ),
      );
    const providerDriverForInstance = (instanceId: ModelSelection["instanceId"]) =>
      providerService.getInstanceInfo(instanceId).pipe(
        Effect.map((info) => (isProviderDriverKind(info.driverKind) ? info.driverKind : null)),
        Effect.catchCause(() => Effect.succeed(null)),
      );

    const threadModelSelections = new Map<string, ModelSelection>();

    const authorityForThreadIncludingArchived = Effect.fn(
      "ProviderCommandReactor.authorityForThreadIncludingArchived",
    )(function* (threadId: ThreadId) {
      const thread = yield* projectionSnapshotQuery
        .getThreadShellByIdIncludingArchived(threadId)
        .pipe(
          Effect.flatMap(
            Option.match({
              onNone: () =>
                Effect.fail(
                  new OrchestrationDispatchCommandError({
                    message: "Provider command target audience could not be resolved.",
                  }),
                ),
              onSome: Effect.succeed,
            }),
          ),
        );
      return threadAudienceSystemDispatchAuthority(thread, "ProviderCommandReactor");
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
      readonly turnStartRequestId?: EventId;
      readonly turnStartMessageId?: MessageId;
    }) =>
      Effect.all({
        commandId: serverCommandId("provider-failure-activity"),
        eventId: serverEventId(),
        authority: authorityForThreadIncludingArchived(input.threadId),
      }).pipe(
        Effect.flatMap(({ commandId, eventId, authority }) =>
          orchestrationEngine.dispatch(
            {
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
                  ...(input.turnStartRequestId !== undefined
                    ? { turnStartRequestId: input.turnStartRequestId }
                    : {}),
                  ...(input.turnStartMessageId !== undefined
                    ? { turnStartMessageId: input.turnStartMessageId }
                    : {}),
                },
                turnId: input.turnId,
                createdAt: input.createdAt,
              },
              createdAt: input.createdAt,
            },
            authority,
          ),
        ),
      );

    const formatFailureDetail = (cause: Cause.Cause<unknown>): string => {
      const failReason = cause.reasons.find(Cause.isFailReason);
      const sendTurnError = isProviderSendTurnFailedError(failReason?.error)
        ? failReason.error
        : undefined;
      if (sendTurnError) {
        return sendTurnError.detail;
      }
      const sessionStartTimeoutError = isProviderSessionStartTimeoutError(failReason?.error)
        ? failReason.error
        : undefined;
      if (sessionStartTimeoutError) {
        return sessionStartTimeoutError.detail;
      }
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
      Effect.all({
        commandId: serverCommandId("provider-session-set"),
        authority: authorityForThreadIncludingArchived(input.threadId),
      }).pipe(
        Effect.flatMap(({ commandId, authority }) =>
          orchestrationEngine.dispatch(
            {
              type: "thread.session.set",
              commandId,
              threadId: input.threadId,
              session: input.session,
              createdAt: input.createdAt,
            },
            authority,
          ),
        ),
      );

    const setThreadSessionErrorOnTurnStartFailure = Effect.fnUntraced(function* (input: {
      readonly threadId: ThreadId;
      readonly detail: string;
      readonly modelSelection?: ModelSelection;
      readonly runtimeMode?: RuntimeMode;
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
      const hasLiveProviderSession =
        session.status === "starting" &&
        (yield* providerService.listSessions()).some(
          (providerSession) => providerSession.threadId === input.threadId,
        );
      const shouldRecordStartupError =
        isUnstartedSyntheticErrorSession(thread) ||
        (session.status === "starting" &&
          !hasLiveProviderSession &&
          !thread?.latestTurn?.startedAt);
      yield* setThreadSession({
        threadId: input.threadId,
        session: {
          ...session,
          status:
            session.status === "stopped" && !shouldRecordStartupError
              ? "stopped"
              : shouldRecordStartupError
                ? "error"
                : "ready",
          ...(shouldRecordStartupError
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
            session.status === "stopped" && !shouldRecordStartupError
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

    const isThreadArchivedOrGone = Effect.fnUntraced(function* (threadId: ThreadId) {
      const thread = yield* resolveThread(threadId);
      return !thread || thread.archivedAt !== null;
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

    const failActiveProviderTurn = Effect.fn("ProviderCommandReactor.failActiveProviderTurn")(
      function* (input: {
        readonly threadId: ThreadId;
        readonly turnId: TurnId | null;
        readonly providerTurnId?: TurnId | null;
        readonly detail: string;
        readonly kind: "provider.turn.start.failed" | "provider.turn.interrupt.failed";
        readonly summary: string;
        readonly createdAt: string;
        readonly sessionOwnershipId?: string;
        readonly sendTurnOperationId?: string;
        readonly turnStartRequestId?: EventId;
        readonly turnStartMessageId?: MessageId;
        readonly liveSessionLookup: Option.Option<ProviderSession>;
        readonly modelSelection?: ModelSelection;
        readonly runtimeMode?: RuntimeMode;
      }) {
        const thread = yield* resolveThread(input.threadId);
        if (!thread) {
          return;
        }
        const liveSession = Option.getOrUndefined(input.liveSessionLookup);
        const providerInstanceId =
          thread.session?.providerInstanceId ??
          liveSession?.providerInstanceId ??
          input.modelSelection?.instanceId;
        const providerName =
          thread.session?.providerName ??
          liveSession?.provider ??
          (providerInstanceId !== undefined
            ? yield* providerDriverForInstance(providerInstanceId)
            : null);
        const failedTurnId =
          input.turnId ??
          thread.session?.activeTurnId ??
          liveSession?.activeTurnId ??
          (thread.latestTurn?.state === "running" ? thread.latestTurn.turnId : null);
        if (failedTurnId === null) {
          yield* Effect.logWarning(
            "provider command reactor could not identify failed turn owner",
            {
              threadId: input.threadId,
              kind: input.kind,
            },
          );
          return;
        }
        const targetInterruptedTurn =
          input.kind === "provider.turn.interrupt.failed" ? failedTurnId : null;
        const failedProviderTurnId = input.providerTurnId ?? failedTurnId;
        const providerStopped = yield* providerService
          .stopFailedSession({
            threadId: input.threadId,
            turnId: failedProviderTurnId,
            reason: input.detail,
            ...(input.sessionOwnershipId !== undefined
              ? { sessionOwnershipId: input.sessionOwnershipId }
              : {}),
            ...(input.sendTurnOperationId !== undefined
              ? { sendTurnOperationId: input.sendTurnOperationId }
              : {}),
            ...(input.sessionOwnershipId === undefined ? { requireSessionAbsent: true } : {}),
            onOwned: Effect.gen(function* () {
              yield* setThreadSession({
                threadId: input.threadId,
                session: {
                  threadId: input.threadId,
                  status: "error",
                  providerName,
                  ...(providerInstanceId !== undefined ? { providerInstanceId } : {}),
                  runtimeMode:
                    input.runtimeMode ??
                    thread.session?.runtimeMode ??
                    liveSession?.runtimeMode ??
                    thread.runtimeMode,
                  // The interrupt request projects before this reactor runs.
                  // Preserve its exact turn id for one terminal event so the
                  // projector can upgrade that interrupted turn to failed.
                  activeTurnId: targetInterruptedTurn,
                  lastError: input.detail,
                  updatedAt: input.createdAt,
                },
                createdAt: input.createdAt,
              });
              yield* appendProviderFailureActivity({
                threadId: input.threadId,
                kind: input.kind,
                summary: input.summary,
                detail: input.detail,
                turnId: failedTurnId,
                createdAt: input.createdAt,
                ...(input.turnStartRequestId !== undefined
                  ? { turnStartRequestId: input.turnStartRequestId }
                  : {}),
                ...(input.turnStartMessageId !== undefined
                  ? { turnStartMessageId: input.turnStartMessageId }
                  : {}),
              });
            }),
            onStopped: setThreadSession({
              threadId: input.threadId,
              session: {
                threadId: input.threadId,
                status: "stopped",
                providerName,
                ...(providerInstanceId !== undefined ? { providerInstanceId } : {}),
                runtimeMode:
                  input.runtimeMode ??
                  thread.session?.runtimeMode ??
                  liveSession?.runtimeMode ??
                  thread.runtimeMode,
                activeTurnId: null,
                lastError: input.detail,
                updatedAt: input.createdAt,
              },
              createdAt: input.createdAt,
            }),
          })
          .pipe(
            Effect.timeoutOption(FAILED_TURN_STOP_TIMEOUT),
            Effect.flatMap((result) =>
              Option.isSome(result)
                ? Effect.succeed(result.value)
                : Effect.logWarning(
                    "provider command reactor timed out releasing failed turn session",
                    {
                      threadId: input.threadId,
                      turnId: input.turnId,
                    },
                  ).pipe(Effect.as(false)),
            ),
            Effect.catchCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.failCause(cause)
                : Effect.logWarning(
                    "provider command reactor failed to release failed turn session",
                    {
                      threadId: input.threadId,
                      turnId: input.turnId,
                      cause: Cause.pretty(cause),
                    },
                  ).pipe(Effect.as(false)),
            ),
          );
        if (!providerStopped) {
          yield* Effect.logInfo("provider command reactor skipped stale failed-turn cleanup", {
            threadId: input.threadId,
            turnId: failedTurnId,
          });
        }
      },
    );

    const dispatchArchivedThreadSessionStop = Effect.fnUntraced(function* (input: {
      readonly threadId: ThreadId;
      readonly createdAt: string;
    }) {
      const commandId = yield* serverCommandId("provider-archived-session-stop");
      const authority = yield* authorityForThreadIncludingArchived(input.threadId);
      yield* orchestrationEngine.dispatch(
        {
          type: "thread.session.stop",
          commandId,
          threadId: input.threadId,
          createdAt: input.createdAt,
        },
        authority,
      );
    });
    const archivedTurnStartCancellationDetail = (threadId: ThreadId) =>
      `Thread '${threadId}' was archived before the queued provider turn start could run.`;

    const stopTurnStartIfThreadArchived = Effect.fnUntraced(function* (input: {
      readonly threadId: ThreadId;
      readonly modelSelection?: ModelSelection;
      readonly runtimeMode?: RuntimeMode;
      readonly createdAt: string;
      readonly turnStartRequestId: EventId;
      readonly turnStartMessageId: MessageId;
    }) {
      const latestThread = yield* resolveThread(input.threadId);
      if (!latestThread || latestThread.archivedAt === null) {
        return false;
      }

      const activeSession = yield* resolveActiveSession(input.threadId);
      const hasSessionToStop =
        (latestThread.session !== null && latestThread.session.status !== "stopped") ||
        activeSession !== undefined;
      const cancellationCreatedAt = yield* nowIso;
      if (hasSessionToStop) {
        yield* dispatchArchivedThreadSessionStop({
          threadId: input.threadId,
          createdAt: cancellationCreatedAt,
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("provider command reactor failed to stop archived thread session", {
              threadId: input.threadId,
              cause: Cause.pretty(cause),
            }),
          ),
        );
      } else {
        yield* setThreadSessionErrorOnTurnStartFailure({
          threadId: input.threadId,
          detail: archivedTurnStartCancellationDetail(input.threadId),
          ...(input.modelSelection !== undefined ? { modelSelection: input.modelSelection } : {}),
          ...(input.runtimeMode !== undefined ? { runtimeMode: input.runtimeMode } : {}),
          createdAt: cancellationCreatedAt,
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("provider command reactor failed to mark archived turn cancelled", {
              threadId: input.threadId,
              cause: Cause.pretty(cause),
            }),
          ),
        );
      }
      yield* appendProviderFailureActivity({
        threadId: input.threadId,
        kind: "provider.turn.start.failed",
        summary: "Provider turn start cancelled",
        detail: archivedTurnStartCancellationDetail(input.threadId),
        turnId: null,
        createdAt: cancellationCreatedAt,
        turnStartRequestId: input.turnStartRequestId,
        turnStartMessageId: input.turnStartMessageId,
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning(
            "provider command reactor failed to record archived turn cancellation",
            {
              threadId: input.threadId,
              cause: Cause.pretty(cause),
            },
          ),
        ),
      );
      return true;
    });

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
        readonly providerSessionDetached?: boolean;
        readonly pendingTurnStart?: boolean;
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
        thread.session !== null &&
        thread.session.status !== "stopped" &&
        thread.session.status !== "error" &&
        activeSession
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
      if (options?.pendingTurnStart === true && thread.session?.status !== "running") {
        yield* setThreadSession({
          threadId,
          session: {
            threadId,
            status: "starting",
            providerName: activeSession?.provider ?? preferredProvider,
            providerInstanceId: activeSession?.providerInstanceId ?? desiredInstanceId,
            runtimeMode: desiredRuntimeMode,
            activeTurnId: null,
            lastError: null,
            updatedAt: createdAt,
          },
          createdAt,
        });
      }
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
          ...(options?.providerSessionDetached !== undefined
            ? { detached: options.providerSessionDetached }
            : {}),
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
              status:
                options?.pendingTurnStart === true && session.status === "ready"
                  ? "starting"
                  : mapProviderSessionStatusToOrchestrationStatus(session.status),
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

      const existingSessionThreadId = activeThreadSession !== null ? thread.id : null;
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
      readonly providerSessionDetached?: boolean;
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
        ...(input.providerSessionDetached !== undefined
          ? { providerSessionDetached: input.providerSessionDetached }
          : {}),
        pendingTurnStart: true,
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
        const initialSnapshot = yield* projectionSnapshotQuery.getSnapshot();
        const initialThread = initialSnapshot.threads.find(
          (thread) => thread.id === input.threadId,
        );
        const initialWorktreeIdentity = worktreeIdentityPath({
          worktreePath: input.worktreePath,
          worktreeRemovalPath: initialThread?.worktreeRemovalPath,
        });
        if (
          !initialThread ||
          initialThread.archivedAt !== null ||
          initialThread.deletedAt !== null ||
          initialSnapshot.threads.some(
            (thread) =>
              thread.id !== input.threadId &&
              initialWorktreeIdentity !== null &&
              worktreeIdentityPath(thread) === initialWorktreeIdentity,
          )
        ) {
          return;
        }

        const settings = yield* serverSettingsService.getSettings;
        const modelSelection =
          settings.sourceControlWriterModelSelection === null
            ? settings.textGenerationModelSelection
            : resolveSourceControlWriterModelSelection(
                settings,
                yield* providerRegistry.getProviders,
              );
        const generated = yield* textGeneration.generateBranchName({
          cwd,
          message: input.messageText,
          ...(attachments.length > 0 ? { attachments } : {}),
          modelSelection,
        });
        if (!generated) return;

        const targetBranch = buildGeneratedWorktreeBranchName(generated.branch);
        if (targetBranch === oldBranch) return;

        yield* worktreeLifecycle.withPermit(
          Effect.gen(function* () {
            const snapshot = yield* projectionSnapshotQuery.getSnapshot();
            const currentThread = snapshot.threads.find((thread) => thread.id === input.threadId);
            if (
              !currentThread ||
              currentThread.archivedAt !== null ||
              currentThread.deletedAt !== null ||
              currentThread.branch !== oldBranch ||
              currentThread.worktreePath !== cwd
            ) {
              return;
            }
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

            if (yield* isThreadArchivedOrGone(input.threadId)) return;

            const renamed = yield* gitWorkflow.renameBranch({
              cwd,
              oldBranch,
              newBranch: targetBranch,
            });
            yield* dispatchAlreadyCoordinated(
              orchestrationEngine,
              {
                type: "thread.meta.update",
                commandId: yield* serverCommandId("worktree-branch-rename"),
                threadId: input.threadId,
                branch: renamed.branch,
                worktreePath: cwd,
              },
              threadAudienceSystemDispatchAuthority(currentThread, "ProviderCommandReactor"),
            );
            yield* vcsStatusBroadcaster.refreshStatus(cwd).pipe(Effect.ignoreCause({ log: true }));
          }),
        );
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning(
            "provider command reactor failed to generate or rename worktree branch",
            {
              threadId: input.threadId,
              cwd,
              oldBranch,
              cause: Cause.pretty(cause),
            },
          ),
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
          if (yield* isThreadArchivedOrGone(input.threadId)) {
            return;
          }
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
          if (thread.archivedAt !== null) {
            return;
          }
          if (!canReplaceThreadTitle(thread.title, input.titleSeed)) {
            return;
          }

          yield* orchestrationEngine.dispatch(
            {
              type: "thread.meta.update",
              commandId: yield* serverCommandId("thread-title-rename"),
              threadId: input.threadId,
              title: generated.title,
            },
            threadAudienceSystemDispatchAuthority(thread, "ProviderCommandReactor"),
          );
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning(
              "provider command reactor failed to generate or rename thread title",
              {
                threadId: input.threadId,
                cwd: input.cwd,
                cause: Cause.pretty(cause),
              },
            ),
          ),
        );
      },
    );

    const regenerateThreadTitle = Effect.fn("regenerateThreadTitle")(function* (
      event: Extract<ProviderIntentEvent, { type: "thread.meta-updated" }>,
      requestId: CommandId,
    ) {
      if (event.payload.regenerateTitle !== true) {
        return { _tag: "Superseded" } as const;
      }

      const thread = yield* resolveThread(event.payload.threadId);
      if (!thread || thread.titleRegeneration?.requestId !== requestId) {
        return { _tag: "Superseded" } as const;
      }

      const { message, attachments } = formatThreadTitleContext(thread.messages);
      if (message.length === 0) {
        return { _tag: "Completed", title: undefined } as const;
      }

      const previousTitle = event.payload.previousTitle ?? thread.title;
      if (thread.title !== previousTitle) {
        return { _tag: "Superseded" } as const;
      }
      const project = yield* resolveProject(thread.projectId);
      const cwd =
        resolveThreadWorkspaceCwd({
          thread,
          projects: project ? [project] : [],
        }) ?? process.cwd();
      const { textGenerationModelSelection: modelSelection } =
        yield* serverSettingsService.getSettings;
      const generated = yield* textGeneration.generateThreadTitle({
        cwd,
        message,
        previousTitle,
        ...(attachments.length > 0 ? { attachments } : {}),
        modelSelection,
      });
      if (generated.title === DEFAULT_THREAD_TITLE || generated.title === previousTitle) {
        return { _tag: "Completed", title: undefined } as const;
      }

      const latestThread = yield* resolveThread(event.payload.threadId);
      if (
        !latestThread ||
        latestThread.titleRegeneration?.requestId !== requestId ||
        latestThread.title !== previousTitle
      ) {
        return { _tag: "Superseded" } as const;
      }

      return { _tag: "Completed", title: generated.title } as const;
    });
    const dispatchThreadTitleRegenerationCompletion = Effect.fn(
      "dispatchThreadTitleRegenerationCompletion",
    )(function* (input: {
      readonly threadId: ThreadId;
      readonly requestId: CommandId;
      readonly title?: string;
    }) {
      const authority = yield* authorityForThreadIncludingArchived(input.threadId);
      yield* orchestrationEngine.dispatch(
        {
          type: "thread.title.regeneration.complete",
          commandId: yield* serverCommandId("thread-title-regeneration-complete"),
          threadId: input.threadId,
          requestId: input.requestId,
          ...(input.title !== undefined ? { title: input.title } : {}),
        },
        authority,
      );
    });
    const findInterruptedThreadTitleRegenerations = Effect.fn(
      "findInterruptedThreadTitleRegenerations",
    )(function* () {
      const readModel = yield* projectionSnapshotQuery.getCommandReadModel();
      return readModel.threads.flatMap((thread) => {
        const requestId = thread.titleRegeneration?.requestId;
        return requestId === undefined ? [] : [{ threadId: thread.id, requestId }];
      });
    });
    const clearInterruptedThreadTitleRegenerations = Effect.fn(
      "clearInterruptedThreadTitleRegenerations",
    )(function* (
      interrupted: ReadonlyArray<{ readonly threadId: ThreadId; readonly requestId: CommandId }>,
    ) {
      yield* Effect.forEach(
        interrupted,
        ({ threadId, requestId }) => {
          return dispatchThreadTitleRegenerationCompletion({
            threadId,
            requestId,
          }).pipe(
            Effect.catchCause((cause) => {
              if (Cause.hasInterruptsOnly(cause)) {
                return Effect.interrupt;
              }
              return Effect.logWarning(
                "provider command reactor failed to clear interrupted title regeneration",
                {
                  threadId,
                  cause: Cause.pretty(cause),
                },
              );
            }),
          );
        },
        { discard: true },
      );
    });
    const processThreadTitleRegenerationSafely = Effect.fn("processThreadTitleRegenerationSafely")(
      function* (event: Extract<ProviderIntentEvent, { type: "thread.meta-updated" }>) {
        if (event.payload.regenerateTitle !== true) {
          return;
        }

        const requestId = event.payload.titleRegeneration?.requestId ?? event.commandId;
        if (requestId === null) {
          return;
        }
        const result = yield* regenerateThreadTitle(event, requestId).pipe(
          Effect.catchCause((cause) => {
            if (Cause.hasInterruptsOnly(cause)) {
              return Effect.failCause(cause);
            }
            return Effect.logWarning("provider command reactor failed to regenerate thread title", {
              threadId: event.payload.threadId,
              cause: Cause.pretty(cause),
            }).pipe(Effect.as({ _tag: "Completed", title: undefined } as const));
          }),
        );
        if (result._tag === "Superseded") {
          return;
        }

        const completion = {
          threadId: event.payload.threadId,
          requestId,
          ...(result.title !== undefined ? { title: result.title } : {}),
        };
        yield* dispatchThreadTitleRegenerationCompletion(completion).pipe(
          Effect.catchCause((cause) => {
            if (Cause.hasInterruptsOnly(cause)) {
              return Effect.failCause(cause);
            }
            return Effect.logWarning(
              "provider command reactor retrying title regeneration completion",
              {
                threadId: event.payload.threadId,
                cause: Cause.pretty(cause),
              },
            ).pipe(Effect.andThen(dispatchThreadTitleRegenerationCompletion(completion)));
          }),
        );
      },
      (effect, event) =>
        effect.pipe(
          Effect.catchCause((cause) => {
            if (Cause.hasInterruptsOnly(cause)) {
              return Effect.failCause(cause);
            }
            return Effect.logWarning(
              "provider command reactor failed to complete title regeneration",
              {
                threadId: event.payload.threadId,
                cause: Cause.pretty(cause),
              },
            );
          }),
        ),
    );
    const threadTitleRegenerationWorker = yield* makeDrainableWorker(
      processThreadTitleRegenerationSafely,
    );

    const processTurnStartRequested = Effect.fn("processTurnStartRequested")(function* (
      event: Extract<ProviderIntentEvent, { type: "thread.turn-start-requested" }>,
    ) {
      const key = turnStartKeyForEvent(event);
      if (yield* hasHandledTurnStartRecently(key)) {
        return;
      }

      const thread = yield* resolveThread(event.payload.threadId);
      if (!thread) {
        return;
      }
      const requestedOrchestrationTurnId =
        thread.latestTurn?.state === "running" ? thread.latestTurn.turnId : null;
      const message = thread.messages.find((entry) => entry.id === event.payload.messageId);
      if (!message || (message.role !== "user" && message.role !== "system")) {
        yield* appendProviderFailureActivity({
          threadId: event.payload.threadId,
          kind: "provider.turn.start.failed",
          summary: "Provider turn start failed",
          detail: `Turn start message '${event.payload.messageId}' was not found for turn start request.`,
          turnId: null,
          createdAt: event.payload.createdAt,
          turnStartRequestId: event.eventId,
          turnStartMessageId: event.payload.messageId,
        });
        return;
      }
      if (
        yield* stopTurnStartIfThreadArchived({
          threadId: event.payload.threadId,
          ...(event.payload.modelSelection !== undefined
            ? { modelSelection: event.payload.modelSelection }
            : {}),
          runtimeMode: event.payload.runtimeMode,
          createdAt: event.payload.createdAt,
          turnStartRequestId: event.eventId,
          turnStartMessageId: event.payload.messageId,
        })
      ) {
        return;
      }

      const isFirstUserMessageTurn =
        message.role === "user" &&
        thread.messages.filter((entry) => entry.role === "user").length === 1;
      if (
        isFirstUserMessageTurn &&
        (yield* stopTurnStartIfThreadArchived({
          threadId: event.payload.threadId,
          ...(event.payload.modelSelection !== undefined
            ? { modelSelection: event.payload.modelSelection }
            : {}),
          runtimeMode: event.payload.runtimeMode,
          createdAt: event.payload.createdAt,
          turnStartRequestId: event.eventId,
          turnStartMessageId: event.payload.messageId,
        }))
      ) {
        return;
      }
      if (isFirstUserMessageTurn) {
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
      }
      if (
        yield* stopTurnStartIfThreadArchived({
          threadId: event.payload.threadId,
          ...(event.payload.modelSelection !== undefined
            ? { modelSelection: event.payload.modelSelection }
            : {}),
          runtimeMode: event.payload.runtimeMode,
          createdAt: event.payload.createdAt,
          turnStartRequestId: event.eventId,
          turnStartMessageId: event.payload.messageId,
        })
      ) {
        return;
      }

      const handleTurnStartFailure = Effect.fnUntraced(function* (cause: Cause.Cause<unknown>) {
        if (Cause.hasInterruptsOnly(cause)) {
          return;
        }
        const sendTurnFailure = findProviderSendTurnFailedError(cause);
        if (sendTurnFailure?.overlapping === true) {
          const failedAt = yield* nowIso;
          yield* appendProviderFailureActivity({
            threadId: event.payload.threadId,
            kind: "provider.turn.start.failed",
            summary: "Provider turn start failed",
            detail: sendTurnFailure.detail,
            turnId: null,
            createdAt: failedAt,
            turnStartRequestId: event.eventId,
            turnStartMessageId: event.payload.messageId,
          });
          return;
        }
        if (sendTurnFailure?.superseded === true) {
          yield* Effect.logInfo("provider command reactor ignored superseded send-turn failure", {
            threadId: event.payload.threadId,
          });
          return;
        }
        if (sendTurnFailure?.preservedActiveTurnId !== undefined) {
          const failedAt = yield* nowIso;
          yield* appendProviderFailureActivity({
            threadId: event.payload.threadId,
            kind: "provider.turn.start.failed",
            summary: "Provider turn start failed",
            detail: sendTurnFailure.detail,
            turnId: null,
            createdAt: failedAt,
            turnStartRequestId: event.eventId,
            turnStartMessageId: event.payload.messageId,
          });
          return;
        }
        const detail = formatFailureDetail(cause);
        const sessionStartTimeoutFailure = findProviderSessionStartTimeoutError(cause);
        const failedAt = yield* nowIso;
        const failedThread = yield* resolveThread(event.payload.threadId);
        const liveSession =
          sendTurnFailure !== undefined
            ? undefined
            : yield* resolveActiveSession(event.payload.threadId).pipe(
                Effect.timeoutOption(FAILED_TURN_SESSION_LOOKUP_TIMEOUT),
                Effect.map(Option.getOrUndefined),
                Effect.catchCause((lookupCause) =>
                  Cause.hasInterruptsOnly(lookupCause)
                    ? Effect.failCause(lookupCause)
                    : Effect.logWarning(
                        "provider command reactor could not enumerate sessions during failure recovery",
                        {
                          threadId: event.payload.threadId,
                          cause: Cause.pretty(lookupCause),
                        },
                      ).pipe(Effect.as(undefined)),
                ),
              );
        const projectedActiveTurnId = failedThread?.session?.activeTurnId ?? null;
        const liveActiveTurnId = liveSession?.activeTurnId ?? null;
        const activeTurnId =
          sendTurnFailure?.turnId !== undefined
            ? TurnId.make(sendTurnFailure.turnId)
            : sendTurnFailure !== undefined
              ? requestedOrchestrationTurnId
              : (projectedActiveTurnId ?? liveActiveTurnId);
        if (activeTurnId !== null) {
          return yield* failActiveProviderTurn({
            threadId: event.payload.threadId,
            turnId: activeTurnId,
            detail,
            kind: "provider.turn.start.failed",
            summary: "Provider turn failed",
            createdAt: failedAt,
            liveSessionLookup: Option.fromUndefinedOr(liveSession),
            ...(sendTurnFailure !== undefined
              ? {
                  sessionOwnershipId: sendTurnFailure.sessionOwnershipId,
                  ...(sendTurnFailure.sendTurnOperationId !== undefined
                    ? { sendTurnOperationId: sendTurnFailure.sendTurnOperationId }
                    : {}),
                }
              : {}),
            ...(sessionStartTimeoutFailure?.sessionOwnershipId !== undefined
              ? { sessionOwnershipId: sessionStartTimeoutFailure.sessionOwnershipId }
              : {}),
            ...(event.payload.modelSelection !== undefined
              ? { modelSelection: event.payload.modelSelection }
              : {}),
            runtimeMode: event.payload.runtimeMode,
            turnStartRequestId: event.eventId,
            turnStartMessageId: event.payload.messageId,
          });
        }

        yield* setThreadSessionErrorOnTurnStartFailure({
          threadId: event.payload.threadId,
          detail,
          ...(event.payload.modelSelection !== undefined
            ? { modelSelection: event.payload.modelSelection }
            : {}),
          runtimeMode: event.payload.runtimeMode,
          createdAt: failedAt,
        });
        yield* appendProviderFailureActivity({
          threadId: event.payload.threadId,
          kind: "provider.turn.start.failed",
          summary: "Provider turn start failed",
          detail,
          turnId: null,
          createdAt: failedAt,
          turnStartRequestId: event.eventId,
          turnStartMessageId: event.payload.messageId,
        });
      });

      const recoverTurnStartFailure = (cause: Cause.Cause<unknown>) =>
        handleTurnStartFailure(cause).pipe(
          Effect.catchCause((recoveryCause) =>
            Effect.logWarning("provider command reactor failed to recover turn start failure", {
              eventType: event.type,
              threadId: event.payload.threadId,
              cause: Cause.pretty(recoveryCause),
              originalCause: Cause.pretty(cause),
            }),
          ),
        );

      const prepareProviderTurn = Effect.gen(function* () {
        if (
          yield* stopTurnStartIfThreadArchived({
            threadId: event.payload.threadId,
            createdAt: event.payload.createdAt,
            turnStartRequestId: event.eventId,
            turnStartMessageId: event.payload.messageId,
          })
        ) {
          return Option.none();
        }

        const sendTurnRequest = yield* buildSendTurnRequestForThread({
          threadId: event.payload.threadId,
          messageText: message.text,
          ...(message.attachments !== undefined ? { attachments: message.attachments } : {}),
          ...(event.payload.modelSelection !== undefined
            ? { modelSelection: event.payload.modelSelection }
            : {}),
          runtimeMode: event.payload.runtimeMode,
          interactionMode: event.payload.interactionMode,
          ...(event.payload.providerSessionDetached !== undefined
            ? { providerSessionDetached: event.payload.providerSessionDetached }
            : {}),
          createdAt: event.payload.createdAt,
        }).pipe(
          Effect.map(Option.some),
          Effect.catchCause((cause) =>
            handleTurnStartFailure(cause).pipe(Effect.as(Option.none())),
          ),
        );
        if (Option.isNone(sendTurnRequest)) {
          return Option.none();
        }
        if (
          yield* stopTurnStartIfThreadArchived({
            threadId: event.payload.threadId,
            createdAt: event.payload.createdAt,
            turnStartRequestId: event.eventId,
            turnStartMessageId: event.payload.messageId,
          })
        ) {
          return Option.none();
        }
        return sendTurnRequest;
      });

      // The worktree-lifecycle permit gates every thread's turn-start dispatch
      // through a single global slot. It must fence worktree teardown for the
      // preparation above AND the adapter's turn registration — but it must not
      // be held for the whole provider RPC: grok's ACP session/prompt resolves
      // only at turn end, and holding the permit across it let one provider
      // stall turn starts server-wide. So the permit is held until sendTurn
      // resolves or a bounded grace window elapses, whichever comes first; the
      // forked sendTurn continues (with its own failure recovery) either way.
      yield* Effect.suspend(() =>
        worktreeLifecycle.withPermit(
          prepareProviderTurn.pipe(
            Effect.flatMap((sendTurnRequest) =>
              Option.isSome(sendTurnRequest)
                ? providerService.sendTurn(sendTurnRequest.value).pipe(
                    Effect.catchCause(recoverTurnStartFailure),
                    Effect.forkScoped,
                    Effect.flatMap((sendTurnFiber) =>
                      Fiber.await(sendTurnFiber).pipe(
                        Effect.timeoutOption(turnStartLifecycleGraceMs),
                        Effect.asVoid,
                      ),
                    ),
                  )
                : Effect.void,
            ),
          ),
        ),
      ).pipe(Effect.catchCause(recoverTurnStartFailure), Effect.forkScoped);
    });

    const processTurnInterruptRequested = Effect.fn("processTurnInterruptRequested")(function* (
      event: Extract<ProviderIntentEvent, { type: "thread.turn-interrupt-requested" }>,
    ) {
      const thread = yield* resolveThread(event.payload.threadId);
      if (!thread) {
        return;
      }
      const activeSession = yield* resolveActiveSession(event.payload.threadId);
      const projectedActiveTurnId = thread.session?.activeTurnId ?? null;
      const projectedTurnIsRunning =
        projectedActiveTurnId !== null &&
        (thread.session?.status === "running" || thread.session?.status === "waiting");
      if (activeSession === undefined && projectedTurnIsRunning) {
        return yield* failActiveProviderTurn({
          threadId: event.payload.threadId,
          turnId: event.payload.turnId ?? projectedActiveTurnId,
          providerTurnId: projectedActiveTurnId,
          detail: "Provider session is no longer alive; the running turn was force-failed.",
          kind: "provider.turn.interrupt.failed",
          summary: "Dead provider turn force-failed",
          createdAt: event.payload.createdAt,
          liveSessionLookup: Option.none(),
        });
      }
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
      yield* providerService.interruptTurn({ threadId: event.payload.threadId }).pipe(
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.failCause(cause)
            : failActiveProviderTurn({
                threadId: event.payload.threadId,
                turnId: event.payload.turnId ?? projectedActiveTurnId,
                providerTurnId: projectedActiveTurnId,
                detail: `Provider turn interrupt failed because the session is unavailable: ${formatFailureDetail(cause)}`,
                kind: "provider.turn.interrupt.failed",
                summary: "Provider turn interrupt force-failed",
                createdAt: event.payload.createdAt,
                liveSessionLookup: Option.fromUndefinedOr(activeSession),
              }),
        ),
      );
    });

    const processApprovalResponseRequested = Effect.fn("processApprovalResponseRequested")(
      function* (
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
      },
    );

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
      const thread = yield* resolveThread(event.payload.threadId);
      if (!thread) {
        return;
      }

      const now = event.payload.createdAt;
      const activeSession = yield* resolveActiveSession(thread.id);
      const shouldStopProjectedSession =
        thread.session &&
        thread.session.status !== "stopped" &&
        !(activeSession === undefined && isUnstartedSyntheticErrorSession(thread));
      if (activeSession !== undefined || shouldStopProjectedSession) {
        yield* providerService.stopSession({ threadId: thread.id });
      }
      const providerInstanceId =
        thread.session?.providerInstanceId ?? activeSession?.providerInstanceId;

      yield* setThreadSession({
        threadId: thread.id,
        session: {
          threadId: thread.id,
          status: "stopped",
          providerName: thread.session?.providerName ?? activeSession?.provider ?? null,
          ...(providerInstanceId !== undefined ? { providerInstanceId } : {}),
          runtimeMode:
            thread.session?.runtimeMode ?? activeSession?.runtimeMode ?? DEFAULT_RUNTIME_MODE,
          activeTurnId: null,
          lastError: thread.session?.lastError ?? null,
          updatedAt: now,
        },
        createdAt: now,
      });
    });

    const processDomainEvent = Effect.fn("processDomainEvent")(function* (
      event: ProviderIntentEvent,
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
        case "thread.meta-updated":
          yield* threadTitleRegenerationWorker.enqueue(event);
          return;
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
          }).pipe(
            Effect.catchCause((cause) => {
              const sessionStartTimeoutFailure = findProviderSessionStartTimeoutError(cause);
              const activeTurnId =
                thread.session?.activeTurnId ?? activeSession?.activeTurnId ?? null;
              if (sessionStartTimeoutFailure === undefined || activeTurnId === null) {
                return Effect.failCause(cause);
              }
              return failActiveProviderTurn({
                threadId: event.payload.threadId,
                turnId: activeTurnId,
                detail: sessionStartTimeoutFailure.detail,
                kind: "provider.turn.start.failed",
                summary: "Provider session restart timed out",
                createdAt: event.occurredAt,
                liveSessionLookup: Option.fromUndefinedOr(activeSession),
                ...(sessionStartTimeoutFailure.sessionOwnershipId !== undefined
                  ? { sessionOwnershipId: sessionStartTimeoutFailure.sessionOwnershipId }
                  : {}),
                runtimeMode: event.payload.runtimeMode,
              });
            }),
          );
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
      }
    });

    const processDomainEventSafely = (event: ProviderIntentEvent) =>
      Effect.suspend(() => processDomainEvent(event)).pipe(
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
      const interruptedTitleRegenerations = yield* findInterruptedThreadTitleRegenerations().pipe(
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.interrupt;
          }
          return Effect.logWarning(
            "provider command reactor failed to find interrupted title regenerations",
            { cause: Cause.pretty(cause) },
          ).pipe(Effect.as([]));
        }),
      );
      const processEvent = Effect.fn("processEvent")(function* (event: OrchestrationEvent) {
        if (
          (event.type === "thread.meta-updated" && event.payload.regenerateTitle === true) ||
          event.type === "thread.runtime-mode-set" ||
          event.type === "thread.turn-start-requested" ||
          event.type === "thread.turn-interrupt-requested" ||
          event.type === "thread.approval-response-requested" ||
          event.type === "thread.user-input-response-requested" ||
          event.type === "thread.session-stop-requested"
        ) {
          return yield* worker.enqueue(event);
        }
      });

      yield* forkParked(Stream.runForEach(orchestrationEngine.streamDomainEvents, processEvent));

      // The domain event stream is hot, so work pending before this reactor
      // starts cannot be resumed. Correlated completions only clear the request
      // captured here, leaving any newer request untouched.
      const clearInterrupted = clearInterruptedThreadTitleRegenerations(
        interruptedTitleRegenerations,
      ).pipe(
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.interrupt;
          }
          return Effect.logWarning(
            "provider command reactor failed to clear interrupted title regenerations",
            {
              cause: Cause.pretty(cause),
            },
          );
        }),
      );
      const activation = yield* ServerActivation;
      if (activation === undefined) {
        yield* clearInterrupted;
      } else {
        yield* forkParked(clearInterrupted);
      }
    });

    return {
      start,
      drain: Effect.gen(function* () {
        yield* worker.drain;
        yield* threadTitleRegenerationWorker.drain;
      }),
    } satisfies ProviderCommandReactorShape;
  });

export const makeProviderCommandReactorLive = (options?: ProviderCommandReactorLiveOptions) =>
  Layer.effect(ProviderCommandReactor, makeReactor(options));

export const ProviderCommandReactorLive = makeProviderCommandReactorLive();
