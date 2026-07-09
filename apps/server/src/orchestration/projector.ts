import type {
  MessageId,
  OrchestrationEvent,
  OrchestrationLatestTurn,
  OrchestrationReadModel,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import {
  OrchestrationCheckpointSummary,
  OrchestrationMessage,
  OrchestrationSession,
  OrchestrationThread,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { toProjectorDecodeError, type OrchestrationProjectorDecodeError } from "./Errors.ts";
import { MAX_THREAD_CHECKPOINTS } from "./checkpointRetention.ts";
import {
  MessageSentPayloadSchema,
  ProjectCreatedPayload,
  ProjectDeletedPayload,
  ProjectMetaUpdatedPayload,
  ThreadActivityAppendedPayload,
  ThreadArchivedPayload,
  ThreadCreatedPayload,
  ThreadDeletedPayload,
  ThreadInteractionModeSetPayload,
  ThreadMetaUpdatedPayload,
  ThreadProposedPlanUpsertedPayload,
  ThreadRuntimeModeSetPayload,
  ThreadParentSetPayload,
  ThreadUnarchivedPayload,
  ThreadRevertedPayload,
  ThreadSessionSetPayload,
  ThreadTurnDiffCompletedPayload,
} from "./Schemas.ts";

type ThreadPatch = Partial<Omit<OrchestrationThread, "id" | "projectId">>;
const MAX_THREAD_MESSAGES = 2_000;

function checkpointStatusToLatestTurnState(status: "ready" | "missing" | "error") {
  if (status === "error") return "error" as const;
  if (status === "missing") return "interrupted" as const;
  return "completed" as const;
}

/**
 * Turn state to settle a still-running latest turn with when its session
 * leaves the "running" status, or null while the session is (re)starting or
 * running and the turn must stay unsettled.
 */
function settledTurnStateForSessionStatus(
  status: OrchestrationSession["status"],
): "completed" | "interrupted" | "error" | null {
  switch (status) {
    case "idle":
    case "ready":
      return "completed";
    case "error":
      return "error";
    case "interrupted":
    case "stopped":
      return "interrupted";
    case "starting":
    case "running":
    case "waiting":
      return null;
  }
}

function upsertCheckpointRebindingAssistantMessage(
  checkpoints: ReadonlyArray<OrchestrationCheckpointSummary>,
  checkpoint: OrchestrationCheckpointSummary,
): OrchestrationCheckpointSummary[] {
  const assistantMessageId = checkpoint.assistantMessageId;
  return [
    ...checkpoints
      .filter((entry) => entry.turnId !== checkpoint.turnId)
      .map((entry) =>
        assistantMessageId !== null && entry.assistantMessageId === assistantMessageId
          ? { ...entry, assistantMessageId: null }
          : entry,
      ),
    checkpoint,
  ]
    .toSorted((left, right) => left.checkpointTurnCount - right.checkpointTurnCount)
    .slice(-MAX_THREAD_CHECKPOINTS);
}

function rebindCheckpointAssistantMessage(
  checkpoints: ReadonlyArray<OrchestrationCheckpointSummary>,
  turnId: TurnId,
  messageId: MessageId,
): OrchestrationCheckpointSummary[] {
  return checkpoints.map((entry) =>
    entry.turnId === turnId
      ? { ...entry, assistantMessageId: messageId }
      : entry.assistantMessageId === messageId
        ? { ...entry, assistantMessageId: null }
        : entry,
  );
}

function rebindLatestTurnAssistantMessage(
  latestTurn: OrchestrationThread["latestTurn"],
  turnId: TurnId,
  messageId: MessageId,
): OrchestrationThread["latestTurn"] {
  if (latestTurn === null) {
    return null;
  }
  if (latestTurn.turnId === turnId) {
    return { ...latestTurn, assistantMessageId: messageId };
  }
  if (latestTurn.assistantMessageId === messageId) {
    return { ...latestTurn, assistantMessageId: null };
  }
  return latestTurn;
}

function clearLatestTurnAssistantMessageForTurn(
  latestTurn: OrchestrationThread["latestTurn"],
  turnId: TurnId,
  messageId: MessageId,
): OrchestrationThread["latestTurn"] {
  if (
    latestTurn !== null &&
    latestTurn.turnId === turnId &&
    latestTurn.assistantMessageId === messageId
  ) {
    return { ...latestTurn, assistantMessageId: null };
  }
  return latestTurn;
}

function updateThread(
  threads: ReadonlyArray<OrchestrationThread>,
  threadId: ThreadId,
  patch: ThreadPatch,
): OrchestrationThread[] {
  return threads.map((thread) => (thread.id === threadId ? { ...thread, ...patch } : thread));
}

function decodeForEvent<A>(
  schema: Schema.Decoder<A, never>,
  value: unknown,
  eventType: OrchestrationEvent["type"],
  field: string,
): Effect.Effect<A, OrchestrationProjectorDecodeError> {
  return Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError(toProjectorDecodeError(`${eventType}:${field}`)),
  );
}

function retainThreadMessagesAfterRevert(
  messages: ReadonlyArray<OrchestrationMessage>,
  retainedTurnIds: ReadonlySet<string>,
  turnCount: number,
): ReadonlyArray<OrchestrationMessage> {
  const retainedMessageIds = new Set<string>();
  for (const message of messages) {
    if (message.role === "system" && message.turnId === null) {
      if (retainedMessageIds.has(message.id) || !isSubAgentWakeSystemMessageText(message.text)) {
        retainedMessageIds.add(message.id);
      }
      continue;
    }
    if (message.turnId !== null && retainedTurnIds.has(message.turnId)) {
      retainedMessageIds.add(message.id);
    }
  }

  const retainedPromptCount = messages.filter(
    (message) => isThreadPromptMessage(message) && retainedMessageIds.has(message.id),
  ).length;
  const missingPromptCount = Math.max(0, turnCount - retainedPromptCount);
  if (missingPromptCount > 0) {
    const fallbackUserMessages = messages
      .filter(
        (message) =>
          message.role === "user" &&
          !retainedMessageIds.has(message.id) &&
          (message.turnId === null || retainedTurnIds.has(message.turnId)),
      )
      .toSorted(
        (left, right) =>
          left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
      )
      .slice(0, missingPromptCount);
    for (const message of fallbackUserMessages) {
      retainedMessageIds.add(message.id);
    }
  }

  const retainedAssistantCount = messages.filter(
    (message) => message.role === "assistant" && retainedMessageIds.has(message.id),
  ).length;
  const missingAssistantCount = Math.max(0, turnCount - retainedAssistantCount);
  if (missingAssistantCount > 0) {
    const fallbackAssistantMessages = messages
      .filter(
        (message) =>
          message.role === "assistant" &&
          !retainedMessageIds.has(message.id) &&
          (message.turnId === null || retainedTurnIds.has(message.turnId)),
      )
      .toSorted(
        (left, right) =>
          left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
      )
      .slice(0, missingAssistantCount);
    for (const message of fallbackAssistantMessages) {
      retainedMessageIds.add(message.id);
    }
  }

  return messages.filter((message) => retainedMessageIds.has(message.id));
}

function bindNextPendingPromptMessageToTurn(
  messages: ReadonlyArray<OrchestrationMessage>,
  turnId: TurnId,
  failedPromptMessageIds: ReadonlySet<string>,
): ReadonlyArray<OrchestrationMessage> {
  const pendingIndex = messages.findIndex((message) => {
    if (message.turnId !== null || !isThreadPromptMessage(message)) {
      return false;
    }
    return !failedPromptMessageIds.has(message.id);
  });
  if (pendingIndex === -1) return messages;
  return messages.map((message, index) =>
    index === pendingIndex ? { ...message, turnId } : message,
  );
}

function failedTurnStartPromptMessageIds(
  messages: ReadonlyArray<OrchestrationMessage>,
  activities: OrchestrationThread["activities"],
): ReadonlySet<string> {
  const ids = new Set<string>();
  const pendingPromptMessages = messages.filter(
    (message) => message.turnId === null && isThreadPromptMessage(message),
  );
  const failedActivities = activities
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

function latestAssistantMessageIdForTurn(
  messages: ReadonlyArray<OrchestrationMessage>,
  turnId: TurnId,
): MessageId | null {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message && message.role === "assistant" && message.turnId === turnId) {
      return message.id;
    }
  }
  return null;
}

function assistantMessageCanAdvanceTurnBoundary(input: {
  readonly messages: ReadonlyArray<OrchestrationMessage>;
  readonly turnId: TurnId;
  readonly candidateMessageId: MessageId;
  readonly currentMessageId: MessageId | null;
}): boolean {
  if (input.currentMessageId === null || input.currentMessageId === input.candidateMessageId) {
    return true;
  }
  const candidate = input.messages.find((message) => message.id === input.candidateMessageId);
  const current = input.messages.find((message) => message.id === input.currentMessageId);
  if (candidate === undefined || current === undefined) {
    return false;
  }
  if (candidate.turnId !== input.turnId) {
    return false;
  }
  if (current.turnId !== input.turnId) {
    return true;
  }
  return compareMessageOrder(candidate, current) >= 0;
}

function compareMessageOrder(left: OrchestrationMessage, right: OrchestrationMessage): number {
  if (left.createdAt !== right.createdAt) {
    return left.createdAt < right.createdAt ? -1 : 1;
  }
  if (left.updatedAt !== right.updatedAt) {
    return left.updatedAt < right.updatedAt ? -1 : 1;
  }
  if (left.id !== right.id) {
    return left.id < right.id ? -1 : 1;
  }
  return 0;
}

function compareTurnOrder(left: OrchestrationLatestTurn, right: OrchestrationLatestTurn): number {
  return (
    left.requestedAt.localeCompare(right.requestedAt) ||
    (left.startedAt ?? "").localeCompare(right.startedAt ?? "") ||
    left.turnId.localeCompare(right.turnId)
  );
}

function upsertTurn(
  turns: ReadonlyArray<OrchestrationLatestTurn>,
  turn: OrchestrationLatestTurn | null,
): OrchestrationLatestTurn[] {
  if (turn === null) {
    return [...turns];
  }
  return [...turns.filter((entry) => entry.turnId !== turn.turnId), turn].toSorted(
    compareTurnOrder,
  );
}

function settleRunningTurnsForSession(input: {
  readonly turns: ReadonlyArray<OrchestrationLatestTurn>;
  readonly activeTurnId: TurnId | null;
  readonly settledTurnState: OrchestrationLatestTurn["state"] | null;
  readonly settledAt: string;
}): OrchestrationLatestTurn[] {
  if (input.activeTurnId !== null) {
    return input.turns.map((turn) =>
      turn.turnId !== input.activeTurnId && turn.state === "running"
        ? { ...turn, state: "completed", completedAt: input.settledAt }
        : turn,
    );
  }
  if (input.settledTurnState === null) {
    return [...input.turns];
  }
  const settledTurnState = input.settledTurnState;
  return input.turns.map((turn) =>
    turn.state === "running"
      ? { ...turn, state: settledTurnState, completedAt: input.settledAt }
      : turn,
  );
}

function rebindTurnAssistantMessage(
  turns: ReadonlyArray<OrchestrationLatestTurn>,
  turnId: TurnId,
  messageId: MessageId,
): OrchestrationLatestTurn[] {
  return turns.map((entry) =>
    entry.turnId === turnId
      ? { ...entry, assistantMessageId: messageId }
      : entry.assistantMessageId === messageId
        ? { ...entry, assistantMessageId: null }
        : entry,
  );
}

function clearTurnAssistantMessageForTurn(
  turns: ReadonlyArray<OrchestrationLatestTurn>,
  turnId: TurnId,
  messageId: MessageId,
): OrchestrationLatestTurn[] {
  return turns.map((entry) =>
    entry.turnId === turnId && entry.assistantMessageId === messageId
      ? { ...entry, assistantMessageId: null }
      : entry,
  );
}

function turnFromCheckpoint(input: {
  readonly existingTurns: ReadonlyArray<OrchestrationLatestTurn>;
  readonly latestTurn: OrchestrationLatestTurn | null;
  readonly turnId: TurnId;
  readonly state: OrchestrationLatestTurn["state"];
  readonly completedAt: string;
  readonly assistantMessageId: MessageId | null;
}): OrchestrationLatestTurn {
  const existing =
    input.existingTurns.find((turn) => turn.turnId === input.turnId) ??
    (input.latestTurn?.turnId === input.turnId ? input.latestTurn : null);
  return {
    turnId: input.turnId,
    state: input.state,
    requestedAt: existing?.requestedAt ?? input.completedAt,
    startedAt: existing?.startedAt ?? input.completedAt,
    completedAt: input.completedAt,
    assistantMessageId: input.assistantMessageId,
    ...(existing?.sourceProposedPlan !== undefined
      ? { sourceProposedPlan: existing.sourceProposedPlan }
      : {}),
  };
}

function currentAssistantMessageIdForTurnBoundary(
  thread: OrchestrationThread,
  turnId: TurnId,
): MessageId | null {
  const turn = thread.turns.find((entry) => entry.turnId === turnId);
  if (turn !== undefined) {
    return turn.assistantMessageId;
  }
  if (thread.latestTurn?.turnId === turnId) {
    return thread.latestTurn.assistantMessageId;
  }
  return (
    thread.checkpoints.find((checkpoint) => checkpoint.turnId === turnId)?.assistantMessageId ??
    null
  );
}

function checkpointCanBecomeLatestTurn(input: {
  readonly thread: OrchestrationThread;
  readonly turnId: TurnId;
  readonly checkpointTurnCount: number;
}): boolean {
  if (input.thread.latestTurn === null || input.thread.latestTurn.turnId === input.turnId) {
    return true;
  }
  const latestTurnCheckpoint = input.thread.checkpoints.find(
    (checkpoint) => checkpoint.turnId === input.thread.latestTurn?.turnId,
  );
  return (
    latestTurnCheckpoint !== undefined &&
    input.checkpointTurnCount >= latestTurnCheckpoint.checkpointTurnCount
  );
}

function updateExistingMessageForMessageSent(
  entry: OrchestrationMessage,
  message: OrchestrationMessage,
): OrchestrationMessage {
  const sameTurn = entry.turnId === message.turnId;
  const text = sameTurn
    ? message.streaming
      ? `${entry.text}${message.text}`
      : message.text.length > 0
        ? message.text
        : entry.text
    : message.text;

  return {
    id: entry.id,
    role: message.role,
    text,
    ...(message.attachments !== undefined
      ? { attachments: message.attachments }
      : sameTurn && entry.attachments !== undefined
        ? { attachments: entry.attachments }
        : {}),
    turnId: message.turnId,
    streaming: message.streaming,
    createdAt: sameTurn ? entry.createdAt : message.createdAt,
    updatedAt: message.updatedAt,
  };
}

function isSubAgentWakeSystemMessageText(text: string): boolean {
  return text.trimStart().startsWith("[sub-agent ");
}

function isThreadPromptMessage(message: Pick<OrchestrationMessage, "role" | "text">): boolean {
  return (
    message.role === "user" ||
    (message.role === "system" && isSubAgentWakeSystemMessageText(message.text))
  );
}

function retainThreadActivitiesAfterRevert(
  activities: ReadonlyArray<OrchestrationThread["activities"][number]>,
  retainedTurnIds: ReadonlySet<string>,
): ReadonlyArray<OrchestrationThread["activities"][number]> {
  return activities.filter(
    (activity) => activity.turnId === null || retainedTurnIds.has(activity.turnId),
  );
}

function retainThreadProposedPlansAfterRevert(
  proposedPlans: ReadonlyArray<OrchestrationThread["proposedPlans"][number]>,
  retainedTurnIds: ReadonlySet<string>,
): ReadonlyArray<OrchestrationThread["proposedPlans"][number]> {
  return proposedPlans.filter(
    (proposedPlan) => proposedPlan.turnId === null || retainedTurnIds.has(proposedPlan.turnId),
  );
}

function compareThreadActivities(
  left: OrchestrationThread["activities"][number],
  right: OrchestrationThread["activities"][number],
): number {
  if (left.sequence !== undefined && right.sequence !== undefined) {
    if (left.sequence !== right.sequence) {
      return left.sequence - right.sequence;
    }
  } else if (left.sequence !== undefined) {
    return 1;
  } else if (right.sequence !== undefined) {
    return -1;
  }

  return left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id);
}

export function createEmptyReadModel(nowIso: string): OrchestrationReadModel {
  return {
    snapshotSequence: 0,
    projects: [],
    threads: [],
    updatedAt: nowIso,
  };
}

export function projectEvent(
  model: OrchestrationReadModel,
  event: OrchestrationEvent,
): Effect.Effect<OrchestrationReadModel, OrchestrationProjectorDecodeError> {
  const nextBase: OrchestrationReadModel = {
    ...model,
    snapshotSequence: event.sequence,
    updatedAt: event.occurredAt,
  };

  switch (event.type) {
    case "project.created":
      return decodeForEvent(ProjectCreatedPayload, event.payload, event.type, "payload").pipe(
        Effect.map((payload) => {
          const existing = nextBase.projects.find((entry) => entry.id === payload.projectId);
          const nextProject = {
            id: payload.projectId,
            title: payload.title,
            workspaceRoot: payload.workspaceRoot,
            defaultModelSelection: payload.defaultModelSelection,
            scripts: payload.scripts,
            createdAt: payload.createdAt,
            updatedAt: payload.updatedAt,
            deletedAt: null,
          };

          return {
            ...nextBase,
            projects: existing
              ? nextBase.projects.map((entry) =>
                  entry.id === payload.projectId ? nextProject : entry,
                )
              : [...nextBase.projects, nextProject],
          };
        }),
      );

    case "project.meta-updated":
      return decodeForEvent(ProjectMetaUpdatedPayload, event.payload, event.type, "payload").pipe(
        Effect.map((payload) => ({
          ...nextBase,
          projects: nextBase.projects.map((project) =>
            project.id === payload.projectId
              ? {
                  ...project,
                  ...(payload.title !== undefined ? { title: payload.title } : {}),
                  ...(payload.workspaceRoot !== undefined
                    ? { workspaceRoot: payload.workspaceRoot }
                    : {}),
                  ...(payload.defaultModelSelection !== undefined
                    ? { defaultModelSelection: payload.defaultModelSelection }
                    : {}),
                  ...(payload.scripts !== undefined ? { scripts: payload.scripts } : {}),
                  updatedAt: payload.updatedAt,
                }
              : project,
          ),
        })),
      );

    case "project.deleted":
      return decodeForEvent(ProjectDeletedPayload, event.payload, event.type, "payload").pipe(
        Effect.map((payload) => ({
          ...nextBase,
          projects: nextBase.projects.map((project) =>
            project.id === payload.projectId
              ? {
                  ...project,
                  deletedAt: payload.deletedAt,
                  updatedAt: payload.deletedAt,
                }
              : project,
          ),
        })),
      );

    case "thread.created":
      return Effect.gen(function* () {
        const payload = yield* decodeForEvent(
          ThreadCreatedPayload,
          event.payload,
          event.type,
          "payload",
        );
        const thread: OrchestrationThread = yield* decodeForEvent(
          OrchestrationThread,
          {
            id: payload.threadId,
            projectId: payload.projectId,
            title: payload.title,
            modelSelection: payload.modelSelection,
            runtimeMode: payload.runtimeMode,
            interactionMode: payload.interactionMode,
            branch: payload.branch,
            worktreePath: payload.worktreePath,
            ...(payload.worktreeRemovable !== undefined
              ? { worktreeRemovable: payload.worktreeRemovable }
              : {}),
            ...(payload.worktreeRemovalPath !== undefined
              ? { worktreeRemovalPath: payload.worktreeRemovalPath }
              : {}),
            latestTurn: null,
            createdAt: payload.createdAt,
            updatedAt: payload.updatedAt,
            archivedAt: null,
            deletedAt: null,
            messages: [],
            activities: [],
            checkpoints: [],
            session: null,
          },
          event.type,
          "thread",
        );
        const existing = nextBase.threads.find((entry) => entry.id === thread.id);
        return {
          ...nextBase,
          threads: existing
            ? nextBase.threads.map((entry) => (entry.id === thread.id ? thread : entry))
            : [...nextBase.threads, thread],
        };
      });

    case "thread.deleted":
      return decodeForEvent(ThreadDeletedPayload, event.payload, event.type, "payload").pipe(
        Effect.map((payload) => ({
          ...nextBase,
          threads: updateThread(nextBase.threads, payload.threadId, {
            deletedAt: payload.deletedAt,
            updatedAt: payload.deletedAt,
          }),
        })),
      );

    case "thread.archived":
      return decodeForEvent(ThreadArchivedPayload, event.payload, event.type, "payload").pipe(
        Effect.map((payload) => ({
          ...nextBase,
          threads: updateThread(nextBase.threads, payload.threadId, {
            archivedAt: payload.archivedAt,
            updatedAt: payload.updatedAt,
          }),
        })),
      );

    case "thread.unarchived":
      return decodeForEvent(ThreadUnarchivedPayload, event.payload, event.type, "payload").pipe(
        Effect.map((payload) => ({
          ...nextBase,
          threads: updateThread(nextBase.threads, payload.threadId, {
            archivedAt: null,
            updatedAt: payload.updatedAt,
          }),
        })),
      );

    case "thread.parent-set":
      // Keep the command read model's parentThreadId current so decider logic
      // that walks parent links (e.g. archive cascade) sees children linked at
      // runtime, not only those present at the last bootstrap from sqlite.
      return decodeForEvent(ThreadParentSetPayload, event.payload, event.type, "payload").pipe(
        Effect.map((payload) => ({
          ...nextBase,
          threads: updateThread(nextBase.threads, payload.threadId, {
            parentThreadId: payload.parentThreadId,
            parentEnvironmentId: payload.parentEnvironmentId ?? null,
            updatedAt: payload.updatedAt,
          }),
        })),
      );

    case "thread.meta-updated":
      return decodeForEvent(ThreadMetaUpdatedPayload, event.payload, event.type, "payload").pipe(
        Effect.map((payload) => ({
          ...nextBase,
          threads: updateThread(nextBase.threads, payload.threadId, {
            ...(payload.title !== undefined ? { title: payload.title } : {}),
            ...(payload.modelSelection !== undefined
              ? { modelSelection: payload.modelSelection }
              : {}),
            ...(payload.branch !== undefined ? { branch: payload.branch } : {}),
            ...(payload.worktreePath !== undefined ? { worktreePath: payload.worktreePath } : {}),
            ...(payload.worktreeRemovable !== undefined
              ? { worktreeRemovable: payload.worktreeRemovable }
              : {}),
            ...(payload.worktreeRemovalPath !== undefined
              ? { worktreeRemovalPath: payload.worktreeRemovalPath }
              : {}),
            updatedAt: payload.updatedAt,
          }),
        })),
      );

    case "thread.runtime-mode-set":
      return decodeForEvent(ThreadRuntimeModeSetPayload, event.payload, event.type, "payload").pipe(
        Effect.map((payload) => ({
          ...nextBase,
          threads: updateThread(nextBase.threads, payload.threadId, {
            runtimeMode: payload.runtimeMode,
            updatedAt: payload.updatedAt,
          }),
        })),
      );

    case "thread.interaction-mode-set":
      return decodeForEvent(
        ThreadInteractionModeSetPayload,
        event.payload,
        event.type,
        "payload",
      ).pipe(
        Effect.map((payload) => ({
          ...nextBase,
          threads: updateThread(nextBase.threads, payload.threadId, {
            interactionMode: payload.interactionMode,
            updatedAt: payload.updatedAt,
          }),
        })),
      );

    case "thread.message-sent":
      return Effect.gen(function* () {
        const payload = yield* decodeForEvent(
          MessageSentPayloadSchema,
          event.payload,
          event.type,
          "payload",
        );
        const thread = nextBase.threads.find((entry) => entry.id === payload.threadId);
        if (!thread) {
          return nextBase;
        }

        const message: OrchestrationMessage = yield* decodeForEvent(
          OrchestrationMessage,
          {
            id: payload.messageId,
            role: payload.role,
            text: payload.text,
            ...(payload.attachments !== undefined ? { attachments: payload.attachments } : {}),
            turnId: payload.turnId,
            streaming: payload.streaming,
            createdAt: payload.createdAt,
            updatedAt: payload.updatedAt,
          },
          event.type,
          "message",
        );

        const existingMessage = thread.messages.find((entry) => entry.id === message.id);
        const messages = existingMessage
          ? thread.messages.map((entry) =>
              entry.id === message.id ? updateExistingMessageForMessageSent(entry, message) : entry,
            )
          : [...thread.messages, message];
        const cappedMessages = messages.slice(-MAX_THREAD_MESSAGES);
        const checkpoints =
          message.role === "assistant" && message.turnId !== null
            ? rebindCheckpointAssistantMessage(thread.checkpoints, message.turnId, message.id)
            : thread.checkpoints;
        const turns =
          message.role === "assistant" && message.turnId !== null
            ? rebindTurnAssistantMessage(thread.turns, message.turnId, message.id)
            : thread.turns;
        const latestTurn =
          message.role === "assistant" && message.turnId !== null
            ? rebindLatestTurnAssistantMessage(thread.latestTurn, message.turnId, message.id)
            : thread.latestTurn;

        return {
          ...nextBase,
          threads: updateThread(nextBase.threads, payload.threadId, {
            messages: cappedMessages,
            checkpoints,
            turns,
            latestTurn,
            updatedAt: event.occurredAt,
          }),
        };
      });

    case "thread.session-set":
      return Effect.gen(function* () {
        const payload = yield* decodeForEvent(
          ThreadSessionSetPayload,
          event.payload,
          event.type,
          "payload",
        );
        const thread = nextBase.threads.find((entry) => entry.id === payload.threadId);
        if (!thread) {
          return nextBase;
        }

        const session: OrchestrationSession = yield* decodeForEvent(
          OrchestrationSession,
          payload.session,
          event.type,
          "session",
        );
        const activeTurnId =
          (session.status === "running" || session.status === "waiting") &&
          session.activeTurnId !== null
            ? session.activeTurnId
            : null;
        const previousActiveTurnId = thread.session?.activeTurnId ?? null;
        const failedPromptMessageIds = failedTurnStartPromptMessageIds(
          thread.messages,
          thread.activities,
        );
        const messages =
          activeTurnId === null || activeTurnId === previousActiveTurnId
            ? thread.messages
            : bindNextPendingPromptMessageToTurn(
                thread.messages,
                activeTurnId,
                failedPromptMessageIds,
              );
        const activeTurnAssistantMessageId =
          activeTurnId === null ? null : latestAssistantMessageIdForTurn(messages, activeTurnId);

        // Leaving the "running" session status is the turn-end signal: settle
        // a still-running latest turn so its duration reflects the whole turn.
        const settledTurnState = settledTurnStateForSessionStatus(session.status);
        const latestTurn =
          activeTurnId !== null
            ? {
                turnId: activeTurnId,
                state: "running" as const,
                requestedAt:
                  thread.latestTurn?.turnId === activeTurnId
                    ? thread.latestTurn.requestedAt
                    : session.updatedAt,
                startedAt:
                  thread.latestTurn?.turnId === activeTurnId
                    ? (thread.latestTurn.startedAt ?? session.updatedAt)
                    : session.updatedAt,
                completedAt: null,
                assistantMessageId:
                  thread.latestTurn?.turnId === activeTurnId
                    ? (thread.latestTurn.assistantMessageId ?? activeTurnAssistantMessageId)
                    : activeTurnAssistantMessageId,
              }
            : thread.latestTurn !== null &&
                thread.latestTurn.state === "running" &&
                settledTurnState !== null
              ? {
                  ...thread.latestTurn,
                  state: settledTurnState,
                  // A running turn's completedAt can only hold a mid-turn
                  // placeholder checkpoint timestamp — the session leaving
                  // "running" is the authoritative turn end.
                  completedAt: session.updatedAt,
                }
              : thread.latestTurn;
        const turns = upsertTurn(
          settleRunningTurnsForSession({
            turns: thread.turns,
            activeTurnId,
            settledTurnState,
            settledAt: session.updatedAt,
          }),
          latestTurn,
        );
        return {
          ...nextBase,
          threads: updateThread(nextBase.threads, payload.threadId, {
            session,
            messages,
            turns,
            latestTurn,
            updatedAt: event.occurredAt,
          }),
        };
      });

    case "thread.proposed-plan-upserted":
      return Effect.gen(function* () {
        const payload = yield* decodeForEvent(
          ThreadProposedPlanUpsertedPayload,
          event.payload,
          event.type,
          "payload",
        );
        const thread = nextBase.threads.find((entry) => entry.id === payload.threadId);
        if (!thread) {
          return nextBase;
        }

        const proposedPlans = [
          ...thread.proposedPlans.filter((entry) => entry.id !== payload.proposedPlan.id),
          payload.proposedPlan,
        ]
          .toSorted(
            (left, right) =>
              left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
          )
          .slice(-200);

        return {
          ...nextBase,
          threads: updateThread(nextBase.threads, payload.threadId, {
            proposedPlans,
            updatedAt: event.occurredAt,
          }),
        };
      });

    case "thread.turn-diff-completed":
      return Effect.gen(function* () {
        const payload = yield* decodeForEvent(
          ThreadTurnDiffCompletedPayload,
          event.payload,
          event.type,
          "payload",
        );
        const thread = nextBase.threads.find((entry) => entry.id === payload.threadId);
        if (!thread) {
          return nextBase;
        }

        const checkpointAssistantMessage = payload.assistantMessageId
          ? thread.messages.find((message) => message.id === payload.assistantMessageId)
          : undefined;
        const checkpointAssistantMessageId =
          payload.assistantMessageId !== null &&
          (checkpointAssistantMessage === undefined ||
            checkpointAssistantMessage.turnId === payload.turnId)
            ? payload.assistantMessageId
            : null;
        const currentTurnBoundaryAssistantMessageId = currentAssistantMessageIdForTurnBoundary(
          thread,
          payload.turnId,
        );
        const turnBoundaryAssistantMessageId =
          checkpointAssistantMessageId !== null &&
          assistantMessageCanAdvanceTurnBoundary({
            messages: thread.messages,
            turnId: payload.turnId,
            candidateMessageId: checkpointAssistantMessageId,
            currentMessageId: currentTurnBoundaryAssistantMessageId,
          })
            ? checkpointAssistantMessageId
            : null;
        const settledAssistantMessageId =
          payload.assistantMessageId === null
            ? null
            : (turnBoundaryAssistantMessageId ??
              (checkpointAssistantMessageId !== null
                ? currentTurnBoundaryAssistantMessageId
                : null));

        const checkpoint = yield* decodeForEvent(
          OrchestrationCheckpointSummary,
          {
            turnId: payload.turnId,
            checkpointTurnCount: payload.checkpointTurnCount,
            checkpointRef: payload.checkpointRef,
            status: payload.status,
            files: payload.files,
            assistantMessageId: checkpointAssistantMessageId,
            completedAt: payload.completedAt,
          },
          event.type,
          "checkpoint",
        );

        // Do not let a placeholder (status "missing") overwrite a checkpoint
        // that has already been captured with a real git ref (status "ready").
        // ProviderRuntimeIngestion may fire multiple turn.diff.updated events
        // per turn; without this guard later placeholders would clobber the
        // real capture dispatched by CheckpointReactor.
        const existing = thread.checkpoints.find((entry) => entry.turnId === checkpoint.turnId);
        if (existing && existing.status !== "missing" && checkpoint.status === "missing") {
          return nextBase;
        }

        const checkpoints = upsertCheckpointRebindingAssistantMessage(
          thread.checkpoints,
          checkpoint,
        );
        const shouldApplyCheckpointTurn =
          payload.assistantMessageId === null || checkpointAssistantMessageId !== null;
        const shouldUpdateLatestTurn = checkpointCanBecomeLatestTurn({
          thread,
          turnId: payload.turnId,
          checkpointTurnCount: payload.checkpointTurnCount,
        });

        // Mid-turn diff updates produce placeholder checkpoints; record the
        // checkpoint, but don't settle a turn its session is still running.
        const turnStillRunning =
          (thread.session?.status === "running" || thread.session?.status === "waiting") &&
          thread.session.activeTurnId === payload.turnId;
        const turnsAfterAssistantRebind =
          payload.assistantMessageId !== null && turnBoundaryAssistantMessageId !== null
            ? rebindTurnAssistantMessage(
                thread.turns,
                payload.turnId,
                turnBoundaryAssistantMessageId,
              )
            : payload.assistantMessageId !== null && turnBoundaryAssistantMessageId === null
              ? clearTurnAssistantMessageForTurn(
                  thread.turns,
                  payload.turnId,
                  payload.assistantMessageId,
                )
              : thread.turns;
        const checkpointTurn =
          !turnStillRunning && shouldApplyCheckpointTurn
            ? turnFromCheckpoint({
                existingTurns: turnsAfterAssistantRebind,
                latestTurn: thread.latestTurn,
                turnId: payload.turnId,
                state: checkpointStatusToLatestTurnState(payload.status),
                completedAt: payload.completedAt,
                assistantMessageId: settledAssistantMessageId,
              })
            : null;

        const latestTurn = !shouldApplyCheckpointTurn
          ? payload.assistantMessageId === null
            ? thread.latestTurn
            : clearLatestTurnAssistantMessageForTurn(
                thread.latestTurn,
                payload.turnId,
                payload.assistantMessageId,
              )
          : turnStillRunning
            ? turnBoundaryAssistantMessageId !== null
              ? rebindLatestTurnAssistantMessage(
                  thread.latestTurn,
                  payload.turnId,
                  turnBoundaryAssistantMessageId,
                )
              : thread.latestTurn
            : shouldUpdateLatestTurn
              ? {
                  turnId: payload.turnId,
                  state: checkpointStatusToLatestTurnState(payload.status),
                  requestedAt:
                    thread.latestTurn?.turnId === payload.turnId
                      ? thread.latestTurn.requestedAt
                      : payload.completedAt,
                  startedAt:
                    thread.latestTurn?.turnId === payload.turnId
                      ? (thread.latestTurn.startedAt ?? payload.completedAt)
                      : payload.completedAt,
                  completedAt: payload.completedAt,
                  assistantMessageId: settledAssistantMessageId,
                }
              : thread.latestTurn;
        const turns = upsertTurn(
          checkpointTurn === null
            ? turnsAfterAssistantRebind
            : upsertTurn(turnsAfterAssistantRebind, checkpointTurn),
          latestTurn,
        );

        return {
          ...nextBase,
          threads: updateThread(nextBase.threads, payload.threadId, {
            checkpoints,
            turns,
            latestTurn,
            updatedAt: event.occurredAt,
          }),
        };
      });

    case "thread.reverted":
      return decodeForEvent(ThreadRevertedPayload, event.payload, event.type, "payload").pipe(
        Effect.map((payload) => {
          const thread = nextBase.threads.find((entry) => entry.id === payload.threadId);
          if (!thread) {
            return nextBase;
          }

          const checkpoints = thread.checkpoints
            .filter((entry) => entry.checkpointTurnCount <= payload.turnCount)
            .toSorted((left, right) => left.checkpointTurnCount - right.checkpointTurnCount)
            .slice(-MAX_THREAD_CHECKPOINTS);
          const retainedTurnIds = new Set(checkpoints.map((checkpoint) => checkpoint.turnId));
          const messages = retainThreadMessagesAfterRevert(
            thread.messages,
            retainedTurnIds,
            payload.turnCount,
          ).slice(-MAX_THREAD_MESSAGES);
          const proposedPlans = retainThreadProposedPlansAfterRevert(
            thread.proposedPlans,
            retainedTurnIds,
          ).slice(-200);
          const activities = retainThreadActivitiesAfterRevert(thread.activities, retainedTurnIds);

          const latestCheckpoint = checkpoints.at(-1) ?? null;
          const latestTurn =
            latestCheckpoint === null
              ? null
              : {
                  turnId: latestCheckpoint.turnId,
                  state: checkpointStatusToLatestTurnState(latestCheckpoint.status),
                  requestedAt: latestCheckpoint.completedAt,
                  startedAt: latestCheckpoint.completedAt,
                  completedAt: latestCheckpoint.completedAt,
                  assistantMessageId: latestCheckpoint.assistantMessageId,
                };
          const turns = upsertTurn(
            thread.turns.filter((turn) => retainedTurnIds.has(turn.turnId)),
            latestTurn,
          );

          return {
            ...nextBase,
            threads: updateThread(nextBase.threads, payload.threadId, {
              checkpoints,
              turns,
              messages,
              proposedPlans,
              activities,
              latestTurn,
              updatedAt: event.occurredAt,
            }),
          };
        }),
      );

    case "thread.activity-appended":
      return decodeForEvent(
        ThreadActivityAppendedPayload,
        event.payload,
        event.type,
        "payload",
      ).pipe(
        Effect.map((payload) => {
          const thread = nextBase.threads.find((entry) => entry.id === payload.threadId);
          if (!thread) {
            return nextBase;
          }

          const activities = [
            ...thread.activities.filter((entry) => entry.id !== payload.activity.id),
            payload.activity,
          ]
            .toSorted(compareThreadActivities)
            .slice(-500);

          return {
            ...nextBase,
            threads: updateThread(nextBase.threads, payload.threadId, {
              activities,
              updatedAt: event.occurredAt,
            }),
          };
        }),
      );

    default:
      return Effect.succeed(nextBase);
  }
}
