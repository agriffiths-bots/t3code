import { pipe } from "effect/Function";
import * as Arr from "effect/Array";
import * as O from "effect/Order";
import type {
  MessageId,
  OrchestrationCheckpointSummary,
  OrchestrationEvent,
  OrchestrationLatestTurn,
  OrchestrationMessage,
  OrchestrationSession,
  OrchestrationThread,
  OrchestrationThreadActivity,
  TurnId,
} from "@t3tools/contracts";

export type ThreadDetailReducerResult =
  | { readonly kind: "updated"; readonly thread: OrchestrationThread }
  | { readonly kind: "deleted" }
  | { readonly kind: "unchanged" };

const proposedPlanOrder = O.combine<OrchestrationThread["proposedPlans"][number]>(
  O.mapInput(O.String, (p) => p.createdAt),
  O.mapInput(O.String, (p) => p.id),
);

const checkpointOrder = O.mapInput(
  O.Number,
  (cp: OrchestrationThread["checkpoints"][number]) =>
    cp.checkpointTurnCount ?? Number.MAX_SAFE_INTEGER,
);

const turnOrder = O.combine<OrchestrationLatestTurn>(
  O.mapInput(O.String, (turn) => turn.requestedAt),
  O.mapInput(O.String, (turn) => turn.turnId),
);

const activityOrder = O.combineAll<OrchestrationThreadActivity>([
  O.mapInput(O.Number, (a) => a.sequence ?? Number.MAX_SAFE_INTEGER),
  O.mapInput(O.String, (a) => a.createdAt),
  O.mapInput(O.String, (a) => a.id),
]);

function updateExistingMessageForMessageSent(
  entry: OrchestrationMessage,
  message: OrchestrationMessage,
): OrchestrationMessage {
  const sameTurn = entry.turnId === message.turnId;
  // Delivery healing can replay events out of order: an older streaming chunk
  // for the same turn must not clobber newer text. Accepted chunks advance
  // `updatedAt` below so the watermark tracks the newest applied chunk.
  if (message.streaming && sameTurn && entry.updatedAt > message.updatedAt) {
    return entry;
  }
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

function upsertCheckpointRebindingAssistantMessage(
  checkpoints: ReadonlyArray<OrchestrationCheckpointSummary>,
  checkpoint: OrchestrationCheckpointSummary,
): OrchestrationCheckpointSummary[] {
  const assistantMessageId = checkpoint.assistantMessageId;
  return pipe(
    checkpoints,
    Arr.filter((entry) => entry.turnId !== checkpoint.turnId),
    Arr.map((entry) =>
      assistantMessageId !== null && entry.assistantMessageId === assistantMessageId
        ? { ...entry, assistantMessageId: null }
        : entry,
    ),
    Arr.append(checkpoint),
    Arr.sort(checkpointOrder),
  );
}

/**
 * Apply a single orchestration event to an `OrchestrationThread`, returning
 * the updated thread, a deletion signal, or an "unchanged" marker when the
 * event doesn't affect this thread.
 *
 * This is a pure reducer operating on contract types. UI-specific mapping
 * (e.g. resolving attachment preview URLs, normalising model slugs, adding
 * scoped fields like `environmentId`) is the caller's responsibility.
 */
export function applyThreadDetailEvent(
  thread: OrchestrationThread,
  event: OrchestrationEvent,
): ThreadDetailReducerResult {
  switch (event.type) {
    // ── Project events (irrelevant to thread detail) ────────────────
    case "project.created":
    case "project.meta-updated":
    case "project.deleted":
      return { kind: "unchanged" };

    // ── Thread lifecycle ────────────────────────────────────────────
    case "thread.created":
      return {
        kind: "updated",
        thread: {
          id: event.payload.threadId,
          projectId: event.payload.projectId,
          title: event.payload.title,
          modelSelection: event.payload.modelSelection,
          runtimeMode: event.payload.runtimeMode,
          interactionMode: event.payload.interactionMode,
          branch: event.payload.branch,
          worktreePath: event.payload.worktreePath,
          latestTurn: null,
          turns: [],
          createdAt: event.payload.createdAt,
          updatedAt: event.payload.updatedAt,
          archivedAt: null,
          deletedAt: null,
          messages: [],
          proposedPlans: [],
          activities: [],
          checkpoints: [],
          session: null,
        },
      };

    case "thread.deleted":
      return { kind: "deleted" };

    case "thread.archived":
      return {
        kind: "updated",
        thread: {
          ...thread,
          archivedAt: event.payload.archivedAt,
          updatedAt: event.payload.updatedAt,
        },
      };

    case "thread.unarchived":
      return {
        kind: "updated",
        thread: { ...thread, archivedAt: null, updatedAt: event.payload.updatedAt },
      };

    // ── Thread metadata ─────────────────────────────────────────────
    case "thread.meta-updated":
      return {
        kind: "updated",
        thread: {
          ...thread,
          ...(event.payload.title !== undefined ? { title: event.payload.title } : {}),
          ...(event.payload.modelSelection !== undefined
            ? { modelSelection: event.payload.modelSelection }
            : {}),
          ...(event.payload.branch !== undefined ? { branch: event.payload.branch } : {}),
          ...(event.payload.worktreePath !== undefined
            ? { worktreePath: event.payload.worktreePath }
            : {}),
          updatedAt: event.payload.updatedAt,
        },
      };

    case "thread.runtime-mode-set":
      return {
        kind: "updated",
        thread: {
          ...thread,
          runtimeMode: event.payload.runtimeMode,
          updatedAt: event.payload.updatedAt,
        },
      };

    case "thread.interaction-mode-set":
      return {
        kind: "updated",
        thread: {
          ...thread,
          interactionMode: event.payload.interactionMode,
          updatedAt: event.payload.updatedAt,
        },
      };

    // ── Turn lifecycle ──────────────────────────────────────────────
    case "thread.turn-start-requested":
      return {
        kind: "updated",
        thread: {
          ...thread,
          ...(event.payload.modelSelection !== undefined
            ? { modelSelection: event.payload.modelSelection }
            : {}),
          runtimeMode: event.payload.runtimeMode,
          interactionMode: event.payload.interactionMode,
          updatedAt: event.occurredAt,
        },
      };

    case "thread.turn-interrupt-requested": {
      if (event.payload.turnId === undefined) {
        return { kind: "unchanged" };
      }
      const latestTurn = thread.latestTurn;
      if (latestTurn === null || latestTurn.turnId !== event.payload.turnId) {
        return { kind: "unchanged" };
      }
      const nextLatestTurn: OrchestrationLatestTurn = {
        ...latestTurn,
        state: "interrupted",
        startedAt: latestTurn.startedAt ?? event.payload.createdAt,
        completedAt: latestTurn.completedAt ?? event.payload.createdAt,
      };
      return {
        kind: "updated",
        thread: {
          ...thread,
          latestTurn: nextLatestTurn,
          turns: upsertTurn(thread.turns, nextLatestTurn),
          updatedAt: event.occurredAt,
        },
      };
    }

    // ── Messages ────────────────────────────────────────────────────
    case "thread.message-sent": {
      const message: OrchestrationMessage = {
        id: event.payload.messageId,
        role: event.payload.role,
        text: event.payload.text,
        ...(event.payload.attachments !== undefined
          ? { attachments: event.payload.attachments }
          : {}),
        turnId: event.payload.turnId,
        streaming: event.payload.streaming,
        createdAt: event.payload.createdAt,
        updatedAt: event.payload.updatedAt,
      };

      const existingMessage = thread.messages.find((entry) => entry.id === message.id);
      // A replayed chunk rejected by the watermark guard must leave the thread
      // fully untouched — advancing thread.updatedAt (or rolling it back to the
      // replay's timestamp) would distort the snapshot freshness comparison.
      if (
        existingMessage !== undefined &&
        updateExistingMessageForMessageSent(existingMessage, message) === existingMessage
      ) {
        return { kind: "unchanged" };
      }
      const messages = existingMessage
        ? Arr.map(thread.messages, (entry) =>
            entry.id !== message.id ? entry : updateExistingMessageForMessageSent(entry, message),
          )
        : Arr.append(thread.messages, message);
      // Update latestTurn for assistant messages bound to a turn. A completed
      // assistant message only settles the turn once the session is no longer
      // running it — providers may emit several assistant messages per turn
      // (commentary between tool calls), and the turn must stay unsettled
      // until the provider reports turn end.
      const turnStillRunning =
        event.payload.turnId !== null &&
        (thread.session?.status === "running" || thread.session?.status === "waiting") &&
        thread.session.activeTurnId === event.payload.turnId;
      const settlesTurn = !event.payload.streaming && !turnStillRunning;
      const latestTurn: OrchestrationThread["latestTurn"] =
        event.payload.role === "assistant" && event.payload.turnId !== null
          ? thread.latestTurn === null || thread.latestTurn.turnId === event.payload.turnId
            ? {
                turnId: event.payload.turnId,
                state: settlesTurn
                  ? thread.latestTurn?.state === "interrupted"
                    ? "interrupted"
                    : thread.latestTurn?.state === "error"
                      ? "error"
                      : "completed"
                  : "running",
                requestedAt:
                  thread.latestTurn?.turnId === event.payload.turnId
                    ? thread.latestTurn.requestedAt
                    : event.payload.createdAt,
                startedAt:
                  thread.latestTurn?.turnId === event.payload.turnId
                    ? (thread.latestTurn.startedAt ?? event.payload.createdAt)
                    : event.payload.createdAt,
                completedAt: settlesTurn
                  ? event.payload.updatedAt
                  : thread.latestTurn?.turnId === event.payload.turnId
                    ? (thread.latestTurn.completedAt ?? null)
                    : null,
                assistantMessageId: event.payload.messageId,
              }
            : rebindLatestTurnAssistantMessage(
                thread.latestTurn,
                event.payload.turnId,
                event.payload.messageId,
              )
          : thread.latestTurn;

      // Rebind checkpoint assistant message IDs for assistant messages.
      const checkpoints =
        event.payload.role === "assistant" && event.payload.turnId !== null
          ? rebindCheckpointAssistantMessage(
              thread.checkpoints,
              event.payload.turnId,
              event.payload.messageId,
            )
          : thread.checkpoints;
      const turns =
        event.payload.role === "assistant" && event.payload.turnId !== null
          ? upsertTurn(
              rebindTurnAssistantMessage(
                thread.turns,
                event.payload.turnId,
                event.payload.messageId,
              ),
              latestTurn,
            )
          : upsertTurn(thread.turns, latestTurn);

      return {
        kind: "updated",
        thread: {
          ...thread,
          messages,
          checkpoints,
          latestTurn,
          turns,
          updatedAt: event.occurredAt,
        },
      };
    }

    // ── Session ─────────────────────────────────────────────────────
    case "thread.session-set": {
      // Leaving the "running" session status is the turn-end signal: settle a
      // still-running latest turn so its duration reflects the whole turn.
      const settledTurnState = settledTurnStateForSessionStatus(event.payload.session.status);
      const activeTurnId =
        (event.payload.session.status === "running" ||
          event.payload.session.status === "waiting") &&
        event.payload.session.activeTurnId !== null
          ? event.payload.session.activeTurnId
          : null;
      const previousActiveTurnId = thread.session?.activeTurnId ?? null;
      const messages =
        activeTurnId === null || activeTurnId === previousActiveTurnId
          ? thread.messages
          : bindLatestPendingPromptMessageToTurn(thread.messages, activeTurnId);
      const activeTurnAssistantMessageId =
        activeTurnId === null ? null : latestAssistantMessageIdForTurn(messages, activeTurnId);
      const latestTurn: OrchestrationLatestTurn | null =
        activeTurnId !== null
          ? {
              turnId: activeTurnId,
              state: "running",
              requestedAt:
                thread.latestTurn?.turnId === activeTurnId
                  ? thread.latestTurn.requestedAt
                  : event.payload.session.updatedAt,
              startedAt:
                thread.latestTurn?.turnId === activeTurnId
                  ? (thread.latestTurn.startedAt ?? event.payload.session.updatedAt)
                  : event.payload.session.updatedAt,
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
                completedAt: event.payload.session.updatedAt,
              }
            : thread.latestTurn;

      return {
        kind: "updated",
        thread: {
          ...thread,
          session: event.payload.session,
          messages,
          latestTurn,
          turns: upsertTurn(
            settleRunningTurnsForSession({
              turns: thread.turns,
              activeTurnId,
              settledTurnState,
              updatedAt: event.payload.session.updatedAt,
            }),
            latestTurn,
          ),
          updatedAt: event.occurredAt,
        },
      };
    }

    case "thread.session-stop-requested":
      return thread.session === null
        ? { kind: "unchanged" }
        : {
            kind: "updated",
            thread: {
              ...thread,
              session: {
                ...thread.session,
                status: "stopped",
                activeTurnId: null,
                updatedAt: event.payload.createdAt,
              },
              updatedAt: event.occurredAt,
            },
          };

    // ── Proposed plans ──────────────────────────────────────────────
    case "thread.proposed-plan-upserted": {
      const proposedPlan = event.payload.proposedPlan;

      const proposedPlans = pipe(
        thread.proposedPlans,
        Arr.filter((entry) => entry.id !== proposedPlan.id),
        Arr.append(proposedPlan),
        Arr.sort(proposedPlanOrder),
      );

      return {
        kind: "updated",
        thread: { ...thread, proposedPlans, updatedAt: event.occurredAt },
      };
    }

    // ── Checkpoints / turn diffs ────────────────────────────────────
    case "thread.turn-diff-completed": {
      const checkpointAssistantMessage = event.payload.assistantMessageId
        ? thread.messages.find((message) => message.id === event.payload.assistantMessageId)
        : undefined;
      const checkpointAssistantMessageId =
        event.payload.assistantMessageId !== null &&
        (checkpointAssistantMessage === undefined ||
          checkpointAssistantMessage.turnId === event.payload.turnId)
          ? event.payload.assistantMessageId
          : null;
      const currentTurnBoundaryAssistantMessageId = currentAssistantMessageIdForTurnBoundary(
        thread,
        event.payload.turnId,
      );
      const turnBoundaryAssistantMessageId =
        checkpointAssistantMessageId !== null &&
        assistantMessageCanAdvanceTurnBoundary({
          messages: thread.messages,
          turnId: event.payload.turnId,
          candidateMessageId: checkpointAssistantMessageId,
          currentMessageId: currentTurnBoundaryAssistantMessageId,
        })
          ? checkpointAssistantMessageId
          : null;
      const settledAssistantMessageId =
        event.payload.assistantMessageId === null
          ? null
          : (turnBoundaryAssistantMessageId ??
            (checkpointAssistantMessageId !== null ? currentTurnBoundaryAssistantMessageId : null));
      const checkpoint: OrchestrationCheckpointSummary = {
        turnId: event.payload.turnId,
        checkpointTurnCount: event.payload.checkpointTurnCount,
        checkpointRef: event.payload.checkpointRef,
        status: event.payload.status,
        files: event.payload.files,
        assistantMessageId: checkpointAssistantMessageId,
        completedAt: event.payload.completedAt,
      };

      const existing = thread.checkpoints.find((entry) => entry.turnId === checkpoint.turnId);
      // Don't overwrite a non-missing checkpoint with a missing one.
      if (existing && existing.status !== "missing" && checkpoint.status === "missing") {
        return { kind: "unchanged" };
      }

      const checkpoints = upsertCheckpointRebindingAssistantMessage(thread.checkpoints, checkpoint);

      // Mid-turn diff updates produce placeholder checkpoints; record the
      // checkpoint, but don't settle a turn its session is still running.
      const diffTurnStillRunning =
        (thread.session?.status === "running" || thread.session?.status === "waiting") &&
        thread.session.activeTurnId === event.payload.turnId;
      const shouldApplyCheckpointTurn =
        event.payload.assistantMessageId === null || checkpointAssistantMessageId !== null;
      const latestTurn = !shouldApplyCheckpointTurn
        ? event.payload.assistantMessageId === null
          ? thread.latestTurn
          : clearLatestTurnAssistantMessageForTurn(
              thread.latestTurn,
              event.payload.turnId,
              event.payload.assistantMessageId,
            )
        : !diffTurnStillRunning &&
            (thread.latestTurn === null || thread.latestTurn.turnId === event.payload.turnId)
          ? {
              turnId: event.payload.turnId,
              state: checkpointStatusToTurnState(event.payload.status),
              requestedAt: thread.latestTurn?.requestedAt ?? event.payload.completedAt,
              startedAt: thread.latestTurn?.startedAt ?? event.payload.completedAt,
              completedAt: event.payload.completedAt,
              assistantMessageId: settledAssistantMessageId,
            }
          : diffTurnStillRunning && turnBoundaryAssistantMessageId !== null
            ? rebindLatestTurnAssistantMessage(
                thread.latestTurn,
                event.payload.turnId,
                turnBoundaryAssistantMessageId,
              )
            : thread.latestTurn;
      const turnsAfterAssistantRebind =
        event.payload.assistantMessageId !== null && turnBoundaryAssistantMessageId !== null
          ? rebindTurnAssistantMessage(
              thread.turns,
              event.payload.turnId,
              turnBoundaryAssistantMessageId,
            )
          : event.payload.assistantMessageId !== null && turnBoundaryAssistantMessageId === null
            ? clearTurnAssistantMessageForTurn(
                thread.turns,
                event.payload.turnId,
                event.payload.assistantMessageId,
              )
            : thread.turns;
      const checkpointTurn =
        !diffTurnStillRunning && shouldApplyCheckpointTurn
          ? turnFromCheckpoint({
              existingTurns: turnsAfterAssistantRebind,
              latestTurn,
              turnId: event.payload.turnId,
              state: checkpointStatusToTurnState(event.payload.status),
              completedAt: event.payload.completedAt,
              assistantMessageId: settledAssistantMessageId,
            })
          : null;
      const turns = upsertTurn(
        checkpointTurn === null
          ? turnsAfterAssistantRebind
          : upsertTurn(turnsAfterAssistantRebind, checkpointTurn),
        latestTurn,
      );

      return {
        kind: "updated",
        thread: { ...thread, checkpoints, latestTurn, turns, updatedAt: event.occurredAt },
      };
    }

    // ── Revert ──────────────────────────────────────────────────────
    case "thread.reverted": {
      const checkpoints = pipe(
        thread.checkpoints,
        Arr.filter(
          (entry) =>
            entry.checkpointTurnCount !== undefined &&
            entry.checkpointTurnCount <= event.payload.turnCount,
        ),
        Arr.sort(checkpointOrder),
      );

      const retainedTurnIds = new Set(Arr.map(checkpoints, (entry) => entry.turnId));
      const messages = retainMessagesAfterRevert(thread.messages, retainedTurnIds);
      const proposedPlans = pipe(
        thread.proposedPlans,
        Arr.filter((plan) => plan.turnId === null || retainedTurnIds.has(plan.turnId)),
      );
      const activities = pipe(
        thread.activities,
        Arr.filter((activity) => activity.turnId === null || retainedTurnIds.has(activity.turnId)),
      );
      const latestCheckpoint = checkpoints.at(-1) ?? null;
      const latestTurn =
        latestCheckpoint === null
          ? null
          : {
              turnId: latestCheckpoint.turnId,
              state: checkpointStatusToTurnState(
                latestCheckpoint.status as "ready" | "missing" | "error",
              ),
              requestedAt: latestCheckpoint.completedAt,
              startedAt: latestCheckpoint.completedAt,
              completedAt: latestCheckpoint.completedAt,
              assistantMessageId: latestCheckpoint.assistantMessageId ?? null,
            };
      const turns = upsertTurn(
        Arr.filter(thread.turns, (turn) => retainedTurnIds.has(turn.turnId)),
        latestTurn,
      );

      return {
        kind: "updated",
        thread: {
          ...thread,
          checkpoints,
          messages,
          proposedPlans,
          activities,
          latestTurn,
          turns,
          updatedAt: event.occurredAt,
        },
      };
    }

    // ── Activities ──────────────────────────────────────────────────
    case "thread.activity-appended": {
      const activities = pipe(
        thread.activities,
        Arr.filter((activity) => activity.id !== event.payload.activity.id),
        Arr.append(event.payload.activity),
        Arr.sort(activityOrder),
      );

      return {
        kind: "updated",
        thread: { ...thread, activities, updatedAt: event.occurredAt },
      };
    }

    // ── Events that don't mutate thread state directly ──────────────
    case "thread.approval-response-requested":
    case "thread.user-input-response-requested":
    case "thread.checkpoint-revert-requested":
      return { kind: "unchanged" };
  }

  // Forward-compatible: ignore unrecognized event types.
  return { kind: "unchanged" };
}

// ── Helpers ──────────────────────────────────────────────────────────

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

function checkpointStatusToTurnState(
  status: "ready" | "missing" | "error",
): OrchestrationLatestTurn["state"] {
  switch (status) {
    case "ready":
      return "completed";
    case "error":
      return "error";
    case "missing":
      return "completed";
  }
}

function latestAssistantMessageIdForTurn(
  messages: ReadonlyArray<OrchestrationMessage>,
  turnId: TurnId,
): MessageId | null {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message?.role === "assistant" && message.turnId === turnId) {
      return message.id;
    }
  }
  return null;
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

function upsertTurn(
  turns: ReadonlyArray<OrchestrationLatestTurn>,
  turn: OrchestrationLatestTurn | null,
): OrchestrationLatestTurn[] {
  if (turn === null) {
    return Array.from(turns);
  }
  return pipe(
    turns,
    Arr.filter((entry) => entry.turnId !== turn.turnId),
    Arr.append(turn),
    Arr.sort(turnOrder),
  );
}

function settleRunningTurnsForSession(input: {
  readonly turns: ReadonlyArray<OrchestrationLatestTurn>;
  readonly activeTurnId: TurnId | null;
  readonly settledTurnState: "completed" | "interrupted" | "error" | null;
  readonly updatedAt: string;
}): OrchestrationLatestTurn[] {
  return Arr.map(input.turns, (turn) => {
    if (turn.state !== "running") {
      return turn;
    }
    if (input.activeTurnId !== null) {
      return turn.turnId === input.activeTurnId
        ? turn
        : {
            ...turn,
            state: "completed",
            startedAt: turn.startedAt ?? input.updatedAt,
            completedAt: input.updatedAt,
          };
    }
    if (input.settledTurnState === null) {
      return turn;
    }
    return {
      ...turn,
      state: input.settledTurnState,
      startedAt: turn.startedAt ?? input.updatedAt,
      completedAt: input.updatedAt,
    };
  });
}

function rebindTurnAssistantMessage(
  turns: ReadonlyArray<OrchestrationLatestTurn>,
  turnId: TurnId,
  messageId: MessageId,
): OrchestrationLatestTurn[] {
  return Arr.map(turns, (entry) =>
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
  return Arr.map(turns, (entry) =>
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

function rebindCheckpointAssistantMessage(
  checkpoints: ReadonlyArray<OrchestrationCheckpointSummary>,
  turnId: TurnId,
  messageId: MessageId,
): OrchestrationCheckpointSummary[] {
  return Arr.map(checkpoints, (entry) =>
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

function retainMessagesAfterRevert(
  messages: ReadonlyArray<OrchestrationMessage>,
  retainedTurnIds: ReadonlySet<string>,
): OrchestrationMessage[] {
  // Keep messages that belong to a retained turn, plus messages without a
  // turn binding (pre-turn-0 user messages and permanent system notices).
  return Arr.filter(messages, (message) => {
    if (message.turnId === null) {
      return message.role !== "system" || !isSubAgentWakeSystemMessageText(message.text);
    }
    return retainedTurnIds.has(message.turnId);
  });
}

function bindLatestPendingPromptMessageToTurn(
  messages: ReadonlyArray<OrchestrationMessage>,
  turnId: TurnId,
): OrchestrationMessage[] {
  let pendingIndex = -1;
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (
      message &&
      message.turnId === null &&
      (message.role === "user" ||
        (message.role === "system" && isSubAgentWakeSystemMessageText(message.text)))
    ) {
      pendingIndex = index;
      break;
    }
  }
  if (pendingIndex === -1) return [...messages];
  return Arr.map(messages, (message, index) =>
    index === pendingIndex ? { ...message, turnId } : message,
  );
}

function isSubAgentWakeSystemMessageText(text: string): boolean {
  return text.trimStart().startsWith("[sub-agent ");
}
