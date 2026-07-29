import {
  EventId,
  type DataAudience,
  type OrchestrationCommand,
  type OrchestrationEvent,
  type OrchestrationMessage,
  type OrchestrationReadModel,
  DEFAULT_DATA_AUDIENCE,
  type ThreadId,
} from "@t3tools/contracts";
import {
  hasActiveThreadSession,
  resolveThreadAttentionBlocker,
} from "@t3tools/shared/threadAttention";
import * as DateTime from "effect/DateTime";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import type * as PlatformError from "effect/PlatformError";

import { OrchestrationCommandInvariantError } from "./Errors.ts";
import {
  listThreadsByProjectId,
  requireActiveProjectWorkspaceRootAbsent,
  requireProject,
  requireProjectAbsent,
  requireThread,
  requireThreadArchived,
  requireThreadAbsent,
  requireThreadNotArchived,
} from "./commandInvariants.ts";
import { projectEvent } from "./projector.ts";

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

function projectCreateDataAudience(command: OrchestrationCommand): DataAudience {
  return (
    (command as { readonly dataAudience?: DataAudience }).dataAudience ?? DEFAULT_DATA_AUDIENCE
  );
}

// Session adoption takes seconds; a prompt message still unadopted after this
// window is a failed/stale start, not pending work. Mirrors the client's
// QUEUED_TURN_START_GRACE_MS in client-runtime threadSettled.ts.
const QUEUED_TURN_START_GRACE_MS = 2 * 60 * 1_000;

function isQueuedTurnPromptMessage(message: Pick<OrchestrationMessage, "role" | "text">): boolean {
  return (
    message.role === "user" ||
    (message.role === "system" && message.text.trimStart().startsWith("[sub-agent "))
  );
}

/**
 * Blocked-on-you work derived from the thread's retained activities: an
 * approval or user-input request with no later resolution for the same
 * requestId. The server-side twin of the shell's hasPendingApprovals /
 * hasPendingUserInput flags, which the decider read model does not carry.
 * The clearing rules MUST match ProjectionPipeline's pending accounting —
 * resolved activities always clear, respond.failed clears only when the
 * failure detail marks the request stale/unknown — or settle would be
 * rejected on threads whose shell flags read as clear.
 */
function isStaleRequestFailureDetail(payload: Record<string, unknown> | null): boolean {
  const detail = typeof payload?.detail === "string" ? payload.detail.toLowerCase() : null;
  if (detail === null) return false;
  return (
    detail.includes("stale pending approval request") ||
    detail.includes("unknown pending approval request") ||
    detail.includes("unknown pending permission request") ||
    detail.includes("stale pending user-input request") ||
    detail.includes("unknown pending user-input request") ||
    detail.includes("unknown pending user input request") ||
    detail.includes("unknown pending codex user input request")
  );
}

function hasOpenBlockingRequest(thread: {
  readonly activities: ReadonlyArray<{ readonly kind: string; readonly payload: unknown }>;
}): boolean {
  // Provider request IDs are only unique inside their request kind. Keep the
  // namespaces separate so resolving an approval cannot accidentally clear a
  // still-open user-input request that happens to reuse the same ID (or vice
  // versa). This mirrors ProjectionPipeline's separate pending maps.
  const openApprovalRequestIds = new Set<string>();
  const openUserInputRequestIds = new Set<string>();
  for (const activity of thread.activities) {
    const payload =
      typeof activity.payload === "object" && activity.payload !== null
        ? (activity.payload as Record<string, unknown>)
        : null;
    const requestId = typeof payload?.requestId === "string" ? payload.requestId : null;
    if (requestId === null) continue;
    if (activity.kind === "approval.requested") {
      openApprovalRequestIds.add(requestId);
    } else if (activity.kind === "user-input.requested") {
      openUserInputRequestIds.add(requestId);
    } else if (
      activity.kind === "approval.resolved" ||
      (activity.kind === "provider.approval.respond.failed" && isStaleRequestFailureDetail(payload))
    ) {
      openApprovalRequestIds.delete(requestId);
    } else if (
      activity.kind === "user-input.resolved" ||
      (activity.kind === "provider.user-input.respond.failed" &&
        isStaleRequestFailureDetail(payload))
    ) {
      openUserInputRequestIds.delete(requestId);
    }
  }
  return openApprovalRequestIds.size > 0 || openUserInputRequestIds.size > 0;
}

function withEventBase(
  input: Pick<OrchestrationCommand, "commandId"> & {
    readonly aggregateKind: OrchestrationEvent["aggregateKind"];
    readonly aggregateId: OrchestrationEvent["aggregateId"];
    readonly occurredAt: string;
    readonly metadata?: OrchestrationEvent["metadata"];
  },
): Effect.Effect<
  Omit<OrchestrationEvent, "sequence" | "type" | "payload">,
  PlatformError.PlatformError,
  Crypto.Crypto
> {
  return Crypto.Crypto.pipe(
    Effect.flatMap((crypto) =>
      crypto.randomUUIDv4.pipe(
        Effect.map((eventId) => ({
          eventId: EventId.make(eventId),
          aggregateKind: input.aggregateKind,
          aggregateId: input.aggregateId,
          occurredAt: input.occurredAt,
          commandId: input.commandId,
          causationEventId: null,
          correlationId: input.commandId,
          metadata: input.metadata ?? {},
        })),
      ),
    ),
  );
}

type PlannedOrchestrationEvent = Omit<OrchestrationEvent, "sequence">;

type DecideOrchestrationCommandResult =
  | PlannedOrchestrationEvent
  | ReadonlyArray<PlannedOrchestrationEvent>;

function isWorkspaceOnlyThreadMetaUpdate(
  command: Extract<OrchestrationCommand, { type: "thread.meta.update" }>,
): boolean {
  return command.title === undefined && command.modelSelection === undefined;
}

/**
 * Every not-yet-archived descendant of `rootThreadId` (children, grandchildren,
 * …), for archive cascade. Iterative BFS over `parentThreadId`; guards against
 * cycles so a malformed parent chain cannot loop forever.
 *
 * The parent/child index includes archived (but not deleted) threads so that an
 * already-archived intermediate node does not sever the subtree: we traverse
 * THROUGH archived nodes but only emit archive events for the unarchived
 * descendants below them. DELETED threads are excluded entirely — a deleted
 * parent re-roots its children in the UI (they render as roots, not under the
 * deleted node), so the cascade must not reach a live grandchild through a
 * deleted link.
 */
function collectUnarchivedDescendantIds(
  readModel: OrchestrationReadModel,
  rootThreadId: ThreadId,
): ReadonlyArray<ThreadId> {
  const childrenByParent = new Map<ThreadId, ThreadId[]>();
  const archivedById = new Map<ThreadId, boolean>();
  for (const thread of readModel.threads) {
    if (thread.deletedAt !== null) continue; // deleted threads are outside the tree
    archivedById.set(thread.id, thread.archivedAt !== null);
    const parentId = thread.parentThreadId ?? null;
    if (parentId === null) continue;
    const siblings = childrenByParent.get(parentId) ?? [];
    siblings.push(thread.id);
    childrenByParent.set(parentId, siblings);
  }
  const collected: ThreadId[] = [];
  const seen = new Set<ThreadId>([rootThreadId]);
  const queue: ThreadId[] = [rootThreadId];
  while (queue.length > 0) {
    const current = queue.shift() as ThreadId;
    for (const childId of childrenByParent.get(current) ?? []) {
      if (seen.has(childId)) continue;
      seen.add(childId);
      queue.push(childId); // traverse through archived nodes too
      if (archivedById.get(childId) !== true) collected.push(childId);
    }
  }
  return collected;
}

const decideCommandSequence = Effect.fn("decideCommandSequence")(function* ({
  commands,
  readModel,
}: {
  readonly commands: ReadonlyArray<OrchestrationCommand>;
  readonly readModel: OrchestrationReadModel;
}): Effect.fn.Return<
  ReadonlyArray<PlannedOrchestrationEvent>,
  OrchestrationCommandInvariantError | PlatformError.PlatformError,
  Crypto.Crypto
> {
  let nextReadModel = readModel;
  let nextSequence = readModel.snapshotSequence;
  const plannedEvents: PlannedOrchestrationEvent[] = [];

  for (const nextCommand of commands) {
    const decided = yield* decideOrchestrationCommand({
      command: nextCommand,
      readModel: nextReadModel,
    });
    const nextEvents = Array.isArray(decided) ? decided : [decided];
    for (const nextEvent of nextEvents) {
      plannedEvents.push(nextEvent);
      nextSequence += 1;
      nextReadModel = yield* projectEvent(nextReadModel, {
        ...nextEvent,
        sequence: nextSequence,
      }).pipe(Effect.orDie);
    }
  }

  return plannedEvents;
});

export const decideOrchestrationCommand = Effect.fn("decideOrchestrationCommand")(function* ({
  command,
  readModel,
  settlementContext,
}: {
  readonly command: OrchestrationCommand;
  readonly readModel: OrchestrationReadModel;
  /** Targeted, uncapped projection summary supplied by the production engine
   * for thread.settle. Direct decider tests may omit it and exercise the
   * event-derived fallback instead. */
  readonly settlementContext?: {
    readonly hasPendingApprovals: boolean;
    readonly hasPendingUserInput: boolean;
    readonly hasActionableProposedPlan: boolean;
    readonly latestPromptMessageAt: string | null;
  };
}): Effect.fn.Return<
  DecideOrchestrationCommandResult,
  OrchestrationCommandInvariantError | PlatformError.PlatformError,
  Crypto.Crypto
> {
  switch (command.type) {
    case "project.create": {
      yield* requireProjectAbsent({
        readModel,
        command,
        projectId: command.projectId,
      });
      yield* requireActiveProjectWorkspaceRootAbsent({
        readModel,
        command,
        workspaceRoot: command.workspaceRoot,
        exceptProjectId: command.projectId,
      });

      return {
        ...(yield* withEventBase({
          aggregateKind: "project",
          aggregateId: command.projectId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "project.created",
        payload: {
          projectId: command.projectId,
          title: command.title,
          workspaceRoot: command.workspaceRoot,
          dataAudience: projectCreateDataAudience(command),
          defaultModelSelection: command.defaultModelSelection ?? null,
          scripts: [],
          createdAt: command.createdAt,
          updatedAt: command.createdAt,
        },
      };
    }

    case "project.meta.update": {
      const project = yield* requireProject({
        readModel,
        command,
        projectId: command.projectId,
      });
      if (command.workspaceRoot !== undefined) {
        if (project.dataAudience === "factory" && command.workspaceRoot !== project.workspaceRoot) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: command.type,
            detail: `Factory project '${command.projectId}' cannot change workspace roots through a standard project command.`,
          });
        }
        if (
          command.workspaceRoot !== project.workspaceRoot &&
          listThreadsByProjectId(readModel, command.projectId).some(
            (thread) =>
              thread.worktreeRemovable === true &&
              (thread.worktreePath !== null || thread.worktreeRemovalPath !== null),
          )
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: command.type,
            detail: `Project '${command.projectId}' cannot change workspace roots while it owns a removable thread worktree.`,
          });
        }
        yield* requireActiveProjectWorkspaceRootAbsent({
          readModel,
          command,
          workspaceRoot: command.workspaceRoot,
          exceptProjectId: command.projectId,
        });
      }
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "project",
          aggregateId: command.projectId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "project.meta-updated",
        payload: {
          projectId: command.projectId,
          ...(command.title !== undefined ? { title: command.title } : {}),
          ...(command.workspaceRoot !== undefined ? { workspaceRoot: command.workspaceRoot } : {}),
          ...(command.defaultModelSelection !== undefined
            ? { defaultModelSelection: command.defaultModelSelection }
            : {}),
          ...(command.scripts !== undefined ? { scripts: command.scripts } : {}),
          updatedAt: occurredAt,
        },
      };
    }

    case "project.data-audience.set": {
      const project = yield* requireProject({
        readModel,
        command,
        projectId: command.projectId,
      });
      if (project.deletedAt !== null) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Project '${command.projectId}' is deleted and cannot handle command '${command.type}'.`,
        });
      }
      if (project.workspaceRoot !== command.expectedWorkspaceRoot) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Project '${command.projectId}' workspace root changed during audience administration.`,
        });
      }

      return {
        ...(yield* withEventBase({
          aggregateKind: "project",
          aggregateId: command.projectId,
          occurredAt: command.occurredAt,
          commandId: command.commandId,
        })),
        type: "project.data-audience-set",
        payload: {
          projectId: command.projectId,
          workspaceRoot: project.workspaceRoot,
          oldDataAudience: project.dataAudience,
          newDataAudience: "factory",
          actor: command.actor,
          updatedAt: command.occurredAt,
        },
      };
    }

    case "project.delete": {
      yield* requireProject({
        readModel,
        command,
        projectId: command.projectId,
      });
      const activeThreads = listThreadsByProjectId(readModel, command.projectId).filter(
        (thread) => thread.deletedAt === null,
      );
      if (activeThreads.length > 0 && command.force !== true) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Project '${command.projectId}' is not empty and cannot be deleted without force=true.`,
        });
      }
      if (activeThreads.length > 0) {
        return yield* decideCommandSequence({
          readModel,
          commands: [
            ...activeThreads.map(
              (thread): Extract<OrchestrationCommand, { type: "thread.delete" }> => ({
                type: "thread.delete",
                commandId: command.commandId,
                threadId: thread.id,
              }),
            ),
            {
              type: "project.delete",
              commandId: command.commandId,
              projectId: command.projectId,
            },
          ],
        });
      }

      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "project",
          aggregateId: command.projectId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "project.deleted" as const,
        payload: {
          projectId: command.projectId,
          deletedAt: occurredAt,
        },
      };
    }

    case "thread.create": {
      yield* requireProject({
        readModel,
        command,
        projectId: command.projectId,
      });
      yield* requireThreadAbsent({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.created",
        payload: {
          threadId: command.threadId,
          projectId: command.projectId,
          title: command.title,
          modelSelection: command.modelSelection,
          runtimeMode: command.runtimeMode,
          interactionMode: command.interactionMode,
          branch: command.branch,
          worktreePath: command.worktreePath,
          worktreeRemovable: command.worktreeRemovable ?? false,
          ...(command.worktreeRemovalPath !== undefined
            ? { worktreeRemovalPath: command.worktreeRemovalPath }
            : {}),
          createdAt: command.createdAt,
          updatedAt: command.createdAt,
        },
      };
    }

    case "thread.delete": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.deleted",
        payload: {
          threadId: command.threadId,
          deletedAt: occurredAt,
        },
      };
    }

    case "thread.archive": {
      yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      const occurredAt = yield* nowIso;
      // Archiving a parent cascades to every not-yet-archived descendant
      // (recursive), so a parent and its whole subtree leave the active list in
      // one command. Already-archived descendants are skipped (idempotent).
      const descendantIds = collectUnarchivedDescendantIds(readModel, command.threadId);
      const archivedThreadIds = [command.threadId, ...descendantIds];
      return yield* Effect.forEach(archivedThreadIds, (threadId) =>
        Effect.map(
          withEventBase({
            aggregateKind: "thread",
            aggregateId: threadId,
            occurredAt,
            commandId: command.commandId,
          }),
          (base) => ({
            ...base,
            type: "thread.archived" as const,
            payload: {
              threadId,
              archivedAt: occurredAt,
              updatedAt: occurredAt,
            },
          }),
        ),
      );
    }

    case "thread.unarchive": {
      yield* requireThreadArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.unarchived",
        payload: {
          threadId: command.threadId,
          updatedAt: occurredAt,
        },
      };
    }

    case "thread.settle": {
      const thread = yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      // Server-side twin of the client's canSettle attention checks: a stale
      // or raced client must not settle work that should stay visible.
      const hasPendingInteraction = settlementContext
        ? settlementContext.hasPendingApprovals || settlementContext.hasPendingUserInput
        : hasOpenBlockingRequest(thread);
      const attentionBlocker = resolveThreadAttentionBlocker({
        hasPendingApprovals: hasPendingInteraction,
        hasPendingUserInput: false,
        hasActionableProposedPlan: settlementContext?.hasActionableProposedPlan ?? false,
        session: thread.session,
        latestTurn: thread.latestTurn,
      });
      if (attentionBlocker !== null) {
        const detail =
          attentionBlocker === "failed"
            ? `thread ${command.threadId} has failed work and cannot be settled`
            : attentionBlocker === "working"
              ? `thread ${command.threadId} has active work and cannot be settled`
              : attentionBlocker === "plan"
                ? `thread ${command.threadId} has an actionable proposed plan and cannot be settled`
                : `thread ${command.threadId} has a pending approval or user-input request and cannot be settled`;
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail,
        });
      }
      const occurredAt = yield* nowIso;
      // A queued turn start — a user or sub-agent wake prompt no turn has picked up yet — is
      // work in flight even though session is still null (turn.start emits
      // message-sent + turn-start-requested; the session arrives later).
      // Settling in that window would hide just-requested work. Detection
      // mirrors the client's hasQueuedTurnStart: the newest prompt message is
      // strictly newer than every latestTurn timestamp (adoption stamps the
      // new turn's requestedAt with the message time, clearing this), and
      // only within the adoption grace window — historical threads whose
      // last prompt message postdates their turn timestamps (older-server
      // data, mid-turn messages) must stay settleable. A failed session
      // start (status "error") clears the block immediately.
      const latestPromptMessageAtMs = settlementContext
        ? settlementContext.latestPromptMessageAt === null
          ? Number.NEGATIVE_INFINITY
          : Date.parse(settlementContext.latestPromptMessageAt)
        : thread.messages.reduce(
            (latest, message) =>
              isQueuedTurnPromptMessage(message)
                ? Math.max(latest, Date.parse(message.createdAt))
                : latest,
            Number.NEGATIVE_INFINITY,
          );
      const latestTurnAtMs =
        thread.latestTurn === null
          ? Number.NEGATIVE_INFINITY
          : Math.max(
              ...[
                thread.latestTurn.requestedAt,
                thread.latestTurn.startedAt,
                thread.latestTurn.completedAt,
              ].map((candidate) =>
                candidate == null ? Number.NEGATIVE_INFINITY : Date.parse(candidate),
              ),
            );
      // The age check is bounded on BOTH sides: message timestamps are
      // client-supplied, so a client clock ahead of the server yields a
      // negative age. Without the lower bound that negative age satisfies
      // `<= grace` for as long as the skew lasts, extending the settle
      // block far past the intended two minutes.
      const queuedAgeMs = Date.parse(occurredAt) - latestPromptMessageAtMs;
      const hasQueuedTurnStart =
        thread.session?.status !== "error" &&
        Number.isFinite(latestPromptMessageAtMs) &&
        latestPromptMessageAtMs > latestTurnAtMs &&
        Math.abs(queuedAgeMs) <= QUEUED_TURN_START_GRACE_MS;
      if (hasQueuedTurnStart) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `thread ${command.threadId} has a queued turn start and cannot be settled`,
        });
      }
      // Settling an already-settled thread re-emits with the original
      // settledAt: the engine rejects zero-event commands, and bulk-settle /
      // double-click must stay silent no-ops rather than surface errors.
      const alreadySettled = thread.settledOverride === "settled" && thread.settledAt !== null;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.settled",
        payload: {
          threadId: command.threadId,
          settledAt: alreadySettled ? thread.settledAt : occurredAt,
          // A re-emission is a projected no-op: keep the existing updatedAt
          // so duplicate settles neither rewind nor churn ordering. A fresh
          // settle stamps the command time.
          updatedAt: alreadySettled ? thread.updatedAt : occurredAt,
        },
      };
    }

    case "thread.unsettle": {
      const thread = yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      // Idempotent by re-emission (see thread.settle): reducing the event a
      // second time lands on the same override state. A re-emission keeps
      // the existing updatedAt so duplicates do not churn ordering.
      const alreadyPinnedActive = thread.settledOverride === "active";
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.unsettled",
        payload: {
          threadId: command.threadId,
          reason: command.reason,
          updatedAt: alreadyPinnedActive ? thread.updatedAt : occurredAt,
        },
      };
    }

    case "thread.meta.update": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      if (thread.archivedAt !== null && !isWorkspaceOnlyThreadMetaUpdate(command)) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Thread '${command.threadId}' is already archived and cannot handle command '${command.type}'.`,
        });
      }
      const branch =
        command.branch !== undefined &&
        command.expectedBranch !== undefined &&
        thread.branch !== command.expectedBranch
          ? thread.branch
          : command.branch;
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.meta-updated",
        payload: {
          threadId: command.threadId,
          ...(command.title !== undefined ? { title: command.title } : {}),
          ...(command.modelSelection !== undefined
            ? { modelSelection: command.modelSelection }
            : {}),
          ...(branch !== undefined ? { branch } : {}),
          ...(command.worktreePath !== undefined ? { worktreePath: command.worktreePath } : {}),
          ...(command.worktreeRemovable !== undefined
            ? { worktreeRemovable: command.worktreeRemovable }
            : {}),
          ...(command.worktreeRemovalPath !== undefined
            ? { worktreeRemovalPath: command.worktreeRemovalPath }
            : {}),
          updatedAt: occurredAt,
        },
      };
    }

    case "thread.runtime-mode.set": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.runtime-mode-set",
        payload: {
          threadId: command.threadId,
          runtimeMode: command.runtimeMode,
          updatedAt: occurredAt,
        },
      };
    }

    case "thread.interaction-mode.set": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.interaction-mode-set",
        payload: {
          threadId: command.threadId,
          interactionMode: command.interactionMode,
          updatedAt: occurredAt,
        },
      };
    }

    case "thread.parent.set": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      if (command.parentEnvironmentId === undefined) {
        yield* requireThread({
          readModel,
          command,
          threadId: command.parentThreadId,
        });
      }
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.parent-set",
        payload: {
          threadId: command.threadId,
          parentThreadId: command.parentThreadId,
          ...(command.parentEnvironmentId !== undefined
            ? { parentEnvironmentId: command.parentEnvironmentId }
            : {}),
          updatedAt: occurredAt,
        },
      };
    }

    case "thread.turn.start": {
      const targetThread = yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      const sourceProposedPlan = command.sourceProposedPlan;
      const sourceThread = sourceProposedPlan
        ? yield* requireThread({
            readModel,
            command,
            threadId: sourceProposedPlan.threadId,
          })
        : null;
      const sourcePlan =
        sourceProposedPlan && sourceThread
          ? sourceThread.proposedPlans.find((entry) => entry.id === sourceProposedPlan.planId)
          : null;
      if (sourceProposedPlan && !sourcePlan) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Proposed plan '${sourceProposedPlan.planId}' does not exist on thread '${sourceProposedPlan.threadId}'.`,
        });
      }
      if (sourceThread && sourceThread.projectId !== targetThread.projectId) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Proposed plan '${sourceProposedPlan?.planId}' belongs to thread '${sourceThread.id}' in a different project.`,
        });
      }
      const userMessageEvent: Omit<OrchestrationEvent, "sequence"> = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.message-sent",
        payload: {
          threadId: command.threadId,
          messageId: command.message.messageId,
          role: command.message.role,
          text: command.message.text,
          attachments: command.message.attachments,
          turnId: null,
          streaming: false,
          createdAt: command.createdAt,
          updatedAt: command.createdAt,
        },
      };
      const turnStartRequestedEvent: Omit<OrchestrationEvent, "sequence"> = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        causationEventId: userMessageEvent.eventId,
        type: "thread.turn-start-requested",
        payload: {
          threadId: command.threadId,
          messageId: command.message.messageId,
          ...(command.modelSelection !== undefined
            ? { modelSelection: command.modelSelection }
            : {}),
          ...(command.titleSeed !== undefined ? { titleSeed: command.titleSeed } : {}),
          runtimeMode:
            Object.hasOwn(command, "runtimeMode") && command.runtimeMode !== undefined
              ? command.runtimeMode
              : targetThread.runtimeMode,
          interactionMode:
            Object.hasOwn(command, "interactionMode") && command.interactionMode !== undefined
              ? command.interactionMode
              : targetThread.interactionMode,
          ...(command.providerSessionDetached !== undefined
            ? { providerSessionDetached: command.providerSessionDetached }
            : {}),
          ...(sourceProposedPlan !== undefined ? { sourceProposedPlan } : {}),
          createdAt: command.createdAt,
        },
      };
      // Real activity resets ANY override: it wakes an explicitly settled
      // thread, and it clears a keep-active pin back to neutral so the
      // thread can auto-settle again after this burst of work goes stale.
      if (targetThread.settledOverride === null) {
        return [userMessageEvent, turnStartRequestedEvent];
      }
      const unsettledEvent: Omit<OrchestrationEvent, "sequence"> = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.unsettled",
        payload: {
          threadId: command.threadId,
          reason: "activity",
          updatedAt: command.createdAt,
        },
      };
      return [unsettledEvent, userMessageEvent, turnStartRequestedEvent];
    }

    case "thread.turn.interrupt": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.turn-interrupt-requested",
        payload: {
          threadId: command.threadId,
          ...(command.turnId !== undefined ? { turnId: command.turnId } : {}),
          createdAt: command.createdAt,
        },
      };
    }

    case "thread.approval.respond": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
          metadata: {
            requestId: command.requestId,
          },
        })),
        type: "thread.approval-response-requested",
        payload: {
          threadId: command.threadId,
          requestId: command.requestId,
          decision: command.decision,
          createdAt: command.createdAt,
        },
      };
    }

    case "thread.user-input.respond": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
          metadata: {
            requestId: command.requestId,
          },
        })),
        type: "thread.user-input-response-requested",
        payload: {
          threadId: command.threadId,
          requestId: command.requestId,
          answers: command.answers,
          createdAt: command.createdAt,
        },
      };
    }

    case "thread.checkpoint.revert": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.checkpoint-revert-requested",
        payload: {
          threadId: command.threadId,
          turnCount: command.turnCount,
          createdAt: command.createdAt,
        },
      };
    }

    case "thread.session.stop": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.session-stop-requested",
        payload: {
          threadId: command.threadId,
          createdAt: command.createdAt,
        },
      };
    }

    case "thread.session.set": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const sessionSetEvent: Omit<OrchestrationEvent, "sequence"> = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
          metadata: {},
        })),
        type: "thread.session-set",
        payload: {
          threadId: command.threadId,
          session: command.session,
        },
      };
      // Only a session coming alive is activity worth waking a settled thread
      // for — status writes like ready/stopped/error arrive after the fact and
      // must not fight a user's explicit settle.
      const isSessionActivity = hasActiveThreadSession(command.session);
      // Real activity resets ANY override (settled wakes, active unpins).
      if (thread.settledOverride === null || !isSessionActivity) {
        return sessionSetEvent;
      }
      const unsettledEvent: Omit<OrchestrationEvent, "sequence"> = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.unsettled",
        payload: {
          threadId: command.threadId,
          reason: "activity",
          updatedAt: command.createdAt,
        },
      };
      return [unsettledEvent, sessionSetEvent];
    }

    case "thread.message.assistant.delta": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.message-sent",
        payload: {
          threadId: command.threadId,
          messageId: command.messageId,
          role: "assistant",
          text: command.delta,
          turnId: command.turnId ?? null,
          streaming: true,
          createdAt: command.createdAt,
          updatedAt: command.createdAt,
        },
      };
    }

    case "thread.message.assistant.complete": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.message-sent",
        payload: {
          threadId: command.threadId,
          messageId: command.messageId,
          role: "assistant",
          text: "",
          turnId: command.turnId ?? null,
          streaming: false,
          createdAt: command.createdAt,
          updatedAt: command.createdAt,
        },
      };
    }

    case "thread.proposed-plan.upsert": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.proposed-plan-upserted",
        payload: {
          threadId: command.threadId,
          proposedPlan: command.proposedPlan,
        },
      };
    }

    case "thread.turn.diff.complete": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.turn-diff-completed",
        payload: {
          threadId: command.threadId,
          turnId: command.turnId,
          checkpointTurnCount: command.checkpointTurnCount,
          checkpointRef: command.checkpointRef,
          status: command.status,
          files: command.files,
          assistantMessageId: command.assistantMessageId ?? null,
          completedAt: command.completedAt,
        },
      };
    }

    case "thread.turn.effective-model.set": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.turn-effective-model-set",
        payload: {
          threadId: command.threadId,
          turnId: command.turnId,
          effectiveModel: command.effectiveModel,
        },
      };
    }

    case "thread.revert.complete": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.reverted",
        payload: {
          threadId: command.threadId,
          turnCount: command.turnCount,
        },
      };
    }

    case "thread.activity.append": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const requestId =
        typeof command.activity.payload === "object" &&
        command.activity.payload !== null &&
        "requestId" in command.activity.payload &&
        typeof (command.activity.payload as { requestId?: unknown }).requestId === "string"
          ? ((command.activity.payload as { requestId: string })
              .requestId as OrchestrationEvent["metadata"]["requestId"])
          : undefined;
      const activityAppendedEvent: Omit<OrchestrationEvent, "sequence"> = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
          ...(requestId !== undefined ? { metadata: { requestId } } : {}),
        })),
        type: "thread.activity-appended",
        payload: {
          threadId: command.threadId,
          activity: command.activity,
        },
      };
      // An approval or user-input request is blocked-on-you work — it must
      // never stay hidden inside a settled slim row.
      const wakesSettledThread =
        command.activity.kind === "approval.requested" ||
        command.activity.kind === "user-input.requested";
      // Real activity resets ANY override (settled wakes, active unpins).
      if (thread.settledOverride === null || !wakesSettledThread) {
        return activityAppendedEvent;
      }
      const unsettledEvent: Omit<OrchestrationEvent, "sequence"> = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.unsettled",
        payload: {
          threadId: command.threadId,
          reason: "activity",
          updatedAt: command.createdAt,
        },
      };
      return [unsettledEvent, activityAppendedEvent];
    }

    default: {
      command satisfies never;
      const fallback = command as never as { type: string };
      return yield* new OrchestrationCommandInvariantError({
        commandType: fallback.type,
        detail: `Unknown command type: ${fallback.type}`,
      });
    }
  }
});
