/**
 * ChildThreadCoordinator implementation - see Services/ChildThreadCoordinator.ts
 * and finalPlan §5 for the design. The `ActiveChildThreadCoordinatorLive`
 * global-capture mirrors `ActiveBootstrapTurnStartDispatcherLive` /
 * `ThreadStartRuntimeLive` so MCP tool handlers can reach the coordinator
 * without threading it through the toolkit `Context`.
 */
import {
  CommandId,
  EventId,
  IsoDateTime,
  MessageId,
  type ModelSelection,
  type OrchestrationEvent,
  type OrchestrationLatestTurn,
  type OrchestrationThread,
  type OrchestrationThreadShell,
  type ThreadId,
  type TurnId,
} from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import {
  dispatchActive,
  type BootstrapTurnStartDispatcherShape,
} from "../Services/BootstrapTurnStartDispatcher.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { ProviderInstanceRegistry } from "../../provider/Services/ProviderInstanceRegistry.ts";
import { SubagentDispatchLimiter } from "../../mcp/toolkits/subagent/SubagentDispatchLimiter.ts";
import {
  PendingDispatchRepository,
  type PendingDispatch,
  type PendingDispatchId,
} from "../../persistence/Services/PendingDispatches.ts";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import * as Schema from "effect/Schema";
import { ThreadStartToolError } from "../../mcp/toolkits/thread/tools.ts";
import {
  ChildThreadCoordinator,
  MAX_DEPTH,
  PENDING_MAX_AGE_MS,
  PROJECTION_READ_TIMEOUT_MS,
  WAIT_SLICE_SECONDS,
  type ChildListEntry,
  type ChildTerminalStatus,
  type ChildThreadCoordinatorShape,
  type EnqueueParentInjectionInput,
  type ChildWaitResult,
  type WaitDeliveredMark,
  type WaitChildResult,
  type WaitSliceInput,
  type WaitSliceResult,
} from "../Services/ChildThreadCoordinator.ts";

interface ChildRecord {
  readonly parentThreadId: ThreadId;
  readonly detached: boolean;
  readonly model: ModelSelection;
  readonly spawnedAtMs: number;
  readonly depth: number;
  readonly terminal: Deferred.Deferred<ChildWaitResult>;
}

interface PendingInjection {
  readonly childThreadId: ThreadId;
  readonly status: ChildTerminalStatus;
  readonly text: string | null;
  readonly error: string | null;
  readonly enqueuedAtMs: number;
  /** The durable `pending_dispatches` row backing this injection (R-B), deleted on drain. */
  readonly dispatchId: PendingDispatchId;
  readonly deliveredByWait: boolean;
  readonly waitCancellable: boolean;
  /**
   * The command id this row must dispatch under (R-B exactly-once). Null for a
   * fresh injection that may be consolidated into a batch under a fresh
   * deterministic id; non-null means it MUST be dispatched alone under this exact
   * id so the engine's receipt dedup makes a landed turn a no-op (no duplicate)
   * and an un-landed turn fire (no loss), regardless of how rows later re-batch.
   */
  readonly claimedCommandId: CommandId | null;
}

type TurnDiffCompletedEvent = Extract<OrchestrationEvent, { type: "thread.turn-diff-completed" }>;
type TurnStartRequestedEvent = Extract<OrchestrationEvent, { type: "thread.turn-start-requested" }>;
type SessionSetEvent = Extract<OrchestrationEvent, { type: "thread.session-set" }>;
type ActivityAppendedEvent = Extract<OrchestrationEvent, { type: "thread.activity-appended" }>;
type ThreadArchivedEvent = Extract<OrchestrationEvent, { type: "thread.archived" }>;
type ThreadUnarchivedEvent = Extract<OrchestrationEvent, { type: "thread.unarchived" }>;
type ThreadDeletedEvent = Extract<OrchestrationEvent, { type: "thread.deleted" }>;

interface ChildTerminalOutcome {
  readonly status: ChildTerminalStatus;
  readonly error: string | null;
  readonly fromSessionProjection?: boolean;
}

const turnTerminalOutcome = (
  latestTurn: OrchestrationLatestTurn,
  session: OrchestrationThread["session"] | OrchestrationThreadShell["session"],
): ChildTerminalOutcome =>
  latestTurn.state === "completed"
    ? { status: "completed", error: null }
    : { status: "failed", error: session?.lastError ?? `turn ${latestTurn.state}` };

const isThreadArchivedOutcome = (outcome: ChildTerminalOutcome): boolean =>
  outcome.status === "killed" && outcome.error === "thread archived";

const ChildRowSchema = Schema.Struct({
  threadId: Schema.String,
  parentThreadId: Schema.NullOr(Schema.String),
});

const WaitDeliveryRowSchema = Schema.Struct({
  childThreadId: Schema.String,
  parentThreadId: Schema.String,
  deliveredAt: Schema.String,
  parentTurnIdAtDelivery: Schema.NullOr(Schema.String),
});

const PromotedChildRowSchema = Schema.Struct({
  childThreadId: Schema.String,
  parentThreadId: Schema.String,
});

const fail = (message: string) => new ThreadStartToolError({ message });

/** Cap on a single consolidated wake/drain injection turn (truncated past this). */
const CONSOLIDATED_INJECTION_MAX_CHARS = 2_000;

/**
 * Pick the latest assistant message authored at or after the latest turn was
 * requested (lens-3 guard so a stale assistant message from a prior turn is
 * never reported as this turn's output).
 */
export const finalAssistantTextFromThread = (thread: OrchestrationThread): string | null => {
  const latestTurn = thread.latestTurn;
  if (!latestTurn) return null;
  let chosen: string | null = null;
  for (const message of thread.messages) {
    if (message.role !== "assistant") continue;
    if (message.createdAt < latestTurn.requestedAt) continue;
    chosen = message.text;
  }
  return chosen;
};

const isThreadIdle = (shell: OrchestrationThreadShell): boolean => {
  const turnRunning = shell.latestTurn?.state === "running";
  if (turnRunning) return false;
  const session = shell.session;
  if (session === null) return true;
  return (
    (session.status === "ready" || session.status === "waiting" || session.status === "stopped") &&
    session.activeTurnId === null
  );
};

const isProjectedChildActive = (shell: OrchestrationThreadShell): boolean => {
  if (shell.latestTurn?.state === "running") return true;
  const session = shell.session;
  return (
    (session?.status === "running" || session?.status === "waiting") &&
    session.activeTurnId !== null
  );
};

const shouldSettleTerminalSession = (
  status: SessionSetEvent["payload"]["session"]["status"],
): boolean =>
  // The provider can no longer advance a child once its session is terminal.
  // A still-running turn projection is therefore stale liveness, not a reason
  // to retain the child's dispatch lease.
  status === "error" || status === "stopped";

const terminalSessionOwnsProjectedSession = (input: {
  readonly session: SessionSetEvent["payload"]["session"];
  readonly shell: Option.Option<OrchestrationThreadShell>;
  readonly allowStoppedProjectionLag: boolean;
  readonly expectedActiveTurnId: TurnId | undefined;
}): boolean =>
  (input.session.status !== "error" && input.session.status !== "stopped") ||
  Option.match(input.shell, {
    onNone: () => false,
    onSome: (projected) => {
      const projectedSession = projected.session;
      if (
        projectedSession?.status === input.session.status &&
        projectedSession.updatedAt === input.session.updatedAt &&
        projectedSession.lastError === input.session.lastError
      ) {
        return true;
      }
      const projectedSessionUpdatedAt = parseIsoMillis(projectedSession?.updatedAt);
      const eventSessionUpdatedAt = parseIsoMillis(input.session.updatedAt);
      return (
        input.session.status === "stopped" &&
        input.allowStoppedProjectionLag &&
        input.expectedActiveTurnId !== undefined &&
        projectedSessionUpdatedAt !== null &&
        eventSessionUpdatedAt !== null &&
        projectedSessionUpdatedAt < eventSessionUpdatedAt &&
        projected.latestTurn?.state === "running" &&
        projected.latestTurn.turnId === input.expectedActiveTurnId &&
        (projectedSession?.status === "running" || projectedSession?.status === "waiting") &&
        projectedSession.activeTurnId === input.expectedActiveTurnId
      );
    },
  });

const activeTurnReportedBySession = (
  session: SessionSetEvent["payload"]["session"],
): TurnId | undefined =>
  (session.status === "running" || session.status === "waiting") && session.activeTurnId !== null
    ? session.activeTurnId
    : undefined;

const pendingSameTurnBecameIdle = (input: {
  readonly event: TurnDiffCompletedEvent;
  readonly shell: Option.Option<OrchestrationThreadShell>;
  readonly pendingSameTurnId: TurnId | undefined;
}): boolean => {
  const latestTurn = Option.isSome(input.shell) ? input.shell.value.latestTurn : null;
  const session = Option.isSome(input.shell) ? input.shell.value.session : null;
  return (
    input.pendingSameTurnId === input.event.payload.turnId &&
    latestTurn?.turnId === input.event.payload.turnId &&
    latestTurn.state !== "running" &&
    (session === null || session.activeTurnId === null)
  );
};

const eventTurnIsStillProjectedRunning = (input: {
  readonly event: TurnDiffCompletedEvent;
  readonly shell: Option.Option<OrchestrationThreadShell>;
}): boolean => {
  const latestTurn = Option.isSome(input.shell) ? input.shell.value.latestTurn : null;
  return latestTurn?.turnId === input.event.payload.turnId && latestTurn.state === "running";
};

const recordPendingTurnStart = (input: {
  readonly event: TurnStartRequestedEvent;
  readonly activeTurns: Map<ThreadId, TurnId>;
  readonly pendingStarts: Map<ThreadId, EventId>;
  readonly pendingSameTurnStarts: Map<ThreadId, TurnId>;
}): void => {
  const threadId = input.event.payload.threadId;
  const sameTurnId = input.activeTurns.get(threadId);
  const alreadyPending = input.pendingStarts.has(threadId);
  if (sameTurnId !== undefined) {
    input.pendingSameTurnStarts.set(threadId, sameTurnId);
  } else if (!alreadyPending) {
    input.pendingSameTurnStarts.delete(threadId);
  }
  input.activeTurns.delete(threadId);
  input.pendingStarts.set(threadId, input.event.eventId);
};

const failedTurnStartRequestId = (
  activity: ActivityAppendedEvent["payload"]["activity"],
): EventId | undefined => {
  if (
    activity.kind !== "provider.turn.start.failed" ||
    activity.payload === null ||
    typeof activity.payload !== "object" ||
    Array.isArray(activity.payload) ||
    !("turnStartRequestId" in activity.payload) ||
    typeof activity.payload.turnStartRequestId !== "string"
  ) {
    return undefined;
  }
  return EventId.make(activity.payload.turnStartRequestId);
};

const clearFailedPendingTurnStart = (input: {
  readonly threadId: ThreadId;
  readonly failedRequestId: EventId | undefined;
  readonly activeTurns: Map<ThreadId, TurnId>;
  readonly pendingStarts: Map<ThreadId, EventId>;
  readonly pendingSameTurnStarts: Map<ThreadId, TurnId>;
}): void => {
  if (
    input.failedRequestId === undefined ||
    input.pendingStarts.get(input.threadId) !== input.failedRequestId
  ) {
    return;
  }
  const preservedActiveTurnId = input.pendingSameTurnStarts.get(input.threadId);
  input.pendingStarts.delete(input.threadId);
  input.pendingSameTurnStarts.delete(input.threadId);
  if (preservedActiveTurnId !== undefined) {
    input.activeTurns.set(input.threadId, preservedActiveTurnId);
  }
};

const parseIsoMillis = (value: string | null | undefined): number | null => {
  if (value == null) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
};

/**
 * "turnCount == 0" parent guard (bug #2336): a parent that has never run a turn
 * and has no session must NEVER be resumed; it can only be enqueued.
 */
const isFreshParent = (shell: OrchestrationThreadShell): boolean =>
  shell.latestTurn === null && shell.session === null;

const isSubagentWakeSystemMessage = (text: string): boolean =>
  text.trimStart().startsWith("[sub-agent ");

const latestUserMessageAtFromThread = (thread: OrchestrationThread): string | null => {
  let latest: string | null = null;
  for (const message of thread.messages) {
    if (
      message.role !== "user" &&
      !(message.role === "system" && isSubagentWakeSystemMessage(message.text))
    ) {
      continue;
    }
    if (latest === null || message.createdAt > latest) latest = message.createdAt;
  }
  return latest;
};

const projectedLifecycleTerminal = (thread: OrchestrationThread): ChildTerminalOutcome | null => {
  if (thread.deletedAt !== null) return { status: "killed", error: "thread deleted" };
  if (
    (thread.session?.status === "running" || thread.session?.status === "waiting") &&
    thread.session.activeTurnId !== null
  ) {
    return null;
  }
  const latestTurn = thread.latestTurn;
  if (latestTurn?.state === "running") {
    if (thread.archivedAt !== null) return { status: "killed", error: "thread archived" };
    if (thread.session?.status === "error") {
      return {
        status: "failed",
        error: thread.session.lastError ?? "session error",
        fromSessionProjection: true,
      };
    }
    if (thread.session?.status === "stopped") {
      return {
        status: "failed",
        error: thread.session.lastError ?? "session stopped",
        fromSessionProjection: true,
      };
    }
    return null;
  }
  const terminalAt = latestTurn?.completedAt ?? latestTurn?.startedAt ?? latestTurn?.requestedAt;
  const latestUserMessageAt = latestUserMessageAtFromThread(thread);
  const staleTerminal =
    terminalAt !== undefined && latestUserMessageAt !== null && latestUserMessageAt > terminalAt;
  const terminalAfterArchive =
    thread.archivedAt !== null &&
    terminalAt !== undefined &&
    (parseIsoMillis(terminalAt) ?? Number.NEGATIVE_INFINITY) >
      (parseIsoMillis(thread.archivedAt) ?? Number.POSITIVE_INFINITY);
  const latestTurnTerminal: ChildTerminalOutcome | null =
    staleTerminal || latestTurn === null
      ? null
      : latestTurn.state === "completed" ||
          latestTurn.state === "error" ||
          latestTurn.state === "interrupted"
        ? turnTerminalOutcome(latestTurn, thread.session)
        : null;
  if (thread.archivedAt !== null) {
    return terminalAfterArchive
      ? { status: "killed", error: "thread archived" }
      : (latestTurnTerminal ?? { status: "killed", error: "thread archived" });
  }
  if (thread.session?.status === "error") {
    return {
      status: "failed",
      error: thread.session.lastError ?? "session error",
      fromSessionProjection: true,
    };
  }
  if (thread.session?.status === "stopped") {
    if (latestTurnTerminal !== null) return latestTurnTerminal;
    return {
      status: "failed",
      error: thread.session.lastError ?? "session stopped",
      fromSessionProjection: true,
    };
  }
  return null;
};

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const orchestrationEngine = yield* OrchestrationEngineService;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const registry = yield* ProviderInstanceRegistry;
  const dispatchLimiter = yield* SubagentDispatchLimiter;
  const pendingDispatches = yield* PendingDispatchRepository;
  const sql = yield* SqlClient;

  const children = new Map<ThreadId, ChildRecord>();
  const byParent = new Map<ThreadId, Set<ThreadId>>();
  const pendingInjections = new Map<ThreadId, Array<PendingInjection>>();
  const archivedChildIds = new Set<ThreadId>();
  const archivedActiveChildIds = new Set<ThreadId>();
  const unarchivedTerminalChildIds = new Set<ThreadId>();
  const pendingDrainRetryParents = new Set<ThreadId>();
  const pendingIdleDrainRetryParents = new Set<ThreadId>();
  const parentWakeLocks = new Map<ThreadId, Semaphore.Semaphore>();
  // Children promoted to wake-on-completion (R-A): a waited child whose waiter
  // stopped, so its completion must wake the parent like a detached child.
  const promotedChildren = new Set<ThreadId>();
  const promotingChildren = new Map<ThreadId, Deferred.Deferred<void>>();
  // Promoted foreground children whose terminal result was returned to a later
  // waiter. Their durable wake fallback stays queued until the parent turn
  // commits, then drainPending prunes it instead of dispatching a duplicate.
  const waitDeliveredPromotedChildren = new Set<ThreadId>();
  // Promoted children with a foreground wait currently delivering a terminal
  // result. If they settle live while the wait is in-flight, wakeParent must
  // queue a cancellable fallback instead of immediately dispatching a duplicate.
  const activePromotedWaitChildren = new Map<ThreadId, number>();
  const hasActivePromotedWait = (childThreadId: ThreadId): boolean =>
    (activePromotedWaitChildren.get(childThreadId) ?? 0) > 0;
  const beginActivePromotedWait = (childThreadId: ThreadId) =>
    activePromotedWaitChildren.set(
      childThreadId,
      (activePromotedWaitChildren.get(childThreadId) ?? 0) + 1,
    );
  const endActivePromotedWait = (childThreadId: ThreadId) => {
    const count = activePromotedWaitChildren.get(childThreadId) ?? 0;
    if (count <= 1) {
      activePromotedWaitChildren.delete(childThreadId);
      return;
    }
    activePromotedWaitChildren.set(childThreadId, count - 1);
  };
  const waitDeliveryMarkedAt = new Map<ThreadId, string>();
  const waitDeliveryParentTurnAt = new Map<ThreadId, TurnId>();
  // Children that already have a durable parent wake row. On restart this lets
  // log reconciliation settle the child without creating a second wake row.
  const queuedWakeChildren = new Set<ThreadId>();
  // Last active provider turn observed for each child. Session-ready is only
  // terminal when the projected terminal turn matches this id.
  const activeTurnByChild = new Map<ThreadId, TurnId>();
  // A provider turn-start request has no turn id, so it invalidates the prior
  // active turn until a session-set reports the new active turn id.
  const pendingTurnStartByChild = new Map<ThreadId, EventId>();
  // If a new request is injected while a provider turn is already active, Claude
  // keeps working in that same turn. Only that remembered turn may clear the
  // pending-start guard from an idle projection; otherwise an old delayed diff
  // for a previously completed turn can settle the new request.
  const pendingSameTurnStartByChild = new Map<ThreadId, TurnId>();
  // Stopped session-set events skipped while a replacement start is pending.
  // Once the pending start clears, wait reconciliation may settle that stopped projection.
  const pendingStoppedSettlementByChild = new Set<ThreadId>();
  // A conservative "missing" turn diff while the projection still shows the
  // turn running is not terminal by itself, but a later stopped/error session
  // must fail the child instead of leaving the waiter pending forever.
  const missingDiffWhileRunningByChild = new Set<ThreadId>();
  // Per-child guard so a deferred child_steer drain (R-C) is serialised and
  // never double-dispatches against the same child.
  const childSteerLocks = new Map<ThreadId, Semaphore.Semaphore>();
  let service: ChildThreadCoordinatorShape;

  const nowMillis = Effect.clockWith((clock) => clock.currentTimeMillis);
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const randomUUID = crypto.randomUUIDv4.pipe(Effect.orDie);
  const newCommandId = (tag: string) =>
    randomUUID.pipe(Effect.map((uuid) => CommandId.make(`server:${tag}:${uuid}`)));

  // Exactly-once delivery (closes the crash-between-dispatch-and-delete window):
  //
  // The orchestration engine upserts a command receipt in the SAME
  // sql.withTransaction as the event append and short-circuits any later
  // dispatch carrying an already-`accepted` commandId
  // (OrchestrationEngine.processEnvelope). So a re-fire under the IDENTICAL
  // commandId is a no-op (returns the existing sequence, appends no event).
  //
  // To exploit that across a crash we must guarantee the commandId is stable for
  // a given row REGARDLESS of how it later re-batches. A deterministic batch id
  // alone is insufficient: a row dispatched in batch [X,Y] then re-batched as
  // [X,Y,Z] on restart would get a different id and dedup would miss, duplicating
  // X,Y. We therefore CLAIM the exact commandId onto the rows durably BEFORE the
  // dispatch (pendingDispatches.claim). On restart a row with a claimed
  // command_id is re-dispatched under THAT id (landed -> dedup no-op -> no
  // duplicate; un-landed -> the intended turn fires -> no loss). The row survives
  // until the delete commits, so nothing is ever lost. Unclaimed rows are free to
  // consolidate under a fresh deterministic batch id.
  const dispatchCommandIdFor = (tag: string, dispatchId: PendingDispatchId): CommandId =>
    CommandId.make(`server:${tag}:${dispatchId}`);

  const batchCommandIdFor = (
    tag: string,
    dispatchIds: ReadonlyArray<PendingDispatchId>,
  ): CommandId => CommandId.make(`server:${tag}:${[...dispatchIds].sort().join(",")}`);

  const listPersistedChildRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ChildRowSchema,
    execute: () =>
      sql`
        SELECT
          thread_id AS "threadId",
          parent_thread_id AS "parentThreadId"
        FROM projection_threads
        WHERE parent_thread_id IS NOT NULL
          AND parent_environment_id IS NULL
      `,
  });

  const listWaitDeliveryRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: WaitDeliveryRowSchema,
    execute: () =>
      sql`
        SELECT
          child_thread_id AS "childThreadId",
          parent_thread_id AS "parentThreadId",
          delivered_at AS "deliveredAt",
          parent_turn_id_at_delivery AS "parentTurnIdAtDelivery"
        FROM subagent_wait_deliveries
      `,
  });

  const listPromotedChildRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: PromotedChildRowSchema,
    execute: () =>
      sql`
        SELECT
          child_thread_id AS "childThreadId",
          parent_thread_id AS "parentThreadId"
        FROM subagent_promoted_children
      `,
  });

  const upsertWaitDeliveryRow = SqlSchema.void({
    Request: Schema.Struct({
      childThreadId: Schema.String,
      parentThreadId: Schema.String,
      deliveredAt: Schema.String,
      parentTurnIdAtDelivery: Schema.NullOr(Schema.String),
    }),
    execute: ({ childThreadId, parentThreadId, deliveredAt, parentTurnIdAtDelivery }) =>
      sql`
        INSERT INTO subagent_wait_deliveries (
          child_thread_id,
          parent_thread_id,
          delivered_at,
          parent_turn_id_at_delivery
        )
        VALUES (
          ${childThreadId},
          ${parentThreadId},
          ${deliveredAt},
          ${parentTurnIdAtDelivery}
        )
        ON CONFLICT (child_thread_id)
        DO NOTHING
      `,
  });

  const upsertPromotedChildRow = SqlSchema.void({
    Request: Schema.Struct({
      childThreadId: Schema.String,
      parentThreadId: Schema.String,
      promotedAt: Schema.String,
    }),
    execute: ({ childThreadId, parentThreadId, promotedAt }) =>
      sql`
        INSERT INTO subagent_promoted_children (
          child_thread_id,
          parent_thread_id,
          promoted_at
        )
        VALUES (
          ${childThreadId},
          ${parentThreadId},
          ${promotedAt}
        )
        ON CONFLICT (child_thread_id)
        DO UPDATE SET
          parent_thread_id = excluded.parent_thread_id,
          promoted_at = excluded.promoted_at
      `,
  });

  const deletePromotedChildRows = (childThreadIds: ReadonlyArray<ThreadId>) =>
    childThreadIds.length === 0
      ? Effect.void
      : sql`
          DELETE FROM subagent_promoted_children
          WHERE ${sql.in("child_thread_id", childThreadIds)}
        `.pipe(Effect.asVoid);

  const deleteWaitDeliveryRows = (childThreadIds: ReadonlyArray<ThreadId>) =>
    childThreadIds.length === 0
      ? Effect.void
      : sql`
          DELETE FROM subagent_wait_deliveries
          WHERE ${sql.in("child_thread_id", childThreadIds)}
        `.pipe(Effect.asVoid);

  const wakeLockFor = (parentThreadId: ThreadId): Effect.Effect<Semaphore.Semaphore> => {
    const existing = parentWakeLocks.get(parentThreadId);
    if (existing) return Effect.succeed(existing);
    return Semaphore.make(1).pipe(
      Effect.tap((semaphore) => Effect.sync(() => parentWakeLocks.set(parentThreadId, semaphore))),
    );
  };

  const trackChild = (childThreadId: ThreadId, record: ChildRecord) => {
    children.set(childThreadId, record);
    const siblings = byParent.get(record.parentThreadId) ?? new Set<ThreadId>();
    siblings.add(childThreadId);
    byParent.set(record.parentThreadId, siblings);
  };

  const enqueuePending = (parentThreadId: ThreadId, entry: PendingInjection) => {
    const queue = pendingInjections.get(parentThreadId) ?? [];
    queue.push(entry);
    pendingInjections.set(parentThreadId, queue);
  };

  // R-B: persist a durable 'parent_injection' row so a wake survives restart.
  // The returned in-memory entry carries the row id; the row is deleted on
  // successful drain/dispatch (delete-on-dispatch => idempotent, no double-fire).
  const persistInjection = (
    parentThreadId: ThreadId,
    result: ChildWaitResult & Pick<EnqueueParentInjectionInput, "dedupeKey">,
    deliveredByWait: boolean,
    waitCancellable: boolean,
  ) =>
    Effect.gen(function* () {
      const now = yield* nowMillis;
      const createdAt = yield* nowIso;
      const id =
        result.dedupeKey === undefined
          ? ((yield* randomUUID) as PendingDispatchId)
          : (result.dedupeKey as PendingDispatchId);
      const claimedCommandId =
        result.dedupeKey === undefined ? null : dispatchCommandIdFor("subagent-wake", id);
      const row: PendingDispatch = {
        id,
        kind: "parent_injection",
        targetThreadId: parentThreadId,
        sourceChildId: result.childThreadId,
        text: result.finalAssistantText,
        error: result.error,
        status: result.status,
        commandId: claimedCommandId,
        deliveredByWait,
        waitCancellable,
        createdAt: IsoDateTime.make(createdAt),
      };
      yield* pendingDispatches.insert(row).pipe(Effect.orDie);
      queuedWakeChildren.add(result.childThreadId);
      return {
        childThreadId: result.childThreadId,
        status: result.status,
        text: result.finalAssistantText,
        error: result.error,
        enqueuedAtMs: now,
        dispatchId: id,
        deliveredByWait,
        waitCancellable,
        claimedCommandId,
      } satisfies PendingInjection;
    });

  const deleteDispatchRows = (ids: ReadonlyArray<PendingDispatchId>) =>
    pendingDispatches.deleteByIds(ids).pipe(Effect.orDie);

  const promotedRowsToDeleteFor = (entries: ReadonlyArray<PendingInjection>): Array<ThreadId> => {
    const promotedRowsToDelete: Array<ThreadId> = [];
    for (const entry of entries) {
      const deliveredByWait =
        entry.deliveredByWait || waitDeliveredPromotedChildren.has(entry.childThreadId);
      if (entry.waitCancellable || deliveredByWait) {
        promotedRowsToDelete.push(entry.childThreadId);
      }
    }
    return promotedRowsToDelete;
  };

  const clearDeletedWakeStateMemory = (entries: ReadonlyArray<PendingInjection>) =>
    Effect.sync(() => {
      for (const entry of entries) {
        const deliveredByWait =
          entry.deliveredByWait || waitDeliveredPromotedChildren.has(entry.childThreadId);
        queuedWakeChildren.delete(entry.childThreadId);
        if (entry.waitCancellable || deliveredByWait) {
          waitDeliveredPromotedChildren.delete(entry.childThreadId);
          promotedChildren.delete(entry.childThreadId);
          activePromotedWaitChildren.delete(entry.childThreadId);
        }
        if (deliveredByWait) {
          waitDeliveryMarkedAt.delete(entry.childThreadId);
          waitDeliveryParentTurnAt.delete(entry.childThreadId);
        }
      }
    });

  const clearWaitDeliveryStateMemory = (childThreadIds: ReadonlyArray<ThreadId>) =>
    Effect.sync(() => {
      for (const childThreadId of childThreadIds) {
        waitDeliveredPromotedChildren.delete(childThreadId);
        waitDeliveryMarkedAt.delete(childThreadId);
        waitDeliveryParentTurnAt.delete(childThreadId);
        promotedChildren.delete(childThreadId);
        activePromotedWaitChildren.delete(childThreadId);
      }
    });

  const deleteDispatchRowsAndClearWakeState = (entries: ReadonlyArray<PendingInjection>) => {
    const ids = entries.map((entry) => entry.dispatchId);
    const promotedRowsToDelete = promotedRowsToDeleteFor(entries);
    return sql
      .withTransaction(
        Effect.gen(function* () {
          yield* deleteDispatchRows(ids);
          yield* deletePromotedChildRows(promotedRowsToDelete).pipe(Effect.orDie);
        }),
      )
      .pipe(Effect.orDie, Effect.andThen(clearDeletedWakeStateMemory(entries)));
  };

  const persistPromotedChild = (parentThreadId: ThreadId, childThreadId: ThreadId) =>
    Effect.gen(function* () {
      const promotedAt = yield* nowIso;
      yield* upsertPromotedChildRow({
        childThreadId,
        parentThreadId,
        promotedAt,
      });
    }).pipe(Effect.orDie);

  const rollbackPromotedChild = (childThreadId: ThreadId) =>
    Effect.sync(() => {
      if (queuedWakeChildren.has(childThreadId)) return;
      promotedChildren.delete(childThreadId);
      activePromotedWaitChildren.delete(childThreadId);
    });

  const ensurePromotedChild = (
    record: ChildRecord,
    childThreadId: ThreadId,
  ): Effect.Effect<boolean> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const inFlight = promotingChildren.get(childThreadId);
        if (inFlight !== undefined) {
          yield* restore(Deferred.await(inFlight));
          if (promotedChildren.has(childThreadId)) return false;
          return yield* ensurePromotedChild(record, childThreadId);
        }
        if (promotedChildren.has(childThreadId)) return false;
        const completed = yield* Deferred.make<void>();
        promotingChildren.set(childThreadId, completed);
        promotedChildren.add(childThreadId);
        return yield* persistPromotedChild(record.parentThreadId, childThreadId).pipe(
          Effect.as(true),
          Effect.catchCause((cause) =>
            rollbackPromotedChild(childThreadId).pipe(Effect.andThen(Effect.failCause(cause))),
          ),
          Effect.ensuring(
            Effect.sync(() => {
              promotingChildren.delete(childThreadId);
            }).pipe(Effect.andThen(Deferred.succeed(completed, undefined).pipe(Effect.orDie))),
          ),
        );
      }),
    );

  const recordWaitDelivery = (parentThreadId: ThreadId, childThreadId: ThreadId) =>
    Effect.gen(function* () {
      const existing = waitDeliveryMarkedAt.get(childThreadId);
      if (existing !== undefined) return existing;
      const shellOption = yield* getThreadShellBounded(parentThreadId);
      const latestTurn = Option.isSome(shellOption) ? shellOption.value.latestTurn : null;
      const parentTurnIdAtDelivery =
        latestTurn?.state === "running" ? String(latestTurn.turnId) : null;
      const deliveredAt = yield* nowIso;
      yield* upsertWaitDeliveryRow({
        childThreadId,
        parentThreadId,
        deliveredAt,
        parentTurnIdAtDelivery,
      });
      waitDeliveryMarkedAt.set(childThreadId, deliveredAt);
      if (parentTurnIdAtDelivery !== null) {
        waitDeliveryParentTurnAt.set(childThreadId, parentTurnIdAtDelivery as TurnId);
      }
      return deliveredAt;
    }).pipe(Effect.orDie);

  const commitWaitDelivery = (
    parentThreadId: ThreadId,
    childThreadId: ThreadId,
    dispatchIds: ReadonlyArray<PendingDispatchId>,
    expectedParentTurnId: TurnId | null,
  ): Effect.Effect<"committed" | "refused-drain" | "refused-retain"> =>
    Effect.gen(function* () {
      const existing = waitDeliveryMarkedAt.get(childThreadId);
      const shellOption =
        existing === undefined ? yield* getThreadShellBounded(parentThreadId) : Option.none();
      const shell = Option.isSome(shellOption) ? shellOption.value : null;
      const latestTurn = Option.isSome(shellOption) ? shellOption.value.latestTurn : null;
      if (existing === undefined && expectedParentTurnId !== null) {
        const sameRunningTurn =
          latestTurn?.state === "running" && latestTurn.turnId === expectedParentTurnId;
        if (!sameRunningTurn) {
          return shell !== null && isThreadIdle(shell) ? "refused-drain" : "refused-retain";
        }
      }
      if (
        existing === undefined &&
        expectedParentTurnId === null &&
        (latestTurn?.state === "error" || latestTurn?.state === "interrupted")
      ) {
        return shell !== null && isThreadIdle(shell) ? "refused-drain" : "refused-retain";
      }
      const parentTurnIdAtDelivery =
        expectedParentTurnId ??
        (latestTurn?.state === "running" ? String(latestTurn.turnId) : null);
      const deliveredAt = existing ?? (yield* nowIso);

      yield* sql.withTransaction(
        Effect.gen(function* () {
          if (existing === undefined) {
            yield* upsertWaitDeliveryRow({
              childThreadId,
              parentThreadId,
              deliveredAt,
              parentTurnIdAtDelivery,
            });
          }
          yield* pendingDispatches.markWaitDelivered({ ids: dispatchIds }).pipe(Effect.orDie);
        }),
      );

      if (existing === undefined) {
        waitDeliveryMarkedAt.set(childThreadId, deliveredAt);
        if (parentTurnIdAtDelivery !== null) {
          waitDeliveryParentTurnAt.set(childThreadId, parentTurnIdAtDelivery as TurnId);
        }
      }
      return "committed";
    }).pipe(Effect.orDie);

  const parentTerminalAfterWaitDelivery = (
    shell: OrchestrationThreadShell,
    childThreadId: ThreadId,
    includeSameMillisecond: boolean,
  ): OrchestrationLatestTurn | null => {
    const latestTurn = shell.latestTurn;
    if (latestTurn === null || latestTurn.state === "running") return null;
    const waitDeliveredAtMs = parseIsoMillis(waitDeliveryMarkedAt.get(childThreadId));
    const parentSettledAtMs = parseIsoMillis(latestTurn.completedAt);
    if (waitDeliveredAtMs === null || parentSettledAtMs === null) return null;
    const sameTurnAsDelivery = waitDeliveryParentTurnAt.get(childThreadId) === latestTurn.turnId;
    return parentSettledAtMs > waitDeliveredAtMs ||
      (parentSettledAtMs === waitDeliveredAtMs && (includeSameMillisecond || sameTurnAsDelivery))
      ? latestTurn
      : null;
  };

  const parentCompletedAfterWaitDelivery = (
    shell: OrchestrationThreadShell,
    childThreadId: ThreadId,
  ): boolean => parentTerminalAfterWaitDelivery(shell, childThreadId, false)?.state === "completed";

  const parentFailedAfterWaitDelivery = (
    shell: OrchestrationThreadShell,
    childThreadId: ThreadId,
  ): boolean => {
    const turn = parentTerminalAfterWaitDelivery(shell, childThreadId, true);
    return turn?.state === "error" || turn?.state === "interrupted";
  };

  const ensurePromotedWaitFallback = (
    record: ChildRecord,
    childThreadId: ThreadId,
    result: ChildWaitResult,
  ) =>
    Effect.gen(function* () {
      if (record.detached || !promotedChildren.has(childThreadId)) return;
      const lock = yield* wakeLockFor(record.parentThreadId);
      yield* lock.withPermits(1)(
        Effect.gen(function* () {
          if (!promotedChildren.has(childThreadId)) return;
          const queue = pendingInjections.get(record.parentThreadId) ?? [];
          if (queue.some((entry) => entry.childThreadId === childThreadId)) return;
          const entry = yield* persistInjection(record.parentThreadId, result, false, true);
          enqueuePending(record.parentThreadId, entry);
        }),
      );
    });

  const childWaitResultFromMark = (mark: WaitDeliveredMark): ChildWaitResult | undefined => {
    if (typeof mark === "string") return undefined;
    if (mark.status === "pending" || mark.status === "timeout") return undefined;
    return {
      childThreadId: mark.childThreadId,
      status: mark.status,
      finalAssistantText: mark.finalAssistantText,
      error: mark.error,
    };
  };
  const parentTurnIdAtWaitFromMark = (mark: WaitDeliveredMark): TurnId | null =>
    typeof mark === "string" ? null : (mark.parentTurnIdAtWait ?? null);

  const markPromotedWakeDeliveredByWait = (
    record: ChildRecord,
    childThreadId: ThreadId,
    expectedParentTurnId: TurnId | null,
    deliveredResult?: ChildWaitResult,
  ) =>
    Effect.gen(function* () {
      if (record.detached || !promotedChildren.has(childThreadId)) return;
      let drainAfterRefusedDelivery = false;
      let terminalResult = deliveredResult ?? null;
      if (terminalResult === null) {
        const completed = yield* Deferred.poll(record.terminal);
        if (Option.isSome(completed)) {
          terminalResult = yield* completed.value;
        }
      }
      const lock = yield* wakeLockFor(record.parentThreadId);
      yield* lock.withPermits(1)(
        Effect.gen(function* () {
          if (!promotedChildren.has(childThreadId)) return;
          activePromotedWaitChildren.delete(childThreadId);
          let queue = pendingInjections.get(record.parentThreadId) ?? [];
          const deliveredIds: Array<PendingDispatchId> = [];
          let updated = queue;
          for (const entry of queue) {
            if (entry.childThreadId === childThreadId) {
              deliveredIds.push(entry.dispatchId);
            }
          }
          if (terminalResult !== null && deliveredIds.length === 0) {
            const entry = yield* persistInjection(
              record.parentThreadId,
              terminalResult,
              false,
              true,
            );
            updated = [...updated, entry];
            pendingInjections.set(record.parentThreadId, updated);
            deliveredIds.push(entry.dispatchId);
          }
          if (deliveredIds.length > 0) {
            const committed = yield* commitWaitDelivery(
              record.parentThreadId,
              childThreadId,
              deliveredIds,
              expectedParentTurnId,
            );
            if (committed === "committed") {
              waitDeliveredPromotedChildren.add(childThreadId);
              updated = updated.map((entry) =>
                entry.childThreadId === childThreadId ? { ...entry, deliveredByWait: true } : entry,
              );
            } else if (committed === "refused-drain") {
              drainAfterRefusedDelivery = true;
            }
          }
          if (updated.length === 0) {
            return;
          }
          pendingInjections.set(record.parentThreadId, updated);
        }),
      );
      if (drainAfterRefusedDelivery) {
        yield* drainPending(record.parentThreadId);
      }
    });

  // Durably stamp the commandId a batch is about to be dispatched under, BEFORE
  // the dispatch, so a crash mid-dispatch leaves a row that re-fires under the
  // SAME id on restart (engine dedup => exactly-once).
  const claimDispatchRows = (ids: ReadonlyArray<PendingDispatchId>, commandId: CommandId) =>
    pendingDispatches.claim({ ids, commandId }).pipe(Effect.orDie);

  const depthFor = (parentThreadId: ThreadId): number => {
    const parentRecord = children.get(parentThreadId);
    return parentRecord ? parentRecord.depth + 1 : 1;
  };

  // Walk the ancestry chain via recorded parent links; a repeated id is a cycle.
  const hasAncestryCycle = (parentThreadId: ThreadId, childThreadId: ThreadId): boolean => {
    if (parentThreadId === childThreadId) return true;
    const seen = new Set<ThreadId>([childThreadId]);
    let cursor: ThreadId | undefined = parentThreadId;
    while (cursor !== undefined) {
      if (seen.has(cursor)) return true;
      seen.add(cursor);
      cursor = children.get(cursor)?.parentThreadId;
    }
    return false;
  };

  const getThreadShell = (threadId: ThreadId) =>
    projectionSnapshotQuery.getThreadShellById(threadId).pipe(Effect.orDie);

  const getThreadDetail = (threadId: ThreadId) =>
    projectionSnapshotQuery.getThreadDetailById(threadId).pipe(Effect.orDie);

  // Bounded projection read for synchronous request/startup paths. A stalled
  // projection must never block past the slice timeout, so a read that exceeds
  // PROJECTION_READ_TIMEOUT_MS resolves to Option.none for request callers
  // (treated as "not yet terminal"; the caller falls through to the bounded
  // Deferred race).
  const getThreadShellBounded = (threadId: ThreadId) =>
    getThreadShell(threadId).pipe(
      Effect.timeoutOption(`${PROJECTION_READ_TIMEOUT_MS} millis`),
      Effect.map(Option.flatten),
    );

  const runningParentTurnIdForWait = (parentThreadId: ThreadId): Effect.Effect<TurnId | null> =>
    getThreadShellBounded(parentThreadId).pipe(
      Effect.map((shellOption) => {
        if (Option.isNone(shellOption)) return null;
        const latestTurn = shellOption.value.latestTurn;
        return latestTurn?.state === "running" ? latestTurn.turnId : null;
      }),
    );

  const getThreadShellForDrain = (threadId: ThreadId) =>
    getThreadShell(threadId).pipe(Effect.timeoutOption(`${PROJECTION_READ_TIMEOUT_MS} millis`));

  const getThreadDetailBounded = (threadId: ThreadId) =>
    getThreadDetail(threadId).pipe(
      Effect.timeoutOption(`${PROJECTION_READ_TIMEOUT_MS} millis`),
      Effect.map(Option.flatten),
    );

  const currentProjectedTerminal = (
    shell: OrchestrationThreadShell,
    guards: {
      readonly activeTurnId: TurnId | undefined;
      readonly pendingTurnStart: boolean;
      readonly pendingSameTurnId: TurnId | undefined;
    },
  ): OrchestrationLatestTurn | null => {
    const latestTurn = shell.latestTurn;
    if (latestTurn === null || latestTurn.state === "running") return null;
    if (shell.session?.status === "error") return null;
    if (shell.session?.status === "stopped" && shell.latestUserMessageAt !== null) {
      const terminalAt = latestTurn.completedAt ?? latestTurn.startedAt ?? latestTurn.requestedAt;
      if (shell.latestUserMessageAt > terminalAt) return null;
    }
    if (
      guards.pendingTurnStart &&
      (guards.pendingSameTurnId === undefined ||
        guards.pendingSameTurnId !== latestTurn.turnId ||
        shell.session?.status !== "stopped")
    ) {
      return null;
    }
    if (guards.activeTurnId !== undefined && guards.activeTurnId !== latestTurn.turnId) {
      return null;
    }
    if (shell.session?.activeTurnId != null && shell.session.activeTurnId !== latestTurn.turnId) {
      return null;
    }
    return latestTurn;
  };

  const currentLiveProjectedTerminal = (
    childThreadId: ThreadId,
    shell: OrchestrationThreadShell,
  ): OrchestrationLatestTurn | null =>
    currentProjectedTerminal(shell, {
      activeTurnId: activeTurnByChild.get(childThreadId),
      pendingTurnStart: pendingTurnStartByChild.has(childThreadId),
      pendingSameTurnId: pendingSameTurnStartByChild.get(childThreadId),
    });

  const shellTerminalOutcome = (
    childThreadId: ThreadId,
    shell: OrchestrationThreadShell,
  ): ChildTerminalOutcome | null => {
    const terminalTurn = currentLiveProjectedTerminal(childThreadId, shell);
    if (terminalTurn === null) return null;
    return turnTerminalOutcome(terminalTurn, shell.session);
  };

  const completeChild = (
    childThreadId: ThreadId,
    status: ChildTerminalStatus,
    finalAssistantText: string | null,
    error: string | null,
  ) =>
    Effect.gen(function* () {
      const record = children.get(childThreadId);
      if (!record) return;
      const settled = yield* Deferred.succeed(record.terminal, {
        childThreadId,
        status,
        finalAssistantText,
        error,
      });
      if (settled) {
        yield* dispatchLimiter.releaseForChild(childThreadId);
        activeTurnByChild.delete(childThreadId);
        pendingTurnStartByChild.delete(childThreadId);
        pendingSameTurnStartByChild.delete(childThreadId);
        pendingStoppedSettlementByChild.delete(childThreadId);
      }
      // A detached child always wakes its parent; a promoted child (R-A: a waited
      // child whose waiter stopped) must wake too, satisfying the notify-guarantee
      // that no child completes with neither an active waiter nor a wake.
      if (
        settled &&
        (record.detached || promotedChildren.has(childThreadId)) &&
        !queuedWakeChildren.has(childThreadId)
      ) {
        yield* wakeParent(record, { childThreadId, status, finalAssistantText, error });
      }
    });

  // Settle a child terminal Deferred exactly once. Deferred.succeed is a no-op
  // when already settled, which makes every signal path idempotent.
  const settleChild = (
    childThreadId: ThreadId,
    status: ChildTerminalStatus,
    error: string | null,
    bounded = false,
  ) =>
    Effect.gen(function* () {
      const detail = yield* (bounded ? getThreadDetailBounded : getThreadDetail)(childThreadId);
      const finalAssistantText = Option.match(detail, {
        onNone: () => null,
        onSome: finalAssistantTextFromThread,
      });
      yield* completeChild(childThreadId, status, finalAssistantText, error);
    });

  const consolidatedInjectionText = (entries: ReadonlyArray<PendingInjection>): string => {
    const joined = entries
      .map(
        (entry) =>
          `[sub-agent ${entry.childThreadId} ${entry.status}] ${entry.error ?? entry.text ?? ""}`,
      )
      .join("\n");
    // Guard against unbounded growth when many children settle with large
    // payloads; the full per-child results remain queryable via t3_check.
    if (joined.length > CONSOLIDATED_INJECTION_MAX_CHARS) {
      return `${joined.slice(0, CONSOLIDATED_INJECTION_MAX_CHARS)}\n[...${entries.length} sub-agent results truncated; use t3_subagents with childThreadId for full output]`;
    }
    return joined;
  };

  // The commandId is derived from the backing dispatch row id so a re-fire after
  // a crash-between-dispatch-and-delete is deduped by the engine (exactly-once).
  const dispatchParentTurn = (
    shell: OrchestrationThreadShell,
    text: string,
    commandId: CommandId,
  ) =>
    Effect.gen(function* () {
      const messageId = MessageId.make(yield* randomUUID);
      const createdAt = yield* nowIso;
      yield* dispatchActive({
        type: "thread.turn.start",
        commandId,
        threadId: shell.id,
        message: { messageId, role: "system", text, attachments: [] },
        runtimeMode: shell.runtimeMode,
        interactionMode: shell.interactionMode,
        createdAt,
      });
    });

  // Dispatch a deferred steer to a now-idle child as a fresh user turn (R-C).
  // The commandId is derived from the backing row id for the same reason.
  const dispatchChildSteer = (
    shell: OrchestrationThreadShell,
    text: string,
    commandId: CommandId,
  ) =>
    Effect.gen(function* () {
      const messageId = MessageId.make(yield* randomUUID);
      const createdAt = yield* nowIso;
      yield* dispatchActive({
        type: "thread.turn.start",
        commandId,
        threadId: shell.id,
        message: { messageId, role: "user", text, attachments: [] },
        runtimeMode: shell.runtimeMode,
        interactionMode: shell.interactionMode,
        createdAt,
      });
    });

  const appendSubagentActivity = (parentThreadId: ThreadId, result: ChildWaitResult) =>
    Effect.gen(function* () {
      const commandId = yield* newCommandId("subagent-activity");
      const activityId = EventId.make(yield* randomUUID);
      const createdAt = yield* nowIso;
      yield* orchestrationEngine.dispatch({
        type: "thread.activity.append",
        commandId,
        threadId: parentThreadId,
        activity: {
          id: activityId,
          tone: result.status === "completed" ? "info" : "error",
          kind: "subagent.completed",
          summary: `Sub-agent ${result.childThreadId} ${result.status}`,
          payload: {
            childThreadId: result.childThreadId,
            status: result.status,
            error: result.error,
          },
          turnId: null,
          createdAt,
        },
        createdAt,
      });
    });

  // Atomic per-parent idle-check + dispatch: never resume a turnCount-0 parent,
  // resume an idle parent with the consolidated text, otherwise enqueue.
  //
  // INVARIANT (non-reentrant lock): the parent-wake Semaphore is NOT reentrant.
  // `dispatchActive`/`orchestrationEngine.dispatch` MUST be asynchronous — the
  // resulting parent turn-diff-completed event is published on the hot stream
  // and processed on the worker fiber, so `drainPending` (which acquires the
  // same per-parent lock) can only run AFTER this lock is released. If dispatch
  // were ever made synchronous and re-entered drainPending for this parent
  // inline, it would deadlock; the WakeParentSyncDispatch regression test guards
  // this by enqueuing the parent's terminal signal during dispatch and asserting
  // wakeParent still completes.
  const wakeParent = (
    record: Pick<ChildRecord, "parentThreadId" | "detached">,
    result: ChildWaitResult & Pick<EnqueueParentInjectionInput, "dedupeKey">,
  ) =>
    Effect.gen(function* () {
      const parentThreadId = record.parentThreadId;
      const lock = yield* wakeLockFor(parentThreadId);
      yield* lock.withPermits(1)(
        Effect.gen(function* () {
          const activeWait = hasActivePromotedWait(result.childThreadId);
          let shellOption: Option.Option<OrchestrationThreadShell> | null = null;
          if (!record.detached && waitDeliveredPromotedChildren.has(result.childThreadId)) {
            shellOption = yield* getThreadShell(parentThreadId);
            if (
              Option.isSome(shellOption) &&
              isThreadIdle(shellOption.value) &&
              parentCompletedAfterWaitDelivery(shellOption.value, result.childThreadId)
            ) {
              yield* recordWaitDelivery(parentThreadId, result.childThreadId);
              waitDeliveredPromotedChildren.delete(result.childThreadId);
              promotedChildren.delete(result.childThreadId);
              activePromotedWaitChildren.delete(result.childThreadId);
              waitDeliveryMarkedAt.delete(result.childThreadId);
              waitDeliveryParentTurnAt.delete(result.childThreadId);
              yield* deletePromotedChildRows([result.childThreadId]).pipe(Effect.orDie);
              return;
            }
          }
          // Persist the durable row up front so the wake survives a restart in
          // every branch below; it is deleted only after a successful dispatch.
          const existingQueue = pendingInjections.get(parentThreadId) ?? [];
          if (
            queuedWakeChildren.has(result.childThreadId) ||
            existingQueue.some((entry) => entry.childThreadId === result.childThreadId)
          ) {
            return;
          }
          const deliveredByWait =
            !record.detached && waitDeliveredPromotedChildren.has(result.childThreadId);
          const waitCancellable = !record.detached;
          const entry = yield* persistInjection(
            parentThreadId,
            result,
            deliveredByWait,
            waitCancellable,
          );
          if (shellOption === null) {
            shellOption = yield* getThreadShell(parentThreadId);
          }
          if (Option.isNone(shellOption)) {
            enqueuePending(parentThreadId, entry);
            yield* Effect.logWarning("subagent wake parent not found; enqueued injection", {
              parentThreadId,
              childThreadId: result.childThreadId,
            });
            return;
          }
          const shell = shellOption.value;
          if (isFreshParent(shell)) {
            enqueuePending(parentThreadId, entry);
            return;
          }
          if (isThreadIdle(shell)) {
            if (activeWait) {
              enqueuePending(parentThreadId, entry);
              return;
            }
            if (deliveredByWait && !parentFailedAfterWaitDelivery(shell, result.childThreadId)) {
              enqueuePending(parentThreadId, entry);
              return;
            }
            const commandId =
              entry.claimedCommandId ?? batchCommandIdFor("subagent-wake", [entry.dispatchId]);
            yield* claimDispatchRows([entry.dispatchId], commandId).pipe(
              Effect.andThen(
                dispatchParentTurn(shell, consolidatedInjectionText([entry]), commandId),
              ),
              Effect.andThen(deleteDispatchRowsAndClearWakeState([entry])),
              Effect.catchCause((cause) => {
                enqueuePending(parentThreadId, entry);
                return Effect.logWarning("subagent wake dispatch failed; enqueued injection", {
                  parentThreadId,
                  childThreadId: result.childThreadId,
                  cause: Cause.pretty(cause),
                });
              }),
            );
            return;
          }
          yield* appendSubagentActivity(parentThreadId, result).pipe(
            Effect.ignoreCause({ log: true }),
          );
          enqueuePending(parentThreadId, entry);
        }),
      );
    });

  // Drain pending injections for a parent that just completed a turn (or whose
  // oldest entry has aged past PENDING_MAX_AGE_MS). One consolidated turn.
  const drainPending = (parentThreadId: ThreadId, options?: { readonly requireIdle?: boolean }) =>
    Effect.gen(function* () {
      const lock = yield* wakeLockFor(parentThreadId);
      yield* lock.withPermits(1)(
        Effect.gen(function* () {
          const queue = pendingInjections.get(parentThreadId);
          if (!queue || queue.length === 0) {
            pendingDrainRetryParents.delete(parentThreadId);
            pendingIdleDrainRetryParents.delete(parentThreadId);
            return;
          }
          const shellRead = yield* getThreadShellForDrain(parentThreadId);
          if (Option.isNone(shellRead)) {
            if (options?.requireIdle === true) {
              pendingIdleDrainRetryParents.add(parentThreadId);
            } else {
              pendingDrainRetryParents.add(parentThreadId);
            }
            yield* Effect.logWarning(
              "parent shell read timed out while draining pending injections",
              {
                parentThreadId,
                pendingCount: queue.length,
              },
            );
            return;
          }
          const shellOption = shellRead.value;
          if (Option.isNone(shellOption)) {
            // Parent no longer exists (deleted): it can never become idle, so an
            // orphaned durable row would reload forever. Drop the in-memory queue
            // AND delete the backing rows so a restart never re-loads them.
            pendingInjections.delete(parentThreadId);
            yield* deleteDispatchRowsAndClearWakeState(queue);
            yield* Effect.logWarning(
              "parent gone while draining pending injections; dropped orphaned rows",
              {
                parentThreadId,
                droppedCount: queue.length,
              },
            );
            return;
          }
          const shell = shellOption.value;
          if (isFreshParent(shell)) {
            yield* Effect.logWarning(
              "parent became fresh while draining pending injections; deferring",
              { parentThreadId, pendingCount: queue.length },
            );
            return;
          }
          const isDeliveredByWait = (entry: PendingInjection) =>
            entry.deliveredByWait || waitDeliveredPromotedChildren.has(entry.childThreadId);
          const deliveredByWaitEntries = queue.filter((entry) => isDeliveredByWait(entry));
          const idle = isThreadIdle(shell);
          if (options?.requireIdle === true && !idle) return;
          const prunedDeliveredByWaitEntries = idle
            ? deliveredByWaitEntries.filter((entry) =>
                parentCompletedAfterWaitDelivery(shell, entry.childThreadId),
              )
            : [];
          const dispatchedDeliveredByWaitEntries = idle
            ? deliveredByWaitEntries.filter((entry) =>
                parentFailedAfterWaitDelivery(shell, entry.childThreadId),
              )
            : [];
          const retainedDeliveredByWaitEntries = deliveredByWaitEntries.filter(
            (entry) =>
              !prunedDeliveredByWaitEntries.includes(entry) &&
              !dispatchedDeliveredByWaitEntries.includes(entry),
          );
          const entries = [
            ...queue.filter((entry) => !isDeliveredByWait(entry)),
            ...dispatchedDeliveredByWaitEntries,
          ];
          if (prunedDeliveredByWaitEntries.length > 0) {
            for (const entry of prunedDeliveredByWaitEntries) {
              yield* recordWaitDelivery(parentThreadId, entry.childThreadId);
            }
            yield* deleteDispatchRowsAndClearWakeState(prunedDeliveredByWaitEntries);
          }
          if (retainedDeliveredByWaitEntries.length > 0) {
            pendingInjections.set(parentThreadId, retainedDeliveredByWaitEntries);
          } else {
            pendingInjections.delete(parentThreadId);
          }
          if (entries.length === 0) return;

          // Drain one batch as a single turn, restoring its entries on a transient
          // dispatch failure so the never-hang retry contract holds. The rows are
          // claimed under the dispatch commandId BEFORE dispatch and deleted after,
          // all inside this per-parent lock (a restart re-fires under the same id).
          const drainBatch = (batch: ReadonlyArray<PendingInjection>, commandId: CommandId) => {
            if (batch.length === 0) return Effect.void;
            const ids = batch.map((entry) => entry.dispatchId);
            return claimDispatchRows(ids, commandId).pipe(
              Effect.andThen(
                dispatchParentTurn(shell, consolidatedInjectionText(batch), commandId),
              ),
              Effect.andThen(deleteDispatchRowsAndClearWakeState(batch)),
              Effect.catchCause((cause) => {
                const restored = pendingInjections.get(parentThreadId) ?? [];
                pendingInjections.set(parentThreadId, [...batch, ...restored]);
                return Effect.logWarning("subagent pending drain dispatch failed; re-enqueued", {
                  parentThreadId,
                  cause: Cause.pretty(cause),
                });
              }),
            );
          };

          // A claimed entry (its turn was already dispatched under a fixed id
          // before a crash) MUST be re-dispatched alone under that exact id so the
          // engine dedups a landed turn — it can never be folded into a fresh
          // consolidated batch (which the engine has no receipt for). Unclaimed
          // entries consolidate into one turn under a fresh deterministic id.
          const claimed = entries.filter((entry) => entry.claimedCommandId !== null);
          const fresh = entries.filter((entry) => entry.claimedCommandId === null);
          for (const entry of claimed) {
            yield* drainBatch([entry], entry.claimedCommandId as CommandId);
          }
          yield* drainBatch(
            fresh,
            batchCommandIdFor(
              "subagent-wake",
              fresh.map((entry) => entry.dispatchId),
            ),
          );
        }),
      );
    });

  const drainPendingWhenParentIdle = (parentThreadId: ThreadId) =>
    drainPending(parentThreadId, { requireIdle: true });

  // Safety valve: flush any parent whose oldest pending entry has aged out, even
  // if that parent never completes another turn.
  const drainAgedPending = Effect.gen(function* () {
    const now = yield* nowMillis;
    const parents: Array<ThreadId> = [];
    for (const [parentThreadId, queue] of pendingInjections) {
      if (queue.some((entry) => now - entry.enqueuedAtMs >= PENDING_MAX_AGE_MS)) {
        parents.push(parentThreadId);
      }
    }
    yield* Effect.forEach(parents, (parentThreadId) => drainPending(parentThreadId), {
      discard: true,
    });
  });

  const drainRetriedPending = Effect.gen(function* () {
    const parents = Array.from(pendingDrainRetryParents);
    const idleParents = Array.from(pendingIdleDrainRetryParents);
    pendingDrainRetryParents.clear();
    pendingIdleDrainRetryParents.clear();
    yield* Effect.forEach(parents, (parentThreadId) => drainPending(parentThreadId), {
      discard: true,
    });
    yield* Effect.forEach(idleParents, drainPendingWhenParentIdle, { discard: true });
  });

  const childSteerLockFor = (childThreadId: ThreadId): Effect.Effect<Semaphore.Semaphore> => {
    const existing = childSteerLocks.get(childThreadId);
    if (existing) return Effect.succeed(existing);
    return Semaphore.make(1).pipe(
      Effect.tap((semaphore) => Effect.sync(() => childSteerLocks.set(childThreadId, semaphore))),
    );
  };

  // R-C: a child that just went idle drains any provider-deferred 'child_steer'
  // rows in created_at order, dispatching thread.turn.start(child, text) under a
  // per-child guard and deleting each row on dispatch (exactly-once).
  const drainChildSteers = (childThreadId: ThreadId) =>
    Effect.gen(function* () {
      const lock = yield* childSteerLockFor(childThreadId);
      yield* lock.withPermits(1)(
        Effect.gen(function* () {
          const rows = yield* pendingDispatches
            .listByTarget({ kind: "child_steer", targetThreadId: childThreadId })
            .pipe(Effect.orDie);
          if (rows.length === 0) return;
          const shellOption = yield* getThreadShell(childThreadId);
          if (Option.isNone(shellOption)) return;
          const shell = shellOption.value;
          // Re-check idleness under the lock: a turn may have restarted; if so,
          // leave the rows for the next idle transition (still exactly-once).
          if (!isThreadIdle(shell)) return;
          for (const row of rows) {
            // commandId is claimed durably before dispatch and re-used verbatim on
            // restart (row.commandId): a crash before the delete re-fires under the
            // same id and the engine dedups it (exactly-once). A fresh row uses the
            // deterministic row-id-derived id.
            const commandId =
              row.commandId !== null
                ? CommandId.make(row.commandId)
                : dispatchCommandIdFor("subagent-steer", row.id);
            yield* claimDispatchRows([row.id], commandId).pipe(
              Effect.andThen(dispatchChildSteer(shell, row.text ?? "", commandId)),
              Effect.andThen(deleteDispatchRows([row.id])),
              Effect.catchCause((cause) =>
                Effect.logWarning("deferred child steer dispatch failed; will retry on next idle", {
                  childThreadId,
                  dispatchId: row.id,
                  cause: Cause.pretty(cause),
                }),
              ),
            );
          }
        }),
      );
    });

  const handleTurnDiffCompleted = (event: TurnDiffCompletedEvent) =>
    Effect.gen(function* () {
      const { threadId, status } = event.payload;
      if (children.has(threadId)) {
        const record = children.get(threadId)!;
        const shellOption = yield* getThreadShellBounded(threadId);
        if (archivedChildIds.has(threadId) || archivedActiveChildIds.has(threadId)) {
          yield* settleChild(threadId, "killed", "thread archived", true);
          return;
        }
        const terminalDoneBeforeSettle = yield* Deferred.isDone(record.terminal);
        if (pendingTurnStartByChild.has(threadId)) {
          const sameTurnBecameIdle = pendingSameTurnBecameIdle({
            event,
            shell: shellOption,
            pendingSameTurnId: pendingSameTurnStartByChild.get(threadId),
          });
          if (!sameTurnBecameIdle) {
            if (
              status === "missing" &&
              eventTurnIsStillProjectedRunning({ event, shell: shellOption })
            ) {
              missingDiffWhileRunningByChild.add(threadId);
            }
            return;
          }
          pendingTurnStartByChild.delete(threadId);
          pendingSameTurnStartByChild.delete(threadId);
          activeTurnByChild.set(threadId, event.payload.turnId);
        }
        const latestTurn = Option.isSome(shellOption) ? shellOption.value.latestTurn : null;
        const latestTurnMatchesEvent = latestTurn?.turnId === event.payload.turnId;
        const turnState = latestTurnMatchesEvent ? latestTurn?.state : null;
        const expectedTurnId = activeTurnByChild.get(threadId);
        if (expectedTurnId !== undefined && expectedTurnId !== event.payload.turnId) {
          return;
        }
        if (
          status === "missing" &&
          (Option.isNone(shellOption) || !latestTurnMatchesEvent || turnState === "running")
        ) {
          missingDiffWhileRunningByChild.add(threadId);
          return;
        }
        if (
          status === "ready" &&
          (Option.isNone(shellOption) || !latestTurnMatchesEvent || turnState === "running")
        ) {
          return;
        }
        if (status !== "missing" && latestTurn !== null && !latestTurnMatchesEvent) {
          return;
        }
        missingDiffWhileRunningByChild.delete(threadId);
        if (
          (turnState === "completed" && status === "missing") ||
          (turnState !== "error" && turnState !== "interrupted" && status === "ready")
        ) {
          yield* settleChild(threadId, "completed", null);
        } else {
          yield* settleChild(
            threadId,
            "failed",
            status !== "missing" && status !== "ready"
              ? `turn diff ${status}`
              : turnState
                ? `turn ${turnState}`
                : `turn diff ${status}`,
          );
        }
        if (!terminalDoneBeforeSettle) {
          unarchivedTerminalChildIds.delete(threadId);
        }
        // A child that went idle drains any provider-deferred steer (R-C). Run
        // after settle so a terminal child still flushes a queued steer if the
        // session is somehow re-runnable; the idle re-check guards the dispatch.
        yield* drainChildSteers(threadId);
      }
      // A parent completing a turn drains its pending injections.
      if (pendingInjections.has(threadId)) {
        yield* drainPending(threadId);
      }
    });

  const handleSessionSet = (event: SessionSetEvent) =>
    Effect.gen(function* () {
      const { threadId, session } = event.payload;
      const record = children.get(threadId);
      if (!record) return;
      const reportedActiveTurnId = activeTurnReportedBySession(session);
      if (reportedActiveTurnId !== undefined) {
        if (
          activeTurnByChild.get(threadId) !== reportedActiveTurnId ||
          pendingSameTurnStartByChild.get(threadId) === reportedActiveTurnId
        ) {
          missingDiffWhileRunningByChild.delete(threadId);
        }
        activeTurnByChild.set(threadId, reportedActiveTurnId);
        pendingTurnStartByChild.delete(threadId);
        pendingSameTurnStartByChild.delete(threadId);
        pendingStoppedSettlementByChild.delete(threadId);
      }
      if (
        session.status !== "ready" &&
        session.status !== "stopped" &&
        session.status !== "error"
      ) {
        return;
      }
      if (archivedChildIds.has(threadId) || archivedActiveChildIds.has(threadId)) {
        yield* settleChild(threadId, "killed", "thread archived", true);
        yield* drainChildSteers(threadId);
        return;
      }
      if (session.status === "ready") {
        const expectedTurnId = activeTurnByChild.get(threadId);
        if (expectedTurnId === undefined) return;
        for (let attempt = 0; attempt < 6; attempt += 1) {
          const shellOption = yield* getThreadShellBounded(threadId);
          const latestTurn = Option.isSome(shellOption) ? shellOption.value.latestTurn : null;
          const turnState =
            latestTurn !== null && latestTurn.turnId === expectedTurnId ? latestTurn.state : null;
          if (turnState === "completed") {
            yield* settleChild(threadId, "completed", null);
            yield* drainChildSteers(threadId);
            return;
          }
          if (turnState === "error" || turnState === "interrupted") {
            const sessionLastError = Option.isSome(shellOption)
              ? (shellOption.value.session?.lastError ?? null)
              : null;
            yield* settleChild(threadId, "failed", sessionLastError ?? `turn ${turnState}`);
            yield* drainChildSteers(threadId);
            return;
          }
          if (attempt < 5) {
            yield* Effect.sleep("200 millis");
          }
        }
        return;
      }
      if (session.status !== "stopped" && session.status !== "error") {
        return;
      }
      if (session.status === "stopped") {
        yield* oneShotTerminalCheck(threadId);
        const settledFromProjection = yield* Deferred.isDone(record.terminal);
        if (settledFromProjection) {
          yield* drainChildSteers(threadId);
          return;
        }
      }
      const shellOption = yield* getThreadShell(threadId);
      if (
        !terminalSessionOwnsProjectedSession({
          session,
          shell: shellOption,
          allowStoppedProjectionLag: missingDiffWhileRunningByChild.has(threadId),
          expectedActiveTurnId: activeTurnByChild.get(threadId),
        })
      ) {
        return;
      }
      // A pending start without a same-turn marker can be an initial placeholder;
      // only same-turn pending starts prove a stopped event is racing a replacement.
      if (session.status === "stopped" && pendingSameTurnStartByChild.has(threadId)) {
        pendingStoppedSettlementByChild.add(threadId);
        return;
      }
      if (session.status === "stopped" && Option.isSome(shellOption)) {
        const terminalTurn = currentLiveProjectedTerminal(threadId, shellOption.value);
        if (terminalTurn?.state === "completed") {
          yield* settleChild(threadId, "completed", null);
          yield* drainChildSteers(threadId);
          return;
        }
      }
      if (!shouldSettleTerminalSession(session.status)) {
        return;
      }
      missingDiffWhileRunningByChild.delete(threadId);
      yield* settleChild(threadId, "failed", session.lastError ?? `session ${session.status}`);
    });

  const handleThreadDeleted = (event: ThreadDeletedEvent) =>
    Effect.gen(function* () {
      const { threadId } = event.payload;
      archivedChildIds.delete(threadId);
      archivedActiveChildIds.delete(threadId);
      unarchivedTerminalChildIds.delete(threadId);
      yield* dispatchLimiter.releaseForChild(threadId);
      if (children.has(threadId)) {
        yield* settleChild(threadId, "killed", "thread deleted");
      }
    });

  const handleThreadArchived = (event: ThreadArchivedEvent) =>
    Effect.gen(function* () {
      const { threadId, archivedAt } = event.payload;
      archivedChildIds.add(threadId);
      const detail = yield* getThreadDetailBounded(threadId);
      if (children.has(threadId)) {
        const projectedOutcome = Option.match(detail, {
          onNone: () => null,
          onSome: (thread) => projectedLifecycleTerminal({ ...thread, archivedAt }),
        });
        if (projectedOutcome === null && Option.isNone(detail)) {
          const shell = yield* getThreadShellBounded(threadId);
          const shellOutcome = Option.isSome(shell)
            ? shellTerminalOutcome(threadId, shell.value)
            : null;
          if (shellOutcome !== null) {
            archivedChildIds.delete(threadId);
            archivedActiveChildIds.delete(threadId);
            const terminalDetail = yield* getThreadDetailBounded(threadId);
            const finalAssistantText = Option.match(terminalDetail, {
              onNone: () => null,
              onSome: finalAssistantTextFromThread,
            });
            yield* completeChild(
              threadId,
              shellOutcome.status,
              finalAssistantText,
              shellOutcome.error,
            );
            return;
          }
        }
        const outcome: ChildTerminalOutcome =
          projectedOutcome ?? ({ status: "killed", error: "thread archived" } as const);
        if (isThreadArchivedOutcome(outcome) && projectedOutcome === null) {
          archivedActiveChildIds.add(threadId);
        } else {
          archivedActiveChildIds.delete(threadId);
        }
        if (!isThreadArchivedOutcome(outcome)) {
          archivedChildIds.delete(threadId);
        }
        const finalAssistantText = Option.match(detail, {
          onNone: () => null,
          onSome: finalAssistantTextFromThread,
        });
        yield* completeChild(threadId, outcome.status, finalAssistantText, outcome.error);
        return;
      }
      if (Option.isNone(detail)) return;
      const outcome = projectedLifecycleTerminal({ ...detail.value, archivedAt });
      if (outcome === null) {
        if (detail.value.parentThreadId !== undefined && detail.value.parentThreadId !== null) {
          archivedActiveChildIds.add(threadId);
          yield* dispatchLimiter.releaseForChild(threadId);
        }
        return;
      }
      if (!isThreadArchivedOutcome(outcome)) {
        archivedChildIds.delete(threadId);
      }
      archivedActiveChildIds.delete(threadId);
      yield* dispatchLimiter.releaseForChild(threadId);
      yield* completeChild(
        threadId,
        outcome.status,
        finalAssistantTextFromThread(detail.value),
        outcome.error,
      );
    });

  const discardQueuedArchivedWake = (childThreadId: ThreadId) =>
    Effect.gen(function* () {
      const archivedEntries: Array<PendingInjection> = [];
      const retainedWakeChildIds = new Set<ThreadId>();
      for (const [parentThreadId, queue] of pendingInjections) {
        const retained = queue.filter((entry) => {
          const archivedWake =
            entry.childThreadId === childThreadId &&
            entry.status === "killed" &&
            entry.error === "thread archived";
          if (archivedWake) archivedEntries.push(entry);
          if (!archivedWake && entry.childThreadId === childThreadId) {
            retainedWakeChildIds.add(entry.childThreadId);
          }
          return !archivedWake;
        });
        if (retained.length === 0) {
          pendingInjections.delete(parentThreadId);
        } else if (retained.length !== queue.length) {
          pendingInjections.set(parentThreadId, retained);
        }
      }
      if (archivedEntries.length > 0) {
        const deliveredChildIds = [
          ...new Set(
            archivedEntries
              .filter(
                (entry) =>
                  entry.deliveredByWait || waitDeliveredPromotedChildren.has(entry.childThreadId),
              )
              .map((entry) => entry.childThreadId),
          ),
        ];
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* deleteDispatchRows(archivedEntries.map((entry) => entry.dispatchId));
              yield* deletePromotedChildRows(promotedRowsToDeleteFor(archivedEntries));
              yield* deleteWaitDeliveryRows(deliveredChildIds);
            }),
          )
          .pipe(Effect.orDie);
        yield* clearDeletedWakeStateMemory(archivedEntries);
        for (const retainedChildId of retainedWakeChildIds) {
          queuedWakeChildren.add(retainedChildId);
        }
        const record = children.get(childThreadId);
        if (
          record !== undefined &&
          !record.detached &&
          !retainedWakeChildIds.has(childThreadId) &&
          !waitDeliveryMarkedAt.has(childThreadId) &&
          !waitDeliveredPromotedChildren.has(childThreadId) &&
          !promotedChildren.has(childThreadId)
        ) {
          children.set(childThreadId, { ...record, detached: true });
        }
      }
    });

  const discardQueuedChildWakes = (childThreadId: ThreadId) =>
    Effect.gen(function* () {
      const staleEntries: Array<PendingInjection> = [];
      for (const [parentThreadId, queue] of pendingInjections) {
        const retained = queue.filter((entry) => {
          if (entry.childThreadId === childThreadId) {
            staleEntries.push(entry);
            return false;
          }
          return true;
        });
        if (retained.length === 0) {
          pendingInjections.delete(parentThreadId);
        } else if (retained.length !== queue.length) {
          pendingInjections.set(parentThreadId, retained);
        }
      }
      if (staleEntries.length > 0) {
        const deliveredChildIds = [
          ...new Set(
            staleEntries
              .filter(
                (entry) =>
                  entry.deliveredByWait || waitDeliveredPromotedChildren.has(entry.childThreadId),
              )
              .map((entry) => entry.childThreadId),
          ),
        ];
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* deleteDispatchRows(staleEntries.map((entry) => entry.dispatchId));
              yield* deletePromotedChildRows(promotedRowsToDeleteFor(staleEntries));
              yield* deleteWaitDeliveryRows(deliveredChildIds);
            }),
          )
          .pipe(Effect.orDie);
        yield* clearDeletedWakeStateMemory(staleEntries);
      } else {
        const persistedChildWakeRows = (yield* pendingDispatches
          .listAll()
          .pipe(Effect.orDie)).filter(
          (row) => row.kind === "parent_injection" && row.sourceChildId === childThreadId,
        );
        if (persistedChildWakeRows.length > 0) {
          const clearWaitDelivery =
            waitDeliveryMarkedAt.has(childThreadId) ||
            waitDeliveredPromotedChildren.has(childThreadId) ||
            persistedChildWakeRows.some((row) => row.deliveredByWait);
          const clearPromoted =
            clearWaitDelivery || persistedChildWakeRows.some((row) => row.waitCancellable);
          yield* sql
            .withTransaction(
              Effect.gen(function* () {
                yield* deleteDispatchRows(persistedChildWakeRows.map((row) => row.id));
                if (clearWaitDelivery) {
                  yield* deleteWaitDeliveryRows([childThreadId]);
                }
                if (clearPromoted) {
                  yield* deletePromotedChildRows([childThreadId]);
                }
              }),
            )
            .pipe(Effect.orDie);
          if (clearWaitDelivery || clearPromoted) {
            yield* clearWaitDeliveryStateMemory([childThreadId]);
          }
        }
        queuedWakeChildren.delete(childThreadId);
        if (
          waitDeliveryMarkedAt.has(childThreadId) ||
          waitDeliveredPromotedChildren.has(childThreadId)
        ) {
          yield* deleteWaitDeliveryRows([childThreadId]).pipe(Effect.orDie);
          yield* clearWaitDeliveryStateMemory([childThreadId]);
        }
      }
      const record = children.get(childThreadId);
      if (
        record !== undefined &&
        !record.detached &&
        !waitDeliveryMarkedAt.has(childThreadId) &&
        !waitDeliveredPromotedChildren.has(childThreadId) &&
        !promotedChildren.has(childThreadId)
      ) {
        children.set(childThreadId, { ...record, detached: true });
      }
    });

  const handleThreadUnarchived = (event: ThreadUnarchivedEvent) =>
    Effect.gen(function* () {
      const { threadId } = event.payload;
      archivedChildIds.delete(threadId);
      const record = children.get(threadId);
      if (!record) {
        if (archivedActiveChildIds.has(threadId)) {
          const shell = yield* getThreadShellBounded(threadId);
          if (Option.isSome(shell) ? isProjectedChildActive(shell.value) : true) {
            yield* dispatchLimiter.seedChild(threadId);
          }
          archivedActiveChildIds.delete(threadId);
        }
        unarchivedTerminalChildIds.add(threadId);
        return;
      }
      const completed = yield* Deferred.poll(record.terminal);
      if (Option.isNone(completed)) {
        if (archivedActiveChildIds.has(threadId)) {
          const shell = yield* getThreadShellBounded(threadId);
          if (Option.isSome(shell) && !isProjectedChildActive(shell.value)) {
            yield* dispatchLimiter.releaseForChild(threadId);
          }
        }
        archivedActiveChildIds.delete(threadId);
        yield* discardQueuedArchivedWake(threadId);
        yield* drainChildSteers(threadId);
        return;
      }
      const result = yield* completed.value;
      if (result.status !== "killed" || result.error !== "thread archived") {
        unarchivedTerminalChildIds.add(threadId);
        return;
      }
      const terminal = yield* Deferred.make<ChildWaitResult>();
      children.set(threadId, { ...record, terminal });
      unarchivedTerminalChildIds.delete(threadId);
      const shell = yield* getThreadShellBounded(threadId);
      if (
        Option.isSome(shell)
          ? isProjectedChildActive(shell.value)
          : archivedActiveChildIds.has(threadId)
      ) {
        yield* dispatchLimiter.seedChild(threadId);
      }
      archivedActiveChildIds.delete(threadId);
      yield* discardQueuedArchivedWake(threadId);
      yield* drainChildSteers(threadId);
    });

  const handleTurnStartRequested = (event: TurnStartRequestedEvent) =>
    Effect.gen(function* () {
      const { threadId } = event.payload;
      let record = children.get(threadId);
      if (!record) return;
      let done = yield* Deferred.isDone(record.terminal);
      if (done && unarchivedTerminalChildIds.has(threadId)) {
        yield* discardQueuedChildWakes(threadId);
        const terminal = yield* Deferred.make<ChildWaitResult>();
        record = children.get(threadId) ?? record;
        record = { ...record, terminal };
        children.set(threadId, record);
        unarchivedTerminalChildIds.delete(threadId);
        done = false;
      }
      if (!done) {
        yield* dispatchLimiter.seedChild(threadId);
      }
      recordPendingTurnStart({
        event,
        activeTurns: activeTurnByChild,
        pendingStarts: pendingTurnStartByChild,
        pendingSameTurnStarts: pendingSameTurnStartByChild,
      });
      missingDiffWhileRunningByChild.delete(threadId);
    });

  const handleActivityAppended = (event: ActivityAppendedEvent) =>
    Effect.sync(() => {
      const { threadId, activity } = event.payload;
      if (!children.has(threadId) || activity.kind !== "provider.turn.start.failed") return;
      clearFailedPendingTurnStart({
        threadId,
        failedRequestId: failedTurnStartRequestId(activity),
        activeTurns: activeTurnByChild,
        pendingStarts: pendingTurnStartByChild,
        pendingSameTurnStarts: pendingSameTurnStartByChild,
      });
    });

  const processEvent = (event: OrchestrationEvent) => {
    switch (event.type) {
      case "thread.turn-diff-completed":
        return handleTurnDiffCompleted(event);
      case "thread.turn-start-requested":
        return handleTurnStartRequested(event);
      case "thread.session-set":
        return handleSessionSet(event);
      case "thread.activity-appended":
        return handleActivityAppended(event);
      case "thread.archived":
        return handleThreadArchived(event);
      case "thread.unarchived":
        return handleThreadUnarchived(event);
      case "thread.deleted":
        return handleThreadDeleted(event);
      default:
        return Effect.void;
    }
  };

  const processEventSafely = (event: OrchestrationEvent) =>
    processEvent(event).pipe(
      Effect.andThen(drainAgedPending),
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Effect.failCause(cause);
        }
        return Effect.logWarning("child thread coordinator failed to process event", {
          eventType: event.type,
          cause: Cause.pretty(cause),
        });
      }),
    );

  const worker = yield* makeDrainableWorker(processEventSafely);

  // Synchronous one-shot terminal check (register + waitSlice entry): if the
  // projection already shows the child terminal, settle now without waiting.
  const oneShotTerminalCheck = (
    childThreadId: ThreadId,
    options?: { readonly prepareWaitFallback: boolean },
  ) =>
    Effect.gen(function* () {
      const record = children.get(childThreadId);
      if (!record) return;
      const done = yield* Deferred.isDone(record.terminal);
      if (done) return;
      if (archivedChildIds.has(childThreadId)) {
        yield* settleChild(childThreadId, "killed", "thread archived", true);
        return;
      }
      const settleProjectedLifecycle = (detail: OrchestrationThread) =>
        Effect.gen(function* () {
          const outcome = projectedLifecycleTerminal(detail);
          if (outcome === null) return false;
          const result: ChildWaitResult = {
            childThreadId,
            status: outcome.status,
            finalAssistantText: finalAssistantTextFromThread(detail),
            error: outcome.error,
          };
          if (options?.prepareWaitFallback) {
            yield* ensurePromotedWaitFallback(record, childThreadId, result);
          }
          yield* completeChild(
            childThreadId,
            result.status,
            result.finalAssistantText,
            result.error,
          );
          yield* drainChildSteers(childThreadId);
          return true;
        });
      const shellOption = yield* getThreadShellBounded(childThreadId);
      if (Option.isNone(shellOption)) {
        const detail = yield* getThreadDetailBounded(childThreadId);
        const hasPendingTurnStart =
          pendingTurnStartByChild.has(childThreadId) ||
          pendingSameTurnStartByChild.has(childThreadId);
        if (
          Option.isSome(detail) &&
          !hasPendingTurnStart &&
          (detail.value.archivedAt !== null || detail.value.deletedAt !== null)
        ) {
          const settled = yield* settleProjectedLifecycle(detail.value);
          if (settled) return;
        }
        return;
      }
      const shell = shellOption.value;
      if (shell.archivedAt !== null) {
        const detail = yield* getThreadDetailBounded(childThreadId);
        if (Option.isSome(detail)) {
          const settled = yield* settleProjectedLifecycle(detail.value);
          if (settled) return;
        }
      }
      const canSettleProjectedStoppedSession =
        (!activeTurnByChild.has(childThreadId) ||
          pendingStoppedSettlementByChild.has(childThreadId)) &&
        !pendingTurnStartByChild.has(childThreadId) &&
        !pendingSameTurnStartByChild.has(childThreadId) &&
        !unarchivedTerminalChildIds.has(childThreadId);
      const settleProjectedSessionFailure = (sessionError: string) =>
        Effect.gen(function* () {
          const result: ChildWaitResult = {
            childThreadId,
            status: "failed",
            finalAssistantText: null,
            error: sessionError,
          };
          if (options?.prepareWaitFallback) {
            yield* ensurePromotedWaitFallback(record, childThreadId, result);
          }
          yield* settleChild(childThreadId, "failed", sessionError, true);
        });
      if (shell.session?.status === "error") {
        yield* settleProjectedSessionFailure(shell.session.lastError ?? "session error");
        return;
      }
      if (shell.latestTurn === null) {
        if (shell.session?.status === "stopped" && canSettleProjectedStoppedSession) {
          yield* settleProjectedSessionFailure(shell.session.lastError ?? "session stopped");
        }
        return;
      }
      const terminalTurn = currentLiveProjectedTerminal(childThreadId, shell);
      const turnState = shell.latestTurn.state;
      if (terminalTurn === null) {
        if (shell.session?.status === "stopped" && canSettleProjectedStoppedSession) {
          yield* settleProjectedSessionFailure(shell.session.lastError ?? "session stopped");
          return;
        }
        if (turnState !== "running") return;
        const activeTurnId = shell.session?.activeTurnId ?? null;
        if (activeTurnId !== null && activeTurnId === shell.latestTurn.turnId) {
          activeTurnByChild.set(childThreadId, activeTurnId);
        }
        return;
      }
      pendingTurnStartByChild.delete(childThreadId);
      pendingSameTurnStartByChild.delete(childThreadId);
      activeTurnByChild.set(childThreadId, terminalTurn.turnId);
      const outcome = turnTerminalOutcome(terminalTurn, shell.session);
      if (outcome.status === "completed") {
        const detail = yield* getThreadDetailBounded(childThreadId);
        if (Option.isNone(detail)) return;
        const result: ChildWaitResult = {
          childThreadId,
          status: "completed",
          finalAssistantText: finalAssistantTextFromThread(detail.value),
          error: null,
        };
        if (options?.prepareWaitFallback) {
          yield* ensurePromotedWaitFallback(record, childThreadId, result);
        }
        yield* completeChild(childThreadId, result.status, result.finalAssistantText, result.error);
      } else {
        const result: ChildWaitResult = {
          childThreadId,
          status: "failed",
          finalAssistantText: null,
          error: outcome.error,
        };
        if (options?.prepareWaitFallback) {
          yield* ensurePromotedWaitFallback(record, childThreadId, result);
        }
        yield* settleChild(childThreadId, "failed", outcome.error, true);
      }
      yield* drainChildSteers(childThreadId);
    });

  const sweepWakeProjectionTerminals = Effect.gen(function* () {
    for (const [childThreadId, record] of children) {
      if (!record.detached && !promotedChildren.has(childThreadId)) continue;
      const done = yield* Deferred.isDone(record.terminal);
      if (done) continue;
      yield* oneShotTerminalCheck(childThreadId);
    }
  });

  const validateSpawn: ChildThreadCoordinatorShape["validateSpawn"] = (input) =>
    Effect.gen(function* () {
      const depth = depthFor(input.parentThreadId);
      if (depth >= MAX_DEPTH) {
        return yield* fail(
          `Sub-agent depth limit (${MAX_DEPTH}) reached; refusing to spawn deeper.`,
        );
      }
      const instance = yield* registry.getInstance(input.model.instanceId);
      if (instance === undefined) {
        return yield* fail(`Provider instance "${input.model.instanceId}" is not available.`);
      }
      return { depth };
    });

  const register: ChildThreadCoordinatorShape["register"] = (input) =>
    Effect.gen(function* () {
      if (children.has(input.childThreadId)) {
        return;
      }
      const { depth } = yield* validateSpawn(input);
      if (hasAncestryCycle(input.parentThreadId, input.childThreadId)) {
        return yield* fail("Sub-agent spawn would create an ancestry cycle.");
      }
      const terminal = yield* Deferred.make<ChildWaitResult>();
      const record: ChildRecord = {
        parentThreadId: input.parentThreadId,
        detached: input.detached,
        model: input.model,
        spawnedAtMs: input.spawnedAtMs,
        depth,
        terminal,
      };
      trackChild(input.childThreadId, record);
      yield* wakeLockFor(input.parentThreadId);
      // Hot-subscribe race: the child may already be terminal in the projection.
      yield* oneShotTerminalCheck(input.childThreadId);
    });

  const assertParent: ChildThreadCoordinatorShape["assertParent"] = (
    parentThreadId,
    childThreadId,
  ) =>
    Effect.gen(function* () {
      const record = children.get(childThreadId);
      if (record && record.parentThreadId === parentThreadId) return;
      const shellOption = yield* getThreadShellBounded(childThreadId);
      const matches = Option.match(shellOption, {
        onNone: () => false,
        onSome: (shell) => shell.parentThreadId === parentThreadId,
      });
      if (matches) return;
      return yield* fail(`Thread ${childThreadId} is not a child of ${parentThreadId}.`);
    });

  // R-A: flip each still-running requested child onto the wake-on-completion
  // path. Already-terminal children are skipped (their result was/will be
  // delivered to the waiter); only a child whose waiter stopped needs the wake.
  const promoteToWake: ChildThreadCoordinatorShape["promoteToWake"] = (childThreadIds) =>
    Effect.gen(function* () {
      for (const childThreadId of childThreadIds) {
        const record = children.get(childThreadId);
        if (!record) continue;
        if (record.detached) continue;
        const completed = yield* Deferred.poll(record.terminal);
        if (Option.isSome(completed)) {
          if (!promotedChildren.has(childThreadId)) {
            yield* ensurePromotedChild(record, childThreadId);
          }
          if (!queuedWakeChildren.has(childThreadId)) {
            const result = yield* completed.value;
            yield* wakeParent(record, result);
          }
          continue;
        }
        yield* ensurePromotedChild(record, childThreadId);
      }
    });

  const markWaitDelivered: ChildThreadCoordinatorShape["markWaitDelivered"] = (marks) =>
    Effect.forEach(
      marks,
      (mark) => {
        const childThreadId = typeof mark === "string" ? mark : mark.childThreadId;
        const record = children.get(childThreadId);
        if (!record) return Effect.void;
        return markPromotedWakeDeliveredByWait(
          record,
          childThreadId,
          parentTurnIdAtWaitFromMark(mark),
          childWaitResultFromMark(mark),
        );
      },
      { discard: true },
    );

  const abandonWaitDelivery: ChildThreadCoordinatorShape["abandonWaitDelivery"] = (
    childThreadIds,
  ) =>
    Effect.forEach(
      childThreadIds,
      (childThreadId) => {
        const record = children.get(childThreadId);
        if (!record) return Effect.void;
        return Effect.sync(() => {
          endActivePromotedWait(childThreadId);
          return !hasActivePromotedWait(childThreadId);
        }).pipe(
          Effect.flatMap((shouldDrain) =>
            shouldDrain ? drainPendingWhenParentIdle(record.parentThreadId) : Effect.void,
          ),
        );
      },
      { discard: true },
    );

  const hasPendingInjections: ChildThreadCoordinatorShape["hasPendingInjections"] = (
    parentThreadId,
  ) => Effect.sync(() => (pendingInjections.get(parentThreadId)?.length ?? 0) > 0);

  const enqueueParentInjection: ChildThreadCoordinatorShape["enqueueParentInjection"] = (
    input: EnqueueParentInjectionInput,
  ) => {
    const result: ChildWaitResult = {
      childThreadId: input.childThreadId,
      status: input.status,
      finalAssistantText: input.finalAssistantText,
      error: input.error,
    };
    return wakeParent(
      {
        parentThreadId: input.parentThreadId,
        detached: true,
      },
      input.dedupeKey === undefined ? result : { ...result, dedupeKey: input.dedupeKey },
    );
  };

  const listChildren: ChildThreadCoordinatorShape["listChildren"] = (parentThreadId) =>
    Effect.gen(function* () {
      const ids = byParent.get(parentThreadId);
      if (!ids) return [];
      const entries: Array<ChildListEntry> = [];
      for (const childThreadId of ids) {
        const record = children.get(childThreadId);
        if (!record) continue;
        const settled = yield* Deferred.isDone(record.terminal);
        entries.push({
          childThreadId,
          parentThreadId,
          detached: record.detached,
          model: record.model,
          spawnedAtMs: record.spawnedAtMs,
          depth: record.depth,
          settled,
        });
      }
      return entries;
    });

  const waitForChild = (childThreadId: ThreadId): Effect.Effect<WaitChildResult> =>
    Effect.gen(function* () {
      if (!children.has(childThreadId)) {
        const shellOption = yield* getThreadShellBounded(childThreadId);
        // Re-check in case register() ran concurrently between the map lookup
        // and the (bounded) projection read; if so, fall through to the normal
        // tracked path rather than reporting a misleading error.
        if (!children.has(childThreadId)) {
          if (Option.isNone(shellOption)) {
            // Never registered AND not in projection: terminal error, never hang.
            return {
              childThreadId,
              status: "failed" as const,
              finalAssistantText: null,
              error: "Unknown sub-agent thread; it was never registered.",
            } satisfies WaitChildResult;
          }
          return {
            childThreadId,
            status: "failed" as const,
            finalAssistantText: null,
            error:
              "Sub-agent thread exists in the projection but is not tracked by this server instance.",
          } satisfies WaitChildResult;
        }
      }
      const record = children.get(childThreadId);
      if (!record) {
        // Registered then untracked mid-call (should not happen): never hang.
        return {
          childThreadId,
          status: "failed" as const,
          finalAssistantText: null,
          error: "Sub-agent thread is no longer tracked by this server instance.",
        } satisfies WaitChildResult;
      }
      const protectPromotedWait = !record.detached && promotedChildren.has(childThreadId);
      const parentTurnIdAtWait = protectPromotedWait
        ? yield* runningParentTurnIdForWait(record.parentThreadId)
        : null;
      if (protectPromotedWait) {
        beginActivePromotedWait(childThreadId);
      }
      return yield* Effect.gen(function* () {
        // Re-check the projection in case it caught up after the hot-subscribe gap.
        // The active promoted-wait marker must cover this bounded read too: a live
        // completion can arrive while the projection check is in flight.
        yield* oneShotTerminalCheck(childThreadId, { prepareWaitFallback: true });
        const timeoutSentinel = Symbol.for("t3/subagent/wait-slice-timeout");
        const raced = yield* Effect.race(
          Deferred.await(record.terminal),
          Effect.sleep(`${WAIT_SLICE_SECONDS} seconds`).pipe(Effect.as(timeoutSentinel)),
        );
        if (raced === timeoutSentinel) {
          if (protectPromotedWait) {
            endActivePromotedWait(childThreadId);
          }
          return {
            childThreadId,
            status: "pending" as const,
            finalAssistantText: null,
            error: null,
            parentTurnIdAtWait,
          } satisfies WaitChildResult;
        }
        return {
          childThreadId,
          status: raced.status,
          finalAssistantText: raced.finalAssistantText,
          error: raced.error,
          parentTurnIdAtWait,
        } satisfies WaitChildResult;
      }).pipe(
        Effect.onInterrupt(() =>
          Effect.sync(() => {
            if (protectPromotedWait) endActivePromotedWait(childThreadId);
          }),
        ),
      );
    });

  const waitSlice: ChildThreadCoordinatorShape["waitSlice"] = (input: WaitSliceInput) =>
    Effect.gen(function* () {
      const sliceResults = yield* Effect.forEach(input.childThreadIds, waitForChild, {
        concurrency: "unbounded",
      });
      const now = yield* nowMillis;
      const budgetExhausted = now >= input.budgetDeadlineMs;
      const results: Array<WaitChildResult> = sliceResults.map((result) => {
        if (result.status === "pending" && budgetExhausted) {
          return {
            childThreadId: result.childThreadId,
            status: "timeout" as const,
            finalAssistantText: null,
            error: `wait exceeded budget`,
            ...(result.parentTurnIdAtWait === undefined
              ? {}
              : { parentTurnIdAtWait: result.parentTurnIdAtWait }),
          } satisfies WaitChildResult;
        }
        return result;
      });
      const settledCount = results.filter(
        (result) => result.status !== "pending" && result.status !== "timeout",
      ).length;
      const timedOutCount = results.filter((result) => result.status === "timeout").length;
      const pendingCount = results.filter((result) => result.status === "pending").length;
      const pending =
        input.mode === "any" ? settledCount === 0 && pendingCount > 0 : pendingCount > 0;
      const resumeToken = yield* randomUUID;
      return {
        results,
        settledCount,
        timedOutCount,
        pending,
        resumeToken,
      } satisfies WaitSliceResult;
    });

  // Reconcile terminal-ness from the PERSISTED log (not the lagging projection):
  // replay readEvents(0), tracking the latest signal per known child id.
  const reconcileFromLog = (knownChildIds: Set<ThreadId>) =>
    Effect.gen(function* () {
      const terminalByChild = new Map<
        ThreadId,
        { status: ChildTerminalStatus; error: string | null }
      >();
      const runningByChild = new Map<ThreadId, boolean>();
      const activeTurnByReplayedChild = new Map<ThreadId, TurnId>();
      const pendingTurnStartByReplayedChild = new Map<ThreadId, EventId>();
      const pendingSameTurnStartByReplayedChild = new Map<ThreadId, TurnId>();
      // Historical failure activities predate turnStartRequestId. Delay their
      // replay fallback until the full log has been seen, and only clear the
      // exact pending request captured before any overlapping/newer start.
      const ambiguousLegacyFailureByReplayedChild = new Set<ThreadId>();
      const legacyFailureCandidateByReplayedChild = new Map<ThreadId, EventId>();
      const missingDiffWhileRunningByReplayedChild = new Set<ThreadId>();
      const pendingStoppedSettlementByReplayedChild = new Set<ThreadId>();
      const lifecycleTerminatedByChild = new Set<ThreadId>();
      const archivedSinceLastUnarchiveByChild = new Set<ThreadId>();
      const unarchivedArchiveChildIds = new Set<ThreadId>();
      const unarchivedArchivedTerminalChildIds = new Set<ThreadId>();
      const unarchivedTerminalChildIds = new Set<ThreadId>();
      const unarchivedTerminalStartedChildIds = new Set<ThreadId>();
      const postUnarchiveTerminalByStartedChild = new Map<
        ThreadId,
        { status: ChildTerminalStatus; error: string | null }
      >();
      const rearchivedUnarchivedTerminalStartedChildIds = new Set<ThreadId>();
      const activeArchiveByReplayedChild = new Set<ThreadId>();
      let maxSequence = 0;
      const rememberPostUnarchiveTerminal = (
        threadId: ThreadId,
        outcome: { readonly status: ChildTerminalStatus; readonly error: string | null },
      ) => {
        if (unarchivedTerminalStartedChildIds.has(threadId)) {
          postUnarchiveTerminalByStartedChild.set(threadId, outcome);
        }
      };
      const markLifecycleTerminal = (
        threadId: ThreadId,
        outcome: { readonly status: ChildTerminalStatus; readonly error: string | null },
        options?: { readonly preserveExistingTerminal?: boolean },
      ) => {
        lifecycleTerminatedByChild.add(threadId);
        if (!terminalByChild.has(threadId) || options?.preserveExistingTerminal === false) {
          terminalByChild.set(threadId, outcome);
        }
        runningByChild.set(threadId, false);
        activeTurnByReplayedChild.delete(threadId);
        pendingTurnStartByReplayedChild.delete(threadId);
        pendingSameTurnStartByReplayedChild.delete(threadId);
        missingDiffWhileRunningByReplayedChild.delete(threadId);
      };
      yield* Stream.runForEach(orchestrationEngine.readEvents(0), (event) =>
        Effect.gen(function* () {
          const sequence = (event as { sequence?: number }).sequence;
          if (typeof sequence === "number" && sequence > maxSequence) {
            maxSequence = sequence;
          }
          switch (event.type) {
            case "thread.turn-diff-completed": {
              const { threadId, status } = event.payload;
              if (!knownChildIds.has(threadId)) return;
              if (lifecycleTerminatedByChild.has(threadId)) return;
              if (activeArchiveByReplayedChild.has(threadId)) {
                markLifecycleTerminal(
                  threadId,
                  { status: "killed", error: "thread archived" },
                  { preserveExistingTerminal: false },
                );
                activeArchiveByReplayedChild.delete(threadId);
                return;
              }
              if (status === "missing" && terminalByChild.get(threadId)?.status === "completed") {
                runningByChild.set(threadId, false);
                activeTurnByReplayedChild.delete(threadId);
                pendingTurnStartByReplayedChild.delete(threadId);
                pendingSameTurnStartByReplayedChild.delete(threadId);
                missingDiffWhileRunningByReplayedChild.delete(threadId);
                return;
              }
              if (pendingTurnStartByReplayedChild.has(threadId)) {
                const shellOption = yield* getThreadShellBounded(threadId);
                const sameTurnBecameIdle = pendingSameTurnBecameIdle({
                  event,
                  shell: shellOption,
                  pendingSameTurnId: pendingSameTurnStartByReplayedChild.get(threadId),
                });
                if (!sameTurnBecameIdle) {
                  if (
                    status === "missing" &&
                    eventTurnIsStillProjectedRunning({ event, shell: shellOption })
                  ) {
                    missingDiffWhileRunningByReplayedChild.add(threadId);
                  }
                  return;
                }
                pendingTurnStartByReplayedChild.delete(threadId);
                pendingSameTurnStartByReplayedChild.delete(threadId);
                activeTurnByReplayedChild.set(threadId, event.payload.turnId);
              }
              const expectedTurnId = activeTurnByReplayedChild.get(threadId);
              if (expectedTurnId !== undefined && expectedTurnId !== event.payload.turnId) return;
              if (status === "missing") {
                if (terminalByChild.get(threadId)?.status === "completed") {
                  runningByChild.set(threadId, false);
                  activeTurnByReplayedChild.delete(threadId);
                  return;
                }
                if (runningByChild.get(threadId) === true) {
                  terminalByChild.delete(threadId);
                  missingDiffWhileRunningByReplayedChild.add(threadId);
                  return;
                }
                const outcome = {
                  status: "failed",
                  error: "turn diff missing",
                } as const;
                terminalByChild.set(threadId, outcome);
                rememberPostUnarchiveTerminal(threadId, outcome);
                runningByChild.set(threadId, false);
                pendingSameTurnStartByReplayedChild.delete(threadId);
                missingDiffWhileRunningByReplayedChild.delete(threadId);
                unarchivedTerminalChildIds.delete(threadId);
                unarchivedArchivedTerminalChildIds.delete(threadId);
                return;
              }
              const outcome =
                status === "ready"
                  ? ({ status: "completed", error: null } as const)
                  : ({ status: "failed", error: `turn diff ${status}` } as const);
              terminalByChild.set(threadId, outcome);
              rememberPostUnarchiveTerminal(threadId, outcome);
              runningByChild.set(threadId, false);
              activeTurnByReplayedChild.delete(threadId);
              pendingSameTurnStartByReplayedChild.delete(threadId);
              missingDiffWhileRunningByReplayedChild.delete(threadId);
              unarchivedTerminalChildIds.delete(threadId);
              unarchivedArchivedTerminalChildIds.delete(threadId);
              return;
            }
            case "thread.turn-start-requested": {
              const { threadId } = event.payload;
              if (!knownChildIds.has(threadId)) return;
              if (lifecycleTerminatedByChild.has(threadId)) return;
              if (activeArchiveByReplayedChild.has(threadId)) return;
              const priorTerminal = terminalByChild.get(threadId);
              if (priorTerminal !== undefined && unarchivedTerminalStartedChildIds.has(threadId)) {
                postUnarchiveTerminalByStartedChild.set(threadId, priorTerminal);
              }
              if (pendingTurnStartByReplayedChild.has(threadId)) {
                ambiguousLegacyFailureByReplayedChild.add(threadId);
              } else {
                ambiguousLegacyFailureByReplayedChild.delete(threadId);
              }
              recordPendingTurnStart({
                event,
                activeTurns: activeTurnByReplayedChild,
                pendingStarts: pendingTurnStartByReplayedChild,
                pendingSameTurnStarts: pendingSameTurnStartByReplayedChild,
              });
              runningByChild.set(threadId, true);
              terminalByChild.delete(threadId);
              if (
                unarchivedTerminalChildIds.has(threadId) ||
                unarchivedArchivedTerminalChildIds.has(threadId)
              ) {
                unarchivedTerminalStartedChildIds.add(threadId);
              }
              unarchivedTerminalChildIds.delete(threadId);
              unarchivedArchivedTerminalChildIds.delete(threadId);
              missingDiffWhileRunningByReplayedChild.delete(threadId);
              return;
            }
            case "thread.activity-appended": {
              const { threadId, activity } = event.payload;
              if (!knownChildIds.has(threadId)) return;
              if (lifecycleTerminatedByChild.has(threadId)) return;
              if (activeArchiveByReplayedChild.has(threadId)) return;
              if (activity.kind !== "provider.turn.start.failed") return;
              const failedRequestId = failedTurnStartRequestId(activity);
              if (failedRequestId === undefined) {
                const pendingRequestId = pendingTurnStartByReplayedChild.get(threadId);
                if (
                  pendingRequestId !== undefined &&
                  !ambiguousLegacyFailureByReplayedChild.has(threadId)
                ) {
                  legacyFailureCandidateByReplayedChild.set(threadId, pendingRequestId);
                }
                return;
              }
              clearFailedPendingTurnStart({
                threadId,
                failedRequestId,
                activeTurns: activeTurnByReplayedChild,
                pendingStarts: pendingTurnStartByReplayedChild,
                pendingSameTurnStarts: pendingSameTurnStartByReplayedChild,
              });
              if (!pendingTurnStartByReplayedChild.has(threadId)) {
                ambiguousLegacyFailureByReplayedChild.delete(threadId);
                legacyFailureCandidateByReplayedChild.delete(threadId);
              }
              return;
            }
            case "thread.session-set": {
              const { threadId, session } = event.payload;
              if (!knownChildIds.has(threadId)) return;
              if (lifecycleTerminatedByChild.has(threadId)) return;
              if (activeArchiveByReplayedChild.has(threadId)) {
                if (
                  session.status === "ready" ||
                  session.status === "stopped" ||
                  session.status === "error"
                ) {
                  markLifecycleTerminal(
                    threadId,
                    { status: "killed", error: "thread archived" },
                    { preserveExistingTerminal: false },
                  );
                  activeArchiveByReplayedChild.delete(threadId);
                }
                return;
              }
              const reportedActiveTurnId = activeTurnReportedBySession(session);
              if (reportedActiveTurnId !== undefined) {
                if (
                  activeTurnByReplayedChild.get(threadId) !== reportedActiveTurnId ||
                  pendingSameTurnStartByReplayedChild.get(threadId) === reportedActiveTurnId
                ) {
                  missingDiffWhileRunningByReplayedChild.delete(threadId);
                }
                activeTurnByReplayedChild.set(threadId, reportedActiveTurnId);
                pendingTurnStartByReplayedChild.delete(threadId);
                pendingSameTurnStartByReplayedChild.delete(threadId);
                pendingStoppedSettlementByReplayedChild.delete(threadId);
                ambiguousLegacyFailureByReplayedChild.delete(threadId);
                legacyFailureCandidateByReplayedChild.delete(threadId);
              }
              if (session.status === "ready") {
                if (pendingTurnStartByReplayedChild.has(threadId)) return;
                const expectedTurnId = activeTurnByReplayedChild.get(threadId);
                if (expectedTurnId === undefined) return;
                const shellOption = yield* getThreadShellBounded(threadId);
                const latestTurn = Option.isSome(shellOption) ? shellOption.value.latestTurn : null;
                if (latestTurn?.turnId !== expectedTurnId) return;
                if (latestTurn.state === "completed") {
                  const outcome = { status: "completed", error: null } as const;
                  terminalByChild.set(threadId, outcome);
                  rememberPostUnarchiveTerminal(threadId, outcome);
                  runningByChild.set(threadId, false);
                  activeTurnByReplayedChild.delete(threadId);
                  pendingTurnStartByReplayedChild.delete(threadId);
                  pendingSameTurnStartByReplayedChild.delete(threadId);
                  missingDiffWhileRunningByReplayedChild.delete(threadId);
                  return;
                }
                if (latestTurn.state === "error" || latestTurn.state === "interrupted") {
                  const outcome = turnTerminalOutcome(latestTurn, session);
                  terminalByChild.set(threadId, outcome);
                  rememberPostUnarchiveTerminal(threadId, outcome);
                  runningByChild.set(threadId, false);
                  activeTurnByReplayedChild.delete(threadId);
                  pendingTurnStartByReplayedChild.delete(threadId);
                  pendingSameTurnStartByReplayedChild.delete(threadId);
                  missingDiffWhileRunningByReplayedChild.delete(threadId);
                  return;
                }
              }
              if (shouldSettleTerminalSession(session.status)) {
                const shellOption = yield* getThreadShellBounded(threadId);
                if (
                  !terminalSessionOwnsProjectedSession({
                    session,
                    shell: shellOption,
                    allowStoppedProjectionLag: missingDiffWhileRunningByReplayedChild.has(threadId),
                    expectedActiveTurnId: activeTurnByReplayedChild.get(threadId),
                  })
                ) {
                  return;
                }
                // A pending start without a same-turn marker can be an initial placeholder;
                // only same-turn pending starts prove a stopped event is racing a replacement.
                if (
                  session.status === "stopped" &&
                  pendingSameTurnStartByReplayedChild.has(threadId)
                ) {
                  pendingStoppedSettlementByReplayedChild.add(threadId);
                  return;
                }
                const priorTerminal = terminalByChild.get(threadId);
                if (session.status === "stopped" && priorTerminal?.status === "completed") {
                  runningByChild.set(threadId, false);
                  activeTurnByReplayedChild.delete(threadId);
                  pendingTurnStartByReplayedChild.delete(threadId);
                  pendingSameTurnStartByReplayedChild.delete(threadId);
                  missingDiffWhileRunningByReplayedChild.delete(threadId);
                  return;
                }
                if (
                  priorTerminal?.status === "failed" &&
                  !missingDiffWhileRunningByReplayedChild.has(threadId)
                ) {
                  runningByChild.set(threadId, false);
                  activeTurnByReplayedChild.delete(threadId);
                  pendingTurnStartByReplayedChild.delete(threadId);
                  pendingSameTurnStartByReplayedChild.delete(threadId);
                  missingDiffWhileRunningByReplayedChild.delete(threadId);
                  return;
                }
                const terminalTurn = Option.isSome(shellOption)
                  ? currentProjectedTerminal(shellOption.value, {
                      activeTurnId: activeTurnByReplayedChild.get(threadId),
                      pendingTurnStart: pendingTurnStartByReplayedChild.has(threadId),
                      pendingSameTurnId: pendingSameTurnStartByReplayedChild.get(threadId),
                    })
                  : null;
                if (
                  session.status === "stopped" &&
                  terminalTurn?.state === "completed" &&
                  (priorTerminal?.status !== "failed" ||
                    missingDiffWhileRunningByReplayedChild.has(threadId))
                ) {
                  const outcome = { status: "completed", error: null } as const;
                  terminalByChild.set(threadId, outcome);
                  rememberPostUnarchiveTerminal(threadId, outcome);
                  runningByChild.set(threadId, false);
                  activeTurnByReplayedChild.delete(threadId);
                  pendingTurnStartByReplayedChild.delete(threadId);
                  pendingSameTurnStartByReplayedChild.delete(threadId);
                  missingDiffWhileRunningByReplayedChild.delete(threadId);
                  return;
                }
                const outcome = {
                  status: "failed",
                  error: session.lastError ?? `session ${session.status}`,
                } as const;
                terminalByChild.set(threadId, outcome);
                rememberPostUnarchiveTerminal(threadId, outcome);
                runningByChild.set(threadId, false);
                activeTurnByReplayedChild.delete(threadId);
                pendingTurnStartByReplayedChild.delete(threadId);
                pendingSameTurnStartByReplayedChild.delete(threadId);
                missingDiffWhileRunningByReplayedChild.delete(threadId);
              }
              return;
            }
            case "thread.deleted": {
              const { threadId } = event.payload;
              if (!knownChildIds.has(threadId)) return;
              markLifecycleTerminal(
                threadId,
                { status: "killed", error: "thread deleted" },
                { preserveExistingTerminal: false },
              );
              return;
            }
            case "thread.archived": {
              const { threadId, archivedAt } = event.payload;
              if (!knownChildIds.has(threadId)) return;
              archivedSinceLastUnarchiveByChild.add(threadId);
              postUnarchiveTerminalByStartedChild.delete(threadId);
              if (unarchivedTerminalStartedChildIds.has(threadId)) {
                rearchivedUnarchivedTerminalStartedChildIds.add(threadId);
              }
              unarchivedArchiveChildIds.delete(threadId);
              unarchivedTerminalChildIds.delete(threadId);
              const detail = yield* getThreadDetailBounded(threadId);
              if (Option.isNone(detail)) {
                markLifecycleTerminal(threadId, { status: "killed", error: "thread archived" });
                return;
              }
              const outcome = projectedLifecycleTerminal({ ...detail.value, archivedAt });
              if (outcome === null) {
                activeArchiveByReplayedChild.add(threadId);
                return;
              }
              markLifecycleTerminal(threadId, outcome);
              return;
            }
            case "thread.unarchived": {
              const { threadId } = event.payload;
              if (!knownChildIds.has(threadId)) return;
              if (archivedSinceLastUnarchiveByChild.has(threadId)) {
                unarchivedArchiveChildIds.add(threadId);
                archivedSinceLastUnarchiveByChild.delete(threadId);
              }
              postUnarchiveTerminalByStartedChild.delete(threadId);
              const terminal = terminalByChild.get(threadId);
              if (terminal?.status === "killed" && terminal.error === "thread archived") {
                terminalByChild.delete(threadId);
                unarchivedArchivedTerminalChildIds.add(threadId);
              } else if (terminal !== undefined) {
                unarchivedTerminalChildIds.add(threadId);
              }
              lifecycleTerminatedByChild.delete(threadId);
              activeArchiveByReplayedChild.delete(threadId);
              return;
            }
            default:
              return;
          }
        }),
      ).pipe(Effect.orDie);
      for (const [threadId, failedRequestId] of legacyFailureCandidateByReplayedChild) {
        if (ambiguousLegacyFailureByReplayedChild.has(threadId)) continue;
        clearFailedPendingTurnStart({
          threadId,
          failedRequestId,
          activeTurns: activeTurnByReplayedChild,
          pendingStarts: pendingTurnStartByReplayedChild,
          pendingSameTurnStarts: pendingSameTurnStartByReplayedChild,
        });
      }
      return {
        terminalByChild,
        activeTurnByReplayedChild,
        pendingTurnStartByReplayedChild,
        pendingSameTurnStartByReplayedChild,
        missingDiffWhileRunningByReplayedChild,
        pendingStoppedSettlementByReplayedChild,
        unarchivedArchiveChildIds,
        unarchivedArchivedTerminalChildIds,
        unarchivedTerminalChildIds,
        unarchivedTerminalStartedChildIds,
        postUnarchiveTerminalByStartedChild,
        rearchivedUnarchivedTerminalStartedChildIds,
        activeArchiveByReplayedChild,
        maxSequence,
      };
    });

  const start: ChildThreadCoordinatorShape["start"] = Effect.fn("ChildThreadCoordinator.start")(
    function* () {
      yield* Effect.sync(() => {
        activeCoordinator = service;
      });
      const persisted = yield* pendingDispatches.listAll().pipe(Effect.orDie);
      const waitDeliveryRows = yield* listWaitDeliveryRows().pipe(Effect.orDie);
      const promotedRows = yield* listPromotedChildRows().pipe(Effect.orDie);
      const promotedParentByChild = new Map<ThreadId, ThreadId>();
      for (const row of promotedRows) {
        promotedParentByChild.set(row.childThreadId as ThreadId, row.parentThreadId as ThreadId);
      }
      const waitDeliveredChildIds = new Set<ThreadId>();
      for (const row of waitDeliveryRows) {
        const childThreadId = row.childThreadId as ThreadId;
        waitDeliveredChildIds.add(childThreadId);
        waitDeliveryMarkedAt.set(childThreadId, row.deliveredAt);
        if (row.parentTurnIdAtDelivery !== null) {
          waitDeliveryParentTurnAt.set(childThreadId, row.parentTurnIdAtDelivery as TurnId);
        }
      }
      for (const row of persisted) {
        if (row.kind !== "parent_injection" || row.sourceChildId === null || !row.deliveredByWait) {
          continue;
        }
        if (waitDeliveryMarkedAt.has(row.sourceChildId)) continue;
        const childThreadId = row.sourceChildId;
        const deliveredAt = String(row.createdAt);
        yield* upsertWaitDeliveryRow({
          childThreadId,
          parentThreadId: row.targetThreadId,
          deliveredAt,
          parentTurnIdAtDelivery: null,
        }).pipe(Effect.orDie);
        waitDeliveredChildIds.add(childThreadId);
        waitDeliveryMarkedAt.set(childThreadId, deliveredAt);
      }
      const pendingWakeChildIds = new Set<ThreadId>();
      for (const row of persisted) {
        if (row.kind !== "parent_injection" || row.sourceChildId === null) continue;
        const deliveredByWait = row.deliveredByWait || waitDeliveredChildIds.has(row.sourceChildId);
        pendingWakeChildIds.add(row.sourceChildId);
        queuedWakeChildren.add(row.sourceChildId);
        if (row.waitCancellable || deliveredByWait) {
          promotedChildren.add(row.sourceChildId);
        }
        if (deliveredByWait) {
          waitDeliveredPromotedChildren.add(row.sourceChildId);
        }
      }
      const stalePromotedChildIds = new Set<ThreadId>();
      for (const childThreadId of promotedParentByChild.keys()) {
        if (waitDeliveredChildIds.has(childThreadId) && !pendingWakeChildIds.has(childThreadId)) {
          stalePromotedChildIds.add(childThreadId);
        }
      }
      if (stalePromotedChildIds.size > 0) {
        yield* deletePromotedChildRows([...stalePromotedChildIds]).pipe(Effect.orDie);
      }

      // (1) Load all parent-linked children from the projection.
      const rows = yield* listPersistedChildRows().pipe(Effect.orDie);
      const knownChildIds = new Set<ThreadId>();
      const projectedTerminalByChild = new Map<ThreadId, ChildTerminalOutcome>();
      for (const row of rows) {
        if (row.parentThreadId === null) continue;
        const childThreadId = row.threadId as ThreadId;
        const parentThreadId = row.parentThreadId as ThreadId;
        const restoredPromoted =
          promotedParentByChild.get(childThreadId) === parentThreadId &&
          !stalePromotedChildIds.has(childThreadId);
        if (children.has(childThreadId)) continue;
        const detailOption = yield* getThreadDetail(childThreadId);
        if (Option.isNone(detailOption)) {
          // No detail row yet (projection lag): do NOT fabricate a model or
          // settle this child. It will be validated when it calls register().
          // Settling it here on a fabricated "unknown" instance would wrongly
          // kill a child that is still running (wake CRITICAL #2).
          continue;
        }
        const detail = detailOption.value;
        const projectedTerminal = projectedLifecycleTerminal(detail);
        if (projectedTerminal !== null) {
          projectedTerminalByChild.set(childThreadId, projectedTerminal);
        }
        const terminal = yield* Deferred.make<ChildWaitResult>();
        trackChild(childThreadId, {
          parentThreadId,
          // A durable wake row is already the recovery path for this child; do
          // not synthesize a second wake while replaying terminal state. A
          // wait-delivery tombstone means the foreground wait already delivered
          // this child and any pending fallback row may have been pruned.
          detached:
            !restoredPromoted &&
            !pendingWakeChildIds.has(childThreadId) &&
            !waitDeliveredChildIds.has(childThreadId),
          model: detail.modelSelection,
          spawnedAtMs: 0,
          depth: 1,
          terminal,
        });
        if (restoredPromoted) {
          promotedChildren.add(childThreadId);
        }
        yield* wakeLockFor(parentThreadId);
        knownChildIds.add(childThreadId);
      }

      // (2) Determine terminal-ness from the persisted immutable log.
      const {
        terminalByChild,
        activeTurnByReplayedChild,
        pendingTurnStartByReplayedChild,
        pendingSameTurnStartByReplayedChild,
        missingDiffWhileRunningByReplayedChild,
        pendingStoppedSettlementByReplayedChild,
        unarchivedArchiveChildIds,
        unarchivedArchivedTerminalChildIds,
        unarchivedTerminalChildIds: replayedUnarchivedTerminalChildIds,
        unarchivedTerminalStartedChildIds: replayedUnarchivedTerminalStartedChildIds,
        postUnarchiveTerminalByStartedChild,
        rearchivedUnarchivedTerminalStartedChildIds,
        activeArchiveByReplayedChild,
      } = yield* reconcileFromLog(knownChildIds);
      for (const [childThreadId, turnId] of activeTurnByReplayedChild) {
        activeTurnByChild.set(childThreadId, turnId);
      }
      for (const [childThreadId, requestId] of pendingTurnStartByReplayedChild) {
        pendingTurnStartByChild.set(childThreadId, requestId);
      }
      for (const [childThreadId, turnId] of pendingSameTurnStartByReplayedChild) {
        pendingSameTurnStartByChild.set(childThreadId, turnId);
      }
      for (const childThreadId of missingDiffWhileRunningByReplayedChild) {
        missingDiffWhileRunningByChild.add(childThreadId);
      }
      for (const childThreadId of pendingStoppedSettlementByReplayedChild) {
        pendingStoppedSettlementByChild.add(childThreadId);
      }
      const terminalTextByStartedChild = new Map<
        ThreadId,
        { readonly text: string | null; readonly textUnavailable: boolean }
      >();
      for (const childThreadId of replayedUnarchivedTerminalStartedChildIds) {
        if (!terminalByChild.has(childThreadId)) continue;
        const detail = yield* getThreadDetailBounded(childThreadId);
        if (Option.isSome(detail)) {
          const text = finalAssistantTextFromThread(detail.value);
          const latestTurn = detail.value.latestTurn;
          terminalTextByStartedChild.set(childThreadId, {
            text,
            textUnavailable:
              text === null &&
              latestTurn?.state === "completed" &&
              latestTurn.assistantMessageId !== null,
          });
        }
      }
      const prunedArchivedWakeDispatchIds = new Set<PendingDispatchId>();
      const prunedArchivedWakeChildIds = new Set<ThreadId>();
      const prunedArchivedDeliveredWakeChildIds = new Set<ThreadId>();
      const deliveredWakeCleanupChildIds = new Set<ThreadId>();
      const promotedWakeCleanupChildIds = new Set<ThreadId>();
      const retainedPromotedWakeChildIds = new Set<ThreadId>();
      const promotedChildCleanupIds = new Set<ThreadId>();
      const retainedPostUnarchiveFallbackWakeIdByChild = new Map<ThreadId, PendingDispatchId>();
      const retainedPostUnarchiveFallbackWakeCreatedAtByChild = new Map<ThreadId, number>();
      for (const row of persisted) {
        if (row.kind !== "parent_injection" || row.sourceChildId === null) continue;
        const childThreadId = row.sourceChildId as ThreadId;
        if (!replayedUnarchivedTerminalStartedChildIds.has(childThreadId)) continue;
        if (terminalByChild.has(childThreadId)) continue;
        const terminal = postUnarchiveTerminalByStartedChild.get(childThreadId);
        if (terminal === undefined) continue;
        if (row.status !== terminal.status || row.error !== terminal.error) continue;
        const createdAtMs = Date.parse(String(row.createdAt));
        const comparableCreatedAt = Number.isFinite(createdAtMs) ? createdAtMs : 0;
        const retainedCreatedAt =
          retainedPostUnarchiveFallbackWakeCreatedAtByChild.get(childThreadId);
        const retainedId = retainedPostUnarchiveFallbackWakeIdByChild.get(childThreadId);
        if (
          retainedCreatedAt === undefined ||
          comparableCreatedAt > retainedCreatedAt ||
          (comparableCreatedAt === retainedCreatedAt &&
            retainedId !== undefined &&
            String(row.id) > String(retainedId))
        ) {
          retainedPostUnarchiveFallbackWakeIdByChild.set(childThreadId, row.id);
          retainedPostUnarchiveFallbackWakeCreatedAtByChild.set(childThreadId, comparableCreatedAt);
        }
      }
      const staleWakeRows = persisted.filter((row) => {
        if (row.kind !== "parent_injection" || row.sourceChildId === null) return false;
        const childThreadId = row.sourceChildId as ThreadId;
        if (
          unarchivedArchiveChildIds.has(childThreadId) &&
          row.status === "killed" &&
          row.error === "thread archived"
        ) {
          return true;
        }
        if (
          rearchivedUnarchivedTerminalStartedChildIds.has(childThreadId) &&
          row.status === "killed" &&
          row.error === "thread archived"
        ) {
          return false;
        }
        if (!replayedUnarchivedTerminalStartedChildIds.has(childThreadId)) return false;
        const replayTerminal = terminalByChild.get(childThreadId);
        const postUnarchiveTerminal = postUnarchiveTerminalByStartedChild.get(childThreadId);
        const terminal = replayTerminal ?? postUnarchiveTerminal;
        if (terminal === undefined) return true;
        if (row.status !== terminal.status || row.error !== terminal.error) return true;
        if (replayTerminal === undefined && postUnarchiveTerminal !== undefined) {
          return retainedPostUnarchiveFallbackWakeIdByChild.get(childThreadId) !== row.id;
        }
        if (terminal.status !== "completed") return false;
        const projectedText = terminalTextByStartedChild.get(childThreadId);
        if (projectedText === undefined) return row.commandId === null;
        if (projectedText.textUnavailable && row.commandId !== null) return false;
        return row.text !== projectedText.text;
      });
      if (staleWakeRows.length > 0) {
        for (const row of staleWakeRows) {
          const childThreadId = row.sourceChildId as ThreadId;
          prunedArchivedWakeDispatchIds.add(row.id);
          prunedArchivedWakeChildIds.add(childThreadId);
          if (row.deliveredByWait) {
            prunedArchivedDeliveredWakeChildIds.add(childThreadId);
          }
          if (row.waitCancellable) {
            promotedWakeCleanupChildIds.add(childThreadId);
          }
        }
        for (const childThreadId of prunedArchivedWakeChildIds) {
          const hasRemainingDeliveredWake = persisted.some(
            (row) =>
              row.kind === "parent_injection" &&
              row.sourceChildId === childThreadId &&
              !prunedArchivedWakeDispatchIds.has(row.id) &&
              row.deliveredByWait,
          );
          if (
            prunedArchivedDeliveredWakeChildIds.has(childThreadId) &&
            !hasRemainingDeliveredWake
          ) {
            deliveredWakeCleanupChildIds.add(childThreadId);
          }
          const hasRemainingPromotedWake = persisted.some(
            (row) =>
              row.kind === "parent_injection" &&
              row.sourceChildId === childThreadId &&
              !prunedArchivedWakeDispatchIds.has(row.id) &&
              row.waitCancellable,
          );
          if (hasRemainingPromotedWake) {
            retainedPromotedWakeChildIds.add(childThreadId);
            promotedWakeCleanupChildIds.delete(childThreadId);
          }
        }
        for (const childThreadId of promotedWakeCleanupChildIds) {
          promotedChildCleanupIds.add(childThreadId);
        }
        for (const childThreadId of deliveredWakeCleanupChildIds) {
          if (!retainedPromotedWakeChildIds.has(childThreadId)) {
            promotedChildCleanupIds.add(childThreadId);
          }
        }
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* deleteDispatchRows(staleWakeRows.map((row) => row.id));
              yield* deleteWaitDeliveryRows([...deliveredWakeCleanupChildIds]);
              yield* deletePromotedChildRows([...promotedChildCleanupIds]);
            }),
          )
          .pipe(Effect.orDie);
      }
      for (const childThreadId of prunedArchivedWakeChildIds) {
        const remainingWakeRows = persisted.filter(
          (row) =>
            row.kind === "parent_injection" &&
            row.sourceChildId === childThreadId &&
            !prunedArchivedWakeDispatchIds.has(row.id),
        );
        const hasRemainingWake = remainingWakeRows.length > 0;
        if (deliveredWakeCleanupChildIds.has(childThreadId)) {
          waitDeliveredChildIds.delete(childThreadId);
          waitDeliveredPromotedChildren.delete(childThreadId);
          waitDeliveryMarkedAt.delete(childThreadId);
          waitDeliveryParentTurnAt.delete(childThreadId);
        }
        if (
          deliveredWakeCleanupChildIds.has(childThreadId) ||
          promotedChildCleanupIds.has(childThreadId)
        ) {
          if (promotedChildCleanupIds.has(childThreadId)) {
            promotedChildren.delete(childThreadId);
            activePromotedWaitChildren.delete(childThreadId);
          }
        }
        if (hasRemainingWake) {
          queuedWakeChildren.add(childThreadId);
          continue;
        }
        queuedWakeChildren.delete(childThreadId);
        if (waitDeliveredChildIds.has(childThreadId)) continue;
        if (promotedChildren.has(childThreadId)) continue;
        const record = children.get(childThreadId);
        if (record !== undefined && !record.detached) {
          children.set(childThreadId, { ...record, detached: true });
        }
      }
      const remainingWakeChildIds = new Set<ThreadId>();
      for (const row of persisted) {
        if (
          row.kind === "parent_injection" &&
          row.sourceChildId !== null &&
          !prunedArchivedWakeDispatchIds.has(row.id)
        ) {
          remainingWakeChildIds.add(row.sourceChildId);
        }
      }
      for (const childThreadId of replayedUnarchivedTerminalChildIds) {
        unarchivedTerminalChildIds.add(childThreadId);
        if (remainingWakeChildIds.has(childThreadId)) {
          queuedWakeChildren.add(childThreadId);
        }
      }
      for (const [childThreadId, outcome] of terminalByChild) {
        yield* settleChild(childThreadId, outcome.status, outcome.error);
      }

      const shouldMarkProjectionUnarchivedTerminal = (childThreadId: ThreadId): boolean =>
        unarchivedArchiveChildIds.has(childThreadId) ||
        unarchivedArchivedTerminalChildIds.has(childThreadId);

      const inactiveProjectionUnarchivedChildIds = new Set<ThreadId>();

      // Non-terminal children may still have terminal projection state even when
      // replay only saw a conservative "missing" diff. Reconcile that before
      // applying the provider-instance kill path.
      for (const childThreadId of knownChildIds) {
        if (terminalByChild.has(childThreadId)) continue;
        const projectedTerminal = projectedTerminalByChild.get(childThreadId);
        if (
          shouldMarkProjectionUnarchivedTerminal(childThreadId) &&
          projectedTerminal !== undefined
        ) {
          unarchivedTerminalChildIds.add(childThreadId);
          if (
            projectedTerminal.status === "failed" &&
            projectedTerminal.fromSessionProjection === true
          ) {
            continue;
          }
        }
        yield* oneShotTerminalCheck(childThreadId);
      }

      // Projection state is the same state the UI uses: if a boot-reconciled
      // child is stopped/errored/archived/deleted, it cannot consume dispatch
      // capacity even when old logs lack a terminal event.
      for (const childThreadId of knownChildIds) {
        if (terminalByChild.has(childThreadId)) continue;
        if (
          activeTurnByChild.has(childThreadId) ||
          pendingTurnStartByChild.has(childThreadId) ||
          pendingSameTurnStartByChild.has(childThreadId)
        ) {
          continue;
        }
        const projectedTerminal = projectedTerminalByChild.get(childThreadId);
        if (projectedTerminal === undefined) continue;
        const record = children.get(childThreadId);
        if (!record) continue;
        const done = yield* Deferred.isDone(record.terminal);
        if (done) continue;
        if (shouldMarkProjectionUnarchivedTerminal(childThreadId)) {
          unarchivedTerminalChildIds.add(childThreadId);
          if (
            projectedTerminal.status === "failed" &&
            projectedTerminal.fromSessionProjection === true
          ) {
            inactiveProjectionUnarchivedChildIds.add(childThreadId);
            continue;
          }
        }
        yield* settleChild(childThreadId, projectedTerminal.status, projectedTerminal.error);
      }
      for (const childThreadId of activeArchiveByReplayedChild) {
        const record = children.get(childThreadId);
        if (!record) continue;
        const done = yield* Deferred.isDone(record.terminal);
        if (!done) archivedActiveChildIds.add(childThreadId);
      }

      // Remaining non-terminal children: validate the provider instance still exists.
      // Seed the dispatch limiter for survivors so a restart cannot launch a
      // full new cap on top of already-running sub-agents.
      for (const childThreadId of knownChildIds) {
        if (terminalByChild.has(childThreadId)) continue;
        const record = children.get(childThreadId);
        if (!record) continue;
        const done = yield* Deferred.isDone(record.terminal);
        if (done) continue;
        const instance = yield* registry.getInstance(record.model.instanceId);
        if (instance === undefined) {
          yield* Effect.logWarning(
            "reconciled non-terminal sub-agent lost its provider instance; terminating",
            {
              childThreadId,
              instanceId: record.model.instanceId,
              parentThreadId: record.parentThreadId,
            },
          );
          yield* settleChild(childThreadId, "killed", "provider instance removed");
          continue;
        }
        if (inactiveProjectionUnarchivedChildIds.has(childThreadId)) continue;
        yield* dispatchLimiter.seedChild(childThreadId);
      }

      // (2b) R-B: load durable 'parent_injection' rows into the in-memory
      // pendingInjections map so a restart resumes delivery (drained on parent
      // idle / next turn-completion / age valve as usual). Dedup by dispatchId
      // against any entry wakeParent already enqueued during reconciliation above.
      const reloadNow = yield* nowMillis;
      const parentsWithRestartDrainRows = new Set<ThreadId>();
      const parentsWithDeferredRestartDrainRows = new Set<ThreadId>();
      for (const row of persisted) {
        if (row.kind !== "parent_injection") continue;
        if (prunedArchivedWakeDispatchIds.has(row.id)) continue;
        const createdAtMs = Date.parse(String(row.createdAt));
        const parentThreadId = row.targetThreadId;
        const deliveredByWait =
          row.deliveredByWait ||
          (row.sourceChildId !== null && waitDeliveredChildIds.has(row.sourceChildId));
        const hasRestoredChild =
          row.sourceChildId !== null && children.has(row.sourceChildId as ThreadId);
        const restartShouldDrain = deliveredByWait || (row.waitCancellable && !hasRestoredChild);
        if (restartShouldDrain) {
          parentsWithRestartDrainRows.add(parentThreadId);
        } else if (row.waitCancellable) {
          parentsWithDeferredRestartDrainRows.add(parentThreadId);
        }
        const queue = pendingInjections.get(parentThreadId) ?? [];
        if (queue.some((entry) => entry.dispatchId === row.id)) continue;
        queue.push({
          childThreadId: row.sourceChildId ?? row.targetThreadId,
          status: (row.status as ChildTerminalStatus | null) ?? "completed",
          text: row.text,
          error: row.error,
          enqueuedAtMs: Number.isFinite(createdAtMs) ? createdAtMs : reloadNow,
          dispatchId: row.id,
          deliveredByWait,
          waitCancellable: row.waitCancellable,
          // A claimed row (its turn was dispatched under this exact id before the
          // crash) must re-fire under that id so the engine dedups a landed turn.
          claimedCommandId: row.commandId !== null ? CommandId.make(row.commandId) : null,
        });
        pendingInjections.set(parentThreadId, queue);
        yield* wakeLockFor(parentThreadId);
      }

      // (3) Fork the hot stream before any restart drain can dispatch fresh
      // parent turns. Those drain-generated events are not in the immutable-log
      // replay above, so the subscription must be live first.
      yield* Effect.forkScoped(
        Stream.runForEach(orchestrationEngine.streamDomainEvents, (event) => worker.enqueue(event)),
      );
      yield* Effect.yieldNow;

      yield* Effect.forEach(parentsWithRestartDrainRows, drainPendingWhenParentIdle, {
        discard: true,
      });
      if (parentsWithDeferredRestartDrainRows.size > 0) {
        yield* Effect.forkScoped(
          Effect.sleep("2 seconds").pipe(
            Effect.andThen(
              Effect.forEach(parentsWithDeferredRestartDrainRows, drainPendingWhenParentIdle, {
                discard: true,
              }),
            ),
          ),
        );
      }

      yield* Effect.forever(
        drainRetriedPending.pipe(
          Effect.andThen(drainAgedPending),
          Effect.andThen(sweepWakeProjectionTerminals),
          Effect.catchCause((cause) =>
            Effect.logWarning("subagent coordinator maintenance sweep failed", {
              cause: Cause.pretty(cause),
            }),
          ),
          Effect.andThen(Effect.sleep("2 seconds")),
        ),
      ).pipe(Effect.forkScoped);

      yield* Effect.logInfo("child.thread.coordinator.reactor.started", {
        reconciledChildren: knownChildIds.size,
      });
    },
  );

  service = {
    validateSpawn,
    register,
    waitSlice,
    assertParent,
    promoteToWake,
    markWaitDelivered,
    abandonWaitDelivery,
    hasPendingInjections,
    enqueueParentInjection,
    listChildren,
    start,
    drain: worker.drain,
  } satisfies ChildThreadCoordinatorShape;

  return service;
});

let activeCoordinator: ChildThreadCoordinatorShape | null = null;

/** Reach the live coordinator from MCP tool handlers (mirrors `dispatchActive`). */
export const coordinatorActive = (): ChildThreadCoordinatorShape | null => activeCoordinator;

export const ChildThreadCoordinatorLive = Layer.effect(
  ChildThreadCoordinator,
  Effect.acquireRelease(make, (coordinator) =>
    Effect.sync(() => {
      if (activeCoordinator === coordinator) activeCoordinator = null;
    }),
  ),
);

export const ActiveChildThreadCoordinatorLive = Layer.effectDiscard(
  Effect.acquireRelease(
    ChildThreadCoordinator.pipe(
      Effect.tap((coordinator) =>
        Effect.sync(() => {
          activeCoordinator = coordinator;
        }),
      ),
    ),
    (coordinator) =>
      Effect.sync(() => {
        if (activeCoordinator === coordinator) activeCoordinator = null;
      }),
  ),
);

// Referenced for type alignment with the dispatcher shape used by dispatchActive.
export type { BootstrapTurnStartDispatcherShape };
