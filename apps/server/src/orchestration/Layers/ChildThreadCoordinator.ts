/**
 * ChildThreadCoordinator implementation - see Services/ChildThreadCoordinator.ts
 * and finalPlan §5 for the design. The `ActiveChildThreadCoordinatorLive`
 * global-capture mirrors `ActiveBootstrapTurnStartDispatcherLive` /
 * `ThreadStartRuntimeLive` so MCP tool handlers can reach the coordinator
 * without threading it through the toolkit `Context`.
 */
import {
  CommandId,
  DataAudience,
  EventId,
  IsoDateTime,
  MessageId,
  ModelSelection,
  type OrchestrationEvent,
  type OrchestrationLatestTurn,
  type OrchestrationThread,
  type OrchestrationThreadShell,
  type ProviderSession,
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
import * as Schedule from "effect/Schedule";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import {
  dispatchActive,
  type BootstrapTurnStartDispatcherShape,
} from "../Services/BootstrapTurnStartDispatcher.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { ProviderInstanceRegistry } from "../../provider/Services/ProviderInstanceRegistry.ts";
import { ProviderService } from "../../provider/Services/ProviderService.ts";
import { SubagentDispatchLimiter } from "../../mcp/toolkits/subagent/SubagentDispatchLimiter.ts";
import { toPersistenceSqlError } from "../../persistence/Errors.ts";
import {
  PendingDispatchRepository,
  type PendingDispatch,
  type PendingDispatchId,
} from "../../persistence/Services/PendingDispatches.ts";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import * as Schema from "effect/Schema";
import { ThreadStartToolError } from "../../mcp/toolkits/thread/tools.ts";
import { OrchestrationCommandAcceptanceDeferredError } from "../Errors.ts";
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

import {
  audienceBoundSystemDispatchAuthority,
  threadAudienceSystemDispatchAuthority,
} from "../commandAudienceGuard.ts";
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
  /** Terminal event cutoff for the child lifecycle that produced this wake. */
  readonly sourceTerminalSequence: number | null;
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
   * deterministic id; non-null means it MUST be dispatched with every other
   * pending row claimed under this exact id so the engine's receipt dedup makes
   * a landed turn a no-op (no duplicate) and an un-landed turn fire (no loss),
   * regardless of how fresh rows later re-batch.
   */
  readonly claimedCommandId: CommandId | null;
}

interface TerminalDeliveryClaim {
  readonly parentThreadId: ThreadId;
  readonly claimId: string;
  readonly claimedAt: string;
  readonly claimedSequence: number;
  readonly terminalKind: "completed" | "failed" | "killed" | "archived";
}

type TerminalWakeInput = ChildWaitResult &
  Pick<EnqueueParentInjectionInput, "dedupeKey"> & {
    readonly sourceTerminalSequence: number | null;
  };

type TurnDiffCompletedEvent = Extract<OrchestrationEvent, { type: "thread.turn-diff-completed" }>;
type TurnStartRequestedEvent = Extract<OrchestrationEvent, { type: "thread.turn-start-requested" }>;
type SessionSetEvent = Extract<OrchestrationEvent, { type: "thread.session-set" }>;
type ActivityAppendedEvent = Extract<OrchestrationEvent, { type: "thread.activity-appended" }>;
type ThreadArchivedEvent = Extract<OrchestrationEvent, { type: "thread.archived" }>;
type ThreadUnarchivedEvent = Extract<OrchestrationEvent, { type: "thread.unarchived" }>;
type ThreadDeletedEvent = Extract<OrchestrationEvent, { type: "thread.deleted" }>;

const ORPHAN_SETTLED_ACTIVITY_KIND = "subagent.orphan.settled";
const ORPHAN_ARCHIVED_PARENT_REASON = "orphaned by archived parent";
const ORPHAN_RETIRED_PARENT_REASON = "orphaned by deleted or retired parent";
const BOOT_EXTERNAL_CALL_TIMEOUT_MS = PROJECTION_READ_TIMEOUT_MS;

const latestTurnTerminalAt = (latestTurn: OrchestrationLatestTurn | null): string | null =>
  latestTurn?.completedAt ?? latestTurn?.startedAt ?? latestTurn?.requestedAt ?? null;

interface ChildTerminalOutcome {
  readonly status: ChildTerminalStatus;
  readonly error: string | null;
  readonly fromSessionProjection?: true;
  readonly terminalAt: string | null;
}

type BoundedProjectionRead<A> =
  | { readonly _tag: "Found"; readonly value: A }
  | { readonly _tag: "Missing" }
  | { readonly _tag: "Unavailable"; readonly cause: string };

const nonSessionTerminalOutcome = (
  status: ChildTerminalStatus,
  error: string | null,
  terminalAt: string | null,
): ChildTerminalOutcome => ({ status, error, terminalAt });

const isTerminalSessionProjection = (
  session: OrchestrationThread["session"] | OrchestrationThreadShell["session"],
): boolean => session?.status === "error" || session?.status === "stopped";

const turnTerminalOutcome = (
  latestTurn: OrchestrationLatestTurn,
  session: OrchestrationThread["session"] | OrchestrationThreadShell["session"],
): ChildTerminalOutcome => {
  const turnTerminalAt = latestTurnTerminalAt(latestTurn);
  if (latestTurn.state === "completed") {
    return nonSessionTerminalOutcome("completed", null, turnTerminalAt);
  }
  if (isTerminalSessionProjection(session)) {
    return {
      status: "failed",
      error: session?.lastError ?? `turn ${latestTurn.state}`,
      fromSessionProjection: true,
      // Session death is the causal evidence. The projection may convert the
      // turn to interrupted/error later; that lag must not move a pre-unarchive
      // provider death across the unarchive boundary.
      terminalAt: session?.updatedAt ?? turnTerminalAt,
    };
  }
  return nonSessionTerminalOutcome(
    "failed",
    session?.lastError ?? `turn ${latestTurn.state}`,
    turnTerminalAt,
  );
};

const sessionTerminalOutcome = (
  session: NonNullable<OrchestrationThread["session"]>,
): ChildTerminalOutcome & { readonly fromSessionProjection: true } => ({
  status: "failed",
  error: session.lastError ?? `session ${session.status}`,
  fromSessionProjection: true,
  terminalAt: session.updatedAt,
});

const isThreadArchivedOutcome = (outcome: ChildTerminalOutcome): boolean =>
  outcome.status === "killed" && outcome.error === "thread archived";

const ChildRowSchema = Schema.Struct({
  threadId: Schema.String,
  parentThreadId: Schema.NullOr(Schema.String),
  dataAudience: DataAudience,
  modelSelection: Schema.fromJsonString(ModelSelection),
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

const TerminalDeliveryClaimRowSchema = Schema.Struct({
  childThreadId: Schema.String,
  parentThreadId: Schema.String,
  claimId: Schema.String,
  claimedAt: Schema.String,
  claimedSequence: Schema.Number,
  terminalKind: Schema.Literals(["completed", "failed", "killed", "archived"]),
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

const orphanSettlementReason = (
  activity: ActivityAppendedEvent["payload"]["activity"],
): string | null => {
  if (
    activity.kind !== ORPHAN_SETTLED_ACTIVITY_KIND ||
    activity.payload === null ||
    typeof activity.payload !== "object" ||
    Array.isArray(activity.payload) ||
    !("reason" in activity.payload) ||
    typeof activity.payload.reason !== "string" ||
    !("status" in activity.payload) ||
    activity.payload.status !== "killed" ||
    (activity.payload.reason !== ORPHAN_ARCHIVED_PARENT_REASON &&
      activity.payload.reason !== ORPHAN_RETIRED_PARENT_REASON)
  ) {
    return null;
  }
  return activity.payload.reason;
};

const isDurableOrphanSettlement = (outcome: ChildTerminalOutcome): boolean =>
  outcome.status === "killed" &&
  (outcome.error === ORPHAN_ARCHIVED_PARENT_REASON ||
    outcome.error === ORPHAN_RETIRED_PARENT_REASON);

const clearFailedPendingTurnStart = (input: {
  readonly threadId: ThreadId;
  readonly failedRequestId: EventId | undefined;
  readonly activeTurns: Map<ThreadId, TurnId>;
  readonly pendingStarts: Map<ThreadId, EventId>;
  readonly pendingSameTurnStarts: Map<ThreadId, TurnId>;
}): boolean => {
  if (
    input.failedRequestId === undefined ||
    input.pendingStarts.get(input.threadId) !== input.failedRequestId
  ) {
    return false;
  }
  const preservedActiveTurnId = input.pendingSameTurnStarts.get(input.threadId);
  input.pendingStarts.delete(input.threadId);
  input.pendingSameTurnStarts.delete(input.threadId);
  if (preservedActiveTurnId !== undefined) {
    input.activeTurns.set(input.threadId, preservedActiveTurnId);
  }
  return true;
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
  if (thread.deletedAt !== null) {
    return nonSessionTerminalOutcome("killed", "thread deleted", thread.deletedAt);
  }
  if (
    (thread.session?.status === "running" || thread.session?.status === "waiting") &&
    thread.session.activeTurnId !== null
  ) {
    return null;
  }
  const latestTurn = thread.latestTurn;
  if (latestTurn?.state === "running") {
    if (thread.archivedAt !== null) {
      return nonSessionTerminalOutcome("killed", "thread archived", thread.archivedAt);
    }
    if (thread.session?.status === "error") {
      return sessionTerminalOutcome(thread.session);
    }
    if (thread.session?.status === "stopped") {
      return sessionTerminalOutcome(thread.session);
    }
    return null;
  }
  const terminalAt = latestTurnTerminalAt(latestTurn);
  const latestUserMessageAt = latestUserMessageAtFromThread(thread);
  const staleTerminal =
    terminalAt !== null && latestUserMessageAt !== null && latestUserMessageAt > terminalAt;
  const terminalAfterArchive =
    thread.archivedAt !== null &&
    terminalAt !== null &&
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
      ? nonSessionTerminalOutcome("killed", "thread archived", thread.archivedAt)
      : (latestTurnTerminal ??
          nonSessionTerminalOutcome("killed", "thread archived", thread.archivedAt));
  }
  if (thread.session?.status === "error") {
    return sessionTerminalOutcome(thread.session);
  }
  if (thread.session?.status === "stopped") {
    if (latestTurnTerminal !== null) return latestTurnTerminal;
    return sessionTerminalOutcome(thread.session);
  }
  return null;
};

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const orchestrationEngine = yield* OrchestrationEngineService;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const registry = yield* ProviderInstanceRegistry;
  const providerService = yield* ProviderService;
  const dispatchLimiter = yield* SubagentDispatchLimiter;
  const pendingDispatches = yield* PendingDispatchRepository;
  const sql = yield* SqlClient;

  const children = new Map<ThreadId, ChildRecord>();
  const byParent = new Map<ThreadId, Set<ThreadId>>();
  const pendingInjections = new Map<ThreadId, Array<PendingInjection>>();
  const archivedChildIds = new Set<ThreadId>();
  const archivedActiveChildIds = new Set<ThreadId>();
  const unarchivedTerminalChildIds = new Set<ThreadId>();
  const unarchivedTerminalMarkedAtByChild = new Map<ThreadId, string>();
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
  // An absent public parentTurnIdAtWait is legacy-compatible and means null.
  // Track unavailable internal reads by result identity so they remain distinct
  // without leaking a new sentinel through the service response shape.
  const unavailableParentTurnWaitResults = new WeakSet<WaitChildResult>();
  // Children that already have a durable parent wake row. On restart this lets
  // log reconciliation settle the child without creating a second wake row.
  const queuedWakeChildren = new Set<ThreadId>();
  // Local terminal lifecycles whose parent wake turn was durably accepted. The
  // tombstone survives pending-row deletion, so immutable-log replay cannot
  // synthesize the same completion/failure/kill/archive wake after restart.
  const terminalDeliveryClaims = new Map<ThreadId, TerminalDeliveryClaim>();
  // Last active provider turn observed for each child. Session-ready is only
  // terminal when the projected terminal turn matches this id.
  const activeTurnByChild = new Map<ThreadId, TurnId>();
  // Sequence cutoffs bind terminal wakes to child lifecycles. These are also
  // used to revoke an older claim if its delayed parent dispatch lands only
  // after a replacement lifecycle has already started.
  const latestChildEventSequence = new Map<ThreadId, number>();
  const latestAcceptedStartSequenceByChild = new Map<ThreadId, number>();
  const latestArchiveUnarchiveSequenceByChild = new Map<ThreadId, number>();
  const latestSettledTerminalByChild = new Map<
    ThreadId,
    { readonly result: ChildWaitResult; readonly sourceTerminalSequence: number | null }
  >();
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
  // Boot reconciliation classifies these children as unharvestable before any
  // terminal path runs. Suppression remains process-local because orphan
  // injections are never persisted and any stale rows are pruned during boot.
  const suppressParentWakeChildIds = new Set<ThreadId>();
  // Children whose current terminal state came from an orphan settlement. An
  // accepted replacement start supersedes that settlement, so this tracks which
  // suppressions are revocable; every other suppression is permanent.
  const orphanSettledChildIds = new Set<ThreadId>();
  // Reopening an orphan terminal at turn-start request time is provisional: the
  // provider may reject that exact request before a replacement lifecycle exists.
  // Retain the prior result so a correlated failure can restore it losslessly.
  const pendingOrphanSupersessionByChild = new Map<
    ThreadId,
    { readonly requestId: EventId; readonly result: ChildWaitResult }
  >();
  // A turn-start request is provisional until a session reports an active turn.
  // Keep the delivered claim and prior result so a correlated provider rejection
  // restores the old terminal lifecycle instead of replaying or losing it.
  const pendingTerminalDeliverySupersessionByChild = new Map<
    ThreadId,
    {
      readonly requestId: EventId;
      readonly result: ChildWaitResult;
      readonly sourceTerminalSequence: number | null;
      readonly hadQueuedWake: boolean;
    }
  >();
  const shouldTrackChildLifecycle = (threadId: ThreadId): boolean =>
    children.has(threadId) ||
    terminalDeliveryClaims.has(threadId) ||
    queuedWakeChildren.has(threadId) ||
    pendingTerminalDeliverySupersessionByChild.has(threadId);
  // Boot orphan cleanup keeps retrying its durable writes in forked fibers, and
  // the live event stream starts while those fibers are still sleeping between
  // attempts. A queued replacement request pauses those retries before worker
  // handling; a confirmed provider turn bumps the generation permanently, while
  // a correlated start failure unpauses the original cleanup.
  const orphanCleanupGenerationByChild = new Map<ThreadId, number>();
  const orphanCleanupPauseRequestByChild = new Map<ThreadId, EventId>();
  // Per-child guard so a deferred child_steer drain (R-C) is serialised and
  // never double-dispatches against the same child.
  const childSteerLocks = new Map<ThreadId, Semaphore.Semaphore>();
  let service: ChildThreadCoordinatorShape;

  const nowMillis = Effect.clockWith((clock) => clock.currentTimeMillis);
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const randomUUID = crypto.randomUUIDv4.pipe(Effect.orDie);
  const newCommandId = (tag: string) =>
    randomUUID.pipe(Effect.map((uuid) => CommandId.make(`server:${tag}:${uuid}`)));

  const markUnarchivedTerminalChild = (threadId: ThreadId, unarchivedAt: string | undefined) => {
    unarchivedTerminalChildIds.add(threadId);
    if (unarchivedAt !== undefined) {
      unarchivedTerminalMarkedAtByChild.set(threadId, unarchivedAt);
    }
  };
  const clearUnarchivedTerminalChild = (threadId: ThreadId) => {
    unarchivedTerminalChildIds.delete(threadId);
    unarchivedTerminalMarkedAtByChild.delete(threadId);
  };
  const markOrphanSettledChild = (threadId: ThreadId) => {
    const supersededRequestId = pendingOrphanSupersessionByChild.get(threadId)?.requestId;
    pendingOrphanSupersessionByChild.delete(threadId);
    if (
      supersededRequestId !== undefined &&
      orphanCleanupPauseRequestByChild.get(threadId) === supersededRequestId
    ) {
      orphanCleanupPauseRequestByChild.delete(threadId);
    }
    orphanSettledChildIds.add(threadId);
    suppressParentWakeChildIds.add(threadId);
  };
  const advanceOrphanCleanupGeneration = (threadId: ThreadId) => {
    orphanCleanupGenerationByChild.set(
      threadId,
      (orphanCleanupGenerationByChild.get(threadId) ?? 0) + 1,
    );
  };
  const clearOrphanSettledChild = (threadId: ThreadId) => {
    if (!orphanSettledChildIds.delete(threadId)) return;
    suppressParentWakeChildIds.delete(threadId);
  };
  const confirmOrphanSupersession = (threadId: ThreadId) => {
    const pending = pendingOrphanSupersessionByChild.get(threadId);
    if (pending === undefined) return;
    pendingOrphanSupersessionByChild.delete(threadId);
    if (orphanCleanupPauseRequestByChild.get(threadId) === pending.requestId) {
      orphanCleanupPauseRequestByChild.delete(threadId);
    }
    advanceOrphanCleanupGeneration(threadId);
  };
  const currentOrphanCleanupGeneration = (threadId: ThreadId): number =>
    orphanCleanupGenerationByChild.get(threadId) ?? 0;
  const waitUntilOrphanCleanupUnpaused = (threadId: ThreadId): Effect.Effect<void> =>
    Effect.suspend(() =>
      orphanCleanupPauseRequestByChild.has(threadId)
        ? Effect.sleep("50 millis").pipe(Effect.andThen(waitUntilOrphanCleanupUnpaused(threadId)))
        : Effect.void,
    );
  // Re-checked on every retry attempt rather than once at fork time: the window
  // this closes is the sleep between attempts, so a fork-time check would prove
  // nothing. `Effect.suspend` keeps the read inside the retried effect.
  const whileOrphanCleanupCurrent = <A, E, R>(
    threadId: ThreadId,
    generation: number,
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<Option.Option<A>, E, R> =>
    waitUntilOrphanCleanupUnpaused(threadId).pipe(
      Effect.andThen(
        Effect.suspend(() =>
          currentOrphanCleanupGeneration(threadId) !== generation
            ? Effect.succeedNone
            : Effect.asSome(effect),
        ),
      ),
    );
  const terminalProjectionAtOrBeforeUnarchive = (
    childThreadId: ThreadId,
    terminalAt: string | null | undefined,
  ): boolean => {
    const terminalAtMs = parseIsoMillis(terminalAt);
    const unarchivedAtMs = parseIsoMillis(unarchivedTerminalMarkedAtByChild.get(childThreadId));
    return terminalAtMs !== null && unarchivedAtMs !== null && terminalAtMs <= unarchivedAtMs;
  };
  const sessionProjectionIsStaleForUnarchive = (
    childThreadId: ThreadId,
    outcome: ChildTerminalOutcome,
  ): boolean =>
    unarchivedTerminalChildIds.has(childThreadId) &&
    outcome.status === "failed" &&
    outcome.fromSessionProjection === true &&
    terminalProjectionAtOrBeforeUnarchive(childThreadId, outcome.terminalAt);

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
          threads.thread_id AS "threadId",
          threads.parent_thread_id AS "parentThreadId",
          projects.data_audience AS "dataAudience",
          threads.model_selection_json AS "modelSelection"
        FROM projection_threads AS threads
        INNER JOIN projection_projects AS projects
          ON projects.project_id = threads.project_id
        WHERE threads.parent_thread_id IS NOT NULL
          AND threads.parent_environment_id IS NULL
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

  const listTerminalDeliveryClaimRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: TerminalDeliveryClaimRowSchema,
    execute: () =>
      sql`
        SELECT
          child_thread_id AS "childThreadId",
          parent_thread_id AS "parentThreadId",
          terminal_delivery_claim_id AS "claimId",
          terminal_delivery_claimed_at AS "claimedAt",
          terminal_delivery_claimed_sequence AS "claimedSequence",
          terminal_kind AS "terminalKind"
        FROM subagent_terminal_deliveries
      `,
  });

  const claimLocalTerminalDeliveryRows = SqlSchema.findAll({
    Request: Schema.Struct({
      dispatchIds: Schema.Array(Schema.String),
      claimId: Schema.String,
      claimedAt: Schema.String,
    }),
    Result: TerminalDeliveryClaimRowSchema,
    execute: ({ dispatchIds, claimId, claimedAt }) =>
      sql`
        INSERT INTO subagent_terminal_deliveries (
          child_thread_id,
          parent_thread_id,
          terminal_delivery_claim_id,
          terminal_delivery_claimed_at,
          terminal_delivery_claimed_sequence,
          terminal_kind
        )
        SELECT
          source_child_id,
          target_thread_id,
          ${claimId},
          ${claimedAt},
          source_terminal_sequence,
          CASE
            WHEN error = 'thread archived' THEN 'archived'
            ELSE status
          END
        FROM pending_dispatches
        WHERE ${sql.in("id", dispatchIds)}
          AND kind = 'parent_injection'
          AND source_child_id IS NOT NULL
          AND source_terminal_sequence IS NOT NULL
          AND source_child_id IN (
            SELECT thread_id
            FROM projection_threads
            WHERE parent_thread_id IS NOT NULL
              AND parent_environment_id IS NULL
          )
          AND NOT EXISTS (
            SELECT 1
            FROM orchestration_events AS newer_active_session
            WHERE newer_active_session.stream_id = pending_dispatches.source_child_id
              AND newer_active_session.event_type = 'thread.session-set'
              AND newer_active_session.sequence > pending_dispatches.source_terminal_sequence
              AND json_extract(
                newer_active_session.payload_json,
                '$.session.activeTurnId'
              ) IS NOT NULL
          )
          AND NOT EXISTS (
            SELECT 1
            FROM orchestration_events AS latest_turn_start
            WHERE latest_turn_start.event_id = (
              SELECT candidate_turn_start.event_id
              FROM orchestration_events AS candidate_turn_start
              WHERE candidate_turn_start.stream_id = pending_dispatches.source_child_id
                AND candidate_turn_start.event_type = 'thread.turn-start-requested'
                AND candidate_turn_start.sequence > pending_dispatches.source_terminal_sequence
              ORDER BY candidate_turn_start.sequence DESC
              LIMIT 1
            )
              AND NOT EXISTS (
                SELECT 1
                FROM orchestration_events AS correlated_start_failure
                WHERE correlated_start_failure.stream_id = pending_dispatches.source_child_id
                  AND correlated_start_failure.event_type = 'thread.activity-appended'
                  AND correlated_start_failure.sequence > latest_turn_start.sequence
                  AND json_extract(
                    correlated_start_failure.payload_json,
                    '$.activity.kind'
                  ) = 'provider.turn.start.failed'
                  AND (
                    json_extract(
                      correlated_start_failure.payload_json,
                      '$.activity.payload.turnStartRequestId'
                    ) = latest_turn_start.event_id
                    OR (
                      COALESCE(
                        json_type(
                          correlated_start_failure.payload_json,
                          '$.activity.payload.turnStartRequestId'
                        ),
                        ''
                      ) <> 'text'
                      AND NOT EXISTS (
                        SELECT 1
                        FROM orchestration_events AS prior_unresolved_turn_start
                        WHERE prior_unresolved_turn_start.stream_id =
                            pending_dispatches.source_child_id
                          AND prior_unresolved_turn_start.event_type =
                            'thread.turn-start-requested'
                          AND prior_unresolved_turn_start.sequence >
                            pending_dispatches.source_terminal_sequence
                          AND prior_unresolved_turn_start.sequence < latest_turn_start.sequence
                          AND NOT EXISTS (
                            SELECT 1
                            FROM orchestration_events AS prior_correlated_start_failure
                            WHERE prior_correlated_start_failure.stream_id =
                                pending_dispatches.source_child_id
                              AND prior_correlated_start_failure.event_type =
                                'thread.activity-appended'
                              AND prior_correlated_start_failure.sequence >
                                prior_unresolved_turn_start.sequence
                              AND prior_correlated_start_failure.sequence <
                                latest_turn_start.sequence
                              AND json_extract(
                                prior_correlated_start_failure.payload_json,
                                '$.activity.kind'
                              ) = 'provider.turn.start.failed'
                              AND json_extract(
                                prior_correlated_start_failure.payload_json,
                                '$.activity.payload.turnStartRequestId'
                              ) = prior_unresolved_turn_start.event_id
                          )
                      )
                    )
                  )
              )
          )
          AND NOT EXISTS (
            SELECT 1
            FROM orchestration_events AS newer_unarchive
            WHERE pending_dispatches.error = 'thread archived'
              AND newer_unarchive.stream_id = pending_dispatches.source_child_id
              AND newer_unarchive.event_type = 'thread.unarchived'
              AND newer_unarchive.sequence > pending_dispatches.source_terminal_sequence
          )
        ON CONFLICT (child_thread_id) DO UPDATE SET
          parent_thread_id = excluded.parent_thread_id,
          terminal_delivery_claim_id = excluded.terminal_delivery_claim_id,
          terminal_delivery_claimed_at = excluded.terminal_delivery_claimed_at,
          terminal_delivery_claimed_sequence = excluded.terminal_delivery_claimed_sequence,
          terminal_kind = excluded.terminal_kind
        WHERE excluded.terminal_delivery_claimed_sequence >=
          subagent_terminal_deliveries.terminal_delivery_claimed_sequence
        RETURNING
          child_thread_id AS "childThreadId",
          parent_thread_id AS "parentThreadId",
          terminal_delivery_claim_id AS "claimId",
          terminal_delivery_claimed_at AS "claimedAt",
          terminal_delivery_claimed_sequence AS "claimedSequence",
          terminal_kind AS "terminalKind"
      `,
  });

  const listPendingReplacementDispatchRows = SqlSchema.findAll({
    Request: Schema.Struct({ dispatchIds: Schema.Array(Schema.String) }),
    Result: Schema.Struct({ id: Schema.String }),
    execute: ({ dispatchIds }) =>
      sql`
        SELECT id
        FROM pending_dispatches
        WHERE ${sql.in("id", dispatchIds)}
          AND source_terminal_sequence IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM orchestration_events AS latest_turn_start
            WHERE latest_turn_start.event_id = (
              SELECT candidate_turn_start.event_id
              FROM orchestration_events AS candidate_turn_start
              WHERE candidate_turn_start.stream_id = pending_dispatches.source_child_id
                AND candidate_turn_start.event_type = 'thread.turn-start-requested'
                AND candidate_turn_start.sequence > pending_dispatches.source_terminal_sequence
              ORDER BY candidate_turn_start.sequence DESC
              LIMIT 1
            )
              AND NOT EXISTS (
                SELECT 1
                FROM orchestration_events AS correlated_start_failure
                WHERE correlated_start_failure.stream_id = pending_dispatches.source_child_id
                  AND correlated_start_failure.event_type = 'thread.activity-appended'
                  AND correlated_start_failure.sequence > latest_turn_start.sequence
                  AND json_extract(
                    correlated_start_failure.payload_json,
                    '$.activity.kind'
                  ) = 'provider.turn.start.failed'
                  AND (
                    json_extract(
                      correlated_start_failure.payload_json,
                      '$.activity.payload.turnStartRequestId'
                    ) = latest_turn_start.event_id
                    OR (
                      COALESCE(
                        json_type(
                          correlated_start_failure.payload_json,
                          '$.activity.payload.turnStartRequestId'
                        ),
                        ''
                      ) <> 'text'
                      AND NOT EXISTS (
                        SELECT 1
                        FROM orchestration_events AS prior_unresolved_turn_start
                        WHERE prior_unresolved_turn_start.stream_id =
                            pending_dispatches.source_child_id
                          AND prior_unresolved_turn_start.event_type =
                            'thread.turn-start-requested'
                          AND prior_unresolved_turn_start.sequence >
                            pending_dispatches.source_terminal_sequence
                          AND prior_unresolved_turn_start.sequence < latest_turn_start.sequence
                          AND NOT EXISTS (
                            SELECT 1
                            FROM orchestration_events AS prior_correlated_start_failure
                            WHERE prior_correlated_start_failure.stream_id =
                                pending_dispatches.source_child_id
                              AND prior_correlated_start_failure.event_type =
                                'thread.activity-appended'
                              AND prior_correlated_start_failure.sequence >
                                prior_unresolved_turn_start.sequence
                              AND prior_correlated_start_failure.sequence <
                                latest_turn_start.sequence
                              AND json_extract(
                                prior_correlated_start_failure.payload_json,
                                '$.activity.kind'
                              ) = 'provider.turn.start.failed'
                              AND json_extract(
                                prior_correlated_start_failure.payload_json,
                                '$.activity.payload.turnStartRequestId'
                              ) = prior_unresolved_turn_start.event_id
                          )
                      )
                    )
                  )
              )
          )
      `,
  });

  const listLifecycleCurrentDispatchRows = SqlSchema.findAll({
    Request: Schema.Struct({ dispatchIds: Schema.Array(Schema.String) }),
    Result: Schema.Struct({ id: Schema.String }),
    execute: ({ dispatchIds }) =>
      sql`
        SELECT id
        FROM pending_dispatches
        WHERE ${sql.in("id", dispatchIds)}
          AND (
            source_terminal_sequence IS NULL
            OR (
              NOT EXISTS (
                SELECT 1
                FROM orchestration_events AS newer_active_session
                WHERE newer_active_session.stream_id = pending_dispatches.source_child_id
                  AND newer_active_session.event_type = 'thread.session-set'
                  AND newer_active_session.sequence > pending_dispatches.source_terminal_sequence
                  AND json_extract(
                    newer_active_session.payload_json,
                    '$.session.activeTurnId'
                  ) IS NOT NULL
              )
              AND NOT EXISTS (
                SELECT 1
                FROM orchestration_events AS newer_unarchive
                WHERE pending_dispatches.error = 'thread archived'
                  AND newer_unarchive.stream_id = pending_dispatches.source_child_id
                  AND newer_unarchive.event_type = 'thread.unarchived'
                  AND newer_unarchive.sequence > pending_dispatches.source_terminal_sequence
              )
            )
          )
      `,
  });

  const requirePendingWakeLifecyclesCurrentAtAcceptance = Effect.fn(
    "ChildThreadCoordinator.requirePendingWakeLifecyclesCurrentAtAcceptance",
  )(function* (entries: ReadonlyArray<PendingInjection>) {
    const dispatchIds = entries.map((entry) => entry.dispatchId);
    const [currentRows, pendingReplacementRows] = yield* Effect.all(
      [
        listLifecycleCurrentDispatchRows({ dispatchIds }),
        listPendingReplacementDispatchRows({ dispatchIds }),
      ],
      { concurrency: "unbounded" },
    ).pipe(
      Effect.mapError(
        toPersistenceSqlError(
          "ChildThreadCoordinator.requirePendingWakeLifecyclesCurrentAtAcceptance",
        ),
      ),
    );
    const currentDispatchIds = new Set(currentRows.map((row) => row.id));
    const pendingReplacementDispatchIds = new Set(pendingReplacementRows.map((row) => row.id));
    const supersededDispatchIds = entries
      .filter(
        (entry) =>
          !currentDispatchIds.has(entry.dispatchId) ||
          pendingReplacementDispatchIds.has(entry.dispatchId),
      )
      .map((entry) => String(entry.dispatchId));
    if (supersededDispatchIds.length === 0) return;
    return yield* new OrchestrationCommandAcceptanceDeferredError({
      commandType: "thread.turn.start",
      detail: `Subagent terminal wake lifecycle is not dispatchable at command acceptance (${supersededDispatchIds.join(",")}).`,
    });
  });

  const deleteTerminalDeliveryClaimRows = (childThreadIds: ReadonlyArray<ThreadId>) =>
    childThreadIds.length === 0
      ? Effect.void
      : sql`
          DELETE FROM subagent_terminal_deliveries
          WHERE ${sql.in("child_thread_id", childThreadIds)}
        `.pipe(Effect.asVoid);

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
  const buildInjection = (
    parentThreadId: ThreadId,
    result: TerminalWakeInput,
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
        sourceTerminalSequence: result.sourceTerminalSequence,
        text: result.finalAssistantText,
        error: result.error,
        status: result.status,
        commandId: claimedCommandId,
        deliveredByWait,
        waitCancellable,
        createdAt: IsoDateTime.make(createdAt),
      };
      return {
        row,
        entry: {
          childThreadId: result.childThreadId,
          sourceTerminalSequence: result.sourceTerminalSequence,
          status: result.status,
          text: result.finalAssistantText,
          error: result.error,
          enqueuedAtMs: now,
          dispatchId: id,
          deliveredByWait,
          waitCancellable,
          claimedCommandId,
        } satisfies PendingInjection,
      };
    });

  const persistInjection = (
    parentThreadId: ThreadId,
    result: TerminalWakeInput,
    deliveredByWait: boolean,
    waitCancellable: boolean,
  ) =>
    Effect.gen(function* () {
      const injection = yield* buildInjection(
        parentThreadId,
        result,
        deliveredByWait,
        waitCancellable,
      );
      yield* pendingDispatches.insert(injection.row).pipe(Effect.orDie);
      queuedWakeChildren.add(result.childThreadId);
      return injection.entry;
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

  const pendingWakeLifecycleIsCurrent = (entry: PendingInjection): boolean => {
    const sourceTerminalSequence = entry.sourceTerminalSequence;
    if (sourceTerminalSequence === null) return true;
    const newerStartSequence = latestAcceptedStartSequenceByChild.get(entry.childThreadId);
    const newerArchiveUnarchiveSequence =
      entry.error === "thread archived"
        ? latestArchiveUnarchiveSequenceByChild.get(entry.childThreadId)
        : undefined;
    const unarchivedBeforeRegistration =
      entry.error === "thread archived" &&
      unarchivedTerminalChildIds.has(entry.childThreadId) &&
      newerArchiveUnarchiveSequence === undefined;
    return !(
      unarchivedBeforeRegistration ||
      (newerStartSequence !== undefined && newerStartSequence > sourceTerminalSequence) ||
      (newerArchiveUnarchiveSequence !== undefined &&
        newerArchiveUnarchiveSequence > sourceTerminalSequence)
    );
  };

  const pendingWakeHasProvisionalReplacement = (entry: PendingInjection): boolean => {
    if (entry.sourceTerminalSequence === null) return false;
    const pending = pendingTerminalDeliverySupersessionByChild.get(entry.childThreadId);
    return (
      pending !== undefined &&
      (pending.sourceTerminalSequence === null ||
        pending.sourceTerminalSequence === entry.sourceTerminalSequence)
    );
  };

  const discardSupersededWakeEntries = (
    entries: ReadonlyArray<PendingInjection>,
    replacementCoveredChildIds: ReadonlySet<ThreadId> = new Set(),
  ) =>
    Effect.gen(function* () {
      if (entries.length === 0) return;
      const replacementInjections: Array<{
        readonly parentThreadId: ThreadId;
        readonly row: PendingDispatch;
        readonly entry: PendingInjection;
      }> = [];
      const replacedChildIds = new Set<ThreadId>();
      for (const entry of entries) {
        if (replacementCoveredChildIds.has(entry.childThreadId)) continue;
        if (replacedChildIds.has(entry.childThreadId)) continue;
        const latestTerminal = latestSettledTerminalByChild.get(entry.childThreadId);
        if (
          entry.sourceTerminalSequence === null ||
          latestTerminal?.sourceTerminalSequence === null ||
          latestTerminal?.sourceTerminalSequence === undefined ||
          latestTerminal.sourceTerminalSequence <= entry.sourceTerminalSequence
        ) {
          continue;
        }
        const record = children.get(entry.childThreadId);
        if (record === undefined) continue;
        const replacement = yield* buildInjection(
          record.parentThreadId,
          {
            ...latestTerminal.result,
            sourceTerminalSequence: latestTerminal.sourceTerminalSequence,
          },
          false,
          false,
        );
        replacementInjections.push({ parentThreadId: record.parentThreadId, ...replacement });
        replacedChildIds.add(entry.childThreadId);
      }
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* deleteDispatchRows(entries.map((entry) => entry.dispatchId));
            yield* deletePromotedChildRows(promotedRowsToDeleteFor(entries)).pipe(Effect.orDie);
            for (const replacement of replacementInjections) {
              yield* pendingDispatches.insert(replacement.row).pipe(Effect.orDie);
            }
          }),
        )
        .pipe(Effect.orDie);
      yield* clearDeletedWakeStateMemory(entries);
      for (const entry of entries) {
        if (replacedChildIds.has(entry.childThreadId)) continue;
        const record = children.get(entry.childThreadId);
        if (record === undefined || !(yield* Deferred.isDone(record.terminal))) continue;
        const latestTerminal = latestSettledTerminalByChild.get(entry.childThreadId);
        if (
          entry.sourceTerminalSequence !== null &&
          latestTerminal?.sourceTerminalSequence !== null &&
          latestTerminal?.sourceTerminalSequence !== undefined &&
          latestTerminal.sourceTerminalSequence > entry.sourceTerminalSequence
        ) {
          continue;
        }
        const terminal = yield* Deferred.make<ChildWaitResult>();
        const detached =
          record.detached ||
          (!waitDeliveryMarkedAt.has(entry.childThreadId) &&
            !waitDeliveredPromotedChildren.has(entry.childThreadId) &&
            !promotedChildren.has(entry.childThreadId));
        children.set(entry.childThreadId, { ...record, detached, terminal });
        latestSettledTerminalByChild.delete(entry.childThreadId);
        clearUnarchivedTerminalChild(entry.childThreadId);
      }
      for (const replacement of replacementInjections) {
        queuedWakeChildren.add(replacement.entry.childThreadId);
        enqueuePending(replacement.parentThreadId, replacement.entry);
      }
    });

  const fencePendingEntriesBeforeDispatch = (entries: ReadonlyArray<PendingInjection>) =>
    Effect.gen(function* () {
      const memoryCurrentEntries = entries.filter(pendingWakeLifecycleIsCurrent);
      const durableCurrentRows =
        memoryCurrentEntries.length === 0
          ? []
          : yield* listLifecycleCurrentDispatchRows({
              dispatchIds: memoryCurrentEntries.map((entry) => entry.dispatchId),
            }).pipe(Effect.orDie);
      const durableCurrentIds = new Set(
        durableCurrentRows.map((row) => row.id as PendingDispatchId),
      );
      const lifecycleCurrentEntries = memoryCurrentEntries.filter((entry) =>
        durableCurrentIds.has(entry.dispatchId),
      );
      const lifecycleCurrentIds = new Set(lifecycleCurrentEntries.map((entry) => entry.dispatchId));
      const supersededEntries = entries.filter(
        (entry) => !lifecycleCurrentIds.has(entry.dispatchId),
      );
      yield* discardSupersededWakeEntries(
        supersededEntries,
        new Set(lifecycleCurrentEntries.map((entry) => entry.childThreadId)),
      );
      const durablePendingReplacementRows =
        lifecycleCurrentEntries.length === 0
          ? []
          : yield* listPendingReplacementDispatchRows({
              dispatchIds: lifecycleCurrentEntries.map((entry) => entry.dispatchId),
            }).pipe(Effect.orDie);
      const durablePendingReplacementIds = new Set(
        durablePendingReplacementRows.map((row) => row.id as PendingDispatchId),
      );
      const deferredEntries = lifecycleCurrentEntries.filter(
        (entry) =>
          pendingWakeHasProvisionalReplacement(entry) ||
          durablePendingReplacementIds.has(entry.dispatchId),
      );
      const deferredIds = new Set(deferredEntries.map((entry) => entry.dispatchId));
      return {
        dispatchableEntries: lifecycleCurrentEntries.filter(
          (entry) => !deferredIds.has(entry.dispatchId),
        ),
        deferredEntries,
      };
    });

  const markDispatchRowsDeliveredAndClearWakeState = (
    entries: ReadonlyArray<PendingInjection>,
    commandId: CommandId,
  ) =>
    Effect.gen(function* () {
      // Migration 053 intentionally leaves legacy rows without a lifecycle
      // sequence. Their claimed command id is the only durable dedupe marker,
      // so retain those rows after acceptance; restart re-dispatches the same id
      // and the engine receipt makes it a no-op. Sequence-bound rows can instead
      // become compact terminal-delivery tombstones and be deleted normally.
      const sequenceBoundEntries = entries.filter((entry) => entry.sourceTerminalSequence !== null);
      const lifecycleCurrentEntries = sequenceBoundEntries.filter(pendingWakeLifecycleIsCurrent);
      const claimableEntries = lifecycleCurrentEntries.filter(
        (entry) => !pendingWakeHasProvisionalReplacement(entry),
      );
      const claimableDispatchIds = new Set(claimableEntries.map((entry) => entry.dispatchId));
      const invalidatedDispatchIds = new Set(
        sequenceBoundEntries
          .filter((entry) => !pendingWakeLifecycleIsCurrent(entry))
          .map((entry) => entry.dispatchId),
      );
      const claimedAt = yield* nowIso;
      const replacementInjections: Array<{
        readonly parentThreadId: ThreadId;
        readonly row: PendingDispatch;
        readonly entry: PendingInjection;
      }> = [];
      const replacementCoveredChildIds = new Set(
        claimableEntries.map((entry) => entry.childThreadId),
      );
      const replacedChildIds = new Set<ThreadId>();
      for (const entry of entries) {
        if (claimableDispatchIds.has(entry.dispatchId) || entry.sourceTerminalSequence === null) {
          continue;
        }
        if (replacementCoveredChildIds.has(entry.childThreadId)) continue;
        if (replacedChildIds.has(entry.childThreadId)) continue;
        const latestTerminal = latestSettledTerminalByChild.get(entry.childThreadId);
        if (
          latestTerminal?.sourceTerminalSequence === null ||
          latestTerminal?.sourceTerminalSequence === undefined ||
          latestTerminal.sourceTerminalSequence <= entry.sourceTerminalSequence
        ) {
          continue;
        }
        const record = children.get(entry.childThreadId);
        if (record === undefined) continue;
        const replacement = yield* buildInjection(
          record.parentThreadId,
          {
            ...latestTerminal.result,
            sourceTerminalSequence: latestTerminal.sourceTerminalSequence,
          },
          false,
          false,
        );
        replacementInjections.push({ parentThreadId: record.parentThreadId, ...replacement });
        replacedChildIds.add(entry.childThreadId);
      }
      const { claimedRows, deletedEntries } = yield* sql.withTransaction(
        Effect.gen(function* () {
          let rows: ReadonlyArray<typeof TerminalDeliveryClaimRowSchema.Type> = [];
          if (claimableEntries.length > 0) {
            rows = yield* claimLocalTerminalDeliveryRows({
              dispatchIds: claimableEntries.map((entry) => entry.dispatchId),
              claimId: String(commandId),
              claimedAt,
            });
          }
          const coveredLifecycleKeys = new Set(
            rows.map((row) => JSON.stringify([row.childThreadId, row.claimedSequence])),
          );
          const entriesToDelete = sequenceBoundEntries.filter(
            (entry) =>
              coveredLifecycleKeys.has(
                JSON.stringify([entry.childThreadId, entry.sourceTerminalSequence]),
              ) || invalidatedDispatchIds.has(entry.dispatchId),
          );
          yield* deleteDispatchRows(entriesToDelete.map((entry) => entry.dispatchId));
          yield* deletePromotedChildRows(promotedRowsToDeleteFor(entriesToDelete)).pipe(
            Effect.orDie,
          );
          for (const replacement of replacementInjections) {
            yield* pendingDispatches.insert(replacement.row).pipe(Effect.orDie);
          }
          return { claimedRows: rows, deletedEntries: entriesToDelete };
        }),
      );
      for (const row of claimedRows) {
        terminalDeliveryClaims.set(row.childThreadId as ThreadId, {
          parentThreadId: row.parentThreadId as ThreadId,
          claimId: row.claimId,
          claimedAt: row.claimedAt,
          claimedSequence: row.claimedSequence,
          terminalKind: row.terminalKind,
        });
      }
      yield* clearDeletedWakeStateMemory(deletedEntries);
      for (const replacement of replacementInjections) {
        queuedWakeChildren.add(replacement.entry.childThreadId);
        enqueuePending(replacement.parentThreadId, replacement.entry);
      }
    }).pipe(Effect.orDie);

  const clearTerminalDeliveryClaim = (childThreadId: ThreadId) =>
    deleteTerminalDeliveryClaimRows([childThreadId]).pipe(
      Effect.orDie,
      Effect.andThen(Effect.sync(() => terminalDeliveryClaims.delete(childThreadId))),
    );

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

  const recordWaitDelivery = (
    parentThreadId: ThreadId,
    childThreadId: ThreadId,
    parentShell: OrchestrationThreadShell,
  ) =>
    Effect.gen(function* () {
      const existing = waitDeliveryMarkedAt.get(childThreadId);
      if (existing !== undefined) return existing;
      const latestTurn = parentShell.latestTurn;
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
    expectedParentTurnId: TurnId | null | undefined,
  ): Effect.Effect<"committed" | "refused-drain" | "refused-retain"> =>
    Effect.gen(function* () {
      if (expectedParentTurnId === undefined) {
        // Retaining the durable fallback is conservative: the parent-turn read
        // at wait time was unavailable, so no later snapshot can prove delivery.
        return "refused-retain";
      }
      const existing = waitDeliveryMarkedAt.get(childThreadId);
      const shellRead =
        existing === undefined ? yield* getThreadShellReadBounded(parentThreadId) : null;
      if (shellRead?._tag === "Unavailable") {
        // Retaining the durable fallback is conservative: an unavailable read
        // cannot prove which parent turn, if any, received the foreground result.
        yield* Effect.logWarning(
          "parent shell unavailable while committing wait delivery; retaining fallback",
          { parentThreadId, childThreadId, cause: shellRead.cause },
        );
        return "refused-retain";
      }
      const shellOption =
        shellRead?._tag === "Found" ? Option.some(shellRead.value) : Option.none();
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
          const entry = yield* persistInjection(
            record.parentThreadId,
            {
              ...result,
              sourceTerminalSequence: latestChildEventSequence.get(childThreadId) ?? null,
            },
            false,
            true,
          );
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
  const parentTurnIdAtWaitFromMark = (mark: WaitDeliveredMark): TurnId | null | undefined => {
    if (typeof mark === "string") return null;
    if (unavailableParentTurnWaitResults.has(mark)) return undefined;
    return mark.parentTurnIdAtWait ?? null;
  };

  const markPromotedWakeDeliveredByWait = (
    record: ChildRecord,
    childThreadId: ThreadId,
    expectedParentTurnId: TurnId | null | undefined,
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
          const persistedParentRows = yield* pendingDispatches
            .listByTarget({
              kind: "parent_injection",
              targetThreadId: record.parentThreadId,
            })
            .pipe(Effect.orDie);
          for (const row of persistedParentRows) {
            if (row.sourceChildId === childThreadId && !deliveredIds.includes(row.id)) {
              deliveredIds.push(row.id);
            }
          }
          if (terminalResult !== null && deliveredIds.length === 0) {
            const entry = yield* persistInjection(
              record.parentThreadId,
              {
                ...terminalResult,
                sourceTerminalSequence: latestChildEventSequence.get(childThreadId) ?? null,
              },
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

  const boundedProjectionRead = <A, E, R>(
    read: Effect.Effect<Option.Option<A>, E, R>,
  ): Effect.Effect<BoundedProjectionRead<A>, never, R> =>
    read.pipe(
      Effect.timeoutOption(`${PROJECTION_READ_TIMEOUT_MS} millis`),
      Effect.map(
        Option.match({
          onNone: () => ({ _tag: "Unavailable" as const, cause: "timeout" }),
          onSome: Option.match({
            onNone: () => ({ _tag: "Missing" as const }),
            onSome: (value) => ({ _tag: "Found" as const, value }),
          }),
        }),
      ),
      Effect.catchCause((cause) =>
        Effect.succeed({ _tag: "Unavailable" as const, cause: Cause.pretty(cause) }),
      ),
    );

  const getThreadShellIncludingArchivedBounded = (threadId: ThreadId) =>
    boundedProjectionRead(projectionSnapshotQuery.getThreadShellByIdIncludingArchived(threadId));

  const getThreadDetail = (threadId: ThreadId) =>
    projectionSnapshotQuery.getThreadDetailById(threadId).pipe(Effect.orDie);

  const getThreadShellReadBounded = (threadId: ThreadId) =>
    boundedProjectionRead(projectionSnapshotQuery.getThreadShellById(threadId));

  // Compatibility view for callers where both missing and unavailable already
  // take the same conservative direction: stay pending, retain the lease, or
  // preserve a queued wake. Callers that act on absence must use the tagged read.
  const getThreadShellBounded = (threadId: ThreadId) =>
    getThreadShellReadBounded(threadId).pipe(
      Effect.map((read) => (read._tag === "Found" ? Option.some(read.value) : Option.none())),
    );

  const runningParentTurnIdForWait = (
    parentThreadId: ThreadId,
  ): Effect.Effect<TurnId | null | undefined> =>
    getThreadShellReadBounded(parentThreadId).pipe(
      Effect.map((shellRead) => {
        if (shellRead._tag === "Unavailable") {
          // Undefined preserves uncertainty through wait delivery: a later read
          // must not credit an unknown parent turn with receiving this result.
          return undefined;
        }
        if (shellRead._tag === "Missing") return null;
        const latestTurn = shellRead.value.latestTurn;
        return latestTurn?.state === "running" ? latestTurn.turnId : null;
      }),
    );

  const getThreadShellForDrain = (threadId: ThreadId) =>
    getThreadShell(threadId).pipe(
      Effect.timeoutOption(`${PROJECTION_READ_TIMEOUT_MS} millis`),
      Effect.catchCause(() => Effect.succeed(Option.none())),
    );

  const getThreadDetailReadBounded = (threadId: ThreadId) =>
    boundedProjectionRead(projectionSnapshotQuery.getThreadDetailById(threadId));

  // Compatibility view, mirroring getThreadShellBounded: only for callers where
  // a missing row and an unavailable read already take the same conservative
  // direction (return without settling). Callers that act terminally on absence
  // must use the tagged read so a timeout or defect cannot pose as a real
  // negative.
  const getThreadDetailBounded = (threadId: ThreadId) =>
    getThreadDetailReadBounded(threadId).pipe(
      Effect.map((read) => (read._tag === "Found" ? Option.some(read.value) : Option.none())),
    );

  const getThreadDetailForBoot = getThreadDetailReadBounded;

  const listBootRuntimeSessions = () =>
    providerService.listSessions().pipe(
      Effect.timeoutOption(`${BOOT_EXTERNAL_CALL_TIMEOUT_MS} millis`),
      Effect.map(
        Option.match({
          onNone: () => ({ _tag: "Unavailable" as const, cause: "timeout" }),
          onSome: (sessions) => ({ _tag: "Available" as const, sessions }),
        }),
      ),
      Effect.catchCause((cause) =>
        Effect.succeed({ _tag: "Unavailable" as const, cause: Cause.pretty(cause) }),
      ),
    );

  const stopBootProviderSession = (threadId: ThreadId) =>
    providerService.stopSession({ threadId }).pipe(
      Effect.timeoutOption(`${BOOT_EXTERNAL_CALL_TIMEOUT_MS} millis`),
      Effect.map(
        Option.match({
          onNone: () => ({ _tag: "Indeterminate" as const, cause: "timeout" }),
          onSome: () => ({ _tag: "Stopped" as const }),
        }),
      ),
      Effect.catchCause((cause) =>
        Effect.succeed({ _tag: "Indeterminate" as const, cause: Cause.pretty(cause) }),
      ),
    );

  const getProviderInstanceForBoot = (instanceId: ModelSelection["instanceId"]) =>
    registry.getInstance(instanceId).pipe(
      Effect.timeoutOption(`${BOOT_EXTERNAL_CALL_TIMEOUT_MS} millis`),
      Effect.map(
        Option.match({
          onNone: () => ({ _tag: "Unavailable" as const, cause: "timeout" }),
          onSome: (instance) =>
            instance === undefined ? { _tag: "Missing" as const } : { _tag: "Found" as const },
        }),
      ),
      Effect.catchCause((cause) =>
        Effect.succeed({ _tag: "Unavailable" as const, cause: Cause.pretty(cause) }),
      ),
    );

  const recordOrphanSettlement = Effect.fn("recordOrphanSettlement")(function* (input: {
    readonly threadId: ThreadId;
    readonly reason: string;
    readonly createdAt: string;
    readonly commandId: CommandId;
    readonly activityId: EventId;
    readonly dataAudience: DataAudience;
  }) {
    yield* orchestrationEngine.dispatch(
      {
        type: "thread.activity.append",
        commandId: input.commandId,
        threadId: input.threadId,
        activity: {
          id: input.activityId,
          tone: "info",
          kind: ORPHAN_SETTLED_ACTIVITY_KIND,
          summary: "Sub-agent orphan settled",
          payload: { status: "killed", reason: input.reason },
          turnId: null,
          createdAt: input.createdAt,
        },
        createdAt: input.createdAt,
      },
      audienceBoundSystemDispatchAuthority({
        reason: "ChildThreadCoordinator",
        sourceThreadId: input.threadId,
        dataAudience: input.dataAudience,
      }),
    );
  });

  const recordStoppedOrphanSession = Effect.fn("recordStoppedOrphanSession")(function* (input: {
    readonly threadId: ThreadId;
    readonly projectedSession: OrchestrationThread["session"];
    readonly runtimeSession: ProviderSession | undefined;
    readonly createdAt: string;
    readonly lastError: string;
    readonly commandId: CommandId;
    readonly dataAudience: DataAudience;
  }) {
    const providerInstanceId =
      input.projectedSession?.providerInstanceId ?? input.runtimeSession?.providerInstanceId;
    yield* orchestrationEngine.dispatch(
      {
        type: "thread.session.set",
        commandId: input.commandId,
        threadId: input.threadId,
        session: {
          threadId: input.threadId,
          status: "stopped",
          providerName:
            input.projectedSession?.providerName ?? input.runtimeSession?.provider ?? null,
          ...(providerInstanceId !== undefined ? { providerInstanceId } : {}),
          runtimeMode:
            input.projectedSession?.runtimeMode ??
            input.runtimeSession?.runtimeMode ??
            "full-access",
          activeTurnId: null,
          lastError: input.lastError,
          updatedAt: input.createdAt,
        },
        createdAt: input.createdAt,
      },
      audienceBoundSystemDispatchAuthority({
        reason: "ChildThreadCoordinator",
        sourceThreadId: input.threadId,
        dataAudience: input.dataAudience,
      }),
    );
  });

  const confirmOrphanProviderCleanup = Effect.fn("confirmOrphanProviderCleanup")(function* (input: {
    readonly threadId: ThreadId;
    readonly projectedSession: OrchestrationThread["session"];
    readonly projectionSessionReadUnavailable: boolean;
  }) {
    const snapshot = yield* listBootRuntimeSessions();
    const runtimeSession =
      snapshot._tag === "Available"
        ? snapshot.sessions.find((session) => session.threadId === input.threadId)
        : undefined;
    const requiresPhysicalStop = runtimeSession !== undefined || snapshot._tag === "Unavailable";
    if (requiresPhysicalStop) {
      const stopResult = yield* stopBootProviderSession(input.threadId);
      if (stopResult._tag === "Indeterminate") {
        return yield* Effect.fail(stopResult.cause);
      }
    }
    const requiresSessionRecording =
      input.projectionSessionReadUnavailable ||
      snapshot._tag === "Unavailable" ||
      runtimeSession !== undefined ||
      (input.projectedSession !== null && !isTerminalSessionProjection(input.projectedSession));
    return { runtimeSession, requiresSessionRecording };
  });

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
    sourceTerminalSequence: number | null = latestChildEventSequence.get(childThreadId) ?? null,
  ) =>
    Effect.gen(function* () {
      const record = children.get(childThreadId);
      if (!record) return;
      const claim = terminalDeliveryClaims.get(childThreadId);
      if (claim !== undefined) {
        const newerStartSequence = latestAcceptedStartSequenceByChild.get(childThreadId);
        const newerArchiveUnarchiveSequence =
          claim.terminalKind === "archived"
            ? latestArchiveUnarchiveSequenceByChild.get(childThreadId)
            : undefined;
        if (
          (newerStartSequence !== undefined && newerStartSequence > claim.claimedSequence) ||
          (newerArchiveUnarchiveSequence !== undefined &&
            newerArchiveUnarchiveSequence > claim.claimedSequence)
        ) {
          yield* clearTerminalDeliveryClaim(childThreadId);
        }
      }
      const terminalResult: ChildWaitResult = {
        childThreadId,
        status,
        finalAssistantText,
        error,
      };
      const settled = yield* Deferred.succeed(record.terminal, terminalResult);
      if (settled) {
        latestSettledTerminalByChild.set(childThreadId, {
          result: terminalResult,
          sourceTerminalSequence,
        });
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
        !suppressParentWakeChildIds.has(childThreadId) &&
        !terminalDeliveryClaims.has(childThreadId) &&
        !queuedWakeChildren.has(childThreadId)
      ) {
        yield* wakeParent(record, {
          childThreadId,
          status,
          finalAssistantText,
          error,
          sourceTerminalSequence,
        });
      }
    });

  // Settle a child terminal Deferred exactly once. Deferred.succeed is a no-op
  // when already settled, which makes every signal path idempotent.
  const settleChild = (
    childThreadId: ThreadId,
    status: ChildTerminalStatus,
    error: string | null,
    bounded = false,
    sourceTerminalSequence: number | null = latestChildEventSequence.get(childThreadId) ?? null,
  ) =>
    Effect.gen(function* () {
      const detail = yield* (bounded ? getThreadDetailBounded : getThreadDetail)(childThreadId);
      const finalAssistantText = Option.match(detail, {
        onNone: () => null,
        onSome: finalAssistantTextFromThread,
      });
      yield* completeChild(
        childThreadId,
        status,
        finalAssistantText,
        error,
        sourceTerminalSequence,
      );
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
    entries: ReadonlyArray<PendingInjection>,
  ) =>
    Effect.gen(function* () {
      const messageId = MessageId.make(yield* randomUUID);
      const createdAt = yield* nowIso;
      return yield* dispatchActive(
        {
          type: "thread.turn.start",
          commandId,
          threadId: shell.id,
          message: { messageId, role: "system", text, attachments: [] },
          runtimeMode: shell.runtimeMode,
          interactionMode: shell.interactionMode,
          createdAt,
        },
        threadAudienceSystemDispatchAuthority(shell, "ChildThreadCoordinator"),
        requirePendingWakeLifecyclesCurrentAtAcceptance(entries),
      );
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
      yield* dispatchActive(
        {
          type: "thread.turn.start",
          commandId,
          threadId: shell.id,
          message: { messageId, role: "user", text, attachments: [] },
          runtimeMode: shell.runtimeMode,
          interactionMode: shell.interactionMode,
          createdAt,
        },
        threadAudienceSystemDispatchAuthority(shell, "ChildThreadCoordinator"),
      );
    });

  const appendSubagentActivity = (
    parentThread: OrchestrationThreadShell,
    result: ChildWaitResult,
  ) =>
    Effect.gen(function* () {
      const commandId = yield* newCommandId("subagent-activity");
      const activityId = EventId.make(yield* randomUUID);
      const createdAt = yield* nowIso;
      yield* orchestrationEngine.dispatch(
        {
          type: "thread.activity.append",
          commandId,
          threadId: parentThread.id,
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
        },
        threadAudienceSystemDispatchAuthority(parentThread, "ChildThreadCoordinator"),
      );
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
    result: TerminalWakeInput,
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
              yield* recordWaitDelivery(parentThreadId, result.childThreadId, shellOption.value);
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
            let retryEntry: PendingInjection | null = entry;
            yield* Effect.gen(function* () {
              yield* claimDispatchRows([entry.dispatchId], commandId);
              const claimedEntry = { ...entry, claimedCommandId: commandId };
              retryEntry = claimedEntry;
              const { dispatchableEntries, deferredEntries } =
                yield* fencePendingEntriesBeforeDispatch([claimedEntry]);
              for (const deferredEntry of deferredEntries) {
                enqueuePending(parentThreadId, deferredEntry);
              }
              retryEntry = dispatchableEntries[0] ?? null;
              if (retryEntry === null) return;
              yield* dispatchParentTurn(shell, consolidatedInjectionText([retryEntry]), commandId, [
                retryEntry,
              ]);
              yield* markDispatchRowsDeliveredAndClearWakeState([retryEntry], commandId);
            }).pipe(
              Effect.catchCause((cause) => {
                if (retryEntry !== null) {
                  enqueuePending(parentThreadId, retryEntry);
                }
                return Effect.logWarning("subagent wake dispatch failed; enqueued injection", {
                  parentThreadId,
                  childThreadId: result.childThreadId,
                  cause: Cause.pretty(cause),
                });
              }),
            );
            return;
          }
          yield* appendSubagentActivity(shell, result).pipe(Effect.ignoreCause({ log: true }));
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
              yield* recordWaitDelivery(parentThreadId, entry.childThreadId, shell);
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
            let retryEntries = batch;
            return Effect.gen(function* () {
              yield* claimDispatchRows(ids, commandId);
              const claimedBatch = batch.map((entry) => ({
                ...entry,
                claimedCommandId: commandId,
              }));
              retryEntries = claimedBatch;
              const fencedEntries = yield* fencePendingEntriesBeforeDispatch(claimedBatch);
              const currentDispatchIds = new Set(
                [...fencedEntries.dispatchableEntries, ...fencedEntries.deferredEntries].map(
                  (entry) => entry.dispatchId,
                ),
              );
              retryEntries = claimedBatch.filter((entry) =>
                currentDispatchIds.has(entry.dispatchId),
              );
              if (fencedEntries.deferredEntries.length > 0) {
                // Every row above was durably claimed under one command id. A
                // partial dispatch would accept that shared receipt while
                // stranding the omitted row, so defer the whole current batch.
                for (const retryEntry of retryEntries) {
                  enqueuePending(parentThreadId, retryEntry);
                }
                return;
              }
              retryEntries = fencedEntries.dispatchableEntries;
              if (retryEntries.length === 0) return;
              yield* dispatchParentTurn(
                shell,
                consolidatedInjectionText(retryEntries),
                commandId,
                retryEntries,
              );
              yield* markDispatchRowsDeliveredAndClearWakeState(retryEntries, commandId);
            }).pipe(
              Effect.catchCause((cause) => {
                const restored = pendingInjections.get(parentThreadId) ?? [];
                pendingInjections.set(parentThreadId, [...retryEntries, ...restored]);
                return Effect.logWarning("subagent pending drain dispatch failed; re-enqueued", {
                  parentThreadId,
                  cause: Cause.pretty(cause),
                });
              }),
            );
          };

          // Claimed entries MUST be re-dispatched in their original command-id
          // groups so an un-landed multi-row batch is preserved and a landed one
          // is deduped. They can never be folded into a fresh consolidated batch
          // (which the engine has no receipt for). Unclaimed entries consolidate
          // into one turn under a fresh deterministic id.
          const claimed = entries.filter((entry) => entry.claimedCommandId !== null);
          const fresh = entries.filter((entry) => entry.claimedCommandId === null);
          const claimedBatches = new Map<CommandId, Array<PendingInjection>>();
          for (const entry of claimed) {
            const commandId = entry.claimedCommandId as CommandId;
            const batch = claimedBatches.get(commandId) ?? [];
            batch.push(entry);
            claimedBatches.set(commandId, batch);
          }
          for (const [commandId, batch] of claimedBatches) {
            yield* drainBatch(batch, commandId);
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
        if (archivedChildIds.has(threadId)) {
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
          const projectedSession = Option.isSome(shellOption) ? shellOption.value.session : null;
          const outcome =
            latestTurnMatchesEvent &&
            latestTurn !== null &&
            (turnState === "error" || turnState === "interrupted") &&
            (status === "missing" || status === "ready")
              ? turnTerminalOutcome(latestTurn, projectedSession)
              : nonSessionTerminalOutcome(
                  "failed",
                  status !== "missing" && status !== "ready"
                    ? `turn diff ${status}`
                    : turnState
                      ? `turn ${turnState}`
                      : `turn diff ${status}`,
                  event.payload.completedAt,
                );
          yield* settleChild(threadId, outcome.status, outcome.error);
        }
        if (!terminalDoneBeforeSettle) {
          clearUnarchivedTerminalChild(threadId);
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
      const reportedActiveTurnId = activeTurnReportedBySession(session);
      const tracksChildLifecycle = shouldTrackChildLifecycle(threadId);
      if (reportedActiveTurnId !== undefined && tracksChildLifecycle) {
        const sequence = (event as { sequence?: number }).sequence;
        if (typeof sequence === "number") {
          latestAcceptedStartSequenceByChild.set(threadId, sequence);
          const deliveryClaim = terminalDeliveryClaims.get(threadId);
          if (deliveryClaim !== undefined && sequence > deliveryClaim.claimedSequence) {
            yield* clearTerminalDeliveryClaim(threadId);
          }
        } else if (terminalDeliveryClaims.has(threadId)) {
          yield* clearTerminalDeliveryClaim(threadId);
        }
        if (queuedWakeChildren.has(threadId)) {
          // Live stream order proves every already-queued terminal wake predates
          // this accepted active session, even when registration has not yet
          // populated the in-memory child record.
          yield* discardQueuedChildWakes(threadId);
        }
      }
      const record = children.get(threadId);
      if (!record) return;
      if (reportedActiveTurnId !== undefined) {
        pendingTerminalDeliverySupersessionByChild.delete(threadId);
        // A real provider turn now exists, so a later failure activity for the
        // request cannot restore the orphan lifecycle it provisionally replaced.
        confirmOrphanSupersession(threadId);
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
      if (archivedChildIds.has(threadId)) {
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
          if (
            (turnState === "error" || turnState === "interrupted") &&
            latestTurn !== null &&
            Option.isSome(shellOption)
          ) {
            const outcome = turnTerminalOutcome(latestTurn, shellOption.value.session);
            yield* settleChild(threadId, outcome.status, outcome.error);
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
      const outcome = sessionTerminalOutcome(session);
      yield* settleChild(threadId, outcome.status, outcome.error);
    });

  const handleThreadDeleted = (event: ThreadDeletedEvent) =>
    Effect.gen(function* () {
      const { threadId } = event.payload;
      archivedChildIds.delete(threadId);
      archivedActiveChildIds.delete(threadId);
      clearUnarchivedTerminalChild(threadId);
      yield* dispatchLimiter.releaseForChild(threadId);
      if (children.has(threadId)) {
        yield* settleChild(threadId, "killed", "thread deleted");
      }
    });

  const handleThreadArchived = (event: ThreadArchivedEvent) =>
    Effect.gen(function* () {
      const { threadId, archivedAt } = event.payload;
      archivedChildIds.add(threadId);
      const detailRead = yield* getThreadDetailReadBounded(threadId);
      if (children.has(threadId)) {
        const projectedOutcome =
          detailRead._tag === "Found"
            ? projectedLifecycleTerminal({ ...detailRead.value, archivedAt })
            : null;
        if (projectedOutcome === null && detailRead._tag !== "Found") {
          const shell = yield* getThreadShellBounded(threadId);
          const shellOutcome = Option.isSome(shell)
            ? shellTerminalOutcome(threadId, shell.value)
            : null;
          if (shellOutcome !== null) {
            const terminalDetailRead = yield* getThreadDetailReadBounded(threadId);
            // The shell is positive evidence that this child reached a terminal
            // state, so the archive markers must be cleared either way. Leaving
            // them set would let the next turn-diff or session event settle the
            // child as killed/"thread archived" (see handleTurnDiffCompleted and
            // handleSessionSet), throwing away the outcome the shell just proved.
            archivedChildIds.delete(threadId);
            archivedActiveChildIds.delete(threadId);
            if (terminalDetailRead._tag === "Unavailable" && shellOutcome.status === "completed") {
              // An unavailable text read cannot prove the result was empty.
              // Delivering null would record a completed child with no output;
              // stay pending and let reconciliation settle it with the real text.
              return;
            }
            const finalAssistantText =
              terminalDetailRead._tag === "Found"
                ? finalAssistantTextFromThread(terminalDetailRead.value)
                : null;
            yield* completeChild(
              threadId,
              shellOutcome.status,
              finalAssistantText,
              shellOutcome.error,
            );
            return;
          }
          if (detailRead._tag === "Unavailable") {
            // No detail, no shell evidence: nothing here proves the child ended.
            // Killing it on an unavailable read would report a possibly-running
            // (or already completed) child as killed by the archive.
            archivedChildIds.delete(threadId);
            archivedActiveChildIds.add(threadId);
            return;
          }
        }
        const outcome: ChildTerminalOutcome =
          projectedOutcome ??
          nonSessionTerminalOutcome("killed", "thread archived", event.payload.archivedAt);
        if (isThreadArchivedOutcome(outcome) && projectedOutcome === null) {
          archivedActiveChildIds.add(threadId);
        } else {
          archivedActiveChildIds.delete(threadId);
        }
        if (!isThreadArchivedOutcome(outcome)) {
          archivedChildIds.delete(threadId);
        }
        const finalAssistantText =
          detailRead._tag === "Found" ? finalAssistantTextFromThread(detailRead.value) : null;
        yield* completeChild(threadId, outcome.status, finalAssistantText, outcome.error);
        return;
      }
      if (detailRead._tag !== "Found") return;
      const detail = detailRead;
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
      const archivedDispatchIds = new Set(archivedEntries.map((entry) => entry.dispatchId));
      const persistedChildWakeRows = (yield* pendingDispatches.listAll().pipe(Effect.orDie)).filter(
        (row) => row.kind === "parent_injection" && row.sourceChildId === childThreadId,
      );
      for (const row of persistedChildWakeRows) {
        const archivedWake = row.status === "killed" && row.error === "thread archived";
        if (!archivedWake) {
          retainedWakeChildIds.add(childThreadId);
          continue;
        }
        if (archivedDispatchIds.has(row.id)) continue;
        const createdAtMs = Date.parse(String(row.createdAt));
        archivedEntries.push({
          childThreadId,
          sourceTerminalSequence: row.sourceTerminalSequence ?? null,
          status: "killed",
          text: row.text,
          error: row.error,
          enqueuedAtMs: Number.isFinite(createdAtMs) ? createdAtMs : 0,
          dispatchId: row.id,
          deliveredByWait: row.deliveredByWait,
          waitCancellable: row.waitCancellable,
          claimedCommandId: row.commandId === null ? null : CommandId.make(row.commandId),
        });
        archivedDispatchIds.add(row.id);
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
      const sequence = (event as { sequence?: number }).sequence;
      if (typeof sequence === "number") {
        latestArchiveUnarchiveSequenceByChild.set(threadId, sequence);
      }
      const terminalDeliveryClaim = terminalDeliveryClaims.get(threadId);
      if (terminalDeliveryClaim?.terminalKind === "archived") {
        yield* clearTerminalDeliveryClaim(threadId);
      }
      const record = children.get(threadId);
      if (!record) {
        if (archivedActiveChildIds.has(threadId)) {
          const shell = yield* getThreadShellBounded(threadId);
          if (Option.isSome(shell) ? isProjectedChildActive(shell.value) : true) {
            yield* dispatchLimiter.seedChild(threadId);
          }
          archivedActiveChildIds.delete(threadId);
        }
        yield* discardQueuedArchivedWake(threadId);
        markUnarchivedTerminalChild(threadId, event.payload.updatedAt);
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
        markUnarchivedTerminalChild(threadId, event.payload.updatedAt);
        return;
      }
      const terminal = yield* Deferred.make<ChildWaitResult>();
      children.set(threadId, { ...record, terminal });
      clearUnarchivedTerminalChild(threadId);
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
      const startsNewLifecycle = !activeTurnByChild.has(threadId);
      const supersedesDeliveredTerminal =
        startsNewLifecycle && terminalDeliveryClaims.has(threadId);
      const supersedesQueuedTerminal = startsNewLifecycle && queuedWakeChildren.has(threadId);
      let done = yield* Deferred.isDone(record.terminal);
      const pendingTerminalDeliverySupersession =
        pendingTerminalDeliverySupersessionByChild.get(threadId);
      if (pendingTerminalDeliverySupersession !== undefined) {
        pendingTerminalDeliverySupersessionByChild.set(threadId, {
          ...pendingTerminalDeliverySupersession,
          requestId: event.eventId,
        });
      } else if ((supersedesDeliveredTerminal || supersedesQueuedTerminal) && done) {
        const latestTerminal = latestSettledTerminalByChild.get(threadId);
        pendingTerminalDeliverySupersessionByChild.set(threadId, {
          requestId: event.eventId,
          result: yield* Deferred.await(record.terminal),
          sourceTerminalSequence: latestTerminal?.sourceTerminalSequence ?? null,
          hadQueuedWake: supersedesQueuedTerminal,
        });
      }
      const pendingOrphanSupersession = pendingOrphanSupersessionByChild.get(threadId);
      if (pendingOrphanSupersession !== undefined) {
        // Overlapping requests still describe one provisional replacement. A
        // stale failure for an older request must not restore the orphan while
        // the newest request remains pending.
        pendingOrphanSupersessionByChild.set(threadId, {
          ...pendingOrphanSupersession,
          requestId: event.eventId,
        });
      }
      // An orphan settlement is superseded by an accepted replacement start the
      // same way an unarchived terminal is: the child is live again, so its
      // Deferred must reopen and its parent-wake suppression must be lifted.
      if (
        done &&
        (unarchivedTerminalChildIds.has(threadId) ||
          orphanSettledChildIds.has(threadId) ||
          supersedesDeliveredTerminal ||
          supersedesQueuedTerminal)
      ) {
        if (orphanSettledChildIds.has(threadId)) {
          pendingOrphanSupersessionByChild.set(threadId, {
            requestId: event.eventId,
            result: yield* Deferred.await(record.terminal),
          });
        }
        if (!supersedesQueuedTerminal) {
          yield* discardQueuedChildWakes(threadId);
        }
        const terminal = yield* Deferred.make<ChildWaitResult>();
        record = children.get(threadId) ?? record;
        record = { ...record, terminal };
        children.set(threadId, record);
        clearUnarchivedTerminalChild(threadId);
        clearOrphanSettledChild(threadId);
        latestSettledTerminalByChild.delete(threadId);
        done = false;
      }
      if (
        orphanCleanupPauseRequestByChild.get(threadId) === event.eventId &&
        !pendingOrphanSupersessionByChild.has(threadId)
      ) {
        // The stream observer saw this request while older worker items still
        // made the child look orphan-settled. Ordered handling has now proved it
        // is not an orphan supersession, so its request-specific pause is stale.
        orphanCleanupPauseRequestByChild.delete(threadId);
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
    Effect.gen(function* () {
      const { threadId, activity } = event.payload;
      if (!children.has(threadId)) return;
      const settledOrphanReason = orphanSettlementReason(activity);
      if (settledOrphanReason !== null) {
        markOrphanSettledChild(threadId);
        yield* settleChild(threadId, "killed", settledOrphanReason, true);
        return;
      }
      if (activity.kind === "provider.turn.start.failed") {
        const failedRequestId = failedTurnStartRequestId(activity);
        const cleared = clearFailedPendingTurnStart({
          threadId,
          failedRequestId,
          activeTurns: activeTurnByChild,
          pendingStarts: pendingTurnStartByChild,
          pendingSameTurnStarts: pendingSameTurnStartByChild,
        });
        const pendingOrphanSupersession = pendingOrphanSupersessionByChild.get(threadId);
        const pendingTerminalDeliverySupersession =
          pendingTerminalDeliverySupersessionByChild.get(threadId);
        if (
          cleared &&
          failedRequestId !== undefined &&
          pendingTerminalDeliverySupersession?.requestId === failedRequestId
        ) {
          pendingTerminalDeliverySupersessionByChild.delete(threadId);
          const record = children.get(threadId);
          const claim = terminalDeliveryClaims.get(threadId);
          if (record !== undefined) {
            yield* Deferred.succeed(record.terminal, pendingTerminalDeliverySupersession.result);
          }
          yield* dispatchLimiter.releaseForChild(threadId);
          latestSettledTerminalByChild.set(threadId, {
            result: pendingTerminalDeliverySupersession.result,
            sourceTerminalSequence:
              claim?.claimedSequence ?? pendingTerminalDeliverySupersession.sourceTerminalSequence,
          });
        }
        if (
          cleared &&
          failedRequestId !== undefined &&
          pendingOrphanSupersession?.requestId === failedRequestId
        ) {
          pendingOrphanSupersessionByChild.delete(threadId);
          if (orphanCleanupPauseRequestByChild.get(threadId) === failedRequestId) {
            orphanCleanupPauseRequestByChild.delete(threadId);
          }
          markOrphanSettledChild(threadId);
          yield* settleChild(
            threadId,
            pendingOrphanSupersession.result.status,
            pendingOrphanSupersession.result.error,
            true,
          );
        }
      }
    });

  const processEvent = (event: OrchestrationEvent) => {
    const rememberSequence = (threadId: ThreadId) => {
      const sequence = (event as { sequence?: number }).sequence;
      if (shouldTrackChildLifecycle(threadId) && typeof sequence === "number") {
        latestChildEventSequence.set(threadId, sequence);
      }
    };
    switch (event.type) {
      case "thread.turn-diff-completed": {
        rememberSequence(event.payload.threadId);
        return handleTurnDiffCompleted(event);
      }
      case "thread.turn-start-requested": {
        rememberSequence(event.payload.threadId);
        return handleTurnStartRequested(event);
      }
      case "thread.session-set": {
        rememberSequence(event.payload.threadId);
        return handleSessionSet(event);
      }
      case "thread.activity-appended": {
        rememberSequence(event.payload.threadId);
        return handleActivityAppended(event);
      }
      case "thread.archived": {
        rememberSequence(event.payload.threadId);
        return handleThreadArchived(event);
      }
      case "thread.unarchived": {
        rememberSequence(event.payload.threadId);
        return handleThreadUnarchived(event);
      }
      case "thread.deleted": {
        rememberSequence(event.payload.threadId);
        return handleThreadDeleted(event);
      }
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
      const settleProjectedSessionFailure = (
        outcome: ChildTerminalOutcome & { readonly fromSessionProjection: true },
      ) =>
        Effect.gen(function* () {
          if (
            unarchivedTerminalChildIds.has(childThreadId) &&
            terminalProjectionAtOrBeforeUnarchive(childThreadId, outcome.terminalAt)
          ) {
            return;
          }
          const result: ChildWaitResult = {
            childThreadId,
            status: "failed",
            finalAssistantText: null,
            error: outcome.error,
          };
          if (options?.prepareWaitFallback) {
            yield* ensurePromotedWaitFallback(record, childThreadId, result);
          }
          yield* settleChild(childThreadId, outcome.status, outcome.error, true);
        });
      if (shell.session?.status === "error") {
        yield* settleProjectedSessionFailure(sessionTerminalOutcome(shell.session));
        return;
      }
      if (shell.latestTurn === null) {
        if (shell.session?.status === "stopped" && canSettleProjectedStoppedSession) {
          yield* settleProjectedSessionFailure(sessionTerminalOutcome(shell.session));
        }
        return;
      }
      const terminalTurn = currentLiveProjectedTerminal(childThreadId, shell);
      const turnState = shell.latestTurn.state;
      if (terminalTurn === null) {
        if (shell.session?.status === "stopped" && canSettleProjectedStoppedSession) {
          yield* settleProjectedSessionFailure(sessionTerminalOutcome(shell.session));
          return;
        }
        if (turnState !== "running") return;
        const activeTurnId = shell.session?.activeTurnId ?? null;
        if (activeTurnId !== null && activeTurnId === shell.latestTurn.turnId) {
          activeTurnByChild.set(childThreadId, activeTurnId);
        }
        return;
      }
      const outcome = turnTerminalOutcome(terminalTurn, shell.session);
      if (sessionProjectionIsStaleForUnarchive(childThreadId, outcome)) {
        return;
      }
      pendingTurnStartByChild.delete(childThreadId);
      pendingSameTurnStartByChild.delete(childThreadId);
      activeTurnByChild.set(childThreadId, terminalTurn.turnId);
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
      const shellRead = yield* getThreadShellReadBounded(childThreadId);
      if (shellRead._tag === "Unavailable") {
        // Reject this attempt as retryable without claiming the relationship is
        // absent; read unavailability is not authoritative authorization evidence.
        return yield* fail(
          `Unable to verify thread ${childThreadId}'s parent because the projection is temporarily unavailable; retry.`,
        );
      }
      if (shellRead._tag === "Found" && shellRead.value.parentThreadId === parentThreadId) return;
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
          if (terminalDeliveryClaims.has(childThreadId)) continue;
          if (!promotedChildren.has(childThreadId)) {
            yield* ensurePromotedChild(record, childThreadId);
          }
          if (!queuedWakeChildren.has(childThreadId)) {
            const result = yield* completed.value;
            yield* wakeParent(record, {
              ...result,
              sourceTerminalSequence: latestChildEventSequence.get(childThreadId) ?? null,
            });
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
      input.dedupeKey === undefined
        ? { ...result, sourceTerminalSequence: null }
        : { ...result, dedupeKey: input.dedupeKey, sourceTerminalSequence: null },
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
        const shellRead = yield* getThreadShellReadBounded(childThreadId);
        // Re-check in case register() ran concurrently between the map lookup
        // and the (bounded) projection read; if so, fall through to the normal
        // tracked path rather than reporting a misleading error.
        if (!children.has(childThreadId)) {
          if (shellRead._tag === "Unavailable") {
            // Staying pending is conservative: a timed-out or failed read cannot
            // prove that this child is genuinely absent from the projection.
            return {
              childThreadId,
              status: "pending" as const,
              finalAssistantText: null,
              error: null,
            } satisfies WaitChildResult;
          }
          if (shellRead._tag === "Missing") {
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
      const attachParentTurnAtWait = (result: WaitChildResult): WaitChildResult => {
        if (parentTurnIdAtWait === undefined) {
          unavailableParentTurnWaitResults.add(result);
          return result;
        }
        return { ...result, parentTurnIdAtWait };
      };
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
          return attachParentTurnAtWait({
            childThreadId,
            status: "pending",
            finalAssistantText: null,
            error: null,
          });
        }
        return attachParentTurnAtWait({
          childThreadId,
          status: raced.status,
          finalAssistantText: raced.finalAssistantText,
          error: raced.error,
        });
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
          const timeoutResult: WaitChildResult = {
            childThreadId: result.childThreadId,
            status: "timeout",
            finalAssistantText: null,
            error: `wait exceeded budget`,
            ...(result.parentTurnIdAtWait === undefined
              ? {}
              : { parentTurnIdAtWait: result.parentTurnIdAtWait }),
          };
          if (unavailableParentTurnWaitResults.has(result)) {
            unavailableParentTurnWaitResults.add(timeoutResult);
          }
          return timeoutResult;
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
  const reconcileFromLog = (
    knownChildIds: Set<ThreadId>,
    queuedTerminalWakeByChild: ReadonlyMap<
      ThreadId,
      {
        readonly result: ChildWaitResult;
        readonly sourceTerminalSequence: number | null;
      }
    >,
  ) =>
    Effect.gen(function* () {
      const terminalByChild = new Map<ThreadId, ChildTerminalOutcome>();
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
      const unarchivedAtByChild = new Map<ThreadId, string>();
      const unarchivedTerminalStartedChildIds = new Set<ThreadId>();
      const postUnarchiveTerminalByStartedChild = new Map<ThreadId, ChildTerminalOutcome>();
      const rearchivedUnarchivedTerminalStartedChildIds = new Set<ThreadId>();
      const activeArchiveByReplayedChild = new Set<ThreadId>();
      const authoritativeDurableOrphanSettlementByChild = new Map<ThreadId, ChildTerminalOutcome>();
      const pendingOrphanSupersessionByReplayedChild = new Map<
        ThreadId,
        {
          readonly requestId: EventId;
          readonly outcome: ChildTerminalOutcome;
          readonly settledAtOrdinal: number;
        }
      >();
      const pendingTerminalDeliverySupersessionByReplayedChild = new Map<
        ThreadId,
        {
          readonly requestId: EventId;
          readonly result: ChildWaitResult;
          readonly outcome: ChildTerminalOutcome;
          readonly sourceTerminalSequence: number | null;
          readonly hadQueuedWake: boolean;
        }
      >();
      const supersededQueuedTerminalSequenceByReplayedChild = new Map<ThreadId, number>();
      // Log position of each durable orphan settlement and of each accepted
      // start, so descendant propagation can compare them. A local ordinal is
      // used rather than event.sequence: sequence is optional on the event
      // shape, and only the relative order within this replay matters.
      const durableOrphanSettlementOrdinalByChild = new Map<ThreadId, number>();
      const acceptedStartOrdinalByChild = new Map<ThreadId, number>();
      const acceptedStartSequenceByChild = new Map<ThreadId, number>();
      const acceptedArchiveUnarchiveSequenceByChild = new Map<ThreadId, number>();
      const lastSequenceByChild = new Map<ThreadId, number>();
      let replayOrdinal = 0;
      let maxSequence = 0;
      const rememberPostUnarchiveTerminal = (threadId: ThreadId, outcome: ChildTerminalOutcome) => {
        if (unarchivedTerminalStartedChildIds.has(threadId)) {
          postUnarchiveTerminalByStartedChild.set(threadId, outcome);
        }
      };
      const markLifecycleTerminal = (
        threadId: ThreadId,
        outcome: ChildTerminalOutcome,
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
      const restoreFailedOrphanSupersession = (threadId: ThreadId, failedRequestId: EventId) => {
        const pending = pendingOrphanSupersessionByReplayedChild.get(threadId);
        if (pending?.requestId !== failedRequestId) return;
        pendingOrphanSupersessionByReplayedChild.delete(threadId);
        authoritativeDurableOrphanSettlementByChild.set(threadId, pending.outcome);
        durableOrphanSettlementOrdinalByChild.set(threadId, pending.settledAtOrdinal);
        acceptedStartOrdinalByChild.delete(threadId);
        activeArchiveByReplayedChild.delete(threadId);
        markLifecycleTerminal(threadId, pending.outcome, { preserveExistingTerminal: false });
      };
      const restoreFailedTerminalDeliverySupersession = (
        threadId: ThreadId,
        failedRequestId: EventId,
      ) => {
        const pending = pendingTerminalDeliverySupersessionByReplayedChild.get(threadId);
        if (pending?.requestId !== failedRequestId) return;
        pendingTerminalDeliverySupersessionByReplayedChild.delete(threadId);
        acceptedStartOrdinalByChild.delete(threadId);
        activeArchiveByReplayedChild.delete(threadId);
        markLifecycleTerminal(threadId, pending.outcome, { preserveExistingTerminal: false });
      };
      yield* Stream.runForEach(orchestrationEngine.readEvents(0), (event) =>
        Effect.gen(function* () {
          replayOrdinal += 1;
          const sequence = (event as { sequence?: number }).sequence;
          if (typeof sequence === "number" && sequence > maxSequence) {
            maxSequence = sequence;
          }
          if (
            typeof sequence === "number" &&
            event.aggregateKind === "thread" &&
            knownChildIds.has(event.aggregateId as ThreadId)
          ) {
            lastSequenceByChild.set(event.aggregateId as ThreadId, sequence);
          }
          switch (event.type) {
            case "thread.turn-diff-completed": {
              const { threadId, status } = event.payload;
              if (!knownChildIds.has(threadId)) return;
              if (lifecycleTerminatedByChild.has(threadId)) return;
              if (activeArchiveByReplayedChild.has(threadId)) {
                markLifecycleTerminal(
                  threadId,
                  nonSessionTerminalOutcome("killed", "thread archived", event.payload.completedAt),
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
                const outcome = nonSessionTerminalOutcome(
                  "failed",
                  "turn diff missing",
                  event.payload.completedAt,
                );
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
                  ? nonSessionTerminalOutcome("completed", null, event.payload.completedAt)
                  : nonSessionTerminalOutcome(
                      "failed",
                      `turn diff ${status}`,
                      event.payload.completedAt,
                    );
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
              const startsNewLifecycle = !activeTurnByReplayedChild.has(threadId);
              const priorTerminal = terminalByChild.get(threadId);
              const pendingTerminalDeliverySupersession =
                pendingTerminalDeliverySupersessionByReplayedChild.get(threadId);
              if (pendingTerminalDeliverySupersession !== undefined) {
                pendingTerminalDeliverySupersessionByReplayedChild.set(threadId, {
                  ...pendingTerminalDeliverySupersession,
                  requestId: event.eventId,
                });
              } else if (startsNewLifecycle) {
                const queuedTerminalWake = queuedTerminalWakeByChild.get(threadId);
                const deliveryClaim = terminalDeliveryClaims.get(threadId);
                const sourceTerminalSequence =
                  deliveryClaim?.claimedSequence ??
                  queuedTerminalWake?.sourceTerminalSequence ??
                  null;
                const requestSequence =
                  typeof sequence === "number" ? sequence : Number.POSITIVE_INFINITY;
                if (
                  (queuedTerminalWake !== undefined || deliveryClaim !== undefined) &&
                  (sourceTerminalSequence === null || sourceTerminalSequence < requestSequence)
                ) {
                  const result =
                    queuedTerminalWake?.result ??
                    (priorTerminal === undefined
                      ? undefined
                      : {
                          childThreadId: threadId,
                          status: priorTerminal.status,
                          finalAssistantText: null,
                          error: priorTerminal.error,
                        });
                  const outcome =
                    priorTerminal ??
                    (result === undefined
                      ? undefined
                      : nonSessionTerminalOutcome(result.status, result.error, null));
                  if (result !== undefined && outcome !== undefined) {
                    pendingTerminalDeliverySupersessionByReplayedChild.set(threadId, {
                      requestId: event.eventId,
                      result,
                      outcome,
                      sourceTerminalSequence,
                      hadQueuedWake: queuedTerminalWake !== undefined,
                    });
                  }
                }
              }
              const pendingOrphanSupersession =
                pendingOrphanSupersessionByReplayedChild.get(threadId);
              if (pendingOrphanSupersession !== undefined) {
                pendingOrphanSupersessionByReplayedChild.set(threadId, {
                  ...pendingOrphanSupersession,
                  requestId: event.eventId,
                });
              }
              if (lifecycleTerminatedByChild.has(threadId)) {
                // A durable orphan settlement is the one lifecycle terminal a
                // replacement start may supersede; without this the supersession
                // below is unreachable and the child replays killed forever.
                if (!authoritativeDurableOrphanSettlementByChild.has(threadId)) return;
                pendingOrphanSupersessionByReplayedChild.set(threadId, {
                  requestId: event.eventId,
                  outcome: authoritativeDurableOrphanSettlementByChild.get(threadId)!,
                  settledAtOrdinal: durableOrphanSettlementOrdinalByChild.get(threadId)!,
                });
                lifecycleTerminatedByChild.delete(threadId);
              }
              if (activeArchiveByReplayedChild.has(threadId)) return;
              if (priorTerminal !== undefined && unarchivedTerminalStartedChildIds.has(threadId)) {
                postUnarchiveTerminalByStartedChild.set(threadId, priorTerminal);
              }
              if (pendingTurnStartByReplayedChild.has(threadId)) {
                ambiguousLegacyFailureByReplayedChild.add(threadId);
              } else {
                ambiguousLegacyFailureByReplayedChild.delete(threadId);
              }
              // A request arriving while a turn is still active is a steer
              // attached to that turn, not a new lifecycle -- recordPendingTurnStart
              // records it as a same-turn start. Only a request that begins a new
              // turn is replacement evidence; counting steers would let a steer
              // delivered after an ancestor settlement cancel inherited orphan
              // cleanup for a child whose turn started before it.
              recordPendingTurnStart({
                event,
                activeTurns: activeTurnByReplayedChild,
                pendingStarts: pendingTurnStartByReplayedChild,
                pendingSameTurnStarts: pendingSameTurnStartByReplayedChild,
              });
              // An accepted replacement start supersedes the prior orphan
              // lifecycle. A later orphan settlement will add the child again.
              authoritativeDurableOrphanSettlementByChild.delete(threadId);
              durableOrphanSettlementOrdinalByChild.delete(threadId);
              if (startsNewLifecycle) {
                acceptedStartOrdinalByChild.set(threadId, replayOrdinal);
              }
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
              const settledOrphanReason = orphanSettlementReason(activity);
              if (settledOrphanReason !== null) {
                pendingOrphanSupersessionByReplayedChild.delete(threadId);
                const outcome = nonSessionTerminalOutcome(
                  "killed",
                  settledOrphanReason,
                  activity.createdAt,
                );
                authoritativeDurableOrphanSettlementByChild.set(threadId, outcome);
                durableOrphanSettlementOrdinalByChild.set(threadId, replayOrdinal);
                markLifecycleTerminal(threadId, outcome, { preserveExistingTerminal: false });
                return;
              }
              if (lifecycleTerminatedByChild.has(threadId)) return;
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
              const cleared = clearFailedPendingTurnStart({
                threadId,
                failedRequestId,
                activeTurns: activeTurnByReplayedChild,
                pendingStarts: pendingTurnStartByReplayedChild,
                pendingSameTurnStarts: pendingSameTurnStartByReplayedChild,
              });
              if (cleared && failedRequestId !== undefined) {
                restoreFailedOrphanSupersession(threadId, failedRequestId);
                restoreFailedTerminalDeliverySupersession(threadId, failedRequestId);
              }
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
                    nonSessionTerminalOutcome("killed", "thread archived", session.updatedAt),
                    { preserveExistingTerminal: false },
                  );
                  activeArchiveByReplayedChild.delete(threadId);
                }
                return;
              }
              const reportedActiveTurnId = activeTurnReportedBySession(session);
              if (reportedActiveTurnId !== undefined) {
                if (typeof sequence === "number") {
                  acceptedStartSequenceByChild.set(threadId, sequence);
                }
                pendingOrphanSupersessionByReplayedChild.delete(threadId);
                const pendingTerminalDeliverySupersession =
                  pendingTerminalDeliverySupersessionByReplayedChild.get(threadId);
                if (
                  pendingTerminalDeliverySupersession?.hadQueuedWake === true &&
                  pendingTerminalDeliverySupersession.sourceTerminalSequence !== null
                ) {
                  supersededQueuedTerminalSequenceByReplayedChild.set(
                    threadId,
                    pendingTerminalDeliverySupersession.sourceTerminalSequence,
                  );
                }
                pendingTerminalDeliverySupersessionByReplayedChild.delete(threadId);
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
                const projectedSession = Option.isSome(shellOption)
                  ? shellOption.value.session
                  : null;
                if (latestTurn?.turnId !== expectedTurnId) return;
                if (latestTurn.state === "completed") {
                  const outcome = turnTerminalOutcome(latestTurn, projectedSession);
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
                  const outcome = turnTerminalOutcome(latestTurn, projectedSession);
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
                  const outcome = turnTerminalOutcome(
                    terminalTurn,
                    Option.isSome(shellOption) ? shellOption.value.session : null,
                  );
                  terminalByChild.set(threadId, outcome);
                  rememberPostUnarchiveTerminal(threadId, outcome);
                  runningByChild.set(threadId, false);
                  activeTurnByReplayedChild.delete(threadId);
                  pendingTurnStartByReplayedChild.delete(threadId);
                  pendingSameTurnStartByReplayedChild.delete(threadId);
                  missingDiffWhileRunningByReplayedChild.delete(threadId);
                  return;
                }
                const outcome = sessionTerminalOutcome(session);
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
                nonSessionTerminalOutcome("killed", "thread deleted", event.payload.deletedAt),
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
              const detailRead = yield* getThreadDetailReadBounded(threadId);
              if (detailRead._tag === "Unavailable") {
                const priorTerminal = terminalByChild.get(threadId);
                if (priorTerminal !== undefined) {
                  // The archive is terminal evidence, but the unavailable detail
                  // read cannot replace an already-proven outcome. Seal that
                  // outcome exactly as the successful-detail path does so later
                  // archive-cleanup session events cannot overwrite it.
                  markLifecycleTerminal(threadId, priorTerminal);
                  return;
                }
                // An unavailable read is not evidence that the thread is gone.
                // Treat the archive as active so live reconciliation decides,
                // rather than killing a child the detail may show as running.
                activeArchiveByReplayedChild.add(threadId);
                return;
              }
              if (detailRead._tag === "Missing") {
                markLifecycleTerminal(
                  threadId,
                  nonSessionTerminalOutcome("killed", "thread archived", archivedAt),
                );
                return;
              }
              const outcome = projectedLifecycleTerminal({ ...detailRead.value, archivedAt });
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
              unarchivedAtByChild.set(threadId, event.payload.updatedAt);
              if (archivedSinceLastUnarchiveByChild.has(threadId)) {
                unarchivedArchiveChildIds.add(threadId);
                archivedSinceLastUnarchiveByChild.delete(threadId);
              }
              postUnarchiveTerminalByStartedChild.delete(threadId);
              const terminal = terminalByChild.get(threadId);
              if (terminal?.status === "killed" && terminal.error === "thread archived") {
                if (typeof sequence === "number") {
                  acceptedArchiveUnarchiveSequenceByChild.set(threadId, sequence);
                }
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
        const cleared = clearFailedPendingTurnStart({
          threadId,
          failedRequestId,
          activeTurns: activeTurnByReplayedChild,
          pendingStarts: pendingTurnStartByReplayedChild,
          pendingSameTurnStarts: pendingSameTurnStartByReplayedChild,
        });
        if (cleared) {
          restoreFailedOrphanSupersession(threadId, failedRequestId);
          restoreFailedTerminalDeliverySupersession(threadId, failedRequestId);
        }
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
        unarchivedAtByChild,
        unarchivedTerminalStartedChildIds,
        postUnarchiveTerminalByStartedChild,
        rearchivedUnarchivedTerminalStartedChildIds,
        activeArchiveByReplayedChild,
        authoritativeDurableOrphanSettlementByChild,
        pendingOrphanSupersessionByReplayedChild,
        pendingTerminalDeliverySupersessionByReplayedChild,
        supersededQueuedTerminalSequenceByReplayedChild,
        durableOrphanSettlementOrdinalByChild,
        acceptedStartOrdinalByChild,
        acceptedStartSequenceByChild,
        acceptedArchiveUnarchiveSequenceByChild,
        lastSequenceByChild,
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
      const terminalDeliveryRows = yield* listTerminalDeliveryClaimRows().pipe(Effect.orDie);
      for (const row of terminalDeliveryRows) {
        terminalDeliveryClaims.set(row.childThreadId as ThreadId, {
          parentThreadId: row.parentThreadId as ThreadId,
          claimId: row.claimId,
          claimedAt: row.claimedAt,
          claimedSequence: row.claimedSequence,
          terminalKind: row.terminalKind,
        });
      }
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
      const queuedTerminalWakeByChild = new Map<
        ThreadId,
        {
          readonly result: ChildWaitResult;
          readonly sourceTerminalSequence: number | null;
        }
      >();
      for (const row of persisted) {
        if (row.kind !== "parent_injection" || row.sourceChildId === null) continue;
        const deliveredByWait = row.deliveredByWait || waitDeliveredChildIds.has(row.sourceChildId);
        pendingWakeChildIds.add(row.sourceChildId);
        queuedWakeChildren.add(row.sourceChildId);
        const queuedTerminalWake = {
          result: {
            childThreadId: row.sourceChildId,
            status: (row.status as ChildTerminalStatus | null) ?? "completed",
            finalAssistantText: row.text,
            error: row.error,
          },
          sourceTerminalSequence: row.sourceTerminalSequence ?? null,
        };
        const existingQueuedTerminalWake = queuedTerminalWakeByChild.get(row.sourceChildId);
        if (
          existingQueuedTerminalWake === undefined ||
          (queuedTerminalWake.sourceTerminalSequence ?? Number.NEGATIVE_INFINITY) >=
            (existingQueuedTerminalWake.sourceTerminalSequence ?? Number.NEGATIVE_INFINITY)
        ) {
          queuedTerminalWakeByChild.set(row.sourceChildId, queuedTerminalWake);
        }
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
      const dataAudienceByChild = new Map<ThreadId, DataAudience>();
      const projectedTerminalByChild = new Map<ThreadId, ChildTerminalOutcome>();
      const restoredDetailByChild = new Map<ThreadId, OrchestrationThread>();
      const projectionDetailUnavailableChildIds = new Set<ThreadId>();
      for (const row of rows) {
        if (row.parentThreadId === null) continue;
        const childThreadId = row.threadId as ThreadId;
        const parentThreadId = row.parentThreadId as ThreadId;
        dataAudienceByChild.set(childThreadId, row.dataAudience);
        const restoredPromoted =
          promotedParentByChild.get(childThreadId) === parentThreadId &&
          !stalePromotedChildIds.has(childThreadId);
        if (children.has(childThreadId)) continue;
        const detailRead = yield* getThreadDetailForBoot(childThreadId);
        if (detailRead._tag === "Missing") {
          // No detail row yet (projection lag): do NOT fabricate a model or
          // settle this child. It will be validated when it calls register().
          // Settling it here on a fabricated "unknown" instance would wrongly
          // kill a child that is still running (wake CRITICAL #2).
          continue;
        }
        if (detailRead._tag === "Unavailable") {
          projectionDetailUnavailableChildIds.add(childThreadId);
          yield* Effect.logWarning(
            "child detail unavailable during reconciliation; preserving conservative lease",
            { childThreadId, cause: detailRead.cause },
          );
        } else {
          const detail = detailRead.value;
          restoredDetailByChild.set(childThreadId, detail);
          const projectedTerminal = projectedLifecycleTerminal(detail);
          if (projectedTerminal !== null) {
            projectedTerminalByChild.set(childThreadId, projectedTerminal);
          }
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
          model: detailRead._tag === "Found" ? detailRead.value.modelSelection : row.modelSelection,
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

      // Every settlement on the boot path must re-read the child's detail under
      // the same bound as the reconciliation read above. An unbounded read here
      // would re-enter the projection that just timed out for this child and
      // block start() indefinitely. The boot snapshot is the fallback so a
      // bounded miss cannot silently downgrade a completed child to empty text.
      const settleBootChild = (
        childThreadId: ThreadId,
        status: ChildTerminalStatus,
        error: string | null,
      ) =>
        Effect.gen(function* () {
          const detailRead = yield* getThreadDetailForBoot(childThreadId);
          const detail =
            detailRead._tag === "Found"
              ? detailRead.value
              : restoredDetailByChild.get(childThreadId);
          yield* completeChild(
            childThreadId,
            status,
            detail === undefined ? null : finalAssistantTextFromThread(detail),
            error,
          );
        });

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
        unarchivedAtByChild: replayedUnarchivedAtByChild,
        unarchivedTerminalStartedChildIds: replayedUnarchivedTerminalStartedChildIds,
        postUnarchiveTerminalByStartedChild,
        rearchivedUnarchivedTerminalStartedChildIds,
        activeArchiveByReplayedChild,
        authoritativeDurableOrphanSettlementByChild,
        pendingOrphanSupersessionByReplayedChild,
        pendingTerminalDeliverySupersessionByReplayedChild,
        supersededQueuedTerminalSequenceByReplayedChild,
        durableOrphanSettlementOrdinalByChild,
        acceptedStartOrdinalByChild,
        acceptedStartSequenceByChild,
        acceptedArchiveUnarchiveSequenceByChild,
        lastSequenceByChild,
      } = yield* reconcileFromLog(knownChildIds, queuedTerminalWakeByChild);
      for (const [childThreadId, sequence] of lastSequenceByChild) {
        latestChildEventSequence.set(childThreadId, sequence);
      }
      for (const [childThreadId, sequence] of acceptedStartSequenceByChild) {
        latestAcceptedStartSequenceByChild.set(childThreadId, sequence);
      }
      for (const [childThreadId, sequence] of acceptedArchiveUnarchiveSequenceByChild) {
        latestArchiveUnarchiveSequenceByChild.set(childThreadId, sequence);
      }
      const supersededTerminalDeliveryChildIds: Array<ThreadId> = [];
      for (const [childThreadId, claim] of terminalDeliveryClaims) {
        const acceptedStartSequence = acceptedStartSequenceByChild.get(childThreadId);
        const acceptedArchiveUnarchiveSequence =
          acceptedArchiveUnarchiveSequenceByChild.get(childThreadId);
        if (
          (acceptedStartSequence !== undefined && acceptedStartSequence > claim.claimedSequence) ||
          (claim.terminalKind === "archived" &&
            acceptedArchiveUnarchiveSequence !== undefined &&
            acceptedArchiveUnarchiveSequence > claim.claimedSequence)
        ) {
          supersededTerminalDeliveryChildIds.push(childThreadId);
        }
      }
      if (supersededTerminalDeliveryChildIds.length > 0) {
        yield* deleteTerminalDeliveryClaimRows(supersededTerminalDeliveryChildIds).pipe(
          Effect.orDie,
        );
        for (const childThreadId of supersededTerminalDeliveryChildIds) {
          terminalDeliveryClaims.delete(childThreadId);
        }
      }
      const durableOrphanSettlementChildIds = new Set<ThreadId>();
      for (const [childThreadId, outcome] of authoritativeDurableOrphanSettlementByChild) {
        if (!isDurableOrphanSettlement(outcome)) continue;
        durableOrphanSettlementChildIds.add(childThreadId);
        // Delayed events from the settled lifecycle are not replacement evidence.
        // Preserve the durable outcome until an accepted start explicitly clears it.
        terminalByChild.set(childThreadId, outcome);
        // The durable settlement is authoritative even if the live parent was
        // later unarchived. Only a replayed replacement child start supersedes
        // it; changing live ancestry alone cannot make an old wake safe.
        markOrphanSettledChild(childThreadId);
      }
      for (const [childThreadId, turnId] of activeTurnByReplayedChild) {
        activeTurnByChild.set(childThreadId, turnId);
      }
      for (const [childThreadId, requestId] of pendingTurnStartByReplayedChild) {
        pendingTurnStartByChild.set(childThreadId, requestId);
      }
      for (const [childThreadId, pending] of pendingOrphanSupersessionByReplayedChild) {
        pendingOrphanSupersessionByChild.set(childThreadId, {
          requestId: pending.requestId,
          result: {
            childThreadId,
            status: pending.outcome.status,
            finalAssistantText: null,
            error: pending.outcome.error,
          },
        });
      }
      for (const [childThreadId, pending] of pendingTerminalDeliverySupersessionByReplayedChild) {
        const detail = restoredDetailByChild.get(childThreadId);
        pendingTerminalDeliverySupersessionByChild.set(childThreadId, {
          requestId: pending.requestId,
          result: {
            ...pending.result,
            finalAssistantText:
              pending.result.finalAssistantText ??
              (detail === undefined ? null : finalAssistantTextFromThread(detail)),
          },
          sourceTerminalSequence: pending.sourceTerminalSequence,
          hadQueuedWake: pending.hadQueuedWake,
        });
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
      const markReplayedUnarchivedTerminalChild = (childThreadId: ThreadId) =>
        markUnarchivedTerminalChild(childThreadId, replayedUnarchivedAtByChild.get(childThreadId));
      const shouldMarkProjectionUnarchivedTerminal = (childThreadId: ThreadId): boolean =>
        unarchivedArchiveChildIds.has(childThreadId) ||
        unarchivedArchivedTerminalChildIds.has(childThreadId);
      for (const childThreadId of replayedUnarchivedTerminalChildIds) {
        markReplayedUnarchivedTerminalChild(childThreadId);
      }
      for (const childThreadId of projectedTerminalByChild.keys()) {
        if (shouldMarkProjectionUnarchivedTerminal(childThreadId)) {
          markReplayedUnarchivedTerminalChild(childThreadId);
        }
      }
      const staleSessionProjectionForReplayUnarchive = (
        childThreadId: ThreadId,
        outcome: ChildTerminalOutcome,
      ): boolean => sessionProjectionIsStaleForUnarchive(childThreadId, outcome);
      const staleSessionProjectionChildIds = new Set<ThreadId>();
      for (const childThreadId of knownChildIds) {
        const replayedTerminal = terminalByChild.get(childThreadId);
        if (replayedTerminal !== undefined) {
          if (
            replayedUnarchivedTerminalChildIds.has(childThreadId) &&
            staleSessionProjectionForReplayUnarchive(childThreadId, replayedTerminal)
          ) {
            staleSessionProjectionChildIds.add(childThreadId);
          }
          continue;
        }
        const projectedTerminal = projectedTerminalByChild.get(childThreadId);
        if (
          projectedTerminal !== undefined &&
          shouldMarkProjectionUnarchivedTerminal(childThreadId) &&
          staleSessionProjectionForReplayUnarchive(childThreadId, projectedTerminal)
        ) {
          staleSessionProjectionChildIds.add(childThreadId);
        }
      }

      // Classify the full recorded ancestry before any boot settlement can wake
      // a parent. The DFS order settles ancestors before descendants; the shell
      // walk also covers ancestors that are not themselves restored children.
      // An inherited orphan reason carries the log position of the durable
      // settlement it came from, so a descendant can tell whether its own start
      // is a replacement accepted *after* that settlement. Reasons derived from
      // the live shell (archived/missing ancestor) have no log position and are
      // never superseded -- only a durable settlement is order-comparable.
      type OrphanClassification = {
        readonly reason: string | null;
        readonly settledAtOrdinal: number | null;
      };
      const LIVE_CLASSIFICATION: OrphanClassification = { reason: null, settledAtOrdinal: null };
      const shellClassification = (reason: string): OrphanClassification => ({
        reason,
        settledAtOrdinal: null,
      });
      const orphanReasonByThread = new Map<ThreadId, OrphanClassification>();
      // A durably settled orphan is an orphan for its own descendants too. The
      // ancestry walk below reads live shells, and an unarchived-then-restarted
      // ancestor reads back as live even though this child's lifecycle already
      // ended -- leaving a survivor grandchild that can only wake a Deferred
      // this boot already killed. Memoize the durable reason so the walk stops
      // at the settlement rather than at the live shell.
      for (const childThreadId of durableOrphanSettlementChildIds) {
        const outcome = terminalByChild.get(childThreadId);
        orphanReasonByThread.set(childThreadId, {
          reason: outcome?.error ?? ORPHAN_RETIRED_PARENT_REASON,
          settledAtOrdinal: durableOrphanSettlementOrdinalByChild.get(childThreadId) ?? null,
        });
      }
      let orphanReasonForThread: (
        threadId: ThreadId,
        ancestry: ReadonlySet<ThreadId>,
      ) => Effect.Effect<OrphanClassification>;
      orphanReasonForThread = (threadId, ancestry) =>
        Effect.gen(function* () {
          const memoized = orphanReasonByThread.get(threadId);
          if (memoized !== undefined) {
            return memoized;
          }
          if (ancestry.has(threadId)) {
            yield* Effect.logWarning(
              "orphan ancestry contains a cycle; treating ancestor as live",
              {
                threadId,
              },
            );
            orphanReasonByThread.set(threadId, LIVE_CLASSIFICATION);
            return LIVE_CLASSIFICATION;
          }
          const read = yield* getThreadShellIncludingArchivedBounded(threadId);
          if (read._tag === "Unavailable") {
            yield* Effect.logWarning(
              "orphan ancestry projection read unavailable; treating ancestor as live",
              {
                threadId,
                cause: read.cause,
              },
            );
            orphanReasonByThread.set(threadId, LIVE_CLASSIFICATION);
            return LIVE_CLASSIFICATION;
          }
          if (read._tag === "Missing") {
            const classification = shellClassification(ORPHAN_RETIRED_PARENT_REASON);
            orphanReasonByThread.set(threadId, classification);
            return classification;
          }
          if (read.value.archivedAt !== null) {
            const classification = shellClassification(ORPHAN_ARCHIVED_PARENT_REASON);
            orphanReasonByThread.set(threadId, classification);
            return classification;
          }
          const ancestorThreadId = read.value.parentThreadId;
          if (ancestorThreadId === null) {
            orphanReasonByThread.set(threadId, LIVE_CLASSIFICATION);
            return LIVE_CLASSIFICATION;
          }
          const nextAncestry = new Set(ancestry);
          nextAncestry.add(threadId);
          const classification = yield* orphanReasonForThread(ancestorThreadId, nextAncestry);
          orphanReasonByThread.set(threadId, classification);
          return classification;
        });

      // Classify a superseding replacement child from its own shell alone. No
      // ancestor recursion: the supersession already decided that the durable
      // settlement above it no longer applies. Only this thread's later fate
      // (archived, deleted) can still orphan its descendants.
      const replacementShellClassification = (threadId: ThreadId) =>
        Effect.gen(function* () {
          const read = yield* getThreadShellIncludingArchivedBounded(threadId);
          if (read._tag === "Unavailable") {
            yield* Effect.logWarning(
              "replacement orphan ancestry read unavailable; treating replacement as live",
              { threadId, cause: read.cause },
            );
            return LIVE_CLASSIFICATION;
          }
          if (read._tag === "Missing") {
            return shellClassification(ORPHAN_RETIRED_PARENT_REASON);
          }
          if (read.value.archivedAt !== null) {
            return shellClassification(ORPHAN_ARCHIVED_PARENT_REASON);
          }
          return LIVE_CLASSIFICATION;
        });

      const bootChildOrder: Array<ThreadId> = [];
      const orderedBootChildren = new Set<ThreadId>();
      const orderingBootChildren = new Set<ThreadId>();
      const orderBootChild = (childThreadId: ThreadId): void => {
        if (orderedBootChildren.has(childThreadId) || orderingBootChildren.has(childThreadId)) {
          return;
        }
        orderingBootChildren.add(childThreadId);
        const parentThreadId = children.get(childThreadId)?.parentThreadId;
        if (parentThreadId !== undefined && knownChildIds.has(parentThreadId)) {
          orderBootChild(parentThreadId);
        }
        orderingBootChildren.delete(childThreadId);
        orderedBootChildren.add(childThreadId);
        bootChildOrder.push(childThreadId);
      };
      for (const childThreadId of knownChildIds) orderBootChild(childThreadId);

      // The invariant every branch below maintains: a thread's classification is
      // the NEWEST durable orphan fact at or above it. Neither an own nor an
      // inherited settlement is unconditionally newer -- both
      // `own -> ancestor` and `ancestor -> own` orderings occur -- so the two
      // are compared by log ordinal rather than by which one is closer. A
      // shell-derived reason carries no ordinal because it describes the
      // ancestor's CURRENT state, which is newer than any logged event.
      const ownDurableSettlementOrdinal = (childThreadId: ThreadId): number | null =>
        durableOrphanSettlementChildIds.has(childThreadId)
          ? (durableOrphanSettlementOrdinalByChild.get(childThreadId) ?? null)
          : null;
      const memoizeNewestClassification = (
        childThreadId: ThreadId,
        inherited: OrphanClassification,
      ): void => {
        const ownOrdinal = ownDurableSettlementOrdinal(childThreadId);
        if (
          ownOrdinal !== null &&
          inherited.settledAtOrdinal !== null &&
          ownOrdinal >= inherited.settledAtOrdinal
        ) {
          // The pre-seeded own settlement is the newer fact; keep it.
          return;
        }
        orphanReasonByThread.set(childThreadId, inherited);
      };

      const orphanReasonByBootChild = new Map<ThreadId, string>();
      for (const childThreadId of bootChildOrder) {
        const record = children.get(childThreadId);
        if (record === undefined) continue;
        if (pendingOrphanSupersessionByReplayedChild.has(childThreadId)) {
          // Replay ended after the replacement request but before its provider
          // result. The current parent shell may still be archived because the
          // projection lags that request; treating it as current orphan evidence
          // would stop the replacement synchronously before the live failure or
          // active-session event can decide the provisional supersession.
          orphanReasonByThread.set(childThreadId, LIVE_CLASSIFICATION);
          continue;
        }
        const classification = yield* orphanReasonForThread(record.parentThreadId, new Set());
        if (classification.reason === null) continue;
        // Live processing revokes an inferred orphan settlement when a start is
        // accepted afterwards (handleTurnStartRequested), so replay must not
        // kill a descendant whose start is newer than the settlement it would
        // inherit -- that would discard a valid replacement turn and its lease.
        const settledAtOrdinal = classification.settledAtOrdinal;
        const startedAtOrdinal = acceptedStartOrdinalByChild.get(childThreadId);
        // A child holding its own durable settlement cannot be superseding: an
        // accepted start clears that settlement during replay, so a settlement
        // that survived is necessarily newer than the child's last start.
        if (
          ownDurableSettlementOrdinal(childThreadId) === null &&
          settledAtOrdinal !== null &&
          startedAtOrdinal !== undefined &&
          startedAtOrdinal > settledAtOrdinal
        ) {
          // The replacement makes this child live again relative to the
          // INHERITED settlement, so stop inheriting -- but the child's own
          // fate after that start still decides what it is to its descendants.
          // The shell decides: a child archived or deleted after the
          // replacement start is an orphan source in its own right, and
          // memoizing a blanket "live" here would shield its descendants from
          // boot cleanup and leak their leases.
          orphanReasonByThread.set(
            childThreadId,
            yield* replacementShellClassification(childThreadId),
          );
          continue;
        }
        orphanReasonByBootChild.set(childThreadId, classification.reason);
        markOrphanSettledChild(childThreadId);
        memoizeNewestClassification(childThreadId, classification);
      }

      const terminalTextByStartedChild = new Map<
        ThreadId,
        { readonly text: string | null; readonly textUnavailable: boolean }
      >();
      const terminalTextReadUnavailableChildIds = new Set<ThreadId>();
      for (const childThreadId of replayedUnarchivedTerminalStartedChildIds) {
        if (!terminalByChild.has(childThreadId)) continue;
        const detailRead = yield* getThreadDetailForBoot(childThreadId);
        if (detailRead._tag === "Unavailable") {
          terminalTextReadUnavailableChildIds.add(childThreadId);
        } else if (detailRead._tag === "Found") {
          const text = finalAssistantTextFromThread(detailRead.value);
          const latestTurn = detailRead.value.latestTurn;
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
      for (const childThreadId of staleSessionProjectionChildIds) {
        prunedArchivedWakeChildIds.add(childThreadId);
        if (waitDeliveredChildIds.has(childThreadId)) {
          deliveredWakeCleanupChildIds.add(childThreadId);
        }
        if (promotedParentByChild.has(childThreadId) || promotedChildren.has(childThreadId)) {
          promotedChildCleanupIds.add(childThreadId);
        }
      }
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
          row.sourceTerminalSequence !== undefined &&
          row.sourceTerminalSequence !== null &&
          supersededQueuedTerminalSequenceByReplayedChild.get(childThreadId) ===
            row.sourceTerminalSequence
        ) {
          return true;
        }
        // Orphan children never have a harvestable parent. Prune by the boot
        // classification, independent of historical result/error wording.
        if (
          orphanReasonByBootChild.has(childThreadId) ||
          durableOrphanSettlementChildIds.has(childThreadId)
        ) {
          return true;
        }
        if (staleSessionProjectionChildIds.has(childThreadId)) return true;
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
        if (terminalTextReadUnavailableChildIds.has(childThreadId)) {
          // Preserving the durable wake is conservative: an unavailable detail
          // read cannot prove that its text is stale or safe to replace.
          return false;
        }
        if (projectedText === undefined) return row.commandId === null;
        if (projectedText.textUnavailable && row.commandId !== null) return false;
        return row.text !== projectedText.text;
      });
      if (
        staleWakeRows.length > 0 ||
        deliveredWakeCleanupChildIds.size > 0 ||
        promotedChildCleanupIds.size > 0
      ) {
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
        if (remainingWakeChildIds.has(childThreadId)) {
          queuedWakeChildren.add(childThreadId);
        }
      }
      for (const [childThreadId, outcome] of terminalByChild) {
        if (staleSessionProjectionChildIds.has(childThreadId)) {
          // Limiter state is process-local and starts empty on boot. Keeping this
          // child in terminalByChild also skips survivor seeding below, so the
          // stale projection stays pending without consuming a dispatch lease.
          continue;
        }
        yield* settleBootChild(childThreadId, outcome.status, outcome.error);
      }
      for (const childThreadId of durableOrphanSettlementChildIds) {
        const outcome = terminalByChild.get(childThreadId);
        if (outcome === undefined) continue;
        const projectedSession = restoredDetailByChild.get(childThreadId)?.session ?? null;
        const projectionSessionReadUnavailable =
          projectionDetailUnavailableChildIds.has(childThreadId);
        if (
          !projectionSessionReadUnavailable &&
          (projectedSession === null || isTerminalSessionProjection(projectedSession))
        ) {
          continue;
        }
        // A durable orphan-settlement activity is written only after physical
        // cleanup is confirmed. Repair the secondary session projection if the
        // process crashed between those two durable writes. When the detail read
        // is unavailable, recording a redundant stop is the conservative direction.
        const repairSessionProjection = recordStoppedOrphanSession({
          threadId: childThreadId,
          projectedSession,
          runtimeSession: undefined,
          createdAt: yield* nowIso,
          lastError: outcome.error ?? ORPHAN_RETIRED_PARENT_REASON,
          commandId: yield* newCommandId("orphan-session-repair"),
          dataAudience: dataAudienceByChild.get(childThreadId)!,
        });
        const repaired = yield* repairSessionProjection.pipe(
          Effect.as(true),
          Effect.catchCause((cause) =>
            Effect.logWarning("durable orphan session projection repair failed; retrying", {
              childThreadId,
              cause: Cause.pretty(cause),
            }).pipe(Effect.as(false)),
          ),
        );
        if (!repaired) {
          const repairGeneration = currentOrphanCleanupGeneration(childThreadId);
          yield* Effect.forkScoped(
            whileOrphanCleanupCurrent(
              childThreadId,
              repairGeneration,
              repairSessionProjection,
            ).pipe(
              Effect.catchCause((cause) => Effect.fail(Cause.pretty(cause))),
              Effect.retry(Schedule.spaced("2 seconds")),
            ),
          );
        }
      }

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
          if (staleSessionProjectionChildIds.has(childThreadId)) {
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
          if (staleSessionProjectionChildIds.has(childThreadId)) {
            inactiveProjectionUnarchivedChildIds.add(childThreadId);
            continue;
          }
        }
        yield* settleBootChild(childThreadId, projectedTerminal.status, projectedTerminal.error);
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
      let bootRuntimeSessionsSnapshot:
        | { readonly _tag: "Available"; readonly sessions: ReadonlyArray<ProviderSession> }
        | { readonly _tag: "Unavailable"; readonly cause: string }
        | undefined;
      for (const childThreadId of bootChildOrder) {
        if (terminalByChild.has(childThreadId)) continue;
        const record = children.get(childThreadId);
        if (!record) continue;
        const done = yield* Deferred.isDone(record.terminal);
        if (done) continue;
        const orphanReason = orphanReasonByBootChild.get(childThreadId);
        if (orphanReason !== undefined) {
          yield* Effect.logWarning(
            "reconciled non-terminal sub-agent lost its harvestable parent; terminating",
            {
              childThreadId,
              parentThreadId: record.parentThreadId,
              reason: orphanReason,
            },
          );
          if (bootRuntimeSessionsSnapshot === undefined) {
            bootRuntimeSessionsSnapshot = yield* listBootRuntimeSessions();
            if (bootRuntimeSessionsSnapshot._tag === "Unavailable") {
              yield* Effect.logWarning(
                "orphan provider snapshot unavailable; attempting conservative cleanup",
                { cause: bootRuntimeSessionsSnapshot.cause },
              );
            }
          }
          const projectedSession = restoredDetailByChild.get(childThreadId)?.session ?? null;
          const projectionSessionReadUnavailable =
            projectionDetailUnavailableChildIds.has(childThreadId);
          const runtimeSession =
            bootRuntimeSessionsSnapshot._tag === "Available"
              ? bootRuntimeSessionsSnapshot.sessions.find(
                  (session) => session.threadId === childThreadId,
                )
              : undefined;
          const createdAt = yield* nowIso;
          const settlementInput = {
            threadId: childThreadId,
            reason: orphanReason,
            createdAt,
            commandId: yield* newCommandId("orphan-settlement"),
            activityId: EventId.make(yield* randomUUID),
            dataAudience: dataAudienceByChild.get(childThreadId)!,
          } as const;
          const sessionCommandId = yield* newCommandId("orphan-session-stop");
          // Captured before the retries below can be forked. Each of them keeps
          // running after the live stream starts, and each performs an
          // externally visible act -- stopping the provider session for this
          // thread, appending the durable settlement, clobbering the session
          // projection. Once a replacement start supersedes the settlement they
          // describe, that thread's provider session and projection belong to the
          // replacement, so every one of them must abandon instead of acting.
          const cleanupGeneration = currentOrphanCleanupGeneration(childThreadId);
          const retrySettlement = whileOrphanCleanupCurrent(
            childThreadId,
            cleanupGeneration,
            recordOrphanSettlement(settlementInput),
          ).pipe(
            Effect.catchCause((cause) => Effect.fail(Cause.pretty(cause))),
            Effect.retry(Schedule.spaced("2 seconds")),
          );
          const retryPhysicalCleanup = whileOrphanCleanupCurrent(
            childThreadId,
            cleanupGeneration,
            confirmOrphanProviderCleanup({
              threadId: childThreadId,
              projectedSession,
              projectionSessionReadUnavailable,
            }),
          ).pipe(
            Effect.catchCause((cause) => Effect.fail(Cause.pretty(cause))),
            Effect.retry(Schedule.spaced("2 seconds")),
          );
          const retrySessionRecording = (confirmation: {
            readonly runtimeSession: ProviderSession | undefined;
            readonly requiresSessionRecording: boolean;
          }) =>
            confirmation.requiresSessionRecording
              ? whileOrphanCleanupCurrent(
                  childThreadId,
                  cleanupGeneration,
                  recordStoppedOrphanSession({
                    threadId: childThreadId,
                    projectedSession,
                    runtimeSession: confirmation.runtimeSession,
                    createdAt,
                    lastError: orphanReason,
                    commandId: sessionCommandId,
                    dataAudience: dataAudienceByChild.get(childThreadId)!,
                  }),
                ).pipe(
                  Effect.catchCause((cause) => Effect.fail(Cause.pretty(cause))),
                  Effect.retry(Schedule.spaced("2 seconds")),
                )
              : Effect.void;
          // Recording a redundant stop is conservative when detail is unavailable:
          // silence cannot prove the projection has no live session to clear.
          const requiresSessionRecording =
            projectionSessionReadUnavailable ||
            bootRuntimeSessionsSnapshot._tag === "Unavailable" ||
            runtimeSession !== undefined ||
            (projectedSession !== null && !isTerminalSessionProjection(projectedSession));
          const requiresPhysicalStop =
            runtimeSession !== undefined || bootRuntimeSessionsSnapshot._tag === "Unavailable";
          let cleanupConfirmation:
            | {
                readonly runtimeSession: ProviderSession | undefined;
                readonly requiresSessionRecording: boolean;
              }
            | undefined = { runtimeSession, requiresSessionRecording };
          if (requiresPhysicalStop) {
            const stopResult = yield* stopBootProviderSession(childThreadId);
            if (stopResult._tag === "Indeterminate") {
              cleanupConfirmation = undefined;
              yield* Effect.logWarning(
                "orphan provider cleanup indeterminate; quarantining and retrying",
                {
                  childThreadId,
                  parentThreadId: record.parentThreadId,
                  cause: stopResult.cause,
                },
              );
            }
          }
          if (cleanupConfirmation === undefined) {
            // Keep the active projection untouched until physical cleanup is
            // confirmed. A crash therefore re-enters orphan classification,
            // while the current waiter receives its killed outcome immediately.
            yield* settleChild(childThreadId, "killed", orphanReason, true);
            yield* Effect.forkScoped(
              retryPhysicalCleanup.pipe(
                Effect.flatMap(
                  Option.match({
                    // Abandoned: a replacement start superseded this settlement
                    // before physical cleanup was ever confirmed, so there is
                    // nothing left to settle or record.
                    onNone: () => Effect.void,
                    onSome: (confirmation) =>
                      retrySettlement.pipe(Effect.andThen(retrySessionRecording(confirmation))),
                  }),
                ),
              ),
            );
            continue;
          }
          const settlementRecorded = yield* recordOrphanSettlement(settlementInput).pipe(
            Effect.catchCause((cause) => Effect.fail(Cause.pretty(cause))),
            Effect.retry({ times: 2 }),
            Effect.as(true),
            Effect.catch((cause) =>
              Effect.logWarning(
                "orphan settlement recording retries exhausted; retrying in background",
                {
                  childThreadId,
                  parentThreadId: record.parentThreadId,
                  cause,
                },
              ).pipe(Effect.as(false)),
            ),
          );
          if (!settlementRecorded) {
            // Physical cleanup is already confirmed, so persistence retry can
            // proceed without keeping a live provider behind the released lease.
            yield* settleChild(childThreadId, "killed", orphanReason, true);
            yield* Effect.forkScoped(
              retrySettlement.pipe(Effect.andThen(retrySessionRecording(cleanupConfirmation))),
            );
            continue;
          }
          if (cleanupConfirmation.requiresSessionRecording) {
            const sessionRecorded = yield* recordStoppedOrphanSession({
              threadId: childThreadId,
              projectedSession,
              runtimeSession: cleanupConfirmation.runtimeSession,
              createdAt,
              lastError: orphanReason,
              commandId: sessionCommandId,
              dataAudience: dataAudienceByChild.get(childThreadId)!,
            }).pipe(
              Effect.as(true),
              Effect.catchCause((cause) =>
                Effect.logWarning(
                  "orphan cleanup succeeded but session projection recording failed; retrying",
                  {
                    childThreadId,
                    parentThreadId: record.parentThreadId,
                    cause: Cause.pretty(cause),
                  },
                ).pipe(Effect.as(false)),
              ),
            );
            if (!sessionRecorded) {
              yield* Effect.forkScoped(retrySessionRecording(cleanupConfirmation));
            }
          }
          // The killed lifecycle signal is durable before this Deferred is
          // released, so replay returns the same terminal classification.
          yield* settleChild(childThreadId, "killed", orphanReason, true);
          continue;
        }
        const instanceRead = yield* getProviderInstanceForBoot(record.model.instanceId);
        if (instanceRead._tag === "Missing") {
          yield* Effect.logWarning(
            "reconciled non-terminal sub-agent lost its provider instance; terminating",
            {
              childThreadId,
              instanceId: record.model.instanceId,
              parentThreadId: record.parentThreadId,
            },
          );
          yield* settleBootChild(childThreadId, "killed", "provider instance removed");
          continue;
        }
        if (instanceRead._tag === "Unavailable") {
          yield* Effect.logWarning(
            "provider instance read unavailable during child reconciliation; assuming live",
            {
              childThreadId,
              instanceId: record.model.instanceId,
              cause: instanceRead.cause,
            },
          );
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
          sourceTerminalSequence: row.sourceTerminalSequence ?? null,
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
        Stream.runForEach(orchestrationEngine.streamDomainEvents, (event) =>
          Effect.sync(() => {
            if (
              event.type === "thread.turn-start-requested" &&
              (orphanSettledChildIds.has(event.payload.threadId) ||
                pendingOrphanSupersessionByChild.has(event.payload.threadId))
            ) {
              // Observe persisted stream order before the event waits behind
              // older worker items. A sleeping orphan retry must not act in the
              // append-to-handler gap. Pause rather than cancel: a correlated
              // start failure must let the original durable cleanup continue.
              orphanCleanupPauseRequestByChild.set(event.payload.threadId, event.eventId);
            }
          }).pipe(Effect.andThen(worker.enqueue(event))),
        ),
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
