import {
  ORCHESTRATION_WS_METHODS,
  type EnvironmentId as EnvironmentIdType,
  type OrchestrationThread,
  type OrchestrationThreadDetailSnapshot,
  type OrchestrationThreadStreamItem,
  type ThreadId as ThreadIdType,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom } from "effect/unstable/reactivity";

import { EnvironmentRegistry } from "../connection/registry.ts";
import { connectionProjectionPhase } from "../connection/model.ts";
import { EnvironmentSupervisor } from "../connection/supervisor.ts";
import { EnvironmentCacheStore } from "../platform/persistence.ts";
import { subscribe } from "../rpc/client.ts";
import { ThreadRevisionLoader, ThreadSnapshotLoader } from "./threadSnapshotHttp.ts";
import { ThreadReconciliationActivity } from "./threadReconciliationActivity.ts";
import { parseThreadKey, threadKey } from "./entities.ts";
import { applyThreadDetailEvent } from "./threadReducer.ts";
import { THREAD_STATE_IDLE_TTL_MS } from "./threadRetention.ts";
import { followStreamInEnvironment } from "./runtime.ts";
import {
  EMPTY_ENVIRONMENT_THREAD_STATE,
  type EnvironmentThreadState,
  type EnvironmentThreadStatus,
} from "./threadState.ts";

export interface ThreadReconciliationPolicy {
  readonly fastIntervalMs: number;
  readonly fastWindowMs: number;
  readonly backoffMultiplier: number;
  readonly maxBackoffMs: number;
}

export const DEFAULT_THREAD_RECONCILIATION_POLICY: ThreadReconciliationPolicy = Object.freeze({
  fastIntervalMs: 2_000,
  fastWindowMs: 30_000,
  backoffMultiplier: 2,
  maxBackoffMs: 60_000,
});

type ThreadReconciliationReason =
  | "recovery-snapshot"
  | "incoming-event"
  | "changed-revision"
  | "locally-initiated-turn"
  | "connection-generation-change"
  | "unchanged-backoff";

type ThreadProjectionReconcileResult =
  | {
      readonly kind: "recovered";
      readonly recoveredThroughSequence: number;
    }
  | {
      readonly kind: "pending";
    };

export interface EnvironmentThreadStateOptions {
  readonly reconciliationPolicy?: ThreadReconciliationPolicy;
}

function statusWithoutLiveData(data: Option.Option<OrchestrationThread>): EnvironmentThreadStatus {
  return Option.isSome(data) ? "cached" : "empty";
}

function threadNeedsProjectionReconcile(thread: OrchestrationThread): boolean {
  if (thread.session?.status === "starting" || thread.session?.status === "running") {
    return true;
  }
  if (thread.session?.status === "waiting" && thread.session.activeTurnId !== null) {
    return true;
  }
  return thread.latestTurn?.state === "running";
}

function threadProjectionMatches(left: OrchestrationThread, right: OrchestrationThread): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function serializedJsonBytes(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

function threadBelongsInActiveDetail(thread: OrchestrationThread): boolean {
  return thread.deletedAt === null;
}

function compareString(left: string, right: string): number {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

function mergeKeyedCollection<T>(
  current: ReadonlyArray<T>,
  snapshot: ReadonlyArray<T>,
  keyOf: (entry: T) => string,
  mergeEntry: (current: T, snapshot: T) => T,
  sortEntries: (left: T, right: T) => number,
): ReadonlyArray<T> {
  const merged = new Map<string, T>();
  for (const entry of current) {
    merged.set(keyOf(entry), entry);
  }
  for (const entry of snapshot) {
    const key = keyOf(entry);
    const existing = merged.get(key);
    merged.set(key, existing === undefined ? entry : mergeEntry(existing, entry));
  }
  return Array.from(merged.values()).sort(sortEntries);
}

function mergeMessage(
  current: OrchestrationThread["messages"][number],
  snapshot: OrchestrationThread["messages"][number],
): OrchestrationThread["messages"][number] {
  const updatedAtOrder = compareString(current.updatedAt, snapshot.updatedAt);
  if (updatedAtOrder < 0) return snapshot;
  if (updatedAtOrder > 0) return current;
  if (!current.streaming && snapshot.streaming) return current;
  if (current.streaming && !snapshot.streaming) {
    return {
      ...snapshot,
      text: snapshot.text.length > 0 ? snapshot.text : current.text,
    };
  }
  return snapshot.text.length >= current.text.length ? snapshot : current;
}

function mergeTurnBoundary(
  current: OrchestrationThread["turns"][number],
  snapshot: OrchestrationThread["turns"][number],
  availableAssistantMessageIds: ReadonlySet<string>,
): OrchestrationThread["turns"][number] {
  const snapshotSettled = snapshot.state !== "running" && snapshot.completedAt !== null;
  const currentSettled = current.state !== "running" && current.completedAt !== null;
  const shouldUseSnapshotSettlement = current.state === "running" && snapshotSettled;
  const snapshotAssistantMessageId =
    snapshot.assistantMessageId !== null &&
    availableAssistantMessageIds.has(String(snapshot.assistantMessageId))
      ? snapshot.assistantMessageId
      : null;
  const snapshotHasAuthoritativeBoundary =
    snapshotSettled &&
    snapshotAssistantMessageId !== null &&
    (current.completedAt === null || snapshot.completedAt >= current.completedAt);
  const snapshotHasAuthoritativeNullBoundary =
    snapshotSettled &&
    snapshot.assistantMessageId === null &&
    (current.completedAt === null || snapshot.completedAt >= current.completedAt);
  const assistantMessageId = snapshotHasAuthoritativeBoundary
    ? snapshotAssistantMessageId
    : snapshotHasAuthoritativeNullBoundary
      ? null
      : (current.assistantMessageId ?? snapshotAssistantMessageId);
  const sourceProposedPlan = current.sourceProposedPlan ?? snapshot.sourceProposedPlan;

  return {
    ...current,
    state: shouldUseSnapshotSettlement ? snapshot.state : current.state,
    requestedAt: current.requestedAt || snapshot.requestedAt,
    startedAt: current.startedAt ?? snapshot.startedAt,
    completedAt: shouldUseSnapshotSettlement
      ? snapshot.completedAt
      : (current.completedAt ?? (!currentSettled && snapshotSettled ? snapshot.completedAt : null)),
    assistantMessageId,
    ...(sourceProposedPlan !== undefined ? { sourceProposedPlan } : {}),
  };
}

function mergeNonAdvancingSnapshotThread(
  current: OrchestrationThread,
  snapshot: OrchestrationThread,
): OrchestrationThread {
  // A non-advancing snapshot has a global min sequence that is not newer than
  // the stream cursor, so absence in its collections is not authoritative.
  // Use it only as an additive message recovery source; advancing snapshots
  // and live events remain responsible for scalar updates and destructive
  // collection changes such as reverts.
  const availableAssistantMessageIds = new Set(
    [...current.messages, ...snapshot.messages]
      .filter((message) => message.role === "assistant" && !message.streaming)
      .map((message) => String(message.id)),
  );

  const messages = mergeKeyedCollection(
    current.messages,
    snapshot.messages.filter((message) => !message.streaming),
    (message) => message.id,
    mergeMessage,
    (left, right) =>
      compareString(left.createdAt, right.createdAt) || compareString(left.id, right.id),
  );
  const turns = mergeKeyedCollection(
    current.turns,
    snapshot.turns,
    (turn) => turn.turnId,
    (currentTurn, snapshotTurn) =>
      mergeTurnBoundary(currentTurn, snapshotTurn, availableAssistantMessageIds),
    (left, right) =>
      compareString(left.requestedAt, right.requestedAt) ||
      compareString(left.turnId, right.turnId),
  );
  const latestTurn =
    current.latestTurn !== null &&
    snapshot.latestTurn !== null &&
    current.latestTurn.turnId === snapshot.latestTurn.turnId
      ? mergeTurnBoundary(current.latestTurn, snapshot.latestTurn, availableAssistantMessageIds)
      : current.latestTurn;

  return {
    ...current,
    messages,
    latestTurn,
    turns,
  };
}

function formatThreadError(cause: Cause.Cause<unknown>): string {
  const error = Cause.squash(cause);
  return error instanceof Error && error.message.trim().length > 0
    ? error.message
    : "Could not synchronize the thread.";
}

export const makeEnvironmentThreadState = Effect.fn("EnvironmentThreadState.make")(function* (
  threadId: ThreadIdType,
  options?: EnvironmentThreadStateOptions,
) {
  const supervisor = yield* EnvironmentSupervisor;
  const cache = yield* EnvironmentCacheStore;
  const snapshotLoader = yield* ThreadSnapshotLoader;
  const revisionLoader = yield* ThreadRevisionLoader;
  const reconciliationActivity = yield* ThreadReconciliationActivity;
  const reconciliationPolicy =
    options?.reconciliationPolicy ?? DEFAULT_THREAD_RECONCILIATION_POLICY;
  const environmentId = supervisor.target.environmentId;
  const cached = yield* cache.loadThread(environmentId, threadId).pipe(
    Effect.catch((error) =>
      Effect.logWarning("Could not load cached thread.").pipe(
        Effect.annotateLogs({
          environmentId,
          threadId,
          error: error.message,
        }),
        Effect.as(Option.none<OrchestrationThreadDetailSnapshot>()),
      ),
    ),
  );
  const cachedThread = Option.map(cached, (snapshot) => snapshot.thread);
  const state = yield* SubscriptionRef.make<EnvironmentThreadState>({
    data: cachedThread,
    status: statusWithoutLiveData(cachedThread),
    error: Option.none(),
  });
  // Seed the resume cursor from the cached snapshot so a warm cache can catch up
  // via `afterSequence` instead of re-downloading the full thread body.
  const lastSequence = yield* SubscriptionRef.make(
    Option.match(cached, { onNone: () => 0, onSome: (snapshot) => snapshot.snapshotSequence }),
  );
  const persistence = yield* Queue.sliding<OrchestrationThreadDetailSnapshot>(1);
  const reconciliationWake = yield* Queue.sliding<ThreadReconciliationReason>(1);
  const reconcileFastUntil = yield* Ref.make(0);
  const reconciliationDelayMs = yield* Ref.make(reconciliationPolicy.fastIntervalMs);
  const reconciliationUnchangedCount = yield* Ref.make(0);
  const lastReconciliationReason = yield* Ref.make<ThreadReconciliationReason>("recovery-snapshot");
  // A later delivered event cannot prove that every earlier event arrived.
  // Only an accepted detail/recovery snapshot advances this verified marker;
  // live events advance the reconnect cursor and leave a revision pending.
  const lastVerifiedRevision = yield* Ref.make(yield* SubscriptionRef.get(lastSequence));
  const pendingRevision = yield* Ref.make(0);
  const revisionCheckUnresolved = yield* Ref.make(false);
  const connectionGeneration = yield* Ref.make(
    (yield* SubscriptionRef.get(supervisor.state)).generation,
  );
  const subscribeInput: { threadId: ThreadIdType; afterSequence?: number } = { threadId };
  // Sequence of the last applied destructive collection change (revert). A
  // non-advancing snapshot taken before this point may still contain pruned
  // messages, so it must not be used as an additive recovery source.
  const lastRevertSequence = yield* Ref.make(0);
  const applyLock = yield* Semaphore.make(1);

  const persist = Effect.fn("EnvironmentThreadState.persist")(function* (
    snapshot: OrchestrationThreadDetailSnapshot,
  ) {
    const [current, currentSequence] = yield* Effect.all([
      SubscriptionRef.get(state),
      SubscriptionRef.get(lastSequence),
    ]);
    if (
      current.status === "deleted" ||
      Option.isNone(current.data) ||
      snapshot.snapshotSequence !== currentSequence ||
      !threadProjectionMatches(current.data.value, snapshot.thread)
    ) {
      return;
    }
    yield* cache.saveThread(environmentId, snapshot).pipe(
      Effect.catch((error) =>
        Effect.logWarning("Could not persist the thread cache.").pipe(
          Effect.annotateLogs({
            environmentId,
            threadId,
            error: error.message,
          }),
        ),
      ),
    );
  });

  yield* Stream.fromQueue(persistence).pipe(
    Stream.debounce("500 millis"),
    Stream.runForEach(persist),
    Effect.forkScoped,
  );

  const setSynchronizing = SubscriptionRef.update(state, (current) => ({
    ...current,
    status: "synchronizing" as const,
    error: Option.none(),
  }));
  const setReady = SubscriptionRef.update(state, (current) =>
    current.status === "live" || current.status === "deleted"
      ? current
      : {
          ...current,
          status: "synchronizing" as const,
          error: Option.none(),
        },
  );
  const setDisconnected = SubscriptionRef.update(state, (current) => ({
    ...current,
    status: current.status === "deleted" ? current.status : statusWithoutLiveData(current.data),
  }));
  const setStreamError = (cause: Cause.Cause<unknown>) =>
    SubscriptionRef.update(state, (current) => ({
      ...current,
      status: current.status === "deleted" ? current.status : statusWithoutLiveData(current.data),
      error: Option.some(formatThreadError(cause)),
    }));

  const setThread = Effect.fn("EnvironmentThreadState.setThread")(function* (
    thread: OrchestrationThread,
    options?: {
      readonly persist?: boolean;
    },
  ) {
    if (!threadBelongsInActiveDetail(thread)) {
      yield* setDeleted();
      return;
    }
    yield* SubscriptionRef.set(state, {
      data: Option.some(thread),
      status: "live",
      error: Option.none(),
    });
    // Persist the thread together with the sequence it reflects so the next warm
    // cache can resume from exactly here.
    if (options?.persist === false) {
      return;
    }
    const snapshotSequence = yield* SubscriptionRef.get(lastSequence);
    yield* Queue.offer(persistence, { snapshotSequence, thread });
  });

  const markThreadRecentlyActive = Effect.fn("EnvironmentThreadState.markThreadRecentlyActive")(
    function* (
      reason: Exclude<ThreadReconciliationReason, "unchanged-backoff">,
      markOptions?: { readonly wake?: boolean },
    ) {
      const now = yield* Clock.currentTimeMillis;
      yield* Ref.set(reconcileFastUntil, now + reconciliationPolicy.fastWindowMs);
      yield* Ref.set(reconciliationDelayMs, reconciliationPolicy.fastIntervalMs);
      yield* Ref.set(reconciliationUnchangedCount, 0);
      yield* Ref.set(lastReconciliationReason, reason);
      if (markOptions?.wake !== false) {
        yield* Queue.offer(reconciliationWake, reason);
      }
    },
  );

  const advanceLastSequence = Effect.fn("EnvironmentThreadState.advanceLastSequence")(function* (
    sequence: number,
  ) {
    const current = yield* SubscriptionRef.get(lastSequence);
    if (sequence > current) {
      yield* SubscriptionRef.set(lastSequence, sequence);
    }
    subscribeInput.afterSequence = Math.max(current, sequence);
  });

  const acknowledgeSnapshotRevision = Effect.fn(
    "EnvironmentThreadState.acknowledgeSnapshotRevision",
  )(function* (sequence: number) {
    yield* Ref.update(lastVerifiedRevision, (revision) => Math.max(revision, sequence));
    yield* Ref.update(pendingRevision, (pending) => (pending <= sequence ? 0 : pending));
  });

  const setDeleted = Effect.fn("EnvironmentThreadState.setDeleted")(function* () {
    yield* SubscriptionRef.set(state, {
      data: Option.none(),
      status: "deleted",
      error: Option.none(),
    });
    yield* cache.removeThread(environmentId, threadId).pipe(
      Effect.catch((error) =>
        Effect.logWarning("Could not remove the cached thread.").pipe(
          Effect.annotateLogs({
            environmentId,
            threadId,
            error: error.message,
          }),
        ),
      ),
    );
  });

  const applySnapshotLocked = Effect.fn("EnvironmentThreadState.applySnapshotLocked")(function* (
    snapshot: OrchestrationThreadDetailSnapshot,
    options?: {
      readonly allowCurrentSequence?: boolean;
      readonly allowOlderFreshThread?: boolean;
      readonly mergeNonAdvancingSnapshot?: boolean;
      readonly skipMatchingCurrentSequence?: boolean;
      readonly activityReason?: Exclude<ThreadReconciliationReason, "unchanged-backoff">;
    },
  ) {
    const sequence = yield* SubscriptionRef.get(lastSequence);
    const current = yield* SubscriptionRef.get(state);
    const olderSequence = snapshot.snapshotSequence < sequence;
    const olderReconcileSnapshot = olderSequence && options?.mergeNonAdvancingSnapshot === true;
    // A snapshot taken before the last applied revert may still contain the
    // messages that revert pruned; using it additively would resurrect them.
    if (
      olderReconcileSnapshot &&
      snapshot.snapshotSequence < (yield* Ref.get(lastRevertSequence))
    ) {
      return false;
    }
    if (olderSequence && options?.allowOlderFreshThread !== true) {
      return false;
    }
    if (
      olderSequence &&
      (Option.isNone(current.data) || snapshot.thread.updatedAt < current.data.value.updatedAt)
    ) {
      return false;
    }
    if (snapshot.snapshotSequence === sequence && options?.allowCurrentSequence !== true) {
      return false;
    }

    const thread =
      olderReconcileSnapshot && Option.isSome(current.data)
        ? mergeNonAdvancingSnapshotThread(current.data.value, snapshot.thread)
        : snapshot.thread;

    if (
      snapshot.snapshotSequence <= sequence &&
      options?.skipMatchingCurrentSequence === true &&
      Option.isSome(current.data) &&
      threadProjectionMatches(current.data.value, thread)
    ) {
      if (current.status !== "live") {
        yield* SubscriptionRef.set(state, {
          ...current,
          status: "live",
          error: Option.none(),
        });
        yield* markThreadRecentlyActive(options?.activityReason ?? "recovery-snapshot");
      }
      return true;
    }

    if (snapshot.snapshotSequence > sequence) {
      yield* advanceLastSequence(snapshot.snapshotSequence);
    }
    yield* setThread(thread, {
      persist: !olderReconcileSnapshot,
    });
    yield* markThreadRecentlyActive(options?.activityReason ?? "recovery-snapshot");
    return true;
  });

  const applySnapshot = Effect.fn("EnvironmentThreadState.applySnapshot")(function* (
    snapshot: OrchestrationThreadDetailSnapshot,
    options?: Parameters<typeof applySnapshotLocked>[1],
  ) {
    return yield* applyLock.withPermit(applySnapshotLocked(snapshot, options));
  });

  const applyItemLocked = Effect.fn("EnvironmentThreadState.applyItemLocked")(function* (
    item: OrchestrationThreadStreamItem,
  ) {
    if (item.kind === "snapshot") {
      const accepted = yield* applySnapshotLocked(item.snapshot, {
        allowCurrentSequence: true,
        skipMatchingCurrentSequence: true,
      });
      if (accepted) {
        yield* acknowledgeSnapshotRevision(item.snapshot.snapshotSequence);
      }
      return;
    }

    const sequence = yield* SubscriptionRef.get(lastSequence);
    if (item.event.sequence <= sequence) {
      return;
    }
    yield* advanceLastSequence(item.event.sequence);
    yield* Ref.update(pendingRevision, (pending) => Math.max(pending, item.event.sequence));
    if (item.event.type === "thread.reverted") {
      yield* Ref.set(lastRevertSequence, item.event.sequence);
    }
    yield* markThreadRecentlyActive("incoming-event");
    yield* Effect.logDebug("Applied live thread event.").pipe(
      Effect.annotateLogs({
        environmentId,
        threadId,
        lastAppliedSequence: item.event.sequence,
        eventType: item.event.type,
      }),
    );

    const current = yield* SubscriptionRef.get(state);
    if (Option.isNone(current.data)) {
      if (item.event.type === "thread.deleted") {
        yield* setDeleted();
      }
      return;
    }
    const result = applyThreadDetailEvent(current.data.value, item.event);
    if (result.kind === "updated") {
      yield* setThread(result.thread);
    } else if (result.kind === "deleted") {
      yield* setDeleted();
    }
  });

  const applyItem = Effect.fn("EnvironmentThreadState.applyItem")(function* (
    item: OrchestrationThreadStreamItem,
  ) {
    yield* applyLock.withPermit(applyItemLocked(item));
  });

  const reconcileFromProjection = Effect.fn("EnvironmentThreadState.reconcileFromProjection")(
    function* (revisionSequence: number, projectionSequence: number | null) {
      const prepared = yield* SubscriptionRef.get(supervisor.prepared);
      if (Option.isNone(prepared)) {
        return { kind: "pending" } as ThreadProjectionReconcileResult;
      }
      const result = yield* snapshotLoader.loadForReconcile(prepared.value, threadId);
      if (result.kind === "missing") {
        const projectionCaughtUp =
          projectionSequence !== null && projectionSequence >= revisionSequence;
        yield* Effect.logDebug(
          projectionCaughtUp
            ? "Thread reconciliation confirmed a projected deletion."
            : "Thread reconciliation detail is missing while projection is behind.",
        ).pipe(
          Effect.annotateLogs({
            environmentId,
            threadId,
            reconciliationReason: "changed-revision",
            revisionSequence,
            projectionSequence,
            projectionCaughtUp,
            responseBytes: 0,
          }),
        );
        if (!projectionCaughtUp) {
          return { kind: "pending" } as ThreadProjectionReconcileResult;
        }
        yield* applyLock.withPermit(setDeleted());
        return {
          kind: "recovered",
          recoveredThroughSequence: revisionSequence,
        } as ThreadProjectionReconcileResult;
      }
      if (result.kind === "unavailable") {
        yield* Effect.logDebug("Thread reconciliation detail was unavailable.").pipe(
          Effect.annotateLogs({
            environmentId,
            threadId,
            reconciliationReason: "changed-revision",
            revisionSequence,
            responseBytes: 0,
          }),
        );
        return { kind: "pending" } as ThreadProjectionReconcileResult;
      }
      const responseBytes = serializedJsonBytes(result.snapshot);
      const accepted = yield* applySnapshot(result.snapshot, {
        allowOlderFreshThread: true,
        allowCurrentSequence: true,
        mergeNonAdvancingSnapshot: true,
        skipMatchingCurrentSequence: true,
        activityReason: "changed-revision",
      });
      yield* Effect.logDebug("Loaded thread detail after its revision advanced.").pipe(
        Effect.annotateLogs({
          environmentId,
          threadId,
          reconciliationReason: "changed-revision",
          revisionSequence,
          snapshotSequence: result.snapshot.snapshotSequence,
          responseBytes,
        }),
      );
      if (!accepted || result.snapshot.snapshotSequence < revisionSequence) {
        yield* Effect.logDebug(
          "Thread reconciliation detail has not reached the revision yet.",
        ).pipe(
          Effect.annotateLogs({
            environmentId,
            threadId,
            reconciliationReason: "changed-revision",
            revisionSequence,
            snapshotSequence: result.snapshot.snapshotSequence,
            snapshotAccepted: accepted,
            responseBytes,
          }),
        );
        return { kind: "pending" } as ThreadProjectionReconcileResult;
      }
      return {
        kind: "recovered",
        recoveredThroughSequence: result.snapshot.snapshotSequence,
      } as ThreadProjectionReconcileResult;
    },
  );

  const recordUnchangedRevision = Effect.fn("EnvironmentThreadState.recordUnchangedRevision")(
    function* (input: { readonly latestSequence: number | null; readonly responseBytes: number }) {
      const now = yield* Clock.currentTimeMillis;
      const fastUntil = yield* Ref.get(reconcileFastUntil);
      const unchangedCount = yield* Ref.updateAndGet(
        reconciliationUnchangedCount,
        (count) => count + 1,
      );
      const currentDelayMs = yield* Ref.get(reconciliationDelayMs);
      const inFastWindow = fastUntil > now;
      const nextDelayMs = inFastWindow
        ? reconciliationPolicy.fastIntervalMs
        : Math.min(
            reconciliationPolicy.maxBackoffMs,
            Math.max(reconciliationPolicy.fastIntervalMs, currentDelayMs) *
              reconciliationPolicy.backoffMultiplier,
          );
      yield* Ref.set(reconciliationDelayMs, nextDelayMs);
      if (!inFastWindow) {
        yield* Ref.set(lastReconciliationReason, "unchanged-backoff");
      }
      const lastAppliedSequence = yield* SubscriptionRef.get(lastSequence);
      yield* Effect.logDebug("Checked thread revision for reconciliation.").pipe(
        Effect.annotateLogs({
          environmentId,
          threadId,
          reconciliationReason: inFastWindow
            ? yield* Ref.get(lastReconciliationReason)
            : "unchanged-backoff",
          latestSequence: input.latestSequence,
          lastAppliedSequence,
          responseBytes: input.responseBytes,
          unchangedCount,
          backoffMs: nextDelayMs,
        }),
      );
    },
  );

  const checkThreadRevision = Effect.fn("EnvironmentThreadState.checkThreadRevision")(function* () {
    const prepared = yield* SubscriptionRef.get(supervisor.prepared);
    if (Option.isNone(prepared)) {
      return;
    }
    const result = yield* revisionLoader.load(prepared.value, threadId);
    yield* Ref.set(revisionCheckUnresolved, result.kind === "unavailable");
    const [verifiedRevision, currentPendingRevision] = yield* Effect.all([
      Ref.get(lastVerifiedRevision),
      Ref.get(pendingRevision),
    ]);
    if (result.kind === "unavailable" && currentPendingRevision <= verifiedRevision) {
      yield* recordUnchangedRevision({ latestSequence: null, responseBytes: 0 });
      return;
    }

    const latestSequence = result.kind === "found" ? result.revision.latestSequence : null;
    const projectionSequence = result.kind === "found" ? result.revision.projectionSequence : null;
    const nextPendingRevision =
      latestSequence === null
        ? currentPendingRevision
        : Math.max(currentPendingRevision, latestSequence);
    const markerAdvanced = nextPendingRevision > Math.max(verifiedRevision, currentPendingRevision);
    if (markerAdvanced) {
      yield* Ref.set(pendingRevision, nextPendingRevision);
      yield* markThreadRecentlyActive("changed-revision", { wake: false });
      yield* Effect.logDebug("Thread revision advanced; loading detail.").pipe(
        Effect.annotateLogs({
          environmentId,
          threadId,
          reconciliationReason: "changed-revision",
          latestSequence,
          lastAppliedSequence: yield* SubscriptionRef.get(lastSequence),
          responseBytes: result.kind === "found" ? result.responseBytes : 0,
          unchangedCount: 0,
          backoffMs: reconciliationPolicy.fastIntervalMs,
        }),
      );
    }

    if (nextPendingRevision <= verifiedRevision) {
      yield* recordUnchangedRevision({
        latestSequence,
        responseBytes: result.kind === "found" ? result.responseBytes : 0,
      });
      return;
    }

    const reconciliation = yield* reconcileFromProjection(nextPendingRevision, projectionSequence);
    if (reconciliation.kind === "recovered") {
      yield* acknowledgeSnapshotRevision(reconciliation.recoveredThroughSequence);
      return;
    }
    yield* recordUnchangedRevision({
      latestSequence,
      responseBytes: result.kind === "found" ? result.responseBytes : 0,
    });
  });

  yield* reconciliationActivity.events.pipe(
    Stream.filter((event) => event.environmentId === environmentId && event.threadId === threadId),
    Stream.runForEach((event) => markThreadRecentlyActive(event.reason)),
    Effect.forkScoped,
  );

  const waitForReconciliationDeadline = Effect.fn(
    "EnvironmentThreadState.waitForReconciliationDeadline",
  )(function* (initialDelayMs: number) {
    let deadline = (yield* Clock.currentTimeMillis) + initialDelayMs;
    for (;;) {
      const now = yield* Clock.currentTimeMillis;
      const remainingMs = Math.max(0, deadline - now);
      if (remainingMs === 0) {
        return;
      }
      const wakeReason = yield* Effect.raceFirst(
        Effect.sleep(Duration.millis(remainingMs)).pipe(Effect.as("timer" as const)),
        Queue.take(reconciliationWake).pipe(Effect.as("activity" as const)),
      );
      if (wakeReason === "timer") {
        return;
      }
      // Activity may shorten an existing backoff to the newly reset fast
      // interval. It must never move an already scheduled check later, or a
      // busy stream could starve reconciliation forever.
      const activityDeadline =
        (yield* Clock.currentTimeMillis) + (yield* Ref.get(reconciliationDelayMs));
      deadline = Math.min(deadline, activityDeadline);
    }
  });

  yield* Effect.gen(function* () {
    for (;;) {
      const current = yield* SubscriptionRef.get(state);
      const now = yield* Clock.currentTimeMillis;
      const fastUntil = yield* Ref.get(reconcileFastUntil);
      const [verifiedRevision, currentPendingRevision] = yield* Effect.all([
        Ref.get(lastVerifiedRevision),
        Ref.get(pendingRevision),
      ]);
      const unresolvedRevisionCheck = yield* Ref.get(revisionCheckUnresolved);
      const eligible =
        unresolvedRevisionCheck ||
        currentPendingRevision > verifiedRevision ||
        (Option.isSome(current.data) &&
          (threadNeedsProjectionReconcile(current.data.value) || fastUntil > now));
      if (!eligible) {
        yield* Queue.take(reconciliationWake);
        continue;
      }

      const delayMs = yield* Ref.get(reconciliationDelayMs);
      yield* waitForReconciliationDeadline(delayMs);
      yield* checkThreadRevision();
    }
  }).pipe(Effect.forkScoped);

  yield* SubscriptionRef.changes(supervisor.state).pipe(
    Stream.runForEach((connectionState) =>
      Effect.gen(function* () {
        const previousGeneration = yield* Ref.get(connectionGeneration);
        if (connectionState.generation !== previousGeneration) {
          yield* Ref.set(connectionGeneration, connectionState.generation);
          yield* markThreadRecentlyActive("connection-generation-change");
          yield* Effect.logDebug("Thread connection generation changed.").pipe(
            Effect.annotateLogs({
              environmentId,
              threadId,
              connectionGeneration: connectionState.generation,
              lastAppliedSequence: yield* SubscriptionRef.get(lastSequence),
              reconciliationReason: "connection-generation-change",
            }),
          );
        }
        switch (connectionProjectionPhase(connectionState)) {
          case "synchronizing":
            yield* setSynchronizing;
            return;
          case "disconnected":
            yield* setDisconnected;
            return;
          case "ready":
            yield* setReady;
            return;
        }
      }),
    ),
    Effect.forkScoped,
  );

  yield* setSynchronizing;
  yield* Effect.forkScoped(
    Effect.gen(function* () {
      // Establish the base snapshot to resume from, minimizing bytes over the
      // wire:
      // - Warm cache: reuse the cached snapshot (zero network) and resume via
      //   `afterSequence` so we only receive events since the cached sequence.
      // - Cold cache: load the full snapshot over HTTP (gzip-compressible, and
      //   off the socket), then resume via `afterSequence`.
      // If no base can be established we fall back to the socket-embedded
      // snapshot so the thread still synchronizes. Overlapping/replayed events
      // are deduped by sequence in applyItem.
      const base = Option.isSome(cached)
        ? cached
        : yield* Effect.gen(function* () {
            // Cold cache only: wait for a prepared connection so we can
            // authenticate the HTTP request; this mirrors the socket path, which
            // likewise waits for a live session.
            const prepared = yield* SubscriptionRef.changes(supervisor.prepared).pipe(
              Stream.filter(Option.isSome),
              Stream.map((current) => current.value),
              Stream.runHead,
            );
            return Option.isSome(prepared)
              ? yield* snapshotLoader.load(prepared.value, threadId)
              : Option.none<OrchestrationThreadDetailSnapshot>();
          });

      if (Option.isSome(base)) {
        yield* applyItem({ kind: "snapshot", snapshot: base.value });
        yield* advanceLastSequence(base.value.snapshotSequence);
      } else {
        delete subscribeInput.afterSequence;
      }

      const logSubscriptionLifecycle = (phase: "start" | "stop") =>
        Effect.all([SubscriptionRef.get(lastSequence), SubscriptionRef.get(supervisor.state)]).pipe(
          Effect.flatMap(([lastAppliedSequence, connectionState]) =>
            Effect.logDebug(`Thread subscription ${phase}.`).pipe(
              Effect.annotateLogs({
                environmentId,
                threadId,
                subscriptionPhase: phase,
                connectionGeneration: connectionState.generation,
                lastAppliedSequence,
              }),
            ),
          ),
        );

      yield* subscribe(ORCHESTRATION_WS_METHODS.subscribeThread, subscribeInput, {
        onExpectedFailure: setStreamError,
        retryExpectedFailureAfter: "250 millis",
        onSessionStart: () => logSubscriptionLifecycle("start"),
        onSessionStop: () => logSubscriptionLifecycle("stop"),
      }).pipe(Stream.runForEach(applyItem));
    }),
  );

  yield* Effect.addFinalizer(() =>
    Effect.all([SubscriptionRef.get(state), SubscriptionRef.get(lastSequence)]).pipe(
      Effect.flatMap(([current, snapshotSequence]) =>
        Option.match(current.data, {
          onNone: () => Effect.void,
          onSome: (thread) => persist({ snapshotSequence, thread }),
        }),
      ),
    ),
  );

  return state;
});

export function threadStateChanges(environmentId: EnvironmentIdType, threadId: ThreadIdType) {
  return followStreamInEnvironment(
    environmentId,
    Stream.unwrap(makeEnvironmentThreadState(threadId).pipe(Effect.map(SubscriptionRef.changes))),
  );
}

export function createEnvironmentThreadStateAtoms<R, E>(
  runtime: Atom.AtomRuntime<
    | EnvironmentRegistry
    | EnvironmentCacheStore
    | ThreadSnapshotLoader
    | ThreadRevisionLoader
    | ThreadReconciliationActivity
    | R,
    E
  >,
) {
  const family = Atom.family((key: string) => {
    const { environmentId, threadId } = parseThreadKey(key);
    return runtime
      .atom(threadStateChanges(environmentId, threadId), {
        initialValue: EMPTY_ENVIRONMENT_THREAD_STATE,
      })
      .pipe(
        Atom.setIdleTTL(THREAD_STATE_IDLE_TTL_MS),
        Atom.withLabel(`environment-thread-state:${key}`),
      );
  });

  return {
    stateAtom: (environmentId: EnvironmentIdType, threadId: ThreadIdType) =>
      family(threadKey({ environmentId, threadId })),
  };
}

export * from "./archivedThreads.ts";
export * from "./checkpointDiff.ts";
export * from "./threadSnapshotHttp.ts";
export * from "./threadReconciliationActivity.ts";
export * from "./composerPathSearch.ts";
export * from "./threadCommands.ts";
export * from "./threadDetail.ts";
export * from "./threadReducer.ts";
export * from "./threadShell.ts";
export * from "./threadState.ts";
