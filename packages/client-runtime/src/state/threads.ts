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
import { ThreadSnapshotLoader } from "./threadSnapshotHttp.ts";
import { parseThreadKey, threadKey } from "./entities.ts";
import { applyThreadDetailEvent } from "./threadReducer.ts";
import { THREAD_STATE_IDLE_TTL_MS } from "./threadRetention.ts";
import { followStreamInEnvironment } from "./runtime.ts";
import {
  EMPTY_ENVIRONMENT_THREAD_STATE,
  type EnvironmentThreadState,
  type EnvironmentThreadStatus,
} from "./threadState.ts";

const ACTIVE_THREAD_RECONCILE_INTERVAL = "2 seconds";
const RECENT_THREAD_RECONCILE_GRACE_MS = 8_000;

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

function threadBelongsInActiveDetail(thread: OrchestrationThread): boolean {
  return thread.deletedAt === null;
}

function formatThreadError(cause: Cause.Cause<unknown>): string {
  const error = Cause.squash(cause);
  return error instanceof Error && error.message.trim().length > 0
    ? error.message
    : "Could not synchronize the thread.";
}

export const makeEnvironmentThreadState = Effect.fn("EnvironmentThreadState.make")(function* (
  threadId: ThreadIdType,
) {
  const supervisor = yield* EnvironmentSupervisor;
  const cache = yield* EnvironmentCacheStore;
  const snapshotLoader = yield* ThreadSnapshotLoader;
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
  const reconcileUntil = yield* Ref.make(0);
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
    const snapshotSequence = yield* SubscriptionRef.get(lastSequence);
    yield* Queue.offer(persistence, { snapshotSequence, thread });
  });

  const markThreadRecentlyActive = Effect.fn("EnvironmentThreadState.markThreadRecentlyActive")(
    function* () {
      const now = yield* Clock.currentTimeMillis;
      yield* Ref.set(reconcileUntil, now + RECENT_THREAD_RECONCILE_GRACE_MS);
    },
  );

  const shouldReconcileThread = Effect.fn("EnvironmentThreadState.shouldReconcileThread")(
    function* (thread: OrchestrationThread) {
      if (threadNeedsProjectionReconcile(thread)) {
        yield* markThreadRecentlyActive();
        return true;
      }
      const [now, until] = yield* Effect.all([Clock.currentTimeMillis, Ref.get(reconcileUntil)]);
      return until > now;
    },
  );

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
      readonly skipMatchingCurrentSequence?: boolean;
    },
  ) {
    const sequence = yield* SubscriptionRef.get(lastSequence);
    if (
      snapshot.snapshotSequence < sequence ||
      (snapshot.snapshotSequence === sequence && options?.allowCurrentSequence !== true)
    ) {
      return;
    }

    const current = yield* SubscriptionRef.get(state);
    if (
      snapshot.snapshotSequence === sequence &&
      options?.skipMatchingCurrentSequence === true &&
      Option.isSome(current.data) &&
      threadProjectionMatches(current.data.value, snapshot.thread)
    ) {
      return;
    }

    yield* SubscriptionRef.set(lastSequence, snapshot.snapshotSequence);
    yield* setThread(snapshot.thread);
    if (
      threadBelongsInActiveDetail(snapshot.thread) &&
      threadNeedsProjectionReconcile(snapshot.thread)
    ) {
      yield* markThreadRecentlyActive();
    }
  });

  const applySnapshot = Effect.fn("EnvironmentThreadState.applySnapshot")(function* (
    snapshot: OrchestrationThreadDetailSnapshot,
    options?: Parameters<typeof applySnapshotLocked>[1],
  ) {
    yield* applyLock.withPermit(applySnapshotLocked(snapshot, options));
  });

  const applyItemLocked = Effect.fn("EnvironmentThreadState.applyItemLocked")(function* (
    item: OrchestrationThreadStreamItem,
  ) {
    if (item.kind === "snapshot") {
      yield* applySnapshotLocked(item.snapshot, { allowCurrentSequence: true });
      return;
    }

    const sequence = yield* SubscriptionRef.get(lastSequence);
    if (item.event.sequence <= sequence) {
      return;
    }
    yield* SubscriptionRef.set(lastSequence, item.event.sequence);
    yield* markThreadRecentlyActive();

    const current = yield* SubscriptionRef.get(state);
    if (Option.isNone(current.data)) {
      if (item.event.type === "thread.deleted") {
        yield* setDeleted();
      } else if (item.event.type === "thread.unarchived") {
        // Archiving tears the detail down to `deleted` with no data, and the
        // unarchive event alone cannot rebuild it — reload the full detail.
        // Forked because reconcile re-enters the (non-reentrant) apply lock.
        yield* Effect.forkScoped(reconcileFromProjection());
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
    function* () {
      const prepared = yield* SubscriptionRef.get(supervisor.prepared);
      if (Option.isNone(prepared)) {
        return;
      }
      const result = yield* snapshotLoader.loadForReconcile(prepared.value, threadId);
      if (result.kind === "missing") {
        yield* applyLock.withPermit(setDeleted());
        return;
      }
      if (result.kind === "unavailable") {
        return;
      }
      yield* applySnapshot(result.snapshot, {
        allowCurrentSequence: true,
        skipMatchingCurrentSequence: true,
      });
    },
  );

  yield* Effect.forkScoped(
    Effect.forever(
      Effect.sleep(ACTIVE_THREAD_RECONCILE_INTERVAL).pipe(
        Effect.andThen(
          SubscriptionRef.get(state).pipe(
            Effect.flatMap((current) => {
              if (Option.isNone(current.data)) {
                return Effect.void;
              }
              return shouldReconcileThread(current.data.value).pipe(
                Effect.flatMap((shouldReconcile) =>
                  shouldReconcile ? reconcileFromProjection() : Effect.void,
                ),
              );
            }),
          ),
        ),
      ),
    ),
  );

  yield* SubscriptionRef.changes(supervisor.state).pipe(
    Stream.runForEach((connectionState) => {
      switch (connectionProjectionPhase(connectionState)) {
        case "synchronizing":
          return setSynchronizing;
        case "disconnected":
          return setDisconnected;
        case "ready":
          return setReady;
      }
    }),
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
      }

      const subscribeInput = Option.match(base, {
        onNone: () => ({ threadId }),
        onSome: (snapshot) => ({ threadId, afterSequence: snapshot.snapshotSequence }),
      });

      yield* subscribe(ORCHESTRATION_WS_METHODS.subscribeThread, subscribeInput, {
        onExpectedFailure: setStreamError,
        retryExpectedFailureAfter: "250 millis",
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
    EnvironmentRegistry | EnvironmentCacheStore | ThreadSnapshotLoader | R,
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
export * from "./composerPathSearch.ts";
export * from "./threadCommands.ts";
export * from "./threadDetail.ts";
export * from "./threadReducer.ts";
export * from "./threadShell.ts";
export * from "./threadState.ts";
