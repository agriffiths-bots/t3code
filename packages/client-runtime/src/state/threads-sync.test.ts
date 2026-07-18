import {
  CheckpointRef,
  EnvironmentId,
  EventId,
  MessageId,
  ORCHESTRATION_WS_METHODS,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type DataAudience,
  type OrchestrationMessage,
  type OrchestrationThread,
  type OrchestrationThreadDetailSnapshot,
  type OrchestrationThreadStreamItem,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";

import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "../connection/model.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import * as Persistence from "../platform/persistence.ts";
import * as RpcSession from "../rpc/session.ts";
import {
  EMPTY_ENVIRONMENT_THREAD_STATE,
  DEFAULT_THREAD_RECONCILIATION_POLICY,
  makeEnvironmentThreadState,
  ThreadReconciliationActivity,
  ThreadRevisionLoader,
  ThreadSnapshotLoader,
  type EnvironmentThreadState,
  type ThreadReconciliationPolicy,
  type ThreadSnapshotLoadResult,
} from "./threads.ts";

const TARGET = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("environment-1"),
  label: "Test environment",
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
});
const THREAD_ID = ThreadId.make("thread-1");
const CACHED_SNAPSHOT_SEQUENCE = 7;
const STORAGE_EPOCH = "storage-epoch-1";
const RESTORED_STORAGE_EPOCH = "storage-epoch-2";
const markerEventId = (sequence: number) =>
  sequence === 0 ? null : EventId.make(`event-marker-${sequence}`);
const PREPARED: PreparedConnection = {
  environmentId: TARGET.environmentId,
  label: TARGET.label,
  httpBaseUrl: TARGET.httpBaseUrl,
  socketUrl: TARGET.wsBaseUrl,
  httpAuthorization: null,
  target: TARGET,
};
const BASE_THREAD: OrchestrationThread = {
  id: THREAD_ID,
  projectId: ProjectId.make("project-1"),
  dataAudience: "private",
  title: "Cached thread",
  modelSelection: {
    instanceId: ProviderInstanceId.make("codex"),
    model: "gpt-5.4",
  },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: "main",
  worktreePath: null,
  latestTurn: null,
  turns: [],
  createdAt: "2026-04-01T00:00:00.000Z",
  updatedAt: "2026-04-01T00:00:00.000Z",
  archivedAt: null,
  deletedAt: null,
  messages: [],
  proposedPlans: [],
  activities: [],
  checkpoints: [],
  session: null,
};

const RUNNING_TURN_ID = TurnId.make("turn-running-1");

function makeRunningThread(
  messages: ReadonlyArray<OrchestrationMessage> = [],
): OrchestrationThread {
  return {
    ...BASE_THREAD,
    updatedAt: "2026-07-07T21:00:00.000Z",
    latestTurn: {
      turnId: RUNNING_TURN_ID,
      state: "running",
      requestedAt: "2026-07-07T21:00:00.000Z",
      startedAt: "2026-07-07T21:00:01.000Z",
      completedAt: null,
      assistantMessageId: null,
    },
    messages,
    session: {
      threadId: THREAD_ID,
      status: "running",
      providerName: "claudeAgent",
      runtimeMode: "full-access",
      activeTurnId: RUNNING_TURN_ID,
      lastError: null,
      updatedAt: "2026-07-07T21:00:01.000Z",
    },
  };
}

type TestThreadInput = OrchestrationThreadStreamItem | Error;

function testSession(client: WsRpcProtocolClient): RpcSession.RpcSession {
  return {
    client,
    initialConfig: Effect.never,
    ready: Effect.void,
    probe: Effect.void,
    closed: Effect.never,
  };
}

function awaitThreadState(
  observed: Queue.Queue<EnvironmentThreadState>,
  predicate: (state: EnvironmentThreadState) => boolean,
) {
  return Queue.take(observed).pipe(
    Effect.repeat({
      until: predicate,
    }),
  );
}

const advanceActiveReconcileInterval = Effect.gen(function* () {
  yield* Effect.yieldNow;
  yield* TestClock.adjust("2 seconds");
  yield* Effect.yieldNow;
});

const makeHarness = Effect.fn("TestEnvironmentThreads.makeHarness")(function* (options?: {
  readonly cached?: OrchestrationThread;
  readonly httpSnapshot?: Option.Option<OrchestrationThreadDetailSnapshot>;
  readonly reconciliationPolicy?: ThreadReconciliationPolicy;
  readonly revisionSequence?: number;
  readonly projectionSequence?: number;
  readonly cachedStorageEpoch?: string | null;
  readonly cachedLatestSequence?: number;
  readonly revisionStorageEpoch?: string;
  readonly revisionEventId?: EventId | null;
  readonly preserveMissingSnapshotMetadata?: boolean;
}) {
  const inputs = yield* Queue.unbounded<TestThreadInput>();
  const observed = yield* Queue.unbounded<EnvironmentThreadState>();
  const latest = yield* Ref.make<EnvironmentThreadState>(EMPTY_ENVIRONMENT_THREAD_STATE);
  const retryCount = yield* Ref.make(0);
  const subscriptionCount = yield* Ref.make(0);
  const loaderCalls = yield* Ref.make(0);
  const revisionCalls = yield* Ref.make(0);
  const revisionCallTimes = yield* Ref.make<ReadonlyArray<number>>([]);
  const revisionAvailable = yield* Ref.make(true);
  const revisionGone = yield* Ref.make(false);
  const revisionSnapshotRequired = yield* Ref.make(false);
  const explicitRevisionSequence = yield* Ref.make<Option.Option<number>>(
    Option.fromNullishOr(options?.revisionSequence),
  );
  const explicitProjectionSequence = yield* Ref.make<Option.Option<number>>(
    Option.fromNullishOr(options?.projectionSequence),
  );
  const explicitRevisionEventId = yield* Ref.make<EventId | null | undefined>(
    options?.revisionEventId,
  );
  const revisionStorageEpoch = yield* Ref.make(
    options?.revisionStorageEpoch ?? options?.cachedStorageEpoch ?? STORAGE_EPOCH,
  );
  const normalizeSnapshot = (
    snapshot: OrchestrationThreadDetailSnapshot,
  ): OrchestrationThreadDetailSnapshot => {
    if (options?.preserveMissingSnapshotMetadata === true) {
      return snapshot;
    }
    const latestSequence = snapshot.latestSequence ?? snapshot.snapshotSequence;
    return {
      ...snapshot,
      storageEpoch: snapshot.storageEpoch ?? options?.revisionStorageEpoch ?? STORAGE_EPOCH,
      latestSequence,
      latestEventId:
        snapshot.latestEventId !== undefined
          ? snapshot.latestEventId
          : markerEventId(latestSequence),
    };
  };
  const httpSnapshot = yield* Ref.make<Option.Option<OrchestrationThreadDetailSnapshot>>(
    Option.map(
      options?.httpSnapshot ?? Option.none<OrchestrationThreadDetailSnapshot>(),
      normalizeSnapshot,
    ),
  );
  const httpReconcileResult = yield* Ref.make<Option.Option<ThreadSnapshotLoadResult>>(
    Option.none(),
  );
  const lastSubscribeAfterSequence = yield* Ref.make<number | undefined>(undefined);
  const lastSubscribeStorageEpoch = yield* Ref.make<string | undefined>(undefined);
  const lastSubscribeVerifiedRevision = yield* Ref.make<number | undefined>(undefined);
  const lastSubscribeObservedRevision = yield* Ref.make<number | undefined>(undefined);
  const lastSubscribeObservedEventId = yield* Ref.make<EventId | null | undefined>(undefined);
  const lastSubscribeObservedDataAudience = yield* Ref.make<DataAudience | undefined>(undefined);
  const subscribeAfterSequences = yield* Ref.make<ReadonlyArray<number | undefined>>([]);
  const savedThreads = yield* Ref.make<ReadonlyArray<OrchestrationThreadDetailSnapshot>>([]);
  const removedThreads = yield* Ref.make<ReadonlyArray<ThreadId>>([]);
  const supervisorState = yield* SubscriptionRef.make<SupervisorConnectionState>(
    AVAILABLE_CONNECTION_STATE,
  );
  const streamFrom = (queue: Queue.Queue<TestThreadInput>) =>
    Stream.fromQueue(queue).pipe(
      Stream.mapEffect((input) =>
        input instanceof Error ? Effect.fail(input) : Effect.succeed(input),
      ),
    );
  const client = {
    [ORCHESTRATION_WS_METHODS.subscribeThread]: (input: {
      readonly afterSequence?: number;
      readonly storageEpoch?: string;
      readonly verifiedRevision?: number;
      readonly observedRevision?: number;
      readonly observedEventId?: EventId | null;
      readonly observedDataAudience?: DataAudience;
    }) =>
      Stream.unwrap(
        Ref.updateAndGet(subscriptionCount, (count) => count + 1).pipe(
          Effect.andThen(Ref.set(lastSubscribeAfterSequence, input.afterSequence)),
          Effect.andThen(Ref.set(lastSubscribeStorageEpoch, input.storageEpoch)),
          Effect.andThen(Ref.set(lastSubscribeVerifiedRevision, input.verifiedRevision)),
          Effect.andThen(Ref.set(lastSubscribeObservedRevision, input.observedRevision)),
          Effect.andThen(Ref.set(lastSubscribeObservedEventId, input.observedEventId)),
          Effect.andThen(Ref.set(lastSubscribeObservedDataAudience, input.observedDataAudience)),
          Effect.andThen(
            Ref.update(subscribeAfterSequences, (current) => [...current, input.afterSequence]),
          ),
          Effect.as(streamFrom(inputs)),
        ),
      ),
  } as unknown as WsRpcProtocolClient;
  const supervisorSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
    Option.some(testSession(client)),
  );
  const prepared = yield* SubscriptionRef.make<Option.Option<PreparedConnection>>(
    Option.some(PREPARED),
  );
  const snapshotLoader = ThreadSnapshotLoader.of({
    load: (_prepared, threadId) =>
      Ref.update(loaderCalls, (count) => count + 1).pipe(
        Effect.andThen(() =>
          threadId === THREAD_ID
            ? Ref.get(httpSnapshot).pipe(Effect.map(Option.map(normalizeSnapshot)))
            : Effect.succeed(Option.none<OrchestrationThreadDetailSnapshot>()),
        ),
      ),
    loadForReconcile: (_prepared, threadId) =>
      Ref.update(loaderCalls, (count) => count + 1).pipe(
        Effect.andThen(
          Effect.gen(function* () {
            if (threadId !== THREAD_ID) {
              return { kind: "unavailable" } as const;
            }
            const explicit = yield* Ref.get(httpReconcileResult);
            if (Option.isSome(explicit)) {
              return explicit.value.kind === "found"
                ? { ...explicit.value, snapshot: normalizeSnapshot(explicit.value.snapshot) }
                : explicit.value;
            }
            const snapshot = yield* Ref.get(httpSnapshot);
            return Option.match(snapshot, {
              onNone: () => ({ kind: "unavailable" }) as const,
              onSome: (value) => ({ kind: "found", snapshot: normalizeSnapshot(value) }) as const,
            });
          }),
        ),
      ),
  });
  const revisionLoader = ThreadRevisionLoader.of({
    load: (_prepared, threadId) =>
      Effect.gen(function* () {
        yield* Ref.update(revisionCalls, (count) => count + 1);
        const now = yield* Clock.currentTimeMillis;
        yield* Ref.update(revisionCallTimes, (current) => [...current, now]);
        if (threadId !== THREAD_ID || !(yield* Ref.get(revisionAvailable))) {
          return { kind: "unavailable" } as const;
        }
        if (yield* Ref.get(revisionGone)) {
          return { kind: "gone" } as const;
        }
        if (yield* Ref.get(revisionSnapshotRequired)) {
          return { kind: "snapshot-required" } as const;
        }
        const explicit = yield* Ref.get(explicitRevisionSequence);
        const latestSequence = Option.isSome(explicit)
          ? explicit.value
          : Option.match(yield* Ref.get(httpSnapshot), {
              onNone: () => (options?.cached === undefined ? 0 : CACHED_SNAPSHOT_SEQUENCE),
              onSome: (current) => current.snapshotSequence,
            });
        const projectionSequence = Option.getOrElse(
          yield* Ref.get(explicitProjectionSequence),
          () => latestSequence,
        );
        const storageEpoch = yield* Ref.get(revisionStorageEpoch);
        const configuredEventId = yield* Ref.get(explicitRevisionEventId);
        const latestEventId =
          configuredEventId === undefined ? markerEventId(latestSequence) : configuredEventId;
        const revision = { storageEpoch, latestSequence, latestEventId, projectionSequence };
        return {
          kind: "found",
          revision,
          responseBytes: new TextEncoder().encode(
            `{"storageEpoch":"${storageEpoch}","latestSequence":${latestSequence},"latestEventId":${latestEventId === null ? "null" : `"${latestEventId}"`},"projectionSequence":${projectionSequence}}`,
          ).byteLength,
        } as const;
      }),
  });
  const localActivity = yield* Queue.unbounded<{
    readonly environmentId: EnvironmentId;
    readonly threadId: ThreadId;
    readonly reason: "locally-initiated-turn";
  }>();
  const reconciliationActivity = ThreadReconciliationActivity.of({
    publish: (event) => Queue.offer(localActivity, event).pipe(Effect.asVoid),
    events: Stream.fromQueue(localActivity),
  });
  const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
    target: TARGET,
    state: supervisorState,
    session: supervisorSession,
    prepared,
    connect: Effect.void,
    disconnect: Effect.void,
    retryNow: Ref.update(retryCount, (count) => count + 1),
  } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
  const cache = Persistence.EnvironmentCacheStore.of({
    loadShell: () => Effect.succeed(Option.none()),
    saveShell: () => Effect.void,
    loadThread: (_environmentId, threadId) =>
      Effect.succeed(
        threadId === THREAD_ID && options?.cached !== undefined
          ? Option.some({
              snapshotSequence: CACHED_SNAPSHOT_SEQUENCE,
              thread: options.cached,
              ...(options.cachedStorageEpoch === null
                ? {}
                : { storageEpoch: options.cachedStorageEpoch ?? STORAGE_EPOCH }),
              latestSequence: options.cachedLatestSequence ?? CACHED_SNAPSHOT_SEQUENCE,
              latestEventId: markerEventId(
                options.cachedLatestSequence ?? CACHED_SNAPSHOT_SEQUENCE,
              ),
              observedRevision: options.cachedLatestSequence ?? CACHED_SNAPSHOT_SEQUENCE,
              observedEventId: markerEventId(
                options.cachedLatestSequence ?? CACHED_SNAPSHOT_SEQUENCE,
              ),
            })
          : Option.none(),
      ),
    saveThread: (_environmentId, thread) =>
      Ref.update(savedThreads, (current) => [...current, thread]),
    removeThread: (_environmentId, threadId) =>
      Ref.update(removedThreads, (current) => [...current, threadId]),
    loadServerConfig: () => Effect.succeed(Option.none()),
    saveServerConfig: () => Effect.void,
    loadVcsRefs: () => Effect.succeed(Option.none()),
    saveVcsRefs: () => Effect.void,
    clear: () => Effect.void,
  });
  const threadState = yield* makeEnvironmentThreadState(THREAD_ID, {
    reconciliationPolicy: options?.reconciliationPolicy ?? DEFAULT_THREAD_RECONCILIATION_POLICY,
  }).pipe(
    Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
    Effect.provideService(Persistence.EnvironmentCacheStore, cache),
    Effect.provideService(ThreadSnapshotLoader, snapshotLoader),
    Effect.provideService(ThreadRevisionLoader, revisionLoader),
    Effect.provideService(ThreadReconciliationActivity, reconciliationActivity),
  );
  yield* SubscriptionRef.changes(threadState).pipe(
    Stream.runForEach((state) =>
      Ref.set(latest, state).pipe(Effect.andThen(Queue.offer(observed, state))),
    ),
    Effect.forkScoped,
  );

  return {
    inputs,
    observed,
    latest,
    retryCount,
    subscriptionCount,
    loaderCalls,
    revisionCalls,
    revisionCallTimes,
    revisionAvailable,
    revisionGone,
    revisionSnapshotRequired,
    explicitRevisionSequence,
    explicitProjectionSequence,
    explicitRevisionEventId,
    revisionStorageEpoch,
    httpSnapshot,
    httpReconcileResult,
    lastSubscribeAfterSequence,
    lastSubscribeStorageEpoch,
    lastSubscribeVerifiedRevision,
    lastSubscribeObservedRevision,
    lastSubscribeObservedEventId,
    lastSubscribeObservedDataAudience,
    subscribeAfterSequences,
    supervisorState,
    supervisorSession,
    savedThreads,
    removedThreads,
    replaceSession: SubscriptionRef.set(supervisorSession, Option.some(testSession(client))),
    markLocalTurn: reconciliationActivity.publish({
      environmentId: TARGET.environmentId,
      threadId: THREAD_ID,
      reason: "locally-initiated-turn",
    }),
  };
});

const snapshot = (
  thread: OrchestrationThread,
  snapshotSequence = 1,
  storageEpoch = STORAGE_EPOCH,
): OrchestrationThreadStreamItem => ({
  kind: "snapshot",
  storageEpoch,
  snapshot: {
    snapshotSequence,
    thread,
    storageEpoch,
    latestSequence: snapshotSequence,
    latestEventId: markerEventId(snapshotSequence),
  },
});

const titleUpdated = (
  title: string,
  sequence = 2,
  storageEpoch = STORAGE_EPOCH,
): OrchestrationThreadStreamItem => ({
  kind: "event",
  storageEpoch,
  event: {
    eventId: EventId.make("event-title"),
    sequence,
    occurredAt: "2026-04-01T01:00:00.000Z",
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    aggregateKind: "thread",
    aggregateId: THREAD_ID,
    type: "thread.meta-updated",
    payload: {
      threadId: THREAD_ID,
      title,
      updatedAt: "2026-04-01T01:00:00.000Z",
    },
  },
});

const legacyTitleUpdated = (title: string, sequence: number): OrchestrationThreadStreamItem => {
  const { storageEpoch: _storageEpoch, ...item } = titleUpdated(title, sequence);
  return item;
};

const legacySnapshot = (
  thread: OrchestrationThread,
  snapshotSequence: number,
): OrchestrationThreadStreamItem => ({
  kind: "snapshot",
  snapshot: {
    snapshotSequence,
    thread,
  },
});

const activityAppended = (
  summary: string,
  sequence = 3,
  storageEpoch = STORAGE_EPOCH,
): OrchestrationThreadStreamItem => ({
  kind: "event",
  storageEpoch,
  event: {
    eventId: EventId.make(`event-activity-${sequence}`),
    sequence,
    occurredAt: "2026-07-07T21:00:02.500Z",
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    aggregateKind: "thread",
    aggregateId: THREAD_ID,
    type: "thread.activity-appended",
    payload: {
      threadId: THREAD_ID,
      activity: {
        id: EventId.make(`activity-${sequence}`),
        tone: "info",
        kind: "test.activity",
        summary,
        payload: {},
        turnId: RUNNING_TURN_ID,
        sequence,
        createdAt: "2026-07-07T21:00:02.500Z",
      },
    },
  },
});

const deleted = (sequence = 3, storageEpoch = STORAGE_EPOCH): OrchestrationThreadStreamItem => ({
  kind: "event",
  storageEpoch,
  event: {
    eventId: EventId.make("event-deleted"),
    sequence,
    occurredAt: "2026-04-01T02:00:00.000Z",
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    aggregateKind: "thread",
    aggregateId: THREAD_ID,
    type: "thread.deleted",
    payload: {
      threadId: THREAD_ID,
      deletedAt: "2026-04-01T02:00:00.000Z",
    },
  },
});

const legacyDeleted = (sequence: number): OrchestrationThreadStreamItem => {
  const { storageEpoch: _storageEpoch, ...item } = deleted(sequence);
  return item;
};

const forcedDeleted = (sequence: number): OrchestrationThreadStreamItem => ({
  ...deleted(sequence),
  force: true,
});

const archived = (sequence = 3, storageEpoch = STORAGE_EPOCH): OrchestrationThreadStreamItem => ({
  kind: "event",
  storageEpoch,
  event: {
    eventId: EventId.make("event-archived"),
    sequence,
    occurredAt: "2026-04-01T02:00:00.000Z",
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    aggregateKind: "thread",
    aggregateId: THREAD_ID,
    type: "thread.archived",
    payload: {
      threadId: THREAD_ID,
      archivedAt: "2026-04-01T02:00:00.000Z",
      updatedAt: "2026-04-01T02:00:00.000Z",
    },
  },
});

const unarchived = (sequence = 4, storageEpoch = STORAGE_EPOCH): OrchestrationThreadStreamItem => ({
  kind: "event",
  storageEpoch,
  event: {
    eventId: EventId.make("event-unarchived"),
    sequence,
    occurredAt: "2026-04-01T02:01:00.000Z",
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    aggregateKind: "thread",
    aggregateId: THREAD_ID,
    type: "thread.unarchived",
    payload: {
      threadId: THREAD_ID,
      updatedAt: "2026-04-01T02:01:00.000Z",
    },
  },
});

const sessionReady = (
  sequence = 3,
  storageEpoch = STORAGE_EPOCH,
): OrchestrationThreadStreamItem => ({
  kind: "event",
  storageEpoch,
  event: {
    eventId: EventId.make("event-session-ready"),
    sequence,
    occurredAt: "2026-07-07T21:00:04.000Z",
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    aggregateKind: "thread",
    aggregateId: THREAD_ID,
    type: "thread.session-set",
    payload: {
      threadId: THREAD_ID,
      session: {
        threadId: THREAD_ID,
        status: "ready",
        providerName: "claudeAgent",
        runtimeMode: "full-access",
        activeTurnId: null,
        lastError: null,
        updatedAt: "2026-07-07T21:00:04.000Z",
      },
    },
  },
});

describe("EnvironmentThreads", () => {
  it.effect("publishes cached data immediately from a warm cache", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: BASE_THREAD });
      const state = yield* awaitThreadState(harness.observed, (value) => Option.isSome(value.data));

      expect(Option.getOrThrow(state.data)).toEqual(BASE_THREAD);
      expect(Option.isNone(state.error)).toBe(true);
    }),
  );

  it.effect("resumes a warm cache via afterSequence without an HTTP fetch", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: BASE_THREAD });

      // The warm cache reaches live from the cached data, and a live event
      // applies on top of it.
      yield* Queue.offer(harness.inputs, titleUpdated("Live title", CACHED_SNAPSHOT_SEQUENCE + 1));
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.title === "Live title",
      );

      // The subscription resumed from the cached sequence and never fetched the
      // full snapshot over HTTP.
      expect(yield* Ref.get(harness.lastSubscribeAfterSequence)).toBe(CACHED_SNAPSHOT_SEQUENCE);
      expect(yield* Ref.get(harness.lastSubscribeObservedDataAudience)).toBe("private");
      expect(yield* Ref.get(harness.loaderCalls)).toBe(0);
    }),
  );

  it.effect("keeps global replay and per-thread revision cursors distinct", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        cachedLatestSequence: 6,
        revisionSequence: 6,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");
      for (let index = 0; index < 5; index += 1) {
        yield* Effect.yieldNow;
      }

      expect(yield* Ref.get(harness.lastSubscribeAfterSequence)).toBe(7);
      expect(yield* Ref.get(harness.lastSubscribeVerifiedRevision)).toBe(6);
      expect(yield* Ref.get(harness.lastSubscribeObservedRevision)).toBe(6);
      for (const delay of [2, 2, 4, 8]) {
        yield* TestClock.adjust(`${delay} seconds`);
        yield* Effect.yieldNow;
      }
      expect(yield* Ref.get(harness.loaderCalls)).toBe(0);
    }),
  );

  it.effect(
    "bounds full loads for a permanently-running unchanged 3.8 MB thread and backs revision checks off",
    () =>
      Effect.gen(function* () {
        const largeMessage: OrchestrationMessage = {
          id: MessageId.make("message-3-8mb-running-fixture"),
          role: "assistant",
          text: "x".repeat(3_800_000),
          attachments: [],
          turnId: RUNNING_TURN_ID,
          streaming: false,
          createdAt: "2026-07-07T21:00:03.000Z",
          updatedAt: "2026-07-07T21:00:03.000Z",
        };
        const harness = yield* makeHarness({
          cached: makeRunningThread([largeMessage]),
          revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
        });
        yield* awaitThreadState(
          harness.observed,
          (value) =>
            value.status === "live" &&
            Option.isSome(value.data) &&
            value.data.value.messages.at(0)?.text.length === 3_800_000,
        );

        for (let index = 0; index < 15; index += 1) {
          yield* TestClock.adjust("2 seconds");
          yield* Effect.yieldNow;
        }
        for (const delay of [4, 8, 16, 32, 60]) {
          yield* TestClock.adjust(`${delay} seconds`);
          yield* Effect.yieldNow;
        }

        const callTimes = yield* Ref.get(harness.revisionCallTimes);
        const intervals = callTimes.slice(1).map((time, index) => time - callTimes[index]!);
        expect(yield* Ref.get(harness.loaderCalls)).toBe(0);
        expect(yield* Ref.get(harness.revisionCalls)).toBe(20);
        expect(intervals.slice(0, 14)).toEqual(Array.from({ length: 14 }, () => 2_000));
        expect(intervals.slice(-5)).toEqual([4_000, 8_000, 16_000, 32_000, 60_000]);
      }),
  );

  it.effect("backs off repeated snapshot-required recoveries on idle threads", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        reconciliationPolicy: {
          fastIntervalMs: 2_000,
          fastWindowMs: 0,
          backoffMultiplier: 2,
          maxBackoffMs: 60_000,
        },
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");
      yield* Ref.set(harness.revisionSnapshotRequired, true);
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE,
          thread: BASE_THREAD,
          storageEpoch: STORAGE_EPOCH,
          latestSequence: CACHED_SNAPSHOT_SEQUENCE,
          latestEventId: markerEventId(CACHED_SNAPSHOT_SEQUENCE),
        }),
      );

      for (const delay of [2, 4, 8]) {
        yield* TestClock.adjust(`${delay} seconds`);
        yield* Effect.yieldNow;
      }

      expect(yield* Ref.get(harness.revisionCallTimes)).toEqual([2_000, 6_000, 14_000]);
    }),
  );

  it.effect("keeps polling an idle mounted thread after the fast window", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
        reconciliationPolicy: {
          fastIntervalMs: 2_000,
          fastWindowMs: 4_000,
          backoffMultiplier: 2,
          maxBackoffMs: 60_000,
        },
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");

      yield* TestClock.adjust("2 seconds");
      yield* Effect.yieldNow;
      yield* TestClock.adjust("2 seconds");
      yield* Effect.yieldNow;
      expect(yield* Ref.get(harness.revisionCallTimes)).toEqual([2_000, 4_000]);

      const healedThread = {
        ...BASE_THREAD,
        title: "Idle missed event healed",
        updatedAt: "2026-07-16T02:00:00.000Z",
      };
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(8));
      yield* Ref.set(harness.explicitProjectionSequence, Option.some(8));
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: 8,
          thread: healedThread,
          storageEpoch: STORAGE_EPOCH,
        }),
      );

      yield* TestClock.adjust("60 seconds");
      yield* Effect.yieldNow;
      const healed = yield* awaitThreadState(
        harness.observed,
        (value) => Option.isSome(value.data) && value.data.value.title === healedThread.title,
      );

      expect(Option.getOrThrow(healed.data).title).toBe(healedThread.title);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
    }),
  );

  it.effect("reconciles a cached thread away when its revision endpoint reports gone", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");
      yield* Ref.set(harness.revisionGone, true);

      yield* advanceActiveReconcileInterval;

      const reconciled = yield* Ref.get(harness.latest);
      expect(reconciled.status).toBe("deleted");
      expect(Option.isNone(reconciled.data)).toBe(true);
      expect(yield* Ref.get(harness.removedThreads)).toEqual([THREAD_ID]);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(0);
      expect(yield* Ref.get(harness.revisionCalls)).toBe(1);

      yield* Queue.offer(
        harness.inputs,
        snapshot({ ...BASE_THREAD, title: "Late stale snapshot" }, CACHED_SNAPSHOT_SEQUENCE + 1),
      );
      yield* Effect.yieldNow;

      yield* TestClock.adjust("2 minutes");
      yield* Effect.yieldNow;
      expect((yield* Ref.get(harness.latest)).status).toBe("deleted");
      expect(yield* Ref.get(harness.revisionCalls)).toBe(1);
      expect(yield* Ref.get(harness.removedThreads)).toEqual([THREAD_ID]);
    }),
  );

  it.effect("retains a cached thread while its revision endpoint is unavailable", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");
      yield* Ref.set(harness.revisionAvailable, false);

      yield* advanceActiveReconcileInterval;
      yield* advanceActiveReconcileInterval;

      const retained = yield* Ref.get(harness.latest);
      expect(retained.status).toBe("live");
      expect(Option.getOrThrow(retained.data).id).toBe(THREAD_ID);
      expect(yield* Ref.get(harness.removedThreads)).toEqual([]);
      expect(yield* Ref.get(harness.revisionCalls)).toBe(2);
    }),
  );

  it.effect("evicts a cached thread when revision denial reconciles to a scoped 404", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");
      yield* Ref.set(harness.revisionSnapshotRequired, true);
      yield* Ref.set(harness.httpReconcileResult, Option.some({ kind: "missing" }));

      yield* advanceActiveReconcileInterval;

      const reconciled = yield* Ref.get(harness.latest);
      expect(reconciled.status).toBe("deleted");
      expect(Option.isNone(reconciled.data)).toBe(true);
      expect(yield* Ref.get(harness.removedThreads)).toEqual([THREAD_ID]);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
      expect(yield* Ref.get(harness.revisionCalls)).toBe(1);
    }),
  );

  it.effect("refreshes a visible thread when revision denial reconciles to a scoped snapshot", () =>
    Effect.gen(function* () {
      const refreshedThread = {
        ...BASE_THREAD,
        dataAudience: "factory" as const,
        title: "Factory-visible refresh",
        updatedAt: "2026-07-18T14:00:00.000Z",
      };
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");
      yield* Ref.set(harness.revisionSnapshotRequired, true);
      yield* Ref.set(
        harness.httpReconcileResult,
        Option.some({
          kind: "found",
          snapshot: {
            snapshotSequence: CACHED_SNAPSHOT_SEQUENCE + 1,
            thread: refreshedThread,
            storageEpoch: STORAGE_EPOCH,
            latestSequence: CACHED_SNAPSHOT_SEQUENCE + 1,
            latestEventId: markerEventId(CACHED_SNAPSHOT_SEQUENCE + 1),
          },
        }),
      );

      yield* advanceActiveReconcileInterval;

      const reconciled = yield* awaitThreadState(
        harness.observed,
        (value) => Option.isSome(value.data) && value.data.value.title === refreshedThread.title,
      );
      expect(reconciled.status).toBe("live");
      expect(Option.getOrThrow(reconciled.data).dataAudience).toBe("factory");
      expect(yield* Ref.get(harness.removedThreads)).toEqual([]);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
      expect(yield* Ref.get(harness.revisionCalls)).toBe(1);
    }),
  );

  it.effect("resets all cursors and loads one authoritative snapshot on an epoch change", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");

      const restoredThread = {
        ...BASE_THREAD,
        title: "Restored epoch snapshot",
        updatedAt: "2026-07-16T02:10:00.000Z",
      };
      yield* Ref.set(harness.revisionStorageEpoch, RESTORED_STORAGE_EPOCH);
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(6));
      yield* Ref.set(harness.explicitProjectionSequence, Option.some(6));
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: 6,
          thread: restoredThread,
          storageEpoch: RESTORED_STORAGE_EPOCH,
        }),
      );

      yield* TestClock.adjust("2 seconds");
      yield* Effect.yieldNow;
      yield* awaitThreadState(
        harness.observed,
        (value) => Option.isSome(value.data) && value.data.value.title === restoredThread.title,
      );
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);

      yield* harness.replaceSession;
      yield* Effect.yieldNow;
      expect(yield* Ref.get(harness.lastSubscribeAfterSequence)).toBe(6);
      expect(yield* Ref.get(harness.lastSubscribeStorageEpoch)).toBe(RESTORED_STORAGE_EPOCH);

      yield* Queue.offer(
        harness.inputs,
        titleUpdated("First post-restore event", 7, RESTORED_STORAGE_EPOCH),
      );
      const postRestore = yield* awaitThreadState(
        harness.observed,
        (value) =>
          Option.isSome(value.data) && value.data.value.title === "First post-restore event",
      );
      expect(Option.getOrThrow(postRestore.data).title).toBe("First post-restore event");
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
    }),
  );

  it.effect("recovers once before accepting legacy stream frames without a storage epoch", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
        preserveMissingSnapshotMetadata: true,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");

      const recoveredThread = {
        ...BASE_THREAD,
        title: "Authoritative legacy recovery",
        updatedAt: "2026-07-16T02:12:00.000Z",
      };
      yield* Ref.set(harness.revisionAvailable, false);
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: 8,
          thread: recoveredThread,
        }),
      );
      yield* Queue.offer(harness.inputs, legacyTitleUpdated("Unverified legacy event", 8));
      for (let index = 0; index < 5; index += 1) {
        yield* Effect.yieldNow;
      }

      yield* TestClock.adjust("2 seconds");
      yield* Effect.yieldNow;
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) => Option.isSome(value.data) && value.data.value.title === recoveredThread.title,
      );
      expect(Option.getOrThrow(recovered.data).title).toBe(recoveredThread.title);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);

      yield* Queue.offer(harness.inputs, legacyTitleUpdated("Subsequent legacy event", 9));
      const updated = yield* awaitThreadState(
        harness.observed,
        (value) =>
          Option.isSome(value.data) && value.data.value.title === "Subsequent legacy event",
      );
      expect(Option.getOrThrow(updated.data).title).toBe("Subsequent legacy event");
      yield* TestClock.adjust("8 seconds");
      yield* Effect.yieldNow;
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
      expect(yield* Ref.get(harness.subscriptionCount)).toBe(1);

      const deletionHarness = yield* makeHarness({
        cached: BASE_THREAD,
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
        preserveMissingSnapshotMetadata: true,
      });
      yield* awaitThreadState(deletionHarness.observed, (value) => value.status === "live");
      yield* Ref.set(deletionHarness.revisionAvailable, false);
      yield* Ref.set(deletionHarness.httpReconcileResult, Option.some({ kind: "missing" }));
      yield* Queue.offer(deletionHarness.inputs, legacyDeleted(8));
      for (let index = 0; index < 5; index += 1) {
        yield* Effect.yieldNow;
      }

      yield* TestClock.adjust("2 seconds");
      yield* Effect.yieldNow;
      const deletedState = yield* awaitThreadState(
        deletionHarness.observed,
        (value) => value.status === "deleted",
      );
      expect(deletedState.status).toBe("deleted");
      expect(yield* Ref.get(deletionHarness.loaderCalls)).toBe(1);
      yield* TestClock.adjust("8 seconds");
      yield* Effect.yieldNow;
      expect(yield* Ref.get(deletionHarness.loaderCalls)).toBe(1);
      expect(yield* Ref.get(deletionHarness.subscriptionCount)).toBe(1);
    }),
  );

  it.effect("revalidates a warm unknown-epoch cache at every session boundary", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: {
          ...BASE_THREAD,
          title: "Stale unknown-epoch cache",
        },
        cachedStorageEpoch: null,
        cachedLatestSequence: 10,
      });

      const firstAuthoritativeThread = {
        ...BASE_THREAD,
        title: "First authoritative legacy snapshot",
      };
      yield* Queue.offer(harness.inputs, legacySnapshot(firstAuthoritativeThread, 4));
      const firstRecovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          Option.isSome(value.data) && value.data.value.title === firstAuthoritativeThread.title,
      );
      expect(Option.getOrThrow(firstRecovered.data).title).toBe(firstAuthoritativeThread.title);
      expect(yield* Ref.get(harness.lastSubscribeAfterSequence)).toBeUndefined();

      yield* Queue.offer(harness.inputs, legacyTitleUpdated("First legacy live event", 5));
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          Option.isSome(value.data) && value.data.value.title === "First legacy live event",
      );

      yield* harness.replaceSession;
      for (let index = 0; index < 5; index += 1) {
        yield* Effect.yieldNow;
      }
      const secondAuthoritativeThread = {
        ...BASE_THREAD,
        title: "Reconnect authoritative legacy snapshot",
      };
      yield* Queue.offer(harness.inputs, legacySnapshot(secondAuthoritativeThread, 3));
      const secondRecovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          Option.isSome(value.data) && value.data.value.title === secondAuthoritativeThread.title,
      );

      expect(Option.getOrThrow(secondRecovered.data).title).toBe(secondAuthoritativeThread.title);
      expect(yield* Ref.get(harness.subscribeAfterSequences)).toEqual([undefined, undefined]);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(0);
      expect(yield* Ref.get(harness.subscriptionCount)).toBe(2);
    }),
  );

  it.effect("applies a forced deletion below the stale same-epoch cursor", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");

      yield* Queue.offer(harness.inputs, forcedDeleted(CACHED_SNAPSHOT_SEQUENCE - 1));
      const deletedState = yield* awaitThreadState(
        harness.observed,
        (value) => value.status === "deleted",
      );

      expect(deletedState.status).toBe("deleted");
      expect(yield* Ref.get(harness.removedThreads)).toEqual([THREAD_ID]);
      expect(yield* Ref.get(harness.subscriptionCount)).toBe(1);
    }),
  );

  it.effect("defers every new-epoch event until authoritative recovery finishes", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");

      const recoveredThread = {
        ...BASE_THREAD,
        title: "Authoritative epoch recovery",
        updatedAt: "2026-07-16T02:15:00.000Z",
      };
      yield* Ref.set(harness.revisionStorageEpoch, RESTORED_STORAGE_EPOCH);
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(8));
      yield* Ref.set(harness.explicitProjectionSequence, Option.some(8));
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: 8,
          thread: recoveredThread,
          storageEpoch: RESTORED_STORAGE_EPOCH,
        }),
      );

      yield* Queue.offer(
        harness.inputs,
        titleUpdated("Deferred epoch event 7", 7, RESTORED_STORAGE_EPOCH),
      );
      yield* Queue.offer(
        harness.inputs,
        titleUpdated("Deferred epoch event 8", 8, RESTORED_STORAGE_EPOCH),
      );
      for (let index = 0; index < 5; index += 1) {
        yield* Effect.yieldNow;
      }
      expect(Option.getOrThrow((yield* Ref.get(harness.latest)).data).title).toBe(
        BASE_THREAD.title,
      );

      yield* TestClock.adjust("2 seconds");
      yield* Effect.yieldNow;
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) => Option.isSome(value.data) && value.data.value.title === recoveredThread.title,
      );
      expect(Option.getOrThrow(recovered.data).title).toBe(recoveredThread.title);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
    }),
  );

  it.effect("forces one authoritative reset when a same-epoch marker moves backwards", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");

      const restoredThread = {
        ...BASE_THREAD,
        title: "Same-epoch restored title",
        updatedAt: "2026-07-16T02:20:00.000Z",
      };
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(6));
      yield* Ref.set(harness.explicitProjectionSequence, Option.some(6));
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: 6,
          thread: restoredThread,
          storageEpoch: STORAGE_EPOCH,
        }),
      );

      yield* TestClock.adjust("2 seconds");
      yield* Effect.yieldNow;
      const restored = yield* awaitThreadState(
        harness.observed,
        (value) => Option.isSome(value.data) && value.data.value.title === restoredThread.title,
      );

      expect(Option.getOrThrow(restored.data).title).toBe(restoredThread.title);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
      yield* TestClock.adjust("4 seconds");
      yield* Effect.yieldNow;
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
    }),
  );

  it.effect("resets when restored history reuses the same epoch and sequence", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");

      const replacementEventId = EventId.make("event-restored-marker-7");
      const restoredThread = {
        ...BASE_THREAD,
        title: "Restored equal-sequence history",
        updatedAt: "2026-07-16T02:22:00.000Z",
      };
      yield* Ref.set(harness.explicitRevisionEventId, replacementEventId);
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE,
          thread: restoredThread,
          storageEpoch: STORAGE_EPOCH,
          latestSequence: CACHED_SNAPSHOT_SEQUENCE,
          latestEventId: replacementEventId,
        }),
      );

      yield* TestClock.adjust("2 seconds");
      yield* Effect.yieldNow;
      const restored = yield* awaitThreadState(
        harness.observed,
        (value) => Option.isSome(value.data) && value.data.value.title === restoredThread.title,
      );

      expect(Option.getOrThrow(restored.data).title).toBe(restoredThread.title);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
      yield* TestClock.adjust("4 seconds");
      yield* Effect.yieldNow;
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
      yield* harness.replaceSession;
      yield* Effect.yieldNow;
      expect(yield* Ref.get(harness.lastSubscribeObservedRevision)).toBe(CACHED_SNAPSHOT_SEQUENCE);
      expect(yield* Ref.get(harness.lastSubscribeObservedEventId)).toBe(replacementEventId);
    }),
  );

  it.effect("resets when a same-epoch marker moves behind an unverified live event", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        cachedLatestSequence: 6,
        revisionSequence: 10,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");

      yield* Queue.offer(harness.inputs, titleUpdated("Unverified live title", 10));
      yield* awaitThreadState(
        harness.observed,
        (value) => Option.isSome(value.data) && value.data.value.title === "Unverified live title",
      );

      const restoredThread = {
        ...BASE_THREAD,
        title: "Restored behind pending event",
        updatedAt: "2026-07-16T02:25:00.000Z",
      };
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(8));
      yield* Ref.set(harness.explicitProjectionSequence, Option.some(8));
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: 8,
          thread: restoredThread,
          storageEpoch: STORAGE_EPOCH,
        }),
      );

      yield* TestClock.adjust("2 seconds");
      yield* Effect.yieldNow;
      const restored = yield* awaitThreadState(
        harness.observed,
        (value) => Option.isSome(value.data) && value.data.value.title === restoredThread.title,
      );

      expect(Option.getOrThrow(restored.data).title).toBe(restoredThread.title);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
      yield* harness.replaceSession;
      yield* Effect.yieldNow;
      expect(yield* Ref.get(harness.lastSubscribeAfterSequence)).toBe(8);
      expect(yield* Ref.get(harness.lastSubscribeVerifiedRevision)).toBe(8);
      expect(yield* Ref.get(harness.lastSubscribeObservedRevision)).toBe(8);
    }),
  );

  it.effect("accepts a forced same-epoch reconnect snapshot below the old cursor", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: BASE_THREAD });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");
      const restoredThread = {
        ...BASE_THREAD,
        title: "Reconnect restored title",
        updatedAt: "2026-07-16T02:30:00.000Z",
      };

      yield* Queue.offer(harness.inputs, {
        kind: "snapshot",
        storageEpoch: STORAGE_EPOCH,
        force: true,
        snapshot: {
          snapshotSequence: 6,
          thread: restoredThread,
          storageEpoch: STORAGE_EPOCH,
          latestSequence: 6,
          latestEventId: markerEventId(6),
        },
      });
      const restored = yield* awaitThreadState(
        harness.observed,
        (value) => Option.isSome(value.data) && value.data.value.title === restoredThread.title,
      );

      expect(Option.getOrThrow(restored.data).title).toBe(restoredThread.title);
      yield* harness.replaceSession;
      yield* Effect.yieldNow;
      expect(yield* Ref.get(harness.lastSubscribeAfterSequence)).toBe(6);
    }),
  );

  it.effect("resets the fast window for a genuine live event but not stale running state", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: makeRunningThread(),
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
        reconciliationPolicy: {
          fastIntervalMs: 2_000,
          fastWindowMs: 4_000,
          backoffMultiplier: 2,
          maxBackoffMs: 8_000,
        },
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");

      yield* TestClock.adjust("2 seconds");
      yield* TestClock.adjust("2 seconds");
      yield* TestClock.adjust("4 seconds");
      yield* Effect.yieldNow;
      const beforeLiveEvent = yield* Ref.get(harness.revisionCallTimes);
      expect(beforeLiveEvent).toEqual([2_000, 4_000, 8_000]);

      yield* Queue.offer(
        harness.inputs,
        activityAppended(
          "A genuine live event reset reconciliation.",
          CACHED_SNAPSHOT_SEQUENCE + 1,
        ),
      );
      const liveEventState = yield* awaitThreadState(
        harness.observed,
        (value) =>
          Option.isSome(value.data) &&
          value.data.value.activities.some(
            (activity) => activity.summary === "A genuine live event reset reconciliation.",
          ),
      );
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE + 1,
          thread: Option.getOrThrow(liveEventState.data),
        }),
      );
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 1));
      yield* TestClock.adjust("2 seconds");
      yield* TestClock.adjust("2 seconds");
      yield* Effect.yieldNow;

      const afterLiveEvent = yield* Ref.get(harness.revisionCallTimes);
      expect(afterLiveEvent.slice(-2)).toEqual([10_000, 12_000]);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
    }),
  );

  it.effect("wakes backed-off reconciliation after a locally initiated turn", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: makeRunningThread(),
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
        reconciliationPolicy: {
          fastIntervalMs: 2_000,
          fastWindowMs: 4_000,
          backoffMultiplier: 2,
          maxBackoffMs: 8_000,
        },
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");
      yield* TestClock.adjust("2 seconds");
      yield* TestClock.adjust("2 seconds");
      yield* TestClock.adjust("4 seconds");
      yield* Effect.yieldNow;

      yield* harness.markLocalTurn;
      yield* Effect.yieldNow;
      yield* TestClock.adjust("2 seconds");
      yield* Effect.yieldNow;

      expect((yield* Ref.get(harness.revisionCallTimes)).at(-1)).toBe(10_000);
    }),
  );

  it.effect("does not let continuous live activity starve the revision deadline", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: makeRunningThread(),
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE + 5,
        projectionSequence: CACHED_SNAPSHOT_SEQUENCE,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");

      for (let index = 1; index <= 5; index += 1) {
        yield* TestClock.adjust("1 second");
        yield* Queue.offer(
          harness.inputs,
          activityAppended(`Continuous activity ${index}`, CACHED_SNAPSHOT_SEQUENCE + index),
        );
        yield* Effect.yieldNow;
      }

      expect(yield* Ref.get(harness.revisionCallTimes)).toEqual([2_000, 4_000]);
    }),
  );

  it.effect("keeps local-turn reconciliation backed off until an unavailable marker recovers", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: BASE_THREAD,
        reconciliationPolicy: {
          fastIntervalMs: 2_000,
          fastWindowMs: 4_000,
          backoffMultiplier: 2,
          maxBackoffMs: 8_000,
        },
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");
      yield* Ref.set(harness.revisionAvailable, false);
      yield* harness.markLocalTurn;
      yield* Effect.yieldNow;

      for (const delay of [2, 2, 4]) {
        yield* TestClock.adjust(`${delay} seconds`);
        yield* Effect.yieldNow;
      }
      expect(yield* Ref.get(harness.revisionCallTimes)).toEqual([2_000, 4_000, 8_000]);

      yield* Ref.set(harness.revisionAvailable, true);
      yield* TestClock.adjust("8 seconds");
      yield* Effect.yieldNow;
      expect(yield* Ref.get(harness.revisionCallTimes)).toEqual([2_000, 4_000, 8_000, 16_000]);

      yield* TestClock.adjust("60 seconds");
      yield* Effect.yieldNow;
      expect(yield* Ref.get(harness.revisionCallTimes)).toEqual([
        2_000, 4_000, 8_000, 16_000, 24_000, 32_000, 40_000, 48_000, 56_000, 64_000, 72_000,
      ]);
    }),
  );

  it.effect("reduces live events and persists the latest thread", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: BASE_THREAD });
      yield* Queue.offer(harness.inputs, snapshot(BASE_THREAD, CACHED_SNAPSHOT_SEQUENCE + 1));
      yield* Queue.offer(harness.inputs, titleUpdated("Live title", CACHED_SNAPSHOT_SEQUENCE + 2));

      const state = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.title === "Live title",
      );
      yield* TestClock.adjust("500 millis");
      yield* Effect.yieldNow;

      expect(Option.getOrThrow(state.data).title).toBe("Live title");
      expect((yield* Ref.get(harness.savedThreads)).at(-1)?.thread.title).toBe("Live title");
      expect((yield* Ref.get(harness.savedThreads)).at(-1)?.snapshotSequence).toBe(
        CACHED_SNAPSHOT_SEQUENCE + 2,
      );
    }),
  );

  it.effect("seeds the thread from the HTTP snapshot and resumes live events", () =>
    Effect.gen(function* () {
      const httpThread: OrchestrationThread = { ...BASE_THREAD, title: "HTTP title" };
      const harness = yield* makeHarness({
        httpSnapshot: Option.some({ snapshotSequence: 1, thread: httpThread }),
      });
      // No socket snapshot is pushed; only a live event arrives over the socket.
      // It can only be applied if the HTTP snapshot already seeded the thread.
      yield* Queue.offer(harness.inputs, titleUpdated("Live title", 2));

      const state = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.title === "Live title",
      );

      expect(Option.getOrThrow(state.data).title).toBe("Live title");
      // Cold cache: the full snapshot was loaded over HTTP and the socket
      // resumed from that snapshot's sequence.
      expect(yield* Ref.get(harness.loaderCalls)).toBeGreaterThanOrEqual(1);
      expect(yield* Ref.get(harness.lastSubscribeAfterSequence)).toBe(1);
    }),
  );

  it.effect("ignores replayed thread events at or below the snapshot sequence", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: BASE_THREAD });
      yield* Queue.offer(harness.inputs, snapshot(BASE_THREAD, CACHED_SNAPSHOT_SEQUENCE + 1));
      yield* Queue.offer(
        harness.inputs,
        titleUpdated("Replayed title", CACHED_SNAPSHOT_SEQUENCE + 1),
      );
      yield* Queue.offer(harness.inputs, titleUpdated("Live title", CACHED_SNAPSHOT_SEQUENCE + 2));

      const state = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.title === "Live title",
      );

      expect(Option.getOrThrow(state.data).title).toBe("Live title");
    }),
  );

  it.effect(
    "ignores duplicate replay without rebuilding or persisting an unchanged full thread",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          cached: BASE_THREAD,
          revisionSequence: CACHED_SNAPSHOT_SEQUENCE,
        });
        yield* awaitThreadState(harness.observed, (value) => value.status === "live");
        yield* TestClock.adjust("500 millis");
        yield* Effect.yieldNow;

        const before = yield* Ref.get(harness.latest);
        const beforeThread = Option.getOrThrow(before.data);
        const savesBeforeReplay = (yield* Ref.get(harness.savedThreads)).length;
        expect(savesBeforeReplay).toBe(0);

        yield* Queue.offer(harness.inputs, snapshot(BASE_THREAD, CACHED_SNAPSHOT_SEQUENCE));
        yield* Queue.offer(
          harness.inputs,
          titleUpdated("Duplicate replay must be ignored", CACHED_SNAPSHOT_SEQUENCE),
        );
        yield* Effect.yieldNow;
        yield* TestClock.adjust("500 millis");
        yield* Effect.yieldNow;

        const after = yield* Ref.get(harness.latest);
        expect(Option.getOrThrow(after.data)).toBe(beforeThread);
        expect((yield* Ref.get(harness.savedThreads)).length).toBe(savesBeforeReplay);
        expect(yield* Ref.get(harness.loaderCalls)).toBe(0);
      }),
  );

  it.effect("removes cached data when the thread is deleted", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: BASE_THREAD });
      yield* Queue.offer(harness.inputs, snapshot(BASE_THREAD, CACHED_SNAPSHOT_SEQUENCE + 1));
      yield* Queue.offer(harness.inputs, deleted(CACHED_SNAPSHOT_SEQUENCE + 2));

      const state = yield* awaitThreadState(
        harness.observed,
        (value) => value.status === "deleted",
      );

      expect(Option.isNone(state.data)).toBe(true);
      expect(yield* Ref.get(harness.removedThreads)).toEqual([THREAD_ID]);
      yield* TestClock.adjust("500 millis");
      yield* Effect.yieldNow;
      expect(yield* Ref.get(harness.savedThreads)).toEqual([]);
    }),
  );

  it.effect("retains reversible active detail data when the thread is archived", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: BASE_THREAD });
      yield* Queue.offer(harness.inputs, snapshot(BASE_THREAD, CACHED_SNAPSHOT_SEQUENCE + 1));
      yield* Queue.offer(harness.inputs, archived(CACHED_SNAPSHOT_SEQUENCE + 2));

      const archivedState = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.archivedAt === "2026-04-01T02:00:00.000Z",
      );

      expect(Option.getOrThrow(archivedState.data).archivedAt).toBe("2026-04-01T02:00:00.000Z");
      expect(yield* Ref.get(harness.removedThreads)).toEqual([]);

      yield* Queue.offer(harness.inputs, unarchived(CACHED_SNAPSHOT_SEQUENCE + 3));
      const unarchivedState = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.archivedAt === null,
      );
      expect(Option.getOrThrow(unarchivedState.data).archivedAt).toBeNull();
    }),
  );

  it.effect("creates a fresh reconnect subscription from the latest applied cursor", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: BASE_THREAD });
      yield* Queue.offer(harness.inputs, snapshot(BASE_THREAD, CACHED_SNAPSHOT_SEQUENCE + 1));
      yield* Queue.offer(harness.inputs, new Error("stream failed"));

      const state = yield* awaitThreadState(harness.observed, (value) =>
        Option.isSome(value.error),
      );

      expect(Option.getOrThrow(state.data)).toEqual(BASE_THREAD);
      expect(Option.getOrThrow(state.error)).toBe("stream failed");
      expect(yield* Ref.get(harness.retryCount)).toBe(0);

      yield* harness.replaceSession;
      for (let attempt = 0; attempt < 100; attempt += 1) {
        if ((yield* Ref.get(harness.subscriptionCount)) >= 2) {
          break;
        }
        yield* Effect.yieldNow;
      }
      yield* Queue.offer(
        harness.inputs,
        snapshot(
          {
            ...BASE_THREAD,
            title: "Recovered thread",
          },
          CACHED_SNAPSHOT_SEQUENCE + 2,
        ),
      );
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.title === "Recovered thread",
      );

      expect(Option.isNone(recovered.error)).toBe(true);
      expect(yield* Ref.get(harness.subscriptionCount)).toBe(2);
      expect(yield* Ref.get(harness.subscribeAfterSequences)).toEqual([
        CACHED_SNAPSHOT_SEQUENCE,
        CACHED_SNAPSHOT_SEQUENCE + 1,
      ]);
    }),
  );

  it.effect("backfills server-persisted assistant messages from a reconnect snapshot", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* Queue.offer(harness.inputs, snapshot(BASE_THREAD));
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.messages.length === 0,
      );

      yield* SubscriptionRef.set(harness.supervisorSession, Option.none());
      yield* harness.replaceSession;
      for (let attempt = 0; attempt < 100; attempt += 1) {
        if ((yield* Ref.get(harness.subscriptionCount)) >= 2) {
          break;
        }
        yield* Effect.yieldNow;
      }

      const missedAssistantMessage = {
        id: MessageId.make("message-assistant-7am"),
        role: "assistant" as const,
        text: "Morning report generated while the client was offline.",
        attachments: [],
        turnId: null,
        streaming: false,
        createdAt: "2026-07-06T06:04:16.000Z",
        updatedAt: "2026-07-06T06:04:16.000Z",
      };
      yield* Queue.offer(
        harness.inputs,
        snapshot(
          {
            ...BASE_THREAD,
            updatedAt: missedAssistantMessage.updatedAt,
            messages: [missedAssistantMessage],
          },
          2,
        ),
      );

      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.messages.some((message) => message.id === missedAssistantMessage.id),
      );

      const messages = Option.getOrThrow(recovered.data).messages;
      expect(messages).toEqual([missedAssistantMessage]);
      expect(yield* Ref.get(harness.subscriptionCount)).toBe(2);
    }),
  );

  it.effect("heals one persisted event omitted from the live path with exactly one full load", () =>
    Effect.gen(function* () {
      const runningThread = makeRunningThread();
      const harness = yield* makeHarness({ cached: runningThread });
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.latestTurn?.state === "running" &&
          value.data.value.messages.length === 0,
      );

      const missedAssistantMessage: OrchestrationMessage = {
        id: MessageId.make("message-live-frame-missed"),
        role: "assistant",
        text: "This assistant update was persisted while the socket frame was missed.",
        attachments: [],
        turnId: RUNNING_TURN_ID,
        streaming: false,
        createdAt: "2026-07-07T21:00:03.000Z",
        updatedAt: "2026-07-07T21:00:03.000Z",
      };
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE + 1,
          thread: makeRunningThread([missedAssistantMessage]),
        }),
      );
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 1));

      yield* advanceActiveReconcileInterval;
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.messages.some((message) => message.id === missedAssistantMessage.id),
      );

      expect(Option.getOrThrow(recovered.data).messages).toEqual([missedAssistantMessage]);
      expect(yield* Ref.get(harness.subscriptionCount)).toBe(1);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);

      for (let index = 0; index < 5; index += 1) {
        yield* advanceActiveReconcileInterval;
      }
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
    }),
  );

  it.effect("heals an omitted event even when a later live event advances the applied cursor", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: makeRunningThread() });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");

      yield* Queue.offer(
        harness.inputs,
        activityAppended(
          "A later event arrived after the missed message.",
          CACHED_SNAPSHOT_SEQUENCE + 2,
        ),
      );
      const afterLaterEvent = yield* awaitThreadState(
        harness.observed,
        (value) =>
          Option.isSome(value.data) &&
          value.data.value.activities.some(
            (activity) => activity.summary === "A later event arrived after the missed message.",
          ),
      );
      const missedAssistantMessage: OrchestrationMessage = {
        id: MessageId.make("message-missed-before-later-live-event"),
        role: "assistant",
        text: "This earlier persisted message was omitted from the live path.",
        attachments: [],
        turnId: RUNNING_TURN_ID,
        streaming: false,
        createdAt: "2026-07-07T21:00:02.000Z",
        updatedAt: "2026-07-07T21:00:02.000Z",
      };
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE + 2,
          thread: {
            ...Option.getOrThrow(afterLaterEvent.data),
            messages: [missedAssistantMessage],
          },
        }),
      );
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 2));

      yield* advanceActiveReconcileInterval;
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          Option.isSome(value.data) &&
          value.data.value.messages.some((message) => message.id === missedAssistantMessage.id),
      );

      expect(Option.getOrThrow(recovered.data).messages).toContainEqual(missedAssistantMessage);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
    }),
  );

  it.effect("keeps an advanced revision pending across a transient detail failure", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: makeRunningThread(),
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE + 1,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");

      yield* advanceActiveReconcileInterval;
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);

      const recoveredMessage: OrchestrationMessage = {
        id: MessageId.make("message-after-transient-detail-failure"),
        role: "assistant",
        text: "Recovered after the first detail request was unavailable.",
        attachments: [],
        turnId: RUNNING_TURN_ID,
        streaming: false,
        createdAt: "2026-07-07T21:00:03.000Z",
        updatedAt: "2026-07-07T21:00:03.000Z",
      };
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE + 1,
          thread: makeRunningThread([recoveredMessage]),
        }),
      );
      yield* advanceActiveReconcileInterval;
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          Option.isSome(value.data) &&
          value.data.value.messages.some((message) => message.id === recoveredMessage.id),
      );
      yield* advanceActiveReconcileInterval;

      expect(Option.getOrThrow(recovered.data).messages).toEqual([recoveredMessage]);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(2);
    }),
  );

  it.effect("retries a pending revision until the detail projection catches up", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: makeRunningThread(),
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE + 2,
        httpSnapshot: Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE + 1,
          thread: makeRunningThread(),
        }),
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");

      yield* advanceActiveReconcileInterval;
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);

      const recoveredMessage: OrchestrationMessage = {
        id: MessageId.make("message-after-projection-catch-up"),
        role: "assistant",
        text: "Recovered after the projection reached the persisted marker.",
        attachments: [],
        turnId: RUNNING_TURN_ID,
        streaming: false,
        createdAt: "2026-07-07T21:00:03.000Z",
        updatedAt: "2026-07-07T21:00:03.000Z",
      };
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE + 2,
          thread: makeRunningThread([recoveredMessage]),
        }),
      );
      yield* advanceActiveReconcileInterval;
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          Option.isSome(value.data) &&
          value.data.value.messages.some((message) => message.id === recoveredMessage.id),
      );
      yield* advanceActiveReconcileInterval;

      expect(Option.getOrThrow(recovered.data).messages).toEqual([recoveredMessage]);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(2);
    }),
  );

  it.effect("applies an equal-sequence projection snapshot when it contains a missed message", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: makeRunningThread() });
      yield* Queue.offer(harness.inputs, titleUpdated("Live title", CACHED_SNAPSHOT_SEQUENCE + 1));
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.title === "Live title",
      );

      const missedAssistantMessage: OrchestrationMessage = {
        id: MessageId.make("message-equal-sequence-missed"),
        role: "assistant",
        text: "This assistant update shares the current projection sequence.",
        attachments: [],
        turnId: RUNNING_TURN_ID,
        streaming: false,
        createdAt: "2026-07-07T21:00:03.000Z",
        updatedAt: "2026-07-07T21:00:03.000Z",
      };
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE + 1,
          thread: { ...makeRunningThread([missedAssistantMessage]), title: "Live title" },
        }),
      );
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 100));

      yield* advanceActiveReconcileInterval;
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.messages.some((message) => message.id === missedAssistantMessage.id),
      );

      expect(Option.getOrThrow(recovered.data).messages).toEqual([missedAssistantMessage]);
    }),
  );

  it.effect("applies an older global-sequence projection snapshot when thread data is fresh", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: makeRunningThread() });
      yield* Queue.offer(harness.inputs, titleUpdated("Live title", CACHED_SNAPSHOT_SEQUENCE + 2));
      const current = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.title === "Live title",
      );

      const missedAssistantMessage: OrchestrationMessage = {
        id: MessageId.make("message-older-global-sequence-missed"),
        role: "assistant",
        text: "This assistant update arrived while an unrelated projector lagged.",
        attachments: [],
        turnId: RUNNING_TURN_ID,
        streaming: false,
        createdAt: "2026-07-07T21:00:03.000Z",
        updatedAt: "2026-07-07T21:00:03.000Z",
      };
      const currentThread = Option.getOrThrow(current.data);
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE + 1,
          thread: {
            ...currentThread,
            messages: [missedAssistantMessage],
          },
        }),
      );
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 100));

      yield* advanceActiveReconcileInterval;
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.messages.some((message) => message.id === missedAssistantMessage.id),
      );

      expect(Option.getOrThrow(recovered.data).messages).toEqual([missedAssistantMessage]);
      yield* Queue.offer(
        harness.inputs,
        titleUpdated("Replayed after reconcile", CACHED_SNAPSHOT_SEQUENCE + 2),
      );
      yield* Effect.yieldNow;
      expect(Option.getOrThrow((yield* Ref.get(harness.latest)).data).title).toBe("Live title");
    }),
  );

  it.effect("merges older projection snapshots without dropping newer live collections", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: makeRunningThread() });
      yield* Queue.offer(
        harness.inputs,
        activityAppended(
          "This live activity is already past the snapshot cursor.",
          CACHED_SNAPSHOT_SEQUENCE + 2,
        ),
      );
      const current = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.activities.some(
            (activity) =>
              activity.summary === "This live activity is already past the snapshot cursor.",
          ),
      );

      const missedAssistantMessage: OrchestrationMessage = {
        id: MessageId.make("message-older-snapshot-preserves-live-activity"),
        role: "assistant",
        text: "This message arrived through projection while another projector lagged.",
        attachments: [],
        turnId: RUNNING_TURN_ID,
        streaming: false,
        createdAt: "2026-07-07T21:00:03.000Z",
        updatedAt: "2026-07-07T21:00:03.000Z",
      };
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE + 1,
          thread: {
            ...Option.getOrThrow(current.data),
            updatedAt: "2026-07-07T21:00:03.000Z",
            messages: [missedAssistantMessage],
            activities: [],
          },
        }),
      );
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 100));

      yield* advanceActiveReconcileInterval;
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.messages.some((message) => message.id === missedAssistantMessage.id) &&
          value.data.value.activities.some(
            (activity) =>
              activity.summary === "This live activity is already past the snapshot cursor.",
          ),
      );

      const thread = Option.getOrThrow(recovered.data);
      expect(thread.messages).toEqual([missedAssistantMessage]);
      expect(thread.activities.map((activity) => activity.summary)).toEqual([
        "This live activity is already past the snapshot cursor.",
      ]);
      yield* Queue.offer(
        harness.inputs,
        activityAppended("Replayed activity should remain deduped.", CACHED_SNAPSHOT_SEQUENCE + 2),
      );
      yield* Effect.yieldNow;
      expect(Option.getOrThrow((yield* Ref.get(harness.latest)).data).activities).toHaveLength(1);
    }),
  );

  it.effect(
    "preserves current scalar fields when an older projection snapshot has the same timestamp",
    () =>
      Effect.gen(function* () {
        const archivedAt = "2026-07-07T21:00:02.000Z";
        const currentThread: OrchestrationThread = {
          ...makeRunningThread(),
          updatedAt: archivedAt,
          archivedAt,
        };
        const harness = yield* makeHarness({ cached: currentThread });
        yield* awaitThreadState(
          harness.observed,
          (value) =>
            value.status === "live" &&
            Option.isSome(value.data) &&
            value.data.value.archivedAt === archivedAt,
        );

        const missedAssistantMessage: OrchestrationMessage = {
          id: MessageId.make("message-equal-timestamp-scalar"),
          role: "assistant",
          text: "Projection recovered a missed row without rolling back archive state.",
          attachments: [],
          turnId: RUNNING_TURN_ID,
          streaming: false,
          createdAt: archivedAt,
          updatedAt: archivedAt,
        };
        yield* Ref.set(
          harness.httpSnapshot,
          Option.some({
            snapshotSequence: CACHED_SNAPSHOT_SEQUENCE - 1,
            thread: {
              ...currentThread,
              archivedAt: null,
              messages: [missedAssistantMessage],
            },
          }),
        );
        yield* Ref.set(
          harness.explicitRevisionSequence,
          Option.some(CACHED_SNAPSHOT_SEQUENCE + 100),
        );

        yield* advanceActiveReconcileInterval;
        const recovered = yield* awaitThreadState(
          harness.observed,
          (value) =>
            value.status === "live" &&
            Option.isSome(value.data) &&
            value.data.value.messages.some((message) => message.id === missedAssistantMessage.id),
        );

        expect(Option.getOrThrow(recovered.data).archivedAt).toBe(archivedAt);
      }),
  );

  it.effect("does not prune collections solely from a lower-checkpoint older snapshot", () =>
    Effect.gen(function* () {
      const retainedTurnId = TurnId.make("turn-retained");
      const prunedTurnId = TurnId.make("turn-pruned");
      const retainedMessage: OrchestrationMessage = {
        id: MessageId.make("message-retained-after-revert"),
        role: "assistant",
        text: "Retained turn output.",
        attachments: [],
        turnId: retainedTurnId,
        streaming: false,
        createdAt: "2026-07-07T21:00:01.000Z",
        updatedAt: "2026-07-07T21:00:01.000Z",
      };
      const prunedMessage: OrchestrationMessage = {
        id: MessageId.make("message-pruned-after-revert"),
        role: "assistant",
        text: "This turn was reverted.",
        attachments: [],
        turnId: prunedTurnId,
        streaming: false,
        createdAt: "2026-07-07T21:00:02.000Z",
        updatedAt: "2026-07-07T21:00:02.000Z",
      };
      const retainedCheckpoint = {
        turnId: retainedTurnId,
        checkpointTurnCount: 1,
        checkpointRef: CheckpointRef.make("refs/t3/checkpoints/thread-1/turn/1"),
        status: "ready" as const,
        files: [],
        assistantMessageId: retainedMessage.id,
        completedAt: "2026-07-07T21:00:01.500Z",
      };
      const prunedCheckpoint = {
        turnId: prunedTurnId,
        checkpointTurnCount: 2,
        checkpointRef: CheckpointRef.make("refs/t3/checkpoints/thread-1/turn/2"),
        status: "ready" as const,
        files: [],
        assistantMessageId: prunedMessage.id,
        completedAt: "2026-07-07T21:00:02.500Z",
      };
      const currentThread: OrchestrationThread = {
        ...makeRunningThread([retainedMessage, prunedMessage]),
        updatedAt: "2026-07-07T21:00:02.500Z",
        proposedPlans: [
          {
            id: "plan-pruned-after-revert",
            turnId: prunedTurnId,
            planMarkdown: "Remove this reverted plan.",
            implementedAt: null,
            implementationThreadId: null,
            createdAt: "2026-07-07T21:00:02.000Z",
            updatedAt: "2026-07-07T21:00:02.000Z",
          },
        ],
        activities: [
          {
            id: EventId.make("activity-pruned-after-revert"),
            tone: "info",
            kind: "test.activity",
            summary: "This reverted activity should disappear.",
            payload: {},
            turnId: prunedTurnId,
            sequence: CACHED_SNAPSHOT_SEQUENCE + 2,
            createdAt: "2026-07-07T21:00:02.000Z",
          },
        ],
        checkpoints: [retainedCheckpoint, prunedCheckpoint],
      };
      const harness = yield* makeHarness({ cached: currentThread });
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.checkpoints.length === 2,
      );

      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE - 1,
          thread: {
            ...currentThread,
            updatedAt: "2026-07-07T21:00:03.000Z",
            messages: [retainedMessage],
            proposedPlans: [],
            activities: [],
            checkpoints: [retainedCheckpoint],
          },
        }),
      );
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 100));

      yield* advanceActiveReconcileInterval;
      yield* Effect.yieldNow;

      const thread = Option.getOrThrow((yield* Ref.get(harness.latest)).data);
      expect(thread.messages.map((message) => message.id)).toEqual([
        retainedMessage.id,
        prunedMessage.id,
      ]);
      expect(thread.proposedPlans).toHaveLength(1);
      expect(thread.activities).toHaveLength(1);
      expect(thread.checkpoints.map((checkpoint) => checkpoint.turnId)).toEqual([
        retainedTurnId,
        prunedTurnId,
      ]);
    }),
  );

  it.effect(
    "does not resurrect revert-pruned messages from a pre-revert non-advancing snapshot",
    () =>
      Effect.gen(function* () {
        const retainedTurnId = TurnId.make("turn-kept");
        const revertedTurnId = TurnId.make("turn-reverted");
        const keptMessage: OrchestrationMessage = {
          id: MessageId.make("message-kept-through-revert"),
          role: "assistant",
          text: "Retained turn output.",
          attachments: [],
          turnId: retainedTurnId,
          streaming: false,
          createdAt: "2026-07-07T21:00:01.000Z",
          updatedAt: "2026-07-07T21:00:01.000Z",
        };
        const revertPrunedMessage: OrchestrationMessage = {
          id: MessageId.make("message-pruned-by-revert"),
          role: "assistant",
          text: "This turn was reverted.",
          attachments: [],
          turnId: revertedTurnId,
          streaming: false,
          createdAt: "2026-07-07T21:00:02.000Z",
          updatedAt: "2026-07-07T21:00:02.000Z",
        };
        const keptCheckpoint = {
          turnId: retainedTurnId,
          checkpointTurnCount: 1,
          checkpointRef: CheckpointRef.make("refs/t3/checkpoints/thread-1/turn/1"),
          status: "ready" as const,
          files: [],
          assistantMessageId: keptMessage.id,
          completedAt: "2026-07-07T21:00:01.500Z",
        };
        const revertedCheckpoint = {
          turnId: revertedTurnId,
          checkpointTurnCount: 2,
          checkpointRef: CheckpointRef.make("refs/t3/checkpoints/thread-1/turn/2"),
          status: "ready" as const,
          files: [],
          assistantMessageId: revertPrunedMessage.id,
          completedAt: "2026-07-07T21:00:02.500Z",
        };
        const currentThread: OrchestrationThread = {
          ...makeRunningThread([keptMessage, revertPrunedMessage]),
          updatedAt: "2026-07-07T21:00:02.500Z",
          checkpoints: [keptCheckpoint, revertedCheckpoint],
        };
        const harness = yield* makeHarness({ cached: currentThread });
        yield* awaitThreadState(
          harness.observed,
          (value) =>
            value.status === "live" &&
            Option.isSome(value.data) &&
            value.data.value.checkpoints.length === 2,
        );

        yield* Queue.offer(harness.inputs, {
          kind: "event",
          storageEpoch: STORAGE_EPOCH,
          event: {
            eventId: EventId.make("event-reverted"),
            sequence: CACHED_SNAPSHOT_SEQUENCE + 2,
            occurredAt: "2026-07-07T21:00:03.000Z",
            commandId: null,
            causationEventId: null,
            correlationId: null,
            metadata: {},
            aggregateKind: "thread",
            aggregateId: THREAD_ID,
            type: "thread.reverted",
            payload: {
              threadId: THREAD_ID,
              turnCount: 1,
            },
          },
        });
        yield* awaitThreadState(
          harness.observed,
          (value) =>
            value.status === "live" &&
            Option.isSome(value.data) &&
            value.data.value.messages.length === 1,
        );

        // A reconcile snapshot taken BEFORE the revert still contains the pruned
        // message; the non-advancing additive merge must not resurrect it.
        const loaderCallsBeforeReconcile = yield* Ref.get(harness.loaderCalls);
        yield* Ref.set(
          harness.httpSnapshot,
          Option.some({
            snapshotSequence: CACHED_SNAPSHOT_SEQUENCE + 1,
            thread: {
              ...currentThread,
              updatedAt: "2026-07-07T21:00:05.000Z",
            },
          }),
        );
        yield* Ref.set(
          harness.explicitRevisionSequence,
          Option.some(CACHED_SNAPSHOT_SEQUENCE + 100),
        );

        // Drive several reconcile intervals so at least one load observes the
        // pre-revert snapshot, and watch whether the pruned message ever
        // (incorrectly) reappears.
        let maxMessages = 0;
        for (let rounds = 0; rounds < 5; rounds += 1) {
          yield* advanceActiveReconcileInterval;
          for (let ticks = 0; ticks < 50; ticks += 1) {
            const observedNow = Option.getOrThrow((yield* Ref.get(harness.latest)).data);
            maxMessages = Math.max(maxMessages, observedNow.messages.length);
            yield* Effect.yieldNow;
          }
        }
        expect(yield* Ref.get(harness.loaderCalls)).toBeGreaterThan(loaderCallsBeforeReconcile);
        expect(maxMessages).toBe(1);

        const thread = Option.getOrThrow((yield* Ref.get(harness.latest)).data);
        expect(thread.messages.map((message) => message.id)).toEqual([keptMessage.id]);
      }),
  );

  it.effect("keeps a recent turn completion eligible for projection reconciliation", () =>
    Effect.gen(function* () {
      const runningThread = makeRunningThread();
      const harness = yield* makeHarness({ cached: runningThread });
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.latestTurn?.state === "running",
      );
      yield* Queue.offer(harness.inputs, sessionReady(CACHED_SNAPSHOT_SEQUENCE + 1));
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.latestTurn?.state === "completed" &&
          value.data.value.messages.length === 0,
      );

      const missedAssistantMessage: OrchestrationMessage = {
        id: MessageId.make("message-completed-turn-missed"),
        role: "assistant",
        text: "This assistant update was missed before the turn completed.",
        attachments: [],
        turnId: RUNNING_TURN_ID,
        streaming: false,
        createdAt: "2026-07-07T21:00:03.000Z",
        updatedAt: "2026-07-07T21:00:03.000Z",
      };
      const completedThread = Option.getOrThrow((yield* Ref.get(harness.latest)).data);
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE + 1,
          thread: {
            ...completedThread,
            messages: [missedAssistantMessage],
            latestTurn: completedThread.latestTurn
              ? {
                  ...completedThread.latestTurn,
                  assistantMessageId: missedAssistantMessage.id,
                }
              : completedThread.latestTurn,
          },
        }),
      );
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 100));

      yield* advanceActiveReconcileInterval;
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.messages.some((message) => message.id === missedAssistantMessage.id),
      );

      expect(Option.getOrThrow(recovered.data).messages).toEqual([missedAssistantMessage]);
    }),
  );

  it.effect("does not regress turn state from a non-advancing reconcile snapshot", () =>
    Effect.gen(function* () {
      const runningThread = makeRunningThread();
      const runningTurn = runningThread.latestTurn;
      if (runningTurn === null) {
        throw new Error("test fixture must have a running latestTurn");
      }
      const harness = yield* makeHarness({
        cached: {
          ...runningThread,
          turns: [runningTurn],
        },
      });
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.turns.at(0)?.state === "running",
      );
      yield* Queue.offer(harness.inputs, sessionReady(CACHED_SNAPSHOT_SEQUENCE + 1));
      const completed = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.latestTurn?.state === "completed" &&
          value.data.value.turns.at(0)?.state === "completed",
      );

      const completedThread = Option.getOrThrow(completed.data);
      const staleRunningTurn = {
        ...runningTurn,
        assistantMessageId: MessageId.make("assistant-stale-boundary"),
      };
      const loaderCallsBeforeReconcile = yield* Ref.get(harness.loaderCalls);
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE,
          thread: {
            ...completedThread,
            latestTurn: staleRunningTurn,
            turns: [staleRunningTurn],
          },
        }),
      );
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 100));

      yield* advanceActiveReconcileInterval;
      for (let ticks = 0; ticks < 50; ticks += 1) {
        if ((yield* Ref.get(harness.loaderCalls)) > loaderCallsBeforeReconcile) {
          break;
        }
        yield* Effect.yieldNow;
      }

      expect(yield* Ref.get(harness.loaderCalls)).toBeGreaterThan(loaderCallsBeforeReconcile);
      const thread = Option.getOrThrow((yield* Ref.get(harness.latest)).data);
      expect(thread.turns).toEqual(completedThread.turns);
      expect(thread.latestTurn).toEqual(completedThread.latestTurn);
    }),
  );

  it.effect("merges recovered turn assistant boundaries from non-advancing snapshots", () =>
    Effect.gen(function* () {
      const runningThread = makeRunningThread();
      const runningTurn = runningThread.latestTurn;
      if (runningTurn === null) {
        throw new Error("test fixture must have a running latestTurn");
      }
      const harness = yield* makeHarness({
        cached: {
          ...runningThread,
          turns: [runningTurn],
        },
      });
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.turns.at(0)?.state === "running",
      );
      yield* Queue.offer(harness.inputs, sessionReady(CACHED_SNAPSHOT_SEQUENCE + 1));
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.latestTurn?.state === "completed" &&
          value.data.value.turns.at(0)?.assistantMessageId === null,
      );

      const recoveredAssistantMessage: OrchestrationMessage = {
        id: MessageId.make("message-recovered-turn-boundary"),
        role: "assistant",
        text: "Recovered final response.",
        attachments: [],
        turnId: RUNNING_TURN_ID,
        streaming: false,
        createdAt: "2026-07-07T21:00:03.000Z",
        updatedAt: "2026-07-07T21:00:03.000Z",
      };
      const completedThread = Option.getOrThrow((yield* Ref.get(harness.latest)).data);
      const completedTurn = completedThread.turns.at(0);
      if (completedTurn === undefined) {
        throw new Error("completed fixture must keep a turn row");
      }
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE,
          thread: {
            ...completedThread,
            messages: [recoveredAssistantMessage],
            turns: [
              {
                ...completedTurn,
                assistantMessageId: recoveredAssistantMessage.id,
              },
            ],
          },
        }),
      );
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 100));

      yield* advanceActiveReconcileInterval;
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.messages.some((message) => message.id === recoveredAssistantMessage.id),
      );

      const thread = Option.getOrThrow(recovered.data);
      expect(thread.turns.at(0)).toMatchObject({
        turnId: RUNNING_TURN_ID,
        state: "completed",
        assistantMessageId: recoveredAssistantMessage.id,
      });
      expect(thread.latestTurn).toEqual(completedThread.latestTurn);
    }),
  );

  it.effect("replaces stale interim turn boundaries from non-advancing snapshots", () =>
    Effect.gen(function* () {
      const interimAssistantMessage: OrchestrationMessage = {
        id: MessageId.make("message-interim-boundary"),
        role: "assistant",
        text: "I will inspect first.",
        attachments: [],
        turnId: RUNNING_TURN_ID,
        streaming: false,
        createdAt: "2026-07-07T21:00:02.000Z",
        updatedAt: "2026-07-07T21:00:02.000Z",
      };
      const runningThread = makeRunningThread([interimAssistantMessage]);
      const runningTurn = runningThread.latestTurn;
      if (runningTurn === null) {
        throw new Error("test fixture must have a running latestTurn");
      }
      const harness = yield* makeHarness({
        cached: {
          ...runningThread,
          latestTurn: {
            ...runningTurn,
            assistantMessageId: interimAssistantMessage.id,
          },
          turns: [
            {
              ...runningTurn,
              assistantMessageId: interimAssistantMessage.id,
            },
          ],
        },
      });
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.turns.at(0)?.assistantMessageId === interimAssistantMessage.id,
      );
      yield* Queue.offer(harness.inputs, sessionReady(CACHED_SNAPSHOT_SEQUENCE + 1));
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.latestTurn?.state === "completed",
      );

      const finalAssistantMessage: OrchestrationMessage = {
        id: MessageId.make("message-recovered-final-boundary"),
        role: "assistant",
        text: "Final response.",
        attachments: [],
        turnId: RUNNING_TURN_ID,
        streaming: false,
        createdAt: "2026-07-07T21:00:03.000Z",
        updatedAt: "2026-07-07T21:00:03.000Z",
      };
      const completedThread = Option.getOrThrow((yield* Ref.get(harness.latest)).data);
      const completedTurn = completedThread.turns.at(0);
      if (completedTurn === undefined) {
        throw new Error("completed fixture must keep a turn row");
      }
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE,
          thread: {
            ...completedThread,
            messages: [interimAssistantMessage, finalAssistantMessage],
            turns: [
              {
                ...completedTurn,
                assistantMessageId: finalAssistantMessage.id,
              },
            ],
          },
        }),
      );
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 100));

      yield* advanceActiveReconcileInterval;
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.messages.some((message) => message.id === finalAssistantMessage.id),
      );

      const thread = Option.getOrThrow(recovered.data);
      expect(thread.turns.at(0)).toMatchObject({
        turnId: RUNNING_TURN_ID,
        state: "completed",
        assistantMessageId: finalAssistantMessage.id,
      });
    }),
  );

  it.effect("uses snapshot completion time when settling placeholder running turns", () =>
    Effect.gen(function* () {
      const runningThread = makeRunningThread();
      const runningTurn = runningThread.latestTurn;
      if (runningTurn === null) {
        throw new Error("test fixture must have a running latestTurn");
      }
      const placeholderTurn = {
        ...runningTurn,
        completedAt: "2026-07-07T21:00:02.000Z",
      };
      const harness = yield* makeHarness({
        cached: {
          ...runningThread,
          turns: [placeholderTurn],
        },
      });
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.turns.at(0)?.completedAt === placeholderTurn.completedAt,
      );

      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE,
          thread: {
            ...runningThread,
            updatedAt: runningThread.updatedAt,
            latestTurn: {
              ...placeholderTurn,
              state: "completed",
              completedAt: "2026-07-07T21:00:04.000Z",
            },
            turns: [
              {
                ...placeholderTurn,
                state: "completed",
                completedAt: "2026-07-07T21:00:04.000Z",
              },
            ],
          },
        }),
      );
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 100));

      yield* advanceActiveReconcileInterval;
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.latestTurn?.state === "completed" &&
          value.data.value.turns.at(0)?.state === "completed",
      );

      const thread = Option.getOrThrow(recovered.data);
      expect(thread.latestTurn?.completedAt).toBe("2026-07-07T21:00:04.000Z");
      expect(thread.turns.at(0)?.completedAt).toBe("2026-07-07T21:00:04.000Z");
    }),
  );

  it.effect("clears interim assistant boundaries from authoritative null snapshots", () =>
    Effect.gen(function* () {
      const interimAssistantMessage: OrchestrationMessage = {
        id: MessageId.make("assistant-interim"),
        role: "assistant",
        text: "I will inspect first.",
        turnId: RUNNING_TURN_ID,
        streaming: false,
        createdAt: "2026-07-07T21:00:02.000Z",
        updatedAt: "2026-07-07T21:00:02.000Z",
      };
      const runningThread = makeRunningThread([interimAssistantMessage]);
      const runningTurn = runningThread.latestTurn;
      if (runningTurn === null) {
        throw new Error("test fixture must have a running latestTurn");
      }
      const interimTurn = {
        ...runningTurn,
        assistantMessageId: interimAssistantMessage.id,
      };
      const harness = yield* makeHarness({
        cached: {
          ...runningThread,
          latestTurn: interimTurn,
          turns: [interimTurn],
        },
      });

      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE,
          thread: {
            ...runningThread,
            latestTurn: {
              ...interimTurn,
              state: "completed",
              completedAt: "2026-07-07T21:00:04.000Z",
              assistantMessageId: null,
            },
            turns: [
              {
                ...interimTurn,
                state: "completed",
                completedAt: "2026-07-07T21:00:04.000Z",
                assistantMessageId: null,
              },
            ],
          },
        }),
      );
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 100));

      yield* advanceActiveReconcileInterval;
      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.latestTurn?.state === "completed",
      );

      const thread = Option.getOrThrow(recovered.data);
      expect(thread.latestTurn?.assistantMessageId).toBeNull();
      expect(thread.turns.at(0)?.assistantMessageId).toBeNull();
    }),
  );

  it.effect("does not poll parked waiting sessions without an active turn", () =>
    Effect.gen(function* () {
      const parkedWaitingThread: OrchestrationThread = {
        ...BASE_THREAD,
        latestTurn: {
          turnId: RUNNING_TURN_ID,
          state: "completed",
          requestedAt: "2026-07-07T21:00:00.000Z",
          startedAt: "2026-07-07T21:00:01.000Z",
          completedAt: "2026-07-07T21:00:04.000Z",
          assistantMessageId: null,
        },
        session: {
          threadId: THREAD_ID,
          status: "waiting",
          providerName: "claudeAgent",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-07-07T21:00:04.000Z",
        },
      };
      const harness = yield* makeHarness({ cached: parkedWaitingThread });
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.session?.status === "waiting",
      );

      yield* advanceActiveReconcileInterval;

      expect(yield* Ref.get(harness.loaderCalls)).toBe(0);
    }),
  );

  it.effect("keeps a missing detail pending until the projection reaches the revision", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({
        cached: makeRunningThread(),
        revisionSequence: CACHED_SNAPSHOT_SEQUENCE + 1,
        projectionSequence: CACHED_SNAPSHOT_SEQUENCE,
      });
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");
      yield* Ref.set(harness.httpReconcileResult, Option.some({ kind: "missing" }));

      yield* advanceActiveReconcileInterval;

      const beforeCatchUp = yield* Ref.get(harness.latest);
      expect(beforeCatchUp.status).toBe("live");
      expect(Option.isSome(beforeCatchUp.data)).toBe(true);
      expect(yield* Ref.get(harness.removedThreads)).toEqual([]);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);

      yield* Ref.set(harness.explicitProjectionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 1));
      yield* advanceActiveReconcileInterval;
      const deletedState = yield* awaitThreadState(
        harness.observed,
        (value) => value.status === "deleted",
      );

      expect(Option.isNone(deletedState.data)).toBe(true);
      expect(yield* Ref.get(harness.removedThreads)).toEqual([THREAD_ID]);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(2);
    }),
  );

  it.effect("treats a missing active-turn reconcile snapshot as a deleted thread", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: makeRunningThread() });
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.latestTurn?.state === "running",
      );
      yield* Ref.set(harness.httpReconcileResult, Option.some({ kind: "missing" }));
      yield* Ref.set(harness.explicitRevisionSequence, Option.some(CACHED_SNAPSHOT_SEQUENCE + 100));

      yield* advanceActiveReconcileInterval;
      const deletedState = yield* awaitThreadState(
        harness.observed,
        (value) => value.status === "deleted",
      );

      expect(Option.isNone(deletedState.data)).toBe(true);
      expect(yield* Ref.get(harness.removedThreads)).toEqual([THREAD_ID]);
    }),
  );

  it.effect("ignores stale snapshots after a newer live event advances the cursor", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: BASE_THREAD });
      yield* Queue.offer(
        harness.inputs,
        snapshot({ ...BASE_THREAD, title: "Snapshot title" }, CACHED_SNAPSHOT_SEQUENCE + 1),
      );
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.title === "Snapshot title",
      );

      yield* Queue.offer(
        harness.inputs,
        titleUpdated("Newer live title", CACHED_SNAPSHOT_SEQUENCE + 2),
      );
      yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.title === "Newer live title",
      );

      yield* Queue.offer(
        harness.inputs,
        snapshot({ ...BASE_THREAD, title: "Stale snapshot title" }, CACHED_SNAPSHOT_SEQUENCE + 1),
      );
      yield* Effect.yieldNow;

      const latest = yield* Ref.get(harness.latest);
      expect(Option.getOrThrow(latest.data).title).toBe("Newer live title");
    }),
  );

  it.effect("recovers from a transient domain failure without replacing the session", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* Queue.offer(harness.inputs, new Error("thread not found yet"));

      const failed = yield* awaitThreadState(harness.observed, (value) =>
        Option.isSome(value.error),
      );
      expect(Option.getOrThrow(failed.error)).toBe("thread not found yet");
      expect(yield* Ref.get(harness.subscriptionCount)).toBe(1);

      yield* TestClock.adjust("250 millis");
      for (let attempt = 0; attempt < 100; attempt += 1) {
        if ((yield* Ref.get(harness.subscriptionCount)) >= 2) {
          break;
        }
        yield* Effect.yieldNow;
      }
      yield* Queue.offer(
        harness.inputs,
        snapshot({
          ...BASE_THREAD,
          title: "Materialized thread",
        }),
      );

      const recovered = yield* awaitThreadState(
        harness.observed,
        (value) =>
          value.status === "live" &&
          Option.isSome(value.data) &&
          value.data.value.title === "Materialized thread",
      );

      expect(Option.isNone(recovered.error)).toBe(true);
      expect(yield* Ref.get(harness.subscriptionCount)).toBe(2);
      expect(yield* Ref.get(harness.retryCount)).toBe(0);
    }),
  );

  it.effect("does not overwrite a live snapshot when the supervisor becomes ready", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: BASE_THREAD });
      yield* SubscriptionRef.set(harness.supervisorState, {
        desired: true,
        network: "online",
        phase: "connecting",
        stage: "synchronizing",
        attempt: 1,
        generation: 0,
        lastFailure: null,
        retryAt: null,
      });
      yield* Queue.offer(harness.inputs, snapshot(BASE_THREAD));
      yield* awaitThreadState(harness.observed, (value) => value.status === "live");

      yield* SubscriptionRef.set(harness.supervisorState, {
        desired: true,
        network: "online",
        phase: "connected",
        stage: null,
        attempt: 1,
        generation: 1,
        lastFailure: null,
        retryAt: null,
      });
      for (let index = 0; index < 10; index += 1) {
        yield* Effect.yieldNow;
      }

      expect((yield* Ref.get(harness.latest)).status).toBe("live");
    }),
  );
});
