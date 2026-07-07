import {
  EnvironmentId,
  EventId,
  MessageId,
  ORCHESTRATION_WS_METHODS,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type OrchestrationMessage,
  type OrchestrationThread,
  type OrchestrationThreadDetailSnapshot,
  type OrchestrationThreadStreamItem,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
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
  makeEnvironmentThreadState,
  ThreadSnapshotLoader,
  type EnvironmentThreadState,
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
}) {
  const inputs = yield* Queue.unbounded<TestThreadInput>();
  const observed = yield* Queue.unbounded<EnvironmentThreadState>();
  const latest = yield* Ref.make<EnvironmentThreadState>(EMPTY_ENVIRONMENT_THREAD_STATE);
  const retryCount = yield* Ref.make(0);
  const subscriptionCount = yield* Ref.make(0);
  const loaderCalls = yield* Ref.make(0);
  const httpSnapshot = yield* Ref.make(
    options?.httpSnapshot ?? Option.none<OrchestrationThreadDetailSnapshot>(),
  );
  const httpReconcileResult = yield* Ref.make<Option.Option<ThreadSnapshotLoadResult>>(
    Option.none(),
  );
  const lastSubscribeAfterSequence = yield* Ref.make<number | undefined>(undefined);
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
    [ORCHESTRATION_WS_METHODS.subscribeThread]: (input: { readonly afterSequence?: number }) =>
      Stream.unwrap(
        Ref.updateAndGet(subscriptionCount, (count) => count + 1).pipe(
          Effect.andThen(Ref.set(lastSubscribeAfterSequence, input.afterSequence)),
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
            ? Ref.get(httpSnapshot)
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
              return explicit.value;
            }
            const snapshot = yield* Ref.get(httpSnapshot);
            return Option.match(snapshot, {
              onNone: () => ({ kind: "unavailable" }) as const,
              onSome: (value) => ({ kind: "found", snapshot: value }) as const,
            });
          }),
        ),
      ),
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
            })
          : Option.none(),
      ),
    saveThread: (_environmentId, thread) =>
      Ref.update(savedThreads, (current) => [...current, thread]),
    removeThread: (_environmentId, threadId) =>
      Ref.update(removedThreads, (current) => [...current, threadId]),
    clear: () => Effect.void,
  });
  const threadState = yield* makeEnvironmentThreadState(THREAD_ID).pipe(
    Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
    Effect.provideService(Persistence.EnvironmentCacheStore, cache),
    Effect.provideService(ThreadSnapshotLoader, snapshotLoader),
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
    httpSnapshot,
    httpReconcileResult,
    lastSubscribeAfterSequence,
    supervisorState,
    supervisorSession,
    savedThreads,
    removedThreads,
    replaceSession: SubscriptionRef.set(supervisorSession, Option.some(testSession(client))),
  };
});

const snapshot = (
  thread: OrchestrationThread,
  snapshotSequence = 1,
): OrchestrationThreadStreamItem => ({
  kind: "snapshot",
  snapshot: {
    snapshotSequence,
    thread,
  },
});

const titleUpdated = (title: string, sequence = 2): OrchestrationThreadStreamItem => ({
  kind: "event",
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

const deleted = (sequence = 3): OrchestrationThreadStreamItem => ({
  kind: "event",
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

const archived = (sequence = 3): OrchestrationThreadStreamItem => ({
  kind: "event",
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

const unarchived = (sequence = 4): OrchestrationThreadStreamItem => ({
  kind: "event",
  event: {
    eventId: EventId.make("event-unarchived"),
    sequence,
    occurredAt: "2026-04-01T03:00:00.000Z",
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    aggregateKind: "thread",
    aggregateId: THREAD_ID,
    type: "thread.unarchived",
    payload: {
      threadId: THREAD_ID,
      updatedAt: "2026-04-01T03:00:00.000Z",
    },
  },
});

const sessionReady = (sequence = 3): OrchestrationThreadStreamItem => ({
  kind: "event",
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
      expect(yield* Ref.get(harness.loaderCalls)).toBe(0);
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

  it.effect("removes active detail data when the thread is archived", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: BASE_THREAD });
      yield* Queue.offer(harness.inputs, snapshot(BASE_THREAD, CACHED_SNAPSHOT_SEQUENCE + 1));
      yield* Queue.offer(harness.inputs, archived(CACHED_SNAPSHOT_SEQUENCE + 2));

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

  it.effect("reloads the detail when an archived thread is unarchived", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ cached: BASE_THREAD });
      yield* Queue.offer(harness.inputs, snapshot(BASE_THREAD, CACHED_SNAPSHOT_SEQUENCE + 1));
      yield* Queue.offer(harness.inputs, archived(CACHED_SNAPSHOT_SEQUENCE + 2));

      yield* awaitThreadState(harness.observed, (value) => value.status === "deleted");

      // The server-side projection now holds the unarchived thread at a later
      // sequence; the unarchive event alone carries no thread data, so the
      // detail must be reloaded through the reconcile path.
      yield* Ref.set(
        harness.httpSnapshot,
        Option.some({
          snapshotSequence: CACHED_SNAPSHOT_SEQUENCE + 3,
          thread: { ...BASE_THREAD, updatedAt: "2026-04-01T03:00:00.000Z" },
        }),
      );
      yield* Queue.offer(harness.inputs, unarchived(CACHED_SNAPSHOT_SEQUENCE + 3));

      const restored = yield* awaitThreadState(
        harness.observed,
        (value) => value.status === "live" && Option.isSome(value.data),
      );
      expect(Option.isSome(restored.data)).toBe(true);
      if (Option.isSome(restored.data)) {
        expect(restored.data.value.archivedAt).toBeNull();
      }
    }),
  );

  it.effect("preserves data after a domain failure and resumes on a replacement session", () =>
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

  it.effect("reconciles a running turn from projection when a live message frame is missed", () =>
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
      expect(yield* Ref.get(harness.loaderCalls)).toBeGreaterThanOrEqual(1);
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
