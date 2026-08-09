// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import type {
  ProviderApprovalDecision,
  ProviderRuntimeEvent,
  ProviderSendTurnInput,
  ProviderSession,
  ProviderTurnStartResult,
} from "@t3tools/contracts";
import {
  ApprovalRequestId,
  EnvironmentId,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionStartInput,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { it, assert, vi } from "@effect/vitest";

import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderSessionDirectoryPersistenceError,
  ProviderUnsupportedError,
  ProviderValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../Services/ProviderAdapterRegistry.ts";
import * as ProviderService from "../Services/ProviderService.ts";
import * as ProviderSessionDirectory from "../Services/ProviderSessionDirectory.ts";
import { getCapturedProviderRuntimeEventBinding } from "../runtimeEventBindingRegistry.ts";
import { makeProviderServiceLive, type ProviderServiceLiveOptions } from "./ProviderService.ts";
import * as ProviderEventLoggers from "./ProviderEventLoggers.ts";
import { PROVIDER_EMPTY_RESPONSE_ERROR } from "./providerFailureMessages.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import {
  makeSqlitePersistenceLive,
  SqlitePersistenceMemory,
} from "../../persistence/Layers/Sqlite.ts";
import * as ServerConfig from "../../config.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as AnalyticsService from "../../telemetry/AnalyticsService.ts";
import {
  makeAdapterRegistryMock,
  type KindAdapterMap,
} from "../testUtils/providerAdapterRegistryMock.ts";

const defaultServerSettingsLayer = ServerSettings.ServerSettingsService.layerTest();
const serverConfigTestLayer = ServerConfig.layerTest(process.cwd(), process.cwd()).pipe(
  Layer.provide(NodeServices.layer),
);

const asRequestId = (value: string): ApprovalRequestId => ApprovalRequestId.make(value);
const asEventId = (value: string): EventId => EventId.make(value);
const asThreadId = (value: string): ThreadId => ThreadId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);
const environmentId = EnvironmentId.make("environment-provider-service-test");
const codexInstanceId = ProviderInstanceId.make("codex");
const claudeAgentInstanceId = ProviderInstanceId.make("claudeAgent");
const grokInstanceId = ProviderInstanceId.make("grok");
const cursorInstanceId = ProviderInstanceId.make("cursor");
const CODEX_DRIVER = ProviderDriverKind.make("codex");
const CLAUDE_AGENT_DRIVER = ProviderDriverKind.make("claudeAgent");
const GROK_DRIVER = ProviderDriverKind.make("grok");
const CURSOR_DRIVER = ProviderDriverKind.make("cursor");

type LegacyProviderRuntimeEvent = {
  readonly type: string;
  readonly eventId: EventId;
  readonly provider: ProviderDriverKind;
  readonly createdAt: string;
  readonly threadId: ThreadId;
  readonly turnId?: string | undefined;
  readonly itemId?: string | undefined;
  readonly requestId?: string | undefined;
  readonly payload?: unknown | undefined;
  readonly [key: string]: unknown;
};

function makeFakeCodexAdapter(provider: ProviderDriverKind = CODEX_DRIVER) {
  const sessions = new Map<ThreadId, ProviderSession>();
  const runtimeEventPubSub = Effect.runSync(PubSub.unbounded<ProviderRuntimeEvent>());
  let runtimeEventGate: Deferred.Deferred<void> | undefined;

  const startSession = vi.fn((input: ProviderSessionStartInput) =>
    Effect.sync(() => {
      const now = "2026-01-01T00:00:00.000Z";
      const session: ProviderSession = {
        provider,
        ...(input.providerInstanceId !== undefined
          ? { providerInstanceId: input.providerInstanceId }
          : {}),
        status: input.activeTurnId !== undefined ? "running" : "ready",
        runtimeMode: input.runtimeMode,
        threadId: input.threadId,
        resumeCursor: input.resumeCursor ?? {
          opaque: `resume-${String(input.threadId)}`,
        },
        cwd: input.cwd ?? process.cwd(),
        createdAt: now,
        updatedAt: now,
        ...(input.activeTurnId !== undefined ? { activeTurnId: input.activeTurnId } : {}),
      };
      sessions.set(session.threadId, session);
      return session;
    }),
  );

  const sendTurn = vi.fn(
    (
      input: ProviderSendTurnInput,
    ): Effect.Effect<ProviderTurnStartResult, ProviderAdapterError> => {
      const session = sessions.get(input.threadId);
      if (!session) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({
            provider,
            threadId: input.threadId,
          }),
        );
      }

      return Effect.sync(() => {
        const turnId = TurnId.make(`turn-${String(input.threadId)}`);
        sessions.set(input.threadId, {
          ...session,
          status: "running",
          activeTurnId: turnId,
          updatedAt: "2026-01-01T00:00:01.000Z",
        });
        return {
          threadId: input.threadId,
          turnId,
        };
      });
    },
  );

  const interruptTurn = vi.fn(
    (_threadId: ThreadId, _turnId?: TurnId): Effect.Effect<void, ProviderAdapterError> =>
      Effect.void,
  );

  const respondToRequest = vi.fn(
    (
      _threadId: ThreadId,
      _requestId: string,
      _decision: ProviderApprovalDecision,
    ): Effect.Effect<void, ProviderAdapterError> => Effect.void,
  );

  const respondToUserInput = vi.fn(
    (
      _threadId: ThreadId,
      _requestId: string,
      _answers: Record<string, unknown>,
    ): Effect.Effect<void, ProviderAdapterError> => Effect.void,
  );

  const stopSession = vi.fn(
    (threadId: ThreadId): Effect.Effect<void, ProviderAdapterError> =>
      Effect.sync(() => {
        sessions.delete(threadId);
      }),
  );

  const listSessions = vi.fn(
    (): Effect.Effect<ReadonlyArray<ProviderSession>> =>
      Effect.sync(() => Array.from(sessions.values())),
  );

  const hasSession = vi.fn(
    (threadId: ThreadId): Effect.Effect<boolean> => Effect.succeed(sessions.has(threadId)),
  );

  const readThread = vi.fn(
    (
      threadId: ThreadId,
    ): Effect.Effect<
      {
        threadId: ThreadId;
        turns: ReadonlyArray<{ id: TurnId; items: readonly [] }>;
      },
      ProviderAdapterError
    > =>
      Effect.succeed({
        threadId,
        turns: [{ id: asTurnId("turn-1"), items: [] }],
      }),
  );

  const rollbackThread = vi.fn(
    (
      threadId: ThreadId,
      _numTurns: number,
    ): Effect.Effect<{ threadId: ThreadId; turns: readonly [] }, ProviderAdapterError> =>
      Effect.succeed({ threadId, turns: [] }),
  );

  const stopAll = vi.fn(
    (): Effect.Effect<void, ProviderAdapterError> =>
      Effect.sync(() => {
        sessions.clear();
      }),
  );

  const adapter: ProviderAdapterShape<ProviderAdapterError> = {
    provider,
    capabilities: {
      sessionModelSwitch: "in-session",
    },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
    get streamEvents() {
      return Stream.fromPubSub(runtimeEventPubSub).pipe(
        Stream.mapEffect((event) => {
          const gate = runtimeEventGate;
          return gate === undefined
            ? Effect.succeed(event)
            : Deferred.await(gate).pipe(Effect.as(event));
        }),
      );
    },
  };

  const emit = (event: LegacyProviderRuntimeEvent): void => {
    Effect.runSync(PubSub.publish(runtimeEventPubSub, event as unknown as ProviderRuntimeEvent));
  };

  const updateSession = (
    threadId: ThreadId,
    update: (session: ProviderSession) => ProviderSession,
  ): void => {
    const existing = sessions.get(threadId);
    if (!existing) {
      return;
    }
    sessions.set(threadId, update(existing));
  };

  const pauseRuntimeEvents = Deferred.make<void>().pipe(
    Effect.map((gate) => {
      runtimeEventGate = gate;
      return Effect.sync(() => {
        if (runtimeEventGate === gate) runtimeEventGate = undefined;
      }).pipe(Effect.andThen(Deferred.succeed(gate, undefined)), Effect.asVoid);
    }),
  );

  return {
    adapter,
    emit,
    sessions,
    updateSession,
    pauseRuntimeEvents,
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
  };
}

const advanceTestClock = (ms: number) =>
  TestClock.adjust(`${ms} millis`).pipe(Effect.andThen(Effect.yieldNow));

interface MeaningfulOutputCase {
  readonly name: string;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId: ProviderInstanceId;
  readonly output: Pick<LegacyProviderRuntimeEvent, "type" | "payload"> & {
    readonly itemId?: string;
  };
}

const assertSuccessfulOutputOnlyTurn = (
  testCase: MeaningfulOutputCase,
  adapter: ReturnType<typeof makeFakeCodexAdapter>,
) =>
  Effect.gen(function* () {
    const provider = yield* ProviderService.ProviderService;
    const testId = testCase.name.toLowerCase().replaceAll(" ", "-");
    const threadId = asThreadId(`thread-${testId}`);
    const turnId = asTurnId(`turn-${testId}`);
    yield* provider.startSession(threadId, {
      provider: testCase.provider,
      providerInstanceId: testCase.providerInstanceId,
      threadId,
      runtimeMode: "full-access",
    });
    const completedEventId = asEventId(`evt-${testId}-completed`);
    const completed = yield* provider.streamEvents.pipe(
      Stream.filter((event) => event.eventId === completedEventId),
      Stream.take(1),
      Stream.runCollect,
      Effect.forkChild,
    );
    yield* advanceTestClock(50);

    adapter.emit({
      type: "turn.started",
      eventId: asEventId(`evt-${testId}-started`),
      provider: testCase.provider,
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId,
      turnId,
      payload: {},
    });
    adapter.emit({
      ...testCase.output,
      eventId: asEventId(`evt-${testId}-output`),
      provider: testCase.provider,
      createdAt: "2026-01-01T00:00:01.000Z",
      threadId,
      turnId,
    });
    adapter.emit({
      type: "turn.completed",
      eventId: completedEventId,
      provider: testCase.provider,
      createdAt: "2026-01-01T00:00:02.000Z",
      threadId,
      turnId,
      payload: { state: "completed" },
    });
    if (testCase.provider === CURSOR_DRIVER) {
      yield* advanceTestClock(200);
    }

    const events = Array.from(yield* Fiber.join(completed));
    assert.equal(events[0]?.type, "turn.completed");
    if (events[0]?.type === "turn.completed") {
      assert.equal(events[0].payload.state, "completed");
      assert.equal(events[0].payload.errorMessage, undefined);
    }
  });

const hasMetricSnapshot = (
  snapshots: ReadonlyArray<Metric.Metric.Snapshot>,
  id: string,
  attributes: Readonly<Record<string, string>>,
) =>
  snapshots.some(
    (snapshot) =>
      snapshot.id === id &&
      Object.entries(attributes).every(([key, value]) => snapshot.attributes?.[key] === value),
  );

function makeProviderServiceLayer(options?: ProviderServiceLiveOptions, adapters?: KindAdapterMap) {
  const codex = makeFakeCodexAdapter();
  const claude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
  const cursor = makeFakeCodexAdapter(CURSOR_DRIVER);
  const registry = makeAdapterRegistryMock(
    adapters ?? {
      [ProviderDriverKind.make("codex")]: codex.adapter,
      [ProviderDriverKind.make("claudeAgent")]: claude.adapter,
      [ProviderDriverKind.make("cursor")]: cursor.adapter,
    },
  );

  const providerAdapterLayer = Layer.succeed(
    ProviderAdapterRegistry.ProviderAdapterRegistry,
    registry,
  );
  const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
    Layer.provide(SqlitePersistenceMemory),
  );
  const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));

  const layer = it.layer(
    Layer.mergeAll(
      makeProviderServiceLive(options).pipe(
        Layer.provide(providerAdapterLayer),
        Layer.provide(directoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(serverConfigTestLayer),
        Layer.provideMerge(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      ),
      directoryLayer,

      runtimeRepositoryLayer,
      NodeServices.layer,
    ),
  );

  return {
    codex,
    claude,
    cursor,
    registry,
    layer,
  };
}

it.effect("ProviderServiceLive catches stopAll failures during shutdown", () =>
  Effect.gen(function* () {
    const codex = makeFakeCodexAdapter();
    codex.stopAll.mockImplementation(() =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: String(CODEX_DRIVER),
          method: "stopAll",
          detail: "simulated stopAll failure",
        }),
      ),
    );
    const registry = makeAdapterRegistryMock({
      [CODEX_DRIVER]: codex.adapter,
    });
    const providerAdapterLayer = Layer.succeed(
      ProviderAdapterRegistry.ProviderAdapterRegistry,
      registry,
    );
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const providerLayer = Layer.mergeAll(
      makeProviderServiceLive().pipe(
        Layer.provide(providerAdapterLayer),
        Layer.provide(directoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(serverConfigTestLayer),
        Layer.provideMerge(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      ),
      directoryLayer,
      runtimeRepositoryLayer,
      NodeServices.layer,
    );
    const scope = yield* Scope.make();
    const runtimeServices = yield* Layer.build(providerLayer).pipe(Scope.provide(scope));

    yield* ProviderService.ProviderService.pipe(Effect.provide(runtimeServices));
    const closeExit = yield* Scope.close(scope, Exit.void).pipe(Effect.exit);

    assert.equal(Exit.isSuccess(closeExit), true);
    assert.equal(codex.stopAll.mock.calls.length, 1);
  }),
);

it.effect("ProviderServiceLive rejects new sessions for disabled providers", () =>
  Effect.gen(function* () {
    const codex = makeFakeCodexAdapter();
    const claude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
    const registryBase = makeAdapterRegistryMock({
      [CODEX_DRIVER]: codex.adapter,
      [CLAUDE_AGENT_DRIVER]: claude.adapter,
    });
    const registry: ProviderAdapterRegistry.ProviderAdapterRegistry["Service"] = {
      ...registryBase,
      getInstanceInfo: (instanceId) =>
        instanceId === claudeAgentInstanceId
          ? Effect.succeed({
              instanceId,
              driverKind: CLAUDE_AGENT_DRIVER,
              displayName: undefined,
              enabled: false,
              continuationIdentity: {
                driverKind: CLAUDE_AGENT_DRIVER,
                continuationKey: "claudeAgent:instance:claudeAgent",
              },
            })
          : registryBase.getInstanceInfo(instanceId),
    };
    const providerAdapterLayer = Layer.succeed(
      ProviderAdapterRegistry.ProviderAdapterRegistry,
      registry,
    );
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(providerAdapterLayer),
      Layer.provide(directoryLayer),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(serverConfigTestLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );

    const failure = yield* Effect.flip(
      Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        return yield* provider.startSession(asThreadId("thread-disabled"), {
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: claudeAgentInstanceId,
          threadId: asThreadId("thread-disabled"),
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(providerLayer)),
    );

    assert.instanceOf(failure, ProviderValidationError);
    assert.include(failure.issue, "Provider instance 'claudeAgent' is disabled");
    assert.equal(claude.startSession.mock.calls.length, 0);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "ProviderServiceLive allows enabled custom instances when legacy driver is disabled",
  () =>
    Effect.gen(function* () {
      const instanceId = ProviderInstanceId.make("codex_personal");
      const driverKind = CODEX_DRIVER;
      const codex = makeFakeCodexAdapter();
      const unsupported = () =>
        new ProviderUnsupportedError({
          provider: driverKind,
        });
      const registry: ProviderAdapterRegistry.ProviderAdapterRegistry["Service"] = {
        getByInstance: (requestedInstanceId) =>
          requestedInstanceId === instanceId
            ? Effect.succeed(codex.adapter)
            : Effect.fail(unsupported()),
        getInstanceInfo: (requestedInstanceId) =>
          requestedInstanceId === instanceId
            ? Effect.succeed({
                instanceId,
                driverKind,
                displayName: "Codex Personal",
                enabled: true,
                continuationIdentity: {
                  driverKind,
                  continuationKey: "codex:/Users/example/.codex",
                },
              })
            : Effect.fail(unsupported()),
        listInstances: () => Effect.succeed([instanceId]),
        listProviders: () => Effect.succeed([driverKind] as const),
        streamChanges: Stream.empty,
        subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), (pubsub) =>
          PubSub.subscribe(pubsub),
        ),
      };
      const providerAdapterLayer = Layer.succeed(
        ProviderAdapterRegistry.ProviderAdapterRegistry,
        registry,
      );
      const serverSettingsLayer = ServerSettings.ServerSettingsService.layerTest({
        providers: {
          codex: {
            enabled: false,
          },
        },
      });
      const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
        Layer.provide(SqlitePersistenceMemory),
      );
      const directoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const providerLayer = makeProviderServiceLive().pipe(
        Layer.provide(providerAdapterLayer),
        Layer.provide(directoryLayer),
        Layer.provide(serverSettingsLayer),
        Layer.provide(serverConfigTestLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );

      const session = yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        return yield* provider.startSession(asThreadId("thread-enabled-custom"), {
          provider: driverKind,
          providerInstanceId: instanceId,
          threadId: asThreadId("thread-enabled-custom"),
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(providerLayer));

      assert.equal(session.providerInstanceId, instanceId);
      assert.equal(codex.startSession.mock.calls.length, 1);
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("ProviderServiceLive rejects new sessions for disabled custom instances", () =>
  Effect.gen(function* () {
    const instanceId = ProviderInstanceId.make("codex_personal");
    const driverKind = ProviderDriverKind.make("codex");
    const codex = makeFakeCodexAdapter();
    const unsupported = () =>
      new ProviderUnsupportedError({
        provider: ProviderDriverKind.make("codex"),
      });
    const registry: ProviderAdapterRegistry.ProviderAdapterRegistry["Service"] = {
      getByInstance: (requestedInstanceId) =>
        requestedInstanceId === instanceId
          ? Effect.succeed(codex.adapter)
          : Effect.fail(unsupported()),
      getInstanceInfo: (requestedInstanceId) =>
        requestedInstanceId === instanceId
          ? Effect.succeed({
              instanceId,
              driverKind,
              displayName: "Codex Personal",
              enabled: false,
              continuationIdentity: {
                driverKind,
                continuationKey: "codex:/Users/example/.codex",
              },
            })
          : Effect.fail(unsupported()),
      listInstances: () => Effect.succeed([instanceId]),
      listProviders: () => Effect.succeed([CODEX_DRIVER] as const),
      streamChanges: Stream.empty,
      subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), (pubsub) =>
        PubSub.subscribe(pubsub),
      ),
    };
    const providerAdapterLayer = Layer.succeed(
      ProviderAdapterRegistry.ProviderAdapterRegistry,
      registry,
    );
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(providerAdapterLayer),
      Layer.provide(directoryLayer),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(serverConfigTestLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );

    const failure = yield* Effect.flip(
      Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        return yield* provider.startSession(asThreadId("thread-disabled-instance"), {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: asThreadId("thread-disabled-instance"),
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(providerLayer)),
    );

    assert.instanceOf(failure, ProviderValidationError);
    assert.include(failure.issue, "Provider instance 'codex_personal' is disabled");
    assert.equal(codex.startSession.mock.calls.length, 0);
  }).pipe(Effect.provide(NodeServices.layer)),
);

const routing = makeProviderServiceLayer({ sessionStartTimeoutMs: 50 });

it.effect("ProviderServiceLive writes canonical events to the emitting thread segment", () =>
  Effect.gen(function* () {
    const codex = makeFakeCodexAdapter();
    const canonicalEvents: ProviderRuntimeEvent[] = [];
    const canonicalThreadIds: Array<string | null> = [];
    const registry = makeAdapterRegistryMock({
      [ProviderDriverKind.make("codex")]: codex.adapter,
    });
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const providerLayer = makeProviderServiceLive({
      canonicalEventLogger: {
        filePath: "memory://provider-canonical-events",
        write: (event, threadId) => {
          canonicalEvents.push(event as ProviderRuntimeEvent);
          canonicalThreadIds.push(threadId ?? null);
          return Effect.void;
        },
        close: () => Effect.void,
      },
    }).pipe(
      Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
      Layer.provide(directoryLayer),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(serverConfigTestLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );

    yield* Effect.gen(function* () {
      yield* ProviderService.ProviderService;
      yield* advanceTestClock(10);
      codex.emit({
        eventId: asEventId("evt-canonical-thread-segment"),
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-canonical-thread-segment"),
        createdAt: "2026-01-01T00:00:00.000Z",
        type: "turn.completed",
        payload: {
          state: "completed",
        },
      });
      yield* advanceTestClock(20);
    }).pipe(Effect.provide(providerLayer));

    assert.equal(canonicalEvents.length, 1);
    assert.equal(canonicalEvents[0]?.threadId, "thread-canonical-thread-segment");
    assert.deepEqual(canonicalThreadIds, ["thread-canonical-thread-segment"]);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("ProviderServiceLive keeps persisted resumable sessions on startup", () =>
  Effect.gen(function* () {
    const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-service-"));
    const dbPath = NodePath.join(tempDir, "orchestration.sqlite");

    const codex = makeFakeCodexAdapter();
    const registry = makeAdapterRegistryMock({
      [ProviderDriverKind.make("codex")]: codex.adapter,
    });

    const persistenceLayer = makeSqlitePersistenceLive(dbPath);
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(persistenceLayer),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));

    yield* Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      yield* directory.upsert({
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: ThreadId.make("thread-stale"),
      });
    }).pipe(Effect.provide(directoryLayer));

    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
      Layer.provide(directoryLayer),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(serverConfigTestLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );

    yield* ProviderService.ProviderService.pipe(Effect.provide(providerLayer));

    const persistedProvider = yield* Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      return yield* directory.getProvider(asThreadId("thread-stale"));
    }).pipe(Effect.provide(directoryLayer));
    assert.equal(persistedProvider, "codex");

    const runtime = yield* Effect.gen(function* () {
      const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      return yield* repository.getByThreadId({
        threadId: asThreadId("thread-stale"),
      });
    }).pipe(Effect.provide(runtimeRepositoryLayer));
    assert.equal(Option.isSome(runtime), true);

    const legacyTableRows = yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      return yield* sql<{ readonly name: string }>`
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'provider_sessions'
      `;
    }).pipe(Effect.provide(persistenceLayer));
    assert.equal(legacyTableRows.length, 0);

    NodeFS.rmSync(tempDir, { recursive: true, force: true });
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "ProviderServiceLive restores rollback routing after restart using persisted thread mapping",
  () =>
    Effect.gen(function* () {
      const tempDir = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "t3-provider-service-restart-"),
      );
      const dbPath = NodePath.join(tempDir, "orchestration.sqlite");
      const persistenceLayer = makeSqlitePersistenceLive(dbPath);
      const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
        Layer.provide(persistenceLayer),
      );

      const firstCodex = makeFakeCodexAdapter();
      const firstRegistry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("codex")]: firstCodex.adapter,
      });

      const firstDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const firstProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(
          Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, firstRegistry),
        ),
        Layer.provide(firstDirectoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(serverConfigTestLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );
      const updatedResumeCursor = {
        threadId: asThreadId("thread-1"),
        resume: "resume-session-1",
        resumeSessionAt: "assistant-message-1",
        turnCount: 1,
      };

      const startedSession = yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const threadId = asThreadId("thread-1");
        const session = yield* provider.startSession(threadId, {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: codexInstanceId,
          cwd: "/tmp/project",
          runtimeMode: "full-access",
          threadId,
        });
        firstCodex.updateSession(threadId, (existing) => ({
          ...existing,
          status: "ready",
          resumeCursor: updatedResumeCursor,
          updatedAt: "2026-01-01T00:00:01.000Z",
        }));
        return session;
      }).pipe(Effect.provide(firstProviderLayer));

      const persistedAfterStopAll = yield* Effect.gen(function* () {
        const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
        return yield* repository.getByThreadId({
          threadId: startedSession.threadId,
        });
      }).pipe(Effect.provide(runtimeRepositoryLayer));
      assert.equal(Option.isSome(persistedAfterStopAll), true);
      if (Option.isSome(persistedAfterStopAll)) {
        assert.equal(persistedAfterStopAll.value.status, "stopped");
        assert.deepEqual(persistedAfterStopAll.value.resumeCursor, updatedResumeCursor);
      }

      const secondCodex = makeFakeCodexAdapter();
      const secondRegistry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("codex")]: secondCodex.adapter,
      });
      const secondDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const secondProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(
          Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, secondRegistry),
        ),
        Layer.provide(secondDirectoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(serverConfigTestLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );

      secondCodex.startSession.mockClear();
      secondCodex.rollbackThread.mockClear();

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        yield* provider.rollbackConversation({
          threadId: startedSession.threadId,
          numTurns: 1,
        });
      }).pipe(Effect.provide(secondProviderLayer));

      assert.equal(secondCodex.startSession.mock.calls.length, 1);
      const resumedStartInput = secondCodex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "codex");
        assert.equal(startPayload.cwd, "/tmp/project");
        assert.deepEqual(startPayload.resumeCursor, updatedResumeCursor);
        assert.equal(startPayload.threadId, startedSession.threadId);
      }
      assert.equal(secondCodex.rollbackThread.mock.calls.length, 1);
      const rollbackCall = secondCodex.rollbackThread.mock.calls[0];
      assert.equal(typeof rollbackCall?.[0], "string");
      assert.equal(rollbackCall?.[1], 1);

      NodeFS.rmSync(tempDir, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
);

routing.layer("ProviderServiceLive routing", (it) => {
  it.effect("routes provider operations and rollback conversation", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const session = yield* provider.startSession(asThreadId("thread-1"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-1"),
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      });
      assert.equal(session.provider, "codex");

      const sessions = yield* provider.listSessions();
      assert.equal(sessions.length, 1);

      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "hello",
        attachments: [],
      });
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);

      yield* provider.interruptTurn({ threadId: session.threadId });
      assert.deepEqual(routing.codex.interruptTurn.mock.calls, [[session.threadId, undefined]]);

      yield* provider.respondToRequest({
        threadId: session.threadId,
        requestId: asRequestId("req-1"),
        decision: "accept",
      });
      assert.deepEqual(routing.codex.respondToRequest.mock.calls, [
        [session.threadId, asRequestId("req-1"), "accept"],
      ]);

      yield* provider.respondToUserInput({
        threadId: session.threadId,
        requestId: asRequestId("req-user-input-1"),
        answers: {
          sandbox_mode: "workspace-write",
        },
      });
      assert.deepEqual(routing.codex.respondToUserInput.mock.calls, [
        [
          session.threadId,
          asRequestId("req-user-input-1"),
          {
            sandbox_mode: "workspace-write",
          },
        ],
      ]);

      yield* provider.rollbackConversation({
        threadId: session.threadId,
        numTurns: 0,
      });

      yield* provider.stopSession({ threadId: session.threadId });
      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "after-stop",
        attachments: [],
      });

      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      const resumedStartInput = routing.codex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "codex");
        assert.equal(startPayload.cwd, "/tmp/project");
        assert.deepEqual(startPayload.resumeCursor, session.resumeCursor);
        assert.equal(startPayload.threadId, session.threadId);
      }
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);
    }),
  );

  it.effect("appends attachment file paths to the turn input text", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const session = yield* provider.startSession(asThreadId("thread-attach"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-attach"),
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      });

      const attachment = {
        type: "image" as const,
        id: "thread-attach-12345678-1234-1234-1234-123456789abc",
        name: "screenshot.png",
        mimeType: "image/png",
        sizeBytes: 123,
      };

      routing.codex.sendTurn.mockClear();
      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "use this screenshot",
        attachments: [attachment],
      });

      const turnInput = routing.codex.sendTurn.mock.calls[0]?.[0] as ProviderSendTurnInput;
      assert.equal(typeof turnInput.input, "string");
      const turnText = turnInput.input ?? "";
      assert.equal(turnText.startsWith("use this screenshot"), true);
      assert.include(turnText, '[Attached image "screenshot.png" is saved at: ');
      assert.equal(turnText.endsWith(`${attachment.id}.png]`), true);

      // An attachment-only turn stays valid and the injected line becomes the
      // whole input text, so the agent still learns the path.
      routing.codex.sendTurn.mockClear();
      yield* provider.sendTurn({
        threadId: session.threadId,
        attachments: [attachment],
      });
      const imageOnlyInput = routing.codex.sendTurn.mock.calls[0]?.[0] as ProviderSendTurnInput;
      assert.equal(imageOnlyInput.input?.startsWith('[Attached image "screenshot.png"'), true);

      yield* provider.stopSession({ threadId: session.threadId });
    }),
  );

  it.effect("recovers stale persisted sessions for rollback by resuming thread identity", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const initial = yield* provider.startSession(asThreadId("thread-1"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-1"),
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      });
      yield* routing.codex.stopSession(initial.threadId);
      routing.codex.startSession.mockClear();
      routing.codex.rollbackThread.mockClear();

      yield* provider.rollbackConversation({
        threadId: initial.threadId,
        numTurns: 1,
      });

      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      const resumedStartInput = routing.codex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "codex");
        assert.equal(startPayload.cwd, "/tmp/project");
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.equal(startPayload.threadId, initial.threadId);
      }
      assert.equal(routing.codex.rollbackThread.mock.calls.length, 1);
      const rollbackCall = routing.codex.rollbackThread.mock.calls[0];
      assert.equal(rollbackCall?.[1], 1);
    }),
  );

  it.effect("preserves the persisted binding when stopping a session", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

      const initial = yield* provider.startSession(asThreadId("thread-reap-preserve"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-reap-preserve"),
        cwd: "/tmp/project-reap-preserve",
        runtimeMode: "full-access",
      });

      yield* provider.stopSession({ threadId: initial.threadId });

      const persistedAfterStop = yield* runtimeRepository.getByThreadId({
        threadId: initial.threadId,
      });
      assert.equal(Option.isSome(persistedAfterStop), true);
      if (Option.isSome(persistedAfterStop)) {
        assert.equal(persistedAfterStop.value.status, "stopped");
        assert.deepEqual(persistedAfterStop.value.resumeCursor, initial.resumeCursor);
      }

      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId: initial.threadId,
        input: "resume after reap",
        attachments: [],
      });

      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      const resumedStartInput = routing.codex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "codex");
        assert.equal(startPayload.cwd, "/tmp/project-reap-preserve");
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.equal(startPayload.threadId, initial.threadId);
      }
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);
    }),
  );

  it.effect("routes explicit claudeAgent provider session starts to the claude adapter", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const session = yield* provider.startSession(asThreadId("thread-claude"), {
        provider: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claudeAgentInstanceId,
        threadId: asThreadId("thread-claude"),
        cwd: "/tmp/project-claude",
        runtimeMode: "full-access",
      });

      assert.equal(session.provider, "claudeAgent");
      assert.equal(routing.claude.startSession.mock.calls.length, 1);
      const startInput = routing.claude.startSession.mock.calls[0]?.[0];
      assert.equal(typeof startInput === "object" && startInput !== null, true);
      if (startInput && typeof startInput === "object") {
        const startPayload = startInput as {
          provider?: string;
          providerInstanceId?: ProviderInstanceId;
          cwd?: string;
        };
        assert.equal(startPayload.provider, "claudeAgent");
        assert.equal(startPayload.providerInstanceId, claudeAgentInstanceId);
        assert.equal(startPayload.cwd, "/tmp/project-claude");
      }
    }),
  );

  it.effect("preserves detached session metadata when sending a turn", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-detached-send-turn");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claudeAgentInstanceId,
        threadId,
        cwd: "/tmp/project-detached-send-turn",
        runtimeMode: "full-access",
        detached: true,
      });

      const turn = yield* provider.sendTurn({
        threadId,
        input: "hello",
        attachments: [],
      });

      const binding = yield* directory.getBinding(threadId);
      assert.equal(Option.isSome(binding), true);
      if (Option.isSome(binding)) {
        const runtimePayload = binding.value.runtimePayload as {
          readonly activeTurnId?: unknown;
          readonly detached?: unknown;
          readonly lastRuntimeEvent?: unknown;
        };
        assert.equal(runtimePayload.detached, true);
        assert.equal(runtimePayload.activeTurnId, turn.turnId);
        assert.equal(runtimePayload.lastRuntimeEvent, "provider.sendTurn");
      }
    }),
  );

  it.effect("clears persisted active turn state on matching terminal turn events", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-terminal-active-turn");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/project-terminal-active-turn",
        runtimeMode: "full-access",
      });

      const turn = yield* provider.sendTurn({
        threadId,
        input: "hello",
        attachments: [],
      });

      const staleCompletedEventId = asEventId("evt-stale-turn-completed");
      const staleCompletedFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === staleCompletedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      routing.codex.emit({
        type: "turn.completed",
        eventId: staleCompletedEventId,
        provider: ProviderDriverKind.make("codex"),
        threadId,
        turnId: asTurnId("turn-stale"),
        createdAt: "2026-01-01T00:00:02.000Z",
        payload: { state: "completed" },
      });
      yield* Fiber.join(staleCompletedFiber);

      const staleBinding = yield* directory.getBinding(threadId);
      assert.equal(Option.isSome(staleBinding), true);
      if (Option.isSome(staleBinding)) {
        const runtimePayload = staleBinding.value.runtimePayload as {
          readonly activeTurnId?: unknown;
        };
        assert.equal(runtimePayload.activeTurnId, turn.turnId);
        assert.equal(staleBinding.value.status, "running");
      }

      const activeCompletedEventId = asEventId("evt-active-turn-completed");
      const activeCompletedFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === activeCompletedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      routing.codex.emit({
        type: "turn.completed",
        eventId: activeCompletedEventId,
        provider: ProviderDriverKind.make("codex"),
        threadId,
        turnId: turn.turnId,
        createdAt: "2026-01-01T00:00:03.000Z",
        payload: { state: "completed" },
      });
      const activeCompletedEvents = Array.from(yield* Fiber.join(activeCompletedFiber));
      assert.equal(activeCompletedEvents[0]?.type, "turn.completed");
      assert.equal(activeCompletedEvents[0]?.threadId, threadId);
      assert.equal(activeCompletedEvents[0]?.turnId, turn.turnId);

      const completedBinding = yield* directory.getBinding(threadId);
      assert.equal(Option.isSome(completedBinding), true);
      if (Option.isSome(completedBinding)) {
        const runtimePayload = completedBinding.value.runtimePayload as {
          readonly activeTurnId?: unknown;
          readonly lastRuntimeEvent?: unknown;
        };
        assert.equal(runtimePayload.lastRuntimeEvent, "turn.completed");
        assert.equal(runtimePayload.activeTurnId, null);
        assert.equal(completedBinding.value.status, "running");
      }
    }),
  );

  it.effect("does not let stale turn.started events replace the persisted active turn", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-stale-started-active-turn");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/project-stale-started-active-turn",
        runtimeMode: "full-access",
      });

      const turn = yield* provider.sendTurn({
        threadId,
        input: "hello",
        attachments: [],
      });

      const staleStartedEventId = asEventId("evt-stale-turn-started");
      const staleStartedFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === staleStartedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      routing.codex.emit({
        type: "content.delta",
        eventId: asEventId("evt-active-before-stale-started-output"),
        provider: CODEX_DRIVER,
        threadId,
        turnId: turn.turnId,
        itemId: "item-active-before-stale-started-output",
        createdAt: "2026-01-01T00:00:03.250Z",
        payload: {
          streamKind: "assistant_text",
          delta: "active turn output",
        },
      });
      routing.codex.emit({
        type: "turn.started",
        eventId: staleStartedEventId,
        provider: ProviderDriverKind.make("codex"),
        threadId,
        turnId: asTurnId("turn-stale-started"),
        createdAt: "2026-01-01T00:00:03.500Z",
        payload: {},
      });
      yield* Fiber.join(staleStartedFiber);

      const staleBinding = yield* directory.getBinding(threadId);
      assert.equal(Option.isSome(staleBinding), true);
      if (Option.isSome(staleBinding)) {
        const runtimePayload = staleBinding.value.runtimePayload as {
          readonly activeTurnId?: unknown;
          readonly lastRuntimeEvent?: unknown;
        };
        assert.equal(runtimePayload.activeTurnId, turn.turnId);
        assert.equal(runtimePayload.lastRuntimeEvent, "provider.sendTurn");
      }

      const activeCompletedEventId = asEventId("evt-active-after-stale-started-completed");
      const activeCompletedFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === activeCompletedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      routing.codex.emit({
        type: "turn.completed",
        eventId: activeCompletedEventId,
        provider: ProviderDriverKind.make("codex"),
        threadId,
        turnId: turn.turnId,
        createdAt: "2026-01-01T00:00:04.000Z",
        payload: { state: "completed" },
      });
      const activeCompletedEvents = Array.from(yield* Fiber.join(activeCompletedFiber));
      assert.equal(activeCompletedEvents[0]?.type, "turn.completed");
      if (activeCompletedEvents[0]?.type === "turn.completed") {
        assert.equal(activeCompletedEvents[0].payload.state, "completed");
        assert.equal(activeCompletedEvents[0].payload.errorMessage, undefined);
      }

      const completedBinding = yield* directory.getBinding(threadId);
      assert.equal(Option.isSome(completedBinding), true);
      if (Option.isSome(completedBinding)) {
        const runtimePayload = completedBinding.value.runtimePayload as {
          readonly activeTurnId?: unknown;
          readonly lastRuntimeEvent?: unknown;
        };
        assert.equal(runtimePayload.activeTurnId, null);
        assert.equal(runtimePayload.lastRuntimeEvent, "turn.completed");
      }
    }),
  );

  it.effect("does not let duplicate turn.started events clear active turn output", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-duplicate-started-active-output");

      yield* provider.startSession(threadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId,
        input: "hello",
        attachments: [],
      });
      const completedEventId = asEventId("evt-duplicate-started-completed");
      const completedFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === completedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      routing.codex.emit({
        type: "turn.started",
        eventId: asEventId("evt-duplicate-started-initial"),
        provider: CODEX_DRIVER,
        threadId,
        turnId: turn.turnId,
        createdAt: "2026-01-01T00:00:00.000Z",
        payload: {},
      });
      routing.codex.emit({
        type: "content.delta",
        eventId: asEventId("evt-duplicate-started-output"),
        provider: CODEX_DRIVER,
        threadId,
        turnId: turn.turnId,
        itemId: "item-duplicate-started-output",
        createdAt: "2026-01-01T00:00:01.000Z",
        payload: {
          streamKind: "assistant_text",
          delta: "active turn output",
        },
      });
      routing.codex.emit({
        type: "turn.started",
        eventId: asEventId("evt-duplicate-started-replayed"),
        provider: CODEX_DRIVER,
        threadId,
        turnId: turn.turnId,
        createdAt: "2026-01-01T00:00:02.000Z",
        payload: {},
      });
      routing.codex.emit({
        type: "turn.completed",
        eventId: completedEventId,
        provider: CODEX_DRIVER,
        threadId,
        turnId: turn.turnId,
        createdAt: "2026-01-01T00:00:03.000Z",
        payload: { state: "completed" },
      });

      const events = Array.from(yield* Fiber.join(completedFiber));
      assert.equal(events[0]?.type, "turn.completed");
      if (events[0]?.type === "turn.completed") {
        assert.equal(events[0].payload.state, "completed");
        assert.equal(events[0].payload.errorMessage, undefined);
      }
    }),
  );

  it.effect("does not rewrite a stale completion after ownership advances", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-accepted-start-cleans-orphan");
      const orphanedTurnId = asTurnId("turn-orphaned-output");
      const nextTurnId = asTurnId("turn-after-orphaned-output");

      yield* provider.startSession(threadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const orphanedCompletionEventId = asEventId("evt-orphaned-output-completed-late");
      const orphanedCompletionFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === orphanedCompletionEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      routing.codex.emit({
        type: "turn.started",
        eventId: asEventId("evt-orphaned-output-started"),
        provider: CODEX_DRIVER,
        threadId,
        turnId: orphanedTurnId,
        createdAt: "2026-01-01T00:00:00.000Z",
        payload: {},
      });
      routing.codex.emit({
        type: "content.delta",
        eventId: asEventId("evt-orphaned-output-delta"),
        provider: CODEX_DRIVER,
        threadId,
        turnId: orphanedTurnId,
        itemId: "item-orphaned-output",
        createdAt: "2026-01-01T00:00:01.000Z",
        payload: {
          streamKind: "assistant_text",
          delta: "orphaned output",
        },
      });

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload =
        binding.runtimePayload !== null &&
        typeof binding.runtimePayload === "object" &&
        !Array.isArray(binding.runtimePayload)
          ? binding.runtimePayload
          : {};
      yield* directory.upsert({
        threadId,
        provider: binding.provider,
        providerInstanceId: codexInstanceId,
        runtimeMode: binding.runtimeMode ?? "full-access",
        status: binding.status ?? "running",
        ...(binding.resumeCursor !== undefined ? { resumeCursor: binding.resumeCursor } : {}),
        runtimePayload: {
          ...runtimePayload,
          activeTurnId: nextTurnId,
          lastRuntimeEvent: "provider.sendTurn",
        },
      });
      routing.codex.emit({
        type: "turn.started",
        eventId: asEventId("evt-after-orphaned-output-started"),
        provider: CODEX_DRIVER,
        threadId,
        turnId: nextTurnId,
        createdAt: "2026-01-01T00:00:02.000Z",
        payload: {},
      });
      routing.codex.emit({
        type: "turn.completed",
        eventId: orphanedCompletionEventId,
        provider: CODEX_DRIVER,
        threadId,
        turnId: orphanedTurnId,
        createdAt: "2026-01-01T00:00:03.000Z",
        payload: { state: "completed" },
      });

      const events = Array.from(yield* Fiber.join(orphanedCompletionFiber));
      assert.equal(events[0]?.type, "turn.completed");
      if (events[0]?.type === "turn.completed") {
        assert.equal(events[0].payload.state, "completed");
        assert.equal(events[0].payload.errorMessage, undefined);
      }
    }),
  );

  it.effect("does not let late turn.started events resurrect completed turns", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-late-started-after-completed");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/project-late-started-after-completed",
        runtimeMode: "full-access",
      });

      const turn = yield* provider.sendTurn({
        threadId,
        input: "hello",
        attachments: [],
      });

      const completedEventId = asEventId("evt-completed-before-late-started");
      const completedFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === completedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      routing.codex.emit({
        type: "turn.completed",
        eventId: completedEventId,
        provider: ProviderDriverKind.make("codex"),
        threadId,
        turnId: turn.turnId,
        createdAt: "2026-01-01T00:00:04.250Z",
        payload: { state: "completed" },
      });
      yield* Fiber.join(completedFiber);

      const lateStartedEventId = asEventId("evt-late-started-after-completed");
      const lateStartedFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === lateStartedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      routing.codex.emit({
        type: "turn.started",
        eventId: lateStartedEventId,
        provider: ProviderDriverKind.make("codex"),
        threadId,
        turnId: turn.turnId,
        createdAt: "2026-01-01T00:00:04.500Z",
        payload: {},
      });
      yield* Fiber.join(lateStartedFiber);

      const binding = yield* directory.getBinding(threadId);
      assert.equal(Option.isSome(binding), true);
      if (Option.isSome(binding)) {
        const runtimePayload = binding.value.runtimePayload as {
          readonly activeTurnId?: unknown;
          readonly lastRuntimeEvent?: unknown;
          readonly lastTerminalTurnId?: unknown;
        };
        assert.equal(runtimePayload.activeTurnId, null);
        assert.equal(runtimePayload.lastRuntimeEvent, "turn.completed");
        assert.equal(runtimePayload.lastTerminalTurnId, turn.turnId);
      }
    }),
  );

  it.effect("clears persisted active turn state when sendTurn fails after turn.started", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-started-then-send-failed");
      const turnId = asTurnId("turn-started-then-send-failed");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/project-started-then-send-failed",
        runtimeMode: "full-access",
      });

      const startedEventId = asEventId("evt-started-before-send-failed");
      const waitingEventId = asEventId("evt-waiting-after-started-before-send-failed");
      const startedFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === startedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      const waitingFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === waitingEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      routing.codex.sendTurn.mockImplementationOnce((input) =>
        Effect.gen(function* () {
          routing.codex.emit({
            type: "turn.started",
            eventId: startedEventId,
            provider: ProviderDriverKind.make("codex"),
            threadId: input.threadId,
            turnId,
            createdAt: "2026-01-01T00:00:04.500Z",
            payload: {},
          });
          yield* Fiber.join(startedFiber);
          routing.codex.emit({
            type: "session.state.changed",
            eventId: waitingEventId,
            provider: ProviderDriverKind.make("codex"),
            threadId: input.threadId,
            turnId,
            createdAt: "2026-01-01T00:00:04.525Z",
            payload: { state: "waiting" },
          });
          yield* Fiber.join(waitingFiber);
          routing.codex.updateSession(input.threadId, (session) => {
            const { activeTurnId: _activeTurnId, ...sessionWithoutActiveTurn } = session;
            return {
              ...sessionWithoutActiveTurn,
              status: "ready",
              updatedAt: "2026-01-01T00:00:04.550Z",
            };
          });
          return yield* new ProviderAdapterRequestError({
            provider: String(CODEX_DRIVER),
            method: "sendTurn",
            detail: "simulated send failure",
          });
        }),
      );

      const exit = yield* provider
        .sendTurn({
          threadId,
          input: "hello",
          attachments: [],
        })
        .pipe(Effect.exit);
      assert.equal(Exit.isFailure(exit), true);

      const binding = yield* directory.getBinding(threadId);
      assert.equal(Option.isSome(binding), true);
      if (Option.isSome(binding)) {
        const runtimePayload = binding.value.runtimePayload as {
          readonly activeTurnId?: unknown;
          readonly lastRuntimeEvent?: unknown;
        };
        assert.equal(binding.value.status, "error");
        assert.equal(runtimePayload.activeTurnId, null);
        assert.equal(runtimePayload.lastRuntimeEvent, "provider.sendTurn.failed");
      }
    }),
  );

  it.effect("fences queued turn lifecycle events after session/prompt rejects", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-queued-start-after-send-failed");
      const turnId = asTurnId("turn-queued-start-after-send-failed");
      const sentinelEventId = asEventId("evt-after-queued-send-failure-events");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      const sentinelFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === sentinelEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      const resumeRuntimeEvents = yield* routing.codex.pauseRuntimeEvents;
      routing.codex.sendTurn.mockImplementationOnce((input) =>
        Effect.gen(function* () {
          routing.codex.emit({
            type: "turn.started",
            eventId: asEventId("evt-queued-start-before-send-failed"),
            provider: ProviderDriverKind.make("codex"),
            threadId: input.threadId,
            turnId,
            createdAt: "2026-01-01T00:00:04.600Z",
            payload: {},
          });
          routing.codex.emit({
            type: "session.state.changed",
            eventId: asEventId("evt-queued-waiting-before-send-failed"),
            provider: ProviderDriverKind.make("codex"),
            threadId: input.threadId,
            turnId,
            createdAt: "2026-01-01T00:00:04.625Z",
            payload: { state: "waiting" },
          });
          return yield* new ProviderAdapterRequestError({
            provider: String(CODEX_DRIVER),
            method: "session/prompt",
            detail: "prompt rejected after queueing lifecycle events",
          });
        }),
      );

      const exit = yield* provider
        .sendTurn({ threadId, input: "hello", attachments: [] })
        .pipe(Effect.exit);
      assert.equal(Exit.isFailure(exit), true);

      yield* resumeRuntimeEvents;
      routing.codex.emit({
        type: "session.started",
        eventId: sentinelEventId,
        provider: ProviderDriverKind.make("codex"),
        threadId,
        createdAt: "2026-01-01T00:00:04.650Z",
        payload: {},
      });
      yield* Fiber.join(sentinelFiber);

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload = binding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly lastRuntimeEvent?: unknown;
        readonly lastFailedSendTurnOperationId?: unknown;
      };
      assert.equal(binding.status, "running");
      assert.equal(runtimePayload.activeTurnId, null);
      assert.equal(runtimePayload.lastRuntimeEvent, "provider.sendTurn.failed");
      assert.equal(typeof runtimePayload.lastFailedSendTurnOperationId, "string");
    }),
  );

  it.effect("keeps a live session reusable when sendTurn fails before turn.started", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-pre-start-send-failure-retry");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.sendTurn.mockImplementationOnce(() =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: String(CODEX_DRIVER),
            method: "session/prompt",
            detail: "pre-start prompt rejection",
          }),
        ),
      );

      const firstFailure = yield* provider
        .sendTurn({
          threadId,
          input: "prompt rejected before turn.started",
          attachments: [],
        })
        .pipe(Effect.flip);
      assert.equal(firstFailure._tag, "ProviderSendTurnFailedError");
      const failedBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const failedRuntimePayload = failedBinding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly lastError?: unknown;
        readonly sendTurnOperationId?: unknown;
      };
      assert.equal(failedBinding.status, "running");
      assert.equal(failedRuntimePayload.activeTurnId, null);
      assert.match(String(failedRuntimePayload.lastError), /pre-start prompt rejection/);
      assert.equal(failedRuntimePayload.sendTurnOperationId, null);

      const retryTurn = yield* provider.sendTurn({
        threadId,
        input: "retry on the same live session",
        attachments: [],
      });
      const retriedBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(retriedBinding.status, "running");
      assert.equal(
        (retriedBinding.runtimePayload as { readonly activeTurnId?: unknown }).activeTurnId,
        retryTurn.turnId,
      );
    }),
  );

  it.effect("preserves a pre-existing active turn when a steer fails before turn.started", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-failed-steer-existing-turn");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const existingTurn = yield* provider.sendTurn({
        threadId,
        input: "keep this provider turn running",
        attachments: [],
      });
      routing.codex.sendTurn.mockImplementationOnce(() =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: String(CODEX_DRIVER),
            method: "session/prompt",
            detail: "steer rejected before turn.started",
          }),
        ),
      );

      const steerFailure = yield* provider
        .sendTurn({
          threadId,
          input: "steer the existing provider turn",
          attachments: [],
        })
        .pipe(Effect.flip);

      assert.equal(steerFailure._tag, "ProviderSendTurnFailedError");
      if (steerFailure._tag === "ProviderSendTurnFailedError") {
        assert.equal(steerFailure.turnId, undefined);
        assert.equal(steerFailure.preservedActiveTurnId, existingTurn.turnId);
        assert.equal(steerFailure.superseded, false);
      }
      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload = binding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly lastError?: unknown;
        readonly lastFailedSendTurnOperationId?: unknown;
        readonly lastRuntimeEvent?: unknown;
        readonly lastTerminalTurnId?: unknown;
        readonly sendTurnOperationId?: unknown;
      };
      assert.equal(binding.status, "running");
      assert.equal(runtimePayload.activeTurnId, existingTurn.turnId);
      assert.equal(runtimePayload.lastError, null);
      assert.equal(typeof runtimePayload.lastFailedSendTurnOperationId, "string");
      assert.equal(runtimePayload.lastRuntimeEvent, "provider.sendTurn");
      assert.equal(runtimePayload.lastTerminalTurnId, null);
      assert.equal(runtimePayload.sendTurnOperationId, null);
      assert.equal(routing.codex.sessions.get(threadId)?.activeTurnId, existingTurn.turnId);

      routing.codex.emit({
        type: "turn.completed",
        eventId: asEventId("evt-preserved-steer-turn-completed"),
        provider: ProviderDriverKind.make("codex"),
        threadId,
        turnId: existingTurn.turnId,
        createdAt: "2026-01-01T00:00:05.000Z",
        payload: { state: "completed" },
      });
      yield* advanceTestClock(50);
      routing.codex.emit({
        type: "session.state.changed",
        eventId: asEventId("evt-session-waiting-after-preserved-steer"),
        provider: ProviderDriverKind.make("codex"),
        threadId,
        createdAt: "2026-01-01T00:00:06.000Z",
        payload: { state: "waiting" },
      });
      yield* advanceTestClock(50);

      const settledBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(settledBinding.status, "waiting");
      assert.equal(
        (settledBinding.runtimePayload as { readonly lastRuntimeEvent?: unknown }).lastRuntimeEvent,
        "session.state.changed",
      );
    }),
  );

  it.effect("fails persisted active turn state when sendTurn rejects despite a live adapter", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-started-failed-but-live");
      const turnId = asTurnId("turn-started-failed-but-live");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/project-started-failed-but-live",
        runtimeMode: "full-access",
      });

      const startedEventId = asEventId("evt-started-before-live-send-failed");
      const startedFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === startedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      routing.codex.sendTurn.mockImplementationOnce((input) =>
        Effect.gen(function* () {
          routing.codex.emit({
            type: "turn.started",
            eventId: startedEventId,
            provider: ProviderDriverKind.make("codex"),
            threadId: input.threadId,
            turnId,
            createdAt: "2026-01-01T00:00:04.750Z",
            payload: {},
          });
          yield* Fiber.join(startedFiber);
          routing.codex.updateSession(input.threadId, (session) => ({
            ...session,
            status: "running",
            activeTurnId: turnId,
            updatedAt: "2026-01-01T00:00:04.800Z",
          }));
          return yield* new ProviderAdapterRequestError({
            provider: String(CODEX_DRIVER),
            method: "sendTurn",
            detail: "simulated send failure with live steer",
          });
        }),
      );

      const exit = yield* provider
        .sendTurn({
          threadId,
          input: "hello",
          attachments: [],
        })
        .pipe(Effect.exit);
      assert.equal(Exit.isFailure(exit), true);

      const binding = yield* directory.getBinding(threadId);
      assert.equal(Option.isSome(binding), true);
      if (Option.isSome(binding)) {
        const runtimePayload = binding.value.runtimePayload as {
          readonly activeTurnId?: unknown;
          readonly lastError?: unknown;
          readonly lastRuntimeEvent?: unknown;
        };
        assert.equal(binding.value.status, "error");
        assert.equal(runtimePayload.activeTurnId, null);
        assert.equal(
          runtimePayload.lastError,
          "Provider adapter request failed (codex) for sendTurn: simulated send failure with live steer",
        );
        assert.equal(runtimePayload.lastRuntimeEvent, "provider.sendTurn.failed");
      }
    }),
  );

  it.effect("does not attribute a delayed sendTurn failure to a replacement session", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-delayed-send-failure-replacement");
      const oldSendEntered = yield* Deferred.make<void>();
      const releaseOldSend = yield* Deferred.make<void>();

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.sendTurn.mockImplementationOnce(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(oldSendEntered, undefined);
          yield* Deferred.await(releaseOldSend);
          return yield* new ProviderAdapterRequestError({
            provider: String(CODEX_DRIVER),
            method: "session/prompt",
            detail: "old prompt rejected after replacement",
          });
        }),
      );
      const oldFailureFiber = yield* provider
        .sendTurn({
          threadId,
          input: "old prompt",
          attachments: [],
        })
        .pipe(Effect.flip, Effect.forkChild);
      yield* Deferred.await(oldSendEntered);

      const oldBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      yield* directory.upsert({
        ...oldBinding,
        runtimePayload: {
          ...(oldBinding.runtimePayload as Record<string, unknown>),
          sessionOwnershipId: "replacement-session-owner",
          sendTurnOperationId: null,
        },
      });
      const replacementTurn = yield* provider.sendTurn({
        threadId,
        input: "replacement prompt",
        attachments: [],
      });
      yield* Deferred.succeed(releaseOldSend, undefined);

      const oldFailure = yield* Fiber.join(oldFailureFiber);
      assert.equal(oldFailure._tag, "ProviderSendTurnFailedError");
      if (oldFailure._tag === "ProviderSendTurnFailedError") {
        assert.equal(oldFailure.superseded, true);
        assert.equal(oldFailure.turnId, undefined);
      }
      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload = binding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly lastError?: unknown;
      };
      assert.equal(binding.status, "running");
      assert.equal(runtimePayload.activeTurnId, replacementTurn.turnId);
      assert.notEqual(runtimePayload.lastError, "old prompt rejected after replacement");
    }),
  );

  it.effect("rejects session replacement while session/prompt is in flight", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-replacement-during-send-success");
      const sendEntered = yield* Deferred.make<void>();
      const releaseSend = yield* Deferred.make<void>();
      const returnedTurnId = asTurnId("turn-returned-after-replacement-refused");
      const startsBeforeTest = routing.codex.startSession.mock.calls.length;

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.sendTurn.mockImplementationOnce(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(sendEntered, undefined);
          yield* Deferred.await(releaseSend);
          return { threadId, turnId: returnedTurnId };
        }),
      );
      const sendFiber = yield* provider
        .sendTurn({
          threadId,
          input: "prompt that must retain session ownership",
          attachments: [],
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(sendEntered);

      const replacementExit = yield* provider
        .startSession(threadId, {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: codexInstanceId,
          threadId,
          runtimeMode: "full-access",
        })
        .pipe(Effect.exit);
      assert.equal(Exit.isFailure(replacementExit), true);
      if (Exit.isFailure(replacementExit)) {
        assert.match(
          Cause.pretty(replacementExit.cause),
          /Cannot replace provider session.*session\/prompt is in flight/,
        );
      }

      yield* Deferred.succeed(releaseSend, undefined);
      const turn = yield* Fiber.join(sendFiber);
      assert.equal(turn.turnId, returnedTurnId);
      assert.equal(routing.codex.startSession.mock.calls.length, startsBeforeTest + 1);

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload = binding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly sendTurnOperationId?: unknown;
      };
      assert.equal(binding.status, "running");
      assert.equal(runtimePayload.activeTurnId, returnedTurnId);
      assert.equal(runtimePayload.sendTurnOperationId, null);
    }),
  );

  it.effect("rejects an overlapping send without replacing the first operation owner", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-overlapping-send-single-flight");
      const firstSendEntered = yield* Deferred.make<void>();
      const releaseFirstSend = yield* Deferred.make<void>();
      const firstTurnId = asTurnId("turn-overlapping-send-first-owner");
      const sendsBeforeTest = routing.codex.sendTurn.mock.calls.length;

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.sendTurn.mockImplementationOnce(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(firstSendEntered, undefined);
          yield* Deferred.await(releaseFirstSend);
          return { threadId, turnId: firstTurnId };
        }),
      );
      const firstSendFiber = yield* provider
        .sendTurn({
          threadId,
          input: "first prompt owns the operation",
          attachments: [],
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(firstSendEntered);

      const overlappingFailure = yield* provider
        .sendTurn({
          threadId,
          input: "overlapping prompt must be rejected",
          attachments: [],
        })
        .pipe(Effect.flip);
      assert.equal(overlappingFailure._tag, "ProviderSendTurnFailedError");
      if (overlappingFailure._tag === "ProviderSendTurnFailedError") {
        assert.equal(overlappingFailure.superseded, true);
        assert.equal(overlappingFailure.overlapping, true);
        assert.match(overlappingFailure.detail, /already in flight/);
      }
      assert.equal(routing.codex.sendTurn.mock.calls.length, sendsBeforeTest + 1);

      yield* Deferred.succeed(releaseFirstSend, undefined);
      const firstTurn = yield* Fiber.join(firstSendFiber);
      assert.equal(firstTurn.turnId, firstTurnId);
      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload = binding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly sendTurnOperationId?: unknown;
      };
      assert.equal(runtimePayload.activeTurnId, firstTurnId);
      assert.equal(runtimePayload.sendTurnOperationId, null);
    }),
  );

  it.effect("fails a successful send whose session ownership was superseded", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-successful-send-owner-superseded");
      const sendEntered = yield* Deferred.make<void>();
      const releaseSend = yield* Deferred.make<void>();
      const staleTurnId = asTurnId("turn-successful-send-owner-superseded");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.sendTurn.mockImplementationOnce(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(sendEntered, undefined);
          yield* Deferred.await(releaseSend);
          return { threadId, turnId: staleTurnId };
        }),
      );
      const staleSendFiber = yield* provider
        .sendTurn({
          threadId,
          input: "prompt accepted by a superseded owner",
          attachments: [],
        })
        .pipe(Effect.flip, Effect.forkChild);
      yield* Deferred.await(sendEntered);

      const oldBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      yield* directory.upsert({
        ...oldBinding,
        status: "running",
        runtimePayload: {
          ...(oldBinding.runtimePayload as Record<string, unknown>),
          sessionOwnershipId: "new-session-owner-after-send",
          sendTurnOperationId: null,
          activeTurnId: null,
          lastRuntimeEvent: "provider.replacement",
        },
      });
      yield* Deferred.succeed(releaseSend, undefined);

      const failure = yield* Fiber.join(staleSendFiber);
      assert.equal(failure._tag, "ProviderSendTurnFailedError");
      if (failure._tag === "ProviderSendTurnFailedError") {
        assert.equal(failure.superseded, true);
        assert.equal(failure.turnId, staleTurnId);
      }
      const replacementBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload = replacementBinding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly lastRuntimeEvent?: unknown;
        readonly sessionOwnershipId?: unknown;
      };
      assert.equal(replacementBinding.status, "running");
      assert.equal(runtimePayload.activeTurnId, null);
      assert.equal(runtimePayload.lastRuntimeEvent, "provider.replacement");
      assert.equal(runtimePayload.sessionOwnershipId, "new-session-owner-after-send");
    }),
  );

  it.effect("reconciles a crash-stale send token before replacing the session", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-crash-stale-send-token");
      const startsBeforeTest = routing.codex.startSession.mock.calls.length;

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const oldBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      yield* directory.upsert({
        ...oldBinding,
        runtimePayload: {
          ...(oldBinding.runtimePayload as Record<string, unknown>),
          sendTurnOperationId: "operation-from-dead-server-process",
        },
      });

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      assert.equal(routing.codex.startSession.mock.calls.length, startsBeforeTest + 2);
      const replacement = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(
        (replacement.runtimePayload as { readonly sendTurnOperationId?: unknown })
          .sendTurnOperationId,
        null,
      );
    }),
  );

  it.effect("bounds provider startup so a hung adapter releases the recovery lock", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-hung-provider-start-timeout");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.startSession.mockImplementationOnce(() => Effect.never);

      const hungStartFiber = yield* provider
        .startSession(threadId, {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: codexInstanceId,
          threadId,
          runtimeMode: "full-access",
        })
        .pipe(Effect.exit, Effect.forkChild);
      yield* Effect.yieldNow;
      yield* advanceTestClock(50);

      const hungStartExit = yield* Fiber.join(hungStartFiber);
      assert.equal(Exit.isFailure(hungStartExit), true);
      if (Exit.isFailure(hungStartExit)) {
        assert.match(Cause.pretty(hungStartExit.cause), /Provider startup timed out/);
      }

      const recovered = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      assert.equal(recovered.threadId, threadId);
    }),
  );

  it.effect("bounds provider startup while recovering a stale session for sendTurn", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-hung-provider-recovery-start-timeout");
      const initial = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/project-hung-recovery-start",
        runtimeMode: "full-access",
      });
      yield* routing.codex.stopAll();
      routing.codex.startSession.mockImplementationOnce(() => Effect.never);
      const sendCallsBeforeRecovery = routing.codex.sendTurn.mock.calls.length;

      const failureFiber = yield* provider
        .sendTurn({
          threadId: initial.threadId,
          input: "resume through a hung provider start",
          attachments: [],
        })
        .pipe(Effect.flip, Effect.forkChild);
      yield* Effect.yieldNow;
      yield* advanceTestClock(50);

      const failure = yield* Fiber.join(failureFiber);
      assert.equal(failure._tag, "ProviderSessionStartTimeoutError");
      if (failure._tag === "ProviderSessionStartTimeoutError") {
        assert.equal(failure.provider, "codex");
        assert.equal(failure.threadId, threadId);
        assert.equal(failure.timeoutMs, 50);
        assert.equal(failure.detail, "Provider startup timed out after 50ms.");
      }
      assert.equal(routing.codex.sendTurn.mock.calls.length, sendCallsBeforeRecovery);
    }),
  );

  it.effect("allows provider startup that completes just under the injected timeout", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-provider-start-just-under-timeout");
      const startEntered = yield* Deferred.make<void>();
      const defaultStartSession = routing.codex.startSession.getMockImplementation();
      assert.ok(defaultStartSession);
      routing.codex.startSession.mockImplementationOnce((input) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(startEntered, undefined);
          yield* Effect.sleep("49 millis");
          return yield* defaultStartSession(input);
        }),
      );

      const startFiber = yield* provider
        .startSession(threadId, {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: codexInstanceId,
          threadId,
          runtimeMode: "full-access",
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(startEntered);
      yield* advanceTestClock(49);

      const session = yield* Fiber.join(startFiber);
      assert.equal(session.threadId, threadId);
      assert.equal(session.status, "ready");
    }),
  );

  it.effect("clears send ownership when an in-flight prompt fiber is interrupted", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-interrupted-send-token");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.sendTurn.mockImplementationOnce(() => Effect.never);
      const sendFiber = yield* provider
        .sendTurn({
          threadId,
          input: "prompt interrupted before the adapter settles",
          attachments: [],
        })
        .pipe(Effect.forkChild);

      let registeredOperation: unknown;
      for (let attempt = 0; attempt < 20 && typeof registeredOperation !== "string"; attempt += 1) {
        yield* Effect.yieldNow;
        const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
        registeredOperation = (binding.runtimePayload as { readonly sendTurnOperationId?: unknown })
          .sendTurnOperationId;
      }
      assert.equal(typeof registeredOperation, "string");

      yield* Fiber.interrupt(sendFiber);
      const interruptedBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(
        (interruptedBinding.runtimePayload as { readonly sendTurnOperationId?: unknown })
          .sendTurnOperationId,
        null,
      );

      const replacement = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      assert.equal(replacement.threadId, threadId);
    }),
  );

  it.effect("does not resurrect a session that exits before sendTurn returns", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-exit-before-send-success");
      const sendEntered = yield* Deferred.make<void>();
      const releaseSend = yield* Deferred.make<void>();
      const returnedTurnId = asTurnId("turn-returned-after-session-exit");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.sendTurn.mockImplementationOnce(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(sendEntered, undefined);
          yield* Deferred.await(releaseSend);
          return {
            threadId,
            turnId: returnedTurnId,
          };
        }),
      );
      const sendFiber = yield* provider
        .sendTurn({
          threadId,
          input: "return after the process exits",
          attachments: [],
        })
        .pipe(Effect.flip, Effect.forkChild);
      yield* Deferred.await(sendEntered);

      routing.codex.sessions.delete(threadId);
      const exitEventId = asEventId("evt-exit-before-send-success");
      const exitConsumer = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === exitEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      routing.codex.emit({
        type: "session.exited",
        eventId: exitEventId,
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:10.000Z",
        threadId,
        payload: {
          exitKind: "error",
          reason: "provider exited while session/prompt was in flight",
        },
      });
      yield* advanceTestClock(50);
      yield* Fiber.join(exitConsumer);
      yield* Deferred.succeed(releaseSend, undefined);
      const sendFailure = yield* Fiber.join(sendFiber);
      assert.equal(sendFailure._tag, "ProviderSendTurnFailedError");
      if (sendFailure._tag === "ProviderSendTurnFailedError") {
        assert.equal(sendFailure.superseded, false);
        assert.equal(sendFailure.turnId, returnedTurnId);
        assert.match(sendFailure.detail, /provider exited while session\/prompt was in flight/);
      }

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload = binding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly lastError?: unknown;
        readonly lastRuntimeEvent?: unknown;
        readonly sendTurnOperationId?: unknown;
      };
      assert.equal(binding.status, "error");
      assert.equal(runtimePayload.activeTurnId, null);
      assert.equal(runtimePayload.lastRuntimeEvent, "session.exited");
      assert.equal(runtimePayload.lastError, "provider exited while session/prompt was in flight");
      assert.equal(runtimePayload.sendTurnOperationId, null);
    }),
  );

  it.effect("ignores terminal turn events from stale provider bindings", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-terminal-stale-provider");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/project-terminal-stale-provider",
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId,
        input: "hello",
        attachments: [],
      });

      yield* directory.upsert({
        threadId,
        provider: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claudeAgentInstanceId,
        runtimeMode: "full-access",
        status: "running",
        runtimePayload: {
          activeTurnId: turn.turnId,
          lastRuntimeEvent: "provider.replacement",
        },
      });

      const staleProviderEventId = asEventId("evt-stale-provider-turn-completed");
      const staleProviderFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === staleProviderEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      routing.codex.emit({
        type: "turn.completed",
        eventId: staleProviderEventId,
        provider: ProviderDriverKind.make("codex"),
        threadId,
        turnId: turn.turnId,
        createdAt: "2026-01-01T00:00:04.000Z",
        payload: { state: "completed" },
      });
      yield* Fiber.join(staleProviderFiber);

      const binding = yield* directory.getBinding(threadId);
      assert.equal(Option.isSome(binding), true);
      if (Option.isSome(binding)) {
        const runtimePayload = binding.value.runtimePayload as {
          readonly activeTurnId?: unknown;
          readonly lastRuntimeEvent?: unknown;
        };
        assert.equal(binding.value.provider, ProviderDriverKind.make("claudeAgent"));
        assert.equal(binding.value.providerInstanceId, claudeAgentInstanceId);
        assert.equal(runtimePayload.activeTurnId, turn.turnId);
        assert.equal(runtimePayload.lastRuntimeEvent, "provider.replacement");
      }
      yield* routing.codex.stopSession(threadId);
    }),
  );

  it.effect("does not repersist active turn state after synchronous terminal events", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-sync-terminal-turn");
      const turnId = asTurnId("turn-sync-terminal");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/project-sync-terminal-turn",
        runtimeMode: "full-access",
      });

      routing.codex.sendTurn.mockImplementationOnce((input) =>
        Effect.sync(() => {
          routing.codex.emit({
            type: "turn.started",
            eventId: asEventId("evt-sync-terminal-started"),
            provider: ProviderDriverKind.make("codex"),
            threadId: input.threadId,
            turnId,
            createdAt: "2026-01-01T00:00:05.000Z",
            payload: {},
          });
          routing.codex.emit({
            type: "turn.completed",
            eventId: asEventId("evt-sync-terminal-completed"),
            provider: ProviderDriverKind.make("codex"),
            threadId: input.threadId,
            turnId,
            createdAt: "2026-01-01T00:00:06.000Z",
            payload: { state: "completed" },
          });
          return {
            threadId: input.threadId,
            turnId,
          };
        }),
      );

      const terminalFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === asEventId("evt-sync-terminal-completed")),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      const turn = yield* provider.sendTurn({
        threadId,
        input: "hello",
        attachments: [],
      });
      yield* Fiber.join(terminalFiber);

      assert.equal(turn.turnId, turnId);
      const binding = yield* directory.getBinding(threadId);
      assert.equal(Option.isSome(binding), true);
      if (Option.isSome(binding)) {
        const runtimePayload = binding.value.runtimePayload as {
          readonly activeTurnId?: unknown;
          readonly lastRuntimeEvent?: unknown;
          readonly lastTerminalTurnId?: unknown;
        };
        assert.equal(runtimePayload.activeTurnId, null);
        assert.equal(runtimePayload.lastRuntimeEvent, "turn.completed");
        assert.equal(runtimePayload.lastTerminalTurnId, turnId);
      }
      yield* routing.codex.stopSession(threadId);
    }),
  );

  it.effect("dies when an active session conflicts with its persisted binding", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-binding-mismatch");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/project-binding-mismatch",
        runtimeMode: "full-access",
      });
      yield* directory.upsert({
        threadId,
        provider: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claudeAgentInstanceId,
        runtimeMode: "full-access",
      });

      const exit = yield* Effect.exit(provider.listSessions());
      assert.equal(Exit.hasDies(exit), true);
      yield* directory.upsert({
        threadId,
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        runtimeMode: "full-access",
      });
    }),
  );

  it.effect("stops stale sessions in other providers after a successful replacement start", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-provider-replacement");

      const codexSession = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/project-provider-replacement",
        runtimeMode: "full-access",
        detached: true,
      });

      routing.codex.stopSession.mockClear();
      routing.claude.stopSession.mockClear();

      const claudeSession = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claudeAgentInstanceId,
        threadId,
        cwd: "/tmp/project-provider-replacement",
        runtimeMode: "full-access",
      });

      assert.equal(codexSession.provider, "codex");
      assert.equal(claudeSession.provider, "claudeAgent");
      assert.deepEqual(routing.codex.stopSession.mock.calls, [[threadId]]);
      assert.equal(routing.claude.stopSession.mock.calls.length, 0);

      const sessions = yield* provider.listSessions();
      assert.deepEqual(
        sessions
          .filter((session) => session.threadId === threadId)
          .map((session) => session.provider),
        ["claudeAgent"],
      );
      const binding = yield* directory.getBinding(threadId);
      assert.equal(Option.isSome(binding), true);
      if (Option.isSome(binding)) {
        const runtimePayload = binding.value.runtimePayload as {
          readonly detached?: unknown;
        };
        assert.equal(runtimePayload.detached, undefined);
      }
    }),
  );

  it.effect("recovers stale sessions for sendTurn using persisted cwd", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const initial = yield* provider.startSession(asThreadId("thread-1"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-1"),
        cwd: "/tmp/project-send-turn",
        runtimeMode: "full-access",
      });

      yield* routing.codex.stopAll();
      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId: initial.threadId,
        input: "resume",
        attachments: [],
      });

      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      const resumedStartInput = routing.codex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "codex");
        assert.equal(startPayload.cwd, "/tmp/project-send-turn");
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.equal(startPayload.threadId, initial.threadId);
      }
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);
    }),
  );

  it.effect("recovers stale claudeAgent sessions for sendTurn using persisted cwd", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const initial = yield* provider.startSession(asThreadId("thread-claude-send-turn"), {
        provider: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claudeAgentInstanceId,
        threadId: asThreadId("thread-claude-send-turn"),
        cwd: "/tmp/project-claude-send-turn",
        modelSelection: createModelSelection(
          ProviderInstanceId.make("claudeAgent"),
          "claude-opus-4-6",
          [{ id: "effort", value: "max" }],
        ),
        runtimeMode: "full-access",
      });

      yield* routing.claude.stopAll();
      routing.claude.startSession.mockClear();
      routing.claude.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId: initial.threadId,
        input: "resume with claude",
        attachments: [],
      });

      assert.equal(routing.claude.startSession.mock.calls.length, 1);
      const resumedStartInput = routing.claude.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          modelSelection?: unknown;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "claudeAgent");
        assert.equal(startPayload.cwd, "/tmp/project-claude-send-turn");
        assert.deepEqual(
          startPayload.modelSelection,
          createModelSelection(ProviderInstanceId.make("claudeAgent"), "claude-opus-4-6", [
            { id: "effort", value: "max" },
          ]),
        );
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.equal(startPayload.threadId, initial.threadId);
      }
      assert.equal(routing.claude.sendTurn.mock.calls.length, 1);
    }),
  );

  it.effect("lists no sessions after adapter runtime clears", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      yield* provider.startSession(asThreadId("thread-1"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-1"),
        runtimeMode: "full-access",
      });
      yield* provider.startSession(asThreadId("thread-2"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-2"),
        runtimeMode: "full-access",
      });

      yield* routing.codex.stopAll();
      yield* routing.claude.stopAll();

      const remaining = yield* provider.listSessions();
      assert.equal(remaining.length, 0);
    }),
  );

  it.effect("persists runtime status transitions in provider_session_runtime", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

      const threadId = asThreadId("thread-runtime-status");
      const session = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "hello",
        attachments: [],
      });

      const runningRuntime = yield* runtimeRepository.getByThreadId({
        threadId: session.threadId,
      });
      assert.equal(Option.isSome(runningRuntime), true);
      if (Option.isSome(runningRuntime)) {
        assert.equal(runningRuntime.value.status, "running");
        assert.deepEqual(runningRuntime.value.resumeCursor, session.resumeCursor);
        const payload = runningRuntime.value.runtimePayload;
        assert.equal(payload !== null && typeof payload === "object", true);
        if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
          const runtimePayload = payload as {
            cwd: string;
            model: string | null;
            activeTurnId: string | null;
            lastError: string | null;
            lastRuntimeEvent: string | null;
          };
          assert.equal(runtimePayload.cwd, session.cwd);
          assert.equal(runtimePayload.model, null);
          assert.equal(runtimePayload.activeTurnId, `turn-${String(session.threadId)}`);
          assert.equal(runtimePayload.lastError, null);
          assert.equal(runtimePayload.lastRuntimeEvent, "provider.sendTurn");
        }
      }
    }),
  );

  it.effect("persists replacement resume cursor from session state before sendTurn returns", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

      const threadId = asThreadId("thread-replacement-resume-before-send");
      const session = yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      yield* directory.upsert({
        threadId,
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        runtimeMode: "full-access",
        status: "waiting",
        resumeCursor: session.resumeCursor,
        runtimePayload: {
          activeTurnId: asTurnId("turn-replacement-resume-waiting"),
          lastRuntimeEvent: "session.state.changed",
        },
      });
      const replacementResumeCursor = {
        schemaVersion: 1,
        sessionId: "mock-replacement-cursor-session",
      };

      yield* advanceTestClock(50);
      routing.cursor.emit({
        type: "session.state.changed",
        eventId: asEventId("evt-cursor-replacement-resume-ready"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        payload: {
          state: "ready",
          detail: { resumeCursor: replacementResumeCursor },
        },
      });
      yield* advanceTestClock(500);

      const preSendRuntime = yield* runtimeRepository.getByThreadId({ threadId });
      assert.equal(Option.isSome(preSendRuntime), true);
      if (Option.isSome(preSendRuntime)) {
        assert.equal(preSendRuntime.value.status, "running");
        assert.deepEqual(preSendRuntime.value.resumeCursor, replacementResumeCursor);
        const payload = preSendRuntime.value.runtimePayload;
        assert.equal(payload !== null && typeof payload === "object", true);
        if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
          const runtimePayload = payload as {
            activeTurnId: string | null;
            lastRuntimeEvent: string | null;
          };
          assert.equal(runtimePayload.activeTurnId, null);
          assert.equal(runtimePayload.lastRuntimeEvent, "session.state.changed");
        }
      }

      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "follow up on replacement Cursor session",
        attachments: [],
      });

      const runtime = yield* runtimeRepository.getByThreadId({ threadId });
      assert.equal(Option.isSome(runtime), true);
      if (Option.isSome(runtime)) {
        assert.deepEqual(runtime.value.resumeCursor, replacementResumeCursor);
        const payload = runtime.value.runtimePayload;
        assert.equal(payload !== null && typeof payload === "object", true);
        if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
          const runtimePayload = payload as {
            activeTurnId: string | null;
            lastRuntimeEvent: string | null;
          };
          assert.equal(runtimePayload.activeTurnId, `turn-${String(threadId)}`);
          assert.equal(runtimePayload.lastRuntimeEvent, "provider.sendTurn");
        }
      }
    }),
  );

  it.effect("ignores replacement resume cursor state from a different provider binding", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

      const threadId = asThreadId("thread-ignore-other-provider-state-resume");
      const session = yield* provider.startSession(threadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const staleCursorResume = {
        schemaVersion: 1,
        sessionId: "stale-cursor-session",
      };

      routing.cursor.emit({
        type: "session.state.changed",
        eventId: asEventId("evt-stale-cursor-resume-ready"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        payload: {
          state: "ready",
          detail: { resumeCursor: staleCursorResume },
        },
      });
      yield* advanceTestClock(50);

      const runtime = yield* runtimeRepository.getByThreadId({ threadId });
      assert.equal(Option.isSome(runtime), true);
      if (Option.isSome(runtime)) {
        assert.equal(runtime.value.providerName, CODEX_DRIVER);
        assert.equal(runtime.value.providerInstanceId, codexInstanceId);
        assert.deepEqual(runtime.value.resumeCursor, session.resumeCursor);
      }
    }),
  );

  // TODO(ADA-192): re-enable when the provider output-preservation guard lands
  it.effect.skip("isolates runtime state and assistant output across adapter generations", () =>
    Effect.gen(function* () {
      const firstCursor = makeFakeCodexAdapter(CURSOR_DRIVER);
      const secondCursor = makeFakeCodexAdapter(CURSOR_DRIVER);
      let currentAdapter = firstCursor.adapter;
      const changes = yield* PubSub.unbounded<void>();
      const registry: ProviderAdapterRegistry.ProviderAdapterRegistry["Service"] = {
        getByInstance: (instanceId) =>
          instanceId === cursorInstanceId
            ? Effect.succeed(currentAdapter)
            : Effect.fail(
                new ProviderUnsupportedError({
                  provider: ProviderDriverKind.make(instanceId),
                }),
              ),
        getInstanceInfo: (instanceId) =>
          Effect.succeed({
            instanceId,
            driverKind: CURSOR_DRIVER,
            displayName: undefined,
            enabled: true,
            continuationIdentity: {
              driverKind: CURSOR_DRIVER,
              continuationKey: `cursor:instance:${instanceId}`,
            },
          }),
        listInstances: () => Effect.succeed([cursorInstanceId]),
        listProviders: () => Effect.succeed([CURSOR_DRIVER]),
        streamChanges: Stream.fromPubSub(changes),
        subscribeChanges: PubSub.subscribe(changes),
      };
      const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
        Layer.provide(SqlitePersistenceMemory),
      );
      const directoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const providerLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
        Layer.provide(directoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(serverConfigTestLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
        const threadId = asThreadId("thread-ignore-stale-generation-state-resume");
        yield* provider.startSession(threadId, {
          provider: CURSOR_DRIVER,
          providerInstanceId: cursorInstanceId,
          threadId,
          runtimeMode: "full-access",
        });

        const preReplacementTurnId = asTurnId("turn-completed-before-generation-replacement");
        const preReplacementCompletedId = asEventId("evt-completed-before-generation-replacement");
        const preReplacementCompleted = yield* provider.streamEvents.pipe(
          Stream.filter((event) => event.eventId === preReplacementCompletedId),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* advanceTestClock(50);
        firstCursor.emit({
          type: "turn.started",
          eventId: asEventId("evt-started-before-generation-replacement"),
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:00.000Z",
          threadId,
          turnId: preReplacementTurnId,
          payload: {},
        });
        firstCursor.emit({
          type: "turn.completed",
          eventId: preReplacementCompletedId,
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:00.500Z",
          threadId,
          turnId: preReplacementTurnId,
          payload: { state: "completed" },
        });
        yield* advanceTestClock(50);

        currentAdapter = secondCursor.adapter;
        yield* PubSub.publish(changes, undefined);
        yield* advanceTestClock(50);

        const preReplacementEvents = Array.from(yield* Fiber.join(preReplacementCompleted));
        assert.equal(preReplacementEvents[0]?.type, "turn.completed");
        if (preReplacementEvents[0]?.type === "turn.completed") {
          assert.equal(preReplacementEvents[0].payload.state, "failed");
          assert.equal(preReplacementEvents[0].payload.errorMessage, PROVIDER_EMPTY_RESPONSE_ERROR);
        }

        const currentResumeCursor = {
          schemaVersion: 1,
          sessionId: "current-cursor-session",
        };
        secondCursor.emit({
          type: "session.state.changed",
          eventId: asEventId("evt-current-cursor-resume-ready"),
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:01.000Z",
          threadId,
          payload: {
            state: "ready",
            detail: { resumeCursor: currentResumeCursor },
          },
        });
        yield* advanceTestClock(50);

        const staleResumeCursor = {
          schemaVersion: 1,
          sessionId: "stale-cursor-session",
        };
        const staleGenerationEvent: LegacyProviderRuntimeEvent = {
          type: "session.state.changed",
          eventId: asEventId("evt-stale-generation-cursor-resume-ready"),
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:02.000Z",
          threadId,
          payload: {
            state: "ready",
            detail: { resumeCursor: staleResumeCursor },
          },
        };
        firstCursor.emit(staleGenerationEvent);
        yield* advanceTestClock(50);

        assert.equal(
          getCapturedProviderRuntimeEventBinding(
            staleGenerationEvent as unknown as ProviderRuntimeEvent,
          ),
          undefined,
        );

        const runtime = yield* runtimeRepository.getByThreadId({ threadId });
        assert.equal(Option.isSome(runtime), true);
        if (Option.isSome(runtime)) {
          assert.deepEqual(runtime.value.resumeCursor, currentResumeCursor);
        }

        const replacementTurnId = asTurnId("turn-current-generation-output");
        const replacementCompletedId = asEventId("evt-current-generation-completed");
        const replacementCompleted = yield* provider.streamEvents.pipe(
          Stream.filter((event) => event.eventId === replacementCompletedId),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* advanceTestClock(50);
        secondCursor.emit({
          type: "turn.started",
          eventId: asEventId("evt-current-generation-started"),
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:03.000Z",
          threadId,
          turnId: replacementTurnId,
          payload: {},
        });
        secondCursor.emit({
          type: "content.delta",
          eventId: asEventId("evt-current-generation-output"),
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:04.000Z",
          threadId,
          turnId: replacementTurnId,
          itemId: "item-current-generation-output",
          payload: { streamKind: "assistant_text", delta: "replacement output" },
        });
        firstCursor.emit({
          type: "session.exited",
          eventId: asEventId("evt-stale-generation-exited"),
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:05.000Z",
          threadId,
          turnId: replacementTurnId,
          payload: { exitKind: "error", reason: "stale adapter exited late" },
        });
        secondCursor.emit({
          type: "turn.completed",
          eventId: replacementCompletedId,
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:06.000Z",
          threadId,
          turnId: replacementTurnId,
          payload: { state: "completed" },
        });
        const replacementEvents = Array.from(yield* Fiber.join(replacementCompleted));
        assert.equal(replacementEvents[0]?.type, "turn.completed");
        if (replacementEvents[0]?.type === "turn.completed") {
          assert.equal(replacementEvents[0].payload.state, "completed");
        }

        const emptyTurnId = asTurnId("turn-current-generation-empty");
        const emptyCompletedId = asEventId("evt-current-generation-empty-completed");
        const emptyCompleted = yield* provider.streamEvents.pipe(
          Stream.filter((event) => event.eventId === emptyCompletedId),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* advanceTestClock(50);
        secondCursor.emit({
          type: "turn.started",
          eventId: asEventId("evt-current-generation-empty-started"),
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:07.000Z",
          threadId,
          turnId: emptyTurnId,
          payload: {},
        });
        const staleGenerationOutput: LegacyProviderRuntimeEvent = {
          type: "content.delta",
          eventId: asEventId("evt-stale-generation-output"),
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:08.000Z",
          threadId,
          turnId: emptyTurnId,
          itemId: "item-stale-generation-output",
          payload: { streamKind: "assistant_text", delta: "stale output" },
        };
        firstCursor.emit(staleGenerationOutput);
        yield* advanceTestClock(50);
        assert.equal(
          getCapturedProviderRuntimeEventBinding(
            staleGenerationOutput as unknown as ProviderRuntimeEvent,
          ),
          undefined,
        );
        secondCursor.emit({
          type: "turn.completed",
          eventId: emptyCompletedId,
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:09.000Z",
          threadId,
          turnId: emptyTurnId,
          payload: { state: "completed" },
        });
        yield* advanceTestClock(200);
        const emptyEvents = Array.from(yield* Fiber.join(emptyCompleted));
        assert.equal(emptyEvents[0]?.type, "turn.completed");
        if (emptyEvents[0]?.type === "turn.completed") {
          assert.equal(emptyEvents[0].payload.state, "failed");
          assert.equal(emptyEvents[0].payload.errorMessage, PROVIDER_EMPTY_RESPONSE_ERROR);
        }
      }).pipe(Effect.provide(Layer.mergeAll(providerLayer, runtimeRepositoryLayer)));
    }),
  );

  it.effect("reuses persisted resume cursor and active turn when startSession restarts", () =>
    Effect.gen(function* () {
      const tempDir = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "t3-provider-service-start-"),
      );
      const dbPath = NodePath.join(tempDir, "orchestration.sqlite");
      const persistenceLayer = makeSqlitePersistenceLive(dbPath);
      const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
        Layer.provide(persistenceLayer),
      );

      const firstCursor = makeFakeCodexAdapter(CURSOR_DRIVER);
      const firstRegistry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("cursor")]: firstCursor.adapter,
      });
      const firstDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const firstProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(
          Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, firstRegistry),
        ),
        Layer.provide(firstDirectoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(serverConfigTestLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );

      const initial = yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        return yield* provider.startSession(asThreadId("thread-cursor-start"), {
          provider: ProviderDriverKind.make("cursor"),
          providerInstanceId: cursorInstanceId,
          threadId: asThreadId("thread-cursor-start"),
          cwd: "/tmp/project-cursor-start",
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(firstProviderLayer));

      const persistedActiveTurnId = asTurnId(`turn-${String(initial.threadId)}`);
      yield* Effect.gen(function* () {
        const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
        yield* directory.upsert({
          threadId: initial.threadId,
          provider: ProviderDriverKind.make("cursor"),
          providerInstanceId: cursorInstanceId,
          runtimeMode: "full-access",
          status: "running",
          resumeCursor: initial.resumeCursor,
          runtimePayload: {
            cwd: "/tmp/project-cursor-start",
            activeTurnId: persistedActiveTurnId,
          },
        });
      }).pipe(Effect.provide(firstDirectoryLayer));

      const secondCursor = makeFakeCodexAdapter(CURSOR_DRIVER);
      const secondRegistry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("cursor")]: secondCursor.adapter,
      });
      const secondDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const secondProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(
          Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, secondRegistry),
        ),
        Layer.provide(secondDirectoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(serverConfigTestLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );

      secondCursor.startSession.mockClear();

      const resumedResult = yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const resumed = yield* provider.startSession(initial.threadId, {
          provider: ProviderDriverKind.make("cursor"),
          providerInstanceId: cursorInstanceId,
          threadId: initial.threadId,
          cwd: "/tmp/project-cursor-start",
          runtimeMode: "full-access",
        });
        const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
        const binding = yield* directory.getBinding(initial.threadId);
        const sessions = yield* provider.listSessions();
        return { binding, resumed, sessions };
      }).pipe(Effect.provide(secondProviderLayer));

      assert.equal(secondCursor.startSession.mock.calls.length, 1);
      const resumedStartInput = secondCursor.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          resumeCursor?: unknown;
          threadId?: string;
          activeTurnId?: string;
        };
        assert.equal(startPayload.provider, "cursor");
        assert.equal(startPayload.cwd, "/tmp/project-cursor-start");
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.equal(startPayload.threadId, initial.threadId);
        assert.equal(startPayload.activeTurnId, persistedActiveTurnId);
      }
      assert.equal(resumedResult.resumed.activeTurnId, persistedActiveTurnId);
      assert.equal(resumedResult.resumed.status, "running");
      assert.equal(
        resumedResult.sessions.find((session) => session.threadId === initial.threadId)
          ?.activeTurnId,
        persistedActiveTurnId,
      );
      assert.equal(
        resumedResult.sessions.find((session) => session.threadId === initial.threadId)?.status,
        "running",
      );
      assert.equal(Option.isSome(resumedResult.binding), true);
      if (Option.isSome(resumedResult.binding)) {
        const runtimePayload = resumedResult.binding.value.runtimePayload as {
          readonly activeTurnId?: unknown;
        };
        assert.equal(runtimePayload.activeTurnId, persistedActiveTurnId);
        assert.equal(resumedResult.binding.value.status, "running");
      }

      const replacementCursorAdapter = makeFakeCodexAdapter(CURSOR_DRIVER);
      const replacementRegistry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("cursor")]: replacementCursorAdapter.adapter,
      });
      const replacementProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(
          Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, replacementRegistry),
        ),
        Layer.provide(secondDirectoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(serverConfigTestLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );
      const replacementCursor = { opaque: "replacement-cursor" };

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        yield* provider.startSession(initial.threadId, {
          provider: ProviderDriverKind.make("cursor"),
          providerInstanceId: cursorInstanceId,
          threadId: initial.threadId,
          cwd: "/tmp/project-cursor-start",
          resumeCursor: replacementCursor,
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(replacementProviderLayer));

      assert.equal(replacementCursorAdapter.startSession.mock.calls.length, 1);
      const replacementStartInput = replacementCursorAdapter.startSession.mock.calls[0]?.[0];
      assert.equal(
        typeof replacementStartInput === "object" && replacementStartInput !== null,
        true,
      );
      if (replacementStartInput && typeof replacementStartInput === "object") {
        const startPayload = replacementStartInput as {
          resumeCursor?: unknown;
          activeTurnId?: string;
        };
        assert.deepEqual(startPayload.resumeCursor, replacementCursor);
        assert.equal(startPayload.activeTurnId, undefined);
      }

      NodeFS.rmSync(tempDir, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("preserves a recovered active turn completion after service restart", () =>
    Effect.gen(function* () {
      const tempDir = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "t3-provider-service-output-restart-"),
      );
      const dbPath = NodePath.join(tempDir, "orchestration.sqlite");
      const persistenceLayer = makeSqlitePersistenceLive(dbPath);
      const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
        Layer.provide(persistenceLayer),
      );

      const firstCursor = makeFakeCodexAdapter(CURSOR_DRIVER);
      const firstRegistry = makeAdapterRegistryMock({
        [CURSOR_DRIVER]: firstCursor.adapter,
      });
      const firstDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const firstProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(
          Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, firstRegistry),
        ),
        Layer.provide(firstDirectoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(serverConfigTestLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );
      const threadId = asThreadId("thread-cursor-output-restart");
      const outputEventId = asEventId("evt-cursor-output-before-restart");
      const turnId = yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        yield* provider.startSession(threadId, {
          provider: CURSOR_DRIVER,
          providerInstanceId: cursorInstanceId,
          threadId,
          runtimeMode: "full-access",
        });
        const turn = yield* provider.sendTurn({
          threadId,
          input: "continue",
          attachments: [],
        });
        const observedOutput = yield* provider.streamEvents.pipe(
          Stream.filter((event) => event.eventId === outputEventId),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* advanceTestClock(50);
        firstCursor.emit({
          type: "content.delta",
          eventId: outputEventId,
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:01.000Z",
          threadId,
          turnId: turn.turnId,
          itemId: "item-cursor-output-before-restart",
          payload: {
            streamKind: "assistant_text",
            delta: "persisted before restart",
          },
        });
        yield* Fiber.join(observedOutput);
        return turn.turnId;
      }).pipe(Effect.provide(firstProviderLayer));

      yield* Effect.gen(function* () {
        const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
        const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
        yield* directory.upsert({
          ...binding,
          status: "running",
          runtimePayload: {
            ...(binding.runtimePayload as Record<string, unknown>),
            activeTurnId: turnId,
          },
        });
      }).pipe(Effect.provide(firstDirectoryLayer));

      const secondCursor = makeFakeCodexAdapter(CURSOR_DRIVER);
      const recoveryStartEntered = yield* Deferred.make<void>();
      const releaseRecoveryStart = yield* Deferred.make<void>();
      const defaultRecoveryStart = secondCursor.startSession.getMockImplementation();
      assert.ok(defaultRecoveryStart);
      secondCursor.startSession.mockImplementationOnce((input) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(recoveryStartEntered, undefined);
          yield* Deferred.await(releaseRecoveryStart);
          return yield* defaultRecoveryStart(input);
        }),
      );
      const secondRegistry = makeAdapterRegistryMock({
        [CURSOR_DRIVER]: secondCursor.adapter,
      });
      const secondDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const secondProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(
          Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, secondRegistry),
        ),
        Layer.provide(secondDirectoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(serverConfigTestLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );

      const completedEventId = asEventId("evt-cursor-output-after-restart-completed");
      const completion = yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const completed = yield* provider.streamEvents.pipe(
          Stream.filter((event) => event.eventId === completedEventId),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* advanceTestClock(50);
        const recovery = yield* provider.interruptTurn({ threadId, turnId }).pipe(Effect.forkChild);
        yield* Deferred.await(recoveryStartEntered);
        secondCursor.emit({
          type: "turn.completed",
          eventId: completedEventId,
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:02.000Z",
          threadId,
          turnId,
          payload: { state: "completed", stopReason: "end_turn" },
        });
        yield* advanceTestClock(200);
        assert.equal(completed.pollUnsafe(), undefined);
        yield* Deferred.succeed(releaseRecoveryStart, undefined);
        yield* Fiber.join(recovery);
        return Array.from(yield* Fiber.join(completed));
      }).pipe(Effect.provide(secondProviderLayer));

      assert.equal(completion[0]?.type, "turn.completed");
      if (completion[0]?.type === "turn.completed") {
        assert.equal(completion[0].payload.state, "completed");
        assert.equal(completion[0].payload.errorMessage, undefined);
      }
      NodeFS.rmSync(tempDir, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("does not resume persisted active turns for adapters without active-turn resume", () =>
    Effect.gen(function* () {
      const tempDir = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "t3-provider-service-unsupported-active-"),
      );
      const dbPath = NodePath.join(tempDir, "orchestration.sqlite");
      const persistenceLayer = makeSqlitePersistenceLive(dbPath);
      const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
        Layer.provide(persistenceLayer),
      );
      const directoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const claude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
      const registry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("claudeAgent")]: claude.adapter,
      });
      const providerLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
        Layer.provide(directoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(serverConfigTestLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );
      const threadId = asThreadId("thread-unsupported-active-resume");
      const resumeCursor = { opaque: "resume-unsupported-active" };
      const persistedActiveTurnId = asTurnId("turn-unsupported-active");

      yield* Effect.gen(function* () {
        const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
        yield* directory.upsert({
          threadId,
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: claudeAgentInstanceId,
          runtimeMode: "full-access",
          status: "running",
          resumeCursor,
          runtimePayload: {
            cwd: "/tmp/project-unsupported-active",
            activeTurnId: persistedActiveTurnId,
          },
        });
      }).pipe(Effect.provide(directoryLayer));

      const resumedResult = yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const resumed = yield* provider.startSession(threadId, {
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: claudeAgentInstanceId,
          threadId,
          cwd: "/tmp/project-unsupported-active",
          runtimeMode: "full-access",
        });
        const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
        const binding = yield* directory.getBinding(threadId);
        const sessions = yield* provider.listSessions();
        return { binding, resumed, sessions };
      }).pipe(Effect.provide(providerLayer));

      assert.equal(claude.startSession.mock.calls.length, 1);
      const startInput = claude.startSession.mock.calls[0]?.[0];
      assert.equal(typeof startInput === "object" && startInput !== null, true);
      if (startInput && typeof startInput === "object") {
        const startPayload = startInput as {
          readonly activeTurnId?: unknown;
          readonly resumeCursor?: unknown;
        };
        assert.deepEqual(startPayload.resumeCursor, resumeCursor);
        assert.equal(startPayload.activeTurnId, undefined);
      }
      assert.equal(resumedResult.resumed.status, "ready");
      assert.equal(resumedResult.resumed.activeTurnId, undefined);
      const listed = resumedResult.sessions.find((session) => session.threadId === threadId);
      assert.equal(listed?.status, "ready");
      assert.equal(listed?.activeTurnId, undefined);
      assert.equal(Option.isSome(resumedResult.binding), true);
      if (Option.isSome(resumedResult.binding)) {
        const runtimePayload = resumedResult.binding.value.runtimePayload as {
          readonly activeTurnId?: unknown;
        };
        assert.equal(runtimePayload.activeTurnId, null);
      }

      NodeFS.rmSync(tempDir, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("does not resurrect persisted active turns over ready live sessions", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-live-ready-stale-active");
      const session = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("cursor"),
        providerInstanceId: cursorInstanceId,
        threadId,
        cwd: "/tmp/project-live-ready-stale-active",
        runtimeMode: "full-access",
      });

      yield* directory.upsert({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        providerInstanceId: cursorInstanceId,
        runtimeMode: "full-access",
        status: "running",
        resumeCursor: session.resumeCursor,
        runtimePayload: {
          cwd: "/tmp/project-live-ready-stale-active",
          activeTurnId: asTurnId("turn-stale-persisted-active"),
        },
      });

      const sessions = yield* provider.listSessions();
      const listed = sessions.find((candidate) => candidate.threadId === threadId);
      assert.equal(listed?.status, "ready");
      assert.equal(listed?.activeTurnId, undefined);
    }),
  );

  it.effect(
    "reuses persisted cwd when startSession resumes a claude session without cwd input",
    () =>
      Effect.gen(function* () {
        const tempDir = NodeFS.mkdtempSync(
          NodePath.join(NodeOS.tmpdir(), "t3-provider-service-cwd-"),
        );
        const dbPath = NodePath.join(tempDir, "orchestration.sqlite");
        const persistenceLayer = makeSqlitePersistenceLive(dbPath);
        const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
          Layer.provide(persistenceLayer),
        );

        const firstClaude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
        const firstRegistry = makeAdapterRegistryMock({
          [ProviderDriverKind.make("claudeAgent")]: firstClaude.adapter,
        });
        const firstDirectoryLayer = ProviderSessionDirectoryLive.pipe(
          Layer.provide(runtimeRepositoryLayer),
        );
        const firstProviderLayer = makeProviderServiceLive().pipe(
          Layer.provide(
            Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, firstRegistry),
          ),
          Layer.provide(firstDirectoryLayer),
          Layer.provide(defaultServerSettingsLayer),
          Layer.provide(serverConfigTestLayer),
          Layer.provide(AnalyticsService.layerTest),
          Layer.provide(
            Layer.succeed(
              ProviderEventLoggers.ProviderEventLoggers,
              ProviderEventLoggers.NoOpProviderEventLoggers,
            ),
          ),
        );

        const initial = yield* Effect.gen(function* () {
          const provider = yield* ProviderService.ProviderService;
          return yield* provider.startSession(asThreadId("thread-claude-cwd"), {
            provider: ProviderDriverKind.make("claudeAgent"),
            providerInstanceId: claudeAgentInstanceId,
            threadId: asThreadId("thread-claude-cwd"),
            cwd: "/tmp/project-claude-cwd",
            runtimeMode: "full-access",
          });
        }).pipe(Effect.provide(firstProviderLayer));

        const secondClaude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
        const secondRegistry = makeAdapterRegistryMock({
          [ProviderDriverKind.make("claudeAgent")]: secondClaude.adapter,
        });
        const secondDirectoryLayer = ProviderSessionDirectoryLive.pipe(
          Layer.provide(runtimeRepositoryLayer),
        );
        const secondProviderLayer = makeProviderServiceLive().pipe(
          Layer.provide(
            Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, secondRegistry),
          ),
          Layer.provide(secondDirectoryLayer),
          Layer.provide(defaultServerSettingsLayer),
          Layer.provide(serverConfigTestLayer),
          Layer.provide(AnalyticsService.layerTest),
          Layer.provide(
            Layer.succeed(
              ProviderEventLoggers.ProviderEventLoggers,
              ProviderEventLoggers.NoOpProviderEventLoggers,
            ),
          ),
        );

        secondClaude.startSession.mockClear();

        yield* Effect.gen(function* () {
          const provider = yield* ProviderService.ProviderService;
          yield* provider.startSession(initial.threadId, {
            provider: ProviderDriverKind.make("claudeAgent"),
            providerInstanceId: claudeAgentInstanceId,
            threadId: initial.threadId,
            runtimeMode: "full-access",
          });
        }).pipe(Effect.provide(secondProviderLayer));

        assert.equal(secondClaude.startSession.mock.calls.length, 1);
        const resumedStartInput = secondClaude.startSession.mock.calls[0]?.[0];
        assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
        if (resumedStartInput && typeof resumedStartInput === "object") {
          const startPayload = resumedStartInput as {
            provider?: string;
            cwd?: string;
            resumeCursor?: unknown;
            threadId?: string;
          };
          assert.equal(startPayload.provider, "claudeAgent");
          assert.equal(startPayload.cwd, "/tmp/project-claude-cwd");
          assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
          assert.equal(startPayload.threadId, initial.threadId);
        }

        NodeFS.rmSync(tempDir, { recursive: true, force: true });
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});

const fanout = makeProviderServiceLayer();
const grokOutputAdapter = makeFakeCodexAdapter(GROK_DRIVER);
const grokFanout = makeProviderServiceLayer(undefined, {
  [GROK_DRIVER]: grokOutputAdapter.adapter,
});
it.effect("ProviderServiceLive clears MCP credentials when a provider adapter is replaced", () =>
  Effect.gen(function* () {
    const firstCodex = makeFakeCodexAdapter();
    const secondCodex = makeFakeCodexAdapter();
    let currentAdapter = firstCodex.adapter;
    const changes = yield* PubSub.unbounded<void>();
    const registry: ProviderAdapterRegistry.ProviderAdapterRegistry["Service"] = {
      getByInstance: (instanceId) =>
        instanceId === codexInstanceId
          ? Effect.succeed(currentAdapter)
          : Effect.fail(
              new ProviderUnsupportedError({
                provider: ProviderDriverKind.make(instanceId),
              }),
            ),
      getInstanceInfo: (instanceId) =>
        Effect.succeed({
          instanceId,
          driverKind: CODEX_DRIVER,
          displayName: undefined,
          enabled: true,
          continuationIdentity: {
            driverKind: CODEX_DRIVER,
            continuationKey: `codex:instance:${instanceId}`,
          },
        }),
      listInstances: () => Effect.succeed([codexInstanceId]),
      listProviders: () => Effect.succeed([CODEX_DRIVER]),
      streamChanges: Stream.fromPubSub(changes),
      subscribeChanges: PubSub.subscribe(changes),
    };
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
      Layer.provide(directoryLayer),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(serverConfigTestLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );

    yield* Effect.gen(function* () {
      const threadId = asThreadId("thread-mcp-adapter-replaced");
      const provider = yield* ProviderService.ProviderService;
      McpProviderSession.setMcpProviderSession({
        environmentId,
        threadId,
        providerSessionId: "provider-session-adapter-replaced",
        providerInstanceId: codexInstanceId,
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer adapter-replaced-token",
      });
      const session = yield* provider.startSession(threadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      currentAdapter = secondCodex.adapter;
      yield* PubSub.publish(changes, undefined);
      yield* advanceTestClock(50);

      assert.equal(McpProviderSession.readMcpProviderSession(session.threadId), undefined);
    }).pipe(
      Effect.ensuring(
        Effect.sync(() =>
          McpProviderSession.clearMcpProviderSession(asThreadId("thread-mcp-adapter-replaced")),
        ),
      ),
      Effect.provide(
        Layer.mergeAll(providerLayer, directoryLayer, runtimeRepositoryLayer, NodeServices.layer),
      ),
    );
  }),
);

it.effect(
  "ProviderServiceLive clears MCP credentials when an adapter is replaced during start",
  () =>
    Effect.gen(function* () {
      const firstCodex = makeFakeCodexAdapter();
      const secondCodex = makeFakeCodexAdapter();
      const startEntered = yield* Deferred.make<void>();
      const releaseStart = yield* Deferred.make<void>();
      firstCodex.startSession.mockImplementation((input: ProviderSessionStartInput) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(startEntered, undefined);
          yield* Deferred.await(releaseStart);
          const now = "2026-01-01T00:00:00.000Z";
          const session: ProviderSession = {
            provider: CODEX_DRIVER,
            ...(input.providerInstanceId !== undefined
              ? { providerInstanceId: input.providerInstanceId }
              : {}),
            status: "ready",
            runtimeMode: input.runtimeMode,
            threadId: input.threadId,
            resumeCursor: input.resumeCursor ?? {
              opaque: `resume-${String(input.threadId)}`,
            },
            cwd: input.cwd ?? process.cwd(),
            createdAt: now,
            updatedAt: now,
          };
          firstCodex.sessions.set(session.threadId, session);
          return session;
        }),
      );

      let currentAdapter = firstCodex.adapter;
      const changes = yield* PubSub.unbounded<void>();
      const registry: ProviderAdapterRegistry.ProviderAdapterRegistry["Service"] = {
        getByInstance: (instanceId) =>
          instanceId === codexInstanceId
            ? Effect.succeed(currentAdapter)
            : Effect.fail(
                new ProviderUnsupportedError({
                  provider: ProviderDriverKind.make(instanceId),
                }),
              ),
        getInstanceInfo: (instanceId) =>
          Effect.succeed({
            instanceId,
            driverKind: CODEX_DRIVER,
            displayName: undefined,
            enabled: true,
            continuationIdentity: {
              driverKind: CODEX_DRIVER,
              continuationKey: `codex:instance:${instanceId}`,
            },
          }),
        listInstances: () => Effect.succeed([codexInstanceId]),
        listProviders: () => Effect.succeed([CODEX_DRIVER]),
        streamChanges: Stream.fromPubSub(changes),
        subscribeChanges: PubSub.subscribe(changes),
      };
      const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
        Layer.provide(SqlitePersistenceMemory),
      );
      const directoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const providerLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
        Layer.provide(directoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(serverConfigTestLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );

      yield* Effect.gen(function* () {
        const threadId = asThreadId("thread-mcp-adapter-replaced-during-start");
        const provider = yield* ProviderService.ProviderService;
        McpProviderSession.setMcpProviderSession({
          environmentId,
          threadId,
          providerSessionId: "provider-session-adapter-replaced-during-start-old",
          providerInstanceId: codexInstanceId,
          endpoint: "http://127.0.0.1:43123/mcp",
          authorizationHeader: "Bearer adapter-replaced-during-start-old-token",
        });

        const startFiber = yield* provider
          .startSession(threadId, {
            provider: CODEX_DRIVER,
            providerInstanceId: codexInstanceId,
            threadId,
            runtimeMode: "full-access",
          })
          .pipe(Effect.exit, Effect.forkScoped);
        yield* Deferred.await(startEntered);

        currentAdapter = secondCodex.adapter;
        yield* PubSub.publish(changes, undefined);
        yield* advanceTestClock(50);
        McpProviderSession.setMcpProviderSession({
          environmentId,
          threadId,
          providerSessionId: "provider-session-adapter-replaced-during-start-replacement",
          providerInstanceId: codexInstanceId,
          endpoint: "http://127.0.0.1:43123/mcp",
          authorizationHeader: "Bearer adapter-replaced-during-start-replacement-token",
        });
        const replacementFiber = yield* provider
          .startSession(threadId, {
            provider: CODEX_DRIVER,
            providerInstanceId: codexInstanceId,
            threadId,
            runtimeMode: "full-access",
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.succeed(releaseStart, undefined);

        const exit = yield* Fiber.join(startFiber);
        const replacementSession = yield* Fiber.join(replacementFiber);
        assert.equal(replacementSession.provider, CODEX_DRIVER);
        assert.equal(Exit.isFailure(exit), true);
        assert.deepEqual(firstCodex.stopSession.mock.calls, [[threadId]]);
        assert.equal(
          McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId,
          "provider-session-adapter-replaced-during-start-replacement",
        );
      }).pipe(
        Effect.ensuring(
          Effect.sync(() =>
            McpProviderSession.clearMcpProviderSession(
              asThreadId("thread-mcp-adapter-replaced-during-start"),
            ),
          ),
        ),
        Effect.provide(
          Layer.mergeAll(providerLayer, directoryLayer, runtimeRepositoryLayer, NodeServices.layer),
        ),
      );
    }),
);

it.effect("drains Cursor completions created while flushing a replaced adapter", () =>
  Effect.gen(function* () {
    const firstCursor = makeFakeCodexAdapter(CURSOR_DRIVER);
    const secondCursor = makeFakeCodexAdapter(CURSOR_DRIVER);
    let currentAdapter = firstCursor.adapter;
    const changes = yield* PubSub.unbounded<void>();
    const registry: ProviderAdapterRegistry.ProviderAdapterRegistry["Service"] = {
      getByInstance: (instanceId) =>
        instanceId === cursorInstanceId
          ? Effect.succeed(currentAdapter)
          : Effect.fail(
              new ProviderUnsupportedError({
                provider: ProviderDriverKind.make(instanceId),
              }),
            ),
      getInstanceInfo: (instanceId) =>
        Effect.succeed({
          instanceId,
          driverKind: CURSOR_DRIVER,
          displayName: undefined,
          enabled: true,
          continuationIdentity: {
            driverKind: CURSOR_DRIVER,
            continuationKey: `cursor:instance:${instanceId}`,
          },
        }),
      listInstances: () => Effect.succeed([cursorInstanceId]),
      listProviders: () => Effect.succeed([CURSOR_DRIVER]),
      streamChanges: Stream.fromPubSub(changes),
      subscribeChanges: PubSub.subscribe(changes),
    };
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
      Layer.provide(directoryLayer),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(serverConfigTestLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );

    yield* Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-cursor-replaced-flush");
      const firstTurnId = asTurnId("turn-cursor-replaced-flush-first");
      const replacementTurnId = asTurnId("turn-cursor-replaced-flush-replacement");
      const firstCompletedEventId = asEventId("evt-cursor-replaced-flush-first-completed");
      const replacementCompletedEventId = asEventId(
        "evt-cursor-replaced-flush-replacement-completed",
      );
      const completedEvents: Array<ProviderRuntimeEvent> = [];

      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      yield* provider.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.eventId === firstCompletedEventId ||
            event.eventId === replacementCompletedEventId,
        ),
        Stream.runForEach((event) =>
          Effect.sync(() => {
            completedEvents.push(event);
          }),
        ),
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      firstCursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-replaced-flush-first-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId: firstTurnId,
        payload: {},
      });
      firstCursor.emit({
        type: "turn.completed",
        eventId: firstCompletedEventId,
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId: firstTurnId,
        payload: { state: "completed" },
      });
      firstCursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-replaced-flush-replacement-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        turnId: replacementTurnId,
        payload: {},
      });
      firstCursor.emit({
        type: "turn.completed",
        eventId: replacementCompletedEventId,
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:03.000Z",
        threadId,
        turnId: replacementTurnId,
        payload: { state: "completed" },
      });
      yield* advanceTestClock(50);

      currentAdapter = secondCursor.adapter;
      yield* PubSub.publish(changes, undefined);
      yield* advanceTestClock(50);
      yield* advanceTestClock(200);

      assert.deepEqual(
        completedEvents.map((event) => event.eventId),
        [firstCompletedEventId, replacementCompletedEventId],
      );
      const replacementCompletion = completedEvents[1];
      assert.equal(replacementCompletion?.type, "turn.completed");
      if (replacementCompletion?.type === "turn.completed") {
        assert.equal(replacementCompletion.payload.state, "failed");
        assert.equal(replacementCompletion.payload.errorMessage, PROVIDER_EMPTY_RESPONSE_ERROR);
      }
    }).pipe(
      Effect.provide(
        Layer.mergeAll(providerLayer, directoryLayer, runtimeRepositoryLayer, NodeServices.layer),
      ),
    );
  }),
);

it.effect("cancels a pending Cursor completion when ProviderService is released", () => {
  const cursor = makeFakeCodexAdapter(CURSOR_DRIVER);
  const registry = makeAdapterRegistryMock({
    [CURSOR_DRIVER]: cursor.adapter,
  });
  const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
    Layer.provide(SqlitePersistenceMemory),
  );
  const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));

  return Effect.gen(function* () {
    const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
      Layer.provide(Layer.succeed(ProviderSessionDirectory.ProviderSessionDirectory, directory)),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(serverConfigTestLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );
    const threadId = asThreadId("thread-cursor-released-grace");
    const staleTurnId = asTurnId("turn-cursor-released-grace-stale");
    const providerScope = yield* Scope.make();
    const providerServices = yield* Layer.build(providerLayer).pipe(Scope.provide(providerScope));
    yield* advanceTestClock(50);

    yield* Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      cursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-released-grace-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId: staleTurnId,
        payload: {},
      });
      cursor.emit({
        type: "turn.completed",
        eventId: asEventId("evt-cursor-released-grace-completed"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId: staleTurnId,
        payload: { state: "completed" },
      });
      yield* advanceTestClock(50);
    }).pipe(Effect.provide(providerServices));

    yield* Scope.close(providerScope, Exit.void);

    const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
    yield* directory.upsert({
      ...binding,
      status: "running",
      runtimePayload: {
        ...(binding.runtimePayload as Record<string, unknown>),
        // A replacement service resumes the same provider turn. Its output
        // state is service-local, so the released service's old grace timer
        // must not classify that still-active turn from a stale snapshot.
        activeTurnId: staleTurnId,
        lastError: null,
      },
    });
    yield* advanceTestClock(200);

    const afterGrace = Option.getOrThrow(yield* directory.getBinding(threadId));
    const runtimePayload = afterGrace.runtimePayload as {
      readonly activeTurnId?: unknown;
      readonly lastError?: unknown;
    };
    assert.equal(runtimePayload.activeTurnId, staleTurnId);
    assert.equal(runtimePayload.lastError, null);
    assert.equal(afterGrace.status, "running");
  }).pipe(
    Effect.provide(Layer.mergeAll(directoryLayer, runtimeRepositoryLayer, NodeServices.layer)),
  );
});

it.effect("orders Cursor completion grace behind pending sendTurn ownership", () => {
  const cursor = makeFakeCodexAdapter(CURSOR_DRIVER);
  const registry = makeAdapterRegistryMock({
    [CURSOR_DRIVER]: cursor.adapter,
  });
  const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
    Layer.provide(SqlitePersistenceMemory),
  );
  const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));

  return Effect.gen(function* () {
    const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
    const ownershipReadEntered = yield* Deferred.make<void>();
    const releaseOwnershipRead = yield* Deferred.make<void>();
    const releaseSend = yield* Deferred.make<void>();
    const threadId = asThreadId("thread-cursor-pending-send-grace");
    const turnId = asTurnId("turn-cursor-pending-send-grace");
    const completedEventId = asEventId("evt-cursor-pending-send-grace-completed");
    let blockNextBindingRead = false;
    let bindingReadBlocked = false;
    const orderedDirectory: ProviderSessionDirectory.ProviderSessionDirectory["Service"] = {
      ...directory,
      getBinding: (requestedThreadId) => {
        if (requestedThreadId !== threadId || !blockNextBindingRead || bindingReadBlocked) {
          return directory.getBinding(requestedThreadId);
        }
        bindingReadBlocked = true;
        return Effect.gen(function* () {
          const staleBinding = yield* directory.getBinding(requestedThreadId);
          yield* Deferred.succeed(ownershipReadEntered, undefined);
          yield* Deferred.await(releaseOwnershipRead);
          return staleBinding;
        });
      },
    };
    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
      Layer.provide(
        Layer.succeed(ProviderSessionDirectory.ProviderSessionDirectory, orderedDirectory),
      ),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(serverConfigTestLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );

    yield* Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const completed = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === completedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      cursor.sendTurn.mockImplementationOnce(() =>
        Effect.gen(function* () {
          blockNextBindingRead = true;
          cursor.emit({
            type: "turn.completed",
            eventId: completedEventId,
            provider: CURSOR_DRIVER,
            createdAt: "2026-01-01T00:00:01.000Z",
            threadId,
            turnId,
            payload: { state: "completed" },
          });
          yield* Deferred.await(releaseSend);
          return { threadId, turnId };
        }),
      );
      const send = yield* provider
        .sendTurn({
          threadId,
          input: "respond while completion races the send result",
          attachments: [],
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(ownershipReadEntered);
      yield* Deferred.succeed(releaseSend, undefined);

      // Without the barrier fix, sendTurn can persist ownership while this
      // deliberately stale read is paused. With the decision inside the lock
      // it cannot, so this bounded loop intentionally accepts either schedule.
      for (let attempt = 0; attempt < 20; attempt += 1) {
        const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
        const activeTurnId = (binding.runtimePayload as { readonly activeTurnId?: unknown })
          .activeTurnId;
        if (activeTurnId === turnId) break;
        yield* Effect.yieldNow;
      }
      yield* Deferred.succeed(releaseOwnershipRead, undefined);
      yield* Fiber.join(send);

      cursor.emit({
        type: "content.delta",
        eventId: asEventId("evt-cursor-pending-send-grace-late-delta"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        turnId,
        itemId: "item-cursor-pending-send-grace-late-delta",
        payload: { streamKind: "assistant_text", delta: "late visible output" },
      });
      yield* advanceTestClock(200);

      const events = Array.from(yield* Fiber.join(completed));
      assert.equal(events[0]?.type, "turn.completed");
      if (events[0]?.type === "turn.completed") {
        assert.equal(events[0].payload.state, "completed");
        assert.equal(events[0].payload.errorMessage, undefined);
      }
    }).pipe(Effect.provide(providerLayer));
  }).pipe(
    Effect.provide(Layer.mergeAll(directoryLayer, runtimeRepositoryLayer, NodeServices.layer)),
  );
});

it.effect("retries terminal ownership without dropping provider failures", () => {
  const codex = makeFakeCodexAdapter(CODEX_DRIVER);
  const registry = makeAdapterRegistryMock({
    [CODEX_DRIVER]: codex.adapter,
  });
  const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
    Layer.provide(SqlitePersistenceMemory),
  );
  const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));

  return Effect.gen(function* () {
    const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
    const threadId = asThreadId("thread-terminal-ownership-transient-read");
    const bindingReadFailuresRemaining = new Map<string, number>();
    let failedOwnershipReads = 0;
    const flakyDirectory: ProviderSessionDirectory.ProviderSessionDirectory["Service"] = {
      ...directory,
      getBinding: (requestedThreadId) => {
        const remainingFailures = bindingReadFailuresRemaining.get(requestedThreadId) ?? 0;
        if (remainingFailures > 0) {
          bindingReadFailuresRemaining.set(requestedThreadId, remainingFailures - 1);
          failedOwnershipReads += 1;
          return Effect.fail(
            new ProviderSessionDirectoryPersistenceError({
              operation: "getBinding",
              detail: "transient ownership read failure",
            }),
          );
        }
        return directory.getBinding(requestedThreadId);
      },
    };
    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
      Layer.provide(
        Layer.succeed(ProviderSessionDirectory.ProviderSessionDirectory, flakyDirectory),
      ),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(serverConfigTestLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );

    yield* Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      yield* provider.startSession(threadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId,
        input: "complete after a transient ownership read failure",
        attachments: [],
      });
      const completedEventId = asEventId("evt-terminal-ownership-transient-read-completed");
      const completed = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === completedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      codex.emit({
        type: "turn.started",
        eventId: asEventId("evt-terminal-ownership-transient-read-started"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId: turn.turnId,
        payload: {},
      });
      yield* advanceTestClock(50);
      const bindingBeforeCompletion = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(
        (bindingBeforeCompletion.runtimePayload as { readonly activeTurnId?: unknown })
          .activeTurnId,
        turn.turnId,
      );

      bindingReadFailuresRemaining.set(threadId, 1);
      codex.emit({
        type: "turn.completed",
        eventId: completedEventId,
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId: turn.turnId,
        payload: { state: "completed", stopReason: "end_turn" },
      });

      const events = Array.from(yield* Fiber.join(completed));
      assert.equal(failedOwnershipReads, 1);
      assert.equal(events[0]?.type, "turn.completed");
      if (events[0]?.type === "turn.completed") {
        assert.equal(events[0].payload.state, "failed");
        assert.equal(events[0].payload.errorMessage, PROVIDER_EMPTY_RESPONSE_ERROR);
      }

      const failedThreadId = asThreadId("thread-terminal-ownership-persistent-read-failure");
      yield* provider.startSession(failedThreadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId: failedThreadId,
        runtimeMode: "full-access",
      });
      const failedTurn = yield* provider.sendTurn({
        threadId: failedThreadId,
        input: "preserve the provider failure when ownership is unreadable",
        attachments: [],
      });
      const failedCompletedEventId = asEventId(
        "evt-terminal-ownership-persistent-read-failed-completed",
      );
      const failedCompleted = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === failedCompletedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      codex.emit({
        type: "turn.started",
        eventId: asEventId("evt-terminal-ownership-persistent-read-failed-started"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId: failedThreadId,
        turnId: failedTurn.turnId,
        payload: {},
      });
      yield* advanceTestClock(50);

      const failedReadsBeforeProviderFailure = failedOwnershipReads;
      bindingReadFailuresRemaining.set(failedThreadId, 3);
      codex.emit({
        type: "turn.completed",
        eventId: failedCompletedEventId,
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:03.000Z",
        threadId: failedThreadId,
        turnId: failedTurn.turnId,
        payload: {
          state: "failed",
          stopReason: "error",
          errorMessage: "provider failed explicitly",
        },
      });

      const failedEvents = Array.from(yield* Fiber.join(failedCompleted));
      assert.equal(failedOwnershipReads - failedReadsBeforeProviderFailure, 3);
      assert.equal(failedEvents[0]?.type, "turn.completed");
      if (failedEvents[0]?.type === "turn.completed") {
        assert.equal(failedEvents[0].payload.state, "failed");
        assert.equal(failedEvents[0].payload.errorMessage, "provider failed explicitly");
      }
    }).pipe(Effect.provide(providerLayer));
  }).pipe(
    Effect.provide(Layer.mergeAll(directoryLayer, runtimeRepositoryLayer, NodeServices.layer)),
  );
});

it.effect("gives a Cursor replacement appended during a drain its own late-output grace", () => {
  const cursor = makeFakeCodexAdapter(CURSOR_DRIVER);
  const registry = makeAdapterRegistryMock({
    [CURSOR_DRIVER]: cursor.adapter,
  });
  const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
    Layer.provide(SqlitePersistenceMemory),
  );
  const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));

  return Effect.gen(function* () {
    const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
    const drainReadEntered = yield* Deferred.make<void>();
    const releaseDrainRead = yield* Deferred.make<void>();
    const threadId = asThreadId("thread-cursor-live-drain-replacement");
    const firstTurnId = asTurnId("turn-cursor-live-drain-first");
    const replacementTurnId = asTurnId("turn-cursor-live-drain-replacement");
    const firstCompletedEventId = asEventId("evt-cursor-live-drain-first-completed");
    const replacementCompletedEventId = asEventId("evt-cursor-live-drain-replacement-completed");
    let blockNextBindingRead = false;
    let bindingReadBlocked = false;
    const orderedDirectory: ProviderSessionDirectory.ProviderSessionDirectory["Service"] = {
      ...directory,
      getBinding: (requestedThreadId) => {
        if (requestedThreadId !== threadId || !blockNextBindingRead || bindingReadBlocked) {
          return directory.getBinding(requestedThreadId);
        }
        bindingReadBlocked = true;
        return Effect.gen(function* () {
          const binding = yield* directory.getBinding(requestedThreadId);
          yield* Deferred.succeed(drainReadEntered, undefined);
          yield* Deferred.await(releaseDrainRead);
          return binding;
        });
      },
    };
    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
      Layer.provide(
        Layer.succeed(ProviderSessionDirectory.ProviderSessionDirectory, orderedDirectory),
      ),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(serverConfigTestLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );

    yield* Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const completionIds = new Set([firstCompletedEventId, replacementCompletedEventId]);
      const completed = yield* provider.streamEvents.pipe(
        Stream.filter((event) => completionIds.has(event.eventId)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      cursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-live-drain-first-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId: firstTurnId,
        payload: {},
      });
      cursor.emit({
        type: "turn.completed",
        eventId: firstCompletedEventId,
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId: firstTurnId,
        payload: { state: "completed", stopReason: "end_turn" },
      });
      yield* advanceTestClock(50);

      cursor.sendTurn.mockImplementationOnce(() =>
        Effect.succeed({ threadId, turnId: replacementTurnId }),
      );
      yield* provider.sendTurn({
        threadId,
        input: "start a replacement without a turn.started event",
        attachments: [],
      });
      const replacementBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(
        (replacementBinding.runtimePayload as { readonly activeTurnId?: unknown }).activeTurnId,
        replacementTurnId,
      );

      blockNextBindingRead = true;
      yield* advanceTestClock(150);
      yield* Deferred.await(drainReadEntered);
      cursor.emit({
        type: "turn.completed",
        eventId: replacementCompletedEventId,
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        turnId: replacementTurnId,
        payload: { state: "completed", stopReason: "end_turn" },
      });
      cursor.emit({
        type: "content.delta",
        eventId: asEventId("evt-cursor-live-drain-replacement-output"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:03.000Z",
        threadId,
        turnId: replacementTurnId,
        itemId: "item-cursor-live-drain-replacement-output",
        payload: { streamKind: "assistant_text", delta: "replacement late output" },
      });
      yield* Deferred.succeed(releaseDrainRead, undefined);
      yield* advanceTestClock(200);

      const events = Array.from(yield* Fiber.join(completed));
      assert.deepEqual(
        events.map((event) => (event.type === "turn.completed" ? event.payload.state : event.type)),
        ["failed", "completed"],
      );
    }).pipe(Effect.provide(providerLayer));
  }).pipe(
    Effect.provide(Layer.mergeAll(directoryLayer, runtimeRepositoryLayer, NodeServices.layer)),
  );
});

it.effect("keeps a sibling instance's pending completion when a shared adapter is removed", () =>
  Effect.gen(function* () {
    const sharedCursor = makeFakeCodexAdapter(CURSOR_DRIVER);
    const retainedInstanceId = ProviderInstanceId.make("cursor_shared_retained");
    const removedInstanceId = ProviderInstanceId.make("cursor_shared_removed");
    let currentInstanceIds: ReadonlyArray<ProviderInstanceId> = [retainedInstanceId];
    const changes = yield* PubSub.unbounded<void>();
    const registry: ProviderAdapterRegistry.ProviderAdapterRegistry["Service"] = {
      getByInstance: (instanceId) =>
        currentInstanceIds.includes(instanceId)
          ? Effect.succeed(sharedCursor.adapter)
          : Effect.fail(
              new ProviderUnsupportedError({
                provider: ProviderDriverKind.make(instanceId),
              }),
            ),
      getInstanceInfo: (instanceId) =>
        Effect.succeed({
          instanceId,
          driverKind: CURSOR_DRIVER,
          displayName: undefined,
          enabled: true,
          continuationIdentity: {
            driverKind: CURSOR_DRIVER,
            continuationKey: `cursor:instance:${instanceId}`,
          },
        }),
      listInstances: () => Effect.succeed(currentInstanceIds),
      listProviders: () => Effect.succeed([CURSOR_DRIVER]),
      streamChanges: Stream.fromPubSub(changes),
      subscribeChanges: PubSub.subscribe(changes),
    };
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
      Layer.provide(directoryLayer),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(serverConfigTestLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );

    yield* Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-shared-adapter-retained");
      const turnId = asTurnId("turn-shared-adapter-retained");
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: retainedInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const completedEventId = asEventId("evt-shared-adapter-retained-completed");
      const completedFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === completedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      sharedCursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-shared-adapter-retained-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId,
        payload: {},
      });
      sharedCursor.emit({
        type: "turn.completed",
        eventId: completedEventId,
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId,
        payload: { state: "completed" },
      });
      yield* advanceTestClock(50);

      currentInstanceIds = [retainedInstanceId, removedInstanceId];
      yield* PubSub.publish(changes, undefined);
      yield* advanceTestClock(10);
      currentInstanceIds = [retainedInstanceId];
      yield* PubSub.publish(changes, undefined);
      yield* advanceTestClock(10);

      sharedCursor.emit({
        type: "content.delta",
        eventId: asEventId("evt-shared-adapter-retained-late-delta"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        turnId,
        itemId: "item-shared-adapter-retained-late-delta",
        payload: { streamKind: "assistant_text", delta: "retained late output" },
      });
      yield* advanceTestClock(200);

      const completedEvents = Array.from(yield* Fiber.join(completedFiber));
      assert.equal(completedEvents[0]?.type, "turn.completed");
      if (completedEvents[0]?.type === "turn.completed") {
        assert.equal(completedEvents[0].payload.state, "completed");
        assert.equal(completedEvents[0].payload.errorMessage, undefined);
      }
    }).pipe(
      Effect.provide(
        Layer.mergeAll(providerLayer, directoryLayer, runtimeRepositoryLayer, NodeServices.layer),
      ),
    );
  }),
);

fanout.layer("ProviderServiceLive fanout", (it) => {
  it.effect("fails an owned output-free successful turn explicitly", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-owned-empty-response");
      const turnId = asTurnId("turn-owned-empty-response");
      yield* provider.startSession(threadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const completedEventId = asEventId("evt-owned-empty-response-completed");
      const completed = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === completedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      fanout.codex.emit({
        type: "turn.started",
        eventId: asEventId("evt-owned-empty-response-started"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId,
        payload: {},
      });
      fanout.codex.emit({
        type: "thread.token-usage.updated",
        eventId: asEventId("evt-owned-empty-response-control"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId,
        payload: { usage: { usedTokens: 1 } },
      });
      fanout.codex.emit({
        type: "turn.proposed.completed",
        eventId: asEventId("evt-owned-empty-response-blank-plan"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:01.250Z",
        threadId,
        turnId,
        payload: { planMarkdown: "   " },
      });
      fanout.codex.emit({
        type: "item.completed",
        eventId: asEventId("evt-owned-empty-response-assistant-shell"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:01.500Z",
        threadId,
        turnId,
        itemId: "item-owned-empty-response-assistant-shell",
        payload: { itemType: "assistant_message", status: "completed" },
      });
      fanout.codex.emit({
        type: "item.updated",
        eventId: asEventId("evt-owned-empty-response-tool-progress"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:01.750Z",
        threadId,
        turnId,
        itemId: "item-owned-empty-response-tool-progress",
        payload: { itemType: "command_execution", status: "inProgress" },
      });
      fanout.codex.emit({
        type: "files.persisted",
        eventId: asEventId("evt-owned-empty-response-no-files"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:01.875Z",
        threadId,
        turnId,
        payload: { files: [] },
      });
      fanout.codex.emit({
        type: "thread.realtime.audio.delta",
        eventId: asEventId("evt-owned-empty-response-empty-audio"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:01.937Z",
        threadId,
        turnId,
        payload: {
          audio: {
            data: "",
            itemId: null,
            numChannels: 1,
            sampleRate: 24_000,
          },
        },
      });
      for (const [index, itemType] of [
        "review_entered",
        "review_exited",
        "context_compaction",
      ].entries()) {
        fanout.codex.emit({
          type: "item.completed",
          eventId: asEventId(`evt-owned-empty-response-control-item-${index}`),
          provider: CODEX_DRIVER,
          createdAt: `2026-01-01T00:00:01.95${index}Z`,
          threadId,
          turnId,
          itemId: `item-owned-empty-response-control-${index}`,
          payload: { itemType, status: "completed" },
        });
      }
      fanout.codex.emit({
        type: "item.updated",
        eventId: asEventId("evt-owned-empty-response-control-item-updated"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:01.959Z",
        threadId,
        turnId,
        itemId: "item-owned-empty-response-control-updated",
        payload: {
          itemType: "review_entered",
          status: "completed",
          detail: "Entered review mode",
        },
      });
      fanout.codex.emit({
        type: "turn.completed",
        eventId: completedEventId,
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        turnId,
        payload: { state: "completed" },
      });

      const events = Array.from(yield* Fiber.join(completed));
      assert.equal(events[0]?.type, "turn.completed");
      if (events[0]?.type === "turn.completed") {
        assert.equal(events[0].payload.state, "failed");
        assert.equal(events[0].payload.errorMessage, PROVIDER_EMPTY_RESPONSE_ERROR);
      }
      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(
        (binding.runtimePayload as { readonly lastError?: unknown }).lastError,
        PROVIDER_EMPTY_RESPONSE_ERROR,
      );
    }),
  );

  it.effect("preserves the empty-failure classification across duplicate completions", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-duplicate-empty-completion");
      const turnId = asTurnId("turn-duplicate-empty-completion");
      yield* provider.startSession(threadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const firstCompletionEventId = asEventId("evt-duplicate-empty-completion-first");
      const secondCompletionEventId = asEventId("evt-duplicate-empty-completion-second");
      const lateOutputEventId = asEventId("evt-duplicate-empty-completion-late-output");
      const completionIds = new Set([firstCompletionEventId, secondCompletionEventId]);
      const completed = yield* provider.streamEvents.pipe(
        Stream.filter((event) => completionIds.has(event.eventId)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      const firstCompletionPublished = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === firstCompletionEventId),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkChild,
      );
      const lateOutputPublished = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === lateOutputEventId),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      fanout.codex.emit({
        type: "turn.started",
        eventId: asEventId("evt-duplicate-empty-completion-started"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId,
        payload: {},
      });
      fanout.codex.emit({
        type: "turn.completed",
        eventId: firstCompletionEventId,
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId,
        payload: { state: "completed" },
      });
      yield* Fiber.join(firstCompletionPublished);
      fanout.codex.emit({
        type: "content.delta",
        eventId: lateOutputEventId,
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        turnId,
        itemId: "item-duplicate-empty-completion-late-output",
        payload: { streamKind: "assistant_text", delta: "too late to change the terminal state" },
      });
      yield* Fiber.join(lateOutputPublished);
      fanout.codex.emit({
        type: "turn.completed",
        eventId: secondCompletionEventId,
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:03.000Z",
        threadId,
        turnId,
        payload: { state: "completed" },
      });

      const events = Array.from(yield* Fiber.join(completed));
      assert.deepEqual(
        events.map((event) =>
          event.type === "turn.completed"
            ? [event.payload.state, event.payload.errorMessage]
            : [event.type, undefined],
        ),
        [
          ["failed", PROVIDER_EMPTY_RESPONSE_ERROR],
          ["failed", PROVIDER_EMPTY_RESPONSE_ERROR],
        ],
      );

      const recoveryTurn = yield* provider.sendTurn({
        threadId,
        input: "recover with output but no turn.started event",
        attachments: [],
      });
      const recoveryOutputEventId = asEventId("evt-after-empty-failure-recovery-output");
      const recoveryCompletedEventId = asEventId("evt-after-empty-failure-recovery-completed");
      const recoveryOutputPublished = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === recoveryOutputEventId),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkChild,
      );
      const recoveryCompleted = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === recoveryCompletedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      fanout.codex.emit({
        type: "content.delta",
        eventId: recoveryOutputEventId,
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:04.000Z",
        threadId,
        turnId: recoveryTurn.turnId,
        itemId: "item-after-empty-failure-recovery-output",
        payload: { streamKind: "assistant_text", delta: "successful recovery" },
      });
      yield* Fiber.join(recoveryOutputPublished);
      fanout.codex.emit({
        type: "turn.completed",
        eventId: recoveryCompletedEventId,
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:05.000Z",
        threadId,
        turnId: recoveryTurn.turnId,
        payload: { state: "completed" },
      });

      const recoveryEvents = Array.from(yield* Fiber.join(recoveryCompleted));
      assert.equal(recoveryEvents[0]?.type, "turn.completed");
      if (recoveryEvents[0]?.type === "turn.completed") {
        assert.equal(recoveryEvents[0].payload.state, "completed");
      }
      const recoveredBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(
        (recoveredBinding.runtimePayload as { readonly lastError?: unknown }).lastError,
        null,
      );

      const historicalReplayEventId = asEventId("evt-duplicate-empty-completion-after-recovery");
      const historicalReplay = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === historicalReplayEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      fanout.codex.emit({
        type: "turn.completed",
        eventId: historicalReplayEventId,
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:06.000Z",
        threadId,
        turnId,
        payload: { state: "completed" },
      });
      const historicalReplayEvents = Array.from(yield* Fiber.join(historicalReplay));
      assert.equal(historicalReplayEvents[0]?.type, "turn.completed");
      if (historicalReplayEvents[0]?.type === "turn.completed") {
        assert.equal(historicalReplayEvents[0].payload.state, "failed");
        assert.equal(historicalReplayEvents[0].payload.errorMessage, PROVIDER_EMPTY_RESPONSE_ERROR);
      }
      const bindingAfterHistoricalReplay = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(
        (bindingAfterHistoricalReplay.runtimePayload as { readonly lastError?: unknown }).lastError,
        null,
      );
    }),
  );

  it.effect("does not seed output when a live Cursor session restarts in the same service", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-live-cursor-restart-empty");
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId,
        input: "restart without output",
        attachments: [],
      });
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const completedEventId = asEventId("evt-live-cursor-restart-empty-completed");
      const completed = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === completedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      fanout.cursor.emit({
        type: "turn.completed",
        eventId: completedEventId,
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId: turn.turnId,
        payload: { state: "completed", stopReason: "end_turn" },
      });
      yield* advanceTestClock(200);

      const events = Array.from(yield* Fiber.join(completed));
      assert.equal(events[0]?.type, "turn.completed");
      if (events[0]?.type === "turn.completed") {
        assert.equal(events[0].payload.state, "failed");
      }
    }),
  );

  it.effect("preserves ingress ownership while buffered Cursor completions drain", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-cursor-buffered-owned-completions");
      const turnId = asTurnId("turn-cursor-buffered-owned-completions");
      const replacementTurnId = asTurnId("turn-cursor-buffered-owned-replacement");
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const initialCompletionIds = [
        "evt-cursor-buffered-owned-completion-first",
        "evt-cursor-buffered-owned-completion-second",
      ];
      const duplicateCompletionId = "evt-cursor-buffered-owned-completion-after-replacement";
      const completionIds = new Set([...initialCompletionIds, duplicateCompletionId]);
      const completed = yield* provider.streamEvents.pipe(
        Stream.filter((event) => completionIds.has(event.eventId)),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      fanout.cursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-buffered-owned-completions-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId,
        payload: {},
      });
      for (const eventId of initialCompletionIds) {
        fanout.cursor.emit({
          type: "turn.completed",
          eventId: asEventId(eventId),
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:01.000Z",
          threadId,
          turnId,
          payload: { state: "completed", stopReason: "end_turn" },
        });
      }
      yield* advanceTestClock(50);
      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      yield* directory.upsert({
        ...binding,
        runtimePayload: {
          ...(binding.runtimePayload as Record<string, unknown>),
          activeTurnId: replacementTurnId,
        },
      });
      yield* advanceTestClock(150);
      fanout.cursor.emit({
        type: "turn.completed",
        eventId: asEventId(duplicateCompletionId),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        turnId,
        payload: { state: "completed", stopReason: "end_turn" },
      });

      const events = Array.from(yield* Fiber.join(completed));
      assert.deepEqual(
        events.map((event) => (event.type === "turn.completed" ? event.payload.state : event.type)),
        ["failed", "failed", "failed"],
      );
      const bindingAfterDuplicate = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload = bindingAfterDuplicate.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly emptyResponseFailureTurnIds?: ReadonlyArray<string>;
      };
      assert.equal(runtimePayload.activeTurnId, replacementTurnId);
      assert.equal(runtimePayload.emptyResponseFailureTurnIds?.includes(String(turnId)), true);
    }),
  );

  it.effect("preserves late-output evidence across duplicate buffered Cursor completions", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-cursor-duplicate-completions-with-output");
      const turnId = asTurnId("turn-cursor-duplicate-completions-with-output");
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const completionIds = new Set([
        "evt-cursor-duplicate-with-output-first",
        "evt-cursor-duplicate-with-output-second",
      ]);
      const completed = yield* provider.streamEvents.pipe(
        Stream.filter((event) => completionIds.has(event.eventId)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      fanout.cursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-duplicate-with-output-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId,
        payload: {},
      });
      for (const eventId of completionIds) {
        fanout.cursor.emit({
          type: "turn.completed",
          eventId: asEventId(eventId),
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:01.000Z",
          threadId,
          turnId,
          payload: { state: "completed", stopReason: "end_turn" },
        });
      }
      fanout.cursor.emit({
        type: "content.delta",
        eventId: asEventId("evt-cursor-duplicate-with-output-delta"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        turnId,
        itemId: "item-cursor-duplicate-with-output-delta",
        payload: { streamKind: "assistant_text", delta: "late output" },
      });
      yield* advanceTestClock(200);

      const events = Array.from(yield* Fiber.join(completed));
      assert.deepEqual(
        events.map((event) => (event.type === "turn.completed" ? event.payload.state : event.type)),
        ["completed", "completed"],
      );
    }),
  );

  it.effect("does not rescue a Cursor completion with output after a replacement turn starts", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-cursor-output-after-replacement-start");
      const completedTurnId = asTurnId("turn-cursor-before-replacement-start");
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const completedEventId = asEventId("evt-cursor-before-replacement-start-completed");
      const completed = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === completedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      fanout.cursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-before-replacement-start-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId: completedTurnId,
        payload: {},
      });
      fanout.cursor.emit({
        type: "turn.completed",
        eventId: completedEventId,
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId: completedTurnId,
        payload: { state: "completed", stopReason: "end_turn" },
      });
      yield* advanceTestClock(50);
      fanout.cursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-replacement-turn-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        turnId: asTurnId("turn-cursor-replacement"),
        payload: {},
      });
      fanout.cursor.emit({
        type: "content.delta",
        eventId: asEventId("evt-cursor-stale-output-after-replacement-start"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:03.000Z",
        threadId,
        turnId: completedTurnId,
        itemId: "item-cursor-stale-output-after-replacement-start",
        payload: { streamKind: "assistant_text", delta: "stale output" },
      });
      yield* advanceTestClock(150);

      const events = Array.from(yield* Fiber.join(completed));
      assert.equal(events[0]?.type, "turn.completed");
      if (events[0]?.type === "turn.completed") {
        assert.equal(events[0].payload.state, "failed");
        assert.equal(events[0].payload.errorMessage, PROVIDER_EMPTY_RESPONSE_ERROR);
      }
    }),
  );

  it.effect("gives a buffered replacement Cursor completion its own late-output grace", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-cursor-buffered-replacement-grace");
      const firstTurnId = asTurnId("turn-cursor-buffered-replacement-first");
      const replacementTurnId = asTurnId("turn-cursor-buffered-replacement-second");
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const completionIds = new Set([
        "evt-cursor-buffered-replacement-first-completed",
        "evt-cursor-buffered-replacement-second-completed",
      ]);
      const completed = yield* provider.streamEvents.pipe(
        Stream.filter((event) => completionIds.has(event.eventId)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      fanout.cursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-buffered-replacement-first-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId: firstTurnId,
        payload: {},
      });
      fanout.cursor.emit({
        type: "turn.completed",
        eventId: asEventId("evt-cursor-buffered-replacement-first-completed"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId: firstTurnId,
        payload: { state: "completed", stopReason: "end_turn" },
      });
      yield* advanceTestClock(50);
      fanout.cursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-buffered-replacement-second-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        turnId: replacementTurnId,
        payload: {},
      });
      fanout.cursor.emit({
        type: "turn.completed",
        eventId: asEventId("evt-cursor-buffered-replacement-second-completed"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:03.000Z",
        threadId,
        turnId: replacementTurnId,
        payload: { state: "completed", stopReason: "end_turn" },
      });
      fanout.cursor.emit({
        type: "content.delta",
        eventId: asEventId("evt-cursor-buffered-replacement-second-output"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:04.000Z",
        threadId,
        turnId: replacementTurnId,
        itemId: "item-cursor-buffered-replacement-second-output",
        payload: { streamKind: "assistant_text", delta: "replacement late output" },
      });
      yield* advanceTestClock(150);
      yield* advanceTestClock(200);

      const events = Array.from(yield* Fiber.join(completed));
      assert.deepEqual(
        events.map((event) => (event.type === "turn.completed" ? event.payload.state : event.type)),
        ["failed", "completed"],
      );
    }),
  );

  it.effect("does not let a stale turn.started clear active turn output", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-output-before-stale-start");
      const turnId = asTurnId("turn-output-before-stale-start");
      yield* provider.startSession(threadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const completedEventId = asEventId("evt-output-before-stale-start-completed");
      const completed = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === completedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      fanout.codex.emit({
        type: "turn.started",
        eventId: asEventId("evt-output-before-stale-start-active"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId,
        payload: {},
      });
      fanout.codex.emit({
        type: "content.delta",
        eventId: asEventId("evt-output-before-stale-start-output"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId,
        itemId: "item-output-before-stale-start",
        payload: { streamKind: "assistant_text", delta: "owned output" },
      });
      fanout.codex.emit({
        type: "turn.started",
        eventId: asEventId("evt-output-before-stale-start-stale"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        turnId: asTurnId("turn-stale-after-active-output"),
        payload: {},
      });
      fanout.codex.emit({
        type: "turn.completed",
        eventId: completedEventId,
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:03.000Z",
        threadId,
        turnId,
        payload: { state: "completed" },
      });

      const events = Array.from(yield* Fiber.join(completed));
      assert.equal(events[0]?.type, "turn.completed");
      if (events[0]?.type === "turn.completed") {
        assert.equal(events[0].payload.state, "completed");
      }
    }),
  );

  it.effect("KNOWN: accepts an old nonterminal lifecycle event across a same-thread restart", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-known-old-lifecycle-after-restart");
      yield* provider.startSession(threadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      const receivedFiber = yield* Stream.runHead(provider.streamEvents).pipe(Effect.forkChild);
      yield* advanceTestClock(50);
      const releaseRuntimeEvent = yield* fanout.codex.pauseRuntimeEvents;
      fanout.codex.emit({
        type: "session.state.changed",
        eventId: asEventId("evt-known-old-lifecycle-after-restart"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        payload: { state: "running" },
      });

      const replacement = yield* provider.startSession(threadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      yield* releaseRuntimeEvent;

      const received = Option.getOrThrow(yield* Fiber.join(receivedFiber));
      assert.equal(received.eventId, "evt-known-old-lifecycle-after-restart");
      assert.deepEqual(getCapturedProviderRuntimeEventBinding(received), {
        threadId: replacement.threadId,
        provider: replacement.provider,
        providerInstanceId: codexInstanceId,
        runtimeMode: replacement.runtimeMode,
        cwd: replacement.cwd,
      });
    }),
  );

  it.effect("captures queued ingress authority but not stopped historical bindings", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-ingress-binding-race");
      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      const receivedFiber = yield* Stream.runHead(provider.streamEvents).pipe(Effect.forkChild);
      yield* advanceTestClock(50);
      const releaseRuntimeEvent = yield* fanout.codex.pauseRuntimeEvents;
      fanout.codex.emit({
        type: "content.delta",
        eventId: asEventId("evt-ingress-binding-race"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId: asTurnId("turn-ingress-binding-race"),
        itemId: "item-ingress-binding-race",
        delta: "queued before teardown",
        streamKind: "assistant_text",
      });
      fanout.codex.sessions.delete(threadId);
      yield* releaseRuntimeEvent;

      const received = Option.getOrThrow(yield* Fiber.join(receivedFiber));
      assert.deepEqual(getCapturedProviderRuntimeEventBinding(received), {
        threadId,
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        runtimeMode: "full-access",
        cwd: undefined,
      });

      yield* directory.upsert({
        threadId,
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        runtimeMode: "full-access",
        status: "stopped",
      });
      const stoppedEventFiber = yield* Stream.runHead(provider.streamEvents).pipe(Effect.forkChild);
      yield* advanceTestClock(50);
      fanout.codex.emit({
        type: "content.delta",
        eventId: asEventId("evt-after-stopped-binding"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId: asTurnId("turn-after-stopped-binding"),
        itemId: "item-after-stopped-binding",
        delta: "must remain unauthoritative",
        streamKind: "assistant_text",
      });
      const stoppedEvent = Option.getOrThrow(yield* Fiber.join(stoppedEventFiber));
      assert.equal(getCapturedProviderRuntimeEventBinding(stoppedEvent), undefined);
    }),
  );

  it.effect("fans out adapter turn completion events", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const session = yield* provider.startSession(asThreadId("thread-1"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-1"),
        runtimeMode: "full-access",
      });

      const completed = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === asEventId("evt-1")),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      const completedEvent: LegacyProviderRuntimeEvent = {
        type: "turn.completed",
        eventId: asEventId("evt-1"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        payload: { state: "completed" },
      };

      fanout.codex.emit({
        type: "content.delta",
        eventId: asEventId("evt-1-delta"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        itemId: "item-1",
        payload: {
          delta: "done",
          streamKind: "assistant_text",
        },
      });
      fanout.codex.emit(completedEvent);
      const events = Array.from(yield* Fiber.join(completed));

      assert.equal(events[0]?.type, "turn.completed");
      assert.equal(events[0]?.providerInstanceId, codexInstanceId);
    }),
  );

  it.effect("holds Cursor completion decision through a late assistant delta", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-cursor-late-output");
      const turnId = asTurnId("turn-cursor-late-output");
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      const outputEvents = yield* provider.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.turnId === turnId &&
            (event.type === "turn.completed" || event.type === "content.delta"),
        ),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      fanout.cursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-late-output-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId,
        payload: {},
      });
      fanout.cursor.emit({
        type: "turn.completed",
        eventId: asEventId("evt-cursor-late-output-completed"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId,
        payload: { state: "completed", stopReason: "end_turn" },
      });
      yield* advanceTestClock(50);
      fanout.cursor.emit({
        type: "content.delta",
        eventId: asEventId("evt-cursor-late-output-delta"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        turnId,
        itemId: "item-cursor-late-output",
        payload: {
          streamKind: "assistant_text",
          delta: "late assistant output",
        },
      });
      yield* advanceTestClock(200);

      const events = Array.from(yield* Fiber.join(outputEvents));
      assert.deepEqual(
        events.map((event) => event.type),
        ["turn.completed", "content.delta"],
      );
      const completed = events[0];
      assert.equal(completed?.type, "turn.completed");
      if (completed?.type === "turn.completed") {
        assert.equal(completed.payload.state, "completed");
        assert.equal(completed.payload.errorMessage, undefined);
      }
    }),
  );

  it.effect("preserves Cursor completion, late delta, and session exit ordering", () => {
    let restoreReadMcpProviderSession = () => {};
    return Effect.gen(function* () {
      const observedMcpProviderSessionIds: Array<string | undefined> = [];
      const readMcpProviderSession = McpProviderSession.readMcpProviderSession;
      const readMcpProviderSessionSpy = vi
        .spyOn(McpProviderSession, "readMcpProviderSession")
        .mockImplementation((threadId) => {
          const observed = readMcpProviderSession(threadId);
          observedMcpProviderSessionIds.push(observed?.providerSessionId);
          return observed;
        });
      restoreReadMcpProviderSession = () => readMcpProviderSessionSpy.mockRestore();
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-cursor-late-output-before-exit");
      const turnId = asTurnId("turn-cursor-late-output-before-exit");
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      McpProviderSession.setMcpProviderSession({
        environmentId,
        threadId,
        providerSessionId: "provider-session-before-buffered-exit",
        providerInstanceId: cursorInstanceId,
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer before-buffered-exit-token",
      });

      const expectedEventIds = new Set([
        "evt-cursor-before-exit-completed",
        "evt-cursor-before-exit-delta",
        "evt-cursor-after-late-output-exited",
      ]);
      const orderedEvents = yield* provider.streamEvents.pipe(
        Stream.filter((event) => expectedEventIds.has(event.eventId)),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      fanout.cursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-before-exit-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId,
        payload: {},
      });
      fanout.cursor.emit({
        type: "turn.completed",
        eventId: asEventId("evt-cursor-before-exit-completed"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId,
        payload: { state: "completed", stopReason: "end_turn" },
      });
      yield* advanceTestClock(50);
      fanout.cursor.emit({
        type: "content.delta",
        eventId: asEventId("evt-cursor-before-exit-delta"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        turnId,
        itemId: "item-cursor-before-exit-delta",
        payload: {
          streamKind: "assistant_text",
          delta: "late output before exit",
        },
      });
      fanout.cursor.sessions.delete(threadId);
      observedMcpProviderSessionIds.length = 0;
      fanout.cursor.emit({
        type: "session.exited",
        eventId: asEventId("evt-cursor-after-late-output-exited"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:03.000Z",
        threadId,
        payload: { exitKind: "graceful" },
      });
      yield* advanceTestClock(50);
      assert.include(
        observedMcpProviderSessionIds,
        "provider-session-before-buffered-exit",
        "the buffered exit must capture its MCP session identity when it arrives",
      );
      McpProviderSession.setMcpProviderSession({
        environmentId,
        threadId,
        providerSessionId: "provider-session-after-buffered-exit",
        providerInstanceId: cursorInstanceId,
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer after-buffered-exit-token",
      });
      yield* advanceTestClock(150);

      const events = Array.from(yield* Fiber.join(orderedEvents));
      assert.deepEqual(
        events.map((event) => event.eventId),
        [
          "evt-cursor-before-exit-completed",
          "evt-cursor-before-exit-delta",
          "evt-cursor-after-late-output-exited",
        ],
      );
      const completed = events[0];
      assert.equal(completed?.type, "turn.completed");
      if (completed?.type === "turn.completed") {
        assert.equal(completed.payload.state, "completed");
        assert.equal(completed.payload.errorMessage, undefined);
      }
      assert.equal(
        McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId,
        "provider-session-after-buffered-exit",
      );
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          restoreReadMcpProviderSession();
          McpProviderSession.clearMcpProviderSession(
            asThreadId("thread-cursor-late-output-before-exit"),
          );
        }),
      ),
    );
  });

  // TODO(ADA-192): re-enable when the provider output-preservation guard lands
  it.effect.skip("does not count a Cursor delta that follows session exit", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-cursor-output-after-exit");
      const turnId = asTurnId("turn-cursor-output-after-exit");
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      const expectedEventIds = new Set([
        "evt-cursor-output-after-exit-completed",
        "evt-cursor-before-late-output-exited",
        "evt-cursor-output-after-exit-delta",
      ]);
      const orderedEvents = yield* provider.streamEvents.pipe(
        Stream.filter((event) => expectedEventIds.has(event.eventId)),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      fanout.cursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-output-after-exit-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId,
        payload: {},
      });
      fanout.cursor.emit({
        type: "turn.completed",
        eventId: asEventId("evt-cursor-output-after-exit-completed"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId,
        payload: { state: "completed", stopReason: "end_turn" },
      });
      yield* advanceTestClock(50);
      fanout.cursor.sessions.delete(threadId);
      fanout.cursor.emit({
        type: "session.exited",
        eventId: asEventId("evt-cursor-before-late-output-exited"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        payload: { exitKind: "graceful" },
      });
      fanout.cursor.emit({
        type: "content.delta",
        eventId: asEventId("evt-cursor-output-after-exit-delta"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:03.000Z",
        threadId,
        turnId,
        itemId: "item-cursor-output-after-exit-delta",
        payload: {
          streamKind: "assistant_text",
          delta: "too-late output",
        },
      });
      yield* advanceTestClock(200);

      const events = Array.from(yield* Fiber.join(orderedEvents));
      assert.deepEqual(
        events.map((event) => event.eventId),
        [
          "evt-cursor-output-after-exit-completed",
          "evt-cursor-before-late-output-exited",
          "evt-cursor-output-after-exit-delta",
        ],
      );
      const completed = events[0];
      assert.equal(completed?.type, "turn.completed");
      if (completed?.type === "turn.completed") {
        assert.equal(completed.payload.state, "failed");
        assert.equal(completed.payload.errorMessage, PROVIDER_EMPTY_RESPONSE_ERROR);
      }
    }),
  );

  it.effect("does not let a stale Cursor completion capture the active turn grace", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-cursor-stale-completion-before-live-turn");
      const staleTurnId = asTurnId("turn-cursor-stale-completion");
      const activeTurnId = asTurnId("turn-cursor-active-after-stale-completion");
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      const expectedEventIds = new Set([
        "evt-cursor-stale-completion",
        "evt-cursor-active-completion",
        "evt-cursor-active-late-delta",
      ]);
      const orderedEvents = yield* provider.streamEvents.pipe(
        Stream.filter((event) => expectedEventIds.has(event.eventId)),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      fanout.cursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-active-before-stale-completion-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId: activeTurnId,
        payload: {},
      });
      yield* advanceTestClock(50);
      fanout.cursor.emit({
        type: "turn.completed",
        eventId: asEventId("evt-cursor-stale-completion"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId: staleTurnId,
        payload: { state: "completed", stopReason: "end_turn" },
      });
      fanout.cursor.emit({
        type: "turn.completed",
        eventId: asEventId("evt-cursor-active-completion"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId,
        turnId: activeTurnId,
        payload: { state: "completed", stopReason: "end_turn" },
      });
      yield* advanceTestClock(50);
      fanout.cursor.emit({
        type: "turn.aborted",
        eventId: asEventId("evt-cursor-unrelated-stale-abort"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:02.500Z",
        threadId,
        turnId: staleTurnId,
        payload: { reason: "stale abort" },
      });
      fanout.cursor.emit({
        type: "content.delta",
        eventId: asEventId("evt-cursor-active-late-delta"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:03.000Z",
        threadId,
        turnId: activeTurnId,
        itemId: "item-cursor-active-late-delta",
        payload: {
          streamKind: "assistant_text",
          delta: "active turn late output",
        },
      });
      yield* advanceTestClock(200);

      const events = Array.from(yield* Fiber.join(orderedEvents));
      assert.deepEqual(
        events.map((event) => event.eventId),
        [
          "evt-cursor-stale-completion",
          "evt-cursor-active-completion",
          "evt-cursor-active-late-delta",
        ],
      );
      const staleCompleted = events[0];
      assert.equal(staleCompleted?.type, "turn.completed");
      if (staleCompleted?.type === "turn.completed") {
        assert.equal(staleCompleted.payload.state, "completed");
        assert.equal(staleCompleted.payload.errorMessage, undefined);
      }
      const activeCompleted = events[1];
      assert.equal(activeCompleted?.type, "turn.completed");
      if (activeCompleted?.type === "turn.completed") {
        assert.equal(activeCompleted.payload.state, "completed");
        assert.equal(activeCompleted.payload.errorMessage, undefined);
      }
    }),
  );

  // TODO(ADA-192): re-enable when the provider output-preservation guard lands
  it.effect.skip("fails an output-free Cursor completion after the late-output grace", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-cursor-empty-after-grace");
      const turnId = asTurnId("turn-cursor-empty-after-grace");
      yield* provider.startSession(threadId, {
        provider: CURSOR_DRIVER,
        providerInstanceId: cursorInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      const completedEventId = asEventId("evt-cursor-empty-after-grace-completed");
      const completedFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === completedEventId),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      fanout.cursor.emit({
        type: "turn.started",
        eventId: asEventId("evt-cursor-empty-after-grace-started"),
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId,
        payload: {},
      });
      fanout.cursor.emit({
        type: "turn.completed",
        eventId: completedEventId,
        provider: CURSOR_DRIVER,
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId,
        turnId,
        payload: { state: "completed", stopReason: "end_turn" },
      });
      yield* advanceTestClock(200);

      const events = Array.from(yield* Fiber.join(completedFiber));
      assert.equal(events[0]?.type, "turn.completed");
      if (events[0]?.type === "turn.completed") {
        assert.equal(events[0].payload.state, "failed");
        assert.equal(events[0].payload.errorMessage, PROVIDER_EMPTY_RESPONSE_ERROR);
      }
    }),
  );

  // TODO(ADA-192): re-enable when the provider output-preservation guard lands
  it.effect.skip("turns a control-only successful adapter response into an explicit failure", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const session = yield* provider.startSession(asThreadId("thread-empty-response"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-empty-response"),
        runtimeMode: "full-access",
      });
      const completed = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.type === "turn.completed"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      fanout.codex.emit({
        type: "turn.started",
        eventId: asEventId("evt-empty-response-started"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-empty-response"),
        payload: {},
      });
      fanout.codex.emit({
        type: "thread.token-usage.updated",
        eventId: asEventId("evt-empty-response-usage"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-empty-response"),
        payload: { usage: { usedTokens: 1 } },
      });
      fanout.codex.emit({
        type: "turn.proposed.delta",
        eventId: asEventId("evt-empty-response-blank-plan"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:02.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-empty-response"),
        payload: { delta: "   " },
      });
      fanout.codex.emit({
        type: "item.completed",
        eventId: asEventId("evt-empty-response-assistant-shell"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:03.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-empty-response"),
        itemId: "item-empty-response-assistant-shell",
        payload: { itemType: "assistant_message", status: "completed" },
      });
      fanout.codex.emit({
        type: "item.completed",
        eventId: asEventId("evt-empty-response-reasoning-shell"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:04.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-empty-response"),
        itemId: "item-empty-response-reasoning-shell",
        payload: { itemType: "reasoning", status: "completed" },
      });
      fanout.codex.emit({
        type: "item.started",
        eventId: asEventId("evt-empty-response-tool-started"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:05.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-empty-response"),
        itemId: "item-empty-response-tool",
        payload: { itemType: "command_execution", status: "inProgress" },
      });
      fanout.codex.emit({
        type: "item.updated",
        eventId: asEventId("evt-empty-response-tool-progress"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:06.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-empty-response"),
        itemId: "item-empty-response-tool",
        payload: { itemType: "command_execution", status: "inProgress" },
      });
      fanout.codex.emit({
        type: "turn.completed",
        eventId: asEventId("evt-empty-response-completed"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:07.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-empty-response"),
        payload: { state: "completed" },
      });

      const events = Array.from(yield* Fiber.join(completed));
      assert.deepEqual(events[0], {
        type: "turn.completed",
        eventId: asEventId("evt-empty-response-completed"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        createdAt: "2026-01-01T00:00:07.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-empty-response"),
        payload: {
          state: "failed",
          errorMessage: PROVIDER_EMPTY_RESPONSE_ERROR,
        },
      });
    }),
  );

  it.effect.each<
    MeaningfulOutputCase & {
      readonly adapter: "codex" | "claude" | "cursor";
    }
  >([
    {
      name: "Codex proposed plan",
      adapter: "codex",
      provider: CODEX_DRIVER,
      providerInstanceId: codexInstanceId,
      output: {
        type: "turn.proposed.completed",
        payload: { planMarkdown: "# Proposed plan" },
      },
    },
    {
      name: "Codex proposed plan delta",
      adapter: "codex",
      provider: CODEX_DRIVER,
      providerInstanceId: codexInstanceId,
      output: {
        type: "turn.proposed.delta",
        payload: { delta: "Draft the migration steps" },
      },
    },
    {
      name: "Codex reasoning delta",
      adapter: "codex",
      provider: CODEX_DRIVER,
      providerInstanceId: codexInstanceId,
      output: {
        type: "content.delta",
        payload: { streamKind: "reasoning_text", delta: "Consider the constraints" },
      },
    },
    {
      name: "Codex diff",
      adapter: "codex",
      provider: CODEX_DRIVER,
      providerInstanceId: codexInstanceId,
      output: {
        type: "turn.diff.updated",
        payload: { unifiedDiff: "@@ -1 +1 @@\n-before\n+after" },
      },
    },
    {
      name: "Codex tool result",
      adapter: "codex",
      provider: CODEX_DRIVER,
      providerInstanceId: codexInstanceId,
      output: {
        type: "item.completed",
        itemId: "codex-tool-item",
        payload: { itemType: "command_execution", status: "completed" },
      },
    },
    {
      name: "Codex assistant item",
      adapter: "codex",
      provider: CODEX_DRIVER,
      providerInstanceId: codexInstanceId,
      output: {
        type: "item.completed",
        itemId: "codex-assistant-item",
        payload: {
          itemType: "assistant_message",
          status: "completed",
          detail: "Final answer",
        },
      },
    },
    {
      name: "Codex completed item update",
      adapter: "codex",
      provider: CODEX_DRIVER,
      providerInstanceId: codexInstanceId,
      output: {
        type: "item.updated",
        itemId: "codex-updated-tool-item",
        payload: { itemType: "command_execution", status: "completed" },
      },
    },
    {
      name: "Codex in-progress assistant item update",
      adapter: "codex",
      provider: CODEX_DRIVER,
      providerInstanceId: codexInstanceId,
      output: {
        type: "item.updated",
        itemId: "codex-in-progress-assistant-item",
        payload: {
          itemType: "assistant_message",
          status: "inProgress",
          detail: "Partial visible answer",
        },
      },
    },
    {
      name: "Codex in-progress command item update",
      adapter: "codex",
      provider: CODEX_DRIVER,
      providerInstanceId: codexInstanceId,
      output: {
        type: "item.updated",
        itemId: "codex-in-progress-command-item",
        payload: {
          itemType: "command_execution",
          status: "inProgress",
          detail: "Visible command output",
        },
      },
    },
    {
      name: "Codex realtime audio",
      adapter: "codex",
      provider: CODEX_DRIVER,
      providerInstanceId: codexInstanceId,
      output: {
        type: "thread.realtime.audio.delta",
        payload: { audio: "base64-audio" },
      },
    },
    {
      name: "Claude proposed plan",
      adapter: "claude",
      provider: CLAUDE_AGENT_DRIVER,
      providerInstanceId: claudeAgentInstanceId,
      output: {
        type: "turn.proposed.completed",
        payload: { planMarkdown: "# Claude plan" },
      },
    },
    {
      name: "Claude task completion",
      adapter: "claude",
      provider: CLAUDE_AGENT_DRIVER,
      providerInstanceId: claudeAgentInstanceId,
      output: {
        type: "task.completed",
        payload: { taskId: "claude-task", status: "completed", summary: "Task finished" },
      },
    },
    {
      name: "Claude task progress",
      adapter: "claude",
      provider: CLAUDE_AGENT_DRIVER,
      providerInstanceId: claudeAgentInstanceId,
      output: {
        type: "task.progress",
        payload: { taskId: "claude-task-progress", description: "Inspecting the repository" },
      },
    },
    {
      name: "Claude hook completion",
      adapter: "claude",
      provider: CLAUDE_AGENT_DRIVER,
      providerInstanceId: claudeAgentInstanceId,
      output: {
        type: "hook.completed",
        payload: { hookId: "claude-hook", outcome: "success", output: "Hook finished" },
      },
    },
    {
      name: "Claude hook progress",
      adapter: "claude",
      provider: CLAUDE_AGENT_DRIVER,
      providerInstanceId: claudeAgentInstanceId,
      output: {
        type: "hook.progress",
        payload: { hookId: "claude-hook-progress", stdout: "Visible hook output" },
      },
    },
    {
      name: "Claude tool progress",
      adapter: "claude",
      provider: CLAUDE_AGENT_DRIVER,
      providerInstanceId: claudeAgentInstanceId,
      output: {
        type: "tool.progress",
        payload: { toolUseId: "claude-tool-progress", summary: "Visible tool progress" },
      },
    },
    {
      name: "Claude tool summary",
      adapter: "claude",
      provider: CLAUDE_AGENT_DRIVER,
      providerInstanceId: claudeAgentInstanceId,
      output: {
        type: "tool.summary",
        payload: { summary: "Inspected the repository" },
      },
    },
    {
      name: "Claude denied tool",
      adapter: "claude",
      provider: CLAUDE_AGENT_DRIVER,
      providerInstanceId: claudeAgentInstanceId,
      output: {
        type: "tool.denied",
        payload: { toolName: "Bash", reason: "Denied by policy" },
      },
    },
    {
      name: "Claude persisted files",
      adapter: "claude",
      provider: CLAUDE_AGENT_DRIVER,
      providerInstanceId: claudeAgentInstanceId,
      output: {
        type: "files.persisted",
        payload: { files: [{ filename: "report.md", fileId: "file-report" }] },
      },
    },
    {
      name: "Cursor proposed plan",
      adapter: "cursor",
      provider: CURSOR_DRIVER,
      providerInstanceId: cursorInstanceId,
      output: {
        type: "turn.proposed.completed",
        payload: { planMarkdown: "# Cursor plan" },
      },
    },
    {
      name: "Cursor permission request",
      adapter: "cursor",
      provider: CURSOR_DRIVER,
      providerInstanceId: cursorInstanceId,
      output: {
        type: "request.opened",
        payload: {
          requestType: "command_execution_approval",
          detail: "Allow this command?",
        },
      },
    },
    {
      name: "Cursor user input request",
      adapter: "cursor",
      provider: CURSOR_DRIVER,
      providerInstanceId: cursorInstanceId,
      output: {
        type: "user-input.requested",
        payload: {
          questions: [
            {
              id: "cursor-question",
              header: "Decision",
              question: "Which option should Cursor use?",
              options: [{ label: "Safe", description: "Use the safe option." }],
            },
          ],
        },
      },
    },
    {
      name: "Cursor tool result",
      adapter: "cursor",
      provider: CURSOR_DRIVER,
      providerInstanceId: cursorInstanceId,
      output: {
        type: "item.completed",
        itemId: "cursor-tool-item",
        payload: { itemType: "file_change", status: "completed" },
      },
    },
  ])("preserves successful $name-only turns", (testCase) =>
    assertSuccessfulOutputOnlyTurn(testCase, fanout[testCase.adapter]),
  );

  it.effect("fans out canonical runtime events in emission order", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const session = yield* provider.startSession(asThreadId("thread-seq"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-seq"),
        runtimeMode: "full-access",
      });

      const receivedRef = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const consumer = yield* Stream.take(provider.streamEvents, 3).pipe(
        Stream.runForEach((event) => Ref.update(receivedRef, (current) => [...current, event])),
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      fanout.codex.emit({
        type: "tool.started",
        eventId: asEventId("evt-seq-1"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        toolKind: "command",
        title: "Ran command",
      });
      fanout.codex.emit({
        type: "tool.completed",
        eventId: asEventId("evt-seq-2"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        toolKind: "command",
        title: "Ran command",
      });
      fanout.codex.emit({
        type: "turn.completed",
        eventId: asEventId("evt-seq-3"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        status: "completed",
      });

      yield* Fiber.join(consumer);
      const received = yield* Ref.get(receivedRef);
      assert.deepEqual(
        received.map((event) => event.eventId),
        [asEventId("evt-seq-1"), asEventId("evt-seq-2"), asEventId("evt-seq-3")],
      );
    }),
  );

  it.effect(
    "persists terminal ownership and publishes an owning adapter's live session error",
    () =>
      Effect.gen(function* () {
        const threadId = asThreadId("thread-live-adapter-session-error");
        const provider = yield* ProviderService.ProviderService;
        const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
        yield* provider.startSession(threadId, {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: codexInstanceId,
          threadId,
          runtimeMode: "full-access",
        });
        const turn = yield* provider.sendTurn({
          threadId,
          input: "fail this provider turn",
          attachments: [],
        });
        const errorEventId = asEventId("evt-live-adapter-session-error");
        const publishedError = yield* provider.streamEvents.pipe(
          Stream.filter((event) => event.eventId === errorEventId),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* advanceTestClock(50);

        fanout.codex.emit({
          type: "session.state.changed",
          eventId: errorEventId,
          provider: ProviderDriverKind.make("codex"),
          createdAt: "2026-01-01T00:00:10.000Z",
          threadId,
          turnId: turn.turnId,
          payload: {
            state: "error",
            reason: "Windows sandbox setup failed",
          },
        });

        const publishedEvents = Array.from(yield* Fiber.join(publishedError));
        assert.equal(publishedEvents[0]?.type, "session.state.changed");
        assert.equal(fanout.codex.sessions.has(threadId), true);
        const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
        const runtimePayload = binding.runtimePayload as {
          readonly activeTurnId?: unknown;
          readonly lastError?: unknown;
          readonly lastRuntimeEvent?: unknown;
          readonly lastTerminalTurnId?: unknown;
        };
        assert.equal(binding.status, "error");
        assert.equal(runtimePayload.activeTurnId, null);
        assert.equal(runtimePayload.lastError, "Windows sandbox setup failed");
        assert.equal(runtimePayload.lastRuntimeEvent, "session.state.changed");
        assert.equal(runtimePayload.lastTerminalTurnId, turn.turnId);
      }),
  );

  it.effect("ignores a stale session error after a replacement turn becomes idle", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-runtime-stale-state-error");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const staleTurn = yield* provider.sendTurn({
        threadId,
        input: "old provider turn",
        attachments: [],
      });
      const replacementTurnId = asTurnId("turn-runtime-state-error-replacement");
      fanout.codex.sendTurn.mockImplementationOnce((input) =>
        Effect.succeed({ threadId: input.threadId, turnId: replacementTurnId }),
      );
      const replacementTurn = yield* provider.sendTurn({
        threadId,
        input: "replacement provider turn",
        attachments: [],
      });

      const replacementOutputEventId = asEventId("evt-runtime-state-error-replacement-output");
      const replacementOutputPublished = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.eventId === replacementOutputEventId),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      fanout.codex.emit({
        type: "content.delta",
        eventId: replacementOutputEventId,
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:10.000Z",
        threadId,
        turnId: replacementTurn.turnId,
        itemId: "item-runtime-state-error-replacement-output",
        payload: { streamKind: "assistant_text", delta: "replacement output" },
      });
      yield* Fiber.join(replacementOutputPublished);
      fanout.codex.emit({
        type: "turn.completed",
        eventId: asEventId("evt-runtime-state-error-replacement-completed"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:11.000Z",
        threadId,
        turnId: replacementTurn.turnId,
        payload: { state: "completed" },
      });
      yield* advanceTestClock(500);

      const staleEventId = asEventId("evt-runtime-stale-state-error");
      const sentinelEventId = asEventId("evt-runtime-after-stale-state-error");
      const published = yield* provider.streamEvents.pipe(
        Stream.filter(
          (event) => event.eventId === staleEventId || event.eventId === sentinelEventId,
        ),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* advanceTestClock(50);
      fanout.codex.emit({
        type: "session.state.changed",
        eventId: staleEventId,
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:12.000Z",
        threadId,
        turnId: staleTurn.turnId,
        payload: { state: "error", reason: "old provider turn failed late" },
      });
      fanout.codex.emit({
        type: "session.started",
        eventId: sentinelEventId,
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:13.000Z",
        threadId,
        payload: {},
      });
      const publishedEvents = Array.from(yield* Fiber.join(published));
      assert.equal(publishedEvents[0]?.eventId, sentinelEventId);

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload = binding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly lastError?: unknown;
        readonly lastRuntimeEvent?: unknown;
        readonly lastTerminalTurnId?: unknown;
      };
      assert.equal(binding.status, "running");
      assert.equal(runtimePayload.activeTurnId, null);
      assert.equal(runtimePayload.lastError, undefined);
      assert.equal(runtimePayload.lastRuntimeEvent, "turn.completed");
      assert.equal(runtimePayload.lastTerminalTurnId, replacementTurn.turnId);
    }),
  );

  it.effect("fails the runtime binding on an untagged provider exit mid-turn", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-runtime-process-exit");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const session = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId,
        input: "run until the provider dies",
        attachments: [],
      });
      fanout.codex.sessions.delete(threadId);

      fanout.codex.emit({
        type: "session.exited",
        eventId: asEventId("evt-runtime-process-exited"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:10.000Z",
        threadId: session.threadId,
        turnId: turn.turnId,
        payload: {
          reason: "provider process exited mid-turn",
        },
      });
      yield* advanceTestClock(500);

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload = binding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly lastError?: unknown;
        readonly lastRuntimeEvent?: unknown;
        readonly lastTerminalTurnId?: unknown;
      };
      assert.equal(binding.status, "error");
      assert.equal(runtimePayload.activeTurnId, null);
      assert.equal(runtimePayload.lastError, "provider process exited mid-turn");
      assert.equal(runtimePayload.lastRuntimeEvent, "session.exited");
      assert.equal(runtimePayload.lastTerminalTurnId, turn.turnId);
    }),
  );

  it.effect("ignores an uncorrelated session exit when the liveness probe fails", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-runtime-exit-liveness-unknown");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const session = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId,
        input: "keep the current owner on uncertain exit",
        attachments: [],
      });
      fanout.codex.sessions.delete(threadId);
      fanout.codex.hasSession.mockImplementationOnce(() =>
        Effect.die("transient provider liveness defect"),
      );

      fanout.codex.emit({
        type: "session.exited",
        eventId: asEventId("evt-runtime-exit-liveness-unknown"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:10.000Z",
        threadId: session.threadId,
        payload: {
          exitKind: "error",
          reason: "uncorrelated old process exit",
        },
      });
      yield* advanceTestClock(500);

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload = binding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly lastRuntimeEvent?: unknown;
        readonly unconfirmedSessionExit?: unknown;
      };
      assert.equal(binding.status, "running");
      assert.equal(runtimePayload.activeTurnId, turn.turnId);
      assert.notEqual(runtimePayload.lastRuntimeEvent, "session.exited");
      assert.deepEqual(runtimePayload.unconfirmedSessionExit, {
        eventId: "evt-runtime-exit-liveness-unknown",
        observedAt: "2026-01-01T00:00:10.000Z",
        reason: "uncorrelated old process exit",
      });
    }),
  );

  it.effect("bounds a hung session-exit liveness probe and records it as unconfirmed", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-runtime-exit-liveness-timeout");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const session = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId,
        input: "do not block runtime ingestion on a hung liveness probe",
        attachments: [],
      });
      fanout.codex.sessions.delete(threadId);
      fanout.codex.hasSession.mockImplementationOnce(() => Effect.never);
      yield* advanceTestClock(10);

      fanout.codex.emit({
        type: "session.exited",
        eventId: asEventId("evt-runtime-exit-liveness-timeout"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:10.000Z",
        threadId: session.threadId,
        payload: {
          exitKind: "error",
          reason: "provider liveness probe did not answer",
        },
      });
      yield* advanceTestClock(500);

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload = binding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly lastRuntimeEvent?: unknown;
        readonly unconfirmedSessionExit?: unknown;
      };
      assert.equal(binding.status, "running");
      assert.equal(runtimePayload.activeTurnId, turn.turnId);
      assert.notEqual(runtimePayload.lastRuntimeEvent, "session.exited");
      assert.deepEqual(runtimePayload.unconfirmedSessionExit, {
        eventId: "evt-runtime-exit-liveness-timeout",
        observedAt: "2026-01-01T00:00:10.000Z",
        reason: "provider liveness probe did not answer",
      });
    }),
  );

  it.effect("accepts a genuine exact-turn exit after asynchronous adapter cleanup", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-runtime-delayed-exit-cleanup");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId,
        input: "wait for delayed adapter cleanup",
        attachments: [],
      });
      fanout.codex.hasSession.mockImplementationOnce(() => Effect.succeed(true));
      fanout.codex.hasSession.mockImplementationOnce(() => Effect.succeed(true));
      fanout.codex.hasSession.mockImplementationOnce(() =>
        Effect.sync(() => {
          fanout.codex.sessions.delete(threadId);
          return false;
        }),
      );

      fanout.codex.emit({
        type: "session.exited",
        eventId: asEventId("evt-runtime-delayed-exit-cleanup"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:10.000Z",
        threadId,
        turnId: turn.turnId,
        payload: {
          exitKind: "error",
          reason: "provider cleanup completed asynchronously",
        },
      });
      yield* advanceTestClock(500);

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload = binding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly lastError?: unknown;
        readonly lastRuntimeEvent?: unknown;
        readonly lastTerminalTurnId?: unknown;
      };
      assert.equal(binding.status, "error");
      assert.equal(runtimePayload.activeTurnId, null);
      assert.equal(runtimePayload.lastError, "provider cleanup completed asynchronously");
      assert.equal(runtimePayload.lastRuntimeEvent, "session.exited");
      assert.equal(runtimePayload.lastTerminalTurnId, turn.turnId);
    }),
  );

  it.effect("ignores an untagged stale exit from an older turn", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-runtime-stale-process-exit");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const session = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const staleTurn = yield* provider.sendTurn({
        threadId,
        input: "old provider turn",
        attachments: [],
      });
      const replacementTurnId = asTurnId("turn-runtime-stale-process-replacement");
      fanout.codex.sendTurn.mockImplementationOnce((input) =>
        Effect.succeed({ threadId: input.threadId, turnId: replacementTurnId }),
      );
      const replacementTurn = yield* provider.sendTurn({
        threadId,
        input: "replacement provider turn",
        attachments: [],
      });

      fanout.codex.emit({
        type: "session.exited",
        eventId: asEventId("evt-runtime-stale-process-exited"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:10.000Z",
        threadId: session.threadId,
        turnId: staleTurn.turnId,
        payload: {
          exitKind: "error",
          reason: "old provider process exited late",
        },
      });
      yield* advanceTestClock(500);

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(binding.status, "running");
      assert.equal(
        (binding.runtimePayload as { readonly activeTurnId?: unknown }).activeTurnId,
        replacementTurn.turnId,
      );

      fanout.codex.emit({
        type: "turn.completed",
        eventId: asEventId("evt-runtime-replacement-turn-completed"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:11.000Z",
        threadId: session.threadId,
        turnId: replacementTurn.turnId,
        payload: { state: "completed" },
      });
      yield* advanceTestClock(500);
      fanout.codex.sessions.delete(threadId);

      fanout.codex.emit({
        type: "session.exited",
        eventId: asEventId("evt-runtime-stale-process-exited-idle"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:12.000Z",
        threadId: session.threadId,
        turnId: staleTurn.turnId,
        payload: {
          exitKind: "error",
          reason: "old provider process exited after replacement went idle",
        },
      });
      yield* advanceTestClock(500);

      const idleBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(idleBinding.status, "running");
      const idlePayload = idleBinding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly lastRuntimeEvent?: unknown;
        readonly lastTerminalTurnId?: unknown;
      };
      assert.equal(idlePayload.activeTurnId, null);
      assert.equal(idlePayload.lastRuntimeEvent, "turn.completed");
      assert.equal(idlePayload.lastTerminalTurnId, replacementTurn.turnId);
    }),
  );

  it.effect("defers an exact-turn exit while a same-turn session is still live", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-runtime-exact-turn-process-exit");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const session = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId,
        input: "provider exits before adapter cleanup completes",
        attachments: [],
      });

      fanout.codex.emit({
        type: "session.exited",
        eventId: asEventId("evt-runtime-exact-turn-process-exited"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:10.000Z",
        threadId: session.threadId,
        turnId: turn.turnId,
        payload: {
          exitKind: "error",
          reason: "provider process exited before adapter cleanup",
        },
      });
      yield* advanceTestClock(500);

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runtimePayload = binding.runtimePayload as {
        readonly activeTurnId?: unknown;
        readonly unconfirmedSessionExit?: unknown;
      };
      assert.equal(binding.status, "running");
      assert.equal(runtimePayload.activeTurnId, turn.turnId);
      assert.deepEqual(runtimePayload.unconfirmedSessionExit, {
        eventId: "evt-runtime-exact-turn-process-exited",
        observedAt: "2026-01-01T00:00:10.000Z",
        reason: "provider process exited before adapter cleanup",
      });
    }),
  );

  it.effect("stops an inactive session only while its full binding snapshot remains current", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-inactive-stop-binding-fence");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const stopInactiveSession = provider.stopInactiveSession;
      if (stopInactiveSession === undefined) {
        return assert.fail("Expected the live provider service to support fenced inactive stops");
      }
      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const expectedBinding = Option.getOrThrow(
        Option.fromUndefinedOr(
          (yield* directory.listBindings()).find((binding) => binding.threadId === threadId),
        ),
      );
      const stopsBefore = fanout.codex.stopSession.mock.calls.length;

      yield* advanceTestClock(1);
      yield* directory.upsert(expectedBinding);
      const refreshedBinding = Option.getOrThrow(
        Option.fromUndefinedOr(
          (yield* directory.listBindings()).find((binding) => binding.threadId === threadId),
        ),
      );
      assert.notEqual(refreshedBinding.lastSeenAt, expectedBinding.lastSeenAt);

      assert.equal(yield* stopInactiveSession({ threadId, expectedBinding }), false);
      assert.equal(fanout.codex.stopSession.mock.calls.length, stopsBefore);
      assert.notEqual(Option.getOrThrow(yield* directory.getBinding(threadId)).status, "stopped");

      assert.equal(
        yield* stopInactiveSession({ threadId, expectedBinding: refreshedBinding }),
        true,
      );
      assert.equal(fanout.codex.stopSession.mock.calls.length, stopsBefore + 1);
      assert.equal(Option.getOrThrow(yield* directory.getBinding(threadId)).status, "stopped");
    }),
  );

  it.effect("serializes a fenced inactive stop against a replacement start", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-inactive-stop-replacement-race");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const stopInactiveSession = provider.stopInactiveSession;
      if (stopInactiveSession === undefined) {
        return assert.fail("Expected the live provider service to support fenced inactive stops");
      }
      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const expectedBinding = Option.getOrThrow(
        Option.fromUndefinedOr(
          (yield* directory.listBindings()).find((binding) => binding.threadId === threadId),
        ),
      );
      const previousOwnershipId = (
        expectedBinding.runtimePayload as { readonly sessionOwnershipId?: unknown }
      ).sessionOwnershipId;
      const stopEntered = yield* Deferred.make<void>();
      const allowStop = yield* Deferred.make<void>();
      fanout.codex.stopSession.mockImplementationOnce(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(stopEntered, undefined);
          yield* Deferred.await(allowStop);
        }),
      );

      const stopFiber = yield* stopInactiveSession({ threadId, expectedBinding }).pipe(
        Effect.forkChild,
      );
      yield* Deferred.await(stopEntered);
      const replacementFiber = yield* provider
        .startSession(threadId, {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: codexInstanceId,
          threadId,
          runtimeMode: "full-access",
        })
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Deferred.succeed(allowStop, undefined);

      assert.equal(yield* Fiber.join(stopFiber), true);
      yield* Fiber.join(replacementFiber);
      const replacementBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(replacementBinding.status, "running");
      assert.notEqual(
        (replacementBinding.runtimePayload as { readonly sessionOwnershipId?: unknown })
          .sessionOwnershipId,
        previousOwnershipId,
      );
    }),
  );

  it.effect("serializes a forced stale failure projection against a replacement start", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-force-fail-replacement-race");
      const turnId = asTurnId("turn-force-fail-replacement-race");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const forceFailStaleSession = provider.forceFailStaleSession;
      if (forceFailStaleSession === undefined) {
        return assert.fail("Expected the live provider service to support fenced force failures");
      }
      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const runningBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const runningPayload = runningBinding.runtimePayload as Record<string, unknown>;
      yield* directory.upsert({
        ...runningBinding,
        status: "error",
        runtimePayload: {
          ...runningPayload,
          activeTurnId: null,
          lastTerminalTurnId: turnId,
        },
      });
      const expectedBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const previousOwnershipId = (
        expectedBinding.runtimePayload as { readonly sessionOwnershipId?: unknown }
      ).sessionOwnershipId;
      const projectionEntered = yield* Deferred.make<void>();
      const allowProjection = yield* Deferred.make<void>();
      const order = yield* Ref.make<ReadonlyArray<string>>([]);

      const forceFiber = yield* forceFailStaleSession({
        threadId,
        turnId,
        expectedBinding,
        onOwned: Effect.gen(function* () {
          yield* Ref.update(order, (events) => [...events, "projection-started"]);
          yield* Deferred.succeed(projectionEntered, undefined);
          yield* Deferred.await(allowProjection);
          yield* Ref.update(order, (events) => [...events, "projection-finished"]);
        }),
        onSettled: directory
          .upsert({
            ...expectedBinding,
            status: "stopped",
            runtimePayload: {
              ...(expectedBinding.runtimePayload as Record<string, unknown>),
              activeTurnId: null,
              lastTerminalTurnId: turnId,
            },
          })
          .pipe(Effect.tap(() => Ref.update(order, (events) => [...events, "settled"]))),
      }).pipe(Effect.forkChild);
      yield* Deferred.await(projectionEntered);
      const replacementFiber = yield* provider
        .startSession(threadId, {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: codexInstanceId,
          threadId,
          runtimeMode: "full-access",
        })
        .pipe(
          Effect.tap(() => Ref.update(order, (events) => [...events, "replacement-started"])),
          Effect.forkChild,
        );
      yield* Effect.yieldNow;
      yield* Deferred.succeed(allowProjection, undefined);

      assert.equal(yield* Fiber.join(forceFiber), true);
      yield* Fiber.join(replacementFiber);
      assert.deepEqual(yield* Ref.get(order), [
        "projection-started",
        "projection-finished",
        "settled",
        "replacement-started",
      ]);
      const replacementBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(replacementBinding.status, "running");
      assert.notEqual(
        (replacementBinding.runtimePayload as { readonly sessionOwnershipId?: unknown })
          .sessionOwnershipId,
        previousOwnershipId,
      );
    }),
  );

  it.effect("does not stop a replacement session when failed cleanup loses the start race", () =>
    Effect.gen(function* () {
      fanout.codex.stopSession.mockClear();
      const threadId = asThreadId("thread-failed-cleanup-replacement-race");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const failedTurn = yield* provider.sendTurn({
        threadId,
        input: "fail before replacement",
        attachments: [],
      });
      const currentBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      const failedSessionOwnershipId = (
        currentBinding.runtimePayload as { readonly sessionOwnershipId?: unknown }
      ).sessionOwnershipId;
      assert.equal(typeof failedSessionOwnershipId, "string");
      yield* directory.upsert({
        ...currentBinding,
        status: "error",
        runtimePayload: {
          ...(currentBinding.runtimePayload as Record<string, unknown>),
          activeTurnId: null,
          lastTerminalTurnId: failedTurn.turnId,
          lastError: "failed cleanup remains pending",
        },
      });

      const replacementStartEntered = yield* Deferred.make<void>();
      const allowReplacementStart = yield* Deferred.make<void>();
      const defaultStartSession = fanout.codex.startSession.getMockImplementation();
      assert.ok(defaultStartSession);
      fanout.codex.startSession.mockImplementationOnce((input) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(replacementStartEntered, undefined);
          yield* Deferred.await(allowReplacementStart);
          return yield* defaultStartSession(input);
        }),
      );
      let staleOwnedCallbackRan = false;
      let staleStoppedCallbackRan = false;

      const replacementFiber = yield* provider
        .startSession(threadId, {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: codexInstanceId,
          threadId,
          runtimeMode: "full-access",
          activeTurnId: failedTurn.turnId,
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(replacementStartEntered);
      const cleanupFiber = yield* provider
        .stopFailedSession({
          threadId,
          turnId: failedTurn.turnId,
          reason: "failed cleanup remains pending",
          sessionOwnershipId: failedSessionOwnershipId as string,
          onOwned: Effect.sync(() => {
            staleOwnedCallbackRan = true;
          }),
          onStopped: Effect.sync(() => {
            staleStoppedCallbackRan = true;
          }),
        })
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Deferred.succeed(allowReplacementStart, undefined);
      yield* Fiber.join(replacementFiber);

      assert.equal(yield* Fiber.join(cleanupFiber), false);
      assert.equal(staleOwnedCallbackRan, false);
      assert.equal(staleStoppedCallbackRan, false);
      assert.equal(fanout.codex.stopSession.mock.calls.length, 0);
      const replacementBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(replacementBinding.status, "running");
      assert.equal(
        (replacementBinding.runtimePayload as { readonly activeTurnId?: unknown }).activeTurnId,
        failedTurn.turnId,
      );

      yield* provider.stopSession({ threadId });
      let stoppedReplacementCallbackRan = false;
      const stoppedReplacementClaimed = yield* provider.stopFailedSession({
        threadId,
        turnId: failedTurn.turnId,
        reason: "late cleanup after replacement stopped",
        sessionOwnershipId: failedSessionOwnershipId as string,
        onOwned: Effect.sync(() => {
          stoppedReplacementCallbackRan = true;
        }),
        onStopped: Effect.void,
      });
      assert.equal(stoppedReplacementClaimed, false);
      assert.equal(stoppedReplacementCallbackRan, false);
      const stoppedReplacementBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(stoppedReplacementBinding.status, "stopped");
    }),
  );

  it.effect("accepts only an exact active-turn match for legacy failed-session cleanup", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-legacy-active-turn-cleanup");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId,
        input: "legacy provider turn",
        attachments: [],
      });
      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      yield* directory.upsert({
        ...binding,
        runtimePayload: {
          ...(binding.runtimePayload as Record<string, unknown>),
          sessionOwnershipId: null,
        },
      });

      let ownedCallbackRan = false;
      const staleClaimed = yield* provider.stopFailedSession({
        threadId,
        turnId: asTurnId("turn-other-legacy-owner"),
        reason: "stale legacy cleanup",
        allowLegacyActiveTurnMatch: true,
        onOwned: Effect.sync(() => {
          ownedCallbackRan = true;
        }),
        onStopped: Effect.void,
      });
      assert.equal(staleClaimed, false);
      assert.equal(ownedCallbackRan, false);
      assert.equal(fanout.codex.sessions.has(threadId), true);

      let stoppedCallbackRan = false;
      const exactClaimed = yield* provider.stopFailedSession({
        threadId,
        turnId: turn.turnId,
        reason: "legacy approval timed out",
        allowLegacyActiveTurnMatch: true,
        onOwned: Effect.sync(() => {
          ownedCallbackRan = true;
        }),
        onStopped: Effect.sync(() => {
          stoppedCallbackRan = true;
        }),
      });
      assert.equal(exactClaimed, true);
      assert.equal(ownedCallbackRan, true);
      assert.equal(stoppedCallbackRan, true);
      assert.equal(fanout.codex.sessions.has(threadId), false);
      assert.equal(Option.getOrThrow(yield* directory.getBinding(threadId)).status, "stopped");
    }),
  );

  it.effect("requires turn ownership when a provider instance is definitively removed", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-removed-instance-cleanup");
      const turnId = asTurnId("turn-removed-instance-cleanup");
      const staleTurnId = asTurnId("turn-stale-removed-instance-cleanup");
      const removedInstanceId = ProviderInstanceId.make("codex_removed");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      yield* directory.upsert({
        threadId,
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: removedInstanceId,
        runtimeMode: "full-access",
        status: "running",
        runtimePayload: {
          activeTurnId: turnId,
          sessionOwnershipId: null,
        },
      });
      let ownedCallbackRuns = 0;
      let stoppedCallbackRuns = 0;
      const codexStopsBefore = fanout.codex.stopSession.mock.calls.length;

      const staleActiveClaimed = yield* provider.stopFailedSession({
        threadId,
        turnId: staleTurnId,
        reason: "stale cleanup after configured provider instance was removed",
        requireSessionAbsent: true,
        onOwned: Effect.sync(() => {
          ownedCallbackRuns += 1;
        }),
        onStopped: Effect.sync(() => {
          stoppedCallbackRuns += 1;
        }),
      });
      assert.equal(staleActiveClaimed, false);
      assert.equal(ownedCallbackRuns, 0);
      assert.equal(stoppedCallbackRuns, 0);
      const activeBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(activeBinding.status, "running");
      assert.equal(
        (activeBinding.runtimePayload as { readonly activeTurnId?: unknown }).activeTurnId,
        turnId,
      );

      yield* directory.upsert({
        ...activeBinding,
        status: "error",
        runtimePayload: {
          ...(activeBinding.runtimePayload as Record<string, unknown>),
          activeTurnId: null,
          lastTerminalTurnId: turnId,
        },
      });
      const staleTerminalClaimed = yield* provider.stopFailedSession({
        threadId,
        turnId: staleTurnId,
        reason: "stale cleanup after the owned turn became terminal",
        requireSessionAbsent: true,
        onOwned: Effect.sync(() => {
          ownedCallbackRuns += 1;
        }),
        onStopped: Effect.sync(() => {
          stoppedCallbackRuns += 1;
        }),
      });
      assert.equal(staleTerminalClaimed, false);
      assert.equal(ownedCallbackRuns, 0);
      assert.equal(stoppedCallbackRuns, 0);
      const terminalBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(terminalBinding.status, "error");
      assert.equal(
        (terminalBinding.runtimePayload as { readonly lastTerminalTurnId?: unknown })
          .lastTerminalTurnId,
        turnId,
      );

      const claimed = yield* provider.stopFailedSession({
        threadId,
        turnId,
        reason: "configured provider instance was removed",
        requireSessionAbsent: true,
        onOwned: Effect.sync(() => {
          ownedCallbackRuns += 1;
        }),
        onStopped: Effect.sync(() => {
          stoppedCallbackRuns += 1;
        }),
      });

      assert.equal(claimed, true);
      assert.equal(ownedCallbackRuns, 1);
      assert.equal(stoppedCallbackRuns, 1);
      assert.equal(fanout.codex.stopSession.mock.calls.length, codexStopsBefore);
      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(binding.status, "stopped");
      assert.equal(
        (binding.runtimePayload as { readonly lastRuntimeEvent?: unknown }).lastRuntimeEvent,
        "provider.turn.watchdog.instance-absent",
      );
    }),
  );

  it.effect("defers failed-session cleanup when provider instance lookup is ambiguous", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-transient-instance-lookup");
      const turnId = asTurnId("turn-transient-instance-lookup");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      yield* directory.upsert({
        threadId,
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        runtimeMode: "full-access",
        status: "running",
        runtimePayload: { activeTurnId: turnId },
      });
      const lookup = vi
        .spyOn(fanout.registry, "getByInstance")
        .mockImplementationOnce(() => Effect.die("transient registry lookup failure") as never);
      let ownedCallbackRan = false;
      const cleanupExit = yield* provider
        .stopFailedSession({
          threadId,
          turnId,
          reason: "ambiguous provider instance lookup",
          requireSessionAbsent: true,
          onOwned: Effect.sync(() => {
            ownedCallbackRan = true;
          }),
          onStopped: Effect.void,
        })
        .pipe(Effect.exit);
      lookup.mockRestore();

      assert.equal(Exit.isFailure(cleanupExit), true);
      assert.equal(ownedCallbackRan, false);
      assert.equal(Option.getOrThrow(yield* directory.getBinding(threadId)).status, "running");
    }),
  );

  it.effect("requires absence and turn ownership for identity-free cleanup", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-failed-cleanup-absence-proof");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId,
        input: "keep the same-turn successor alive",
        attachments: [],
      });
      let ownedCallbackRan = false;

      const liveClaimFiber = yield* provider
        .stopFailedSession({
          threadId,
          turnId: turn.turnId,
          reason: "stale cleanup must not stop a live owner",
          requireSessionAbsent: true,
          onOwned: Effect.sync(() => {
            ownedCallbackRan = true;
          }),
          onStopped: Effect.void,
        })
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* advanceTestClock(250);
      const liveClaimed = yield* Fiber.join(liveClaimFiber);
      assert.equal(liveClaimed, false);
      assert.equal(ownedCallbackRan, false);
      assert.equal(fanout.codex.sessions.has(threadId), true);

      const bindingWithoutRuntimeTurn = Option.getOrThrow(yield* directory.getBinding(threadId));
      yield* directory.upsert({
        ...bindingWithoutRuntimeTurn,
        runtimePayload: {
          ...(bindingWithoutRuntimeTurn.runtimePayload as Record<string, unknown>),
          activeTurnId: null,
          lastTerminalTurnId: turn.turnId,
        },
      });
      fanout.codex.sessions.delete(threadId);
      const deadClaimFiber = yield* provider
        .stopFailedSession({
          threadId,
          turnId: turn.turnId,
          reason: "the exact turn has no provider session",
          requireSessionAbsent: true,
          onOwned: Effect.sync(() => {
            ownedCallbackRan = true;
          }),
          onStopped: Effect.void,
        })
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* advanceTestClock(50);
      const deadClaimed = yield* Fiber.join(deadClaimFiber);
      assert.equal(deadClaimed, true);
      assert.equal(ownedCallbackRan, true);
    }),
  );

  it.effect("clears MCP credentials when a provider session exits", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-mcp-exit");
      const provider = yield* ProviderService.ProviderService;
      McpProviderSession.setMcpProviderSession({
        environmentId,
        threadId,
        providerSessionId: "provider-session-mcp-exit",
        providerInstanceId: codexInstanceId,
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer test-mcp-token",
      });
      const session = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      fanout.codex.emit({
        type: "session.exited",
        eventId: asEventId("evt-mcp-session-exited"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        payload: {
          exitKind: "error",
          reason: "provider process exited",
          mcpProviderSessionId: "provider-session-mcp-exit",
        },
      });
      yield* advanceTestClock(500);

      assert.equal(McpProviderSession.readMcpProviderSession(session.threadId), undefined);
    }).pipe(
      Effect.ensuring(
        Effect.sync(() =>
          McpProviderSession.clearMcpProviderSession(asThreadId("thread-mcp-exit")),
        ),
      ),
    ),
  );

  it.effect("clears MCP credentials when a tagged exit arrives before tracking catches up", () =>
    Effect.gen(function* () {
      const codex = makeFakeCodexAdapter();
      codex.startSession.mockImplementation((input: ProviderSessionStartInput) =>
        Effect.gen(function* () {
          const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
          if (mcpSession) {
            codex.emit({
              type: "session.exited",
              eventId: asEventId("evt-mcp-exit-before-track"),
              provider: CODEX_DRIVER,
              createdAt: "2026-01-01T00:00:00.000Z",
              threadId: input.threadId,
              payload: {
                exitKind: "error",
                reason: "provider process exited before tracking completed",
                mcpProviderSessionId: mcpSession.providerSessionId,
              },
            });
          }
          yield* Effect.sleep("6000 millis");
          const now = "2026-01-01T00:00:00.000Z";
          const session: ProviderSession = {
            provider: CODEX_DRIVER,
            ...(input.providerInstanceId !== undefined
              ? { providerInstanceId: input.providerInstanceId }
              : {}),
            status: "ready",
            runtimeMode: input.runtimeMode,
            threadId: input.threadId,
            resumeCursor: input.resumeCursor ?? {
              opaque: `resume-${String(input.threadId)}`,
            },
            cwd: input.cwd ?? process.cwd(),
            createdAt: now,
            updatedAt: now,
          };
          codex.sessions.set(session.threadId, session);
          return session;
        }),
      );
      const registry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("codex")]: codex.adapter,
      });
      const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
        Layer.provide(SqlitePersistenceMemory),
      );
      const directoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const providerLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
        Layer.provide(directoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(serverConfigTestLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );

      yield* Effect.gen(function* () {
        const threadId = asThreadId("thread-mcp-exit-before-track");
        const provider = yield* ProviderService.ProviderService;
        McpProviderSession.setMcpProviderSession({
          environmentId,
          threadId,
          providerSessionId: "provider-session-mcp-exit-before-track",
          providerInstanceId: codexInstanceId,
          endpoint: "http://127.0.0.1:43123/mcp",
          authorizationHeader: "Bearer mcp-exit-before-track-token",
        });

        const startFiber = yield* provider
          .startSession(threadId, {
            provider: CODEX_DRIVER,
            providerInstanceId: codexInstanceId,
            threadId,
            runtimeMode: "full-access",
          })
          .pipe(Effect.forkScoped);

        yield* advanceTestClock(6_500);
        yield* Fiber.join(startFiber);
        yield* advanceTestClock(1_000);

        assert.equal(McpProviderSession.readMcpProviderSession(threadId), undefined);
      }).pipe(
        Effect.ensuring(
          Effect.sync(() =>
            McpProviderSession.clearMcpProviderSession(asThreadId("thread-mcp-exit-before-track")),
          ),
        ),
        Effect.provide(
          Layer.mergeAll(providerLayer, directoryLayer, runtimeRepositoryLayer, NodeServices.layer),
        ),
      );
    }),
  );

  it.effect("keeps MCP credentials when a provider session exit lacks a session id", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-mcp-untagged-exit");
      const provider = yield* ProviderService.ProviderService;
      McpProviderSession.setMcpProviderSession({
        environmentId,
        threadId,
        providerSessionId: "provider-session-mcp-untagged-exit",
        providerInstanceId: codexInstanceId,
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer untagged-mcp-token",
      });
      const session = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      fanout.codex.emit({
        type: "session.exited",
        eventId: asEventId("evt-mcp-untagged-session-exited"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        payload: { exitKind: "error", reason: "legacy untagged exit" },
      });
      yield* advanceTestClock(500);

      assert.equal(
        McpProviderSession.readMcpProviderSession(session.threadId)?.providerSessionId,
        "provider-session-mcp-untagged-exit",
      );
    }).pipe(
      Effect.ensuring(
        Effect.sync(() =>
          McpProviderSession.clearMcpProviderSession(asThreadId("thread-mcp-untagged-exit")),
        ),
      ),
    ),
  );

  it.effect("keeps replacement MCP credentials after a stale same-adapter session exit", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-mcp-replacement");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      McpProviderSession.setMcpProviderSession({
        environmentId,
        threadId,
        providerSessionId: "provider-session-mcp-old",
        providerInstanceId: codexInstanceId,
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer old-mcp-token",
      });
      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      McpProviderSession.setMcpProviderSession({
        environmentId,
        threadId,
        providerSessionId: "provider-session-mcp-replacement",
        providerInstanceId: codexInstanceId,
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer replacement-mcp-token",
      });
      const session = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const replacementTurn = yield* provider.sendTurn({
        threadId,
        input: "keep the replacement turn running",
        attachments: [],
      });

      fanout.codex.emit({
        type: "session.exited",
        eventId: asEventId("evt-mcp-stale-session-exited"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        payload: {
          exitKind: "graceful",
          reason: "old provider session stopped",
          mcpProviderSessionId: "provider-session-mcp-old",
        },
      });
      yield* advanceTestClock(500);

      assert.equal(
        McpProviderSession.readMcpProviderSession(session.threadId)?.providerSessionId,
        "provider-session-mcp-replacement",
      );
      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(binding.status, "running");
      assert.equal(
        (binding.runtimePayload as { readonly activeTurnId?: unknown }).activeTurnId,
        replacementTurn.turnId,
      );
    }).pipe(
      Effect.ensuring(
        Effect.sync(() =>
          McpProviderSession.clearMcpProviderSession(asThreadId("thread-mcp-replacement")),
        ),
      ),
    ),
  );

  it.effect("accepts the current MCP session exit while replacement binding persistence lags", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-mcp-current-exit-before-rebind");
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      McpProviderSession.setMcpProviderSession({
        environmentId,
        threadId,
        providerSessionId: "provider-session-mcp-before-rebind-old",
        providerInstanceId: codexInstanceId,
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer old-before-rebind-token",
      });
      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      // prepareMcpSession advances the in-memory identity before the external
      // provider start returns and the directory binding can be refreshed.
      McpProviderSession.setMcpProviderSession({
        environmentId,
        threadId,
        providerSessionId: "provider-session-mcp-before-rebind-current",
        providerInstanceId: codexInstanceId,
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer current-before-rebind-token",
      });
      yield* advanceTestClock(10);
      const beforeExitBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(
        (beforeExitBinding.runtimePayload as { readonly mcpProviderSessionId?: unknown })
          .mcpProviderSessionId,
        "provider-session-mcp-before-rebind-old",
      );
      assert.equal(
        McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId,
        "provider-session-mcp-before-rebind-current",
      );
      fanout.codex.emit({
        type: "session.exited",
        eventId: asEventId("evt-mcp-current-session-exited-before-rebind"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        payload: {
          exitKind: "error",
          reason: "replacement provider exited before binding caught up",
          mcpProviderSessionId: "provider-session-mcp-before-rebind-current",
        },
      });
      yield* Effect.yieldNow;
      let binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      for (let attempt = 0; attempt < 10 && binding.status !== "error"; attempt += 1) {
        yield* advanceTestClock(250);
        binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      }
      assert.equal(binding.status, "error");
      assert.equal(
        (binding.runtimePayload as { readonly activeTurnId?: unknown }).activeTurnId,
        null,
      );
    }).pipe(
      Effect.ensuring(
        Effect.sync(() =>
          McpProviderSession.clearMcpProviderSession(
            asThreadId("thread-mcp-current-exit-before-rebind"),
          ),
        ),
      ),
    ),
  );

  it.effect("keeps replacement MCP credentials after a stale stopped state event", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-mcp-stopped-state");
      const provider = yield* ProviderService.ProviderService;
      McpProviderSession.setMcpProviderSession({
        environmentId,
        threadId,
        providerSessionId: "provider-session-mcp-stopped-old",
        providerInstanceId: codexInstanceId,
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer old-stopped-mcp-token",
      });
      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      McpProviderSession.setMcpProviderSession({
        environmentId,
        threadId,
        providerSessionId: "provider-session-mcp-stopped-replacement",
        providerInstanceId: codexInstanceId,
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer replacement-stopped-mcp-token",
      });
      const session = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      fanout.codex.emit({
        type: "session.state.changed",
        eventId: asEventId("evt-mcp-stale-stopped-state"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        payload: { state: "stopped", reason: "old provider session stopped" },
      });
      yield* advanceTestClock(500);

      assert.equal(
        McpProviderSession.readMcpProviderSession(session.threadId)?.providerSessionId,
        "provider-session-mcp-stopped-replacement",
      );
    }).pipe(
      Effect.ensuring(
        Effect.sync(() =>
          McpProviderSession.clearMcpProviderSession(asThreadId("thread-mcp-stopped-state")),
        ),
      ),
    ),
  );

  it.effect("keeps subscriber delivery ordered and isolates failing subscribers", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const session = yield* provider.startSession(asThreadId("thread-1"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-1"),
        runtimeMode: "full-access",
      });

      const receivedByHealthy: string[] = [];
      const expectedEventIds = new Set<string>(["evt-ordered-1", "evt-ordered-2", "evt-ordered-3"]);
      const healthyFiber = yield* Stream.take(provider.streamEvents, 3).pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            receivedByHealthy.push(event.eventId);
          }),
        ),
        Effect.forkChild,
      );
      const failingFiber = yield* Stream.take(provider.streamEvents, 1).pipe(
        Stream.runForEach(() => Effect.fail("listener crash")),
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      const events: ReadonlyArray<LegacyProviderRuntimeEvent> = [
        {
          type: "tool.completed",
          eventId: asEventId("evt-ordered-1"),
          provider: ProviderDriverKind.make("codex"),
          createdAt: "2026-01-01T00:00:00.000Z",
          threadId: session.threadId,
          turnId: asTurnId("turn-1"),
          toolKind: "command",
          title: "Ran command",
          detail: "echo one",
        },
        {
          type: "message.delta",
          eventId: asEventId("evt-ordered-2"),
          provider: ProviderDriverKind.make("codex"),
          createdAt: "2026-01-01T00:00:00.000Z",
          threadId: session.threadId,
          turnId: asTurnId("turn-1"),
          delta: "hello",
        },
        {
          type: "turn.completed",
          eventId: asEventId("evt-ordered-3"),
          provider: ProviderDriverKind.make("codex"),
          createdAt: "2026-01-01T00:00:00.000Z",
          threadId: session.threadId,
          turnId: asTurnId("turn-1"),
          status: "completed",
        },
      ];

      for (const event of events) {
        fanout.codex.emit(event);
      }
      const failingResult = yield* Effect.result(Fiber.join(failingFiber));
      assert.equal(failingResult._tag, "Failure");
      yield* Fiber.join(healthyFiber);

      assert.deepEqual(
        receivedByHealthy.filter((eventId) => expectedEventIds.has(eventId)).slice(0, 3),
        ["evt-ordered-1", "evt-ordered-2", "evt-ordered-3"],
      );
    }),
  );

  it.effect("records provider metrics with the routed provider label", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const session = yield* provider.startSession(asThreadId("thread-metrics"), {
        provider: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claudeAgentInstanceId,
        threadId: asThreadId("thread-metrics"),
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      });

      yield* provider.interruptTurn({ threadId: session.threadId });
      yield* provider.respondToRequest({
        threadId: session.threadId,
        requestId: asRequestId("req-metrics-1"),
        decision: "accept",
      });
      yield* provider.respondToUserInput({
        threadId: session.threadId,
        requestId: asRequestId("req-metrics-2"),
        answers: {
          sandbox_mode: "workspace-write",
        },
      });
      yield* provider.rollbackConversation({
        threadId: session.threadId,
        numTurns: 1,
      });
      yield* provider.stopSession({ threadId: session.threadId });

      const snapshots = yield* Metric.snapshot;

      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
          provider: ProviderDriverKind.make("claudeAgent"),
          operation: "interrupt",
          outcome: "success",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
          provider: ProviderDriverKind.make("claudeAgent"),
          operation: "approval-response",
          outcome: "success",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
          provider: ProviderDriverKind.make("claudeAgent"),
          operation: "user-input-response",
          outcome: "success",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
          provider: ProviderDriverKind.make("claudeAgent"),
          operation: "rollback",
          outcome: "success",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_sessions_total", {
          provider: ProviderDriverKind.make("claudeAgent"),
          operation: "stop",
          outcome: "success",
        }),
        true,
      );
    }),
  );

  it.effect(
    "records sendTurn metrics with the resolved provider when modelSelection is omitted",
    () =>
      Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;

        const session = yield* provider.startSession(asThreadId("thread-send-metrics"), {
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: claudeAgentInstanceId,
          threadId: asThreadId("thread-send-metrics"),
          cwd: "/tmp/project-send-metrics",
          runtimeMode: "full-access",
        });

        yield* provider.sendTurn({
          threadId: session.threadId,
          input: "hello",
          attachments: [],
        });

        const snapshots = yield* Metric.snapshot;

        assert.equal(
          hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
            provider: ProviderDriverKind.make("claudeAgent"),
            operation: "send",
            outcome: "success",
          }),
          true,
        );
        assert.equal(
          hasMetricSnapshot(snapshots, "t3_provider_turn_duration", {
            provider: ProviderDriverKind.make("claudeAgent"),
            operation: "send",
          }),
          true,
        );
      }),
  );
});

grokFanout.layer("ProviderServiceLive Grok output guard", (it) => {
  it.effect.each<MeaningfulOutputCase>([
    {
      name: "Grok plan update",
      provider: GROK_DRIVER,
      providerInstanceId: grokInstanceId,
      output: {
        type: "turn.plan.updated",
        payload: { plan: [{ step: "Inspect", status: "completed" }] },
      },
    },
    {
      name: "Grok tool result",
      provider: GROK_DRIVER,
      providerInstanceId: grokInstanceId,
      output: {
        type: "item.completed",
        itemId: "grok-tool-item",
        payload: { itemType: "dynamic_tool_call", status: "completed" },
      },
    },
  ])("preserves successful $name-only turns", (testCase) =>
    assertSuccessfulOutputOnlyTurn(testCase, grokOutputAdapter),
  );
});

const validation = makeProviderServiceLayer();
validation.layer("ProviderServiceLive validation", (it) => {
  it.effect("rejects session starts without an explicit provider instance id", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      validation.codex.startSession.mockClear();
      const failure = yield* Effect.flip(
        provider.startSession(asThreadId("thread-missing-instance-id"), {
          provider: ProviderDriverKind.make("codex"),
          threadId: asThreadId("thread-missing-instance-id"),
          runtimeMode: "full-access",
        }),
      );

      assert.instanceOf(failure, ProviderValidationError);
      assert.include(failure.issue, "Provider instance id is required for provider 'codex'.");
      assert.equal(validation.codex.startSession.mock.calls.length, 0);
    }),
  );

  it.effect("rejects mismatched provider kind and provider instance id", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      validation.codex.startSession.mockClear();
      validation.claude.startSession.mockClear();
      const failure = yield* Effect.flip(
        provider.startSession(asThreadId("thread-instance-mismatch"), {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: claudeAgentInstanceId,
          threadId: asThreadId("thread-instance-mismatch"),
          runtimeMode: "full-access",
        }),
      );

      assert.instanceOf(failure, ProviderValidationError);
      assert.include(
        failure.issue,
        "Provider instance 'claudeAgent' belongs to driver 'claudeAgent', not 'codex'.",
      );
      assert.equal(validation.codex.startSession.mock.calls.length, 0);
      assert.equal(validation.claude.startSession.mock.calls.length, 0);
    }),
  );

  it.effect("returns ProviderValidationError for invalid input payloads", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const failure = yield* Effect.result(
        provider.startSession(asThreadId("thread-validation"), {
          threadId: asThreadId("thread-validation"),
          provider: "invalid-provider",
          runtimeMode: "full-access",
        } as never),
      );

      assert.equal(failure._tag, "Failure");
      if (failure._tag !== "Failure") {
        return;
      }
      assert.equal(failure.failure._tag, "ProviderValidationError");
      if (failure.failure._tag !== "ProviderValidationError") {
        return;
      }
      assert.equal(failure.failure.operation, "ProviderService.startSession");
      assert.equal(failure.failure.issue.includes("invalid-provider"), true);
    }),
  );

  it.effect("accepts startSession when adapter has not emitted provider thread id yet", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

      validation.codex.startSession.mockImplementationOnce((input: ProviderSessionStartInput) =>
        Effect.sync(() => {
          const now = "2026-01-01T00:00:00.000Z";
          return {
            provider: ProviderDriverKind.make("codex"),
            status: "ready",
            threadId: input.threadId,
            runtimeMode: input.runtimeMode,
            cwd: input.cwd ?? process.cwd(),
            createdAt: now,
            updatedAt: now,
          } satisfies ProviderSession;
        }),
      );

      const session = yield* provider.startSession(asThreadId("thread-missing"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-missing"),
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      });

      assert.equal(session.threadId, asThreadId("thread-missing"));

      const runtime = yield* runtimeRepository.getByThreadId({
        threadId: session.threadId,
      });
      assert.equal(Option.isSome(runtime), true);
      if (Option.isSome(runtime)) {
        assert.equal(runtime.value.threadId, session.threadId);
      }
    }),
  );
});
