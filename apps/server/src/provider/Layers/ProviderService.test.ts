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
  ProviderUnsupportedError,
  ProviderValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../Services/ProviderAdapterRegistry.ts";
import * as ProviderService from "../Services/ProviderService.ts";
import * as ProviderSessionDirectory from "../Services/ProviderSessionDirectory.ts";
import { makeProviderServiceLive, type ProviderServiceLiveOptions } from "./ProviderService.ts";
import * as ProviderEventLoggers from "./ProviderEventLoggers.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import {
  makeSqlitePersistenceLive,
  SqlitePersistenceMemory,
} from "../../persistence/Layers/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as AnalyticsService from "../../telemetry/AnalyticsService.ts";
import { makeAdapterRegistryMock } from "../testUtils/providerAdapterRegistryMock.ts";

const defaultServerSettingsLayer = ServerSettings.ServerSettingsService.layerTest();

const asRequestId = (value: string): ApprovalRequestId => ApprovalRequestId.make(value);
const asEventId = (value: string): EventId => EventId.make(value);
const asThreadId = (value: string): ThreadId => ThreadId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);
const environmentId = EnvironmentId.make("environment-provider-service-test");
const codexInstanceId = ProviderInstanceId.make("codex");
const claudeAgentInstanceId = ProviderInstanceId.make("claudeAgent");
const cursorInstanceId = ProviderInstanceId.make("cursor");
const CODEX_DRIVER = ProviderDriverKind.make("codex");
const CLAUDE_AGENT_DRIVER = ProviderDriverKind.make("claudeAgent");
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

function makeProviderServiceLayer(options?: ProviderServiceLiveOptions) {
  const codex = makeFakeCodexAdapter();
  const claude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
  const cursor = makeFakeCodexAdapter(CURSOR_DRIVER);
  const registry = makeAdapterRegistryMock({
    [ProviderDriverKind.make("codex")]: codex.adapter,
    [ProviderDriverKind.make("claudeAgent")]: claude.adapter,
    [ProviderDriverKind.make("cursor")]: cursor.adapter,
  });

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
      yield* Fiber.join(activeCompletedFiber);

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
      assert.equal(runtimePayload.lastFailedSendTurnOperationId, null);
      assert.equal(runtimePayload.lastRuntimeEvent, "provider.sendTurn");
      assert.equal(runtimePayload.lastTerminalTurnId, null);
      assert.equal(runtimePayload.sendTurnOperationId, null);
      assert.equal(routing.codex.sessions.get(threadId)?.activeTurnId, existingTurn.turnId);
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

  it.effect("ignores replacement resume cursor state from stale adapter generations", () =>
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

        currentAdapter = secondCursor.adapter;
        yield* PubSub.publish(changes, undefined);
        yield* advanceTestClock(50);

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
        firstCursor.emit({
          type: "session.state.changed",
          eventId: asEventId("evt-stale-generation-cursor-resume-ready"),
          provider: CURSOR_DRIVER,
          createdAt: "2026-01-01T00:00:02.000Z",
          threadId,
          payload: {
            state: "ready",
            detail: { resumeCursor: staleResumeCursor },
          },
        });
        yield* advanceTestClock(50);

        const runtime = yield* runtimeRepository.getByThreadId({ threadId });
        assert.equal(Option.isSome(runtime), true);
        if (Option.isSome(runtime)) {
          assert.deepEqual(runtime.value.resumeCursor, currentResumeCursor);
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

fanout.layer("ProviderServiceLive fanout", (it) => {
  it.effect("fans out adapter turn completion events", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const session = yield* provider.startSession(asThreadId("thread-1"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-1"),
        runtimeMode: "full-access",
      });

      const eventsRef = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const consumer = yield* Stream.runForEach(provider.streamEvents, (event) =>
        Ref.update(eventsRef, (current) => [...current, event]),
      ).pipe(Effect.forkChild);
      yield* advanceTestClock(50);

      const completedEvent: LegacyProviderRuntimeEvent = {
        type: "turn.completed",
        eventId: asEventId("evt-1"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        status: "completed",
      };

      fanout.codex.emit(completedEvent);
      yield* advanceTestClock(50);

      const events = yield* Ref.get(eventsRef);
      yield* Fiber.interrupt(consumer);

      assert.equal(
        events.some((entry) => entry.type === "turn.completed"),
        true,
      );
      assert.equal(
        events.some(
          (entry) =>
            entry.type === "turn.completed" && entry.providerInstanceId === codexInstanceId,
        ),
        true,
      );
    }),
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

  it.effect("persists and publishes an owning adapter's live session error", () =>
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
      };
      assert.equal(binding.status, "error");
      assert.equal(runtimePayload.activeTurnId, null);
      assert.equal(runtimePayload.lastError, "Windows sandbox setup failed");
      assert.equal(runtimePayload.lastRuntimeEvent, "session.state.changed");
    }),
  );

  it.effect("marks the runtime binding failed when a provider process exits mid-turn", () =>
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
          exitKind: "error",
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
      assert.equal(
        (idleBinding.runtimePayload as { readonly activeTurnId?: unknown }).activeTurnId,
        null,
      );
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

  it.effect("does not stop a replacement session when failed cleanup loses the start race", () =>
    Effect.gen(function* () {
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

  it.effect("requires proven absence before identity-free failed-turn cleanup", () =>
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
          lastTerminalTurnId: null,
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
