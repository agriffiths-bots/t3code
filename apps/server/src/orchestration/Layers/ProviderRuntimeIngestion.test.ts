/* oxlint-disable t3code/no-manual-effect-runtime-in-tests -- These integration tests intentionally manage long-lived runtimes and scopes across async harness boundaries. */
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import {
  CodexSettings,
  OrchestrationReadModel,
  ProviderDriverKind,
  ProviderRuntimeEvent,
  ProviderSession,
  ProviderInstanceId,
} from "@t3tools/contracts";
import {
  ApprovalRequestId,
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  ProjectId,
  ProviderItemId,
  type ServerSettings,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import { afterEach, describe, expect, it } from "vite-plus/test";

import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { PendingDispatchRepositoryLive } from "../../persistence/Layers/PendingDispatches.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as SubagentDispatchLimiter from "../../mcp/toolkits/subagent/SubagentDispatchLimiter.ts";
import { makeCodexAdapter } from "../../provider/Layers/CodexAdapter.ts";
import type { ProviderAdapterError } from "../../provider/Errors.ts";
import type { CodexAdapterShape } from "../../provider/Services/CodexAdapter.ts";
import type { ProviderAdapterShape } from "../../provider/Services/ProviderAdapter.ts";
import { ProviderAdapterRegistry } from "../../provider/Services/ProviderAdapterRegistry.ts";
import { ProviderInstanceRegistry } from "../../provider/Services/ProviderInstanceRegistry.ts";
import {
  ProviderSessionDirectory,
  type ProviderRuntimeBinding,
} from "../../provider/Services/ProviderSessionDirectory.ts";
import {
  ProviderService,
  type ProviderServiceShape,
} from "../../provider/Services/ProviderService.ts";
import { makeProviderServiceLive } from "../../provider/Layers/ProviderService.ts";
import { ProviderSessionDirectoryLive } from "../../provider/Layers/ProviderSessionDirectory.ts";
import { makeProviderSessionReaperLive } from "../../provider/Layers/ProviderSessionReaper.ts";
import * as ProviderEventLoggers from "../../provider/Layers/ProviderEventLoggers.ts";
import { ProviderSessionReaper } from "../../provider/Services/ProviderSessionReaper.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import * as AnalyticsService from "../../telemetry/AnalyticsService.ts";
import { makeAdapterRegistryMock } from "../../provider/testUtils/providerAdapterRegistryMock.ts";
import { captureProviderRuntimeEventBinding } from "../../provider/runtimeEventBindingRegistry.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { ChildThreadCoordinatorLive } from "./ChildThreadCoordinator.ts";
import { OrchestrationEngineLive } from "./OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";
import { ProviderRuntimeIngestionLive } from "./ProviderRuntimeIngestion.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ChildThreadCoordinator } from "../Services/ChildThreadCoordinator.ts";
import { ProviderRuntimeIngestionService } from "../Services/ProviderRuntimeIngestion.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { readDetailedReadModel } from "../testUtils/readModel.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { isThreadDetailEvent } from "../../ws.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";

import { trustedSystemDispatchAuthority } from "../commandAudienceGuard.ts";
const testDispatchAuthority = trustedSystemDispatchAuthority("orchestration-test");
// Retained alias keeps the restored runtime-event coverage easy to distinguish.
const xit = it;

function makeTestServerSettingsLayer(overrides: Partial<ServerSettings> = {}) {
  return ServerSettingsService.layerTest(overrides);
}

const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asItemId = (value: string): ProviderItemId => ProviderItemId.make(value);
const asEventId = (value: string): EventId => EventId.make(value);
const asMessageId = (value: string): MessageId => MessageId.make(value);
const asThreadId = (value: string): ThreadId => ThreadId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);
const decodeCodexSettings = Schema.decodeSync(CodexSettings);

class RecordedCodexAdapter extends Context.Service<RecordedCodexAdapter, CodexAdapterShape>()(
  "t3/orchestration/Layers/ProviderRuntimeIngestion.test/RecordedCodexAdapter",
) {}

type LegacyProviderRuntimeEvent = {
  readonly type: string;
  readonly eventId: EventId;
  readonly provider: ProviderRuntimeEvent["provider"];
  readonly createdAt: string;
  readonly threadId: ThreadId;
  readonly turnId?: string | undefined;
  readonly itemId?: string | undefined;
  readonly requestId?: string | undefined;
  readonly payload?: unknown | undefined;
  readonly [key: string]: unknown;
};

type LegacyTurnCompletedEvent = LegacyProviderRuntimeEvent & {
  readonly type: "turn.completed";
  readonly payload?: undefined;
  readonly status: "completed" | "failed" | "interrupted" | "cancelled";
  readonly errorMessage?: string | undefined;
};

function isLegacyTurnCompletedEvent(
  event: LegacyProviderRuntimeEvent,
): event is LegacyTurnCompletedEvent {
  return (
    event.type === "turn.completed" &&
    event.payload === undefined &&
    typeof event.status === "string"
  );
}

function createProviderServiceHarness(adapter?: CodexAdapterShape) {
  const runtimeEventPubSub = Effect.runSync(PubSub.unbounded<ProviderRuntimeEvent>());
  const runtimeSessions: ProviderSession[] = [];
  const persistedBindings = new Map<ThreadId, ProviderRuntimeBinding>();

  const unsupported = () => Effect.die(new Error("Unsupported provider call in test")) as never;
  const service: ProviderServiceShape = {
    startSession: () => unsupported(),
    sendTurn: () => unsupported(),
    interruptTurn: () => unsupported(),
    respondToRequest: () => unsupported(),
    respondToUserInput: () => unsupported(),
    stopSession: () => unsupported(),
    stopFailedSession: () => unsupported(),
    listSessions: adapter?.listSessions ?? (() => Effect.succeed([...runtimeSessions])),
    getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
    getInstanceInfo: (instanceId) => {
      const driverKind = ProviderDriverKind.make(String(instanceId));
      return Effect.succeed({
        instanceId,
        driverKind,
        displayName: undefined,
        enabled: true,
        continuationIdentity: {
          driverKind,
          continuationKey: `${driverKind}:instance:${instanceId}`,
        },
      });
    },
    rollbackConversation: () => unsupported(),
    get streamEvents() {
      return adapter
        ? adapter.streamEvents.pipe(
            Stream.map((event) => {
              const canonicalEvent = {
                ...event,
                providerInstanceId:
                  event.providerInstanceId ?? ProviderInstanceId.make(String(event.provider)),
              };
              captureBinding(canonicalEvent);
              return canonicalEvent;
            }),
          )
        : Stream.fromPubSub(runtimeEventPubSub);
    },
  };

  const setSession = (session: ProviderSession): void => {
    const normalizedSession = {
      ...session,
      providerInstanceId:
        session.providerInstanceId ?? ProviderInstanceId.make(String(session.provider)),
    } satisfies ProviderSession;
    const existingIndex = runtimeSessions.findIndex(
      (entry) => entry.threadId === normalizedSession.threadId,
    );
    if (existingIndex >= 0) {
      runtimeSessions[existingIndex] = normalizedSession;
    } else {
      runtimeSessions.push(normalizedSession);
    }
    persistedBindings.set(normalizedSession.threadId, {
      threadId: normalizedSession.threadId,
      provider: normalizedSession.provider,
      providerInstanceId: normalizedSession.providerInstanceId,
      runtimeMode: normalizedSession.runtimeMode,
    });
  };

  const removeLiveSession = (threadId: ThreadId): void => {
    const index = runtimeSessions.findIndex((entry) => entry.threadId === threadId);
    if (index >= 0) runtimeSessions.splice(index, 1);
  };

  const captureBinding = (event: ProviderRuntimeEvent): void => {
    const liveBinding = runtimeSessions.find((session) => session.threadId === event.threadId);
    const persistedBinding = persistedBindings.get(event.threadId);
    if (liveBinding !== undefined) {
      captureProviderRuntimeEventBinding(event, {
        threadId: liveBinding.threadId,
        provider: liveBinding.provider,
        providerInstanceId: liveBinding.providerInstanceId,
        runtimeMode: liveBinding.runtimeMode,
        cwd: liveBinding.cwd,
      });
      return;
    }
    if (event.type === "session.exited" && persistedBinding !== undefined) {
      captureProviderRuntimeEventBinding(event, {
        threadId: persistedBinding.threadId,
        provider: persistedBinding.provider,
        providerInstanceId: persistedBinding.providerInstanceId,
        runtimeMode: persistedBinding.runtimeMode,
        cwd: undefined,
      });
    }
  };

  const normalizeLegacyEvent = (event: LegacyProviderRuntimeEvent): ProviderRuntimeEvent => {
    if (isLegacyTurnCompletedEvent(event)) {
      const normalized: Extract<ProviderRuntimeEvent, { type: "turn.completed" }> = {
        ...(event as Omit<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>, "payload">),
        payload: {
          state: event.status,
          ...(typeof event.errorMessage === "string" ? { errorMessage: event.errorMessage } : {}),
        },
      };
      return normalized;
    }

    return event as ProviderRuntimeEvent;
  };

  const publish = (event: ProviderRuntimeEvent): void => {
    captureBinding(event);
    Effect.runSync(PubSub.publish(runtimeEventPubSub, event));
  };

  const emit = (event: LegacyProviderRuntimeEvent): void => {
    const normalized = normalizeLegacyEvent(event);
    if (normalized.providerInstanceId === undefined) {
      const providerInstanceId = ProviderInstanceId.make(String(normalized.provider));
      const existing = runtimeSessions.find((session) => session.threadId === normalized.threadId);
      if (existing !== undefined) {
        setSession({
          ...existing,
          provider: normalized.provider,
          providerInstanceId,
        });
      } else {
        setSession({
          provider: normalized.provider,
          providerInstanceId,
          status: "ready",
          runtimeMode: "approval-required",
          threadId: normalized.threadId,
          createdAt: normalized.createdAt,
          updatedAt: normalized.createdAt,
        });
      }
    }
    publish({
      ...normalized,
      providerInstanceId:
        normalized.providerInstanceId ?? ProviderInstanceId.make(String(normalized.provider)),
    });
  };

  const emitUnstamped = (event: LegacyProviderRuntimeEvent): void => {
    publish(normalizeLegacyEvent(event));
  };

  const emitWithCapturedBinding = (
    event: LegacyProviderRuntimeEvent,
    binding: Parameters<typeof captureProviderRuntimeEventBinding>[1],
  ): void => {
    const normalized = normalizeLegacyEvent(event);
    captureProviderRuntimeEventBinding(normalized, binding);
    Effect.runSync(PubSub.publish(runtimeEventPubSub, normalized));
  };

  const emitQueuedBeforeLiveRemoval = (
    events: ReadonlyArray<LegacyProviderRuntimeEvent>,
    threadId: ThreadId,
  ): void => {
    const canonicalEvents = events.map((event) => {
      const normalized = normalizeLegacyEvent(event);
      return {
        ...normalized,
        providerInstanceId:
          normalized.providerInstanceId ?? ProviderInstanceId.make(String(normalized.provider)),
      } satisfies ProviderRuntimeEvent;
    });
    for (const event of canonicalEvents) captureBinding(event);
    removeLiveSession(threadId);
    for (const event of canonicalEvents) publish(event);
  };

  return {
    service,
    emit,
    emitQueuedBeforeLiveRemoval,
    emitUnstamped,
    emitWithCapturedBinding,
    removeLiveSession,
    setSession,
  };
}

type ProviderRuntimeTestReadModel = OrchestrationReadModel;
type ProviderRuntimeTestThread = ProviderRuntimeTestReadModel["threads"][number];
type ProviderRuntimeTestMessage = ProviderRuntimeTestThread["messages"][number];
type ProviderRuntimeTestProposedPlan = ProviderRuntimeTestThread["proposedPlans"][number];
type ProviderRuntimeTestActivity = ProviderRuntimeTestThread["activities"][number];
type ProviderRuntimeTestCheckpoint = ProviderRuntimeTestThread["checkpoints"][number];

async function waitForThread(
  readModel: () => Promise<ProviderRuntimeTestReadModel>,
  predicate: (thread: ProviderRuntimeTestThread) => boolean,
  timeoutMs = 2000,
  threadId: ThreadId = asThreadId("thread-1"),
) {
  const deadline = (await Effect.runPromise(Clock.currentTimeMillis)) + timeoutMs;
  const poll = async (): Promise<ProviderRuntimeTestThread> => {
    const snapshot = await readModel();
    const thread = snapshot.threads.find((entry) => entry.id === threadId);
    if (thread && predicate(thread)) {
      return thread;
    }
    if ((await Effect.runPromise(Clock.currentTimeMillis)) >= deadline) {
      throw new Error("Timed out waiting for thread state");
    }
    await Effect.runPromise(Effect.yieldNow);
    return poll();
  };
  return poll();
}

describe("ProviderRuntimeIngestion", () => {
  let runtime: ManagedRuntime.ManagedRuntime<
    | OrchestrationEngineService
    | ProviderRuntimeIngestionService
    | ProjectionSnapshotQuery
    | ChildThreadCoordinator
    | SubagentDispatchLimiter.SubagentDispatchLimiter,
    unknown
  > | null = null;
  let scope: Scope.Closeable | null = null;
  let providerStackScope: Scope.Closeable | null = null;
  let watchdogScope: Scope.Closeable | null = null;
  const tempDirs: string[] = [];

  function makeTempDir(prefix: string): string {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), prefix));
    tempDirs.push(dir);
    return dir;
  }

  async function createRecordedCodexStack(scenario: "success" | "empty" | "death" = "success") {
    const fixtureBinary = NodePath.join(
      NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
      "../../provider/Layers/fixtures/replay-codex-app-server-turn.mjs",
    );
    const sqliteLayer = SqlitePersistenceMemory;
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(Layer.provide(sqliteLayer));
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const serverSettingsLayer = ServerSettingsService.layerTest();
    const serverConfigLayer = ServerConfig.layerTest(process.cwd(), process.cwd());
    const adapterLayer = Layer.effect(
      RecordedCodexAdapter,
      makeCodexAdapter(decodeCodexSettings({ binaryPath: fixtureBinary }), {
        environment: {
          ...process.env,
          T3_PROVIDER_E2E_SCENARIO: scenario,
        },
      }),
    ).pipe(
      Layer.provideMerge(directoryLayer),
      Layer.provideMerge(serverConfigLayer),
      Layer.provideMerge(serverSettingsLayer),
      Layer.provideMerge(NodeServices.layer),
    );
    const adapterRegistryLayer = Layer.effect(
      ProviderAdapterRegistry,
      Effect.gen(function* () {
        const adapter = yield* RecordedCodexAdapter;
        return makeAdapterRegistryMock({
          [ProviderDriverKind.make("codex")]: adapter as ProviderAdapterShape<ProviderAdapterError>,
        });
      }),
    ).pipe(Layer.provide(adapterLayer));
    const providerServiceLayer = makeProviderServiceLive().pipe(
      Layer.provide(adapterRegistryLayer),
      Layer.provide(directoryLayer),
      Layer.provide(serverSettingsLayer),
      Layer.provideMerge(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );
    const layer = Layer.mergeAll(
      adapterLayer,
      providerServiceLayer,
      directoryLayer,
      runtimeRepositoryLayer,
    ).pipe(
      Layer.provideMerge(sqliteLayer),
      Layer.provideMerge(serverConfigLayer),
      Layer.provideMerge(serverSettingsLayer),
      Layer.provideMerge(NodeServices.layer),
    );
    providerStackScope = await Effect.runPromise(Scope.make("sequential"));
    const context = await Effect.runPromise(Layer.buildWithScope(layer, providerStackScope));
    return Effect.runPromise(
      Effect.all({
        adapter: Effect.service(RecordedCodexAdapter),
        providerService: Effect.service(ProviderService),
        directory: Effect.service(ProviderSessionDirectory),
      }).pipe(Effect.provide(context)),
    );
  }

  afterEach(async () => {
    if (watchdogScope) {
      await Effect.runPromise(Scope.close(watchdogScope, Exit.void));
    }
    watchdogScope = null;
    if (scope) {
      await Effect.runPromise(Scope.close(scope, Exit.void));
    }
    scope = null;
    if (providerStackScope) {
      await Effect.runPromise(Scope.close(providerStackScope, Exit.void));
    }
    providerStackScope = null;
    if (runtime) {
      await runtime.dispose();
    }
    runtime = null;
    for (const dir of tempDirs.splice(0)) {
      NodeFS.rmSync(dir, { recursive: true, force: true });
    }
  });

  async function createHarness(options?: {
    serverSettings?: Partial<ServerSettings>;
    provider?: ProviderDriverKind;
    providerAdapter?: CodexAdapterShape;
    providerService?: ProviderServiceShape;
  }) {
    const workspaceRoot = makeTempDir("t3-provider-project-");
    NodeFS.mkdirSync(NodePath.join(workspaceRoot, ".git"));
    const providerDriver = options?.provider ?? ProviderDriverKind.make("codex");
    const providerInstanceId = ProviderInstanceId.make(String(providerDriver));
    const provider = createProviderServiceHarness(options?.providerAdapter);
    const auditLogs: string[] = [];
    const captureLogger = Logger.make<unknown, void>(({ message }) => {
      auditLogs.push(JSON.stringify(message));
    });
    const modelSelection = {
      instanceId: providerInstanceId,
      model: providerDriver === "codex" ? "gpt-5.4-mini" : `${providerDriver}-test-model`,
    };
    const orchestrationLayer = OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(OrchestrationProjectionPipelineLive),
      Layer.provide(OrchestrationEventStoreLive),
      Layer.provide(OrchestrationCommandReceiptRepositoryLive),
      Layer.provide(RepositoryIdentityResolver.layer),
      Layer.provide(SqlitePersistenceMemory),
    );
    const projectionSnapshotLayer = OrchestrationProjectionSnapshotQueryLive.pipe(
      Layer.provide(RepositoryIdentityResolver.layer),
      Layer.provide(SqlitePersistenceMemory),
    );
    const providerInstance = { instanceId: providerInstanceId } as never;
    const providerInstanceRegistryLayer = Layer.succeed(ProviderInstanceRegistry, {
      getInstance: (instanceId) =>
        Effect.succeed(
          String(instanceId) === String(providerInstanceId) ? providerInstance : undefined,
        ),
      listInstances: Effect.succeed([providerInstance]),
      listUnavailable: Effect.succeed([]),
      streamChanges: Stream.empty,
      subscribeChanges: Effect.die("Provider registry subscriptions are not used in this test"),
    });
    const layer = Layer.mergeAll(ProviderRuntimeIngestionLive, ChildThreadCoordinatorLive).pipe(
      Layer.provideMerge(PendingDispatchRepositoryLive),
      Layer.provideMerge(orchestrationLayer),
      Layer.provideMerge(projectionSnapshotLayer),
      Layer.provideMerge(SqlitePersistenceMemory),
      Layer.provideMerge(
        Layer.succeed(ProviderService, options?.providerService ?? provider.service),
      ),
      Layer.provideMerge(providerInstanceRegistryLayer),
      Layer.provideMerge(SubagentDispatchLimiter.layerTest(1)),
      Layer.provideMerge(makeTestServerSettingsLayer(options?.serverSettings)),
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(NodeServices.layer),
      Layer.provideMerge(Logger.layer([captureLogger])),
    );
    const managedRuntime = ManagedRuntime.make(layer);
    runtime = managedRuntime;
    const engine = await managedRuntime.runPromise(Effect.service(OrchestrationEngineService));
    const snapshotQuery = await managedRuntime.runPromise(Effect.service(ProjectionSnapshotQuery));
    const ingestion = await managedRuntime.runPromise(
      Effect.service(ProviderRuntimeIngestionService),
    );
    const coordinator = await managedRuntime.runPromise(Effect.service(ChildThreadCoordinator));
    const sqlClient = await managedRuntime.runPromise(Effect.service(SqlClient));
    scope = await Effect.runPromise(Scope.make("sequential"));
    await Effect.runPromise(coordinator.start().pipe(Scope.provide(scope)));
    await Effect.runPromise(ingestion.start().pipe(Scope.provide(scope)));
    const drain = async () => {
      await Effect.runPromise(ingestion.drain);
      for (let index = 0; index < 50; index += 1) {
        await Effect.runPromise(Effect.yieldNow);
      }
      await Effect.runPromise(ingestion.drain);
      await Effect.runPromise(coordinator.drain);
    };

    const createdAt = "2026-01-01T00:00:00.000Z";
    await Effect.runPromise(
      engine.dispatch(
        {
          type: "project.create",
          commandId: CommandId.make("cmd-provider-project-create"),
          projectId: asProjectId("project-1"),
          title: "Provider Project",
          workspaceRoot,
          defaultModelSelection: modelSelection,
          createdAt,
        },
        testDispatchAuthority,
      ),
    );
    await Effect.runPromise(
      engine.dispatch(
        {
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-create"),
          threadId: ThreadId.make("thread-1"),
          projectId: asProjectId("project-1"),
          title: "Thread",
          modelSelection,
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        },
        testDispatchAuthority,
      ),
    );
    await Effect.runPromise(
      engine.dispatch(
        {
          type: "thread.session.set",
          commandId: CommandId.make("cmd-session-seed"),
          threadId: ThreadId.make("thread-1"),
          session: {
            threadId: ThreadId.make("thread-1"),
            status: "ready",
            providerName: providerDriver,
            providerInstanceId,
            runtimeMode: "approval-required",
            activeTurnId: null,
            updatedAt: createdAt,
            lastError: null,
          },
          createdAt,
        },
        testDispatchAuthority,
      ),
    );
    provider.setSession({
      provider: providerDriver,
      providerInstanceId,
      status: "ready",
      runtimeMode: "approval-required",
      threadId: ThreadId.make("thread-1"),
      createdAt,
      updatedAt: createdAt,
    });

    return {
      engine,
      snapshotQuery,
      sqlClient,
      workspaceRoot,
      readModel: () => readDetailedReadModel(snapshotQuery),
      readEvents: (fromSequence: number) =>
        managedRuntime.runPromise(
          Stream.runCollect(engine.readEvents(fromSequence)).pipe(
            Effect.map((chunk) => Array.from(chunk)),
          ),
        ),
      emit: provider.emit,
      emitQueuedBeforeLiveRemoval: provider.emitQueuedBeforeLiveRemoval,
      emitUnstamped: provider.emitUnstamped,
      emitWithCapturedBinding: provider.emitWithCapturedBinding,
      setProviderSession: provider.setSession,
      removeLiveProviderSession: provider.removeLiveSession,
      auditLogs,
      readBindingDropCount: (outcome: "no-binding" | "target-mismatch") =>
        managedRuntime.runPromise(
          Metric.snapshot.pipe(
            Effect.map((snapshots) =>
              snapshots.reduce((count, snapshot) => {
                if (
                  snapshot.type !== "Counter" ||
                  snapshot.id !== "t3_provider_runtime_event_binding_drops_total" ||
                  snapshot.attributes?.consumer !== "ProviderRuntimeIngestion" ||
                  snapshot.attributes.outcome !== outcome
                ) {
                  return count;
                }
                return count + Number(snapshot.state.count);
              }, 0),
            ),
          ),
        ),
      coordinator,
      modelSelection,
      acquireDispatchLease: (childThreadId: ThreadId) =>
        managedRuntime.runPromise(
          Effect.gen(function* () {
            const limiter = yield* SubagentDispatchLimiter.SubagentDispatchLimiter;
            const lease = yield* limiter.acquire;
            yield* limiter.bindChild(lease, childThreadId);
          }),
        ),
      leasedChildThreadIds: () =>
        managedRuntime.runPromise(
          Effect.flatMap(
            Effect.service(SubagentDispatchLimiter.SubagentDispatchLimiter),
            (limiter) => limiter.leasedChildThreadIds,
          ),
        ),
      readProjectedTurns: (threadId: ThreadId) =>
        managedRuntime.runPromise(
          Effect.flatMap(
            Effect.service(SqlClient),
            (sql) => sql<{ readonly turnId: string | null; readonly state: string }>`
              SELECT turn_id AS "turnId", state
              FROM projection_turns
              WHERE thread_id = ${threadId}
              ORDER BY requested_at ASC
            `,
          ),
        ),
      readProjectedEffectiveModel: (threadId: ThreadId, turnId: TurnId) =>
        managedRuntime.runPromise(
          Effect.flatMap(
            Effect.service(SqlClient),
            (sql) => sql<{ readonly effectiveModel: string | null }>`
              SELECT effective_model AS "effectiveModel"
              FROM projection_turns
              WHERE thread_id = ${threadId}
                AND turn_id = ${turnId}
              LIMIT 1
            `,
          ),
        ),
      drain,
    };
  }

  async function startProviderWatchdog(
    harness: Awaited<ReturnType<typeof createHarness>>,
    stack: Awaited<ReturnType<typeof createRecordedCodexStack>>,
  ) {
    if (!runtime) throw new Error("Provider runtime ingestion harness is not initialized.");
    const layer = makeProviderSessionReaperLive({
      sweepIntervalMs: 10,
      stopTimeoutMs: 1_000,
    }).pipe(
      Layer.provideMerge(Layer.succeed(ProviderService, stack.providerService)),
      Layer.provideMerge(Layer.succeed(ProviderSessionDirectory, stack.directory)),
      Layer.provideMerge(Layer.succeed(ProjectionSnapshotQuery, harness.snapshotQuery)),
      Layer.provideMerge(Layer.succeed(OrchestrationEngineService, harness.engine)),
      Layer.provideMerge(Layer.succeed(SqlClient, harness.sqlClient)),
    );
    watchdogScope = await Effect.runPromise(Scope.make("sequential"));
    const context = await Effect.runPromise(Layer.buildWithScope(layer, watchdogScope));
    const reaper = await Effect.runPromise(
      Effect.service(ProviderSessionReaper).pipe(Effect.provide(context)),
    );
    await Effect.runPromise(reaper.start().pipe(Scope.provide(watchdogScope)));
  }

  it("drops an unbound runtime event and increments the no-binding counter", async () => {
    const harness = await createHarness({
      serverSettings: { enableAssistantStreaming: true },
    });
    const privateThreadId = asThreadId("thread-forged-private-target");
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.create",
          commandId: CommandId.make("cmd-forged-private-target-create"),
          threadId: privateThreadId,
          projectId: asProjectId("project-1"),
          title: "Unrelated private thread",
          modelSelection: harness.modelSelection,
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        },
        testDispatchAuthority,
      ),
    );

    const dropsBefore = await harness.readBindingDropCount("no-binding");
    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-forged-private-target"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: harness.modelSelection.instanceId,
      threadId: privateThreadId,
      turnId: asTurnId("turn-forged-private-target"),
      itemId: asItemId("item-forged-private-target"),
      createdAt,
      payload: {
        streamKind: "assistant_text",
        delta: "must not reach the private thread",
      },
    });
    await harness.drain();

    const privateThread = (await harness.readModel()).threads.find(
      (thread) => thread.id === privateThreadId,
    );
    expect(privateThread?.messages).toEqual([]);
    expect(privateThread?.activities).toEqual([]);
    expect(
      harness.auditLogs.some(
        (entry) =>
          entry.includes("provider runtime event without tracked binding dropped") &&
          entry.includes("no-binding"),
      ),
    ).toBe(true);
    expect(await harness.readBindingDropCount("no-binding")).toBe(dropsBefore + 1);
  });

  it("drops a spoofed bound-thread target and emits a target-mismatch audit", async () => {
    const harness = await createHarness({
      serverSettings: { enableAssistantStreaming: true },
    });

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-unproven-ordinary-target"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex-spoofed-instance"),
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-unproven-ordinary-target"),
      itemId: asItemId("item-unproven-ordinary-target"),
      createdAt: "2026-01-01T00:00:00.000Z",
      payload: {
        streamKind: "assistant_text",
        delta: "must not project under a spoofed provider target",
      },
    });
    await harness.drain();

    const thread = (await harness.readModel()).threads.find(
      (entry) => entry.id === asThreadId("thread-1"),
    );
    expect(thread?.messages).toEqual([]);
    expect(thread?.activities).toEqual([]);
    expect(
      harness.auditLogs.some(
        (entry) =>
          entry.includes("provider runtime event target mismatch dropped") &&
          entry.includes("target-mismatch"),
      ),
    ).toBe(true);
  });

  it("drops an event with missing instance identity in runtime ingestion", async () => {
    const harness = await createHarness({
      serverSettings: { enableAssistantStreaming: true },
    });

    harness.emitUnstamped({
      type: "content.delta",
      eventId: asEventId("evt-missing-instance-identity"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-missing-instance-identity"),
      itemId: asItemId("item-missing-instance-identity"),
      createdAt: "2026-01-01T00:00:00.000Z",
      payload: {
        streamKind: "assistant_text",
        delta: "must not project without an exact instance target",
      },
    });
    await harness.drain();

    const thread = (await harness.readModel()).threads.find(
      (entry) => entry.id === asThreadId("thread-1"),
    );
    expect(thread?.messages).toEqual([]);
    expect(
      harness.auditLogs.some(
        (entry) =>
          entry.includes("evt-missing-instance-identity") && entry.includes("target-mismatch"),
      ),
    ).toBe(true);
  });

  it("drops an event when both claimed and captured instance identity are absent", async () => {
    const harness = await createHarness({
      serverSettings: { enableAssistantStreaming: true },
    });
    const threadId = asThreadId("thread-1");
    harness.emitWithCapturedBinding(
      {
        type: "content.delta",
        eventId: asEventId("evt-both-missing-instance-identity"),
        provider: ProviderDriverKind.make("codex"),
        threadId,
        turnId: asTurnId("turn-both-missing-instance-identity"),
        itemId: asItemId("item-both-missing-instance-identity"),
        createdAt: "2026-01-01T00:00:00.000Z",
        payload: {
          streamKind: "assistant_text",
          delta: "must not project without either instance identity",
        },
      },
      {
        threadId,
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: undefined,
        runtimeMode: "approval-required",
        cwd: undefined,
      },
    );
    await harness.drain();

    const thread = (await harness.readModel()).threads.find((entry) => entry.id === threadId);
    expect(thread?.messages).toEqual([]);
    expect(
      harness.auditLogs.some(
        (entry) =>
          entry.includes("evt-both-missing-instance-identity") && entry.includes("target-mismatch"),
      ),
    ).toBe(true);
  });

  it("preserves a restricted session mode when captured mode metadata is absent", async () => {
    const harness = await createHarness();
    const threadId = asThreadId("thread-1");
    harness.emitWithCapturedBinding(
      {
        type: "session.state.changed",
        eventId: asEventId("evt-session-state-without-captured-mode"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: harness.modelSelection.instanceId,
        threadId,
        createdAt: "2026-01-01T00:00:00.000Z",
        payload: { state: "waiting", reason: "awaiting approval" },
      },
      {
        threadId,
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: harness.modelSelection.instanceId,
        runtimeMode: undefined,
        cwd: undefined,
      },
    );

    const thread = await waitForThread(
      harness.readModel,
      (entry) => entry.session?.status === "waiting",
    );
    expect(thread.session?.runtimeMode).toBe("approval-required");
  });

  it("drops a nonterminal event after its live session binding is removed", async () => {
    const harness = await createHarness({
      serverSettings: { enableAssistantStreaming: true },
    });
    const threadId = asThreadId("thread-1");
    const dropsBefore = await harness.readBindingDropCount("no-binding");
    harness.removeLiveProviderSession(threadId);

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-nonterminal-after-live-removal"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: harness.modelSelection.instanceId,
      threadId,
      turnId: asTurnId("turn-nonterminal-after-live-removal"),
      itemId: asItemId("item-nonterminal-after-live-removal"),
      createdAt: "2026-01-01T00:00:00.000Z",
      payload: {
        streamKind: "assistant_text",
        delta: "must not project after teardown",
      },
    });
    await harness.drain();

    const thread = (await harness.readModel()).threads.find((entry) => entry.id === threadId);
    expect(thread?.messages).toEqual([]);
    expect(await harness.readBindingDropCount("no-binding")).toBe(dropsBefore + 1);
  });

  it("projects trailing delta and completion after terminal state with multiple live sessions", async () => {
    const cursor = ProviderDriverKind.make("cursor");
    const instanceId = ProviderInstanceId.make("cursor");
    const harness = await createHarness({ provider: cursor });
    const now = "2026-01-01T00:00:00.000Z";
    const threadId = asThreadId("thread-1");
    const turnId = asTurnId("turn-multi-session-terminal-late");
    harness.setProviderSession({
      provider: cursor,
      providerInstanceId: instanceId,
      status: "ready",
      runtimeMode: "approval-required",
      threadId: asThreadId("thread-peer-on-cursor"),
      createdAt: now,
      updatedAt: now,
    });

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-multi-session-terminal-started"),
      provider: cursor,
      providerInstanceId: instanceId,
      threadId,
      turnId,
      createdAt: now,
    });
    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-multi-session-terminal-completed"),
      provider: cursor,
      providerInstanceId: instanceId,
      threadId,
      turnId,
      createdAt: now,
      payload: { state: "completed" },
    });
    await waitForThread(
      harness.readModel,
      (thread) => thread.session?.status === "ready" && thread.session.activeTurnId === null,
    );

    harness.emitQueuedBeforeLiveRemoval(
      [
        {
          type: "content.delta",
          eventId: asEventId("evt-multi-session-terminal-late-delta"),
          provider: cursor,
          providerInstanceId: instanceId,
          threadId,
          turnId,
          itemId: asItemId("item-multi-session-terminal-late"),
          createdAt: now,
          payload: {
            streamKind: "assistant_text",
            delta: "late output after terminal state",
          },
        },
        {
          type: "turn.completed",
          eventId: asEventId("evt-multi-session-terminal-trailing-completed"),
          provider: cursor,
          providerInstanceId: instanceId,
          threadId,
          turnId,
          createdAt: now,
          payload: { state: "completed" },
        },
      ],
      threadId,
    );

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message) =>
          message.id === "assistant:item-multi-session-terminal-late" &&
          message.turnId === turnId &&
          message.text === "late output after terminal state" &&
          !message.streaming,
      ),
    );
    expect(thread.latestTurn).toMatchObject({ turnId, state: "completed" });
  });

  it("projects old-turn trailing events after a later turn starts with multiple live sessions", async () => {
    const cursor = ProviderDriverKind.make("cursor");
    const instanceId = ProviderInstanceId.make("cursor");
    const harness = await createHarness({ provider: cursor });
    const now = "2026-01-01T00:00:00.000Z";
    const threadId = asThreadId("thread-1");
    const oldTurnId = asTurnId("turn-multi-session-old");
    const activeTurnId = asTurnId("turn-multi-session-active");
    harness.setProviderSession({
      provider: cursor,
      providerInstanceId: instanceId,
      status: "ready",
      runtimeMode: "approval-required",
      threadId: asThreadId("thread-peer-on-cursor-active"),
      createdAt: now,
      updatedAt: now,
    });

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-multi-session-old-started"),
      provider: cursor,
      providerInstanceId: instanceId,
      threadId,
      turnId: oldTurnId,
      createdAt: now,
    });
    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-multi-session-old-completed"),
      provider: cursor,
      providerInstanceId: instanceId,
      threadId,
      turnId: oldTurnId,
      createdAt: now,
      payload: { state: "completed" },
    });
    await waitForThread(
      harness.readModel,
      (thread) => thread.session?.status === "ready" && thread.session.activeTurnId === null,
    );

    harness.setProviderSession({
      provider: cursor,
      providerInstanceId: instanceId,
      status: "running",
      runtimeMode: "approval-required",
      threadId,
      activeTurnId,
      createdAt: now,
      updatedAt: now,
    });
    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-multi-session-active-started"),
      provider: cursor,
      providerInstanceId: instanceId,
      threadId,
      turnId: activeTurnId,
      createdAt: now,
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" && thread.session.activeTurnId === activeTurnId,
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-multi-session-old-trailing-delta"),
      provider: cursor,
      providerInstanceId: instanceId,
      threadId,
      turnId: oldTurnId,
      itemId: asItemId("item-multi-session-old-trailing"),
      createdAt: now,
      payload: {
        streamKind: "assistant_text",
        delta: "old output after the next turn started",
      },
    });
    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-multi-session-old-trailing-completed"),
      provider: cursor,
      providerInstanceId: instanceId,
      threadId,
      turnId: oldTurnId,
      createdAt: now,
      payload: { state: "completed" },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message) =>
          message.id === "assistant:item-multi-session-old-trailing" &&
          message.turnId === oldTurnId &&
          message.text === "old output after the next turn started" &&
          !message.streaming,
      ),
    );
    expect(thread.session).toMatchObject({ status: "running", activeTurnId });
    expect(thread.latestTurn).toMatchObject({ turnId: activeTurnId, state: "running" });
  });

  xit("maps turn started/completed events into thread session updates", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: now,
      turnId: asTurnId("turn-1"),
    });

    await waitForThread(
      harness.readModel,
      (thread) => thread.session?.status === "running" && thread.session?.activeTurnId === "turn-1",
    );

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-turn-completed"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: "2026-01-01T00:00:00.000Z",
      turnId: asTurnId("turn-1"),
      payload: {
        state: "failed",
        errorMessage: "turn failed",
      },
    });

    const thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.session?.status === "error" &&
        entry.session?.activeTurnId === null &&
        entry.session?.lastError === "turn failed",
    );
    expect(thread.session?.status).toBe("error");
    expect(thread.session?.lastError).toBe("turn failed");
  });

  xit("persists effective models from turn completion and explicit reroutes", async () => {
    const harness = await createHarness();
    const turnId = asTurnId("turn-effective-model");

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-effective-model-started"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: "2026-01-01T00:00:00.000Z",
      turnId,
    });
    await waitForThread(harness.readModel, (thread) => thread.latestTurn?.turnId === turnId);

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-effective-model-completed"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: "2026-01-01T00:00:01.000Z",
      turnId,
      payload: {
        state: "completed",
        effectiveModel: "claude-opus-4-8",
      },
    });

    const completedThread = await waitForThread(
      harness.readModel,
      (thread) => thread.latestTurn?.effectiveModel === "claude-opus-4-8",
    );
    expect(completedThread.turns[0]?.effectiveModel).toBe("claude-opus-4-8");
    expect(await harness.readProjectedEffectiveModel(asThreadId("thread-1"), turnId)).toEqual([
      { effectiveModel: "claude-opus-4-8" },
    ]);
    const effectiveModelEvent = (await harness.readEvents(0)).find(
      (event) => event.type === "thread.turn-effective-model-set",
    );
    expect(effectiveModelEvent).toBeDefined();
    if (!effectiveModelEvent) {
      throw new Error("Expected the effective-model event to be persisted");
    }
    // subscribeThread shares this predicate for both live delivery and catch-up replay.
    expect(isThreadDetailEvent(effectiveModelEvent)).toBe(true);

    const codexTurnId = asTurnId("turn-codex-rerouted");
    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-codex-reroute-started"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: "2026-01-01T00:00:02.000Z",
      turnId: codexTurnId,
    });
    await waitForThread(harness.readModel, (thread) => thread.latestTurn?.turnId === codexTurnId);
    harness.emit({
      type: "model.rerouted",
      eventId: asEventId("evt-codex-rerouted"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: "2026-01-01T00:00:03.000Z",
      turnId: codexTurnId,
      payload: {
        fromModel: "gpt-5.4-mini",
        toModel: "gpt-5.4",
        reason: "fallback",
      },
    });

    const reroutedThread = await waitForThread(
      harness.readModel,
      (thread) => thread.latestTurn?.effectiveModel === "gpt-5.4",
    );
    expect(reroutedThread.latestTurn?.turnId).toBe(codexTurnId);
  });

  xit("projects a session exit from the persisted binding after live removal", async () => {
    const harness = await createHarness();
    const turnId = asTurnId("turn-provider-process-exit");

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-process-exit-turn-started"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: "2026-01-01T00:00:00.000Z",
      turnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) => thread.latestTurn?.turnId === turnId && thread.latestTurn.state === "running",
    );
    harness.removeLiveProviderSession(asThreadId("thread-1"));

    harness.emit({
      type: "session.exited",
      eventId: asEventId("evt-provider-process-exited"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: "2026-01-01T00:00:10.000Z",
      turnId,
      payload: {
        reason: "provider process died unexpectedly",
      },
    });

    const thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.latestTurn?.turnId === turnId &&
        entry.latestTurn.state === "error" &&
        entry.session?.status === "error" &&
        entry.session.activeTurnId === null,
    );
    expect(thread.latestTurn).toMatchObject({ turnId, state: "error" });
    expect(thread.session).toMatchObject({
      status: "error",
      activeTurnId: null,
      lastError: "provider process died unexpectedly",
    });
  });

  xit("ignores a stale provider exit after a replacement turn starts", async () => {
    const harness = await createHarness();
    const staleTurnId = asTurnId("turn-stale-provider-process");
    const replacementTurnId = asTurnId("turn-replacement-provider-process");

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-stale-process-turn-started"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: "2026-01-01T00:00:00.000Z",
      turnId: staleTurnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) => thread.session?.activeTurnId === staleTurnId,
    );

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-stale-process-turn-failed"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: "2026-01-01T00:00:04.000Z",
      turnId: staleTurnId,
      payload: { state: "failed", errorMessage: "old provider turn failed" },
    });
    await waitForThread(harness.readModel, (thread) => thread.session?.activeTurnId === null);

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-replacement-process-turn-started"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: "2026-01-01T00:00:05.000Z",
      turnId: replacementTurnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) => thread.session?.activeTurnId === replacementTurnId,
    );

    harness.emit({
      type: "session.exited",
      eventId: asEventId("evt-stale-provider-process-exited"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: "2026-01-01T00:00:10.000Z",
      turnId: staleTurnId,
      payload: {
        exitKind: "error",
        reason: "stale provider process exited late",
      },
    });
    await harness.drain();

    const thread = (await harness.readModel()).threads.find(
      (entry) => entry.id === asThreadId("thread-1"),
    );
    expect(thread?.latestTurn).toMatchObject({ turnId: replacementTurnId, state: "running" });
    expect(thread?.session).toMatchObject({
      status: "running",
      activeTurnId: replacementTurnId,
    });
    expect(thread?.session?.lastError).not.toBe("stale provider process exited late");
  });

  xit("replays a live Codex PONG stream through projection and releases the child lease", async () => {
    const stack = await createRecordedCodexStack();
    const harness = await createHarness({ providerService: stack.providerService });
    const childThreadId = asThreadId("thread-1");
    const parentThreadId = asThreadId("recorded-codex-parent");
    const providerTurnId = asTurnId("019f682e-41b8-7a13-9074-e6404b1747b0");
    const createdAt = "2026-01-01T00:00:01.000Z";

    await harness.acquireDispatchLease(childThreadId);
    await Effect.runPromise(
      harness.coordinator.register({
        parentThreadId,
        childThreadId,
        detached: true,
        model: harness.modelSelection,
        spawnedAtMs: 1,
      }),
    );
    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-recorded-codex-turn"),
          threadId: childThreadId,
          message: {
            messageId: asMessageId("message-recorded-codex-turn"),
            role: "user",
            text: "Reply exactly PONG.",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt,
        },
        testDispatchAuthority,
      ),
    );

    expect(await harness.readProjectedTurns(childThreadId)).toEqual([
      { turnId: null, state: "pending" },
    ]);
    expect(await harness.leasedChildThreadIds()).toEqual([childThreadId]);

    const runtimeEventsPromise = Effect.runPromise(
      stack.providerService.streamEvents.pipe(
        Stream.filter((event) => event.threadId === childThreadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.map((events) => Array.from(events)),
      ),
    );
    await Effect.runPromise(Effect.yieldNow);

    // Each runPromise is a short-lived caller, matching ProviderCommandReactor's
    // per-event fiber. The adapter must keep forwarding events after startSession
    // returns and that caller exits.
    await Effect.runPromise(
      stack.providerService.startSession(childThreadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: harness.modelSelection.instanceId,
        threadId: childThreadId,
        cwd: harness.workspaceRoot,
        modelSelection: harness.modelSelection,
        runtimeMode: "approval-required",
      }),
    );
    await Effect.runPromise(
      stack.providerService.sendTurn({
        threadId: childThreadId,
        input: "Reply exactly PONG.",
        modelSelection: harness.modelSelection,
        attachments: [],
      }),
    );
    const runtimeEvents = await runtimeEventsPromise;

    const completedThread = await waitForThread(
      harness.readModel,
      (thread) =>
        thread.latestTurn?.turnId === providerTurnId &&
        thread.latestTurn.state === "completed" &&
        thread.session?.status === "ready" &&
        thread.session.activeTurnId === null,
      5_000,
      childThreadId,
    );
    await harness.drain();

    expect(
      runtimeEvents
        .filter(
          (event): event is Extract<ProviderRuntimeEvent, { type: "content.delta" }> =>
            event.type === "content.delta" && event.payload.streamKind === "assistant_text",
        )
        .map((event) => event.payload.delta),
    ).toEqual(["P", "ONG"]);
    const assistantMessage = completedThread.messages.find(
      (message) => message.role === "assistant" && message.text === "PONG",
    );
    expect(assistantMessage?.id).toEqual(expect.any(String));
    expect(assistantMessage?.id).not.toBeNull();
    expect(await harness.readProjectedTurns(childThreadId)).toEqual([
      { turnId: providerTurnId, state: "completed" },
    ]);
    expect(await Effect.runPromise(stack.providerService.listSessions())).toEqual([
      expect.objectContaining({
        threadId: childThreadId,
        status: "ready",
        activeTurnId: undefined,
      }),
    ]);

    const childResult = await Effect.runPromise(
      harness.coordinator.waitSlice({
        childThreadIds: [childThreadId],
        mode: "all",
        budgetDeadlineMs: Number.MAX_SAFE_INTEGER,
      }),
    );
    expect(childResult.results).toEqual([
      {
        childThreadId,
        status: "completed",
        finalAssistantText: "PONG",
        error: null,
        parentTurnIdAtWait: null,
      },
    ]);
    expect(await harness.leasedChildThreadIds()).toEqual([]);
  });

  it("fails a recorded Codex turn explicitly when the assistant response is empty", async () => {
    const stack = await createRecordedCodexStack("empty");
    const harness = await createHarness({ providerService: stack.providerService });
    const threadId = asThreadId("thread-1");
    const providerTurnId = asTurnId("019f682e-41b8-7a13-9074-e6404b1747b0");
    const createdAt = "2026-01-01T00:00:01.000Z";

    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-recorded-codex-empty-turn"),
          threadId,
          message: {
            messageId: asMessageId("message-recorded-codex-empty-turn"),
            role: "user",
            text: "Reply exactly PONG.",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt,
        },
        testDispatchAuthority,
      ),
    );
    await Effect.runPromise(
      stack.providerService.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: harness.modelSelection.instanceId,
        threadId,
        cwd: harness.workspaceRoot,
        modelSelection: harness.modelSelection,
        runtimeMode: "approval-required",
      }),
    );
    await Effect.runPromise(
      stack.providerService.sendTurn({
        threadId,
        input: "Reply exactly PONG.",
        modelSelection: harness.modelSelection,
        attachments: [],
      }),
    );

    const failedThread = await waitForThread(
      harness.readModel,
      (thread) =>
        thread.latestTurn?.turnId === providerTurnId &&
        thread.latestTurn.state === "error" &&
        thread.session?.status === "error" &&
        thread.session.activeTurnId === null,
      5_000,
      threadId,
    );
    await harness.drain();

    expect(failedThread.session?.lastError).toBe(
      "Provider completed the turn without emitting an assistant response.",
    );
    expect(failedThread.messages.filter((message) => message.role === "assistant")).toEqual([]);
    expect(await harness.readProjectedTurns(threadId)).toEqual([
      { turnId: providerTurnId, state: "error" },
    ]);
  });

  it("lets the watchdog fail a recorded Codex turn after process death loses its terminal event", async () => {
    const stack = await createRecordedCodexStack("death");
    const providerServiceWithoutTerminalEvents: ProviderServiceShape = {
      ...stack.providerService,
      streamEvents: stack.providerService.streamEvents.pipe(
        Stream.filter((event) => event.type !== "session.exited"),
      ),
    };
    const harness = await createHarness({
      providerService: providerServiceWithoutTerminalEvents,
    });
    const threadId = asThreadId("thread-1");
    const providerTurnId = asTurnId("019f682e-41b8-7a13-9074-e6404b1747b0");
    const createdAt = "2026-01-01T00:00:01.000Z";

    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-recorded-codex-process-death"),
          threadId,
          message: {
            messageId: asMessageId("message-recorded-codex-process-death"),
            role: "user",
            text: "Reply exactly PONG.",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt,
        },
        testDispatchAuthority,
      ),
    );
    await Effect.runPromise(
      stack.providerService.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: harness.modelSelection.instanceId,
        threadId,
        cwd: harness.workspaceRoot,
        modelSelection: harness.modelSelection,
        runtimeMode: "approval-required",
      }),
    );
    await Effect.runPromise(
      stack.providerService.sendTurn({
        threadId,
        input: "Reply exactly PONG.",
        modelSelection: harness.modelSelection,
        attachments: [],
      }),
    );

    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.latestTurn?.turnId === providerTurnId &&
        thread.latestTurn.state === "running" &&
        thread.session?.activeTurnId === providerTurnId,
      5_000,
      threadId,
    );
    await startProviderWatchdog(harness, stack);
    const failedThread = await waitForThread(
      harness.readModel,
      (thread) =>
        thread.latestTurn?.turnId === providerTurnId &&
        thread.latestTurn.state === "error" &&
        thread.session?.status === "stopped" &&
        thread.session.activeTurnId === null,
      5_000,
      threadId,
    );
    await harness.drain();

    expect(failedThread.session?.lastError).toContain("Provider session disappeared while turn");
    const binding = Option.getOrThrow(
      await Effect.runPromise(stack.directory.getBinding(threadId)),
    );
    expect(binding.runtimePayload).toMatchObject({
      activeTurnId: null,
      lastTerminalTurnId: providerTurnId,
      lastRuntimeEvent: "provider.turn.watchdog.failed",
    });
    expect(await harness.readProjectedTurns(threadId)).toEqual([
      { turnId: providerTurnId, state: "error" },
    ]);
  });

  xit.each(["codex", "grok", "claudeAgent"] as const)(
    "settles a detached %s child and releases its dispatch lease from the provider completion sequence",
    async (driver) => {
      const providerDriver = ProviderDriverKind.make(driver);
      const harness = await createHarness({ provider: providerDriver });
      const childThreadId = asThreadId("thread-1");
      const parentThreadId = asThreadId(`detached-parent-${driver}`);
      const turnId = asTurnId(`detached-turn-${driver}`);
      const createdAt = "2026-01-01T00:00:01.000Z";

      await harness.acquireDispatchLease(childThreadId);
      await Effect.runPromise(
        harness.coordinator.register({
          parentThreadId,
          childThreadId,
          detached: true,
          model: harness.modelSelection,
          spawnedAtMs: 1,
        }),
      );
      await Effect.runPromise(
        harness.engine.dispatch(
          {
            type: "thread.turn.start",
            commandId: CommandId.make(`cmd-detached-turn-${driver}`),
            threadId: childThreadId,
            message: {
              messageId: asMessageId(`message-detached-turn-${driver}`),
              role: "user",
              text: `complete the detached ${driver} child`,
              attachments: [],
            },
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            runtimeMode: "approval-required",
            providerSessionDetached: true,
            createdAt,
          },
          testDispatchAuthority,
        ),
      );

      expect(await harness.readProjectedTurns(childThreadId)).toEqual([
        { turnId: null, state: "pending" },
      ]);
      expect(await harness.leasedChildThreadIds()).toEqual([childThreadId]);

      // CodexAdapter maps the real app-server turn/started -> item/completed ->
      // turn/completed notification order into these canonical runtime events.
      // Grok and Claude produce the same canonical sequence from per-child sessions.
      harness.emit({
        type: "turn.started",
        eventId: asEventId(`evt-detached-turn-started-${driver}`),
        provider: providerDriver,
        threadId: childThreadId,
        turnId,
        createdAt,
        payload: {},
      });
      harness.emit({
        type: "item.completed",
        eventId: asEventId(`evt-detached-item-completed-${driver}`),
        provider: providerDriver,
        threadId: childThreadId,
        turnId,
        itemId: asItemId(`detached-message-${driver}`),
        createdAt: "2026-01-01T00:00:02.000Z",
        payload: {
          itemType: "assistant_message",
          status: "completed",
          detail: `${driver} detached child completed`,
        },
      });
      harness.emit({
        type: "turn.completed",
        eventId: asEventId(`evt-detached-turn-completed-${driver}`),
        provider: providerDriver,
        threadId: childThreadId,
        turnId,
        createdAt: "2026-01-01T00:00:03.000Z",
        payload: { state: "completed" },
      });

      await harness.drain();
      const completedThread = await waitForThread(
        harness.readModel,
        (thread) => thread.latestTurn?.turnId === turnId && thread.latestTurn.state === "completed",
      );
      expect(completedThread.latestTurn?.state).toBe("completed");
      expect(await harness.readProjectedTurns(childThreadId)).toEqual([
        { turnId, state: "completed" },
      ]);

      const childResult = await Effect.runPromise(
        harness.coordinator.waitSlice({
          childThreadIds: [childThreadId],
          mode: "all",
          budgetDeadlineMs: Number.MAX_SAFE_INTEGER,
        }),
      );
      expect(childResult.results).toEqual([
        {
          childThreadId,
          status: "completed",
          finalAssistantText: `${driver} detached child completed`,
          error: null,
          parentTurnIdAtWait: null,
        },
      ]);
      expect(await harness.leasedChildThreadIds()).toEqual([]);
    },
  );

  it("applies provider session.state.changed transitions directly", async () => {
    const harness = await createHarness();
    const waitingAt = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "session.state.changed",
      eventId: asEventId("evt-session-state-waiting"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: waitingAt,
      payload: {
        state: "waiting",
        reason: "awaiting approval",
      },
    });

    let thread = await waitForThread(
      harness.readModel,
      (entry) => entry.session?.status === "waiting" && entry.session?.activeTurnId === null,
    );
    expect(thread.session?.status).toBe("waiting");
    expect(thread.session?.lastError).toBeNull();

    harness.emit({
      type: "session.state.changed",
      eventId: asEventId("evt-session-state-error"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: "2026-01-01T00:00:00.000Z",
      payload: {
        state: "error",
        reason: "provider crashed",
      },
    });

    thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.session?.status === "error" &&
        entry.session?.activeTurnId === null &&
        entry.session?.lastError === "provider crashed",
    );
    expect(thread.session?.status).toBe("error");
    expect(thread.session?.lastError).toBe("provider crashed");

    harness.emit({
      type: "session.state.changed",
      eventId: asEventId("evt-session-state-stopped"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: "2026-01-01T00:00:00.000Z",
      payload: {
        state: "stopped",
      },
    });

    thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.session?.status === "stopped" &&
        entry.session?.activeTurnId === null &&
        entry.session?.lastError === "provider crashed",
    );
    expect(thread.session?.status).toBe("stopped");
    expect(thread.session?.lastError).toBe("provider crashed");

    harness.emit({
      type: "session.state.changed",
      eventId: asEventId("evt-session-state-ready"),
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      createdAt: "2026-01-01T00:00:00.000Z",
      payload: {
        state: "ready",
      },
    });

    thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.session?.status === "ready" &&
        entry.session?.activeTurnId === null &&
        entry.session?.lastError === null,
    );
    expect(thread.session?.status).toBe("ready");
    expect(thread.session?.lastError).toBeNull();
  });

  xit("does not clear active turn when session/thread started arrives mid-turn", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-midturn-lifecycle"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-midturn-lifecycle"),
    });

    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-midturn-lifecycle",
    );

    harness.emit({
      type: "thread.started",
      eventId: asEventId("evt-thread-started-midturn-lifecycle"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: asThreadId("thread-1"),
    });
    harness.emit({
      type: "session.started",
      eventId: asEventId("evt-session-started-midturn-lifecycle"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: asThreadId("thread-1"),
    });

    await harness.drain();
    const midReadModel = await harness.readModel();
    const midThread = midReadModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(midThread?.session?.status).toBe("running");
    expect(midThread?.session?.activeTurnId).toBe("turn-midturn-lifecycle");

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-turn-completed-midturn-lifecycle"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-midturn-lifecycle"),
      status: "completed",
    });

    await waitForThread(
      harness.readModel,
      (thread) => thread.session?.status === "ready" && thread.session?.activeTurnId === null,
    );
  });

  xit("accepts claude turn lifecycle when seeded thread id is a synthetic placeholder", async () => {
    const harness = await createHarness();
    const seededAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.session.set",
          commandId: CommandId.make("cmd-session-seed-claude-placeholder"),
          threadId: ThreadId.make("thread-1"),
          session: {
            threadId: ThreadId.make("thread-1"),
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "approval-required",
            activeTurnId: null,
            updatedAt: seededAt,
            lastError: null,
          },
          createdAt: seededAt,
        },
        testDispatchAuthority,
      ),
    );

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-claude-placeholder"),
      provider: ProviderDriverKind.make("claudeAgent"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-claude-placeholder"),
    });

    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-claude-placeholder",
    );

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-turn-completed-claude-placeholder"),
      provider: ProviderDriverKind.make("claudeAgent"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-claude-placeholder"),
      status: "completed",
    });

    await waitForThread(
      harness.readModel,
      (thread) => thread.session?.status === "ready" && thread.session?.activeTurnId === null,
    );
  });

  xit("ignores auxiliary turn completions from a different provider thread", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-primary"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-primary"),
    });

    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" && thread.session?.activeTurnId === "turn-primary",
    );

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-turn-completed-aux"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-aux"),
      status: "completed",
    });

    await harness.drain();
    const midReadModel = await harness.readModel();
    const midThread = midReadModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(midThread?.session?.status).toBe("running");
    expect(midThread?.session?.activeTurnId).toBe("turn-primary");

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-turn-completed-primary"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-primary"),
      status: "completed",
    });

    await waitForThread(
      harness.readModel,
      (thread) => thread.session?.status === "ready" && thread.session?.activeTurnId === null,
    );
  });

  xit("ignores non-active turn completion when runtime omits thread id", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-guarded"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-guarded-main"),
    });

    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-guarded-main",
    );

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-turn-completed-guarded-other"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-guarded-other"),
      status: "completed",
    });

    await harness.drain();
    const midReadModel = await harness.readModel();
    const midThread = midReadModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(midThread?.session?.status).toBe("running");
    expect(midThread?.session?.activeTurnId).toBe("turn-guarded-main");

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-turn-completed-guarded-main"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-guarded-main"),
      status: "completed",
    });

    await waitForThread(
      harness.readModel,
      (thread) => thread.session?.status === "ready" && thread.session?.activeTurnId === null,
    );
  });

  xit("maps canonical content delta/item completed into finalized assistant messages", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-message-delta-1"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-2"),
      itemId: asItemId("item-1"),
      payload: {
        streamKind: "assistant_text",
        delta: "hello",
      },
    });
    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-message-delta-2"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-2"),
      itemId: asItemId("item-1"),
      payload: {
        streamKind: "assistant_text",
        delta: " world",
      },
    });
    harness.emit({
      type: "item.completed",
      eventId: asEventId("evt-message-completed"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-2"),
      itemId: asItemId("item-1"),
      payload: {
        itemType: "assistant_message",
        status: "completed",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-1" && !message.streaming,
      ),
    );
    const message = thread.messages.find(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:item-1",
    );
    expect(message?.text).toBe("hello world");
    expect(message?.streaming).toBe(false);
  });

  xit("uses assistant item completion detail when no assistant deltas were streamed", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "item.completed",
      eventId: asEventId("evt-assistant-item-completed-no-delta"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-no-delta"),
      itemId: asItemId("item-no-delta"),
      payload: {
        itemType: "assistant_message",
        status: "completed",
        detail: "assistant-only final text",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-no-delta" && !message.streaming,
      ),
    );
    const message = thread.messages.find(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:item-no-delta",
    );
    expect(message?.text).toBe("assistant-only final text");
    expect(message?.streaming).toBe(false);
  });

  xit("preserves completed tool metadata on projected tool activities", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "item.completed",
      eventId: asEventId("evt-tool-completed-with-data"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-tool-completed"),
      itemId: asItemId("item-tool-completed"),
      payload: {
        itemType: "dynamic_tool_call",
        status: "completed",
        title: "Read file",
        data: {
          toolCallId: "tool-read-1",
          kind: "read",
          rawOutput: {
            content: 'import * as Effect from "effect/Effect"\n',
          },
        },
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.activities.some(
        (activity: ProviderRuntimeTestActivity) => activity.id === "evt-tool-completed-with-data",
      ),
    );
    const activity = thread.activities.find(
      (entry: ProviderRuntimeTestActivity) => entry.id === "evt-tool-completed-with-data",
    );
    const payload =
      activity?.payload && typeof activity.payload === "object"
        ? (activity.payload as Record<string, unknown>)
        : undefined;
    const data =
      payload?.data && typeof payload.data === "object"
        ? (payload.data as Record<string, unknown>)
        : undefined;
    const rawOutput =
      data?.rawOutput && typeof data.rawOutput === "object"
        ? (data.rawOutput as Record<string, unknown>)
        : undefined;

    expect(activity?.kind).toBe("tool.completed");
    expect(activity?.summary).toBe("Read file");
    expect(payload?.itemType).toBe("dynamic_tool_call");
    expect(payload?.detail).toBeUndefined();
    expect(data?.toolCallId).toBe("tool-read-1");
    expect(data?.kind).toBe("read");
    expect(rawOutput?.content).toBe('import * as Effect from "effect/Effect"\n');
  });

  xit("normalizes command execution activities to ran-command summaries", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "item.completed",
      eventId: asEventId("evt-command-completed"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-command-completed"),
      itemId: asItemId("item-command-completed"),
      payload: {
        itemType: "command_execution",
        status: "completed",
        title: "Ran command",
        detail: "bun run lint",
        data: {
          toolCallId: "tool-command-1",
          kind: "execute",
          command: "bun run lint",
        },
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.activities.some(
        (activity: ProviderRuntimeTestActivity) => activity.id === "evt-command-completed",
      ),
    );
    const activity = thread.activities.find(
      (entry: ProviderRuntimeTestActivity) => entry.id === "evt-command-completed",
    );
    const payload =
      activity?.payload && typeof activity.payload === "object"
        ? (activity.payload as Record<string, unknown>)
        : undefined;

    expect(activity?.summary).toBe("Ran command");
    expect(payload?.detail).toBe("bun run lint");
  });

  xit("uses structured read-file paths when available", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "item.completed",
      eventId: asEventId("evt-read-path-completed"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-read-path"),
      itemId: asItemId("item-read-path"),
      payload: {
        itemType: "dynamic_tool_call",
        status: "completed",
        title: "Read file",
        detail: "/tmp/app.ts",
        data: {
          toolCallId: "tool-read-path-1",
          kind: "read",
          locations: [{ path: "/tmp/app.ts" }],
        },
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.activities.some(
        (activity: ProviderRuntimeTestActivity) => activity.id === "evt-read-path-completed",
      ),
    );
    const activity = thread.activities.find(
      (entry: ProviderRuntimeTestActivity) => entry.id === "evt-read-path-completed",
    );
    const payload =
      activity?.payload && typeof activity.payload === "object"
        ? (activity.payload as Record<string, unknown>)
        : undefined;

    expect(activity?.summary).toBe("Read file");
    expect(payload?.detail).toBe("/tmp/app.ts");
  });

  xit("projects completed plan items into first-class proposed plans", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "turn.proposed.completed",
      eventId: asEventId("evt-plan-item-completed"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-plan-final"),
      payload: {
        planMarkdown: "## Ship plan\n\n- wire projection\n- render follow-up",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.proposedPlans.some(
        (proposedPlan: ProviderRuntimeTestProposedPlan) =>
          proposedPlan.id === "plan:thread-1:turn:turn-plan-final",
      ),
    );
    const proposedPlan = thread.proposedPlans.find(
      (entry: ProviderRuntimeTestProposedPlan) => entry.id === "plan:thread-1:turn:turn-plan-final",
    );
    expect(proposedPlan?.planMarkdown).toBe(
      "## Ship plan\n\n- wire projection\n- render follow-up",
    );
  });

  xit("marks the source proposed plan implemented only after the target turn starts", async () => {
    const harness = await createHarness();
    const sourceThreadId = asThreadId("thread-plan");
    const targetThreadId = asThreadId("thread-implement");
    const sourceTurnId = asTurnId("turn-plan-source");
    const targetTurnId = asTurnId("turn-plan-implement");
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-create-plan-source"),
          threadId: sourceThreadId,
          projectId: asProjectId("project-1"),
          title: "Plan Source",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: "plan",
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        },
        testDispatchAuthority,
      ),
    );
    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.session.set",
          commandId: CommandId.make("cmd-session-set-plan-source"),
          threadId: sourceThreadId,
          session: {
            threadId: sourceThreadId,
            status: "ready",
            providerName: "codex",
            runtimeMode: "approval-required",
            activeTurnId: null,
            updatedAt: createdAt,
            lastError: null,
          },
          createdAt,
        },
        testDispatchAuthority,
      ),
    );
    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-create-plan-target"),
          threadId: targetThreadId,
          projectId: asProjectId("project-1"),
          title: "Plan Target",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        },
        testDispatchAuthority,
      ),
    );
    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.session.set",
          commandId: CommandId.make("cmd-session-set-plan-target"),
          threadId: targetThreadId,
          session: {
            threadId: targetThreadId,
            status: "ready",
            providerName: "codex",
            runtimeMode: "approval-required",
            activeTurnId: null,
            updatedAt: createdAt,
            lastError: null,
          },
          createdAt,
        },
        testDispatchAuthority,
      ),
    );
    harness.setProviderSession({
      provider: ProviderDriverKind.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      threadId: targetThreadId,
      createdAt,
      updatedAt: createdAt,
      activeTurnId: targetTurnId,
    });

    harness.emit({
      type: "turn.proposed.completed",
      eventId: asEventId("evt-plan-source-completed"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId: sourceThreadId,
      turnId: sourceTurnId,
      payload: {
        planMarkdown: "# Source plan",
      },
    });

    const sourceThreadWithPlan = await waitForThread(
      harness.readModel,
      (thread) =>
        thread.proposedPlans.some(
          (proposedPlan: ProviderRuntimeTestProposedPlan) =>
            proposedPlan.id === "plan:thread-plan:turn:turn-plan-source" &&
            proposedPlan.implementedAt === null,
        ),
      2_000,
      sourceThreadId,
    );
    const sourcePlan = sourceThreadWithPlan.proposedPlans.find(
      (entry: ProviderRuntimeTestProposedPlan) =>
        entry.id === "plan:thread-plan:turn:turn-plan-source",
    );
    expect(sourcePlan).toBeDefined();
    if (!sourcePlan) {
      throw new Error("Expected source plan to exist.");
    }

    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-turn-start-plan-target"),
          threadId: targetThreadId,
          message: {
            messageId: asMessageId("msg-plan-target"),
            role: "user",
            text: "PLEASE IMPLEMENT THIS PLAN:\n# Source plan",
            attachments: [],
          },
          sourceProposedPlan: {
            threadId: sourceThreadId,
            planId: sourcePlan.id,
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt: "2026-01-01T00:00:00.000Z",
        },
        testDispatchAuthority,
      ),
    );

    const sourceThreadBeforeStart = await waitForThread(
      harness.readModel,
      (thread) =>
        thread.proposedPlans.some(
          (proposedPlan: ProviderRuntimeTestProposedPlan) =>
            proposedPlan.id === sourcePlan.id && proposedPlan.implementedAt === null,
        ),
      2_000,
      sourceThreadId,
    );
    expect(
      sourceThreadBeforeStart.proposedPlans.find((entry) => entry.id === sourcePlan.id),
    ).toMatchObject({
      implementedAt: null,
      implementationThreadId: null,
    });

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-plan-target-started"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: targetThreadId,
      turnId: targetTurnId,
    });

    const sourceThreadAfterStart = await waitForThread(
      harness.readModel,
      (thread) =>
        thread.proposedPlans.some(
          (proposedPlan: ProviderRuntimeTestProposedPlan) =>
            proposedPlan.id === sourcePlan.id &&
            proposedPlan.implementedAt !== null &&
            proposedPlan.implementationThreadId === targetThreadId,
        ),
      2_000,
      sourceThreadId,
    );
    expect(
      sourceThreadAfterStart.proposedPlans.find((entry) => entry.id === sourcePlan.id),
    ).toMatchObject({
      implementationThreadId: "thread-implement",
    });
  });

  xit("does not mark the source proposed plan implemented for a rejected turn.started event", async () => {
    const harness = await createHarness();
    const sourceThreadId = asThreadId("thread-plan");
    const targetThreadId = asThreadId("thread-1");
    const sourceTurnId = asTurnId("turn-plan-source");
    const activeTurnId = asTurnId("turn-already-running");
    const staleTurnId = asTurnId("turn-stale-start");
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      Effect.andThen(
        harness.engine.dispatch(
          {
            type: "thread.create",
            commandId: CommandId.make("cmd-thread-create-plan-source-guarded"),
            threadId: sourceThreadId,
            projectId: asProjectId("project-1"),
            title: "Plan Source",
            modelSelection: {
              instanceId: ProviderInstanceId.make("codex"),
              model: "gpt-5-codex",
            },
            interactionMode: "plan",
            runtimeMode: "approval-required",
            branch: null,
            worktreePath: null,
            createdAt,
          },
          testDispatchAuthority,
        ),
        harness.engine.dispatch(
          {
            type: "thread.session.set",
            commandId: CommandId.make("cmd-session-set-plan-source-guarded"),
            threadId: sourceThreadId,
            session: {
              threadId: sourceThreadId,
              status: "ready",
              providerName: "codex",
              runtimeMode: "approval-required",
              activeTurnId: null,
              updatedAt: createdAt,
              lastError: null,
            },
            createdAt,
          },
          testDispatchAuthority,
        ),
      ),
    );
    harness.setProviderSession({
      provider: ProviderDriverKind.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      threadId: targetThreadId,
      createdAt,
      updatedAt: createdAt,
      activeTurnId,
    });

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-already-running"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId: targetThreadId,
      turnId: activeTurnId,
    });

    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" && thread.session?.activeTurnId === activeTurnId,
      2_000,
      targetThreadId,
    );

    harness.emit({
      type: "turn.proposed.completed",
      eventId: asEventId("evt-plan-source-completed-guarded"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId: sourceThreadId,
      turnId: sourceTurnId,
      payload: {
        planMarkdown: "# Source plan",
      },
    });

    const sourceThreadWithPlan = await waitForThread(
      harness.readModel,
      (thread) =>
        thread.proposedPlans.some(
          (proposedPlan: ProviderRuntimeTestProposedPlan) =>
            proposedPlan.id === "plan:thread-plan:turn:turn-plan-source" &&
            proposedPlan.implementedAt === null,
        ),
      2_000,
      sourceThreadId,
    );
    const sourcePlan = sourceThreadWithPlan.proposedPlans.find(
      (entry: ProviderRuntimeTestProposedPlan) =>
        entry.id === "plan:thread-plan:turn:turn-plan-source",
    );
    expect(sourcePlan).toBeDefined();
    if (!sourcePlan) {
      throw new Error("Expected source plan to exist.");
    }

    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-turn-start-plan-target-guarded"),
          threadId: targetThreadId,
          message: {
            messageId: asMessageId("msg-plan-target-guarded"),
            role: "user",
            text: "PLEASE IMPLEMENT THIS PLAN:\n# Source plan",
            attachments: [],
          },
          sourceProposedPlan: {
            threadId: sourceThreadId,
            planId: sourcePlan.id,
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt: "2026-01-01T00:00:00.000Z",
        },
        testDispatchAuthority,
      ),
    );

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-stale-plan-implementation"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: targetThreadId,
      turnId: staleTurnId,
    });

    await harness.drain();

    const readModel = await harness.readModel();
    const sourceThreadAfterRejectedStart = readModel.threads.find(
      (entry) => entry.id === sourceThreadId,
    );
    expect(
      sourceThreadAfterRejectedStart?.proposedPlans.find((entry) => entry.id === sourcePlan.id),
    ).toMatchObject({
      implementedAt: null,
      implementationThreadId: null,
    });

    const targetThreadAfterRejectedStart = readModel.threads.find(
      (entry) => entry.id === targetThreadId,
    );
    expect(targetThreadAfterRejectedStart?.session?.status).toBe("running");
    expect(targetThreadAfterRejectedStart?.session?.activeTurnId).toBe(activeTurnId);
  });

  xit("accepts a conflicting turn.started for a pending turn start when the provider expects that turn", async () => {
    // Steering a running turn: the server requests a new turn while the old
    // one is still active, and providers like opencode open the new turn
    // without ever completing the superseded one. The new turn.started must
    // replace the active turn instead of being rejected as stale.
    const harness = await createHarness();
    const threadId = asThreadId("thread-1");
    const oldTurnId = asTurnId("turn-steered-over");
    const newTurnId = asTurnId("turn-from-steer");
    const createdAt = "2026-01-01T00:00:00.000Z";

    harness.setProviderSession({
      provider: ProviderDriverKind.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      threadId,
      createdAt,
      updatedAt: createdAt,
      activeTurnId: oldTurnId,
    });
    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-steered-over"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId,
      turnId: oldTurnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" && thread.session?.activeTurnId === oldTurnId,
      2_000,
      threadId,
    );

    // The steer: a user-requested turn start while the old turn still runs.
    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-turn-start-steer"),
          threadId,
          message: {
            messageId: asMessageId("msg-steer"),
            role: "user",
            text: "actually, do 15 instead",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt,
        },
        testDispatchAuthority,
      ),
    );

    // The provider session tracks the new turn before emitting turn.started
    // (sendTurn updates the session first).
    harness.setProviderSession({
      provider: ProviderDriverKind.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      threadId,
      createdAt,
      updatedAt: createdAt,
      activeTurnId: newTurnId,
    });
    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-from-steer"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId,
      turnId: newTurnId,
    });

    const threadAfterSteer = await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" && thread.session?.activeTurnId === newTurnId,
      2_000,
      threadId,
    );
    expect(threadAfterSteer.session?.activeTurnId).toBe(newTurnId);
    expect(threadAfterSteer.latestTurn?.turnId).toBe(newTurnId);
    expect(threadAfterSteer.latestTurn?.state).toBe("running");

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-late-delta-for-steered-over-turn"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId,
      turnId: oldTurnId,
      itemId: asItemId("late-steered-over-item"),
      payload: {
        streamKind: "assistant_text",
        delta: "late old turn text",
      },
    });

    const threadWithLateOldTurnDelta = await waitForThread(
      harness.readModel,
      (thread) =>
        thread.messages.some(
          (message: ProviderRuntimeTestMessage) =>
            message.id === "assistant:late-steered-over-item" &&
            message.turnId === oldTurnId &&
            message.text === "late old turn text" &&
            !message.streaming,
        ),
      2_000,
      threadId,
    );
    const lateOldTurnMessage = threadWithLateOldTurnDelta.messages.find(
      (message: ProviderRuntimeTestMessage) => message.id === "assistant:late-steered-over-item",
    );
    expect(lateOldTurnMessage?.streaming).toBe(false);
  });

  xit("does not mark the source proposed plan implemented for an unrelated turn.started when no thread active turn is tracked", async () => {
    const harness = await createHarness();
    const sourceThreadId = asThreadId("thread-plan");
    const targetThreadId = asThreadId("thread-implement");
    const sourceTurnId = asTurnId("turn-plan-source");
    const expectedTurnId = asTurnId("turn-plan-implement");
    const replayedTurnId = asTurnId("turn-replayed");
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-create-plan-source-unrelated"),
          threadId: sourceThreadId,
          projectId: asProjectId("project-1"),
          title: "Plan Source",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: "plan",
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        },
        testDispatchAuthority,
      ),
    );
    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.session.set",
          commandId: CommandId.make("cmd-session-set-plan-source-unrelated"),
          threadId: sourceThreadId,
          session: {
            threadId: sourceThreadId,
            status: "ready",
            providerName: "codex",
            runtimeMode: "approval-required",
            activeTurnId: null,
            updatedAt: createdAt,
            lastError: null,
          },
          createdAt,
        },
        testDispatchAuthority,
      ),
    );
    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-create-plan-target-unrelated"),
          threadId: targetThreadId,
          projectId: asProjectId("project-1"),
          title: "Plan Target",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt,
        },
        testDispatchAuthority,
      ),
    );
    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.session.set",
          commandId: CommandId.make("cmd-session-set-plan-target-unrelated"),
          threadId: targetThreadId,
          session: {
            threadId: targetThreadId,
            status: "ready",
            providerName: "codex",
            runtimeMode: "approval-required",
            activeTurnId: null,
            updatedAt: createdAt,
            lastError: null,
          },
          createdAt,
        },
        testDispatchAuthority,
      ),
    );

    harness.emit({
      type: "turn.proposed.completed",
      eventId: asEventId("evt-plan-source-completed-unrelated"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId: sourceThreadId,
      turnId: sourceTurnId,
      payload: {
        planMarkdown: "# Source plan",
      },
    });

    const sourceThreadWithPlan = await waitForThread(
      harness.readModel,
      (thread) =>
        thread.proposedPlans.some(
          (proposedPlan: ProviderRuntimeTestProposedPlan) =>
            proposedPlan.id === "plan:thread-plan:turn:turn-plan-source" &&
            proposedPlan.implementedAt === null,
        ),
      2_000,
      sourceThreadId,
    );
    const sourcePlan = sourceThreadWithPlan.proposedPlans.find(
      (entry: ProviderRuntimeTestProposedPlan) =>
        entry.id === "plan:thread-plan:turn:turn-plan-source",
    );
    expect(sourcePlan).toBeDefined();
    if (!sourcePlan) {
      throw new Error("Expected source plan to exist.");
    }

    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-turn-start-plan-target-unrelated"),
          threadId: targetThreadId,
          message: {
            messageId: asMessageId("msg-plan-target-unrelated"),
            role: "user",
            text: "PLEASE IMPLEMENT THIS PLAN:\n# Source plan",
            attachments: [],
          },
          sourceProposedPlan: {
            threadId: sourceThreadId,
            planId: sourcePlan.id,
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt: "2026-01-01T00:00:00.000Z",
        },
        testDispatchAuthority,
      ),
    );

    harness.setProviderSession({
      provider: ProviderDriverKind.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      threadId: targetThreadId,
      createdAt,
      updatedAt: createdAt,
      activeTurnId: expectedTurnId,
    });

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-unrelated-plan-implementation"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: targetThreadId,
      turnId: replayedTurnId,
    });

    await harness.drain();

    const readModel = await harness.readModel();
    const sourceThreadAfterUnrelatedStart = readModel.threads.find(
      (entry) => entry.id === sourceThreadId,
    );
    expect(
      sourceThreadAfterUnrelatedStart?.proposedPlans.find((entry) => entry.id === sourcePlan.id),
    ).toMatchObject({
      implementedAt: null,
      implementationThreadId: null,
    });
  });

  xit("finalizes buffered proposed-plan deltas into a first-class proposed plan on turn completion", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-plan-buffer"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-plan-buffer"),
    });

    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" && thread.session?.activeTurnId === "turn-plan-buffer",
    );

    harness.emit({
      type: "turn.proposed.delta",
      eventId: asEventId("evt-plan-delta-1"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-plan-buffer"),
      payload: {
        delta: "## Buffered plan\n\n- first",
      },
    });
    harness.emit({
      type: "turn.proposed.delta",
      eventId: asEventId("evt-plan-delta-2"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-plan-buffer"),
      payload: {
        delta: "\n- second",
      },
    });
    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-turn-completed-plan-buffer"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-plan-buffer"),
      payload: {
        state: "completed",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.proposedPlans.some(
        (proposedPlan: ProviderRuntimeTestProposedPlan) =>
          proposedPlan.id === "plan:thread-1:turn:turn-plan-buffer",
      ),
    );
    const proposedPlan = thread.proposedPlans.find(
      (entry: ProviderRuntimeTestProposedPlan) =>
        entry.id === "plan:thread-1:turn:turn-plan-buffer",
    );
    expect(proposedPlan?.planMarkdown).toBe("## Buffered plan\n\n- first\n- second");
  });

  xit("buffers assistant deltas by default until completion", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-buffered"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered"),
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" && thread.session?.activeTurnId === "turn-buffered",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-message-delta-buffered"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered"),
      itemId: asItemId("item-buffered"),
      payload: {
        streamKind: "assistant_text",
        delta: "buffer me",
      },
    });

    await harness.drain();
    const midReadModel = await harness.readModel();
    const midThread = midReadModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      midThread?.messages.some(
        (message: ProviderRuntimeTestMessage) => message.id === "assistant:item-buffered",
      ),
    ).toBe(false);

    harness.emit({
      type: "item.completed",
      eventId: asEventId("evt-message-completed-buffered"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered"),
      itemId: asItemId("item-buffered"),
      payload: {
        itemType: "assistant_message",
        status: "completed",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-buffered" && !message.streaming,
      ),
    );
    const message = thread.messages.find(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:item-buffered",
    );
    expect(message?.text).toBe("buffer me");
    expect(message?.streaming).toBe(false);
  });

  xit("finalizes Cursor assistant deltas that arrive after turn completion", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const threadId = asThreadId("thread-1");
    const turnId = asTurnId("turn-cursor-late-delta");

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-cursor-late-delta-started"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-cursor-late-delta",
    );

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-cursor-late-delta-completed"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      payload: {
        state: "completed",
        stopReason: "end_turn",
      },
    });
    await waitForThread(
      harness.readModel,
      (thread) => thread.session?.status === "ready" && thread.session?.activeTurnId === null,
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-cursor-late-delta-text"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      itemId: asItemId("cursor-late-item"),
      payload: {
        streamKind: "assistant_text",
        delta: "MODEL_OK composer",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:cursor-late-item" &&
          message.turnId === turnId &&
          message.text === "MODEL_OK composer" &&
          !message.streaming,
      ),
    );
    const message = thread.messages.find(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:cursor-late-item",
    );
    expect(message?.text).toBe("MODEL_OK composer");
    expect(message?.turnId).toBe(turnId);
    expect(message?.streaming).toBe(false);
  });

  xit("keeps split Cursor assistant deltas on one message across terminal ordering", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const threadId = asThreadId("thread-1");
    const turnId = asTurnId("turn-cursor-split-terminal-delta");

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-cursor-split-delta-started"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-cursor-split-terminal-delta",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-cursor-split-delta-before-complete"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      itemId: asItemId("cursor-split-item"),
      payload: {
        streamKind: "assistant_text",
        delta: "MODEL",
      },
    });
    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-cursor-split-delta-completed"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      payload: {
        state: "completed",
        stopReason: "end_turn",
      },
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "ready" &&
        thread.messages.some(
          (message: ProviderRuntimeTestMessage) =>
            message.id === "assistant:cursor-split-item" &&
            message.text === "MODEL" &&
            !message.streaming,
        ),
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-cursor-split-delta-after-complete-1"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      itemId: asItemId("cursor-split-item"),
      payload: {
        streamKind: "assistant_text",
        delta: "_OK",
      },
    });
    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-cursor-split-delta-after-complete-2"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      itemId: asItemId("cursor-split-item"),
      payload: {
        streamKind: "assistant_text",
        delta: " composer",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:cursor-split-item" &&
          message.turnId === turnId &&
          message.text === "MODEL_OK composer" &&
          !message.streaming,
      ),
    );
    const messages = thread.messages.filter(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:cursor-split-item",
    );
    expect(messages).toHaveLength(1);
    expect(messages[0]?.text).toBe("MODEL_OK composer");
    expect(messages[0]?.streaming).toBe(false);
  });

  xit("keeps late Cursor deltas on their explicit old turn when a new turn is active", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const threadId = asThreadId("thread-1");
    const oldTurnId = asTurnId("turn-cursor-old-late-delta");
    const newTurnId = asTurnId("turn-cursor-new-active");

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-cursor-old-late-started"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId: oldTurnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-cursor-old-late-delta",
    );
    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-cursor-old-late-completed"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId: oldTurnId,
      payload: {
        state: "completed",
      },
    });
    await waitForThread(
      harness.readModel,
      (thread) => thread.session?.status === "ready" && thread.session?.activeTurnId === null,
    );

    harness.setProviderSession({
      provider: ProviderDriverKind.make("cursor"),
      status: "running",
      runtimeMode: "approval-required",
      threadId,
      createdAt: now,
      updatedAt: now,
      activeTurnId: newTurnId,
    });
    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-cursor-new-active-started"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId: newTurnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-cursor-new-active",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-cursor-old-late-delta-text"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId: oldTurnId,
      itemId: asItemId("cursor-old-late-item"),
      payload: {
        streamKind: "assistant_text",
        delta: "old turn result",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:cursor-old-late-item" &&
          message.turnId === oldTurnId &&
          message.text === "old turn result" &&
          !message.streaming,
      ),
    );
    const message = thread.messages.find(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:cursor-old-late-item",
    );
    expect(message?.turnId).toBe(oldTurnId);
    expect(
      thread.messages.some(
        (entry: ProviderRuntimeTestMessage) =>
          entry.id === "assistant:cursor-old-late-item" && entry.turnId === newTurnId,
      ),
    ).toBe(false);
  });

  xit("remembers non-active Cursor turn completions before late deltas arrive", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const threadId = asThreadId("thread-1");
    const activeTurnId = asTurnId("turn-cursor-active-during-old-completion");
    const oldTurnId = asTurnId("turn-cursor-rejected-completion");

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-cursor-active-during-old-completion-started"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId: activeTurnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-cursor-active-during-old-completion",
    );

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-cursor-rejected-old-completion"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId: oldTurnId,
      payload: {
        state: "completed",
      },
    });
    await harness.drain();
    const afterRejectedCompletion = await harness.readModel();
    const activeThread = afterRejectedCompletion.threads.find((entry) => entry.id === threadId);
    expect(activeThread?.session?.status).toBe("running");
    expect(activeThread?.session?.activeTurnId).toBe(activeTurnId);

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-cursor-delta-after-rejected-old-completion"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId: oldTurnId,
      itemId: asItemId("cursor-rejected-old-item"),
      payload: {
        streamKind: "assistant_text",
        delta: "old completed text",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:cursor-rejected-old-item" &&
          message.turnId === oldTurnId &&
          message.text === "old completed text" &&
          !message.streaming,
      ),
    );
    const message = thread.messages.find(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:cursor-rejected-old-item",
    );
    expect(message?.turnId).toBe(oldTurnId);
    expect(thread.session?.activeTurnId).toBe(activeTurnId);
  });

  xit("completes streaming Cursor assistant deltas that arrive after turn completion", async () => {
    const harness = await createHarness({ serverSettings: { enableAssistantStreaming: true } });
    const now = "2026-01-01T00:00:00.000Z";
    const threadId = asThreadId("thread-1");
    const turnId = asTurnId("turn-cursor-streaming-late-delta");

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-cursor-streaming-late-started"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-cursor-streaming-late-delta",
    );

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-cursor-streaming-late-completed"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      payload: {
        state: "completed",
      },
    });
    await waitForThread(
      harness.readModel,
      (thread) => thread.session?.status === "ready" && thread.session?.activeTurnId === null,
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-cursor-streaming-late-delta"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      itemId: asItemId("cursor-streaming-late-item"),
      payload: {
        streamKind: "assistant_text",
        delta: "streamed after completion",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:cursor-streaming-late-item" &&
          message.turnId === turnId &&
          message.text === "streamed after completion" &&
          !message.streaming,
      ),
    );
    const message = thread.messages.find(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:cursor-streaming-late-item",
    );
    expect(message?.streaming).toBe(false);
  });

  xit("drops Cursor assistant deltas after cancelled or interrupted turn completion", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const threadId = asThreadId("thread-1");

    for (const terminalState of ["cancelled", "interrupted"] as const) {
      const turnId = asTurnId(`turn-cursor-${terminalState}-late-output`);
      const itemId = asItemId(`cursor-${terminalState}-late-item`);

      harness.emit({
        type: "turn.started",
        eventId: asEventId(`evt-cursor-${terminalState}-started`),
        provider: ProviderDriverKind.make("cursor"),
        createdAt: now,
        threadId,
        turnId,
      });
      await waitForThread(
        harness.readModel,
        (thread) =>
          thread.session?.status === "running" &&
          thread.session?.activeTurnId === `turn-cursor-${terminalState}-late-output`,
        2000,
        threadId,
      );

      harness.emit({
        type: "content.delta",
        eventId: asEventId(`evt-cursor-${terminalState}-before-terminal`),
        provider: ProviderDriverKind.make("cursor"),
        createdAt: now,
        threadId,
        turnId,
        itemId,
        payload: {
          streamKind: "assistant_text",
          delta: "text before cancel",
        },
      });
      harness.emit({
        type: "turn.completed",
        eventId: asEventId(`evt-cursor-${terminalState}-completed`),
        provider: ProviderDriverKind.make("cursor"),
        createdAt: now,
        threadId,
        turnId,
        payload: {
          state: terminalState,
          stopReason: terminalState,
        },
      });
      await waitForThread(
        harness.readModel,
        (thread) => thread.session?.status === "ready" && thread.session?.activeTurnId === null,
        2000,
        threadId,
      );

      harness.emit({
        type: "content.delta",
        eventId: asEventId(`evt-cursor-${terminalState}-after-terminal`),
        provider: ProviderDriverKind.make("cursor"),
        createdAt: now,
        threadId,
        turnId,
        itemId,
        payload: {
          streamKind: "assistant_text",
          delta: " late stale text",
        },
      });
      harness.emit({
        type: "item.completed",
        eventId: asEventId(`evt-cursor-${terminalState}-assistant-completed`),
        provider: ProviderDriverKind.make("cursor"),
        createdAt: now,
        threadId,
        turnId,
        itemId,
        payload: {
          itemType: "assistant_message",
          status: "completed",
        },
      });

      await harness.drain();
      const snapshot = await harness.readModel();
      const thread = snapshot.threads.find((entry) => entry.id === threadId);
      const message = thread?.messages.find(
        (entry: ProviderRuntimeTestMessage) =>
          entry.id === `assistant:cursor-${terminalState}-late-item`,
      );
      expect(message?.text).toBe("text before cancel");
      expect(message?.streaming).toBe(false);
    }
  });

  xit("finalizes Cursor assistant deltas that arrive after failed turn completion", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const threadId = asThreadId("thread-1");
    const turnId = asTurnId("turn-cursor-failed-late-delta");

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-cursor-failed-late-started"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-cursor-failed-late-delta",
    );

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-cursor-failed-late-completed"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      payload: {
        state: "failed",
        errorMessage: "Cursor failed after producing output",
      },
    });
    await waitForThread(
      harness.readModel,
      (thread) => thread.session?.status === "error" && thread.session?.activeTurnId === null,
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-cursor-failed-late-delta"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      itemId: asItemId("cursor-failed-late-item"),
      payload: {
        streamKind: "assistant_text",
        delta: "failed turn text",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:cursor-failed-late-item" &&
          message.turnId === turnId &&
          message.text === "failed turn text" &&
          !message.streaming,
      ),
    );
    const message = thread.messages.find(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:cursor-failed-late-item",
    );
    expect(message?.streaming).toBe(false);
  });

  xit("preserves buffered Cursor assistant text when a turn is cancelled", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const threadId = asThreadId("thread-1");
    const turnId = asTurnId("turn-cursor-buffered-cancelled");
    const itemId = asItemId("cursor-buffered-cancelled-item");

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-cursor-buffered-cancelled-started"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-cursor-buffered-cancelled",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-cursor-buffered-cancelled-before-terminal"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      itemId,
      payload: {
        streamKind: "assistant_text",
        delta: "visible before cancel",
      },
    });
    await harness.drain();
    expect((await harness.readModel()).threads[0]?.messages).toEqual([]);

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-cursor-buffered-cancelled-completed"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      payload: {
        state: "cancelled",
        stopReason: "cancelled",
      },
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "ready" &&
        thread.session?.activeTurnId === null &&
        thread.messages.some(
          (message: ProviderRuntimeTestMessage) =>
            message.id === "assistant:cursor-buffered-cancelled-item" &&
            message.text === "visible before cancel" &&
            !message.streaming,
        ),
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-cursor-buffered-cancelled-after-terminal"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      itemId,
      payload: {
        streamKind: "assistant_text",
        delta: " stale after cancel",
      },
    });
    await harness.drain();
    const snapshot = await harness.readModel();
    const thread = snapshot.threads.find((entry) => entry.id === threadId);
    const message = thread?.messages.find(
      (entry: ProviderRuntimeTestMessage) =>
        entry.id === "assistant:cursor-buffered-cancelled-item",
    );
    expect(message?.text).toBe("visible before cancel");
    expect(message?.streaming).toBe(false);
  });

  xit("completes already-streamed Cursor assistant text when a turn is cancelled", async () => {
    const harness = await createHarness({ serverSettings: { enableAssistantStreaming: true } });
    const now = "2026-01-01T00:00:00.000Z";
    const threadId = asThreadId("thread-1");
    const turnId = asTurnId("turn-cursor-streaming-cancelled");
    const itemId = asItemId("cursor-streaming-cancelled-item");

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-cursor-streaming-cancelled-started"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-cursor-streaming-cancelled",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-cursor-streaming-cancelled-before-terminal"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      itemId,
      payload: {
        streamKind: "assistant_text",
        delta: "visible before cancel",
      },
    });
    await waitForThread(harness.readModel, (thread) =>
      thread.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:cursor-streaming-cancelled-item" &&
          message.text === "visible before cancel" &&
          message.streaming,
      ),
    );

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-cursor-streaming-cancelled-completed"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      payload: {
        state: "cancelled",
        stopReason: "cancelled",
      },
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "ready" &&
        thread.session?.activeTurnId === null &&
        thread.messages.some(
          (message: ProviderRuntimeTestMessage) =>
            message.id === "assistant:cursor-streaming-cancelled-item" &&
            message.text === "visible before cancel" &&
            !message.streaming,
        ),
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-cursor-streaming-cancelled-after-terminal"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      itemId,
      payload: {
        streamKind: "assistant_text",
        delta: " stale after cancel",
      },
    });
    await harness.drain();
    const snapshot = await harness.readModel();
    const thread = snapshot.threads.find((entry) => entry.id === threadId);
    const message = thread?.messages.find(
      (entry: ProviderRuntimeTestMessage) =>
        entry.id === "assistant:cursor-streaming-cancelled-item",
    );
    expect(message?.text).toBe("visible before cancel");
    expect(message?.streaming).toBe(false);
  });

  xit("flushes and completes buffered assistant text when an approval request opens", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-buffered-request-flush"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered-request-flush"),
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-buffered-request-flush",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-message-delta-buffered-request-flush"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered-request-flush"),
      itemId: asItemId("item-buffered-request-flush"),
      payload: {
        streamKind: "assistant_text",
        delta: "visible before approval",
      },
    });
    harness.emit({
      type: "request.opened",
      eventId: asEventId("evt-request-opened-buffered-request-flush"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered-request-flush"),
      requestId: ApprovalRequestId.make("req-buffered-request-flush"),
      payload: {
        requestType: "command_execution_approval",
        detail: "pwd",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-buffered-request-flush" &&
          !message.streaming &&
          message.text === "visible before approval",
      ),
    );
    const message = thread.messages.find(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:item-buffered-request-flush",
    );
    expect(message?.streaming).toBe(false);
  });

  xit("flushes and completes buffered assistant text when user input is requested", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-buffered-user-input-flush"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered-user-input-flush"),
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-buffered-user-input-flush",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-message-delta-buffered-user-input-flush"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered-user-input-flush"),
      itemId: asItemId("item-buffered-user-input-flush"),
      payload: {
        streamKind: "assistant_text",
        delta: "visible before user input",
      },
    });
    harness.emit({
      type: "user-input.requested",
      eventId: asEventId("evt-user-input-requested-buffered-user-input-flush"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered-user-input-flush"),
      requestId: ApprovalRequestId.make("req-buffered-user-input-flush"),
      payload: {
        questions: [
          {
            id: "choice",
            header: "Choice",
            question: "Pick one",
            options: [{ label: "A", description: "Option A" }],
          },
        ],
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-buffered-user-input-flush" &&
          !message.streaming &&
          message.text === "visible before user input",
      ),
    );
    const message = thread.messages.find(
      (entry: ProviderRuntimeTestMessage) =>
        entry.id === "assistant:item-buffered-user-input-flush",
    );
    expect(message?.streaming).toBe(false);
  });

  xit("does not create assistant segments for whitespace-only buffered text at approval boundaries", async () => {
    const harness = await createHarness();
    const startedAt = "2026-03-28T06:28:00.000Z";
    const pausedAt = "2026-03-28T06:28:01.000Z";

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-buffered-whitespace-request"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: startedAt,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered-whitespace-request"),
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-buffered-whitespace-request",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-message-delta-buffered-whitespace-request"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: startedAt,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered-whitespace-request"),
      itemId: asItemId("item-buffered-whitespace-request"),
      payload: {
        streamKind: "assistant_text",
        delta: "\n\n\n",
      },
    });
    harness.emit({
      type: "request.opened",
      eventId: asEventId("evt-request-opened-buffered-whitespace-request"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: pausedAt,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered-whitespace-request"),
      requestId: ApprovalRequestId.make("req-buffered-whitespace-request"),
      payload: {
        requestType: "command_execution_approval",
        detail: "pwd",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.activities.some(
        (activity: ProviderRuntimeTestActivity) => activity.kind === "approval.requested",
      ),
    );
    expect(
      thread.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-buffered-whitespace-request",
      ),
    ).toBe(false);
  });

  xit("starts a new buffered assistant message segment after approval and completes without duplication", async () => {
    const harness = await createHarness();
    const startedAt = "2026-03-28T06:07:00.000Z";
    const pausedAt = "2026-03-28T06:07:01.000Z";
    const resumedAt = "2026-03-28T06:07:02.000Z";
    const completedAt = "2026-03-28T06:07:03.000Z";

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-buffered-request-append"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: startedAt,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered-request-append"),
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-buffered-request-append",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-message-delta-buffered-request-append-initial"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: startedAt,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered-request-append"),
      itemId: asItemId("item-buffered-request-append"),
      payload: {
        streamKind: "assistant_text",
        delta: "first half",
      },
    });
    harness.emit({
      type: "request.opened",
      eventId: asEventId("evt-request-opened-buffered-request-append"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: pausedAt,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered-request-append"),
      requestId: ApprovalRequestId.make("req-buffered-request-append"),
      payload: {
        requestType: "command_execution_approval",
        detail: "pwd",
      },
    });

    await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-buffered-request-append" &&
          !message.streaming &&
          message.text === "first half",
      ),
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-message-delta-buffered-request-append-followup"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: resumedAt,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered-request-append"),
      itemId: asItemId("item-buffered-request-append"),
      payload: {
        streamKind: "assistant_text",
        delta: " second half",
      },
    });
    harness.emit({
      type: "item.completed",
      eventId: asEventId("evt-message-completed-buffered-request-append"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: completedAt,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffered-request-append"),
      itemId: asItemId("item-buffered-request-append"),
      payload: {
        itemType: "assistant_message",
        status: "completed",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-buffered-request-append:segment:1" &&
          !message.streaming &&
          message.text === " second half",
      ),
    );
    const firstMessage = thread.messages.find(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:item-buffered-request-append",
    );
    const resumedMessage = thread.messages.find(
      (entry: ProviderRuntimeTestMessage) =>
        entry.id === "assistant:item-buffered-request-append:segment:1",
    );
    expect(firstMessage?.text).toBe("first half");
    expect(firstMessage?.streaming).toBe(false);
    expect(resumedMessage?.text).toBe(" second half");
    expect(resumedMessage?.streaming).toBe(false);

    const events = await harness.readEvents(0);
    const assistantEvents = events.filter(
      (event): event is Extract<(typeof events)[number], { type: "thread.message-sent" }> =>
        event.type === "thread.message-sent" &&
        event.payload.messageId.startsWith("assistant:item-buffered-request-append"),
    );
    expect(assistantEvents).toHaveLength(4);
    expect(assistantEvents[0]?.payload.streaming).toBe(true);
    expect(assistantEvents[0]?.payload.text).toBe("first half");
    expect(assistantEvents[1]?.payload.streaming).toBe(false);
    expect(assistantEvents[1]?.payload.text).toBe("");
    expect(assistantEvents[2]?.payload.messageId).toBe(
      "assistant:item-buffered-request-append:segment:1",
    );
    expect(assistantEvents[2]?.payload.streaming).toBe(true);
    expect(assistantEvents[2]?.payload.text).toBe(" second half");
    expect(assistantEvents[3]?.payload.messageId).toBe(
      "assistant:item-buffered-request-append:segment:1",
    );
    expect(assistantEvents[3]?.payload.streaming).toBe(false);
    expect(assistantEvents[3]?.payload.text).toBe("");
  });

  xit("starts a new streaming assistant message segment after approval", async () => {
    const harness = await createHarness({ serverSettings: { enableAssistantStreaming: true } });
    const startedAt = "2026-03-28T07:00:00.000Z";
    const pausedAt = "2026-03-28T07:00:01.000Z";
    const resumedAt = "2026-03-28T07:00:02.000Z";
    const completedAt = "2026-03-28T07:00:03.000Z";

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-streaming-request-segment"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: startedAt,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-streaming-request-segment"),
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-streaming-request-segment",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-message-delta-streaming-request-segment-initial"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: startedAt,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-streaming-request-segment"),
      itemId: asItemId("item-streaming-request-segment"),
      payload: {
        streamKind: "assistant_text",
        delta: "before approval",
      },
    });
    harness.emit({
      type: "request.opened",
      eventId: asEventId("evt-request-opened-streaming-request-segment"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: pausedAt,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-streaming-request-segment"),
      requestId: ApprovalRequestId.make("req-streaming-request-segment"),
      payload: {
        requestType: "command_execution_approval",
        detail: "pwd",
      },
    });

    await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-streaming-request-segment" &&
          !message.streaming &&
          message.text === "before approval",
      ),
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-message-delta-streaming-request-segment-followup"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: resumedAt,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-streaming-request-segment"),
      itemId: asItemId("item-streaming-request-segment"),
      payload: {
        streamKind: "assistant_text",
        delta: " after approval",
      },
    });
    harness.emit({
      type: "item.completed",
      eventId: asEventId("evt-message-completed-streaming-request-segment"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: completedAt,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-streaming-request-segment"),
      itemId: asItemId("item-streaming-request-segment"),
      payload: {
        itemType: "assistant_message",
        status: "completed",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-streaming-request-segment:segment:1" &&
          !message.streaming &&
          message.text === " after approval",
      ),
    );
    expect(
      thread.messages.find(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-streaming-request-segment",
      )?.text,
    ).toBe("before approval");
    expect(
      thread.messages.find(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-streaming-request-segment:segment:1",
      )?.text,
    ).toBe(" after approval");
  });

  xit("persists each assistant text block in a multi-block turn", async () => {
    const harness = await createHarness();
    const startedAt = "2026-03-28T08:00:00.000Z";
    const completedAt = "2026-03-28T08:00:03.000Z";
    const turnId = asTurnId("turn-multi-assistant-blocks");
    const threadId = asThreadId("thread-1");

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-multi-assistant-blocks"),
      provider: ProviderDriverKind.make("claudeAgent"),
      createdAt: startedAt,
      threadId,
      turnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-multi-assistant-blocks",
    );

    const emitAssistantDelta = (eventId: string, itemId: string, delta: string) => {
      harness.emit({
        type: "content.delta",
        eventId: asEventId(eventId),
        provider: ProviderDriverKind.make("claudeAgent"),
        createdAt: startedAt,
        threadId,
        turnId,
        itemId: asItemId(itemId),
        payload: {
          streamKind: "assistant_text",
          delta,
        },
      });
    };
    const emitToolLifecycle = (
      lifecycle: "item.started" | "item.completed",
      eventId: string,
      itemId: string,
      status: "inProgress" | "completed",
    ) => {
      harness.emit({
        type: lifecycle,
        eventId: asEventId(eventId),
        provider: ProviderDriverKind.make("claudeAgent"),
        createdAt: startedAt,
        threadId,
        turnId,
        itemId: asItemId(itemId),
        payload: {
          itemType: "command_execution",
          status,
          title: "Ran command",
          detail: "pwd",
          data: { toolName: "Bash" },
        },
      });
    };

    emitAssistantDelta("evt-text-block-1-delta", "item-text-block-1", "before tool");
    emitToolLifecycle("item.started", "evt-tool-1-started", "item-tool-1", "inProgress");
    emitToolLifecycle("item.completed", "evt-tool-1-completed", "item-tool-1", "completed");
    emitAssistantDelta("evt-text-block-2-delta", "item-text-block-2", "between tools");
    emitToolLifecycle("item.started", "evt-tool-2-started", "item-tool-2", "inProgress");
    emitToolLifecycle("item.completed", "evt-tool-2-completed", "item-tool-2", "completed");
    emitAssistantDelta("evt-text-block-3-delta", "item-text-block-3", "after tools");
    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-turn-completed-multi-assistant-blocks"),
      provider: ProviderDriverKind.make("claudeAgent"),
      createdAt: completedAt,
      threadId,
      turnId,
      payload: {
        state: "completed",
      },
    });

    const thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.session?.status === "ready" &&
        entry.messages.filter(
          (message) =>
            message.role === "assistant" && message.turnId === turnId && !message.streaming,
        ).length === 3,
    );
    const assistantMessages = thread.messages
      .filter((message) => message.role === "assistant" && message.turnId === turnId)
      .map((message) => ({
        id: message.id,
        text: message.text,
        streaming: message.streaming,
      }));
    expect(assistantMessages).toEqual([
      { id: "assistant:item-text-block-1", text: "before tool", streaming: false },
      { id: "assistant:item-text-block-2", text: "between tools", streaming: false },
      { id: "assistant:item-text-block-3", text: "after tools", streaming: false },
    ]);
  });

  xit("flushes buffered assistant text before a pending turn supersedes the active turn", async () => {
    const harness = await createHarness();
    const threadId = asThreadId("thread-1");
    const oldTurnId = asTurnId("turn-interrupted-before-completion");
    const newTurnId = asTurnId("turn-interrupting-user-message");
    const createdAt = "2026-03-28T09:00:00.000Z";

    harness.setProviderSession({
      provider: ProviderDriverKind.make("claudeAgent"),
      status: "running",
      runtimeMode: "approval-required",
      threadId,
      createdAt,
      updatedAt: createdAt,
      activeTurnId: oldTurnId,
    });
    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-interrupted-before-completion"),
      provider: ProviderDriverKind.make("claudeAgent"),
      createdAt,
      threadId,
      turnId: oldTurnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-interrupted-before-completion",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-interrupted-text-delta"),
      provider: ProviderDriverKind.make("claudeAgent"),
      createdAt,
      threadId,
      turnId: oldTurnId,
      itemId: asItemId("item-interrupted-text"),
      payload: {
        streamKind: "assistant_text",
        delta: "text before interruption",
      },
    });

    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-turn-start-interrupting-user-message"),
          threadId,
          message: {
            messageId: asMessageId("msg-interrupting-user-message"),
            role: "user",
            text: "interrupt with a new turn",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt,
        },
        testDispatchAuthority,
      ),
    );
    harness.setProviderSession({
      provider: ProviderDriverKind.make("claudeAgent"),
      status: "running",
      runtimeMode: "approval-required",
      threadId,
      createdAt,
      updatedAt: createdAt,
      activeTurnId: newTurnId,
    });
    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-interrupting-user-message"),
      provider: ProviderDriverKind.make("claudeAgent"),
      createdAt,
      threadId,
      turnId: newTurnId,
    });

    const thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.session?.status === "running" &&
        entry.session?.activeTurnId === "turn-interrupting-user-message" &&
        entry.messages.some(
          (message) =>
            message.id === "assistant:item-interrupted-text" &&
            message.turnId === oldTurnId &&
            message.text === "text before interruption" &&
            !message.streaming,
        ),
    );
    const interruptedMessage = thread.messages.find(
      (message) => message.id === "assistant:item-interrupted-text",
    );
    expect(interruptedMessage?.turnId).toBe(oldTurnId);
    expect(interruptedMessage?.streaming).toBe(false);
  });

  xit("streams assistant deltas when thread.turn.start requests streaming mode", async () => {
    const harness = await createHarness({ serverSettings: { enableAssistantStreaming: true } });
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch(
        {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-turn-start-streaming-mode"),
          threadId: ThreadId.make("thread-1"),
          message: {
            messageId: asMessageId("message-streaming-mode"),
            role: "user",
            text: "stream please",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt: now,
        },
        testDispatchAuthority,
      ),
    );
    await harness.drain();

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-streaming-mode"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-streaming-mode"),
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-streaming-mode",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-message-delta-streaming-mode"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-streaming-mode"),
      itemId: asItemId("item-streaming-mode"),
      payload: {
        streamKind: "assistant_text",
        delta: "hello live",
      },
    });

    const liveThread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-streaming-mode" &&
          message.streaming &&
          message.text === "hello live",
      ),
    );
    const liveMessage = liveThread.messages.find(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:item-streaming-mode",
    );
    expect(liveMessage?.streaming).toBe(true);

    harness.emit({
      type: "item.completed",
      eventId: asEventId("evt-message-completed-streaming-mode"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-streaming-mode"),
      itemId: asItemId("item-streaming-mode"),
      payload: {
        itemType: "assistant_message",
        status: "completed",
        detail: "hello live",
      },
    });

    const finalThread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-streaming-mode" && !message.streaming,
      ),
    );
    const finalMessage = finalThread.messages.find(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:item-streaming-mode",
    );
    expect(finalMessage?.text).toBe("hello live");
    expect(finalMessage?.streaming).toBe(false);
  });

  xit("spills oversized buffered deltas and still finalizes full assistant text", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const oversizedText = "x".repeat(40_000);

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-buffer-spill"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffer-spill"),
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-buffer-spill",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-message-delta-buffer-spill"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffer-spill"),
      itemId: asItemId("item-buffer-spill"),
      payload: {
        streamKind: "assistant_text",
        delta: oversizedText,
      },
    });
    harness.emit({
      type: "item.completed",
      eventId: asEventId("evt-message-completed-buffer-spill"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-buffer-spill"),
      itemId: asItemId("item-buffer-spill"),
      payload: {
        itemType: "assistant_message",
        status: "completed",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-buffer-spill" && !message.streaming,
      ),
    );
    const message = thread.messages.find(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:item-buffer-spill",
    );
    expect(message?.text.length).toBe(oversizedText.length);
    expect(message?.text).toBe(oversizedText);
    expect(message?.streaming).toBe(false);
  });

  xit("completes spilled buffered assistant text when a turn is cancelled", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const oversizedText = "x".repeat(40_000);
    const threadId = asThreadId("thread-1");
    const turnId = asTurnId("turn-buffer-spill-cancelled");
    const itemId = asItemId("item-buffer-spill-cancelled");

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-buffer-spill-cancelled"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
    });
    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-buffer-spill-cancelled",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-message-delta-buffer-spill-cancelled"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      itemId,
      payload: {
        streamKind: "assistant_text",
        delta: oversizedText,
      },
    });
    await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-buffer-spill-cancelled" &&
          message.text === oversizedText &&
          message.streaming,
      ),
    );

    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-turn-completed-buffer-spill-cancelled"),
      provider: ProviderDriverKind.make("cursor"),
      createdAt: now,
      threadId,
      turnId,
      payload: {
        state: "cancelled",
        stopReason: "cancelled",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.messages.some(
        (message: ProviderRuntimeTestMessage) =>
          message.id === "assistant:item-buffer-spill-cancelled" &&
          message.text === oversizedText &&
          !message.streaming,
      ),
    );
    const message = thread.messages.find(
      (entry: ProviderRuntimeTestMessage) => entry.id === "assistant:item-buffer-spill-cancelled",
    );
    expect(message?.text.length).toBe(oversizedText.length);
    expect(message?.streaming).toBe(false);
  });

  xit("does not duplicate assistant completion when item.completed is followed by turn.completed", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-turn-started-for-complete-dedup"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-complete-dedup"),
    });

    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "running" &&
        thread.session?.activeTurnId === "turn-complete-dedup",
    );

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-message-delta-for-complete-dedup"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-complete-dedup"),
      itemId: asItemId("item-complete-dedup"),
      payload: {
        streamKind: "assistant_text",
        delta: "done",
      },
    });
    harness.emit({
      type: "item.completed",
      eventId: asEventId("evt-message-completed-for-complete-dedup"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-complete-dedup"),
      itemId: asItemId("item-complete-dedup"),
      payload: {
        itemType: "assistant_message",
        status: "completed",
      },
    });
    harness.emit({
      type: "turn.completed",
      eventId: asEventId("evt-turn-completed-for-complete-dedup"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-complete-dedup"),
      payload: {
        state: "completed",
      },
    });

    await waitForThread(
      harness.readModel,
      (thread) =>
        thread.session?.status === "ready" &&
        thread.session?.activeTurnId === null &&
        thread.messages.some(
          (message: ProviderRuntimeTestMessage) =>
            message.id === "assistant:item-complete-dedup" && !message.streaming,
        ),
    );

    const events = await harness.readEvents(0);
    const completionEvents = events.filter((event) => {
      if (event.type !== "thread.message-sent") {
        return false;
      }
      return (
        event.payload.messageId === "assistant:item-complete-dedup" &&
        event.payload.streaming === false
      );
    });
    expect(completionEvents).toHaveLength(1);
  });

  xit("maps canonical request events into approval activities with requestKind", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "request.opened",
      eventId: asEventId("evt-request-opened"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      requestId: ApprovalRequestId.make("req-open"),
      payload: {
        requestType: "command_execution_approval",
        detail: "pwd",
      },
    });

    harness.emit({
      type: "request.resolved",
      eventId: asEventId("evt-request-resolved"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      requestId: ApprovalRequestId.make("req-open"),
      payload: {
        requestType: "command_execution_approval",
        decision: "accept",
      },
    });

    await waitForThread(
      harness.readModel,
      (entry) =>
        entry.activities.some(
          (activity: ProviderRuntimeTestActivity) => activity.kind === "approval.requested",
        ) &&
        entry.activities.some(
          (activity: ProviderRuntimeTestActivity) => activity.kind === "approval.resolved",
        ),
    );

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread).toBeDefined();

    const requested = thread?.activities.find(
      (activity: ProviderRuntimeTestActivity) => activity.id === "evt-request-opened",
    );
    const requestedPayload =
      requested?.payload && typeof requested.payload === "object"
        ? (requested.payload as Record<string, unknown>)
        : undefined;
    expect(requestedPayload?.requestKind).toBe("command");
    expect(requestedPayload?.requestType).toBe("command_execution_approval");

    const resolved = thread?.activities.find(
      (activity: ProviderRuntimeTestActivity) => activity.id === "evt-request-resolved",
    );
    const resolvedPayload =
      resolved?.payload && typeof resolved.payload === "object"
        ? (resolved.payload as Record<string, unknown>)
        : undefined;
    expect(resolvedPayload?.requestKind).toBe("command");
    expect(resolvedPayload?.requestType).toBe("command_execution_approval");
  });

  xit("maps runtime.error into errored session state", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "runtime.error",
      eventId: asEventId("evt-runtime-error"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-3"),
      payload: {
        message: "runtime exploded",
      },
    });

    const thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.session?.status === "error" &&
        entry.session?.activeTurnId === "turn-3" &&
        entry.session?.lastError === "runtime exploded",
    );
    expect(thread.session?.status).toBe("error");
    expect(thread.session?.lastError).toBe("runtime exploded");
  });

  xit("records runtime.error activities from the typed payload message", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "runtime.error",
      eventId: asEventId("evt-runtime-error-activity"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-runtime-error-activity"),
      payload: {
        message: "runtime activity exploded",
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.activities.some((activity) => activity.id === "evt-runtime-error-activity"),
    );
    const activity = thread.activities.find(
      (entry: ProviderRuntimeTestActivity) => entry.id === "evt-runtime-error-activity",
    );
    const activityPayload =
      activity?.payload && typeof activity.payload === "object"
        ? (activity.payload as Record<string, unknown>)
        : undefined;

    expect(activity?.kind).toBe("runtime.error");
    expect(activityPayload?.message).toBe("runtime activity exploded");
  });

  xit("keeps the session running when a runtime.warning arrives during an active turn", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "turn.started",
      eventId: asEventId("evt-warning-turn-started"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-warning"),
      payload: {},
    });

    harness.emit({
      type: "runtime.warning",
      eventId: asEventId("evt-warning-runtime"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-warning"),
      payload: {
        message: "Reconnecting... 2/5",
        detail: {
          willRetry: true,
        },
      },
    });

    const thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.session?.status === "running" &&
        entry.session?.activeTurnId === "turn-warning" &&
        entry.activities.some(
          (activity: ProviderRuntimeTestActivity) =>
            activity.id === "evt-warning-runtime" && activity.kind === "runtime.warning",
        ),
    );
    expect(thread.session?.status).toBe("running");
    expect(thread.session?.activeTurnId).toBe("turn-warning");
    expect(thread.session?.lastError).toBeNull();
  });

  xit("maps session/thread lifecycle and item.started into session/activity projections", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "session.started",
      eventId: asEventId("evt-session-started"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      message: "session started",
    });
    harness.emit({
      type: "thread.started",
      eventId: asEventId("evt-thread-started"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
    });
    harness.emit({
      type: "item.started",
      eventId: asEventId("evt-tool-started"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-9"),
      payload: {
        itemType: "command_execution",
        status: "in_progress",
        title: "Read file",
        detail: "/tmp/file.ts",
      },
    });

    const thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.session?.status === "ready" &&
        entry.session?.activeTurnId === null &&
        entry.activities.some(
          (activity: ProviderRuntimeTestActivity) => activity.kind === "tool.started",
        ),
    );

    expect(thread.session?.status).toBe("ready");
    expect(
      thread.activities.some(
        (activity: ProviderRuntimeTestActivity) => activity.kind === "tool.started",
      ),
    ).toBe(true);
  });

  xit("consumes P1 runtime events into thread metadata, diff checkpoints, and activities", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "thread.metadata.updated",
      eventId: asEventId("evt-thread-metadata-updated"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      payload: {
        name: "Renamed by provider",
        metadata: { source: "provider" },
      },
    });

    harness.emit({
      type: "turn.plan.updated",
      eventId: asEventId("evt-turn-plan-updated"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-p1"),
      payload: {
        explanation: "Working through the plan",
        plan: [
          { step: "Inspect files", status: "completed" },
          { step: "Apply patch", status: "in_progress" },
        ],
      },
    });

    harness.emit({
      type: "item.updated",
      eventId: asEventId("evt-item-updated"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-p1"),
      itemId: asItemId("item-p1-tool"),
      payload: {
        itemType: "command_execution",
        status: "in_progress",
        title: "Run tests",
        detail: "bun test",
        data: { pid: 123 },
      },
    });

    harness.emit({
      type: "runtime.warning",
      eventId: asEventId("evt-runtime-warning"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-p1"),
      payload: {
        message: "Provider got slow",
        detail: { latencyMs: 1500 },
      },
    });

    harness.emit({
      type: "turn.diff.updated",
      eventId: asEventId("evt-turn-diff-updated"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-p1"),
      itemId: asItemId("item-p1-assistant"),
      payload: {
        unifiedDiff: "diff --git a/file.txt b/file.txt\n+hello\n",
      },
    });

    const thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.title === "Renamed by provider" &&
        entry.activities.some(
          (activity: ProviderRuntimeTestActivity) => activity.kind === "turn.plan.updated",
        ) &&
        entry.activities.some(
          (activity: ProviderRuntimeTestActivity) => activity.kind === "tool.updated",
        ) &&
        entry.activities.some(
          (activity: ProviderRuntimeTestActivity) => activity.kind === "runtime.warning",
        ) &&
        entry.checkpoints.some(
          (checkpoint: ProviderRuntimeTestCheckpoint) => checkpoint.turnId === "turn-p1",
        ),
    );

    expect(thread.title).toBe("Renamed by provider");

    const planActivity = thread.activities.find(
      (activity: ProviderRuntimeTestActivity) => activity.id === "evt-turn-plan-updated",
    );
    const planPayload =
      planActivity?.payload && typeof planActivity.payload === "object"
        ? (planActivity.payload as Record<string, unknown>)
        : undefined;
    expect(planActivity?.kind).toBe("turn.plan.updated");
    expect(Array.isArray(planPayload?.plan)).toBe(true);

    const toolUpdate = thread.activities.find(
      (activity: ProviderRuntimeTestActivity) => activity.id === "evt-item-updated",
    );
    const toolUpdatePayload =
      toolUpdate?.payload && typeof toolUpdate.payload === "object"
        ? (toolUpdate.payload as Record<string, unknown>)
        : undefined;
    expect(toolUpdate?.kind).toBe("tool.updated");
    expect(toolUpdatePayload?.itemType).toBe("command_execution");
    expect(toolUpdatePayload?.status).toBe("in_progress");

    const warning = thread.activities.find(
      (activity: ProviderRuntimeTestActivity) => activity.id === "evt-runtime-warning",
    );
    const warningPayload =
      warning?.payload && typeof warning.payload === "object"
        ? (warning.payload as Record<string, unknown>)
        : undefined;
    expect(warning?.kind).toBe("runtime.warning");
    expect(warningPayload?.message).toBe("Provider got slow");

    const checkpoint = thread.checkpoints.find(
      (entry: ProviderRuntimeTestCheckpoint) => entry.turnId === "turn-p1",
    );
    expect(checkpoint?.status).toBe("missing");
    expect(checkpoint?.assistantMessageId).toBe("assistant:item-p1-assistant");
    expect(checkpoint?.checkpointRef).toBe("provider-diff:evt-turn-diff-updated");
  });

  xit("projects context window updates into normalized thread activities", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "thread.token-usage.updated",
      eventId: asEventId("evt-thread-token-usage-updated"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      payload: {
        usage: {
          usedTokens: 1075,
          totalProcessedTokens: 10_200,
          maxTokens: 128_000,
          inputTokens: 1000,
          cachedInputTokens: 500,
          outputTokens: 50,
          reasoningOutputTokens: 25,
          lastUsedTokens: 1075,
          lastInputTokens: 1000,
          lastCachedInputTokens: 500,
          lastOutputTokens: 50,
          lastReasoningOutputTokens: 25,
          compactsAutomatically: true,
        },
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.activities.some(
        (activity: ProviderRuntimeTestActivity) => activity.kind === "context-window.updated",
      ),
    );

    const usageActivity = thread.activities.find(
      (activity: ProviderRuntimeTestActivity) => activity.kind === "context-window.updated",
    );
    expect(usageActivity).toBeDefined();
    expect(usageActivity?.payload).toMatchObject({
      usedTokens: 1075,
      totalProcessedTokens: 10_200,
      maxTokens: 128_000,
      inputTokens: 1000,
      cachedInputTokens: 500,
      outputTokens: 50,
      reasoningOutputTokens: 25,
      lastUsedTokens: 1075,
      compactsAutomatically: true,
    });
  });

  xit("projects Codex camelCase token usage payloads into normalized thread activities", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "thread.token-usage.updated",
      eventId: asEventId("evt-thread-token-usage-updated-camel"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      payload: {
        usage: {
          usedTokens: 126,
          totalProcessedTokens: 11_839,
          maxTokens: 258_400,
          inputTokens: 120,
          cachedInputTokens: 0,
          outputTokens: 6,
          reasoningOutputTokens: 0,
          lastUsedTokens: 126,
          lastInputTokens: 120,
          lastCachedInputTokens: 0,
          lastOutputTokens: 6,
          lastReasoningOutputTokens: 0,
          compactsAutomatically: true,
        },
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.activities.some(
        (activity: ProviderRuntimeTestActivity) => activity.kind === "context-window.updated",
      ),
    );

    const usageActivity = thread.activities.find(
      (activity: ProviderRuntimeTestActivity) => activity.kind === "context-window.updated",
    );
    expect(usageActivity?.payload).toMatchObject({
      usedTokens: 126,
      totalProcessedTokens: 11_839,
      maxTokens: 258_400,
      inputTokens: 120,
      cachedInputTokens: 0,
      outputTokens: 6,
      reasoningOutputTokens: 0,
      lastUsedTokens: 126,
      lastInputTokens: 120,
      lastOutputTokens: 6,
      compactsAutomatically: true,
    });
  });

  xit("projects Claude usage snapshots with context window into normalized thread activities", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "thread.token-usage.updated",
      eventId: asEventId("evt-thread-token-usage-updated-claude-window"),
      provider: ProviderDriverKind.make("claudeAgent"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      payload: {
        usage: {
          usedTokens: 31_251,
          lastUsedTokens: 31_251,
          maxTokens: 200_000,
          toolUses: 25,
          durationMs: 43_567,
        },
      },
      raw: {
        source: "claude.sdk.message",
        method: "claude/result/success",
        payload: {},
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.activities.some(
        (activity: ProviderRuntimeTestActivity) => activity.kind === "context-window.updated",
      ),
    );

    const usageActivity = thread.activities.find(
      (activity: ProviderRuntimeTestActivity) => activity.kind === "context-window.updated",
    );
    expect(usageActivity?.payload).toMatchObject({
      usedTokens: 31_251,
      lastUsedTokens: 31_251,
      maxTokens: 200_000,
      toolUses: 25,
      durationMs: 43_567,
    });
  });

  xit("projects compacted thread state into context compaction activities", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "thread.state.changed",
      eventId: asEventId("evt-thread-compacted"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-1"),
      payload: {
        state: "compacted",
        detail: { source: "provider" },
      },
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.activities.some(
        (activity: ProviderRuntimeTestActivity) => activity.kind === "context-compaction",
      ),
    );

    const activity = thread.activities.find(
      (candidate: ProviderRuntimeTestActivity) => candidate.kind === "context-compaction",
    );
    expect(activity?.summary).toBe("Context compacted");
    expect(activity?.tone).toBe("info");
  });

  xit("projects Codex task lifecycle chunks into thread activities", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "task.started",
      eventId: asEventId("evt-task-started"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-task-1"),
      payload: {
        taskId: "turn-task-1",
        taskType: "plan",
      },
    });

    harness.emit({
      type: "task.progress",
      eventId: asEventId("evt-task-progress"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-task-1"),
      payload: {
        taskId: "turn-task-1",
        description: "Comparing the desktop rollout chunks to the app-server stream.",
        summary: "Code reviewer is validating the desktop rollout chunks.",
      },
    });

    harness.emit({
      type: "task.completed",
      eventId: asEventId("evt-task-completed"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-task-1"),
      payload: {
        taskId: "turn-task-1",
        status: "completed",
        summary: "<proposed_plan>\n# Plan title\n</proposed_plan>",
      },
    });
    harness.emit({
      type: "turn.proposed.completed",
      eventId: asEventId("evt-task-proposed-plan-completed"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-task-1"),
      payload: {
        planMarkdown: "# Plan title",
      },
    });

    const thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.activities.some(
          (activity: ProviderRuntimeTestActivity) => activity.kind === "task.completed",
        ) &&
        entry.proposedPlans.some(
          (proposedPlan: ProviderRuntimeTestProposedPlan) =>
            proposedPlan.id === "plan:thread-1:turn:turn-task-1",
        ),
    );

    const started = thread.activities.find(
      (activity: ProviderRuntimeTestActivity) => activity.id === "evt-task-started",
    );
    const progress = thread.activities.find(
      (activity: ProviderRuntimeTestActivity) => activity.id === "evt-task-progress",
    );
    const completed = thread.activities.find(
      (activity: ProviderRuntimeTestActivity) => activity.id === "evt-task-completed",
    );

    const progressPayload =
      progress?.payload && typeof progress.payload === "object"
        ? (progress.payload as Record<string, unknown>)
        : undefined;
    const completedPayload =
      completed?.payload && typeof completed.payload === "object"
        ? (completed.payload as Record<string, unknown>)
        : undefined;

    expect(started?.kind).toBe("task.started");
    expect(started?.summary).toBe("Plan task started");
    expect(progress?.kind).toBe("task.progress");
    expect(progressPayload?.detail).toBe("Code reviewer is validating the desktop rollout chunks.");
    expect(progressPayload?.summary).toBe(
      "Code reviewer is validating the desktop rollout chunks.",
    );
    expect(completed?.kind).toBe("task.completed");
    expect(completedPayload?.detail).toBe("<proposed_plan>\n# Plan title\n</proposed_plan>");
    expect(
      thread.proposedPlans.find(
        (entry: ProviderRuntimeTestProposedPlan) => entry.id === "plan:thread-1:turn:turn-task-1",
      )?.planMarkdown,
    ).toBe("# Plan title");
  });

  xit("projects structured user input request and resolution as thread activities", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "user-input.requested",
      eventId: asEventId("evt-user-input-requested"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-user-input"),
      requestId: ApprovalRequestId.make("req-user-input-1"),
      payload: {
        questions: [
          {
            id: "sandbox_mode",
            header: "Sandbox",
            question: "Which mode should be used?",
            options: [
              {
                label: "workspace-write",
                description: "Allow workspace writes only",
              },
            ],
          },
        ],
      },
    });

    harness.emit({
      type: "user-input.resolved",
      eventId: asEventId("evt-user-input-resolved"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-user-input"),
      requestId: ApprovalRequestId.make("req-user-input-1"),
      payload: {
        answers: {
          sandbox_mode: "workspace-write",
        },
      },
    });

    const thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.activities.some(
          (activity: ProviderRuntimeTestActivity) => activity.kind === "user-input.requested",
        ) &&
        entry.activities.some(
          (activity: ProviderRuntimeTestActivity) => activity.kind === "user-input.resolved",
        ),
    );

    const requested = thread.activities.find(
      (activity: ProviderRuntimeTestActivity) => activity.id === "evt-user-input-requested",
    );
    expect(requested?.kind).toBe("user-input.requested");

    const resolved = thread.activities.find(
      (activity: ProviderRuntimeTestActivity) => activity.id === "evt-user-input-resolved",
    );
    const resolvedPayload =
      resolved?.payload && typeof resolved.payload === "object"
        ? (resolved.payload as Record<string, unknown>)
        : undefined;
    expect(resolved?.kind).toBe("user-input.resolved");
    expect(resolvedPayload?.answers).toEqual({
      sandbox_mode: "workspace-write",
    });
  });

  xit("continues processing runtime events after a single event handler failure", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.emit({
      type: "content.delta",
      eventId: asEventId("evt-invalid-delta"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: now,
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-invalid"),
      itemId: asItemId("item-invalid"),
      payload: {
        streamKind: "assistant_text",
        delta: undefined,
      },
    } as unknown as ProviderRuntimeEvent);

    harness.emit({
      type: "runtime.error",
      eventId: asEventId("evt-runtime-error-after-failure"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: asThreadId("thread-1"),
      turnId: asTurnId("turn-after-failure"),
      payload: {
        message: "runtime still processed",
      },
    });

    const thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.session?.status === "error" &&
        entry.session?.activeTurnId === "turn-after-failure" &&
        entry.session?.lastError === "runtime still processed",
    );
    expect(thread.session?.status).toBe("error");
    expect(thread.session?.lastError).toBe("runtime still processed");
  });
});
