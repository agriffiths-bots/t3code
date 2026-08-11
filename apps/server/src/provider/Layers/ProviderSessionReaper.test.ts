import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ApprovalRequestId,
  CorrelationId,
  EventId,
  type OrchestrationCommand,
  type OrchestrationEvent,
  type OrchestrationLatestTurn,
  type OrchestrationReadModel,
  ProjectId,
  ThreadId,
  TurnId,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { projectEvent } from "../../orchestration/projector.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { ProjectionPendingApprovalRepository } from "../../persistence/Services/ProjectionPendingApprovals.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import { ProviderValidationError } from "../Errors.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import {
  ProviderSessionDirectory,
  type ProviderRuntimeBinding,
} from "../Services/ProviderSessionDirectory.ts";
import { ProviderSessionReaper } from "../Services/ProviderSessionReaper.ts";
import { ProviderService, type ProviderServiceShape } from "../Services/ProviderService.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";
import { makeProviderSessionReaperLive } from "./ProviderSessionReaper.ts";
import {
  PROVIDER_SESSION_FAILED_DURING_TURN_ERROR,
  providerSessionDisappearedDuringTurnError,
} from "./providerFailureMessages.ts";

const defaultModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5-codex",
} as const;

const drainFibers = Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
  discard: true,
});

const unsupported = () => Effect.die(new Error("Unsupported provider call in test")) as never;
const encodeUnknownJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

function makeReadModel(
  threads: ReadonlyArray<{
    readonly id: ThreadId;
    readonly session: {
      readonly threadId: ThreadId;
      readonly status: "starting" | "running" | "ready" | "interrupted" | "stopped" | "error";
      readonly providerName: "codex" | "claudeAgent";
      readonly runtimeMode: "approval-required" | "full-access" | "auto-accept-edits";
      readonly activeTurnId: TurnId | null;
      readonly lastError: string | null;
      readonly updatedAt: string;
    } | null;
    readonly latestTurn?: OrchestrationLatestTurn | null;
    readonly backgroundLiveness?: "working" | "monitoring" | null;
  }>,
) {
  const now = "2026-01-01T00:00:00.000Z";
  const projectId = ProjectId.make("project-provider-session-reaper");

  return {
    snapshotSequence: 0,
    updatedAt: now,
    projects: [
      {
        id: projectId,
        title: "Provider Reaper Project",
        workspaceRoot: "/tmp/provider-reaper-project",
        dataAudience: "private" as const,
        defaultModelSelection,
        scripts: [],
        createdAt: now,
        updatedAt: now,
        deletedAt: null,
      },
    ],
    threads: threads.map((thread) => ({
      id: thread.id,
      projectId,
      dataAudience: "private" as const,
      title: `Thread ${thread.id}`,
      modelSelection: defaultModelSelection,
      interactionMode: "default" as const,
      runtimeMode: "full-access" as const,
      branch: null,
      worktreePath: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      latestUserMessageAt: null,
      hasPendingApprovals: false,
      hasPendingUserInput: false,
      hasActionableProposedPlan: false,
      latestTurn: thread.latestTurn ?? null,
      messages: [],
      turns:
        thread.latestTurn === undefined || thread.latestTurn === null ? [] : [thread.latestTurn],
      session: thread.session,
      backgroundLiveness: thread.backgroundLiveness ?? null,
      activities: [],
      proposedPlans: [],
      checkpoints: [],
      deletedAt: null,
      parentThreadId: null,
    })),
  };
}

describe("ProviderSessionReaper", () => {
  let runtime: ManagedRuntime.ManagedRuntime<
    | ProviderSessionReaper
    | ProviderSessionRuntime.ProviderSessionRuntimeRepository
    | ProjectionPendingApprovalRepository,
    unknown
  > | null = null;
  let scope: Scope.Closeable | null = null;

  async function waitFor(
    predicate: () => boolean | Promise<boolean>,
    timeoutMs = 2_000,
  ): Promise<void> {
    const deadline = (await runtime!.runPromise(Clock.currentTimeMillis)) + timeoutMs;
    const poll = async (): Promise<void> => {
      if (await predicate()) {
        return;
      }
      if ((await runtime!.runPromise(Clock.currentTimeMillis)) >= deadline) {
        throw new Error("Timed out waiting for expectation.");
      }
      await runtime!.runPromise(Effect.yieldNow);
      return poll();
    };

    return poll();
  }

  afterEach(async () => {
    if (scope && runtime) {
      await runtime.runPromise(Scope.close(scope, Exit.void));
    }
    scope = null;
    if (runtime) {
      await runtime.dispose();
    }
    runtime = null;
  });

  async function createHarness(input: {
    readonly readModel: ReturnType<typeof makeReadModel>;
    readonly stopSessionImplementation?: (input: {
      readonly threadId: ThreadId;
    }) => ReturnType<ProviderServiceShape["stopSession"]>;
    readonly listSessionsImplementation?: ProviderServiceShape["listSessions"];
    readonly dispatchImplementation?: (
      command: OrchestrationCommand,
    ) => Effect.Effect<{ readonly sequence: number }>;
    readonly permissionRequestTimeoutMs?: number;
    readonly sweepIntervalMs?: number;
    readonly forceFailNoopSweeps?: number;
    readonly stopFailedSessionImplementation?: ProviderServiceShape["stopFailedSession"];
    readonly stopInactiveSessionImplementation?: NonNullable<
      ProviderServiceShape["stopInactiveSession"]
    >;
    readonly forceFailStaleSessionImplementation?: NonNullable<
      ProviderServiceShape["forceFailStaleSession"]
    >;
    readonly afterBindingRead?: (readCount: number) => Effect.Effect<void>;
  }) {
    let readModel = input.readModel as unknown as OrchestrationReadModel;
    const stoppedThreadIds = new Set<ThreadId>();
    const dispatchedCommands: OrchestrationCommand[] = [];
    const stopFailedSessionCalls: Array<{
      readonly threadId: ThreadId;
      readonly turnId: TurnId;
      readonly sessionOwnershipId?: string;
      readonly requireSessionAbsent?: boolean;
      readonly allowLegacyActiveTurnMatch?: boolean;
    }> = [];
    let runtimeRepositoryForInactiveStop:
      | ProviderSessionRuntime.ProviderSessionRuntimeRepository["Service"]
      | undefined;
    const bindingMatches = (
      current: ProviderSessionRuntime.ProviderSessionRuntime | undefined,
      expected: ProviderRuntimeBinding,
    ) =>
      current !== undefined &&
      current.threadId === expected.threadId &&
      current.providerName === expected.provider &&
      (current.providerInstanceId ?? ProviderInstanceId.make(current.providerName)) ===
        expected.providerInstanceId &&
      current.adapterKey === expected.adapterKey &&
      current.runtimeMode === expected.runtimeMode &&
      current.status === expected.status &&
      encodeUnknownJson(current.resumeCursor) === encodeUnknownJson(expected.resumeCursor) &&
      encodeUnknownJson(current.runtimePayload) === encodeUnknownJson(expected.runtimePayload);
    const stopSession = vi.fn<ProviderServiceShape["stopSession"]>(
      (request) =>
        (input.stopSessionImplementation
          ? input.stopSessionImplementation(request)
          : Effect.sync(() => {
              stoppedThreadIds.add(request.threadId);
            })) as ReturnType<ProviderServiceShape["stopSession"]>,
    );
    const dispatch = vi.fn((command: OrchestrationCommand) =>
      (input.dispatchImplementation
        ? input.dispatchImplementation(command)
        : Effect.gen(function* () {
            const event =
              command.type === "thread.session.set"
                ? ({
                    eventId: EventId.make(`test-session-set:${command.commandId}`),
                    type: "thread.session-set",
                    aggregateKind: "thread",
                    aggregateId: command.threadId,
                    occurredAt: command.createdAt,
                    commandId: command.commandId,
                    causationEventId: null,
                    correlationId: CorrelationId.make(String(command.commandId)),
                    metadata: {},
                    payload: { threadId: command.threadId, session: command.session },
                  } as unknown as OrchestrationEvent)
                : command.type === "thread.activity.append"
                  ? ({
                      eventId: EventId.make(`test-activity:${command.commandId}`),
                      type: "thread.activity-appended",
                      aggregateKind: "thread",
                      aggregateId: command.threadId,
                      occurredAt: command.createdAt,
                      commandId: command.commandId,
                      causationEventId: null,
                      correlationId: CorrelationId.make(String(command.commandId)),
                      metadata: {},
                      payload: { threadId: command.threadId, activity: command.activity },
                    } as unknown as OrchestrationEvent)
                  : null;
            if (event !== null) {
              readModel = yield* projectEvent(readModel, event);
            }
            return { sequence: dispatchedCommands.length + 1 };
          })
      ).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            dispatchedCommands.push(command);
          }),
        ),
      ),
    );

    const providerService: ProviderServiceShape = {
      startSession: () => unsupported(),
      sendTurn: () => unsupported(),
      interruptTurn: () => unsupported(),
      respondToRequest: () => unsupported(),
      respondToUserInput: () => unsupported(),
      stopSession,
      stopInactiveSession: (request) => {
        if (input.stopInactiveSessionImplementation) {
          return input.stopInactiveSessionImplementation(request);
        }
        return Effect.gen(function* () {
          const repository = runtimeRepositoryForInactiveStop;
          if (repository === undefined) {
            return yield* Effect.die("Provider session runtime repository was not initialized");
          }
          const current = Option.getOrUndefined(
            yield* repository.getByThreadId({ threadId: request.threadId }),
          );
          const expected = request.expectedBinding;
          const matches =
            bindingMatches(current, expected) && current?.lastSeenAt === expected.lastSeenAt;
          if (!matches) {
            return false;
          }
          yield* stopSession({ threadId: request.threadId });
          return true;
        });
      },
      forceFailStaleSession: (request) => {
        if (input.forceFailStaleSessionImplementation) {
          return input.forceFailStaleSessionImplementation(request);
        }
        return Effect.gen(function* () {
          const repository = runtimeRepositoryForInactiveStop;
          if (repository === undefined) {
            return yield* Effect.die("Provider session runtime repository was not initialized");
          }
          const current = Option.getOrUndefined(
            yield* repository.getByThreadId({ threadId: request.threadId }),
          );
          const runtimePayload =
            current?.runtimePayload !== null &&
            typeof current?.runtimePayload === "object" &&
            !Array.isArray(current.runtimePayload)
              ? (current.runtimePayload as Record<string, unknown>)
              : {};
          const activeTurnId = runtimePayload.activeTurnId;
          const lastTerminalTurnId = runtimePayload.lastTerminalTurnId;
          const matches =
            bindingMatches(current, request.expectedBinding) &&
            (current?.status === "error" || current?.status === "stopped") &&
            (activeTurnId === null ||
              activeTurnId === undefined ||
              activeTurnId === request.turnId) &&
            (lastTerminalTurnId === null ||
              lastTerminalTurnId === undefined ||
              lastTerminalTurnId === request.turnId);
          if (!matches) {
            return false;
          }
          yield* request.onOwned;
          yield* request.onSettled;
          return true;
        });
      },
      stopFailedSession: (request) => {
        stopFailedSessionCalls.push({
          threadId: request.threadId,
          turnId: request.turnId,
          ...(request.sessionOwnershipId !== undefined
            ? { sessionOwnershipId: request.sessionOwnershipId }
            : {}),
          ...(request.requireSessionAbsent !== undefined
            ? { requireSessionAbsent: request.requireSessionAbsent }
            : {}),
          ...(request.allowLegacyActiveTurnMatch !== undefined
            ? { allowLegacyActiveTurnMatch: request.allowLegacyActiveTurnMatch }
            : {}),
        });
        if (input.stopFailedSessionImplementation) {
          return input.stopFailedSessionImplementation(request);
        }
        return Effect.gen(function* () {
          yield* request.onOwned;
          yield* stopSession({ threadId: request.threadId });
          yield* request.onStopped;
          return true;
        });
      },
      listSessions: input.listSessionsImplementation ?? (() => Effect.succeed([])),
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
      streamEvents: Stream.empty,
    };

    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const providerSessionDirectoryLayer = ProviderSessionDirectoryLive.pipe(
      Layer.provide(runtimeRepositoryLayer),
    );
    let bindingReadCount = 0;
    const observedProviderSessionDirectoryLayer =
      input.afterBindingRead === undefined
        ? providerSessionDirectoryLayer
        : Layer.effect(
            ProviderSessionDirectory,
            Effect.gen(function* () {
              const directory = yield* ProviderSessionDirectory;
              return ProviderSessionDirectory.of({
                ...directory,
                getBinding: (threadId) =>
                  directory.getBinding(threadId).pipe(
                    Effect.tap(() => {
                      bindingReadCount += 1;
                      return input.afterBindingRead!(bindingReadCount);
                    }),
                  ),
              });
            }),
          ).pipe(Layer.provide(providerSessionDirectoryLayer));
    const getThreadShell = (threadId: ThreadId) => {
      const thread = readModel.threads.find((entry) => entry.id === threadId);
      return thread === undefined
        ? Option.none()
        : Option.some({
            ...thread,
            parentThreadId: thread.parentThreadId ?? null,
            latestUserMessageAt: null,
            hasPendingApprovals: false,
            hasPendingUserInput: false,
            hasActionableProposedPlan: false,
          });
    };
    const layer = makeProviderSessionReaperLive({
      inactivityThresholdMs: 1_000,
      sweepIntervalMs: input.sweepIntervalMs ?? 60_000,
      ...(input.permissionRequestTimeoutMs !== undefined
        ? { permissionRequestTimeoutMs: input.permissionRequestTimeoutMs }
        : {}),
      ...(input.forceFailNoopSweeps !== undefined
        ? { forceFailNoopSweeps: input.forceFailNoopSweeps }
        : {}),
    }).pipe(
      Layer.provideMerge(observedProviderSessionDirectoryLayer),
      Layer.provideMerge(runtimeRepositoryLayer),
      Layer.provideMerge(SqlitePersistenceMemory),
      Layer.provideMerge(Layer.succeed(ProviderService, providerService)),
      Layer.provideMerge(
        Layer.succeed(OrchestrationEngineService, {
          readEvents: () => Stream.empty,
          dispatch,
          latestSequence: Effect.succeed(0),
          streamDomainEvents: Stream.empty,
        }),
      ),
      Layer.provideMerge(
        Layer.succeed(ProjectionSnapshotQuery, {
          getCommandReadModel: () => Effect.die("unused"),
          getSnapshot: () => Effect.die("unused"),
          searchThreads: () => Effect.die("unused"),
          getShellSnapshot: () => Effect.die("unused"),
          getArchivedShellSnapshot: () => Effect.die("unused"),
          getSnapshotSequence: () =>
            Effect.succeed({ snapshotSequence: readModel.snapshotSequence }),
          getCounts: () => Effect.die("unused"),
          getActiveProjectByWorkspaceRoot: () => Effect.die("unused"),
          getProjectShellById: () => Effect.die("unused"),
          getFirstActiveThreadIdByProjectId: () => Effect.die("unused"),
          getThreadCheckpointContext: () => Effect.die("unused"),
          getFullThreadDiffContext: () => Effect.die("unused"),
          getThreadShellById: (threadId) => Effect.sync(() => getThreadShell(threadId)),
          getThreadShellByIdIncludingArchived: (threadId) =>
            Effect.sync(() => getThreadShell(threadId)),
          getThreadShellSnapshotByIdIncludingArchived: () => Effect.die("unused"),
          getThreadDetailById: () => Effect.die("unused"),
          getThreadDetailSnapshot: () => Effect.die("unused"),
        }),
      ),
      Layer.provideMerge(NodeServices.layer),
    );

    runtime = ManagedRuntime.make(layer);
    runtimeRepositoryForInactiveStop = await runtime.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );
    return {
      dispatch,
      dispatchedCommands,
      setReadModel: (nextReadModel: ReturnType<typeof makeReadModel>) => {
        readModel = nextReadModel as unknown as OrchestrationReadModel;
      },
      stopSession,
      stopFailedSessionCalls,
      stoppedThreadIds,
      readModel: () => readModel,
    };
  }

  async function startReaper(): Promise<void> {
    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));
  }

  async function seedActiveRuntimeBinding(input: {
    readonly threadId: ThreadId;
    readonly turnId: TurnId;
    readonly status?: "starting" | "running" | "waiting" | "error" | "stopped";
    readonly activeTurnId?: TurnId | null;
    readonly lastError?: string;
    readonly lastSeenAt?: string;
    readonly lastTerminalTurnId?: TurnId;
    readonly sessionOwnershipId?: string;
  }) {
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );
    await runtime!.runPromise(
      repository.upsert({
        threadId: input.threadId,
        providerName: "claudeAgent",
        providerInstanceId: ProviderInstanceId.make("claudeAgent"),
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: input.status ?? "running",
        lastSeenAt: input.lastSeenAt ?? "2026-01-01T00:00:00.000Z",
        resumeCursor: { opaque: `resume-${input.threadId}` },
        runtimePayload: {
          cwd: `/tmp/${input.threadId}`,
          detached: true,
          model: "test-watchdog-model",
          activeTurnId:
            input.activeTurnId !== undefined
              ? input.activeTurnId
              : input.status === "error" || input.lastTerminalTurnId !== undefined
                ? null
                : input.turnId,
          ...(input.status === "error" || input.lastTerminalTurnId !== undefined
            ? {
                lastTerminalTurnId: input.lastTerminalTurnId ?? input.turnId,
                lastRuntimeEvent:
                  input.status === "error"
                    ? "provider.turn.watchdog.stop-pending"
                    : "turn.completed",
              }
            : {}),
          ...(input.lastError !== undefined ? { lastError: input.lastError } : {}),
          ...(input.sessionOwnershipId !== undefined
            ? { sessionOwnershipId: input.sessionOwnershipId }
            : {}),
        },
      }),
    );
    return repository;
  }

  it("fails and frees a running turn after its provider process disappears", async () => {
    const threadId = ThreadId.make("thread-watchdog-process-death");
    const turnId = TurnId.make("turn-watchdog-process-death");
    const startedAt = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      // A healthy empty snapshot is definitive process loss, so the first
      // sweep must fail even though the next sweep is a minute away.
      sweepIntervalMs: 60_000,
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "running",
            requestedAt: startedAt,
            startedAt,
            completedAt: null,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    });
    const repository = await seedActiveRuntimeBinding({ threadId, turnId });

    await startReaper();

    await waitFor(() => harness.readModel().threads[0]?.latestTurn?.state === "error");

    const thread = harness.readModel().threads[0];
    expect(thread?.latestTurn?.state).toBe("error");
    expect(thread?.latestTurn?.completedAt).not.toBeNull();
    expect(thread?.session).toMatchObject({
      status: "stopped",
      activeTurnId: null,
      lastError: providerSessionDisappearedDuringTurnError(turnId),
    });
    expect(harness.stopSession).toHaveBeenCalledWith({ threadId });
    const released = Option.getOrThrow(
      await runtime!.runPromise(repository.getByThreadId({ threadId })),
    );
    expect(released.status).toBe("stopped");
    expect(released.resumeCursor).toEqual({ opaque: `resume-${threadId}` });
    expect(released.runtimePayload).toMatchObject({
      activeTurnId: null,
      cwd: `/tmp/${threadId}`,
      detached: true,
      model: "test-watchdog-model",
    });
  });

  it("defers a projected running turn while its live session is already ready", async () => {
    const threadId = ThreadId.make("thread-watchdog-ready-projection-lag");
    const turnId = TurnId.make("turn-watchdog-ready-projection-lag");
    const startedAt = "2026-01-01T00:00:00.000Z";
    const lastSeenAt = DateTime.formatIso(DateTime.nowUnsafe());
    let snapshotCount = 0;
    const runningReadModel = makeReadModel([
      {
        id: threadId,
        latestTurn: {
          turnId,
          state: "running",
          requestedAt: startedAt,
          startedAt,
          completedAt: null,
          assistantMessageId: null,
        },
        session: {
          threadId,
          status: "running",
          providerName: "claudeAgent",
          runtimeMode: "full-access",
          activeTurnId: turnId,
          lastError: null,
          updatedAt: startedAt,
        },
      },
    ]);
    const harness = await createHarness({
      sweepIntervalMs: 100,
      listSessionsImplementation: () =>
        Effect.sync(() => {
          snapshotCount += 1;
          return [
            {
              provider: ProviderDriverKind.make("claudeAgent"),
              providerInstanceId: ProviderInstanceId.make("claudeAgent"),
              status: "ready" as const,
              runtimeMode: "full-access" as const,
              threadId,
              createdAt: startedAt,
              updatedAt: startedAt,
            },
          ];
        }),
      readModel: runningReadModel,
    });
    await seedActiveRuntimeBinding({ threadId, turnId, lastSeenAt });

    await startReaper();
    await waitFor(() => snapshotCount >= 1);
    await runtime!.runPromise(drainFibers);

    expect(harness.readModel().threads[0]?.latestTurn?.state).toBe("running");
    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();

    const snapshotsBeforeProjectionCatchUp = snapshotCount;
    harness.setReadModel(
      makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "completed",
            requestedAt: startedAt,
            startedAt,
            completedAt: startedAt,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    );
    await waitFor(() => snapshotCount > snapshotsBeforeProjectionCatchUp);
    await runtime!.runPromise(drainFibers);

    expect(harness.readModel().threads[0]?.latestTurn?.state).toBe("completed");
    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();
  });

  it("does not classify a present ready session as process disappearance", async () => {
    const threadId = ThreadId.make("thread-watchdog-ready-stuck-projection");
    const turnId = TurnId.make("turn-watchdog-ready-stuck-projection");
    const startedAt = "2026-01-01T00:00:00.000Z";
    let snapshotCount = 0;
    const harness = await createHarness({
      sweepIntervalMs: 2,
      listSessionsImplementation: () =>
        Effect.sync(() => {
          snapshotCount += 1;
          return [
            {
              provider: ProviderDriverKind.make("claudeAgent"),
              providerInstanceId: ProviderInstanceId.make("claudeAgent"),
              status: "ready" as const,
              runtimeMode: "full-access" as const,
              threadId,
              createdAt: startedAt,
              updatedAt: startedAt,
            },
          ];
        }),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "running",
            requestedAt: startedAt,
            startedAt,
            completedAt: null,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    });
    await seedActiveRuntimeBinding({ threadId, turnId });

    await startReaper();
    await waitFor(() => snapshotCount >= 2);
    await runtime!.runPromise(drainFibers);

    expect(snapshotCount).toBeGreaterThanOrEqual(2);
    expect(harness.stopSession).not.toHaveBeenCalled();
    expect(harness.readModel().threads[0]?.latestTurn?.state).toBe("running");
  });

  it("defers a projected running turn already recorded terminal in its binding", async () => {
    const threadId = ThreadId.make("thread-watchdog-terminal-binding-projection-lag");
    const turnId = TurnId.make("turn-watchdog-terminal-binding-projection-lag");
    const startedAt = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      sweepIntervalMs: 60_000,
      listSessionsImplementation: () => Effect.succeed([]),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "running",
            requestedAt: startedAt,
            startedAt,
            completedAt: null,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    });
    await seedActiveRuntimeBinding({
      threadId,
      turnId,
      lastTerminalTurnId: turnId,
    });

    await startReaper();
    await runtime!.runPromise(drainFibers);

    expect(harness.readModel().threads[0]?.latestTurn?.state).toBe("running");
    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();
  });

  it("fails an orphaned projected turn after one terminal-binding grace sweep", async () => {
    const threadId = ThreadId.make("thread-watchdog-terminal-binding-stuck-projection");
    const turnId = TurnId.make("turn-watchdog-terminal-binding-stuck-projection");
    const startedAt = "2026-01-01T00:00:00.000Z";
    let snapshotCount = 0;
    const harness = await createHarness({
      sweepIntervalMs: 2,
      listSessionsImplementation: () =>
        Effect.sync(() => {
          snapshotCount += 1;
          return [];
        }),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "running",
            requestedAt: startedAt,
            startedAt,
            completedAt: null,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    });
    await seedActiveRuntimeBinding({
      threadId,
      turnId,
      lastTerminalTurnId: turnId,
    });

    await startReaper();
    await waitFor(() => harness.readModel().threads[0]?.latestTurn?.state === "error");

    expect(snapshotCount).toBeGreaterThanOrEqual(2);
    expect(harness.stopSession).toHaveBeenCalledWith({ threadId });
    expect(harness.readModel().threads[0]?.session).toMatchObject({
      status: "stopped",
      lastError: providerSessionDisappearedDuringTurnError(turnId),
    });
  });

  it("continues binding-backed healing when the live-session snapshot fails", async () => {
    const failedThreadId = ThreadId.make("thread-watchdog-snapshot-binding-error");
    const healthyThreadId = ThreadId.make("thread-watchdog-snapshot-unknown");
    const failedTurnId = TurnId.make("turn-watchdog-snapshot-binding-error");
    const healthyTurnId = TurnId.make("turn-watchdog-snapshot-unknown");
    const startedAt = "2026-01-01T00:00:00.000Z";
    const runningThread = (threadId: ThreadId, turnId: TurnId) => ({
      id: threadId,
      latestTurn: {
        turnId,
        state: "running" as const,
        requestedAt: startedAt,
        startedAt,
        completedAt: null,
        assistantMessageId: null,
      },
      session: {
        threadId,
        status: "running" as const,
        providerName: "claudeAgent" as const,
        runtimeMode: "full-access" as const,
        activeTurnId: turnId,
        lastError: null,
        updatedAt: startedAt,
      },
    });
    const harness = await createHarness({
      listSessionsImplementation: () => Effect.die("simulated listSessions failure"),
      readModel: makeReadModel([
        runningThread(failedThreadId, failedTurnId),
        runningThread(healthyThreadId, healthyTurnId),
      ]),
    });
    await seedActiveRuntimeBinding({
      threadId: failedThreadId,
      turnId: failedTurnId,
      status: "error",
      lastError: "provider process already failed",
    });
    await seedActiveRuntimeBinding({ threadId: healthyThreadId, turnId: healthyTurnId });

    await startReaper();
    await waitFor(
      () =>
        harness.readModel().threads.find((thread) => thread.id === failedThreadId)?.latestTurn
          ?.state === "error",
    );

    expect(
      harness.readModel().threads.find((thread) => thread.id === failedThreadId)?.latestTurn?.state,
    ).toBe("error");
    expect(
      harness.readModel().threads.find((thread) => thread.id === healthyThreadId)?.latestTurn
        ?.state,
    ).toBe("running");
    expect(harness.stopSession).toHaveBeenCalledTimes(1);
    expect(harness.stopSession).toHaveBeenCalledWith({ threadId: failedThreadId });
  });

  it("keeps failed physical cleanup retryable without publishing a stopped session", async () => {
    const threadId = ThreadId.make("thread-watchdog-stop-failed");
    const turnId = TurnId.make("turn-watchdog-stop-failed");
    const startedAt = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      listSessionsImplementation: () =>
        Effect.succeed([
          {
            provider: ProviderDriverKind.make("claudeAgent"),
            providerInstanceId: ProviderInstanceId.make("claudeAgent"),
            status: "closed",
            runtimeMode: "full-access",
            threadId,
            activeTurnId: turnId,
            lastError: "provider transport closed",
            createdAt: startedAt,
            updatedAt: startedAt,
          },
        ]),
      stopSessionImplementation: () =>
        Effect.fail(
          new ProviderValidationError({
            operation: "ProviderSessionReaper.test",
            issue: "simulated watchdog cleanup failure",
          }),
        ),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "running",
            requestedAt: startedAt,
            startedAt,
            completedAt: null,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    });
    const repository = await seedActiveRuntimeBinding({
      threadId,
      turnId,
      status: "error",
      lastError: "provider transport closed",
    });

    await startReaper();
    await waitFor(() => harness.readModel().threads[0]?.latestTurn?.state === "error");

    expect(harness.readModel().threads[0]?.session).toMatchObject({
      status: "error",
      activeTurnId: null,
      lastError: PROVIDER_SESSION_FAILED_DURING_TURN_ERROR,
    });
    const binding = Option.getOrThrow(
      await runtime!.runPromise(repository.getByThreadId({ threadId })),
    );
    expect(binding.status).toBe("error");
    expect(binding.runtimePayload).toMatchObject({
      activeTurnId: null,
      lastRuntimeEvent: "provider.turn.watchdog.stop-pending",
    });
  });

  it("force-fails a stuck running turn after consecutive unconfirmable cleanup sweeps", async () => {
    const threadId = ThreadId.make("thread-watchdog-force-fail");
    const turnId = TurnId.make("turn-watchdog-force-fail");
    const startedAt = "2026-01-01T00:00:00.000Z";
    // Grok wedge regression (2026-07-20): the binding records a stopped
    // session while the projected turn is still running, and the provider stop
    // is never confirmable (stale ownership), so every sweep used to end in a
    // cleanup-pending-or-stale no-op — the turn stayed "running" forever,
    // surviving server restarts.
    const harness = await createHarness({
      sweepIntervalMs: 50,
      forceFailNoopSweeps: 3,
      stopFailedSessionImplementation: () => Effect.succeed(false),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "running",
            requestedAt: startedAt,
            startedAt,
            completedAt: null,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    });
    const repository = await seedActiveRuntimeBinding({
      threadId,
      turnId,
      status: "stopped",
    });

    await startReaper();
    await waitFor(() => harness.readModel().threads[0]?.latestTurn?.state === "error");

    expect(harness.stopFailedSessionCalls.length).toBeGreaterThanOrEqual(3);
    expect(harness.readModel().threads[0]?.session).toMatchObject({
      status: "error",
      activeTurnId: null,
      lastError: "Provider session closed while the turn was running.",
    });
    const binding = Option.getOrThrow(
      await runtime!.runPromise(repository.getByThreadId({ threadId })),
    );
    expect(binding.status).toBe("stopped");
    expect(binding.runtimePayload).toMatchObject({
      activeTurnId: null,
      lastRuntimeEvent: "provider.turn.watchdog.force-failed",
      lastTerminalTurnId: turnId,
    });
  });

  it("never force-fails while the binding's ownership keeps changing (replacement fence)", async () => {
    const threadId = ThreadId.make("thread-watchdog-force-fence");
    const turnId = TurnId.make("turn-watchdog-force-fence");
    const startedAt = "2026-01-01T00:00:00.000Z";
    // Replacement fence: an unconfirmable stop whose binding ownership changes
    // between sweeps means a replacement session is claiming the thread — the
    // consecutive-no-op counter must reset instead of force-failing the turn.
    let ownershipCounter = 0;
    let mutateBinding: (() => Promise<void>) | null = null;
    const harness = await createHarness({
      sweepIntervalMs: 50,
      forceFailNoopSweeps: 2,
      stopFailedSessionImplementation: () =>
        Effect.promise(async () => {
          await mutateBinding?.();
          return false;
        }),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "running",
            requestedAt: startedAt,
            startedAt,
            completedAt: null,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    });
    await seedActiveRuntimeBinding({
      threadId,
      turnId,
      status: "stopped",
      sessionOwnershipId: "ownership-0",
    });
    mutateBinding = async () => {
      ownershipCounter += 1;
      await seedActiveRuntimeBinding({
        threadId,
        turnId,
        status: "stopped",
        sessionOwnershipId: `ownership-${ownershipCounter}`,
      });
    };

    await startReaper();
    await waitFor(() => harness.stopFailedSessionCalls.length >= 6);

    expect(harness.readModel().threads[0]?.latestTurn?.state).toBe("running");
    expect(
      harness.dispatchedCommands.filter((command) => command.type === "thread.session.set"),
    ).toEqual([]);
  });

  it.each([
    { change: "ownership-and-turn", replacementOwnershipId: "replacement-owner", movesTurn: true },
    { change: "turn", replacementOwnershipId: "stale-owner", movesTurn: true },
  ])(
    "does not publish a forced failure when replacement $change changes between force checks",
    async ({ change, replacementOwnershipId, movesTurn }) => {
      const threadId = ThreadId.make(`thread-watchdog-force-dispatch-fence-${change}`);
      const staleTurnId = TurnId.make(`turn-watchdog-force-dispatch-fence-${change}-stale`);
      const replacementTurnId = movesTurn
        ? TurnId.make(`turn-watchdog-force-dispatch-fence-${change}-replacement`)
        : staleTurnId;
      const startedAt = "2026-01-01T00:00:00.000Z";
      let replaceBinding = async () => {};
      const harness = await createHarness({
        sweepIntervalMs: 50,
        forceFailNoopSweeps: 1,
        stopFailedSessionImplementation: () => Effect.succeed(false),
        afterBindingRead: (readCount) =>
          readCount === 1 ? Effect.promise(() => replaceBinding()) : Effect.void,
        readModel: makeReadModel([
          {
            id: threadId,
            latestTurn: {
              turnId: staleTurnId,
              state: "running",
              requestedAt: startedAt,
              startedAt,
              completedAt: null,
              assistantMessageId: null,
            },
            session: {
              threadId,
              status: "running",
              providerName: "claudeAgent",
              runtimeMode: "full-access",
              activeTurnId: staleTurnId,
              lastError: null,
              updatedAt: startedAt,
            },
          },
        ]),
      });
      const repository = await seedActiveRuntimeBinding({
        threadId,
        turnId: staleTurnId,
        status: "stopped",
        sessionOwnershipId: "stale-owner",
      });
      replaceBinding = () =>
        seedActiveRuntimeBinding({
          threadId,
          turnId: replacementTurnId,
          status: "stopped",
          sessionOwnershipId: replacementOwnershipId,
        }).then(() => undefined);

      const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
      scope = await runtime!.runPromise(Scope.make("sequential"));
      await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));
      await waitFor(() => harness.stopFailedSessionCalls.length >= 3);

      expect(harness.readModel().threads[0]?.latestTurn?.state).toBe("running");
      expect(
        harness.dispatchedCommands.filter((command) => command.type === "thread.session.set"),
      ).toEqual([]);
      expect(
        Option.getOrThrow(await runtime!.runPromise(repository.getByThreadId({ threadId })))
          .runtimePayload,
      ).toMatchObject({
        activeTurnId: replacementTurnId,
        sessionOwnershipId: replacementOwnershipId,
      });
    },
  );

  it.each(["starting", "running", "waiting"] as const)(
    "does not force-fail when the latest binding becomes non-terminal (%s)",
    async (status) => {
      const threadId = ThreadId.make(`thread-watchdog-force-live-${status}`);
      const turnId = TurnId.make(`turn-watchdog-force-live-${status}`);
      const startedAt = "2026-01-01T00:00:00.000Z";
      let reviveBinding = async () => {};
      const harness = await createHarness({
        sweepIntervalMs: 50,
        forceFailNoopSweeps: 1,
        stopFailedSessionImplementation: () =>
          Effect.promise(async () => {
            await reviveBinding();
            return false;
          }),
        readModel: makeReadModel([
          {
            id: threadId,
            latestTurn: {
              turnId,
              state: "running",
              requestedAt: startedAt,
              startedAt,
              completedAt: null,
              assistantMessageId: null,
            },
            session: {
              threadId,
              status: "running",
              providerName: "claudeAgent",
              runtimeMode: "full-access",
              activeTurnId: turnId,
              lastError: null,
              updatedAt: startedAt,
            },
          },
        ]),
      });
      await seedActiveRuntimeBinding({
        threadId,
        turnId,
        status: "stopped",
        sessionOwnershipId: "stable-owner",
      });
      reviveBinding = () =>
        seedActiveRuntimeBinding({
          threadId,
          turnId,
          status,
          sessionOwnershipId: "stable-owner",
        }).then(() => undefined);

      const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
      scope = await runtime!.runPromise(Scope.make("sequential"));
      await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));
      await waitFor(() => harness.stopFailedSessionCalls.length >= 3);

      expect(harness.readModel().threads[0]?.latestTurn?.state).toBe("running");
      expect(
        harness.dispatchedCommands.filter((command) => command.type === "thread.session.set"),
      ).toEqual([]);
    },
  );

  it("retries failed physical cleanup before publishing a stopped session", async () => {
    const threadId = ThreadId.make("thread-watchdog-stop-retry");
    const turnId = TurnId.make("turn-watchdog-stop-retry");
    const failedAt = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      listSessionsImplementation: () =>
        Effect.succeed([
          {
            provider: ProviderDriverKind.make("claudeAgent"),
            providerInstanceId: ProviderInstanceId.make("claudeAgent"),
            status: "closed",
            runtimeMode: "full-access",
            threadId,
            activeTurnId: turnId,
            lastError: "provider transport closed",
            createdAt: failedAt,
            updatedAt: failedAt,
          },
        ]),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "error",
            requestedAt: failedAt,
            startedAt: failedAt,
            completedAt: failedAt,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "error",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: "provider transport closed",
            updatedAt: failedAt,
          },
        },
      ]),
    });
    const repository = await seedActiveRuntimeBinding({
      threadId,
      turnId,
      status: "error",
      lastError: "provider transport closed",
    });

    await startReaper();
    await waitFor(() => harness.readModel().threads[0]?.session?.status === "stopped");

    expect(harness.stopSession).toHaveBeenCalledWith({ threadId });
    expect(harness.readModel().threads[0]?.session).toMatchObject({
      status: "stopped",
      activeTurnId: null,
      lastError: "provider transport closed",
    });
    const binding = Option.getOrThrow(
      await runtime!.runPromise(repository.getByThreadId({ threadId })),
    );
    expect(binding.status).toBe("stopped");
    expect(binding.runtimePayload).toMatchObject({
      activeTurnId: null,
      lastRuntimeEvent: "provider.turn.watchdog.retry-released",
    });
  });

  it("fails a legacy live turn when its permission request exceeds the timeout", async () => {
    const threadId = ThreadId.make("thread-watchdog-permission-timeout");
    const turnId = TurnId.make("turn-watchdog-permission-timeout");
    const requestId = ApprovalRequestId.make("approval-watchdog-timeout");
    const startedAt = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      permissionRequestTimeoutMs: 1,
      listSessionsImplementation: () =>
        Effect.succeed([
          {
            provider: ProviderDriverKind.make("claudeAgent"),
            providerInstanceId: ProviderInstanceId.make("claudeAgent"),
            status: "waiting",
            runtimeMode: "full-access",
            threadId,
            activeTurnId: turnId,
            createdAt: startedAt,
            updatedAt: startedAt,
          },
        ]),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "running",
            requestedAt: startedAt,
            startedAt,
            completedAt: null,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    });
    const repository = await seedActiveRuntimeBinding({ threadId, turnId });
    const approvals = await runtime!.runPromise(
      Effect.service(ProjectionPendingApprovalRepository),
    );
    await runtime!.runPromise(
      approvals.upsert({
        requestId,
        threadId,
        turnId,
        status: "pending",
        decision: null,
        createdAt: startedAt,
        resolvedAt: null,
      }),
    );

    await startReaper();

    await waitFor(() => harness.readModel().threads[0]?.latestTurn?.state === "error");

    const thread = harness.readModel().threads[0];
    expect(harness.stopFailedSessionCalls).toContainEqual({
      threadId,
      turnId,
      allowLegacyActiveTurnMatch: true,
    });
    expect(thread?.latestTurn?.state).toBe("error");
    expect(thread?.session).toMatchObject({
      status: "stopped",
      activeTurnId: null,
      lastError: expect.stringContaining(String(requestId)),
    });
    expect(harness.stopSession).toHaveBeenCalledWith({ threadId });
    expect(
      harness.dispatchedCommands.some(
        (command) =>
          command.type === "thread.activity.append" &&
          command.activity.kind === "approval.resolved" &&
          typeof command.activity.payload === "object" &&
          command.activity.payload !== null &&
          "requestId" in command.activity.payload &&
          command.activity.payload.requestId === requestId,
      ),
    ).toBe(true);
    expect(
      Option.getOrThrow(await runtime!.runPromise(repository.getByThreadId({ threadId }))).status,
    ).toBe("stopped");
  });

  it("prefers dead-session failure over an expired permission in the same sweep", async () => {
    const threadId = ThreadId.make("thread-watchdog-dead-with-expired-permission");
    const turnId = TurnId.make("turn-watchdog-dead-with-expired-permission");
    const requestId = ApprovalRequestId.make("approval-watchdog-dead-session");
    const startedAt = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      permissionRequestTimeoutMs: 1,
      sweepIntervalMs: 60_000,
      listSessionsImplementation: () => Effect.succeed([]),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "running",
            requestedAt: startedAt,
            startedAt,
            completedAt: null,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    });
    await seedActiveRuntimeBinding({ threadId, turnId });
    const approvals = await runtime!.runPromise(
      Effect.service(ProjectionPendingApprovalRepository),
    );
    await runtime!.runPromise(
      approvals.upsert({
        requestId,
        threadId,
        turnId,
        status: "pending",
        decision: null,
        createdAt: startedAt,
        resolvedAt: null,
      }),
    );

    await startReaper();
    await waitFor(() => harness.readModel().threads[0]?.latestTurn?.state === "error");

    expect(harness.readModel().threads[0]?.session).toMatchObject({
      status: "stopped",
      activeTurnId: null,
      lastError: providerSessionDisappearedDuringTurnError(turnId),
    });
    expect(harness.readModel().threads[0]?.session?.lastError).not.toContain(String(requestId));
    expect(
      harness.dispatchedCommands.some(
        (command) =>
          command.type === "thread.activity.append" &&
          command.activity.kind === "approval.resolved" &&
          typeof command.activity.payload === "object" &&
          command.activity.payload !== null &&
          "requestId" in command.activity.payload &&
          command.activity.payload.requestId === requestId,
      ),
    ).toBe(true);
  });

  it("does not expire a permission when provider liveness cannot be proven", async () => {
    const threadId = ThreadId.make("thread-watchdog-unknown-liveness-permission");
    const turnId = TurnId.make("turn-watchdog-unknown-liveness-permission");
    const requestId = ApprovalRequestId.make("approval-watchdog-unknown-liveness");
    const startedAt = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      permissionRequestTimeoutMs: 1,
      sweepIntervalMs: 2,
      listSessionsImplementation: () => Effect.die("simulated session snapshot failure"),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "running",
            requestedAt: startedAt,
            startedAt,
            completedAt: null,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    });
    await seedActiveRuntimeBinding({ threadId, turnId });
    const approvals = await runtime!.runPromise(
      Effect.service(ProjectionPendingApprovalRepository),
    );
    await runtime!.runPromise(
      approvals.upsert({
        requestId,
        threadId,
        turnId,
        status: "pending",
        decision: null,
        createdAt: startedAt,
        resolvedAt: null,
      }),
    );

    await startReaper();
    await runtime!.runPromise(Effect.sleep("30 millis"));

    expect(harness.readModel().threads[0]?.latestTurn?.state).toBe("running");
    expect(harness.stopSession).not.toHaveBeenCalled();
    expect(harness.dispatch).not.toHaveBeenCalled();
  });

  it("never fails a slow turn while its provider session remains live", async () => {
    const threadId = ThreadId.make("thread-watchdog-slow-healthy");
    const turnId = TurnId.make("turn-watchdog-slow-healthy");
    const startedAt = "2020-01-01T00:00:00.000Z";
    const harness = await createHarness({
      sweepIntervalMs: 2,
      listSessionsImplementation: () =>
        Effect.succeed([
          {
            provider: ProviderDriverKind.make("claudeAgent"),
            providerInstanceId: ProviderInstanceId.make("claudeAgent"),
            status: "running",
            runtimeMode: "full-access",
            threadId,
            activeTurnId: turnId,
            createdAt: startedAt,
            updatedAt: startedAt,
          },
        ]),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "running",
            requestedAt: startedAt,
            startedAt,
            completedAt: null,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    });
    const repository = await seedActiveRuntimeBinding({ threadId, turnId });

    await startReaper();
    await runtime!.runPromise(Effect.sleep("30 millis"));

    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();
    expect(harness.readModel().threads[0]?.latestTurn?.state).toBe("running");
    expect(
      Option.getOrThrow(await runtime!.runPromise(repository.getByThreadId({ threadId }))).status,
    ).toBe("running");
  });

  it("treats a connecting session that owns the projected turn as live", async () => {
    const threadId = ThreadId.make("thread-watchdog-connecting-live");
    const turnId = TurnId.make("turn-watchdog-connecting-live");
    const startedAt = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      sweepIntervalMs: 2,
      listSessionsImplementation: () =>
        Effect.succeed([
          {
            provider: ProviderDriverKind.make("claudeAgent"),
            providerInstanceId: ProviderInstanceId.make("claudeAgent"),
            status: "connecting",
            runtimeMode: "full-access",
            threadId,
            activeTurnId: turnId,
            createdAt: startedAt,
            updatedAt: startedAt,
          },
        ]),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "running",
            requestedAt: startedAt,
            startedAt,
            completedAt: null,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    });
    await seedActiveRuntimeBinding({ threadId, turnId });

    await startReaper();
    await runtime!.runPromise(Effect.sleep("30 millis"));

    expect(harness.readModel().threads[0]?.latestTurn?.state).toBe("running");
    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();
  });

  it("prefers an exact live turn over a stale same-thread session", async () => {
    const threadId = ThreadId.make("thread-watchdog-replacement-live");
    const turnId = TurnId.make("turn-watchdog-replacement-live");
    const staleTurnId = TurnId.make("turn-watchdog-stale-session");
    const startedAt = "2020-01-01T00:00:00.000Z";
    const harness = await createHarness({
      sweepIntervalMs: 2,
      listSessionsImplementation: () =>
        Effect.succeed([
          {
            provider: ProviderDriverKind.make("claudeAgent"),
            providerInstanceId: ProviderInstanceId.make("claudeAgent"),
            status: "error",
            runtimeMode: "full-access",
            threadId,
            activeTurnId: staleTurnId,
            lastError: "stale provider process failed",
            createdAt: startedAt,
            updatedAt: startedAt,
          },
          {
            provider: ProviderDriverKind.make("claudeAgent"),
            providerInstanceId: ProviderInstanceId.make("claudeAgent"),
            status: "running",
            runtimeMode: "full-access",
            threadId,
            activeTurnId: turnId,
            createdAt: startedAt,
            updatedAt: startedAt,
          },
        ]),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "running",
            requestedAt: startedAt,
            startedAt,
            completedAt: null,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    });
    const repository = await seedActiveRuntimeBinding({ threadId, turnId });

    await startReaper();
    await runtime!.runPromise(Effect.sleep("30 millis"));

    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();
    expect(harness.readModel().threads[0]?.latestTurn?.state).toBe("running");
    expect(
      Option.getOrThrow(await runtime!.runPromise(repository.getByThreadId({ threadId }))).status,
    ).toBe("running");
  });

  it("defers a projected old turn while a live replacement turn owns the thread", async () => {
    const threadId = ThreadId.make("thread-watchdog-mismatched-live-turn");
    const turnId = TurnId.make("turn-watchdog-projected");
    const otherTurnId = TurnId.make("turn-watchdog-provider");
    const startedAt = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      sweepIntervalMs: 2,
      listSessionsImplementation: () =>
        Effect.succeed([
          {
            provider: ProviderDriverKind.make("claudeAgent"),
            providerInstanceId: ProviderInstanceId.make("claudeAgent"),
            status: "running",
            runtimeMode: "full-access",
            threadId,
            activeTurnId: otherTurnId,
            createdAt: startedAt,
            updatedAt: startedAt,
          },
        ]),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "running",
            requestedAt: startedAt,
            startedAt,
            completedAt: null,
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: startedAt,
          },
        },
      ]),
    });
    await seedActiveRuntimeBinding({ threadId, turnId });

    await startReaper();
    await runtime!.runPromise(Effect.sleep("30 millis"));

    expect(harness.readModel().threads[0]?.latestTurn).toMatchObject({
      turnId,
      state: "running",
    });
    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();
  });

  it("reaps stale persisted sessions without active turns", async () => {
    const threadId = ThreadId.make("thread-reaper-stale");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-stale",
        },
        runtimePayload: null,
      }),
    );

    await startReaper();

    await waitFor(() => harness.stopSession.mock.calls.length === 1);

    expect(harness.stopSession.mock.calls[0]?.[0]).toEqual({ threadId });
    expect(harness.stoppedThreadIds.has(threadId)).toBe(true);
  });

  it("does not reap a fresh replacement from a stale inactivity snapshot", async () => {
    const threadId = ThreadId.make("thread-reaper-stale-snapshot-replacement");
    const staleTurnId = TurnId.make("turn-reaper-stale-snapshot");
    const replacementTurnId = TurnId.make("turn-reaper-stale-snapshot-replacement");
    const now = DateTime.formatIso(DateTime.nowUnsafe());
    let replaceBinding = async () => {};
    let snapshotCount = 0;
    const harness = await createHarness({
      sweepIntervalMs: 50,
      listSessionsImplementation: () =>
        Effect.promise(async () => {
          snapshotCount += 1;
          if (snapshotCount === 1) {
            await replaceBinding();
          }
          return [];
        }),
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
    });
    await seedActiveRuntimeBinding({
      threadId,
      turnId: staleTurnId,
      status: "running",
      activeTurnId: null,
      lastSeenAt: "2020-01-01T00:00:00.000Z",
      sessionOwnershipId: "stale-owner",
    });
    replaceBinding = () =>
      seedActiveRuntimeBinding({
        threadId,
        turnId: replacementTurnId,
        status: "running",
        lastSeenAt: now,
        sessionOwnershipId: "replacement-owner",
      }).then(() => undefined);

    await startReaper();
    await waitFor(() => snapshotCount >= 3);

    expect(harness.stopSession).not.toHaveBeenCalled();
  });

  it("skips stale sessions when the thread still has an active turn", async () => {
    const threadId = ThreadId.make("thread-reaper-active-turn");
    const turnId = TurnId.make("turn-reaper-active");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-active-turn",
        },
        runtimePayload: null,
      }),
    );

    await startReaper();
    await runtime!.runPromise(drainFibers);

    expect(harness.stopSession).not.toHaveBeenCalled();
    const remaining = await runtime!.runPromise(repository.getByThreadId({ threadId }));
    expect(Option.isSome(remaining)).toBe(true);
  });

  it("skips terminal active-turn projections instead of reconciling them", async () => {
    const threadId = ThreadId.make("thread-reaper-terminal-active-turn");
    const turnId = TurnId.make("turn-reaper-terminal");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "completed",
            requestedAt: "2026-01-01T00:00:00.000Z",
            startedAt: "2026-01-01T00:00:05.000Z",
            completedAt: "2026-01-01T00:00:10.000Z",
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: ProviderInstanceId.make("claudeAgent"),
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-terminal-active-turn",
        },
        runtimePayload: { activeTurnId: turnId },
      }),
    );

    await startReaper();
    await runtime!.runPromise(drainFibers);

    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();
    const remaining = await runtime!.runPromise(repository.getByThreadId({ threadId }));
    expect(Option.getOrThrow(remaining).status).toBe("running");
  });

  it("does not reconcile terminal active-turn projections while the provider session is alive", async () => {
    const threadId = ThreadId.make("thread-reaper-provider-alive");
    const turnId = TurnId.make("turn-reaper-provider-alive");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      listSessionsImplementation: () =>
        Effect.succeed([
          {
            provider: ProviderDriverKind.make("claudeAgent"),
            providerInstanceId: ProviderInstanceId.make("claudeAgent"),
            status: "running",
            runtimeMode: "full-access",
            threadId,
            activeTurnId: turnId,
            createdAt: now,
            updatedAt: now,
          },
        ]),
      readModel: makeReadModel([
        {
          id: threadId,
          latestTurn: {
            turnId,
            state: "completed",
            requestedAt: "2026-01-01T00:00:00.000Z",
            startedAt: "2026-01-01T00:00:05.000Z",
            completedAt: "2026-01-01T00:00:10.000Z",
            assistantMessageId: null,
          },
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: ProviderInstanceId.make("claudeAgent"),
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-provider-alive",
        },
        runtimePayload: { activeTurnId: turnId },
      }),
    );

    await startReaper();
    await runtime!.runPromise(drainFibers);

    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();
    const remaining = await runtime!.runPromise(repository.getByThreadId({ threadId }));
    expect(Option.getOrThrow(remaining).status).toBe("running");
  });

  it("skips multiple terminal active-turn projections without reconciling", async () => {
    const defectThreadId = ThreadId.make("thread-reaper-reconcile-defect");
    const reconciledThreadId = ThreadId.make("thread-reaper-reconcile-after-defect");
    const defectTurnId = TurnId.make("turn-reaper-reconcile-defect");
    const reconciledTurnId = TurnId.make("turn-reaper-reconcile-after-defect");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: defectThreadId,
          latestTurn: {
            turnId: defectTurnId,
            state: "completed",
            requestedAt: "2026-01-01T00:00:00.000Z",
            startedAt: "2026-01-01T00:00:05.000Z",
            completedAt: "2026-01-01T00:00:10.000Z",
            assistantMessageId: null,
          },
          session: {
            threadId: defectThreadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: defectTurnId,
            lastError: null,
            updatedAt: now,
          },
        },
        {
          id: reconciledThreadId,
          latestTurn: {
            turnId: reconciledTurnId,
            state: "completed",
            requestedAt: "2026-01-01T00:00:00.000Z",
            startedAt: "2026-01-01T00:00:05.000Z",
            completedAt: "2026-01-01T00:00:10.000Z",
            assistantMessageId: null,
          },
          session: {
            threadId: reconciledThreadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: reconciledTurnId,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId: defectThreadId,
        providerName: "claudeAgent",
        providerInstanceId: ProviderInstanceId.make("claudeAgent"),
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: null,
        runtimePayload: null,
      }),
    );
    await runtime!.runPromise(
      repository.upsert({
        threadId: reconciledThreadId,
        providerName: "claudeAgent",
        providerInstanceId: ProviderInstanceId.make("claudeAgent"),
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:01:00.000Z",
        resumeCursor: null,
        runtimePayload: null,
      }),
    );

    await startReaper();
    await runtime!.runPromise(drainFibers);

    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();
    const defectRuntime = await runtime!.runPromise(
      repository.getByThreadId({ threadId: defectThreadId }),
    );
    const reconciledRuntime = await runtime!.runPromise(
      repository.getByThreadId({ threadId: reconciledThreadId }),
    );
    expect(Option.getOrThrow(defectRuntime).status).toBe("running");
    expect(Option.getOrThrow(reconciledRuntime).status).toBe("running");
  });

  it("does not reap sessions that are still within the inactivity threshold", async () => {
    const threadId = ThreadId.make("thread-reaper-fresh");
    const now = DateTime.formatIso(DateTime.nowUnsafe());
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: now,
        resumeCursor: {
          opaque: "resume-fresh",
        },
        runtimePayload: null,
      }),
    );

    await startReaper();
    await runtime!.runPromise(drainFibers);

    expect(harness.stopSession).not.toHaveBeenCalled();
    const remaining = await runtime!.runPromise(repository.getByThreadId({ threadId }));
    expect(Option.isSome(remaining)).toBe(true);
  });

  it("skips persisted sessions that are already marked stopped", async () => {
    const threadId = ThreadId.make("thread-reaper-stopped");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "stopped",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "stopped",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-stopped",
        },
        runtimePayload: null,
      }),
    );

    await startReaper();
    await runtime!.runPromise(drainFibers);

    expect(harness.stopSession).not.toHaveBeenCalled();
    const remaining = await runtime!.runPromise(repository.getByThreadId({ threadId }));
    expect(Option.isSome(remaining)).toBe(true);
  });

  it("continues reaping other sessions when one stop attempt fails", async () => {
    const failedThreadId = ThreadId.make("thread-reaper-stop-failure");
    const reapedThreadId = ThreadId.make("thread-reaper-stop-success");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: failedThreadId,
          session: {
            threadId: failedThreadId,
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
        {
          id: reapedThreadId,
          session: {
            threadId: reapedThreadId,
            status: "ready",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
      stopSessionImplementation: (request) =>
        request.threadId === failedThreadId
          ? Effect.fail(
              new ProviderValidationError({
                operation: "ProviderSessionReaper.test",
                issue: "simulated stop failure",
              }),
            )
          : Effect.void,
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId: failedThreadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-failure",
        },
        runtimePayload: null,
      }),
    );
    await runtime!.runPromise(
      repository.upsert({
        threadId: reapedThreadId,
        providerName: "codex",
        providerInstanceId: null,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:01:00.000Z",
        resumeCursor: {
          opaque: "resume-success",
        },
        runtimePayload: null,
      }),
    );

    await startReaper();

    await waitFor(() => harness.stopSession.mock.calls.length === 2);

    expect(harness.stopSession.mock.calls.map(([request]) => request.threadId)).toEqual([
      failedThreadId,
      reapedThreadId,
    ]);
  });

  it("continues reaping other sessions when one stop attempt defects", async () => {
    const defectThreadId = ThreadId.make("thread-reaper-stop-defect");
    const reapedThreadId = ThreadId.make("thread-reaper-stop-after-defect");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: defectThreadId,
          session: {
            threadId: defectThreadId,
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
        {
          id: reapedThreadId,
          session: {
            threadId: reapedThreadId,
            status: "ready",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
      stopSessionImplementation: (request) =>
        request.threadId === defectThreadId
          ? Effect.die(new Error("simulated stop defect"))
          : Effect.void,
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId: defectThreadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-defect",
        },
        runtimePayload: null,
      }),
    );
    await runtime!.runPromise(
      repository.upsert({
        threadId: reapedThreadId,
        providerName: "codex",
        providerInstanceId: null,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:01:00.000Z",
        resumeCursor: {
          opaque: "resume-after-defect",
        },
        runtimePayload: null,
      }),
    );

    await startReaper();

    await waitFor(() => harness.stopSession.mock.calls.length === 2);

    expect(harness.stopSession.mock.calls.map(([request]) => request.threadId)).toEqual([
      defectThreadId,
      reapedThreadId,
    ]);
  });
  it("skips stale sessions while background work is still live", async () => {
    const threadId = ThreadId.make("thread-reaper-background-work");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
          backgroundLiveness: "working",
        },
      ]),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-background-work",
        },
        runtimePayload: null,
      }),
    );

    await startReaper();
    await runtime!.runPromise(drainFibers);

    expect(harness.stopSession).not.toHaveBeenCalled();
    const remaining = await runtime!.runPromise(repository.getByThreadId({ threadId }));
    expect(Option.isSome(remaining)).toBe(true);
  });
});
