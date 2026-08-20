import {
  CheckpointRef,
  EnvironmentId,
  EventId,
  MessageId,
  NonNegativeInt,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type MatrixBridgeStatus,
  type OrchestrationEvent,
  type OrchestrationMessage,
  type OrchestrationThread,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { TestClock } from "effect/testing";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { ServerActivation } from "../serverActivation.ts";
import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { makeFakeMatrixBridgeClient, type FakeMatrixBridgeClient } from "./MatrixBridgeClient.ts";
import {
  MatrixBridgeReactor,
  MATRIX_BRIDGE_OUTBOUND_CAPACITY,
  MATRIX_BRIDGE_RETRY_WINDOW_MS,
  layer as MatrixBridgeReactorLive,
  makeBoundedDrainableWorker,
} from "./MatrixBridgeReactor.ts";
import { MatrixBridgeConfig, type MatrixBridgeConfigV1 } from "./MatrixBridgeConfig.ts";

const projectId = ProjectId.make("matrix-project");
const threadA = ThreadId.make("matrix-thread-a");
const threadB = ThreadId.make("matrix-thread-b");
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.5",
};
const baseAt = "2026-08-19T10:00:00.000Z";

const unsupported = () => Effect.die(new Error("Unsupported test dependency call")) as never;

function message(input: {
  readonly id: string;
  readonly role: OrchestrationMessage["role"];
  readonly text: string;
  readonly turnId?: TurnId | null;
  readonly streaming?: boolean;
  readonly at: string;
}): OrchestrationMessage {
  return {
    id: MessageId.make(input.id),
    role: input.role,
    text: input.text,
    turnId: input.turnId ?? null,
    streaming: input.streaming ?? false,
    createdAt: input.at,
    updatedAt: input.at,
  };
}

function thread(input: {
  readonly threadId?: ThreadId;
  readonly turnId: TurnId;
  readonly state: "running" | "completed" | "interrupted" | "error";
  readonly messages: ReadonlyArray<OrchestrationMessage>;
  readonly turns?: OrchestrationThread["turns"];
  readonly requestedAt?: string;
  readonly terminalAt?: string | null;
  readonly archivedAt?: string | null;
  readonly deletedAt?: string | null;
  readonly sessionStatus?: "running" | "waiting" | "ready" | "idle" | "error";
}): OrchestrationThread {
  const threadId = input.threadId ?? threadA;
  const terminalAt =
    input.terminalAt === undefined
      ? input.state === "running"
        ? null
        : "2026-08-19T10:00:10.000Z"
      : input.terminalAt;
  const latestTurn = {
    turnId: input.turnId,
    state: input.state,
    requestedAt: input.requestedAt ?? baseAt,
    startedAt: input.requestedAt ?? baseAt,
    completedAt: terminalAt,
    assistantMessageId: null,
  } as const;
  const sessionStatus = input.sessionStatus ?? (input.state === "running" ? "running" : "ready");

  return {
    id: threadId,
    projectId,
    dataAudience: "private",
    title: "Matrix test thread",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    latestTurn,
    turns: input.turns ?? [latestTurn],
    createdAt: baseAt,
    updatedAt: terminalAt ?? baseAt,
    archivedAt: input.archivedAt ?? null,
    deletedAt: input.deletedAt ?? null,
    settledOverride: null,
    settledAt: null,
    messages: [...input.messages],
    proposedPlans: [],
    activities: [],
    checkpoints: [],
    session: {
      threadId,
      status: sessionStatus,
      providerName: "codex",
      runtimeMode: "full-access",
      activeTurnId: input.state === "running" ? input.turnId : null,
      lastError: input.state === "error" ? "turn failed" : null,
      updatedAt: terminalAt ?? baseAt,
    },
  };
}

function eventBase(threadId: ThreadId, eventId: string, occurredAt: string) {
  return {
    sequence: NonNegativeInt.make(1),
    eventId: EventId.make(eventId),
    aggregateKind: "thread" as const,
    aggregateId: threadId,
    occurredAt,
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
  };
}

function atSequence(event: OrchestrationEvent, sequence: number): OrchestrationEvent {
  return { ...event, sequence: NonNegativeInt.make(sequence) };
}

function assistantEvent(input: {
  readonly threadId?: ThreadId;
  readonly turnId: TurnId | null;
  readonly messageId: string;
  readonly streaming?: boolean;
  readonly role?: "assistant" | "user" | "system";
  readonly at: string;
  readonly eventText?: string;
}): OrchestrationEvent {
  const threadId = input.threadId ?? threadA;
  return {
    ...eventBase(threadId, "event-" + input.messageId, input.at),
    type: "thread.message-sent",
    payload: {
      threadId,
      messageId: MessageId.make(input.messageId),
      role: input.role ?? "assistant",
      text: input.eventText ?? "",
      turnId: input.turnId,
      streaming: input.streaming ?? false,
      createdAt: input.at,
      updatedAt: input.at,
    },
  };
}

function toolEvent(threadId: ThreadId, turnId: TurnId, at: string): OrchestrationEvent {
  return {
    ...eventBase(threadId, "event-tool", at),
    type: "thread.activity-appended",
    payload: {
      threadId,
      activity: {
        id: EventId.make("activity-tool"),
        tone: "tool",
        kind: "tool.completed",
        summary: "Tool completed",
        payload: {},
        turnId,
        createdAt: at,
      },
    },
  };
}

function terminalMarkerEvent(input: {
  readonly kind: "session" | "turn-diff";
  readonly threadId?: ThreadId;
  readonly turnId: TurnId;
  readonly at: string;
  readonly sessionStatus?: "ready" | "idle" | "interrupted" | "stopped" | "error";
}): OrchestrationEvent {
  const threadId = input.threadId ?? threadA;
  if (input.kind === "session") {
    const sessionStatus = input.sessionStatus ?? "ready";
    return {
      ...eventBase(threadId, `event-session-${sessionStatus}-${input.turnId}`, input.at),
      type: "thread.session-set",
      payload: {
        threadId,
        session: {
          threadId,
          status: sessionStatus,
          providerName: "codex",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: sessionStatus === "error" ? "turn failed" : null,
          updatedAt: input.at,
        },
      },
    };
  }
  return {
    ...eventBase(threadId, `event-turn-diff-terminal-${input.turnId}`, input.at),
    type: "thread.turn-diff-completed",
    payload: {
      threadId,
      turnId: input.turnId,
      checkpointTurnCount: NonNegativeInt.make(1),
      checkpointRef: CheckpointRef.make(`refs/t3/checkpoints/${threadId}/${input.turnId}`),
      status: "ready",
      files: [],
      assistantMessageId: null,
      completedAt: input.at,
    },
  };
}

function lifecycleEvent(
  type: "thread.archived" | "thread.deleted",
  threadId: ThreadId,
  at: string,
): OrchestrationEvent {
  return type === "thread.archived"
    ? {
        ...eventBase(threadId, "event-archived", at),
        type,
        payload: { threadId, archivedAt: at, updatedAt: at },
      }
    : {
        ...eventBase(threadId, "event-deleted", at),
        type,
        payload: { threadId, deletedAt: at },
      };
}

function status(ownerThreadId: ThreadId | null): MatrixBridgeStatus {
  return {
    state: "active",
    ownerThreadId,
    encryptionReady: true,
    reason: null,
  };
}

interface ReactorHarness {
  readonly reactor: MatrixBridgeReactor["Service"];
  readonly fake: FakeMatrixBridgeClient;
  readonly publish: (event: OrchestrationEvent) => Effect.Effect<void>;
  readonly publishWithoutDrain: (event: OrchestrationEvent) => Effect.Effect<void>;
  readonly setThread: (value: OrchestrationThread | null) => Effect.Effect<void>;
  readonly setOwner: (ownerThreadId: ThreadId | null) => Effect.Effect<MatrixBridgeStatus>;
  readonly replaceConfig: (config: MatrixBridgeConfigV1) => Effect.Effect<void>;
  readonly currentConfig: Effect.Effect<Option.Option<MatrixBridgeConfigV1>>;
  readonly inspectionCount: Effect.Effect<number>;
  readonly blockNextInspection: (gate: Deferred.Deferred<void>) => Effect.Effect<void>;
  readonly blockNextInspectionBeforeRead: (gate: Deferred.Deferred<void>) => Effect.Effect<void>;
  readonly awaitInspection: Effect.Effect<void>;
  readonly awaitInspectionBeforeRead: Effect.Effect<void>;
  readonly awaitCheckpointInitialized: Effect.Effect<void>;
  readonly awaitOwner: (ownerThreadId: ThreadId | null) => Effect.Effect<void>;
}

interface ReactorHarnessOptions {
  readonly initialOwner?: ThreadId | null;
  readonly initialThread?: OrchestrationThread | null;
  readonly lastDeliveredTurnId?: TurnId | null;
  readonly deliveryBaselineSequence?: number;
  readonly deliveryCheckpointInitialized?: boolean;
  readonly historicalEvents?: ReadonlyArray<OrchestrationEvent>;
  readonly activation?: Effect.Effect<void>;
}

function withHarness<A, E>(
  use: (harness: ReactorHarness) => Effect.Effect<A, E>,
  options: ReactorHarnessOptions = {},
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const domainEvents = yield* Queue.unbounded<OrchestrationEvent>();
      const observedEvents = yield* Queue.unbounded<void>();
      const observedInspections = yield* Queue.unbounded<void>();
      const observedInspectionsBeforeRead = yield* Queue.unbounded<void>();
      const initialSequence = Math.max(
        options.deliveryBaselineSequence ?? 0,
        ...(options.historicalEvents ?? []).map((event) => event.sequence),
      );
      const sequenceRef = yield* Ref.make(initialSequence);
      const startupTurnId = TurnId.make("turn-startup-running");
      const initialThread =
        options.initialThread === undefined
          ? thread({ turnId: startupTurnId, state: "running", messages: [] })
          : options.initialThread;
      const detailRef = yield* Ref.make<Option.Option<OrchestrationThread>>(
        Option.fromNullishOr(initialThread),
      );
      const inspectionCountRef = yield* Ref.make(0);
      const inspectionGateRef = yield* Ref.make<Option.Option<Deferred.Deferred<void>>>(
        Option.none(),
      );
      const inspectionBeforeReadGateRef = yield* Ref.make<Option.Option<Deferred.Deferred<void>>>(
        Option.none(),
      );
      const initialConfig: MatrixBridgeConfigV1 = {
        version: 1,
        homeserverUrl: "https://matrix.example",
        accessToken: "secret-test-token",
        allowedUserIds: ["@adam:example"],
        roomId: "!room:example",
        pairing: {
          state: "paired",
          userId: "@adam:example",
          pairedAt: baseAt,
        },
        ownerThreadId: options.initialOwner === undefined ? threadA : options.initialOwner,
        ownershipEpoch: NonNegativeInt.make(1),
        cryptoStoreGeneration: "test-generation",
        lastDeliveredTurnId: options.lastDeliveredTurnId ?? null,
        deliveryBaselineSequence: NonNegativeInt.make(options.deliveryBaselineSequence ?? 0),
        deliveryCheckpointInitialized: options.deliveryCheckpointInitialized ?? true,
      };
      const configRef = yield* SubscriptionRef.make<Option.Option<MatrixBridgeConfigV1>>(
        Option.some(initialConfig),
      );
      const fake = yield* makeFakeMatrixBridgeClient;

      const setOwner = (ownerThreadId: ThreadId | null) =>
        Effect.gen(function* () {
          const detail = Option.getOrNull(yield* Ref.get(detailRef));
          const baselineTurn =
            ownerThreadId !== null && detail?.id === ownerThreadId ? detail.latestTurn : null;
          const baselineSequence = NonNegativeInt.make(yield* Ref.get(sequenceRef));
          return yield* SubscriptionRef.modify(configRef, (current) => {
            if (Option.isNone(current)) {
              return [status(null), current] as const;
            }
            const next: MatrixBridgeConfigV1 = {
              ...current.value,
              ownerThreadId,
              ownershipEpoch: NonNegativeInt.make(current.value.ownershipEpoch + 1),
              lastDeliveredTurnId: baselineTurn?.turnId ?? null,
              deliveryBaselineSequence: baselineSequence,
              deliveryCheckpointInitialized: true,
            };
            return [status(ownerThreadId), Option.some(next)] as const;
          });
        });

      const clearOwnerIfMatches = (expected: {
        readonly ownerThreadId: ThreadId;
        readonly ownershipEpoch: MatrixBridgeConfigV1["ownershipEpoch"];
      }) =>
        SubscriptionRef.modify(configRef, (current) => {
          if (Option.isNone(current)) return [status(null), current] as const;
          if (
            current.value.ownerThreadId !== expected.ownerThreadId ||
            current.value.ownershipEpoch !== expected.ownershipEpoch
          ) {
            return [status(current.value.ownerThreadId), current] as const;
          }
          const next: MatrixBridgeConfigV1 = {
            ...current.value,
            ownerThreadId: null,
            ownershipEpoch: NonNegativeInt.make(current.value.ownershipEpoch + 1),
            lastDeliveredTurnId: null,
            deliveryBaselineSequence: NonNegativeInt.make(0),
            deliveryCheckpointInitialized: true,
          };
          return [status(null), Option.some(next)] as const;
        });

      const initializeDeliveryCheckpointIfMissing: MatrixBridgeConfig["Service"]["initializeDeliveryCheckpointIfMissing"] =
        (expected) =>
          SubscriptionRef.modify(configRef, (current) => {
            if (Option.isNone(current)) return [false, current] as const;
            if (
              current.value.ownerThreadId !== expected.ownerThreadId ||
              current.value.ownershipEpoch !== expected.ownershipEpoch ||
              current.value.cryptoStoreGeneration !== expected.cryptoStoreGeneration ||
              current.value.roomId !== expected.roomId ||
              current.value.pairing.state !== "paired"
            ) {
              return [false, current] as const;
            }
            if (current.value.deliveryCheckpointInitialized) {
              return [true, current] as const;
            }
            return [
              true,
              Option.some({
                ...current.value,
                lastDeliveredTurnId: expected.baselineTurnId,
                deliveryBaselineSequence: expected.baselineSequence,
                deliveryCheckpointInitialized: true,
              }),
            ] as const;
          });

      const markDeliveredIfMatches: MatrixBridgeConfig["Service"]["markDeliveredIfMatches"] = (
        expected,
      ) =>
        SubscriptionRef.modify(configRef, (current) => {
          if (Option.isNone(current)) return [false, current] as const;
          if (
            current.value.ownerThreadId !== expected.ownerThreadId ||
            current.value.ownershipEpoch !== expected.ownershipEpoch ||
            current.value.cryptoStoreGeneration !== expected.cryptoStoreGeneration ||
            current.value.roomId !== expected.roomId ||
            current.value.pairing.state !== "paired"
          ) {
            return [false, current] as const;
          }
          return [
            true,
            Option.some({
              ...current.value,
              lastDeliveredTurnId: expected.turnId,
              deliveryBaselineSequence: expected.turnSequence,
              deliveryCheckpointInitialized: true,
            }),
          ] as const;
        });

      const configLayer = Layer.succeed(
        MatrixBridgeConfig,
        MatrixBridgeConfig.of({
          currentConfig: SubscriptionRef.get(configRef),
          status: SubscriptionRef.get(configRef).pipe(
            Effect.map((current) =>
              status(Option.isSome(current) ? current.value.ownerThreadId : null),
            ),
          ),
          statusChanges: SubscriptionRef.changes(configRef).pipe(
            Stream.map((current) =>
              status(Option.isSome(current) ? current.value.ownerThreadId : null),
            ),
          ),
          configure: () => unsupported(),
          disconnect: unsupported(),
          setOwner,
          clearOwnerIfMatches,
          recordRoomIfMatches: () => unsupported(),
          reportTransportStateIfMatches: () => unsupported(),
          initializeDeliveryCheckpointIfMissing,
          markDeliveredIfMatches,
          reportPermanentSendFailureIfMatches: () => Effect.succeed(true),
        }),
      );
      const engineLayer = Layer.succeed(OrchestrationEngineService, {
        readEvents: (fromSequenceExclusive) =>
          Stream.fromIterable(
            (options.historicalEvents ?? []).filter(
              (event) => event.sequence > fromSequenceExclusive,
            ),
          ),
        dispatch: () => unsupported(),
        streamDomainEvents: Stream.fromQueue(domainEvents).pipe(
          Stream.tap(() => Queue.offer(observedEvents, undefined)),
        ),
        latestSequence: Ref.get(sequenceRef),
      } satisfies OrchestrationEngineShape);
      const inspectThread = Effect.gen(function* () {
        yield* Ref.update(inspectionCountRef, (count) => count + 1);
        const beforeReadGate = yield* Ref.getAndSet(inspectionBeforeReadGateRef, Option.none());
        if (Option.isSome(beforeReadGate)) {
          yield* Queue.offer(observedInspectionsBeforeRead, undefined);
          yield* Deferred.await(beforeReadGate.value);
        }
        const detail = yield* Ref.get(detailRef);
        const snapshotSequence = NonNegativeInt.make(yield* Ref.get(sequenceRef));
        const gate = yield* Ref.getAndSet(inspectionGateRef, Option.none());
        yield* Queue.offer(observedInspections, undefined);
        if (Option.isSome(gate)) yield* Deferred.await(gate.value);
        return { detail, snapshotSequence };
      });
      const projectionLayer = Layer.mock(ProjectionSnapshotQuery)({
        getThreadDetailById: () => inspectThread.pipe(Effect.map(({ detail }) => detail)),
        getThreadDetailSnapshot: () =>
          inspectThread.pipe(
            Effect.map(({ detail, snapshotSequence }) =>
              Option.map(detail, (thread) => ({ snapshotSequence, thread })),
            ),
          ),
      });
      const environmentLayer = Layer.mock(ServerEnvironment.ServerEnvironment)({
        getEnvironmentId: Effect.succeed(EnvironmentId.make("matrix-env")),
      });
      const dependencies = Layer.mergeAll(
        fake.layer,
        configLayer,
        engineLayer,
        projectionLayer,
        environmentLayer,
      );
      const reactorLayer = MatrixBridgeReactorLive.pipe(Layer.provide(dependencies));

      return yield* Effect.gen(function* () {
        const reactor = yield* MatrixBridgeReactor;
        yield* reactor.start();
        yield* reactor.drain;
        yield* Ref.set(inspectionCountRef, 0);

        const publishWithoutDrain = (event: OrchestrationEvent) =>
          Effect.gen(function* () {
            const sequence = yield* Ref.updateAndGet(sequenceRef, (current) => current + 1);
            yield* Queue.offer(domainEvents, {
              ...event,
              sequence: NonNegativeInt.make(sequence),
            });
            yield* Queue.take(observedEvents);
            yield* Effect.yieldNow;
          });

        const publish = (event: OrchestrationEvent) =>
          publishWithoutDrain(event).pipe(Effect.andThen(reactor.drain));

        const awaitOwner = (ownerThreadId: ThreadId | null) =>
          SubscriptionRef.changes(configRef).pipe(
            Stream.filter(
              (current) => Option.isSome(current) && current.value.ownerThreadId === ownerThreadId,
            ),
            Stream.runHead,
            Effect.asVoid,
          );

        return yield* use({
          reactor,
          fake,
          publish,
          publishWithoutDrain,
          setThread: (value) => Ref.set(detailRef, Option.fromNullishOr(value)),
          setOwner,
          replaceConfig: (config) => SubscriptionRef.set(configRef, Option.some(config)),
          currentConfig: SubscriptionRef.get(configRef),
          inspectionCount: Ref.get(inspectionCountRef),
          blockNextInspection: (gate) => Ref.set(inspectionGateRef, Option.some(gate)),
          blockNextInspectionBeforeRead: (gate) =>
            Ref.set(inspectionBeforeReadGateRef, Option.some(gate)),
          awaitInspection: Queue.take(observedInspections),
          awaitInspectionBeforeRead: Queue.take(observedInspectionsBeforeRead),
          awaitCheckpointInitialized: SubscriptionRef.changes(configRef).pipe(
            Stream.filter(
              (current) => Option.isSome(current) && current.value.deliveryCheckpointInitialized,
            ),
            Stream.runHead,
            Effect.asVoid,
          ),
          awaitOwner,
        });
      }).pipe(
        Effect.provide(reactorLayer),
        Effect.provideService(ServerActivation, options.activation),
      );
    }),
  );
}

it.effect("parks startup reconciliation until server activation", () =>
  Effect.gen(function* () {
    const activation = yield* Deferred.make<void>();
    return yield* withHarness(
      (harness) =>
        Effect.gen(function* () {
          expect(yield* harness.inspectionCount).toBe(0);
          yield* Deferred.succeed(activation, undefined);
          yield* harness.awaitInspection;
          yield* harness.reactor.drain;
          expect(yield* harness.inspectionCount).toBe(1);
        }),
      { activation: Deferred.await(activation) },
    );
  }),
);

it.effect("baselines a legacy terminal turn without replaying it on upgrade", () => {
  const turnId = TurnId.make("turn-legacy-baseline");
  const final = message({
    id: "message-legacy-baseline-final",
    role: "assistant",
    text: "Historical response",
    turnId,
    at: "2026-08-19T10:00:09.000Z",
  });
  return withHarness(
    (harness) =>
      Effect.gen(function* () {
        expect(yield* harness.fake.sent).toEqual([]);
        const config = Option.getOrThrow(yield* harness.currentConfig);
        expect(config.lastDeliveredTurnId).toBe(turnId);
        expect(config.deliveryCheckpointInitialized).toBe(true);
      }),
    {
      initialThread: thread({ turnId, state: "completed", messages: [final] }),
      deliveryCheckpointInitialized: false,
    },
  );
});

it.effect("delivers a legacy running turn after it becomes terminal", () => {
  const turnId = TurnId.make("turn-legacy-running");
  return withHarness(
    (harness) =>
      Effect.gen(function* () {
        const migrated = Option.getOrThrow(yield* harness.currentConfig);
        expect(migrated.lastDeliveredTurnId).toBe(null);
        expect(migrated.deliveryCheckpointInitialized).toBe(true);

        const final = message({
          id: "message-legacy-running-final",
          role: "assistant",
          text: "Completed after migration",
          turnId,
          at: "2026-08-19T10:00:09.000Z",
        });
        yield* harness.setThread(thread({ turnId, state: "completed", messages: [final] }));
        yield* harness.publish(
          assistantEvent({
            turnId,
            messageId: "message-legacy-running-final",
            at: final.updatedAt,
          }),
        );

        expect((yield* harness.fake.sent).map((entry) => entry.content.body)).toEqual([
          "Completed after migration",
        ]);
      }),
    {
      initialThread: thread({ turnId, state: "running", messages: [] }),
      deliveryCheckpointInitialized: false,
    },
  );
});

it.effect("preserves a post-snapshot final during legacy checkpoint initialization", () =>
  Effect.gen(function* () {
    const activation = yield* Deferred.make<void>();
    const snapshotGate = yield* Deferred.make<void>();
    const snapshotTurnId = TurnId.make("turn-legacy-snapshot-running");
    const snapshotSegment = message({
      id: "message-legacy-snapshot-running",
      role: "assistant",
      text: "Historical in-flight response",
      turnId: snapshotTurnId,
      at: "2026-08-19T10:00:09.000Z",
    });
    const nextTurnId = TurnId.make("turn-during-legacy-initialization");
    return yield* withHarness(
      (harness) =>
        Effect.gen(function* () {
          yield* harness.blockNextInspection(snapshotGate);
          yield* Deferred.succeed(activation, undefined);
          yield* harness.awaitInspection;

          const nextFinal = message({
            id: "message-during-legacy-initialization",
            role: "assistant",
            text: "Captured after the migration snapshot",
            turnId: nextTurnId,
            at: "2026-08-19T10:00:20.000Z",
          });
          yield* harness.setThread(
            thread({ turnId: nextTurnId, state: "running", messages: [nextFinal] }),
          );
          yield* harness.publishWithoutDrain(
            assistantEvent({
              turnId: nextTurnId,
              messageId: "message-during-legacy-initialization",
              at: nextFinal.updatedAt,
            }),
          );

          const checkpointInitialized = yield* harness.awaitCheckpointInitialized.pipe(
            Effect.forkChild,
          );
          yield* Deferred.succeed(snapshotGate, undefined);
          yield* Fiber.join(checkpointInitialized);
          yield* harness.reactor.drain;

          expect(yield* harness.fake.sent).toEqual([]);
          yield* harness.setThread(
            thread({ turnId: nextTurnId, state: "completed", messages: [nextFinal] }),
          );
          yield* harness.publish(
            terminalMarkerEvent({
              kind: "turn-diff",
              turnId: nextTurnId,
              at: "2026-08-19T10:00:30.000Z",
            }),
          );

          expect((yield* harness.fake.sent).map((entry) => entry.content.body)).toEqual([
            "Captured after the migration snapshot",
          ]);
          expect(Option.getOrThrow(yield* harness.currentConfig).lastDeliveredTurnId).toBe(
            nextTurnId,
          );
        }),
      {
        activation: Deferred.await(activation),
        initialThread: thread({
          turnId: snapshotTurnId,
          state: "running",
          messages: [snapshotSegment],
        }),
        deliveryCheckpointInitialized: false,
        historicalEvents: [
          atSequence(
            assistantEvent({
              turnId: snapshotTurnId,
              messageId: "message-legacy-snapshot-running",
              at: snapshotSegment.updatedAt,
            }),
            1,
          ),
        ],
      },
    );
  }),
);

it.effect("reconciles one undelivered terminal owner turn on startup", () => {
  const turnId = TurnId.make("turn-restart-window");
  const final = message({
    id: "message-restart-window-final",
    role: "assistant",
    text: "Recovered after restart",
    turnId,
    at: "2026-08-19T10:00:09.000Z",
  });
  return withHarness(
    (harness) =>
      Effect.gen(function* () {
        expect((yield* harness.fake.sent).map((entry) => entry.content.body)).toEqual([
          "Recovered after restart",
        ]);
        expect(Option.getOrThrow(yield* harness.currentConfig).lastDeliveredTurnId).toBe(turnId);
      }),
    {
      initialThread: thread({ turnId, state: "completed", messages: [final] }),
      lastDeliveredTurnId: TurnId.make("turn-delivered-before-restart"),
      deliveryBaselineSequence: 1,
      historicalEvents: [
        atSequence(
          assistantEvent({
            turnId,
            messageId: "message-restart-window-final",
            at: final.updatedAt,
          }),
          2,
        ),
      ],
    },
  );
});

it.effect("retains a projected final across restart until its terminal marker", () => {
  const turnId = TurnId.make("turn-restart-before-terminal");
  const final = message({
    id: "message-restart-before-terminal-final",
    role: "assistant",
    text: "Recovered when terminal arrived",
    turnId,
    at: "2026-08-19T10:00:09.000Z",
  });
  return withHarness(
    (harness) =>
      Effect.gen(function* () {
        expect(yield* harness.fake.sent).toEqual([]);
        yield* harness.setThread(thread({ turnId, state: "completed", messages: [final] }));
        yield* harness.publish(
          terminalMarkerEvent({
            kind: "turn-diff",
            turnId,
            at: "2026-08-19T10:00:10.000Z",
          }),
        );

        expect((yield* harness.fake.sent).map((entry) => entry.content.body)).toEqual([
          "Recovered when terminal arrived",
        ]);
        expect(Option.getOrThrow(yield* harness.currentConfig).lastDeliveredTurnId).toBe(turnId);
      }),
    {
      initialThread: thread({ turnId, state: "running", messages: [final] }),
      lastDeliveredTurnId: TurnId.make("turn-before-restart-window"),
      deliveryBaselineSequence: 1,
      historicalEvents: [
        atSequence(
          assistantEvent({
            turnId,
            messageId: "message-restart-before-terminal-final",
            at: final.updatedAt,
          }),
          2,
        ),
      ],
    },
  );
});

it.effect(
  "ignores a projected turn older than the persisted delivery baseline after revert",
  () => {
    const revertedTurnId = TurnId.make("turn-before-revert");
    const deliveredTurnId = TurnId.make("turn-delivered-before-revert");
    const revertedFinal = message({
      id: "message-before-revert-final",
      role: "assistant",
      text: "Historical response after revert",
      turnId: revertedTurnId,
      at: "2026-08-19T10:00:05.000Z",
    });
    return withHarness(
      (harness) =>
        Effect.gen(function* () {
          expect(yield* harness.fake.sent).toEqual([]);
          expect(Option.getOrThrow(yield* harness.currentConfig).lastDeliveredTurnId).toBe(
            deliveredTurnId,
          );

          const nextTurnId = TurnId.make("turn-after-revert");
          const nextFinal = message({
            id: "message-after-revert-final",
            role: "assistant",
            text: "Forward-only response",
            turnId: nextTurnId,
            at: "2026-08-19T10:00:35.000Z",
          });
          yield* harness.setThread(
            thread({
              turnId: nextTurnId,
              state: "completed",
              requestedAt: "2026-08-19T10:00:30.000Z",
              messages: [nextFinal],
              terminalAt: "2026-08-19T10:00:40.000Z",
            }),
          );
          yield* harness.publish(
            assistantEvent({
              turnId: nextTurnId,
              messageId: "message-after-revert-final",
              at: nextFinal.updatedAt,
            }),
          );

          expect((yield* harness.fake.sent).map((entry) => entry.content.body)).toEqual([
            "Forward-only response",
          ]);
        }),
      {
        initialThread: thread({
          turnId: revertedTurnId,
          state: "completed",
          requestedAt: "2026-08-19T10:00:00.000Z",
          messages: [revertedFinal],
        }),
        lastDeliveredTurnId: deliveredTurnId,
        deliveryBaselineSequence: 2,
        historicalEvents: [
          atSequence(
            assistantEvent({
              turnId: revertedTurnId,
              messageId: "message-before-revert-final",
              at: revertedFinal.updatedAt,
            }),
            1,
          ),
        ],
      },
    );
  },
);

it.effect("does not redeliver the persisted terminal owner turn on startup", () => {
  const turnId = TurnId.make("turn-already-delivered");
  const final = message({
    id: "message-already-delivered-final",
    role: "assistant",
    text: "Already delivered",
    turnId,
    at: "2026-08-19T10:00:09.000Z",
  });
  return withHarness(
    (harness) =>
      Effect.gen(function* () {
        expect(yield* harness.fake.sent).toEqual([]);
        expect(Option.getOrThrow(yield* harness.currentConfig).lastDeliveredTurnId).toBe(turnId);
      }),
    {
      initialThread: thread({ turnId, state: "completed", messages: [final] }),
      lastDeliveredTurnId: turnId,
    },
  );
});

it.effect("sends only the final text after two mid-turn assistant completions and tools", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      const turnId = TurnId.make("turn-tools");
      const user = message({
        id: "message-user-tools",
        role: "user",
        text: "Do the work",
        at: baseAt,
      });
      const first = message({
        id: "message-assistant-mid-1",
        role: "assistant",
        text: "I will inspect that.",
        turnId,
        at: "2026-08-19T10:00:01.000Z",
      });
      yield* harness.setThread(thread({ turnId, state: "running", messages: [user, first] }));
      yield* harness.publish(
        assistantEvent({
          turnId,
          messageId: "message-assistant-mid-1",
          at: first.updatedAt,
        }),
      );
      yield* harness.publish(toolEvent(threadA, turnId, "2026-08-19T10:00:02.000Z"));

      const second = message({
        id: "message-assistant-mid-2",
        role: "assistant",
        text: "The tool found another step.",
        turnId,
        at: "2026-08-19T10:00:03.000Z",
      });
      yield* harness.setThread(
        thread({ turnId, state: "running", messages: [user, first, second] }),
      );
      yield* harness.publish(
        assistantEvent({
          turnId,
          messageId: "message-assistant-mid-2",
          at: second.updatedAt,
        }),
      );
      expect(yield* harness.fake.sent).toEqual([]);

      const final = message({
        id: "message-assistant-final",
        role: "assistant",
        text: "The final projected answer.",
        turnId,
        at: "2026-08-19T10:00:09.000Z",
      });
      yield* harness.setThread(
        thread({
          turnId,
          state: "completed",
          messages: [user, first, second, final],
          terminalAt: "2026-08-19T10:00:10.000Z",
        }),
      );
      yield* harness.publish(
        assistantEvent({
          turnId,
          messageId: "message-assistant-final",
          at: final.updatedAt,
          eventText: "",
        }),
      );

      const sent = yield* harness.fake.sent;
      expect(sent.map((entry) => entry.content.body)).toEqual(["The final projected answer."]);
      expect(Option.getOrThrow(yield* harness.currentConfig).lastDeliveredTurnId).toBe(turnId);
    }),
  ),
);

it.effect("delivers a live final once when a newer turn starts before inspection", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      const turnA = TurnId.make("turn-fast-follow-a");
      const finalA = message({
        id: "message-fast-follow-a-final",
        role: "assistant",
        text: "First final survives the fast follow-up",
        turnId: turnA,
        at: "2026-08-19T10:00:09.000Z",
      });
      const completedA = thread({
        turnId: turnA,
        state: "completed",
        messages: [finalA],
        terminalAt: "2026-08-19T10:00:10.000Z",
      });
      yield* harness.setThread(completedA);

      const inspectionGate = yield* Deferred.make<void>();
      yield* harness.blockNextInspectionBeforeRead(inspectionGate);
      yield* harness.publishWithoutDrain(
        assistantEvent({
          turnId: turnA,
          messageId: "message-fast-follow-a-final",
          at: finalA.updatedAt,
        }),
      );
      yield* harness.awaitInspectionBeforeRead;

      const turnB = TurnId.make("turn-fast-follow-b");
      const userB = message({
        id: "message-fast-follow-b-user",
        role: "user",
        text: "One more thing",
        turnId: turnB,
        at: "2026-08-19T10:00:20.000Z",
      });
      const runningB = thread({
        turnId: turnB,
        state: "running",
        requestedAt: "2026-08-19T10:00:20.000Z",
        messages: [finalA, userB],
      });
      yield* harness.setThread({
        ...runningB,
        turns: [completedA.latestTurn!, runningB.latestTurn!],
      });
      yield* harness.publishWithoutDrain(
        assistantEvent({
          turnId: turnB,
          messageId: "message-fast-follow-b-user",
          role: "user",
          at: userB.updatedAt,
        }),
      );

      yield* Deferred.succeed(inspectionGate, undefined);
      yield* harness.reactor.drain;
      expect((yield* harness.fake.sent).map((entry) => entry.content.body)).toEqual([
        "First final survives the fast follow-up",
      ]);

      const finalB = message({
        id: "message-fast-follow-b-final",
        role: "assistant",
        text: "Second final arrives later",
        turnId: turnB,
        at: "2026-08-19T10:00:29.000Z",
      });
      const completedB = thread({
        turnId: turnB,
        state: "completed",
        requestedAt: "2026-08-19T10:00:20.000Z",
        messages: [finalA, userB, finalB],
        terminalAt: "2026-08-19T10:00:30.000Z",
      });
      yield* harness.setThread({
        ...completedB,
        turns: [completedA.latestTurn!, completedB.latestTurn!],
      });
      yield* harness.publish(
        assistantEvent({
          turnId: turnB,
          messageId: "message-fast-follow-b-final",
          at: finalB.updatedAt,
        }),
      );
      yield* harness.publish(
        assistantEvent({
          turnId: turnA,
          messageId: "message-fast-follow-a-final-repeated",
          at: finalA.updatedAt,
        }),
      );

      expect((yield* harness.fake.sent).map((entry) => entry.content.body)).toEqual([
        "First final survives the fast follow-up",
        "Second final arrives later",
      ]);
    }),
  ),
);

it.effect("bridges completed and interrupted turns but never errored partial text", () =>
  Effect.forEach(
    [
      { state: "completed", expected: ["Visible completed response"] },
      { state: "interrupted", expected: ["Visible interrupted response"] },
      { state: "error", expected: [] },
    ] as const,
    ({ state, expected }) =>
      withHarness((harness) =>
        Effect.gen(function* () {
          const turnId = TurnId.make(`turn-terminal-state-${state}`);
          const final = message({
            id: `message-terminal-state-${state}`,
            role: "assistant",
            text: `Visible ${state} response`,
            turnId,
            at: "2026-08-19T10:00:09.000Z",
          });
          yield* harness.setThread(thread({ turnId, state, messages: [final] }));
          yield* harness.publish(
            assistantEvent({
              turnId,
              messageId: `message-terminal-state-${state}`,
              at: final.updatedAt,
            }),
          );

          expect((yield* harness.fake.sent).map((entry) => entry.content.body)).toEqual(expected);
        }),
      ),
  ),
);

it.effect("clears an errored candidate before a newer system-started turn settles", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      const erroredTurnId = TurnId.make("turn-errored-pending");
      const erroredPartial = message({
        id: "message-errored-pending",
        role: "assistant",
        text: "Partial text must stay hidden",
        turnId: erroredTurnId,
        at: "2026-08-19T10:00:09.000Z",
      });
      yield* harness.setThread(
        thread({ turnId: erroredTurnId, state: "running", messages: [erroredPartial] }),
      );
      yield* harness.publish(
        assistantEvent({
          turnId: erroredTurnId,
          messageId: "message-errored-pending",
          at: erroredPartial.updatedAt,
        }),
      );

      const erroredThread = thread({
        turnId: erroredTurnId,
        state: "error",
        messages: [erroredPartial],
      });
      yield* harness.setThread(erroredThread);
      yield* harness.publish(
        terminalMarkerEvent({
          kind: "session",
          turnId: erroredTurnId,
          sessionStatus: "error",
          at: "2026-08-19T10:00:10.000Z",
        }),
      );

      const newerTurnId = TurnId.make("turn-system-started-after-error");
      const newerThread = thread({
        turnId: newerTurnId,
        state: "completed",
        messages: [erroredPartial],
        requestedAt: "2026-08-19T10:00:20.000Z",
        terminalAt: "2026-08-19T10:00:30.000Z",
      });
      yield* harness.setThread({
        ...newerThread,
        turns: [erroredThread.latestTurn!, newerThread.latestTurn!],
      });
      yield* harness.publish(
        terminalMarkerEvent({
          kind: "session",
          turnId: newerTurnId,
          at: "2026-08-19T10:00:30.000Z",
        }),
      );

      expect(yield* harness.fake.sent).toEqual([]);
    }),
  ),
);

it.effect("coalesces repeated assistant candidates before projection inspection", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      const turnId = TurnId.make("turn-coalesced-inspection");
      const segment = message({
        id: "message-coalesced-segment",
        role: "assistant",
        text: "Still working",
        turnId,
        at: "2026-08-19T10:00:01.000Z",
      });
      yield* harness.setThread(thread({ turnId, state: "running", messages: [segment] }));
      const inspectionGate = yield* Deferred.make<void>();
      yield* harness.blockNextInspection(inspectionGate);

      yield* Effect.forEach(
        Array.from({ length: 20 }, (_, index) => index),
        (index) =>
          harness.publishWithoutDrain(
            assistantEvent({
              turnId,
              messageId: `message-coalesced-${index}`,
              at: segment.updatedAt,
            }),
          ),
        { discard: true },
      );
      expect(yield* harness.inspectionCount).toBe(1);
      yield* Deferred.succeed(inspectionGate, undefined);
      yield* harness.reactor.drain;

      expect(yield* harness.inspectionCount).toBe(1);
      expect(yield* harness.fake.sent).toEqual([]);
    }),
  ),
);

it.effect("runs one trailing inspection when a terminal marker overlaps an active read", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      const turnId = TurnId.make("turn-terminal-overlap");
      const final = message({
        id: "message-terminal-overlap-final",
        role: "assistant",
        text: "Delivered by trailing inspection",
        turnId,
        at: "2026-08-19T10:00:09.000Z",
      });
      yield* harness.setThread(thread({ turnId, state: "running", messages: [final] }));
      const inspectionGate = yield* Deferred.make<void>();
      yield* harness.blockNextInspection(inspectionGate);
      yield* harness.publishWithoutDrain(
        assistantEvent({
          turnId,
          messageId: "message-terminal-overlap-final",
          at: final.updatedAt,
        }),
      );

      yield* harness.setThread(thread({ turnId, state: "completed", messages: [final] }));
      yield* harness.publishWithoutDrain(
        terminalMarkerEvent({
          kind: "turn-diff",
          turnId,
          at: "2026-08-19T10:00:10.000Z",
        }),
      );
      expect(yield* harness.inspectionCount).toBe(1);

      yield* Deferred.succeed(inspectionGate, undefined);
      yield* harness.reactor.drain;

      expect(yield* harness.inspectionCount).toBe(2);
      expect((yield* harness.fake.sent).map((entry) => entry.content.body)).toEqual([
        "Delivered by trailing inspection",
      ]);
    }),
  ),
);

it.effect("re-evaluates a pending final once when each terminal marker arrives", () =>
  Effect.forEach(
    [
      { kind: "session", sessionStatus: "ready" },
      { kind: "session", sessionStatus: "idle" },
      { kind: "session", sessionStatus: "interrupted" },
      { kind: "session", sessionStatus: "stopped" },
      { kind: "session", sessionStatus: "error" },
      { kind: "turn-diff" },
    ] as const,
    (marker) =>
      withHarness((harness) =>
        Effect.gen(function* () {
          const markerName = marker.kind === "session" ? marker.sessionStatus : marker.kind;
          const turnId = TurnId.make(`turn-terminal-race-${markerName}`);
          const final = message({
            id: `message-terminal-race-${markerName}`,
            role: "assistant",
            text: `Final after ${markerName} terminal marker`,
            turnId,
            at: "2026-08-19T10:00:09.000Z",
          });
          yield* harness.setThread(
            thread({
              turnId,
              state: "running",
              messages: [final],
            }),
          );

          const assistantFinalEvent = assistantEvent({
            turnId,
            messageId: `message-terminal-race-${markerName}`,
            at: final.updatedAt,
          });
          yield* harness.publish(assistantFinalEvent);
          expect(yield* harness.fake.sent).toEqual([]);

          yield* harness.setThread(
            thread({
              turnId,
              state: "completed",
              messages: [final],
              terminalAt: "2026-08-19T10:00:10.000Z",
            }),
          );
          const terminalEvent = terminalMarkerEvent({
            kind: marker.kind,
            turnId,
            at: "2026-08-19T10:00:10.000Z",
            ...(marker.kind === "session" ? { sessionStatus: marker.sessionStatus } : {}),
          });
          yield* harness.publish(terminalEvent);
          yield* harness.publish(assistantFinalEvent);
          yield* harness.publish(terminalEvent);

          expect((yield* harness.fake.sent).map((entry) => entry.content.body)).toEqual([
            `Final after ${markerName} terminal marker`,
          ]);
        }),
      ),
  ),
);

it.effect("ignores stale startup history plus non-final live events", () => {
  const staleTurnId = TurnId.make("turn-stale-startup-history");
  const staleFinal = message({
    id: "message-stale-startup-final",
    role: "assistant",
    text: "Superseded startup response",
    turnId: staleTurnId,
    at: "2026-08-19T10:00:09.000Z",
  });
  const newerUser = message({
    id: "message-newer-user",
    role: "user",
    text: "Start the next turn",
    at: "2026-08-19T10:00:20.000Z",
  });
  return withHarness(
    (harness) =>
      Effect.gen(function* () {
        expect(yield* harness.fake.sent).toEqual([]);

        const liveTurnId = TurnId.make("turn-negative-live-events");
        const segment = message({
          id: "message-approval-opening",
          role: "assistant",
          text: "Please approve this command.",
          turnId: liveTurnId,
          at: "2026-08-19T10:00:21.000Z",
        });
        yield* harness.setThread(
          thread({
            turnId: liveTurnId,
            state: "running",
            requestedAt: "2026-08-19T10:00:20.000Z",
            messages: [staleFinal, newerUser, segment],
            sessionStatus: "waiting",
          }),
        );
        yield* harness.publish(
          assistantEvent({
            turnId: liveTurnId,
            messageId: "message-approval-opening",
            at: segment.updatedAt,
          }),
        );
        yield* harness.publish(
          assistantEvent({
            turnId: liveTurnId,
            messageId: "message-streaming",
            streaming: true,
            at: "2026-08-19T10:00:22.000Z",
          }),
        );
        yield* harness.publish(
          assistantEvent({
            turnId: null,
            messageId: "message-user",
            role: "user",
            at: "2026-08-19T10:00:23.000Z",
          }),
        );
        yield* harness.publish(toolEvent(threadA, liveTurnId, "2026-08-19T10:00:24.000Z"));

        expect(yield* harness.fake.sent).toEqual([]);
      }),
    {
      initialThread: thread({
        turnId: staleTurnId,
        state: "completed",
        messages: [staleFinal, newerUser],
        terminalAt: "2026-08-19T10:00:10.000Z",
      }),
      historicalEvents: [
        atSequence(
          assistantEvent({
            turnId: staleTurnId,
            messageId: "message-stale-startup-final",
            at: staleFinal.updatedAt,
          }),
          1,
        ),
      ],
    },
  );
});

it.effect("uses projected text and dedupes repeated terminal events after the terminal check", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      const turnId = TurnId.make("turn-dedupe");
      const final = message({
        id: "message-projected-final",
        role: "assistant",
        text: "Projected body, not event text.",
        turnId,
        at: "2026-08-19T10:00:09.000Z",
      });
      yield* harness.setThread(
        thread({
          turnId,
          state: "completed",
          messages: [final],
          terminalAt: "2026-08-19T10:00:10.000Z",
        }),
      );
      const terminalEvent = assistantEvent({
        turnId,
        messageId: "message-projected-final",
        at: final.updatedAt,
        eventText: "",
      });
      yield* harness.publish(terminalEvent);
      yield* harness.publish(terminalEvent);

      const sent = yield* harness.fake.sent;
      expect(sent).toHaveLength(1);
      expect(sent[0]?.content).toEqual({
        msgtype: "m.text",
        body: "Projected body, not event text.",
      });
    }),
  ),
);

it.effect("delivers later-sequence turns even when their timestamps regress", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      const firstTurn = TurnId.make("turn-one");
      const firstFinal = message({
        id: "message-final-one",
        role: "assistant",
        text: "First final",
        turnId: firstTurn,
        at: "2026-08-19T10:00:05.000Z",
      });
      yield* harness.setThread(
        thread({
          turnId: firstTurn,
          state: "completed",
          messages: [firstFinal],
          terminalAt: "2026-08-19T10:00:06.000Z",
        }),
      );
      yield* harness.publish(
        assistantEvent({
          turnId: firstTurn,
          messageId: "message-final-one",
          at: firstFinal.updatedAt,
        }),
      );

      const secondTurn = TurnId.make("turn-two");
      const secondUser = message({
        id: "message-user-two",
        role: "user",
        text: "Next",
        at: "2026-08-19T10:00:07.000Z",
      });
      const secondFinal = message({
        id: "message-final-two",
        role: "assistant",
        text: "Second final",
        turnId: secondTurn,
        at: "2026-08-19T10:00:08.000Z",
      });
      yield* harness.setThread(
        thread({
          turnId: secondTurn,
          state: "completed",
          requestedAt: "2026-08-18T10:00:00.000Z",
          messages: [firstFinal, secondUser, secondFinal],
          terminalAt: "2026-08-19T10:00:09.000Z",
        }),
      );
      yield* harness.publish(
        assistantEvent({
          turnId: secondTurn,
          messageId: "message-final-two",
          at: secondFinal.updatedAt,
        }),
      );

      expect((yield* harness.fake.sent).map((entry) => entry.content.body)).toEqual([
        "First final",
        "Second final",
      ]);
    }),
  ),
);

it.effect("drops an old owner mid-turn and clears archived, deleted, and unset owners", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      const turnA = TurnId.make("turn-owner-a");
      const aSegment = message({
        id: "message-owner-a-segment",
        role: "assistant",
        text: "A is still working",
        turnId: turnA,
        at: "2026-08-19T10:00:01.000Z",
      });
      yield* harness.setThread(
        thread({ threadId: threadA, turnId: turnA, state: "running", messages: [aSegment] }),
      );
      yield* harness.publish(
        assistantEvent({
          threadId: threadA,
          turnId: turnA,
          messageId: "message-owner-a-segment",
          at: aSegment.updatedAt,
        }),
      );

      yield* harness.setOwner(threadB);
      const aFinal = message({
        id: "message-owner-a-final",
        role: "assistant",
        text: "A final must be dropped",
        turnId: turnA,
        at: "2026-08-19T10:00:05.000Z",
      });
      yield* harness.setThread(
        thread({
          threadId: threadA,
          turnId: turnA,
          state: "completed",
          messages: [aSegment, aFinal],
        }),
      );
      yield* harness.publish(
        assistantEvent({
          threadId: threadA,
          turnId: turnA,
          messageId: "message-owner-a-final",
          at: aFinal.updatedAt,
        }),
      );

      const turnB = TurnId.make("turn-owner-b");
      const bFinal = message({
        id: "message-owner-b-final",
        role: "assistant",
        text: "B final is delivered",
        turnId: turnB,
        at: "2026-08-19T10:00:06.000Z",
      });
      yield* harness.setThread(
        thread({
          threadId: threadB,
          turnId: turnB,
          state: "completed",
          messages: [bFinal],
        }),
      );
      yield* harness.publish(
        assistantEvent({
          threadId: threadB,
          turnId: turnB,
          messageId: "message-owner-b-final",
          at: bFinal.updatedAt,
        }),
      );
      expect((yield* harness.fake.sent).map((entry) => entry.content.body)).toEqual([
        "B final is delivered",
      ]);

      yield* harness.setOwner(null);
      const silentTurn = TurnId.make("turn-unset");
      const silentFinal = message({
        id: "message-unset-final",
        role: "assistant",
        text: "Unbridged final",
        turnId: silentTurn,
        at: "2026-08-19T10:00:07.000Z",
      });
      yield* harness.setThread(
        thread({
          threadId: threadB,
          turnId: silentTurn,
          state: "completed",
          messages: [silentFinal],
        }),
      );
      yield* harness.publish(
        assistantEvent({
          threadId: threadB,
          turnId: silentTurn,
          messageId: "message-unset-final",
          at: silentFinal.updatedAt,
        }),
      );

      yield* harness.setOwner(threadB);
      yield* harness.setThread(
        thread({
          threadId: threadB,
          turnId: silentTurn,
          state: "completed",
          messages: [silentFinal],
          archivedAt: "2026-08-19T10:00:08.000Z",
        }),
      );
      yield* harness.publishWithoutDrain(
        lifecycleEvent("thread.archived", threadB, "2026-08-19T10:00:08.000Z"),
      );
      yield* harness.awaitOwner(null);

      yield* harness.setThread(
        thread({
          threadId: threadB,
          turnId: silentTurn,
          state: "completed",
          messages: [silentFinal],
        }),
      );
      yield* harness.setOwner(threadB);
      yield* harness.setThread(
        thread({
          threadId: threadB,
          turnId: silentTurn,
          state: "completed",
          messages: [silentFinal],
          deletedAt: "2026-08-19T10:00:09.000Z",
        }),
      );
      yield* harness.publishWithoutDrain(
        lifecycleEvent("thread.deleted", threadB, "2026-08-19T10:00:09.000Z"),
      );
      yield* harness.awaitOwner(null);

      expect((yield* harness.fake.sent).map((entry) => entry.content.body)).toEqual([
        "B final is delivered",
      ]);
    }),
  ),
);

it.effect("keeps new ownership when a stale archive event finishes after unarchive", () =>
  Effect.gen(function* () {
    const turnId = TurnId.make("turn-stale-archive-owner");
    const final = message({
      id: "message-stale-archive-owner",
      role: "assistant",
      text: "Existing owner response",
      turnId,
      at: "2026-08-19T10:00:09.000Z",
    });
    const activeThread = thread({
      threadId: threadB,
      turnId,
      state: "completed",
      messages: [final],
    });
    const archivedThread = thread({
      threadId: threadB,
      turnId,
      state: "completed",
      messages: [final],
      archivedAt: "2026-08-19T10:00:20.000Z",
    });

    return yield* withHarness(
      (harness) =>
        Effect.gen(function* () {
          const inspectionGate = yield* Deferred.make<void>();
          yield* harness.setThread(archivedThread);
          yield* harness.blockNextInspection(inspectionGate);
          yield* harness.publishWithoutDrain(
            lifecycleEvent("thread.archived", threadB, "2026-08-19T10:00:20.000Z"),
          );
          yield* harness.awaitInspection;

          yield* harness.setThread(activeThread);
          yield* harness.setOwner(threadB);
          yield* Deferred.succeed(inspectionGate, undefined);
          yield* harness.reactor.drain;

          expect(Option.getOrThrow(yield* harness.currentConfig).ownerThreadId).toBe(threadB);
        }),
      {
        initialOwner: threadB,
        initialThread: activeThread,
        lastDeliveredTurnId: turnId,
        deliveryBaselineSequence: 1,
      },
    );
  }),
);

it.effect("drops permanent transport failures after one attempt", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      const turnId = TurnId.make("turn-permanent-send-failure");
      const final = message({
        id: "message-permanent-send-failure",
        role: "assistant",
        text: "Do not retry permanently",
        turnId,
        at: "2026-08-19T10:00:09.000Z",
      });
      yield* harness.setThread(thread({ turnId, state: "completed", messages: [final] }));
      yield* harness.fake.failNextSends(1, "permanent");
      yield* harness.publishWithoutDrain(
        assistantEvent({
          turnId,
          messageId: "message-permanent-send-failure",
          at: final.updatedAt,
        }),
      );
      const drainFiber = yield* harness.reactor.drain.pipe(Effect.forkChild);
      yield* harness.fake.awaitAttemptCount(1);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("1 second");
      yield* Fiber.join(drainFiber);

      expect(yield* harness.fake.attempts).toHaveLength(1);
      expect(yield* harness.fake.sent).toEqual([]);
    }),
  ),
);

it.effect("retries with one transaction id and drops when the owner epoch changes", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      const turnId = TurnId.make("turn-retry");
      const final = message({
        id: "message-retry-final",
        role: "assistant",
        text: "Retry me",
        turnId,
        at: "2026-08-19T10:00:09.000Z",
      });
      yield* harness.setThread(thread({ turnId, state: "completed", messages: [final] }));
      yield* harness.fake.failNextSends(2);
      yield* harness.publishWithoutDrain(
        assistantEvent({
          turnId,
          messageId: "message-retry-final",
          at: final.updatedAt,
        }),
      );
      const drainFiber = yield* harness.reactor.drain.pipe(Effect.forkChild);
      yield* harness.fake.awaitAttemptCount(1);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("1 second");
      yield* harness.fake.awaitAttemptCount(2);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("2 seconds");
      yield* Fiber.join(drainFiber);

      const attempts = yield* harness.fake.attempts;
      expect(attempts).toHaveLength(3);
      expect(new Set(attempts.map((entry) => entry.transactionId)).size).toBe(1);
      expect((yield* harness.fake.sent).map((entry) => entry.content.body)).toEqual(["Retry me"]);
    }),
  ).pipe(
    Effect.andThen(
      withHarness((harness) =>
        Effect.gen(function* () {
          const turnId = TurnId.make("turn-epoch-drop");
          const final = message({
            id: "message-epoch-final",
            role: "assistant",
            text: "Old epoch",
            turnId,
            at: "2026-08-19T10:00:09.000Z",
          });
          yield* harness.setThread(thread({ turnId, state: "completed", messages: [final] }));
          yield* harness.fake.failNextSends(1);
          yield* harness.publishWithoutDrain(
            assistantEvent({
              turnId,
              messageId: "message-epoch-final",
              at: final.updatedAt,
            }),
          );
          const drainFiber = yield* harness.reactor.drain.pipe(Effect.forkChild);
          yield* harness.fake.awaitAttemptCount(1);
          yield* Effect.yieldNow;
          yield* harness.setOwner(threadB);
          yield* TestClock.adjust("1 second");
          yield* Fiber.join(drainFiber);

          expect(yield* harness.fake.sent).toEqual([]);
          expect(yield* harness.fake.attempts).toHaveLength(1);
        }),
      ),
    ),
  ),
);

it.effect("drops a sleeping retry after the same owner epoch is reconfigured", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      const turnId = TurnId.make("turn-stale-room-retry");
      const final = message({
        id: "message-stale-room-final",
        role: "assistant",
        text: "Never send this to the old room",
        turnId,
        at: "2026-08-19T10:00:09.000Z",
      });
      yield* harness.setThread(thread({ turnId, state: "completed", messages: [final] }));
      yield* harness.fake.failNextSends(1);
      yield* harness.publishWithoutDrain(
        assistantEvent({
          turnId,
          messageId: "message-stale-room-final",
          at: final.updatedAt,
        }),
      );
      const drainFiber = yield* harness.reactor.drain.pipe(Effect.forkChild);
      yield* harness.fake.awaitAttemptCount(1);
      yield* Effect.yieldNow;

      const previous = Option.getOrThrow(yield* harness.currentConfig);
      yield* harness.replaceConfig({
        ...previous,
        roomId: "!replacement:example",
        ownerThreadId: threadA,
        ownershipEpoch: previous.ownershipEpoch,
        cryptoStoreGeneration: "replacement-generation",
        lastDeliveredTurnId: null,
      });
      yield* TestClock.adjust("1 second");
      yield* Fiber.join(drainFiber);

      expect(yield* harness.fake.attempts).toHaveLength(1);
      expect(yield* harness.fake.sent).toEqual([]);
    }),
  ),
);

it.effect(
  "expires retries at ten minutes and rejects the newest final when all 64 slots are full",
  () =>
    withHarness((harness) =>
      Effect.gen(function* () {
        const turnId = TurnId.make("turn-expiry");
        const final = message({
          id: "message-expiry-final",
          role: "assistant",
          text: "Expire me",
          turnId,
          at: "2026-08-19T10:00:09.000Z",
        });
        yield* harness.setThread(thread({ turnId, state: "completed", messages: [final] }));
        yield* harness.fake.failNextSends(100);
        yield* harness.publishWithoutDrain(
          assistantEvent({
            turnId,
            messageId: "message-expiry-final",
            at: final.updatedAt,
          }),
        );
        const drainFiber = yield* harness.reactor.drain.pipe(Effect.forkChild);
        yield* harness.fake.awaitAttemptCount(1);
        yield* Effect.yieldNow;
        yield* TestClock.adjust(MATRIX_BRIDGE_RETRY_WINDOW_MS);
        yield* Fiber.join(drainFiber);
        expect(yield* harness.fake.sent).toEqual([]);
      }),
    ).pipe(
      Effect.andThen(
        Effect.scoped(
          Effect.gen(function* () {
            const firstStarted = yield* Deferred.make<void>();
            const releaseFirst = yield* Deferred.make<void>();
            const processedRef = yield* Ref.make<ReadonlyArray<number>>([]);
            const worker = yield* makeBoundedDrainableWorker(
              MATRIX_BRIDGE_OUTBOUND_CAPACITY,
              (item: number) =>
                Effect.gen(function* () {
                  if (item === 0) {
                    yield* Deferred.succeed(firstStarted, undefined);
                    yield* Deferred.await(releaseFirst);
                  }
                  yield* Ref.update(processedRef, (processed) => [...processed, item]);
                }),
            );

            expect(yield* worker.enqueue(0)).toBe(true);
            yield* Deferred.await(firstStarted);
            for (let item = 1; item <= MATRIX_BRIDGE_OUTBOUND_CAPACITY; item += 1) {
              expect(yield* worker.enqueue(item)).toBe(true);
            }
            expect(yield* worker.enqueue(MATRIX_BRIDGE_OUTBOUND_CAPACITY + 1)).toBe(false);

            yield* Deferred.succeed(releaseFirst, undefined);
            yield* worker.drain;
            expect(yield* Ref.get(processedRef)).toHaveLength(MATRIX_BRIDGE_OUTBOUND_CAPACITY + 1);
          }),
        ),
      ),
    ),
);
