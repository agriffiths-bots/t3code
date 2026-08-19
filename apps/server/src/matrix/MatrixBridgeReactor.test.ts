import {
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
    requestedAt: baseAt,
    startedAt: baseAt,
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
    turns: [latestTurn],
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
  readonly currentConfig: Effect.Effect<Option.Option<MatrixBridgeConfigV1>>;
  readonly awaitOwner: (ownerThreadId: ThreadId | null) => Effect.Effect<void>;
}

function withHarness<A, E>(
  use: (harness: ReactorHarness) => Effect.Effect<A, E>,
  initialOwner: ThreadId = threadA,
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const domainEvents = yield* Queue.unbounded<OrchestrationEvent>();
      const observedEvents = yield* Queue.unbounded<void>();
      const detailRef = yield* Ref.make<Option.Option<OrchestrationThread>>(Option.none());
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
        ownerThreadId: initialOwner,
        ownershipEpoch: NonNegativeInt.make(1),
        cryptoStoreGeneration: "test-generation",
      };
      const configRef = yield* SubscriptionRef.make<Option.Option<MatrixBridgeConfigV1>>(
        Option.some(initialConfig),
      );
      const fake = yield* makeFakeMatrixBridgeClient;

      const setOwner = (ownerThreadId: ThreadId | null) =>
        SubscriptionRef.modify(configRef, (current) => {
          if (Option.isNone(current)) {
            return [status(null), current] as const;
          }
          const next: MatrixBridgeConfigV1 = {
            ...current.value,
            ownerThreadId,
            ownershipEpoch: NonNegativeInt.make(current.value.ownershipEpoch + 1),
          };
          return [status(ownerThreadId), Option.some(next)] as const;
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
          };
          return [status(null), Option.some(next)] as const;
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
          statusChanges: Stream.empty,
          configure: () => unsupported(),
          disconnect: unsupported(),
          setOwner,
          clearOwnerIfMatches,
        }),
      );
      const engineLayer = Layer.succeed(OrchestrationEngineService, {
        readEvents: () => Stream.empty,
        dispatch: () => unsupported(),
        streamDomainEvents: Stream.fromQueue(domainEvents).pipe(
          Stream.tap(() => Queue.offer(observedEvents, undefined)),
        ),
        latestSequence: Effect.succeed(0),
      } satisfies OrchestrationEngineShape);
      const projectionLayer = Layer.mock(ProjectionSnapshotQuery)({
        getThreadDetailById: () => Ref.get(detailRef),
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

        const publishWithoutDrain = (event: OrchestrationEvent) =>
          Queue.offer(domainEvents, event).pipe(
            Effect.andThen(Queue.take(observedEvents)),
            Effect.andThen(Effect.yieldNow),
            Effect.asVoid,
          );

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
          currentConfig: SubscriptionRef.get(configRef),
          awaitOwner,
        });
      }).pipe(Effect.provide(reactorLayer));
    }),
  );
}

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
    }),
  ),
);

it.effect("ignores approval-opening, stale-terminal, chunk, user, and tool events", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      const turnId = TurnId.make("turn-negative");
      const segment = message({
        id: "message-approval-opening",
        role: "assistant",
        text: "Please approve this command.",
        turnId,
        at: "2026-08-19T10:00:01.000Z",
      });
      yield* harness.setThread(
        thread({
          turnId,
          state: "running",
          messages: [segment],
          sessionStatus: "waiting",
        }),
      );
      yield* harness.publish(
        assistantEvent({
          turnId,
          messageId: "message-approval-opening",
          at: segment.updatedAt,
        }),
      );
      yield* harness.publish(
        assistantEvent({
          turnId,
          messageId: "message-streaming",
          streaming: true,
          at: "2026-08-19T10:00:02.000Z",
        }),
      );
      yield* harness.publish(
        assistantEvent({
          turnId: null,
          messageId: "message-user",
          role: "user",
          at: "2026-08-19T10:00:03.000Z",
        }),
      );
      yield* harness.publish(toolEvent(threadA, turnId, "2026-08-19T10:00:04.000Z"));

      const staleUser = message({
        id: "message-newer-user",
        role: "user",
        text: "Start the next turn",
        at: "2026-08-19T10:00:20.000Z",
      });
      yield* harness.setThread(
        thread({
          turnId,
          state: "completed",
          messages: [segment, staleUser],
          terminalAt: "2026-08-19T10:00:10.000Z",
        }),
      );
      yield* harness.publish(
        assistantEvent({
          turnId,
          messageId: "message-approval-opening",
          at: segment.updatedAt,
        }),
      );

      expect(yield* harness.fake.sent).toEqual([]);
    }),
  ),
);

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

it.effect("sends one projected completion for each separate turn", () =>
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
      yield* harness.publishWithoutDrain(
        lifecycleEvent("thread.archived", threadB, "2026-08-19T10:00:08.000Z"),
      );
      yield* harness.awaitOwner(null);

      yield* harness.setOwner(threadB);
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
