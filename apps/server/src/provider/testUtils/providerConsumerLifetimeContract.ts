import { assert } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";

import type {
  ProviderRuntimeEvent,
  ProviderSendTurnInput,
  ProviderSessionStartInput,
  ProviderTurnStartResult,
} from "@t3tools/contracts";

import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";

type TurnCompletedEvent = Extract<ProviderRuntimeEvent, { type: "turn.completed" }>;
type SessionExitedEvent = Extract<ProviderRuntimeEvent, { type: "session.exited" }>;

export interface ProviderConsumerLifetimeContractHarness<TError> {
  readonly adapterName: string;
  readonly adapter: Pick<
    ProviderAdapterShape<TError>,
    "hasSession" | "sendTurn" | "startSession" | "stopSession" | "streamEvents"
  >;
  readonly startInput: ProviderSessionStartInput;
  readonly turnInput: ProviderSendTurnInput;
  readonly driveTurn?: (turn: ProviderTurnStartResult) => Effect.Effect<void>;
}

/**
 * Verifies that a provider's persistent event consumer belongs to the session,
 * not to the short-lived fiber that starts it.
 */
export const providerConsumerLifetimeContract = Effect.fn("providerConsumerLifetimeContract")(
  function* <TError>(harness: ProviderConsumerLifetimeContractHarness<TError>) {
    const { adapter, adapterName, driveTurn, startInput, turnInput } = harness;
    const threadId = startInput.threadId;
    const runtimeEvents: Array<ProviderRuntimeEvent> = [];
    const turnCompleted = yield* Deferred.make<TurnCompletedEvent>();
    const sessionExited = yield* Deferred.make<SessionExitedEvent>();

    const runtimeEventsFiber = yield* adapter.streamEvents.pipe(
      Stream.filter((event) => String(event.threadId) === String(threadId)),
      Stream.runForEach((event) =>
        Effect.gen(function* () {
          yield* Effect.sync(() => runtimeEvents.push(event));
          if (event.type === "turn.completed") {
            yield* Deferred.succeed(turnCompleted, event).pipe(Effect.asVoid);
          }
          if (event.type === "session.exited") {
            yield* Deferred.succeed(sessionExited, event).pipe(Effect.asVoid);
          }
        }),
      ),
      Effect.forkChild,
    );

    return yield* Effect.gen(function* () {
      const startFiber = yield* adapter.startSession(startInput).pipe(Effect.forkChild);
      yield* Fiber.join(startFiber);

      const turn = yield* adapter.sendTurn(turnInput).pipe(Effect.timeout("2 seconds"));
      if (driveTurn !== undefined) {
        yield* driveTurn(turn);
      }
      const completed = yield* Deferred.await(turnCompleted).pipe(Effect.timeout("2 seconds"));

      const turnStartedIndex = runtimeEvents.findIndex((event) => event.type === "turn.started");
      const contentDeltaIndex = runtimeEvents.findIndex((event) => event.type === "content.delta");
      const turnCompletedIndex = runtimeEvents.indexOf(completed);

      assert.isAtLeast(turnStartedIndex, 0, `${adapterName} did not stream turn.started`);
      assert.isAbove(
        contentDeltaIndex,
        turnStartedIndex,
        `${adapterName} did not stream content after turn.started`,
      );
      assert.isAbove(
        turnCompletedIndex,
        contentDeltaIndex,
        `${adapterName} reached its terminal event before streaming content`,
      );
      assert.equal(completed.payload.state, "completed");

      yield* adapter.stopSession(threadId).pipe(Effect.timeout("2 seconds"));
      // Adapters emit session.exited only after interrupting their stored
      // consumer. Observing it therefore proves stop did not leave that fiber
      // running, without exposing test-only internals from product code.
      yield* Deferred.await(sessionExited).pipe(Effect.timeout("2 seconds"));
      assert.isFalse(yield* adapter.hasSession(threadId));
    }).pipe(
      Effect.ensuring(
        adapter.stopSession(threadId).pipe(Effect.timeout("2 seconds"), Effect.ignore),
      ),
      Effect.ensuring(Fiber.interrupt(runtimeEventsFiber).pipe(Effect.asVoid)),
    );
  },
);
