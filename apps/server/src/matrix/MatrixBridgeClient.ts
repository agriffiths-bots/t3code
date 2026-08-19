import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

export interface MatrixBridgeOutboundText {
  readonly roomId: string;
  readonly transactionId: string;
  readonly content: {
    readonly msgtype: "m.text";
    readonly body: string;
  };
}

/** Decrypted timeline shape that the production adapter will deliver in PR2b. */
export interface MatrixBridgeInboundText {
  readonly eventId: string;
  readonly roomId: string;
  readonly sender: string;
  readonly body: string;
  readonly isEdit: boolean;
}

export type MatrixBridgeInboundHandler = (
  event: MatrixBridgeInboundText,
) => Effect.Effect<void, never>;

export class MatrixBridgeClientError extends Schema.TaggedErrorClass<MatrixBridgeClientError>()(
  "MatrixBridgeClientError",
  {
    operation: Schema.Literals(["listen", "send"]),
    reason: Schema.String,
  },
) {}

export class MatrixBridgeClient extends Context.Service<
  MatrixBridgeClient,
  {
    readonly listen: (
      onInboundText: MatrixBridgeInboundHandler,
    ) => Effect.Effect<never, MatrixBridgeClientError, Scope.Scope>;
    readonly sendText: (
      message: MatrixBridgeOutboundText,
    ) => Effect.Effect<void, MatrixBridgeClientError>;
  }
>()("t3/matrix/MatrixBridgeClient") {}

/**
 * Production placeholder until the encrypted SDK adapter lands in PR2.
 * It is intentionally not a fake: it never accepts traffic or records data.
 */
export const unavailableLayer = Layer.succeed(MatrixBridgeClient, {
  listen: () => Effect.never,
  sendText: () =>
    Effect.fail(
      new MatrixBridgeClientError({
        operation: "send",
        reason: "Encrypted Matrix transport is unavailable.",
      }),
    ),
});

export interface FakeMatrixBridgeClient {
  readonly layer: Layer.Layer<MatrixBridgeClient>;
  readonly attempts: Effect.Effect<ReadonlyArray<MatrixBridgeOutboundText>>;
  readonly awaitAttemptCount: (
    count: number,
  ) => Effect.Effect<ReadonlyArray<MatrixBridgeOutboundText>>;
  readonly sent: Effect.Effect<ReadonlyArray<MatrixBridgeOutboundText>>;
  readonly isListening: Effect.Effect<boolean>;
  readonly failNextSends: (count: number) => Effect.Effect<void>;
  readonly emitInbound: (event: MatrixBridgeInboundText) => Effect.Effect<boolean>;
  readonly awaitSentCount: (
    count: number,
  ) => Effect.Effect<ReadonlyArray<MatrixBridgeOutboundText>>;
}

/** In-memory Matrix transport used only by focused tests. */
export const makeFakeMatrixBridgeClient = Effect.gen(function* () {
  const attemptsRef = yield* SubscriptionRef.make<ReadonlyArray<MatrixBridgeOutboundText>>([]);
  const sentRef = yield* SubscriptionRef.make<ReadonlyArray<MatrixBridgeOutboundText>>([]);
  const failuresRemainingRef = yield* Ref.make(0);
  const inboundHandlerRef = yield* Ref.make<Option.Option<MatrixBridgeInboundHandler>>(
    Option.none(),
  );

  const listen: MatrixBridgeClient["Service"]["listen"] = (onInboundText) =>
    Effect.acquireRelease(Ref.set(inboundHandlerRef, Option.some(onInboundText)), () =>
      Ref.set(inboundHandlerRef, Option.none()),
    ).pipe(Effect.andThen(Effect.never));

  const sendText: MatrixBridgeClient["Service"]["sendText"] = Effect.fn(
    "FakeMatrixBridgeClient.sendText",
  )(function* (message) {
    yield* SubscriptionRef.update(attemptsRef, (attempts) => [...attempts, message]);
    const shouldFail = yield* Ref.modify(
      failuresRemainingRef,
      (remaining) => [remaining > 0, Math.max(0, remaining - 1)] as const,
    );
    if (shouldFail) {
      return yield* new MatrixBridgeClientError({
        operation: "send",
        reason: "Injected fake Matrix send failure.",
      });
    }
    yield* SubscriptionRef.update(sentRef, (sent) => [...sent, message]);
  });
  const awaitCount = (
    ref: SubscriptionRef.SubscriptionRef<ReadonlyArray<MatrixBridgeOutboundText>>,
    count: number,
  ) =>
    SubscriptionRef.changes(ref).pipe(
      Stream.filter((messages) => messages.length >= count),
      Stream.runHead,
      Effect.map(Option.getOrThrow),
    );

  const awaitSentCount = (count: number) =>
    SubscriptionRef.changes(sentRef).pipe(
      Stream.filter((sent) => sent.length >= count),
      Stream.runHead,
      Effect.map(Option.getOrThrow),
    );

  return {
    layer: Layer.succeed(MatrixBridgeClient, MatrixBridgeClient.of({ listen, sendText })),
    attempts: SubscriptionRef.get(attemptsRef),
    awaitAttemptCount: (count) => awaitCount(attemptsRef, count),
    sent: SubscriptionRef.get(sentRef),
    isListening: Ref.get(inboundHandlerRef).pipe(Effect.map(Option.isSome)),
    failNextSends: (count) => Ref.set(failuresRemainingRef, Math.max(0, count)),
    emitInbound: (event) =>
      Ref.get(inboundHandlerRef).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.succeed(false),
            onSome: (handler) => handler(event).pipe(Effect.as(true)),
          }),
        ),
      ),
    awaitSentCount,
  } satisfies FakeMatrixBridgeClient;
});
