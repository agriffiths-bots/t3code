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
    retryability: Schema.Literals(["transient", "permanent"]),
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

export interface FakeMatrixBridgeClient {
  readonly layer: Layer.Layer<MatrixBridgeClient>;
  readonly attempts: Effect.Effect<ReadonlyArray<MatrixBridgeOutboundText>>;
  readonly awaitAttemptCount: (
    count: number,
  ) => Effect.Effect<ReadonlyArray<MatrixBridgeOutboundText>>;
  readonly sent: Effect.Effect<ReadonlyArray<MatrixBridgeOutboundText>>;
  readonly isListening: Effect.Effect<boolean>;
  readonly failNextSends: (
    count: number,
    retryability?: MatrixBridgeClientError["retryability"],
  ) => Effect.Effect<void>;
  readonly emitInbound: (event: MatrixBridgeInboundText) => Effect.Effect<boolean>;
  readonly awaitSentCount: (
    count: number,
  ) => Effect.Effect<ReadonlyArray<MatrixBridgeOutboundText>>;
}

/** In-memory Matrix transport used only by focused tests. */
export const makeFakeMatrixBridgeClient = Effect.gen(function* () {
  const attemptsRef = yield* SubscriptionRef.make<ReadonlyArray<MatrixBridgeOutboundText>>([]);
  const sentRef = yield* SubscriptionRef.make<ReadonlyArray<MatrixBridgeOutboundText>>([]);
  const failureRef = yield* Ref.make<{
    readonly remaining: number;
    readonly retryability: MatrixBridgeClientError["retryability"];
  }>({ remaining: 0, retryability: "transient" });
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
    const retryability = yield* Ref.modify(failureRef, (failure) =>
      failure.remaining > 0
        ? [failure.retryability, { ...failure, remaining: Math.max(0, failure.remaining - 1) }]
        : [null, failure],
    );
    if (retryability !== null) {
      return yield* new MatrixBridgeClientError({
        operation: "send",
        reason: "Injected fake Matrix send failure.",
        retryability,
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
    failNextSends: (count, retryability = "transient") =>
      Ref.set(failureRef, { remaining: Math.max(0, count), retryability }),
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
