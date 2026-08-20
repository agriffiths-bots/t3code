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
  /**
   * The bridge ownership epoch this message belongs to, checked again against
   * the live configuration immediately before the send. Gate messages are not
   * owner-scoped and carry null.
   */
  readonly ownershipEpoch: number | null;
}

/** Decrypted timeline text from the bridged room. */
export interface MatrixBridgeInboundText {
  readonly kind: "text";
  readonly eventId: string;
  readonly roomId: string;
  readonly sender: string;
  readonly body: string;
  readonly isEdit: boolean;
  /**
   * Whether the room held only allowed accounts at the moment this event was
   * handed over, read from the same membership the transport sends against.
   * It travels with the event so a delayed membership report cannot let text
   * be treated as safe.
   */
  readonly roomAllowedOnly: boolean;
  /**
   * The bridge ownership epoch when the transport took this event off its
   * queue. A message belongs to the thread that was bridged then, so a move
   * during the work that follows invalidates it rather than redirecting it.
   */
  readonly ownershipEpoch: number;
}

/**
 * Joined membership of the bridged room, published once per connection and
 * again on every membership change. The bridge compares it against the allowed
 * list to decide whether it may prompt for pairing or keep sending.
 */
export interface MatrixBridgeRoomMembership {
  readonly kind: "membership";
  readonly roomId: string;
  readonly botUserId: string;
  readonly joined: ReadonlyArray<string>;
}

/**
 * Inbound text arrived faster than the bridge could dispatch it and was
 * dropped. It is reported rather than swallowed, so the operator sees why a
 * message never became a turn.
 */
export interface MatrixBridgeInboundOverflow {
  readonly kind: "overflow";
  readonly roomId: string;
}

export type MatrixBridgeInboundEvent =
  | MatrixBridgeInboundText
  | MatrixBridgeRoomMembership
  | MatrixBridgeInboundOverflow;

/** Inbound events are delivered one at a time so room order is preserved. */
export type MatrixBridgeInboundHandler = (
  event: MatrixBridgeInboundEvent,
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
    /**
     * Drops inbound text the transport is still holding. The bridge calls it
     * when ownership moves: a message typed for the thread that was bridged
     * then must not be started in the one bridged now.
     */
    readonly discardPendingInbound: Effect.Effect<void>;
  }
>()("t3/matrix/MatrixBridgeClient") {}

export interface FakeMatrixBridgeClient {
  readonly layer: Layer.Layer<MatrixBridgeClient>;
  readonly discardedInbound: Effect.Effect<number>;
  readonly attempts: Effect.Effect<ReadonlyArray<MatrixBridgeOutboundText>>;
  readonly awaitAttemptCount: (
    count: number,
  ) => Effect.Effect<ReadonlyArray<MatrixBridgeOutboundText>>;
  readonly sent: Effect.Effect<ReadonlyArray<MatrixBridgeOutboundText>>;
  readonly isListening: Effect.Effect<boolean>;
  /** Completes once a reactor is ready to receive inbound events. */
  readonly awaitListening: Effect.Effect<void>;
  readonly failNextSends: (
    count: number,
    retryability?: MatrixBridgeClientError["retryability"],
  ) => Effect.Effect<void>;
  readonly emitInbound: (event: MatrixBridgeInboundEvent) => Effect.Effect<boolean>;
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
  const inboundHandlerRef = yield* SubscriptionRef.make<Option.Option<MatrixBridgeInboundHandler>>(
    Option.none(),
  );
  const discardedRef = yield* Ref.make(0);

  const listen: MatrixBridgeClient["Service"]["listen"] = (onInboundText) =>
    Effect.acquireRelease(SubscriptionRef.set(inboundHandlerRef, Option.some(onInboundText)), () =>
      SubscriptionRef.set(inboundHandlerRef, Option.none()),
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
    layer: Layer.succeed(
      MatrixBridgeClient,
      MatrixBridgeClient.of({
        listen,
        sendText,
        discardPendingInbound: Ref.update(discardedRef, (count) => count + 1),
      }),
    ),
    discardedInbound: Ref.get(discardedRef),
    attempts: SubscriptionRef.get(attemptsRef),
    awaitAttemptCount: (count) => awaitCount(attemptsRef, count),
    sent: SubscriptionRef.get(sentRef),
    isListening: SubscriptionRef.get(inboundHandlerRef).pipe(Effect.map(Option.isSome)),
    awaitListening: SubscriptionRef.changes(inboundHandlerRef).pipe(
      Stream.filter(Option.isSome),
      Stream.runHead,
      Effect.asVoid,
    ),
    failNextSends: (count, retryability = "transient") =>
      Ref.set(failureRef, { remaining: Math.max(0, count), retryability }),
    emitInbound: (event) =>
      SubscriptionRef.get(inboundHandlerRef).pipe(
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
