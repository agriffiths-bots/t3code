import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  CommandId,
  MessageId,
  type AuthAudienceCeiling,
  type AuthEnvironmentScope,
  type EnvironmentId,
  type OrchestrationEvent,
  type OrchestrationThread,
  type ThreadId,
  type TurnId,
} from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TxQueue from "effect/TxQueue";
import * as TxRef from "effect/TxRef";

import { EnvironmentAuth } from "../auth/EnvironmentAuth.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { threadAudienceSystemDispatchAuthority } from "../orchestration/commandAudienceGuard.ts";
import { BootstrapTurnStartDispatcher } from "../orchestration/Services/BootstrapTurnStartDispatcher.ts";
import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { forkParked } from "../serverActivation.ts";
import {
  MatrixBridgeClient,
  type MatrixBridgeInboundEvent,
  type MatrixBridgeInboundOverflow,
  type MatrixBridgeInboundText,
  type MatrixBridgeRoomMembership,
} from "./MatrixBridgeClient.ts";
import { MatrixBridgeConfig, type MatrixBridgeConfigV1 } from "./MatrixBridgeConfig.ts";
import { matrixTextContent } from "./matrixFormattedBody.ts";

export const MATRIX_BRIDGE_OUTBOUND_CAPACITY = 64;
/** Reserved separately so a full model-output queue cannot hide the gate. */
export const MATRIX_BRIDGE_PAIRING_CAPACITY = 8;
/** A pairing outcome gets its own slots so queued prompts cannot bury it. */
export const MATRIX_BRIDGE_PAIRING_RESULT_CAPACITY = 4;
export const MATRIX_BRIDGE_RETRY_WINDOW_MS = 10 * 60 * 1_000;
export const MATRIX_BRIDGE_RETRY_MAX_DELAY_MS = 30_000;
export const MATRIX_BRIDGE_SEEN_EVENT_CAPACITY = 1_024;
const MATRIX_BRIDGE_SEEN_TURN_CAPACITY = 1_024;
/** Same bound the scheduled-task dispatcher uses for one turn start. */
const MATRIX_BRIDGE_DISPATCH_TIMEOUT_SECONDS = 30;
/** A transient engine failure must not silently swallow a Matrix command. */
const MATRIX_BRIDGE_DISPATCH_ATTEMPTS = 3;
/** Nor may a dependency the handler needs before it can even dispatch. */
const MATRIX_BRIDGE_INBOUND_ATTEMPTS = 3;
/**
 * What a paired Matrix account can do through the bridge: read a private
 * thread's output and start turns in it. Pairing therefore accepts only a
 * credential that was granted the same reach, so a narrower code cannot unlock
 * a wider surface than it was minted for.
 */
export const MATRIX_BRIDGE_REQUIRED_PROOF: {
  readonly audienceCeiling: AuthAudienceCeiling;
  readonly scopes: ReadonlyArray<AuthEnvironmentScope>;
} = {
  audienceCeiling: "private",
  scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
};

/**
 * The only non-model messages the bridge ever sends. They are fixed strings so
 * the gate reads identically for every code, and a rejection never says which
 * of the four failures happened.
 */
export const MATRIX_BRIDGE_PAIRING_PROMPT =
  "T3 bridge is locked. Reply with a pairing code from T3 Settings > Connections.";
export const MATRIX_BRIDGE_PAIRING_REJECTED =
  "Pairing code rejected. It is invalid, expired, revoked, or already used.";
export const MATRIX_BRIDGE_PAIRING_SUCCESS =
  "Pairing complete. T3 bridging is active when a thread is selected.";
export const MATRIX_BRIDGE_PAIRING_FAILED =
  "Pairing could not be completed. Create a new code in T3 Settings > Connections and try again.";

interface TerminalCandidate {
  readonly threadId: ThreadId;
  readonly turnId: TurnId;
  readonly sequence: number;
  readonly ownershipEpoch: number;
  readonly cryptoStoreGeneration: string;
  readonly origin: "live-event" | "startup-recovery";
}

interface OutboundJob extends TerminalCandidate {
  readonly roomId: string;
  readonly transactionId: string;
  readonly body: string;
  readonly enqueuedAt: number;
}

interface PairingJob {
  readonly cryptoStoreGeneration: string;
  readonly roomId: string;
  /** A prompt is re-armed when it is not delivered; an answer is not. */
  readonly kind: "prompt" | "rejection" | "result";
  readonly transactionId: string;
  readonly body: string;
  /** A gate message is only true while the bridge is in this pairing state. */
  readonly validWhile: MatrixBridgeConfigV1["pairing"]["state"];
  readonly enqueuedAt: number;
}

/**
 * An inbound message the bridge could not turn into work. It fails rather than
 * returning, so the retry above it runs and the transport keeps its hold on the
 * sync cursor until the message is handled or reported.
 */
export class MatrixBridgeInboundNotHandledError extends Schema.TaggedErrorClass<MatrixBridgeInboundNotHandledError>()(
  "MatrixBridgeInboundNotHandledError",
  { eventId: Schema.String },
) {}

/** Joined membership of the bridged room for one connection generation. */
interface RoomMembershipState {
  readonly cryptoStoreGeneration: string;
  readonly botUserId: string;
  readonly allowedMemberPresent: boolean;
  readonly unexpectedMemberPresent: boolean;
}

type ActiveMatrixBridgeConfig = MatrixBridgeConfigV1 & {
  readonly roomId: string;
  readonly pairing: Extract<MatrixBridgeConfigV1["pairing"], { readonly state: "paired" }>;
};

export interface BoundedDrainableWorker<A> {
  readonly enqueue: (item: A) => Effect.Effect<boolean>;
  readonly drain: Effect.Effect<void>;
}

/** Dropping FIFO used for final messages; the newest item is rejected at capacity. */
export const makeBoundedDrainableWorker = <A>(
  capacity: number,
  process: (item: A) => Effect.Effect<void, never>,
): Effect.Effect<BoundedDrainableWorker<A>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const queue = yield* Effect.acquireRelease(TxQueue.dropping<A>(capacity), TxQueue.shutdown);
    const outstanding = yield* TxRef.make(0);

    yield* TxQueue.take(queue).pipe(
      Effect.tap((item) =>
        process(item).pipe(Effect.ensuring(TxRef.update(outstanding, (count) => count - 1))),
      ),
      Effect.forever,
      Effect.forkScoped,
    );

    const enqueue = (item: A) =>
      Effect.tx(
        Effect.gen(function* () {
          const offered = yield* TxQueue.offer(queue, item);
          if (offered) {
            yield* TxRef.update(outstanding, (count) => count + 1);
          }
          return offered;
        }),
      );

    const drain = Effect.tx(
      TxRef.get(outstanding).pipe(
        Effect.tap((count) => (count > 0 ? Effect.txRetry : Effect.void)),
        Effect.asVoid,
      ),
    );

    return { enqueue, drain } satisfies BoundedDrainableWorker<A>;
  });

export class MatrixBridgeReactor extends Context.Service<
  MatrixBridgeReactor,
  {
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
    readonly drain: Effect.Effect<void>;
  }
>()("t3/matrix/MatrixBridgeReactor") {}

function transactionId(input: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly turnId: TurnId;
}): string {
  return `t3.${encodeURIComponent(input.environmentId)}.${encodeURIComponent(input.threadId)}.${encodeURIComponent(input.turnId)}`;
}

/** Stable per connection and cause, so a retried gate message never doubles. */
function pairingTransactionId(input: {
  readonly environmentId: EnvironmentId;
  readonly cryptoStoreGeneration: string;
  readonly cause: string;
}): string {
  return `t3.pairing.${encodeURIComponent(input.environmentId)}.${encodeURIComponent(input.cryptoStoreGeneration)}.${encodeURIComponent(input.cause)}`;
}

function currentOwner(
  config: Option.Option<MatrixBridgeConfigV1>,
  threadId: ThreadId,
): MatrixBridgeConfigV1 | null {
  if (Option.isNone(config) || config.value.ownerThreadId !== threadId) {
    return null;
  }
  return config.value;
}

function latestUserMessageAt(thread: OrchestrationThread): string | null {
  let latest: string | null = null;
  for (const message of thread.messages) {
    if (message.role === "user" && (latest === null || message.createdAt > latest)) {
      latest = message.createdAt;
    }
  }
  return latest;
}

function awarenessCompleted(thread: OrchestrationThread): boolean {
  if (thread.messages.length === 0) return false;
  if (thread.session?.status === "error" || thread.latestTurn?.state === "error") return false;
  if (thread.session?.status === "starting") return false;
  if (
    thread.session?.status === "running" ||
    thread.session?.status === "waiting" ||
    thread.latestTurn?.state === "running"
  ) {
    return false;
  }
  if (thread.latestTurn?.state === "completed") return true;
  if (thread.latestTurn?.state === "interrupted" && thread.latestTurn.completedAt !== null) {
    return true;
  }
  return thread.session?.status === "ready" || thread.session?.status === "idle";
}

function projectedTerminalAt(
  thread: OrchestrationThread,
  turnId: TurnId,
  fallbackAt: string,
): string | null {
  const latestTurn =
    thread.latestTurn?.turnId === turnId
      ? thread.latestTurn
      : (thread.turns.find((turn) => turn.turnId === turnId) ?? null);
  if (
    latestTurn !== null &&
    (latestTurn.state === "completed" || latestTurn.state === "interrupted")
  ) {
    return latestTurn.completedAt ?? latestTurn.startedAt ?? latestTurn.requestedAt;
  }
  return thread.latestTurn?.turnId === turnId && awarenessCompleted(thread) ? fallbackAt : null;
}

function isAfterDeliveryBaseline(
  config: MatrixBridgeConfigV1,
  candidate: TerminalCandidate,
): boolean {
  return candidate.sequence > config.deliveryBaselineSequence;
}

function rememberBounded(
  seen: Map<ThreadId, Set<TurnId>>,
  order: Array<readonly [ThreadId, TurnId]>,
  threadId: ThreadId,
  turnId: TurnId,
): boolean {
  const threadTurns = seen.get(threadId);
  if (threadTurns?.has(turnId) === true) return false;
  if (threadTurns === undefined) {
    seen.set(threadId, new Set([turnId]));
  } else {
    threadTurns.add(turnId);
  }
  order.push([threadId, turnId]);
  if (order.length > MATRIX_BRIDGE_SEEN_TURN_CAPACITY) {
    const oldest = order.shift();
    if (oldest !== undefined) {
      const oldestThreadTurns = seen.get(oldest[0]);
      oldestThreadTurns?.delete(oldest[1]);
      if (oldestThreadTurns?.size === 0) seen.delete(oldest[0]);
    }
  }
  return true;
}

/** Fixed-size recent-event memory; no room timeline or decrypted body is kept. */
function rememberEventId(
  seen: Set<string>,
  order: Array<string>,
  eventId: string,
  capacity: number,
): boolean {
  if (seen.has(eventId)) return false;
  seen.add(eventId);
  order.push(eventId);
  if (order.length > capacity) {
    const oldest = order.shift();
    if (oldest !== undefined) seen.delete(oldest);
  }
  return true;
}

function forgetEventId(seen: Set<string>, order: Array<string>, eventId: string): void {
  if (!seen.delete(eventId)) return;
  const index = order.lastIndexOf(eventId);
  if (index >= 0) order.splice(index, 1);
}

function retryDelayMs(attempt: number): number {
  return Math.min(1_000 * 2 ** Math.min(attempt, 10), MATRIX_BRIDGE_RETRY_MAX_DELAY_MS);
}

export const make = Effect.gen(function* () {
  const client = yield* MatrixBridgeClient;
  const configService = yield* MatrixBridgeConfig;
  const orchestrationEngine = yield* OrchestrationEngine.OrchestrationEngineService;
  const projection = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const environmentAuth = yield* EnvironmentAuth;
  const dispatcher = yield* BootstrapTurnStartDispatcher;
  const crypto = yield* Crypto.Crypto;
  const environmentId = yield* (yield* ServerEnvironment.ServerEnvironment).getEnvironmentId;
  const randomUUID = crypto.randomUUIDv4.pipe(Effect.orDie);
  const seenEventIds = new Set<string>();
  const seenEventOrder: Array<string> = [];
  let membership: RoomMembershipState | null = null;
  let observedOwnership: string | null = null;
  let promptedGeneration: string | null = null;
  const seenTurns = new Map<ThreadId, Set<TurnId>>();
  const seenTurnOrder: Array<readonly [ThreadId, TurnId]> = [];
  const pendingCandidates = new Map<ThreadId, TerminalCandidate>();
  const scheduledCandidates = new Map<ThreadId, TerminalCandidate>();
  const trailingCandidates = new Map<ThreadId, TerminalCandidate>();

  const sameTerminalCandidate = (left: TerminalCandidate, right: TerminalCandidate) =>
    left.threadId === right.threadId &&
    left.turnId === right.turnId &&
    left.ownershipEpoch === right.ownershipEpoch &&
    left.cryptoStoreGeneration === right.cryptoStoreGeneration &&
    left.origin === right.origin;

  const retainPendingCandidate = (candidate: TerminalCandidate) => {
    const retained = pendingCandidates.get(candidate.threadId);
    if (
      retained !== undefined &&
      retained.ownershipEpoch === candidate.ownershipEpoch &&
      retained.cryptoStoreGeneration === candidate.cryptoStoreGeneration &&
      retained.sequence > candidate.sequence
    ) {
      return false;
    }
    for (const [threadId, pending] of pendingCandidates) {
      if (
        threadId !== candidate.threadId ||
        pending.ownershipEpoch !== candidate.ownershipEpoch ||
        pending.cryptoStoreGeneration !== candidate.cryptoStoreGeneration
      ) {
        pendingCandidates.delete(threadId);
      }
    }
    pendingCandidates.set(candidate.threadId, candidate);
    return true;
  };

  const forgetPendingCandidate = (candidate: TerminalCandidate) => {
    const pending = pendingCandidates.get(candidate.threadId);
    if (pending !== undefined && sameTerminalCandidate(pending, candidate)) {
      pendingCandidates.delete(candidate.threadId);
    }
  };

  const prunePendingCandidates = Effect.fn("MatrixBridgeReactor.prunePendingCandidates")(
    function* () {
      const config = yield* configService.currentConfig;
      const ownership = Option.isNone(config)
        ? null
        : `${config.value.cryptoStoreGeneration}:${config.value.ownershipEpoch}`;
      if (ownership !== observedOwnership) {
        observedOwnership = ownership;
        // Inbound text the transport is still holding was typed for the thread
        // that was bridged then, so it is dropped rather than started in the
        // one bridged now.
        yield* client.discardPendingInbound;
      }
      for (const [threadId, candidate] of pendingCandidates) {
        if (
          Option.isNone(config) ||
          config.value.ownerThreadId !== threadId ||
          config.value.ownershipEpoch !== candidate.ownershipEpoch ||
          config.value.cryptoStoreGeneration !== candidate.cryptoStoreGeneration
        ) {
          pendingCandidates.delete(threadId);
          scheduledCandidates.delete(threadId);
          trailingCandidates.delete(threadId);
        }
      }
    },
  );

  const candidateMatchesConfig = (
    config: MatrixBridgeConfigV1 | null,
    candidate: TerminalCandidate,
  ): config is ActiveMatrixBridgeConfig =>
    config !== null &&
    config.ownershipEpoch === candidate.ownershipEpoch &&
    config.cryptoStoreGeneration === candidate.cryptoStoreGeneration &&
    config.deliveryCheckpointInitialized &&
    config.pairing.state === "paired" &&
    config.roomId !== null;

  const candidateMatchesOwnerVersion = (
    config: MatrixBridgeConfigV1 | null,
    candidate: TerminalCandidate,
  ) =>
    config !== null &&
    config.ownershipEpoch === candidate.ownershipEpoch &&
    config.cryptoStoreGeneration === candidate.cryptoStoreGeneration;

  /**
   * Fail closed on membership: output leaves only while this connection's own
   * membership is known and holds nobody outside the allowed list. Unknown or
   * previous-connection membership blocks too, because a reconnect may be
   * joining a room somebody else entered while the bridge was away. Delivery is
   * paused rather than dropped, so a room that becomes safe again inside the
   * retry window is still served.
   */
  const membershipFor = (cryptoStoreGeneration: string) =>
    membership !== null && membership.cryptoStoreGeneration === cryptoStoreGeneration
      ? membership
      : null;

  const outboundPaused = (cryptoStoreGeneration: string) => {
    const current = membershipFor(cryptoStoreGeneration);
    // No allowed member in the room means the message would be encrypted
    // without them; holding it beats spending the delivery marker on output
    // their device could never decrypt after they rejoin.
    return current === null || current.unexpectedMemberPresent || !current.allowedMemberPresent;
  };

  /**
   * Gate messages exist for the allowed member reading them, so they wait while
   * the room is unsafe or unknown and are abandoned once that member is gone: a
   * rejoin is owed a fresh prompt rather than this one.
   */
  const pairingGate = (cryptoStoreGeneration: string): "send" | "wait" | "abandon" => {
    const current = membershipFor(cryptoStoreGeneration);
    if (current === null || current.unexpectedMemberPresent) return "wait";
    return current.allowedMemberPresent ? "send" : "abandon";
  };

  /** Sleeps one capped backoff step; false once the retry window is spent. */
  const waitBeforeRetry = Effect.fn("MatrixBridgeReactor.waitBeforeRetry")(function* (
    enqueuedAt: number,
    attempt: number,
  ) {
    const now = yield* Clock.currentTimeMillis;
    const remaining = MATRIX_BRIDGE_RETRY_WINDOW_MS - (now - enqueuedAt);
    if (remaining <= 0) return false;
    yield* Effect.sleep(Math.min(retryDelayMs(attempt), remaining));
    return true;
  });

  const ownerStillMatches = Effect.fn("MatrixBridgeReactor.ownerStillMatches")(function* (
    job: OutboundJob,
  ) {
    const current = currentOwner(yield* configService.currentConfig, job.threadId);
    return (
      candidateMatchesConfig(current, job) &&
      current.roomId === job.roomId &&
      current.lastDeliveredTurnId !== job.turnId &&
      isAfterDeliveryBaseline(current, job)
    );
  });

  const deliver = Effect.fn("MatrixBridgeReactor.deliver")(function* (job: OutboundJob) {
    let attempt = 0;
    let sent = false;
    let content: ReturnType<typeof matrixTextContent> | undefined;
    while (true) {
      if (!(yield* ownerStillMatches(job))) {
        yield* Effect.logDebug("Matrix bridge dropped stale owner turn", {
          threadId: job.threadId,
          turnId: job.turnId,
        });
        return;
      }
      content ??= matrixTextContent(job.body);

      if (!sent && outboundPaused(job.cryptoStoreGeneration)) {
        if (yield* waitBeforeRetry(job.enqueuedAt, attempt)) {
          attempt += 1;
          continue;
        }
        yield* Effect.logWarning("Matrix bridge dropped a final held by the room membership gate", {
          threadId: job.threadId,
          turnId: job.turnId,
        });
        return;
      }

      const result = sent
        ? yield* Effect.result(
            configService.markDeliveredIfMatches({
              ownerThreadId: job.threadId,
              ownershipEpoch: job.ownershipEpoch,
              cryptoStoreGeneration: job.cryptoStoreGeneration,
              roomId: job.roomId,
              turnId: job.turnId,
              turnSequence: job.sequence,
            }),
          )
        : yield* Effect.result(
            client.sendText({
              roomId: job.roomId,
              transactionId: job.transactionId,
              content,
              ownershipEpoch: job.ownershipEpoch,
            }),
          );
      if (result._tag === "Success") {
        if (sent) return;
        sent = true;
        continue;
      }

      if (
        !sent &&
        result.failure._tag === "MatrixBridgeClientError" &&
        result.failure.retryability === "permanent"
      ) {
        yield* Effect.logWarning("Matrix bridge dropped a permanent outbound failure", {
          threadId: job.threadId,
          turnId: job.turnId,
          attempts: attempt + 1,
        });
        yield* configService.reportPermanentSendFailureIfMatches({
          ownerThreadId: job.threadId,
          ownershipEpoch: job.ownershipEpoch,
          cryptoStoreGeneration: job.cryptoStoreGeneration,
          roomId: job.roomId,
        });
        return;
      }

      if (!(yield* waitBeforeRetry(job.enqueuedAt, attempt))) {
        yield* Effect.logWarning(
          sent
            ? "Matrix bridge delivery-marker retry window expired"
            : "Matrix bridge outbound retry window expired",
          {
            threadId: job.threadId,
            turnId: job.turnId,
            attempts: attempt + 1,
          },
        );
        return;
      }
      attempt += 1;
    }
  });

  const outboundWorker = yield* makeBoundedDrainableWorker(
    MATRIX_BRIDGE_OUTBOUND_CAPACITY,
    deliver,
  );

  const deliverPairingMessage = Effect.fn("MatrixBridgeReactor.deliverPairingMessage")(function* (
    job: PairingJob,
  ) {
    // An undelivered prompt must not keep the connection marked as prompted, or
    // an unpaired room could sit silent until it reconnects.
    const rearmPrompt = () => {
      if (job.kind === "prompt" && promptedGeneration === job.cryptoStoreGeneration) {
        promptedGeneration = null;
      }
    };

    const content = matrixTextContent(job.body);
    let attempt = 0;
    while (true) {
      const config = Option.getOrNull(yield* configService.currentConfig);
      if (
        config === null ||
        config.cryptoStoreGeneration !== job.cryptoStoreGeneration ||
        config.roomId !== job.roomId ||
        // A prompt or rejection that lost a race to a working code would
        // contradict the room's state and delay the acknowledgement behind it.
        config.pairing.state !== job.validWhile
      ) {
        return;
      }

      const gate = pairingGate(job.cryptoStoreGeneration);
      if (gate === "abandon") {
        // Nobody the gate is for is in the room any more. Delivering now would
        // spend the prompt on an empty room, and a returning device could not
        // decrypt it, so drop it and let a rejoin prompt afresh.
        yield* Effect.logDebug("Matrix bridge abandoned a pairing message with no allowed member");
        rearmPrompt();
        return;
      }
      if (gate === "send") {
        const result = yield* Effect.result(
          client.sendText({
            roomId: job.roomId,
            transactionId: job.transactionId,
            content,
            // A gate message answers the room, not a bridged thread.
            ownershipEpoch: null,
          }),
        );
        if (result._tag === "Success") return;
        if (
          result.failure._tag === "MatrixBridgeClientError" &&
          result.failure.retryability === "permanent"
        ) {
          yield* Effect.logWarning("Matrix bridge dropped a permanent pairing-message failure");
          rearmPrompt();
          return;
        }
      }

      if (!(yield* waitBeforeRetry(job.enqueuedAt, attempt))) {
        yield* Effect.logWarning("Matrix bridge pairing-message retry window expired", {
          attempts: attempt + 1,
        });
        rearmPrompt();
        return;
      }
      attempt += 1;
    }
  });

  const pairingWorker = yield* makeBoundedDrainableWorker(
    MATRIX_BRIDGE_PAIRING_CAPACITY,
    deliverPairingMessage,
  );
  // The outcome of a consumed code is the one gate message that cannot be
  // reproduced, so a full prompt/rejection queue must not be able to drop it.
  const pairingResultWorker = yield* makeBoundedDrainableWorker(
    MATRIX_BRIDGE_PAIRING_RESULT_CAPACITY,
    deliverPairingMessage,
  );

  const enqueuePairingMessage = Effect.fn("MatrixBridgeReactor.enqueuePairingMessage")(function* (
    config: MatrixBridgeConfigV1 & { readonly roomId: string },
    message: {
      readonly kind: PairingJob["kind"];
      readonly cause: string;
      readonly body: string;
      readonly validWhile?: MatrixBridgeConfigV1["pairing"]["state"];
    },
  ) {
    const enqueuedAt = yield* Clock.currentTimeMillis;
    const worker = message.kind === "result" ? pairingResultWorker : pairingWorker;
    const accepted = yield* worker.enqueue({
      cryptoStoreGeneration: config.cryptoStoreGeneration,
      roomId: config.roomId,
      kind: message.kind,
      transactionId: pairingTransactionId({
        environmentId,
        cryptoStoreGeneration: config.cryptoStoreGeneration,
        cause: message.cause,
      }),
      body: message.body,
      validWhile: message.validWhile ?? config.pairing.state,
      enqueuedAt,
    });
    if (!accepted) {
      yield* Effect.logWarning("Matrix bridge pairing queue is full; newest message dropped", {
        capacity: MATRIX_BRIDGE_PAIRING_CAPACITY,
      });
    }
  });

  const inspectProjectedCandidate = Effect.fn("MatrixBridgeReactor.inspectProjectedCandidate")(
    function* (candidate: TerminalCandidate, detail: Option.Option<OrchestrationThread>) {
      const current = currentOwner(yield* configService.currentConfig, candidate.threadId);
      if (
        !candidateMatchesConfig(current, candidate) ||
        current.lastDeliveredTurnId === candidate.turnId ||
        !isAfterDeliveryBaseline(current, candidate)
      ) {
        forgetPendingCandidate(candidate);
        return;
      }

      if (
        Option.isNone(detail) ||
        detail.value.archivedAt !== null ||
        detail.value.deletedAt !== null
      ) {
        forgetPendingCandidate(candidate);
        yield* configService
          .clearOwnerIfMatches({
            ownerThreadId: candidate.threadId,
            ownershipEpoch: candidate.ownershipEpoch,
          })
          .pipe(
            Effect.catchCause(() =>
              Effect.logWarning("Matrix bridge failed to clear an inactive owner", {
                threadId: candidate.threadId,
              }),
            ),
          );
        return;
      }

      const projectedTurn =
        detail.value.latestTurn?.turnId === candidate.turnId
          ? detail.value.latestTurn
          : detail.value.turns.find((turn) => turn.turnId === candidate.turnId);
      if (
        projectedTurn?.state === "error" ||
        (detail.value.latestTurn?.turnId === candidate.turnId &&
          detail.value.session?.status === "error")
      ) {
        forgetPendingCandidate(candidate);
        return;
      }

      const finalMessage = detail.value.messages.findLast(
        (message) =>
          message.role === "assistant" &&
          message.streaming === false &&
          message.turnId === candidate.turnId,
      );
      if (finalMessage === undefined || finalMessage.text.length === 0) return;

      const terminalAt = projectedTerminalAt(
        detail.value,
        candidate.turnId,
        finalMessage.updatedAt,
      );
      if (terminalAt === null) return;
      if (candidate.origin === "startup-recovery") {
        // Startup recovery can rediscover superseded history. A live event is
        // already a delivery obligation and must survive later user activity.
        const latestUserAt = latestUserMessageAt(detail.value);
        if (latestUserAt !== null && latestUserAt > terminalAt) {
          forgetPendingCandidate(candidate);
          return;
        }
      }

      const afterRead = currentOwner(yield* configService.currentConfig, candidate.threadId);
      if (
        !candidateMatchesConfig(afterRead, candidate) ||
        afterRead.lastDeliveredTurnId === candidate.turnId ||
        !isAfterDeliveryBaseline(afterRead, candidate)
      ) {
        forgetPendingCandidate(candidate);
        return;
      }

      if (!rememberBounded(seenTurns, seenTurnOrder, candidate.threadId, candidate.turnId)) {
        forgetPendingCandidate(candidate);
        return;
      }
      forgetPendingCandidate(candidate);

      const enqueuedAt = yield* Clock.currentTimeMillis;
      const accepted = yield* outboundWorker.enqueue({
        ...candidate,
        roomId: afterRead.roomId,
        transactionId: transactionId({
          environmentId,
          threadId: candidate.threadId,
          turnId: candidate.turnId,
        }),
        body: finalMessage.text,
        enqueuedAt,
      });
      if (!accepted) {
        yield* Effect.logWarning("Matrix bridge outbound queue is full; newest final dropped", {
          threadId: candidate.threadId,
          turnId: candidate.turnId,
          capacity: MATRIX_BRIDGE_OUTBOUND_CAPACITY,
        });
      }
    },
  );

  const inspectTerminalCandidate = Effect.fn("MatrixBridgeReactor.inspectTerminalCandidate")(
    function* (candidate: TerminalCandidate) {
      const beforeRead = currentOwner(yield* configService.currentConfig, candidate.threadId);
      if (
        !candidateMatchesConfig(beforeRead, candidate) ||
        beforeRead.lastDeliveredTurnId === candidate.turnId
      ) {
        forgetPendingCandidate(candidate);
        return;
      }

      const detail = yield* projection.getThreadDetailById(candidate.threadId);
      yield* inspectProjectedCandidate(candidate, detail);
    },
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.interrupt
        : Effect.logWarning("Matrix bridge failed to inspect an outbound candidate"),
    ),
  );

  let enqueueTrailingCandidate: (candidate: TerminalCandidate) => Effect.Effect<void> = () =>
    Effect.void;
  const terminalWorker = yield* makeDrainableWorker((candidate: TerminalCandidate) =>
    inspectTerminalCandidate(candidate).pipe(
      Effect.ensuring(
        Effect.gen(function* () {
          const scheduled = scheduledCandidates.get(candidate.threadId);
          if (scheduled !== undefined && sameTerminalCandidate(scheduled, candidate)) {
            scheduledCandidates.delete(candidate.threadId);
          }
          const trailing = trailingCandidates.get(candidate.threadId);
          const pending = pendingCandidates.get(candidate.threadId);
          if (
            trailing !== undefined &&
            sameTerminalCandidate(trailing, candidate) &&
            pending !== undefined &&
            sameTerminalCandidate(pending, candidate)
          ) {
            trailingCandidates.delete(candidate.threadId);
            scheduledCandidates.set(candidate.threadId, candidate);
            yield* enqueueTrailingCandidate(candidate);
          }
        }),
      ),
    ),
  );
  enqueueTrailingCandidate = (candidate) => terminalWorker.enqueue(candidate).pipe(Effect.asVoid);

  const scheduleTerminalCandidate = (candidate: TerminalCandidate, trailingOnDuplicate = false) => {
    const scheduled = scheduledCandidates.get(candidate.threadId);
    if (scheduled !== undefined && sameTerminalCandidate(scheduled, candidate)) {
      if (trailingOnDuplicate) trailingCandidates.set(candidate.threadId, candidate);
      return Effect.void;
    }
    trailingCandidates.delete(candidate.threadId);
    scheduledCandidates.set(candidate.threadId, candidate);
    return terminalWorker.enqueue(candidate);
  };

  const recheckPendingCandidate = (threadId: ThreadId, turnId?: TurnId) =>
    Effect.gen(function* () {
      const candidate = pendingCandidates.get(threadId);
      if (candidate === undefined || (turnId !== undefined && candidate.turnId !== turnId)) return;
      const current = currentOwner(yield* configService.currentConfig, threadId);
      if (!candidateMatchesOwnerVersion(current, candidate)) {
        forgetPendingCandidate(candidate);
        return;
      }
      if (!candidateMatchesConfig(current, candidate)) return;
      yield* scheduleTerminalCandidate(candidate, true);
    });

  const clearInactiveOwner = Effect.fn("MatrixBridgeReactor.clearInactiveOwner")(
    function* (threadId: ThreadId) {
      const beforeRead = currentOwner(yield* configService.currentConfig, threadId);
      if (beforeRead === null) return;

      const detail = yield* projection.getThreadDetailById(threadId);
      if (
        Option.isSome(detail) &&
        detail.value.archivedAt === null &&
        detail.value.deletedAt === null
      ) {
        return;
      }

      const afterRead = currentOwner(yield* configService.currentConfig, threadId);
      if (
        afterRead === null ||
        afterRead.ownershipEpoch !== beforeRead.ownershipEpoch ||
        afterRead.cryptoStoreGeneration !== beforeRead.cryptoStoreGeneration
      ) {
        return;
      }

      for (const candidates of [pendingCandidates, scheduledCandidates, trailingCandidates]) {
        const candidate = candidates.get(threadId);
        if (
          candidate !== undefined &&
          candidate.ownershipEpoch === beforeRead.ownershipEpoch &&
          candidate.cryptoStoreGeneration === beforeRead.cryptoStoreGeneration
        ) {
          candidates.delete(threadId);
        }
      }

      yield* configService.clearOwnerIfMatches({
        ownerThreadId: threadId,
        ownershipEpoch: beforeRead.ownershipEpoch,
      });
    },
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.interrupt
        : Effect.logWarning("Matrix bridge failed to clear archived or deleted owner"),
    ),
  );

  /** The bridge this message was taken off the queue for is still the bridge. */
  const inboundOwnerUnchanged = (
    config: MatrixBridgeConfigV1,
    event: MatrixBridgeInboundText,
    ownerThreadId: ThreadId,
  ): Effect.Effect<boolean> =>
    Effect.map(configService.currentConfig, (current) => {
      const live = Option.getOrNull(current);
      return (
        live !== null &&
        live.cryptoStoreGeneration === config.cryptoStoreGeneration &&
        live.pairing.state === "paired" &&
        live.ownershipEpoch === event.ownershipEpoch &&
        live.ownerThreadId === ownerThreadId
      );
    });

  const dispatchInboundText = Effect.fn("MatrixBridgeReactor.dispatchInboundText")(function* (
    config: MatrixBridgeConfigV1,
    event: MatrixBridgeInboundText,
  ) {
    const ownerThreadId = config.ownerThreadId;
    // No owner means the bridge stays silent rather than guessing a thread.
    if (ownerThreadId === null) return;

    for (let attempt = 0; attempt < MATRIX_BRIDGE_DISPATCH_ATTEMPTS; attempt += 1) {
      // Ownership is read immediately before every attempt: a message must land
      // in the thread that is bridged now, or nowhere. Moving the bridge while
      // this waits means the message belongs to the thread it was typed for,
      // which is no longer bridged, so it is dropped rather than redirected.
      if (!(yield* inboundOwnerUnchanged(config, event, ownerThreadId))) {
        yield* Effect.logDebug("Matrix bridge dropped an inbound message for a former owner");
        return;
      }

      const shell = yield* projection.getThreadShellByIdIncludingArchived(ownerThreadId);
      if (Option.isNone(shell) || shell.value.archivedAt !== null) {
        yield* clearInactiveOwner(ownerThreadId);
        return;
      }

      // Read again on the far side of that lookup: it is asynchronous, and the
      // dispatch below is the irreversible step.
      if (!(yield* inboundOwnerUnchanged(config, event, ownerThreadId))) {
        yield* Effect.logDebug("Matrix bridge dropped an inbound message for a former owner");
        return;
      }

      // The composer sends `thread.turn.start` whether the thread is idle or
      // running, and the provider adapter turns the running case into steering
      // on the live turn. Using the same command keeps both behaviors identical.
      //
      // Both ids come from the Matrix event, so a retry - here or from a later
      // redelivery of the same event - is the same command and produces one
      // turn and one user message rather than a duplicate.
      const dispatched = yield* Effect.result(
        dispatcher
          .dispatch(
            {
              type: "thread.turn.start",
              commandId: CommandId.make(`server:matrix-bridge:${event.eventId}`),
              threadId: ownerThreadId,
              message: {
                messageId: MessageId.make(`matrix-bridge:${event.eventId}`),
                role: "user",
                text: event.body,
                attachments: [],
              },
              modelSelection: shell.value.modelSelection,
              runtimeMode: shell.value.runtimeMode,
              interactionMode: shell.value.interactionMode,
              bootstrap: undefined,
              createdAt: DateTime.formatIso(yield* DateTime.now),
            },
            threadAudienceSystemDispatchAuthority(shell.value, "MatrixBridge"),
          )
          .pipe(Effect.timeout(Duration.seconds(MATRIX_BRIDGE_DISPATCH_TIMEOUT_SECONDS))),
      );
      if (dispatched._tag === "Success") return;
      yield* Effect.logWarning("Matrix bridge could not dispatch an inbound message", {
        threadId: ownerThreadId,
        attempt: attempt + 1,
      });
      if (attempt + 1 < MATRIX_BRIDGE_DISPATCH_ATTEMPTS) {
        yield* Effect.sleep(retryDelayMs(attempt));
      }
    }

    // Out of attempts: fail into the inbound retry above rather than reporting
    // the message as handled, which would release the transport's hold on the
    // sync cursor and lose the command. The deterministic command id is what
    // makes every one of those attempts the same turn.
    return yield* new MatrixBridgeInboundNotHandledError({ eventId: event.eventId });
  });

  /**
   * Delivers what the gate was holding, without the pairing event riding on the
   * outcome. It retries on its own and never fails: pairing is already
   * committed when this runs, so its caller has nothing left to redo.
   */
  const recoverHeldFinalsAfterPairing = Effect.fn(
    "MatrixBridgeReactor.recoverHeldFinalsAfterPairing",
  )(
    function* (ownerThreadId: ThreadId) {
      for (let attempt = 0; attempt < MATRIX_BRIDGE_INBOUND_ATTEMPTS; attempt += 1) {
        const recovered = yield* Effect.result(
          // The in-memory candidate first, then the same rebuild startup does
          // for a process that never saw the turn finish. Reconciliation's
          // failing view, so a read that fails here is retried rather than
          // logged and counted as recovered.
          recheckPendingCandidate(ownerThreadId).pipe(Effect.andThen(reconcileOwner())),
        );
        if (recovered._tag === "Success") {
          // Both of those hand the candidate to the terminal worker, which
          // keeps a thread's deliveries in order and swallows its own read
          // failures. So the outcome is read from the queue rather than from
          // the call: an inspected candidate has been sent or forgotten, and
          // one still pending once the worker is idle is one to try again.
          yield* terminalWorker.drain;
          if (!pendingCandidates.has(ownerThreadId)) return;
        }
        yield* Effect.logWarning("Matrix bridge could not recover a final held by the gate", {
          threadId: ownerThreadId,
          attempt: attempt + 1,
        });
        if (attempt + 1 < MATRIX_BRIDGE_INBOUND_ATTEMPTS) {
          yield* Effect.sleep(retryDelayMs(attempt));
        }
      }
    },
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.interrupt
        : Effect.logWarning("Matrix bridge gave up recovering a final held by the gate"),
    ),
  );

  const attemptPairing = Effect.fn("MatrixBridgeReactor.attemptPairing")(function* (
    config: MatrixBridgeConfigV1 & { readonly roomId: string },
    event: MatrixBridgeInboundText,
  ) {
    // Consumes the grant as proof only: no access-token session is created, so
    // a paired room never appears as an authorized client. The grant must still
    // cover what the bridge does on the paired account's behalf.
    const proof = yield* Effect.result(
      environmentAuth.consumePairingCredentialForProof(
        event.body.trim(),
        MATRIX_BRIDGE_REQUIRED_PROOF,
      ),
    );
    if (proof._tag === "Failure") {
      // Unknown, expired, revoked, and consumed codes answer identically, and
      // the code itself is never logged.
      yield* Effect.logWarning("Matrix bridge rejected a pairing code");
      yield* enqueuePairingMessage(config, {
        kind: "rejection",
        cause: `rejected.${event.eventId}`,
        body: MATRIX_BRIDGE_PAIRING_REJECTED,
      });
      return;
    }

    const marked = yield* Effect.result(
      configService.markPairedIfMatches({
        cryptoStoreGeneration: config.cryptoStoreGeneration,
        roomId: config.roomId,
        userId: event.sender,
        pairedAt: DateTime.formatIso(yield* DateTime.now),
        eventId: event.eventId,
      }),
    );
    if (marked._tag === "Failure") {
      // The code is spent and the pairing is not durable, so the bridge stays
      // locked instead of activating on memory alone.
      yield* configService.reportDegradedIfMatches({
        cryptoStoreGeneration: config.cryptoStoreGeneration,
        cause: "pairing-persist-failure",
      });
      yield* enqueuePairingMessage(config, {
        kind: "result",
        cause: `failed.${event.eventId}`,
        body: MATRIX_BRIDGE_PAIRING_FAILED,
      });
      return;
    }
    // A refusal here means the connection was replaced mid-pairing; the next
    // code minted for the new connection is the way forward.
    if (!marked.success) return;

    // Everything the transport still holds was sent to a locked room, so it is
    // a gate answer, not a turn: the newly opened gate does not let it through.
    yield* client.discardPendingInbound;
    yield* enqueuePairingMessage(config, {
      kind: "result",
      cause: `paired.${event.eventId}`,
      body: MATRIX_BRIDGE_PAIRING_SUCCESS,
      validWhile: "paired",
    });

    // A turn that finished while the room was still locked was held rather than
    // delivered, because an unpaired bridge may not send. Pairing is what makes
    // it deliverable, so it is looked at again, in its own retry: the code is
    // spent and the pairing is committed by this point, and a recovery that
    // failed into the inbound retry would replay this event as ordinary text.
    if (config.ownerThreadId !== null) {
      yield* recoverHeldFinalsAfterPairing(config.ownerThreadId);
    }
  });

  const handleRoomMembership = Effect.fn("MatrixBridgeReactor.handleRoomMembership")(function* (
    event: MatrixBridgeRoomMembership,
  ) {
    const config = Option.getOrNull(yield* configService.currentConfig);
    const roomId = config?.roomId ?? null;
    if (config === null || roomId === null || roomId !== event.roomId) return;

    const allowed = new Set<string>(config.allowedUserIds);
    const joined = event.joined.filter((userId) => userId !== event.botUserId);
    const active = [...joined, ...event.invited].filter((userId) => userId !== event.botUserId);
    const next: RoomMembershipState = {
      cryptoStoreGeneration: config.cryptoStoreGeneration,
      botUserId: event.botUserId,
      // Only a joined account can read what is sent now, so only a joined one
      // is worth prompting or delivering for.
      allowedMemberPresent: joined.some((userId) => allowed.has(userId)),
      // An outstanding invitation to an outsider is as unsafe as their
      // presence: the room starts their view at the invitation.
      unexpectedMemberPresent: active.some((userId) => !allowed.has(userId)),
    };
    membership = next;
    yield* configService.reportRoomMembershipIfMatches({
      cryptoStoreGeneration: config.cryptoStoreGeneration,
      allowedMemberPresent: next.allowedMemberPresent,
      unexpectedMemberPresent: next.unexpectedMemberPresent,
    });

    if (!next.allowedMemberPresent && promptedGeneration === config.cryptoStoreGeneration) {
      // The member this connection prompted has left, so the next arrival is
      // owed a fresh prompt rather than one they may not be able to decrypt.
      promptedGeneration = null;
    }
    if (
      config.pairing.state !== "unpaired" ||
      !next.allowedMemberPresent ||
      next.unexpectedMemberPresent ||
      promptedGeneration === config.cryptoStoreGeneration
    ) {
      return;
    }
    // Prompting waits for an allowed member so their devices hold the Megolm
    // session for it. Each prompt carries its own nonce, so a re-prompt (after
    // a leave, or after a restart) is a new encrypted room event rather than
    // the homeserver replaying one the device could not read, while retries of
    // a single prompt keep its transaction.
    promptedGeneration = config.cryptoStoreGeneration;
    yield* enqueuePairingMessage(
      { ...config, roomId },
      {
        kind: "prompt",
        cause: `prompt.${yield* randomUUID}`,
        body: MATRIX_BRIDGE_PAIRING_PROMPT,
      },
    );
  });

  const handleInboundText = Effect.fn("MatrixBridgeReactor.handleInboundText")(function* (
    event: MatrixBridgeInboundText,
  ) {
    const config = Option.getOrNull(yield* configService.currentConfig);
    const roomId = config?.roomId ?? null;
    if (config === null || roomId === null || roomId !== event.roomId) return;
    // A room the bridge considers unsafe is inert in both directions: a pairing
    // code typed while an outside account is joined is not treated as proof,
    // and no turn is started for output that could not be delivered anyway.
    const roomMembership = membershipFor(config.cryptoStoreGeneration);
    if (
      !event.roomAllowedOnly ||
      roomMembership === null ||
      roomMembership.unexpectedMemberPresent
    ) {
      yield* Effect.logDebug("Matrix bridge ignored inbound text from an unsafe room");
      return;
    }
    // The bot's own events, pairing messages and finals alike, are ignored
    // before every other rule so bridge output can never loop back in.
    if (event.sender === roomMembership.botUserId) return;
    if (event.isEdit) return;
    if (
      !rememberEventId(
        seenEventIds,
        seenEventOrder,
        event.eventId,
        MATRIX_BRIDGE_SEEN_EVENT_CAPACITY,
      )
    ) {
      return;
    }

    if (config.pairing.state === "paired") {
      if (event.sender !== config.pairing.userId) return;
      // The bridge moved after the transport took this message off its queue,
      // so the thread it was typed for is no longer the one bridged.
      if (config.ownershipEpoch !== event.ownershipEpoch) {
        yield* Effect.logDebug("Matrix bridge dropped an inbound message for a former owner");
        return;
      }
      // The reply that paired this connection is remembered durably, so a
      // redelivery after a restart is still a pairing reply and not a turn.
      if (config.pairing.eventId === event.eventId) return;
      // A code typed just before pairing completed, or one sent afterwards by
      // mistake, must not become a user message: thread history and model
      // context are the last places a redeemable credential should land. An
      // unavailable credential store withholds too, rather than guessing.
      // A store that cannot answer fails into the retry above rather than
      // returning: withholding is right, but dropping the message on the way
      // would lose a command to a blip in a component it only consults.
      if (yield* environmentAuth.isLivePairingCredential(event.body.trim())) {
        yield* Effect.logWarning(
          "Matrix bridge withheld a possible pairing credential from a turn",
        );
        return;
      }
      yield* dispatchInboundText(config, event);
      return;
    }
    // A sender outside the allowed list is ignored without revealing that a
    // gate exists, and pre-pairing text is a code candidate, never a turn.
    if (!config.allowedUserIds.includes(event.sender)) return;
    yield* attemptPairing({ ...config, roomId }, event);
  });

  const handleInboundOverflow = Effect.fn("MatrixBridgeReactor.handleInboundOverflow")(function* (
    event: MatrixBridgeInboundOverflow,
  ) {
    const config = Option.getOrNull(yield* configService.currentConfig);
    if (config === null || config.roomId !== event.roomId) return;
    // Bounded memory means a burst can outrun dispatch. Say so in the status
    // rather than letting a message disappear without a trace.
    yield* Effect.logWarning("Matrix bridge dropped inbound messages under load");
    yield* configService.reportDegradedIfMatches({
      cryptoStoreGeneration: config.cryptoStoreGeneration,
      cause: "inbound-overflow",
    });
  });

  /**
   * A message the bridge could not handle is retried rather than swallowed:
   * the transport releases its hold on the sync cursor once this returns, so a
   * failure that looks like success here loses the command for good. A
   * dependency that stays down is reported instead of disappearing.
   */
  const handleInboundTextWithRetry = Effect.fn("MatrixBridgeReactor.handleInboundTextWithRetry")(
    function* (event: MatrixBridgeInboundText) {
      // Captured before the attempts: a failure belongs to the connection that
      // received the message, and `reportDegradedIfMatches` drops it if that
      // connection has since been replaced rather than marking a fresh one.
      const received = Option.getOrNull(yield* configService.currentConfig);
      for (let attempt = 0; attempt < MATRIX_BRIDGE_INBOUND_ATTEMPTS; attempt += 1) {
        const handled = yield* Effect.result(handleInboundText(event));
        if (handled._tag === "Success") return;
        yield* Effect.logWarning("Matrix bridge could not handle an inbound message", {
          attempt: attempt + 1,
        });
        // The dedupe entry is what a retry would trip over, so it is released
        // with the attempt that recorded it.
        forgetEventId(seenEventIds, seenEventOrder, event.eventId);
        if (attempt + 1 < MATRIX_BRIDGE_INBOUND_ATTEMPTS) {
          yield* Effect.sleep(retryDelayMs(attempt));
        }
      }

      if (received === null) return;
      yield* configService.reportDegradedIfMatches({
        cryptoStoreGeneration: received.cryptoStoreGeneration,
        cause: "inbound-failed",
      });
    },
  );

  const handleInboundEvent = (event: MatrixBridgeInboundEvent): Effect.Effect<void> =>
    (event.kind === "membership"
      ? handleRoomMembership(event)
      : event.kind === "overflow"
        ? handleInboundOverflow(event)
        : handleInboundTextWithRetry(event)
    ).pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.interrupt
          : Effect.logWarning("Matrix bridge failed to handle an inbound Matrix event"),
      ),
    );

  const reconcileOwner = Effect.fn("MatrixBridgeReactor.reconcileOwner")(function* () {
    let config = Option.getOrNull(yield* configService.currentConfig);
    // Deliberately not gated on pairing. The delivery baseline exists so an
    // upgrade does not replay history, which has nothing to do with the gate:
    // establishing it at start is what lets a turn completed while the room
    // is still locked be recovered once the code arrives, rather than being
    // baselined away by an initialization that runs after it.
    if (config === null || config.ownerThreadId === null || config.roomId === null) {
      return;
    }

    const ownerThreadId = config.ownerThreadId;
    const detailSnapshot = yield* projection.getThreadDetailSnapshot(ownerThreadId);
    if (
      Option.isNone(detailSnapshot) ||
      detailSnapshot.value.thread.archivedAt !== null ||
      detailSnapshot.value.thread.deletedAt !== null
    ) {
      yield* clearInactiveOwner(ownerThreadId);
      return;
    }

    const detail = detailSnapshot.value.thread;
    const latestTurn = detail.latestTurn;
    if (!config.deliveryCheckpointInitialized) {
      const hasInFlightTurn = latestTurn?.state === "running";
      const baselineTurnId = hasInFlightTurn ? null : (latestTurn?.turnId ?? null);
      const baselineSequence = hasInFlightTurn
        ? config.deliveryBaselineSequence
        : detailSnapshot.value.snapshotSequence;
      const initialized = yield* configService.initializeDeliveryCheckpointIfMissing({
        ownerThreadId,
        ownershipEpoch: config.ownershipEpoch,
        cryptoStoreGeneration: config.cryptoStoreGeneration,
        roomId: config.roomId,
        baselineTurnId,
        baselineSequence,
      });
      if (!initialized) return;

      const initializedConfig = currentOwner(yield* configService.currentConfig, ownerThreadId);
      if (
        initializedConfig === null ||
        initializedConfig.ownershipEpoch !== config.ownershipEpoch ||
        initializedConfig.cryptoStoreGeneration !== config.cryptoStoreGeneration ||
        !initializedConfig.deliveryCheckpointInitialized ||
        initializedConfig.pairing.state !== "paired" ||
        initializedConfig.roomId === null
      ) {
        return;
      }
      config = initializedConfig;
      for (const candidates of [pendingCandidates, scheduledCandidates, trailingCandidates]) {
        const candidate = candidates.get(ownerThreadId);
        if (
          candidate !== undefined &&
          candidate.ownershipEpoch === config.ownershipEpoch &&
          candidate.cryptoStoreGeneration === config.cryptoStoreGeneration &&
          candidate.sequence <= config.deliveryBaselineSequence
        ) {
          candidates.delete(ownerThreadId);
        }
      }
      yield* recheckPendingCandidate(ownerThreadId);
    }

    // Recovery itself waits for the gate: an unpaired bridge may not send,
    // and the candidate stays pending until pairing looks again.
    if (config.pairing.state !== "paired") return;
    if (latestTurn === null || config.lastDeliveredTurnId === latestTurn.turnId) return;
    const recoveryEvent = yield* orchestrationEngine
      .readEvents(config.deliveryBaselineSequence, Number.MAX_SAFE_INTEGER)
      .pipe(
        Stream.filter(
          (event) =>
            event.type === "thread.message-sent" &&
            event.payload.threadId === ownerThreadId &&
            event.payload.role === "assistant" &&
            event.payload.streaming === false &&
            event.payload.turnId === latestTurn.turnId,
        ),
        Stream.runHead,
      );
    if (Option.isNone(recoveryEvent)) return;

    const candidate = {
      threadId: ownerThreadId,
      turnId: latestTurn.turnId,
      sequence: recoveryEvent.value.sequence,
      ownershipEpoch: config.ownershipEpoch,
      cryptoStoreGeneration: config.cryptoStoreGeneration,
      origin: "startup-recovery" as const,
    };
    if (retainPendingCandidate(candidate)) {
      yield* inspectProjectedCandidate(candidate, Option.some(detail));
    }
  });

  /**
   * Startup's fail-safe view of the same work: nothing is waiting on it there,
   * and a read that fails must not take the reactor down with it. Callers that
   * can retry use {@link reconcileOwner} and see the failure.
   */
  const reconcileOwnerAtStartup = Effect.fn("MatrixBridgeReactor.reconcileOwnerAtStartup")(
    function* () {
      yield* reconcileOwner();
    },
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.interrupt
        : Effect.logWarning("Matrix bridge failed to reconcile the owner at startup"),
    ),
  );

  const processDomainEvent = (event: OrchestrationEvent): Effect.Effect<void> => {
    if (event.type === "thread.archived" || event.type === "thread.deleted") {
      return clearInactiveOwner(event.payload.threadId);
    }
    if (event.type === "thread.turn-diff-completed") {
      return recheckPendingCandidate(event.payload.threadId, event.payload.turnId);
    }
    if (event.type === "thread.session-set") {
      return recheckPendingCandidate(event.payload.threadId);
    }
    if (
      event.type !== "thread.message-sent" ||
      event.payload.role !== "assistant" ||
      event.payload.streaming ||
      event.payload.turnId === null
    )
      return Effect.void;
    return Effect.gen(function* () {
      const current = currentOwner(yield* configService.currentConfig, event.payload.threadId);
      if (current === null) return;
      const candidate = {
        threadId: event.payload.threadId,
        turnId: event.payload.turnId as TurnId,
        sequence: event.sequence,
        ownershipEpoch: current.ownershipEpoch,
        cryptoStoreGeneration: current.cryptoStoreGeneration,
        origin: "live-event" as const,
      };
      if (!retainPendingCandidate(candidate)) return;
      const refreshed = currentOwner(yield* configService.currentConfig, candidate.threadId);
      if (!candidateMatchesOwnerVersion(refreshed, candidate)) {
        forgetPendingCandidate(candidate);
        return;
      }
      if (candidateMatchesConfig(refreshed, candidate)) {
        yield* scheduleTerminalCandidate(candidate);
      }
    });
  };

  const start: MatrixBridgeReactor["Service"]["start"] = Effect.fn("MatrixBridgeReactor.start")(
    function* () {
      // Acquire the hot PubSub subscription before activation so scheduled work
      // cannot publish a final into a subscriber-free window.
      const domainEventsPull = yield* Stream.toPull(orchestrationEngine.streamDomainEvents);
      yield* forkParked(
        Stream.fromPull(Effect.succeed(domainEventsPull)).pipe(
          Stream.runForEach(processDomainEvent),
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.failCause(cause)
              : Effect.logWarning("Matrix bridge domain-event consumer stopped"),
          ),
        ),
      );
      yield* forkParked(
        Stream.runForEach(configService.statusChanges, () => prunePendingCandidates()).pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.failCause(cause)
              : Effect.logWarning("Matrix bridge ownership observer stopped"),
          ),
        ),
      );
      yield* forkParked(
        client
          .listen(handleInboundEvent)
          .pipe(
            Effect.catchCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.failCause(cause)
                : Effect.logWarning("Matrix bridge client listener stopped"),
            ),
          ),
      );
      yield* forkParked(reconcileOwnerAtStartup());
    },
  );

  return MatrixBridgeReactor.of({
    start,
    drain: Effect.yieldNow.pipe(
      Effect.andThen(terminalWorker.drain),
      Effect.andThen(outboundWorker.drain),
      Effect.andThen(pairingWorker.drain),
      Effect.andThen(pairingResultWorker.drain),
    ),
  });
});

export const layer = Layer.effect(MatrixBridgeReactor, make);
