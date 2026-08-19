import {
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
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TxQueue from "effect/TxQueue";
import * as TxRef from "effect/TxRef";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { forkParked } from "../serverActivation.ts";
import { MatrixBridgeClient } from "./MatrixBridgeClient.ts";
import { MatrixBridgeConfig, type MatrixBridgeConfigV1 } from "./MatrixBridgeConfig.ts";

export const MATRIX_BRIDGE_OUTBOUND_CAPACITY = 64;
export const MATRIX_BRIDGE_RETRY_WINDOW_MS = 10 * 60 * 1_000;
export const MATRIX_BRIDGE_RETRY_MAX_DELAY_MS = 30_000;
const MATRIX_BRIDGE_SEEN_TURN_CAPACITY = 1_024;

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

function retryDelayMs(attempt: number): number {
  return Math.min(1_000 * 2 ** Math.min(attempt, 10), MATRIX_BRIDGE_RETRY_MAX_DELAY_MS);
}

export const make = Effect.gen(function* () {
  const client = yield* MatrixBridgeClient;
  const configService = yield* MatrixBridgeConfig;
  const orchestrationEngine = yield* OrchestrationEngine.OrchestrationEngineService;
  const projection = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const environmentId = yield* (yield* ServerEnvironment.ServerEnvironment).getEnvironmentId;
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
    while (true) {
      if (!(yield* ownerStillMatches(job))) {
        yield* Effect.logDebug("Matrix bridge dropped stale owner turn", {
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
              content: { msgtype: "m.text", body: job.body },
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

      const now = yield* Clock.currentTimeMillis;
      const remaining = MATRIX_BRIDGE_RETRY_WINDOW_MS - (now - job.enqueuedAt);
      if (remaining <= 0) {
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

      const delay = Math.min(retryDelayMs(attempt), remaining);
      attempt += 1;
      yield* Effect.sleep(delay);
    }
  });

  const outboundWorker = yield* makeBoundedDrainableWorker(
    MATRIX_BRIDGE_OUTBOUND_CAPACITY,
    deliver,
  );

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

  const reconcileOwnerAtStartup = Effect.fn("MatrixBridgeReactor.reconcileOwnerAtStartup")(
    function* () {
      let config = Option.getOrNull(yield* configService.currentConfig);
      if (
        config === null ||
        config.ownerThreadId === null ||
        config.pairing.state !== "paired" ||
        config.roomId === null
      ) {
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
          .listen(() => Effect.void)
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
    ),
  });
});

export const layer = Layer.effect(MatrixBridgeReactor, make);
