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
  readonly ownershipEpoch: number;
}

interface OutboundJob extends TerminalCandidate {
  readonly roomId: string;
  readonly transactionId: string;
  readonly body: string;
  readonly enqueuedAt: number;
}

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
  const latestTurn = thread.latestTurn;
  if (
    latestTurn?.turnId === turnId &&
    (latestTurn.state === "completed" ||
      latestTurn.state === "interrupted" ||
      latestTurn.state === "error")
  ) {
    return latestTurn.completedAt ?? latestTurn.startedAt ?? latestTurn.requestedAt;
  }
  return awarenessCompleted(thread) ? fallbackAt : null;
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

  const retainPendingCandidate = (candidate: TerminalCandidate) => {
    for (const [threadId, pending] of pendingCandidates) {
      if (threadId !== candidate.threadId || pending.ownershipEpoch !== candidate.ownershipEpoch) {
        pendingCandidates.delete(threadId);
      }
    }
    pendingCandidates.set(candidate.threadId, candidate);
  };

  const forgetPendingCandidate = (candidate: TerminalCandidate) => {
    const pending = pendingCandidates.get(candidate.threadId);
    if (
      pending?.turnId === candidate.turnId &&
      pending.ownershipEpoch === candidate.ownershipEpoch
    ) {
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
          config.value.ownershipEpoch !== candidate.ownershipEpoch
        ) {
          pendingCandidates.delete(threadId);
        }
      }
    },
  );

  const ownerStillMatches = Effect.fn("MatrixBridgeReactor.ownerStillMatches")(function* (
    job: TerminalCandidate,
  ) {
    const current = currentOwner(yield* configService.currentConfig, job.threadId);
    return (
      current !== null &&
      current.ownershipEpoch === job.ownershipEpoch &&
      current.pairing.state === "paired" &&
      current.roomId !== null
    );
  });

  const deliver = Effect.fn("MatrixBridgeReactor.deliver")(function* (job: OutboundJob) {
    let attempt = 0;
    while (true) {
      if (!(yield* ownerStillMatches(job))) {
        yield* Effect.logDebug("Matrix bridge dropped stale owner turn", {
          threadId: job.threadId,
          turnId: job.turnId,
        });
        return;
      }

      const result = yield* Effect.result(
        client.sendText({
          roomId: job.roomId,
          transactionId: job.transactionId,
          content: { msgtype: "m.text", body: job.body },
        }),
      );
      if (result._tag === "Success") return;

      const now = yield* Clock.currentTimeMillis;
      const remaining = MATRIX_BRIDGE_RETRY_WINDOW_MS - (now - job.enqueuedAt);
      if (remaining <= 0) {
        yield* Effect.logWarning("Matrix bridge outbound retry window expired", {
          threadId: job.threadId,
          turnId: job.turnId,
          attempts: attempt + 1,
        });
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

  const inspectTerminalCandidate = Effect.fn("MatrixBridgeReactor.inspectTerminalCandidate")(
    function* (candidate: TerminalCandidate) {
      const beforeRead = currentOwner(yield* configService.currentConfig, candidate.threadId);
      if (
        beforeRead === null ||
        beforeRead.ownershipEpoch !== candidate.ownershipEpoch ||
        beforeRead.pairing.state !== "paired" ||
        beforeRead.roomId === null
      ) {
        forgetPendingCandidate(candidate);
        return;
      }

      const detail = yield* projection.getThreadDetailById(candidate.threadId);
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
      const latestUserAt = latestUserMessageAt(detail.value);
      if (latestUserAt !== null && latestUserAt > terminalAt) {
        forgetPendingCandidate(candidate);
        return;
      }

      const afterRead = currentOwner(yield* configService.currentConfig, candidate.threadId);
      if (
        afterRead === null ||
        afterRead.ownershipEpoch !== candidate.ownershipEpoch ||
        afterRead.pairing.state !== "paired" ||
        afterRead.roomId === null
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
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.failCause(cause)
        : Effect.logWarning("Matrix bridge failed to inspect an outbound candidate"),
    ),
  );

  const terminalWorker = yield* makeDrainableWorker(inspectTerminalCandidate);

  const recheckPendingCandidate = (threadId: ThreadId, turnId?: TurnId) =>
    Effect.gen(function* () {
      const candidate = pendingCandidates.get(threadId);
      if (candidate === undefined || (turnId !== undefined && candidate.turnId !== turnId)) return;
      const current = currentOwner(yield* configService.currentConfig, threadId);
      if (current === null || current.ownershipEpoch !== candidate.ownershipEpoch) {
        forgetPendingCandidate(candidate);
        return;
      }
      yield* terminalWorker.enqueue(candidate);
    });

  const clearInactiveOwner = (threadId: ThreadId) =>
    Effect.gen(function* () {
      pendingCandidates.delete(threadId);
      const current = currentOwner(yield* configService.currentConfig, threadId);
      if (current === null) return;
      yield* configService
        .clearOwnerIfMatches({
          ownerThreadId: threadId,
          ownershipEpoch: current.ownershipEpoch,
        })
        .pipe(
          Effect.catchCause(() =>
            Effect.logWarning("Matrix bridge failed to clear archived or deleted owner", {
              threadId,
            }),
          ),
        );
    });

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
        ownershipEpoch: current.ownershipEpoch,
      };
      retainPendingCandidate(candidate);
      yield* terminalWorker.enqueue(candidate);
    });
  };

  const start: MatrixBridgeReactor["Service"]["start"] = Effect.fn("MatrixBridgeReactor.start")(
    function* () {
      yield* forkParked(
        Stream.runForEach(orchestrationEngine.streamDomainEvents, processDomainEvent).pipe(
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
