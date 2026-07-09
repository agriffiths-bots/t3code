import * as NodeCrypto from "node:crypto";

import {
  CommandId,
  type OrchestrationEvent,
  type OrchestrationSession,
  type ProviderSession,
  type ThreadId,
} from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";

import { ProviderService } from "../../provider/Services/ProviderService.ts";
import * as TerminalManager from "../../terminal/Manager.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  ThreadDeletionReactor,
  type ThreadDeletionReactorShape,
} from "../Services/ThreadDeletionReactor.ts";

type ThreadDeletedEvent = Extract<OrchestrationEvent, { type: "thread.deleted" }>;
type ThreadArchivedEvent = Extract<OrchestrationEvent, { type: "thread.archived" }>;
type ThreadCleanupEvent = ThreadDeletedEvent | ThreadArchivedEvent;
type ThreadCleanupEventSource = "live" | "replay";
interface ThreadDeletedCleanupWorkItem {
  readonly event: ThreadDeletedEvent;
  readonly source: ThreadCleanupEventSource;
}
interface ThreadArchivedCleanupWorkItem {
  readonly event: ThreadArchivedEvent;
  readonly source: ThreadCleanupEventSource;
  readonly archiveSnapshotRetries: number;
}
type ThreadCleanupWorkItem = ThreadDeletedCleanupWorkItem | ThreadArchivedCleanupWorkItem;
type ArchivedThreadSnapshotState =
  | {
      readonly _tag: "Current";
      readonly session: OrchestrationSession | null;
      readonly projectedSessionLive: boolean;
    }
  | { readonly _tag: "Stale" }
  | { readonly _tag: "Unknown" };

const ARCHIVE_SNAPSHOT_UNKNOWN_FAST_RETRY_LIMIT = 5;
const ARCHIVE_SNAPSHOT_UNKNOWN_DELAYED_RETRY = "250 millis";

export const logCleanupCauseUnlessInterrupted = <R, E>({
  effect,
  message,
  threadId,
}: {
  readonly effect: Effect.Effect<void, E, R>;
  readonly message: string;
  readonly threadId: ThreadId;
}): Effect.Effect<void, E, R> =>
  effect.pipe(
    Effect.catchCause((cause) => {
      if (Cause.hasInterruptsOnly(cause)) {
        return Effect.failCause(cause);
      }
      return Effect.logDebug(message, {
        threadId,
        cause: Cause.pretty(cause),
      });
    }),
  );

const make = Effect.gen(function* () {
  const orchestrationEngine = yield* OrchestrationEngineService;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const providerService = yield* ProviderService;
  const terminalManager = yield* TerminalManager.TerminalManager;
  const unknownArchiveRetryRequests = yield* PubSub.unbounded<ThreadArchivedCleanupWorkItem>();

  const stopProviderSession = (threadId: ThreadId) =>
    logCleanupCauseUnlessInterrupted({
      effect: providerService.stopSession({ threadId }),
      message: "thread deletion cleanup skipped provider session stop",
      threadId,
    });

  const closeDeletedThreadTerminals = (threadId: ThreadId) =>
    logCleanupCauseUnlessInterrupted({
      effect: terminalManager.close({ threadId, deleteHistory: true }),
      message: "thread deletion cleanup skipped terminal close",
      threadId,
    });

  const closeArchivedThreadTerminals = (threadId: ThreadId) =>
    logCleanupCauseUnlessInterrupted({
      effect: terminalManager.close({ threadId }),
      message: "thread archive cleanup skipped terminal close",
      threadId,
    });

  const readArchiveSnapshotState = (event: ThreadArchivedEvent) =>
    projectionSnapshotQuery.getThreadShellByIdIncludingArchived(event.payload.threadId).pipe(
      Effect.map(
        Option.match({
          onNone: (): ArchivedThreadSnapshotState => ({ _tag: "Stale" }),
          onSome: (thread): ArchivedThreadSnapshotState =>
            thread.archivedAt === event.payload.archivedAt
              ? {
                  _tag: "Current",
                  session: thread.session,
                  projectedSessionLive:
                    thread.session !== null && thread.session.status !== "stopped",
                }
              : { _tag: "Stale" },
        }),
      ),
      Effect.orElseSucceed((): ArchivedThreadSnapshotState => ({ _tag: "Unknown" })),
    );

  const readArchiveSnapshotStateWithFastRetries = Effect.fn(
    "readArchiveSnapshotStateWithFastRetries",
  )(function* (event: ThreadArchivedEvent, delayedRetryCount: number) {
    for (let retry = 1; retry <= ARCHIVE_SNAPSHOT_UNKNOWN_FAST_RETRY_LIMIT; retry += 1) {
      const snapshotState = yield* readArchiveSnapshotState(event);
      if (snapshotState._tag !== "Unknown") {
        return snapshotState;
      }
      yield* Effect.logWarning("thread archive cleanup retrying unknown archive state", {
        threadId: event.payload.threadId,
        eventId: event.eventId,
        attempt: retry,
        delayedRetryCount,
      });
      yield* Effect.yieldNow;
    }
    return { _tag: "Unknown" } as const;
  });

  const resolveRuntimeSession = (threadId: ThreadId) =>
    providerService.listSessions().pipe(
      Effect.map((sessions) => sessions.find((session) => session.threadId === threadId)),
      Effect.orElseSucceed(() => undefined),
    );

  const sessionStopCommandIdForArchive = (
    event: ThreadArchivedEvent,
    source: ThreadCleanupEventSource,
  ) =>
    source === "replay"
      ? Effect.succeed(
          CommandId.make(
            `session-stop-for-archive-replay:${event.eventId}:${NodeCrypto.randomUUID()}`,
          ),
        )
      : Effect.succeed(CommandId.make(`session-stop-for-archive:${event.eventId}`));

  const dispatchSessionStopForArchive = (
    event: ThreadArchivedEvent,
    source: ThreadCleanupEventSource,
  ) =>
    logCleanupCauseUnlessInterrupted({
      effect: Effect.gen(function* () {
        const commandId = yield* sessionStopCommandIdForArchive(event, source);
        yield* orchestrationEngine.dispatch({
          type: "thread.session.stop",
          commandId,
          threadId: event.payload.threadId,
          createdAt: event.occurredAt,
        });
      }),
      message: "thread archive cleanup skipped provider session stop dispatch",
      threadId: event.payload.threadId,
    });

  const sessionSetCommandIdForArchiveReplay = (event: ThreadArchivedEvent) =>
    Effect.succeed(
      CommandId.make(`session-set-for-archive-replay:${event.eventId}:${NodeCrypto.randomUUID()}`),
    );

  const stopAndRecordReplayArchivedSession = (input: {
    readonly event: ThreadArchivedEvent;
    readonly projectedSession: OrchestrationSession | null;
    readonly runtimeSession: ProviderSession | undefined;
  }) =>
    logCleanupCauseUnlessInterrupted({
      effect: Effect.gen(function* () {
        const { event, projectedSession, runtimeSession } = input;
        const threadId = event.payload.threadId;
        yield* logCleanupCauseUnlessInterrupted({
          effect: providerService.stopSession({ threadId }),
          message: "thread archive replay cleanup skipped provider session stop",
          threadId,
        });
        const providerInstanceId =
          projectedSession?.providerInstanceId ?? runtimeSession?.providerInstanceId;
        yield* orchestrationEngine.dispatch({
          type: "thread.session.set",
          commandId: yield* sessionSetCommandIdForArchiveReplay(event),
          threadId,
          session: {
            threadId,
            status: "stopped",
            providerName: projectedSession?.providerName ?? runtimeSession?.provider ?? null,
            ...(providerInstanceId !== undefined ? { providerInstanceId } : {}),
            runtimeMode:
              projectedSession?.runtimeMode ?? runtimeSession?.runtimeMode ?? "full-access",
            activeTurnId: null,
            lastError: projectedSession?.lastError ?? null,
            updatedAt: event.occurredAt,
          },
          createdAt: event.occurredAt,
        });
      }),
      message: "thread archive replay cleanup skipped stopped session projection",
      threadId: input.event.payload.threadId,
    });

  const processThreadDeleted = Effect.fn("processThreadDeleted")(function* (
    event: ThreadDeletedEvent,
  ) {
    const { threadId } = event.payload;
    yield* stopProviderSession(threadId);
    yield* closeDeletedThreadTerminals(threadId);
  });

  const processThreadArchived = Effect.fn("processThreadArchived")(function* (
    item: ThreadArchivedCleanupWorkItem,
  ) {
    const { event, source } = item;
    const { threadId } = event.payload;
    const snapshotState = yield* readArchiveSnapshotStateWithFastRetries(
      event,
      item.archiveSnapshotRetries,
    );
    if (snapshotState._tag === "Stale") {
      return;
    }
    if (snapshotState._tag === "Unknown") {
      yield* Effect.logWarning("thread archive cleanup scheduled retry for unknown archive state", {
        threadId,
        eventId: event.eventId,
        delayedRetryCount: item.archiveSnapshotRetries + 1,
        delay: ARCHIVE_SNAPSHOT_UNKNOWN_DELAYED_RETRY,
      });
      yield* PubSub.publish(unknownArchiveRetryRequests, {
        ...item,
        archiveSnapshotRetries: item.archiveSnapshotRetries + 1,
      });
      return;
    }
    const projectedSessionLive =
      snapshotState._tag === "Current" && snapshotState.projectedSessionLive;
    const runtimeSession = yield* resolveRuntimeSession(threadId);
    if (projectedSessionLive || runtimeSession !== undefined) {
      if (source === "replay") {
        yield* stopAndRecordReplayArchivedSession({
          event,
          projectedSession: snapshotState.session,
          runtimeSession,
        });
      } else {
        yield* dispatchSessionStopForArchive(event, source);
      }
    }
    yield* closeArchivedThreadTerminals(threadId);
  });

  const processThreadCleanupEvent = Effect.fn("processThreadCleanupEvent")(function* (
    item: ThreadCleanupWorkItem,
  ) {
    switch (item.event.type) {
      case "thread.deleted":
        yield* processThreadDeleted(item.event);
        return;
      case "thread.archived":
        if (!("archiveSnapshotRetries" in item)) {
          return;
        }
        yield* processThreadArchived(item);
        return;
    }
  });

  const processThreadCleanupEventSafely = (item: ThreadCleanupWorkItem) =>
    processThreadCleanupEvent(item).pipe(
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Effect.failCause(cause);
        }
        return Effect.logWarning("thread cleanup reactor failed to process event", {
          eventType: item.event.type,
          threadId: item.event.payload.threadId,
          cause: Cause.pretty(cause),
        });
      }),
    );

  const worker = yield* makeDrainableWorker(processThreadCleanupEventSafely);
  const enqueuedEventIds = new Set<string>();

  const enqueueThreadCleanupEvent = (event: ThreadCleanupEvent, source: ThreadCleanupEventSource) =>
    Effect.sync(() => {
      const eventId = String(event.eventId);
      if (enqueuedEventIds.has(eventId)) {
        return false;
      }
      enqueuedEventIds.add(eventId);
      return true;
    }).pipe(
      Effect.flatMap((shouldEnqueue) =>
        shouldEnqueue
          ? worker.enqueue(
              event.type === "thread.archived"
                ? { event, source, archiveSnapshotRetries: 0 }
                : { event, source },
            )
          : Effect.void,
      ),
    );

  const retryUnknownArchiveSnapshots = Stream.fromPubSub(unknownArchiveRetryRequests).pipe(
    Stream.runForEach((item) =>
      Effect.sleep(ARCHIVE_SNAPSHOT_UNKNOWN_DELAYED_RETRY).pipe(
        Effect.andThen(worker.enqueue(item)),
      ),
    ),
  );

  const replayPersistedThreadArchivedEvents = orchestrationEngine
    .readEvents(0, Number.MAX_SAFE_INTEGER)
    .pipe(
      Stream.runForEach((event) =>
        event.type === "thread.archived" ? enqueueThreadCleanupEvent(event, "replay") : Effect.void,
      ),
      Effect.catchCause((cause) =>
        Effect.logWarning("thread cleanup reactor failed to replay archived events", {
          cause: Cause.pretty(cause),
        }),
      ),
    );

  const start: ThreadDeletionReactorShape["start"] = Effect.fn("start")(function* () {
    yield* Effect.forkScoped(retryUnknownArchiveSnapshots);
    yield* Effect.forkScoped(
      Stream.runForEach(orchestrationEngine.streamDomainEvents, (event) => {
        if (event.type !== "thread.deleted" && event.type !== "thread.archived") {
          return Effect.void;
        }
        return enqueueThreadCleanupEvent(event, "live");
      }),
    );
    yield* Effect.forkScoped(replayPersistedThreadArchivedEvents);
  });

  return {
    start,
    drain: worker.drain,
  } satisfies ThreadDeletionReactorShape;
});

export const ThreadDeletionReactorLive = Layer.effect(ThreadDeletionReactor, make);
