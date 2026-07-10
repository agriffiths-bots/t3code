// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";

import {
  CommandId,
  type OrchestrationEvent,
  type OrchestrationSession,
  type ProjectId,
  type ProviderSession,
  ThreadId,
} from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { GitWorkflowService } from "../../git/GitWorkflowService.ts";
import { ProviderService } from "../../provider/Services/ProviderService.ts";
import * as TerminalManager from "../../terminal/Manager.ts";
import {
  dispatchAlreadyCoordinated,
  OrchestrationEngineService,
} from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  ThreadDeletionReactor,
  type ThreadDeletionReactorShape,
} from "../Services/ThreadDeletionReactor.ts";
import { WorktreeLifecycleCoordinator } from "../Services/WorktreeLifecycleCoordinator.ts";

type ThreadDeletedEvent = Extract<OrchestrationEvent, { type: "thread.deleted" }>;
type ThreadArchivedEvent = Extract<OrchestrationEvent, { type: "thread.archived" }>;
type ThreadSessionSetEvent = Extract<OrchestrationEvent, { type: "thread.session-set" }>;
type ThreadCleanupEvent = ThreadDeletedEvent | ThreadArchivedEvent | ThreadSessionSetEvent;
type ThreadCleanupEventSource = "live" | "replay";

interface ThreadDeletedCleanupWorkItem {
  readonly event: ThreadDeletedEvent;
  readonly source: ThreadCleanupEventSource;
  readonly teardownRetries: number;
}

interface ThreadArchivedCleanupWorkItem {
  readonly event: ThreadArchivedEvent;
  readonly source: ThreadCleanupEventSource;
  readonly archiveSnapshotRetries: number;
  readonly teardownRetries: number;
}

interface ThreadSessionSetCleanupWorkItem {
  readonly event: ThreadSessionSetEvent;
  readonly source: ThreadCleanupEventSource;
  readonly teardownRetries: number;
}

type ThreadCleanupWorkItem =
  | ThreadDeletedCleanupWorkItem
  | ThreadArchivedCleanupWorkItem
  | ThreadSessionSetCleanupWorkItem;

type ArchivedThreadSnapshotState =
  | {
      readonly _tag: "Current";
      readonly session: OrchestrationSession | null;
      readonly projectedSessionLive: boolean;
    }
  | { readonly _tag: "Stale" }
  | { readonly _tag: "Unknown" };

export interface WorktreeTeardownSnapshot {
  readonly projects: ReadonlyArray<{
    readonly id: ProjectId;
    readonly workspaceRoot: string;
  }>;
  readonly threads: ReadonlyArray<{
    readonly id: ThreadId;
    readonly projectId: ProjectId;
    readonly worktreePath: string | null;
    readonly worktreeRemovable: boolean | undefined;
    readonly worktreeRemovalPath: string | null | undefined;
    readonly archivedAt: string | null;
    readonly deletedAt: string | null;
    readonly sessionStatus: string | null;
  }>;
}

export type WorktreeTeardownReason = "deleted" | "archived" | "stopped" | "errored";

export interface WorktreeTeardown {
  readonly threadId: ThreadId;
  readonly projectCwd: string;
  readonly path: string;
  readonly force: boolean;
}

const ARCHIVE_SNAPSHOT_UNKNOWN_FAST_RETRY_LIMIT = 5;
const ARCHIVE_SNAPSHOT_UNKNOWN_DELAYED_RETRY = "250 millis";
const TEARDOWN_RETRY_LIMIT = 5;
const TEARDOWN_RETRY_DELAY = "250 millis";

class WorktreeLifecycleTeardownError extends Schema.TaggedErrorClass<WorktreeLifecycleTeardownError>()(
  "WorktreeLifecycleTeardownError",
  {
    threadId: ThreadId,
    detail: Schema.String,
  },
) {}

function normalizeWorktreePath(value: string | null | undefined): string | null {
  const trimmed = value?.trim();
  if (!trimmed || !NodePath.isAbsolute(trimmed)) {
    return null;
  }
  const normalized = NodePath.resolve(trimmed);
  return normalized === NodePath.parse(normalized).root ? null : normalized;
}

function isSameOrNestedPath(candidate: string | null, root: string): boolean {
  if (candidate === null) {
    return false;
  }
  const normalizedCandidate = candidate.replace(/\\/g, "/").replace(/\/+$/, "");
  const normalizedRoot = root.replace(/\\/g, "/").replace(/\/+$/, "");
  return (
    normalizedCandidate === normalizedRoot ||
    normalizedCandidate.startsWith(
      normalizedRoot.endsWith("/") ? normalizedRoot : `${normalizedRoot}/`,
    )
  );
}

function pathsOverlap(left: string | null, right: string): boolean {
  return left !== null && (isSameOrNestedPath(left, right) || isSameOrNestedPath(right, left));
}

function worktreeIdentityPath(input: {
  readonly worktreePath: string | null;
  readonly worktreeRemovalPath: string | null | undefined;
}): string | null {
  return normalizeWorktreePath(input.worktreeRemovalPath ?? input.worktreePath);
}

function isWorktreeConsumerWithinRoot(
  input: {
    readonly worktreePath: string | null;
    readonly worktreeRemovalPath: string | null | undefined;
  },
  root: string,
): boolean {
  return [input.worktreePath, input.worktreeRemovalPath].some((path) =>
    isSameOrNestedPath(normalizeWorktreePath(path), root),
  );
}

function isSessionLive(status: string | null): boolean {
  return status !== null && status !== "stopped" && status !== "error" && status !== "closed";
}

export function selectWorktreeTeardown(input: {
  readonly reason: WorktreeTeardownReason;
  readonly threadId: ThreadId;
  readonly snapshot: WorktreeTeardownSnapshot;
}): WorktreeTeardown | null {
  const lifecycleThread = input.snapshot.threads.find(
    (candidate) => candidate.id === input.threadId,
  );
  if (
    !lifecycleThread ||
    (normalizeWorktreePath(lifecycleThread.worktreePath) === null &&
      normalizeWorktreePath(lifecycleThread.worktreeRemovalPath) === null)
  ) {
    return null;
  }

  const terminalRemovableOwners = input.snapshot.threads.filter(
    (candidate) =>
      candidate.worktreeRemovable === true &&
      (candidate.deletedAt !== null || candidate.archivedAt !== null) &&
      !isSessionLive(candidate.sessionStatus) &&
      worktreeIdentityPath(candidate) !== null,
  );
  const thread =
    terminalRemovableOwners.find((candidate) => candidate.id === lifecycleThread.id) ??
    terminalRemovableOwners.find((candidate) => {
      const root = worktreeIdentityPath(candidate);
      return root !== null && isWorktreeConsumerWithinRoot(lifecycleThread, root);
    });
  if (!thread) {
    return null;
  }

  const path = worktreeIdentityPath(thread);
  const project = input.snapshot.projects.find((candidate) => candidate.id === thread.projectId);
  const projectCwd = project ? normalizeWorktreePath(project.workspaceRoot) : null;
  const overlapsAnotherProjectRoot =
    path !== null &&
    input.snapshot.projects.some(
      (candidate) =>
        candidate.id !== thread.projectId &&
        pathsOverlap(normalizeWorktreePath(candidate.workspaceRoot), path),
    );
  if (!path || !projectCwd || isSameOrNestedPath(projectCwd, path) || overlapsAnotherProjectRoot) {
    return null;
  }

  const hasSharedConsumer = input.snapshot.threads.some(
    (candidate) =>
      candidate.id !== thread.id &&
      (isSameOrNestedPath(normalizeWorktreePath(candidate.worktreePath), path) ||
        isSameOrNestedPath(normalizeWorktreePath(candidate.worktreeRemovalPath), path)),
  );
  if (hasSharedConsumer) {
    return null;
  }

  return {
    threadId: thread.id,
    projectCwd,
    path,
    force: thread.deletedAt !== null,
  };
}

function worktreeTeardownReason(event: ThreadCleanupEvent): WorktreeTeardownReason | null {
  switch (event.type) {
    case "thread.deleted":
      return "deleted";
    case "thread.archived":
      return "archived";
    case "thread.session-set":
      return event.payload.session.status === "stopped"
        ? "stopped"
        : event.payload.session.status === "error"
          ? "errored"
          : null;
  }
}

const make = Effect.gen(function* () {
  const orchestrationEngine = yield* OrchestrationEngineService;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const providerService = yield* ProviderService;
  const terminalManager = yield* TerminalManager.TerminalManager;
  const gitWorkflow = yield* GitWorkflowService;
  const fileSystem = yield* FileSystem.FileSystem;
  const worktreeLifecycle = yield* WorktreeLifecycleCoordinator;
  const unknownArchiveRetryRequests = yield* PubSub.unbounded<ThreadArchivedCleanupWorkItem>();
  const teardownRetryRequests = yield* PubSub.unbounded<ThreadCleanupWorkItem>();

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
    providerService
      .listSessions()
      .pipe(Effect.map((sessions) => sessions.find((session) => session.threadId === threadId)));

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

  const dispatchSessionStopForArchive = Effect.fn("dispatchSessionStopForArchive")(function* (
    event: ThreadArchivedEvent,
    source: ThreadCleanupEventSource,
  ) {
    yield* orchestrationEngine.dispatch({
      type: "thread.session.stop",
      commandId: yield* sessionStopCommandIdForArchive(event, source),
      threadId: event.payload.threadId,
      createdAt: event.occurredAt,
    });
  });

  const sessionSetCommandIdForArchiveReplay = (event: ThreadArchivedEvent) =>
    Effect.succeed(
      CommandId.make(`session-set-for-archive-replay:${event.eventId}:${NodeCrypto.randomUUID()}`),
    );

  const stopAndRecordReplayArchivedSession = Effect.fn("stopAndRecordReplayArchivedSession")(
    function* (input: {
      readonly event: ThreadArchivedEvent;
      readonly projectedSession: OrchestrationSession | null;
      readonly runtimeSession: ProviderSession | undefined;
    }) {
      const { event, projectedSession, runtimeSession } = input;
      const threadId = event.payload.threadId;
      if (runtimeSession !== undefined) {
        yield* providerService.stopSession({ threadId });
      }
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
    },
  );

  const stopRuntimeIfPresent = Effect.fn("stopRuntimeIfPresent")(function* (threadId: ThreadId) {
    if ((yield* resolveRuntimeSession(threadId)) === undefined) {
      return;
    }
    yield* providerService.stopSession({ threadId });
    if ((yield* resolveRuntimeSession(threadId)) !== undefined) {
      return yield* new WorktreeLifecycleTeardownError({
        threadId,
        detail: "Provider runtime remained live after stop.",
      });
    }
  });

  const closeThreadTerminals = (threadId: ThreadId, deleteHistory: boolean) =>
    terminalManager.close({ threadId, ...(deleteHistory ? { deleteHistory: true } : {}) });

  const teardownWorktree = Effect.fn("ThreadDeletionReactor.teardownWorktree")(function* (
    event: ThreadCleanupEvent,
    reason: WorktreeTeardownReason,
  ) {
    const runtimeSessions = yield* providerService.listSessions();
    const snapshot = yield* projectionSnapshotQuery.getSnapshot();
    const candidate = selectWorktreeTeardown({
      reason,
      threadId: event.payload.threadId,
      snapshot: {
        projects: snapshot.projects.map((project) => ({
          id: project.id,
          workspaceRoot: project.workspaceRoot,
        })),
        threads: snapshot.threads.map((thread) => ({
          id: thread.id,
          projectId: thread.projectId,
          worktreePath: thread.worktreePath,
          worktreeRemovable: thread.worktreeRemovable,
          worktreeRemovalPath: thread.worktreeRemovalPath,
          archivedAt: thread.archivedAt,
          deletedAt: thread.deletedAt,
          sessionStatus:
            runtimeSessions.find((session) => session.threadId === thread.id)?.status ?? null,
        })),
      },
    });
    if (candidate === null) {
      return;
    }

    if (event.type === "thread.session-set") {
      yield* closeThreadTerminals(candidate.threadId, false);
    }
    if ((yield* resolveRuntimeSession(candidate.threadId)) !== undefined) {
      return yield* new WorktreeLifecycleTeardownError({
        threadId: candidate.threadId,
        detail: "Provider runtime is live during teardown.",
      });
    }

    let pathExists = yield* fileSystem.exists(candidate.path);
    let requiresRepositoryProbeBeforePrune = !pathExists;
    if (pathExists && !candidate.force) {
      yield* gitWorkflow.invalidateLocalStatus(candidate.path);
      const status = yield* gitWorkflow.localStatus({ cwd: candidate.path });
      if (!status.isRepo) {
        return yield* new WorktreeLifecycleTeardownError({
          threadId: candidate.threadId,
          detail: `Archived owned worktree '${candidate.path}' is no longer a Git worktree.`,
        });
      }
      if (status.hasWorkingTreeChanges) {
        yield* Effect.logWarning("thread archive retained dirty owned worktree", {
          threadId: candidate.threadId,
          path: candidate.path,
        });
        return;
      }
    }

    if (pathExists) {
      const removeResult = yield* gitWorkflow
        .removeWorktree({
          cwd: candidate.projectCwd,
          path: candidate.path,
          ...(candidate.force ? { force: true } : {}),
        })
        .pipe(Effect.result);
      if (removeResult._tag === "Failure") {
        pathExists = yield* fileSystem.exists(candidate.path);
        if (pathExists) {
          return yield* removeResult.failure;
        }
        requiresRepositoryProbeBeforePrune = true;
        yield* Effect.logDebug("thread lifecycle worktree disappeared during removal", {
          threadId: candidate.threadId,
          path: candidate.path,
        });
      }
    } else {
      yield* Effect.logDebug("thread lifecycle worktree already absent", {
        threadId: candidate.threadId,
        path: candidate.path,
      });
    }

    if (requiresRepositoryProbeBeforePrune) {
      if (yield* fileSystem.exists(candidate.projectCwd)) {
        yield* gitWorkflow.invalidateLocalStatus(candidate.projectCwd);
        const projectStatus = yield* gitWorkflow.localStatus({ cwd: candidate.projectCwd });
        if (projectStatus.isRepo) {
          yield* gitWorkflow.pruneWorktrees(candidate.projectCwd);
        } else {
          yield* Effect.logWarning(
            "thread lifecycle skipped worktree prune because owning project is no longer a Git repository",
            {
              threadId: candidate.threadId,
              projectRoot: candidate.projectCwd,
              path: candidate.path,
            },
          );
        }
      } else {
        yield* Effect.logWarning(
          "thread lifecycle skipped worktree prune because owning project is absent",
          {
            threadId: candidate.threadId,
            projectRoot: candidate.projectCwd,
            path: candidate.path,
          },
        );
      }
    } else {
      yield* gitWorkflow.pruneWorktrees(candidate.projectCwd);
    }
    yield* dispatchAlreadyCoordinated(orchestrationEngine, {
      type: "thread.meta.update",
      commandId: CommandId.make(`server:worktree-teardown:${event.eventId}`),
      threadId: candidate.threadId,
      worktreePath: null,
      worktreeRemovable: false,
      worktreeRemovalPath: null,
    });
    yield* Effect.logInfo("thread lifecycle worktree removed", {
      eventType: event.type,
      threadId: candidate.threadId,
      projectRoot: candidate.projectCwd,
      path: candidate.path,
    });
  });

  const processThreadDeleted = Effect.fn("processThreadDeleted")(function* (
    event: ThreadDeletedEvent,
  ) {
    yield* stopRuntimeIfPresent(event.payload.threadId);
    yield* closeThreadTerminals(event.payload.threadId, true);
    yield* teardownWorktree(event, "deleted");
  });

  const processThreadArchived = Effect.fn("processThreadArchived")(function* (
    item: ThreadArchivedCleanupWorkItem,
  ) {
    const { event, source } = item;
    const snapshotState = yield* readArchiveSnapshotStateWithFastRetries(
      event,
      item.archiveSnapshotRetries,
    );
    if (snapshotState._tag === "Stale") {
      return;
    }
    if (snapshotState._tag === "Unknown") {
      yield* Effect.logWarning("thread archive cleanup scheduled retry for unknown archive state", {
        threadId: event.payload.threadId,
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

    const runtimeSession = yield* resolveRuntimeSession(event.payload.threadId);
    if (snapshotState.projectedSessionLive || runtimeSession !== undefined) {
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
    yield* closeThreadTerminals(event.payload.threadId, false);
    yield* teardownWorktree(event, "archived");
  });

  const processThreadSessionSet = Effect.fn("processThreadSessionSet")(function* (
    event: ThreadSessionSetEvent,
  ) {
    const reason = worktreeTeardownReason(event);
    if (reason !== null) {
      yield* teardownWorktree(event, reason);
    }
  });

  const processThreadCleanupEvent = Effect.fn("processThreadCleanupEvent")(function* (
    item: ThreadCleanupWorkItem,
  ) {
    yield* worktreeLifecycle.withPermit(
      Effect.gen(function* () {
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
          case "thread.session-set":
            yield* processThreadSessionSet(item.event);
            return;
        }
      }),
    );
  });

  const processThreadCleanupEventSafely = (item: ThreadCleanupWorkItem) =>
    processThreadCleanupEvent(item).pipe(
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Effect.failCause(cause);
        }
        const nextRetry = item.teardownRetries + 1;
        return Effect.logWarning("thread cleanup reactor failed to process event", {
          eventType: item.event.type,
          threadId: item.event.payload.threadId,
          teardownRetry: nextRetry,
          cause: Cause.pretty(cause),
        }).pipe(
          Effect.andThen(
            nextRetry <= TEARDOWN_RETRY_LIMIT
              ? PubSub.publish(teardownRetryRequests, {
                  ...item,
                  teardownRetries: nextRetry,
                })
              : Effect.logError("thread lifecycle teardown retries exhausted", {
                  eventType: item.event.type,
                  threadId: item.event.payload.threadId,
                  retries: item.teardownRetries,
                }),
          ),
        );
      }),
    );

  const worker = yield* makeDrainableWorker(processThreadCleanupEventSafely);
  const enqueuedEventIds = new Set<string>();

  const toCleanupWorkItem = (
    event: ThreadCleanupEvent,
    source: ThreadCleanupEventSource,
  ): ThreadCleanupWorkItem => {
    switch (event.type) {
      case "thread.archived":
        return { event, source, archiveSnapshotRetries: 0, teardownRetries: 0 };
      case "thread.deleted":
        return { event, source, teardownRetries: 0 };
      case "thread.session-set":
        return { event, source, teardownRetries: 0 };
    }
  };

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
        shouldEnqueue ? worker.enqueue(toCleanupWorkItem(event, source)) : Effect.void,
      ),
    );

  const retryUnknownArchiveSnapshots = Stream.fromPubSub(unknownArchiveRetryRequests).pipe(
    Stream.runForEach((item) =>
      Effect.sleep(ARCHIVE_SNAPSHOT_UNKNOWN_DELAYED_RETRY).pipe(
        Effect.andThen(worker.enqueue(item)),
      ),
    ),
  );

  const retryFailedTeardowns = Stream.fromPubSub(teardownRetryRequests).pipe(
    Stream.runForEach((item) =>
      Effect.sleep(TEARDOWN_RETRY_DELAY).pipe(Effect.andThen(worker.enqueue(item))),
    ),
  );

  const replayPersistedThreadLifecycleEvents = orchestrationEngine
    .readEvents(0, Number.MAX_SAFE_INTEGER)
    .pipe(
      Stream.runForEach((event) =>
        event.type === "thread.archived" || event.type === "thread.deleted"
          ? enqueueThreadCleanupEvent(event, "replay")
          : Effect.void,
      ),
      Effect.catchCause((cause) =>
        Effect.logWarning("thread cleanup reactor failed to replay lifecycle events", {
          cause: Cause.pretty(cause),
        }),
      ),
    );

  const start: ThreadDeletionReactorShape["start"] = Effect.fn("start")(function* () {
    yield* Effect.forkScoped(retryUnknownArchiveSnapshots);
    yield* Effect.forkScoped(retryFailedTeardowns);
    yield* Effect.forkScoped(
      Stream.runForEach(orchestrationEngine.streamDomainEvents, (event) => {
        if (
          event.type !== "thread.deleted" &&
          event.type !== "thread.archived" &&
          (event.type !== "thread.session-set" || worktreeTeardownReason(event) === null)
        ) {
          return Effect.void;
        }
        return enqueueThreadCleanupEvent(event, "live");
      }),
    );
    yield* Effect.forkScoped(replayPersistedThreadLifecycleEvents);
  });

  return {
    start,
    drain: worker.drain,
  } satisfies ThreadDeletionReactorShape;
});

export const ThreadDeletionReactorLive = Layer.effect(ThreadDeletionReactor, make);
