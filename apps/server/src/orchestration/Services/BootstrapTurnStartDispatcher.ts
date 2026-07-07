import {
  CommandId,
  EventId,
  type OrchestrationThread,
  OrchestrationDispatchCommandError,
  type OrchestrationCommand,
  type ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SynchronizedRef from "effect/SynchronizedRef";

import { GitWorkflowService } from "../../git/GitWorkflowService.ts";
import { OrchestrationCommandReceiptRepository } from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import {
  ProjectSetupScriptRunner,
  type ProjectSetupScriptRunnerError,
} from "../../project/ProjectSetupScriptRunner.ts";
import { VcsStatusBroadcaster } from "../../vcs/VcsStatusBroadcaster.ts";
import { OrchestrationCommandInvariantError } from "../Errors.ts";
import { OrchestrationEngineService } from "./OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./ProjectionSnapshotQuery.ts";

type ThreadTurnStartCommand = Extract<OrchestrationCommand, { type: "thread.turn.start" }>;
type BootstrapCreateThreadCommand = NonNullable<
  NonNullable<ThreadTurnStartCommand["bootstrap"]>["createThread"]
>;
type BootstrapPrepareWorktreeCommand = NonNullable<
  NonNullable<ThreadTurnStartCommand["bootstrap"]>["prepareWorktree"]
>;

const isOrchestrationCommandInvariantError = Schema.is(OrchestrationCommandInvariantError);
const isOrchestrationDispatchCommandError = Schema.is(OrchestrationDispatchCommandError);

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

function unexpectedCompatibilityError(error: never): never {
  throw new Error(`Unhandled compatibility error: ${String(error)}`);
}

/** Preserve the setup runner's broader pre-refactor message normalization. */
function legacySetupFailureDescription(cause: unknown): string {
  if (
    typeof cause === "object" &&
    cause !== null &&
    "message" in cause &&
    typeof cause.message === "string"
  ) {
    return cause.message;
  }
  return String(cause);
}

function projectSetupScriptCompatibilityDetail(error: ProjectSetupScriptRunnerError): string {
  switch (error._tag) {
    case "ProjectSetupScriptOperationError":
      return legacySetupFailureDescription(error.cause);
    case "ProjectSetupScriptProjectNotFoundError":
      return "Project was not found for setup script execution.";
    default:
      return unexpectedCompatibilityError(error);
  }
}

function threadAlreadyExistsDetail(threadId: ThreadId): string {
  return `Thread '${threadId}' already exists and cannot be created twice.`;
}

function isDuplicateThreadCreateError(
  error: OrchestrationDispatchCommandError,
  threadId: ThreadId,
) {
  const detail = threadAlreadyExistsDetail(threadId);
  const cause = error.cause;
  if (isOrchestrationCommandInvariantError(cause)) {
    return cause.commandType === "thread.create" && cause.detail === detail;
  }
  return error.message === `Orchestration command invariant failed (thread.create): ${detail}`;
}

function toCanonicalJson(value: unknown): string {
  return JSON.stringify(toCanonicalJsonValue(value));
}

function toCanonicalJsonValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(toCanonicalJsonValue);
  }
  if (typeof value !== "object" || value === null) {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value)
      .filter(([, entryValue]) => entryValue !== undefined)
      .sort(([leftKey], [rightKey]) => leftKey.localeCompare(rightKey))
      .map(([entryKey, entryValue]) => [entryKey, toCanonicalJsonValue(entryValue)]),
  );
}

function threadMatchesBootstrapCreate(
  thread: OrchestrationThread,
  createThread: BootstrapCreateThreadCommand,
  hasPrepareWorktree: boolean,
) {
  if (thread.deletedAt !== null) return false;
  if (thread.messages.length > 0 || thread.latestTurn !== null) return false;
  if (thread.projectId !== createThread.projectId) return false;
  if (thread.createdAt !== createThread.createdAt) return false;
  if (toCanonicalJson(thread.modelSelection) !== toCanonicalJson(createThread.modelSelection)) {
    return false;
  }
  if (thread.runtimeMode !== createThread.runtimeMode) return false;
  if (thread.interactionMode !== createThread.interactionMode) return false;
  if (!hasPrepareWorktree && thread.branch !== createThread.branch) return false;
  if (!hasPrepareWorktree && thread.worktreePath !== createThread.worktreePath) return false;
  return true;
}

function threadHasPreparedBootstrapWorktree(
  thread: OrchestrationThread,
  createThread: BootstrapCreateThreadCommand | undefined,
  prepareWorktree: BootstrapPrepareWorktreeCommand,
) {
  if (thread.deletedAt !== null || thread.worktreePath === null) return false;
  if (prepareWorktree.branch !== undefined) {
    if (thread.branch !== prepareWorktree.branch) {
      return false;
    }
  } else if (!createThread) {
    return false;
  }
  if (
    createThread &&
    thread.branch === createThread.branch &&
    thread.worktreePath === createThread.worktreePath
  ) {
    return false;
  }
  return true;
}

export interface BootstrapTurnStartDispatcherShape {
  readonly dispatch: (
    command: ThreadTurnStartCommand,
  ) => Effect.Effect<{ readonly sequence: number }, OrchestrationDispatchCommandError>;
}

export class BootstrapTurnStartDispatcher extends Context.Service<
  BootstrapTurnStartDispatcher,
  BootstrapTurnStartDispatcherShape
>()("t3/orchestration/Services/BootstrapTurnStartDispatcher") {}

let activeDispatcher: BootstrapTurnStartDispatcherShape | null = null;

export const dispatchActive = (
  command: ThreadTurnStartCommand,
): Effect.Effect<{ readonly sequence: number }, OrchestrationDispatchCommandError> => {
  const dispatcher = activeDispatcher;
  if (!dispatcher) {
    return Effect.fail(
      new OrchestrationDispatchCommandError({
        message: "Bootstrap turn start dispatcher is not available.",
      }),
    );
  }
  return dispatcher.dispatch(command);
};

export const ActiveBootstrapTurnStartDispatcherLive = Layer.effectDiscard(
  Effect.acquireRelease(
    BootstrapTurnStartDispatcher.pipe(
      Effect.tap((dispatcher) =>
        Effect.sync(() => {
          activeDispatcher = dispatcher;
        }),
      ),
    ),
    (dispatcher) =>
      Effect.sync(() => {
        if (activeDispatcher === dispatcher) activeDispatcher = null;
      }),
  ),
);

export const layer = Layer.effect(
  BootstrapTurnStartDispatcher,
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const orchestrationEngine = yield* OrchestrationEngineService;
    const commandReceiptRepository = yield* OrchestrationCommandReceiptRepository;
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const gitWorkflow = yield* GitWorkflowService;
    const projectSetupScriptRunner = yield* ProjectSetupScriptRunner;
    const vcsStatusBroadcaster = yield* VcsStatusBroadcaster;
    const path = yield* Path.Path;
    const bootstrapLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());

    const toDispatchCommandError = (cause: unknown, fallbackMessage: string) =>
      isOrchestrationDispatchCommandError(cause)
        ? cause
        : new OrchestrationDispatchCommandError({
            message: cause instanceof Error ? cause.message : fallbackMessage,
            cause,
          });
    const randomUUID = crypto.randomUUIDv4.pipe(
      Effect.mapError((cause) =>
        toDispatchCommandError(cause, "Failed to generate orchestration command identifier."),
      ),
    );
    const serverEventId = randomUUID.pipe(Effect.map(EventId.make));
    const serverCommandId = (tag: string) =>
      randomUUID.pipe(Effect.map((uuid) => CommandId.make(`server:${tag}:${uuid}`)));

    const appendSetupScriptActivity = (input: {
      readonly threadId: ThreadId;
      readonly kind: "setup-script.requested" | "setup-script.started" | "setup-script.failed";
      readonly summary: string;
      readonly createdAt: string;
      readonly payload: Record<string, unknown>;
      readonly tone: "info" | "error";
    }) =>
      Effect.all({
        commandId: serverCommandId("setup-script-activity"),
        activityId: serverEventId,
      }).pipe(
        Effect.flatMap(({ commandId, activityId }) =>
          orchestrationEngine.dispatch({
            type: "thread.activity.append",
            commandId,
            threadId: input.threadId,
            activity: {
              id: activityId,
              tone: input.tone,
              kind: input.kind,
              summary: input.summary,
              payload: input.payload,
              turnId: null,
              createdAt: input.createdAt,
            },
            createdAt: input.createdAt,
          }),
        ),
      );

    const toBootstrapDispatchCommandCauseError = (cause: Cause.Cause<unknown>) => {
      const error = Cause.squash(cause);
      return isOrchestrationDispatchCommandError(error)
        ? error
        : new OrchestrationDispatchCommandError({
            message:
              error instanceof Error ? error.message : "Failed to bootstrap thread turn start.",
            cause,
          });
    };

    const refreshGitStatus = (cwd: string) =>
      vcsStatusBroadcaster
        .refreshStatus(cwd)
        .pipe(Effect.ignoreCause({ log: true }), Effect.forkDetach, Effect.asVoid);

    const normalizeWorkspaceRelativePath = (workspaceRelativePath: string | undefined) => {
      if (!workspaceRelativePath) {
        return Effect.succeed(null);
      }
      const normalizedRelativePath = path.normalize(workspaceRelativePath);
      if (normalizedRelativePath === "." || normalizedRelativePath.length === 0) {
        return Effect.succeed(null);
      }
      if (
        path.isAbsolute(normalizedRelativePath) ||
        normalizedRelativePath === ".." ||
        normalizedRelativePath.startsWith(`..${path.sep}`)
      ) {
        return Effect.fail(
          new OrchestrationDispatchCommandError({
            message: `Invalid worktree workspace relative path: ${workspaceRelativePath}`,
          }),
        );
      }
      return Effect.succeed(normalizedRelativePath);
    };

    const applyWorkspaceRelativePath = (
      worktreeRoot: string,
      workspaceRelativePath: string | null,
    ) => (workspaceRelativePath ? path.join(worktreeRoot, workspaceRelativePath) : worktreeRoot);

    const getThreadBootstrapSemaphore = (threadId: ThreadId) =>
      SynchronizedRef.modifyEffect(bootstrapLocksRef, (current) => {
        const existing = Option.fromNullishOr(current.get(threadId));
        return Option.match(existing, {
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
        });
      });

    const withThreadBootstrapLock = <A, E, R>(
      threadId: ThreadId,
      effect: Effect.Effect<A, E, R>,
    ): Effect.Effect<A, E, R> =>
      Effect.flatMap(getThreadBootstrapSemaphore(threadId), (semaphore) =>
        semaphore.withPermit(effect),
      );

    const dispatch = Effect.fn("BootstrapTurnStartDispatcher.dispatch")(function* (
      command: ThreadTurnStartCommand,
    ) {
      const bootstrap = command.bootstrap;
      const { bootstrap: _bootstrap, ...finalTurnStartCommand } = command;
      let createdThread = false;
      let targetProjectId = bootstrap?.createThread?.projectId;
      let targetProjectCwd = bootstrap?.prepareWorktree?.projectCwd;
      let targetWorktreePath = bootstrap?.createThread?.worktreePath ?? null;
      let skipWorktreePreparation = false;

      const cleanupCreatedThread = () =>
        createdThread
          ? serverCommandId("bootstrap-thread-delete").pipe(
              Effect.flatMap((commandId) =>
                orchestrationEngine.dispatch({
                  type: "thread.delete",
                  commandId,
                  threadId: command.threadId,
                }),
              ),
              Effect.ignoreCause({ log: true }),
            )
          : Effect.void;

      const recordSetupScriptLaunchFailure = (input: {
        readonly error: ProjectSetupScriptRunnerError;
        readonly requestedAt: string;
        readonly worktreePath: string;
      }) => {
        const detail = projectSetupScriptCompatibilityDetail(input.error);
        return appendSetupScriptActivity({
          threadId: command.threadId,
          kind: "setup-script.failed",
          summary: "Setup script failed to start",
          createdAt: input.requestedAt,
          payload: {
            detail,
            worktreePath: input.worktreePath,
          },
          tone: "error",
        }).pipe(
          Effect.ignoreCause({ log: false }),
          Effect.flatMap(() =>
            Effect.logWarning("bootstrap turn start failed to launch setup script", {
              threadId: command.threadId,
              worktreePath: input.worktreePath,
              detail,
            }),
          ),
        );
      };

      const recordSetupScriptStarted = (input: {
        readonly requestedAt: string;
        readonly worktreePath: string;
        readonly scriptId: string;
        readonly scriptName: string;
        readonly terminalId: string;
      }) =>
        Effect.gen(function* () {
          const startedAt = yield* nowIso;
          const payload = {
            scriptId: input.scriptId,
            scriptName: input.scriptName,
            terminalId: input.terminalId,
            worktreePath: input.worktreePath,
          };
          yield* Effect.all([
            appendSetupScriptActivity({
              threadId: command.threadId,
              kind: "setup-script.requested",
              summary: "Starting setup script",
              createdAt: input.requestedAt,
              payload,
              tone: "info",
            }),
            appendSetupScriptActivity({
              threadId: command.threadId,
              kind: "setup-script.started",
              summary: "Setup script started",
              createdAt: startedAt,
              payload,
              tone: "info",
            }),
          ]).pipe(
            Effect.asVoid,
            Effect.catch((error) =>
              Effect.logWarning(
                "bootstrap turn start launched setup script but failed to record setup activity",
                {
                  threadId: command.threadId,
                  worktreePath: input.worktreePath,
                  scriptId: input.scriptId,
                  terminalId: input.terminalId,
                  detail: error.message,
                },
              ),
            ),
          );
        });

      const getFinalTurnReceipt = () =>
        commandReceiptRepository
          .getByCommandId({ commandId: command.commandId })
          .pipe(
            Effect.mapError((cause) =>
              toDispatchCommandError(cause, "Failed to read bootstrap turn command receipt."),
            ),
          );

      const runSetupProgram = () =>
        Effect.gen(function* () {
          if (!bootstrap?.runSetupScript || !targetWorktreePath) {
            return;
          }
          const finalTurnReceipt = yield* getFinalTurnReceipt();
          if (Option.isSome(finalTurnReceipt)) {
            return;
          }
          const worktreePath = targetWorktreePath;
          const requestedAt = yield* nowIso;
          yield* projectSetupScriptRunner
            .runForThread({
              threadId: command.threadId,
              ...(targetProjectId ? { projectId: targetProjectId } : {}),
              ...(targetProjectCwd ? { projectCwd: targetProjectCwd } : {}),
              worktreePath,
            })
            .pipe(
              Effect.matchEffect({
                onFailure: (error) =>
                  recordSetupScriptLaunchFailure({
                    error,
                    requestedAt,
                    worktreePath,
                  }),
                onSuccess: (setupResult) => {
                  if (setupResult.status !== "started") {
                    return Effect.void;
                  }
                  return recordSetupScriptStarted({
                    requestedAt,
                    worktreePath,
                    scriptId: setupResult.scriptId,
                    scriptName: setupResult.scriptName,
                    terminalId: setupResult.terminalId,
                  });
                },
              }),
            );
        });

      const getCompatibleExistingThread = (createThread: BootstrapCreateThreadCommand) =>
        projectionSnapshotQuery.getThreadDetailById(command.threadId).pipe(
          Effect.map((maybeThread) => {
            if (
              Option.isSome(maybeThread) &&
              threadMatchesBootstrapCreate(
                maybeThread.value,
                createThread,
                bootstrap?.prepareWorktree !== undefined,
              )
            ) {
              return Option.some(maybeThread.value);
            }
            return Option.none<OrchestrationThread>();
          }),
        );

      const reuseExistingThread = (thread: OrchestrationThread) => {
        targetWorktreePath = thread.worktreePath ?? targetWorktreePath;
        if (
          bootstrap?.prepareWorktree &&
          threadHasPreparedBootstrapWorktree(
            thread,
            bootstrap.createThread,
            bootstrap.prepareWorktree,
          )
        ) {
          skipWorktreePreparation = true;
        }
      };

      const reusePreparedWorktreeIfPresent = () =>
        Effect.gen(function* () {
          if (!bootstrap?.prepareWorktree || skipWorktreePreparation) {
            return;
          }
          const finalTurnReceipt = yield* getFinalTurnReceipt();
          if (Option.isSome(finalTurnReceipt)) {
            skipWorktreePreparation = true;
            return;
          }
          const existingThread = yield* projectionSnapshotQuery
            .getThreadDetailById(command.threadId)
            .pipe(
              Effect.mapError((cause) =>
                toDispatchCommandError(cause, "Failed to read bootstrap thread projection."),
              ),
            );
          if (
            Option.isSome(existingThread) &&
            threadHasPreparedBootstrapWorktree(
              existingThread.value,
              bootstrap.createThread,
              bootstrap.prepareWorktree,
            )
          ) {
            reuseExistingThread(existingThread.value);
          }
        });

      const dispatchCreateThread = (createThread: BootstrapCreateThreadCommand) =>
        Effect.gen(function* () {
          const existingThread = yield* getCompatibleExistingThread(createThread);
          if (Option.isSome(existingThread)) {
            reuseExistingThread(existingThread.value);
            return;
          }

          yield* orchestrationEngine
            .dispatch({
              type: "thread.create",
              commandId: yield* serverCommandId("bootstrap-thread-create"),
              threadId: command.threadId,
              projectId: createThread.projectId,
              title: createThread.title,
              modelSelection: createThread.modelSelection,
              runtimeMode: createThread.runtimeMode,
              interactionMode: createThread.interactionMode,
              branch: createThread.branch,
              worktreePath: createThread.worktreePath,
              worktreeRemovable:
                createThread.worktreeRemovable ?? bootstrap?.prepareWorktree !== undefined,
              worktreeRemovalPath:
                createThread.worktreeRemovalPath ??
                (bootstrap?.prepareWorktree === undefined ? createThread.worktreePath : null),
              createdAt: createThread.createdAt,
            })
            .pipe(
              Effect.matchEffect({
                onFailure: (error) => {
                  const dispatchError = toDispatchCommandError(
                    error,
                    "Failed to create bootstrap thread.",
                  );
                  if (!isDuplicateThreadCreateError(dispatchError, command.threadId)) {
                    return Effect.fail(dispatchError);
                  }
                  return getCompatibleExistingThread(createThread).pipe(
                    Effect.mapError(() => dispatchError),
                    Effect.flatMap((currentThread) => {
                      if (Option.isNone(currentThread)) {
                        return Effect.fail(dispatchError);
                      }
                      reuseExistingThread(currentThread.value);
                      return Effect.void;
                    }),
                  );
                },
                onSuccess: () =>
                  Effect.sync(() => {
                    createdThread = true;
                  }),
              }),
            );
        });

      const bootstrapProgram = Effect.gen(function* () {
        const existingFinalTurnReceipt = yield* getFinalTurnReceipt();
        if (Option.isSome(existingFinalTurnReceipt)) {
          return yield* orchestrationEngine.dispatch(finalTurnStartCommand);
        }

        if (bootstrap?.createThread) {
          yield* dispatchCreateThread(bootstrap.createThread);
        }

        yield* reusePreparedWorktreeIfPresent();

        if (bootstrap?.prepareWorktree && !skipWorktreePreparation) {
          const workspaceRelativePath = yield* normalizeWorkspaceRelativePath(
            bootstrap.prepareWorktree.workspaceRelativePath,
          );
          let worktreeBaseRef = bootstrap.prepareWorktree.baseBranch;
          if (bootstrap.prepareWorktree.startFromOrigin) {
            yield* gitWorkflow.fetchRemote({
              cwd: bootstrap.prepareWorktree.projectCwd,
              remoteName: "origin",
            });
            const resolvedRemoteBase = yield* gitWorkflow.resolveRemoteTrackingCommit({
              cwd: bootstrap.prepareWorktree.projectCwd,
              refName: bootstrap.prepareWorktree.baseBranch,
              fallbackRemoteName: "origin",
            });
            worktreeBaseRef = resolvedRemoteBase.commitSha;
          }
          const worktree = yield* gitWorkflow.createWorktree({
            cwd: bootstrap.prepareWorktree.projectCwd,
            refName: worktreeBaseRef,
            newRefName: bootstrap.prepareWorktree.branch,
            baseRefName: bootstrap.prepareWorktree.baseBranch,
            path: null,
          });
          targetWorktreePath = applyWorkspaceRelativePath(
            worktree.worktree.path,
            workspaceRelativePath,
          );
          yield* orchestrationEngine.dispatch({
            type: "thread.meta.update",
            commandId: yield* serverCommandId("bootstrap-thread-meta-update"),
            threadId: command.threadId,
            branch: worktree.worktree.refName,
            worktreePath: targetWorktreePath,
            worktreeRemovable: true,
            worktreeRemovalPath: worktree.worktree.path,
          });
          yield* refreshGitStatus(targetWorktreePath);
        }

        yield* runSetupProgram();

        return yield* orchestrationEngine.dispatch(finalTurnStartCommand);
      });

      const guardedBootstrapProgram = bootstrapProgram.pipe(
        Effect.catchCause((cause) => {
          const dispatchError = toBootstrapDispatchCommandCauseError(cause);
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.fail(dispatchError);
          }
          return cleanupCreatedThread().pipe(Effect.flatMap(() => Effect.fail(dispatchError)));
        }),
      );

      return yield* withThreadBootstrapLock(command.threadId, guardedBootstrapProgram);
    });

    return BootstrapTurnStartDispatcher.of({ dispatch });
  }),
);
