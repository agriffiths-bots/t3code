import {
  CommandId,
  type DataAudience,
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
import {
  threadHasPreparedBootstrapWorktree,
  threadMatchesBootstrapCreate,
  type BootstrapCreateThreadCommand,
} from "../bootstrapCommandState.ts";
import {
  audienceBoundSystemDispatchAuthority,
  authorizeOrchestrationCommandMutation,
  type OrchestrationCommandDispatchAuthority,
} from "../commandAudienceGuard.ts";
import { dispatchAlreadyCoordinated, OrchestrationEngineService } from "./OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./ProjectionSnapshotQuery.ts";
import { WorktreeLifecycleCoordinator } from "./WorktreeLifecycleCoordinator.ts";

type ThreadTurnStartCommand = Extract<OrchestrationCommand, { type: "thread.turn.start" }>;
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

function threadArchivedTurnStartDetail(threadId: ThreadId): string {
  return `Thread '${threadId}' is already archived and cannot handle command 'thread.turn.start'.`;
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

function isArchivedFinalTurnStartCause(cause: Cause.Cause<unknown>, threadId: ThreadId): boolean {
  const error = Cause.squash(cause);
  const detail = threadArchivedTurnStartDetail(threadId);
  if (isOrchestrationCommandInvariantError(error)) {
    return error.commandType === "thread.turn.start" && error.detail === detail;
  }
  if (isOrchestrationDispatchCommandError(error)) {
    return error.message.includes(detail);
  }
  return error instanceof Error && error.message.includes(detail);
}

export interface BootstrapTurnStartDispatcherShape {
  readonly dispatch: (
    command: ThreadTurnStartCommand,
    authority: OrchestrationCommandDispatchAuthority,
  ) => Effect.Effect<{ readonly sequence: number }, OrchestrationDispatchCommandError>;
}

export class BootstrapTurnStartDispatcher extends Context.Service<
  BootstrapTurnStartDispatcher,
  BootstrapTurnStartDispatcherShape
>()("t3/orchestration/Services/BootstrapTurnStartDispatcher") {}

let activeDispatcher: BootstrapTurnStartDispatcherShape | null = null;

export const dispatchActive = (
  command: ThreadTurnStartCommand,
  authority: OrchestrationCommandDispatchAuthority,
): Effect.Effect<{ readonly sequence: number }, OrchestrationDispatchCommandError> => {
  const dispatcher = activeDispatcher;
  if (!dispatcher) {
    return Effect.fail(
      new OrchestrationDispatchCommandError({
        message: "Bootstrap turn start dispatcher is not available.",
      }),
    );
  }
  return dispatcher.dispatch(command, authority);
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
    const worktreeLifecycle = yield* WorktreeLifecycleCoordinator;
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
      readonly authority: OrchestrationCommandDispatchAuthority;
    }) =>
      Effect.all({
        commandId: serverCommandId("setup-script-activity"),
        activityId: serverEventId,
      }).pipe(
        Effect.flatMap(({ commandId, activityId }) =>
          orchestrationEngine.dispatch(
            {
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
            },
            input.authority,
          ),
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
      authority: OrchestrationCommandDispatchAuthority,
    ) {
      const bootstrap = command.bootstrap;
      const { bootstrap: _bootstrap, ...finalTurnStartCommand } = command;
      let createdThread = false;
      let targetProjectId = bootstrap?.createThread?.projectId;
      let targetProjectCwd = bootstrap?.prepareWorktree?.projectCwd;
      let targetWorktreePath = bootstrap?.createThread?.worktreePath ?? null;
      let skipWorktreePreparation = false;
      let internalAuthority: OrchestrationCommandDispatchAuthority = authority;
      // The worktree THIS bootstrap created (if any), for cleanup on failure.
      // A failed bootstrap must not leave its worktree behind — and for
      // directory-targeted (non-removable, foreign-repo) worktrees lifecycle
      // teardown must not remove them, so the dispatcher owns cleanup.
      let createdWorktree: { readonly cwd: string; readonly path: string } | null = null;

      const cleanupCreatedThread = () =>
        createdThread
          ? serverCommandId("bootstrap-thread-delete").pipe(
              Effect.flatMap((commandId) =>
                dispatchAlreadyCoordinated(
                  orchestrationEngine,
                  {
                    type: "thread.delete",
                    commandId,
                    threadId: command.threadId,
                  },
                  authority,
                ),
              ),
              Effect.ignoreCause({ log: true }),
            )
          : Effect.void;

      const cleanupCreatedWorktree = () =>
        createdWorktree
          ? gitWorkflow
              .removeWorktree({
                cwd: createdWorktree.cwd,
                path: createdWorktree.path,
                force: true,
              })
              .pipe(Effect.ignoreCause({ log: true }))
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
          authority,
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
              authority,
            }),
            appendSetupScriptActivity({
              threadId: command.threadId,
              kind: "setup-script.started",
              summary: "Setup script started",
              createdAt: startedAt,
              payload,
              tone: "info",
              authority,
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

      const authorizeBootstrapCommand = (): Effect.Effect<
        DataAudience,
        OrchestrationDispatchCommandError
      > =>
        Effect.gen(function* () {
          const readModel = yield* projectionSnapshotQuery
            .getCommandReadModel()
            .pipe(
              Effect.mapError((cause) =>
                toDispatchCommandError(cause, "Failed to read bootstrap authorization model."),
              ),
            );
          yield* authorizeOrchestrationCommandMutation({ command, readModel, authority }).pipe(
            Effect.mapError((cause) =>
              toDispatchCommandError(cause, "Bootstrap turn start command is not authorized."),
            ),
          );
          const targetAudience =
            readModel.threads.find((thread) => thread.id === command.threadId)?.dataAudience ??
            readModel.projects.find((project) => project.id === bootstrap?.createThread?.projectId)
              ?.dataAudience;
          if (targetAudience === undefined) {
            return yield* new OrchestrationDispatchCommandError({
              message: "Bootstrap turn start target audience could not be resolved.",
            });
          }
          return targetAudience;
        });

      const getFinalTurnReceipt = () =>
        commandReceiptRepository
          .getByCommandId({ commandId: command.commandId })
          .pipe(
            Effect.mapError((cause) =>
              toDispatchCommandError(cause, "Failed to read bootstrap turn command receipt."),
            ),
          );

      const rejectFinalTurnIfThreadArchived = () =>
        projectionSnapshotQuery.getThreadShellByIdIncludingArchived(command.threadId).pipe(
          Effect.mapError((cause) =>
            toDispatchCommandError(cause, "Failed to read bootstrap thread archive state."),
          ),
          Effect.flatMap(
            Option.match({
              onNone: () => Effect.void,
              onSome: (thread) =>
                thread.archivedAt === null
                  ? Effect.void
                  : dispatchAlreadyCoordinated(
                      orchestrationEngine,
                      finalTurnStartCommand,
                      authority,
                    ).pipe(Effect.asVoid),
            }),
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
          yield* rejectFinalTurnIfThreadArchived();
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

          yield* dispatchAlreadyCoordinated(
            orchestrationEngine,
            {
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
            },
            internalAuthority,
          ).pipe(
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
          return yield* dispatchAlreadyCoordinated(
            orchestrationEngine,
            finalTurnStartCommand,
            authority,
          );
        }

        const targetAudience = yield* authorizeBootstrapCommand();
        internalAuthority = audienceBoundSystemDispatchAuthority({
          reason: "bootstrap-turn-start",
          sourceThreadId: command.threadId,
          dataAudience: targetAudience,
        });

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
          createdWorktree = {
            cwd: bootstrap.prepareWorktree.projectCwd,
            path: worktree.worktree.path,
          };
          targetWorktreePath = applyWorkspaceRelativePath(
            worktree.worktree.path,
            workspaceRelativePath,
          );
          // An explicit cleanup policy from createThread survives worktree
          // preparation: cross-repo (directory-targeted) worktrees are marked
          // non-removable there because lifecycle teardown runs against the
          // project's repository and cannot safely remove a foreign checkout.
          const preparedWorktreeRemovable = bootstrap.createThread?.worktreeRemovable ?? true;
          yield* dispatchAlreadyCoordinated(
            orchestrationEngine,
            {
              type: "thread.meta.update",
              commandId: yield* serverCommandId("bootstrap-thread-meta-update"),
              threadId: command.threadId,
              branch: worktree.worktree.refName,
              worktreePath: targetWorktreePath,
              worktreeRemovable: preparedWorktreeRemovable,
              worktreeRemovalPath: preparedWorktreeRemovable ? worktree.worktree.path : null,
            },
            internalAuthority,
          );
          yield* refreshGitStatus(targetWorktreePath);
        }

        yield* runSetupProgram();

        return yield* dispatchAlreadyCoordinated(
          orchestrationEngine,
          finalTurnStartCommand,
          authority,
        );
      });

      const guardedBootstrapProgram = bootstrapProgram.pipe(
        Effect.catchCause((cause) => {
          const dispatchError = toBootstrapDispatchCommandCauseError(cause);
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.fail(dispatchError);
          }
          if (isArchivedFinalTurnStartCause(cause, command.threadId)) {
            return Effect.fail(dispatchError);
          }
          return cleanupCreatedThread().pipe(
            Effect.flatMap(() => cleanupCreatedWorktree()),
            Effect.flatMap(() => Effect.fail(dispatchError)),
          );
        }),
      );

      return yield* withThreadBootstrapLock(
        command.threadId,
        worktreeLifecycle.withPermit(guardedBootstrapProgram),
      );
    });

    return BootstrapTurnStartDispatcher.of({ dispatch });
  }),
);
