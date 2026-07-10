import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  MessageId,
  ThreadId,
  VcsProcessSpawnError,
  VcsUnsupportedOperationError,
  type ModelSelection,
  type OrchestrationProjectShell,
  type OrchestrationThreadShell,
  type VcsRepositoryIdentity,
} from "@t3tools/contracts";
import { buildTemporaryWorktreeBranchName } from "@t3tools/shared/git";
import {
  buildProviderOptionSelectionsFromDescriptors,
  pickModelSelectionFromInstances,
  type ProviderModelSource,
} from "@t3tools/shared/model";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { resolveThreadWorkspaceCwd } from "../../../checkpointing/Utils.ts";
import * as BootstrapTurnStartDispatcher from "../../../orchestration/Services/BootstrapTurnStartDispatcher.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderInstanceRegistry } from "../../../provider/Services/ProviderInstanceRegistry.ts";
import { GitWorkflowService } from "../../../git/GitWorkflowService.ts";
import * as VcsDriverRegistry from "../../../vcs/VcsDriverRegistry.ts";
import {
  ThreadStartToolError,
  type ThreadStartInternalInput,
  type ThreadStartMode,
  type ThreadStartPublicInput,
  type ThreadStartToolOutput,
  ThreadToolkit,
} from "./tools.ts";
import { applyMcpReasoningEffort } from "./reasoningEffort.ts";

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
const isThreadStartToolError = Schema.is(ThreadStartToolError);
const isVcsProcessSpawnError = Schema.is(VcsProcessSpawnError);
const isVcsUnsupportedOperationError = Schema.is(VcsUnsupportedOperationError);

const fail = (message: string) => new ThreadStartToolError({ message });

const isMissingCwdSpawnError = (error: unknown, cwd: string): boolean => {
  if (!isVcsProcessSpawnError(error) || error.cwd !== cwd) return false;
  const cause = error.cause;
  if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") {
    return true;
  }
  return (
    cause instanceof PlatformError.PlatformError &&
    cause.reason._tag === "NotFound" &&
    cause.reason.module === "ChildProcess" &&
    (cause.reason.method === "spawn" || cause.reason.syscall === "chdir")
  );
};

const truncateTitle = (value: string): string => {
  const trimmed = value.trim().replace(/\s+/g, " ");
  if (trimmed.length === 0) return "New thread";
  return trimmed.length <= 80 ? trimmed : `${trimmed.slice(0, 77)}...`;
};

const resolveOption = <A>(
  option: Option.Option<A>,
  message: string,
): Effect.Effect<A, ThreadStartToolError> =>
  Option.match(option, {
    onNone: () => Effect.fail(fail(message)),
    onSome: Effect.succeed,
  });

const bytesToHex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

const WORKTREE_DEGRADE_WARNING =
  "Project directory is not a Git repository; requested worktree mode was ignored and the child was started on the current checkout without a worktree. Concurrent writes may conflict.";

interface SourceCwdProjectMatch {
  readonly belongsToProject: boolean;
  readonly workspaceRelativePath: string | null;
}

interface SourceCwdProjectContext {
  readonly usable: boolean;
  readonly workspaceRelativePath: string | null;
}

export type ActiveThreadStartRuntime = (
  input: ThreadStartInternalInput,
  invocation: McpInvocationContext.McpInvocationScope,
) => Effect.Effect<ThreadStartToolOutput, ThreadStartToolError>;

let activeThreadStartRuntime: ActiveThreadStartRuntime | null = null;

/** Reach the live thread-start runtime from sub-agent tool handlers (mirrors `dispatchActive`). */
export const activeThreadStartRuntimeOf = (): ActiveThreadStartRuntime | null =>
  activeThreadStartRuntime;

const makeActiveThreadStartRuntime = Effect.fn("ThreadToolkit.makeActiveRuntime")(function* () {
  const crypto = yield* Crypto.Crypto;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const providerInstanceRegistry = yield* ProviderInstanceRegistry;
  const gitWorkflow = yield* GitWorkflowService;
  const vcsDriverRegistry = yield* VcsDriverRegistry.VcsDriverRegistry;
  const path = yield* Path.Path;
  const fileSystem = yield* FileSystem.FileSystem;
  const uuid = () => crypto.randomUUIDv4.pipe(Effect.orDie);

  // Explicit spawn-base override (`directory` input): the caller directs where the
  // child runs, so validation is strict and failures are agent-legible instead of
  // silently degrading to the project root.
  const validateExplicitDirectory = Effect.fn("ThreadToolkit.validateExplicitDirectory")(function* (
    directory: string,
  ) {
    if (!path.isAbsolute(directory)) {
      return yield* fail(`directory must be an absolute path (got "${directory}").`);
    }
    const normalized = path.normalize(directory);
    const resolved = yield* fileSystem
      .realPath(normalized)
      .pipe(
        Effect.mapError(() =>
          fail(`directory "${normalized}" does not exist or is not accessible.`),
        ),
      );
    const info = yield* fileSystem
      .stat(resolved)
      .pipe(
        Effect.mapError(() =>
          fail(`directory "${normalized}" does not exist or is not accessible.`),
        ),
      );
    if (info.type !== "Directory") {
      return yield* fail(`directory "${normalized}" is not a directory.`);
    }
    return path.normalize(resolved);
  });

  // Fail-CLOSED same-repository check for explicit directories: any detection
  // failure counts as foreign, because the foreign-directory restrictions
  // (no setup script, no auto-cleanup, no project branch fallback) are the safe
  // side. The fail-open semantics of sourceCwdProjectContext are only
  // appropriate for the source thread's own cwd.
  const explicitDirectoryRepositoryContext = Effect.fn(
    "ThreadToolkit.explicitDirectoryRepositoryContext",
  )(function* (projectRoot: string, candidate: string) {
    const candidateHandle = yield* vcsDriverRegistry
      .detect({ cwd: candidate, cache: "bypass" })
      .pipe(Effect.orElseSucceed(() => null));
    if (!candidateHandle) {
      return { sameRepository: false, workspaceRelativePath: null };
    }
    const workspaceRelativePath = workspaceRelativePathFromRepositoryRoot(
      candidateHandle.repository.rootPath,
      candidate,
    );
    const projectHandle = yield* vcsDriverRegistry
      .detect({ cwd: projectRoot })
      .pipe(Effect.orElseSucceed(() => null));
    if (!projectHandle) {
      return { sameRepository: false, workspaceRelativePath };
    }
    return {
      sameRepository:
        repositoryIdentityPath(candidateHandle.repository, candidate) ===
        repositoryIdentityPath(projectHandle.repository, projectRoot),
      workspaceRelativePath,
    };
  });

  const makeIds = Effect.fn("ThreadToolkit.makeIds")(function* () {
    return {
      commandId: CommandId.make(yield* uuid()),
      messageId: MessageId.make(yield* uuid()),
      threadId: ThreadId.make(yield* uuid()),
    };
  });

  const makeTemporaryBranchName = Effect.fn("ThreadToolkit.makeTemporaryBranchName")(function* () {
    const bytes = yield* crypto.randomBytes(4).pipe(Effect.orDie);
    return buildTemporaryWorktreeBranchName((byteLength) =>
      byteLength === 4 ? bytesToHex(bytes) : "",
    );
  });

  const resolveCurrentBranch = Effect.fn("ThreadToolkit.resolveCurrentBranch")(function* (
    cwd: string,
  ) {
    return yield* gitWorkflow.status({ cwd }).pipe(
      Effect.map((status) => status.refName),
      Effect.orElseSucceed(() => null),
    );
  });

  const resolveDefaultBranch = Effect.fn("ThreadToolkit.resolveDefaultBranch")(function* (
    cwd: string,
  ) {
    return yield* gitWorkflow.listRefs({ cwd, limit: 200 }).pipe(
      Effect.map(
        (result) => result.refs.find((ref) => ref.isDefault && !ref.isRemote)?.name ?? null,
      ),
      Effect.orElseSucceed(() => null),
    );
  });

  const repositoryIdentityPath = (
    repository: VcsRepositoryIdentity,
    detectionCwd: string,
  ): string => {
    const metadataPath = repository.metadataPath?.trim();
    const identityPath =
      metadataPath && metadataPath.length > 0 ? metadataPath : repository.rootPath;
    return path.normalize(
      path.isAbsolute(identityPath) ? identityPath : path.resolve(detectionCwd, identityPath),
    );
  };

  const isPathWithin = (parent: string, candidate: string): boolean => {
    const relative = path.relative(path.normalize(parent), path.normalize(candidate));
    return (
      relative === "" ||
      (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))
    );
  };

  const workspaceRelativePathFromRepositoryRoot = (
    repositoryRoot: string,
    candidate: string,
  ): string | null => {
    const relative = path.relative(path.normalize(repositoryRoot), path.normalize(candidate));
    if (
      relative === "" ||
      relative === "." ||
      relative === ".." ||
      relative.startsWith(`..${path.sep}`) ||
      path.isAbsolute(relative)
    ) {
      return null;
    }
    return relative;
  };

  const sourceCwdBelongsToProject = Effect.fn("ThreadToolkit.sourceCwdBelongsToProject")(function* (
    projectRoot: string,
    candidate: string,
  ) {
    const candidateHandle = yield* vcsDriverRegistry.detect({ cwd: candidate, cache: "bypass" });
    if (!candidateHandle) {
      return {
        belongsToProject: false,
        workspaceRelativePath: null,
      } satisfies SourceCwdProjectMatch;
    }

    const workspaceRelativePath = workspaceRelativePathFromRepositoryRoot(
      candidateHandle.repository.rootPath,
      candidate,
    );

    const projectDetection = yield* vcsDriverRegistry.detect({ cwd: projectRoot }).pipe(
      Effect.matchEffect({
        onFailure: () => Effect.succeed({ _tag: "failure" as const }),
        onSuccess: (handle) => Effect.succeed({ _tag: "success" as const, handle }),
      }),
    );
    if (projectDetection._tag === "failure") {
      return {
        belongsToProject: true,
        workspaceRelativePath,
      } satisfies SourceCwdProjectMatch;
    }

    const projectHandle = projectDetection.handle;
    if (!projectHandle) {
      return {
        belongsToProject: isPathWithin(projectRoot, candidateHandle.repository.rootPath),
        workspaceRelativePath,
      } satisfies SourceCwdProjectMatch;
    }

    return {
      belongsToProject:
        candidateHandle.kind === projectHandle.kind &&
        repositoryIdentityPath(candidateHandle.repository, candidate) ===
          repositoryIdentityPath(projectHandle.repository, projectRoot),
      workspaceRelativePath,
    } satisfies SourceCwdProjectMatch;
  });

  const cwdHasGitRepository = Effect.fn("ThreadToolkit.cwdHasGitRepository")(function* (
    cwd: string,
  ) {
    return yield* vcsDriverRegistry.detect({ cwd, cache: "bypass" }).pipe(
      Effect.map((handle) => handle?.kind === "git"),
      Effect.catch((error) => Effect.succeed(!isVcsUnsupportedOperationError(error))),
    );
  });

  const workspaceRelativePathForCwd = Effect.fn("ThreadToolkit.workspaceRelativePathForCwd")(
    function* (cwd: string, cache: "allow" | "bypass" = "allow") {
      const handle = yield* vcsDriverRegistry
        .detect(cache === "bypass" ? { cwd, cache } : { cwd })
        .pipe(Effect.orElseSucceed(() => null));
      return handle
        ? workspaceRelativePathFromRepositoryRoot(handle.repository.rootPath, cwd)
        : null;
    },
  );

  const sourceCwdProjectContext = Effect.fn("ThreadToolkit.sourceCwdProjectContext")(function* (
    projectRoot: string,
    candidate: string,
  ) {
    return yield* sourceCwdBelongsToProject(projectRoot, candidate).pipe(
      Effect.matchEffect({
        onFailure: (error) => {
          const usable = !isMissingCwdSpawnError(error, candidate);
          const fallback: Effect.Effect<SourceCwdProjectContext> = usable
            ? Effect.map(
                workspaceRelativePathForCwd(candidate, "bypass"),
                (workspaceRelativePath) =>
                  ({
                    usable: true,
                    workspaceRelativePath,
                  }) satisfies SourceCwdProjectContext,
              )
            : Effect.succeed({
                usable: false,
                workspaceRelativePath: null,
              } satisfies SourceCwdProjectContext);
          return fallback;
        },
        onSuccess: (match) =>
          Effect.succeed({
            usable: match.belongsToProject,
            workspaceRelativePath: match.belongsToProject ? match.workspaceRelativePath : null,
          } satisfies SourceCwdProjectContext),
      }),
    );
  });

  const resolveSourceCwd = Effect.fn("ThreadToolkit.resolveSourceCwd")(function* (
    project: OrchestrationProjectShell,
    sourceThread: OrchestrationThreadShell,
  ) {
    const candidate = resolveThreadWorkspaceCwd({ thread: sourceThread, projects: [project] });
    if (!candidate || candidate === project.workspaceRoot) {
      return {
        cwd: project.workspaceRoot,
        canUseSourceBranch: true,
        workspaceRelativePath: yield* workspaceRelativePathForCwd(project.workspaceRoot),
      };
    }
    const sourceContext = yield* sourceCwdProjectContext(project.workspaceRoot, candidate);
    if (sourceContext.usable) {
      return {
        cwd: candidate,
        canUseSourceBranch: true,
        workspaceRelativePath: sourceContext.workspaceRelativePath,
      };
    }
    return {
      cwd: project.workspaceRoot,
      canUseSourceBranch: false,
      workspaceRelativePath: yield* workspaceRelativePathForCwd(project.workspaceRoot),
    };
  });

  const resolveNewWorktreeBaseBranch = Effect.fn("ThreadToolkit.resolveNewWorktreeBaseBranch")(
    function* (
      input: ThreadStartInternalInput,
      project: OrchestrationProjectShell,
      sourceThread: OrchestrationThreadShell,
      sourceCwd: string,
      canUseSourceBranch: boolean,
      // An explicit `directory` may name a DIFFERENT repository than the caller's
      // project; falling back to the project checkout there could select an
      // unrelated ref that happens to share a name. Explicit directories must
      // resolve their base branch from the target repository alone.
      allowProjectFallback: boolean,
    ) {
      const sourceBranch = canUseSourceBranch ? sourceThread.branch : null;
      if (input.baseBranch) return input.baseBranch;
      if (input.baseBranchSource === "source" && sourceBranch) return sourceBranch;

      const defaultBranch = yield* resolveDefaultBranch(sourceCwd);
      if (defaultBranch) return defaultBranch;
      if (sourceBranch) return sourceBranch;

      const currentBranch = yield* resolveCurrentBranch(sourceCwd);
      if (currentBranch) return currentBranch;

      if (allowProjectFallback && project.workspaceRoot !== sourceCwd) {
        const projectDefaultBranch = yield* resolveDefaultBranch(project.workspaceRoot);
        if (projectDefaultBranch) return projectDefaultBranch;

        const projectCurrentBranch = yield* resolveCurrentBranch(project.workspaceRoot);
        if (projectCurrentBranch) return projectCurrentBranch;
      }

      return yield* fail(
        allowProjectFallback
          ? "Could not resolve a base branch for the new worktree."
          : `Could not resolve a base branch in directory "${sourceCwd}"; pass baseBranch explicitly.`,
      );
    },
  );

  const resolveInitialBranch = Effect.fn("ThreadToolkit.resolveInitialBranch")(function* (
    mode: ThreadStartMode,
    input: ThreadStartInternalInput,
    sourceThread: OrchestrationThreadShell,
    sourceCwd: string,
    canUseSourceBranch: boolean,
  ) {
    if (input.branch) return input.branch;
    if (mode === "new_worktree") return yield* makeTemporaryBranchName();
    if (mode === "existing_worktree") {
      if (!input.worktreePath) {
        return yield* fail("existing_worktree mode requires worktreePath.");
      }
      return yield* resolveCurrentBranch(input.worktreePath);
    }
    return canUseSourceBranch && sourceThread.branch
      ? sourceThread.branch
      : yield* resolveCurrentBranch(sourceCwd);
  });

  const loadProviderSourceContext = Effect.fn("ThreadToolkit.loadProviderSourceContext")(function* (
    invocation: McpInvocationContext.ProviderMcpInvocationScope,
  ) {
    const sourceThread = yield* projectionSnapshotQuery
      .getThreadShellById(invocation.threadId)
      .pipe(
        Effect.flatMap((thread) =>
          resolveOption(thread, `Source thread ${invocation.threadId} was not found.`),
        ),
        Effect.mapError((error) =>
          isThreadStartToolError(error)
            ? error
            : fail(error instanceof Error ? error.message : "Failed to load source thread."),
        ),
      );
    const project = yield* projectionSnapshotQuery.getProjectShellById(sourceThread.projectId).pipe(
      Effect.flatMap((project) =>
        resolveOption(project, `Project ${sourceThread.projectId} was not found.`),
      ),
      Effect.mapError((error) =>
        isThreadStartToolError(error)
          ? error
          : fail(error instanceof Error ? error.message : "Failed to load source project."),
      ),
    );

    return { sourceThread, project };
  });

  const loadPeerSourceContext = Effect.fn("ThreadToolkit.loadPeerSourceContext")(function* (
    input: ThreadStartInternalInput,
  ) {
    if (input.directory === undefined) {
      return yield* fail(
        "Peer-scoped sub-agent spawn requires directory so the target backend can choose a local project.",
      );
    }
    const explicitDirectory = yield* validateExplicitDirectory(input.directory);
    const snapshot = yield* projectionSnapshotQuery
      .getShellSnapshot()
      .pipe(
        Effect.mapError((error) =>
          fail(error instanceof Error ? error.message : "Failed to load target projects."),
        ),
      );
    const projectMatches = yield* Effect.forEach(snapshot.projects, (candidate) =>
      fileSystem.realPath(candidate.workspaceRoot).pipe(
        Effect.map((realWorkspaceRoot) => ({
          project: candidate,
          realWorkspaceRoot: path.normalize(realWorkspaceRoot),
        })),
        Effect.orElseSucceed(() => null),
      ),
    );
    const project = projectMatches
      .filter(
        (candidate): candidate is NonNullable<typeof candidate> =>
          candidate !== null && isPathWithin(candidate.realWorkspaceRoot, explicitDirectory),
      )
      .toSorted(
        (left, right) => right.realWorkspaceRoot.length - left.realWorkspaceRoot.length,
      )[0]?.project;
    if (project === undefined) {
      return yield* fail(
        `Peer-scoped sub-agent spawn directory "${explicitDirectory}" is not inside an active target project.`,
      );
    }
    let inheritedModelSelection = input.modelSelection ?? project.defaultModelSelection;
    if (inheritedModelSelection === null) {
      if (input.model === undefined) {
        return yield* fail(
          "Peer-scoped sub-agent spawn requires model/modelSelection when the target project has no default model.",
        );
      }
      const providerInstances = yield* providerInstanceRegistry.listInstances;
      const modelSources = yield* Effect.forEach(
        providerInstances.filter((providerInstance) => providerInstance.enabled),
        (providerInstance) =>
          Effect.map(providerInstance.snapshot.getSnapshot, (snapshot) => ({
            instanceId: providerInstance.instanceId,
            driverKind: providerInstance.driverKind,
            models: snapshot.models.map((providerModel) => ({
              slug: providerModel.slug,
              optionDescriptors: providerModel.capabilities?.optionDescriptors,
              defaultOptions: buildProviderOptionSelectionsFromDescriptors(
                providerModel.capabilities?.optionDescriptors,
              ),
            })),
          })),
      );
      inheritedModelSelection = pickModelSelectionFromInstances(
        input.model,
        modelSources,
        undefined,
      );
      if (inheritedModelSelection === null) {
        return yield* fail(
          `Model "${input.model}" is not served by any configured provider. Pass a model shown in the model picker, or configure a default model on the target project.`,
        );
      }
    }
    const createdAt = yield* nowIso;
    const sourceThread: OrchestrationThreadShell = {
      id: ThreadId.make("remote-parent"),
      projectId: project.id,
      title: "Remote parent",
      modelSelection: inheritedModelSelection,
      runtimeMode: input.runtimeMode ?? DEFAULT_RUNTIME_MODE,
      interactionMode: input.interactionMode ?? DEFAULT_PROVIDER_INTERACTION_MODE,
      branch: null,
      worktreePath: null,
      worktreeRemovable: false,
      worktreeRemovalPath: null,
      latestTurn: null,
      createdAt,
      updatedAt: createdAt,
      archivedAt: null,
      session: null,
      latestUserMessageAt: null,
      hasPendingApprovals: false,
      hasPendingUserInput: false,
      hasActionableProposedPlan: false,
      parentThreadId: null,
      parentEnvironmentId: null,
    };
    return { sourceThread, project };
  });

  return Effect.fn("ThreadToolkit.startThread")(function* (
    input: ThreadStartInternalInput,
    invocation: McpInvocationContext.McpInvocationScope,
  ) {
    const { sourceThread, project } = McpInvocationContext.isProviderInvocationScope(invocation)
      ? yield* loadProviderSourceContext(invocation)
      : yield* loadPeerSourceContext(input);
    const requestedMode = input.mode ?? "new_worktree";
    if (input.directory !== undefined && input.worktreePath !== undefined) {
      return yield* fail(
        "Pass either directory or worktreePath, not both: worktreePath already names the exact run directory for existing_worktree mode.",
      );
    }
    const explicitDirectory =
      input.directory !== undefined ? yield* validateExplicitDirectory(input.directory) : null;
    const explicitDirectoryContext =
      explicitDirectory !== null
        ? yield* explicitDirectoryRepositoryContext(project.workspaceRoot, explicitDirectory)
        : null;
    // A FOREIGN explicit directory (different repository than the caller's
    // project, fail-closed) gets the cross-repo restrictions; a same-repo
    // explicit directory keeps cleanup and branch behavior (worktrees are
    // repo-global, so the projectId-keyed reaper still sees them).
    const explicitForeignDirectory =
      explicitDirectory !== null && !(explicitDirectoryContext?.sameRepository ?? false);
    // Setup scripts are stricter than cleanup: they are resolved by the
    // caller's projectId, so they may only run when the directory is BOTH the
    // same repository AND inside the caller project's own workspace. Each
    // condition alone is insufficient: a same-repo monorepo SIBLING package
    // is outside the workspace, and a NESTED foreign checkout (vendored repo /
    // submodule) is inside the workspace but a different repository.
    // Containment is checked on RESOLVED paths (fail-closed on resolution
    // errors) so a symlink under the workspace cannot smuggle the setup script
    // into a target outside it.
    const explicitDirectoryAllowsSetupScript =
      explicitDirectory === null ||
      (!explicitForeignDirectory &&
        (yield* Effect.gen(function* () {
          const realWorkspaceRoot = yield* fileSystem
            .realPath(project.workspaceRoot)
            .pipe(Effect.orElseSucceed(() => null));
          const realDirectory = yield* fileSystem
            .realPath(explicitDirectory)
            .pipe(Effect.orElseSucceed(() => null));
          return (
            realWorkspaceRoot !== null &&
            realDirectory !== null &&
            isPathWithin(realWorkspaceRoot, realDirectory)
          );
        })));
    if (!explicitDirectoryAllowsSetupScript && input.runSetupScript === true) {
      return yield* fail(
        "runSetupScript is not supported when directory is outside the calling project's workspace or in a different repository: setup scripts belong to the calling project and must not run in other checkouts.",
      );
    }
    // An explicit directory overrides the source-thread cwd resolution entirely:
    // the caller directed the spawn base, so project-membership degradation must
    // not silently reroute it.
    const sourceCwdContext =
      explicitDirectory !== null
        ? {
            cwd: explicitDirectory,
            // Never inherit the source thread's branch for an explicit
            // directory: it is a different checkout (even when same-repo) and
            // may be on a different branch, so the branch must be resolved from
            // the target directory itself (resolveInitialBranch queries cwd).
            canUseSourceBranch: false,
            workspaceRelativePath: explicitDirectoryContext?.workspaceRelativePath ?? null,
          }
        : yield* resolveSourceCwd(project, sourceThread);
    const { cwd: sourceCwd, canUseSourceBranch, workspaceRelativePath } = sourceCwdContext;
    if (requestedMode === "existing_worktree" && input.worktreePath !== undefined) {
      // ADA-97: an explicit existing_worktree path is honored even when the
      // project (or the path itself) is not a Git repository — the mode runs in
      // an existing directory and creates nothing, so there is nothing to
      // degrade. Validate it exists instead of silently rerouting to the
      // project root.
      yield* validateExplicitDirectory(input.worktreePath);
    }
    // Only a mode that would CREATE a worktree degrades on a non-Git base
    // (Adam's rule: never create a worktree for a non-Git directory, even if
    // requested). existing_worktree and current_checkout create nothing.
    const shouldUseCurrentCheckout =
      requestedMode === "new_worktree" && !(yield* cwdHasGitRepository(sourceCwd));
    const mode: ThreadStartMode = shouldUseCurrentCheckout ? "current_checkout" : requestedMode;
    const ids = yield* makeIds();
    const createdAt = yield* nowIso;
    const branch =
      (yield* resolveInitialBranch(mode, input, sourceThread, sourceCwd, canUseSourceBranch)) ??
      null;
    const worktreePath: string | null =
      mode === "existing_worktree"
        ? (input.worktreePath ?? null)
        : mode === "current_checkout" && sourceCwd !== project.workspaceRoot
          ? sourceCwd
          : null;
    // Cross-repo worktrees (explicit directory) must not be marked removable:
    // the stale-worktree reaper resolves the repository via the caller's
    // projectId and would never find them there — it would clear the thread
    // metadata and orphan the actual worktree. They are user-managed until
    // cleanup understands foreign repositories.
    const worktreeRemovable = mode === "new_worktree" && !explicitForeignDirectory;
    // Removal-root inheritance only applies to children actually running
    // inside the SOURCE thread's worktree: an explicit directory (even
    // same-repo) inherits only when it lies under the source removal root,
    // otherwise a sibling checkout would carry the source worktree's cleanup
    // identity — suppressing first-turn branch renaming and corrupting removal
    // metadata. Downstream prefers worktreeRemovalPath for shared-worktree
    // decisions, so a wrong inherit is worse than none.
    const sourceRemovalRoot =
      sourceThread.worktreeRemovalPath ??
      (sourceThread.worktreeRemovable === true ? sourceThread.worktreePath : null);
    const worktreeRemovalPath = worktreeRemovable
      ? worktreePath
      : mode === "current_checkout" &&
          worktreePath !== null &&
          (explicitDirectory === null ||
            (sourceRemovalRoot !== null && isPathWithin(sourceRemovalRoot, explicitDirectory)))
        ? sourceRemovalRoot
        : null;
    const title = input.title ?? truncateTitle(input.prompt);
    const titleSeed = input.title === undefined ? title : undefined;
    const providerInstances = yield* providerInstanceRegistry.listInstances;
    const modelSources = yield* Effect.forEach(
      providerInstances.filter((providerInstance) => providerInstance.enabled),
      (providerInstance) =>
        Effect.map(providerInstance.snapshot.getSnapshot, (snapshot) => ({
          instanceId: providerInstance.instanceId,
          driverKind: providerInstance.driverKind,
          models: snapshot.models.map((providerModel) => ({
            slug: providerModel.slug,
            optionDescriptors: providerModel.capabilities?.optionDescriptors,
            defaultOptions: buildProviderOptionSelectionsFromDescriptors(
              providerModel.capabilities?.optionDescriptors,
            ),
          })),
        })),
    );
    const modelSelection = yield* resolveModelSelection(input, sourceThread, modelSources);
    const runtimeMode = input.runtimeMode ?? sourceThread.runtimeMode;
    const interactionMode = input.interactionMode ?? sourceThread.interactionMode;
    const prepareWorktree =
      mode === "new_worktree"
        ? {
            projectCwd: sourceCwd,
            baseBranch: yield* resolveNewWorktreeBaseBranch(
              input,
              project,
              sourceThread,
              sourceCwd,
              canUseSourceBranch,
              !explicitForeignDirectory,
            ),
            branch: branch ?? undefined,
            ...(workspaceRelativePath !== null ? { workspaceRelativePath } : {}),
          }
        : undefined;

    if (mode === "existing_worktree" && !worktreePath) {
      return yield* fail("existing_worktree mode requires worktreePath.");
    }

    yield* BootstrapTurnStartDispatcher.dispatchActive({
      type: "thread.turn.start",
      commandId: ids.commandId,
      threadId: ids.threadId,
      message: {
        messageId: ids.messageId,
        role: "user",
        text: input.prompt,
        attachments: [],
      },
      modelSelection,
      ...(titleSeed !== undefined ? { titleSeed } : {}),
      runtimeMode,
      interactionMode,
      bootstrap: {
        createThread: {
          projectId: project.id,
          title,
          modelSelection,
          runtimeMode,
          interactionMode,
          branch,
          worktreePath,
          worktreeRemovable,
          worktreeRemovalPath,
          createdAt,
        },
        ...(prepareWorktree
          ? {
              prepareWorktree,
              // Setup scripts are resolved by the CALLER's projectId; running one
              // inside an explicitly targeted other repository would execute an
              // unrelated project's setup command there. Suppressed for
              // directory targets (explicit true is rejected at input).
              runSetupScript: explicitDirectoryAllowsSetupScript && (input.runSetupScript ?? true),
            }
          : {}),
      },
      createdAt,
    }).pipe(
      Effect.mapError((error) =>
        fail(error instanceof Error ? error.message : "Failed to start child thread."),
      ),
    );

    return {
      threadId: ids.threadId,
      projectId: project.id,
      mode,
      branch,
      worktreePath,
      ...(shouldUseCurrentCheckout
        ? {
            warning: WORKTREE_DEGRADE_WARNING,
          }
        : mode === "current_checkout"
          ? {
              warning:
                "Child thread was started on the current checkout and may conflict with concurrent writes.",
            }
          : mode === "new_worktree" && explicitForeignDirectory
            ? {
                warning:
                  "Worktree was created in an explicitly targeted repository and is not auto-cleaned; remove it manually when the work is done.",
              }
            : {}),
    };
  });
});

export const ThreadStartRuntimeLive = Layer.effectDiscard(
  Effect.acquireRelease(
    makeActiveThreadStartRuntime().pipe(
      Effect.tap((runtime) =>
        Effect.sync(() => {
          activeThreadStartRuntime = runtime;
        }),
      ),
    ),
    (runtime) =>
      Effect.sync(() => {
        if (activeThreadStartRuntime === runtime) activeThreadStartRuntime = null;
      }),
  ),
);

const resolveModelSelection = (
  input: ThreadStartInternalInput,
  sourceThread: OrchestrationThreadShell,
  modelSources: ReadonlyArray<ProviderModelSource>,
): Effect.Effect<ModelSelection, ThreadStartToolError> => {
  // An explicit bare `model` is resolved against the live provider model lists;
  // if the caller named a model no provider serves, fail loudly rather than
  // silently starting the thread on a different (inherited) model.
  if (input.model !== undefined) {
    const resolved = pickModelSelectionFromInstances(
      input.model,
      modelSources,
      sourceThread.modelSelection.instanceId,
    );
    if (resolved === null) {
      return Effect.fail(
        fail(
          `Model "${input.model}" is not served by any configured provider. Pass a model shown in the model picker, or omit "model" to keep the thread's current model.`,
        ),
      );
    }
    const effort = applyMcpReasoningEffort(resolved, modelSources, input.reasoningEffort);
    return effort.error === undefined
      ? Effect.succeed(effort.selection)
      : Effect.fail(fail(effort.error));
  }
  // Otherwise an explicit modelSelection wins, else inherit the source thread.
  return Effect.succeed(input.modelSelection ?? sourceThread.modelSelection);
};

const startThread = Effect.fn("ThreadToolkit.startThread")(function* (
  input: ThreadStartPublicInput,
) {
  const invocation = yield* McpInvocationContext.requireProviderMcpCapability(
    "thread-management",
  ).pipe(Effect.mapError((error) => fail(error.message)));
  const runtime = activeThreadStartRuntime;
  if (!runtime) return yield* fail("Thread start runtime is not available.");
  return yield* runtime(input, invocation);
});

const handlers = {
  t3_thread_start: startThread,
} satisfies Parameters<typeof ThreadToolkit.toLayer>[0];

export const ThreadToolkitHandlersLive = ThreadToolkit.toLayer(handlers);
