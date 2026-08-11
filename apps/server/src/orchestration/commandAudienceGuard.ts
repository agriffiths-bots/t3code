import type {
  AuthAudienceCeiling,
  DataAudience,
  EnvironmentId,
  OrchestrationCommand,
  OrchestrationProject,
  OrchestrationReadModel,
  OrchestrationThread,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { OrchestrationCommandAudienceAuthorizationError } from "./Errors.ts";
import {
  threadHasPreparedBootstrapWorktree,
  threadMatchesBootstrapCreate,
} from "./bootstrapCommandState.ts";

export type OrchestrationCommandDispatchAuthority =
  | {
      readonly kind: "session";
      readonly subject: string;
      readonly audienceCeiling: AuthAudienceCeiling;
      /**
       * Set only for sessions authenticated by a peer backend token; carries that token's
       * `sourceEnvironmentId`. Absent for every ordinary user session.
       *
       * Provenance only — NO authorization decision reads this field. Environment identity is not
       * audience identity, so a matching environment grants nothing (see
       * `requireRemoteParentAllowed`). Do not reintroduce a permit keyed on it; authenticating a
       * remote parent's audience is tracked as ADA-184.
       */
      readonly peerSourceEnvironmentId?: EnvironmentId;
    }
  | {
      readonly kind: "trusted-system";
      readonly reason: string;
    }
  | {
      readonly kind: "audience-bound-system";
      readonly reason: string;
      readonly sourceThreadId: ThreadId;
      readonly dataAudience: DataAudience;
    };

export function sessionDispatchAuthority(input: {
  readonly subject: string;
  readonly audienceCeiling: AuthAudienceCeiling;
}): OrchestrationCommandDispatchAuthority {
  return {
    kind: "session",
    subject: input.subject,
    audienceCeiling: input.audienceCeiling,
  };
}

/**
 * Authority for a session authenticated by a peer backend token. Identical to a session authority
 * except that it records, as provenance, the environment the peer authenticated as. That record
 * grants no additional capability: a peer authority is authorized exactly like any other session
 * authority of the same `audienceCeiling`.
 */
export function peerSessionDispatchAuthority(input: {
  readonly subject: string;
  readonly audienceCeiling: AuthAudienceCeiling;
  readonly sourceEnvironmentId: EnvironmentId;
}): OrchestrationCommandDispatchAuthority {
  return {
    kind: "session",
    subject: input.subject,
    audienceCeiling: input.audienceCeiling,
    peerSourceEnvironmentId: input.sourceEnvironmentId,
  };
}

export function trustedSystemDispatchAuthority(
  reason: string,
): OrchestrationCommandDispatchAuthority {
  return {
    kind: "trusted-system",
    reason,
  };
}

export function audienceBoundSystemDispatchAuthority(input: {
  readonly reason: string;
  readonly sourceThreadId: ThreadId;
  readonly dataAudience: DataAudience;
}): OrchestrationCommandDispatchAuthority {
  return {
    kind: "audience-bound-system",
    reason: input.reason,
    sourceThreadId: input.sourceThreadId,
    dataAudience: input.dataAudience,
  };
}

export function threadAudienceSystemDispatchAuthority(
  thread: Pick<OrchestrationThread, "id" | "dataAudience">,
  reason: string,
): OrchestrationCommandDispatchAuthority {
  return audienceBoundSystemDispatchAuthority({
    reason,
    sourceThreadId: thread.id,
    dataAudience: thread.dataAudience,
  });
}

export function orchestrationCommandAggregateRef(command: OrchestrationCommand):
  | {
      readonly aggregateKind: "project";
      readonly aggregateId: ProjectId;
    }
  | {
      readonly aggregateKind: "thread";
      readonly aggregateId: ThreadId;
    } {
  switch (command.type) {
    case "project.create":
    case "project.meta.update":
    case "project.data-audience.set":
    case "project.delete":
      return {
        aggregateKind: "project",
        aggregateId: command.projectId,
      };
    default:
      return {
        aggregateKind: "thread",
        aggregateId: command.threadId,
      };
  }
}

function projectNotFoundDetail(command: OrchestrationCommand, projectId: ProjectId): string {
  return `Project '${projectId}' does not exist for command '${command.type}'.`;
}

function threadNotFoundDetail(command: OrchestrationCommand, threadId: ThreadId): string {
  return `Thread '${threadId}' does not exist for command '${command.type}'.`;
}

function failAuthorization(command: OrchestrationCommand, detail: string) {
  return Effect.fail(
    new OrchestrationCommandAudienceAuthorizationError({
      commandType: command.type,
      detail,
    }),
  );
}

function isFactorySessionAuthority(authority: OrchestrationCommandDispatchAuthority): boolean {
  return authority.kind === "session" && authority.audienceCeiling === "factory";
}

function hasThreadCreateWorktreeMetadata(
  command: Extract<OrchestrationCommand, { type: "thread.create" }>,
): boolean {
  return (
    command.worktreePath !== null ||
    (command.worktreeRemovalPath ?? null) !== null ||
    command.worktreeRemovable === true
  );
}

function hasThreadMetaWorktreeMutation(
  command: Extract<OrchestrationCommand, { type: "thread.meta.update" }>,
): boolean {
  return (
    command.worktreePath !== undefined ||
    command.worktreeRemovalPath !== undefined ||
    command.worktreeRemovable !== undefined
  );
}

function findProject(
  readModel: OrchestrationReadModel,
  projectId: ProjectId,
): OrchestrationProject | undefined {
  return readModel.projects.find((project) => project.id === projectId);
}

function findThread(
  readModel: OrchestrationReadModel,
  threadId: ThreadId,
): OrchestrationThread | undefined {
  return readModel.threads.find((thread) => thread.id === threadId);
}

const canonicalPathForComparison = Effect.fn("canonicalPathForComparison")(function* (
  inputPath: string,
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
) {
  let existingAncestor = path.resolve(inputPath);
  const missingSegments: string[] = [];

  while (true) {
    const realPathResult = yield* fileSystem.realPath(existingAncestor).pipe(
      Effect.match({
        onFailure: (cause) => ({ _tag: "failure" as const, cause }),
        onSuccess: (realPath) => ({ _tag: "success" as const, realPath }),
      }),
    );
    if (realPathResult._tag === "success") {
      return normalizeProjectPathForComparison(
        path.join(realPathResult.realPath, ...missingSegments),
      ).replaceAll("\\", "/");
    }
    if (realPathResult.cause.reason._tag !== "NotFound") return null;
    const readLinkResult = yield* fileSystem.readLink(existingAncestor).pipe(
      Effect.match({
        onFailure: (cause) => ({ _tag: "failure" as const, cause }),
        onSuccess: (target) => ({ _tag: "success" as const, target }),
      }),
    );
    if (readLinkResult._tag === "success") return null;
    if (readLinkResult.cause.reason._tag !== "NotFound") return null;
    const parent = path.dirname(existingAncestor);
    if (parent === existingAncestor) return null;
    missingSegments.unshift(path.basename(existingAncestor));
    existingAncestor = parent;
  }
});

const canonicalPathsEqual = Effect.fn("canonicalPathsEqual")(function* (
  leftPath: string | null,
  rightPath: string | null,
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
) {
  if (leftPath === null || rightPath === null) return leftPath === rightPath;
  const canonicalLeft = yield* canonicalPathForComparison(leftPath, fileSystem, path);
  const canonicalRight = yield* canonicalPathForComparison(rightPath, fileSystem, path);
  return canonicalLeft !== null && canonicalRight !== null && canonicalLeft === canonicalRight;
});

const isCanonicalPathBinding = Effect.fn("isCanonicalPathBinding")(function* (
  inputPath: string,
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
) {
  const canonicalPath = yield* canonicalPathForComparison(inputPath, fileSystem, path);
  const lexicalPath = normalizeProjectPathForComparison(path.resolve(inputPath)).replaceAll(
    "\\",
    "/",
  );
  return canonicalPath !== null && canonicalPath === lexicalPath;
});

const findActiveProjectContainingPath = Effect.fn("findActiveProjectContainingPath")(
  function* (input: {
    readonly readModel: OrchestrationReadModel;
    readonly candidatePath: string;
    readonly fileSystem: FileSystem.FileSystem;
    readonly path: Path.Path;
  }) {
    const canonicalCandidate = yield* canonicalPathForComparison(
      input.candidatePath,
      input.fileSystem,
      input.path,
    );
    if (canonicalCandidate === null) return undefined;
    const candidates: Array<{ project: OrchestrationProject; canonicalRoot: string }> = [];
    for (const project of input.readModel.projects) {
      if (project.deletedAt !== null) continue;
      const canonicalRoot = yield* canonicalPathForComparison(
        project.workspaceRoot,
        input.fileSystem,
        input.path,
      );
      if (canonicalRoot !== null && pathContainsCanonical(canonicalRoot, canonicalCandidate)) {
        candidates.push({ project, canonicalRoot });
      }
    }
    return candidates.toSorted(
      (left, right) => right.canonicalRoot.length - left.canonicalRoot.length,
    )[0]?.project;
  },
);

function pathContainsCanonical(normalizedRoot: string, normalizedCandidate: string): boolean {
  return (
    normalizedCandidate === normalizedRoot ||
    normalizedCandidate.startsWith(
      normalizedRoot.endsWith("/") ? normalizedRoot : `${normalizedRoot}/`,
    )
  );
}

const pathsOverlap = Effect.fn("pathsOverlap")(function* (
  leftPath: string,
  rightPath: string,
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
) {
  const canonicalLeft = yield* canonicalPathForComparison(leftPath, fileSystem, path);
  const canonicalRight = yield* canonicalPathForComparison(rightPath, fileSystem, path);
  if (canonicalLeft === null || canonicalRight === null) return null;
  return (
    pathContainsCanonical(canonicalLeft, canonicalRight) ||
    pathContainsCanonical(canonicalRight, canonicalLeft)
  );
});

const hasHiddenAudiencePathCollision = Effect.fn("hasHiddenAudiencePathCollision")(
  function* (input: {
    readonly readModel: OrchestrationReadModel;
    readonly authority: OrchestrationCommandDispatchAuthority;
    readonly candidatePath: string | null | undefined;
    readonly fileSystem: FileSystem.FileSystem;
    readonly path: Path.Path;
  }) {
    if (input.candidatePath == null) return false;
    const candidatePath = input.candidatePath;
    // A caller-selected path that cannot be canonicalized is never authorized:
    // lexical fallback would reintroduce symlink and inaccessible-ancestor gaps.
    if ((yield* canonicalPathForComparison(candidatePath, input.fileSystem, input.path)) === null) {
      return true;
    }
    for (const project of input.readModel.projects) {
      if (project.deletedAt !== null || canAccessAudience(input.authority, project.dataAudience)) {
        continue;
      }
      const overlap = yield* pathsOverlap(
        project.workspaceRoot,
        candidatePath,
        input.fileSystem,
        input.path,
      );
      if (overlap !== false) {
        return true;
      }
    }

    // Deleted and archived thread rows intentionally remain in the complete
    // projection, and their worktrees may still exist after failed cleanup.
    // Keep them in this collision check so a bootstrap retry cannot run setup
    // inside a hidden checkout merely because its owning thread is inactive.
    for (const thread of input.readModel.threads) {
      if (canAccessAudience(input.authority, thread.dataAudience)) continue;
      for (const hiddenPath of [thread.worktreePath, thread.worktreeRemovalPath]) {
        if (hiddenPath === null || hiddenPath === undefined) continue;
        const overlap = yield* pathsOverlap(
          hiddenPath,
          candidatePath,
          input.fileSystem,
          input.path,
        );
        if (overlap !== false) return true;
      }
    }
    return false;
  },
);

function collectUnarchivedDescendantThreads(
  readModel: OrchestrationReadModel,
  rootThreadId: ThreadId,
): ReadonlyArray<OrchestrationThread> {
  const threadById = new Map<ThreadId, OrchestrationThread>();
  const childrenByParent = new Map<ThreadId, OrchestrationThread[]>();
  for (const thread of readModel.threads) {
    if (thread.deletedAt !== null) continue;
    threadById.set(thread.id, thread);
    const parentId = thread.parentThreadId ?? null;
    if (parentId === null) continue;
    const siblings = childrenByParent.get(parentId) ?? [];
    siblings.push(thread);
    childrenByParent.set(parentId, siblings);
  }

  const collected: OrchestrationThread[] = [];
  const seen = new Set<ThreadId>([rootThreadId]);
  const queue: ThreadId[] = [rootThreadId];
  while (queue.length > 0) {
    const current = queue.shift() as ThreadId;
    for (const child of childrenByParent.get(current) ?? []) {
      if (seen.has(child.id)) continue;
      seen.add(child.id);
      queue.push(child.id);
      const canonicalChild = threadById.get(child.id) ?? child;
      if (canonicalChild.archivedAt !== null) continue;
      collected.push(canonicalChild);
    }
  }
  return collected;
}

function createdProjectAudience(authority: OrchestrationCommandDispatchAuthority): DataAudience {
  if (authority.kind === "audience-bound-system") return authority.dataAudience;
  return authority.kind === "session" && authority.audienceCeiling === "factory"
    ? "factory"
    : "private";
}

function canAccessAudience(
  authority: OrchestrationCommandDispatchAuthority,
  audience: DataAudience,
): boolean {
  if (authority.kind === "audience-bound-system") {
    return authority.dataAudience === audience;
  }
  return (
    authority.kind === "trusted-system" ||
    authority.audienceCeiling === "private" ||
    audience === "factory"
  );
}

function requireProjectAudience(input: {
  readonly command: OrchestrationCommand;
  readonly readModel: OrchestrationReadModel;
  readonly authority: OrchestrationCommandDispatchAuthority;
  readonly projectId: ProjectId;
}): Effect.Effect<OrchestrationProject, OrchestrationCommandAudienceAuthorizationError> {
  const project = findProject(input.readModel, input.projectId);
  if (project === undefined) {
    return failAuthorization(input.command, projectNotFoundDetail(input.command, input.projectId));
  }
  if (!canAccessAudience(input.authority, project.dataAudience)) {
    return failAuthorization(input.command, projectNotFoundDetail(input.command, input.projectId));
  }
  return Effect.succeed(project);
}

function requireThreadAudience(input: {
  readonly command: OrchestrationCommand;
  readonly readModel: OrchestrationReadModel;
  readonly authority: OrchestrationCommandDispatchAuthority;
  readonly threadId: ThreadId;
}): Effect.Effect<OrchestrationThread, OrchestrationCommandAudienceAuthorizationError> {
  const thread = findThread(input.readModel, input.threadId);
  if (thread === undefined) {
    return failAuthorization(input.command, threadNotFoundDetail(input.command, input.threadId));
  }
  if (!canAccessAudience(input.authority, thread.dataAudience)) {
    return failAuthorization(input.command, threadNotFoundDetail(input.command, input.threadId));
  }
  return Effect.succeed(thread);
}

export const authorizeOrchestrationCommandReceiptReplay = Effect.fn(
  "authorizeOrchestrationCommandReceiptReplay",
)(function* (input: {
  readonly command: OrchestrationCommand;
  readonly readModel: OrchestrationReadModel;
  readonly authority: OrchestrationCommandDispatchAuthority | undefined;
  readonly receipt: {
    readonly aggregateKind: "project" | "thread";
    readonly aggregateId: ProjectId | ThreadId;
  };
}) {
  const authority = input.authority;
  if (authority === undefined) {
    return yield* failAuthorization(input.command, "Orchestration dispatch authority is required.");
  }
  const aggregateRef = orchestrationCommandAggregateRef(input.command);
  if (
    input.receipt.aggregateKind !== aggregateRef.aggregateKind ||
    input.receipt.aggregateId !== aggregateRef.aggregateId
  ) {
    return yield* failAuthorization(
      input.command,
      aggregateRef.aggregateKind === "project"
        ? projectNotFoundDetail(input.command, aggregateRef.aggregateId)
        : threadNotFoundDetail(input.command, aggregateRef.aggregateId),
    );
  }

  if (aggregateRef.aggregateKind === "project") {
    yield* requireProjectAudience({
      command: input.command,
      readModel: input.readModel,
      authority,
      projectId: aggregateRef.aggregateId,
    });
  } else {
    yield* requireThreadAudience({
      command: input.command,
      readModel: input.readModel,
      authority,
      threadId: aggregateRef.aggregateId,
    });
  }
});

function requireSameThreadAudience(input: {
  readonly command: OrchestrationCommand;
  readonly referenceThreadId: ThreadId;
  readonly referenceAudience: DataAudience;
  readonly targetAudience: DataAudience;
}): Effect.Effect<void, OrchestrationCommandAudienceAuthorizationError> {
  if (input.referenceAudience !== input.targetAudience) {
    return failAuthorization(
      input.command,
      threadNotFoundDetail(input.command, input.referenceThreadId),
    );
  }
  return Effect.void;
}

function requireRemoteParentAllowed(input: {
  readonly command: OrchestrationCommand;
  readonly authority: OrchestrationCommandDispatchAuthority;
  readonly parentThreadId: ThreadId;
}): Effect.Effect<void, OrchestrationCommandAudienceAuthorizationError> {
  // There is deliberately no environment-match permit here. Environment identity is NOT audience
  // identity: threads of every audience share an environment, so a caller whose peer token
  // authenticated as the parent's environment has still proven nothing about its right to reach
  // that parent's data. This branch runs only when the parent is absent from the local read model,
  // so the parent's `dataAudience` is unreadable and the caller's ceiling is the only segregation
  // left — hence the ceiling check below is the whole decision, and it fails closed.
  //
  // What this costs, stated plainly: every peer-scoped remote sub-agent spawn arrives here with a
  // factory ceiling (subagent/handlers.ts hardcodes it), so this refusal currently disables that
  // shipped capability end to end — the caller gets a masked not-found and the started child is
  // deleted. That is a deliberate trade, not a bug; restoring the capability requires
  // authenticating the remote parent's audience rather than trusting the caller's ceiling
  // (ADA-184). Non-factory ceilings are unaffected, which is what the companion permit test pins.
  if (
    (input.authority.kind === "session" && input.authority.audienceCeiling === "factory") ||
    (input.authority.kind === "audience-bound-system" && input.authority.dataAudience === "factory")
  ) {
    return failAuthorization(
      input.command,
      threadNotFoundDetail(input.command, input.parentThreadId),
    );
  }
  return Effect.void;
}

function bindProjectCreate(
  command: Extract<OrchestrationCommand, { type: "project.create" }>,
  authority: OrchestrationCommandDispatchAuthority,
): OrchestrationCommand {
  const bound = {
    ...command,
    dataAudience: createdProjectAudience(authority),
  };
  return bound;
}

function requireSameBootstrapProjectAsThread(input: {
  readonly command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>;
  readonly thread: OrchestrationThread;
  readonly project: OrchestrationProject;
}): Effect.Effect<void, OrchestrationCommandAudienceAuthorizationError> {
  if (
    input.thread.projectId !== input.project.id ||
    input.thread.dataAudience !== input.project.dataAudience
  ) {
    return failAuthorization(input.command, threadNotFoundDetail(input.command, input.thread.id));
  }
  return Effect.void;
}

const requireBootstrapSideEffectsAuthorized = Effect.fn("requireBootstrapSideEffectsAuthorized")(
  function* (input: {
    readonly command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>;
    readonly readModel: OrchestrationReadModel;
    readonly authority: OrchestrationCommandDispatchAuthority;
    readonly targetProject: OrchestrationProject | undefined;
    readonly existingThread: OrchestrationThread | undefined;
    readonly fileSystem: FileSystem.FileSystem;
    readonly path: Path.Path;
  }) {
    const bootstrap = input.command.bootstrap;
    if (bootstrap === undefined) {
      return;
    }
    if (!isFactorySessionAuthority(input.authority)) {
      return;
    }

    const createThreadWorktreePath = bootstrap.createThread?.worktreePath ?? null;
    const createThreadWorktreeRemovalPath = bootstrap.createThread?.worktreeRemovalPath ?? null;
    if (createThreadWorktreePath !== null || createThreadWorktreeRemovalPath !== null) {
      if (input.targetProject === undefined) {
        return yield* failAuthorization(
          input.command,
          threadNotFoundDetail(input.command, input.command.threadId),
        );
      }
      return yield* failAuthorization(
        input.command,
        projectNotFoundDetail(input.command, input.targetProject.id),
      );
    }

    if (bootstrap.runSetupScript === true) {
      if (input.existingThread !== undefined) {
        const createThread = bootstrap.createThread;
        const prepareWorktree = bootstrap.prepareWorktree;
        const worktreePathsMatch =
          createThread !== undefined &&
          (yield* canonicalPathsEqual(
            input.existingThread.worktreePath,
            createThread.worktreePath,
            input.fileSystem,
            input.path,
          ));
        const matchesPrePreparationState =
          createThread !== undefined &&
          input.existingThread.branch === createThread.branch &&
          worktreePathsMatch;
        const compatibleRetry =
          createThread !== undefined &&
          threadMatchesBootstrapCreate(
            input.existingThread,
            createThread,
            prepareWorktree !== undefined,
            worktreePathsMatch,
          ) &&
          (prepareWorktree === undefined ||
            matchesPrePreparationState ||
            threadHasPreparedBootstrapWorktree(
              input.existingThread,
              createThread,
              prepareWorktree,
              worktreePathsMatch,
            ));
        const hiddenWorktreeCollision = yield* hasHiddenAudiencePathCollision({
          readModel: input.readModel,
          authority: input.authority,
          candidatePath: input.existingThread.worktreePath,
          fileSystem: input.fileSystem,
          path: input.path,
        });
        const hiddenRemovalPathCollision = yield* hasHiddenAudiencePathCollision({
          readModel: input.readModel,
          authority: input.authority,
          candidatePath: input.existingThread.worktreeRemovalPath,
          fileSystem: input.fileSystem,
          path: input.path,
        });
        const worktreePathIsCanonical =
          input.existingThread.worktreePath === null ||
          (yield* isCanonicalPathBinding(
            input.existingThread.worktreePath,
            input.fileSystem,
            input.path,
          ));
        const removalPathIsCanonical =
          input.existingThread.worktreeRemovalPath == null ||
          (yield* isCanonicalPathBinding(
            input.existingThread.worktreeRemovalPath,
            input.fileSystem,
            input.path,
          ));
        if (
          !compatibleRetry ||
          hiddenWorktreeCollision ||
          hiddenRemovalPathCollision ||
          !worktreePathIsCanonical ||
          !removalPathIsCanonical
        ) {
          return yield* failAuthorization(
            input.command,
            threadNotFoundDetail(input.command, input.command.threadId),
          );
        }
      }
      if (input.targetProject === undefined) {
        return yield* failAuthorization(
          input.command,
          threadNotFoundDetail(input.command, input.command.threadId),
        );
      }
    }

    const prepareWorktree = bootstrap.prepareWorktree;
    if (prepareWorktree === undefined) {
      return;
    }

    const targetProject = input.targetProject;
    if (targetProject === undefined) {
      return yield* failAuthorization(
        input.command,
        threadNotFoundDetail(input.command, input.command.threadId),
      );
    }

    const canonicalProjectCwd = yield* canonicalPathForComparison(
      prepareWorktree.projectCwd,
      input.fileSystem,
      input.path,
    );
    if (canonicalProjectCwd === null) {
      return yield* failAuthorization(
        input.command,
        projectNotFoundDetail(input.command, targetProject.id),
      );
    }

    if (
      yield* hasHiddenAudiencePathCollision({
        readModel: input.readModel,
        authority: input.authority,
        candidatePath: canonicalProjectCwd,
        fileSystem: input.fileSystem,
        path: input.path,
      })
    ) {
      return yield* failAuthorization(
        input.command,
        projectNotFoundDetail(input.command, targetProject.id),
      );
    }

    const prepareProject = yield* findActiveProjectContainingPath({
      readModel: input.readModel,
      candidatePath: canonicalProjectCwd,
      fileSystem: input.fileSystem,
      path: input.path,
    });
    if (
      prepareProject === undefined ||
      prepareProject.id !== targetProject.id ||
      !canAccessAudience(input.authority, prepareProject.dataAudience)
    ) {
      return yield* failAuthorization(
        input.command,
        projectNotFoundDetail(input.command, targetProject.id),
      );
    }
    return canonicalProjectCwd;
  },
);

export const authorizeOrchestrationCommandMutation = Effect.fn(
  "authorizeOrchestrationCommandMutation",
)(function* (input: {
  readonly command: OrchestrationCommand;
  readonly readModel: OrchestrationReadModel;
  readonly authority: OrchestrationCommandDispatchAuthority | undefined;
  readonly fileSystem: FileSystem.FileSystem;
  readonly path: Path.Path;
}) {
  const { command, fileSystem, path, readModel } = input;
  const authority = input.authority;
  if (authority === undefined) {
    return yield* failAuthorization(command, "Orchestration dispatch authority is required.");
  }

  switch (command.type) {
    case "project.create": {
      const existingProject = findProject(readModel, command.projectId);
      if (
        existingProject !== undefined &&
        !canAccessAudience(authority, existingProject.dataAudience)
      ) {
        return yield* failAuthorization(command, projectNotFoundDetail(command, command.projectId));
      }
      const canonicalWorkspaceRoot = yield* canonicalPathForComparison(
        command.workspaceRoot,
        fileSystem,
        path,
      );
      if (
        canonicalWorkspaceRoot === null ||
        (yield* hasHiddenAudiencePathCollision({
          readModel,
          authority,
          candidatePath: canonicalWorkspaceRoot,
          fileSystem,
          path,
        }))
      ) {
        return yield* failAuthorization(command, projectNotFoundDetail(command, command.projectId));
      }
      return bindProjectCreate({ ...command, workspaceRoot: canonicalWorkspaceRoot }, authority);
    }
    case "project.meta.update": {
      yield* requireProjectAudience({
        command,
        readModel,
        authority,
        projectId: command.projectId,
      });
      if (command.workspaceRoot !== undefined) {
        const canonicalWorkspaceRoot = yield* canonicalPathForComparison(
          command.workspaceRoot,
          fileSystem,
          path,
        );
        if (
          canonicalWorkspaceRoot === null ||
          (yield* hasHiddenAudiencePathCollision({
            readModel,
            authority,
            candidatePath: canonicalWorkspaceRoot,
            fileSystem,
            path,
          }))
        ) {
          return yield* failAuthorization(
            command,
            projectNotFoundDetail(command, command.projectId),
          );
        }
        return { ...command, workspaceRoot: canonicalWorkspaceRoot };
      }
      return command;
    }
    case "project.data-audience.set":
      yield* requireProjectAudience({
        command,
        readModel,
        authority,
        projectId: command.projectId,
      });
      return command;

    case "project.delete": {
      const project = yield* requireProjectAudience({
        command,
        readModel,
        authority,
        projectId: command.projectId,
      });
      if (command.force === true) {
        for (const thread of readModel.threads) {
          if (thread.projectId !== project.id || thread.deletedAt !== null) continue;
          if (
            !canAccessAudience(authority, thread.dataAudience) ||
            thread.dataAudience !== project.dataAudience
          ) {
            return yield* failAuthorization(
              command,
              projectNotFoundDetail(command, command.projectId),
            );
          }
        }
      }
      return command;
    }

    case "thread.create": {
      const existingThread = findThread(readModel, command.threadId);
      if (
        existingThread !== undefined &&
        !canAccessAudience(authority, existingThread.dataAudience)
      ) {
        return yield* failAuthorization(command, threadNotFoundDetail(command, command.threadId));
      }
      yield* requireProjectAudience({
        command,
        readModel,
        authority,
        projectId: command.projectId,
      });
      if (isFactorySessionAuthority(authority) && hasThreadCreateWorktreeMetadata(command)) {
        return yield* failAuthorization(command, projectNotFoundDetail(command, command.projectId));
      }
      const canonicalWorktreePath =
        command.worktreePath === null
          ? null
          : yield* canonicalPathForComparison(command.worktreePath, fileSystem, path);
      const canonicalRemovalPath =
        command.worktreeRemovalPath == null
          ? command.worktreeRemovalPath
          : yield* canonicalPathForComparison(command.worktreeRemovalPath, fileSystem, path);
      if (
        (command.worktreePath !== null && canonicalWorktreePath === null) ||
        (command.worktreeRemovalPath != null && canonicalRemovalPath === null)
      ) {
        return yield* failAuthorization(command, projectNotFoundDetail(command, command.projectId));
      }
      return {
        ...command,
        worktreePath: canonicalWorktreePath,
        ...(command.worktreeRemovalPath !== undefined
          ? { worktreeRemovalPath: canonicalRemovalPath }
          : {}),
      };
    }

    case "thread.parent.set": {
      const thread = yield* requireThreadAudience({
        command,
        readModel,
        authority,
        threadId: command.threadId,
      });
      if (command.parentEnvironmentId === undefined) {
        const parentThread = yield* requireThreadAudience({
          command,
          readModel,
          authority,
          threadId: command.parentThreadId,
        });
        yield* requireSameThreadAudience({
          command,
          referenceThreadId: command.parentThreadId,
          referenceAudience: parentThread.dataAudience,
          targetAudience: thread.dataAudience,
        });
      } else {
        yield* requireRemoteParentAllowed({
          command,
          authority,
          parentThreadId: command.parentThreadId,
        });
      }
      return command;
    }

    case "thread.turn.start": {
      const thread = findThread(readModel, command.threadId);
      let targetAudience: DataAudience;
      let targetProject: OrchestrationProject | undefined;
      if (thread !== undefined) {
        const authorizedThread = yield* requireThreadAudience({
          command,
          readModel,
          authority,
          threadId: command.threadId,
        });
        targetAudience = authorizedThread.dataAudience;
        if (command.bootstrap?.createThread !== undefined) {
          const project = yield* requireProjectAudience({
            command,
            readModel,
            authority,
            projectId: command.bootstrap.createThread.projectId,
          });
          yield* requireSameBootstrapProjectAsThread({
            command,
            thread: authorizedThread,
            project,
          });
          targetProject = project;
        }
      } else if (command.bootstrap?.createThread !== undefined) {
        const project = yield* requireProjectAudience({
          command,
          readModel,
          authority,
          projectId: command.bootstrap.createThread.projectId,
        });
        targetAudience = project.dataAudience;
        targetProject = project;
      } else {
        return yield* failAuthorization(command, threadNotFoundDetail(command, command.threadId));
      }
      const canonicalPrepareProjectCwd = yield* requireBootstrapSideEffectsAuthorized({
        command,
        readModel,
        authority,
        targetProject,
        existingThread: thread,
        fileSystem,
        path,
      });
      if (command.sourceProposedPlan !== undefined) {
        const sourceThread = yield* requireThreadAudience({
          command,
          readModel,
          authority,
          threadId: command.sourceProposedPlan.threadId,
        });
        yield* requireSameThreadAudience({
          command,
          referenceThreadId: command.sourceProposedPlan.threadId,
          referenceAudience: sourceThread.dataAudience,
          targetAudience,
        });
      }
      if (
        canonicalPrepareProjectCwd !== undefined &&
        command.bootstrap?.prepareWorktree !== undefined
      ) {
        return {
          ...command,
          bootstrap: {
            ...command.bootstrap,
            prepareWorktree: {
              ...command.bootstrap.prepareWorktree,
              projectCwd: canonicalPrepareProjectCwd,
            },
          },
        };
      }
      return command;
    }

    case "thread.meta.update": {
      yield* requireThreadAudience({ command, readModel, authority, threadId: command.threadId });
      if (isFactorySessionAuthority(authority) && hasThreadMetaWorktreeMutation(command)) {
        return yield* failAuthorization(command, threadNotFoundDetail(command, command.threadId));
      }
      const canonicalWorktreePath =
        command.worktreePath == null
          ? command.worktreePath
          : yield* canonicalPathForComparison(command.worktreePath, fileSystem, path);
      const canonicalRemovalPath =
        command.worktreeRemovalPath == null
          ? command.worktreeRemovalPath
          : yield* canonicalPathForComparison(command.worktreeRemovalPath, fileSystem, path);
      if (
        (command.worktreePath != null && canonicalWorktreePath === null) ||
        (command.worktreeRemovalPath != null && canonicalRemovalPath === null)
      ) {
        return yield* failAuthorization(command, threadNotFoundDetail(command, command.threadId));
      }
      return {
        ...command,
        ...(command.worktreePath !== undefined ? { worktreePath: canonicalWorktreePath } : {}),
        ...(command.worktreeRemovalPath !== undefined
          ? { worktreeRemovalPath: canonicalRemovalPath }
          : {}),
      };
    }

    case "thread.delete":
    case "thread.unarchive":
    case "thread.settle":
    case "thread.unsettle":
    case "thread.runtime-mode.set":
    case "thread.interaction-mode.set":
    case "thread.turn.interrupt":
    case "thread.approval.respond":
    case "thread.user-input.respond":
    case "thread.checkpoint.revert":
    case "thread.session.stop":
    case "thread.session.set":
    case "thread.message.assistant.delta":
    case "thread.message.assistant.complete":
    case "thread.proposed-plan.upsert":
    case "thread.turn.diff.complete":
    case "thread.snooze":
    case "thread.unsnooze":
    case "thread.pin":
    case "thread.unpin":
    case "thread.pin.reorder":
    case "thread.title.regeneration.complete":
    case "thread.turn.effective-model.set":
    case "thread.activity.append":
    case "thread.revert.complete":
      yield* requireThreadAudience({ command, readModel, authority, threadId: command.threadId });
      return command;

    case "thread.archive": {
      const thread = yield* requireThreadAudience({
        command,
        readModel,
        authority,
        threadId: command.threadId,
      });
      for (const descendant of collectUnarchivedDescendantThreads(readModel, command.threadId)) {
        if (
          !canAccessAudience(authority, descendant.dataAudience) ||
          descendant.dataAudience !== thread.dataAudience
        ) {
          return yield* failAuthorization(command, threadNotFoundDetail(command, command.threadId));
        }
      }
      return command;
    }

    default: {
      command satisfies never;
      const fallback = command as never as { readonly type: string };
      return yield* failAuthorization(
        fallback as OrchestrationCommand,
        `Command target audience cannot be resolved for command '${fallback.type}'.`,
      );
    }
  }
});
