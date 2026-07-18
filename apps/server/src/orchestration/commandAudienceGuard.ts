import type {
  AuthAudienceCeiling,
  DataAudience,
  OrchestrationCommand,
  OrchestrationProject,
  OrchestrationReadModel,
  OrchestrationThread,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import * as Effect from "effect/Effect";

import { OrchestrationCommandAudienceAuthorizationError } from "./Errors.ts";

export type OrchestrationCommandDispatchAuthority =
  | {
      readonly kind: "session";
      readonly subject: string;
      readonly audienceCeiling: AuthAudienceCeiling;
    }
  | {
      readonly kind: "trusted-system";
      readonly reason: string;
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

export function trustedSystemDispatchAuthority(
  reason: string,
): OrchestrationCommandDispatchAuthority {
  return {
    kind: "trusted-system",
    reason,
  };
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

function findActiveProjectByWorkspaceRoot(input: {
  readonly readModel: OrchestrationReadModel;
  readonly workspaceRoot: string;
}): OrchestrationProject | undefined {
  const normalizedWorkspaceRoot = normalizeProjectPathForComparison(input.workspaceRoot);
  return input.readModel.projects.find(
    (project) =>
      project.deletedAt === null &&
      normalizeProjectPathForComparison(project.workspaceRoot) === normalizedWorkspaceRoot,
  );
}

function findActiveProjectWorkspaceRootCollision(input: {
  readonly readModel: OrchestrationReadModel;
  readonly workspaceRoot: string;
  readonly exceptProjectId: ProjectId;
}): OrchestrationProject | undefined {
  const existingProject = findActiveProjectByWorkspaceRoot({
    readModel: input.readModel,
    workspaceRoot: input.workspaceRoot,
  });
  return existingProject !== undefined && existingProject.id !== input.exceptProjectId
    ? existingProject
    : undefined;
}

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
  return authority.kind === "session" && authority.audienceCeiling === "factory"
    ? "factory"
    : "private";
}

function canAccessAudience(
  authority: OrchestrationCommandDispatchAuthority,
  audience: DataAudience,
): boolean {
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
  if (input.authority.kind === "session" && input.authority.audienceCeiling === "factory") {
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

function requireBootstrapSideEffectsAuthorized(input: {
  readonly command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>;
  readonly readModel: OrchestrationReadModel;
  readonly authority: OrchestrationCommandDispatchAuthority;
  readonly targetProject: OrchestrationProject | undefined;
  readonly existingThread: OrchestrationThread | undefined;
}): Effect.Effect<void, OrchestrationCommandAudienceAuthorizationError> {
  const bootstrap = input.command.bootstrap;
  if (bootstrap === undefined) {
    return Effect.void;
  }
  if (!isFactorySessionAuthority(input.authority)) {
    return Effect.void;
  }

  const createThreadWorktreePath = bootstrap.createThread?.worktreePath ?? null;
  const createThreadWorktreeRemovalPath = bootstrap.createThread?.worktreeRemovalPath ?? null;
  if (createThreadWorktreePath !== null || createThreadWorktreeRemovalPath !== null) {
    if (input.targetProject === undefined) {
      return failAuthorization(
        input.command,
        threadNotFoundDetail(input.command, input.command.threadId),
      );
    }
    return failAuthorization(
      input.command,
      projectNotFoundDetail(input.command, input.targetProject.id),
    );
  }

  if (bootstrap.runSetupScript === true) {
    if (input.existingThread !== undefined) {
      return failAuthorization(
        input.command,
        threadNotFoundDetail(input.command, input.command.threadId),
      );
    }
    if (input.targetProject === undefined) {
      return failAuthorization(
        input.command,
        threadNotFoundDetail(input.command, input.command.threadId),
      );
    }
  }

  const prepareWorktree = bootstrap.prepareWorktree;
  if (prepareWorktree === undefined) {
    return Effect.void;
  }

  const targetProject = input.targetProject;
  if (targetProject === undefined) {
    return failAuthorization(
      input.command,
      threadNotFoundDetail(input.command, input.command.threadId),
    );
  }

  const prepareProject = findActiveProjectByWorkspaceRoot({
    readModel: input.readModel,
    workspaceRoot: prepareWorktree.projectCwd,
  });
  if (
    prepareProject === undefined ||
    prepareProject.id !== targetProject.id ||
    !canAccessAudience(input.authority, prepareProject.dataAudience)
  ) {
    return failAuthorization(input.command, projectNotFoundDetail(input.command, targetProject.id));
  }

  return Effect.void;
}

export const authorizeOrchestrationCommandMutation = Effect.fn(
  "authorizeOrchestrationCommandMutation",
)(function* (input: {
  readonly command: OrchestrationCommand;
  readonly readModel: OrchestrationReadModel;
  readonly authority: OrchestrationCommandDispatchAuthority | undefined;
}) {
  const { command, readModel } = input;
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
      const workspaceRootCollision = findActiveProjectWorkspaceRootCollision({
        readModel,
        workspaceRoot: command.workspaceRoot,
        exceptProjectId: command.projectId,
      });
      if (
        workspaceRootCollision !== undefined &&
        !canAccessAudience(authority, workspaceRootCollision.dataAudience)
      ) {
        return yield* failAuthorization(command, projectNotFoundDetail(command, command.projectId));
      }
      return bindProjectCreate(command, authority);
    }
    case "project.meta.update": {
      yield* requireProjectAudience({
        command,
        readModel,
        authority,
        projectId: command.projectId,
      });
      if (command.workspaceRoot !== undefined) {
        const workspaceRootCollision = findActiveProjectWorkspaceRootCollision({
          readModel,
          workspaceRoot: command.workspaceRoot,
          exceptProjectId: command.projectId,
        });
        if (
          workspaceRootCollision !== undefined &&
          !canAccessAudience(authority, workspaceRootCollision.dataAudience)
        ) {
          return yield* failAuthorization(
            command,
            projectNotFoundDetail(command, command.projectId),
          );
        }
      }
      return command;
    }
    case "project.data-audience.set":
    case "project.delete":
      yield* requireProjectAudience({
        command,
        readModel,
        authority,
        projectId: command.projectId,
      });
      return command;

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
      return command;
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
      yield* requireBootstrapSideEffectsAuthorized({
        command,
        readModel,
        authority,
        targetProject,
        existingThread: thread,
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
      return command;
    }

    case "thread.meta.update":
      yield* requireThreadAudience({ command, readModel, authority, threadId: command.threadId });
      if (isFactorySessionAuthority(authority) && hasThreadMetaWorktreeMutation(command)) {
        return yield* failAuthorization(command, threadNotFoundDetail(command, command.threadId));
      }
      return command;

    case "thread.delete":
    case "thread.unarchive":
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
          return yield* failAuthorization(command, threadNotFoundDetail(command, descendant.id));
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
