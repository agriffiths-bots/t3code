import * as NodeOS from "node:os";

import { CommandId, ProjectId } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { RepositoryIdentityResolver } from "./RepositoryIdentityResolver.ts";

import { trustedSystemDispatchAuthority } from "../orchestration/commandAudienceGuard.ts";
export class ProjectAudienceAdministrationError extends Schema.TaggedErrorClass<ProjectAudienceAdministrationError>()(
  "ProjectAudienceAdministrationError",
  {
    operation: Schema.Literal("setAudienceToFactory"),
    projectId: ProjectId,
    workspaceRoot: Schema.optional(Schema.String),
    reason: Schema.Literals([
      "project-not-found",
      "broad-root",
      "not-repository",
      "not-repository-root",
      "workspace-root-resolution-failed",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "project-not-found":
        return `Project '${this.projectId}' was not found.`;
      case "broad-root":
        return `Project '${this.projectId}' uses a broad workspace root and cannot be classified factory.`;
      case "not-repository":
        return `Project '${this.projectId}' is not a Git checkout and cannot be classified factory.`;
      case "not-repository-root":
        return `Project '${this.projectId}' must use the dedicated Git checkout root before it can be classified factory.`;
      case "workspace-root-resolution-failed":
        return `Project '${this.projectId}' workspace root could not be resolved safely.`;
    }
  }
}

function containsPath(path: Path.Path, candidateAncestor: string, target: string): boolean {
  const relative = path.relative(candidateAncestor, target);
  return (
    relative.length === 0 ||
    (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))
  );
}

export function localProjectAudienceAdminSubject(): string {
  const user = NodeOS.userInfo();
  const uid = typeof user.uid === "number" && user.uid >= 0 ? `:${user.uid}` : "";
  return `local-admin:${user.username}${uid}`;
}

const requireDedicatedRepositoryRoot = Effect.fn(
  "ProjectAudienceAdministration.requireDedicatedRepositoryRoot",
)(function* (input: { readonly projectId: ProjectId; readonly workspaceRoot: string }) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const repositoryIdentityResolver = yield* RepositoryIdentityResolver;

  const canonicalWorkspaceRoot = yield* fileSystem.realPath(input.workspaceRoot).pipe(
    Effect.mapError(
      (cause) =>
        new ProjectAudienceAdministrationError({
          operation: "setAudienceToFactory",
          projectId: input.projectId,
          workspaceRoot: input.workspaceRoot,
          reason: "workspace-root-resolution-failed",
          cause,
        }),
    ),
  );
  const accountHome = NodeOS.userInfo().homedir;
  const canonicalHome = yield* fileSystem.realPath(accountHome).pipe(
    Effect.mapError(
      (cause) =>
        new ProjectAudienceAdministrationError({
          operation: "setAudienceToFactory",
          projectId: input.projectId,
          workspaceRoot: input.workspaceRoot,
          reason: "workspace-root-resolution-failed",
          cause,
        }),
    ),
  );

  if (containsPath(path, canonicalWorkspaceRoot, canonicalHome)) {
    return yield* new ProjectAudienceAdministrationError({
      operation: "setAudienceToFactory",
      projectId: input.projectId,
      workspaceRoot: input.workspaceRoot,
      reason: "broad-root",
    });
  }

  const repositoryIdentity = yield* repositoryIdentityResolver.resolve(canonicalWorkspaceRoot);
  if (repositoryIdentity === null) {
    return yield* new ProjectAudienceAdministrationError({
      operation: "setAudienceToFactory",
      projectId: input.projectId,
      workspaceRoot: input.workspaceRoot,
      reason: "not-repository",
    });
  }

  if (repositoryIdentity.rootPath === undefined) {
    return yield* new ProjectAudienceAdministrationError({
      operation: "setAudienceToFactory",
      projectId: input.projectId,
      workspaceRoot: input.workspaceRoot,
      reason: "not-repository",
    });
  }
  const canonicalRepositoryRoot = yield* fileSystem.realPath(repositoryIdentity.rootPath).pipe(
    Effect.mapError(
      (cause) =>
        new ProjectAudienceAdministrationError({
          operation: "setAudienceToFactory",
          projectId: input.projectId,
          workspaceRoot: input.workspaceRoot,
          reason: "workspace-root-resolution-failed",
          cause,
        }),
    ),
  );
  if (canonicalRepositoryRoot !== canonicalWorkspaceRoot) {
    return yield* new ProjectAudienceAdministrationError({
      operation: "setAudienceToFactory",
      projectId: input.projectId,
      workspaceRoot: input.workspaceRoot,
      reason: "not-repository-root",
    });
  }

  return canonicalWorkspaceRoot;
});

export const setProjectAudienceToFactory = Effect.fn(
  "ProjectAudienceAdministration.setProjectAudienceToFactory",
)(function* (input: { readonly projectId: ProjectId; readonly actor: string }) {
  const snapshotQuery = yield* ProjectionSnapshotQuery;
  const orchestrationEngine = yield* OrchestrationEngineService;
  const crypto = yield* Crypto.Crypto;

  const project = yield* snapshotQuery.getProjectShellById(input.projectId);
  if (Option.isNone(project)) {
    return yield* new ProjectAudienceAdministrationError({
      operation: "setAudienceToFactory",
      projectId: input.projectId,
      reason: "project-not-found",
    });
  }

  yield* requireDedicatedRepositoryRoot({
    projectId: input.projectId,
    workspaceRoot: project.value.workspaceRoot,
  });
  const commandUuid = yield* crypto.randomUUIDv4;

  return yield* orchestrationEngine.dispatch(
    {
      type: "project.data-audience.set",
      commandId: CommandId.make(`local-admin:project-audience:${commandUuid}`),
      projectId: input.projectId,
      expectedWorkspaceRoot: project.value.workspaceRoot,
      actor: input.actor,
      occurredAt: DateTime.formatIso(yield* DateTime.now),
    },
    trustedSystemDispatchAuthority("ProjectAudienceAdministration"),
  );
});
