import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentAuthenticatedPrincipal,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type AuthAudienceCeiling,
  type OrchestrationProject,
  type OrchestrationReadModel,
  type OrchestrationThread,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import { canReadDataAudience, currentReadAudienceCeiling } from "../auth/audienceDataPolicy.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as ProjectFilesystemAudienceGuard from "./ProjectFilesystemAudienceGuard.ts";

const now = "2026-07-18T12:00:00.000Z";
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5-codex",
} as const;

const principal = (audienceCeiling: AuthAudienceCeiling) =>
  EnvironmentAuthenticatedPrincipal.of({
    sessionId: AuthSessionId.make(`session-${audienceCeiling}`),
    subject: `test-${audienceCeiling}`,
    method: "bearer-access-token",
    scopes: new Set([AuthOrchestrationReadScope]),
    audienceCeiling,
  });

const makeProject = (
  id: string,
  workspaceRoot: string,
  dataAudience: OrchestrationProject["dataAudience"],
): OrchestrationProject => ({
  id: ProjectId.make(id),
  title: id,
  workspaceRoot,
  dataAudience,
  repositoryIdentity: null,
  defaultModelSelection: modelSelection,
  scripts: [],
  createdAt: now,
  updatedAt: now,
  deletedAt: null,
});

const makeThread = (input: {
  readonly id: string;
  readonly projectId: ProjectId;
  readonly dataAudience: OrchestrationThreadShell["dataAudience"];
  readonly worktreePath: string | null;
}): OrchestrationThreadShell & Pick<OrchestrationThread, "deletedAt"> => ({
  id: ThreadId.make(input.id),
  projectId: input.projectId,
  dataAudience: input.dataAudience,
  title: input.id,
  modelSelection,
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: input.worktreePath,
  latestTurn: null,
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  deletedAt: null,
  session: null,
  latestUserMessageAt: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
  parentThreadId: null,
});

const makeProjectionLayer = (input: {
  readonly projects: ReadonlyArray<OrchestrationProject>;
  readonly threads: ReadonlyArray<OrchestrationThreadShell>;
}) =>
  Layer.succeed(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
    getCommandReadModel: () =>
      Effect.succeed({
        snapshotSequence: 1,
        updatedAt: now,
        projects: input.projects,
        threads: input.threads,
      } as unknown as OrchestrationReadModel),
    getSnapshot: () => Effect.die("unused"),
    getShellSnapshot: () =>
      currentReadAudienceCeiling.pipe(
        Effect.map((audienceCeiling) => ({
          snapshotSequence: 1,
          updatedAt: now,
          projects: input.projects.filter((project) =>
            canReadDataAudience(audienceCeiling, project.dataAudience),
          ),
          threads: input.threads.filter((thread) =>
            canReadDataAudience(audienceCeiling, thread.dataAudience),
          ),
        })),
      ),
    getArchivedShellSnapshot: () => Effect.die("unused"),
    getSnapshotSequence: () => Effect.die("unused"),
    getCounts: () => Effect.die("unused"),
    getActiveProjectByWorkspaceRoot: (workspaceRoot) =>
      currentReadAudienceCeiling.pipe(
        Effect.map((audienceCeiling) => {
          const project = input.projects.find(
            (candidate) =>
              candidate.workspaceRoot === workspaceRoot &&
              canReadDataAudience(audienceCeiling, candidate.dataAudience),
          );
          return project === undefined ? Option.none() : Option.some(project);
        }),
      ),
    getProjectShellById: () => Effect.die("unused"),
    getFirstActiveThreadIdByProjectId: () => Effect.die("unused"),
    getThreadCheckpointContext: () => Effect.die("unused"),
    getFullThreadDiffContext: () => Effect.die("unused"),
    getThreadShellById: () => Effect.die("unused"),
    getThreadShellByIdIncludingArchived: () => Effect.die("unused"),
    getThreadShellSnapshotByIdIncludingArchived: () => Effect.die("unused"),
    getThreadDetailById: () => Effect.die("unused"),
    getThreadDetailSnapshot: () => Effect.die("unused"),
  });

describe("ProjectFilesystemAudienceGuard", () => {
  it.effect("checks repo-wide Git reads from the repository root, not a clean subdirectory", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const repoRoot = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-guard-repo-root-",
      });
      const cleanSubdirectory = path.join(repoRoot, "src");
      const privateSibling = path.join(repoRoot, "private");
      yield* fileSystem.makeDirectory(cleanSubdirectory);
      yield* fileSystem.makeDirectory(privateSibling);
      yield* fileSystem.writeFileString(path.join(privateSibling, "secret.txt"), "private\n");

      const registry = yield* VcsDriverRegistry.VcsDriverRegistry;
      const git = yield* registry.get("git");
      yield* git.execute({ operation: "test.git.init", cwd: repoRoot, args: ["init"] });
      yield* git.execute({
        operation: "test.git.add-private-sibling",
        cwd: repoRoot,
        args: ["add", "private/secret.txt"],
      });
      const unguardedStatus = yield* git.execute({
        operation: "test.git.status",
        cwd: cleanSubdirectory,
        args: ["status", "--porcelain=2", "--branch"],
      });
      expect(unguardedStatus.stdout).toContain("../private/secret.txt");

      const factoryProject = makeProject("project-repo-factory", repoRoot, "factory");
      const privateProject = makeProject("project-repo-private", privateSibling, "private");
      const projectionLayer = makeProjectionLayer({
        projects: [factoryProject, privateProject],
        threads: [],
      });
      const runAsFactory = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal("factory")));

      const result = yield* Effect.all({
        callerScopedCheck: runAsFactory(
          ProjectFilesystemAudienceGuard.hasHiddenDescendantForCurrentAudience(cleanSubdirectory),
        ),
        repositoryScopedCheck: runAsFactory(
          ProjectFilesystemAudienceGuard.hasHiddenDescendantAtGitRepositoryRootForCurrentAudience(
            cleanSubdirectory,
          ),
        ),
      }).pipe(Effect.provide(projectionLayer));

      expect(result).toEqual({
        callerScopedCheck: false,
        repositoryScopedCheck: true,
      });
    }).pipe(
      Effect.provide(
        VcsDriverRegistry.layer.pipe(
          Layer.provideMerge(VcsProcess.layer),
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("keeps factory filesystem and VCS paths inside factory project roots", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const privateRoot = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-guard-private-",
      });
      const factoryRoot = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-guard-factory-",
      });
      const factoryWorktree = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-guard-factory-worktree-",
      });
      const outsideRoot = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-guard-outside-",
      });
      yield* fileSystem.writeFileString(path.join(privateRoot, "secret.txt"), "private\n");
      yield* fileSystem.writeFileString(path.join(factoryRoot, "visible.txt"), "factory\n");
      yield* fileSystem.writeFileString(path.join(factoryWorktree, "diff.txt"), "worktree\n");
      const privateNestedRoot = path.join(factoryRoot, "private-nested");
      yield* fileSystem.makeDirectory(privateNestedRoot);
      yield* fileSystem.writeFileString(
        path.join(privateNestedRoot, "secret.txt"),
        "nested private\n",
      );
      yield* fileSystem.writeFileString(path.join(outsideRoot, "escaped.txt"), "outside\n");
      yield* fileSystem.symlink(outsideRoot, path.join(factoryRoot, "linked-outside"));

      const factoryProject = makeProject("project-factory", factoryRoot, "factory");
      const privateProject = makeProject("project-private", privateRoot, "private");
      const nestedPrivateProject = makeProject(
        "project-private-nested",
        privateNestedRoot,
        "private",
      );
      const projectionLayer = makeProjectionLayer({
        projects: [privateProject, nestedPrivateProject, factoryProject],
        threads: [
          makeThread({
            id: "thread-factory-worktree",
            projectId: factoryProject.id,
            dataAudience: "factory",
            worktreePath: factoryWorktree,
          }),
        ],
      });
      const runAsFactory = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal("factory")));

      const result = yield* Effect.all({
        factoryProjectFile: runAsFactory(
          ProjectFilesystemAudienceGuard.isPathVisibleToCurrentAudience(
            path.join(factoryRoot, "visible.txt"),
          ),
        ),
        factoryWorktreeFile: runAsFactory(
          ProjectFilesystemAudienceGuard.isPathVisibleToCurrentAudience(
            path.join(factoryWorktree, "diff.txt"),
          ),
        ),
        privateProjectFile: runAsFactory(
          ProjectFilesystemAudienceGuard.isPathVisibleToCurrentAudience(
            path.join(privateRoot, "secret.txt"),
          ),
        ),
        nestedPrivateProjectFile: runAsFactory(
          ProjectFilesystemAudienceGuard.isPathVisibleToCurrentAudience(
            path.join(privateNestedRoot, "secret.txt"),
          ),
        ),
        symlinkEscape: runAsFactory(
          ProjectFilesystemAudienceGuard.isPathVisibleToCurrentAudience(
            path.join(factoryRoot, "linked-outside", "escaped.txt"),
          ),
        ),
        privateBrowseTarget: runAsFactory(
          ProjectFilesystemAudienceGuard.isBrowseTargetVisibleToCurrentAudience({
            partialPath: `${privateRoot}/`,
          }),
        ),
        factoryBrowseTargetWithHiddenDescendant: runAsFactory(
          ProjectFilesystemAudienceGuard.isBrowseTargetVisibleToCurrentAudience({
            partialPath: `${factoryRoot}/`,
          }),
        ),
        absoluteBrowseTargetIgnoresCleanFactoryCwd: runAsFactory(
          ProjectFilesystemAudienceGuard.isBrowseTargetVisibleToCurrentAudience({
            cwd: factoryWorktree,
            partialPath: `${factoryRoot}/`,
          }),
        ),
        cleanFactoryBrowseTarget: runAsFactory(
          ProjectFilesystemAudienceGuard.isBrowseTargetVisibleToCurrentAudience({
            partialPath: `${factoryWorktree}/`,
          }),
        ),
        factoryWorktreeDestination: runAsFactory(
          ProjectFilesystemAudienceGuard.isMutationTargetVisibleToCurrentAudience({
            cwd: factoryRoot,
            targetPath: "worktrees/new-factory",
          }),
        ),
        privateWorktreeDestination: runAsFactory(
          ProjectFilesystemAudienceGuard.isMutationTargetVisibleToCurrentAudience({
            cwd: factoryRoot,
            targetPath: path.join(privateRoot, "new-private-worktree"),
          }),
        ),
        symlinkWorktreeDestination: runAsFactory(
          ProjectFilesystemAudienceGuard.isMutationTargetVisibleToCurrentAudience({
            cwd: factoryRoot,
            targetPath: path.join(factoryRoot, "linked-outside"),
          }),
        ),
      }).pipe(Effect.provide(projectionLayer), Effect.provideService(HostProcessPlatform, "linux"));

      expect(result).toEqual({
        factoryProjectFile: true,
        factoryWorktreeFile: true,
        privateProjectFile: false,
        nestedPrivateProjectFile: false,
        symlinkEscape: false,
        privateBrowseTarget: false,
        factoryBrowseTargetWithHiddenDescendant: false,
        absoluteBrowseTargetIgnoresCleanFactoryCwd: false,
        cleanFactoryBrowseTarget: true,
        factoryWorktreeDestination: true,
        privateWorktreeDestination: false,
        symlinkWorktreeDestination: false,
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("leaves private filesystem sessions unrestricted", () =>
    Effect.gen(function* () {
      const result = yield* ProjectFilesystemAudienceGuard.isPathVisibleToCurrentAudience(
        "/definitely/not/a/project/missing.txt",
      ).pipe(
        Effect.provide(
          Layer.mergeAll(
            makeProjectionLayer({ projects: [], threads: [] }),
            NodeServices.layer,
            Layer.succeed(EnvironmentAuthenticatedPrincipal, principal("private")),
          ),
        ),
      );

      expect(result).toBe(true);
    }),
  );

  it.effect("ignores deleted project roots and stale or deleted worktrees", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-guard-readded-private-",
      });
      const staleAudienceWorktree = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-guard-stale-audience-worktree-",
      });
      const privateAudienceWorktree = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-guard-private-audience-worktree-",
      });
      const factoryRoot = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-guard-active-factory-",
      });
      const secretPath = path.join(root, "secret.txt");
      const staleAudienceSecretPath = path.join(staleAudienceWorktree, "secret.txt");
      const privateAudienceSecretPath = path.join(privateAudienceWorktree, "secret.txt");
      yield* fileSystem.writeFileString(secretPath, "private\n");
      yield* fileSystem.writeFileString(staleAudienceSecretPath, "private worktree\n");
      yield* fileSystem.writeFileString(privateAudienceSecretPath, "private thread worktree\n");

      const deletedFactoryProject = {
        ...makeProject("project-deleted-factory", root, "factory"),
        deletedAt: now,
      };
      const privateProject = makeProject("project-readded-private", root, "private");
      const factoryProject = makeProject("project-active-factory", factoryRoot, "factory");
      const deletedFactoryWorktree = {
        ...makeThread({
          id: "thread-deleted-factory-worktree",
          projectId: deletedFactoryProject.id,
          dataAudience: "factory",
          worktreePath: root,
        }),
        deletedAt: now,
      };
      const staleFactoryWorktree = makeThread({
        id: "thread-stale-factory-worktree",
        projectId: deletedFactoryProject.id,
        dataAudience: "factory",
        worktreePath: root,
      });
      const staleAudienceFactoryWorktree = makeThread({
        id: "thread-stale-audience-factory-worktree",
        projectId: privateProject.id,
        dataAudience: "factory",
        worktreePath: staleAudienceWorktree,
      });
      const privateAudienceFactoryWorktree = makeThread({
        id: "thread-private-audience-factory-worktree",
        projectId: factoryProject.id,
        dataAudience: "private",
        worktreePath: privateAudienceWorktree,
      });
      const projectionLayer = makeProjectionLayer({
        projects: [deletedFactoryProject, privateProject, factoryProject],
        threads: [
          deletedFactoryWorktree,
          staleFactoryWorktree,
          staleAudienceFactoryWorktree,
          privateAudienceFactoryWorktree,
        ],
      });

      const runAsFactory = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal("factory")));
      const result = yield* Effect.all({
        readdedPrivateProject: runAsFactory(
          ProjectFilesystemAudienceGuard.isPathVisibleToCurrentAudience(secretPath),
        ),
        staleAudienceWorktree: runAsFactory(
          ProjectFilesystemAudienceGuard.isPathVisibleToCurrentAudience(staleAudienceSecretPath),
        ),
        privateAudienceWorktree: runAsFactory(
          ProjectFilesystemAudienceGuard.isPathVisibleToCurrentAudience(privateAudienceSecretPath),
        ),
      }).pipe(Effect.provide(projectionLayer));

      expect(result).toEqual({
        readdedPrivateProject: false,
        staleAudienceWorktree: false,
        privateAudienceWorktree: false,
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
