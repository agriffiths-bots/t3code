import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import { currentReadAudienceCeiling, strictestDataAudience } from "../auth/audienceDataPolicy.ts";
import { expandHomePath } from "../pathExpansion.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as WorkspaceEntries from "../workspace/WorkspaceEntries.ts";

function isWithinRoot(path: Path.Path, root: string, candidate: string): boolean {
  const relative = path.relative(root, candidate);
  return (
    relative === "" ||
    (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))
  );
}

const realPathOption = (fileSystem: FileSystem.FileSystem, value: string) =>
  fileSystem.realPath(value).pipe(
    Effect.map(Option.some),
    Effect.orElseSucceed(() => Option.none<string>()),
  );

const classifiedProjectRoots = Effect.fn("ProjectFilesystemAudienceGuard.classifiedProjectRoots")(
  function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const snapshotQuery = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
    const snapshot = yield* snapshotQuery.getCommandReadModel();
    const activeProjects = snapshot.projects.filter((project) => project.deletedAt === null);
    const activeProjectById = new Map(activeProjects.map((project) => [project.id, project]));
    const candidates = [
      ...activeProjects.map((project) => ({
        path: project.workspaceRoot,
        dataAudience: project.dataAudience,
      })),
      ...snapshot.threads.flatMap((thread) => {
        const project = activeProjectById.get(thread.projectId);
        return thread.deletedAt !== null || !project || thread.worktreePath === null
          ? []
          : [
              {
                path: thread.worktreePath,
                dataAudience: strictestDataAudience(thread.dataAudience, project.dataAudience),
              },
            ];
      }),
    ];
    const roots: Array<{ readonly path: string; dataAudience: "factory" | "private" }> = [];
    for (const candidate of candidates) {
      const realRoot = yield* realPathOption(fileSystem, candidate.path);
      if (Option.isNone(realRoot)) continue;

      const existingRoot = roots.find((root) => root.path === realRoot.value);
      if (existingRoot) {
        if (candidate.dataAudience === "private") {
          existingRoot.dataAudience = "private";
        }
      } else {
        roots.push({ path: realRoot.value, dataAudience: candidate.dataAudience });
      }
    }
    return roots.sort((left, right) => right.path.length - left.path.length);
  },
);

export const classifyCanonicalPathAudience = Effect.fn(
  "ProjectFilesystemAudienceGuard.classifyCanonicalPathAudience",
)(function* (realCandidatePath: string) {
  const path = yield* Path.Path;
  const roots = yield* classifiedProjectRoots();
  const match = roots.find((root) => isWithinRoot(path, root.path, realCandidatePath));
  return match?.dataAudience ?? null;
});

export const classifyPathAudience = Effect.fn(
  "ProjectFilesystemAudienceGuard.classifyPathAudience",
)(function* (candidatePath: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const realCandidate = yield* realPathOption(
    fileSystem,
    path.resolve(expandHomePath(candidatePath)),
  );
  if (Option.isNone(realCandidate)) return null;
  return yield* classifyCanonicalPathAudience(realCandidate.value);
});

export const isPathVisibleToCurrentAudience = Effect.fn(
  "ProjectFilesystemAudienceGuard.isPathVisibleToCurrentAudience",
)(function* (candidatePath: string) {
  const audienceCeiling = yield* currentReadAudienceCeiling;
  if (audienceCeiling === "private") return true;

  return (yield* classifyPathAudience(candidatePath)) === "factory";
});

export const isMutationTargetVisibleToCurrentAudience = Effect.fn(
  "ProjectFilesystemAudienceGuard.isMutationTargetVisibleToCurrentAudience",
)(function* (input: { readonly cwd: string; readonly targetPath: string }) {
  const audienceCeiling = yield* currentReadAudienceCeiling;
  if (audienceCeiling === "private") return true;

  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const expandedTargetPath = expandHomePath(input.targetPath);
  let candidatePath = path.isAbsolute(expandedTargetPath)
    ? path.resolve(expandedTargetPath)
    : path.resolve(expandHomePath(input.cwd), expandedTargetPath);

  while (true) {
    const realCandidate = yield* realPathOption(fileSystem, candidatePath);
    if (Option.isSome(realCandidate)) {
      return (yield* classifyCanonicalPathAudience(realCandidate.value)) === "factory";
    }

    const parentPath = path.dirname(candidatePath);
    if (parentPath === candidatePath) return false;
    candidatePath = parentPath;
  }
});

export const hasHiddenDescendantForCurrentAudience = Effect.fn(
  "ProjectFilesystemAudienceGuard.hasHiddenDescendantForCurrentAudience",
)(function* (candidatePath: string) {
  const audienceCeiling = yield* currentReadAudienceCeiling;
  if (audienceCeiling === "private") return false;

  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const realCandidate = yield* realPathOption(
    fileSystem,
    path.resolve(expandHomePath(candidatePath)),
  );
  if (Option.isNone(realCandidate)) return true;

  const roots = yield* classifiedProjectRoots();
  return roots.some(
    (root) =>
      root.dataAudience !== "factory" &&
      root.path !== realCandidate.value &&
      isWithinRoot(path, realCandidate.value, root.path),
  );
});

export const isBrowseTargetVisibleToCurrentAudience = Effect.fn(
  "ProjectFilesystemAudienceGuard.isBrowseTargetVisibleToCurrentAudience",
)(function* (input: { readonly cwd?: string | undefined; readonly partialPath: string }) {
  const audienceCeiling = yield* currentReadAudienceCeiling;
  if (audienceCeiling === "private") return true;

  const target = yield* WorkspaceEntries.resolveBrowseTarget(input).pipe(Effect.option);
  if (Option.isNone(target)) return false;
  if (!(yield* isPathVisibleToCurrentAudience(target.value.parentPath))) return false;
  return !(yield* hasHiddenDescendantForCurrentAudience(target.value.parentPath));
});
