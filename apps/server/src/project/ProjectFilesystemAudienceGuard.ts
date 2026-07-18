// @effect-diagnostics nodeBuiltinImport:off
import * as NodeOS from "node:os";

import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { isExplicitRelativePath, isWindowsAbsolutePath } from "@t3tools/shared/path";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import { currentReadAudienceCeiling } from "../auth/audienceDataPolicy.ts";
import { expandHomePath } from "../pathExpansion.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";

function resolveHomeAwarePath(input: string): string {
  const trimmed = input.trim();
  return trimmed.length === 0 ? NodeOS.homedir() : expandHomePath(trimmed);
}

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
    const candidates = [
      ...snapshot.projects.map((project) => ({
        path: project.workspaceRoot,
        dataAudience: project.dataAudience,
      })),
      ...snapshot.threads.flatMap((thread) =>
        thread.worktreePath === null
          ? []
          : [{ path: thread.worktreePath, dataAudience: thread.dataAudience }],
      ),
    ];
    const roots: Array<{ readonly path: string; readonly dataAudience: "factory" | "private" }> =
      [];
    for (const candidate of candidates) {
      const realRoot = yield* realPathOption(fileSystem, candidate.path);
      if (Option.isSome(realRoot) && !roots.some((root) => root.path === realRoot.value)) {
        roots.push({ path: realRoot.value, dataAudience: candidate.dataAudience });
      }
    }
    return roots.sort((left, right) => right.path.length - left.path.length);
  },
);

const classifyExistingPathAudience = Effect.fn(
  "ProjectFilesystemAudienceGuard.classifyExistingPathAudience",
)(function* (realCandidatePath: string) {
  const path = yield* Path.Path;
  const roots = yield* classifiedProjectRoots();
  const match = roots.find((root) => isWithinRoot(path, root.path, realCandidatePath));
  return match?.dataAudience ?? null;
});

export const isPathVisibleToCurrentAudience = Effect.fn(
  "ProjectFilesystemAudienceGuard.isPathVisibleToCurrentAudience",
)(function* (candidatePath: string) {
  const audienceCeiling = yield* currentReadAudienceCeiling;
  if (audienceCeiling === "private") return true;

  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const realCandidate = yield* realPathOption(
    fileSystem,
    path.resolve(expandHomePath(candidatePath)),
  );
  if (Option.isNone(realCandidate)) return false;

  return (yield* classifyExistingPathAudience(realCandidate.value)) === "factory";
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
      return (yield* classifyExistingPathAudience(realCandidate.value)) === "factory";
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

  const platform = yield* HostProcessPlatform;
  const path = yield* Path.Path;
  const partialPath = input.partialPath.trim();
  if (platform !== "win32" && isWindowsAbsolutePath(partialPath)) {
    return false;
  }

  let resolvedInputPath: string;
  if (!isExplicitRelativePath(partialPath)) {
    resolvedInputPath = path.resolve(resolveHomeAwarePath(partialPath));
  } else {
    if (!input.cwd) return false;
    resolvedInputPath = path.resolve(resolveHomeAwarePath(input.cwd), partialPath);
  }

  const endsWithSeparator =
    partialPath.length === 0 || /[\\/]$/.test(partialPath) || partialPath === "~";
  const parentPath = endsWithSeparator ? resolvedInputPath : path.dirname(resolvedInputPath);
  return yield* isPathVisibleToCurrentAudience(parentPath);
});
