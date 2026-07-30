// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as RcMap from "effect/RcMap";
import * as Schema from "effect/Schema";

import type {
  FilesystemBrowseInput,
  FilesystemBrowseResult,
  ProjectListEntriesInput,
  ProjectListEntriesResult,
  ProjectSearchEntriesInput,
  ProjectSearchEntriesResult,
} from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { isExplicitRelativePath, isWindowsAbsolutePath } from "@t3tools/shared/path";

import { expandHomePath } from "../pathExpansion.ts";
import * as WorkspacePaths from "./WorkspacePaths.ts";
import * as WorkspaceSearchIndex from "./WorkspaceSearchIndex.ts";

export class WorkspaceEntriesWindowsPathUnsupportedError extends Schema.TaggedErrorClass<WorkspaceEntriesWindowsPathUnsupportedError>()(
  "WorkspaceEntriesWindowsPathUnsupportedError",
  {
    cwd: Schema.optional(Schema.String),
    partialPath: Schema.String,
    platform: Schema.String,
  },
) {
  override get message(): string {
    const cwd = this.cwd ? ` from '${this.cwd}'` : "";
    return `Windows-style workspace path '${this.partialPath}' is not supported on '${this.platform}'${cwd}.`;
  }
}

export class WorkspaceEntriesCurrentProjectRequiredError extends Schema.TaggedErrorClass<WorkspaceEntriesCurrentProjectRequiredError>()(
  "WorkspaceEntriesCurrentProjectRequiredError",
  {
    partialPath: Schema.String,
  },
) {
  override get message(): string {
    return `A current project is required to browse relative workspace path '${this.partialPath}'.`;
  }
}

export class WorkspaceEntriesReadDirectoryError extends Schema.TaggedErrorClass<WorkspaceEntriesReadDirectoryError>()(
  "WorkspaceEntriesReadDirectoryError",
  {
    cwd: Schema.optional(Schema.String),
    partialPath: Schema.String,
    parentPath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    const cwd = this.cwd ? ` from '${this.cwd}'` : "";
    return `Failed to read workspace directory '${this.parentPath}' while browsing '${this.partialPath}'${cwd}.`;
  }
}

export const WorkspaceEntriesBrowseError = Schema.Union([
  WorkspaceEntriesWindowsPathUnsupportedError,
  WorkspaceEntriesCurrentProjectRequiredError,
  WorkspaceEntriesReadDirectoryError,
]);
export type WorkspaceEntriesBrowseError = typeof WorkspaceEntriesBrowseError.Type;

export const WorkspaceEntriesError = Schema.Union([
  WorkspacePaths.WorkspaceRootNotExistsError,
  WorkspacePaths.WorkspaceRootCreateFailedError,
  WorkspacePaths.WorkspaceRootStatFailedError,
  WorkspacePaths.WorkspaceRootNotDirectoryError,
  WorkspaceSearchIndex.WorkspaceSearchIndexCreateFailed,
  WorkspaceSearchIndex.WorkspaceSearchIndexScanTimedOut,
  WorkspaceSearchIndex.WorkspaceSearchIndexSearchFailed,
]);
export type WorkspaceEntriesError = typeof WorkspaceEntriesError.Type;

export interface ResolvedBrowseTarget {
  readonly parentPath: string;
  readonly prefix: string;
  readonly showHidden: boolean;
}

export class WorkspaceEntries extends Context.Service<
  WorkspaceEntries,
  {
    readonly browse: (
      input: FilesystemBrowseInput,
    ) => Effect.Effect<FilesystemBrowseResult, WorkspaceEntriesBrowseError>;
    readonly browseResolved: (
      input: FilesystemBrowseInput,
      target: ResolvedBrowseTarget,
    ) => Effect.Effect<FilesystemBrowseResult, WorkspaceEntriesReadDirectoryError>;
    readonly list: (
      input: ProjectListEntriesInput,
    ) => Effect.Effect<ProjectListEntriesResult, WorkspaceEntriesError>;
    readonly search: (
      input: ProjectSearchEntriesInput,
    ) => Effect.Effect<ProjectSearchEntriesResult, WorkspaceEntriesError>;
    readonly refresh: (cwd: string) => Effect.Effect<void>;
  }
>()("t3/workspace/WorkspaceEntries") {}

function resolveHomeAwarePath(input: string): string {
  const trimmed = input.trim();
  return trimmed.length === 0 ? NodeOS.homedir() : expandHomePath(trimmed);
}

export const resolveBrowseTarget = Effect.fn("WorkspaceEntries.resolveBrowseTarget")(function* (
  input: FilesystemBrowseInput,
): Effect.fn.Return<ResolvedBrowseTarget, WorkspaceEntriesBrowseError, Path.Path> {
  const platform = yield* HostProcessPlatform;
  const path = yield* Path.Path;
  const partialPath = input.partialPath.trim();

  if (platform !== "win32" && isWindowsAbsolutePath(partialPath)) {
    return yield* new WorkspaceEntriesWindowsPathUnsupportedError({
      cwd: input.cwd,
      partialPath,
      platform,
    });
  }

  const resolvedInputPath = !isExplicitRelativePath(partialPath)
    ? path.resolve(resolveHomeAwarePath(partialPath))
    : input.cwd
      ? path.resolve(resolveHomeAwarePath(input.cwd), partialPath)
      : yield* new WorkspaceEntriesCurrentProjectRequiredError({ partialPath });
  const endsWithSeparator =
    partialPath.length === 0 || /[\\/]$/.test(partialPath) || partialPath === "~";
  const prefix = endsWithSeparator ? "" : path.basename(resolvedInputPath);
  return {
    parentPath: endsWithSeparator ? resolvedInputPath : path.dirname(resolvedInputPath),
    prefix,
    showHidden: endsWithSeparator || prefix.startsWith("."),
  };
});

export const make = Effect.gen(function* () {
  const path = yield* Path.Path;
  const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
  const workspaceSearchIndexes = yield* WorkspaceSearchIndex.WorkspaceSearchIndexMap;

  const normalizeWorkspaceRoot = Effect.fn("WorkspaceEntries.normalizeWorkspaceRoot")(function* (
    cwd: string,
  ): Effect.fn.Return<string, WorkspaceEntriesError> {
    return yield* workspacePaths.normalizeWorkspaceRoot(cwd);
  });

  const refresh: WorkspaceEntries["Service"]["refresh"] = Effect.fn("WorkspaceEntries.refresh")(
    function* (cwd) {
      const normalizedCwd = yield* normalizeWorkspaceRoot(cwd).pipe(
        Effect.orElseSucceed(() => cwd),
      );
      if (!(yield* RcMap.has(workspaceSearchIndexes.rcMap, normalizedCwd))) {
        return;
      }
      const recoverRefreshFailure = (
        cause:
          | WorkspaceSearchIndex.WorkspaceSearchIndexCreateFailed
          | WorkspaceSearchIndex.WorkspaceSearchIndexScanTimedOut
          | WorkspaceSearchIndex.WorkspaceSearchIndexRefreshFailed,
      ) =>
        Effect.gen(function* () {
          yield* Effect.logWarning("Failed to refresh workspace search index", {
            cwd,
            cause,
          });
          yield* workspaceSearchIndexes.invalidate(normalizedCwd);
        });
      yield* Effect.gen(function* () {
        const searchIndex = yield* WorkspaceSearchIndex.WorkspaceSearchIndex;
        yield* searchIndex.refresh();
      }).pipe(
        Effect.provide(workspaceSearchIndexes.get(normalizedCwd)),
        Effect.catchTags({
          WorkspaceSearchIndexCreateFailed: recoverRefreshFailure,
          WorkspaceSearchIndexScanTimedOut: recoverRefreshFailure,
          WorkspaceSearchIndexRefreshFailed: recoverRefreshFailure,
        }),
      );
    },
  );

  const browseResolved: WorkspaceEntries["Service"]["browseResolved"] = Effect.fn(
    "WorkspaceEntries.browseResolved",
  )(function* (input, target) {
    const partialPath = input.partialPath.trim();

    const dirents = yield* Effect.tryPromise({
      try: () => NodeFSP.readdir(target.parentPath, { withFileTypes: true }),
      catch: (cause) =>
        new WorkspaceEntriesReadDirectoryError({
          cwd: input.cwd,
          partialPath,
          parentPath: target.parentPath,
          cause,
        }),
    }).pipe(
      Effect.catchIf(
        (error) => {
          const code = (error.cause as NodeJS.ErrnoException | undefined)?.code;
          return code === "EACCES" || code === "EPERM";
        },
        () => Effect.succeed([]),
      ),
    );

    const lowerPrefix = target.prefix.toLowerCase();
    const entries: Array<{ readonly name: string; readonly fullPath: string }> = [];
    for (const dirent of dirents) {
      if (
        dirent.isDirectory() &&
        dirent.name.toLowerCase().startsWith(lowerPrefix) &&
        (target.showHidden || !dirent.name.startsWith("."))
      ) {
        entries.push({
          name: dirent.name,
          fullPath: path.join(target.parentPath, dirent.name),
        });
      }
    }

    return {
      parentPath: target.parentPath,
      entries: entries.toSorted((left, right) => left.name.localeCompare(right.name)),
    };
  });

  const browse: WorkspaceEntries["Service"]["browse"] = Effect.fn("WorkspaceEntries.browse")(
    function* (input) {
      const target = yield* resolveBrowseTarget(input).pipe(Effect.provideService(Path.Path, path));
      return yield* browseResolved(input, target);
    },
  );

  const search: WorkspaceEntries["Service"]["search"] = Effect.fn("WorkspaceEntries.search")(
    function* (input) {
      const normalizedCwd = yield* normalizeWorkspaceRoot(input.cwd);
      const normalizedQuery = input.query
        .trim()
        .toLowerCase()
        .replace(/^[@./]+/, "");
      return yield* Effect.gen(function* () {
        const searchIndex = yield* WorkspaceSearchIndex.WorkspaceSearchIndex;
        return yield* searchIndex.search(normalizedQuery, input.limit);
      }).pipe(Effect.provide(workspaceSearchIndexes.get(normalizedCwd)));
    },
  );

  const list: WorkspaceEntries["Service"]["list"] = Effect.fn("WorkspaceEntries.list")(
    function* (input) {
      const normalizedCwd = yield* normalizeWorkspaceRoot(input.cwd);
      return yield* Effect.gen(function* () {
        const searchIndex = yield* WorkspaceSearchIndex.WorkspaceSearchIndex;
        return yield* searchIndex.list();
      }).pipe(Effect.provide(workspaceSearchIndexes.get(normalizedCwd)));
    },
  );

  return WorkspaceEntries.of({ browse, browseResolved, list, refresh, search });
});

export const layer = Layer.effect(WorkspaceEntries, make).pipe(
  Layer.provide(WorkspaceSearchIndex.WorkspaceSearchIndexMap.layer),
);
