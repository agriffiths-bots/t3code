// @effect-diagnostics nodeBuiltinImport:off
/**
 * WorkspaceFileSystem - Effect service contract for workspace file mutations.
 *
 * Owns workspace-root-relative file read/write operations and their associated
 * safety checks and cache invalidation hooks.
 *
 * @module WorkspaceFileSystem
 */
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";

import type {
  ProjectReadFileInput,
  ProjectReadFileResult,
  ProjectWriteFileInput,
  ProjectWriteFileResult,
} from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as WorkspaceEntries from "./WorkspaceEntries.ts";
import * as WorkspacePaths from "./WorkspacePaths.ts";

const PROJECT_READ_FILE_MAX_BYTES = 1024 * 1024;

export class WorkspaceFileSystemOperationError extends Schema.TaggedErrorClass<WorkspaceFileSystemOperationError>()(
  "WorkspaceFileSystemOperationError",
  {
    workspaceRoot: Schema.String,
    relativePath: Schema.String,
    resolvedPath: Schema.String,
    operationPath: Schema.String,
    operation: Schema.Literals([
      "realpath-workspace-root",
      "realpath-target",
      "open",
      "stat",
      "read",
      "close",
      "make-directory",
      "write-file",
    ]),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Workspace file operation '${this.operation}' failed at '${this.operationPath}' for resolved path '${this.resolvedPath}' (requested as '${this.relativePath}' in '${this.workspaceRoot}').`;
  }
}

export class WorkspaceFilePathEscapeError extends Schema.TaggedErrorClass<WorkspaceFilePathEscapeError>()(
  "WorkspaceFilePathEscapeError",
  {
    workspaceRoot: Schema.String,
    relativePath: Schema.String,
    resolvedWorkspaceRoot: Schema.String,
    resolvedPath: Schema.String,
  },
) {
  override get message(): string {
    return `Workspace file '${this.relativePath}' resolves outside workspace root '${this.workspaceRoot}': ${this.resolvedPath}`;
  }
}

export class WorkspacePathNotFileError extends Schema.TaggedErrorClass<WorkspacePathNotFileError>()(
  "WorkspacePathNotFileError",
  {
    workspaceRoot: Schema.String,
    relativePath: Schema.String,
    resolvedPath: Schema.String,
  },
) {
  override get message(): string {
    return `Workspace path '${this.relativePath}' in '${this.workspaceRoot}' is not a file: ${this.resolvedPath}`;
  }
}

export class WorkspaceBinaryFileError extends Schema.TaggedErrorClass<WorkspaceBinaryFileError>()(
  "WorkspaceBinaryFileError",
  {
    workspaceRoot: Schema.String,
    relativePath: Schema.String,
    resolvedPath: Schema.String,
  },
) {
  override get message(): string {
    return `Workspace file '${this.relativePath}' in '${this.workspaceRoot}' is binary and cannot be previewed as text.`;
  }
}

export const WorkspaceFileSystemError = Schema.Union([
  WorkspaceFileSystemOperationError,
  WorkspaceFilePathEscapeError,
  WorkspacePathNotFileError,
  WorkspaceBinaryFileError,
]);
export type WorkspaceFileSystemError = typeof WorkspaceFileSystemError.Type;

const isWorkspaceFileSystemOperationError = Schema.is(WorkspaceFileSystemOperationError);
const isWorkspaceFilePathEscapeError = Schema.is(WorkspaceFilePathEscapeError);

/** Service tag for workspace file operations. */
export class WorkspaceFileSystem extends Context.Service<
  WorkspaceFileSystem,
  {
    /** Read a UTF-8 text file relative to the workspace root. */
    readonly readFile: (
      input: ProjectReadFileInput,
    ) => Effect.Effect<
      ProjectReadFileResult,
      WorkspaceFileSystemError | WorkspacePaths.WorkspacePathOutsideRootError
    >;
    /**
     * Write a file relative to the workspace root.
     *
     * Creates parent directories as needed and rejects paths that escape the
     * workspace root.
     */
    readonly writeFile: (
      input: ProjectWriteFileInput,
    ) => Effect.Effect<
      ProjectWriteFileResult,
      WorkspaceFileSystemError | WorkspacePaths.WorkspacePathOutsideRootError
    >;
  }
>()("t3/workspace/WorkspaceFileSystem") {}

export const make = Effect.gen(function* () {
  const path = yield* Path.Path;
  const hostPlatform = yield* HostProcessPlatform;
  const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
  const workspaceEntries = yield* WorkspaceEntries.WorkspaceEntries;

  const realPathInsideWorkspace = (realWorkspaceRoot: string, candidatePath: string) => {
    const relativeRealPath = path.relative(realWorkspaceRoot, candidatePath);
    return (
      relativeRealPath === "" ||
      (relativeRealPath !== ".." &&
        !relativeRealPath.startsWith(`..${path.sep}`) &&
        !path.isAbsolute(relativeRealPath))
    );
  };

  const readFile: WorkspaceFileSystem["Service"]["readFile"] = Effect.fn(
    "WorkspaceFileSystem.readFile",
  )(function* (input) {
    const target = yield* workspacePaths.resolveRelativePathWithinRoot({
      workspaceRoot: input.cwd,
      relativePath: input.relativePath,
    });

    const realWorkspaceRoot = yield* Effect.tryPromise({
      try: () => NodeFSP.realpath(input.cwd),
      catch: (cause) =>
        new WorkspaceFileSystemOperationError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
          resolvedPath: target.absolutePath,
          operationPath: input.cwd,
          operation: "realpath-workspace-root",
          cause,
        }),
    });
    const realTargetPath = yield* Effect.tryPromise({
      try: () => NodeFSP.realpath(target.absolutePath),
      catch: (cause) =>
        new WorkspaceFileSystemOperationError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
          resolvedPath: target.absolutePath,
          operationPath: target.absolutePath,
          operation: "realpath-target",
          cause,
        }),
    });
    const relativeRealPath = path.relative(realWorkspaceRoot, realTargetPath);
    if (
      relativeRealPath.startsWith(`..${path.sep}`) ||
      relativeRealPath === ".." ||
      path.isAbsolute(relativeRealPath)
    ) {
      return yield* new WorkspaceFilePathEscapeError({
        workspaceRoot: input.cwd,
        relativePath: input.relativePath,
        resolvedWorkspaceRoot: realWorkspaceRoot,
        resolvedPath: realTargetPath,
      });
    }

    return yield* Effect.acquireUseRelease(
      Effect.tryPromise({
        try: () =>
          NodeFSP.open(realTargetPath, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW),
        catch: (cause) =>
          new WorkspaceFileSystemOperationError({
            workspaceRoot: input.cwd,
            relativePath: input.relativePath,
            resolvedPath: realTargetPath,
            operationPath: realTargetPath,
            operation: "open",
            cause,
          }),
      }),
      (handle) =>
        Effect.gen(function* () {
          const stat = yield* Effect.tryPromise({
            try: () => handle.stat(),
            catch: (cause) =>
              new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: realTargetPath,
                operationPath: realTargetPath,
                operation: "stat",
                cause,
              }),
          });
          if (!stat.isFile()) {
            return yield* new WorkspacePathNotFileError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedPath: realTargetPath,
            });
          }

          const currentRealTargetPath = yield* Effect.tryPromise({
            try: () => NodeFSP.realpath(target.absolutePath),
            catch: (cause) =>
              new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: realTargetPath,
                operationPath: target.absolutePath,
                operation: "realpath-target",
                cause,
              }),
          });
          if (!realPathInsideWorkspace(realWorkspaceRoot, currentRealTargetPath)) {
            return yield* new WorkspaceFilePathEscapeError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedWorkspaceRoot: realWorkspaceRoot,
              resolvedPath: currentRealTargetPath,
            });
          }
          const currentStat = yield* Effect.tryPromise({
            try: () => NodeFSP.stat(currentRealTargetPath),
            catch: (cause) =>
              new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: currentRealTargetPath,
                operationPath: currentRealTargetPath,
                operation: "stat",
                cause,
              }),
          });
          if (currentStat.dev !== stat.dev || currentStat.ino !== stat.ino) {
            return yield* new WorkspaceFileSystemOperationError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedPath: currentRealTargetPath,
              operationPath: currentRealTargetPath,
              operation: "stat",
              cause: new Error("Workspace target changed while it was being authorized."),
            });
          }

          const bytesToRead = Math.min(stat.size, PROJECT_READ_FILE_MAX_BYTES);
          const buffer = Buffer.alloc(bytesToRead);
          const { bytesRead } = yield* Effect.tryPromise({
            try: () => handle.read(buffer, 0, bytesToRead, 0),
            catch: (cause) =>
              new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: realTargetPath,
                operationPath: realTargetPath,
                operation: "read",
                cause,
              }),
          });
          const fileBytes = buffer.subarray(0, bytesRead);
          if (fileBytes.includes(0)) {
            return yield* new WorkspaceBinaryFileError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedPath: realTargetPath,
            });
          }

          return {
            relativePath: target.relativePath,
            contents: new TextDecoder("utf-8").decode(fileBytes),
            byteLength: stat.size,
            truncated: stat.size > PROJECT_READ_FILE_MAX_BYTES,
          };
        }),
      (handle) =>
        Effect.tryPromise({
          try: () => handle.close(),
          catch: (cause) =>
            new WorkspaceFileSystemOperationError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedPath: realTargetPath,
              operationPath: realTargetPath,
              operation: "close",
              cause,
            }),
        }),
    );
  });

  const writeFile: WorkspaceFileSystem["Service"]["writeFile"] = Effect.fn(
    "WorkspaceFileSystem.writeFile",
  )(function* (input) {
    const target = yield* workspacePaths.resolveRelativePathWithinRoot({
      workspaceRoot: input.cwd,
      relativePath: input.relativePath,
    });

    const realWorkspaceRoot = yield* Effect.tryPromise({
      try: () => NodeFSP.realpath(input.cwd),
      catch: (cause) =>
        new WorkspaceFileSystemOperationError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
          resolvedPath: target.absolutePath,
          operationPath: input.cwd,
          operation: "realpath-workspace-root",
          cause,
        }),
    });

    const targetParentPath = path.dirname(target.absolutePath);
    const parentRelativePath = path.relative(input.cwd, targetParentPath);
    const parentSegments = parentRelativePath === "" ? [] : parentRelativePath.split(path.sep);
    const leafName = path.basename(target.absolutePath);

    if (hostPlatform !== "linux") {
      yield* Effect.tryPromise({
        try: async () => {
          let ancestorPath = input.cwd;
          for (const segment of parentSegments) {
            ancestorPath = path.join(ancestorPath, segment);
            let ancestorStat: NodeFS.Stats;
            try {
              ancestorStat = await NodeFSP.lstat(ancestorPath);
            } catch (cause) {
              if ((cause as NodeJS.ErrnoException).code === "ENOENT") break;
              throw new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: target.absolutePath,
                operationPath: ancestorPath,
                operation: "stat",
                cause,
              });
            }

            if (ancestorStat.isSymbolicLink()) {
              throw new WorkspaceFilePathEscapeError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedWorkspaceRoot: realWorkspaceRoot,
                resolvedPath: ancestorPath,
              });
            }
            if (!ancestorStat.isDirectory()) {
              throw new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: target.absolutePath,
                operationPath: ancestorPath,
                operation: "make-directory",
                cause: new Error("Existing parent path is not a directory."),
              });
            }

            const realAncestorPath = await NodeFSP.realpath(ancestorPath);
            if (!realPathInsideWorkspace(realWorkspaceRoot, realAncestorPath)) {
              throw new WorkspaceFilePathEscapeError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedWorkspaceRoot: realWorkspaceRoot,
                resolvedPath: realAncestorPath,
              });
            }
          }

          try {
            await NodeFSP.mkdir(targetParentPath, { recursive: true });
          } catch (cause) {
            throw new WorkspaceFileSystemOperationError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedPath: target.absolutePath,
              operationPath: targetParentPath,
              operation: "make-directory",
              cause,
            });
          }

          const realTargetParentPath = await NodeFSP.realpath(targetParentPath);
          const realTargetPath = path.join(realTargetParentPath, leafName);
          if (!realPathInsideWorkspace(realWorkspaceRoot, realTargetPath)) {
            throw new WorkspaceFilePathEscapeError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedWorkspaceRoot: realWorkspaceRoot,
              resolvedPath: realTargetPath,
            });
          }

          try {
            const leafStat = await NodeFSP.lstat(realTargetPath);
            if (leafStat.isSymbolicLink()) {
              throw new WorkspaceFilePathEscapeError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedWorkspaceRoot: realWorkspaceRoot,
                resolvedPath: realTargetPath,
              });
            }
          } catch (cause) {
            if (isWorkspaceFilePathEscapeError(cause)) throw cause;
            if ((cause as NodeJS.ErrnoException).code !== "ENOENT") {
              throw new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: realTargetPath,
                operationPath: realTargetPath,
                operation: "stat",
                cause,
              });
            }
          }

          let handle: NodeFSP.FileHandle | null = null;
          try {
            handle = await NodeFSP.open(
              realTargetPath,
              NodeFS.constants.O_WRONLY |
                NodeFS.constants.O_CREAT |
                NodeFS.constants.O_TRUNC |
                NodeFS.constants.O_NOFOLLOW,
            );
            await handle.writeFile(input.contents, "utf8");
          } catch (cause) {
            if ((cause as NodeJS.ErrnoException).code === "ELOOP") {
              throw new WorkspaceFilePathEscapeError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedWorkspaceRoot: realWorkspaceRoot,
                resolvedPath: realTargetPath,
              });
            }
            throw new WorkspaceFileSystemOperationError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedPath: realTargetPath,
              operationPath: realTargetPath,
              operation: "write-file",
              cause,
            });
          } finally {
            await handle?.close().catch(() => undefined);
          }
        },
        catch: (cause) =>
          isWorkspaceFilePathEscapeError(cause) || isWorkspaceFileSystemOperationError(cause)
            ? cause
            : new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: target.absolutePath,
                operationPath: target.absolutePath,
                operation: "write-file",
                cause,
              }),
      });

      yield* workspaceEntries.refresh(input.cwd);
      return { relativePath: target.relativePath };
    }

    const fdDirectoryRoot = "/proc/self/fd";

    yield* Effect.tryPromise({
      try: async () => {
        const directoryHandles: NodeFSP.FileHandle[] = [];
        const fdPath = (fd: number) => path.join(fdDirectoryRoot, String(fd));
        const openDirectory = async (directoryPath: string) =>
          await NodeFSP.open(
            directoryPath,
            NodeFS.constants.O_RDONLY | NodeFS.constants.O_DIRECTORY | NodeFS.constants.O_NOFOLLOW,
          );

        try {
          let currentDirectory = await openDirectory(realWorkspaceRoot);
          directoryHandles.push(currentDirectory);

          for (const segment of parentSegments) {
            const childPath = path.join(fdPath(currentDirectory.fd), segment);
            let childStat: NodeFS.Stats;
            try {
              childStat = await NodeFSP.lstat(childPath);
            } catch (cause) {
              if ((cause as NodeJS.ErrnoException).code !== "ENOENT") {
                throw new WorkspaceFileSystemOperationError({
                  workspaceRoot: input.cwd,
                  relativePath: input.relativePath,
                  resolvedPath: target.absolutePath,
                  operationPath: childPath,
                  operation: "stat",
                  cause,
                });
              }
              try {
                await NodeFSP.mkdir(childPath);
                childStat = await NodeFSP.lstat(childPath);
              } catch (mkdirCause) {
                if ((mkdirCause as NodeJS.ErrnoException).code === "EEXIST") {
                  childStat = await NodeFSP.lstat(childPath);
                } else {
                  throw new WorkspaceFileSystemOperationError({
                    workspaceRoot: input.cwd,
                    relativePath: input.relativePath,
                    resolvedPath: target.absolutePath,
                    operationPath: childPath,
                    operation: "make-directory",
                    cause: mkdirCause,
                  });
                }
              }
            }

            if (childStat.isSymbolicLink()) {
              throw new WorkspaceFilePathEscapeError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedWorkspaceRoot: realWorkspaceRoot,
                resolvedPath: childPath,
              });
            }
            if (!childStat.isDirectory()) {
              throw new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: target.absolutePath,
                operationPath: childPath,
                operation: "make-directory",
                cause: new Error("Existing parent path is not a directory."),
              });
            }

            const realChildPath = await NodeFSP.realpath(childPath);
            if (!realPathInsideWorkspace(realWorkspaceRoot, realChildPath)) {
              throw new WorkspaceFilePathEscapeError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedWorkspaceRoot: realWorkspaceRoot,
                resolvedPath: realChildPath,
              });
            }

            currentDirectory = await openDirectory(childPath);
            directoryHandles.push(currentDirectory);
          }

          const leafPath = path.join(fdPath(currentDirectory.fd), leafName);
          try {
            const leafStat = await NodeFSP.lstat(leafPath);
            if (leafStat.isSymbolicLink()) {
              throw new WorkspaceFilePathEscapeError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedWorkspaceRoot: realWorkspaceRoot,
                resolvedPath: leafPath,
              });
            }
          } catch (cause) {
            if (isWorkspaceFilePathEscapeError(cause)) throw cause;
            if ((cause as NodeJS.ErrnoException).code !== "ENOENT") {
              throw new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: target.absolutePath,
                operationPath: leafPath,
                operation: "stat",
                cause,
              });
            }
          }

          let handle: NodeFSP.FileHandle | null = null;
          try {
            handle = await NodeFSP.open(
              leafPath,
              NodeFS.constants.O_WRONLY |
                NodeFS.constants.O_CREAT |
                NodeFS.constants.O_TRUNC |
                NodeFS.constants.O_NOFOLLOW,
            );
            await handle.writeFile(input.contents, "utf8");
          } catch (cause) {
            if ((cause as NodeJS.ErrnoException).code === "ELOOP") {
              throw new WorkspaceFilePathEscapeError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedWorkspaceRoot: realWorkspaceRoot,
                resolvedPath: leafPath,
              });
            }
            throw new WorkspaceFileSystemOperationError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedPath: target.absolutePath,
              operationPath: leafPath,
              operation: "write-file",
              cause,
            });
          } finally {
            await handle?.close().catch(() => undefined);
          }
        } finally {
          for (const handle of directoryHandles.toReversed()) {
            await handle.close().catch(() => undefined);
          }
        }
      },
      catch: (cause) =>
        isWorkspaceFilePathEscapeError(cause) || isWorkspaceFileSystemOperationError(cause)
          ? cause
          : new WorkspaceFileSystemOperationError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedPath: target.absolutePath,
              operationPath: target.absolutePath,
              operation: "write-file",
              cause,
            }),
    });

    yield* workspaceEntries.refresh(input.cwd);
    return { relativePath: target.relativePath };
  });

  return WorkspaceFileSystem.of({ readFile, writeFile });
});

export const layer = Layer.effect(WorkspaceFileSystem, make);
