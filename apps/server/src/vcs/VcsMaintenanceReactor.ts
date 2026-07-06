// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";

import { CommandId, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import type * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import { MAX_THREAD_CHECKPOINTS } from "../orchestration/checkpointRetention.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import * as GitVcsDriver from "./GitVcsDriver.ts";

// The projection exposes the newest positive-turn checkpoints; pruning also
// preserves turn 0 as the thread baseline when that ref exists.
export const CHECKPOINT_REFS_KEEP_PER_THREAD = MAX_THREAD_CHECKPOINTS + 1;
const MAINTENANCE_BOOT_DELAY = Duration.seconds(10);
const MAINTENANCE_SWEEP_INTERVAL = Duration.minutes(30);
const STOPPED_WORKTREE_REAP_AGE_MS = Duration.toMillis(Duration.hours(12));
const ARCHIVED_WORKTREE_REAP_AGE_MS = Duration.toMillis(Duration.hours(1));

const REAPABLE_SESSION_STATUSES = new Set(["stopped", "error"]);

export interface WorktreeMaintenanceRow {
  readonly threadId: string;
  readonly projectCwd: string;
  readonly worktreePath: string | null;
  readonly worktreeRemovable: boolean;
  readonly worktreeRemovalPath: string | null;
  readonly updatedAt: string;
  readonly archivedAt: string | null;
  readonly deletedAt: string | null;
  readonly sessionStatus: string | null;
  readonly runtimeStatus: string | null;
  readonly pendingApprovalCount: number;
  readonly pendingUserInputCount: number;
}

export interface WorktreeReapCandidate {
  readonly threadId: string;
  readonly threadIds: ReadonlyArray<string>;
  readonly projectCwd: string;
  readonly path: string;
}

export interface WorktreeReapOptions {
  readonly stoppedAgeMs?: number;
  readonly archivedAgeMs?: number;
}

function normalizePath(value: string | null | undefined): string | null {
  const trimmed = value?.trim();
  if (!trimmed || !NodePath.isAbsolute(trimmed)) {
    return null;
  }
  const normalized = NodePath.resolve(trimmed);
  return normalized === NodePath.parse(normalized).root ? null : normalized;
}

function isSameOrNestedPath(candidate: string | null, root: string): boolean {
  if (!candidate) {
    return false;
  }
  const normalizedCandidate = candidate.replace(/\\/g, "/").replace(/\/+$/, "");
  const normalizedRoot = root.replace(/\\/g, "/").replace(/\/+$/, "");
  return (
    normalizedCandidate === normalizedRoot ||
    normalizedCandidate.startsWith(
      normalizedRoot.endsWith("/") ? normalizedRoot : `${normalizedRoot}/`,
    )
  );
}

function pathsOverlap(left: string | null, right: string): boolean {
  return left !== null && (isSameOrNestedPath(left, right) || isSameOrNestedPath(right, left));
}

function rowRemovalPath(row: WorktreeMaintenanceRow): string | null {
  return normalizePath(row.worktreeRemovalPath ?? row.worktreePath);
}

function rowWorktreePath(row: WorktreeMaintenanceRow): string | null {
  return normalizePath(row.worktreePath);
}

function isRowActive(row: WorktreeMaintenanceRow): boolean {
  if (row.deletedAt !== null) {
    return false;
  }

  const hasLiveSessionStatus =
    (row.sessionStatus !== null && !REAPABLE_SESSION_STATUSES.has(row.sessionStatus)) ||
    (row.runtimeStatus !== null && !REAPABLE_SESSION_STATUSES.has(row.runtimeStatus));

  return hasLiveSessionStatus || row.pendingApprovalCount > 0 || row.pendingUserInputCount > 0;
}

function isRowOldEnough(
  row: WorktreeMaintenanceRow,
  nowMs: number,
  options: Required<WorktreeReapOptions>,
): boolean {
  const updatedAtMs = Date.parse(row.updatedAt);
  if (!Number.isFinite(updatedAtMs)) {
    return false;
  }
  const ageMs = nowMs - updatedAtMs;
  if (ageMs < 0) {
    return false;
  }
  if (row.deletedAt !== null || row.archivedAt !== null) {
    return ageMs >= options.archivedAgeMs;
  }
  return ageMs >= options.stoppedAgeMs;
}

function isProtectedProjectPath(path: string, projectRoots: ReadonlyArray<string>): boolean {
  return (
    projectRoots.some((projectRoot) => isSameOrNestedPath(projectRoot, path)) ||
    projectRoots.some((projectRoot) => isSameOrNestedPath(path, projectRoot))
  );
}

function reapEligibility(
  row: WorktreeMaintenanceRow,
  normalizedProjectRoots: ReadonlyArray<string>,
  nowMs: number,
  options: Required<WorktreeReapOptions>,
): { readonly path: string; readonly key: string } | null {
  const removalPath = rowRemovalPath(row);
  if (
    !removalPath ||
    !row.worktreeRemovable ||
    isRowActive(row) ||
    !isRowOldEnough(row, nowMs, options) ||
    isProtectedProjectPath(removalPath, normalizedProjectRoots)
  ) {
    return null;
  }

  return {
    path: removalPath,
    key: `${row.projectCwd}\0${removalPath}`,
  };
}

export function selectStaleWorktreeReapCandidates(
  rows: ReadonlyArray<WorktreeMaintenanceRow>,
  projectRoots: ReadonlyArray<string>,
  nowMs: number,
  options: WorktreeReapOptions = {},
): ReadonlyArray<WorktreeReapCandidate> {
  const resolvedOptions = {
    stoppedAgeMs: options.stoppedAgeMs ?? STOPPED_WORKTREE_REAP_AGE_MS,
    archivedAgeMs: options.archivedAgeMs ?? ARCHIVED_WORKTREE_REAP_AGE_MS,
  };
  const normalizedProjectRoots = projectRoots.flatMap((projectRoot) => {
    const normalized = normalizePath(projectRoot);
    return normalized ? [normalized] : [];
  });
  const candidates: WorktreeReapCandidate[] = [];
  const selectedPaths = new Set<string>();
  const eligibilityByThreadId = new Map<string, { readonly path: string; readonly key: string }>();

  for (const row of rows) {
    const eligibility = reapEligibility(row, normalizedProjectRoots, nowMs, resolvedOptions);
    if (eligibility) {
      eligibilityByThreadId.set(row.threadId, eligibility);
    }
  }

  for (const row of rows) {
    const eligibility = eligibilityByThreadId.get(row.threadId);
    if (!eligibility || selectedPaths.has(eligibility.key)) {
      continue;
    }

    const sharedByRetainedThread = rows.some((other) => {
      if (other.threadId === row.threadId || other.deletedAt !== null) {
        return false;
      }
      if (
        !pathsOverlap(rowWorktreePath(other), eligibility.path) &&
        !pathsOverlap(rowRemovalPath(other), eligibility.path)
      ) {
        return false;
      }

      const otherEligibility = eligibilityByThreadId.get(other.threadId);
      return otherEligibility?.key !== eligibility.key;
    });
    if (sharedByRetainedThread) {
      continue;
    }

    const threadIds = Array.from(
      new Set(
        rows.flatMap((other) => {
          const otherEligibility = eligibilityByThreadId.get(other.threadId);
          return otherEligibility?.key === eligibility.key ? [other.threadId] : [];
        }),
      ),
    );

    selectedPaths.add(eligibility.key);
    candidates.push({
      threadId: row.threadId,
      threadIds,
      projectCwd: row.projectCwd,
      path: eligibility.path,
    });
  }

  return candidates;
}

export function isWorktreePathListed(porcelainOutput: string, worktreePath: string): boolean {
  return porcelainOutput.split("\n").some((line) => {
    if (!line.startsWith("worktree ")) {
      return false;
    }
    return normalizePath(line.slice("worktree ".length)) === worktreePath;
  });
}

export function shouldRetainWorktreeMetadataAfterListFailure(input: {
  readonly projectRootExists: boolean;
  readonly worktreePathExists: boolean;
}): boolean {
  return input.projectRootExists || input.worktreePathExists;
}

export interface VcsMaintenanceReactorShape {
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  readonly sweep: () => Effect.Effect<void>;
}

export class VcsMaintenanceReactor extends Context.Service<
  VcsMaintenanceReactor,
  VcsMaintenanceReactorShape
>()("t3/vcs/VcsMaintenanceReactor") {}

type ProjectRootRow = {
  readonly workspaceRoot: string;
};

type WorktreeRow = {
  readonly threadId: string;
  readonly projectCwd: string;
  readonly worktreePath: string | null;
  readonly worktreeRemovable: number;
  readonly worktreeRemovalPath: string | null;
  readonly updatedAt: string;
  readonly archivedAt: string | null;
  readonly deletedAt: string | null;
  readonly sessionStatus: string | null;
  readonly runtimeStatus: string | null;
  readonly pendingApprovalCount: number;
  readonly pendingUserInputCount: number;
};

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const checkpointStore = yield* CheckpointStore.CheckpointStore;
  const gitWorkflow = yield* GitWorkflowService.GitWorkflowService;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const orchestrationEngine = yield* OrchestrationEngineService;
  const crypto = yield* Crypto.Crypto;
  const fileSystem = yield* FileSystem.FileSystem;

  const serverCommandId = (tag: string) =>
    crypto.randomUUIDv4.pipe(Effect.map((uuid) => CommandId.make(`server:${tag}:${uuid}`)));

  const errorDetail = (error: unknown): string =>
    error instanceof Error ? error.message : String(error);

  const listProjectRoots = Effect.fn("VcsMaintenanceReactor.listProjectRoots")(function* () {
    const rows = yield* sql<ProjectRootRow>`
      SELECT workspace_root AS "workspaceRoot"
      FROM projection_projects
    `;
    return rows.map((row) => row.workspaceRoot);
  });

  const listActiveProjectRoots = Effect.fn("VcsMaintenanceReactor.listActiveProjectRoots")(
    function* () {
      const rows = yield* sql<ProjectRootRow>`
      SELECT workspace_root AS "workspaceRoot"
      FROM projection_projects
      WHERE deleted_at IS NULL
    `;
      return rows.map((row) => row.workspaceRoot);
    },
  );

  const listWorktreeRows = Effect.fn("VcsMaintenanceReactor.listWorktreeRows")(function* () {
    const rows = yield* sql<WorktreeRow>`
      SELECT
        t.thread_id AS "threadId",
        p.workspace_root AS "projectCwd",
        t.worktree_path AS "worktreePath",
        t.worktree_removable AS "worktreeRemovable",
        t.worktree_removal_path AS "worktreeRemovalPath",
        t.updated_at AS "updatedAt",
        t.archived_at AS "archivedAt",
        t.deleted_at AS "deletedAt",
        s.status AS "sessionStatus",
        r.status AS "runtimeStatus",
        t.pending_approval_count AS "pendingApprovalCount",
        t.pending_user_input_count AS "pendingUserInputCount"
      FROM projection_threads t
      INNER JOIN projection_projects p ON p.project_id = t.project_id
      LEFT JOIN projection_thread_sessions s ON s.thread_id = t.thread_id
      LEFT JOIN provider_session_runtime r ON r.thread_id = t.thread_id
      WHERE t.worktree_path IS NOT NULL
    `;
    return rows.map(
      (row): WorktreeMaintenanceRow => ({
        ...row,
        worktreeRemovable: row.worktreeRemovable > 0,
      }),
    );
  });

  const isWorktreeRegistered = Effect.fn("VcsMaintenanceReactor.isWorktreeRegistered")(function* (
    candidate: WorktreeReapCandidate,
  ) {
    const pathExists = (path: string) =>
      fileSystem.stat(path).pipe(
        Effect.as(true),
        Effect.catchTags({
          PlatformError: (error: PlatformError.PlatformError) =>
            Effect.succeed(error.reason._tag !== "NotFound"),
        }),
      );

    const failClosedUnlessBothPathsAreGone = Effect.fn(
      "VcsMaintenanceReactor.failClosedUnlessBothPathsAreGone",
    )(function* (detail: string) {
      const [projectRootExists, worktreePathExists] = yield* Effect.all([
        pathExists(candidate.projectCwd),
        pathExists(candidate.path),
      ]);
      if (!projectRootExists && !worktreePathExists) {
        yield* Effect.logWarning("vcs.maintenance.worktree-list-root-missing", {
          threadIds: candidate.threadIds,
          projectRoot: candidate.projectCwd,
          path: candidate.path,
          detail,
        });
      }
      return shouldRetainWorktreeMetadataAfterListFailure({
        projectRootExists,
        worktreePathExists,
      });
    });

    const result = yield* git
      .execute({
        operation: "VcsMaintenanceReactor.isWorktreeRegistered",
        cwd: candidate.projectCwd,
        args: ["worktree", "list", "--porcelain"],
        allowNonZeroExit: true,
        timeoutMs: 5_000,
      })
      .pipe(Effect.result);

    if (Result.isFailure(result)) {
      yield* Effect.logWarning("vcs.maintenance.worktree-list-failed", {
        threadIds: candidate.threadIds,
        projectRoot: candidate.projectCwd,
        path: candidate.path,
        detail: errorDetail(result.failure),
      });
      return yield* failClosedUnlessBothPathsAreGone(errorDetail(result.failure));
    }

    if (result.success.exitCode !== 0) {
      const detail = result.success.stderr.trim() || "git worktree list failed";
      yield* Effect.logWarning("vcs.maintenance.worktree-list-failed", {
        threadIds: candidate.threadIds,
        projectRoot: candidate.projectCwd,
        path: candidate.path,
        detail,
      });
      return yield* failClosedUnlessBothPathsAreGone(detail);
    }

    return isWorktreePathListed(result.success.stdout, candidate.path);
  });

  const pruneProjectRepository = Effect.fn("VcsMaintenanceReactor.pruneProjectRepository")(
    function* (projectRoot: string) {
      const isGitRepository = yield* checkpointStore
        .isGitRepository(projectRoot)
        .pipe(Effect.orElseSucceed(() => false));
      if (!isGitRepository) {
        return;
      }

      yield* checkpointStore
        .pruneCheckpointRefs({
          cwd: projectRoot,
          keepPerThread: CHECKPOINT_REFS_KEEP_PER_THREAD,
        })
        .pipe(
          Effect.tap((checkpointResult) =>
            checkpointResult.deletedCount > 0
              ? Effect.logInfo("vcs.maintenance.checkpoint-refs-pruned", {
                  projectRoot,
                  ...checkpointResult,
                })
              : Effect.void,
          ),
          Effect.catch((error) =>
            Effect.logWarning("vcs.maintenance.checkpoint-refs-prune-failed", {
              projectRoot,
              detail: errorDetail(error),
            }),
          ),
        );

      yield* git.pruneWorktrees(projectRoot).pipe(
        Effect.catch((error) =>
          Effect.logWarning("vcs.maintenance.worktree-prune-failed", {
            projectRoot,
            detail: errorDetail(error),
          }),
        ),
      );
    },
  );

  const reapWorktrees = Effect.fn("VcsMaintenanceReactor.reapWorktrees")(function* (
    projectRoots: ReadonlyArray<string>,
  ) {
    const rows = yield* listWorktreeRows();
    const now = yield* DateTime.now;
    const candidates = selectStaleWorktreeReapCandidates(
      rows,
      projectRoots,
      DateTime.toEpochMillis(now),
    );

    const clearCandidateMetadata = Effect.fn("VcsMaintenanceReactor.clearCandidateMetadata")(
      function* (candidate: WorktreeReapCandidate) {
        for (const threadId of candidate.threadIds) {
          const commandId = yield* serverCommandId("vcs-maintenance-worktree-reaped").pipe(
            Effect.orDie,
          );
          yield* orchestrationEngine
            .dispatch({
              type: "thread.meta.update",
              commandId,
              threadId: ThreadId.make(threadId),
              worktreePath: null,
              worktreeRemovable: false,
              worktreeRemovalPath: null,
            })
            .pipe(
              Effect.tap(() =>
                Effect.logInfo("vcs.maintenance.worktree-reaped", {
                  threadId,
                  projectRoot: candidate.projectCwd,
                  path: candidate.path,
                }),
              ),
              Effect.catchCause((cause) =>
                Effect.logWarning("vcs.maintenance.worktree-metadata-clear-failed", {
                  threadId,
                  projectRoot: candidate.projectCwd,
                  path: candidate.path,
                  cause: Cause.pretty(cause),
                }),
              ),
            );
        }
      },
    );

    for (const candidate of candidates) {
      if (yield* isWorktreeRegistered(candidate)) {
        const removeResult = yield* gitWorkflow
          .removeWorktree({
            cwd: candidate.projectCwd,
            path: candidate.path,
          })
          .pipe(Effect.result);
        if (Result.isFailure(removeResult)) {
          yield* Effect.logWarning("vcs.maintenance.worktree-remove-failed", {
            threadIds: candidate.threadIds,
            projectRoot: candidate.projectCwd,
            path: candidate.path,
            detail: errorDetail(removeResult.failure),
          });
          yield* git.pruneWorktrees(candidate.projectCwd).pipe(
            Effect.catch((error) =>
              Effect.logWarning("vcs.maintenance.worktree-prune-failed", {
                projectRoot: candidate.projectCwd,
                detail: errorDetail(error),
              }),
            ),
          );
          if (yield* isWorktreeRegistered(candidate)) {
            continue;
          }
        }
      }
      yield* clearCandidateMetadata(candidate);
    }
  });

  const runSweep = Effect.fn("VcsMaintenanceReactor.runSweep")(function* () {
    const protectionProjectRoots = yield* listProjectRoots();
    yield* reapWorktrees(protectionProjectRoots);
    const activeProjectRoots = yield* listActiveProjectRoots();
    for (const projectRoot of activeProjectRoots) {
      yield* pruneProjectRepository(projectRoot);
    }
  });

  const sweep: VcsMaintenanceReactorShape["sweep"] = () =>
    runSweep().pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("vcs.maintenance.sweep-failed", {
          cause: Cause.pretty(cause),
        }),
      ),
    );

  const start: VcsMaintenanceReactorShape["start"] = () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(
        Effect.sleep(MAINTENANCE_BOOT_DELAY).pipe(
          Effect.andThen(sweep()),
          Effect.repeat(Schedule.spaced(MAINTENANCE_SWEEP_INTERVAL)),
        ),
      );
      yield* Effect.logInfo("vcs.maintenance.reactor.started", {
        checkpointRefsKeepPerThread: CHECKPOINT_REFS_KEEP_PER_THREAD,
        sweepIntervalMs: Duration.toMillis(MAINTENANCE_SWEEP_INTERVAL),
        stoppedWorktreeReapAgeMs: STOPPED_WORKTREE_REAP_AGE_MS,
        archivedWorktreeReapAgeMs: ARCHIVED_WORKTREE_REAP_AGE_MS,
      });
    });

  return {
    start,
    sweep,
  } satisfies VcsMaintenanceReactorShape;
});

export const layer = Layer.effect(VcsMaintenanceReactor, make);
