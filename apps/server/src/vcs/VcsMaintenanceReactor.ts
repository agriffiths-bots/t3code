import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import type * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import { MAX_THREAD_CHECKPOINTS } from "../orchestration/checkpointRetention.ts";

// The projection exposes the newest positive-turn checkpoints. Pruning also
// preserves turn 0 as the thread baseline and one predecessor for boundary
// per-turn diffs when those refs exist.
export const CHECKPOINT_REFS_KEEP_PER_THREAD = MAX_THREAD_CHECKPOINTS + 2;
const MAINTENANCE_BOOT_DELAY = Duration.seconds(10);
const MAINTENANCE_SWEEP_INTERVAL = Duration.minutes(30);

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

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const checkpointStore = yield* CheckpointStore.CheckpointStore;

  const errorDetail = (error: unknown): string =>
    error instanceof Error ? error.message : String(error);

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
    },
  );

  const runSweep = Effect.fn("VcsMaintenanceReactor.runSweep")(function* () {
    const activeProjectRoots = yield* listActiveProjectRoots();
    for (const projectRoot of activeProjectRoots) {
      yield* pruneProjectRepository(projectRoot);
    }
  });

  const sweep: VcsMaintenanceReactorShape["sweep"] = () =>
    runSweep().pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("vcs.maintenance.sweep-failed", {
          cause: errorDetail(cause),
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
      });
    });

  return {
    start,
    sweep,
  } satisfies VcsMaintenanceReactorShape;
});

export const layer = Layer.effect(VcsMaintenanceReactor, make);
