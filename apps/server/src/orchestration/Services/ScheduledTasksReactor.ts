/**
 * ScheduledTasksReactor - background reactor that fires due scheduled tasks.
 *
 * On a fixed tick it lists due rows from the `scheduled_tasks` table, validates
 * the target thread still exists, computes the next run in the task's timezone,
 * and dispatches a `thread.turn.start` to the SAME stored thread with the stored
 * prompt. The next-run advance is committed (via the repo's `BEGIN IMMEDIATE`
 * `markRun`) BEFORE the dispatch is awaited, so a crash mid-dispatch can never
 * re-run the same trigger (at-most-once per tick). See finalPlan §7.
 *
 * @module ScheduledTasksReactor
 */
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

/** Tick cadence of the scheduler sweep. */
export const SCHED_TICK_SECONDS = 15;

export interface ScheduledTasksReactorShape {
  /**
   * Start the background scheduler sweep within the provided scope.
   */
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
}

export class ScheduledTasksReactor extends Context.Service<
  ScheduledTasksReactor,
  ScheduledTasksReactorShape
>()("t3/orchestration/Services/ScheduledTasksReactor") {}
