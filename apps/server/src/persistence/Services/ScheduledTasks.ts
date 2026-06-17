/**
 * ScheduledTaskRepository - Repository interface for scheduled tasks.
 *
 * Owns persistence operations for the plain (non event-sourced)
 * `scheduled_tasks` table that drives the scheduler reactor.
 *
 * @module ScheduledTaskRepository
 */
import { IsoDateTime, NonNegativeInt, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { ProjectionRepositoryError } from "../Errors.ts";

export const ScheduledTaskId = Schema.String.pipe(Schema.brand("ScheduledTaskId"));
export type ScheduledTaskId = typeof ScheduledTaskId.Type;

export const ScheduleKind = Schema.Literals(["interval", "cron"]);
export type ScheduleKind = typeof ScheduleKind.Type;

export const ScheduleBusyPolicy = Schema.Literals(["skip", "queue_once"]);
export type ScheduleBusyPolicy = typeof ScheduleBusyPolicy.Type;

export const ScheduledTask = Schema.Struct({
  taskId: ScheduledTaskId,
  threadId: ThreadId,
  prompt: Schema.String,
  scheduleKind: ScheduleKind,
  intervalSeconds: Schema.NullOr(Schema.Int),
  cronExpr: Schema.NullOr(Schema.String),
  timezoneName: Schema.String,
  enabled: NonNegativeInt,
  busyPolicy: ScheduleBusyPolicy,
  nextRunAt: Schema.NullOr(IsoDateTime),
  lastRunAt: Schema.NullOr(IsoDateTime),
  lastStatus: Schema.NullOr(Schema.String),
  lastError: Schema.NullOr(Schema.String),
  skippedCount: NonNegativeInt,
  retryCount: NonNegativeInt,
  queuedCount: NonNegativeInt,
  createdAt: IsoDateTime,
});
export type ScheduledTask = typeof ScheduledTask.Type;

export const ListDueScheduledTasksInput = Schema.Struct({
  nowIso: IsoDateTime,
});
export type ListDueScheduledTasksInput = typeof ListDueScheduledTasksInput.Type;

export const ListScheduledTasksByThreadInput = Schema.Struct({
  threadId: ThreadId,
});
export type ListScheduledTasksByThreadInput = typeof ListScheduledTasksByThreadInput.Type;

export const DeleteScheduledTaskInput = Schema.Struct({
  taskId: ScheduledTaskId,
});
export type DeleteScheduledTaskInput = typeof DeleteScheduledTaskInput.Type;

export const MarkScheduledTaskRunInput = Schema.Struct({
  taskId: ScheduledTaskId,
  status: Schema.String,
  lastRunAt: IsoDateTime,
  nextRunAt: Schema.NullOr(IsoDateTime),
  error: Schema.optional(Schema.String),
});
export type MarkScheduledTaskRunInput = typeof MarkScheduledTaskRunInput.Type;

/**
 * ScheduledTaskRepositoryShape - Service API for scheduled task persistence.
 */
export interface ScheduledTaskRepositoryShape {
  /**
   * List enabled tasks whose `next_run_at` is at or before `nowIso`.
   *
   * Returned in `next_run_at` order (oldest first).
   */
  readonly listDue: (
    input: ListDueScheduledTasksInput,
  ) => Effect.Effect<ReadonlyArray<ScheduledTask>, ProjectionRepositoryError>;

  /**
   * Insert a new scheduled task row.
   */
  readonly insert: (task: ScheduledTask) => Effect.Effect<void, ProjectionRepositoryError>;

  /**
   * Replace an existing scheduled task row by `taskId`.
   */
  readonly update: (task: ScheduledTask) => Effect.Effect<void, ProjectionRepositoryError>;

  /**
   * Hard-delete a scheduled task row by id.
   */
  readonly delete: (
    input: DeleteScheduledTaskInput,
  ) => Effect.Effect<void, ProjectionRepositoryError>;

  /**
   * Record the outcome of a run and advance `next_run_at` atomically.
   *
   * The status update and the `next_run_at` advance are committed in a
   * single transaction so a crash mid-dispatch cannot re-run the task.
   */
  readonly markRun: (
    input: MarkScheduledTaskRunInput,
  ) => Effect.Effect<void, ProjectionRepositoryError>;

  /**
   * List all scheduled task rows.
   */
  readonly listAll: () => Effect.Effect<ReadonlyArray<ScheduledTask>, ProjectionRepositoryError>;

  /**
   * List scheduled task rows for a thread.
   */
  readonly listByThread: (
    input: ListScheduledTasksByThreadInput,
  ) => Effect.Effect<ReadonlyArray<ScheduledTask>, ProjectionRepositoryError>;
}

/**
 * ScheduledTaskRepository - Service tag for scheduled task persistence.
 */
export class ScheduledTaskRepository extends Context.Service<
  ScheduledTaskRepository,
  ScheduledTaskRepositoryShape
>()("t3/persistence/Services/ScheduledTasks/ScheduledTaskRepository") {}
