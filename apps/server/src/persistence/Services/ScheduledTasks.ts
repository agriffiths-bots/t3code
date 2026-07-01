/**
 * ScheduledTaskRepository - Repository interface for scheduled tasks.
 *
 * Owns persistence operations for the plain (non event-sourced)
 * `scheduled_tasks` table that drives the scheduler reactor.
 *
 * @module ScheduledTaskRepository
 */
import {
  IsoDateTime,
  ModelSelection,
  NonNegativeInt,
  ScheduleBusyPolicy,
  type ScheduledTaskEntry,
  ScheduledTaskId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Stream from "effect/Stream";

import type { ProjectionRepositoryError } from "../Errors.ts";

// `ScheduledTaskId` and `ScheduleBusyPolicy` are the canonical schemas lifted
// into `@t3tools/contracts` so the MCP toolkit, the persistence row, and the
// web client subscription all share one brand. Re-exported here to keep the
// existing import sites (handlers.ts, tools.ts) unchanged.
export { ScheduleBusyPolicy, ScheduledTaskId };

export const ScheduleKind = Schema.Literals(["interval", "cron"]);
export type ScheduleKind = typeof ScheduleKind.Type;

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
  // Model/harness each dispatched run uses, or null to inherit the thread's
  // current model. Persisted as a JSON TEXT column (see the DB-row schema in
  // Layers/ScheduledTasks.ts); the reactor passes it as a per-turn override.
  modelSelection: Schema.NullOr(ModelSelection),
  createdAt: IsoDateTime,
});
export type ScheduledTask = typeof ScheduledTask.Type;

/**
 * Map a persisted `ScheduledTask` row to the canonical wire `ScheduledTaskEntry`.
 *
 * Shared by the MCP toolkit (t3_schedule_*) and the `subscribeScheduledTasks`
 * WS handler so both surfaces project the row identically: `enabled` 0/1 →
 * boolean, `timezoneName` → `timezone`, and the internal liveness counters
 * (skipped/retry/queued, lastError, createdAt) are dropped from the wire shape.
 */
export const toScheduleEntry = (task: ScheduledTask): ScheduledTaskEntry => ({
  taskId: task.taskId,
  threadId: task.threadId,
  prompt: task.prompt,
  scheduleKind: task.scheduleKind,
  intervalSeconds: task.intervalSeconds,
  cronExpr: task.cronExpr,
  timezone: task.timezoneName,
  enabled: task.enabled !== 0,
  busyPolicy: task.busyPolicy,
  nextRunAt: task.nextRunAt,
  lastRunAt: task.lastRunAt,
  lastStatus: task.lastStatus,
  modelSelection: task.modelSelection,
});

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

  /**
   * In-process liveness signal. The `scheduled_tasks` table is not
   * event-sourced and emits no domain events, so this monotonic revision
   * counter is the smallest native freshness primitive: every write
   * (insert/update/delete/markRun) bumps it, and the `subscribeScheduledTasks`
   * WS handler re-reads `listAll()` on each emission. Sub-second freshness,
   * no polling. The stream replays the current value on subscribe.
   */
  readonly revisionChanges: Stream.Stream<number>;
}

/**
 * ScheduledTaskRepository - Service tag for scheduled task persistence.
 */
export class ScheduledTaskRepository extends Context.Service<
  ScheduledTaskRepository,
  ScheduledTaskRepositoryShape
>()("t3/persistence/Services/ScheduledTasks/ScheduledTaskRepository") {}
