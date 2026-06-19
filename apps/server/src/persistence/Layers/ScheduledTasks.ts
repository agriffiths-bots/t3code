import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

import { toPersistenceSqlError } from "../Errors.ts";
import {
  DeleteScheduledTaskInput,
  ListDueScheduledTasksInput,
  ListScheduledTasksByThreadInput,
  MarkScheduledTaskRunInput,
  ScheduledTask,
  ScheduledTaskRepository,
  type ScheduledTaskRepositoryShape,
} from "../Services/ScheduledTasks.ts";

const makeScheduledTaskRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Liveness counter (finalPlan: the table emits no domain events). Bumped
  // after every successful write so `subscribeScheduledTasks` can re-read.
  const revision = yield* SubscriptionRef.make(0);
  const bump = SubscriptionRef.update(revision, (n) => n + 1);

  const writeScheduledTaskRow = SqlSchema.void({
    Request: ScheduledTask,
    execute: (row) =>
      sql`
        INSERT INTO scheduled_tasks (
          task_id,
          thread_id,
          prompt,
          schedule_kind,
          interval_seconds,
          cron_expr,
          timezone_name,
          enabled,
          busy_policy,
          next_run_at,
          last_run_at,
          last_status,
          last_error,
          skipped_count,
          retry_count,
          queued_count,
          created_at
        )
        VALUES (
          ${row.taskId},
          ${row.threadId},
          ${row.prompt},
          ${row.scheduleKind},
          ${row.intervalSeconds},
          ${row.cronExpr},
          ${row.timezoneName},
          ${row.enabled},
          ${row.busyPolicy},
          ${row.nextRunAt},
          ${row.lastRunAt},
          ${row.lastStatus},
          ${row.lastError},
          ${row.skippedCount},
          ${row.retryCount},
          ${row.queuedCount},
          ${row.createdAt}
        )
        ON CONFLICT (task_id)
        DO UPDATE SET
          thread_id = excluded.thread_id,
          prompt = excluded.prompt,
          schedule_kind = excluded.schedule_kind,
          interval_seconds = excluded.interval_seconds,
          cron_expr = excluded.cron_expr,
          timezone_name = excluded.timezone_name,
          enabled = excluded.enabled,
          busy_policy = excluded.busy_policy,
          next_run_at = excluded.next_run_at,
          last_run_at = excluded.last_run_at,
          last_status = excluded.last_status,
          last_error = excluded.last_error,
          skipped_count = excluded.skipped_count,
          retry_count = excluded.retry_count,
          queued_count = excluded.queued_count,
          created_at = excluded.created_at
      `,
  });

  const listDueScheduledTaskRows = SqlSchema.findAll({
    Request: ListDueScheduledTasksInput,
    Result: ScheduledTask,
    execute: ({ nowIso }) =>
      sql`
        SELECT
          task_id AS "taskId",
          thread_id AS "threadId",
          prompt,
          schedule_kind AS "scheduleKind",
          interval_seconds AS "intervalSeconds",
          cron_expr AS "cronExpr",
          timezone_name AS "timezoneName",
          enabled,
          busy_policy AS "busyPolicy",
          next_run_at AS "nextRunAt",
          last_run_at AS "lastRunAt",
          last_status AS "lastStatus",
          last_error AS "lastError",
          skipped_count AS "skippedCount",
          retry_count AS "retryCount",
          queued_count AS "queuedCount",
          created_at AS "createdAt"
        FROM scheduled_tasks
        WHERE enabled = 1
          AND next_run_at IS NOT NULL
          AND next_run_at <= ${nowIso}
        ORDER BY next_run_at ASC, task_id ASC
      `,
  });

  const listAllScheduledTaskRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ScheduledTask,
    execute: () =>
      sql`
        SELECT
          task_id AS "taskId",
          thread_id AS "threadId",
          prompt,
          schedule_kind AS "scheduleKind",
          interval_seconds AS "intervalSeconds",
          cron_expr AS "cronExpr",
          timezone_name AS "timezoneName",
          enabled,
          busy_policy AS "busyPolicy",
          next_run_at AS "nextRunAt",
          last_run_at AS "lastRunAt",
          last_status AS "lastStatus",
          last_error AS "lastError",
          skipped_count AS "skippedCount",
          retry_count AS "retryCount",
          queued_count AS "queuedCount",
          created_at AS "createdAt"
        FROM scheduled_tasks
        ORDER BY created_at ASC, task_id ASC
      `,
  });

  const listScheduledTaskRowsByThread = SqlSchema.findAll({
    Request: ListScheduledTasksByThreadInput,
    Result: ScheduledTask,
    execute: ({ threadId }) =>
      sql`
        SELECT
          task_id AS "taskId",
          thread_id AS "threadId",
          prompt,
          schedule_kind AS "scheduleKind",
          interval_seconds AS "intervalSeconds",
          cron_expr AS "cronExpr",
          timezone_name AS "timezoneName",
          enabled,
          busy_policy AS "busyPolicy",
          next_run_at AS "nextRunAt",
          last_run_at AS "lastRunAt",
          last_status AS "lastStatus",
          last_error AS "lastError",
          skipped_count AS "skippedCount",
          retry_count AS "retryCount",
          queued_count AS "queuedCount",
          created_at AS "createdAt"
        FROM scheduled_tasks
        WHERE thread_id = ${threadId}
        ORDER BY created_at ASC, task_id ASC
      `,
  });

  const deleteScheduledTaskRow = SqlSchema.void({
    Request: DeleteScheduledTaskInput,
    execute: ({ taskId }) =>
      sql`
        DELETE FROM scheduled_tasks
        WHERE task_id = ${taskId}
      `,
  });

  const markScheduledTaskRunRow = SqlSchema.void({
    Request: MarkScheduledTaskRunInput,
    execute: ({ taskId, status, lastRunAt, nextRunAt, error }) =>
      sql`
        UPDATE scheduled_tasks
        SET last_status = ${status},
            last_error = ${error ?? null},
            last_run_at = ${lastRunAt},
            next_run_at = ${nextRunAt}
        WHERE task_id = ${taskId}
      `,
  });

  const listDue: ScheduledTaskRepositoryShape["listDue"] = (input) =>
    listDueScheduledTaskRows(input).pipe(
      Effect.mapError(toPersistenceSqlError("ScheduledTaskRepository.listDue:query")),
    );

  const insert: ScheduledTaskRepositoryShape["insert"] = (task) =>
    writeScheduledTaskRow(task).pipe(
      Effect.mapError(toPersistenceSqlError("ScheduledTaskRepository.insert:query")),
      Effect.tap(() => bump),
    );

  const update: ScheduledTaskRepositoryShape["update"] = (task) =>
    writeScheduledTaskRow(task).pipe(
      Effect.mapError(toPersistenceSqlError("ScheduledTaskRepository.update:query")),
      Effect.tap(() => bump),
    );

  const deleteTask: ScheduledTaskRepositoryShape["delete"] = (input) =>
    deleteScheduledTaskRow(input).pipe(
      Effect.mapError(toPersistenceSqlError("ScheduledTaskRepository.delete:query")),
      Effect.tap(() => bump),
    );

  // markRun and the next_run_at advance are committed in a single
  // BEGIN IMMEDIATE transaction so the write lock is taken up front and a
  // crash after commit cannot re-run the same trigger (exactly-once-per-tick).
  const markRun: ScheduledTaskRepositoryShape["markRun"] = (input) =>
    sql`BEGIN IMMEDIATE`.pipe(
      Effect.andThen(markScheduledTaskRunRow(input)),
      Effect.andThen(sql`COMMIT`),
      Effect.catch((error) => sql`ROLLBACK`.pipe(Effect.andThen(Effect.fail(error)))),
      Effect.asVoid,
      Effect.mapError(toPersistenceSqlError("ScheduledTaskRepository.markRun:query")),
      Effect.tap(() => bump),
    );

  const listAll: ScheduledTaskRepositoryShape["listAll"] = () =>
    listAllScheduledTaskRows().pipe(
      Effect.mapError(toPersistenceSqlError("ScheduledTaskRepository.listAll:query")),
    );

  const listByThread: ScheduledTaskRepositoryShape["listByThread"] = (input) =>
    listScheduledTaskRowsByThread(input).pipe(
      Effect.mapError(toPersistenceSqlError("ScheduledTaskRepository.listByThread:query")),
    );

  return {
    listDue,
    insert,
    update,
    delete: deleteTask,
    markRun,
    listAll,
    listByThread,
    revisionChanges: SubscriptionRef.changes(revision),
  } satisfies ScheduledTaskRepositoryShape;
});

export const ScheduledTaskRepositoryLive = Layer.effect(
  ScheduledTaskRepository,
  makeScheduledTaskRepository,
);
