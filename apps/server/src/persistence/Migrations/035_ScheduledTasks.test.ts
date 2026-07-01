import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("035_ScheduledTasks", (it) => {
  it.effect("creates the scheduled_tasks table with the expected columns and index", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 35 });

      const columns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(scheduled_tasks)
      `;
      const names = new Set(columns.map((column) => column.name));
      for (const expected of [
        "task_id",
        "thread_id",
        "prompt",
        "schedule_kind",
        "interval_seconds",
        "cron_expr",
        "timezone_name",
        "enabled",
        "busy_policy",
        "next_run_at",
        "last_run_at",
        "last_status",
        "last_error",
        "skipped_count",
        "retry_count",
        "queued_count",
        "created_at",
      ]) {
        assert.isTrue(names.has(expected), `missing column ${expected}`);
      }

      const indexes = yield* sql<{ readonly name: string }>`
        PRAGMA index_list(scheduled_tasks)
      `;
      assert.isTrue(indexes.some((index) => index.name === "idx_scheduled_tasks_enabled_next_run"));
    }),
  );

  it.effect("is idempotent when re-run", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 35 });
      yield* runMigrations({ toMigrationInclusive: 35 });

      const tables = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'scheduled_tasks'
      `;
      assert.equal(tables.length, 1);
    }),
  );

  it.effect("supports insert and listDue filtering by enabled and next_run_at", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 35 });

      yield* sql`
        INSERT INTO scheduled_tasks (
          task_id, thread_id, prompt, schedule_kind, timezone_name,
          enabled, busy_policy, next_run_at, skipped_count, retry_count,
          queued_count, created_at
        ) VALUES
          ('due-now', 'thread-1', 'p', 'interval', 'UTC', 1, 'skip',
           '2026-06-17T10:00:00.000Z', 0, 0, 0, '2026-06-17T09:00:00.000Z'),
          ('due-future', 'thread-1', 'p', 'interval', 'UTC', 1, 'skip',
           '2026-06-17T12:00:00.000Z', 0, 0, 0, '2026-06-17T09:00:00.000Z'),
          ('due-disabled', 'thread-1', 'p', 'interval', 'UTC', 0, 'skip',
           '2026-06-17T10:00:00.000Z', 0, 0, 0, '2026-06-17T09:00:00.000Z')
      `;

      const due = yield* sql<{ readonly task_id: string }>`
        SELECT task_id FROM scheduled_tasks
        WHERE enabled = 1
          AND next_run_at IS NOT NULL
          AND next_run_at <= '2026-06-17T10:30:00.000Z'
        ORDER BY next_run_at ASC, task_id ASC
      `;
      assert.deepEqual(
        due.map((row) => row.task_id),
        ["due-now"],
      );
    }),
  );
});
