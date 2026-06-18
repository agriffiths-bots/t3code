import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Effect from "effect/Effect";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS scheduled_tasks (
      task_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL,
      prompt TEXT NOT NULL,
      schedule_kind TEXT NOT NULL,
      interval_seconds INTEGER,
      cron_expr TEXT,
      timezone_name TEXT NOT NULL DEFAULT 'UTC',
      enabled INTEGER NOT NULL DEFAULT 1,
      busy_policy TEXT NOT NULL DEFAULT 'skip',
      next_run_at TEXT,
      last_run_at TEXT,
      last_status TEXT,
      last_error TEXT,
      skipped_count INTEGER NOT NULL DEFAULT 0,
      retry_count INTEGER NOT NULL DEFAULT 0,
      queued_count INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL
    )
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_enabled_next_run
    ON scheduled_tasks(enabled, next_run_at)
  `;
});
