import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const projectionThreadColumns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_threads)
  `;
  if (!projectionThreadColumns.some((column) => column.name === "parent_environment_id")) {
    yield* sql`
      ALTER TABLE projection_threads
      ADD COLUMN parent_environment_id TEXT
    `;
  }

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_projection_threads_parent_environment
    ON projection_threads(parent_environment_id, parent_thread_id)
  `;

  yield* sql`
    CREATE TABLE IF NOT EXISTS remote_children (
      parent_thread_id TEXT NOT NULL,
      child_env_id TEXT NOT NULL,
      child_thread_id TEXT NOT NULL,
      alias TEXT NOT NULL,
      spawn_params_json TEXT NOT NULL,
      status TEXT NOT NULL,
      last_polled_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (parent_thread_id, child_env_id, child_thread_id)
    )
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_remote_children_parent
    ON remote_children(parent_thread_id, created_at, child_env_id, child_thread_id)
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_remote_children_child
    ON remote_children(child_env_id, child_thread_id)
  `;
});
