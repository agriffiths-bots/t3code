import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_threads)
  `;

  // NULL parent_thread_id = pre-migration row or a thread with no parent.
  if (!columns.some((column) => column.name === "parent_thread_id")) {
    yield* sql`
      ALTER TABLE projection_threads
      ADD COLUMN parent_thread_id TEXT
    `;
  }

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_projection_threads_parent_id
    ON projection_threads(parent_thread_id)
  `;
});
