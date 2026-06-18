import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Effect from "effect/Effect";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS pending_dispatches (
      id TEXT PRIMARY KEY,
      kind TEXT NOT NULL,
      target_thread_id TEXT NOT NULL,
      source_child_id TEXT,
      text TEXT,
      error TEXT,
      status TEXT,
      created_at TEXT NOT NULL
    )
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_pending_dispatches_kind_target
    ON pending_dispatches(kind, target_thread_id)
  `;
});
