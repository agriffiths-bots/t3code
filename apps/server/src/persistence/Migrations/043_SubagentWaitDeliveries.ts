import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Effect from "effect/Effect";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS subagent_wait_deliveries (
      child_thread_id TEXT PRIMARY KEY,
      parent_thread_id TEXT NOT NULL,
      delivered_at TEXT NOT NULL,
      parent_turn_id_at_delivery TEXT
    )
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_subagent_wait_deliveries_parent
    ON subagent_wait_deliveries(parent_thread_id)
  `;
});
