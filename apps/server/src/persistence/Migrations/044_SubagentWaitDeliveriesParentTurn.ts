import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(subagent_wait_deliveries)
  `;
  if (columns.some((column) => column.name === "parent_turn_id_at_delivery")) {
    return;
  }

  yield* sql`
    ALTER TABLE subagent_wait_deliveries
    ADD COLUMN parent_turn_id_at_delivery TEXT
  `;
});
