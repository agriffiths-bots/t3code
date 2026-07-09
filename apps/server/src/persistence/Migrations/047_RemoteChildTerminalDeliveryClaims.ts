import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(remote_children)
  `;

  if (!columns.some((column) => column.name === "terminal_delivery_claim_id")) {
    yield* sql`
      ALTER TABLE remote_children
      ADD COLUMN terminal_delivery_claim_id TEXT
    `;
  }

  if (!columns.some((column) => column.name === "terminal_delivery_claimed_at")) {
    yield* sql`
      ALTER TABLE remote_children
      ADD COLUMN terminal_delivery_claimed_at TEXT
    `;
  }
});
