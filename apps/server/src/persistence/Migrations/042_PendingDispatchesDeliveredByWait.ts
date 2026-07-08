import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Effect from "effect/Effect";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    ALTER TABLE pending_dispatches
    ADD COLUMN delivered_by_wait INTEGER NOT NULL DEFAULT 0
  `;
  yield* sql`
    ALTER TABLE pending_dispatches
    ADD COLUMN wait_cancellable INTEGER NOT NULL DEFAULT 0
  `;
});
