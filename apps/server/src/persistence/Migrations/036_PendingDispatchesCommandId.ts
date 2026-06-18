import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Effect from "effect/Effect";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Exactly-once delivery (R-B): the command_id a row was dispatched under is
  // claimed durably BEFORE the orchestration dispatch. A non-null command_id
  // means "this row's wake/steer turn was already dispatched under this exact
  // id"; on restart it is re-dispatched under the SAME id so the engine's
  // transactional receipt dedup makes a landed turn a no-op (no duplicate) and
  // an un-landed turn fire (no loss), independent of how the rows re-batch.
  yield* sql`
    ALTER TABLE pending_dispatches
    ADD COLUMN command_id TEXT
  `;
});
