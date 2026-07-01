import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Effect from "effect/Effect";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Per-schedule model/harness routing (Fix 1): a schedule may pin the model its
  // dispatched turns run under, resolved from a plain model name at create/update
  // time. Stored as a JSON blob of the canonical `ModelSelection`; NULL means the
  // run inherits the target thread's current model. Existing rows default to NULL.
  yield* sql`
    ALTER TABLE scheduled_tasks
    ADD COLUMN model_selection TEXT
  `;
});
