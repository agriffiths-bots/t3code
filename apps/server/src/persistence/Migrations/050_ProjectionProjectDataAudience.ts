import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    ALTER TABLE projection_projects
    ADD COLUMN data_audience TEXT NOT NULL DEFAULT 'private'
      CHECK (data_audience IN ('private', 'factory'))
  `;

  yield* sql`
    UPDATE orchestration_events
    SET payload_json = json_set(payload_json, '$.dataAudience', 'private')
    WHERE event_type = 'project.created'
      AND json_type(payload_json, '$.dataAudience') IS NULL
  `;
});
