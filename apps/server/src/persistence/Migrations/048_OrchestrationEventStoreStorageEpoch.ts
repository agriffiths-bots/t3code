import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS orchestration_event_store_metadata (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      storage_epoch TEXT NOT NULL
    )
  `;

  yield* sql`
    INSERT OR IGNORE INTO orchestration_event_store_metadata (singleton, storage_epoch)
    VALUES (1, lower(hex(randomblob(16))))
  `;
});
