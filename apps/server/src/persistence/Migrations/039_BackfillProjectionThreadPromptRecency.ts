import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Effect from "effect/Effect";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    UPDATE projection_threads
    SET latest_user_message_at = (
      SELECT MAX(message.created_at)
      FROM projection_thread_messages AS message
      WHERE message.thread_id = projection_threads.thread_id
        AND (
          message.role = 'user'
          OR (
            message.role = 'system'
            AND ltrim(message.text) LIKE '[sub-agent %'
          )
        )
    )
  `;
});
