import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    ALTER TABLE auth_pairing_links
    ADD COLUMN audience_ceiling TEXT NOT NULL DEFAULT 'factory'
      CHECK (audience_ceiling IN ('private', 'factory'))
  `;
  yield* sql`
    ALTER TABLE auth_sessions
    ADD COLUMN audience_ceiling TEXT NOT NULL DEFAULT 'factory'
      CHECK (audience_ceiling IN ('private', 'factory'))
  `;

  const activatedAt = yield* sql<{ readonly activatedAt: string }>`
    SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now') AS "activatedAt"
  `;
  const revokedAt = activatedAt[0]?.activatedAt;
  if (revokedAt === undefined) {
    return yield* Effect.die("Could not determine auth audience activation time.");
  }

  yield* sql`
    UPDATE auth_pairing_links
    SET revoked_at = ${revokedAt}
    WHERE revoked_at IS NULL
  `;
  yield* sql`
    UPDATE auth_sessions
    SET revoked_at = ${revokedAt}
    WHERE revoked_at IS NULL
  `;
});
