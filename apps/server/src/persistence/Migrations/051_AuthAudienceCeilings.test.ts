import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("051_AuthAudienceCeilings", (it) => {
  it.effect("revokes every pre-audience grant and session at activation", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 50 });
      yield* sql`
        INSERT INTO auth_pairing_links (
          id,
          credential,
          method,
          scopes,
          subject,
          label,
          proof_key_thumbprint,
          created_at,
          expires_at,
          consumed_at,
          revoked_at
        )
        VALUES (
          'pre-audience-grant',
          'pre-audience-credential',
          'one-time-token',
          '["orchestration:read"]',
          'one-time-token',
          NULL,
          NULL,
          '2026-07-17T00:00:00.000Z',
          '2026-07-18T00:00:00.000Z',
          NULL,
          NULL
        )
      `;
      yield* sql`
        INSERT INTO auth_sessions (
          session_id,
          subject,
          scopes,
          method,
          client_device_type,
          issued_at,
          expires_at,
          revoked_at
        )
        VALUES (
          'pre-audience-session',
          'one-time-token',
          '["orchestration:read"]',
          'bearer-access-token',
          'unknown',
          '2026-07-17T00:00:00.000Z',
          '2026-07-18T00:00:00.000Z',
          NULL
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 51 });

      const grants = yield* sql<{
        readonly audienceCeiling: string;
        readonly revokedAt: string | null;
      }>`
        SELECT
          audience_ceiling AS "audienceCeiling",
          revoked_at AS "revokedAt"
        FROM auth_pairing_links
        WHERE id = 'pre-audience-grant'
      `;
      const sessions = yield* sql<{
        readonly audienceCeiling: string;
        readonly revokedAt: string | null;
      }>`
        SELECT
          audience_ceiling AS "audienceCeiling",
          revoked_at AS "revokedAt"
        FROM auth_sessions
        WHERE session_id = 'pre-audience-session'
      `;

      assert.deepEqual(
        grants.map(({ audienceCeiling }) => audienceCeiling),
        ["factory"],
      );
      assert.deepEqual(
        sessions.map(({ audienceCeiling }) => audienceCeiling),
        ["factory"],
      );
      assert.isString(grants[0]?.revokedAt);
      assert.isString(sessions[0]?.revokedAt);
    }),
  );
});
