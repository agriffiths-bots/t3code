import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("046_RemoteSubagentChildren", (it) => {
  it.effect("adds remote parent metadata and caller-side remote child rows", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 47 });

      const projectionColumns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_threads)
      `;
      assert.isTrue(
        projectionColumns.some((column) => column.name === "parent_environment_id"),
        "missing projection_threads.parent_environment_id column",
      );

      const remoteColumns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(remote_children)
      `;
      for (const name of [
        "parent_thread_id",
        "child_env_id",
        "child_thread_id",
        "alias",
        "spawn_params_json",
        "status",
        "last_polled_at",
        "terminal_delivery_claim_id",
        "terminal_delivery_claimed_at",
      ]) {
        assert.isTrue(
          remoteColumns.some((column) => column.name === name),
          `missing remote_children.${name} column`,
        );
      }

      yield* sql`
        INSERT INTO remote_children (
          parent_thread_id,
          child_env_id,
          child_thread_id,
          alias,
          spawn_params_json,
          status,
          last_polled_at,
          created_at,
          updated_at
        ) VALUES (
          'thread-parent',
          'env-b',
          'thread-child',
          'b',
          '{"prompt":"work"}',
          'running',
          NULL,
          '2026-07-08T09:00:00.000Z',
          '2026-07-08T09:00:00.000Z'
        )
      `;

      const rows = yield* sql<{
        readonly parentThreadId: string;
        readonly childEnvironmentId: string;
        readonly childThreadId: string;
      }>`
        SELECT
          parent_thread_id AS "parentThreadId",
          child_env_id AS "childEnvironmentId",
          child_thread_id AS "childThreadId"
        FROM remote_children
      `;
      assert.deepStrictEqual(rows, [
        {
          parentThreadId: "thread-parent",
          childEnvironmentId: "env-b",
          childThreadId: "thread-child",
        },
      ]);
    }),
  );
});
