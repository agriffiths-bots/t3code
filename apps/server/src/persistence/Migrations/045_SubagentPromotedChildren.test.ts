import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("045_SubagentPromotedChildren", (it) => {
  it.effect("creates durable promoted-child markers keyed by child", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 45 });

      const columns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(subagent_promoted_children)
      `;
      assert.isTrue(
        columns.some((column) => column.name === "child_thread_id"),
        "missing child_thread_id column",
      );
      assert.isTrue(
        columns.some((column) => column.name === "parent_thread_id"),
        "missing parent_thread_id column",
      );
      assert.isTrue(
        columns.some((column) => column.name === "promoted_at"),
        "missing promoted_at column",
      );

      yield* sql`
        INSERT INTO subagent_promoted_children (
          child_thread_id,
          parent_thread_id,
          promoted_at
        ) VALUES (
          'child-1',
          'parent-1',
          '2026-06-17T09:00:00.000Z'
        )
        ON CONFLICT (child_thread_id) DO UPDATE SET
          parent_thread_id = excluded.parent_thread_id,
          promoted_at = excluded.promoted_at
      `;

      const rows = yield* sql<{
        readonly childThreadId: string;
        readonly parentThreadId: string;
      }>`
        SELECT
          child_thread_id AS "childThreadId",
          parent_thread_id AS "parentThreadId"
        FROM subagent_promoted_children
      `;
      assert.deepStrictEqual(rows, [{ childThreadId: "child-1", parentThreadId: "parent-1" }]);
    }),
  );
});
