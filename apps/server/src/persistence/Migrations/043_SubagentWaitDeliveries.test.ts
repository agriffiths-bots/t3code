import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("043_SubagentWaitDeliveries", (it) => {
  it.effect("creates durable wait-delivery tombstones keyed by child", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 43 });

      const columns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(subagent_wait_deliveries)
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
        columns.some((column) => column.name === "delivered_at"),
        "missing delivered_at column",
      );
      assert.isTrue(
        columns.some((column) => column.name === "parent_turn_id_at_delivery"),
        "missing parent_turn_id_at_delivery column",
      );

      yield* sql`
        INSERT INTO subagent_wait_deliveries (
          child_thread_id,
          parent_thread_id,
          delivered_at,
          parent_turn_id_at_delivery
        ) VALUES (
          'child-1',
          'parent-1',
          '2026-06-17T09:00:00.000Z',
          'turn-parent-1'
        )
        ON CONFLICT (child_thread_id) DO UPDATE SET
          parent_thread_id = excluded.parent_thread_id,
          delivered_at = excluded.delivered_at,
          parent_turn_id_at_delivery = excluded.parent_turn_id_at_delivery
      `;

      const rows = yield* sql<{
        readonly childThreadId: string;
        readonly parentTurnIdAtDelivery: string | null;
      }>`
        SELECT
          child_thread_id AS "childThreadId",
          parent_turn_id_at_delivery AS "parentTurnIdAtDelivery"
        FROM subagent_wait_deliveries
      `;
      assert.deepStrictEqual(rows, [
        { childThreadId: "child-1", parentTurnIdAtDelivery: "turn-parent-1" },
      ]);
    }),
  );
});
