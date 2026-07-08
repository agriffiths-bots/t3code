import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("042_PendingDispatchesDeliveredByWait", (it) => {
  it.effect("adds wait-delivery markers with false defaults", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 42 });

      const columns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(pending_dispatches)
      `;
      assert.isTrue(
        columns.some((column) => column.name === "delivered_by_wait"),
        "missing delivered_by_wait column",
      );
      assert.isTrue(
        columns.some((column) => column.name === "wait_cancellable"),
        "missing wait_cancellable column",
      );

      yield* sql`
        INSERT INTO pending_dispatches (
          id, kind, target_thread_id, source_child_id, text, error, status, command_id, created_at
        ) VALUES (
          'delivered-default',
          'parent_injection',
          'parent-1',
          'child-1',
          'done',
          NULL,
          'completed',
          NULL,
          '2026-06-17T09:00:00.000Z'
        )
      `;

      const rows = yield* sql<{
        readonly deliveredByWait: number;
        readonly waitCancellable: number;
      }>`
        SELECT
          delivered_by_wait AS "deliveredByWait",
          wait_cancellable AS "waitCancellable"
        FROM pending_dispatches
        WHERE id = 'delivered-default'
      `;
      assert.equal(rows[0]?.deliveredByWait, 0);
      assert.equal(rows[0]?.waitCancellable, 0);
    }),
  );
});
