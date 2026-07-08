import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import Migration0044 from "./044_SubagentWaitDeliveriesParentTurn.ts";

const freshLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const legacyLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

freshLayer("044_SubagentWaitDeliveriesParentTurn fresh database", (it) => {
  it.effect("keeps fresh databases compatible when migration 43 already created the column", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 44 });

      const columns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(subagent_wait_deliveries)
      `;
      assert.isTrue(
        columns.some((column) => column.name === "parent_turn_id_at_delivery"),
        "missing parent_turn_id_at_delivery column",
      );
    }),
  );
});

legacyLayer("044_SubagentWaitDeliveriesParentTurn legacy database", (it) => {
  it.effect("adds the parent turn column to databases that already ran old migration 43", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* sql`
        CREATE TABLE subagent_wait_deliveries (
          child_thread_id TEXT PRIMARY KEY,
          parent_thread_id TEXT NOT NULL,
          delivered_at TEXT NOT NULL
        )
      `;

      yield* Migration0044;

      const columns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(subagent_wait_deliveries)
      `;
      assert.isTrue(
        columns.some((column) => column.name === "parent_turn_id_at_delivery"),
        "missing parent_turn_id_at_delivery column",
      );
    }),
  );
});
