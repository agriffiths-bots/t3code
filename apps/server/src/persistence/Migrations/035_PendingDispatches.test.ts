import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("035_PendingDispatches", (it) => {
  it.effect("creates the pending_dispatches table with the expected columns and index", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 35 });

      const columns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(pending_dispatches)
      `;
      const names = columns.map((column) => column.name);
      for (const expected of [
        "id",
        "kind",
        "target_thread_id",
        "source_child_id",
        "text",
        "error",
        "status",
        "created_at",
      ]) {
        assert.isTrue(names.includes(expected), `missing column ${expected}`);
      }

      const indexes = yield* sql<{ readonly name: string }>`
        PRAGMA index_list(pending_dispatches)
      `;
      assert.isTrue(
        indexes.some((index) => index.name === "idx_pending_dispatches_kind_target"),
      );
    }),
  );

  it.effect("is idempotent when re-run", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 35 });
      yield* runMigrations({ toMigrationInclusive: 35 });

      const tables = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'pending_dispatches'
      `;
      assert.equal(tables.length, 1);
    }),
  );

  it.effect("supports insert and listByTarget filtering by kind and target", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 35 });

      yield* sql`
        INSERT INTO pending_dispatches (
          id, kind, target_thread_id, source_child_id, text, error, status, created_at
        ) VALUES
          ('inj-1', 'parent_injection', 'parent-1', 'child-1', 'done', NULL, NULL,
           '2026-06-17T09:00:00.000Z'),
          ('steer-1', 'child_steer', 'parent-1', NULL, 'retry', NULL, NULL,
           '2026-06-17T09:01:00.000Z'),
          ('inj-other', 'parent_injection', 'parent-2', 'child-2', 'done', NULL, NULL,
           '2026-06-17T09:02:00.000Z')
      `;

      const injections = yield* sql<{ readonly id: string }>`
        SELECT id FROM pending_dispatches
        WHERE kind = 'parent_injection' AND target_thread_id = 'parent-1'
        ORDER BY created_at ASC, id ASC
      `;
      assert.deepEqual(
        injections.map((row) => row.id),
        ["inj-1"],
      );
    }),
  );
});
