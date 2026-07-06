import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("039_ProjectionThreadWorktreeRemovable", (it) => {
  it.effect("adds worktree cleanup metadata to projection_threads", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 38 });
      yield* runMigrations({ toMigrationInclusive: 39 });

      const columns = yield* sql<{ readonly name: string; readonly dflt_value: string | null }>`
        PRAGMA table_info(projection_threads)
      `;
      const column = columns.find((entry) => entry.name === "worktree_removable");
      assert.isDefined(column);
      assert.strictEqual(column.dflt_value, "0");
      assert.isTrue(columns.some((entry) => entry.name === "worktree_removal_path"));
    }),
  );
});
