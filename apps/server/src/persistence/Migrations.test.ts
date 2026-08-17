import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { migrationManifest, runMigrations } from "./Migrations.ts";
import * as NodeSqliteClient from "./NodeSqliteClient.ts";

it("keeps migration ids unique, ascending, and preserves the deployed tail mapping", () => {
  const ids = migrationManifest.map(([id]) => id);
  assert.deepStrictEqual(ids, [...new Set(ids)]);
  assert.deepStrictEqual(
    ids,
    [...ids].sort((left, right) => left - right),
  );
  assert.deepStrictEqual(
    migrationManifest.filter(([id]) => id >= 55),
    [
      [55, "ProjectionThreadsSettled"],
      [56, "ProjectionThreadsSnoozed"],
      [57, "ProjectionThreadTitleRegeneration"],
      [58, "ProjectionThreadsPinned"],
      [59, "ProjectionTurnsKeysetIndex"],
      [60, "ProjectionThreadsPinOrderKey"],
      [61, "ProjectionProjectsDefaultThreadEnvMode"],
      [62, "ProjectionProjectFaviconPath"],
    ],
  );
});

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("migration registry", (it) => {
  it.effect("migrates an empty database once and is idempotent", () =>
    Effect.gen(function* () {
      const firstRun = yield* runMigrations();
      assert.deepStrictEqual(
        firstRun.map(([id]) => id),
        migrationManifest.map(([id]) => id),
      );

      const secondRun = yield* runMigrations();
      assert.deepStrictEqual(secondRun, []);
    }),
  );
});
