import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("050_ProjectionProjectDataAudience", (it) => {
  it.effect("backfills pre-audience projections and events as private", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 49 });
      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'project-pre-audience',
          'Pre-audience project',
          '/tmp/project-pre-audience',
          NULL,
          '[]',
          '2026-07-17T00:00:00.000Z',
          '2026-07-17T00:00:00.000Z',
          NULL
        )
      `;
      yield* sql`
        INSERT INTO orchestration_events (
          event_id,
          aggregate_kind,
          stream_id,
          stream_version,
          event_type,
          occurred_at,
          command_id,
          causation_event_id,
          correlation_id,
          actor_kind,
          payload_json,
          metadata_json
        )
        VALUES (
          'event-pre-audience',
          'project',
          'project-pre-audience',
          0,
          'project.created',
          '2026-07-17T00:00:00.000Z',
          'command-pre-audience',
          NULL,
          'command-pre-audience',
          'client',
          '{"projectId":"project-pre-audience","title":"Pre-audience project","workspaceRoot":"/tmp/project-pre-audience","defaultModelSelection":null,"scripts":[],"createdAt":"2026-07-17T00:00:00.000Z","updatedAt":"2026-07-17T00:00:00.000Z"}',
          '{}'
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 50 });

      const projectRows = yield* sql<{ readonly dataAudience: string }>`
        SELECT data_audience AS "dataAudience"
        FROM projection_projects
        WHERE project_id = 'project-pre-audience'
      `;
      const eventRows = yield* sql<{ readonly dataAudience: string }>`
        SELECT json_extract(payload_json, '$.dataAudience') AS "dataAudience"
        FROM orchestration_events
        WHERE event_id = 'event-pre-audience'
      `;

      assert.deepEqual(projectRows, [{ dataAudience: "private" }]);
      assert.deepEqual(eventRows, [{ dataAudience: "private" }]);
    }),
  );
});
