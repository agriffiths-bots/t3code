import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("055_ProjectionThreadsSettled", (it) => {
  it.effect("migrates an existing thread as unsettled after the deployed migration watermark", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 54 });
      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          worktree_removable,
          worktree_removal_path,
          latest_turn_id,
          created_at,
          updated_at,
          archived_at,
          latest_user_message_at,
          pending_approval_count,
          pending_user_input_count,
          has_actionable_proposed_plan,
          deleted_at,
          parent_thread_id,
          parent_environment_id
        )
        VALUES (
          'thread-before-settlement',
          'project-1',
          'Existing thread',
          '{"instanceId":"codex","model":"gpt-5.4"}',
          'full-access',
          'default',
          NULL,
          NULL,
          0,
          NULL,
          NULL,
          '2026-07-21T00:00:00.000Z',
          '2026-07-21T00:00:00.000Z',
          NULL,
          NULL,
          0,
          0,
          0,
          NULL,
          NULL,
          NULL
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 55 });

      const rows = yield* sql<{
        readonly threadId: string;
        readonly settledOverride: string | null;
        readonly settledAt: string | null;
      }>`
        SELECT
          thread_id AS "threadId",
          settled_override AS "settledOverride",
          settled_at AS "settledAt"
        FROM projection_threads
        WHERE thread_id = 'thread-before-settlement'
      `;

      assert.deepEqual(rows, [
        {
          threadId: "thread-before-settlement",
          settledOverride: null,
          settledAt: null,
        },
      ]);
    }),
  );
});
