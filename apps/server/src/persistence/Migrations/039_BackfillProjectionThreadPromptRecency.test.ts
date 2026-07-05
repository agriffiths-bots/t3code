import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("039_BackfillProjectionThreadPromptRecency", (it) => {
  it.effect("backfills latest prompt timestamps from sub-agent wake system messages", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 38 });

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
          latest_turn_id,
          latest_user_message_at,
          pending_approval_count,
          pending_user_input_count,
          has_actionable_proposed_plan,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES
          (
            'thread-recency',
            'project-1',
            'Thread Recency',
            '{"provider":"codex","model":"gpt-5-codex"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            '2026-02-24T00:01:00.000Z',
            0,
            0,
            0,
            '2026-02-24T00:00:00.000Z',
            '2026-02-24T00:04:00.000Z',
            NULL
          ),
          (
            'thread-no-prompts',
            'project-1',
            'Thread No Prompts',
            '{"provider":"codex","model":"gpt-5-codex"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            '2026-02-24T00:09:00.000Z',
            0,
            0,
            0,
            '2026-02-24T00:00:00.000Z',
            '2026-02-24T00:09:00.000Z',
            NULL
          )
      `;

      yield* sql`
        INSERT INTO projection_thread_messages (
          message_id,
          thread_id,
          turn_id,
          role,
          text,
          attachments_json,
          is_streaming,
          created_at,
          updated_at
        )
        VALUES
          (
            'message-user',
            'thread-recency',
            'turn-1',
            'user',
            'older prompt',
            NULL,
            0,
            '2026-02-24T00:01:00.000Z',
            '2026-02-24T00:01:00.000Z'
          ),
          (
            'message-system-wake',
            'thread-recency',
            'turn-2',
            'system',
            '  [sub-agent child-1 completed] done',
            NULL,
            0,
            '2026-02-24T00:03:00.000Z',
            '2026-02-24T00:03:00.000Z'
          ),
          (
            'message-system-other',
            'thread-recency',
            NULL,
            'system',
            'System maintenance',
            NULL,
            0,
            '2026-02-24T00:04:00.000Z',
            '2026-02-24T00:04:00.000Z'
          ),
          (
            'message-assistant',
            'thread-no-prompts',
            NULL,
            'assistant',
            'no prompt here',
            NULL,
            0,
            '2026-02-24T00:05:00.000Z',
            '2026-02-24T00:05:00.000Z'
          )
      `;

      yield* runMigrations({ toMigrationInclusive: 39 });

      const rows = yield* sql<{
        readonly threadId: string;
        readonly latestUserMessageAt: string | null;
      }>`
        SELECT
          thread_id AS "threadId",
          latest_user_message_at AS "latestUserMessageAt"
        FROM projection_threads
        ORDER BY thread_id ASC
      `;
      assert.deepEqual(rows, [
        {
          threadId: "thread-no-prompts",
          latestUserMessageAt: null,
        },
        {
          threadId: "thread-recency",
          latestUserMessageAt: "2026-02-24T00:03:00.000Z",
        },
      ]);
    }),
  );
});
