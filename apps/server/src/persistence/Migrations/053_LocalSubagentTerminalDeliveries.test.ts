import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

const noisyChildDeliveries = [
  ["208508a2-e57e-46dc-a4cd-52da9157b3e7", 262356, "thread.archived", 262989, "archived"],
  ["ce8fd3ef-e030-4e23-ad6f-84a6ad7076b0", 262351, "thread.archived", 262991, "archived"],
  [
    "304c7858-7086-4032-bdbb-0106357f91f8",
    262721,
    "thread.turn-diff-completed",
    262993,
    "completed",
  ],
  [
    "8c426cc1-ffe8-43d9-a486-4666051df1c4",
    262792,
    "thread.turn-diff-completed",
    262995,
    "completed",
  ],
  [
    "d54bb5b7-afea-4426-af23-bf3cadfd4530",
    262851,
    "thread.turn-diff-completed",
    262997,
    "completed",
  ],
  ["5acd60d2-c801-47b8-b59e-33df1a1a90e9", 262357, "thread.archived", 262987, "archived"],
] as const;

layer("053_LocalSubagentTerminalDeliveries", (it) => {
  it.effect("backfills only the six evidence-backed ADA-194 noisy children", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 52 });
      const expectedParentThreadId = "8ebe24fb-e1d7-4f12-9139-9afed7ff4e05";
      for (const childThreadId of [
        ...noisyChildDeliveries.map(([threadId]) => threadId),
        "unverified-terminal-child",
      ]) {
        yield* sql`
          INSERT INTO projection_threads (
            thread_id,
            project_id,
            title,
            model_selection_json,
            runtime_mode,
            interaction_mode,
            created_at,
            updated_at,
            parent_thread_id,
            parent_environment_id
          )
          VALUES (
            ${childThreadId},
            ${"ada-194-project"},
            ${"terminal child"},
            ${'{"instanceId":"codex","model":"gpt-5-codex"}'},
            ${"full-access"},
            ${"default"},
            ${"2026-07-21T19:00:00.000Z"},
            ${"2026-07-21T19:00:00.000Z"},
            ${expectedParentThreadId},
            ${null}
          )
        `;
      }
      for (const [
        index,
        [childThreadId, terminalSequence, terminalEventType, deliveredSequence],
      ] of noisyChildDeliveries.entries()) {
        const deliveredPayload = `{"role":"system","text":"[sub-agent ${childThreadId} completed]"}`;
        yield* sql`
          INSERT INTO orchestration_events (
            sequence,
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
            ${terminalSequence},
            ${`ada-194-terminal-${childThreadId}`},
            ${"thread"},
            ${childThreadId},
            ${0},
            ${terminalEventType},
            ${"2026-07-21T19:00:00.000Z"},
            ${null},
            ${null},
            ${null},
            ${"server"},
            ${"{}"},
            ${"{}"}
          )
        `;
        yield* sql`
          INSERT INTO orchestration_events (
            sequence,
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
            ${deliveredSequence},
            ${`ada-194-delivered-${childThreadId}`},
            ${"thread"},
            ${expectedParentThreadId},
            ${index},
            ${"thread.message-sent"},
            ${"2026-07-21T19:16:04.000Z"},
            ${null},
            ${null},
            ${null},
            ${"server"},
            ${deliveredPayload},
            ${"{}"}
          )
        `;
      }
      yield* runMigrations({ toMigrationInclusive: 53 });

      const rows = yield* sql<{
        readonly childThreadId: string;
        readonly parentThreadId: string;
        readonly claimId: string;
        readonly claimedSequence: number;
        readonly terminalKind: string;
      }>`
        SELECT
          child_thread_id AS "childThreadId",
          parent_thread_id AS "parentThreadId",
          terminal_delivery_claim_id AS "claimId",
          terminal_delivery_claimed_sequence AS "claimedSequence",
          terminal_kind AS "terminalKind"
        FROM subagent_terminal_deliveries
        ORDER BY child_thread_id
      `;

      assert.deepEqual(
        rows.map(({ childThreadId }) => childThreadId),
        noisyChildDeliveries.map(([childThreadId]) => childThreadId).sort(),
      );
      assert.deepEqual(
        new Map(rows.map(({ childThreadId, terminalKind }) => [childThreadId, terminalKind])),
        new Map(
          noisyChildDeliveries.map(([childThreadId, , , , terminalKind]) => [
            childThreadId,
            terminalKind,
          ]),
        ),
      );
      assert.isTrue(rows.every(({ parentThreadId }) => parentThreadId === expectedParentThreadId));
      assert.isTrue(
        rows.every(
          ({ childThreadId, claimId }) => claimId === `migration:ada-194:${childThreadId}`,
        ),
      );
      assert.deepEqual(
        new Map(rows.map(({ childThreadId, claimedSequence }) => [childThreadId, claimedSequence])),
        new Map(
          noisyChildDeliveries.map(([childThreadId, terminalSequence]) => [
            childThreadId,
            terminalSequence,
          ]),
        ),
      );

      const pendingColumns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(pending_dispatches)
      `;
      assert.isTrue(pendingColumns.some(({ name }) => name === "source_terminal_sequence"));
    }),
  );
});

const olderSnapshotLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

olderSnapshotLayer("053_LocalSubagentTerminalDeliveries older snapshot", (it) => {
  it.effect("does not backfill a target child without the exact delivered history", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 52 });
      const [childThreadId] = noisyChildDeliveries[0];
      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          created_at,
          updated_at,
          parent_thread_id,
          parent_environment_id
        )
        VALUES (
          ${childThreadId},
          ${"ada-194-older-snapshot"},
          ${"terminal child"},
          ${'{"instanceId":"codex","model":"gpt-5-codex"}'},
          ${"full-access"},
          ${"default"},
          ${"2026-07-20T19:00:00.000Z"},
          ${"2026-07-20T19:00:00.000Z"},
          ${"8ebe24fb-e1d7-4f12-9139-9afed7ff4e05"},
          ${null}
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 53 });

      const rows = yield* sql`SELECT child_thread_id FROM subagent_terminal_deliveries`;
      assert.equal(rows.length, 0);
    }),
  );
});
