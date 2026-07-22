import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import { backfillPreexistingLocalSubagentTerminalDeliveries } from "./054_BackfillPreexistingLocalSubagentTerminalDeliveries.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

const parentThreadId = "terminal-backfill-parent";
const timestamp = "2000-01-01T08:00:00.000Z";

layer("054_BackfillPreexistingLocalSubagentTerminalDeliveries", (it) => {
  it.effect("backfills every delivered pre-existing terminal local child and is idempotent", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 53 });

      const seedChild = Effect.fn("seedTerminalBackfillChild")(function* (input: {
        readonly threadId: string;
        readonly turnState?: "completed" | "error" | "interrupted" | "pending" | "running";
        readonly turnAt?: string;
        readonly archivedAt?: string;
        readonly deletedAt?: string;
        readonly latestUserMessageAt?: string;
        readonly sessionStatus?: "ready" | "running" | "waiting" | "error" | "stopped";
        readonly sessionUpdatedAt?: string;
        readonly activeTurn?: boolean;
        readonly parentEnvironmentId?: string;
        readonly sequence?: number;
      }) {
        const turnId = input.turnState === undefined ? null : `${input.threadId}-turn`;
        yield* sql`
          INSERT INTO projection_threads (
            thread_id,
            project_id,
            title,
            latest_turn_id,
            model_selection_json,
            runtime_mode,
            interaction_mode,
            created_at,
            updated_at,
            deleted_at,
            archived_at,
            latest_user_message_at,
            parent_thread_id,
            parent_environment_id
          )
          VALUES (
            ${input.threadId},
            ${"terminal-backfill-project"},
            ${"terminal child"},
            ${turnId},
            ${'{"instanceId":"codex","model":"gpt-5-codex"}'},
            ${"full-access"},
            ${"default"},
            ${timestamp},
            ${timestamp},
            ${input.deletedAt ?? null},
            ${input.archivedAt ?? null},
            ${input.latestUserMessageAt ?? null},
            ${parentThreadId},
            ${input.parentEnvironmentId ?? null}
          )
        `;
        if (turnId !== null) {
          const turnAt = input.turnAt ?? timestamp;
          yield* sql`
            INSERT INTO projection_turns (
              thread_id,
              turn_id,
              state,
              requested_at,
              started_at,
              completed_at,
              checkpoint_files_json
            )
            VALUES (
              ${input.threadId},
              ${turnId},
              ${input.turnState},
              ${turnAt},
              ${turnAt},
              ${input.turnState === "pending" || input.turnState === "running" ? null : turnAt},
              ${"[]"}
            )
          `;
        }
        if (input.sessionStatus !== undefined) {
          yield* sql`
            INSERT INTO projection_thread_sessions (
              thread_id,
              status,
              active_turn_id,
              updated_at
            )
            VALUES (
              ${input.threadId},
              ${input.sessionStatus},
              ${input.activeTurn === true ? turnId : null},
              ${input.sessionUpdatedAt ?? timestamp}
            )
          `;
        }
        if (input.sequence !== undefined) {
          yield* sql`
            INSERT INTO orchestration_events (
              sequence,
              event_id,
              aggregate_kind,
              stream_id,
              stream_version,
              event_type,
              occurred_at,
              actor_kind,
              payload_json,
              metadata_json
            )
            VALUES (
              ${input.sequence},
              ${`terminal-backfill-event-${input.threadId}`},
              ${"thread"},
              ${input.threadId},
              ${0},
              ${"thread.turn-diff-completed"},
              ${timestamp},
              ${"server"},
              ${"{}"},
              ${"{}"}
            )
          `;
        }
      });

      yield* Effect.forEach(
        [
          {
            threadId: "completed-local",
            turnState: "completed" as const,
            sessionStatus: "ready" as const,
            sequence: 101,
          },
          {
            threadId: "failed-local",
            turnState: "error" as const,
            sessionStatus: "error" as const,
            sequence: 102,
          },
          { threadId: "stopped-local", sessionStatus: "stopped" as const },
          { threadId: "archived-local", archivedAt: "2000-01-01T09:00:00.000Z" },
          {
            threadId: "archived-completed-local",
            turnState: "completed" as const,
            turnAt: "2000-01-01T08:00:00.000Z",
            archivedAt: "2000-01-01T09:00:00.000Z",
          },
          {
            threadId: "archived-before-terminal-local",
            turnState: "completed" as const,
            turnAt: "2000-01-01T10:00:00.000Z",
            archivedAt: "2000-01-01T09:00:00.000Z",
          },
          {
            threadId: "deleted-active-local",
            turnState: "running" as const,
            deletedAt: "2000-01-01T09:00:00.000Z",
            sessionStatus: "running" as const,
            activeTurn: true,
          },
          {
            threadId: "already-claimed-local",
            turnState: "completed" as const,
            sessionStatus: "ready" as const,
          },
          {
            threadId: "live-local",
            turnState: "running" as const,
            sessionStatus: "running" as const,
            activeTurn: true,
          },
          {
            threadId: "stale-completed-local",
            turnState: "completed" as const,
            turnAt: "2000-01-01T08:00:00.000Z",
            latestUserMessageAt: "2000-01-01T09:00:00.000Z",
            sessionStatus: "ready" as const,
          },
          {
            threadId: "undelivered-pre-053-local",
            turnState: "completed" as const,
            sessionStatus: "ready" as const,
          },
          {
            threadId: "wait-delivered-local",
            turnState: "completed" as const,
            sessionStatus: "ready" as const,
          },
          {
            threadId: "consolidated-second-local",
            turnState: "completed" as const,
            sessionStatus: "ready" as const,
          },
          {
            threadId: "wrong-parent-wait-local",
            turnState: "completed" as const,
            sessionStatus: "ready" as const,
          },
          {
            threadId: "wild_card-local",
            turnState: "completed" as const,
            sessionStatus: "ready" as const,
          },
          {
            threadId: "wild%card-local",
            turnState: "completed" as const,
            sessionStatus: "ready" as const,
          },
          {
            threadId: "post-053-completed-local",
            turnState: "completed" as const,
            turnAt: "2999-01-01T08:00:00.000Z",
            sessionStatus: "ready" as const,
          },
          {
            threadId: "post-053-archived-local",
            archivedAt: "2999-01-01T08:00:00.000Z",
          },
          {
            threadId: "pre-053-archive-post-053-stop-local",
            archivedAt: "2000-01-01T09:00:00.000Z",
            sessionStatus: "stopped" as const,
            sessionUpdatedAt: "2999-01-01T08:00:00.000Z",
          },
          {
            threadId: "remote-terminal",
            turnState: "completed" as const,
            sessionStatus: "ready" as const,
            parentEnvironmentId: "remote-environment",
          },
        ],
        seedChild,
      );

      const messageDeliveredChildren = [
        "archived-before-terminal-local",
        "archived-completed-local",
        "archived-local",
        "completed-local",
        "deleted-active-local",
        "failed-local",
        "post-053-archived-local",
        "post-053-completed-local",
        "stopped-local",
      ] as const;
      for (const [index, childThreadId] of messageDeliveredChildren.entries()) {
        const deliveredAt = childThreadId.startsWith("post-053-")
          ? "3000-01-01T08:00:00.000Z"
          : childThreadId === "archived-before-terminal-local"
            ? "2000-01-01T09:30:00.000Z"
            : "2001-01-01T08:00:00.000Z";
        yield* sql`
          INSERT INTO orchestration_events (
            sequence,
            event_id,
            aggregate_kind,
            stream_id,
            stream_version,
            event_type,
            occurred_at,
            actor_kind,
            payload_json,
            metadata_json
          )
          VALUES (
            ${1_001 + index},
            ${`terminal-backfill-delivered-${childThreadId}`},
            ${"thread"},
            ${parentThreadId},
            ${1_001 + index},
            ${"thread.message-sent"},
            ${deliveredAt},
            ${"server"},
            ${`{"role":"system","text":"[sub-agent ${childThreadId} completed] delivered"}`},
            ${"{}"}
          )
        `;
      }
      yield* sql`
        INSERT INTO orchestration_events (
          sequence,
          event_id,
          aggregate_kind,
          stream_id,
          stream_version,
          event_type,
          occurred_at,
          actor_kind,
          payload_json,
          metadata_json
        )
        VALUES (
          ${1_100},
          ${"terminal-backfill-delivered-consolidated"},
          ${"thread"},
          ${parentThreadId},
          ${1_100},
          ${"thread.message-sent"},
          ${"2001-01-01T08:00:00.000Z"},
          ${"server"},
          ${'{"role":"system","text":"[sub-agent already-delivered-first completed] delivered\\n[sub-agent consolidated-second-local completed] delivered\\n[sub-agent wildXcard-local completed] delivered\\n[sub-agent wildXYZcard-local completed] delivered"}'},
          ${"{}"}
        )
      `;
      yield* sql`
        INSERT INTO subagent_wait_deliveries (
          child_thread_id,
          parent_thread_id,
          delivered_at,
          parent_turn_id_at_delivery
        )
        VALUES (
          ${"wait-delivered-local"},
          ${parentThreadId},
          ${"2001-01-01T08:00:00.000Z"},
          ${null}
        ), (
          ${"wrong-parent-wait-local"},
          ${"previous-parent"},
          ${"2001-01-01T08:00:00.000Z"},
          ${null}
        )
      `;

      yield* sql`
        INSERT INTO subagent_terminal_deliveries (
          child_thread_id,
          parent_thread_id,
          terminal_delivery_claim_id,
          terminal_delivery_claimed_at,
          terminal_delivery_claimed_sequence,
          terminal_kind
        )
        VALUES (
          ${"already-claimed-local"},
          ${parentThreadId},
          ${"existing-claim"},
          ${timestamp},
          ${99},
          ${"completed"}
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 54 });
      yield* backfillPreexistingLocalSubagentTerminalDeliveries();

      const rows = yield* sql<{
        readonly childThreadId: string;
        readonly claimId: string;
        readonly claimedSequence: number;
        readonly terminalKind: string;
      }>`
        SELECT
          child_thread_id AS "childThreadId",
          terminal_delivery_claim_id AS "claimId",
          terminal_delivery_claimed_sequence AS "claimedSequence",
          terminal_kind AS "terminalKind"
        FROM subagent_terminal_deliveries
        ORDER BY child_thread_id
      `;

      assert.deepEqual(
        rows.map(({ childThreadId }) => childThreadId),
        [
          "already-claimed-local",
          "archived-before-terminal-local",
          "archived-completed-local",
          "archived-local",
          "completed-local",
          "deleted-active-local",
          "failed-local",
          "stopped-local",
          "wait-delivered-local",
        ],
      );
      assert.deepEqual(
        new Map(rows.map(({ childThreadId, terminalKind }) => [childThreadId, terminalKind])),
        new Map([
          ["already-claimed-local", "completed"],
          ["archived-before-terminal-local", "archived"],
          ["archived-completed-local", "completed"],
          ["archived-local", "archived"],
          ["completed-local", "completed"],
          ["deleted-active-local", "killed"],
          ["failed-local", "failed"],
          ["stopped-local", "failed"],
          ["wait-delivered-local", "completed"],
        ]),
      );
      assert.equal(
        rows.find(({ childThreadId }) => childThreadId === "completed-local")?.claimedSequence,
        101,
      );
      assert.equal(
        rows.find(({ childThreadId }) => childThreadId === "failed-local")?.claimedSequence,
        102,
      );
      assert.equal(
        rows.find(({ childThreadId }) => childThreadId === "archived-local")?.claimedSequence,
        0,
      );
      assert.equal(
        rows.find(({ childThreadId }) => childThreadId === "already-claimed-local")?.claimId,
        "existing-claim",
      );
      assert.isTrue(
        rows
          .filter(({ childThreadId }) => childThreadId !== "already-claimed-local")
          .every(({ childThreadId, claimId }) => claimId === `migration:054:${childThreadId}`),
      );
    }),
  );
});
