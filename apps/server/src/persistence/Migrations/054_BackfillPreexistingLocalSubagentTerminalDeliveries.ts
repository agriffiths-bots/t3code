import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Mark local child lifecycles that were already terminal when durable delivery
 * claims shipped, and whose parent log proves the wake was delivered. Migration
 * 053's durable creation time is the cutoff, so a child that terminates in the
 * later upgrade window remains unclaimed and is recovered by the normal replay
 * path. Per-child delivery evidence keeps skip-level and crash-interrupted
 * upgrades from turning undelivered results into tombstones. This is
 * deliberately one-time: doing the same reconciliation on every boot could
 * suppress a genuinely undelivered wake after a later crash between terminal
 * persistence and parent delivery.
 *
 * Exported so the migration and restart regression exercise the same query.
 */
export const backfillPreexistingLocalSubagentTerminalDeliveries = Effect.fn(
  "backfillPreexistingLocalSubagentTerminalDeliveries",
)(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    WITH durable_claims_cutoff AS (
      SELECT created_at AS cutoff_at
      FROM effect_sql_migrations
      WHERE migration_id = 53
    ),
    local_children AS (
      SELECT
        threads.thread_id,
        threads.parent_thread_id,
        threads.deleted_at,
        threads.archived_at,
        threads.latest_user_message_at,
        turns.state AS turn_state,
        COALESCE(turns.completed_at, turns.started_at, turns.requested_at) AS terminal_at,
        sessions.status AS session_status,
        sessions.active_turn_id,
        sessions.updated_at AS session_updated_at,
        durable_claims_cutoff.cutoff_at,
        CASE
          WHEN turns.state IN ('completed', 'error', 'interrupted')
            AND (
              threads.latest_user_message_at IS NULL
              OR COALESCE(turns.completed_at, turns.started_at, turns.requested_at) IS NULL
              OR threads.latest_user_message_at <=
                COALESCE(turns.completed_at, turns.started_at, turns.requested_at)
            )
          THEN 1
          ELSE 0
        END AS has_fresh_terminal_turn
      FROM projection_threads AS threads
      LEFT JOIN projection_turns AS turns
        ON turns.thread_id = threads.thread_id
        AND turns.turn_id = threads.latest_turn_id
      LEFT JOIN projection_thread_sessions AS sessions
        ON sessions.thread_id = threads.thread_id
      CROSS JOIN durable_claims_cutoff
      WHERE threads.parent_thread_id IS NOT NULL
        AND threads.parent_environment_id IS NULL
    ),
    terminal_children AS (
      SELECT
        thread_id,
        parent_thread_id,
        cutoff_at,
        CASE
          WHEN deleted_at IS NOT NULL THEN 'killed'
          WHEN archived_at IS NOT NULL
            AND (
              has_fresh_terminal_turn = 0
              OR (terminal_at IS NOT NULL AND terminal_at > archived_at)
            )
          THEN 'archived'
          WHEN has_fresh_terminal_turn = 1 AND turn_state = 'completed' THEN 'completed'
          WHEN has_fresh_terminal_turn = 1 THEN 'failed'
          WHEN archived_at IS NOT NULL THEN 'archived'
          ELSE 'failed'
        END AS terminal_kind,
        CASE
          WHEN deleted_at IS NOT NULL THEN deleted_at
          WHEN archived_at IS NOT NULL
            AND (
              has_fresh_terminal_turn = 0
              OR (terminal_at IS NOT NULL AND terminal_at > archived_at)
            )
          THEN archived_at
          WHEN has_fresh_terminal_turn = 1 THEN terminal_at
          WHEN archived_at IS NOT NULL AND session_status IN ('error', 'stopped')
          THEN CASE
            WHEN session_updated_at IS NULL OR archived_at >= session_updated_at THEN archived_at
            ELSE session_updated_at
          END
          WHEN archived_at IS NOT NULL THEN archived_at
          ELSE session_updated_at
        END AS terminal_evidence_at
      FROM local_children
      WHERE deleted_at IS NOT NULL
        OR (
          NOT (
            session_status IN ('running', 'waiting')
            AND active_turn_id IS NOT NULL
          )
          AND (
            archived_at IS NOT NULL
            OR has_fresh_terminal_turn = 1
            OR session_status IN ('error', 'stopped')
          )
        )
    ),
    delivered_parent_wakes AS MATERIALIZED (
      SELECT
        parent_thread_id,
        occurred_at,
        wake_text
      FROM (
        SELECT
          stream_id AS parent_thread_id,
          occurred_at,
          json_extract(payload_json, '$.text') AS wake_text
        FROM orchestration_events
        WHERE event_type = 'thread.message-sent'
          AND json_extract(payload_json, '$.role') = 'system'
      )
      WHERE wake_text LIKE '%[sub-agent % %'
    )
    INSERT INTO subagent_terminal_deliveries (
      child_thread_id,
      parent_thread_id,
      terminal_delivery_claim_id,
      terminal_delivery_claimed_at,
      terminal_delivery_claimed_sequence,
      terminal_kind
    )
    SELECT
      terminal_children.thread_id,
      terminal_children.parent_thread_id,
      'migration:054:' || terminal_children.thread_id,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      COALESCE((
        SELECT MAX(events.sequence)
        FROM orchestration_events AS events
        WHERE events.stream_id = terminal_children.thread_id
          AND julianday(events.occurred_at) <= julianday(terminal_children.cutoff_at)
      ), 0),
      terminal_children.terminal_kind
    FROM terminal_children
    WHERE julianday(terminal_children.terminal_evidence_at) <=
      julianday(terminal_children.cutoff_at)
      AND (
        EXISTS (
          SELECT 1
          FROM delivered_parent_wakes AS delivered_event
          WHERE delivered_event.parent_thread_id = terminal_children.parent_thread_id
            AND substr(
              delivered_event.wake_text,
              1,
              length('[sub-agent ' || terminal_children.thread_id || ' ')
            ) = '[sub-agent ' || terminal_children.thread_id || ' '
            AND (
              substr(
                delivered_event.wake_text,
                length('[sub-agent ' || terminal_children.thread_id || ' ') + 1,
                length('completed] ')
              ) = 'completed] '
              OR substr(
                delivered_event.wake_text,
                length('[sub-agent ' || terminal_children.thread_id || ' ') + 1,
                length('failed] ')
              ) = 'failed] '
              OR substr(
                delivered_event.wake_text,
                length('[sub-agent ' || terminal_children.thread_id || ' ') + 1,
                length('killed] ')
              ) = 'killed] '
            )
            AND julianday(delivered_event.occurred_at) >=
              julianday(terminal_children.terminal_evidence_at)
        )
        OR EXISTS (
          SELECT 1
          FROM subagent_wait_deliveries AS wait_delivery
          WHERE wait_delivery.child_thread_id = terminal_children.thread_id
            AND wait_delivery.parent_thread_id = terminal_children.parent_thread_id
            AND julianday(wait_delivery.delivered_at) >=
              julianday(terminal_children.terminal_evidence_at)
        )
      )
    ON CONFLICT (child_thread_id) DO NOTHING
  `;
});

export default backfillPreexistingLocalSubagentTerminalDeliveries();
