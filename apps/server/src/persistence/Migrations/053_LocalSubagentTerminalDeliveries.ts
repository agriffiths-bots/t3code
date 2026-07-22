import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const ADA_194_NOISY_CHILD_DELIVERIES = [
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

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // New rows bind a parent wake to the child terminal event that produced it.
  // Existing rows remain NULL and deliberately do not create a tombstone: an
  // unbound historical row is safer to replay than to suppress a newer result.
  yield* sql`
    ALTER TABLE pending_dispatches
    ADD COLUMN source_terminal_sequence INTEGER
  `;

  yield* sql`
    CREATE TABLE IF NOT EXISTS subagent_terminal_deliveries (
      child_thread_id TEXT PRIMARY KEY,
      parent_thread_id TEXT NOT NULL,
      terminal_delivery_claim_id TEXT NOT NULL,
      terminal_delivery_claimed_at TEXT NOT NULL,
      terminal_delivery_claimed_sequence INTEGER NOT NULL,
      terminal_kind TEXT NOT NULL
    )
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_subagent_terminal_deliveries_parent
    ON subagent_terminal_deliveries(parent_thread_id)
  `;

  // These six local children are known to have already woken their parent many
  // times. Backfill only that evidence-backed set: claiming every historical
  // terminal child could suppress a wake that never actually reached its parent.
  // Each cutoff is the exact terminal event observed re-delivering on restart,
  // not migration-time MAX(sequence). A later lifecycle therefore has a larger
  // start/unarchive sequence and supersedes this backfill during reconciliation.
  for (const [
    childThreadId,
    terminalSequence,
    terminalEventType,
    deliveredSequence,
    terminalKind,
  ] of ADA_194_NOISY_CHILD_DELIVERIES) {
    yield* sql`
      INSERT INTO subagent_terminal_deliveries (
        child_thread_id,
        parent_thread_id,
        terminal_delivery_claim_id,
        terminal_delivery_claimed_at,
        terminal_delivery_claimed_sequence,
        terminal_kind
      )
      SELECT
        thread_id,
        parent_thread_id,
        'migration:ada-194:' || thread_id,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
        ${terminalSequence},
        ${terminalKind}
      FROM projection_threads
      WHERE thread_id = ${childThreadId}
        AND parent_thread_id IS NOT NULL
        AND parent_environment_id IS NULL
        AND EXISTS (
          SELECT 1
          FROM orchestration_events AS terminal_event
          WHERE terminal_event.sequence = ${terminalSequence}
            AND terminal_event.stream_id = projection_threads.thread_id
            AND terminal_event.event_type = ${terminalEventType}
        )
        AND EXISTS (
          SELECT 1
          FROM orchestration_events AS delivered_event
          WHERE delivered_event.sequence = ${deliveredSequence}
            AND delivered_event.stream_id = projection_threads.parent_thread_id
            AND delivered_event.event_type = 'thread.message-sent'
            AND json_extract(delivered_event.payload_json, '$.role') = 'system'
            AND json_extract(delivered_event.payload_json, '$.text') LIKE
              '[sub-agent ' || projection_threads.thread_id || ' %'
        )
      ON CONFLICT (child_thread_id) DO NOTHING
    `;
  }
});
