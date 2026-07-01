// assert.mjs — read-only SQLite helpers for the t3 sub-agent + scheduler e2e.
//
// All e2e assertions derive from observable persisted state. This module opens
// <T3CODE_HOME>/userdata/state.sqlite in read-only mode (mode=ro) so it never
// contends with the running server's writer, and exposes typed accessors over
// the projection tables (migration 005) plus scheduled_tasks (migration 035)
// and the parent_thread_id linkage (migration 034).
//
// Usage:
//   import { openState, turnCountForThread, childrenOf } from "./assert.mjs";
//   const db = openState("/tmp/t3-e2e");           // pass T3CODE_HOME
//   const n = turnCountForThread(db, rootThreadId);
//   db.close();
//
// Every helper takes the db handle as its first arg so a single read-only
// connection can be reused across a polling loop.

import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

/**
 * Open the t3 state DB read-only. Accepts either a T3CODE_HOME directory
 * (preferred — we append userdata/state.sqlite) or a full path to a .sqlite
 * file.
 */
export function openState(homeOrDbPath) {
  const dbPath = homeOrDbPath.endsWith(".sqlite")
    ? homeOrDbPath
    : NodePath.join(homeOrDbPath, "userdata", "state.sqlite");
  return new NodeSqlite.DatabaseSync(dbPath, { readOnly: true });
}

// ---- turns ---------------------------------------------------------------

/**
 * Count turns recorded for a thread. Used by scenario (a) to prove the
 * schedule fires ~1/interval and by the "no double turn after kill -9" check.
 */
export function turnCountForThread(db, threadId) {
  const row = db
    .prepare(`SELECT COUNT(*) AS n FROM projection_turns WHERE thread_id = ?`)
    .get(threadId);
  return row.n;
}

/**
 * Ordered turn timestamps for a thread (oldest first). Returns requested_at /
 * started_at / completed_at per turn so scenario (a) can assert intervals
 * (~60s apart, no duplicate within one interval) and scenario (c) can
 * cross-check completed_at - requested_at against the script duration.
 */
export function turnTimestamps(db, threadId) {
  return db
    .prepare(
      `SELECT turn_id, state, requested_at, started_at, completed_at
         FROM projection_turns
        WHERE thread_id = ?
        ORDER BY row_id ASC`,
    )
    .all(threadId);
}

// ---- thread linkage (parent/child) --------------------------------------

/**
 * Child threads of a parent (migration 034 parent_thread_id). Scenario (b)
 * asserts one row per spawned sub-agent with the correct linkage; the `model`
 * column lets it verify per-provider routing. Excludes soft-deleted rows.
 */
export function childrenOf(db, parentThreadId) {
  return db
    .prepare(
      `SELECT thread_id, project_id, title, model_selection_json, parent_thread_id,
              latest_turn_id, created_at, updated_at, deleted_at
         FROM projection_threads
        WHERE parent_thread_id = ?
        ORDER BY created_at ASC`,
    )
    .all(parentThreadId);
}

// ---- scheduled tasks (migration 035) ------------------------------------

/** One scheduled task by id (scenario a: next_run_at / last_run_at / status). */
export function scheduledTask(db, taskId) {
  return db.prepare(`SELECT * FROM scheduled_tasks WHERE task_id = ?`).get(taskId);
}

/** All scheduled tasks (discovery / counting). */
export function listScheduledTasks(db) {
  return db.prepare(`SELECT * FROM scheduled_tasks ORDER BY created_at ASC`).all();
}

// ---- thread shell (latest turn + session) -------------------------------

/**
 * Combined "shell" view for a thread: its latest turn state joined with the
 * session status + last_error. Scenarios (b)/(c)/(d) use this to read
 * latestTurn.state ("completed"/"failed"/...) and session liveness, e.g. to
 * assert a child settles or that a killed child surfaces an error.
 */
export function threadShell(db, threadId) {
  const turn = db
    .prepare(
      `SELECT turn_id, state, requested_at, started_at, completed_at,
              assistant_message_id
         FROM projection_turns
        WHERE thread_id = ?
        ORDER BY row_id DESC LIMIT 1`,
    )
    .get(threadId);
  const session = db
    .prepare(
      `SELECT status, provider_name, provider_session_id, active_turn_id, last_error
         FROM projection_thread_sessions WHERE thread_id = ?`,
    )
    .get(threadId);
  const thread = db
    .prepare(
      `SELECT thread_id, title, model_selection_json, parent_thread_id, deleted_at
         FROM projection_threads WHERE thread_id = ?`,
    )
    .get(threadId);
  return { thread: thread ?? null, latestTurn: turn ?? null, session: session ?? null };
}

// ---- assistant messages --------------------------------------------------

/**
 * Assistant messages for a thread, oldest first. Scenario (b) reads these for
 * non-null finalAssistantText and to find the coordinator wake injection
 * (user-role "[sub-agent <id> completed]"); set role="user" for that check.
 */
export function assistantMessages(db, threadId, role = "assistant") {
  return db
    .prepare(
      `SELECT message_id, turn_id, role, text, is_streaming, created_at, updated_at
         FROM projection_thread_messages
        WHERE thread_id = ? AND role = ?
        ORDER BY created_at ASC`,
    )
    .all(threadId, role);
}
