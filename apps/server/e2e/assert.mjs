// e2e/assert.mjs — read-only assertion helpers for the t3-update.sh GATE.
//
// The update pipeline boots a throwaway server on a RESTORED COPY of real prod
// state, drives a sub-agent spawn (e2e/drive.sh), then calls childrenOf() here to
// prove a child thread row materialised under the spawning parent. Pure read-only
// SQL over projection_threads.parent_thread_id (the load-bearing feature column).
//
// Exports:
//   openState(dbPath)            -> a node:sqlite DatabaseSync opened read-only
//   childrenOf(db, parentId)     -> array of { thread_id, parent_thread_id } children
//   rootThreads(db)              -> array of root (parent_thread_id IS NULL) thread ids
//   close(db)
//
// node:sqlite is the same SQLite the server build uses (node v22 --experimental).
import { DatabaseSync } from "node:sqlite";

export function openState(dbPath) {
  // readOnly so the gate can never mutate the restored copy it is validating.
  return new DatabaseSync(dbPath, { readOnly: true });
}

export function childrenOf(db, parentThreadId) {
  const stmt = db.prepare(
    `SELECT thread_id, parent_thread_id
       FROM projection_threads
      WHERE parent_thread_id = ?
        AND deleted_at IS NULL`,
  );
  return stmt.all(parentThreadId);
}

export function rootThreads(db) {
  const stmt = db.prepare(
    `SELECT thread_id
       FROM projection_threads
      WHERE parent_thread_id IS NULL
        AND deleted_at IS NULL
      ORDER BY rowid DESC`,
  );
  return stmt.all().map((r) => r.thread_id);
}

export function close(db) {
  try {
    db.close();
  } catch {
    // already closed; ignore
  }
}
