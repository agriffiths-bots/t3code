import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_threads)
  `;

  if (!columns.some((column) => column.name === "worktree_removable")) {
    yield* sql`
      ALTER TABLE projection_threads
      ADD COLUMN worktree_removable INTEGER NOT NULL DEFAULT 0
    `;
  }
  if (!columns.some((column) => column.name === "worktree_removal_path")) {
    yield* sql`
      ALTER TABLE projection_threads
      ADD COLUMN worktree_removal_path TEXT
    `;
  }
});
