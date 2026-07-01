// report-children.mjs <db> <label> <rootThreadId>
import * as NodeSqlite from "node:sqlite";
const [dbPath, label, root] = process.argv.slice(2);
const db = new NodeSqlite.DatabaseSync(dbPath, { readOnly: true });
const kids = db
  .prepare(
    `SELECT thread_id, parent_thread_id, model_selection_json FROM projection_threads WHERE parent_thread_id=? ORDER BY created_at ASC`,
  )
  .all(root);
console.log(`== ${label} root=${root} childrenOf=${kids.length} ==`);
for (const k of kids) {
  const t = db
    .prepare(`SELECT state FROM projection_turns WHERE thread_id=? ORDER BY row_id DESC LIMIT 1`)
    .get(k.thread_id);
  const a = db
    .prepare(
      `SELECT text FROM projection_thread_messages WHERE thread_id=? AND role='assistant' ORDER BY created_at DESC LIMIT 1`,
    )
    .get(k.thread_id);
  const s = db
    .prepare(
      `SELECT provider_name, provider_instance_id, last_error FROM projection_thread_sessions WHERE thread_id=?`,
    )
    .get(k.thread_id);
  const text = (a?.text || "").trim();
  console.log(
    JSON.stringify({
      child: k.thread_id,
      parent_ok: k.parent_thread_id === root,
      model_selection_json: k.model_selection_json,
      provider_name: s?.provider_name ?? null,
      last_error: s?.last_error ?? null,
      latestTurnState: t?.state ?? null,
      finalAssistantText_nonEmpty: text.length > 0,
      finalAssistantText: text.slice(0, 60),
    }),
  );
}
db.close();
