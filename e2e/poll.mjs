// poll.mjs <db> <waitMs> <threadId...> — poll given threads until all settle, print state+answer
import * as NodeSqlite from "node:sqlite";
const [db_path, waitMsStr, ...ids] = process.argv.slice(2);
const waitMs = Number(waitMsStr);
const db = new NodeSqlite.DatabaseSync(db_path, { readOnly: true });
const lastTurn = (id) =>
  db
    .prepare(
      `SELECT state, assistant_message_id, completed_at FROM projection_turns WHERE thread_id=? ORDER BY row_id DESC LIMIT 1`,
    )
    .get(id);
const lastAsst = (id) =>
  db
    .prepare(
      `SELECT text FROM projection_thread_messages WHERE thread_id=? AND role='assistant' ORDER BY created_at DESC LIMIT 1`,
    )
    .get(id);
const sess = (id) =>
  db
    .prepare(
      `SELECT status, provider_name, provider_instance_id, last_error FROM projection_thread_sessions WHERE thread_id=?`,
    )
    .get(id);
const settled = (s) =>
  s && (s.state === "completed" || s.state === "failed" || s.state === "interrupted");
const deadline = Date.now() + waitMs;
while (Date.now() < deadline) {
  if (ids.map(lastTurn).every(settled)) break;
  await new Promise((r) => setTimeout(r, 3000));
}
for (const id of ids) {
  const t = lastTurn(id);
  const a = lastAsst(id);
  const sv = sess(id);
  console.log(
    JSON.stringify({
      thread_id: id,
      turnState: t?.state ?? null,
      provider_name: sv?.provider_name ?? null,
      provider_instance_id: sv?.provider_instance_id ?? null,
      last_error: sv?.last_error ?? null,
      finalAssistantText: a?.text ? a.text.trim() : null,
    }),
  );
}
db.close();
