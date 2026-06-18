// poll-wake.mjs <db> <rootThreadId> <baselineTurnCount> <waitMs>
// Poll the root thread for a NEW turn + a user-role wake injection message.
import { DatabaseSync } from "node:sqlite";
const [dbPath, root, baselineStr, waitMsStr] = process.argv.slice(2);
const baseline = Number(baselineStr);
const waitMs = Number(waitMsStr);
const db = new DatabaseSync(dbPath, { readOnly: true });
const turnCount = () => db.prepare(`SELECT COUNT(*) n FROM projection_turns WHERE thread_id=?`).get(root).n;
const userMsgs = () =>
  db.prepare(`SELECT message_id, turn_id, role, text, created_at FROM projection_thread_messages WHERE thread_id=? AND role='user' ORDER BY created_at ASC`).all(root);
const deadline = Date.now() + waitMs;
let result = { incremented: false, finalCount: baseline };
while (Date.now() < deadline) {
  const c = turnCount();
  const wake = userMsgs().filter((m) => /\[sub-agent/i.test(m.text || ""));
  if (c > baseline || wake.length > 0) {
    result = { incremented: c > baseline, baseline, finalCount: c };
    break;
  }
  await new Promise((r) => setTimeout(r, 3000));
}
const finalCount = turnCount();
const allUser = userMsgs();
const wakeMsgs = allUser.filter((m) => /\[sub-agent/i.test(m.text || ""));
console.log(JSON.stringify({
  root,
  baseline,
  finalCount,
  incremented: finalCount > baseline,
  userMsgCount: allUser.length,
  wakeMsgCount: wakeMsgs.length,
}, null, 2));
for (const m of wakeMsgs) {
  console.log("--- WAKE USER MESSAGE (turn " + m.turn_id + ", " + m.created_at + ") ---");
  console.log(m.text);
}
db.close();
