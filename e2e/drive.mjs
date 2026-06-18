#!/usr/bin/env node
// drive.mjs — programmatically drive a RUNNING t3 instance over the
// Environment HTTP API: create a project, create a thread bound to a chosen
// provider instance (default: claudeAgent), send a USER TURN, then poll the
// SQLite projection tables until the turn settles and an assistant message
// appears.
//
// AUTH: a scoped admin bearer is minted out-of-band with:
//   T3CODE_HOME=<home> node apps/server/src/bin.ts auth session issue --token-only
// and passed in via the T3_TOKEN env var (the wrapper drive.sh does this for
// you). The token carries orchestration:operate, which the dispatch endpoint
// requires.
//
// Endpoints used (see packages/contracts/src/environmentHttp.ts):
//   GET  /api/orchestration/snapshot   -> read model (projects/threads)
//   POST /api/orchestration/dispatch   -> ClientOrchestrationCommand
//
// Usage:
//   T3_TOKEN=... node drive.mjs \
//     --origin http://127.0.0.1:13910 \
//     --db /tmp/t3-e2e-drive/userdata/state.sqlite \
//     --workspace /tmp/t3-e2e-proj \
//     --instance claudeAgent --model claude-sonnet-4-6 \
//     --prompt "reply with the word READY"
//
// Exit 0 on success (turn completed + assistant output captured), 1 otherwise.

import { DatabaseSync } from "node:sqlite";
import { randomUUID } from "node:crypto";

function arg(name, def) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && i + 1 < process.argv.length ? process.argv[i + 1] : def;
}

const ORIGIN = arg("origin", "http://127.0.0.1:13910");
const DB_PATH = arg("db", "/tmp/t3-e2e-drive/userdata/state.sqlite");
const WORKSPACE = arg("workspace", "/tmp/t3-e2e-proj");
const INSTANCE = arg("instance", "claudeAgent");
const MODEL = arg("model", "claude-sonnet-4-6");
const PROMPT = arg("prompt", "reply with the word READY");
const TITLE = arg("title", "e2e-drive");
const TIMEOUT_MS = Number(arg("timeout-ms", "180000"));
const TOKEN = process.env.T3_TOKEN;

if (!TOKEN) {
  console.error("FATAL: T3_TOKEN env var is required (mint via `t3 auth session issue --token-only`).");
  process.exit(2);
}

const nowIso = () => new Date().toISOString();

async function dispatch(command) {
  const res = await fetch(`${ORIGIN}/api/orchestration/dispatch`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${TOKEN}` },
    body: JSON.stringify(command),
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`dispatch ${command.type} failed: HTTP ${res.status} ${text}`);
  }
  return text ? JSON.parse(text) : {};
}

async function snapshot() {
  const res = await fetch(`${ORIGIN}/api/orchestration/snapshot`, {
    headers: { authorization: `Bearer ${TOKEN}` },
  });
  if (!res.ok) throw new Error(`snapshot failed: HTTP ${res.status} ${await res.text()}`);
  return res.json();
}

function openDb() {
  // Read-only is enough to assert; readwrite=false keeps us off the writer's lane.
  return new DatabaseSync(DB_PATH, { readOnly: true });
}

async function main() {
  console.log(`[drive] origin=${ORIGIN} instance=${INSTANCE} model=${MODEL}`);

  // 1) Reuse an existing active project for this workspace, else create one.
  const snap = await snapshot();
  let project = (snap.projects ?? []).find(
    (p) => p.deletedAt === null && p.workspaceRoot === WORKSPACE,
  );
  if (!project) {
    const projectId = randomUUID();
    await dispatch({
      type: "project.create",
      commandId: randomUUID(),
      projectId,
      title: TITLE,
      workspaceRoot: WORKSPACE,
      defaultModelSelection: { instanceId: INSTANCE, model: MODEL },
      createdAt: nowIso(),
    });
    project = { id: projectId };
    console.log(`[drive] created project ${projectId}`);
  } else {
    console.log(`[drive] reusing project ${project.id}`);
  }

  // 2) Create a thread bound to the chosen provider instance + model.
  const threadId = randomUUID();
  await dispatch({
    type: "thread.create",
    commandId: randomUUID(),
    threadId,
    projectId: project.id,
    title: TITLE,
    modelSelection: { instanceId: INSTANCE, model: MODEL },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdAt: nowIso(),
  });
  console.log(`[drive] created thread ${threadId}`);

  // 3) Send a USER TURN (a prompt) to that thread.
  const messageId = randomUUID();
  const startSeq = await dispatch({
    type: "thread.turn.start",
    commandId: randomUUID(),
    threadId,
    message: { messageId, role: "user", text: PROMPT, attachments: [] },
    runtimeMode: "full-access",
    interactionMode: "default",
    createdAt: nowIso(),
  });
  console.log(`[drive] dispatched turn.start (seq=${JSON.stringify(startSeq)}) prompt=${JSON.stringify(PROMPT)}`);

  // 4) Poll SQLite projection tables until the turn settles.
  const db = openDb();
  const turnStmt = db.prepare(
    `SELECT turn_id, state, requested_at, started_at, completed_at, assistant_message_id
       FROM projection_turns WHERE thread_id = ? ORDER BY row_id DESC LIMIT 1`,
  );
  const asstStmt = db.prepare(
    `SELECT message_id, text, is_streaming, created_at, updated_at
       FROM projection_thread_messages
      WHERE thread_id = ? AND role = 'assistant' ORDER BY created_at DESC LIMIT 1`,
  );
  const sessStmt = db.prepare(
    `SELECT status, provider_name, last_error FROM projection_thread_sessions WHERE thread_id = ?`,
  );

  const deadline = Date.now() + TIMEOUT_MS;
  let lastState = null;
  while (Date.now() < deadline) {
    const turn = turnStmt.get(threadId);
    const sess = sessStmt.get(threadId);
    if (turn && turn.state !== lastState) {
      console.log(`[drive] turn state=${turn.state} session=${sess?.status ?? "?"}${sess?.last_error ? ` err=${sess.last_error}` : ""}`);
      lastState = turn.state;
    }
    const settled = turn && (turn.state === "completed" || turn.state === "failed" || turn.state === "interrupted");
    if (settled) {
      const asst = asstStmt.get(threadId);
      const ok = turn.state === "completed" && asst && asst.text && asst.text.trim().length > 0;
      console.log("\n========== RESULT ==========");
      console.log(`thread_id           : ${threadId}`);
      console.log(`turn.state          : ${turn.state}`);
      console.log(`turn.requested_at   : ${turn.requested_at}`);
      console.log(`turn.completed_at   : ${turn.completed_at}`);
      console.log(`session.status      : ${sess?.status}`);
      console.log(`session.last_error  : ${sess?.last_error ?? "(none)"}`);
      console.log(`assistant.message_id: ${asst?.message_id ?? "(none)"}`);
      console.log(`assistant.text      : ${asst?.text ? JSON.stringify(asst.text.slice(0, 400)) : "(none)"}`);
      console.log("============================");
      db.close();
      if (ok) {
        console.log("[drive] SUCCESS: turn completed with assistant output.");
        process.exit(0);
      }
      console.error(`[drive] FAILURE: turn settled as ${turn.state} without usable assistant output.`);
      process.exit(1);
    }
    await new Promise((r) => setTimeout(r, 2000));
  }
  db.close();
  console.error(`[drive] TIMEOUT after ${TIMEOUT_MS}ms; last turn state=${lastState ?? "(no turn row)"}.`);
  process.exit(1);
}

main().catch((e) => {
  console.error("[drive] ERROR:", e?.stack || e?.message || String(e));
  process.exit(1);
});
