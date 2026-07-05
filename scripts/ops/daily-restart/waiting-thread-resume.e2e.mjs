#!/usr/bin/env node
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

const args = new Map();
for (let i = 2; i < process.argv.length; i += 1) {
  const key = process.argv[i];
  if (!key?.startsWith("--")) continue;
  const value = process.argv[i + 1]?.startsWith("--") ? "1" : (process.argv[++i] ?? "1");
  args.set(key.slice(2), value);
}

const transcriptPath = args.get("transcript") ?? process.env.T3DR_E2E_TRANSCRIPT;
if (!transcriptPath) {
  throw new Error("usage: waiting-thread-resume.e2e.mjs --transcript PATH");
}

await NodeFSP.mkdir(NodePath.dirname(transcriptPath), { recursive: true });
const transcript = NodeFS.createWriteStream(transcriptPath, { flags: "a", mode: 0o600 });
const log = (line) => {
  const text = `[${new Date().toISOString()}] ${line}`;
  transcript.write(`${text}\n`);
  console.error(text);
};

if (process.env.T3DR_E2E_LIVE_PROVIDER !== "1") {
  log("BLOCKED_ON_LIVE_PROVIDER set T3DR_E2E_LIVE_PROVIDER=1 to run the real provider resume path");
  process.exitCode = 75;
  await new Promise((resolve) => transcript.end(resolve));
  process.exit(75);
}

const requiredEnv = ["T3_ORIGIN", "T3_TOKEN", "T3_DB", "T3_HOME", "T3_PID", "T3_ENTRY", "T3_PORT"];
for (const name of requiredEnv) {
  if (!process.env[name]) throw new Error(`${name} is required; start with t3-up.sh`);
}

let origin = process.env.T3_ORIGIN;
const token = process.env.T3_TOKEN;
const dbPath = process.env.T3_DB;
const home = process.env.T3_HOME;
const entry = process.env.T3_ENTRY;
const port = process.env.T3_PORT;
const instance = args.get("instance") ?? process.env.T3DR_E2E_INSTANCE ?? "codex";
const model = args.get("model") ?? process.env.T3DR_E2E_MODEL ?? "gpt-5.5";
const timeoutMs = Number(args.get("timeout-ms") ?? process.env.T3DR_E2E_TIMEOUT_MS ?? "300000");

const db = new NodeSqlite.DatabaseSync(dbPath, { readOnly: true });
const nowIso = () => new Date().toISOString();

async function dispatch(command) {
  const response = await fetch(`${origin}/api/orchestration/dispatch`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify(command),
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`dispatch ${command.type} failed: HTTP ${response.status} ${text}`);
  }
  return text ? JSON.parse(text) : {};
}

async function authedSnapshot() {
  const response = await fetch(`${origin}/api/orchestration/snapshot`, {
    headers: { authorization: `Bearer ${token}` },
  });
  if (!response.ok) throw new Error(`snapshot failed: HTTP ${response.status}`);
  return response.json();
}

async function waitFor(description, read, predicate, timeout = timeoutMs) {
  const deadline = Date.now() + timeout;
  let last;
  while (Date.now() < deadline) {
    last = read();
    if (predicate(last)) return last;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  throw new Error(`${description} timed out; last=${JSON.stringify(last)}`);
}

function latestTurn(threadId) {
  return db
    .prepare(
      `SELECT turn_id, state, requested_at, completed_at
         FROM projection_turns
        WHERE thread_id = ?
        ORDER BY row_id DESC
        LIMIT 1`,
    )
    .get(threadId);
}

function latestUserMessage(threadId) {
  return db
    .prepare(
      `SELECT message_id, text, created_at
         FROM projection_thread_messages
        WHERE thread_id = ? AND role = 'user'
        ORDER BY created_at DESC
        LIMIT 1`,
    )
    .get(threadId);
}

function childLinked(parentThreadId, childThreadId) {
  return (
    db
      .prepare(
        `SELECT COUNT(*) AS count
         FROM projection_threads
        WHERE thread_id = ? AND parent_thread_id = ? AND deleted_at IS NULL`,
      )
      .get(childThreadId, parentThreadId).count === 1
  );
}

function pendingDispatchCount() {
  return db.prepare(`SELECT COUNT(*) AS count FROM pending_dispatches`).get().count;
}

function shSingle(value) {
  return `'${String(value).replaceAll("'", "'\\''")}'`;
}

async function updateRegistryPid(pid) {
  const name = process.env.T3_NAME;
  if (!name) return;
  const root =
    process.env.T3_EPHEMERAL_REGISTRY ??
    NodePath.join(process.env.HOME, ".cache/t3-ephemeral/instances");
  const envPath = NodePath.join(root, name, "instance.env");
  if (!NodeFS.existsSync(envPath)) return;
  const body = [
    "# Consumers shell out to the T3 CLI against this instance: an inherited dev",
    "# URL would flip the CLI to a different state dir than the server's.",
    "unset VITE_DEV_SERVER_URL 2>/dev/null || true",
    `export T3_NAME=${shSingle(name)}`,
    `export T3_ORIGIN=${shSingle(origin)}`,
    `export T3_TOKEN=${shSingle(token)}`,
    `export T3_DB=${shSingle(dbPath)}`,
    `export T3_HOME=${shSingle(home)}`,
    `export T3CODE_HOME=${shSingle(home)}`,
    `export T3_PORT=${shSingle(port)}`,
    `export T3_PID=${shSingle(String(pid))}`,
    `export T3_ENTRY=${shSingle(entry)}`,
    "",
  ].join("\n");
  await NodeFSP.writeFile(envPath, body, { mode: 0o600 });
}

async function stopServer() {
  const pid = Number(process.env.T3_PID);
  if (!Number.isFinite(pid) || pid <= 0) throw new Error(`invalid T3_PID=${process.env.T3_PID}`);
  log(`stopping ephemeral server pid=${pid}`);
  try {
    process.kill(-pid, "SIGTERM");
  } catch {
    try {
      process.kill(pid, "SIGTERM");
    } catch {
      return;
    }
  }
  for (let i = 0; i < 50; i += 1) {
    try {
      process.kill(pid, 0);
      await new Promise((resolve) => setTimeout(resolve, 100));
    } catch {
      return;
    }
  }
  try {
    process.kill(-pid, "SIGKILL");
  } catch {
    try {
      process.kill(pid, "SIGKILL");
    } catch {
      // Already gone.
    }
  }
}

async function startServer(label) {
  const restartLog = NodePath.join(home, `server-${label}.log`);
  const out = NodeFS.openSync(restartLog, "a", 0o600);
  const env = {
    ...process.env,
    T3CODE_HOME: home,
    T3CODE_NO_BROWSER: "1",
    T3CODE_LOG_LEVEL: "Info",
  };
  delete env.VITE_DEV_SERVER_URL;
  const child = NodeChildProcess.spawn(
    process.execPath,
    [entry, "serve", "--port", port, "--host", "127.0.0.1"],
    {
      detached: true,
      stdio: ["ignore", out, out],
      env,
    },
  );
  child.unref();
  process.env.T3_PID = String(child.pid);
  origin = `http://127.0.0.1:${port}`;
  await updateRegistryPid(child.pid);
  log(`started ephemeral server pid=${child.pid} log=${restartLog}`);
  const deadline = Date.now() + 120000;
  while (Date.now() < deadline) {
    try {
      await authedSnapshot();
      return;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
  throw new Error(`restarted server did not become command-ready; log=${restartLog}`);
}

async function linkParentOffline(childThreadId, parentThreadId) {
  await stopServer();
  log(`linking child=${childThreadId} parent=${parentThreadId} offline via cos link-parent`);
  const env = { ...process.env, T3CODE_HOME: home };
  delete env.VITE_DEV_SERVER_URL;
  const result = NodeChildProcess.spawnSync(
    process.execPath,
    [entry, "cos", "link-parent", childThreadId, parentThreadId, "--base-dir", home],
    {
      cwd: process.cwd(),
      env,
      encoding: "utf8",
    },
  );
  transcript.write(result.stdout ?? "");
  transcript.write(result.stderr ?? "");
  if (result.status !== 0) {
    throw new Error(`cos link-parent failed rc=${result.status}`);
  }
  await startServer("after-link-parent");
  await waitFor(
    "child parent projection",
    () => childLinked(parentThreadId, childThreadId),
    Boolean,
    30000,
  );
}

async function main() {
  log(`BEGIN live provider waiting-thread-resume instance=${instance} model=${model}`);
  await authedSnapshot();
  const projectId = NodeCrypto.randomUUID();
  const parentThreadId = NodeCrypto.randomUUID();
  const childThreadId = NodeCrypto.randomUUID();
  const workspace = await NodeFSP.mkdtemp("/tmp/t3-waiting-resume-e2e-");

  await dispatch({
    type: "project.create",
    commandId: NodeCrypto.randomUUID(),
    projectId,
    title: "waiting-thread-resume-e2e",
    workspaceRoot: workspace,
    defaultModelSelection: { instanceId: instance, model },
    createdAt: nowIso(),
  });
  await dispatch({
    type: "thread.create",
    commandId: NodeCrypto.randomUUID(),
    threadId: parentThreadId,
    projectId,
    title: "waiting-thread-resume-parent",
    modelSelection: { instanceId: instance, model },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdAt: nowIso(),
  });
  await dispatch({
    type: "thread.turn.start",
    commandId: NodeCrypto.randomUUID(),
    threadId: parentThreadId,
    message: {
      messageId: NodeCrypto.randomUUID(),
      role: "user",
      text: "Reply with READY.",
      attachments: [],
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    createdAt: nowIso(),
  });
  await waitFor(
    "parent initial turn",
    () => latestTurn(parentThreadId),
    (turn) => turn?.state === "completed",
  );
  log(`parent initial turn completed parent=${parentThreadId}`);

  await dispatch({
    type: "thread.create",
    commandId: NodeCrypto.randomUUID(),
    threadId: childThreadId,
    projectId,
    title: "waiting-thread-resume-child",
    modelSelection: { instanceId: instance, model },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdAt: nowIso(),
  });
  await dispatch({
    type: "thread.turn.start",
    commandId: NodeCrypto.randomUUID(),
    threadId: childThreadId,
    message: {
      messageId: NodeCrypto.randomUUID(),
      role: "user",
      text: "Reply with CHILD_DONE.",
      attachments: [],
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    createdAt: nowIso(),
  });
  await waitFor(
    "child turn completion",
    () => latestTurn(childThreadId),
    (turn) => turn?.state === "completed",
  );
  log(`child completed child=${childThreadId}; pendingBeforeRestart=${pendingDispatchCount()}`);

  await linkParentOffline(childThreadId, parentThreadId);
  const wake = await waitFor(
    "parent wake injection after restart",
    () => latestUserMessage(parentThreadId),
    (message) =>
      typeof message?.text === "string" &&
      message.text.includes(`[sub-agent ${childThreadId} completed]`),
    60000,
  );
  await waitFor("pending dispatch cleanup", pendingDispatchCount, (count) => count === 0, 30000);
  log(`PASS parent=${parentThreadId} child=${childThreadId} wake_message=${wake.message_id}`);
}

try {
  await main();
} finally {
  db.close();
  transcript.end();
}
