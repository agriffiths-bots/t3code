#!/usr/bin/env node
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeUtil from "node:util";

const execFile = NodeUtil.promisify(NodeChildProcess.execFile);

const dir = process.argv[2];
if (!dir) throw new Error("usage: inject-resume.integration.mjs TMPDIR");
const { T3_ORIGIN, T3_TOKEN, T3_DB } = process.env;
if (!T3_ORIGIN || !T3_TOKEN || !T3_DB)
  throw new Error("T3_ORIGIN, T3_TOKEN, and T3_DB are required");

async function dispatch(command) {
  const response = await fetch(`${T3_ORIGIN}/api/orchestration/dispatch`, {
    method: "POST",
    headers: { authorization: `Bearer ${T3_TOKEN}`, "content-type": "application/json" },
    body: JSON.stringify(command),
  });
  if (!response.ok)
    throw new Error(`dispatch failed: HTTP ${response.status} ${await response.text()}`);
}

const now = new Date().toISOString();
const projectId = NodeCrypto.randomUUID();
const threadId = NodeCrypto.randomUUID();
await dispatch({
  type: "project.create",
  commandId: NodeCrypto.randomUUID(),
  projectId,
  title: "inject-resume-integration",
  workspaceRoot: dir,
  defaultModelSelection: { instanceId: "claudeAgent", model: "claude-sonnet-4-6" },
  createdAt: now,
});
await dispatch({
  type: "thread.create",
  commandId: NodeCrypto.randomUUID(),
  threadId,
  projectId,
  title: "inject-resume-integration",
  modelSelection: { instanceId: "claudeAgent", model: "claude-sonnet-4-6" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  createdAt: now,
});

const manifestPath = NodePath.join(dir, "resume-manifest.json");
await NodeFSP.writeFile(
  manifestPath,
  JSON.stringify(
    {
      version: 1,
      captured_at: now,
      threads: [
        {
          thread_id: threadId,
          role: "active",
          status: "interrupted",
          active_turn_id: null,
          title: "inject-resume-integration",
          injected_at: null,
        },
      ],
    },
    null,
    2,
  ),
);

await execFile(
  "node",
  [
    "scripts/ops/daily-restart/inject-resume.ts",
    "--manifest",
    manifestPath,
    "--origin",
    T3_ORIGIN,
    "--token",
    T3_TOKEN,
  ],
  {
    env: { ...process.env, T3_TOKEN },
  },
);

const db = new NodeSqlite.DatabaseSync(T3_DB, { readOnly: true });
const row = db
  .prepare(
    "SELECT text FROM projection_thread_messages WHERE thread_id = ? AND role = 'user' ORDER BY created_at DESC LIMIT 1",
  )
  .get(threadId);
if (!row?.text?.startsWith("Continue after restart:")) {
  throw new Error(`resume message did not land for thread ${threadId}`);
}

const manifest = JSON.parse(await NodeFSP.readFile(manifestPath, "utf8"));
if (typeof manifest.threads[0]?.injected_at !== "string") {
  throw new Error("manifest injected_at was not persisted");
}
