#!/usr/bin/env node
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeURL from "node:url";

const scriptDir = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const repoRoot = NodePath.resolve(scriptDir, "..", "..");

const DEFAULT_MODEL_SELECTION = {
  instanceId: "codex",
  model: "gpt-5.5",
  options: [
    { id: "reasoningEffort", value: "xhigh" },
    { id: "serviceTier", value: "default" },
  ],
};

const DEFAULT_PROMPT = `You are the autonomous Linear issue pickup loop for agriffiths-bots/t3code.

Follow the repo skill .agents/skills/linear-issue-fix/SKILL.md exactly. Do not ask Adam unless blocked by auth or a product decision only Adam can make. For validation, do not use the live T3 MCP tools; use the ephemeral T3 server and e2e skills.

On this scheduled run:
1. Query Linear workspace adamfg, team Adam, for Todo or Backlog ADA-* issues. Prefer issues reported by report-t3-bug or issues whose title, description, or labels mention t3code, T3 harness, automation, scheduler, subagent, MCP, or Cloudflare.
2. If no eligible issue exists, reply exactly: No eligible Linear issue found.
3. If eligible issues exist, take exactly one highest-priority oldest issue and run the full issue loop to Done: reproduce, fix, verify, factory-gated commit, PR, Codex review, CI babysit, wizzo-approve merge, and Linear closeout.
4. If blocked, comment on the Linear issue with the concrete blocker and leave the issue In Progress.`;

function parseArgs(argv) {
  const opts = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg.startsWith("--")) throw new Error(`Unexpected argument: ${arg}`);
    const key = arg.slice(2);
    if (key === "help") {
      opts.help = true;
      continue;
    }
    const value = argv[i + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new Error(`Missing value for --${key}`);
    }
    opts[key] = value;
    i += 1;
  }
  return opts;
}

function usage() {
  console.log(`usage: install-linear-issue-pickup.mjs [options]

Options:
  --origin URL             T3 server origin (default: http://127.0.0.1:3773)
  --base-dir PATH          T3 base dir / T3CODE_HOME (default: ~/.t3-vps)
  --workspace PATH         Project workspace root (default: current repo)
  --project-title TITLE    Project title when creating one (default: T3 Code)
  --thread-title TITLE     Dedicated pickup thread title
  --task-id ID             Stable scheduled task id
  --interval-seconds N     Poll interval in seconds (default: 900)
  --first-delay-seconds N  Delay before first run (default: 60)
  --model MODEL            Codex model name (default: gpt-5.5)
`);
}

function positiveInt(name, value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`--${name} must be a positive integer`);
  }
  return parsed;
}

function mintToken(baseDir) {
  const entry = NodePath.resolve(repoRoot, "apps/server/src/bin.ts");
  return NodeChildProcess.execFileSync(
    process.execPath,
    [
      entry,
      "auth",
      "session",
      "issue",
      "--base-dir",
      baseDir,
      "--token-only",
      "--label",
      "linear-issue-pickup-installer",
    ],
    { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
  ).trim();
}

async function requestJson(origin, token, path, init = {}) {
  const response = await fetch(`${origin}${path}`, {
    ...init,
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${token}`,
      ...init.headers,
    },
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${init.method ?? "GET"} ${path} failed: HTTP ${response.status} ${text}`);
  }
  return text.length > 0 ? JSON.parse(text) : {};
}

async function dispatch(origin, token, command) {
  await requestJson(origin, token, "/api/orchestration/dispatch", {
    method: "POST",
    body: JSON.stringify(command),
  });
}

function nowIso() {
  return new Date().toISOString();
}

function upsertSchedule({
  baseDir,
  taskId,
  threadId,
  prompt,
  intervalSeconds,
  firstDelaySeconds,
  modelSelection,
}) {
  const dbPath = NodePath.resolve(baseDir, "userdata", "state.sqlite");
  if (!NodeFS.existsSync(dbPath)) throw new Error(`State DB not found: ${dbPath}`);

  const db = new NodeSqlite.DatabaseSync(dbPath);
  try {
    const existing = db
      .prepare("SELECT created_at FROM scheduled_tasks WHERE task_id = ?")
      .get(taskId);
    const createdAt = existing?.created_at ?? nowIso();
    const nextRunAt = new Date(Date.now() + firstDelaySeconds * 1_000).toISOString();

    db.exec("BEGIN IMMEDIATE");
    db.prepare(
      `INSERT INTO scheduled_tasks (
        task_id,
        thread_id,
        prompt,
        schedule_kind,
        interval_seconds,
        cron_expr,
        timezone_name,
        enabled,
        busy_policy,
        next_run_at,
        last_run_at,
        last_status,
        last_error,
        skipped_count,
        retry_count,
        queued_count,
        model_selection,
        created_at
      ) VALUES (?, ?, ?, 'interval', ?, NULL, 'UTC', 1, 'skip', ?, NULL, NULL, NULL, 0, 0, 0, ?, ?)
      ON CONFLICT(task_id) DO UPDATE SET
        thread_id = excluded.thread_id,
        prompt = excluded.prompt,
        schedule_kind = excluded.schedule_kind,
        interval_seconds = excluded.interval_seconds,
        cron_expr = excluded.cron_expr,
        timezone_name = excluded.timezone_name,
        enabled = excluded.enabled,
        busy_policy = excluded.busy_policy,
        next_run_at = excluded.next_run_at,
        model_selection = excluded.model_selection`,
    ).run(
      taskId,
      threadId,
      prompt,
      intervalSeconds,
      nextRunAt,
      JSON.stringify(modelSelection),
      createdAt,
    );
    db.exec("COMMIT");
    return { dbPath, nextRunAt };
  } catch (error) {
    try {
      db.exec("ROLLBACK");
    } catch {
      // No active transaction.
    }
    throw error;
  } finally {
    db.close();
  }
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) {
    usage();
    return;
  }

  const origin = opts.origin ?? "http://127.0.0.1:3773";
  const baseDir = NodePath.resolve(
    opts["base-dir"] ?? NodePath.resolve(NodeOS.homedir(), ".t3-vps"),
  );
  const workspace = NodePath.resolve(opts.workspace ?? repoRoot);
  const projectTitle = opts["project-title"] ?? "T3 Code";
  const threadTitle = opts["thread-title"] ?? "Linear issue pickup loop";
  const taskId = opts["task-id"] ?? "linear-issue-pickup";
  const intervalSeconds = positiveInt("interval-seconds", opts["interval-seconds"] ?? "900");
  const firstDelaySeconds = positiveInt("first-delay-seconds", opts["first-delay-seconds"] ?? "60");
  const modelSelection = {
    ...DEFAULT_MODEL_SELECTION,
    model: opts.model ?? DEFAULT_MODEL_SELECTION.model,
  };

  const token = process.env.T3_TOKEN?.trim() || mintToken(baseDir);
  const snapshot = await requestJson(origin, token, "/api/orchestration/snapshot");

  let project = (snapshot.projects ?? []).find(
    (candidate) => candidate.workspaceRoot === workspace && candidate.deletedAt === null,
  );
  if (project === undefined) {
    const projectId = NodeCrypto.randomUUID();
    await dispatch(origin, token, {
      type: "project.create",
      commandId: NodeCrypto.randomUUID(),
      projectId,
      title: projectTitle,
      workspaceRoot: workspace,
      defaultModelSelection: modelSelection,
      createdAt: nowIso(),
    });
    project = { id: projectId, title: projectTitle, workspaceRoot: workspace };
  }

  let thread = (snapshot.threads ?? []).find(
    (candidate) =>
      candidate.projectId === project.id &&
      candidate.title === threadTitle &&
      candidate.deletedAt === null,
  );
  if (thread === undefined) {
    const threadId = NodeCrypto.randomUUID();
    await dispatch(origin, token, {
      type: "thread.create",
      commandId: NodeCrypto.randomUUID(),
      threadId,
      projectId: project.id,
      title: threadTitle,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdAt: nowIso(),
    });
    thread = { id: threadId, title: threadTitle, projectId: project.id };
  }

  const schedule = upsertSchedule({
    baseDir,
    taskId,
    threadId: thread.id,
    prompt: DEFAULT_PROMPT,
    intervalSeconds,
    firstDelaySeconds,
    modelSelection,
  });

  console.log(
    JSON.stringify(
      {
        origin,
        baseDir,
        projectId: project.id,
        threadId: thread.id,
        taskId,
        intervalSeconds,
        nextRunAt: schedule.nextRunAt,
        dbPath: schedule.dbPath,
      },
      null,
      2,
    ),
  );
}

main().catch((error) => {
  console.error(error?.stack ?? error?.message ?? String(error));
  process.exit(1);
});
