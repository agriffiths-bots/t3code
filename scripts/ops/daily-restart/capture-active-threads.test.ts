// @effect-diagnostics-next-line nodeBuiltinImport:off - tests inspect manifest files written by the script.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off - tests create fixture file paths.
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import * as NodeSqlite from "node:sqlite";

import { assert, describe, it } from "@effect/vitest";

import {
  captureActiveThreads,
  openCaptureDatabase,
  parseArgs,
  runCli,
  type CaptureManifest,
} from "./capture-active-threads.ts";

function makeTempDir(): string {
  return NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "capture-active-threads-"));
}

function makeFixtureDb(dir: string): string {
  const dbPath = NodePath.join(dir, "state.sqlite");
  const db = new NodeSqlite.DatabaseSync(dbPath);
  db.exec(`
    CREATE TABLE projection_threads (
      thread_id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      title TEXT NOT NULL,
      runtime_mode TEXT NOT NULL DEFAULT 'full-access',
      interaction_mode TEXT NOT NULL DEFAULT 'default',
      deleted_at TEXT,
      archived_at TEXT
    );

    CREATE TABLE projection_thread_sessions (
      thread_id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      active_turn_id TEXT,
      runtime_mode TEXT NOT NULL DEFAULT 'full-access',
      updated_at TEXT NOT NULL
    );

    CREATE TABLE projection_thread_messages (
      message_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL,
      turn_id TEXT,
      role TEXT NOT NULL,
      text TEXT NOT NULL,
      attachments_json TEXT,
      is_streaming INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE projection_turns (
      thread_id TEXT NOT NULL,
      turn_id TEXT,
      pending_message_id TEXT,
      state TEXT NOT NULL,
      requested_at TEXT NOT NULL,
      started_at TEXT,
      completed_at TEXT,
      checkpoint_turn_count INTEGER,
      checkpoint_ref TEXT,
      checkpoint_status TEXT,
      checkpoint_files_json TEXT NOT NULL DEFAULT '[]',
      UNIQUE(thread_id, turn_id)
    );

    CREATE TABLE orchestration_events (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      aggregate_kind TEXT NOT NULL,
      stream_id TEXT NOT NULL,
      stream_version INTEGER NOT NULL,
      event_type TEXT NOT NULL,
      occurred_at TEXT NOT NULL,
      command_id TEXT,
      causation_event_id TEXT,
      correlation_id TEXT,
      actor_kind TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      metadata_json TEXT NOT NULL
    );
  `);

  const rows: ReadonlyArray<
    readonly [string, string, string | null, string | null, string, string | null]
  > = [
    ["active-turn", "Active Turn", null, null, "running", "turn-1"],
    [
      "active-with-pending-turn",
      "Active With Pending Turn",
      null,
      null,
      "running",
      "turn-active-with-pending",
    ],
    ["pending-no-session", "Pending No Session", null, null, "ready", null],
    ["running-no-turn", "Running No Turn", null, null, "running", null],
    ["starting-no-turn", "Starting No Turn", null, null, "starting", null],
    ["ready-pending-turn", "Ready Pending Turn", null, null, "ready", null],
    ["ready-stale-pending-turn", "Ready Stale Pending Turn", null, null, "ready", null],
    ["terminal-equal-pending-turn", "Terminal Equal Pending Turn", null, null, "error", null],
    ["terminal-fresh-pending-turn", "Terminal Fresh Pending Turn", null, null, "error", null],
    ["stopped-running-turn", "Stopped Running Turn", null, null, "stopped", null],
    ["stopped-new-running-turn", "Stopped New Running Turn", null, null, "stopped", null],
    ["stopped-interrupted-turn", "Stopped Interrupted Turn", null, null, "stopped", null],
    ["interrupted-current-turn", "Interrupted Current Turn", null, null, "interrupted", null],
    ["stopped-current-pending-turn", "Stopped Current Pending Turn", null, null, "stopped", null],
    [
      "interrupted-current-pending-turn",
      "Interrupted Current Pending Turn",
      null,
      null,
      "interrupted",
      null,
    ],
    ["stopped-stale-pending-turn", "Stopped Stale Pending Turn", null, null, "stopped", null],
    [
      "stopped-stale-interrupted-turn",
      "Stopped Stale Interrupted Turn",
      null,
      null,
      "stopped",
      null,
    ],
    ["stopped-completed-turn", "Stopped Completed Turn", null, null, "stopped", null],
    ["ready-running-projection", "Ready Running Projection", null, null, "ready", null],
    [
      "terminal-stale-active-pending-turn",
      "Terminal Stale Active Pending Turn",
      null,
      null,
      "error",
      "turn-stale",
    ],
    ["waiting", "Waiting Parent", null, null, "waiting", null],
    ["idle", "Idle Thread", null, null, "ready", null],
    ["errored-stale", "Errored Stale", null, null, "error", "turn-error"],
    ["deleted", "Deleted Thread", "2026-07-03T00:00:00.000Z", null, "running", "turn-2"],
    ["archived", "Archived Thread", null, "2026-07-03T00:00:00.000Z", "running", "turn-3"],
    ["excluded", "Excluded Thread", null, null, "running", "turn-4"],
  ];
  for (const row of rows) {
    insertThread(db, ...row);
  }
  db.prepare("DELETE FROM projection_thread_sessions WHERE thread_id = 'pending-no-session'").run();

  db.close();
  return dbPath;
}

function insertThread(
  db: NodeSqlite.DatabaseSync,
  threadId: string,
  title: string,
  deletedAt: string | null,
  archivedAt: string | null,
  status: string,
  activeTurnId: string | null,
): void {
  db.prepare(`
    INSERT INTO projection_threads (
      thread_id,
      project_id,
      title,
      runtime_mode,
      interaction_mode,
      deleted_at,
      archived_at
    ) VALUES (?, 'project-1', ?, ?, ?, ?, ?)
  `).run(
    threadId,
    title,
    threadId === "waiting" ? "approval-required" : "full-access",
    threadId === "waiting" ? "plan" : "default",
    deletedAt,
    archivedAt,
  );

  const sessionUpdatedAt =
    threadId === "ready-stale-pending-turn"
      ? "2026-07-03T00:00:02.000Z"
      : threadId === "active-with-pending-turn"
        ? "2026-07-03T00:00:03.000Z"
        : threadId === "starting-no-turn"
          ? "2026-07-03T00:00:02.000Z"
          : threadId === "stopped-current-pending-turn" ||
              threadId === "interrupted-current-pending-turn" ||
              threadId === "stopped-new-running-turn" ||
              threadId === "stopped-stale-pending-turn" ||
              threadId === "stopped-interrupted-turn" ||
              threadId === "interrupted-current-turn"
            ? "2026-07-03T00:00:03.000Z"
            : threadId === "terminal-equal-pending-turn"
              ? "2026-07-03T00:00:01.000Z"
              : "2026-07-03T00:00:00.000Z";

  db.prepare(`
    INSERT INTO projection_thread_sessions (
      thread_id,
      status,
      active_turn_id,
      runtime_mode,
      updated_at
    ) VALUES (?, ?, ?, ?, ?)
  `).run(
    threadId,
    status,
    activeTurnId,
    threadId === "active-turn" || threadId === "active-with-pending-turn"
      ? "approval-required"
      : "full-access",
    sessionUpdatedAt,
  );

  if (
    threadId === "active-turn" ||
    threadId === "errored-stale" ||
    threadId === "pending-no-session" ||
    threadId === "starting-no-turn" ||
    threadId === "ready-pending-turn" ||
    threadId === "ready-stale-pending-turn" ||
    threadId === "terminal-equal-pending-turn" ||
    threadId === "terminal-fresh-pending-turn" ||
    threadId === "terminal-stale-active-pending-turn" ||
    threadId === "stopped-current-pending-turn" ||
    threadId === "interrupted-current-pending-turn" ||
    threadId === "stopped-stale-pending-turn" ||
    threadId === "stopped-running-turn" ||
    threadId === "stopped-new-running-turn" ||
    threadId === "stopped-interrupted-turn" ||
    threadId === "interrupted-current-turn" ||
    threadId === "stopped-stale-interrupted-turn" ||
    threadId === "stopped-completed-turn" ||
    threadId === "ready-running-projection"
  ) {
    const pendingMessageId =
      threadId === "active-turn" ? "message-active-turn" : `message-${threadId}`;
    const pendingMessageAttachments =
      threadId === "ready-pending-turn"
        ? [
            {
              type: "image",
              id: "ready-pending-turn-image",
              name: "ready.png",
              mimeType: "image/png",
              sizeBytes: 120,
            },
          ]
        : [];
    db.prepare(`
      INSERT INTO projection_thread_messages (
        message_id,
        thread_id,
        turn_id,
        role,
        text,
        attachments_json,
        is_streaming,
        created_at,
        updated_at
      ) VALUES (?, ?, NULL, ?, ?, ?, 0, '2026-07-03T00:00:01.000Z', '2026-07-03T00:00:01.000Z')
    `).run(
      pendingMessageId,
      threadId,
      threadId === "pending-no-session" ? "system" : "user",
      `Pending prompt for ${threadId}`,
      JSON.stringify(pendingMessageAttachments),
    );
    const turnRequestedAt =
      threadId === "stopped-current-pending-turn" || threadId === "stopped-new-running-turn"
        ? "2026-07-03T00:00:02.500Z"
        : "2026-07-03T00:00:01.000Z";
    db.prepare(`
      INSERT INTO projection_turns (
        thread_id,
        turn_id,
        pending_message_id,
        state,
        requested_at,
        completed_at,
        checkpoint_files_json
      ) VALUES (?, ?, ?, ?, ?, ?, '[]')
    `).run(
      threadId,
      threadId === "active-turn"
        ? activeTurnId
        : threadId === "errored-stale"
          ? activeTurnId
          : threadId === "stopped-running-turn"
            ? "turn-stopped-running"
            : threadId === "stopped-new-running-turn"
              ? "turn-stopped-new-running"
              : threadId === "stopped-interrupted-turn"
                ? "turn-stopped-interrupted"
                : threadId === "interrupted-current-turn"
                  ? "turn-interrupted-current"
                  : threadId === "stopped-stale-interrupted-turn"
                    ? "turn-stopped-stale-interrupted"
                    : threadId === "stopped-completed-turn"
                      ? "turn-stopped-completed"
                      : threadId === "ready-running-projection"
                        ? "turn-ready-running"
                        : null,
      pendingMessageId,
      threadId === "active-turn" ||
        threadId === "errored-stale" ||
        threadId === "stopped-running-turn" ||
        threadId === "stopped-new-running-turn" ||
        threadId === "ready-running-projection"
        ? "running"
        : threadId === "stopped-interrupted-turn" ||
            threadId === "interrupted-current-turn" ||
            threadId === "stopped-stale-interrupted-turn"
          ? "interrupted"
          : threadId === "stopped-completed-turn"
            ? "completed"
            : "pending",
      turnRequestedAt,
      threadId === "stopped-interrupted-turn"
        ? "2026-07-03T00:00:03.000Z"
        : threadId === "interrupted-current-turn"
          ? "2026-07-03T00:00:03.000Z"
          : threadId === "stopped-stale-interrupted-turn"
            ? "2026-07-03T00:00:00.000Z"
            : null,
    );

    if (threadId === "terminal-stale-active-pending-turn") {
      db.prepare(`
        INSERT INTO projection_turns (
          thread_id,
          turn_id,
          pending_message_id,
          state,
          requested_at,
          checkpoint_files_json
        ) VALUES (?, ?, 'message-stale-active-turn', 'error', '2026-07-03T00:00:00.000Z', '[]')
	      `).run(threadId, activeTurnId);
    }

    const usesApprovalRuntime =
      threadId === "pending-no-session" ||
      threadId === "ready-pending-turn" ||
      threadId === "starting-no-turn";
    db.prepare(`
	      INSERT INTO orchestration_events (
        event_id,
        aggregate_kind,
        stream_id,
        stream_version,
        event_type,
        occurred_at,
        actor_kind,
        payload_json,
        metadata_json
      ) VALUES (?, 'thread', ?, 1, 'thread.turn-start-requested', '2026-07-03T00:00:01.000Z', 'system', ?, '{}')
    `).run(
      `event-${threadId}-start`,
      threadId,
      JSON.stringify({
        threadId,
        messageId: pendingMessageId,
        ...(threadId === "active-turn" ? { runtimeMode: "full-access" } : {}),
        ...(threadId === "ready-pending-turn"
          ? {
              modelSelection: { provider: "codex", model: "gpt-5.4" },
              titleSeed: "Investigate capture",
              sourceProposedPlan: {
                threadId: "source-plan-thread",
                planId: "plan-1",
              },
            }
          : {}),
        ...(usesApprovalRuntime ? { runtimeMode: "approval-required" } : {}),
        interactionMode: "plan",
      }),
    );
  }

  if (threadId === "active-with-pending-turn") {
    db.prepare(`
      INSERT INTO projection_thread_messages (
        message_id,
        thread_id,
        turn_id,
        role,
        text,
        attachments_json,
        is_streaming,
        created_at,
        updated_at
      ) VALUES
        ('message-active-with-pending-active', ?, NULL, 'user', 'Active prompt', '[]', 0, '2026-07-03T00:00:00.000Z', '2026-07-03T00:00:00.000Z'),
        ('message-active-with-pending-queued', ?, NULL, 'user', 'Queued prompt', '[]', 0, '2026-07-03T00:00:02.000Z', '2026-07-03T00:00:02.000Z')
    `).run(threadId, threadId);
    db.prepare(`
      INSERT INTO projection_turns (
        thread_id,
        turn_id,
        pending_message_id,
        state,
        requested_at,
        started_at,
        checkpoint_files_json
      ) VALUES
        (?, 'turn-active-with-pending', 'message-active-with-pending-active', 'running', '2026-07-03T00:00:00.000Z', '2026-07-03T00:00:00.000Z', '[]'),
        (?, NULL, 'message-active-with-pending-queued', 'pending', '2026-07-03T00:00:02.000Z', NULL, '[]')
    `).run(threadId, threadId);
    db.prepare(`
      INSERT INTO orchestration_events (
        event_id,
        aggregate_kind,
        stream_id,
        stream_version,
        event_type,
        occurred_at,
        actor_kind,
        payload_json,
        metadata_json
      ) VALUES ('event-active-with-pending-queued-start', 'thread', ?, 1, 'thread.turn-start-requested', '2026-07-03T00:00:02.000Z', 'system', ?, '{}')
    `).run(
      threadId,
      JSON.stringify({
        threadId,
        messageId: "message-active-with-pending-queued",
        runtimeMode: "full-access",
        interactionMode: "plan",
      }),
    );
  }
}

function expectedPendingMessage(threadId: string): {
  readonly message_id: string;
  readonly role: "user" | "system";
  readonly text: string;
  readonly attachments: ReadonlyArray<unknown>;
} {
  return {
    message_id: `message-${threadId}`,
    role: threadId === "pending-no-session" ? "system" : "user",
    text: `Pending prompt for ${threadId}`,
    attachments:
      threadId === "ready-pending-turn"
        ? [
            {
              type: "image",
              id: "ready-pending-turn-image",
              name: "ready.png",
              mimeType: "image/png",
              sizeBytes: 120,
            },
          ]
        : [],
    ...(threadId === "ready-pending-turn"
      ? {
          model_selection: { provider: "codex", model: "gpt-5.4" },
          title_seed: "Investigate capture",
          source_proposed_plan: {
            threadId: "source-plan-thread",
            planId: "plan-1",
          },
        }
      : {}),
  };
}

function readManifest(outPath: string): CaptureManifest {
  return JSON.parse(NodeFS.readFileSync(outPath, "utf8")) as CaptureManifest;
}

describe("capture-active-threads", () => {
  it("captures active sessions, pending turn starts, and waiting sessions while skipping idle, deleted, archived, and excluded threads", () => {
    const tempDir = makeTempDir();
    const dbPath = makeFixtureDb(tempDir);
    const outPath = NodePath.join(tempDir, "nested", "resume-manifest.json");

    const manifest = captureActiveThreads({
      dbPath,
      outPath,
      excludedThreadIds: ["excluded"],
      // @effect-diagnostics-next-line globalDate:off - deterministic fixture timestamp.
      capturedAt: new Date("2026-07-03T21:00:00.000Z"),
    });

    assert.deepStrictEqual(manifest, readManifest(outPath));
    assert.deepStrictEqual(
      NodeFS.readdirSync(NodePath.dirname(outPath)).filter((entry) => entry.endsWith(".tmp")),
      [],
    );
    assert.equal((NodeFS.statSync(outPath).mode & 0o777).toString(8), "600");
    assert.deepStrictEqual(manifest, {
      version: 1,
      captured_at: "2026-07-03T21:00:00.000Z",
      pre_sha: "",
      db_snapshot: "",
      threads: [
        {
          thread_id: "active-turn",
          role: "active",
          status: "running",
          active_turn_id: "turn-1",
          runtime_mode: "approval-required",
          interaction_mode: "plan",
          title: "Active Turn",
          project_id: "project-1",
          injected_at: null,
        },
        {
          thread_id: "active-with-pending-turn",
          role: "active",
          status: "running",
          active_turn_id: "turn-active-with-pending",
          runtime_mode: "approval-required",
          interaction_mode: "default",
          title: "Active With Pending Turn",
          project_id: "project-1",
          pending_message: {
            message_id: "message-active-with-pending-queued",
            role: "user",
            text: "Queued prompt",
            attachments: [],
            runtime_mode: "full-access",
            interaction_mode: "plan",
          },
          injected_at: null,
        },
        {
          thread_id: "pending-no-session",
          role: "active",
          status: "ready",
          active_turn_id: null,
          runtime_mode: "approval-required",
          interaction_mode: "plan",
          title: "Pending No Session",
          project_id: "project-1",
          pending_message: expectedPendingMessage("pending-no-session"),
          injected_at: null,
        },
        {
          thread_id: "ready-pending-turn",
          role: "active",
          status: "ready",
          active_turn_id: null,
          runtime_mode: "approval-required",
          interaction_mode: "plan",
          title: "Ready Pending Turn",
          project_id: "project-1",
          pending_message: expectedPendingMessage("ready-pending-turn"),
          injected_at: null,
        },
        {
          thread_id: "running-no-turn",
          role: "active",
          status: "running",
          active_turn_id: null,
          runtime_mode: "full-access",
          interaction_mode: "default",
          title: "Running No Turn",
          project_id: "project-1",
          injected_at: null,
        },
        {
          thread_id: "starting-no-turn",
          role: "active",
          status: "starting",
          active_turn_id: null,
          runtime_mode: "approval-required",
          interaction_mode: "plan",
          title: "Starting No Turn",
          project_id: "project-1",
          pending_message: expectedPendingMessage("starting-no-turn"),
          injected_at: null,
        },
        {
          thread_id: "terminal-fresh-pending-turn",
          role: "active",
          status: "error",
          active_turn_id: null,
          runtime_mode: "full-access",
          interaction_mode: "plan",
          title: "Terminal Fresh Pending Turn",
          project_id: "project-1",
          pending_message: expectedPendingMessage("terminal-fresh-pending-turn"),
          injected_at: null,
        },
        {
          thread_id: "terminal-stale-active-pending-turn",
          role: "active",
          status: "error",
          active_turn_id: null,
          runtime_mode: "full-access",
          interaction_mode: "plan",
          title: "Terminal Stale Active Pending Turn",
          project_id: "project-1",
          pending_message: expectedPendingMessage("terminal-stale-active-pending-turn"),
          injected_at: null,
        },
        {
          thread_id: "waiting",
          role: "waiting",
          status: "waiting",
          active_turn_id: null,
          runtime_mode: "full-access",
          interaction_mode: "plan",
          title: "Waiting Parent",
          project_id: "project-1",
          injected_at: null,
        },
      ],
    });
  });

  it("captures rows stopped during the current shutdown without replaying stale stopped interruptions", () => {
    const tempDir = makeTempDir();
    const dbPath = makeFixtureDb(tempDir);

    const manifest = captureActiveThreads({
      dbPath,
      outPath: NodePath.join(tempDir, "resume-manifest.json"),
      stoppedSince: "2026-07-03T00:00:02.900Z",
      pendingSince: "2026-07-03T00:00:02.000Z",
      includedPendingMessageIds: ["message-interrupted-current-pending-turn"],
      includedActiveThreadIds: ["stopped-interrupted-turn", "interrupted-current-turn"],
      // @effect-diagnostics-next-line globalDate:off - deterministic fixture timestamp.
      capturedAt: new Date("2026-07-03T21:00:00.000Z"),
    });
    const threadsById = new Map(manifest.threads.map((thread) => [thread.thread_id, thread]));

    assert.deepStrictEqual(threadsById.get("stopped-current-pending-turn"), {
      thread_id: "stopped-current-pending-turn",
      role: "active",
      status: "stopped",
      active_turn_id: null,
      runtime_mode: "full-access",
      interaction_mode: "plan",
      title: "Stopped Current Pending Turn",
      project_id: "project-1",
      pending_message: expectedPendingMessage("stopped-current-pending-turn"),
      injected_at: null,
    });
    assert.deepStrictEqual(threadsById.get("stopped-new-running-turn"), {
      thread_id: "stopped-new-running-turn",
      role: "active",
      status: "stopped",
      active_turn_id: "turn-stopped-new-running",
      runtime_mode: "full-access",
      interaction_mode: "plan",
      title: "Stopped New Running Turn",
      project_id: "project-1",
      injected_at: null,
    });
    assert.deepStrictEqual(threadsById.get("stopped-interrupted-turn"), {
      thread_id: "stopped-interrupted-turn",
      role: "active",
      status: "stopped",
      active_turn_id: "turn-stopped-interrupted",
      runtime_mode: "full-access",
      interaction_mode: "plan",
      title: "Stopped Interrupted Turn",
      project_id: "project-1",
      injected_at: null,
    });
    assert.deepStrictEqual(threadsById.get("interrupted-current-turn"), {
      thread_id: "interrupted-current-turn",
      role: "active",
      status: "interrupted",
      active_turn_id: "turn-interrupted-current",
      runtime_mode: "full-access",
      interaction_mode: "plan",
      title: "Interrupted Current Turn",
      project_id: "project-1",
      injected_at: null,
    });
    assert.deepStrictEqual(threadsById.get("interrupted-current-pending-turn"), {
      thread_id: "interrupted-current-pending-turn",
      role: "active",
      status: "interrupted",
      active_turn_id: null,
      runtime_mode: "full-access",
      interaction_mode: "plan",
      title: "Interrupted Current Pending Turn",
      project_id: "project-1",
      pending_message: expectedPendingMessage("interrupted-current-pending-turn"),
      injected_at: null,
    });
    assert.equal(threadsById.has("stopped-stale-pending-turn"), false);
    assert.equal(threadsById.has("stopped-stale-interrupted-turn"), false);
    assert.equal(threadsById.has("stopped-running-turn"), false);
    assert.equal(threadsById.has("errored-stale"), false);

    const includedManifest = captureActiveThreads({
      dbPath,
      outPath: NodePath.join(tempDir, "resume-manifest-included.json"),
      stoppedSince: "2026-07-03T00:00:02.900Z",
      pendingSince: "2026-07-03T00:00:02.000Z",
      includedPendingMessageIds: ["message-stopped-stale-pending-turn"],
      includedActiveThreadIds: ["stopped-interrupted-turn", "interrupted-current-turn"],
      // @effect-diagnostics-next-line globalDate:off - deterministic fixture timestamp.
      capturedAt: new Date("2026-07-03T21:00:00.000Z"),
    });
    const includedThreadsById = new Map(
      includedManifest.threads.map((thread) => [thread.thread_id, thread]),
    );

    assert.deepStrictEqual(includedThreadsById.get("stopped-stale-pending-turn"), {
      thread_id: "stopped-stale-pending-turn",
      role: "active",
      status: "stopped",
      active_turn_id: null,
      runtime_mode: "full-access",
      interaction_mode: "plan",
      title: "Stopped Stale Pending Turn",
      project_id: "project-1",
      pending_message: expectedPendingMessage("stopped-stale-pending-turn"),
      injected_at: null,
    });
  });

  it("rejects stopped-only capture without a pre-stop activity boundary", () => {
    const tempDir = makeTempDir();
    const dbPath = makeFixtureDb(tempDir);

    assert.throws(
      () =>
        captureActiveThreads({
          dbPath,
          outPath: NodePath.join(tempDir, "resume-manifest.json"),
          stoppedSince: "2026-07-03T00:00:02.900Z",
          // @effect-diagnostics-next-line globalDate:off - deterministic fixture timestamp.
          capturedAt: new Date("2026-07-03T21:00:00.000Z"),
        }),
      /--stopped-since requires --pending-since or at least one --include-active-thread-id/u,
    );
  });

  it("writes an empty manifest and exits successfully when no active sessions are captured", () => {
    const tempDir = makeTempDir();
    const dbPath = makeFixtureDb(tempDir);
    const outPath = NodePath.join(tempDir, "resume-manifest.json");
    const originalStdoutWrite = NodeProcess.stdout.write;
    let stdout = "";

    NodeProcess.stdout.write = ((chunk: string | Uint8Array) => {
      stdout += String(chunk);
      return true;
    }) as typeof NodeProcess.stdout.write;
    try {
      const exitCode = runCli(
        [
          "--db",
          dbPath,
          "--out",
          outPath,
          "--exclude",
          "active-turn",
          "--exclude",
          "active-with-pending-turn",
          "--exclude",
          "pending-no-session",
          "--exclude",
          "running-no-turn",
          "--exclude",
          "starting-no-turn",
          "--exclude",
          "ready-pending-turn",
          "--exclude",
          "terminal-fresh-pending-turn",
          "--exclude",
          "stopped-interrupted-turn",
          "--exclude",
          "stopped-running-turn",
          "--exclude",
          "terminal-stale-active-pending-turn",
          "--exclude",
          "waiting",
          "--exclude",
          "excluded",
        ],
        {},
      );

      assert.equal(exitCode, 0);
      assert.equal(stdout, "0\n");
      assert.deepStrictEqual(readManifest(outPath).threads, []);
    } finally {
      NodeProcess.stdout.write = originalStdoutWrite;
    }
  });

  it("deduplicates replayed turn-start events with the same message id", () => {
    const tempDir = makeTempDir();
    const dbPath = makeFixtureDb(tempDir);
    const db = new NodeSqlite.DatabaseSync(dbPath);
    try {
      db.prepare(`
        INSERT INTO orchestration_events (
          event_id,
          aggregate_kind,
          stream_id,
          stream_version,
          event_type,
          occurred_at,
          actor_kind,
          payload_json,
          metadata_json
        ) VALUES ('event-active-turn-replayed', 'thread', 'active-turn', 2, 'thread.turn-start-requested', '2026-07-03T00:00:02.000Z', 'system', ?, '{}')
      `).run(
        JSON.stringify({
          threadId: "active-turn",
          messageId: "message-active-turn",
          runtimeMode: "full-access",
          interactionMode: "default",
        }),
      );
    } finally {
      db.close();
    }

    const manifest = captureActiveThreads({
      dbPath,
      outPath: NodePath.join(tempDir, "resume-manifest.json"),
      // @effect-diagnostics-next-line globalDate:off - deterministic fixture timestamp.
      capturedAt: new Date("2026-07-03T21:00:00.000Z"),
    });
    const activeTurnRows = manifest.threads.filter((thread) => thread.thread_id === "active-turn");

    assert.equal(activeTurnRows.length, 1);
    assert.deepStrictEqual(activeTurnRows[0], {
      thread_id: "active-turn",
      role: "active",
      status: "running",
      active_turn_id: "turn-1",
      runtime_mode: "approval-required",
      interaction_mode: "default",
      title: "Active Turn",
      project_id: "project-1",
      injected_at: null,
    });
  });

  it("opens the SQLite database with mode=ro and query_only enabled", () => {
    const tempDir = makeTempDir();
    const dbPath = makeFixtureDb(tempDir);
    const readonlyDb = openCaptureDatabase(dbPath);
    try {
      const queryOnly = readonlyDb.prepare("PRAGMA query_only").all()[0] as {
        readonly query_only: number;
      };
      assert.equal(queryOnly.query_only, 1);
      assert.throws(() => readonlyDb.exec("CREATE TABLE read_only_probe (id TEXT)"));
    } finally {
      readonlyDb.close();
    }
  });

  it("uses T3DR_DB as the default DB path and lets --db override it", () => {
    assert.deepStrictEqual(
      parseArgs(["--out", "/tmp/out.json"], { T3DR_DB: "/tmp/from-env.sqlite" }),
      {
        dbPath: "/tmp/from-env.sqlite",
        outPath: "/tmp/out.json",
        excludedThreadIds: [],
        stoppedSince: null,
        pendingSince: null,
        includedPendingMessageIds: [],
        includedActiveThreadIds: [],
      },
    );

    assert.deepStrictEqual(
      parseArgs(
        [
          "--db",
          "/tmp/from-flag.sqlite",
          "--out",
          "/tmp/out.json",
          "--exclude",
          "thread-1",
          "--stopped-since",
          "2026-07-03T00:00:02.000Z",
          "--pending-since",
          "2026-07-03T00:00:01.000Z",
          "--include-pending-message-id",
          "message-1",
          "--include-active-thread-id",
          "thread-active",
        ],
        {
          T3DR_DB: "/tmp/from-env.sqlite",
        },
      ),
      {
        dbPath: "/tmp/from-flag.sqlite",
        outPath: "/tmp/out.json",
        excludedThreadIds: ["thread-1"],
        stoppedSince: "2026-07-03T00:00:02.000Z",
        pendingSince: "2026-07-03T00:00:01.000Z",
        includedPendingMessageIds: ["message-1"],
        includedActiveThreadIds: ["thread-active"],
      },
    );
  });
});
