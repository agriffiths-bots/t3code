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
    ["running-no-turn", "Running No Turn", null, null, "running", null],
    ["starting-no-turn", "Starting No Turn", null, null, "starting", null],
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

  db.prepare(`
    INSERT INTO projection_thread_sessions (
      thread_id,
      status,
      active_turn_id,
      runtime_mode,
      updated_at
    ) VALUES (?, ?, ?, ?, '2026-07-03T00:00:00.000Z')
  `).run(
    threadId,
    status,
    activeTurnId,
    threadId === "active-turn" ? "approval-required" : "full-access",
  );

  if (threadId === "active-turn") {
    db.prepare(`
      INSERT INTO projection_turns (
        thread_id,
        turn_id,
        pending_message_id,
        state,
        requested_at,
        checkpoint_files_json
      ) VALUES (?, ?, ?, 'running', '2026-07-03T00:00:01.000Z', '[]')
    `).run(threadId, activeTurnId, "message-active-turn");

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
      "event-active-turn-start",
      threadId,
      JSON.stringify({
        threadId,
        messageId: "message-active-turn",
        interactionMode: "plan",
      }),
    );
  }
}

function readManifest(outPath: string): CaptureManifest {
  return JSON.parse(NodeFS.readFileSync(outPath, "utf8")) as CaptureManifest;
}

describe("capture-active-threads", () => {
  it("captures active and waiting sessions while skipping idle, deleted, archived, and excluded threads", () => {
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
          runtime_mode: "full-access",
          interaction_mode: "default",
          title: "Starting No Turn",
          project_id: "project-1",
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
          "running-no-turn",
          "--exclude",
          "starting-no-turn",
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
      },
    );

    assert.deepStrictEqual(
      parseArgs(
        ["--db", "/tmp/from-flag.sqlite", "--out", "/tmp/out.json", "--exclude", "thread-1"],
        {
          T3DR_DB: "/tmp/from-env.sqlite",
        },
      ),
      {
        dbPath: "/tmp/from-flag.sqlite",
        outPath: "/tmp/out.json",
        excludedThreadIds: ["thread-1"],
      },
    );
  });
});
