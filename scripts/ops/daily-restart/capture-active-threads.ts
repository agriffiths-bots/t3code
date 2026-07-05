#!/usr/bin/env node

// @effect-diagnostics-next-line nodeBuiltinImport:off - shutdown capture writes an atomic JSON manifest.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off - shutdown capture resolves CLI file paths.
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import * as NodeSqlite from "node:sqlite";
import * as NodeURL from "node:url";

const DEFAULT_DB_PATH = "/home/adam/.t3-vps/userdata/state.sqlite";
const ACTIVE_STATUSES = new Set(["running", "starting"]);
const TERMINAL_STATUSES = new Set(["error", "interrupted", "stopped"]);

export interface CaptureActiveThreadsOptions {
  readonly dbPath: string;
  readonly outPath: string;
  readonly excludedThreadIds?: ReadonlyArray<string>;
  readonly capturedAt?: Date;
  readonly openDatabase?: (dbPath: string) => CaptureDatabase;
}

export interface CaptureDatabase {
  readonly exec: (sql: string) => unknown;
  readonly prepare: (sql: string) => {
    readonly all: (
      ...params: Array<NodeSqlite.SQLInputValue>
    ) => ReadonlyArray<Record<string, NodeSqlite.SQLOutputValue>>;
  };
  readonly close: () => unknown;
}

export interface CapturedThread {
  readonly thread_id: string;
  readonly role: "active" | "waiting";
  readonly status: string;
  readonly active_turn_id: string | null;
  readonly runtime_mode: string;
  readonly interaction_mode: string;
  readonly title: string;
  readonly project_id: string;
  readonly pending_message?: CapturedPendingMessage;
  readonly injected_at: null;
}

export interface CapturedPendingMessage {
  readonly message_id: string;
  readonly text: string;
  readonly attachments: ReadonlyArray<unknown>;
  readonly model_selection?: unknown;
  readonly title_seed?: string;
  readonly source_proposed_plan?: unknown;
}

export interface CaptureManifest {
  readonly version: 1;
  readonly captured_at: string;
  readonly pre_sha: "";
  readonly db_snapshot: "";
  readonly threads: ReadonlyArray<CapturedThread>;
}

interface SessionRow {
  readonly thread_id: string;
  readonly status: string;
  readonly active_turn_id: string | null;
  readonly pending_turn_thread_id: string | null;
  readonly runtime_mode: string;
  readonly interaction_mode: string;
  readonly title: string;
  readonly project_id: string;
  readonly pending_message_id: string | null;
  readonly pending_message_text: string | null;
  readonly pending_message_attachments_json: string | null;
  readonly pending_model_selection_json: string | null;
  readonly pending_title_seed: string | null;
  readonly pending_source_proposed_plan_json: string | null;
}

interface ParsedArgs {
  readonly dbPath: string;
  readonly outPath: string;
  readonly excludedThreadIds: ReadonlyArray<string>;
}

function dbFileUri(dbPath: string): string {
  const url = NodeURL.pathToFileURL(NodePath.resolve(dbPath));
  url.searchParams.set("mode", "ro");
  return url.href;
}

export function openCaptureDatabase(dbPath: string): CaptureDatabase {
  const db = new NodeSqlite.DatabaseSync(dbFileUri(dbPath));
  db.exec("PRAGMA query_only = ON");
  return db;
}

function roleForRow(row: SessionRow): CapturedThread["role"] {
  return (row.active_turn_id !== null && !TERMINAL_STATUSES.has(row.status)) ||
    row.pending_turn_thread_id !== null ||
    ACTIVE_STATUSES.has(row.status)
    ? "active"
    : "waiting";
}

function buildCaptureQuery(excludedThreadIds: ReadonlyArray<string>) {
  const excludedClause =
    excludedThreadIds.length > 0
      ? `AND threads.thread_id NOT IN (${excludedThreadIds.map(() => "?").join(", ")})`
      : "";
  const livePendingPredicate = `(
          pending_turns.thread_id IS NOT NULL
          AND (
            sessions.updated_at IS NULL
            OR (
              sessions.status IN ('error', 'interrupted', 'stopped')
              AND pending_turns.requested_at > sessions.updated_at
            )
            OR sessions.status = 'starting'
            OR (
              sessions.status NOT IN ('error', 'interrupted', 'stopped')
              AND pending_turns.requested_at >= sessions.updated_at
            )
          )
        )`;

  return `
    SELECT
      threads.thread_id,
      COALESCE(sessions.status, 'ready') AS status,
      sessions.active_turn_id,
      CASE
        WHEN ${livePendingPredicate} THEN pending_turns.thread_id
        ELSE NULL
      END AS pending_turn_thread_id,
      COALESCE(
        CASE
          WHEN ${livePendingPredicate}
            THEN json_extract(turn_start_events.payload_json, '$.runtimeMode')
          ELSE NULL
        END,
        sessions.runtime_mode,
        threads.runtime_mode
      ) AS runtime_mode,
      COALESCE(
        json_extract(turn_start_events.payload_json, '$.interactionMode'),
        threads.interaction_mode
      ) AS interaction_mode,
      threads.title,
      threads.project_id,
      CASE
        WHEN ${livePendingPredicate} THEN pending_turns.pending_message_id
        ELSE NULL
      END AS pending_message_id,
      CASE
        WHEN ${livePendingPredicate} THEN pending_messages.text
        ELSE NULL
      END AS pending_message_text,
      CASE
        WHEN ${livePendingPredicate} THEN pending_messages.attachments_json
        ELSE NULL
      END AS pending_message_attachments_json,
      CASE
        WHEN ${livePendingPredicate} THEN json_extract(turn_start_events.payload_json, '$.modelSelection')
        ELSE NULL
      END AS pending_model_selection_json,
      CASE
        WHEN ${livePendingPredicate} THEN json_extract(turn_start_events.payload_json, '$.titleSeed')
        ELSE NULL
      END AS pending_title_seed,
      CASE
        WHEN ${livePendingPredicate} THEN json_extract(turn_start_events.payload_json, '$.sourceProposedPlan')
        ELSE NULL
      END AS pending_source_proposed_plan_json
    FROM projection_threads threads
    LEFT JOIN projection_thread_sessions sessions ON sessions.thread_id = threads.thread_id
    LEFT JOIN projection_turns active_turns
      ON active_turns.thread_id = threads.thread_id
      AND active_turns.turn_id = sessions.active_turn_id
    LEFT JOIN projection_turns pending_turns
      ON pending_turns.thread_id = threads.thread_id
      AND (
        sessions.active_turn_id IS NULL
        OR sessions.status IN ('error', 'interrupted', 'stopped')
      )
      AND pending_turns.turn_id IS NULL
      AND pending_turns.state = 'pending'
    LEFT JOIN orchestration_events turn_start_events
      ON turn_start_events.stream_id = threads.thread_id
      AND turn_start_events.event_type = 'thread.turn-start-requested'
      AND json_extract(turn_start_events.payload_json, '$.messageId') =
        COALESCE(
          CASE
            WHEN sessions.status NOT IN ('error', 'interrupted', 'stopped')
              THEN active_turns.pending_message_id
            ELSE NULL
          END,
          CASE WHEN ${livePendingPredicate} THEN pending_turns.pending_message_id ELSE NULL END
        )
    LEFT JOIN projection_thread_messages pending_messages
      ON pending_messages.thread_id = threads.thread_id
      AND pending_messages.message_id = pending_turns.pending_message_id
    WHERE threads.deleted_at IS NULL
      AND threads.archived_at IS NULL
      AND (
        (
          sessions.active_turn_id IS NOT NULL
          AND sessions.status NOT IN ('error', 'interrupted', 'stopped')
        )
        OR sessions.status = 'running'
        OR sessions.status = 'starting'
        OR sessions.status = 'waiting'
        OR ${livePendingPredicate}
      )
      ${excludedClause}
    ORDER BY threads.thread_id
  `;
}

function parsePendingMessageAttachments(
  threadId: string,
  attachmentsJson: string | null,
): ReadonlyArray<unknown> {
  if (attachmentsJson === null) return [];

  const parsed = JSON.parse(attachmentsJson) as unknown;
  if (!Array.isArray(parsed)) {
    throw new Error(`thread ${threadId} pending message attachments must be an array`);
  }
  return parsed;
}

function parseOptionalJsonField(
  threadId: string,
  fieldName: string,
  valueJson: string | null,
): unknown | undefined {
  if (valueJson === null) return undefined;

  try {
    return JSON.parse(valueJson) as unknown;
  } catch (error) {
    throw new Error(`thread ${threadId} pending ${fieldName} metadata must be valid JSON`, {
      cause: error,
    });
  }
}

function pendingMessageForRow(row: SessionRow): CapturedPendingMessage | undefined {
  if (row.pending_turn_thread_id === null) return undefined;
  if (row.pending_message_id === null) {
    throw new Error(`thread ${row.thread_id} pending message id was not found`);
  }
  if (row.pending_message_text === null) {
    throw new Error(`thread ${row.thread_id} pending message text was not found`);
  }

  const modelSelection = parseOptionalJsonField(
    row.thread_id,
    "model_selection",
    row.pending_model_selection_json,
  );
  const sourceProposedPlan = parseOptionalJsonField(
    row.thread_id,
    "source_proposed_plan",
    row.pending_source_proposed_plan_json,
  );

  return {
    message_id: row.pending_message_id,
    text: row.pending_message_text,
    attachments: parsePendingMessageAttachments(
      row.thread_id,
      row.pending_message_attachments_json,
    ),
    ...(modelSelection !== undefined ? { model_selection: modelSelection } : {}),
    ...(row.pending_title_seed !== null ? { title_seed: row.pending_title_seed } : {}),
    ...(sourceProposedPlan !== undefined ? { source_proposed_plan: sourceProposedPlan } : {}),
  };
}

function readCapturedThreads(
  db: CaptureDatabase,
  excludedThreadIds: ReadonlyArray<string>,
): ReadonlyArray<CapturedThread> {
  const rows = db
    .prepare(buildCaptureQuery(excludedThreadIds))
    .all(...excludedThreadIds) as unknown as ReadonlyArray<SessionRow>;

  return rows.map((row) => {
    const pendingMessage = pendingMessageForRow(row);
    return {
      thread_id: row.thread_id,
      role: roleForRow(row),
      status: row.status,
      active_turn_id: row.active_turn_id,
      runtime_mode: row.runtime_mode,
      interaction_mode: row.interaction_mode,
      title: row.title,
      project_id: row.project_id,
      ...(pendingMessage ? { pending_message: pendingMessage } : {}),
      injected_at: null,
    };
  });
}

function writeJsonAtomic(outPath: string, manifest: CaptureManifest): void {
  const outDir = NodePath.dirname(outPath);
  NodeFS.mkdirSync(outDir, { recursive: true });

  const tempPath = NodePath.join(
    outDir,
    // @effect-diagnostics-next-line globalDate:off - temp suffix only needs a unique wall-clock component.
    `.${NodePath.basename(outPath)}.${NodeProcess.pid}.${Date.now()}.tmp`,
  );
  const payload = `${JSON.stringify(manifest, null, 2)}\n`;

  try {
    NodeFS.writeFileSync(tempPath, payload, { encoding: "utf8", flag: "wx", mode: 0o600 });
    NodeFS.renameSync(tempPath, outPath);
  } catch (error) {
    try {
      NodeFS.rmSync(tempPath, { force: true });
    } catch {
      // Preserve the original write/rename error.
    }
    throw error;
  }
}

export function captureActiveThreads(options: CaptureActiveThreadsOptions): CaptureManifest {
  const db = (options.openDatabase ?? openCaptureDatabase)(options.dbPath);
  try {
    const threads = readCapturedThreads(db, options.excludedThreadIds ?? []);
    const manifest = {
      version: 1,
      // @effect-diagnostics-next-line globalDate:off - CLI capture timestamp is an ISO UTC wall-clock value.
      captured_at: (options.capturedAt ?? new Date()).toISOString(),
      pre_sha: "",
      db_snapshot: "",
      threads,
    } satisfies CaptureManifest;

    writeJsonAtomic(options.outPath, manifest);
    return manifest;
  } finally {
    db.close();
  }
}

function requireValue(args: ReadonlyArray<string>, index: number, flag: string): string {
  const value = args[index + 1];
  if (value === undefined || value.startsWith("--")) {
    throw new Error(`Missing value for ${flag}.`);
  }
  return value;
}

export function parseArgs(
  args: ReadonlyArray<string>,
  env: NodeJS.ProcessEnv = NodeProcess.env,
): ParsedArgs {
  let dbPath = env.T3DR_DB ?? DEFAULT_DB_PATH;
  let outPath: string | undefined;
  const excludedThreadIds: Array<string> = [];

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    switch (arg) {
      case "--db":
        dbPath = requireValue(args, index, arg);
        index += 1;
        break;
      case "--out":
        outPath = requireValue(args, index, arg);
        index += 1;
        break;
      case "--exclude":
        excludedThreadIds.push(requireValue(args, index, arg));
        index += 1;
        break;
      case "--help":
      case "-h":
        throw new Error(
          "Usage: capture-active-threads --db PATH --out FILE [--exclude THREAD_ID]...",
        );
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  if (!outPath) {
    throw new Error("Missing required --out FILE.");
  }

  return { dbPath, outPath, excludedThreadIds };
}

export function runCli(
  args: ReadonlyArray<string>,
  env: NodeJS.ProcessEnv = NodeProcess.env,
): number {
  try {
    const parsed = parseArgs(args, env);
    const manifest = captureActiveThreads({
      dbPath: parsed.dbPath,
      outPath: parsed.outPath,
      excludedThreadIds: parsed.excludedThreadIds,
    });
    const activeCount = manifest.threads.filter((thread) => thread.role === "active").length;
    NodeProcess.stdout.write(`${activeCount}\n`);
    return 0;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    NodeProcess.stderr.write(`capture-active-threads: ${message}\n`);
    return 1;
  }
}

if (import.meta.main) {
  NodeProcess.exit(runCli(NodeProcess.argv.slice(2)));
}
