#!/usr/bin/env node

// @effect-diagnostics-next-line nodeBuiltinImport:off - shutdown capture writes an atomic JSON manifest.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off - shutdown capture resolves CLI file paths.
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import * as NodeSqlite from "node:sqlite";
import * as NodeURL from "node:url";

const DEFAULT_DB_PATH = "/home/adam/.t3-vps/userdata/state.sqlite";
const WAITING_STATUSES = new Set(["waiting"]);

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
  readonly title: string;
  readonly project_id: string;
  readonly injected_at: null;
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
  readonly title: string;
  readonly project_id: string;
}

interface ParsedArgs {
  readonly dbPath: string;
  readonly outPath: string;
  readonly excludedThreadIds: ReadonlyArray<string>;
}

export class CaptureActiveThreadsCliError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CaptureActiveThreadsCliError";
  }
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
  return row.active_turn_id !== null || row.status === "running" ? "active" : "waiting";
}

function buildCaptureQuery(excludedThreadIds: ReadonlyArray<string>) {
  const excludedClause =
    excludedThreadIds.length > 0
      ? `AND sessions.thread_id NOT IN (${excludedThreadIds.map(() => "?").join(", ")})`
      : "";

  return `
    SELECT
      sessions.thread_id,
      sessions.status,
      sessions.active_turn_id,
      threads.title,
      threads.project_id
    FROM projection_thread_sessions sessions
    INNER JOIN projection_threads threads ON threads.thread_id = sessions.thread_id
    WHERE threads.deleted_at IS NULL
      AND threads.archived_at IS NULL
      AND (
        sessions.active_turn_id IS NOT NULL
        OR sessions.status = 'running'
        OR sessions.status = 'waiting'
      )
      ${excludedClause}
    ORDER BY sessions.thread_id
  `;
}

function readCapturedThreads(
  db: CaptureDatabase,
  excludedThreadIds: ReadonlyArray<string>,
): ReadonlyArray<CapturedThread> {
  const rows = db
    .prepare(buildCaptureQuery(excludedThreadIds))
    .all(...excludedThreadIds) as unknown as ReadonlyArray<SessionRow>;

  return rows
    .filter(
      (row) =>
        row.active_turn_id !== null || row.status === "running" || WAITING_STATUSES.has(row.status),
    )
    .map((row) => ({
      thread_id: row.thread_id,
      role: roleForRow(row),
      status: row.status,
      active_turn_id: row.active_turn_id,
      title: row.title,
      project_id: row.project_id,
      injected_at: null,
    }));
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
    NodeFS.writeFileSync(tempPath, payload, { encoding: "utf8", flag: "wx" });
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
    throw new CaptureActiveThreadsCliError(`Missing value for ${flag}.`);
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
        throw new CaptureActiveThreadsCliError(
          "Usage: capture-active-threads --db PATH --out FILE [--exclude THREAD_ID]...",
        );
      default:
        throw new CaptureActiveThreadsCliError(`Unknown argument: ${arg}`);
    }
  }

  if (!outPath) {
    throw new CaptureActiveThreadsCliError("Missing required --out FILE.");
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
