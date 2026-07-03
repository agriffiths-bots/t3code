#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off - Standalone ops CLI uses Node filesystem APIs before an Effect runtime exists.
// @effect-diagnostics globalConsole:off - Standalone ops CLI writes its required stdout/stderr summary directly.
// @effect-diagnostics globalDate:off - Injection timestamps must be wall-clock ISO strings for the restart manifest.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

export const RESUME_MESSAGE =
  "Continue after restart: the T3 server completed its scheduled daily restart/update. Your previous turn may have been interrupted mid-work. Re-read your task brief and ledger memo, reconcile actual state (files, PRs, processes) against your plan, and continue from where you left off. If you had already finished, verify your memo is written and settle.";

export interface ResumeManifestThread {
  readonly thread_id: string;
  readonly role: "active" | "waiting";
  readonly status?: string;
  readonly active_turn_id?: string | null;
  readonly title?: string | null;
  injected_at: string | null;
}

export interface ResumeManifest {
  readonly version: 1;
  readonly captured_at: string;
  readonly threads: Array<ResumeManifestThread>;
  readonly [key: string]: unknown;
}

export interface InjectResumeOptions {
  readonly manifestPath: string;
  readonly origin: string;
  readonly token?: string;
  readonly dryRun: boolean;
  readonly now?: () => Date;
  readonly fetchImpl?: typeof fetch;
}

export interface InjectResumeResult {
  readonly injected: number;
  readonly skipped: number;
  readonly failed: number;
  readonly failures: Array<{ readonly threadId: string; readonly error: string }>;
}

function usage(): string {
  return `usage: inject-resume --manifest FILE [--origin URL] [--token TOKEN] [--dry-run]

Defaults: --origin reads T3DR_ORIGIN, then http://127.0.0.1:3773.
Auth: --token overrides T3DR_TOKEN, then T3_TOKEN.`;
}

function parseArgs(argv: ReadonlyArray<string>, env: NodeJS.ProcessEnv) {
  let manifestPath: string | undefined;
  let origin = env.T3DR_ORIGIN?.trim() || "http://127.0.0.1:3773";
  let token = env.T3DR_TOKEN?.trim() || env.T3_TOKEN?.trim() || undefined;
  let dryRun = false;

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    switch (arg) {
      case "--manifest":
        manifestPath = argv[++index];
        break;
      case "--origin":
        origin = argv[++index] ?? "";
        break;
      case "--token":
        token = argv[++index];
        break;
      case "--dry-run":
        dryRun = true;
        break;
      case "--help":
      case "-h":
        console.log(usage());
        process.exit(0);
      default:
        throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (!manifestPath) throw new Error("--manifest FILE is required");
  if (!origin) throw new Error("--origin URL is required");
  if (!dryRun && !token)
    throw new Error("--token TOKEN is required unless T3DR_TOKEN or T3_TOKEN is set");

  return { manifestPath, origin, token, dryRun };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function parseManifest(raw: string): ResumeManifest {
  const parsed = JSON.parse(raw) as unknown;
  if (!isRecord(parsed)) throw new Error("manifest must be a JSON object");
  if (parsed.version !== 1) throw new Error("manifest version must be 1");
  if (typeof parsed.captured_at !== "string")
    throw new Error("manifest captured_at must be a string");
  if (!Array.isArray(parsed.threads)) throw new Error("manifest threads must be an array");

  for (const [index, thread] of parsed.threads.entries()) {
    if (!isRecord(thread)) throw new Error(`thread ${index} must be an object`);
    if (typeof thread.thread_id !== "string" || thread.thread_id.length === 0) {
      throw new Error(`thread ${index} thread_id must be a non-empty string`);
    }
    if (thread.role !== "active" && thread.role !== "waiting") {
      throw new Error(`thread ${thread.thread_id} role must be active or waiting`);
    }
    if (thread.injected_at !== null && typeof thread.injected_at !== "string") {
      throw new Error(`thread ${thread.thread_id} injected_at must be null or a string`);
    }
  }

  return parsed as ResumeManifest;
}

async function readManifest(path: string): Promise<ResumeManifest> {
  return parseManifest(await NodeFSP.readFile(path, "utf8"));
}

async function writeManifestAtomic(path: string, manifest: ResumeManifest): Promise<void> {
  const dir = NodePath.dirname(path);
  const base = NodePath.basename(path);
  const tmp = NodePath.join(dir, `.${base}.${process.pid}.${NodeCrypto.randomUUID()}.tmp`);
  await NodeFSP.writeFile(tmp, `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o600 });
  await NodeFSP.rename(tmp, path);
}

function stableDispatchId(
  kind: "command" | "message",
  manifest: ResumeManifest,
  threadId: string,
): string {
  const digest = NodeCrypto.createHash("sha256")
    .update(`daily-restart-resume:${kind}:${manifest.version}:${manifest.captured_at}:${threadId}`)
    .digest("hex")
    .slice(0, 32);
  return `daily-restart-resume-${kind}-${digest}`;
}

async function postResumeTurn(
  fetchImpl: typeof fetch,
  origin: string,
  token: string,
  threadId: string,
  commandId: string,
  messageId: string,
  createdAt: string,
): Promise<void> {
  const response = await fetchImpl(new URL("/api/orchestration/dispatch", origin), {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({
      type: "thread.turn.start",
      commandId,
      threadId,
      message: {
        messageId,
        role: "user",
        text: RESUME_MESSAGE,
        attachments: [],
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      createdAt,
    }),
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`HTTP ${response.status}${body ? ` ${body}` : ""}`);
  }
}

export async function injectResume(options: InjectResumeOptions): Promise<InjectResumeResult> {
  const manifest = await readManifest(options.manifestPath);
  const fetchImpl = options.fetchImpl ?? fetch;
  const now = options.now ?? (() => new Date());
  const failures: Array<{ threadId: string; error: string }> = [];
  let injected = 0;
  let skipped = 0;

  for (const thread of manifest.threads) {
    if (thread.role !== "active" || thread.injected_at !== null) {
      skipped += 1;
      continue;
    }

    if (options.dryRun) {
      injected += 1;
      continue;
    }

    const injectedAt = now().toISOString();
    try {
      const token = options.token;
      if (!token) throw new Error("missing bearer token");
      await postResumeTurn(
        fetchImpl,
        options.origin,
        token,
        thread.thread_id,
        stableDispatchId("command", manifest, thread.thread_id),
        stableDispatchId("message", manifest, thread.thread_id),
        injectedAt,
      );
      thread.injected_at = injectedAt;
      await writeManifestAtomic(options.manifestPath, manifest);
      injected += 1;
    } catch (error) {
      failures.push({
        threadId: thread.thread_id,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return { injected, skipped, failed: failures.length, failures };
}

function printSummary(result: InjectResumeResult): void {
  console.log(`injected=${result.injected} skipped=${result.skipped} failed=${result.failed}`);
  for (const failure of result.failures) {
    console.error(`failed thread_id=${failure.threadId}: ${failure.error}`);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    const args = parseArgs(process.argv.slice(2), process.env);
    const options: InjectResumeOptions = {
      manifestPath: args.manifestPath,
      origin: args.origin,
      dryRun: args.dryRun,
      ...(args.token ? { token: args.token } : {}),
    };
    const result = await injectResume({
      ...options,
    });
    printSummary(result);
    process.exit(result.failed > 0 ? 1 : 0);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    console.error(usage());
    process.exit(2);
  }
}
