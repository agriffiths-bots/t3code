#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off - Standalone ops CLI uses Node filesystem APIs before an Effect runtime exists.
// @effect-diagnostics globalConsole:off - Standalone ops CLI writes its required stdout/stderr summary directly.
// @effect-diagnostics globalDate:off - Injection timestamps must be wall-clock ISO strings for the restart manifest.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";

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
  readonly sleep?: (ms: number) => Promise<void>;
  readonly random?: () => number;
  readonly dispatchAttemptTimeoutMs?: number;
}

export interface InjectResumeResult {
  readonly injected: number;
  readonly skipped: number;
  readonly failed: number;
  readonly failures: Array<{ readonly threadId: string; readonly error: string }>;
}

const MAX_DISPATCH_ATTEMPTS = 5;
const DISPATCH_RETRY_BASE_MS = 4_000;
const DISPATCH_RETRY_JITTER = 0.2;
const DISPATCH_RETRY_SLEEP_BUDGET_MS = 60_000;
const DISPATCH_ATTEMPT_TIMEOUT_MS = 10_000;

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
  kind: "interaction-mode" | "command" | "message",
  manifest: ResumeManifest,
  threadId: string,
): string {
  const digest = NodeCrypto.createHash("sha256")
    .update(`daily-restart-resume:${kind}:${manifest.version}:${manifest.captured_at}:${threadId}`)
    .digest("hex")
    .slice(0, 32);
  return `daily-restart-resume-${kind}-${digest}`;
}

class DispatchHttpError extends Error {
  readonly status: number;
  readonly body: string;
  readonly retryAfterMs: number | null;

  constructor(status: number, body: string, retryAfterMs: number | null) {
    super(`HTTP ${status}${body ? ` ${body}` : ""}`);
    this.status = status;
    this.body = body;
    this.retryAfterMs = retryAfterMs;
  }
}

function parseRetryAfter(value: string | null): number | null {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1_000;

  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) return null;
  return Math.max(0, timestamp - Date.now());
}

function isTransientDispatchError(error: unknown): boolean {
  if (error instanceof DispatchHttpError) {
    return error.status === 429 || error.status >= 500;
  }

  if (error instanceof TypeError) return true;
  if (!isRecord(error)) return false;

  if (error.name === "AbortError") return true;

  const code = error.code;
  if (code === "ECONNREFUSED" || code === "ECONNRESET" || code === "ETIMEDOUT") return true;

  const cause = error.cause;
  return isRecord(cause) && isTransientDispatchError(cause);
}

function dispatchRetryDelayMs(attemptIndex: number, error: unknown, random: () => number): number {
  const retryAfterMs = error instanceof DispatchHttpError ? error.retryAfterMs : null;
  const exponentialMs = DISPATCH_RETRY_BASE_MS * 2 ** attemptIndex;
  const jitter = 1 + (random() * 2 - 1) * DISPATCH_RETRY_JITTER;
  const backoffMs = Math.round(exponentialMs * jitter);
  return Math.max(retryAfterMs ?? 0, backoffMs);
}

function sleep(ms: number): Promise<void> {
  return NodeTimersPromises.setTimeout(ms);
}

function clampDelayToBudget(delayMs: number, remainingSleepBudgetMs: number): number {
  return Math.max(0, Math.min(delayMs, remainingSleepBudgetMs));
}

async function postDispatchCommand(
  fetchImpl: typeof fetch,
  origin: string,
  token: string,
  command: Record<string, unknown>,
  attemptTimeoutMs: number,
): Promise<void> {
  const controller = new AbortController();
  const timeout = NodeTimersPromises.setTimeout(attemptTimeoutMs, undefined, {
    signal: controller.signal,
  })
    .then(() => controller.abort())
    .catch(() => undefined);

  try {
    const response = await fetchImpl(new URL("/api/orchestration/dispatch", origin), {
      method: "POST",
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      body: JSON.stringify(command),
      signal: controller.signal,
    });

    if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new DispatchHttpError(
        response.status,
        body,
        parseRetryAfter(response.headers.get("retry-after")),
      );
    }
  } finally {
    controller.abort();
    await timeout;
  }
}

async function postDispatchCommandWithRetry(
  fetchImpl: typeof fetch,
  origin: string,
  token: string,
  command: Record<string, unknown>,
  attemptTimeoutMs: number,
  sleepImpl: (ms: number) => Promise<void>,
  random: () => number,
): Promise<void> {
  let lastError: unknown;
  let remainingSleepBudgetMs = DISPATCH_RETRY_SLEEP_BUDGET_MS;
  for (let attempt = 0; attempt < MAX_DISPATCH_ATTEMPTS; attempt += 1) {
    try {
      await postDispatchCommand(fetchImpl, origin, token, command, attemptTimeoutMs);
      return;
    } catch (error) {
      lastError = error;
      if (!isTransientDispatchError(error) || attempt === MAX_DISPATCH_ATTEMPTS - 1) {
        throw error;
      }
      const delayMs = clampDelayToBudget(
        dispatchRetryDelayMs(attempt, error, random),
        remainingSleepBudgetMs,
      );
      remainingSleepBudgetMs -= delayMs;
      await sleepImpl(delayMs);
    }
  }

  throw lastError;
}

async function dispatchResumeCommands(
  fetchImpl: typeof fetch,
  origin: string,
  token: string,
  manifest: ResumeManifest,
  threadId: string,
  createdAt: string,
  attemptTimeoutMs: number,
  sleepImpl: (ms: number) => Promise<void>,
  random: () => number,
): Promise<void> {
  await postDispatchCommandWithRetry(
    fetchImpl,
    origin,
    token,
    {
      type: "thread.interaction-mode.set",
      commandId: stableDispatchId("interaction-mode", manifest, threadId),
      threadId,
      interactionMode: "default",
      createdAt,
    },
    attemptTimeoutMs,
    sleepImpl,
    random,
  );

  await postDispatchCommandWithRetry(
    fetchImpl,
    origin,
    token,
    {
      type: "thread.turn.start",
      commandId: stableDispatchId("command", manifest, threadId),
      threadId,
      message: {
        messageId: stableDispatchId("message", manifest, threadId),
        role: "user",
        text: RESUME_MESSAGE,
        attachments: [],
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      createdAt,
    },
    attemptTimeoutMs,
    sleepImpl,
    random,
  );
}

export async function injectResume(options: InjectResumeOptions): Promise<InjectResumeResult> {
  const manifest = await readManifest(options.manifestPath);
  const fetchImpl = options.fetchImpl ?? fetch;
  const now = options.now ?? (() => new Date());
  const sleepImpl = options.sleep ?? sleep;
  const random = options.random ?? Math.random;
  const dispatchAttemptTimeoutMs = options.dispatchAttemptTimeoutMs ?? DISPATCH_ATTEMPT_TIMEOUT_MS;
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
      await dispatchResumeCommands(
        fetchImpl,
        options.origin,
        token,
        manifest,
        thread.thread_id,
        injectedAt,
        dispatchAttemptTimeoutMs,
        sleepImpl,
        random,
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
