#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off - Standalone ops CLI uses Node filesystem APIs before an Effect runtime exists.
// @effect-diagnostics globalConsole:off - Standalone ops CLI writes its required stdout/stderr summary directly.
// @effect-diagnostics globalDate:off - Injection timestamps must be wall-clock ISO strings for the restart manifest.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeTimersPromises from "node:timers/promises";

export const RESUME_MESSAGE =
  "Continue after restart: the T3 server completed its scheduled daily restart/update. Your previous turn may have been interrupted mid-work. Re-read your task brief and ledger memo, reconcile actual state (files, PRs, processes) against your plan, and continue from where you left off. If you had already finished, verify your memo is written and settle.";

export interface ResumeManifestThread {
  readonly thread_id: string;
  readonly role: "active" | "waiting";
  readonly status?: string;
  readonly active_turn_id?: string | null;
  readonly runtime_mode?: ResumeRuntimeMode;
  readonly interaction_mode?: ResumeInteractionMode;
  readonly pending_message?: ResumeManifestPendingMessage;
  readonly title?: string | null;
  injected_at: string | null;
}

export interface ResumeManifestPendingMessage {
  readonly message_id: string;
  readonly role?: "user" | "system";
  readonly text: string;
  readonly attachments?: ReadonlyArray<unknown>;
  readonly runtime_mode?: ResumeRuntimeMode;
  readonly interaction_mode?: ResumeInteractionMode;
  readonly model_selection?: unknown;
  readonly title_seed?: string;
  readonly source_proposed_plan?: unknown;
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
  readonly attachmentsDir?: string;
  readonly now?: () => Date;
  readonly fetchImpl?: typeof fetch;
  readonly sleep?: (ms: number) => Promise<void>;
  readonly random?: () => number;
  readonly dispatchAttemptTimeoutMs?: number;
  readonly resumeStartTimeoutMs?: number;
  readonly resumeStartPollMs?: number;
}

export interface InjectResumeResult {
  readonly injected: number;
  readonly skipped: number;
  readonly failed: number;
  readonly failures: Array<{ readonly threadId: string; readonly error: string }>;
}

export interface InjectResumeCliArgs {
  readonly manifestPath: string;
  readonly origin: string;
  readonly token?: string;
  readonly attachmentsDir?: string;
  readonly dryRun: boolean;
}

const MAX_DISPATCH_ATTEMPTS = 5;
const DISPATCH_RETRY_BASE_MS = 4_000;
const DISPATCH_RETRY_JITTER = 0.2;
const DISPATCH_RETRY_SLEEP_BUDGET_MS = 60_000;
const DISPATCH_ATTEMPT_TIMEOUT_MS = 10_000;
const RESUME_START_TIMEOUT_MS = 120_000;
const RESUME_START_POLL_MS = 1_000;
type ResumeRuntimeMode = "approval-required" | "auto-accept-edits" | "full-access";
type ResumeInteractionMode = "default" | "plan";
const DEFAULT_RESUME_RUNTIME_MODE: ResumeRuntimeMode = "full-access";
const DEFAULT_RESUME_INTERACTION_MODE: ResumeInteractionMode = "default";
const IMAGE_EXTENSION_BY_MIME_TYPE: Record<string, string> = {
  "image/avif": ".avif",
  "image/bmp": ".bmp",
  "image/gif": ".gif",
  "image/heic": ".heic",
  "image/heif": ".heif",
  "image/jpeg": ".jpg",
  "image/jpg": ".jpg",
  "image/png": ".png",
  "image/svg+xml": ".svg",
  "image/tiff": ".tiff",
  "image/webp": ".webp",
};
const ATTACHMENT_FILE_EXTENSIONS = [
  ".avif",
  ".bmp",
  ".gif",
  ".heic",
  ".heif",
  ".ico",
  ".jpeg",
  ".jpg",
  ".png",
  ".svg",
  ".tiff",
  ".webp",
  ".bin",
];
const RESUME_RUNTIME_MODES = new Set<ResumeRuntimeMode>([
  "approval-required",
  "auto-accept-edits",
  "full-access",
]);
const RESUME_INTERACTION_MODES = new Set<ResumeInteractionMode>(["default", "plan"]);

function usage(): string {
  return `usage: inject-resume --manifest FILE [--origin URL] [--token TOKEN] [--attachments-dir DIR] [--dry-run]

Defaults: --origin reads T3DR_ORIGIN, then http://127.0.0.1:3773.
Auth: --token overrides T3DR_TOKEN, then T3_TOKEN.
Attachments: --attachments-dir overrides T3DR_ATTACHMENTS_DIR.`;
}

export function parseArgs(
  argv: ReadonlyArray<string>,
  env: NodeJS.ProcessEnv,
): InjectResumeCliArgs {
  let manifestPath: string | undefined;
  let origin = env.T3DR_ORIGIN?.trim() || "http://127.0.0.1:3773";
  let token = env.T3DR_TOKEN?.trim() || env.T3_TOKEN?.trim() || undefined;
  let attachmentsDir = env.T3DR_ATTACHMENTS_DIR?.trim() || undefined;
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
      case "--attachments-dir":
        attachmentsDir = argv[++index];
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

  return {
    manifestPath,
    origin,
    dryRun,
    ...(token ? { token } : {}),
    ...(attachmentsDir ? { attachmentsDir } : {}),
  };
}

export function optionsFromCliArgs(args: InjectResumeCliArgs): InjectResumeOptions {
  return {
    manifestPath: args.manifestPath,
    origin: args.origin,
    dryRun: args.dryRun,
    ...(args.token ? { token: args.token } : {}),
    ...(args.attachmentsDir ? { attachmentsDir: args.attachmentsDir } : {}),
  };
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
    if (
      thread.runtime_mode !== undefined &&
      !RESUME_RUNTIME_MODES.has(thread.runtime_mode as ResumeRuntimeMode)
    ) {
      throw new Error(`thread ${thread.thread_id} runtime_mode must be a valid runtime mode`);
    }
    if (
      thread.interaction_mode !== undefined &&
      !RESUME_INTERACTION_MODES.has(thread.interaction_mode as ResumeInteractionMode)
    ) {
      throw new Error(
        `thread ${thread.thread_id} interaction_mode must be a valid interaction mode`,
      );
    }
    if (thread.pending_message !== undefined) {
      if (!isRecord(thread.pending_message)) {
        throw new Error(`thread ${thread.thread_id} pending_message must be an object`);
      }
      if (
        typeof thread.pending_message.message_id !== "string" ||
        thread.pending_message.message_id.length === 0
      ) {
        throw new Error(`thread ${thread.thread_id} pending_message.message_id must be a string`);
      }
      if (typeof thread.pending_message.text !== "string") {
        throw new Error(`thread ${thread.thread_id} pending_message.text must be a string`);
      }
      if (
        thread.pending_message.role !== undefined &&
        thread.pending_message.role !== "user" &&
        thread.pending_message.role !== "system"
      ) {
        throw new Error(`thread ${thread.thread_id} pending_message.role must be user or system`);
      }
      if (
        thread.pending_message.attachments !== undefined &&
        !Array.isArray(thread.pending_message.attachments)
      ) {
        throw new Error(`thread ${thread.thread_id} pending_message.attachments must be an array`);
      }
      if (
        thread.pending_message.runtime_mode !== undefined &&
        !RESUME_RUNTIME_MODES.has(thread.pending_message.runtime_mode as ResumeRuntimeMode)
      ) {
        throw new Error(
          `thread ${thread.thread_id} pending_message.runtime_mode must be a valid runtime mode`,
        );
      }
      if (
        thread.pending_message.interaction_mode !== undefined &&
        !RESUME_INTERACTION_MODES.has(
          thread.pending_message.interaction_mode as ResumeInteractionMode,
        )
      ) {
        throw new Error(
          `thread ${thread.thread_id} pending_message.interaction_mode must be a valid interaction mode`,
        );
      }
      if (
        thread.pending_message.model_selection !== undefined &&
        !isRecord(thread.pending_message.model_selection)
      ) {
        throw new Error(
          `thread ${thread.thread_id} pending_message.model_selection must be an object`,
        );
      }
      if (
        thread.pending_message.title_seed !== undefined &&
        typeof thread.pending_message.title_seed !== "string"
      ) {
        throw new Error(`thread ${thread.thread_id} pending_message.title_seed must be a string`);
      }
      if (
        thread.pending_message.source_proposed_plan !== undefined &&
        !isRecord(thread.pending_message.source_proposed_plan)
      ) {
        throw new Error(
          `thread ${thread.thread_id} pending_message.source_proposed_plan must be an object`,
        );
      }
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
  kind: "interaction-mode" | "queued-interaction-mode" | "command" | "queued-command" | "message",
  manifest: ResumeManifest,
  threadId: string,
): string {
  const digest = NodeCrypto.createHash("sha256")
    .update(`daily-restart-resume:${kind}:${manifest.version}:${manifest.captured_at}:${threadId}`)
    .digest("hex")
    .slice(0, 32);
  return `daily-restart-resume-${kind}-${digest}`;
}

interface PersistedImageAttachment {
  readonly type: "image";
  readonly id: string;
  readonly name: string;
  readonly mimeType: string;
  readonly sizeBytes: number;
}

function isPersistedImageAttachment(value: unknown): value is PersistedImageAttachment {
  return (
    isRecord(value) &&
    value.type === "image" &&
    typeof value.id === "string" &&
    value.id.length > 0 &&
    typeof value.name === "string" &&
    value.name.length > 0 &&
    typeof value.mimeType === "string" &&
    value.mimeType.toLowerCase().startsWith("image/") &&
    typeof value.sizeBytes === "number" &&
    Number.isInteger(value.sizeBytes) &&
    value.sizeBytes >= 0
  );
}

function inferImageExtension(input: {
  readonly mimeType: string;
  readonly fileName: string;
}): string {
  const fromMime = IMAGE_EXTENSION_BY_MIME_TYPE[input.mimeType.toLowerCase()];
  if (fromMime) return fromMime;

  const extensionMatch = /\.([a-z0-9]{1,8})$/i.exec(input.fileName.trim());
  const fromFileName = extensionMatch ? `.${extensionMatch[1]!.toLowerCase()}` : "";
  return ATTACHMENT_FILE_EXTENSIONS.includes(fromFileName) ? fromFileName : ".bin";
}

function safeAttachmentPaths(input: {
  readonly attachmentsDir: string;
  readonly attachment: PersistedImageAttachment;
}): ReadonlyArray<string> {
  if (!/^[a-z0-9_-]+$/i.test(input.attachment.id) || input.attachment.id.includes(".")) {
    return [];
  }

  const root = NodePath.resolve(input.attachmentsDir);
  const extensions = [
    inferImageExtension({ mimeType: input.attachment.mimeType, fileName: input.attachment.name }),
    ...ATTACHMENT_FILE_EXTENSIONS,
  ];
  const candidates: Array<string> = [];
  for (const extension of new Set(extensions)) {
    const candidate = NodePath.resolve(NodePath.join(root, `${input.attachment.id}${extension}`));
    if (candidate.startsWith(`${root}${NodePath.sep}`)) {
      candidates.push(candidate);
    }
  }
  return candidates;
}

async function readAttachmentUploadPayload(input: {
  readonly threadId: string;
  readonly attachmentsDir: string | undefined;
  readonly attachment: unknown;
}) {
  if (!isPersistedImageAttachment(input.attachment)) {
    throw new Error(`thread ${input.threadId} pending attachment must be a persisted image`);
  }
  if (!input.attachmentsDir) {
    throw new Error(
      `thread ${input.threadId} pending attachment replay requires --attachments-dir`,
    );
  }

  const attachmentPaths = safeAttachmentPaths({
    attachmentsDir: input.attachmentsDir,
    attachment: input.attachment,
  });
  if (attachmentPaths.length === 0) {
    throw new Error(`thread ${input.threadId} pending attachment path was not safe`);
  }

  let bytes: Buffer | null = null;
  let lastReadError: unknown;
  for (const attachmentPath of attachmentPaths) {
    try {
      bytes = await NodeFSP.readFile(attachmentPath);
      break;
    } catch (error) {
      lastReadError = error;
      if (!isRecord(error) || error.code !== "ENOENT") {
        throw new Error(`thread ${input.threadId} pending attachment could not be read`, {
          cause: error,
        });
      }
    }
  }
  if (bytes === null) {
    throw new Error(`thread ${input.threadId} pending attachment file was not found`, {
      cause: lastReadError,
    });
  }
  if (bytes.byteLength !== input.attachment.sizeBytes) {
    throw new Error(`thread ${input.threadId} pending attachment size did not match metadata`);
  }

  return {
    type: "image" as const,
    name: input.attachment.name,
    mimeType: input.attachment.mimeType,
    sizeBytes: input.attachment.sizeBytes,
    dataUrl: `data:${input.attachment.mimeType};base64,${bytes.toString("base64")}`,
  };
}

async function resumeMessageForThread(
  manifest: ResumeManifest,
  thread: ResumeManifestThread,
  attachmentsDir: string | undefined,
  usePendingMessage = true,
) {
  if (usePendingMessage && thread.pending_message !== undefined) {
    return {
      messageId: thread.pending_message.message_id,
      role: thread.pending_message.role ?? "user",
      text: thread.pending_message.text,
      attachments: await Promise.all(
        (thread.pending_message.attachments ?? []).map((attachment) =>
          readAttachmentUploadPayload({
            threadId: thread.thread_id,
            attachmentsDir,
            attachment,
          }),
        ),
      ),
    };
  }

  return {
    messageId: stableDispatchId("message", manifest, thread.thread_id),
    role: "user" as const,
    text: RESUME_MESSAGE,
    attachments: [],
  };
}

function pendingTurnMetadataForThread(thread: ResumeManifestThread) {
  const pendingMessage = thread.pending_message;
  if (pendingMessage === undefined) return {};

  return {
    ...(pendingMessage.model_selection !== undefined
      ? { modelSelection: pendingMessage.model_selection }
      : {}),
    ...(pendingMessage.title_seed !== undefined ? { titleSeed: pendingMessage.title_seed } : {}),
    ...(pendingMessage.source_proposed_plan !== undefined
      ? { sourceProposedPlan: pendingMessage.source_proposed_plan }
      : {}),
  };
}

function hasActiveTurn(thread: ResumeManifestThread): boolean {
  return typeof thread.active_turn_id === "string" && thread.active_turn_id.length > 0;
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

async function fetchOrchestrationSnapshot(
  fetchImpl: typeof fetch,
  origin: string,
  token: string,
  attemptTimeoutMs: number,
): Promise<unknown> {
  const controller = new AbortController();
  const timeout = NodeTimersPromises.setTimeout(attemptTimeoutMs, undefined, {
    signal: controller.signal,
  })
    .then(() => controller.abort())
    .catch(() => undefined);

  try {
    const response = await fetchImpl(new URL("/api/orchestration/snapshot", origin), {
      method: "GET",
      headers: { authorization: `Bearer ${token}` },
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

    return await response.json();
  } finally {
    controller.abort();
    await timeout;
  }
}

async function assertQueuedReplaySnapshotReadable(
  fetchImpl: typeof fetch,
  origin: string,
  token: string,
  attemptTimeoutMs: number,
  sleepImpl: (ms: number) => Promise<void>,
  random: () => number,
): Promise<void> {
  let remainingSleepBudgetMs = DISPATCH_RETRY_SLEEP_BUDGET_MS;
  let lastError: unknown;

  for (let attempt = 0; attempt < MAX_DISPATCH_ATTEMPTS; attempt += 1) {
    try {
      await fetchOrchestrationSnapshot(fetchImpl, origin, token, attemptTimeoutMs);
      return;
    } catch (error) {
      lastError = error;
      if (!isTransientDispatchError(error) || attempt === MAX_DISPATCH_ATTEMPTS - 1) break;
      const delayMs = clampDelayToBudget(
        dispatchRetryDelayMs(attempt, error, random),
        remainingSleepBudgetMs,
      );
      remainingSleepBudgetMs -= delayMs;
      await sleepImpl(delayMs);
    }
  }

  const message = lastError instanceof Error ? lastError.message : String(lastError);
  throw new Error(
    `queued replay requires orchestration snapshot read access before dispatch; snapshot preflight failed: ${message}`,
    { cause: lastError },
  );
}

async function ensureQueuedReplaySnapshotPreflight(input: {
  readonly alreadyPassed: boolean;
  readonly fetchImpl: typeof fetch;
  readonly origin: string;
  readonly token: string;
  readonly attemptTimeoutMs: number;
  readonly sleepImpl: (ms: number) => Promise<void>;
  readonly random: () => number;
}): Promise<boolean> {
  if (input.alreadyPassed) return true;
  await assertQueuedReplaySnapshotReadable(
    input.fetchImpl,
    input.origin,
    input.token,
    input.attemptTimeoutMs,
    input.sleepImpl,
    input.random,
  );
  return true;
}

function snapshotHasProjectedTurnStart(input: {
  readonly snapshot: unknown;
  readonly threadId: string;
  readonly messageId: string;
}): boolean {
  if (!isRecord(input.snapshot) || !Array.isArray(input.snapshot.threads)) return false;

  const thread = input.snapshot.threads.find(
    (candidate) => isRecord(candidate) && candidate.id === input.threadId,
  );
  if (!isRecord(thread) || !Array.isArray(thread.messages)) return false;

  return thread.messages.some(
    (message) =>
      isRecord(message) &&
      message.id === input.messageId &&
      typeof message.turnId === "string" &&
      message.turnId.length > 0,
  );
}

async function waitForProjectedTurnStart(input: {
  readonly fetchImpl: typeof fetch;
  readonly origin: string;
  readonly token: string;
  readonly threadId: string;
  readonly messageId: string;
  readonly attemptTimeoutMs: number;
  readonly timeoutMs: number;
  readonly pollMs: number;
  readonly sleepImpl: (ms: number) => Promise<void>;
}): Promise<void> {
  const deadlineMs = NodePerfHooks.performance.now() + input.timeoutMs;
  let lastTransientError: unknown;

  while (true) {
    const remainingMs = deadlineMs - NodePerfHooks.performance.now();
    if (remainingMs <= 0) break;

    try {
      const snapshot = await fetchOrchestrationSnapshot(
        input.fetchImpl,
        input.origin,
        input.token,
        Math.max(1, Math.min(input.attemptTimeoutMs, Math.ceil(remainingMs))),
      );
      if (
        snapshotHasProjectedTurnStart({
          snapshot,
          threadId: input.threadId,
          messageId: input.messageId,
        })
      ) {
        return;
      }
      lastTransientError = undefined;
    } catch (error) {
      if (!isTransientDispatchError(error)) throw error;
      lastTransientError = error;
    }

    const remainingAfterAttemptMs = deadlineMs - NodePerfHooks.performance.now();
    if (remainingAfterAttemptMs <= 0) break;
    const delayMs = Math.min(input.pollMs, remainingAfterAttemptMs);
    await input.sleepImpl(delayMs);
  }

  const suffix =
    lastTransientError instanceof Error
      ? `; last snapshot error: ${lastTransientError.message}`
      : "";
  throw new Error(
    `thread ${input.threadId} resume turn did not start before queued replay timeout${suffix}`,
  );
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
  thread: ResumeManifestThread,
  createdAt: string,
  attachmentsDir: string | undefined,
  attemptTimeoutMs: number,
  resumeStartTimeoutMs: number,
  resumeStartPollMs: number,
  sleepImpl: (ms: number) => Promise<void>,
  random: () => number,
): Promise<void> {
  const runtimeMode = thread.runtime_mode ?? DEFAULT_RESUME_RUNTIME_MODE;
  const interactionMode = thread.interaction_mode ?? DEFAULT_RESUME_INTERACTION_MODE;
  const pendingRuntimeMode = thread.pending_message?.runtime_mode ?? runtimeMode;
  const pendingInteractionMode = thread.pending_message?.interaction_mode ?? interactionMode;
  const hasQueuedPromptAfterActive = hasActiveTurn(thread) && thread.pending_message !== undefined;
  const dispatchInteractionMode = async (
    mode: ResumeInteractionMode,
    kind: "interaction-mode" | "queued-interaction-mode",
  ) => {
    await postDispatchCommandWithRetry(
      fetchImpl,
      origin,
      token,
      {
        type: "thread.interaction-mode.set",
        commandId: stableDispatchId(kind, manifest, thread.thread_id),
        threadId: thread.thread_id,
        interactionMode: mode,
        createdAt,
      },
      attemptTimeoutMs,
      sleepImpl,
      random,
    );
  };

  await dispatchInteractionMode(interactionMode, "interaction-mode");

  const dispatchTurnStart = async (input: {
    readonly commandId: string;
    readonly message: Awaited<ReturnType<typeof resumeMessageForThread>>;
    readonly metadata: Record<string, unknown>;
    readonly runtimeMode: ResumeRuntimeMode;
    readonly interactionMode: ResumeInteractionMode;
  }) => {
    await postDispatchCommandWithRetry(
      fetchImpl,
      origin,
      token,
      {
        type: "thread.turn.start",
        commandId: input.commandId,
        threadId: thread.thread_id,
        message: input.message,
        ...input.metadata,
        runtimeMode: input.runtimeMode,
        interactionMode: input.interactionMode,
        createdAt,
      },
      attemptTimeoutMs,
      sleepImpl,
      random,
    );
  };

  const resumeMessage = await resumeMessageForThread(
    manifest,
    thread,
    attachmentsDir,
    !hasQueuedPromptAfterActive,
  );
  await dispatchTurnStart({
    commandId: stableDispatchId("command", manifest, thread.thread_id),
    message: resumeMessage,
    metadata: hasQueuedPromptAfterActive ? {} : pendingTurnMetadataForThread(thread),
    runtimeMode: hasQueuedPromptAfterActive ? runtimeMode : pendingRuntimeMode,
    interactionMode: hasQueuedPromptAfterActive ? interactionMode : pendingInteractionMode,
  });

  if (hasQueuedPromptAfterActive) {
    await waitForProjectedTurnStart({
      fetchImpl,
      origin,
      token,
      threadId: thread.thread_id,
      messageId: resumeMessage.messageId,
      attemptTimeoutMs,
      timeoutMs: resumeStartTimeoutMs,
      pollMs: resumeStartPollMs,
      sleepImpl,
    });
    const queuedMessage = await resumeMessageForThread(manifest, thread, attachmentsDir);
    await dispatchTurnStart({
      commandId: stableDispatchId("queued-command", manifest, thread.thread_id),
      message: queuedMessage,
      metadata: pendingTurnMetadataForThread(thread),
      runtimeMode: pendingRuntimeMode,
      interactionMode: pendingInteractionMode,
    });
    if (pendingInteractionMode !== interactionMode) {
      await dispatchInteractionMode(pendingInteractionMode, "queued-interaction-mode");
    }
  }
}

export async function injectResume(options: InjectResumeOptions): Promise<InjectResumeResult> {
  const manifest = await readManifest(options.manifestPath);
  const fetchImpl = options.fetchImpl ?? fetch;
  const now = options.now ?? (() => new Date());
  const sleepImpl = options.sleep ?? sleep;
  const random = options.random ?? Math.random;
  const dispatchAttemptTimeoutMs = options.dispatchAttemptTimeoutMs ?? DISPATCH_ATTEMPT_TIMEOUT_MS;
  const resumeStartTimeoutMs = options.resumeStartTimeoutMs ?? RESUME_START_TIMEOUT_MS;
  const resumeStartPollMs = options.resumeStartPollMs ?? RESUME_START_POLL_MS;
  const failures: Array<{ threadId: string; error: string }> = [];
  let queuedReplaySnapshotPreflightPassed = false;
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
      if (hasActiveTurn(thread) && thread.pending_message !== undefined) {
        queuedReplaySnapshotPreflightPassed = await ensureQueuedReplaySnapshotPreflight({
          alreadyPassed: queuedReplaySnapshotPreflightPassed,
          fetchImpl,
          origin: options.origin,
          token,
          attemptTimeoutMs: dispatchAttemptTimeoutMs,
          sleepImpl,
          random,
        });
      }
      await dispatchResumeCommands(
        fetchImpl,
        options.origin,
        token,
        manifest,
        thread,
        injectedAt,
        options.attachmentsDir,
        dispatchAttemptTimeoutMs,
        resumeStartTimeoutMs,
        resumeStartPollMs,
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
    const result = await injectResume(optionsFromCliArgs(args));
    printSummary(result);
    process.exit(result.failed > 0 ? 1 : 0);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    console.error(usage());
    process.exit(2);
  }
}
