// @effect-diagnostics nodeBuiltinImport:off - Provider usage discovery reads CLI-owned files and binaries.
// @effect-diagnostics globalDate:off - Provider APIs exchange reset timestamps as Unix/ISO dates.
// @effect-diagnostics globalFetch:off - The Codex usage boundary calls provider OAuth usage endpoints directly.
import type {
  PlanUsageProvider,
  PlanUsageSnapshot,
  PlanUsageWindow,
  ServerSettings,
} from "@t3tools/contracts";
import {
  ProviderInstanceId,
  type ProviderInstanceId as ProviderInstanceIdType,
} from "@t3tools/contracts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { expandHomePath } from "../pathExpansion.ts";

const CODEX_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage";
const CODEX_REFRESH_URL = "https://auth.openai.com/oauth/token";
const CODEX_OAUTH_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann";
const CLAUDE_CLI_USAGE_ARGS = [
  "--safe-mode",
  "--setting-sources",
  "user",
  "--no-session-persistence",
  "-p",
  "--output-format",
  "json",
  "/usage",
] as const;
const CLAUDE_CLI_AUTH_STATUS_ARGS = [
  "--safe-mode",
  "--setting-sources",
  "user",
  "auth",
  "status",
  "--json",
] as const;
const CACHE_TTL_MS = 60_000;
const MAX_CACHE_ENTRIES = 32;
const UPSTREAM_TIMEOUT_MS = 10_000;
const CLI_MAX_BUFFER_BYTES = 1024 * 1024;
const CLAUDE_SAFE_TRANSPORT_ENV_KEYS = new Set([
  "CLAUDE_CODE_CERT_STORE",
  "CLAUDE_CODE_CLIENT_CERT",
  "CLAUDE_CODE_CLIENT_KEY",
  "CLAUDE_CODE_CLIENT_KEY_PASSPHRASE",
  "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC",
]);

const execFile = NodeUtil.promisify(NodeChildProcess.execFile);

const cached = new Map<
  string,
  { readonly expiresAt: number; readonly snapshot: PlanUsageSnapshot }
>();

interface ProviderUsage {
  readonly provider: PlanUsageProvider;
  readonly plan: string | null;
  readonly windows: ReadonlyArray<PlanUsageWindow>;
}

interface UsageCredentialSource {
  readonly provider: PlanUsageProvider;
  readonly instanceId: ProviderInstanceIdType;
  readonly home: string;
  readonly executable: string | null;
}

interface UsageCredentialScope {
  readonly cacheKey: string;
  readonly sources: ReadonlyArray<UsageCredentialSource>;
}

interface LoadPlanUsageOptions {
  readonly settings?: ServerSettings | undefined;
  readonly providerInstanceId?: ProviderInstanceIdType | null | undefined;
}

interface UsageProviderInstance {
  readonly instanceId: ProviderInstanceIdType;
  readonly driver: string;
  readonly enabled?: boolean | undefined;
  readonly config?: unknown;
}

function numberValue(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function stringValue(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

function objectValue(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function configEnabled(value: unknown): boolean | undefined {
  const enabled = objectValue(value)?.enabled;
  return typeof enabled === "boolean" ? enabled : undefined;
}

function clampPercent(value: number): number {
  return Math.max(0, Math.min(100, value));
}

function unixSecondsToIso(value: unknown): string | null {
  const seconds = numberValue(value);
  if (seconds === null || seconds <= 0) return null;
  return new Date(seconds * 1000).toISOString();
}

function isoDateValue(value: unknown): string | null {
  const raw = stringValue(value);
  if (!raw) return null;
  const time = Date.parse(raw);
  return Number.isFinite(time) ? new Date(time).toISOString() : null;
}

function truthyEnvFlag(value: string | undefined): boolean {
  if (!value) return false;
  return /^(1|true|yes|on)$/i.test(value.trim());
}

function falseyEnvFlag(value: string | undefined): boolean {
  if (!value) return false;
  return /^(0|false|no|off)$/i.test(value.trim());
}

function isPlanUsagePollingDisabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return (
    truthyEnvFlag(env.T3_DISABLE_PLAN_USAGE_POLLING) || falseyEnvFlag(env.T3_PLAN_USAGE_POLLING)
  );
}

const MONTHS: Readonly<Record<string, number>> = {
  jan: 0,
  january: 0,
  feb: 1,
  february: 1,
  mar: 2,
  march: 2,
  apr: 3,
  april: 3,
  may: 4,
  jun: 5,
  june: 5,
  jul: 6,
  july: 6,
  aug: 7,
  august: 7,
  sep: 8,
  sept: 8,
  september: 8,
  oct: 9,
  october: 9,
  nov: 10,
  november: 10,
  dec: 11,
  december: 11,
};

function timeZoneDateParts(
  timeZone: string,
  instant: Date,
): {
  readonly year: number;
  readonly month: number;
  readonly day: number;
  readonly hour: number;
  readonly minute: number;
  readonly second: number;
} | null {
  try {
    const formatter = new Intl.DateTimeFormat("en-US", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23",
    });
    const parts = Object.fromEntries(
      formatter.formatToParts(instant).map((part) => [part.type, part.value]),
    );
    const year = Number(parts.year);
    const month = Number(parts.month);
    const day = Number(parts.day);
    const hour = Number(parts.hour);
    const minute = Number(parts.minute);
    const second = Number(parts.second);
    if (![year, month, day, hour, minute, second].every(Number.isFinite)) return null;
    return { year, month, day, hour, minute, second };
  } catch {
    return null;
  }
}

function timeZoneOffsetMs(timeZone: string, utcMillis: number): number | null {
  const parts = timeZoneDateParts(timeZone, new Date(utcMillis));
  if (!parts) return null;
  const asUtc = Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    parts.hour,
    parts.minute,
    parts.second,
  );
  return asUtc - utcMillis;
}

function localTimeInZoneToUtcMillis(input: {
  readonly year: number;
  readonly month: number;
  readonly day: number;
  readonly hour: number;
  readonly minute: number;
  readonly timeZone: string;
}): number | null {
  const localAsUtc = Date.UTC(input.year, input.month, input.day, input.hour, input.minute, 0);
  const firstOffset = timeZoneOffsetMs(input.timeZone, localAsUtc);
  if (firstOffset === null) return null;
  let utcMillis = localAsUtc - firstOffset;
  const secondOffset = timeZoneOffsetMs(input.timeZone, utcMillis);
  if (secondOffset !== null && secondOffset !== firstOffset) {
    utcMillis = localAsUtc - secondOffset;
  }
  return utcMillis;
}

function parseClaudeCliResetAt(raw: string | null, now = Date.now()): string | null {
  if (!raw) return null;
  const match = raw
    .trim()
    .match(/^([A-Za-z]+)\s+(\d{1,2}),\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?(?:\s+\(([^)]+)\))?$/i);
  if (!match) return null;
  const month = MONTHS[match[1]?.toLowerCase() ?? ""];
  if (month === undefined) return null;
  const day = Number(match[2]);
  let hour = Number(match[3]);
  const minute = match[4] === undefined ? 0 : Number(match[4]);
  const meridiem = match[5]?.toLowerCase();
  const timeZone = match[6]?.trim() || Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  if (!Number.isFinite(day) || !Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  if (meridiem === "pm" && hour < 12) hour += 12;
  if (meridiem === "am" && hour === 12) hour = 0;

  const currentYear =
    timeZoneDateParts(timeZone, new Date(now))?.year ?? new Date(now).getUTCFullYear();
  let utcMillis = localTimeInZoneToUtcMillis({
    year: currentYear,
    month,
    day,
    hour,
    minute,
    timeZone,
  });
  if (utcMillis === null) return null;
  if (utcMillis < now - 60 * 60 * 1000) {
    utcMillis =
      localTimeInZoneToUtcMillis({
        year: currentYear + 1,
        month,
        day,
        hour,
        minute,
        timeZone,
      }) ?? utcMillis;
  }
  return new Date(utcMillis).toISOString();
}

function claudeScopeTitle(scope: string): string {
  return scope
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
    .join(" ");
}

function claudeCliGenericKind(title: string, index: number): string {
  const slug = title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return slug || `limit_${index}`;
}

function claudeCliUsageWindow(input: {
  readonly kind: string;
  readonly title: string;
  readonly percent: number;
  readonly resetRaw: string | null;
  readonly index: number;
  readonly now: number;
}): PlanUsageWindow {
  return {
    id: `claude-${input.kind}-${input.index}`,
    provider: "claude",
    kind: input.kind,
    title: input.title,
    usedPercent: clampPercent(input.percent),
    resetAt: parseClaudeCliResetAt(input.resetRaw, input.now),
    used: null,
    limit: null,
    unit: null,
    severity: null,
  };
}

function parseClaudeCliResetLine(line: string): string | null {
  const match = line.trim().match(/^(?:[·•-]\s*)?reset(?:s)?(?:\s+(?:at|time))?:?\s+(.+)$/i);
  return match?.[1]?.trim() || null;
}

function parseClaudeCliUsageText(
  text: string,
  plan: string | null,
  now = Date.now(),
): ProviderUsage | null {
  const windows: PlanUsageWindow[] = [];
  let pendingResetWindowIndex: number | null = null;
  const applyPendingReset = (line: string): boolean => {
    if (pendingResetWindowIndex === null) return false;
    const resetRaw = parseClaudeCliResetLine(line);
    if (!resetRaw) return false;
    const pendingWindow = windows[pendingResetWindowIndex];
    if (pendingWindow) {
      windows[pendingResetWindowIndex] = {
        ...pendingWindow,
        resetAt: parseClaudeCliResetAt(resetRaw, now),
      };
    }
    pendingResetWindowIndex = null;
    return true;
  };
  const pushWindow = (input: {
    readonly kind: string;
    readonly title: string;
    readonly percent: number;
    readonly resetRaw: string | null;
  }): void => {
    const index = windows.length;
    windows.push(claudeCliUsageWindow({ ...input, index, now }));
    pendingResetWindowIndex = input.resetRaw ? null : index;
  };

  for (const line of text.split(/\r?\n/)) {
    if (applyPendingReset(line)) continue;
    if (pendingResetWindowIndex !== null && line.trim().length === 0) continue;
    pendingResetWindowIndex = null;

    const currentMatch = line.match(
      /^Current\s+(session|week)(?:\s+\(([^)]+)\))?:\s+([0-9]+(?:\.[0-9]+)?)%\s+used(?:\s+.*?\bresets\s+(.+))?$/i,
    );
    if (currentMatch) {
      const scope = currentMatch[2]?.trim() ?? null;
      const percent = Number(currentMatch[3]);
      if (!Number.isFinite(percent)) continue;
      const isSession = currentMatch[1]?.toLowerCase() === "session";
      const isAllModels = scope?.toLowerCase() === "all models";
      const kind = isSession ? "session" : isAllModels ? "weekly_all" : "weekly_scoped";
      const title = isSession
        ? "Claude Session"
        : isAllModels || !scope
          ? "Claude Weekly All"
          : `Claude Weekly Scoped ${claudeScopeTitle(scope)}`;
      pushWindow({
        kind,
        title,
        percent,
        resetRaw: currentMatch[4] ?? null,
      });
      continue;
    }

    const genericMatch = line.match(
      /^([^:]+):\s+([0-9]+(?:\.[0-9]+)?)%\s+used(?:\s+.*?\bresets\s+(.+))?$/i,
    );
    if (!genericMatch) continue;
    const title = genericMatch[1]?.trim();
    const percent = Number(genericMatch[2]);
    if (!title || !Number.isFinite(percent)) continue;
    pushWindow({
      kind: claudeCliGenericKind(title, windows.length),
      title,
      percent,
      resetRaw: genericMatch[3] ?? null,
    });
  }

  return windows.length > 0 ? { provider: "claude", plan, windows } : null;
}

function codexWindow(input: {
  readonly id: string;
  readonly kind: PlanUsageWindow["kind"];
  readonly title: string;
  readonly raw: unknown;
}): PlanUsageWindow | null {
  const raw = objectValue(input.raw);
  const usedPercent = numberValue(raw?.used_percent);
  if (usedPercent === null) return null;
  return {
    id: input.id,
    provider: "codex",
    kind: input.kind,
    title: input.title,
    usedPercent: clampPercent(usedPercent),
    resetAt: unixSecondsToIso(raw?.reset_at),
    used: null,
    limit: null,
    unit: null,
    severity: null,
  };
}

function parseCodexUsageResponse(payload: unknown): ProviderUsage | null {
  const root = objectValue(payload);
  const rateLimit = objectValue(root?.rate_limit);
  if (!root || !rateLimit) return null;

  const windows = [
    codexWindow({
      id: "codex-five-hour",
      kind: "five_hour",
      title: "Codex 5h",
      raw: rateLimit.primary_window,
    }),
    codexWindow({
      id: "codex-weekly",
      kind: "weekly",
      title: "Codex weekly",
      raw: rateLimit.secondary_window,
    }),
  ].filter((window): window is PlanUsageWindow => window !== null);

  if (windows.length === 0) return null;
  return {
    provider: "codex",
    plan: stringValue(root.plan_type),
    windows,
  };
}

function titleCaseKind(kind: string): string {
  return kind
    .split("_")
    .filter((part) => part.length > 0)
    .map((part) => part[0]?.toUpperCase() + part.slice(1))
    .join(" ");
}

function normalizedSeverity(value: unknown): PlanUsageWindow["severity"] {
  const severity = stringValue(value)?.toLowerCase();
  return severity === "normal" ||
    severity === "info" ||
    severity === "warning" ||
    severity === "critical"
    ? severity
    : null;
}

function claudeLimitTitle(kind: string, limit: Record<string, unknown>): string {
  const scope = objectValue(limit.scope);
  const model = objectValue(scope?.model);
  const modelName = stringValue(model?.display_name);
  const base = `Claude ${titleCaseKind(kind)}`;
  return modelName ? `${base} ${modelName}` : base;
}

function claudeLimitWindow(limit: unknown, index: number): PlanUsageWindow | null {
  const raw = objectValue(limit);
  if (!raw) return null;
  const kind = stringValue(raw.kind);
  const percent = numberValue(raw.percent);
  if (!kind || percent === null) return null;
  return {
    id: `claude-${kind}-${index}`,
    provider: "claude",
    kind,
    title: claudeLimitTitle(kind, raw),
    usedPercent: clampPercent(percent),
    resetAt: isoDateValue(raw.resets_at),
    used: numberValue(raw.used),
    limit: numberValue(raw.limit),
    unit: stringValue(raw.unit),
    severity: normalizedSeverity(raw.severity),
  };
}

function parseClaudeUsageResponse(payload: unknown, plan: string | null): ProviderUsage | null {
  const root = objectValue(payload);
  if (!root) return null;
  const limits = Array.isArray(root.limits) ? root.limits : [];
  const windows = limits
    .map((limit, index) => claudeLimitWindow(limit, index))
    .filter((window): window is PlanUsageWindow => window !== null);

  if (windows.length === 0) return null;
  return {
    provider: "claude",
    plan,
    windows,
  };
}

async function readJsonFile(path: string): Promise<Record<string, unknown> | null> {
  try {
    return JSON.parse(await NodeFSP.readFile(path, "utf8")) as Record<string, unknown>;
  } catch {
    return null;
  }
}

async function writeJsonFilePrivate(
  path: string,
  value: Record<string, unknown>,
): Promise<boolean> {
  const tempPath = `${path}.${process.pid}.${Date.now()}.tmp`;
  try {
    await NodeFSP.writeFile(tempPath, `${JSON.stringify(value, null, 2)}\n`, {
      encoding: "utf8",
      mode: 0o600,
    });
    await NodeFSP.rename(tempPath, path);
    await NodeFSP.chmod(path, 0o600).catch(() => undefined);
    return true;
  } catch {
    await NodeFSP.rm(tempPath, { force: true }).catch(() => undefined);
    return false;
  }
}

async function fetchJson(
  url: string,
  headers: Record<string, string>,
  options?: {
    readonly method?: "GET" | "POST";
    readonly body?: string;
  },
): Promise<unknown | null> {
  try {
    const response = await fetch(url, {
      headers,
      method: options?.method ?? "GET",
      body: options?.body,
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
    });
    if (!response.ok) return null;
    return await response.json();
  } catch {
    return null;
  }
}

async function fetchJsonStatus(
  url: string,
  headers: Record<string, string>,
  options?: {
    readonly method?: "GET" | "POST";
    readonly body?: string;
  },
): Promise<{ readonly status: number; readonly payload: unknown | null }> {
  try {
    const response = await fetch(url, {
      headers,
      method: options?.method ?? "GET",
      body: options?.body,
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
    });
    if (!response.ok) return { status: response.status, payload: null };
    return { status: response.status, payload: await response.json() };
  } catch {
    return { status: 0, payload: null };
  }
}

function defaultCodexHome(): string {
  return process.env.CODEX_HOME ?? NodePath.join(process.env.HOME ?? "", ".codex");
}

function defaultClaudeHome(): string {
  return process.env.HOME ?? "";
}

function configuredPathOrNull(value: unknown): string | null {
  const raw = stringValue(value)?.trim();
  return raw ? NodePath.resolve(expandHomePath(raw)) : null;
}

function configuredCommandOrNull(value: unknown): string | null {
  const raw = stringValue(value)?.trim();
  if (!raw) return null;
  return raw.startsWith("~") ? NodePath.resolve(expandHomePath(raw)) : raw;
}

function legacyDefaultUsageProviderInstance(
  settings: ServerSettings,
  providerInstanceId: ProviderInstanceIdType,
): UsageProviderInstance | null {
  if (providerInstanceId === "codex") {
    return {
      instanceId: providerInstanceId,
      driver: "codex",
      enabled: settings.providers.codex.enabled,
      config: settings.providers.codex,
    };
  }

  if (providerInstanceId === "claudeAgent") {
    return {
      instanceId: providerInstanceId,
      driver: "claudeAgent",
      enabled: settings.providers.claudeAgent.enabled,
      config: settings.providers.claudeAgent,
    };
  }

  return null;
}

function isUsageDriverForProvider(driver: string, provider: PlanUsageProvider): boolean {
  return provider === "codex"
    ? driver === "codex"
    : driver === "claudeAgent" || driver === "claude";
}

function configuredUsageProviderInstances(
  settings: ServerSettings,
  provider: PlanUsageProvider,
): ReadonlyArray<UsageProviderInstance> {
  const defaultInstanceId = (
    provider === "codex" ? "codex" : "claudeAgent"
  ) as ProviderInstanceIdType;
  const explicitInstances: UsageProviderInstance[] = Object.entries(settings.providerInstances).map(
    ([instanceId, instance]) => ({
      instanceId: ProviderInstanceId.make(instanceId),
      ...instance,
    }),
  );
  const explicitCandidates = explicitInstances.filter((instance) =>
    isUsageDriverForProvider(instance.driver, provider),
  );
  if (!explicitInstances.some((instance) => instance.instanceId === defaultInstanceId)) {
    const legacyDefault = legacyDefaultUsageProviderInstance(settings, defaultInstanceId);
    if (legacyDefault && isUsageDriverForProvider(legacyDefault.driver, provider)) {
      explicitCandidates.push(legacyDefault);
    }
  }

  return explicitCandidates.filter(
    (instance) => (instance.enabled ?? configEnabled(instance.config) ?? true) !== false,
  );
}

function configuredHomeForProvider(
  provider: PlanUsageProvider,
  instance: UsageProviderInstance | null,
): string {
  const config = objectValue(instance?.config);
  if (provider === "codex") {
    return (
      configuredPathOrNull(config?.shadowHomePath) ??
      configuredPathOrNull(config?.homePath) ??
      defaultCodexHome()
    );
  }
  return configuredPathOrNull(config?.homePath) ?? defaultClaudeHome();
}

function usageSourceForInstance(
  provider: PlanUsageProvider,
  instance: UsageProviderInstance,
): UsageCredentialSource {
  const config = objectValue(instance.config);
  return {
    provider,
    instanceId: instance.instanceId,
    home: configuredHomeForProvider(provider, instance),
    executable: provider === "claude" ? configuredCommandOrNull(config?.binaryPath) : null,
  };
}

function uniqueUsageSources(
  sources: ReadonlyArray<UsageCredentialSource>,
): ReadonlyArray<UsageCredentialSource> {
  const seen = new Set<string>();
  return sources.filter((source) => {
    const key = `${source.provider}:${source.home}:${source.executable ?? ""}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function usageScopeCacheKey(sources: ReadonlyArray<UsageCredentialSource>): string {
  return `sources:${JSON.stringify(
    sources.map((source) => [source.provider, source.instanceId, source.home, source.executable]),
  )}`;
}

export function resolveUsageCredentialScope(
  options: LoadPlanUsageOptions = {},
): UsageCredentialScope {
  const providerInstanceId = options.providerInstanceId;
  const settings = options.settings;
  if (providerInstanceId && settings) {
    const instance =
      settings.providerInstances[providerInstanceId] ??
      legacyDefaultUsageProviderInstance(settings, providerInstanceId);
    if (!instance || (instance.enabled ?? configEnabled(instance.config) ?? true) === false) {
      return {
        cacheKey: `instance:${providerInstanceId}:disabled-or-missing`,
        sources: [],
      };
    }

    if (instance.driver === "codex") {
      const source = usageSourceForInstance("codex", {
        instanceId: providerInstanceId,
        ...instance,
      });
      return {
        cacheKey: usageScopeCacheKey([source]),
        sources: [source],
      };
    }

    if (instance.driver === "claudeAgent" || instance.driver === "claude") {
      const source = usageSourceForInstance("claude", {
        instanceId: providerInstanceId,
        ...instance,
      });
      return {
        cacheKey: usageScopeCacheKey([source]),
        sources: [source],
      };
    }

    return {
      cacheKey: `instance:${providerInstanceId}:${instance.driver}`,
      sources: [],
    };
  }

  if (settings) {
    const sources = uniqueUsageSources([
      ...configuredUsageProviderInstances(settings, "codex").map((instance) =>
        usageSourceForInstance("codex", instance),
      ),
      ...configuredUsageProviderInstances(settings, "claude").map((instance) =>
        usageSourceForInstance("claude", instance),
      ),
    ]);
    return {
      cacheKey: usageScopeCacheKey(sources),
      sources,
    };
  }

  const sources = [
    {
      provider: "codex",
      instanceId: ProviderInstanceId.make("codex"),
      home: defaultCodexHome(),
      executable: null,
    },
    {
      provider: "claude",
      instanceId: ProviderInstanceId.make("claudeAgent"),
      home: defaultClaudeHome(),
      executable: null,
    },
  ] satisfies ReadonlyArray<UsageCredentialSource>;
  return {
    cacheKey: usageScopeCacheKey(sources),
    sources,
  };
}

interface CodexAuth {
  readonly authPath: string;
  readonly auth: Record<string, unknown>;
  readonly tokens: Record<string, unknown>;
  readonly accessToken: string;
  readonly refreshToken: string | null;
  readonly accountId: string | null;
}

function parseCodexAuthFile(
  authPath: string,
  auth: Record<string, unknown> | null,
): CodexAuth | null {
  if (!auth) return null;
  const tokens = objectValue(auth?.tokens);
  if (!tokens) return null;
  const accessToken = stringValue(tokens?.access_token) ?? stringValue(tokens?.accessToken);
  if (!accessToken) return null;
  const accountId = stringValue(tokens?.account_id) ?? stringValue(tokens?.accountId);
  const refreshToken = stringValue(tokens?.refresh_token) ?? stringValue(tokens?.refreshToken);
  return {
    authPath,
    auth,
    tokens,
    accessToken,
    refreshToken,
    accountId,
  };
}

async function readCodexAuth(codexHome: string): Promise<CodexAuth | null> {
  const authPath = NodePath.join(codexHome, "auth.json");
  return parseCodexAuthFile(authPath, await readJsonFile(authPath));
}

async function refreshCodexAuth(auth: CodexAuth): Promise<CodexAuth | null> {
  if (!auth.refreshToken) return null;
  const payload = await fetchJson(
    CODEX_REFRESH_URL,
    {
      Accept: "application/json",
      "Content-Type": "application/json",
      "User-Agent": "T3Code",
    },
    {
      method: "POST",
      body: JSON.stringify({
        client_id: CODEX_OAUTH_CLIENT_ID,
        grant_type: "refresh_token",
        refresh_token: auth.refreshToken,
        scope: "openid profile email",
      }),
    },
  );
  const response = objectValue(payload);
  if (!response) return null;

  const accessToken = stringValue(response.access_token);
  if (!accessToken) return null;
  const refreshToken = stringValue(response.refresh_token) ?? auth.refreshToken;
  const nextTokens: Record<string, unknown> = {
    ...auth.tokens,
    access_token: accessToken,
    refresh_token: refreshToken,
  };
  const idToken = stringValue(response.id_token);
  if (idToken) nextTokens.id_token = idToken;
  const expiresIn = numberValue(response.expires_in);
  if (expiresIn !== null) {
    nextTokens.expires_at = Math.floor(Date.now() / 1000) + expiresIn;
  }
  const currentAuth = parseCodexAuthFile(auth.authPath, await readJsonFile(auth.authPath));
  if (!currentAuth || currentAuth.accountId !== auth.accountId) return null;
  if (currentAuth.refreshToken !== auth.refreshToken) return currentAuth;

  const nextAuth = { ...auth.auth, tokens: nextTokens };
  const wrote = await writeJsonFilePrivate(auth.authPath, nextAuth);
  if (!wrote) return null;
  return {
    ...auth,
    auth: nextAuth,
    tokens: nextTokens,
    accessToken,
    refreshToken,
  };
}

const codexUsageHeaders = (auth: Pick<CodexAuth, "accessToken" | "accountId">) => ({
  Authorization: `Bearer ${auth.accessToken}`,
  Accept: "application/json",
  "User-Agent": "T3Code",
  ...(auth.accountId ? { "ChatGPT-Account-Id": auth.accountId } : {}),
});

async function loadCodexUsage(codexHome: string): Promise<ProviderUsage | null> {
  const auth = await readCodexAuth(codexHome);
  if (!auth) return null;
  let response = await fetchJsonStatus(CODEX_USAGE_URL, codexUsageHeaders(auth));
  if (response.status === 401) {
    const refreshed = await refreshCodexAuth(auth);
    if (!refreshed) return null;
    response = await fetchJsonStatus(CODEX_USAGE_URL, codexUsageHeaders(refreshed));
  }
  return parseCodexUsageResponse(response.payload);
}

function isClaudeAuthOverrideEnvKey(key: string): boolean {
  const normalized = key.toUpperCase();
  if (CLAUDE_SAFE_TRANSPORT_ENV_KEYS.has(normalized)) return false;
  return normalized.startsWith("ANTHROPIC_") || normalized.startsWith("CLAUDE_CODE_");
}

function claudeCliEnvironment(
  home: string,
  baseEnv: NodeJS.ProcessEnv = process.env,
): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = { ...baseEnv, HOME: home };
  for (const key of Object.keys(env)) {
    if (isClaudeAuthOverrideEnvKey(key)) {
      delete env[key];
    }
  }
  env.HOME = home;
  return env;
}

async function runClaudeCliJson(
  source: UsageCredentialSource,
  args: ReadonlyArray<string>,
): Promise<unknown | null> {
  const env = claudeCliEnvironment(source.home);
  try {
    const spawnCommand = await Effect.runPromise(
      resolveSpawnCommand(source.executable ?? "claude", args, { env }),
    );
    const { stdout } = await execFile(spawnCommand.command, spawnCommand.args, {
      cwd: source.home || undefined,
      env,
      encoding: "utf8",
      maxBuffer: CLI_MAX_BUFFER_BYTES,
      shell: spawnCommand.shell,
      timeout: UPSTREAM_TIMEOUT_MS,
    });
    return JSON.parse(stdout);
  } catch {
    return null;
  }
}

function parseClaudeCliResultText(payload: unknown): string | null {
  const root = objectValue(payload);
  if (!root || root.is_error === true) return null;
  return stringValue(root.result);
}

function parseClaudeAuthStatusPlan(payload: unknown): string | null {
  const root = objectValue(payload);
  if (!root || root.loggedIn === false) return null;
  const subscriptionType =
    stringValue(root.subscriptionType) ?? stringValue(root.subscription_type);
  const rateLimitTier = stringValue(root.rateLimitTier) ?? stringValue(root.rate_limit_tier);
  return [subscriptionType, rateLimitTier].filter(Boolean).join(" ") || null;
}

async function loadClaudeUsage(source: UsageCredentialSource): Promise<ProviderUsage | null> {
  const [usagePayload, authStatusPayload] = await Promise.all([
    runClaudeCliJson(source, CLAUDE_CLI_USAGE_ARGS),
    runClaudeCliJson(source, CLAUDE_CLI_AUTH_STATUS_ARGS),
  ]);
  const usageText = parseClaudeCliResultText(usagePayload);
  if (!usageText) return null;
  return parseClaudeCliUsageText(usageText, parseClaudeAuthStatusPlan(authStatusPayload));
}

async function loadProviderUsage(
  scope: UsageCredentialScope,
): Promise<ReadonlyArray<ProviderUsage>> {
  const sourceCounts = new Map<PlanUsageProvider, number>();
  for (const source of scope.sources) {
    sourceCounts.set(source.provider, (sourceCounts.get(source.provider) ?? 0) + 1);
  }
  const tasks = scope.sources.map(async (source): Promise<ProviderUsage | null> => {
    const usage =
      source.provider === "codex"
        ? await loadCodexUsage(source.home)
        : await loadClaudeUsage(source);
    if (!usage) return null;
    const includeInstanceLabel = (sourceCounts.get(source.provider) ?? 0) > 1;
    return {
      ...usage,
      windows: usage.windows.map((window) => ({
        ...window,
        id: `${source.provider}:${source.instanceId}:${window.id}`,
        title: includeInstanceLabel ? `${window.title} (${source.instanceId})` : window.title,
      })),
    };
  });
  return (await Promise.all(tasks)).filter(
    (provider): provider is ProviderUsage => provider !== null && provider.windows.length > 0,
  );
}

function pruneUsageCache(now: number): void {
  for (const [key, value] of cached) {
    if (value.expiresAt <= now) {
      cached.delete(key);
    }
  }

  while (cached.size >= MAX_CACHE_ENTRIES) {
    const oldestKey = cached.keys().next().value as string | undefined;
    if (!oldestKey) return;
    cached.delete(oldestKey);
  }
}

export async function loadPlanUsageSnapshot(
  options: LoadPlanUsageOptions = {},
  now = Date.now(),
): Promise<PlanUsageSnapshot> {
  if (isPlanUsagePollingDisabled()) {
    return {
      updatedAt: new Date(now).toISOString(),
      providers: [],
    };
  }

  const scope = resolveUsageCredentialScope(options);
  const existing = cached.get(scope.cacheKey);
  if (existing && existing.expiresAt > now) {
    return existing.snapshot;
  }

  const providers = await loadProviderUsage(scope);
  const snapshot: PlanUsageSnapshot = {
    updatedAt: new Date(now).toISOString(),
    providers: providers.map((provider) => ({
      provider: provider.provider,
      plan: provider.plan,
      windows: [...provider.windows],
    })),
  };
  if (providers.length > 0) {
    pruneUsageCache(now);
    cached.set(scope.cacheKey, { expiresAt: now + CACHE_TTL_MS, snapshot });
  }
  return snapshot;
}

export const __testing = {
  CLAUDE_CLI_USAGE_ARGS,
  parseCodexUsageResponse,
  parseClaudeUsageResponse,
  parseClaudeCliUsageText,
  parseClaudeCliResetAt,
  claudeCliEnvironment,
  claudeLimitWindow,
  isPlanUsagePollingDisabled,
  resolveUsageCredentialScope,
};
