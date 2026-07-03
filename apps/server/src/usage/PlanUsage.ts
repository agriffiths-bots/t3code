// @effect-diagnostics nodeBuiltinImport:off - Provider credential discovery reads CLI-owned files.
// @effect-diagnostics globalDate:off - Provider APIs exchange reset timestamps as Unix/ISO dates.
// @effect-diagnostics globalFetch:off - This boundary calls provider OAuth usage endpoints directly.
import type {
  PlanUsageProvider,
  PlanUsageSnapshot,
  PlanUsageWindow,
  ProviderInstanceId,
  ServerSettings,
} from "@t3tools/contracts";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { expandHomePath } from "../pathExpansion.ts";

const CODEX_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage";
const CODEX_REFRESH_URL = "https://auth.openai.com/oauth/token";
const CODEX_OAUTH_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann";
const CLAUDE_USAGE_URL = "https://api.anthropic.com/api/oauth/usage";
const CLAUDE_BETA_HEADER = "oauth-2025-04-20";
const CACHE_TTL_MS = 60_000;
const MAX_CACHE_ENTRIES = 32;
const UPSTREAM_TIMEOUT_MS = 10_000;

const cached = new Map<
  string,
  { readonly expiresAt: number; readonly snapshot: PlanUsageSnapshot }
>();

interface ProviderUsage {
  readonly provider: PlanUsageProvider;
  readonly plan: string | null;
  readonly windows: ReadonlyArray<PlanUsageWindow>;
}

interface UsageCredentialScope {
  readonly cacheKey: string;
  readonly providers: ReadonlyArray<PlanUsageProvider>;
  readonly codexHome: string | null;
  readonly claudeHome: string | null;
}

interface LoadPlanUsageOptions {
  readonly settings?: ServerSettings | undefined;
  readonly providerInstanceId?: ProviderInstanceId | null | undefined;
}

interface UsageProviderInstance {
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

function legacyDefaultUsageProviderInstance(
  settings: ServerSettings,
  providerInstanceId: ProviderInstanceId,
): UsageProviderInstance | null {
  if (providerInstanceId === "codex") {
    return {
      driver: "codex",
      enabled: settings.providers.codex.enabled,
      config: settings.providers.codex,
    };
  }

  if (providerInstanceId === "claudeAgent") {
    return {
      driver: "claudeAgent",
      enabled: settings.providers.claudeAgent.enabled,
      config: settings.providers.claudeAgent,
    };
  }

  return null;
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
    if (!instance || instance.enabled === false) {
      return {
        cacheKey: `instance:${providerInstanceId}:disabled-or-missing`,
        providers: [],
        codexHome: null,
        claudeHome: null,
      };
    }

    const config = objectValue(instance.config);
    if (instance.driver === "codex") {
      const codexHome =
        configuredPathOrNull(config?.shadowHomePath) ??
        configuredPathOrNull(config?.homePath) ??
        defaultCodexHome();
      return {
        cacheKey: `instance:${providerInstanceId}:codex:${codexHome}`,
        providers: ["codex"],
        codexHome,
        claudeHome: null,
      };
    }

    if (instance.driver === "claudeAgent" || instance.driver === "claude") {
      const claudeHome = configuredPathOrNull(config?.homePath) ?? defaultClaudeHome();
      return {
        cacheKey: `instance:${providerInstanceId}:claude:${claudeHome}`,
        providers: ["claude"],
        codexHome: null,
        claudeHome,
      };
    }

    return {
      cacheKey: `instance:${providerInstanceId}:${instance.driver}`,
      providers: [],
      codexHome: null,
      claudeHome: null,
    };
  }

  const codexHome = defaultCodexHome();
  const claudeHome = defaultClaudeHome();
  return {
    cacheKey: `default:${codexHome}:${claudeHome}`,
    providers: ["codex", "claude"],
    codexHome,
    claudeHome,
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

async function loadClaudeUsage(claudeHome: string): Promise<ProviderUsage | null> {
  const readClaudeAuth = async () => {
    const credentials = await readJsonFile(NodePath.join(claudeHome, ".claude/.credentials.json"));
    const oauth = objectValue(credentials?.claudeAiOauth) ?? credentials;
    const accessToken = stringValue(oauth?.accessToken) ?? stringValue(oauth?.access_token);
    if (!accessToken) return null;
    const subscriptionType =
      stringValue(oauth?.subscriptionType) ?? stringValue(oauth?.subscription_type);
    const rateLimitTier = stringValue(oauth?.rateLimitTier) ?? stringValue(oauth?.rate_limit_tier);
    const plan = [subscriptionType, rateLimitTier].filter(Boolean).join(" ") || null;
    return { accessToken, plan };
  };

  const fetchClaudeUsage = (accessToken: string) =>
    fetchJsonStatus(CLAUDE_USAGE_URL, {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/json",
      "Content-Type": "application/json",
      "anthropic-beta": CLAUDE_BETA_HEADER,
      "User-Agent": "claude-code/2.1.0",
    });

  const auth = await readClaudeAuth();
  if (!auth) return null;
  let response = await fetchClaudeUsage(auth.accessToken);
  let plan = auth.plan;
  if (response.status === 401) {
    const freshAuth = await readClaudeAuth();
    if (!freshAuth) return null;
    plan = freshAuth.plan;
    response = await fetchClaudeUsage(freshAuth.accessToken);
  }
  return parseClaudeUsageResponse(response.payload, plan);
}

async function loadProviderUsage(
  scope: UsageCredentialScope,
): Promise<ReadonlyArray<ProviderUsage>> {
  const tasks: Array<Promise<ProviderUsage | null>> = [];
  if (scope.providers.includes("codex") && scope.codexHome) {
    tasks.push(loadCodexUsage(scope.codexHome));
  }
  if (scope.providers.includes("claude") && scope.claudeHome) {
    tasks.push(loadClaudeUsage(scope.claudeHome));
  }
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
  parseCodexUsageResponse,
  parseClaudeUsageResponse,
  claudeLimitWindow,
  resolveUsageCredentialScope,
};
