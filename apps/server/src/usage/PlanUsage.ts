// @effect-diagnostics nodeBuiltinImport:off - Provider usage discovery reads CLI-owned files and binaries.
// @effect-diagnostics globalDate:off - Provider APIs exchange reset timestamps as Unix/ISO dates.
import type {
  PlanUsageProvider,
  PlanUsageSnapshot,
  PlanUsageWindow,
  ProviderInstanceEnvironment,
  ServerSettings,
} from "@t3tools/contracts";
import {
  ProviderInstanceId,
  type ProviderInstanceId as ProviderInstanceIdType,
} from "@t3tools/contracts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";
import * as NodeUtil from "node:util";
import { expandHomePath } from "../pathExpansion.ts";
import { mergeProviderInstanceEnvironment } from "../provider/ProviderInstanceEnvironment.ts";

const CODEX_APP_SERVER_ARGS = ["app-server"] as const;
const CODEX_APP_SERVER_USAGE_TIMEOUT_MS = 10_000;
const CODEX_APP_SERVER_USAGE_FORCE_KILL_AFTER_MS = 1_000;
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
const CLAUDE_CLI_TIMEOUT_MS = 30_000;
const CLI_MAX_BUFFER_BYTES = 1024 * 1024;
const CLAUDE_SAFE_TRANSPORT_ENV_KEYS = new Set([
  "CLAUDE_CODE_CERT_STORE",
  "CLAUDE_CODE_CLIENT_CERT",
  "CLAUDE_CODE_CLIENT_KEY",
  "CLAUDE_CODE_CLIENT_KEY_PASSPHRASE",
  "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC",
]);
const CODEX_AUTH_OVERRIDE_ENV_KEYS = new Set([
  "AZURE_OPENAI_API_KEY",
  "CODEX_ACCESS_TOKEN",
  "CODEX_AUTH_TOKEN",
  "CODEX_REFRESH_TOKEN",
  "OPENAI_ACCESS_TOKEN",
  "OPENAI_API_KEY",
]);

const execFile = NodeUtil.promisify(NodeChildProcess.execFile);

const cached = new Map<
  string,
  {
    readonly expiresAt: number;
    readonly providers: ReadonlyArray<ProviderUsage>;
    readonly snapshot: PlanUsageSnapshot;
  }
>();

interface ProviderUsage {
  readonly sourceKey: string;
  readonly provider: PlanUsageProvider;
  readonly plan: string | null;
  readonly fetchedAt: string;
  readonly windows: ReadonlyArray<PlanUsageWindow>;
}

type ParsedProviderUsage = Omit<ProviderUsage, "fetchedAt" | "sourceKey">;

interface UsageCredentialSource {
  readonly provider: PlanUsageProvider;
  readonly instanceId: ProviderInstanceIdType;
  readonly home: string;
  readonly executable: string | null;
  readonly environment?: ProviderInstanceEnvironment | undefined;
}

interface UsageCredentialScope {
  readonly cacheKey: string;
  readonly sources: ReadonlyArray<UsageCredentialSource>;
}

interface CodexAppServerResponse {
  readonly id?: unknown;
  readonly method?: unknown;
  readonly result?: unknown;
  readonly error?: unknown;
}

interface LoadPlanUsageOptions {
  readonly settings?: ServerSettings | undefined;
  readonly providerInstanceId?: ProviderInstanceIdType | null | undefined;
}

interface UsageProviderInstance {
  readonly instanceId: ProviderInstanceIdType;
  readonly driver: string;
  readonly enabled?: boolean | undefined;
  readonly environment?: ProviderInstanceEnvironment | undefined;
  readonly config?: unknown;
}

function numberValue(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function numberLikeValue(value: unknown): number | null {
  const numeric = numberValue(value);
  if (numeric !== null) return numeric;
  if (typeof value !== "string") return null;
  const parsed = Number(value.trim());
  return Number.isFinite(parsed) ? parsed : null;
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
  const trimmed = raw.trim();
  const match = trimmed.match(
    /^([A-Za-z]+)\s+(\d{1,2}),\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?(?:\s+\(([^)]+)\))?$/i,
  );
  const timeOnlyMatch = trimmed.match(/^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?(?:\s+\(([^)]+)\))?$/i);
  const parseTime = (input: {
    readonly hour: number;
    readonly minute: number;
    readonly meridiem: string | undefined;
  }): { readonly hour: number; readonly minute: number } | null => {
    if (!Number.isInteger(input.hour) || !Number.isInteger(input.minute)) return null;
    if (input.minute < 0 || input.minute > 59) return null;
    if (input.meridiem) {
      if (input.hour < 1 || input.hour > 12) return null;
      let hour = input.hour;
      if (input.meridiem === "pm" && hour < 12) hour += 12;
      if (input.meridiem === "am" && hour === 12) hour = 0;
      return { hour, minute: input.minute };
    }
    if (input.hour < 0 || input.hour > 23) return null;
    return { hour: input.hour, minute: input.minute };
  };

  if (!match) {
    if (!timeOnlyMatch) return null;
    const hour = Number(timeOnlyMatch[1]);
    const minute = timeOnlyMatch[2] === undefined ? 0 : Number(timeOnlyMatch[2]);
    const meridiem = timeOnlyMatch[3]?.toLowerCase();
    const time = parseTime({ hour, minute, meridiem });
    if (!time) return null;
    const timeZone =
      timeOnlyMatch[4]?.trim() || Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
    const parts = timeZoneDateParts(timeZone, new Date(now));
    if (!parts) return null;
    let utcMillis = localTimeInZoneToUtcMillis({
      year: parts.year,
      month: parts.month - 1,
      day: parts.day,
      hour: time.hour,
      minute: time.minute,
      timeZone,
    });
    if (utcMillis === null) return null;
    if (utcMillis < now - 60 * 60 * 1000) {
      utcMillis =
        localTimeInZoneToUtcMillis({
          year: parts.year,
          month: parts.month - 1,
          day: parts.day + 1,
          hour: time.hour,
          minute: time.minute,
          timeZone,
        }) ?? utcMillis;
    }
    return new Date(utcMillis).toISOString();
  }

  const month = MONTHS[match[1]?.toLowerCase() ?? ""];
  if (month === undefined) return null;
  const day = Number(match[2]);
  const hour = Number(match[3]);
  const minute = match[4] === undefined ? 0 : Number(match[4]);
  const meridiem = match[5]?.toLowerCase();
  const time = parseTime({ hour, minute, meridiem });
  const timeZone = match[6]?.trim() || Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  if (!Number.isFinite(day) || !time) return null;

  const currentYear =
    timeZoneDateParts(timeZone, new Date(now))?.year ?? new Date(now).getUTCFullYear();
  let utcMillis = localTimeInZoneToUtcMillis({
    year: currentYear,
    month,
    day,
    hour: time.hour,
    minute: time.minute,
    timeZone,
  });
  if (utcMillis === null) return null;
  if (utcMillis < now - 60 * 60 * 1000) {
    utcMillis =
      localTimeInZoneToUtcMillis({
        year: currentYear + 1,
        month,
        day,
        hour: time.hour,
        minute: time.minute,
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
): ParsedProviderUsage | null {
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
  const usedPercent = numberValue(raw?.usedPercent);
  if (usedPercent === null) return null;
  return {
    id: input.id,
    provider: "codex",
    kind: input.kind,
    title: input.title,
    usedPercent: clampPercent(usedPercent),
    resetAt: unixSecondsToIso(raw?.resetsAt),
    used: null,
    limit: null,
    unit: null,
    severity: null,
  };
}

function codexIndividualLimitWindow(input: {
  readonly id: string;
  readonly title: string;
  readonly raw: unknown;
}): PlanUsageWindow | null {
  const limit = objectValue(input.raw);
  const remainingPercent = numberValue(limit?.remainingPercent);
  if (remainingPercent === null) return null;
  return {
    id: input.id,
    provider: "codex",
    kind: "individual_limit",
    title: input.title,
    usedPercent: clampPercent(100 - remainingPercent),
    resetAt: unixSecondsToIso(limit?.resetsAt),
    used: numberLikeValue(limit?.used),
    limit: numberLikeValue(limit?.limit),
    unit: null,
    severity: null,
  };
}

function codexRateLimitBucketSlug(value: string | null, index: number): string {
  const slug = value
    ?.toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return slug || `limit-${index + 1}`;
}

function codexRateLimitBucketLabel(
  rateLimits: Record<string, unknown>,
  fallbackKey: string | null,
): string | null {
  return stringValue(rateLimits.limitName) ?? fallbackKey;
}

function codexDurationLabel(durationMins: number): string {
  if (durationMins % 60 === 0) {
    return `${durationMins / 60}h`;
  }
  return `${durationMins}m`;
}

function codexPrimaryWindowDescriptor(raw: unknown): {
  readonly idSuffix: string;
  readonly kind: PlanUsageWindow["kind"];
  readonly title: string;
} {
  const durationMins = numberValue(objectValue(raw)?.windowDurationMins);
  if (durationMins === 300) {
    return { idSuffix: "five-hour", kind: "five_hour", title: "Codex 5h" };
  }
  if (durationMins !== null && Number.isInteger(durationMins) && durationMins > 0) {
    return {
      idSuffix: `${durationMins}-minute`,
      kind: `duration_${durationMins}_minutes`,
      title: `Codex ${codexDurationLabel(durationMins)}`,
    };
  }
  return { idSuffix: "primary", kind: "primary", title: "Codex primary" };
}

function codexWindowsForRateLimitBucket(input: {
  readonly rateLimits: Record<string, unknown>;
  readonly idPrefix: string;
  readonly titleSuffix: string;
}): ReadonlyArray<PlanUsageWindow> {
  const primary = codexPrimaryWindowDescriptor(input.rateLimits.primary);
  return [
    codexWindow({
      id: `${input.idPrefix}${primary.idSuffix}`,
      kind: primary.kind,
      title: `${primary.title}${input.titleSuffix}`,
      raw: input.rateLimits.primary,
    }),
    codexWindow({
      id: `${input.idPrefix}weekly`,
      kind: "weekly",
      title: `Codex weekly${input.titleSuffix}`,
      raw: input.rateLimits.secondary,
    }),
    codexIndividualLimitWindow({
      id: `${input.idPrefix}individual-limit`,
      title: `Codex individual limit${input.titleSuffix}`,
      raw: input.rateLimits.individualLimit,
    }),
  ].filter((window): window is PlanUsageWindow => window !== null);
}

function parseCodexRateLimitsResponse(payload: unknown): ParsedProviderUsage | null {
  const root = objectValue(payload);
  const rateLimits = objectValue(root?.rateLimits);
  if (!root || !rateLimits) return null;

  const rateLimitsByLimitId = objectValue(root.rateLimitsByLimitId);
  const availableBucketEntries = rateLimitsByLimitId
    ? Object.entries(rateLimitsByLimitId)
        .map(([key, value], index) => ({ index, key, rateLimits: objectValue(value) }))
        .filter(
          (
            entry,
          ): entry is {
            readonly index: number;
            readonly key: string;
            readonly rateLimits: Record<string, unknown>;
          } => entry.rateLimits !== null,
        )
    : [];
  const bucketEntries = availableBucketEntries.filter(({ key, rateLimits: bucket }) => {
    const id = stringValue(bucket.limitId) ?? key;
    const label = codexRateLimitBucketLabel(bucket, key);
    const isSparkBucket = (value: string) => /(^|[^a-z])spark([^a-z]|$)/i.test(value);
    return !isSparkBucket(id) && !isSparkBucket(label ?? "");
  });
  const bucketWindows = bucketEntries.flatMap(({ index, key, rateLimits: bucket }) => {
    const slug = codexRateLimitBucketSlug(stringValue(bucket.limitId) ?? key, index);
    const label = codexRateLimitBucketLabel(bucket, key);
    return codexWindowsForRateLimitBucket({
      rateLimits: bucket,
      idPrefix: `codex-${slug}-`,
      titleSuffix: label && bucketEntries.length > 1 ? ` (${label})` : "",
    });
  });
  const windows =
    availableBucketEntries.length > 0
      ? bucketWindows
      : codexWindowsForRateLimitBucket({
          rateLimits,
          idPrefix: "codex-",
          titleSuffix: "",
        });

  if (windows.length === 0) return null;
  return {
    provider: "codex",
    plan: stringValue(rateLimits.planType),
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

function parseClaudeUsageResponse(
  payload: unknown,
  plan: string | null,
): ParsedProviderUsage | null {
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
    executable: configuredCommandOrNull(config?.binaryPath),
    environment: instance.environment,
  };
}

function uniqueUsageSources(
  sources: ReadonlyArray<UsageCredentialSource>,
): ReadonlyArray<UsageCredentialSource> {
  const seen = new Set<string>();
  return sources.filter((source) => {
    const key = usageProbeKey(source);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function usageEnvironmentKeyFromEnv(
  environment: NodeJS.ProcessEnv,
): ReadonlyArray<readonly [string, string]> {
  return Object.entries(environment)
    .filter((entry): entry is [string, string] => entry[0] !== "HOME" && entry[1] !== undefined)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(
      ([name, value]) =>
        [name, NodeCrypto.createHash("sha256").update(value).digest("base64url")] as const,
    );
}

function usageEffectiveEnvironmentKey(
  source: UsageCredentialSource,
): ReadonlyArray<readonly [string, string]> {
  return usageEnvironmentKeyFromEnv(
    source.provider === "codex"
      ? codexAppServerEnvironment(source.home, source.environment, {} as NodeJS.ProcessEnv)
      : claudeCliEnvironment(source.home, source.environment, {} as NodeJS.ProcessEnv),
  );
}

function usageProbeKey(source: UsageCredentialSource): string {
  return JSON.stringify([
    source.provider,
    source.home,
    source.executable,
    usageEffectiveEnvironmentKey(source),
  ]);
}

function usageSourceKey(source: UsageCredentialSource): string {
  return JSON.stringify([
    source.provider,
    source.instanceId,
    source.home,
    source.executable,
    usageEffectiveEnvironmentKey(source),
  ]);
}

function usageScopeCacheKey(sources: ReadonlyArray<UsageCredentialSource>): string {
  return `sources:${JSON.stringify(sources.map((source) => usageSourceKey(source)))}`;
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

export function scopePlanUsageSnapshot(
  snapshot: PlanUsageSnapshot,
  providerInstanceId: ProviderInstanceIdType,
  settings: ServerSettings,
): PlanUsageSnapshot {
  const requestedSource = resolveUsageCredentialScope({ settings, providerInstanceId }).sources[0];
  if (!requestedSource) return { updatedAt: snapshot.updatedAt, providers: [] };
  const canonicalSource = resolveUsageCredentialScope({ settings }).sources.find(
    (source) => usageProbeKey(source) === usageProbeKey(requestedSource),
  );
  if (!canonicalSource) return { updatedAt: snapshot.updatedAt, providers: [] };
  const canonicalMarker = `:${canonicalSource.instanceId}:`;
  const requestedMarker = `:${providerInstanceId}:`;
  const canonicalTitleSuffix = ` (${canonicalSource.instanceId})`;
  return {
    updatedAt: snapshot.updatedAt,
    providers: snapshot.providers.flatMap((provider) => {
      const windows = provider.windows
        .filter((window) => window.id.includes(canonicalMarker))
        .map((window) => ({
          ...window,
          id: window.id.replace(canonicalMarker, requestedMarker),
          title: window.title.endsWith(canonicalTitleSuffix)
            ? window.title.slice(0, -canonicalTitleSuffix.length)
            : window.title,
        }));
      return windows.length > 0 ? [{ ...provider, windows }] : [];
    }),
  };
}

function codexAppServerEnvironment(
  codexHome: string,
  environment?: ProviderInstanceEnvironment | undefined,
  baseEnv: NodeJS.ProcessEnv = process.env,
): NodeJS.ProcessEnv {
  const sanitizedBaseEnv = codexUsageBaseEnvironment(baseEnv);
  return {
    ...mergeProviderInstanceEnvironment(environment, sanitizedBaseEnv),
    CODEX_HOME: codexHome,
  };
}

function isCodexAuthOverrideEnvKey(key: string): boolean {
  const normalized = key.toUpperCase();
  return (
    CODEX_AUTH_OVERRIDE_ENV_KEYS.has(normalized) ||
    (normalized.startsWith("CODEX_") && normalized.includes("TOKEN")) ||
    (normalized.startsWith("OPENAI_") &&
      (normalized.includes("API_KEY") || normalized.includes("TOKEN"))) ||
    (normalized.startsWith("AZURE_OPENAI_") &&
      (normalized.includes("API_KEY") || normalized.includes("TOKEN")))
  );
}

function codexUsageBaseEnvironment(baseEnv: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = { ...baseEnv };
  for (const key of Object.keys(env)) {
    if (isCodexAuthOverrideEnvKey(key)) {
      delete env[key];
    }
  }
  return env;
}

function writeCodexAppServerMessage(
  child: NodeChildProcess.ChildProcess,
  message: Record<string, unknown>,
): void {
  child.stdin?.write(`${JSON.stringify(message)}\n`);
}

function parseCodexAppServerLine(line: string): CodexAppServerResponse | null {
  const trimmed = line.trim();
  if (!trimmed) return null;
  try {
    const payload = JSON.parse(trimmed) as unknown;
    return objectValue(payload) as CodexAppServerResponse | null;
  } catch {
    return null;
  }
}

function codexAppServerMessageId(value: unknown): string | number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  return stringValue(value);
}

function writeCodexAppServerError(
  child: NodeChildProcess.ChildProcess,
  id: string | number,
  message: string,
): void {
  writeCodexAppServerMessage(child, {
    id,
    error: {
      code: -32601,
      message,
    },
  });
}

async function terminateCodexAppServer(child: NodeChildProcess.ChildProcess): Promise<void> {
  child.stdin?.end();
  child.stdout?.destroy();
  child.stderr?.destroy();
  if (child.exitCode !== null || child.signalCode !== null) return;

  const abort = new AbortController();
  let onExit: (() => void) | undefined;
  const exited = new Promise<void>((resolve) => {
    onExit = () => resolve();
    child.once("exit", onExit);
  });
  try {
    child.kill("SIGTERM");
    await Promise.race([
      exited,
      NodeTimersPromises.scheduler
        .wait(CODEX_APP_SERVER_USAGE_FORCE_KILL_AFTER_MS, {
          signal: abort.signal,
        })
        .then(() => {
          if (child.exitCode === null && child.signalCode === null) {
            child.kill("SIGKILL");
          }
        }),
    ]);
  } finally {
    abort.abort();
    if (onExit) child.off("exit", onExit);
  }
}

async function withCodexAppServerUsageTimeout<T>(task: Promise<T>): Promise<T> {
  const abort = new AbortController();
  try {
    return await Promise.race([
      task,
      NodeTimersPromises.scheduler
        .wait(CODEX_APP_SERVER_USAGE_TIMEOUT_MS, { signal: abort.signal })
        .then(() => {
          throw new Error("Codex app-server usage probe timed out.");
        }),
    ]);
  } finally {
    abort.abort();
  }
}

async function readCodexAppServerRateLimits(
  source: UsageCredentialSource,
): Promise<unknown | null> {
  const env = codexAppServerEnvironment(source.home, source.environment);
  const executable = source.executable ?? "codex";
  const spawnCommand = await Effect.runPromise(
    resolveSpawnCommand(executable, CODEX_APP_SERVER_ARGS, { env }),
  );
  const child = NodeChildProcess.spawn(spawnCommand.command, spawnCommand.args, {
    cwd: process.cwd(),
    env,
    shell: spawnCommand.shell,
    stdio: ["pipe", "pipe", "pipe"],
  });
  child.stdin?.on("error", () => undefined);
  child.stderr?.resume();

  let nextRequestId = 0;
  let stdoutBuffer = "";
  const pending = new Map<
    string,
    {
      readonly resolve: (value: unknown) => void;
      readonly reject: (cause: Error) => void;
    }
  >();

  const rejectPending = (cause: Error): void => {
    for (const request of pending.values()) {
      request.reject(cause);
    }
    pending.clear();
  };

  const handleLine = (line: string): void => {
    const message = parseCodexAppServerLine(line);
    const id = codexAppServerMessageId(message?.id);
    if (id === null) return;
    const method = stringValue(message?.method);
    if (method) {
      writeCodexAppServerError(child, id, `Method not found: ${method}`);
      return;
    }
    const request = pending.get(String(id));
    if (!request) return;
    pending.delete(String(id));
    if (message?.error !== undefined) {
      request.reject(new Error("Codex app-server returned an error response."));
      return;
    }
    request.resolve(message?.result ?? null);
  };

  child.stdout?.setEncoding("utf8");
  child.stdout?.on("data", (chunk: string) => {
    stdoutBuffer += chunk;
    if (stdoutBuffer.length > CLI_MAX_BUFFER_BYTES) {
      rejectPending(new Error("Codex app-server usage response exceeded the output limit."));
      return;
    }
    for (;;) {
      const newlineIndex = stdoutBuffer.indexOf("\n");
      if (newlineIndex === -1) break;
      const line = stdoutBuffer.slice(0, newlineIndex);
      stdoutBuffer = stdoutBuffer.slice(newlineIndex + 1);
      handleLine(line);
    }
  });
  child.once("error", (cause) => rejectPending(cause));
  child.once("exit", (code, signal) => {
    rejectPending(new Error(`Codex app-server exited before usage was read: ${code ?? signal}`));
  });

  const request = (method: string, params?: unknown): Promise<unknown> => {
    const id = ++nextRequestId;
    return new Promise((resolve, reject) => {
      pending.set(String(id), { resolve, reject });
      try {
        writeCodexAppServerMessage(child, {
          id,
          method,
          ...(params === undefined ? {} : { params }),
        });
      } catch (cause) {
        pending.delete(String(id));
        reject(cause instanceof Error ? cause : new Error(String(cause)));
      }
    });
  };

  const notify = (method: string, params?: unknown): void => {
    writeCodexAppServerMessage(child, {
      method,
      ...(params === undefined ? {} : { params }),
    });
  };

  try {
    const rateLimits = await withCodexAppServerUsageTimeout(
      (async () => {
        await request("initialize", {
          clientInfo: {
            name: "t3code_usage",
            title: "T3 Code Usage",
            version: "0.0.0",
          },
          capabilities: {
            experimentalApi: true,
          },
        });
        notify("initialized");
        return await request("account/rateLimits/read");
      })(),
    );
    return rateLimits;
  } finally {
    rejectPending(new Error("Codex app-server usage probe finished."));
    await terminateCodexAppServer(child);
  }
}

async function loadCodexUsage(source: UsageCredentialSource): Promise<ParsedProviderUsage | null> {
  try {
    return parseCodexRateLimitsResponse(await readCodexAppServerRateLimits(source));
  } catch {
    return null;
  }
}

function isClaudeAuthOverrideEnvKey(key: string): boolean {
  const normalized = key.toUpperCase();
  if (CLAUDE_SAFE_TRANSPORT_ENV_KEYS.has(normalized)) return false;
  return normalized.startsWith("ANTHROPIC_") || normalized.startsWith("CLAUDE_CODE_");
}

function claudeCliEnvironment(
  home: string,
  environment?: ProviderInstanceEnvironment | undefined,
  baseEnv: NodeJS.ProcessEnv = process.env,
): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {
    ...mergeProviderInstanceEnvironment(environment, baseEnv),
    HOME: home,
  };
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
  const env = claudeCliEnvironment(source.home, source.environment);
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
      timeout: CLAUDE_CLI_TIMEOUT_MS,
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

async function loadClaudeUsage(source: UsageCredentialSource): Promise<ParsedProviderUsage | null> {
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
  now: number,
): Promise<ReadonlyArray<ProviderUsage>> {
  const sourceCounts = new Map<PlanUsageProvider, number>();
  for (const source of scope.sources) {
    sourceCounts.set(source.provider, (sourceCounts.get(source.provider) ?? 0) + 1);
  }
  const fetchedAt = new Date(now).toISOString();
  const tasks = scope.sources.map(async (source): Promise<ProviderUsage | null> => {
    const usage =
      source.provider === "codex" ? await loadCodexUsage(source) : await loadClaudeUsage(source);
    if (!usage) return null;
    const includeInstanceLabel = (sourceCounts.get(source.provider) ?? 0) > 1;
    return {
      ...usage,
      fetchedAt,
      sourceKey: usageSourceKey(source),
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

function staleProviderUsage(provider: ProviderUsage): ProviderUsage {
  return {
    ...provider,
    windows: provider.windows.map((window) => ({
      ...window,
      staleAt: window.staleAt ?? provider.fetchedAt,
    })),
  };
}

function mergeProviderUsageWithFallback(input: {
  readonly sources: ReadonlyArray<UsageCredentialSource>;
  readonly freshProviders: ReadonlyArray<ProviderUsage>;
  readonly currentCachedProviders: ReadonlyArray<ProviderUsage>;
  readonly staleProviders: ReadonlyArray<ProviderUsage>;
}): ReadonlyArray<ProviderUsage> {
  const freshBySource = new Map(
    input.freshProviders.map((provider) => [provider.sourceKey, provider]),
  );
  const currentCachedBySource = new Map(
    input.currentCachedProviders.map((provider) => [provider.sourceKey, provider]),
  );
  const staleBySource = new Map(
    input.staleProviders.map((provider) => [provider.sourceKey, provider]),
  );
  return input.sources.flatMap((source) => {
    const sourceKey = usageSourceKey(source);
    const fresh = freshBySource.get(sourceKey);
    if (fresh) return [fresh];
    const currentCached = currentCachedBySource.get(sourceKey);
    if (currentCached) return [currentCached];
    const stale = staleBySource.get(sourceKey);
    return stale ? [staleProviderUsage(stale)] : [];
  });
}

function providerUsageSnapshot(
  providers: ReadonlyArray<ProviderUsage>,
  now: number,
): PlanUsageSnapshot {
  return {
    updatedAt: new Date(now).toISOString(),
    providers: providers.map((provider) => ({
      provider: provider.provider,
      plan: provider.plan,
      windows: [...provider.windows],
    })),
  };
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

  const freshProviders = await loadProviderUsage(scope, now);
  const latest = cached.get(scope.cacheKey);
  const currentCachedProviders =
    latest && latest !== existing && latest.expiresAt > now ? latest.providers : [];
  if (freshProviders.length === 0 && currentCachedProviders.length > 0 && latest) {
    return latest.snapshot;
  }

  const providers = mergeProviderUsageWithFallback({
    sources: scope.sources,
    freshProviders,
    currentCachedProviders,
    staleProviders: existing?.providers ?? [],
  });
  const snapshot = providerUsageSnapshot(providers, now);
  if (providers.length > 0) {
    pruneUsageCache(now);
    cached.set(scope.cacheKey, { expiresAt: now + CACHE_TTL_MS, providers, snapshot });
  }
  return snapshot;
}

export const __testing = {
  CODEX_APP_SERVER_USAGE_TIMEOUT_MS,
  CLAUDE_CLI_USAGE_ARGS,
  CLAUDE_CLI_TIMEOUT_MS,
  codexAppServerEnvironment,
  parseCodexRateLimitsResponse,
  parseClaudeUsageResponse,
  parseClaudeCliUsageText,
  parseClaudeCliResetAt,
  claudeCliEnvironment,
  claudeLimitWindow,
  isPlanUsagePollingDisabled,
  resolveUsageCredentialScope,
};
