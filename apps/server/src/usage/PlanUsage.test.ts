// @effect-diagnostics nodeBuiltinImport:off
import { describe, expect, it } from "@effect/vitest";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderInstanceEnvironment,
  type ServerSettings,
} from "@t3tools/contracts";

import { __testing, loadPlanUsageSnapshot } from "./PlanUsage.ts";

async function makeFakeCodexAppServer(input: {
  readonly rateLimitsResponse: unknown;
  readonly requiredEnv?: Readonly<Record<string, string>>;
  readonly forbiddenEnv?: ReadonlyArray<string>;
  readonly serverRequestMethod?: string | undefined;
}): Promise<{
  readonly binaryPath: string;
  readonly homePath: string;
  readonly requestsPath: string;
}> {
  const homePath = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-usage-codex-home-"));
  const requestsPath = NodePath.join(homePath, "requests.jsonl");
  const binaryPath = NodePath.join(homePath, "codex-fake");
  const script = `#!/usr/bin/env node
const fs = require("node:fs");
const readline = require("node:readline");

if (!process.argv.slice(2).includes("app-server")) {
  console.error("expected app-server argument");
  process.exit(2);
}

const requiredEnv = ${JSON.stringify(input.requiredEnv ?? {})};
const forbiddenEnv = ${JSON.stringify(input.forbiddenEnv ?? [])};
for (const [key, value] of Object.entries(requiredEnv)) {
  if (process.env[key] !== value) {
    console.error(\`missing required env \${key}\`);
    process.exit(3);
  }
}
for (const key of forbiddenEnv) {
  if (process.env[key] !== undefined) {
    console.error(\`forbidden env \${key}\`);
    process.exit(6);
  }
}
if (process.env.CODEX_HOME !== ${JSON.stringify(homePath)}) {
  console.error("unexpected CODEX_HOME");
  process.exit(4);
}

const requestsPath = ${JSON.stringify(requestsPath)};
const rateLimitsResponse = ${JSON.stringify(input.rateLimitsResponse)};
const serverRequestMethod = ${JSON.stringify(input.serverRequestMethod ?? null)};
let sawServerRequestResponse = serverRequestMethod === null;
const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
const rl = readline.createInterface({ input: process.stdin });
rl.on("line", (line) => {
  const rpc = JSON.parse(line);
  fs.appendFileSync(
    requestsPath,
    JSON.stringify({
      id: rpc.id ?? null,
      method: rpc.method ?? null,
      params: rpc.params ?? null,
      error: rpc.error ?? null
    }) + "\\n"
  );
  if (rpc.id === "server-request-1" && rpc.error) {
    sawServerRequestResponse = true;
    return;
  }
  if (rpc.id && rpc.method === "initialize") {
    if (serverRequestMethod) {
      write({ id: "server-request-1", method: serverRequestMethod, params: {} });
    }
    write({
      id: rpc.id,
      result: {
        userAgent: "codex-fake/0.0.0",
        codexHome: process.env.CODEX_HOME,
        platformFamily: "unix",
        platformOs: "linux"
      }
    });
    return;
  }
  if (rpc.method === "initialized") {
    return;
  }
  if (rpc.id && rpc.method === "account/rateLimits/read") {
    if (!sawServerRequestResponse) {
      console.error("missing server request response");
      process.exit(5);
    }
    write({ id: rpc.id, result: rateLimitsResponse });
    process.exit(0);
  }
  if (rpc.id) {
    write({ id: rpc.id, error: { message: "unexpected request" } });
  }
});
`;
  await NodeFSP.writeFile(binaryPath, script, { mode: 0o700 });
  await NodeFSP.chmod(binaryPath, 0o700);
  return { binaryPath, homePath, requestsPath };
}

async function makeFakeClaude(input: {
  readonly usageTexts: ReadonlyArray<string>;
  readonly usageDelaysMs?: ReadonlyArray<number>;
  readonly requiredEnv?: Readonly<Record<string, string>>;
  readonly forbiddenEnv?: ReadonlyArray<string>;
}): Promise<{ readonly binaryPath: string; readonly homePath: string }> {
  const homePath = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-usage-claude-home-"));
  const statePath = NodePath.join(homePath, "usage-index");
  const binaryPath = NodePath.join(homePath, "claude-fake");
  const script = `#!/usr/bin/env node
const fs = require("node:fs");
const args = process.argv.slice(2);
const requiredEnv = ${JSON.stringify(input.requiredEnv ?? {})};
const forbiddenEnv = ${JSON.stringify(input.forbiddenEnv ?? [])};
for (const [key, value] of Object.entries(requiredEnv)) {
  if (process.env[key] !== value) {
    console.error(\`missing required env \${key}\`);
    process.exit(2);
  }
}
for (const key of forbiddenEnv) {
  if (process.env[key] !== undefined) {
    console.error(\`forbidden env \${key}\`);
    process.exit(3);
  }
}
if (args.includes("auth") && args.includes("status")) {
  console.log(JSON.stringify({ loggedIn: true, subscriptionType: "max" }));
  process.exit(0);
}
const usageTexts = ${JSON.stringify(input.usageTexts)};
const usageDelaysMs = ${JSON.stringify(input.usageDelaysMs ?? [])};
const statePath = ${JSON.stringify(statePath)};
const current = Number(fs.existsSync(statePath) ? fs.readFileSync(statePath, "utf8") : "0");
fs.writeFileSync(statePath, String(current + 1));
const delayMs = usageDelaysMs[Math.min(current, usageDelaysMs.length - 1)] ?? 0;
if (delayMs > 0) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, delayMs);
}
console.log(JSON.stringify({ is_error: false, result: usageTexts[Math.min(current, usageTexts.length - 1)] ?? "" }));
`;
  await NodeFSP.writeFile(binaryPath, script, { mode: 0o700 });
  await NodeFSP.chmod(binaryPath, 0o700);
  return { binaryPath, homePath };
}

function fakeClaudeSettings(input: {
  readonly binaryPath: string;
  readonly homePath: string;
  readonly environment?: ProviderInstanceEnvironment;
}): ServerSettings {
  return {
    ...DEFAULT_SERVER_SETTINGS,
    providerInstances: {
      [ProviderInstanceId.make("claudeAgent")]: {
        driver: ProviderDriverKind.make("claudeAgent"),
        config: {
          binaryPath: input.binaryPath,
          homePath: input.homePath,
        },
        environment: input.environment ?? [],
      },
    },
  };
}

function fakeCodexSettings(input: {
  readonly binaryPath: string;
  readonly homePath: string;
  readonly environment?: ProviderInstanceEnvironment;
}): ServerSettings {
  return {
    ...DEFAULT_SERVER_SETTINGS,
    providerInstances: {
      [ProviderInstanceId.make("codex")]: {
        driver: ProviderDriverKind.make("codex"),
        config: {
          binaryPath: input.binaryPath,
          homePath: input.homePath,
        },
        environment: input.environment ?? [],
      },
    },
  };
}

describe("PlanUsage", () => {
  it("maps official Codex app-server primary, weekly, and individual limit windows", () => {
    const result = __testing.parseCodexRateLimitsResponse({
      rateLimits: {
        planType: "pro",
        primary: { usedPercent: 77, resetsAt: 1783103404, windowDurationMins: 300 },
        secondary: { usedPercent: 59, resetsAt: 1783419037, windowDurationMins: 10080 },
        individualLimit: {
          limit: "100",
          remainingPercent: 25,
          resetsAt: 1783419037,
          used: "75",
        },
        credits: {
          balance: "42",
          hasCredits: true,
          unlimited: false,
        },
      },
    });

    expect(result?.plan).toBe("pro");
    expect(result?.windows.map((window) => window.id)).toEqual([
      "codex-five-hour",
      "codex-weekly",
      "codex-individual-limit",
    ]);
    expect(result?.windows[0]?.resetAt).toBe("2026-07-03T18:30:04.000Z");
    expect(result?.windows[2]).toMatchObject({
      usedPercent: 75,
      used: 75,
      limit: 100,
      resetAt: "2026-07-07T10:10:37.000Z",
    });
  });

  it("omits Codex app-server windows that are absent for the current plan", () => {
    const result = __testing.parseCodexRateLimitsResponse({
      rateLimits: {
        planType: "enterprise",
        primary: null,
        secondary: { usedPercent: 59, resetsAt: 1783419037, windowDurationMins: 10080 },
      },
    });

    expect(result?.plan).toBe("enterprise");
    expect(result?.windows.map((window) => window.id)).toEqual(["codex-weekly"]);
  });

  it("derives Codex primary window metadata from the app-server duration", () => {
    const result = __testing.parseCodexRateLimitsResponse({
      rateLimits: {
        planType: "pro",
        primary: { usedPercent: 6, resetsAt: 1783103404, windowDurationMins: 15 },
      },
    });

    expect(result?.windows[0]).toMatchObject({
      id: "codex-15-minute",
      kind: "duration_15_minutes",
      title: "Codex 15m",
      usedPercent: 6,
    });
  });

  it("maps Codex app-server multi-bucket rate limits", () => {
    const result = __testing.parseCodexRateLimitsResponse({
      rateLimits: {
        planType: "pro",
        primary: { usedPercent: 1, resetsAt: 1783103404 },
      },
      rateLimitsByLimitId: {
        codex: {
          limitId: "codex",
          limitName: "Codex",
          planType: "pro",
          primary: { usedPercent: 22, resetsAt: 1783103404, windowDurationMins: 300 },
          secondary: { usedPercent: 44, resetsAt: 1783419037, windowDurationMins: 10080 },
        },
        spark: {
          limitId: "spark",
          limitName: "Spark",
          planType: "pro",
          secondary: { usedPercent: 91, resetsAt: 1783419037, windowDurationMins: 10080 },
        },
      },
    });

    expect(result?.windows.map((window) => window.id)).toEqual([
      "codex-codex-five-hour",
      "codex-codex-weekly",
      "codex-spark-weekly",
    ]);
    expect(result?.windows.map((window) => window.title)).toEqual([
      "Codex 5h (Codex)",
      "Codex weekly (Codex)",
      "Codex weekly (Spark)",
    ]);
    expect(result?.windows.map((window) => window.usedPercent)).toEqual([22, 44, 91]);
  });

  it("reads Codex usage through the official app-server probe", async () => {
    const now = Date.parse("2026-07-06T15:10:00.000Z");
    const fake = await makeFakeCodexAppServer({
      requiredEnv: {
        HTTPS_PROXY: "http://proxy.example",
      },
      forbiddenEnv: ["CODEX_ACCESS_TOKEN"],
      serverRequestMethod: "account/chatgptAuthTokens/refresh",
      rateLimitsResponse: {
        rateLimits: {
          planType: "team",
          primary: { usedPercent: 12, resetsAt: 1783103404, windowDurationMins: 300 },
          secondary: { usedPercent: 34, resetsAt: 1783419037, windowDurationMins: 10080 },
        },
      },
    });
    await NodeFSP.writeFile(NodePath.join(fake.homePath, "auth.json"), "not-json\n", {
      mode: 0o600,
    });
    const settings = fakeCodexSettings({
      ...fake,
      environment: [{ name: "HTTPS_PROXY", value: "http://proxy.example", sensitive: false }],
    });
    const previousCodexAccessToken = process.env.CODEX_ACCESS_TOKEN;
    process.env.CODEX_ACCESS_TOKEN = "ambient-token";

    const snapshot = await loadPlanUsageSnapshot(
      { settings, providerInstanceId: ProviderInstanceId.make("codex") },
      now,
    ).finally(() => {
      if (previousCodexAccessToken === undefined) {
        delete process.env.CODEX_ACCESS_TOKEN;
      } else {
        process.env.CODEX_ACCESS_TOKEN = previousCodexAccessToken;
      }
    });

    expect(snapshot.providers).toHaveLength(1);
    expect(snapshot.providers[0]?.provider).toBe("codex");
    expect(snapshot.providers[0]?.plan).toBe("team");
    expect(snapshot.providers[0]?.windows.map((window) => window.id)).toEqual([
      "codex:codex:codex-five-hour",
      "codex:codex:codex-weekly",
    ]);
    expect(snapshot.providers[0]?.windows.map((window) => window.usedPercent)).toEqual([12, 34]);
    const requests = (await NodeFSP.readFile(fake.requestsPath, "utf8"))
      .trim()
      .split("\n")
      .map(
        (line) =>
          JSON.parse(line) as {
            readonly id: string | number | null;
            readonly method: string | null;
            readonly params: unknown | null;
            readonly error: { readonly code?: number; readonly message?: string } | null;
          },
      );
    expect(requests.map((request) => request.method)).toEqual([
      "initialize",
      null,
      "initialized",
      "account/rateLimits/read",
    ]);
    expect(requests[0]?.params).toMatchObject({
      capabilities: { experimentalApi: true },
      clientInfo: { name: "t3code_usage" },
    });
    expect(requests[1]?.id).toBe("server-request-1");
    expect(requests[1]?.error).toMatchObject({
      code: -32601,
      message: "Method not found: account/chatgptAuthTokens/refresh",
    });
    expect(requests[2]?.params).toBeNull();
    expect(requests[3]?.params).toBeNull();
  });

  it("allows explicit Codex auth environment configured on the provider instance", async () => {
    const now = Date.parse("2026-07-06T15:10:00.000Z");
    const fake = await makeFakeCodexAppServer({
      requiredEnv: {
        CODEX_ACCESS_TOKEN: "provider-token",
      },
      rateLimitsResponse: {
        rateLimits: {
          planType: "team",
          primary: { usedPercent: 12, resetsAt: 1783103404, windowDurationMins: 300 },
        },
      },
    });
    const settings = fakeCodexSettings({
      ...fake,
      environment: [{ name: "CODEX_ACCESS_TOKEN", value: "provider-token", sensitive: true }],
    });
    const previousCodexAccessToken = process.env.CODEX_ACCESS_TOKEN;
    process.env.CODEX_ACCESS_TOKEN = "ambient-token";

    const snapshot = await loadPlanUsageSnapshot(
      { settings, providerInstanceId: ProviderInstanceId.make("codex") },
      now,
    ).finally(() => {
      if (previousCodexAccessToken === undefined) {
        delete process.env.CODEX_ACCESS_TOKEN;
      } else {
        process.env.CODEX_ACCESS_TOKEN = previousCodexAccessToken;
      }
    });

    expect(snapshot.providers[0]?.windows.map((window) => window.usedPercent)).toEqual([12]);
  });

  it("maps Claude dynamic limits including scoped Fable weekly usage", () => {
    const result = __testing.parseClaudeUsageResponse(
      {
        seven_day_opus: null,
        seven_day_sonnet: null,
        limits: [
          {
            kind: "session",
            percent: 8,
            resets_at: "2026-07-03T17:00:00.166952+00:00",
            severity: "normal",
          },
          {
            kind: "weekly_all",
            percent: 15,
            resets_at: "2026-07-09T19:00:00.166975+00:00",
            severity: "warning",
          },
          {
            kind: "weekly_scoped",
            scope: { model: { display_name: "Fable" } },
            percent: 21,
            resets_at: "2026-07-09T19:00:00.166975+00:00",
            is_active: true,
            severity: "critical",
          },
        ],
      },
      "max default_claude_max_20x",
    );

    expect(result?.windows.map((window) => window.id)).toEqual([
      "claude-session-0",
      "claude-weekly_all-1",
      "claude-weekly_scoped-2",
    ]);
    expect(result?.windows.map((window) => window.title)).toEqual([
      "Claude Session",
      "Claude Weekly All",
      "Claude Weekly Scoped Fable",
    ]);
    expect(result?.windows[2]?.severity).toBe("critical");
  });

  it("ignores legacy Claude fields when limits are absent", () => {
    const result = __testing.parseClaudeUsageResponse(
      {
        seven_day_opus: { utilization: 99, resets_at: "2026-07-09T19:00:00.166975+00:00" },
        seven_day_sonnet: { utilization: 99, resets_at: "2026-07-09T19:00:00.166975+00:00" },
      },
      "max default_claude_max_20x",
    );

    expect(result).toBeNull();
  });

  it("maps official Claude CLI /usage text without requiring token access", () => {
    const result = __testing.parseClaudeCliUsageText(
      [
        "You are currently using your subscription to power your Claude Code usage",
        "",
        "Current session: 100% used \u00b7 resets Jul 6, 5pm (Europe/London)",
        "Current week (all models): 80% used \u00b7 resets Jul 9, 8pm (Europe/London)",
        "Current week (Fable): 86% used \u00b7 resets Jul 9, 8pm (Europe/London)",
        "Claude Code/Cowork: 99% used \u00b7 resets Jul 8, 9am (Europe/London)",
      ].join("\n"),
      "max",
      Date.parse("2026-07-06T15:10:00.000Z"),
    );

    expect(result?.plan).toBe("max");
    expect(result?.windows.map((window) => window.id)).toEqual([
      "claude-session-0",
      "claude-weekly_all-1",
      "claude-weekly_scoped-2",
      "claude-claude_code_cowork-3",
    ]);
    expect(result?.windows.map((window) => window.title)).toEqual([
      "Claude Session",
      "Claude Weekly All",
      "Claude Weekly Scoped Fable",
      "Claude Code/Cowork",
    ]);
    expect(result?.windows.map((window) => window.resetAt)).toEqual([
      "2026-07-06T16:00:00.000Z",
      "2026-07-09T19:00:00.000Z",
      "2026-07-09T19:00:00.000Z",
      "2026-07-08T08:00:00.000Z",
    ]);
  });

  it("maps split Claude CLI reset lines onto the preceding usage window", () => {
    const result = __testing.parseClaudeCliUsageText(
      [
        "Current session: 100% used",
        "Resets Jul 6, 5pm (Europe/London)",
        "Current week (all models): 80% used",
        "Reset time: Jul 9, 8pm (Europe/London)",
        "Claude Code/Cowork: 99% used",
        "Reset at Jul 8, 9am (Europe/London)",
      ].join("\n"),
      "max",
      Date.parse("2026-07-06T15:10:00.000Z"),
    );

    expect(result?.windows.map((window) => window.resetAt)).toEqual([
      "2026-07-06T16:00:00.000Z",
      "2026-07-09T19:00:00.000Z",
      "2026-07-08T08:00:00.000Z",
    ]);
  });

  it("parses time-only Claude CLI reset strings", () => {
    const result = __testing.parseClaudeCliUsageText(
      "Current session: 28% used \u00b7 resets 4:10am (Europe/London)",
      "max",
      Date.parse("2026-07-06T01:00:00.000Z"),
    );

    expect(result?.windows[0]?.resetAt).toBe("2026-07-06T03:10:00.000Z");
  });

  it("serves stale Claude windows when a later CLI report has no percent windows", async () => {
    const now = Date.parse("2026-07-06T15:10:00.000Z");
    const fake = await makeFakeClaude({
      usageTexts: [
        "Current session: 64% used \u00b7 resets Jul 6, 5pm (Europe/London)",
        [
          "You are currently using your subscription to power your Claude Code usage",
          "",
          "What's contributing to your limits usage?",
          "Last 24h \u00b7 1361 requests \u00b7 22 sessions",
        ].join("\n"),
      ],
    });
    const settings = fakeClaudeSettings(fake);

    const first = await loadPlanUsageSnapshot(
      { settings, providerInstanceId: ProviderInstanceId.make("claudeAgent") },
      now,
    );
    const second = await loadPlanUsageSnapshot(
      { settings, providerInstanceId: ProviderInstanceId.make("claudeAgent") },
      now + 61_000,
    );

    expect(first.providers[0]?.windows[0]?.usedPercent).toBe(64);
    expect(first.providers[0]?.windows[0]?.staleAt).toBeUndefined();
    expect(second.providers.map((provider) => provider.provider)).toEqual(["claude"]);
    expect(second.providers[0]?.windows[0]).toMatchObject({
      provider: "claude",
      usedPercent: 64,
      staleAt: "2026-07-06T15:10:00.000Z",
    });
  });

  it("does not let a slower stale fallback overwrite a concurrent fresh usage result", async () => {
    const now = Date.parse("2026-07-06T15:10:00.000Z");
    const fake = await makeFakeClaude({
      usageTexts: [
        "Current session: 64% used \u00b7 resets Jul 6, 5pm (Europe/London)",
        "Current session: 42% used \u00b7 resets Jul 6, 6pm (Europe/London)",
        [
          "You are currently using your subscription to power your Claude Code usage",
          "",
          "What's contributing to your limits usage?",
          "Last 24h \u00b7 1361 requests \u00b7 22 sessions",
        ].join("\n"),
      ],
      usageDelaysMs: [0, 0, 250],
    });
    const settings = fakeClaudeSettings(fake);
    const options = { settings, providerInstanceId: ProviderInstanceId.make("claudeAgent") };

    await loadPlanUsageSnapshot(options, now);
    await Promise.all([
      loadPlanUsageSnapshot(options, now + 61_000),
      loadPlanUsageSnapshot(options, now + 61_001),
    ]);
    const cachedAfterRace = await loadPlanUsageSnapshot(options, now + 62_000);

    expect(cachedAfterRace.providers[0]?.windows[0]?.usedPercent).toBe(42);
    expect(cachedAfterRace.providers[0]?.windows[0]?.staleAt).toBeUndefined();
  });

  it("runs Claude usage probes without saved print-mode sessions", () => {
    expect(__testing.CLAUDE_CLI_USAGE_ARGS).toContain("--no-session-persistence");
    expect(__testing.CLAUDE_CLI_USAGE_ARGS.indexOf("--no-session-persistence")).toBeLessThan(
      __testing.CLAUDE_CLI_USAGE_ARGS.indexOf("-p"),
    );
  });

  it("scrubs ambient Claude auth overrides before running the official CLI", () => {
    const env = __testing.claudeCliEnvironment(
      "/tmp/claude-home",
      [
        { name: "HTTPS_PROXY", value: "http://proxy.example", sensitive: false },
        { name: "NODE_EXTRA_CA_CERTS", value: "/tmp/root-ca.pem", sensitive: false },
        { name: "ANTHROPIC_API_KEY", value: "instance-api-key", sensitive: true },
      ],
      {
        HOME: "/tmp/ambient-home",
        PATH: "/bin",
        ANTHROPIC_BASE_URL: "https://proxy.example",
        ANTHROPIC_API_KEY: "ambient-api-key",
        ANTHROPIC_AUTH_TOKEN: "ambient-auth-token",
        ANTHROPIC_IDENTITY_TOKEN: "ambient-identity-token",
        ANTHROPIC_ACCESS_TOKEN: "ambient-access-token",
        CLAUDE_CODE_OAUTH_TOKEN: "ambient-oauth-token",
        CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR: "7",
        CLAUDE_CODE_SESSION_ACCESS_TOKEN: "ambient-session-token",
        CLAUDE_CODE_USE_GATEWAY: "1",
        CLAUDE_CODE_GATEWAY_URL: "https://gateway.example",
        CLAUDE_CODE_CERT_STORE: "/tmp/certs.pem",
        CLAUDE_CODE_CLIENT_CERT: "/tmp/client.pem",
        CLAUDE_CODE_CLIENT_KEY: "/tmp/client.key",
        CLAUDE_CODE_CLIENT_KEY_PASSPHRASE: "passphrase",
        CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1",
      } as NodeJS.ProcessEnv,
    );

    expect(env.HOME).toBe("/tmp/claude-home");
    expect(env.PATH).toBe("/bin");
    expect(env.HTTPS_PROXY).toBe("http://proxy.example");
    expect(env.NODE_EXTRA_CA_CERTS).toBe("/tmp/root-ca.pem");
    expect(env.ANTHROPIC_BASE_URL).toBeUndefined();
    expect(env.ANTHROPIC_API_KEY).toBeUndefined();
    expect(env.ANTHROPIC_AUTH_TOKEN).toBeUndefined();
    expect(env.ANTHROPIC_IDENTITY_TOKEN).toBeUndefined();
    expect(env.ANTHROPIC_ACCESS_TOKEN).toBeUndefined();
    expect(env.CLAUDE_CODE_OAUTH_TOKEN).toBeUndefined();
    expect(env.CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR).toBeUndefined();
    expect(env.CLAUDE_CODE_SESSION_ACCESS_TOKEN).toBeUndefined();
    expect(env.CLAUDE_CODE_USE_GATEWAY).toBeUndefined();
    expect(env.CLAUDE_CODE_GATEWAY_URL).toBeUndefined();
    expect(env.CLAUDE_CODE_CERT_STORE).toBe("/tmp/certs.pem");
    expect(env.CLAUDE_CODE_CLIENT_CERT).toBe("/tmp/client.pem");
    expect(env.CLAUDE_CODE_CLIENT_KEY).toBe("/tmp/client.key");
    expect(env.CLAUDE_CODE_CLIENT_KEY_PASSPHRASE).toBe("passphrase");
    expect(env.CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC).toBe("1");
  });

  it("uses provider-instance transport env while stripping API keys for Claude probes", async () => {
    const fake = await makeFakeClaude({
      usageTexts: ["Current session: 32% used \u00b7 resets Jul 6, 5pm (Europe/London)"],
      requiredEnv: {
        HTTPS_PROXY: "http://proxy.example",
        NODE_EXTRA_CA_CERTS: "/tmp/root-ca.pem",
      },
      forbiddenEnv: ["ANTHROPIC_API_KEY"],
    });
    const settings = fakeClaudeSettings({
      ...fake,
      environment: [
        { name: "HTTPS_PROXY", value: "http://proxy.example", sensitive: false },
        { name: "NODE_EXTRA_CA_CERTS", value: "/tmp/root-ca.pem", sensitive: false },
        { name: "ANTHROPIC_API_KEY", value: "poison-api-key", sensitive: true },
      ],
    });

    const snapshot = await loadPlanUsageSnapshot(
      { settings, providerInstanceId: ProviderInstanceId.make("claudeAgent") },
      Date.parse("2026-07-06T15:10:00.000Z"),
    );

    expect(snapshot.providers[0]?.provider).toBe("claude");
    expect(snapshot.providers[0]?.windows[0]?.usedPercent).toBe(32);
  });

  it("deduplicates equivalent Claude probes after endpoint and auth env are scrubbed", () => {
    const settings = {
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {
        [ProviderInstanceId.make("claudeAgent")]: {
          driver: ProviderDriverKind.make("claudeAgent"),
          enabled: false,
          config: {},
        },
        [ProviderInstanceId.make("claude_one")]: {
          driver: ProviderDriverKind.make("claudeAgent"),
          config: {
            binaryPath: "/usr/bin/claude",
            homePath: "/tmp/claude-home",
          },
          environment: [
            { name: "ANTHROPIC_API_KEY", value: "first-key", sensitive: true },
            { name: "ANTHROPIC_BASE_URL", value: "https://proxy-one.example", sensitive: false },
          ],
        },
        [ProviderInstanceId.make("claude_two")]: {
          driver: ProviderDriverKind.make("claudeAgent"),
          config: {
            binaryPath: "/usr/bin/claude",
            homePath: "/tmp/claude-home",
          },
          environment: [
            { name: "ANTHROPIC_API_KEY", value: "second-key", sensitive: true },
            { name: "CLAUDE_CODE_GATEWAY_URL", value: "https://gateway.example", sensitive: false },
          ],
        },
        [ProviderInstanceId.make("claude_proxy")]: {
          driver: ProviderDriverKind.make("claudeAgent"),
          config: {
            binaryPath: "/usr/bin/claude",
            homePath: "/tmp/claude-home",
          },
          environment: [{ name: "HTTPS_PROXY", value: "http://proxy.example", sensitive: false }],
        },
      },
    } satisfies ServerSettings;

    const scope = __testing.resolveUsageCredentialScope({ settings });

    expect(scope.sources).toMatchObject([
      { instanceId: "codex", provider: "codex" },
      { instanceId: "claude_one", provider: "claude" },
      { instanceId: "claude_proxy", provider: "claude" },
    ]);
  });

  it("supports disabling live plan usage polling with environment flags", () => {
    expect(
      __testing.isPlanUsagePollingDisabled({
        T3_DISABLE_PLAN_USAGE_POLLING: "1",
      } as NodeJS.ProcessEnv),
    ).toBe(true);
    expect(
      __testing.isPlanUsagePollingDisabled({
        T3_PLAN_USAGE_POLLING: "0",
      } as NodeJS.ProcessEnv),
    ).toBe(true);
    expect(
      __testing.isPlanUsagePollingDisabled({
        T3_PLAN_USAGE_POLLING: "1",
      } as NodeJS.ProcessEnv),
    ).toBe(false);
  });

  it("scopes credential homes to the selected provider instance", () => {
    const codexInstanceId = ProviderInstanceId.make("codex_work");
    const claudeInstanceId = ProviderInstanceId.make("claude_work");
    const settings = {
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {
        ...DEFAULT_SERVER_SETTINGS.providerInstances,
        [codexInstanceId]: {
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          config: {
            homePath: "/tmp/codex-base",
            shadowHomePath: " /tmp/codex-shadow ",
          },
        },
        [claudeInstanceId]: {
          driver: ProviderDriverKind.make("claudeAgent"),
          enabled: true,
          config: {
            homePath: " /tmp/claude-home ",
          },
        },
      },
    };

    expect(
      __testing.resolveUsageCredentialScope({
        settings,
        providerInstanceId: codexInstanceId,
      }),
    ).toMatchObject({
      sources: [{ provider: "codex", instanceId: "codex_work", home: "/tmp/codex-shadow" }],
    });
    expect(
      __testing.resolveUsageCredentialScope({
        settings,
        providerInstanceId: claudeInstanceId,
      }),
    ).toMatchObject({
      sources: [{ provider: "claude", instanceId: "claude_work", home: "/tmp/claude-home" }],
    });
  });

  it("uses all enabled Codex and Claude homes for aggregate usage", () => {
    const settings = {
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {
        ...DEFAULT_SERVER_SETTINGS.providerInstances,
        codex_work: {
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          config: {
            homePath: "/tmp/codex-base",
            shadowHomePath: "/tmp/codex-shadow",
          },
        },
        claude_work: {
          driver: ProviderDriverKind.make("claudeAgent"),
          enabled: true,
          config: {
            homePath: "/tmp/claude-home",
          },
        },
      },
      providers: {
        ...DEFAULT_SERVER_SETTINGS.providers,
        codex: {
          ...DEFAULT_SERVER_SETTINGS.providers.codex,
          homePath: "/tmp/codex-default",
          shadowHomePath: "",
        },
        claudeAgent: {
          ...DEFAULT_SERVER_SETTINGS.providers.claudeAgent,
          homePath: "/tmp/claude-default",
        },
      },
    };

    expect(
      __testing.resolveUsageCredentialScope({
        settings,
      }),
    ).toMatchObject({
      sources: [
        { provider: "codex", instanceId: "codex_work", home: "/tmp/codex-shadow" },
        { provider: "codex", instanceId: "codex", home: "/tmp/codex-default" },
        { provider: "claude", instanceId: "claude_work", home: "/tmp/claude-home" },
        { provider: "claude", instanceId: "claudeAgent", home: "/tmp/claude-default" },
      ],
    });
  });

  it("does not revive disabled default provider instances from legacy settings", () => {
    const settings = {
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {
        codex: {
          driver: ProviderDriverKind.make("codex"),
          enabled: false,
          config: {},
        },
        claudeAgent: {
          driver: ProviderDriverKind.make("claudeAgent"),
          enabled: false,
          config: {},
        },
      },
    };

    expect(
      __testing.resolveUsageCredentialScope({
        settings,
      }),
    ).toMatchObject({
      sources: [],
    });
  });

  it("does not revive legacy defaults when default instance ids are replaced by other drivers", () => {
    const settings = {
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {
        codex: {
          driver: ProviderDriverKind.make("ollama"),
          enabled: true,
          config: {},
        },
        claudeAgent: {
          driver: ProviderDriverKind.make("opencode"),
          enabled: true,
          config: {},
        },
      },
      providers: {
        ...DEFAULT_SERVER_SETTINGS.providers,
        codex: {
          ...DEFAULT_SERVER_SETTINGS.providers.codex,
          homePath: "/tmp/codex-default",
        },
        claudeAgent: {
          ...DEFAULT_SERVER_SETTINGS.providers.claudeAgent,
          homePath: "/tmp/claude-default",
        },
      },
    };

    expect(
      __testing.resolveUsageCredentialScope({
        settings,
      }),
    ).toMatchObject({
      sources: [],
    });
  });

  it("falls back to legacy defaults when only secondary aggregate instances are disabled", () => {
    const settings = {
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {
        codex_work: {
          driver: ProviderDriverKind.make("codex"),
          enabled: false,
          config: {
            shadowHomePath: "/tmp/disabled-codex-shadow",
          },
        },
        claude_work: {
          driver: ProviderDriverKind.make("claudeAgent"),
          enabled: false,
          config: {
            homePath: "/tmp/disabled-claude-home",
          },
        },
      },
      providers: {
        ...DEFAULT_SERVER_SETTINGS.providers,
        codex: {
          ...DEFAULT_SERVER_SETTINGS.providers.codex,
          homePath: "/tmp/codex-default",
          shadowHomePath: "",
        },
        claudeAgent: {
          ...DEFAULT_SERVER_SETTINGS.providers.claudeAgent,
          homePath: "/tmp/claude-default",
        },
      },
    };

    expect(
      __testing.resolveUsageCredentialScope({
        settings,
      }),
    ).toMatchObject({
      sources: [
        { provider: "codex", instanceId: "codex", home: "/tmp/codex-default" },
        { provider: "claude", instanceId: "claudeAgent", home: "/tmp/claude-default" },
      ],
    });
  });

  it("respects driver config disabled flags for aggregate and selected instances", () => {
    const codexInstanceId = ProviderInstanceId.make("codex_work");
    const claudeInstanceId = ProviderInstanceId.make("claude_work");
    const settings = {
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {
        [codexInstanceId]: {
          driver: ProviderDriverKind.make("codex"),
          config: {
            enabled: false,
            shadowHomePath: "/tmp/disabled-codex-shadow",
          },
        },
        [claudeInstanceId]: {
          driver: ProviderDriverKind.make("claudeAgent"),
          config: {
            enabled: false,
            homePath: "/tmp/disabled-claude-home",
          },
        },
      },
      providers: {
        ...DEFAULT_SERVER_SETTINGS.providers,
        codex: {
          ...DEFAULT_SERVER_SETTINGS.providers.codex,
          homePath: "/tmp/codex-default",
          shadowHomePath: "",
        },
        claudeAgent: {
          ...DEFAULT_SERVER_SETTINGS.providers.claudeAgent,
          homePath: "/tmp/claude-default",
        },
      },
    };

    expect(
      __testing.resolveUsageCredentialScope({
        settings,
      }),
    ).toMatchObject({
      sources: [
        { provider: "codex", instanceId: "codex", home: "/tmp/codex-default" },
        { provider: "claude", instanceId: "claudeAgent", home: "/tmp/claude-default" },
      ],
    });
    expect(
      __testing.resolveUsageCredentialScope({
        settings,
        providerInstanceId: codexInstanceId,
      }),
    ).toMatchObject({
      sources: [],
    });
  });

  it("falls back to legacy provider settings for default synthesized instances", () => {
    const settings = {
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {},
      providers: {
        ...DEFAULT_SERVER_SETTINGS.providers,
        codex: {
          ...DEFAULT_SERVER_SETTINGS.providers.codex,
          homePath: "/tmp/codex-default",
          shadowHomePath: "",
        },
      },
    };

    expect(
      __testing.resolveUsageCredentialScope({
        settings,
        providerInstanceId: ProviderInstanceId.make("codex"),
      }),
    ).toMatchObject({
      sources: [{ provider: "codex", instanceId: "codex", home: "/tmp/codex-default" }],
    });
  });
});
