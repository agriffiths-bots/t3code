import { describe, expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";

import { __testing } from "./PlanUsage.ts";

describe("PlanUsage", () => {
  it("maps Codex primary and weekly windows and ignores additional Spark limits", () => {
    const result = __testing.parseCodexUsageResponse({
      plan_type: "pro",
      rate_limit: {
        primary_window: { used_percent: 77, reset_at: 1783103404, limit_window_seconds: 18000 },
        secondary_window: { used_percent: 59, reset_at: 1783419037, limit_window_seconds: 604800 },
      },
      additional_rate_limits: [{ limit_name: "GPT-5.3-Codex-Spark" }],
    });

    expect(result?.plan).toBe("pro");
    expect(result?.windows.map((window) => window.id)).toEqual(["codex-five-hour", "codex-weekly"]);
    expect(result?.windows[0]?.resetAt).toBe("2026-07-03T18:30:04.000Z");
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

  it("runs Claude usage probes without saved print-mode sessions", () => {
    expect(__testing.CLAUDE_CLI_USAGE_ARGS).toContain("--no-session-persistence");
    expect(__testing.CLAUDE_CLI_USAGE_ARGS.indexOf("--no-session-persistence")).toBeLessThan(
      __testing.CLAUDE_CLI_USAGE_ARGS.indexOf("-p"),
    );
  });

  it("scrubs ambient Claude auth overrides before running the official CLI", () => {
    const env = __testing.claudeCliEnvironment("/tmp/claude-home", {
      HOME: "/tmp/ambient-home",
      PATH: "/bin",
      HTTPS_PROXY: "http://proxy.example",
      NODE_EXTRA_CA_CERTS: "/tmp/root-ca.pem",
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
    } as NodeJS.ProcessEnv);

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
