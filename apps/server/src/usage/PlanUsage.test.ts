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
      providers: ["codex"],
      codexHome: "/tmp/codex-shadow",
      claudeHome: null,
    });
    expect(
      __testing.resolveUsageCredentialScope({
        settings,
        providerInstanceId: claudeInstanceId,
      }),
    ).toMatchObject({
      providers: ["claude"],
      codexHome: null,
      claudeHome: "/tmp/claude-home",
    });
  });

  it("uses configured Codex and Claude homes for aggregate usage", () => {
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
    };

    expect(
      __testing.resolveUsageCredentialScope({
        settings,
      }),
    ).toMatchObject({
      providers: ["codex", "claude"],
      codexHome: "/tmp/codex-shadow",
      claudeHome: "/tmp/claude-home",
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
      providers: [],
      codexHome: null,
      claudeHome: null,
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
      providers: ["codex", "claude"],
      codexHome: "/tmp/codex-default",
      claudeHome: "/tmp/claude-default",
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
      providers: ["codex"],
      codexHome: "/tmp/codex-default",
      claudeHome: null,
    });
  });
});
