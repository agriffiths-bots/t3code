import { describe, expect, it } from "vite-plus/test";

import {
  formatPlanUsageReset,
  formatPlanUsageStaleAt,
  formatPlanUsageValue,
  planUsageColor,
  selectMostConstrainedPlanUsageWindow,
} from "./PlanUsageMeter.logic";

describe("PlanUsageMeter logic", () => {
  it("selects the closest-to-exhaustion window", () => {
    const selected = selectMostConstrainedPlanUsageWindow({
      updatedAt: "2026-07-03T12:00:00.000Z",
      providers: [
        {
          provider: "codex",
          plan: "pro",
          windows: [
            {
              id: "codex-five-hour",
              provider: "codex",
              kind: "five_hour",
              title: "Codex 5h",
              usedPercent: 77,
              resetAt: null,
              used: null,
              limit: null,
              unit: null,
              severity: null,
            },
          ],
        },
        {
          provider: "claude",
          plan: "max",
          windows: [
            {
              id: "claude-weekly",
              provider: "claude",
              kind: "weekly",
              title: "Claude weekly",
              usedPercent: 84,
              resetAt: null,
              used: null,
              limit: null,
              unit: null,
              severity: "warning",
            },
          ],
        },
      ],
    });

    expect(selected?.id).toBe("claude-weekly");
  });

  it("uses the most-consumed limit for the collapsed meter", () => {
    const selected = selectMostConstrainedPlanUsageWindow({
      updatedAt: "2026-07-03T12:00:00.000Z",
      providers: [
        {
          provider: "codex",
          plan: "pro",
          windows: [
            {
              id: "codex-weekly",
              provider: "codex",
              kind: "weekly",
              title: "Codex weekly",
              usedPercent: 50,
              resetAt: null,
              used: null,
              limit: null,
              unit: null,
              severity: null,
            },
          ],
        },
        {
          provider: "claude",
          plan: "max",
          windows: [
            {
              id: "claude-weekly-scoped",
              provider: "claude",
              kind: "weekly_scoped",
              title: "Claude Weekly Scoped Fable",
              usedPercent: 21,
              resetAt: null,
              used: null,
              limit: null,
              unit: null,
              severity: "critical",
            },
          ],
        },
      ],
    });

    expect(selected?.id).toBe("codex-weekly");
  });

  it("formats reset and absolute usage text", () => {
    expect(
      formatPlanUsageReset("2026-07-03T14:30:00.000Z", Date.parse("2026-07-03T12:00:00.000Z")),
    ).toBe("Resets in 3h");
    expect(
      formatPlanUsageValue({
        id: "codex",
        provider: "codex",
        kind: "weekly",
        title: "Codex weekly",
        usedPercent: 7.5,
        resetAt: null,
        used: 3,
        limit: 10,
        unit: "credits",
        severity: null,
      }),
    ).toBe("7.5% · 3/10 credits");
  });

  it("formats stale usage snapshots", () => {
    expect(
      formatPlanUsageStaleAt("2026-07-03T11:42:00.000Z", Date.parse("2026-07-03T12:00:00.000Z")),
    ).toBe("Last good 18m ago");
    expect(formatPlanUsageStaleAt(undefined, Date.parse("2026-07-03T12:00:00.000Z"))).toBeNull();
  });

  it("prefers provider severity for usage colors", () => {
    expect(
      planUsageColor({
        id: "claude-weekly-scoped",
        provider: "claude",
        kind: "weekly_scoped",
        title: "Claude Weekly Scoped Fable",
        usedPercent: 21,
        resetAt: null,
        used: null,
        limit: null,
        unit: null,
        severity: "critical",
      }),
    ).toBe("var(--color-red-500)");
    expect(
      planUsageColor({
        id: "codex-weekly",
        provider: "codex",
        kind: "weekly",
        title: "Codex weekly",
        usedPercent: 50,
        resetAt: null,
        used: null,
        limit: null,
        unit: null,
        severity: null,
      }),
    ).toBe("var(--color-emerald-500)");
  });
});
