import type { EnvironmentId, ScheduledTaskEntry, ScheduledTaskId, ThreadId } from "@t3tools/contracts";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vite-plus/test";

// The Card is interactive (navigate + atom commands) but its presentation is a
// pure function of props. Stub the router + atom-command + atom-target imports
// so we can render it to static markup and assert the design contract
// (design.md SURFACE 1 + UNIT TEST PLAN) without a live runtime.
vi.mock("@tanstack/react-router", () => ({ useNavigate: () => () => {} }));
vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: () => async () => ({ _tag: "Success" }),
}));
vi.mock("../../state/schedules", () => ({
  scheduledTasksEnvironment: { setEnabled: {}, delete: {} },
}));

import { ScheduledTaskCard, type ScheduledTaskCardProps } from "./ScheduledTaskCard";

const ENV = "env-1" as EnvironmentId;

const baseTask: ScheduledTaskEntry = {
  taskId: "task-1" as ScheduledTaskId,
  threadId: "thread-1" as ThreadId,
  prompt: "summarize the inbox",
  scheduleKind: "interval",
  intervalSeconds: 1800,
  cronExpr: null,
  timezone: "UTC",
  enabled: true,
  busyPolicy: "skip",
  nextRunAt: "2030-01-01T00:00:00.000Z",
  lastRunAt: null,
  lastStatus: null,
};

function render(overrides: Partial<ScheduledTaskCardProps> = {}): string {
  const props: ScheduledTaskCardProps = {
    environmentId: ENV,
    task: baseTask,
    threadTitle: "Inbox digest",
    workspaceLabel: "acme · main",
    overdue: false,
    lastStatusFailed: false,
    ...overrides,
  };
  return renderToStaticMarkup(<ScheduledTaskCard {...props} />);
}

describe("ScheduledTaskCard", () => {
  it("renders the title at card-frame size (text-sm), not CardTitle text-lg", () => {
    const html = render();
    expect(html).toContain('data-slot="card-frame-title"');
    expect(html).toContain("text-sm");
    expect(html).not.toContain("text-lg");
  });

  it("healthy interval → success Badge + ClockIcon, humanized cadence label", () => {
    const html = render();
    expect(html).toContain("lucide-clock");
    expect(html).toContain("bg-success");
    expect(html).toContain("Every 30 min");
    expect(html).not.toContain("bg-warning");
  });

  it("cron → info Badge with humanized prose, raw cron never the visible label", () => {
    const html = render({
      task: {
        ...baseTask,
        scheduleKind: "cron",
        intervalSeconds: null,
        cronExpr: "0 7 * * 1-5",
      },
    });
    expect(html).toContain("bg-info");
    expect(html).toContain("Mon–Fri 07:00");
    // The raw cron is reserved for the tooltip; it must not be the visible Badge label.
    expect(html).not.toContain(">0 7 * * 1-5<");
  });

  it("disabled → outline Badge + PAUSED tag at full contrast (no opacity-dim on the card root)", () => {
    const html = render({ task: { ...baseTask, enabled: false } });
    expect(html).toContain('data-enabled="false"');
    expect(html).toContain("Paused");
    expect(html).toContain("uppercase tracking-wider");
    // Disabled is NOT dimmed via opacity on the card itself (WCAG: full-contrast title).
    expect(html).not.toContain('opacity-64" data-slot="card"');
    // The disabled cadence Badge is the outline variant, not success/info/warning.
    expect(html).not.toContain("bg-success");
    expect(html).not.toContain("bg-info");
    expect(html).not.toContain("bg-warning");
  });

  it("overdue + failed → TriangleAlert leading icon, warning Badge, error line", () => {
    const html = render({
      task: {
        ...baseTask,
        nextRunAt: "2020-01-01T00:00:00.000Z",
        lastRunAt: "2020-01-01T00:00:00.000Z",
        lastStatus: "error",
      },
      overdue: true,
      lastStatusFailed: true,
    });
    expect(html).toContain('data-overdue="true"');
    expect(html).toContain("lucide-triangle-alert");
    expect(html).toContain("bg-warning");
    expect(html).toContain("destructive-foreground");
    expect(html).toContain("last run failed");
  });

  it("exposes an explicit enable/disable Switch aria-label naming the task", () => {
    expect(render()).toContain('aria-label="Disable Inbox digest"');
    expect(render({ task: { ...baseTask, enabled: false } })).toContain(
      'aria-label="Enable Inbox digest"',
    );
  });
});
