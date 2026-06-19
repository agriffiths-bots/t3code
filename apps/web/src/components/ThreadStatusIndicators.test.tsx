import { ThreadId } from "@t3tools/contracts";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import type { ThreadScheduleSummary } from "../state/schedules";
import { ScheduledTaskIcon, ThreadWorktreeIndicator, scheduleIconPresentation } from "./ThreadStatusIndicators";

const summary = (overrides: Partial<ThreadScheduleSummary> = {}): ThreadScheduleSummary => ({
  threadId: "T1" as ThreadId,
  nextRunAt: "2030-01-01T00:00:00.000Z",
  enabled: true,
  overdue: false,
  lastStatusFailed: false,
  count: 1,
  cadenceLabel: "Every 30 min",
  ...overrides,
});

describe("ThreadWorktreeIndicator", () => {
  it("renders the worktree folder and branch in an accessible label", () => {
    const markup = renderToStaticMarkup(
      <ThreadWorktreeIndicator
        thread={{
          id: ThreadId.make("thread-1"),
          branch: "feature/sidebar-indicator",
          worktreePath: "/tmp/worktrees/sidebar-indicator",
        }}
      />,
    );

    expect(markup).toContain('role="img"');
    expect(markup).toContain(
      'aria-label="Worktree: sidebar-indicator (feature/sidebar-indicator)"',
    );
    expect(markup).toContain('data-testid="thread-worktree-thread-1"');
  });

  it.each([null, "", "   "])("renders nothing for an absent worktree path", (worktreePath) => {
    const markup = renderToStaticMarkup(
      <ThreadWorktreeIndicator
        thread={{
          id: ThreadId.make("thread-1"),
          branch: "main",
          worktreePath,
        }}
      />,
    );

    expect(markup).toBe("");
  });
});

describe("scheduleIconPresentation", () => {
  it("healthy → ClockIcon + text-info with a next-run label", () => {
    const presentation = scheduleIconPresentation(summary());
    expect(presentation.colorClass).toBe("text-info");
    expect(presentation.label).toContain("next run");
  });

  it("overdue + failed → TriangleAlertIcon + text-warning + failed label", () => {
    const presentation = scheduleIconPresentation(
      summary({ overdue: true, lastStatusFailed: true }),
    );
    expect(presentation.colorClass).toBe("text-warning");
    expect(presentation.label).toBe("Scheduled · overdue (last run failed)");
  });

  it("disabled → dimmed muted-foreground + paused label", () => {
    const presentation = scheduleIconPresentation(summary({ enabled: false }));
    expect(presentation.colorClass).toBe("text-muted-foreground/50");
    expect(presentation.label).toBe("Scheduled · paused");
  });
});

describe("ScheduledTaskIcon", () => {
  it("renders nothing when the thread has no schedule", () => {
    expect(renderToStaticMarkup(<ScheduledTaskIcon summary={null} />)).toBe("");
    expect(renderToStaticMarkup(<ScheduledTaskIcon summary={undefined} />)).toBe("");
  });

  it("renders a ClockIcon text-info span (not a button) with a next-run aria-label when healthy", () => {
    const html = renderToStaticMarkup(<ScheduledTaskIcon summary={summary()} />);
    expect(html).toContain("<span");
    expect(html).not.toContain("<button");
    expect(html).toContain("text-info");
    expect(html).toContain("lucide-clock");
    expect(html).toContain('aria-label="Scheduled · next run');
  });

  it("swaps to a TriangleAlert text-warning span when overdue + failed", () => {
    const html = renderToStaticMarkup(
      <ScheduledTaskIcon summary={summary({ overdue: true, lastStatusFailed: true })} />,
    );
    expect(html).toContain("text-warning");
    expect(html).toContain("lucide-triangle-alert");
    expect(html).toContain('aria-label="Scheduled · overdue (last run failed)"');
  });

  it("dims the icon when disabled", () => {
    const html = renderToStaticMarkup(<ScheduledTaskIcon summary={summary({ enabled: false })} />);
    expect(html).toContain("text-muted-foreground/50");
    expect(html).toContain("lucide-clock");
    expect(html).toContain('aria-label="Scheduled · paused"');
  });
});
