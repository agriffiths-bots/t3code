import type { ThreadId } from "@t3tools/contracts";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { removeLocalStorageItem } from "../../hooks/useLocalStorage";
import {
  dismissScheduleBanner,
  isScheduleBannerDismissed,
  SCHEDULE_BANNER_DISMISSALS_STORAGE_KEY,
} from "../../scheduleBannerDismissal";
import type { ThreadScheduleSummary } from "../../state/schedules";
import { buildScheduleBanner } from "./scheduleBanner";

const summary = (overrides: Partial<ThreadScheduleSummary> = {}): ThreadScheduleSummary => ({
  threadId: "T1" as ThreadId,
  nextRunAt: "2030-01-01T12:30:00.000Z",
  enabled: true,
  overdue: false,
  lastStatusFailed: false,
  count: 1,
  cadenceLabel: "Every 30 min",
  ...overrides,
});

afterEach(() => {
  removeLocalStorageItem(SCHEDULE_BANNER_DISMISSALS_STORAGE_KEY);
  vi.useRealTimers();
});

describe("buildScheduleBanner", () => {
  it("builds a banner only when an enabled schedule has a non-null nextRunAt", () => {
    const banner = buildScheduleBanner("env|T1", summary());
    expect(banner).not.toBeNull();
    expect(banner?.id).toBe("scheduled:env|T1");
    expect(banner?.dismissKey).toBe("env|T1:2030-01-01T12:30:00.000Z");
    expect(banner?.title).toContain("Scheduled task runs");
    expect(banner?.description).toContain("runs in this thread");
    expect(banner?.description).toContain("Every 30 min");
  });

  it("is suppressed when there is no active thread", () => {
    expect(buildScheduleBanner(null, summary())).toBeNull();
  });

  it("is suppressed when there is no schedule summary", () => {
    expect(buildScheduleBanner("env|T1", null)).toBeNull();
  });

  it("is suppressed when the schedule is disabled", () => {
    expect(buildScheduleBanner("env|T1", summary({ enabled: false }))).toBeNull();
  });

  it("is suppressed when there is no upcoming run", () => {
    expect(buildScheduleBanner("env|T1", summary({ nextRunAt: null }))).toBeNull();
  });

  it("overdue swaps the title + description but the descriptor stays the same shape (variant stays info in ChatView)", () => {
    const banner = buildScheduleBanner("env|T1", summary({ overdue: true }));
    expect(banner?.overdue).toBe(true);
    expect(banner?.title).toBe("Scheduled task is overdue");
    expect(banner?.description).toContain("will run when this thread is free");
  });

  it("summarizes multiple schedules on one thread with a +N more suffix on the earliest run", () => {
    const banner = buildScheduleBanner("env|T1", summary({ count: 3 }));
    // The reduced summary already carries the EARLIEST nextRunAt; the banner
    // appends the count of the remaining schedules.
    expect(banner?.dismissKey).toBe("env|T1:2030-01-01T12:30:00.000Z");
    expect(banner?.description).toContain("+2 more");
  });

  it("a single schedule does not append a +N more suffix", () => {
    const banner = buildScheduleBanner("env|T1", summary({ count: 1 }));
    expect(banner?.description).not.toContain("more");
  });

  it("the dismiss key embeds nextRunAt so a dismissal hides only the current run", () => {
    const banner = buildScheduleBanner("env|T1", summary())!;
    expect(isScheduleBannerDismissed(banner.dismissKey)).toBe(false);
    dismissScheduleBanner(banner.dismissKey);
    expect(isScheduleBannerDismissed(banner.dismissKey)).toBe(true);

    // When the reactor advances next_run_at, the descriptor key changes and the
    // banner is no longer dismissed (it re-surfaces for the new run).
    const advanced = buildScheduleBanner(
      "env|T1",
      summary({ nextRunAt: "2030-01-01T13:00:00.000Z" }),
    )!;
    expect(advanced.dismissKey).not.toBe(banner.dismissKey);
    expect(isScheduleBannerDismissed(advanced.dismissKey)).toBe(false);
  });
});
