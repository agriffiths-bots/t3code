import type { ScheduledTaskEntry, ScheduledTaskId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { isScheduleOverdue, reduceSchedulesByThreadId } from "./schedules";

const NOW = Date.parse("2026-06-19T12:00:00.000Z");

const taskId = (value: string) => value as ScheduledTaskId;
const threadId = (value: string) => value as ThreadId;

function task(
  overrides: { taskId: string; threadId: string } & Partial<
    Omit<ScheduledTaskEntry, "taskId" | "threadId">
  >,
): ScheduledTaskEntry {
  const base: ScheduledTaskEntry = {
    taskId: taskId("task"),
    threadId: threadId("thread"),
    prompt: "do the thing",
    scheduleKind: "interval",
    intervalSeconds: 1800,
    cronExpr: null,
    timezone: "UTC",
    enabled: true,
    busyPolicy: "skip",
    nextRunAt: null,
    lastRunAt: null,
    lastStatus: null,
    modelSelection: null,
  };
  return {
    ...base,
    ...overrides,
    taskId: taskId(overrides.taskId),
    threadId: threadId(overrides.threadId),
  };
}

describe("reduceSchedulesByThreadId", () => {
  it("groups by thread keeping the earliest upcoming run and a count", () => {
    const map = reduceSchedulesByThreadId(
      [
        task({ taskId: "a", threadId: "T1", nextRunAt: "2026-06-19T13:00:00.000Z" }),
        task({ taskId: "b", threadId: "T1", nextRunAt: "2026-06-19T12:30:00.000Z" }),
        task({ taskId: "c", threadId: "T2", nextRunAt: "2026-06-19T14:00:00.000Z" }),
      ],
      NOW,
    );

    expect(map.size).toBe(2);
    expect(map.get(threadId("T1"))?.count).toBe(2);
    expect(map.get(threadId("T1"))?.nextRunAt).toBe("2026-06-19T12:30:00.000Z");
    expect(map.get(threadId("T2"))?.count).toBe(1);
  });

  it("marks a thread disabled when its only schedule is disabled", () => {
    const map = reduceSchedulesByThreadId(
      [
        task({
          taskId: "a",
          threadId: "T1",
          enabled: false,
          nextRunAt: "2026-06-19T13:00:00.000Z",
        }),
      ],
      NOW,
    );
    expect(map.get(threadId("T1"))?.enabled).toBe(false);
    expect(map.get(threadId("T1"))?.nextRunAt).toBeNull();
  });
});

describe("isScheduleOverdue", () => {
  it("is overdue only when the last run failed", () => {
    expect(
      isScheduleOverdue(
        { enabled: true, nextRunAt: "2026-06-19T13:00:00.000Z", lastStatus: "error" },
        NOW,
      ),
    ).toBe(true);
  });

  it("is overdue when nextRunAt slipped past the reactor grace", () => {
    // 16s in the past -> beyond the 15s grace window.
    expect(
      isScheduleOverdue(
        { enabled: true, nextRunAt: "2026-06-19T11:59:44.000Z", lastStatus: "ok" },
        NOW,
      ),
    ).toBe(true);
  });

  it("is not overdue inside the grace window (reactor in flight)", () => {
    // 5s in the past -> inside the 15s grace window.
    expect(
      isScheduleOverdue(
        { enabled: true, nextRunAt: "2026-06-19T11:59:55.000Z", lastStatus: "ok" },
        NOW,
      ),
    ).toBe(false);
  });

  it("is never overdue when disabled", () => {
    expect(
      isScheduleOverdue(
        { enabled: false, nextRunAt: "2026-06-19T10:00:00.000Z", lastStatus: "error" },
        NOW,
      ),
    ).toBe(false);
  });
});
