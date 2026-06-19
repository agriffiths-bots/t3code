import { ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import { ScheduledTask, ScheduledTaskId, toScheduleEntry } from "./ScheduledTasks.ts";

const makeTask = (overrides: Partial<ScheduledTask> = {}): ScheduledTask =>
  ({
    taskId: ScheduledTaskId.make("task-1"),
    threadId: ThreadId.make("thread-1"),
    prompt: "run the report",
    scheduleKind: "interval",
    intervalSeconds: 60,
    cronExpr: null,
    timezoneName: "UTC",
    enabled: 1,
    busyPolicy: "skip",
    nextRunAt: "2026-06-17T10:00:00.000Z",
    lastRunAt: null,
    lastStatus: null,
    lastError: null,
    skippedCount: 0,
    retryCount: 0,
    queuedCount: 0,
    createdAt: "2026-06-17T09:00:00.000Z",
    ...overrides,
  }) satisfies ScheduledTask;

describe("toScheduleEntry", () => {
  it("maps enabled 1 to boolean true", () => {
    expect(toScheduleEntry(makeTask({ enabled: 1 })).enabled).toBe(true);
  });

  it("maps enabled 0 to boolean false", () => {
    expect(toScheduleEntry(makeTask({ enabled: 0 })).enabled).toBe(false);
  });

  it("projects the row timezoneName onto the wire `timezone` field", () => {
    const entry = toScheduleEntry(makeTask({ timezoneName: "Europe/London" }));
    expect(entry.timezone).toBe("Europe/London");
    expect("timezoneName" in entry).toBe(false);
  });

  it("drops the internal liveness counters from the wire shape", () => {
    const entry = toScheduleEntry(
      makeTask({
        skippedCount: 3,
        retryCount: 2,
        queuedCount: 1,
        lastError: "boom",
        createdAt: "2026-06-17T09:00:00.000Z",
      }),
    );
    const keys = Object.keys(entry).sort();
    expect(keys).toStrictEqual(
      [
        "busyPolicy",
        "cronExpr",
        "enabled",
        "intervalSeconds",
        "lastRunAt",
        "lastStatus",
        "nextRunAt",
        "prompt",
        "scheduleKind",
        "taskId",
        "threadId",
        "timezone",
      ].sort(),
    );
    expect("skippedCount" in entry).toBe(false);
    expect("retryCount" in entry).toBe(false);
    expect("queuedCount" in entry).toBe(false);
    expect("lastError" in entry).toBe(false);
    expect("createdAt" in entry).toBe(false);
  });

  it("passes through the identifying + cadence fields unchanged (MCP/ws parity)", () => {
    const entry = toScheduleEntry(
      makeTask({
        taskId: ScheduledTaskId.make("task-42"),
        threadId: ThreadId.make("thread-42"),
        prompt: "summarize the inbox",
        scheduleKind: "cron",
        intervalSeconds: null,
        cronExpr: "0 7 * * 1-5",
        busyPolicy: "queue_once",
        nextRunAt: "2026-06-18T07:00:00.000Z",
        lastRunAt: "2026-06-17T07:00:00.000Z",
        lastStatus: "ok",
      }),
    );
    expect(entry).toStrictEqual({
      taskId: "task-42",
      threadId: "thread-42",
      prompt: "summarize the inbox",
      scheduleKind: "cron",
      intervalSeconds: null,
      cronExpr: "0 7 * * 1-5",
      timezone: "UTC",
      enabled: true,
      busyPolicy: "queue_once",
      nextRunAt: "2026-06-18T07:00:00.000Z",
      lastRunAt: "2026-06-17T07:00:00.000Z",
      lastStatus: "ok",
    });
  });
});
