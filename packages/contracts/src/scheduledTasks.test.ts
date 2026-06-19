import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import { ScheduledTaskEntry, ScheduledTasksStreamItem } from "./orchestration.ts";

const decodeEntry = Schema.decodeUnknownSync(ScheduledTaskEntry);
const encodeEntry = Schema.encodeSync(ScheduledTaskEntry);
const decodeStreamItem = Schema.decodeUnknownSync(ScheduledTasksStreamItem);

const sampleEntry = {
  taskId: "task-1",
  threadId: "thread-1",
  prompt: "summarize the inbox",
  scheduleKind: "interval",
  intervalSeconds: 1800,
  cronExpr: null,
  timezone: "UTC",
  enabled: true,
  busyPolicy: "skip",
  nextRunAt: "2026-06-19T14:30:00.000Z",
  lastRunAt: null,
  lastStatus: null,
};

describe("ScheduledTaskEntry", () => {
  it("round-trips a decoded entry", () => {
    const decoded = decodeEntry(sampleEntry);
    expect(decoded.taskId).toBe("task-1");
    expect(decoded.enabled).toBe(true);
    expect(encodeEntry(decoded)).toStrictEqual(sampleEntry);
  });
});

describe("ScheduledTasksStreamItem", () => {
  it("decodes the snapshot variant", () => {
    const item = decodeStreamItem({
      kind: "snapshot",
      snapshot: { sequence: 3, tasks: [sampleEntry] },
    });
    expect(item.kind).toBe("snapshot");
  });

  it("decodes the task-upserted variant", () => {
    const item = decodeStreamItem({ kind: "task-upserted", sequence: 4, task: sampleEntry });
    expect(item.kind).toBe("task-upserted");
  });

  it("decodes the task-removed variant", () => {
    const item = decodeStreamItem({ kind: "task-removed", sequence: 5, taskId: "task-1" });
    expect(item.kind).toBe("task-removed");
  });
});
