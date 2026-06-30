import { ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import {
  ScheduledTask,
  ScheduledTaskId,
  ScheduledTaskRepository,
} from "../Services/ScheduledTasks.ts";
import { ScheduledTaskRepositoryLive } from "./ScheduledTasks.ts";
import { SqlitePersistenceMemory } from "./Sqlite.ts";

const layer = it.layer(
  ScheduledTaskRepositoryLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);

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

layer("ScheduledTaskRepository", (it) => {
  it.effect("inserts and lists due tasks", () =>
    Effect.gen(function* () {
      const repository = yield* ScheduledTaskRepository;
      const threadId = ThreadId.make("thread-listdue");

      yield* repository.insert(makeTask({ taskId: ScheduledTaskId.make("listdue-now"), threadId }));
      yield* repository.insert(
        makeTask({
          taskId: ScheduledTaskId.make("listdue-future"),
          threadId,
          nextRunAt: "2026-06-17T12:00:00.000Z",
        }),
      );
      yield* repository.insert(
        makeTask({
          taskId: ScheduledTaskId.make("listdue-disabled"),
          threadId,
          enabled: 0,
        }),
      );

      const due = yield* repository.listDue({ nowIso: "2026-06-17T10:30:00.000Z" });
      const dueForThread = due.filter((task) => task.threadId === threadId);
      assert.equal(dueForThread.length, 1);
      assert.equal(dueForThread[0]?.taskId, "listdue-now");

      const byThread = yield* repository.listByThread({ threadId });
      assert.equal(byThread.length, 3);
    }),
  );

  it.effect("markRun advances next_run_at and records status atomically", () =>
    Effect.gen(function* () {
      const repository = yield* ScheduledTaskRepository;
      const threadId = ThreadId.make("thread-markrun");
      const taskId = ScheduledTaskId.make("markrun-task");

      yield* repository.insert(makeTask({ taskId, threadId }));

      yield* repository.markRun({
        taskId,
        status: "dispatched",
        lastRunAt: "2026-06-17T10:00:00.000Z",
        nextRunAt: "2026-06-17T11:00:00.000Z",
      });

      const stillDueAtRun = yield* repository.listDue({ nowIso: "2026-06-17T10:30:00.000Z" });
      assert.equal(
        stillDueAtRun.some((task) => task.taskId === taskId),
        false,
      );

      const dueAfterAdvance = yield* repository.listDue({ nowIso: "2026-06-17T11:30:00.000Z" });
      const advanced = dueAfterAdvance.find((task) => task.taskId === taskId);
      assert.equal(advanced?.lastStatus, "dispatched");
      assert.equal(advanced?.lastRunAt, "2026-06-17T10:00:00.000Z");
      assert.equal(advanced?.nextRunAt, "2026-06-17T11:00:00.000Z");
    }),
  );

  it.effect("markRun records error text", () =>
    Effect.gen(function* () {
      const repository = yield* ScheduledTaskRepository;
      const threadId = ThreadId.make("thread-markrun-error");
      const taskId = ScheduledTaskId.make("markrun-error-task");

      yield* repository.insert(makeTask({ taskId, threadId }));
      yield* repository.markRun({
        taskId,
        status: "error",
        lastRunAt: "2026-06-17T10:00:00.000Z",
        nextRunAt: "2026-06-17T11:00:00.000Z",
        error: "dispatch timed out",
      });

      const byThread = yield* repository.listByThread({ threadId });
      assert.equal(byThread[0]?.lastStatus, "error");
      assert.equal(byThread[0]?.lastError, "dispatch timed out");
    }),
  );

  it.effect("delete hard-removes the row", () =>
    Effect.gen(function* () {
      const repository = yield* ScheduledTaskRepository;
      const threadId = ThreadId.make("thread-delete");
      const taskId = ScheduledTaskId.make("delete-task");

      yield* repository.insert(makeTask({ taskId, threadId }));
      yield* repository.delete({ taskId });

      const byThread = yield* repository.listByThread({ threadId });
      assert.equal(byThread.length, 0);
    }),
  );
});
