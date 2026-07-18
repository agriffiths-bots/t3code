import { assert, it } from "@effect/vitest";
import {
  NonNegativeInt,
  ScheduledTaskId,
  type ScheduledTasksStreamItem,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

import { scopeScheduledTaskStreamForAudience } from "./scheduledTaskAudienceStream.ts";

it.effect("suppresses schedule refreshes and rebases the audience-visible sequence densely", () =>
  Effect.gen(function* () {
    const task = {
      taskId: ScheduledTaskId.make("schedule-visible"),
      threadId: ThreadId.make("thread-visible"),
      prompt: "Visible task",
      scheduleKind: "interval",
      intervalSeconds: 3_600,
      cronExpr: null,
      timezone: "UTC",
      enabled: true,
      busyPolicy: "skip" as const,
      nextRunAt: null,
      lastRunAt: null,
      lastStatus: null,
      modelSelection: null,
    };
    const snapshot = (sequence: number, prompt: string): ScheduledTasksStreamItem => ({
      kind: "snapshot",
      snapshot: {
        sequence: NonNegativeInt.make(sequence),
        tasks: [{ ...task, prompt }],
      },
    });

    const items = yield* scopeScheduledTaskStreamForAudience(
      Stream.make(snapshot(0, "Visible task"), snapshot(1, "Visible task"), snapshot(2, "Changed")),
    ).pipe(Stream.runCollect);

    assert.deepEqual(
      Array.from(items).map((item) => (item.kind === "snapshot" ? item.snapshot.sequence : -1)),
      [0, 1],
    );
  }),
);
