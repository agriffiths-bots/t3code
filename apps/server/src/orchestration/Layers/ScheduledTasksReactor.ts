/**
 * ScheduledTasksReactor implementation - see Services/ScheduledTasksReactor.ts
 * and finalPlan §7. Mirrors ProviderSessionReaper.start(): a forked sweep that
 * `Effect.catchCause(logWarning)` then `Effect.repeat(Schedule.spaced(...))`.
 */
import {
  CommandId,
  MessageId,
  type OrchestrationThreadShell,
  type ThreadId,
} from "@t3tools/contracts";
import { Cron } from "croner";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { ChildThreadCoordinator } from "../Services/ChildThreadCoordinator.ts";
import {
  BootstrapTurnStartDispatcher,
  type BootstrapTurnStartDispatcherShape,
} from "../Services/BootstrapTurnStartDispatcher.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  ScheduledTaskRepository,
  type ScheduledTask,
} from "../../persistence/Services/ScheduledTasks.ts";
import {
  SCHED_TICK_SECONDS,
  ScheduledTasksReactor,
  type ScheduledTasksReactorShape,
} from "../Services/ScheduledTasksReactor.ts";

/** A failed dispatch increments retry_count; the task is disabled past this. */
const MAX_RETRIES = 5;

/** Hard bound on a single dispatch so a stuck turn never stalls the sweep. */
const DISPATCH_TIMEOUT_SECONDS = 30;

/**
 * When a task is skipped (busy / pending injections) we nudge `next_run_at`
 * forward by this small amount so the same row is not re-listed every tick yet
 * the schedule still fires promptly once the thread frees up.
 */
const SKIP_ADVANCE_SECONDS = SCHED_TICK_SECONDS;

/**
 * Clock-skew guard: a `next_run_at` further than this into the FUTURE for a row
 * `listDue` returned indicates a corrupt/clock-skewed value; we log and skip
 * rather than fire it.
 */
const CLOCK_SKEW_FUTURE_TOLERANCE_MS = 60 * 60 * 1_000;

// Validate a cron expression by attempting to construct it; croner throws on a
// malformed pattern. Returns false on any failure so the caller can disable.
const isValidCron = (cronExpr: string | null, timezone: string): boolean => {
  if (cronExpr === null) return false;
  try {
    const _cron = new Cron(cronExpr, { timezone });
    return true;
  } catch {
    return false;
  }
};

const makeScheduledTasksReactor = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const dispatcher = yield* BootstrapTurnStartDispatcher;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const coordinator = yield* ChildThreadCoordinator;
  const repository = yield* ScheduledTaskRepository;
  const sql = yield* SqlClient;

  const nowMillis = Effect.clockWith((clock) => clock.currentTimeMillis);
  const randomUUID = crypto.randomUUIDv4.pipe(Effect.orDie);
  const newCommandId = randomUUID.pipe(
    Effect.map((uuid) => CommandId.make(`server:scheduled-task:${uuid}`)),
  );
  const dateTimeOf = (millis: number) => Option.getOrThrow(DateTime.make(millis));
  const isoOf = (millis: number) => DateTime.formatIso(dateTimeOf(millis));

  // Once-breaker: only log a healthcheck failure the first time it trips so a
  // persistently unavailable database does not spam the log every tick.
  let healthcheckBroken = false;

  // Compute the next run instant (epoch ms) for a task, in its own timezone.
  // `interval` is wall-clock-relative to `nowMs`; `cron` is parsed via croner.
  const computeNextRunMs = (task: ScheduledTask, nowMs: number): number | null => {
    if (task.scheduleKind === "interval") {
      const seconds = task.intervalSeconds;
      if (seconds === null || seconds <= 0) return null;
      return nowMs + seconds * 1_000;
    }
    if (task.cronExpr === null) return null;
    const cron = new Cron(task.cronExpr, { timezone: task.timezoneName });
    const next = cron.nextRun(DateTime.toDateUtc(dateTimeOf(nowMs)));
    return next === null ? null : next.getTime();
  };

  const disableTask = (task: ScheduledTask, lastStatus: string, error: string, nowMs: number) =>
    repository.update({
      ...task,
      enabled: 0,
      lastStatus,
      lastError: error,
      lastRunAt: isoOf(nowMs),
      nextRunAt: null,
    });

  // Skip this trigger and nudge next_run_at forward a small amount.
  const skipTask = (
    task: ScheduledTask,
    lastStatus: string,
    nowMs: number,
    counters?: Partial<Pick<ScheduledTask, "skippedCount" | "queuedCount">>,
  ) =>
    repository.update({
      ...task,
      lastStatus,
      nextRunAt: isoOf(nowMs + SKIP_ADVANCE_SECONDS * 1_000),
      ...counters,
    });

  // Dispatch the stored prompt to the stored thread, bounded by a timeout. The
  // next_run_at advance is committed via markRun BEFORE this awaits, so a crash
  // here cannot re-run the trigger.
  const dispatchTurn = (task: ScheduledTask, shell: OrchestrationThreadShell) =>
    Effect.gen(function* () {
      const commandId = yield* newCommandId;
      const messageId = MessageId.make(yield* randomUUID);
      const createdAt = DateTime.formatIso(yield* DateTime.now);
      yield* dispatcher.dispatch({
        type: "thread.turn.start",
        commandId,
        threadId: task.threadId,
        message: { messageId, role: "user", text: task.prompt, attachments: [] },
        // A schedule may pin its own model/harness (Fix 1); dispatch it as a
        // per-turn override. Fall back to the thread's current model so an
        // unpinned run always re-asserts the thread's model explicitly and never
        // inherits a selection cached in-process by a different pinned schedule
        // that fired on the same thread.
        modelSelection: task.modelSelection ?? shell.modelSelection,
        runtimeMode: shell.runtimeMode,
        interactionMode: shell.interactionMode,
        bootstrap: undefined,
        createdAt,
      });
    }).pipe(Effect.timeout(Duration.seconds(DISPATCH_TIMEOUT_SECONDS)));

  // Fire a due task: advance next_run_at (committed) then dispatch.
  const runTask = (task: ScheduledTask, shell: OrchestrationThreadShell, nowMs: number) =>
    Effect.gen(function* () {
      const nextRunMs = computeNextRunMs(task, nowMs);
      const lastRunAt = isoOf(nowMs);
      // Commit the advance up front (BEGIN IMMEDIATE inside markRun) so the
      // trigger is consumed even if the dispatch below crashes (at-most-once).
      yield* repository.markRun({
        taskId: task.taskId,
        status: "dispatched",
        lastRunAt,
        nextRunAt: nextRunMs === null ? null : isoOf(nextRunMs),
      });
      yield* dispatchTurn(task, shell).pipe(
        Effect.matchCauseEffect({
          onSuccess: () =>
            // Reset the per-run counters now the dispatch succeeded.
            repository.update({
              ...task,
              lastStatus: "dispatched",
              lastError: null,
              lastRunAt,
              nextRunAt: nextRunMs === null ? null : isoOf(nextRunMs),
              skippedCount: 0,
              queuedCount: 0,
              retryCount: 0,
            }),
          onFailure: (cause) => {
            const retryCount = task.retryCount + 1;
            const disable = retryCount > MAX_RETRIES;
            return repository
              .update({
                ...task,
                enabled: disable ? 0 : task.enabled,
                lastStatus: "error",
                lastError: Cause.pretty(cause).slice(0, 1_000),
                lastRunAt,
                nextRunAt: disable ? null : nextRunMs === null ? null : isoOf(nextRunMs),
                retryCount,
              })
              .pipe(
                Effect.andThen(
                  Effect.logWarning("scheduled.task.dispatch-failed", {
                    taskId: task.taskId,
                    threadId: task.threadId,
                    retryCount,
                    disabled: disable,
                    cause: Cause.pretty(cause),
                  }),
                ),
              );
          },
        }),
      );
    });

  // Decide what to do with a single due task: validate the thread, guard cron,
  // honour the busy policy, otherwise fire it.
  const processTask = (task: ScheduledTask, nowMs: number) =>
    Effect.gen(function* () {
      // Clock-skew guard: a future-dated row that listDue still returned (string
      // comparison anomaly) must not be fired this tick.
      if (task.nextRunAt !== null) {
        const nextMs = Date.parse(task.nextRunAt);
        if (!Number.isNaN(nextMs) && nextMs - nowMs > CLOCK_SKEW_FUTURE_TOLERANCE_MS) {
          yield* Effect.logWarning("scheduled.task.clock-skew-skip", {
            taskId: task.taskId,
            nextRunAt: task.nextRunAt,
          });
          return;
        }
      }

      // Validate the target thread still exists; a deleted thread disables the
      // task without rescheduling.
      const shellOption = yield* projectionSnapshotQuery
        .getThreadShellById(task.threadId)
        .pipe(Effect.orDie);
      if (Option.isNone(shellOption)) {
        yield* disableTask(task, "error", "thread deleted", nowMs);
        return;
      }
      const shell = shellOption.value;

      // Avoid racing the coordinator's pending-injection drain on the same
      // thread: skip this tick and nudge forward.
      const hasPending = yield* coordinator.hasPendingInjections(task.threadId);
      if (hasPending) {
        yield* skipTask(task, "skipped", nowMs);
        return;
      }

      // Validate cron at tick time too; an invalid expression disables the task.
      if (task.scheduleKind === "cron" && !isValidCron(task.cronExpr, task.timezoneName)) {
        yield* disableTask(task, "error", `invalid cron expression: ${task.cronExpr}`, nowMs);
        return;
      }

      // Busy check: a running session or an active turn is "busy".
      const busy = shell.session?.status === "running" || shell.session?.activeTurnId != null;
      if (busy) {
        if (task.busyPolicy === "skip") {
          yield* skipTask(task, "skipped", nowMs, { skippedCount: task.skippedCount + 1 });
          return;
        }
        // queue_once: cap at a single queued run so a long-busy thread cannot
        // accumulate an unbounded backlog.
        if (task.queuedCount >= 1) {
          yield* skipTask(task, "skipped", nowMs, { skippedCount: task.skippedCount + 1 });
          return;
        }
        yield* runTask({ ...task, queuedCount: task.queuedCount + 1 }, shell, nowMs);
        return;
      }

      yield* runTask(task, shell, nowMs);
    });

  const tick = Effect.gen(function* () {
    // (1) Healthcheck with a once-breaker.
    const healthy = yield* sql`SELECT 1`.pipe(
      Effect.as(true),
      Effect.catchCause((cause) =>
        Effect.gen(function* () {
          if (!healthcheckBroken) {
            healthcheckBroken = true;
            yield* Effect.logWarning("scheduled.task.healthcheck-failed", {
              cause: Cause.pretty(cause),
            });
          }
          return false;
        }),
      ),
    );
    if (!healthy) return;
    if (healthcheckBroken) {
      healthcheckBroken = false;
      yield* Effect.logInfo("scheduled.task.healthcheck-recovered");
    }

    const nowMs = yield* nowMillis;
    const due = yield* repository.listDue({ nowIso: isoOf(nowMs) }).pipe(Effect.orDie);

    // Process sequentially: each task takes a write lock via markRun, and the
    // ordering keeps the sweep deterministic.
    for (const task of due) {
      yield* processTask(task, nowMs).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("scheduled.task.process-failed", {
            taskId: task.taskId,
            cause: Cause.pretty(cause),
          }),
        ),
      );
    }
  });

  const start: ScheduledTasksReactorShape["start"] = () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(
        tick.pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("scheduled.task.tick-failed", {
              cause: Cause.pretty(cause),
            }),
          ),
          Effect.repeat(Schedule.spaced(Duration.seconds(SCHED_TICK_SECONDS))),
        ),
      );

      yield* Effect.logInfo("scheduled.task.reactor.started", {
        tickSeconds: SCHED_TICK_SECONDS,
      });
    });

  return {
    start,
  } satisfies ScheduledTasksReactorShape;
});

export const ScheduledTasksReactorLive = Layer.effect(
  ScheduledTasksReactor,
  makeScheduledTasksReactor,
);

// Referenced for type alignment with the dispatcher shape used to fire turns.
export type { BootstrapTurnStartDispatcherShape };

// Re-exported so test code can reference the thread-id brand without importing
// from the contracts package directly in the reactor module surface.
export type { ThreadId };
