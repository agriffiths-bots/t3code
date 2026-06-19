import { useAtomValue } from "@effect/atom-react";
import {
  createEnvironmentScheduledTasksAtoms,
  createScheduledTasksEnvironmentAtoms,
} from "@t3tools/client-runtime/state/schedules";
import type { EnvironmentId, ScheduledTaskEntry, ThreadId } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { Atom } from "effect/unstable/reactivity";

import { connectionAtomRuntime } from "../connection/runtime";
import { scheduleCadenceLabel } from "../scheduled/formatCadence";

export const environmentScheduledTasks =
  createEnvironmentScheduledTasksAtoms(connectionAtomRuntime);
export const scheduledTasksEnvironment =
  createScheduledTasksEnvironmentAtoms(connectionAtomRuntime);

/**
 * A fired task is only "overdue" when its last run FAILED or its `nextRunAt`
 * has slipped past `now` by more than one reactor tick (15s) of grace. Bare
 * staleness inside the grace window is the reactor in flight, not a problem —
 * the icon/badge must not escalate. (design.md ONE ACCENT, ONE GLYPH FAMILY.)
 */
const REACTOR_TICK_GRACE_MS = 15_000;

const FAILURE_STATUSES = new Set(["error", "failed", "failure", "timeout", "cancelled"]);

export function lastStatusIsFailure(lastStatus: string | null): boolean {
  return lastStatus !== null && FAILURE_STATUSES.has(lastStatus.toLowerCase());
}

export function isScheduleOverdue(
  task: Pick<ScheduledTaskEntry, "enabled" | "nextRunAt" | "lastStatus">,
  nowMs: number = Date.now(),
): boolean {
  if (!task.enabled) {
    return false;
  }
  if (lastStatusIsFailure(task.lastStatus)) {
    return true;
  }
  if (task.nextRunAt === null) {
    return false;
  }
  const due = new Date(task.nextRunAt).getTime();
  if (Number.isNaN(due)) {
    return false;
  }
  return nowMs - due > REACTOR_TICK_GRACE_MS;
}

/**
 * One row of the reduced per-thread schedule summary. Multiple schedules on a
 * single thread collapse to the EARLIEST upcoming run plus a count, the shape
 * the sidebar icon and composer banner both read.
 */
export interface ThreadScheduleSummary {
  readonly threadId: ThreadId;
  readonly nextRunAt: string | null;
  readonly enabled: boolean;
  readonly overdue: boolean;
  readonly lastStatusFailed: boolean;
  readonly count: number;
  /** Humanized cadence of the representative (earliest) schedule, e.g. "Every 30 min". */
  readonly cadenceLabel: string;
}

function compareNextRunAt(a: string | null, b: string | null): number {
  if (a === null) return b === null ? 0 : 1;
  if (b === null) return -1;
  return new Date(a).getTime() - new Date(b).getTime();
}

export function reduceSchedulesByThreadId(
  tasks: ReadonlyArray<ScheduledTaskEntry>,
  nowMs: number = Date.now(),
): ReadonlyMap<ThreadId, ThreadScheduleSummary> {
  const byThread = new Map<ThreadId, ThreadScheduleSummary>();
  for (const task of tasks) {
    const overdue = isScheduleOverdue(task, nowMs);
    const lastStatusFailed = lastStatusIsFailure(task.lastStatus);
    const existing = byThread.get(task.threadId);
    if (existing === undefined) {
      byThread.set(task.threadId, {
        threadId: task.threadId,
        nextRunAt: task.enabled ? task.nextRunAt : null,
        enabled: task.enabled,
        overdue,
        lastStatusFailed,
        count: 1,
        cadenceLabel: scheduleCadenceLabel(task),
      });
      continue;
    }
    // Prefer the earliest enabled upcoming run; a thread is "enabled" if any of
    // its schedules is enabled, and "overdue" if any enabled schedule is overdue.
    // The cadence label tracks whichever schedule owns the chosen earliest run.
    const takesNewRun =
      task.enabled && compareNextRunAt(task.nextRunAt, existing.nextRunAt) < 0;
    byThread.set(task.threadId, {
      threadId: task.threadId,
      nextRunAt: takesNewRun ? task.nextRunAt : existing.nextRunAt,
      enabled: existing.enabled || task.enabled,
      overdue: existing.overdue || overdue,
      lastStatusFailed: existing.lastStatusFailed || lastStatusFailed,
      count: existing.count + 1,
      cadenceLabel: takesNewRun ? scheduleCadenceLabel(task) : existing.cadenceLabel,
    });
  }
  return byThread;
}

function tasksForEnvironment(
  state: ReturnType<typeof environmentScheduledTasks.stateValueAtom> extends Atom.Atom<infer S>
    ? S
    : never,
): ReadonlyArray<ScheduledTaskEntry> {
  return Option.match(state.snapshot, {
    onNone: () => [],
    onSome: (snapshot) => snapshot.tasks,
  });
}

export const scheduledTasksForEnvironmentAtom = Atom.family((environmentId: EnvironmentId) =>
  Atom.make((get) => tasksForEnvironment(get(environmentScheduledTasks.stateValueAtom(environmentId)))).pipe(
    Atom.withLabel(`scheduled-tasks-for-environment:${environmentId}`),
  ),
);

export const schedulesByThreadIdAtom = Atom.family((environmentId: EnvironmentId) =>
  Atom.make((get) =>
    reduceSchedulesByThreadId(get(scheduledTasksForEnvironmentAtom(environmentId))),
  ).pipe(Atom.withLabel(`schedules-by-thread-id:${environmentId}`)),
);

export const enabledScheduleCountAtom = Atom.family((environmentId: EnvironmentId) =>
  Atom.make((get) =>
    get(scheduledTasksForEnvironmentAtom(environmentId)).filter((task) => task.enabled).length,
  ).pipe(Atom.withLabel(`enabled-schedule-count:${environmentId}`)),
);

export function useScheduledTasks(environmentId: EnvironmentId | null): ReadonlyArray<ScheduledTaskEntry> {
  return useAtomValue(
    environmentId !== null
      ? scheduledTasksForEnvironmentAtom(environmentId)
      : EMPTY_SCHEDULED_TASKS_ATOM,
  );
}

export function useSchedulesByThreadId(
  environmentId: EnvironmentId | null,
): ReadonlyMap<ThreadId, ThreadScheduleSummary> {
  return useAtomValue(
    environmentId !== null ? schedulesByThreadIdAtom(environmentId) : EMPTY_SCHEDULE_SUMMARY_ATOM,
  );
}

export function useThreadScheduleSummary(
  environmentId: EnvironmentId | null,
  threadId: ThreadId | null,
): ThreadScheduleSummary | null {
  const byThread = useSchedulesByThreadId(environmentId);
  return threadId === null ? null : (byThread.get(threadId) ?? null);
}

export function useEnabledScheduleCount(environmentId: EnvironmentId | null): number {
  return useAtomValue(
    environmentId !== null ? enabledScheduleCountAtom(environmentId) : EMPTY_SCHEDULE_COUNT_ATOM,
  );
}

const EMPTY_SCHEDULED_TASKS_ATOM = Atom.make<ReadonlyArray<ScheduledTaskEntry>>([]).pipe(
  Atom.withLabel("scheduled-tasks-for-environment:empty"),
);
const EMPTY_SCHEDULE_SUMMARY_ATOM = Atom.make<ReadonlyMap<ThreadId, ThreadScheduleSummary>>(
  new Map(),
).pipe(Atom.withLabel("schedules-by-thread-id:empty"));
const EMPTY_SCHEDULE_COUNT_ATOM = Atom.make(0).pipe(
  Atom.withLabel("enabled-schedule-count:empty"),
);
