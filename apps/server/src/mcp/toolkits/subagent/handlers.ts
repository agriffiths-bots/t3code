/**
 * Sub-agent + scheduler MCP toolkit handlers (finalPlan §6/§7).
 *
 * Mirrors the thread toolkit: a module-level active runtime (captured by
 * `SubagentRuntimeLive`, cloned from `ThreadStartRuntimeLive`) holds the live
 * services so the toolkit handlers can reach them without threading them
 * through the toolkit `Context`. The handlers reuse pr3107's
 * `activeThreadStartRuntime` for spawning, the live `ChildThreadCoordinator`
 * for the never-hang wait/registration, and `dispatchActive` for steering.
 *
 * @module subagent/handlers
 */
import {
  CommandId,
  IsoDateTime,
  MessageId,
  NonNegativeInt,
  ThreadId,
  type ModelSelection,
  type OrchestrationThread,
} from "@t3tools/contracts";
import { Cron } from "croner";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { coordinatorActive } from "../../../orchestration/Layers/ChildThreadCoordinator.ts";
import type {
  ChildThreadCoordinatorShape,
  WaitSliceResult,
} from "../../../orchestration/Services/ChildThreadCoordinator.ts";
import { dispatchActive } from "../../../orchestration/Services/BootstrapTurnStartDispatcher.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import {
  ScheduledTaskRepository,
  type ScheduledTask,
  type ScheduledTaskId,
  type ScheduledTaskRepositoryShape,
} from "../../../persistence/Services/ScheduledTasks.ts";
import { ProviderInstanceRegistry } from "../../../provider/Services/ProviderInstanceRegistry.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import {
  activeThreadStartRuntimeOf,
  type ActiveThreadStartRuntime,
} from "../thread/handlers.ts";
import { ThreadStartToolError } from "../thread/tools.ts";
import {
  SubagentToolkit,
  WAIT_TIMEOUT_DEFAULT_SECONDS,
  WAIT_TIMEOUT_MAX_SECONDS,
  WAIT_TIMEOUT_MIN_SECONDS,
  type CheckSubagentInput,
  type CheckSubagentOutput,
  type ListSubagentsInput,
  type ListSubagentsOutput,
  type ScheduleCreateInput,
  type ScheduleDeleteInput,
  type ScheduleDeleteOutput,
  type ScheduleEntry,
  type ScheduleListInput,
  type ScheduleListOutput,
  type ScheduleUpdateInput,
  type SpawnSubagentInput,
  type SpawnSubagentOutput,
  type SteerSubagentInput,
  type SteerSubagentOutput,
  type WaitSubagentInput,
  type WaitSubagentOutput,
} from "./tools.ts";

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
const isThreadStartToolError = Schema.is(ThreadStartToolError);

const fail = (message: string) => new ThreadStartToolError({ message });

const toToolError = (error: unknown, fallback: string): ThreadStartToolError =>
  isThreadStartToolError(error)
    ? error
    : fail(error instanceof Error ? error.message : fallback);

const requireCoordinator = (): Effect.Effect<ChildThreadCoordinatorShape, ThreadStartToolError> => {
  const coordinator = coordinatorActive();
  return coordinator
    ? Effect.succeed(coordinator)
    : Effect.fail(fail("Sub-agent coordinator is not available."));
};

const requireSpawnRuntime = (): Effect.Effect<ActiveThreadStartRuntime, ThreadStartToolError> => {
  const runtime = activeThreadStartRuntimeOf();
  return runtime ? Effect.succeed(runtime) : Effect.fail(fail("Thread start runtime is not available."));
};

const clamp = (value: number, min: number, max: number): number =>
  value < min ? min : value > max ? max : value;

/**
 * Derive a turn count from a thread detail: the highest checkpoint turn count,
 * or 0 when the thread has not completed a turn yet.
 */
const turnCountOf = (thread: OrchestrationThread): number => {
  let count = 0;
  for (const checkpoint of thread.checkpoints) {
    if (checkpoint.checkpointTurnCount > count) count = checkpoint.checkpointTurnCount;
  }
  return count;
};

/** Latest assistant message text on a thread, or null. */
const latestAssistantTextOf = (thread: OrchestrationThread): string | null => {
  let chosen: string | null = null;
  for (const message of thread.messages) {
    if (message.role === "assistant") chosen = message.text;
  }
  return chosen;
};

/**
 * Map a thread shell's turn/session state to a coarse readonly status used by
 * `t3_check_subagent` / `t3_list_subagents` (matches the coordinator's terminal
 * vocabulary where it overlaps).
 */
const statusOf = (thread: Pick<OrchestrationThread, "latestTurn" | "session">): string => {
  if (thread.latestTurn?.state === "running") return "running";
  const session = thread.session;
  if (session !== null && (session.status === "stopped" || session.status === "error")) {
    return "failed";
  }
  switch (thread.latestTurn?.state) {
    case "completed":
      return "completed";
    case "error":
      return "failed";
    case "interrupted":
      return "interrupted";
    default:
      return session === null ? "idle" : "running";
  }
};

interface SubagentRuntime {
  readonly crypto: Crypto.Crypto;
  readonly orchestrationEngine: typeof OrchestrationEngineService.Service;
  readonly projectionSnapshotQuery: typeof ProjectionSnapshotQuery.Service;
  readonly providerInstanceRegistry: typeof ProviderInstanceRegistry.Service;
  readonly scheduledTasks: ScheduledTaskRepositoryShape;
}

let activeRuntime: SubagentRuntime | null = null;

const requireRuntime = (): Effect.Effect<SubagentRuntime, ThreadStartToolError> =>
  activeRuntime ? Effect.succeed(activeRuntime) : Effect.fail(fail("Sub-agent runtime is not available."));

const requireInvocation = McpInvocationContext.requireMcpCapability("thread-management").pipe(
  Effect.mapError((error) => fail(error.message)),
);

const loadThreadShell = (runtime: SubagentRuntime, threadId: ThreadId) =>
  runtime.projectionSnapshotQuery.getThreadShellById(threadId).pipe(
    Effect.mapError((error) => toToolError(error, "Failed to load thread.")),
  );

const loadThreadDetail = (runtime: SubagentRuntime, threadId: ThreadId) =>
  runtime.projectionSnapshotQuery.getThreadDetailById(threadId).pipe(
    Effect.mapError((error) => toToolError(error, "Failed to load thread detail.")),
  );

const spawnSubagent = Effect.fn("SubagentToolkit.spawn")(function* (
  input: SpawnSubagentInput,
) {
  const invocation = yield* requireInvocation;
  const runtime = yield* requireRuntime();
  const coordinator = yield* requireCoordinator();
  const spawnRuntime = yield* requireSpawnRuntime();

  const { detached: detachedInput, waitTimeoutSeconds, ...threadStartInput } = input;
  const detached = detachedInput ?? true;

  // Fail-fast: refuse to spawn against a provider instance that no longer exists.
  const source = yield* loadThreadShell(runtime, invocation.threadId).pipe(
    Effect.flatMap((shell) =>
      Option.match(shell, {
        onNone: () => Effect.fail(fail(`Source thread ${invocation.threadId} was not found.`)),
        onSome: Effect.succeed,
      }),
    ),
  );
  const modelSelection: ModelSelection = threadStartInput.modelSelection ?? source.modelSelection;
  const instance = yield* runtime.providerInstanceRegistry.getInstance(modelSelection.instanceId);
  if (instance === undefined) {
    return yield* fail(`Provider instance ${modelSelection.instanceId} is not available.`);
  }

  const started = yield* spawnRuntime(threadStartInput, invocation);
  const spawnedAtMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);

  yield* coordinator.register({
    parentThreadId: invocation.threadId,
    childThreadId: started.threadId,
    detached,
    model: modelSelection,
    spawnedAtMs,
  });

  // Persist the parent linkage so the coordinator's reconciliation (and the web
  // tree) can recover it across restarts.
  yield* dispatchParentSet(runtime, started.threadId, invocation.threadId).pipe(
    Effect.mapError((error) => toToolError(error, "Failed to link sub-agent to parent.")),
  );

  const base: SpawnSubagentOutput = {
    childThreadId: started.threadId,
    projectId: started.projectId,
    mode: started.mode,
    branch: started.branch,
    worktreePath: started.worktreePath,
    parentThreadId: invocation.threadId,
    ...(started.warning ? { warning: started.warning } : {}),
  };

  if (detached) return base;

  const budgetSeconds = clamp(
    waitTimeoutSeconds ?? WAIT_TIMEOUT_DEFAULT_SECONDS,
    WAIT_TIMEOUT_MIN_SECONDS,
    WAIT_TIMEOUT_MAX_SECONDS,
  );
  const budgetDeadlineMs = spawnedAtMs + budgetSeconds * 1_000;
  const settled = yield* waitForChildren(coordinator, [started.threadId], "all", budgetDeadlineMs);
  const row = settled.results[0];
  return {
    ...base,
    status: row?.status ?? "timeout",
    finalAssistantText: row?.finalAssistantText ?? null,
  };
});

const dispatchParentSet = Effect.fn("SubagentToolkit.dispatchParentSet")(function* (
  runtime: SubagentRuntime,
  childThreadId: ThreadId,
  parentThreadId: ThreadId,
) {
  const uuid = yield* runtime.crypto.randomUUIDv4.pipe(Effect.orDie);
  const createdAt = yield* nowIso;
  yield* runtime.orchestrationEngine.dispatch({
    type: "thread.parent.set",
    commandId: CommandId.make(`server:subagent-link:${uuid}`),
    threadId: childThreadId,
    parentThreadId,
    createdAt,
  });
});

/**
 * Re-call the coordinator's bounded `waitSlice` until every requested child is
 * settled, the wait mode is satisfied, or the logical budget is exhausted. Each
 * slice is bounded (never a single long HTTP hold); this loop is server-side so
 * a foreground spawn returns one consolidated result.
 */
const waitForChildren = (
  coordinator: ChildThreadCoordinatorShape,
  childThreadIds: ReadonlyArray<ThreadId>,
  mode: "all" | "any",
  budgetDeadlineMs: number,
): Effect.Effect<WaitSliceResult> =>
  coordinator
    .waitSlice({ childThreadIds, mode, budgetDeadlineMs })
    .pipe(
      Effect.flatMap((result) =>
        result.pending
          ? waitForChildren(coordinator, childThreadIds, mode, budgetDeadlineMs)
          : Effect.succeed(result),
      ),
    );

const steerSubagent = Effect.fn("SubagentToolkit.steer")(function* (
  input: SteerSubagentInput,
) {
  const invocation = yield* requireInvocation;
  const runtime = yield* requireRuntime();
  const coordinator = yield* requireCoordinator();

  yield* coordinator.assertParent(invocation.threadId, input.childThreadId);

  const child = yield* loadThreadShell(runtime, input.childThreadId).pipe(
    Effect.flatMap((shell) =>
      Option.match(shell, {
        onNone: () => Effect.fail(fail(`Sub-agent ${input.childThreadId} was not found.`)),
        onSome: Effect.succeed,
      }),
    ),
  );

  const uuid = yield* runtime.crypto.randomUUIDv4.pipe(Effect.orDie);
  const messageId = MessageId.make(yield* runtime.crypto.randomUUIDv4.pipe(Effect.orDie));
  const createdAt = yield* nowIso;
  yield* dispatchActive({
    type: "thread.turn.start",
    commandId: CommandId.make(`server:subagent-steer:${uuid}`),
    threadId: input.childThreadId,
    message: { messageId, role: "user", text: input.message, attachments: [] },
    runtimeMode: child.runtimeMode,
    interactionMode: child.interactionMode,
    bootstrap: undefined,
    createdAt,
  }).pipe(Effect.mapError((error) => toToolError(error, "Failed to steer sub-agent.")));

  return { childThreadId: input.childThreadId, accepted: true };
});

const checkSubagent = Effect.fn("SubagentToolkit.check")(function* (
  input: CheckSubagentInput,
) {
  yield* requireInvocation;
  const runtime = yield* requireRuntime();

  const detail = yield* loadThreadDetail(runtime, input.childThreadId).pipe(
    Effect.flatMap((thread) =>
      Option.match(thread, {
        onNone: () => Effect.fail(fail(`Thread ${input.childThreadId} was not found.`)),
        onSome: Effect.succeed,
      }),
    ),
  );

  return {
    threadId: input.childThreadId,
    status: statusOf(detail),
    turnCount: turnCountOf(detail),
    latestAssistantText: latestAssistantTextOf(detail),
  };
});

const waitSubagent = Effect.fn("SubagentToolkit.wait")(function* (
  input: WaitSubagentInput,
) {
  yield* requireInvocation;
  const runtime = yield* requireRuntime();
  const coordinator = yield* requireCoordinator();

  const budgetSeconds = clamp(
    input.timeoutSeconds ?? WAIT_TIMEOUT_DEFAULT_SECONDS,
    WAIT_TIMEOUT_MIN_SECONDS,
    WAIT_TIMEOUT_MAX_SECONDS,
  );
  const nowMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
  const budgetDeadlineMs = nowMs + budgetSeconds * 1_000;

  // One bounded slice per invocation — the agent re-calls with the returned
  // resumeToken while `pending` is true (never one long HTTP hold).
  const slice = yield* coordinator.waitSlice({
    childThreadIds: input.childThreadIds,
    mode: input.mode ?? "all",
    budgetDeadlineMs,
    ...(input.resumeToken !== undefined ? { resumeToken: input.resumeToken } : {}),
  });

  // Enrich each row with a turn count from the projection (the coordinator's
  // terminal result intentionally does not track it).
  const results: WaitSubagentOutput["results"] = yield* Effect.forEach(slice.results, (row) =>
    loadThreadDetail(runtime, row.childThreadId).pipe(
      Effect.map((thread) => ({
        childThreadId: row.childThreadId,
        status: row.status,
        turnCount: Option.match(thread, { onNone: () => 0, onSome: turnCountOf }),
        finalAssistantText: row.finalAssistantText,
        error: row.error,
      })),
    ),
  );

  return {
    results,
    settledCount: slice.settledCount,
    timedOutCount: slice.timedOutCount,
    pending: slice.pending,
    resumeToken: slice.resumeToken,
  };
});

const listSubagents = Effect.fn("SubagentToolkit.list")(function* (
  input: ListSubagentsInput,
) {
  const invocation = yield* requireInvocation;
  const runtime = yield* requireRuntime();
  const coordinator = yield* requireCoordinator();

  const parentThreadId = input.parentThreadId ?? invocation.threadId;
  const registered = yield* coordinator.listChildren(parentThreadId);

  const children = yield* Effect.forEach(registered, (entry) =>
    loadThreadDetail(runtime, entry.childThreadId).pipe(
      Effect.map((thread) =>
        Option.match(thread, {
          onNone: () => ({
            childThreadId: entry.childThreadId,
            parentThreadId: entry.parentThreadId,
            detached: entry.detached,
            depth: entry.depth,
            spawnedAtMs: entry.spawnedAtMs,
            settled: entry.settled,
            status: "unknown",
            turnCount: 0,
          }),
          onSome: (detail) => ({
            childThreadId: entry.childThreadId,
            parentThreadId: entry.parentThreadId,
            detached: entry.detached,
            depth: entry.depth,
            spawnedAtMs: entry.spawnedAtMs,
            settled: entry.settled,
            status: statusOf(detail),
            turnCount: turnCountOf(detail),
          }),
        }),
      ),
    ),
  );

  return { parentThreadId, children };
});

const toScheduleEntry = (task: ScheduledTask): ScheduleEntry => ({
  taskId: task.taskId,
  threadId: task.threadId,
  prompt: task.prompt,
  scheduleKind: task.scheduleKind,
  intervalSeconds: task.intervalSeconds,
  cronExpr: task.cronExpr,
  timezone: task.timezoneName,
  enabled: task.enabled !== 0,
  busyPolicy: task.busyPolicy,
  nextRunAt: task.nextRunAt,
  lastRunAt: task.lastRunAt,
  lastStatus: task.lastStatus,
});

const validateCron = (cronExpr: string, timezone: string): Effect.Effect<void, ThreadStartToolError> =>
  Effect.try({
    try: () => {
      new Cron(cronExpr, { timezone });
    },
    catch: () => fail(`Invalid cron expression: ${cronExpr}`),
  });

const computeNextRunIso = (
  scheduleKind: "interval" | "cron",
  intervalSeconds: number | null,
  cronExpr: string | null,
  timezone: string,
  nowMs: number,
): string | null => {
  if (scheduleKind === "interval") {
    if (intervalSeconds === null || intervalSeconds <= 0) return null;
    return DateTime.formatIso(Option.getOrThrow(DateTime.make(nowMs + intervalSeconds * 1_000)));
  }
  if (cronExpr === null) return null;
  const next = new Cron(cronExpr, { timezone }).nextRun(
    DateTime.toDateUtc(Option.getOrThrow(DateTime.make(nowMs))),
  );
  return next === null ? null : DateTime.formatIso(Option.getOrThrow(DateTime.make(next.getTime())));
};

const scheduleCreate = Effect.fn("SubagentToolkit.scheduleCreate")(function* (
  input: ScheduleCreateInput,
) {
  const invocation = yield* requireInvocation;
  const runtime = yield* requireRuntime();

  const threadId = input.threadId ?? invocation.threadId;
  const hasInterval = input.intervalSeconds !== undefined;
  const hasCron = input.cronExpr !== undefined;
  if (hasInterval === hasCron) {
    return yield* fail("Provide exactly one of intervalSeconds or cronExpr.");
  }
  if (hasInterval && (input.intervalSeconds as number) <= 0) {
    return yield* fail("intervalSeconds must be positive.");
  }

  const timezone = input.timezone ?? "UTC";
  const scheduleKind: "interval" | "cron" = hasCron ? "cron" : "interval";
  const cronExpr = input.cronExpr ?? null;
  const intervalSeconds = input.intervalSeconds ?? null;
  if (cronExpr !== null) yield* validateCron(cronExpr, timezone);

  yield* loadThreadShell(runtime, threadId).pipe(
    Effect.flatMap((shell) =>
      Option.match(shell, {
        onNone: () => Effect.fail(fail(`Thread ${threadId} was not found.`)),
        onSome: () => Effect.void,
      }),
    ),
  );

  const uuid = yield* runtime.crypto.randomUUIDv4.pipe(Effect.orDie);
  const nowMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
  const createdAt = yield* nowIso;
  const nextRunAt = computeNextRunIso(scheduleKind, intervalSeconds, cronExpr, timezone, nowMs);

  const task: ScheduledTask = {
    taskId: uuid as ScheduledTaskId,
    threadId,
    prompt: input.prompt,
    scheduleKind,
    intervalSeconds,
    cronExpr,
    timezoneName: timezone,
    enabled: NonNegativeInt.make(1),
    busyPolicy: input.busyPolicy ?? "skip",
    nextRunAt: nextRunAt === null ? null : IsoDateTime.make(nextRunAt),
    lastRunAt: null,
    lastStatus: null,
    lastError: null,
    skippedCount: NonNegativeInt.make(0),
    retryCount: NonNegativeInt.make(0),
    queuedCount: NonNegativeInt.make(0),
    createdAt: IsoDateTime.make(createdAt),
  };

  yield* runtime.scheduledTasks
    .insert(task)
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to create scheduled task.")));

  return toScheduleEntry(task);
});

const scheduleList = Effect.fn("SubagentToolkit.scheduleList")(function* (
  input: ScheduleListInput,
) {
  yield* requireInvocation;
  const runtime = yield* requireRuntime();

  const tasks = yield* (
    input.threadId !== undefined
      ? runtime.scheduledTasks.listByThread({ threadId: input.threadId })
      : runtime.scheduledTasks.listAll()
  ).pipe(Effect.mapError((error) => toToolError(error, "Failed to list scheduled tasks.")));

  return { tasks: tasks.map(toScheduleEntry) };
});

const loadTaskById = (
  runtime: SubagentRuntime,
  taskId: ScheduledTaskId,
): Effect.Effect<ScheduledTask, ThreadStartToolError> =>
  runtime.scheduledTasks
    .listAll()
    .pipe(
      Effect.mapError((error) => toToolError(error, "Failed to load scheduled task.")),
      Effect.flatMap((tasks) => {
        const found = tasks.find((task) => task.taskId === taskId);
        return found ? Effect.succeed(found) : Effect.fail(fail(`Scheduled task ${taskId} was not found.`));
      }),
    );

const scheduleUpdate = Effect.fn("SubagentToolkit.scheduleUpdate")(function* (
  input: ScheduleUpdateInput,
) {
  yield* requireInvocation;
  const runtime = yield* requireRuntime();

  const existing = yield* loadTaskById(runtime, input.taskId);

  const scheduleKind: "interval" | "cron" =
    input.cronExpr !== undefined
      ? "cron"
      : input.intervalSeconds !== undefined
        ? "interval"
        : existing.scheduleKind;
  const cronExpr =
    input.cronExpr !== undefined ? input.cronExpr : scheduleKind === "cron" ? existing.cronExpr : null;
  const intervalSeconds =
    input.intervalSeconds !== undefined
      ? input.intervalSeconds
      : scheduleKind === "interval"
        ? existing.intervalSeconds
        : null;
  if (cronExpr !== null) yield* validateCron(cronExpr, existing.timezoneName);
  if (scheduleKind === "interval" && intervalSeconds !== null && intervalSeconds <= 0) {
    return yield* fail("intervalSeconds must be positive.");
  }

  // Recompute next_run_at whenever the cadence changed so the reactor honours it.
  const cadenceChanged = input.cronExpr !== undefined || input.intervalSeconds !== undefined;
  let nextRunAt = existing.nextRunAt;
  if (cadenceChanged) {
    const nowMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
    const computed = computeNextRunIso(
      scheduleKind,
      intervalSeconds,
      cronExpr,
      existing.timezoneName,
      nowMs,
    );
    nextRunAt = computed === null ? null : IsoDateTime.make(computed);
  }

  const updated: ScheduledTask = {
    ...existing,
    enabled: input.enabled === undefined ? existing.enabled : NonNegativeInt.make(input.enabled ? 1 : 0),
    busyPolicy: input.busyPolicy ?? existing.busyPolicy,
    scheduleKind,
    intervalSeconds,
    cronExpr,
    nextRunAt,
  };

  yield* runtime.scheduledTasks
    .update(updated)
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to update scheduled task.")));

  return toScheduleEntry(updated);
});

const scheduleDelete = Effect.fn("SubagentToolkit.scheduleDelete")(function* (
  input: ScheduleDeleteInput,
) {
  yield* requireInvocation;
  const runtime = yield* requireRuntime();

  yield* runtime.scheduledTasks
    .delete({ taskId: input.taskId })
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to delete scheduled task.")));

  return { taskId: input.taskId, deleted: true };
});

const handlers = {
  t3_spawn_subagent: spawnSubagent,
  t3_steer_subagent: steerSubagent,
  t3_check_subagent: checkSubagent,
  t3_wait_subagent: waitSubagent,
  t3_list_subagents: listSubagents,
  t3_schedule_create: scheduleCreate,
  t3_schedule_list: scheduleList,
  t3_schedule_update: scheduleUpdate,
  t3_schedule_delete: scheduleDelete,
} satisfies Parameters<typeof SubagentToolkit.toLayer>[0];

export const SubagentToolkitHandlersLive = SubagentToolkit.toLayer(handlers);

const makeSubagentRuntime = Effect.fn("SubagentToolkit.makeActiveRuntime")(function* () {
  const crypto = yield* Crypto.Crypto;
  const orchestrationEngine = yield* OrchestrationEngineService;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const providerInstanceRegistry = yield* ProviderInstanceRegistry;
  const scheduledTasks = yield* ScheduledTaskRepository;
  return { crypto, orchestrationEngine, projectionSnapshotQuery, providerInstanceRegistry, scheduledTasks };
});

export const SubagentRuntimeLive = Layer.effectDiscard(
  Effect.acquireRelease(
    makeSubagentRuntime().pipe(
      Effect.tap((runtime) =>
        Effect.sync(() => {
          activeRuntime = runtime;
        }),
      ),
    ),
    (runtime) =>
      Effect.sync(() => {
        if (activeRuntime === runtime) activeRuntime = null;
      }),
  ),
);
