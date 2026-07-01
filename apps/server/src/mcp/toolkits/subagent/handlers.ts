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
  type ProviderInstanceId,
} from "@t3tools/contracts";
import {
  buildProviderOptionSelectionsFromDescriptors,
  pickModelSelectionFromInstances,
} from "@t3tools/shared/model";
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
  toScheduleEntry,
  type ScheduledTask,
  type ScheduledTaskId,
  type ScheduledTaskRepositoryShape,
} from "../../../persistence/Services/ScheduledTasks.ts";
import {
  PendingDispatchRepository,
  type PendingDispatch,
  type PendingDispatchId,
  type PendingDispatchRepositoryShape,
} from "../../../persistence/Services/PendingDispatches.ts";
import { ProviderInstanceRegistry } from "../../../provider/Services/ProviderInstanceRegistry.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { activeThreadStartRuntimeOf, type ActiveThreadStartRuntime } from "../thread/handlers.ts";
import { ThreadStartToolError } from "../thread/tools.ts";
import {
  SubagentToolkit,
  WAIT_AUTO_PROMOTE_SECONDS,
  WAIT_TIMEOUT_DEFAULT_SECONDS,
  WAIT_TIMEOUT_MAX_SECONDS,
  WAIT_TIMEOUT_MIN_SECONDS,
  type CheckSubagentInput,
  type ListSubagentsInput,
  type ScheduleCreateInput,
  type ScheduleDeleteInput,
  type ScheduleListInput,
  type ScheduleUpdateInput,
  type SpawnSubagentInput,
  type SpawnSubagentOutput,
  type SteerSubagentInput,
  type WaitSubagentInput,
  type WaitSubagentOutput,
} from "./tools.ts";

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
const isThreadStartToolError = Schema.is(ThreadStartToolError);

const fail = (message: string) => new ThreadStartToolError({ message });

const toToolError = (error: unknown, fallback: string): ThreadStartToolError =>
  isThreadStartToolError(error) ? error : fail(error instanceof Error ? error.message : fallback);

const requireCoordinator = (): Effect.Effect<ChildThreadCoordinatorShape, ThreadStartToolError> => {
  const coordinator = coordinatorActive();
  return coordinator
    ? Effect.succeed(coordinator)
    : Effect.fail(fail("Sub-agent coordinator is not available."));
};

const requireSpawnRuntime = (): Effect.Effect<ActiveThreadStartRuntime, ThreadStartToolError> => {
  const runtime = activeThreadStartRuntimeOf();
  return runtime
    ? Effect.succeed(runtime)
    : Effect.fail(fail("Thread start runtime is not available."));
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
  readonly pendingDispatches: PendingDispatchRepositoryShape;
}

let activeRuntime: SubagentRuntime | null = null;

const requireRuntime = (): Effect.Effect<SubagentRuntime, ThreadStartToolError> =>
  activeRuntime
    ? Effect.succeed(activeRuntime)
    : Effect.fail(fail("Sub-agent runtime is not available."));

const requireInvocation = McpInvocationContext.requireMcpCapability("thread-management").pipe(
  Effect.mapError((error) => fail(error.message)),
);

const loadThreadShell = (runtime: SubagentRuntime, threadId: ThreadId) =>
  runtime.projectionSnapshotQuery
    .getThreadShellById(threadId)
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to load thread.")));

const loadThreadDetail = (runtime: SubagentRuntime, threadId: ThreadId) =>
  runtime.projectionSnapshotQuery
    .getThreadDetailById(threadId)
    .pipe(Effect.mapError((error) => toToolError(error, "Failed to load thread detail.")));

/**
 * Snapshot every ENABLED provider instance's live model list into the shape
 * `pickModelSelectionFromInstances` consumes (routing id + driver kind + the
 * models it currently serves, each with default options). This is the same
 * upstream-maintained registry the model picker reads, so newly-shipped models
 * are matched without any code change here.
 */
const buildModelSources = (runtime: SubagentRuntime) =>
  runtime.providerInstanceRegistry.listInstances.pipe(
    Effect.flatMap((providerInstances) =>
      Effect.forEach(
        providerInstances.filter((providerInstance) => providerInstance.enabled),
        (providerInstance) =>
          Effect.map(providerInstance.snapshot.getSnapshot, (snapshot) => ({
            instanceId: providerInstance.instanceId,
            driverKind: providerInstance.driverKind,
            models: snapshot.models.map((providerModel) => ({
              slug: providerModel.slug,
              defaultOptions: buildProviderOptionSelectionsFromDescriptors(
                providerModel.capabilities?.optionDescriptors,
              ),
            })),
          })),
      ),
    ),
  );

/**
 * Resolve an explicit plain model name to a `ModelSelection` against the live
 * provider lists, preferring `preferInstanceId` (usually the target thread's
 * instance) on ties. Fails loudly when no configured provider serves the model
 * rather than silently falling back, so a schedule/sub-agent never runs on a
 * different model than the caller named.
 */
const resolveExplicitModelSelection = (
  runtime: SubagentRuntime,
  model: string,
  preferInstanceId: ProviderInstanceId | undefined,
): Effect.Effect<ModelSelection, ThreadStartToolError> =>
  buildModelSources(runtime).pipe(
    Effect.flatMap((modelSources) => {
      const resolved = pickModelSelectionFromInstances(model, modelSources, preferInstanceId);
      return resolved === null
        ? Effect.fail(
            fail(
              `Model "${model}" is not served by any configured provider. Pass a model shown in the model picker, or omit "model" to keep the thread's current model.`,
            ),
          )
        : Effect.succeed(resolved);
    }),
  );

const spawnSubagent = Effect.fn("SubagentToolkit.spawn")(function* (input: SpawnSubagentInput) {
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
  // An explicit bare `model` resolves against the live provider model lists; a
  // named model no provider serves fails loudly instead of silently spawning on
  // a different (inherited) model. Prefer the source thread's instance on ties.
  const modelSelection: ModelSelection =
    threadStartInput.model !== undefined
      ? yield* resolveExplicitModelSelection(
          runtime,
          threadStartInput.model,
          source.modelSelection.instanceId,
        )
      : (threadStartInput.modelSelection ?? source.modelSelection);
  const instance = yield* runtime.providerInstanceRegistry.getInstance(modelSelection.instanceId);
  if (instance === undefined) {
    return yield* fail(`Provider instance ${modelSelection.instanceId} is not available.`);
  }

  // Spawn with the ALREADY-resolved selection (drop `model`) so the thread
  // runtime does not re-resolve against a possibly-different registry snapshot —
  // the coordinator record and the started thread then share one selection.
  const { model: _resolvedModel, ...threadStartInputWithSelection } = threadStartInput;
  const started = yield* spawnRuntime(
    { ...threadStartInputWithSelection, modelSelection },
    invocation,
  );
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

const steerSubagent = Effect.fn("SubagentToolkit.steer")(function* (input: SteerSubagentInput) {
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

  // R-C: pick a provider-safe mechanism from the child's turn state + driver.
  // An idle child is steered now; a mid-turn child dispatches now for every known
  // driver (claudeAgent/codex/cursor/grok/opencode — all support mid-turn steer).
  // Only an unknown/future driver is deferred to a durable 'child_steer' row that
  // the coordinator drains when the child idles, since its mid-turn semantics are
  // unverified.
  const midTurn = child.latestTurn?.state === "running";
  const instance = yield* runtime.providerInstanceRegistry.getInstance(
    child.modelSelection.instanceId,
  );
  const driverKind = instance?.driverKind;
  const MIDTURN_STEER_DRIVERS = ["claudeAgent", "codex", "cursor", "grok", "opencode"];
  const deferMidTurn = midTurn && !MIDTURN_STEER_DRIVERS.includes(driverKind ?? "");

  if (deferMidTurn) {
    const id = (yield* runtime.crypto.randomUUIDv4.pipe(Effect.orDie)) as PendingDispatchId;
    const createdAt = yield* nowIso;
    const row: PendingDispatch = {
      id,
      kind: "child_steer",
      targetThreadId: input.childThreadId,
      sourceChildId: null,
      text: input.message,
      error: null,
      status: null,
      commandId: null,
      createdAt: IsoDateTime.make(createdAt),
    };
    yield* runtime.pendingDispatches
      .insert(row)
      .pipe(Effect.mapError((error) => toToolError(error, "Failed to defer steer.")));
    return {
      childThreadId: input.childThreadId,
      accepted: true,
      applied: "deferred-until-idle" as const,
    };
  }

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

  return {
    childThreadId: input.childThreadId,
    accepted: true,
    applied: midTurn ? ("queued-midturn" as const) : ("now" as const),
  };
});

const checkSubagent = Effect.fn("SubagentToolkit.check")(function* (input: CheckSubagentInput) {
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

// R-A: the cumulative wait-start epoch ms is carried across resumeToken
// re-calls by prefixing it onto the coordinator's opaque slice token
// ("<waitStartMs>:<coordinatorToken>"). The first call (no resumeToken) marks
// now; subsequent calls recover the marker so the 90s budget spans the whole
// wait regardless of how many slices the model re-issued.
const parseWaitStartMs = (resumeToken: string | undefined, nowMs: number): number => {
  if (resumeToken === undefined) return nowMs;
  const separator = resumeToken.indexOf(":");
  if (separator <= 0) return nowMs;
  const parsed = Number(resumeToken.slice(0, separator));
  return Number.isFinite(parsed) ? parsed : nowMs;
};

const waitSubagent = Effect.fn("SubagentToolkit.wait")(function* (input: WaitSubagentInput) {
  yield* requireInvocation;
  const runtime = yield* requireRuntime();
  const coordinator = yield* requireCoordinator();

  const budgetSeconds = clamp(
    input.timeoutSeconds ?? WAIT_TIMEOUT_DEFAULT_SECONDS,
    WAIT_TIMEOUT_MIN_SECONDS,
    WAIT_TIMEOUT_MAX_SECONDS,
  );
  const nowMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
  const waitStartMs = parseWaitStartMs(input.resumeToken, nowMs);
  // Hand the coordinator only its own opaque token (the part after the marker).
  const coordinatorToken =
    input.resumeToken !== undefined && input.resumeToken.indexOf(":") > 0
      ? input.resumeToken.slice(input.resumeToken.indexOf(":") + 1)
      : input.resumeToken;
  // Cap the cumulative blocking budget at WAIT_AUTO_PROMOTE_SECONDS regardless
  // of the caller's timeoutSeconds; the earlier of the two deadlines bounds the
  // slice so the slice itself never blocks past the auto-promote horizon.
  const autoPromoteDeadlineMs = waitStartMs + WAIT_AUTO_PROMOTE_SECONDS * 1_000;
  const callerDeadlineMs = nowMs + budgetSeconds * 1_000;
  const budgetDeadlineMs = Math.min(callerDeadlineMs, autoPromoteDeadlineMs);

  // One bounded slice per invocation — the agent re-calls with the returned
  // resumeToken while `pending` is true (never one long HTTP hold).
  const slice = yield* coordinator.waitSlice({
    childThreadIds: input.childThreadIds,
    mode: input.mode ?? "all",
    budgetDeadlineMs,
    ...(coordinatorToken !== undefined ? { resumeToken: coordinatorToken } : {}),
  });

  const afterSliceMs = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
  // Auto-promote (R-A): the 90s budget elapsed and one+ children are still
  // running. Flip them onto the wake-on-completion path so they will NOTIFY the
  // parent (notify-guarantee), and return promoted so the model stops polling.
  const autoPromote = slice.pending && afterSliceMs >= autoPromoteDeadlineMs;
  const stillRunningIds = autoPromote
    ? slice.results.filter((row) => row.status === "pending").map((row) => row.childThreadId)
    : [];
  if (autoPromote && stillRunningIds.length > 0) {
    yield* coordinator.promoteToWake(stillRunningIds);
  }

  // Enrich each row with a turn count from the projection (the coordinator's
  // terminal result intentionally does not track it). On auto-promote, a still
  // "pending" child is reported as "running" with a note telling the model to
  // stop waiting.
  const results: WaitSubagentOutput["results"] = yield* Effect.forEach(slice.results, (row) =>
    loadThreadDetail(runtime, row.childThreadId).pipe(
      Effect.map((thread) => {
        const promotedRunning = autoPromote && row.status === "pending";
        return {
          childThreadId: row.childThreadId,
          status: promotedRunning ? "running" : row.status,
          turnCount: Option.match(thread, { onNone: () => 0, onSome: turnCountOf }),
          finalAssistantText: row.finalAssistantText,
          error: row.error,
          ...(promotedRunning
            ? {
                note: "still running — you will be NOTIFIED when it completes; stop calling wait and do other work",
              }
            : {}),
        };
      }),
    ),
  );

  return {
    results,
    settledCount: slice.settledCount,
    timedOutCount: slice.timedOutCount,
    // Auto-promote is a terminal return: the model must stop waiting, so this is
    // never reported as still-pending even though some children are running.
    pending: autoPromote ? false : slice.pending,
    resumeToken: `${waitStartMs}:${slice.resumeToken}`,
    ...(autoPromote ? { promoted: true } : {}),
  };
});

const listSubagents = Effect.fn("SubagentToolkit.list")(function* (input: ListSubagentsInput) {
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

const validateCron = (
  cronExpr: string,
  timezone: string,
): Effect.Effect<void, ThreadStartToolError> =>
  Effect.try({
    try: () => {
      const _cron = new Cron(cronExpr, { timezone });
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
  return next === null
    ? null
    : DateTime.formatIso(Option.getOrThrow(DateTime.make(next.getTime())));
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

  const shell = yield* loadThreadShell(runtime, threadId).pipe(
    Effect.flatMap((shellOption) =>
      Option.match(shellOption, {
        onNone: () => Effect.fail(fail(`Thread ${threadId} was not found.`)),
        onSome: Effect.succeed,
      }),
    ),
  );

  // Resolve an explicit plain `model` to a full selection (provider/harness
  // inferred), preferring the thread's current instance on ties; null inherits
  // the thread's model on each run.
  const modelSelection =
    input.model !== undefined
      ? yield* resolveExplicitModelSelection(runtime, input.model, shell.modelSelection.instanceId)
      : null;

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
    modelSelection,
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
  runtime.scheduledTasks.listAll().pipe(
    Effect.mapError((error) => toToolError(error, "Failed to load scheduled task.")),
    Effect.flatMap((tasks) => {
      const found = tasks.find((task) => task.taskId === taskId);
      return found
        ? Effect.succeed(found)
        : Effect.fail(fail(`Scheduled task ${taskId} was not found.`));
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
    input.cronExpr !== undefined
      ? input.cronExpr
      : scheduleKind === "cron"
        ? existing.cronExpr
        : null;
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

  // Three-way `model` handling: omit (undefined) keeps the current selection;
  // an explicit null un-pins it (runs inherit the thread's model again); a plain
  // name re-routes. On a re-route, prefer the schedule's current instance, or —
  // when it is still inheriting — the target thread's instance, so a
  // multi-instance setup keeps continuity (matches create).
  let modelSelection: ModelSelection | null;
  if (input.model === undefined) {
    modelSelection = existing.modelSelection;
  } else if (input.model === null) {
    modelSelection = null;
  } else {
    const preferInstanceId =
      existing.modelSelection?.instanceId ??
      (yield* loadThreadShell(runtime, existing.threadId).pipe(
        Effect.map((shellOption) =>
          Option.match(shellOption, {
            onNone: () => undefined,
            onSome: (shell) => shell.modelSelection.instanceId,
          }),
        ),
      ));
    modelSelection = yield* resolveExplicitModelSelection(runtime, input.model, preferInstanceId);
  }

  const updated: ScheduledTask = {
    ...existing,
    enabled:
      input.enabled === undefined ? existing.enabled : NonNegativeInt.make(input.enabled ? 1 : 0),
    busyPolicy: input.busyPolicy ?? existing.busyPolicy,
    scheduleKind,
    intervalSeconds,
    cronExpr,
    nextRunAt,
    modelSelection,
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
  const pendingDispatches = yield* PendingDispatchRepository;
  return {
    crypto,
    orchestrationEngine,
    projectionSnapshotQuery,
    providerInstanceRegistry,
    scheduledTasks,
    pendingDispatches,
  };
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
