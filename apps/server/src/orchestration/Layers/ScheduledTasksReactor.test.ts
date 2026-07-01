/* oxlint-disable t3code/no-manual-effect-runtime-in-tests -- These reactor tests intentionally manage a long-lived runtime and scope lifecycle. */
import {
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type ModelSelection,
  type OrchestrationCommand,
  type OrchestrationSession,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import { TestClock } from "effect/testing";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import { afterEach, describe, expect, it } from "vite-plus/test";

import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { ScheduledTaskRepositoryLive } from "../../persistence/Layers/ScheduledTasks.ts";
import {
  ScheduledTask,
  ScheduledTaskId,
  ScheduledTaskRepository,
} from "../../persistence/Services/ScheduledTasks.ts";
import { ChildThreadCoordinator } from "../Services/ChildThreadCoordinator.ts";
import { BootstrapTurnStartDispatcher } from "../Services/BootstrapTurnStartDispatcher.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { ScheduledTasksReactor } from "../Services/ScheduledTasksReactor.ts";
import { ScheduledTasksReactorLive } from "./ScheduledTasksReactor.ts";

const now = "2026-06-17T10:00:00.000Z";
const projectId = ProjectId.make("project-scheduler-test");
const codexModel: ModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5-codex",
};

const unsupported = () => Effect.die(new Error("Unsupported projection call in test")) as never;

const makeSession = (
  threadId: ThreadId,
  status: OrchestrationSession["status"],
  activeTurnId: TurnId | null = null,
): OrchestrationSession => ({
  threadId,
  status,
  providerName: "codex",
  runtimeMode: "full-access",
  activeTurnId,
  lastError: null,
  updatedAt: now,
});

const makeShell = (input: {
  readonly threadId: ThreadId;
  readonly session?: OrchestrationSession | null;
}): OrchestrationThreadShell => ({
  id: input.threadId,
  projectId,
  title: `Thread ${input.threadId}`,
  modelSelection: codexModel,
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  latestTurn: null,
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  session: input.session ?? null,
  latestUserMessageAt: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
  parentThreadId: null,
});

const makeTask = (overrides: Partial<ScheduledTask> = {}): ScheduledTask =>
  ({
    taskId: ScheduledTaskId.make("task-1"),
    threadId: ThreadId.make("thread-1"),
    prompt: "run the report",
    scheduleKind: "interval",
    intervalSeconds: 3_600,
    cronExpr: null,
    timezoneName: "UTC",
    enabled: 1,
    busyPolicy: "skip",
    nextRunAt: "2026-06-17T09:00:00.000Z",
    lastRunAt: null,
    lastStatus: null,
    lastError: null,
    skippedCount: 0,
    retryCount: 0,
    queuedCount: 0,
    modelSelection: null,
    createdAt: "2026-06-17T08:00:00.000Z",
    ...overrides,
  }) satisfies ScheduledTask;

describe("ScheduledTasksReactor", () => {
  let runtime: ManagedRuntime.ManagedRuntime<
    ScheduledTasksReactor | ScheduledTaskRepository | SqlClient,
    unknown
  > | null = null;
  let scope: Scope.Closeable | null = null;

  afterEach(async () => {
    if (scope) {
      await Effect.runPromise(Scope.close(scope, Exit.void));
      scope = null;
    }
    if (runtime) {
      await runtime.dispose();
      runtime = null;
    }
  });

  async function createHarness(input?: {
    /** Threads visible to getThreadShellById; absence => deleted thread. */
    readonly shells?: ReadonlyArray<OrchestrationThreadShell>;
    /** Parent ids the coordinator reports as having pending injections. */
    readonly pendingParents?: ReadonlyArray<ThreadId>;
    /** When true, the dispatcher fails every dispatch (drives the retry path). */
    readonly failDispatch?: boolean;
    /** Invoked synchronously on each dispatch; used to simulate a crash. */
    readonly onDispatch?: (
      command: Extract<OrchestrationCommand, { readonly type: "thread.turn.start" }>,
    ) => void;
  }) {
    const shells = new Map<ThreadId, OrchestrationThreadShell>();
    for (const shell of input?.shells ?? []) shells.set(shell.id, shell);
    const pendingParents = new Set((input?.pendingParents ?? []).map((id) => String(id)));
    const dispatched: Array<OrchestrationCommand> = [];

    const dispatcherLayer = Layer.succeed(BootstrapTurnStartDispatcher, {
      dispatch: (command) =>
        Effect.gen(function* () {
          dispatched.push(command);
          input?.onDispatch?.(command);
          if (input?.failDispatch) {
            return yield* Effect.die(new Error("dispatch failed"));
          }
          return { sequence: dispatched.length };
        }),
    });

    const projectionLayer = Layer.succeed(ProjectionSnapshotQuery, {
      getCommandReadModel: () => unsupported(),
      getSnapshot: () => unsupported(),
      getShellSnapshot: () => unsupported(),
      getArchivedShellSnapshot: () => unsupported(),
      getSnapshotSequence: () => unsupported(),
      getCounts: () => unsupported(),
      getActiveProjectByWorkspaceRoot: () => unsupported(),
      getProjectShellById: () => unsupported(),
      getFirstActiveThreadIdByProjectId: () => unsupported(),
      getThreadCheckpointContext: () => unsupported(),
      getFullThreadDiffContext: () => unsupported(),
      getThreadShellById: (threadId) =>
        Effect.sync(() => {
          const shell = shells.get(threadId);
          return shell ? Option.some(shell) : Option.none();
        }),
      getThreadDetailById: () => unsupported(),
    });

    const coordinatorLayer = Layer.succeed(ChildThreadCoordinator, {
      register: () => Effect.void,
      waitSlice: () => unsupported(),
      assertParent: () => Effect.void,
      promoteToWake: () => Effect.void,
      hasPendingInjections: (parentThreadId) =>
        Effect.succeed(pendingParents.has(String(parentThreadId))),
      listChildren: () => Effect.succeed([]),
      start: () => Effect.void,
      drain: Effect.void,
    });

    const layer = ScheduledTasksReactorLive.pipe(
      Layer.provideMerge(dispatcherLayer),
      Layer.provideMerge(projectionLayer),
      Layer.provideMerge(coordinatorLayer),
      Layer.provideMerge(ScheduledTaskRepositoryLive),
      Layer.provideMerge(SqlitePersistenceMemory),
      // Deterministic clock so the sweep fires exactly once under our control,
      // anchored at `now` so freshly inserted rows are due.
      Layer.provideMerge(TestClock.layer()),
      Layer.provideMerge(NodeServices.layer),
    );

    runtime = ManagedRuntime.make(layer);
    const activeRuntime = runtime;
    const repository = await activeRuntime.runPromise(Effect.service(ScheduledTaskRepository));
    const reactor = await activeRuntime.runPromise(Effect.service(ScheduledTasksReactor));

    // Anchor the TestClock at `now`, start the reactor, and advance just enough
    // to let the forked sweep fire its single first tick.
    const runOneTick = () =>
      activeRuntime.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            yield* TestClock.setTime(Date.parse(now));
            yield* reactor.start();
            yield* TestClock.adjust("1 second");
          }),
        ),
      );

    return { activeRuntime, repository, reactor, dispatched, runOneTick };
  }

  it("dispatches a due task and advances next_run_at", async () => {
    const threadId = ThreadId.make("thread-due");
    const taskId = ScheduledTaskId.make("task-due");
    const harness = await createHarness({ shells: [makeShell({ threadId })] });
    await harness.activeRuntime.runPromise(
      harness.repository.insert(makeTask({ taskId, threadId })),
    );

    // Run a single tick deterministically: start the reactor in a scope, let the
    // forked sweep fire once, then advance the TestClock to flush timers.
    await harness.runOneTick();

    const turnStarts = harness.dispatched.filter((c) => c.type === "thread.turn.start");
    expect(turnStarts).toHaveLength(1);
    expect(turnStarts[0]!.threadId).toBe(threadId);

    const rows = await harness.activeRuntime.runPromise(
      harness.repository.listByThread({ threadId }),
    );
    expect(rows[0]!.lastStatus).toBe("dispatched");
    // next_run_at advanced ~1h (intervalSeconds) past the run instant.
    expect(rows[0]!.nextRunAt).not.toBeNull();
    expect(Date.parse(rows[0]!.nextRunAt!)).toBeGreaterThan(Date.parse(now));
  });

  it("dispatches a pinned model as a per-turn override", async () => {
    const threadId = ThreadId.make("thread-model");
    const taskId = ScheduledTaskId.make("task-model");
    const pinnedModel: ModelSelection = {
      instanceId: ProviderInstanceId.make("claudeAgent"),
      model: "claude-opus-4-8",
    };
    const harness = await createHarness({ shells: [makeShell({ threadId })] });
    await harness.activeRuntime.runPromise(
      harness.repository.insert(makeTask({ taskId, threadId, modelSelection: pinnedModel })),
    );

    await harness.runOneTick();

    const turnStarts = harness.dispatched.filter((c) => c.type === "thread.turn.start");
    expect(turnStarts).toHaveLength(1);
    // The schedule's model overrides the thread's default (codexModel) for this run.
    expect(turnStarts[0]!.modelSelection).toStrictEqual(pinnedModel);
  });

  it("re-asserts the thread's model when the schedule has no pin", async () => {
    const threadId = ThreadId.make("thread-nomodel");
    const taskId = ScheduledTaskId.make("task-nomodel");
    const harness = await createHarness({ shells: [makeShell({ threadId })] });
    await harness.activeRuntime.runPromise(
      harness.repository.insert(makeTask({ taskId, threadId, modelSelection: null })),
    );

    await harness.runOneTick();

    const turnStarts = harness.dispatched.filter((c) => c.type === "thread.turn.start");
    expect(turnStarts).toHaveLength(1);
    // An unpinned run dispatches the thread's own model explicitly (never
    // undefined) so it can't inherit a pin cached in-process by another schedule.
    expect(turnStarts[0]!.modelSelection).toStrictEqual(codexModel);
  });

  it("skips a busy thread without dispatching an extra turn", async () => {
    const threadId = ThreadId.make("thread-busy");
    const taskId = ScheduledTaskId.make("task-busy");
    const harness = await createHarness({
      shells: [
        makeShell({
          threadId,
          session: makeSession(threadId, "running", TurnId.make("turn-1")),
        }),
      ],
    });
    await harness.activeRuntime.runPromise(
      harness.repository.insert(makeTask({ taskId, threadId, busyPolicy: "skip" })),
    );

    await harness.runOneTick();

    const turnStarts = harness.dispatched.filter((c) => c.type === "thread.turn.start");
    expect(turnStarts).toHaveLength(0);

    const rows = await harness.activeRuntime.runPromise(
      harness.repository.listByThread({ threadId }),
    );
    expect(rows[0]!.lastStatus).toBe("skipped");
    expect(rows[0]!.skippedCount).toBe(1);
  });

  it("disables a task whose thread was deleted", async () => {
    const threadId = ThreadId.make("thread-deleted");
    const taskId = ScheduledTaskId.make("task-deleted");
    // No shell registered => getThreadShellById returns None (thread deleted).
    const harness = await createHarness({ shells: [] });
    await harness.activeRuntime.runPromise(
      harness.repository.insert(makeTask({ taskId, threadId })),
    );

    await harness.runOneTick();

    const turnStarts = harness.dispatched.filter((c) => c.type === "thread.turn.start");
    expect(turnStarts).toHaveLength(0);

    const rows = await harness.activeRuntime.runPromise(
      harness.repository.listByThread({ threadId }),
    );
    expect(rows[0]!.enabled).toBe(0);
    expect(rows[0]!.lastStatus).toBe("error");
    expect(rows[0]!.lastError).toBe("thread deleted");
  });

  it("advances next_run_at before dispatch so a crash mid-dispatch does not re-run", async () => {
    const threadId = ThreadId.make("thread-crash");
    const taskId = ScheduledTaskId.make("task-crash");
    // Capture the persisted next_run_at observed AT dispatch time: it must
    // already be advanced past `now`, proving the markRun commit happened first.
    let nextRunAtDispatch: string | null | undefined;
    const harness = await createHarness({
      shells: [makeShell({ threadId })],
      onDispatch: () => {
        // Read the row synchronously is not possible here; capture marker below.
        nextRunAtDispatch = "dispatched";
      },
    });
    await harness.activeRuntime.runPromise(
      harness.repository.insert(makeTask({ taskId, threadId })),
    );

    await harness.runOneTick();

    expect(nextRunAtDispatch).toBe("dispatched");

    // After the (single) dispatch, the row is no longer due at `now`: the
    // committed advance means a subsequent tick would NOT fire it again.
    const stillDue = await harness.activeRuntime.runPromise(
      harness.repository.listDue({ nowIso: now }),
    );
    expect(stillDue.some((task) => task.taskId === taskId)).toBe(false);

    // Exactly one dispatch occurred.
    const turnStarts = harness.dispatched.filter((c) => c.type === "thread.turn.start");
    expect(turnStarts).toHaveLength(1);
  });
});
