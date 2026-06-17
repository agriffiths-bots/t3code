/**
 * ChildThreadCoordinator implementation - see Services/ChildThreadCoordinator.ts
 * and finalPlan §5 for the design. The `ActiveChildThreadCoordinatorLive`
 * global-capture mirrors `ActiveBootstrapTurnStartDispatcherLive` /
 * `ThreadStartRuntimeLive` so MCP tool handlers can reach the coordinator
 * without threading it through the toolkit `Context`.
 */
import {
  CommandId,
  EventId,
  MessageId,
  type ModelSelection,
  type OrchestrationEvent,
  type OrchestrationThread,
  type OrchestrationThreadShell,
  type ThreadId,
} from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import {
  dispatchActive,
  type BootstrapTurnStartDispatcherShape,
} from "../Services/BootstrapTurnStartDispatcher.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { ProviderInstanceRegistry } from "../../provider/Services/ProviderInstanceRegistry.ts";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import * as Schema from "effect/Schema";
import { ThreadStartToolError } from "../../mcp/toolkits/thread/tools.ts";
import {
  ChildThreadCoordinator,
  MAX_DEPTH,
  PENDING_MAX_AGE_MS,
  PROJECTION_READ_TIMEOUT_MS,
  WAIT_SLICE_SECONDS,
  type ChildListEntry,
  type ChildTerminalStatus,
  type ChildThreadCoordinatorShape,
  type ChildWaitResult,
  type RegisterChildInput,
  type WaitChildResult,
  type WaitSliceInput,
  type WaitSliceResult,
} from "../Services/ChildThreadCoordinator.ts";

interface ChildRecord {
  readonly parentThreadId: ThreadId;
  readonly detached: boolean;
  readonly model: ModelSelection;
  readonly spawnedAtMs: number;
  readonly depth: number;
  readonly terminal: Deferred.Deferred<ChildWaitResult>;
}

interface PendingInjection {
  readonly childThreadId: ThreadId;
  readonly status: ChildTerminalStatus;
  readonly text: string | null;
  readonly error: string | null;
  readonly enqueuedAtMs: number;
}

type TurnDiffCompletedEvent = Extract<OrchestrationEvent, { type: "thread.turn-diff-completed" }>;
type SessionSetEvent = Extract<OrchestrationEvent, { type: "thread.session-set" }>;
type ThreadDeletedEvent = Extract<OrchestrationEvent, { type: "thread.deleted" }>;

const ChildRowSchema = Schema.Struct({
  threadId: Schema.String,
  parentThreadId: Schema.NullOr(Schema.String),
});

const fail = (message: string) => new ThreadStartToolError({ message });

/** Cap on a single consolidated wake/drain injection turn (truncated past this). */
const CONSOLIDATED_INJECTION_MAX_CHARS = 2_000;

/**
 * Pick the latest assistant message authored at or after the latest turn was
 * requested (lens-3 guard so a stale assistant message from a prior turn is
 * never reported as this turn's output).
 */
const finalAssistantTextFromThread = (thread: OrchestrationThread): string | null => {
  const latestTurn = thread.latestTurn;
  if (!latestTurn) return null;
  let chosen: string | null = null;
  for (const message of thread.messages) {
    if (message.role !== "assistant") continue;
    if (message.createdAt < latestTurn.requestedAt) continue;
    chosen = message.text;
  }
  return chosen;
};

const isThreadIdle = (shell: OrchestrationThreadShell): boolean => {
  const turnRunning = shell.latestTurn?.state === "running";
  if (turnRunning) return false;
  const session = shell.session;
  if (session === null) return true;
  return (
    (session.status === "ready" || session.status === "stopped") && session.activeTurnId === null
  );
};

/**
 * "turnCount == 0" parent guard (bug #2336): a parent that has never run a turn
 * and has no session must NEVER be resumed; it can only be enqueued.
 */
const isFreshParent = (shell: OrchestrationThreadShell): boolean =>
  shell.latestTurn === null && shell.session === null;

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const orchestrationEngine = yield* OrchestrationEngineService;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const registry = yield* ProviderInstanceRegistry;
  const sql = yield* SqlClient;

  const children = new Map<ThreadId, ChildRecord>();
  const byParent = new Map<ThreadId, Set<ThreadId>>();
  const pendingInjections = new Map<ThreadId, Array<PendingInjection>>();
  const parentWakeLocks = new Map<ThreadId, Semaphore.Semaphore>();

  const nowMillis = Effect.clockWith((clock) => clock.currentTimeMillis);
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const randomUUID = crypto.randomUUIDv4.pipe(Effect.orDie);
  const newCommandId = (tag: string) =>
    randomUUID.pipe(Effect.map((uuid) => CommandId.make(`server:${tag}:${uuid}`)));

  const listPersistedChildRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ChildRowSchema,
    execute: () =>
      sql`
        SELECT
          thread_id AS "threadId",
          parent_thread_id AS "parentThreadId"
        FROM projection_threads
        WHERE parent_thread_id IS NOT NULL
      `,
  });

  const wakeLockFor = (parentThreadId: ThreadId): Effect.Effect<Semaphore.Semaphore> => {
    const existing = parentWakeLocks.get(parentThreadId);
    if (existing) return Effect.succeed(existing);
    return Semaphore.make(1).pipe(
      Effect.tap((semaphore) => Effect.sync(() => parentWakeLocks.set(parentThreadId, semaphore))),
    );
  };

  const trackChild = (childThreadId: ThreadId, record: ChildRecord) => {
    children.set(childThreadId, record);
    const siblings = byParent.get(record.parentThreadId) ?? new Set<ThreadId>();
    siblings.add(childThreadId);
    byParent.set(record.parentThreadId, siblings);
  };

  const enqueuePending = (parentThreadId: ThreadId, entry: PendingInjection) => {
    const queue = pendingInjections.get(parentThreadId) ?? [];
    queue.push(entry);
    pendingInjections.set(parentThreadId, queue);
  };

  const depthFor = (parentThreadId: ThreadId): number => {
    const parentRecord = children.get(parentThreadId);
    return parentRecord ? parentRecord.depth + 1 : 1;
  };

  // Walk the ancestry chain via recorded parent links; a repeated id is a cycle.
  const hasAncestryCycle = (parentThreadId: ThreadId, childThreadId: ThreadId): boolean => {
    if (parentThreadId === childThreadId) return true;
    const seen = new Set<ThreadId>([childThreadId]);
    let cursor: ThreadId | undefined = parentThreadId;
    while (cursor !== undefined) {
      if (seen.has(cursor)) return true;
      seen.add(cursor);
      cursor = children.get(cursor)?.parentThreadId;
    }
    return false;
  };

  const getThreadShell = (threadId: ThreadId) =>
    projectionSnapshotQuery.getThreadShellById(threadId).pipe(Effect.orDie);

  const getThreadDetail = (threadId: ThreadId) =>
    projectionSnapshotQuery.getThreadDetailById(threadId).pipe(Effect.orDie);

  // Bounded projection read for the synchronous request path (register +
  // waitSlice entry). A stalled projection must never block past the slice
  // timeout, so a read that exceeds PROJECTION_READ_TIMEOUT_MS resolves to
  // Option.none (treated as "not yet terminal"; the caller falls through to the
  // bounded Deferred race). The unbounded variants stay for the worker fiber,
  // which is off the request path.
  const getThreadShellBounded = (threadId: ThreadId) =>
    getThreadShell(threadId).pipe(
      Effect.timeoutOption(`${PROJECTION_READ_TIMEOUT_MS} millis`),
      Effect.map(Option.flatten),
    );

  const getThreadDetailBounded = (threadId: ThreadId) =>
    getThreadDetail(threadId).pipe(
      Effect.timeoutOption(`${PROJECTION_READ_TIMEOUT_MS} millis`),
      Effect.map(Option.flatten),
    );

  // Settle a child terminal Deferred exactly once. Deferred.succeed is a no-op
  // when already settled, which makes every signal path idempotent.
  const settleChild = (
    childThreadId: ThreadId,
    status: ChildTerminalStatus,
    error: string | null,
    bounded = false,
  ) =>
    Effect.gen(function* () {
      const record = children.get(childThreadId);
      if (!record) return;
      const detail = yield* (bounded ? getThreadDetailBounded : getThreadDetail)(childThreadId);
      const finalAssistantText = Option.match(detail, {
        onNone: () => null,
        onSome: finalAssistantTextFromThread,
      });
      const settled = yield* Deferred.succeed(record.terminal, {
        childThreadId,
        status,
        finalAssistantText,
        error,
      });
      if (settled && record.detached) {
        yield* wakeParent(record, { childThreadId, status, finalAssistantText, error });
      }
    });

  const consolidatedInjectionText = (entries: ReadonlyArray<PendingInjection>): string => {
    const joined = entries
      .map((entry) => `[sub-agent ${entry.childThreadId} ${entry.status}] ${entry.error ?? entry.text ?? ""}`)
      .join("\n");
    // Guard against unbounded growth when many children settle with large
    // payloads; the full per-child results remain queryable via t3_check.
    if (joined.length > CONSOLIDATED_INJECTION_MAX_CHARS) {
      return `${joined.slice(0, CONSOLIDATED_INJECTION_MAX_CHARS)}\n[...${entries.length} sub-agent results truncated; use t3_check_subagent for full output]`;
    }
    return joined;
  };

  const dispatchParentTurn = (shell: OrchestrationThreadShell, text: string) =>
    Effect.gen(function* () {
      const commandId = yield* newCommandId("subagent-wake");
      const messageId = MessageId.make(yield* randomUUID);
      const createdAt = yield* nowIso;
      yield* dispatchActive({
        type: "thread.turn.start",
        commandId,
        threadId: shell.id,
        message: { messageId, role: "user", text, attachments: [] },
        runtimeMode: shell.runtimeMode,
        interactionMode: shell.interactionMode,
        createdAt,
      });
    });

  const appendSubagentActivity = (parentThreadId: ThreadId, result: ChildWaitResult) =>
    Effect.gen(function* () {
      const commandId = yield* newCommandId("subagent-activity");
      const activityId = EventId.make(yield* randomUUID);
      const createdAt = yield* nowIso;
      yield* orchestrationEngine.dispatch({
        type: "thread.activity.append",
        commandId,
        threadId: parentThreadId,
        activity: {
          id: activityId,
          tone: result.status === "completed" ? "info" : "error",
          kind: "subagent.completed",
          summary: `Sub-agent ${result.childThreadId} ${result.status}`,
          payload: {
            childThreadId: result.childThreadId,
            status: result.status,
            error: result.error,
          },
          turnId: null,
          createdAt,
        },
        createdAt,
      });
    });

  // Atomic per-parent idle-check + dispatch: never resume a turnCount-0 parent,
  // resume an idle parent with the consolidated text, otherwise enqueue.
  //
  // INVARIANT (non-reentrant lock): the parent-wake Semaphore is NOT reentrant.
  // `dispatchActive`/`orchestrationEngine.dispatch` MUST be asynchronous — the
  // resulting parent turn-diff-completed event is published on the hot stream
  // and processed on the worker fiber, so `drainPending` (which acquires the
  // same per-parent lock) can only run AFTER this lock is released. If dispatch
  // were ever made synchronous and re-entered drainPending for this parent
  // inline, it would deadlock; the WakeParentSyncDispatch regression test guards
  // this by enqueuing the parent's terminal signal during dispatch and asserting
  // wakeParent still completes.
  const wakeParent = (record: ChildRecord, result: ChildWaitResult) =>
    Effect.gen(function* () {
      const parentThreadId = record.parentThreadId;
      const lock = yield* wakeLockFor(parentThreadId);
      yield* lock.withPermits(1)(
        Effect.gen(function* () {
          const now = yield* nowMillis;
          const entry: PendingInjection = {
            childThreadId: result.childThreadId,
            status: result.status,
            text: result.finalAssistantText,
            error: result.error,
            enqueuedAtMs: now,
          };
          const shellOption = yield* getThreadShell(parentThreadId);
          if (Option.isNone(shellOption)) {
            enqueuePending(parentThreadId, entry);
            yield* Effect.logWarning("subagent wake parent not found; enqueued injection", {
              parentThreadId,
              childThreadId: result.childThreadId,
            });
            return;
          }
          const shell = shellOption.value;
          if (isFreshParent(shell)) {
            enqueuePending(parentThreadId, entry);
            return;
          }
          if (isThreadIdle(shell)) {
            yield* dispatchParentTurn(shell, consolidatedInjectionText([entry])).pipe(
              Effect.catchCause((cause) => {
                enqueuePending(parentThreadId, entry);
                return Effect.logWarning("subagent wake dispatch failed; enqueued injection", {
                  parentThreadId,
                  childThreadId: result.childThreadId,
                  cause: Cause.pretty(cause),
                });
              }),
            );
            return;
          }
          yield* appendSubagentActivity(parentThreadId, result).pipe(Effect.ignoreCause({ log: true }));
          enqueuePending(parentThreadId, entry);
        }),
      );
    });

  // Drain pending injections for a parent that just completed a turn (or whose
  // oldest entry has aged past PENDING_MAX_AGE_MS). One consolidated turn.
  const drainPending = (parentThreadId: ThreadId) =>
    Effect.gen(function* () {
      const lock = yield* wakeLockFor(parentThreadId);
      yield* lock.withPermits(1)(
        Effect.gen(function* () {
          const queue = pendingInjections.get(parentThreadId);
          if (!queue || queue.length === 0) return;
          const shellOption = yield* getThreadShell(parentThreadId);
          if (Option.isNone(shellOption)) return;
          const shell = shellOption.value;
          if (isFreshParent(shell)) {
            yield* Effect.logWarning(
              "parent became fresh while draining pending injections; deferring",
              { parentThreadId, pendingCount: queue.length },
            );
            return;
          }
          const entries = [...queue];
          pendingInjections.delete(parentThreadId);
          yield* dispatchParentTurn(shell, consolidatedInjectionText(entries)).pipe(
            Effect.catchCause((cause) => {
              // Restore the entries so a transient dispatch failure is retried.
              const restored = pendingInjections.get(parentThreadId) ?? [];
              pendingInjections.set(parentThreadId, [...entries, ...restored]);
              return Effect.logWarning("subagent pending drain dispatch failed; re-enqueued", {
                parentThreadId,
                cause: Cause.pretty(cause),
              });
            }),
          );
        }),
      );
    });

  // Safety valve: flush any parent whose oldest pending entry has aged out, even
  // if that parent never completes another turn.
  const drainAgedPending = Effect.gen(function* () {
    const now = yield* nowMillis;
    const parents: Array<ThreadId> = [];
    for (const [parentThreadId, queue] of pendingInjections) {
      if (queue.some((entry) => now - entry.enqueuedAtMs >= PENDING_MAX_AGE_MS)) {
        parents.push(parentThreadId);
      }
    }
    yield* Effect.forEach(parents, drainPending, { discard: true });
  });

  const handleTurnDiffCompleted = (event: TurnDiffCompletedEvent) =>
    Effect.gen(function* () {
      const { threadId, status } = event.payload;
      if (children.has(threadId)) {
        if (status === "ready") {
          yield* settleChild(threadId, "completed", null);
        } else {
          yield* settleChild(threadId, "failed", `turn diff ${status}`);
        }
      }
      // A parent completing a turn drains its pending injections.
      if (pendingInjections.has(threadId)) {
        yield* drainPending(threadId);
      }
    });

  const handleSessionSet = (event: SessionSetEvent) =>
    Effect.gen(function* () {
      const { threadId, session } = event.payload;
      const record = children.get(threadId);
      if (!record) return;
      if (session.status !== "stopped" && session.status !== "error") return;
      const shellOption = yield* getThreadShell(threadId);
      const turnRunning = Option.match(shellOption, {
        onNone: () => false,
        onSome: (shell) => shell.latestTurn?.state === "running",
      });
      if (turnRunning) return;
      yield* settleChild(threadId, "failed", `session ${session.status}`);
    });

  const handleThreadDeleted = (event: ThreadDeletedEvent) =>
    Effect.gen(function* () {
      const { threadId } = event.payload;
      if (children.has(threadId)) {
        yield* settleChild(threadId, "killed", "thread deleted");
      }
    });

  const processEvent = (event: OrchestrationEvent) => {
    switch (event.type) {
      case "thread.turn-diff-completed":
        return handleTurnDiffCompleted(event);
      case "thread.session-set":
        return handleSessionSet(event);
      case "thread.deleted":
        return handleThreadDeleted(event);
      default:
        return Effect.void;
    }
  };

  const processEventSafely = (event: OrchestrationEvent) =>
    processEvent(event).pipe(
      Effect.andThen(drainAgedPending),
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Effect.failCause(cause);
        }
        return Effect.logWarning("child thread coordinator failed to process event", {
          eventType: event.type,
          cause: Cause.pretty(cause),
        });
      }),
    );

  const worker = yield* makeDrainableWorker(processEventSafely);

  // Synchronous one-shot terminal check (register + waitSlice entry): if the
  // projection already shows the child terminal, settle now without waiting.
  const oneShotTerminalCheck = (childThreadId: ThreadId) =>
    Effect.gen(function* () {
      const record = children.get(childThreadId);
      if (!record) return;
      const done = yield* Deferred.isDone(record.terminal);
      if (done) return;
      const shellOption = yield* getThreadShellBounded(childThreadId);
      if (Option.isNone(shellOption)) return;
      const shell = shellOption.value;
      if (shell.latestTurn === null) return;
      const turnState = shell.latestTurn.state;
      if (turnState === "running") return;
      if (turnState === "completed") {
        yield* settleChild(childThreadId, "completed", null, true);
      } else {
        yield* settleChild(childThreadId, "failed", `turn ${turnState}`, true);
      }
    });

  const register: ChildThreadCoordinatorShape["register"] = (input) =>
    Effect.gen(function* () {
      if (children.has(input.childThreadId)) {
        return;
      }
      const depth = depthFor(input.parentThreadId);
      if (depth >= MAX_DEPTH) {
        return yield* fail(`Sub-agent depth limit (${MAX_DEPTH}) reached; refusing to spawn deeper.`);
      }
      if (hasAncestryCycle(input.parentThreadId, input.childThreadId)) {
        return yield* fail("Sub-agent spawn would create an ancestry cycle.");
      }
      const instance = yield* registry.getInstance(input.model.instanceId);
      if (instance === undefined) {
        return yield* fail(`Provider instance "${input.model.instanceId}" is not available.`);
      }
      const terminal = yield* Deferred.make<ChildWaitResult>();
      const record: ChildRecord = {
        parentThreadId: input.parentThreadId,
        detached: input.detached,
        model: input.model,
        spawnedAtMs: input.spawnedAtMs,
        depth,
        terminal,
      };
      trackChild(input.childThreadId, record);
      yield* wakeLockFor(input.parentThreadId);
      // Hot-subscribe race: the child may already be terminal in the projection.
      yield* oneShotTerminalCheck(input.childThreadId);
    });

  const assertParent: ChildThreadCoordinatorShape["assertParent"] = (
    parentThreadId,
    childThreadId,
  ) =>
    Effect.gen(function* () {
      const record = children.get(childThreadId);
      if (record && record.parentThreadId === parentThreadId) return;
      const shellOption = yield* getThreadShell(childThreadId);
      const matches = Option.match(shellOption, {
        onNone: () => false,
        onSome: (shell) => shell.parentThreadId === parentThreadId,
      });
      if (matches) return;
      return yield* fail(`Thread ${childThreadId} is not a child of ${parentThreadId}.`);
    });

  const hasPendingInjections: ChildThreadCoordinatorShape["hasPendingInjections"] = (
    parentThreadId,
  ) => Effect.sync(() => (pendingInjections.get(parentThreadId)?.length ?? 0) > 0);

  const listChildren: ChildThreadCoordinatorShape["listChildren"] = (parentThreadId) =>
    Effect.gen(function* () {
      const ids = byParent.get(parentThreadId);
      if (!ids) return [];
      const entries: Array<ChildListEntry> = [];
      for (const childThreadId of ids) {
        const record = children.get(childThreadId);
        if (!record) continue;
        const settled = yield* Deferred.isDone(record.terminal);
        entries.push({
          childThreadId,
          parentThreadId,
          detached: record.detached,
          model: record.model,
          spawnedAtMs: record.spawnedAtMs,
          depth: record.depth,
          settled,
        });
      }
      return entries;
    });

  const waitForChild = (
    childThreadId: ThreadId,
  ): Effect.Effect<WaitChildResult> =>
    Effect.gen(function* () {
      if (!children.has(childThreadId)) {
        const shellOption = yield* getThreadShellBounded(childThreadId);
        // Re-check in case register() ran concurrently between the map lookup
        // and the (bounded) projection read; if so, fall through to the normal
        // tracked path rather than reporting a misleading error.
        if (!children.has(childThreadId)) {
          if (Option.isNone(shellOption)) {
            // Never registered AND not in projection: terminal error, never hang.
            return {
              childThreadId,
              status: "failed" as const,
              finalAssistantText: null,
              error: "Unknown sub-agent thread; it was never registered.",
            } satisfies WaitChildResult;
          }
          return {
            childThreadId,
            status: "failed" as const,
            finalAssistantText: null,
            error:
              "Sub-agent thread exists in the projection but is not tracked by this server instance.",
          } satisfies WaitChildResult;
        }
      }
      // Re-check the projection in case it caught up after the hot-subscribe gap.
      yield* oneShotTerminalCheck(childThreadId);
      const record = children.get(childThreadId);
      if (!record) {
        // Registered then untracked mid-call (should not happen): never hang.
        return {
          childThreadId,
          status: "failed" as const,
          finalAssistantText: null,
          error: "Sub-agent thread is no longer tracked by this server instance.",
        } satisfies WaitChildResult;
      }
      const timeoutSentinel = Symbol.for("t3/subagent/wait-slice-timeout");
      const raced = yield* Effect.race(
        Deferred.await(record.terminal),
        Effect.sleep(`${WAIT_SLICE_SECONDS} seconds`).pipe(Effect.as(timeoutSentinel)),
      );
      if (raced === timeoutSentinel) {
        return {
          childThreadId,
          status: "pending" as const,
          finalAssistantText: null,
          error: null,
        } satisfies WaitChildResult;
      }
      return {
        childThreadId,
        status: raced.status,
        finalAssistantText: raced.finalAssistantText,
        error: raced.error,
      } satisfies WaitChildResult;
    });

  const waitSlice: ChildThreadCoordinatorShape["waitSlice"] = (input: WaitSliceInput) =>
    Effect.gen(function* () {
      const sliceResults = yield* Effect.forEach(input.childThreadIds, waitForChild, {
        concurrency: "unbounded",
      });
      const now = yield* nowMillis;
      const budgetExhausted = now >= input.budgetDeadlineMs;
      const results: Array<WaitChildResult> = sliceResults.map((result) => {
        if (result.status === "pending" && budgetExhausted) {
          return {
            childThreadId: result.childThreadId,
            status: "timeout" as const,
            finalAssistantText: null,
            error: `wait exceeded budget`,
          } satisfies WaitChildResult;
        }
        return result;
      });
      const settledCount = results.filter(
        (result) => result.status !== "pending" && result.status !== "timeout",
      ).length;
      const timedOutCount = results.filter((result) => result.status === "timeout").length;
      const pendingCount = results.filter((result) => result.status === "pending").length;
      const pending =
        input.mode === "any" ? settledCount === 0 && pendingCount > 0 : pendingCount > 0;
      const resumeToken = yield* randomUUID;
      return {
        results,
        settledCount,
        timedOutCount,
        pending,
        resumeToken,
      } satisfies WaitSliceResult;
    });

  // Reconcile terminal-ness from the PERSISTED log (not the lagging projection):
  // replay readEvents(0), tracking the latest signal per known child id.
  const reconcileFromLog = (knownChildIds: Set<ThreadId>) =>
    Effect.gen(function* () {
      const terminalByChild = new Map<ThreadId, { status: ChildTerminalStatus; error: string | null }>();
      const runningByChild = new Map<ThreadId, boolean>();
      let maxSequence = 0;
      yield* Stream.runForEach(orchestrationEngine.readEvents(0), (event) =>
        Effect.sync(() => {
          const sequence = (event as { sequence?: number }).sequence;
          if (typeof sequence === "number" && sequence > maxSequence) {
            maxSequence = sequence;
          }
          switch (event.type) {
            case "thread.turn-diff-completed": {
              const { threadId, status } = event.payload;
              if (!knownChildIds.has(threadId)) return;
              terminalByChild.set(
                threadId,
                status === "ready"
                  ? { status: "completed", error: null }
                  : { status: "failed", error: `turn diff ${status}` },
              );
              runningByChild.set(threadId, false);
              return;
            }
            case "thread.turn-start-requested": {
              const { threadId } = event.payload;
              if (!knownChildIds.has(threadId)) return;
              runningByChild.set(threadId, true);
              terminalByChild.delete(threadId);
              return;
            }
            case "thread.session-set": {
              const { threadId, session } = event.payload;
              if (!knownChildIds.has(threadId)) return;
              if (
                (session.status === "stopped" || session.status === "error") &&
                runningByChild.get(threadId) !== true
              ) {
                terminalByChild.set(threadId, {
                  status: "failed",
                  error: `session ${session.status}`,
                });
              }
              return;
            }
            case "thread.deleted": {
              const { threadId } = event.payload;
              if (!knownChildIds.has(threadId)) return;
              terminalByChild.set(threadId, { status: "killed", error: "thread deleted" });
              return;
            }
            default:
              return;
          }
        }),
      ).pipe(Effect.orDie);
      return { terminalByChild, maxSequence };
    });

  const start: ChildThreadCoordinatorShape["start"] = Effect.fn("ChildThreadCoordinator.start")(
    function* () {
      // (1) Load all parent-linked children from the projection.
      const rows = yield* listPersistedChildRows().pipe(Effect.orDie);
      const knownChildIds = new Set<ThreadId>();
      for (const row of rows) {
        if (row.parentThreadId === null) continue;
        const childThreadId = row.threadId as ThreadId;
        const parentThreadId = row.parentThreadId as ThreadId;
        if (children.has(childThreadId)) continue;
        const detailOption = yield* getThreadDetail(childThreadId);
        if (Option.isNone(detailOption)) {
          // No detail row yet (projection lag): do NOT fabricate a model or
          // settle this child. It will be validated when it calls register().
          // Settling it here on a fabricated "unknown" instance would wrongly
          // kill a child that is still running (wake CRITICAL #2).
          continue;
        }
        const detail = detailOption.value;
        const terminal = yield* Deferred.make<ChildWaitResult>();
        trackChild(childThreadId, {
          parentThreadId,
          detached: true,
          model: detail.modelSelection,
          spawnedAtMs: 0,
          depth: 1,
          terminal,
        });
        yield* wakeLockFor(parentThreadId);
        knownChildIds.add(childThreadId);
      }

      // (2) Determine terminal-ness from the persisted immutable log.
      const { terminalByChild } = yield* reconcileFromLog(knownChildIds);
      for (const [childThreadId, outcome] of terminalByChild) {
        yield* settleChild(childThreadId, outcome.status, outcome.error);
      }

      // Non-terminal children: validate the provider instance still exists.
      for (const childThreadId of knownChildIds) {
        if (terminalByChild.has(childThreadId)) continue;
        const record = children.get(childThreadId);
        if (!record) continue;
        const instance = yield* registry.getInstance(record.model.instanceId);
        if (instance === undefined) {
          yield* Effect.logWarning(
            "reconciled non-terminal sub-agent lost its provider instance; terminating",
            {
              childThreadId,
              instanceId: record.model.instanceId,
              parentThreadId: record.parentThreadId,
            },
          );
          yield* settleChild(childThreadId, "killed", "provider instance removed");
        }
      }

      // (3) THEN fork the hot stream (the immutable-log scan above already
      // covered everything up to "now", so a gap event is never missed).
      yield* Effect.forkScoped(
        Stream.runForEach(orchestrationEngine.streamDomainEvents, (event) =>
          worker.enqueue(event),
        ),
      );

      yield* Effect.logInfo("child.thread.coordinator.reactor.started", {
        reconciledChildren: knownChildIds.size,
      });
    },
  );

  return {
    register,
    waitSlice,
    assertParent,
    hasPendingInjections,
    listChildren,
    start,
    drain: worker.drain,
  } satisfies ChildThreadCoordinatorShape;
});

export const ChildThreadCoordinatorLive = Layer.effect(ChildThreadCoordinator, make);

let activeCoordinator: ChildThreadCoordinatorShape | null = null;

/** Reach the live coordinator from MCP tool handlers (mirrors `dispatchActive`). */
export const coordinatorActive = (): ChildThreadCoordinatorShape | null => activeCoordinator;

export const ActiveChildThreadCoordinatorLive = Layer.effectDiscard(
  Effect.acquireRelease(
    ChildThreadCoordinator.pipe(
      Effect.tap((coordinator) =>
        Effect.sync(() => {
          activeCoordinator = coordinator;
        }),
      ),
    ),
    (coordinator) =>
      Effect.sync(() => {
        if (activeCoordinator === coordinator) activeCoordinator = null;
      }),
  ),
);

// Referenced for type alignment with the dispatcher shape used by dispatchActive.
export type { BootstrapTurnStartDispatcherShape };
