/**
 * ChildThreadCoordinator - the never-hang core of the sub-agent layer.
 *
 * Tracks parent/child thread relationships in memory, settles a per-child
 * terminal `Deferred` exactly once from death/completion domain events, and
 * exposes a BOUNDED `waitSlice` so the `t3_wait_subagent` MCP tool never holds
 * a single long HTTP call (see finalPlan §5/C6). Detached children wake their
 * parent (idle -> dispatch a turn, mid-turn -> enqueue) under a per-parent
 * lock; pending injections drain when the parent next completes a turn.
 *
 * @module ChildThreadCoordinator
 */
import type {
  ModelSelection,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

import type { ThreadStartToolError } from "../../mcp/toolkits/thread/tools.ts";

/**
 * Bound on a single `waitSlice`: each MCP `t3_wait_subagent` invocation waits
 * at most this long before returning `pending` so the agent re-calls. Keeps
 * every HTTP call well within the cross-provider timeout tolerance (C6).
 */
export const WAIT_SLICE_SECONDS = 20;

/** Reject spawns at or beyond this ancestry depth (cycle/recursion guard). */
export const MAX_DEPTH = 8;

/**
 * A pending injection older than this is dispatched on the next parent tick
 * even if the parent never went idle, so it can never wait forever.
 */
export const PENDING_MAX_AGE_MS = 10 * 60 * 1_000;

/**
 * Hard upper bound on any synchronous one-shot projection read performed inside
 * `register`/`waitSlice` entry. If a projection query stalls, the read is
 * abandoned and the child is treated as not-yet-terminal so the caller falls
 * through to the bounded {@link WAIT_SLICE_SECONDS} Deferred race instead of
 * hanging on the projection (never-hang guarantee).
 */
export const PROJECTION_READ_TIMEOUT_MS = 500;

/** Terminal disposition of a child thread once its `Deferred` is settled. */
export type ChildTerminalStatus = "completed" | "failed" | "killed";

/** Settled terminal result captured at signal time (never re-races the projection). */
export interface ChildWaitResult {
  readonly childThreadId: ThreadId;
  readonly status: ChildTerminalStatus;
  readonly finalAssistantText: string | null;
  readonly error: string | null;
}

/** Per-child wait status surfaced to the MCP tool (adds the non-terminal states). */
export type WaitChildStatus = ChildTerminalStatus | "timeout" | "pending";

export interface WaitChildResult {
  readonly childThreadId: ThreadId;
  readonly status: WaitChildStatus;
  readonly finalAssistantText: string | null;
  readonly error: string | null;
}

export interface WaitSliceResult {
  readonly results: ReadonlyArray<WaitChildResult>;
  readonly settledCount: number;
  readonly timedOutCount: number;
  readonly pending: boolean;
  readonly resumeToken: string;
}

export interface RegisterChildInput {
  readonly parentThreadId: ThreadId;
  readonly childThreadId: ThreadId;
  readonly detached: boolean;
  readonly model: ModelSelection;
  readonly spawnedAtMs: number;
}

export type WaitMode = "all" | "any";

export interface WaitSliceInput {
  readonly childThreadIds: ReadonlyArray<ThreadId>;
  readonly mode: WaitMode;
  /** Logical wall-clock budget deadline (epoch ms); exhausted children -> `timeout`. */
  readonly budgetDeadlineMs: number;
  readonly resumeToken?: string;
}

export interface ChildListEntry {
  readonly childThreadId: ThreadId;
  readonly parentThreadId: ThreadId;
  readonly detached: boolean;
  readonly model: ModelSelection;
  readonly spawnedAtMs: number;
  readonly depth: number;
  readonly settled: boolean;
}

export interface ChildThreadCoordinatorShape {
  /**
   * Register a freshly spawned child. Fail-fast validates the provider
   * instance, enforces the depth/cycle guard, and performs a synchronous
   * one-shot terminal check so a child that finished before the hot stream
   * subscribed is settled immediately (closes the hot-subscribe race).
   */
  readonly register: (input: RegisterChildInput) => Effect.Effect<void, ThreadStartToolError>;

  /**
   * Wait at most {@link WAIT_SLICE_SECONDS} for the requested children. Always
   * returns exactly one row per requested id and never hangs: an unknown id
   * yields a terminal error row, a still-running child yields `pending` (or
   * `timeout` once the logical budget is exhausted).
   */
  readonly waitSlice: (input: WaitSliceInput) => Effect.Effect<WaitSliceResult>;

  /** Assert `child`'s registered parent is `parent`, else a tool error. */
  readonly assertParent: (
    parentThreadId: ThreadId,
    childThreadId: ThreadId,
  ) => Effect.Effect<void, ThreadStartToolError>;

  /** Whether the parent has queued sub-agent completion injections awaiting drain. */
  readonly hasPendingInjections: (parentThreadId: ThreadId) => Effect.Effect<boolean>;

  /** List the parent's registered children (in-memory view). */
  readonly listChildren: (
    parentThreadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<ChildListEntry>>;

  /**
   * Reconcile from the persisted log, then fork the hot event stream. MUST run
   * before any MCP endpoint is live so no terminal signal is missed.
   */
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;

  /** Resolves once the in-flight event processing has quiesced (test hook). */
  readonly drain: Effect.Effect<void>;
}

export class ChildThreadCoordinator extends Context.Service<
  ChildThreadCoordinator,
  ChildThreadCoordinatorShape
>()("t3/orchestration/Services/ChildThreadCoordinator") {}

/** Re-exported for layer/test code that builds `ModelSelection` instance ids. */
export type { ModelSelection, ProviderInstanceId };
