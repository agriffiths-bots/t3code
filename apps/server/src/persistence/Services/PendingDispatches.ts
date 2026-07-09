/**
 * PendingDispatchRepository - Repository interface for durable pending dispatches.
 *
 * Owns persistence operations for the plain (non event-sourced)
 * `pending_dispatches` table that backs the sub-agent coordinator's
 * restart-safe wake/steer/turn delivery (R-B). A row records either a parent
 * injection (a child completion that must wake the parent), a child steer
 * (a provider-deferred steer awaiting the child going idle), or an accepted
 * thread turn whose provider send must wait for the current turn to go idle.
 *
 * @module PendingDispatchRepository
 */
import { IsoDateTime, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { ProjectionRepositoryError } from "../Errors.ts";

export const PendingDispatchId = Schema.String.pipe(Schema.brand("PendingDispatchId"));
export type PendingDispatchId = typeof PendingDispatchId.Type;

export const PendingDispatchKind = Schema.Literals([
  "parent_injection",
  "child_steer",
  "thread_turn",
]);
export type PendingDispatchKind = typeof PendingDispatchKind.Type;

export const PendingDispatch = Schema.Struct({
  id: PendingDispatchId,
  kind: PendingDispatchKind,
  targetThreadId: ThreadId,
  sourceChildId: Schema.NullOr(ThreadId),
  text: Schema.NullOr(Schema.String),
  error: Schema.NullOr(Schema.String),
  status: Schema.NullOr(Schema.String),
  /**
   * The orchestration command id this row's wake/steer turn was dispatched
   * under, claimed durably BEFORE the dispatch (R-B exactly-once). Null means
   * the row has not yet been dispatched and is free to be (re)batched under a
   * fresh deterministic id; non-null means it must be re-dispatched under THIS
   * exact id on restart so the engine's receipt dedup makes a landed turn a
   * no-op and an un-landed turn fire — independent of how rows re-batch.
   */
  commandId: Schema.NullOr(Schema.String),
  /**
   * True when a foreground promoted child result has already been returned by
   * t3_wait_subagent. The row remains durable until the parent turn completes;
   * then the coordinator prunes it instead of dispatching a duplicate wake.
   */
  deliveredByWait: Schema.Boolean,
  /**
   * True only for parent wakes created when a foreground wait is promoted to the
   * wake-on-completion path. Detached child wakes remain non-cancellable by wait.
   */
  waitCancellable: Schema.Boolean,
  createdAt: IsoDateTime,
});
export type PendingDispatch = typeof PendingDispatch.Type;

export const ClaimPendingDispatchesInput = Schema.Struct({
  ids: Schema.Array(PendingDispatchId),
  commandId: Schema.String,
});
export type ClaimPendingDispatchesInput = typeof ClaimPendingDispatchesInput.Type;

export const MarkPendingDispatchesWaitDeliveredInput = Schema.Struct({
  ids: Schema.Array(PendingDispatchId),
});
export type MarkPendingDispatchesWaitDeliveredInput =
  typeof MarkPendingDispatchesWaitDeliveredInput.Type;

export const ResetPendingDispatchClaimsInput = Schema.Struct({
  ids: Schema.Array(PendingDispatchId),
});
export type ResetPendingDispatchClaimsInput = typeof ResetPendingDispatchClaimsInput.Type;

export const ListPendingDispatchesByTargetInput = Schema.Struct({
  kind: PendingDispatchKind,
  targetThreadId: ThreadId,
});
export type ListPendingDispatchesByTargetInput = typeof ListPendingDispatchesByTargetInput.Type;

/**
 * PendingDispatchRepositoryShape - Service API for pending dispatch persistence.
 */
export interface PendingDispatchRepositoryShape {
  /**
   * Insert a new pending dispatch row.
   */
  readonly insert: (row: PendingDispatch) => Effect.Effect<void, ProjectionRepositoryError>;

  /**
   * List pending dispatch rows for a given kind and target thread, oldest first.
   */
  readonly listByTarget: (
    input: ListPendingDispatchesByTargetInput,
  ) => Effect.Effect<ReadonlyArray<PendingDispatch>, ProjectionRepositoryError>;

  /**
   * List all pending dispatch rows, oldest first.
   */
  readonly listAll: () => Effect.Effect<ReadonlyArray<PendingDispatch>, ProjectionRepositoryError>;

  /**
   * Durably stamp the command id a batch of rows is being dispatched under,
   * BEFORE the orchestration dispatch (R-B exactly-once claim). A no-op for an
   * empty id list.
   */
  readonly claim: (
    input: ClaimPendingDispatchesInput,
  ) => Effect.Effect<void, ProjectionRepositoryError>;

  /**
   * Durably mark rows whose child result was delivered through t3_wait_subagent
   * before the parent turn committed. A no-op for an empty id list.
   */
  readonly markWaitDelivered: (
    input: MarkPendingDispatchesWaitDeliveredInput,
  ) => Effect.Effect<void, ProjectionRepositoryError>;

  /**
   * Clear durable claim ids so crash-recovered rows whose provider delivery was not proven can retry.
   */
  readonly resetClaims: (
    input: ResetPendingDispatchClaimsInput,
  ) => Effect.Effect<void, ProjectionRepositoryError>;

  /**
   * Hard-delete pending dispatch rows by id. A no-op for an empty id list.
   */
  readonly deleteByIds: (
    ids: ReadonlyArray<PendingDispatchId>,
  ) => Effect.Effect<void, ProjectionRepositoryError>;
}

/**
 * PendingDispatchRepository - Service tag for pending dispatch persistence.
 */
export class PendingDispatchRepository extends Context.Service<
  PendingDispatchRepository,
  PendingDispatchRepositoryShape
>()("t3/persistence/Services/PendingDispatches/PendingDispatchRepository") {}
