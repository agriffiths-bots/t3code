/**
 * PendingDispatchRepository - Repository interface for durable pending dispatches.
 *
 * Owns persistence operations for the plain (non event-sourced)
 * `pending_dispatches` table that backs the sub-agent coordinator's
 * restart-safe wake/steer delivery (R-B). A row records either a parent
 * injection (a child completion that must wake the parent) or a child steer
 * (a provider-deferred steer awaiting the child going idle).
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

export const PendingDispatchKind = Schema.Literals(["parent_injection", "child_steer"]);
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
  createdAt: IsoDateTime,
});
export type PendingDispatch = typeof PendingDispatch.Type;

export const ClaimPendingDispatchesInput = Schema.Struct({
  ids: Schema.Array(PendingDispatchId),
  commandId: Schema.String,
});
export type ClaimPendingDispatchesInput = typeof ClaimPendingDispatchesInput.Type;

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
