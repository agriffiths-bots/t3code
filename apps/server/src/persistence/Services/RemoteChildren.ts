/**
 * RemoteChildRepository - Caller-side index of sub-agents spawned on peers.
 *
 * The child thread lives in the remote backend's database. This table gives the
 * parent backend enough durable state to proxy status/wait/list and to poll for
 * wake delivery after restarts.
 *
 * @module RemoteChildren
 */
import { EnvironmentId, IsoDateTime, ThreadId, TrimmedNonEmptyString } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { ProjectionRepositoryError } from "../Errors.ts";

export const RemoteChildStatus = Schema.Literals([
  "running",
  "completed",
  "failed",
  "interrupted",
  "killed",
  "unknown",
]);
export type RemoteChildStatus = typeof RemoteChildStatus.Type;

export const RemoteChild = Schema.Struct({
  parentThreadId: ThreadId,
  childEnvironmentId: EnvironmentId,
  childThreadId: ThreadId,
  alias: TrimmedNonEmptyString,
  spawnParams: Schema.Unknown,
  status: RemoteChildStatus,
  lastPolledAt: Schema.NullOr(IsoDateTime),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type RemoteChild = typeof RemoteChild.Type;

export const RemoteChildKey = Schema.Struct({
  parentThreadId: ThreadId,
  childEnvironmentId: EnvironmentId,
  childThreadId: ThreadId,
});
export type RemoteChildKey = typeof RemoteChildKey.Type;

export const RemoteChildThreadKey = Schema.Struct({
  childEnvironmentId: EnvironmentId,
  childThreadId: ThreadId,
});
export type RemoteChildThreadKey = typeof RemoteChildThreadKey.Type;

export const ListRemoteChildrenByParentInput = Schema.Struct({
  parentThreadId: ThreadId,
});
export type ListRemoteChildrenByParentInput = typeof ListRemoteChildrenByParentInput.Type;

export const UpdateRemoteChildStatusInput = Schema.Struct({
  ...RemoteChildKey.fields,
  status: RemoteChildStatus,
  lastPolledAt: Schema.optional(Schema.NullOr(IsoDateTime)),
  updatedAt: IsoDateTime,
});
export type UpdateRemoteChildStatusInput = typeof UpdateRemoteChildStatusInput.Type;

export interface RemoteChildRepositoryShape {
  readonly upsert: (row: RemoteChild) => Effect.Effect<void, ProjectionRepositoryError>;
  readonly getByChild: (
    input: RemoteChildThreadKey,
  ) => Effect.Effect<Option.Option<RemoteChild>, ProjectionRepositoryError>;
  readonly listByParent: (
    input: ListRemoteChildrenByParentInput,
  ) => Effect.Effect<ReadonlyArray<RemoteChild>, ProjectionRepositoryError>;
  readonly listAll: () => Effect.Effect<ReadonlyArray<RemoteChild>, ProjectionRepositoryError>;
  readonly updateStatus: (
    input: UpdateRemoteChildStatusInput,
  ) => Effect.Effect<void, ProjectionRepositoryError>;
}

export class RemoteChildRepository extends Context.Service<
  RemoteChildRepository,
  RemoteChildRepositoryShape
>()("t3/persistence/Services/RemoteChildren/RemoteChildRepository") {}
