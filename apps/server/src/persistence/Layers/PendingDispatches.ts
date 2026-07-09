import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";

import { toPersistenceSqlError } from "../Errors.ts";
import {
  ClaimPendingDispatchesInput,
  MarkPendingDispatchesWaitDeliveredInput,
  ListPendingDispatchesByTargetInput,
  PendingDispatch,
  PendingDispatchRepository,
  ResetPendingDispatchClaimsInput,
  type PendingDispatchRepositoryShape,
} from "../Services/PendingDispatches.ts";

const PendingDispatchDbRow = PendingDispatch.mapFields(
  Struct.assign({
    deliveredByWait: Schema.Number,
    waitCancellable: Schema.Number,
  }),
);

const toPendingDispatch = (row: Schema.Schema.Type<typeof PendingDispatchDbRow>): PendingDispatch =>
  ({
    ...row,
    deliveredByWait: row.deliveredByWait === 1,
    waitCancellable: row.waitCancellable === 1,
  }) satisfies PendingDispatch;

const makePendingDispatchRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const writePendingDispatchRow = SqlSchema.void({
    Request: PendingDispatch,
    execute: (row) =>
      sql`
        INSERT INTO pending_dispatches (
          id,
          kind,
          target_thread_id,
          source_child_id,
          text,
          error,
          status,
          command_id,
          delivered_by_wait,
          wait_cancellable,
          created_at
        )
        VALUES (
          ${row.id},
          ${row.kind},
          ${row.targetThreadId},
          ${row.sourceChildId},
          ${row.text},
          ${row.error},
          ${row.status},
          ${row.commandId},
          ${row.deliveredByWait ? 1 : 0},
          ${row.waitCancellable ? 1 : 0},
          ${row.createdAt}
        )
        ON CONFLICT (id)
        DO UPDATE SET
          kind = excluded.kind,
          target_thread_id = excluded.target_thread_id,
          source_child_id = excluded.source_child_id,
          text = excluded.text,
          error = excluded.error,
          status = excluded.status,
          command_id = excluded.command_id,
          delivered_by_wait = excluded.delivered_by_wait,
          wait_cancellable = excluded.wait_cancellable,
          created_at = excluded.created_at
      `,
  });

  const listPendingDispatchRowsByTarget = SqlSchema.findAll({
    Request: ListPendingDispatchesByTargetInput,
    Result: PendingDispatchDbRow,
    execute: ({ kind, targetThreadId }) =>
      sql`
        SELECT
          id,
          kind,
          target_thread_id AS "targetThreadId",
          source_child_id AS "sourceChildId",
          text,
          error,
          status,
          command_id AS "commandId",
          delivered_by_wait AS "deliveredByWait",
          wait_cancellable AS "waitCancellable",
          created_at AS "createdAt"
        FROM pending_dispatches
        WHERE kind = ${kind}
          AND target_thread_id = ${targetThreadId}
        ORDER BY rowid ASC
      `,
  });

  const listAllPendingDispatchRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: PendingDispatchDbRow,
    execute: () =>
      sql`
        SELECT
          id,
          kind,
          target_thread_id AS "targetThreadId",
          source_child_id AS "sourceChildId",
          text,
          error,
          status,
          command_id AS "commandId",
          delivered_by_wait AS "deliveredByWait",
          wait_cancellable AS "waitCancellable",
          created_at AS "createdAt"
        FROM pending_dispatches
        ORDER BY rowid ASC
      `,
  });

  const claimPendingDispatchRows = SqlSchema.void({
    Request: ClaimPendingDispatchesInput,
    execute: ({ ids, commandId }) =>
      sql`
        UPDATE pending_dispatches
        SET command_id = ${commandId}
        WHERE ${sql.in("id", ids)}
      `,
  });

  const markPendingDispatchRowsWaitDelivered = SqlSchema.void({
    Request: MarkPendingDispatchesWaitDeliveredInput,
    execute: ({ ids }) =>
      sql`
        UPDATE pending_dispatches
        SET delivered_by_wait = 1
        WHERE ${sql.in("id", ids)}
      `,
  });

  const resetPendingDispatchClaims = SqlSchema.void({
    Request: ResetPendingDispatchClaimsInput,
    execute: ({ ids }) =>
      sql`
        UPDATE pending_dispatches
        SET command_id = NULL
        WHERE ${sql.in("id", ids)}
      `,
  });

  const insert: PendingDispatchRepositoryShape["insert"] = (row) =>
    writePendingDispatchRow(row).pipe(
      Effect.mapError(toPersistenceSqlError("PendingDispatchRepository.insert:query")),
    );

  const listByTarget: PendingDispatchRepositoryShape["listByTarget"] = (input) =>
    listPendingDispatchRowsByTarget(input).pipe(
      Effect.map((rows) => rows.map(toPendingDispatch)),
      Effect.mapError(toPersistenceSqlError("PendingDispatchRepository.listByTarget:query")),
    );

  const listAll: PendingDispatchRepositoryShape["listAll"] = () =>
    listAllPendingDispatchRows().pipe(
      Effect.map((rows) => rows.map(toPendingDispatch)),
      Effect.mapError(toPersistenceSqlError("PendingDispatchRepository.listAll:query")),
    );

  const claim: PendingDispatchRepositoryShape["claim"] = (input) =>
    input.ids.length === 0
      ? Effect.void
      : claimPendingDispatchRows(input).pipe(
          Effect.mapError(toPersistenceSqlError("PendingDispatchRepository.claim:query")),
        );

  const markWaitDelivered: PendingDispatchRepositoryShape["markWaitDelivered"] = (input) =>
    input.ids.length === 0
      ? Effect.void
      : markPendingDispatchRowsWaitDelivered(input).pipe(
          Effect.mapError(
            toPersistenceSqlError("PendingDispatchRepository.markWaitDelivered:query"),
          ),
        );

  const resetClaims: PendingDispatchRepositoryShape["resetClaims"] = (input) =>
    input.ids.length === 0
      ? Effect.void
      : resetPendingDispatchClaims(input).pipe(
          Effect.mapError(toPersistenceSqlError("PendingDispatchRepository.resetClaims:query")),
        );

  const deleteByIds: PendingDispatchRepositoryShape["deleteByIds"] = (ids) =>
    ids.length === 0
      ? Effect.void
      : sql`
          DELETE FROM pending_dispatches
          WHERE ${sql.in("id", ids)}
        `.pipe(
          Effect.asVoid,
          Effect.mapError(toPersistenceSqlError("PendingDispatchRepository.deleteByIds:query")),
        );

  return {
    insert,
    listByTarget,
    listAll,
    claim,
    markWaitDelivered,
    resetClaims,
    deleteByIds,
  } satisfies PendingDispatchRepositoryShape;
});

export const PendingDispatchRepositoryLive = Layer.effect(
  PendingDispatchRepository,
  makePendingDispatchRepository,
);
