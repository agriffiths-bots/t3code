import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { toPersistenceSqlError } from "../Errors.ts";
import {
  ListPendingDispatchesByTargetInput,
  PendingDispatch,
  PendingDispatchRepository,
  type PendingDispatchRepositoryShape,
} from "../Services/PendingDispatches.ts";

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
          created_at = excluded.created_at
      `,
  });

  const listPendingDispatchRowsByTarget = SqlSchema.findAll({
    Request: ListPendingDispatchesByTargetInput,
    Result: PendingDispatch,
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
          created_at AS "createdAt"
        FROM pending_dispatches
        WHERE kind = ${kind}
          AND target_thread_id = ${targetThreadId}
        ORDER BY created_at ASC, id ASC
      `,
  });

  const listAllPendingDispatchRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: PendingDispatch,
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
          created_at AS "createdAt"
        FROM pending_dispatches
        ORDER BY created_at ASC, id ASC
      `,
  });

  const insert: PendingDispatchRepositoryShape["insert"] = (row) =>
    writePendingDispatchRow(row).pipe(
      Effect.mapError(toPersistenceSqlError("PendingDispatchRepository.insert:query")),
    );

  const listByTarget: PendingDispatchRepositoryShape["listByTarget"] = (input) =>
    listPendingDispatchRowsByTarget(input).pipe(
      Effect.mapError(toPersistenceSqlError("PendingDispatchRepository.listByTarget:query")),
    );

  const listAll: PendingDispatchRepositoryShape["listAll"] = () =>
    listAllPendingDispatchRows().pipe(
      Effect.mapError(toPersistenceSqlError("PendingDispatchRepository.listAll:query")),
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
    deleteByIds,
  } satisfies PendingDispatchRepositoryShape;
});

export const PendingDispatchRepositoryLive = Layer.effect(
  PendingDispatchRepository,
  makePendingDispatchRepository,
);
