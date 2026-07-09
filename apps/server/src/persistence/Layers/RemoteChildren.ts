import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import { toPersistenceSqlError } from "../Errors.ts";
import {
  ListRemoteChildrenByParentInput,
  RemoteChild,
  RemoteChildRepository,
  RemoteChildThreadKey,
  UpdateRemoteChildStatusInput,
  type RemoteChildRepositoryShape,
} from "../Services/RemoteChildren.ts";

const RemoteChildDbRow = RemoteChild.mapFields(
  Struct.assign({
    spawnParams: Schema.fromJsonString(Schema.Unknown),
  }),
);
type RemoteChildDbRow = typeof RemoteChildDbRow.Type;

const toRemoteChild = (row: RemoteChildDbRow): RemoteChild =>
  ({
    ...row,
  }) satisfies RemoteChild;

const makeRemoteChildRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const writeRemoteChildRow = SqlSchema.void({
    Request: RemoteChild,
    execute: (row) =>
      sql`
        INSERT INTO remote_children (
          parent_thread_id,
          child_env_id,
          child_thread_id,
          alias,
          spawn_params_json,
          status,
          last_polled_at,
          created_at,
          updated_at
        )
        VALUES (
          ${row.parentThreadId},
          ${row.childEnvironmentId},
          ${row.childThreadId},
          ${row.alias},
          ${JSON.stringify(row.spawnParams)},
          ${row.status},
          ${row.lastPolledAt},
          ${row.createdAt},
          ${row.updatedAt}
        )
        ON CONFLICT (parent_thread_id, child_env_id, child_thread_id)
        DO UPDATE SET
          alias = excluded.alias,
          spawn_params_json = excluded.spawn_params_json,
          status = excluded.status,
          last_polled_at = excluded.last_polled_at,
          updated_at = excluded.updated_at
      `,
  });

  const getRemoteChildByChildRow = SqlSchema.findOneOption({
    Request: RemoteChildThreadKey,
    Result: RemoteChildDbRow,
    execute: ({ childEnvironmentId, childThreadId }) =>
      sql`
        SELECT
          parent_thread_id AS "parentThreadId",
          child_env_id AS "childEnvironmentId",
          child_thread_id AS "childThreadId",
          alias,
          spawn_params_json AS "spawnParams",
          status,
          last_polled_at AS "lastPolledAt",
          created_at AS "createdAt",
          updated_at AS "updatedAt"
        FROM remote_children
        WHERE child_env_id = ${childEnvironmentId}
          AND child_thread_id = ${childThreadId}
        ORDER BY created_at ASC
        LIMIT 1
      `,
  });

  const listRemoteChildRowsByParent = SqlSchema.findAll({
    Request: ListRemoteChildrenByParentInput,
    Result: RemoteChildDbRow,
    execute: ({ parentThreadId }) =>
      sql`
        SELECT
          parent_thread_id AS "parentThreadId",
          child_env_id AS "childEnvironmentId",
          child_thread_id AS "childThreadId",
          alias,
          spawn_params_json AS "spawnParams",
          status,
          last_polled_at AS "lastPolledAt",
          created_at AS "createdAt",
          updated_at AS "updatedAt"
        FROM remote_children
        WHERE parent_thread_id = ${parentThreadId}
        ORDER BY created_at ASC, child_env_id ASC, child_thread_id ASC
      `,
  });

  const listAllRemoteChildRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: RemoteChildDbRow,
    execute: () =>
      sql`
        SELECT
          parent_thread_id AS "parentThreadId",
          child_env_id AS "childEnvironmentId",
          child_thread_id AS "childThreadId",
          alias,
          spawn_params_json AS "spawnParams",
          status,
          last_polled_at AS "lastPolledAt",
          created_at AS "createdAt",
          updated_at AS "updatedAt"
        FROM remote_children
        ORDER BY created_at ASC, child_env_id ASC, child_thread_id ASC
      `,
  });

  const updateRemoteChildStatusRow = SqlSchema.void({
    Request: UpdateRemoteChildStatusInput,
    execute: ({
      parentThreadId,
      childEnvironmentId,
      childThreadId,
      status,
      lastPolledAt,
      updatedAt,
    }) =>
      sql`
        UPDATE remote_children
        SET
          status = ${status},
          last_polled_at = ${lastPolledAt ?? null},
          updated_at = ${updatedAt}
        WHERE parent_thread_id = ${parentThreadId}
          AND child_env_id = ${childEnvironmentId}
          AND child_thread_id = ${childThreadId}
      `,
  });

  const upsert: RemoteChildRepositoryShape["upsert"] = (row) =>
    writeRemoteChildRow(row).pipe(
      Effect.mapError(toPersistenceSqlError("RemoteChildRepository.upsert:query")),
    );

  const getByChild: RemoteChildRepositoryShape["getByChild"] = (input) =>
    getRemoteChildByChildRow(input).pipe(
      Effect.map(Option.map(toRemoteChild)),
      Effect.mapError(toPersistenceSqlError("RemoteChildRepository.getByChild:query")),
    );

  const listByParent: RemoteChildRepositoryShape["listByParent"] = (input) =>
    listRemoteChildRowsByParent(input).pipe(
      Effect.map((rows) => rows.map(toRemoteChild)),
      Effect.mapError(toPersistenceSqlError("RemoteChildRepository.listByParent:query")),
    );

  const listAll: RemoteChildRepositoryShape["listAll"] = () =>
    listAllRemoteChildRows().pipe(
      Effect.map((rows) => rows.map(toRemoteChild)),
      Effect.mapError(toPersistenceSqlError("RemoteChildRepository.listAll:query")),
    );

  const updateStatus: RemoteChildRepositoryShape["updateStatus"] = (input) =>
    updateRemoteChildStatusRow(input).pipe(
      Effect.mapError(toPersistenceSqlError("RemoteChildRepository.updateStatus:query")),
    );

  return {
    upsert,
    getByChild,
    listByParent,
    listAll,
    updateStatus,
  } satisfies RemoteChildRepositoryShape;
});

export const RemoteChildRepositoryLive = Layer.effect(
  RemoteChildRepository,
  makeRemoteChildRepository,
);
