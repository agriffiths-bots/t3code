import {
  AuthOrchestrationReadScope,
  AuthSessionId,
  CheckpointRef,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentId,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  TurnId,
  ProviderInstanceId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { MAX_THREAD_CHECKPOINTS } from "../checkpointRetention.ts";
import { ORCHESTRATION_PROJECTOR_NAMES } from "./ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "../ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../ThreadPlanProgress.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { encodeThreadDetailPageCursor } from "../threadDetailCursor.ts";

const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asEnvironmentId = (value: string): EnvironmentId => EnvironmentId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);
const asMessageId = (value: string): MessageId => MessageId.make(value);
const asEventId = (value: string): EventId => EventId.make(value);
const asCheckpointRef = (value: string): CheckpointRef => CheckpointRef.make(value);
const encodeUnknownJsonString = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

const projectionSnapshotLayer = it.layer(
  OrchestrationProjectionSnapshotQueryLive.pipe(
    Layer.provide(ThreadBackgroundLiveness.layer),
    Layer.provide(ThreadPlanProgress.layer),
    Layer.provideMerge(RepositoryIdentityResolver.layer),
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  ),
);

projectionSnapshotLayer("ProjectionSnapshotQuery", (it) => {
  it.effect("fails closed when resolving event aggregate visibility", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;
      const privateProjectId = asProjectId("project-event-guard-private");
      const factoryProjectId = asProjectId("project-event-guard-factory");
      const privateThreadId = ThreadId.make("thread-event-guard-private-deleted");
      const factoryThreadId = ThreadId.make("thread-event-guard-factory-deleted");

      yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, data_audience,
          default_model_selection_json, scripts_json, created_at, updated_at, deleted_at
        ) VALUES
          (${privateProjectId}, 'Private event guard', '/tmp/event-guard-private', 'private',
           NULL, '[]', '2026-07-18T00:00:00.000Z', '2026-07-18T00:00:00.000Z',
           '2026-07-18T00:01:00.000Z'),
          (${factoryProjectId}, 'Factory event guard', '/tmp/event-guard-factory', 'factory',
           NULL, '[]', '2026-07-18T00:00:00.000Z', '2026-07-18T00:00:00.000Z',
           '2026-07-18T00:01:00.000Z')
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id, project_id, title, model_selection_json, runtime_mode,
          interaction_mode, branch, worktree_path, latest_turn_id,
          created_at, updated_at, deleted_at
        ) VALUES
          (${privateThreadId}, ${privateProjectId}, 'Private deleted event guard',
           '{"instanceId":"codex","model":"gpt-5.5"}',
           'full-access', 'default', NULL, NULL, NULL,
           '2026-07-18T00:00:00.000Z', '2026-07-18T00:00:00.000Z',
           '2026-07-18T00:01:00.000Z'),
          (${factoryThreadId}, ${factoryProjectId}, 'Factory deleted event guard',
           '{"instanceId":"codex","model":"gpt-5.5"}',
           'full-access', 'default', NULL, NULL, NULL,
           '2026-07-18T00:00:00.000Z', '2026-07-18T00:00:00.000Z',
           '2026-07-18T00:01:00.000Z')
      `;

      const asAudience = <A, E, R>(
        audienceCeiling: "private" | "factory",
        effect: Effect.Effect<A, E, R>,
      ) =>
        effect.pipe(
          Effect.provideService(EnvironmentAuthenticatedPrincipal, {
            sessionId: AuthSessionId.make(`event-guard-${audienceCeiling}`),
            subject: `event-guard-${audienceCeiling}`,
            method: "bearer-access-token",
            scopes: new Set([AuthOrchestrationReadScope]),
            audienceCeiling,
          }),
        );
      const refs = [
        { name: "private project", aggregateKind: "project", aggregateId: privateProjectId },
        { name: "factory project", aggregateKind: "project", aggregateId: factoryProjectId },
        { name: "private deleted thread", aggregateKind: "thread", aggregateId: privateThreadId },
        { name: "factory deleted thread", aggregateKind: "thread", aggregateId: factoryThreadId },
        {
          name: "missing guessed thread",
          aggregateKind: "thread",
          aggregateId: ThreadId.make("thread-event-guard-missing"),
        },
      ] as const;
      const canReadEventAggregate = snapshotQuery.canReadEventAggregate;
      assert.isDefined(canReadEventAggregate);
      if (canReadEventAggregate === undefined) {
        return yield* Effect.die("Expected live event aggregate visibility query");
      }

      for (const ref of refs) {
        const factoryVisible = yield* asAudience("factory", canReadEventAggregate(ref));
        const privateVisible = yield* asAudience("private", canReadEventAggregate(ref));
        assert.equal(factoryVisible, ref.name.startsWith("factory"), ref.name);
        assert.equal(privateVisible, !ref.name.startsWith("missing"), ref.name);
      }
    }).pipe(
      Effect.ensuring(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`DELETE FROM projection_threads WHERE thread_id IN ('thread-event-guard-private-deleted', 'thread-event-guard-factory-deleted')`;
          yield* sql`DELETE FROM projection_projects WHERE project_id IN ('project-event-guard-private', 'project-event-guard-factory')`;
        }).pipe(Effect.orDie),
      ),
    ),
  );

  it.effect(
    "centrally scopes every project and thread read surface by session audience ceiling",
    () =>
      Effect.gen(function* () {
        const snapshotQuery = yield* ProjectionSnapshotQuery;
        const sql = yield* SqlClient.SqlClient;
        const privateProjectId = asProjectId("project-read-guard-private");
        const factoryProjectId = asProjectId("project-read-guard-factory");
        const privateActiveThreadId = ThreadId.make("thread-read-guard-private-active");
        const factoryActiveThreadId = ThreadId.make("thread-read-guard-factory-active");
        const privateArchivedThreadId = ThreadId.make("thread-read-guard-private-archived");
        const factoryArchivedThreadId = ThreadId.make("thread-read-guard-factory-archived");

        yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          data_audience,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES
          (
            ${privateProjectId},
            'Private read guard project',
            '/tmp/read-guard-private',
            'private',
            NULL,
            '[]',
            '2026-07-18T00:00:00.000Z',
            '2026-07-18T00:00:00.000Z',
            NULL
          ),
          (
            ${factoryProjectId},
            'Factory read guard project',
            '/tmp/read-guard-factory',
            'factory',
            NULL,
            '[]',
            '2026-07-18T00:00:01.000Z',
            '2026-07-18T00:00:01.000Z',
            NULL
          )
      `;
        yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          created_at,
          updated_at,
          archived_at,
          deleted_at
        )
        VALUES
          (
            ${privateActiveThreadId},
            ${privateProjectId},
            'Private active read guard thread',
            '{"instanceId":"codex","model":"gpt-5.5"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            '2026-07-18T00:00:02.000Z',
            '2026-07-18T00:00:02.000Z',
            NULL,
            NULL
          ),
          (
            ${factoryActiveThreadId},
            ${factoryProjectId},
            'Factory active read guard thread',
            '{"instanceId":"codex","model":"gpt-5.5"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            '2026-07-18T00:00:03.000Z',
            '2026-07-18T00:00:03.000Z',
            NULL,
            NULL
          ),
          (
            ${privateArchivedThreadId},
            ${privateProjectId},
            'Private archived read guard thread',
            '{"instanceId":"codex","model":"gpt-5.5"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            '2026-07-18T00:00:04.000Z',
            '2026-07-18T00:00:04.000Z',
            '2026-07-18T00:00:05.000Z',
            NULL
          ),
          (
            ${factoryArchivedThreadId},
            ${factoryProjectId},
            'Factory archived read guard thread',
            '{"instanceId":"codex","model":"gpt-5.5"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            '2026-07-18T00:00:06.000Z',
            '2026-07-18T00:00:06.000Z',
            '2026-07-18T00:00:07.000Z',
            NULL
          )
      `;

        const asAudience = <A, E, R>(
          audienceCeiling: "private" | "factory",
          effect: Effect.Effect<A, E, R>,
        ) =>
          effect.pipe(
            Effect.provideService(EnvironmentAuthenticatedPrincipal, {
              sessionId: AuthSessionId.make(`read-guard-${audienceCeiling}`),
              subject: `read-guard-${audienceCeiling}`,
              method: "bearer-access-token",
              scopes: new Set([AuthOrchestrationReadScope]),
              audienceCeiling,
            }),
          );
        const ids = <A extends { readonly id: string }>(rows: ReadonlyArray<A>) =>
          rows.map((row) => row.id);
        const listSurfaces = [
          {
            name: "full snapshot",
            read: () =>
              snapshotQuery.getSnapshot().pipe(
                Effect.map((snapshot) => ({
                  projectIds: ids(snapshot.projects),
                  threadIds: ids(snapshot.threads),
                })),
              ),
            expectedFactoryThreadId: factoryActiveThreadId,
          },
          {
            name: "active shell snapshot",
            read: () =>
              snapshotQuery.getShellSnapshot().pipe(
                Effect.map((snapshot) => ({
                  projectIds: ids(snapshot.projects),
                  threadIds: ids(snapshot.threads),
                })),
              ),
            expectedFactoryThreadId: factoryActiveThreadId,
          },
          {
            name: "archived shell snapshot",
            read: () =>
              snapshotQuery.getArchivedShellSnapshot().pipe(
                Effect.map((snapshot) => ({
                  projectIds: ids(snapshot.projects),
                  threadIds: ids(snapshot.threads),
                })),
              ),
            expectedFactoryThreadId: factoryArchivedThreadId,
          },
        ] as const;

        for (const surface of listSurfaces) {
          const factoryRead = yield* asAudience("factory", surface.read());
          assert.include(factoryRead.projectIds, factoryProjectId, surface.name);
          assert.notInclude(factoryRead.projectIds, privateProjectId, surface.name);
          assert.include(factoryRead.threadIds, surface.expectedFactoryThreadId, surface.name);
          assert.notInclude(factoryRead.threadIds, privateActiveThreadId, surface.name);
          assert.notInclude(factoryRead.threadIds, privateArchivedThreadId, surface.name);

          const privateRead = yield* asAudience("private", surface.read());
          assert.include(privateRead.projectIds, factoryProjectId, surface.name);
          assert.include(privateRead.projectIds, privateProjectId, surface.name);
        }

        assert.deepEqual(yield* asAudience("factory", snapshotQuery.getCounts()), {
          projectCount: 1,
          threadCount: 2,
        });
        assert.deepEqual(yield* asAudience("private", snapshotQuery.getCounts()), {
          projectCount: 2,
          threadCount: 4,
        });

        const lookupSurfaces = [
          {
            name: "project by id",
            privateRead: snapshotQuery
              .getProjectShellById(privateProjectId)
              .pipe(Effect.map(Option.isSome)),
            factoryRead: snapshotQuery
              .getProjectShellById(factoryProjectId)
              .pipe(Effect.map(Option.isSome)),
            missingRead: snapshotQuery
              .getProjectShellById(asProjectId("project-read-guard-missing"))
              .pipe(Effect.map(Option.isSome)),
          },
          {
            name: "project by workspace root",
            privateRead: snapshotQuery
              .getActiveProjectByWorkspaceRoot("/tmp/read-guard-private")
              .pipe(Effect.map(Option.isSome)),
            factoryRead: snapshotQuery
              .getActiveProjectByWorkspaceRoot("/tmp/read-guard-factory")
              .pipe(Effect.map(Option.isSome)),
            missingRead: snapshotQuery
              .getActiveProjectByWorkspaceRoot("/tmp/read-guard-missing")
              .pipe(Effect.map(Option.isSome)),
          },
          {
            name: "first active thread by project",
            privateRead: snapshotQuery
              .getFirstActiveThreadIdByProjectId(privateProjectId)
              .pipe(Effect.map(Option.isSome)),
            factoryRead: snapshotQuery
              .getFirstActiveThreadIdByProjectId(factoryProjectId)
              .pipe(Effect.map(Option.isSome)),
            missingRead: snapshotQuery
              .getFirstActiveThreadIdByProjectId(asProjectId("project-read-guard-missing"))
              .pipe(Effect.map(Option.isSome)),
          },
          {
            name: "active thread shell by id",
            privateRead: snapshotQuery
              .getThreadShellById(privateActiveThreadId)
              .pipe(Effect.map(Option.isSome)),
            factoryRead: snapshotQuery
              .getThreadShellById(factoryActiveThreadId)
              .pipe(Effect.map(Option.isSome)),
            missingRead: snapshotQuery
              .getThreadShellById(ThreadId.make("thread-read-guard-missing"))
              .pipe(Effect.map(Option.isSome)),
          },
          {
            name: "archived thread shell by id",
            privateRead: snapshotQuery
              .getThreadShellByIdIncludingArchived(privateArchivedThreadId)
              .pipe(Effect.map(Option.isSome)),
            factoryRead: snapshotQuery
              .getThreadShellByIdIncludingArchived(factoryArchivedThreadId)
              .pipe(Effect.map(Option.isSome)),
            missingRead: snapshotQuery
              .getThreadShellByIdIncludingArchived(ThreadId.make("thread-read-guard-missing"))
              .pipe(Effect.map(Option.isSome)),
          },
          {
            name: "archived thread shell snapshot by id",
            privateRead: snapshotQuery
              .getThreadShellSnapshotByIdIncludingArchived(privateArchivedThreadId)
              .pipe(Effect.map(({ thread }) => Option.isSome(thread))),
            factoryRead: snapshotQuery
              .getThreadShellSnapshotByIdIncludingArchived(factoryArchivedThreadId)
              .pipe(Effect.map(({ thread }) => Option.isSome(thread))),
            missingRead: snapshotQuery
              .getThreadShellSnapshotByIdIncludingArchived(
                ThreadId.make("thread-read-guard-missing"),
              )
              .pipe(Effect.map(({ thread }) => Option.isSome(thread))),
          },
          {
            name: "thread detail by id",
            privateRead: snapshotQuery
              .getThreadDetailById(privateActiveThreadId)
              .pipe(Effect.map(Option.isSome)),
            factoryRead: snapshotQuery
              .getThreadDetailById(factoryActiveThreadId)
              .pipe(Effect.map(Option.isSome)),
            missingRead: snapshotQuery
              .getThreadDetailById(ThreadId.make("thread-read-guard-missing"))
              .pipe(Effect.map(Option.isSome)),
          },
          {
            name: "thread detail snapshot by id",
            privateRead: snapshotQuery
              .getThreadDetailSnapshot(privateActiveThreadId)
              .pipe(Effect.map(Option.isSome)),
            factoryRead: snapshotQuery
              .getThreadDetailSnapshot(factoryActiveThreadId)
              .pipe(Effect.map(Option.isSome)),
            missingRead: snapshotQuery
              .getThreadDetailSnapshot(ThreadId.make("thread-read-guard-missing"))
              .pipe(Effect.map(Option.isSome)),
          },
          {
            name: "thread checkpoint revision context",
            privateRead: snapshotQuery
              .getThreadCheckpointContext(privateActiveThreadId)
              .pipe(Effect.map(Option.isSome)),
            factoryRead: snapshotQuery
              .getThreadCheckpointContext(factoryActiveThreadId)
              .pipe(Effect.map(Option.isSome)),
            missingRead: snapshotQuery
              .getThreadCheckpointContext(ThreadId.make("thread-read-guard-missing"))
              .pipe(Effect.map(Option.isSome)),
          },
          {
            name: "full thread diff revision context",
            privateRead: snapshotQuery
              .getFullThreadDiffContext(privateActiveThreadId, 0)
              .pipe(Effect.map(Option.isSome)),
            factoryRead: snapshotQuery
              .getFullThreadDiffContext(factoryActiveThreadId, 0)
              .pipe(Effect.map(Option.isSome)),
            missingRead: snapshotQuery
              .getFullThreadDiffContext(ThreadId.make("thread-read-guard-missing"), 0)
              .pipe(Effect.map(Option.isSome)),
          },
        ] as const;

        for (const surface of lookupSurfaces) {
          assert.isFalse(yield* asAudience("factory", surface.privateRead), surface.name);
          assert.isFalse(yield* asAudience("factory", surface.missingRead), surface.name);
          assert.isTrue(yield* asAudience("factory", surface.factoryRead), surface.name);
          assert.isTrue(yield* asAudience("private", surface.privateRead), surface.name);
          assert.isTrue(yield* asAudience("private", surface.factoryRead), surface.name);
        }
      }),
  );

  it.effect("redacts private cross-thread references from factory-scoped snapshots", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;
      const privateProjectId = asProjectId("project-reference-redaction-private");
      const factoryProjectId = asProjectId("project-reference-redaction-factory");
      const privateParentThreadId = ThreadId.make("thread-reference-redaction-private-parent");
      const factoryChildThreadId = ThreadId.make("thread-reference-redaction-factory-child");
      const factoryArchivedChildThreadId = ThreadId.make(
        "thread-reference-redaction-factory-archived-child",
      );
      const factoryTurnId = asTurnId("turn-reference-redaction-factory");

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          data_audience,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES
          (
            ${privateProjectId},
            'Private reference redaction project',
            '/tmp/reference-redaction-private',
            'private',
            NULL,
            '[]',
            '2026-07-18T01:00:00.000Z',
            '2026-07-18T01:00:00.000Z',
            NULL
          ),
          (
            ${factoryProjectId},
            'Factory reference redaction project',
            '/tmp/reference-redaction-factory',
            'factory',
            NULL,
            '[]',
            '2026-07-18T01:00:01.000Z',
            '2026-07-18T01:00:01.000Z',
            NULL
          )
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          created_at,
          updated_at,
          archived_at,
          deleted_at,
          parent_thread_id
        )
        VALUES
          (
            ${privateParentThreadId},
            ${privateProjectId},
            'Private reference parent',
            '{"instanceId":"codex","model":"gpt-5.5"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            '2026-07-18T01:00:02.000Z',
            '2026-07-18T01:00:02.000Z',
            NULL,
            NULL,
            NULL
          ),
          (
            ${factoryChildThreadId},
            ${factoryProjectId},
            'Factory reference child',
            '{"instanceId":"codex","model":"gpt-5.5"}',
            'full-access',
            'default',
            NULL,
            NULL,
            ${factoryTurnId},
            '2026-07-18T01:00:03.000Z',
            '2026-07-18T01:00:03.000Z',
            NULL,
            NULL,
            ${privateParentThreadId}
          ),
          (
            ${factoryArchivedChildThreadId},
            ${factoryProjectId},
            'Factory archived reference child',
            '{"instanceId":"codex","model":"gpt-5.5"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            '2026-07-18T01:00:04.000Z',
            '2026-07-18T01:00:04.000Z',
            '2026-07-18T01:00:05.000Z',
            NULL,
            ${privateParentThreadId}
          )
      `;
      yield* sql`
        INSERT INTO projection_turns (
          thread_id,
          turn_id,
          pending_message_id,
          source_proposed_plan_thread_id,
          source_proposed_plan_id,
          assistant_message_id,
          state,
          requested_at,
          started_at,
          completed_at,
          checkpoint_turn_count,
          checkpoint_ref,
          checkpoint_status,
          checkpoint_files_json
        )
        VALUES (
          ${factoryChildThreadId},
          ${factoryTurnId},
          NULL,
          ${privateParentThreadId},
          'plan-reference-redaction-private',
          NULL,
          'completed',
          '2026-07-18T01:00:06.000Z',
          '2026-07-18T01:00:06.000Z',
          '2026-07-18T01:00:07.000Z',
          NULL,
          NULL,
          NULL,
          '[]'
        )
      `;

      yield* sql`
        INSERT INTO projection_thread_proposed_plans (
          plan_id,
          thread_id,
          turn_id,
          plan_markdown,
          implemented_at,
          implementation_thread_id,
          created_at,
          updated_at
        )
        VALUES (
          'plan-reference-redaction-factory',
          ${factoryChildThreadId},
          ${factoryTurnId},
          '# Implement privately',
          '2026-07-18T01:00:08.000Z',
          ${privateParentThreadId},
          '2026-07-18T01:00:08.000Z',
          '2026-07-18T01:00:08.000Z'
        )
      `;
      yield* sql`
        INSERT INTO projection_thread_activities (
          activity_id,
          thread_id,
          turn_id,
          tone,
          kind,
          summary,
          payload_json,
          created_at
        )
        VALUES (
          'activity-reference-redaction-child',
          ${factoryChildThreadId},
          ${factoryTurnId},
          'info',
          'subagent.completed',
          ${`Sub-agent ${privateParentThreadId} completed`},
          ${encodeUnknownJsonString({
            childThreadId: privateParentThreadId,
            status: "completed",
            error: null,
          })},
          '2026-07-18T01:00:09.000Z'
        )
      `;

      const asAudience = <A, E, R>(
        audienceCeiling: "private" | "factory",
        effect: Effect.Effect<A, E, R>,
      ) =>
        effect.pipe(
          Effect.provideService(EnvironmentAuthenticatedPrincipal, {
            sessionId: AuthSessionId.make(`reference-redaction-${audienceCeiling}`),
            subject: `reference-redaction-${audienceCeiling}`,
            method: "bearer-access-token",
            scopes: new Set([AuthOrchestrationReadScope]),
            audienceCeiling,
          }),
        );
      const threadById = <A extends { readonly id: ThreadId }>(
        rows: ReadonlyArray<A>,
        threadId: ThreadId,
      ): A => {
        const row = rows.find((candidate) => candidate.id === threadId);
        assert.isDefined(row);
        return row;
      };

      const factoryShell = yield* asAudience("factory", snapshotQuery.getShellSnapshot());
      const factoryShellChild = threadById(factoryShell.threads, factoryChildThreadId);
      assert.equal(factoryShellChild.parentThreadId, null);
      assert.equal(factoryShellChild.parentEnvironmentId, null);
      assert.isUndefined(factoryShellChild.latestTurn?.sourceProposedPlan);

      const factoryArchivedShell = yield* asAudience(
        "factory",
        snapshotQuery.getArchivedShellSnapshot(),
      );
      assert.equal(
        threadById(factoryArchivedShell.threads, factoryArchivedChildThreadId).parentThreadId,
        null,
      );

      const factorySnapshot = yield* asAudience("factory", snapshotQuery.getSnapshot());
      const factorySnapshotChild = threadById(factorySnapshot.threads, factoryChildThreadId);
      assert.equal(factorySnapshotChild.parentThreadId, null);
      assert.isUndefined(factorySnapshotChild.latestTurn?.sourceProposedPlan);

      const factoryShellLookup = Option.getOrThrow(
        yield* asAudience("factory", snapshotQuery.getThreadShellById(factoryChildThreadId)),
      );
      assert.equal(factoryShellLookup.parentThreadId, null);
      assert.isUndefined(factoryShellLookup.latestTurn?.sourceProposedPlan);

      const factoryDetail = Option.getOrThrow(
        yield* asAudience("factory", snapshotQuery.getThreadDetailById(factoryChildThreadId)),
      );
      assert.equal(factoryDetail.parentThreadId, null);
      assert.isUndefined(factoryDetail.latestTurn?.sourceProposedPlan);
      assert.isUndefined(factoryDetail.turns[0]?.sourceProposedPlan);
      assert.equal(factoryDetail.proposedPlans[0]?.implementationThreadId, null);
      assert.deepEqual(factoryDetail.activities[0]?.payload, {
        childThreadId: null,
        status: "completed",
        error: null,
      });
      assert.notInclude(factoryDetail.activities[0]?.summary, privateParentThreadId);

      const privateShell = yield* asAudience("private", snapshotQuery.getShellSnapshot());
      const privateShellChild = threadById(privateShell.threads, factoryChildThreadId);
      assert.equal(privateShellChild.parentThreadId, privateParentThreadId);
      assert.equal(
        privateShellChild.latestTurn?.sourceProposedPlan?.threadId,
        privateParentThreadId,
      );

      const privateDetail = Option.getOrThrow(
        yield* asAudience("private", snapshotQuery.getThreadDetailById(factoryChildThreadId)),
      );
      assert.equal(privateDetail.parentThreadId, privateParentThreadId);
      assert.equal(privateDetail.latestTurn?.sourceProposedPlan?.threadId, privateParentThreadId);
      assert.equal(privateDetail.turns[0]?.sourceProposedPlan?.threadId, privateParentThreadId);
      assert.equal(privateDetail.proposedPlans[0]?.implementationThreadId, privateParentThreadId);
      assert.deepEqual(privateDetail.activities[0]?.payload, {
        childThreadId: privateParentThreadId,
        status: "completed",
        error: null,
      });
      assert.include(privateDetail.activities[0]?.summary, privateParentThreadId);
    }),
  );

  it.effect("exposes thread audience only through its parent project", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_threads WHERE thread_id = 'thread-audience-inheritance'`;
      yield* sql`DELETE FROM projection_projects WHERE project_id = 'project-audience-inheritance'`;
      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          data_audience,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'project-audience-inheritance',
          'Audience inheritance',
          '/tmp/project-audience-inheritance',
          'factory',
          NULL,
          '[]',
          '2026-07-17T00:00:00.000Z',
          '2026-07-17T00:00:00.000Z',
          NULL
        )
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'thread-audience-inheritance',
          'project-audience-inheritance',
          'Inherited audience',
          '{"instanceId":"codex","model":"gpt-5.5"}',
          'full-access',
          'default',
          NULL,
          NULL,
          NULL,
          '2026-07-17T00:00:00.000Z',
          '2026-07-17T00:00:00.000Z',
          NULL
        )
      `;

      const threadColumns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_threads)
      `;
      assert.equal(
        threadColumns.some((column) => column.name === "data_audience"),
        false,
      );

      const shell = yield* snapshotQuery.getThreadShellById(
        ThreadId.make("thread-audience-inheritance"),
      );
      assert.equal(shell._tag, "Some");
      if (shell._tag === "Some") {
        assert.equal(shell.value.dataAudience, "factory");
      }

      const detail = yield* snapshotQuery.getThreadDetailById(
        ThreadId.make("thread-audience-inheritance"),
      );
      assert.equal(detail._tag, "Some");
      if (detail._tag === "Some") {
        assert.equal(detail.value.dataAudience, "factory");
      }
    }),
  );

  it.effect("hydrates read model from projection tables and computes snapshot sequence", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_projects`;
      yield* sql`DELETE FROM projection_state`;
      yield* sql`DELETE FROM projection_thread_proposed_plans`;
      yield* sql`DELETE FROM projection_turns`;

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'project-1',
          'Project 1',
          '/tmp/project-1',
          '{"provider":"codex","model":"gpt-5-codex"}',
          '[{"id":"script-1","name":"Build","command":"bun run build","icon":"build","runOnWorktreeCreate":false}]',
          '2026-02-24T00:00:00.000Z',
          '2026-02-24T00:00:01.000Z',
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          latest_user_message_at,
          pending_approval_count,
          pending_user_input_count,
          has_actionable_proposed_plan,
          pinned_at,
          pin_order_key,
          created_at,
          updated_at,
          deleted_at,
          parent_thread_id,
          parent_environment_id
        )
        VALUES (
          'thread-1',
          'project-1',
          'Thread 1',
          '{"provider":"codex","model":"gpt-5-codex"}',
          'full-access',
          'default',
          NULL,
          NULL,
          'turn-1',
          '2026-02-24T00:00:04.000Z',
          1,
          0,
          0,
          '2026-02-24T00:00:01.000Z',
          'gm',
          '2026-02-24T00:00:02.000Z',
          '2026-02-24T00:00:03.000Z',
          NULL,
          'thread-remote-parent',
          'environment-remote-parent'
        )
      `;

      yield* sql`
        INSERT INTO projection_thread_messages (
          message_id,
          thread_id,
          turn_id,
          role,
          text,
          is_streaming,
          created_at,
          updated_at
        )
        VALUES (
          'message-1',
          'thread-1',
          'turn-1',
          'assistant',
          'hello from projection',
          0,
          '2026-02-24T00:00:04.000Z',
          '2026-02-24T00:00:05.000Z'
        )
      `;

      yield* sql`
        INSERT INTO projection_thread_messages (
          message_id,
          thread_id,
          turn_id,
          role,
          text,
          is_streaming,
          created_at,
          updated_at
        )
        VALUES (
          'message-prompt',
          'thread-1',
          NULL,
          'system',
          '[sub-agent child-1 completed] done',
          0,
          '2026-02-24T00:00:03.500Z',
          '2026-02-24T00:00:03.500Z'
        )
      `;

      yield* sql`
        INSERT INTO projection_thread_proposed_plans (
          plan_id,
          thread_id,
          turn_id,
          plan_markdown,
          implemented_at,
          implementation_thread_id,
          created_at,
          updated_at
        )
        VALUES (
          'plan-1',
          'thread-1',
          'turn-1',
          '# Ship it',
          '2026-02-24T00:00:05.500Z',
          'thread-2',
          '2026-02-24T00:00:05.000Z',
          '2026-02-24T00:00:05.500Z'
        )
      `;

      yield* sql`
        INSERT INTO projection_thread_activities (
          activity_id,
          thread_id,
          turn_id,
          tone,
          kind,
          summary,
          payload_json,
          created_at
        )
        VALUES (
          'activity-1',
          'thread-1',
          'turn-1',
          'info',
          'runtime.note',
          'provider started',
          '{"stage":"start"}',
          '2026-02-24T00:00:06.000Z'
        )
      `;

      yield* sql`
        INSERT INTO projection_thread_sessions (
          thread_id,
          status,
          provider_name,
          provider_session_id,
          provider_thread_id,
          runtime_mode,
          active_turn_id,
          last_error,
          updated_at
        )
        VALUES (
          'thread-1',
          'running',
          'codex',
          'provider-session-1',
          'provider-thread-1',
          'approval-required',
          'turn-1',
          NULL,
          '2026-02-24T00:00:07.000Z'
        )
      `;

      yield* sql`
        INSERT INTO projection_turns (
          thread_id,
          turn_id,
          pending_message_id,
          source_proposed_plan_thread_id,
          source_proposed_plan_id,
          assistant_message_id,
          state,
          requested_at,
          started_at,
          completed_at,
          checkpoint_turn_count,
          checkpoint_ref,
          checkpoint_status,
          checkpoint_files_json
        )
        VALUES (
          'thread-1',
          'turn-1',
          'message-prompt',
          'thread-1',
          'plan-1',
          'message-1',
          'completed',
          '2026-02-24T00:00:08.000Z',
          '2026-02-24T00:00:08.000Z',
          '2026-02-24T00:00:08.000Z',
          1,
          'checkpoint-1',
          'ready',
          '[{"path":"README.md","kind":"modified","additions":2,"deletions":1}]'
        )
      `;

      let sequence = 5;
      for (const projector of Object.values(ORCHESTRATION_PROJECTOR_NAMES)) {
        yield* sql`
          INSERT INTO projection_state (
            projector,
            last_applied_sequence,
            updated_at
          )
          VALUES (
            ${projector},
            ${sequence},
            '2026-02-24T00:00:09.000Z'
          )
        `;
        sequence += 1;
      }

      const snapshot = yield* snapshotQuery.getSnapshot();

      assert.equal(snapshot.snapshotSequence, 5);
      assert.equal(snapshot.updatedAt, "2026-02-24T00:00:09.000Z");
      assert.deepEqual(snapshot.projects, [
        {
          id: asProjectId("project-1"),
          title: "Project 1",
          workspaceRoot: "/tmp/project-1",
          dataAudience: "private",
          repositoryIdentity: null,
          defaultModelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          faviconPath: null,
          scripts: [
            {
              id: "script-1",
              name: "Build",
              command: "bun run build",
              icon: "build",
              runOnWorktreeCreate: false,
            },
          ],
          defaultThreadEnvMode: null,
          createdAt: "2026-02-24T00:00:00.000Z",
          updatedAt: "2026-02-24T00:00:01.000Z",
          deletedAt: null,
        },
      ]);
      assert.deepEqual(snapshot.threads, [
        {
          id: ThreadId.make("thread-1"),
          projectId: asProjectId("project-1"),
          dataAudience: "private",
          title: "Thread 1",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: "default",
          runtimeMode: "full-access",
          branch: null,
          worktreePath: null,
          worktreeRemovable: false,
          worktreeRemovalPath: null,
          latestTurn: {
            turnId: asTurnId("turn-1"),
            state: "completed",
            requestedAt: "2026-02-24T00:00:08.000Z",
            startedAt: "2026-02-24T00:00:08.000Z",
            completedAt: "2026-02-24T00:00:08.000Z",
            assistantMessageId: asMessageId("message-1"),
            sourceProposedPlan: {
              threadId: ThreadId.make("thread-1"),
              planId: "plan-1",
            },
          },
          turns: [],
          createdAt: "2026-02-24T00:00:02.000Z",
          updatedAt: "2026-02-24T00:00:03.000Z",
          archivedAt: null,
          parentThreadId: ThreadId.make("thread-remote-parent"),
          parentEnvironmentId: asEnvironmentId("environment-remote-parent"),
          settledOverride: null,
          settledAt: null,
          snoozedUntil: null,
          snoozedAt: null,
          pinnedAt: "2026-02-24T00:00:01.000Z",
          pinOrderKey: "gm",
          titleRegeneration: null,
          deletedAt: null,
          messages: [],
          proposedPlans: [],
          activities: [],
          checkpoints: [],
          session: {
            threadId: ThreadId.make("thread-1"),
            status: "running",
            providerName: "codex",
            runtimeMode: "approval-required",
            activeTurnId: asTurnId("turn-1"),
            lastError: null,
            updatedAt: "2026-02-24T00:00:07.000Z",
          },
        },
      ]);

      const shellSnapshot = yield* snapshotQuery.getShellSnapshot();
      assert.equal(shellSnapshot.snapshotSequence, 5);
      assert.deepEqual(shellSnapshot.projects, [
        {
          id: asProjectId("project-1"),
          title: "Project 1",
          workspaceRoot: "/tmp/project-1",
          dataAudience: "private",
          repositoryIdentity: null,
          defaultModelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          faviconPath: null,
          scripts: [
            {
              id: "script-1",
              name: "Build",
              command: "bun run build",
              icon: "build",
              runOnWorktreeCreate: false,
            },
          ],
          defaultThreadEnvMode: null,
          createdAt: "2026-02-24T00:00:00.000Z",
          updatedAt: "2026-02-24T00:00:01.000Z",
        },
      ]);
      assert.deepEqual(shellSnapshot.threads, [
        {
          id: ThreadId.make("thread-1"),
          projectId: asProjectId("project-1"),
          dataAudience: "private",
          title: "Thread 1",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: "default",
          runtimeMode: "full-access",
          branch: null,
          worktreePath: null,
          worktreeRemovable: false,
          worktreeRemovalPath: null,
          latestTurn: {
            turnId: asTurnId("turn-1"),
            state: "completed",
            requestedAt: "2026-02-24T00:00:08.000Z",
            startedAt: "2026-02-24T00:00:08.000Z",
            completedAt: "2026-02-24T00:00:08.000Z",
            assistantMessageId: asMessageId("message-1"),
            sourceProposedPlan: {
              threadId: ThreadId.make("thread-1"),
              planId: "plan-1",
            },
          },
          createdAt: "2026-02-24T00:00:02.000Z",
          updatedAt: "2026-02-24T00:00:03.000Z",
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          snoozedUntil: null,
          snoozedAt: null,
          pinnedAt: "2026-02-24T00:00:01.000Z",
          pinOrderKey: "gm",
          titleRegeneration: null,

          session: {
            threadId: ThreadId.make("thread-1"),
            status: "running",
            providerName: "codex",
            runtimeMode: "approval-required",
            activeTurnId: asTurnId("turn-1"),
            lastError: null,
            updatedAt: "2026-02-24T00:00:07.000Z",
          },
          latestUserMessageAt: "2026-02-24T00:00:04.000Z",
          hasPendingApprovals: true,
          hasPendingUserInput: false,
          hasActionableProposedPlan: false,
          parentThreadId: ThreadId.make("thread-remote-parent"),
          parentEnvironmentId: asEnvironmentId("environment-remote-parent"),
          backgroundLiveness: null,
          planProgress: null,
        },
      ]);

      const threadShell = yield* snapshotQuery.getThreadShellById(ThreadId.make("thread-1"));
      assert.equal(threadShell._tag, "Some");
      if (threadShell._tag === "Some") {
        assert.equal(threadShell.value.parentThreadId, ThreadId.make("thread-remote-parent"));
        assert.equal(
          threadShell.value.parentEnvironmentId,
          asEnvironmentId("environment-remote-parent"),
        );
      }

      const threadDetail = yield* snapshotQuery.getThreadDetailById(ThreadId.make("thread-1"));
      assert.equal(threadDetail._tag, "Some");
      if (threadDetail._tag === "Some") {
        assert.equal(threadDetail.value.parentThreadId, ThreadId.make("thread-remote-parent"));
        assert.equal(
          threadDetail.value.parentEnvironmentId,
          asEnvironmentId("environment-remote-parent"),
        );
        assert.deepEqual(threadDetail.value.messages, [
          {
            id: asMessageId("message-prompt"),
            role: "system",
            text: "[sub-agent child-1 completed] done",
            turnId: asTurnId("turn-1"),
            streaming: false,
            createdAt: "2026-02-24T00:00:03.500Z",
            updatedAt: "2026-02-24T00:00:03.500Z",
          },
          {
            id: asMessageId("message-1"),
            role: "assistant",
            text: "hello from projection",
            turnId: asTurnId("turn-1"),
            streaming: false,
            createdAt: "2026-02-24T00:00:04.000Z",
            updatedAt: "2026-02-24T00:00:05.000Z",
          },
        ]);
        assert.deepEqual(threadDetail.value.proposedPlans, [
          {
            id: "plan-1",
            turnId: asTurnId("turn-1"),
            planMarkdown: "# Ship it",
            implementedAt: "2026-02-24T00:00:05.500Z",
            implementationThreadId: ThreadId.make("thread-2"),
            createdAt: "2026-02-24T00:00:05.000Z",
            updatedAt: "2026-02-24T00:00:05.500Z",
          },
        ]);
        assert.deepEqual(threadDetail.value.activities, [
          {
            id: asEventId("activity-1"),
            tone: "info",
            kind: "runtime.note",
            summary: "provider started",
            payload: { stage: "start" },
            turnId: asTurnId("turn-1"),
            createdAt: "2026-02-24T00:00:06.000Z",
          },
        ]);
        assert.deepEqual(threadDetail.value.checkpoints, [
          {
            turnId: asTurnId("turn-1"),
            checkpointTurnCount: 1,
            checkpointRef: asCheckpointRef("checkpoint-1"),
            status: "ready",
            files: [{ path: "README.md", kind: "modified", additions: 2, deletions: 1 }],
            assistantMessageId: asMessageId("message-1"),
            completedAt: "2026-02-24T00:00:08.000Z",
          },
        ]);
      }
    }),
  );

  it.effect("requires thread turn projector state for snapshot sequence", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_state`;
      yield* sql`
        INSERT INTO projection_state (projector, last_applied_sequence, updated_at)
        VALUES
          (${ORCHESTRATION_PROJECTOR_NAMES.projects}, 9, '2026-04-09T00:00:00.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threads}, 9, '2026-04-09T00:00:00.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadMessages}, 9, '2026-04-09T00:00:00.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadProposedPlans}, 9, '2026-04-09T00:00:00.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadActivities}, 9, '2026-04-09T00:00:00.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadSessions}, 9, '2026-04-09T00:00:00.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.checkpoints}, 9, '2026-04-09T00:00:00.000Z')
      `;

      assert.equal((yield* snapshotQuery.getSnapshot()).snapshotSequence, 0);

      yield* sql`
        INSERT INTO projection_state (projector, last_applied_sequence, updated_at)
        VALUES (${ORCHESTRATION_PROJECTOR_NAMES.threadTurns}, 7, '2026-04-09T00:00:01.000Z')
      `;

      assert.equal((yield* snapshotQuery.getSnapshot()).snapshotSequence, 7);
    }),
  );

  it.effect("keeps archived threads out of the main shell snapshot", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_projects`;
      yield* sql`DELETE FROM projection_threads`;
      yield* sql`DELETE FROM projection_state`;

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'project-archive-test',
          'Archive Test',
          '/tmp/archive-test',
          '{"provider":"codex","model":"gpt-5-codex"}',
          '[]',
          '2026-04-06T00:00:00.000Z',
          '2026-04-06T00:00:01.000Z',
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          latest_user_message_at,
          pending_approval_count,
          pending_user_input_count,
          has_actionable_proposed_plan,
          created_at,
          updated_at,
          archived_at,
          deleted_at
        )
        VALUES
          (
            'thread-active',
            'project-archive-test',
            'Active Thread',
            '{"provider":"codex","model":"gpt-5-codex"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            NULL,
            0,
            0,
            0,
            '2026-04-06T00:00:02.000Z',
            '2026-04-06T00:00:03.000Z',
            NULL,
            NULL
          ),
          (
            'thread-archived',
            'project-archive-test',
            'Archived Thread',
            '{"provider":"codex","model":"gpt-5-codex"}',
            'full-access',
            'default',
            NULL,
            NULL,
            'turn-archived',
            NULL,
            0,
            0,
            0,
            '2026-04-06T00:00:04.000Z',
            '2026-04-06T00:00:05.000Z',
            '2026-04-06T00:00:06.000Z',
            NULL
          )
      `;

      yield* sql`
        INSERT INTO projection_turns (
          thread_id,
          turn_id,
          pending_message_id,
          source_proposed_plan_thread_id,
          source_proposed_plan_id,
          assistant_message_id,
          state,
          requested_at,
          started_at,
          completed_at,
          checkpoint_turn_count,
          checkpoint_ref,
          checkpoint_status,
          checkpoint_files_json
        )
        VALUES (
          'thread-archived',
          'turn-archived',
          NULL,
          NULL,
          NULL,
          NULL,
          'completed',
          '2026-04-06T00:00:04.000Z',
          '2026-04-06T00:00:04.500Z',
          '2026-04-06T00:00:05.000Z',
          NULL,
          NULL,
          NULL,
          '[]'
        )
      `;

      yield* sql`
        INSERT INTO projection_state (projector, last_applied_sequence, updated_at)
        VALUES
          (${ORCHESTRATION_PROJECTOR_NAMES.projects}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threads}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadMessages}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadProposedPlans}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadActivities}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadSessions}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadTurns}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.checkpoints}, 4, '2026-04-06T00:00:07.000Z')
      `;

      const shellSnapshot = yield* snapshotQuery.getShellSnapshot();
      assert.deepEqual(
        shellSnapshot.threads.map((thread) => thread.id),
        [ThreadId.make("thread-active")],
      );

      const archivedShellSnapshot = yield* snapshotQuery.getArchivedShellSnapshot();
      assert.deepEqual(
        archivedShellSnapshot.threads.map((thread) => thread.id),
        [ThreadId.make("thread-archived")],
      );
      assert.equal(archivedShellSnapshot.threads[0]?.archivedAt, "2026-04-06T00:00:06.000Z");

      const archivedDetailSnapshot = yield* snapshotQuery.getThreadDetailSnapshot(
        ThreadId.make("thread-archived"),
      );
      assert.equal(archivedDetailSnapshot._tag, "Some");
      if (archivedDetailSnapshot._tag === "Some") {
        assert.equal(archivedDetailSnapshot.value.thread.id, "thread-archived");
        assert.equal(archivedDetailSnapshot.value.thread.archivedAt, "2026-04-06T00:00:06.000Z");
        assert.equal(archivedDetailSnapshot.value.thread.latestTurn?.turnId, "turn-archived");
      }

      const activeShellForArchived = yield* snapshotQuery.getThreadShellById(
        ThreadId.make("thread-archived"),
      );
      assert.equal(activeShellForArchived._tag, "None");

      const archivedInclusiveShell = yield* snapshotQuery.getThreadShellByIdIncludingArchived(
        ThreadId.make("thread-archived"),
      );
      assert.equal(archivedInclusiveShell._tag, "Some");
      if (archivedInclusiveShell._tag === "Some") {
        assert.equal(archivedInclusiveShell.value.id, "thread-archived");
        assert.equal(archivedInclusiveShell.value.archivedAt, "2026-04-06T00:00:06.000Z");
        assert.equal(archivedInclusiveShell.value.latestTurn?.turnId, "turn-archived");
      }
    }),
  );

  it.effect("keeps global snapshots lightweight for active and archived heavy threads", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;
      const heavyText = "x".repeat(2_048);
      const heavyPayload = encodeUnknownJsonString({ detail: "y".repeat(4_096) });

      yield* sql`DELETE FROM projection_projects`;
      yield* sql`DELETE FROM projection_threads`;
      yield* sql`DELETE FROM projection_thread_messages`;
      yield* sql`DELETE FROM projection_thread_activities`;
      yield* sql`DELETE FROM projection_state`;

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'project-heavy',
          'Heavy Snapshot Test',
          '/tmp/heavy-snapshot-test',
          '{"provider":"codex","model":"gpt-5-codex"}',
          '[]',
          '2026-04-07T00:00:00.000Z',
          '2026-04-07T00:00:01.000Z',
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          latest_user_message_at,
          pending_approval_count,
          pending_user_input_count,
          has_actionable_proposed_plan,
          created_at,
          updated_at,
          archived_at,
          deleted_at
        )
        VALUES
          (
            'thread-active-heavy',
            'project-heavy',
            'Active Heavy Thread',
            '{"provider":"codex","model":"gpt-5-codex"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            NULL,
            0,
            0,
            0,
            '2026-04-07T00:00:02.000Z',
            '2026-04-07T00:00:03.000Z',
            NULL,
            NULL
          ),
          (
            'thread-archived-heavy',
            'project-heavy',
            'Archived Heavy Thread',
            '{"provider":"codex","model":"gpt-5-codex"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            NULL,
            0,
            0,
            0,
            '2026-04-07T00:00:04.000Z',
            '2026-04-07T00:00:05.000Z',
            '2026-04-07T00:00:06.000Z',
            NULL
          )
      `;

      for (const threadId of ["thread-active-heavy", "thread-archived-heavy"] as const) {
        for (let index = 0; index < 25; index += 1) {
          yield* sql`
            INSERT INTO projection_thread_messages (
              message_id,
              thread_id,
              turn_id,
              role,
              text,
              is_streaming,
              created_at,
              updated_at
            )
            VALUES (
              ${`message-${threadId}-${index}`},
              ${threadId},
              NULL,
              'assistant',
              ${heavyText},
              0,
              '2026-04-07T00:00:07.000Z',
              '2026-04-07T00:00:08.000Z'
            )
          `;
          yield* sql`
            INSERT INTO projection_thread_activities (
              activity_id,
              thread_id,
              turn_id,
              tone,
              kind,
              summary,
              payload_json,
              sequence,
              created_at
            )
            VALUES (
              ${`activity-${threadId}-${index}`},
              ${threadId},
              NULL,
              'info',
              'runtime.note',
              'heavy activity',
              ${heavyPayload},
              ${index},
              '2026-04-07T00:00:09.000Z'
            )
          `;
        }
      }

      yield* sql`
        INSERT INTO projection_state (projector, last_applied_sequence, updated_at)
        VALUES
          (${ORCHESTRATION_PROJECTOR_NAMES.projects}, 8, '2026-04-07T00:00:10.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threads}, 8, '2026-04-07T00:00:10.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadMessages}, 8, '2026-04-07T00:00:10.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadProposedPlans}, 8, '2026-04-07T00:00:10.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadActivities}, 8, '2026-04-07T00:00:10.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadSessions}, 8, '2026-04-07T00:00:10.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadTurns}, 8, '2026-04-07T00:00:10.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.checkpoints}, 8, '2026-04-07T00:00:10.000Z')
      `;

      const snapshot = yield* snapshotQuery.getSnapshot();
      const encodedBytes = new TextEncoder().encode(encodeUnknownJsonString(snapshot)).byteLength;

      assert.isBelow(encodedBytes, 8_000);
      for (const thread of snapshot.threads) {
        assert.deepEqual(thread.messages, []);
        assert.deepEqual(thread.proposedPlans, []);
        assert.deepEqual(thread.activities, []);
        assert.deepEqual(thread.checkpoints, []);
      }

      const activeDetail = yield* snapshotQuery.getThreadDetailById(
        ThreadId.make("thread-active-heavy"),
      );
      assert.equal(activeDetail._tag, "Some");
      if (activeDetail._tag === "Some") {
        assert.equal(activeDetail.value.messages.length, 25);
        assert.equal(activeDetail.value.activities.length, 25);
      }
    }),
  );

  it.effect("keeps settled threads in the shell snapshot with non-null settlement fields", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_projects`;
      yield* sql`DELETE FROM projection_threads`;
      yield* sql`DELETE FROM projection_state`;

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'project-settled-test',
          'Settled Test',
          '/tmp/settled-test',
          '{"provider":"codex","model":"gpt-5-codex"}',
          '[]',
          '2026-04-06T00:00:00.000Z',
          '2026-04-06T00:00:01.000Z',
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          latest_user_message_at,
          pending_approval_count,
          pending_user_input_count,
          has_actionable_proposed_plan,
          created_at,
          updated_at,
          archived_at,
          settled_override,
          settled_at,
          deleted_at
        )
        VALUES (
          'thread-settled',
          'project-settled-test',
          'Settled Thread',
          '{"provider":"codex","model":"gpt-5-codex"}',
          'full-access',
          'default',
          NULL,
          NULL,
          NULL,
          '2026-04-06T00:00:03.000Z',
          1,
          0,
          0,
          '2026-04-06T00:00:02.000Z',
          '2026-04-06T00:00:05.000Z',
          NULL,
          'settled',
          '2026-04-06T00:00:04.000Z',
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_thread_messages (
          message_id,
          thread_id,
          turn_id,
          role,
          text,
          is_streaming,
          created_at,
          updated_at
        )
        VALUES (
          'message-settled-user',
          'thread-settled',
          NULL,
          'user',
          'Queued before restart',
          0,
          '2026-04-06T00:00:03.000Z',
          '2026-04-06T00:00:03.000Z'
        )
      `;

      yield* sql`
        INSERT INTO projection_thread_activities (
          activity_id,
          thread_id,
          turn_id,
          tone,
          kind,
          summary,
          payload_json,
          sequence,
          created_at
        )
        VALUES (
          'activity-settled-approval',
          'thread-settled',
          NULL,
          'approval',
          'approval.requested',
          'Approval pending before restart',
          '{"requestId":"request-before-restart"}',
          1,
          '2026-04-06T00:00:03.500Z'
        )
      `;

      yield* sql`
        INSERT INTO projection_state (projector, last_applied_sequence, updated_at)
        VALUES
          (${ORCHESTRATION_PROJECTOR_NAMES.projects}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threads}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadMessages}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadProposedPlans}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadActivities}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadSessions}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.checkpoints}, 4, '2026-04-06T00:00:07.000Z')
      `;

      // Settled ≠ archived: the thread must appear in the LIVE shell
      // snapshot, carrying its settlement fields through the row aliases.
      const shellSnapshot = yield* snapshotQuery.getShellSnapshot();
      assert.deepEqual(
        shellSnapshot.threads.map((thread) => thread.id),
        [ThreadId.make("thread-settled")],
      );
      assert.equal(shellSnapshot.threads[0]?.settledOverride, "settled");
      assert.equal(shellSnapshot.threads[0]?.settledAt, "2026-04-06T00:00:04.000Z");

      // The global command model carries lifecycle metadata but deliberately
      // does not hydrate conversation/activity bodies.
      const readModel = yield* snapshotQuery.getCommandReadModel();
      const thread = readModel.threads.find(
        (candidate) => candidate.id === ThreadId.make("thread-settled"),
      );
      assert.equal(thread?.settledOverride, "settled");
      assert.equal(thread?.settledAt, "2026-04-06T00:00:04.000Z");
      assert.deepEqual(thread?.messages, []);
      assert.deepEqual(thread?.activities, []);

      // Settle-time guards use the narrow shell query: it retains prompt
      // recency and uncapped projection counters without loading either body.
      const settleShell = yield* snapshotQuery.getThreadShellByIdIncludingArchived(
        ThreadId.make("thread-settled"),
      );
      assert.equal(settleShell._tag, "Some");
      if (settleShell._tag === "Some") {
        assert.equal(settleShell.value.latestUserMessageAt, "2026-04-06T00:00:03.000Z");
        assert.equal(settleShell.value.hasPendingApprovals, true);
        assert.equal(settleShell.value.hasPendingUserInput, false);
      }
    }),
  );

  it.effect("keeps settled threads in the shell snapshot with non-null settlement fields", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_projects`;
      yield* sql`DELETE FROM projection_threads`;
      yield* sql`DELETE FROM projection_state`;

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'project-settled-test',
          'Settled Test',
          '/tmp/settled-test',
          '{"provider":"codex","model":"gpt-5-codex"}',
          '[]',
          '2026-04-06T00:00:00.000Z',
          '2026-04-06T00:00:01.000Z',
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          latest_user_message_at,
          pending_approval_count,
          pending_user_input_count,
          has_actionable_proposed_plan,
          created_at,
          updated_at,
          archived_at,
          settled_override,
          settled_at,
          deleted_at
        )
        VALUES (
          'thread-settled',
          'project-settled-test',
          'Settled Thread',
          '{"provider":"codex","model":"gpt-5-codex"}',
          'full-access',
          'default',
          NULL,
          NULL,
          NULL,
          NULL,
          0,
          0,
          0,
          '2026-04-06T00:00:02.000Z',
          '2026-04-06T00:00:05.000Z',
          NULL,
          'settled',
          '2026-04-06T00:00:04.000Z',
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_state (projector, last_applied_sequence, updated_at)
        VALUES
          (${ORCHESTRATION_PROJECTOR_NAMES.projects}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threads}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadMessages}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadProposedPlans}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadActivities}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadSessions}, 4, '2026-04-06T00:00:07.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.checkpoints}, 4, '2026-04-06T00:00:07.000Z')
      `;

      // Settled ≠ archived: the thread must appear in the LIVE shell
      // snapshot, carrying its settlement fields through the row aliases.
      const shellSnapshot = yield* snapshotQuery.getShellSnapshot();
      assert.deepEqual(
        shellSnapshot.threads.map((thread) => thread.id),
        [ThreadId.make("thread-settled")],
      );
      assert.equal(shellSnapshot.threads[0]?.settledOverride, "settled");
      assert.equal(shellSnapshot.threads[0]?.settledAt, "2026-04-06T00:00:04.000Z");

      // And the full command read model carries them too.
      const readModel = yield* snapshotQuery.getCommandReadModel();
      const thread = readModel.threads.find(
        (candidate) => candidate.id === ThreadId.make("thread-settled"),
      );
      assert.equal(thread?.settledOverride, "settled");
      assert.equal(thread?.settledAt, "2026-04-06T00:00:04.000Z");
    }),
  );

  it.effect(
    "reads targeted project, thread, and count queries without hydrating the full snapshot",
    () =>
      Effect.gen(function* () {
        const snapshotQuery = yield* ProjectionSnapshotQuery;
        const sql = yield* SqlClient.SqlClient;

        yield* sql`DELETE FROM projection_projects`;
        yield* sql`DELETE FROM projection_threads`;
        yield* sql`DELETE FROM projection_turns`;

        yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES
          (
            'project-active',
            'Active Project',
            '/tmp/workspace',
            '{"provider":"codex","model":"gpt-5-codex"}',
            '[]',
            '2026-03-01T00:00:00.000Z',
            '2026-03-01T00:00:01.000Z',
            NULL
          ),
          (
            'project-deleted',
            'Deleted Project',
            '/tmp/deleted',
            NULL,
            '[]',
            '2026-03-01T00:00:02.000Z',
            '2026-03-01T00:00:03.000Z',
            '2026-03-01T00:00:04.000Z'
          )
      `;

        yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          created_at,
          updated_at,
          archived_at,
          deleted_at
        )
        VALUES
          (
            'thread-first',
            'project-active',
            'First Thread',
            '{"provider":"codex","model":"gpt-5-codex"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            '2026-03-01T00:00:05.000Z',
            '2026-03-01T00:00:06.000Z',
            NULL,
            NULL
          ),
          (
            'thread-second',
            'project-active',
            'Second Thread',
            '{"provider":"codex","model":"gpt-5-codex"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            '2026-03-01T00:00:07.000Z',
            '2026-03-01T00:00:08.000Z',
            NULL,
            NULL
          ),
          (
            'thread-deleted',
            'project-active',
            'Deleted Thread',
            '{"provider":"codex","model":"gpt-5-codex"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            '2026-03-01T00:00:09.000Z',
            '2026-03-01T00:00:10.000Z',
            NULL,
            '2026-03-01T00:00:11.000Z'
          )
      `;

        const counts = yield* snapshotQuery.getCounts();
        assert.deepEqual(counts, {
          projectCount: 2,
          threadCount: 3,
        });

        const project = yield* snapshotQuery.getActiveProjectByWorkspaceRoot("/tmp/workspace");
        assert.equal(project._tag, "Some");
        if (project._tag === "Some") {
          assert.equal(project.value.id, asProjectId("project-active"));
        }

        const missingProject = yield* snapshotQuery.getActiveProjectByWorkspaceRoot("/tmp/missing");
        assert.equal(missingProject._tag, "None");

        const firstThreadId = yield* snapshotQuery.getFirstActiveThreadIdByProjectId(
          asProjectId("project-active"),
        );
        assert.equal(firstThreadId._tag, "Some");
        if (firstThreadId._tag === "Some") {
          assert.equal(firstThreadId.value, ThreadId.make("thread-first"));
        }
      }),
  );

  it.effect("reads single-thread checkpoint context without hydrating unrelated threads", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_projects`;
      yield* sql`DELETE FROM projection_threads`;
      yield* sql`DELETE FROM projection_turns`;

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'project-context',
          'Context Project',
          '/tmp/context-workspace',
          NULL,
          '[]',
          '2026-03-02T00:00:00.000Z',
          '2026-03-02T00:00:01.000Z',
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          created_at,
          updated_at,
          archived_at,
          deleted_at
        )
        VALUES (
          'thread-context',
          'project-context',
          'Context Thread',
          '{"provider":"codex","model":"gpt-5-codex"}',
          'full-access',
          'default',
          'feature/perf',
          '/tmp/context-worktree',
          NULL,
          '2026-03-02T00:00:02.000Z',
          '2026-03-02T00:00:03.000Z',
          NULL,
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_turns (
          thread_id,
          turn_id,
          pending_message_id,
          source_proposed_plan_thread_id,
          source_proposed_plan_id,
          assistant_message_id,
          state,
          requested_at,
          started_at,
          completed_at,
          checkpoint_turn_count,
          checkpoint_ref,
          checkpoint_status,
          checkpoint_files_json
        )
        VALUES
          (
            'thread-context',
            'turn-1',
            NULL,
            NULL,
            NULL,
            NULL,
            'completed',
            '2026-03-02T00:00:04.000Z',
            '2026-03-02T00:00:04.000Z',
            '2026-03-02T00:00:04.000Z',
            1,
            'checkpoint-a',
            'ready',
            '[]'
          ),
          (
            'thread-context',
            'turn-2',
            NULL,
            NULL,
            NULL,
            NULL,
            'completed',
            '2026-03-02T00:00:05.000Z',
            '2026-03-02T00:00:05.000Z',
            '2026-03-02T00:00:05.000Z',
            2,
            'checkpoint-b',
            'ready',
            '[]'
          )
      `;

      const context = yield* snapshotQuery.getThreadCheckpointContext(
        ThreadId.make("thread-context"),
        { trackedTurnId: asTurnId("turn-2") },
      );
      assert.equal(context._tag, "Some");
      if (context._tag === "Some") {
        assert.deepEqual(context.value, {
          threadId: ThreadId.make("thread-context"),
          projectId: asProjectId("project-context"),
          workspaceRoot: "/tmp/context-workspace",
          worktreePath: "/tmp/context-worktree",
          trackedCheckpointTurnIds: [asTurnId("turn-2")],
          checkpoints: [
            {
              turnId: asTurnId("turn-1"),
              checkpointTurnCount: 1,
              checkpointRef: asCheckpointRef("checkpoint-a"),
              status: "ready",
              files: [],
              assistantMessageId: null,
              completedAt: "2026-03-02T00:00:04.000Z",
            },
            {
              turnId: asTurnId("turn-2"),
              checkpointTurnCount: 2,
              checkpointRef: asCheckpointRef("checkpoint-b"),
              status: "ready",
              files: [],
              assistantMessageId: null,
              completedAt: "2026-03-02T00:00:05.000Z",
            },
          ],
        });
      }
    }),
  );

  it.effect("exposes only checkpoint refs in the retained tail", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_projects`;
      yield* sql`DELETE FROM projection_threads`;
      yield* sql`DELETE FROM projection_turns`;

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'project-retention',
          'Retention Project',
          '/tmp/retention-workspace',
          NULL,
          '[]',
          '2026-03-03T00:00:00.000Z',
          '2026-03-03T00:00:01.000Z',
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          created_at,
          updated_at,
          archived_at,
          deleted_at
        )
        VALUES (
          'thread-retention',
          'project-retention',
          'Retention Thread',
          '{"provider":"codex","model":"gpt-5-codex"}',
          'full-access',
          'default',
          'feature/retention',
          '/tmp/retention-worktree',
          NULL,
          '2026-03-03T00:00:02.000Z',
          '2026-03-03T00:00:03.000Z',
          NULL,
          NULL
        )
      `;

      yield* sql`
        WITH RECURSIVE checkpoint_counts(checkpoint_turn_count) AS (
          SELECT 1
          UNION ALL
          SELECT checkpoint_turn_count + 1
          FROM checkpoint_counts
          WHERE checkpoint_turn_count < ${MAX_THREAD_CHECKPOINTS + 5}
        )
        INSERT INTO projection_turns (
          thread_id,
          turn_id,
          pending_message_id,
          source_proposed_plan_thread_id,
          source_proposed_plan_id,
          assistant_message_id,
          state,
          requested_at,
          started_at,
          completed_at,
          checkpoint_turn_count,
          checkpoint_ref,
          checkpoint_status,
          checkpoint_files_json
        )
        SELECT
          'thread-retention',
          'turn-' || checkpoint_turn_count,
          NULL,
          NULL,
          NULL,
          NULL,
          'completed',
          '2026-03-03T00:00:04.000Z',
          '2026-03-03T00:00:04.000Z',
          '2026-03-03T00:00:04.000Z',
          checkpoint_turn_count,
          'refs/t3/checkpoints/thread-retention/turn/' || checkpoint_turn_count,
          'ready',
          '[]'
        FROM checkpoint_counts
      `;

      const context = yield* snapshotQuery.getThreadCheckpointContext(
        ThreadId.make("thread-retention"),
        { trackedTurnId: asTurnId("turn-1") },
      );
      assert.equal(context._tag, "Some");
      if (context._tag === "Some") {
        assert.equal(context.value.trackedCheckpointTurnIds?.length, 1);
        assert.equal(context.value.trackedCheckpointTurnIds?.[0], asTurnId("turn-1"));
        assert.equal(context.value.checkpoints.length, MAX_THREAD_CHECKPOINTS + 1);
        assert.equal(context.value.checkpoints[0]?.checkpointTurnCount, 5);
        assert.equal(
          context.value.checkpoints[0]?.checkpointRef,
          asCheckpointRef("refs/t3/checkpoints/thread-retention/turn/5"),
        );
        assert.equal(context.value.checkpoints.at(-1)?.checkpointTurnCount, 505);
      }

      const detail = yield* snapshotQuery.getThreadDetailById(ThreadId.make("thread-retention"));
      assert.equal(detail._tag, "Some");
      if (detail._tag === "Some") {
        assert.equal(detail.value.checkpoints.length, MAX_THREAD_CHECKPOINTS);
        assert.equal(detail.value.checkpoints[0]?.checkpointTurnCount, 6);
        assert.equal(detail.value.checkpoints.at(-1)?.checkpointTurnCount, 505);
      }

      const prunedDiffContext = yield* snapshotQuery.getFullThreadDiffContext(
        ThreadId.make("thread-retention"),
        4,
      );
      assert.equal(prunedDiffContext._tag, "Some");
      if (prunedDiffContext._tag === "Some") {
        assert.equal(prunedDiffContext.value.toCheckpointRef, null);
      }

      const boundaryDiffContext = yield* snapshotQuery.getFullThreadDiffContext(
        ThreadId.make("thread-retention"),
        5,
      );
      assert.equal(boundaryDiffContext._tag, "Some");
      if (boundaryDiffContext._tag === "Some") {
        assert.equal(
          boundaryDiffContext.value.toCheckpointRef,
          asCheckpointRef("refs/t3/checkpoints/thread-retention/turn/5"),
        );
      }

      const retainedDiffContext = yield* snapshotQuery.getFullThreadDiffContext(
        ThreadId.make("thread-retention"),
        6,
      );
      assert.equal(retainedDiffContext._tag, "Some");
      if (retainedDiffContext._tag === "Some") {
        assert.equal(
          retainedDiffContext.value.toCheckpointRef,
          asCheckpointRef("refs/t3/checkpoints/thread-retention/turn/6"),
        );
      }
    }),
  );

  it.effect("keeps thread detail activity ordering consistent with shell snapshot ordering", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_projects`;
      yield* sql`DELETE FROM projection_threads`;
      yield* sql`DELETE FROM projection_thread_activities`;
      yield* sql`DELETE FROM projection_state`;

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'project-1',
          'Project 1',
          '/tmp/project-1',
          '{"provider":"codex","model":"gpt-5-codex"}',
          '[]',
          '2026-04-01T00:00:00.000Z',
          '2026-04-01T00:00:01.000Z',
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          latest_user_message_at,
          pending_approval_count,
          pending_user_input_count,
          has_actionable_proposed_plan,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'thread-1',
          'project-1',
          'Thread 1',
          '{"provider":"codex","model":"gpt-5-codex"}',
          'full-access',
          'default',
          NULL,
          NULL,
          NULL,
          NULL,
          0,
          0,
          0,
          '2026-04-01T00:00:02.000Z',
          '2026-04-01T00:00:03.000Z',
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_thread_activities (
          activity_id,
          thread_id,
          turn_id,
          tone,
          kind,
          summary,
          payload_json,
          sequence,
          created_at
        )
        VALUES
          (
            'activity-unsequenced',
            'thread-1',
            NULL,
            'info',
            'runtime.note',
            'unsequenced first',
            '{"source":"unsequenced"}',
            NULL,
            '2026-04-01T00:00:06.000Z'
          ),
          (
            'activity-sequence-2',
            'thread-1',
            NULL,
            'info',
            'runtime.note',
            'sequence two',
            '{"source":"sequence-2"}',
            2,
            '2026-04-01T00:00:04.000Z'
          ),
          (
            'activity-sequence-1',
            'thread-1',
            NULL,
            'info',
            'runtime.note',
            'sequence one',
            '{"source":"sequence-1"}',
            1,
            '2026-04-01T00:00:05.000Z'
          )
      `;

      const snapshot = yield* snapshotQuery.getSnapshot();
      const threadDetail = yield* snapshotQuery.getThreadDetailById(ThreadId.make("thread-1"));

      assert.deepEqual(snapshot.threads[0]?.activities ?? [], []);
      assert.equal(threadDetail._tag, "Some");
      if (threadDetail._tag === "Some") {
        assert.deepEqual(threadDetail.value.activities, [
          {
            id: asEventId("activity-unsequenced"),
            tone: "info",
            kind: "runtime.note",
            summary: "unsequenced first",
            payload: { source: "unsequenced" },
            turnId: null,
            createdAt: "2026-04-01T00:00:06.000Z",
          },
          {
            id: asEventId("activity-sequence-1"),
            tone: "info",
            kind: "runtime.note",
            summary: "sequence one",
            payload: { source: "sequence-1" },
            turnId: null,
            sequence: 1,
            createdAt: "2026-04-01T00:00:05.000Z",
          },
          {
            id: asEventId("activity-sequence-2"),
            tone: "info",
            kind: "runtime.note",
            summary: "sequence two",
            payload: { source: "sequence-2" },
            turnId: null,
            sequence: 2,
            createdAt: "2026-04-01T00:00:04.000Z",
          },
        ]);
      }
    }),
  );

  it.effect("uses projection_threads.latest_turn_id for targeted thread latest turn queries", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_projects`;
      yield* sql`DELETE FROM projection_threads`;
      yield* sql`DELETE FROM projection_turns`;

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'project-1',
          'Project 1',
          '/tmp/project-1',
          '{"provider":"codex","model":"gpt-5-codex"}',
          '[]',
          '2026-04-02T00:00:00.000Z',
          '2026-04-02T00:00:01.000Z',
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          latest_user_message_at,
          pending_approval_count,
          pending_user_input_count,
          has_actionable_proposed_plan,
          created_at,
          updated_at,
          archived_at,
          deleted_at
        )
        VALUES (
          'thread-1',
          'project-1',
          'Thread 1',
          '{"provider":"codex","model":"gpt-5-codex"}',
          'full-access',
          'default',
          NULL,
          NULL,
          'turn-running',
          '2026-04-02T00:00:04.000Z',
          0,
          0,
          0,
          '2026-04-02T00:00:02.000Z',
          '2026-04-02T00:00:03.000Z',
          NULL,
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_turns (
          thread_id,
          turn_id,
          pending_message_id,
          source_proposed_plan_thread_id,
          source_proposed_plan_id,
          assistant_message_id,
          state,
          requested_at,
          started_at,
          completed_at,
          checkpoint_turn_count,
          checkpoint_ref,
          checkpoint_status,
          checkpoint_files_json
        )
        VALUES
          (
            'thread-1',
            'turn-completed',
            'message-user-1',
            NULL,
            NULL,
            'message-assistant-1',
            'completed',
            '2026-04-02T00:00:05.000Z',
            '2026-04-02T00:00:06.000Z',
            '2026-04-02T00:00:20.000Z',
            5,
            'checkpoint-5',
            'ready',
            '[]'
          ),
          (
            'thread-1',
            'turn-running',
            'message-user-2',
            NULL,
            NULL,
            NULL,
            'running',
            '2026-04-02T00:00:30.000Z',
            '2026-04-02T00:00:30.000Z',
            NULL,
            NULL,
            NULL,
            NULL,
            '[]'
          )
      `;

      const threadShell = yield* snapshotQuery.getThreadShellById(ThreadId.make("thread-1"));
      assert.equal(threadShell._tag, "Some");
      if (threadShell._tag === "Some") {
        assert.equal(threadShell.value.latestTurn?.turnId, asTurnId("turn-running"));
        assert.equal(threadShell.value.latestTurn?.state, "running");
        assert.equal(threadShell.value.latestTurn?.startedAt, "2026-04-02T00:00:30.000Z");
      }

      const threadDetail = yield* snapshotQuery.getThreadDetailById(ThreadId.make("thread-1"));
      assert.equal(threadDetail._tag, "Some");
      if (threadDetail._tag === "Some") {
        assert.equal(threadDetail.value.latestTurn?.turnId, asTurnId("turn-running"));
        assert.equal(threadDetail.value.latestTurn?.state, "running");
        assert.equal(threadDetail.value.latestTurn?.startedAt, "2026-04-02T00:00:30.000Z");
        assert.deepEqual(
          threadDetail.value.turns.map((turn) => ({
            turnId: turn.turnId,
            state: turn.state,
            assistantMessageId: turn.assistantMessageId,
          })),
          [
            {
              turnId: asTurnId("turn-completed"),
              state: "completed",
              assistantMessageId: asMessageId("message-assistant-1"),
            },
            {
              turnId: asTurnId("turn-running"),
              state: "running",
              assistantMessageId: null,
            },
          ],
        );
      }
    }),
  );

  it.effect("uses projection_threads.latest_turn_id for bulk command and shell snapshots", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_projects`;
      yield* sql`DELETE FROM projection_threads`;
      yield* sql`DELETE FROM projection_turns`;
      yield* sql`DELETE FROM projection_state`;

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'project-1',
          'Project 1',
          '/tmp/project-1',
          '{"provider":"codex","model":"gpt-5-codex"}',
          '[]',
          '2026-04-03T00:00:00.000Z',
          '2026-04-03T00:00:01.000Z',
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          latest_user_message_at,
          pending_approval_count,
          pending_user_input_count,
          has_actionable_proposed_plan,
          created_at,
          updated_at,
          archived_at,
          deleted_at
        )
        VALUES (
          'thread-1',
          'project-1',
          'Thread 1',
          '{"provider":"codex","model":"gpt-5-codex"}',
          'full-access',
          'default',
          NULL,
          NULL,
          'turn-running',
          '2026-04-03T00:00:04.000Z',
          0,
          0,
          0,
          '2026-04-03T00:00:02.000Z',
          '2026-04-03T00:00:03.000Z',
          NULL,
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_turns (
          thread_id,
          turn_id,
          pending_message_id,
          source_proposed_plan_thread_id,
          source_proposed_plan_id,
          assistant_message_id,
          state,
          requested_at,
          started_at,
          completed_at,
          checkpoint_turn_count,
          checkpoint_ref,
          checkpoint_status,
          checkpoint_files_json
        )
        VALUES
          (
            'thread-1',
            'turn-running',
            'message-user-2',
            NULL,
            NULL,
            NULL,
            'running',
            '2026-04-03T00:00:30.000Z',
            '2026-04-03T00:00:30.000Z',
            NULL,
            NULL,
            NULL,
            NULL,
            '[]'
          ),
          (
            'thread-1',
            'turn-completed',
            'message-user-1',
            NULL,
            NULL,
            'message-assistant-1',
            'completed',
            '2026-04-03T00:00:05.000Z',
            '2026-04-03T00:00:06.000Z',
            '2026-04-03T00:00:20.000Z',
            NULL,
            NULL,
            NULL,
            '[]'
          )
      `;

      yield* sql`
        INSERT INTO projection_state (projector, last_applied_sequence, updated_at)
        VALUES
          (${ORCHESTRATION_PROJECTOR_NAMES.projects}, 3, '2026-04-03T00:00:40.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threads}, 3, '2026-04-03T00:00:40.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadMessages}, 3, '2026-04-03T00:00:40.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadProposedPlans}, 3, '2026-04-03T00:00:40.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadActivities}, 3, '2026-04-03T00:00:40.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadSessions}, 3, '2026-04-03T00:00:40.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.threadTurns}, 3, '2026-04-03T00:00:40.000Z'),
          (${ORCHESTRATION_PROJECTOR_NAMES.checkpoints}, 3, '2026-04-03T00:00:40.000Z')
      `;

      const commandReadModel = yield* snapshotQuery.getCommandReadModel();
      assert.equal(commandReadModel.threads[0]?.latestTurn?.turnId, asTurnId("turn-running"));
      assert.equal(commandReadModel.threads[0]?.latestTurn?.state, "running");

      const shellSnapshot = yield* snapshotQuery.getShellSnapshot();
      assert.equal(shellSnapshot.threads[0]?.latestTurn?.turnId, asTurnId("turn-running"));
      assert.equal(shellSnapshot.threads[0]?.latestTurn?.state, "running");

      const fullSnapshot = yield* snapshotQuery.getSnapshot();
      assert.equal(fullSnapshot.threads[0]?.latestTurn?.turnId, asTurnId("turn-running"));
      assert.equal(fullSnapshot.threads[0]?.latestTurn?.state, "running");
    }),
  );

  it.effect("keeps deleted project and thread tombstones in the command read model", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_projects`;
      yield* sql`DELETE FROM projection_threads`;
      yield* sql`DELETE FROM projection_turns`;
      yield* sql`DELETE FROM projection_state`;

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'project-deleted',
          'Deleted Project',
          '/tmp/deleted-project',
          '{"provider":"codex","model":"gpt-5-codex"}',
          '[]',
          '2026-04-05T00:00:00.000Z',
          '2026-04-05T00:00:01.000Z',
          '2026-04-05T00:00:02.000Z'
        )
      `;

      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          latest_user_message_at,
          pending_approval_count,
          pending_user_input_count,
          has_actionable_proposed_plan,
          created_at,
          updated_at,
          archived_at,
          deleted_at
        )
        VALUES (
          'thread-deleted',
          'project-deleted',
          'Deleted Thread',
          '{"provider":"codex","model":"gpt-5-codex"}',
          'full-access',
          'default',
          NULL,
          NULL,
          'turn-deleted',
          NULL,
          0,
          0,
          0,
          '2026-04-05T00:00:03.000Z',
          '2026-04-05T00:00:04.000Z',
          NULL,
          '2026-04-05T00:00:05.000Z'
        )
      `;

      yield* sql`
        INSERT INTO projection_turns (
          thread_id,
          turn_id,
          pending_message_id,
          source_proposed_plan_thread_id,
          source_proposed_plan_id,
          assistant_message_id,
          state,
          requested_at,
          started_at,
          completed_at,
          checkpoint_turn_count,
          checkpoint_ref,
          checkpoint_status,
          checkpoint_files_json
        )
        VALUES (
          'thread-deleted',
          'turn-deleted',
          'message-deleted-user',
          NULL,
          NULL,
          'message-deleted-assistant',
          'completed',
          '2026-04-05T00:00:04.100Z',
          '2026-04-05T00:00:04.200Z',
          '2026-04-05T00:00:04.300Z',
          NULL,
          NULL,
          NULL,
          '[]'
        )
      `;

      const commandReadModel = yield* snapshotQuery.getCommandReadModel();
      assert.equal(commandReadModel.projects[0]?.id, asProjectId("project-deleted"));
      assert.equal(commandReadModel.projects[0]?.deletedAt, "2026-04-05T00:00:02.000Z");
      assert.equal(commandReadModel.threads[0]?.id, ThreadId.make("thread-deleted"));
      assert.equal(commandReadModel.threads[0]?.deletedAt, "2026-04-05T00:00:05.000Z");
      assert.equal(commandReadModel.threads[0]?.latestTurn?.turnId, asTurnId("turn-deleted"));
      assert.equal(commandReadModel.threads[0]?.latestTurn?.state, "completed");

      const fullSnapshot = yield* snapshotQuery.getSnapshot();
      assert.equal(fullSnapshot.threads[0]?.id, ThreadId.make("thread-deleted"));
      assert.equal(fullSnapshot.threads[0]?.latestTurn?.turnId, asTurnId("turn-deleted"));
      assert.equal(fullSnapshot.threads[0]?.latestTurn?.state, "completed");

      const shellSnapshot = yield* snapshotQuery.getShellSnapshot();
      assert.equal(shellSnapshot.projects.length, 0);
      assert.equal(shellSnapshot.threads.length, 0);
    }),
  );

  it.effect("searches active user messages and canonical assistant outputs", () =>
    Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_thread_messages`;
      yield* sql`DELETE FROM projection_turns`;
      yield* sql`DELETE FROM projection_threads`;
      yield* sql`DELETE FROM projection_projects`;

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'project-search',
          'Project Needle',
          '/tmp/project-search',
          '{"provider":"codex","model":"gpt-5-codex"}',
          '[]',
          '2026-05-01T00:00:00.000Z',
          '2026-05-01T00:00:01.000Z',
          NULL
        )
      `;

      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          latest_user_message_at,
          pending_approval_count,
          pending_user_input_count,
          has_actionable_proposed_plan,
          created_at,
          updated_at,
          archived_at,
          deleted_at
        )
        VALUES
          (
            'thread-active',
            'project-search',
            'Literal 100% fix',
            '{"provider":"codex","model":"gpt-5-codex"}',
            'full-access',
            'default',
            'search-branch',
            NULL,
            'turn-active',
            '2026-05-01T00:00:02.000Z',
            0,
            0,
            0,
            '2026-05-01T00:00:02.000Z',
            '2026-05-01T00:00:03.000Z',
            NULL,
            NULL
          ),
          (
            'thread-percent-decoy',
            'project-search',
            'Literal 100x fix',
            '{"provider":"codex","model":"gpt-5-codex"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            NULL,
            0,
            0,
            0,
            '2026-05-01T00:00:04.000Z',
            '2026-05-01T00:00:05.000Z',
            NULL,
            NULL
          ),
          (
            'thread-hidden',
            'project-search',
            'Archived search',
            '{"provider":"codex","model":"gpt-5-codex"}',
            'full-access',
            'default',
            NULL,
            NULL,
            NULL,
            NULL,
            0,
            0,
            0,
            '2026-05-01T00:00:06.000Z',
            '2026-05-01T00:00:07.000Z',
            '2026-05-01T00:00:08.000Z',
            NULL
          )
      `;

      yield* sql`
        INSERT INTO projection_thread_messages (
          message_id,
          thread_id,
          turn_id,
          role,
          text,
          is_streaming,
          created_at,
          updated_at
        )
        VALUES
          (
            'message-user',
            'thread-active',
            'turn-active',
            'user',
            'Please find this USER needle in an old prompt.',
            0,
            '2026-05-01T00:00:12.000Z',
            '2026-05-01T00:00:12.000Z'
          ),
          (
            'message-percent',
            'thread-active',
            NULL,
            'user',
            'Literal 100% fix in a prompt.',
            0,
            '2026-05-01T00:00:11.000Z',
            '2026-05-01T00:00:11.000Z'
          ),
          (
            'message-percent-decoy',
            'thread-percent-decoy',
            NULL,
            'user',
            'Literal 100x fix in a prompt.',
            0,
            '2026-05-01T00:00:11.000Z',
            '2026-05-01T00:00:11.000Z'
          ),
          (
            'message-final',
            'thread-active',
            'turn-active',
            'assistant',
            'The canonical final needle appears in this completed answer.',
            0,
            '2026-05-01T00:00:13.000Z',
            '2026-05-01T00:00:13.000Z'
          ),
          (
            'message-interim',
            'thread-active',
            'turn-active',
            'assistant',
            'Interim needle must not be searchable.',
            0,
            '2026-05-01T00:00:14.000Z',
            '2026-05-01T00:00:14.000Z'
          ),
          (
            'message-system',
            'thread-active',
            NULL,
            'system',
            'System needle must not be searchable.',
            0,
            '2026-05-01T00:00:15.000Z',
            '2026-05-01T00:00:15.000Z'
          ),
          (
            'message-hidden',
            'thread-hidden',
            NULL,
            'user',
            'Hidden needle in archive.',
            0,
            '2026-05-01T00:00:16.000Z',
            '2026-05-01T00:00:16.000Z'
          )
      `;

      yield* sql`
        INSERT INTO projection_turns (
          thread_id,
          turn_id,
          pending_message_id,
          assistant_message_id,
          state,
          requested_at,
          started_at,
          completed_at,
          checkpoint_files_json
        )
        VALUES (
          'thread-active',
          'turn-active',
          'message-user',
          'message-final',
          'completed',
          '2026-05-01T00:00:12.000Z',
          '2026-05-01T00:00:12.000Z',
          '2026-05-01T00:00:13.000Z',
          '[]'
        )
      `;

      const literalPercent = yield* snapshotQuery.searchThreads({ query: "100%" });
      assert.deepStrictEqual(
        literalPercent.matches.map((match) => [match.threadId, match.source]),
        [[ThreadId.make("thread-active"), "user"]],
      );

      const user = yield* snapshotQuery.searchThreads({ query: "user needle" });
      assert.equal(user.matches[0]?.source, "user");
      assert.match(user.matches[0]?.snippet ?? "", /USER needle/);

      const assistant = yield* snapshotQuery.searchThreads({ query: "FINAL NEEDLE" });
      assert.equal(assistant.matches[0]?.source, "assistant");

      const deduped = yield* snapshotQuery.searchThreads({ query: "needle" });
      assert.deepStrictEqual(
        deduped.matches.map((match) => [match.threadId, match.source]),
        [[ThreadId.make("thread-active"), "user"]],
      );

      assert.deepStrictEqual(
        (yield* snapshotQuery.searchThreads({ query: "interim needle" })).matches,
        [],
      );
      assert.deepStrictEqual(
        (yield* snapshotQuery.searchThreads({ query: "system needle" })).matches,
        [],
      );
      assert.deepStrictEqual(
        (yield* snapshotQuery.searchThreads({ query: "hidden needle" })).matches,
        [],
      );
      yield* sql`
        UPDATE projection_threads
        SET deleted_at = '2026-05-01T00:00:20.000Z'
        WHERE thread_id = 'thread-active'
      `;
      assert.deepStrictEqual(
        (yield* snapshotQuery.searchThreads({ query: "user needle" })).matches,
        [],
      );
    }),
  );
});

it.effect(
  "ProjectionSnapshotQuery dedupes repository identity resolution by workspace root and skips deleted projects for shell snapshots",
  () => {
    const resolveCalls: string[] = [];
    const layer = OrchestrationProjectionSnapshotQueryLive.pipe(
      Layer.provide(ThreadBackgroundLiveness.layer),
      Layer.provide(ThreadPlanProgress.layer),
      Layer.provideMerge(
        Layer.succeed(RepositoryIdentityResolver.RepositoryIdentityResolver, {
          resolve: (cwd: string) =>
            Effect.sync(() => {
              resolveCalls.push(cwd);
              return {
                canonicalKey: `github.com/acme${cwd}`,
                locator: {
                  source: "git-remote" as const,
                  remoteName: "origin",
                  remoteUrl: `https://github.com/acme${cwd}.git`,
                },
                rootPath: cwd,
              };
            }),
        }),
      ),
      Layer.provideMerge(SqlitePersistenceMemory),
    );

    return Effect.gen(function* () {
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_projects`;
      yield* sql`DELETE FROM projection_threads`;
      yield* sql`DELETE FROM projection_turns`;
      yield* sql`DELETE FROM projection_state`;

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES
          (
            'project-1',
            'Shared Project 1',
            '/tmp/shared-root',
            '{"provider":"codex","model":"gpt-5-codex"}',
            '[]',
            '2026-04-04T00:00:00.000Z',
            '2026-04-04T00:00:01.000Z',
            NULL
          ),
          (
            'project-2',
            'Shared Project 2',
            '/tmp/shared-root',
            '{"provider":"codex","model":"gpt-5-codex"}',
            '[]',
            '2026-04-04T00:00:02.000Z',
            '2026-04-04T00:00:03.000Z',
            NULL
          ),
          (
            'project-3',
            'Deleted Project',
            '/tmp/deleted-root',
            '{"provider":"codex","model":"gpt-5-codex"}',
            '[]',
            '2026-04-04T00:00:04.000Z',
            '2026-04-04T00:00:05.000Z',
            '2026-04-04T00:00:06.000Z'
          )
      `;

      const shellSnapshot = yield* snapshotQuery.getShellSnapshot();
      assert.deepStrictEqual(resolveCalls.toSorted(), ["/tmp/shared-root"]);
      assert.equal(shellSnapshot.projects.length, 2);
      assert.equal(shellSnapshot.projects[0]?.repositoryIdentity?.rootPath, "/tmp/shared-root");
      assert.equal(shellSnapshot.projects[1]?.repositoryIdentity?.rootPath, "/tmp/shared-root");

      resolveCalls.length = 0;

      const fullSnapshot = yield* snapshotQuery.getSnapshot();
      assert.deepStrictEqual(resolveCalls.toSorted(), ["/tmp/deleted-root", "/tmp/shared-root"]);
      assert.equal(fullSnapshot.projects.length, 3);
      assert.equal(fullSnapshot.projects[2]?.repositoryIdentity?.rootPath, "/tmp/deleted-root");
    }).pipe(Effect.provide(layer));
  },
);

projectionSnapshotLayer("ProjectionSnapshotQuery windowed thread detail", (it) => {
  // A thread shaped like real fan-out usage: user turns interleaved with
  // subagent turns (no user pending message), plus a turnless straggler user
  // message and a turnless activity anchored between turns.
  //
  //   row  turn      pending msg        anchor (requested_at)
  //   1    turn-1    user-msg-1         T00
  //   2    turn-2    (subagent)         T01
  //   3    turn-3    (subagent)         T02
  //   4    turn-4    user-msg-4         T03
  //   5    turn-5    user-msg-5         T04
  //
  // Straggler user message at T03.5 (turn_id NULL, not any pending_message_id)
  // and a turnless activity at T03.6 — both belong to the page containing T03+.
  const seedFanOutThread = Effect.fnUntraced(function* () {
    const sql = yield* SqlClient.SqlClient;

    // Tests in this block share one in-memory database; reset before seeding.
    yield* sql`DELETE FROM projection_projects`;
    yield* sql`DELETE FROM projection_threads`;
    yield* sql`DELETE FROM projection_turns`;
    yield* sql`DELETE FROM projection_thread_messages`;
    yield* sql`DELETE FROM projection_thread_activities`;
    yield* sql`DELETE FROM projection_state`;

    yield* sql`
      INSERT INTO projection_projects (
        project_id, title, workspace_root, scripts_json, created_at, updated_at, deleted_at
      )
      VALUES ('project-w', 'Windowed', '/tmp/project-w', '[]',
        '2026-03-01T00:00:00.000Z', '2026-03-01T00:00:00.000Z', NULL)
    `;
    yield* sql`
      INSERT INTO projection_threads (
        thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode,
        latest_turn_id, pending_approval_count, pending_user_input_count,
        has_actionable_proposed_plan, created_at, updated_at, deleted_at
      )
      VALUES ('thread-w', 'project-w', 'Windowed thread',
        '{"provider":"codex","model":"gpt-5-codex"}', 'full-access', 'default',
        'turn-5', 0, 0, 0, '2026-03-01T00:00:00.000Z', '2026-03-01T00:00:10.000Z', NULL)
    `;

    const turns: ReadonlyArray<{
      turn: string;
      pendingMessage: string | null;
      at: string;
    }> = [
      { turn: "turn-1", pendingMessage: "user-msg-1", at: "2026-03-01T00:00:00.000Z" },
      { turn: "turn-2", pendingMessage: null, at: "2026-03-01T00:01:00.000Z" },
      { turn: "turn-3", pendingMessage: null, at: "2026-03-01T00:02:00.000Z" },
      { turn: "turn-4", pendingMessage: "user-msg-4", at: "2026-03-01T00:03:00.000Z" },
      { turn: "turn-5", pendingMessage: "user-msg-5", at: "2026-03-01T00:04:00.000Z" },
    ];
    for (const { turn, pendingMessage, at } of turns) {
      yield* sql`
        INSERT INTO projection_turns (
          thread_id, turn_id, pending_message_id, state, requested_at, started_at, completed_at,
          checkpoint_files_json
        )
        VALUES ('thread-w', ${turn}, ${pendingMessage}, 'completed', ${at}, ${at}, ${at}, '[]')
      `;
      if (pendingMessage !== null) {
        yield* sql`
          INSERT INTO projection_thread_messages (
            message_id, thread_id, turn_id, role, text, is_streaming, created_at, updated_at
          )
          VALUES (${pendingMessage}, 'thread-w', NULL, 'user', ${"prompt for " + turn}, 0, ${at}, ${at})
        `;
      }
      yield* sql`
        INSERT INTO projection_thread_messages (
          message_id, thread_id, turn_id, role, text, is_streaming, created_at, updated_at
        )
        VALUES (${turn + "-reply"}, 'thread-w', ${turn}, 'assistant', ${"reply from " + turn}, 0, ${at}, ${at})
      `;
      yield* sql`
        INSERT INTO projection_thread_activities (
          activity_id, thread_id, turn_id, tone, kind, summary, payload_json, created_at
        )
        VALUES (${turn + "-activity"}, 'thread-w', ${turn}, 'tool', 'tool.completed',
          'ran tool', '{"ok":true}', ${at})
      `;
    }

    // Straggler user message sent while turn-4 ran: turn_id NULL and not any
    // turn's pending_message_id.
    yield* sql`
      INSERT INTO projection_thread_messages (
        message_id, thread_id, turn_id, role, text, is_streaming, created_at, updated_at
      )
      VALUES ('user-msg-straggler', 'thread-w', NULL, 'user', 'while you are at it',
        0, '2026-03-01T00:03:30.000Z', '2026-03-01T00:03:30.000Z')
    `;
    // Turnless activity in the same time range.
    yield* sql`
      INSERT INTO projection_thread_activities (
        activity_id, thread_id, turn_id, tone, kind, summary, payload_json, created_at
      )
      VALUES ('turnless-activity', 'thread-w', NULL, 'info', 'context-window.updated',
        'usage', '{"usedTokens":1}', '2026-03-01T00:03:36.000Z')
    `;

    for (const projector of Object.values(ORCHESTRATION_PROJECTOR_NAMES)) {
      yield* sql`
        INSERT INTO projection_state (projector, last_applied_sequence, updated_at)
        VALUES (${projector}, 42, '2026-03-01T00:00:10.000Z')
      `;
    }
  });

  const threadW = ThreadId.make("thread-w");
  const messageIds = (snapshot: { thread: { messages: ReadonlyArray<{ id: string }> } }) =>
    snapshot.thread.messages.map((message) => message.id).toSorted();
  const activityIds = (snapshot: { thread: { activities: ReadonlyArray<{ id: string }> } }) =>
    snapshot.thread.activities.map((activity) => activity.id).toSorted();

  it.effect("returns the full thread with no page metadata when no window is requested", () =>
    Effect.gen(function* () {
      yield* seedFanOutThread();
      const snapshotQuery = yield* ProjectionSnapshotQuery;

      const snapshot = yield* snapshotQuery.getThreadDetailSnapshot(threadW);
      assert.equal(snapshot._tag, "Some");
      if (snapshot._tag === "Some") {
        assert.equal(snapshot.value.page, undefined);
        assert.equal(snapshot.value.thread.messages.length, 9);
        assert.equal(snapshot.value.thread.activities.length, 6);
        assert.equal(snapshot.value.snapshotSequence, 42);
      }
    }),
  );

  it.effect("windows to the last N user-anchored turns with subagent turns riding along", () =>
    Effect.gen(function* () {
      yield* seedFanOutThread();
      const snapshotQuery = yield* ProjectionSnapshotQuery;

      // turnLimit 2 walks back: turn-5 (user), turn-4 (user) -> window is
      // rows 4..5. Subagent turns 2-3 are older than the 2nd user turn and
      // stay out; the straggler message and turnless activity (T03.5/T03.6,
      // after turn-4's anchor) ride along.
      const snapshot = yield* snapshotQuery.getThreadDetailSnapshot(threadW, { turnLimit: 2 });
      assert.equal(snapshot._tag, "Some");
      if (snapshot._tag === "Some") {
        assert.deepEqual(messageIds(snapshot.value), [
          "turn-4-reply",
          "turn-5-reply",
          "user-msg-4",
          "user-msg-5",
          "user-msg-straggler",
        ]);
        assert.deepEqual(activityIds(snapshot.value), [
          "turn-4-activity",
          "turn-5-activity",
          "turnless-activity",
        ]);
        assert.equal(snapshot.value.page?.hasMore, true);
        assert.notEqual(snapshot.value.page?.beforeCursor, null);
        assert.equal(snapshot.value.page?.snapshotSequence, 42);
      }
    }),
  );

  it.effect("subagent turns between user turns ride along inside the window", () =>
    Effect.gen(function* () {
      yield* seedFanOutThread();
      const snapshotQuery = yield* ProjectionSnapshotQuery;

      // turnLimit 3 reaches user turn-1, dragging subagent turns 2-3 along:
      // the full thread, so no further pages.
      const snapshot = yield* snapshotQuery.getThreadDetailSnapshot(threadW, { turnLimit: 3 });
      assert.equal(snapshot._tag, "Some");
      if (snapshot._tag === "Some") {
        assert.equal(snapshot.value.thread.messages.length, 9);
        assert.equal(snapshot.value.thread.activities.length, 6);
        assert.equal(snapshot.value.page?.hasMore, false);
        assert.equal(snapshot.value.page?.beforeCursor, null);
      }
    }),
  );

  it.effect("cursors survive a projection rewrite that reassigns turn row ids", () =>
    Effect.gen(function* () {
      // The revert projector (and any projection rebuild) deletes and
      // re-upserts projection_turns, assigning fresh autoincrement row ids.
      // The keyset cursor is derived from event content, so a page cursor
      // minted before the rewrite must keep working after it.
      yield* seedFanOutThread();
      const sql = yield* SqlClient.SqlClient;
      const snapshotQuery = yield* ProjectionSnapshotQuery;

      const firstPage = yield* snapshotQuery.getThreadDetailSnapshot(threadW, { turnLimit: 2 });
      assert.equal(firstPage._tag, "Some");
      if (firstPage._tag !== "Some") return;
      const cursor = firstPage.value.page?.beforeCursor;
      assert.notEqual(cursor, null);
      if (cursor === null || cursor === undefined) return;

      // Simulate the rewrite: delete and re-insert every turn row with the
      // same content, which reassigns all row ids.
      const turnRows = yield* sql`
        SELECT thread_id, turn_id, pending_message_id, state, requested_at, started_at,
          completed_at, checkpoint_files_json
        FROM projection_turns WHERE thread_id = 'thread-w' ORDER BY row_id
      `;
      yield* sql`DELETE FROM projection_turns WHERE thread_id = 'thread-w'`;
      for (const row of turnRows) {
        yield* sql`
          INSERT INTO projection_turns (
            thread_id, turn_id, pending_message_id, state, requested_at, started_at,
            completed_at, checkpoint_files_json
          )
          VALUES (${row.thread_id as string}, ${row.turn_id as string},
            ${row.pending_message_id as string | null}, ${row.state as string},
            ${row.requested_at as string}, ${row.started_at as string},
            ${row.completed_at as string}, ${row.checkpoint_files_json as string})
        `;
      }

      const olderPage = yield* snapshotQuery.getThreadDetailSnapshot(threadW, {
        turnLimit: 1,
        beforeCursor: cursor,
      });
      assert.equal(olderPage._tag, "Some");
      if (olderPage._tag === "Some") {
        // Identical older slice to what the pre-rewrite cursor would return.
        assert.deepEqual(messageIds(olderPage.value), [
          "turn-1-reply",
          "turn-2-reply",
          "turn-3-reply",
          "user-msg-1",
        ]);
        assert.equal(olderPage.value.page?.hasMore, false);
      }
    }),
  );

  it.effect("beforeCursor returns the disjoint adjacent older slice", () =>
    Effect.gen(function* () {
      yield* seedFanOutThread();
      const snapshotQuery = yield* ProjectionSnapshotQuery;

      const firstPage = yield* snapshotQuery.getThreadDetailSnapshot(threadW, { turnLimit: 2 });
      assert.equal(firstPage._tag, "Some");
      if (firstPage._tag !== "Some") return;
      const cursor = firstPage.value.page?.beforeCursor;
      assert.notEqual(cursor, null);
      assert.notEqual(cursor, undefined);
      if (cursor === null || cursor === undefined) return;

      // Older page: user turn-1 plus subagent turns 2-3 riding along. Disjoint
      // from the first page: no turn-4/5 rows, no straggler.
      const olderPage = yield* snapshotQuery.getThreadDetailSnapshot(threadW, {
        turnLimit: 1,
        beforeCursor: cursor,
      });
      assert.equal(olderPage._tag, "Some");
      if (olderPage._tag === "Some") {
        assert.deepEqual(messageIds(olderPage.value), [
          "turn-1-reply",
          "turn-2-reply",
          "turn-3-reply",
          "user-msg-1",
        ]);
        assert.deepEqual(activityIds(olderPage.value), [
          "turn-1-activity",
          "turn-2-activity",
          "turn-3-activity",
        ]);
        assert.equal(olderPage.value.page?.hasMore, false);
        assert.equal(olderPage.value.page?.beforeCursor, null);
      }
    }),
  );

  it.effect("a cursor for a different thread degrades to the first page", () =>
    Effect.gen(function* () {
      yield* seedFanOutThread();
      const snapshotQuery = yield* ProjectionSnapshotQuery;

      const firstPage = yield* snapshotQuery.getThreadDetailSnapshot(threadW, { turnLimit: 2 });
      assert.equal(firstPage._tag, "Some");
      if (firstPage._tag !== "Some") return;

      const foreign = encodeThreadDetailPageCursor({
        threadId: ThreadId.make("thread-other"),
        beforeAnchorAt: "2026-03-01T00:01:00.000Z",
        beforeTurnId: "turn-2",
      });
      const snapshot = yield* snapshotQuery.getThreadDetailSnapshot(threadW, {
        turnLimit: 2,
        beforeCursor: foreign,
      });
      assert.equal(snapshot._tag, "Some");
      if (snapshot._tag === "Some") {
        assert.deepEqual(messageIds(snapshot.value), messageIds(firstPage.value));
      }
    }),
  );

  it.effect("a malformed cursor degrades to the first page instead of failing", () =>
    Effect.gen(function* () {
      yield* seedFanOutThread();
      const snapshotQuery = yield* ProjectionSnapshotQuery;

      const snapshot = yield* snapshotQuery.getThreadDetailSnapshot(threadW, {
        turnLimit: 2,
        beforeCursor: "not-a-cursor",
      });
      assert.equal(snapshot._tag, "Some");
      if (snapshot._tag === "Some") {
        assert.equal(snapshot.value.page?.hasMore, true);
        assert.equal(snapshot.value.thread.messages.length, 5);
      }
    }),
  );

  it.effect("windows never split below the raw-turn ceiling boundary contiguously", () =>
    Effect.gen(function* () {
      yield* seedFanOutThread();
      const snapshotQuery = yield* ProjectionSnapshotQuery;

      // Page repeatedly with turnLimit 1 and assert the union of all pages is
      // exactly the full thread with no duplicates (disjointness + coverage).
      const seenMessages: string[] = [];
      const seenActivities: string[] = [];
      let cursor: string | undefined;
      for (let page = 0; page < 10; page += 1) {
        const snapshot = yield* snapshotQuery.getThreadDetailSnapshot(threadW, {
          turnLimit: 1,
          ...(cursor !== undefined ? { beforeCursor: cursor } : {}),
        });
        assert.equal(snapshot._tag, "Some");
        if (snapshot._tag !== "Some") return;
        seenMessages.push(...snapshot.value.thread.messages.map((message) => message.id));
        seenActivities.push(...snapshot.value.thread.activities.map((activity) => activity.id));
        const next = snapshot.value.page?.beforeCursor;
        if (next === null || next === undefined) break;
        cursor = next;
      }
      assert.equal(new Set(seenMessages).size, seenMessages.length);
      assert.equal(new Set(seenActivities).size, seenActivities.length);
      assert.equal(seenMessages.length, 9);
      assert.equal(seenActivities.length, 6);
    }),
  );

  it.effect("bounds activity hydration and preserves unresolved requests", () =>
    Effect.gen(function* () {
      yield* seedFanOutThread();
      const snapshotQuery = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;

      yield* sql`DELETE FROM projection_thread_activities`;
      yield* sql`
        WITH RECURSIVE activity_rows(sequence) AS (
          SELECT 1
          UNION ALL
          SELECT sequence + 1 FROM activity_rows WHERE sequence < 501
        )
        INSERT INTO projection_thread_activities (
          activity_id, thread_id, turn_id, tone, kind, summary, payload_json, sequence, created_at
        )
        SELECT
          printf('activity-%04d', sequence),
          'thread-w',
          'turn-5',
          'tool',
          'tool.completed',
          'ran tool',
          printf('{"sequence":%d}', sequence),
          sequence,
          '2026-03-01T00:04:00.000Z'
        FROM activity_rows
      `;

      const fullDetail = yield* snapshotQuery.getThreadDetailById(threadW);
      assert.equal(fullDetail._tag, "Some");
      if (fullDetail._tag === "Some") {
        assert.equal(fullDetail.value.activities.length, 500);
        assert.equal(fullDetail.value.activities[0]?.id, asEventId("activity-0002"));
        assert.equal(fullDetail.value.activities.at(-1)?.id, asEventId("activity-0501"));
      }

      const windowedDetail = yield* snapshotQuery.getThreadDetailSnapshot(threadW, {
        turnLimit: 2,
      });
      assert.equal(windowedDetail._tag, "Some");
      if (windowedDetail._tag === "Some") {
        assert.equal(windowedDetail.value.thread.activities.length, 500);
        assert.equal(windowedDetail.value.thread.activities[0]?.id, asEventId("activity-0002"));
        assert.equal(windowedDetail.value.thread.activities.at(-1)?.id, asEventId("activity-0501"));
      }

      yield* sql`
        INSERT INTO projection_thread_activities (
          activity_id, thread_id, turn_id, tone, kind, summary, payload_json, sequence, created_at
        )
        VALUES
          (
            'approval-old', 'thread-w', NULL, 'approval', 'approval.requested',
            'Approve old command', '{"requestId":"approval-1"}', NULL,
            '2026-03-01T00:00:01.000Z'
          ),
          (
            'user-input-old', 'thread-w', NULL, 'approval', 'user-input.requested',
            'Answer old question', '{"requestId":"input-1"}', NULL,
            '2026-03-01T00:00:02.000Z'
          ),
          (
            'user-input-closed', 'thread-w', NULL, 'approval', 'user-input.requested',
            'Closed question', '{"requestId":"input-closed"}', NULL,
            '2026-03-01T00:00:03.000Z'
          ),
          (
            'user-input-closed-resolution', 'thread-w', NULL, 'info', 'user-input.resolved',
            'Closed question', '{"requestId":"input-closed"}', NULL,
            '2026-03-01T00:00:04.000Z'
          ),
          (
            'user-input-tied-z-request', 'thread-w', NULL, 'approval', 'user-input.requested',
            'Tied open question', '{"requestId":"input-tied-open"}', NULL,
            '2026-03-01T00:00:05.000Z'
          ),
          (
            'user-input-tied-a-resolution', 'thread-w', NULL, 'info', 'user-input.resolved',
            'Tied open question', '{"requestId":"input-tied-open"}', NULL,
            '2026-03-01T00:00:05.000Z'
          )
      `;
      yield* sql`
        INSERT INTO projection_pending_approvals (
          request_id, thread_id, turn_id, status, decision, created_at, resolved_at
        )
        VALUES (
          'approval-1', 'thread-w', NULL, 'pending', NULL,
          '2026-03-01T00:00:01.000Z', NULL
        )
      `;
      yield* sql`
        UPDATE projection_threads
        SET pending_approval_count = 1, pending_user_input_count = 1
        WHERE thread_id = 'thread-w'
      `;

      const detailWithPinnedRequests = yield* snapshotQuery.getThreadDetailById(threadW);
      assert.equal(detailWithPinnedRequests._tag, "Some");
      if (detailWithPinnedRequests._tag === "Some") {
        const ids = detailWithPinnedRequests.value.activities.map((activity) => activity.id);
        assert.equal(detailWithPinnedRequests.value.activities.length, 503);
        assert.equal(ids.includes(asEventId("approval-old")), true);
        assert.equal(ids.includes(asEventId("user-input-old")), true);
        assert.equal(ids.includes(asEventId("user-input-closed")), false);
        assert.equal(ids.includes(asEventId("user-input-tied-z-request")), true);
      }

      const windowWithPinnedRequests = yield* snapshotQuery.getThreadDetailSnapshot(threadW, {
        turnLimit: 2,
      });
      assert.equal(windowWithPinnedRequests._tag, "Some");
      if (windowWithPinnedRequests._tag === "Some") {
        const ids = windowWithPinnedRequests.value.thread.activities.map((activity) => activity.id);
        assert.equal(windowWithPinnedRequests.value.thread.activities.length, 503);
        assert.equal(ids.includes(asEventId("approval-old")), true);
        assert.equal(ids.includes(asEventId("user-input-old")), true);
        assert.equal(ids.includes(asEventId("user-input-closed")), false);
        assert.equal(ids.includes(asEventId("user-input-tied-z-request")), true);
      }
    }),
  );

  it.effect("a thread with no turns returns its content unwindowed on the first page", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const snapshotQuery = yield* ProjectionSnapshotQuery;

      yield* sql`DELETE FROM projection_projects`;
      yield* sql`DELETE FROM projection_threads`;
      yield* sql`DELETE FROM projection_turns`;
      yield* sql`DELETE FROM projection_thread_messages`;
      yield* sql`DELETE FROM projection_thread_activities`;
      yield* sql`DELETE FROM projection_state`;

      yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, scripts_json, created_at, updated_at, deleted_at
        )
        VALUES ('project-e', 'Empty', '/tmp/project-e', '[]',
          '2026-03-02T00:00:00.000Z', '2026-03-02T00:00:00.000Z', NULL)
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode,
          pending_approval_count, pending_user_input_count, has_actionable_proposed_plan,
          created_at, updated_at, deleted_at
        )
        VALUES ('thread-e', 'project-e', 'Turnless thread',
          '{"provider":"codex","model":"gpt-5-codex"}', 'full-access', 'default',
          0, 0, 0, '2026-03-02T00:00:00.000Z', '2026-03-02T00:00:00.000Z', NULL)
      `;
      yield* sql`
        INSERT INTO projection_thread_messages (
          message_id, thread_id, turn_id, role, text, is_streaming, created_at, updated_at
        )
        VALUES ('pre-turn-msg', 'thread-e', NULL, 'user', 'first prompt', 0,
          '2026-03-02T00:00:01.000Z', '2026-03-02T00:00:01.000Z')
      `;
      for (const projector of Object.values(ORCHESTRATION_PROJECTOR_NAMES)) {
        yield* sql`
          INSERT INTO projection_state (projector, last_applied_sequence, updated_at)
          VALUES (${projector}, 7, '2026-03-02T00:00:01.000Z')
        `;
      }

      const snapshot = yield* snapshotQuery.getThreadDetailSnapshot(ThreadId.make("thread-e"), {
        turnLimit: 5,
      });
      assert.equal(snapshot._tag, "Some");
      if (snapshot._tag === "Some") {
        assert.deepEqual(messageIds(snapshot.value), ["pre-turn-msg"]);
        assert.equal(snapshot.value.page?.hasMore, false);
        assert.equal(snapshot.value.page?.beforeCursor, null);
      }
    }),
  );
});
