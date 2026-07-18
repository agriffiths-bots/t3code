import {
  AuthAudienceCeiling,
  ChatAttachment,
  CheckpointRef,
  DataAudience,
  IsoDateTime,
  MessageId,
  NonNegativeInt,
  OrchestrationCheckpointFile,
  OrchestrationProposedPlanId,
  OrchestrationReadModel,
  OrchestrationShellSnapshot,
  OrchestrationThread,
  OrchestrationThreadDetailSnapshot,
  ProjectScript,
  TurnId,
  type OrchestrationCheckpointSummary,
  type OrchestrationLatestTurn,
  type OrchestrationProjectShell,
  type OrchestrationProposedPlan,
  type OrchestrationProject,
  type OrchestrationSession,
  type OrchestrationThreadShell,
  ModelSelection,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as Arr from "effect/Array";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import {
  isPersistenceError,
  toPersistenceDecodeError,
  toPersistenceSqlError,
  type ProjectionRepositoryError,
} from "../../persistence/Errors.ts";
import { canReadDataAudience, currentReadAudienceCeiling } from "../../auth/audienceDataPolicy.ts";
import { MAX_THREAD_CHECKPOINTS } from "../checkpointRetention.ts";
import { ProjectionCheckpoint } from "../../persistence/Services/ProjectionCheckpoints.ts";
import { ProjectionProject } from "../../persistence/Services/ProjectionProjects.ts";
import { ProjectionState } from "../../persistence/Services/ProjectionState.ts";
import { ProjectionThreadActivity } from "../../persistence/Services/ProjectionThreadActivities.ts";
import { ProjectionThreadMessage } from "../../persistence/Services/ProjectionThreadMessages.ts";
import { ProjectionThreadProposedPlan } from "../../persistence/Services/ProjectionThreadProposedPlans.ts";
import { ProjectionThreadSession } from "../../persistence/Services/ProjectionThreadSessions.ts";
import { ProjectionThread } from "../../persistence/Services/ProjectionThreads.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { ORCHESTRATION_PROJECTOR_NAMES } from "./ProjectionPipeline.ts";
import {
  ProjectionSnapshotQuery,
  type ProjectionFullThreadDiffContext,
  type ProjectionSnapshotCounts,
  type ProjectionThreadCheckpointContext,
  type ProjectionSnapshotQueryShape,
} from "../Services/ProjectionSnapshotQuery.ts";

const CHECKPOINT_DIFF_CONTEXT_KEEP_PER_THREAD = MAX_THREAD_CHECKPOINTS + 1;

const decodeReadModel = Schema.decodeUnknownEffect(OrchestrationReadModel);
const decodeShellSnapshot = Schema.decodeUnknownEffect(OrchestrationShellSnapshot);
const decodeThread = Schema.decodeUnknownEffect(OrchestrationThread);
const isThreadId = Schema.is(ThreadId);
const ProjectionProjectDbRowSchema = ProjectionProject.mapFields(
  Struct.assign({
    defaultModelSelection: Schema.NullOr(Schema.fromJsonString(ModelSelection)),
    scripts: Schema.fromJsonString(Schema.Array(ProjectScript)),
  }),
);
const ProjectionThreadMessageDbRowSchema = ProjectionThreadMessage.mapFields(
  Struct.assign({
    isStreaming: Schema.Number,
    attachments: Schema.NullOr(Schema.fromJsonString(Schema.Array(ChatAttachment))),
  }),
);
const ProjectionThreadProposedPlanDbRowSchema = ProjectionThreadProposedPlan;
const ProjectionThreadDbRowSchema = ProjectionThread.mapFields(
  Struct.assign({
    modelSelection: Schema.fromJsonString(ModelSelection),
    dataAudience: DataAudience,
  }),
);
const ProjectionThreadActivityDbRowSchema = ProjectionThreadActivity.mapFields(
  Struct.assign({
    payload: Schema.fromJsonString(Schema.Unknown),
    sequence: Schema.NullOr(NonNegativeInt),
  }),
);
const ProjectionThreadSessionDbRowSchema = ProjectionThreadSession;
const ProjectionCheckpointDbRowSchema = ProjectionCheckpoint.mapFields(
  Struct.assign({
    files: Schema.fromJsonString(Schema.Array(OrchestrationCheckpointFile)),
  }),
);
const ProjectionLatestTurnDbRowSchema = Schema.Struct({
  threadId: ProjectionThread.fields.threadId,
  turnId: TurnId,
  state: Schema.String,
  requestedAt: IsoDateTime,
  startedAt: Schema.NullOr(IsoDateTime),
  completedAt: Schema.NullOr(IsoDateTime),
  assistantMessageId: Schema.NullOr(MessageId),
  effectiveModel: Schema.NullOr(Schema.String),
  sourceProposedPlanThreadId: Schema.NullOr(ThreadId),
  sourceProposedPlanId: Schema.NullOr(OrchestrationProposedPlanId),
});
const ProjectionStateDbRowSchema = ProjectionState;
const ProjectionCountsRowSchema = Schema.Struct({
  projectCount: Schema.Number,
  threadCount: Schema.Number,
});
const AudienceCeilingInput = Schema.Struct({
  audienceCeiling: AuthAudienceCeiling,
});
const WorkspaceRootLookupInput = Schema.Struct({
  workspaceRoot: Schema.String,
  audienceCeiling: AuthAudienceCeiling,
});
const ProjectIdLookupInput = Schema.Struct({
  projectId: ProjectId,
  audienceCeiling: AuthAudienceCeiling,
});
const ThreadIdLookupInput = Schema.Struct({
  threadId: ThreadId,
});
const AudienceThreadIdLookupInput = Schema.Struct({
  threadId: ThreadId,
  audienceCeiling: AuthAudienceCeiling,
});
const ProjectionProjectLookupRowSchema = ProjectionProjectDbRowSchema;
const ProjectionThreadIdLookupRowSchema = Schema.Struct({
  threadId: ThreadId,
  dataAudience: DataAudience,
});
const ProjectionThreadCheckpointContextThreadRowSchema = Schema.Struct({
  threadId: ThreadId,
  projectId: ProjectId,
  workspaceRoot: Schema.String,
  worktreePath: Schema.NullOr(Schema.String),
  dataAudience: DataAudience,
});
const ProjectionCheckpointTurnIdRowSchema = Schema.Struct({
  turnId: TurnId,
});
const CheckpointTurnIdLookupInput = Schema.Struct({
  threadId: ThreadId,
  turnId: TurnId,
});
const FullThreadDiffContextLookupInput = Schema.Struct({
  threadId: ThreadId,
  checkpointTurnCount: NonNegativeInt,
  audienceCeiling: AuthAudienceCeiling,
});
const ProjectionFullThreadDiffContextRowSchema = Schema.Struct({
  threadId: ThreadId,
  projectId: ProjectId,
  workspaceRoot: Schema.String,
  worktreePath: Schema.NullOr(Schema.String),
  latestCheckpointTurnCount: Schema.NullOr(NonNegativeInt),
  toCheckpointRef: Schema.NullOr(CheckpointRef),
  dataAudience: DataAudience,
});

const REQUIRED_SNAPSHOT_PROJECTORS = [
  ORCHESTRATION_PROJECTOR_NAMES.projects,
  ORCHESTRATION_PROJECTOR_NAMES.threads,
  ORCHESTRATION_PROJECTOR_NAMES.threadMessages,
  ORCHESTRATION_PROJECTOR_NAMES.threadProposedPlans,
  ORCHESTRATION_PROJECTOR_NAMES.threadActivities,
  ORCHESTRATION_PROJECTOR_NAMES.threadSessions,
  ORCHESTRATION_PROJECTOR_NAMES.threadTurns,
  ORCHESTRATION_PROJECTOR_NAMES.checkpoints,
] as const;

function maxIso(left: string | null, right: string): string {
  if (left === null) {
    return right;
  }
  return left > right ? left : right;
}

function scopeProjectAndThreadRows(input: {
  readonly audienceCeiling: AuthAudienceCeiling;
  readonly projectRows: ReadonlyArray<Schema.Schema.Type<typeof ProjectionProjectDbRowSchema>>;
  readonly threadRows: ReadonlyArray<Schema.Schema.Type<typeof ProjectionThreadDbRowSchema>>;
  readonly sessionRows: ReadonlyArray<
    Schema.Schema.Type<typeof ProjectionThreadSessionDbRowSchema>
  >;
  readonly latestTurnRows: ReadonlyArray<
    Schema.Schema.Type<typeof ProjectionLatestTurnDbRowSchema>
  >;
}) {
  const projectRows = input.projectRows.filter((row) =>
    canReadDataAudience(input.audienceCeiling, row.dataAudience),
  );
  const threadRows = input.threadRows.filter((row) =>
    canReadDataAudience(input.audienceCeiling, row.dataAudience),
  );
  const visibleThreadIds = new Set(threadRows.map((row) => row.threadId));
  return {
    projectRows,
    threadRows,
    sessionRows: input.sessionRows.filter((row) => visibleThreadIds.has(row.threadId)),
    latestTurnRows: input.latestTurnRows.filter((row) => visibleThreadIds.has(row.threadId)),
    readableThreadIds: input.audienceCeiling === "private" ? undefined : visibleThreadIds,
  };
}

type ReadableThreadIds = ReadonlySet<ThreadId> | undefined;

function readableThreadIdsForAudience(
  audienceCeiling: AuthAudienceCeiling,
  rows: ReadonlyArray<Schema.Schema.Type<typeof ProjectionThreadIdLookupRowSchema>>,
): ReadableThreadIds {
  if (audienceCeiling === "private") {
    return undefined;
  }
  return new Set(
    rows
      .filter((row) => canReadDataAudience(audienceCeiling, row.dataAudience))
      .map((row) => row.threadId),
  );
}

function canExposeThreadReference(
  threadId: ThreadId,
  readableThreadIds: ReadableThreadIds,
): boolean {
  return readableThreadIds === undefined || readableThreadIds.has(threadId);
}

function mapParentThreadReference(
  row: Pick<
    Schema.Schema.Type<typeof ProjectionThreadDbRowSchema>,
    "parentThreadId" | "parentEnvironmentId"
  >,
  readableThreadIds: ReadableThreadIds,
) {
  if (
    row.parentThreadId === null ||
    canExposeThreadReference(row.parentThreadId, readableThreadIds)
  ) {
    return {
      parentThreadId: row.parentThreadId,
      parentEnvironmentId: row.parentEnvironmentId ?? null,
    };
  }
  return { parentThreadId: null, parentEnvironmentId: null };
}

function computeSnapshotSequence(
  stateRows: ReadonlyArray<Schema.Schema.Type<typeof ProjectionStateDbRowSchema>>,
): number {
  if (stateRows.length === 0) {
    return 0;
  }
  const sequenceByProjector = new Map(
    stateRows.map((row) => [row.projector, row.lastAppliedSequence] as const),
  );

  let minSequence = Number.POSITIVE_INFINITY;
  for (const projector of REQUIRED_SNAPSHOT_PROJECTORS) {
    const sequence = sequenceByProjector.get(projector);
    if (sequence === undefined) {
      return 0;
    }
    if (sequence < minSequence) {
      minSequence = sequence;
    }
  }

  return Number.isFinite(minSequence) ? minSequence : 0;
}

function mapLatestTurn(
  row: Schema.Schema.Type<typeof ProjectionLatestTurnDbRowSchema>,
  readableThreadIds?: ReadableThreadIds,
): OrchestrationLatestTurn {
  return {
    turnId: row.turnId,
    state:
      row.state === "error"
        ? "error"
        : row.state === "interrupted"
          ? "interrupted"
          : row.state === "completed"
            ? "completed"
            : "running",
    requestedAt: row.requestedAt,
    startedAt: row.startedAt,
    completedAt: row.completedAt,
    assistantMessageId: row.assistantMessageId,
    ...(row.effectiveModel !== null ? { effectiveModel: row.effectiveModel } : {}),
    ...(row.sourceProposedPlanThreadId !== null &&
    row.sourceProposedPlanId !== null &&
    canExposeThreadReference(row.sourceProposedPlanThreadId, readableThreadIds)
      ? {
          sourceProposedPlan: {
            threadId: row.sourceProposedPlanThreadId,
            planId: row.sourceProposedPlanId,
          },
        }
      : {}),
  };
}

function mapSessionRow(
  row: Schema.Schema.Type<typeof ProjectionThreadSessionDbRowSchema>,
): OrchestrationSession {
  return {
    threadId: row.threadId,
    status: row.status,
    providerName: row.providerName,
    ...(row.providerInstanceId !== null ? { providerInstanceId: row.providerInstanceId } : {}),
    runtimeMode: row.runtimeMode,
    activeTurnId: row.activeTurnId,
    lastError: row.lastError,
    updatedAt: row.updatedAt,
  };
}

function mapProjectShellRow(
  row: Schema.Schema.Type<typeof ProjectionProjectDbRowSchema>,
  repositoryIdentity: OrchestrationProject["repositoryIdentity"],
): OrchestrationProjectShell {
  return {
    id: row.projectId,
    title: row.title,
    workspaceRoot: row.workspaceRoot,
    dataAudience: row.dataAudience,
    repositoryIdentity,
    defaultModelSelection: row.defaultModelSelection,
    scripts: row.scripts,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

function mapProposedPlanRow(
  row: Schema.Schema.Type<typeof ProjectionThreadProposedPlanDbRowSchema>,
  readableThreadIds?: ReadableThreadIds,
): OrchestrationProposedPlan {
  return {
    id: row.planId,
    turnId: row.turnId,
    planMarkdown: row.planMarkdown,
    implementedAt: row.implementedAt,
    implementationThreadId:
      row.implementationThreadId === null ||
      canExposeThreadReference(row.implementationThreadId, readableThreadIds)
        ? row.implementationThreadId
        : null,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

function mapActivityThreadReferences(
  row: Schema.Schema.Type<typeof ProjectionThreadActivityDbRowSchema>,
  readableThreadIds: ReadableThreadIds,
): { readonly summary: string; readonly payload: unknown } {
  const payload = row.payload;
  if (
    readableThreadIds === undefined ||
    row.kind !== "subagent.completed" ||
    typeof payload !== "object" ||
    payload === null ||
    Array.isArray(payload)
  ) {
    return { summary: row.summary, payload };
  }
  const childThreadId = Reflect.get(payload, "childThreadId");
  if (!isThreadId(childThreadId) || readableThreadIds.has(childThreadId)) {
    return { summary: row.summary, payload };
  }
  return {
    summary: row.summary.replaceAll(childThreadId, "[redacted thread]"),
    payload: { ...payload, childThreadId: null },
  };
}

function toPersistenceSqlOrDecodeError(sqlOperation: string, decodeOperation: string) {
  return (cause: unknown): ProjectionRepositoryError =>
    Schema.isSchemaError(cause)
      ? toPersistenceDecodeError(decodeOperation)(cause)
      : toPersistenceSqlError(sqlOperation)(cause);
}

const makeProjectionSnapshotQuery = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const repositoryIdentityResolver = yield* RepositoryIdentityResolver.RepositoryIdentityResolver;
  const repositoryIdentityResolutionConcurrency = 4;
  const resolveRepositoryIdentitiesForProjects = Effect.fn(
    "ProjectionSnapshotQuery.resolveRepositoryIdentitiesForProjects",
  )(function* (
    projectRows: ReadonlyArray<Schema.Schema.Type<typeof ProjectionProjectDbRowSchema>>,
    options?: {
      readonly includeDeleted?: boolean;
    },
  ) {
    const filteredProjectRows =
      options?.includeDeleted === true
        ? projectRows
        : projectRows.filter((row) => row.deletedAt === null);
    const uniqueWorkspaceRoots = [...new Set(filteredProjectRows.map((row) => row.workspaceRoot))];
    const repositoryIdentityByWorkspaceRoot = new Map(
      yield* Effect.forEach(
        uniqueWorkspaceRoots,
        (workspaceRoot) =>
          repositoryIdentityResolver
            .resolve(workspaceRoot)
            .pipe(Effect.map((identity) => [workspaceRoot, identity] as const)),
        { concurrency: repositoryIdentityResolutionConcurrency },
      ),
    );

    return new Map(
      filteredProjectRows.map((row) => [
        row.projectId,
        repositoryIdentityByWorkspaceRoot.get(row.workspaceRoot) ?? null,
      ]),
    );
  });

  const listProjectRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionProjectDbRowSchema,
    execute: () =>
      sql`
        SELECT
          project_id AS "projectId",
          title,
          workspace_root AS "workspaceRoot",
          data_audience AS "dataAudience",
          default_model_selection_json AS "defaultModelSelection",
          scripts_json AS "scripts",
          created_at AS "createdAt",
          updated_at AS "updatedAt",
          deleted_at AS "deletedAt"
        FROM projection_projects
        ORDER BY created_at ASC, project_id ASC
      `,
  });

  const listThreadRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionThreadDbRowSchema,
    execute: () =>
      sql`
        SELECT
          threads.thread_id AS "threadId",
          threads.project_id AS "projectId",
          projects.data_audience AS "dataAudience",
          threads.title,
          threads.model_selection_json AS "modelSelection",
          threads.runtime_mode AS "runtimeMode",
          threads.interaction_mode AS "interactionMode",
          threads.branch,
          threads.worktree_path AS "worktreePath",
          threads.worktree_removable AS "worktreeRemovable",
          threads.worktree_removal_path AS "worktreeRemovalPath",
          threads.latest_turn_id AS "latestTurnId",
          threads.created_at AS "createdAt",
          threads.updated_at AS "updatedAt",
          threads.archived_at AS "archivedAt",
          threads.latest_user_message_at AS "latestUserMessageAt",
          threads.pending_approval_count AS "pendingApprovalCount",
          threads.pending_user_input_count AS "pendingUserInputCount",
          threads.has_actionable_proposed_plan AS "hasActionableProposedPlan",
          threads.deleted_at AS "deletedAt",
          threads.parent_thread_id AS "parentThreadId",
          threads.parent_environment_id AS "parentEnvironmentId"
        FROM projection_threads AS threads
        INNER JOIN projection_projects AS projects
          ON projects.project_id = threads.project_id
        ORDER BY threads.created_at ASC, threads.thread_id ASC
      `,
  });

  const listThreadAudienceRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionThreadIdLookupRowSchema,
    execute: () =>
      sql`
        SELECT
          threads.thread_id AS "threadId",
          projects.data_audience AS "dataAudience"
        FROM projection_threads AS threads
        INNER JOIN projection_projects AS projects
          ON projects.project_id = threads.project_id
        ORDER BY threads.thread_id ASC
      `,
  });

  const listActiveThreadRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionThreadDbRowSchema,
    execute: () =>
      sql`
        SELECT
          threads.thread_id AS "threadId",
          threads.project_id AS "projectId",
          projects.data_audience AS "dataAudience",
          threads.title,
          threads.model_selection_json AS "modelSelection",
          threads.runtime_mode AS "runtimeMode",
          threads.interaction_mode AS "interactionMode",
          threads.branch,
          threads.worktree_path AS "worktreePath",
          threads.worktree_removable AS "worktreeRemovable",
          threads.worktree_removal_path AS "worktreeRemovalPath",
          threads.latest_turn_id AS "latestTurnId",
          threads.created_at AS "createdAt",
          threads.updated_at AS "updatedAt",
          threads.archived_at AS "archivedAt",
          threads.latest_user_message_at AS "latestUserMessageAt",
          threads.pending_approval_count AS "pendingApprovalCount",
          threads.pending_user_input_count AS "pendingUserInputCount",
          threads.has_actionable_proposed_plan AS "hasActionableProposedPlan",
          threads.deleted_at AS "deletedAt",
          threads.parent_thread_id AS "parentThreadId",
          threads.parent_environment_id AS "parentEnvironmentId"
        FROM projection_threads AS threads
        INNER JOIN projection_projects AS projects
          ON projects.project_id = threads.project_id
        WHERE threads.deleted_at IS NULL
          AND threads.archived_at IS NULL
        ORDER BY threads.project_id ASC, threads.created_at ASC, threads.thread_id ASC
      `,
  });

  const listArchivedThreadRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionThreadDbRowSchema,
    execute: () =>
      sql`
        SELECT
          threads.thread_id AS "threadId",
          threads.project_id AS "projectId",
          projects.data_audience AS "dataAudience",
          threads.title,
          threads.model_selection_json AS "modelSelection",
          threads.runtime_mode AS "runtimeMode",
          threads.interaction_mode AS "interactionMode",
          threads.branch,
          threads.worktree_path AS "worktreePath",
          threads.worktree_removable AS "worktreeRemovable",
          threads.worktree_removal_path AS "worktreeRemovalPath",
          threads.latest_turn_id AS "latestTurnId",
          threads.created_at AS "createdAt",
          threads.updated_at AS "updatedAt",
          threads.archived_at AS "archivedAt",
          threads.latest_user_message_at AS "latestUserMessageAt",
          threads.pending_approval_count AS "pendingApprovalCount",
          threads.pending_user_input_count AS "pendingUserInputCount",
          threads.has_actionable_proposed_plan AS "hasActionableProposedPlan",
          threads.deleted_at AS "deletedAt",
          threads.parent_thread_id AS "parentThreadId",
          threads.parent_environment_id AS "parentEnvironmentId"
        FROM projection_threads AS threads
        INNER JOIN projection_projects AS projects
          ON projects.project_id = threads.project_id
        WHERE threads.deleted_at IS NULL
          AND threads.archived_at IS NOT NULL
        ORDER BY threads.project_id ASC, threads.archived_at DESC, threads.thread_id DESC
      `,
  });

  const listThreadProposedPlanRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionThreadProposedPlanDbRowSchema,
    execute: () =>
      sql`
        SELECT
          plan_id AS "planId",
          thread_id AS "threadId",
          turn_id AS "turnId",
          plan_markdown AS "planMarkdown",
          implemented_at AS "implementedAt",
          implementation_thread_id AS "implementationThreadId",
          created_at AS "createdAt",
          updated_at AS "updatedAt"
        FROM projection_thread_proposed_plans
        ORDER BY thread_id ASC, created_at ASC, plan_id ASC
      `,
  });

  const listThreadSessionRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionThreadSessionDbRowSchema,
    execute: () =>
      sql`
        SELECT
          thread_id AS "threadId",
          status,
          provider_name AS "providerName",
          provider_instance_id AS "providerInstanceId",
          provider_session_id AS "providerSessionId",
          provider_thread_id AS "providerThreadId",
          runtime_mode AS "runtimeMode",
          active_turn_id AS "activeTurnId",
          last_error AS "lastError",
          updated_at AS "updatedAt"
        FROM projection_thread_sessions
        ORDER BY thread_id ASC
      `,
  });

  const listActiveThreadSessionRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionThreadSessionDbRowSchema,
    execute: () =>
      sql`
        SELECT
          sessions.thread_id AS "threadId",
          sessions.status,
          sessions.provider_name AS "providerName",
          sessions.provider_instance_id AS "providerInstanceId",
          sessions.provider_session_id AS "providerSessionId",
          sessions.provider_thread_id AS "providerThreadId",
          sessions.runtime_mode AS "runtimeMode",
          sessions.active_turn_id AS "activeTurnId",
          sessions.last_error AS "lastError",
          sessions.updated_at AS "updatedAt"
        FROM projection_thread_sessions sessions
        INNER JOIN projection_threads threads
          ON threads.thread_id = sessions.thread_id
        WHERE threads.deleted_at IS NULL
          AND threads.archived_at IS NULL
        ORDER BY sessions.thread_id ASC
      `,
  });

  const listArchivedThreadSessionRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionThreadSessionDbRowSchema,
    execute: () =>
      sql`
        SELECT
          sessions.thread_id AS "threadId",
          sessions.status,
          sessions.provider_name AS "providerName",
          sessions.provider_instance_id AS "providerInstanceId",
          sessions.provider_session_id AS "providerSessionId",
          sessions.provider_thread_id AS "providerThreadId",
          sessions.runtime_mode AS "runtimeMode",
          sessions.active_turn_id AS "activeTurnId",
          sessions.last_error AS "lastError",
          sessions.updated_at AS "updatedAt"
        FROM projection_thread_sessions sessions
        INNER JOIN projection_threads threads
          ON threads.thread_id = sessions.thread_id
        WHERE threads.deleted_at IS NULL
          AND threads.archived_at IS NOT NULL
        ORDER BY sessions.thread_id ASC
      `,
  });

  const listLatestTurnRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionLatestTurnDbRowSchema,
    execute: () =>
      sql`
        SELECT
          turns.thread_id AS "threadId",
          turns.turn_id AS "turnId",
          turns.state,
          turns.requested_at AS "requestedAt",
          turns.started_at AS "startedAt",
          turns.completed_at AS "completedAt",
          turns.assistant_message_id AS "assistantMessageId",
          turns.effective_model AS "effectiveModel",
          turns.source_proposed_plan_thread_id AS "sourceProposedPlanThreadId",
          turns.source_proposed_plan_id AS "sourceProposedPlanId"
        FROM projection_threads threads
        JOIN projection_turns turns
          ON turns.thread_id = threads.thread_id
          AND turns.turn_id = threads.latest_turn_id
        WHERE threads.latest_turn_id IS NOT NULL
        ORDER BY turns.thread_id ASC
      `,
  });

  const listActiveLatestTurnRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionLatestTurnDbRowSchema,
    execute: () =>
      sql`
        SELECT
          turns.thread_id AS "threadId",
          turns.turn_id AS "turnId",
          turns.state,
          turns.requested_at AS "requestedAt",
          turns.started_at AS "startedAt",
          turns.completed_at AS "completedAt",
          turns.assistant_message_id AS "assistantMessageId",
          turns.effective_model AS "effectiveModel",
          turns.source_proposed_plan_thread_id AS "sourceProposedPlanThreadId",
          turns.source_proposed_plan_id AS "sourceProposedPlanId"
        FROM projection_threads threads
        JOIN projection_turns turns
          ON turns.thread_id = threads.thread_id
          AND turns.turn_id = threads.latest_turn_id
        WHERE threads.deleted_at IS NULL
          AND threads.archived_at IS NULL
          AND threads.latest_turn_id IS NOT NULL
        ORDER BY turns.thread_id ASC
      `,
  });

  const listArchivedLatestTurnRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionLatestTurnDbRowSchema,
    execute: () =>
      sql`
        SELECT
          turns.thread_id AS "threadId",
          turns.turn_id AS "turnId",
          turns.state,
          turns.requested_at AS "requestedAt",
          turns.started_at AS "startedAt",
          turns.completed_at AS "completedAt",
          turns.assistant_message_id AS "assistantMessageId",
          turns.effective_model AS "effectiveModel",
          turns.source_proposed_plan_thread_id AS "sourceProposedPlanThreadId",
          turns.source_proposed_plan_id AS "sourceProposedPlanId"
        FROM projection_threads threads
        JOIN projection_turns turns
          ON turns.thread_id = threads.thread_id
          AND turns.turn_id = threads.latest_turn_id
        WHERE threads.deleted_at IS NULL
          AND threads.archived_at IS NOT NULL
          AND threads.latest_turn_id IS NOT NULL
        ORDER BY turns.thread_id ASC
      `,
  });

  const listProjectionStateRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionStateDbRowSchema,
    execute: () =>
      sql`
        SELECT
          projector,
          last_applied_sequence AS "lastAppliedSequence",
          updated_at AS "updatedAt"
        FROM projection_state
      `,
  });

  const readProjectionCounts = SqlSchema.findOne({
    Request: AudienceCeilingInput,
    Result: ProjectionCountsRowSchema,
    execute: ({ audienceCeiling }) =>
      sql`
        SELECT
          (
            SELECT COUNT(*)
            FROM projection_projects projects
            WHERE ${audienceCeiling} = 'private'
              OR projects.data_audience = 'factory'
          ) AS "projectCount",
          (
            SELECT COUNT(*)
            FROM projection_threads threads
            INNER JOIN projection_projects projects
              ON projects.project_id = threads.project_id
            WHERE ${audienceCeiling} = 'private'
              OR projects.data_audience = 'factory'
          ) AS "threadCount"
      `,
  });

  const getActiveProjectRowByWorkspaceRoot = SqlSchema.findOneOption({
    Request: WorkspaceRootLookupInput,
    Result: ProjectionProjectLookupRowSchema,
    execute: ({ workspaceRoot, audienceCeiling }) =>
      sql`
        SELECT
          project_id AS "projectId",
          title,
          workspace_root AS "workspaceRoot",
          data_audience AS "dataAudience",
          default_model_selection_json AS "defaultModelSelection",
          scripts_json AS "scripts",
          created_at AS "createdAt",
          updated_at AS "updatedAt",
          deleted_at AS "deletedAt"
        FROM projection_projects
        WHERE workspace_root = ${workspaceRoot}
          AND deleted_at IS NULL
          AND (${audienceCeiling} = 'private' OR data_audience = 'factory')
        ORDER BY created_at ASC, project_id ASC
        LIMIT 1
      `,
  });

  const getActiveProjectRowById = SqlSchema.findOneOption({
    Request: ProjectIdLookupInput,
    Result: ProjectionProjectLookupRowSchema,
    execute: ({ projectId, audienceCeiling }) =>
      sql`
        SELECT
          project_id AS "projectId",
          title,
          workspace_root AS "workspaceRoot",
          data_audience AS "dataAudience",
          default_model_selection_json AS "defaultModelSelection",
          scripts_json AS "scripts",
          created_at AS "createdAt",
          updated_at AS "updatedAt",
          deleted_at AS "deletedAt"
        FROM projection_projects
        WHERE project_id = ${projectId}
          AND deleted_at IS NULL
          AND (${audienceCeiling} = 'private' OR data_audience = 'factory')
        LIMIT 1
      `,
  });

  const getFirstActiveThreadIdByProject = SqlSchema.findOneOption({
    Request: ProjectIdLookupInput,
    Result: ProjectionThreadIdLookupRowSchema,
    execute: ({ projectId, audienceCeiling }) =>
      sql`
        SELECT
          threads.thread_id AS "threadId",
          projects.data_audience AS "dataAudience"
        FROM projection_threads threads
        INNER JOIN projection_projects projects
          ON projects.project_id = threads.project_id
        WHERE threads.project_id = ${projectId}
          AND threads.deleted_at IS NULL
          AND threads.archived_at IS NULL
          AND (${audienceCeiling} = 'private' OR projects.data_audience = 'factory')
        ORDER BY threads.created_at ASC, threads.thread_id ASC
        LIMIT 1
      `,
  });

  const getThreadCheckpointContextThreadRow = SqlSchema.findOneOption({
    Request: AudienceThreadIdLookupInput,
    Result: ProjectionThreadCheckpointContextThreadRowSchema,
    execute: ({ threadId, audienceCeiling }) =>
      sql`
        SELECT
          threads.thread_id AS "threadId",
          threads.project_id AS "projectId",
          projects.workspace_root AS "workspaceRoot",
          threads.worktree_path AS "worktreePath",
          projects.data_audience AS "dataAudience"
        FROM projection_threads AS threads
        INNER JOIN projection_projects AS projects
          ON projects.project_id = threads.project_id
        WHERE threads.thread_id = ${threadId}
          AND threads.deleted_at IS NULL
          AND (${audienceCeiling} = 'private' OR projects.data_audience = 'factory')
        LIMIT 1
      `,
  });

  const getActiveThreadRowById = SqlSchema.findOneOption({
    Request: AudienceThreadIdLookupInput,
    Result: ProjectionThreadDbRowSchema,
    execute: ({ threadId, audienceCeiling }) =>
      sql`
        SELECT
          threads.thread_id AS "threadId",
          threads.project_id AS "projectId",
          projects.data_audience AS "dataAudience",
          threads.title,
          threads.model_selection_json AS "modelSelection",
          threads.runtime_mode AS "runtimeMode",
          threads.interaction_mode AS "interactionMode",
          threads.branch,
          threads.worktree_path AS "worktreePath",
          threads.worktree_removable AS "worktreeRemovable",
          threads.worktree_removal_path AS "worktreeRemovalPath",
          threads.latest_turn_id AS "latestTurnId",
          threads.created_at AS "createdAt",
          threads.updated_at AS "updatedAt",
          threads.archived_at AS "archivedAt",
          threads.latest_user_message_at AS "latestUserMessageAt",
          threads.pending_approval_count AS "pendingApprovalCount",
          threads.pending_user_input_count AS "pendingUserInputCount",
          threads.has_actionable_proposed_plan AS "hasActionableProposedPlan",
          threads.deleted_at AS "deletedAt",
          threads.parent_thread_id AS "parentThreadId",
          threads.parent_environment_id AS "parentEnvironmentId"
        FROM projection_threads AS threads
        INNER JOIN projection_projects AS projects
          ON projects.project_id = threads.project_id
        WHERE threads.thread_id = ${threadId}
          AND threads.deleted_at IS NULL
          AND threads.archived_at IS NULL
          AND (${audienceCeiling} = 'private' OR projects.data_audience = 'factory')
        LIMIT 1
      `,
  });

  const getNonDeletedThreadRowById = SqlSchema.findOneOption({
    Request: AudienceThreadIdLookupInput,
    Result: ProjectionThreadDbRowSchema,
    execute: ({ threadId, audienceCeiling }) =>
      sql`
        SELECT
          threads.thread_id AS "threadId",
          threads.project_id AS "projectId",
          projects.data_audience AS "dataAudience",
          threads.title,
          threads.model_selection_json AS "modelSelection",
          threads.runtime_mode AS "runtimeMode",
          threads.interaction_mode AS "interactionMode",
          threads.branch,
          threads.worktree_path AS "worktreePath",
          threads.worktree_removable AS "worktreeRemovable",
          threads.worktree_removal_path AS "worktreeRemovalPath",
          threads.latest_turn_id AS "latestTurnId",
          threads.created_at AS "createdAt",
          threads.updated_at AS "updatedAt",
          threads.archived_at AS "archivedAt",
          threads.latest_user_message_at AS "latestUserMessageAt",
          threads.pending_approval_count AS "pendingApprovalCount",
          threads.pending_user_input_count AS "pendingUserInputCount",
          threads.has_actionable_proposed_plan AS "hasActionableProposedPlan",
          threads.deleted_at AS "deletedAt",
          threads.parent_thread_id AS "parentThreadId",
          threads.parent_environment_id AS "parentEnvironmentId"
        FROM projection_threads AS threads
        INNER JOIN projection_projects AS projects
          ON projects.project_id = threads.project_id
        WHERE threads.thread_id = ${threadId}
          AND threads.deleted_at IS NULL
          AND (${audienceCeiling} = 'private' OR projects.data_audience = 'factory')
        LIMIT 1
      `,
  });

  const listThreadMessageRowsByThread = SqlSchema.findAll({
    Request: ThreadIdLookupInput,
    Result: ProjectionThreadMessageDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          messages.message_id AS "messageId",
          messages.thread_id AS "threadId",
          COALESCE(messages.turn_id, prompt_turns.turn_id) AS "turnId",
          messages.role,
          messages.text,
          messages.attachments_json AS "attachments",
          messages.is_streaming AS "isStreaming",
          messages.created_at AS "createdAt",
          messages.updated_at AS "updatedAt"
        FROM projection_thread_messages messages
        LEFT JOIN projection_turns prompt_turns
          ON prompt_turns.thread_id = messages.thread_id
          AND prompt_turns.pending_message_id = messages.message_id
          AND prompt_turns.turn_id IS NOT NULL
        WHERE messages.thread_id = ${threadId}
        ORDER BY messages.created_at ASC, messages.message_id ASC
      `,
  });

  const listThreadProposedPlanRowsByThread = SqlSchema.findAll({
    Request: ThreadIdLookupInput,
    Result: ProjectionThreadProposedPlanDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          plan_id AS "planId",
          thread_id AS "threadId",
          turn_id AS "turnId",
          plan_markdown AS "planMarkdown",
          implemented_at AS "implementedAt",
          implementation_thread_id AS "implementationThreadId",
          created_at AS "createdAt",
          updated_at AS "updatedAt"
        FROM projection_thread_proposed_plans
        WHERE thread_id = ${threadId}
        ORDER BY created_at ASC, plan_id ASC
      `,
  });

  const listThreadActivityRowsByThread = SqlSchema.findAll({
    Request: ThreadIdLookupInput,
    Result: ProjectionThreadActivityDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          activity_id AS "activityId",
          thread_id AS "threadId",
          turn_id AS "turnId",
          tone,
          kind,
          summary,
          payload_json AS "payload",
          sequence,
          created_at AS "createdAt"
        FROM projection_thread_activities
        WHERE thread_id = ${threadId}
        ORDER BY
          sequence ASC,
          created_at ASC,
          activity_id ASC
      `,
  });

  const getThreadSessionRowByThread = SqlSchema.findOneOption({
    Request: ThreadIdLookupInput,
    Result: ProjectionThreadSessionDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          thread_id AS "threadId",
          status,
          provider_name AS "providerName",
          provider_instance_id AS "providerInstanceId",
          runtime_mode AS "runtimeMode",
          active_turn_id AS "activeTurnId",
          last_error AS "lastError",
          updated_at AS "updatedAt"
        FROM projection_thread_sessions
        WHERE thread_id = ${threadId}
        LIMIT 1
      `,
  });

  const getLatestTurnRowByThread = SqlSchema.findOneOption({
    Request: ThreadIdLookupInput,
    Result: ProjectionLatestTurnDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          turns.thread_id AS "threadId",
          turns.turn_id AS "turnId",
          turns.state,
          turns.requested_at AS "requestedAt",
          turns.started_at AS "startedAt",
          turns.completed_at AS "completedAt",
          turns.assistant_message_id AS "assistantMessageId",
          turns.effective_model AS "effectiveModel",
          turns.source_proposed_plan_thread_id AS "sourceProposedPlanThreadId",
          turns.source_proposed_plan_id AS "sourceProposedPlanId"
        FROM projection_threads threads
        JOIN projection_turns turns
          ON turns.thread_id = threads.thread_id
          AND turns.turn_id = threads.latest_turn_id
        WHERE threads.thread_id = ${threadId}
          AND threads.deleted_at IS NULL
        LIMIT 1
      `,
  });

  const listTurnRowsByThread = SqlSchema.findAll({
    Request: ThreadIdLookupInput,
    Result: ProjectionLatestTurnDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          turns.thread_id AS "threadId",
          turns.turn_id AS "turnId",
          turns.state,
          turns.requested_at AS "requestedAt",
          turns.started_at AS "startedAt",
          turns.completed_at AS "completedAt",
          turns.assistant_message_id AS "assistantMessageId",
          turns.effective_model AS "effectiveModel",
          turns.source_proposed_plan_thread_id AS "sourceProposedPlanThreadId",
          turns.source_proposed_plan_id AS "sourceProposedPlanId"
        FROM projection_turns turns
        WHERE turns.thread_id = ${threadId}
          AND turns.turn_id IS NOT NULL
        ORDER BY turns.requested_at ASC, turns.turn_id ASC
      `,
  });

  const listCheckpointRowsByThread = SqlSchema.findAll({
    Request: ThreadIdLookupInput,
    Result: ProjectionCheckpointDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          checkpoint_rows.thread_id AS "threadId",
          checkpoint_rows.turn_id AS "turnId",
          checkpoint_rows.checkpoint_turn_count AS "checkpointTurnCount",
          checkpoint_rows.checkpoint_ref AS "checkpointRef",
          checkpoint_rows.checkpoint_status AS "status",
          checkpoint_rows.checkpoint_files_json AS "files",
          checkpoint_rows.assistant_message_id AS "assistantMessageId",
          checkpoint_rows.completed_at AS "completedAt"
        FROM (
          SELECT
            thread_id,
            turn_id,
            checkpoint_turn_count,
            checkpoint_ref,
            checkpoint_status,
            checkpoint_files_json,
            assistant_message_id,
            completed_at,
            ROW_NUMBER() OVER (
              ORDER BY checkpoint_turn_count DESC
            ) AS checkpoint_rank
          FROM projection_turns
          WHERE thread_id = ${threadId}
            AND checkpoint_turn_count IS NOT NULL
        ) checkpoint_rows
        WHERE checkpoint_rows.checkpoint_rank <= ${CHECKPOINT_DIFF_CONTEXT_KEEP_PER_THREAD}
        ORDER BY checkpoint_rows.checkpoint_turn_count ASC
      `,
  });

  const listDetailCheckpointRowsByThread = SqlSchema.findAll({
    Request: ThreadIdLookupInput,
    Result: ProjectionCheckpointDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          checkpoint_rows.thread_id AS "threadId",
          checkpoint_rows.turn_id AS "turnId",
          checkpoint_rows.checkpoint_turn_count AS "checkpointTurnCount",
          checkpoint_rows.checkpoint_ref AS "checkpointRef",
          checkpoint_rows.checkpoint_status AS "status",
          checkpoint_rows.checkpoint_files_json AS "files",
          checkpoint_rows.assistant_message_id AS "assistantMessageId",
          checkpoint_rows.completed_at AS "completedAt"
        FROM (
          SELECT
            thread_id,
            turn_id,
            checkpoint_turn_count,
            checkpoint_ref,
            checkpoint_status,
            checkpoint_files_json,
            assistant_message_id,
            completed_at,
            ROW_NUMBER() OVER (
              ORDER BY checkpoint_turn_count DESC
            ) AS checkpoint_rank
          FROM projection_turns
          WHERE thread_id = ${threadId}
            AND checkpoint_turn_count IS NOT NULL
        ) checkpoint_rows
        WHERE checkpoint_rows.checkpoint_rank <= ${MAX_THREAD_CHECKPOINTS}
        ORDER BY checkpoint_rows.checkpoint_turn_count ASC
      `,
  });

  const listCheckpointTurnIdRowsByThread = SqlSchema.findAll({
    Request: CheckpointTurnIdLookupInput,
    Result: ProjectionCheckpointTurnIdRowSchema,
    execute: ({ threadId, turnId }) =>
      sql`
        SELECT
          turn_id AS "turnId"
        FROM projection_turns
        WHERE thread_id = ${threadId}
          AND turn_id = ${turnId}
          AND checkpoint_turn_count IS NOT NULL
        LIMIT 1
      `,
  });

  const getFullThreadDiffContextRow = SqlSchema.findOneOption({
    Request: FullThreadDiffContextLookupInput,
    Result: ProjectionFullThreadDiffContextRowSchema,
    execute: ({ threadId, checkpointTurnCount, audienceCeiling }) =>
      sql`
        SELECT
          threads.thread_id AS "threadId",
          threads.project_id AS "projectId",
          projects.workspace_root AS "workspaceRoot",
          threads.worktree_path AS "worktreePath",
          projects.data_audience AS "dataAudience",
          (
            SELECT MAX(turns.checkpoint_turn_count)
            FROM projection_turns AS turns
            WHERE turns.thread_id = threads.thread_id
              AND turns.checkpoint_turn_count IS NOT NULL
          ) AS "latestCheckpointTurnCount",
          (
            SELECT checkpoint_rows.checkpoint_ref
            FROM (
              SELECT
                turns.checkpoint_turn_count,
                turns.checkpoint_ref,
                ROW_NUMBER() OVER (
                  ORDER BY turns.checkpoint_turn_count DESC
                ) AS checkpoint_rank
              FROM projection_turns AS turns
              WHERE turns.thread_id = threads.thread_id
                AND turns.checkpoint_turn_count IS NOT NULL
            ) checkpoint_rows
            WHERE checkpoint_rows.checkpoint_turn_count = ${checkpointTurnCount}
              AND checkpoint_rows.checkpoint_rank <= ${CHECKPOINT_DIFF_CONTEXT_KEEP_PER_THREAD}
            LIMIT 1
          ) AS "toCheckpointRef"
        FROM projection_threads AS threads
        INNER JOIN projection_projects AS projects
          ON projects.project_id = threads.project_id
        WHERE threads.thread_id = ${threadId}
          AND threads.deleted_at IS NULL
          AND (${audienceCeiling} = 'private' OR projects.data_audience = 'factory')
        LIMIT 1
      `,
  });

  const loadReadableThreadIds = (
    audienceCeiling: AuthAudienceCeiling,
    operation: string,
  ): Effect.Effect<ReadableThreadIds, ProjectionRepositoryError> =>
    Effect.gen(function* () {
      if (audienceCeiling === "private") {
        return undefined;
      }
      const rows = yield* listThreadAudienceRows(undefined).pipe(
        Effect.mapError(
          toPersistenceSqlOrDecodeError(`${operation}:query`, `${operation}:decodeRows`),
        ),
      );
      return readableThreadIdsForAudience(audienceCeiling, rows);
    });

  const getSnapshot: ProjectionSnapshotQueryShape["getSnapshot"] = () =>
    sql
      .withTransaction(
        Effect.all([
          listProjectRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getSnapshot:listProjects:query",
                "ProjectionSnapshotQuery.getSnapshot:listProjects:decodeRows",
              ),
            ),
          ),
          listThreadRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getSnapshot:listThreads:query",
                "ProjectionSnapshotQuery.getSnapshot:listThreads:decodeRows",
              ),
            ),
          ),
          listThreadSessionRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getSnapshot:listThreadSessions:query",
                "ProjectionSnapshotQuery.getSnapshot:listThreadSessions:decodeRows",
              ),
            ),
          ),
          listLatestTurnRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getSnapshot:listLatestTurns:query",
                "ProjectionSnapshotQuery.getSnapshot:listLatestTurns:decodeRows",
              ),
            ),
          ),
          listProjectionStateRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getSnapshot:listProjectionState:query",
                "ProjectionSnapshotQuery.getSnapshot:listProjectionState:decodeRows",
              ),
            ),
          ),
        ]),
      )
      .pipe(
        Effect.flatMap(([projectRows, threadRows, sessionRows, latestTurnRows, stateRows]) =>
          Effect.gen(function* () {
            const audienceCeiling = yield* currentReadAudienceCeiling;
            const {
              projectRows: visibleProjectRows,
              threadRows: visibleThreadRows,
              sessionRows: visibleSessionRows,
              latestTurnRows: visibleLatestTurnRows,
              readableThreadIds,
            } = scopeProjectAndThreadRows({
              audienceCeiling,
              projectRows,
              threadRows,
              sessionRows,
              latestTurnRows,
            });
            const sessionsByThread = new Map<string, OrchestrationSession>();
            const latestTurnByThread = new Map<string, OrchestrationLatestTurn>();

            let updatedAt: string | null = null;

            for (const row of visibleProjectRows) {
              updatedAt = maxIso(updatedAt, row.updatedAt);
            }
            for (const row of visibleThreadRows) {
              updatedAt = maxIso(updatedAt, row.updatedAt);
            }
            for (const row of stateRows) {
              updatedAt = maxIso(updatedAt, row.updatedAt);
            }

            for (const row of visibleLatestTurnRows) {
              updatedAt = maxIso(updatedAt, row.requestedAt);
              if (row.startedAt !== null) {
                updatedAt = maxIso(updatedAt, row.startedAt);
              }
              if (row.completedAt !== null) {
                updatedAt = maxIso(updatedAt, row.completedAt);
              }
              if (latestTurnByThread.has(row.threadId)) {
                continue;
              }
              latestTurnByThread.set(row.threadId, mapLatestTurn(row, readableThreadIds));
            }

            for (const row of visibleSessionRows) {
              updatedAt = maxIso(updatedAt, row.updatedAt);
              sessionsByThread.set(row.threadId, {
                threadId: row.threadId,
                status: row.status,
                providerName: row.providerName,
                ...(row.providerInstanceId !== null
                  ? { providerInstanceId: row.providerInstanceId }
                  : {}),
                runtimeMode: row.runtimeMode,
                activeTurnId: row.activeTurnId,
                lastError: row.lastError,
                updatedAt: row.updatedAt,
              });
            }

            const repositoryIdentities = yield* resolveRepositoryIdentitiesForProjects(
              visibleProjectRows,
              { includeDeleted: true },
            );

            const projects: ReadonlyArray<OrchestrationProject> = visibleProjectRows.map((row) => ({
              id: row.projectId,
              title: row.title,
              workspaceRoot: row.workspaceRoot,
              dataAudience: row.dataAudience,
              repositoryIdentity: repositoryIdentities.get(row.projectId) ?? null,
              defaultModelSelection: row.defaultModelSelection,
              scripts: row.scripts,
              createdAt: row.createdAt,
              updatedAt: row.updatedAt,
              deletedAt: row.deletedAt,
            }));

            const threads: ReadonlyArray<OrchestrationThread> = visibleThreadRows.map((row) => ({
              id: row.threadId,
              projectId: row.projectId,
              dataAudience: row.dataAudience,
              title: row.title,
              modelSelection: row.modelSelection,
              runtimeMode: row.runtimeMode,
              interactionMode: row.interactionMode,
              branch: row.branch,
              worktreePath: row.worktreePath,
              worktreeRemovable: row.worktreeRemovable > 0,
              worktreeRemovalPath: row.worktreeRemovalPath,
              latestTurn: latestTurnByThread.get(row.threadId) ?? null,
              createdAt: row.createdAt,
              updatedAt: row.updatedAt,
              archivedAt: row.archivedAt,
              deletedAt: row.deletedAt,
              ...mapParentThreadReference(row, readableThreadIds),
              messages: [],
              turns: [],
              proposedPlans: [],
              activities: [],
              checkpoints: [],
              session: sessionsByThread.get(row.threadId) ?? null,
            }));

            const snapshot = {
              snapshotSequence: computeSnapshotSequence(stateRows),
              projects,
              threads,
              updatedAt: updatedAt ?? "1970-01-01T00:00:00.000Z",
            };

            return yield* decodeReadModel(snapshot).pipe(
              Effect.mapError(
                toPersistenceDecodeError("ProjectionSnapshotQuery.getSnapshot:decodeReadModel"),
              ),
            );
          }),
        ),
        Effect.mapError((error) => {
          if (isPersistenceError(error)) {
            return error;
          }
          return toPersistenceSqlError("ProjectionSnapshotQuery.getSnapshot:query")(error);
        }),
      );

  const getCommandReadModel: ProjectionSnapshotQueryShape["getCommandReadModel"] = () =>
    sql
      .withTransaction(
        Effect.all([
          listProjectRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getCommandReadModel:listProjects:query",
                "ProjectionSnapshotQuery.getCommandReadModel:listProjects:decodeRows",
              ),
            ),
          ),
          listThreadRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getCommandReadModel:listThreads:query",
                "ProjectionSnapshotQuery.getCommandReadModel:listThreads:decodeRows",
              ),
            ),
          ),
          listThreadProposedPlanRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getCommandReadModel:listThreadProposedPlans:query",
                "ProjectionSnapshotQuery.getCommandReadModel:listThreadProposedPlans:decodeRows",
              ),
            ),
          ),
          listThreadSessionRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getCommandReadModel:listThreadSessions:query",
                "ProjectionSnapshotQuery.getCommandReadModel:listThreadSessions:decodeRows",
              ),
            ),
          ),
          listLatestTurnRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getCommandReadModel:listLatestTurns:query",
                "ProjectionSnapshotQuery.getCommandReadModel:listLatestTurns:decodeRows",
              ),
            ),
          ),
          listProjectionStateRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getCommandReadModel:listProjectionState:query",
                "ProjectionSnapshotQuery.getCommandReadModel:listProjectionState:decodeRows",
              ),
            ),
          ),
        ]),
      )
      .pipe(
        Effect.flatMap(
          ([projectRows, threadRows, proposedPlanRows, sessionRows, latestTurnRows, stateRows]) =>
            Effect.sync(() => {
              let updatedAt: string | null = null;
              const projects: OrchestrationProject[] = [];
              const threads: OrchestrationThread[] = [];

              for (let index = 0; index < projectRows.length; index += 1) {
                const row = projectRows[index];
                if (!row) {
                  continue;
                }
                updatedAt = maxIso(updatedAt, row.updatedAt);
                projects.push({
                  id: row.projectId,
                  title: row.title,
                  workspaceRoot: row.workspaceRoot,
                  dataAudience: row.dataAudience,
                  defaultModelSelection: row.defaultModelSelection,
                  scripts: row.scripts,
                  createdAt: row.createdAt,
                  updatedAt: row.updatedAt,
                  deletedAt: row.deletedAt,
                });
              }
              for (let index = 0; index < threadRows.length; index += 1) {
                const row = threadRows[index];
                if (!row) {
                  continue;
                }
                updatedAt = maxIso(updatedAt, row.updatedAt);
              }
              for (let index = 0; index < proposedPlanRows.length; index += 1) {
                const row = proposedPlanRows[index];
                if (!row) {
                  continue;
                }
                updatedAt = maxIso(updatedAt, row.updatedAt);
              }
              for (let index = 0; index < sessionRows.length; index += 1) {
                const row = sessionRows[index];
                if (!row) {
                  continue;
                }
                updatedAt = maxIso(updatedAt, row.updatedAt);
              }
              for (let index = 0; index < latestTurnRows.length; index += 1) {
                const row = latestTurnRows[index];
                if (!row) {
                  continue;
                }
                updatedAt = maxIso(updatedAt, row.requestedAt);
                if (row.startedAt !== null) {
                  updatedAt = maxIso(updatedAt, row.startedAt);
                }
                if (row.completedAt !== null) {
                  updatedAt = maxIso(updatedAt, row.completedAt);
                }
              }
              for (let index = 0; index < stateRows.length; index += 1) {
                const row = stateRows[index];
                if (!row) {
                  continue;
                }
                updatedAt = maxIso(updatedAt, row.updatedAt);
              }

              const latestTurnByThread = new Map<string, OrchestrationLatestTurn>();
              for (let index = 0; index < latestTurnRows.length; index += 1) {
                const row = latestTurnRows[index];
                if (!row) {
                  continue;
                }
                latestTurnByThread.set(row.threadId, mapLatestTurn(row));
              }
              const proposedPlansByThread = new Map<string, Array<OrchestrationProposedPlan>>();
              const sessionByThread = new Map<string, OrchestrationSession>();

              for (let index = 0; index < sessionRows.length; index += 1) {
                const row = sessionRows[index];
                if (!row) {
                  continue;
                }
                sessionByThread.set(row.threadId, mapSessionRow(row));
              }

              for (let index = 0; index < proposedPlanRows.length; index += 1) {
                const row = proposedPlanRows[index];
                if (!row) {
                  continue;
                }
                const threadProposedPlans = proposedPlansByThread.get(row.threadId) ?? [];
                threadProposedPlans.push(mapProposedPlanRow(row));
                proposedPlansByThread.set(row.threadId, threadProposedPlans);
              }

              for (let index = 0; index < threadRows.length; index += 1) {
                const row = threadRows[index];
                if (!row) {
                  continue;
                }
                threads.push({
                  id: row.threadId,
                  projectId: row.projectId,
                  dataAudience: row.dataAudience,
                  title: row.title,
                  modelSelection: row.modelSelection,
                  runtimeMode: row.runtimeMode,
                  interactionMode: row.interactionMode,
                  branch: row.branch,
                  worktreePath: row.worktreePath,
                  worktreeRemovable: row.worktreeRemovable > 0,
                  worktreeRemovalPath: row.worktreeRemovalPath,
                  latestTurn: latestTurnByThread.get(row.threadId) ?? null,
                  createdAt: row.createdAt,
                  updatedAt: row.updatedAt,
                  archivedAt: row.archivedAt,
                  deletedAt: row.deletedAt,
                  parentThreadId: row.parentThreadId,
                  parentEnvironmentId: row.parentEnvironmentId ?? null,
                  messages: [],
                  turns: [],
                  proposedPlans: proposedPlansByThread.get(row.threadId) ?? [],
                  activities: [],
                  checkpoints: [],
                  session: sessionByThread.get(row.threadId) ?? null,
                });
              }

              return {
                snapshotSequence: computeSnapshotSequence(stateRows),
                projects,
                threads,
                updatedAt: updatedAt ?? "1970-01-01T00:00:00.000Z",
              } satisfies OrchestrationReadModel;
            }),
        ),
        Effect.mapError((error) => {
          if (isPersistenceError(error)) {
            return error;
          }
          return toPersistenceSqlError("ProjectionSnapshotQuery.getCommandReadModel:query")(error);
        }),
      );

  const getShellSnapshot: ProjectionSnapshotQueryShape["getShellSnapshot"] = () =>
    sql
      .withTransaction(
        Effect.all([
          listProjectRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getShellSnapshot:listProjects:query",
                "ProjectionSnapshotQuery.getShellSnapshot:listProjects:decodeRows",
              ),
            ),
          ),
          listActiveThreadRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getShellSnapshot:listThreads:query",
                "ProjectionSnapshotQuery.getShellSnapshot:listThreads:decodeRows",
              ),
            ),
          ),
          listActiveThreadSessionRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getShellSnapshot:listThreadSessions:query",
                "ProjectionSnapshotQuery.getShellSnapshot:listThreadSessions:decodeRows",
              ),
            ),
          ),
          listActiveLatestTurnRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getShellSnapshot:listLatestTurns:query",
                "ProjectionSnapshotQuery.getShellSnapshot:listLatestTurns:decodeRows",
              ),
            ),
          ),
          listThreadAudienceRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getShellSnapshot:listThreadAudiences:query",
                "ProjectionSnapshotQuery.getShellSnapshot:listThreadAudiences:decodeRows",
              ),
            ),
          ),
          listProjectionStateRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getShellSnapshot:listProjectionState:query",
                "ProjectionSnapshotQuery.getShellSnapshot:listProjectionState:decodeRows",
              ),
            ),
          ),
        ]),
      )
      .pipe(
        Effect.flatMap(
          ([projectRows, threadRows, sessionRows, latestTurnRows, threadAudienceRows, stateRows]) =>
            Effect.gen(function* () {
              const audienceCeiling = yield* currentReadAudienceCeiling;
              const readableThreadIds = readableThreadIdsForAudience(
                audienceCeiling,
                threadAudienceRows,
              );
              const {
                projectRows: visibleProjectRows,
                threadRows: visibleThreadRows,
                sessionRows: visibleSessionRows,
                latestTurnRows: visibleLatestTurnRows,
              } = scopeProjectAndThreadRows({
                audienceCeiling,
                projectRows,
                threadRows,
                sessionRows,
                latestTurnRows,
              });
              let updatedAt: string | null = null;
              for (const row of visibleProjectRows) {
                updatedAt = maxIso(updatedAt, row.updatedAt);
              }
              for (const row of visibleThreadRows) {
                updatedAt = maxIso(updatedAt, row.updatedAt);
              }
              for (const row of visibleSessionRows) {
                updatedAt = maxIso(updatedAt, row.updatedAt);
              }
              for (const row of visibleLatestTurnRows) {
                updatedAt = maxIso(updatedAt, row.requestedAt);
                if (row.startedAt !== null) {
                  updatedAt = maxIso(updatedAt, row.startedAt);
                }
                if (row.completedAt !== null) {
                  updatedAt = maxIso(updatedAt, row.completedAt);
                }
              }
              for (const row of stateRows) {
                updatedAt = maxIso(updatedAt, row.updatedAt);
              }

              const repositoryIdentities =
                yield* resolveRepositoryIdentitiesForProjects(visibleProjectRows);
              const latestTurnByThread = new Map(
                visibleLatestTurnRows.map(
                  (row) => [row.threadId, mapLatestTurn(row, readableThreadIds)] as const,
                ),
              );
              const sessionByThread = new Map(
                visibleSessionRows.map((row) => [row.threadId, mapSessionRow(row)] as const),
              );

              const snapshot = {
                snapshotSequence: computeSnapshotSequence(stateRows),
                projects: Arr.filterMap(visibleProjectRows, (row) =>
                  row.deletedAt === null
                    ? Result.succeed(
                        mapProjectShellRow(row, repositoryIdentities.get(row.projectId) ?? null),
                      )
                    : Result.failVoid,
                ),
                threads: Arr.filterMap(visibleThreadRows, (row) =>
                  row.deletedAt === null
                    ? Result.succeed({
                        id: row.threadId,
                        projectId: row.projectId,
                        dataAudience: row.dataAudience,
                        title: row.title,
                        modelSelection: row.modelSelection,
                        runtimeMode: row.runtimeMode,
                        interactionMode: row.interactionMode,
                        branch: row.branch,
                        worktreePath: row.worktreePath,
                        worktreeRemovable: row.worktreeRemovable > 0,
                        worktreeRemovalPath: row.worktreeRemovalPath,
                        latestTurn: latestTurnByThread.get(row.threadId) ?? null,
                        createdAt: row.createdAt,
                        updatedAt: row.updatedAt,
                        archivedAt: row.archivedAt,
                        session: sessionByThread.get(row.threadId) ?? null,
                        latestUserMessageAt: row.latestUserMessageAt,
                        hasPendingApprovals: row.pendingApprovalCount > 0,
                        hasPendingUserInput: row.pendingUserInputCount > 0,
                        hasActionableProposedPlan: row.hasActionableProposedPlan > 0,
                        ...mapParentThreadReference(row, readableThreadIds),
                      } satisfies OrchestrationThreadShell)
                    : Result.failVoid,
                ),
                updatedAt: updatedAt ?? "1970-01-01T00:00:00.000Z",
              };

              return yield* decodeShellSnapshot(snapshot).pipe(
                Effect.mapError(
                  toPersistenceDecodeError(
                    "ProjectionSnapshotQuery.getShellSnapshot:decodeShellSnapshot",
                  ),
                ),
              );
            }),
        ),
        Effect.mapError((error) => {
          if (isPersistenceError(error)) {
            return error;
          }
          return toPersistenceSqlError("ProjectionSnapshotQuery.getShellSnapshot:query")(error);
        }),
      );

  const getArchivedShellSnapshot: ProjectionSnapshotQueryShape["getArchivedShellSnapshot"] = () =>
    sql
      .withTransaction(
        Effect.all([
          listProjectRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getArchivedShellSnapshot:listProjects:query",
                "ProjectionSnapshotQuery.getArchivedShellSnapshot:listProjects:decodeRows",
              ),
            ),
          ),
          listArchivedThreadRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getArchivedShellSnapshot:listThreads:query",
                "ProjectionSnapshotQuery.getArchivedShellSnapshot:listThreads:decodeRows",
              ),
            ),
          ),
          listArchivedThreadSessionRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getArchivedShellSnapshot:listThreadSessions:query",
                "ProjectionSnapshotQuery.getArchivedShellSnapshot:listThreadSessions:decodeRows",
              ),
            ),
          ),
          listArchivedLatestTurnRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getArchivedShellSnapshot:listLatestTurns:query",
                "ProjectionSnapshotQuery.getArchivedShellSnapshot:listLatestTurns:decodeRows",
              ),
            ),
          ),
          listThreadAudienceRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getArchivedShellSnapshot:listThreadAudiences:query",
                "ProjectionSnapshotQuery.getArchivedShellSnapshot:listThreadAudiences:decodeRows",
              ),
            ),
          ),
          listProjectionStateRows(undefined).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getArchivedShellSnapshot:listProjectionState:query",
                "ProjectionSnapshotQuery.getArchivedShellSnapshot:listProjectionState:decodeRows",
              ),
            ),
          ),
        ]),
      )
      .pipe(
        Effect.flatMap(
          ([projectRows, threadRows, sessionRows, latestTurnRows, threadAudienceRows, stateRows]) =>
            Effect.gen(function* () {
              const audienceCeiling = yield* currentReadAudienceCeiling;
              const readableThreadIds = readableThreadIdsForAudience(
                audienceCeiling,
                threadAudienceRows,
              );
              const {
                projectRows: visibleProjectRows,
                threadRows: visibleThreadRows,
                sessionRows: visibleSessionRows,
                latestTurnRows: visibleLatestTurnRows,
              } = scopeProjectAndThreadRows({
                audienceCeiling,
                projectRows,
                threadRows,
                sessionRows,
                latestTurnRows,
              });
              let updatedAt: string | null = null;
              for (const row of visibleProjectRows) {
                updatedAt = maxIso(updatedAt, row.updatedAt);
              }
              for (const row of visibleThreadRows) {
                updatedAt = maxIso(updatedAt, row.updatedAt);
              }
              for (const row of visibleSessionRows) {
                updatedAt = maxIso(updatedAt, row.updatedAt);
              }
              for (const row of visibleLatestTurnRows) {
                updatedAt = maxIso(updatedAt, row.requestedAt);
                if (row.startedAt !== null) {
                  updatedAt = maxIso(updatedAt, row.startedAt);
                }
                if (row.completedAt !== null) {
                  updatedAt = maxIso(updatedAt, row.completedAt);
                }
              }
              for (const row of stateRows) {
                updatedAt = maxIso(updatedAt, row.updatedAt);
              }

              const activeProjectIds = new Set(visibleThreadRows.map((row) => row.projectId));
              const repositoryIdentities = yield* resolveRepositoryIdentitiesForProjects(
                visibleProjectRows.filter((row) => activeProjectIds.has(row.projectId)),
              );
              const latestTurnByThread = new Map(
                visibleLatestTurnRows.map(
                  (row) => [row.threadId, mapLatestTurn(row, readableThreadIds)] as const,
                ),
              );
              const sessionByThread = new Map(
                visibleSessionRows.map((row) => [row.threadId, mapSessionRow(row)] as const),
              );

              const snapshot = {
                snapshotSequence: computeSnapshotSequence(stateRows),
                projects: Arr.filterMap(visibleProjectRows, (row) =>
                  row.deletedAt === null && activeProjectIds.has(row.projectId)
                    ? Result.succeed(
                        mapProjectShellRow(row, repositoryIdentities.get(row.projectId) ?? null),
                      )
                    : Result.failVoid,
                ),
                threads: visibleThreadRows.map(
                  (row): OrchestrationThreadShell => ({
                    id: row.threadId,
                    projectId: row.projectId,
                    dataAudience: row.dataAudience,
                    title: row.title,
                    modelSelection: row.modelSelection,
                    runtimeMode: row.runtimeMode,
                    interactionMode: row.interactionMode,
                    branch: row.branch,
                    worktreePath: row.worktreePath,
                    worktreeRemovable: row.worktreeRemovable > 0,
                    worktreeRemovalPath: row.worktreeRemovalPath,
                    latestTurn: latestTurnByThread.get(row.threadId) ?? null,
                    createdAt: row.createdAt,
                    updatedAt: row.updatedAt,
                    archivedAt: row.archivedAt,
                    session: sessionByThread.get(row.threadId) ?? null,
                    latestUserMessageAt: row.latestUserMessageAt,
                    hasPendingApprovals: row.pendingApprovalCount > 0,
                    hasPendingUserInput: row.pendingUserInputCount > 0,
                    hasActionableProposedPlan: row.hasActionableProposedPlan > 0,
                    ...mapParentThreadReference(row, readableThreadIds),
                  }),
                ),
                updatedAt: updatedAt ?? "1970-01-01T00:00:00.000Z",
              };

              return yield* decodeShellSnapshot(snapshot).pipe(
                Effect.mapError(
                  toPersistenceDecodeError(
                    "ProjectionSnapshotQuery.getArchivedShellSnapshot:decodeShellSnapshot",
                  ),
                ),
              );
            }),
        ),
        Effect.mapError((error) => {
          if (isPersistenceError(error)) {
            return error;
          }
          return toPersistenceSqlError("ProjectionSnapshotQuery.getArchivedShellSnapshot:query")(
            error,
          );
        }),
      );

  const getSnapshotSequence: ProjectionSnapshotQueryShape["getSnapshotSequence"] = () =>
    listProjectionStateRows(undefined).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "ProjectionSnapshotQuery.getSnapshotSequence:query",
          "ProjectionSnapshotQuery.getSnapshotSequence:decodeRows",
        ),
      ),
      Effect.map((stateRows) => ({
        snapshotSequence: computeSnapshotSequence(stateRows),
      })),
    );

  const getCounts: ProjectionSnapshotQueryShape["getCounts"] = () =>
    currentReadAudienceCeiling.pipe(
      Effect.flatMap((audienceCeiling) =>
        readProjectionCounts({ audienceCeiling }).pipe(
          Effect.mapError(
            toPersistenceSqlOrDecodeError(
              "ProjectionSnapshotQuery.getCounts:query",
              "ProjectionSnapshotQuery.getCounts:decodeRow",
            ),
          ),
          Effect.map(
            (row): ProjectionSnapshotCounts => ({
              projectCount: row.projectCount,
              threadCount: row.threadCount,
            }),
          ),
        ),
      ),
    );

  const getActiveProjectByWorkspaceRoot: ProjectionSnapshotQueryShape["getActiveProjectByWorkspaceRoot"] =
    (workspaceRoot) =>
      currentReadAudienceCeiling.pipe(
        Effect.flatMap((audienceCeiling) =>
          getActiveProjectRowByWorkspaceRoot({ workspaceRoot, audienceCeiling }).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getActiveProjectByWorkspaceRoot:query",
                "ProjectionSnapshotQuery.getActiveProjectByWorkspaceRoot:decodeRow",
              ),
            ),
            Effect.flatMap((option) =>
              Option.isNone(option)
                ? Effect.succeed(Option.none<OrchestrationProject>())
                : repositoryIdentityResolver.resolve(option.value.workspaceRoot).pipe(
                    Effect.map((repositoryIdentity) =>
                      Option.some({
                        id: option.value.projectId,
                        title: option.value.title,
                        workspaceRoot: option.value.workspaceRoot,
                        dataAudience: option.value.dataAudience,
                        repositoryIdentity,
                        defaultModelSelection: option.value.defaultModelSelection,
                        scripts: option.value.scripts,
                        createdAt: option.value.createdAt,
                        updatedAt: option.value.updatedAt,
                        deletedAt: option.value.deletedAt,
                      } satisfies OrchestrationProject),
                    ),
                  ),
            ),
          ),
        ),
      );

  const getProjectShellById: ProjectionSnapshotQueryShape["getProjectShellById"] = (projectId) =>
    currentReadAudienceCeiling.pipe(
      Effect.flatMap((audienceCeiling) =>
        getActiveProjectRowById({ projectId, audienceCeiling }).pipe(
          Effect.mapError(
            toPersistenceSqlOrDecodeError(
              "ProjectionSnapshotQuery.getProjectShellById:query",
              "ProjectionSnapshotQuery.getProjectShellById:decodeRow",
            ),
          ),
          Effect.flatMap((option) =>
            Option.isNone(option)
              ? Effect.succeed(Option.none<OrchestrationProjectShell>())
              : repositoryIdentityResolver
                  .resolve(option.value.workspaceRoot)
                  .pipe(
                    Effect.map((repositoryIdentity) =>
                      Option.some(mapProjectShellRow(option.value, repositoryIdentity)),
                    ),
                  ),
          ),
        ),
      ),
    );

  const getFirstActiveThreadIdByProjectId: ProjectionSnapshotQueryShape["getFirstActiveThreadIdByProjectId"] =
    (projectId) =>
      currentReadAudienceCeiling.pipe(
        Effect.flatMap((audienceCeiling) =>
          getFirstActiveThreadIdByProject({ projectId, audienceCeiling }).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getFirstActiveThreadIdByProjectId:query",
                "ProjectionSnapshotQuery.getFirstActiveThreadIdByProjectId:decodeRow",
              ),
            ),
            Effect.map(Option.map((row) => row.threadId)),
          ),
        ),
      );

  const getThreadCheckpointContext: ProjectionSnapshotQueryShape["getThreadCheckpointContext"] = (
    threadId,
    options,
  ) =>
    Effect.gen(function* () {
      const audienceCeiling = yield* currentReadAudienceCeiling;
      const threadRow = yield* getThreadCheckpointContextThreadRow({
        threadId,
        audienceCeiling,
      }).pipe(
        Effect.mapError(
          toPersistenceSqlOrDecodeError(
            "ProjectionSnapshotQuery.getThreadCheckpointContext:getThread:query",
            "ProjectionSnapshotQuery.getThreadCheckpointContext:getThread:decodeRow",
          ),
        ),
      );
      if (Option.isNone(threadRow)) {
        return Option.none<ProjectionThreadCheckpointContext>();
      }

      const checkpointRows = yield* listCheckpointRowsByThread({ threadId }).pipe(
        Effect.mapError(
          toPersistenceSqlOrDecodeError(
            "ProjectionSnapshotQuery.getThreadCheckpointContext:listCheckpoints:query",
            "ProjectionSnapshotQuery.getThreadCheckpointContext:listCheckpoints:decodeRows",
          ),
        ),
      );

      const trackedCheckpointTurnIds =
        options?.trackedTurnId !== undefined
          ? yield* listCheckpointTurnIdRowsByThread({
              threadId,
              turnId: options.trackedTurnId,
            }).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getThreadCheckpointContext:listCheckpointTurnId:query",
                  "ProjectionSnapshotQuery.getThreadCheckpointContext:listCheckpointTurnId:decodeRows",
                ),
              ),
              Effect.map((rows) => rows.map((row) => row.turnId)),
            )
          : undefined;

      return Option.some({
        threadId: threadRow.value.threadId,
        projectId: threadRow.value.projectId,
        workspaceRoot: threadRow.value.workspaceRoot,
        worktreePath: threadRow.value.worktreePath,
        ...(trackedCheckpointTurnIds ? { trackedCheckpointTurnIds } : {}),
        checkpoints: checkpointRows.map(
          (row): OrchestrationCheckpointSummary => ({
            turnId: row.turnId,
            checkpointTurnCount: row.checkpointTurnCount,
            checkpointRef: row.checkpointRef,
            status: row.status,
            files: row.files,
            assistantMessageId: row.assistantMessageId,
            completedAt: row.completedAt,
          }),
        ),
      });
    });

  const getFullThreadDiffContext: NonNullable<
    ProjectionSnapshotQueryShape["getFullThreadDiffContext"]
  > = (threadId, toTurnCount) =>
    Effect.gen(function* () {
      const audienceCeiling = yield* currentReadAudienceCeiling;
      const row = yield* getFullThreadDiffContextRow({
        threadId,
        checkpointTurnCount: toTurnCount,
        audienceCeiling,
      }).pipe(
        Effect.mapError(
          toPersistenceSqlOrDecodeError(
            "ProjectionSnapshotQuery.getFullThreadDiffContext:query",
            "ProjectionSnapshotQuery.getFullThreadDiffContext:decodeRow",
          ),
        ),
      );
      if (Option.isNone(row)) {
        return Option.none<ProjectionFullThreadDiffContext>();
      }

      return Option.some({
        threadId: row.value.threadId,
        projectId: row.value.projectId,
        workspaceRoot: row.value.workspaceRoot,
        worktreePath: row.value.worktreePath,
        latestCheckpointTurnCount: row.value.latestCheckpointTurnCount ?? 0,
        toCheckpointRef: row.value.toCheckpointRef,
      });
    });

  const getThreadShellById: ProjectionSnapshotQueryShape["getThreadShellById"] = (threadId) =>
    Effect.gen(function* () {
      const audienceCeiling = yield* currentReadAudienceCeiling;
      const threadRow = yield* getActiveThreadRowById({ threadId, audienceCeiling }).pipe(
        Effect.mapError(
          toPersistenceSqlOrDecodeError(
            "ProjectionSnapshotQuery.getThreadShellById:getThread:query",
            "ProjectionSnapshotQuery.getThreadShellById:getThread:decodeRow",
          ),
        ),
      );
      if (Option.isNone(threadRow)) {
        return Option.none<OrchestrationThreadShell>();
      }
      const readableThreadIds = yield* loadReadableThreadIds(
        audienceCeiling,
        "ProjectionSnapshotQuery.getThreadShellById:listThreadAudiences",
      );

      const [latestTurnRow, sessionRow] = yield* Effect.all([
        getLatestTurnRowByThread({ threadId }).pipe(
          Effect.mapError(
            toPersistenceSqlOrDecodeError(
              "ProjectionSnapshotQuery.getThreadShellById:getLatestTurn:query",
              "ProjectionSnapshotQuery.getThreadShellById:getLatestTurn:decodeRow",
            ),
          ),
        ),
        getThreadSessionRowByThread({ threadId }).pipe(
          Effect.mapError(
            toPersistenceSqlOrDecodeError(
              "ProjectionSnapshotQuery.getThreadShellById:getSession:query",
              "ProjectionSnapshotQuery.getThreadShellById:getSession:decodeRow",
            ),
          ),
        ),
      ]);

      return Option.some({
        id: threadRow.value.threadId,
        projectId: threadRow.value.projectId,
        dataAudience: threadRow.value.dataAudience,
        title: threadRow.value.title,
        modelSelection: threadRow.value.modelSelection,
        runtimeMode: threadRow.value.runtimeMode,
        interactionMode: threadRow.value.interactionMode,
        branch: threadRow.value.branch,
        worktreePath: threadRow.value.worktreePath,
        worktreeRemovable: threadRow.value.worktreeRemovable > 0,
        worktreeRemovalPath: threadRow.value.worktreeRemovalPath,
        latestTurn: Option.isSome(latestTurnRow)
          ? mapLatestTurn(latestTurnRow.value, readableThreadIds)
          : null,
        createdAt: threadRow.value.createdAt,
        updatedAt: threadRow.value.updatedAt,
        archivedAt: threadRow.value.archivedAt,
        session: Option.isSome(sessionRow) ? mapSessionRow(sessionRow.value) : null,
        latestUserMessageAt: threadRow.value.latestUserMessageAt,
        hasPendingApprovals: threadRow.value.pendingApprovalCount > 0,
        hasPendingUserInput: threadRow.value.pendingUserInputCount > 0,
        hasActionableProposedPlan: threadRow.value.hasActionableProposedPlan > 0,
        ...mapParentThreadReference(threadRow.value, readableThreadIds),
      } satisfies OrchestrationThreadShell);
    });

  const getThreadShellByIdIncludingArchived: ProjectionSnapshotQueryShape["getThreadShellByIdIncludingArchived"] =
    (threadId) =>
      Effect.gen(function* () {
        const audienceCeiling = yield* currentReadAudienceCeiling;
        const threadRow = yield* getNonDeletedThreadRowById({ threadId, audienceCeiling }).pipe(
          Effect.mapError(
            toPersistenceSqlOrDecodeError(
              "ProjectionSnapshotQuery.getThreadShellByIdIncludingArchived:getThread:query",
              "ProjectionSnapshotQuery.getThreadShellByIdIncludingArchived:getThread:decodeRow",
            ),
          ),
        );
        if (Option.isNone(threadRow)) {
          return Option.none<OrchestrationThreadShell>();
        }
        const readableThreadIds = yield* loadReadableThreadIds(
          audienceCeiling,
          "ProjectionSnapshotQuery.getThreadShellByIdIncludingArchived:listThreadAudiences",
        );

        const [latestTurnRow, sessionRow] = yield* Effect.all([
          getLatestTurnRowByThread({ threadId }).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getThreadShellByIdIncludingArchived:getLatestTurn:query",
                "ProjectionSnapshotQuery.getThreadShellByIdIncludingArchived:getLatestTurn:decodeRow",
              ),
            ),
          ),
          getThreadSessionRowByThread({ threadId }).pipe(
            Effect.mapError(
              toPersistenceSqlOrDecodeError(
                "ProjectionSnapshotQuery.getThreadShellByIdIncludingArchived:getSession:query",
                "ProjectionSnapshotQuery.getThreadShellByIdIncludingArchived:getSession:decodeRow",
              ),
            ),
          ),
        ]);

        return Option.some({
          id: threadRow.value.threadId,
          projectId: threadRow.value.projectId,
          dataAudience: threadRow.value.dataAudience,
          title: threadRow.value.title,
          modelSelection: threadRow.value.modelSelection,
          runtimeMode: threadRow.value.runtimeMode,
          interactionMode: threadRow.value.interactionMode,
          branch: threadRow.value.branch,
          worktreePath: threadRow.value.worktreePath,
          worktreeRemovable: threadRow.value.worktreeRemovable > 0,
          worktreeRemovalPath: threadRow.value.worktreeRemovalPath,
          latestTurn: Option.isSome(latestTurnRow)
            ? mapLatestTurn(latestTurnRow.value, readableThreadIds)
            : null,
          createdAt: threadRow.value.createdAt,
          updatedAt: threadRow.value.updatedAt,
          archivedAt: threadRow.value.archivedAt,
          session: Option.isSome(sessionRow) ? mapSessionRow(sessionRow.value) : null,
          latestUserMessageAt: threadRow.value.latestUserMessageAt,
          hasPendingApprovals: threadRow.value.pendingApprovalCount > 0,
          hasPendingUserInput: threadRow.value.pendingUserInputCount > 0,
          hasActionableProposedPlan: threadRow.value.hasActionableProposedPlan > 0,
          ...mapParentThreadReference(threadRow.value, readableThreadIds),
        } satisfies OrchestrationThreadShell);
      });

  const getThreadShellSnapshotByIdIncludingArchived: ProjectionSnapshotQueryShape["getThreadShellSnapshotByIdIncludingArchived"] =
    (threadId) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const thread = yield* getThreadShellByIdIncludingArchived(threadId);
            const { snapshotSequence } = yield* getSnapshotSequence();
            return { snapshotSequence, thread };
          }),
        )
        .pipe(
          Effect.mapError((error) =>
            isPersistenceError(error)
              ? error
              : toPersistenceSqlError(
                  "ProjectionSnapshotQuery.getThreadShellSnapshotByIdIncludingArchived:transaction",
                )(error),
          ),
        );

  const getThreadDetailById: ProjectionSnapshotQueryShape["getThreadDetailById"] = (threadId) =>
    Effect.gen(function* () {
      const audienceCeiling = yield* currentReadAudienceCeiling;
      const threadRow = yield* getNonDeletedThreadRowById({ threadId, audienceCeiling }).pipe(
        Effect.mapError(
          toPersistenceSqlOrDecodeError(
            "ProjectionSnapshotQuery.getThreadDetailById:getThread:query",
            "ProjectionSnapshotQuery.getThreadDetailById:getThread:decodeRow",
          ),
        ),
      );
      if (Option.isNone(threadRow)) {
        return Option.none<OrchestrationThread>();
      }
      const readableThreadIds = yield* loadReadableThreadIds(
        audienceCeiling,
        "ProjectionSnapshotQuery.getThreadDetailById:listThreadAudiences",
      );

      const [
        messageRows,
        proposedPlanRows,
        activityRows,
        checkpointRows,
        latestTurnRow,
        turnRows,
        sessionRow,
      ] = yield* Effect.all([
        listThreadMessageRowsByThread({ threadId }).pipe(
          Effect.mapError(
            toPersistenceSqlOrDecodeError(
              "ProjectionSnapshotQuery.getThreadDetailById:listMessages:query",
              "ProjectionSnapshotQuery.getThreadDetailById:listMessages:decodeRows",
            ),
          ),
        ),
        listThreadProposedPlanRowsByThread({ threadId }).pipe(
          Effect.mapError(
            toPersistenceSqlOrDecodeError(
              "ProjectionSnapshotQuery.getThreadDetailById:listPlans:query",
              "ProjectionSnapshotQuery.getThreadDetailById:listPlans:decodeRows",
            ),
          ),
        ),
        listThreadActivityRowsByThread({ threadId }).pipe(
          Effect.mapError(
            toPersistenceSqlOrDecodeError(
              "ProjectionSnapshotQuery.getThreadDetailById:listActivities:query",
              "ProjectionSnapshotQuery.getThreadDetailById:listActivities:decodeRows",
            ),
          ),
        ),
        listDetailCheckpointRowsByThread({ threadId }).pipe(
          Effect.mapError(
            toPersistenceSqlOrDecodeError(
              "ProjectionSnapshotQuery.getThreadDetailById:listCheckpoints:query",
              "ProjectionSnapshotQuery.getThreadDetailById:listCheckpoints:decodeRows",
            ),
          ),
        ),
        getLatestTurnRowByThread({ threadId }).pipe(
          Effect.mapError(
            toPersistenceSqlOrDecodeError(
              "ProjectionSnapshotQuery.getThreadDetailById:getLatestTurn:query",
              "ProjectionSnapshotQuery.getThreadDetailById:getLatestTurn:decodeRow",
            ),
          ),
        ),
        listTurnRowsByThread({ threadId }).pipe(
          Effect.mapError(
            toPersistenceSqlOrDecodeError(
              "ProjectionSnapshotQuery.getThreadDetailById:listTurns:query",
              "ProjectionSnapshotQuery.getThreadDetailById:listTurns:decodeRows",
            ),
          ),
        ),
        getThreadSessionRowByThread({ threadId }).pipe(
          Effect.mapError(
            toPersistenceSqlOrDecodeError(
              "ProjectionSnapshotQuery.getThreadDetailById:getSession:query",
              "ProjectionSnapshotQuery.getThreadDetailById:getSession:decodeRow",
            ),
          ),
        ),
      ]);

      const thread = {
        id: threadRow.value.threadId,
        projectId: threadRow.value.projectId,
        dataAudience: threadRow.value.dataAudience,
        title: threadRow.value.title,
        modelSelection: threadRow.value.modelSelection,
        runtimeMode: threadRow.value.runtimeMode,
        interactionMode: threadRow.value.interactionMode,
        branch: threadRow.value.branch,
        worktreePath: threadRow.value.worktreePath,
        worktreeRemovable: threadRow.value.worktreeRemovable > 0,
        worktreeRemovalPath: threadRow.value.worktreeRemovalPath,
        latestTurn: Option.isSome(latestTurnRow)
          ? mapLatestTurn(latestTurnRow.value, readableThreadIds)
          : null,
        turns: turnRows.map((row) => mapLatestTurn(row, readableThreadIds)),
        createdAt: threadRow.value.createdAt,
        updatedAt: threadRow.value.updatedAt,
        archivedAt: threadRow.value.archivedAt,
        ...mapParentThreadReference(threadRow.value, readableThreadIds),
        deletedAt: null,
        messages: messageRows.map((row) => {
          const message = {
            id: row.messageId,
            role: row.role,
            text: row.text,
            turnId: row.turnId,
            streaming: row.isStreaming === 1,
            createdAt: row.createdAt,
            updatedAt: row.updatedAt,
          };
          if (row.attachments !== null) {
            return Object.assign(message, { attachments: row.attachments });
          }
          return message;
        }),
        proposedPlans: proposedPlanRows.map((row) => mapProposedPlanRow(row, readableThreadIds)),
        activities: activityRows.map((row) => {
          const references = mapActivityThreadReferences(row, readableThreadIds);
          const activity = {
            id: row.activityId,
            tone: row.tone,
            kind: row.kind,
            summary: references.summary,
            payload: references.payload,
            turnId: row.turnId,
            createdAt: row.createdAt,
          };
          if (row.sequence !== null) {
            return Object.assign(activity, { sequence: row.sequence });
          }
          return activity;
        }),
        checkpoints: checkpointRows.map((row) => ({
          turnId: row.turnId,
          checkpointTurnCount: row.checkpointTurnCount,
          checkpointRef: row.checkpointRef,
          status: row.status,
          files: row.files,
          assistantMessageId: row.assistantMessageId,
          completedAt: row.completedAt,
        })),
        session: Option.isSome(sessionRow) ? mapSessionRow(sessionRow.value) : null,
      };

      return Option.some(
        yield* decodeThread(thread).pipe(
          Effect.mapError(
            toPersistenceDecodeError("ProjectionSnapshotQuery.getThreadDetailById:decodeThread"),
          ),
        ),
      );
    });

  const getThreadDetailSnapshot: ProjectionSnapshotQueryShape["getThreadDetailSnapshot"] = (
    threadId,
  ) =>
    // Read the thread detail and the snapshot sequence within a single
    // transaction so the sequence is consistent with the returned state; a
    // projector update landing between two separate reads could otherwise return
    // a sequence ahead of the thread detail, causing the client to resume from
    // too far and drop events.
    sql
      .withTransaction(
        Effect.gen(function* () {
          const thread = yield* getThreadDetailById(threadId);
          if (Option.isNone(thread)) {
            return Option.none<OrchestrationThreadDetailSnapshot>();
          }
          const { snapshotSequence } = yield* getSnapshotSequence();
          return Option.some({ snapshotSequence, thread: thread.value });
        }),
      )
      .pipe(
        Effect.mapError((error) =>
          isPersistenceError(error)
            ? error
            : toPersistenceSqlError("ProjectionSnapshotQuery.getThreadDetailSnapshot:transaction")(
                error,
              ),
        ),
      );

  return {
    getCommandReadModel,
    getSnapshot,
    getShellSnapshot,
    getArchivedShellSnapshot,
    getSnapshotSequence,
    getCounts,
    getActiveProjectByWorkspaceRoot,
    getProjectShellById,
    getFirstActiveThreadIdByProjectId,
    getThreadCheckpointContext,
    getFullThreadDiffContext,
    getThreadShellById,
    getThreadShellByIdIncludingArchived,
    getThreadShellSnapshotByIdIncludingArchived,
    getThreadDetailById,
    getThreadDetailSnapshot,
  } satisfies ProjectionSnapshotQueryShape;
});

export const OrchestrationProjectionSnapshotQueryLive = Layer.effect(
  ProjectionSnapshotQuery,
  makeProjectionSnapshotQuery,
);
