import {
  ApprovalRequestId,
  CheckpointRef,
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  TurnId,
  type OrchestrationCommand,
  type OrchestrationReadModel,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { describe, expect, it } from "@effect/vitest";

import {
  audienceBoundSystemDispatchAuthority,
  authorizeOrchestrationCommandMutation,
  sessionDispatchAuthority,
  trustedSystemDispatchAuthority,
} from "../commandAudienceGuard.ts";

const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asMessageId = (value: string): MessageId => MessageId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);
const asCheckpointRef = (value: string): CheckpointRef => CheckpointRef.make(value);
const asApprovalRequestId = (value: string): ApprovalRequestId => ApprovalRequestId.make(value);

const createdAt = "2026-01-01T00:00:00.000Z";
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5-codex",
} as const;
const privateProjectId = asProjectId("project-private-command-guard");
const factoryProjectId = asProjectId("project-factory-command-guard");
const privateThreadId = ThreadId.make("thread-private-command-guard");
const archivedPrivateThreadId = ThreadId.make("thread-private-command-guard-archived");
const factoryThreadId = ThreadId.make("thread-factory-command-guard");

function readModel(): OrchestrationReadModel {
  return {
    snapshotSequence: 4,
    updatedAt: createdAt,
    projects: [
      {
        id: privateProjectId,
        title: "Private",
        workspaceRoot: "/tmp/private-command-guard",
        dataAudience: "private",
        defaultModelSelection: modelSelection,
        scripts: [],
        createdAt,
        updatedAt: createdAt,
        deletedAt: null,
      },
      {
        id: factoryProjectId,
        title: "Factory",
        workspaceRoot: "/tmp/factory-command-guard",
        dataAudience: "factory",
        defaultModelSelection: modelSelection,
        scripts: [],
        createdAt,
        updatedAt: createdAt,
        deletedAt: null,
      },
    ],
    threads: [
      {
        id: privateThreadId,
        projectId: privateProjectId,
        dataAudience: "private",
        title: "Private Thread",
        modelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: null,
        latestTurn: null,
        createdAt,
        updatedAt: createdAt,
        archivedAt: null,
        deletedAt: null,
        messages: [],
        turns: [],
        proposedPlans: [],
        activities: [],
        checkpoints: [],
        session: null,
      },
      {
        id: archivedPrivateThreadId,
        projectId: privateProjectId,
        dataAudience: "private",
        title: "Archived Private Thread",
        modelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: null,
        latestTurn: null,
        createdAt,
        updatedAt: createdAt,
        archivedAt: createdAt,
        deletedAt: null,
        messages: [],
        turns: [],
        proposedPlans: [],
        activities: [],
        checkpoints: [],
        session: null,
      },
      {
        id: factoryThreadId,
        projectId: factoryProjectId,
        dataAudience: "factory",
        title: "Factory Thread",
        modelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: null,
        latestTurn: null,
        createdAt,
        updatedAt: createdAt,
        archivedAt: null,
        deletedAt: null,
        messages: [],
        turns: [],
        proposedPlans: [
          {
            id: "factory-plan",
            turnId: null,
            planMarkdown: "factory plan",
            implementedAt: null,
            implementationThreadId: null,
            createdAt,
            updatedAt: createdAt,
          },
        ],
        activities: [],
        checkpoints: [],
        session: null,
      },
    ],
  };
}

const factoryAuthority = sessionDispatchAuthority({
  subject: "factory-test-session",
  audienceCeiling: "factory",
});
const privateAuthority = sessionDispatchAuthority({
  subject: "private-test-session",
  audienceCeiling: "private",
});
const trustedAuthority = trustedSystemDispatchAuthority(
  "orchestration-command-audience-guard-test-seed",
);

function authorizeEffect(
  command: OrchestrationCommand,
  authority = factoryAuthority,
  model = readModel(),
) {
  return authorizeOrchestrationCommandMutation({ command, readModel: model, authority });
}

function authorizeFailureEffect(
  command: OrchestrationCommand,
  authority = factoryAuthority,
  model = readModel(),
) {
  return Effect.exit(authorizeEffect(command, authority, model));
}

describe("authorizeOrchestrationCommandMutation", () => {
  it.effect("requires explicit dispatch authority", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        authorizeOrchestrationCommandMutation({
          command: {
            type: "project.create",
            commandId: CommandId.make("cmd-project-missing-authority"),
            projectId: asProjectId("project-missing-authority"),
            title: "Missing Authority",
            workspaceRoot: "/tmp/project-missing-authority",
            defaultModelSelection: modelSelection,
            createdAt,
          },
          readModel: readModel(),
          authority: undefined,
        }),
      );
      expect(String(exit)).toContain("dispatch authority");
    }),
  );

  it.effect("binds project creation audience to the caller ceiling", () =>
    Effect.gen(function* () {
      const factoryCommand = yield* authorizeEffect({
        type: "project.create",
        commandId: CommandId.make("cmd-project-factory-create-binding"),
        projectId: asProjectId("project-factory-create-binding"),
        title: "Factory Create Binding",
        workspaceRoot: "/tmp/project-factory-create-binding",
        defaultModelSelection: modelSelection,
        createdAt,
      });
      expect(factoryCommand).toMatchObject({ type: "project.create", dataAudience: "factory" });

      const privateCommand = yield* authorizeEffect(
        {
          type: "project.create",
          commandId: CommandId.make("cmd-project-private-create-binding"),
          projectId: asProjectId("project-private-create-binding"),
          title: "Private Create Binding",
          workspaceRoot: "/tmp/project-private-create-binding",
          defaultModelSelection: modelSelection,
          createdAt,
        },
        privateAuthority,
      );
      expect(privateCommand).toMatchObject({ type: "project.create", dataAudience: "private" });
    }),
  );

  it.effect("lets trusted-system and private-ceiling authorities mutate any known audience", () =>
    Effect.gen(function* () {
      const command: OrchestrationCommand = {
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-trusted-private-thread-meta"),
        threadId: privateThreadId,
        title: "Trusted update",
      };
      expect(yield* authorizeEffect(command, trustedAuthority)).toMatchObject(command);
      expect(yield* authorizeEffect(command, privateAuthority)).toMatchObject(command);
    }),
  );

  it.effect("keeps provider-internal system authority bound to its source thread audience", () =>
    Effect.gen(function* () {
      const factorySystemAuthority = audienceBoundSystemDispatchAuthority({
        reason: "factory-provider-test",
        sourceThreadId: factoryThreadId,
        dataAudience: "factory",
      });
      const privateSystemAuthority = audienceBoundSystemDispatchAuthority({
        reason: "private-provider-test",
        sourceThreadId: privateThreadId,
        dataAudience: "private",
      });

      const privateExit = yield* authorizeFailureEffect(
        {
          type: "thread.meta.update",
          commandId: CommandId.make("cmd-factory-system-private-target"),
          threadId: privateThreadId,
          title: "must not update",
        },
        factorySystemAuthority,
      );
      const factoryExit = yield* authorizeFailureEffect(
        {
          type: "thread.meta.update",
          commandId: CommandId.make("cmd-private-system-factory-target"),
          threadId: factoryThreadId,
          title: "must not update",
        },
        privateSystemAuthority,
      );

      expect(String(privateExit)).toContain("Thread 'thread-private-command-guard' does not exist");
      expect(String(factoryExit)).toContain("Thread 'thread-factory-command-guard' does not exist");
    }),
  );

  it.effect("allows audience-bound lifecycle metadata cleanup on its deleted source thread", () =>
    Effect.gen(function* () {
      const model = readModel();
      const deletedModel = {
        ...model,
        threads: model.threads.map((thread) =>
          thread.id === factoryThreadId ? { ...thread, deletedAt: createdAt } : thread,
        ),
      };
      const authority = audienceBoundSystemDispatchAuthority({
        reason: "thread-deletion-lifecycle-test",
        sourceThreadId: factoryThreadId,
        dataAudience: "factory",
      });
      const command: OrchestrationCommand = {
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-factory-deleted-worktree-metadata-clear"),
        threadId: factoryThreadId,
        worktreePath: null,
        worktreeRemovable: false,
        worktreeRemovalPath: null,
      };

      expect(yield* authorizeEffect(command, authority, deletedModel)).toMatchObject(command);
    }),
  );

  it.effect("rejects factory-ceiling mutations against private targets", () =>
    Effect.gen(function* () {
      const forbiddenCommands: ReadonlyArray<readonly [string, OrchestrationCommand]> = [
        [
          "project.meta.update",
          {
            type: "project.meta.update",
            commandId: CommandId.make("cmd-private-guard-project-meta"),
            projectId: privateProjectId,
            title: "Leaked title",
          },
        ],
        [
          "project.data-audience.set",
          {
            type: "project.data-audience.set",
            commandId: CommandId.make("cmd-private-guard-project-audience"),
            projectId: privateProjectId,
            expectedWorkspaceRoot: "/tmp/private-command-guard",
            actor: "factory-test",
            occurredAt: createdAt,
          },
        ],
        [
          "project.delete",
          {
            type: "project.delete",
            commandId: CommandId.make("cmd-private-guard-project-delete"),
            projectId: privateProjectId,
            force: true,
          },
        ],
        [
          "thread.create",
          {
            type: "thread.create",
            commandId: CommandId.make("cmd-private-guard-thread-create"),
            threadId: ThreadId.make("thread-private-guard-created-by-factory"),
            projectId: privateProjectId,
            title: "Forbidden child",
            modelSelection,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
          },
        ],
        [
          "thread.delete",
          {
            type: "thread.delete",
            commandId: CommandId.make("cmd-private-guard-thread-delete"),
            threadId: privateThreadId,
          },
        ],
        [
          "thread.archive",
          {
            type: "thread.archive",
            commandId: CommandId.make("cmd-private-guard-thread-archive"),
            threadId: privateThreadId,
          },
        ],
        [
          "thread.unarchive",
          {
            type: "thread.unarchive",
            commandId: CommandId.make("cmd-private-guard-thread-unarchive"),
            threadId: archivedPrivateThreadId,
          },
        ],
        [
          "thread.meta.update",
          {
            type: "thread.meta.update",
            commandId: CommandId.make("cmd-private-guard-thread-meta"),
            threadId: privateThreadId,
            title: "Leaked thread title",
          },
        ],
        [
          "thread.runtime-mode.set",
          {
            type: "thread.runtime-mode.set",
            commandId: CommandId.make("cmd-private-guard-thread-runtime"),
            threadId: privateThreadId,
            runtimeMode: "approval-required",
            createdAt,
          },
        ],
        [
          "thread.interaction-mode.set",
          {
            type: "thread.interaction-mode.set",
            commandId: CommandId.make("cmd-private-guard-thread-interaction"),
            threadId: privateThreadId,
            interactionMode: "plan",
            createdAt,
          },
        ],
        [
          "thread.parent.set",
          {
            type: "thread.parent.set",
            commandId: CommandId.make("cmd-private-guard-thread-parent"),
            threadId: privateThreadId,
            parentThreadId: archivedPrivateThreadId,
            createdAt,
          },
        ],
        [
          "thread.turn.start",
          {
            type: "thread.turn.start",
            commandId: CommandId.make("cmd-private-guard-turn-start"),
            threadId: privateThreadId,
            message: {
              messageId: asMessageId("msg-private-guard-turn-start"),
              role: "user",
              text: "must not dispatch",
              attachments: [],
            },
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            runtimeMode: "full-access",
            createdAt,
          },
        ],
        [
          "thread.turn.interrupt",
          {
            type: "thread.turn.interrupt",
            commandId: CommandId.make("cmd-private-guard-turn-interrupt"),
            threadId: privateThreadId,
            createdAt,
          },
        ],
        [
          "thread.approval.respond",
          {
            type: "thread.approval.respond",
            commandId: CommandId.make("cmd-private-guard-approval"),
            threadId: privateThreadId,
            requestId: asApprovalRequestId("approval-private-guard"),
            decision: "accept",
            createdAt,
          },
        ],
        [
          "thread.user-input.respond",
          {
            type: "thread.user-input.respond",
            commandId: CommandId.make("cmd-private-guard-user-input"),
            threadId: privateThreadId,
            requestId: asApprovalRequestId("input-private-guard"),
            answers: {},
            createdAt,
          },
        ],
        [
          "thread.checkpoint.revert",
          {
            type: "thread.checkpoint.revert",
            commandId: CommandId.make("cmd-private-guard-checkpoint-revert"),
            threadId: privateThreadId,
            turnCount: 0,
            createdAt,
          },
        ],
        [
          "thread.session.stop",
          {
            type: "thread.session.stop",
            commandId: CommandId.make("cmd-private-guard-session-stop"),
            threadId: privateThreadId,
            createdAt,
          },
        ],
        [
          "thread.session.set",
          {
            type: "thread.session.set",
            commandId: CommandId.make("cmd-private-guard-session-set"),
            threadId: privateThreadId,
            session: {
              threadId: privateThreadId,
              status: "running",
              providerName: "codex",
              providerInstanceId: ProviderInstanceId.make("codex"),
              runtimeMode: "full-access",
              activeTurnId: asTurnId("turn-private-guard-session"),
              lastError: null,
              updatedAt: createdAt,
            },
            createdAt,
          },
        ],
        [
          "thread.message.assistant.delta",
          {
            type: "thread.message.assistant.delta",
            commandId: CommandId.make("cmd-private-guard-assistant-delta"),
            threadId: privateThreadId,
            messageId: asMessageId("msg-private-guard-assistant"),
            delta: "secret",
            createdAt,
          },
        ],
        [
          "thread.message.assistant.complete",
          {
            type: "thread.message.assistant.complete",
            commandId: CommandId.make("cmd-private-guard-assistant-complete"),
            threadId: privateThreadId,
            messageId: asMessageId("msg-private-guard-assistant"),
            createdAt,
          },
        ],
        [
          "thread.proposed-plan.upsert",
          {
            type: "thread.proposed-plan.upsert",
            commandId: CommandId.make("cmd-private-guard-plan-upsert"),
            threadId: privateThreadId,
            proposedPlan: {
              id: "plan-private-guard",
              turnId: null,
              planMarkdown: "secret plan",
              implementedAt: null,
              implementationThreadId: null,
              createdAt,
              updatedAt: createdAt,
            },
            createdAt,
          },
        ],
        [
          "thread.turn.diff.complete",
          {
            type: "thread.turn.diff.complete",
            commandId: CommandId.make("cmd-private-guard-diff-complete"),
            threadId: privateThreadId,
            turnId: asTurnId("turn-private-guard-diff"),
            completedAt: createdAt,
            checkpointRef: asCheckpointRef("checkpoint-private-guard"),
            status: "ready",
            files: [],
            checkpointTurnCount: 0,
            createdAt,
          },
        ],
        [
          "thread.turn.effective-model.set",
          {
            type: "thread.turn.effective-model.set",
            commandId: CommandId.make("cmd-private-guard-effective-model"),
            threadId: privateThreadId,
            turnId: asTurnId("turn-private-guard-model"),
            effectiveModel: "gpt-5-codex",
            createdAt,
          },
        ],
        [
          "thread.activity.append",
          {
            type: "thread.activity.append",
            commandId: CommandId.make("cmd-private-guard-activity"),
            threadId: privateThreadId,
            activity: {
              id: EventId.make("activity-private-guard"),
              tone: "info",
              kind: "private.guard",
              summary: "secret activity",
              payload: {},
              turnId: null,
              createdAt,
            },
            createdAt,
          },
        ],
        [
          "thread.revert.complete",
          {
            type: "thread.revert.complete",
            commandId: CommandId.make("cmd-private-guard-revert-complete"),
            threadId: privateThreadId,
            turnCount: 0,
            createdAt,
          },
        ],
      ];

      for (const [name, command] of forbiddenCommands) {
        const exit = yield* authorizeFailureEffect(command);
        expect(String(exit), name).toContain("does not exist");
      }
    }),
  );

  it.effect("rejects create commands that collide with hidden private ids", () =>
    Effect.gen(function* () {
      const projectExit = yield* authorizeFailureEffect({
        type: "project.create",
        commandId: CommandId.make("cmd-private-guard-project-create-id-collision"),
        projectId: privateProjectId,
        title: "Factory collision",
        workspaceRoot: "/tmp/factory-project-create-id-collision",
        defaultModelSelection: modelSelection,
        createdAt,
      });
      expect(String(projectExit)).toContain(
        "Project 'project-private-command-guard' does not exist",
      );

      const workspaceRootExit = yield* authorizeFailureEffect({
        type: "project.create",
        commandId: CommandId.make("cmd-private-guard-project-create-root-collision"),
        projectId: asProjectId("project-factory-root-collision"),
        title: "Factory root collision",
        workspaceRoot: "/tmp/private-command-guard",
        defaultModelSelection: modelSelection,
        createdAt,
      });
      expect(String(workspaceRootExit)).toContain(
        "Project 'project-factory-root-collision' does not exist",
      );

      const threadExit = yield* authorizeFailureEffect({
        type: "thread.create",
        commandId: CommandId.make("cmd-private-guard-thread-create-id-collision"),
        threadId: privateThreadId,
        projectId: factoryProjectId,
        title: "Factory thread collision",
        modelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: null,
        createdAt,
      });
      expect(String(threadExit)).toContain("Thread 'thread-private-command-guard' does not exist");
    }),
  );

  it.effect("rejects project creates whose roots contain or are contained by hidden roots", () =>
    Effect.gen(function* () {
      const collisions = [
        {
          commandId: "cmd-private-guard-project-create-root-nested",
          projectId: "project-factory-root-nested",
          workspaceRoot: "/tmp/private-command-guard/nested",
        },
        {
          commandId: "cmd-private-guard-project-create-root-parent",
          projectId: "project-factory-root-parent",
          workspaceRoot: "/tmp",
        },
      ] as const;

      for (const collision of collisions) {
        const exit = yield* authorizeFailureEffect({
          type: "project.create",
          commandId: CommandId.make(collision.commandId),
          projectId: asProjectId(collision.projectId),
          title: "Factory overlapping root",
          workspaceRoot: collision.workspaceRoot,
          defaultModelSelection: modelSelection,
          createdAt,
        });
        const rendered = String(exit);
        expect(rendered).toContain(`Project '${collision.projectId}' does not exist`);
        expect(rendered).not.toContain("project-private-command-guard");
        expect(rendered).not.toContain("/tmp/private-command-guard");
      }
    }),
  );

  it.effect("rejects project creates that overlap a hidden thread worktree", () =>
    Effect.gen(function* () {
      const hiddenWorktreePath = "/tmp/private-command-guard-thread-worktree";
      const model = readModel();
      const modelWithHiddenWorktree = {
        ...model,
        threads: model.threads.map((thread) =>
          thread.id === privateThreadId
            ? {
                ...thread,
                worktreePath: hiddenWorktreePath,
                worktreeRemovalPath: hiddenWorktreePath,
              }
            : thread,
        ),
      };
      const projectId = asProjectId("project-factory-hidden-worktree-collision");
      const exit = yield* authorizeFailureEffect(
        {
          type: "project.create",
          commandId: CommandId.make("cmd-private-guard-project-create-worktree-collision"),
          projectId,
          title: "Factory hidden worktree collision",
          workspaceRoot: hiddenWorktreePath,
          defaultModelSelection: modelSelection,
          createdAt,
        },
        factoryAuthority,
        modelWithHiddenWorktree,
      );
      const rendered = String(exit);
      expect(rendered).toContain(`Project '${projectId}' does not exist`);
      expect(rendered).not.toContain("thread-private-command-guard");
      expect(rendered).not.toContain(hiddenWorktreePath);
    }),
  );

  it.effect("rejects direct thread.create worktree metadata from factory sessions", () =>
    Effect.gen(function* () {
      const exit = yield* authorizeFailureEffect({
        type: "thread.create",
        commandId: CommandId.make("cmd-private-guard-thread-create-worktree-path"),
        threadId: ThreadId.make("thread-factory-command-guard-direct-worktree-path"),
        projectId: factoryProjectId,
        title: "Factory direct thread with private cwd",
        modelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: "/tmp/private-command-guard",
        worktreeRemovable: true,
        worktreeRemovalPath: "/tmp/private-command-guard",
        createdAt,
      });
      const rendered = String(exit);
      expect(rendered).toContain("Project 'project-factory-command-guard' does not exist");
      expect(rendered).not.toContain("project-private-command-guard");
      expect(rendered).not.toContain("/tmp/private-command-guard");
    }),
  );

  it.effect("rejects direct thread.meta.update worktree metadata from factory sessions", () =>
    Effect.gen(function* () {
      const exit = yield* authorizeFailureEffect({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-private-guard-thread-meta-worktree-path"),
        threadId: factoryThreadId,
        worktreePath: "/tmp/private-command-guard",
        worktreeRemovalPath: "/tmp/private-command-guard",
        worktreeRemovable: true,
      });
      const rendered = String(exit);
      expect(rendered).toContain("Thread 'thread-factory-command-guard' does not exist");
      expect(rendered).not.toContain("project-private-command-guard");
      expect(rendered).not.toContain("/tmp/private-command-guard");
    }),
  );

  it.effect("masks hidden project root collisions on project meta update", () =>
    Effect.gen(function* () {
      const exit = yield* authorizeFailureEffect({
        type: "project.meta.update",
        commandId: CommandId.make("cmd-private-guard-project-meta-root-collision"),
        projectId: factoryProjectId,
        workspaceRoot: "/tmp/private-command-guard/",
      });
      const rendered = String(exit);
      expect(rendered).toContain("Project 'project-factory-command-guard' does not exist");
      expect(rendered).not.toContain("project-private-command-guard");
      expect(rendered).not.toContain("/tmp/private-command-guard");
    }),
  );

  it.effect(
    "rejects prepareWorktree bootstraps on existing threads before filesystem side effects",
    () =>
      Effect.gen(function* () {
        const exit = yield* authorizeFailureEffect({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-private-guard-existing-thread-prepare-worktree"),
          threadId: factoryThreadId,
          message: {
            messageId: asMessageId("msg-private-guard-existing-thread-prepare-worktree"),
            role: "user",
            text: "must not create a private worktree",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          bootstrap: {
            prepareWorktree: {
              projectCwd: "/tmp/private-command-guard",
              baseBranch: "main",
              branch: "private-side-effect",
            },
          },
          createdAt,
        });
        const rendered = String(exit);
        expect(rendered).toContain("Thread 'thread-factory-command-guard' does not exist");
        expect(rendered).not.toContain("project-private-command-guard");
        expect(rendered).not.toContain("/tmp/private-command-guard");
      }),
  );

  it.effect("rejects an unauthorized prepare-only setup retry on an existing thread", () =>
    Effect.gen(function* () {
      const exit = yield* authorizeFailureEffect(
        {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-private-guard-existing-thread-setup-script"),
          threadId: factoryThreadId,
          message: {
            messageId: asMessageId("msg-private-guard-existing-thread-setup-script"),
            role: "user",
            text: "must not launch setup from a bare bootstrap",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          bootstrap: {
            prepareWorktree: {
              projectCwd: "/tmp/factory-command-guard",
              baseBranch: "main",
              branch: "factory-unauthorized-prepare-only-retry",
            },
            runSetupScript: true,
          },
          createdAt,
        },
        factoryAuthority,
        {
          ...readModel(),
          threads: readModel().threads.map((thread) =>
            thread.id === factoryThreadId
              ? { ...thread, worktreePath: "/tmp/factory-command-guard-worktree" }
              : thread,
          ),
        },
      );
      const rendered = String(exit);
      expect(rendered).toContain("Thread 'thread-factory-command-guard' does not exist");
      expect(rendered).not.toContain("project-private-command-guard");
      expect(rendered).not.toContain("/tmp/private-command-guard");
    }),
  );

  it.effect("rejects setup-script replays that would reuse an existing hidden worktree path", () =>
    Effect.gen(function* () {
      const model = readModel();
      const exit = yield* authorizeFailureEffect(
        {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-private-guard-existing-thread-reused-setup-path"),
          threadId: factoryThreadId,
          message: {
            messageId: asMessageId("msg-private-guard-existing-thread-reused-setup-path"),
            role: "user",
            text: "must not launch setup from reused hidden metadata",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          bootstrap: {
            createThread: {
              projectId: factoryProjectId,
              title: "Factory Thread",
              modelSelection,
              interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
              runtimeMode: "full-access",
              branch: null,
              worktreePath: null,
              createdAt,
            },
            prepareWorktree: {
              projectCwd: "/tmp/factory-command-guard",
              baseBranch: "main",
              branch: "factory-side-effect",
            },
            runSetupScript: true,
          },
          createdAt,
        },
        factoryAuthority,
        {
          ...model,
          threads: model.threads.map((thread) =>
            thread.id === factoryThreadId
              ? {
                  ...thread,
                  branch: "factory-side-effect",
                  worktreePath: "/tmp/private-command-guard",
                }
              : thread,
          ),
        },
      );
      const rendered = String(exit);
      expect(rendered).toContain("Thread 'thread-factory-command-guard' does not exist");
      expect(rendered).not.toContain("project-private-command-guard");
      expect(rendered).not.toContain("/tmp/private-command-guard");
    }),
  );

  it.effect(
    "rejects setup-script retries that collide with hidden external thread worktree metadata",
    () =>
      Effect.gen(function* () {
        const hiddenWorktreePath = "/tmp/external-private-worktrees/hidden";
        const hiddenRemovalPath = "/tmp/external-private-removals/hidden";
        const model = readModel();
        const modelWithHiddenExternalWorktree = {
          ...model,
          threads: model.threads.map((thread) =>
            thread.id === privateThreadId
              ? {
                  ...thread,
                  worktreePath: hiddenWorktreePath,
                  worktreeRemovable: true,
                  worktreeRemovalPath: hiddenRemovalPath,
                }
              : thread,
          ),
        };

        for (const [index, candidatePath] of [hiddenWorktreePath, hiddenRemovalPath].entries()) {
          const exit = yield* authorizeFailureEffect(
            {
              type: "thread.turn.start",
              commandId: CommandId.make(`cmd-private-guard-hidden-external-worktree-${index}`),
              threadId: factoryThreadId,
              message: {
                messageId: asMessageId(`msg-private-guard-hidden-external-worktree-${index}`),
                role: "user",
                text: "must not launch setup in a hidden external worktree",
                attachments: [],
              },
              interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
              runtimeMode: "full-access",
              bootstrap: {
                createThread: {
                  projectId: factoryProjectId,
                  title: "Factory Thread",
                  modelSelection,
                  interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
                  runtimeMode: "full-access",
                  branch: null,
                  worktreePath: null,
                  createdAt,
                },
                prepareWorktree: {
                  projectCwd: "/tmp/factory-command-guard",
                  baseBranch: "main",
                  branch: "factory-side-effect",
                },
                runSetupScript: true,
              },
              createdAt,
            },
            factoryAuthority,
            {
              ...modelWithHiddenExternalWorktree,
              threads: modelWithHiddenExternalWorktree.threads.map((thread) =>
                thread.id === factoryThreadId
                  ? {
                      ...thread,
                      branch: "factory-side-effect",
                      worktreePath: candidatePath,
                      worktreeRemovable: true,
                      worktreeRemovalPath: candidatePath,
                    }
                  : thread,
              ),
            },
          );
          const rendered = String(exit);
          expect(rendered).toContain("Thread 'thread-factory-command-guard' does not exist");
          expect(rendered).not.toContain("thread-private-command-guard");
          expect(rendered).not.toContain(candidatePath);
        }
      }),
  );

  it.effect("rejects an authorized-bootstrap retry after its target was archived", () =>
    Effect.gen(function* () {
      const model = readModel();
      const exit = yield* authorizeFailureEffect(
        {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-factory-guard-archived-bootstrap-retry"),
          threadId: factoryThreadId,
          message: {
            messageId: asMessageId("msg-factory-guard-archived-bootstrap-retry"),
            role: "user",
            text: "must not resume setup for an archived target",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          bootstrap: {
            createThread: {
              projectId: factoryProjectId,
              title: "Factory Thread",
              modelSelection,
              interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
              runtimeMode: "full-access",
              branch: null,
              worktreePath: null,
              createdAt,
            },
            prepareWorktree: {
              projectCwd: "/tmp/factory-command-guard",
              baseBranch: "main",
              branch: "factory-retry-worktree",
            },
            runSetupScript: true,
          },
          createdAt,
        },
        factoryAuthority,
        {
          ...model,
          threads: model.threads.map((thread) =>
            thread.id === factoryThreadId
              ? {
                  ...thread,
                  archivedAt: createdAt,
                  branch: "factory-retry-worktree",
                  worktreePath: "/tmp/t3-worktrees/factory-retry-worktree",
                  worktreeRemovable: true,
                  worktreeRemovalPath: "/tmp/t3-worktrees/factory-retry-worktree",
                }
              : thread,
          ),
        },
      );
      expect(String(exit)).toContain("Thread 'thread-factory-command-guard' does not exist");
    }),
  );

  it.effect("allows an authorized factory bootstrap retry after worktree metadata persisted", () =>
    Effect.gen(function* () {
      const model = readModel();
      const command: OrchestrationCommand = {
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-factory-guard-authorized-bootstrap-retry"),
        threadId: factoryThreadId,
        message: {
          messageId: asMessageId("msg-factory-guard-authorized-bootstrap-retry"),
          role: "user",
          text: "resume after the bootstrap metadata commit",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        bootstrap: {
          createThread: {
            projectId: factoryProjectId,
            title: "Factory Thread",
            modelSelection,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
          },
          prepareWorktree: {
            projectCwd: "/tmp/factory-command-guard",
            baseBranch: "main",
            branch: "factory-retry-worktree",
          },
          runSetupScript: true,
        },
        createdAt,
      };
      const retryModel = {
        ...model,
        threads: model.threads.map((thread) =>
          thread.id === factoryThreadId
            ? {
                ...thread,
                branch: "factory-retry-worktree",
                worktreePath: "/tmp/t3-worktrees/factory-retry-worktree",
                worktreeRemovable: true,
                worktreeRemovalPath: "/tmp/t3-worktrees/factory-retry-worktree",
              }
            : thread,
        ),
      };

      expect(yield* authorizeEffect(command, factoryAuthority, retryModel)).toMatchObject(command);
    }),
  );

  it.effect("rejects bootstrap worktreePath metadata even without setup scripts", () =>
    Effect.gen(function* () {
      const exit = yield* authorizeFailureEffect({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-private-guard-create-thread-bare-worktree-path"),
        threadId: ThreadId.make("thread-factory-command-guard-bare-worktree-path"),
        message: {
          messageId: asMessageId("msg-private-guard-create-thread-bare-worktree-path"),
          role: "user",
          text: "must not start a turn from a caller-supplied worktree path",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        bootstrap: {
          createThread: {
            projectId: factoryProjectId,
            title: "Factory thread with bare provider cwd",
            modelSelection,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            runtimeMode: "full-access",
            branch: null,
            worktreePath: "/tmp/private-command-guard",
            createdAt,
          },
        },
        createdAt,
      });
      const rendered = String(exit);
      expect(rendered).toContain("Project 'project-factory-command-guard' does not exist");
      expect(rendered).not.toContain("project-private-command-guard");
      expect(rendered).not.toContain("/tmp/private-command-guard");
    }),
  );

  it.effect("rejects bootstrap worktreeRemovalPath metadata even without setup scripts", () =>
    Effect.gen(function* () {
      const exit = yield* authorizeFailureEffect({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-private-guard-create-thread-bare-removal-path"),
        threadId: ThreadId.make("thread-factory-command-guard-bare-removal-path"),
        message: {
          messageId: asMessageId("msg-private-guard-create-thread-bare-removal-path"),
          role: "user",
          text: "must not persist caller-supplied worktree removal metadata",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        bootstrap: {
          createThread: {
            projectId: factoryProjectId,
            title: "Factory thread with bare removal cwd",
            modelSelection,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            worktreeRemovalPath: "/tmp/private-command-guard",
            createdAt,
          },
        },
        createdAt,
      });
      const rendered = String(exit);
      expect(rendered).toContain("Project 'project-factory-command-guard' does not exist");
      expect(rendered).not.toContain("project-private-command-guard");
      expect(rendered).not.toContain("/tmp/private-command-guard");
    }),
  );

  it.effect(
    "rejects setup-script worktreePath without prepareWorktree even inside the authorized project",
    () =>
      Effect.gen(function* () {
        const exit = yield* authorizeFailureEffect({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-private-guard-create-thread-bare-setup-worktree-path"),
          threadId: ThreadId.make("thread-factory-command-guard-bare-setup-worktree-path"),
          message: {
            messageId: asMessageId("msg-private-guard-create-thread-bare-setup-worktree-path"),
            role: "user",
            text: "must not launch setup from a caller-supplied worktree path",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          bootstrap: {
            createThread: {
              projectId: factoryProjectId,
              title: "Factory thread with bare setup cwd",
              modelSelection,
              interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
              runtimeMode: "full-access",
              branch: null,
              worktreePath: "/tmp/factory-command-guard/link-to-private",
              createdAt,
            },
            runSetupScript: true,
          },
          createdAt,
        });
        const rendered = String(exit);
        expect(rendered).toContain("Project 'project-factory-command-guard' does not exist");
        expect(rendered).not.toContain("project-private-command-guard");
        expect(rendered).not.toContain("/tmp/private-command-guard");
      }),
  );

  it.effect(
    "allows prepareWorktree when createThread authorizes the same accessible project root",
    () =>
      Effect.gen(function* () {
        const command: OrchestrationCommand = {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-factory-guard-create-thread-prepare-worktree"),
          threadId: ThreadId.make("thread-factory-command-guard-created-with-worktree"),
          message: {
            messageId: asMessageId("msg-factory-guard-create-thread-prepare-worktree"),
            role: "user",
            text: "prepare an authorized factory worktree",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          bootstrap: {
            createThread: {
              projectId: factoryProjectId,
              title: "Factory prepared thread",
              modelSelection,
              interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
              runtimeMode: "full-access",
              branch: null,
              worktreePath: null,
              createdAt,
            },
            prepareWorktree: {
              projectCwd: "/tmp/factory-command-guard",
              baseBranch: "main",
              branch: "factory-side-effect",
            },
            runSetupScript: true,
          },
          createdAt,
        };
        expect(yield* authorizeEffect(command)).toMatchObject(command);
      }),
  );

  it.effect(
    "rejects prepareWorktree whose cwd belongs to a hidden project even when createThread is factory",
    () =>
      Effect.gen(function* () {
        const exit = yield* authorizeFailureEffect({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-private-guard-create-thread-prepare-worktree"),
          threadId: ThreadId.make("thread-factory-command-guard-hidden-prepare-worktree"),
          message: {
            messageId: asMessageId("msg-private-guard-create-thread-prepare-worktree"),
            role: "user",
            text: "must not prepare a private worktree",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          bootstrap: {
            createThread: {
              projectId: factoryProjectId,
              title: "Factory thread with private cwd",
              modelSelection,
              interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
              runtimeMode: "full-access",
              branch: null,
              worktreePath: null,
              createdAt,
            },
            prepareWorktree: {
              projectCwd: "/tmp/private-command-guard",
              baseBranch: "main",
              branch: "private-side-effect",
            },
          },
          createdAt,
        });
        const rendered = String(exit);
        expect(rendered).toContain("Project 'project-factory-command-guard' does not exist");
        expect(rendered).not.toContain("project-private-command-guard");
        expect(rendered).not.toContain("/tmp/private-command-guard");
      }),
  );

  it.effect("rejects unknown target audience fail closed", () =>
    Effect.gen(function* () {
      const exit = yield* authorizeFailureEffect({
        type: "thread.create",
        commandId: CommandId.make("cmd-unknown-target-audience"),
        threadId: ThreadId.make("thread-unknown-target-audience"),
        projectId: asProjectId("project-unknown-target-audience"),
        title: "Unknown target",
        modelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        branch: null,
        worktreePath: null,
        createdAt,
      });
      expect(String(exit)).toContain("Project 'project-unknown-target-audience' does not exist");
    }),
  );

  it.effect("rejects unclassified mutation commands fail closed", () =>
    Effect.gen(function* () {
      const exit = yield* authorizeFailureEffect({
        type: "thread.future.mutate",
        commandId: CommandId.make("cmd-unclassified-mutation"),
        threadId: factoryThreadId,
      } as unknown as OrchestrationCommand);
      expect(String(exit)).toContain(
        "Command target audience cannot be resolved for command 'thread.future.mutate'",
      );
    }),
  );

  it.effect("rejects archive cascades that would mutate hidden descendants", () =>
    Effect.gen(function* () {
      const model = readModel();
      const mixedTree = {
        ...model,
        threads: model.threads.map((thread) =>
          thread.id === privateThreadId ? { ...thread, parentThreadId: factoryThreadId } : thread,
        ),
      };
      const exit = yield* authorizeFailureEffect(
        {
          type: "thread.archive",
          commandId: CommandId.make("cmd-private-guard-archive-hidden-descendant"),
          threadId: factoryThreadId,
        },
        factoryAuthority,
        mixedTree,
      );
      const rendered = String(exit);
      expect(rendered).toContain("Thread 'thread-factory-command-guard' does not exist");
      expect(rendered).not.toContain("thread-private-command-guard");
    }),
  );

  it.effect("rejects force-delete cascades that would mutate hidden project threads", () =>
    Effect.gen(function* () {
      const model = readModel();
      const mixedProject = {
        ...model,
        threads: model.threads.map((thread) =>
          thread.id === privateThreadId ? { ...thread, projectId: factoryProjectId } : thread,
        ),
      };
      const exit = yield* authorizeFailureEffect(
        {
          type: "project.delete",
          commandId: CommandId.make("cmd-private-guard-force-delete-hidden-thread"),
          projectId: factoryProjectId,
          force: true,
        },
        factoryAuthority,
        mixedProject,
      );
      const rendered = String(exit);
      expect(rendered).toContain("Project 'project-factory-command-guard' does not exist");
      expect(rendered).not.toContain("thread-private-command-guard");
    }),
  );

  it.effect("rejects cross-audience references even for private-ceiling sessions", () =>
    Effect.gen(function* () {
      const parentExit = yield* authorizeFailureEffect(
        {
          type: "thread.parent.set",
          commandId: CommandId.make("cmd-private-ceiling-cross-audience-parent"),
          threadId: privateThreadId,
          parentThreadId: factoryThreadId,
          createdAt,
        },
        privateAuthority,
      );
      expect(String(parentExit)).toContain("Thread 'thread-factory-command-guard' does not exist");

      const sourcePlanExit = yield* authorizeFailureEffect(
        {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-private-ceiling-cross-audience-source-plan"),
          threadId: factoryThreadId,
          message: {
            messageId: asMessageId("msg-private-ceiling-cross-audience-source-plan"),
            role: "user",
            text: "must not dispatch",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          sourceProposedPlan: { threadId: privateThreadId, planId: "private-plan" },
          createdAt,
        },
        privateAuthority,
      );
      expect(String(sourcePlanExit)).toContain(
        "Thread 'thread-private-command-guard' does not exist",
      );
    }),
  );

  it.effect("rejects cross-audience parent and proposed-plan references", () =>
    Effect.gen(function* () {
      const parentExit = yield* authorizeFailureEffect({
        type: "thread.parent.set",
        commandId: CommandId.make("cmd-cross-audience-parent"),
        threadId: factoryThreadId,
        parentThreadId: privateThreadId,
        createdAt,
      });
      expect(String(parentExit)).toContain("Thread 'thread-private-command-guard' does not exist");

      const sourcePlanExit = yield* authorizeFailureEffect({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-cross-audience-source-plan"),
        threadId: factoryThreadId,
        message: {
          messageId: asMessageId("msg-cross-audience-source-plan"),
          role: "user",
          text: "must not dispatch",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        sourceProposedPlan: { threadId: privateThreadId, planId: "private-plan" },
        createdAt,
      });
      expect(String(sourcePlanExit)).toContain(
        "Thread 'thread-private-command-guard' does not exist",
      );
    }),
  );
});
