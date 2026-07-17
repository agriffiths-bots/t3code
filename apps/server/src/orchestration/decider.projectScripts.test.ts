import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  ProviderInstanceId,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as NodeServices from "@effect/platform-node/NodeServices";

import { decideOrchestrationCommand } from "./decider.ts";
import { createEmptyReadModel, projectEvent } from "./projector.ts";

const asEventId = (value: string): EventId => EventId.make(value);
const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asMessageId = (value: string): MessageId => MessageId.make(value);
it.layer(NodeServices.layer)("decider project scripts", (it) => {
  it.effect("emits empty scripts on project.create", () =>
    Effect.gen(function* () {
      const now = "2026-01-01T00:00:00.000Z";
      const readModel = createEmptyReadModel(now);

      const result = yield* decideOrchestrationCommand({
        command: {
          type: "project.create",
          commandId: CommandId.make("cmd-project-create-scripts"),
          projectId: asProjectId("project-scripts"),
          title: "Scripts",
          workspaceRoot: "/tmp/scripts",
          createdAt: now,
        },
        readModel,
      });

      const event = Array.isArray(result) ? result[0] : result;
      expect(event.type).toBe("project.created");
      expect((event.payload as { scripts: unknown[] }).scripts).toEqual([]);
      expect((event.payload as { dataAudience: string }).dataAudience).toBe("private");
    }),
  );

  it.effect("propagates scripts in project.meta.update payload", () =>
    Effect.gen(function* () {
      const now = "2026-01-01T00:00:00.000Z";
      const initial = createEmptyReadModel(now);
      const readModel = yield* projectEvent(initial, {
        sequence: 1,
        eventId: asEventId("evt-project-create-scripts"),
        aggregateKind: "project",
        aggregateId: asProjectId("project-scripts"),
        type: "project.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-project-create-scripts"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-project-create-scripts"),
        metadata: {},
        payload: {
          projectId: asProjectId("project-scripts"),
          title: "Scripts",
          workspaceRoot: "/tmp/scripts",
          dataAudience: "private",
          defaultModelSelection: null,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });

      const scripts = [
        {
          id: "lint",
          name: "Lint",
          command: "bun run lint",
          icon: "lint",
          runOnWorktreeCreate: false,
        },
      ] as const;

      const result = yield* decideOrchestrationCommand({
        command: {
          type: "project.meta.update",
          commandId: CommandId.make("cmd-project-update-scripts"),
          projectId: asProjectId("project-scripts"),
          scripts: Array.from(scripts),
        },
        readModel,
      });

      const event = Array.isArray(result) ? result[0] : result;
      expect(event.type).toBe("project.meta-updated");
      expect((event.payload as { scripts?: unknown[] }).scripts).toEqual(scripts);
    }),
  );

  it.effect("rejects a workspace-root retarget while the project owns a removable worktree", () =>
    Effect.gen(function* () {
      const now = "2026-01-01T00:00:00.000Z";
      const projectId = asProjectId("project-owned-worktree");
      const threadId = ThreadId.make("thread-owned-worktree");
      const withProject = yield* projectEvent(createEmptyReadModel(now), {
        sequence: 1,
        eventId: asEventId("evt-project-owned-worktree"),
        aggregateKind: "project",
        aggregateId: projectId,
        type: "project.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-project-owned-worktree"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-project-owned-worktree"),
        metadata: {},
        payload: {
          projectId,
          title: "Owned worktree",
          workspaceRoot: "/repo-a",
          dataAudience: "private",
          defaultModelSelection: null,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });
      const readModel = yield* projectEvent(withProject, {
        sequence: 2,
        eventId: asEventId("evt-thread-owned-worktree"),
        aggregateKind: "thread",
        aggregateId: threadId,
        type: "thread.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-thread-owned-worktree"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-thread-owned-worktree"),
        metadata: {},
        payload: {
          threadId,
          projectId,
          title: "Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: "plan",
          runtimeMode: "approval-required",
          branch: "feature/owned",
          worktreePath: "/repo-a-worktrees/owned",
          worktreeRemovable: true,
          worktreeRemovalPath: "/repo-a-worktrees/owned",
          createdAt: now,
          updatedAt: now,
        },
      });

      const error = yield* decideOrchestrationCommand({
        command: {
          type: "project.meta.update",
          commandId: CommandId.make("cmd-project-retarget-owned-worktree"),
          projectId,
          workspaceRoot: "/repo-b",
        },
        readModel,
      }).pipe(Effect.flip);

      expect(error).toMatchObject({
        _tag: "OrchestrationCommandInvariantError",
        commandType: "project.meta.update",
      });
      if (error._tag === "OrchestrationCommandInvariantError") {
        expect(error.detail).toContain("owns a removable thread worktree");
      }
    }),
  );

  it.effect("rejects project.create for an active workspace root that already exists", () =>
    Effect.gen(function* () {
      const now = "2026-01-01T00:00:00.000Z";
      const initial = createEmptyReadModel(now);
      const readModel = yield* projectEvent(initial, {
        sequence: 1,
        eventId: asEventId("evt-project-create"),
        aggregateKind: "project",
        aggregateId: asProjectId("project-existing"),
        type: "project.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-project-create"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-project-create"),
        metadata: {},
        payload: {
          projectId: asProjectId("project-existing"),
          title: "Project",
          workspaceRoot: "/tmp/project",
          dataAudience: "private",
          defaultModelSelection: null,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });

      const failure = yield* Effect.flip(
        decideOrchestrationCommand({
          command: {
            type: "project.create",
            commandId: CommandId.make("cmd-project-create-duplicate-root"),
            projectId: asProjectId("project-duplicate-root"),
            title: "Duplicate Project",
            workspaceRoot: "/tmp/project/",
            createdAt: now,
          },
          readModel,
        }),
      );

      expect(failure.message).toContain(
        "Active project 'project-existing' already exists for workspace root '/tmp/project'.",
      );
    }),
  );

  it.effect("rejects project.meta.update when moving onto another active workspace root", () =>
    Effect.gen(function* () {
      const now = "2026-01-01T00:00:00.000Z";
      const initial = createEmptyReadModel(now);
      const withFirstProject = yield* projectEvent(initial, {
        sequence: 1,
        eventId: asEventId("evt-project-create-first"),
        aggregateKind: "project",
        aggregateId: asProjectId("project-first"),
        type: "project.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-project-create-first"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-project-create-first"),
        metadata: {},
        payload: {
          projectId: asProjectId("project-first"),
          title: "First",
          workspaceRoot: "/tmp/project-first",
          dataAudience: "private",
          defaultModelSelection: null,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });
      const readModel = yield* projectEvent(withFirstProject, {
        sequence: 2,
        eventId: asEventId("evt-project-create-second"),
        aggregateKind: "project",
        aggregateId: asProjectId("project-second"),
        type: "project.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-project-create-second"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-project-create-second"),
        metadata: {},
        payload: {
          projectId: asProjectId("project-second"),
          title: "Second",
          workspaceRoot: "/tmp/project-second",
          dataAudience: "private",
          defaultModelSelection: null,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });

      const failure = yield* Effect.flip(
        decideOrchestrationCommand({
          command: {
            type: "project.meta.update",
            commandId: CommandId.make("cmd-project-update-duplicate-root"),
            projectId: asProjectId("project-second"),
            workspaceRoot: "/tmp/project-first",
          },
          readModel,
        }),
      );

      expect(failure.message).toContain(
        "Active project 'project-first' already exists for workspace root '/tmp/project-first'.",
      );
    }),
  );

  it.effect("emits user message and turn-start-requested events for thread.turn.start", () =>
    Effect.gen(function* () {
      const now = "2026-01-01T00:00:00.000Z";
      const initial = createEmptyReadModel(now);
      const withProject = yield* projectEvent(initial, {
        sequence: 1,
        eventId: asEventId("evt-project-create"),
        aggregateKind: "project",
        aggregateId: asProjectId("project-1"),
        type: "project.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-project-create"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-project-create"),
        metadata: {},
        payload: {
          projectId: asProjectId("project-1"),
          title: "Project",
          workspaceRoot: "/tmp/project",
          dataAudience: "private",
          defaultModelSelection: null,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });
      const readModel = yield* projectEvent(withProject, {
        sequence: 2,
        eventId: asEventId("evt-thread-create"),
        aggregateKind: "thread",
        aggregateId: ThreadId.make("thread-1"),
        type: "thread.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-thread-create"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-thread-create"),
        metadata: {},
        payload: {
          threadId: ThreadId.make("thread-1"),
          projectId: asProjectId("project-1"),
          title: "Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: "plan",
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt: now,
          updatedAt: now,
        },
      });

      const result = yield* decideOrchestrationCommand({
        command: {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-turn-start"),
          threadId: ThreadId.make("thread-1"),
          message: {
            messageId: asMessageId("message-user-1"),
            role: "user",
            text: "hello",
            attachments: [],
          },
          modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.3-codex", [
            { id: "reasoningEffort", value: "high" },
            { id: "fastMode", value: true },
          ]),
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: DEFAULT_RUNTIME_MODE,
          createdAt: now,
        },
        readModel,
      });

      expect(Array.isArray(result)).toBe(true);
      const events = Array.isArray(result) ? result : [result];
      expect(events).toHaveLength(2);
      expect(events[0]?.type).toBe("thread.message-sent");
      const turnStartEvent = events[1];
      expect(turnStartEvent?.type).toBe("thread.turn-start-requested");
      expect(turnStartEvent?.causationEventId).toBe(events[0]?.eventId ?? null);
      if (turnStartEvent?.type !== "thread.turn-start-requested") {
        return;
      }
      expect(turnStartEvent.payload).toMatchObject({
        threadId: ThreadId.make("thread-1"),
        messageId: asMessageId("message-user-1"),
        modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.3-codex", [
          { id: "reasoningEffort", value: "high" },
          { id: "fastMode", value: true },
        ]),
        runtimeMode: DEFAULT_RUNTIME_MODE,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      });
    }),
  );

  it.effect("inherits thread modes when legacy turn-start commands omit them", () =>
    Effect.gen(function* () {
      const now = "2026-01-01T00:00:00.000Z";
      const initial = createEmptyReadModel(now);
      const withProject = yield* projectEvent(initial, {
        sequence: 1,
        eventId: asEventId("evt-project-create-legacy-turn"),
        aggregateKind: "project",
        aggregateId: asProjectId("project-1"),
        type: "project.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-project-create-legacy-turn"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-project-create-legacy-turn"),
        metadata: {},
        payload: {
          projectId: asProjectId("project-1"),
          title: "Project",
          workspaceRoot: "/tmp/project",
          dataAudience: "private",
          defaultModelSelection: null,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });
      const readModel = yield* projectEvent(withProject, {
        sequence: 2,
        eventId: asEventId("evt-thread-create-legacy-turn"),
        aggregateKind: "thread",
        aggregateId: ThreadId.make("thread-1"),
        type: "thread.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-thread-create-legacy-turn"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-thread-create-legacy-turn"),
        metadata: {},
        payload: {
          threadId: ThreadId.make("thread-1"),
          projectId: asProjectId("project-1"),
          title: "Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: "plan",
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt: now,
          updatedAt: now,
        },
      });

      const result = yield* decideOrchestrationCommand({
        command: {
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-turn-start-legacy"),
          threadId: ThreadId.make("thread-1"),
          message: {
            messageId: asMessageId("message-user-legacy"),
            role: "user",
            text: "hello",
            attachments: [],
          },
          createdAt: now,
        } as never,
        readModel,
      });

      const events = Array.isArray(result) ? result : [result];
      const turnStartEvent = events.find((event) => event.type === "thread.turn-start-requested");
      expect(turnStartEvent?.type).toBe("thread.turn-start-requested");
      if (turnStartEvent?.type !== "thread.turn-start-requested") return;
      expect(turnStartEvent.payload.runtimeMode).toBe("approval-required");
      expect(turnStartEvent.payload.interactionMode).toBe("plan");
    }),
  );

  it.effect("emits thread.runtime-mode-set from thread.runtime-mode.set", () =>
    Effect.gen(function* () {
      const now = "2026-01-01T00:00:00.000Z";
      const initial = createEmptyReadModel(now);
      const withProject = yield* projectEvent(initial, {
        sequence: 1,
        eventId: asEventId("evt-project-create"),
        aggregateKind: "project",
        aggregateId: asProjectId("project-1"),
        type: "project.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-project-create"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-project-create"),
        metadata: {},
        payload: {
          projectId: asProjectId("project-1"),
          title: "Project",
          workspaceRoot: "/tmp/project",
          dataAudience: "private",
          defaultModelSelection: null,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });
      const readModel = yield* projectEvent(withProject, {
        sequence: 2,
        eventId: asEventId("evt-thread-create"),
        aggregateKind: "thread",
        aggregateId: ThreadId.make("thread-1"),
        type: "thread.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-thread-create"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-thread-create"),
        metadata: {},
        payload: {
          threadId: ThreadId.make("thread-1"),
          projectId: asProjectId("project-1"),
          title: "Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          branch: null,
          worktreePath: null,
          createdAt: now,
          updatedAt: now,
        },
      });

      const result = yield* decideOrchestrationCommand({
        command: {
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("cmd-runtime-mode-set"),
          threadId: ThreadId.make("thread-1"),
          runtimeMode: "approval-required",
          createdAt: now,
        },
        readModel,
      });

      const singleResult = Array.isArray(result) ? null : result;
      if (singleResult === null) {
        throw new Error("Expected a single runtime-mode-set event.");
      }
      expect(singleResult).toMatchObject({
        type: "thread.runtime-mode-set",
        payload: {
          threadId: ThreadId.make("thread-1"),
          runtimeMode: "approval-required",
        },
      });
    }),
  );

  it.effect("emits thread.interaction-mode-set from thread.interaction-mode.set", () =>
    Effect.gen(function* () {
      const now = "2026-01-01T00:00:00.000Z";
      const initial = createEmptyReadModel(now);
      const withProject = yield* projectEvent(initial, {
        sequence: 1,
        eventId: asEventId("evt-project-create"),
        aggregateKind: "project",
        aggregateId: asProjectId("project-1"),
        type: "project.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-project-create"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-project-create"),
        metadata: {},
        payload: {
          projectId: asProjectId("project-1"),
          title: "Project",
          workspaceRoot: "/tmp/project",
          dataAudience: "private",
          defaultModelSelection: null,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });
      const readModel = yield* projectEvent(withProject, {
        sequence: 2,
        eventId: asEventId("evt-thread-create"),
        aggregateKind: "thread",
        aggregateId: ThreadId.make("thread-1"),
        type: "thread.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-thread-create"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-thread-create"),
        metadata: {},
        payload: {
          threadId: ThreadId.make("thread-1"),
          projectId: asProjectId("project-1"),
          title: "Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt: now,
          updatedAt: now,
        },
      });

      const result = yield* decideOrchestrationCommand({
        command: {
          type: "thread.interaction-mode.set",
          commandId: CommandId.make("cmd-interaction-mode-set"),
          threadId: ThreadId.make("thread-1"),
          interactionMode: "plan",
          createdAt: now,
        },
        readModel,
      });

      const singleResult = Array.isArray(result) ? null : result;
      if (singleResult === null) {
        throw new Error("Expected a single interaction-mode-set event.");
      }
      expect(singleResult).toMatchObject({
        type: "thread.interaction-mode-set",
        payload: {
          threadId: ThreadId.make("thread-1"),
          interactionMode: "plan",
        },
      });
    }),
  );

  it.effect("rejects an audience change after the project is deleted", () =>
    Effect.gen(function* () {
      const createdAt = "2026-07-17T12:00:00.000Z";
      const deletedAt = "2026-07-17T12:01:00.000Z";
      const projectId = asProjectId("project-deleted-audience");
      const withProject = yield* projectEvent(createEmptyReadModel(createdAt), {
        sequence: 1,
        eventId: asEventId("evt-project-deleted-audience-created"),
        aggregateKind: "project",
        aggregateId: projectId,
        type: "project.created",
        occurredAt: createdAt,
        commandId: CommandId.make("cmd-project-deleted-audience-created"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-project-deleted-audience-created"),
        metadata: {},
        payload: {
          projectId,
          title: "Deleted audience project",
          workspaceRoot: "/repo/deleted-audience",
          dataAudience: "private",
          defaultModelSelection: null,
          scripts: [],
          createdAt,
          updatedAt: createdAt,
        },
      });
      const readModel = yield* projectEvent(withProject, {
        sequence: 2,
        eventId: asEventId("evt-project-deleted-audience-deleted"),
        aggregateKind: "project",
        aggregateId: projectId,
        type: "project.deleted",
        occurredAt: deletedAt,
        commandId: CommandId.make("cmd-project-deleted-audience-deleted"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-project-deleted-audience-deleted"),
        metadata: {},
        payload: { projectId, deletedAt },
      });

      const error = yield* decideOrchestrationCommand({
        command: {
          type: "project.data-audience.set",
          commandId: CommandId.make("cmd-project-deleted-audience-set"),
          projectId,
          expectedWorkspaceRoot: "/repo/deleted-audience",
          actor: "local-admin:test",
          occurredAt: "2026-07-17T12:02:00.000Z",
        },
        readModel,
      }).pipe(Effect.flip);

      expect(error).toMatchObject({
        _tag: "OrchestrationCommandInvariantError",
        commandType: "project.data-audience.set",
      });
      if (error._tag === "OrchestrationCommandInvariantError") {
        expect(error.detail).toContain("is deleted");
      }
    }),
  );

  it.effect("prevents a standard command from retargeting a factory project root", () =>
    Effect.gen(function* () {
      const now = "2026-07-17T12:00:00.000Z";
      const projectId = asProjectId("project-factory-root-lock");
      const readModel = yield* projectEvent(createEmptyReadModel(now), {
        sequence: 1,
        eventId: asEventId("evt-project-factory-root-lock"),
        aggregateKind: "project",
        aggregateId: projectId,
        type: "project.created",
        occurredAt: now,
        commandId: CommandId.make("cmd-project-factory-root-lock"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-project-factory-root-lock"),
        metadata: {},
        payload: {
          projectId,
          title: "Factory",
          workspaceRoot: "/repo/factory",
          dataAudience: "factory",
          defaultModelSelection: null,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });

      const error = yield* decideOrchestrationCommand({
        command: {
          type: "project.meta.update",
          commandId: CommandId.make("cmd-project-factory-retarget"),
          projectId,
          workspaceRoot: "/home/user",
        },
        readModel,
      }).pipe(Effect.flip);

      expect(error._tag).toBe("OrchestrationCommandInvariantError");
      if (error._tag !== "OrchestrationCommandInvariantError") {
        throw new Error(`Expected OrchestrationCommandInvariantError, got ${error._tag}`);
      }
      expect(error.detail).toContain("cannot change workspace roots");
    }),
  );
});
