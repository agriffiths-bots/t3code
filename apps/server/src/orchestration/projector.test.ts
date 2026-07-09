import {
  CommandId,
  EventId,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  type OrchestrationEvent,
} from "@t3tools/contracts";
import { it as effectIt } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vite-plus/test";

import { createEmptyReadModel, projectEvent } from "./projector.ts";

function makeEvent(input: {
  sequence: number;
  type: OrchestrationEvent["type"];
  occurredAt: string;
  aggregateKind: OrchestrationEvent["aggregateKind"];
  aggregateId: string;
  commandId: string | null;
  payload: unknown;
}): OrchestrationEvent {
  return {
    sequence: input.sequence,
    eventId: EventId.make(`event-${input.sequence}`),
    type: input.type,
    aggregateKind: input.aggregateKind,
    aggregateId:
      input.aggregateKind === "project"
        ? ProjectId.make(input.aggregateId)
        : ThreadId.make(input.aggregateId),
    occurredAt: input.occurredAt,
    commandId: input.commandId === null ? null : CommandId.make(input.commandId),
    causationEventId: null,
    correlationId: null,
    metadata: {},
    payload: input.payload as never,
  } as OrchestrationEvent;
}

describe("orchestration projector", () => {
  it("applies thread.created events", async () => {
    const now = "2026-01-01T00:00:00.000Z";
    const model = createEmptyReadModel(now);

    const next = await Effect.runPromise(
      projectEvent(
        model,
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: now,
          commandId: "cmd-thread-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt: now,
            updatedAt: now,
          },
        }),
      ),
    );

    expect(next.snapshotSequence).toBe(1);
    expect(next.threads).toEqual([
      {
        id: "thread-1",
        projectId: "project-1",
        title: "demo",
        modelSelection: {
          instanceId: "codex",
          model: "gpt-5-codex",
        },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        latestTurn: null,
        turns: [],
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        deletedAt: null,
        messages: [],
        proposedPlans: [],
        activities: [],
        checkpoints: [],
        session: null,
      },
    ]);
  });

  it("fails when event payload cannot be decoded by runtime schema", async () => {
    const now = "2026-01-01T00:00:00.000Z";
    const model = createEmptyReadModel(now);

    await expect(
      Effect.runPromise(
        projectEvent(
          model,
          makeEvent({
            sequence: 1,
            type: "thread.created",
            aggregateKind: "thread",
            aggregateId: "thread-1",
            occurredAt: now,
            commandId: "cmd-invalid",
            payload: {
              // missing required threadId
              projectId: "project-1",
              title: "demo",
              modelSelection: {
                provider: ProviderDriverKind.make("codex"),
                model: "gpt-5-codex",
              },
              branch: null,
              worktreePath: null,
              createdAt: now,
              updatedAt: now,
            },
          }),
        ),
      ),
    ).rejects.toBeDefined();
  });

  it("applies thread.archived and thread.unarchived events", async () => {
    const now = "2026-01-01T00:00:00.000Z";
    const later = "2026-01-01T00:00:01.000Z";
    const created = await Effect.runPromise(
      projectEvent(
        createEmptyReadModel(now),
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: now,
          commandId: "cmd-thread-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5-codex",
            },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdAt: now,
            updatedAt: now,
          },
        }),
      ),
    );

    const archived = await Effect.runPromise(
      projectEvent(
        created,
        makeEvent({
          sequence: 2,
          type: "thread.archived",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: later,
          commandId: "cmd-thread-archive",
          payload: {
            threadId: "thread-1",
            archivedAt: later,
            updatedAt: later,
          },
        }),
      ),
    );
    expect(archived.threads[0]?.archivedAt).toBe(later);

    const unarchived = await Effect.runPromise(
      projectEvent(
        archived,
        makeEvent({
          sequence: 3,
          type: "thread.unarchived",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: later,
          commandId: "cmd-thread-unarchive",
          payload: {
            threadId: "thread-1",
            updatedAt: later,
          },
        }),
      ),
    );
    expect(unarchived.threads[0]?.archivedAt).toBeNull();
  });

  it("keeps projector forward-compatible for unhandled event types", async () => {
    const now = "2026-01-01T00:00:00.000Z";
    const model = createEmptyReadModel(now);

    const next = await Effect.runPromise(
      projectEvent(
        model,
        makeEvent({
          sequence: 7,
          type: "thread.turn-start-requested",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: "2026-01-01T00:00:00.000Z",
          commandId: "cmd-unhandled",
          payload: {
            threadId: "thread-1",
            messageId: "message-1",
            runtimeMode: "approval-required",
            createdAt: "2026-01-01T00:00:00.000Z",
          },
        }),
      ),
    );

    expect(next.snapshotSequence).toBe(7);
    expect(next.updatedAt).toBe("2026-01-01T00:00:00.000Z");
    expect(next.threads).toEqual([]);
  });

  it("tracks latest turn id from session lifecycle events", async () => {
    const createdAt = "2026-02-23T08:00:00.000Z";
    const startedAt = "2026-02-23T08:00:05.000Z";
    const model = createEmptyReadModel(createdAt);

    const afterCreate = await Effect.runPromise(
      projectEvent(
        model,
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: createdAt,
          commandId: "cmd-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5.3-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
      ),
    );

    const settledAt = "2026-02-23T08:01:00.000Z";
    const [afterRunning, afterReady] = await Effect.runPromise(
      Effect.flatMap(
        projectEvent(
          afterCreate,
          makeEvent({
            sequence: 2,
            type: "thread.session-set",
            aggregateKind: "thread",
            aggregateId: "thread-1",
            occurredAt: startedAt,
            commandId: "cmd-running",
            payload: {
              threadId: "thread-1",
              session: {
                threadId: "thread-1",
                status: "running",
                providerName: "codex",
                providerSessionId: "session-1",
                providerThreadId: "provider-thread-1",
                runtimeMode: "approval-required",
                activeTurnId: "turn-1",
                lastError: null,
                updatedAt: startedAt,
              },
            },
          }),
        ),
        (running) =>
          Effect.map(
            projectEvent(
              running,
              makeEvent({
                sequence: 3,
                type: "thread.session-set",
                aggregateKind: "thread",
                aggregateId: "thread-1",
                occurredAt: settledAt,
                commandId: "cmd-ready",
                payload: {
                  threadId: "thread-1",
                  session: {
                    threadId: "thread-1",
                    status: "ready",
                    providerName: "codex",
                    providerSessionId: "session-1",
                    providerThreadId: "provider-thread-1",
                    runtimeMode: "approval-required",
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: settledAt,
                  },
                },
              }),
            ),
            (ready) => [running, ready] as const,
          ),
      ),
    );

    const thread = afterRunning.threads[0];
    expect(thread?.latestTurn?.turnId).toBe("turn-1");
    expect(thread?.turns[0]?.turnId).toBe("turn-1");
    expect(thread?.turns[0]?.state).toBe("running");
    expect(thread?.session?.status).toBe("running");

    // Leaving the "running" session status settles the running turn with the
    // session timestamp as the turn end.
    const settledThread = afterReady.threads[0];
    expect(settledThread?.latestTurn?.turnId).toBe("turn-1");
    expect(settledThread?.latestTurn?.state).toBe("completed");
    expect(settledThread?.latestTurn?.completedAt).toBe(settledAt);
    expect(settledThread?.turns[0]?.turnId).toBe("turn-1");
    expect(settledThread?.turns[0]?.state).toBe("completed");
    expect(settledThread?.turns[0]?.completedAt).toBe(settledAt);
  });

  it("updates canonical thread runtime mode from thread.runtime-mode-set", async () => {
    const createdAt = "2026-02-23T08:00:00.000Z";
    const updatedAt = "2026-02-23T08:00:05.000Z";
    const model = createEmptyReadModel(createdAt);

    const afterCreate = await Effect.runPromise(
      projectEvent(
        model,
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: createdAt,
          commandId: "cmd-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5.3-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
      ),
    );

    const afterUpdate = await Effect.runPromise(
      projectEvent(
        afterCreate,
        makeEvent({
          sequence: 2,
          type: "thread.runtime-mode-set",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: updatedAt,
          commandId: "cmd-runtime-mode-set",
          payload: {
            threadId: "thread-1",
            runtimeMode: "approval-required",
            updatedAt,
          },
        }),
      ),
    );

    expect(afterUpdate.threads[0]?.runtimeMode).toBe("approval-required");
    expect(afterUpdate.threads[0]?.updatedAt).toBe(updatedAt);
  });

  it("marks assistant messages completed with non-streaming updates", async () => {
    const createdAt = "2026-02-23T09:00:00.000Z";
    const deltaAt = "2026-02-23T09:00:01.000Z";
    const completeAt = "2026-02-23T09:00:03.500Z";
    const model = createEmptyReadModel(createdAt);

    const afterCreate = await Effect.runPromise(
      projectEvent(
        model,
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: createdAt,
          commandId: "cmd-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5.3-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
      ),
    );

    const afterDelta = await Effect.runPromise(
      projectEvent(
        afterCreate,
        makeEvent({
          sequence: 2,
          type: "thread.message-sent",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: deltaAt,
          commandId: "cmd-delta",
          payload: {
            threadId: "thread-1",
            messageId: "assistant:msg-1",
            role: "assistant",
            text: "hello",
            turnId: "turn-1",
            streaming: true,
            createdAt: deltaAt,
            updatedAt: deltaAt,
          },
        }),
      ),
    );

    const afterComplete = await Effect.runPromise(
      projectEvent(
        afterDelta,
        makeEvent({
          sequence: 3,
          type: "thread.message-sent",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: completeAt,
          commandId: "cmd-complete",
          payload: {
            threadId: "thread-1",
            messageId: "assistant:msg-1",
            role: "assistant",
            text: "",
            turnId: "turn-1",
            streaming: false,
            createdAt: completeAt,
            updatedAt: completeAt,
          },
        }),
      ),
    );

    const message = afterComplete.threads[0]?.messages[0];
    expect(message?.id).toBe("assistant:msg-1");
    expect(message?.text).toBe("hello");
    expect(message?.streaming).toBe(false);
    expect(message?.updatedAt).toBe(completeAt);
  });

  effectIt.effect("replaces message state when an existing message id moves to another turn", () =>
    Effect.gen(function* () {
      const createdAt = "2026-02-23T09:30:00.000Z";
      const firstAt = "2026-02-23T09:30:01.000Z";
      const secondAt = "2026-02-23T09:31:01.000Z";
      const model = createEmptyReadModel(createdAt);

      const afterCreate = yield* projectEvent(
        model,
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: createdAt,
          commandId: "cmd-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("cursor"),
              model: "cursor",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
      );

      const afterFirst = yield* projectEvent(
        afterCreate,
        makeEvent({
          sequence: 2,
          type: "thread.message-sent",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: firstAt,
          commandId: "cmd-first",
          payload: {
            threadId: "thread-1",
            messageId: "assistant:session-1:segment:0",
            role: "assistant",
            text: "first turn assistant",
            attachments: [
              {
                type: "image",
                id: "thread-reused-message-att-1",
                name: "first-turn.png",
                mimeType: "image/png",
                sizeBytes: 5,
              },
            ],
            turnId: "turn-1",
            streaming: false,
            createdAt: firstAt,
            updatedAt: firstAt,
          },
        }),
      );

      const afterFirstCheckpoint = yield* projectEvent(
        afterFirst,
        makeEvent({
          sequence: 3,
          type: "thread.turn-diff-completed",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: "2026-02-23T09:30:02.000Z",
          commandId: "cmd-first-checkpoint",
          payload: {
            threadId: "thread-1",
            turnId: "turn-1",
            checkpointTurnCount: 1,
            checkpointRef: "refs/t3/checkpoints/thread-1/turn/1",
            status: "ready",
            files: [],
            assistantMessageId: "assistant:session-1:segment:0",
            completedAt: "2026-02-23T09:30:02.000Z",
          },
        }),
      );

      const afterSecond = yield* projectEvent(
        afterFirstCheckpoint,
        makeEvent({
          sequence: 4,
          type: "thread.message-sent",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: secondAt,
          commandId: "cmd-second",
          payload: {
            threadId: "thread-1",
            messageId: "assistant:session-1:segment:0",
            role: "assistant",
            text: "second turn assistant",
            turnId: "turn-2",
            streaming: true,
            createdAt: secondAt,
            updatedAt: secondAt,
          },
        }),
      );

      const message = afterSecond.threads[0]?.messages[0];
      expect(message?.turnId).toBe("turn-2");
      expect(message?.text).toBe("second turn assistant");
      expect(message?.attachments).toBeUndefined();
      expect(message?.createdAt).toBe(secondAt);
      expect(afterSecond.threads[0]?.checkpoints[0]?.assistantMessageId).toBeNull();
      expect(afterSecond.threads[0]?.latestTurn?.assistantMessageId).toBeNull();

      const afterSecondSession = yield* projectEvent(
        afterSecond,
        makeEvent({
          sequence: 5,
          type: "thread.session-set",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: "2026-02-23T09:31:15.000Z",
          commandId: "cmd-second-session",
          payload: {
            threadId: "thread-1",
            session: {
              threadId: "thread-1",
              status: "running",
              providerName: "cursor",
              runtimeMode: "full-access",
              activeTurnId: "turn-2",
              lastError: null,
              updatedAt: "2026-02-23T09:31:15.000Z",
            },
          },
        }),
      );

      const afterDelayedCheckpoint = yield* projectEvent(
        afterSecondSession,
        makeEvent({
          sequence: 6,
          type: "thread.turn-diff-completed",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: "2026-02-23T09:31:30.000Z",
          commandId: "cmd-delayed-first-checkpoint",
          payload: {
            threadId: "thread-1",
            turnId: "turn-1",
            checkpointTurnCount: 1,
            checkpointRef: "refs/t3/checkpoints/thread-1/turn/1-delayed",
            status: "ready",
            files: [],
            assistantMessageId: "assistant:session-1:segment:0",
            completedAt: "2026-02-23T09:31:30.000Z",
          },
        }),
      );

      expect(afterDelayedCheckpoint.threads[0]?.checkpoints[0]?.assistantMessageId).toBeNull();
      expect(afterDelayedCheckpoint.threads[0]?.latestTurn?.turnId).toBe("turn-2");
      expect(afterDelayedCheckpoint.threads[0]?.latestTurn?.assistantMessageId).toBe(
        "assistant:session-1:segment:0",
      );

      const afterRevert = yield* projectEvent(
        afterDelayedCheckpoint,
        makeEvent({
          sequence: 7,
          type: "thread.reverted",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: "2026-02-23T09:32:00.000Z",
          commandId: "cmd-revert",
          payload: {
            threadId: "thread-1",
            turnCount: 1,
          },
        }),
      );

      expect(afterRevert.threads[0]?.messages).toHaveLength(0);
      expect(afterRevert.threads[0]?.latestTurn?.assistantMessageId).toBeNull();
    }),
  );

  effectIt.effect(
    "keeps a later same-turn assistant boundary when an older checkpoint arrives late",
    () =>
      Effect.gen(function* () {
        const createdAt = "2026-02-23T09:40:00.000Z";
        const model = createEmptyReadModel(createdAt);

        const afterCreate = yield* projectEvent(
          model,
          makeEvent({
            sequence: 1,
            type: "thread.created",
            aggregateKind: "thread",
            aggregateId: "thread-1",
            occurredAt: createdAt,
            commandId: "cmd-create",
            payload: {
              threadId: "thread-1",
              projectId: "project-1",
              title: "demo",
              modelSelection: {
                provider: ProviderDriverKind.make("claude"),
                model: "claude-sonnet-4-5",
              },
              runtimeMode: "full-access",
              branch: null,
              worktreePath: null,
              createdAt,
              updatedAt: createdAt,
            },
          }),
        );

        const afterOlderMessage = yield* projectEvent(
          afterCreate,
          makeEvent({
            sequence: 2,
            type: "thread.message-sent",
            aggregateKind: "thread",
            aggregateId: "thread-1",
            occurredAt: "2026-02-23T09:40:01.000Z",
            commandId: "cmd-older-message",
            payload: {
              threadId: "thread-1",
              messageId: "assistant-older",
              role: "assistant",
              text: "working",
              turnId: "turn-1",
              streaming: false,
              createdAt: "2026-02-23T09:40:01.000Z",
              updatedAt: "2026-02-23T09:40:01.000Z",
            },
          }),
        );

        const afterLaterMessage = yield* projectEvent(
          afterOlderMessage,
          makeEvent({
            sequence: 3,
            type: "thread.message-sent",
            aggregateKind: "thread",
            aggregateId: "thread-1",
            occurredAt: "2026-02-23T09:40:05.000Z",
            commandId: "cmd-later-message",
            payload: {
              threadId: "thread-1",
              messageId: "assistant-later",
              role: "assistant",
              text: "final",
              turnId: "turn-1",
              streaming: false,
              createdAt: "2026-02-23T09:40:05.000Z",
              updatedAt: "2026-02-23T09:40:05.000Z",
            },
          }),
        );

        const afterLaterCheckpoint = yield* projectEvent(
          afterLaterMessage,
          makeEvent({
            sequence: 4,
            type: "thread.turn-diff-completed",
            aggregateKind: "thread",
            aggregateId: "thread-1",
            occurredAt: "2026-02-23T09:40:06.000Z",
            commandId: "cmd-later-checkpoint",
            payload: {
              threadId: "thread-1",
              turnId: "turn-1",
              checkpointTurnCount: 1,
              checkpointRef: "refs/t3/checkpoints/thread-1/turn/1",
              status: "ready",
              files: [],
              assistantMessageId: "assistant-later",
              completedAt: "2026-02-23T09:40:06.000Z",
            },
          }),
        );

        const afterSecondTurnMessage = yield* projectEvent(
          afterLaterCheckpoint,
          makeEvent({
            sequence: 5,
            type: "thread.message-sent",
            aggregateKind: "thread",
            aggregateId: "thread-1",
            occurredAt: "2026-02-23T09:40:08.000Z",
            commandId: "cmd-second-turn-message",
            payload: {
              threadId: "thread-1",
              messageId: "assistant-turn-2",
              role: "assistant",
              text: "second turn final",
              turnId: "turn-2",
              streaming: false,
              createdAt: "2026-02-23T09:40:08.000Z",
              updatedAt: "2026-02-23T09:40:08.000Z",
            },
          }),
        );

        const afterSecondTurnCheckpoint = yield* projectEvent(
          afterSecondTurnMessage,
          makeEvent({
            sequence: 6,
            type: "thread.turn-diff-completed",
            aggregateKind: "thread",
            aggregateId: "thread-1",
            occurredAt: "2026-02-23T09:40:09.000Z",
            commandId: "cmd-second-turn-checkpoint",
            payload: {
              threadId: "thread-1",
              turnId: "turn-2",
              checkpointTurnCount: 2,
              checkpointRef: "refs/t3/checkpoints/thread-1/turn/2",
              status: "ready",
              files: [],
              assistantMessageId: "assistant-turn-2",
              completedAt: "2026-02-23T09:40:09.000Z",
            },
          }),
        );

        const afterDelayedOlderCheckpoint = yield* projectEvent(
          afterSecondTurnCheckpoint,
          makeEvent({
            sequence: 7,
            type: "thread.turn-diff-completed",
            aggregateKind: "thread",
            aggregateId: "thread-1",
            occurredAt: "2026-02-23T09:40:07.000Z",
            commandId: "cmd-delayed-older-checkpoint",
            payload: {
              threadId: "thread-1",
              turnId: "turn-1",
              checkpointTurnCount: 1,
              checkpointRef: "refs/t3/checkpoints/thread-1/turn/1-delayed",
              status: "ready",
              files: [],
              assistantMessageId: "assistant-pruned-older",
              completedAt: "2026-02-23T09:40:07.000Z",
            },
          }),
        );

        const thread = afterDelayedOlderCheckpoint.threads[0];
        expect(thread?.checkpoints[0]?.assistantMessageId).toBe("assistant-pruned-older");
        expect(thread?.latestTurn?.turnId).toBe("turn-2");
        expect(thread?.latestTurn?.state).toBe("completed");
        expect(thread?.latestTurn?.completedAt).toBe("2026-02-23T09:40:09.000Z");
        expect(thread?.latestTurn?.assistantMessageId).toBe("assistant-turn-2");
        expect(thread?.turns.map((turn) => [turn.turnId, turn.assistantMessageId])).toEqual([
          ["turn-1", "assistant-later"],
          ["turn-2", "assistant-turn-2"],
        ]);
      }),
  );

  it("prunes reverted turn messages from in-memory thread snapshot", async () => {
    const createdAt = "2026-02-23T10:00:00.000Z";
    const model = createEmptyReadModel(createdAt);

    const afterCreate = await Effect.runPromise(
      projectEvent(
        model,
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: createdAt,
          commandId: "cmd-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5.3-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
      ),
    );

    const events: ReadonlyArray<OrchestrationEvent> = [
      makeEvent({
        sequence: 2,
        type: "thread.message-sent",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:01.000Z",
        commandId: "cmd-user-1",
        payload: {
          threadId: "thread-1",
          messageId: "user-msg-1",
          role: "user",
          text: "First edit",
          turnId: null,
          streaming: false,
          createdAt: "2026-02-23T10:00:01.000Z",
          updatedAt: "2026-02-23T10:00:01.000Z",
        },
      }),
      makeEvent({
        sequence: 3,
        type: "thread.message-sent",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:02.000Z",
        commandId: "cmd-assistant-1",
        payload: {
          threadId: "thread-1",
          messageId: "assistant-msg-1",
          role: "assistant",
          text: "Updated README to v2.\n",
          turnId: "turn-1",
          streaming: false,
          createdAt: "2026-02-23T10:00:02.000Z",
          updatedAt: "2026-02-23T10:00:02.000Z",
        },
      }),
      makeEvent({
        sequence: 4,
        type: "thread.turn-diff-completed",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:02.500Z",
        commandId: "cmd-turn-1-complete",
        payload: {
          threadId: "thread-1",
          turnId: "turn-1",
          checkpointTurnCount: 1,
          checkpointRef: "refs/t3/checkpoints/thread-1/turn/1",
          status: "ready",
          files: [],
          assistantMessageId: "assistant-msg-1",
          completedAt: "2026-02-23T10:00:02.500Z",
        },
      }),
      makeEvent({
        sequence: 5,
        type: "thread.activity-appended",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:02.750Z",
        commandId: "cmd-activity-1",
        payload: {
          threadId: "thread-1",
          activity: {
            id: "activity-1",
            tone: "tool",
            kind: "tool.started",
            summary: "Edit file started",
            payload: { toolKind: "command" },
            turnId: "turn-1",
            createdAt: "2026-02-23T10:00:02.750Z",
          },
        },
      }),
      makeEvent({
        sequence: 6,
        type: "thread.message-sent",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:03.000Z",
        commandId: "cmd-user-2",
        payload: {
          threadId: "thread-1",
          messageId: "user-msg-2",
          role: "user",
          text: "Second edit",
          turnId: null,
          streaming: false,
          createdAt: "2026-02-23T10:00:03.000Z",
          updatedAt: "2026-02-23T10:00:03.000Z",
        },
      }),
      makeEvent({
        sequence: 7,
        type: "thread.message-sent",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:04.000Z",
        commandId: "cmd-assistant-2",
        payload: {
          threadId: "thread-1",
          messageId: "assistant-msg-2",
          role: "assistant",
          text: "Updated README to v3.\n",
          turnId: "turn-2",
          streaming: false,
          createdAt: "2026-02-23T10:00:04.000Z",
          updatedAt: "2026-02-23T10:00:04.000Z",
        },
      }),
      makeEvent({
        sequence: 8,
        type: "thread.turn-diff-completed",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:04.500Z",
        commandId: "cmd-turn-2-complete",
        payload: {
          threadId: "thread-1",
          turnId: "turn-2",
          checkpointTurnCount: 2,
          checkpointRef: "refs/t3/checkpoints/thread-1/turn/2",
          status: "ready",
          files: [],
          assistantMessageId: "assistant-msg-2",
          completedAt: "2026-02-23T10:00:04.500Z",
        },
      }),
      makeEvent({
        sequence: 9,
        type: "thread.activity-appended",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:04.750Z",
        commandId: "cmd-activity-2",
        payload: {
          threadId: "thread-1",
          activity: {
            id: "activity-2",
            tone: "tool",
            kind: "tool.completed",
            summary: "Edit file complete",
            payload: { toolKind: "command" },
            turnId: "turn-2",
            createdAt: "2026-02-23T10:00:04.750Z",
          },
        },
      }),
      makeEvent({
        sequence: 10,
        type: "thread.reverted",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:05.000Z",
        commandId: "cmd-revert",
        payload: {
          threadId: "thread-1",
          turnCount: 1,
        },
      }),
    ];

    const afterRevert = await events.reduce<Promise<ReturnType<typeof createEmptyReadModel>>>(
      (statePromise, event) =>
        statePromise.then((state) => Effect.runPromise(projectEvent(state, event))),
      Promise.resolve(afterCreate),
    );

    const thread = afterRevert.threads[0];
    expect(thread?.messages.map((message) => ({ role: message.role, text: message.text }))).toEqual(
      [
        { role: "user", text: "First edit" },
        { role: "assistant", text: "Updated README to v2.\n" },
      ],
    );
    expect(
      thread?.activities.map((activity) => ({ id: activity.id, turnId: activity.turnId })),
    ).toEqual([{ id: "activity-1", turnId: "turn-1" }]);
    expect(thread?.checkpoints.map((checkpoint) => checkpoint.checkpointTurnCount)).toEqual([1]);
    expect(thread?.latestTurn?.turnId).toBe("turn-1");
  });

  it("prunes system wake messages that are bound to reverted turns", async () => {
    const createdAt = "2026-02-23T10:00:00.000Z";
    const model = createEmptyReadModel(createdAt);

    const afterCreate = await Effect.runPromise(
      projectEvent(
        model,
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: createdAt,
          commandId: "cmd-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5.3-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
      ),
    );

    const events: ReadonlyArray<OrchestrationEvent> = [
      makeEvent({
        sequence: 2,
        type: "thread.message-sent",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:01.000Z",
        commandId: "cmd-system-wake",
        payload: {
          threadId: "thread-1",
          messageId: "system-wake-1",
          role: "system",
          text: "[sub-agent child-1 completed] done",
          turnId: null,
          streaming: false,
          createdAt: "2026-02-23T10:00:01.000Z",
          updatedAt: "2026-02-23T10:00:01.000Z",
        },
      }),
      makeEvent({
        sequence: 3,
        type: "thread.session-set",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:02.000Z",
        commandId: "cmd-running",
        payload: {
          threadId: "thread-1",
          session: {
            threadId: "thread-1",
            status: "running",
            providerName: "codex",
            runtimeMode: "approval-required",
            activeTurnId: "turn-1",
            lastError: null,
            updatedAt: "2026-02-23T10:00:02.000Z",
          },
        },
      }),
      makeEvent({
        sequence: 4,
        type: "thread.turn-diff-completed",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:03.000Z",
        commandId: "cmd-turn-1-complete",
        payload: {
          threadId: "thread-1",
          turnId: "turn-1",
          checkpointTurnCount: 1,
          checkpointRef: "refs/t3/checkpoints/thread-1/turn/1",
          status: "ready",
          files: [],
          assistantMessageId: null,
          completedAt: "2026-02-23T10:00:03.000Z",
        },
      }),
      makeEvent({
        sequence: 5,
        type: "thread.reverted",
        aggregateKind: "thread",
        aggregateId: "thread-1",
        occurredAt: "2026-02-23T10:00:04.000Z",
        commandId: "cmd-revert",
        payload: {
          threadId: "thread-1",
          turnCount: 0,
        },
      }),
    ];

    const afterRevert = await events.reduce<Promise<ReturnType<typeof createEmptyReadModel>>>(
      (statePromise, event) =>
        statePromise.then((state) => Effect.runPromise(projectEvent(state, event))),
      Promise.resolve(afterCreate),
    );

    const thread = afterRevert.threads[0];
    expect(thread?.messages).toEqual([]);
    expect(thread?.checkpoints).toEqual([]);
    expect(thread?.latestTurn).toBeNull();
  });

  effectIt.effect("binds pending prompt messages only when active turn changes", () =>
    Effect.gen(function* () {
      const createdAt = "2026-02-23T10:00:00.000Z";
      const model = createEmptyReadModel(createdAt);

      const afterCreate = yield* projectEvent(
        model,
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: createdAt,
          commandId: "cmd-create",
          payload: {
            threadId: "thread-1",
            projectId: "project-1",
            title: "demo",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5.3-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
      );

      const afterOldTurn = yield* projectEvent(
        afterCreate,
        makeEvent({
          sequence: 2,
          type: "thread.session-set",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: "2026-02-23T10:00:01.000Z",
          commandId: "cmd-running-old",
          payload: {
            threadId: "thread-1",
            session: {
              threadId: "thread-1",
              status: "running",
              providerName: "codex",
              runtimeMode: "approval-required",
              activeTurnId: "turn-old",
              lastError: null,
              updatedAt: "2026-02-23T10:00:01.000Z",
            },
          },
        }),
      );

      const afterWakeMessage = yield* projectEvent(
        afterOldTurn,
        makeEvent({
          sequence: 3,
          type: "thread.message-sent",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: "2026-02-23T10:00:02.000Z",
          commandId: "cmd-system-wake",
          payload: {
            threadId: "thread-1",
            messageId: "system-wake-1",
            role: "system",
            text: "[sub-agent child-1 completed] done",
            turnId: null,
            streaming: false,
            createdAt: "2026-02-23T10:00:02.000Z",
            updatedAt: "2026-02-23T10:00:02.000Z",
          },
        }),
      );

      const afterNoticeMessage = yield* projectEvent(
        afterWakeMessage,
        makeEvent({
          sequence: 4,
          type: "thread.message-sent",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: "2026-02-23T10:00:02.500Z",
          commandId: "cmd-system-notice",
          payload: {
            threadId: "thread-1",
            messageId: "system-notice-1",
            role: "system",
            text: "System maintenance",
            turnId: null,
            streaming: false,
            createdAt: "2026-02-23T10:00:02.500Z",
            updatedAt: "2026-02-23T10:00:02.500Z",
          },
        }),
      );

      const afterUnchangedOldTurn = yield* projectEvent(
        afterNoticeMessage,
        makeEvent({
          sequence: 5,
          type: "thread.session-set",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: "2026-02-23T10:00:03.000Z",
          commandId: "cmd-running-old-again",
          payload: {
            threadId: "thread-1",
            session: {
              threadId: "thread-1",
              status: "running",
              providerName: "codex",
              runtimeMode: "approval-required",
              activeTurnId: "turn-old",
              lastError: null,
              updatedAt: "2026-02-23T10:00:03.000Z",
            },
          },
        }),
      );
      expect(afterUnchangedOldTurn.threads[0]?.messages[0]?.turnId).toBeNull();

      const afterNewTurn = yield* projectEvent(
        afterUnchangedOldTurn,
        makeEvent({
          sequence: 6,
          type: "thread.session-set",
          aggregateKind: "thread",
          aggregateId: "thread-1",
          occurredAt: "2026-02-23T10:00:04.000Z",
          commandId: "cmd-running-new",
          payload: {
            threadId: "thread-1",
            session: {
              threadId: "thread-1",
              status: "running",
              providerName: "codex",
              runtimeMode: "approval-required",
              activeTurnId: "turn-new",
              lastError: null,
              updatedAt: "2026-02-23T10:00:04.000Z",
            },
          },
        }),
      );
      expect(
        afterNewTurn.threads[0]?.messages.find((message) => message.id === "system-wake-1")?.turnId,
      ).toBe("turn-new");
      expect(
        afterNewTurn.threads[0]?.messages.find((message) => message.id === "system-notice-1")
          ?.turnId,
      ).toBeNull();
      expect(
        afterNewTurn.threads[0]?.turns.find((turn) => turn.turnId === "turn-old"),
      ).toMatchObject({
        state: "completed",
        completedAt: "2026-02-23T10:00:04.000Z",
      });
      expect(
        afterNewTurn.threads[0]?.turns.find((turn) => turn.turnId === "turn-new"),
      ).toMatchObject({
        state: "running",
        completedAt: null,
      });
    }),
  );

  effectIt.effect(
    "does not fallback-retain removed or unrelated prompt messages after revert",
    () =>
      Effect.gen(function* () {
        const createdAt = "2026-02-26T12:00:00.000Z";
        const model = createEmptyReadModel(createdAt);

        const afterCreate = yield* projectEvent(
          model,
          makeEvent({
            sequence: 1,
            type: "thread.created",
            aggregateKind: "thread",
            aggregateId: "thread-revert",
            occurredAt: createdAt,
            commandId: "cmd-create-revert",
            payload: {
              threadId: "thread-revert",
              projectId: "project-1",
              title: "demo",
              modelSelection: {
                provider: ProviderDriverKind.make("codex"),
                model: "gpt-5.3-codex",
              },
              runtimeMode: "full-access",
              branch: null,
              worktreePath: null,
              createdAt,
              updatedAt: createdAt,
            },
          }),
        );

        const events: ReadonlyArray<OrchestrationEvent> = [
          makeEvent({
            sequence: 2,
            type: "thread.turn-diff-completed",
            aggregateKind: "thread",
            aggregateId: "thread-revert",
            occurredAt: "2026-02-26T12:00:01.000Z",
            commandId: "cmd-turn-1",
            payload: {
              threadId: "thread-revert",
              turnId: "turn-1",
              checkpointTurnCount: 1,
              checkpointRef: "refs/t3/checkpoints/thread-revert/turn/1",
              status: "ready",
              files: [],
              assistantMessageId: "assistant-keep",
              completedAt: "2026-02-26T12:00:01.000Z",
            },
          }),
          makeEvent({
            sequence: 3,
            type: "thread.message-sent",
            aggregateKind: "thread",
            aggregateId: "thread-revert",
            occurredAt: "2026-02-26T12:00:01.050Z",
            commandId: "cmd-system-wake-keep",
            payload: {
              threadId: "thread-revert",
              messageId: "system-wake-keep",
              role: "system",
              text: "[sub-agent child-1 completed] done",
              turnId: "turn-1",
              streaming: false,
              createdAt: "2026-02-26T12:00:01.050Z",
              updatedAt: "2026-02-26T12:00:01.050Z",
            },
          }),
          makeEvent({
            sequence: 4,
            type: "thread.message-sent",
            aggregateKind: "thread",
            aggregateId: "thread-revert",
            occurredAt: "2026-02-26T12:00:01.100Z",
            commandId: "cmd-assistant-keep",
            payload: {
              threadId: "thread-revert",
              messageId: "assistant-keep",
              role: "assistant",
              text: "kept",
              turnId: "turn-1",
              streaming: false,
              createdAt: "2026-02-26T12:00:01.100Z",
              updatedAt: "2026-02-26T12:00:01.100Z",
            },
          }),
          makeEvent({
            sequence: 5,
            type: "thread.turn-diff-completed",
            aggregateKind: "thread",
            aggregateId: "thread-revert",
            occurredAt: "2026-02-26T12:00:02.000Z",
            commandId: "cmd-turn-2",
            payload: {
              threadId: "thread-revert",
              turnId: "turn-2",
              checkpointTurnCount: 2,
              checkpointRef: "refs/t3/checkpoints/thread-revert/turn/2",
              status: "ready",
              files: [],
              assistantMessageId: "assistant-remove",
              completedAt: "2026-02-26T12:00:02.000Z",
            },
          }),
          makeEvent({
            sequence: 6,
            type: "thread.message-sent",
            aggregateKind: "thread",
            aggregateId: "thread-revert",
            occurredAt: "2026-02-26T12:00:02.050Z",
            commandId: "cmd-user-remove",
            payload: {
              threadId: "thread-revert",
              messageId: "user-remove",
              role: "user",
              text: "removed",
              turnId: "turn-2",
              streaming: false,
              createdAt: "2026-02-26T12:00:02.050Z",
              updatedAt: "2026-02-26T12:00:02.050Z",
            },
          }),
          makeEvent({
            sequence: 7,
            type: "thread.message-sent",
            aggregateKind: "thread",
            aggregateId: "thread-revert",
            occurredAt: "2026-02-26T12:00:02.100Z",
            commandId: "cmd-assistant-remove",
            payload: {
              threadId: "thread-revert",
              messageId: "assistant-remove",
              role: "assistant",
              text: "removed",
              turnId: "turn-2",
              streaming: false,
              createdAt: "2026-02-26T12:00:02.100Z",
              updatedAt: "2026-02-26T12:00:02.100Z",
            },
          }),
          makeEvent({
            sequence: 8,
            type: "thread.message-sent",
            aggregateKind: "thread",
            aggregateId: "thread-revert",
            occurredAt: "2026-02-26T12:00:02.200Z",
            commandId: "cmd-user-unrelated",
            payload: {
              threadId: "thread-revert",
              messageId: "user-unrelated",
              role: "user",
              text: "unrelated pending user",
              turnId: null,
              streaming: false,
              createdAt: "2026-02-26T12:00:02.200Z",
              updatedAt: "2026-02-26T12:00:02.200Z",
            },
          }),
          makeEvent({
            sequence: 9,
            type: "thread.reverted",
            aggregateKind: "thread",
            aggregateId: "thread-revert",
            occurredAt: "2026-02-26T12:00:03.000Z",
            commandId: "cmd-revert",
            payload: {
              threadId: "thread-revert",
              turnCount: 1,
            },
          }),
        ];

        let afterRevert = afterCreate;
        for (const event of events) {
          afterRevert = yield* projectEvent(afterRevert, event);
        }

        const thread = afterRevert.threads[0];
        expect(
          thread?.messages.map((message) => ({
            id: message.id,
            role: message.role,
            turnId: message.turnId,
          })),
        ).toEqual([
          { id: "system-wake-keep", role: "system", turnId: "turn-1" },
          { id: "assistant-keep", role: "assistant", turnId: "turn-1" },
        ]);
      }),
  );

  effectIt.effect("caps message and checkpoint retention for long-lived threads", () =>
    Effect.gen(function* () {
      const createdAt = "2026-03-01T10:00:00.000Z";
      const model = createEmptyReadModel(createdAt);

      const afterCreate = yield* projectEvent(
        model,
        makeEvent({
          sequence: 1,
          type: "thread.created",
          aggregateKind: "thread",
          aggregateId: "thread-capped",
          occurredAt: createdAt,
          commandId: "cmd-create-capped",
          payload: {
            threadId: "thread-capped",
            projectId: "project-1",
            title: "capped",
            modelSelection: {
              provider: ProviderDriverKind.make("codex"),
              model: "gpt-5-codex",
            },
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        }),
      );

      const messageEvents: ReadonlyArray<OrchestrationEvent> = Array.from(
        { length: 2_100 },
        (_, index) =>
          makeEvent({
            sequence: index + 2,
            type: "thread.message-sent",
            aggregateKind: "thread",
            aggregateId: "thread-capped",
            occurredAt: `2026-03-01T10:00:${String(index % 60).padStart(2, "0")}.000Z`,
            commandId: `cmd-message-${index}`,
            payload: {
              threadId: "thread-capped",
              messageId: `msg-${index}`,
              role: "assistant",
              text: `message-${index}`,
              turnId: `turn-${index}`,
              streaming: false,
              createdAt: `2026-03-01T10:00:${String(index % 60).padStart(2, "0")}.000Z`,
              updatedAt: `2026-03-01T10:00:${String(index % 60).padStart(2, "0")}.000Z`,
            },
          }),
      );
      let afterMessages = afterCreate;
      for (const event of messageEvents) {
        afterMessages = yield* projectEvent(afterMessages, event);
      }

      const checkpointEvents: ReadonlyArray<OrchestrationEvent> = Array.from(
        { length: 600 },
        (_, index) =>
          makeEvent({
            sequence: index + 2_102,
            type: "thread.turn-diff-completed",
            aggregateKind: "thread",
            aggregateId: "thread-capped",
            occurredAt: `2026-03-01T10:30:${String(index % 60).padStart(2, "0")}.000Z`,
            commandId: `cmd-checkpoint-${index}`,
            payload: {
              threadId: "thread-capped",
              turnId: `turn-${index}`,
              checkpointTurnCount: index + 1,
              checkpointRef: `refs/t3/checkpoints/thread-capped/turn/${index + 1}`,
              status: "ready",
              files: [],
              assistantMessageId: `msg-${index}`,
              completedAt: `2026-03-01T10:30:${String(index % 60).padStart(2, "0")}.000Z`,
            },
          }),
      );
      let finalState = afterMessages;
      for (const event of checkpointEvents) {
        finalState = yield* projectEvent(finalState, event);
      }

      const thread = finalState.threads[0];
      expect(thread?.messages).toHaveLength(2_000);
      expect(thread?.messages[0]?.id).toBe("msg-100");
      expect(thread?.messages.at(-1)?.id).toBe("msg-2099");
      expect(thread?.checkpoints).toHaveLength(500);
      expect(thread?.checkpoints[0]?.turnId).toBe("turn-100");
      expect(thread?.checkpoints.at(-1)?.turnId).toBe("turn-599");
    }),
  );
});
