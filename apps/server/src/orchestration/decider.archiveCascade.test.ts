import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationCommand,
  type OrchestrationEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";

import { decideOrchestrationCommand } from "./decider.ts";
import { createEmptyReadModel, projectEvent } from "./projector.ts";

const asCommandId = (v: string): CommandId => CommandId.make(v);
const asEventId = (v: string): EventId => EventId.make(v);
const asProjectId = (v: string): ProjectId => ProjectId.make(v);
const asThreadId = (v: string): ThreadId => ThreadId.make(v);

type PlannedEvent = Omit<OrchestrationEvent, "sequence">;
const now = "2026-01-01T00:00:00.000Z";
const projectId = asProjectId("project-archive");

const project = (base: ReturnType<typeof createEmptyReadModel>) =>
  projectEvent(base, {
    sequence: 1,
    eventId: asEventId("evt-project"),
    aggregateKind: "project",
    aggregateId: projectId,
    type: "project.created",
    occurredAt: now,
    commandId: asCommandId("cmd-project"),
    causationEventId: null,
    correlationId: asCommandId("cmd-project"),
    metadata: {},
    payload: {
      projectId,
      title: "Archive Project",
      workspaceRoot: "/tmp/archive",
      defaultModelSelection: null,
      scripts: [],
      createdAt: now,
      updatedAt: now,
    },
  });

// Apply an event to a read model, advancing the sequence.
const apply = (
  model: ReturnType<typeof createEmptyReadModel>,
  seq: number,
  event: Omit<OrchestrationEvent, "sequence">,
) => projectEvent(model, { ...event, sequence: seq } as Parameters<typeof projectEvent>[1]);

const createThreadEvent = (id: string, seq: number) => ({
  seq,
  event: {
    eventId: asEventId(`evt-create-${id}`),
    aggregateKind: "thread" as const,
    aggregateId: asThreadId(id),
    type: "thread.created" as const,
    occurredAt: now,
    commandId: asCommandId(`cmd-create-${id}`),
    causationEventId: null,
    correlationId: asCommandId(`cmd-create-${id}`),
    metadata: {},
    payload: {
      threadId: asThreadId(id),
      projectId,
      title: `Thread ${id}`,
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      runtimeMode: "approval-required" as const,
      branch: null,
      worktreePath: null,
      createdAt: now,
      updatedAt: now,
    },
  },
});

const parentSetEvent = (childId: string, parentId: string, seq: number) => ({
  seq,
  event: {
    eventId: asEventId(`evt-parent-${childId}`),
    aggregateKind: "thread" as const,
    aggregateId: asThreadId(childId),
    type: "thread.parent-set" as const,
    occurredAt: now,
    commandId: asCommandId(`cmd-parent-${childId}`),
    causationEventId: null,
    correlationId: asCommandId(`cmd-parent-${childId}`),
    metadata: {},
    payload: {
      threadId: asThreadId(childId),
      parentThreadId: asThreadId(parentId),
      updatedAt: now,
    },
  },
});

// Tree: parent -> childA, childB; childA -> grandchild. Plus an unrelated root.
const seedTree = Effect.gen(function* () {
  let model = yield* project(createEmptyReadModel(now));
  const steps = [
    createThreadEvent("parent", 2),
    createThreadEvent("childA", 3),
    createThreadEvent("childB", 4),
    createThreadEvent("grandchild", 5),
    createThreadEvent("unrelated", 6),
    parentSetEvent("childA", "parent", 7),
    parentSetEvent("childB", "parent", 8),
    parentSetEvent("grandchild", "childA", 9),
  ];
  for (const { seq, event } of steps) {
    model = yield* apply(model, seq, event);
  }
  return model;
});

it.layer(NodeServices.layer)("archive cascade decider", (it) => {
  it.effect("archives a parent and every unarchived descendant", () =>
    Effect.gen(function* () {
      const readModel = yield* seedTree;
      const command: Extract<OrchestrationCommand, { type: "thread.archive" }> = {
        type: "thread.archive",
        commandId: asCommandId("cmd-archive-parent"),
        threadId: asThreadId("parent"),
      };
      const decided = yield* decideOrchestrationCommand({ command, readModel });
      const events = (Array.isArray(decided) ? decided : [decided]) as PlannedEvent[];

      expect(events.every((e) => e.type === "thread.archived")).toBe(true);
      const archivedIds = events.map((e) => (e.payload as { threadId: string }).threadId).sort();
      // parent + childA + childB + grandchild; NOT the unrelated root.
      expect(archivedIds).toEqual(["childA", "childB", "grandchild", "parent"]);
    }),
  );

  it.effect("skips already-archived descendants (idempotent)", () =>
    Effect.gen(function* () {
      let readModel = yield* seedTree;
      // Pre-archive childB.
      readModel = yield* apply(readModel, 10, {
        eventId: asEventId("evt-prearchive-childB"),
        aggregateKind: "thread",
        aggregateId: asThreadId("childB"),
        type: "thread.archived",
        occurredAt: now,
        commandId: asCommandId("cmd-prearchive-childB"),
        causationEventId: null,
        correlationId: asCommandId("cmd-prearchive-childB"),
        metadata: {},
        payload: { threadId: asThreadId("childB"), archivedAt: now, updatedAt: now },
      });

      const decided = yield* decideOrchestrationCommand({
        command: {
          type: "thread.archive",
          commandId: asCommandId("cmd-archive-parent-2"),
          threadId: asThreadId("parent"),
        },
        readModel,
      });
      const events = (Array.isArray(decided) ? decided : [decided]) as PlannedEvent[];
      const archivedIds = events.map((e) => (e.payload as { threadId: string }).threadId).sort();
      // childB already archived -> excluded; the rest still cascade.
      expect(archivedIds).toEqual(["childA", "grandchild", "parent"]);
    }),
  );

  it.effect("archiving a leaf child does not touch its parent or siblings", () =>
    Effect.gen(function* () {
      const readModel = yield* seedTree;
      const decided = yield* decideOrchestrationCommand({
        command: {
          type: "thread.archive",
          commandId: asCommandId("cmd-archive-grandchild"),
          threadId: asThreadId("grandchild"),
        },
        readModel,
      });
      const events = (Array.isArray(decided) ? decided : [decided]) as PlannedEvent[];
      const archivedIds = events.map((e) => (e.payload as { threadId: string }).threadId);
      expect(archivedIds).toEqual(["grandchild"]);
    }),
  );

  it.effect("traverses through an already-archived intermediate to reach live grandchildren", () =>
    // An archived childA must not sever the subtree: its unarchived grandchild
    // still gets archived when the parent is archived.
    Effect.gen(function* () {
      let readModel = yield* seedTree;
      readModel = yield* apply(readModel, 10, {
        eventId: asEventId("evt-prearchive-childA"),
        aggregateKind: "thread",
        aggregateId: asThreadId("childA"),
        type: "thread.archived",
        occurredAt: now,
        commandId: asCommandId("cmd-prearchive-childA"),
        causationEventId: null,
        correlationId: asCommandId("cmd-prearchive-childA"),
        metadata: {},
        payload: { threadId: asThreadId("childA"), archivedAt: now, updatedAt: now },
      });

      const decided = yield* decideOrchestrationCommand({
        command: {
          type: "thread.archive",
          commandId: asCommandId("cmd-archive-parent-3"),
          threadId: asThreadId("parent"),
        },
        readModel,
      });
      const events = (Array.isArray(decided) ? decided : [decided]) as PlannedEvent[];
      const archivedIds = events.map((e) => (e.payload as { threadId: string }).threadId).sort();
      // childA already archived (skipped) but its live grandchild is still reached.
      expect(archivedIds).toEqual(["childB", "grandchild", "parent"]);
    }),
  );
});
