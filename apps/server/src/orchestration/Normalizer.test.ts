import {
  CommandId,
  type ClientOrchestrationCommand,
  IsoDateTime,
  MessageId,
  NonNegativeInt,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationReadModel,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import * as ServerConfig from "../config.ts";
import {
  type OrchestrationCommandReceipt,
  OrchestrationCommandReceiptRepository,
} from "../persistence/Services/OrchestrationCommandReceipts.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { sessionDispatchAuthority } from "./commandAudienceGuard.ts";
import {
  canonicalizeClientCommandTimestamps,
  normalizeAuthorizedDispatchCommand,
} from "./Normalizer.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";

const createdAt = "2026-01-01T00:00:00.000Z";

const normalizerTestLayer = (
  getReadModel: () => OrchestrationReadModel,
  configPrefix: string,
  getReceipt: () => Option.Option<OrchestrationCommandReceipt> = () => Option.none(),
) =>
  Layer.mergeAll(
    Layer.mock(ProjectionSnapshotQuery)({
      getCommandReadModel: () => Effect.sync(getReadModel),
    }),
    Layer.mock(OrchestrationCommandReceiptRepository)({
      getByCommandId: () => Effect.sync(getReceipt),
    }),
    WorkspacePaths.layer,
    ServerConfig.layerTest(process.cwd(), { prefix: configPrefix }),
  ).pipe(Layer.provideMerge(NodeServices.layer));

it.effect("authorizes project creation before creating a missing workspace root", () => {
  let authorizationReadModel: OrchestrationReadModel | undefined;
  return Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const parent = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-normalizer-command-guard-",
    });
    const deniedWorkspaceRoot = path.join(parent, "private-project-root");
    const privateProjectId = ProjectId.make("normalizer-private-project");
    const readModel: OrchestrationReadModel = {
      snapshotSequence: 1,
      updatedAt: createdAt,
      projects: [
        {
          id: privateProjectId,
          title: "Private project",
          workspaceRoot: deniedWorkspaceRoot,
          dataAudience: "private",
          defaultModelSelection: null,
          scripts: [],
          createdAt,
          updatedAt: createdAt,
          deletedAt: null,
        },
      ],
      threads: [],
    };
    authorizationReadModel = readModel;
    const result = yield* Effect.exit(
      normalizeAuthorizedDispatchCommand(
        {
          type: "project.create",
          commandId: CommandId.make("normalizer-denied-project-create"),
          projectId: ProjectId.make("normalizer-factory-project"),
          title: "Denied factory project",
          workspaceRoot: deniedWorkspaceRoot,
          createWorkspaceRootIfMissing: true,
          createdAt,
        },
        sessionDispatchAuthority({
          subject: "normalizer-factory-test",
          audienceCeiling: "factory",
        }),
      ),
    );

    expect(result._tag).toBe("Failure");
    expect(yield* fileSystem.exists(deniedWorkspaceRoot)).toBe(false);
  }).pipe(
    Effect.provide(
      normalizerTestLayer(() => {
        if (authorizationReadModel === undefined) {
          throw new Error("Authorization read model was not initialized.");
        }
        return authorizationReadModel;
      }, "t3-normalizer-config-"),
    ),
  );
});

it.effect("authorizes turn attachments before persisting them", () => {
  let authorizationReadModel: OrchestrationReadModel | undefined;
  return Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const config = yield* ServerConfig.ServerConfig;
    const privateProjectId = ProjectId.make("normalizer-private-attachment-project");
    const privateThreadId = ThreadId.make("normalizer-private-attachment-thread");
    const modelSelection = {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5-codex",
    } as const;
    authorizationReadModel = {
      snapshotSequence: 1,
      updatedAt: createdAt,
      projects: [
        {
          id: privateProjectId,
          title: "Private attachment project",
          workspaceRoot: "/tmp/normalizer-private-attachment-project",
          dataAudience: "private",
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
          title: "Private attachment thread",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          latestTurn: null,
          createdAt,
          updatedAt: createdAt,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          deletedAt: null,
          messages: [],
          turns: [],
          proposedPlans: [],
          activities: [],
          checkpoints: [],
          session: null,
        },
      ],
    };

    const result = yield* Effect.exit(
      normalizeAuthorizedDispatchCommand(
        {
          type: "thread.turn.start",
          commandId: CommandId.make("normalizer-denied-attachment-turn"),
          threadId: privateThreadId,
          message: {
            messageId: MessageId.make("normalizer-denied-attachment-message"),
            role: "user",
            text: "must not persist",
            attachments: [
              {
                type: "image",
                name: "private.png",
                mimeType: "image/png",
                sizeBytes: NonNegativeInt.make(5),
                dataUrl: "data:image/png;base64,aGVsbG8=",
              },
            ],
          },
          runtimeMode: "full-access",
          interactionMode: "default",
          createdAt,
        },
        sessionDispatchAuthority({
          subject: "normalizer-factory-attachment-test",
          audienceCeiling: "factory",
        }),
      ),
    );
    const attachmentEntries = yield* fileSystem.readDirectory(config.attachmentsDir);

    expect(result._tag).toBe("Failure");
    expect(attachmentEntries).toEqual([]);
  }).pipe(
    Effect.provide(
      normalizerTestLayer(() => {
        if (authorizationReadModel === undefined) {
          throw new Error("Authorization read model was not initialized.");
        }
        return authorizationReadModel;
      }, "t3-normalizer-attachment-config-"),
    ),
  );
});

it.effect("replays a visible receipt before current cascade authorization", () => {
  const commandId = CommandId.make("normalizer-visible-archive-receipt");
  const projectId = ProjectId.make("normalizer-receipt-project");
  const factoryThreadId = ThreadId.make("normalizer-receipt-factory-thread");
  const hiddenThreadId = ThreadId.make("normalizer-receipt-hidden-thread");
  const modelSelection = {
    instanceId: ProviderInstanceId.make("codex"),
    model: "gpt-5-codex",
  } as const;
  const readModel: OrchestrationReadModel = {
    snapshotSequence: 4,
    updatedAt: createdAt,
    projects: [
      {
        id: projectId,
        title: "Factory receipt project",
        workspaceRoot: "/tmp/normalizer-receipt-project",
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
        id: factoryThreadId,
        projectId,
        dataAudience: "factory",
        title: "Archived factory thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        latestTurn: null,
        createdAt,
        updatedAt: createdAt,
        archivedAt: createdAt,
        settledOverride: null,
        settledAt: null,
        deletedAt: null,
        messages: [],
        turns: [],
        proposedPlans: [],
        activities: [],
        checkpoints: [],
        session: null,
      },
      {
        id: hiddenThreadId,
        projectId,
        dataAudience: "private",
        title: "Hidden descendant added after the receipt",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        latestTurn: null,
        createdAt,
        updatedAt: createdAt,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        deletedAt: null,
        parentThreadId: factoryThreadId,
        messages: [],
        turns: [],
        proposedPlans: [],
        activities: [],
        checkpoints: [],
        session: null,
      },
    ],
  };
  const receipt: OrchestrationCommandReceipt = {
    commandId,
    aggregateKind: "thread",
    aggregateId: factoryThreadId,
    acceptedAt: IsoDateTime.make(createdAt),
    resultSequence: NonNegativeInt.make(3),
    status: "accepted",
    error: null,
  };

  return Effect.gen(function* () {
    const command = {
      type: "thread.archive" as const,
      commandId,
      threadId: factoryThreadId,
    };
    expect(
      yield* normalizeAuthorizedDispatchCommand(
        command,
        sessionDispatchAuthority({
          subject: "normalizer-factory-receipt-test",
          audienceCeiling: "factory",
        }),
      ),
    ).toEqual(command);
  }).pipe(
    Effect.provide(
      normalizerTestLayer(
        () => readModel,
        "t3-normalizer-receipt-config-",
        () => Option.some(receipt),
      ),
    ),
  );
});

it.effect("does not replay a receipt whose aggregate is outside the caller audience", () => {
  const commandId = CommandId.make("normalizer-hidden-receipt");
  const privateProjectId = ProjectId.make("normalizer-hidden-receipt-project");
  const privateThreadId = ThreadId.make("normalizer-hidden-receipt-thread");
  const modelSelection = {
    instanceId: ProviderInstanceId.make("codex"),
    model: "gpt-5-codex",
  } as const;
  const readModel: OrchestrationReadModel = {
    snapshotSequence: 2,
    updatedAt: createdAt,
    projects: [
      {
        id: privateProjectId,
        title: "Private receipt project",
        workspaceRoot: "/tmp/normalizer-hidden-receipt-project",
        dataAudience: "private",
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
        title: "Private receipt thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        latestTurn: null,
        createdAt,
        updatedAt: createdAt,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        deletedAt: null,
        messages: [],
        turns: [],
        proposedPlans: [],
        activities: [],
        checkpoints: [],
        session: null,
      },
    ],
  };
  const receipt: OrchestrationCommandReceipt = {
    commandId,
    aggregateKind: "thread",
    aggregateId: privateThreadId,
    acceptedAt: IsoDateTime.make(createdAt),
    resultSequence: NonNegativeInt.make(2),
    status: "accepted",
    error: null,
  };

  return Effect.gen(function* () {
    const exit = yield* Effect.exit(
      normalizeAuthorizedDispatchCommand(
        {
          type: "thread.delete",
          commandId,
          threadId: privateThreadId,
        },
        sessionDispatchAuthority({
          subject: "normalizer-hidden-receipt-test",
          audienceCeiling: "factory",
        }),
      ),
    );
    expect(exit._tag).toBe("Failure");
    expect(String(exit)).not.toContain("normalizer-hidden-receipt-project");
  }).pipe(
    Effect.provide(
      normalizerTestLayer(
        () => readModel,
        "t3-normalizer-hidden-receipt-config-",
        () => Option.some(receipt),
      ),
    ),
  );
});

const clientCreatedAt = "2031-01-01T00:00:00.000Z";
const serverReceivedAt = "2026-07-18T00:00:00.000Z";

describe("canonicalizeClientCommandTimestamps", () => {
  it("replaces a client command timestamp with the server receipt timestamp", () => {
    const command: ClientOrchestrationCommand = {
      type: "project.create",
      commandId: CommandId.make("command-1"),
      projectId: ProjectId.make("project-1"),
      title: "Clock-safe project",
      workspaceRoot: "/tmp/clock-safe-project",
      createdAt: clientCreatedAt,
    };

    expect(canonicalizeClientCommandTimestamps(command, serverReceivedAt)).toEqual({
      ...command,
      createdAt: serverReceivedAt,
    });
  });

  it("replaces both timestamps when the first turn bootstraps a thread", () => {
    const command: ClientOrchestrationCommand = {
      type: "thread.turn.start",
      commandId: CommandId.make("command-2"),
      threadId: ThreadId.make("thread-1"),
      message: {
        messageId: MessageId.make("message-1"),
        role: "user",
        text: "Start a thread",
        attachments: [],
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      bootstrap: {
        createThread: {
          projectId: ProjectId.make("project-1"),
          title: "Clock-safe thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5.4",
          },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt: clientCreatedAt,
        },
      },
      createdAt: clientCreatedAt,
    };

    const result = canonicalizeClientCommandTimestamps(command, serverReceivedAt);

    expect(result.type).toBe("thread.turn.start");
    if (result.type !== "thread.turn.start") {
      throw new Error("Expected a thread.turn.start command");
    }
    expect(result.createdAt).toBe(serverReceivedAt);
    expect(result.bootstrap?.createThread?.createdAt).toBe(serverReceivedAt);
  });
});
