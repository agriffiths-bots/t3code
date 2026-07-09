// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  ModelSelection,
  ProviderRuntimeEvent,
  ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
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
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { it as effectIt } from "@effect/vitest";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { deriveServerPaths, ServerConfig } from "../../config.ts";
import { TextGenerationError } from "@t3tools/contracts";
import { ProviderAdapterRequestError } from "../../provider/Errors.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { PendingDispatchRepositoryLive } from "../../persistence/Layers/PendingDispatches.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import {
  PendingDispatchId,
  PendingDispatchRepository,
} from "../../persistence/Services/PendingDispatches.ts";
import {
  ProviderService,
  type ProviderServiceShape,
} from "../../provider/Services/ProviderService.ts";
import { makeProviderRegistryLayer } from "../../provider/testUtils/providerRegistryMock.ts";
import { TextGeneration, type TextGenerationShape } from "../../textGeneration/TextGeneration.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { OrchestrationEngineLive } from "./OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";
import {
  providerErrorLabel,
  providerErrorLabelFromInstanceHint,
  ProviderCommandReactorLive,
} from "./ProviderCommandReactor.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProviderCommandReactor } from "../Services/ProviderCommandReactor.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { readDetailedReadModel } from "../testUtils/readModel.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import { ServerSettingsService } from "../../serverSettings.ts";
import { VcsStatusBroadcaster } from "../../vcs/VcsStatusBroadcaster.ts";
import * as GitWorkflowService from "../../git/GitWorkflowService.ts";

const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asApprovalRequestId = (value: string): ApprovalRequestId => ApprovalRequestId.make(value);
const asMessageId = (value: string): MessageId => MessageId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);
const asPendingDispatchId = (value: string): PendingDispatchId => PendingDispatchId.make(value);

const deriveServerPathsSync = (baseDir: string, devUrl: URL | undefined) =>
  Effect.runSync(deriveServerPaths(baseDir, devUrl).pipe(Effect.provide(NodeServices.layer)));

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 10_000,
): Promise<void> {
  const deadline = (await Effect.runPromise(Clock.currentTimeMillis)) + timeoutMs;
  const poll = async (): Promise<void> => {
    if (await predicate()) {
      return;
    }
    if ((await Effect.runPromise(Clock.currentTimeMillis)) >= deadline) {
      throw new Error("Timed out waiting for expectation.");
    }
    await Effect.runPromise(Effect.yieldNow);
    return poll();
  };

  return poll();
}

describe("ProviderCommandReactor", () => {
  let runtime: ManagedRuntime.ManagedRuntime<
    | OrchestrationEngineService
    | ProviderCommandReactor
    | ProjectionSnapshotQuery
    | PendingDispatchRepository,
    unknown
  > | null = null;
  let scope: Scope.Closeable | null = null;
  const createdStateDirs = new Set<string>();
  const createdBaseDirs = new Set<string>();

  afterEach(async () => {
    if (scope) {
      await Effect.runPromise(Scope.close(scope, Exit.void));
    }
    scope = null;
    if (runtime) {
      await runtime.dispose();
    }
    runtime = null;
    for (const stateDir of createdStateDirs) {
      NodeFS.rmSync(stateDir, { recursive: true, force: true });
    }
    createdStateDirs.clear();
    for (const baseDir of createdBaseDirs) {
      NodeFS.rmSync(baseDir, { recursive: true, force: true });
    }
    createdBaseDirs.clear();
  });

  describe("provider error attribution", () => {
    it("uses the current provider instance slug when current instance lookup fails", () => {
      expect(
        providerErrorLabelFromInstanceHint({
          instanceId: "codex_personal",
          modelSelectionInstanceId: "codex",
          sessionProvider: "codex",
        }),
      ).toBe("codex_personal");
    });

    it("uses the desired provider instance slug when desired instance lookup fails", () => {
      expect(
        providerErrorLabelFromInstanceHint({
          instanceId: "claude_openrouter",
        }),
      ).toBe("claude_openrouter");
    });

    it("uses the unknown driver kind when the resolved driver is not registered locally", () => {
      expect(providerErrorLabel("third_party_driver")).toBe("third_party_driver");
    });
  });

  async function createHarness(input?: {
    readonly baseDir?: string;
    readonly threadModelSelection?: ModelSelection;
    readonly sessionModelSwitch?: "unsupported" | "in-session";
    readonly requiresNewThreadForModelChange?: boolean;
    readonly startReactor?: boolean;
  }) {
    const now = "2026-01-01T00:00:00.000Z";
    const baseDir =
      input?.baseDir ?? NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3code-reactor-"));
    createdBaseDirs.add(baseDir);
    const { stateDir } = deriveServerPathsSync(baseDir, undefined);
    createdStateDirs.add(stateDir);
    const runtimeEventPubSub = Effect.runSync(PubSub.unbounded<ProviderRuntimeEvent>());
    let nextSessionIndex = 1;
    const runtimeSessions: Array<ProviderSession> = [];
    const modelSelection = input?.threadModelSelection ?? {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5-codex",
    };
    const startSession = vi.fn((_: unknown, input: unknown) => {
      const sessionIndex = nextSessionIndex++;
      const resumeCursor =
        typeof input === "object" && input !== null && "resumeCursor" in input
          ? input.resumeCursor
          : undefined;
      const activeTurnId =
        typeof input === "object" && input !== null && "activeTurnId" in input
          ? (input.activeTurnId as TurnId | undefined)
          : undefined;
      const threadId =
        typeof input === "object" &&
        input !== null &&
        "threadId" in input &&
        typeof input.threadId === "string"
          ? ThreadId.make(input.threadId)
          : ThreadId.make(`thread-${sessionIndex}`);
      const inputModelSelection =
        typeof input === "object" && input !== null && "modelSelection" in input
          ? (input.modelSelection as ModelSelection | undefined)
          : undefined;
      const providerInstanceId =
        typeof input === "object" && input !== null && "providerInstanceId" in input
          ? (input.providerInstanceId as ProviderInstanceId | undefined)
          : inputModelSelection?.instanceId;
      const provider =
        typeof input === "object" &&
        input !== null &&
        "provider" in input &&
        typeof input.provider === "string"
          ? (input.provider as ProviderSession["provider"])
          : ProviderDriverKind.make(inputModelSelection?.instanceId ?? modelSelection.instanceId);
      const session: ProviderSession = {
        provider,
        ...(providerInstanceId ? { providerInstanceId } : {}),
        status: activeTurnId !== undefined ? ("running" as const) : ("ready" as const),
        runtimeMode:
          typeof input === "object" &&
          input !== null &&
          "runtimeMode" in input &&
          (input.runtimeMode === "approval-required" || input.runtimeMode === "full-access")
            ? input.runtimeMode
            : "full-access",
        ...(typeof input === "object" &&
        input !== null &&
        "cwd" in input &&
        typeof input.cwd === "string"
          ? { cwd: input.cwd }
          : {}),
        ...((inputModelSelection?.model ?? modelSelection.model)
          ? { model: inputModelSelection?.model ?? modelSelection.model }
          : {}),
        threadId,
        resumeCursor: resumeCursor ?? { opaque: `resume-${sessionIndex}` },
        ...(activeTurnId !== undefined ? { activeTurnId } : {}),
        createdAt: now,
        updatedAt: now,
      };
      runtimeSessions.push(session);
      return Effect.succeed(session);
    });
    const sendTurn = vi.fn((_: unknown) =>
      Effect.succeed({
        threadId: ThreadId.make("thread-1"),
        turnId: asTurnId("turn-1"),
      }),
    );
    const interruptTurn = vi.fn((_: unknown) => Effect.void);
    const respondToRequest = vi.fn<ProviderServiceShape["respondToRequest"]>(() => Effect.void);
    const respondToUserInput = vi.fn<ProviderServiceShape["respondToUserInput"]>(() => Effect.void);
    const stopSession = vi.fn((input: unknown) =>
      Effect.sync(() => {
        const threadId =
          typeof input === "object" && input !== null && "threadId" in input
            ? (input as { threadId?: ThreadId }).threadId
            : undefined;
        if (!threadId) {
          return;
        }
        const index = runtimeSessions.findIndex((session) => session.threadId === threadId);
        if (index >= 0) {
          runtimeSessions.splice(index, 1);
        }
      }),
    );
    const listSessions = vi.fn<ProviderServiceShape["listSessions"]>(() =>
      Effect.succeed(runtimeSessions),
    );
    const renameBranch = vi.fn((input: unknown) =>
      Effect.succeed({
        branch:
          typeof input === "object" &&
          input !== null &&
          "newBranch" in input &&
          typeof input.newBranch === "string"
            ? input.newBranch
            : "renamed-branch",
      }),
    );
    const refreshStatus = vi.fn((_: string) =>
      Effect.succeed({
        isRepo: true,
        hasPrimaryRemote: true,
        isDefaultRef: false,
        refName: "renamed-branch",
        hasWorkingTreeChanges: false,
        workingTree: {
          files: [],
          insertions: 0,
          deletions: 0,
        },
        hasUpstream: true,
        aheadCount: 0,
        behindCount: 0,
        pr: null,
      }),
    );
    const generateBranchName = vi.fn<TextGenerationShape["generateBranchName"]>((_) =>
      Effect.fail(
        new TextGenerationError({
          operation: "generateBranchName",
          detail: "disabled in test harness",
        }),
      ),
    );
    const generateThreadTitle = vi.fn<TextGenerationShape["generateThreadTitle"]>((_) =>
      Effect.fail(
        new TextGenerationError({
          operation: "generateThreadTitle",
          detail: "disabled in test harness",
        }),
      ),
    );
    const providerSnapshots = [
      {
        instanceId: modelSelection.instanceId,
        ...(input?.requiresNewThreadForModelChange === true
          ? { requiresNewThreadForModelChange: true }
          : {}),
      },
    ];

    const unsupported = () => Effect.die(new Error("Unsupported provider call in test")) as never;
    const service: ProviderServiceShape = {
      startSession: startSession as ProviderServiceShape["startSession"],
      sendTurn: sendTurn as ProviderServiceShape["sendTurn"],
      interruptTurn: interruptTurn as ProviderServiceShape["interruptTurn"],
      respondToRequest: respondToRequest as ProviderServiceShape["respondToRequest"],
      respondToUserInput: respondToUserInput as ProviderServiceShape["respondToUserInput"],
      stopSession: stopSession as ProviderServiceShape["stopSession"],
      listSessions,
      getCapabilities: (_provider) =>
        Effect.succeed({
          sessionModelSwitch: input?.sessionModelSwitch ?? "in-session",
        }),
      getInstanceInfo: (instanceId) => {
        const raw = String(instanceId);
        const driverKind = ProviderDriverKind.make(
          raw.startsWith("claude") ? "claudeAgent" : raw.startsWith("codex") ? "codex" : raw,
        );
        return Effect.succeed({
          instanceId,
          driverKind,
          displayName: undefined,
          enabled: true,
          continuationIdentity: {
            driverKind,
            continuationKey:
              driverKind === ProviderDriverKind.make("codex")
                ? "codex:home:/shared-codex"
                : `${driverKind}:instance:${instanceId}`,
          },
        });
      },
      rollbackConversation: () => unsupported(),
      get streamEvents() {
        return Stream.fromPubSub(runtimeEventPubSub);
      },
    };

    const orchestrationLayer = OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(OrchestrationProjectionPipelineLive),
      Layer.provide(OrchestrationEventStoreLive),
      Layer.provide(OrchestrationCommandReceiptRepositoryLive),
      Layer.provide(RepositoryIdentityResolver.layer),
      Layer.provide(SqlitePersistenceMemory),
    );
    const projectionSnapshotLayer = OrchestrationProjectionSnapshotQueryLive.pipe(
      Layer.provide(RepositoryIdentityResolver.layer),
      Layer.provide(SqlitePersistenceMemory),
    );
    const layer = ProviderCommandReactorLive.pipe(
      Layer.provideMerge(PendingDispatchRepositoryLive),
      Layer.provideMerge(orchestrationLayer),
      Layer.provideMerge(projectionSnapshotLayer),
      Layer.provideMerge(Layer.succeed(ProviderService, service)),
      Layer.provideMerge(makeProviderRegistryLayer(providerSnapshots as never)),
      Layer.provideMerge(
        Layer.mock(GitWorkflowService.GitWorkflowService)({
          renameBranch,
        } satisfies Partial<GitWorkflowService.GitWorkflowService["Service"]>),
      ),
      Layer.provideMerge(
        Layer.succeed(VcsStatusBroadcaster, {
          getStatus: () => Effect.die("getStatus should not be called in this test"),
          refreshLocalStatus: () =>
            Effect.die("refreshLocalStatus should not be called in this test"),
          refreshStatus,
          streamStatus: () => Stream.die("streamStatus should not be called in this test"),
        }),
      ),
      Layer.provideMerge(
        Layer.mock(TextGeneration, {
          generateBranchName,
          generateThreadTitle,
        }),
      ),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), baseDir)),
      Layer.provideMerge(SqlitePersistenceMemory),
      Layer.provideMerge(NodeServices.layer),
    );
    runtime = ManagedRuntime.make(layer);

    const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
    const snapshotQuery = await runtime.runPromise(Effect.service(ProjectionSnapshotQuery));
    const reactor = await runtime.runPromise(Effect.service(ProviderCommandReactor));
    let reactorStarted = false;
    const startReactor = async () => {
      if (reactorStarted) {
        return;
      }
      scope = await Effect.runPromise(Scope.make("sequential"));
      await Effect.runPromise(reactor.start().pipe(Scope.provide(scope)));
      reactorStarted = true;
    };
    if (input?.startReactor !== false) {
      await startReactor();
    }
    const runStartupRecoveryAgain = async () => {
      const transientScope = await Effect.runPromise(Scope.make("sequential"));
      try {
        await Effect.runPromise(reactor.start().pipe(Scope.provide(transientScope)));
      } finally {
        await Effect.runPromise(Scope.close(transientScope, Exit.void));
      }
    };
    const drain = () => Effect.runPromise(reactor.drain);
    const listPendingDispatches = () =>
      runtime!.runPromise(
        Effect.flatMap(Effect.service(PendingDispatchRepository), (repo) => repo.listAll()),
      );

    await Effect.runPromise(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-create"),
        projectId: asProjectId("project-1"),
        title: "Provider Project",
        workspaceRoot: "/tmp/provider-project",
        defaultModelSelection: modelSelection,
        createdAt: now,
      }),
    );
    await Effect.runPromise(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-create"),
        threadId: ThreadId.make("thread-1"),
        projectId: asProjectId("project-1"),
        title: "Thread",
        modelSelection: modelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt: now,
      }),
    );

    return {
      engine,
      readModel: () => readDetailedReadModel(snapshotQuery),
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      renameBranch,
      refreshStatus,
      generateBranchName,
      generateThreadTitle,
      runtimeSessions,
      stateDir,
      drain,
      startReactor,
      runStartupRecoveryAgain,
      listPendingDispatches,
    };
  }

  it("reacts to thread.turn.start by ensuring session and sending provider turn", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-1"),
          role: "user",
          text: "hello reactor",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.startSession.mock.calls[0]?.[0]).toEqual(ThreadId.make("thread-1"));
    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      cwd: "/tmp/provider-project",
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
      runtimeMode: "approval-required",
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.threadId).toBe("thread-1");
    expect(thread?.session?.runtimeMode).toBe("approval-required");
  });

  it("accepts server-originated system turn starts as provider input", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-system-wake"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("system-message-wake"),
          role: "system",
          text: "[sub-agent child-1 completed] done",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.generateThreadTitle).not.toHaveBeenCalled();
    expect(harness.generateBranchName).not.toHaveBeenCalled();
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      input: "[sub-agent child-1 completed] done",
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.messages.at(-1)).toMatchObject({
      role: "system",
      text: "[sub-agent child-1 completed] done",
    });
  });

  it("does not recover a turn start already handled by the live stream", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-live-before-recovery"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-live-before-recovery"),
          role: "user",
          text: "live stream handled before recovery",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(await harness.listPendingDispatches()).toHaveLength(0);

    await harness.runStartupRecoveryAgain();
    await harness.drain();

    expect(harness.sendTurn).toHaveBeenCalledTimes(1);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
  });

  it("queues accepted mid-turn user messages until the active turn is idle", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const idleAt = "2026-01-01T00:00:05.000Z";

    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running" },
      activeTurnId: asTurnId("turn-running"),
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-running"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-midturn-accepted"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-midturn-accepted"),
          role: "user",
          text: "accepted while the model is still working",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.drain();

    expect(harness.sendTurn).not.toHaveBeenCalled();
    let readModel = await harness.readModel();
    let thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.messages.at(-1)).toMatchObject({
      role: "user",
      text: "accepted while the model is still working",
      turnId: null,
    });
    expect(thread?.session?.status).toBe("running");
    expect(thread?.session?.lastError).toBeNull();
    expect(
      thread?.activities.some((activity) => activity.kind === "provider.turn.start.failed") ??
        false,
    ).toBe(false);
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    let releaseQueuedSend = () => {};
    const queuedSendFinished = new Promise<void>((resolve) => {
      releaseQueuedSend = resolve;
    });
    harness.sendTurn.mockImplementationOnce(
      () =>
        Effect.promise(() => queuedSendFinished).pipe(
          Effect.as({
            threadId: ThreadId.make("thread-1"),
            turnId: asTurnId("turn-midturn-queued"),
          }),
        ) as never,
    );

    harness.runtimeSessions.splice(0, 1, {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running" },
      createdAt: now,
      updatedAt: idleAt,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-idle"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: idleAt,
        },
        createdAt: idleAt,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      input: "accepted while the model is still working",
    });
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-idle-duplicate"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: idleAt,
        },
        createdAt: idleAt,
      }),
    );
    await harness.drain();
    expect(harness.sendTurn).toHaveBeenCalledTimes(1);

    releaseQueuedSend();
    await waitFor(async () => (await harness.listPendingDispatches()).length === 0);
    readModel = await harness.readModel();
    thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.lastError).toBeNull();
    expect(
      thread?.activities.some((activity) => activity.kind === "provider.turn.start.failed") ??
        false,
    ).toBe(false);
  });

  it("recovers queued turn starts committed before the reactor could persist a dispatch row", async () => {
    const harness = await createHarness({ startReactor: false });
    const now = "2026-01-01T00:00:00.000Z";
    const idleAt = "2026-01-01T00:00:05.000Z";
    harness.generateThreadTitle.mockReturnValue(
      Effect.succeed({ title: "Recovered queued title" }),
    );

    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-before-recovery" },
      activeTurnId: asTurnId("turn-running-before-recovery"),
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-before-recovery"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-running-before-recovery"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-recovered-queued"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-recovered-queued"),
          role: "user",
          text: "queued before reactor startup",
          attachments: [],
        },
        titleSeed: "Thread",
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    expect(await harness.listPendingDispatches()).toHaveLength(0);

    await harness.startReactor();
    await harness.drain();

    expect(harness.sendTurn).not.toHaveBeenCalled();
    const recoveredRows = await harness.listPendingDispatches();
    expect(recoveredRows).toHaveLength(1);
    expect(recoveredRows[0]?.id).toBe("thread-turn:user-message-recovered-queued");
    await waitFor(() => harness.generateThreadTitle.mock.calls.length === 1);
    expect(harness.generateThreadTitle.mock.calls[0]?.[0]).toMatchObject({
      message: "queued before reactor startup",
    });
    await waitFor(async () => {
      const readModel = await harness.readModel();
      return (
        readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"))?.title ===
        "Recovered queued title"
      );
    });

    harness.runtimeSessions.splice(0, 1, {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-before-recovery" },
      createdAt: now,
      updatedAt: idleAt,
    });
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-ready-after-recovery"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: idleAt,
        },
        createdAt: idleAt,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      input: "queued before reactor startup",
    });
    await waitFor(async () => (await harness.listPendingDispatches()).length === 0);
  });

  it("does not recover queued turn starts superseded by a persisted stop request", async () => {
    const harness = await createHarness({ startReactor: false });
    const now = "2026-01-01T00:00:00.000Z";
    const stoppedAt = "2026-01-01T00:00:05.000Z";
    const messageId = asMessageId("user-message-before-recovery-stop");

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-before-recovery-stop"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId,
          role: "user",
          text: "queued before persisted stop",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-session-stop-before-recovery"),
        threadId: ThreadId.make("thread-1"),
        createdAt: stoppedAt,
      }),
    );

    await harness.startReactor();
    await harness.drain();

    expect(harness.sendTurn).not.toHaveBeenCalled();
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.messages.find((message) => message.id === messageId)?.turnId).toBeNull();
    expect(
      thread?.activities.some((activity) => {
        if (activity.kind !== "provider.turn.start.failed") return false;
        const payload =
          typeof activity.payload === "object" && activity.payload !== null
            ? (activity.payload as Record<string, unknown>)
            : {};
        return (
          activity.summary === "Queued turn canceled" &&
          payload.messageId === messageId &&
          payload.canceled === true
        );
      }) ?? false,
    ).toBe(true);
  });

  it("does not mark already-run turn starts canceled during persisted stop recovery", async () => {
    const harness = await createHarness({ startReactor: false });
    const now = "2026-01-01T00:00:00.000Z";
    const completedAt = "2026-01-01T00:00:02.000Z";
    const stoppedAt = "2026-01-01T00:00:05.000Z";
    const messageId = asMessageId("user-message-completed-before-recovery-stop");
    const assistantMessageId = asMessageId("assistant-message-completed-before-recovery-stop");
    const turnId = asTurnId("turn-completed-before-recovery-stop");

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-completed-before-recovery-stop"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId,
          role: "user",
          text: "completed before persisted stop",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-completed-before-recovery-stop"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: turnId,
          lastError: null,
          updatedAt: "2026-01-01T00:00:00.500Z",
        },
        createdAt: "2026-01-01T00:00:00.500Z",
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.message.assistant.complete",
        commandId: CommandId.make("cmd-assistant-complete-before-recovery-stop"),
        threadId: ThreadId.make("thread-1"),
        messageId: assistantMessageId,
        turnId,
        createdAt: "2026-01-01T00:00:01.000Z",
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-turn-diff-complete-before-recovery-stop"),
        threadId: ThreadId.make("thread-1"),
        turnId,
        completedAt,
        checkpointRef: CheckpointRef.make("refs/t3/checkpoints/thread-1/turn/1"),
        status: "ready",
        files: [],
        assistantMessageId,
        checkpointTurnCount: 1,
        createdAt: completedAt,
      }),
    );
    await runtime!.runPromise(
      Effect.flatMap(Effect.service(PendingDispatchRepository), (repo) =>
        repo.insert({
          id: asPendingDispatchId(`thread-turn:${messageId}`),
          kind: "thread_turn",
          targetThreadId: ThreadId.make("thread-1"),
          sourceChildId: null,
          text: JSON.stringify({
            messageId,
            runtimeMode: "approval-required",
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            createdAt: now,
          }),
          error: null,
          status: null,
          commandId: "server:queued-turn-send:stale-after-success",
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now,
        }),
      ),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-session-stop-after-completed-turn"),
        threadId: ThreadId.make("thread-1"),
        createdAt: stoppedAt,
      }),
    );

    await harness.startReactor();
    await harness.drain();

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.messages.find((message) => message.id === messageId)?.turnId).toBe(turnId);
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    expect(
      thread?.activities.some((activity) => {
        if (activity.kind !== "provider.turn.start.failed") return false;
        const payload =
          typeof activity.payload === "object" && activity.payload !== null
            ? (activity.payload as Record<string, unknown>)
            : {};
        return payload.messageId === messageId && payload.canceled === true;
      }) ?? false,
    ).toBe(false);
  });

  it("cancels durable queued turn rows superseded by a persisted stop request on startup", async () => {
    const harness = await createHarness({ startReactor: false });
    const now = "2026-01-01T00:00:00.000Z";
    const stoppedAt = "2026-01-01T00:00:05.000Z";
    const messageId = asMessageId("user-message-durable-before-recovery-stop");

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-durable-before-recovery-stop"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId,
          role: "user",
          text: "durable queued row before persisted stop",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await runtime!.runPromise(
      Effect.flatMap(Effect.service(PendingDispatchRepository), (repo) =>
        repo.insert({
          id: asPendingDispatchId(`thread-turn:${messageId}`),
          kind: "thread_turn",
          targetThreadId: ThreadId.make("thread-1"),
          sourceChildId: null,
          text: JSON.stringify({
            messageId,
            runtimeMode: "approval-required",
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            createdAt: now,
          }),
          error: null,
          status: null,
          commandId: "server:queued-turn-send:durable-before-stop",
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: now,
        }),
      ),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-session-stop-after-durable-row"),
        threadId: ThreadId.make("thread-1"),
        createdAt: stoppedAt,
      }),
    );
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    await harness.startReactor();
    await harness.drain();

    expect(harness.sendTurn).not.toHaveBeenCalled();
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.messages.find((message) => message.id === messageId)?.turnId).toBeNull();
    expect(
      thread?.activities.some((activity) => {
        if (activity.kind !== "provider.turn.start.failed") return false;
        const payload =
          typeof activity.payload === "object" && activity.payload !== null
            ? (activity.payload as Record<string, unknown>)
            : {};
        return (
          activity.summary === "Queued turn canceled" &&
          payload.messageId === messageId &&
          payload.canceled === true
        );
      }) ?? false,
    ).toBe(true);
  });

  it("drains durable queued turn rows created after a persisted stop on startup", async () => {
    const harness = await createHarness({ startReactor: false });
    const stoppedAt = "2026-01-01T00:00:01.000Z";
    const promptAt = "2026-01-01T00:00:02.000Z";
    const messageId = asMessageId("user-message-durable-after-recovery-stop");

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-session-stop-before-durable-post-stop-row"),
        threadId: ThreadId.make("thread-1"),
        createdAt: stoppedAt,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-stale-ready-after-stop"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-01-01T00:00:00.500Z",
        },
        createdAt: "2026-01-01T00:00:01.500Z",
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-stopped-before-durable-post-stop-row"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "stopped",
          providerName: null,
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: stoppedAt,
        },
        createdAt: stoppedAt,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-durable-after-recovery-stop"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId,
          role: "user",
          text: "durable queued row after persisted stop",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: promptAt,
      }),
    );
    await runtime!.runPromise(
      Effect.flatMap(Effect.service(PendingDispatchRepository), (repo) =>
        repo.insert({
          id: asPendingDispatchId(`thread-turn:${messageId}`),
          kind: "thread_turn",
          targetThreadId: ThreadId.make("thread-1"),
          sourceChildId: null,
          text: JSON.stringify({
            messageId,
            runtimeMode: "approval-required",
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            createdAt: promptAt,
          }),
          error: null,
          status: null,
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: promptAt,
        }),
      ),
    );

    await harness.startReactor();
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    await harness.drain();

    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      input: "durable queued row after persisted stop",
    });
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.some((activity) => {
        if (activity.kind !== "provider.turn.start.failed") return false;
        const payload =
          typeof activity.payload === "object" && activity.payload !== null
            ? (activity.payload as Record<string, unknown>)
            : {};
        return payload.messageId === messageId && payload.canceled === true;
      }) ?? false,
    ).toBe(false);
  });

  it("does not treat queued rows after a resumed session as post-stop recovery rows", async () => {
    const harness = await createHarness({ startReactor: false });
    const stoppedAt = "2026-01-01T00:00:01.000Z";
    const resumedAt = "2026-01-01T00:00:02.000Z";
    const closedAt = "2026-01-01T00:00:03.000Z";
    const promptAt = "2026-01-01T00:00:04.000Z";
    const messageId = asMessageId("user-message-after-resumed-session-close");

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-session-stop-before-resume"),
        threadId: ThreadId.make("thread-1"),
        createdAt: stoppedAt,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-ready-after-old-stop"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: resumedAt,
        },
        createdAt: resumedAt,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-stopped-after-resume"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "stopped",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: closedAt,
        },
        createdAt: closedAt,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-after-resumed-close"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId,
          role: "user",
          text: "legacy queued row after resumed session close",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: promptAt,
      }),
    );
    await runtime!.runPromise(
      Effect.flatMap(Effect.service(PendingDispatchRepository), (repo) =>
        repo.insert({
          id: asPendingDispatchId(`thread-turn:${messageId}`),
          kind: "thread_turn",
          targetThreadId: ThreadId.make("thread-1"),
          sourceChildId: null,
          text: JSON.stringify({
            messageId,
            runtimeMode: "approval-required",
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            createdAt: promptAt,
          }),
          error: null,
          status: null,
          commandId: null,
          deliveredByWait: false,
          waitCancellable: false,
          createdAt: promptAt,
        }),
      ),
    );

    await harness.startReactor();
    await harness.drain();

    expect(harness.sendTurn).not.toHaveBeenCalled();
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.some((activity) => {
        if (activity.kind !== "provider.turn.start.failed") return false;
        const payload =
          typeof activity.payload === "object" && activity.payload !== null
            ? (activity.payload as Record<string, unknown>)
            : {};
        return (
          activity.summary === "Queued turn canceled" &&
          payload.messageId === messageId &&
          payload.canceled === true
        );
      }) ?? false,
    ).toBe(true);
  });

  it("does not immediately send a live turn start after a later stop request is observed", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const queuedAt = "2026-01-01T00:00:01.000Z";
    const stoppedAt = "2026-01-01T00:00:02.000Z";
    const defaultStartSession = harness.startSession.getMockImplementation();
    expect(defaultStartSession).toBeDefined();
    const firstStartEntered = Effect.runSync(Deferred.make<void>());
    const releaseFirstStart = Effect.runSync(Deferred.make<void>());

    harness.startSession.mockImplementationOnce((provider, input) =>
      Effect.gen(function* () {
        yield* Deferred.succeed(firstStartEntered, undefined);
        yield* Deferred.await(releaseFirstStart);
        return yield* defaultStartSession!(provider, input);
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-blocking-before-live-stop"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-blocking-before-live-stop"),
          role: "user",
          text: "block worker before stop",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await Effect.runPromise(Deferred.await(firstStartEntered));

    const stoppedMessageId = asMessageId("user-message-live-stop-superseded");
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-live-stop-superseded"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: stoppedMessageId,
          role: "user",
          text: "must not send after live stop",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: queuedAt,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-session-stop-before-live-turn-processed"),
        threadId: ThreadId.make("thread-1"),
        createdAt: stoppedAt,
      }),
    );
    await Effect.runPromise(Effect.yieldNow);
    await Effect.runPromise(Deferred.succeed(releaseFirstStart, undefined));

    await waitFor(async () => {
      await harness.drain();
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.session?.status === "stopped";
    });

    expect(harness.sendTurn).toHaveBeenCalledTimes(1);
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      input: "block worker before stop",
    });
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.messages.find((message) => message.id === stoppedMessageId)?.turnId).toBeNull();
    expect(
      thread?.activities.some((activity) => {
        if (activity.kind !== "provider.turn.start.failed") return false;
        const payload =
          typeof activity.payload === "object" && activity.payload !== null
            ? (activity.payload as Record<string, unknown>)
            : {};
        return (
          activity.summary === "Queued turn canceled" &&
          payload.messageId === stoppedMessageId &&
          payload.canceled === true
        );
      }) ?? false,
    ).toBe(true);
  });

  it("does not cancel a live turn start created after the stop request", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const stoppedAt = "2026-01-01T00:00:01.000Z";
    const nextPromptAt = "2026-01-01T00:00:02.000Z";
    const defaultStartSession = harness.startSession.getMockImplementation();
    expect(defaultStartSession).toBeDefined();
    const firstStartEntered = Effect.runSync(Deferred.make<void>());
    const releaseFirstStart = Effect.runSync(Deferred.make<void>());

    harness.startSession.mockImplementationOnce((provider, input) =>
      Effect.gen(function* () {
        yield* Deferred.succeed(firstStartEntered, undefined);
        yield* Deferred.await(releaseFirstStart);
        return yield* defaultStartSession!(provider, input);
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-blocking-before-post-stop"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-blocking-before-post-stop"),
          role: "user",
          text: "block worker before post-stop prompt",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await Effect.runPromise(Deferred.await(firstStartEntered));
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-session-stop-before-new-live-turn"),
        threadId: ThreadId.make("thread-1"),
        createdAt: stoppedAt,
      }),
    );

    const postStopMessageId = asMessageId("user-message-post-stop-live-turn");
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-after-live-stop"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: postStopMessageId,
          role: "user",
          text: "send after stop request",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: nextPromptAt,
      }),
    );
    await Effect.runPromise(Effect.yieldNow);
    await Effect.runPromise(Deferred.succeed(releaseFirstStart, undefined));

    await waitFor(() => harness.sendTurn.mock.calls.length === 2);
    await harness.drain();

    expect(harness.sendTurn.mock.calls[1]?.[0]).toMatchObject({
      input: "send after stop request",
    });
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.some((activity) => {
        if (activity.kind !== "provider.turn.start.failed") return false;
        const payload =
          typeof activity.payload === "object" && activity.payload !== null
            ? (activity.payload as Record<string, unknown>)
            : {};
        return payload.messageId === postStopMessageId && payload.canceled === true;
      }) ?? false,
    ).toBe(false);
  });

  it("drains queued turn rows created after a stop once the immediate send settles", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const stoppedAt = "2026-01-01T00:00:01.000Z";
    const nextPromptAt = "2026-01-01T00:00:02.000Z";
    const secondPromptAt = "2026-01-01T00:00:03.000Z";
    const defaultSendTurn = harness.sendTurn.getMockImplementation();
    expect(defaultSendTurn).toBeDefined();
    const firstSendEntered = Effect.runSync(Deferred.make<void>());
    const releaseFirstSend = Effect.runSync(Deferred.make<void>());

    harness.sendTurn.mockImplementationOnce((input) =>
      Effect.gen(function* () {
        yield* Deferred.succeed(firstSendEntered, undefined);
        yield* Deferred.await(releaseFirstSend);
        return yield* defaultSendTurn!(input);
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-inflight-before-post-stop-queue"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-inflight-before-post-stop-queue"),
          role: "user",
          text: "hold immediate send before post-stop queue",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await Effect.runPromise(Deferred.await(firstSendEntered));
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-session-stop-before-post-stop-queue"),
        threadId: ThreadId.make("thread-1"),
        createdAt: stoppedAt,
      }),
    );

    const postStopMessageId = asMessageId("user-message-post-stop-queued-turn");
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-post-stop-queued"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: postStopMessageId,
          role: "user",
          text: "queued after stop while send is in flight",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: nextPromptAt,
      }),
    );

    await waitFor(async () => (await harness.listPendingDispatches()).length === 1);
    const secondPostStopMessageId = asMessageId("user-message-second-post-stop-queued-turn");
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-second-post-stop-queued"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: secondPostStopMessageId,
          role: "user",
          text: "second queued after stop while send is in flight",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: secondPromptAt,
      }),
    );
    await waitFor(async () => {
      const rows = await harness.listPendingDispatches();
      const ids = new Set(rows.map((row) => row.id));
      return (
        rows.length === 2 &&
        ids.has(asPendingDispatchId(`thread-turn:${postStopMessageId}`)) &&
        ids.has(asPendingDispatchId(`thread-turn:${secondPostStopMessageId}`))
      );
    });
    await Effect.runPromise(Deferred.succeed(releaseFirstSend, undefined));
    await waitFor(() => harness.sendTurn.mock.calls.length === 2);
    harness.runtimeSessions.splice(0, 1, {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-post-stop-queued-first" },
      createdAt: nextPromptAt,
      updatedAt: secondPromptAt,
    });
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-ready-between-post-stop-queued"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: secondPromptAt,
        },
        createdAt: secondPromptAt,
      }),
    );
    await waitFor(() => harness.sendTurn.mock.calls.length === 3);
    await harness.drain();

    expect(harness.sendTurn.mock.calls[1]?.[0]).toMatchObject({
      input: "queued after stop while send is in flight",
    });
    expect(harness.sendTurn.mock.calls[2]?.[0]).toMatchObject({
      input: "second queued after stop while send is in flight",
    });
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.some((activity) => {
        if (activity.kind !== "provider.turn.start.failed") return false;
        const payload =
          typeof activity.payload === "object" && activity.payload !== null
            ? (activity.payload as Record<string, unknown>)
            : {};
        return (
          (payload.messageId === postStopMessageId ||
            payload.messageId === secondPostStopMessageId) &&
          payload.canceled === true
        );
      }) ?? false,
    ).toBe(false);
  });

  it("does not drain queued mid-turn messages after an explicit stop", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const stoppedAt = "2026-01-01T00:00:05.000Z";

    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-before-stop" },
      activeTurnId: asTurnId("turn-running-before-stop"),
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-before-stop"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-running-before-stop"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-queued-before-stop"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-queued-before-stop"),
          role: "user",
          text: "queued before stop",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.drain();
    expect(harness.sendTurn).not.toHaveBeenCalled();
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    harness.runtimeSessions.splice(0);
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-stopped-with-queued-message"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "stopped",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: stoppedAt,
        },
        createdAt: stoppedAt,
      }),
    );
    await harness.drain();

    expect(harness.sendTurn).not.toHaveBeenCalled();
    expect(await harness.listPendingDispatches()).toHaveLength(0);
    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return (
        thread?.activities.some((activity) => {
          if (activity.kind !== "provider.turn.start.failed") return false;
          const payload =
            typeof activity.payload === "object" && activity.payload !== null
              ? (activity.payload as Record<string, unknown>)
              : null;
          return (
            payload?.messageId === "user-message-queued-before-stop" && payload.canceled === true
          );
        }) ?? false
      );
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-after-stop"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-after-stop"),
          role: "user",
          text: "fresh prompt after stop",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: stoppedAt,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      input: "fresh prompt after stop",
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-after-stop"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-after-stop"),
          lastError: null,
          updatedAt: stoppedAt,
        },
        createdAt: stoppedAt,
      }),
    );
    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return (
        thread?.messages.find((message) => message.id === "user-message-after-stop")?.turnId ===
        "turn-after-stop"
      );
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.messages.find((message) => message.id === "user-message-queued-before-stop")?.turnId,
    ).toBeNull();
    expect(
      thread?.messages.find((message) => message.id === "user-message-after-stop")?.turnId,
    ).toBe("turn-after-stop");
  });

  it("cancels queued mid-turn messages when a stopped session preserves an error", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const stoppedAt = "2026-01-01T00:00:05.000Z";

    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-before-error-stop" },
      activeTurnId: asTurnId("turn-running-before-error-stop"),
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-before-error-stop"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-running-before-error-stop"),
          lastError: "provider failed before stop",
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-queued-before-error-stop"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-queued-before-error-stop"),
          role: "user",
          text: "queued before stop with existing error",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.drain();
    expect(harness.sendTurn).not.toHaveBeenCalled();
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    harness.runtimeSessions.splice(0);
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-stopped-with-queued-message-and-error"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "stopped",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: "provider failed before stop",
          updatedAt: stoppedAt,
        },
        createdAt: stoppedAt,
      }),
    );
    await waitFor(async () => (await harness.listPendingDispatches()).length === 0);

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.some((activity) => {
        if (activity.kind !== "provider.turn.start.failed") return false;
        const payload =
          typeof activity.payload === "object" && activity.payload !== null
            ? (activity.payload as Record<string, unknown>)
            : null;
        return (
          payload?.messageId === "user-message-queued-before-error-stop" &&
          payload.canceled === true
        );
      }) ?? false,
    ).toBe(true);
  });

  it("interrupts an in-flight queued send when the session stops", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const readyAt = "2026-01-01T00:00:05.000Z";
    const stoppedAt = "2026-01-01T00:00:06.000Z";
    const afterStopTurnAt = "2026-01-01T00:00:07.000Z";

    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-before-inflight-stop" },
      activeTurnId: asTurnId("turn-running-before-inflight-stop"),
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-before-inflight-stop"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-running-before-inflight-stop"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-queued-before-inflight-stop"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-queued-before-inflight-stop"),
          role: "user",
          text: "queued before in-flight stop",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.drain();
    expect(harness.sendTurn).not.toHaveBeenCalled();
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    let releaseQueuedSend = () => {};
    const queuedSendFinished = new Promise<void>((resolve) => {
      releaseQueuedSend = resolve;
    });
    harness.sendTurn.mockImplementationOnce(
      () =>
        Effect.promise(() => queuedSendFinished).pipe(
          Effect.andThen(
            harness.engine.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make("cmd-session-set-running-from-interrupted-queued-send"),
              threadId: ThreadId.make("thread-1"),
              session: {
                threadId: ThreadId.make("thread-1"),
                status: "running",
                providerName: "codex",
                providerInstanceId: ProviderInstanceId.make("codex"),
                runtimeMode: "approval-required",
                activeTurnId: asTurnId("turn-queued-after-stop"),
                lastError: null,
                updatedAt: afterStopTurnAt,
              },
              createdAt: afterStopTurnAt,
            }),
          ),
          Effect.as({
            threadId: ThreadId.make("thread-1"),
            turnId: asTurnId("turn-queued-after-stop"),
          }),
        ) as never,
    );

    harness.runtimeSessions.splice(0, 1, {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-before-inflight-stop" },
      createdAt: now,
      updatedAt: readyAt,
    });
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-ready-before-inflight-stop"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: readyAt,
        },
        createdAt: readyAt,
      }),
    );
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    harness.runtimeSessions.splice(0);
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-stopped-during-queued-send"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "stopped",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: stoppedAt,
        },
        createdAt: stoppedAt,
      }),
    );
    await waitFor(async () => (await harness.listPendingDispatches()).length === 0);

    releaseQueuedSend();
    await Effect.runPromise(Effect.sleep("10 millis"));
    await harness.drain();

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.messages.find((message) => message.id === "user-message-queued-before-inflight-stop")
        ?.turnId,
    ).toBeNull();
    expect(thread?.session?.status).toBe("stopped");
    expect(thread?.session?.activeTurnId).toBeNull();
    expect(thread?.latestTurn?.turnId).not.toBe("turn-queued-after-stop");
  });

  it("keeps stale durable queued-send claims retryable while another turn is active", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-stale-claim-active-turn" },
      activeTurnId: asTurnId("turn-stale-claim-active"),
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-stale-claim"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-stale-claim-active"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-stale-claim"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-stale-claim"),
          role: "user",
          text: "queued before stale durable claim",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await waitFor(async () => (await harness.listPendingDispatches()).length === 1);
    const queuedRow = (await harness.listPendingDispatches())[0];
    expect(queuedRow?.commandId).toBeNull();

    await runtime!.runPromise(
      Effect.flatMap(Effect.service(PendingDispatchRepository), (repo) =>
        repo.claim({
          ids: [queuedRow!.id],
          commandId: "stale-claim-before-provider-send",
        }),
      ),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-ready-with-stale-claim"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );
    await harness.drain();

    const rowsAfterDrain = await harness.listPendingDispatches();
    expect(rowsAfterDrain).toHaveLength(1);
    expect(rowsAfterDrain[0]?.id).toBe(queuedRow?.id);
    expect(rowsAfterDrain[0]?.commandId).toBeNull();
    expect(harness.sendTurn).not.toHaveBeenCalled();
  });

  it("does not release a queued send when a stop lands after claim but before release", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const readyAt = "2026-01-01T00:00:05.000Z";
    const stoppedAt = "2026-01-01T00:00:06.000Z";

    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-before-release-race" },
      activeTurnId: asTurnId("turn-running-before-release-race"),
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-before-release-race"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-running-before-release-race"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-before-release-race"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-before-release-race"),
          role: "user",
          text: "queued before release race",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.drain();
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    harness.runtimeSessions.splice(0, 1, {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-before-release-race" },
      createdAt: now,
      updatedAt: readyAt,
    });

    let listSessionsAfterReady = 0;
    harness.listSessions.mockImplementation(() =>
      Effect.gen(function* () {
        listSessionsAfterReady += 1;
        const sessions = [...harness.runtimeSessions];
        if (listSessionsAfterReady === 5) {
          harness.runtimeSessions.splice(0);
          yield* harness.engine
            .dispatch({
              type: "thread.session.set",
              commandId: CommandId.make("cmd-session-set-stopped-before-release"),
              threadId: ThreadId.make("thread-1"),
              session: {
                threadId: ThreadId.make("thread-1"),
                status: "stopped",
                providerName: "codex",
                providerInstanceId: ProviderInstanceId.make("codex"),
                runtimeMode: "approval-required",
                activeTurnId: null,
                lastError: null,
                updatedAt: stoppedAt,
              },
              createdAt: stoppedAt,
            })
            .pipe(Effect.orDie);
        }
        return sessions;
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-ready-before-release-race"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: readyAt,
        },
        createdAt: readyAt,
      }),
    );

    await waitFor(async () => (await harness.listPendingDispatches()).length === 0);
    await Effect.runPromise(Effect.sleep("10 millis"));
    await harness.drain();

    expect(harness.sendTurn).not.toHaveBeenCalled();
    expect(listSessionsAfterReady).toBe(5);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.messages.find((message) => message.id === "user-message-before-release-race")?.turnId,
    ).toBeNull();
    expect(thread?.session?.status).toBe("stopped");
  });

  it("resets a durable queued-send claim when the provider becomes busy before release", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const readyAt = "2026-01-01T00:00:05.000Z";
    const retryAt = "2026-01-01T00:00:06.000Z";

    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-busy-before-release" },
      activeTurnId: asTurnId("turn-busy-before-release"),
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-before-claim-reset"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-busy-before-release"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-before-claim-reset"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-before-claim-reset"),
          role: "user",
          text: "queued before claim reset",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.drain();
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    harness.runtimeSessions.splice(0, 1, {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-busy-before-release" },
      createdAt: now,
      updatedAt: readyAt,
    });

    let busyInjected = false;
    let listSessionsAfterReady = 0;
    harness.listSessions.mockImplementation(() =>
      Effect.sync(() => {
        listSessionsAfterReady += 1;
        if (!busyInjected && listSessionsAfterReady === 5) {
          busyInjected = true;
          harness.runtimeSessions.splice(0, 1, {
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: ProviderInstanceId.make("codex"),
            status: "running",
            runtimeMode: "approval-required",
            model: "gpt-5-codex",
            threadId: ThreadId.make("thread-1"),
            resumeCursor: { opaque: "resume-busy-before-release" },
            activeTurnId: asTurnId("turn-runtime-busy-before-release"),
            createdAt: now,
            updatedAt: readyAt,
          });
        }
        return [...harness.runtimeSessions];
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-ready-before-claim-reset"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: readyAt,
        },
        createdAt: readyAt,
      }),
    );

    await waitFor(() => listSessionsAfterReady >= 5);
    await harness.drain();
    const rowsAfterAbort = await harness.listPendingDispatches();
    expect(rowsAfterAbort).toHaveLength(1);
    expect(rowsAfterAbort[0]?.commandId).toBeNull();
    expect(harness.sendTurn).not.toHaveBeenCalled();

    harness.runtimeSessions.splice(0, 1, {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-busy-before-release" },
      createdAt: now,
      updatedAt: retryAt,
    });
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-ready-after-claim-reset"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: retryAt,
        },
        createdAt: retryAt,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      input: "queued before claim reset",
    });
    await waitFor(async () => (await harness.listPendingDispatches()).length === 0);
  });

  it("queues rapid follow-up messages while an immediate provider send is in flight", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const idleAt = "2026-01-01T00:00:05.000Z";
    let releaseFirstSend = () => {};
    const firstSendFinished = new Promise<void>((resolve) => {
      releaseFirstSend = resolve;
    });
    harness.sendTurn.mockImplementationOnce(
      () =>
        Effect.promise(() => firstSendFinished).pipe(
          Effect.as({
            threadId: ThreadId.make("thread-1"),
            turnId: asTurnId("turn-first-immediate"),
          }),
        ) as never,
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-immediate-first"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-immediate-first"),
          role: "user",
          text: "first prompt",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-immediate-second"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-immediate-second"),
          role: "user",
          text: "second prompt",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.drain();

    expect(harness.sendTurn).toHaveBeenCalledTimes(1);
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-idle-while-immediate-in-flight"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: idleAt,
        },
        createdAt: idleAt,
      }),
    );
    await harness.drain();
    expect(harness.sendTurn).toHaveBeenCalledTimes(1);

    releaseFirstSend();
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-idle-after-immediate-send"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: idleAt,
        },
        createdAt: idleAt,
      }),
    );
    await waitFor(() => harness.sendTurn.mock.calls.length === 2);
    expect(harness.sendTurn.mock.calls[1]?.[0]).toMatchObject({ input: "second prompt" });
    await waitFor(async () => (await harness.listPendingDispatches()).length === 0);
  });

  it("claims a queued turn before building the provider request", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const idleAt = "2026-01-01T00:00:05.000Z";
    let releaseFirstSend = () => {};
    const firstSendFinished = new Promise<void>((resolve) => {
      releaseFirstSend = resolve;
    });
    harness.sendTurn.mockImplementationOnce(
      () =>
        Effect.promise(() => firstSendFinished).pipe(
          Effect.as({
            threadId: ThreadId.make("thread-1"),
            turnId: asTurnId("turn-first-before-claim-race"),
          }),
        ) as never,
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-claim-race-first"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-claim-race-first"),
          role: "user",
          text: "first prompt",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-claim-race-second"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-claim-race-second"),
          role: "user",
          text: "queued prompt",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.drain();
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    let releaseQueuedStartSession = () => {};
    const queuedStartSessionReleased = new Promise<void>((resolve) => {
      releaseQueuedStartSession = resolve;
    });
    harness.startSession.mockImplementationOnce(
      () =>
        Effect.promise(() => queuedStartSessionReleased).pipe(
          Effect.map(() => {
            const session: ProviderSession = {
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              status: "ready",
              runtimeMode: "approval-required",
              model: "gpt-5-codex",
              threadId: ThreadId.make("thread-1"),
              resumeCursor: { opaque: "resume-claim-race" },
              createdAt: now,
              updatedAt: idleAt,
            };
            harness.runtimeSessions.push(session);
            return session;
          }),
        ) as never,
    );
    harness.runtimeSessions.splice(0);

    releaseFirstSend();
    await waitFor(() => harness.startSession.mock.calls.length === 2);
    expect(harness.sendTurn).toHaveBeenCalledTimes(1);

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-ready-claim-race-duplicate"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: idleAt,
        },
        createdAt: idleAt,
      }),
    );
    await harness.drain();

    expect(harness.startSession).toHaveBeenCalledTimes(2);
    expect(harness.sendTurn).toHaveBeenCalledTimes(1);
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    releaseQueuedStartSession();
    await waitFor(() => harness.sendTurn.mock.calls.length === 2);
    expect(harness.sendTurn.mock.calls[1]?.[0]).toMatchObject({
      input: "queued prompt",
    });
    await waitFor(async () => (await harness.listPendingDispatches()).length === 0);
  });

  it("retries crash-recovered claimed queued turns when delivery is not visible", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const readyAt = "2026-01-01T00:00:05.000Z";

    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-claimed-before-crash" },
      activeTurnId: asTurnId("turn-running-before-claimed-crash"),
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-before-claimed-crash"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-running-before-claimed-crash"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-claimed-before-crash"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-claimed-before-crash"),
          role: "user",
          text: "queued before claimed crash",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.drain();
    const [queuedRow] = await harness.listPendingDispatches();
    expect(queuedRow).toBeDefined();
    await runtime!.runPromise(
      Effect.flatMap(Effect.service(PendingDispatchRepository), (repository) =>
        repository.claim({
          ids: [queuedRow!.id],
          commandId: "server:queued-turn-send:claimed-before-crash",
        }),
      ),
    );

    harness.runtimeSessions.splice(0, 1, {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-claimed-before-crash" },
      createdAt: now,
      updatedAt: readyAt,
    });
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-ready-after-claimed-crash"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: readyAt,
        },
        createdAt: readyAt,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      input: "queued before claimed crash",
    });
    await waitFor(async () => (await harness.listPendingDispatches()).length === 0);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.some((activity) => activity.kind === "provider.turn.start.failed") ??
        false,
    ).toBe(false);
  });

  it("drains queued turns when a failed active turn leaves the session idle", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const failedAt = "2026-01-01T00:00:05.000Z";

    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-before-error-idle" },
      activeTurnId: asTurnId("turn-running-before-error-idle"),
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-before-error-idle"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-running-before-error-idle"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-queued-before-error-idle"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-queued-before-error-idle"),
          role: "user",
          text: "queued before idle error",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.drain();
    expect(harness.sendTurn).not.toHaveBeenCalled();
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    harness.runtimeSessions.splice(0);
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-error-idle-with-queued-turn"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "error",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: "provider turn failed after accepting queued prompt",
          updatedAt: failedAt,
        },
        createdAt: failedAt,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      input: "queued before idle error",
    });
    await waitFor(async () => (await harness.listPendingDispatches()).length === 0);
  });

  it("drains queued rapid follow-up messages after an immediate provider send fails", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    let releaseFirstSendFailure = () => {};
    const firstSendFailureReleased = new Promise<void>((resolve) => {
      releaseFirstSendFailure = resolve;
    });
    harness.sendTurn.mockImplementationOnce(
      () =>
        Effect.promise(() => firstSendFailureReleased).pipe(
          Effect.andThen(
            Effect.fail(
              new ProviderAdapterRequestError({
                provider: "codex",
                method: "thread.turn.start",
                detail: "simulated immediate send failure",
              }),
            ),
          ),
        ) as never,
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-immediate-failing-first"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-immediate-failing-first"),
          role: "user",
          text: "first prompt fails",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-immediate-failing-second"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-immediate-failing-second"),
          role: "user",
          text: "second prompt after failure",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.drain();
    expect(harness.sendTurn).toHaveBeenCalledTimes(1);
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    releaseFirstSendFailure();
    await waitFor(() => harness.sendTurn.mock.calls.length === 2);
    expect(harness.sendTurn.mock.calls[1]?.[0]).toMatchObject({
      input: "second prompt after failure",
    });
    await waitFor(async () => (await harness.listPendingDispatches()).length === 0);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.some((activity) => activity.kind === "provider.turn.start.failed") ??
        false,
    ).toBe(true);
  });

  it("drains multiple accepted mid-turn user messages in FIFO order", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const readyAt = "2026-01-01T00:00:05.000Z";
    const turnAAt = "2026-01-01T00:00:06.000Z";
    const readyAfterAAt = "2026-01-01T00:00:10.000Z";
    const turnBAt = "2026-01-01T00:00:11.000Z";

    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-fifo" },
      activeTurnId: asTurnId("turn-running-fifo"),
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-fifo"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-running-fifo"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    for (const [messageId, text] of [
      ["user-message-queued-z", "queued A"],
      ["user-message-queued-a", "queued B"],
    ] as const) {
      await runtime!.runPromise(
        harness.engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make(`cmd-turn-start-${messageId}`),
          threadId: ThreadId.make("thread-1"),
          message: {
            messageId: asMessageId(messageId),
            role: "user",
            text,
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt: now,
        }),
      );
    }
    await harness.drain();

    expect(harness.sendTurn).not.toHaveBeenCalled();
    expect(await harness.listPendingDispatches()).toHaveLength(2);

    harness.runtimeSessions.splice(0, 1, {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-fifo" },
      createdAt: now,
      updatedAt: readyAt,
    });
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-ready-fifo"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: readyAt,
        },
        createdAt: readyAt,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({ input: "queued A" });
    await waitFor(async () => (await harness.listPendingDispatches()).length === 1);

    harness.runtimeSessions.splice(0, 1, {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-fifo" },
      activeTurnId: asTurnId("turn-queued-a"),
      createdAt: now,
      updatedAt: turnAAt,
    });
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-a"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-queued-a"),
          lastError: null,
          updatedAt: turnAAt,
        },
        createdAt: turnAAt,
      }),
    );

    let readModel = await harness.readModel();
    let thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.messages.find((message) => message.id === "user-message-queued-z")?.turnId).toBe(
      "turn-queued-a",
    );
    expect(
      thread?.messages.find((message) => message.id === "user-message-queued-a")?.turnId,
    ).toBeNull();

    harness.runtimeSessions.splice(0, 1, {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-fifo" },
      createdAt: now,
      updatedAt: readyAfterAAt,
    });
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-ready-after-a"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: readyAfterAAt,
        },
        createdAt: readyAfterAAt,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 2);
    expect(harness.sendTurn.mock.calls[1]?.[0]).toMatchObject({ input: "queued B" });
    await waitFor(async () => (await harness.listPendingDispatches()).length === 0);

    harness.runtimeSessions.splice(0, 1, {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-fifo" },
      activeTurnId: asTurnId("turn-queued-b"),
      createdAt: now,
      updatedAt: turnBAt,
    });
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-b"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-queued-b"),
          lastError: null,
          updatedAt: turnBAt,
        },
        createdAt: turnBAt,
      }),
    );

    readModel = await harness.readModel();
    thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.messages.find((message) => message.id === "user-message-queued-a")?.turnId).toBe(
      "turn-queued-b",
    );
  });

  it("drains the next queued message only after queued start failure cleanup", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const readyAt = "2026-01-01T00:00:05.000Z";
    const secondRunningAt = "2026-01-01T00:00:06.000Z";

    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-failure" },
      activeTurnId: asTurnId("turn-running-failure"),
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-failure"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-running-failure"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    for (const [messageId, text] of [
      ["user-message-queued-failure-a", "queued failure A"],
      ["user-message-queued-failure-b", "queued failure B"],
    ] as const) {
      await runtime!.runPromise(
        harness.engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make(`cmd-turn-start-${messageId}`),
          threadId: ThreadId.make("thread-1"),
          message: {
            messageId: asMessageId(messageId),
            role: "user",
            text,
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt: now,
        }),
      );
    }
    await harness.drain();

    expect(harness.sendTurn).not.toHaveBeenCalled();
    expect(await harness.listPendingDispatches()).toHaveLength(2);

    harness.sendTurn.mockImplementationOnce(
      () =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: "codex",
            method: "thread.turn.start",
            detail: "simulated queued start failure",
          }),
        ) as never,
    );
    harness.sendTurn.mockImplementationOnce(
      () =>
        harness.engine
          .dispatch({
            type: "thread.session.set",
            commandId: CommandId.make("cmd-session-set-running-after-queued-failure"),
            threadId: ThreadId.make("thread-1"),
            session: {
              threadId: ThreadId.make("thread-1"),
              status: "running",
              providerName: "codex",
              providerInstanceId: ProviderInstanceId.make("codex"),
              runtimeMode: "approval-required",
              activeTurnId: asTurnId("turn-queued-b-after-failure"),
              lastError: null,
              updatedAt: secondRunningAt,
            },
            createdAt: secondRunningAt,
          })
          .pipe(
            Effect.as({
              threadId: ThreadId.make("thread-1"),
              turnId: asTurnId("turn-queued-b-after-failure"),
            }),
          ) as never,
    );

    harness.runtimeSessions.splice(0, 1, {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-failure" },
      createdAt: now,
      updatedAt: readyAt,
    });
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-ready-failure"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: readyAt,
        },
        createdAt: readyAt,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 2);
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({ input: "queued failure A" });
    expect(harness.sendTurn.mock.calls[1]?.[0]).toMatchObject({ input: "queued failure B" });
    await waitFor(async () => (await harness.listPendingDispatches()).length === 0);

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return (
        thread?.messages.find((message) => message.id === "user-message-queued-failure-b")
          ?.turnId === "turn-queued-b-after-failure"
      );
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.messages.find((message) => message.id === "user-message-queued-failure-a")?.turnId,
    ).toBeNull();
    expect(
      thread?.messages.find((message) => message.id === "user-message-queued-failure-b")?.turnId,
    ).toBe("turn-queued-b-after-failure");
    expect(
      thread?.activities.filter((activity) => activity.kind === "provider.turn.start.failed"),
    ).toHaveLength(1);
  });

  it("queues server-originated system wakes until the active turn is idle", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const idleAt = "2026-01-01T00:00:05.000Z";

    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-system" },
      activeTurnId: asTurnId("turn-running-system"),
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-running-system"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-running-system"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-midturn-system-wake"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("system-message-midturn-wake"),
          role: "system",
          text: "[sub-agent child-1 completed] done",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.drain();

    expect(harness.sendTurn).not.toHaveBeenCalled();
    let readModel = await harness.readModel();
    let thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.messages.at(-1)).toMatchObject({
      role: "system",
      text: "[sub-agent child-1 completed] done",
      turnId: null,
    });
    expect(thread?.session?.lastError).toBeNull();
    expect(await harness.listPendingDispatches()).toHaveLength(1);

    harness.runtimeSessions.splice(0, 1, {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      model: "gpt-5-codex",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-running-system" },
      createdAt: now,
      updatedAt: idleAt,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-idle-system"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: idleAt,
        },
        createdAt: idleAt,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      input: "[sub-agent child-1 completed] done",
    });
    await waitFor(async () => (await harness.listPendingDispatches()).length === 0);
    readModel = await harness.readModel();
    thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.lastError).toBeNull();
    expect(
      thread?.activities.some((activity) => activity.kind === "provider.turn.start.failed") ??
        false,
    ).toBe(false);
  });

  it("records a shell-visible error session when the first provider session fails to start", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    harness.startSession.mockImplementationOnce(
      (_: unknown, __: unknown) => Effect.fail("simulated first start failure") as never,
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-first-session-failure"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-first-session-failure"),
          role: "user",
          text: "hello reactor",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.session?.status === "error";
    });

    expect(harness.sendTurn).not.toHaveBeenCalled();
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      status: "error",
      providerName: "codex",
      providerInstanceId: ProviderInstanceId.make("codex"),
      runtimeMode: "approval-required",
      activeTurnId: null,
      lastError: expect.stringContaining("simulated first start failure"),
      updatedAt: now,
    });
    expect(
      thread?.activities.find((activity) => activity.kind === "provider.turn.start.failed"),
    ).toMatchObject({
      payload: {
        detail: expect.stringContaining("simulated first start failure"),
      },
    });
  });

  it("records the driver kind separately from custom provider instance ids on first-start failure", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    harness.startSession.mockImplementationOnce(
      (_: unknown, __: unknown) => Effect.fail("simulated custom instance start failure") as never,
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-custom-instance-failure"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-custom-instance-failure"),
          role: "user",
          text: "hello custom codex",
          attachments: [],
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex_personal"),
          model: "gpt-5.5",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.session?.status === "error";
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.providerName).toBe("codex");
    expect(thread?.session?.providerInstanceId).toBe(ProviderInstanceId.make("codex_personal"));
  });

  it("stops a synthetic first-start error session without provider routing", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    harness.startSession.mockImplementationOnce(
      (_: unknown, __: unknown) => Effect.fail("simulated first start failure") as never,
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-stop-synthetic-error"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-stop-synthetic-error"),
          role: "user",
          text: "hello reactor",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.session?.status === "error";
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-stop-synthetic-start-error"),
        threadId: ThreadId.make("thread-1"),
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.session?.status === "stopped";
    });

    expect(harness.stopSession).not.toHaveBeenCalled();
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session).toMatchObject({
      status: "stopped",
      activeTurnId: null,
      lastError: expect.stringContaining("simulated first start failure"),
    });

    const requestedModelSelection = {
      instanceId: ProviderInstanceId.make("claudeAgent"),
      model: "claude-opus-4-6",
    };
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-retry-stopped-synthetic-error"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-retry-stopped-synthetic-error"),
          role: "user",
          text: "retry with claude",
          attachments: [],
        },
        modelSelection: requestedModelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 2);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.startSession.mock.calls[1]?.[1]).toMatchObject({
      provider: ProviderDriverKind.make("claudeAgent"),
      providerInstanceId: ProviderInstanceId.make("claudeAgent"),
      modelSelection: requestedModelSelection,
    });
  });

  it("records a visible error when retrying a stopped synthetic first-start failure fails again", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    harness.startSession.mockImplementationOnce(
      (_: unknown, __: unknown) => Effect.fail("simulated first start failure") as never,
    );
    harness.startSession.mockImplementationOnce(
      (_: unknown, __: unknown) => Effect.fail("simulated retry start failure") as never,
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-stopped-retry-error-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-stopped-retry-error-1"),
          role: "user",
          text: "first attempt",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.session?.status === "error";
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-stop-stopped-retry-error"),
        threadId: ThreadId.make("thread-1"),
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.session?.status === "stopped";
    });

    const requestedModelSelection = {
      instanceId: ProviderInstanceId.make("claudeAgent"),
      model: "claude-opus-4-6",
    };
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-stopped-retry-error-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-stopped-retry-error-2"),
          role: "user",
          text: "second attempt",
          attachments: [],
        },
        modelSelection: requestedModelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return (
        harness.startSession.mock.calls.length === 2 &&
        thread?.session?.status === "error" &&
        thread.session.providerInstanceId === ProviderInstanceId.make("claudeAgent")
      );
    });
    expect(harness.sendTurn).not.toHaveBeenCalled();
  });

  it("routes stop for a real provider error session before any turn starts", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "error",
      runtimeMode: "approval-required",
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-error" },
      createdAt: now,
      updatedAt: now,
      lastError: "provider failed before turn start",
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-real-error-session-set"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "error",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: "provider failed before turn start",
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-stop-real-error-session"),
        threadId: ThreadId.make("thread-1"),
        createdAt: now,
      }),
    );

    await waitFor(() => harness.stopSession.mock.calls.length === 1);
    expect(harness.runtimeSessions).toHaveLength(0);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.status).toBe("stopped");
  });

  it("allows retrying the first turn with the same requested provider after repeated session start failures", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const requestedModelSelection = {
      instanceId: ProviderInstanceId.make("claudeAgent"),
      model: "claude-opus-4-6",
    };
    harness.startSession.mockImplementationOnce(
      (_: unknown, __: unknown) => Effect.fail("simulated first claude start failure") as never,
    );
    harness.startSession.mockImplementationOnce(
      (_: unknown, __: unknown) => Effect.fail("simulated second claude start failure") as never,
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-first-claude-failure"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-first-claude-failure"),
          role: "user",
          text: "hello claude",
          attachments: [],
        },
        modelSelection: requestedModelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.session?.status === "error";
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-first-claude-retry-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-first-claude-retry-1"),
          role: "user",
          text: "hello claude again",
          attachments: [],
        },
        modelSelection: requestedModelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return (
        harness.startSession.mock.calls.length === 2 &&
        thread?.session?.status === "error" &&
        thread.session.providerInstanceId === ProviderInstanceId.make("claudeAgent")
      );
    });
    expect(harness.sendTurn).not.toHaveBeenCalled();

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-first-claude-retry-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-first-claude-retry-2"),
          role: "user",
          text: "hello claude once more",
          attachments: [],
        },
        modelSelection: requestedModelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 3);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.providerName).toBe("claudeAgent");
    expect(thread?.session?.providerInstanceId).toBe(ProviderInstanceId.make("claudeAgent"));
    expect(harness.startSession.mock.calls[2]?.[1]).toMatchObject({
      provider: ProviderDriverKind.make("claudeAgent"),
      providerInstanceId: ProviderInstanceId.make("claudeAgent"),
      modelSelection: requestedModelSelection,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-successful-retry"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "claudeAgent",
          providerInstanceId: ProviderInstanceId.make("claudeAgent"),
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-successful-retry"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    const afterRetryStart = await harness.readModel();
    const retryThread = afterRetryStart.threads.find(
      (entry) => entry.id === ThreadId.make("thread-1"),
    );
    expect(
      retryThread?.messages.find((message) => message.id === "user-message-first-claude-failure")
        ?.turnId,
    ).toBeNull();
    expect(
      retryThread?.messages.find((message) => message.id === "user-message-first-claude-retry-1")
        ?.turnId,
    ).toBeNull();
    expect(
      retryThread?.messages.find((message) => message.id === "user-message-first-claude-retry-2")
        ?.turnId,
    ).toBe("turn-successful-retry");
  });

  it("uses the turn-start runtime mode when the projected thread runtime is stale", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-runtime-mode-stale-full-access"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );
    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.runtimeMode === "full-access";
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-runtime-mode-stale"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-runtime-mode-stale"),
          role: "user",
          text: "run with queued mode",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      runtimeMode: "approval-required",
    });

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.session?.runtimeMode === "approval-required";
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.runtimeMode).toBe("full-access");
    expect(thread?.session?.runtimeMode).toBe("approval-required");
  });

  it("restarts a parked waiting Claude session before sending the next turn", async () => {
    const harness = await createHarness({
      threadModelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-opus-4-6",
      },
    });
    const now = "2026-01-01T00:00:00.000Z";
    const resumeCursor = { resume: "sdk-session-waiting", resumeSessionAt: "assistant-uuid-1" };
    const providerInstanceId = ProviderInstanceId.make("claudeAgent");

    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("claudeAgent"),
      providerInstanceId,
      status: "waiting",
      runtimeMode: "approval-required",
      model: "claude-opus-4-6",
      threadId: ThreadId.make("thread-1"),
      resumeCursor,
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-waiting-claude"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "waiting",
          providerName: "claudeAgent",
          providerInstanceId,
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-waiting-claude"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-waiting-claude"),
          role: "user",
          text: "child finished, continue",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      providerInstanceId,
      resumeCursor,
      runtimeMode: "full-access",
    });
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      input: "child finished, continue",
    });
  });

  it("generates a thread title on the first turn", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const seededTitle = "Please investigate reconnect failures after restar...";
    harness.generateThreadTitle.mockReturnValue(Effect.succeed({ title: "Generated title" }));

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-seed"),
        threadId: ThreadId.make("thread-1"),
        title: seededTitle,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-title"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-title"),
          role: "user",
          text: "Please investigate reconnect failures after restarting the session.",
          attachments: [],
        },
        titleSeed: seededTitle,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.generateThreadTitle.mock.calls.length === 1);
    expect(harness.generateThreadTitle.mock.calls[0]?.[0]).toMatchObject({
      message: "Please investigate reconnect failures after restarting the session.",
    });

    await waitFor(async () => {
      const readModel = await harness.readModel();
      return (
        readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"))?.title ===
        "Generated title"
      );
    });
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Generated title");
  });

  it("does not overwrite an existing custom thread title on the first turn", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const seededTitle = "Please investigate reconnect failures after restar...";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-custom"),
        threadId: ThreadId.make("thread-1"),
        title: "Keep this custom title",
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-title-preserve"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-title-preserve"),
          role: "user",
          text: "Please investigate reconnect failures after restarting the session.",
          attachments: [],
        },
        titleSeed: seededTitle,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.generateThreadTitle).not.toHaveBeenCalled();

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Keep this custom title");
  });

  it("matches the client-seeded title even when the outgoing prompt is reformatted", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const seededTitle = "Fix reconnect spinner on resume";
    harness.generateThreadTitle.mockReturnValue(
      Effect.succeed({
        title: "Reconnect spinner resume bug",
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-formatted-seed"),
        threadId: ThreadId.make("thread-1"),
        title: seededTitle,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-title-formatted"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-title-formatted"),
          role: "user",
          text: "[effort:high]\\n\\nFix reconnect spinner on resume",
          attachments: [],
        },
        titleSeed: seededTitle,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.generateThreadTitle.mock.calls.length === 1);
    await waitFor(async () => {
      const readModel = await harness.readModel();
      return (
        readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"))?.title ===
        "Reconnect spinner resume bug"
      );
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Reconnect spinner resume bug");
  });

  it("generates a worktree branch name for the first turn", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-branch"),
        threadId: ThreadId.make("thread-1"),
        branch: "t3code/1234abcd",
        worktreePath: "/tmp/provider-project-worktree",
      }),
    );

    harness.generateBranchName.mockImplementation((input: unknown) =>
      Effect.succeed({
        branch:
          typeof input === "object" &&
          input !== null &&
          "modelSelection" in input &&
          typeof input.modelSelection === "object" &&
          input.modelSelection !== null &&
          "model" in input.modelSelection &&
          typeof input.modelSelection.model === "string"
            ? `feature/${input.modelSelection.model}`
            : "feature/generated",
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-branch-model"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-branch-model"),
          role: "user",
          text: "Add a safer reconnect backoff.",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.generateBranchName.mock.calls.length === 1);
    await waitFor(() => harness.refreshStatus.mock.calls.length === 1);
    expect(harness.generateBranchName.mock.calls[0]?.[0]).toMatchObject({
      message: "Add a safer reconnect backoff.",
    });
    expect(harness.refreshStatus.mock.calls[0]?.[0]).toBe("/tmp/provider-project-worktree");
  });

  it("does not rename a temporary branch when another thread shares the worktree", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const sharedWorktreePath = "/tmp/provider-project-worktree";
    const temporaryBranch = "t3code/1234abcd";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-shared-branch"),
        threadId: ThreadId.make("thread-1"),
        branch: temporaryBranch,
        worktreePath: sharedWorktreePath,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-shared-parent-create"),
        threadId: ThreadId.make("thread-shared-parent"),
        projectId: asProjectId("project-1"),
        title: "Shared parent",
        modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5-codex"),
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: temporaryBranch,
        worktreePath: sharedWorktreePath,
        createdAt: now,
      }),
    );

    harness.generateBranchName.mockReturnValue(Effect.succeed({ branch: "feature/generated" }));

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-shared-worktree-branch"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-shared-worktree-branch"),
          role: "user",
          text: "Read this shared checkout.",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    await harness.drain();
    await runtime!.runPromise(Effect.yieldNow);

    expect(harness.generateBranchName).not.toHaveBeenCalled();
    expect(harness.renameBranch).not.toHaveBeenCalled();
    expect(harness.refreshStatus).not.toHaveBeenCalled();
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.branch).toBe(temporaryBranch);
    expect(thread?.worktreePath).toBe(sharedWorktreePath);
  });

  it("does not rename a temporary branch when another thread shares the removable worktree root", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const sharedWorktreeRoot = "/tmp/provider-project-worktree";
    const appWorkspace = `${sharedWorktreeRoot}/packages/app`;
    const libWorkspace = `${sharedWorktreeRoot}/packages/lib`;
    const temporaryBranch = "t3code/1234abcd";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-shared-root-branch"),
        threadId: ThreadId.make("thread-1"),
        branch: temporaryBranch,
        worktreePath: appWorkspace,
        worktreeRemovalPath: sharedWorktreeRoot,
      }),
    );
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-shared-root-sibling-create"),
        threadId: ThreadId.make("thread-shared-root-sibling"),
        projectId: asProjectId("project-1"),
        title: "Shared root sibling",
        modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5-codex"),
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: temporaryBranch,
        worktreePath: libWorkspace,
        worktreeRemovable: true,
        worktreeRemovalPath: sharedWorktreeRoot,
        createdAt: now,
      }),
    );

    harness.generateBranchName.mockReturnValue(Effect.succeed({ branch: "feature/generated" }));

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-shared-worktree-root-branch"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-shared-worktree-root-branch"),
          role: "user",
          text: "Read this shared checkout.",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    await harness.drain();
    await runtime!.runPromise(Effect.yieldNow);

    expect(harness.generateBranchName).not.toHaveBeenCalled();
    expect(harness.renameBranch).not.toHaveBeenCalled();
    expect(harness.refreshStatus).not.toHaveBeenCalled();
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.branch).toBe(temporaryBranch);
    expect(thread?.worktreePath).toBe(appWorkspace);
    expect(thread?.worktreeRemovalPath).toBe(sharedWorktreeRoot);
  });

  it("forwards codex model options through session start and turn send", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-fast"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-fast"),
          role: "user",
          text: "hello fast mode",
          attachments: [],
        },
        modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.3-codex", [
          { id: "reasoningEffort", value: "high" },
          { id: "fastMode", value: true },
        ]),
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.3-codex", [
        { id: "reasoningEffort", value: "high" },
        { id: "fastMode", value: true },
      ]),
    });
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.3-codex", [
        { id: "reasoningEffort", value: "high" },
        { id: "fastMode", value: true },
      ]),
    });
  });

  it("forwards claude effort options through session start and turn send", async () => {
    const harness = await createHarness({
      threadModelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-sonnet-4-6",
      },
    });
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-claude-effort"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-claude-effort"),
          role: "user",
          text: "hello with effort",
          attachments: [],
        },
        modelSelection: createModelSelection(
          ProviderInstanceId.make("claudeAgent"),
          "claude-sonnet-4-6",
          [{ id: "effort", value: "max" }],
        ),
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      modelSelection: createModelSelection(
        ProviderInstanceId.make("claudeAgent"),
        "claude-sonnet-4-6",
        [{ id: "effort", value: "max" }],
      ),
    });
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      modelSelection: createModelSelection(
        ProviderInstanceId.make("claudeAgent"),
        "claude-sonnet-4-6",
        [{ id: "effort", value: "max" }],
      ),
    });
  });

  it("forwards claude fast mode options through session start and turn send", async () => {
    const harness = await createHarness({
      threadModelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-opus-4-6",
      },
    });
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-claude-fast-mode"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-claude-fast-mode"),
          role: "user",
          text: "hello with fast mode",
          attachments: [],
        },
        modelSelection: createModelSelection(
          ProviderInstanceId.make("claudeAgent"),
          "claude-opus-4-6",
          [{ id: "fastMode", value: true }],
        ),
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      modelSelection: createModelSelection(
        ProviderInstanceId.make("claudeAgent"),
        "claude-opus-4-6",
        [{ id: "fastMode", value: true }],
      ),
    });
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      modelSelection: createModelSelection(
        ProviderInstanceId.make("claudeAgent"),
        "claude-opus-4-6",
        [{ id: "fastMode", value: true }],
      ),
    });
  });

  it("forwards plan interaction mode to the provider turn request", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.interaction-mode.set",
        commandId: CommandId.make("cmd-interaction-mode-set-plan"),
        threadId: ThreadId.make("thread-1"),
        interactionMode: "plan",
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-plan"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-plan"),
          role: "user",
          text: "plan this change",
          attachments: [],
        },
        interactionMode: "plan",
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      interactionMode: "plan",
    });
  });

  it("preserves the active session model when in-session model switching is unsupported", async () => {
    const harness = await createHarness({ sessionModelSwitch: "unsupported" });
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-unsupported-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-unsupported-1"),
          role: "user",
          text: "first",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-unsupported-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-unsupported-2"),
          role: "user",
          text: "second",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 2);

    expect(harness.sendTurn.mock.calls[1]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
    });
  });

  effectIt.effect(
    "rejects changing models after start when the provider requires a new thread",
    () =>
      Effect.gen(function* () {
        const harness = yield* Effect.promise(() =>
          createHarness({ requiresNewThreadForModelChange: true }),
        );
        const now = "2026-01-01T00:00:00.000Z";

        yield* harness.engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-turn-start-restricted-1"),
          threadId: ThreadId.make("thread-1"),
          message: {
            messageId: asMessageId("user-message-restricted-1"),
            role: "user",
            text: "first",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt: now,
        });

        yield* Effect.promise(() => waitFor(() => harness.sendTurn.mock.calls.length === 1));

        yield* harness.engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-turn-start-restricted-2"),
          threadId: ThreadId.make("thread-1"),
          message: {
            messageId: asMessageId("user-message-restricted-2"),
            role: "user",
            text: "second",
            attachments: [],
          },
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5.1-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt: now,
        });

        yield* Effect.promise(() =>
          waitFor(async () => {
            const readModel = await harness.readModel();
            const thread = readModel.threads.find(
              (entry) => entry.id === ThreadId.make("thread-1"),
            );
            return (
              thread?.activities.some(
                (activity) => activity.kind === "provider.turn.start.failed",
              ) ?? false
            );
          }),
        );

        expect(harness.sendTurn).toHaveBeenCalledTimes(1);
        const readModel = yield* Effect.promise(() => harness.readModel());
        const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
        expect(
          thread?.activities.find((activity) => activity.kind === "provider.turn.start.failed"),
        ).toMatchObject({
          payload: {
            detail: expect.stringContaining(
              "cannot switch models after the conversation has started",
            ),
          },
        });
      }),
  );

  it("starts a first turn on the requested provider instance even when it differs from the thread model", async () => {
    const harness = await createHarness({
      threadModelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5-codex" },
    });
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-provider-first"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-provider-first"),
          role: "user",
          text: "hello claude",
          attachments: [],
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("claudeAgent"),
          model: "claude-opus-4-6",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    expect(harness.startSession).toHaveBeenCalledTimes(1);
    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      provider: ProviderDriverKind.make("claudeAgent"),
      providerInstanceId: ProviderInstanceId.make("claudeAgent"),
      modelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-opus-4-6",
      },
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.providerName).toBe("claudeAgent");
    expect(thread?.session?.providerInstanceId).toBe(ProviderInstanceId.make("claudeAgent"));
    expect(
      thread?.activities.find((activity) => activity.kind === "provider.turn.start.failed"),
    ).toBeUndefined();
  });

  it("reuses the same provider session when runtime mode is unchanged", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-unchanged-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-unchanged-1"),
          role: "user",
          text: "first",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-unchanged-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-unchanged-2"),
          role: "user",
          text: "second",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 2);
    expect(harness.startSession.mock.calls.length).toBe(1);
    expect(harness.stopSession.mock.calls.length).toBe(0);
  });

  it("restarts an existing Codex thread on a compatible requested instance", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-compatible-codex-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-compatible-codex-1"),
          role: "user",
          text: "first",
          attachments: [],
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-compatible-codex-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-compatible-codex-2"),
          role: "user",
          text: "second",
          attachments: [],
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex_work"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: "2026-01-01T00:00:00.000Z",
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 2);

    expect(harness.startSession).toHaveBeenCalledTimes(2);
    expect(harness.startSession.mock.calls[1]?.[1]).toMatchObject({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex_work"),
      resumeCursor: { opaque: "resume-1" },
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.providerInstanceId).toBe(ProviderInstanceId.make("codex_work"));
  });

  it("restarts the provider session when the thread workspace changes", async () => {
    const harness = await createHarness({
      threadModelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-sonnet-4-6",
      },
    });
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-workspace-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-workspace-1"),
          role: "user",
          text: "first in project root",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      cwd: "/tmp/provider-project",
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-worktree-change"),
        threadId: ThreadId.make("thread-1"),
        worktreePath: "/tmp/provider-project-worktree",
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-workspace-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-workspace-2"),
          role: "user",
          text: "second in worktree",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 2);
    await waitFor(() => harness.sendTurn.mock.calls.length === 2);
    expect(harness.stopSession.mock.calls.length).toBe(0);
    expect(harness.startSession.mock.calls[1]?.[1]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      cwd: "/tmp/provider-project-worktree",
      resumeCursor: { opaque: "resume-1" },
      modelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-sonnet-4-6",
      },
      runtimeMode: "approval-required",
    });
  });

  it("passes the active turn when restarting an in-flight provider session", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-active-restart-runtime-initial"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-active-restart"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-active-restart"),
          role: "user",
          text: "first active turn",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    const activeTurnId = asTurnId("turn-1");
    const activeSession = harness.runtimeSessions[0];
    expect(activeSession).toBeDefined();
    harness.runtimeSessions[0] = {
      ...activeSession!,
      status: "running",
      activeTurnId,
    };

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-active-restart-runtime-change"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 2);
    expect(harness.startSession.mock.calls[1]?.[1]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-1" },
      activeTurnId,
      runtimeMode: "approval-required",
    });
    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.session?.activeTurnId === activeTurnId;
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.status).toBe("running");
    expect(thread?.session?.activeTurnId).toBe(activeTurnId);
  });

  it("does not pass stale active turn ids from ready provider sessions", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-stale-active-restart-runtime-initial"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-stale-active-restart"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-stale-active-restart"),
          role: "user",
          text: "completed turn with stale active id",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    const activeSession = harness.runtimeSessions[0];
    expect(activeSession).toBeDefined();
    harness.runtimeSessions[0] = {
      ...activeSession!,
      status: "ready",
      activeTurnId: asTurnId("turn-1"),
    };

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-stale-active-restart-runtime-change"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 2);
    const restartInput = harness.startSession.mock.calls[1]?.[1] as
      | { activeTurnId?: TurnId; resumeCursor?: unknown }
      | undefined;
    expect(restartInput).toMatchObject({
      resumeCursor: { opaque: "resume-1" },
    });
    expect(restartInput?.activeTurnId).toBeUndefined();
  });

  it("restarts claude sessions when claude effort changes", async () => {
    const harness = await createHarness({
      threadModelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-sonnet-4-6",
      },
    });
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-claude-effort-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-claude-effort-1"),
          role: "user",
          text: "first claude turn",
          attachments: [],
        },
        modelSelection: createModelSelection(
          ProviderInstanceId.make("claudeAgent"),
          "claude-sonnet-4-6",
          [{ id: "effort", value: "medium" }],
        ),
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-claude-effort-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-claude-effort-2"),
          role: "user",
          text: "second claude turn",
          attachments: [],
        },
        modelSelection: createModelSelection(
          ProviderInstanceId.make("claudeAgent"),
          "claude-sonnet-4-6",
          [{ id: "effort", value: "max" }],
        ),
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 2);
    await waitFor(() => harness.sendTurn.mock.calls.length === 2);
    expect(harness.startSession.mock.calls[1]?.[1]).toMatchObject({
      resumeCursor: { opaque: "resume-1" },
      modelSelection: createModelSelection(
        ProviderInstanceId.make("claudeAgent"),
        "claude-sonnet-4-6",
        [{ id: "effort", value: "max" }],
      ),
    });
  });

  it("restarts the provider session when runtime mode is updated on the thread", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-runtime-mode-set-initial-full-access"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-runtime-mode-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-runtime-mode-1"),
          role: "user",
          text: "first",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-runtime-mode-set-1"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.runtimeMode === "approval-required";
    });
    await waitFor(() => harness.startSession.mock.calls.length === 2);
    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-runtime-mode-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-runtime-mode-2"),
          role: "user",
          text: "second",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 2);

    expect(harness.stopSession.mock.calls.length).toBe(0);
    expect(harness.startSession.mock.calls[1]?.[1]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-1" },
      runtimeMode: "approval-required",
    });
    expect(harness.sendTurn.mock.calls[1]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.threadId).toBe("thread-1");
    expect(thread?.session?.runtimeMode).toBe("approval-required");
  });

  it("does not inject derived model options when restarting claude on runtime mode changes", async () => {
    const harness = await createHarness({
      threadModelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-opus-4-6",
      },
    });
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-runtime-mode-claude"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "claudeAgent",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-runtime-mode-set-claude-no-options"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);

    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      modelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-opus-4-6",
      },
      runtimeMode: "approval-required",
    });
  });

  it("does not stop the active session when restart fails before rebind", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-runtime-mode-set-initial-full-access-2"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-restart-failure-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-restart-failure-1"),
          role: "user",
          text: "first",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    harness.startSession.mockImplementationOnce(
      (_: unknown, __: unknown) => Effect.fail("simulated restart failure") as never,
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-runtime-mode-set-restart-failure"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.runtimeMode === "approval-required";
    });
    await waitFor(() => harness.startSession.mock.calls.length === 2);
    await harness.drain();

    expect(harness.stopSession.mock.calls.length).toBe(0);
    expect(harness.sendTurn.mock.calls.length).toBe(1);

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.threadId).toBe("thread-1");
    expect(thread?.session?.runtimeMode).toBe("full-access");
  });

  it("rejects provider changes after a thread is already bound to a session provider", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-provider-switch-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-provider-switch-1"),
          role: "user",
          text: "first",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-provider-switch-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-provider-switch-2"),
          role: "user",
          text: "second",
          attachments: [],
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("claudeAgent"),
          model: "claude-opus-4-6",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return (
        thread?.activities.some((activity) => activity.kind === "provider.turn.start.failed") ??
        false
      );
    });

    expect(harness.startSession.mock.calls.length).toBe(1);
    expect(harness.sendTurn.mock.calls.length).toBe(1);
    expect(harness.stopSession.mock.calls.length).toBe(0);

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.threadId).toBe("thread-1");
    expect(thread?.session?.providerName).toBe("codex");
    expect(thread?.session?.runtimeMode).toBe("approval-required");
    expect(
      thread?.activities.find((activity) => activity.kind === "provider.turn.start.failed"),
    ).toMatchObject({
      payload: {
        detail: expect.stringContaining("cannot switch to 'claudeAgent'"),
      },
    });
  });

  it("rejects cross-driver provider changes after the existing thread session has stopped", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-stopped-provider-switch"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "stopped",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-stopped-provider-switch"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-stopped-provider-switch"),
          role: "user",
          text: "continue with claude",
          attachments: [],
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("claudeAgent"),
          model: "claude-opus-4-6",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return (
        thread?.activities.some((activity) => activity.kind === "provider.turn.start.failed") ??
        false
      );
    });

    expect(harness.startSession.mock.calls.length).toBe(0);
    expect(harness.sendTurn.mock.calls.length).toBe(0);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.status).toBe("stopped");
    expect(thread?.session?.lastError).toBeNull();
    expect(
      thread?.activities.find((activity) => activity.kind === "provider.turn.start.failed"),
    ).toMatchObject({
      payload: {
        detail: expect.stringContaining("cannot switch to 'claudeAgent'"),
      },
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-stopped-provider-switch-retry"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-stopped-provider-switch-retry"),
          role: "user",
          text: "try claude again",
          attachments: [],
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("claudeAgent"),
          model: "claude-opus-4-6",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const nextReadModel = await harness.readModel();
      const nextThread = nextReadModel.threads.find(
        (entry) => entry.id === ThreadId.make("thread-1"),
      );
      return (
        nextThread?.activities.filter((activity) => activity.kind === "provider.turn.start.failed")
          .length === 2
      );
    });
    expect(harness.startSession.mock.calls.length).toBe(0);
  });

  it("reacts to thread.turn.interrupt-requested by calling provider interrupt", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-1"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.interrupt",
        commandId: CommandId.make("cmd-turn-interrupt"),
        threadId: ThreadId.make("thread-1"),
        turnId: asTurnId("turn-1"),
        createdAt: now,
      }),
    );

    await waitFor(() => harness.interruptTurn.mock.calls.length === 1);
    expect(harness.interruptTurn.mock.calls[0]?.[0]).toEqual({
      threadId: "thread-1",
    });
  });

  it("starts a fresh session when only projected session state exists", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-stale"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-stale"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-stale"),
          role: "user",
          text: "resume codex",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
      runtimeMode: "approval-required",
    });
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
    });
  });

  it("rejects active runtime sessions that are missing provider instance ids", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-missing-instance"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );
    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      threadId: ThreadId.make("thread-1"),
      cwd: "/tmp/provider-project",
      resumeCursor: { opaque: "resume-without-instance" },
      createdAt: now,
      updatedAt: now,
    });

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-missing-instance"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-missing-instance"),
          role: "user",
          text: "resume codex",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return (
        thread?.activities.some((activity) => activity.kind === "provider.turn.start.failed") ??
        false
      );
    });

    expect(harness.startSession.mock.calls.length).toBe(0);
    expect(harness.sendTurn.mock.calls.length).toBe(0);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.find((activity) => activity.kind === "provider.turn.start.failed"),
    ).toMatchObject({
      payload: {
        detail: expect.stringContaining("without a provider instance id"),
      },
    });
  });

  it("reacts to thread.approval.respond by forwarding provider approval response", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-for-approval"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.approval.respond",
        commandId: CommandId.make("cmd-approval-respond"),
        threadId: ThreadId.make("thread-1"),
        requestId: asApprovalRequestId("approval-request-1"),
        decision: "accept",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.respondToRequest.mock.calls.length === 1);
    expect(harness.respondToRequest.mock.calls[0]?.[0]).toEqual({
      threadId: "thread-1",
      requestId: "approval-request-1",
      decision: "accept",
    });
  });

  it("reacts to thread.user-input.respond by forwarding structured user input answers", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-for-user-input"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.user-input.respond",
        commandId: CommandId.make("cmd-user-input-respond"),
        threadId: ThreadId.make("thread-1"),
        requestId: asApprovalRequestId("user-input-request-1"),
        answers: {
          sandbox_mode: "workspace-write",
        },
        createdAt: now,
      }),
    );

    await waitFor(() => harness.respondToUserInput.mock.calls.length === 1);
    expect(harness.respondToUserInput.mock.calls[0]?.[0]).toEqual({
      threadId: "thread-1",
      requestId: "user-input-request-1",
      answers: {
        sandbox_mode: "workspace-write",
      },
    });
  });

  it("surfaces stale provider approval request failures without faking approval resolution", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    harness.respondToRequest.mockImplementation(() =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: ProviderDriverKind.make("codex"),
          method: "session/request_permission",
          detail: "Unknown pending permission request: approval-request-1",
        }),
      ),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-for-approval-error"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.activity.append",
        commandId: CommandId.make("cmd-approval-requested"),
        threadId: ThreadId.make("thread-1"),
        activity: {
          id: EventId.make("activity-approval-requested"),
          tone: "approval",
          kind: "approval.requested",
          summary: "Command approval requested",
          payload: {
            requestId: "approval-request-1",
            requestKind: "command",
          },
          turnId: null,
          createdAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.approval.respond",
        commandId: CommandId.make("cmd-approval-respond-stale"),
        threadId: ThreadId.make("thread-1"),
        requestId: asApprovalRequestId("approval-request-1"),
        decision: "acceptForSession",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      if (!thread) return false;
      return thread.activities.some(
        (activity) => activity.kind === "provider.approval.respond.failed",
      );
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread).toBeDefined();

    const failureActivity = thread?.activities.find(
      (activity) => activity.kind === "provider.approval.respond.failed",
    );
    expect(failureActivity).toBeDefined();
    expect(failureActivity?.payload).toMatchObject({
      requestId: "approval-request-1",
      detail: expect.stringContaining("Stale pending approval request: approval-request-1"),
    });

    const resolvedActivity = thread?.activities.find(
      (activity) =>
        activity.kind === "approval.resolved" &&
        typeof activity.payload === "object" &&
        activity.payload !== null &&
        (activity.payload as Record<string, unknown>).requestId === "approval-request-1",
    );
    expect(resolvedActivity).toBeUndefined();
  });

  it("surfaces non-resumable provider user-input callbacks as stale failures", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    harness.respondToUserInput.mockImplementation(() =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: ProviderDriverKind.make("claudeAgent"),
          method: "item/tool/respondToUserInput",
          detail: "Unknown pending Codex user input request: user-input-request-1",
        }),
      ),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-for-user-input-error"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "claudeAgent",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.activity.append",
        commandId: CommandId.make("cmd-user-input-requested"),
        threadId: ThreadId.make("thread-1"),
        activity: {
          id: EventId.make("activity-user-input-requested"),
          tone: "info",
          kind: "user-input.requested",
          summary: "User input requested",
          payload: {
            requestId: "user-input-request-1",
            questions: [
              {
                id: "sandbox_mode",
                header: "Sandbox",
                question: "Which mode should be used?",
                options: [
                  {
                    label: "workspace-write",
                    description: "Allow workspace writes only",
                  },
                ],
              },
            ],
          },
          turnId: null,
          createdAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.user-input.respond",
        commandId: CommandId.make("cmd-user-input-respond-stale"),
        threadId: ThreadId.make("thread-1"),
        requestId: asApprovalRequestId("user-input-request-1"),
        answers: {
          sandbox_mode: "workspace-write",
        },
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      if (!thread) return false;
      return thread.activities.some(
        (activity) => activity.kind === "provider.user-input.respond.failed",
      );
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread).toBeDefined();

    const failureActivity = thread?.activities.find(
      (activity) => activity.kind === "provider.user-input.respond.failed",
    );
    expect(failureActivity).toBeDefined();
    expect(failureActivity?.payload).toMatchObject({
      requestId: "user-input-request-1",
      detail: expect.stringContaining("Stale pending user-input request: user-input-request-1"),
    });

    const resolvedActivity = thread?.activities.find(
      (activity) =>
        activity.kind === "user-input.resolved" &&
        typeof activity.payload === "object" &&
        activity.payload !== null &&
        (activity.payload as Record<string, unknown>).requestId === "user-input-request-1",
    );
    expect(resolvedActivity).toBeUndefined();
  });

  it("reacts to thread.session.stop by stopping provider session and clearing thread session state", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-for-stop"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex_work"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await runtime!.runPromise(
      harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-session-stop"),
        threadId: ThreadId.make("thread-1"),
        createdAt: now,
      }),
    );

    await waitFor(() => harness.stopSession.mock.calls.length === 1);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session).not.toBeNull();
    expect(thread?.session?.status).toBe("stopped");
    expect(thread?.session?.threadId).toBe("thread-1");
    expect(thread?.session?.providerInstanceId).toBe(ProviderInstanceId.make("codex_work"));
    expect(thread?.session?.activeTurnId).toBeNull();
  });
});
