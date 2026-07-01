import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type ModelSelection,
  type OrchestrationCommand,
  type OrchestrationProjectShell,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Crypto from "effect/Crypto";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { McpSchema, McpServer } from "effect/unstable/ai";

import { GitWorkflowService } from "../../../git/GitWorkflowService.ts";
import * as BootstrapTurnStartDispatcher from "../../../orchestration/Services/BootstrapTurnStartDispatcher.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderInstanceRegistry } from "../../../provider/Services/ProviderInstanceRegistry.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { ThreadToolkitRegistrationLive } from "../../McpHttpServer.ts";
import { ThreadStartRuntimeLive } from "./handlers.ts";

const projectId = ProjectId.make("project-thread-mcp");
const sourceThreadId = ThreadId.make("source-thread-mcp");
const modelSelection: ModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5",
  options: [{ id: "reasoningEffort", value: "high" }],
};
const sourceThread: OrchestrationThreadShell = {
  id: sourceThreadId,
  projectId,
  title: "Source",
  modelSelection,
  runtimeMode: "auto-accept-edits",
  interactionMode: "plan",
  branch: "feature/source",
  worktreePath: null,
  latestTurn: null,
  createdAt: "2026-06-16T00:00:00.000Z",
  updatedAt: "2026-06-16T00:00:00.000Z",
  archivedAt: null,
  session: null,
  latestUserMessageAt: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
  parentThreadId: null,
};
const project: OrchestrationProjectShell = {
  id: projectId,
  title: "Project",
  workspaceRoot: "/repo",
  defaultModelSelection: modelSelection,
  scripts: [],
  createdAt: "2026-06-16T00:00:00.000Z",
  updatedAt: "2026-06-16T00:00:00.000Z",
};
const invocation: McpInvocationContext.McpInvocationScope = {
  environmentId: EnvironmentId.make("environment-thread-mcp"),
  threadId: sourceThreadId,
  providerSessionId: "provider-session-thread-mcp",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(["thread-management"]),
  issuedAt: 1,
  expiresAt: Number.MAX_SAFE_INTEGER,
};
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  initializePayload: {
    protocolVersion: "2025-03-26",
    capabilities: {},
    clientInfo: { name: "thread-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

const TestCryptoLive = Layer.sync(Crypto.Crypto, () => {
  let nextByte = 0;
  return Crypto.make({
    randomBytes: (size) =>
      Uint8Array.from({ length: size }, () => {
        nextByte = (nextByte + 1) % 256;
        return nextByte;
      }),
    digest: (_algorithm, data) => Effect.succeed(data),
  });
});

const makeTestLayer = (commands: OrchestrationCommand[]) => {
  const bootstrapTurnStartDispatcherLayer = Layer.mock(
    BootstrapTurnStartDispatcher.BootstrapTurnStartDispatcher,
  )({
    dispatch: (command) =>
      Effect.sync(() => {
        commands.push(command);
        return { sequence: 1 };
      }),
  });

  return ThreadToolkitRegistrationLive.pipe(
    Layer.provideMerge(ThreadStartRuntimeLive),
    Layer.provideMerge(
      BootstrapTurnStartDispatcher.ActiveBootstrapTurnStartDispatcherLive.pipe(
        Layer.provide(bootstrapTurnStartDispatcherLayer),
      ),
    ),
    Layer.provideMerge(TestCryptoLive),
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provide(
      Layer.mock(ProjectionSnapshotQuery)({
        getProjectShellById: () => Effect.succeed(Option.some(project)),
        getThreadShellById: () => Effect.succeed(Option.some(sourceThread)),
      }),
    ),
    Layer.provide(
      Layer.mock(ProviderInstanceRegistry)({
        listInstances: Effect.succeed([]),
      }),
    ),
    Layer.provide(
      Layer.mock(GitWorkflowService)({
        listRefs: () =>
          Effect.succeed({
            refs: [
              {
                name: "main",
                current: false,
                isDefault: true,
                isRemote: false,
                worktreePath: null,
              },
            ],
            isRepo: true,
            hasPrimaryRemote: true,
            nextCursor: null,
            totalCount: 1,
          }),
        status: () =>
          Effect.succeed({
            isRepo: true,
            hasPrimaryRemote: true,
            isDefaultRef: false,
            refName: "feature/source",
            hasWorkingTreeChanges: false,
            workingTree: {
              files: [],
              insertions: 0,
              deletions: 0,
            },
            hasUpstream: true,
            aheadCount: 0,
            behindCount: 0,
            aheadOfDefaultCount: 0,
            pr: null,
          }),
      }),
    ),
    Layer.provide(
      Layer.mock(OrchestrationEngineService)({
        readEvents: () => Stream.empty,
        dispatch: () => Effect.succeed({ sequence: 1 }),
        streamDomainEvents: Stream.empty,
      }),
    ),
  );
};

const callStartTool = (arguments_: Record<string, unknown>, commands: OrchestrationCommand[]) =>
  Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    return yield* server
      .callTool({ name: "t3_thread_start", arguments: arguments_ })
      .pipe(
        Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
        Effect.provideService(McpSchema.McpServerClient, client),
      );
  }).pipe(Effect.provide(makeTestLayer(commands)));

it.effect("starts a new worktree thread by default and inherits source settings", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const result = yield* callStartTool({ prompt: "Investigate flaky tests" }, commands);

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      projectId,
      mode: "new_worktree",
      worktreePath: null,
    });
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.message.text).toBe("Investigate flaky tests");
    expect(command.modelSelection).toEqual(modelSelection);
    expect(command.runtimeMode).toBe("auto-accept-edits");
    expect(command.interactionMode).toBe("plan");
    expect(command.bootstrap?.createThread?.modelSelection).toEqual(modelSelection);
    expect(command.bootstrap?.prepareWorktree).toMatchObject({
      projectCwd: "/repo",
      baseBranch: "main",
    });
    expect(command.bootstrap?.runSetupScript).toBe(true);
  }),
);

it.effect("starts current-checkout threads with warning metadata", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const result = yield* callStartTool(
      { prompt: "Read current diff", mode: "current_checkout" },
      commands,
    );

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      projectId,
      mode: "current_checkout",
      branch: "feature/source",
      worktreePath: null,
    });
    expect(result.structuredContent).toHaveProperty("warning");
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toBeUndefined();
    expect(command.bootstrap?.createThread?.worktreePath).toBeNull();
  }),
);
