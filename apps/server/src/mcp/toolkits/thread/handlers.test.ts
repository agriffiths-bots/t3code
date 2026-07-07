import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  VcsProcessSpawnError,
  ThreadId,
  VcsUnsupportedOperationError,
  type ModelSelection,
  type OrchestrationCommand,
  type OrchestrationProjectShell,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import { McpSchema, McpServer } from "effect/unstable/ai";

import { GitWorkflowService } from "../../../git/GitWorkflowService.ts";
import * as VcsDriverRegistry from "../../../vcs/VcsDriverRegistry.ts";
import * as BootstrapTurnStartDispatcher from "../../../orchestration/Services/BootstrapTurnStartDispatcher.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderInstanceRegistry } from "../../../provider/Services/ProviderInstanceRegistry.ts";
import type { ProviderInstance } from "../../../provider/ProviderDriver.ts";
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
  repositoryIdentity: {
    canonicalKey: "git-local:/repo",
    locator: {
      source: "git-local",
      rootPath: "/repo",
    },
    rootPath: "/repo",
  },
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

const makeTempDirectory = (prefix: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    return yield* fileSystem.makeTempDirectory({ prefix });
  }).pipe(Effect.provide(NodeServices.layer), Effect.orDie);

const errorText = (content: ReadonlyArray<unknown> | undefined): string =>
  (content ?? [])
    .map((entry) => {
      const text = (entry as { readonly text?: unknown }).text;
      return typeof text === "string" ? text : "";
    })
    .join(" ");

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

type ModelFixture =
  | string
  | {
      readonly slug: string;
      readonly optionId: string;
      readonly value: string;
    };

const makeModelInstance = (
  instanceId: string,
  driverKind: string,
  models: ReadonlyArray<ModelFixture>,
) =>
  ({
    instanceId: ProviderInstanceId.make(instanceId),
    driverKind: ProviderDriverKind.make(driverKind),
    enabled: true,
    snapshot: {
      getSnapshot: Effect.succeed({
        status: "ready",
        models: models.map((entry) =>
          typeof entry === "string"
            ? { slug: entry, capabilities: null }
            : {
                slug: entry.slug,
                capabilities: {
                  optionDescriptors: [
                    {
                      id: entry.optionId,
                      label: "Reasoning",
                      type: "select" as const,
                      options: ["low", "medium", "high", "xhigh", "max"].map((value) => ({
                        id: value,
                        label: value,
                        ...(value === entry.value ? { isDefault: true } : {}),
                      })),
                      currentValue: entry.value,
                    },
                  ],
                },
              },
        ),
      }),
    },
  }) as unknown as ProviderInstance;

interface TestLayerOptions {
  readonly providerInstances?: ReadonlyArray<ProviderInstance>;
  readonly project?: OrchestrationProjectShell;
  readonly sourceThread?: OrchestrationThreadShell;
  readonly gitWorkflow?: Partial<GitWorkflowService["Service"]>;
  readonly vcsDetect?: VcsDriverRegistry.VcsDriverRegistry["Service"]["detect"];
}

const vcsFreshness = {
  source: "live-local" as const,
  observedAt: DateTime.makeUnsafe("1970-01-01T00:00:00.000Z"),
  expiresAt: Option.none(),
};

const repositoryMetadataRootForCwd = (cwd: string): string => {
  if (cwd.startsWith("/home/adam")) return "/home/adam/t3code";
  if (cwd.startsWith("/repo")) return "/repo";
  return cwd;
};

const makeGitHandle = (
  cwd: string,
  options: {
    readonly rootPath?: string;
    readonly metadataPath?: string;
  } = {},
): VcsDriverRegistry.VcsDriverHandle => {
  const metadataRoot = repositoryMetadataRootForCwd(cwd);
  return {
    kind: "git",
    repository: {
      kind: "git",
      rootPath: options.rootPath ?? cwd,
      metadataPath: options.metadataPath ?? `${metadataRoot}/.git`,
      freshness: vcsFreshness,
    },
    driver: {} as VcsDriverRegistry.VcsDriverHandle["driver"],
  };
};

const defaultGitWorkflow = {
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
} satisfies Partial<GitWorkflowService["Service"]>;

const nonRepoStatus = {
  isRepo: false,
  hasPrimaryRemote: false,
  isDefaultRef: false,
  refName: null,
  hasWorkingTreeChanges: false,
  workingTree: {
    files: [],
    insertions: 0,
    deletions: 0,
  },
  hasUpstream: false,
  aheadCount: 0,
  behindCount: 0,
  aheadOfDefaultCount: 0,
  pr: null,
};

const makeTestLayer = (commands: OrchestrationCommand[], options: TestLayerOptions = {}) => {
  const testProject = options.project ?? project;
  const testSourceThread = options.sourceThread ?? sourceThread;
  const providerInstances = options.providerInstances ?? [];
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
        getProjectShellById: () => Effect.succeed(Option.some(testProject)),
        getThreadShellById: () => Effect.succeed(Option.some(testSourceThread)),
      }),
    ),
    Layer.provide(
      Layer.mock(ProviderInstanceRegistry)({
        listInstances: Effect.succeed(providerInstances),
      }),
    ),
    Layer.provide(
      Layer.mock(GitWorkflowService)({
        ...defaultGitWorkflow,
        ...options.gitWorkflow,
      }),
    ),
    Layer.provide(
      Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
        detect: options.vcsDetect ?? ((input) => Effect.succeed(makeGitHandle(input.cwd))),
      }),
    ),
    Layer.provide(
      Layer.mock(OrchestrationEngineService)({
        readEvents: () => Stream.empty,
        dispatch: () => Effect.succeed({ sequence: 1 }),
        streamDomainEvents: Stream.empty,
      }),
    ),
    Layer.provideMerge(NodeServices.layer),
  );
};

const callStartTool = (
  arguments_: Record<string, unknown>,
  commands: OrchestrationCommand[],
  options: TestLayerOptions = {},
) =>
  Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    return yield* server
      .callTool({ name: "t3_thread_start", arguments: arguments_ })
      .pipe(
        Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
        Effect.provideService(McpSchema.McpServerClient, client),
      );
  }).pipe(Effect.provide(makeTestLayer(commands, options)));

it.effect("starts a new worktree thread by default and inherits source settings", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const result = yield* callStartTool({ prompt: "Investigate flaky tests" }, commands, {
      project: {
        ...project,
        workspaceRoot: "/repo/packages/app",
      },
      vcsDetect: (input) =>
        Effect.succeed(
          makeGitHandle(input.cwd, {
            rootPath: "/repo",
            metadataPath: "/repo/.git",
          }),
        ),
    });

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
    expect(command.titleSeed).toBe("Investigate flaky tests");
    expect(command.runtimeMode).toBe("auto-accept-edits");
    expect(command.interactionMode).toBe("plan");
    expect(command.bootstrap?.createThread?.modelSelection).toEqual(modelSelection);
    expect(command.bootstrap?.prepareWorktree).toMatchObject({
      projectCwd: "/repo/packages/app",
      baseBranch: "main",
      workspaceRelativePath: "packages/app",
    });
    expect(command.bootstrap?.createThread?.worktreeRemovable).toBe(true);
    expect(command.bootstrap?.runSetupScript).toBe(true);
  }),
);

it.effect(
  "degrades default worktree mode to current checkout when the project is not a git repo",
  () =>
    Effect.gen(function* () {
      const commands: OrchestrationCommand[] = [];
      const vcsCalls: string[] = [];
      const gitCalls: string[] = [];
      const result = yield* callStartTool({ prompt: "Run dispatch probe" }, commands, {
        project: {
          ...project,
          workspaceRoot: "/home/adam",
          repositoryIdentity: null,
        },
        sourceThread: {
          ...sourceThread,
          branch: null,
        },
        gitWorkflow: {
          status: (input) =>
            Effect.sync(() => {
              gitCalls.push(input.cwd);
              return nonRepoStatus;
            }),
        },
        vcsDetect: (input) =>
          Effect.sync(() => {
            vcsCalls.push(`${input.cwd}:${input.cache ?? "allow"}`);
            return null;
          }),
      });

      expect(result.isError).toBe(false);
      expect(result.structuredContent).toMatchObject({
        projectId,
        mode: "current_checkout",
        branch: null,
        worktreePath: null,
      });
      expect(result.structuredContent).toHaveProperty("warning");
      expect(String(result.structuredContent?.warning)).toContain("not a Git repository");
      const command = commands[0];
      expect(command?.type).toBe("thread.turn.start");
      if (command?.type !== "thread.turn.start") return;
      expect(command.bootstrap?.prepareWorktree).toBeUndefined();
      expect(command.bootstrap?.createThread?.worktreePath).toBeNull();
      expect(command.bootstrap?.createThread?.worktreeRemovable).toBe(false);
      expect(command.bootstrap?.createThread?.branch).toBeNull();
      expect(vcsCalls).toEqual(["/home/adam:allow", "/home/adam:bypass"]);
      expect(gitCalls).toEqual(["/home/adam"]);
    }),
);

it.effect("honors explicit existing worktree paths when the project is not a git repo", () =>
  // ADA-97: existing_worktree runs in an existing directory and creates nothing,
  // so a non-git project must not silently reroute an explicit path to the
  // project root.
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const explicitPath = yield* makeTempDirectory("t3-existing-wt-");
    const result = yield* callStartTool(
      {
        prompt: "Run current-directory worker",
        mode: "existing_worktree",
        worktreePath: explicitPath,
      },
      commands,
      {
        project: {
          ...project,
          workspaceRoot: "/home/adam",
          repositoryIdentity: null,
        },
        sourceThread: {
          ...sourceThread,
          branch: null,
        },
        gitWorkflow: {
          status: () => Effect.succeed(nonRepoStatus),
        },
        vcsDetect: () => Effect.succeed(null),
      },
    );

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      projectId,
      mode: "existing_worktree",
      branch: null,
      worktreePath: explicitPath,
    });
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toBeUndefined();
    expect(command.bootstrap?.createThread?.worktreePath).toBe(explicitPath);
    expect(command.bootstrap?.createThread?.worktreeRemovable).toBe(false);
  }),
);

it.effect("bases the child on an explicit git directory with a new worktree", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const explicitDir = yield* makeTempDirectory("t3-dir-git-");
    const result = yield* callStartTool(
      { prompt: "Work in the other repository", directory: explicitDir },
      commands,
      {},
    );

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      projectId,
      mode: "new_worktree",
      worktreePath: null,
    });
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toMatchObject({
      projectCwd: explicitDir,
      baseBranch: "main",
    });
    // Cross-repo worktrees are user-managed: the reaper resolves repositories
    // via the caller's projectId and would orphan them if marked removable.
    expect(command.bootstrap?.createThread?.worktreeRemovable).toBe(false);
    expect(String(result.structuredContent?.warning)).toContain("not auto-cleaned");
    // The caller project's setup script must never run inside an explicitly
    // targeted other repository.
    expect(command.bootstrap?.runSetupScript).toBe(false);
  }),
);

it.effect("rejects runSetupScript with an explicit directory", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const explicitDir = yield* makeTempDirectory("t3-dir-setup-");
    const result = yield* callStartTool(
      { prompt: "Setup elsewhere", directory: explicitDir, runSetupScript: true },
      commands,
      {},
    );
    expect(result.isError).toBe(true);
    expect(errorText(result.content)).toContain("runSetupScript is not supported with directory");
    expect(commands).toHaveLength(0);
  }),
);

it.effect("runs in place with a warning for an explicit non-git directory", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const explicitDir = yield* makeTempDirectory("t3-dir-plain-");
    const result = yield* callStartTool(
      { prompt: "Work in the plain directory", directory: explicitDir },
      commands,
      {
        sourceThread: {
          ...sourceThread,
          worktreePath: "/repo/wt-source",
          worktreeRemovalPath: "/repo/wt-source",
          worktreeRemovable: true,
        },
        gitWorkflow: {
          status: () => Effect.succeed(nonRepoStatus),
        },
        vcsDetect: () => Effect.succeed(null),
      },
    );

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      projectId,
      mode: "current_checkout",
      worktreePath: explicitDir,
    });
    expect(String(result.structuredContent?.warning)).toContain("not a Git repository");
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toBeUndefined();
    expect(command.bootstrap?.createThread?.worktreePath).toBe(explicitDir);
    // The child was directed elsewhere: it must not inherit the source thread's
    // worktree removal root (cleanup would remove the source's worktree).
    expect(command.bootstrap?.createThread?.worktreeRemovalPath).toBeNull();
  }),
);

it.effect("never borrows the caller project's branch for an explicit directory", () =>
  // The explicit directory may be a different repository: when its own refs
  // cannot resolve a base branch, the spawn must fail instead of falling back
  // to the caller project's checkout (an unrelated ref could share a name).
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const explicitDir = yield* makeTempDirectory("t3-dir-norefs-");
    const result = yield* callStartTool(
      { prompt: "Work in the detached repository", directory: explicitDir },
      commands,
      {
        gitWorkflow: {
          // The target repo resolves no default/current branch (e.g. detached
          // HEAD, remote-only refs); the caller project would resolve "main".
          listRefs: (input) =>
            input.cwd === explicitDir
              ? Effect.succeed({
                  refs: [],
                  isRepo: true,
                  hasPrimaryRemote: false,
                  nextCursor: null,
                  totalCount: 0,
                })
              : defaultGitWorkflow.listRefs(),
          status: (input) =>
            input.cwd === explicitDir
              ? Effect.succeed({ ...nonRepoStatus, isRepo: true })
              : defaultGitWorkflow.status(),
        },
      },
    );
    expect(result.isError).toBe(true);
    expect(errorText(result.content)).toContain("Could not resolve a base branch in directory");
    expect(commands).toHaveLength(0);
  }),
);

it.effect("rejects a relative directory", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const result = yield* callStartTool(
      { prompt: "Bad directory", directory: "relative/path" },
      commands,
      {},
    );
    expect(result.isError).toBe(true);
    expect(errorText(result.content)).toContain("absolute path");
    expect(commands).toHaveLength(0);
  }),
);

it.effect("rejects a directory that does not exist", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const result = yield* callStartTool(
      { prompt: "Bad directory", directory: "/nonexistent-t3-directory-test-8f2c" },
      commands,
      {},
    );
    expect(result.isError).toBe(true);
    expect(errorText(result.content)).toContain("does not exist");
    expect(commands).toHaveLength(0);
  }),
);

it.effect("rejects directory combined with worktreePath", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const explicitDir = yield* makeTempDirectory("t3-dir-conflict-");
    const result = yield* callStartTool(
      {
        prompt: "Ambiguous target",
        mode: "existing_worktree",
        directory: explicitDir,
        worktreePath: explicitDir,
      },
      commands,
      {},
    );
    expect(result.isError).toBe(true);
    expect(errorText(result.content)).toContain("not both");
    expect(commands).toHaveLength(0);
  }),
);

it.effect("degrades worktree mode when VCS detection reports an unsupported backend", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const result = yield* callStartTool({ prompt: "Run under unsupported vcs" }, commands, {
      project: {
        ...project,
        workspaceRoot: "/repo-jj",
        repositoryIdentity: null,
      },
      sourceThread: {
        ...sourceThread,
        branch: null,
      },
      gitWorkflow: {
        status: () => Effect.succeed(nonRepoStatus),
      },
      vcsDetect: () =>
        Effect.fail(
          new VcsUnsupportedOperationError({
            operation: "VcsDriverRegistry.detect",
            kind: "jj",
            detail: "Driver 'jj' is not registered.",
          }),
        ),
    });

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      projectId,
      mode: "current_checkout",
      worktreePath: null,
    });
    expect(result.structuredContent).toHaveProperty("warning");
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toBeUndefined();
    expect(command.bootstrap?.createThread?.worktreeRemovable).toBe(false);
  }),
);

it.effect("keeps an explicit child title authoritative by not auto-title seeding it", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const result = yield* callStartTool(
      { prompt: "Investigate flaky tests", title: "Worker T11 UX" },
      commands,
    );

    expect(result.isError).toBe(false);
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.createThread?.title).toBe("Worker T11 UX");
    expect(command.titleSeed).toBeUndefined();
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
    expect(command.bootstrap?.createThread?.worktreeRemovable).toBe(false);
  }),
);

it.effect("starts current-checkout threads on the source worktree checkout", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const result = yield* callStartTool(
      { prompt: "Read current worktree", mode: "current_checkout" },
      commands,
      {
        project: {
          ...project,
          workspaceRoot: "/repo/project",
        },
        sourceThread: {
          ...sourceThread,
          worktreePath: "/repo/worktree",
        },
      },
    );

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      projectId,
      mode: "current_checkout",
      branch: "feature/source",
      worktreePath: "/repo/worktree",
    });
    expect(result.structuredContent).toHaveProperty("warning");
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toBeUndefined();
    expect(command.bootstrap?.createThread?.worktreePath).toBe("/repo/worktree");
    expect(command.bootstrap?.createThread?.worktreeRemovable).toBe(false);
  }),
);

it.effect("preserves source worktree removal root for current-checkout subdirectory children", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const result = yield* callStartTool(
      { prompt: "Read current worktree package", mode: "current_checkout" },
      commands,
      {
        project: {
          ...project,
          workspaceRoot: "/repo/project",
        },
        sourceThread: {
          ...sourceThread,
          worktreePath: "/repo/worktree/packages/app",
          worktreeRemovable: true,
          worktreeRemovalPath: "/repo/worktree",
        },
        vcsDetect: (input) =>
          Effect.succeed(
            makeGitHandle(input.cwd, {
              rootPath: input.cwd.startsWith("/repo/worktree") ? "/repo/worktree" : "/repo",
              metadataPath: "/repo/.git",
            }),
          ),
      },
    );

    expect(result.isError).toBe(false);
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toBeUndefined();
    expect(command.bootstrap?.createThread?.worktreePath).toBe("/repo/worktree/packages/app");
    expect(command.bootstrap?.createThread?.worktreeRemovable).toBe(false);
    expect(command.bootstrap?.createThread?.worktreeRemovalPath).toBe("/repo/worktree");
  }),
);

it.effect("does not mark caller-supplied existing worktrees as removable", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const existingWorktree = yield* makeTempDirectory("t3-existing-keep-");
    const result = yield* callStartTool(
      {
        prompt: "Use existing checkout",
        mode: "existing_worktree",
        worktreePath: existingWorktree,
      },
      commands,
    );

    expect(result.isError).toBe(false);
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.createThread?.worktreePath).toBe(existingWorktree);
    expect(command.bootstrap?.createThread?.worktreeRemovable).toBe(false);
    expect(command.bootstrap?.createThread?.worktreeRemovalPath).toBeNull();
  }),
);

it.effect("starts a new worktree from a detached parent using the project default branch", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const listRefCwds: string[] = [];
    const result = yield* callStartTool({ prompt: "Continue in isolation" }, commands, {
      sourceThread: {
        ...sourceThread,
        branch: null,
        worktreePath: "/repo/worktree",
      },
      gitWorkflow: {
        listRefs: (input) =>
          Effect.sync(() => {
            listRefCwds.push(input.cwd);
            return {
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
            };
          }),
      },
    });

    expect(result.isError).toBe(false);
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toMatchObject({
      projectCwd: "/repo/worktree",
      baseBranch: "main",
    });
    expect(listRefCwds).toEqual(["/repo/worktree"]);
  }),
);

it.effect(
  "falls back to the project checkout branch when a detached worktree has no branch refs",
  () =>
    Effect.gen(function* () {
      const commands: OrchestrationCommand[] = [];
      const gitCalls: string[] = [];
      const result = yield* callStartTool({ prompt: "Continue local checkout" }, commands, {
        project: {
          ...project,
          workspaceRoot: "/repo/project",
        },
        sourceThread: {
          ...sourceThread,
          branch: null,
          worktreePath: "/repo/worktree",
        },
        gitWorkflow: {
          listRefs: (input) =>
            Effect.sync(() => {
              gitCalls.push(`listRefs:${input.cwd}`);
              return {
                refs: [],
                isRepo: true,
                hasPrimaryRemote: false,
                nextCursor: null,
                totalCount: 0,
              };
            }),
          status: (input) =>
            Effect.sync(() => {
              gitCalls.push(`status:${input.cwd}`);
              return {
                isRepo: true,
                hasPrimaryRemote: input.cwd === "/repo/project",
                isDefaultRef: input.cwd === "/repo/project",
                refName: input.cwd === "/repo/project" ? "main" : null,
                hasWorkingTreeChanges: false,
                workingTree: {
                  files: [],
                  insertions: 0,
                  deletions: 0,
                },
                hasUpstream: input.cwd === "/repo/project",
                aheadCount: 0,
                behindCount: 0,
                aheadOfDefaultCount: 0,
                pr: null,
              };
            }),
        },
      });

      expect(result.isError).toBe(false);
      const command = commands[0];
      expect(command?.type).toBe("thread.turn.start");
      if (command?.type !== "thread.turn.start") return;
      expect(command.bootstrap?.prepareWorktree).toMatchObject({
        projectCwd: "/repo/worktree",
        baseBranch: "main",
      });
      expect(gitCalls).toEqual([
        "listRefs:/repo/worktree",
        "status:/repo/worktree",
        "listRefs:/repo/project",
        "status:/repo/project",
      ]);
    }),
);

it.effect("falls back to the project checkout when the source worktree path is stale", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const gitCalls: string[] = [];
    const vcsCalls: string[] = [];
    const result = yield* callStartTool({ prompt: "Continue after cleanup" }, commands, {
      project: {
        ...project,
        workspaceRoot: "/repo/packages/app",
      },
      sourceThread: {
        ...sourceThread,
        branch: null,
        worktreePath: "/repo/missing-worktree",
      },
      gitWorkflow: {
        listRefs: (input) =>
          Effect.sync(() => {
            gitCalls.push(`listRefs:${input.cwd}`);
            if (input.cwd === "/repo/missing-worktree") {
              return {
                refs: [],
                isRepo: false,
                hasPrimaryRemote: false,
                nextCursor: null,
                totalCount: 0,
              };
            }
            return {
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
            };
          }),
      },
      vcsDetect: (input) =>
        Effect.sync(() => {
          vcsCalls.push(input.cwd);
          return input.cwd === "/repo/missing-worktree"
            ? null
            : makeGitHandle(input.cwd, {
                rootPath: "/repo",
                metadataPath: "/repo/.git",
              });
        }),
    });

    expect(result.isError).toBe(false);
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toMatchObject({
      projectCwd: "/repo/packages/app",
      baseBranch: "main",
      workspaceRelativePath: "packages/app",
    });
    expect(vcsCalls).toEqual([
      "/repo/missing-worktree",
      "/repo/packages/app",
      "/repo/packages/app",
    ]);
    expect(gitCalls).toEqual(["listRefs:/repo/packages/app"]);
  }),
);

it.effect("falls back when source worktree detection fails because the cwd is missing", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const vcsCalls: string[] = [];
    const missingCwdCause = PlatformError.systemError({
      _tag: "NotFound",
      module: "ChildProcess",
      method: "spawn",
      syscall: "chdir",
      pathOrDescriptor: "/repo/missing-worktree",
      cause: Object.assign(new Error("spawn ENOENT"), { code: "ENOENT" }),
    });
    const result = yield* callStartTool(
      { prompt: "Continue after deleted checkout", baseBranch: "main" },
      commands,
      {
        project: {
          ...project,
          workspaceRoot: "/repo/project",
        },
        sourceThread: {
          ...sourceThread,
          worktreePath: "/repo/missing-worktree",
        },
        vcsDetect: (input) =>
          Effect.sync(() => {
            vcsCalls.push(input.cwd);
            return input.cwd;
          }).pipe(
            Effect.flatMap((cwd) =>
              cwd === "/repo/missing-worktree"
                ? Effect.fail(
                    new VcsProcessSpawnError({
                      operation: "VcsDriverRegistry.detect",
                      command: "git",
                      cwd,
                      cause: missingCwdCause,
                    }),
                  )
                : Effect.succeed(makeGitHandle(cwd)),
            ),
          ),
      },
    );

    expect(result.isError).toBe(false);
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toMatchObject({
      projectCwd: "/repo/project",
      baseBranch: "main",
    });
    expect(vcsCalls).toEqual(["/repo/missing-worktree", "/repo/project", "/repo/project"]);
  }),
);

it.effect("bypasses stale cached source worktree detection before reusing its cwd", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const vcsCalls: {
      readonly cwd: string;
      readonly cache: VcsDriverRegistry.VcsDriverResolveInput["cache"];
    }[] = [];
    const result = yield* callStartTool(
      { prompt: "Continue after cached checkout cleanup", baseBranch: "main" },
      commands,
      {
        project: {
          ...project,
          workspaceRoot: "/repo/project",
        },
        sourceThread: {
          ...sourceThread,
          worktreePath: "/repo/missing-worktree",
        },
        vcsDetect: (input) =>
          Effect.sync(() => {
            vcsCalls.push({ cwd: input.cwd, cache: input.cache });
            if (input.cwd === "/repo/missing-worktree" && input.cache === "bypass") {
              return null;
            }
            return makeGitHandle(input.cwd);
          }),
      },
    );

    expect(result.isError).toBe(false);
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toMatchObject({
      projectCwd: "/repo/project",
      baseBranch: "main",
    });
    expect(vcsCalls).toEqual([
      { cwd: "/repo/missing-worktree", cache: "bypass" },
      { cwd: "/repo/project", cache: undefined },
      { cwd: "/repo/project", cache: "bypass" },
    ]);
  }),
);

it.effect("keeps source package cwd when only the project checkout validation fails", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const vcsCalls: string[] = [];
    const projectMissingCause = PlatformError.systemError({
      _tag: "NotFound",
      module: "ChildProcess",
      method: "spawn",
      syscall: "chdir",
      pathOrDescriptor: "/repo/packages/app",
      cause: Object.assign(new Error("spawn ENOENT"), { code: "ENOENT" }),
    });
    const result = yield* callStartTool(
      { prompt: "Continue from valid source package", baseBranch: "main" },
      commands,
      {
        project: {
          ...project,
          workspaceRoot: "/repo/packages/app",
        },
        sourceThread: {
          ...sourceThread,
          worktreePath: "/repo/worktree/packages/app",
        },
        vcsDetect: (input) =>
          Effect.sync(() => {
            vcsCalls.push(input.cwd);
            return input.cwd;
          }).pipe(
            Effect.flatMap((cwd) =>
              cwd === "/repo/packages/app"
                ? Effect.fail(
                    new VcsProcessSpawnError({
                      operation: "VcsDriverRegistry.detect",
                      command: "git",
                      cwd,
                      cause: projectMissingCause,
                    }),
                  )
                : Effect.succeed(
                    makeGitHandle(cwd, {
                      rootPath: "/repo/worktree",
                      metadataPath: "/repo/.git",
                    }),
                  ),
            ),
          ),
      },
    );

    expect(result.isError).toBe(false);
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toMatchObject({
      projectCwd: "/repo/worktree/packages/app",
      baseBranch: "main",
      workspaceRelativePath: "packages/app",
    });
    expect(vcsCalls).toEqual([
      "/repo/worktree/packages/app",
      "/repo/packages/app",
      "/repo/worktree/packages/app",
    ]);
  }),
);

it.effect("resolves current-checkout branch from the project checkout after source fallback", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const gitCalls: string[] = [];
    const vcsCalls: string[] = [];
    const result = yield* callStartTool(
      { prompt: "Continue in project checkout", mode: "current_checkout" },
      commands,
      {
        project: {
          ...project,
          workspaceRoot: "/repo/project",
        },
        sourceThread: {
          ...sourceThread,
          branch: "feature/removed-worktree",
          worktreePath: "/repo/missing-worktree",
        },
        gitWorkflow: {
          status: (input) =>
            Effect.sync(() => {
              gitCalls.push(`status:${input.cwd}`);
              return {
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: input.cwd === "/repo/project",
                refName: input.cwd === "/repo/project" ? "main" : "feature/removed-worktree",
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
              };
            }),
        },
        vcsDetect: (input) =>
          Effect.sync(() => {
            vcsCalls.push(input.cwd);
            return input.cwd === "/repo/missing-worktree" ? null : makeGitHandle(input.cwd);
          }),
      },
    );

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      projectId,
      mode: "current_checkout",
      branch: "main",
      worktreePath: null,
    });
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.createThread?.branch).toBe("main");
    expect(command.bootstrap?.createThread?.worktreePath).toBeNull();
    expect(vcsCalls).toEqual(["/repo/missing-worktree", "/repo/project"]);
    expect(gitCalls).toEqual(["status:/repo/project"]);
  }),
);

it.effect(
  "resolves new-worktree fallback branch from the project checkout after source fallback",
  () =>
    Effect.gen(function* () {
      const commands: OrchestrationCommand[] = [];
      const gitCalls: string[] = [];
      const vcsCalls: string[] = [];
      const result = yield* callStartTool({ prompt: "Continue from project branch" }, commands, {
        project: {
          ...project,
          workspaceRoot: "/repo/project",
        },
        sourceThread: {
          ...sourceThread,
          branch: "feature/removed-worktree",
          worktreePath: "/repo/missing-worktree",
        },
        gitWorkflow: {
          listRefs: (input) =>
            Effect.sync(() => {
              gitCalls.push(`listRefs:${input.cwd}`);
              return {
                refs: [],
                isRepo: true,
                hasPrimaryRemote: input.cwd === "/repo/project",
                nextCursor: null,
                totalCount: 0,
              };
            }),
          status: (input) =>
            Effect.sync(() => {
              gitCalls.push(`status:${input.cwd}`);
              return {
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: input.cwd === "/repo/project",
                refName: input.cwd === "/repo/project" ? "main" : "feature/removed-worktree",
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
              };
            }),
        },
        vcsDetect: (input) =>
          Effect.sync(() => {
            vcsCalls.push(input.cwd);
            return input.cwd === "/repo/missing-worktree" ? null : makeGitHandle(input.cwd);
          }),
      });

      expect(result.isError).toBe(false);
      const command = commands[0];
      expect(command?.type).toBe("thread.turn.start");
      if (command?.type !== "thread.turn.start") return;
      expect(command.bootstrap?.prepareWorktree).toMatchObject({
        projectCwd: "/repo/project",
        baseBranch: "main",
      });
      expect(vcsCalls).toEqual(["/repo/missing-worktree", "/repo/project", "/repo/project"]);
      expect(gitCalls).toEqual(["listRefs:/repo/project", "status:/repo/project"]);
    }),
);

it.effect("falls back to the project checkout when the source worktree is a different repo", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const result = yield* callStartTool({ prompt: "Continue from another checkout" }, commands, {
      project: {
        ...project,
        workspaceRoot: "/repo/project",
      },
      sourceThread: {
        ...sourceThread,
        worktreePath: "/other/worktree",
      },
    });

    expect(result.isError).toBe(false);
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toMatchObject({
      projectCwd: "/repo/project",
      baseBranch: "main",
    });
  }),
);

it.effect("keeps the source worktree cwd when repository validation fails transiently", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const vcsCalls: string[] = [];
    const result = yield* callStartTool(
      { prompt: "Continue despite transient git failure", baseBranch: "main" },
      commands,
      {
        project: {
          ...project,
          workspaceRoot: "/repo/project",
        },
        sourceThread: {
          ...sourceThread,
          worktreePath: "/repo/worktree",
        },
        vcsDetect: (input) =>
          Effect.sync(() => {
            vcsCalls.push(input.cwd);
          }).pipe(
            Effect.flatMap(() =>
              Effect.fail(
                new VcsProcessSpawnError({
                  operation: "VcsDriverRegistry.detect",
                  command: "git",
                  cwd: input.cwd,
                  cause: new Error("transient repository detection failure"),
                }),
              ),
            ),
          ),
      },
    );

    expect(result.isError).toBe(false);
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toMatchObject({
      projectCwd: "/repo/worktree",
      baseBranch: "main",
    });
    expect(vcsCalls).toEqual(["/repo/worktree", "/repo/worktree", "/repo/worktree"]);
  }),
);

it.effect("preserves source package cwd when repository validation recovers after failure", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const vcsCalls: string[] = [];
    const result = yield* callStartTool(
      { prompt: "Continue despite transient source failure", baseBranch: "main" },
      commands,
      {
        project: {
          ...project,
          workspaceRoot: "/repo/project",
        },
        sourceThread: {
          ...sourceThread,
          worktreePath: "/repo/worktree/packages/app",
        },
        vcsDetect: (input) =>
          Effect.sync(() => {
            vcsCalls.push(input.cwd);
            return vcsCalls.length;
          }).pipe(
            Effect.flatMap((callCount) =>
              callCount === 1
                ? Effect.fail(
                    new VcsUnsupportedOperationError({
                      operation: "VcsDriverRegistry.detect",
                      kind: "git",
                      detail: "transient repository detection failure",
                    }),
                  )
                : Effect.succeed(
                    makeGitHandle(input.cwd, {
                      rootPath: "/repo/worktree",
                      metadataPath: "/repo/.git",
                    }),
                  ),
            ),
          ),
      },
    );

    expect(result.isError).toBe(false);
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toMatchObject({
      projectCwd: "/repo/worktree/packages/app",
      baseBranch: "main",
      workspaceRelativePath: "packages/app",
    });
    expect(vcsCalls).toEqual([
      "/repo/worktree/packages/app",
      "/repo/worktree/packages/app",
      "/repo/worktree/packages/app",
    ]);
  }),
);

it.effect("preserves source git worktrees under non-repo projects", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const result = yield* callStartTool(
      {
        prompt: "Spawn from project checkout",
        baseBranch: "main",
        baseBranchSource: "default",
      },
      commands,
      {
        project: {
          ...project,
          workspaceRoot: "/home/adam",
          repositoryIdentity: null,
        },
        sourceThread: {
          ...sourceThread,
          worktreePath: "/home/adam/wt-t20-worktree-fix",
        },
        vcsDetect: (input) =>
          Effect.succeed(input.cwd === "/home/adam" ? null : makeGitHandle(input.cwd)),
      },
    );

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      projectId,
      mode: "new_worktree",
      worktreePath: null,
    });
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toMatchObject({
      projectCwd: "/home/adam/wt-t20-worktree-fix",
      baseBranch: "main",
    });
    expect(command.bootstrap?.createThread?.worktreePath).toBeNull();
    expect(command.bootstrap?.createThread?.worktreeRemovable).toBe(true);
  }),
);

it.effect("re-detects before trusting a cached null project repository identity", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const vcsCalls: Array<{ readonly cwd: string; readonly cache?: string }> = [];
    const result = yield* callStartTool(
      {
        prompt: "Spawn after git init",
        baseBranch: "main",
      },
      commands,
      {
        project: {
          ...project,
          workspaceRoot: "/repo",
          repositoryIdentity: null,
        },
        vcsDetect: (input) =>
          Effect.sync(() => {
            vcsCalls.push(input);
            return makeGitHandle(input.cwd);
          }),
      },
    );

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      projectId,
      mode: "new_worktree",
      worktreePath: null,
    });
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toMatchObject({
      projectCwd: "/repo",
      baseBranch: "main",
    });
    expect(vcsCalls).toContainEqual({ cwd: "/repo", cache: "bypass" });
  }),
);

it.effect(
  "degrades to the project checkout when a source worktree is outside a non-repo project",
  () =>
    Effect.gen(function* () {
      const commands: OrchestrationCommand[] = [];
      const result = yield* callStartTool(
        {
          prompt: "Spawn from outside checkout",
          baseBranch: "main",
        },
        commands,
        {
          project: {
            ...project,
            workspaceRoot: "/home/adam",
            repositoryIdentity: null,
          },
          sourceThread: {
            ...sourceThread,
            worktreePath: "/tmp/other-worktree",
          },
          vcsDetect: (input) =>
            Effect.succeed(input.cwd === "/home/adam" ? null : makeGitHandle(input.cwd)),
        },
      );

      expect(result.isError).toBe(false);
      expect(result.structuredContent).toMatchObject({
        projectId,
        mode: "current_checkout",
        worktreePath: null,
      });
      expect(result.structuredContent).toHaveProperty("warning");
      const command = commands[0];
      expect(command?.type).toBe("thread.turn.start");
      if (command?.type !== "thread.turn.start") return;
      expect(command.bootstrap?.prepareWorktree).toBeUndefined();
      expect(command.bootstrap?.createThread?.worktreePath).toBeNull();
      expect(command.bootstrap?.createThread?.worktreeRemovable).toBe(false);
    }),
);

it.effect("accepts same-repo source worktrees when project detection uses relative metadata", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const result = yield* callStartTool(
      {
        prompt: "Spawn from monorepo package checkout",
        baseBranch: "main",
      },
      commands,
      {
        project: {
          ...project,
          workspaceRoot: "/repo/packages/app",
        },
        sourceThread: {
          ...sourceThread,
          worktreePath: "/repo-worktree/packages/app",
        },
        vcsDetect: (input) => {
          if (input.cwd === "/repo/packages/app") {
            return Effect.succeed(
              makeGitHandle(input.cwd, {
                rootPath: "/repo",
                metadataPath: "../../.git",
              }),
            );
          }
          return Effect.succeed(
            makeGitHandle(input.cwd, {
              rootPath: "/repo-worktree",
              metadataPath: "/repo/.git",
            }),
          );
        },
      },
    );

    expect(result.isError).toBe(false);
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    expect(command.bootstrap?.prepareWorktree).toMatchObject({
      projectCwd: "/repo-worktree/packages/app",
      baseBranch: "main",
      workspaceRelativePath: "packages/app",
    });
  }),
);

it.effect("applies directive default effort when resolving a plain model", () =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const result = yield* callStartTool(
      { prompt: "Investigate flaky tests", model: "claude-opus-4-8" },
      commands,
      {
        providerInstances: [
          makeModelInstance("claudeAgent", "claudeAgent", [
            { slug: "claude-opus-4-8", optionId: "effort", value: "high" },
          ]),
        ],
      },
    );

    expect(result.isError).toBe(false);
    const command = commands[0];
    expect(command?.type).toBe("thread.turn.start");
    if (command?.type !== "thread.turn.start") return;
    const expectedSelection = {
      instanceId: "claudeAgent",
      model: "claude-opus-4-8",
      options: [{ id: "effort", value: "xhigh" }],
    };
    expect(command.modelSelection).toEqual(expectedSelection);
    expect(command.bootstrap?.createThread?.modelSelection).toEqual(expectedSelection);
  }),
);
