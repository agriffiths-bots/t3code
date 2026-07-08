import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationProjectShell,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { McpSchema, McpServer } from "effect/unstable/ai";

import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderRegistry } from "../../../provider/Services/ProviderRegistry.ts";
import { makeProviderRegistryMock } from "../../../provider/testUtils/providerRegistryMock.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { VisibilityToolkitRegistrationLive } from "../../McpHttpServer.ts";

const client = McpSchema.McpServerClient.of({
  clientId: 1,
  initializePayload: {
    protocolVersion: "2025-03-26",
    capabilities: {},
    clientInfo: { name: "visibility-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

const timestamp = "2026-07-07T00:00:00.000Z";
const invocation: McpInvocationContext.ProviderMcpInvocationScope = {
  credentialKind: "provider-session",
  environmentId: EnvironmentId.make("environment-visibility-test"),
  threadId: ThreadId.make("thread-visibility-test"),
  providerSessionId: "provider-session-visibility-test",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(["thread-management"]),
  issuedAt: 1,
  expiresAt: Number.MAX_SAFE_INTEGER,
};

const peerInvocation: McpInvocationContext.PeerMcpInvocationScope = {
  credentialKind: "peer",
  environmentId: EnvironmentId.make("environment-visibility-test"),
  peerTokenId: "peer-visibility-test",
  capabilities: new Set(["subagent:list"]),
  issuedAt: 1,
  expiresAt: null,
};

const makeProvider = (input: {
  readonly instanceId: string;
  readonly driver: string;
  readonly displayName?: string;
  readonly models?: ReadonlyArray<string>;
  readonly availability?: "available" | "unavailable";
  readonly enabled?: boolean;
  readonly installed?: boolean;
  readonly status?: ServerProvider["status"];
}): ServerProvider =>
  ({
    instanceId: ProviderInstanceId.make(input.instanceId),
    driver: ProviderDriverKind.make(input.driver),
    ...(input.displayName !== undefined ? { displayName: input.displayName } : {}),
    enabled: input.enabled ?? true,
    installed: input.installed ?? true,
    version: null,
    status: input.status ?? "ready",
    auth: { status: "authenticated" },
    checkedAt: timestamp,
    availability: input.availability ?? "available",
    models: (input.models ?? []).map((slug) => ({
      slug,
      name: slug,
      isCustom: false,
      capabilities: null,
    })),
    slashCommands: [],
    skills: [],
  }) as ServerProvider;

const makeProject = (input: {
  readonly id: string;
  readonly title: string;
  readonly workspaceRoot: string;
  readonly instanceId?: string;
  readonly model?: string;
}): OrchestrationProjectShell => ({
  id: ProjectId.make(input.id),
  title: input.title,
  workspaceRoot: input.workspaceRoot,
  defaultModelSelection:
    input.instanceId === undefined
      ? null
      : {
          instanceId: ProviderInstanceId.make(input.instanceId),
          model: input.model ?? "gpt-5",
        },
  scripts: [],
  createdAt: timestamp,
  updatedAt: timestamp,
});

const makeLayer = (input: {
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly projects: ReadonlyArray<OrchestrationProjectShell>;
}) =>
  VisibilityToolkitRegistrationLive.pipe(
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provide(Layer.succeed(ProviderRegistry, makeProviderRegistryMock(input.providers))),
    Layer.provide(
      Layer.mock(ProjectionSnapshotQuery)({
        getShellSnapshot: () =>
          Effect.succeed({
            snapshotSequence: 1,
            projects: input.projects,
            threads: [],
            updatedAt: timestamp,
          }),
        getProjectShellById: () => Effect.succeed(Option.none()),
        getThreadShellById: () => Effect.succeed(Option.none()),
        getSnapshot: () => Effect.die("unused"),
        getCommandReadModel: () => Effect.die("unused"),
        getArchivedShellSnapshot: () => Effect.die("unused"),
        getSnapshotSequence: () => Effect.die("unused"),
        getCounts: () => Effect.die("unused"),
        getActiveProjectByWorkspaceRoot: () => Effect.die("unused"),
        getFirstActiveThreadIdByProjectId: () => Effect.die("unused"),
        getThreadCheckpointContext: () => Effect.die("unused"),
        getFullThreadDiffContext: () => Effect.die("unused"),
        getThreadDetailById: () => Effect.die("unused"),
        getThreadDetailSnapshot: () => Effect.die("unused"),
      }),
    ),
  );

const callListBackends = (
  input: {
    readonly providers: ReadonlyArray<ServerProvider>;
    readonly projects: ReadonlyArray<OrchestrationProjectShell>;
  },
  scope: McpInvocationContext.McpInvocationScope = invocation,
) =>
  Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    return yield* server
      .callTool({ name: "t3_list_backends", arguments: {} })
      .pipe(
        Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
        Effect.provideService(McpSchema.McpServerClient, client),
      );
  }).pipe(Effect.provide(makeLayer(input)));

it.effect("lists configured backends with exact instance ids and grouped projects", () =>
  Effect.gen(function* () {
    const result = yield* callListBackends({
      providers: [
        makeProvider({
          instanceId: "codex_work",
          driver: "codex",
          displayName: "Codex Work",
          models: ["gpt-5.5", "gpt-5.4"],
        }),
        makeProvider({ instanceId: "claudeAgent", driver: "claudeAgent", models: ["opus-4.8"] }),
      ],
      projects: [
        makeProject({
          id: "project-codex",
          title: "Codex Project",
          workspaceRoot: "/repo/codex",
          instanceId: "codex_work",
          model: "gpt-5.5",
        }),
        makeProject({
          id: "project-claude",
          title: "Claude Project",
          workspaceRoot: "/repo/claude",
          instanceId: "claudeAgent",
          model: "opus-4.8",
        }),
      ],
    });

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      backends: [
        {
          instanceId: "codex_work",
          driver: "codex",
          label: "Codex Work",
          displayName: "Codex Work",
          available: true,
          models: [
            { slug: "gpt-5.5", name: "gpt-5.5" },
            { slug: "gpt-5.4", name: "gpt-5.4" },
          ],
          projects: [{ id: "project-codex", title: "Codex Project" }],
        },
        {
          instanceId: "claudeAgent",
          driver: "claudeAgent",
          label: "claudeAgent",
          available: true,
          projects: [{ id: "project-claude", title: "Claude Project" }],
        },
      ],
      unassignedProjects: [],
      unknownBackendProjects: [],
    });
  }),
);

it.effect("surfaces unassigned projects and projects that reference missing backend ids", () =>
  Effect.gen(function* () {
    const result = yield* callListBackends({
      providers: [
        makeProvider({
          instanceId: "codex",
          driver: "codex",
          models: ["gpt-5.5"],
          availability: "unavailable",
        }),
        makeProvider({
          instanceId: "claudeAgent",
          driver: "claudeAgent",
          models: ["opus-4.8"],
          status: "error",
        }),
      ],
      projects: [
        makeProject({
          id: "project-unassigned",
          title: "Unassigned",
          workspaceRoot: "/repo/unassigned",
        }),
        makeProject({
          id: "project-missing",
          title: "Missing",
          workspaceRoot: "/repo/missing",
          instanceId: "missing_backend",
        }),
      ],
    });

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      backends: [
        {
          instanceId: "codex",
          label: "codex",
          availability: "unavailable",
          available: false,
          projects: [],
        },
        {
          instanceId: "claudeAgent",
          label: "claudeAgent",
          status: "error",
          available: false,
          projects: [],
        },
      ],
      unassignedProjects: [{ id: "project-unassigned", title: "Unassigned" }],
      unknownBackendProjects: [
        {
          id: "project-missing",
          title: "Missing",
          requestedInstanceId: "missing_backend",
        },
      ],
    });
  }),
);

it.effect("rejects peer-scoped credentials", () =>
  Effect.gen(function* () {
    const result = yield* callListBackends(
      {
        providers: [],
        projects: [],
      },
      peerInvocation,
    );

    expect(result.isError).toBe(true);
    const content = result.content?.[0];
    expect(content?.type).toBe("text");
    if (content?.type !== "text") throw new Error("Expected text error content.");
    expect(content.text).toContain(
      "MCP credential does not grant the thread-management capability",
    );
  }),
);
