// @effect-diagnostics globalFetch:off globalFetchInEffect:off - This conformance test intentionally drives the HTTP boundary through the official MCP SDK transport and raw protocol probes.
import { NodeHttpServer } from "@effect/platform-node";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { CallToolResultSchema, ListToolsResultSchema } from "@modelcontextprotocol/sdk/types.js";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  type ExecutionEnvironmentDescriptor,
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
import * as Stream from "effect/Stream";
import { FetchHttpClient, HttpRouter, HttpServer } from "effect/unstable/http";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as DeviceNotifications from "../notifications/DeviceNotifications.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderRegistry } from "../provider/Services/ProviderRegistry.ts";
import { makeProviderRegistryMock } from "../provider/testUtils/providerRegistryMock.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as SubagentPeerRegistry from "../subagents/SubagentPeerRegistry.ts";
import * as PlanUsageSnapshot from "../usage/PlanUsageSnapshot.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";

type SdkTransport = Parameters<Client["connect"]>[0];
type ParsedCallToolResult = {
  readonly isError?: boolean;
  readonly structuredContent?: unknown;
  readonly content: ReadonlyArray<{ readonly type: string }>;
};

const bearerToken = "mcp-conformance-token";
const authorizationHeader = `Bearer ${bearerToken}`;
const timestamp = "2026-07-09T00:00:00.000Z";
const environmentId = EnvironmentId.make("environment-mcp-conformance");
const threadId = ThreadId.make("thread-mcp-conformance");
const providerInstanceId = ProviderInstanceId.make("codex");

const expectedToolNamesAfterSpawnTrim = [
  "t3_archive_thread",
  "t3_get_usage",
  "t3_list_backends",
  "t3_notify",
  "t3_schedule_create",
  "t3_schedule_delete",
  "t3_schedule_list",
  "t3_schedule_update",
  "t3_spawn_subagent",
  "t3_steer_subagent",
  "t3_subagents",
  "t3_thread_start",
] as const;

type ExpectedToolName = (typeof expectedToolNamesAfterSpawnTrim)[number];

const expectedToolInputShapes: Record<
  ExpectedToolName,
  { readonly properties: ReadonlyArray<string>; readonly required: ReadonlyArray<string> }
> = {
  t3_archive_thread: { properties: ["threadId"], required: ["threadId"] },
  t3_get_usage: { properties: ["providerInstanceId"], required: [] },
  t3_list_backends: { properties: [], required: [] },
  t3_notify: { properties: ["body", "deepLink", "title"], required: ["title"] },
  t3_schedule_create: {
    properties: ["cronExpr", "intervalSeconds", "model", "prompt", "threadId", "timezone"],
    required: ["prompt"],
  },
  t3_schedule_delete: { properties: ["taskId"], required: ["taskId"] },
  t3_schedule_list: { properties: ["threadId"], required: [] },
  t3_schedule_update: {
    properties: ["cronExpr", "enabled", "intervalSeconds", "model", "taskId"],
    required: ["taskId"],
  },
  t3_spawn_subagent: {
    properties: ["branch", "directory", "model", "prompt", "reasoningEffort", "title"],
    required: ["model", "prompt", "title"],
  },
  t3_steer_subagent: {
    properties: ["childThreadId", "message"],
    required: ["childThreadId", "message"],
  },
  t3_subagents: { properties: ["childThreadId"], required: [] },
  t3_thread_start: {
    properties: ["branch", "directory", "model", "prompt", "reasoningEffort", "title"],
    required: ["model", "prompt", "title"],
  },
};

const removedSubagentToolNames = [
  "t3_check_subagent",
  "t3_list_subagents",
  "t3_wait_subagent",
] as const;

const removedPreviewToolNames = [
  "preview_click",
  "preview_evaluate",
  "preview_navigate",
  "preview_open",
  "preview_press",
  "preview_recording_start",
  "preview_recording_stop",
  "preview_resize",
  "preview_scroll",
  "preview_snapshot",
  "preview_status",
  "preview_type",
  "preview_wait_for",
] as const;

const invocation: McpInvocationContext.ProviderMcpInvocationScope = {
  credentialKind: "provider-session",
  environmentId,
  threadId,
  providerSessionId: "provider-session-mcp-conformance",
  providerInstanceId,
  capabilities: new Set([
    "thread-management",
    "notification",
    "subagent:spawn",
    "subagent:check",
    "subagent:wait",
    "subagent:list",
  ]),
  issuedAt: 1,
  expiresAt: Number.MAX_SAFE_INTEGER,
};

const makeProvider = (): ServerProvider =>
  ({
    instanceId: providerInstanceId,
    driver: ProviderDriverKind.make("codex"),
    displayName: "Codex",
    enabled: true,
    installed: true,
    version: "1.0.0",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: timestamp,
    availability: "available",
    models: [
      {
        slug: "gpt-5-codex",
        name: "GPT-5 Codex",
        isCustom: false,
        capabilities: null,
      },
    ],
    slashCommands: [],
    skills: [],
  }) as ServerProvider;

const project: OrchestrationProjectShell = {
  id: ProjectId.make("project-mcp-conformance"),
  title: "MCP Conformance",
  workspaceRoot: "/tmp/t3-mcp-conformance",
  dataAudience: "private",
  defaultModelSelection: {
    instanceId: providerInstanceId,
    model: "gpt-5-codex",
  },
  scripts: [],
  createdAt: timestamp,
  updatedAt: timestamp,
};

const mcpSessionRegistryLayer = Layer.succeed(
  McpSessionRegistry.McpSessionRegistry,
  McpSessionRegistry.McpSessionRegistry.of({
    issue: () => Effect.die("unused"),
    issuePeerToken: () => Effect.die("unused"),
    resolve: (rawToken) => Effect.succeed(rawToken === bearerToken ? invocation : undefined),
    touchThread: () => Effect.void,
    revokeProviderSession: () => Effect.void,
    revokeThread: () => Effect.void,
    revokePeerTokensBySourceSession: () => Effect.void,
    revokePeerToken: () => Effect.void,
    revokePeerTokensExceptSourceSession: () => Effect.void,
    revokePeerTokensNotInSourceSessions: () => Effect.void,
    revokeAllProviderSessions: Effect.void,
    revokeAll: Effect.void,
  }),
);

const projectionSnapshotQueryLayer = Layer.mock(ProjectionSnapshotQuery)({
  getShellSnapshot: () =>
    Effect.succeed({
      snapshotSequence: 1,
      projects: [project],
      threads: [],
      updatedAt: timestamp,
    }),
  getProjectShellById: () => Effect.succeed(Option.none()),
  getThreadShellById: () => Effect.succeed(Option.none()),
  getThreadShellByIdIncludingArchived: () => Effect.succeed(Option.none()),
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
});

const deviceNotificationsLayer = Layer.mock(DeviceNotifications.DeviceNotifications)({
  getConfig: Effect.die("unused"),
  registerDevice: () => Effect.die("unused"),
  recoverSubscription: () => Effect.die("unused"),
  ackNotification: () => Effect.die("unused"),
  notify: () => Effect.die("unused"),
  events: Stream.empty,
});

const conformanceEnvironmentDescriptor: ExecutionEnvironmentDescriptor = {
  environmentId,
  label: "MCP Conformance",
  platform: { os: "linux", arch: "x64" },
  serverVersion: "0.0.0-test",
  capabilities: { repositoryIdentity: true },
};

const serverEnvironmentLayer = Layer.succeed(ServerEnvironment.ServerEnvironment, {
  getEnvironmentId: Effect.succeed(environmentId),
  getDescriptor: Effect.succeed(conformanceEnvironmentDescriptor),
});

const subagentPeerRegistryLayer = Layer.succeed(SubagentPeerRegistry.SubagentPeerRegistry, {
  add: () => Effect.die("unused"),
  list: Effect.succeed([]),
  remove: () => Effect.die("unused"),
  getByAlias: () => Effect.succeed(Option.none()),
  resolveTarget: () => Effect.die("unused"),
  updateLastSeen: () => Effect.succeed(Option.none()),
});

const conformanceLayer = McpHttpServer.layer.pipe(
  Layer.provide(mcpSessionRegistryLayer),
  Layer.provide(deviceNotificationsLayer),
  Layer.provide(ServerSettingsService.layerTest()),
  Layer.provide(
    PlanUsageSnapshot.layerTest({
      updatedAt: "2026-07-10T08:00:00.000Z",
      providers: [],
    }),
  ),
  Layer.provide(Layer.succeed(ProviderRegistry, makeProviderRegistryMock([makeProvider()]))),
  Layer.provide(serverEnvironmentLayer),
  Layer.provide(subagentPeerRegistryLayer),
  Layer.provide(FetchHttpClient.layer),
  Layer.provide(projectionSnapshotQueryLayer),
);

const mcpUrlFromServer = (address: HttpServer.Address): URL => {
  if (address._tag !== "TcpAddress") {
    throw new Error(`Expected a TCP test server address, got ${address._tag}.`);
  }
  const host = address.hostname === "0.0.0.0" ? "127.0.0.1" : address.hostname;
  return new URL(`http://${host}:${address.port}/mcp`);
};

const parseJsonRpcHttpBody = async (response: Response): Promise<unknown> => {
  const text = await response.text();
  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("text/event-stream")) {
    const data = text
      .split(/\r?\n/)
      .map((line) => line.trim())
      .find((line) => line.startsWith("data:"))
      ?.slice("data:".length)
      .trim();
    if (!data) throw new Error(`SSE response did not contain a data frame: ${text}`);
    return JSON.parse(data);
  }
  return JSON.parse(text);
};

const postJsonRpc = (url: URL, body: unknown, sessionId?: string, protocolVersion?: string) =>
  fetch(url, {
    method: "POST",
    headers: {
      accept: "application/json, text/event-stream",
      authorization: authorizationHeader,
      "content-type": "application/json",
      ...(sessionId ? { "mcp-session-id": sessionId } : {}),
      ...(protocolVersion ? { "mcp-protocol-version": protocolVersion } : {}),
    },
    body: typeof body === "string" ? body : JSON.stringify(body),
  });

const assertNoNullableInputSchema = (value: unknown, path: string): void => {
  if (Array.isArray(value)) {
    value.forEach((item, index) => assertNoNullableInputSchema(item, `${path}[${index}]`));
    return;
  }
  if (typeof value !== "object" || value === null) return;

  const record = value as Readonly<Record<string, unknown>>;
  expect(record.type, `${path} must not accept null`).not.toBe("null");
  for (const [key, child] of Object.entries(record)) {
    assertNoNullableInputSchema(child, `${path}.${key}`);
  }
};

it.effect("conforms to the official streamable HTTP MCP client", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* HttpRouter.serve(conformanceLayer, {
        disableListenLog: true,
        disableLogger: true,
      }).pipe(Layer.build);
      const httpServer = yield* HttpServer.HttpServer;
      const mcpUrl = mcpUrlFromServer(httpServer.address);

      const client = new Client(
        { name: "t3-mcp-conformance", version: "1.0.0" },
        { capabilities: {} },
      );
      const transport = new StreamableHTTPClientTransport(mcpUrl, {
        requestInit: {
          headers: { authorization: authorizationHeader },
        },
      });

      yield* Effect.addFinalizer(() =>
        Effect.promise(async () => {
          await transport.terminateSession().catch(() => undefined);
          await client.close().catch(() => undefined);
        }),
      );

      yield* Effect.promise(() => client.connect(transport as SdkTransport));
      expect(client.getServerVersion()).toMatchObject({ name: "T3 Code" });

      const getResponse = yield* Effect.promise(() =>
        fetch(mcpUrl, {
          method: "GET",
          headers: {
            accept: "text/event-stream",
            authorization: authorizationHeader,
          },
        }),
      );
      expect(getResponse.status).toBe(405);
      expect(getResponse.headers.get("allow")).toBe("POST, DELETE");
      yield* Effect.promise(() => getResponse.body?.cancel() ?? Promise.resolve());

      const toolsResult = yield* Effect.promise(() => client.listTools());
      ListToolsResultSchema.parse(toolsResult);
      const toolNames = new Set(toolsResult.tools.map((tool) => tool.name));
      expect([...toolNames].toSorted()).toEqual([...expectedToolNamesAfterSpawnTrim]);
      for (const removedName of removedPreviewToolNames) {
        expect(toolNames.has(removedName), `${removedName} must be absent from tools/list`).toBe(
          false,
        );
      }
      for (const removedName of removedSubagentToolNames) {
        expect(toolNames.has(removedName), `${removedName} must be absent from tools/list`).toBe(
          false,
        );
      }

      for (const tool of toolsResult.tools) {
        const expected = expectedToolInputShapes[tool.name as ExpectedToolName];
        expect(expected, `${tool.name} must have a pinned input shape`).toBeDefined();
        expect(Object.keys(tool.inputSchema.properties ?? {}).toSorted()).toEqual(
          expected?.properties,
        );
        expect([...(tool.inputSchema.required ?? [])].toSorted()).toEqual(expected?.required);
        assertNoNullableInputSchema(tool.inputSchema, `${tool.name}.inputSchema`);

        expect(tool.inputSchema.type, `${tool.name} input schema must be an object`).toBe("object");
        expect(tool.inputSchema.anyOf, `${tool.name} input schema must not root anyOf`).toBe(
          undefined,
        );
        expect(tool.inputSchema.oneOf, `${tool.name} input schema must not root oneOf`).toBe(
          undefined,
        );
        expect(tool.inputSchema.allOf, `${tool.name} input schema must not root allOf`).toBe(
          undefined,
        );
        if (tool.outputSchema) {
          expect(tool.outputSchema.type, `${tool.name} output schema must be an object`).toBe(
            "object",
          );
          expect(tool.outputSchema.anyOf, `${tool.name} output schema must not root anyOf`).toBe(
            undefined,
          );
          expect(tool.outputSchema.oneOf, `${tool.name} output schema must not root oneOf`).toBe(
            undefined,
          );
          expect(tool.outputSchema.allOf, `${tool.name} output schema must not root allOf`).toBe(
            undefined,
          );
        }
      }

      const listBackendsTool = toolsResult.tools.find(
        (candidate) => candidate.name === "t3_list_backends",
      );
      expect(listBackendsTool?.annotations).toMatchObject({
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: false,
        openWorldHint: true,
      });

      const callResult = CallToolResultSchema.parse(
        yield* Effect.promise(() =>
          client.callTool({ name: "t3_list_backends", arguments: {} }, CallToolResultSchema),
        ),
      ) as ParsedCallToolResult;
      expect(callResult.isError).toBe(false);
      expect(callResult.structuredContent).toBeDefined();
      expect(callResult.structuredContent).not.toBeNull();
      expect(callResult.structuredContent).toMatchObject({
        backends: [
          {
            alias: "local",
            environmentId: "environment-mcp-conformance",
            os: "linux",
            status: "online",
            providers: [{ instanceId: "codex", driver: "codex" }],
          },
        ],
      });
      expect(callResult.content.length).toBeGreaterThan(0);
      for (const block of callResult.content) {
        expect(block.type).toMatch(/^(text|image|audio|resource|resource_link)$/);
      }

      const sessionId = transport.sessionId;
      expect(sessionId).toEqual(expect.any(String));

      const unknownMethodResponse = yield* Effect.promise(() =>
        postJsonRpc(
          mcpUrl,
          {
            jsonrpc: "2.0",
            id: 9_001,
            method: "t3/unknown_method",
            params: {},
          },
          sessionId,
          transport.protocolVersion,
        ),
      );
      const unknownMethodDebugBody = yield* Effect.promise(() =>
        unknownMethodResponse.clone().text(),
      );
      expect(unknownMethodResponse.status, unknownMethodDebugBody).toBeLessThan(500);
      const unknownMethodBody = (yield* Effect.promise(() =>
        parseJsonRpcHttpBody(unknownMethodResponse),
      )) as { readonly jsonrpc?: unknown; readonly id?: unknown; readonly error?: unknown };
      expect(unknownMethodBody).toMatchObject({
        jsonrpc: "2.0",
        error: { code: expect.any(Number), message: expect.any(String) },
      });
      expect(unknownMethodBody.id).toBeDefined();

      const audienceAdminToolResponse = yield* Effect.promise(() =>
        postJsonRpc(
          mcpUrl,
          {
            jsonrpc: "2.0",
            id: 9_002,
            method: "tools/call",
            params: {
              name: "t3_set_audience_to_factory",
              arguments: { projectId: "project-mcp-conformance" },
            },
          },
          sessionId,
          transport.protocolVersion,
        ),
      );
      const audienceAdminToolBody = (yield* Effect.promise(() =>
        parseJsonRpcHttpBody(audienceAdminToolResponse),
      )) as { readonly error?: unknown };
      expect(audienceAdminToolBody).toMatchObject({
        error: { code: expect.any(Number), message: expect.any(String) },
      });

      const malformedResponse = yield* Effect.promise(() =>
        postJsonRpc(
          mcpUrl,
          '{"jsonrpc":"2.0","id":"malformed"',
          sessionId,
          transport.protocolVersion,
        ),
      );
      const malformedDebugBody = yield* Effect.promise(() => malformedResponse.clone().text());
      expect(malformedResponse.status, malformedDebugBody).toBeLessThan(500);
      const malformedBody = (yield* Effect.promise(() =>
        parseJsonRpcHttpBody(malformedResponse),
      )) as { readonly jsonrpc?: unknown; readonly error?: unknown };
      expect(malformedBody).toMatchObject({
        jsonrpc: "2.0",
        error: { code: expect.any(Number), message: expect.any(String) },
      });

      yield* Effect.promise(() => client.ping());
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
