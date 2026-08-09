import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ExecutionEnvironmentDescriptor,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { McpSchema, McpServer } from "effect/unstable/ai";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import * as ServerEnvironment from "../../../environment/ServerEnvironment.ts";
import { ProviderRegistry } from "../../../provider/Services/ProviderRegistry.ts";
import { makeProviderRegistryMock } from "../../../provider/testUtils/providerRegistryMock.ts";
import * as SubagentPeerRegistry from "../../../subagents/SubagentPeerRegistry.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { VisibilityToolkitRegistrationLive } from "../../McpHttpServer.ts";

const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-03-26",
    capabilities: {},
    clientInfo: { name: "visibility-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

const timestamp = "2026-07-07T00:00:00.000Z";
const localEnvironmentId = EnvironmentId.make("environment-visibility-local");
const invocation: McpInvocationContext.ProviderMcpInvocationScope = {
  credentialKind: "provider-session",
  environmentId: localEnvironmentId,
  threadId: ThreadId.make("thread-visibility-test"),
  providerSessionId: "provider-session-visibility-test",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(["thread-management"]),
  issuedAt: 1,
  expiresAt: Number.MAX_SAFE_INTEGER,
};

const peerInvocation: McpInvocationContext.PeerMcpInvocationScope = {
  credentialKind: "peer",
  environmentId: localEnvironmentId,
  peerTokenId: "peer-visibility-test",
  capabilities: new Set(["subagent:list"]),
  issuedAt: 1,
  expiresAt: null,
};

const localDescriptor: ExecutionEnvironmentDescriptor = {
  environmentId: localEnvironmentId,
  label: "WSL",
  platform: { os: "linux", arch: "x64" },
  serverVersion: "0.0.0-test",
  capabilities: { repositoryIdentity: true },
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

const windowsPeer = (): SubagentPeerRegistry.SubagentPeer => ({
  alias: "windows",
  environmentId: EnvironmentId.make("environment-visibility-windows"),
  httpBaseUrl: "https://windows.example/",
  mcpEndpoint: "https://windows.example/mcp",
  credential: new SubagentPeerRegistry.SubagentPeerBearerCredential({ token: "peer-token" }),
  pairedAt: "2026-07-06T00:00:00.000Z",
  lastSeenAt: "2026-07-06T12:00:00.000Z",
});

type TestHttpHandler = (
  request: HttpClientRequest.HttpClientRequest,
) => Effect.Effect<HttpClientResponse.HttpClientResponse>;

const decodeRequest = (request: HttpClientRequest.HttpClientRequest) => {
  const raw = (request.body as { readonly body?: Uint8Array }).body;
  if (raw === undefined) throw new Error("Expected request body.");
  return JSON.parse(new TextDecoder().decode(raw)) as {
    readonly id?: number;
    readonly method: string;
    readonly params?: { readonly name?: string };
  };
};

const jsonRpcResult = (id: number, result: unknown) => ({ jsonrpc: "2.0", id, result });

const remoteInventoryHandler =
  (calledTools: string[], environmentId = "environment-visibility-windows"): TestHttpHandler =>
  (request) => {
    if (request.method === "DELETE") {
      return Effect.succeed(
        HttpClientResponse.fromWeb(request, new Response(null, { status: 204 })),
      );
    }
    if (request.method === "GET") {
      return Effect.succeed(
        HttpClientResponse.fromWeb(request, Response.json({ error: "unexpected descriptor call" })),
      );
    }
    return Effect.sync(() => {
      const body = decodeRequest(request);
      if (body.method === "initialize") {
        return HttpClientResponse.fromWeb(
          request,
          Response.json(
            jsonRpcResult(body.id ?? 1, {
              protocolVersion: "2025-06-18",
              capabilities: { tools: {} },
              serverInfo: { name: "peer", version: "0.0.0-test" },
            }),
            { headers: { "mcp-session-id": "visibility-peer-session" } },
          ),
        );
      }
      if (body.method === "notifications/initialized") {
        return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
      }
      if (body.method === "tools/call") {
        calledTools.push(body.params?.name ?? "");
        return HttpClientResponse.fromWeb(
          request,
          Response.json(
            jsonRpcResult(body.id ?? 2, {
              content: [{ type: "text", text: "listed" }],
              structuredContent: {
                backends: [
                  {
                    alias: "local",
                    environmentId,
                    label: "Windows Workstation",
                    os: "windows",
                    status: "online",
                    providers: [
                      {
                        instanceId: "claudeAgent",
                        driver: "claudeAgent",
                        label: "Claude",
                        displayName: "Claude",
                        enabled: true,
                        installed: true,
                        status: "ready",
                        availability: "available",
                        available: true,
                        models: [{ slug: "opus-4.8", name: "Opus 4.8" }],
                      },
                    ],
                  },
                ],
              },
              isError: false,
            }),
          ),
        );
      }
      throw new Error(`Unexpected method ${body.method}`);
    });
  };

const failedInventoryWithDescriptorHandler: TestHttpHandler = (request) => {
  if (request.method === "GET") {
    return Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        Response.json({
          environmentId: "environment-visibility-windows",
          label: "Windows Workstation",
          platform: { os: "windows", arch: "x64" },
          serverVersion: "0.0.0-test",
          capabilities: { repositoryIdentity: true },
        }),
      ),
    );
  }
  return Effect.succeed(
    HttpClientResponse.fromWeb(request, Response.json({ error: "offline" }, { status: 503 })),
  );
};

const makeLayer = (input: {
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly peers?: ReadonlyArray<SubagentPeerRegistry.SubagentPeer>;
  readonly httpHandler?: TestHttpHandler;
  readonly seenAliases?: string[];
}) => {
  const peers = input.peers ?? [];
  return VisibilityToolkitRegistrationLive.pipe(
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provide(Layer.succeed(ProviderRegistry, makeProviderRegistryMock(input.providers))),
    Layer.provide(
      Layer.succeed(ServerEnvironment.ServerEnvironment, {
        getEnvironmentId: Effect.succeed(localEnvironmentId),
        getDescriptor: Effect.succeed(localDescriptor),
      }),
    ),
    Layer.provide(
      Layer.succeed(SubagentPeerRegistry.SubagentPeerRegistry, {
        add: () => Effect.die("unused"),
        list: Effect.succeed(peers),
        remove: () => Effect.die("unused"),
        getByAlias: () => Effect.succeed(Option.none()),
        resolveTarget: () => Effect.die("unused"),
        updateLastSeen: (alias) =>
          Effect.sync(() => {
            input.seenAliases?.push(alias);
            return Option.fromUndefinedOr(peers.find((peer) => peer.alias === alias));
          }),
      }),
    ),
    Layer.provide(
      Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make(input.httpHandler ?? (() => Effect.die("unexpected visibility HTTP call"))),
      ),
    ),
  );
};

const callListBackends = (
  layer: ReturnType<typeof makeLayer>,
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
  }).pipe(Effect.provide(layer));

it.effect("lists execution backends with providers and models nested", () =>
  Effect.gen(function* () {
    const calledTools: string[] = [];
    const seenAliases: string[] = [];
    const result = yield* callListBackends(
      makeLayer({
        providers: [
          makeProvider({
            instanceId: "codex",
            driver: "codex",
            displayName: "Codex",
            models: ["gpt-5.5", "gpt-5.4"],
          }),
        ],
        peers: [windowsPeer()],
        httpHandler: remoteInventoryHandler(calledTools),
        seenAliases,
      }),
    );

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      backends: [
        {
          alias: "local",
          environmentId: "environment-visibility-local",
          label: "WSL",
          os: "linux",
          status: "online",
          providers: [
            {
              instanceId: "codex",
              driver: "codex",
              models: [
                { slug: "gpt-5.5", name: "gpt-5.5" },
                { slug: "gpt-5.4", name: "gpt-5.4" },
              ],
            },
          ],
        },
        {
          alias: "windows",
          environmentId: "environment-visibility-windows",
          os: "windows",
          status: "online",
          providers: [
            {
              instanceId: "claudeAgent",
              models: [{ slug: "opus-4.8", name: "Opus 4.8" }],
            },
          ],
        },
      ],
    });
    expect(calledTools).toEqual(["t3_list_backends"]);
    expect(seenAliases).toEqual(["windows"]);
  }),
);

it.effect("keeps failed configured peers visible with descriptor OS and no providers", () =>
  Effect.gen(function* () {
    const result = yield* callListBackends(
      makeLayer({
        providers: [],
        peers: [windowsPeer()],
        httpHandler: failedInventoryWithDescriptorHandler,
      }),
    );

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      backends: [
        { alias: "local", status: "online" },
        {
          alias: "windows",
          environmentId: "environment-visibility-windows",
          label: "Windows Workstation",
          os: "windows",
          status: "error",
          lastSeenAt: "2026-07-06T12:00:00.000Z",
          error: expect.any(String),
          providers: [],
        },
      ],
    });
  }),
);

it.effect("rejects peer inventory from a different environment", () =>
  Effect.gen(function* () {
    const calledTools: string[] = [];
    const seenAliases: string[] = [];
    const result = yield* callListBackends(
      makeLayer({
        providers: [],
        peers: [windowsPeer()],
        httpHandler: remoteInventoryHandler(calledTools, "environment-visibility-unexpected"),
        seenAliases,
      }),
    );

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      backends: [
        { alias: "local", status: "online" },
        {
          alias: "windows",
          environmentId: "environment-visibility-windows",
          status: "error",
          error: expect.stringContaining("environment-visibility-unexpected"),
          providers: [],
        },
      ],
    });
    expect(calledTools).toEqual(["t3_list_backends"]);
    expect(seenAliases).toEqual([]);
  }),
);

it.effect("returns only the local inventory to peer-scoped callers", () =>
  Effect.gen(function* () {
    const result = yield* callListBackends(
      makeLayer({
        providers: [makeProvider({ instanceId: "codex", driver: "codex" })],
        peers: [windowsPeer()],
      }),
      peerInvocation,
    );

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      backends: [{ alias: "local", providers: [{ instanceId: "codex" }] }],
    });
    expect((result.structuredContent as { backends: unknown[] }).backends).toHaveLength(1);
  }),
);
