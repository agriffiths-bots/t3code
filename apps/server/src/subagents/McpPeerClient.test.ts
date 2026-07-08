import { NodeHttpServer } from "@effect/platform-node";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { AuthOrchestrationOperateScope, AuthSessionId, EnvironmentId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { McpSchema, McpServer } from "effect/unstable/ai";
import { HttpClient, HttpClientResponse, HttpRouter } from "effect/unstable/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { mcpPeerTokenRouteLayer } from "../http.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
import * as McpPeerClient from "./McpPeerClient.ts";
import { SUBAGENT_PEER_MCP_TOKEN_PATH, SubagentPeerMcpTokenResult } from "./SubagentPeerHttp.ts";
import * as SubagentPeerRegistry from "./SubagentPeerRegistry.ts";

const JsonRpcRequest = Schema.Struct({
  jsonrpc: Schema.Literal("2.0"),
  id: Schema.optional(Schema.Union([Schema.Number, Schema.String])),
  method: Schema.String,
  params: Schema.Unknown,
});
type JsonRpcRequest = typeof JsonRpcRequest.Type;
type CapturedHttpRequest = Parameters<Parameters<typeof HttpClient.make>[0]>[0];

const decodeJsonRpcRequest = Schema.decodeUnknownSync(Schema.fromJsonString(JsonRpcRequest));
const encodeUnknownJson = Schema.encodeSync(Schema.UnknownFromJsonString);

const bearerPeer: SubagentPeerRegistry.SubagentPeer = {
  alias: "vps",
  environmentId: EnvironmentId.make("env-vps"),
  httpBaseUrl: "https://peer.example/",
  mcpEndpoint: "https://peer.example/mcp",
  credential: new SubagentPeerRegistry.SubagentPeerBearerCredential({
    token: "peer-token",
  }),
  cfAccess: {
    _tag: "service-token",
    clientId: "cf-client-id",
    clientSecret: "cf-client-secret",
  },
  pairedAt: "2026-07-08T10:00:00.000Z",
};

const registerPeerPingTool = Layer.effectDiscard(
  Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    yield* server.addTool({
      tool: new McpSchema.Tool({
        name: "t3_peer_ping",
        description: "Test peer MCP round trip",
        inputSchema: {
          type: "object",
          properties: {
            message: { type: "string" },
          },
          required: ["message"],
          additionalProperties: false,
        },
      }),
      annotations: Context.empty(),
      handle: (payload) => {
        const message =
          typeof payload === "object" &&
          payload !== null &&
          "message" in payload &&
          typeof payload.message === "string"
            ? payload.message
            : "";
        return Effect.succeed(
          new McpSchema.CallToolResult({
            content: [{ type: "text", text: `echo:${message}` }],
            structuredContent: { echoed: message },
            isError: false,
          }),
        );
      },
    });
  }),
);

const fakeEnvironment = ServerEnvironment.ServerEnvironment.of({
  getEnvironmentId: Effect.succeed(EnvironmentId.make("env-peer-client-test")),
  getDescriptor: Effect.die("unused"),
});

let lastAuthenticatedHttpRequestAuthorization: string | undefined;

const fakeEnvironmentAuth = EnvironmentAuth.EnvironmentAuth.of({
  getDescriptor: () => Effect.die("unused"),
  getSessionState: () => Effect.die("unused"),
  createBrowserSession: () => Effect.die("unused"),
  exchangeBootstrapCredentialForAccessToken: () => Effect.die("unused"),
  createPairingLink: () => Effect.die("unused"),
  issuePairingCredential: () => Effect.die("unused"),
  issueStartupPairingCredential: () => Effect.die("unused"),
  listPairingLinks: () => Effect.die("unused"),
  revokePairingLink: () => Effect.die("unused"),
  issueSession: () => Effect.die("unused"),
  listSessions: () => Effect.die("unused"),
  revokeSession: () => Effect.die("unused"),
  revokeOtherSessionsExcept: () => Effect.die("unused"),
  listClientSessions: () => Effect.die("unused"),
  revokeClientSession: () => Effect.die("unused"),
  revokeOtherClientSessions: () => Effect.die("unused"),
  authenticateHttpRequest: (request) => {
    lastAuthenticatedHttpRequestAuthorization = request.headers.authorization;
    return Effect.succeed({
      sessionId: AuthSessionId.make("peer-client-route-session"),
      subject: "peer-client-route-test",
      method: "bearer-access-token" as const,
      scopes: [AuthOrchestrationOperateScope],
      expiresAt: DateTime.makeUnsafe("2036-07-08T10:00:00.000Z"),
    });
  },
  confirmHttpRequestSessionActive: (_request, sessionId) =>
    Effect.succeed({
      sessionId,
      subject: "peer-client-route-test",
      method: "bearer-access-token" as const,
      scopes: [AuthOrchestrationOperateScope],
      expiresAt: DateTime.makeUnsafe("2036-07-08T10:00:00.000Z"),
    }),
  authenticateWebSocketUpgrade: () => Effect.die("unused"),
  issueWebSocketTicket: () => Effect.die("unused"),
  issueStartupPairingUrl: () => Effect.die("unused"),
});

const unauthenticatedTestMcpTransportLayer = McpServer.layerHttp({
  name: "peer-test",
  version: "1.0.0",
  path: "/mcp",
});

const authenticatedTestMcpTransportLayer = McpServer.layerHttp({
  name: "peer-auth-test",
  version: "1.0.0",
  path: "/mcp",
}).pipe(Layer.provide(McpHttpServer.__testing.McpAuthMiddlewareLive));

const decodeRequestBody = (request: CapturedHttpRequest) => {
  const rawBody = (request.body as { readonly body?: Uint8Array }).body;
  assert.ok(rawBody);
  return decodeJsonRpcRequest(new TextDecoder().decode(rawBody));
};

it.effect("initializes a peer session and sends tools/call over MCP HTTP", () => {
  const requests: Array<CapturedHttpRequest> = [];
  const httpLayer = Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) =>
      Effect.sync(() => {
        requests.push(request);
        const rpc = decodeRequestBody(request);
        assert.equal(request.url, "https://peer.example/mcp");
        assert.equal(request.method, "POST");
        if (rpc.method === "initialize") {
          return HttpClientResponse.fromWeb(
            request,
            Response.json(
              {
                jsonrpc: "2.0",
                id: rpc.id,
                result: {
                  protocolVersion: "2025-06-18",
                  capabilities: { tools: {} },
                  serverInfo: { name: "peer-test", version: "1.0.0" },
                },
              },
              {
                headers: {
                  "mcp-session-id": "session-1",
                  "mcp-protocol-version": "2025-06-18",
                },
              },
            ),
          );
        }
        if (rpc.method === "notifications/initialized") {
          return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
        }
        assert.equal(rpc.method, "tools/call");
        return HttpClientResponse.fromWeb(
          request,
          Response.json({
            jsonrpc: "2.0",
            id: rpc.id,
            result: {
              content: [{ type: "text", text: '{"state":"complete"}' }],
              structuredContent: { state: "complete" },
              isError: false,
            },
          }),
        );
      }),
    ),
  );

  return Effect.gen(function* () {
    const session = yield* McpPeerClient.connect(bearerPeer, {
      clientName: "mcp-peer-client-test",
      clientVersion: "1.0.0",
    }).pipe(Effect.provide(httpLayer));

    assert.equal(session.sessionId, "session-1");
    assert.equal(session.protocolVersion, "2025-06-18");
    assert.equal(session.initializeResult.serverInfo.name, "peer-test");

    const result = yield* session.callTool({
      name: "t3_check_subagent",
      arguments: { childThreadId: "child-thread-1" },
    });

    assert.equal(result.isError, false);
    assert.deepStrictEqual(result.structuredContent, { state: "complete" });
    assert.equal(requests.length, 3);

    const initializeRequest = requests[0];
    const initializedRequest = requests[1];
    const toolRequest = requests[2];
    assert.ok(initializeRequest);
    assert.ok(initializedRequest);
    assert.ok(toolRequest);

    assert.equal(initializeRequest.headers.authorization, "Bearer peer-token");
    assert.equal(initializeRequest.headers.accept, "application/json, text/event-stream");
    assert.equal(initializeRequest.headers["cf-access-client-id"], "cf-client-id");
    assert.equal(initializeRequest.headers["cf-access-client-secret"], "cf-client-secret");
    assert.equal(initializeRequest.headers["mcp-session-id"], undefined);

    assert.equal(initializedRequest.headers.authorization, "Bearer peer-token");
    assert.equal(initializedRequest.headers["mcp-session-id"], "session-1");
    assert.equal(initializedRequest.headers["mcp-protocol-version"], "2025-06-18");

    assert.equal(toolRequest.headers.authorization, "Bearer peer-token");
    assert.equal(toolRequest.headers["mcp-session-id"], "session-1");
    assert.equal(toolRequest.headers["mcp-protocol-version"], "2025-06-18");

    const initializeRpc = decodeRequestBody(initializeRequest);
    assert.equal(initializeRpc.method, "initialize");
    assert.deepStrictEqual(initializeRpc.params, {
      protocolVersion: "2025-06-18",
      capabilities: {},
      clientInfo: {
        name: "mcp-peer-client-test",
        version: "1.0.0",
      },
    });

    const initializedRpc = decodeRequestBody(initializedRequest);
    assert.equal(initializedRpc.method, "notifications/initialized");
    assert.deepStrictEqual(initializedRpc.params, {});
    assert.equal(initializedRpc.id, undefined);

    const callRpc = decodeRequestBody(toolRequest);
    assert.equal(callRpc.method, "tools/call");
    assert.deepStrictEqual(callRpc.params, {
      name: "t3_check_subagent",
      arguments: { childThreadId: "child-thread-1" },
    });
  });
});

it.effect("supports stateless MCP peers that do not return an MCP session id", () => {
  const requests: Array<CapturedHttpRequest> = [];
  const httpLayer = Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) =>
      Effect.sync(() => {
        requests.push(request);
        const rpc = decodeRequestBody(request);
        if (rpc.method === "initialize") {
          return HttpClientResponse.fromWeb(
            request,
            Response.json({
              jsonrpc: "2.0",
              id: rpc.id,
              result: {
                protocolVersion: "2025-06-18",
                capabilities: { tools: {} },
                serverInfo: { name: "peer-test", version: "1.0.0" },
              },
            }),
          );
        }
        if (rpc.method === "notifications/initialized") {
          return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
        }
        assert.equal(rpc.method, "tools/call");
        return HttpClientResponse.fromWeb(
          request,
          Response.json({
            jsonrpc: "2.0",
            id: rpc.id,
            result: {
              content: [{ type: "text", text: "stateless-ok" }],
              structuredContent: { state: "stateless-ok" },
              isError: false,
            },
          }),
        );
      }),
    ),
  );

  return Effect.gen(function* () {
    const session = yield* McpPeerClient.connect(bearerPeer).pipe(Effect.provide(httpLayer));

    assert.equal(session.sessionId, undefined);
    const result = yield* session.callTool({
      name: "t3_check_subagent",
      arguments: { childThreadId: "child-thread-1" },
    });

    assert.equal(result.isError, false);
    assert.deepStrictEqual(result.structuredContent, { state: "stateless-ok" });
    assert.equal(requests.length, 3);
    assert.equal(requests[1]?.headers["mcp-session-id"], undefined);
    assert.equal(requests[2]?.headers["mcp-session-id"], undefined);
  });
});

it.effect("decodes JSON-RPC responses delivered as finite SSE events", () => {
  const requests: Array<CapturedHttpRequest> = [];
  const httpLayer = Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) =>
      Effect.sync(() => {
        requests.push(request);
        const rpc = decodeRequestBody(request);
        if (rpc.method === "initialize") {
          return HttpClientResponse.fromWeb(
            request,
            Response.json(
              {
                jsonrpc: "2.0",
                id: rpc.id,
                result: {
                  protocolVersion: "2025-06-18",
                  capabilities: { tools: {} },
                  serverInfo: { name: "peer-test", version: "1.0.0" },
                },
              },
              {
                headers: {
                  "mcp-session-id": "session-sse",
                  "mcp-protocol-version": "2025-06-18",
                },
              },
            ),
          );
        }
        if (rpc.method === "notifications/initialized") {
          return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
        }
        assert.equal(rpc.method, "tools/call");
        const notificationPayload = encodeUnknownJson({
          jsonrpc: "2.0",
          method: "notifications/progress",
          params: { progressToken: "peer-progress", progress: 1, total: 2 },
        });
        const payload = encodeUnknownJson({
          jsonrpc: "2.0",
          id: rpc.id,
          result: {
            content: [{ type: "text", text: "sse-ok" }],
            structuredContent: { state: "sse-ok" },
            isError: false,
          },
        });
        return HttpClientResponse.fromWeb(
          request,
          new Response(`event: message\ndata: ${notificationPayload}\n\ndata: ${payload}\n\n`, {
            headers: { "content-type": "text/event-stream" },
          }),
        );
      }),
    ),
  );

  return Effect.gen(function* () {
    const session = yield* McpPeerClient.connect(bearerPeer).pipe(Effect.provide(httpLayer));
    const result = yield* session.callTool({
      name: "t3_check_subagent",
      arguments: { childThreadId: "child-thread-1" },
    });

    assert.equal(result.isError, false);
    assert.deepStrictEqual(result.content, [{ type: "text", text: "sse-ok" }]);
    assert.deepStrictEqual(result.structuredContent, { state: "sse-ok" });
    assert.equal(requests.length, 3);
  });
});

it.effect("fails before HTTP when a peer uses an unresolved credential reference", () => {
  const peer: SubagentPeerRegistry.SubagentPeer = {
    ...bearerPeer,
    credential: new SubagentPeerRegistry.SubagentPeerCredentialRef({
      ref: "secret/peer-token",
    }),
  };
  let httpRequests = 0;
  const httpLayer = Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) =>
      Effect.sync(() => {
        httpRequests += 1;
        return HttpClientResponse.fromWeb(request, Response.json({}));
      }),
    ),
  );

  return Effect.gen(function* () {
    const error = yield* McpPeerClient.connect(peer).pipe(Effect.provide(httpLayer), Effect.flip);

    assert.instanceOf(error, McpPeerClient.McpPeerClientError);
    assert.equal(error.operation, "credential");
    assert.equal(httpRequests, 0);
  });
});

it.effect("calls a tool on a real ephemeral MCP /mcp server", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const serverLayer = Layer.mergeAll(
        registerPeerPingTool.pipe(Layer.provideMerge(unauthenticatedTestMcpTransportLayer)),
        McpHttpServer.McpGetMethodNotAllowedLive,
      );
      yield* HttpRouter.serve(serverLayer, {
        disableListenLog: true,
        disableLogger: true,
      }).pipe(Layer.build);

      const httpClient = yield* HttpClient.HttpClient;
      const getResponse = yield* httpClient.get("/mcp", {
        headers: { accept: "text/event-stream" },
      });
      assert.equal(getResponse.status, 405);
      assert.equal(getResponse.headers["allow"], "POST, DELETE");

      const session = yield* McpPeerClient.connect({
        ...bearerPeer,
        mcpEndpoint: "/mcp",
      });
      const result = yield* session.callTool({
        name: "t3_peer_ping",
        arguments: { message: "hello from peer A" },
      });

      assert.equal(result.isError, false);
      assert.deepStrictEqual(result.structuredContent, { echoed: "hello from peer A" });
      assert.deepStrictEqual(result.content, [{ type: "text", text: "echo:hello from peer A" }]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("uses route-minted peer tokens for authenticated MCP calls", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const serverLayer = Layer.mergeAll(
        mcpPeerTokenRouteLayer,
        registerPeerPingTool.pipe(Layer.provideMerge(authenticatedTestMcpTransportLayer)),
        McpHttpServer.McpGetMethodNotAllowedLive,
      ).pipe(Layer.provide(McpSessionRegistry.layer));
      yield* HttpRouter.serve(serverLayer, {
        disableListenLog: true,
        disableLogger: true,
      }).pipe(
        Layer.provide(Layer.succeed(EnvironmentAuth.EnvironmentAuth, fakeEnvironmentAuth)),
        Layer.provide(Layer.succeed(ServerEnvironment.ServerEnvironment, fakeEnvironment)),
        Layer.provide(NodeServices.layer),
        Layer.build,
      );

      const accessTokenError = yield* McpPeerClient.connect({
        ...bearerPeer,
        mcpEndpoint: "/mcp",
        credential: new SubagentPeerRegistry.SubagentPeerBearerCredential({
          token: "env-access-token",
        }),
      }).pipe(Effect.flip);
      assert.equal(accessTokenError.operation, "http-status");
      assert.equal(accessTokenError.status, 401);

      const httpClient = yield* HttpClient.HttpClient;
      lastAuthenticatedHttpRequestAuthorization = undefined;
      const tokenResponse = yield* httpClient.post(SUBAGENT_PEER_MCP_TOKEN_PATH, {
        headers: { authorization: "Bearer env-access-token" },
      });
      if (tokenResponse.status !== 200) {
        const body = yield* tokenResponse.text;
        assert.fail(
          `Expected peer-token mint to return 200, got ${tokenResponse.status}: ${body}; auth=${String(lastAuthenticatedHttpRequestAuthorization)}`,
        );
      }
      assert.equal(lastAuthenticatedHttpRequestAuthorization, "Bearer env-access-token");
      assert.equal(tokenResponse.headers["cache-control"], "no-store");
      const peerToken = yield* HttpClientResponse.schemaBodyJson(SubagentPeerMcpTokenResult)(
        tokenResponse,
      );
      assert.equal(peerToken.authorizationHeader, `Bearer ${peerToken.token}`);
      assert.deepEqual(peerToken.capabilities, [
        "subagent:spawn",
        "subagent:check",
        "subagent:wait",
        "subagent:list",
      ]);

      const session = yield* McpPeerClient.connect({
        ...bearerPeer,
        mcpEndpoint: "/mcp",
        credential: new SubagentPeerRegistry.SubagentPeerBearerCredential({
          token: peerToken.token,
        }),
      });
      const result = yield* session.callTool({
        name: "t3_peer_ping",
        arguments: { message: "authenticated" },
      });

      assert.equal(result.isError, false);
      assert.deepStrictEqual(result.structuredContent, { echoed: "authenticated" });

      yield* McpSessionRegistry.revokeActiveMcpPeerCredentialsForAuthSession(
        AuthSessionId.make("peer-client-route-session"),
      );

      const revokedTokenError = yield* McpPeerClient.connect({
        ...bearerPeer,
        mcpEndpoint: "/mcp",
        credential: new SubagentPeerRegistry.SubagentPeerBearerCredential({
          token: peerToken.token,
        }),
      }).pipe(Effect.flip);
      assert.equal(revokedTokenError.operation, "http-status");
      assert.equal(revokedTokenError.status, 401);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("rejects route-minted peer tokens when source session confirmation fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let authCalls = 0;
      let confirmCalls = 0;
      const racingEnvironmentAuth = EnvironmentAuth.EnvironmentAuth.of({
        ...fakeEnvironmentAuth,
        authenticateHttpRequest: (request) => {
          lastAuthenticatedHttpRequestAuthorization = request.headers.authorization;
          authCalls += 1;
          return Effect.succeed({
            sessionId: AuthSessionId.make("peer-client-route-racing-session"),
            subject: "peer-client-route-race-test",
            method: "bearer-access-token" as const,
            scopes: [AuthOrchestrationOperateScope],
            expiresAt: DateTime.makeUnsafe("2036-07-08T10:00:00.000Z"),
          });
        },
        confirmHttpRequestSessionActive: () => {
          confirmCalls += 1;
          return Effect.fail(
            new EnvironmentAuth.ServerAuthInvalidCredentialError({
              reason: "invalid_credential",
            }),
          );
        },
      });
      const serverLayer = Layer.mergeAll(
        mcpPeerTokenRouteLayer,
        registerPeerPingTool.pipe(Layer.provideMerge(authenticatedTestMcpTransportLayer)),
        McpHttpServer.McpGetMethodNotAllowedLive,
      ).pipe(Layer.provide(McpSessionRegistry.layer));
      yield* HttpRouter.serve(serverLayer, {
        disableListenLog: true,
        disableLogger: true,
      }).pipe(
        Layer.provide(Layer.succeed(EnvironmentAuth.EnvironmentAuth, racingEnvironmentAuth)),
        Layer.provide(Layer.succeed(ServerEnvironment.ServerEnvironment, fakeEnvironment)),
        Layer.provide(NodeServices.layer),
        Layer.build,
      );

      const existingPeerToken = yield* McpSessionRegistry.issueActiveMcpPeerCredential({
        sourceSessionId: AuthSessionId.make("peer-client-route-racing-session"),
        expiresAt: DateTime.makeUnsafe("2036-07-08T10:00:00.000Z").epochMilliseconds,
      });
      if (existingPeerToken === undefined) {
        assert.fail("Expected active MCP registry to mint an existing peer token");
      }

      const httpClient = yield* HttpClient.HttpClient;
      lastAuthenticatedHttpRequestAuthorization = undefined;
      const tokenResponse = yield* httpClient.post(SUBAGENT_PEER_MCP_TOKEN_PATH, {
        headers: { authorization: "Bearer racing-env-access-token" },
      });

      assert.equal(tokenResponse.status, 401);
      assert.equal(lastAuthenticatedHttpRequestAuthorization, "Bearer racing-env-access-token");
      assert.equal(authCalls, 1);
      assert.equal(confirmCalls, 1);

      const session = yield* McpPeerClient.connect({
        ...bearerPeer,
        mcpEndpoint: "/mcp",
        credential: new SubagentPeerRegistry.SubagentPeerBearerCredential({
          token: existingPeerToken.token,
        }),
      });
      const result = yield* session.callTool({
        name: "t3_peer_ping",
        arguments: { message: "existing token survived" },
      });
      assert.equal(result.isError, false);
      assert.deepStrictEqual(result.structuredContent, { echoed: "existing token survived" });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
