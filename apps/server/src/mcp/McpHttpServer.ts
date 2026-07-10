import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Types from "effect/Types";
import { McpServer } from "effect/unstable/ai";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import packageJson from "../../package.json" with { type: "json" };
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";
import { ThreadToolkitHandlersLive } from "./toolkits/thread/handlers.ts";
import { ThreadToolkit } from "./toolkits/thread/tools.ts";
import {
  installPeerSubagentCompatibility,
  SubagentToolkitHandlersLive,
} from "./toolkits/subagent/handlers.ts";
import { SubagentToolkit } from "./toolkits/subagent/tools.ts";
import { NotifyToolkitHandlersLive } from "./toolkits/notify/handlers.ts";
import { NotifyToolkit } from "./toolkits/notify/tools.ts";
import { UsageToolkitHandlersLive } from "./toolkits/usage/handlers.ts";
import { UsageToolkit } from "./toolkits/usage/tools.ts";
import { VisibilityToolkitHandlersLive } from "./toolkits/visibility/handlers.ts";
import { VisibilityToolkit } from "./toolkits/visibility/tools.ts";

const mcpCredentialRecoveryHint =
  "T3 Code MCP authentication failed because this provider-session credential is not active. Start a fresh T3 Code provider session; OAuth re-authorization cannot recover this local session credential.";

const unauthorized = HttpServerResponse.jsonUnsafe(
  {
    error: "invalid_mcp_credential",
    message: mcpCredentialRecoveryHint,
    recovery: "Start a fresh T3 Code provider session.",
  },
  {
    status: 401,
    headers: {
      "cache-control": "no-store",
      "www-authenticate": `Bearer error="invalid_token", error_description="${mcpCredentialRecoveryHint}"`,
    },
  },
);

type AuthenticatedHttpEffect = Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  Types.unhandled,
  McpInvocationContext.McpInvocationContext
>;

type McpAuthMiddleware = (
  httpEffect: AuthenticatedHttpEffect,
) => Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  Types.unhandled,
  HttpServerRequest.HttpServerRequest
>;

export const normalizeMcpHttpResponse = (
  response: HttpServerResponse.HttpServerResponse,
): HttpServerResponse.HttpServerResponse => {
  const bodyIsEmpty =
    response.body._tag === "Empty" ||
    (response.body._tag === "Uint8Array" && response.body.contentLength === 0) ||
    (response.body._tag === "Raw" && response.body.contentLength === 0);
  return response.status === 200 && bodyIsEmpty
    ? HttpServerResponse.setStatus(response, 202)
    : response;
};

const makeMcpAuthMiddleware = McpSessionRegistry.McpSessionRegistry.pipe(
  Effect.map(
    (registry): McpAuthMiddleware =>
      Effect.fn("McpHttpServer.authenticateRequest")(function* (httpEffect) {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const authorization = request.headers.authorization;
        const token =
          authorization?.startsWith("Bearer ") === true
            ? authorization.slice("Bearer ".length).trim()
            : "";
        const invocation = yield* registry.resolve(token);
        if (!invocation) return unauthorized;
        return yield* httpEffect.pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.map(normalizeMcpHttpResponse),
        );
      }),
  ),
  Effect.withSpan("McpHttpServer.makeAuthMiddleware"),
);

const McpAuthMiddlewareLive = HttpRouter.middleware<{
  provides: McpInvocationContext.McpInvocationContext;
}>()(makeMcpAuthMiddleware).layer;

export const ThreadToolkitRegistrationLive = McpServer.toolkit(ThreadToolkit).pipe(
  Layer.provide(ThreadToolkitHandlersLive),
);

export const SubagentToolkitRegistrationLive = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* McpServer.registerToolkit(SubagentToolkit);
    yield* installPeerSubagentCompatibility;
  }),
).pipe(Layer.provide(SubagentToolkitHandlersLive), Layer.provide(McpServer.McpServer.layer));

export const NotifyToolkitRegistrationLive = McpServer.toolkit(NotifyToolkit).pipe(
  Layer.provide(NotifyToolkitHandlersLive),
);

export const UsageToolkitRegistrationLive = McpServer.toolkit(UsageToolkit).pipe(
  Layer.provide(UsageToolkitHandlersLive),
);

export const VisibilityToolkitRegistrationLive = McpServer.toolkit(VisibilityToolkit).pipe(
  Layer.provide(VisibilityToolkitHandlersLive),
);

const MCP_PATH = "/mcp";

const McpTransportLive = McpServer.layerHttp({
  name: "T3 Code",
  version: packageJson.version,
  path: MCP_PATH,
}).pipe(Layer.provide(McpAuthMiddlewareLive));

// The Streamable-HTTP transport above registers POST (JSON-RPC) and DELETE
// (session termination) on MCP_PATH — but no GET route. Without an explicit
// GET handler the request falls through to the SPA fallback, which serves
// index.html with 200. MCP clients open GET as the optional server-initiated
// SSE stream and treat that instant-closing HTML response as a broken stream,
// so they reconnect in a tight loop and the transport never settles — tools
// never surface (observed live: thousands of sub-10ms `GET /mcp` 200s per
// session while the agent reports "still connecting"). We do not offer a
// server-initiated stream, and the MCP spec requires servers that don't to
// answer GET with 405 Method Not Allowed; conformant clients then settle into
// POST-only operation.
const mcpGetMethodNotAllowed = HttpServerResponse.empty({
  status: 405,
  headers: { allow: "POST, DELETE" },
});
export const McpGetMethodNotAllowedLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const router = yield* HttpRouter.HttpRouter;
    yield* router.add("GET", MCP_PATH, Effect.succeed(mcpGetMethodNotAllowed));
  }),
);

export const ToolkitRegistrationLive = Layer.mergeAll(
  ThreadToolkitRegistrationLive,
  SubagentToolkitRegistrationLive,
  NotifyToolkitRegistrationLive,
  UsageToolkitRegistrationLive,
  VisibilityToolkitRegistrationLive,
);

export const layer = Layer.mergeAll(
  ToolkitRegistrationLive.pipe(Layer.provideMerge(McpTransportLive)),
  McpGetMethodNotAllowedLive,
);

export const __testing = {
  McpAuthMiddlewareLive,
  unauthorized,
};
