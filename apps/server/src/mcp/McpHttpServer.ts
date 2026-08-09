import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Types from "effect/Types";
import { McpProtocol, McpServer } from "effect/unstable/ai";
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
        if (!invocation) {
          // Without this the only symptom of a dead credential is the agent
          // quietly losing the whole `t3-code` toolkit for the rest of its
          // session, with nothing on the server to explain why.
          yield* Effect.logWarning("rejected MCP request with an unusable credential", {
            reason: token.length === 0 ? "missing_bearer_token" : "unknown_or_expired_token",
          });
          return unauthorized;
        }
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
  protocols: [McpProtocol.v2025_06_18],
}).pipe(Layer.provide(McpAuthMiddlewareLive));

export const ToolkitRegistrationLive = Layer.mergeAll(
  ThreadToolkitRegistrationLive,
  SubagentToolkitRegistrationLive,
  NotifyToolkitRegistrationLive,
  UsageToolkitRegistrationLive,
  VisibilityToolkitRegistrationLive,
);

export const layer = ToolkitRegistrationLive.pipe(Layer.provideMerge(McpTransportLive));

export const __testing = {
  McpAuthMiddlewareLive,
  unauthorized,
};
