import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { HttpClient, HttpClientRequest, type HttpClientResponse } from "effect/unstable/http";
import { McpSchema } from "effect/unstable/ai";

import type { SubagentPeer } from "./SubagentPeerRegistry.ts";
import { cloudflareAccessHeaders } from "./SubagentPeerHttp.ts";

const MCP_PROTOCOL_VERSION = "2025-06-18";

const JsonRpcId = Schema.Union([Schema.Number, Schema.String]);
const JsonRpcRequest = Schema.Struct({
  jsonrpc: Schema.Literal("2.0"),
  id: JsonRpcId,
  method: Schema.String,
  params: Schema.Unknown,
});
const JsonRpcErrorObject = Schema.Struct({
  code: Schema.optional(Schema.Number),
  message: Schema.String,
  data: Schema.optional(Schema.Unknown),
  _tag: Schema.optional(Schema.String),
});
const JsonRpcResponse = Schema.Union([
  Schema.Struct({
    jsonrpc: Schema.Literal("2.0"),
    id: JsonRpcId,
    result: Schema.Unknown,
  }),
  Schema.Struct({
    jsonrpc: Schema.Literal("2.0"),
    id: Schema.optional(JsonRpcId),
    error: JsonRpcErrorObject,
  }),
]);
type JsonRpcResponse = typeof JsonRpcResponse.Type;

const encodeJsonRpcRequest = Schema.encodeEffect(Schema.fromJsonString(JsonRpcRequest));
const decodeJsonRpcResponse = Schema.decodeEffect(Schema.fromJsonString(JsonRpcResponse));
const decodeInitializeResult = Schema.decodeUnknownEffect(McpSchema.InitializeResult);
const decodeCallToolResult = Schema.decodeUnknownEffect(McpSchema.CallToolResult);

export class McpPeerClientError extends Schema.TaggedErrorClass<McpPeerClientError>()(
  "McpPeerClientError",
  {
    operation: Schema.Literals([
      "credential",
      "encode-request",
      "http-request",
      "http-status",
      "decode-response",
      "json-rpc",
      "initialize",
      "call-tool",
    ]),
    method: Schema.optional(Schema.String),
    status: Schema.optional(Schema.Number),
    detail: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    const method = this.method === undefined ? "" : ` for ${this.method}`;
    const status = this.status === undefined ? "" : ` (HTTP ${this.status})`;
    return `MCP peer client ${this.operation}${method} failed${status}: ${this.detail}`;
  }
}

export interface McpPeerClientConnectOptions {
  readonly clientName?: string | undefined;
  readonly clientVersion?: string | undefined;
}

export interface McpPeerCallToolInput {
  readonly name: string;
  readonly arguments?: Record<string, unknown> | undefined;
}

export interface McpPeerClientSession {
  readonly peer: SubagentPeer;
  readonly sessionId: string;
  readonly protocolVersion: string;
  readonly initializeResult: McpSchema.InitializeResult;
  readonly callTool: (
    input: McpPeerCallToolInput,
  ) => Effect.Effect<McpSchema.CallToolResult, McpPeerClientError>;
}

interface JsonRpcSuccess {
  readonly result: unknown;
  readonly response: HttpClientResponse.HttpClientResponse;
}

const jsonRpcErrorDetail = (response: Extract<JsonRpcResponse, { readonly error: unknown }>) =>
  response.error.message;

const error = (
  operation: McpPeerClientError["operation"],
  input: {
    readonly method?: string | undefined;
    readonly status?: number | undefined;
    readonly detail: string;
    readonly cause: unknown;
  },
) =>
  new McpPeerClientError({
    operation,
    ...(input.method === undefined ? {} : { method: input.method }),
    ...(input.status === undefined ? {} : { status: input.status }),
    detail: input.detail,
    cause: input.cause,
  });

const authorizationHeader = (peer: SubagentPeer): Effect.Effect<string, McpPeerClientError> => {
  if (peer.credential._tag === "bearer") {
    return Effect.succeed(`Bearer ${peer.credential.token}`);
  }
  return Effect.fail(
    error("credential", {
      detail: "Credential references are not resolved by McpPeerClient yet.",
      cause: peer.credential,
    }),
  );
};

const postJsonRpc = Effect.fn("McpPeerClient.postJsonRpc")(function* (input: {
  readonly client: HttpClient.HttpClient;
  readonly endpoint: string;
  readonly authorizationHeader: string;
  readonly cfAccessHeaders: Record<string, string>;
  readonly sessionId?: string | undefined;
  readonly protocolVersion?: string | undefined;
  readonly id: number;
  readonly method: string;
  readonly params: unknown;
}): Effect.fn.Return<JsonRpcSuccess, McpPeerClientError> {
  const encoded = yield* encodeJsonRpcRequest({
    jsonrpc: "2.0",
    id: input.id,
    method: input.method,
    params: input.params,
  }).pipe(
    Effect.mapError((cause) =>
      error("encode-request", {
        method: input.method,
        detail: "Could not encode JSON-RPC request.",
        cause,
      }),
    ),
  );
  const headers = {
    accept: "application/json",
    authorization: input.authorizationHeader,
    ...input.cfAccessHeaders,
    ...(input.sessionId === undefined ? {} : { "mcp-session-id": input.sessionId }),
    ...(input.protocolVersion === undefined
      ? {}
      : { "mcp-protocol-version": input.protocolVersion }),
  };
  const request = HttpClientRequest.post(input.endpoint, { headers }).pipe(
    HttpClientRequest.bodyText(encoded, "application/json"),
  );
  const response = yield* input.client.execute(request).pipe(
    Effect.mapError((cause) =>
      error("http-request", {
        method: input.method,
        detail: "Could not reach MCP peer.",
        cause,
      }),
    ),
  );
  if (response.status < 200 || response.status >= 300) {
    return yield* error("http-status", {
      method: input.method,
      status: response.status,
      detail: "MCP peer returned a non-success status.",
      cause: response.status,
    });
  }
  const text = yield* response.text.pipe(
    Effect.mapError((cause) =>
      error("decode-response", {
        method: input.method,
        detail: "Could not read MCP peer response body.",
        cause,
      }),
    ),
  );
  const decoded = yield* decodeJsonRpcResponse(text).pipe(
    Effect.mapError((cause) =>
      error("decode-response", {
        method: input.method,
        detail: "Could not decode MCP peer JSON-RPC response.",
        cause,
      }),
    ),
  );
  if ("error" in decoded) {
    return yield* error("json-rpc", {
      method: input.method,
      detail: jsonRpcErrorDetail(decoded),
      cause: decoded.error,
    });
  }
  return { result: decoded.result, response };
});

export const connect = Effect.fn("McpPeerClient.connect")(function* (
  peer: SubagentPeer,
  options: McpPeerClientConnectOptions = {},
): Effect.fn.Return<McpPeerClientSession, McpPeerClientError, HttpClient.HttpClient> {
  const client = yield* HttpClient.HttpClient;
  const auth = yield* authorizationHeader(peer);
  const cfHeaders = cloudflareAccessHeaders(peer.cfAccess);
  let nextId = 1;
  const initialized = yield* postJsonRpc({
    client,
    endpoint: peer.mcpEndpoint,
    authorizationHeader: auth,
    cfAccessHeaders: cfHeaders,
    id: nextId++,
    method: "initialize",
    params: {
      protocolVersion: MCP_PROTOCOL_VERSION,
      capabilities: {},
      clientInfo: {
        name: options.clientName ?? "t3-code-peer-client",
        version: options.clientVersion ?? "0.0.0",
      },
    },
  });
  const initializeResult = yield* decodeInitializeResult(initialized.result).pipe(
    Effect.mapError((cause) =>
      error("initialize", {
        method: "initialize",
        detail: "Could not decode MCP initialize result.",
        cause,
      }),
    ),
  );
  const sessionId = initialized.response.headers["mcp-session-id"];
  if (sessionId === undefined || sessionId.trim().length === 0) {
    return yield* error("initialize", {
      method: "initialize",
      detail: "MCP peer did not return an mcp-session-id header.",
      cause: initialized.response.headers,
    });
  }
  const protocolVersion =
    initialized.response.headers["mcp-protocol-version"] ?? initializeResult.protocolVersion;

  return {
    peer,
    sessionId,
    protocolVersion,
    initializeResult,
    callTool: (input) =>
      postJsonRpc({
        client,
        endpoint: peer.mcpEndpoint,
        authorizationHeader: auth,
        cfAccessHeaders: cfHeaders,
        sessionId,
        protocolVersion,
        id: nextId++,
        method: "tools/call",
        params: {
          name: input.name,
          arguments: input.arguments ?? {},
        },
      }).pipe(
        Effect.flatMap(({ result }) =>
          decodeCallToolResult(result).pipe(
            Effect.mapError((cause) =>
              error("call-tool", {
                method: "tools/call",
                detail: "Could not decode MCP tools/call result.",
                cause,
              }),
            ),
          ),
        ),
      ),
  };
});
