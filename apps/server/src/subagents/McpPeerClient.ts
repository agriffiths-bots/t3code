import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
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
const JsonRpcNotification = Schema.Struct({
  jsonrpc: Schema.Literal("2.0"),
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
const encodeJsonRpcNotification = Schema.encodeEffect(Schema.fromJsonString(JsonRpcNotification));
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
      "terminate",
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
  readonly sessionId?: string | undefined;
  readonly protocolVersion: string;
  readonly initializeResult: McpSchema.InitializeResult;
  readonly callTool: (
    input: McpPeerCallToolInput,
  ) => Effect.Effect<McpSchema.CallToolResult, McpPeerClientError>;
  readonly close: () => Effect.Effect<void, McpPeerClientError>;
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

const MCP_HTTP_ACCEPT = "application/json, text/event-stream";

const jsonRpcHeaders = (input: {
  readonly authorizationHeader: string;
  readonly cfAccessHeaders: Record<string, string>;
  readonly sessionId?: string | undefined;
  readonly protocolVersion?: string | undefined;
}) => ({
  accept: MCP_HTTP_ACCEPT,
  authorization: input.authorizationHeader,
  ...input.cfAccessHeaders,
  ...(input.sessionId === undefined ? {} : { "mcp-session-id": input.sessionId }),
  ...(input.protocolVersion === undefined ? {} : { "mcp-protocol-version": input.protocolVersion }),
});

const sseDataPayloads = (text: string): ReadonlyArray<string> => {
  const normalized = text.replace(/\r\n/g, "\n");
  const payloads: Array<string> = [];
  for (const event of normalized.split("\n\n")) {
    const data = event
      .split("\n")
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice("data:".length).trimStart())
      .join("\n")
      .trim();
    if (data.length > 0 && data !== "[DONE]") {
      payloads.push(data);
    }
  }
  return payloads;
};

const matchingSseResponseJsonText = Effect.fn("McpPeerClient.matchingSseResponseJsonText")(
  function* (input: {
    readonly text: string;
    readonly method: string;
    readonly id: number;
  }): Effect.fn.Return<string, McpPeerClientError> {
    const payloads = sseDataPayloads(input.text);
    if (payloads.length === 0) {
      return yield* error("decode-response", {
        method: input.method,
        detail: "MCP peer SSE response did not include a JSON-RPC data event.",
        cause: input.text,
      });
    }
    for (const payload of payloads) {
      const decoded = yield* decodeJsonRpcResponse(payload).pipe(Effect.option);
      if (Option.isSome(decoded) && decoded.value.id === input.id) {
        return payload;
      }
    }
    return yield* error("decode-response", {
      method: input.method,
      detail: "MCP peer SSE response did not include a matching JSON-RPC response.",
      cause: input.text,
    });
  },
);

const jsonRpcResponseIdMatches = (response: JsonRpcResponse, id: number) => {
  return response.id === id;
};

const responseJsonText = (
  response: HttpClientResponse.HttpClientResponse,
  method: string,
  id: number,
): Effect.Effect<string, McpPeerClientError> =>
  response.text.pipe(
    Effect.mapError((cause) =>
      error("decode-response", {
        method,
        detail: "Could not read MCP peer response body.",
        cause,
      }),
    ),
    Effect.flatMap((text) => {
      const contentType = response.headers["content-type"] ?? "";
      if (!contentType.toLowerCase().includes("text/event-stream")) {
        return Effect.succeed(text);
      }
      return matchingSseResponseJsonText({ text, method, id });
    }),
  );

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
  const headers = jsonRpcHeaders({
    authorizationHeader: input.authorizationHeader,
    cfAccessHeaders: input.cfAccessHeaders,
    sessionId: input.sessionId,
    protocolVersion: input.protocolVersion,
  });
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
  const text = yield* responseJsonText(response, input.method, input.id);
  const decoded = yield* decodeJsonRpcResponse(text).pipe(
    Effect.mapError((cause) =>
      error("decode-response", {
        method: input.method,
        detail: "Could not decode MCP peer JSON-RPC response.",
        cause,
      }),
    ),
  );
  if (!jsonRpcResponseIdMatches(decoded, input.id)) {
    return yield* error("decode-response", {
      method: input.method,
      detail: "MCP peer JSON-RPC response id did not match the request id.",
      cause: decoded,
    });
  }
  if ("error" in decoded) {
    return yield* error("json-rpc", {
      method: input.method,
      detail: jsonRpcErrorDetail(decoded),
      cause: decoded.error,
    });
  }
  return { result: decoded.result, response };
});

const postJsonRpcNotification = Effect.fn("McpPeerClient.postJsonRpcNotification")(
  function* (input: {
    readonly client: HttpClient.HttpClient;
    readonly endpoint: string;
    readonly authorizationHeader: string;
    readonly cfAccessHeaders: Record<string, string>;
    readonly sessionId?: string | undefined;
    readonly protocolVersion?: string | undefined;
    readonly method: string;
    readonly params: unknown;
  }): Effect.fn.Return<void, McpPeerClientError> {
    const encoded = yield* encodeJsonRpcNotification({
      jsonrpc: "2.0",
      method: input.method,
      params: input.params,
    }).pipe(
      Effect.mapError((cause) =>
        error("encode-request", {
          method: input.method,
          detail: "Could not encode JSON-RPC notification.",
          cause,
        }),
      ),
    );
    const headers = jsonRpcHeaders({
      authorizationHeader: input.authorizationHeader,
      cfAccessHeaders: input.cfAccessHeaders,
      sessionId: input.sessionId,
      protocolVersion: input.protocolVersion,
    });
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
  },
);

const deleteSession = Effect.fn("McpPeerClient.deleteSession")(function* (input: {
  readonly client: HttpClient.HttpClient;
  readonly endpoint: string;
  readonly authorizationHeader: string;
  readonly cfAccessHeaders: Record<string, string>;
  readonly sessionId?: string | undefined;
  readonly protocolVersion?: string | undefined;
}): Effect.fn.Return<void, McpPeerClientError> {
  if (input.sessionId === undefined) return;
  const request = HttpClientRequest.delete(input.endpoint, {
    headers: jsonRpcHeaders({
      authorizationHeader: input.authorizationHeader,
      cfAccessHeaders: input.cfAccessHeaders,
      sessionId: input.sessionId,
      protocolVersion: input.protocolVersion,
    }),
  });
  const response = yield* input.client.execute(request).pipe(
    Effect.mapError((cause) =>
      error("terminate", {
        detail: "Could not reach MCP peer to terminate the session.",
        cause,
      }),
    ),
  );
  if ((response.status >= 200 && response.status < 300) || response.status === 404) {
    return;
  }
  return yield* error("terminate", {
    status: response.status,
    detail: "MCP peer returned a non-success status when terminating the session.",
    cause: response.status,
  });
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
  const rawSessionId = initialized.response.headers["mcp-session-id"]?.trim();
  const sessionId = rawSessionId && rawSessionId.length > 0 ? rawSessionId : undefined;
  const protocolVersion =
    initialized.response.headers["mcp-protocol-version"] ?? initializeResult.protocolVersion;

  yield* postJsonRpcNotification({
    client,
    endpoint: peer.mcpEndpoint,
    authorizationHeader: auth,
    cfAccessHeaders: cfHeaders,
    sessionId,
    protocolVersion,
    method: "notifications/initialized",
    params: {},
  });

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
    close: () =>
      deleteSession({
        client,
        endpoint: peer.mcpEndpoint,
        authorizationHeader: auth,
        cfAccessHeaders: cfHeaders,
        sessionId,
        protocolVersion,
      }),
  };
});
