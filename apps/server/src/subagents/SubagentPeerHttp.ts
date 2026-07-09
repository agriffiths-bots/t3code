import { EnvironmentId, TrimmedNonEmptyString } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import type { SubagentPeerCloudflareAccess } from "./SubagentPeerRegistry.ts";

export const SUBAGENT_PEER_MCP_TOKEN_PATH = "/api/mcp/peer-token";

export const SubagentPeerMcpTokenRequest = Schema.Struct({
  sourceEnvironmentId: Schema.optional(EnvironmentId),
});
export type SubagentPeerMcpTokenRequest = typeof SubagentPeerMcpTokenRequest.Type;

export const SubagentPeerMcpTokenResult = Schema.Struct({
  peerTokenId: TrimmedNonEmptyString,
  token: TrimmedNonEmptyString,
  authorizationHeader: TrimmedNonEmptyString,
  issuedAt: Schema.Number,
  capabilities: Schema.Array(McpInvocationContext.McpCapability),
});
export type SubagentPeerMcpTokenResult = typeof SubagentPeerMcpTokenResult.Type;

export const environmentUrl = (httpBaseUrl: string, pathname: string): string => {
  const url = new URL(httpBaseUrl);
  url.pathname = pathname;
  url.search = "";
  url.hash = "";
  return url.toString();
};

export const cloudflareAccessHeaders = (
  access: SubagentPeerCloudflareAccess | undefined,
): Record<string, string> => {
  if (access === undefined) return {};
  if (access._tag === "service-token") {
    return {
      "cf-access-client-id": access.clientId,
      "cf-access-client-secret": access.clientSecret,
    };
  }
  const jwt = access._tag === "cookie" ? access.cookieValue : access.jwt;
  return {
    "cf-access-jwt-assertion": jwt,
    cookie: `CF_Authorization=${jwt}`,
  };
};
