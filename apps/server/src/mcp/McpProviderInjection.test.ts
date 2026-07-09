import { assert, describe, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";

import type { McpProviderSessionConfig } from "./McpProviderSession.ts";
import {
  makeAcpMcpServers,
  makeClaudeMcpServers,
  makeCodexMcpRuntimeConfig,
  T3_MCP_BEARER_TOKEN_ENV,
  T3_MCP_SERVER_NAME,
} from "./McpProviderInjection.ts";

const providerSession: McpProviderSessionConfig = {
  environmentId: EnvironmentId.make("environment-mcp-injection-audit"),
  threadId: ThreadId.make("thread-mcp-injection-audit"),
  providerSessionId: "provider-session-mcp-injection-audit",
  providerInstanceId: ProviderInstanceId.make("codex"),
  endpoint: "http://127.0.0.1:3773/mcp",
  authorizationHeader: "Bearer mcp-session-token",
};

describe("MCP provider injection audit", () => {
  it("injects Claude's MCP server from the canonical tools/list endpoint", () => {
    assert.deepEqual(makeClaudeMcpServers(providerSession), {
      [T3_MCP_SERVER_NAME]: {
        type: "http",
        url: providerSession.endpoint,
        headers: {
          Authorization: providerSession.authorizationHeader,
        },
      },
    });
  });

  it("injects Cursor's ACP MCP server from the canonical tools/list endpoint", () => {
    assert.deepEqual(makeAcpMcpServers(providerSession), [
      {
        type: "http",
        name: T3_MCP_SERVER_NAME,
        url: providerSession.endpoint,
        headers: [
          {
            name: "Authorization",
            value: providerSession.authorizationHeader,
          },
        ],
      },
    ]);
  });

  it("injects Codex's MCP server from the canonical tools/list endpoint", () => {
    const baseEnvironment = {
      KEEP_ME: "1",
    };
    const runtimeConfig = makeCodexMcpRuntimeConfig(providerSession, baseEnvironment);
    const expectedEnvironment: Record<string, string> = {
      KEEP_ME: "1",
      [T3_MCP_BEARER_TOKEN_ENV]: "mcp-session-token",
    };

    assert.deepEqual(runtimeConfig.environment, expectedEnvironment);
    assert.deepEqual(runtimeConfig.appServerArgs, [
      "-c",
      `mcp_servers.${T3_MCP_SERVER_NAME}.url=${providerSession.endpoint}`,
      "-c",
      `mcp_servers.${T3_MCP_SERVER_NAME}.bearer_token_env_var="${T3_MCP_BEARER_TOKEN_ENV}"`,
    ]);
  });
});
