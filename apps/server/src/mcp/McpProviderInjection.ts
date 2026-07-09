import type { McpProviderSessionConfig } from "./McpProviderSession.ts";

export const T3_MCP_SERVER_NAME = "t3-code";
export const T3_MCP_BEARER_TOKEN_ENV = "T3_MCP_BEARER_TOKEN";

export type ProviderEnvironment = Readonly<Record<string, string | undefined>>;

export const makeClaudeMcpServers = (session: McpProviderSessionConfig) => ({
  [T3_MCP_SERVER_NAME]: {
    type: "http" as const,
    url: session.endpoint,
    headers: {
      Authorization: session.authorizationHeader,
    },
  },
});

export const makeAcpMcpServers = (session: McpProviderSessionConfig) =>
  [
    {
      type: "http" as const,
      name: T3_MCP_SERVER_NAME,
      url: session.endpoint,
      headers: [
        {
          name: "Authorization",
          value: session.authorizationHeader,
        },
      ],
    },
  ] as const;

export const makeCodexMcpRuntimeConfig = (
  session: McpProviderSessionConfig,
  environment: ProviderEnvironment,
) => ({
  environment: {
    ...environment,
    [T3_MCP_BEARER_TOKEN_ENV]: session.authorizationHeader.replace(/^Bearer\s+/, ""),
  },
  appServerArgs: [
    "-c",
    `mcp_servers.${T3_MCP_SERVER_NAME}.url=${session.endpoint}`,
    "-c",
    `mcp_servers.${T3_MCP_SERVER_NAME}.bearer_token_env_var="${T3_MCP_BEARER_TOKEN_ENV}"`,
  ],
});
