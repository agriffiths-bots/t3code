import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as McpInvocationContext from "./McpInvocationContext.ts";

it.effect("reports generic capability denial for provider-scoped credentials", () => {
  const invocation: McpInvocationContext.ProviderMcpInvocationScope = {
    credentialKind: "provider-session",
    environmentId: EnvironmentId.make("environment-1"),
    threadId: ThreadId.make("thread-1"),
    providerSessionId: "provider-session-1",
    providerInstanceId: ProviderInstanceId.make("codex"),
    capabilities: new Set(),
    issuedAt: 1,
    expiresAt: Number.MAX_SAFE_INTEGER,
  };

  return Effect.gen(function* () {
    const error = yield* McpInvocationContext.requireMcpCapability("thread-management").pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
      Effect.flip,
    );

    expect(error).toBeInstanceOf(McpInvocationContext.McpCapabilityUnavailableError);
    expect(error).toMatchObject({
      capability: "thread-management",
      environmentId: invocation.environmentId,
      credentialKind: "provider-session",
    });
    expect(error.message).toBe("MCP credential does not grant the thread-management capability.");
  });
});

it.effect("reports generic capability denial for peer-scoped credentials", () => {
  const invocation: McpInvocationContext.PeerMcpInvocationScope = {
    credentialKind: "peer",
    environmentId: EnvironmentId.make("environment-1"),
    peerTokenId: "peer-token-1",
    capabilities: new Set(["subagent:spawn"]),
    issuedAt: 1,
    expiresAt: null,
  };

  return Effect.gen(function* () {
    const error = yield* McpInvocationContext.requireMcpCapability("notification").pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
      Effect.flip,
    );

    expect(error).toBeInstanceOf(McpInvocationContext.McpCapabilityUnavailableError);
    expect(error).toMatchObject({
      capability: "notification",
      environmentId: invocation.environmentId,
      credentialKind: "peer",
    });
    expect(error.message).toBe("MCP credential does not grant the notification capability.");
  });
});
