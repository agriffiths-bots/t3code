import { expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { McpSchema, McpServer } from "effect/unstable/ai";

import { ServerSettingsService } from "../../../serverSettings.ts";
import * as PlanUsageSnapshot from "../../../usage/PlanUsageSnapshot.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { UsageToolkitRegistrationLive } from "../../McpHttpServer.ts";

const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-03-26",
    capabilities: {},
    clientInfo: { name: "usage-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

const originalDisabled = process.env.T3_DISABLE_PLAN_USAGE_POLLING;
const invocation: McpInvocationContext.ProviderMcpInvocationScope = {
  credentialKind: "provider-session",
  environmentId: EnvironmentId.make("environment-usage-test"),
  threadId: ThreadId.make("thread-usage-test"),
  providerSessionId: "provider-session-usage-test",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(["thread-management"]),
  issuedAt: 1,
  expiresAt: Number.MAX_SAFE_INTEGER,
};

const peerInvocation: McpInvocationContext.PeerMcpInvocationScope = {
  credentialKind: "peer",
  environmentId: EnvironmentId.make("environment-usage-test"),
  peerTokenId: "peer-usage-test",
  capabilities: new Set(["subagent:check"]),
  issuedAt: 1,
  expiresAt: null,
};

const withPlanUsagePollingDisabled = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      process.env.T3_DISABLE_PLAN_USAGE_POLLING = "1";
    }),
    () => effect,
    () =>
      Effect.sync(() => {
        if (originalDisabled === undefined) {
          delete process.env.T3_DISABLE_PLAN_USAGE_POLLING;
        } else {
          process.env.T3_DISABLE_PLAN_USAGE_POLLING = originalDisabled;
        }
      }),
  );

const makeLayer = () =>
  UsageToolkitRegistrationLive.pipe(
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provide(
      PlanUsageSnapshot.layerTest({
        updatedAt: "2026-07-10T08:00:00.000Z",
        providers: [],
      }),
    ),
    Layer.provide(ServerSettingsService.layerTest(DEFAULT_SERVER_SETTINGS)),
  );

const callGetUsage = (
  arguments_: Record<string, unknown>,
  scope: McpInvocationContext.McpInvocationScope = invocation,
) =>
  Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    return yield* server
      .callTool({ name: "t3_get_usage", arguments: arguments_ })
      .pipe(
        Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
        Effect.provideService(McpSchema.McpServerClient, client),
      );
  }).pipe(Effect.provide(makeLayer()), withPlanUsagePollingDisabled);

it.effect("returns an empty usage snapshot when plan usage polling is disabled", () =>
  Effect.gen(function* () {
    const result = yield* callGetUsage({});

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      providers: [],
    });
    expect(typeof result.structuredContent?.updatedAt).toBe("string");
  }),
);

it.effect("accepts a provider instance id for scoped usage reads", () =>
  Effect.gen(function* () {
    const result = yield* callGetUsage({
      providerInstanceId: ProviderInstanceId.make("codex"),
    });

    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({
      providers: [],
    });
  }),
);

it.effect("rejects peer-scoped credentials", () =>
  Effect.gen(function* () {
    const result = yield* callGetUsage({}, peerInvocation);

    expect(result.isError).toBe(true);
    const content = result.content?.[0];
    expect(content?.type).toBe("text");
    if (content?.type !== "text") throw new Error("Expected text error content.");
    expect(content.text).toContain("MCP credential is not authorized for usage");
  }),
);
