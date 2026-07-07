import { expect, it } from "@effect/vitest";
import { DEFAULT_SERVER_SETTINGS, ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { McpSchema, McpServer } from "effect/unstable/ai";

import { ServerSettingsService } from "../../../serverSettings.ts";
import { UsageToolkitRegistrationLive } from "../../McpHttpServer.ts";

const client = McpSchema.McpServerClient.of({
  clientId: 1,
  initializePayload: {
    protocolVersion: "2025-03-26",
    capabilities: {},
    clientInfo: { name: "usage-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

const originalDisabled = process.env.T3_DISABLE_PLAN_USAGE_POLLING;

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
    Layer.provide(ServerSettingsService.layerTest(DEFAULT_SERVER_SETTINGS)),
  );

const callGetUsage = (arguments_: Record<string, unknown>) =>
  Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    return yield* server
      .callTool({ name: "t3_get_usage", arguments: arguments_ })
      .pipe(Effect.provideService(McpSchema.McpServerClient, client));
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
