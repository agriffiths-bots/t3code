import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { PlanUsageSnapshotStore } from "../../../usage/PlanUsageSnapshot.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { GetUsageToolError, UsageToolkit, type GetUsageInput } from "./tools.ts";

const toToolError = (message: string, error: unknown): GetUsageToolError =>
  new GetUsageToolError({
    message: error instanceof Error ? `${message}: ${error.message}` : message,
  });

const makeHandlers = Effect.fn("UsageToolkit.makeHandlers")(function* () {
  const planUsage = yield* PlanUsageSnapshotStore;

  const getUsage = (input: GetUsageInput) =>
    McpInvocationContext.requireProviderMcpCapability("thread-management").pipe(
      Effect.mapError((error) => toToolError("MCP credential is not authorized for usage", error)),
      Effect.andThen(planUsage.read(input.providerInstanceId ?? null)),
    );

  return {
    t3_get_usage: getUsage,
  } satisfies Parameters<typeof UsageToolkit.toLayer>[0];
});

export const UsageToolkitHandlersLive = Layer.unwrap(
  makeHandlers().pipe(Effect.map((handlers) => UsageToolkit.toLayer(handlers))),
);
