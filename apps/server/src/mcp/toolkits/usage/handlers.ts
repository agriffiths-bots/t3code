import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { ServerSettingsService } from "../../../serverSettings.ts";
import { loadPlanUsageSnapshot } from "../../../usage/PlanUsage.ts";
import { GetUsageToolError, UsageToolkit, type GetUsageInput } from "./tools.ts";

const toToolError = (message: string, error: unknown): GetUsageToolError =>
  new GetUsageToolError({
    message: error instanceof Error ? `${message}: ${error.message}` : message,
  });

const makeHandlers = Effect.fn("UsageToolkit.makeHandlers")(function* () {
  const serverSettings = yield* ServerSettingsService;

  const getUsage = (input: GetUsageInput) =>
    serverSettings.getSettings.pipe(
      Effect.mapError((error) => toToolError("Failed to read server settings", error)),
      Effect.flatMap((settings) =>
        Effect.tryPromise({
          try: () =>
            loadPlanUsageSnapshot({
              settings,
              providerInstanceId: input.providerInstanceId ?? null,
            }),
          catch: (error) => toToolError("Failed to load provider usage", error),
        }),
      ),
    );

  return {
    t3_get_usage: getUsage,
  } satisfies Parameters<typeof UsageToolkit.toLayer>[0];
});

export const UsageToolkitHandlersLive = Layer.unwrap(
  makeHandlers().pipe(Effect.map((handlers) => UsageToolkit.toLayer(handlers))),
);
