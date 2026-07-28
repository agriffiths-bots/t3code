import type { ProviderRuntimeEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { increment, providerRuntimeEventBindingDropsTotal } from "../observability/Metrics.ts";
import {
  getCapturedProviderRuntimeEventBinding,
  type CapturedProviderRuntimeEventBinding,
} from "../provider/runtimeEventBindingRegistry.ts";

export type ProviderRuntimeEventBinding = CapturedProviderRuntimeEventBinding;

export type ProviderRuntimeEventConsumer = "CheckpointReactor" | "ProviderRuntimeIngestion";

function recordBindingDrop(input: {
  readonly consumer: ProviderRuntimeEventConsumer;
  readonly event: ProviderRuntimeEvent;
  readonly outcome: "no-binding" | "target-mismatch";
}) {
  return increment(providerRuntimeEventBindingDropsTotal, {
    consumer: input.consumer,
    outcome: input.outcome,
    eventType: input.event.type,
    provider: input.event.provider,
  });
}

/**
 * Read authority captured by ProviderService when the event entered its
 * canonical stream. This avoids a teardown race between independent async
 * consumers without adding a provider-controlled envelope field.
 */
export const resolveProviderRuntimeEventBinding = Effect.fn("resolveProviderRuntimeEventBinding")(
  function* (input: {
    readonly consumer: ProviderRuntimeEventConsumer;
    readonly event: ProviderRuntimeEvent;
  }): Effect.fn.Return<ProviderRuntimeEventBinding | undefined> {
    const binding = getCapturedProviderRuntimeEventBinding(input.event);

    if (binding === undefined) {
      yield* recordBindingDrop({ ...input, outcome: "no-binding" });
      yield* Effect.logWarning("provider runtime event without tracked binding dropped", {
        outcome: "no-binding",
        consumer: input.consumer,
        eventId: input.event.eventId,
        eventType: input.event.type,
        claimedProvider: input.event.provider,
        claimedProviderInstanceId: input.event.providerInstanceId ?? null,
        claimedThreadId: input.event.threadId,
      });
      return undefined;
    }

    if (
      binding.providerInstanceId === undefined ||
      input.event.providerInstanceId === undefined ||
      binding.provider !== input.event.provider ||
      binding.providerInstanceId !== input.event.providerInstanceId
    ) {
      yield* recordBindingDrop({ ...input, outcome: "target-mismatch" });
      yield* Effect.logError("provider runtime event target mismatch dropped", {
        outcome: "target-mismatch",
        consumer: input.consumer,
        eventId: input.event.eventId,
        eventType: input.event.type,
        claimedProvider: input.event.provider,
        claimedProviderInstanceId: input.event.providerInstanceId ?? null,
        claimedThreadId: input.event.threadId,
        trackedProvider: binding.provider,
        trackedProviderInstanceId: binding.providerInstanceId ?? null,
        trackedThreadId: binding.threadId,
      });
      return undefined;
    }

    return binding;
  },
);
