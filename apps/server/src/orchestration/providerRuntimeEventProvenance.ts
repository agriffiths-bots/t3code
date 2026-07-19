import type { ProviderRuntimeEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

/**
 * ProviderRuntimeEvent currently carries only target-selected identifiers.
 * Keep this fail-closed until the provider adapter/service supplies an
 * immutable, session-specific source identity in a trusted envelope.
 */
export function hasIndependentRuntimeEventSourceProvenance(_event: ProviderRuntimeEvent): boolean {
  return false;
}

export function warnDroppedUnprovenRuntimeEvent(
  consumer: "CheckpointReactor" | "ProviderRuntimeIngestion",
  event: ProviderRuntimeEvent,
) {
  return Effect.logWarning("provider runtime event dropped without source provenance", {
    consumer,
    eventKind: event.type,
    providerInstanceId: event.providerInstanceId ?? null,
    targetThreadId: event.threadId,
  });
}
