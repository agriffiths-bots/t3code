import type { ProviderDriverKind, ProviderInstanceId, ServerProvider } from "@t3tools/contracts";
import type * as Stream from "effect/Stream";
import type { ServerProviderShape } from "./Services/ServerProvider.ts";

const RETIRED_BUILT_IN_MODEL_SLUGS_BY_DRIVER: Readonly<Record<string, ReadonlySet<string>>> = {
  claudeAgent: new Set(["claude-opus-4-8"]),
};

export const isRetiredBuiltInProviderModel = (
  driver: ProviderDriverKind,
  model: ServerProvider["models"][number],
): boolean =>
  model.isCustom === false &&
  (RETIRED_BUILT_IN_MODEL_SLUGS_BY_DRIVER[driver]?.has(model.slug) ?? false);

export type ProviderSnapshotSource = {
  /**
   * Routing key — uniquely identifies this instance in the aggregated
   * snapshot list. Two different snapshot sources may share the same
   * driver kind (multiple instances of the same driver).
   */
  readonly instanceId: ProviderInstanceId;
  /** Driver implementation kind. */
  readonly driverKind: ProviderDriverKind;
  readonly getSnapshot: ServerProviderShape["getSnapshot"];
  readonly refresh: ServerProviderShape["refresh"];
  readonly streamChanges: Stream.Stream<ServerProvider>;
};
