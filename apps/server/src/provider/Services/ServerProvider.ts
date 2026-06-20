import type { ServerProvider } from "@t3tools/contracts";
import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";
import type { ProviderMaintenanceCapabilities } from "../providerMaintenance.ts";

export interface ServerProviderShape {
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  /** The pre-probe pending placeholder captured at construction time. */
  readonly initialSnapshot: ServerProvider;
  readonly getSnapshot: Effect.Effect<ServerProvider>;
  readonly refresh: Effect.Effect<ServerProvider>;
  readonly streamChanges: Stream.Stream<ServerProvider>;
}
