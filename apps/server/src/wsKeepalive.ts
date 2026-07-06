import type { ServerConfigStreamEvent } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Stream from "effect/Stream";

export const WS_KEEPALIVE_INTERVAL_MS = 30_000;

export function makeServerConfigHeartbeatEvent(): ServerConfigStreamEvent {
  return {
    version: 1,
    type: "heartbeat",
  };
}

export function makeServerConfigHeartbeatStream(): Stream.Stream<ServerConfigStreamEvent> {
  return Stream.tick(Duration.millis(WS_KEEPALIVE_INTERVAL_MS)).pipe(
    Stream.drop(1),
    Stream.map(() => makeServerConfigHeartbeatEvent()),
  );
}
