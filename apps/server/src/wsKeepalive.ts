import type { ServerConfigStreamEvent } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Stream from "effect/Stream";

export const WS_KEEPALIVE_INTERVAL_MS = 30_000;

/**
 * Whether to emit websocket keepalive `heartbeat` frames on this
 * `subscribeServerConfig` subscription. Heartbeats are a new
 * `ServerConfigStreamEvent` union variant, so they must only be sent to clients
 * that declared support — otherwise an older/version-skewed client's schema
 * decoder fails on the unknown variant and tears down the config subscription.
 */
export function shouldSendServerConfigHeartbeat(input: {
  readonly supportsHeartbeat?: boolean | undefined;
}): boolean {
  return input.supportsHeartbeat === true;
}

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
