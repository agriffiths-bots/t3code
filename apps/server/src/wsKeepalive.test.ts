import { describe, expect, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { makeServerConfigHeartbeatStream, WS_KEEPALIVE_INTERVAL_MS } from "./wsKeepalive.ts";

describe("websocket keepalive", () => {
  it.effect("emits application frames well before Cloudflare's idle websocket timeout", () =>
    Effect.gen(function* () {
      expect(WS_KEEPALIVE_INTERVAL_MS).toBeLessThan(100_000);

      const eventsFiber = yield* makeServerConfigHeartbeatStream().pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* TestClock.adjust(Duration.millis(WS_KEEPALIVE_INTERVAL_MS));
      yield* TestClock.adjust(Duration.millis(WS_KEEPALIVE_INTERVAL_MS));

      expect(Array.from(yield* Fiber.join(eventsFiber))).toEqual([
        { version: 1, type: "heartbeat" },
        { version: 1, type: "heartbeat" },
      ]);
    }).pipe(Effect.provide(TestClock.layer())),
  );
});
