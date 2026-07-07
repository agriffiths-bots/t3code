import { ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";

import {
  SubagentDispatchLimiter,
  type SubagentDispatchLease,
  layerTest,
} from "./SubagentDispatchLimiter.ts";

describe("SubagentDispatchLimiter", () => {
  it.effect("queues acquisition until a child-bound lease is released", () =>
    Effect.gen(function* () {
      const limiter = yield* SubagentDispatchLimiter;
      const first = yield* limiter.acquire;
      const secondAcquired = yield* Deferred.make<SubagentDispatchLease>();
      const secondFiber = yield* limiter.acquire.pipe(
        Effect.flatMap((lease) => Deferred.succeed(secondAcquired, lease)),
        Effect.forkChild,
      );

      yield* Effect.yieldNow;
      expect(Option.isNone(yield* Deferred.poll(secondAcquired))).toBe(true);

      yield* limiter.bindChild(first, ThreadId.make("child-1"));
      yield* limiter.releaseForChild(ThreadId.make("child-1"));

      const second = yield* Deferred.await(secondAcquired);
      expect(second.id).not.toBe(first.id);
      yield* limiter.release(second);
      yield* Fiber.join(secondFiber);
    }).pipe(Effect.provide(layerTest(1))),
  );

  it.effect("ignores duplicate child releases", () =>
    Effect.gen(function* () {
      const limiter = yield* SubagentDispatchLimiter;
      const lease = yield* limiter.acquire;
      yield* limiter.bindChild(lease, ThreadId.make("child-1"));

      yield* limiter.releaseForChild(ThreadId.make("child-1"));
      yield* limiter.releaseForChild(ThreadId.make("child-1"));

      const next = yield* limiter.acquire;
      yield* limiter.release(next);
    }).pipe(Effect.provide(layerTest(1))),
  );
});
