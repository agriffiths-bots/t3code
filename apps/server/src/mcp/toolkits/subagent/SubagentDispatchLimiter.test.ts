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

  it.effect("counts reconciled children against the cap before granting queued spawns", () =>
    Effect.gen(function* () {
      const limiter = yield* SubagentDispatchLimiter;
      yield* limiter.seedChild(ThreadId.make("child-1"));
      yield* limiter.seedChild(ThreadId.make("child-2"));
      yield* limiter.seedChild(ThreadId.make("child-3"));

      const acquired = yield* Deferred.make<SubagentDispatchLease>();
      const fiber = yield* limiter.acquire.pipe(
        Effect.flatMap((lease) => Deferred.succeed(acquired, lease)),
        Effect.forkChild,
      );

      yield* Effect.yieldNow;
      expect(Option.isNone(yield* Deferred.poll(acquired))).toBe(true);

      yield* limiter.releaseForChild(ThreadId.make("child-1"));
      yield* Effect.yieldNow;
      expect(Option.isNone(yield* Deferred.poll(acquired))).toBe(true);

      yield* limiter.releaseForChild(ThreadId.make("child-2"));
      const lease = yield* Deferred.await(acquired);
      yield* limiter.release(lease);
      yield* Fiber.join(fiber);
    }).pipe(Effect.provide(layerTest(2))),
  );

  it.effect("removes an interrupted queued acquire before granting the next waiter", () =>
    Effect.gen(function* () {
      const limiter = yield* SubagentDispatchLimiter;
      const first = yield* limiter.acquire;
      const interruptedFiber = yield* limiter.acquire.pipe(Effect.forkChild);

      yield* Effect.yieldNow;
      yield* Fiber.interrupt(interruptedFiber);
      yield* limiter.release(first);

      const next = yield* limiter.acquire;
      yield* limiter.release(next);
    }).pipe(Effect.provide(layerTest(1))),
  );
});
