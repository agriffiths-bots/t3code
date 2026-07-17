import { DEFAULT_SERVER_SETTINGS } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";

import * as ServerSettings from "../serverSettings.ts";
import { makeLayer, PlanUsageSnapshotStore } from "./PlanUsageSnapshot.ts";

it.effect("serves the current snapshot without blocking while the background probe warms", () =>
  Effect.gen(function* () {
    let releaseProbe!: () => void;
    const blockedProbe = new Promise<void>((resolve) => {
      releaseProbe = resolve;
    });
    const layer = makeLayer({
      load: async () => {
        await blockedProbe;
        return { updatedAt: "2026-07-10T08:01:00.000Z", providers: [] };
      },
    }).pipe(Layer.provide(ServerSettings.layerTest(DEFAULT_SERVER_SETTINGS)));

    const scope = yield* Effect.scope;
    const store = yield* Layer.buildWithScope(layer, scope).pipe(
      Effect.map((context) => Context.get(context, PlanUsageSnapshotStore)),
    );
    assert.deepEqual((yield* store.current).providers, []);

    releaseProbe();
    yield* Effect.yieldNow;
    yield* Effect.yieldNow;
    assert.equal((yield* store.current).updatedAt, "2026-07-10T08:01:00.000Z");
  }),
);

it.effect("refreshes plan usage in the background without request-driven probes", () => {
  let probes = 0;
  const layer = makeLayer({
    refreshIntervalMs: 120_000,
    load: async () => {
      probes += 1;
      return {
        updatedAt: `2026-07-10T08:0${probes}:00.000Z`,
        providers: [],
      };
    },
  }).pipe(Layer.provide(ServerSettings.layerTest(DEFAULT_SERVER_SETTINGS)));

  return Effect.gen(function* () {
    const store = yield* PlanUsageSnapshotStore;
    yield* Effect.yieldNow;
    yield* Effect.yieldNow;
    assert.equal(probes, 1);
    assert.equal((yield* store.current).updatedAt, "2026-07-10T08:01:00.000Z");

    yield* TestClock.adjust("119999 millis");
    assert.equal(probes, 1);
    yield* TestClock.adjust("1 millis");
    yield* Effect.yieldNow;
    assert.equal(probes, 2);
  }).pipe(Effect.provide(layer));
});

it.effect("retains the last good snapshot when a background refresh fails", () => {
  let probes = 0;
  const layer = makeLayer({
    refreshIntervalMs: 120_000,
    load: async () => {
      probes += 1;
      return probes === 1
        ? {
            updatedAt: "2026-07-10T08:01:00.000Z",
            providers: [
              {
                provider: "codex" as const,
                plan: "pro",
                windows: [
                  {
                    id: "codex:codex:codex-weekly",
                    provider: "codex" as const,
                    kind: "weekly",
                    title: "Codex weekly",
                    usedPercent: 20,
                    resetAt: null,
                    used: null,
                    limit: null,
                    unit: null,
                    severity: null,
                  },
                ],
              },
            ],
          }
        : {
            updatedAt: "2026-07-10T08:03:00.000Z",
            providers: [
              {
                provider: "codex" as const,
                plan: "pro",
                windows: [
                  {
                    id: "codex:codex:codex-weekly",
                    provider: "codex" as const,
                    kind: "weekly",
                    title: "Codex weekly",
                    usedPercent: 20,
                    resetAt: null,
                    used: null,
                    limit: null,
                    unit: null,
                    severity: null,
                    staleAt: "2026-07-10T08:01:00.000Z",
                  },
                ],
              },
            ],
          };
    },
  }).pipe(Layer.provide(ServerSettings.layerTest(DEFAULT_SERVER_SETTINGS)));

  return Effect.gen(function* () {
    const store = yield* PlanUsageSnapshotStore;
    yield* Effect.yieldNow;
    yield* Effect.yieldNow;
    yield* TestClock.adjust("120000 millis");
    yield* Effect.yieldNow;
    assert.equal(probes, 2);
    const current = yield* store.current;
    assert.equal(current.providers[0]?.windows[0]?.usedPercent, 20);
    assert.equal(current.providers[0]?.windows[0]?.staleAt, "2026-07-10T08:01:00.000Z");
  }).pipe(Effect.provide(layer));
});
