// @effect-diagnostics globalDate:off - Usage snapshots carry provider-facing ISO timestamps.
import {
  DEFAULT_SERVER_SETTINGS,
  type PlanUsageSnapshot,
  type ProviderInstanceId,
  type ServerSettings as ServerSettingsType,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

import * as ServerSettings from "../serverSettings.ts";
import { loadPlanUsageSnapshot, scopePlanUsageSnapshot } from "./PlanUsage.ts";

export const PLAN_USAGE_REFRESH_INTERVAL_MS = 120_000;

class PlanUsageRefreshError extends Data.TaggedError("PlanUsageRefreshError")<{
  readonly cause: unknown;
}> {}

export function emptyPlanUsageSnapshot(now = Date.now()): PlanUsageSnapshot {
  return { updatedAt: new Date(now).toISOString(), providers: [] };
}

function preserveLastGoodProviders(
  previous: PlanUsageSnapshot,
  next: PlanUsageSnapshot,
): PlanUsageSnapshot {
  return {
    ...next,
    providers: next.providers.map((provider) => {
      if (provider.windows.length === 0 || provider.windows.some((window) => !window.staleAt)) {
        return provider;
      }
      const ids = new Set(provider.windows.map((window) => window.id));
      const retained = previous.providers.find((candidate) =>
        candidate.windows.some((window) => ids.has(window.id)),
      );
      if (!retained) return provider;
      const staleAt = provider.windows[0]?.staleAt;
      return {
        ...retained,
        windows: retained.windows.map((window) => ({
          ...window,
          ...(staleAt ? { staleAt } : {}),
        })),
      };
    }),
  };
}

function markPlanUsageSnapshotStale(
  snapshot: PlanUsageSnapshot,
  staleAt: string,
): PlanUsageSnapshot {
  return {
    ...snapshot,
    providers: snapshot.providers.map((provider) => ({
      ...provider,
      windows: provider.windows.map((window) => ({
        ...window,
        staleAt: window.staleAt ?? staleAt,
      })),
    })),
  };
}

export class PlanUsageSnapshotStore extends Context.Service<
  PlanUsageSnapshotStore,
  {
    readonly current: Effect.Effect<PlanUsageSnapshot>;
    readonly read: (
      providerInstanceId?: ProviderInstanceId | null,
    ) => Effect.Effect<PlanUsageSnapshot>;
    readonly changes: Stream.Stream<PlanUsageSnapshot>;
  }
>()("t3/usage/PlanUsageSnapshot/PlanUsageSnapshotStore") {}

export const makeLayer = (options?: {
  readonly refreshIntervalMs?: number;
  readonly load?: (settings: ServerSettingsType) => Promise<PlanUsageSnapshot>;
}) =>
  Layer.effect(
    PlanUsageSnapshotStore,
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettings.ServerSettingsService;
      const state = yield* SubscriptionRef.make({
        snapshot: emptyPlanUsageSnapshot(),
        settings: DEFAULT_SERVER_SETTINGS,
      });
      const refresh = Effect.gen(function* () {
        const settings = yield* serverSettings.getSettings;
        const next = yield* Effect.tryPromise({
          try: () => (options?.load ? options.load(settings) : loadPlanUsageSnapshot({ settings })),
          catch: (cause) => new PlanUsageRefreshError({ cause }),
        });
        const previous = yield* SubscriptionRef.get(state);
        yield* SubscriptionRef.set(state, {
          snapshot: preserveLastGoodProviders(previous.snapshot, next),
          settings,
        });
      }).pipe(
        Effect.catch((cause) =>
          Effect.gen(function* () {
            const staleAt = DateTime.formatIso(yield* DateTime.now);
            yield* SubscriptionRef.update(state, (current) => ({
              ...current,
              snapshot: markPlanUsageSnapshotStale(current.snapshot, staleAt),
            }));
            yield* Effect.logWarning("Plan usage background refresh failed", { cause });
          }),
        ),
      );
      const refreshInterval = Effect.sleep(
        `${options?.refreshIntervalMs ?? PLAN_USAGE_REFRESH_INTERVAL_MS} millis`,
      );
      const refreshLoop = Effect.forever(refresh.pipe(Effect.andThen(refreshInterval)));
      yield* Effect.forkScoped(refreshLoop);

      const read = Effect.fn("PlanUsageSnapshotStore.read")(function* (
        providerInstanceId?: ProviderInstanceId | null,
      ) {
        const current = yield* SubscriptionRef.get(state);
        return providerInstanceId
          ? scopePlanUsageSnapshot(current.snapshot, providerInstanceId, current.settings)
          : current.snapshot;
      });

      return PlanUsageSnapshotStore.of({
        current: SubscriptionRef.get(state).pipe(Effect.map((state) => state.snapshot)),
        read,
        changes: SubscriptionRef.changes(state).pipe(Stream.map((state) => state.snapshot)),
      });
    }),
  );

export const layer = makeLayer();

export const layerTest = (snapshot: PlanUsageSnapshot) =>
  Layer.succeed(
    PlanUsageSnapshotStore,
    PlanUsageSnapshotStore.of({
      current: Effect.succeed(snapshot),
      read: (providerInstanceId) =>
        Effect.succeed(
          providerInstanceId
            ? {
                updatedAt: snapshot.updatedAt,
                providers: snapshot.providers.flatMap((provider) => {
                  const windows = provider.windows.filter((window) =>
                    window.id.includes(`:${providerInstanceId}:`),
                  );
                  return windows.length > 0 ? [{ ...provider, windows }] : [];
                }),
              }
            : snapshot,
        ),
      changes: Stream.empty,
    }),
  );
