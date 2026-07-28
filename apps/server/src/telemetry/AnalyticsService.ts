/**
 * Anonymous PostHog telemetry service.
 *
 * Persists an installation-scoped anonymous identifier, buffers events in
 * memory, and flushes batches over Effect's HTTP client.
 *
 * @module AnalyticsService
 */
import { HostProcessArchitecture, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";

import packageJson from "../../package.json" with { type: "json" };
import * as ServerConfig from "../config.ts";
import { getTelemetryIdentifier } from "./Identify.ts";

interface BufferedAnalyticsEvent {
  readonly event: string;
  readonly properties?: Readonly<Record<string, unknown>>;
  readonly capturedAt: string;
}

const TelemetryEnvConfig = Config.all({
  posthogKey: Config.string("T3CODE_POSTHOG_KEY").pipe(
    Config.withDefault("phc_XOWci4oZP4VvLiEyrFqkFjP4CZn55mjYYBMREK5Wd6m"),
  ),
  posthogHost: Config.string("T3CODE_POSTHOG_HOST").pipe(
    Config.withDefault("https://us.i.posthog.com"),
  ),
  enabled: Config.boolean("T3CODE_TELEMETRY_ENABLED").pipe(Config.withDefault(true)),
  flushBatchSize: Config.number("T3CODE_TELEMETRY_FLUSH_BATCH_SIZE").pipe(Config.withDefault(20)),
  maxBufferedEvents: Config.number("T3CODE_TELEMETRY_MAX_BUFFERED_EVENTS").pipe(
    Config.withDefault(1_000),
  ),
  doNotTrack: Config.string("DO_NOT_TRACK").pipe(Config.option),
  wslDistroName: Config.string("WSL_DISTRO_NAME").pipe(Config.option),
});

const FLUSH_INTERVAL_MS = 1_000;
const SEND_TIMEOUT_MS = 10_000;
const SHUTDOWN_FLUSH_TIMEOUT_MS = 2_000;
const RETRY_BASE_DELAY_MS = 1_000;
const RETRY_MAX_DELAY_MS = 60_000;
const DO_NOT_TRACK_VALUES = new Set(["1", "true", "yes", "on"]);

interface TelemetryRetryState {
  readonly consecutiveFailures: number;
  readonly retryAtMs: number | null;
}

const INITIAL_RETRY_STATE: TelemetryRetryState = {
  consecutiveFailures: 0,
  retryAtMs: null,
};

export function resolveTelemetryEnabled(
  configuredEnabled: boolean,
  doNotTrack: Option.Option<string>,
): boolean {
  return (
    configuredEnabled &&
    !Option.exists(doNotTrack, (value) => DO_NOT_TRACK_VALUES.has(value.trim().toLowerCase()))
  );
}

export function telemetryRetryDelayMs(failureCount: number): number {
  return Math.min(RETRY_BASE_DELAY_MS * 2 ** Math.max(0, failureCount - 1), RETRY_MAX_DELAY_MS);
}

export class AnalyticsService extends Context.Service<
  AnalyticsService,
  {
    /** Record an anonymous event for best-effort buffered delivery. */
    readonly record: (
      event: string,
      properties?: Readonly<Record<string, unknown>>,
    ) => Effect.Effect<void>;

    /** Flush all currently queued telemetry events. */
    readonly flush: Effect.Effect<void>;
  }
>()("t3/telemetry/AnalyticsService") {
  /** No-op layer for callers that intentionally disable telemetry. */
  static readonly layerTest = Layer.succeed(
    AnalyticsService,
    AnalyticsService.of({
      record: () => Effect.void,
      flush: Effect.void,
    }),
  );
}

export const make = Effect.gen(function* () {
  const telemetryConfig = yield* TelemetryEnvConfig;
  const enabled = resolveTelemetryEnabled(telemetryConfig.enabled, telemetryConfig.doNotTrack);
  if (!enabled) {
    return AnalyticsService.of({
      record: () => Effect.void,
      flush: Effect.void,
    });
  }

  const httpClient = yield* HttpClient.HttpClient;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const identifier = yield* getTelemetryIdentifier;
  const bufferRef = yield* Ref.make<ReadonlyArray<BufferedAnalyticsEvent>>([]);
  const retryStateRef = yield* Ref.make<TelemetryRetryState>(INITIAL_RETRY_STATE);
  const flushSemaphore = yield* Semaphore.make(1);
  const clientType = serverConfig.mode === "desktop" ? "desktop-app" : "cli-web-client";
  const hostPlatform = yield* HostProcessPlatform;
  const hostArchitecture = yield* HostProcessArchitecture;

  const enqueueBufferedEvent = (event: string, properties?: Readonly<Record<string, unknown>>) =>
    Effect.flatMap(DateTime.now, (now) =>
      Ref.modify(bufferRef, (current) => {
        const appended = [
          ...current,
          {
            event,
            ...(properties ? { properties } : {}),
            capturedAt: DateTime.formatIso(now),
          } satisfies BufferedAnalyticsEvent,
        ];

        const next =
          appended.length > telemetryConfig.maxBufferedEvents
            ? appended.slice(appended.length - telemetryConfig.maxBufferedEvents)
            : appended;

        return [
          {
            size: next.length,
            dropped: next.length !== appended.length,
          } as const,
          next,
        ] as const;
      }),
    );

  const sendBatch = (events: ReadonlyArray<BufferedAnalyticsEvent>) =>
    Effect.gen(function* () {
      if (!identifier) return;

      const payload = {
        api_key: telemetryConfig.posthogKey,
        batch: events.map((event) => ({
          event: event.event,
          distinct_id: identifier,
          properties: {
            ...event.properties,
            $process_person_profile: false,
            platform: hostPlatform,
            wsl: Option.getOrUndefined(telemetryConfig.wslDistroName),
            arch: hostArchitecture,
            t3CodeVersion: packageJson.version,
            clientType,
          },
          timestamp: event.capturedAt,
        })),
      };

      yield* HttpClientRequest.post(`${telemetryConfig.posthogHost}/batch/`).pipe(
        HttpClientRequest.bodyJson(payload),
        Effect.flatMap(httpClient.execute),
        Effect.flatMap(HttpClientResponse.filterStatusOk),
        Effect.timeout(Duration.millis(SEND_TIMEOUT_MS)),
      );
    }).pipe(Effect.withTracerEnabled(false));

  const flushUnlocked = (ignoreBackoff = false) =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const retryState = yield* Ref.get(retryStateRef);
      if (!ignoreBackoff && retryState.retryAtMs !== null && now < retryState.retryAtMs) {
        return;
      }

      while (true) {
        const batch = yield* Ref.modify(bufferRef, (current) => {
          if (current.length === 0) {
            return [[] as ReadonlyArray<BufferedAnalyticsEvent>, current] as const;
          }
          const nextBatch = current.slice(0, telemetryConfig.flushBatchSize);
          const remaining = current.slice(nextBatch.length);
          return [nextBatch, remaining] as const;
        });

        if (batch.length === 0) {
          yield* Ref.set(retryStateRef, INITIAL_RETRY_STATE);
          return;
        }

        const delivered = yield* sendBatch(batch).pipe(
          Effect.as(true),
          Effect.catch((cause) =>
            Effect.gen(function* () {
              yield* Ref.update(bufferRef, (current) => [...batch, ...current]);
              const failedAtMs = yield* Clock.currentTimeMillis;
              const nextRetryState = yield* Ref.modify(retryStateRef, (current) => {
                const consecutiveFailures = current.consecutiveFailures + 1;
                const retryInMs = telemetryRetryDelayMs(consecutiveFailures);
                const next = {
                  consecutiveFailures,
                  retryAtMs: failedAtMs + retryInMs,
                } satisfies TelemetryRetryState;
                return [{ retryInMs, consecutiveFailures }, next] as const;
              });
              yield* Effect.logWarning("Failed to flush telemetry; backing off before retry.", {
                cause,
                retryInMs: nextRetryState.retryInMs,
                consecutiveFailures: nextRetryState.consecutiveFailures,
              });
              return false;
            }),
          ),
        );
        if (!delivered) {
          return;
        }
        yield* Ref.set(retryStateRef, INITIAL_RETRY_STATE);
      }
    });

  const flush: AnalyticsService["Service"]["flush"] = flushSemaphore.withPermit(flushUnlocked());

  const record: AnalyticsService["Service"]["record"] = Effect.fn("AnalyticsService.record")(
    function* (event, properties) {
      if (!identifier) return;

      const enqueueResult = yield* enqueueBufferedEvent(event, properties);
      if (enqueueResult.dropped) {
        yield* Effect.logDebug("analytics buffer full; dropping oldest event", {
          size: enqueueResult.size,
          event,
        });
      }
    },
  );

  yield* Effect.forever(
    Effect.gen(function* () {
      const retryState = yield* Ref.get(retryStateRef);
      const now = yield* Clock.currentTimeMillis;
      const delayMs =
        retryState.retryAtMs === null ? FLUSH_INTERVAL_MS : Math.max(1, retryState.retryAtMs - now);
      yield* Effect.sleep(Duration.millis(delayMs));
      yield* flush;
    }),
    { disableYield: true },
  ).pipe(Effect.forkScoped);

  yield* Effect.addFinalizer(() =>
    flushSemaphore.withPermit(flushUnlocked(true)).pipe(
      Effect.timeoutOrElse({
        duration: Duration.millis(SHUTDOWN_FLUSH_TIMEOUT_MS),
        orElse: () =>
          Effect.logWarning("Timed out during the final best-effort telemetry flush.", {
            timeoutMs: SHUTDOWN_FLUSH_TIMEOUT_MS,
          }),
      }),
      Effect.interruptible,
    ),
  );

  return AnalyticsService.of({ record, flush });
});

export const layer = Layer.effect(AnalyticsService, make);

export const layerTest = AnalyticsService.layerTest;
