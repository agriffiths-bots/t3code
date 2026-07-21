import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Tracer from "effect/Tracer";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpServer from "effect/unstable/http/HttpServer";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";

import * as ServerConfig from "../config.ts";
import { getTelemetryIdentifier } from "./Identify.ts";
import * as AnalyticsService from "./AnalyticsService.ts";

it("honors explicit telemetry opt-out and standard DO_NOT_TRACK values", () => {
  assert.equal(AnalyticsService.resolveTelemetryEnabled(false, Option.none()), false);
  for (const value of ["1", "true", "TRUE", "yes", "on"]) {
    assert.equal(AnalyticsService.resolveTelemetryEnabled(true, Option.some(value)), false);
  }
  assert.equal(AnalyticsService.resolveTelemetryEnabled(true, Option.some("0")), true);
  assert.equal(AnalyticsService.resolveTelemetryEnabled(true, Option.none()), true);
});

it("uses capped exponential telemetry retry delays", () => {
  assert.equal(AnalyticsService.telemetryRetryDelayMs(1), 1_000);
  assert.equal(AnalyticsService.telemetryRetryDelayMs(2), 2_000);
  assert.equal(AnalyticsService.telemetryRetryDelayMs(3), 4_000);
  assert.equal(AnalyticsService.telemetryRetryDelayMs(20), 60_000);
});

interface RecordedBatchRequest {
  readonly path: string;
  readonly body: {
    readonly batch?: ReadonlyArray<{
      readonly event?: string;
      readonly properties?: {
        readonly index?: number;
        readonly clientType?: string;
      };
    }>;
  } | null;
}

interface RecordedBatchBody {
  readonly batch: ReadonlyArray<{
    readonly event?: string;
    readonly properties?: {
      readonly index?: number;
      readonly clientType?: string;
    };
  }>;
}

it.layer(NodeServices.layer)("AnalyticsService test", (it) => {
  it.effect("flush drains all buffered events across multiple batches", () =>
    Effect.gen(function* () {
      const capturedRequests: Array<RecordedBatchRequest> = [];
      const spanNames: string[] = [];
      const tracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options);
          const end = span.end.bind(span);
          span.end = (endTime, exit) => {
            end(endTime, exit);
            spanNames.push(span.name);
          };
          return span;
        },
      });
      const serverConfigLayer = ServerConfig.ServerConfig.layerTest(process.cwd(), {
        prefix: "t3-telemetry-base-",
      });

      const telemetryLayer = AnalyticsService.layer.pipe(Layer.provideMerge(serverConfigLayer));
      const configLayer = ConfigProvider.layer(
        ConfigProvider.fromUnknown({
          T3CODE_TELEMETRY_ENABLED: true,
          T3CODE_POSTHOG_KEY: "phc_test_key",
          T3CODE_POSTHOG_HOST: "",
          T3CODE_TELEMETRY_FLUSH_BATCH_SIZE: 20,
        }),
      );
      const batchServerLayer = HttpServer.serve(
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          if (request.method !== "POST") {
            return HttpServerResponse.empty({ status: 404 });
          }

          const payload = yield* request.json.pipe(
            Effect.map((body) => body as RecordedBatchRequest["body"]),
            Effect.orElseSucceed(() => null),
          );

          capturedRequests.push({ path: request.url, body: payload });

          return HttpServerResponse.jsonUnsafe({});
        }),
      );
      const runtimeLayer = telemetryLayer.pipe(
        Layer.provide(configLayer),
        Layer.provideMerge(NodeHttpServer.layerTest),
      );

      yield* Effect.gen(function* () {
        yield* Layer.launch(batchServerLayer).pipe(Effect.forkScoped);
        const telemetryIdentifier = yield* getTelemetryIdentifier;
        assert.equal(telemetryIdentifier !== null, true);
        const analytics = yield* AnalyticsService.AnalyticsService;

        for (let index = 0; index < 45; index += 1) {
          yield* analytics.record("test.flush.drain", { index });
        }

        yield* analytics.flush;
      }).pipe(Effect.withTracer(tracer), Effect.provide(runtimeLayer));

      const batchRequests = capturedRequests.filter(
        (request): request is RecordedBatchRequest & { readonly body: RecordedBatchBody } =>
          Array.isArray(request.body?.batch),
      );
      assert.equal(batchRequests.length, 3);
      assert.equal(
        batchRequests.every((request) => request.path === "/batch/" || request.path === "/batch"),
        true,
      );
      const deliveredIndexes = batchRequests.flatMap((request) =>
        request.body.batch
          .filter((event) => event.event === "test.flush.drain")
          .map((event) => event.properties?.index)
          .filter((index): index is number => typeof index === "number"),
      );

      const sorted = deliveredIndexes.toSorted((a, b) => a - b);
      assert.equal(sorted.length, 45);
      assert.deepEqual(
        sorted,
        Array.from({ length: 45 }, (_, index) => index),
      );
      assert.equal(
        batchRequests.every((request) =>
          request.body.batch.every((event) => event.properties?.clientType === "cli-web-client"),
        ),
        true,
      );
      assert.equal(spanNames.includes("AnalyticsService.sendBatch"), false);
    }),
  );

  it.effect("does not record or flush when DO_NOT_TRACK is enabled", () =>
    Effect.gen(function* () {
      let requestCount = 0;
      const serverConfigLayer = ServerConfig.ServerConfig.layerTest(process.cwd(), {
        prefix: "t3-telemetry-dnt-",
      });
      const telemetryLayer = AnalyticsService.layer.pipe(Layer.provideMerge(serverConfigLayer));
      const configLayer = ConfigProvider.layer(
        ConfigProvider.fromUnknown({
          T3CODE_TELEMETRY_ENABLED: true,
          DO_NOT_TRACK: "1",
          T3CODE_POSTHOG_HOST: "",
        }),
      );
      const batchServerLayer = HttpServer.serve(
        Effect.sync(() => {
          requestCount += 1;
          return HttpServerResponse.jsonUnsafe({});
        }),
      );
      const runtimeLayer = telemetryLayer.pipe(
        Layer.provide(configLayer),
        Layer.provideMerge(NodeHttpServer.layerTest),
      );

      yield* Effect.gen(function* () {
        yield* Layer.launch(batchServerLayer).pipe(Effect.forkScoped);
        const analytics = yield* AnalyticsService.AnalyticsService;
        yield* analytics.record("test.dnt.disabled");
        yield* analytics.flush;
        yield* TestClock.adjust("1 minute");
        yield* Effect.yieldNow;
      }).pipe(Effect.provide(runtimeLayer));

      assert.equal(requestCount, 0);
    }),
  );

  it.effect("backs off failed flushes instead of retrying every interval", () =>
    Effect.gen(function* () {
      let requestCount = 0;
      const statuses = [503, 200, 503, 200];
      const serverConfigLayer = ServerConfig.ServerConfig.layerTest(process.cwd(), {
        prefix: "t3-telemetry-backoff-",
      });
      const telemetryLayer = AnalyticsService.layer.pipe(Layer.provideMerge(serverConfigLayer));
      const configLayer = ConfigProvider.layer(
        ConfigProvider.fromUnknown({
          T3CODE_TELEMETRY_ENABLED: true,
          T3CODE_POSTHOG_KEY: "phc_test_key",
          T3CODE_POSTHOG_HOST: "",
        }),
      );
      const batchServerLayer = HttpServer.serve(
        Effect.sync(() => {
          requestCount += 1;
          return HttpServerResponse.empty({ status: statuses.shift() ?? 200 });
        }),
      );
      const runtimeLayer = telemetryLayer.pipe(
        Layer.provide(configLayer),
        Layer.provideMerge(NodeHttpServer.layerTest),
      );

      yield* Effect.gen(function* () {
        yield* Layer.launch(batchServerLayer).pipe(Effect.forkScoped);
        const analytics = yield* AnalyticsService.AnalyticsService;
        yield* analytics.record("test.flush.backoff");

        yield* analytics.flush;
        yield* analytics.flush;
        assert.equal(requestCount, 1);

        yield* TestClock.adjust("999 millis");
        yield* analytics.flush;
        assert.equal(requestCount, 1);

        yield* TestClock.adjust("1 millis");
        yield* Effect.yieldNow;
        yield* analytics.flush;
        assert.equal(requestCount, 2);

        yield* analytics.record("test.flush.backoff.after-success");
        yield* analytics.flush;
        assert.equal(requestCount, 3);

        yield* TestClock.adjust("999 millis");
        yield* analytics.flush;
        assert.equal(requestCount, 3);

        yield* TestClock.adjust("1 millis");
        yield* Effect.yieldNow;
        yield* analytics.flush;
        assert.equal(requestCount, 4);
      }).pipe(Effect.provide(runtimeLayer));
    }),
  );

  it.effect("makes one final best-effort flush even during backoff", () =>
    Effect.gen(function* () {
      let requestCount = 0;
      const statuses = [503, 200];
      const serverConfigLayer = ServerConfig.ServerConfig.layerTest(process.cwd(), {
        prefix: "t3-telemetry-finalizer-",
      });
      const telemetryLayer = AnalyticsService.layer.pipe(Layer.provideMerge(serverConfigLayer));
      const configLayer = ConfigProvider.layer(
        ConfigProvider.fromUnknown({
          T3CODE_TELEMETRY_ENABLED: true,
          T3CODE_POSTHOG_KEY: "phc_test_key",
          T3CODE_POSTHOG_HOST: "",
        }),
      );
      const batchServerLayer = HttpServer.serve(
        Effect.sync(() => {
          requestCount += 1;
          return HttpServerResponse.empty({ status: statuses.shift() ?? 200 });
        }),
      );
      const baseLayer = Layer.merge(configLayer, NodeHttpServer.layerTest);

      yield* Effect.gen(function* () {
        yield* Layer.launch(batchServerLayer).pipe(Effect.forkScoped);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const analytics = yield* AnalyticsService.AnalyticsService;
            yield* analytics.record("test.flush.finalizer");
            yield* analytics.flush;
            assert.equal(requestCount, 1);
          }).pipe(Effect.provide(telemetryLayer)),
        );
        assert.equal(requestCount, 2);
      }).pipe(Effect.provide(baseLayer));
    }),
  );

  it.effect("bounds the final flush when an HTTP request never completes", () =>
    Effect.gen(function* () {
      let requestCount = 0;
      const serverConfigLayer = ServerConfig.ServerConfig.layerTest(process.cwd(), {
        prefix: "t3-telemetry-finalizer-timeout-",
      });
      const telemetryLayer = AnalyticsService.layer.pipe(Layer.provideMerge(serverConfigLayer));
      const configLayer = ConfigProvider.layer(
        ConfigProvider.fromUnknown({
          T3CODE_TELEMETRY_ENABLED: true,
          T3CODE_POSTHOG_KEY: "phc_test_key",
          T3CODE_POSTHOG_HOST: "https://posthog.example",
        }),
      );
      const httpLayer = Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make(() =>
          Effect.sync(() => {
            requestCount += 1;
          }).pipe(Effect.andThen(Effect.never)),
        ),
      );
      const runtimeLayer = telemetryLayer.pipe(
        Layer.provide(configLayer),
        Layer.provide(httpLayer),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const analytics = yield* AnalyticsService.AnalyticsService;
          yield* analytics.record("test.flush.finalizer.timeout");
        }).pipe(Effect.provide(runtimeLayer)),
      );
      assert.equal(requestCount, 1);
    }).pipe(TestClock.withLive),
  );
});
