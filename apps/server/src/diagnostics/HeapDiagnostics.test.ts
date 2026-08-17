import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { ServerHeapSnapshotError, type ServerWriteHeapSnapshotInput } from "@t3tools/contracts";
import * as ServerConfig from "../config.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import {
  HEAP_DIAGNOSTICS_INTERVAL,
  HeapDiagnostics,
  type HeapDiagnosticsOperations,
  layerWithOperations,
} from "./HeapDiagnostics.ts";

const commandModelStats = {
  threadCount: 4,
  deletedThreadCount: 1,
  retainedMessageCount: 9,
  retainedMessageTextCodeUnits: 21,
  retainedMessageTextUtf16Bytes: 42,
} as const;

const makeOperations = (
  overrides: Partial<HeapDiagnosticsOperations> = {},
): HeapDiagnosticsOperations => ({
  memoryUsage: () => ({
    heapUsed: 10,
    heapTotal: 20,
    external: 30,
    arrayBuffers: 40,
    rss: 50,
  }),
  getHeapStatistics: () => ({
    number_of_native_contexts: 2,
    number_of_detached_contexts: 3,
  }),
  writeHeapSnapshot: (path) => Effect.succeed(path),
  ...overrides,
});

const makeTestLayer = (operations: HeapDiagnosticsOperations) =>
  layerWithOperations(operations).pipe(
    Layer.provide(
      Layer.succeed(
        OrchestrationEngineService,
        OrchestrationEngineService.of({
          readEvents: () => Stream.empty,
          dispatch: () => Effect.die("unused"),
          streamDomainEvents: Stream.empty,
          latestSequence: Effect.succeed(0),
          readCommandModelHeapDiagnostics: Effect.succeed(commandModelStats),
        }),
      ),
    ),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-heap-diagnostics-" })),
  );

it.layer(NodeServices.layer)("HeapDiagnostics", (it) => {
  it.effect("maps process, V8, and live orchestration fields", () =>
    Effect.gen(function* () {
      const diagnostics = yield* HeapDiagnostics;

      expect(yield* diagnostics.sample).toEqual({
        heapUsed: 10,
        heapTotal: 20,
        external: 30,
        arrayBuffers: 40,
        rss: 50,
        nativeContextCount: 2,
        detachedContextCount: 3,
        orchestrationStatsAvailable: true,
        ...commandModelStats,
      });
    }).pipe(Effect.provide(makeTestLayer(makeOperations()))),
  );

  it.effect("samples immediately and then once per 60-second scoped interval", () => {
    let sampleCount = 0;
    const operations = makeOperations({
      memoryUsage: () => {
        sampleCount += 1;
        return { heapUsed: 1, heapTotal: 2, external: 3, arrayBuffers: 4, rss: 5 };
      },
    });

    return Effect.gen(function* () {
      yield* HeapDiagnostics;
      expect(sampleCount).toBe(1);
      yield* Effect.yieldNow;

      yield* TestClock.adjust(Duration.subtract(HEAP_DIAGNOSTICS_INTERVAL, Duration.millis(1)));
      expect(sampleCount).toBe(1);

      yield* TestClock.adjust(Duration.millis(1));
      yield* Effect.yieldNow;
      expect(sampleCount).toBe(2);
    }).pipe(Effect.provide(makeTestLayer(operations).pipe(Layer.provideMerge(TestClock.layer()))));
  });

  it.effect("constructs snapshot paths under T3 state and rejects traversal filenames", () => {
    const writtenPaths: Array<string> = [];
    const operations = makeOperations({
      writeHeapSnapshot: (path) =>
        Effect.sync(() => {
          writtenPaths.push(path);
          return path;
        }),
    });

    return Effect.gen(function* () {
      const diagnostics = yield* HeapDiagnostics;
      const result = yield* diagnostics.writeSnapshot({ filename: "manual.heapsnapshot" });
      expect(result.path).toMatch(/\/userdata\/diagnostics\/heap-snapshots\/manual\.heapsnapshot$/);
      expect(writtenPaths).toEqual([result.path]);

      const invalidInput = {
        filename: "../escape.heapsnapshot",
      } as unknown as ServerWriteHeapSnapshotInput;
      const error = yield* diagnostics.writeSnapshot(invalidInput).pipe(Effect.flip);
      expect(error.reason).toBe("invalidFilename");
      expect(writtenPaths).toEqual([result.path]);
    }).pipe(Effect.provide(makeTestLayer(operations)));
  });

  it.effect("rejects concurrent snapshots instead of queueing them", () =>
    Effect.gen(function* () {
      const writerStarted = yield* Deferred.make<void>();
      const releaseWriter = yield* Deferred.make<void>();
      const operations = makeOperations({
        writeHeapSnapshot: (path) =>
          Deferred.succeed(writerStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseWriter)),
            Effect.as(path),
          ),
      });

      yield* Effect.gen(function* () {
        const diagnostics = yield* HeapDiagnostics;
        const first = yield* diagnostics
          .writeSnapshot({ filename: "first.heapsnapshot" })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(writerStarted);

        const error = yield* diagnostics
          .writeSnapshot({ filename: "second.heapsnapshot" })
          .pipe(Effect.flip);
        expect(error.reason).toBe("busy");

        yield* Deferred.succeed(releaseWriter, undefined);
        expect((yield* Fiber.join(first)).path).toMatch(/first\.heapsnapshot$/);
      }).pipe(Effect.provide(makeTestLayer(operations)));
    }),
  );

  it.effect("clears the single-flight guard when a snapshot request is interrupted", () =>
    Effect.gen(function* () {
      const writerStarted = yield* Deferred.make<void>();
      let attempt = 0;
      const operations = makeOperations({
        writeHeapSnapshot: (path) =>
          Effect.suspend(() => {
            attempt += 1;
            return attempt === 1
              ? Deferred.succeed(writerStarted, undefined).pipe(Effect.andThen(Effect.never))
              : Effect.succeed(path);
          }),
      });

      yield* Effect.gen(function* () {
        const diagnostics = yield* HeapDiagnostics;
        const first = yield* diagnostics
          .writeSnapshot({ filename: "interrupted.heapsnapshot" })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(writerStarted);
        yield* Fiber.interrupt(first);

        const result = yield* diagnostics.writeSnapshot({
          filename: "after-interrupt.heapsnapshot",
        });
        expect(result.path).toMatch(/after-interrupt\.heapsnapshot$/);
        expect(attempt).toBe(2);
      }).pipe(Effect.provide(makeTestLayer(operations)));
    }),
  );

  it.effect("clears the single-flight guard after a failed write", () => {
    let attempt = 0;
    const operations = makeOperations({
      writeHeapSnapshot: (path) =>
        Effect.suspend(() => {
          attempt += 1;
          return attempt === 1
            ? Effect.fail(
                new ServerHeapSnapshotError({
                  reason: "writeFailed",
                  detail: "disk full",
                }),
              )
            : Effect.succeed(path);
        }),
    });

    return Effect.gen(function* () {
      const diagnostics = yield* HeapDiagnostics;
      const firstError = yield* diagnostics
        .writeSnapshot({ filename: "retry.heapsnapshot" })
        .pipe(Effect.flip);
      expect(firstError.reason).toBe("writeFailed");

      const result = yield* diagnostics.writeSnapshot({ filename: "retry.heapsnapshot" });
      expect(result.path).toMatch(/retry\.heapsnapshot$/);
      expect(attempt).toBe(2);
    }).pipe(Effect.provide(makeTestLayer(operations)));
  });
});
