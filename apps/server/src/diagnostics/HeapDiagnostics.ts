// @effect-diagnostics nodeBuiltinImport:off
import * as NodeV8 from "node:v8";
import * as NodeCrypto from "node:crypto";

import {
  ServerHeapSnapshotError,
  ServerHeapSnapshotFilename,
  type ServerWriteHeapSnapshotInput,
  type ServerWriteHeapSnapshotResult,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";

import * as ServerConfig from "../config.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { WS_KEEPALIVE_INTERVAL_MS } from "../wsKeepalive.ts";

export const HEAP_DIAGNOSTICS_INTERVAL = Duration.seconds(60);

export interface HeapDiagnosticsSample {
  readonly heapUsed: number;
  readonly heapTotal: number;
  readonly external: number;
  readonly arrayBuffers: number;
  readonly rss: number;
  readonly nativeContextCount: number;
  readonly detachedContextCount: number;
  readonly orchestrationStatsAvailable: boolean;
  readonly threadCount: number;
  readonly deletedThreadCount: number;
  readonly retainedMessageCount: number;
  readonly retainedMessageTextCodeUnits: number;
  readonly retainedMessageTextUtf16Bytes: number;
}

export interface HeapDiagnosticsOperations {
  readonly memoryUsage: () => {
    readonly heapUsed: number;
    readonly heapTotal: number;
    readonly external: number;
    readonly arrayBuffers: number;
    readonly rss: number;
  };
  readonly getHeapStatistics: () => {
    readonly number_of_native_contexts: number;
    readonly number_of_detached_contexts: number;
  };
  readonly writeHeapSnapshot: (path: string) => Effect.Effect<string, ServerHeapSnapshotError>;
}

const liveOperations: HeapDiagnosticsOperations = {
  memoryUsage: () => process.memoryUsage(),
  getHeapStatistics: () => NodeV8.getHeapStatistics(),
  writeHeapSnapshot: (path) =>
    Effect.try({
      try: () => NodeV8.writeHeapSnapshot(path),
      catch: () =>
        new ServerHeapSnapshotError({
          reason: "writeFailed",
          detail: "V8 could not write the heap snapshot.",
        }),
    }),
};

const emptyCommandModelStats = {
  threadCount: 0,
  deletedThreadCount: 0,
  retainedMessageCount: 0,
  retainedMessageTextCodeUnits: 0,
  retainedMessageTextUtf16Bytes: 0,
} as const;

const isHeapSnapshotFilename = Schema.is(ServerHeapSnapshotFilename);

export class HeapDiagnostics extends Context.Service<
  HeapDiagnostics,
  {
    readonly sample: Effect.Effect<HeapDiagnosticsSample>;
    readonly writeSnapshot: (
      input: ServerWriteHeapSnapshotInput,
    ) => Effect.Effect<ServerWriteHeapSnapshotResult, ServerHeapSnapshotError>;
  }
>()("t3/diagnostics/HeapDiagnostics") {}

export const make = Effect.fn("makeHeapDiagnostics")(function* (
  operations: HeapDiagnosticsOperations = liveOperations,
) {
  const config = yield* ServerConfig.ServerConfig;
  const engine = yield* OrchestrationEngineService;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const snapshotActive = yield* Ref.make(false);

  const commandModelStats = engine.readCommandModelHeapDiagnostics;
  const sample = Effect.gen(function* () {
    const orchestrationStatsAvailable = commandModelStats !== undefined;
    const orchestrationStats =
      commandModelStats === undefined ? emptyCommandModelStats : yield* commandModelStats;
    const processMemory = operations.memoryUsage();
    const heapStatistics = operations.getHeapStatistics();

    return {
      heapUsed: processMemory.heapUsed,
      heapTotal: processMemory.heapTotal,
      external: processMemory.external,
      arrayBuffers: processMemory.arrayBuffers,
      rss: processMemory.rss,
      nativeContextCount: heapStatistics.number_of_native_contexts,
      detachedContextCount: heapStatistics.number_of_detached_contexts,
      orchestrationStatsAvailable,
      ...orchestrationStats,
    };
  });

  const logSample = sample.pipe(
    Effect.flatMap((value) =>
      Effect.logInfo("heap diagnostics sample").pipe(Effect.annotateLogs(value)),
    ),
  );

  const writeSnapshot: HeapDiagnostics["Service"]["writeSnapshot"] = (input) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (input.filename !== undefined && !isHeapSnapshotFilename(input.filename)) {
          return yield* new ServerHeapSnapshotError({
            reason: "invalidFilename",
            detail: "Filename must be a basename ending in .heapsnapshot.",
          });
        }

        const acquired = yield* Ref.modify(snapshotActive, (active) =>
          active ? ([false, true] as const) : ([true, true] as const),
        );
        if (!acquired) {
          return yield* new ServerHeapSnapshotError({
            reason: "busy",
            detail: "A heap snapshot is already in progress.",
          });
        }

        return yield* restore(
          Effect.gen(function* () {
            const filename =
              input.filename ??
              `Heap-${yield* Clock.currentTimeMillis}-${process.pid}-${NodeCrypto.randomUUID()}.heapsnapshot`;
            yield* fileSystem.makeDirectory(config.heapSnapshotsDir, { recursive: true }).pipe(
              Effect.mapError(
                () =>
                  new ServerHeapSnapshotError({
                    reason: "writeFailed",
                    detail: "Could not create the heap snapshot directory.",
                  }),
              ),
            );
            yield* fileSystem.chmod(config.heapSnapshotsDir, 0o700).pipe(
              Effect.mapError(
                () =>
                  new ServerHeapSnapshotError({
                    reason: "writeFailed",
                    detail: "Could not secure the heap snapshot directory.",
                  }),
              ),
            );
            const destination = path.join(config.heapSnapshotsDir, filename);
            const removeSnapshot = fileSystem.remove(destination).pipe(Effect.ignore);
            yield* fileSystem
              .writeFile(destination, new Uint8Array(), { flag: "wx", mode: 0o600 })
              .pipe(
                Effect.mapError(
                  () =>
                    new ServerHeapSnapshotError({
                      reason: "writeFailed",
                      detail: "Could not create the heap snapshot file.",
                    }),
                ),
              );
            yield* fileSystem.chmod(destination, 0o600).pipe(
              Effect.onError(() => removeSnapshot),
              Effect.mapError(
                () =>
                  new ServerHeapSnapshotError({
                    reason: "writeFailed",
                    detail: "Could not secure the heap snapshot file.",
                  }),
              ),
            );
            const startedAtMs = yield* Clock.currentTimeMillis;

            yield* Effect.logWarning(
              "Writing a V8 heap snapshot blocks the server event loop and may require heap-sized extra memory.",
            ).pipe(
              Effect.annotateLogs({
                destination,
                websocketKeepaliveIntervalMs: WS_KEEPALIVE_INTERVAL_MS,
              }),
            );

            const writtenPath = yield* operations.writeHeapSnapshot(destination).pipe(
              Effect.tapError((cause) =>
                Effect.logWarning("V8 heap snapshot write failed").pipe(
                  Effect.annotateLogs({ destination, cause }),
                ),
              ),
              Effect.onError(() => removeSnapshot),
              Effect.mapError(
                () =>
                  new ServerHeapSnapshotError({
                    reason: "writeFailed",
                    detail: "V8 could not write the heap snapshot.",
                  }),
              ),
            );
            yield* fileSystem.chmod(writtenPath, 0o600).pipe(
              Effect.onError(() => fileSystem.remove(writtenPath).pipe(Effect.ignore)),
              Effect.mapError(
                () =>
                  new ServerHeapSnapshotError({
                    reason: "writeFailed",
                    detail: "Could not secure the heap snapshot file.",
                  }),
              ),
            );
            const elapsedMs = (yield* Clock.currentTimeMillis) - startedAtMs;
            yield* Effect.logInfo("V8 heap snapshot completed").pipe(
              Effect.annotateLogs({ path: writtenPath, elapsedMs }),
            );
            return { path: writtenPath };
          }),
        ).pipe(Effect.ensuring(Ref.set(snapshotActive, false)));
      }),
    );

  yield* logSample;
  yield* Effect.forkScoped(
    Effect.sleep(HEAP_DIAGNOSTICS_INTERVAL).pipe(Effect.andThen(logSample), Effect.forever),
  );

  return HeapDiagnostics.of({ sample, writeSnapshot });
});

export const layerWithOperations = (operations: HeapDiagnosticsOperations) =>
  Layer.effect(HeapDiagnostics, make(operations));

export const layer = layerWithOperations(liveOperations);
