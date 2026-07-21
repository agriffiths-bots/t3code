// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  GrokSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { ServerConfig } from "../../config.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as AnalyticsService from "../../telemetry/AnalyticsService.ts";
import type { GrokAdapterShape } from "../Services/GrokAdapter.ts";
import * as ProviderAdapterRegistry from "../Services/ProviderAdapterRegistry.ts";
import * as ProviderService from "../Services/ProviderService.ts";
import { makeAdapterRegistryMock } from "../testUtils/providerAdapterRegistryMock.ts";
import { makeGrokAdapter } from "./GrokAdapter.ts";
import * as ProviderEventLoggers from "./ProviderEventLoggers.ts";
import { makeProviderServiceLive } from "./ProviderService.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";

const GROK_DRIVER = ProviderDriverKind.make("grok");
const GROK_INSTANCE_ID = ProviderInstanceId.make("grok");
const decodeGrokSettings = Schema.decodeSync(GrokSettings);
const replayBinaryPath = NodePath.join(
  NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
  "fixtures/replay-grok-acp-turn.mjs",
);

class RecordedGrokAdapter extends Context.Service<RecordedGrokAdapter, GrokAdapterShape>()(
  "t3/provider/Layers/GrokProviderServiceE2E.test/RecordedGrokAdapter",
) {}

const sqliteLayer = SqlitePersistenceMemory;
const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(Layer.provide(sqliteLayer));
const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
const serverSettingsLayer = ServerSettings.ServerSettingsService.layerTest();
const serverConfigLayer = ServerConfig.layerTest(process.cwd(), process.cwd());
const adapterLayer = Layer.effect(
  RecordedGrokAdapter,
  makeGrokAdapter(decodeGrokSettings({ binaryPath: replayBinaryPath }), {
    instanceId: GROK_INSTANCE_ID,
  }),
).pipe(
  Layer.provideMerge(serverConfigLayer),
  Layer.provideMerge(serverSettingsLayer),
  Layer.provideMerge(NodeServices.layer),
);
const adapterRegistryLayer = Layer.effect(
  ProviderAdapterRegistry.ProviderAdapterRegistry,
  Effect.gen(function* () {
    const adapter = yield* RecordedGrokAdapter;
    return makeAdapterRegistryMock({ [GROK_DRIVER]: adapter });
  }),
).pipe(Layer.provide(adapterLayer));
const providerServiceLayer = makeProviderServiceLive().pipe(
  Layer.provide(adapterRegistryLayer),
  Layer.provide(directoryLayer),
  Layer.provide(serverSettingsLayer),
  Layer.provideMerge(AnalyticsService.layerTest),
  Layer.provide(
    Layer.succeed(
      ProviderEventLoggers.ProviderEventLoggers,
      ProviderEventLoggers.NoOpProviderEventLoggers,
    ),
  ),
);
const recordedGrokProviderLayer = Layer.mergeAll(
  adapterLayer,
  providerServiceLayer,
  directoryLayer,
  runtimeRepositoryLayer,
).pipe(
  Layer.provideMerge(sqliteLayer),
  Layer.provideMerge(serverConfigLayer),
  Layer.provideMerge(serverSettingsLayer),
  Layer.provideMerge(NodeServices.layer),
);

it.layer(recordedGrokProviderLayer)("ProviderService recorded Grok ACP session", (it) => {
  it.effect("replays a recorded Grok first turn after the session start caller exits", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const adapter = yield* RecordedGrokAdapter;
      const threadId = ThreadId.make("recorded-grok-consumer-lifetime");
      const modelSelection = { instanceId: GROK_INSTANCE_ID, model: "grok-4.5" };

      const runtimeEventsFiber = yield* provider.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;

      const startFiber = yield* provider
        .startSession(threadId, {
          provider: GROK_DRIVER,
          providerInstanceId: GROK_INSTANCE_ID,
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
          modelSelection,
        })
        .pipe(Effect.forkChild);
      yield* Fiber.join(startFiber);

      const turnResult = yield* provider
        .sendTurn({
          threadId,
          input: "Reply exactly PONG.",
          attachments: [],
          modelSelection,
        })
        .pipe(Effect.timeoutOption("2 seconds"));
      assert.isTrue(Option.isSome(turnResult), "recorded Grok turn timed out");

      const runtimeEvents: ReadonlyArray<ProviderRuntimeEvent> = Array.from(
        yield* Fiber.join(runtimeEventsFiber).pipe(Effect.timeout("2 seconds")),
      );
      const assistantDeltas = runtimeEvents.flatMap((event) =>
        event.type === "content.delta" && event.payload.streamKind === "assistant_text"
          ? [event.payload.delta]
          : [],
      );
      assert.deepStrictEqual(assistantDeltas, ["P", "ONG"]);
      assert.equal(runtimeEvents.at(-1)?.type, "turn.completed");
      const completed = runtimeEvents.findLast((event) => event.type === "turn.completed");
      assert.equal(completed?.payload.state, "completed");

      const recordedThread = yield* adapter.readThread(threadId);
      assert.isAbove(recordedThread.turns.length, 0, "recorded Grok turnCount must be positive");

      yield* provider.stopSession({ threadId });
    }),
  );
});
