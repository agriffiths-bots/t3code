import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import * as ElectronApp from "../electron/ElectronApp.ts";
import * as ElectronTheme from "../electron/ElectronTheme.ts";
import * as DesktopEnvironment from "./DesktopEnvironment.ts";
import * as DesktopLifecycle from "./DesktopLifecycle.ts";
import * as DesktopShutdown from "./DesktopShutdown.ts";
import * as DesktopState from "./DesktopState.ts";
import * as DesktopWindow from "../window/DesktopWindow.ts";

type AppListener = (...args: ReadonlyArray<unknown>) => void;

function makeElectronAppLayer(input: {
  readonly lockResult: boolean;
  readonly listenerRef: Ref.Ref<Map<string, AppListener>>;
  readonly quitCount: Ref.Ref<number>;
  readonly lockRequests: Ref.Ref<number>;
  readonly whenReady?: Effect.Effect<void>;
}) {
  return Layer.succeed(ElectronApp.ElectronApp, {
    metadata: Effect.die("unexpected metadata read"),
    name: Effect.succeed("T3 Code"),
    whenReady: input.whenReady ?? Effect.void,
    quit: Ref.update(input.quitCount, (count) => count + 1),
    exit: () => Effect.void,
    relaunch: () => Effect.void,
    setPath: () => Effect.void,
    setName: () => Effect.void,
    setAboutPanelOptions: () => Effect.void,
    setAppUserModelId: () => Effect.void,
    requestSingleInstanceLock: Ref.update(input.lockRequests, (count) => count + 1).pipe(
      Effect.as(input.lockResult),
    ),
    isDefaultProtocolClient: () => Effect.succeed(false),
    setAsDefaultProtocolClient: () => Effect.succeed(true),
    setDesktopName: () => Effect.void,
    setDockIcon: () => Effect.void,
    appendCommandLineSwitch: () => Effect.void,
    on: (eventName, listener) =>
      Effect.acquireRelease(
        Ref.update(input.listenerRef, (listeners) => {
          const next = new Map(listeners);
          next.set(eventName, listener as AppListener);
          return next;
        }),
        () =>
          Ref.update(input.listenerRef, (listeners) => {
            const next = new Map(listeners);
            next.delete(eventName);
            return next;
          }),
      ),
  } satisfies ElectronApp.ElectronApp["Service"]);
}

function makeDesktopWindowLayer(input: { readonly activateCount: Ref.Ref<number> }) {
  return Layer.succeed(DesktopWindow.DesktopWindow, {
    createMain: Effect.die("unexpected createMain"),
    ensureMain: Effect.die("unexpected ensureMain"),
    revealOrCreateMain: Effect.die("unexpected revealOrCreateMain"),
    activate: Ref.update(input.activateCount, (count) => count + 1),
    createMainIfBackendReady: Effect.die("unexpected createMainIfBackendReady"),
    showBackendStartupError: () => Effect.die("unexpected showBackendStartupError"),
    showConnectingSplash: Effect.die("unexpected showConnectingSplash"),
    handleBackendReady: () => Effect.die("unexpected handleBackendReady"),
    handleBackendNotReady: Effect.die("unexpected handleBackendNotReady"),
    dispatchMenuAction: () => Effect.die("unexpected dispatchMenuAction"),
    syncAppearance: Effect.void,
  } satisfies DesktopWindow.DesktopWindow["Service"]);
}

function makeLifecycleLayer(input: {
  readonly lockResult: boolean;
  readonly listenerRef: Ref.Ref<Map<string, AppListener>>;
  readonly quitCount: Ref.Ref<number>;
  readonly lockRequests: Ref.Ref<number>;
  readonly activateCount: Ref.Ref<number>;
  readonly whenReady?: Effect.Effect<void>;
}) {
  return Layer.mergeAll(
    DesktopLifecycle.layer,
    DesktopShutdown.layer,
    DesktopState.layer,
    Layer.succeed(
      DesktopEnvironment.DesktopEnvironment,
      DesktopEnvironment.DesktopEnvironment.of({
        isDevelopment: false,
        platform: "win32",
      } as unknown as DesktopEnvironment.DesktopEnvironment["Service"]),
    ),
    makeDesktopWindowLayer({ activateCount: input.activateCount }),
    makeElectronAppLayer(input),
    Layer.succeed(ElectronTheme.ElectronTheme, {
      shouldUseDarkColors: Effect.succeed(false),
      setSource: () => Effect.void,
      onUpdated: () => Effect.void,
    } satisfies ElectronTheme.ElectronTheme["Service"]),
  );
}

describe("DesktopLifecycle", () => {
  it.effect("quits early when another process already holds the single-instance lock", () =>
    Effect.gen(function* () {
      const listenerRef = yield* Ref.make(new Map<string, AppListener>());
      const quitCount = yield* Ref.make(0);
      const lockRequests = yield* Ref.make(0);
      const activateCount = yield* Ref.make(0);
      const layer = makeLifecycleLayer({
        lockResult: false,
        listenerRef,
        quitCount,
        lockRequests,
        activateCount,
      });

      const exit = yield* Effect.scoped(
        Effect.gen(function* () {
          const lifecycle = yield* DesktopLifecycle.DesktopLifecycle;
          yield* lifecycle.register;
        }),
      ).pipe(Effect.provide(layer), Effect.exit);

      assert.equal(exit._tag, "Failure");
      assert.equal(yield* Ref.get(lockRequests), 1);
      assert.equal(yield* Ref.get(quitCount), 1);
      assert.equal(yield* Ref.get(activateCount), 0);
      assert.isUndefined((yield* Ref.get(listenerRef)).get("second-instance"));
    }),
  );

  it.effect("activates the existing window when Electron reports a second instance", () =>
    Effect.gen(function* () {
      const listenerRef = yield* Ref.make(new Map<string, AppListener>());
      const quitCount = yield* Ref.make(0);
      const lockRequests = yield* Ref.make(0);
      const activateCount = yield* Ref.make(0);
      const layer = makeLifecycleLayer({
        lockResult: true,
        listenerRef,
        quitCount,
        lockRequests,
        activateCount,
      });

      yield* Effect.scoped(
        Effect.gen(function* () {
          const lifecycle = yield* DesktopLifecycle.DesktopLifecycle;
          yield* lifecycle.register;

          const secondInstance = (yield* Ref.get(listenerRef)).get("second-instance");
          assert.isFunction(secondInstance);
          secondInstance?.({});
          yield* Effect.promise(() => Promise.resolve());

          assert.equal(yield* Ref.get(lockRequests), 1);
          assert.equal(yield* Ref.get(quitCount), 0);
          assert.equal(yield* Ref.get(activateCount), 1);
        }),
      ).pipe(Effect.provide(layer));
    }),
  );

  it.effect("waits for Electron readiness before activating a second instance", () =>
    Effect.gen(function* () {
      const listenerRef = yield* Ref.make(new Map<string, AppListener>());
      const quitCount = yield* Ref.make(0);
      const lockRequests = yield* Ref.make(0);
      const activateCount = yield* Ref.make(0);
      const ready = yield* Deferred.make<void>();
      const layer = makeLifecycleLayer({
        lockResult: true,
        listenerRef,
        quitCount,
        lockRequests,
        activateCount,
        whenReady: Deferred.await(ready),
      });

      yield* Effect.scoped(
        Effect.gen(function* () {
          const lifecycle = yield* DesktopLifecycle.DesktopLifecycle;
          yield* lifecycle.register;

          const secondInstance = (yield* Ref.get(listenerRef)).get("second-instance");
          assert.isFunction(secondInstance);
          secondInstance?.({});
          yield* Effect.promise(() => Promise.resolve());
          assert.equal(yield* Ref.get(activateCount), 0);

          yield* Deferred.succeed(ready, undefined);
          yield* Effect.promise(() => Promise.resolve());
          assert.equal(yield* Ref.get(activateCount), 1);
        }),
      ).pipe(Effect.provide(layer));
    }),
  );
});
