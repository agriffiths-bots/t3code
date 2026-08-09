import { assert, describe, it } from "@effect/vitest";
import {
  PRIMARY_LOCAL_ENVIRONMENT_ID,
  type DesktopNotificationQueuedAction,
  type ServerDeviceNotification,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";
import { beforeEach, vi } from "vite-plus/test";

import * as DesktopBackendPool from "../backend/DesktopBackendPool.ts";
import * as IpcChannels from "../ipc/channels.ts";
import * as DesktopWindow from "../window/DesktopWindow.ts";
import * as ElectronWindow from "./ElectronWindow.ts";

const {
  NotificationMock,
  notificationInstances,
  notificationIsSupportedMock,
  revealOrCreateMainMock,
  sendAllMock,
} = vi.hoisted(() => {
  class NotificationMock {
    static isSupported = vi.fn(() => true);

    readonly options: unknown;
    readonly listeners = new Map<string, () => void>();
    readonly show = vi.fn();
    readonly close = vi.fn(() => {
      this.listeners.get("close")?.();
    });

    constructor(options: unknown) {
      this.options = options;
      notificationInstances.push(this);
    }

    on(eventName: string, listener: () => void) {
      this.listeners.set(eventName, listener);
      return this;
    }
  }

  const notificationInstances: NotificationMock[] = [];
  return {
    NotificationMock,
    notificationInstances,
    notificationIsSupportedMock: NotificationMock.isSupported,
    revealOrCreateMainMock: vi.fn(),
    sendAllMock: vi.fn(),
  };
});

vi.mock("electron", () => ({
  Notification: NotificationMock,
}));

import * as ElectronNotification from "./ElectronNotification.ts";

const textDecoder = new TextDecoder();

function makeTestWindow(input: { readonly loading?: boolean } = {}) {
  let loading = input.loading ?? false;
  const listeners = new Map<string, () => void>();
  const webContents = {
    send: vi.fn(),
    isLoadingMainFrame: vi.fn(() => loading),
    once: vi.fn((eventName: string, listener: () => void) => {
      listeners.set(eventName, listener);
      return webContents;
    }),
  };
  const window = {
    isDestroyed: vi.fn(() => false),
    webContents,
  };
  return {
    window,
    webContents,
    finishLoad: () => {
      loading = false;
      listeners.get("did-finish-load")?.();
    },
  };
}

function requestBodyJson(request: HttpClientRequest.HttpClientRequest): unknown {
  const rawBody = (request.body as { readonly body?: Uint8Array }).body;
  assert.instanceOf(rawBody, Uint8Array);
  return JSON.parse(textDecoder.decode(rawBody));
}

function queuedEvents(actions: readonly DesktopNotificationQueuedAction[]) {
  return actions.map((action) => action.event);
}

const notification: ServerDeviceNotification = {
  notificationId: "notification-1",
  ackToken: "ack-token",
  title: "Task finished",
  body: "The task finished.",
  deepLink: "/environment/thread",
  createdAt: "2026-07-09T00:00:00.000Z" as ServerDeviceNotification["createdAt"],
  requireInteraction: true,
};

function makeLayer(
  input: {
    readonly targetWindow?: ReturnType<typeof makeTestWindow>;
    readonly ackRequests?: HttpClientRequest.HttpClientRequest[];
    readonly ackHandler?: (
      request: HttpClientRequest.HttpClientRequest,
    ) => Effect.Effect<HttpClientResponse.HttpClientResponse>;
  } = {},
) {
  const targetWindow = input.targetWindow ?? makeTestWindow();
  const ackRequests = input.ackRequests ?? [];
  revealOrCreateMainMock.mockImplementation(() => targetWindow.window);
  return ElectronNotification.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(DesktopBackendPool.DesktopBackendPool, {
          get: () => Effect.succeed(Option.none()),
          list: Effect.succeed([]),
          primary: Effect.succeed({
            id: PRIMARY_LOCAL_ENVIRONMENT_ID,
            label: Effect.succeed("Primary"),
            currentConfig: Effect.succeed(
              Option.some({
                httpBaseUrl: new URL("http://127.0.0.1:3773/"),
              }),
            ),
          } as unknown as DesktopBackendPool.DesktopBackendInstance),
          register: () => Effect.die("unused"),
          unregister: () => Effect.die("unused"),
        } satisfies DesktopBackendPool.DesktopBackendPool["Service"]),
        Layer.succeed(
          HttpClient.HttpClient,
          HttpClient.make((request) => {
            ackRequests.push(request);
            return (
              input.ackHandler?.(request) ??
              Effect.succeed(
                HttpClientResponse.fromWeb(
                  request,
                  new Response(
                    JSON.stringify({ notificationId: "notification-1", accepted: true }),
                    {
                      headers: { "content-type": "application/json" },
                      status: 200,
                    },
                  ),
                ),
              )
            );
          }),
        ),
        Layer.succeed(
          DesktopWindow.DesktopWindow,
          DesktopWindow.DesktopWindow.of({
            createMain: Effect.die("unused"),
            ensureMain: Effect.die("unused"),
            revealOrCreateMain: Effect.sync(() => revealOrCreateMainMock()),
            activate: Effect.die("unused"),
            createMainIfBackendReady: Effect.die("unused"),
            showBackendStartupError: () => Effect.die("unused"),
            showConnectingSplash: Effect.die("unused"),
            handleBackendReady: () => Effect.die("unused"),
            handleBackendNotReady: Effect.die("unused"),
            dispatchMenuAction: () => Effect.die("unused"),
            syncAppearance: Effect.die("unused"),
            zoomMain: () => Effect.die("unused"),
            flushMainWindowBounds: Effect.die("unused"),
          }),
        ),
        Layer.succeed(
          ElectronWindow.ElectronWindow,
          ElectronWindow.ElectronWindow.of({
            create: () => Effect.die("unused"),
            main: Effect.succeed(Option.none()),
            currentMainOrFirst: Effect.succeed(Option.none()),
            focusedMainOrFirst: Effect.succeed(Option.none()),
            setMain: () => Effect.void,
            clearMain: () => Effect.void,
            reveal: () => Effect.die("unused"),
            sendAll: (channel, ...args) => Effect.sync(() => sendAllMock(channel, ...args)),
            destroyAll: Effect.void,
            syncAllAppearance: () => Effect.void,
          }),
        ),
      ),
    ),
  );
}

describe("ElectronNotification", () => {
  beforeEach(() => {
    notificationInstances.length = 0;
    notificationIsSupportedMock.mockReset();
    notificationIsSupportedMock.mockReturnValue(true);
    revealOrCreateMainMock.mockReset();
    sendAllMock.mockReset();
  });

  it.effect("shows a native notification and emits click actions over desktop IPC", () => {
    const targetWindow = makeTestWindow();
    return Effect.gen(function* () {
      const notifications = yield* ElectronNotification.ElectronNotification;

      const shown = yield* notifications.show(notification);

      assert.equal(shown, true);
      assert.equal(notificationInstances.length, 1);
      const rendered = notificationInstances[0]!;
      assert.deepEqual(rendered.options, {
        title: "Task finished",
        body: "The task finished.",
        timeoutType: "never",
        silent: false,
      });
      assert.equal(rendered.show.mock.calls.length, 1);

      rendered.listeners.get("click")?.();
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      assert.equal(revealOrCreateMainMock.mock.calls.length, 1);
      assert.deepEqual(targetWindow.webContents.send.mock.calls, [
        [IpcChannels.NOTIFICATION_ACTION_CHANNEL],
      ]);
      assert.deepEqual(queuedEvents(yield* notifications.drainActions), [
        {
          notificationId: "notification-1",
          action: "opened",
          deepLink: "/environment/thread",
        },
      ]);
    }).pipe(Effect.provide(makeLayer({ targetWindow })));
  });

  it.effect("waits for a recreated renderer before emitting click actions", () => {
    const targetWindow = makeTestWindow({ loading: true });
    return Effect.gen(function* () {
      const notifications = yield* ElectronNotification.ElectronNotification;

      yield* notifications.show(notification);
      notificationInstances[0]!.listeners.get("click")?.();
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      assert.equal(revealOrCreateMainMock.mock.calls.length, 1);
      assert.deepEqual(targetWindow.webContents.send.mock.calls, []);
      assert.deepEqual(
        targetWindow.webContents.once.mock.calls.map(([event]) => event),
        ["did-finish-load"],
      );

      targetWindow.finishLoad();
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      assert.deepEqual(targetWindow.webContents.send.mock.calls, [
        [IpcChannels.NOTIFICATION_ACTION_CHANNEL],
      ]);
      assert.deepEqual(queuedEvents(yield* notifications.drainActions), [
        {
          notificationId: "notification-1",
          action: "opened",
          deepLink: "/environment/thread",
        },
      ]);
    }).pipe(Effect.provide(makeLayer({ targetWindow })));
  });

  it.effect("does not block click delivery on the acknowledgement request", () => {
    const targetWindow = makeTestWindow();
    const ackRequests: HttpClientRequest.HttpClientRequest[] = [];
    return Effect.gen(function* () {
      const notifications = yield* ElectronNotification.ElectronNotification;

      yield* notifications.show(notification);
      notificationInstances[0]!.listeners.get("click")?.();
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      assert.equal(ackRequests.length, 1);
      assert.deepEqual(targetWindow.webContents.send.mock.calls, [
        [IpcChannels.NOTIFICATION_ACTION_CHANNEL],
      ]);
      assert.deepEqual(queuedEvents(yield* notifications.drainActions), [
        {
          notificationId: "notification-1",
          action: "opened",
          deepLink: "/environment/thread",
        },
      ]);
    }).pipe(
      Effect.provide(
        makeLayer({
          targetWindow,
          ackRequests,
          ackHandler: () => Effect.never,
        }),
      ),
    );
  });

  it.effect("acknowledges native close actions without a renderer window", () => {
    const ackRequests: HttpClientRequest.HttpClientRequest[] = [];
    return Effect.gen(function* () {
      const notifications = yield* ElectronNotification.ElectronNotification;

      yield* notifications.show(notification);
      notificationInstances[0]!.listeners.get("close")?.();
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      assert.equal(ackRequests.length, 1);
      assert.equal(ackRequests[0]!.method, "POST");
      assert.equal(ackRequests[0]!.url, "http://127.0.0.1:3773/api/notifications/ack");
      assert.deepEqual(requestBodyJson(ackRequests[0]!), {
        notificationId: "notification-1",
        ackToken: "ack-token",
        action: "closed",
      });
      assert.deepEqual(sendAllMock.mock.calls, [[IpcChannels.NOTIFICATION_ACTION_CHANNEL]]);
      assert.deepEqual(queuedEvents(yield* notifications.drainActions), [
        {
          notificationId: "notification-1",
          action: "closed",
          deepLink: "/environment/thread",
        },
      ]);
    }).pipe(Effect.provide(makeLayer({ ackRequests })));
  });

  it.effect("keeps action payloads in main until the renderer acknowledges them", () =>
    Effect.gen(function* () {
      const notifications = yield* ElectronNotification.ElectronNotification;

      yield* notifications.show(notification);
      notificationInstances[0]!.listeners.get("click")?.();
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      const drained = yield* notifications.drainActions;
      assert.deepEqual(queuedEvents(drained), [
        {
          notificationId: "notification-1",
          action: "opened",
          deepLink: "/environment/thread",
        },
      ]);
      assert.deepEqual(queuedEvents(yield* notifications.drainActions), [
        {
          notificationId: "notification-1",
          action: "opened",
          deepLink: "/environment/thread",
        },
      ]);
      yield* notifications.ackActions(drained.map((action) => action.id));
      assert.deepEqual(yield* notifications.drainActions, []);
    }).pipe(Effect.provide(makeLayer())),
  );

  it.effect("ignores delayed close events from replaced native notifications", () =>
    Effect.gen(function* () {
      const notifications = yield* ElectronNotification.ElectronNotification;

      yield* notifications.show(notification);
      const firstRendered = notificationInstances[0]!;
      firstRendered.close.mockImplementation(() => undefined);

      yield* notifications.show(notification);
      const secondRendered = notificationInstances[1]!;
      secondRendered.close.mockImplementation(() => undefined);

      yield* notifications.close(notification.notificationId);
      firstRendered.listeners.get("close")?.();
      secondRendered.listeners.get("close")?.();
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      assert.equal(secondRendered.close.mock.calls.length, 1);
      assert.deepEqual(sendAllMock.mock.calls, []);
      assert.deepEqual(yield* notifications.drainActions, []);
    }).pipe(Effect.provide(makeLayer())),
  );

  it.effect("does not let stale replacement suppression hide a user close", () => {
    const ackRequests: HttpClientRequest.HttpClientRequest[] = [];
    return Effect.gen(function* () {
      const notifications = yield* ElectronNotification.ElectronNotification;

      yield* notifications.show(notification);
      const firstRendered = notificationInstances[0]!;
      firstRendered.close.mockImplementation(() => undefined);

      yield* notifications.show(notification);
      const secondRendered = notificationInstances[1]!;
      firstRendered.listeners.get("close")?.();
      secondRendered.listeners.get("close")?.();
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      assert.equal(ackRequests.length, 1);
      assert.deepEqual(requestBodyJson(ackRequests[0]!), {
        notificationId: "notification-1",
        ackToken: "ack-token",
        action: "closed",
      });
      assert.deepEqual(queuedEvents(yield* notifications.drainActions), [
        {
          notificationId: "notification-1",
          action: "closed",
          deepLink: "/environment/thread",
        },
      ]);
    }).pipe(Effect.provide(makeLayer({ ackRequests })));
  });

  it.effect("does not claim delivery when native notifications are unsupported", () =>
    Effect.gen(function* () {
      notificationIsSupportedMock.mockReturnValue(false);
      const notifications = yield* ElectronNotification.ElectronNotification;

      const shown = yield* notifications.show(notification);

      assert.equal(shown, false);
      assert.equal(notificationInstances.length, 0);
    }).pipe(Effect.provide(makeLayer())),
  );
});
