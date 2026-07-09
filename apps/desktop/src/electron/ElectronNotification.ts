import type {
  DesktopNotificationActionEvent,
  ServerDeviceNotification,
  ServerNotificationAckAction,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";
import * as Electron from "electron";

import * as DesktopBackendPool from "../backend/DesktopBackendPool.ts";
import * as IpcChannels from "../ipc/channels.ts";
import * as DesktopWindow from "../window/DesktopWindow.ts";
import * as ElectronWindow from "./ElectronWindow.ts";

export class ElectronNotification extends Context.Service<
  ElectronNotification,
  {
    readonly isSupported: Effect.Effect<boolean>;
    readonly show: (notification: ServerDeviceNotification) => Effect.Effect<boolean>;
    readonly close: (notificationId: string) => Effect.Effect<void>;
  }
>()("@t3tools/desktop/electron/ElectronNotification") {}

function canShowNativeNotifications(): boolean {
  const notificationConstructor = Electron.Notification as typeof Electron.Notification & {
    readonly isSupported?: () => boolean;
  };
  return notificationConstructor.isSupported?.() ?? true;
}

interface NativeNotificationEntry {
  readonly notification: ServerDeviceNotification;
  readonly rendered: Electron.Notification;
}

export const make = Effect.gen(function* () {
  const backendPool = yield* DesktopBackendPool.DesktopBackendPool;
  const electronWindow = yield* ElectronWindow.ElectronWindow;
  const desktopWindow = yield* DesktopWindow.DesktopWindow;
  const httpClient = yield* HttpClient.HttpClient;
  const activeNotifications = yield* Ref.make(new Map<string, NativeNotificationEntry>());
  const suppressedCloseNotifications = new Set<string>();
  const context = yield* Effect.context();
  const runFork = Effect.runForkWith(context);

  const runDetached = <E>(effect: Effect.Effect<void, E>) => {
    runFork(
      effect.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to handle desktop notification event", { cause }),
        ),
      ),
    );
  };

  const acknowledgeNotification = Effect.fn("desktop.notification.acknowledge")(function* (
    notification: ServerDeviceNotification,
    action: ServerNotificationAckAction,
  ) {
    yield* Effect.gen(function* () {
      const primary = yield* backendPool.primary;
      const config = yield* primary.currentConfig;
      if (Option.isNone(config)) {
        return;
      }

      const ackUrl = new URL("/api/notifications/ack", config.value.httpBaseUrl);
      const request = HttpClientRequest.post(ackUrl.toString()).pipe(
        HttpClientRequest.bodyJsonUnsafe({
          notificationId: notification.notificationId,
          ackToken: notification.ackToken,
          action,
        }),
      );
      yield* httpClient.execute(request).pipe(Effect.flatMap(HttpClientResponse.filterStatusOk));
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Failed to acknowledge desktop notification", { cause }),
      ),
    );
  });

  const sendActionToWindow = Effect.fn("desktop.notification.sendActionToWindow")(function* (
    window: Electron.BrowserWindow,
    event: DesktopNotificationActionEvent,
  ) {
    const send = Effect.sync(() => {
      if (window.isDestroyed()) {
        return;
      }
      window.webContents.send(IpcChannels.NOTIFICATION_ACTION_CHANNEL, event);
    });

    if (window.isDestroyed()) {
      return;
    }

    if (window.webContents.isLoadingMainFrame()) {
      window.webContents.once("did-finish-load", () => runDetached(send));
      return;
    }

    yield* send;
  });

  const emitAction = Effect.fn("desktop.notification.emitAction")(function* (
    event: DesktopNotificationActionEvent,
  ) {
    if (event.action === "opened") {
      const window = yield* desktopWindow.revealOrCreateMain;
      yield* sendActionToWindow(window, event);
      return;
    }
    yield* electronWindow.sendAll(IpcChannels.NOTIFICATION_ACTION_CHANNEL, event);
  });

  const forgetNotification = (notificationId: string) =>
    Ref.update(activeNotifications, (current) => {
      const next = new Map(current);
      next.delete(notificationId);
      return next;
    });

  const closeNotification = Effect.fn("desktop.notification.close")(function* (
    notificationId: string,
    suppressAction: boolean,
  ) {
    const active = (yield* Ref.get(activeNotifications)).get(notificationId);
    if (!active) {
      return;
    }
    if (suppressAction) {
      suppressedCloseNotifications.add(notificationId);
    }
    active.rendered.close();
  });

  const show = Effect.fn("desktop.notification.show")(function* (
    notification: ServerDeviceNotification,
  ) {
    if (!canShowNativeNotifications()) {
      return false;
    }

    yield* closeNotification(notification.notificationId, true);
    const rendered = new Electron.Notification({
      title: notification.title,
      body: notification.body ?? "",
      ...(notification.requireInteraction ? { timeoutType: "never" as const } : {}),
      silent: false,
    });

    rendered.on("click", () => {
      suppressedCloseNotifications.add(notification.notificationId);
      runDetached(acknowledgeNotification(notification, "opened"));
      runDetached(
        Effect.gen(function* () {
          yield* emitAction({
            notificationId: notification.notificationId,
            action: "opened",
            ...(notification.deepLink === undefined ? {} : { deepLink: notification.deepLink }),
          });
          yield* closeNotification(notification.notificationId, true);
        }),
      );
    });
    rendered.on("close", () => {
      runDetached(
        Effect.gen(function* () {
          yield* forgetNotification(notification.notificationId);
          if (suppressedCloseNotifications.delete(notification.notificationId)) {
            return;
          }
          runDetached(acknowledgeNotification(notification, "closed"));
          yield* emitAction({
            notificationId: notification.notificationId,
            action: "closed",
            ...(notification.deepLink === undefined ? {} : { deepLink: notification.deepLink }),
          });
        }),
      );
    });

    yield* Ref.update(activeNotifications, (current) => {
      const next = new Map(current);
      next.set(notification.notificationId, { notification, rendered });
      return next;
    });
    rendered.show();
    return true;
  });

  return ElectronNotification.of({
    isSupported: Effect.sync(canShowNativeNotifications),
    show,
    close: (notificationId) => closeNotification(notificationId, true),
  });
});

export const layer = Layer.effect(ElectronNotification, make);
