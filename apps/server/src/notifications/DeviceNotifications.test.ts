import * as NodeServices from "@effect/platform-node/NodeServices";
import { afterEach, assert, expect, it, vi } from "@effect/vitest";
import type { ServerNotificationStreamEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import webPush, { WebPushError } from "web-push";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as DeviceNotifications from "./DeviceNotifications.ts";
import * as WebPushEndpointGuard from "./WebPushEndpointGuard.ts";

const makeNotificationsLayer = (guardLayer = WebPushEndpointGuard.layer) =>
  DeviceNotifications.layer.pipe(
    Layer.provide(guardLayer),
    Layer.provide(ServerSecretStore.layer),
    Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-notifications-test-" })),
  );

const makeNotificationsLayerForBaseDir = (
  baseDir: string,
  guardLayer: Layer.Layer<WebPushEndpointGuard.WebPushEndpointGuard> = WebPushEndpointGuard.layer,
) =>
  DeviceNotifications.layer.pipe(
    Layer.provide(guardLayer),
    Layer.provide(ServerSecretStore.layer),
    Layer.provide(ServerConfig.layerTest(process.cwd(), baseDir)),
  );

const takeEvent = (queue: Queue.Queue<ServerNotificationStreamEvent>) => Queue.take(queue);

const makeGuardLayer = (
  resolve: Parameters<typeof WebPushEndpointGuard.make>[0],
): Layer.Layer<WebPushEndpointGuard.WebPushEndpointGuard> =>
  Layer.succeed(WebPushEndpointGuard.WebPushEndpointGuard, WebPushEndpointGuard.make(resolve));

const webPushSubscription = (endpoint = "https://push.example.net/send") => ({
  endpoint,
  expirationTime: null,
  keys: {
    p256dh: "p256dh-key",
    auth: "auth-key",
  },
});

const makeRebindingGuardLayer = () => {
  let resolutionAttempt = 0;
  return makeGuardLayer(() =>
    Effect.sync(() => {
      resolutionAttempt += 1;
      return [
        resolutionAttempt === 1
          ? { address: "93.184.216.34", family: 4 as const }
          : { address: "169.254.169.254", family: 4 as const },
      ];
    }),
  );
};

afterEach(() => {
  vi.restoreAllMocks();
});

it.layer(NodeServices.layer)("DeviceNotifications.layer", (it) => {
  it.effect("counts registered desktop clients as delivered devices", () =>
    Effect.gen(function* () {
      const notifications = yield* DeviceNotifications.DeviceNotifications;

      yield* notifications.registerDevice({
        deviceId: "desktop-1",
        deviceKind: "desktop",
        deviceLabel: "Desktop app",
      });

      const config = yield* notifications.getConfig;
      const result = yield* notifications.notify({
        title: "Task finished",
        body: "The scheduled task finished.",
      });

      assert.isAbove(config.vapidPublicKey.length, 20);
      assert.equal(result.deliveredDevices, 1);
    }).pipe(Effect.provide(makeNotificationsLayer())),
  );

  it.effect("surfaces device persistence failures during registration", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-notifications-persist-fails-",
      });
      const storePath = path.join(baseDir, "userdata", "notification-devices.json");

      const result = yield* Effect.gen(function* () {
        const notifications = yield* DeviceNotifications.DeviceNotifications;
        yield* fs.makeDirectory(storePath);
        const registrationExit = yield* Effect.exit(
          notifications.registerDevice({
            deviceId: "desktop-persist-fails",
            deviceKind: "desktop",
            deviceLabel: "Unpersistable desktop",
          }),
        );
        const notification = yield* notifications.notify({
          title: "Should not count failed registration",
        });
        return { registrationExit, deliveredDevices: notification.deliveredDevices };
      }).pipe(Effect.provide(makeNotificationsLayerForBaseDir(baseDir)));

      assert.isTrue(Exit.isFailure(result.registrationExit));
      assert.equal(result.deliveredDevices, 0);
    }).pipe(Effect.scoped),
  );

  it.effect("keeps expired web-push cleanup best-effort when persistence fails", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-notifications-cleanup-fails-",
      });
      const storePath = path.join(baseDir, "userdata", "notification-devices.json");
      const sendNotification = vi
        .spyOn(webPush, "sendNotification")
        .mockRejectedValueOnce(
          new WebPushError("Subscription expired", 410, {}, "", "https://push.example/device"),
        );

      const result = yield* Effect.gen(function* () {
        const notifications = yield* DeviceNotifications.DeviceNotifications;
        yield* notifications.registerDevice({
          deviceId: "stale-web-push",
          deviceKind: "web-push",
          subscription: {
            endpoint: "https://push.example/device",
            expirationTime: null,
            keys: {
              p256dh: "p256dh",
              auth: "auth",
            },
          },
        });
        yield* fs.remove(storePath);
        yield* fs.makeDirectory(storePath);
        const notification = yield* notifications.notify({
          title: "Expired device cleanup should not fail notify",
        });
        return { notification, sendCalls: sendNotification.mock.calls.length };
      }).pipe(
        Effect.provide(
          makeNotificationsLayerForBaseDir(
            baseDir,
            makeGuardLayer(() => Effect.succeed([{ address: "93.184.216.34", family: 4 }])),
          ),
        ),
        Effect.ensuring(Effect.sync(() => sendNotification.mockRestore())),
      );

      assert.equal(result.notification.deliveredDevices, 0);
      assert.equal(result.sendCalls, 1);
    }).pipe(Effect.scoped),
  );

  it.effect("requires the notification ACK token before broadcasting dismissal", () =>
    Effect.gen(function* () {
      const notifications = yield* DeviceNotifications.DeviceNotifications;
      const eventQueue = yield* Queue.unbounded<ServerNotificationStreamEvent>();
      const eventFiber = yield* notifications.events.pipe(
        Stream.runForEach((event) => Queue.offer(eventQueue, event)),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.gen(function* () {
        yield* Effect.yieldNow;

        const sent = yield* notifications.notify({
          title: "Needs attention",
          deepLink: "/threads/environment-1/thread-1",
        });
        const showEvent = yield* takeEvent(eventQueue);
        if (showEvent.type !== "show") {
          assert.fail(`Expected show event, got ${showEvent.type}`);
        }

        const rejected = yield* notifications.ackNotification(
          {
            notificationId: sent.notificationId,
            ackToken: "wrong-token",
            action: "opened",
          },
          { requireAckToken: true },
        );
        assert.equal(rejected.accepted, false);

        const accepted = yield* notifications.ackNotification(
          {
            notificationId: sent.notificationId,
            ackToken: showEvent.notification.ackToken,
            action: "opened",
          },
          { requireAckToken: true },
        );
        const dismissEvent = yield* takeEvent(eventQueue);

        assert.equal(accepted.accepted, true);
        if (dismissEvent.type !== "dismiss") {
          assert.fail(`Expected dismiss event, got ${dismissEvent.type}`);
        }
        assert.equal(dismissEvent.notificationId, sent.notificationId);
      }).pipe(Effect.ensuring(Fiber.interrupt(eventFiber).pipe(Effect.ignore)));
    }).pipe(Effect.provide(makeNotificationsLayer())),
  );

  it.effect("rejects private DNS web-push endpoints before registration persists", () =>
    Effect.gen(function* () {
      const sendNotification = vi
        .spyOn(webPush, "sendNotification")
        .mockResolvedValue({ statusCode: 201, body: "", headers: {} });
      const notifications = yield* DeviceNotifications.DeviceNotifications;

      const exit = yield* notifications
        .registerDevice({
          deviceId: "web-1",
          deviceKind: "web-push",
          subscription: webPushSubscription(),
        })
        .pipe(Effect.exit);
      const result = yield* notifications.notify({ title: "Should not deliver" });

      assert.isTrue(exit._tag === "Failure");
      if (exit._tag === "Failure") {
        assert.isTrue(exit.cause.toString().includes("ServerNotificationEndpointError"));
      }
      assert.equal(result.deliveredDevices, 0);
      expect(sendNotification).not.toHaveBeenCalled();
    }).pipe(
      Effect.provide(
        makeNotificationsLayer(
          makeGuardLayer(() => Effect.succeed([{ address: "10.0.0.10", family: 4 }])),
        ),
      ),
    ),
  );

  it.effect("revalidates stored web-push endpoints before sending", () =>
    Effect.gen(function* () {
      const sendNotification = vi
        .spyOn(webPush, "sendNotification")
        .mockResolvedValue({ statusCode: 201, body: "", headers: {} });
      const notifications = yield* DeviceNotifications.DeviceNotifications;

      yield* notifications.registerDevice({
        deviceId: "web-1",
        deviceKind: "web-push",
        subscription: webPushSubscription(),
      });
      const result = yield* notifications.notify({ title: "Blocked by send guard" });

      assert.equal(result.deliveredDevices, 0);
      expect(sendNotification).not.toHaveBeenCalled();
    }).pipe(Effect.provide(makeNotificationsLayer(makeRebindingGuardLayer()))),
  );

  it.effect("sends public web-push endpoints with a pinned agent", () =>
    Effect.gen(function* () {
      const sendNotification = vi
        .spyOn(webPush, "sendNotification")
        .mockResolvedValue({ statusCode: 201, body: "", headers: {} });
      const notifications = yield* DeviceNotifications.DeviceNotifications;

      yield* notifications.registerDevice({
        deviceId: "web-1",
        deviceKind: "web-push",
        subscription: webPushSubscription(
          "https://updates.push.services.mozilla.com/wpush/v2/test",
        ),
      });
      const result = yield* notifications.notify({ title: "Delivered" });

      assert.equal(result.deliveredDevices, 1);
      expect(sendNotification).toHaveBeenCalledTimes(1);
      expect(sendNotification.mock.calls[0]?.[2]?.agent).toBeDefined();
    }).pipe(
      Effect.provide(
        makeNotificationsLayer(
          makeGuardLayer(() => Effect.succeed([{ address: "93.184.216.34", family: 4 }])),
        ),
      ),
    ),
  );

  it.effect("expires undismissed active notifications", () =>
    Effect.gen(function* () {
      const notifications = yield* DeviceNotifications.DeviceNotifications;
      const eventQueue = yield* Queue.unbounded<ServerNotificationStreamEvent>();
      const eventFiber = yield* notifications.events.pipe(
        Stream.runForEach((event) => Queue.offer(eventQueue, event)),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.gen(function* () {
        yield* Effect.yieldNow;

        const first = yield* notifications.notify({
          title: "First task finished",
        });
        const firstEvent = yield* takeEvent(eventQueue);
        if (firstEvent.type !== "show") {
          assert.fail(`Expected show event, got ${firstEvent.type}`);
        }

        yield* TestClock.adjust("24 hours");
        yield* TestClock.adjust("1 millis");

        const second = yield* notifications.notify({
          title: "Second task finished",
        });
        const secondEvent = yield* takeEvent(eventQueue);
        if (secondEvent.type !== "show") {
          assert.fail(`Expected show event, got ${secondEvent.type}`);
        }

        const expiredAck = yield* notifications.ackNotification(
          {
            notificationId: first.notificationId,
            ackToken: firstEvent.notification.ackToken,
            action: "opened",
          },
          { requireAckToken: true },
        );
        const currentAck = yield* notifications.ackNotification(
          {
            notificationId: second.notificationId,
            ackToken: secondEvent.notification.ackToken,
            action: "opened",
          },
          { requireAckToken: true },
        );

        assert.equal(expiredAck.accepted, false);
        assert.equal(currentAck.accepted, true);
      }).pipe(Effect.ensuring(Fiber.interrupt(eventFiber).pipe(Effect.ignore)));
    }).pipe(Effect.provide(Layer.merge(makeNotificationsLayer(), TestClock.layer()))),
  );
});
