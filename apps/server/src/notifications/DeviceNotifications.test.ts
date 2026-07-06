import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it, vi } from "@effect/vitest";
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

const makeNotificationsLayer = () =>
  DeviceNotifications.layer.pipe(
    Layer.provide(ServerSecretStore.layer),
    Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-notifications-test-" })),
  );

const makeNotificationsLayerForBaseDir = (baseDir: string) =>
  DeviceNotifications.layer.pipe(
    Layer.provide(ServerSecretStore.layer),
    Layer.provide(ServerConfig.layerTest(process.cwd(), baseDir)),
  );

const takeEvent = (queue: Queue.Queue<ServerNotificationStreamEvent>) => Queue.take(queue);

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
        Effect.provide(makeNotificationsLayerForBaseDir(baseDir)),
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
