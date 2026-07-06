import * as NodeServices from "@effect/platform-node/NodeServices";
import { afterEach, assert, expect, it, vi } from "@effect/vitest";
import type { ServerNotificationStreamEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import webPush from "web-push";

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
});
