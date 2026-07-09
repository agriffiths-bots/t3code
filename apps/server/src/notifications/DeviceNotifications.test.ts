import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off - The web-push integration test needs raw headers from a local HTTPS push endpoint.
import type * as NodeHttp from "node:http";
// @effect-diagnostics-next-line nodeBuiltinImport:off - web-push sends through Node HTTPS, so the mock endpoint and trusted agent must use Node HTTPS.
import * as NodeHttps from "node:https";
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

const TEST_PUSH_SERVER_KEY = `-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCn9YCqyzB0eWu4
RrcB22XQfy8EatXSCIZcuREFAHJfnBI7kimBcVZ+OZCYcZJYKFZgVhqfOUjj2nzT
Y1Vw9JBopn8Sj0m/ymPWDSppgCTQn2D0q+XfNFFlo0C6rQ9aNnFPeBSth5vqrPse
9MC+M2xOzDVTe7OAPBzqS0FnslbQluozWGnW9zPmaQOr2VkLIbShpz8j+pW1uxKL
iSqkizeycvDEKOOjN15JB0c7M6DeLJwsyyk9f4nZIizZQIHk3Iqzk8S2QJaSOws3
YU7Ftj3GPDk4HKbP2Ew4iJ41+8NjNKVNg/hbe5OAu+6HcXX3bMLW7siqDtPEF3qQ
1pepTdDhAgMBAAECggEACL+xRNRCMSL9ATOPx1U8E7T4SEDfY+olQ5GxSQMpsgDB
c2Bs8IzsjjUl6sbpslVhkHGTv+z+Nr40B9fSBMj8d7MKhfc4Rnj+o9D6P1YZNNPE
e2Iz3mFNhx+OmNq4ZMDWvKa63wIkFUCO8SEbJB0uHFmnQnK8Wibnc63ZWZMw0fsg
vcxsv0HxhLx4a6mEs8KwOKWPnNVOnBwKO/ij8+hEszOa4im9YtoJsEXCFcNlV3IF
/D67m2wUxX2u1HGjL7k+qvbxRhIo0zXnW3GgpzTQlfYRPKhasyGGqitmbK3PnVqu
2arP9xaTcYaP8ntvAYBZ6zPLgojkL20rbDyTjN97dQKBgQDmRNxvbWMs1AfopgY0
T+h0xs9rnPAbuw7bW5TBzqy1dGmLVU7oAXYYyqGK1GjM7K1ELQVXParHT4TJD6R2
atgAPBpwBuo25np/lEdjnMu66+DRO9xB4frSySiw8kmy4HjLl2GV2QS9rXUy0iY/
RK9h4nX+P7/OTSeLtQmYamqubQKBgQC6ui84vCD9XaIqx5ZC1odMKmy4S4Xg0Q9w
r4rT/mdlgRykzIZKGliP/hxeOuyQ4D2s+ynGg50G9hetAa+4XBUybjRNd041H5O7
vIY3g1Hm0FWUXPIFUSLWY+o9iUT34e5cNO5//Q1qHLEg562HDkbcl5Rj4MhqP0lC
KWXJwyGTxQKBgCgXARHdR5D8cUwN67Kb3urF2kLwFdWeIZ4LOcDsTKFi3SVG+u/l
oTv7u1hCVuSmqBvggreHov4EWCfxMz7ypxyTWj761TgttFIV7L/pAodOnduPwm8t
+s9L+mukIzSjZCR9/J1sJSko0+i/Ma1+NdKi7MwRKUGvqRznrf39OQmZAoGAdTxO
R6W+ZLU0Cv3ytpYwrj54siEgti0sL4jXdhBlZJJypHmQ2te9wPI/Z15BhxhzQLcU
3IFnxqYd6U6EwBB4cohEqFp+rNXdkGJmNlZpxqwI/zR386SkZcynlekodyXP3O6S
y6LamEPZZhpvlbr9/KPi0+6ehi6j1TleohW0cC0CgYAdVoQ8qAjhalA1C3jYxcJb
42kHTn14mIMruH6/hr3S0e/BFGIOFRNlHeu5XXzQJTWiclCuO6SLJPK0Xc2EB5ta
zhJgNbpS6HXyyXi7xSOOVuc20o3U1tTSpWzJaukCkZmCLLXIwzuM1J/wBRs9HYr2
HVLFuTQyjglEsZfSOmlaxw==
-----END PRIVATE KEY-----`;

const TEST_PUSH_SERVER_CERT = `-----BEGIN CERTIFICATE-----
MIIDJTCCAg2gAwIBAgIUcFdXq6esSYgutJl8OFoqNAYqrIMwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDcwOTAwMDYyNloXDTM2MDcw
NjAwMDYyNlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAp/WAqsswdHlruEa3Adtl0H8vBGrV0giGXLkRBQByX5wS
O5IpgXFWfjmQmHGSWChWYFYanzlI49p802NVcPSQaKZ/Eo9Jv8pj1g0qaYAk0J9g
9Kvl3zRRZaNAuq0PWjZxT3gUrYeb6qz7HvTAvjNsTsw1U3uzgDwc6ktBZ7JW0Jbq
M1hp1vcz5mkDq9lZCyG0oac/I/qVtbsSi4kqpIs3snLwxCjjozdeSQdHOzOg3iyc
LMspPX+J2SIs2UCB5NyKs5PEtkCWkjsLN2FOxbY9xjw5OBymz9hMOIieNfvDYzSl
TYP4W3uTgLvuh3F192zC1u7Iqg7TxBd6kNaXqU3Q4QIDAQABo28wbTAdBgNVHQ4E
FgQUfZmQ9xZ5SkpZqXRpBn5hfipmAmwwHwYDVR0jBBgwFoAUfZmQ9xZ5SkpZqXRp
Bn5hfipmAmwwDwYDVR0TAQH/BAUwAwEB/zAaBgNVHREEEzARgglsb2NhbGhvc3SH
BH8AAAEwDQYJKoZIhvcNAQELBQADggEBAG/Yy72nBZ6B1RfZvqlm/1WCFJpNUZI5
2pHU02QkndQHJ7gIrI2F6A9tmKUP6l/E/+XzCttIy5JefKanzWd8yylQA0B+8Oph
7jAb06QrwP+YliS7yhaLu/E/Yq3XbTmkY6cNk5gxymyTidc89AYrOIsQp70gzAk8
vuHOjS7cVclCl/s7TNbtp1U1PTkQtfU+gcDxn1LpMGCs9xQdHT8B5xJb0zNM5e3Q
rzh5lnyxAlQn3b1ra+gXOu9X4DYPkldpEPvl0Ckq132y4GTWKq/3C1+92+8X7dir
Hp+komj6aaPGBoStmFx8DMAOWmdGRiHB/dob5t05L/JHiHQOB613ZR8=
-----END CERTIFICATE-----`;

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

const makePushSubscriptionKeys = () => {
  const ecdh = NodeCrypto.createECDH("prime256v1");
  ecdh.generateKeys();
  return {
    p256dh: ecdh.getPublicKey().toString("base64url"),
    auth: NodeCrypto.randomBytes(16).toString("base64url"),
  };
};

const webPushSubscription = (endpoint = "https://push.example.net/send") => ({
  endpoint,
  expirationTime: null,
  keys: makePushSubscriptionKeys(),
});

interface MockPushRequest {
  readonly method: string | undefined;
  readonly url: string | undefined;
  readonly headers: NodeHttp.IncomingHttpHeaders;
  readonly body: Buffer;
}

const makeMockPushServer = Effect.acquireRelease(
  Effect.promise(
    () =>
      new Promise<{
        readonly agent: NodeHttps.Agent;
        readonly endpoint: string;
        readonly requests: MockPushRequest[];
        readonly server: NodeHttps.Server;
      }>((resolve, reject) => {
        const requests: MockPushRequest[] = [];
        const server = NodeHttps.createServer(
          { key: TEST_PUSH_SERVER_KEY, cert: TEST_PUSH_SERVER_CERT },
          (request, response) => {
            const chunks: Buffer[] = [];
            request.on("data", (chunk) => {
              chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
            });
            request.on("end", () => {
              requests.push({
                method: request.method,
                url: request.url,
                headers: request.headers,
                body: Buffer.concat(chunks),
              });
              response.statusCode = 201;
              response.end();
            });
          },
        );
        server.once("error", reject);
        server.listen(0, "127.0.0.1", () => {
          const address = server.address();
          if (!address || typeof address === "string") {
            reject(new Error("Expected a TCP address for mock push server."));
            return;
          }
          resolve({
            agent: new NodeHttps.Agent({ ca: TEST_PUSH_SERVER_CERT }),
            endpoint: `https://127.0.0.1:${address.port}/push`,
            requests,
            server,
          });
        });
      }),
  ),
  ({ server }) =>
    Effect.promise(
      () =>
        new Promise<void>((resolve, reject) => {
          server.close((error) => {
            if (error) {
              reject(error);
              return;
            }
            resolve();
          });
        }),
    ),
);

const makeAcceptedEndpointGuardLayer = (
  agent: NodeHttps.Agent,
): Layer.Layer<WebPushEndpointGuard.WebPushEndpointGuard> =>
  Layer.succeed(
    WebPushEndpointGuard.WebPushEndpointGuard,
    WebPushEndpointGuard.WebPushEndpointGuard.of({
      prepare: (endpoint) =>
        Effect.succeed({
          agent,
          address: { address: "127.0.0.1", family: 4 },
          hostname: new URL(endpoint).hostname,
        }),
    }),
  );

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

  it.effect("sends accepted web-push notifications as POSTs to the push endpoint", () =>
    Effect.gen(function* () {
      const mockPush = yield* makeMockPushServer;

      const result = yield* Effect.gen(function* () {
        const notifications = yield* DeviceNotifications.DeviceNotifications;
        yield* notifications.registerDevice({
          deviceId: "web-push-post",
          deviceKind: "web-push",
          ackUrl: "https://app.example.test/api/notifications/ack",
          subscription: webPushSubscription(mockPush.endpoint),
        });
        return yield* notifications.notify({
          title: "Accepted POST",
          body: "This should reach the mock push service.",
          deepLink: "/environment/thread",
        });
      }).pipe(
        Effect.provide(makeNotificationsLayer(makeAcceptedEndpointGuardLayer(mockPush.agent))),
      );

      assert.equal(result.deliveredDevices, 1);
      assert.equal(mockPush.requests.length, 1);
      const request = mockPush.requests[0]!;
      assert.equal(request.method, "POST");
      assert.equal(request.url, "/push");
      assert.equal(request.headers.ttl, "86400");
      assert.equal(request.headers.urgency, "high");
      assert.equal(request.headers["content-type"], "application/octet-stream");
      assert.isAbove(request.body.byteLength, 0);
    }).pipe(Effect.scoped),
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
