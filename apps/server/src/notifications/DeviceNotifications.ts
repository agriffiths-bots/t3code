import {
  IsoDateTime,
  NonNegativeInt,
  ServerDeviceNotification,
  ServerNotificationEndpointError,
  type ServerNotificationAckAction,
  type ServerNotificationAckInput,
  type ServerNotificationAckResult,
  type ServerNotificationConfig,
  ServerNotificationPersistenceError,
  type ServerNotificationRegisterInput,
  type ServerNotificationRegisterResult,
  type ServerNotificationStreamEvent,
  type ServerNotifyInput,
  type ServerNotifyResult,
  ServerWebPushSubscription,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Data from "effect/Data";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import webPush, { type PushSubscription, WebPushError } from "web-push";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import { writeFileStringAtomically } from "../atomicWrite.ts";
import * as WebPushEndpointGuard from "./WebPushEndpointGuard.ts";

const VAPID_KEYS_SECRET = "web-push-vapid-keys";
const DEVICE_STORE_FILE = "notification-devices.json";
const VAPID_SUBJECT = "mailto:t3code@localhost";
const ACTIVE_NOTIFICATION_TTL_MILLIS = 24 * 60 * 60 * 1000;
const MAX_ACTIVE_NOTIFICATIONS = 1_000;

const NotificationDeviceRecord = Schema.Struct({
  deviceId: Schema.String,
  deviceKind: Schema.Literals(["web-push", "desktop"]),
  deviceLabel: Schema.optional(Schema.String),
  userAgent: Schema.optional(Schema.String),
  ackUrl: Schema.optional(Schema.String),
  subscription: Schema.optional(ServerWebPushSubscription),
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
type NotificationDeviceRecord = typeof NotificationDeviceRecord.Type;

const NotificationDeviceStore = Schema.Struct({
  version: Schema.Literal(1),
  devices: Schema.Array(NotificationDeviceRecord),
});
type NotificationDeviceStore = typeof NotificationDeviceStore.Type;

const VapidKeyPair = Schema.Struct({
  publicKey: Schema.String,
  privateKey: Schema.String,
});
type VapidKeyPair = typeof VapidKeyPair.Type;

const NotificationShowPayload = Schema.Struct({
  kind: Schema.Literal("show"),
  notification: ServerDeviceNotification,
  ackUrl: Schema.optional(Schema.String),
});

const NotificationDismissPayload = Schema.Struct({
  kind: Schema.Literal("dismiss"),
  notificationId: Schema.String,
});

interface NotificationState {
  readonly devices: ReadonlyMap<string, NotificationDeviceRecord>;
  readonly activeNotifications: ReadonlyMap<string, ActiveNotificationEntry>;
  readonly subscribers: ReadonlySet<Queue.Queue<ServerNotificationStreamEvent>>;
}

export class DeviceNotifications extends Context.Service<
  DeviceNotifications,
  {
    readonly getConfig: Effect.Effect<ServerNotificationConfig>;
    readonly registerDevice: (
      input: ServerNotificationRegisterInput,
    ) => Effect.Effect<
      ServerNotificationRegisterResult,
      ServerNotificationEndpointError | ServerNotificationPersistenceError
    >;
    readonly ackNotification: (
      input: ServerNotificationAckInput,
      options?: { readonly requireAckToken?: boolean },
    ) => Effect.Effect<ServerNotificationAckResult>;
    readonly notify: (input: ServerNotifyInput) => Effect.Effect<ServerNotifyResult>;
    readonly events: Stream.Stream<ServerNotificationStreamEvent>;
  }
>()("t3/notifications/DeviceNotifications") {}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

class WebPushSendError extends Data.TaggedError("WebPushSendError")<{
  readonly cause: unknown;
}> {}

interface ActiveNotificationEntry {
  readonly notification: ServerDeviceNotification;
  readonly expiresAtMillis: number;
}

function toPushSubscription(record: NotificationDeviceRecord): PushSubscription | null {
  if (record.subscription === undefined) {
    return null;
  }
  return {
    endpoint: record.subscription.endpoint,
    expirationTime: record.subscription.expirationTime,
    keys: {
      p256dh: record.subscription.keys.p256dh,
      auth: record.subscription.keys.auth,
    },
  };
}

function notificationTopic(notificationId: string): string {
  return notificationId.replace(/[^A-Za-z0-9_-]/g, "").slice(0, 32) || "t3-notification";
}

function serializeStore(devices: ReadonlyMap<string, NotificationDeviceRecord>): string {
  const store: NotificationDeviceStore = {
    version: 1,
    devices: Array.from(devices.values()).toSorted((left, right) =>
      left.deviceId.localeCompare(right.deviceId),
    ),
  };
  return `${encodeDeviceStoreJson(store)}\n`;
}

const NotificationDeviceStoreJson = Schema.fromJsonString(NotificationDeviceStore);
const VapidKeyPairJson = Schema.fromJsonString(VapidKeyPair);
const NotificationShowPayloadJson = Schema.fromJsonString(NotificationShowPayload);
const NotificationDismissPayloadJson = Schema.fromJsonString(NotificationDismissPayload);
const decodeDeviceStoreJson = Schema.decodeEffect(NotificationDeviceStoreJson);
const decodeVapidKeyPairJson = Schema.decodeEffect(VapidKeyPairJson);
const encodeDeviceStoreJson = Schema.encodeSync(NotificationDeviceStoreJson);
const encodeVapidKeyPairJson = Schema.encodeSync(VapidKeyPairJson);
const encodeNotificationShowPayloadJson = Schema.encodeSync(NotificationShowPayloadJson);
const encodeNotificationDismissPayloadJson = Schema.encodeSync(NotificationDismissPayloadJson);

const readDeviceStore = Effect.fn("DeviceNotifications.readDeviceStore")(function* (
  filePath: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const raw = yield* fs
    .readFileString(filePath)
    .pipe(
      Effect.catch((cause) =>
        cause.reason._tag === "NotFound"
          ? Effect.succeed(null)
          : Effect.logWarning("Failed to read notification device store", { cause }).pipe(
              Effect.as(null),
            ),
      ),
    );
  if (raw === null) {
    return new Map();
  }
  const parsed = yield* decodeDeviceStoreJson(raw).pipe(
    Effect.catch((cause) =>
      Effect.logWarning("Failed to decode notification device store", { cause }).pipe(
        Effect.as({ version: 1, devices: [] } satisfies NotificationDeviceStore),
      ),
    ),
  );
  return new Map(parsed.devices.map((device) => [device.deviceId, device]));
});

const persistDeviceStore = Effect.fn("DeviceNotifications.persistDeviceStore")(function* (
  filePath: string,
  devices: ReadonlyMap<string, NotificationDeviceRecord>,
  operation: ServerNotificationPersistenceError["operation"],
) {
  yield* writeFileStringAtomically({
    filePath,
    contents: serializeStore(devices),
  }).pipe(
    Effect.mapError(
      (cause) =>
        new ServerNotificationPersistenceError({
          operation,
          detail: "Failed to persist notification device store.",
          cause,
        }),
    ),
    Effect.tapError((cause) =>
      Effect.logWarning("Failed to persist notification device store", { cause }),
    ),
  );
});

const getOrCreateVapidKeys = Effect.fn("DeviceNotifications.getOrCreateVapidKeys")(function* (
  secrets: ServerSecretStore.ServerSecretStore["Service"],
) {
  const existing = yield* secrets.get(VAPID_KEYS_SECRET);
  if (Option.isSome(existing)) {
    const decoded = yield* decodeVapidKeyPairJson(textDecoder.decode(existing.value)).pipe(
      Effect.option,
    );
    if (Option.isSome(decoded)) {
      return decoded.value;
    }
  }

  const generated = webPush.generateVAPIDKeys();
  const keyPair: VapidKeyPair = {
    publicKey: generated.publicKey,
    privateKey: generated.privateKey,
  };
  yield* secrets
    .set(VAPID_KEYS_SECRET, textEncoder.encode(encodeVapidKeyPairJson(keyPair)))
    .pipe(
      Effect.catch((cause) =>
        Effect.logWarning("Failed to persist generated web-push VAPID keys", { cause }),
      ),
    );
  return keyPair;
});

function upsertDevice(
  devices: ReadonlyMap<string, NotificationDeviceRecord>,
  input: ServerNotificationRegisterInput,
  timestamp: string,
): ReadonlyMap<string, NotificationDeviceRecord> {
  const current = devices.get(input.deviceId);
  const next = new Map(devices);
  next.set(input.deviceId, {
    deviceId: input.deviceId,
    deviceKind: input.deviceKind,
    ...(input.deviceLabel === undefined ? {} : { deviceLabel: input.deviceLabel }),
    ...(input.userAgent === undefined ? {} : { userAgent: input.userAgent }),
    ...(input.ackUrl === undefined ? {} : { ackUrl: input.ackUrl }),
    ...(input.subscription === undefined ? {} : { subscription: input.subscription }),
    createdAt: current?.createdAt ?? timestamp,
    updatedAt: timestamp,
  });
  return next;
}

function isExpiredPushError(error: WebPushSendError): boolean {
  return (
    error.cause instanceof WebPushError &&
    (error.cause.statusCode === 404 || error.cause.statusCode === 410)
  );
}

function makeDismissPayload(notificationId: string): string {
  return encodeNotificationDismissPayloadJson({
    kind: "dismiss",
    notificationId,
  });
}

function makeShowPayload(
  notification: ServerDeviceNotification,
  ackUrl: string | undefined,
): string {
  return encodeNotificationShowPayloadJson({
    kind: "show",
    notification,
    ...(ackUrl === undefined ? {} : { ackUrl }),
  });
}

function pruneActiveNotifications(
  activeNotifications: ReadonlyMap<string, ActiveNotificationEntry>,
  nowMillis: number,
): ReadonlyMap<string, ActiveNotificationEntry> {
  const pruned = new Map(
    Array.from(activeNotifications).filter(([, entry]) => entry.expiresAtMillis > nowMillis),
  );

  while (pruned.size > MAX_ACTIVE_NOTIFICATIONS) {
    const oldestKey = pruned.keys().next().value;
    if (oldestKey === undefined) {
      break;
    }
    pruned.delete(oldestKey);
  }

  return pruned;
}

const makeNotification = Effect.fn("DeviceNotifications.makeNotification")(function* (
  input: ServerNotifyInput,
) {
  const crypto = yield* Crypto.Crypto;
  const notificationId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
  const ackToken = yield* crypto.randomBytes(24).pipe(
    Effect.map((bytes) => Buffer.from(bytes).toString("base64url")),
    Effect.orDie,
  );
  const createdAt = yield* nowIso;
  return {
    notificationId,
    ackToken,
    title: input.title,
    ...(input.body === undefined ? {} : { body: input.body }),
    ...(input.deepLink === undefined ? {} : { deepLink: input.deepLink }),
    createdAt: IsoDateTime.make(createdAt),
    requireInteraction: input.requireInteraction ?? true,
  } satisfies ServerDeviceNotification;
});

export const make = Effect.fn("DeviceNotifications.make")(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const crypto = yield* Crypto.Crypto;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const endpointGuard = yield* WebPushEndpointGuard.WebPushEndpointGuard;
  const vapidKeys = yield* getOrCreateVapidKeys(secrets);
  webPush.setVapidDetails(VAPID_SUBJECT, vapidKeys.publicKey, vapidKeys.privateKey);

  const storePath = path.join(config.stateDir, DEVICE_STORE_FILE);
  const initialDevices = yield* readDeviceStore(storePath).pipe(
    Effect.provideService(FileSystem.FileSystem, fileSystem),
  );
  const persistDevices = (
    devices: ReadonlyMap<string, NotificationDeviceRecord>,
    operation: ServerNotificationPersistenceError["operation"],
  ) =>
    persistDeviceStore(storePath, devices, operation).pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
    );
  const makeDeviceNotification = (input: ServerNotifyInput) =>
    makeNotification(input).pipe(Effect.provideService(Crypto.Crypto, crypto));
  const state = yield* SynchronizedRef.make<NotificationState>({
    devices: initialDevices,
    activeNotifications: new Map(),
    subscribers: new Set(),
  });

  const publish = Effect.fn("DeviceNotifications.publish")(function* (
    event: ServerNotificationStreamEvent,
  ) {
    const snapshot = yield* SynchronizedRef.get(state);
    yield* Effect.forEach(
      snapshot.subscribers,
      (subscriber) => Queue.offer(subscriber, event).pipe(Effect.ignore),
      { discard: true },
    );
  });

  const removeDevice = Effect.fn("DeviceNotifications.removeDevice")(function* (deviceId: string) {
    yield* SynchronizedRef.modifyEffect(state, (current) => {
      if (!current.devices.has(deviceId)) {
        return Effect.succeed([undefined, current] as const);
      }
      const nextDevices = new Map(current.devices);
      nextDevices.delete(deviceId);
      return persistDevices(nextDevices, "remove-device").pipe(
        Effect.as([undefined, { ...current, devices: nextDevices }] as const),
      );
    });
  });

  const sendWebPush = Effect.fn("DeviceNotifications.sendWebPush")(function* (
    device: NotificationDeviceRecord,
    payload: string,
    notificationId: string,
  ) {
    const subscription = toPushSubscription(device);
    if (subscription === null) {
      return false;
    }
    const guardedEndpoint = yield* endpointGuard.prepare(subscription.endpoint).pipe(
      Effect.catch((cause) =>
        Effect.logWarning("Rejected unsafe web push endpoint before send", {
          cause,
          deviceId: device.deviceId,
        }).pipe(Effect.as(null)),
      ),
    );
    if (guardedEndpoint === null) {
      return false;
    }
    return yield* Effect.tryPromise({
      try: () =>
        webPush.sendNotification(subscription, payload, {
          agent: guardedEndpoint.agent,
          TTL: 60 * 60 * 24,
          urgency: "high",
          topic: notificationTopic(notificationId),
        }),
      catch: (cause) => new WebPushSendError({ cause }),
    }).pipe(
      Effect.as(true),
      Effect.catch((cause) =>
        isExpiredPushError(cause)
          ? removeDevice(device.deviceId).pipe(
              Effect.catch((removeCause) =>
                Effect.logWarning("Failed to remove expired notification device", {
                  cause: removeCause,
                  deviceId: device.deviceId,
                }),
              ),
              Effect.as(false),
            )
          : Effect.logWarning("Failed to send web push notification", {
              cause,
              deviceId: device.deviceId,
            }).pipe(Effect.as(false)),
      ),
    );
  });

  const getConfig: DeviceNotifications["Service"]["getConfig"] = Effect.succeed({
    vapidPublicKey: vapidKeys.publicKey,
  });

  const registerDevice: DeviceNotifications["Service"]["registerDevice"] = Effect.fn(
    "DeviceNotifications.registerDevice",
  )(function* (input) {
    if (input.subscription !== undefined) {
      yield* endpointGuard.prepare(input.subscription.endpoint);
    }
    const timestamp = yield* nowIso;
    yield* SynchronizedRef.modifyEffect(state, (current) => {
      const nextDevices = upsertDevice(current.devices, input, timestamp);
      return persistDevices(nextDevices, "register-device").pipe(
        Effect.as([undefined, { ...current, devices: nextDevices }] as const),
      );
    });
    return {
      deviceId: input.deviceId,
      vapidPublicKey: vapidKeys.publicKey,
    };
  });

  const notify: DeviceNotifications["Service"]["notify"] = Effect.fn("DeviceNotifications.notify")(
    function* (input) {
      const notification = yield* makeDeviceNotification(input);
      const nowMillis = yield* Clock.currentTimeMillis;
      const event: ServerNotificationStreamEvent = { type: "show", notification };
      const devices = yield* SynchronizedRef.modify(state, (current) => {
        const nextActive = new Map(
          pruneActiveNotifications(current.activeNotifications, nowMillis),
        );
        nextActive.set(notification.notificationId, {
          notification,
          expiresAtMillis: nowMillis + ACTIVE_NOTIFICATION_TTL_MILLIS,
        });
        const boundedActive = pruneActiveNotifications(nextActive, nowMillis);
        return [current.devices, { ...current, activeNotifications: boundedActive }] as const;
      });

      yield* publish(event);
      const delivered = yield* Effect.forEach(
        devices.values(),
        (device) =>
          device.deviceKind === "web-push"
            ? sendWebPush(
                device,
                makeShowPayload(notification, device.ackUrl),
                notification.notificationId,
              )
            : Effect.succeed(false),
        { concurrency: 8 },
      );

      return {
        notificationId: notification.notificationId,
        deliveredDevices: NonNegativeInt.make(
          delivered.filter(Boolean).length +
            Array.from(devices.values()).filter((device) => device.deviceKind === "desktop").length,
        ),
      };
    },
  );

  const ackNotification: DeviceNotifications["Service"]["ackNotification"] = Effect.fn(
    "DeviceNotifications.ackNotification",
  )(function* (input, options) {
    const dismissedActions: ReadonlySet<ServerNotificationAckAction> = new Set([
      "opened",
      "dismissed",
      "closed",
    ]);
    if (!dismissedActions.has(input.action)) {
      return { notificationId: input.notificationId, accepted: false };
    }

    const nowMillis = yield* Clock.currentTimeMillis;
    const result = yield* SynchronizedRef.modify(
      state,
      (
        current,
      ): readonly [
        {
          readonly notification: ServerDeviceNotification | null;
          readonly devices: ReadonlyMap<string, NotificationDeviceRecord>;
        },
        NotificationState,
      ] => {
        const activeNotifications = pruneActiveNotifications(
          current.activeNotifications,
          nowMillis,
        );
        const existing = activeNotifications.get(input.notificationId);
        if (!existing) {
          return [
            { notification: null, devices: current.devices },
            { ...current, activeNotifications },
          ] as const;
        }
        if (options?.requireAckToken && input.ackToken !== existing.notification.ackToken) {
          return [
            { notification: null, devices: current.devices },
            { ...current, activeNotifications },
          ] as const;
        }
        const nextActive = new Map(activeNotifications);
        nextActive.delete(input.notificationId);
        return [
          { notification: existing.notification, devices: current.devices },
          { ...current, activeNotifications: nextActive },
        ] as const;
      },
    );
    if (result.notification === null) {
      return { notificationId: input.notificationId, accepted: false };
    }

    const dismissEvent: ServerNotificationStreamEvent = {
      type: "dismiss",
      notificationId: input.notificationId,
    };
    yield* publish(dismissEvent);
    yield* Effect.forEach(
      result.devices.values(),
      (device) =>
        device.deviceKind === "web-push"
          ? sendWebPush(device, makeDismissPayload(input.notificationId), input.notificationId)
          : Effect.void,
      { concurrency: 8, discard: true },
    );
    return { notificationId: input.notificationId, accepted: true };
  });

  const acquireSubscriber = Effect.fn("DeviceNotifications.acquireSubscriber")(function* () {
    const queue = yield* Queue.unbounded<ServerNotificationStreamEvent>();
    yield* SynchronizedRef.update(state, (current) => ({
      ...current,
      subscribers: new Set([...current.subscribers, queue]),
    }));
    return queue;
  });

  const releaseSubscriber = Effect.fn("DeviceNotifications.releaseSubscriber")(function* (
    queue: Queue.Queue<ServerNotificationStreamEvent>,
  ) {
    yield* SynchronizedRef.update(state, (current) => {
      const subscribers = new Set(current.subscribers);
      subscribers.delete(queue);
      return { ...current, subscribers };
    });
    yield* Queue.shutdown(queue);
  });

  const events = Stream.scoped(
    Stream.fromEffect(Effect.acquireRelease(acquireSubscriber(), releaseSubscriber)),
  ).pipe(Stream.flatMap((queue) => Stream.fromQueue(queue)));

  return DeviceNotifications.of({
    getConfig,
    registerDevice,
    ackNotification,
    notify,
    events,
  });
});

export const layer = Layer.effect(DeviceNotifications, make());
