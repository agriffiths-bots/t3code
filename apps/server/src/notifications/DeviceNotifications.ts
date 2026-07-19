import {
  AuthAudienceCeiling,
  DataAudience,
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
  type ServerNotificationRecoveryInput,
  type ServerNotificationRecoveryResult,
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
import * as Encoding from "effect/Encoding";
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
import { canReadDataAudience } from "../auth/audienceDataPolicy.ts";
import { signPayload, timingSafeEqualBase64Url } from "../auth/utils.ts";
import * as ServerConfig from "../config.ts";
import { writeFileStringAtomically } from "../atomicWrite.ts";
import * as WebPushEndpointGuard from "./WebPushEndpointGuard.ts";

const VAPID_KEYS_SECRET = "web-push-vapid-keys";
const RECOVERY_TOKEN_DERIVATION_KEY_SECRET = "push-recovery-token-derivation-key";
const DEVICE_STORE_FILE = "notification-devices.json";
const VAPID_SUBJECT = "mailto:t3code@localhost";
const ACTIVE_NOTIFICATION_TTL_MILLIS = 24 * 60 * 60 * 1000;
const EXPIRED_DEVICE_TTL_MILLIS = 30 * 24 * 60 * 60 * 1000;
const MAX_ACTIVE_NOTIFICATIONS = 1_000;
const MAX_DEVICE_REMOVAL_TOMBSTONES = 100;
const RECOVERY_TOKEN_BYTES = 32;
const DUMMY_RECOVERY_TOKEN_HASH = Encoding.encodeBase64Url(new Uint8Array(32));

const NotificationDeviceRecordFields = {
  deviceId: Schema.String,
  deviceKind: Schema.Literals(["web-push", "desktop"]),
  deviceLabel: Schema.optional(Schema.String),
  userAgent: Schema.optional(Schema.String),
  ackUrl: Schema.optional(Schema.String),
  subscription: Schema.optional(ServerWebPushSubscription),
  status: Schema.optional(Schema.Literals(["active", "expired"])),
  expiredAt: Schema.optional(Schema.String),
  recoveryTokenHash: Schema.optional(Schema.String),
  recoveryReplay: Schema.optional(
    Schema.Struct({
      previousTokenHash: Schema.String,
      oldEndpoint: Schema.String,
      newSubscription: ServerWebPushSubscription,
    }),
  ),
  createdAt: Schema.String,
  updatedAt: Schema.String,
} as const;

const LegacyNotificationDeviceRecord = Schema.Struct({
  ...NotificationDeviceRecordFields,
  audienceCeiling: Schema.optional(AuthAudienceCeiling),
});
const NotificationDeviceRecord = Schema.Struct({
  ...NotificationDeviceRecordFields,
  audienceCeiling: AuthAudienceCeiling,
});
type NotificationDeviceRecord = typeof NotificationDeviceRecord.Type;

const LegacyNotificationDeviceStore = Schema.Struct({
  version: Schema.Literal(1),
  devices: Schema.Array(LegacyNotificationDeviceRecord),
});
const NotificationDeviceStoreV2 = Schema.Struct({
  version: Schema.Literal(2),
  devices: Schema.Array(NotificationDeviceRecord),
});
const NotificationDeviceRemovalReason = Schema.Literals(["push-service-404", "push-service-410"]);
type NotificationDeviceRemovalReason = typeof NotificationDeviceRemovalReason.Type;
const NotificationDeviceRemovalTombstone = Schema.Struct({
  deviceId: Schema.String,
  deviceLabel: Schema.optional(Schema.String),
  platform: Schema.String,
  reason: NotificationDeviceRemovalReason,
  removedAt: Schema.String,
});
type NotificationDeviceRemovalTombstone = typeof NotificationDeviceRemovalTombstone.Type;
const NotificationDeviceStore = Schema.Struct({
  version: Schema.Literal(3),
  devices: Schema.Array(NotificationDeviceRecord),
  tombstones: Schema.Array(NotificationDeviceRemovalTombstone),
});
type NotificationDeviceStore = typeof NotificationDeviceStore.Type;
const PersistedNotificationDeviceStore = Schema.Union([
  LegacyNotificationDeviceStore,
  NotificationDeviceStoreV2,
  NotificationDeviceStore,
]);

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
  readonly removalTombstones: ReadonlyArray<NotificationDeviceRemovalTombstone>;
  readonly activeNotifications: ReadonlyMap<string, ActiveNotificationEntry>;
  readonly subscribers: ReadonlySet<Queue.Queue<AudienceNotificationStreamEntry>>;
}

interface AudienceNotificationStreamEntry {
  readonly event: ServerNotificationStreamEvent;
  readonly dataAudience: DataAudience;
}

export class DeviceNotifications extends Context.Service<
  DeviceNotifications,
  {
    readonly getConfig: Effect.Effect<ServerNotificationConfig>;
    readonly registerDevice: (
      input: ServerNotificationRegisterInput,
      options: { readonly audienceCeiling: AuthAudienceCeiling },
    ) => Effect.Effect<
      ServerNotificationRegisterResult,
      ServerNotificationEndpointError | ServerNotificationPersistenceError
    >;
    readonly recoverSubscription: (
      input: ServerNotificationRecoveryInput,
    ) => Effect.Effect<
      ServerNotificationRecoveryResult | null,
      ServerNotificationEndpointError | ServerNotificationPersistenceError
    >;
    readonly ackNotification: (
      input: ServerNotificationAckInput,
      options?: {
        readonly requireAckToken?: boolean;
        readonly audienceCeiling?: AuthAudienceCeiling;
      },
    ) => Effect.Effect<ServerNotificationAckResult>;
    readonly notify: (
      input: ServerNotifyInput,
      options?: {
        readonly dataAudience: DataAudience;
        readonly resultAudienceCeiling?: AuthAudienceCeiling;
      },
    ) => Effect.Effect<ServerNotifyResult>;
    readonly events: Stream.Stream<ServerNotificationStreamEvent>;
    readonly eventsForAudience: (
      audienceCeiling: AuthAudienceCeiling,
    ) => Stream.Stream<ServerNotificationStreamEvent>;
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
  readonly dataAudience: DataAudience;
  readonly expiresAtMillis: number;
  readonly isRemovalAlert: boolean;
}

interface WebPushDeliveryOptions {
  readonly isRemovalAlert?: boolean;
}

function toPushSubscription(record: NotificationDeviceRecord): PushSubscription | null {
  if (record.status === "expired" || record.subscription === undefined) {
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

function subscriptionMatches(
  record: NotificationDeviceRecord,
  subscription: PushSubscription,
): boolean {
  return (
    record.subscription?.endpoint === subscription.endpoint &&
    record.subscription.expirationTime === subscription.expirationTime &&
    record.subscription.keys.p256dh === subscription.keys.p256dh &&
    record.subscription.keys.auth === subscription.keys.auth
  );
}

function serverSubscriptionsMatch(
  left: ServerWebPushSubscription,
  right: ServerWebPushSubscription,
): boolean {
  return (
    left.endpoint === right.endpoint &&
    left.expirationTime === right.expirationTime &&
    left.keys.p256dh === right.keys.p256dh &&
    left.keys.auth === right.keys.auth
  );
}

function recoveryTokenDerivationPayload(subscription: ServerWebPushSubscription): string {
  return JSON.stringify([
    "push-recovery-subscription-v1",
    subscription.endpoint,
    subscription.expirationTime,
    subscription.keys.p256dh,
    subscription.keys.auth,
  ]);
}

function notificationTopic(notificationId: string): string {
  return notificationId.replace(/[^A-Za-z0-9_-]/g, "").slice(0, 32) || "t3-notification";
}

function serializeStore(
  devices: ReadonlyMap<string, NotificationDeviceRecord>,
  tombstones: ReadonlyArray<NotificationDeviceRemovalTombstone>,
): string {
  const store: NotificationDeviceStore = {
    version: 3,
    devices: Array.from(devices.values()).toSorted((left, right) =>
      left.deviceId.localeCompare(right.deviceId),
    ),
    tombstones,
  };
  return `${encodeDeviceStoreJson(store)}\n`;
}

const NotificationDeviceStoreJson = Schema.fromJsonString(PersistedNotificationDeviceStore);
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
    return {
      devices: new Map<string, NotificationDeviceRecord>(),
      tombstones: [] as ReadonlyArray<NotificationDeviceRemovalTombstone>,
    };
  }
  const parsed = yield* decodeDeviceStoreJson(raw).pipe(
    Effect.catch((cause) =>
      Effect.logWarning("Failed to decode notification device store", { cause }).pipe(
        Effect.as({ version: 3, devices: [], tombstones: [] } satisfies NotificationDeviceStore),
      ),
    ),
  );
  const devices =
    parsed.version === 1
      ? parsed.devices.map((device) => ({
          ...device,
          audienceCeiling: device.audienceCeiling ?? ("factory" as const),
        }))
      : parsed.devices;
  return {
    devices: new Map(devices.map((device) => [device.deviceId, device])),
    tombstones: parsed.version === 3 ? parsed.tombstones : [],
  };
});

const persistDeviceStore = Effect.fn("DeviceNotifications.persistDeviceStore")(function* (
  filePath: string,
  devices: ReadonlyMap<string, NotificationDeviceRecord>,
  tombstones: ReadonlyArray<NotificationDeviceRemovalTombstone>,
  operation: ServerNotificationPersistenceError["operation"],
) {
  yield* writeFileStringAtomically({
    filePath,
    contents: serializeStore(devices, tombstones),
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
      Effect.tapError(() => Effect.logWarning("Failed to decode persisted web-push VAPID keys")),
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
  recoveryTokenHash: string,
  audienceCeiling: AuthAudienceCeiling,
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
    status: "active",
    recoveryTokenHash,
    audienceCeiling,
    createdAt: current?.createdAt ?? timestamp,
    updatedAt: timestamp,
  });
  return next;
}

function expiredPushStatus(error: WebPushSendError): 404 | 410 | null {
  if (
    error.cause instanceof WebPushError &&
    (error.cause.statusCode === 404 || error.cause.statusCode === 410)
  ) {
    return error.cause.statusCode;
  }
  return null;
}

function isRecoveryEligible(record: NotificationDeviceRecord, nowMillis: number): boolean {
  if (record.status !== "expired") {
    return true;
  }
  if (record.expiredAt === undefined) {
    return false;
  }
  const expiredAtMillis = Date.parse(record.expiredAt);
  return (
    Number.isFinite(expiredAtMillis) && nowMillis - expiredAtMillis < EXPIRED_DEVICE_TTL_MILLIS
  );
}

function pruneExpiredDevices(
  devices: ReadonlyMap<string, NotificationDeviceRecord>,
  nowMillis: number,
): ReadonlyMap<string, NotificationDeviceRecord> {
  return new Map(
    Array.from(devices).filter(([, device]) => {
      if (device.status !== "expired" || device.expiredAt === undefined) {
        return true;
      }
      const expiredAtMillis = Date.parse(device.expiredAt);
      return (
        !Number.isFinite(expiredAtMillis) || nowMillis - expiredAtMillis < EXPIRED_DEVICE_TTL_MILLIS
      );
    }),
  );
}

function endpointHost(endpoint: string): string {
  try {
    return new URL(endpoint).hostname;
  } catch {
    return "invalid";
  }
}

function deviceDisplayName(device: NotificationDeviceRecord): string {
  return device.deviceLabel ?? device.deviceId;
}

function devicePlatform(device: NotificationDeviceRecord): string {
  const userAgent = device.userAgent;
  if (userAgent === undefined) return device.deviceKind;
  if (/Android/i.test(userAgent)) return "android";
  if (/iPhone|iPad|iPod/i.test(userAgent)) return "ios";
  if (/Windows/i.test(userAgent)) return "windows";
  if (/Macintosh|Mac OS/i.test(userAgent)) return "macos";
  if (/Linux/i.test(userAgent)) return "linux";
  return device.deviceKind;
}

function appendRemovalTombstone(
  tombstones: ReadonlyArray<NotificationDeviceRemovalTombstone>,
  tombstone: NotificationDeviceRemovalTombstone,
): ReadonlyArray<NotificationDeviceRemovalTombstone> {
  return [...tombstones, tombstone].slice(-MAX_DEVICE_REMOVAL_TOMBSTONES);
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
  const recoveryTokenDerivationKey = yield* secrets.getOrCreateRandom(
    RECOVERY_TOKEN_DERIVATION_KEY_SECRET,
    RECOVERY_TOKEN_BYTES,
  );
  webPush.setVapidDetails(VAPID_SUBJECT, vapidKeys.publicKey, vapidKeys.privateKey);

  const storePath = path.join(config.stateDir, DEVICE_STORE_FILE);
  const initialStore = yield* readDeviceStore(storePath).pipe(
    Effect.provideService(FileSystem.FileSystem, fileSystem),
  );
  const persistStore = (
    devices: ReadonlyMap<string, NotificationDeviceRecord>,
    tombstones: ReadonlyArray<NotificationDeviceRemovalTombstone>,
    operation: ServerNotificationPersistenceError["operation"],
  ) =>
    persistDeviceStore(storePath, devices, tombstones, operation).pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
    );
  const makeDeviceNotification = (input: ServerNotifyInput) =>
    makeNotification(input).pipe(Effect.provideService(Crypto.Crypto, crypto));
  const hashRecoveryToken = (token: string) =>
    crypto
      .digest("SHA-256", textEncoder.encode(token))
      .pipe(Effect.map(Encoding.encodeBase64Url), Effect.orDie);
  const deriveRecoveryToken = (subscription: ServerWebPushSubscription) =>
    signPayload(recoveryTokenDerivationPayload(subscription), recoveryTokenDerivationKey);
  const makeRecoveryCredential = Effect.fn("DeviceNotifications.makeRecoveryCredential")(function* (
    subscription: ServerWebPushSubscription | undefined,
  ) {
    const recoveryToken =
      subscription === undefined
        ? yield* crypto
            .randomBytes(RECOVERY_TOKEN_BYTES)
            .pipe(Effect.map(Encoding.encodeBase64Url), Effect.orDie)
        : deriveRecoveryToken(subscription);
    const recoveryTokenHash = yield* hashRecoveryToken(recoveryToken);
    return { recoveryToken, recoveryTokenHash };
  });
  const state = yield* SynchronizedRef.make<NotificationState>({
    devices: initialStore.devices,
    removalTombstones: initialStore.tombstones,
    activeNotifications: new Map(),
    subscribers: new Set(),
  });

  const publish = Effect.fn("DeviceNotifications.publish")(function* (
    entry: AudienceNotificationStreamEntry,
  ) {
    const snapshot = yield* SynchronizedRef.get(state);
    yield* Effect.forEach(
      snapshot.subscribers,
      (subscriber) => Queue.offer(subscriber, entry).pipe(Effect.ignore),
      { discard: true },
    );
  });

  const prepareNotificationDelivery = Effect.fn("DeviceNotifications.prepareNotificationDelivery")(
    function* (input: ServerNotifyInput, dataAudience: DataAudience, isRemovalAlert: boolean) {
      const notification = yield* makeDeviceNotification(input);
      const nowMillis = yield* Clock.currentTimeMillis;
      const event: ServerNotificationStreamEvent = { type: "show", notification };
      const devices = yield* SynchronizedRef.modify(state, (current) => {
        const nextActive = new Map(
          pruneActiveNotifications(current.activeNotifications, nowMillis),
        );
        nextActive.set(notification.notificationId, {
          notification,
          dataAudience,
          expiresAtMillis: nowMillis + ACTIVE_NOTIFICATION_TTL_MILLIS,
          isRemovalAlert,
        });
        const boundedActive = pruneActiveNotifications(nextActive, nowMillis);
        const activeDevices = new Map(
          Array.from(current.devices).filter(([, device]) => device.status !== "expired"),
        );
        return [activeDevices, { ...current, activeNotifications: boundedActive }] as const;
      });

      yield* publish({ event, dataAudience });
      return {
        notification,
        visibleDevices: Array.from(devices.values()).filter((device) =>
          canReadDataAudience(device.audienceCeiling, dataAudience),
        ),
      };
    },
  );

  const expireDevice = Effect.fn("DeviceNotifications.expireDevice")(function* (
    deviceId: string,
    failedSubscription: PushSubscription,
    timestamp: string,
    reason: NotificationDeviceRemovalReason,
  ) {
    return yield* SynchronizedRef.modifyEffect(state, (current) => {
      const existing = current.devices.get(deviceId);
      if (
        existing === undefined ||
        existing.status === "expired" ||
        !subscriptionMatches(existing, failedSubscription)
      ) {
        return Effect.succeed([null, current] as const);
      }
      const nextDevices = new Map(current.devices);
      nextDevices.set(deviceId, {
        ...existing,
        status: "expired",
        expiredAt: timestamp,
        updatedAt: timestamp,
      });
      const tombstone: NotificationDeviceRemovalTombstone = {
        deviceId,
        ...(existing.deviceLabel === undefined ? {} : { deviceLabel: existing.deviceLabel }),
        platform: devicePlatform(existing),
        reason,
        removedAt: timestamp,
      };
      const nextTombstones = appendRemovalTombstone(current.removalTombstones, tombstone);
      const remainingDeviceCount = Array.from(nextDevices.values()).filter(
        (device) => device.status !== "expired",
      ).length;
      return persistStore(nextDevices, nextTombstones, "expire-device").pipe(
        Effect.as([
          { tombstone, remainingDeviceCount },
          {
            ...current,
            devices: nextDevices,
            removalTombstones: nextTombstones,
          },
        ] as const),
      );
    });
  });

  const expireAfterPushServiceRejection: (
    device: NotificationDeviceRecord,
    subscription: PushSubscription,
    reason: NotificationDeviceRemovalReason,
    suppressRemovalAlert: boolean,
  ) => Effect.Effect<boolean> = Effect.fn("DeviceNotifications.expireAfterPushServiceRejection")(
    function* (device, subscription, reason, suppressRemovalAlert) {
      const timestamp = yield* nowIso;
      const removal = yield* expireDevice(device.deviceId, subscription, timestamp, reason).pipe(
        Effect.catch((expireCause) =>
          Effect.logWarning("Failed to expire notification device", {
            cause: expireCause,
            deviceId: device.deviceId,
            deviceLabel: deviceDisplayName(device),
            platform: devicePlatform(device),
            reason,
          }).pipe(Effect.as(null)),
        ),
      );
      if (removal === null) {
        return false;
      }

      yield* Effect.logWarning("Web push subscription removed after push service rejection", {
        deviceId: removal.tombstone.deviceId,
        deviceLabel: removal.tombstone.deviceLabel ?? removal.tombstone.deviceId,
        platform: removal.tombstone.platform,
        endpointHost: endpointHost(subscription.endpoint),
        reason: removal.tombstone.reason,
      });
      if (suppressRemovalAlert || removal.remainingDeviceCount === 0) {
        return false;
      }

      const removalAlert = Effect.gen(function* () {
        const prepared = yield* prepareNotificationDelivery(
          {
            title: "Push subscription removed",
            body: `Push subscription for ${deviceDisplayName(device)} was removed (expired). Reopen T3 on that device to re-register.`,
          },
          "private",
          true,
        );
        yield* Effect.forEach(
          prepared.visibleDevices,
          (remainingDevice) =>
            remainingDevice.deviceKind === "web-push"
              ? sendWebPush(
                  remainingDevice,
                  makeShowPayload(prepared.notification, remainingDevice.ackUrl),
                  prepared.notification.notificationId,
                  { isRemovalAlert: true },
                )
              : Effect.void,
          { concurrency: 8, discard: true },
        );
      }).pipe(
        Effect.catchCause((alertCause) =>
          Effect.logWarning("Failed to dispatch push subscription removal alert", {
            cause: alertCause,
            deviceId: removal.tombstone.deviceId,
            reason: removal.tombstone.reason,
          }),
        ),
      );
      yield* removalAlert.pipe(Effect.forkDetach({ startImmediately: true }), Effect.asVoid);
      return false;
    },
  );

  const sendWebPush: (
    device: NotificationDeviceRecord,
    payload: string,
    notificationId: string,
    options?: WebPushDeliveryOptions,
  ) => Effect.Effect<boolean> = Effect.fn("DeviceNotifications.sendWebPush")(
    function* (device, payload, notificationId, options) {
      const subscription = toPushSubscription(device);
      if (subscription === null) {
        if (device.status !== "expired") {
          yield* Effect.logWarning("Skipped web push delivery without an active subscription", {
            deviceId: device.deviceId,
            deviceLabel: deviceDisplayName(device),
            platform: devicePlatform(device),
          });
        }
        return false;
      }
      const guardedEndpoint = yield* endpointGuard.prepare(subscription.endpoint).pipe(
        Effect.catch((cause) =>
          Effect.logWarning("Rejected unsafe web push endpoint before send", {
            cause,
            deviceId: device.deviceId,
            deviceLabel: deviceDisplayName(device),
            platform: devicePlatform(device),
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
        Effect.catch((cause) => {
          const status = expiredPushStatus(cause);
          if (status === null) {
            return Effect.logWarning("Failed to send web push notification", {
              cause,
              deviceId: device.deviceId,
              deviceLabel: deviceDisplayName(device),
              platform: devicePlatform(device),
            }).pipe(Effect.as(false));
          }
          const reason: NotificationDeviceRemovalReason = `push-service-${status}`;
          if (options?.isRemovalAlert === true) {
            return Effect.logWarning("Failed to deliver push subscription removal alert", {
              deviceId: device.deviceId,
              deviceLabel: deviceDisplayName(device),
              platform: devicePlatform(device),
              endpointHost: endpointHost(subscription.endpoint),
              reason,
            }).pipe(
              Effect.andThen(expireAfterPushServiceRejection(device, subscription, reason, true)),
            );
          }
          return expireAfterPushServiceRejection(device, subscription, reason, false);
        }),
      );
    },
  );

  const getConfig: DeviceNotifications["Service"]["getConfig"] = Effect.succeed({
    vapidPublicKey: vapidKeys.publicKey,
  });

  const registerDevice: DeviceNotifications["Service"]["registerDevice"] = Effect.fn(
    "DeviceNotifications.registerDevice",
  )(function* (input, options) {
    if (input.subscription !== undefined) {
      yield* endpointGuard.prepare(input.subscription.endpoint);
    }
    const timestamp = yield* nowIso;
    const nowMillis = yield* Clock.currentTimeMillis;
    const recovery = yield* makeRecoveryCredential(input.subscription);
    yield* SynchronizedRef.modifyEffect(state, (current) => {
      const retainedDevices = pruneExpiredDevices(current.devices, nowMillis);
      const nextDevices = upsertDevice(
        retainedDevices,
        input,
        timestamp,
        recovery.recoveryTokenHash,
        options.audienceCeiling,
      );
      return persistStore(nextDevices, current.removalTombstones, "register-device").pipe(
        Effect.as([undefined, { ...current, devices: nextDevices }] as const),
      );
    });
    return {
      deviceId: input.deviceId,
      vapidPublicKey: vapidKeys.publicKey,
      recoveryToken: recovery.recoveryToken,
    };
  });

  const recoverSubscription: DeviceNotifications["Service"]["recoverSubscription"] = Effect.fn(
    "DeviceNotifications.recoverSubscription",
  )(function* (input) {
    const timestamp = yield* nowIso;
    const nowMillis = yield* Clock.currentTimeMillis;
    const candidateHash = yield* hashRecoveryToken(input.recoveryToken);
    const rotatedToken = deriveRecoveryToken(input.newSubscription);
    const rotatedTokenHash = yield* hashRecoveryToken(rotatedToken);

    return yield* SynchronizedRef.modifyEffect(state, (current) => {
      const retainedDevices = pruneExpiredDevices(current.devices, nowMillis);
      let matchingDevice:
        | { readonly device: NotificationDeviceRecord; readonly kind: "rotate" | "replay" }
        | undefined;
      let comparedStoredHash = false;
      for (const device of retainedDevices.values()) {
        if (!isRecoveryEligible(device, nowMillis)) {
          continue;
        }
        if (device.subscription?.endpoint === input.oldEndpoint) {
          comparedStoredHash = true;
          if (
            timingSafeEqualBase64Url(
              candidateHash,
              device.recoveryTokenHash ?? DUMMY_RECOVERY_TOKEN_HASH,
            )
          ) {
            matchingDevice ??= { device, kind: "rotate" };
          }
        }
        const replay = device.recoveryReplay;
        if (
          replay?.oldEndpoint === input.oldEndpoint &&
          serverSubscriptionsMatch(replay.newSubscription, input.newSubscription)
        ) {
          comparedStoredHash = true;
          const previousTokenMatches = timingSafeEqualBase64Url(
            candidateHash,
            replay.previousTokenHash,
          );
          const rotatedTokenStillCurrent = timingSafeEqualBase64Url(
            rotatedTokenHash,
            device.recoveryTokenHash ?? DUMMY_RECOVERY_TOKEN_HASH,
          );
          if (previousTokenMatches && rotatedTokenStillCurrent) {
            matchingDevice ??= { device, kind: "replay" };
          }
        }
      }
      if (!comparedStoredHash) {
        timingSafeEqualBase64Url(candidateHash, DUMMY_RECOVERY_TOKEN_HASH);
      }

      if (matchingDevice === undefined) {
        if (retainedDevices.size === current.devices.size) {
          return Effect.succeed([null, current] as const);
        }
        return persistStore(
          retainedDevices,
          current.removalTombstones,
          "purge-expired-devices",
        ).pipe(
          Effect.catch(() => Effect.void),
          Effect.as([null, { ...current, devices: retainedDevices }] as const),
        );
      }

      if (matchingDevice.kind === "replay") {
        const result = { recoveryToken: rotatedToken };
        if (retainedDevices.size === current.devices.size) {
          return Effect.succeed([result, current] as const);
        }
        return persistStore(
          retainedDevices,
          current.removalTombstones,
          "purge-expired-devices",
        ).pipe(
          Effect.catch(() => Effect.void),
          Effect.as([result, { ...current, devices: retainedDevices }] as const),
        );
      }

      return endpointGuard.prepare(input.newSubscription.endpoint).pipe(
        Effect.andThen(() => {
          const nextDevices = new Map(retainedDevices);
          const {
            expiredAt: _expiredAt,
            recoveryReplay: _recoveryReplay,
            ...activeDevice
          } = matchingDevice.device;
          nextDevices.set(matchingDevice.device.deviceId, {
            ...activeDevice,
            subscription: input.newSubscription,
            status: "active",
            recoveryTokenHash: rotatedTokenHash,
            recoveryReplay: {
              previousTokenHash: candidateHash,
              oldEndpoint: input.oldEndpoint,
              newSubscription: input.newSubscription,
            },
            updatedAt: timestamp,
          });
          return persistStore(nextDevices, current.removalTombstones, "recover-device").pipe(
            Effect.as([
              { recoveryToken: rotatedToken },
              { ...current, devices: nextDevices },
            ] as const),
          );
        }),
      );
    });
  });

  const notify: DeviceNotifications["Service"]["notify"] = Effect.fn("DeviceNotifications.notify")(
    function* (input, options) {
      const dataAudience = options?.dataAudience ?? "private";
      const resultAudienceCeiling = options?.resultAudienceCeiling ?? "private";
      const prepared = yield* prepareNotificationDelivery(input, dataAudience, false);
      const delivered = yield* Effect.forEach(
        prepared.visibleDevices,
        (device) =>
          device.deviceKind === "web-push"
            ? sendWebPush(
                device,
                makeShowPayload(prepared.notification, device.ackUrl),
                prepared.notification.notificationId,
              )
            : Effect.succeed(false),
        { concurrency: 8 },
      );

      return {
        notificationId: prepared.notification.notificationId,
        deliveredDevices: NonNegativeInt.make(
          prepared.visibleDevices.reduce(
            (count, device, index) =>
              canReadDataAudience(resultAudienceCeiling, device.audienceCeiling) &&
              (device.deviceKind === "desktop" || delivered[index] === true)
                ? count + 1
                : count,
            0,
          ),
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
          readonly notification: ActiveNotificationEntry | null;
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
        if (
          options?.audienceCeiling !== undefined &&
          !canReadDataAudience(options.audienceCeiling, existing.dataAudience)
        ) {
          return [
            { notification: null, devices: current.devices },
            { ...current, activeNotifications },
          ] as const;
        }
        const nextActive = new Map(activeNotifications);
        nextActive.delete(input.notificationId);
        return [
          { notification: existing, devices: current.devices },
          { ...current, activeNotifications: nextActive },
        ] as const;
      },
    );
    const dismissedNotification = result.notification;
    if (dismissedNotification === null) {
      return { notificationId: input.notificationId, accepted: false };
    }

    const dismissEvent: ServerNotificationStreamEvent = {
      type: "dismiss",
      notificationId: input.notificationId,
    };
    yield* publish({ event: dismissEvent, dataAudience: dismissedNotification.dataAudience });
    yield* Effect.forEach(
      Array.from(result.devices.values()).filter((device) =>
        canReadDataAudience(device.audienceCeiling, dismissedNotification.dataAudience),
      ),
      (device) =>
        device.deviceKind === "web-push" && device.status !== "expired"
          ? sendWebPush(device, makeDismissPayload(input.notificationId), input.notificationId, {
              isRemovalAlert: dismissedNotification.isRemovalAlert,
            })
          : Effect.void,
      { concurrency: 8, discard: true },
    );
    return { notificationId: input.notificationId, accepted: true };
  });

  const acquireSubscriber = Effect.fn("DeviceNotifications.acquireSubscriber")(function* () {
    const queue = yield* Queue.unbounded<AudienceNotificationStreamEntry>();
    yield* SynchronizedRef.update(state, (current) => ({
      ...current,
      subscribers: new Set([...current.subscribers, queue]),
    }));
    return queue;
  });

  const releaseSubscriber = Effect.fn("DeviceNotifications.releaseSubscriber")(function* (
    queue: Queue.Queue<AudienceNotificationStreamEntry>,
  ) {
    yield* SynchronizedRef.update(state, (current) => {
      const subscribers = new Set(current.subscribers);
      subscribers.delete(queue);
      return { ...current, subscribers };
    });
    yield* Queue.shutdown(queue);
  });

  const audienceEvents = Stream.scoped(
    Stream.fromEffect(Effect.acquireRelease(acquireSubscriber(), releaseSubscriber)),
  ).pipe(Stream.flatMap((queue) => Stream.fromQueue(queue)));
  const events = audienceEvents.pipe(Stream.map((entry) => entry.event));
  const eventsForAudience: DeviceNotifications["Service"]["eventsForAudience"] = (
    audienceCeiling,
  ) =>
    audienceEvents.pipe(
      Stream.filter((entry) => canReadDataAudience(audienceCeiling, entry.dataAudience)),
      Stream.map((entry) => entry.event),
    );

  return DeviceNotifications.of({
    getConfig,
    registerDevice,
    recoverSubscription,
    ackNotification,
    notify,
    events,
    eventsForAudience,
  });
});

export const layer = Layer.effect(DeviceNotifications, make());
