import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../config.ts";
import {
  MatrixBridgeClient,
  MatrixBridgeClientError,
  type MatrixBridgeInboundHandler,
  type MatrixBridgeInboundText,
  type MatrixBridgeOutboundText,
} from "./MatrixBridgeClient.ts";
import { MatrixBridgeConfig, type MatrixBridgeConfigV1 } from "./MatrixBridgeConfig.ts";

export const MEGOLM_ALGORITHM = "m.megolm.v1.aes-sha2";
export const MATRIX_STORE_DIRECTORY_NAME = "matrix-bridge";
export const MATRIX_SYNC_STORE_FILE_NAME = "sync.json";
export const MATRIX_CRYPTO_STORE_DIRECTORY_NAME = "crypto";
/**
 * `RustSdkCryptoStoreType.Sqlite`. The crypto package declares it as a
 * TypeScript `const enum`, so the value is erased from the shipped JavaScript
 * and cannot be read off the loaded module at runtime.
 */
export const MATRIX_CRYPTO_SQLITE_STORE_TYPE = 0;
const MATRIX_DECRYPTED_EVENT = "room.decrypted_event";
const MATRIX_INBOUND_QUEUE_CAPACITY = 64;
/** Retries reuse one ciphertext per transaction, so only in-flight sends are held. */
const MATRIX_ENCRYPTED_PAYLOAD_CAPACITY = 16;
const MATRIX_RECONNECT_MIN_DELAY_MS = 1_000;
const MATRIX_RECONNECT_MAX_DELAY_MS = 15_000;

/** Recoverable transport faults retry forever, at a bounded rate. */
export function reconnectDelayMs(attempt: number): number {
  return Math.min(
    MATRIX_RECONNECT_MIN_DELAY_MS * 2 ** Math.min(attempt, 10),
    MATRIX_RECONNECT_MAX_DELAY_MS,
  );
}

/**
 * Sync filter for the one bridged room. To-device traffic is deliberately
 * unfiltered because room keys arrive there.
 */
export const buildSyncFilter = (roomId: string) => ({
  presence: { types: [] },
  account_data: { types: [] },
  room: {
    rooms: [roomId],
    timeline: {
      limit: 20,
      types: ["m.room.encrypted", "m.room.message", "m.room.member", "m.room.encryption"],
    },
    state: {
      types: ["m.room.member", "m.room.encryption", "m.room.join_rules"],
      lazy_load_members: true,
    },
    ephemeral: { types: [] },
    account_data: { types: [] },
  },
});

export interface MatrixCreateRoomOptions {
  readonly visibility: "private";
  readonly preset: "private_chat";
  readonly is_direct: true;
  readonly invite: ReadonlyArray<string>;
  readonly power_level_content_override: Record<string, unknown>;
  readonly initial_state: ReadonlyArray<{
    readonly type: string;
    readonly state_key: string;
    readonly content: Record<string, unknown>;
  }>;
}

/** Room administration stays with the bot; members may only send messages. */
export const MATRIX_BRIDGE_ADMIN_POWER_LEVEL = 100;

/**
 * One private, invite-only, Megolm-encrypted room; no plaintext fallback.
 *
 * `private_chat` rather than `trusted_private_chat` because the latter promotes
 * every invitee to the creator's power level, which would let a member invite
 * accounts outside the allowlist and read later bridge output.
 */
export const buildEncryptedRoomCreateOptions = (
  invite: ReadonlyArray<string>,
): MatrixCreateRoomOptions => ({
  visibility: "private",
  preset: "private_chat",
  is_direct: true,
  invite: [...invite],
  power_level_content_override: {
    users_default: 0,
    events_default: 0,
    state_default: MATRIX_BRIDGE_ADMIN_POWER_LEVEL,
    invite: MATRIX_BRIDGE_ADMIN_POWER_LEVEL,
    kick: MATRIX_BRIDGE_ADMIN_POWER_LEVEL,
    ban: MATRIX_BRIDGE_ADMIN_POWER_LEVEL,
    redact: MATRIX_BRIDGE_ADMIN_POWER_LEVEL,
  },
  initial_state: [
    { type: "m.room.encryption", state_key: "", content: { algorithm: MEGOLM_ALGORITHM } },
  ],
});

/** Idempotent per transaction ID: a repeated PUT returns the original event. */
export const encryptedSendEndpoint = (roomId: string, transactionId: string): string =>
  `/_matrix/client/v3/rooms/${encodeURIComponent(roomId)}/send/m.room.encrypted/${encodeURIComponent(transactionId)}`;

export type MatrixSdkEventHandler = (roomId: string, event: unknown) => void;

export interface MatrixSdkFilterInfo {
  readonly id: string;
  readonly filter: unknown;
}

/** Persistent sync state; the SDK reads and writes it synchronously. */
export interface MatrixSdkStorageProvider {
  getSyncToken(): string | null;
  setSyncToken(token: string | null): void;
  getFilter(): MatrixSdkFilterInfo | null | undefined;
  setFilter(filter: MatrixSdkFilterInfo): void;
}

export interface MatrixSdkCryptoClient {
  readonly isReady: boolean;
  encryptRoomEvent(roomId: string, eventType: string, content: unknown): Promise<unknown>;
}

/** The slice of `matrix-bot-sdk`'s MatrixClient this bridge depends on. */
export interface MatrixSdkClient {
  readonly crypto?: MatrixSdkCryptoClient | undefined;
  getUserId(): Promise<string>;
  getJoinedRooms(): Promise<ReadonlyArray<string>>;
  getRoomStateEvent(roomId: string, type: string, stateKey: string): Promise<unknown>;
  createRoom(options: MatrixCreateRoomOptions): Promise<string>;
  doRequest(method: string, endpoint: string, qs?: unknown, body?: unknown): Promise<unknown>;
  start(filter?: unknown): Promise<unknown>;
  stop(): void;
  on(event: string, handler: MatrixSdkEventHandler): unknown;
  off(event: string, handler: MatrixSdkEventHandler): unknown;
}

export interface MatrixSdkModule {
  readonly MatrixClient: new (
    homeserverUrl: string,
    accessToken: string,
    storage: MatrixSdkStorageProvider,
    cryptoStore: unknown,
  ) => MatrixSdkClient;
  readonly SimpleFsStorageProvider: new (filename: string) => MatrixSdkStorageProvider;
  readonly RustSdkCryptoStorageProvider: new (storagePath: string, storeType: number) => unknown;
  readonly LogService?: {
    setLogger(logger: {
      info: (module: string, ...rest: ReadonlyArray<unknown>) => void;
      warn: (module: string, ...rest: ReadonlyArray<unknown>) => void;
      error: (module: string, ...rest: ReadonlyArray<unknown>) => void;
      debug: (module: string, ...rest: ReadonlyArray<unknown>) => void;
      trace: (module: string, ...rest: ReadonlyArray<unknown>) => void;
    }): void;
  };
}

export type MatrixSdkLoader = () => Promise<MatrixSdkModule>;

// Resolved through a widened specifier so the optional dependency is never a
// build-time or typecheck-time requirement: an install without it still starts
// a healthy server with the bridge reported unavailable.
const MATRIX_BOT_SDK_SPECIFIER: string = "matrix-bot-sdk";

export const loadMatrixBotSdk: MatrixSdkLoader = async () => {
  const loaded: unknown = await import(MATRIX_BOT_SDK_SPECIFIER);
  const module = loaded as Partial<MatrixSdkModule>;
  if (
    typeof module.MatrixClient !== "function" ||
    typeof module.SimpleFsStorageProvider !== "function" ||
    typeof module.RustSdkCryptoStorageProvider !== "function"
  ) {
    throw new Error("The Matrix SDK did not expose its client and storage constructors.");
  }
  return module as MatrixSdkModule;
};

/** Distinguishes "the homeserver has no such state event" from a read failure. */
const MISSING_STATE_EVENT = "missing-state-event" as const;

/** Matches matrix-bot-sdk's own stored-filter comparison byte for byte. */
const sameSyncFilter = (left: unknown, right: unknown) =>
  JSON.stringify(left) === JSON.stringify(right);

const clientError = (
  operation: "listen" | "send",
  reason: string,
  retryability: "transient" | "permanent",
): MatrixBridgeClientError => new MatrixBridgeClientError({ operation, reason, retryability });

function readRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null ? (value as Record<string, unknown>) : null;
}

function readString(value: unknown, key: string): string | null {
  const record = readRecord(value);
  const field = record?.[key];
  return typeof field === "string" ? field : null;
}

/** Matrix errors carry the HTTP status; anything else is treated as a defect. */
function readStatusCode(cause: unknown): number | null {
  const record = readRecord(cause);
  const status = record?.statusCode ?? record?.status;
  return typeof status === "number" ? status : null;
}

/**
 * 4xx answers are configuration or permission problems that will not fix
 * themselves; everything else (timeouts, 5xx, rate limits) is worth retrying.
 */
function retryabilityFor(cause: unknown): "transient" | "permanent" {
  const status = readStatusCode(cause);
  if (status === null) return "transient";
  if (status === 429) return "transient";
  return status >= 400 && status < 500 ? "permanent" : "transient";
}

const request = <A>(
  operation: "listen" | "send",
  reason: string,
  call: () => Promise<A>,
): Effect.Effect<A, MatrixBridgeClientError> =>
  Effect.tryPromise({
    try: call,
    catch: (cause) => clientError(operation, reason, retryabilityFor(cause)),
  });

function toInboundText(
  event: unknown,
  eventRoomId: string,
  roomId: string,
  botUserId: string,
): MatrixBridgeInboundText | null {
  if (eventRoomId !== roomId) return null;
  const record = readRecord(event);
  if (record === null || record.type !== "m.room.message") return null;
  const eventId = readString(record, "event_id");
  const sender = readString(record, "sender");
  if (eventId === null || sender === null || sender === botUserId) return null;
  const content = readRecord(record.content);
  if (content === null || content.msgtype !== "m.text") return null;
  const body = typeof content.body === "string" ? content.body : null;
  if (body === null) return null;
  const relation = readRecord(content["m.relates_to"]);
  return {
    eventId,
    roomId,
    sender,
    body,
    isEdit: relation?.rel_type === "m.replace",
  };
}

function readNumber(record: Record<string, unknown> | null, key: string): number | null {
  const value = record?.[key];
  return typeof value === "number" ? value : null;
}

/**
 * Only the bot may widen the room. Everyone else is a member who can send
 * messages, so a member's device cannot pull an unlisted account into the room
 * and read later bridge output.
 */
function inviteIsBotOnly(powerLevels: unknown, botUserId: string): boolean {
  const record = readRecord(powerLevels);
  if (record === null) return false;
  const usersDefault = readNumber(record, "users_default") ?? 0;
  // The Matrix default invite level is 0, so an absent value is a failure.
  const inviteLevel = readNumber(record, "invite");
  if (inviteLevel === null) return false;
  const users = readRecord(record.users) ?? {};
  const botLevel = readNumber(users, botUserId) ?? usersDefault;
  const highestMemberLevel = Object.entries(users).reduce(
    (highest, [userId, level]) =>
      userId === botUserId || typeof level !== "number" ? highest : Math.max(highest, level),
    usersDefault,
  );
  return inviteLevel > highestMemberLevel && botLevel >= inviteLevel;
}

/**
 * Fails closed unless the bot is joined to an invite-only, Megolm-encrypted
 * room that only the bot can widen. Shared with the live homeserver smoke so
 * both check the same state.
 */
export const verifyEncryptedRoom = Effect.fn("MatrixBotSdkClient.verifyEncryptedRoom")(function* (
  client: MatrixSdkClient,
  roomId: string,
  botUserId: string,
) {
  const joinedRooms = yield* request("listen", "The bridged Matrix room could not be read.", () =>
    client.getJoinedRooms(),
  );
  if (!joinedRooms.includes(roomId)) {
    return yield* clientError(
      "listen",
      "The Matrix bot is not joined to the configured room.",
      "permanent",
    );
  }

  const joinRules = yield* request(
    "listen",
    "The bridged Matrix room join rules could not be read.",
    () => client.getRoomStateEvent(roomId, "m.room.join_rules", ""),
  );
  if (readString(joinRules, "join_rule") !== "invite") {
    return yield* clientError(
      "listen",
      "The configured Matrix room is not invite-only.",
      "permanent",
    );
  }

  // A missing encryption event is a plaintext room, not a transport fault.
  const encryption = yield* Effect.tryPromise({
    try: () => client.getRoomStateEvent(roomId, "m.room.encryption", ""),
    catch: (cause) =>
      readStatusCode(cause) === 404
        ? MISSING_STATE_EVENT
        : clientError(
            "listen",
            "The bridged Matrix room encryption state could not be read.",
            retryabilityFor(cause),
          ),
  }).pipe(
    Effect.catch((error) =>
      error === MISSING_STATE_EVENT ? Effect.succeed(null) : Effect.fail(error),
    ),
  );
  if (readString(encryption, "algorithm") !== MEGOLM_ALGORITHM) {
    return yield* clientError(
      "listen",
      "The configured Matrix room is not end-to-end encrypted. Encryption cannot be added to a room that already has plaintext history.",
      "permanent",
    );
  }

  const powerLevels = yield* request(
    "listen",
    "The bridged Matrix room power levels could not be read.",
    () => client.getRoomStateEvent(roomId, "m.room.power_levels", ""),
  );
  if (!inviteIsBotOnly(powerLevels, botUserId)) {
    return yield* clientError(
      "listen",
      "The configured Matrix room lets its members invite other accounts, so bridge output could reach someone outside the allowed list.",
      "permanent",
    );
  }
});

/**
 * Registers the sync filter before the cursor is fenced.
 *
 * `MatrixClient.start` creates a missing or mismatched filter itself and clears
 * the stored sync token when it does, which would discard the fence and replay
 * room history on the first sync. Creating the identical filter here makes that
 * branch a no-op.
 */
export const ensureSyncFilter = Effect.fn("MatrixBotSdkClient.ensureSyncFilter")(function* (
  client: MatrixSdkClient,
  storage: MatrixSdkStorageProvider,
  botUserId: string,
  filter: unknown,
) {
  const existing = yield* Effect.try({
    try: () => storage.getFilter(),
    catch: () => clientError("listen", "The Matrix sync store could not be read.", "permanent"),
  });
  // Compared the way the SDK compares it, so a match there is a match here.
  if (existing !== null && existing !== undefined && sameSyncFilter(existing.filter, filter)) {
    return;
  }

  const response = yield* request("listen", "The Matrix sync filter could not be created.", () =>
    client.doRequest(
      "POST",
      `/_matrix/client/v3/user/${encodeURIComponent(botUserId)}/filter`,
      null,
      filter,
    ),
  );
  const filterId = readString(response, "filter_id");
  if (filterId === null) {
    return yield* clientError(
      "listen",
      "The homeserver did not return a Matrix sync filter.",
      "transient",
    );
  }
  yield* Effect.try({
    try: () => {
      // Same order as the SDK: a new filter invalidates any old cursor, and the
      // fence that follows writes the replacement.
      storage.setSyncToken(null);
      storage.setFilter({ id: filterId, filter });
    },
    catch: () =>
      clientError("listen", "The Matrix sync filter could not be persisted.", "transient"),
  });
});

/**
 * Suppresses the catch-up sync's timeline without starving encryption.
 *
 * A fresh store has no cursor, so the SDK's first sync returns whatever the
 * homeserver still holds: room keys and device-list updates that encryption
 * needs, and old timeline messages that must never reach T3 as commands.
 * Discarding that response would acknowledge the to-device messages and leave
 * later events undecryptable, so the response is processed in full and only the
 * bridge's own handlers are gated.
 *
 * `MatrixClient` persists each batch's cursor before it emits that batch's
 * events (`persistTokenAfterSync` is false), so the number of stored cursors
 * identifies the batch being emitted: the first belongs to the catch-up sync,
 * everything after it is live.
 */
export interface FencedSyncStorage {
  /** Handed to the SDK in place of the real provider. */
  readonly storage: MatrixSdkStorageProvider;
  /** False only while the pre-fence catch-up batch is being emitted. */
  readonly acceptsTimelineEvents: () => boolean;
  readonly syncedBatches: () => number;
  /**
   * Completes once the catch-up boundary exists, so nothing is announced as
   * ready while a message the owner sends could still land in the fenced batch.
   */
  readonly awaitSyncBoundary: Effect.Effect<void>;
}

export function createFencedSyncStorage(inner: MatrixSdkStorageProvider): FencedSyncStorage {
  // A store that already has a cursor cannot replay history, so nothing is
  // fenced and no live message is dropped after a restart.
  let fenced = (inner.getSyncToken() ?? "") === "";
  let syncedBatches = 0;
  let boundary = Deferred.makeUnsafe<void>();
  if (!fenced) Deferred.doneUnsafe(boundary, Effect.void);

  const storage: MatrixSdkStorageProvider = {
    getSyncToken: () => inner.getSyncToken(),
    setSyncToken: (token) => {
      // The write comes first: a store that failed to persist the cursor has
      // not advanced the batch, and a retry of the same catch-up must stay
      // fenced rather than count as a second batch.
      inner.setSyncToken(token);
      if (token === null || token === "") {
        // Registering a filter clears the cursor, and the sync that follows is
        // a fresh catch-up: re-arm rather than trusting the state at startup.
        fenced = true;
        syncedBatches = 0;
        boundary = Deferred.makeUnsafe<void>();
      } else {
        syncedBatches += 1;
        Deferred.doneUnsafe(boundary, Effect.void);
      }
    },
    getFilter: () => inner.getFilter(),
    setFilter: (filter) => inner.setFilter(filter),
  };

  return {
    storage,
    acceptsTimelineEvents: () => !fenced || syncedBatches >= 2,
    syncedBatches: () => syncedBatches,
    awaitSyncBoundary: Effect.suspend(() => Deferred.await(boundary)),
  };
}

interface MatrixConnection {
  readonly cryptoStoreGeneration: string;
  readonly roomId: string;
  readonly client: MatrixSdkClient;
  /** Retries must present the ciphertext that was encrypted for the first attempt. */
  readonly encryptedPayloads: Map<string, unknown>;
}

export const make = Effect.fn("MatrixBotSdkClient.make")(function* (
  loadModule: MatrixSdkLoader = loadMatrixBotSdk,
) {
  const configService = yield* MatrixBridgeConfig;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const connectionRef = yield* Ref.make(Option.none<MatrixConnection>());
  /** Rooms created but not yet persisted, keyed by connection generation. */
  const createdRooms = new Map<string, string>();

  const currentConfig = configService.currentConfig;

  /** Emits once the bridge is configured, starting from the current value. */
  const awaitConfigured = configService.statusChanges.pipe(
    Stream.mapEffect(() => currentConfig),
    Stream.filter(Option.isSome),
    Stream.map((config) => config.value),
    Stream.runHead,
    Effect.map(Option.getOrThrow),
  );

  /**
   * Completes when the stored connection identity is replaced or removed.
   * Reconfiguration mints a new crypto-store generation whenever the
   * homeserver, token, or allowlist changes.
   */
  const awaitGenerationReplaced = (cryptoStoreGeneration: string) =>
    configService.statusChanges.pipe(
      Stream.mapEffect(() => currentConfig),
      Stream.filter(
        (config) =>
          Option.isNone(config) || config.value.cryptoStoreGeneration !== cryptoStoreGeneration,
      ),
      Stream.runHead,
      Effect.asVoid,
    );

  /**
   * Completes on a replaced identity or on a fresh `connecting` publication.
   * Resubmitting identical settings keeps the generation, and an operator who
   * fixed a permission problem elsewhere expects that Connect to retry.
   */
  const awaitReconnectSignal = (cryptoStoreGeneration: string) =>
    configService.statusChanges.pipe(
      Stream.mapEffect((status) => Effect.map(currentConfig, (config) => ({ status, config }))),
      Stream.filter(
        ({ status, config }) =>
          Option.isNone(config) ||
          config.value.cryptoStoreGeneration !== cryptoStoreGeneration ||
          status.state === "connecting",
      ),
      Stream.runHead,
      Effect.asVoid,
    );

  const installQuietSdkLogger = (module: MatrixSdkModule) =>
    Effect.sync(() => {
      // The SDK logs room events and request bodies at info/debug. Only the
      // module name of a warning survives, so no body or token can reach a log.
      const drop = () => {};
      module.LogService?.setLogger({
        info: drop,
        debug: drop,
        trace: drop,
        warn: drop,
        error: drop,
      });
    });

  const prepareStoreDirectory = Effect.fn("MatrixBotSdkClient.prepareStoreDirectory")(function* (
    cryptoStoreGeneration: string,
  ) {
    const bridgeDir = path.join(serverConfig.secretsDir, MATRIX_STORE_DIRECTORY_NAME);
    const storeDir = path.join(bridgeDir, cryptoStoreGeneration);
    yield* fs
      .makeDirectory(storeDir, { recursive: true })
      .pipe(
        Effect.mapError(() =>
          clientError("listen", "The Matrix encryption store could not be created.", "transient"),
        ),
      );
    // The store holds device keys and Megolm sessions; it is as sensitive as
    // the secrets directory that contains it.
    yield* Effect.forEach([bridgeDir, storeDir], (directory) => fs.chmod(directory, 0o700)).pipe(
      Effect.mapError(() =>
        clientError("listen", "The Matrix encryption store could not be secured.", "permanent"),
      ),
    );
    return storeDir;
  });

  const ensureRoom = Effect.fn("MatrixBotSdkClient.ensureRoom")(function* (
    client: MatrixSdkClient,
    config: MatrixBridgeConfigV1,
    botUserId: string,
  ) {
    if (config.roomId !== null) {
      yield* verifyEncryptedRoom(client, config.roomId, botUserId);
      return config.roomId;
    }

    // A room created but not yet persisted is adopted on the next attempt.
    // Without this, a secret-store outage would create and invite into a fresh
    // room on every retry.
    const unpersisted = createdRooms.get(config.cryptoStoreGeneration);
    const roomId =
      unpersisted ??
      (yield* request("listen", "The encrypted Matrix room could not be created.", () =>
        client.createRoom(buildEncryptedRoomCreateOptions(config.allowedUserIds)),
      ));
    createdRooms.set(config.cryptoStoreGeneration, roomId);
    const recorded = yield* configService
      .recordRoomIfMatches({
        cryptoStoreGeneration: config.cryptoStoreGeneration,
        roomId,
      })
      .pipe(
        Effect.mapError(() =>
          clientError("listen", "The Matrix room could not be persisted.", "transient"),
        ),
      );
    if (!recorded) {
      return yield* clientError(
        "listen",
        "The Matrix bridge configuration changed while the room was being created.",
        "permanent",
      );
    }
    createdRooms.delete(config.cryptoStoreGeneration);
    // Presets and power levels are homeserver-implemented: confirm the room
    // really came back invite-only, encrypted, and bot-administered before
    // anything is sent to it.
    yield* verifyEncryptedRoom(client, roomId, botUserId);
    return roomId;
  });

  const runConnection = Effect.fn("MatrixBotSdkClient.runConnection")(function* (
    config: MatrixBridgeConfigV1,
    onInboundText: MatrixBridgeInboundHandler,
  ) {
    const module = yield* Effect.tryPromise({
      try: loadModule,
      catch: () =>
        clientError(
          "listen",
          "Matrix end-to-end encryption is unavailable on this installation. Reinstall T3 Code on a supported platform to enable the bridge.",
          "permanent",
        ),
    });
    yield* installQuietSdkLogger(module);

    const storeDir = yield* prepareStoreDirectory(config.cryptoStoreGeneration);
    const { client, storage, fence } = yield* Effect.acquireRelease(
      Effect.try({
        try: () => {
          const storageProvider = new module.SimpleFsStorageProvider(
            path.join(storeDir, MATRIX_SYNC_STORE_FILE_NAME),
          );
          const cryptoStore = new module.RustSdkCryptoStorageProvider(
            path.join(storeDir, MATRIX_CRYPTO_STORE_DIRECTORY_NAME),
            MATRIX_CRYPTO_SQLITE_STORE_TYPE,
          );
          // The SDK writes through the fence so the catch-up batch is
          // recognisable; encryption still receives that batch in full.
          const fencedStorage = createFencedSyncStorage(storageProvider);
          return {
            client: new module.MatrixClient(
              config.homeserverUrl,
              config.accessToken,
              fencedStorage.storage,
              cryptoStore,
            ),
            // Everything writes through the fence, so a cleared cursor re-arms
            // it wherever the clearing happened.
            storage: fencedStorage.storage,
            fence: fencedStorage,
          };
        },
        catch: () =>
          clientError(
            "listen",
            "The Matrix client could not open its encryption store.",
            "permanent",
          ),
      }),
      ({ client }) => Effect.sync(() => client.stop()),
    );

    const botUserId = yield* request(
      "listen",
      "The Matrix bot identity could not be confirmed.",
      () => client.getUserId(),
    );
    const roomId = yield* ensureRoom(client, config, botUserId);
    const syncFilter = buildSyncFilter(roomId);
    // Registering a filter clears the stored cursor, so it happens before the
    // sync loop starts rather than inside it.
    yield* ensureSyncFilter(client, storage, botUserId, syncFilter);

    const inbound = yield* Queue.dropping<MatrixBridgeInboundText>(MATRIX_INBOUND_QUEUE_CAPACITY);
    yield* Effect.acquireRelease(
      Effect.sync(() => {
        const handler: MatrixSdkEventHandler = (eventRoomId, event) => {
          // Pre-fence history is dropped here, after the SDK has taken the
          // batch's room keys and device updates into the crypto store.
          if (!fence.acceptsTimelineEvents()) return;
          const message = toInboundText(event, eventRoomId, roomId, botUserId);
          if (message !== null) Queue.offerUnsafe(inbound, message);
        };
        client.on(MATRIX_DECRYPTED_EVENT, handler);
        return handler;
      }),
      (handler) =>
        Effect.sync(() => {
          client.off(MATRIX_DECRYPTED_EVENT, handler);
        }),
    );
    // Room order is preserved by draining one message at a time.
    yield* Effect.forkScoped(Stream.runForEach(Stream.fromQueue(inbound), onInboundText));

    // Starting the sync loop also prepares encryption. A homeserver rejects the
    // device keys when the bot token was paired with a different crypto store,
    // which a new bot login repairs.
    yield* request(
      "listen",
      "The Matrix sync and encryption session could not be started. The bot access token may belong to a device that was paired with a different encryption store.",
      () => client.start(syncFilter),
    );
    if (client.crypto?.isReady !== true) {
      return yield* clientError(
        "listen",
        "Matrix end-to-end encryption did not initialize, so the bridge stays disabled.",
        "permanent",
      );
    }

    // `start` only launches the sync loop. Until its first response fixes the
    // catch-up boundary, a message sent now would arrive inside the fenced
    // batch and be dropped, so readiness waits for that boundary. The wait is
    // unbounded on purpose: the SDK retries the sync itself, and `stop()`
    // cannot cancel a request in flight, so timing out here would leave a
    // second client writing to the same sync and crypto stores.
    yield* Effect.logDebug("Matrix bridge waiting for its first sync response");
    yield* fence.awaitSyncBoundary;

    const connection: MatrixConnection = {
      cryptoStoreGeneration: config.cryptoStoreGeneration,
      roomId,
      client,
      encryptedPayloads: new Map(),
    };
    yield* Effect.acquireRelease(Ref.set(connectionRef, Option.some(connection)), () =>
      Ref.update(connectionRef, (current) =>
        Option.isSome(current) && current.value === connection ? Option.none() : current,
      ),
    );
    yield* configService.reportTransportStateIfMatches({
      cryptoStoreGeneration: config.cryptoStoreGeneration,
      transport: { state: "ready" },
    });
    yield* Effect.logInfo("Matrix bridge transport connected");
    return yield* Effect.never;
  });

  const listen: MatrixBridgeClient["Service"]["listen"] = (onInboundText) =>
    Effect.gen(function* () {
      let attempt = 0;
      while (true) {
        const config = yield* awaitConfigured;
        const outcome = yield* Effect.result(
          Effect.scoped(
            Effect.raceFirst(
              runConnection(config, onInboundText),
              awaitGenerationReplaced(config.cryptoStoreGeneration),
            ),
          ),
        );
        if (outcome._tag === "Success") {
          // The configuration was replaced; connect the new one immediately.
          attempt = 0;
          continue;
        }

        yield* configService.reportTransportStateIfMatches({
          cryptoStoreGeneration: config.cryptoStoreGeneration,
          transport: { state: "unavailable", reason: outcome.failure.reason },
        });
        yield* Effect.logWarning("Matrix bridge transport unavailable", {
          reason: outcome.failure.reason,
          retryability: outcome.failure.retryability,
        });
        if (outcome.failure.retryability === "permanent") {
          // A deterministic failure cannot be retried into success, so wait for
          // a human to reconfigure instead of looping on the same error.
          yield* awaitReconnectSignal(config.cryptoStoreGeneration);
          attempt = 0;
          continue;
        }
        yield* Effect.sleep(reconnectDelayMs(attempt));
        attempt += 1;
      }
    });

  const encryptOnce = Effect.fn("MatrixBotSdkClient.encryptOnce")(function* (
    connection: MatrixConnection,
    message: MatrixBridgeOutboundText,
  ) {
    const cached = connection.encryptedPayloads.get(message.transactionId);
    if (cached !== undefined) return cached;

    const crypto = connection.client.crypto;
    if (crypto?.isReady !== true) {
      return yield* clientError("send", "Matrix encryption is not ready.", "transient");
    }
    const encrypted = yield* request("send", "The Matrix message could not be encrypted.", () =>
      crypto.encryptRoomEvent(message.roomId, "m.room.message", message.content),
    );
    if (connection.encryptedPayloads.size >= MATRIX_ENCRYPTED_PAYLOAD_CAPACITY) {
      const oldest = connection.encryptedPayloads.keys().next();
      if (!oldest.done) connection.encryptedPayloads.delete(oldest.value);
    }
    connection.encryptedPayloads.set(message.transactionId, encrypted);
    return encrypted;
  });

  const sendText: MatrixBridgeClient["Service"]["sendText"] = Effect.fn(
    "MatrixBotSdkClient.sendText",
  )(function* (message) {
    const current = yield* Ref.get(connectionRef);
    if (Option.isNone(current)) {
      return yield* clientError("send", "The Matrix transport is not connected.", "transient");
    }
    const connection = current.value;
    if (connection.roomId !== message.roomId) {
      return yield* clientError(
        "send",
        "The Matrix message targets a room this transport does not serve.",
        "permanent",
      );
    }

    const encrypted = yield* encryptOnce(connection, message);
    // The transaction ID makes the send idempotent across retries: the
    // homeserver returns the original event for a repeated PUT.
    yield* request("send", "The Matrix message could not be delivered.", () =>
      connection.client.doRequest(
        "PUT",
        encryptedSendEndpoint(message.roomId, message.transactionId),
        null,
        encrypted,
      ),
    );
    connection.encryptedPayloads.delete(message.transactionId);
  });

  return MatrixBridgeClient.of({ listen, sendText });
});

export const layer = Layer.effect(MatrixBridgeClient, make());
