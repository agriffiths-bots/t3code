import * as Crypto from "effect/Crypto";
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
/** 128 bits of the device digest: collision-free in practice, short on disk. */
const MATRIX_STORE_KEY_BYTES = 16;
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
  getJoinedRooms(): Promise<ReadonlyArray<string>>;
  getRoomState(roomId: string): Promise<ReadonlyArray<unknown>>;
  createRoom(options: MatrixCreateRoomOptions): Promise<string>;
  inviteUser(userId: string, roomId: string): Promise<unknown>;
  doRequest(
    method: string,
    endpoint: string,
    qs?: unknown,
    body?: unknown,
    ...rest: ReadonlyArray<unknown>
  ): Promise<unknown>;
  start(filter?: unknown): Promise<unknown>;
  stop(): void;
  on(event: string, handler: MatrixSdkEventHandler): unknown;
  off(event: string, handler: MatrixSdkEventHandler): unknown;
}

export interface MatrixSdkModule {
  readonly MatrixClient: new (
    homeserverUrl: string,
    accessToken: string,
    storage?: MatrixSdkStorageProvider,
    cryptoStore?: unknown,
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

export interface MatrixBotIdentity {
  readonly userId: string;
  readonly deviceId: string | null;
}

/**
 * Confirms who the bot is and, when the homeserver reports it, which device the
 * access token belongs to. The device is what an encryption store is tied to.
 */
export const whoami = Effect.fn("MatrixBotSdkClient.whoami")(function* (client: MatrixSdkClient) {
  const response = yield* request("listen", "The Matrix bot identity could not be confirmed.", () =>
    client.doRequest("GET", "/_matrix/client/v3/account/whoami"),
  );
  const userId = readString(response, "user_id");
  if (userId === null) {
    return yield* clientError(
      "listen",
      "The homeserver did not identify the Matrix bot account.",
      "permanent",
    );
  }
  return { userId, deviceId: readString(response, "device_id") } satisfies MatrixBotIdentity;
});

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

/** Matches matrix-bot-sdk's own stored-filter comparison byte for byte. */
const sameSyncFilter = (left: unknown, right: unknown) =>
  JSON.stringify(left) === JSON.stringify(right);

/** Identity of the filter a sync loop is running, compared the same way. */
const syncFilterIdentity = (filter: unknown) => JSON.stringify(filter);

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

/** Older stable room versions allow integer strings in power-level content. */
function readPowerLevel(value: unknown): number | null | "invalid" {
  if (value === undefined) return null;
  if (typeof value === "number") return Number.isInteger(value) ? value : "invalid";
  if (typeof value === "string" && /^-?\d+$/u.test(value)) return Number.parseInt(value, 10);
  return "invalid";
}

/** State whose change would widen the room or undo its protections. */
const PROTECTED_ROOM_STATE_TYPES = [
  "m.room.join_rules",
  "m.room.power_levels",
  "m.room.encryption",
] as const;

/**
 * Only the bot may widen the room. Members can send messages and nothing else,
 * so no member's device can pull an unlisted account in, publish the room, or
 * take encryption off it, and read later bridge output.
 *
 * Anything unparseable is a refusal rather than a default, because the Matrix
 * defaults (invite 0, state_default 50) are the permissive direction.
 */
function roomIsBotAdministered(
  powerLevels: unknown,
  botUserId: string,
  botHasCreatorPower: boolean,
): boolean {
  const record = readRecord(powerLevels);
  if (record === null) return false;

  const usersDefault = readPowerLevel(record.users_default) ?? 0;
  const stateDefault = readPowerLevel(record.state_default) ?? 50;
  const inviteLevel = readPowerLevel(record.invite) ?? 0;
  if (usersDefault === "invalid" || stateDefault === "invalid" || inviteLevel === "invalid") {
    return false;
  }

  const users = readRecord(record.users) ?? {};
  // Room version 12 gives creators unbounded power and forbids listing them
  // here, so the bot's own entry is absent by design in a correct room.
  const listedBotLevel = readPowerLevel(users[botUserId]) ?? usersDefault;
  if (listedBotLevel === "invalid") return false;
  const botLevel = botHasCreatorPower ? Number.POSITIVE_INFINITY : listedBotLevel;

  let highestMemberLevel = usersDefault;
  for (const [userId, value] of Object.entries(users)) {
    if (userId === botUserId) continue;
    const level = readPowerLevel(value) ?? usersDefault;
    if (level === "invalid") return false;
    highestMemberLevel = Math.max(highestMemberLevel, level);
  }

  const events = readRecord(record.events) ?? {};
  const requiredLevels: Array<number> = [inviteLevel];
  for (const type of PROTECTED_ROOM_STATE_TYPES) {
    const level = readPowerLevel(events[type]) ?? stateDefault;
    if (level === "invalid") return false;
    requiredLevels.push(level);
  }

  return requiredLevels.every((level) => level > highestMemberLevel && botLevel >= level);
}

interface MatrixRoomStateEvent {
  readonly type: string;
  readonly stateKey: string;
  readonly sender: string | null;
  readonly content: Record<string, unknown> | null;
}

function readRoomState(events: ReadonlyArray<unknown>): ReadonlyArray<MatrixRoomStateEvent> {
  const parsed: Array<MatrixRoomStateEvent> = [];
  for (const event of events) {
    const record = readRecord(event);
    const type = readString(record, "type");
    const stateKey = readString(record, "state_key");
    if (record === null || type === null || stateKey === null) continue;
    parsed.push({
      type,
      stateKey,
      sender: readString(record, "sender"),
      content: readRecord(record.content),
    });
  }
  return parsed;
}

function findStateContent(
  state: ReadonlyArray<MatrixRoomStateEvent>,
  type: string,
): Record<string, unknown> | null {
  return state.find((event) => event.type === type && event.stateKey === "")?.content ?? null;
}

/** Version 12 and later grant the room's creators unbounded power. */
const MATRIX_CREATOR_POWER_ROOM_VERSION = 12;
/**
 * Room versions whose authorization rules this bridge implements. Identifiers
 * are opaque strings with no ordering, so they are matched exactly: an
 * unlisted one (a future version, or a homeserver's own dialect) is refused
 * rather than assumed to behave like its neighbours. Versions below 10 are
 * excluded because they permit floating-point and loosely formatted power
 * levels that this verification deliberately does not interpret.
 */
const MATRIX_SUPPORTED_ROOM_VERSIONS = new Map([
  ["10", 10],
  ["11", 11],
  ["12", 12],
]);

interface MatrixRoomCreation {
  readonly version: number;
  readonly creatorsAreBotOnly: boolean;
  readonly botHasCreatorPower: boolean;
}

/**
 * Reads who really controls the room.
 *
 * A room someone else created can never be trusted from version 12 onwards,
 * because its creators keep unbounded power whatever the power levels say, and
 * a forged `content.creator` must not be able to claim otherwise: from version
 * 11 the create event's sender is the only authority. An unrecognised room
 * version is refused rather than guessed at.
 */
function readRoomCreation(
  state: ReadonlyArray<MatrixRoomStateEvent>,
  botUserId: string,
): MatrixRoomCreation | null {
  const createEvent = state.find(
    (event) => event.type === "m.room.create" && event.stateKey === "",
  );
  if (createEvent === undefined) return null;

  const rawVersion = readString(createEvent.content, "room_version") ?? "1";
  const version = MATRIX_SUPPORTED_ROOM_VERSIONS.get(rawVersion);
  if (version === undefined) return null;

  // `content.creator` was removed in version 11; from there the create event's
  // sender is the only authority, so a `creator` field cannot claim otherwise.
  const creator = createEvent.sender;
  const additional = createEvent.content?.additional_creators;
  const additionalAreBotOnly =
    additional === undefined ||
    (Array.isArray(additional) && additional.every((entry) => entry === botUserId));

  const creatorsAreBotOnly = creator === botUserId && additionalAreBotOnly;
  return {
    version,
    creatorsAreBotOnly,
    botHasCreatorPower: creatorsAreBotOnly && version >= MATRIX_CREATOR_POWER_ROOM_VERSION,
  };
}

/** Joined members and outstanding invitations: both can read what comes next. */
function activeMembers(state: ReadonlyArray<MatrixRoomStateEvent>): ReadonlyArray<string> {
  return state
    .filter((event) => {
      if (event.type !== "m.room.member") return false;
      const membership = readString(event.content, "membership");
      return membership === "join" || membership === "invite";
    })
    .map((event) => event.stateKey);
}

function membershipOf(state: ReadonlyArray<MatrixRoomStateEvent>, userId: string): string | null {
  const event = state.find((entry) => entry.type === "m.room.member" && entry.stateKey === userId);
  return readString(event?.content ?? null, "membership");
}

/**
 * Fails closed unless the bot is joined to an invite-only, Megolm-encrypted
 * room that only the bot created, only the bot can widen, and nobody outside
 * the allowed list has joined. Shared with the live homeserver smoke so both
 * check the same state.
 */
export const verifyEncryptedRoom = Effect.fn("MatrixBotSdkClient.verifyEncryptedRoom")(function* (
  client: MatrixSdkClient,
  roomId: string,
  botUserId: string,
  allowedUserIds: ReadonlyArray<string>,
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

  const state = readRoomState(
    yield* request("listen", "The bridged Matrix room state could not be read.", () =>
      client.getRoomState(roomId),
    ),
  );

  const creation = readRoomCreation(state, botUserId);
  if (creation === null || !creation.creatorsAreBotOnly) {
    return yield* clientError(
      "listen",
      "The configured Matrix room was created by another account, or uses a room version this bridge does not verify (10, 11, and 12 are supported), so control of it cannot be established.",
      "permanent",
    );
  }

  if (readString(findStateContent(state, "m.room.join_rules"), "join_rule") !== "invite") {
    return yield* clientError(
      "listen",
      "The configured Matrix room is not invite-only.",
      "permanent",
    );
  }

  if (readString(findStateContent(state, "m.room.encryption"), "algorithm") !== MEGOLM_ALGORITHM) {
    return yield* clientError(
      "listen",
      "The configured Matrix room is not end-to-end encrypted. Encryption cannot be added to a room that already has plaintext history.",
      "permanent",
    );
  }

  if (
    !roomIsBotAdministered(
      findStateContent(state, "m.room.power_levels"),
      botUserId,
      creation.botHasCreatorPower,
    )
  ) {
    return yield* clientError(
      "listen",
      "The configured Matrix room lets its members invite other accounts or change its access rules, so bridge output could reach someone outside the allowed list.",
      "permanent",
    );
  }

  const allowed = new Set([botUserId, ...allowedUserIds]);
  if (activeMembers(state).some((userId) => !allowed.has(userId))) {
    // Removing someone from the room is the operator's call, so the bridge
    // stops rather than keeps talking to a room somebody outside the list can
    // read. A pending invitation counts: it can still be accepted later.
    return yield* clientError(
      "listen",
      "The bridged Matrix room has a member or a pending invitation outside the allowed list, so the bridge stays disabled until the room membership matches it.",
      "permanent",
    );
  }

  return state;
});

/**
 * Re-invites allowed users who never accepted or have left.
 *
 * The room is created with its invitations, but leaving would otherwise be a
 * one-way door: the room is reused forever and nothing invites again. A failure
 * here is logged rather than fatal, because an otherwise healthy encrypted room
 * should not be taken offline by one unreachable invitee.
 */
export const reconcileAllowedMembership = Effect.fn(
  "MatrixBotSdkClient.reconcileAllowedMembership",
)(function* (
  client: MatrixSdkClient,
  roomId: string,
  allowedUserIds: ReadonlyArray<string>,
  state: ReadonlyArray<MatrixRoomStateEvent>,
) {
  for (const userId of allowedUserIds) {
    const membership = membershipOf(state, userId);
    if (membership === "join" || membership === "invite") continue;

    yield* request("listen", "An allowed Matrix user could not be invited.", () =>
      client.inviteUser(userId, roomId),
    ).pipe(
      Effect.matchEffect({
        onSuccess: () => Effect.logInfo("Matrix bridge invited an allowed user to the room"),
        onFailure: () =>
          Effect.logWarning("Matrix bridge could not invite an allowed user to the room"),
      }),
    );
  }
});

/** Registers a filter with the homeserver and returns its identifier. */
export const registerSyncFilter = Effect.fn("MatrixBotSdkClient.registerSyncFilter")(function* (
  client: MatrixSdkClient,
  botUserId: string,
  filter: unknown,
) {
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
  return filterId;
});

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

  const filterId = yield* registerSyncFilter(client, botUserId, filter);
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
  /** Permanently drops writes and events from a client being discarded. */
  readonly retire: () => void;
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
  let retired = false;
  let boundary = Deferred.makeUnsafe<void>();
  if (!fenced) Deferred.doneUnsafe(boundary, Effect.void);
  const storage: MatrixSdkStorageProvider = {
    getSyncToken: () => inner.getSyncToken(),
    setSyncToken: (token) => {
      // A retired client's in-flight sync still resolves and still tries to
      // write; its cursor belongs to a connection that no longer exists.
      if (retired) return;
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
    setFilter: (filter) => {
      if (retired) return;
      inner.setFilter(filter);
    },
  };

  return {
    storage,
    acceptsTimelineEvents: () => !retired && (!fenced || syncedBatches >= 2),
    syncedBatches: () => syncedBatches,
    awaitSyncBoundary: Effect.suspend(() => Deferred.await(boundary)),
    retire: () => {
      retired = true;
    },
  };
}

const MATRIX_SYNC_ENDPOINT = "/_matrix/client/v3/sync";

interface TrackedMatrixClient {
  readonly client: MatrixSdkClient;
  /**
   * Resolves once no sync request is outstanding.
   *
   * `MatrixClient.stop` only sets a flag, so the request it cannot cancel still
   * resolves, writes its cursor and drives crypto. Nothing may reopen the
   * stores until that has finished, and a request that never lands must not
   * hold the handover open forever.
   */
  readonly awaitSyncIdle: Effect.Effect<void>;
}

export function trackSyncRequests(
  MatrixClientClass: MatrixSdkModule["MatrixClient"],
  construct: (clientClass: MatrixSdkModule["MatrixClient"]) => MatrixSdkClient,
): TrackedMatrixClient {
  let pending = 0;
  let idle: Deferred.Deferred<void> | null = null;

  const track = <A>(work: Promise<A>): Promise<A> => {
    pending += 1;
    return work.finally(() => {
      pending -= 1;
      if (pending > 0 || idle === null) return;
      Deferred.doneUnsafe(idle, Effect.void);
      idle = null;
    });
  };

  class SyncTrackingMatrixClient extends MatrixClientClass {
    override doRequest(
      method: string,
      endpoint: string,
      qs?: unknown,
      body?: unknown,
      ...rest: ReadonlyArray<unknown>
    ): Promise<unknown> {
      const call = super.doRequest(method, endpoint, qs, body, ...rest);
      return endpoint === MATRIX_SYNC_ENDPOINT ? track(call) : call;
    }

    /**
     * The response is only half an iteration: the SDK then persists the cursor,
     * feeds the crypto store and emits events. A client is idle only once that
     * has finished too, otherwise a replacement could reopen the stores while
     * the retired loop is still writing to them.
     */
    processSync(raw?: unknown, emitFn?: unknown): Promise<unknown> {
      const base = (
        MatrixClientClass.prototype as {
          processSync?: (this: unknown, raw?: unknown, emitFn?: unknown) => Promise<unknown>;
        }
      ).processSync;
      if (base === undefined) return Promise.resolve(undefined);
      return track(base.call(this, raw, emitFn));
    }
  }

  return {
    client: construct(SyncTrackingMatrixClient),
    // Deliberately unbounded: reopening a store a retired client might still
    // write to is worse than a bridge that stays unavailable until it is safe.
    awaitSyncIdle: Effect.suspend(() => {
      if (pending === 0) return Effect.void;
      const pendingIdle = Deferred.makeUnsafe<void>();
      idle = pendingIdle;
      return Deferred.await(pendingIdle);
    }),
  };
}

interface LiveTransport {
  readonly storeKey: string;
  /** Homeserver and token this client authenticates with. */
  readonly credentialKey: string;
  readonly client: MatrixSdkClient;
  readonly storage: MatrixSdkStorageProvider;
  readonly fence: FencedSyncStorage;
  readonly awaitSyncIdle: Effect.Effect<void>;
  /**
   * Buffers decrypted room text for whichever connection is draining it. The
   * handler that fills it is attached once, for the client's whole life, so a
   * reused sync loop cannot advance its cursor through a gap between
   * connections and drop the messages in that batch.
   */
  readonly inbound: Queue.Queue<MatrixBridgeInboundText>;
  /** The room the attached handler accepts, set by the live connection. */
  roomId: string | null;
  /** The filter its sync loop is running, or null before it started. */
  syncFilterKey: string | null;
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
  const crypto = yield* Crypto.Crypto;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const connectionRef = yield* Ref.make(Option.none<MatrixConnection>());
  /** Rooms created but not yet persisted, keyed by connection generation. */
  const createdRooms = new Map<string, string>();
  /**
   * At most one Matrix client exists at a time, and only one ever opens a given
   * device's stores. Reconfiguration reuses it, because `MatrixClient.stop`
   * cannot cancel a sync already in flight: a second client on the same sync
   * file and crypto database would race the first one's last response.
   */
  let live: LiveTransport | null = null;
  /**
   * One barrier per retired client's stores. Reconfiguring back to an earlier
   * device must still wait for that device's own retired client, so the
   * barriers are kept until each is used.
   */
  const retiredByStoreKey = new Map<string, Effect.Effect<void>>();

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

  /**
   * Retires the running client: its fence stops accepting writes and events,
   * and its sync loop is told to stop.
   *
   * The request `stop()` cannot cancel is not waited for here, because at
   * shutdown there is nothing left to protect. The wait belongs to whatever
   * reopens the stores, and is kept for exactly that.
   */
  const retireLiveTransport = Effect.fn("MatrixBotSdkClient.retireLiveTransport")(function* () {
    const retiring = live;
    if (retiring === null) return;
    // One indivisible step: a reconfiguration interrupting halfway through
    // could otherwise leave a client syncing with nothing recorded to wait for,
    // and the next connection would open a second one on the same stores.
    yield* Effect.sync(() => {
      live = null;
      retiring.fence.retire();
      retiring.client.stop();
      retiredByStoreKey.set(retiring.storeKey, retiring.awaitSyncIdle);
    });
    yield* Effect.logInfo("Matrix bridge retired its Matrix client");
  });

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

  /**
   * Names the store after the Matrix device rather than the configuration.
   *
   * A device may upload one-time keys from exactly one crypto store, so the
   * store has to outlive an allowlist edit, a disconnect and reconnect, and a
   * token refresh, all of which keep the same device. The homeserver's own
   * answer for the session is therefore the key; the access token is only a
   * fallback for a server that reports no device. The digest is one-way and
   * sits inside the 0700 secrets directory.
   */
  const deviceStoreKey = Effect.fn("MatrixBotSdkClient.deviceStoreKey")(function* (
    config: MatrixBridgeConfigV1,
    identity: MatrixBotIdentity,
  ) {
    const identifier =
      identity.deviceId === null
        ? `${config.homeserverUrl}\ntoken\n${config.accessToken}`
        : `${config.homeserverUrl}\ndevice\n${identity.userId}\n${identity.deviceId}`;
    const digest = yield* crypto
      .digest("SHA-256", new TextEncoder().encode(identifier))
      .pipe(
        Effect.mapError(() =>
          clientError("listen", "The Matrix encryption store could not be located.", "permanent"),
        ),
      );
    return Array.from(digest.subarray(0, MATRIX_STORE_KEY_BYTES))
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  });

  const prepareStoreDirectory = Effect.fn("MatrixBotSdkClient.prepareStoreDirectory")(function* (
    storeKey: string,
    legacyStoreKey: string,
  ) {
    const bridgeDir = path.join(serverConfig.secretsDir, MATRIX_STORE_DIRECTORY_NAME);
    const storeDir = path.join(bridgeDir, storeKey);

    // Stores were once named after the configuration generation. Moving that
    // directory keeps the device's one store rather than starting an empty
    // second one the homeserver would reject.
    const legacyDir = path.join(bridgeDir, legacyStoreKey);
    const exists = (directory: string) =>
      fs.exists(directory).pipe(Effect.orElseSucceed(() => false));
    if (legacyDir !== storeDir && !(yield* exists(storeDir)) && (yield* exists(legacyDir))) {
      yield* fs
        .rename(legacyDir, storeDir)
        .pipe(
          Effect.mapError(() =>
            clientError("listen", "The Matrix encryption store could not be moved.", "transient"),
          ),
        );
      yield* Effect.logInfo("Matrix bridge moved its encryption store to the device-named path");
    }
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
      const state = yield* verifyEncryptedRoom(
        client,
        config.roomId,
        botUserId,
        config.allowedUserIds,
      );
      yield* reconcileAllowedMembership(client, config.roomId, config.allowedUserIds, state);
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
    yield* verifyEncryptedRoom(client, roomId, botUserId, config.allowedUserIds);
    return roomId;
  });

  /** Opens the one client for a device, waiting out any retired predecessor. */
  const openTransport = Effect.fn("MatrixBotSdkClient.openTransport")(function* (input: {
    readonly module: MatrixSdkModule;
    readonly config: MatrixBridgeConfigV1;
    readonly storeKey: string;
    readonly storeDir: string;
    readonly credentialKey: string;
    readonly botUserId: string;
  }) {
    // Only the stores about to be opened need protecting; another device's
    // retired client can finish whenever it likes, and its barrier is kept in
    // case the bridge is pointed back at it.
    const barrier = retiredByStoreKey.get(input.storeKey);
    if (barrier !== undefined) {
      yield* barrier;
      retiredByStoreKey.delete(input.storeKey);
    }

    const inbound = yield* Queue.dropping<MatrixBridgeInboundText>(MATRIX_INBOUND_QUEUE_CAPACITY);
    return yield* Effect.try({
      try: () => {
        const storageProvider = new input.module.SimpleFsStorageProvider(
          path.join(input.storeDir, MATRIX_SYNC_STORE_FILE_NAME),
        );
        const cryptoStore = new input.module.RustSdkCryptoStorageProvider(
          path.join(input.storeDir, MATRIX_CRYPTO_STORE_DIRECTORY_NAME),
          MATRIX_CRYPTO_SQLITE_STORE_TYPE,
        );
        // The SDK writes through the fence so the catch-up batch is
        // recognisable; encryption still receives that batch in full.
        const fencedStorage = createFencedSyncStorage(storageProvider);
        const tracked = trackSyncRequests(
          input.module.MatrixClient,
          (MatrixClientClass) =>
            new MatrixClientClass(
              input.config.homeserverUrl,
              input.config.accessToken,
              fencedStorage.storage,
              cryptoStore,
            ),
        );
        const transport: LiveTransport = {
          storeKey: input.storeKey,
          credentialKey: input.credentialKey,
          client: tracked.client,
          awaitSyncIdle: tracked.awaitSyncIdle,
          // Everything writes through the fence, so a cleared cursor re-arms it
          // wherever the clearing happened.
          storage: fencedStorage.storage,
          fence: fencedStorage,
          inbound,
          roomId: null,
          syncFilterKey: null,
        };
        // Attached once and never removed: the buffer outlives an individual
        // connection so no batch is decrypted into nowhere.
        tracked.client.on(MATRIX_DECRYPTED_EVENT, (eventRoomId, event) => {
          // Pre-fence history is dropped here, after the SDK has taken the
          // batch's room keys and device updates into the crypto store.
          if (transport.roomId === null || !fencedStorage.acceptsTimelineEvents()) return;
          const message = toInboundText(event, eventRoomId, transport.roomId, input.botUserId);
          if (message !== null) Queue.offerUnsafe(inbound, message);
        });
        return transport;
      },
      catch: () =>
        clientError(
          "listen",
          "The Matrix client could not open its encryption store.",
          "permanent",
        ),
    });
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

    // The store is named after the device, and only the homeserver can say
    // which device this token belongs to, so identity is resolved first with a
    // client that owns no persistent state.
    // Credentials are settled before anything is asked of the homeserver.
    // A configuration naming different ones must not leave the running client
    // syncing under the credentials it replaced, and the replacement must not
    // be handed to that client either: until the homeserver says which device
    // the new token belongs to, a loop using it could pull another device's
    // to-device messages into this one's stores.
    const credentialKey = `${config.homeserverUrl}\n${config.accessToken}`;
    if (live !== null && live.credentialKey !== credentialKey) {
      yield* retireLiveTransport();
    }

    const identity = yield* Effect.acquireRelease(
      Effect.try({
        try: () => new module.MatrixClient(config.homeserverUrl, config.accessToken),
        catch: () => clientError("listen", "The Matrix client could not be created.", "permanent"),
      }),
      (probe) => Effect.sync(() => probe.stop()),
    ).pipe(Effect.flatMap(whoami));
    const botUserId = identity.userId;
    const storeKey = yield* deviceStoreKey(config, identity);
    const storeDir = yield* prepareStoreDirectory(storeKey, config.cryptoStoreGeneration);

    // The device the credentials name may itself have changed, and a different
    // device is a different set of stores.
    if (live !== null && live.storeKey !== storeKey) {
      yield* retireLiveTransport();
    }
    if (live === null) {
      live = yield* openTransport({ module, config, storeKey, storeDir, credentialKey, botUserId });
    }
    const roomId = yield* ensureRoom(live.client, config, botUserId);
    const syncFilter = buildSyncFilter(roomId);
    const syncFilterKey = syncFilterIdentity(syncFilter);
    if (live.syncFilterKey !== null && live.syncFilterKey !== syncFilterKey) {
      // A new room needs a new filter, and the running loop cannot take one:
      // the request already in flight still carries the old filter and would
      // advance the cursor past events for the new room. Retiring the client
      // and opening a fresh loop on the same stores resumes from the stored
      // cursor under the new filter, so nothing is skipped.
      yield* retireLiveTransport();
      live = yield* openTransport({ module, config, storeKey, storeDir, credentialKey, botUserId });
    }
    const { client, storage, fence } = live;

    // The handler keeps accepting this room between connections: the sync loop
    // is still running, and a batch that lands in that gap belongs to the next
    // connection rather than to nobody. A different room arrives with a
    // different filter, which restarts the loop anyway.
    live.roomId = roomId;
    const transport = live;
    // Room order is preserved by draining one message at a time. The queue
    // belongs to the transport, so anything buffered between connections is
    // delivered by the next one rather than lost, and the allowed list is read
    // at delivery: a member removed while a message was queued is not one the
    // bridge still listens to.
    yield* Effect.forkScoped(
      Stream.runForEach(Stream.fromQueue(transport.inbound), (event) =>
        Effect.gen(function* () {
          // The queue outlives a connection, so an event buffered for the room
          // this connection replaced is not one it may deliver.
          if (event.roomId !== roomId) return;
          const stored = yield* currentConfig;
          if (Option.isNone(stored) || !stored.value.allowedUserIds.includes(event.sender)) {
            return;
          }
          yield* onInboundText(event);
        }),
      ),
    );

    // Starting the sync loop also prepares encryption. A homeserver rejects the
    // device keys when the bot token was paired with a different crypto store,
    // which a new bot login repairs. A loop already running this filter is left
    // alone: restarting it would reopen stores the client still holds.
    if (live.syncFilterKey !== syncFilterKey) {
      const starting = live;
      // Interruption cannot cancel the SDK's start, and a half-started client
      // whose filter is still unrecorded would be reused and started a second
      // time, putting two sync loops on one store. The start therefore
      // completes before a reconfiguration is allowed to take effect.
      yield* Effect.uninterruptible(
        Effect.gen(function* () {
          // Registering a filter clears the stored cursor, so it happens before
          // the sync loop starts rather than inside it.
          yield* ensureSyncFilter(client, storage, botUserId, syncFilter);
          yield* request(
            "listen",
            "The Matrix sync and encryption session could not be started. The bot access token may belong to a device that was paired with a different encryption store.",
            () => client.start(syncFilter),
          );
          starting.syncFilterKey = syncFilterKey;
        }),
      );
    }
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
    const publishReady = configService.reportTransportStateIfMatches({
      cryptoStoreGeneration: config.cryptoStoreGeneration,
      transport: { state: "ready" },
    });
    yield* publishReady;
    // Resubmitting identical settings republishes `connecting` without
    // replacing this connection, so the live transport restores the truth
    // rather than leaving the status stuck mid-connect.
    yield* Effect.forkScoped(
      Stream.runForEach(configService.statusChanges, (status) =>
        status.state === "connecting" ? Effect.asVoid(publishReady) : Effect.void,
      ),
    );
    yield* Effect.logInfo("Matrix bridge transport connected");
    return yield* Effect.never;
  });

  const listen: MatrixBridgeClient["Service"]["listen"] = (onInboundText) =>
    Effect.gen(function* () {
      // The client outlives an individual connection so reconfiguration can
      // reuse it; shutting the listener down is what finally releases it.
      yield* Effect.addFinalizer(() => retireLiveTransport());

      let attempt = 0;
      while (true) {
        // Disconnect means no Matrix activity at all, not a client left
        // syncing in the background.
        if (Option.isNone(yield* currentConfig)) yield* retireLiveTransport();
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

    // Reconfiguration and disconnect happen while a send is in flight, and the
    // connection captured above may already be retired: check the live
    // configuration immediately before the request so output cannot reach a
    // room the operator has just replaced or left.
    const stored = Option.getOrNull(yield* configService.currentConfig);
    if (
      stored === null ||
      stored.cryptoStoreGeneration !== connection.cryptoStoreGeneration ||
      stored.roomId !== connection.roomId
    ) {
      return yield* clientError(
        "send",
        "The Matrix connection was replaced before the message could be sent.",
        "permanent",
      );
    }

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
