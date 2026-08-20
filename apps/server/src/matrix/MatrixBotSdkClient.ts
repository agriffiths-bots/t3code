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
  type MatrixBridgeInboundOverflow,
  type MatrixBridgeInboundText,
  type MatrixBridgeRoomMembership,
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
/** Membership is plaintext state, so it arrives on the raw timeline event. */
const MATRIX_ROOM_EVENT = "room.event";
export const MATRIX_INBOUND_QUEUE_CAPACITY = 64;
/** Bounded so a room that keeps changing fails closed rather than looping. */
const MATRIX_MEMBERSHIP_READ_ATTEMPTS = 3;
/** A transient read must not turn an inbound command into a silent drop. */
const MATRIX_MEMBERSHIP_VERIFY_ATTEMPTS = 3;
/**
 * The timeline is not a complete membership feed: a change during a sync gap
 * can appear only in the state block, which the SDK does not emit. A quiet
 * room therefore reconciles on this interval so a join is noticed even with no
 * traffic to carry it.
 */
const MATRIX_MEMBERSHIP_RECONCILE_INTERVAL_MS = 60_000;
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
  readonly room_version: string;
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
/** Requested for new rooms; within the versions this bridge verifies. */
export const MATRIX_BRIDGE_ROOM_VERSION = "11";

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
  // Asked for explicitly: a homeserver whose default sits outside the versions
  // this bridge verifies would otherwise create a room it must then refuse.
  room_version: MATRIX_BRIDGE_ROOM_VERSION,
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
    // Members send messages and nothing else; a redaction from a member on the
    // bot's own homeserver would otherwise be authorised by domain alone.
    events: { "m.room.redaction": MATRIX_BRIDGE_ADMIN_POWER_LEVEL },
  },
  initial_state: [
    { type: "m.room.encryption", state_key: "", content: { algorithm: MEGOLM_ALGORITHM } },
    {
      type: "m.room.history_visibility",
      state_key: "",
      content: { history_visibility: MATRIX_BRIDGE_HISTORY_VISIBILITY },
    },
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

/**
 * The packed SDK that ships beside the server bundle, and the bare specifier a
 * source checkout resolves instead. Either way the import happens only when a
 * bridge is configured, so an installation without the native crypto binding
 * still starts a healthy server with the bridge reported unavailable.
 */
const MATRIX_BOT_SDK_PACKED_MODULE = "./matrix/matrixBotSdkModule.mjs";
const MATRIX_BOT_SDK_SPECIFIER: string = "matrix-bot-sdk";

export const loadMatrixBotSdk: MatrixSdkLoader = async () => {
  const loaded: unknown = await import(
    new URL(MATRIX_BOT_SDK_PACKED_MODULE, import.meta.url).href
  ).catch(() => import(MATRIX_BOT_SDK_SPECIFIER));
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
  // Rate limiting and request timeouts are the two 4xx answers that say "later",
  // not "never".
  if (status === 408 || status === 429) return "transient";
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
): Omit<MatrixBridgeInboundText, "roomAllowedOnly" | "ownershipEpoch"> | null {
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
    kind: "text",
    eventId,
    roomId,
    sender,
    body,
    isEdit: relation?.rel_type === "m.replace",
  };
}

/**
 * A membership transition for one account in the bridged room. An invitation
 * counts as active, exactly as the startup verifier counts it: the room starts
 * a member's view at their invitation, so an invited account will be able to
 * read whatever is sent from that moment once it joins.
 */
function toMembershipChange(
  event: unknown,
  eventRoomId: string,
  roomId: string,
): { readonly userId: string; readonly membership: "join" | "invite" | null } | null {
  if (eventRoomId !== roomId) return null;
  const record = readRecord(event);
  if (record === null || record.type !== "m.room.member") return null;
  const userId = readString(record, "state_key");
  if (userId === null) return null;
  const membership = readString(record.content, "membership");
  if (membership === null) return null;
  if (membership !== "join" && membership !== "invite") return { userId, membership: null };
  return { userId, membership };
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
  "m.room.history_visibility",
] as const;
/**
 * History a member may read. `invited` is the conventional private value for an
 * invite-only Matrix room: it starts a member's view at their invitation, which
 * for this bridge is the moment the room is created, so the owner sees the
 * whole conversation while nobody reads it earlier. `shared` would let a member
 * invited later read everything that came before them, and `world_readable`
 * serves history to accounts that never joined at all, so both are refused.
 */
export const MATRIX_BRIDGE_HISTORY_VISIBILITY = "invited";
const ACCEPTED_HISTORY_VISIBILITIES = new Set([MATRIX_BRIDGE_HISTORY_VISIBILITY, "joined"]);
/**
 * Message types members must not be able to send. A redaction is authorised by
 * the `redact` level *or* by the sender sharing the original sender's domain,
 * so on a shared homeserver the level alone would not stop a member erasing
 * the bridge's output: the send level has to be out of reach too.
 */
const PROTECTED_ROOM_MESSAGE_TYPES = ["m.room.redaction"] as const;

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
  const eventsDefault = readPowerLevel(record.events_default) ?? 0;
  const inviteLevel = readPowerLevel(record.invite) ?? 0;
  if (
    usersDefault === "invalid" ||
    stateDefault === "invalid" ||
    eventsDefault === "invalid" ||
    inviteLevel === "invalid"
  ) {
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

  const redactLevel = readPowerLevel(record.redact) ?? 50;
  if (redactLevel === "invalid") return false;

  const events = readRecord(record.events) ?? {};
  const requiredLevels: Array<number> = [inviteLevel, redactLevel];
  for (const type of PROTECTED_ROOM_STATE_TYPES) {
    const level = readPowerLevel(events[type]) ?? stateDefault;
    if (level === "invalid") return false;
    requiredLevels.push(level);
  }
  for (const type of PROTECTED_ROOM_MESSAGE_TYPES) {
    const level = readPowerLevel(events[type]) ?? eventsDefault;
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
  return [...activeMemberships(state).keys()];
}

/** The same, keeping which state each account is in. */
function activeMemberships(
  state: ReadonlyArray<MatrixRoomStateEvent>,
): ReadonlyMap<string, "join" | "invite"> {
  const members = new Map<string, "join" | "invite">();
  for (const event of state) {
    if (event.type !== "m.room.member") continue;
    const membership = readString(event.content, "membership");
    if (membership === "join" || membership === "invite") members.set(event.stateKey, membership);
  }
  return members;
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

  const historyVisibility = readString(
    findStateContent(state, "m.room.history_visibility"),
    "history_visibility",
  );
  if (historyVisibility === null || !ACCEPTED_HISTORY_VISIBILITIES.has(historyVisibility)) {
    return yield* clientError(
      "listen",
      "The configured Matrix room shares its history beyond the people in it, so bridge output could be read by an account that was never invited.",
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
/** One sync batch whose cursor is waiting on the work it delivered. */
export interface SyncBatchClaim {
  readonly token: string;
  outstanding: number;
  emitted: boolean;
}

export interface FencedSyncStorage {
  /** Handed to the SDK in place of the real provider. */
  readonly storage: MatrixSdkStorageProvider;
  /** False only while the pre-fence catch-up batch is being emitted. */
  readonly acceptsTimelineEvents: () => boolean;
  readonly syncedBatches: () => number;
  /**
   * Claims the batch being emitted for a message that is not handled yet, and
   * returns the claim to release later. The SDK persists a batch's cursor
   * before it emits that batch, so a crash between the two would resume after
   * a command nobody acted on: the cursor is held back until the work it
   * covers is done, and Matrix redelivers instead of skipping.
   */
  readonly beginInbound: () => SyncBatchClaim | null;
  /** Releases one claim; a batch with none left lets its cursor through. */
  readonly endInbound: (claim: SyncBatchClaim) => void;
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
  /**
   * Batches whose cursor is not stored yet, oldest first. A batch is written
   * once it has finished being emitted and nothing it delivered is still being
   * handled, so the stored cursor always names work that is done.
   */
  let batches: Array<SyncBatchClaim> = [];

  const flushBatches = () => {
    while (batches.length > 0) {
      const head = batches[0];
      if (head === undefined || head.outstanding > 0 || !head.emitted) return;
      batches.shift();
      // Only the durable write waits: the fence's own bookkeeping happened
      // when the SDK announced this cursor, so nothing else is delayed.
      inner.setSyncToken(head.token);
    }
  };

  const storage: MatrixSdkStorageProvider = {
    getSyncToken: () => inner.getSyncToken(),
    setSyncToken: (token) => {
      // A retired client's in-flight sync still resolves and still tries to
      // write; its cursor belongs to a connection that no longer exists.
      if (retired) return;
      // A cleared cursor is a filter change, which must take effect at once.
      if (token === null || token === "") {
        batches = [];
        inner.setSyncToken(token);
        // Registering a filter clears the cursor, and the sync that follows is
        // a fresh catch-up: re-arm rather than trusting the state at startup.
        fenced = true;
        syncedBatches = 0;
        boundary = Deferred.makeUnsafe<void>();
        return;
      }
      // A newer cursor means every earlier batch has finished being emitted,
      // so those are writable as soon as their work is done.
      for (const batch of batches) batch.emitted = true;
      flushBatches();
      // A batch whose timeline is fenced delivers nothing to hold the cursor
      // for, and its whole point is that a restart does not replay it.
      const deliversEvents = !fenced || syncedBatches + 1 >= 2;
      if (deliversEvents) {
        // Held, not dropped: the sync loop keeps running from the older cursor,
        // so room keys and device updates still arrive, and a batch whose work
        // this process never finished is simply redelivered.
        batches.push({ token, outstanding: 0, emitted: false });
      } else {
        inner.setSyncToken(token);
      }
      // Counted and announced now, so the catch-up fence and readiness behave
      // exactly as they did when the cursor was written here.
      syncedBatches += 1;
      Deferred.doneUnsafe(boundary, Effect.void);
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
    beginInbound: () => {
      if (retired) return null;
      const current = batches[batches.length - 1];
      if (current === undefined) return null;
      current.outstanding += 1;
      return current;
    },
    endInbound: (claim) => {
      if (retired) return;
      claim.outstanding = Math.max(0, claim.outstanding - 1);
      // The handler that queued this message ran during the batch's emission,
      // and this release runs after it, so that emission is over.
      claim.emitted = true;
      flushBatches();
    },
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
  // A sync response is followed, a few microtasks later, by the processing that
  // writes the cursor and feeds crypto. The response's hold is carried across
  // that gap so the barrier cannot open inside it.
  let heldForProcessing = false;
  let handover = 0;

  const acquire = () => {
    pending += 1;
  };

  const release = () => {
    pending -= 1;
    if (pending > 0 || idle === null) return;
    Deferred.doneUnsafe(idle, Effect.void);
    idle = null;
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
      if (endpoint !== MATRIX_SYNC_ENDPOINT) return call;
      acquire();
      return call.finally(() => {
        heldForProcessing = true;
        handover += 1;
        const current = handover;
        // A macrotask lands after the SDK's own awaits between the response and
        // `processSync`; this is plain promise plumbing around a callback SDK
        // rather than Effect-scheduled work.
        // @effect-diagnostics-next-line globalTimers:off - defers past the SDK's internal awaits.
        setTimeout(() => {
          if (handover !== current || !heldForProcessing) return;
          heldForProcessing = false;
          release();
        }, 0);
      });
    }

    /**
     * The response is only half an iteration: the SDK then persists the cursor,
     * feeds the crypto store and emits events. A client is idle only once that
     * has finished too, otherwise a replacement could reopen the stores while
     * the retired loop is still writing to them.
     */
    processSync(raw?: unknown, emitFn?: unknown): Promise<unknown> {
      if (heldForProcessing) {
        // Takes over the hold the sync response is still carrying.
        heldForProcessing = false;
        handover += 1;
      } else {
        acquire();
      }
      const base = (
        MatrixClientClass.prototype as {
          processSync?: (this: unknown, raw?: unknown, emitFn?: unknown) => Promise<unknown>;
        }
      ).processSync;
      if (base === undefined) {
        release();
        return Promise.resolve(undefined);
      }
      return base.call(this, raw, emitFn).finally(release);
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
  readonly inbound: Queue.Queue<
    Omit<MatrixBridgeInboundText, "roomAllowedOnly" | "ownershipEpoch">
  >;
  /**
   * Reports text the buffer could not take. It rides its own slot because the
   * report must survive the queue that is full, and only the latest matters.
   */
  readonly overflow: Queue.Queue<MatrixBridgeInboundOverflow>;
  /** Events taken from a batch but not finished with, and the batch each holds. */
  readonly inFlightEvents: Map<string, SyncBatchClaim | null>;
  /** The room the attached handler accepts, set by the live connection. */
  roomId: string | null;
  /** The filter its sync loop is running, or null before it started. */
  syncFilterKey: string | null;
}

interface MatrixConnection {
  readonly cryptoStoreGeneration: string;
  readonly roomId: string;
  readonly client: MatrixSdkClient;
  readonly botUserId: string;
  readonly allowedUserIds: ReadonlySet<string>;
  /**
   * Active membership by account, joined or invited, mutated synchronously by
   * the timeline handler. Sending reads it at the last moment, so an arrival
   * cannot slip in between a bridge decision and the encryption that follows
   * it. The two states are kept apart because only a joined account can read
   * what is sent now, while an invited one still makes the room unsafe.
   */
  readonly members: Map<string, "join" | "invite">;
  /** Incremented whenever the room's joined membership changes. */
  readonly membershipRevision: { value: number };
  /** Publishes the current membership when it differs from the last snapshot. */
  readonly publishMembership: Effect.Effect<void>;
  /**
   * Drops inbound text the transport is still holding, for when ownership
   * moves or the gate opens: those messages belong to what came before.
   */
  readonly discardPendingText: Effect.Effect<void>;
  /** Retries must present the ciphertext that was encrypted for the first attempt. */
  readonly encryptedPayloads: Map<string, unknown>;
  /** Transactions whose PUT may already have reached the homeserver. */
  readonly attemptedTransactions: Set<string>;
  /** Drops one transaction's ciphertext and its send record together. */
  readonly forgetTransaction: (transactionId: string) => void;
  /** Drops ciphertext for transactions no send has been attempted for. */
  readonly retireUnsentCiphertext: () => void;
}

/**
 * A room is safe to send into when nobody outside the allowed list is in it and
 * at least one allowed account is: a room with no reader would take a message
 * their devices could never decrypt after they return.
 */
function roomSendState(
  connection: MatrixConnection,
): "safe" | "unexpected-member" | "no-allowed-member" {
  let allowedReader = false;
  for (const [userId, membership] of connection.members) {
    if (userId === connection.botUserId) continue;
    // An outstanding invitation to an outsider is as unsafe as their presence.
    if (!connection.allowedUserIds.has(userId)) return "unexpected-member";
    // Only a joined account holds the keys for what is sent now.
    if (membership === "join") allowedReader = true;
  }
  return allowedReader ? "safe" : "no-allowed-member";
}

/** Both states of the membership, for the bridge to weigh separately. */
function membershipLists(members: ReadonlyMap<string, "join" | "invite">): {
  readonly joined: ReadonlyArray<string>;
  readonly invited: ReadonlyArray<string>;
} {
  const joined: Array<string> = [];
  const invited: Array<string> = [];
  for (const [userId, membership] of members) {
    if (membership === "join") joined.push(userId);
    else invited.push(userId);
  }
  return { joined: joined.toSorted(), invited: invited.toSorted() };
}

/** Compares the whole membership, so invite-to-join counts as a change. */
function membershipKey(members: ReadonlyMap<string, "join" | "invite">): string {
  return [...members]
    .map(([userId, membership]) => `${userId}:${membership}`)
    .toSorted()
    .join("\u0000");
}

const ROOM_SEND_STATE_REASONS = {
  "unexpected-member": "An account outside the allowed list is in the Matrix room.",
  "no-allowed-member": "No account from the allowed list is in the Matrix room.",
  unverified: "The Matrix room membership kept changing while it was being read.",
} as const;

/**
 * Re-reads the room's joined members from the homeserver and answers whether
 * only allowed accounts are in it. The timeline feed alone is not a complete
 * membership record: a gappy sync reports membership in the state block, which
 * the SDK does not emit. A change during the read invalidates the answer in
 * either direction, so it is read again rather than merged.
 */
const refreshRoomMembership = Effect.fn("MatrixBotSdkClient.refreshRoomMembership")(function* (
  connection: MatrixConnection,
  operation: "listen" | "send",
) {
  for (let attempt = 0; attempt < MATRIX_MEMBERSHIP_READ_ATTEMPTS; attempt += 1) {
    const revisionBeforeRead = connection.membershipRevision.value;
    const stateNow = yield* request(
      operation,
      "The bridged Matrix room members could not be read.",
      () => connection.client.getRoomState(connection.roomId),
    );
    const activeNow = activeMemberships(readRoomState(stateNow));
    if (connection.membershipRevision.value !== revisionBeforeRead) continue;
    const before = membershipKey(connection.members);
    connection.members.clear();
    for (const [userId, membership] of activeNow) connection.members.set(userId, membership);
    if (membershipKey(connection.members) !== before) {
      // An authoritative change counts like a timeline one: it moves the
      // revision and retires ciphertext encrypted for the previous devices,
      // except where a send under that transaction may already have landed.
      connection.membershipRevision.value += 1;
      connection.retireUnsentCiphertext();
    }
    yield* connection.publishMembership;
    return roomSendState(connection);
  }
  return "unverified" as const;
});

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
  /** Monotonic count of connection requests an operator has published. */
  let connectRequests = 0;
  /**
   * Woken by the counter itself. The count is maintained by a separate fiber,
   * so a request can still be in that fiber's queue when a waiter parks;
   * signalling the waiter from the increment is what makes the two orders
   * equivalent.
   */
  let connectWaiter: Deferred.Deferred<void> | null = null;
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
  const awaitReconnectSignal = (cryptoStoreGeneration: string, requestsAtStart: number) =>
    Effect.suspend(() =>
      // A request counted while the failure was being published is already the
      // answer; waiting for the next one would park behind an intent that has
      // been served.
      connectRequests !== requestsAtStart
        ? Effect.void
        : awaitReconnectPublication(cryptoStoreGeneration),
    );

  const awaitReconnectPublication = (cryptoStoreGeneration: string) =>
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

    const inbound = yield* Queue.dropping<
      Omit<MatrixBridgeInboundText, "roomAllowedOnly" | "ownershipEpoch">
    >(MATRIX_INBOUND_QUEUE_CAPACITY);
    const overflow = yield* Queue.sliding<MatrixBridgeInboundOverflow>(1);
    /** Events taken from a batch but not finished with, so a redelivered batch
     * does not queue them twice. Bounded by the queue it mirrors. */
    const inFlightEvents = new Map<string, SyncBatchClaim | null>();
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
          overflow,
          inFlightEvents,
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
          if (message === null) return;
          // Holding the cursor means the homeserver redelivers a batch whose
          // work is still running, so an event already in hand is not taken a
          // second time.
          if (inFlightEvents.has(message.eventId)) return;
          // A dropped message is reported rather than swallowed: the operator
          // sees why it never became a turn.
          if (!Queue.offerUnsafe(inbound, message)) {
            Queue.offerUnsafe(overflow, { kind: "overflow", roomId: message.roomId });
            return;
          }
          // The cursor this batch would advance to is now this message's to
          // release: a crash before that leaves Matrix redelivering it.
          inFlightEvents.set(message.eventId, fencedStorage.beginInbound());
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

    const members = new Map<string, "join" | "invite">();
    const membershipRevision = { value: 0 };
    const membershipQueue = yield* Queue.sliding<MatrixBridgeRoomMembership>(1);
    const encryptedPayloads = new Map<string, unknown>();
    /**
     * Transactions whose PUT may already have reached the homeserver. Their
     * ciphertext is what that transaction id now means: a repeat returns the
     * original event, so re-encrypting under it would report a message the
     * room never received.
     */
    const attemptedTransactions = new Set<string>();
    /**
     * Drops one transaction's ciphertext and the note that it was sent. The
     * two are kept together so the set cannot outgrow the cache it describes.
     */
    const forgetTransaction = (transactionId: string) => {
      encryptedPayloads.delete(transactionId);
      attemptedTransactions.delete(transactionId);
    };
    /** Retires only ciphertext that no send has been attempted for. */
    const retireUnsentCiphertext = () => {
      // Deleting the current entry is safe while iterating a Map.
      for (const transactionId of encryptedPayloads.keys()) {
        if (!attemptedTransactions.has(transactionId)) forgetTransaction(transactionId);
      }
    };
    let lastPublishedMembers: string | null = null;
    const membershipEvent = (): MatrixBridgeRoomMembership => ({
      kind: "membership",
      roomId,
      botUserId,
      ...membershipLists(members),
    });
    // Registered before the first read, so a change during it bumps the
    // revision and forces the read again rather than being missed until the
    // reconciliation interval comes round.
    yield* Effect.acquireRelease(
      Effect.sync(() => {
        const timelineHandler: MatrixSdkEventHandler = (eventRoomId, event) => {
          const change = toMembershipChange(event, eventRoomId, roomId);
          if (change === null) return;
          // Invite to join is a change too: it is what makes an allowed
          // account a reader, and what retires ciphertext built without them.
          const changed = (members.get(change.userId) ?? null) !== change.membership;
          if (change.membership === null) members.delete(change.userId);
          else members.set(change.userId, change.membership);
          if (!changed) return;
          membershipRevision.value += 1;
          // A held retry must not present a session the current members may not
          // have; the next attempt encrypts for the room as it is now. A
          // transaction already put to the homeserver keeps its ciphertext.
          retireUnsentCiphertext();
          Queue.offerUnsafe(membershipQueue, membershipEvent());
        };
        client.on(MATRIX_ROOM_EVENT, timelineHandler);
        return timelineHandler;
      }),
      (timelineHandler) =>
        Effect.sync(() => {
          client.off(MATRIX_ROOM_EVENT, timelineHandler);
        }),
    );

    // Seeded before the room is served so the first thing the bridge learns
    // about it is who is in it, and re-read when the room changes under it.
    for (let attempt = 0; attempt < MATRIX_MEMBERSHIP_READ_ATTEMPTS; attempt += 1) {
      const revisionBeforeRead = membershipRevision.value;
      const seeded = yield* request(
        "listen",
        "The bridged Matrix room members could not be read.",
        () => client.getRoomState(roomId),
      );
      if (membershipRevision.value !== revisionBeforeRead) continue;
      members.clear();
      for (const [userId, membership] of activeMemberships(readRoomState(seeded))) {
        members.set(userId, membership);
      }
      break;
    }

    const connection: MatrixConnection = {
      cryptoStoreGeneration: config.cryptoStoreGeneration,
      roomId,
      client,
      botUserId,
      allowedUserIds: new Set(config.allowedUserIds),
      members,
      membershipRevision,
      publishMembership: Effect.suspend(() => {
        const snapshot = membershipEvent();
        const key = `${snapshot.joined.join("\u0000")}|${snapshot.invited.join("\u0000")}`;
        if (key === lastPublishedMembers) return Effect.void;
        lastPublishedMembers = key;
        return Queue.offer(membershipQueue, snapshot).pipe(Effect.asVoid);
      }),
      // `clear` and not `takeAll`: nothing to discard is the common case, and
      // `takeAll` would wait for a message to arrive first.
      discardPendingText: Queue.clear(transport.inbound).pipe(
        Effect.tap((dropped) =>
          Effect.sync(() => {
            // A discarded message still holds its batch's cursor, so releasing
            // the claim is what lets the cursor advance past work nobody will
            // do; leaving it would stall the cursor for the whole connection.
            for (const event of dropped) {
              const claim = transport.inFlightEvents.get(event.eventId);
              transport.inFlightEvents.delete(event.eventId);
              if (claim != null) transport.fence.endInbound(claim);
            }
          }),
        ),
        Effect.tap((dropped) =>
          dropped.length === 0
            ? Effect.void
            : Effect.logWarning("Matrix bridge discarded inbound messages it may no longer start", {
                dropped: dropped.length,
              }),
        ),
        Effect.asVoid,
      ),
      encryptedPayloads,
      attemptedTransactions,
      forgetTransaction,
      retireUnsentCiphertext,
    };

    // This connection's membership is applied before anything can be sent on
    // it, so a reconnect never delivers under the previous connection's
    // membership and the first published status already reflects the room.
    const initialMembership = membershipEvent();
    lastPublishedMembers = `${initialMembership.joined.join("\u0000")}|${initialMembership.invited.join("\u0000")}`;
    yield* onInboundText(initialMembership);

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

    // Membership and overflow drain independently of text: they decide whether
    // output may leave, and a report must not queue behind a dispatch that can
    // take the full thirty seconds.
    yield* Effect.forkScoped(Stream.runForEach(Stream.fromQueue(membershipQueue), onInboundText));
    yield* Effect.forkScoped(
      Stream.runForEach(Stream.fromQueue(transport.overflow), onInboundText),
    );
    yield* Effect.forkScoped(
      Effect.sleep(MATRIX_MEMBERSHIP_RECONCILE_INTERVAL_MS).pipe(
        Effect.andThen(refreshRoomMembership(connection, "listen").pipe(Effect.ignore)),
        Effect.forever,
      ),
    );
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
          // Read before the verification below, so the message carries the
          // bridge it was taken off the queue for: a move during that work
          // invalidates it rather than redirecting it into another thread.
          const ownershipEpoch = stored.value.ownershipEpoch;
          // Asked at hand-off, from the homeserver, for the same reason the
          // send gate asks: a room that cannot be verified is not safe.
          const state = yield* verifyRoomForInbound(connection);
          yield* onInboundText({ ...event, roomAllowedOnly: state === "safe", ownershipEpoch });
        }).pipe(
          // Released however this ends: the cursor must not be held by a
          // message the bridge has finished with, or refused.
          Effect.ensuring(
            Effect.sync(() => {
              const claim = transport.inFlightEvents.get(event.eventId);
              transport.inFlightEvents.delete(event.eventId);
              if (claim != null) transport.fence.endInbound(claim);
            }),
          ),
        ),
      ),
    );
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

  /**
   * A message is only dropped as unsafe once verification has really failed: a
   * transient read is retried, and a room that stays unverifiable is reported
   * rather than quietly swallowing commands.
   */
  const verifyRoomForInbound = Effect.fn("MatrixBotSdkClient.verifyRoomForInbound")(function* (
    connection: MatrixConnection,
  ) {
    for (let attempt = 0; attempt < MATRIX_MEMBERSHIP_VERIFY_ATTEMPTS; attempt += 1) {
      const verified = yield* Effect.result(refreshRoomMembership(connection, "listen"));
      // A room that kept changing under the read is unverified, not verified
      // unsafe: it is retried like a failed read and reported the same way.
      if (verified._tag === "Success" && verified.success !== "unverified") {
        return verified.success;
      }
      if (verified._tag === "Failure" && verified.failure.retryability === "permanent") break;
      yield* Effect.sleep(reconnectDelayMs(attempt));
    }
    yield* Effect.logWarning("Matrix bridge could not verify the room for an inbound message");
    yield* configService.reportDegradedIfMatches({
      cryptoStoreGeneration: connection.cryptoStoreGeneration,
      cause: "inbound-unverified",
    });
    return "unverified" as const;
  });

  const listen: MatrixBridgeClient["Service"]["listen"] = (onInboundText) =>
    Effect.gen(function* () {
      // The client outlives an individual connection so reconfiguration can
      // reuse it; shutting the listener down is what finally releases it.
      yield* Effect.addFinalizer(() => retireLiveTransport());

      // Every `configure` publishes `connecting`, so counting those is a
      // monotonic record of how many times an operator has asked for a
      // connection. The subscription is acquired here, before any connection
      // runs, so an intent expressed while a failure is being reported is
      // counted rather than overwritten and lost.
      const statusPull = yield* Stream.toPull(configService.statusChanges);
      let replayedStatus = false;
      yield* Effect.forkScoped(
        Stream.runForEach(Stream.fromPull(Effect.succeed(statusPull)), (status) =>
          Effect.sync(() => {
            // The subscription replays the status this listener starts from,
            // which describes a request that has already been taken up.
            if (!replayedStatus) {
              replayedStatus = true;
              return;
            }
            if (status.state !== "connecting") return;
            connectRequests += 1;
            if (connectWaiter === null) return;
            Deferred.doneUnsafe(connectWaiter, Effect.void);
            connectWaiter = null;
          }),
        ).pipe(Effect.catchCause(() => Effect.void)),
      );

      let attempt = 0;
      while (true) {
        // Disconnect means no Matrix activity at all, not a client left
        // syncing in the background.
        if (Option.isNone(yield* currentConfig)) yield* retireLiveTransport();
        const config = yield* awaitConfigured;
        const requestsAtStart = connectRequests;
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

        if (connectRequests !== requestsAtStart) {
          // Somebody pressed Connect while this attempt was failing. Their
          // intent is newer than the failure, so it is answered instead of
          // being buried under an unavailable status nobody asked for.
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
          yield* awaitReconnectSignal(config.cryptoStoreGeneration, requestsAtStart);
          attempt = 0;
          continue;
        }
        // An explicit Connect or Disconnect during the backoff is an answer to
        // the failure, so it is not left waiting out the remaining delay.
        yield* Effect.raceFirst(
          Effect.sleep(reconnectDelayMs(attempt)),
          awaitReconnectSignal(config.cryptoStoreGeneration, requestsAtStart),
        );
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
      if (!oldest.done) connection.forgetTransaction(oldest.value);
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

    // The timeline feed alone is not a complete membership record, so the gate
    // before encryption asks the homeserver. That read also fails when the bot
    // itself is no longer in the room.
    const beforeEncryption = yield* refreshRoomMembership(connection, "send");
    if (beforeEncryption !== "safe") {
      return yield* clientError("send", ROOM_SEND_STATE_REASONS[beforeEncryption], "transient");
    }

    // Encryption is asynchronous, so it is fenced on the membership revision:
    // any change at all during it - a join, a leave, or one reader swapped for
    // another - means the ciphertext was built for a device set that is no
    // longer the room's, so it is discarded and the retry encrypts afresh.
    const revisionBeforeEncryption = connection.membershipRevision.value;
    const encrypted = yield* encryptOnce(connection, message);
    // Asked of the homeserver again, not of the cache: preparing encryption is
    // itself a joined-member lookup inside the SDK, so an account that joined
    // through a sync gap can have taken this room key without the timeline ever
    // saying so. The revision comparison then covers the rest: any change at
    // all - a join, a leave, or one reader swapped for another - means the
    // ciphertext was built for a device set that is no longer the room's.
    const afterEncryption = yield* refreshRoomMembership(connection, "send");
    if (
      afterEncryption !== "safe" ||
      connection.membershipRevision.value !== revisionBeforeEncryption
    ) {
      connection.forgetTransaction(message.transactionId);
      return yield* clientError(
        "send",
        afterEncryption === "safe"
          ? "The Matrix room membership changed while the message was being encrypted."
          : `${ROOM_SEND_STATE_REASONS[afterEncryption]} The room changed while the message was being encrypted.`,
        "transient",
      );
    }

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
    // Ownership can move while the reads and the encryption above are running,
    // and neither the generation nor the room changes when it does. Transient,
    // not permanent: the bridge drops the job on its next look at the owner,
    // and this is a normal move rather than a delivery fault to report.
    if (message.ownershipEpoch !== null && stored.ownershipEpoch !== message.ownershipEpoch) {
      return yield* clientError(
        "send",
        "The Matrix bridge moved to another thread before the message could be sent.",
        "transient",
      );
    }

    // The transaction ID makes the send idempotent across retries: the
    // homeserver returns the original event for a repeated PUT. From here on
    // this ciphertext is what that transaction means, even if the response is
    // lost, so it is never replaced by a re-encryption.
    connection.attemptedTransactions.add(message.transactionId);
    yield* request("send", "The Matrix message could not be delivered.", () =>
      connection.client.doRequest(
        "PUT",
        encryptedSendEndpoint(message.roomId, message.transactionId),
        null,
        encrypted,
      ),
    );
    connection.forgetTransaction(message.transactionId);
  });

  const discardPendingInbound: MatrixBridgeClient["Service"]["discardPendingInbound"] = Ref.get(
    connectionRef,
  ).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.void,
        onSome: (connection) => connection.discardPendingText,
      }),
    ),
  );

  return MatrixBridgeClient.of({ listen, sendText, discardPendingInbound });
});

export const layer = Layer.effect(MatrixBridgeClient, make());
