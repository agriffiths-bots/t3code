import * as NodeServices from "@effect/platform-node/NodeServices";
import { MatrixBridgeOperationError } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import type {
  MatrixBridgeInboundEvent,
  MatrixBridgeInboundText,
  MatrixBridgeRoomMembership,
} from "./MatrixBridgeClient.ts";
import {
  MATRIX_INBOUND_QUEUE_CAPACITY,
  MEGOLM_ALGORITHM,
  buildSyncFilter,
  make,
  trackSyncRequests,
  type MatrixCreateRoomOptions,
  type MatrixSdkClient,
  type MatrixSdkEventHandler,
  type MatrixSdkFilterInfo,
  type MatrixSdkModule,
  type MatrixSdkStorageProvider,
} from "./MatrixBotSdkClient.ts";
import { MatrixBridgeConfig, layer as matrixBridgeConfigLayer } from "./MatrixBridgeConfig.ts";

const BOT_USER_ID = "@t3bot:matrix.example.test";
const ALLOWED_USER_ID = "@adam:beeper.com";
const ROOM_ID = "!bridge:matrix.example.test";
const HOMESERVER_URL = "https://matrix.example.test/";

interface MatrixHttpError {
  readonly statusCode: number;
}

const httpError = (statusCode: number): MatrixHttpError => ({ statusCode });

interface RecordedRequest {
  readonly method: string;
  readonly endpoint: string;
  readonly qs: unknown;
  readonly body: unknown;
}

interface FakeSdkOptions {
  readonly roomState?: Record<string, unknown>;
  readonly joinedRooms?: ReadonlyArray<string>;
  /** The homeserver's own view of who is joined to the bridged room. */
  readonly joinedMembers?: ReadonlyArray<string>;
  readonly syncToken?: string | null;
  readonly storedFilter?: MatrixSdkFilterInfo | null;
  readonly roomMembership?: Record<string, string>;
  readonly roomCreator?: string;
  readonly roomCreateContent?: Record<string, unknown>;
  readonly deviceId?: string | null;
  /** Room identifiers handed out by successive room creations. */
  readonly roomIds?: ReadonlyArray<string>;
  /** Leaves a sync request outstanding until the test settles it. */
  readonly pendingSync?: boolean;
  /** Leaves the post-response processing outstanding until the test settles it. */
  readonly pendingProcessing?: boolean;
  /** Content of the catch-up batch each `start()` delivers, as a server would. */
  readonly catchUp?: {
    readonly roomKeys?: ReadonlyArray<string>;
    readonly timeline?: ReadonlyArray<{
      readonly roomId?: string;
      readonly sessionKey?: string;
      readonly event: unknown;
    }>;
  };
  readonly cryptoReady?: boolean;
  readonly failSendsWith?: ReadonlyArray<unknown>;
}

const botAdministeredPowerLevels = () => ({
  users_default: 0,
  events_default: 0,
  state_default: 100,
  invite: 100,
  kick: 100,
  ban: 100,
  redact: 100,
  events: { "m.room.redaction": 100 },
  users: { [BOT_USER_ID]: 100 },
});

const encryptedRoomState = (): Record<string, unknown> => ({
  "m.room.join_rules": { join_rule: "invite" },
  "m.room.encryption": { algorithm: MEGOLM_ALGORITHM },
  "m.room.history_visibility": { history_visibility: "invited" },
  "m.room.power_levels": botAdministeredPowerLevels(),
});

interface FakeSdk {
  readonly module: MatrixSdkModule;
  readonly load: () => Promise<MatrixSdkModule>;
  readonly createdRooms: Array<MatrixCreateRoomOptions>;
  readonly requests: Array<RecordedRequest>;
  readonly encryptions: Array<unknown>;
  readonly storageWrites: Array<string | null>;
  readonly startedFilters: Array<unknown>;
  readonly startedAfterStoredToken: Array<string | null>;
  readonly clientOptions: Array<{
    readonly homeserverUrl: string;
    readonly accessToken: string;
  }>;
  readonly syncStorePaths: Array<string>;
  readonly cryptoStores: Array<{ readonly storagePath: string; readonly storeType: number }>;
  readonly invitations: Array<{ readonly userId: string; readonly roomId: string }>;
  /** Mirrors one processed sync batch: cursor first, then crypto, then events. */
  readonly deliverSyncBatch: (batch: {
    readonly cursor: string;
    readonly roomKeys?: ReadonlyArray<string>;
    readonly timeline?: ReadonlyArray<{
      readonly roomId?: string;
      readonly sessionKey?: string;
      readonly event: unknown;
    }>;
    /** Defaults to the newest client; earlier indexes are retired ones. */
    readonly clientIndex?: number;
  }) => void;
  readonly clients: Array<{
    readonly storage: MatrixSdkStorageProvider;
    readonly accessToken: () => string;
    readonly filterId: () => string | undefined;
  }>;
  readonly storedSyncToken: (storePath: string) => string | null;
  readonly setDeviceId: (next: string) => void;
  readonly pendingSyncCount: () => number;
  readonly stoppedClients: () => ReadonlyArray<number>;
  readonly resolvePendingSyncs: () => void;
  readonly pendingProcessingCount: () => number;
  readonly resolvePendingProcessing: () => void;
  /** Delivers a plaintext timeline event, as a membership change arrives. */
  readonly emitTimeline: (roomId: string, event: unknown) => void;
  /** Joins an account the way a gappy sync does: no room event is emitted. */
  readonly joinSilently: (userId: string) => void;
  /** The same for a leave the client never hears about. */
  readonly leaveSilently: (userId: string) => void;
  /** Runs once, inside the next membership read, to race a change against it. */
  readonly onMembersRead: (hook: () => void) => void;
  /** The same, on every read, for a room that never settles. */
  readonly onEveryMembersRead: (hook: () => void) => void;
  /** Runs once, inside the next encryption, to race membership against it. */
  readonly onEncrypt: (hook: () => void) => void;
  /** Fails the next `count` membership reads, the way a blip would. */
  readonly failNextMemberReads: (count: number) => void;
  readonly started: Deferred.Deferred<void>;
}

const makeFakeSdk = (options: FakeSdkOptions = {}): FakeSdk => {
  const createdRooms: Array<MatrixCreateRoomOptions> = [];
  // The homeserver's own view of the room, which a membership event updates
  // before any client hears about it.
  const joinedMembers = new Set(options.joinedMembers ?? [BOT_USER_ID]);
  let membersReadHook: (() => void) | null = null;
  let everyMembersReadHook: (() => void) | null = null;
  let encryptHook: (() => void) | null = null;
  let failingMemberReads = 0;
  const requests: Array<RecordedRequest> = [];
  const encryptions: Array<unknown> = [];
  const storageWrites: Array<string | null> = [];
  const startedFilters: Array<unknown> = [];
  const startedAfterStoredToken: Array<string | null> = [];
  const clientOptions: Array<{ readonly homeserverUrl: string; readonly accessToken: string }> = [];
  const syncStorePaths: Array<string> = [];
  const cryptoStores: Array<{ readonly storagePath: string; readonly storeType: number }> = [];
  const handlers = new Map<string, Set<MatrixSdkEventHandler>>();
  const knownRoomKeys = new Set<string>();
  const invitations: Array<{ readonly userId: string; readonly roomId: string }> = [];
  const roomMembership = new Map<string, string>([
    [BOT_USER_ID, "join"],
    ...Object.entries(options.roomMembership ?? {}),
  ]);
  const started = Deferred.makeUnsafe<void>();
  const clients: Array<{
    readonly storage: MatrixSdkStorageProvider;
    readonly processSync: () => void;
    readonly accessToken: () => string;
    readonly filterId: () => string | undefined;
  }> = [];
  const failSends = [...(options.failSendsWith ?? [])];
  const roomState = options.roomState ?? encryptedRoomState();
  // One entry per store path, so a replacement client cannot silently share
  // the retired client's cursor.
  const storedTokens = new Map<string, string | null>();
  const storedFilters = new Map<string, MatrixSdkFilterInfo | null>();
  const pendingSyncs: Array<() => void> = [];
  const pendingProcessing: Array<() => void> = [];
  const stopCalls: Array<number> = [];
  let deviceId = options.deviceId === undefined ? "T3DEVICE" : options.deviceId;
  let encryptionCounter = 0;
  let batchCounter = 0;

  const deliverSyncBatch: FakeSdk["deliverSyncBatch"] = ({
    cursor,
    roomKeys = [],
    timeline = [],
    clientIndex,
  }) => {
    // matrix-bot-sdk persists the cursor before it processes the batch, and the
    // processing itself is what the drain barrier has to outlast.
    const target = clients[clientIndex ?? clients.length - 1];
    target?.storage.setSyncToken(cursor);
    target?.processSync();
    // To-device room keys reach the crypto store even for a fenced batch.
    for (const key of roomKeys) knownRoomKeys.add(key);
    for (const entry of timeline) {
      // An event whose Megolm session never arrived cannot be decrypted, so
      // the SDK would emit a failed decryption instead of this event.
      if (entry.sessionKey !== undefined && !knownRoomKeys.has(entry.sessionKey)) continue;
      for (const handler of handlers.get("room.decrypted_event") ?? []) {
        handler(entry.roomId ?? ROOM_ID, entry.event);
      }
    }
  };

  class FakeStorageProvider implements MatrixSdkStorageProvider {
    readonly path: string;

    constructor(filename: string, persistent = true) {
      this.path = filename;
      if (!persistent) return;
      syncStorePaths.push(filename);
      if (!storedTokens.has(filename)) storedTokens.set(filename, options.syncToken ?? null);
      if (!storedFilters.has(filename)) storedFilters.set(filename, options.storedFilter ?? null);
    }
    getSyncToken(): string | null {
      return storedTokens.get(this.path) ?? null;
    }
    setSyncToken(token: string | null): void {
      storedTokens.set(this.path, token);
      storageWrites.push(token);
    }
    getFilter(): MatrixSdkFilterInfo | null {
      return storedFilters.get(this.path) ?? null;
    }
    setFilter(filter: MatrixSdkFilterInfo): void {
      storedFilters.set(this.path, filter);
    }
  }

  class FakeCryptoStorageProvider {
    readonly storagePath: string;
    readonly storeType: number;

    constructor(storagePath: string, storeType: number) {
      this.storagePath = storagePath;
      this.storeType = storeType;
      cryptoStores.push({ storagePath, storeType });
    }
  }

  class FakeMatrixClient implements MatrixSdkClient {
    readonly storage: MatrixSdkStorageProvider;
    readonly cryptoStore: unknown;
    /** Writable, as on the real client: a token refresh retunes it in place. */
    accessToken: string;
    /** -1 for the identity probe, which owns no persistent state. */
    readonly index: number;
    readonly crypto = {
      isReady: options.cryptoReady ?? true,
      encryptRoomEvent: (roomId: string, eventType: string, content: unknown) => {
        encryptionCounter += 1;
        const hook = encryptHook;
        encryptHook = null;
        hook?.();
        const encrypted = {
          algorithm: MEGOLM_ALGORITHM,
          ciphertext: `ciphertext-${encryptionCounter}`,
          roomId,
          eventType,
          content,
        };
        encryptions.push(encrypted);
        return Promise.resolve(encrypted);
      },
    };

    constructor(
      homeserverUrl: string,
      accessToken: string,
      storage?: MatrixSdkStorageProvider,
      cryptoStore?: unknown,
    ) {
      // The identity probe owns no persistent state, exactly like the SDK's
      // default in-memory storage.
      this.accessToken = accessToken;
      this.storage = storage ?? new FakeStorageProvider("memory", false);
      this.cryptoStore = cryptoStore;
      this.index = storage === undefined ? -1 : clients.length;
      if (storage !== undefined) {
        const self = this as unknown as {
          processSync: () => unknown;
          accessToken: string;
          filterId?: string;
        };
        clients.push({
          storage,
          // Runs through the tracking subclass, so the barrier counts it.
          processSync: () => void self.processSync(),
          accessToken: () => self.accessToken,
          filterId: () => self.filterId,
        });
        clientOptions.push({ homeserverUrl, accessToken });
      }
    }

    getJoinedRooms(): Promise<ReadonlyArray<string>> {
      return Promise.resolve(options.joinedRooms ?? [ROOM_ID]);
    }

    getJoinedRoomMembers(_roomId: string): Promise<ReadonlyArray<string>> {
      if (failingMemberReads > 0) {
        failingMemberReads -= 1;
        return Promise.reject(httpError(502));
      }
      // Deliberately the pre-hook snapshot: a homeserver answers what it knew
      // when the request arrived.
      const snapshot = [...joinedMembers];
      const hook = membersReadHook;
      membersReadHook = null;
      hook?.();
      everyMembersReadHook?.();
      return Promise.resolve(snapshot);
    }

    getRoomState(_roomId: string): Promise<ReadonlyArray<unknown>> {
      const events: Array<unknown> = [
        {
          type: "m.room.create",
          state_key: "",
          sender: options.roomCreator ?? BOT_USER_ID,
          // Version 11 is what current homeservers create, Beeper included.
          content: options.roomCreateContent ?? { room_version: "11" },
        },
        ...Object.entries(roomState).map(([type, content]) => ({
          type,
          state_key: "",
          sender: BOT_USER_ID,
          content,
        })),
        ...[...roomMembership].map(([userId, membership]) => ({
          type: "m.room.member",
          state_key: userId,
          sender: userId,
          content: { membership },
        })),
      ];
      return Promise.resolve(events);
    }

    createRoom(createOptions: MatrixCreateRoomOptions): Promise<string> {
      createdRooms.push(createOptions);
      for (const userId of createOptions.invite) roomMembership.set(userId, "invite");
      const roomId = (options.roomIds ?? [])[createdRooms.length - 1] ?? ROOM_ID;
      return Promise.resolve(roomId);
    }

    inviteUser(userId: string, roomId: string): Promise<unknown> {
      invitations.push({ userId, roomId });
      roomMembership.set(userId, "invite");
      return Promise.resolve({});
    }

    doRequest(method: string, endpoint: string, qs?: unknown, body?: unknown): Promise<unknown> {
      requests.push({ method, endpoint, qs, body });
      if (endpoint.endsWith("/filter")) {
        return Promise.resolve({ filter_id: "server-filter-id" });
      }
      if (endpoint === "/_matrix/client/v3/account/whoami") {
        return Promise.resolve({
          user_id: BOT_USER_ID,
          ...(deviceId === null ? {} : { device_id: deviceId }),
        });
      }
      if (endpoint === "/_matrix/client/v3/sync") {
        // A long poll the caller settles by hand, standing in for the request
        // `stop()` cannot cancel.
        return new Promise<unknown>((resolve) => {
          pendingSyncs.push(() => resolve({ next_batch: "late-cursor" }));
        });
      }
      const failure = failSends.shift();
      if (failure !== undefined) return Promise.reject(failure);
      return Promise.resolve({ event_id: "$sent" });
    }

    // Mirrors matrix-bot-sdk 0.8.0: an absent or mismatched stored filter is
    // created here, and creating it wipes the stored sync token.
    start(filter?: unknown): Promise<unknown> {
      if (
        filter !== undefined &&
        filter !== null &&
        JSON.stringify(this.storage.getFilter()?.filter) !== JSON.stringify(filter)
      ) {
        this.storage.setSyncToken(null);
        this.storage.setFilter({ id: "sdk-created-filter", filter });
      }
      startedFilters.push(filter);
      startedAfterStoredToken.push(this.storage.getSyncToken());
      Deferred.doneUnsafe(started, Effect.void);
      // The sync loop runs in the background; its first response arrives just
      // after `start` resolves.
      batchCounter += 1;
      const cursor = `catch-up-${batchCounter}`;
      const clientIndex = clients.length - 1;
      queueMicrotask(() => deliverSyncBatch({ cursor, clientIndex, ...options.catchUp }));
      // Mirrors the SDK's own long poll, so a caller can hold one open across a
      // handover.
      if (options.pendingSync === true) void this.doRequest("GET", "/_matrix/client/v3/sync");
      return Promise.resolve(undefined);
    }

    stop(): void {
      if (this.index >= 0) stopCalls.push(this.index);
    }

    /** Stands in for the SDK's post-response work: cursor, crypto, events. */
    processSync(): Promise<unknown> {
      if (options.pendingProcessing !== true) return Promise.resolve(undefined);
      return new Promise<unknown>((resolve) => {
        pendingProcessing.push(() => resolve(undefined));
      });
    }

    on(event: string, handler: MatrixSdkEventHandler): unknown {
      const existing = handlers.get(event) ?? new Set<MatrixSdkEventHandler>();
      existing.add(handler);
      handlers.set(event, existing);
      return this;
    }

    off(event: string, handler: MatrixSdkEventHandler): unknown {
      handlers.get(event)?.delete(handler);
      return this;
    }
  }

  const module: MatrixSdkModule = {
    MatrixClient: FakeMatrixClient,
    SimpleFsStorageProvider: FakeStorageProvider,
    RustSdkCryptoStorageProvider: FakeCryptoStorageProvider,
  };

  return {
    module,
    load: () => Promise.resolve(module),
    createdRooms,
    requests,
    encryptions,
    storageWrites,
    startedFilters,
    startedAfterStoredToken,
    clientOptions,
    syncStorePaths,
    cryptoStores,
    invitations,
    clients,
    storedSyncToken: (storePath) => storedTokens.get(storePath) ?? null,
    setDeviceId: (next) => {
      deviceId = next;
    },
    pendingSyncCount: () => pendingSyncs.length,
    stoppedClients: () => [...stopCalls],
    resolvePendingSyncs: () => {
      for (const resolve of pendingSyncs.splice(0)) resolve();
    },
    pendingProcessingCount: () => pendingProcessing.length,
    resolvePendingProcessing: () => {
      for (const resolve of pendingProcessing.splice(0)) resolve();
    },
    deliverSyncBatch,
    emitTimeline: (roomId, event) => {
      const record = event as {
        type?: string;
        state_key?: string;
        content?: { membership?: string };
      };
      if (record.type === "m.room.member" && typeof record.state_key === "string") {
        if (record.content?.membership === "join") joinedMembers.add(record.state_key);
        else joinedMembers.delete(record.state_key);
      }
      for (const handler of handlers.get("room.event") ?? []) handler(roomId, event);
    },
    joinSilently: (userId) => {
      joinedMembers.add(userId);
    },
    leaveSilently: (userId) => {
      joinedMembers.delete(userId);
    },
    onMembersRead: (hook) => {
      membersReadHook = hook;
    },
    onEveryMembersRead: (hook) => {
      everyMembersReadHook = hook;
    },
    onEncrypt: (hook) => {
      encryptHook = hook;
    },
    failNextMemberReads: (count) => {
      failingMemberReads = count;
    },
    started,
  };
};

const decryptedTextEvent = (input: {
  readonly eventId: string;
  readonly sender: string;
  readonly body: string;
}) => ({
  type: "m.room.message",
  event_id: input.eventId,
  sender: input.sender,
  content: { msgtype: "m.text", body: input.body },
});

const testLayer = matrixBridgeConfigLayer.pipe(
  Layer.provideMerge(ServerSecretStore.layer),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-matrix-adapter-test-" })),
);

const membershipEvent = (input: { readonly userId: string; readonly membership: string }) => ({
  type: "m.room.member",
  event_id: `$member-${input.userId}-${input.membership}`,
  sender: input.userId,
  state_key: input.userId,
  content: { membership: input.membership },
});

const configureBridge = Effect.fn("configureBridge")(function* () {
  const configService = yield* MatrixBridgeConfig;
  yield* configService.configure({
    homeserverUrl: HOMESERVER_URL,
    accessToken: "matrix-secret-token",
    allowedUserIds: [ALLOWED_USER_ID],
  });
  return configService;
});

const awaitStatusState = (
  configService: MatrixBridgeConfig["Service"],
  state: string,
): Effect.Effect<void> =>
  configService.statusChanges.pipe(
    Stream.filter((status) => status.state === state),
    Stream.runHead,
    Effect.asVoid,
  );

/** Waits on a synchronous condition without touching the test clock. */
const awaitCondition = Effect.fn("awaitCondition")(function* (predicate: () => boolean) {
  for (let attempt = 0; attempt < 5_000; attempt += 1) {
    if (predicate()) return;
    yield* Effect.yieldNow;
  }
  assert.fail("condition never became true");
});

/** Starts the adapter's listen loop for the life of the test scope. */
const startAdapter = Effect.fn("startAdapter")(function* (
  sdk: FakeSdk,
  onInbound: (event: MatrixBridgeInboundEvent) => Effect.Effect<void> = () => Effect.void,
) {
  const client = yield* make(sdk.load);
  yield* Effect.forkScoped(client.listen(onInbound));
  return client;
});

it.layer(NodeServices.layer)("MatrixBotSdkClient", (it) => {
  it.effect("creates one private encrypted room and invites the allowed users", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedRooms: [ROOM_ID] });
      const configService = yield* configureBridge();
      yield* startAdapter(sdk);
      yield* Deferred.await(sdk.started);
      yield* awaitStatusState(configService, "waiting-for-member");

      assert.lengthOf(sdk.createdRooms, 1);
      const created = sdk.createdRooms[0];
      assert.isDefined(created);
      assert.equal(created?.visibility, "private");
      // `trusted_private_chat` would promote invitees to the bot's power level.
      assert.equal(created?.preset, "private_chat");
      assert.equal(created?.room_version, "11");
      assert.isTrue(created?.is_direct);
      assert.deepEqual([...(created?.invite ?? [])], [ALLOWED_USER_ID]);
      assert.equal(created?.power_level_content_override.invite, 100);
      assert.equal(created?.power_level_content_override.users_default, 0);
      assert.equal(created?.power_level_content_override.events_default, 0);
      assert.equal(created?.power_level_content_override.state_default, 100);
      assert.deepEqual(created?.power_level_content_override.events, {
        "m.room.redaction": 100,
      });
      assert.deepEqual(
        [...(created?.initial_state ?? [])],
        [
          {
            type: "m.room.encryption",
            state_key: "",
            content: { algorithm: MEGOLM_ALGORITHM },
          },
          {
            type: "m.room.history_visibility",
            state_key: "",
            content: { history_visibility: "invited" },
          },
        ],
      );

      const persisted = Option.getOrThrow(yield* configService.currentConfig);
      assert.equal(persisted.roomId, ROOM_ID);
      assert.deepEqual(
        [...sdk.clientOptions],
        [{ homeserverUrl: HOMESERVER_URL, accessToken: "matrix-secret-token" }],
      );
      const status = yield* configService.status;
      assert.isTrue(status.encryptionReady);
      assert.equal(status.reason, null);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("keeps one store per Matrix device across restarts and allowlist edits", () =>
    Effect.gen(function* () {
      const configService = yield* configureBridge();
      const serverConfig = yield* ServerConfig.ServerConfig;
      const path = yield* Path.Path;
      const bridgeDir = path.join(serverConfig.secretsDir, "matrix-bridge");

      const first = makeFakeSdk();
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* startAdapter(first);
          yield* Deferred.await(first.started);
        }),
      );

      // An allowlist edit keeps the same access token, so it must keep the same
      // encryption store: a Matrix device can only upload keys from one store.
      yield* configService.configure({
        homeserverUrl: HOMESERVER_URL,
        accessToken: "matrix-secret-token",
        allowedUserIds: [ALLOWED_USER_ID, "@second:beeper.com"],
      });
      const second = makeFakeSdk({ syncToken: "stored-cursor" });
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* startAdapter(second);
          yield* Deferred.await(second.started);
        }),
      );

      const storeDir = path.dirname(first.syncStorePaths[0] ?? "");
      assert.equal(path.dirname(storeDir), bridgeDir);
      assert.deepEqual([...second.syncStorePaths], [...first.syncStorePaths]);
      assert.deepEqual([...second.cryptoStores], [...first.cryptoStores]);
      assert.deepEqual(
        [...first.cryptoStores],
        [{ storagePath: path.join(storeDir, "crypto"), storeType: 0 }],
      );
      const storeStat = yield* (yield* FileSystem.FileSystem).stat(storeDir);
      assert.equal(storeStat.mode & 0o777, 0o700);

      // A refreshed token that still names the same device keeps the store,
      // because the device is what may upload keys from exactly one store.
      yield* configService.configure({
        homeserverUrl: HOMESERVER_URL,
        accessToken: "refreshed-token",
        allowedUserIds: [ALLOWED_USER_ID],
      });
      const refreshed = makeFakeSdk({ syncToken: "stored-cursor" });
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* startAdapter(refreshed);
          yield* Deferred.await(refreshed.started);
        }),
      );
      assert.deepEqual([...refreshed.syncStorePaths], [...first.syncStorePaths]);

      // A different device does get its own store.
      const rotated = makeFakeSdk({ deviceId: "T3DEVICE-2" });
      yield* startAdapter(rotated);
      yield* Deferred.await(rotated.started);
      assert.notDeepEqual([...rotated.syncStorePaths], [...first.syncStorePaths]);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("reuses one client for the device instead of opening its stores twice", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      // An allowlist edit mints a new connection identity but not a new device,
      // and `stop()` cannot cancel a sync already in flight, so a second client
      // on these stores would race the first one's last response.
      yield* configService.configure({
        homeserverUrl: HOMESERVER_URL,
        accessToken: "matrix-secret-token",
        allowedUserIds: [ALLOWED_USER_ID, "@second:beeper.com"],
      });
      yield* awaitStatusState(configService, "waiting-for-member");

      assert.lengthOf(sdk.clients, 1);
      assert.lengthOf(sdk.cryptoStores, 1);
      assert.lengthOf(new Set(sdk.syncStorePaths), 1);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("opens a fresh loop on the same stores when the room changes", () =>
    Effect.gen(function* () {
      const secondRoom = "!second:matrix.example.test";
      const sdk = makeFakeSdk({
        roomIds: [ROOM_ID, secondRoom],
        joinedRooms: [ROOM_ID, secondRoom],
      });
      const configService = yield* configureBridge();
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      // A new connection identity means a new room and a new filter. The
      // running loop cannot take one, because the request already in flight
      // carries the old filter and would advance the cursor past the new
      // room's events, so it is retired and a fresh loop opens on the same
      // stores and resumes from the stored cursor.
      yield* configService.configure({
        homeserverUrl: HOMESERVER_URL,
        accessToken: "matrix-secret-token",
        allowedUserIds: [ALLOWED_USER_ID, "@second:beeper.com"],
      });
      yield* awaitCondition(() => sdk.createdRooms.length > 1);
      yield* awaitStatusState(configService, "waiting-for-member");

      assert.deepEqual([...sdk.stoppedClients()], [0]);
      assert.lengthOf(sdk.clients, 2);
      // Same device, so the same stores, opened only after the first client
      // was retired and had gone idle.
      assert.lengthOf(new Set(sdk.syncStorePaths), 1);
      assert.deepEqual([...sdk.startedFilters].at(-1), buildSyncFilter(secondRoom));
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("retires the old client before a replacement token is used", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      // Until the homeserver says which device a replacement token belongs to,
      // no running loop may use it: it could pull another device's to-device
      // messages into these stores. The old client is retired first, and the
      // replacement opens the stores only once that one has gone idle.
      yield* configService.configure({
        homeserverUrl: HOMESERVER_URL,
        accessToken: "replacement-token",
        allowedUserIds: [ALLOWED_USER_ID],
      });
      yield* awaitCondition(() => sdk.clients.length === 2);
      yield* awaitStatusState(configService, "waiting-for-member");

      assert.deepEqual([...sdk.stoppedClients()], [0]);
      assert.deepEqual(
        sdk.clientOptions.map((options) => options.accessToken),
        ["matrix-secret-token", "replacement-token"],
      );
      // The device is unchanged, so the replacement holds the same stores.
      assert.lengthOf(new Set(sdk.syncStorePaths), 1);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("counts the processing that follows a sync response as busy", () =>
    Effect.gen(function* () {
      // The response is half an iteration: the SDK then writes the cursor,
      // feeds crypto and emits events, and a client is only idle after that.
      const settlers: Array<() => void> = [];
      class BaseClient {
        processSync(): Promise<unknown> {
          return new Promise<unknown>((resolve) => {
            settlers.push(() => resolve(undefined));
          });
        }
      }
      const tracked = trackSyncRequests(
        BaseClient as unknown as MatrixSdkModule["MatrixClient"],
        (ClientClass) => new ClientClass("https://matrix.example.test", "token"),
      );
      const processing = (
        tracked.client as unknown as { processSync: () => Promise<unknown> }
      ).processSync();

      const idleBeforeSettling = yield* Effect.race(
        Effect.as(tracked.awaitSyncIdle, true),
        Effect.as(Effect.yieldNow, false),
      );
      assert.isFalse(idleBeforeSettling);

      for (const settle of settlers.splice(0)) settle();
      yield* Effect.promise(() => processing);
      yield* tracked.awaitSyncIdle;
    }),
  );

  it.effect("keeps buffering room text between connections on one sync loop", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk();
      const received = yield* Queue.unbounded<MatrixBridgeInboundText>();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk, (event) =>
        event.kind === "text" ? Queue.offer(received, event).pipe(Effect.asVoid) : Effect.void,
      );
      yield* awaitStatusState(configService, "waiting-for-member");

      // An allowlist edit reuses the running sync loop, and a batch can land
      // while one connection has ended and the next has not started.
      yield* configService.configure({
        homeserverUrl: HOMESERVER_URL,
        accessToken: "matrix-secret-token",
        allowedUserIds: [ALLOWED_USER_ID, "@second:beeper.com"],
      });
      sdk.deliverSyncBatch({
        cursor: "handover-batch",
        roomKeys: ["megolm-session-1"],
        timeline: [
          {
            sessionKey: "megolm-session-1",
            event: decryptedTextEvent({
              eventId: "$during-handover",
              sender: ALLOWED_USER_ID,
              body: "sent while the bridge was reconfiguring",
            }),
          },
        ],
      });
      yield* awaitStatusState(configService, "waiting-for-member");

      assert.lengthOf(sdk.clients, 1);
      assert.equal((yield* Queue.take(received)).eventId, "$during-handover");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("hands the stores over only after the retired sync has landed", () =>
    Effect.gen(function* () {
      // The retired client keeps a long poll open across the handover, exactly
      // as matrix-bot-sdk does when `stop()` sets its flag mid-request.
      const sdk = makeFakeSdk({ pendingSync: true, deviceId: "T3DEVICE-1" });
      const received = yield* Queue.unbounded<MatrixBridgeInboundText>();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk, (event) =>
        event.kind === "text" ? Queue.offer(received, event).pipe(Effect.asVoid) : Effect.void,
      );
      yield* awaitStatusState(configService, "waiting-for-member");
      assert.equal(sdk.pendingSyncCount(), 1);
      const retiredStorePath = sdk.syncStorePaths[0] ?? "";

      // A new bot login is a new device, so the replacement needs its own
      // stores and the old client must be finished with the old ones.
      sdk.setDeviceId("T3DEVICE-2");
      yield* configService.configure({
        homeserverUrl: HOMESERVER_URL,
        accessToken: "rotated-token",
        allowedUserIds: [ALLOWED_USER_ID],
      });
      // Stopping the old client is the step right after retiring its fence.
      yield* awaitCondition(() => sdk.stoppedClients().includes(0));

      // The response `stop()` could not cancel now lands. It must not write the
      // cursor, and its timeline must not reach the bridge.
      const cursorBeforeLateBatch = sdk.storedSyncToken(retiredStorePath);
      sdk.deliverSyncBatch({
        cursor: "late-cursor",
        clientIndex: 0,
        roomKeys: ["megolm-session-late"],
        timeline: [
          {
            sessionKey: "megolm-session-late",
            event: decryptedTextEvent({
              eventId: "$late",
              sender: ALLOWED_USER_ID,
              body: "arrived after the handover",
            }),
          },
        ],
      });
      assert.equal(sdk.storedSyncToken(retiredStorePath), cursorBeforeLateBatch);
      assert.equal(yield* Queue.size(received), 0);

      // The replacement is a different device, so it holds different stores and
      // the retired request can land whenever it likes.
      sdk.resolvePendingSyncs();
      yield* awaitCondition(() => sdk.clients.length === 2);
      yield* awaitStatusState(configService, "waiting-for-member");
      assert.lengthOf(new Set(sdk.syncStorePaths), 2);
      assert.notEqual(sdk.syncStorePaths[1], retiredStorePath);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("moves a generation-named store to the device path on upgrade", () =>
    Effect.gen(function* () {
      const configService = yield* configureBridge();
      const generation = Option.getOrThrow(
        yield* configService.currentConfig,
      ).cryptoStoreGeneration;
      const serverConfig = yield* ServerConfig.ServerConfig;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;

      // The layout an earlier build of this branch left behind.
      const bridgeDir = path.join(serverConfig.secretsDir, "matrix-bridge");
      const legacyDir = path.join(bridgeDir, generation);
      yield* fs.makeDirectory(legacyDir, { recursive: true });
      yield* fs.writeFileString(path.join(legacyDir, "sync.json"), '{"syncToken":"legacy"}');

      const sdk = makeFakeSdk();
      yield* startAdapter(sdk);
      yield* Deferred.await(sdk.started);

      const storeDir = path.dirname(sdk.syncStorePaths[0] ?? "");
      assert.notEqual(storeDir, legacyDir);
      assert.isFalse(yield* fs.exists(legacyDir));
      // The device keeps its one store, contents and all.
      assert.equal(
        yield* fs.readFileString(path.join(storeDir, "sync.json")),
        '{"syncToken":"legacy"}',
      );
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("verifies an existing room instead of creating another", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk();
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* Deferred.await(sdk.started);

      assert.lengthOf(sdk.createdRooms, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a room that is not invite-only", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        roomState: {
          "m.room.join_rules": { join_rule: "public" },
          "m.room.encryption": { algorithm: MEGOLM_ALGORITHM },
          "m.room.history_visibility": { history_visibility: "invited" },
          "m.room.power_levels": botAdministeredPowerLevels(),
        },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      const status = yield* configService.status;
      assert.isFalse(status.encryptionReady);
      assert.equal(status.reason, "The configured Matrix room is not invite-only.");
      assert.lengthOf(sdk.startedFilters, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a room whose members can invite other accounts", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        roomState: {
          "m.room.join_rules": { join_rule: "invite" },
          "m.room.encryption": { algorithm: MEGOLM_ALGORITHM },
          "m.room.history_visibility": { history_visibility: "invited" },
          // What `trusted_private_chat` produces: the member matches the bot.
          "m.room.power_levels": {
            users_default: 0,
            invite: 100,
            users: { [BOT_USER_ID]: 100, [ALLOWED_USER_ID]: 100 },
          },
        },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      const status = yield* configService.status;
      assert.include(status.reason ?? "", "invite other accounts");
      assert.lengthOf(sdk.startedFilters, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("accepts a version 12 room the bot created without listing itself", () =>
    Effect.gen(function* () {
      // Version 12 forbids listing creators in the power levels, because they
      // hold unbounded power in their own right.
      const sdk = makeFakeSdk({
        roomCreateContent: { room_version: "12" },
        roomState: {
          "m.room.join_rules": { join_rule: "invite" },
          "m.room.encryption": { algorithm: MEGOLM_ALGORITHM },
          "m.room.history_visibility": { history_visibility: "invited" },
          "m.room.power_levels": {
            users_default: 0,
            invite: 100,
            state_default: 100,
            redact: 100,
            events: { "m.room.redaction": 100 },
          },
        },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      assert.isTrue((yield* configService.status).encryptionReady);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a version 12 room whose create event only claims the bot", () =>
    Effect.gen(function* () {
      // From version 11 the sender is the only authority, so a `creator` field
      // naming the bot cannot launder someone else's room.
      const sdk = makeFakeSdk({
        roomCreator: "@someone-else:matrix.example.test",
        roomCreateContent: { room_version: "12", creator: BOT_USER_ID },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      assert.include((yield* configService.status).reason ?? "", "created by another account");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a room whose version it does not verify", () =>
    Effect.gen(function* () {
      // Version identifiers are opaque: a homeserver dialect, a version whose
      // rules predate this check, and a future number are all refused.
      const sdk = makeFakeSdk({ roomCreateContent: { room_version: "org.example.unstable" } });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      assert.include((yield* configService.status).reason ?? "", "does not verify");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a room version newer than the rules it implements", () =>
    Effect.gen(function* () {
      // Version identifiers are opaque strings with no ordering, so a higher
      // number is not a safe bet either.
      const sdk = makeFakeSdk({ roomCreateContent: { room_version: "13" } });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      assert.include((yield* configService.status).reason ?? "", "does not verify");
      assert.lengthOf(sdk.startedFilters, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a room another account created", () =>
    Effect.gen(function* () {
      // Room version 12 gives creators unbounded power whatever the power
      // levels say, so a room the bot did not create can never be trusted.
      const sdk = makeFakeSdk({ roomCreator: "@someone-else:matrix.example.test" });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      assert.include((yield* configService.status).reason ?? "", "created by another account");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a room with an additional creator", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        roomCreateContent: { additional_creators: ["@someone-else:matrix.example.test"] },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      assert.include((yield* configService.status).reason ?? "", "created by another account");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("reads power levels written as integer strings", () =>
    Effect.gen(function* () {
      // Older stable room versions allow these, and treating them as absent
      // would silently fall back to the permissive Matrix defaults.
      const sdk = makeFakeSdk({
        roomState: {
          "m.room.join_rules": { join_rule: "invite" },
          "m.room.encryption": { algorithm: MEGOLM_ALGORITHM },
          "m.room.history_visibility": { history_visibility: "invited" },
          "m.room.power_levels": {
            users_default: "0",
            invite: "100",
            state_default: "0",
            users: { [BOT_USER_ID]: "100" },
          },
        },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      assert.include((yield* configService.status).reason ?? "", "change its access rules");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a room joined by someone outside the allowed list", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        roomMembership: { [ALLOWED_USER_ID]: "join", "@stranger:matrix.example.test": "join" },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      assert.include((yield* configService.status).reason ?? "", "outside the allowed list");
      assert.lengthOf(sdk.startedFilters, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a room with a pending invitation outside the allowed list", () =>
    Effect.gen(function* () {
      // An invitation left over from an earlier allowlist can still be
      // accepted, so it is as unsafe as a joined stranger.
      const sdk = makeFakeSdk({
        roomMembership: { [ALLOWED_USER_ID]: "join", "@removed:matrix.example.test": "invite" },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      assert.include((yield* configService.status).reason ?? "", "pending invitation");
      assert.lengthOf(sdk.startedFilters, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a room with world-readable history", () =>
    Effect.gen(function* () {
      // `world_readable` serves history to accounts that never joined at all.
      const sdk = makeFakeSdk({
        roomState: {
          "m.room.join_rules": { join_rule: "invite" },
          "m.room.encryption": { algorithm: MEGOLM_ALGORITHM },
          "m.room.history_visibility": { history_visibility: "world_readable" },
          "m.room.power_levels": botAdministeredPowerLevels(),
        },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      assert.include((yield* configService.status).reason ?? "", "shares its history");
      assert.lengthOf(sdk.startedFilters, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a room that shares history from before a member arrived", () =>
    Effect.gen(function* () {
      // `shared` hands everything said before an invitation to whoever is
      // invited next, which is the allowlist boundary this bridge holds.
      const sdk = makeFakeSdk({
        roomState: {
          "m.room.join_rules": { join_rule: "invite" },
          "m.room.encryption": { algorithm: MEGOLM_ALGORITHM },
          "m.room.history_visibility": { history_visibility: "shared" },
          "m.room.power_levels": botAdministeredPowerLevels(),
        },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      assert.include((yield* configService.status).reason ?? "", "shares its history");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("accepts a room whose history starts at the member's arrival", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        roomState: {
          "m.room.join_rules": { join_rule: "invite" },
          "m.room.encryption": { algorithm: MEGOLM_ALGORITHM },
          "m.room.history_visibility": { history_visibility: "joined" },
          "m.room.power_levels": botAdministeredPowerLevels(),
        },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      assert.isTrue((yield* configService.status).encryptionReady);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a room whose members can change its history visibility", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        roomState: {
          "m.room.join_rules": { join_rule: "invite" },
          "m.room.encryption": { algorithm: MEGOLM_ALGORITHM },
          "m.room.history_visibility": { history_visibility: "invited" },
          // Private today, but any member could publish the whole history.
          "m.room.power_levels": {
            ...botAdministeredPowerLevels(),
            events: { "m.room.redaction": 100, "m.room.history_visibility": 0 },
          },
        },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      assert.include((yield* configService.status).reason ?? "", "change its access rules");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a room whose members can send redactions", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        roomState: {
          "m.room.join_rules": { join_rule: "invite" },
          "m.room.encryption": { algorithm: MEGOLM_ALGORITHM },
          "m.room.history_visibility": { history_visibility: "invited" },
          // Everything is locked down except redaction, which would let a
          // member erase the bridge's own output after the fact.
          "m.room.power_levels": {
            users_default: 0,
            invite: 100,
            state_default: 100,
            redact: 100,
            // Redaction is authorised by the sender's domain as well as by the
            // redact level, so the send level has to be out of reach too.
            events_default: 0,
            users: { [BOT_USER_ID]: 100 },
          },
        },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      assert.include((yield* configService.status).reason ?? "", "change its access rules");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a room whose members can change its access rules", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        roomState: {
          "m.room.join_rules": { join_rule: "invite" },
          "m.room.encryption": { algorithm: MEGOLM_ALGORITHM },
          "m.room.history_visibility": { history_visibility: "invited" },
          // Only the bot can invite, but anyone may publish the room by
          // rewriting its join rules.
          "m.room.power_levels": {
            users_default: 0,
            invite: 100,
            state_default: 0,
            users: { [BOT_USER_ID]: 100 },
          },
        },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      assert.include((yield* configService.status).reason ?? "", "change its access rules");
      assert.lengthOf(sdk.startedFilters, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("re-invites an allowed user who left the bridged room", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ roomMembership: { [ALLOWED_USER_ID]: "leave" } });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      assert.deepEqual([...sdk.invitations], [{ userId: ALLOWED_USER_ID, roomId: ROOM_ID }]);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("leaves a joined allowed user alone", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ roomMembership: { [ALLOWED_USER_ID]: "join" } });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      assert.lengthOf(sdk.invitations, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a plaintext room and never sends into it", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        roomState: {
          "m.room.join_rules": { join_rule: "invite" },
          "m.room.history_visibility": { history_visibility: "invited" },
          "m.room.power_levels": botAdministeredPowerLevels(),
        },
      });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });
      const client = yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "unavailable");

      const status = yield* configService.status;
      assert.include(status.reason ?? "", "not end-to-end encrypted");
      assert.lengthOf(sdk.startedFilters, 0);

      const result = yield* Effect.result(
        client.sendText({
          roomId: ROOM_ID,
          transactionId: "t3-transaction",
          content: { msgtype: "m.text", body: "should never leave" },
        }),
      );
      assert.equal(result._tag, "Failure");
      assert.lengthOf(sdk.encryptions, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("adopts the created room when persisting it fails", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk();
      const configService = yield* configureBridge();
      let persistFailures = 1;
      const flakyPersistence = MatrixBridgeConfig.of({
        ...configService,
        recordRoomIfMatches: (expected) => {
          if (persistFailures > 0) {
            persistFailures -= 1;
            return Effect.fail(
              new MatrixBridgeOperationError({
                reason: "persistenceFailed",
                message: "Matrix bridge configuration could not be persisted.",
              }),
            );
          }
          return configService.recordRoomIfMatches(expected);
        },
      });

      const client = yield* make(sdk.load).pipe(
        Effect.provideService(MatrixBridgeConfig, flakyPersistence),
      );
      yield* Effect.forkScoped(client.listen(() => Effect.void));
      yield* awaitStatusState(configService, "unavailable");

      // Drive the reconnect backoff; the retry must reuse the created room
      // instead of creating a second one and re-inviting everybody.
      const ticker = yield* Effect.forkScoped(
        TestClock.adjust("1 second").pipe(Effect.andThen(Effect.yieldNow), Effect.forever),
      );
      yield* awaitStatusState(configService, "waiting-for-member");
      yield* Fiber.interrupt(ticker);

      assert.lengthOf(sdk.createdRooms, 1);
      assert.equal(Option.getOrThrow(yield* configService.currentConfig).roomId, ROOM_ID);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("registers the sync filter before the sync loop can clear the cursor", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk();
      yield* configureBridge();
      yield* startAdapter(sdk);
      yield* Deferred.await(sdk.started);

      const filterRequest = sdk.requests.find((request) => request.endpoint.endsWith("/filter"));
      assert.isDefined(filterRequest);
      assert.equal(filterRequest?.method, "POST");
      assert.deepEqual(filterRequest?.body, buildSyncFilter(ROOM_ID));
      // A filter the SDK would have to create is what wipes a stored cursor;
      // creating the identical filter first makes that branch a no-op.
      assert.deepEqual([...sdk.startedFilters], [buildSyncFilter(ROOM_ID)]);
      assert.deepEqual([...sdk.storageWrites], [null]);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("fences catch-up history while its room keys still reach encryption", () =>
    Effect.gen(function* () {
      // The catch-up batch carries a room key and old history at once.
      const sdk = makeFakeSdk({
        catchUp: {
          roomKeys: ["megolm-session-1"],
          timeline: [
            {
              sessionKey: "megolm-session-1",
              event: decryptedTextEvent({
                eventId: "$history",
                sender: ALLOWED_USER_ID,
                body: "sent before the bridge existed",
              }),
            },
          ],
        },
      });
      const received = yield* Queue.unbounded<MatrixBridgeInboundText>();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk, (event) =>
        event.kind === "text" ? Queue.offer(received, event).pipe(Effect.asVoid) : Effect.void,
      );
      yield* awaitStatusState(configService, "waiting-for-member");
      assert.equal(yield* Queue.size(received), 0);

      // A later event encrypted with the same session decrypts, which is only
      // possible because the fenced batch was processed rather than discarded.
      sdk.deliverSyncBatch({
        cursor: "live-batch",
        timeline: [
          {
            sessionKey: "megolm-session-1",
            event: decryptedTextEvent({
              eventId: "$live",
              sender: ALLOWED_USER_ID,
              body: "sent after the bridge started",
            }),
          },
        ],
      });

      const delivered = yield* Queue.take(received);
      assert.equal(delivered.eventId, "$live");
      assert.equal(delivered.body, "sent after the bridge started");
      assert.equal(yield* Queue.size(received), 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("re-arms the fence when a changed filter clears the stored cursor", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        syncToken: "stored-cursor",
        // A filter from an older release: registering the new one wipes the
        // cursor, so the sync that follows is catch-up, not live traffic.
        storedFilter: { id: "stale-filter", filter: { room: { timeline: { limit: 1 } } } },
        catchUp: {
          roomKeys: ["megolm-session-1"],
          timeline: [
            {
              sessionKey: "megolm-session-1",
              event: decryptedTextEvent({
                eventId: "$history",
                sender: ALLOWED_USER_ID,
                body: "replayed history",
              }),
            },
          ],
        },
      });
      const received = yield* Queue.unbounded<MatrixBridgeInboundText>();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk, (event) =>
        event.kind === "text" ? Queue.offer(received, event).pipe(Effect.asVoid) : Effect.void,
      );
      yield* awaitStatusState(configService, "waiting-for-member");

      assert.deepEqual([...sdk.storageWrites], [null, "catch-up-1"]);
      assert.equal(yield* Queue.size(received), 0);

      sdk.deliverSyncBatch({
        cursor: "live-batch",
        timeline: [
          {
            sessionKey: "megolm-session-1",
            event: decryptedTextEvent({
              eventId: "$live",
              sender: ALLOWED_USER_ID,
              body: "live traffic",
            }),
          },
        ],
      });
      assert.equal((yield* Queue.take(received)).eventId, "$live");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("delivers the first batch after a restart with a stored cursor", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        syncToken: "stored-cursor",
        storedFilter: { id: "stored-filter", filter: buildSyncFilter(ROOM_ID) },
        // A cursor means the first batch is new traffic, not replayed history.
        catchUp: {
          roomKeys: ["megolm-session-1"],
          timeline: [
            {
              sessionKey: "megolm-session-1",
              event: decryptedTextEvent({
                eventId: "$after-restart",
                sender: ALLOWED_USER_ID,
                body: "sent while the server was down",
              }),
            },
          ],
        },
      });
      const received = yield* Queue.unbounded<MatrixBridgeInboundText>();
      yield* configureBridge();
      yield* startAdapter(sdk, (event) =>
        event.kind === "text" ? Queue.offer(received, event).pipe(Effect.asVoid) : Effect.void,
      );
      yield* Deferred.await(sdk.started);

      assert.equal((yield* Queue.take(received)).eventId, "$after-restart");
      // Nothing was refenced, so the filter survives untouched.
      assert.isUndefined(sdk.requests.find((request) => request.endpoint.endsWith("/filter")));
      assert.deepEqual([...sdk.startedAfterStoredToken], ["stored-cursor"]);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("encrypts once and retries one transaction id", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID],
        failSendsWith: [httpError(502)],
      });
      const configService = yield* configureBridge();
      const client = yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      const message = {
        roomId: ROOM_ID,
        transactionId: "t3-environment-thread-turn",
        content: { msgtype: "m.text", body: "final answer" },
      } as const;
      const firstAttempt = yield* Effect.result(client.sendText(message));
      assert.equal(firstAttempt._tag, "Failure");
      if (firstAttempt._tag === "Failure") {
        assert.equal(firstAttempt.failure.retryability, "transient");
      }
      yield* client.sendText(message);

      const sends = sdk.requests.filter((request) => request.method === "PUT");
      assert.lengthOf(sends, 2);
      assert.lengthOf(sdk.encryptions, 1);
      const expectedEndpoint = `/_matrix/client/v3/rooms/${encodeURIComponent(ROOM_ID)}/send/m.room.encrypted/${encodeURIComponent(message.transactionId)}`;
      assert.deepEqual(
        sends.map((send) => send.endpoint),
        [expectedEndpoint, expectedEndpoint],
      );
      assert.strictEqual(sends[0]?.body, sends[1]?.body);
      assert.strictEqual(sends[0]?.body, sdk.encryptions[0]);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses to send after the connection has been replaced", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk();
      const configService = yield* configureBridge();
      const client = yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      // An allowlist edit mints a new connection identity while a send is in
      // flight; the captured connection must not deliver into the old room.
      yield* configService.configure({
        homeserverUrl: HOMESERVER_URL,
        accessToken: "matrix-secret-token",
        allowedUserIds: [ALLOWED_USER_ID, "@second:beeper.com"],
      });

      const result = yield* Effect.result(
        client.sendText({
          roomId: ROOM_ID,
          transactionId: "t3-stale-connection",
          content: { msgtype: "m.text", body: "must not be delivered" },
        }),
      );
      // Refused either as a retired connection or as no connection at all,
      // depending on how far the teardown has run; what matters is that
      // nothing reached the room the operator just replaced.
      assert.equal(result._tag, "Failure");
      assert.lengthOf(
        sdk.requests.filter((request) => request.method === "PUT"),
        0,
      );
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("answers a Connect that lands while a failure is being reported", () =>
    Effect.gen(function* () {
      // The room is refused at first, and the operator fixes it and presses
      // Connect in the window between the failure and its status being
      // published. That intent is newer than the failure, so it must win
      // rather than be buried under an unavailable status.
      const roomState: Record<string, unknown> = {
        "m.room.join_rules": { join_rule: "public" },
        "m.room.encryption": { algorithm: MEGOLM_ALGORITHM },
        "m.room.history_visibility": { history_visibility: "invited" },
        "m.room.power_levels": botAdministeredPowerLevels(),
      };
      const sdk = makeFakeSdk({ roomState });
      const configService = yield* configureBridge();
      yield* configService.recordRoomIfMatches({
        cryptoStoreGeneration: Option.getOrThrow(yield* configService.currentConfig)
          .cryptoStoreGeneration,
        roomId: ROOM_ID,
      });

      let racedOnce = false;
      const racingConfig = MatrixBridgeConfig.of({
        ...configService,
        reportTransportStateIfMatches: (expected) =>
          Effect.gen(function* () {
            if (!racedOnce && expected.transport.state === "unavailable") {
              racedOnce = true;
              roomState["m.room.join_rules"] = { join_rule: "invite" };
              // Connect, published before the failure state is written.
              yield* configService
                .configure({
                  homeserverUrl: HOMESERVER_URL,
                  accessToken: "matrix-secret-token",
                  allowedUserIds: [ALLOWED_USER_ID],
                })
                .pipe(Effect.orDie);
            }
            return yield* configService.reportTransportStateIfMatches(expected);
          }),
      });

      const client = yield* make(sdk.load).pipe(
        Effect.provideService(MatrixBridgeConfig, racingConfig),
      );
      yield* Effect.forkScoped(client.listen(() => Effect.void));

      yield* awaitStatusState(configService, "waiting-for-member");
      assert.isTrue(racedOnce);
      assert.isTrue((yield* configService.status).encryptionReady);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("restores readiness when identical settings are resubmitted", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      // Connect with unchanged settings keeps this connection alive, so the
      // status it publishes has to come back rather than stick on connecting.
      yield* configService.configure({
        homeserverUrl: HOMESERVER_URL,
        accessToken: "matrix-secret-token",
        allowedUserIds: [ALLOWED_USER_ID],
      });
      yield* awaitStatusState(configService, "waiting-for-member");

      const status = yield* configService.status;
      assert.equal(status.state, "waiting-for-member");
      assert.isTrue(status.encryptionReady);
      assert.lengthOf(sdk.startedFilters, 1);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("publishes joined membership on connect and on every change", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID] });
      const received = yield* Queue.unbounded<MatrixBridgeRoomMembership>();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk, (event) =>
        event.kind === "membership"
          ? Queue.offer(received, event).pipe(Effect.asVoid)
          : Effect.void,
      );
      yield* awaitStatusState(configService, "waiting-for-member");

      const connected = yield* Queue.take(received);
      assert.deepEqual([...connected.joined], [ALLOWED_USER_ID, BOT_USER_ID]);
      assert.equal(connected.botUserId, BOT_USER_ID);
      assert.equal(connected.roomId, ROOM_ID);

      // A membership event from another room, and a repeat of a state already
      // held, must not republish.
      sdk.emitTimeline(
        "!other:matrix.example.test",
        membershipEvent({ userId: "@elsewhere:example.test", membership: "join" }),
      );
      sdk.emitTimeline(ROOM_ID, membershipEvent({ userId: ALLOWED_USER_ID, membership: "join" }));
      sdk.emitTimeline(
        ROOM_ID,
        membershipEvent({ userId: "@stranger:example.test", membership: "join" }),
      );
      const joined = yield* Queue.take(received);
      assert.deepEqual(
        [...joined.joined],
        [ALLOWED_USER_ID, "@stranger:example.test", BOT_USER_ID],
      );

      sdk.emitTimeline(
        ROOM_ID,
        membershipEvent({ userId: "@stranger:example.test", membership: "leave" }),
      );
      const left = yield* Queue.take(received);
      assert.deepEqual([...left.joined], [ALLOWED_USER_ID, BOT_USER_ID]);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("applies this connection's membership before the transport is usable", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID] });
      const configService = yield* configureBridge();
      const observed = yield* Ref.make<ReadonlyArray<string>>([]);
      yield* startAdapter(sdk, (event) =>
        event.kind === "membership"
          ? configService.status.pipe(
              Effect.flatMap((status) =>
                Ref.update(observed, (states) => [...states, status.state]),
              ),
            )
          : Effect.void,
      );
      yield* awaitStatusState(configService, "waiting-for-member");

      // The bridge learns who is in the room while the transport is still
      // connecting, so a reconnect can never deliver under the old membership.
      assert.deepEqual([...(yield* Ref.get(observed))], ["connecting"]);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses to encrypt for a room an outside account has joined", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID] });
      const configService = yield* configureBridge();
      const client = yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      const message = {
        roomId: ROOM_ID,
        transactionId: "t3-guarded-send",
        content: { msgtype: "m.text", body: "private final" },
      } as const;
      yield* client.sendText(message);
      assert.lengthOf(sdk.encryptions, 1);

      // The join is applied by the same synchronous handler the SDK calls, so
      // the very next send is refused without encrypting for the newcomer.
      sdk.emitTimeline(
        ROOM_ID,
        membershipEvent({ userId: "@stranger:example.test", membership: "join" }),
      );
      const blocked = yield* Effect.result(
        client.sendText({ ...message, transactionId: "t3-guarded-send-2" }),
      );
      assert.equal(blocked._tag, "Failure");
      if (blocked._tag === "Failure") {
        // Transient: the room can become safe again inside the retry window.
        assert.equal(blocked.failure.retryability, "transient");
        assert.include(blocked.failure.reason, "outside the allowed list");
      }
      assert.lengthOf(sdk.encryptions, 1);

      sdk.emitTimeline(
        ROOM_ID,
        membershipEvent({ userId: "@stranger:example.test", membership: "leave" }),
      );
      yield* client.sendText({ ...message, transactionId: "t3-guarded-send-3" });
      assert.lengthOf(sdk.encryptions, 2);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a send when only the homeserver knows about a joined account", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID] });
      const configService = yield* configureBridge();
      const client = yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      // A gappy sync reports membership in the state block, which the SDK never
      // emits as a room event, so the room can be unsafe with no local signal.
      sdk.joinSilently("@unseen:example.test");
      const blocked = yield* Effect.result(
        client.sendText({
          roomId: ROOM_ID,
          transactionId: "t3-unseen-member",
          content: { msgtype: "m.text", body: "private final" },
        }),
      );

      assert.equal(blocked._tag, "Failure");
      if (blocked._tag === "Failure") {
        assert.include(blocked.failure.reason, "outside the allowed list");
      }
      assert.lengthOf(sdk.encryptions, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses a send into a room the allowed reader has left", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID] });
      const configService = yield* configureBridge();
      const client = yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      sdk.leaveSilently(ALLOWED_USER_ID);
      const blocked = yield* Effect.result(
        client.sendText({
          roomId: ROOM_ID,
          transactionId: "t3-no-reader",
          content: { msgtype: "m.text", body: "private final" },
        }),
      );

      assert.equal(blocked._tag, "Failure");
      if (blocked._tag === "Failure") {
        assert.include(blocked.failure.reason, "No account from the allowed list");
        // Transient: the reader can come back inside the retry window.
        assert.equal(blocked.failure.retryability, "transient");
      }
      assert.lengthOf(sdk.encryptions, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("re-reads membership rather than merging a snapshot that changed under it", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID] });
      const configService = yield* configureBridge();
      const client = yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      // The reader leaves while the first read is in flight, so its answer
      // still lists them; merging it would resurrect a member who is gone.
      sdk.onMembersRead(() => {
        sdk.emitTimeline(
          ROOM_ID,
          membershipEvent({ userId: ALLOWED_USER_ID, membership: "leave" }),
        );
      });
      const blocked = yield* Effect.result(
        client.sendText({
          roomId: ROOM_ID,
          transactionId: "t3-racing-leave",
          content: { msgtype: "m.text", body: "private final" },
        }),
      );

      assert.equal(blocked._tag, "Failure");
      if (blocked._tag === "Failure") {
        assert.include(blocked.failure.reason, "No account from the allowed list");
      }
      assert.lengthOf(sdk.encryptions, 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("discards ciphertext when the reader changes during encryption", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID] });
      const configService = yield* configureBridge();
      const client = yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      // The reader leaves and rejoins while the message is being encrypted:
      // still only allowed accounts, but not the devices it was built for.
      sdk.onEncrypt(() => {
        sdk.emitTimeline(
          ROOM_ID,
          membershipEvent({ userId: ALLOWED_USER_ID, membership: "leave" }),
        );
        sdk.emitTimeline(ROOM_ID, membershipEvent({ userId: ALLOWED_USER_ID, membership: "join" }));
      });
      const raced = yield* Effect.result(
        client.sendText({
          roomId: ROOM_ID,
          transactionId: "t3-reader-swap",
          content: { msgtype: "m.text", body: "private final" },
        }),
      );

      assert.equal(raced._tag, "Failure");
      if (raced._tag === "Failure") {
        assert.include(raced.failure.reason, "changed while the message was being encrypted");
      }
      assert.lengthOf(
        sdk.requests.filter((request) => request.endpoint.includes("t3-reader-swap")),
        0,
      );

      // The retry encrypts again rather than reusing the retired ciphertext.
      yield* client.sendText({
        roomId: ROOM_ID,
        transactionId: "t3-reader-swap",
        content: { msgtype: "m.text", body: "private final" },
      });
      assert.lengthOf(sdk.encryptions, 2);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("keeps ciphertext for a transaction whose send may already have landed", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID],
        failSendsWith: [httpError(502)],
      });
      const configService = yield* configureBridge();
      const client = yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      const message = {
        roomId: ROOM_ID,
        transactionId: "t3-membership-retry",
        content: { msgtype: "m.text", body: "final answer" },
      } as const;
      const firstAttempt = yield* Effect.result(client.sendText(message));
      assert.equal(firstAttempt._tag, "Failure");
      assert.lengthOf(sdk.encryptions, 1);

      // The reader left and came back while the bridge held the retry. The
      // first PUT may still have reached the homeserver, and a transaction id
      // is an idempotency key: re-encrypting under it would claim to replace an
      // event the homeserver will happily return unchanged.
      sdk.emitTimeline(ROOM_ID, membershipEvent({ userId: ALLOWED_USER_ID, membership: "leave" }));
      sdk.emitTimeline(ROOM_ID, membershipEvent({ userId: ALLOWED_USER_ID, membership: "join" }));
      yield* client.sendText(message);

      assert.lengthOf(sdk.encryptions, 1);
      const sends = sdk.requests.filter((request) => request.method === "PUT");
      assert.lengthOf(sends, 2);
      assert.equal(new Set(sends.map((send) => send.endpoint)).size, 1);
      assert.strictEqual(sends[0]?.body, sends[1]?.body);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("marks inbound text unsafe when only the homeserver knows about a joiner", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID] });
      const received = yield* Queue.unbounded<MatrixBridgeInboundText>();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk, (event) =>
        event.kind === "text" ? Queue.offer(received, event).pipe(Effect.asVoid) : Effect.void,
      );
      yield* awaitStatusState(configService, "waiting-for-member");

      sdk.joinSilently("@unseen:example.test");
      sdk.deliverSyncBatch({
        cursor: "unsafe-batch",
        timeline: [
          {
            event: decryptedTextEvent({ eventId: "$unsafe", sender: ALLOWED_USER_ID, body: "hi" }),
          },
        ],
      });
      const unsafe = yield* Queue.take(received);
      assert.isFalse(unsafe.roomAllowedOnly);

      sdk.emitTimeline(
        ROOM_ID,
        membershipEvent({ userId: "@unseen:example.test", membership: "leave" }),
      );
      sdk.deliverSyncBatch({
        cursor: "safe-batch",
        timeline: [
          { event: decryptedTextEvent({ eventId: "$safe", sender: ALLOWED_USER_ID, body: "hi" }) },
        ],
      });
      const safe = yield* Queue.take(received);
      assert.isTrue(safe.roomAllowedOnly);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("reconciles a membership change the timeline never reported", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID] });
      const memberships = yield* Queue.unbounded<MatrixBridgeRoomMembership>();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk, (event) =>
        event.kind === "membership"
          ? Queue.offer(memberships, event).pipe(Effect.asVoid)
          : Effect.void,
      );
      yield* awaitStatusState(configService, "waiting-for-member");
      const connected = yield* Queue.take(memberships);
      assert.deepEqual([...connected.joined], [BOT_USER_ID]);

      // The reader joined during a sync gap, so no room event ever arrives and
      // nothing else would prompt them.
      sdk.joinSilently(ALLOWED_USER_ID);
      const ticker = yield* Effect.forkChild(
        Effect.forever(TestClock.adjust("60 seconds").pipe(Effect.andThen(Effect.yieldNow))),
      );
      const reconciled = yield* Queue.take(memberships);
      yield* Fiber.interrupt(ticker);

      assert.deepEqual([...reconciled.joined], [ALLOWED_USER_ID, BOT_USER_ID]);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("retries a failed membership read rather than dropping the message", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID] });
      const received = yield* Queue.unbounded<MatrixBridgeInboundText>();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk, (event) =>
        event.kind === "text" ? Queue.offer(received, event).pipe(Effect.asVoid) : Effect.void,
      );
      yield* awaitStatusState(configService, "waiting-for-member");

      // A blip while verifying must not turn a valid command into an unsafe
      // hand-off: Matrix will not deliver that timeline event again.
      sdk.failNextMemberReads(1);
      sdk.deliverSyncBatch({
        cursor: "retry-batch",
        timeline: [
          {
            event: decryptedTextEvent({ eventId: "$retried", sender: ALLOWED_USER_ID, body: "hi" }),
          },
        ],
      });

      // The retry waits on a backoff, so the test clock has to move for it.
      const ticker = yield* Effect.forkChild(
        Effect.forever(TestClock.adjust("1 second").pipe(Effect.andThen(Effect.yieldNow))),
      );
      const delivered = yield* Queue.take(received);
      yield* Fiber.interrupt(ticker);

      assert.equal(delivered.eventId, "$retried");
      assert.isTrue(delivered.roomAllowedOnly);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("reports a room that never settles instead of quietly dropping text", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID] });
      const received = yield* Queue.unbounded<MatrixBridgeInboundText>();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk, (event) =>
        event.kind === "text" ? Queue.offer(received, event).pipe(Effect.asVoid) : Effect.void,
      );
      yield* awaitStatusState(configService, "waiting-for-member");

      // Every read is invalidated by another change, so the room is never
      // verified rather than verified unsafe.
      let churn = 0;
      sdk.onEveryMembersRead(() => {
        churn += 1;
        sdk.emitTimeline(
          ROOM_ID,
          membershipEvent({ userId: `@churn-${churn}:example.test`, membership: "join" }),
        );
      });
      sdk.deliverSyncBatch({
        cursor: "churn-batch",
        timeline: [
          {
            event: decryptedTextEvent({
              eventId: "$churning",
              sender: ALLOWED_USER_ID,
              body: "hi",
            }),
          },
        ],
      });
      const ticker = yield* Effect.forkChild(
        Effect.forever(TestClock.adjust("1 second").pipe(Effect.andThen(Effect.yieldNow))),
      );
      const delivered = yield* Queue.take(received);
      yield* awaitStatusState(configService, "degraded");
      yield* Fiber.interrupt(ticker);

      assert.isFalse(delivered.roomAllowedOnly);
      assert.include((yield* configService.status).reason ?? "", "could not be verified");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("reports overflow instead of dropping inbound text silently", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID] });
      const blocked = yield* Deferred.make<void>();
      const overflows = yield* Queue.unbounded<MatrixBridgeInboundEvent>();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk, (event) =>
        event.kind === "text"
          ? Deferred.await(blocked)
          : Queue.offer(overflows, event).pipe(Effect.asVoid),
      );
      yield* awaitStatusState(configService, "waiting-for-member");

      // One message occupies the worker; the rest fill the bounded queue.
      sdk.deliverSyncBatch({
        cursor: "burst-batch",
        timeline: Array.from({ length: MATRIX_INBOUND_QUEUE_CAPACITY + 4 }, (_unused, index) => ({
          event: decryptedTextEvent({
            eventId: `$burst-${index}`,
            sender: ALLOWED_USER_ID,
            body: "burst",
          }),
        })),
      });

      const reported = yield* Queue.take(overflows).pipe(
        Effect.repeat({ until: (event) => event.kind === "overflow" }),
      );
      assert.equal(reported.kind, "overflow");
      yield* Deferred.succeed(blocked, undefined);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("holds the sync cursor until an inbound message has been handled", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID] });
      const handling = yield* Deferred.make<void>();
      const received = yield* Queue.unbounded<MatrixBridgeInboundText>();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk, (event) =>
        event.kind === "text"
          ? Queue.offer(received, event).pipe(Effect.andThen(Deferred.await(handling)))
          : Effect.void,
      );
      yield* awaitStatusState(configService, "waiting-for-member");
      const storePath = sdk.syncStorePaths[0] ?? "";
      const beforeBatch = sdk.storedSyncToken(storePath);

      sdk.deliverSyncBatch({
        cursor: "batch-with-work",
        timeline: [
          { event: decryptedTextEvent({ eventId: "$held", sender: ALLOWED_USER_ID, body: "hi" }) },
        ],
      });
      yield* Queue.take(received);

      // A crash here must resume before this message, not after it: the
      // cursor the batch would advance to is still unwritten.
      assert.equal(sdk.storedSyncToken(storePath), beforeBatch);

      yield* Deferred.succeed(handling, undefined);
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      assert.equal(sdk.storedSyncToken(storePath), "batch-with-work");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("releases a discarded message's hold on the sync cursor", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID] });
      const handling = yield* Deferred.make<void>();
      const received = yield* Queue.unbounded<MatrixBridgeInboundText>();
      const configService = yield* configureBridge();
      const client = yield* startAdapter(sdk, (event) =>
        event.kind === "text"
          ? Queue.offer(received, event).pipe(Effect.andThen(Deferred.await(handling)))
          : Effect.void,
      );
      yield* awaitStatusState(configService, "waiting-for-member");
      const storePath = sdk.syncStorePaths[0] ?? "";

      // Two messages in one batch: the first occupies the worker, the second
      // waits in the queue and is then discarded.
      sdk.deliverSyncBatch({
        cursor: "batch-with-discards",
        timeline: [
          { event: decryptedTextEvent({ eventId: "$first", sender: ALLOWED_USER_ID, body: "a" }) },
          { event: decryptedTextEvent({ eventId: "$second", sender: ALLOWED_USER_ID, body: "b" }) },
        ],
      });
      yield* Queue.take(received);
      yield* client.discardPendingInbound;
      yield* Deferred.succeed(handling, undefined);
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      // A discarded message must not hold the cursor for the rest of the
      // connection: nobody is ever going to finish it.
      sdk.deliverSyncBatch({ cursor: "later-batch" });
      yield* Effect.yieldNow;
      assert.equal(sdk.storedSyncToken(storePath), "batch-with-discards");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("refuses ciphertext when an unannounced account joins during encryption", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID, ALLOWED_USER_ID] });
      const configService = yield* configureBridge();
      const client = yield* startAdapter(sdk);
      yield* awaitStatusState(configService, "waiting-for-member");

      // Preparing encryption is itself a joined-member lookup in the SDK, so an
      // account that joined through a sync gap can take this room key with no
      // timeline event and no local revision change to notice it by.
      sdk.onEncrypt(() => {
        sdk.joinSilently("@late:example.test");
      });
      const blocked = yield* Effect.result(
        client.sendText({
          roomId: ROOM_ID,
          transactionId: "t3-gappy-join",
          content: { msgtype: "m.text", body: "private final" },
        }),
      );

      assert.equal(blocked._tag, "Failure");
      if (blocked._tag === "Failure") {
        assert.include(blocked.failure.reason, "outside the allowed list");
        assert.equal(blocked.failure.retryability, "transient");
      }
      assert.lengthOf(
        sdk.requests.filter((request) => request.endpoint.includes("t3-gappy-join")),
        0,
      );
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("sees a join that races the connection's first membership read", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ joinedMembers: [BOT_USER_ID] });
      const memberships = yield* Queue.unbounded<MatrixBridgeRoomMembership>();
      const configService = yield* configureBridge();
      // The reader joins between the snapshot and the listener that would have
      // reported it, which used to leave them unseen until the reconciliation.
      sdk.onMembersRead(() => {
        sdk.emitTimeline(ROOM_ID, membershipEvent({ userId: ALLOWED_USER_ID, membership: "join" }));
      });
      yield* startAdapter(sdk, (event) =>
        event.kind === "membership"
          ? Queue.offer(memberships, event).pipe(Effect.asVoid)
          : Effect.void,
      );
      yield* awaitStatusState(configService, "waiting-for-member");

      const connected = yield* Queue.take(memberships);
      assert.deepEqual([...connected.joined], [ALLOWED_USER_ID, BOT_USER_ID]);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("delivers decrypted room text and ignores the bot's own events", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk();
      const received = yield* Queue.unbounded<MatrixBridgeInboundText>();
      const configService = yield* configureBridge();
      yield* startAdapter(sdk, (event) =>
        event.kind === "text" ? Queue.offer(received, event).pipe(Effect.asVoid) : Effect.void,
      );
      // Readiness means the catch-up batch landed, so this one is live traffic.
      yield* awaitStatusState(configService, "waiting-for-member");
      sdk.deliverSyncBatch({
        cursor: "live-batch",
        timeline: [
          { event: decryptedTextEvent({ eventId: "$bot", sender: BOT_USER_ID, body: "echo" }) },
          {
            roomId: "!other:matrix.example.test",
            event: decryptedTextEvent({
              eventId: "$other-room",
              sender: ALLOWED_USER_ID,
              body: "elsewhere",
            }),
          },
          {
            event: decryptedTextEvent({
              eventId: "$adam",
              sender: ALLOWED_USER_ID,
              body: "hello",
            }),
          },
        ],
      });

      // Only the allowed sender's message in the bridged room is delivered, so
      // the first taken event proves the two ignored ones never queued.
      const delivered = yield* Queue.take(received);
      assert.equal(delivered.eventId, "$adam");
      assert.equal(delivered.body, "hello");
      assert.equal(delivered.sender, ALLOWED_USER_ID);
      assert.isFalse(delivered.isEdit);
      assert.equal(yield* Queue.size(received), 0);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("stays unavailable and healthy when the native module cannot load", () =>
    Effect.gen(function* () {
      const configService = yield* configureBridge();
      const client = yield* make(() =>
        Promise.reject(new Error("Cannot find module 'matrix-sdk-crypto.linux-x64-gnu.node'")),
      );
      yield* Effect.forkScoped(client.listen(() => Effect.void));
      yield* awaitStatusState(configService, "unavailable");

      const status = yield* configService.status;
      assert.isFalse(status.encryptionReady);
      assert.include(status.reason ?? "", "encryption is unavailable");
      // The sanitized reason never carries the loader failure.
      assert.notInclude(status.reason ?? "", ".node");

      const result = yield* Effect.result(
        client.sendText({
          roomId: ROOM_ID,
          transactionId: "t3-transaction",
          content: { msgtype: "m.text", body: "unreachable" },
        }),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure.operation, "send");
      }
    }).pipe(Effect.provide(testLayer)),
  );
});
