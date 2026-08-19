import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import type { MatrixBridgeInboundText } from "./MatrixBridgeClient.ts";
import {
  MEGOLM_ALGORITHM,
  buildSyncFilter,
  make,
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
  readonly syncToken?: string | null;
  readonly storedFilter?: MatrixSdkFilterInfo | null;
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
  users: { [BOT_USER_ID]: 100 },
});

const encryptedRoomState = (): Record<string, unknown> => ({
  "m.room.join_rules": { join_rule: "invite" },
  "m.room.encryption": { algorithm: MEGOLM_ALGORITHM },
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
  readonly emit: (roomId: string, event: unknown) => void;
  readonly started: Deferred.Deferred<void>;
}

const makeFakeSdk = (options: FakeSdkOptions = {}): FakeSdk => {
  const createdRooms: Array<MatrixCreateRoomOptions> = [];
  const requests: Array<RecordedRequest> = [];
  const encryptions: Array<unknown> = [];
  const storageWrites: Array<string | null> = [];
  const startedFilters: Array<unknown> = [];
  const startedAfterStoredToken: Array<string | null> = [];
  const clientOptions: Array<{ readonly homeserverUrl: string; readonly accessToken: string }> = [];
  const syncStorePaths: Array<string> = [];
  const cryptoStores: Array<{ readonly storagePath: string; readonly storeType: number }> = [];
  const handlers = new Map<string, Set<MatrixSdkEventHandler>>();
  const started = Deferred.makeUnsafe<void>();
  const failSends = [...(options.failSendsWith ?? [])];
  const roomState = options.roomState ?? encryptedRoomState();
  let syncToken: string | null = options.syncToken ?? null;
  let storedFilter: MatrixSdkFilterInfo | null = options.storedFilter ?? null;
  let encryptionCounter = 0;

  class FakeStorageProvider implements MatrixSdkStorageProvider {
    constructor(filename: string) {
      syncStorePaths.push(filename);
    }
    getSyncToken(): string | null {
      return syncToken;
    }
    setSyncToken(token: string | null): void {
      syncToken = token;
      storageWrites.push(token);
    }
    getFilter(): MatrixSdkFilterInfo | null {
      return storedFilter;
    }
    setFilter(filter: MatrixSdkFilterInfo): void {
      storedFilter = filter;
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
    readonly crypto = {
      isReady: options.cryptoReady ?? true,
      encryptRoomEvent: (roomId: string, eventType: string, content: unknown) => {
        encryptionCounter += 1;
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
      storage: MatrixSdkStorageProvider,
      cryptoStore: unknown,
    ) {
      this.storage = storage;
      this.cryptoStore = cryptoStore;
      clientOptions.push({ homeserverUrl, accessToken });
    }

    getUserId(): Promise<string> {
      return Promise.resolve(BOT_USER_ID);
    }

    getJoinedRooms(): Promise<ReadonlyArray<string>> {
      return Promise.resolve(options.joinedRooms ?? [ROOM_ID]);
    }

    getRoomStateEvent(_roomId: string, type: string, _stateKey: string): Promise<unknown> {
      const state = roomState[type];
      return state === undefined ? Promise.reject(httpError(404)) : Promise.resolve(state);
    }

    createRoom(createOptions: MatrixCreateRoomOptions): Promise<string> {
      createdRooms.push(createOptions);
      return Promise.resolve(ROOM_ID);
    }

    doRequest(method: string, endpoint: string, qs?: unknown, body?: unknown): Promise<unknown> {
      requests.push({ method, endpoint, qs, body });
      if (endpoint === "/_matrix/client/v3/sync") {
        return Promise.resolve({ next_batch: "fence-cursor" });
      }
      if (endpoint.endsWith("/filter")) {
        return Promise.resolve({ filter_id: "server-filter-id" });
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
        JSON.stringify(storedFilter?.filter) !== JSON.stringify(filter)
      ) {
        syncToken = null;
        storageWrites.push(null);
        storedFilter = { id: "sdk-created-filter", filter };
      }
      startedFilters.push(filter);
      startedAfterStoredToken.push(syncToken);
      Deferred.doneUnsafe(started, Effect.void);
      return Promise.resolve(undefined);
    }

    stop(): void {}

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
    emit: (roomId, event) => {
      for (const handler of handlers.get("room.decrypted_event") ?? []) handler(roomId, event);
    },
    started,
  };
};

/** Decodes the filter exactly as it travels on the wire. */
const decodeFenceFilter = Schema.decodeSync(
  Schema.fromJsonString(
    Schema.Struct({
      room: Schema.Struct({
        rooms: Schema.Array(Schema.String),
        timeline: Schema.Struct({ limit: Schema.Number }),
      }),
    }),
  ),
);

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

/** Starts the adapter's listen loop for the life of the test scope. */
const startAdapter = Effect.fn("startAdapter")(function* (
  sdk: FakeSdk,
  onInbound: (event: MatrixBridgeInboundText) => Effect.Effect<void> = () => Effect.void,
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
      assert.isTrue(created?.is_direct);
      assert.deepEqual([...(created?.invite ?? [])], [ALLOWED_USER_ID]);
      assert.equal(created?.power_level_content_override.invite, 100);
      assert.equal(created?.power_level_content_override.users_default, 0);
      assert.equal(created?.power_level_content_override.events_default, 0);
      assert.equal(created?.power_level_content_override.state_default, 100);
      assert.deepEqual(
        [...(created?.initial_state ?? [])],
        [
          {
            type: "m.room.encryption",
            state_key: "",
            content: { algorithm: MEGOLM_ALGORITHM },
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

  it.effect("keeps sync and crypto stores paired with the connection across restarts", () =>
    Effect.gen(function* () {
      const configService = yield* configureBridge();
      const generation = Option.getOrThrow(
        yield* configService.currentConfig,
      ).cryptoStoreGeneration;
      const serverConfig = yield* ServerConfig.ServerConfig;
      const path = yield* Path.Path;
      const expectedDir = path.join(serverConfig.secretsDir, "matrix-bridge", generation);

      const first = makeFakeSdk();
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* startAdapter(first);
          yield* Deferred.await(first.started);
        }),
      );

      const second = makeFakeSdk({ syncToken: "fence-cursor" });
      yield* startAdapter(second);
      yield* Deferred.await(second.started);

      const expectedSyncStore = path.join(expectedDir, "sync.json");
      const expectedCryptoStore = path.join(expectedDir, "crypto");
      assert.deepEqual([...first.syncStorePaths], [expectedSyncStore]);
      assert.deepEqual([...second.syncStorePaths], [expectedSyncStore]);
      assert.deepEqual(
        [...first.cryptoStores],
        [{ storagePath: expectedCryptoStore, storeType: 0 }],
      );
      assert.deepEqual([...second.cryptoStores], [...first.cryptoStores]);
      const storeStat = yield* (yield* FileSystem.FileSystem).stat(expectedDir);
      assert.equal(storeStat.mode & 0o777, 0o700);
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

  it.effect("refuses a plaintext room and never sends into it", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        roomState: {
          "m.room.join_rules": { join_rule: "invite" },
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

  it.effect("fences the initial sync cursor before the sync loop starts", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk();
      yield* configureBridge();
      yield* startAdapter(sdk);
      yield* Deferred.await(sdk.started);

      const fence = sdk.requests.find((request) => request.endpoint === "/_matrix/client/v3/sync");
      assert.isDefined(fence);
      assert.equal(fence?.method, "GET");
      const query = fence?.qs as { readonly filter: string; readonly timeout: number };
      assert.equal(query.timeout, 0);
      const filter = decodeFenceFilter(query.filter);
      assert.deepEqual([...filter.room.rooms], [ROOM_ID]);
      assert.equal(filter.room.timeline.limit, 0);

      // The filter is registered first, because registering it is what clears
      // a stored cursor; the fence is then the last write before syncing.
      const filterRequestIndex = sdk.requests.findIndex((request) =>
        request.endpoint.endsWith("/filter"),
      );
      const fenceRequestIndex = sdk.requests.findIndex(
        (request) => request.endpoint === "/_matrix/client/v3/sync",
      );
      assert.isAbove(fenceRequestIndex, filterRequestIndex);
      assert.deepEqual([...sdk.storageWrites], [null, "fence-cursor"]);
      // The cursor survives the sync start, so history cannot replay.
      assert.deepEqual([...sdk.startedAfterStoredToken], ["fence-cursor"]);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("keeps an existing sync cursor instead of refencing", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({
        syncToken: "stored-cursor",
        storedFilter: { id: "stored-filter", filter: buildSyncFilter(ROOM_ID) },
      });
      yield* configureBridge();
      yield* startAdapter(sdk);
      yield* Deferred.await(sdk.started);

      assert.isUndefined(
        sdk.requests.find((request) => request.endpoint === "/_matrix/client/v3/sync"),
      );
      assert.isUndefined(sdk.requests.find((request) => request.endpoint.endsWith("/filter")));
      assert.lengthOf(sdk.storageWrites, 0);
      assert.deepEqual([...sdk.startedAfterStoredToken], ["stored-cursor"]);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("encrypts once and retries one transaction id", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk({ failSendsWith: [httpError(502)] });
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

  it.effect("delivers decrypted room text and ignores the bot's own events", () =>
    Effect.gen(function* () {
      const sdk = makeFakeSdk();
      const received = yield* Queue.unbounded<MatrixBridgeInboundText>();
      yield* configureBridge();
      yield* startAdapter(sdk, (event) => Queue.offer(received, event));
      yield* Deferred.await(sdk.started);

      sdk.emit(ROOM_ID, decryptedTextEvent({ eventId: "$bot", sender: BOT_USER_ID, body: "echo" }));
      sdk.emit(
        "!other:matrix.example.test",
        decryptedTextEvent({ eventId: "$other-room", sender: ALLOWED_USER_ID, body: "elsewhere" }),
      );
      sdk.emit(
        ROOM_ID,
        decryptedTextEvent({ eventId: "$adam", sender: ALLOWED_USER_ID, body: "hello" }),
      );

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
