// @effect-diagnostics nodeBuiltinImport:off - the credentials gate runs at
// module load, before any Effect runtime exists, so it reads the file directly.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { assert, describe, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  MATRIX_CRYPTO_STORE_DIRECTORY_NAME,
  MATRIX_SYNC_STORE_FILE_NAME,
  MATRIX_CRYPTO_SQLITE_STORE_TYPE,
  buildEncryptedRoomCreateOptions,
  buildSyncFilter,
  encryptedSendEndpoint,
  createFencedSyncStorage,
  ensureSyncFilter,
  loadMatrixBotSdk,
  verifyEncryptedRoom,
  type FencedSyncStorage,
  type MatrixSdkClient,
} from "./MatrixBotSdkClient.ts";

/**
 * Live homeserver smoke for the encrypted transport. It is skipped unless
 * `T3CODE_MATRIX_SMOKE_CREDENTIALS` points at a JSON file holding a bot
 * account, so CI and ordinary local runs never touch the network. Run it after
 * changing the adapter, the SDK pin, or the crypto pin:
 *
 *   T3CODE_MATRIX_SMOKE_CREDENTIALS=/path/to/bot.json \
 *     vp test run apps/server/src/matrix/MatrixBotSdkClient.smoke.test.ts
 *
 * The credentials file is `{ "homeserverUrl", "userId", "accessToken" }`. The
 * smoke creates one room, verifies it, sends one encrypted message to itself,
 * then leaves and forgets the room. It never invites anyone.
 */
const SmokeCredentials = Schema.Struct({
  homeserverUrl: Schema.String,
  userId: Schema.String,
  accessToken: Schema.String,
});
const decodeCredentials = Schema.decodeUnknownOption(Schema.fromJsonString(SmokeCredentials));

function readCredentials(): typeof SmokeCredentials.Type | null {
  const credentialsPath = process.env.T3CODE_MATRIX_SMOKE_CREDENTIALS?.trim();
  if (credentialsPath === undefined || credentialsPath === "") return null;
  try {
    const decoded = decodeCredentials(NodeFS.readFileSync(credentialsPath, "utf8"));
    return decoded._tag === "Some" ? decoded.value : null;
  } catch {
    return null;
  }
}

const credentials = readCredentials();
const describeSmoke = credentials === null ? describe.skip : describe;

const FIRST_SYNC_POLL_INTERVAL_MS = 250;
const FIRST_SYNC_ATTEMPTS = 120;

/** The sync loop runs in the background, so its first batch is polled for. */
const awaitFirstSyncBatch = Effect.fn("awaitFirstSyncBatch")(function* (fence: FencedSyncStorage) {
  for (let attempt = 0; attempt < FIRST_SYNC_ATTEMPTS; attempt += 1) {
    if (fence.syncedBatches() > 0) return;
    yield* Effect.sleep(FIRST_SYNC_POLL_INTERVAL_MS);
  }
  assert.fail("The homeserver never delivered a sync batch.");
});

/** Cleanup-only calls the bridge never makes in production. */
interface SmokeClient extends MatrixSdkClient {
  leaveRoom(roomId: string): Promise<unknown>;
  forgetRoom(roomId: string): Promise<unknown>;
}

describeSmoke("MatrixBotSdkClient live homeserver smoke", () => {
  // Real clock: this test waits on a live homeserver, not on virtual time.
  it.live(
    "logs in, creates a verified encrypted room, and sends one idempotent message",
    () =>
      Effect.gen(function* () {
        if (credentials === null) return;

        // The store is kept, not temporary: a Matrix device may upload one-time
        // keys from exactly one crypto store, so a fresh store against an
        // already-prepared device fails `keys/upload` with `M_UNKNOWN`. Delete
        // this directory only together with a fresh bot login.
        const storeDir = yield* Effect.sync(() =>
          NodePath.join(
            process.env.T3CODE_MATRIX_SMOKE_STORE_DIR?.trim() ||
              NodePath.join(NodeOS.tmpdir(), "t3-matrix-smoke-store"),
            credentials.userId.replaceAll(/[^a-z0-9]+/giu, "-"),
          ),
        );
        yield* Effect.sync(() => NodeFS.mkdirSync(storeDir, { recursive: true, mode: 0o700 }));

        const module = yield* Effect.promise(loadMatrixBotSdk);
        const storage = new module.SimpleFsStorageProvider(
          NodePath.join(storeDir, MATRIX_SYNC_STORE_FILE_NAME),
        );
        const cryptoStore = new module.RustSdkCryptoStorageProvider(
          NodePath.join(storeDir, MATRIX_CRYPTO_STORE_DIRECTORY_NAME),
          MATRIX_CRYPTO_SQLITE_STORE_TYPE,
        );
        const fence = createFencedSyncStorage(storage);
        const client = yield* Effect.acquireRelease(
          Effect.sync(
            () =>
              new module.MatrixClient(
                credentials.homeserverUrl,
                credentials.accessToken,
                fence.storage,
                cryptoStore,
              ) as SmokeClient,
          ),
          (live) => Effect.sync(() => live.stop()),
        );

        const botUserId = yield* Effect.promise(() => client.getUserId());
        assert.equal(botUserId, credentials.userId);

        // Left on the way out so the smoke leaves no joined room behind.
        // `forget` is best-effort: some homeservers (Beeper's included) do not
        // implement it.
        const roomId = yield* Effect.acquireRelease(
          Effect.promise(() => client.createRoom(buildEncryptedRoomCreateOptions([]))),
          (created) =>
            Effect.tryPromise(() => client.leaveRoom(created)).pipe(
              Effect.andThen(
                Effect.tryPromise(() => client.forgetRoom(created)).pipe(Effect.ignore),
              ),
              Effect.ignore,
            ),
        );

        yield* verifyEncryptedRoom(client, roomId, botUserId);
        const syncFilter = buildSyncFilter(roomId);
        yield* ensureSyncFilter(client, fence.storage, botUserId, syncFilter);

        yield* Effect.promise(() => client.start(syncFilter));
        // `start` resolves once the sync loop is launched, so the first batch
        // has to be waited for. The SDK processes it in full, which is how its
        // room keys reach the crypto store even though its timeline is fenced
        // off the bridge.
        yield* awaitFirstSyncBatch(fence);
        assert.isNotNull(fence.storage.getSyncToken());
        const crypto = client.crypto;
        assert.isDefined(crypto);
        assert.isTrue(crypto?.isReady);
        if (crypto === undefined) return;

        const encrypted = yield* Effect.promise(() =>
          crypto.encryptRoomEvent(roomId, "m.room.message", {
            msgtype: "m.text",
            body: "t3 bridge smoke",
          }),
        );
        const endpoint = encryptedSendEndpoint(
          roomId,
          `t3-smoke-${yield* Clock.currentTimeMillis}`,
        );
        const first = yield* Effect.promise(() =>
          client.doRequest("PUT", endpoint, null, encrypted),
        );
        const retry = yield* Effect.promise(() =>
          client.doRequest("PUT", endpoint, null, encrypted),
        );
        // One transaction ID is one event, however many times it is retried.
        assert.deepEqual(retry, first);
      }),
    { timeout: 120_000 },
  );
});
