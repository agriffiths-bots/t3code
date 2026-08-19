import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  NonNegativeInt,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type MatrixBridgeConfigureInput,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import {
  MATRIX_BRIDGE_CONFIG_SECRET,
  MatrixBridgeConfig,
  type MatrixBridgeConfigV1,
  decodeMatrixBridgeConfigJson,
  encodeMatrixBridgeConfigJson,
  layer as matrixBridgeConfigLayer,
  make,
} from "./MatrixBridgeConfig.ts";

const ownerA = ThreadId.make("thread-owner-a");
const ownerB = ThreadId.make("thread-owner-b");
const validInput: MatrixBridgeConfigureInput = {
  homeserverUrl: "https://matrix.example.test",
  accessToken: "matrix-secret-token",
  allowedUserIds: ["@adam:beeper.com"],
};

const persistedConfig = (overrides: Partial<MatrixBridgeConfigV1> = {}): MatrixBridgeConfigV1 => ({
  version: 1,
  homeserverUrl: "https://matrix.example.test/",
  accessToken: "matrix-secret-token",
  allowedUserIds: ["@adam:beeper.com"],
  roomId: null,
  pairing: { state: "unpaired" },
  ownerThreadId: null,
  ownershipEpoch: NonNegativeInt.make(0),
  cryptoStoreGeneration: "generation-one",
  ...overrides,
});

const makeThreadShell = (
  id: ThreadId,
  archivedAt: string | null = null,
): OrchestrationThreadShell => ({
  id,
  projectId: ProjectId.make("project-matrix"),
  dataAudience: "private",
  title: "Matrix owner",
  modelSelection: {
    instanceId: ProviderInstanceId.make("codex"),
    model: "gpt-5-codex",
  },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  latestTurn: null,
  createdAt: "2026-08-19T00:00:00.000Z",
  updatedAt: "2026-08-19T00:00:00.000Z",
  archivedAt,
  settledOverride: null,
  settledAt: null,
  session: null,
  latestUserMessageAt: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
  parentThreadId: null,
});

const projectionLayer = Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
  getThreadShellByIdIncludingArchived: (threadId) =>
    Effect.succeed(Option.some(makeThreadShell(threadId))),
});

const unusedCreate: ServerSecretStore.ServerSecretStore["Service"]["create"] = () =>
  Effect.die("unused secret-store create");
const unusedGetOrCreateRandom: ServerSecretStore.ServerSecretStore["Service"]["getOrCreateRandom"] =
  () => Effect.die("unused secret-store getOrCreateRandom");
const unusedRemove: ServerSecretStore.ServerSecretStore["Service"]["remove"] = () =>
  Effect.die("unused secret-store remove");

const makeMemorySecretStore = (initial?: Uint8Array) => {
  let bytes = initial;
  let failWrites = false;
  const service = ServerSecretStore.ServerSecretStore.of({
    get: () => Effect.succeed(Option.fromNullishOr(bytes)),
    set: (_name, value) =>
      failWrites
        ? Effect.fail(
            new ServerSecretStore.SecretStorePersistError({
              resource: "matrix bridge config",
              cause: PlatformError.systemError({
                _tag: "PermissionDenied",
                module: "FileSystem",
                method: "rename",
                pathOrDescriptor: "matrix-bridge-config.bin",
              }),
            }),
          )
        : Effect.sync(() => {
            bytes = Uint8Array.from(value);
          }),
    create: unusedCreate,
    getOrCreateRandom: unusedGetOrCreateRandom,
    remove: () =>
      Effect.sync(() => {
        bytes = undefined;
      }),
  });
  return {
    service,
    read: () => bytes,
    failWrites: () => {
      failWrites = true;
    },
  };
};

const makeService = (secretStore: ServerSecretStore.ServerSecretStore["Service"]) =>
  make.pipe(Effect.provideService(ServerSecretStore.ServerSecretStore, secretStore));

const makeLiveLayer = () => {
  const configLayer = ServerConfig.layerTest(process.cwd(), { prefix: "t3-matrix-config-test-" });
  return matrixBridgeConfigLayer.pipe(
    Layer.provideMerge(ServerSecretStore.layer),
    Layer.provideMerge(configLayer),
  );
};

it.layer(NodeServices.layer)("MatrixBridgeConfig", (it) => {
  it.effect("encodes and decodes the versioned config blob", () =>
    Effect.gen(function* () {
      const input = persistedConfig({
        roomId: "!room:matrix.example.test",
        pairing: {
          state: "paired",
          userId: "@adam:beeper.com",
          pairedAt: "2026-08-19T10:00:00.000Z",
        },
        ownerThreadId: ownerA,
        ownershipEpoch: NonNegativeInt.make(4),
      });

      const encoded = yield* encodeMatrixBridgeConfigJson(input);
      const decoded = decodeMatrixBridgeConfigJson(encoded);

      assert.deepEqual(Option.getOrThrow(decoded), input);
    }),
  );

  it.effect("persists one atomic 0600 secret through ServerSecretStore", () =>
    Effect.gen(function* () {
      const bridge = yield* MatrixBridgeConfig;
      const serverConfig = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;

      yield* bridge.configure(validInput);

      const secretPath = path.join(serverConfig.secretsDir, `${MATRIX_BRIDGE_CONFIG_SECRET}.bin`);
      const info = yield* fileSystem.stat(secretPath);
      const entries = yield* fileSystem.readDirectory(serverConfig.secretsDir);
      assert.equal(info.mode & 0o777, 0o600);
      assert.deepEqual(
        entries.filter((entry) => entry.includes(`${MATRIX_BRIDGE_CONFIG_SECRET}.bin.`)),
        [],
      );
    }).pipe(Effect.provide(makeLiveLayer())),
  );

  it.effect("fails closed on a malformed stored blob and can be recovered by configure", () =>
    Effect.gen(function* () {
      const memory = makeMemorySecretStore(
        new TextEncoder().encode('{"version":1,"accessToken":"leaked"}'),
      );
      const bridge = yield* makeService(memory.service);

      assert.deepEqual(yield* bridge.status, {
        state: "unavailable",
        ownerThreadId: null,
        encryptionReady: false,
        reason: "Stored Matrix bridge configuration is invalid.",
      });
      assert.isTrue(Option.isNone(yield* bridge.currentConfig));

      const view = yield* bridge.configure(validInput);
      assert.equal(view.homeserverUrl, "https://matrix.example.test/");
      assert.equal((yield* bridge.status).state, "connecting");
    }),
  );

  it.effect("publishes neither config nor status when persistence fails", () =>
    Effect.gen(function* () {
      const memory = makeMemorySecretStore();
      const bridge = yield* makeService(memory.service);
      yield* bridge.configure(validInput);
      const beforeConfig = yield* bridge.currentConfig;
      const beforeStatus = yield* bridge.status;
      memory.failWrites();

      const error = yield* Effect.flip(
        bridge.configure({ ...validInput, accessToken: "replacement-secret-token" }),
      );

      assert.equal(error.reason, "persistenceFailed");
      assert.deepEqual(yield* bridge.currentConfig, beforeConfig);
      assert.deepEqual(yield* bridge.status, beforeStatus);
    }),
  );

  it.effect("serializes concurrent owner replacement without losing epochs", () =>
    Effect.gen(function* () {
      let bytes: Uint8Array | undefined;
      let writeCount = 0;
      const firstOwnerWriteStarted = yield* Deferred.make<void>();
      const releaseFirstOwnerWrite = yield* Deferred.make<void>();
      const store = ServerSecretStore.ServerSecretStore.of({
        get: () => Effect.succeed(Option.fromNullishOr(bytes)),
        set: (_name, value) =>
          Effect.gen(function* () {
            writeCount += 1;
            if (writeCount === 2) {
              yield* Deferred.succeed(firstOwnerWriteStarted, undefined);
              yield* Deferred.await(releaseFirstOwnerWrite);
            }
            bytes = Uint8Array.from(value);
          }),
        create: unusedCreate,
        getOrCreateRandom: unusedGetOrCreateRandom,
        remove: unusedRemove,
      });
      const bridge = yield* makeService(store);
      yield* bridge.configure(validInput);

      const first = yield* bridge
        .setOwner(ownerA)
        .pipe(Effect.provide(projectionLayer), Effect.forkChild);
      yield* Deferred.await(firstOwnerWriteStarted);
      const second = yield* bridge
        .setOwner(ownerB)
        .pipe(Effect.provide(projectionLayer), Effect.forkChild);
      yield* Deferred.succeed(releaseFirstOwnerWrite, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(second);

      const config = Option.getOrThrow(yield* bridge.currentConfig);
      assert.equal(config.ownerThreadId, ownerB);
      assert.equal(config.ownershipEpoch, 2);
    }),
  );

  it.effect("clears an owner only when both the thread and epoch still match", () =>
    Effect.gen(function* () {
      const memory = makeMemorySecretStore();
      const bridge = yield* makeService(memory.service);
      yield* bridge.configure(validInput);

      yield* bridge.setOwner(ownerA).pipe(Effect.provide(projectionLayer));
      const staleOwner = Option.getOrThrow(yield* bridge.currentConfig);

      yield* bridge.setOwner(ownerB).pipe(Effect.provide(projectionLayer));
      const currentOwner = Option.getOrThrow(yield* bridge.currentConfig);
      assert.equal(currentOwner.ownerThreadId, ownerB);

      const unchanged = yield* bridge.clearOwnerIfMatches({
        ownerThreadId: staleOwner.ownerThreadId!,
        ownershipEpoch: staleOwner.ownershipEpoch,
      });
      assert.deepEqual(yield* bridge.currentConfig, Option.some(currentOwner));
      assert.equal(unchanged.ownerThreadId, ownerB);

      const cleared = yield* bridge.clearOwnerIfMatches({
        ownerThreadId: ownerB,
        ownershipEpoch: currentOwner.ownershipEpoch,
      });
      const afterClear = Option.getOrThrow(yield* bridge.currentConfig);
      assert.equal(afterClear.ownerThreadId, null);
      assert.equal(afterClear.ownershipEpoch, currentOwner.ownershipEpoch + 1);
      assert.equal(cleared.ownerThreadId, null);
    }),
  );

  it.effect("publishes a persisted owner change before honoring interruption", () =>
    Effect.gen(function* () {
      let bytes: Uint8Array | undefined;
      let writeCount = 0;
      const ownerPersisted = yield* Deferred.make<void>();
      const releaseOwnerWrite = yield* Deferred.make<void>();
      const store = ServerSecretStore.ServerSecretStore.of({
        get: () => Effect.succeed(Option.fromNullishOr(bytes)),
        set: (_name, value) =>
          Effect.gen(function* () {
            writeCount += 1;
            bytes = Uint8Array.from(value);
            if (writeCount === 2) {
              yield* Deferred.succeed(ownerPersisted, undefined);
              yield* Deferred.await(releaseOwnerWrite);
            }
          }),
        create: unusedCreate,
        getOrCreateRandom: unusedGetOrCreateRandom,
        remove: unusedRemove,
      });
      const bridge = yield* makeService(store);
      yield* bridge.configure(validInput);
      const ownerMutation = yield* bridge
        .setOwner(ownerA)
        .pipe(Effect.provide(projectionLayer), Effect.forkChild);
      yield* Deferred.await(ownerPersisted);

      const interruption = yield* Fiber.interrupt(ownerMutation).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Deferred.succeed(releaseOwnerWrite, undefined);
      yield* Fiber.join(interruption);

      const inMemory = Option.getOrThrow(yield* bridge.currentConfig);
      const persisted = Option.getOrThrow(
        decodeMatrixBridgeConfigJson(new TextDecoder().decode(bytes)),
      );
      assert.equal(inMemory.ownerThreadId, ownerA);
      assert.equal(inMemory.ownershipEpoch, 1);
      assert.deepEqual(inMemory, persisted);
    }),
  );

  it.effect("resets room, pairing, and ownership on identity changes and disconnect", () =>
    Effect.gen(function* () {
      const initial = persistedConfig({
        roomId: "!room:matrix.example.test",
        pairing: {
          state: "paired",
          userId: "@adam:beeper.com",
          pairedAt: "2026-08-19T10:00:00.000Z",
        },
        ownerThreadId: ownerA,
        ownershipEpoch: NonNegativeInt.make(7),
      });
      const memory = makeMemorySecretStore(
        new TextEncoder().encode(yield* encodeMatrixBridgeConfigJson(initial)),
      );
      const bridge = yield* makeService(memory.service);

      const view = yield* bridge.configure({
        ...validInput,
        allowedUserIds: ["@adam:beeper.com", "@second:example.test"],
      });
      const reconfigured = Option.getOrThrow(yield* bridge.currentConfig);
      assert.equal(view.roomId, null);
      assert.deepEqual(reconfigured.pairing, { state: "unpaired" });
      assert.equal(reconfigured.ownerThreadId, null);
      assert.equal(reconfigured.ownershipEpoch, 0);
      assert.notEqual(reconfigured.cryptoStoreGeneration, initial.cryptoStoreGeneration);

      assert.deepEqual(yield* bridge.disconnect, {
        state: "disabled",
        ownerThreadId: null,
        encryptionReady: false,
        reason: null,
      });
      assert.isTrue(Option.isNone(yield* bridge.currentConfig));
      assert.isUndefined(memory.read());
    }),
  );

  it.effect("redacts the token from every RPC-safe view and status", () =>
    Effect.gen(function* () {
      const memory = makeMemorySecretStore();
      const bridge = yield* makeService(memory.service);

      const view = yield* bridge.configure(validInput);
      // @effect-diagnostics-next-line preferSchemaOverJson:off -- proves the RPC-safe shape cannot serialize the token.
      const serialized = JSON.stringify({ view, status: yield* bridge.status });

      assert.notInclude(serialized, validInput.accessToken);
      assert.notProperty(view, "accessToken");
      assert.deepEqual(view.allowedUserIds, ["@adam:beeper.com"]);
    }),
  );

  it.effect("accepts HTTP only for literal loopback homeservers", () =>
    Effect.gen(function* () {
      const memory = makeMemorySecretStore();
      const bridge = yield* makeService(memory.service);

      const loopback = yield* bridge.configure({
        ...validInput,
        homeserverUrl: "http://127.0.0.1:8008",
      });
      assert.equal(loopback.homeserverUrl, "http://127.0.0.1:8008/");

      const error = yield* Effect.flip(
        bridge.configure({ ...validInput, homeserverUrl: "http://matrix.example.test" }),
      );
      assert.equal(error.reason, "invalidConfiguration");
    }),
  );
});
