import {
  IsoDateTime,
  MatrixBridgeOperationError,
  NonNegativeInt,
  ThreadId,
  TrimmedNonEmptyString,
  TurnId,
  type MatrixBridgeConfigureInput,
  type MatrixBridgeConfigView,
  type MatrixBridgeStatus,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";

export const MATRIX_BRIDGE_CONFIG_SECRET = "matrix-bridge-config";

const MatrixUserId = TrimmedNonEmptyString.check(Schema.isPattern(/^@[^\s:]+:[^\s]+$/u));

const MatrixBridgePairing = Schema.Union([
  Schema.Struct({ state: Schema.Literal("unpaired") }),
  Schema.Struct({
    state: Schema.Literal("paired"),
    userId: MatrixUserId,
    pairedAt: IsoDateTime,
  }),
]);

const matrixBridgeConfigV1Fields = {
  version: Schema.Literal(1),
  homeserverUrl: TrimmedNonEmptyString,
  accessToken: TrimmedNonEmptyString,
  allowedUserIds: Schema.Array(MatrixUserId).check(Schema.isNonEmpty()),
  roomId: Schema.NullOr(TrimmedNonEmptyString),
  pairing: MatrixBridgePairing,
  ownerThreadId: Schema.NullOr(ThreadId),
  ownershipEpoch: NonNegativeInt,
  cryptoStoreGeneration: TrimmedNonEmptyString,
};

export const MatrixBridgeConfigV1 = Schema.Struct({
  ...matrixBridgeConfigV1Fields,
  lastDeliveredTurnId: Schema.NullOr(TurnId),
  deliveryBaselineSequence: NonNegativeInt,
  deliveryCheckpointInitialized: Schema.Boolean,
});
export type MatrixBridgeConfigV1 = typeof MatrixBridgeConfigV1.Type;

const MatrixBridgeConfigJson = Schema.fromJsonString(MatrixBridgeConfigV1);
const StoredMatrixBridgeConfigJson = Schema.fromJsonString(
  Schema.Struct({
    ...matrixBridgeConfigV1Fields,
    lastDeliveredTurnId: Schema.optionalKey(Schema.NullOr(TurnId)),
    deliveryBaselineSequence: Schema.optionalKey(NonNegativeInt),
    deliveryCheckpointInitialized: Schema.optionalKey(Schema.Boolean),
  }),
);
export const encodeMatrixBridgeConfigJson = Schema.encodeEffect(MatrixBridgeConfigJson);
const decodeStoredMatrixBridgeConfigJson = Schema.decodeUnknownOption(StoredMatrixBridgeConfigJson);
export const decodeMatrixBridgeConfigJson = (input: unknown): Option.Option<MatrixBridgeConfigV1> =>
  Option.map(decodeStoredMatrixBridgeConfigJson(input), (config) => {
    const hasDeliveryBaseline = config.deliveryBaselineSequence !== undefined;
    return {
      ...config,
      lastDeliveredTurnId: config.lastDeliveredTurnId ?? null,
      deliveryBaselineSequence: config.deliveryBaselineSequence ?? NonNegativeInt.make(0),
      deliveryCheckpointInitialized:
        (config.deliveryCheckpointInitialized ?? false) && hasDeliveryBaseline,
    };
  });

const MatrixBridgePublicConfigureFields = Schema.Struct({
  homeserverUrl: TrimmedNonEmptyString,
  allowedUserIds: Schema.Array(MatrixUserId).check(Schema.isNonEmpty()),
});
const MatrixBridgeAccessToken = TrimmedNonEmptyString;
const decodePublicConfigureFields = Schema.decodeUnknownOption(MatrixBridgePublicConfigureFields);
const decodeAccessToken = Schema.decodeUnknownOption(MatrixBridgeAccessToken);
const decodeUrl = Schema.decodeUnknownOption(Schema.URLFromString);

const DISABLED_STATUS: MatrixBridgeStatus = {
  state: "disabled",
  ownerThreadId: null,
  encryptionReady: false,
  reason: null,
};

const PERMANENT_SEND_FAILURE_REASON =
  "Matrix delivery is unavailable. Check the bridge credentials, room, and bot permissions.";

const LOOPBACK_HTTP_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);

const operationError = (
  reason: MatrixBridgeOperationError["reason"],
  message: string,
): MatrixBridgeOperationError => new MatrixBridgeOperationError({ reason, message });

const persistenceError = (): MatrixBridgeOperationError =>
  operationError("persistenceFailed", "Matrix bridge configuration could not be persisted.");

const configStatus = (config: MatrixBridgeConfigV1): MatrixBridgeStatus => ({
  state: "connecting",
  ownerThreadId: config.ownerThreadId,
  encryptionReady: false,
  reason: null,
});

export const toMatrixBridgeConfigView = (config: MatrixBridgeConfigV1): MatrixBridgeConfigView => ({
  homeserverUrl: config.homeserverUrl,
  allowedUserIds: config.allowedUserIds,
  roomId: config.roomId,
});

function sameStrings(left: ReadonlyArray<string>, right: ReadonlyArray<string>): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function bytesToString(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}

function stringToBytes(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

function normalizeConfigureInput(input: MatrixBridgeConfigureInput): Effect.Effect<
  {
    readonly homeserverUrl: string;
    readonly accessToken: string;
    readonly allowedUserIds: readonly [string, ...string[]];
  },
  MatrixBridgeOperationError
> {
  const publicFields = Option.getOrNull(
    decodePublicConfigureFields({
      homeserverUrl: input.homeserverUrl,
      allowedUserIds: input.allowedUserIds,
    }),
  );
  const accessToken = Option.getOrNull(decodeAccessToken(input.accessToken));
  if (publicFields === null || accessToken === null) {
    return Effect.fail(
      operationError(
        "invalidConfiguration",
        "A homeserver URL, bot access token, and at least one valid Matrix user ID are required.",
      ),
    );
  }

  const homeserver = Option.getOrNull(decodeUrl(publicFields.homeserverUrl));
  if (
    homeserver === null ||
    (homeserver.protocol !== "https:" &&
      !(homeserver.protocol === "http:" && LOOPBACK_HTTP_HOSTS.has(homeserver.hostname))) ||
    homeserver.username !== "" ||
    homeserver.password !== ""
  ) {
    return Effect.fail(
      operationError(
        "invalidConfiguration",
        "The Matrix homeserver must use HTTPS, except for an HTTP loopback address.",
      ),
    );
  }

  const allowedUserIds = [...new Set(publicFields.allowedUserIds)].toSorted();
  return Effect.succeed({
    homeserverUrl: homeserver.toString(),
    accessToken,
    allowedUserIds: allowedUserIds as [string, ...string[]],
  });
}

export class MatrixBridgeConfig extends Context.Service<
  MatrixBridgeConfig,
  {
    /** Internal secret-bearing view for the bridge reactor. Never return or log it. */
    readonly currentConfig: Effect.Effect<Option.Option<MatrixBridgeConfigV1>>;
    readonly status: Effect.Effect<MatrixBridgeStatus>;
    readonly statusChanges: Stream.Stream<MatrixBridgeStatus>;
    readonly configure: (
      input: MatrixBridgeConfigureInput,
    ) => Effect.Effect<MatrixBridgeConfigView, MatrixBridgeOperationError>;
    readonly disconnect: Effect.Effect<MatrixBridgeStatus, MatrixBridgeOperationError>;
    readonly setOwner: (
      ownerThreadId: ThreadId | null,
    ) => Effect.Effect<
      MatrixBridgeStatus,
      MatrixBridgeOperationError,
      ProjectionSnapshotQuery.ProjectionSnapshotQuery
    >;
    readonly clearOwnerIfMatches: (expected: {
      readonly ownerThreadId: ThreadId;
      readonly ownershipEpoch: MatrixBridgeConfigV1["ownershipEpoch"];
    }) => Effect.Effect<MatrixBridgeStatus, MatrixBridgeOperationError>;
    readonly initializeDeliveryCheckpointIfMissing: (expected: {
      readonly ownerThreadId: ThreadId;
      readonly ownershipEpoch: MatrixBridgeConfigV1["ownershipEpoch"];
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly roomId: string;
      readonly baselineTurnId: TurnId | null;
      readonly baselineSequence: MatrixBridgeConfigV1["deliveryBaselineSequence"];
    }) => Effect.Effect<boolean, MatrixBridgeOperationError>;
    readonly markDeliveredIfMatches: (expected: {
      readonly ownerThreadId: ThreadId;
      readonly ownershipEpoch: MatrixBridgeConfigV1["ownershipEpoch"];
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly roomId: string;
      readonly turnId: TurnId;
      readonly turnSequence: MatrixBridgeConfigV1["deliveryBaselineSequence"];
    }) => Effect.Effect<boolean, MatrixBridgeOperationError>;
    readonly reportPermanentSendFailureIfMatches: (expected: {
      readonly ownerThreadId: ThreadId;
      readonly ownershipEpoch: MatrixBridgeConfigV1["ownershipEpoch"];
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly roomId: string;
    }) => Effect.Effect<boolean>;
  }
>()("t3/matrix/MatrixBridgeConfig") {}

export const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const secretStore = yield* ServerSecretStore.ServerSecretStore;
  const mutationSemaphore = yield* Semaphore.make(1);

  const storedResult = yield* Effect.result(secretStore.get(MATRIX_BRIDGE_CONFIG_SECRET));
  let initialConfig = Option.none<MatrixBridgeConfigV1>();
  let initialStatus: MatrixBridgeStatus = DISABLED_STATUS;
  if (Result.isFailure(storedResult)) {
    initialStatus = {
      state: "unavailable",
      ownerThreadId: null,
      encryptionReady: false,
      reason: "Stored Matrix bridge configuration could not be read.",
    };
  } else if (Option.isSome(storedResult.success)) {
    const decoded = decodeMatrixBridgeConfigJson(bytesToString(storedResult.success.value));
    if (Option.isSome(decoded)) {
      initialConfig = decoded;
      initialStatus = configStatus(decoded.value);
    } else {
      initialStatus = {
        state: "unavailable",
        ownerThreadId: null,
        encryptionReady: false,
        reason: "Stored Matrix bridge configuration is invalid.",
      };
    }
  }

  const configRef = yield* Ref.make(initialConfig);
  const statusRef = yield* SubscriptionRef.make(initialStatus);

  const persist = Effect.fn("MatrixBridgeConfig.persist")(
    function* (config: MatrixBridgeConfigV1) {
      const encoded = yield* encodeMatrixBridgeConfigJson(config);
      yield* secretStore.set(MATRIX_BRIDGE_CONFIG_SECRET, stringToBytes(encoded));
    },
    Effect.mapError(() => persistenceError()),
  );

  const configure = Effect.fn("MatrixBridgeConfig.configure")((input: MatrixBridgeConfigureInput) =>
    mutationSemaphore.withPermits(1)(
      Effect.gen(function* () {
        const normalized = yield* normalizeConfigureInput(input);
        const current = Option.getOrNull(yield* Ref.get(configRef));
        const identityChanged =
          current === null ||
          current.homeserverUrl !== normalized.homeserverUrl ||
          current.accessToken !== normalized.accessToken ||
          !sameStrings(current.allowedUserIds, normalized.allowedUserIds);
        const generation = identityChanged
          ? yield* crypto.randomUUIDv4.pipe(Effect.mapError(() => persistenceError()))
          : current.cryptoStoreGeneration;
        const next: MatrixBridgeConfigV1 = identityChanged
          ? {
              version: 1,
              homeserverUrl: normalized.homeserverUrl,
              accessToken: normalized.accessToken,
              allowedUserIds: normalized.allowedUserIds,
              roomId: null,
              pairing: { state: "unpaired" },
              ownerThreadId: null,
              ownershipEpoch: NonNegativeInt.make(0),
              cryptoStoreGeneration: generation,
              lastDeliveredTurnId: null,
              deliveryBaselineSequence: NonNegativeInt.make(0),
              deliveryCheckpointInitialized: true,
            }
          : current;
        const nextStatus = configStatus(next);

        // Persistence is the linearization point. Mask interruption through
        // publication so cancellation cannot split disk from live state.
        return yield* Effect.uninterruptible(
          Effect.gen(function* () {
            yield* persist(next);
            yield* Ref.set(configRef, Option.some(next));
            yield* SubscriptionRef.set(statusRef, nextStatus);
            return toMatrixBridgeConfigView(next);
          }),
        );
      }),
    ),
  );

  const disconnect = mutationSemaphore.withPermits(1)(
    Effect.uninterruptible(
      secretStore.remove(MATRIX_BRIDGE_CONFIG_SECRET).pipe(
        Effect.mapError(() => persistenceError()),
        Effect.andThen(Ref.set(configRef, Option.none())),
        Effect.andThen(SubscriptionRef.set(statusRef, DISABLED_STATUS)),
        Effect.as(DISABLED_STATUS),
      ),
    ),
  );

  const setOwner = Effect.fn("MatrixBridgeConfig.setOwner")((ownerThreadId: ThreadId | null) =>
    mutationSemaphore.withPermits(1)(
      Effect.gen(function* () {
        const current = Option.getOrNull(yield* Ref.get(configRef));
        if (current === null) {
          if (ownerThreadId === null) return yield* SubscriptionRef.get(statusRef);
          return yield* operationError(
            "notConfigured",
            "Configure the Matrix bridge before selecting an owner thread.",
          );
        }

        let baselineTurnId: TurnId | null = null;
        let baselineSequence = NonNegativeInt.make(0);
        if (ownerThreadId !== null) {
          const projection = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
          const snapshot = yield* projection
            .getThreadShellSnapshotByIdIncludingArchived(ownerThreadId)
            .pipe(
              Effect.mapError(() =>
                operationError(
                  "threadLookupFailed",
                  "The selected owner thread could not be validated.",
                ),
              ),
            );
          if (Option.isNone(snapshot.thread)) {
            return yield* operationError(
              "threadNotFound",
              "The selected owner thread was not found.",
            );
          }
          if (snapshot.thread.value.archivedAt !== null) {
            return yield* operationError(
              "threadArchived",
              "An archived thread cannot own the Matrix bridge.",
            );
          }
          baselineTurnId = snapshot.thread.value.latestTurn?.turnId ?? null;
          baselineSequence = NonNegativeInt.make(snapshot.snapshotSequence);
        }

        const next: MatrixBridgeConfigV1 = {
          ...current,
          ownerThreadId,
          ownershipEpoch: NonNegativeInt.make(current.ownershipEpoch + 1),
          lastDeliveredTurnId: baselineTurnId,
          deliveryBaselineSequence: baselineSequence,
          deliveryCheckpointInitialized: true,
        };
        const currentStatus = yield* SubscriptionRef.get(statusRef);
        const nextStatus: MatrixBridgeStatus = { ...currentStatus, ownerThreadId };
        return yield* Effect.uninterruptible(
          Effect.gen(function* () {
            yield* persist(next);
            yield* Ref.set(configRef, Option.some(next));
            yield* SubscriptionRef.set(statusRef, nextStatus);
            return nextStatus;
          }),
        );
      }),
    ),
  );

  const clearOwnerIfMatches = Effect.fn("MatrixBridgeConfig.clearOwnerIfMatches")(
    (expected: {
      readonly ownerThreadId: ThreadId;
      readonly ownershipEpoch: MatrixBridgeConfigV1["ownershipEpoch"];
    }) =>
      mutationSemaphore.withPermits(1)(
        Effect.gen(function* () {
          const current = Option.getOrNull(yield* Ref.get(configRef));
          if (
            current === null ||
            current.ownerThreadId !== expected.ownerThreadId ||
            current.ownershipEpoch !== expected.ownershipEpoch
          ) {
            return yield* SubscriptionRef.get(statusRef);
          }

          const next: MatrixBridgeConfigV1 = {
            ...current,
            ownerThreadId: null,
            ownershipEpoch: NonNegativeInt.make(current.ownershipEpoch + 1),
            lastDeliveredTurnId: null,
            deliveryBaselineSequence: NonNegativeInt.make(0),
            deliveryCheckpointInitialized: true,
          };
          const currentStatus = yield* SubscriptionRef.get(statusRef);
          const nextStatus: MatrixBridgeStatus = { ...currentStatus, ownerThreadId: null };
          return yield* Effect.uninterruptible(
            Effect.gen(function* () {
              yield* persist(next);
              yield* Ref.set(configRef, Option.some(next));
              yield* SubscriptionRef.set(statusRef, nextStatus);
              return nextStatus;
            }),
          );
        }),
      ),
  );

  const initializeDeliveryCheckpointIfMissing = Effect.fn(
    "MatrixBridgeConfig.initializeDeliveryCheckpointIfMissing",
  )(
    (expected: {
      readonly ownerThreadId: ThreadId;
      readonly ownershipEpoch: MatrixBridgeConfigV1["ownershipEpoch"];
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly roomId: string;
      readonly baselineTurnId: TurnId | null;
      readonly baselineSequence: MatrixBridgeConfigV1["deliveryBaselineSequence"];
    }) =>
      mutationSemaphore.withPermits(1)(
        Effect.gen(function* () {
          const current = Option.getOrNull(yield* Ref.get(configRef));
          if (
            current === null ||
            current.ownerThreadId !== expected.ownerThreadId ||
            current.ownershipEpoch !== expected.ownershipEpoch ||
            current.cryptoStoreGeneration !== expected.cryptoStoreGeneration ||
            current.roomId !== expected.roomId ||
            current.pairing.state !== "paired"
          ) {
            return false;
          }
          if (current.deliveryCheckpointInitialized) return true;

          const next: MatrixBridgeConfigV1 = {
            ...current,
            lastDeliveredTurnId: expected.baselineTurnId,
            deliveryBaselineSequence: expected.baselineSequence,
            deliveryCheckpointInitialized: true,
          };
          return yield* Effect.uninterruptible(
            Effect.gen(function* () {
              yield* persist(next);
              yield* Ref.set(configRef, Option.some(next));
              return true;
            }),
          );
        }),
      ),
  );

  const markDeliveredIfMatches = Effect.fn("MatrixBridgeConfig.markDeliveredIfMatches")(
    (expected: {
      readonly ownerThreadId: ThreadId;
      readonly ownershipEpoch: MatrixBridgeConfigV1["ownershipEpoch"];
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly roomId: string;
      readonly turnId: TurnId;
      readonly turnSequence: MatrixBridgeConfigV1["deliveryBaselineSequence"];
    }) =>
      mutationSemaphore.withPermits(1)(
        Effect.gen(function* () {
          const current = Option.getOrNull(yield* Ref.get(configRef));
          if (
            current === null ||
            current.ownerThreadId !== expected.ownerThreadId ||
            current.ownershipEpoch !== expected.ownershipEpoch ||
            current.cryptoStoreGeneration !== expected.cryptoStoreGeneration ||
            current.roomId !== expected.roomId ||
            current.pairing.state !== "paired"
          ) {
            return false;
          }
          if (current.lastDeliveredTurnId === expected.turnId) return true;

          const next: MatrixBridgeConfigV1 = {
            ...current,
            lastDeliveredTurnId: expected.turnId,
            deliveryBaselineSequence: expected.turnSequence,
            deliveryCheckpointInitialized: true,
          };
          return yield* Effect.uninterruptible(
            Effect.gen(function* () {
              yield* persist(next);
              yield* Ref.set(configRef, Option.some(next));
              return true;
            }),
          );
        }),
      ),
  );

  const reportPermanentSendFailureIfMatches = Effect.fn(
    "MatrixBridgeConfig.reportPermanentSendFailureIfMatches",
  )(
    (expected: {
      readonly ownerThreadId: ThreadId;
      readonly ownershipEpoch: MatrixBridgeConfigV1["ownershipEpoch"];
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly roomId: string;
    }) =>
      mutationSemaphore.withPermits(1)(
        Effect.gen(function* () {
          const current = Option.getOrNull(yield* Ref.get(configRef));
          if (
            current === null ||
            current.ownerThreadId !== expected.ownerThreadId ||
            current.ownershipEpoch !== expected.ownershipEpoch ||
            current.cryptoStoreGeneration !== expected.cryptoStoreGeneration ||
            current.roomId !== expected.roomId ||
            current.pairing.state !== "paired"
          ) {
            return false;
          }

          const status = yield* SubscriptionRef.get(statusRef);
          yield* SubscriptionRef.set(statusRef, {
            ...status,
            state: "degraded",
            ownerThreadId: expected.ownerThreadId,
            reason: PERMANENT_SEND_FAILURE_REASON,
          });
          return true;
        }),
      ),
  );

  return MatrixBridgeConfig.of({
    currentConfig: Ref.get(configRef),
    status: SubscriptionRef.get(statusRef),
    statusChanges: SubscriptionRef.changes(statusRef),
    configure,
    disconnect,
    setOwner,
    clearOwnerIfMatches,
    initializeDeliveryCheckpointIfMissing,
    markDeliveredIfMatches,
    reportPermanentSendFailureIfMatches,
  });
});

export const layer = Layer.effect(MatrixBridgeConfig, make);
