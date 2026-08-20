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
import * as DateTime from "effect/DateTime";
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
    /**
     * The room event that carried the code. Persisted so a redelivery after a
     * restart is recognised as the pairing reply and never becomes a turn.
     * Absent on connections paired by an earlier build.
     */
    eventId: Schema.optionalKey(TrimmedNonEmptyString),
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
  configuredAt: Schema.NullOr(IsoDateTime),
  lastDeliveredTurnId: Schema.NullOr(TurnId),
  deliveryBaselineSequence: NonNegativeInt,
  deliveryCheckpointInitialized: Schema.Boolean,
});
export type MatrixBridgeConfigV1 = typeof MatrixBridgeConfigV1.Type;

const MatrixBridgeConfigJson = Schema.fromJsonString(MatrixBridgeConfigV1);
const StoredMatrixBridgeConfigJson = Schema.fromJsonString(
  Schema.Struct({
    ...matrixBridgeConfigV1Fields,
    configuredAt: Schema.optionalKey(Schema.NullOr(IsoDateTime)),
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
      configuredAt: config.configuredAt ?? null,
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

export const MATRIX_BRIDGE_PERMANENT_SEND_FAILURE_REASON =
  "Matrix delivery is unavailable. Check the bridge credentials, room, and bot permissions.";

export const MATRIX_BRIDGE_UNEXPECTED_MEMBER_REASON =
  "An account outside the allowed list is in the Matrix room, so bridge output is paused.";

export const MATRIX_BRIDGE_PAIRING_PERSIST_FAILURE_REASON =
  "Pairing could not be saved, so the Matrix bridge stays locked.";

export const MATRIX_BRIDGE_INBOUND_OVERFLOW_REASON =
  "Matrix messages arrived faster than the bridge could start turns, so some were dropped.";

export const MATRIX_BRIDGE_INBOUND_UNVERIFIED_REASON =
  "Matrix room membership could not be verified, so some messages were not started.";

export const MATRIX_BRIDGE_INBOUND_FAILED_REASON =
  "Some Matrix messages could not be started. Check the server log for the failure.";

/** Sanitized operator-facing reasons; never a body, token, or room id. */
export const MATRIX_BRIDGE_DEGRADED_REASONS = {
  "pairing-persist-failure": MATRIX_BRIDGE_PAIRING_PERSIST_FAILURE_REASON,
  "inbound-overflow": MATRIX_BRIDGE_INBOUND_OVERFLOW_REASON,
  "permanent-send-failure": MATRIX_BRIDGE_PERMANENT_SEND_FAILURE_REASON,
  "inbound-unverified": MATRIX_BRIDGE_INBOUND_UNVERIFIED_REASON,
  "inbound-failed": MATRIX_BRIDGE_INBOUND_FAILED_REASON,
} as const;
export type MatrixBridgeDegradedCause = keyof typeof MATRIX_BRIDGE_DEGRADED_REASONS;

/** Reported in this order when more than one fault stands. */
const MATRIX_BRIDGE_DEGRADED_CAUSE_ORDER = [
  "pairing-persist-failure",
  "inbound-failed",
  "inbound-unverified",
  "inbound-overflow",
  "permanent-send-failure",
] as const satisfies ReadonlyArray<MatrixBridgeDegradedCause>;

const withDegradedCause = (
  causes: ReadonlyArray<MatrixBridgeDegradedCause>,
  cause: MatrixBridgeDegradedCause,
): ReadonlyArray<MatrixBridgeDegradedCause> =>
  causes.includes(cause) ? causes : [...causes, cause];

const withoutDegradedCause = (
  causes: ReadonlyArray<MatrixBridgeDegradedCause>,
  cause: MatrixBridgeDegradedCause,
): ReadonlyArray<MatrixBridgeDegradedCause> => causes.filter((candidate) => candidate !== cause);

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
  ...(config.configuredAt === null ? {} : { configuredAt: config.configuredAt }),
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

/**
 * Transport lifecycle reported by the Matrix client adapter. `unavailable`
 * carries an operator-facing reason only: never a token, room, message body, or
 * native failure detail.
 */
export type MatrixBridgeTransportState =
  | { readonly state: "ready" }
  | { readonly state: "unavailable"; readonly reason: string };

/**
 * Everything a connected bridge knows that is not persisted: it is rebuilt from
 * the transport and the room on every connection, and reset whenever the
 * connection identity changes.
 */
interface MatrixBridgeRuntimeState {
  readonly cryptoStoreGeneration: string | null;
  readonly transportReady: boolean;
  readonly allowedMemberPresent: boolean;
  readonly unexpectedMemberPresent: boolean;
  /**
   * Every fault currently standing. They are independent, so each is cleared
   * only by its own recovery and the status reports the first that applies.
   */
  readonly degradedCauses: ReadonlyArray<MatrixBridgeDegradedCause>;
}

const INITIAL_RUNTIME_STATE: MatrixBridgeRuntimeState = {
  cryptoStoreGeneration: null,
  transportReady: false,
  allowedMemberPresent: false,
  unexpectedMemberPresent: false,
  degradedCauses: [],
};

/**
 * Status for a connected transport. An unexpected member outranks everything
 * else because it is the one condition that pauses delivery.
 */
const connectedStatus = (
  config: MatrixBridgeConfigV1,
  runtime: MatrixBridgeRuntimeState,
): MatrixBridgeStatus => {
  const base = { ownerThreadId: config.ownerThreadId, encryptionReady: true } as const;
  if (runtime.unexpectedMemberPresent) {
    return { ...base, state: "degraded", reason: MATRIX_BRIDGE_UNEXPECTED_MEMBER_REASON };
  }
  const cause = MATRIX_BRIDGE_DEGRADED_CAUSE_ORDER.find((candidate) =>
    runtime.degradedCauses.includes(candidate),
  );
  if (cause !== undefined) {
    return { ...base, state: "degraded", reason: MATRIX_BRIDGE_DEGRADED_REASONS[cause] };
  }
  // Delivery is paused without an allowed reader in the room, so a paired
  // bridge is not active either: it is waiting for that member to come back.
  if (!runtime.allowedMemberPresent) {
    return { ...base, state: "waiting-for-member", reason: null };
  }
  return {
    ...base,
    state: config.pairing.state === "paired" ? "active" : "awaiting-pairing",
    reason: null,
  };
};

export class MatrixBridgeConfig extends Context.Service<
  MatrixBridgeConfig,
  {
    /** Internal secret-bearing view for the bridge reactor. Never return or log it. */
    readonly currentConfig: Effect.Effect<Option.Option<MatrixBridgeConfigV1>>;
    /** Sanitized saved connection for privileged clients; never the token. */
    readonly configView: Effect.Effect<Option.Option<MatrixBridgeConfigView>>;
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
    /**
     * Binds the encrypted room the transport created or verified to the
     * connection that produced it. A generation mismatch means the connection is
     * stale, so the caller must abandon the room rather than adopt it.
     */
    readonly recordRoomIfMatches: (expected: {
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly roomId: string;
    }) => Effect.Effect<boolean, MatrixBridgeOperationError>;
    /** Publishes transport lifecycle for one connection generation. */
    readonly reportTransportStateIfMatches: (expected: {
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly transport: MatrixBridgeTransportState;
    }) => Effect.Effect<boolean>;
    /**
     * Records the pairing proof for one connection. The paired state is durably
     * written before it is published, so a failed write leaves the bridge
     * locked and the consumed code cannot activate anything.
     */
    readonly markPairedIfMatches: (expected: {
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly roomId: string;
      readonly userId: string;
      readonly pairedAt: string;
      readonly eventId: string;
    }) => Effect.Effect<boolean, MatrixBridgeOperationError>;
    /**
     * Room membership for one connection. An unexpected joined member pauses
     * outbound delivery and reports `degraded` rather than sharing later T3
     * output outside the allowed list.
     */
    readonly reportRoomMembershipIfMatches: (expected: {
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly allowedMemberPresent: boolean;
      readonly unexpectedMemberPresent: boolean;
    }) => Effect.Effect<boolean>;
    /**
     * Reports a repairable fault for one connection: a consumed code that could
     * not be persisted, or inbound messages dropped under load.
     */
    readonly reportDegradedIfMatches: (expected: {
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly cause: MatrixBridgeDegradedCause;
    }) => Effect.Effect<boolean>;
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
  const runtimeRef = yield* Ref.make(INITIAL_RUNTIME_STATE);

  /** Runtime facts belong to one connection; a new generation starts blank. */
  const runtimeForGeneration = (cryptoStoreGeneration: string) =>
    Ref.updateAndGet(runtimeRef, (runtime) =>
      runtime.cryptoStoreGeneration === cryptoStoreGeneration
        ? runtime
        : { ...INITIAL_RUNTIME_STATE, cryptoStoreGeneration },
    );

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
              configuredAt: DateTime.formatIso(yield* DateTime.now),
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
            yield* Ref.set(runtimeRef, {
              ...INITIAL_RUNTIME_STATE,
              cryptoStoreGeneration: next.cryptoStoreGeneration,
            });
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
        Effect.andThen(Ref.set(runtimeRef, INITIAL_RUNTIME_STATE)),
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

  const recordRoomIfMatches = Effect.fn("MatrixBridgeConfig.recordRoomIfMatches")(
    (expected: {
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly roomId: string;
    }) =>
      mutationSemaphore.withPermits(1)(
        Effect.gen(function* () {
          const current = Option.getOrNull(yield* Ref.get(configRef));
          if (
            current === null ||
            current.cryptoStoreGeneration !== expected.cryptoStoreGeneration
          ) {
            return false;
          }
          // A different room on the same generation means two transports raced
          // or the stored room was replaced; refuse rather than silently move.
          if (current.roomId !== null) return current.roomId === expected.roomId;

          const next: MatrixBridgeConfigV1 = { ...current, roomId: expected.roomId };
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

  const reportTransportStateIfMatches = Effect.fn(
    "MatrixBridgeConfig.reportTransportStateIfMatches",
  )(
    (expected: {
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly transport: MatrixBridgeTransportState;
    }) =>
      mutationSemaphore.withPermits(1)(
        Effect.gen(function* () {
          const current = Option.getOrNull(yield* Ref.get(configRef));
          if (
            current === null ||
            current.cryptoStoreGeneration !== expected.cryptoStoreGeneration
          ) {
            return false;
          }

          const runtime = yield* runtimeForGeneration(expected.cryptoStoreGeneration);
          if (expected.transport.state !== "ready") {
            yield* Ref.set(runtimeRef, { ...runtime, transportReady: false });
            yield* SubscriptionRef.set(statusRef, {
              state: "unavailable",
              ownerThreadId: current.ownerThreadId,
              encryptionReady: false,
              reason: expected.transport.reason,
            });
            return true;
          }

          // A fresh connection only recovers what a connection can: a delivery
          // that could not be sent. A dropped inbound burst and a failed
          // pairing write are unrelated to reconnecting and keep reporting.
          const nextRuntime = {
            ...runtime,
            transportReady: true,
            degradedCauses: withoutDegradedCause(runtime.degradedCauses, "permanent-send-failure"),
          };
          yield* Ref.set(runtimeRef, nextRuntime);
          yield* SubscriptionRef.set(statusRef, connectedStatus(current, nextRuntime));
          return true;
        }),
      ),
  );

  const markPairedIfMatches = Effect.fn("MatrixBridgeConfig.markPairedIfMatches")(
    (expected: {
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly roomId: string;
      readonly userId: string;
      readonly pairedAt: string;
      readonly eventId: string;
    }) =>
      mutationSemaphore.withPermits(1)(
        Effect.gen(function* () {
          const current = Option.getOrNull(yield* Ref.get(configRef));
          if (
            current === null ||
            current.cryptoStoreGeneration !== expected.cryptoStoreGeneration ||
            current.roomId !== expected.roomId ||
            !current.allowedUserIds.includes(expected.userId)
          ) {
            return false;
          }
          if (current.pairing.state === "paired") return current.pairing.userId === expected.userId;

          const next: MatrixBridgeConfigV1 = {
            ...current,
            pairing: {
              state: "paired",
              userId: expected.userId,
              pairedAt: expected.pairedAt,
              eventId: expected.eventId,
            },
          };
          return yield* Effect.uninterruptible(
            Effect.gen(function* () {
              // Written before publication: a failed write raises, leaving the
              // bridge unpaired with the code already spent.
              yield* persist(next);
              yield* Ref.set(configRef, Option.some(next));
              const runtime = yield* runtimeForGeneration(expected.cryptoStoreGeneration);
              // Pairing succeeded, so a failed write of it is history. Other
              // faults are untouched: they have their own recovery.
              const nextRuntime = {
                ...runtime,
                degradedCauses: withoutDegradedCause(
                  runtime.degradedCauses,
                  "pairing-persist-failure",
                ),
              };
              yield* Ref.set(runtimeRef, nextRuntime);
              if (nextRuntime.transportReady) {
                yield* SubscriptionRef.set(statusRef, connectedStatus(next, nextRuntime));
              }
              return true;
            }),
          );
        }),
      ),
  );

  const reportRoomMembershipIfMatches = Effect.fn(
    "MatrixBridgeConfig.reportRoomMembershipIfMatches",
  )(
    (expected: {
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly allowedMemberPresent: boolean;
      readonly unexpectedMemberPresent: boolean;
    }) =>
      mutationSemaphore.withPermits(1)(
        Effect.gen(function* () {
          const current = Option.getOrNull(yield* Ref.get(configRef));
          if (
            current === null ||
            current.cryptoStoreGeneration !== expected.cryptoStoreGeneration
          ) {
            return false;
          }

          const runtime = yield* runtimeForGeneration(expected.cryptoStoreGeneration);
          const nextRuntime = {
            ...runtime,
            allowedMemberPresent: expected.allowedMemberPresent,
            unexpectedMemberPresent: expected.unexpectedMemberPresent,
          };
          yield* Ref.set(runtimeRef, nextRuntime);
          if (nextRuntime.transportReady) {
            yield* SubscriptionRef.set(statusRef, connectedStatus(current, nextRuntime));
          }
          return true;
        }),
      ),
  );

  const reportDegradedIfMatches = Effect.fn("MatrixBridgeConfig.reportDegradedIfMatches")(
    (expected: {
      readonly cryptoStoreGeneration: MatrixBridgeConfigV1["cryptoStoreGeneration"];
      readonly cause: MatrixBridgeDegradedCause;
    }) =>
      mutationSemaphore.withPermits(1)(
        Effect.gen(function* () {
          const current = Option.getOrNull(yield* Ref.get(configRef));
          if (
            current === null ||
            current.cryptoStoreGeneration !== expected.cryptoStoreGeneration
          ) {
            return false;
          }

          const runtime = yield* runtimeForGeneration(expected.cryptoStoreGeneration);
          const nextRuntime = {
            ...runtime,
            degradedCauses: withDegradedCause(runtime.degradedCauses, expected.cause),
          };
          yield* Ref.set(runtimeRef, nextRuntime);
          // Derived, never written directly: an unexpected member outranks this
          // fault, and a transport that is down keeps saying so.
          if (nextRuntime.transportReady) {
            yield* SubscriptionRef.set(statusRef, connectedStatus(current, nextRuntime));
          }
          return true;
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
            current.roomId !== expected.roomId
          ) {
            return false;
          }
          // Not gated on pairing: the baseline records where this installation
          // starts, and a bridge waiting for its code is still past that point.
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
              // Delivery worked, so an earlier send failure is history. A
              // dropped inbound burst is not: nothing recovered those.
              const runtime = yield* runtimeForGeneration(expected.cryptoStoreGeneration);
              if (runtime.degradedCauses.includes("permanent-send-failure")) {
                const recovered = {
                  ...runtime,
                  degradedCauses: withoutDegradedCause(
                    runtime.degradedCauses,
                    "permanent-send-failure",
                  ),
                };
                yield* Ref.set(runtimeRef, recovered);
                if (recovered.transportReady) {
                  yield* SubscriptionRef.set(statusRef, connectedStatus(next, recovered));
                }
              }
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

          const runtime = yield* runtimeForGeneration(expected.cryptoStoreGeneration);
          const nextRuntime = {
            ...runtime,
            degradedCauses: withDegradedCause(runtime.degradedCauses, "permanent-send-failure"),
          };
          yield* Ref.set(runtimeRef, nextRuntime);
          if (nextRuntime.transportReady) {
            yield* SubscriptionRef.set(statusRef, connectedStatus(current, nextRuntime));
          }
          return true;
        }),
      ),
  );

  return MatrixBridgeConfig.of({
    currentConfig: Ref.get(configRef),
    configView: Ref.get(configRef).pipe(Effect.map(Option.map(toMatrixBridgeConfigView))),
    status: SubscriptionRef.get(statusRef),
    statusChanges: SubscriptionRef.changes(statusRef),
    configure,
    disconnect,
    setOwner,
    clearOwnerIfMatches,
    recordRoomIfMatches,
    reportTransportStateIfMatches,
    markPairedIfMatches,
    reportRoomMembershipIfMatches,
    reportDegradedIfMatches,
    initializeDeliveryCheckpointIfMissing,
    markDeliveredIfMatches,
    reportPermanentSendFailureIfMatches,
  });
});

export const layer = Layer.effect(MatrixBridgeConfig, make);
