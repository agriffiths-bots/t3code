import { EnvironmentId, IsoDateTime, TrimmedNonEmptyString } from "@t3tools/contracts";
import { normalizeHttpBaseUrl } from "@t3tools/shared/advertisedEndpoint";
import * as NodeOS from "node:os";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import * as ServerConfig from "../config.ts";

export const SubagentPeerAlias = TrimmedNonEmptyString.pipe((schema) =>
  schema.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._-]*$/)),
);
export type SubagentPeerAlias = typeof SubagentPeerAlias.Type;

export class SubagentPeerBearerCredential extends Schema.TaggedClass<SubagentPeerBearerCredential>()(
  "bearer",
  {
    token: TrimmedNonEmptyString,
  },
) {}

export class SubagentPeerCredentialRef extends Schema.TaggedClass<SubagentPeerCredentialRef>()(
  "credential-ref",
  {
    ref: TrimmedNonEmptyString,
  },
) {}

export const SubagentPeerCredential = Schema.Union([
  SubagentPeerBearerCredential,
  SubagentPeerCredentialRef,
]);
export type SubagentPeerCredential = typeof SubagentPeerCredential.Type;

export const SubagentPeerCloudflareAccess = Schema.Union([
  Schema.TaggedStruct("jwt", {
    jwt: TrimmedNonEmptyString,
  }),
  Schema.TaggedStruct("cookie", {
    cookieValue: TrimmedNonEmptyString,
  }),
  Schema.TaggedStruct("service-token", {
    clientId: TrimmedNonEmptyString,
    clientSecret: TrimmedNonEmptyString,
  }),
]);
export type SubagentPeerCloudflareAccess = typeof SubagentPeerCloudflareAccess.Type;

export const SubagentPeer = Schema.Struct({
  alias: SubagentPeerAlias,
  environmentId: EnvironmentId,
  httpBaseUrl: TrimmedNonEmptyString,
  mcpEndpoint: TrimmedNonEmptyString,
  credential: SubagentPeerCredential,
  cfAccess: Schema.optionalKey(SubagentPeerCloudflareAccess),
  pairedAt: IsoDateTime,
  lastSeenAt: Schema.optionalKey(IsoDateTime),
});
export type SubagentPeer = typeof SubagentPeer.Type;

export const PersistedSubagentPeers = Schema.Struct({
  version: Schema.Literal(1),
  peers: Schema.Array(SubagentPeer),
});
export type PersistedSubagentPeers = typeof PersistedSubagentPeers.Type;

const SubagentPeerRegistryLockOwner = Schema.Struct({
  hostname: TrimmedNonEmptyString,
  pid: Schema.Number,
  acquiredAt: Schema.Number,
  processStartToken: Schema.optionalKey(TrimmedNonEmptyString),
});
type SubagentPeerRegistryLockOwner = typeof SubagentPeerRegistryLockOwner.Type;
const SubagentPeerRegistryLockOwnerJson = Schema.fromJsonString(SubagentPeerRegistryLockOwner);

export interface SubagentPeerAddInput {
  readonly alias: string;
  readonly environmentId: string;
  readonly httpBaseUrl: string;
  readonly mcpEndpoint?: string | undefined;
  readonly credential: SubagentPeerCredential;
  readonly cfAccess?: SubagentPeerCloudflareAccess | undefined;
  readonly pairedAt?: string | undefined;
  readonly lastSeenAt?: string | undefined;
}

export class SubagentPeerRegistryError extends Schema.TaggedErrorClass<SubagentPeerRegistryError>()(
  "SubagentPeerRegistryError",
  {
    operation: Schema.Literals(["read", "decode", "persist", "validate", "lock"]),
    peerPath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to ${this.operation} subagent peers at ${this.peerPath}.`;
  }
}

export class SubagentPeerAliasExistsError extends Schema.TaggedErrorClass<SubagentPeerAliasExistsError>()(
  "SubagentPeerAliasExistsError",
  {
    alias: Schema.String,
  },
) {
  override get message(): string {
    return `Subagent peer alias '${this.alias}' already exists.`;
  }
}

export class SubagentPeerAliasNotFoundError extends Schema.TaggedErrorClass<SubagentPeerAliasNotFoundError>()(
  "SubagentPeerAliasNotFoundError",
  {
    alias: Schema.String,
  },
) {
  override get message(): string {
    return `Subagent peer alias '${this.alias}' is not registered.`;
  }
}

export class SubagentPeerTargetNotFoundError extends Schema.TaggedErrorClass<SubagentPeerTargetNotFoundError>()(
  "SubagentPeerTargetNotFoundError",
  {
    target: Schema.String,
    knownAliases: Schema.Array(Schema.String),
  },
) {
  override get message(): string {
    const suffix =
      this.knownAliases.length > 0 ? ` Known aliases: ${this.knownAliases.join(", ")}.` : "";
    return `Subagent peer target '${this.target}' is not registered.${suffix}`;
  }
}

export type SubagentPeerRegistryFailure =
  | SubagentPeerRegistryError
  | SubagentPeerAliasExistsError
  | SubagentPeerAliasNotFoundError
  | SubagentPeerTargetNotFoundError;

export interface SubagentPeerRegistryShape {
  readonly add: (
    input: SubagentPeerAddInput,
  ) => Effect.Effect<SubagentPeer, SubagentPeerRegistryFailure>;
  readonly list: Effect.Effect<ReadonlyArray<SubagentPeer>, SubagentPeerRegistryError>;
  readonly remove: (
    alias: string,
  ) => Effect.Effect<boolean, SubagentPeerRegistryError | SubagentPeerAliasNotFoundError>;
  readonly getByAlias: (
    alias: string,
  ) => Effect.Effect<Option.Option<SubagentPeer>, SubagentPeerRegistryError>;
  readonly resolveTarget: (
    target: string,
  ) => Effect.Effect<SubagentPeer, SubagentPeerRegistryError | SubagentPeerTargetNotFoundError>;
  readonly updateLastSeen: (
    alias: string,
    timestamp?: string,
  ) => Effect.Effect<Option.Option<SubagentPeer>, SubagentPeerRegistryError>;
}

export class SubagentPeerRegistry extends Context.Service<
  SubagentPeerRegistry,
  SubagentPeerRegistryShape
>()("t3/subagents/SubagentPeerRegistry") {}

const decodePersistedPeers = Schema.decodeUnknownEffect(
  Schema.fromJsonString(PersistedSubagentPeers),
);
const decodeLockOwner = Schema.decodeUnknownEffect(SubagentPeerRegistryLockOwnerJson);
const encodeLockOwner = Schema.encodeEffect(SubagentPeerRegistryLockOwnerJson);
const decodePeer = Schema.decodeUnknownEffect(SubagentPeer);

const isNotFound = (cause: PlatformError.PlatformError): boolean =>
  cause.reason._tag === "NotFound";

const errorCode = (cause: unknown): string | undefined =>
  typeof cause === "object" && cause !== null && "code" in cause
    ? String((cause as { readonly code?: unknown }).code)
    : undefined;

const isAlreadyExists = (cause: PlatformError.PlatformError): boolean =>
  cause.reason._tag === "AlreadyExists" || errorCode(cause.reason.cause) === "EEXIST";

const isLockOwnerReadRace = (cause: PlatformError.PlatformError): boolean =>
  cause.reason._tag === "BadResource" ||
  errorCode(cause.reason.cause) === "ENOTDIR" ||
  errorCode(cause.reason.cause) === "EISDIR";

const PEER_REGISTRY_LOCK_RETRY_DELAY = Duration.millis(25);
const PEER_REGISTRY_LOCK_STALE_AFTER = Duration.seconds(30);
const PEER_REGISTRY_LOCK_MAX_ATTEMPTS =
  Math.ceil(
    Duration.toMillis(PEER_REGISTRY_LOCK_STALE_AFTER) /
      Duration.toMillis(PEER_REGISTRY_LOCK_RETRY_DELAY),
  ) + 1;

interface SubagentPeerRegistryLockSnapshot {
  readonly dev: number;
  readonly ino: Option.Option<number>;
  readonly mtimeMs: number;
  readonly ownerContents: Option.Option<string>;
}

const optionNumberMatches = (
  expected: Option.Option<number>,
  actual: Option.Option<number>,
): boolean => Option.isNone(expected) || (Option.isSome(actual) && actual.value === expected.value);

const optionStringMatches = (
  expected: Option.Option<string>,
  actual: Option.Option<string>,
): boolean =>
  Option.isNone(expected)
    ? Option.isNone(actual)
    : Option.isSome(actual) && actual.value === expected.value;

const lockSnapshotMatchesMetadata = (
  expected: SubagentPeerRegistryLockSnapshot,
  actual: SubagentPeerRegistryLockSnapshot,
): boolean =>
  actual.dev === expected.dev &&
  optionNumberMatches(expected.ino, actual.ino) &&
  actual.mtimeMs === expected.mtimeMs &&
  optionStringMatches(expected.ownerContents, actual.ownerContents);

const mcpEndpointFromHttpBaseUrl = (httpBaseUrl: string): string => {
  const url = new URL(httpBaseUrl);
  url.pathname = "/mcp";
  url.search = "";
  url.hash = "";
  return url.toString();
};

const normalizePeerInput = Effect.fn("SubagentPeerRegistry.normalizePeerInput")(function* (
  input: SubagentPeerAddInput,
): Effect.fn.Return<SubagentPeer, SubagentPeerRegistryError> {
  const pairedAt = input.pairedAt ?? DateTime.formatIso(yield* DateTime.now);
  return yield* Effect.try({
    try: () => {
      const httpBaseUrl = normalizeHttpBaseUrl(input.httpBaseUrl);
      const mcpEndpoint =
        input.mcpEndpoint !== undefined
          ? new URL(input.mcpEndpoint).toString()
          : mcpEndpointFromHttpBaseUrl(httpBaseUrl);
      return {
        alias: input.alias,
        environmentId: input.environmentId,
        httpBaseUrl,
        mcpEndpoint,
        credential: input.credential,
        ...(input.cfAccess ? { cfAccess: input.cfAccess } : {}),
        pairedAt,
        ...(input.lastSeenAt ? { lastSeenAt: input.lastSeenAt } : {}),
      };
    },
    catch: (cause) =>
      new SubagentPeerRegistryError({
        operation: "validate",
        peerPath: "<input>",
        cause,
      }),
  }).pipe(
    Effect.flatMap(decodePeer),
    Effect.mapError(
      (cause) =>
        new SubagentPeerRegistryError({
          operation: "validate",
          peerPath: "<input>",
          cause,
        }),
    ),
  );
});

export const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const lock = yield* Semaphore.make(1);
  const peerPath = serverConfig.subagentPeersPath;
  const lockPath = `${peerPath}.lock`;
  const lockOwnerPath = path.join(lockPath, "owner.json");
  const lockHostname = NodeOS.hostname();

  const lockError = (cause: unknown) =>
    new SubagentPeerRegistryError({
      operation: "lock",
      peerPath,
      cause,
    });

  const parseProcessStartToken = (contents: string): Option.Option<string> => {
    const metadataEnd = contents.lastIndexOf(") ");
    if (metadataEnd < 0) return Option.none();
    const fields = contents
      .slice(metadataEnd + 2)
      .trim()
      .split(/\s+/);
    const startTime = fields[19];
    return startTime !== undefined && /^\d+$/.test(startTime)
      ? Option.some(startTime)
      : Option.none();
  };

  const readProcessStartToken = (pid: number) =>
    !Number.isSafeInteger(pid) || pid <= 0
      ? Effect.succeed(Option.none<string>())
      : fs.readFileString(`/proc/${pid}/stat`).pipe(
          Effect.map(parseProcessStartToken),
          Effect.orElseSucceed(() => Option.none<string>()),
        );

  const makeLockOwnerContents = Effect.gen(function* () {
    const acquiredAt = yield* Clock.currentTimeMillis;
    const processStartToken = yield* readProcessStartToken(process.pid);
    const owner: SubagentPeerRegistryLockOwner = {
      hostname: lockHostname,
      pid: process.pid,
      acquiredAt,
      ...(Option.isSome(processStartToken) ? { processStartToken: processStartToken.value } : {}),
    };
    const encoded = yield* encodeLockOwner(owner).pipe(Effect.mapError(lockError));
    return `${encoded}\n`;
  });

  const readLockOwnerContents = fs.stat(lockPath).pipe(
    Effect.matchEffect({
      onFailure: (cause) =>
        isNotFound(cause) ? Effect.succeed(Option.none<string>()) : Effect.fail(lockError(cause)),
      onSuccess: (stat) => {
        const ownerPath = stat.type === "Directory" ? lockOwnerPath : lockPath;
        return fs.readFileString(ownerPath).pipe(
          Effect.matchEffect({
            onFailure: (cause) =>
              isNotFound(cause) || isLockOwnerReadRace(cause)
                ? Effect.succeed(Option.none<string>())
                : Effect.fail(lockError(cause)),
            onSuccess: (contents) => Effect.succeed(Option.some(contents)),
          }),
        );
      },
    }),
  );

  const decodeLockOwnerContents = (contents: Option.Option<string>) =>
    Option.isNone(contents)
      ? Effect.succeed(Option.none<SubagentPeerRegistryLockOwner>())
      : decodeLockOwner(contents.value).pipe(
          Effect.map(Option.some),
          Effect.orElseSucceed(() => Option.none<SubagentPeerRegistryLockOwner>()),
        );

  const lockSnapshotMatches = (input: SubagentPeerRegistryLockSnapshot) =>
    Effect.gen(function* () {
      const stat = yield* fs.stat(lockPath).pipe(
        Effect.matchEffect({
          onFailure: (cause) =>
            isNotFound(cause) ? Effect.succeed(null) : Effect.fail(lockError(cause)),
          onSuccess: Effect.succeed,
        }),
      );
      if (stat === null) return false;
      const mtime = Option.getOrUndefined(stat.mtime);
      if (mtime === undefined) return false;
      const ownerContents = yield* readLockOwnerContents;
      return lockSnapshotMatchesMetadata(input, {
        dev: stat.dev,
        ino: stat.ino,
        mtimeMs: mtime.getTime(),
        ownerContents,
      });
    });

  const isLiveLocalOwner = (owner: SubagentPeerRegistryLockOwner) =>
    Effect.gen(function* () {
      if (owner.hostname !== lockHostname) return false;
      if (owner.processStartToken === undefined) {
        return yield* Effect.sync(() => {
          if (!Number.isSafeInteger(owner.pid) || owner.pid <= 0) return false;
          try {
            process.kill(owner.pid, 0);
            return true;
          } catch (cause) {
            return errorCode(cause) !== "ESRCH";
          }
        });
      }
      const processStartToken = yield* readProcessStartToken(owner.pid);
      return (
        Option.isSome(processStartToken) && processStartToken.value === owner.processStartToken
      );
    });

  const breakStaleLockIfPresent = Effect.fn("SubagentPeerRegistry.breakStaleLockIfPresent")(
    function* () {
      const stat = yield* fs.stat(lockPath).pipe(
        Effect.catch((cause) =>
          isNotFound(cause)
            ? Effect.succeed(null)
            : Effect.fail(
                new SubagentPeerRegistryError({
                  operation: "lock",
                  peerPath,
                  cause,
                }),
              ),
        ),
      );
      if (stat === null) return false;
      const mtime = Option.getOrUndefined(stat.mtime);
      if (mtime === undefined) return false;
      const now = yield* Clock.currentTimeMillis;
      const ageMs = now - mtime.getTime();
      if (ageMs < Duration.toMillis(PEER_REGISTRY_LOCK_STALE_AFTER)) return false;
      const ownerContents = yield* readLockOwnerContents;
      const owner = yield* decodeLockOwnerContents(ownerContents);
      if (Option.isSome(owner) && (yield* isLiveLocalOwner(owner.value))) return false;
      const stillSameLock = yield* lockSnapshotMatches({
        dev: stat.dev,
        ino: stat.ino,
        mtimeMs: mtime.getTime(),
        ownerContents,
      });
      if (!stillSameLock) return false;
      yield* fs.remove(lockPath, { recursive: true }).pipe(
        Effect.mapError(
          (cause) =>
            new SubagentPeerRegistryError({
              operation: "lock",
              peerPath,
              cause,
            }),
        ),
      );
      return true;
    },
  );

  const publishLockFile = (tempLockPath: string) =>
    fs.link(tempLockPath, lockPath).pipe(
      Effect.as(true),
      Effect.catch((cause) =>
        isAlreadyExists(cause) ? Effect.succeed(false) : Effect.fail(lockError(cause)),
      ),
    );

  const tryAcquireFileLock = Effect.gen(function* () {
    const acquiredAt = yield* Clock.currentTimeMillis;
    const nonce = (yield* Random.nextInt).toString(36).replace(/^-/, "n");
    const tempLockPath = `${lockPath}.${process.pid}.${acquiredAt}.${nonce}`;
    const ownerContents = yield* makeLockOwnerContents;
    const published = yield* fs.writeFileString(tempLockPath, ownerContents).pipe(
      Effect.mapError(lockError),
      Effect.tap(() => fs.chmod(tempLockPath, 0o600).pipe(Effect.ignore)),
      Effect.andThen(publishLockFile(tempLockPath)),
      Effect.tapError(() => fs.remove(tempLockPath).pipe(Effect.ignore)),
    );
    yield* fs.remove(tempLockPath).pipe(Effect.ignore);
    return published;
  });

  const readState = Effect.gen(function* () {
    const raw = yield* fs.readFileString(peerPath).pipe(
      Effect.matchEffect({
        onFailure: (cause) =>
          isNotFound(cause)
            ? Effect.succeed(Option.none<string>())
            : Effect.fail(
                new SubagentPeerRegistryError({
                  operation: "read",
                  peerPath,
                  cause,
                }),
              ),
        onSuccess: (contents) => Effect.succeed(Option.some(contents)),
      }),
    );
    if (Option.isNone(raw) || raw.value.trim().length === 0) {
      return { version: 1, peers: [] } satisfies PersistedSubagentPeers;
    }
    return yield* decodePersistedPeers(raw.value).pipe(
      Effect.mapError(
        (cause) =>
          new SubagentPeerRegistryError({
            operation: "decode",
            peerPath,
            cause,
          }),
      ),
    );
  });

  const writePeerFilePrivate = (contents: string) =>
    Effect.scoped(
      Effect.gen(function* () {
        const targetDirectory = path.dirname(peerPath);
        yield* fs.makeDirectory(targetDirectory, { recursive: true });
        const tempDirectory = yield* fs.makeTempDirectoryScoped({
          directory: targetDirectory,
          prefix: `${path.basename(peerPath)}.`,
        });
        const tempPath = path.join(tempDirectory, "contents.tmp");
        const file = yield* fs.open(tempPath, {
          flag: "wx",
          mode: 0o600,
        });
        yield* file.writeAll(new TextEncoder().encode(contents));
        yield* file.sync;
        yield* fs.chmod(tempPath, 0o600);
        yield* fs.rename(tempPath, peerPath);
        yield* fs.chmod(peerPath, 0o600);
      }),
    );

  const persistState = (state: PersistedSubagentPeers) =>
    writePeerFilePrivate(`${JSON.stringify(state, null, 2)}\n`).pipe(
      Effect.mapError(
        (cause) =>
          new SubagentPeerRegistryError({
            operation: "persist",
            peerPath,
            cause,
          }),
      ),
    );

  const acquireFileLock = Effect.gen(function* () {
    for (let attempt = 0; attempt < PEER_REGISTRY_LOCK_MAX_ATTEMPTS; attempt += 1) {
      const acquired = yield* tryAcquireFileLock;
      if (acquired) return;
      const brokeStaleLock = yield* breakStaleLockIfPresent();
      if (brokeStaleLock) {
        const acquiredAfterCleanup = yield* tryAcquireFileLock;
        if (acquiredAfterCleanup) return;
      }
      yield* Effect.sleep(PEER_REGISTRY_LOCK_RETRY_DELAY);
    }
    return yield* lockError(new Error("Timed out waiting for the subagent peer registry lock."));
  });

  const withFileLock = <A, E>(effect: Effect.Effect<A, E>) =>
    Effect.acquireUseRelease(
      acquireFileLock,
      () => effect,
      () => fs.remove(lockPath, { recursive: true }).pipe(Effect.ignore),
    );

  const mutate = <A, E>(
    f: (peers: ReadonlyArray<SubagentPeer>) => Effect.Effect<[A, ReadonlyArray<SubagentPeer>], E>,
  ) =>
    lock.withPermits(1)(
      withFileLock(
        Effect.gen(function* () {
          const state = yield* readState;
          const [result, peers] = yield* f(state.peers);
          const sorted = [...peers].toSorted((left, right) =>
            left.alias.localeCompare(right.alias),
          );
          yield* persistState({ version: 1, peers: sorted });
          return result;
        }),
      ),
    );

  const list: SubagentPeerRegistryShape["list"] = readState.pipe(
    Effect.map((state) => state.peers),
  );

  const add: SubagentPeerRegistryShape["add"] = Effect.fn("SubagentPeerRegistry.add")(
    function* (input) {
      const peer = yield* normalizePeerInput(input);
      return yield* mutate((peers) => {
        if (peers.some((existing) => existing.alias === peer.alias)) {
          return Effect.fail(new SubagentPeerAliasExistsError({ alias: peer.alias }));
        }
        return Effect.succeed([peer, [...peers, peer]]);
      });
    },
  );

  const remove: SubagentPeerRegistryShape["remove"] = Effect.fn("SubagentPeerRegistry.remove")(
    (alias) =>
      mutate((peers) => {
        const next = peers.filter((peer) => peer.alias !== alias);
        if (next.length === peers.length) {
          return Effect.fail(new SubagentPeerAliasNotFoundError({ alias }));
        }
        return Effect.succeed([true, next]);
      }),
  );

  const getByAlias: SubagentPeerRegistryShape["getByAlias"] = (alias) =>
    list.pipe(
      Effect.map((peers) => Option.fromUndefinedOr(peers.find((peer) => peer.alias === alias))),
    );

  const resolveTarget: SubagentPeerRegistryShape["resolveTarget"] = Effect.fn(
    "SubagentPeerRegistry.resolveTarget",
  )(function* (target) {
    const peers = yield* list;
    const peer =
      peers.find((candidate) => candidate.alias === target) ??
      peers.find((candidate) => candidate.environmentId === target);
    if (peer === undefined) {
      return yield* new SubagentPeerTargetNotFoundError({
        target,
        knownAliases: peers.map((candidate) => candidate.alias),
      });
    }
    return peer;
  });

  const updateLastSeen: SubagentPeerRegistryShape["updateLastSeen"] = Effect.fn(
    "SubagentPeerRegistry.updateLastSeen",
  )(function* (alias, timestamp) {
    const lastSeenAt = timestamp ?? DateTime.formatIso(yield* DateTime.now);
    return yield* mutate((peers) => {
      const existing = peers.find((peer) => peer.alias === alias);
      if (existing === undefined) {
        return Effect.succeed([Option.none<SubagentPeer>(), peers]);
      }
      const updated = {
        ...existing,
        lastSeenAt,
      };
      return Effect.succeed([
        Option.some(updated),
        peers.map((peer) => (peer.alias === alias ? updated : peer)),
      ]);
    });
  });

  return SubagentPeerRegistry.of({
    add,
    list,
    remove,
    getByAlias,
    resolveTarget,
    updateLastSeen,
  });
});

export const layer = Layer.effect(SubagentPeerRegistry, make);

export const __testing = {
  isLockOwnerReadRace,
  lockSnapshotMatchesMetadata,
};
