import { AuthSessionId, EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { HttpServer } from "effect/unstable/http";

import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as McpProviderSession from "./McpProviderSession.ts";

export interface McpCredentialRequest {
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId;
}

export interface McpIssuedCredential {
  readonly config: McpProviderSession.McpProviderSessionConfig;
  readonly expiresAt: number;
}

export interface McpPeerCredentialRequest {
  readonly label?: string;
  readonly sourceSessionId?: AuthSessionId;
  readonly sourceEnvironmentId?: EnvironmentId;
  readonly expiresAt?: number;
  readonly allowedParentThreadIds?: ReadonlyArray<ThreadId>;
  readonly allowedChildThreadIds?: ReadonlyArray<ThreadId>;
}

export interface McpIssuedPeerCredential {
  readonly peerTokenId: string;
  readonly token: string;
  readonly authorizationHeader: string;
  readonly issuedAt: number;
  readonly expiresAt: number | null;
  readonly capabilities: ReadonlyArray<McpInvocationContext.McpCapability>;
}

export class McpPeerTokenStoreError extends Schema.TaggedErrorClass<McpPeerTokenStoreError>()(
  "McpPeerTokenStoreError",
  {
    operation: Schema.Literals(["read", "decode", "persist"]),
    peerTokenStorePath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to ${this.operation} MCP peer tokens at ${this.peerTokenStorePath}.`;
  }
}

export interface McpSessionRegistryShape {
  readonly issue: (request: McpCredentialRequest) => Effect.Effect<McpIssuedCredential>;
  readonly issuePeerToken: (
    request?: McpPeerCredentialRequest,
  ) => Effect.Effect<McpIssuedPeerCredential, McpPeerTokenStoreError>;
  readonly resolve: (
    rawToken: string,
  ) => Effect.Effect<McpInvocationContext.McpInvocationScope | undefined>;
  readonly touchThread: (threadId: ThreadId) => Effect.Effect<void>;
  readonly revokeProviderSession: (providerSessionId: string) => Effect.Effect<void>;
  readonly revokeThread: (threadId: ThreadId) => Effect.Effect<void>;
  readonly revokePeerTokensBySourceSession: (
    sessionId: AuthSessionId,
  ) => Effect.Effect<void, McpPeerTokenStoreError>;
  readonly revokePeerToken: (peerTokenId: string) => Effect.Effect<void, McpPeerTokenStoreError>;
  readonly revokePeerTokensExceptSourceSession: (
    sessionId: AuthSessionId,
  ) => Effect.Effect<void, McpPeerTokenStoreError>;
  readonly revokePeerTokensNotInSourceSessions: (
    activeSessionIds: ReadonlySet<AuthSessionId>,
  ) => Effect.Effect<void, McpPeerTokenStoreError>;
  readonly revokeAllProviderSessions: Effect.Effect<void>;
  readonly revokeAll: Effect.Effect<void, McpPeerTokenStoreError>;
}

export class McpSessionRegistry extends Context.Service<
  McpSessionRegistry,
  McpSessionRegistryShape
>()("t3/mcp/McpSessionRegistry") {}

interface ProviderCredentialRecord {
  readonly credentialKind: "provider-session";
  readonly tokenHash: string;
  readonly scope: McpInvocationContext.ProviderMcpInvocationScope;
  readonly lastUsedAt: number;
}

interface PeerCredentialRecord {
  readonly credentialKind: "peer";
  readonly tokenHash: string;
  readonly scope: McpInvocationContext.PeerMcpInvocationScope;
  readonly lastUsedAt: number;
}

type CredentialRecord = ProviderCredentialRecord | PeerCredentialRecord;

interface RegistryState {
  readonly records: ReadonlyMap<string, CredentialRecord>;
}

export interface McpSessionRegistryOptions {
  readonly now?: () => number;
  readonly idleTimeoutMs?: number;
  readonly peerTokenStorePath?: string;
}

const DEFAULT_MCP_CREDENTIAL_IDLE_TIMEOUT_MS = 30 * 24 * 60 * 60 * 1_000;
const PEER_TOKEN_CAPABILITIES: ReadonlyArray<McpInvocationContext.McpCapability> = [
  "subagent:spawn",
  "subagent:check",
  "subagent:wait",
  "subagent:list",
];

const PersistedPeerTokenRecord = Schema.Struct({
  tokenHash: Schema.String,
  peerTokenId: Schema.String,
  sourceSessionId: Schema.optionalKey(AuthSessionId),
  sourceEnvironmentId: Schema.optionalKey(EnvironmentId),
  environmentId: EnvironmentId,
  capabilities: Schema.Array(McpInvocationContext.McpCapability),
  allowedParentThreadIds: Schema.optionalKey(Schema.Array(ThreadId)),
  allowedChildThreadIds: Schema.optionalKey(Schema.Array(ThreadId)),
  issuedAt: Schema.Number,
  expiresAt: Schema.optionalKey(Schema.Number),
  lastUsedAt: Schema.Number,
});
type PersistedPeerTokenRecord = typeof PersistedPeerTokenRecord.Type;

const PersistedPeerTokens = Schema.Struct({
  version: Schema.Literal(1),
  records: Schema.Array(PersistedPeerTokenRecord),
});
type PersistedPeerTokens = typeof PersistedPeerTokens.Type;

const PersistedPeerTokensJson = Schema.fromJsonString(PersistedPeerTokens);
const decodePersistedPeerTokens = Schema.decodeUnknownEffect(PersistedPeerTokensJson);
const encodePersistedPeerTokens = Schema.encodeEffect(PersistedPeerTokensJson);

const bytesToHex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

const tokenFromBytes = (bytes: Uint8Array): string => Buffer.from(bytes).toString("base64url");

const isExpired = (record: CredentialRecord, timestamp: number): boolean =>
  record.scope.expiresAt !== null && record.scope.expiresAt <= timestamp;

const pruneExpired = (
  records: ReadonlyMap<string, CredentialRecord>,
  timestamp: number,
): ReadonlyMap<string, CredentialRecord> =>
  new Map(Array.from(records).filter(([, record]) => !isExpired(record, timestamp)));

const getHttpMcpEndpointHost = (hostname: string): string => {
  const normalized = hostname.toLowerCase();
  const endpointHostname =
    normalized === "0.0.0.0" || normalized === "::" || normalized === "[::]"
      ? "127.0.0.1"
      : hostname;
  return endpointHostname.includes(":") && !endpointHostname.startsWith("[")
    ? `[${endpointHostname}]`
    : endpointHostname;
};

const makeWithOptions = Effect.fn("McpSessionRegistry.make")(function* (
  options: McpSessionRegistryOptions = {},
) {
  const crypto = yield* Crypto.Crypto;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const config = yield* Effect.serviceOption(ServerConfig.ServerConfig);
  const sessions = yield* Effect.serviceOption(SessionStore.SessionStore);
  const environmentId = yield* environment.getEnvironmentId;
  const httpServer = yield* HttpServer.HttpServer;
  const peerTokenStorePath =
    options.peerTokenStorePath ??
    Option.match(config, {
      onNone: () => undefined,
      onSome: (serverConfig) => path.join(serverConfig.secretsDir, "mcp-peer-tokens.json"),
    });
  const currentTimeMillis = options.now ? Effect.sync(options.now) : Clock.currentTimeMillis;
  const idleTimeoutMs = options.idleTimeoutMs ?? DEFAULT_MCP_CREDENTIAL_IDLE_TIMEOUT_MS;
  const endpoint =
    httpServer.address._tag === "TcpAddress"
      ? `http://${getHttpMcpEndpointHost(httpServer.address.hostname)}:${httpServer.address.port}/mcp`
      : "http://127.0.0.1/mcp";

  const peerTokenStoreError = (operation: McpPeerTokenStoreError["operation"], cause: unknown) =>
    new McpPeerTokenStoreError({
      operation,
      peerTokenStorePath: peerTokenStorePath ?? "<disabled>",
      cause,
    });

  const isNotFound = (cause: PlatformError.PlatformError): boolean =>
    cause.reason._tag === "NotFound";

  const readPersistedPeerRecords = Effect.gen(function* () {
    if (peerTokenStorePath === undefined) return new Map<string, CredentialRecord>();
    const raw = yield* fs.readFileString(peerTokenStorePath).pipe(
      Effect.matchEffect({
        onFailure: (cause) =>
          isNotFound(cause)
            ? Effect.succeed(Option.none<string>())
            : Effect.fail(peerTokenStoreError("read", cause)),
        onSuccess: (contents) => Effect.succeed(Option.some(contents)),
      }),
    );
    if (Option.isNone(raw) || raw.value.trim().length === 0) {
      return new Map<string, CredentialRecord>();
    }
    const persisted = yield* decodePersistedPeerTokens(raw.value).pipe(
      Effect.matchEffect({
        onFailure: () => Effect.succeed({ version: 1, records: [] } satisfies PersistedPeerTokens),
        onSuccess: (decoded) => Effect.succeed(decoded),
      }),
    );
    const timestamp = yield* currentTimeMillis;
    return new Map<string, CredentialRecord>(
      persisted.records
        .filter(
          (record) =>
            record.environmentId === environmentId &&
            (record.expiresAt === undefined || record.expiresAt > timestamp),
        )
        .map((record) => [
          record.tokenHash,
          {
            credentialKind: "peer" as const,
            tokenHash: record.tokenHash,
            scope: {
              credentialKind: "peer" as const,
              environmentId: record.environmentId,
              peerTokenId: record.peerTokenId,
              ...(record.sourceSessionId ? { sourceSessionId: record.sourceSessionId } : {}),
              ...(record.sourceEnvironmentId
                ? { sourceEnvironmentId: record.sourceEnvironmentId }
                : {}),
              capabilities: new Set(record.capabilities),
              ...(record.allowedParentThreadIds
                ? { allowedParentThreadIds: new Set(record.allowedParentThreadIds) }
                : {}),
              ...(record.allowedChildThreadIds
                ? { allowedChildThreadIds: new Set(record.allowedChildThreadIds) }
                : {}),
              issuedAt: record.issuedAt,
              expiresAt: record.expiresAt ?? null,
            },
            lastUsedAt: record.lastUsedAt,
          },
        ]),
    );
  });

  const persistedPeerRecordFromCredential = (
    record: PeerCredentialRecord,
  ): PersistedPeerTokenRecord => ({
    tokenHash: record.tokenHash,
    peerTokenId: record.scope.peerTokenId,
    ...(record.scope.sourceSessionId ? { sourceSessionId: record.scope.sourceSessionId } : {}),
    ...(record.scope.sourceEnvironmentId
      ? { sourceEnvironmentId: record.scope.sourceEnvironmentId }
      : {}),
    environmentId: record.scope.environmentId,
    capabilities: [...record.scope.capabilities].toSorted(),
    ...(record.scope.allowedParentThreadIds
      ? { allowedParentThreadIds: [...record.scope.allowedParentThreadIds].toSorted() }
      : {}),
    ...(record.scope.allowedChildThreadIds
      ? { allowedChildThreadIds: [...record.scope.allowedChildThreadIds].toSorted() }
      : {}),
    issuedAt: record.scope.issuedAt,
    ...(record.scope.expiresAt !== null ? { expiresAt: record.scope.expiresAt } : {}),
    lastUsedAt: record.lastUsedAt,
  });

  const writePeerTokenStore = (persisted: PersistedPeerTokens) =>
    peerTokenStorePath === undefined
      ? Effect.void
      : Effect.scoped(
          Effect.gen(function* () {
            const targetDirectory = path.dirname(peerTokenStorePath);
            yield* fs.makeDirectory(targetDirectory, { recursive: true });
            yield* fs.chmod(targetDirectory, 0o700).pipe(Effect.ignore);
            const tempDirectory = yield* fs.makeTempDirectoryScoped({
              directory: targetDirectory,
              prefix: `${path.basename(peerTokenStorePath)}.`,
            });
            const tempPath = path.join(tempDirectory, "contents.tmp");
            const encoded = yield* encodePersistedPeerTokens(persisted).pipe(
              Effect.mapError((cause) => peerTokenStoreError("persist", cause)),
            );
            const contents = `${encoded}\n`;
            const file = yield* fs.open(tempPath, { flag: "wx", mode: 0o600 });
            yield* file.writeAll(new TextEncoder().encode(contents));
            yield* file.sync;
            yield* fs.chmod(tempPath, 0o600);
            yield* fs.rename(tempPath, peerTokenStorePath);
            yield* fs.chmod(peerTokenStorePath, 0o600);
          }),
        ).pipe(Effect.mapError((cause) => peerTokenStoreError("persist", cause)));

  const persistPeerRecords = (records: ReadonlyMap<string, CredentialRecord>) => {
    const persisted: PersistedPeerTokens = {
      version: 1,
      records: Array.from(records.values())
        .filter((record): record is PeerCredentialRecord => record.credentialKind === "peer")
        .map(persistedPeerRecordFromCredential)
        .toSorted((left, right) => left.peerTokenId.localeCompare(right.peerTokenId)),
    };
    return writePeerTokenStore(persisted);
  };

  const state = yield* readPersistedPeerRecords.pipe(
    Effect.flatMap((records) => SynchronizedRef.make<RegistryState>({ records })),
  );

  const hashToken = (token: string) =>
    crypto
      .digest("SHA-256", new TextEncoder().encode(token))
      .pipe(Effect.map(bytesToHex), Effect.orDie);

  const issue: McpSessionRegistryShape["issue"] = Effect.fn("McpSessionRegistry.issue")(
    function* (request) {
      const issuedAt = yield* currentTimeMillis;
      const providerSessionId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
      const rawToken = yield* crypto.randomBytes(32).pipe(Effect.map(tokenFromBytes), Effect.orDie);
      const tokenHash = yield* hashToken(rawToken);
      const expiresAt = issuedAt + idleTimeoutMs;
      const scope: McpInvocationContext.ProviderMcpInvocationScope = {
        credentialKind: "provider-session",
        environmentId,
        threadId: ThreadId.make(request.threadId),
        providerSessionId,
        providerInstanceId: ProviderInstanceId.make(request.providerInstanceId),
        capabilities: new Set(["thread-management", "notification"]),
        issuedAt,
        expiresAt,
      };
      yield* SynchronizedRef.update(state, ({ records }) => {
        const next = new Map(pruneExpired(records, issuedAt));
        next.set(tokenHash, {
          credentialKind: "provider-session",
          tokenHash,
          scope,
          lastUsedAt: issuedAt,
        });
        return { records: next };
      });
      return {
        config: {
          environmentId,
          threadId: scope.threadId,
          providerSessionId,
          providerInstanceId: scope.providerInstanceId,
          endpoint,
          authorizationHeader: `Bearer ${rawToken}`,
        },
        expiresAt,
      };
    },
  );

  const issuePeerToken: McpSessionRegistryShape["issuePeerToken"] = Effect.fn(
    "McpSessionRegistry.issuePeerToken",
  )(function* (request = {}) {
    const issuedAt = yield* currentTimeMillis;
    const peerTokenId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const rawToken = yield* crypto.randomBytes(32).pipe(Effect.map(tokenFromBytes), Effect.orDie);
    const tokenHash = yield* hashToken(rawToken);
    const scope: McpInvocationContext.PeerMcpInvocationScope = {
      credentialKind: "peer",
      environmentId,
      peerTokenId,
      ...(request.sourceSessionId ? { sourceSessionId: request.sourceSessionId } : {}),
      ...(request.sourceEnvironmentId ? { sourceEnvironmentId: request.sourceEnvironmentId } : {}),
      capabilities: new Set(PEER_TOKEN_CAPABILITIES),
      ...(request.allowedParentThreadIds
        ? { allowedParentThreadIds: new Set(request.allowedParentThreadIds) }
        : {}),
      ...(request.allowedChildThreadIds
        ? { allowedChildThreadIds: new Set(request.allowedChildThreadIds) }
        : {}),
      issuedAt,
      expiresAt: request.expiresAt ?? null,
    };
    const issued = yield* SynchronizedRef.modifyEffect(state, ({ records }) => {
      const next = new Map(pruneExpired(records, issuedAt));
      next.set(tokenHash, {
        credentialKind: "peer",
        tokenHash,
        scope,
        lastUsedAt: issuedAt,
      });
      return persistPeerRecords(next).pipe(
        Effect.as([
          {
            peerTokenId,
            token: rawToken,
            authorizationHeader: `Bearer ${rawToken}`,
            issuedAt,
            expiresAt: scope.expiresAt,
            capabilities: PEER_TOKEN_CAPABILITIES,
          } satisfies McpIssuedPeerCredential,
          { records: next },
        ] as const),
      );
    });
    return issued;
  });

  const resolve: McpSessionRegistryShape["resolve"] = Effect.fn("McpSessionRegistry.resolve")(
    function* (rawToken) {
      if (rawToken.length === 0) return undefined;
      const tokenHash = yield* hashToken(rawToken);
      const timestamp = yield* currentTimeMillis;
      return yield* SynchronizedRef.modifyEffect(state, ({ records }) => {
        const record = records.get(tokenHash);
        if (!record) return Effect.succeed([undefined, { records }] as const);
        const next = new Map(records);
        if (isExpired(record, timestamp)) {
          next.delete(tokenHash);
          return Effect.succeed([undefined, { records: next }] as const);
        }
        if (record.credentialKind === "peer") {
          const refreshPeerRecord = Effect.sync(
            (): readonly [McpInvocationContext.McpInvocationScope | undefined, RegistryState] => {
              next.set(tokenHash, {
                ...record,
                lastUsedAt: timestamp,
              });
              return [record.scope, { records: next }] as const;
            },
          );
          if (record.scope.sourceSessionId === undefined || Option.isNone(sessions)) {
            return refreshPeerRecord;
          }
          return sessions.value.isActive(record.scope.sourceSessionId).pipe(
            Effect.map(Option.some),
            Effect.catch((error) =>
              Effect.logWarning("Failed to validate MCP peer token source session.", {
                peerTokenId: record.scope.peerTokenId,
                sourceSessionId: record.scope.sourceSessionId,
                error,
              }).pipe(Effect.as(Option.none<boolean>())),
            ),
            Effect.flatMap((active) => {
              if (Option.isNone(active)) {
                return Effect.succeed([undefined, { records }] as const);
              }
              if (active.value) return refreshPeerRecord;
              next.delete(tokenHash);
              return persistPeerRecords(next).pipe(
                Effect.catch((error) =>
                  Effect.logWarning("Failed to persist inactive MCP peer token removal.", {
                    peerTokenId: record.scope.peerTokenId,
                    sourceSessionId: record.scope.sourceSessionId,
                    error,
                  }),
                ),
                Effect.as([undefined, { records: next }] as const),
              );
            }),
          );
        }
        const refreshedScope = {
          ...record.scope,
          expiresAt: timestamp + idleTimeoutMs,
        };
        next.set(tokenHash, {
          ...record,
          scope: refreshedScope,
          lastUsedAt: timestamp,
        });
        return Effect.succeed([refreshedScope, { records: next }] as const);
      });
    },
  );

  const touchThread: McpSessionRegistryShape["touchThread"] = Effect.fn(
    "McpSessionRegistry.touchThread",
  )(function* (threadId) {
    const timestamp = yield* currentTimeMillis;
    yield* SynchronizedRef.update(state, ({ records }) => {
      const next = new Map(pruneExpired(records, timestamp));
      for (const [tokenHash, record] of next) {
        if (record.credentialKind !== "provider-session" || record.scope.threadId !== threadId) {
          continue;
        }
        next.set(tokenHash, {
          ...record,
          scope: {
            ...record.scope,
            expiresAt: timestamp + idleTimeoutMs,
          },
          lastUsedAt: timestamp,
        });
      }
      return { records: next };
    });
  });

  const revokeWhere = (predicate: (record: CredentialRecord) => boolean) =>
    SynchronizedRef.update(state, ({ records }) => ({
      records: new Map(Array.from(records).filter(([, record]) => !predicate(record))),
    }));

  const revokeWhereAndPersist = (predicate: (record: CredentialRecord) => boolean) =>
    SynchronizedRef.modifyEffect(state, ({ records }) => {
      const entries = Array.from(records);
      const hasMatchingRecord = entries.some(([, record]) => predicate(record));
      if (!hasMatchingRecord) {
        return Effect.succeed([Option.none<McpPeerTokenStoreError>(), { records }] as const);
      }
      const next = new Map(entries.filter(([, record]) => !predicate(record)));
      return persistPeerRecords(next).pipe(
        Effect.as([Option.none<McpPeerTokenStoreError>(), { records: next }] as const),
        Effect.catch((error) => Effect.succeed([Option.some(error), { records: next }] as const)),
      );
    }).pipe(
      Effect.flatMap((error) => (Option.isSome(error) ? Effect.fail(error.value) : Effect.void)),
    );

  return McpSessionRegistry.of({
    issue,
    issuePeerToken,
    resolve,
    touchThread,
    revokeProviderSession: Effect.fn("McpSessionRegistry.revokeProviderSession")(
      function* (providerSessionId) {
        yield* revokeWhere(
          (record) =>
            record.credentialKind === "provider-session" &&
            record.scope.providerSessionId === providerSessionId,
        );
      },
    ),
    revokePeerTokensBySourceSession: Effect.fn(
      "McpSessionRegistry.revokePeerTokensBySourceSession",
    )(function* (sessionId) {
      yield* revokeWhereAndPersist(
        (record) => record.credentialKind === "peer" && record.scope.sourceSessionId === sessionId,
      );
    }),
    revokePeerToken: Effect.fn("McpSessionRegistry.revokePeerToken")(function* (peerTokenId) {
      yield* revokeWhereAndPersist(
        (record) => record.credentialKind === "peer" && record.scope.peerTokenId === peerTokenId,
      );
    }),
    revokePeerTokensExceptSourceSession: Effect.fn(
      "McpSessionRegistry.revokePeerTokensExceptSourceSession",
    )(function* (sessionId) {
      yield* revokeWhereAndPersist(
        (record) =>
          record.credentialKind === "peer" &&
          record.scope.sourceSessionId !== undefined &&
          record.scope.sourceSessionId !== sessionId,
      );
    }),
    revokePeerTokensNotInSourceSessions: Effect.fn(
      "McpSessionRegistry.revokePeerTokensNotInSourceSessions",
    )(function* (activeSessionIds) {
      yield* revokeWhereAndPersist(
        (record) =>
          record.credentialKind === "peer" &&
          record.scope.sourceSessionId !== undefined &&
          !activeSessionIds.has(record.scope.sourceSessionId),
      );
    }),
    revokeThread: Effect.fn("McpSessionRegistry.revokeThread")(function* (threadId) {
      yield* revokeWhere(
        (record) =>
          record.credentialKind === "provider-session" && record.scope.threadId === threadId,
      );
    }),
    revokeAllProviderSessions: SynchronizedRef.update(state, ({ records }) => ({
      records: new Map(
        Array.from(records).filter(([, record]) => record.credentialKind === "peer"),
      ),
    })),
    revokeAll: SynchronizedRef.modifyEffect(state, () =>
      persistPeerRecords(new Map()).pipe(Effect.as([undefined, { records: new Map() }] as const)),
    ),
  });
});

let activeMcpSessionRegistry: McpSessionRegistryShape | undefined;

const make = Effect.acquireRelease(
  makeWithOptions().pipe(
    Effect.tap((registry) =>
      Effect.sync(() => {
        activeMcpSessionRegistry = registry;
      }),
    ),
  ),
  (registry) =>
    Effect.sync(() => {
      if (activeMcpSessionRegistry === registry) {
        activeMcpSessionRegistry = undefined;
      }
    }),
);

export const layer = Layer.effect(McpSessionRegistry, make);

export const issueActiveMcpCredential = (
  request: McpCredentialRequest,
): Effect.Effect<McpIssuedCredential | undefined> =>
  activeMcpSessionRegistry
    ? activeMcpSessionRegistry
        .revokeThread(request.threadId)
        .pipe(Effect.andThen(activeMcpSessionRegistry.issue(request)))
    : Effect.sync((): McpIssuedCredential | undefined => undefined);

export const issueActiveMcpPeerCredential = (
  request?: McpPeerCredentialRequest,
): Effect.Effect<McpIssuedPeerCredential | undefined, McpPeerTokenStoreError> =>
  activeMcpSessionRegistry
    ? activeMcpSessionRegistry
        .issuePeerToken(request)
        .pipe(Effect.map((issued): McpIssuedPeerCredential | undefined => issued))
    : Effect.sync((): McpIssuedPeerCredential | undefined => undefined);

export const resolveActiveMcpInvocation = (
  rawToken: string,
): Effect.Effect<McpInvocationContext.McpInvocationScope | undefined> =>
  activeMcpSessionRegistry
    ? activeMcpSessionRegistry.resolve(rawToken)
    : Effect.sync((): McpInvocationContext.McpInvocationScope | undefined => undefined);

export const touchActiveMcpThread = (threadId: ThreadId): Effect.Effect<void> =>
  activeMcpSessionRegistry ? activeMcpSessionRegistry.touchThread(threadId) : Effect.void;

export const revokeActiveMcpThread = (threadId: ThreadId): Effect.Effect<void> =>
  activeMcpSessionRegistry ? activeMcpSessionRegistry.revokeThread(threadId) : Effect.void;

export const revokeActiveMcpProviderSession = (providerSessionId: string): Effect.Effect<void> =>
  activeMcpSessionRegistry
    ? activeMcpSessionRegistry.revokeProviderSession(providerSessionId)
    : Effect.void;

export const revokeActiveMcpPeerCredentialsForAuthSession = (
  sessionId: AuthSessionId,
): Effect.Effect<void, McpPeerTokenStoreError> =>
  activeMcpSessionRegistry
    ? activeMcpSessionRegistry.revokePeerTokensBySourceSession(sessionId)
    : Effect.void;

export const revokeActiveMcpPeerCredential = (
  peerTokenId: string,
): Effect.Effect<void, McpPeerTokenStoreError> =>
  activeMcpSessionRegistry ? activeMcpSessionRegistry.revokePeerToken(peerTokenId) : Effect.void;

export const revokeActiveMcpPeerCredentialsExceptAuthSession = (
  sessionId: AuthSessionId,
): Effect.Effect<void, McpPeerTokenStoreError> =>
  activeMcpSessionRegistry
    ? activeMcpSessionRegistry.revokePeerTokensExceptSourceSession(sessionId)
    : Effect.void;

export const revokeActiveMcpPeerCredentialsForInactiveAuthSessions = (
  activeSessionIds: ReadonlySet<AuthSessionId>,
): Effect.Effect<void, McpPeerTokenStoreError> =>
  activeMcpSessionRegistry
    ? activeMcpSessionRegistry.revokePeerTokensNotInSourceSessions(activeSessionIds)
    : Effect.void;

export const revokeAllActiveMcpProviderCredentials = (): Effect.Effect<void> =>
  activeMcpSessionRegistry ? activeMcpSessionRegistry.revokeAllProviderSessions : Effect.void;

export const revokeAllActiveMcpCredentials = (): Effect.Effect<void, McpPeerTokenStoreError> =>
  activeMcpSessionRegistry ? activeMcpSessionRegistry.revokeAll : Effect.void;

/** Exposed for tests. */
export const __testing = {
  DEFAULT_MCP_CREDENTIAL_IDLE_TIMEOUT_MS,
  PEER_TOKEN_CAPABILITIES,
  make: makeWithOptions,
  pruneExpired,
};
