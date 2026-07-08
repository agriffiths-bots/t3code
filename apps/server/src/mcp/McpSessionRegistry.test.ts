import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { AuthSessionId, EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpServer } from "effect/unstable/http";

import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import type { McpCapability } from "./McpInvocationContext.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";

const environmentId = EnvironmentId.make("environment-1");
const mcpCapabilities = () =>
  new Set<McpCapability>(["preview", "thread-management", "notification"]);
const makeFakeHttpServer = (hostname: string, port = 43123) =>
  HttpServer.HttpServer.of({
    address: { _tag: "TcpAddress", hostname, port },
    serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
  });
const fakeHttpServer = makeFakeHttpServer("127.0.0.1");
const fakeEnvironment = ServerEnvironment.ServerEnvironment.of({
  getEnvironmentId: Effect.succeed(environmentId),
  getDescriptor: Effect.die("unused"),
});
const makeFakeSessionStore = (
  isActive: SessionStore.SessionStore["Service"]["isActive"],
): SessionStore.SessionStore["Service"] =>
  SessionStore.SessionStore.of({
    cookieName: "t3.test.sid",
    issue: () => Effect.die("unused"),
    verify: () => Effect.die("unused"),
    issueWebSocketToken: () => Effect.die("unused"),
    verifyWebSocketToken: () => Effect.die("unused"),
    listActive: () => Effect.die("unused"),
    isActive,
    get streamChanges() {
      return Stream.empty;
    },
    revoke: () => Effect.die("unused"),
    revokeAllExcept: () => Effect.die("unused"),
    markConnected: () => Effect.die("unused"),
    markDisconnected: () => Effect.die("unused"),
  });
const PersistedPeerTokenStoreFixture = Schema.Struct({
  version: Schema.Literal(1),
  records: Schema.Array(
    Schema.Struct({
      tokenHash: Schema.String,
      peerTokenId: Schema.String,
      sourceSessionId: Schema.optionalKey(Schema.String),
      environmentId: Schema.String,
      capabilities: Schema.Array(Schema.String),
      allowedParentThreadIds: Schema.optionalKey(Schema.Array(Schema.String)),
      allowedChildThreadIds: Schema.optionalKey(Schema.Array(Schema.String)),
      issuedAt: Schema.Number,
      expiresAt: Schema.optionalKey(Schema.Number),
      lastUsedAt: Schema.Number,
    }),
  ),
});
const decodePeerTokenStoreFixture = Schema.decodeEffect(
  Schema.fromJsonString(PersistedPeerTokenStoreFixture),
);
const encodePeerTokenStoreFixture = Schema.encodeEffect(
  Schema.fromJsonString(PersistedPeerTokenStoreFixture),
);

type RegistryTestOptions = Omit<McpSessionRegistry.McpSessionRegistryOptions, "now"> & {
  readonly sessionStore?: SessionStore.SessionStore["Service"];
};

const makeRegistry = (
  now: () => number,
  httpServer = fakeHttpServer,
  options: RegistryTestOptions = {},
) => {
  const { sessionStore, ...registryOptions } = options;
  const registry = McpSessionRegistry.__testing
    .make({ now, ...registryOptions })
    .pipe(
      Effect.provideService(HttpServer.HttpServer, httpServer),
      Effect.provideService(ServerEnvironment.ServerEnvironment, fakeEnvironment),
      Effect.provide(NodeServices.layer),
    );
  return sessionStore
    ? registry.pipe(Effect.provideService(SessionStore.SessionStore, sessionStore))
    : registry;
};

it.effect("stores only a token hash, resolves the bearer token, and revokes by thread", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp);
    const threadId = ThreadId.make("thread-1");
    const issued = yield* registry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    expect(issued.config.endpoint).toBe("http://127.0.0.1:43123/mcp");
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");
    expect(token.length).toBeGreaterThan(20);

    const resolved = yield* registry.resolve(token);
    expect(resolved?.credentialKind).toBe("provider-session");
    if (resolved?.credentialKind !== "provider-session") {
      throw new Error("Expected a provider-session credential.");
    }
    expect(resolved?.threadId).toBe(threadId);
    expect(resolved?.capabilities.has("preview")).toBe(true);
    expect(resolved?.capabilities.has("thread-management")).toBe(true);
    expect(resolved?.capabilities.has("notification")).toBe(true);

    yield* registry.revokeThread(threadId);
    expect(yield* registry.resolve(token)).toBeUndefined();

    timestamp += 2_000;
  }),
);

it.effect("builds MCP endpoints from the bound server host", () =>
  Effect.gen(function* () {
    const cases = [
      ["100.64.0.40", "http://100.64.0.40:43123/mcp"],
      ["0.0.0.0", "http://127.0.0.1:43123/mcp"],
      ["localhost", "http://localhost:43123/mcp"],
      ["127.0.0.1", "http://127.0.0.1:43123/mcp"],
    ] as const;

    for (const [hostname, expectedEndpoint] of cases) {
      const registry = yield* makeRegistry(() => 1_000, makeFakeHttpServer(hostname));
      const issued = yield* registry.issue({
        threadId: ThreadId.make(`thread-${hostname}`),
        providerInstanceId: ProviderInstanceId.make("codex"),
      });
      expect(issued.config.endpoint).toBe(expectedEndpoint);
    }
  }),
);

it.effect("issues tool capabilities so model-initiated tools stay authorized", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry(() => 1_000);
    const issued = yield* registry.issue({
      threadId: ThreadId.make("thread-subagent"),
      providerInstanceId: ProviderInstanceId.make("claude"),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");
    const resolved = yield* registry.resolve(token);
    expect(resolved?.capabilities.has("thread-management")).toBe(true);
    expect(resolved?.capabilities.has("notification")).toBe(true);
  }),
);

it.effect("keeps live provider-session credentials valid past the legacy expiration window", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp);
    const issued = yield* registry.issue({
      threadId: ThreadId.make("thread-2"),
      providerInstanceId: ProviderInstanceId.make("claude"),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");
    timestamp += 48 * 60 * 60 * 1_000;

    const resolved = yield* registry.resolve(token);
    expect(resolved?.credentialKind).toBe("provider-session");
    if (resolved?.credentialKind !== "provider-session") {
      throw new Error("Expected a provider-session credential.");
    }
    expect(resolved?.threadId).toBe(ThreadId.make("thread-2"));
    expect(resolved?.expiresAt).toBeGreaterThan(timestamp);
    expect(resolved?.expiresAt).toBeGreaterThan(issued.expiresAt);
  }),
);

it.effect("refreshes the idle fallback expiry when a live credential is used", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp, fakeHttpServer, {
      idleTimeoutMs: 1_000,
    });
    const issued = yield* registry.issue({
      threadId: ThreadId.make("thread-refresh"),
      providerInstanceId: ProviderInstanceId.make("claude"),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");

    timestamp += 900;
    const firstResolved = yield* registry.resolve(token);
    expect(firstResolved?.credentialKind).toBe("provider-session");
    if (firstResolved?.credentialKind !== "provider-session") {
      throw new Error("Expected a provider-session credential.");
    }
    expect(firstResolved?.expiresAt).toBe(2_900);

    timestamp += 900;
    const secondResolved = yield* registry.resolve(token);
    expect(secondResolved?.credentialKind).toBe("provider-session");
    if (secondResolved?.credentialKind !== "provider-session") {
      throw new Error("Expected a provider-session credential.");
    }
    expect(secondResolved?.threadId).toBe(ThreadId.make("thread-refresh"));
    expect(secondResolved?.expiresAt).toBe(3_800);
  }),
);

it.effect("rejects idle credentials after the fallback expiry", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp, fakeHttpServer, {
      idleTimeoutMs: 1_000,
    });
    const issued = yield* registry.issue({
      threadId: ThreadId.make("thread-idle-expired"),
      providerInstanceId: ProviderInstanceId.make("claude"),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");

    timestamp += 1_001;

    expect(yield* registry.resolve(token)).toBeUndefined();
  }),
);

it("prunes expired credentials before issuing new ones", () => {
  const liveRecord = {
    credentialKind: "provider-session" as const,
    tokenHash: "live-token-hash",
    lastUsedAt: 1_500,
    scope: {
      credentialKind: "provider-session" as const,
      environmentId,
      threadId: ThreadId.make("thread-live"),
      providerSessionId: "provider-session-live",
      providerInstanceId: ProviderInstanceId.make("claude"),
      capabilities: mcpCapabilities(),
      issuedAt: 1_500,
      expiresAt: 3_000,
    },
  };
  const expiredRecord = {
    credentialKind: "provider-session" as const,
    tokenHash: "expired-token-hash",
    lastUsedAt: 1_000,
    scope: {
      credentialKind: "provider-session" as const,
      environmentId,
      threadId: ThreadId.make("thread-expired"),
      providerSessionId: "provider-session-expired",
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: mcpCapabilities(),
      issuedAt: 1_000,
      expiresAt: 1_999,
    },
  };

  const pruned = McpSessionRegistry.__testing.pruneExpired(
    new Map([
      [liveRecord.tokenHash, liveRecord],
      [expiredRecord.tokenHash, expiredRecord],
    ]),
    2_000,
  );

  expect(Array.from(pruned.keys())).toEqual(["live-token-hash"]);
});

it.effect("rejects provider-session credentials once the session is revoked", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp);
    const threadId = ThreadId.make("thread-ended");
    const issued = yield* registry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("claude"),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");
    timestamp += 48 * 60 * 60 * 1_000;

    yield* registry.revokeThread(threadId);

    expect(yield* registry.resolve(token)).toBeUndefined();
  }),
);

it.effect("issues peer tokens with only the subagent proxy capabilities", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry(() => 1_000);
    const issued = yield* registry.issuePeerToken();
    const resolved = yield* registry.resolve(issued.token);

    expect(issued.authorizationHeader).toBe(`Bearer ${issued.token}`);
    expect(issued.capabilities).toEqual(McpSessionRegistry.__testing.PEER_TOKEN_CAPABILITIES);
    expect(resolved?.credentialKind).toBe("peer");
    if (resolved?.credentialKind !== "peer") {
      throw new Error("Expected a peer-scoped credential.");
    }
    expect("threadId" in resolved).toBe(false);
    expect(resolved.expiresAt).toBeNull();
    expect([...resolved.capabilities].toSorted()).toEqual(
      [...McpSessionRegistry.__testing.PEER_TOKEN_CAPABILITIES].toSorted(),
    );
    expect(resolved.capabilities.has("preview")).toBe(false);
    expect(resolved.capabilities.has("thread-management")).toBe(false);
    expect(resolved.capabilities.has("notification")).toBe(false);
  }),
);

it.effect("does not expire peer tokens on the provider idle timeout", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp, fakeHttpServer, {
      idleTimeoutMs: 1_000,
    });
    const issued = yield* registry.issuePeerToken();

    timestamp += 30 * 24 * 60 * 60 * 1_000;

    const resolved = yield* registry.resolve(issued.token);
    expect(resolved?.credentialKind).toBe("peer");
    expect(resolved?.expiresAt).toBeNull();
  }),
);

it.effect("expires source-session-bound peer tokens at the source session expiry", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp);
    const issued = yield* registry.issuePeerToken({
      sourceSessionId: AuthSessionId.make("auth-session-expiring"),
      expiresAt: 2_000,
    });

    const live = yield* registry.resolve(issued.token);
    expect(live?.credentialKind).toBe("peer");
    expect(live?.expiresAt).toBe(2_000);

    timestamp = 2_001;

    expect(yield* registry.resolve(issued.token)).toBeUndefined();
  }),
);

it.effect("resolves peer tokens after registry reload without persisting raw bearer tokens", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-mcp-peer-token-" });
    const peerTokenStorePath = `${baseDir}/mcp-peer-tokens.json`;
    let timestamp = 1_000;
    const firstRegistry = yield* makeRegistry(() => timestamp, fakeHttpServer, {
      peerTokenStorePath,
    });
    const allowedParentThreadId = ThreadId.make("parent-reload");
    const allowedChildThreadId = ThreadId.make("child-reload");
    const issued = yield* firstRegistry.issuePeerToken({
      allowedParentThreadIds: [allowedParentThreadId],
      allowedChildThreadIds: [allowedChildThreadId],
    });
    timestamp += 5_000;

    const secondRegistry = yield* makeRegistry(() => timestamp, fakeHttpServer, {
      peerTokenStorePath,
      idleTimeoutMs: 1,
    });
    const resolved = yield* secondRegistry.resolve(issued.token);
    const persisted = yield* fs.readFileString(peerTokenStorePath);

    expect(resolved?.credentialKind).toBe("peer");
    if (resolved?.credentialKind !== "peer") {
      throw new Error("Expected persisted peer-scoped credential.");
    }
    expect(resolved.peerTokenId).toBe(issued.peerTokenId);
    expect(resolved.allowedParentThreadIds?.has(allowedParentThreadId)).toBe(true);
    expect(resolved.allowedChildThreadIds?.has(allowedChildThreadId)).toBe(true);
    expect(resolved.capabilities.has("subagent:spawn")).toBe(true);
    expect(persisted).toContain(issued.peerTokenId);
    expect(persisted).not.toContain(issued.token);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("does not reload expired source-session-bound peer tokens", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-mcp-peer-expired-" });
    const peerTokenStorePath = `${baseDir}/mcp-peer-tokens.json`;
    let timestamp = 1_000;
    const firstRegistry = yield* makeRegistry(() => timestamp, fakeHttpServer, {
      peerTokenStorePath,
    });
    const issued = yield* firstRegistry.issuePeerToken({
      sourceSessionId: AuthSessionId.make("auth-session-reload-expired"),
      expiresAt: 2_000,
    });
    timestamp = 2_001;

    const secondRegistry = yield* makeRegistry(() => timestamp, fakeHttpServer, {
      peerTokenStorePath,
    });

    expect(yield* secondRegistry.resolve(issued.token)).toBeUndefined();
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("starts with no peer tokens when the persisted token store is corrupt", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-mcp-peer-corrupt-" });
    const peerTokenStorePath = `${baseDir}/mcp-peer-tokens.json`;
    yield* fs.writeFileString(peerTokenStorePath, "{not-json");

    const registry = yield* makeRegistry(() => 1_000, fakeHttpServer, {
      peerTokenStorePath,
    });
    const issued = yield* registry.issuePeerToken();

    expect((yield* registry.resolve(issued.token))?.credentialKind).toBe("peer");
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("rejects persisted peer tokens from a previous server environment", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-mcp-peer-env-" });
    const peerTokenStorePath = `${baseDir}/mcp-peer-tokens.json`;
    const firstRegistry = yield* makeRegistry(() => 1_000, fakeHttpServer, {
      peerTokenStorePath,
    });
    const issued = yield* firstRegistry.issuePeerToken();
    const persisted = yield* fs.readFileString(peerTokenStorePath);
    const parsed = yield* decodePeerTokenStoreFixture(persisted);
    const rewritten = {
      ...parsed,
      records: parsed.records.map((record) => ({
        ...record,
        environmentId: EnvironmentId.make("environment-previous"),
      })),
    };
    yield* fs.writeFileString(peerTokenStorePath, yield* encodePeerTokenStoreFixture(rewritten));

    const secondRegistry = yield* makeRegistry(() => 2_000, fakeHttpServer, {
      peerTokenStorePath,
    });

    expect(yield* secondRegistry.resolve(issued.token)).toBeUndefined();
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("keeps peer tokens when only provider-session credentials are revoked", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry(() => 1_000);
    const provider = yield* registry.issue({
      threadId: ThreadId.make("thread-provider-revoke-all"),
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    const peer = yield* registry.issuePeerToken();
    const providerToken = provider.config.authorizationHeader.replace(/^Bearer\s+/, "");

    yield* registry.revokeAllProviderSessions;

    expect(yield* registry.resolve(providerToken)).toBeUndefined();
    expect((yield* registry.resolve(peer.token))?.credentialKind).toBe("peer");
  }),
);

it.effect("revokes source-session-bound peer tokens and updates the persisted store", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-mcp-peer-session-revoke-" });
    const peerTokenStorePath = `${baseDir}/mcp-peer-tokens.json`;
    const registry = yield* makeRegistry(() => 1_000, fakeHttpServer, {
      peerTokenStorePath,
    });
    const sessionId = AuthSessionId.make("auth-session-revoked");
    const peer = yield* registry.issuePeerToken({
      sourceSessionId: sessionId,
      expiresAt: 30_000,
    });
    const durablePeer = yield* registry.issuePeerToken();

    yield* registry.revokePeerTokensBySourceSession(sessionId);

    const persisted = yield* fs.readFileString(peerTokenStorePath);
    expect(yield* registry.resolve(peer.token)).toBeUndefined();
    expect((yield* registry.resolve(durablePeer.token))?.credentialKind).toBe("peer");
    expect(persisted).not.toContain(peer.peerTokenId);
    expect(persisted).toContain(durablePeer.peerTokenId);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "rejects source-session-bound peer tokens after the source auth session is inactive",
  () =>
    Effect.gen(function* () {
      const sessionId = AuthSessionId.make("auth-session-source-bound");
      let sourceSessionActive = true;
      const registry = yield* makeRegistry(() => 1_000, fakeHttpServer, {
        sessionStore: makeFakeSessionStore((requestedSessionId) =>
          Effect.succeed(requestedSessionId === sessionId && sourceSessionActive),
        ),
      });
      const peer = yield* registry.issuePeerToken({
        sourceSessionId: sessionId,
        expiresAt: 30_000,
      });

      expect((yield* registry.resolve(peer.token))?.credentialKind).toBe("peer");

      sourceSessionActive = false;

      expect(yield* registry.resolve(peer.token)).toBeUndefined();
    }),
);

it.effect("revokes peer tokens for other auth sessions only", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry(() => 1_000);
    const currentSessionId = AuthSessionId.make("auth-session-current");
    const otherSessionId = AuthSessionId.make("auth-session-other");
    const currentPeer = yield* registry.issuePeerToken({
      sourceSessionId: currentSessionId,
      expiresAt: 30_000,
    });
    const otherPeer = yield* registry.issuePeerToken({
      sourceSessionId: otherSessionId,
      expiresAt: 30_000,
    });
    const durablePeer = yield* registry.issuePeerToken();

    yield* registry.revokePeerTokensExceptSourceSession(currentSessionId);

    expect((yield* registry.resolve(currentPeer.token))?.credentialKind).toBe("peer");
    expect(yield* registry.resolve(otherPeer.token)).toBeUndefined();
    expect((yield* registry.resolve(durablePeer.token))?.credentialKind).toBe("peer");
  }),
);

it.effect("revokes peer tokens whose source auth sessions are no longer active", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry(() => 1_000);
    const activeSessionId = AuthSessionId.make("auth-session-still-active");
    const inactiveSessionId = AuthSessionId.make("auth-session-inactive");
    const activePeer = yield* registry.issuePeerToken({
      sourceSessionId: activeSessionId,
      expiresAt: 30_000,
    });
    const inactivePeer = yield* registry.issuePeerToken({
      sourceSessionId: inactiveSessionId,
      expiresAt: 30_000,
    });
    const durablePeer = yield* registry.issuePeerToken();

    yield* registry.revokePeerTokensNotInSourceSessions(new Set([activeSessionId]));

    expect((yield* registry.resolve(activePeer.token))?.credentialKind).toBe("peer");
    expect(yield* registry.resolve(inactivePeer.token)).toBeUndefined();
    expect((yield* registry.resolve(durablePeer.token))?.credentialKind).toBe("peer");
  }),
);

it.effect("does not create the persisted peer token store for no-op peer revocation", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-mcp-peer-noop-revoke-" });
    const peerTokenStorePath = `${baseDir}/mcp-peer-tokens.json`;
    const registry = yield* makeRegistry(() => 1_000, fakeHttpServer, {
      peerTokenStorePath,
    });

    yield* registry.revokePeerTokensBySourceSession(AuthSessionId.make("auth-session-missing"));

    expect(yield* fs.exists(peerTokenStorePath)).toBe(false);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("revokes all credentials and removes persisted peer tokens", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-mcp-peer-revoke-all-" });
    const peerTokenStorePath = `${baseDir}/mcp-peer-tokens.json`;
    const registry = yield* makeRegistry(() => 1_000, fakeHttpServer, {
      peerTokenStorePath,
    });
    const peer = yield* registry.issuePeerToken();

    yield* registry.revokeAll;

    const persisted = yield* fs.readFileString(peerTokenStorePath);
    expect(yield* registry.resolve(peer.token)).toBeUndefined();
    expect(persisted).not.toContain(peer.peerTokenId);
  }).pipe(Effect.provide(NodeServices.layer)),
);
