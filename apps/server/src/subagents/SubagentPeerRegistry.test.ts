import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as NodeOS from "node:os";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import * as ServerConfig from "../config.ts";
import * as SubagentPeerRegistry from "./SubagentPeerRegistry.ts";

const bearerCredential = new SubagentPeerRegistry.SubagentPeerBearerCredential({
  token: "peer-token",
});

const encodeLockOwnerJson = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      hostname: Schema.String,
      pid: Schema.Number,
      acquiredAt: Schema.Number,
      processStartToken: Schema.optionalKey(Schema.String),
    }),
  ),
);

const parseProcessStartToken = (contents: string): string => {
  const metadataEnd = contents.lastIndexOf(") ");
  assert.isAtLeast(metadataEnd, 0);
  const fields = contents
    .slice(metadataEnd + 2)
    .trim()
    .split(/\s+/);
  const startTime = fields[19];
  if (startTime === undefined) {
    throw new Error("Missing process start token in /proc stat.");
  }
  return startTime;
};

const makeRegistryLayer = (baseDir: string) =>
  SubagentPeerRegistry.layer.pipe(Layer.provide(ServerConfig.layerTest(process.cwd(), baseDir)));

const addPeer = (
  registry: SubagentPeerRegistry.SubagentPeerRegistry["Service"],
  input: Partial<SubagentPeerRegistry.SubagentPeerAddInput> = {},
) =>
  registry.add({
    alias: "vps",
    environmentId: "env-vps",
    httpBaseUrl: "http://127.0.0.1:3773/api/ignored",
    credential: bearerCredential,
    pairedAt: "2026-07-08T10:00:00.000Z",
    ...input,
  });

it.layer(NodeServices.layer)("SubagentPeerRegistry", (it) => {
  it.effect("adds, lists, and removes peers", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-registry-" });
      const registry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
        Effect.provide(makeRegistryLayer(baseDir)),
      );

      const added = yield* addPeer(registry);
      const listed = yield* registry.list;
      const removed = yield* registry.remove("vps");
      const afterRemove = yield* registry.list;

      assert.equal(added.alias, "vps");
      assert.equal(added.httpBaseUrl, "http://127.0.0.1:3773/");
      assert.equal(added.mcpEndpoint, "http://127.0.0.1:3773/mcp");
      assert.deepEqual(
        listed.map((peer) => peer.alias),
        ["vps"],
      );
      assert.isTrue(removed);
      assert.deepEqual(afterRemove, []);
    }),
  );

  it.effect("rejects duplicate aliases", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-registry-" });
      const registry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
        Effect.provide(makeRegistryLayer(baseDir)),
      );

      yield* addPeer(registry);
      const error = yield* Effect.flip(
        addPeer(registry, {
          environmentId: "env-other",
        }),
      );

      assert.instanceOf(error, SubagentPeerRegistry.SubagentPeerAliasExistsError);
      assert.equal(error.alias, "vps");
    }),
  );

  it.effect("round-trips peers through the persisted JSON file", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-registry-" });
      const firstRegistry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
        Effect.provide(makeRegistryLayer(baseDir)),
      );
      yield* addPeer(firstRegistry, {
        alias: "windows",
        environmentId: "env-windows",
        httpBaseUrl: "https://windows.example.test",
        mcpEndpoint: "https://windows.example.test/custom-mcp",
        lastSeenAt: "2026-07-08T10:01:00.000Z",
      });

      const secondRegistry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
        Effect.provide(makeRegistryLayer(baseDir)),
      );
      const listed = yield* secondRegistry.list;

      assert.lengthOf(listed, 1);
      assert.equal(listed[0]?.alias, "windows");
      assert.equal(listed[0]?.environmentId, "env-windows");
      assert.equal(listed[0]?.httpBaseUrl, "https://windows.example.test/");
      assert.equal(listed[0]?.mcpEndpoint, "https://windows.example.test/custom-mcp");
      assert.equal(listed[0]?.lastSeenAt, "2026-07-08T10:01:00.000Z");
    }),
  );

  it.effect("persists peer credentials with owner-only file permissions", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-registry-" });
      const registry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
        Effect.provide(makeRegistryLayer(baseDir)),
      );

      yield* addPeer(registry);
      const stat = yield* fs.stat(path.join(baseDir, "subagent-peers.json"));

      assert.equal(stat.mode & 0o777, 0o600);
    }),
  );

  it.effect("does not chmod the base directory when persisting peer credentials", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-registry-" });
      yield* fs.chmod(baseDir, 0o755);
      const registry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
        Effect.provide(makeRegistryLayer(baseDir)),
      );

      yield* addPeer(registry);
      const stat = yield* fs.stat(baseDir);

      assert.equal(stat.mode & 0o777, 0o755);
    }),
  );

  it.effect("breaks stale lock directories before mutating the registry", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-registry-" });
      const lockPath = path.join(baseDir, "subagent-peers.json.lock");
      yield* fs.makeDirectory(lockPath, { recursive: true });
      yield* fs.writeFileString(path.join(lockPath, "owner.json"), "{");
      yield* fs.utimes(lockPath, 0, 0);
      yield* TestClock.adjust(Duration.seconds(31));
      const registry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
        Effect.provide(makeRegistryLayer(baseDir)),
      );

      yield* addPeer(registry);
      const listed = yield* registry.list;
      const lockExists = yield* fs.exists(lockPath);

      assert.deepEqual(
        listed.map((peer) => peer.alias),
        ["vps"],
      );
      assert.isFalse(lockExists);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("waits long enough for fresh lock directories to become stale", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-registry-" });
      const lockPath = path.join(baseDir, "subagent-peers.json.lock");
      yield* fs.makeDirectory(lockPath, { recursive: true });
      yield* fs.writeFileString(path.join(lockPath, "owner.json"), "{");
      yield* fs.utimes(lockPath, 0, 0);
      const registry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
        Effect.provide(makeRegistryLayer(baseDir)),
      );

      const addFiber = yield* addPeer(registry).pipe(Effect.forkScoped);
      yield* TestClock.adjust(Duration.seconds(30));
      yield* Fiber.join(addFiber);
      const listed = yield* registry.list;

      assert.deepEqual(
        listed.map((peer) => peer.alias),
        ["vps"],
      );
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it("rejects stale lock snapshots after the lock directory is replaced", () => {
    const staleOwner = Option.some("{");
    const freshOwner = Option.some('{"hostname":"host","pid":1,"acquiredAt":1}\n');

    assert.isFalse(
      SubagentPeerRegistry.__testing.lockSnapshotMatchesMetadata(
        {
          dev: 1,
          ino: Option.some(10),
          mtimeMs: 0,
          ownerContents: staleOwner,
        },
        {
          dev: 1,
          ino: Option.some(11),
          mtimeMs: 0,
          ownerContents: freshOwner,
        },
      ),
    );
  });

  it("accepts unchanged stale lock snapshots before deletion", () => {
    const owner = Option.some("{");

    assert.isTrue(
      SubagentPeerRegistry.__testing.lockSnapshotMatchesMetadata(
        {
          dev: 1,
          ino: Option.some(10),
          mtimeMs: 0,
          ownerContents: owner,
        },
        {
          dev: 1,
          ino: Option.some(10),
          mtimeMs: 0,
          ownerContents: owner,
        },
      ),
    );
  });

  it("treats lock owner path type changes as retryable read races", () => {
    const enotdir = PlatformError.systemError({
      _tag: "BadResource",
      module: "FileSystem",
      method: "readFileString",
      cause: { code: "ENOTDIR" },
    });
    const eisdir = PlatformError.systemError({
      _tag: "BadResource",
      module: "FileSystem",
      method: "readFileString",
      cause: { code: "EISDIR" },
    });

    assert.isTrue(SubagentPeerRegistry.__testing.isLockOwnerReadRace(enotdir));
    assert.isTrue(SubagentPeerRegistry.__testing.isLockOwnerReadRace(eisdir));
  });

  it.effect("does not break stale lock directories still owned by a live local process", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-registry-" });
      const lockPath = path.join(baseDir, "subagent-peers.json.lock");
      yield* fs.makeDirectory(lockPath, { recursive: true });
      const processStartToken = parseProcessStartToken(
        yield* fs.readFileString(`/proc/${process.pid}/stat`),
      );
      const ownerJson = yield* encodeLockOwnerJson({
        hostname: NodeOS.hostname(),
        pid: process.pid,
        acquiredAt: 0,
        processStartToken,
      });
      yield* fs.writeFileString(path.join(lockPath, "owner.json"), `${ownerJson}\n`);
      yield* fs.utimes(lockPath, 0, 0);
      const registry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
        Effect.provide(makeRegistryLayer(baseDir)),
      );

      const addFiber = yield* addPeer(registry).pipe(Effect.forkDetach({ startImmediately: true }));
      yield* TestClock.adjust(Duration.seconds(30));
      yield* Effect.yieldNow;
      const lockExists = yield* fs.exists(lockPath);

      assert.isUndefined(addFiber.pollUnsafe());
      assert.isTrue(lockExists);
      addFiber.interruptUnsafe();
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("breaks ownerless stale lock directories from non-atomic older writers", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-registry-" });
      const lockPath = path.join(baseDir, "subagent-peers.json.lock");
      yield* fs.makeDirectory(lockPath, { recursive: true });
      yield* fs.utimes(lockPath, 0, 0);
      yield* TestClock.adjust(Duration.seconds(30));
      const registry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
        Effect.provide(makeRegistryLayer(baseDir)),
      );

      yield* addPeer(registry);
      const listed = yield* registry.list;
      const lockExists = yield* fs.exists(lockPath);

      assert.deepEqual(
        listed.map((peer) => peer.alias),
        ["vps"],
      );
      assert.isFalse(lockExists);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("does not break stale same-host locks without process start tokens", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-registry-" });
      const lockPath = path.join(baseDir, "subagent-peers.json.lock");
      yield* fs.makeDirectory(lockPath, { recursive: true });
      const ownerJson = yield* encodeLockOwnerJson({
        hostname: NodeOS.hostname(),
        pid: process.pid,
        acquiredAt: 0,
      });
      yield* fs.writeFileString(path.join(lockPath, "owner.json"), `${ownerJson}\n`);
      yield* fs.utimes(lockPath, 0, 0);
      const registry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
        Effect.provide(makeRegistryLayer(baseDir)),
      );

      const addFiber = yield* addPeer(registry).pipe(Effect.forkDetach({ startImmediately: true }));
      yield* TestClock.adjust(Duration.seconds(30));
      yield* Effect.yieldNow;
      const lockExists = yield* fs.exists(lockPath);

      assert.isUndefined(addFiber.pollUnsafe());
      assert.isTrue(lockExists);
      addFiber.interruptUnsafe();
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("breaks stale lock directories with malformed owner metadata", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-registry-" });
      const lockPath = path.join(baseDir, "subagent-peers.json.lock");
      yield* fs.makeDirectory(lockPath, { recursive: true });
      yield* fs.writeFileString(path.join(lockPath, "owner.json"), "{");
      yield* fs.utimes(lockPath, 0, 0);
      yield* TestClock.adjust(Duration.seconds(30));
      const registry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
        Effect.provide(makeRegistryLayer(baseDir)),
      );

      yield* addPeer(registry);
      const listed = yield* registry.list;
      const lockExists = yield* fs.exists(lockPath);

      assert.deepEqual(
        listed.map((peer) => peer.alias),
        ["vps"],
      );
      assert.isFalse(lockExists);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("breaks stale lock directories when the owner pid was reused", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-registry-" });
      const lockPath = path.join(baseDir, "subagent-peers.json.lock");
      yield* fs.makeDirectory(lockPath, { recursive: true });
      const ownerJson = yield* encodeLockOwnerJson({
        hostname: NodeOS.hostname(),
        pid: process.pid,
        acquiredAt: 0,
        processStartToken: "0",
      });
      yield* fs.writeFileString(path.join(lockPath, "owner.json"), `${ownerJson}\n`);
      yield* fs.utimes(lockPath, 0, 0);
      yield* TestClock.adjust(Duration.seconds(30));
      const registry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
        Effect.provide(makeRegistryLayer(baseDir)),
      );

      yield* addPeer(registry);
      const listed = yield* registry.list;
      const lockExists = yield* fs.exists(lockPath);

      assert.deepEqual(
        listed.map((peer) => peer.alias),
        ["vps"],
      );
      assert.isFalse(lockExists);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect(
    "resolves targets by alias before raw environment id and lists known aliases on miss",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-registry-" });
        const registry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
          Effect.provide(makeRegistryLayer(baseDir)),
        );
        yield* addPeer(registry);
        yield* addPeer(registry, {
          alias: "a",
          environmentId: "prod",
          httpBaseUrl: "https://a.example.test",
        });
        yield* addPeer(registry, {
          alias: "prod",
          environmentId: "env-prod",
          httpBaseUrl: "https://prod.example.test",
        });

        const byAlias = yield* registry.resolveTarget("vps");
        const byEnvironment = yield* registry.resolveTarget("env-vps");
        const aliasWins = yield* registry.resolveTarget("prod");
        const missing = yield* Effect.flip(registry.resolveTarget("windows"));

        assert.equal(byAlias.environmentId, "env-vps");
        assert.equal(byEnvironment.alias, "vps");
        assert.equal(aliasWins.alias, "prod");
        assert.instanceOf(missing, SubagentPeerRegistry.SubagentPeerTargetNotFoundError);
        assert.deepEqual(missing.knownAliases, ["a", "prod", "vps"]);
      }),
  );

  it.effect("updates lastSeenAt without creating missing aliases", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-peer-registry-" });
      const registry = yield* SubagentPeerRegistry.SubagentPeerRegistry.pipe(
        Effect.provide(makeRegistryLayer(baseDir)),
      );
      yield* addPeer(registry);

      const updated = yield* registry.updateLastSeen("vps", "2026-07-08T11:00:00.000Z");
      const missing = yield* registry.updateLastSeen("windows", "2026-07-08T11:01:00.000Z");

      assert.equal(Option.getOrThrow(updated).lastSeenAt, "2026-07-08T11:00:00.000Z");
      assert.isTrue(Option.isNone(missing));
    }),
  );
});
