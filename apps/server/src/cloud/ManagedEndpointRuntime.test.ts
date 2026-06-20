import { describe, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as RelayClient from "@t3tools/shared/relayClient";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ManagedEndpointRuntime from "./ManagedEndpointRuntime.ts";

const relayClientAvailableLayer = Layer.succeed(
  RelayClient.RelayClient,
  RelayClient.RelayClient.of({
    resolve: Effect.succeed({
      status: "available",
      executablePath: "cloudflared",
      source: "path",
      version: RelayClient.CLOUDFLARED_VERSION,
    }),
    install: Effect.die("unused"),
    installWithProgress: () => Effect.die("unused"),
  }),
);

const runtimeDependencies = (
  spawner: ReturnType<typeof ChildProcessSpawner.make>,
  relayClientLayer = relayClientAvailableLayer,
  getSecret: ServerSecretStore.ServerSecretStore["Service"]["get"] = () =>
    Effect.succeed(Option.none()),
) =>
  Layer.mergeAll(
    Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
    relayClientLayer,
    Layer.mock(ServerSecretStore.ServerSecretStore)({
      get: getSecret,
    }),
  );

const buildCloudManagedEndpointRuntime = (
  spawner: ReturnType<typeof ChildProcessSpawner.make>,
  relayClientLayer = relayClientAvailableLayer,
  getSecret?: ServerSecretStore.ServerSecretStore["Service"]["get"],
) =>
  Effect.gen(function* () {
    const context = yield* Layer.build(
      ManagedEndpointRuntime.layer.pipe(
        Layer.provide(runtimeDependencies(spawner, relayClientLayer, getSecret)),
      ),
    );
    return yield* Effect.service(ManagedEndpointRuntime.CloudManagedEndpointRuntime).pipe(
      Effect.provide(context),
    );
  });

function makeHandle(input: {
  readonly pid: number;
  readonly onKill: () => void;
  readonly isRunning?: () => boolean;
  readonly isRunningEffect?: ChildProcessSpawner.ChildProcessHandle["isRunning"];
  readonly exitCode?: Effect.Effect<ChildProcessSpawner.ExitCode>;
  readonly output?: string;
}) {
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(input.pid),
    exitCode: input.exitCode ?? Effect.never,
    isRunning: input.isRunningEffect ?? Effect.sync(() => input.isRunning?.() ?? true),
    kill: () =>
      Effect.sync(() => {
        input.onKill();
      }),
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: Stream.empty,
    stderr: Stream.empty,
    all:
      input.output === undefined
        ? Stream.empty
        : Stream.make(new TextEncoder().encode(input.output)),
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

describe("CloudManagedEndpointRuntime", () => {
  it("classifies Cloudflare connection and warning output", () => {
    expect(
      ManagedEndpointRuntime.classifyRelayClientOutput(
        "2026-06-17T02:00:00Z INF Registered tunnel connection connIndex=0",
      ),
    ).toBe("connected");
    expect(
      ManagedEndpointRuntime.classifyRelayClientOutput(
        "2026-06-17T02:00:00Z ERR Failed to serve tunnel connection",
      ),
    ).toBe("warning");
    expect(
      ManagedEndpointRuntime.classifyRelayClientOutput(
        "2026-06-17T02:00:00Z INF Starting metrics server",
      ),
    ).toBe("debug");
  });

  it.effect("starts, deduplicates, rotates, and stops the Cloudflare connector", () =>
    Effect.gen(function* () {
      const spawned: Array<ChildProcess.StandardCommand> = [];
      const killed: Array<number> = [];
      let nextPid = 100;
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          if (!ChildProcess.isStandardCommand(command)) {
            throw new Error("Expected standard command.");
          }
          spawned.push(command);
          const pid = nextPid;
          nextPid += 1;
          const handle = makeHandle({
            pid,
            onKill: () => {
              killed.push(pid);
            },
          });
          yield* Effect.addFinalizer(() => handle.kill().pipe(Effect.ignore));
          return handle;
        }),
      );
      const runtime = yield* buildCloudManagedEndpointRuntime(spawner);

      yield* runtime.applyConfig({
        providerKind: "cloudflare_tunnel",
        connectorToken: "token-1",
        tunnelId: "tunnel-1",
        tunnelName: "t3-code-env-1",
      });
      yield* runtime.applyConfig({
        providerKind: "cloudflare_tunnel",
        connectorToken: "token-1",
        tunnelId: "tunnel-1",
        tunnelName: "t3-code-env-1",
      });
      yield* runtime.applyConfig({
        providerKind: "cloudflare_tunnel",
        connectorToken: "token-2",
        tunnelId: "tunnel-1",
        tunnelName: "t3-code-env-1",
      });
      const stopped = yield* runtime.applyConfig(null);

      expect(spawned.map((command) => command.command)).toEqual(["cloudflared", "cloudflared"]);
      expect(spawned.map((command) => command.args)).toEqual([
        ["tunnel", "run"],
        ["tunnel", "run"],
      ]);
      expect(spawned.map((command) => command.options.env?.TUNNEL_TOKEN)).toEqual([
        "token-1",
        "token-2",
      ]);
      expect(spawned.map((command) => command.options.stdout)).toEqual(["pipe", "pipe"]);
      expect(spawned.map((command) => command.options.stderr)).toEqual(["pipe", "pipe"]);
      expect(spawned.map((command) => command.options.detached)).toEqual([false, false]);
      expect(spawned.map((command) => command.options.shell)).toEqual([false, false]);
      expect(killed).toEqual([100, 101]);
      expect(stopped).toEqual({ status: "disabled" });
    }),
  );

  it.effect("stops an active connector when a non-Cloudflare runtime config is applied", () =>
    Effect.gen(function* () {
      const killed: Array<number> = [];
      const spawner = ChildProcessSpawner.make(() =>
        Effect.gen(function* () {
          const handle = makeHandle({
            pid: 200,
            onKill: () => {
              killed.push(200);
            },
          });
          yield* Effect.addFinalizer(() => handle.kill().pipe(Effect.ignore));
          return handle;
        }),
      );
      const runtime = yield* buildCloudManagedEndpointRuntime(spawner);

      const started = yield* runtime.applyConfig({
        providerKind: "cloudflare_tunnel",
        connectorToken: "token",
      });
      const unsupported = yield* runtime.applyConfig({
        providerKind: "manual",
        connectorToken: "manual-token",
      });

      expect(started.status).toBe("running");
      expect(unsupported).toEqual({ status: "unsupported", providerKind: "manual" });
      expect(killed).toEqual([200]);
    }),
  );

  it.effect("restarts after exit or a failed active-process probe", () => {
    const logMessages: unknown[] = [];
    const logger = Logger.make(({ message }) => {
      logMessages.push(message);
    });

    return Effect.gen(function* () {
      const spawned: Array<number> = [];
      const killed: Array<number> = [];
      const probeCause = PlatformError.systemError({
        _tag: "PermissionDenied",
        module: "ChildProcess",
        method: "isRunning",
        description: "process state is unavailable",
      });
      let activeProbe: ChildProcessSpawner.ChildProcessHandle["isRunning"] = Effect.succeed(true);
      const spawner = ChildProcessSpawner.make(() =>
        Effect.gen(function* () {
          const pid = 300 + spawned.length;
          spawned.push(pid);
          const handle = makeHandle({
            pid,
            isRunningEffect: Effect.suspend(() => activeProbe),
            onKill: () => {
              killed.push(pid);
            },
          });
          yield* Effect.addFinalizer(() => handle.kill().pipe(Effect.ignore));
          return handle;
        }),
      );
      const runtime = yield* buildCloudManagedEndpointRuntime(spawner);
      const config = {
        providerKind: "cloudflare_tunnel" as const,
        connectorToken: "token",
        tunnelId: "tunnel-1",
      };

      const first = yield* runtime.applyConfig(config);
      activeProbe = Effect.succeed(false);
      const second = yield* runtime.applyConfig(config);
      activeProbe = Effect.fail(probeCause);
      const third = yield* runtime.applyConfig(config);

      expect(first).toMatchObject({ status: "running", pid: 300 });
      expect(second).toMatchObject({ status: "running", pid: 301 });
      expect(third).toMatchObject({ status: "running", pid: 302 });
      expect(spawned).toEqual([300, 301, 302]);
      expect(killed).toEqual([300, 301]);

      const warning = logMessages.find(
        (message) =>
          Array.isArray(message) && message[0] === "Failed to inspect relay client process",
      );
      expect(warning).toBeDefined();
      if (!Array.isArray(warning)) return;
      expect(warning[1]).toMatchObject({
        pid: 301,
        tunnelId: "tunnel-1",
      });
      expect((warning[1] as { cause: unknown }).cause).toBe(probeCause);
    }).pipe(Effect.provide(Logger.layer([logger], { mergeWithExisting: false })));
  });

  it.effect("does not copy relay client output into log annotations", () => {
    const connectorToken = "connector-token-sentinel";
    const signedUrl = "https://user:password@example.com/private?token=secret#fragment";
    const output = `ERR failed request ${signedUrl} ${connectorToken}`;
    const logMessages: unknown[] = [];
    let resolveObserved!: () => void;
    const observed = new Promise<void>((resolve) => {
      resolveObserved = resolve;
    });
    const logger = Logger.make(({ message }) => {
      logMessages.push(message);
      if (Array.isArray(message) && message[0] === "Relay client reported a transport warning") {
        resolveObserved();
      }
    });
    const spawner = ChildProcessSpawner.make(() =>
      Effect.gen(function* () {
        const handle = makeHandle({
          pid: 350,
          output,
          onKill: () => undefined,
        });
        yield* Effect.addFinalizer(() => handle.kill().pipe(Effect.ignore));
        return handle;
      }),
    );

    return Effect.gen(function* () {
      const runtime = yield* buildCloudManagedEndpointRuntime(spawner);
      yield* runtime.applyConfig({
        providerKind: "cloudflare_tunnel",
        connectorToken,
        tunnelId: "tunnel-1",
      });
      yield* Effect.promise(() => observed);

      const warning = logMessages.find(
        (message) =>
          Array.isArray(message) && message[0] === "Relay client reported a transport warning",
      );
      expect(warning).toBeDefined();
      if (!Array.isArray(warning)) return;
      expect(warning[1]).toMatchObject({
        pid: 350,
        tunnelId: "tunnel-1",
        outputLength: output.length,
      });
      expect(warning[1]).not.toHaveProperty("output");
      const diagnosticText = Object.values(warning[1] as Record<string, unknown>)
        .map(String)
        .join("\n");
      expect(diagnosticText).not.toContain(connectorToken);
      expect(diagnosticText).not.toContain(signedUrl);
      expect(diagnosticText).not.toContain("user:password");
      expect(diagnosticText).not.toContain("token=secret");
    }).pipe(Effect.provide(Logger.layer([logger], { mergeWithExisting: false })));
  });

  it.effect("supervises the active connector and restarts it after process exit", () =>
    Effect.gen(function* () {
      const spawned: Array<number> = [];
      const killed: Array<number> = [];
      const firstExit = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
      const secondSpawned = yield* Deferred.make<void>();
      const spawner = ChildProcessSpawner.make(() =>
        Effect.gen(function* () {
          const pid = spawned.length === 0 ? 400 : 401;
          spawned.push(pid);
          if (pid === 401) {
            yield* Deferred.succeed(secondSpawned, undefined);
          }
          const handle = makeHandle({
            pid,
            exitCode:
              pid === 400
                ? Deferred.await(firstExit)
                : (Effect.never as Effect.Effect<ChildProcessSpawner.ExitCode>),
            onKill: () => {
              killed.push(pid);
            },
          });
          yield* Effect.addFinalizer(() => handle.kill().pipe(Effect.ignore));
          return handle;
        }),
      );
      const runtime = yield* buildCloudManagedEndpointRuntime(spawner);

      const started = yield* runtime.applyConfig({
        providerKind: "cloudflare_tunnel",
        connectorToken: "token",
        tunnelId: "tunnel-1",
      });
      yield* Deferred.succeed(firstExit, ChildProcessSpawner.ExitCode(1));
      yield* Deferred.await(secondSpawned);

      expect(started).toMatchObject({ status: "running", pid: 400 });
      expect(spawned).toEqual([400, 401]);
      expect(killed).toEqual([400]);
    }),
  );

  it.effect("serializes concurrent connector config changes", () =>
    Effect.gen(function* () {
      const spawned: Array<number> = [];
      const killed: Array<number> = [];
      const firstSpawnEntered = yield* Deferred.make<void>();
      const releaseFirstSpawn = yield* Deferred.make<void>();
      const spawner = ChildProcessSpawner.make(() =>
        Effect.gen(function* () {
          const pid = 500 + spawned.length;
          spawned.push(pid);
          if (pid === 500) {
            yield* Deferred.succeed(firstSpawnEntered, undefined);
            yield* Deferred.await(releaseFirstSpawn);
          }
          const handle = makeHandle({
            pid,
            onKill: () => {
              killed.push(pid);
            },
          });
          yield* Effect.addFinalizer(() => handle.kill().pipe(Effect.ignore));
          return handle;
        }),
      );
      const runtime = yield* buildCloudManagedEndpointRuntime(spawner);

      const first = yield* runtime
        .applyConfig({
          providerKind: "cloudflare_tunnel",
          connectorToken: "token-1",
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(firstSpawnEntered);
      const second = yield* runtime
        .applyConfig({
          providerKind: "cloudflare_tunnel",
          connectorToken: "token-2",
        })
        .pipe(Effect.forkChild);
      yield* Deferred.succeed(releaseFirstSpawn, undefined);

      yield* Fiber.join(first);
      const status = yield* Fiber.join(second);

      expect(status).toMatchObject({ status: "running", pid: 501 });
      expect(spawned).toEqual([500, 501]);
      expect(killed).toEqual([500]);
    }),
  );

  it.effect("reports connector spawn failures", () =>
    Effect.gen(function* () {
      const spawner = ChildProcessSpawner.make(() =>
        Effect.fail(
          PlatformError.systemError({
            _tag: "NotFound",
            module: "ChildProcess",
            method: "spawn",
            description: "cloudflared missing",
          }),
        ),
      );
      const runtime = yield* buildCloudManagedEndpointRuntime(spawner);

      const status = yield* runtime.applyConfig({
        providerKind: "cloudflare_tunnel",
        connectorToken: "token",
        tunnelId: "tunnel-1",
      });

      expect(status).toMatchObject({
        status: "failed",
        providerKind: "cloudflare_tunnel",
        reason: "Failed to start the relay client.",
        tunnelId: "tunnel-1",
      });
    }),
  );

  it.effect("retains malformed persisted runtime configuration diagnostics", () => {
    const logMessages: unknown[] = [];
    const logger = Logger.make(({ message }) => {
      logMessages.push(message);
    });
    const spawn = vi.fn();
    const spawner = ChildProcessSpawner.make(spawn);

    return Effect.gen(function* () {
      yield* buildCloudManagedEndpointRuntime(spawner, relayClientAvailableLayer, () =>
        Effect.succeed(Option.some(new TextEncoder().encode("not-json"))),
      );

      expect(spawn).not.toHaveBeenCalled();
      const warning = logMessages.find(
        (message) =>
          Array.isArray(message) && message[0] === "Failed to read managed endpoint runtime config",
      );
      expect(warning).toBeDefined();
      if (!Array.isArray(warning)) return;
      const cause = (warning[1] as { cause: unknown }).cause;
      expect(cause).toMatchObject({
        _tag: "CloudManagedEndpointRuntimeConfigDecodeError",
        resource: "cloud-endpoint-runtime-config",
      });
      expect(
        (cause as ManagedEndpointRuntime.CloudManagedEndpointRuntimeConfigDecodeError).cause,
      ).toBeDefined();
    }).pipe(Effect.provide(Logger.layer([logger], { mergeWithExisting: false })));
  });

  it.effect("reports a missing relay client executable without spawning", () =>
    Effect.gen(function* () {
      const spawn = vi.fn();
      const spawner = ChildProcessSpawner.make(spawn);
      const runtime = yield* buildCloudManagedEndpointRuntime(
        spawner,
        Layer.succeed(
          RelayClient.RelayClient,
          RelayClient.RelayClient.of({
            resolve: Effect.succeed({
              status: "missing",
              version: RelayClient.CLOUDFLARED_VERSION,
            }),
            install: Effect.die("unused"),
            installWithProgress: () => Effect.die("unused"),
          }),
        ),
      );

      const status = yield* runtime.applyConfig({
        providerKind: "cloudflare_tunnel",
        connectorToken: "token",
      });

      expect(status).toEqual({
        status: "failed",
        providerKind: "cloudflare_tunnel",
        reason: "The relay client is not installed.",
      });
      expect(spawn).not.toHaveBeenCalled();
    }),
  );
});
