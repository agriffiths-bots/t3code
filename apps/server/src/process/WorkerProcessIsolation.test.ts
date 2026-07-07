import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import * as WorkerProcessIsolation from "./WorkerProcessIsolation.ts";

function makeHandle(code = 0) {
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(1),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(code)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: Stream.empty,
    stderr: Stream.empty,
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

describe("WorkerProcessIsolation", () => {
  it.effect("wraps worker spawns in a user systemd scope after configuring the slice", () =>
    Effect.gen(function* () {
      const commands: ChildProcess.StandardCommand[] = [];
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.sync(() => {
          if (!ChildProcess.isStandardCommand(command)) {
            throw new Error("expected standard command");
          }
          commands.push(command);
          return makeHandle(0);
        }),
      );
      const isolation = yield* WorkerProcessIsolation.WorkerProcessIsolation;
      const wrapped = isolation.wrapSpawner(spawner);

      yield* wrapped.spawn(
        ChildProcess.make("codex", ["app-server"], {
          cwd: "/workspace",
          env: { SECRET_TOKEN: "kept-in-env" },
          extendEnv: true,
        }),
      );

      expect(commands.map((command) => command.command)).toEqual([
        "systemd-run",
        "systemctl",
        "systemd-run",
      ]);
      expect(commands[0]?.args).toEqual([
        "--user",
        "--scope",
        "--quiet",
        "--collect",
        "--slice=factory-workers.slice",
        "--nice=10",
        "--",
        "/bin/true",
      ]);
      expect(commands[1]?.args).toEqual([
        "--user",
        "set-property",
        "--runtime",
        "factory-workers.slice",
        "CPUQuota=200%",
      ]);
      expect(commands[2]?.args).toEqual([
        "--user",
        "--scope",
        "--quiet",
        "--collect",
        "--slice=factory-workers.slice",
        "--nice=10",
        "--",
        "codex",
        "app-server",
      ]);
      expect(commands[2]?.options.cwd).toBe("/workspace");
      expect(commands[2]?.options.env?.SECRET_TOKEN).toBe("kept-in-env");
      expect(commands[2]?.options.shell).toBe(false);
    }).pipe(Effect.provide(WorkerProcessIsolation.layerTest())),
  );

  it.effect("falls back to the original command when user systemd is unavailable", () =>
    Effect.gen(function* () {
      const commands: ChildProcess.StandardCommand[] = [];
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.sync(() => {
          if (!ChildProcess.isStandardCommand(command)) {
            throw new Error("expected standard command");
          }
          commands.push(command);
          return makeHandle(commands.length === 1 ? 1 : 0);
        }),
      );
      const isolation = yield* WorkerProcessIsolation.WorkerProcessIsolation;
      const wrapped = isolation.wrapSpawner(spawner);

      yield* wrapped.spawn(ChildProcess.make("codex", ["app-server"]));

      expect(commands.map((command) => command.command)).toEqual(["systemd-run", "codex"]);
      expect(commands[1]?.args).toEqual(["app-server"]);
    }).pipe(Effect.provide(WorkerProcessIsolation.layerTest())),
  );

  it.effect("falls back to the original command when the slice quota cannot be configured", () =>
    Effect.gen(function* () {
      const commands: ChildProcess.StandardCommand[] = [];
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.sync(() => {
          if (!ChildProcess.isStandardCommand(command)) {
            throw new Error("expected standard command");
          }
          commands.push(command);
          return makeHandle(commands.length === 2 ? 1 : 0);
        }),
      );
      const isolation = yield* WorkerProcessIsolation.WorkerProcessIsolation;
      const wrapped = isolation.wrapSpawner(spawner);

      yield* wrapped.spawn(ChildProcess.make("codex", ["app-server"]));

      expect(commands.map((command) => command.command)).toEqual([
        "systemd-run",
        "systemctl",
        "codex",
      ]);
      expect(commands[2]?.args).toEqual(["app-server"]);
    }).pipe(Effect.provide(WorkerProcessIsolation.layerTest())),
  );

  it.effect("does not wrap worker spawns outside Linux", () =>
    Effect.gen(function* () {
      const commands: ChildProcess.StandardCommand[] = [];
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.sync(() => {
          if (!ChildProcess.isStandardCommand(command)) {
            throw new Error("expected standard command");
          }
          commands.push(command);
          return makeHandle(0);
        }),
      );
      const isolation = yield* WorkerProcessIsolation.WorkerProcessIsolation;
      const wrapped = isolation.wrapSpawner(spawner);

      yield* wrapped.spawn(ChildProcess.make("codex", ["app-server"]));

      expect(commands.map((command) => command.command)).toEqual(["codex"]);
      expect(commands[0]?.args).toEqual(["app-server"]);
    }).pipe(Effect.provide(WorkerProcessIsolation.layerTest({}, "win32"))),
  );

  it.effect("prepares a wrapper executable for SDK-managed workers", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-worker-wrapper-" });
      const isolation = yield* WorkerProcessIsolation.WorkerProcessIsolation;

      const executable = yield* isolation.prepareExecutable({
        realCommand: "/usr/bin/claude",
        directory,
      });
      const contents = yield* fs.readFileString(executable.executablePath);

      expect(executable.executablePath).toBe(path.join(directory, "t3-worker-systemd-run"));
      expect(executable.env.T3_WORKER_REAL_COMMAND).toBe("/usr/bin/claude");
      expect(contents).toContain("systemd-run");
      expect(contents).toContain("CPUQuota=$quota");
      expect(contents).toContain("/bin/true");
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.mergeAll(WorkerProcessIsolation.layerTest(), NodeServices.layer)),
    ),
  );

  it.effect("keeps SDK-managed workers on the real executable outside Linux", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-worker-wrapper-" });
      const isolation = yield* WorkerProcessIsolation.WorkerProcessIsolation;

      const executable = yield* isolation.prepareExecutable({
        realCommand: "C:\\claude.cmd",
        directory,
      });

      expect(executable).toEqual({
        executablePath: "C:\\claude.cmd",
        env: {},
      });
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(WorkerProcessIsolation.layerTest({}, "win32"), NodeServices.layer),
      ),
    ),
  );
});
