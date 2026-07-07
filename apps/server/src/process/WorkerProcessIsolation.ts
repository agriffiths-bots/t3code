import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Config from "effect/Config";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

export interface WorkerProcessIsolationConfig {
  readonly enabled: boolean;
  readonly slice: string;
  readonly cpuQuota: string;
  readonly nice: number;
  readonly systemdRunPath: string;
  readonly systemctlPath: string;
}

export interface WorkerLaunchExecutable {
  readonly executablePath: string;
  readonly env: Readonly<Record<string, string>>;
}

export interface WorkerProcessIsolationShape {
  readonly config: WorkerProcessIsolationConfig;
  readonly wrapSpawner: (
    spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  ) => ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly prepareExecutable: (input: {
    readonly realCommand: string;
    readonly directory: string;
  }) => Effect.Effect<WorkerLaunchExecutable, never, FileSystem.FileSystem | Path.Path>;
}

export class WorkerProcessIsolation extends Context.Service<
  WorkerProcessIsolation,
  WorkerProcessIsolationShape
>()("t3/process/WorkerProcessIsolation") {}

const DEFAULT_CONFIG: WorkerProcessIsolationConfig = {
  enabled: true,
  slice: "factory-workers.slice",
  cpuQuota: "200%",
  nice: 10,
  systemdRunPath: "systemd-run",
  systemctlPath: "systemctl",
};

const truthy = (value: string): boolean => {
  const normalized = value.trim().toLowerCase();
  return (
    normalized !== "0" && normalized !== "false" && normalized !== "no" && normalized !== "off"
  );
};

const parseNice = (value: string): number => {
  const parsed = Number.parseInt(value.trim(), 10);
  if (!Number.isFinite(parsed)) return DEFAULT_CONFIG.nice;
  return Math.min(19, Math.max(0, parsed));
};

const trimOr = (value: string, fallback: string): string => {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
};

const supportsSystemdIsolation = (platform: NodeJS.Platform): boolean => platform === "linux";

const EnvConfig = Config.all({
  enabled: Config.string("T3CODE_WORKER_SYSTEMD_ENABLED").pipe(Config.withDefault("1")),
  slice: Config.string("T3CODE_WORKER_SYSTEMD_SLICE").pipe(
    Config.withDefault(DEFAULT_CONFIG.slice),
  ),
  cpuQuota: Config.string("T3CODE_WORKER_SYSTEMD_CPU_QUOTA").pipe(
    Config.withDefault(DEFAULT_CONFIG.cpuQuota),
  ),
  nice: Config.string("T3CODE_WORKER_NICE").pipe(Config.withDefault(String(DEFAULT_CONFIG.nice))),
  systemdRunPath: Config.string("T3CODE_WORKER_SYSTEMD_RUN").pipe(
    Config.withDefault(DEFAULT_CONFIG.systemdRunPath),
  ),
  systemctlPath: Config.string("T3CODE_WORKER_SYSTEMCTL").pipe(
    Config.withDefault(DEFAULT_CONFIG.systemctlPath),
  ),
}).pipe(
  Config.map(
    (env): WorkerProcessIsolationConfig => ({
      enabled: truthy(env.enabled),
      slice: trimOr(env.slice, DEFAULT_CONFIG.slice),
      cpuQuota: trimOr(env.cpuQuota, DEFAULT_CONFIG.cpuQuota),
      nice: parseNice(env.nice),
      systemdRunPath: trimOr(env.systemdRunPath, DEFAULT_CONFIG.systemdRunPath),
      systemctlPath: trimOr(env.systemctlPath, DEFAULT_CONFIG.systemctlPath),
    }),
  ),
);

const runSetProperty = (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  config: WorkerProcessIsolationConfig,
): Effect.Effect<boolean> =>
  spawner
    .exitCode(
      ChildProcess.make(config.systemctlPath, [
        "--user",
        "set-property",
        "--runtime",
        config.slice,
        `CPUQuota=${config.cpuQuota}`,
      ]),
    )
    .pipe(
      Effect.map((code) => Number(code) === 0),
      Effect.orElseSucceed(() => false),
    );

const runScopeProbe = (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  config: WorkerProcessIsolationConfig,
): Effect.Effect<boolean> =>
  spawner
    .exitCode(
      ChildProcess.make(config.systemdRunPath, [
        "--user",
        "--scope",
        "--quiet",
        "--collect",
        `--slice=${config.slice}`,
        `--nice=${String(config.nice)}`,
        "--",
        "/bin/true",
      ]),
    )
    .pipe(
      Effect.map((code) => Number(code) === 0),
      Effect.orElseSucceed(() => false),
    );

const wrapCommand = (
  command: ChildProcess.StandardCommand,
  config: WorkerProcessIsolationConfig,
): ChildProcess.StandardCommand => {
  const systemdArgs = [
    "--user",
    "--scope",
    "--quiet",
    "--collect",
    `--slice=${config.slice}`,
    `--nice=${String(config.nice)}`,
    "--",
    command.command,
    ...command.args,
  ];
  return ChildProcess.make(config.systemdRunPath, systemdArgs, {
    ...command.options,
    shell: false,
  });
};

const shouldWrapCommand = (
  command: ChildProcess.StandardCommand,
  config: WorkerProcessIsolationConfig,
  systemdIsolationSupported: boolean,
): boolean => {
  if (!config.enabled) return false;
  if (!systemdIsolationSupported) return false;
  if (command.command === config.systemdRunPath || command.command.endsWith("/systemd-run")) {
    return false;
  }
  if (command.options.shell || command.options.detached) return false;
  return true;
};

const wrapperScript = `#!/bin/sh
real_command="\${T3_WORKER_REAL_COMMAND:-}"
if [ -z "$real_command" ]; then
  echo "T3_WORKER_REAL_COMMAND is not set" >&2
  exit 127
fi
enabled="\${T3CODE_WORKER_SYSTEMD_ENABLED:-1}"
slice="\${T3CODE_WORKER_SYSTEMD_SLICE:-factory-workers.slice}"
quota="\${T3CODE_WORKER_SYSTEMD_CPU_QUOTA:-200%}"
nice="\${T3CODE_WORKER_NICE:-10}"
systemd_run="\${T3CODE_WORKER_SYSTEMD_RUN:-systemd-run}"
systemctl="\${T3CODE_WORKER_SYSTEMCTL:-systemctl}"
case "$enabled" in
  0|false|FALSE|no|NO|off|OFF)
    exec "$real_command" "$@"
    ;;
esac
if command -v "$systemd_run" >/dev/null 2>&1 && command -v "$systemctl" >/dev/null 2>&1 && "$systemd_run" --user --scope --quiet --collect "--slice=$slice" "--nice=$nice" -- /bin/true >/dev/null 2>&1 && "$systemctl" --user set-property --runtime "$slice" "CPUQuota=$quota" >/dev/null 2>&1; then
  exec "$systemd_run" --user --scope --quiet --collect "--slice=$slice" "--nice=$nice" -- "$real_command" "$@"
fi
exec "$real_command" "$@"
`;

const makeWithConfig = (config: WorkerProcessIsolationConfig) =>
  Effect.gen(function* () {
    const platform = yield* HostProcessPlatform;
    const systemdIsolationSupported = supportsSystemdIsolation(platform);
    const availabilityRef = yield* Ref.make<Option.Option<boolean>>(Option.none());

    const ensureAvailable = (
      spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
    ): Effect.Effect<boolean> =>
      Ref.get(availabilityRef).pipe(
        Effect.flatMap((cached) =>
          Option.match(cached, {
            onSome: Effect.succeed,
            onNone: () =>
              runScopeProbe(spawner, config).pipe(
                Effect.flatMap((scopeAvailable) =>
                  scopeAvailable ? runSetProperty(spawner, config) : Effect.succeed(false),
                ),
                Effect.tap((available) => Ref.set(availabilityRef, Option.some(available))),
                Effect.tap((available) =>
                  available
                    ? Effect.logInfo("worker.process.isolation.enabled", {
                        slice: config.slice,
                        cpuQuota: config.cpuQuota,
                        nice: config.nice,
                      })
                    : Effect.logWarning("worker.process.isolation.unavailable", {
                        slice: config.slice,
                      }),
                ),
              ),
          }),
        ),
      );

    const wrapSpawner: WorkerProcessIsolationShape["wrapSpawner"] = (spawner) =>
      ChildProcessSpawner.make((command) => {
        if (!ChildProcess.isStandardCommand(command)) return spawner.spawn(command);
        if (!shouldWrapCommand(command, config, systemdIsolationSupported)) {
          return spawner.spawn(command);
        }
        return ensureAvailable(spawner).pipe(
          Effect.flatMap((available) => {
            if (!available) return spawner.spawn(command);
            return spawner.spawn(wrapCommand(command, config)).pipe(
              Effect.catch(() =>
                Effect.logWarning("worker.process.isolation.spawn-fallback", {
                  slice: config.slice,
                  command: command.command,
                }).pipe(Effect.flatMap(() => spawner.spawn(command))),
              ),
            );
          }),
        );
      });

    const prepareExecutable: WorkerProcessIsolationShape["prepareExecutable"] = (input) =>
      Effect.gen(function* () {
        if (!config.enabled || !systemdIsolationSupported) {
          return {
            executablePath: input.realCommand,
            env: {},
          } satisfies WorkerLaunchExecutable;
        }

        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        yield* fs.makeDirectory(input.directory, { recursive: true });
        const executablePath = path.join(input.directory, "t3-worker-systemd-run");
        yield* fs.writeFileString(executablePath, wrapperScript);
        yield* fs.chmod(executablePath, 0o755);
        return {
          executablePath,
          env: {
            T3_WORKER_REAL_COMMAND: input.realCommand,
            T3CODE_WORKER_SYSTEMD_ENABLED: config.enabled ? "1" : "0",
            T3CODE_WORKER_SYSTEMD_SLICE: config.slice,
            T3CODE_WORKER_SYSTEMD_CPU_QUOTA: config.cpuQuota,
            T3CODE_WORKER_NICE: String(config.nice),
            T3CODE_WORKER_SYSTEMD_RUN: config.systemdRunPath,
            T3CODE_WORKER_SYSTEMCTL: config.systemctlPath,
          },
        } satisfies WorkerLaunchExecutable;
      }).pipe(
        Effect.orElseSucceed(() => ({
          executablePath: input.realCommand,
          env: {},
        })),
      );

    return WorkerProcessIsolation.of({
      config,
      wrapSpawner,
      prepareExecutable,
    });
  });

export const make = makeWithConfig;

export const layer = Layer.effect(WorkerProcessIsolation, EnvConfig.pipe(Effect.flatMap(make)));

export const layerTest = (
  config: Partial<WorkerProcessIsolationConfig> = {},
  platform: NodeJS.Platform = "linux",
) =>
  Layer.effect(
    WorkerProcessIsolation,
    makeWithConfig({ ...DEFAULT_CONFIG, ...config }).pipe(
      Effect.provideService(HostProcessPlatform, platform),
    ),
  );

export const disabled = WorkerProcessIsolation.of({
  config: { ...DEFAULT_CONFIG, enabled: false },
  wrapSpawner: (spawner) => spawner,
  prepareExecutable: (input) =>
    Effect.succeed({
      executablePath: input.realCommand,
      env: {},
    }),
});

export const currentOrDisabled = Effect.serviceOption(WorkerProcessIsolation).pipe(
  Effect.map(Option.getOrElse(() => disabled)),
);
