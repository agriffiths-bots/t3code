import * as NodeCrypto from "node:crypto";

import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Config from "effect/Config";
import * as Duration from "effect/Duration";
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
  readonly forceKillAfterSeconds: number;
}

export interface WorkerLaunchExecutable {
  readonly executablePath: string;
  readonly env: Readonly<Record<string, string>>;
}

interface SystemdAvailability {
  readonly available: boolean;
  readonly expandEnvironmentFlag: boolean;
}

export interface WorkerProcessIsolationShape {
  readonly config: WorkerProcessIsolationConfig;
  readonly bootIdentity: string;
  readonly wrapSpawner: (
    spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  ) => ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly reapStaleScopes: (
    spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  ) => Effect.Effect<void>;
  readonly prepareExecutable: (input: {
    readonly realCommand: string;
    readonly directory: string;
  }) => Effect.Effect<WorkerLaunchExecutable, never, FileSystem.FileSystem | Path.Path>;
}

export class WorkerProcessIsolation extends Context.Service<
  WorkerProcessIsolation,
  WorkerProcessIsolationShape
>()("t3/process/WorkerProcessIsolation") {}

export const FALLBACK_WARNING_PREFIX = "t3 worker process isolation unavailable";

const FALLBACK_REASON_VALUES = [
  "missing-boot-identity",
  "missing-systemd-run",
  "missing-systemctl",
  "scope-probe-failed",
  "slice-quota-configuration-failed",
] as const;

export type FallbackReason = (typeof FALLBACK_REASON_VALUES)[number];

const FALLBACK_REASONS = new Set<string>(FALLBACK_REASON_VALUES);

export const parseFallbackWarningLine = (line: string): FallbackReason | undefined => {
  const prefix = `${FALLBACK_WARNING_PREFIX}: `;
  if (!line.startsWith(prefix)) return undefined;
  const reason = line.slice(prefix.length);
  return FALLBACK_REASONS.has(reason) ? (reason as FallbackReason) : undefined;
};

const DEFAULT_CONFIG: WorkerProcessIsolationConfig = {
  enabled: true,
  slice: "factory-workers.slice",
  cpuQuota: "200%",
  nice: 10,
  systemdRunPath: "systemd-run",
  systemctlPath: "systemctl",
  forceKillAfterSeconds: 2,
};

let nextScopeId = 0;

interface WorkerProcessIsolationRuntimeOptions {
  readonly bootIdentity?: string;
  readonly classifyBootOwner?: (pid: number, processStartTime: string) => BootOwnerStatus;
}

type ReadProcessStartTime = (pid: number) => Effect.Effect<string | undefined>;
type BootOwnerStatus = "alive" | "dead" | "unverifiable";
type PidOccupancy = "occupied" | "free" | "unverifiable";

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

const parseForceKillAfterSeconds = (value: string): number => {
  const parsed = Number.parseFloat(value.trim());
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : DEFAULT_CONFIG.forceKillAfterSeconds;
};

const trimOr = (value: string, fallback: string): string => {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
};

const supportsSystemdIsolation = (platform: NodeJS.Platform): boolean => platform === "linux";

const parseProcessStartTime = (stat: string): string | undefined => {
  const commandEnd = stat.lastIndexOf(")");
  if (commandEnd < 0) return undefined;
  const fieldsAfterCommand = stat
    .slice(commandEnd + 1)
    .trim()
    .split(/\s+/u);
  const startTime = fieldsAfterCommand[19];
  return startTime !== undefined && /^\d+$/u.test(startTime) ? startTime : undefined;
};

const makeBootIdentity = (processStartTime: string | undefined): string =>
  `b${process.pid}-${processStartTime ?? "0"}-${NodeCrypto.randomUUID().replaceAll("-", "")}`;

const makeScopeUnitName = (bootIdentity: string): string => {
  nextScopeId += 1;
  return `t3-worker-${bootIdentity}-${nextScopeId}-${NodeCrypto.randomUUID().replaceAll("-", "")}.scope`;
};

const OWNED_SCOPE_UNIT_PATTERN =
  /^t3-worker-(b([1-9]\d*)-(\d+)-[a-f0-9]{32})-[a-zA-Z0-9]+(?:-[a-zA-Z0-9]+)*\.scope$/;

const parseOwnedScopeUnit = (
  unitName: string,
):
  | {
      readonly bootIdentity: string;
      readonly ownerPid: number;
      readonly processStartTime: string;
    }
  | undefined => {
  const match = OWNED_SCOPE_UNIT_PATTERN.exec(unitName);
  if (!match?.[1] || !match[2] || !match[3]) return undefined;
  const ownerPid = Number.parseInt(match[2], 10);
  if (!Number.isSafeInteger(ownerPid) || ownerPid <= 0) return undefined;
  return { bootIdentity: match[1], ownerPid, processStartTime: match[3] };
};

const pidOccupancy = (pid: number): PidOccupancy => {
  try {
    process.kill(pid, 0);
    return "occupied";
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ESRCH") return "free";
    if (code === "EPERM") return "occupied";
    return "unverifiable";
  }
};

const escapeSystemdEnvironmentExpansion = (value: string): string => value.split("$").join("$$");

const maybeEscapeSystemdArgument = (value: string, expandEnvironmentFlag: boolean): string =>
  expandEnvironmentFlag ? value : escapeSystemdEnvironmentExpansion(value);

const FREEZE_SIGNALS = new Set<ChildProcess.Signal>(["SIGSTOP", "SIGTSTP", "SIGTTIN", "SIGTTOU"]);

const normalizeKillOptions = (
  options: ChildProcess.KillOptions | undefined,
): ChildProcess.KillOptions | undefined => {
  if (options?.killSignal === undefined || !FREEZE_SIGNALS.has(options.killSignal)) return options;
  return {
    ...options,
    killSignal: "SIGTERM",
  };
};

const mergeKillOptions = (
  defaults: ChildProcess.KillOptions | undefined,
  options: ChildProcess.KillOptions | undefined,
): ChildProcess.KillOptions | undefined => {
  const killSignal = options?.killSignal ?? defaults?.killSignal;
  const forceKillAfter = options?.forceKillAfter ?? defaults?.forceKillAfter;
  if (killSignal === undefined && forceKillAfter === undefined) return undefined;
  return normalizeKillOptions({
    ...(killSignal !== undefined ? { killSignal } : {}),
    ...(forceKillAfter !== undefined ? { forceKillAfter } : {}),
  });
};

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
  forceKillAfterSeconds: Config.string("T3CODE_WORKER_SYSTEMD_FORCE_KILL_AFTER_SECONDS").pipe(
    Config.withDefault(String(DEFAULT_CONFIG.forceKillAfterSeconds)),
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
      forceKillAfterSeconds: parseForceKillAfterSeconds(env.forceKillAfterSeconds),
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

const makeSystemdRunArgs = (input: {
  readonly config: WorkerProcessIsolationConfig;
  readonly expandEnvironmentFlag: boolean;
  readonly unitName?: string;
  readonly command: string;
  readonly args: ReadonlyArray<string>;
}): ReadonlyArray<string> => [
  "--user",
  "--scope",
  "--quiet",
  "--collect",
  ...(input.expandEnvironmentFlag ? ["--expand-environment=no"] : []),
  ...(input.unitName ? [`--unit=${input.unitName}`] : []),
  `--slice=${input.config.slice}`,
  `--nice=${String(input.config.nice)}`,
  "--",
  maybeEscapeSystemdArgument(input.command, input.expandEnvironmentFlag),
  ...input.args.map((arg) => maybeEscapeSystemdArgument(arg, input.expandEnvironmentFlag)),
];

const runScopeProbe = (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  config: WorkerProcessIsolationConfig,
  expandEnvironmentFlag: boolean,
): Effect.Effect<boolean> =>
  spawner
    .exitCode(
      ChildProcess.make(
        config.systemdRunPath,
        makeSystemdRunArgs({
          config,
          expandEnvironmentFlag,
          command: "/bin/true",
          args: [],
        }),
      ),
    )
    .pipe(
      Effect.map((code) => Number(code) === 0),
      Effect.orElseSucceed(() => false),
    );

const probeSystemdAvailability = (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  config: WorkerProcessIsolationConfig,
): Effect.Effect<SystemdAvailability> =>
  runScopeProbe(spawner, config, true).pipe(
    Effect.flatMap((modernAvailable) => {
      if (modernAvailable) {
        return Effect.succeed({ available: true, expandEnvironmentFlag: true });
      }
      return runScopeProbe(spawner, config, false).pipe(
        Effect.map(
          (legacyAvailable): SystemdAvailability => ({
            available: legacyAvailable,
            expandEnvironmentFlag: false,
          }),
        ),
      );
    }),
  );

const signalScopeUnit = (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  config: WorkerProcessIsolationConfig,
  unitName: string,
  signal: ChildProcess.Signal,
): Effect.Effect<void> => {
  return spawner
    .exitCode(
      ChildProcess.make(config.systemctlPath, ["--user", "kill", `--signal=${signal}`, unitName]),
    )
    .pipe(Effect.asVoid, Effect.ignore);
};

const signalScopeUnitChecked = (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  config: WorkerProcessIsolationConfig,
  unitName: string,
  signal: ChildProcess.Signal,
): Effect.Effect<boolean> =>
  spawner
    .exitCode(
      ChildProcess.make(config.systemctlPath, ["--user", "kill", `--signal=${signal}`, unitName]),
    )
    .pipe(
      Effect.map((code) => Number(code) === 0),
      Effect.orElseSucceed(() => false),
    );

const listWorkerScopeUnits = (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  config: WorkerProcessIsolationConfig,
): Effect.Effect<ReadonlyArray<string>> =>
  spawner
    .string(
      ChildProcess.make(config.systemctlPath, [
        "--user",
        "list-units",
        "--type=scope",
        "--all",
        "--no-legend",
        "--plain",
        "--no-pager",
        "t3-worker-*.scope",
      ]),
    )
    .pipe(
      Effect.map((output) =>
        output
          .split(/\r?\n/u)
          .map((line) =>
            line
              .trim()
              .split(/\s+/u)
              .find((token) => token.startsWith("t3-worker-") && token.endsWith(".scope")),
          )
          .filter((unitName): unitName is string => Boolean(unitName)),
      ),
      Effect.orElseSucceed(() => []),
    );

const scheduleScopeForceKill = (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  config: WorkerProcessIsolationConfig,
  unitName: string,
  options: ChildProcess.KillOptions | undefined,
  signal: ChildProcess.Signal,
): Effect.Effect<void> => {
  if (options?.forceKillAfter === undefined || signal === "SIGKILL") return Effect.void;
  return Effect.sleep(options.forceKillAfter).pipe(
    Effect.andThen(signalScopeUnit(spawner, config, unitName, "SIGKILL")),
    Effect.forkDetach,
    Effect.asVoid,
  );
};

const wrapScopeHandle = (
  handle: ChildProcessSpawner.ChildProcessHandle,
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  config: WorkerProcessIsolationConfig,
  unitName: string,
  killDefaults: ChildProcess.KillOptions | undefined,
): ChildProcessSpawner.ChildProcessHandle =>
  ChildProcessSpawner.makeHandle({
    pid: handle.pid,
    exitCode: handle.exitCode,
    isRunning: handle.isRunning,
    kill: (options) => {
      const mergedOptions = mergeKillOptions(killDefaults, options);
      const signal = mergedOptions?.killSignal ?? "SIGTERM";
      return signalScopeUnit(spawner, config, unitName, signal).pipe(
        Effect.andThen(scheduleScopeForceKill(spawner, config, unitName, mergedOptions, signal)),
        Effect.andThen(handle.kill(mergedOptions).pipe(Effect.ignore)),
      );
    },
    stdin: handle.stdin,
    stdout: handle.stdout,
    stderr: handle.stderr,
    all: handle.all,
    getInputFd: handle.getInputFd,
    getOutputFd: handle.getOutputFd,
    unref: handle.unref,
  });

const wrapCommand = (
  command: ChildProcess.StandardCommand,
  config: WorkerProcessIsolationConfig,
  expandEnvironmentFlag: boolean,
  unitName: string,
): ChildProcess.StandardCommand => {
  return ChildProcess.make(
    config.systemdRunPath,
    makeSystemdRunArgs({
      config,
      expandEnvironmentFlag,
      unitName,
      command: command.command,
      args: command.args,
    }),
    {
      ...command.options,
      shell: false,
    },
  );
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
boot_identity="\${T3CODE_SERVER_BOOT_IDENTITY:-}"
force_kill_after="\${T3CODE_WORKER_SYSTEMD_FORCE_KILL_AFTER_SECONDS:-2}"
case "$enabled" in
  0|false|FALSE|no|NO|off|OFF)
    exec "$real_command" "$@"
    ;;
esac

escape_systemd_arg() {
  printf '%s\\n' "$1" | sed 's/\\$/\\$\\$/g'
}

systemd_supports_expand_environment() {
  "$systemd_run" --help 2>/dev/null | grep -q -- '--expand-environment='
}

warn_fallback() {
  printf '%s: %s\n' "${FALLBACK_WARNING_PREFIX}" "$1" >&2
}

run_systemd_scope() {
  if [ -z "$boot_identity" ]; then
    warn_fallback "missing-boot-identity"
    exec "$real_command" "$@"
  fi
  unit="t3-worker-$boot_identity-$$-$(date +%s 2>/dev/null || echo 0).scope"
  if systemd_supports_expand_environment; then
    "$systemd_run" --user --scope --quiet --collect --expand-environment=no "--unit=$unit" "--slice=$slice" "--nice=$nice" -- "$real_command" "$@" <&0 &
  else
    escaped_command="$(escape_systemd_arg "$real_command")"
    first=1
    for arg do
      escaped_arg="$(escape_systemd_arg "$arg")"
      if [ "$first" = 1 ]; then
        set -- "$escaped_arg"
        first=0
      else
        set -- "$@" "$escaped_arg"
      fi
    done
    if [ "$first" = 1 ]; then
      set --
    fi
    "$systemd_run" --user --scope --quiet --collect "--unit=$unit" "--slice=$slice" "--nice=$nice" -- "$escaped_command" "$@" <&0 &
  fi

  systemd_pid=$!
  stop_scope() {
    "$systemctl" --user kill --signal=TERM "$unit" >/dev/null 2>&1 || kill "$systemd_pid" >/dev/null 2>&1 || true
    ( sleep "$force_kill_after"; "$systemctl" --user kill --signal=KILL "$unit" >/dev/null 2>&1 || true ) &
  }
  trap 'stop_scope; wait "$systemd_pid"; exit $?' INT TERM HUP
  wait "$systemd_pid"
  status=$?
  trap - INT TERM HUP
  exit "$status"
}

if ! command -v "$systemd_run" >/dev/null 2>&1; then
  warn_fallback "missing-systemd-run"
  exec "$real_command" "$@"
fi
if ! command -v "$systemctl" >/dev/null 2>&1; then
  warn_fallback "missing-systemctl"
  exec "$real_command" "$@"
fi
if systemd_supports_expand_environment; then
  "$systemd_run" --user --scope --quiet --collect --expand-environment=no "--slice=$slice" "--nice=$nice" -- /bin/true >/dev/null 2>&1
  probe_status=$?
else
  "$systemd_run" --user --scope --quiet --collect "--slice=$slice" "--nice=$nice" -- /bin/true >/dev/null 2>&1
  probe_status=$?
fi
if [ "$probe_status" != 0 ]; then
  warn_fallback "scope-probe-failed"
  exec "$real_command" "$@"
fi
if ! "$systemctl" --user set-property --runtime "$slice" "CPUQuota=$quota" >/dev/null 2>&1; then
  warn_fallback "slice-quota-configuration-failed"
  exec "$real_command" "$@"
fi
run_systemd_scope "$@"
`;

const writeExecutableFileAtomically = (input: {
  readonly filePath: string;
  readonly contents: string;
}) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const targetDirectory = path.dirname(input.filePath);
      yield* fs.makeDirectory(targetDirectory, { recursive: true });
      const tempDirectory = yield* fs.makeTempDirectoryScoped({
        directory: targetDirectory,
        prefix: `${path.basename(input.filePath)}.`,
      });
      const tempPath = path.join(tempDirectory, "contents.tmp");
      yield* fs.writeFileString(tempPath, input.contents);
      yield* fs.chmod(tempPath, 0o755);
      yield* fs.rename(tempPath, input.filePath);
    }),
  );

const makeWithConfig = (
  config: WorkerProcessIsolationConfig,
  runtimeOptions: WorkerProcessIsolationRuntimeOptions = {},
  readProcessStartTime: ReadProcessStartTime,
) =>
  Effect.gen(function* () {
    const platform = yield* HostProcessPlatform;
    const systemdIsolationSupported = supportsSystemdIsolation(platform);
    const bootIdentity =
      runtimeOptions.bootIdentity ?? makeBootIdentity(yield* readProcessStartTime(process.pid));
    const classifyBootOwner = (
      pid: number,
      processStartTime: string,
    ): Effect.Effect<BootOwnerStatus> => {
      if (runtimeOptions.classifyBootOwner !== undefined) {
        return Effect.sync(() => runtimeOptions.classifyBootOwner!(pid, processStartTime));
      }
      const occupancy = pidOccupancy(pid);
      if (occupancy === "free") return Effect.succeed("dead");
      if (occupancy === "unverifiable") return Effect.succeed("unverifiable");
      return readProcessStartTime(pid).pipe(
        Effect.map((observedStartTime): BootOwnerStatus => {
          if (processStartTime === "0" || observedStartTime === undefined) {
            return "unverifiable";
          }
          return observedStartTime === processStartTime ? "alive" : "unverifiable";
        }),
      );
    };
    const availabilityRef = yield* Ref.make<Option.Option<SystemdAvailability>>(Option.none());

    const ensureAvailable = (
      spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
    ): Effect.Effect<SystemdAvailability> =>
      Ref.get(availabilityRef).pipe(
        Effect.flatMap((cached) =>
          Option.match(cached, {
            onSome: Effect.succeed,
            onNone: () =>
              probeSystemdAvailability(spawner, config).pipe(
                Effect.flatMap((availability) =>
                  availability.available
                    ? runSetProperty(spawner, config).pipe(
                        Effect.map((quotaAvailable) => ({
                          available: quotaAvailable,
                          expandEnvironmentFlag: availability.expandEnvironmentFlag,
                        })),
                      )
                    : Effect.succeed(availability),
                ),
                Effect.tap((available) => Ref.set(availabilityRef, Option.some(available))),
                Effect.tap((available) =>
                  available.available
                    ? Effect.logInfo("worker.process.isolation.enabled", {
                        slice: config.slice,
                        cpuQuota: config.cpuQuota,
                        nice: config.nice,
                        expandEnvironmentFlag: available.expandEnvironmentFlag,
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
            if (!available.available) return spawner.spawn(command);
            const unitName = makeScopeUnitName(bootIdentity);
            return spawner
              .spawn(wrapCommand(command, config, available.expandEnvironmentFlag, unitName))
              .pipe(
                Effect.map((handle) =>
                  wrapScopeHandle(handle, spawner, config, unitName, command.options),
                ),
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

    const reapStaleScopes: WorkerProcessIsolationShape["reapStaleScopes"] = (spawner) => {
      if (!config.enabled || !systemdIsolationSupported) return Effect.void;
      return Effect.gen(function* () {
        const unitNames = yield* listWorkerScopeUnits(spawner, config);
        const staleFlags = yield* Effect.forEach(
          unitNames,
          (unitName) => {
            const owner = parseOwnedScopeUnit(unitName);
            if (owner === undefined || owner.bootIdentity === bootIdentity) {
              return Effect.succeed(false);
            }
            return classifyBootOwner(owner.ownerPid, owner.processStartTime).pipe(
              Effect.tap((status) =>
                status === "unverifiable"
                  ? Effect.logWarning("worker.process.isolation.scope-owner-unverifiable", {
                      unitName,
                      ownerPid: owner.ownerPid,
                      recordedStartTime: owner.processStartTime,
                      bootIdentity,
                    })
                  : Effect.void,
              ),
              Effect.map((status) => status === "dead"),
            );
          },
          { concurrency: "unbounded" },
        );
        const staleUnitNames = unitNames.filter((_, index) => staleFlags[index] === true);
        yield* Effect.forEach(
          staleUnitNames,
          (unitName) =>
            Effect.gen(function* () {
              const termSent = yield* signalScopeUnitChecked(spawner, config, unitName, "SIGTERM");
              yield* Effect.sleep(Duration.seconds(config.forceKillAfterSeconds));
              const killSent = yield* signalScopeUnitChecked(spawner, config, unitName, "SIGKILL");
              yield* termSent || killSent
                ? Effect.logInfo("worker.process.isolation.stale-scope-reaped", {
                    unitName,
                    bootIdentity,
                  })
                : Effect.logWarning("worker.process.isolation.stale-scope-reap-failed", {
                    unitName,
                    bootIdentity,
                  });
            }),
          { concurrency: "unbounded", discard: true },
        );
      });
    };

    const prepareExecutable: WorkerProcessIsolationShape["prepareExecutable"] = (input) =>
      Effect.gen(function* () {
        if (!config.enabled || !systemdIsolationSupported) {
          return {
            executablePath: input.realCommand,
            env: {},
          } satisfies WorkerLaunchExecutable;
        }

        const path = yield* Path.Path;
        const executablePath = path.join(input.directory, "t3-worker-systemd-run");
        yield* writeExecutableFileAtomically({
          filePath: executablePath,
          contents: wrapperScript,
        });
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
            T3CODE_WORKER_SYSTEMD_FORCE_KILL_AFTER_SECONDS: String(config.forceKillAfterSeconds),
            T3CODE_SERVER_BOOT_IDENTITY: bootIdentity,
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
      bootIdentity,
      wrapSpawner,
      reapStaleScopes,
      prepareExecutable,
    });
  });

export const make = (
  config: WorkerProcessIsolationConfig,
  runtimeOptions: WorkerProcessIsolationRuntimeOptions = {},
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const readProcessStartTime: ReadProcessStartTime = (pid) =>
      fs.readFileString(`/proc/${pid}/stat`).pipe(
        Effect.map(parseProcessStartTime),
        Effect.orElseSucceed(() => undefined),
      );
    return yield* makeWithConfig(config, runtimeOptions, readProcessStartTime);
  });

export const layer = Layer.effect(WorkerProcessIsolation, EnvConfig.pipe(Effect.flatMap(make)));

export const layerTest = (
  config: Partial<WorkerProcessIsolationConfig> = {},
  platform: NodeJS.Platform = "linux",
  runtimeOptions: WorkerProcessIsolationRuntimeOptions = {},
) =>
  Layer.effect(
    WorkerProcessIsolation,
    makeWithConfig({ ...DEFAULT_CONFIG, ...config }, runtimeOptions, (pid) =>
      Effect.succeed(pid === process.pid ? "1" : undefined),
    ).pipe(Effect.provideService(HostProcessPlatform, platform)),
  );

export const disabled = WorkerProcessIsolation.of({
  config: { ...DEFAULT_CONFIG, enabled: false },
  bootIdentity: "disabled",
  wrapSpawner: (spawner) => spawner,
  reapStaleScopes: () => Effect.void,
  prepareExecutable: (input) =>
    Effect.succeed({
      executablePath: input.realCommand,
      env: {},
    }),
});

export const currentOrDisabled = Effect.serviceOption(WorkerProcessIsolation).pipe(
  Effect.map(Option.getOrElse(() => disabled)),
);
