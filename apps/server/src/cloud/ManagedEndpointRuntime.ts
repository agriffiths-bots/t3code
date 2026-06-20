import type { RelayManagedEndpointRuntimeConfig } from "@t3tools/contracts/relay";
import * as RelayClient from "@t3tools/shared/relayClient";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import { CLOUD_ENDPOINT_RUNTIME_CONFIG, decodeRuntimeConfig } from "./config.ts";

const MAX_CAUSE_DIAGNOSTIC_TAGS = 8;
const MAX_DIAGNOSTIC_VALUE_LENGTH = 128;

function boundedDiagnosticValue(value: string): string {
  return value.slice(0, MAX_DIAGNOSTIC_VALUE_LENGTH);
}

function diagnosticValueTag(value: unknown): string {
  try {
    if (
      typeof value === "object" &&
      value !== null &&
      "_tag" in value &&
      typeof value._tag === "string"
    ) {
      return boundedDiagnosticValue(value._tag);
    }
    if (value instanceof Error) {
      return boundedDiagnosticValue(value.name);
    }
    return typeof value;
  } catch {
    return "Uninspectable";
  }
}

function addUniqueDiagnosticTag(tags: Array<string>, tag: string): void {
  if (tags.length < MAX_CAUSE_DIAGNOSTIC_TAGS && !tags.includes(tag)) {
    tags.push(tag);
  }
}

export function managedEndpointCauseDiagnostics(cause: Cause.Cause<unknown>) {
  const failureTags: Array<string> = [];
  const defectTags: Array<string> = [];
  let failureCount = 0;
  let defectCount = 0;
  let interruptionCount = 0;

  for (const reason of cause.reasons) {
    if (Cause.isFailReason(reason)) {
      failureCount += 1;
      addUniqueDiagnosticTag(failureTags, diagnosticValueTag(reason.error));
      continue;
    }
    if (Cause.isDieReason(reason)) {
      defectCount += 1;
      addUniqueDiagnosticTag(defectTags, diagnosticValueTag(reason.defect));
      continue;
    }
    interruptionCount += 1;
  }

  return {
    reasonCount: cause.reasons.length,
    failureCount,
    failureTags,
    defectCount,
    defectTags,
    interruptionCount,
  };
}

function logManagedEndpointCause(
  message: string,
  cause: Cause.Cause<unknown>,
  attributes: Readonly<Record<string, unknown>>,
) {
  const interruptionReasons = cause.reasons.filter(Cause.isInterruptReason);
  if (interruptionReasons.length > 0) {
    return Effect.failCause(Cause.fromReasons<never>(interruptionReasons));
  }
  return Effect.logWarning(message, {
    ...attributes,
    ...managedEndpointCauseDiagnostics(cause),
  });
}

function platformErrorDiagnostics(error: PlatformError.PlatformError) {
  return {
    errorTag: error._tag,
    reasonTag: error.reason._tag,
    errorModule: boundedDiagnosticValue(error.reason.module),
    errorMethod: boundedDiagnosticValue(error.reason.method),
  };
}

export class CloudManagedEndpointRuntimeConfigReadError extends Schema.TaggedErrorClass<CloudManagedEndpointRuntimeConfigReadError>()(
  "CloudManagedEndpointRuntimeConfigReadError",
  {
    resource: Schema.Literal(CLOUD_ENDPOINT_RUNTIME_CONFIG),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to read managed endpoint runtime configuration from ${this.resource}.`;
  }
}

export class CloudManagedEndpointRuntimeConfigDecodeError extends Schema.TaggedErrorClass<CloudManagedEndpointRuntimeConfigDecodeError>()(
  "CloudManagedEndpointRuntimeConfigDecodeError",
  {
    resource: Schema.Literal(CLOUD_ENDPOINT_RUNTIME_CONFIG),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to decode managed endpoint runtime configuration from ${this.resource}.`;
  }
}

function bytesToString(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}

const readRuntimeConfig = Effect.gen(function* () {
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const bytes = yield* secrets.get(CLOUD_ENDPOINT_RUNTIME_CONFIG).pipe(
    Effect.mapError(
      (cause) =>
        new CloudManagedEndpointRuntimeConfigReadError({
          resource: CLOUD_ENDPOINT_RUNTIME_CONFIG,
          cause,
        }),
    ),
  );
  if (Option.isNone(bytes)) {
    return null;
  }
  return yield* decodeRuntimeConfig(bytesToString(bytes.value)).pipe(
    Effect.mapError(
      (cause) =>
        new CloudManagedEndpointRuntimeConfigDecodeError({
          resource: CLOUD_ENDPOINT_RUNTIME_CONFIG,
          cause,
        }),
    ),
  );
});

export type CloudManagedEndpointRuntimeStatus =
  | {
      readonly status: "disabled";
    }
  | {
      readonly status: "failed";
      readonly providerKind: RelayManagedEndpointRuntimeConfig["providerKind"];
      readonly reason: string;
      readonly tunnelId?: string;
      readonly tunnelName?: string;
    }
  | {
      readonly status: "running";
      readonly providerKind: "cloudflare_tunnel";
      readonly pid: number;
      readonly tunnelId?: string;
      readonly tunnelName?: string;
    }
  | {
      readonly status: "unsupported";
      readonly providerKind: RelayManagedEndpointRuntimeConfig["providerKind"];
    };

export class CloudManagedEndpointRuntime extends Context.Service<
  CloudManagedEndpointRuntime,
  {
    readonly applyConfig: (
      config: RelayManagedEndpointRuntimeConfig | null,
    ) => Effect.Effect<CloudManagedEndpointRuntimeStatus>;
  }
>()("t3/cloud/ManagedEndpointRuntime/CloudManagedEndpointRuntime") {}

interface ActiveConnector {
  readonly child: ChildProcessSpawner.ChildProcessHandle;
  readonly scope: Scope.Closeable;
  readonly configKey: string;
  readonly config: RelayManagedEndpointRuntimeConfig;
}

export function classifyRelayClientOutput(line: string): "connected" | "warning" | "debug" {
  if (/\bRegistered tunnel connection\b/iu.test(line)) {
    return "connected";
  }
  return /\b(?:ERR|WRN)\b/u.test(line) ? "warning" : "debug";
}

function runtimeConfigKey(config: RelayManagedEndpointRuntimeConfig): string {
  return JSON.stringify({
    providerKind: config.providerKind,
    connectorToken: config.connectorToken,
    tunnelId: config.tunnelId ?? null,
    tunnelName: config.tunnelName ?? null,
  });
}

const stopConnector = (connector: ActiveConnector | null) =>
  connector
    ? Scope.close(connector.scope, Exit.void).pipe(
        Effect.tap(() =>
          Effect.logInfo("Relay client stopped", {
            pid: Number(connector.child.pid),
          }),
        ),
        Effect.catchCause((cause) =>
          logManagedEndpointCause("Failed to stop relay client", cause, {
            pid: Number(connector.child.pid),
            tunnelId: connector.config.tunnelId,
            tunnelName: connector.config.tunnelName,
          }),
        ),
      )
    : Effect.void;

export const make = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const relayClient = yield* RelayClient.RelayClient;
  const activeRef = yield* Ref.make<ActiveConnector | null>(null);
  const desiredConfigRef = yield* Ref.make<RelayManagedEndpointRuntimeConfig | null>(null);
  const reconcileSemaphore = yield* Semaphore.make(1);
  let reconcileConfig: CloudManagedEndpointRuntime["Service"]["applyConfig"];

  const stopActive = Effect.gen(function* () {
    const active = yield* Ref.getAndSet(activeRef, null);
    yield* stopConnector(active);
  });

  const superviseConnector = (connector: ActiveConnector) =>
    Effect.gen(function* () {
      const result = yield* Effect.result(connector.child.exitCode);
      yield* reconcileSemaphore.withPermits(1)(
        Effect.gen(function* () {
          const active = yield* Ref.get(activeRef);
          if (
            active?.child.pid !== connector.child.pid ||
            active.configKey !== connector.configKey
          ) {
            return;
          }
          yield* Ref.set(activeRef, null);
          yield* stopConnector(connector);

          const desiredConfig = yield* Ref.get(desiredConfigRef);
          if (
            !desiredConfig ||
            desiredConfig.providerKind !== "cloudflare_tunnel" ||
            runtimeConfigKey(desiredConfig) !== connector.configKey
          ) {
            return;
          }

          yield* Effect.logWarning("Relay client exited; restarting", {
            pid: Number(connector.child.pid),
            ...(Result.isSuccess(result)
              ? { exitCode: Number(result.success) }
              : {
                  exitErrorTag: result.failure._tag,
                  exitReasonTag: result.failure.reason._tag,
                  exitErrorModule: boundedDiagnosticValue(result.failure.reason.module),
                  exitErrorMethod: boundedDiagnosticValue(result.failure.reason.method),
                }),
            tunnelId: connector.config.tunnelId,
            tunnelName: connector.config.tunnelName,
          });
          yield* reconcileConfig(desiredConfig);
        }),
      );
    }).pipe(
      Effect.catchCause((cause) =>
        logManagedEndpointCause("Relay client supervisor failed", cause, {}),
      ),
    );

  const observeConnectorOutput = (connector: ActiveConnector) =>
    connector.child.all.pipe(
      Stream.decodeText(),
      Stream.splitLines,
      Stream.map((line) => line.trim()),
      Stream.filter((line) => line.length > 0),
      Stream.runForEach((line) => {
        const attributes = {
          pid: Number(connector.child.pid),
          tunnelId: connector.config.tunnelId,
          tunnelName: connector.config.tunnelName,
          outputLength: line.length,
        };
        switch (classifyRelayClientOutput(line)) {
          case "connected":
            return Effect.logInfo("Relay client tunnel connection registered", attributes);
          case "warning":
            return Effect.logWarning("Relay client reported a transport warning", attributes);
          case "debug":
            return Effect.logDebug("Relay client output", attributes);
        }
      }),
      Effect.catchCause((cause) =>
        logManagedEndpointCause("Relay client output observer failed", cause, {
          pid: Number(connector.child.pid),
          tunnelId: connector.config.tunnelId,
          tunnelName: connector.config.tunnelName,
        }),
      ),
    );

  reconcileConfig = Effect.fn("CloudManagedEndpointRuntime.reconcileConfig")(function* (config) {
    if (!config || config.providerKind !== "cloudflare_tunnel") {
      yield* stopActive;
      return config
        ? { status: "unsupported", providerKind: config.providerKind }
        : { status: "disabled" };
    }

    const nextConfigKey = runtimeConfigKey(config);
    const active = yield* Ref.get(activeRef);
    if (active?.configKey === nextConfigKey) {
      const isRunning = yield* active.child.isRunning.pipe(
        Effect.catchTags({
          PlatformError: (error) =>
            Effect.logWarning("Failed to inspect relay client process", {
              ...platformErrorDiagnostics(error),
              pid: Number(active.child.pid),
              tunnelId: active.config.tunnelId,
              tunnelName: active.config.tunnelName,
            }).pipe(Effect.as(false)),
        }),
      );
      if (isRunning) {
        return {
          status: "running",
          providerKind: "cloudflare_tunnel",
          pid: Number(active.child.pid),
          ...(active.config.tunnelId ? { tunnelId: active.config.tunnelId } : {}),
          ...(active.config.tunnelName ? { tunnelName: active.config.tunnelName } : {}),
        } satisfies CloudManagedEndpointRuntimeStatus;
      }
    }

    yield* stopActive;

    const executable = yield* relayClient.resolve;
    if (executable.status !== "available") {
      return {
        status: "failed",
        providerKind: "cloudflare_tunnel",
        reason:
          executable.status === "unsupported"
            ? `Relay client is unsupported on ${executable.platform}-${executable.arch}.`
            : "The relay client is not installed.",
        ...(config.tunnelId ? { tunnelId: config.tunnelId } : {}),
        ...(config.tunnelName ? { tunnelName: config.tunnelName } : {}),
      } satisfies CloudManagedEndpointRuntimeStatus;
    }

    const connectorScope = yield* Scope.make("sequential");
    const child = yield* spawner
      .spawn(
        ChildProcess.make(executable.executablePath, ["tunnel", "run"], {
          detached: false,
          env: {
            ...process.env,
            TUNNEL_TOKEN: config.connectorToken,
          },
          shell: false,
          stderr: "pipe",
          stdout: "pipe",
        }),
      )
      .pipe(
        Effect.provideService(Scope.Scope, connectorScope),
        Effect.tap((child) =>
          Effect.logInfo("Relay client process started; waiting for tunnel connection", {
            pid: Number(child.pid),
            tunnelId: config.tunnelId,
            tunnelName: config.tunnelName,
          }),
        ),
        Effect.catchTags({
          PlatformError: (error) =>
            Effect.logWarning("Failed to start relay client", {
              ...platformErrorDiagnostics(error),
              tunnelId: config.tunnelId,
              tunnelName: config.tunnelName,
            }).pipe(
              Effect.andThen(Scope.close(connectorScope, Exit.void).pipe(Effect.ignore)),
              Effect.as({
                status: "failed",
                providerKind: "cloudflare_tunnel",
                reason: "Failed to start the relay client.",
                ...(config.tunnelId ? { tunnelId: config.tunnelId } : {}),
                ...(config.tunnelName ? { tunnelName: config.tunnelName } : {}),
              } satisfies CloudManagedEndpointRuntimeStatus),
            ),
        }),
      );

    if ("status" in child && child.status === "failed") {
      return child;
    }

    if (!("status" in child)) {
      const connector = {
        child,
        scope: connectorScope,
        configKey: nextConfigKey,
        config,
      } satisfies ActiveConnector;
      yield* Ref.set(activeRef, connector);
      yield* Effect.forkIn(observeConnectorOutput(connector), connectorScope);
      yield* Effect.forkIn(superviseConnector(connector), connectorScope);
      return {
        status: "running",
        providerKind: "cloudflare_tunnel",
        pid: Number(child.pid),
        ...(config.tunnelId ? { tunnelId: config.tunnelId } : {}),
        ...(config.tunnelName ? { tunnelName: config.tunnelName } : {}),
      } satisfies CloudManagedEndpointRuntimeStatus;
    }

    return {
      status: "failed",
      providerKind: "cloudflare_tunnel",
      reason: "Relay client did not start.",
      ...(config.tunnelId ? { tunnelId: config.tunnelId } : {}),
      ...(config.tunnelName ? { tunnelName: config.tunnelName } : {}),
    } satisfies CloudManagedEndpointRuntimeStatus;
  });

  const applyConfig = Effect.fn("CloudManagedEndpointRuntime.applyConfig")(
    (config: RelayManagedEndpointRuntimeConfig | null) =>
      reconcileSemaphore.withPermits(1)(
        Ref.set(desiredConfigRef, config).pipe(Effect.andThen(reconcileConfig(config))),
      ),
  );

  const runtime = CloudManagedEndpointRuntime.of({
    applyConfig,
  });

  const recoverRuntimeConfigError = (
    error:
      | CloudManagedEndpointRuntimeConfigReadError
      | CloudManagedEndpointRuntimeConfigDecodeError,
  ) =>
    Effect.logWarning("Failed to read managed endpoint runtime config", {
      errorTag: error._tag,
      resource: error.resource,
      causeTag: diagnosticValueTag(error.cause),
    }).pipe(Effect.as(null));
  const initialConfig = yield* readRuntimeConfig.pipe(
    Effect.catchTags({
      CloudManagedEndpointRuntimeConfigReadError: recoverRuntimeConfigError,
      CloudManagedEndpointRuntimeConfigDecodeError: recoverRuntimeConfigError,
    }),
  );
  yield* runtime.applyConfig(initialConfig);
  yield* Effect.addFinalizer(() => runtime.applyConfig(null));
  return runtime;
});

export const layer = Layer.effect(CloudManagedEndpointRuntime, make);
