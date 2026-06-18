import {
  CommandId,
  AuthAdministrativeScopes,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  EnvironmentHttpApi,
  EnvironmentHttpCommonError,
  MessageId,
  type OrchestrationCommand,
  type OrchestrationReadModel,
  type OrchestrationThread,
  ThreadId,
} from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as Data from "effect/Data";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { Argument, Command, GlobalFlag } from "effect/unstable/cli";
import { FetchHttpClient, HttpClient, HttpClientError } from "effect/unstable/http";
import * as HttpApiClient from "effect/unstable/httpapi/HttpApiClient";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";

import { ServerConfig, type ServerConfigShape } from "../config.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { createEmptyReadModel, projectEvent } from "../orchestration/projector.ts";
import { OrchestrationLayerLive } from "../orchestration/runtimeLayer.ts";
import { layerConfig as SqlitePersistenceLayerLive } from "../persistence/Layers/Sqlite.ts";
import { RepositoryIdentityResolverLive } from "../project/Layers/RepositoryIdentityResolver.ts";
import {
  clearPersistedServerRuntimeState,
  readPersistedServerRuntimeState,
} from "../serverRuntimeState.ts";
import { WorkspacePathsLive } from "../workspace/Layers/WorkspacePaths.ts";
import { type CliAuthLocationFlags, projectLocationFlags, resolveCliAuthConfig } from "./config.ts";

type CosCliDispatchCommand = Extract<OrchestrationCommand, { type: "thread.turn.start" }>;

class CosCommandError extends Data.TaggedError("CosCommandError")<{
  readonly message: string;
}> {}

// `can-restart` prints `no` and exits non-zero when a turn is in flight so a
// restart script can AND-gate on it (`t3 cos can-restart && restart`).
class CosNotSafeToRestartError extends Data.TaggedError("CosNotSafeToRestartError")<{
  readonly message: string;
}> {}

const CosCliRuntimeLive = Layer.mergeAll(
  WorkspacePathsLive,
  OrchestrationLayerLive.pipe(
    Layer.provideMerge(RepositoryIdentityResolverLive),
    Layer.provideMerge(SqlitePersistenceLayerLive),
  ),
);

const COS_CLI_LIVE_SERVER_TIMEOUT = Duration.seconds(1);
const isEnvironmentHttpCommonError = Schema.is(EnvironmentHttpCommonError);

const withCosCliSessionToken = <A, E, R>(
  environmentAuth: EnvironmentAuth.EnvironmentAuthShape,
  run: (token: string) => Effect.Effect<A, E, R>,
) =>
  Effect.acquireUseRelease(
    environmentAuth.issueSession({
      scopes: AuthAdministrativeScopes,
      label: "t3 cos cli",
    }),
    (issued) => run(issued.token),
    (issued) => environmentAuth.revokeSession(issued.sessionId).pipe(Effect.ignore({ log: true })),
  );

const withCosCliLiveServerTimeout = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.timeout(COS_CLI_LIVE_SERVER_TIMEOUT));

const failLiveServerRequest = (cause: unknown) => {
  if (isEnvironmentHttpCommonError(cause)) {
    return Effect.fail(
      new CosCommandError({
        message: `Server request failed (${cause.code}, trace ${cause.traceId}).`,
      }),
    );
  }
  if (HttpClientError.isHttpClientError(cause) && cause.response !== undefined) {
    return Effect.fail(
      new CosCommandError({
        message: `Server request failed with undeclared status ${cause.response.status}.`,
      }),
    );
  }
  return Effect.fail(
    new CosCommandError({
      message: `Failed to call running server: ${String(cause)}.`,
    }),
  );
};

const makeLiveServerClient = (origin: string) =>
  HttpApiClient.make(EnvironmentHttpApi, {
    baseUrl: origin,
  });

const fetchLiveOrchestrationSnapshot = (origin: string, bearerToken: string) =>
  Effect.gen(function* () {
    const client = yield* makeLiveServerClient(origin);
    return yield* client.orchestration.snapshot({
      headers: { authorization: `Bearer ${bearerToken}` },
    });
  }).pipe(withCosCliLiveServerTimeout, Effect.catch(failLiveServerRequest));

const dispatchLiveOrchestrationCommand = (
  origin: string,
  bearerToken: string,
  command: CosCliDispatchCommand,
) =>
  Effect.gen(function* () {
    const client = yield* makeLiveServerClient(origin);
    yield* client.orchestration.dispatch({
      headers: { authorization: `Bearer ${bearerToken}` },
      payload: command,
    } as unknown as Parameters<typeof client.orchestration.dispatch>[0]);
  }).pipe(withCosCliLiveServerTimeout, Effect.catch(failLiveServerRequest));

// Offline (no live server) snapshot is folded from the engine's authoritative
// event log rather than the projection read model. The event log is the source
// of truth for active-turn state — every turn-lifecycle transition is appended
// there synchronously — so this avoids gating a restart on a snapshot that could
// trail the engine. This is the same fold the engine applies to build its own
// in-memory read model.
const getOfflineSnapshot = Effect.fn("getOfflineSnapshot")(function* () {
  const orchestrationEngine = yield* OrchestrationEngineService;
  const initial = createEmptyReadModel(DateTime.formatIso(yield* DateTime.now));
  return yield* orchestrationEngine
    .readEvents(0)
    .pipe(
      Stream.runFoldEffect(
        () => initial,
        (model, event) => projectEvent(model, event),
      ),
    );
});

const tryResolveLiveCosExecutionMode = Effect.fn("tryResolveLiveCosExecutionMode")(function* (
  environmentAuth: EnvironmentAuth.EnvironmentAuthShape,
  config: ServerConfigShape,
) {
  const runtimeState = yield* readPersistedServerRuntimeState(config.serverRuntimeStatePath);
  if (Option.isNone(runtimeState)) {
    return Option.none<{ readonly origin: string }>();
  }

  const attempt = withCosCliSessionToken(environmentAuth, (token) =>
    fetchLiveOrchestrationSnapshot(runtimeState.value.origin, token).pipe(
      Effect.as({ origin: runtimeState.value.origin }),
    ),
  );

  const attempted = yield* Effect.exit(attempt);
  if (Exit.isSuccess(attempted)) {
    return Option.some(attempted.value);
  }

  yield* clearPersistedServerRuntimeState(config.serverRuntimeStatePath);
  return Option.none<{ readonly origin: string }>();
});

// A thread is "busy" while its latest turn is still running, or while its
// provider session is spinning up or executing a turn.
const isThreadBusy = (thread: OrchestrationThread): boolean => {
  if (thread.deletedAt !== null) {
    return false;
  }
  if (thread.latestTurn?.state === "running") {
    return true;
  }
  const sessionStatus = thread.session?.status;
  return sessionStatus === "starting" || sessionStatus === "running";
};

const runCosCommand = Effect.fn("runCosCommand")(function* <A, E>(
  flags: CliAuthLocationFlags,
  run: (input: {
    readonly snapshot: OrchestrationReadModel;
    readonly dispatch: (
      command: CosCliDispatchCommand,
    ) => Effect.Effect<void, Error, FileSystem.FileSystem | HttpClient.HttpClient | Path.Path>;
  }) => Effect.Effect<A, E, FileSystem.FileSystem | HttpClient.HttpClient | Path.Path>,
) {
  const logLevel = yield* GlobalFlag.LogLevel;
  const config = yield* resolveCliAuthConfig(flags, logLevel);
  const minimumLogLevel = config.logLevel;

  return yield* Effect.gen(function* () {
    const environmentAuth = yield* EnvironmentAuth.EnvironmentAuth;
    const liveMode = yield* tryResolveLiveCosExecutionMode(environmentAuth, config);

    if (Option.isSome(liveMode)) {
      return yield* withCosCliSessionToken(environmentAuth, (token) =>
        Effect.gen(function* () {
          const snapshot = yield* fetchLiveOrchestrationSnapshot(liveMode.value.origin, token);
          return yield* run({
            snapshot,
            dispatch: (command) =>
              dispatchLiveOrchestrationCommand(liveMode.value.origin, token, command),
          });
        }),
      );
    }

    const offlineRuntimeLayer = CosCliRuntimeLive.pipe(
      Layer.provide(Layer.succeed(ServerConfig, config)),
      Layer.provide(Layer.succeed(References.MinimumLogLevel, minimumLogLevel)),
    );

    return yield* Effect.gen(function* () {
      const snapshot = yield* getOfflineSnapshot();
      const orchestrationEngine = yield* OrchestrationEngineService;
      return yield* run({
        snapshot,
        dispatch: (command) => orchestrationEngine.dispatch(command).pipe(Effect.asVoid),
      });
    }).pipe(Effect.provide(offlineRuntimeLayer));
  }).pipe(
    Effect.provide(
      Layer.mergeAll(EnvironmentAuth.runtimeLayer, WorkspacePathsLive).pipe(
        Layer.provideMerge(FetchHttpClient.layer),
        Layer.provide(Layer.succeed(ServerConfig, config)),
        Layer.provide(Layer.succeed(References.MinimumLogLevel, minimumLogLevel)),
      ),
    ),
  );
});

const cosCanRestartCommand = Command.make("can-restart", {
  ...projectLocationFlags,
}).pipe(
  Command.withDescription(
    "Print `yes` (exit 0) when no turn is in flight, `no` (non-zero) otherwise.",
  ),
  Command.withHandler((flags) =>
    runCosCommand(
      flags,
      Effect.fn("cosCanRestart")(function* ({
        snapshot,
      }: {
        readonly snapshot: OrchestrationReadModel;
      }) {
        const busy = snapshot.threads.some(isThreadBusy);
        if (busy) {
          yield* Console.log("no");
          return yield* new CosNotSafeToRestartError({
            message: "A turn is in flight; not safe to restart.",
          });
        }
        yield* Console.log("yes");
      }),
    ),
  ),
);

const cosWakeCommand = Command.make("wake", {
  ...projectLocationFlags,
  threadId: Argument.string("threadId").pipe(
    Argument.withDescription("Thread to wake with a turn after a restart."),
  ),
}).pipe(
  Command.withDescription("Wake a thread by dispatching a turn (idempotent)."),
  Command.withHandler((flags) =>
    runCosCommand(
      flags,
      Effect.fn("cosWake")(function* ({
        snapshot,
        dispatch,
      }: {
        readonly snapshot: OrchestrationReadModel;
        readonly dispatch: (
          command: CosCliDispatchCommand,
        ) => Effect.Effect<void, Error, FileSystem.FileSystem | HttpClient.HttpClient | Path.Path>;
      }) {
        const trimmed = flags.threadId.trim();
        if (trimmed.length === 0) {
          return yield* new CosCommandError({ message: "Thread id cannot be empty." });
        }
        const threadId = ThreadId.make(trimmed);
        // Key the wake on the thread's current observed state (its latest turn, or
        // the snapshot sequence when it has no turn yet). Retrying the SAME wake
        // (the thread hasn't advanced) re-derives the same commandId and is deduped
        // by the engine's command receipts; a LATER wake of a thread that has since
        // run and gone idle again derives a distinct commandId and is not swallowed.
        const thread = snapshot.threads.find((entry) => entry.id === threadId);
        const wakeDiscriminator = thread?.latestTurn?.turnId ?? `seq:${snapshot.snapshotSequence}`;
        const commandId = CommandId.make(`server:cos-wake:${threadId}:${wakeDiscriminator}`);
        const messageId = MessageId.make(`server:cos-wake:${threadId}:${wakeDiscriminator}`);
        yield* dispatch({
          type: "thread.turn.start",
          commandId,
          threadId,
          message: { messageId, role: "user", text: "Resume.", attachments: [] },
          runtimeMode: DEFAULT_RUNTIME_MODE,
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          bootstrap: undefined,
          createdAt: DateTime.formatIso(yield* DateTime.now),
        });
        yield* Console.log(`Woke thread ${threadId}.`);
      }),
    ),
  ),
);

export const cosCommand = Command.make("cos").pipe(
  Command.withDescription("Chief-of-staff orchestration controls."),
  Command.withSubcommands([cosCanRestartCommand, cosWakeCommand]),
);
