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
import * as Context from "effect/Context";
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

import { ServerConfig } from "../config.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { createEmptyReadModel, projectEvent } from "../orchestration/projector.ts";
import { OrchestrationLayerLive } from "../orchestration/runtimeLayer.ts";
import { layerConfig as SqlitePersistenceLayerLive } from "../persistence/Layers/Sqlite.ts";
import { layer as RepositoryIdentityResolverLive } from "../project/RepositoryIdentityResolver.ts";
import {
  clearPersistedServerRuntimeState,
  readPersistedServerRuntimeState,
} from "../serverRuntimeState.ts";
import { layer as WorkspacePathsLive } from "../workspace/WorkspacePaths.ts";
import { type CliAuthLocationFlags, projectLocationFlags, resolveCliAuthConfig } from "./config.ts";

type CosCliDispatchCommand = Extract<
  OrchestrationCommand,
  { type: "thread.turn.start" } | { type: "thread.parent.set" }
>;

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
  environmentAuth: Context.Service.Shape<typeof EnvironmentAuth.EnvironmentAuth>,
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
  environmentAuth: Context.Service.Shape<typeof EnvironmentAuth.EnvironmentAuth>,
  config: Context.Service.Shape<typeof ServerConfig>,
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

// How recently a thread must have been updated to count as genuinely "live".
// Orphan turns from a prior crash (no terminal event was ever written) keep a
// "running" session in an offline event-fold forever; this window excludes them
// while staying generous enough that a real long-running turn (which emits
// progress events well within 2h) is never mistaken for idle. This is only
// load-bearing in the offline-fold path; when the live server is reachable the
// snapshot reflects real session state. Pairs with the restart script's 20s
// HTTP drain + sub-second recheck.
const THREAD_BUSY_RECENCY_MS = 2 * 60 * 60 * 1000;

// A thread is "busy" only while it has a LIVE provider session (spinning up or
// executing a turn) AND has been active recently. We deliberately do NOT treat
// `latestTurn.state==="running"` as busy on its own: a turn left "running" with
// no live session is an orphan from a prior crash/restart and must never block a
// restart forever. The recency guard additionally drops stale orphans whose
// session also never received a terminal event in the offline fold. A genuinely
// long-running turn keeps its session "running" and emits events, so it stays
// busy — orphan-safe and long-turn-safe.
const isThreadBusy = (thread: OrchestrationThread, nowMs: number): boolean => {
  if (thread.deletedAt !== null) {
    return false;
  }
  const sessionStatus = thread.session?.status;
  if (sessionStatus !== "starting" && sessionStatus !== "running") {
    return false;
  }
  const updatedMs = thread.updatedAt ? Date.parse(thread.updatedAt) : 0;
  return Number.isFinite(updatedMs) && nowMs - updatedMs < THREAD_BUSY_RECENCY_MS;
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
        const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
        const busy = snapshot.threads.some((thread) => isThreadBusy(thread, nowMs));
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

// `link-parent` dispatches a `thread.parent.set` through the SAME engine path as the
// sub-agent spawn handler's dispatchParentSet(). It is used by the update GATE
// (apps/server/e2e/drive.sh) to exercise the genuine OrchestrationEngine -> decider
// (requireThread on BOTH child + parent) -> ProjectionPipeline parent_thread_id
// projection on a RESTORED PROD DB, WITHOUT needing a live AI provider. A broken
// decider, projector, or missing parent_thread_id migration makes this fail (the gate
// is fail-closed). It is idempotent: the engine dedupes by the derived commandId.
const cosLinkParentCommand = Command.make("link-parent", {
  ...projectLocationFlags,
  childThreadId: Argument.string("childThreadId").pipe(
    Argument.withDescription("Child thread to link under a parent."),
  ),
  parentThreadId: Argument.string("parentThreadId").pipe(
    Argument.withDescription("Parent thread the child is linked under."),
  ),
}).pipe(
  Command.withDescription(
    "Link a child thread under a parent via thread.parent.set (idempotent; gate/spawn linkage path).",
  ),
  Command.withHandler((flags) =>
    runCosCommand(
      flags,
      Effect.fn("cosLinkParent")(function* ({
        dispatch,
      }: {
        readonly snapshot: OrchestrationReadModel;
        readonly dispatch: (
          command: CosCliDispatchCommand,
        ) => Effect.Effect<void, Error, FileSystem.FileSystem | HttpClient.HttpClient | Path.Path>;
      }) {
        const child = flags.childThreadId.trim();
        const parent = flags.parentThreadId.trim();
        if (child.length === 0 || parent.length === 0) {
          return yield* new CosCommandError({ message: "Child and parent thread ids cannot be empty." });
        }
        const childThreadId = ThreadId.make(child);
        const parentThreadId = ThreadId.make(parent);
        const commandId = CommandId.make(`server:cos-link-parent:${childThreadId}:${parentThreadId}`);
        yield* dispatch({
          type: "thread.parent.set",
          commandId,
          threadId: childThreadId,
          parentThreadId,
          createdAt: DateTime.formatIso(yield* DateTime.now),
        });
        yield* Console.log(`Linked thread ${childThreadId} under ${parentThreadId}.`);
      }),
    ),
  ),
);

export const cosCommand = Command.make("cos").pipe(
  Command.withDescription("Chief-of-staff orchestration controls."),
  Command.withSubcommands([cosCanRestartCommand, cosWakeCommand, cosLinkParentCommand]),
);
