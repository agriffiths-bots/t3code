import {
  EnvironmentAuthorizationError,
  ORCHESTRATION_WS_METHODS,
  type EnvironmentId,
  type OrchestrationShellSnapshot,
  type OrchestrationShellStreamItem,
  type ServerConfig,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom } from "effect/unstable/reactivity";

import { EnvironmentRegistry } from "../connection/registry.ts";
import { connectionProjectionPhase } from "../connection/model.ts";
import { EnvironmentSupervisor } from "../connection/supervisor.ts";
import { safeErrorLogAttributes } from "../errors/safeLog.ts";
import { EnvironmentCacheStore } from "../platform/persistence.ts";
import { subscribe } from "../rpc/client.ts";
import { ShellSnapshotLoader } from "./shellSnapshotHttp.ts";
import { applyShellStreamEvent } from "./shellReducer.ts";
import type { EnvironmentCatalogState } from "./connections.ts";
import { followStreamInEnvironment } from "./runtime.ts";

export type EnvironmentShellStatus = "empty" | "cached" | "synchronizing" | "live";

export interface EnvironmentShellState {
  readonly snapshot: Option.Option<OrchestrationShellSnapshot>;
  readonly status: EnvironmentShellStatus;
  readonly error: Option.Option<string>;
}

const EMPTY_SHELL_STATE: EnvironmentShellState = {
  snapshot: Option.none(),
  status: "empty",
  error: Option.none(),
};

function shellStatusForSnapshot(
  snapshot: Option.Option<OrchestrationShellSnapshot>,
): EnvironmentShellStatus {
  return Option.isSome(snapshot) ? "cached" : "empty";
}

function synchronizingStatusForSnapshot(
  snapshot: Option.Option<OrchestrationShellSnapshot>,
): EnvironmentShellStatus {
  return Option.isSome(snapshot) ? "synchronizing" : "empty";
}

const SHELL_SYNCHRONIZATION_ERROR_MESSAGE = "Could not synchronize environment data.";
const SHELL_REPLAY_STALL_TIMEOUT = "5 seconds";
const SHELL_EXPECTED_FAILURE_RETRY_DELAY = "250 millis";
const isEnvironmentAuthorizationError = Schema.is(EnvironmentAuthorizationError);

function isTerminalShellSubscriptionFailure(cause: Cause.Cause<unknown>): boolean {
  return (
    cause.reasons.length > 0 &&
    cause.reasons.every(
      (reason) => reason._tag === "Fail" && isEnvironmentAuthorizationError(reason.error),
    )
  );
}

export const makeEnvironmentShellState = Effect.fn("EnvironmentShellState.make")(function* () {
  const supervisor = yield* EnvironmentSupervisor;
  const cache = yield* EnvironmentCacheStore;
  const snapshotLoader = yield* ShellSnapshotLoader;
  const environmentId = supervisor.target.environmentId;
  const cachedSnapshot = yield* cache.loadShell(environmentId).pipe(
    Effect.catch((error) =>
      Effect.logWarning("Could not load cached environment shell.").pipe(
        Effect.annotateLogs({
          environmentId,
          ...safeErrorLogAttributes(error),
        }),
        Effect.as(Option.none<OrchestrationShellSnapshot>()),
      ),
    ),
  );
  const state = yield* SubscriptionRef.make<EnvironmentShellState>({
    snapshot: cachedSnapshot,
    status: shellStatusForSnapshot(cachedSnapshot),
    error: Option.none(),
  });
  const persistence = yield* Queue.sliding<OrchestrationShellSnapshot>(1);
  const serverItemSeen = yield* Ref.make(false);
  const replayWatchdogEpoch = yield* Ref.make(0);
  const subscribeInput: { afterSequence?: number } = {};

  const persist = Effect.fn("EnvironmentShellState.persist")(function* (
    snapshot: OrchestrationShellSnapshot,
  ) {
    yield* cache.saveShell(environmentId, snapshot).pipe(
      Effect.catch((error) =>
        Effect.logWarning("Could not persist environment shell cache.").pipe(
          Effect.annotateLogs({
            environmentId,
            ...safeErrorLogAttributes(error),
          }),
        ),
      ),
    );
  });

  yield* Stream.fromQueue(persistence).pipe(
    Stream.debounce("500 millis"),
    Stream.runForEach(persist),
    Effect.forkScoped,
  );

  const setDisconnected = SubscriptionRef.update(state, (current) => ({
    ...current,
    status: shellStatusForSnapshot(current.snapshot),
  }));
  const setSynchronizing = SubscriptionRef.update(state, (current) => ({
    ...current,
    status: "synchronizing" as const,
    error: Option.none(),
  }));
  const setReady = SubscriptionRef.update(state, (current) =>
    current.status === "live"
      ? current
      : {
          ...current,
          status: "synchronizing" as const,
          error: Option.none(),
        },
  );
  const setStreamError = (error: unknown) =>
    Effect.logWarning("Could not synchronize the environment shell.").pipe(
      Effect.annotateLogs({
        environmentId,
        ...safeErrorLogAttributes(error),
      }),
      Effect.andThen(
        SubscriptionRef.update(state, (current) => ({
          ...current,
          status: synchronizingStatusForSnapshot(current.snapshot),
          error: Option.some(SHELL_SYNCHRONIZATION_ERROR_MESSAGE),
        })),
      ),
    );

  const setExpectedStreamError = Effect.fn("EnvironmentShellState.setExpectedStreamError")(
    function* (cause: Cause.Cause<unknown>) {
      yield* setStreamError(Cause.squash(cause));
      if (isTerminalShellSubscriptionFailure(cause)) {
        return yield* Effect.never;
      }
    },
  );

  const applyItem = Effect.fn("EnvironmentShellState.applyItem")(function* (
    item: OrchestrationShellStreamItem,
  ) {
    if (item.kind === "caught-up") {
      const current = yield* SubscriptionRef.get(state);
      if (Option.isNone(current.snapshot)) {
        yield* SubscriptionRef.set(state, {
          ...current,
          error: Option.none(),
        });
        return;
      }

      const nextSnapshot =
        item.sequence > current.snapshot.value.snapshotSequence
          ? { ...current.snapshot.value, snapshotSequence: item.sequence }
          : current.snapshot.value;
      yield* SubscriptionRef.set(state, {
        snapshot: Option.some(nextSnapshot),
        status: "live",
        error: Option.none(),
      });
      subscribeInput.afterSequence = nextSnapshot.snapshotSequence;
      if (nextSnapshot !== current.snapshot.value) {
        yield* Queue.offer(persistence, nextSnapshot);
      }
      return;
    }

    const current = yield* SubscriptionRef.get(state);
    const nextSnapshot =
      item.kind === "snapshot"
        ? Option.match(current.snapshot, {
            onNone: () => item.snapshot,
            onSome: (snapshot) =>
              item.force === true || item.snapshot.snapshotSequence >= snapshot.snapshotSequence
                ? item.snapshot
                : snapshot,
          })
        : Option.match(current.snapshot, {
            onNone: () => null,
            onSome: (snapshot) =>
              item.sequence > snapshot.snapshotSequence
                ? applyShellStreamEvent(snapshot, item)
                : snapshot,
          });
    if (nextSnapshot === null) {
      return;
    }

    yield* SubscriptionRef.set(state, {
      snapshot: Option.some(nextSnapshot),
      status: "live",
      error: Option.none(),
    });
    subscribeInput.afterSequence = nextSnapshot.snapshotSequence;
    yield* Queue.offer(persistence, nextSnapshot);
  });

  const applyRecoverySnapshot = Effect.fn("EnvironmentShellState.applyRecoverySnapshot")(function* (
    snapshot: OrchestrationShellSnapshot,
  ) {
    const current = yield* SubscriptionRef.get(state);
    const nextSnapshot = Option.match(current.snapshot, {
      onNone: () => snapshot,
      onSome: (currentSnapshot) =>
        snapshot.snapshotSequence >= currentSnapshot.snapshotSequence ? snapshot : currentSnapshot,
    });

    yield* SubscriptionRef.set(state, {
      snapshot: Option.some(nextSnapshot),
      status: "synchronizing",
      error: Option.none(),
    });
    subscribeInput.afterSequence = nextSnapshot.snapshotSequence;
    if (Option.isNone(current.snapshot) || nextSnapshot !== current.snapshot.value) {
      yield* Queue.offer(persistence, nextSnapshot);
    }
  });

  const recoverFromStalledReplay = Effect.fn("EnvironmentShellState.recoverFromStalledReplay")(
    function* (epoch: number) {
      if ((yield* Ref.get(replayWatchdogEpoch)) !== epoch || (yield* Ref.get(serverItemSeen))) {
        return;
      }

      yield* SubscriptionRef.update(state, (current) => ({
        ...current,
        status: Option.isSome(current.snapshot) ? "synchronizing" : current.status,
      }));

      const prepared = yield* SubscriptionRef.get(supervisor.prepared);
      if (Option.isNone(prepared)) {
        yield* setStreamError(new Error("Shell replay stalled before the server caught up."));
        return;
      }

      const snapshot = yield* snapshotLoader.load(prepared.value);
      if ((yield* Ref.get(replayWatchdogEpoch)) !== epoch || (yield* Ref.get(serverItemSeen))) {
        return;
      }
      if (Option.isSome(snapshot)) {
        yield* applyRecoverySnapshot(snapshot.value);
        return;
      }

      yield* setStreamError(
        new Error("Shell replay stalled and snapshot refresh was unavailable."),
      );
    },
  );

  const armReplayWatchdog = Effect.fn("EnvironmentShellState.armReplayWatchdog")(function* () {
    const current = yield* SubscriptionRef.get(state);
    if (Option.isNone(current.snapshot)) {
      return;
    }

    const epoch = yield* Ref.updateAndGet(replayWatchdogEpoch, (value) => value + 1);
    yield* Ref.set(serverItemSeen, false);
    yield* Effect.sleep(SHELL_REPLAY_STALL_TIMEOUT).pipe(
      Effect.andThen(recoverFromStalledReplay(epoch)),
      Effect.forkScoped,
    );
  });

  yield* Effect.forkScoped(
    Effect.gen(function* () {
      // Establish the base shell snapshot to resume from, minimizing bytes over
      // the wire:
      // - Warm cache: reuse the cached snapshot (zero network) and resume via
      //   `afterSequence` so we only receive shell events since the cached
      //   sequence.
      // - Cold cache: load the full shell snapshot over HTTP (gzip-compressible,
      //   and off the socket), then resume via `afterSequence`.
      // If no base can be established we fall back to the socket-embedded
      // snapshot so the shell still synchronizes. Overlapping/replayed events are
      // deduped by sequence in applyItem.
      const base = Option.isSome(cachedSnapshot)
        ? cachedSnapshot
        : yield* Effect.gen(function* () {
            const prepared = yield* SubscriptionRef.changes(supervisor.prepared).pipe(
              Stream.filter(Option.isSome),
              Stream.map((current) => current.value),
              Stream.runHead,
            );
            return Option.isSome(prepared)
              ? yield* snapshotLoader.load(prepared.value)
              : Option.none<OrchestrationShellSnapshot>();
          });

      if (Option.isSome(base)) {
        yield* applyItem({ kind: "snapshot", snapshot: base.value });
        yield* armReplayWatchdog();
      }

      Option.match(base, {
        onNone: () => {
          delete subscribeInput.afterSequence;
        },
        onSome: (snapshot) => {
          subscribeInput.afterSequence = snapshot.snapshotSequence;
        },
      });

      yield* subscribe(ORCHESTRATION_WS_METHODS.subscribeShell, subscribeInput, {
        onExpectedFailure: setExpectedStreamError,
        retryExpectedFailureAfter: SHELL_EXPECTED_FAILURE_RETRY_DELAY,
      }).pipe(
        Stream.tap(() => Ref.set(serverItemSeen, true)),
        Stream.catchCause((cause) =>
          Stream.fromEffect(setStreamError(Cause.squash(cause))).pipe(Stream.drain),
        ),
        Stream.runForEach(applyItem),
      );
    }),
  );
  yield* SubscriptionRef.changes(supervisor.state).pipe(
    Stream.runForEach((connectionState) => {
      switch (connectionProjectionPhase(connectionState)) {
        case "synchronizing":
          return setSynchronizing.pipe(Effect.andThen(armReplayWatchdog()));
        case "disconnected":
          return setDisconnected;
        case "ready":
          return setReady;
      }
    }),
    Effect.forkScoped,
  );

  return state;
});

export function shellStateChanges(environmentId: EnvironmentId) {
  return followStreamInEnvironment(
    environmentId,
    Stream.unwrap(makeEnvironmentShellState().pipe(Effect.map(SubscriptionRef.changes))),
  );
}

export interface EnvironmentShellSummary {
  readonly hasSnapshot: boolean;
  readonly hasSynchronizingShell: boolean;
  readonly hasCachedShell: boolean;
  readonly hasLiveShell: boolean;
  readonly firstError: string | null;
  readonly latestSnapshotUpdatedAt: string | null;
}

const EMPTY_ENVIRONMENT_SHELL_SUMMARY: EnvironmentShellSummary = Object.freeze({
  hasSnapshot: false,
  hasSynchronizingShell: false,
  hasCachedShell: false,
  hasLiveShell: false,
  firstError: null,
  latestSnapshotUpdatedAt: null,
});

const EMPTY_SERVER_CONFIGS: ReadonlyMap<EnvironmentId, ServerConfig> = new Map();

function shellSummariesEqual(
  left: EnvironmentShellSummary,
  right: EnvironmentShellSummary,
): boolean {
  return (
    left.hasSnapshot === right.hasSnapshot &&
    left.hasSynchronizingShell === right.hasSynchronizingShell &&
    left.hasCachedShell === right.hasCachedShell &&
    left.hasLiveShell === right.hasLiveShell &&
    left.firstError === right.firstError &&
    left.latestSnapshotUpdatedAt === right.latestSnapshotUpdatedAt
  );
}

function mapsEqual<K, V>(left: ReadonlyMap<K, V>, right: ReadonlyMap<K, V>): boolean {
  if (left.size !== right.size) {
    return false;
  }
  for (const [key, value] of left) {
    if (right.get(key) !== value) {
      return false;
    }
  }
  return true;
}

export function createEnvironmentShellSummaryAtom(input: {
  readonly catalogValueAtom: Atom.Atom<EnvironmentCatalogState>;
  readonly shellStateValueAtom: (environmentId: EnvironmentId) => Atom.Atom<EnvironmentShellState>;
}) {
  let previousSummary = EMPTY_ENVIRONMENT_SHELL_SUMMARY;
  return Atom.make((get) => {
    let hasSnapshot = false;
    let hasSynchronizingShell = false;
    let hasCachedShell = false;
    let hasLiveShell = false;
    let firstError: string | null = null;
    let latestSnapshotUpdatedAt: string | null = null;

    for (const environmentId of get(input.catalogValueAtom).entries.keys()) {
      const state = get(input.shellStateValueAtom(environmentId));
      hasSynchronizingShell ||= state.status === "synchronizing";
      hasCachedShell ||= state.status === "cached";
      hasLiveShell ||= state.status === "live";
      if (firstError === null) {
        firstError = Option.getOrNull(state.error);
      }
      if (Option.isNone(state.snapshot)) {
        continue;
      }
      hasSnapshot = true;
      const updatedAt = state.snapshot.value.updatedAt;
      if (latestSnapshotUpdatedAt === null || updatedAt > latestSnapshotUpdatedAt) {
        latestSnapshotUpdatedAt = updatedAt;
      }
    }

    const next: EnvironmentShellSummary = {
      hasSnapshot,
      hasSynchronizingShell,
      hasCachedShell,
      hasLiveShell,
      firstError,
      latestSnapshotUpdatedAt,
    };
    if (shellSummariesEqual(previousSummary, next)) {
      return previousSummary;
    }
    previousSummary = next;
    return previousSummary;
  }).pipe(Atom.withLabel("environment-shell-summary"));
}

export function createEnvironmentServerConfigsAtom(input: {
  readonly catalogValueAtom: Atom.Atom<EnvironmentCatalogState>;
  readonly serverConfigValueAtom: (environmentId: EnvironmentId) => Atom.Atom<ServerConfig | null>;
}) {
  let previousServerConfigs = EMPTY_SERVER_CONFIGS;
  return Atom.make((get) => {
    const next = new Map<EnvironmentId, ServerConfig>();
    for (const environmentId of get(input.catalogValueAtom).entries.keys()) {
      const config = get(input.serverConfigValueAtom(environmentId));
      if (config !== null) {
        next.set(environmentId, config);
      }
    }
    if (mapsEqual(previousServerConfigs, next)) {
      return previousServerConfigs;
    }
    previousServerConfigs = next;
    return previousServerConfigs;
  }).pipe(Atom.withLabel("environment-server-configs"));
}

export function createEnvironmentShellAtoms<R, E>(
  runtime: Atom.AtomRuntime<
    EnvironmentRegistry | EnvironmentCacheStore | ShellSnapshotLoader | R,
    E
  >,
) {
  const stateAtom = Atom.family((environmentId: EnvironmentId) =>
    runtime.atom(shellStateChanges(environmentId), {
      initialValue: EMPTY_SHELL_STATE,
    }),
  );

  const stateValueAtom = Atom.family((environmentId: EnvironmentId) =>
    Atom.make((get) =>
      Option.getOrElse(AsyncResult.value(get(stateAtom(environmentId))), () => EMPTY_SHELL_STATE),
    ).pipe(Atom.withLabel(`environment-shell-state-value:${environmentId}`)),
  );

  return {
    stateAtom,
    stateValueAtom,
  };
}

export * from "./models.ts";
export * from "./shellCommands.ts";
export * from "./shellReducer.ts";
export * from "./shellSnapshotHttp.ts";
export * from "./snapshots.ts";
