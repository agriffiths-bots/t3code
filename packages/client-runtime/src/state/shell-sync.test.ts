import {
  EnvironmentAuthorizationError,
  EnvironmentId,
  ORCHESTRATION_WS_METHODS,
  OrchestrationGetSnapshotError,
  type OrchestrationShellSnapshot,
  type OrchestrationShellStreamItem,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";

import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "../connection/model.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import * as Persistence from "../platform/persistence.ts";
import * as RpcSession from "../rpc/session.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import { makeEnvironmentShellState, ShellSnapshotLoader } from "./shell.ts";

const TARGET = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("environment-1"),
  label: "Test environment",
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
});

const PREPARED: PreparedConnection = {
  environmentId: TARGET.environmentId,
  label: TARGET.label,
  httpBaseUrl: TARGET.httpBaseUrl,
  socketUrl: TARGET.wsBaseUrl,
  httpAuthorization: null,
  target: TARGET,
};

const LIVE_SHELL_SNAPSHOT: OrchestrationShellSnapshot = {
  snapshotSequence: 1,
  projects: [],
  threads: [],
  updatedAt: "2026-06-06T00:00:00.000Z",
};

function session(client: WsRpcProtocolClient): RpcSession.RpcSession {
  return {
    client,
    initialConfig: Effect.never,
    ready: Effect.void,
    probe: Effect.void,
    closed: Effect.never,
  };
}

describe("environment shell synchronization", () => {
  it.effect("publishes live state before persistence and preserves it when ready", () =>
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<OrchestrationShellStreamItem>();
      const client = {
        [ORCHESTRATION_WS_METHODS.subscribeShell]: () => Stream.fromQueue(events),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.none()),
        saveShell: () => Effect.never,
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      // Cold cache with no HTTP snapshot available → falls back to the
      // socket-embedded snapshot.
      const snapshotLoader = ShellSnapshotLoader.of({
        load: () => Effect.succeed(Option.none()),
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader, snapshotLoader),
      );

      yield* SubscriptionRef.set(supervisorState, {
        desired: true,
        network: "online",
        phase: "connecting",
        stage: "synchronizing",
        attempt: 1,
        generation: 0,
        lastFailure: null,
        retryAt: null,
      });
      yield* Queue.offer(events, {
        kind: "snapshot",
        snapshot: LIVE_SHELL_SNAPSHOT,
      });
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => state.status === "live"),
        Stream.runHead,
      );

      yield* SubscriptionRef.set(supervisorState, {
        desired: true,
        network: "online",
        phase: "connected",
        stage: null,
        attempt: 1,
        generation: 1,
        lastFailure: null,
        retryAt: null,
      });
      for (let index = 0; index < 10; index += 1) {
        yield* Effect.yieldNow;
      }

      const state = yield* SubscriptionRef.get(shellState);
      expect(state.status).toBe("live");
      expect(Option.getOrThrow(state.snapshot)).toEqual(LIVE_SHELL_SNAPSHOT);
    }),
  );

  it.effect("resumes a warm shell cache via afterSequence without an HTTP fetch", () =>
    Effect.gen(function* () {
      const cachedSnapshot: OrchestrationShellSnapshot = {
        snapshotSequence: 5,
        projects: [],
        threads: [],
        updatedAt: "2026-06-06T00:00:00.000Z",
      };
      const events = yield* Queue.unbounded<OrchestrationShellStreamItem>();
      const capturedAfterSequence = yield* SubscriptionRef.make<number | undefined>(undefined);
      const loaderCalls = yield* SubscriptionRef.make(0);
      const client = {
        [ORCHESTRATION_WS_METHODS.subscribeShell]: (input: { readonly afterSequence?: number }) =>
          Stream.unwrap(
            SubscriptionRef.set(capturedAfterSequence, input.afterSequence).pipe(
              Effect.as(Stream.fromQueue(events)),
            ),
          ),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.some(cachedSnapshot)),
        saveShell: () => Effect.void,
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.of({
        load: () =>
          SubscriptionRef.update(loaderCalls, (count) => count + 1).pipe(Effect.as(Option.none())),
      });
      yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader, snapshotLoader),
      );

      // Wait until the subscription is established from the warm cache.
      yield* SubscriptionRef.changes(capturedAfterSequence).pipe(
        Stream.filter((value) => value !== undefined),
        Stream.runHead,
      );

      expect(yield* SubscriptionRef.get(capturedAfterSequence)).toBe(5);
      expect(yield* SubscriptionRef.get(loaderCalls)).toBe(0);
    }),
  );

  it.effect("moves a stalled warm replay out of live state", () =>
    Effect.gen(function* () {
      const cachedSnapshot: OrchestrationShellSnapshot = {
        snapshotSequence: 5,
        projects: [],
        threads: [],
        updatedAt: "2026-06-06T00:00:00.000Z",
      };
      const client = {
        [ORCHESTRATION_WS_METHODS.subscribeShell]: () => Stream.never,
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.some(cachedSnapshot)),
        saveShell: () => Effect.void,
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.of({
        load: () => Effect.never,
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader, snapshotLoader),
      );

      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => state.status === "live"),
        Stream.runHead,
      );
      yield* TestClock.adjust("5 seconds");
      yield* Effect.yieldNow;

      const state = yield* SubscriptionRef.get(shellState);
      expect(state.status).toBe("synchronizing");
      expect(Option.getOrThrow(state.snapshot)).toEqual(cachedSnapshot);
    }),
  );

  it.effect("moves a failed warm replay out of live state without retrying denied access", () =>
    Effect.gen(function* () {
      const cachedSnapshot: OrchestrationShellSnapshot = {
        snapshotSequence: 5,
        projects: [],
        threads: [],
        updatedAt: "2026-06-06T00:00:00.000Z",
      };
      const subscriptionCalls = yield* SubscriptionRef.make(0);
      const client = {
        [ORCHESTRATION_WS_METHODS.subscribeShell]: () =>
          Stream.fromEffect(SubscriptionRef.update(subscriptionCalls, (count) => count + 1)).pipe(
            Stream.drain,
            Stream.concat(
              Stream.fail(
                new EnvironmentAuthorizationError({
                  message: "Denied",
                  requiredScope: "orchestration:read",
                }),
              ),
            ),
          ),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.some(cachedSnapshot)),
        saveShell: () => Effect.void,
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.of({
        load: () => Effect.never,
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader, snapshotLoader),
      );

      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => Option.isSome(state.error)),
        Stream.runHead,
      );

      const state = yield* SubscriptionRef.get(shellState);
      expect(state.status).toBe("synchronizing");
      expect(state.error).toEqual(Option.some("Could not synchronize environment data."));
      expect(Option.getOrThrow(state.snapshot)).toEqual(cachedSnapshot);
      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      expect(yield* SubscriptionRef.get(subscriptionCalls)).toBe(1);
    }),
  );

  it.effect("retries a recoverable warm replay failure and returns live", () =>
    Effect.gen(function* () {
      const cachedSnapshot: OrchestrationShellSnapshot = {
        snapshotSequence: 5,
        projects: [],
        threads: [],
        updatedAt: "2026-06-06T00:00:00.000Z",
      };
      const subscriptionCalls = yield* SubscriptionRef.make(0);
      const client = {
        [ORCHESTRATION_WS_METHODS.subscribeShell]: () =>
          Stream.unwrap(
            SubscriptionRef.updateAndGet(subscriptionCalls, (count) => count + 1).pipe(
              Effect.map((call) =>
                call === 1
                  ? Stream.fail(
                      new OrchestrationGetSnapshotError({
                        message: "Replay projection failed",
                      }),
                    )
                  : Stream.succeed({
                      kind: "caught-up",
                      sequence: 6,
                    } satisfies OrchestrationShellStreamItem),
              ),
            ),
          ),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const savedSequence = yield* SubscriptionRef.make(cachedSnapshot.snapshotSequence);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.some(cachedSnapshot)),
        saveShell: (_environmentId, snapshot) =>
          SubscriptionRef.set(savedSequence, snapshot.snapshotSequence),
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.of({
        load: () => Effect.never,
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader, snapshotLoader),
      );

      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => Option.isSome(state.error)),
        Stream.runHead,
      );
      expect(yield* SubscriptionRef.get(subscriptionCalls)).toBe(1);

      yield* TestClock.adjust("250 millis");
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => state.status === "live"),
        Stream.runHead,
      );

      const state = yield* SubscriptionRef.get(shellState);
      expect(state.status).toBe("live");
      expect(state.error).toEqual(Option.none());
      expect(Option.getOrThrow(state.snapshot).snapshotSequence).toBe(6);
      yield* TestClock.adjust("500 millis");
      expect(yield* SubscriptionRef.get(savedSequence)).toBe(6);
      expect(yield* SubscriptionRef.get(subscriptionCalls)).toBe(2);
    }),
  );

  it.effect("keeps an acknowledged idle warm replay live without snapshot recovery", () =>
    Effect.gen(function* () {
      const cachedSnapshot: OrchestrationShellSnapshot = {
        snapshotSequence: 5,
        projects: [],
        threads: [],
        updatedAt: "2026-06-06T00:00:00.000Z",
      };
      const acknowledgedSequence = 10;
      const events = yield* Queue.unbounded<OrchestrationShellStreamItem>();
      const client = {
        [ORCHESTRATION_WS_METHODS.subscribeShell]: () => Stream.fromQueue(events),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const savedSequence = yield* SubscriptionRef.make(cachedSnapshot.snapshotSequence);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.some(cachedSnapshot)),
        saveShell: (_environmentId, snapshot) =>
          SubscriptionRef.update(savedSequence, (current) =>
            Math.max(current, snapshot.snapshotSequence),
          ),
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const loaderCalls = yield* SubscriptionRef.make(0);
      const snapshotLoader = ShellSnapshotLoader.of({
        load: () =>
          SubscriptionRef.updateAndGet(loaderCalls, (count) => count + 1).pipe(
            Effect.as(Option.none()),
          ),
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader, snapshotLoader),
      );

      yield* Queue.offer(events, { kind: "caught-up", sequence: acknowledgedSequence });
      for (let index = 0; index < 10; index += 1) {
        yield* Effect.yieldNow;
      }
      yield* TestClock.adjust("6 seconds");
      yield* Effect.yieldNow;

      const state = yield* SubscriptionRef.get(shellState);
      expect(state.status).toBe("live");
      expect(state.error).toEqual(Option.none());
      expect(Option.getOrThrow(state.snapshot)).toEqual({
        ...cachedSnapshot,
        snapshotSequence: acknowledgedSequence,
      });
      expect(yield* SubscriptionRef.get(savedSequence)).toBe(acknowledgedSequence);
      expect(yield* SubscriptionRef.get(loaderCalls)).toBe(0);
    }),
  );

  it.effect("accepts a forced snapshot when the server cursor moved backwards", () =>
    Effect.gen(function* () {
      const cachedSnapshot: OrchestrationShellSnapshot = {
        snapshotSequence: 10,
        projects: [],
        threads: [],
        updatedAt: "2026-06-06T00:00:00.000Z",
      };
      const resetSnapshot: OrchestrationShellSnapshot = {
        snapshotSequence: 3,
        projects: [],
        threads: [],
        updatedAt: "2026-06-06T00:00:01.000Z",
      };
      const events = yield* Queue.unbounded<OrchestrationShellStreamItem>();
      const capturedAfterSequences: Array<number | undefined> = [];
      const client = {
        [ORCHESTRATION_WS_METHODS.subscribeShell]: (input: { readonly afterSequence?: number }) => {
          capturedAfterSequences.push(input.afterSequence);
          return Stream.fromQueue(events);
        },
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.some(cachedSnapshot)),
        saveShell: () => Effect.void,
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.of({
        load: () => Effect.never,
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader, snapshotLoader),
      );

      yield* Queue.offer(events, { kind: "snapshot", snapshot: resetSnapshot, force: true });
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter(
          (state) =>
            state.status === "live" &&
            Option.isSome(state.snapshot) &&
            state.snapshot.value.snapshotSequence === resetSnapshot.snapshotSequence,
        ),
        Stream.runHead,
      );

      expect(Option.getOrThrow((yield* SubscriptionRef.get(shellState)).snapshot)).toEqual(
        resetSnapshot,
      );
      yield* SubscriptionRef.set(activeSession, Option.none());
      yield* Effect.yieldNow;
      yield* SubscriptionRef.set(activeSession, Option.some(session(client)));
      for (let index = 0; index < 10; index += 1) {
        yield* Effect.yieldNow;
      }
      expect(capturedAfterSequences).toEqual([
        cachedSnapshot.snapshotSequence,
        resetSnapshot.snapshotSequence,
      ]);
    }),
  );

  it.effect(
    "keeps HTTP recovery snapshots in synchronizing state until the socket catches up",
    () =>
      Effect.gen(function* () {
        const cachedSnapshot: OrchestrationShellSnapshot = {
          snapshotSequence: 5,
          projects: [],
          threads: [],
          updatedAt: "2026-06-06T00:00:00.000Z",
        };
        const recoverySnapshot: OrchestrationShellSnapshot = {
          snapshotSequence: 6,
          projects: [],
          threads: [],
          updatedAt: "2026-06-06T00:00:01.000Z",
        };
        const client = {
          [ORCHESTRATION_WS_METHODS.subscribeShell]: () => Stream.never,
        } as unknown as WsRpcProtocolClient;
        const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
        const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
          Option.some(session(client)),
        );
        const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
          target: TARGET,
          state: supervisorState,
          session: activeSession,
          prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
          connect: Effect.void,
          disconnect: Effect.void,
          retryNow: Effect.void,
        } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
        const cache = Persistence.EnvironmentCacheStore.of({
          loadShell: () => Effect.succeed(Option.some(cachedSnapshot)),
          saveShell: () => Effect.void,
          loadThread: () => Effect.succeed(Option.none()),
          saveThread: () => Effect.void,
          removeThread: () => Effect.void,
          loadServerConfig: () => Effect.succeed(Option.none()),
          saveServerConfig: () => Effect.void,
          loadVcsRefs: () => Effect.succeed(Option.none()),
          saveVcsRefs: () => Effect.void,
          clear: () => Effect.void,
        });
        const snapshotLoader = ShellSnapshotLoader.of({
          load: () => Effect.succeed(Option.some(recoverySnapshot)),
        });
        const shellState = yield* makeEnvironmentShellState().pipe(
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
          Effect.provideService(Persistence.EnvironmentCacheStore, cache),
          Effect.provideService(ShellSnapshotLoader, snapshotLoader),
        );

        yield* TestClock.adjust("5 seconds");
        yield* Effect.yieldNow;

        const state = yield* SubscriptionRef.get(shellState);
        expect(state.status).toBe("synchronizing");
        expect(Option.getOrThrow(state.snapshot)).toEqual(recoverySnapshot);
      }),
  );

  it.effect("rearms stalled replay recovery when the environment reconnects", () =>
    Effect.gen(function* () {
      const cachedSnapshot: OrchestrationShellSnapshot = {
        snapshotSequence: 5,
        projects: [],
        threads: [],
        updatedAt: "2026-06-06T00:00:00.000Z",
      };
      const recoverySnapshot: OrchestrationShellSnapshot = {
        snapshotSequence: 8,
        projects: [],
        threads: [],
        updatedAt: "2026-06-06T00:00:01.000Z",
      };
      const events = yield* Queue.unbounded<OrchestrationShellStreamItem>();
      const client = {
        [ORCHESTRATION_WS_METHODS.subscribeShell]: () => Stream.fromQueue(events),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.some(cachedSnapshot)),
        saveShell: () => Effect.void,
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.of({
        load: () => Effect.succeed(Option.some(recoverySnapshot)),
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader, snapshotLoader),
      );

      yield* Queue.offer(events, { kind: "caught-up", sequence: cachedSnapshot.snapshotSequence });
      for (let index = 0; index < 10; index += 1) {
        yield* Effect.yieldNow;
      }
      yield* SubscriptionRef.set(supervisorState, {
        desired: true,
        network: "online",
        phase: "connecting",
        stage: "synchronizing",
        attempt: 2,
        generation: 1,
        lastFailure: null,
        retryAt: null,
      });
      yield* TestClock.adjust("5 seconds");
      yield* Effect.yieldNow;

      const state = yield* SubscriptionRef.get(shellState);
      expect(state.status).toBe("synchronizing");
      expect(Option.getOrThrow(state.snapshot)).toEqual(recoverySnapshot);
    }),
  );

  it.effect("does not let stalled replay recovery overwrite a newer socket snapshot", () =>
    Effect.gen(function* () {
      const cachedSnapshot: OrchestrationShellSnapshot = {
        snapshotSequence: 5,
        projects: [],
        threads: [],
        updatedAt: "2026-06-06T00:00:00.000Z",
      };
      const recoverySnapshot: OrchestrationShellSnapshot = {
        snapshotSequence: 6,
        projects: [],
        threads: [],
        updatedAt: "2026-06-06T00:00:01.000Z",
      };
      const socketSnapshot: OrchestrationShellSnapshot = {
        snapshotSequence: 7,
        projects: [],
        threads: [],
        updatedAt: "2026-06-06T00:00:02.000Z",
      };
      const events = yield* Queue.unbounded<OrchestrationShellStreamItem>();
      const client = {
        [ORCHESTRATION_WS_METHODS.subscribeShell]: () => Stream.fromQueue(events),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.some(cachedSnapshot)),
        saveShell: () => Effect.void,
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.of({
        load: () => Effect.sleep("1 second").pipe(Effect.as(Option.some(recoverySnapshot))),
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader, snapshotLoader),
      );

      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => state.status === "live"),
        Stream.runHead,
      );
      yield* TestClock.adjust("5 seconds");
      yield* Queue.offer(events, { kind: "snapshot", snapshot: socketSnapshot });
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter(
          (state) =>
            state.status === "live" &&
            Option.isSome(state.snapshot) &&
            state.snapshot.value.snapshotSequence === socketSnapshot.snapshotSequence,
        ),
        Stream.runHead,
      );

      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;

      const state = yield* SubscriptionRef.get(shellState);
      expect(Option.getOrThrow(state.snapshot)).toEqual(socketSnapshot);
    }),
  );
});
