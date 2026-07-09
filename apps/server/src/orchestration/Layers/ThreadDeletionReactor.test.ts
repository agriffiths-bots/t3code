import {
  CommandId,
  EventId,
  type OrchestrationCommand,
  type OrchestrationEvent,
  type OrchestrationSession,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it, vi } from "@effect/vitest";

import {
  ProviderService,
  type ProviderServiceShape,
} from "../../provider/Services/ProviderService.ts";
import { ProviderSessionNotFoundError } from "../../provider/Errors.ts";
import { makeProviderRegistryLayer } from "../../provider/testUtils/providerRegistryMock.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { PersistenceSqlError } from "../../persistence/Errors.ts";
import * as TerminalManager from "../../terminal/Manager.ts";
import { TextGeneration } from "../../textGeneration/TextGeneration.ts";
import * as GitWorkflowService from "../../git/GitWorkflowService.ts";
import { VcsStatusBroadcaster } from "../../vcs/VcsStatusBroadcaster.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { ProviderCommandReactor } from "../Services/ProviderCommandReactor.ts";
import { ThreadDeletionReactor } from "../Services/ThreadDeletionReactor.ts";
import { ProviderCommandReactorLive } from "./ProviderCommandReactor.ts";
import {
  logCleanupCauseUnlessInterrupted,
  ThreadDeletionReactorLive,
} from "./ThreadDeletionReactor.ts";

const now = "2026-01-01T00:00:00.000Z";

const unsupported = () => Effect.die(new Error("Unsupported call in test")) as never;

const waitFor = (predicate: () => boolean) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 10_000; attempt += 1) {
      if (predicate()) return;
      yield* Effect.yieldNow;
    }
    throw new Error("Timed out waiting for expectation.");
  });

const sessionFor = (threadId: ThreadId): OrchestrationSession => ({
  threadId,
  status: "ready",
  providerName: "codex",
  providerInstanceId: ProviderInstanceId.make("codex"),
  runtimeMode: "approval-required",
  activeTurnId: null,
  lastError: null,
  updatedAt: now,
});

const providerSessionFor = (threadId: ThreadId): ProviderSession => ({
  provider: ProviderDriverKind.make("codex"),
  providerInstanceId: ProviderInstanceId.make("codex"),
  status: "ready",
  runtimeMode: "approval-required",
  threadId,
  resumeCursor: { opaque: `resume-${threadId}` },
  createdAt: now,
  updatedAt: now,
});

const archivedEvent = (
  threadId: ThreadId,
  sequence: number,
): Extract<OrchestrationEvent, { type: "thread.archived" }> => ({
  sequence,
  eventId: EventId.make(`event-archive-${threadId}`),
  aggregateKind: "thread",
  aggregateId: threadId,
  type: "thread.archived",
  occurredAt: now,
  commandId: CommandId.make("cmd-archive-parent"),
  causationEventId: null,
  correlationId: CommandId.make("cmd-archive-parent"),
  metadata: {},
  payload: {
    threadId,
    archivedAt: now,
    updatedAt: now,
  },
});

const deletedEvent = (
  threadId: ThreadId,
  sequence: number,
): Extract<OrchestrationEvent, { type: "thread.deleted" }> => ({
  sequence,
  eventId: EventId.make(`event-delete-${threadId}`),
  aggregateKind: "thread",
  aggregateId: threadId,
  type: "thread.deleted",
  occurredAt: now,
  commandId: CommandId.make("cmd-delete-thread"),
  causationEventId: null,
  correlationId: CommandId.make("cmd-delete-thread"),
  metadata: {},
  payload: {
    threadId,
    deletedAt: now,
  },
});

const sessionStopRequestedEvent = (
  command: Extract<OrchestrationCommand, { type: "thread.session.stop" }>,
  sequence: number,
): Extract<OrchestrationEvent, { type: "thread.session-stop-requested" }> => ({
  sequence,
  eventId: EventId.make(`event-${command.commandId}`),
  aggregateKind: "thread",
  aggregateId: command.threadId,
  type: "thread.session-stop-requested",
  occurredAt: command.createdAt,
  commandId: command.commandId,
  causationEventId: null,
  correlationId: command.commandId,
  metadata: {},
  payload: {
    threadId: command.threadId,
    createdAt: command.createdAt,
  },
});

describe("logCleanupCauseUnlessInterrupted", () => {
  const threadId = ThreadId.make("thread-deletion-reactor-test");

  it.effect("swallows ordinary cleanup failures", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        logCleanupCauseUnlessInterrupted({
          effect: Effect.fail("cleanup failed"),
          message: "thread deletion cleanup skipped provider session stop",
          threadId,
        }),
      );

      expect(Exit.isSuccess(exit)).toBe(true);
    }),
  );

  it.effect("preserves interrupt causes", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        logCleanupCauseUnlessInterrupted({
          effect: Effect.interrupt,
          message: "thread deletion cleanup skipped provider session stop",
          threadId,
        }),
      );

      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isFailure(exit)) {
        expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true);
      }
    }),
  );
});

describe("ThreadDeletionReactor", () => {
  it.effect("cleans up provider sessions and terminals for every archived descendant", () =>
    Effect.gen(function* () {
      const domainEvents = yield* PubSub.unbounded<OrchestrationEvent>();
      const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();
      const threadIds = [
        ThreadId.make("parent"),
        ThreadId.make("child"),
        ThreadId.make("grandchild"),
      ] as const;
      const sessionsByThread = new Map<ThreadId, OrchestrationSession>(
        threadIds.map((threadId) => [threadId, sessionFor(threadId)]),
      );
      const runtimeSessions = threadIds.map(providerSessionFor);
      const dispatchedCommands: OrchestrationCommand[] = [];
      const closeTerminal = vi.fn<TerminalManager.TerminalManager["Service"]["close"]>(
        () => Effect.void,
      );
      const stopSession = vi.fn<ProviderServiceShape["stopSession"]>((input) =>
        Effect.sync(() => {
          const index = runtimeSessions.findIndex((session) => session.threadId === input.threadId);
          if (index >= 0) {
            runtimeSessions.splice(index, 1);
          }
        }),
      );
      let nextSequence = 10;

      const engine = {
        readEvents: () => Stream.empty,
        streamDomainEvents: Stream.fromPubSub(domainEvents),
        dispatch: (command: OrchestrationCommand) =>
          Effect.gen(function* () {
            dispatchedCommands.push(command);
            nextSequence += 1;
            if (command.type === "thread.session.stop") {
              yield* PubSub.publish(domainEvents, sessionStopRequestedEvent(command, nextSequence));
            }
            if (command.type === "thread.session.set") {
              sessionsByThread.set(command.threadId, command.session);
            }
            return { sequence: nextSequence };
          }),
      } satisfies OrchestrationEngineService["Service"];

      const providerService: ProviderServiceShape = {
        startSession: () => unsupported(),
        sendTurn: () => unsupported(),
        interruptTurn: () => unsupported(),
        respondToRequest: () => unsupported(),
        respondToUserInput: () => unsupported(),
        stopSession,
        listSessions: () => Effect.succeed(runtimeSessions),
        getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
        getInstanceInfo: (instanceId) =>
          Effect.succeed({
            instanceId,
            driverKind: ProviderDriverKind.make("codex"),
            displayName: undefined,
            enabled: true,
            continuationIdentity: {
              driverKind: ProviderDriverKind.make("codex"),
              continuationKey: "codex:home:/shared-codex",
            },
          }),
        rollbackConversation: () => unsupported(),
        streamEvents: Stream.fromPubSub(runtimeEventPubSub),
      };

      const terminalManager: TerminalManager.TerminalManager["Service"] = {
        open: () => unsupported(),
        attachStream: () => unsupported(),
        write: () => unsupported(),
        resize: () => unsupported(),
        clear: () => unsupported(),
        restart: () => unsupported(),
        close: closeTerminal,
        subscribe: () => Effect.succeed(() => undefined),
        subscribeMetadata: () => Effect.succeed(() => undefined),
      };

      const layer = Layer.mergeAll(ProviderCommandReactorLive, ThreadDeletionReactorLive).pipe(
        Layer.provideMerge(Layer.succeed(OrchestrationEngineService, engine)),
        Layer.provideMerge(
          Layer.mock(ProjectionSnapshotQuery)({
            getThreadShellByIdIncludingArchived: (threadId) =>
              Effect.succeed(
                Option.some({
                  id: threadId,
                  archivedAt: now,
                  session: sessionsByThread.get(threadId) ?? null,
                } as never),
              ),
            getThreadDetailById: (threadId) =>
              Effect.succeed(
                Option.some({
                  id: threadId,
                  session: sessionsByThread.get(threadId) ?? null,
                } as never),
              ),
          }),
        ),
        Layer.provideMerge(Layer.succeed(ProviderService, providerService)),
        Layer.provideMerge(Layer.succeed(TerminalManager.TerminalManager, terminalManager)),
        Layer.provideMerge(
          makeProviderRegistryLayer([{ instanceId: ProviderInstanceId.make("codex") }] as never),
        ),
        Layer.provideMerge(
          Layer.mock(GitWorkflowService.GitWorkflowService)({
            renameBranch: () => unsupported(),
          }),
        ),
        Layer.provideMerge(
          Layer.succeed(VcsStatusBroadcaster, {
            getStatus: () => unsupported(),
            refreshLocalStatus: () => unsupported(),
            refreshStatus: () => unsupported(),
            streamStatus: () => Stream.die("streamStatus should not be called in this test"),
          }),
        ),
        Layer.provideMerge(
          Layer.mock(TextGeneration, {
            generateBranchName: () => unsupported(),
            generateThreadTitle: () => unsupported(),
          }),
        ),
        Layer.provideMerge(ServerSettingsService.layerTest()),
        Layer.provideMerge(NodeServices.layer),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const providerCommandReactor = yield* ProviderCommandReactor;
          const threadDeletionReactor = yield* ThreadDeletionReactor;
          yield* providerCommandReactor.start();
          yield* threadDeletionReactor.start();
          yield* Effect.yieldNow;
          yield* Effect.yieldNow;

          yield* Effect.forEach(
            threadIds,
            (threadId, index) => PubSub.publish(domainEvents, archivedEvent(threadId, index + 1)),
            { discard: true },
          );

          yield* waitFor(
            () => stopSession.mock.calls.length === 3 && closeTerminal.mock.calls.length === 3,
          );
          yield* threadDeletionReactor.drain;
          yield* providerCommandReactor.drain;
        }).pipe(Effect.provide(layer)),
      );

      const commandTypes = dispatchedCommands.map((command) => command.type);
      expect(commandTypes.filter((type) => type === "thread.session.stop")).toHaveLength(3);
      expect(commandTypes.filter((type) => type === "thread.session.set")).toHaveLength(3);
      expect(stopSession.mock.calls.map(([input]) => input.threadId).sort()).toEqual([
        ThreadId.make("child"),
        ThreadId.make("grandchild"),
        ThreadId.make("parent"),
      ]);
      expect(
        closeTerminal.mock.calls
          .map(([input]) => input)
          .sort((a, b) => String(a.threadId).localeCompare(String(b.threadId))),
      ).toEqual([
        { threadId: ThreadId.make("child") },
        { threadId: ThreadId.make("grandchild") },
        { threadId: ThreadId.make("parent") },
      ]);
      expect(runtimeSessions).toHaveLength(0);
    }),
  );

  it.effect("replays persisted archive events on startup", () =>
    Effect.gen(function* () {
      const domainEvents = yield* PubSub.unbounded<OrchestrationEvent>();
      const threadId = ThreadId.make("startup-archived-child");
      const archiveEvent = archivedEvent(threadId, 1);
      const runtimeSessions = [providerSessionFor(threadId)];
      const dispatchedCommands: OrchestrationCommand[] = [];
      const closeTerminal = vi.fn<TerminalManager.TerminalManager["Service"]["close"]>(
        () => Effect.void,
      );
      const stopSession = vi.fn<ProviderServiceShape["stopSession"]>((input) =>
        Effect.sync(() => {
          const index = runtimeSessions.findIndex((session) => session.threadId === input.threadId);
          if (index >= 0) {
            runtimeSessions.splice(index, 1);
          }
        }),
      );

      const engine = {
        readEvents: () => Stream.fromIterable([archiveEvent]),
        streamDomainEvents: Stream.fromPubSub(domainEvents),
        dispatch: (command: OrchestrationCommand) =>
          Effect.sync(() => {
            dispatchedCommands.push(command);
            return { sequence: 2 };
          }),
      } satisfies OrchestrationEngineService["Service"];

      const providerService: ProviderServiceShape = {
        startSession: () => unsupported(),
        sendTurn: () => unsupported(),
        interruptTurn: () => unsupported(),
        respondToRequest: () => unsupported(),
        respondToUserInput: () => unsupported(),
        stopSession,
        listSessions: () => Effect.succeed(runtimeSessions),
        getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
        getInstanceInfo: () => unsupported(),
        rollbackConversation: () => unsupported(),
        streamEvents: Stream.empty,
      };

      const terminalManager: TerminalManager.TerminalManager["Service"] = {
        open: () => unsupported(),
        attachStream: () => unsupported(),
        write: () => unsupported(),
        resize: () => unsupported(),
        clear: () => unsupported(),
        restart: () => unsupported(),
        close: closeTerminal,
        subscribe: () => Effect.succeed(() => undefined),
        subscribeMetadata: () => Effect.succeed(() => undefined),
      };

      const layer = ThreadDeletionReactorLive.pipe(
        Layer.provideMerge(Layer.succeed(OrchestrationEngineService, engine)),
        Layer.provideMerge(
          Layer.mock(ProjectionSnapshotQuery)({
            getThreadShellByIdIncludingArchived: () =>
              Effect.succeed(
                Option.some({
                  id: threadId,
                  archivedAt: now,
                  session: sessionFor(threadId),
                } as never),
              ),
          }),
        ),
        Layer.provideMerge(Layer.succeed(ProviderService, providerService)),
        Layer.provideMerge(Layer.succeed(TerminalManager.TerminalManager, terminalManager)),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const threadDeletionReactor = yield* ThreadDeletionReactor;
          yield* threadDeletionReactor.start();
          yield* waitFor(
            () =>
              stopSession.mock.calls.length === 1 &&
              dispatchedCommands.length === 1 &&
              closeTerminal.mock.calls.length === 1,
          );
          yield* threadDeletionReactor.drain;
        }).pipe(Effect.provide(layer)),
      );

      expect(dispatchedCommands).toMatchObject([
        {
          type: "thread.session.set",
          threadId,
          session: {
            status: "stopped",
            providerName: "codex",
            providerInstanceId: ProviderInstanceId.make("codex"),
            runtimeMode: "approval-required",
          },
        },
      ]);
      expect(String(dispatchedCommands[0]?.commandId)).toContain(
        `session-set-for-archive-replay:${archiveEvent.eventId}:`,
      );
      expect(dispatchedCommands[0]?.commandId).not.toBe(
        CommandId.make(`session-stop-for-archive:${archiveEvent.eventId}`),
      );
      expect(stopSession).toHaveBeenCalledWith({ threadId });
      expect(closeTerminal).toHaveBeenCalledWith({ threadId });
      expect(runtimeSessions).toHaveLength(0);
    }),
  );

  it.effect("records replayed stopped sessions when provider stop cannot route", () =>
    Effect.gen(function* () {
      const domainEvents = yield* PubSub.unbounded<OrchestrationEvent>();
      const threadId = ThreadId.make("startup-archived-stop-fails");
      const archiveEvent = archivedEvent(threadId, 1);
      const dispatchedCommands: OrchestrationCommand[] = [];
      const closeTerminal = vi.fn<TerminalManager.TerminalManager["Service"]["close"]>(
        () => Effect.void,
      );
      const stopSession = vi.fn<ProviderServiceShape["stopSession"]>(() =>
        Effect.fail(new ProviderSessionNotFoundError({ threadId })),
      );

      const engine = {
        readEvents: () => Stream.fromIterable([archiveEvent]),
        streamDomainEvents: Stream.fromPubSub(domainEvents),
        dispatch: (command: OrchestrationCommand) =>
          Effect.sync(() => {
            dispatchedCommands.push(command);
            return { sequence: 2 };
          }),
      } satisfies OrchestrationEngineService["Service"];

      const providerService: ProviderServiceShape = {
        startSession: () => unsupported(),
        sendTurn: () => unsupported(),
        interruptTurn: () => unsupported(),
        respondToRequest: () => unsupported(),
        respondToUserInput: () => unsupported(),
        stopSession,
        listSessions: () => Effect.succeed([]),
        getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
        getInstanceInfo: () => unsupported(),
        rollbackConversation: () => unsupported(),
        streamEvents: Stream.empty,
      };

      const terminalManager: TerminalManager.TerminalManager["Service"] = {
        open: () => unsupported(),
        attachStream: () => unsupported(),
        write: () => unsupported(),
        resize: () => unsupported(),
        clear: () => unsupported(),
        restart: () => unsupported(),
        close: closeTerminal,
        subscribe: () => Effect.succeed(() => undefined),
        subscribeMetadata: () => Effect.succeed(() => undefined),
      };

      const layer = ThreadDeletionReactorLive.pipe(
        Layer.provideMerge(Layer.succeed(OrchestrationEngineService, engine)),
        Layer.provideMerge(
          Layer.mock(ProjectionSnapshotQuery)({
            getThreadShellByIdIncludingArchived: () =>
              Effect.succeed(
                Option.some({
                  id: threadId,
                  archivedAt: now,
                  session: sessionFor(threadId),
                } as never),
              ),
          }),
        ),
        Layer.provideMerge(Layer.succeed(ProviderService, providerService)),
        Layer.provideMerge(Layer.succeed(TerminalManager.TerminalManager, terminalManager)),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const threadDeletionReactor = yield* ThreadDeletionReactor;
          yield* threadDeletionReactor.start();
          yield* waitFor(
            () =>
              stopSession.mock.calls.length === 1 &&
              dispatchedCommands.length === 1 &&
              closeTerminal.mock.calls.length === 1,
          );
          yield* threadDeletionReactor.drain;
        }).pipe(Effect.provide(layer)),
      );

      expect(dispatchedCommands).toMatchObject([
        {
          type: "thread.session.set",
          threadId,
          session: {
            status: "stopped",
            providerName: "codex",
            providerInstanceId: ProviderInstanceId.make("codex"),
            runtimeMode: "approval-required",
          },
        },
      ]);
      expect(stopSession).toHaveBeenCalledWith({ threadId });
      expect(closeTerminal).toHaveBeenCalledWith({ threadId });
    }),
  );

  it.effect(
    "skips archive cleanup when retry observes the archive event is stale after unarchive",
    () =>
      Effect.gen(function* () {
        const domainEvents = yield* PubSub.unbounded<OrchestrationEvent>();
        const threadId = ThreadId.make("stale-after-unarchive");
        const runtimeSessions = [providerSessionFor(threadId)];
        const dispatchedCommands: OrchestrationCommand[] = [];
        const closeTerminal = vi.fn<TerminalManager.TerminalManager["Service"]["close"]>(
          () => Effect.void,
        );
        const stopSession = vi.fn<ProviderServiceShape["stopSession"]>(() => Effect.void);
        let lookupAttempts = 0;
        const getThreadShellByIdIncludingArchived = vi.fn<
          ProjectionSnapshotQuery["Service"]["getThreadShellByIdIncludingArchived"]
        >(() => {
          lookupAttempts += 1;
          if (lookupAttempts === 1) {
            return Effect.fail(
              new PersistenceSqlError({
                operation: "test.getThreadShellByIdIncludingArchived",
                detail: "projection lookup failed before unarchive",
              }),
            );
          }
          return Effect.succeed(
            Option.some({
              id: threadId,
              archivedAt: null,
              session: sessionFor(threadId),
            } as never),
          );
        });

        const engine = {
          readEvents: () => Stream.empty,
          streamDomainEvents: Stream.fromPubSub(domainEvents),
          dispatch: (command: OrchestrationCommand) =>
            Effect.sync(() => {
              dispatchedCommands.push(command);
              return { sequence: 2 };
            }),
        } satisfies OrchestrationEngineService["Service"];

        const providerService: ProviderServiceShape = {
          startSession: () => unsupported(),
          sendTurn: () => unsupported(),
          interruptTurn: () => unsupported(),
          respondToRequest: () => unsupported(),
          respondToUserInput: () => unsupported(),
          stopSession,
          listSessions: () => Effect.succeed(runtimeSessions),
          getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
          getInstanceInfo: () => unsupported(),
          rollbackConversation: () => unsupported(),
          streamEvents: Stream.empty,
        };

        const terminalManager: TerminalManager.TerminalManager["Service"] = {
          open: () => unsupported(),
          attachStream: () => unsupported(),
          write: () => unsupported(),
          resize: () => unsupported(),
          clear: () => unsupported(),
          restart: () => unsupported(),
          close: closeTerminal,
          subscribe: () => Effect.succeed(() => undefined),
          subscribeMetadata: () => Effect.succeed(() => undefined),
        };

        const layer = ThreadDeletionReactorLive.pipe(
          Layer.provideMerge(Layer.succeed(OrchestrationEngineService, engine)),
          Layer.provideMerge(
            Layer.mock(ProjectionSnapshotQuery)({
              getThreadShellByIdIncludingArchived,
            }),
          ),
          Layer.provideMerge(Layer.succeed(ProviderService, providerService)),
          Layer.provideMerge(Layer.succeed(TerminalManager.TerminalManager, terminalManager)),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const threadDeletionReactor = yield* ThreadDeletionReactor;
            yield* threadDeletionReactor.start();
            yield* Effect.yieldNow;
            yield* PubSub.publish(domainEvents, archivedEvent(threadId, 1));
            yield* waitFor(() => getThreadShellByIdIncludingArchived.mock.calls.length >= 2);
            yield* threadDeletionReactor.drain;
          }).pipe(Effect.provide(layer)),
        );

        expect(dispatchedCommands).toHaveLength(0);
        expect(stopSession).not.toHaveBeenCalled();
        expect(closeTerminal).not.toHaveBeenCalled();
        expect(runtimeSessions).toHaveLength(1);
      }),
  );

  it.effect("retries archive cleanup after a transient projection lookup failure", () =>
    Effect.gen(function* () {
      const domainEvents = yield* PubSub.unbounded<OrchestrationEvent>();
      const threadId = ThreadId.make("archive-lookup-failure");
      const runtimeSessions = [providerSessionFor(threadId)];
      const dispatchedCommands: OrchestrationCommand[] = [];
      const closeTerminal = vi.fn<TerminalManager.TerminalManager["Service"]["close"]>(
        () => Effect.void,
      );
      let lookupAttempts = 0;
      const getThreadShellByIdIncludingArchived = vi.fn<
        ProjectionSnapshotQuery["Service"]["getThreadShellByIdIncludingArchived"]
      >(() => {
        lookupAttempts += 1;
        if (lookupAttempts === 1) {
          return Effect.fail(
            new PersistenceSqlError({
              operation: "test.getThreadShellByIdIncludingArchived",
              detail: "projection lookup failed",
            }),
          );
        }
        return Effect.succeed(
          Option.some({
            id: threadId,
            archivedAt: now,
            session: sessionFor(threadId),
          } as never),
        );
      });

      const engine = {
        readEvents: () => Stream.empty,
        streamDomainEvents: Stream.fromPubSub(domainEvents),
        dispatch: (command: OrchestrationCommand) =>
          Effect.sync(() => {
            dispatchedCommands.push(command);
            return { sequence: 2 };
          }),
      } satisfies OrchestrationEngineService["Service"];

      const providerService: ProviderServiceShape = {
        startSession: () => unsupported(),
        sendTurn: () => unsupported(),
        interruptTurn: () => unsupported(),
        respondToRequest: () => unsupported(),
        respondToUserInput: () => unsupported(),
        stopSession: () => unsupported(),
        listSessions: () => Effect.succeed(runtimeSessions),
        getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
        getInstanceInfo: () => unsupported(),
        rollbackConversation: () => unsupported(),
        streamEvents: Stream.empty,
      };

      const terminalManager: TerminalManager.TerminalManager["Service"] = {
        open: () => unsupported(),
        attachStream: () => unsupported(),
        write: () => unsupported(),
        resize: () => unsupported(),
        clear: () => unsupported(),
        restart: () => unsupported(),
        close: closeTerminal,
        subscribe: () => Effect.succeed(() => undefined),
        subscribeMetadata: () => Effect.succeed(() => undefined),
      };

      const layer = ThreadDeletionReactorLive.pipe(
        Layer.provideMerge(Layer.succeed(OrchestrationEngineService, engine)),
        Layer.provideMerge(
          Layer.mock(ProjectionSnapshotQuery)({
            getThreadShellByIdIncludingArchived,
          }),
        ),
        Layer.provideMerge(Layer.succeed(ProviderService, providerService)),
        Layer.provideMerge(Layer.succeed(TerminalManager.TerminalManager, terminalManager)),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const threadDeletionReactor = yield* ThreadDeletionReactor;
          yield* threadDeletionReactor.start();
          yield* Effect.yieldNow;
          yield* PubSub.publish(domainEvents, archivedEvent(threadId, 1));
          yield* waitFor(
            () =>
              getThreadShellByIdIncludingArchived.mock.calls.length >= 2 &&
              dispatchedCommands.length === 1 &&
              closeTerminal.mock.calls.length === 1,
          );
          yield* threadDeletionReactor.drain;
        }).pipe(Effect.provide(layer)),
      );

      expect(dispatchedCommands).toMatchObject([
        {
          type: "thread.session.stop",
          threadId,
        },
      ]);
      expect(closeTerminal).toHaveBeenCalledWith({ threadId });
    }),
  );

  it.effect(
    "does not block later deletion cleanup while an archive snapshot retry is pending",
    () =>
      Effect.gen(function* () {
        const domainEvents = yield* PubSub.unbounded<OrchestrationEvent>();
        const archivedThreadId = ThreadId.make("archive-lookup-persistent-failure");
        const deletedThreadId = ThreadId.make("delete-after-archive-lookup-failure");
        const stopSession = vi.fn<ProviderServiceShape["stopSession"]>(() => Effect.void);
        const closeTerminal = vi.fn<TerminalManager.TerminalManager["Service"]["close"]>(
          () => Effect.void,
        );
        const getThreadShellByIdIncludingArchived = vi.fn<
          ProjectionSnapshotQuery["Service"]["getThreadShellByIdIncludingArchived"]
        >(() =>
          Effect.fail(
            new PersistenceSqlError({
              operation: "test.getThreadShellByIdIncludingArchived",
              detail: "persistent projection lookup failure",
            }),
          ),
        );

        const engine = {
          readEvents: () => Stream.empty,
          streamDomainEvents: Stream.fromPubSub(domainEvents),
          dispatch: () => unsupported(),
        } satisfies OrchestrationEngineService["Service"];

        const providerService: ProviderServiceShape = {
          startSession: () => unsupported(),
          sendTurn: () => unsupported(),
          interruptTurn: () => unsupported(),
          respondToRequest: () => unsupported(),
          respondToUserInput: () => unsupported(),
          stopSession,
          listSessions: () => Effect.succeed([providerSessionFor(archivedThreadId)]),
          getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
          getInstanceInfo: () => unsupported(),
          rollbackConversation: () => unsupported(),
          streamEvents: Stream.empty,
        };

        const terminalManager: TerminalManager.TerminalManager["Service"] = {
          open: () => unsupported(),
          attachStream: () => unsupported(),
          write: () => unsupported(),
          resize: () => unsupported(),
          clear: () => unsupported(),
          restart: () => unsupported(),
          close: closeTerminal,
          subscribe: () => Effect.succeed(() => undefined),
          subscribeMetadata: () => Effect.succeed(() => undefined),
        };

        const layer = ThreadDeletionReactorLive.pipe(
          Layer.provideMerge(Layer.succeed(OrchestrationEngineService, engine)),
          Layer.provideMerge(
            Layer.mock(ProjectionSnapshotQuery)({
              getThreadShellByIdIncludingArchived,
            }),
          ),
          Layer.provideMerge(Layer.succeed(ProviderService, providerService)),
          Layer.provideMerge(Layer.succeed(TerminalManager.TerminalManager, terminalManager)),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const threadDeletionReactor = yield* ThreadDeletionReactor;
            yield* threadDeletionReactor.start();
            yield* Effect.yieldNow;
            yield* PubSub.publish(domainEvents, archivedEvent(archivedThreadId, 1));
            yield* PubSub.publish(domainEvents, deletedEvent(deletedThreadId, 2));
            yield* waitFor(
              () => stopSession.mock.calls.length === 1 && closeTerminal.mock.calls.length === 1,
            );
            yield* threadDeletionReactor.drain;
          }).pipe(Effect.provide(layer)),
        );

        expect(getThreadShellByIdIncludingArchived).toHaveBeenCalled();
        expect(stopSession).toHaveBeenCalledWith({ threadId: deletedThreadId });
        expect(closeTerminal).toHaveBeenCalledWith({
          threadId: deletedThreadId,
          deleteHistory: true,
        });
      }),
  );
});
