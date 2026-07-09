// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeFSP from "node:fs/promises";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { createModelSelection } from "@t3tools/shared/model";

import {
  ApprovalRequestId,
  CursorSettings,
  EnvironmentId,
  ProviderDriverKind,
  type ProviderRuntimeEvent,
  ThreadId,
  TurnId,
  ProviderInstanceId,
} from "@t3tools/contracts";

import { ServerConfig } from "../../config.ts";
import { makeAcpMcpServers } from "../../mcp/McpProviderInjection.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import type { CursorAdapterShape } from "../Services/CursorAdapter.ts";
import { makeCursorAdapter } from "./CursorAdapter.ts";
const decodeCursorSettings = Schema.decodeSync(CursorSettings);

// Test-local service tag so the rest of the file can keep using `yield* CursorAdapter`.
class CursorAdapter extends Context.Service<CursorAdapter, CursorAdapterShape>()(
  "t3/provider/Layers/CursorAdapter.test/CursorAdapter",
) {}

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockAgentPath = NodePath.join(__dirname, "../../../scripts/acp-mock-agent.ts");
const mockAgentCommand = "node";
const mockAgentArgs = [mockAgentPath] as const;

async function makeMockAgentWrapper(
  extraEnv?: Record<string, string>,
  options?: { initialDelaySeconds?: number },
) {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "cursor-acp-mock-"));
  const wrapperPath = NodePath.join(dir, "fake-agent.sh");
  const envExports = Object.entries(extraEnv ?? {})
    .map(([key, value]) => `export ${key}=${JSON.stringify(value)}`)
    .join("\n");
  const script = `#!/bin/sh
${envExports}
${options?.initialDelaySeconds ? `sleep ${JSON.stringify(String(options.initialDelaySeconds))}` : ""}
exec ${JSON.stringify(mockAgentCommand)} ${mockAgentArgs.map((arg) => JSON.stringify(arg)).join(" ")} "$@"
`;
  await NodeFSP.writeFile(wrapperPath, script, "utf8");
  await NodeFSP.chmod(wrapperPath, 0o755);
  return wrapperPath;
}

async function makeProbeWrapper(
  requestLogPath: string,
  argvLogPath: string,
  extraEnv?: Record<string, string>,
) {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "cursor-acp-probe-"));
  const wrapperPath = NodePath.join(dir, "fake-agent.sh");
  const envExports = Object.entries(extraEnv ?? {})
    .map(([key, value]) => `export ${key}=${JSON.stringify(value)}`)
    .join("\n");
  const script = `#!/bin/sh
printf '%s\t' "$@" >> ${JSON.stringify(argvLogPath)}
printf '\n' >> ${JSON.stringify(argvLogPath)}
export T3_ACP_REQUEST_LOG_PATH=${JSON.stringify(requestLogPath)}
${envExports}
exec ${JSON.stringify(mockAgentCommand)} ${mockAgentArgs.map((arg) => JSON.stringify(arg)).join(" ")} "$@"
`;
  await NodeFSP.writeFile(wrapperPath, script, "utf8");
  await NodeFSP.chmod(wrapperPath, 0o755);
  return wrapperPath;
}

async function readArgvLog(filePath: string) {
  const raw = await NodeFSP.readFile(filePath, "utf8");
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => line.split("\t").filter((token) => token.length > 0));
}

async function readJsonLines(filePath: string) {
  const raw = await NodeFSP.readFile(filePath, "utf8");
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

async function waitForFileContent(filePath: string, attempts = 40) {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      const raw = await NodeFSP.readFile(filePath, "utf8");
      if (raw.trim().length > 0) {
        return raw;
      }
    } catch {}
    await Effect.runPromise(Effect.yieldNow);
  }
  throw new Error(`Timed out waiting for file content at ${filePath}`);
}

function waitForJsonLogMatch(
  filePath: string,
  predicate: (entry: Record<string, unknown>) => boolean,
  attempts = 40,
) {
  return Effect.gen(function* () {
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const requests = yield* Effect.promise(() => readJsonLines(filePath));
      if (requests.some(predicate)) {
        return requests;
      }
      yield* Effect.yieldNow;
    }
    return yield* Effect.promise(() => readJsonLines(filePath));
  });
}

// Tests mutate `ServerSettingsService` mid-flight (e.g. setting
// `providers.cursor.binaryPath` to a mock ACP wrapper). The adapter
// captures `cursorSettings` once at construction, so without a resolver
// the mutation is invisible — sessions would spawn the constructor's
// (empty) binary path. Wiring `resolveSettings` through
// `ServerSettingsService.getSettings` makes each session read the latest
// snapshot, matching the old "always read live" behavior that these
// tests assumed.
const makeResolveCursorSettings = Effect.gen(function* () {
  const serverSettings = yield* ServerSettingsService;
  return yield* Effect.succeed(
    serverSettings.getSettings.pipe(
      Effect.map((snapshot) => snapshot.providers.cursor),
      Effect.orDie,
    ),
  );
});

const cursorAdapterTestLayer = it.layer(
  Layer.effect(
    CursorAdapter,
    Effect.gen(function* () {
      const cursorConfig = decodeCursorSettings({});
      const resolveSettings = yield* makeResolveCursorSettings;
      return yield* makeCursorAdapter(cursorConfig, { resolveSettings });
    }),
  ).pipe(
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(
      ServerConfig.layerTest(process.cwd(), {
        prefix: "t3code-cursor-adapter-test-",
      }),
    ),
    Layer.provideMerge(NodeServices.layer),
  ),
);

cursorAdapterTestLayer("CursorAdapterLive", (it) => {
  it.effect("starts a session and maps mock ACP prompt flow to runtime events", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-mock-thread");

      const wrapperPath = yield* Effect.promise(() => makeMockAgentWrapper());
      yield* settings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* Stream.take(adapter.streamEvents, 9).pipe(
        Stream.runCollect,
        Effect.forkChild,
      );

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      assert.equal(session.provider, "cursor");
      assert.deepStrictEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "mock-session-1",
      });

      yield* adapter.sendTurn({
        threadId,
        input: "hello mock",
        attachments: [],
      });

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      const types = runtimeEvents.map((e) => e.type);

      for (const t of [
        "session.started",
        "session.state.changed",
        "thread.started",
        "turn.started",
        "turn.plan.updated",
        "item.started",
        "content.delta",
        "item.completed",
        "turn.completed",
      ] as const) {
        assert.include(types, t);
      }

      const assistantStarted = runtimeEvents.find(
        (event) => event.type === "item.started" && event.payload.itemType === "assistant_message",
      );
      assert.isDefined(assistantStarted);

      const delta = runtimeEvents.find((e) => e.type === "content.delta");
      assert.isDefined(delta);
      if (delta?.type === "content.delta") {
        assert.equal(delta.payload.delta, "hello from mock");
        assert.match(String(delta.itemId), /^assistant:mock-session-1:segment:0$/);
      }

      const assistantCompleted = runtimeEvents.find(
        (event) =>
          event.type === "item.completed" && event.payload.itemType === "assistant_message",
      );
      assert.isDefined(assistantCompleted);

      const planUpdate = runtimeEvents.find((event) => event.type === "turn.plan.updated");
      assert.isDefined(planUpdate);
      if (planUpdate?.type === "turn.plan.updated") {
        assert.deepStrictEqual(planUpdate.payload.plan, [
          { step: "Inspect mock ACP state", status: "completed" },
          { step: "Implement the requested change", status: "inProgress" },
        ]);
      }

      const completedSessions = yield* adapter.listSessions();
      const completedSession = completedSessions.find((entry) => entry.threadId === threadId);
      assert.equal(completedSession?.status, "ready");
      assert.equal(completedSession?.activeTurnId, undefined);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("steers a running turn instead of opening a new one on mid-turn sendTurn", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-steer-thread");

      // Keep the first prompt in flight long enough for the steer to land.
      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({ T3_ACP_PROMPT_DELAY_MS: "1500" }),
      );
      yield* settings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const firstTurnFiber = yield* adapter
        .sendTurn({
          threadId,
          input: "run 5 commands",
          attachments: [],
        })
        .pipe(Effect.forkChild);

      // Poll until the first prompt is in flight — sendTurn binds the active
      // turn id before prompting. The mock agent runs on the real clock, so
      // each TestClock.adjust just provides the scheduler hops for its stdio
      // responses to land.
      yield* Effect.gen(function* () {
        for (let attempt = 0; attempt < 200; attempt += 1) {
          const sessions = yield* adapter.listSessions();
          const session = sessions.find((entry) => entry.threadId === threadId);
          if (session?.activeTurnId !== undefined) {
            return;
          }
          yield* TestClock.adjust("10 millis");
        }
        throw new Error("Timed out waiting for the first prompt to be in flight.");
      });

      // Steer: a second sendTurn while the first prompt is still in flight
      // continues the same turn.
      const steeredTurn = yield* adapter.sendTurn({
        threadId,
        input: "actually run 15",
        attachments: [],
      });
      const firstTurn = yield* Fiber.join(firstTurnFiber);
      assert.equal(String(steeredTurn.turnId), String(firstTurn.turnId));

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      const turnStartedEvents = runtimeEvents.filter((event) => event.type === "turn.started");
      const turnCompletedEvents = runtimeEvents.filter((event) => event.type === "turn.completed");

      // One turn boundary for the whole run: the superseded first prompt
      // resolving must not settle the merged turn.
      assert.equal(turnStartedEvents.length, 1);
      assert.equal(String(turnStartedEvents[0]?.turnId), String(firstTurn.turnId));
      assert.equal(turnCompletedEvents.length, 1);
      assert.equal(String(turnCompletedEvents[0]?.turnId), String(firstTurn.turnId));

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("keeps a steered turn active when an earlier prompt fails", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-steer-first-fails-thread");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({
          T3_ACP_FAIL_FIRST_PROMPT: "1",
          T3_ACP_FIRST_PROMPT_DELAY_MS: "100",
          T3_ACP_SECOND_PROMPT_DELAY_MS: "1000",
        }),
      );
      yield* settings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const firstTurnFiber = yield* adapter
        .sendTurn({
          threadId,
          input: "start a turn that will fail",
          attachments: [],
        })
        .pipe(Effect.exit, Effect.forkChild);

      let activeTurnId: TurnId | undefined;
      yield* Effect.gen(function* () {
        for (let attempt = 0; attempt < 200; attempt += 1) {
          const sessions = yield* adapter.listSessions();
          const session = sessions.find((entry) => entry.threadId === threadId);
          if (session?.activeTurnId !== undefined) {
            activeTurnId = session.activeTurnId;
            return;
          }
          yield* TestClock.adjust("10 millis");
        }
        throw new Error("Timed out waiting for the first prompt to be in flight.");
      });

      const steeredTurnFiber = yield* adapter
        .sendTurn({
          threadId,
          input: "keep working on the same turn",
          attachments: [],
        })
        .pipe(Effect.forkChild);

      const firstExit = yield* Fiber.join(firstTurnFiber);
      assert.equal(firstExit._tag, "Failure");

      const runningSessions = yield* adapter.listSessions();
      const runningSession = runningSessions.find((entry) => entry.threadId === threadId);
      assert.equal(runningSession?.status, "running");
      assert.equal(String(runningSession?.activeTurnId), String(activeTurnId));

      const steeredTurn = yield* Fiber.join(steeredTurnFiber);
      assert.equal(String(steeredTurn.turnId), String(activeTurnId));

      const completedSessions = yield* adapter.listSessions();
      const completedSession = completedSessions.find((entry) => entry.threadId === threadId);
      assert.equal(completedSession?.status, "ready");
      assert.equal(completedSession?.activeTurnId, undefined);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("rolls back active session state when sendTurn fails during prompt preparation", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-send-turn-rollback");

      const wrapperPath = yield* Effect.promise(() => makeMockAgentWrapper());
      yield* settings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      yield* Effect.flip(
        adapter.sendTurn({
          threadId,
          input: "send a missing image",
          attachments: [
            {
              type: "image",
              id: "missing-image",
              name: "missing.png",
              mimeType: "image/png",
              sizeBytes: 1,
            },
          ],
        }),
      );

      const sessions = yield* adapter.listSessions();
      const session = sessions.find((entry) => entry.threadId === threadId);
      assert.equal(session?.status, "ready");
      assert.equal(session?.activeTurnId, undefined);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("steers a resumed active turn before idle settlement", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-resume-steer-thread");
      const activeTurnId = TurnId.make("turn-cursor-resume-steer");

      const wrapperPath = yield* Effect.promise(() => makeMockAgentWrapper());
      yield* settings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
        resumeCursor: {
          schemaVersion: 1,
          sessionId: "mock-session-1",
        },
        activeTurnId,
      });

      const turn = yield* adapter.sendTurn({
        threadId,
        input: "continue the resumed turn",
        attachments: [],
      });
      assert.equal(String(turn.turnId), String(activeTurnId));

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      const turnStartedEvents = runtimeEvents.filter((event) => event.type === "turn.started");
      assert.equal(turnStartedEvents.length, 0);
      const completed = runtimeEvents.find((event) => event.type === "turn.completed");
      assert.isDefined(completed);
      if (completed?.type === "turn.completed") {
        assert.equal(String(completed.turnId), String(activeTurnId));
      }

      const sessions = yield* adapter.listSessions();
      const settledSession = sessions.find((entry) => entry.threadId === threadId);
      assert.equal(settledSession?.status, "ready");
      assert.equal(settledSession?.activeTurnId, undefined);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("settles a completed turn when the Cursor notification stream exits before drain", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-drain-notification-exit-thread");
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();

      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({
          T3_ACP_EXIT_AFTER_PROMPT_RETURN: "1",
        }),
      );
      yield* settings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          if (event.type === "turn.completed") {
            yield* Deferred.succeed(turnCompleted, event).pipe(Effect.ignore);
          }
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      yield* adapter
        .sendTurn({
          threadId,
          input: "complete even if Cursor notifications exit",
          attachments: [],
        })
        .pipe(Effect.timeout("2 seconds"));
      const completed = yield* Deferred.await(turnCompleted).pipe(Effect.timeout("2 seconds"));
      assert.equal(completed.payload.state, "completed");

      yield* adapter.stopSession(threadId);
      yield* Fiber.interrupt(runtimeEventsFiber);
    }),
  );

  it.effect("clears a resumed active turn when interrupted before a local prompt", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-resume-interrupt-thread");
      const activeTurnId = TurnId.make("turn-cursor-resume-interrupt");

      const wrapperPath = yield* Effect.promise(() => makeMockAgentWrapper());
      yield* settings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const completionFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.filter((event) => event.type === "turn.completed"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
        resumeCursor: {
          schemaVersion: 1,
          sessionId: "mock-session-1",
        },
        activeTurnId,
      });

      yield* adapter.interruptTurn(threadId);
      const completionEvents = Array.from(yield* Fiber.join(completionFiber));
      const completion = completionEvents[0];
      assert.equal(completion?.type, "turn.completed");
      if (completion?.type === "turn.completed") {
        assert.equal(String(completion.turnId), String(activeTurnId));
        assert.equal(completion.payload.state, "cancelled");
      }

      const interruptedSessions = yield* adapter.listSessions();
      const interruptedSession = interruptedSessions.find((entry) => entry.threadId === threadId);
      assert.equal(interruptedSession?.status, "ready");
      assert.equal(interruptedSession?.activeTurnId, undefined);

      const nextTurn = yield* adapter.sendTurn({
        threadId,
        input: "start fresh after interrupt",
        attachments: [],
      });
      assert.notEqual(String(nextTurn.turnId), String(activeTurnId));

      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("unblocks follow-up sends after interrupting a resumed pending user input", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-resume-pending-input-interrupt");
      const activeTurnId = TurnId.make("turn-cursor-resume-pending-input");

      const onceDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "cursor-load-question-once-")),
      );
      const oncePath = NodePath.join(onceDir, "emitted");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({
          T3_ACP_EMIT_LOAD_PENDING_ASK_QUESTION: "1",
          T3_ACP_LOAD_PENDING_ASK_QUESTION_ONCE_PATH: oncePath,
        }),
      );
      yield* settings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const userInputRequested =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "user-input.requested" }>>();
      const followUpTurnStarted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.started" }>>();
      let sawInitialUserInput = false;
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          if (event.type === "turn.started") {
            yield* Deferred.succeed(followUpTurnStarted, event).pipe(Effect.ignore);
            return;
          }
          if (event.type !== "user-input.requested") {
            return;
          }
          if (!sawInitialUserInput) {
            sawInitialUserInput = true;
            yield* Deferred.succeed(userInputRequested, event).pipe(Effect.ignore);
            return;
          }
          if (event.requestId === undefined) {
            return;
          }
          yield* adapter.respondToUserInput(
            threadId,
            ApprovalRequestId.make(String(event.requestId)),
            {},
          );
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
        resumeCursor: {
          schemaVersion: 1,
          sessionId: "mock-session-1",
        },
        activeTurnId,
      });

      yield* Deferred.await(userInputRequested).pipe(Effect.timeout("2 seconds"));

      yield* adapter.interruptTurn(threadId).pipe(Effect.timeout("2 seconds"));
      const followUpFiber = yield* adapter
        .sendTurn({
          threadId,
          input: "start fresh after resumed input interrupt",
          attachments: [],
        })
        .pipe(Effect.forkChild);
      const nextTurnStarted = yield* Deferred.await(followUpTurnStarted).pipe(
        Effect.timeout("6 seconds"),
      );
      assert.notEqual(String(nextTurnStarted.turnId), String(activeTurnId));

      const sessions = yield* adapter.listSessions();
      const session = sessions.find((entry) => entry.threadId === threadId);
      assert.equal(session?.status, "running");
      assert.equal(String(session?.activeTurnId), String(nextTurnStarted.turnId));

      yield* adapter.stopSession(threadId);
      yield* Fiber.interrupt(followUpFiber);
      yield* Fiber.interrupt(runtimeEventsFiber);
    }).pipe(TestClock.withLive),
  );

  it.effect("binds session/load live continuations to the resumed active turn", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-resume-live-continuation");
      const activeTurnId = TurnId.make("turn-cursor-resume-live");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({
          T3_ACP_EMIT_LOAD_REPLAY_LIVE_CONTINUATION: "1",
          T3_ACP_LOAD_REPLAY_LIVE_CONTINUATION_DELAY_MS: "0",
        }),
      );
      yield* settings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const runtimeEventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.takeUntil(
          (event) => event.type === "content.delta" && event.payload.delta === " live continuation",
        ),
        Stream.runCollect,
        Effect.forkChild,
      );

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
        resumeCursor: {
          schemaVersion: 1,
          sessionId: "mock-session-1",
        },
        activeTurnId,
      });

      assert.equal(String(session.activeTurnId), String(activeTurnId));
      assert.equal(session.status, "running");
      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      const sessionState = runtimeEvents.find((event) => event.type === "session.state.changed");
      assert.isDefined(sessionState);
      if (sessionState?.type === "session.state.changed") {
        assert.equal(sessionState.payload.state, "running");
      }
      const assistantStarted = runtimeEvents.find(
        (event) => event.type === "item.started" && event.payload.itemType === "assistant_message",
      );
      assert.isDefined(assistantStarted);
      if (assistantStarted?.type === "item.started") {
        assert.equal(String(assistantStarted.turnId), String(activeTurnId));
        assert.equal(assistantStarted.itemId, "assistant:mock-session-1:segment:0");
      }

      const delta = runtimeEvents.find((event) => event.type === "content.delta");
      assert.isDefined(delta);
      if (delta?.type === "content.delta") {
        assert.equal(String(delta.turnId), String(activeTurnId));
        assert.equal(delta.itemId, "assistant:mock-session-1:segment:0");
        assert.equal(delta.payload.delta, " live continuation");
      }

      assert.isUndefined(runtimeEvents.find((event) => event.type === "turn.completed"));

      const sessions = yield* adapter.listSessions();
      const resumedSession = sessions.find((entry) => entry.threadId === threadId);
      assert.equal(resumedSession?.status, "running");
      assert.equal(String(resumedSession?.activeTurnId), String(activeTurnId));

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("closes the ACP child process when a session stops", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-stop-session-close");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "cursor-adapter-exit-log-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({
          T3_ACP_EXIT_LOG_PATH: exitLogPath,
        }),
      );
      yield* settings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      yield* adapter.stopSession(threadId);

      const exitLog = yield* Effect.promise(() => waitForFileContent(exitLogPath));
      assert.include(exitLog, "SIGTERM");
    }),
  );

  it.effect(
    "serializes concurrent startSession calls for the same thread and closes the replaced ACP session",
    () =>
      Effect.gen(function* () {
        const adapter = yield* CursorAdapter;
        const settings = yield* ServerSettingsService;
        const threadId = ThreadId.make("cursor-concurrent-start-session");
        const tempDir = yield* Effect.promise(() =>
          NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "cursor-adapter-concurrent-exit-log-")),
        );
        const exitLogPath = NodePath.join(tempDir, "exit.log");

        const wrapperPath = yield* Effect.promise(() =>
          makeMockAgentWrapper(
            {
              T3_ACP_EXIT_LOG_PATH: exitLogPath,
            },
            { initialDelaySeconds: 0.2 },
          ),
        );
        yield* settings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

        const [firstSession, secondSession] = yield* Effect.all(
          [
            adapter.startSession({
              threadId,
              provider: ProviderDriverKind.make("cursor"),
              cwd: process.cwd(),
              runtimeMode: "full-access",
              modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
            }),
            adapter.startSession({
              threadId,
              provider: ProviderDriverKind.make("cursor"),
              cwd: process.cwd(),
              runtimeMode: "full-access",
              modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
            }),
          ],
          { concurrency: "unbounded" },
        );

        assert.equal(firstSession.threadId, threadId);
        assert.equal(secondSession.threadId, threadId);

        yield* adapter.stopSession(threadId);

        const exitLog = yield* Effect.promise(() => waitForFileContent(exitLogPath));
        assert.equal(exitLog.match(/SIGTERM/g)?.length ?? 0, 2);
      }),
  );

  it.effect("rejects startSession when provider mismatches", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const result = yield* adapter
        .startSession({
          threadId: ThreadId.make("bad-provider"),
          provider: ProviderDriverKind.make("codex"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
        })
        .pipe(Effect.result);

      assert.equal(result._tag, "Failure");
    }),
  );

  it.effect("injects the canonical MCP server into Cursor ACP sessions", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-mcp-injection-probe");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "cursor-acp-mcp-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const argvLogPath = NodePath.join(tempDir, "argv.txt");
      yield* Effect.promise(() => NodeFSP.writeFile(requestLogPath, "", "utf8"));
      const wrapperPath = yield* Effect.promise(() =>
        makeProbeWrapper(requestLogPath, argvLogPath),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const mcpSession: McpProviderSession.McpProviderSessionConfig = {
        environmentId: EnvironmentId.make("environment-cursor-mcp-injection"),
        threadId,
        providerSessionId: "provider-session-cursor-mcp-injection",
        providerInstanceId: ProviderInstanceId.make("cursor"),
        endpoint: "http://127.0.0.1:3773/mcp",
        authorizationHeader: "Bearer cursor-mcp-token",
      };
      McpProviderSession.setMcpProviderSession(mcpSession);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => McpProviderSession.clearMcpProviderSession(threadId)),
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "composer-2" },
      });
      yield* adapter.stopSession(threadId);

      const requests = yield* waitForJsonLogMatch(
        requestLogPath,
        (entry) => entry.method === "session/new",
      );
      const sessionNew = requests.find((entry) => entry.method === "session/new");
      assert.isDefined(sessionNew);
      assert.deepEqual(
        (sessionNew?.params as Record<string, unknown> | undefined)?.mcpServers,
        makeAcpMcpServers(mcpSession),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("maps app plan mode onto the ACP plan session mode", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-plan-mode-probe");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "cursor-acp-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const argvLogPath = NodePath.join(tempDir, "argv.txt");
      yield* Effect.promise(() => NodeFSP.writeFile(requestLogPath, "", "utf8"));
      const wrapperPath = yield* Effect.promise(() =>
        makeProbeWrapper(requestLogPath, argvLogPath),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "composer-2" },
      });

      yield* adapter.sendTurn({
        threadId,
        input: "plan this change",
        attachments: [],
        interactionMode: "plan",
      });
      yield* adapter.stopSession(threadId);

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const modeRequest = requests
        .toReversed()
        .find(
          (entry) =>
            entry.method === "session/set_mode" ||
            (entry.method === "session/set_config_option" &&
              (entry.params as Record<string, unknown> | undefined)?.configId === "mode"),
        );
      assert.isDefined(modeRequest);
      assert.equal(
        (modeRequest?.params as Record<string, unknown> | undefined)?.sessionId,
        "mock-session-1",
      );
      assert.include(
        ["architect", "plan"],
        String(
          (modeRequest?.params as Record<string, unknown> | undefined)?.modeId ??
            (modeRequest?.params as Record<string, unknown> | undefined)?.value,
        ),
      );
    }),
  );

  it.effect(
    "applies initial model and mode configuration during startSession and skips repeating it on first send",
    () =>
      Effect.gen(function* () {
        const adapter = yield* CursorAdapter;
        const serverSettings = yield* ServerSettingsService;
        const threadId = ThreadId.make("cursor-initial-config-probe");
        const tempDir = yield* Effect.promise(() =>
          NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "cursor-acp-")),
        );
        const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
        const argvLogPath = NodePath.join(tempDir, "argv.txt");
        yield* Effect.promise(() => NodeFSP.writeFile(requestLogPath, "", "utf8"));
        const wrapperPath = yield* Effect.promise(() =>
          makeProbeWrapper(requestLogPath, argvLogPath),
        );
        yield* serverSettings.updateSettings({
          providers: { cursor: { binaryPath: wrapperPath } },
        });

        const modelSelection = createModelSelection(ProviderInstanceId.make("cursor"), "gpt-5.4", [
          { id: "reasoning", value: "xhigh" },
          { id: "contextWindow", value: "1m" },
          { id: "fastMode", value: true },
        ]);

        yield* adapter.startSession({
          threadId,
          provider: ProviderDriverKind.make("cursor"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
          modelSelection,
        });

        yield* Effect.promise(() => waitForFileContent(requestLogPath));

        const requestsAfterStart = yield* Effect.promise(() => readJsonLines(requestLogPath));
        const configIdsAfterStart = requestsAfterStart.flatMap((entry) =>
          entry.method === "session/set_config_option" &&
          typeof (entry.params as Record<string, unknown> | undefined)?.configId === "string"
            ? [String((entry.params as Record<string, unknown>).configId)]
            : [],
        );
        assert.deepStrictEqual(configIdsAfterStart, [
          "model",
          "reasoning",
          "context",
          "fast",
          "mode",
        ]);

        yield* adapter.sendTurn({
          threadId,
          input: "hello mock",
          attachments: [],
          modelSelection,
          interactionMode: "default",
        });
        yield* adapter.stopSession(threadId);

        const finalRequests = yield* Effect.promise(() => readJsonLines(requestLogPath));
        const finalConfigIds = finalRequests.flatMap((entry) =>
          entry.method === "session/set_config_option" &&
          typeof (entry.params as Record<string, unknown> | undefined)?.configId === "string"
            ? [String((entry.params as Record<string, unknown>).configId)]
            : [],
        );
        assert.deepStrictEqual(finalConfigIds, ["model", "reasoning", "context", "fast", "mode"]);
        assert.equal(finalRequests.filter((entry) => entry.method === "session/prompt").length, 1);
      }),
  );

  it.effect(
    "streams ACP tool calls and approvals on the active turn in approval-required mode",
    () =>
      Effect.gen(function* () {
        const previousEmitToolCalls = process.env.T3_ACP_EMIT_TOOL_CALLS;
        process.env.T3_ACP_EMIT_TOOL_CALLS = "1";

        const adapter = yield* CursorAdapter;
        const serverSettings = yield* ServerSettingsService;
        const threadId = ThreadId.make("cursor-tool-call-probe");
        const runtimeEvents: Array<ProviderRuntimeEvent> = [];
        const settledEventTypes = new Set<string>();
        const settledEventsReady = yield* Deferred.make<void>();

        const wrapperPath = yield* Effect.promise(() =>
          makeMockAgentWrapper({ T3_ACP_EMIT_TOOL_CALLS: "1" }),
        );
        yield* serverSettings.updateSettings({
          providers: { cursor: { binaryPath: wrapperPath } },
        });

        yield* Stream.runForEach(adapter.streamEvents, (event) =>
          Effect.gen(function* () {
            runtimeEvents.push(event);
            if (String(event.threadId) !== String(threadId)) {
              return;
            }
            if (event.type === "request.opened" && event.requestId) {
              yield* adapter.respondToRequest(
                threadId,
                ApprovalRequestId.make(String(event.requestId)),
                "accept",
              );
            }
            if (
              event.type === "turn.completed" ||
              (event.type === "item.completed" && event.payload.itemType === "command_execution") ||
              event.type === "content.delta"
            ) {
              settledEventTypes.add(event.type);
              if (settledEventTypes.size === 3) {
                yield* Deferred.succeed(settledEventsReady, undefined).pipe(Effect.orDie);
              }
            }
          }),
        ).pipe(Effect.forkChild);

        const program = Effect.gen(function* () {
          yield* adapter.startSession({
            threadId,
            provider: ProviderDriverKind.make("cursor"),
            cwd: process.cwd(),
            runtimeMode: "approval-required",
            modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
          });

          const turn = yield* adapter.sendTurn({
            threadId,
            input: "run a tool call",
            attachments: [],
          });
          yield* Deferred.await(settledEventsReady);

          const threadEvents = runtimeEvents.filter(
            (event) => String(event.threadId) === String(threadId),
          );
          assert.includeMembers(
            threadEvents.map((event) => event.type),
            [
              "session.started",
              "session.state.changed",
              "thread.started",
              "turn.started",
              "request.opened",
              "request.resolved",
              "item.updated",
              "item.completed",
              "content.delta",
              "turn.completed",
            ],
          );

          const turnEvents = threadEvents.filter(
            (event) => String(event.turnId) === String(turn.turnId),
          );
          const toolUpdates = turnEvents.filter((event) => event.type === "item.updated");
          // ACP updates can arrive either as distinct pending + in-progress events
          // or as a single coalesced in-progress update before approval resolves.
          assert.isAtLeast(toolUpdates.length, 1);
          for (const toolUpdate of toolUpdates) {
            if (toolUpdate.type !== "item.updated") {
              continue;
            }
            assert.equal(toolUpdate.payload.itemType, "command_execution");
            assert.equal(toolUpdate.payload.status, "inProgress");
            assert.equal(toolUpdate.payload.detail, "cat server/package.json");
            assert.equal(String(toolUpdate.itemId), "tool-call-1");
          }

          const requestOpened = turnEvents.find((event) => event.type === "request.opened");
          assert.isDefined(requestOpened);
          if (requestOpened?.type === "request.opened") {
            assert.equal(String(requestOpened.turnId), String(turn.turnId));
            assert.equal(requestOpened.payload.requestType, "exec_command_approval");
            assert.equal(requestOpened.payload.detail, "cat server/package.json");
          }

          const requestResolved = turnEvents.find((event) => event.type === "request.resolved");
          assert.isDefined(requestResolved);
          if (requestResolved?.type === "request.resolved") {
            assert.equal(String(requestResolved.turnId), String(turn.turnId));
            assert.equal(requestResolved.payload.requestType, "exec_command_approval");
            assert.equal(requestResolved.payload.decision, "accept");
          }

          const toolCompleted = turnEvents.find(
            (event) =>
              event.type === "item.completed" && event.payload.itemType === "command_execution",
          );
          assert.isDefined(toolCompleted);
          if (toolCompleted?.type === "item.completed") {
            assert.equal(String(toolCompleted.turnId), String(turn.turnId));
            assert.equal(toolCompleted.payload.itemType, "command_execution");
            assert.equal(toolCompleted.payload.status, "completed");
            assert.equal(toolCompleted.payload.detail, "cat server/package.json");
            assert.equal(String(toolCompleted.itemId), "tool-call-1");
          }

          const contentDelta = turnEvents.find((event) => event.type === "content.delta");
          assert.isDefined(contentDelta);
          if (contentDelta?.type === "content.delta") {
            assert.equal(String(contentDelta.turnId), String(turn.turnId));
            assert.equal(contentDelta.payload.delta, "hello from mock");
            assert.equal(String(contentDelta.itemId), "assistant:mock-session-1:segment:0");
          }
        });

        yield* program.pipe(
          Effect.ensuring(
            Effect.sync(() => {
              if (previousEmitToolCalls === undefined) {
                delete process.env.T3_ACP_EMIT_TOOL_CALLS;
              } else {
                process.env.T3_ACP_EMIT_TOOL_CALLS = previousEmitToolCalls;
              }
            }),
          ),
        );
      }).pipe(
        Effect.provide(
          Layer.effect(
            CursorAdapter,
            Effect.gen(function* () {
              const cursorConfig = decodeCursorSettings({});
              const resolveSettings = yield* makeResolveCursorSettings;
              return yield* makeCursorAdapter(cursorConfig, { resolveSettings });
            }),
          ).pipe(
            Layer.provideMerge(ServerSettingsService.layerTest()),
            Layer.provideMerge(
              ServerConfig.layerTest(process.cwd(), {
                prefix: "t3code-cursor-adapter-test-",
              }),
            ),
            Layer.provideMerge(NodeServices.layer),
          ),
        ),
      ),
  );

  it.effect(
    "auto-approves ACP tool permissions in full-access mode without approval runtime events",
    () =>
      Effect.gen(function* () {
        const adapter = yield* CursorAdapter;
        const serverSettings = yield* ServerSettingsService;
        const threadId = ThreadId.make("cursor-full-access-auto-approve");
        const runtimeEvents: Array<ProviderRuntimeEvent> = [];
        const settledEventTypes = new Set<string>();
        const settledEventsReady = yield* Deferred.make<void>();
        const tempDir = yield* Effect.promise(() =>
          NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "cursor-acp-")),
        );
        const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
        const argvLogPath = NodePath.join(tempDir, "argv.txt");
        yield* Effect.promise(() => NodeFSP.writeFile(requestLogPath, "", "utf8"));
        const wrapperPath = yield* Effect.promise(() =>
          makeProbeWrapper(requestLogPath, argvLogPath, { T3_ACP_EMIT_TOOL_CALLS: "1" }),
        );
        yield* serverSettings.updateSettings({
          providers: { cursor: { binaryPath: wrapperPath } },
        });

        const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
          Effect.gen(function* () {
            runtimeEvents.push(event);
            if (String(event.threadId) !== String(threadId)) {
              return;
            }
            if (
              event.type === "turn.completed" ||
              (event.type === "item.completed" && event.payload.itemType === "command_execution") ||
              event.type === "content.delta"
            ) {
              settledEventTypes.add(event.type);
              if (settledEventTypes.size === 3) {
                yield* Deferred.succeed(settledEventsReady, undefined).pipe(Effect.orDie);
              }
            }
          }),
        ).pipe(Effect.forkChild);

        yield* adapter.startSession({
          threadId,
          provider: ProviderDriverKind.make("cursor"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
          modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
        });

        const turn = yield* adapter.sendTurn({
          threadId,
          input: "run a tool call",
          attachments: [],
        });

        yield* Deferred.await(settledEventsReady);
        yield* Fiber.interrupt(runtimeEventsFiber);

        const turnEvents = runtimeEvents.filter(
          (event) =>
            String(event.threadId) === String(threadId) &&
            String(event.turnId) === String(turn.turnId),
        );
        assert.notInclude(
          turnEvents.map((event) => event.type),
          "request.opened",
        );
        assert.notInclude(
          turnEvents.map((event) => event.type),
          "request.resolved",
        );
        assert.includeMembers(
          turnEvents.map((event) => event.type),
          ["item.updated", "item.completed", "content.delta", "turn.completed"],
        );

        const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
        const permissionResponse = requests.find(
          (entry) =>
            !("method" in entry) &&
            typeof entry.result === "object" &&
            entry.result !== null &&
            "outcome" in entry.result &&
            typeof entry.result.outcome === "object" &&
            entry.result.outcome !== null &&
            "outcome" in entry.result.outcome &&
            entry.result.outcome.outcome === "selected" &&
            "optionId" in entry.result.outcome &&
            entry.result.outcome.optionId === "allow-always",
        );
        assert.isDefined(permissionResponse);

        yield* adapter.stopSession(threadId);
      }),
  );

  it.effect("segments assistant messages around ACP tool activity in full-access mode", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-assistant-tool-segmentation");
      const runtimeEvents: Array<ProviderRuntimeEvent> = [];
      const settledEventTypes = new Set<string>();
      const settledEventsReady = yield* Deferred.make<void>();

      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({ T3_ACP_EMIT_INTERLEAVED_ASSISTANT_TOOL_CALLS: "1" }),
      );
      yield* serverSettings.updateSettings({
        providers: { cursor: { binaryPath: wrapperPath } },
      });

      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          runtimeEvents.push(event);
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          if (
            event.type === "content.delta" ||
            (event.type === "item.completed" && event.payload.itemType === "command_execution") ||
            event.type === "turn.completed"
          ) {
            if (event.type === "content.delta") {
              settledEventTypes.add(`delta:${event.payload.delta}`);
            } else {
              settledEventTypes.add(event.type);
            }
            if (
              settledEventTypes.has("delta:before tool") &&
              settledEventTypes.has("delta:after tool") &&
              settledEventTypes.has("item.completed") &&
              settledEventTypes.has("turn.completed")
            ) {
              yield* Deferred.succeed(settledEventsReady, undefined).pipe(Effect.orDie);
            }
          }
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const turn = yield* adapter.sendTurn({
        threadId,
        input: "run an interleaved tool call",
        attachments: [],
      });

      yield* Deferred.await(settledEventsReady);
      yield* Fiber.interrupt(runtimeEventsFiber);

      const turnEvents = runtimeEvents.filter(
        (event) =>
          String(event.threadId) === String(threadId) &&
          String(event.turnId) === String(turn.turnId),
      );
      const firstAssistantStartIndex = turnEvents.findIndex(
        (event) => event.type === "item.started" && event.payload.itemType === "assistant_message",
      );
      const firstAssistantDeltaIndex = turnEvents.findIndex(
        (event) => event.type === "content.delta" && event.payload.delta === "before tool",
      );
      const assistantBoundaryIndex = turnEvents.findIndex(
        (event) =>
          event.type === "item.completed" && event.payload.itemType === "assistant_message",
      );
      const toolUpdateIndex = turnEvents.findIndex(
        (event) => event.type === "item.updated" && event.payload.itemType === "command_execution",
      );
      const toolCompletedIndex = turnEvents.findIndex(
        (event) =>
          event.type === "item.completed" && event.payload.itemType === "command_execution",
      );
      const secondAssistantStartIndex = turnEvents.findIndex(
        (event, index) =>
          index > toolCompletedIndex &&
          event.type === "item.started" &&
          event.payload.itemType === "assistant_message",
      );
      const secondAssistantDeltaIndex = turnEvents.findIndex(
        (event) => event.type === "content.delta" && event.payload.delta === "after tool",
      );

      assert.isAtLeast(firstAssistantStartIndex, 0);
      assert.isAtLeast(firstAssistantDeltaIndex, 0);
      assert.isAtLeast(assistantBoundaryIndex, 0);
      assert.isAtLeast(toolUpdateIndex, 0);
      assert.isAtLeast(toolCompletedIndex, 0);
      assert.isAtLeast(secondAssistantStartIndex, 0);
      assert.isAtLeast(secondAssistantDeltaIndex, 0);
      assert.isBelow(firstAssistantStartIndex, firstAssistantDeltaIndex);
      assert.isBelow(firstAssistantDeltaIndex, assistantBoundaryIndex);
      assert.isBelow(assistantBoundaryIndex, toolUpdateIndex);
      assert.isBelow(toolUpdateIndex, toolCompletedIndex);
      assert.isBelow(toolCompletedIndex, secondAssistantStartIndex);
      assert.isBelow(secondAssistantStartIndex, secondAssistantDeltaIndex);

      const assistantStarts = turnEvents.filter(
        (event) => event.type === "item.started" && event.payload.itemType === "assistant_message",
      );
      const assistantDeltas = turnEvents.filter((event) => event.type === "content.delta");
      assert.lengthOf(assistantStarts, 2);
      assert.lengthOf(assistantDeltas, 2);
      if (
        assistantStarts[0]?.type === "item.started" &&
        assistantStarts[1]?.type === "item.started" &&
        assistantDeltas[0]?.type === "content.delta" &&
        assistantDeltas[1]?.type === "content.delta"
      ) {
        assert.notEqual(String(assistantStarts[0].itemId), String(assistantStarts[1].itemId));
        assert.equal(String(assistantDeltas[0].itemId), String(assistantStarts[0].itemId));
        assert.equal(String(assistantDeltas[1].itemId), String(assistantStarts[1].itemId));
      }

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("cancels pending ACP approvals and marks the turn cancelled when interrupted", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-cancel-probe");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "cursor-acp-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const argvLogPath = NodePath.join(tempDir, "argv.txt");
      yield* Effect.promise(() => NodeFSP.writeFile(requestLogPath, "", "utf8"));
      const wrapperPath = yield* Effect.promise(() =>
        makeProbeWrapper(requestLogPath, argvLogPath, { T3_ACP_EMIT_TOOL_CALLS: "1" }),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const requestResolvedReady = yield* Deferred.make<ProviderRuntimeEvent>();
      const turnCompletedReady = yield* Deferred.make<ProviderRuntimeEvent>();
      let interrupted = false;

      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          if (event.type === "request.opened" && !interrupted) {
            interrupted = true;
            yield* adapter.interruptTurn(threadId);
            return;
          }
          if (event.type === "request.resolved") {
            yield* Deferred.succeed(requestResolvedReady, event).pipe(Effect.ignore);
            return;
          }
          if (event.type === "turn.completed") {
            yield* Deferred.succeed(turnCompletedReady, event).pipe(Effect.ignore);
          }
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({
          threadId,
          input: "cancel this turn",
          attachments: [],
        })
        .pipe(Effect.forkChild);

      const requestResolved = yield* Deferred.await(requestResolvedReady);
      const turnCompleted = yield* Deferred.await(turnCompletedReady);
      yield* Fiber.join(sendTurnFiber);
      yield* Fiber.interrupt(runtimeEventsFiber);

      assert.equal(requestResolved.type, "request.resolved");
      if (requestResolved.type === "request.resolved") {
        assert.equal(requestResolved.payload.decision, "cancel");
      }

      assert.equal(turnCompleted.type, "turn.completed");
      if (turnCompleted.type === "turn.completed") {
        assert.equal(turnCompleted.payload.state, "cancelled");
        assert.equal(turnCompleted.payload.stopReason, "cancelled");
      }

      const isCancelledApprovalResponse = (entry: Record<string, unknown>) =>
        !("method" in entry) &&
        typeof entry.result === "object" &&
        entry.result !== null &&
        "outcome" in entry.result &&
        typeof entry.result.outcome === "object" &&
        entry.result.outcome !== null &&
        "outcome" in entry.result.outcome &&
        entry.result.outcome.outcome === "cancelled";
      yield* waitForJsonLogMatch(requestLogPath, (entry) => entry.method === "session/cancel");
      const requests = yield* waitForJsonLogMatch(requestLogPath, isCancelledApprovalResponse);
      assert.isTrue(requests.some((entry) => entry.method === "session/cancel"));
      assert.isTrue(requests.some(isCancelledApprovalResponse));

      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("drops late ACP notifications after a turn is cancelled", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-drop-late-cancelled-notifications");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({
          T3_ACP_EMIT_TOOL_CALLS: "1",
          T3_ACP_EMIT_LATE_UPDATE_AFTER_CANCEL: "1",
          T3_ACP_EMIT_LATE_CURSOR_EXTENSION_AFTER_CANCEL: "1",
        }),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const requestOpened =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "request.opened" }>>();
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      let interrupted = false;
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          runtimeEvents.push(event);
          if (event.type === "request.opened" && !interrupted) {
            interrupted = true;
            yield* Deferred.succeed(requestOpened, event).pipe(Effect.ignore);
            yield* adapter.interruptTurn(threadId);
            return;
          }
          if (event.type === "turn.completed") {
            yield* Deferred.succeed(turnCompleted, event).pipe(Effect.ignore);
          }
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "cancel before the late update", attachments: [] })
        .pipe(Effect.forkChild);
      const opened = yield* Deferred.await(requestOpened).pipe(Effect.timeout("2 seconds"));
      const completed = yield* Deferred.await(turnCompleted).pipe(Effect.timeout("2 seconds"));
      yield* Fiber.join(sendTurnFiber).pipe(Effect.timeout("2 seconds"));
      yield* Effect.sleep("150 millis");

      assert.equal(completed.type, "turn.completed");
      if (completed.type === "turn.completed") {
        assert.equal(completed.payload.state, "cancelled");
        assert.equal(completed.payload.stopReason, "cancelled");
      }

      const requestOpenedIndex = runtimeEvents.indexOf(opened);
      const cancelledIndex = runtimeEvents.findIndex(
        (event) =>
          event.type === "turn.completed" &&
          String(event.threadId) === String(threadId) &&
          String(event.turnId) === String(completed.turnId) &&
          event.payload.state === "cancelled",
      );
      const turnOutputTypes = new Set([
        "content.delta",
        "item.started",
        "item.updated",
        "item.completed",
        "turn.plan.updated",
      ]);
      const outputAfterCancellation = runtimeEvents
        .slice(cancelledIndex + 1)
        .filter(
          (event) => String(event.threadId) === String(threadId) && turnOutputTypes.has(event.type),
        );

      assert.isAtLeast(requestOpenedIndex, 0);
      assert.isAtLeast(cancelledIndex, 0);
      assert.deepEqual(
        runtimeEvents
          .slice(requestOpenedIndex + 1, cancelledIndex)
          .filter(
            (event) =>
              String(event.threadId) === String(threadId) && turnOutputTypes.has(event.type),
          ),
        [],
      );
      assert.deepEqual(outputAfterCancellation, []);
      assert.isFalse(
        runtimeEvents.some(
          (event) =>
            event.type === "content.delta" &&
            (event.payload.delta === "hello from mock" ||
              event.payload.delta === "late after cancel"),
        ),
      );

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("keeps late completed-turn notifications when an idle interrupt arrives", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-idle-interrupt-keeps-late-completed-output");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({
          T3_ACP_EMIT_DETACHED_LATE_UPDATE_AFTER_PROMPT_RETURN: "1",
          T3_ACP_DETACHED_LATE_UPDATE_AFTER_PROMPT_RETURN_DELAY_MS: "150",
        }),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const lateDelta =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "content.delta" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          if (event.type === "turn.completed") {
            yield* Deferred.succeed(turnCompleted, event).pipe(Effect.ignore);
            return;
          }
          if (
            event.type === "content.delta" &&
            event.payload.delta === "detached late after completion"
          ) {
            yield* Deferred.succeed(lateDelta, event).pipe(Effect.ignore);
          }
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "complete before idle interrupt", attachments: [] })
        .pipe(Effect.forkChild);
      const completed = yield* Deferred.await(turnCompleted).pipe(Effect.timeout("2 seconds"));
      yield* Fiber.join(sendTurnFiber).pipe(Effect.timeout("2 seconds"));
      yield* adapter.interruptTurn(threadId).pipe(Effect.timeout("2 seconds"));
      const delta = yield* Deferred.await(lateDelta).pipe(Effect.timeout("2 seconds"));

      assert.equal(completed.payload.state, "completed");
      assert.equal(delta.payload.delta, "detached late after completion");

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("keeps late completed-turn notifications off an immediate follow-up turn", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-follow-up-keeps-late-completed-output-on-old-turn");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({
          T3_ACP_EMIT_DETACHED_LATE_UPDATE_AFTER_PROMPT_RETURN: "1",
          T3_ACP_DETACHED_LATE_UPDATE_AFTER_PROMPT_RETURN_DELAY_MS: "150",
        }),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const lateDelta =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "content.delta" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          if (
            event.type === "content.delta" &&
            event.payload.delta === "detached late after completion"
          ) {
            yield* Deferred.succeed(lateDelta, event).pipe(Effect.ignore);
          }
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const firstTurn = yield* adapter.sendTurn({
        threadId,
        input: "complete before immediate follow-up",
        attachments: [],
      });
      const followUpFiber = yield* adapter
        .sendTurn({ threadId, input: "follow up before late chunk", attachments: [] })
        .pipe(Effect.forkChild);
      const secondFollowUpFiber = yield* adapter
        .sendTurn({ threadId, input: "second follow up during late chunk grace", attachments: [] })
        .pipe(Effect.forkChild);
      const delta = yield* Deferred.await(lateDelta).pipe(Effect.timeout("3 seconds"));
      const followUp = yield* Fiber.join(followUpFiber).pipe(Effect.timeout("3 seconds"));
      const secondFollowUp = yield* Fiber.join(secondFollowUpFiber).pipe(
        Effect.timeout("3 seconds"),
      );

      assert.equal(delta.payload.delta, "detached late after completion");
      assert.equal(String(delta.turnId), String(firstTurn.turnId));
      assert.notEqual(String(followUp.turnId), String(firstTurn.turnId));
      assert.equal(String(secondFollowUp.turnId), String(followUp.turnId));

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("does not send a prompt cancelled during completed-turn grace", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-cancel-during-completed-turn-grace");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({
          T3_ACP_EMIT_DETACHED_LATE_UPDATE_AFTER_PROMPT_RETURN: "1",
          T3_ACP_DETACHED_LATE_UPDATE_AFTER_PROMPT_RETURN_DELAY_MS: "150",
          T3_ACP_FAIL_PROMPTS_AFTER_FIRST: "1",
        }),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const cancelledTurnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const lateCompletedDelta =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "content.delta" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          if (
            event.type === "content.delta" &&
            event.payload.delta === "detached late after completion"
          ) {
            yield* Deferred.succeed(lateCompletedDelta, event).pipe(Effect.ignore);
          }
          if (event.type === "turn.completed" && event.payload.state === "cancelled") {
            yield* Deferred.succeed(cancelledTurnCompleted, event).pipe(Effect.ignore);
          }
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const firstTurn = yield* adapter.sendTurn({
        threadId,
        input: "complete before cancelled follow-up",
        attachments: [],
      });
      const followUpFiber = yield* adapter
        .sendTurn({ threadId, input: "cancel before this prompt is sent", attachments: [] })
        .pipe(Effect.forkChild);

      yield* Effect.sleep("50 millis");
      yield* adapter.interruptTurn(threadId).pipe(Effect.timeout("2 seconds"));
      const followUp = yield* Fiber.join(followUpFiber).pipe(Effect.timeout("3 seconds"));
      const cancelled = yield* Deferred.await(cancelledTurnCompleted).pipe(
        Effect.timeout("2 seconds"),
      );
      const lateDelta = yield* Deferred.await(lateCompletedDelta).pipe(Effect.timeout("2 seconds"));

      assert.notEqual(String(followUp.turnId), String(firstTurn.turnId));
      assert.equal(String(cancelled.turnId), String(followUp.turnId));
      assert.equal(String(lateDelta.turnId), String(firstTurn.turnId));
      const sessions = yield* adapter.listSessions();
      const session = sessions.find((entry) => entry.threadId === threadId);
      assert.equal(session?.status, "ready");
      assert.equal(session?.activeTurnId, undefined);

      yield* adapter.stopSession(threadId);
      yield* Fiber.interrupt(runtimeEventsFiber);
    }).pipe(TestClock.withLive),
  );

  it.effect("keeps no-pending cancelled turns suppressing detached late ACP updates", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-drop-detached-late-cancelled-output");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({
          T3_ACP_PROMPT_DELAY_MS: "1000",
          T3_ACP_EMIT_DETACHED_LATE_UPDATE_AFTER_CANCEL: "1",
          T3_ACP_EMIT_LOAD_REPLAY_LIVE_CONTINUATION: "1",
          T3_ACP_LOAD_REPLAY_LIVE_CONTINUATION_DELAY_MS: "50",
          T3_ACP_DELAY_LOAD_SESSION_AFTER_REPLAY: "1",
          T3_ACP_LOAD_SESSION_DELAY_MS: "250",
        }),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnStarted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.started" }>>();
      const followUpTurnStarted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.started" }>>();
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      let turnStartedCount = 0;
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          runtimeEvents.push(event);
          if (event.type === "turn.started") {
            turnStartedCount += 1;
            yield* Deferred.succeed(
              turnStartedCount === 1 ? turnStarted : followUpTurnStarted,
              event,
            ).pipe(Effect.ignore);
            return;
          }
          if (event.type === "turn.completed") {
            yield* Deferred.succeed(turnCompleted, event).pipe(Effect.ignore);
          }
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "cancel before any pending request", attachments: [] })
        .pipe(Effect.forkChild);
      yield* Deferred.await(turnStarted).pipe(Effect.timeout("2 seconds"));
      yield* Effect.sleep("50 millis");
      yield* adapter.interruptTurn(threadId).pipe(Effect.timeout("2 seconds"));
      const completed = yield* Deferred.await(turnCompleted).pipe(Effect.timeout("2 seconds"));
      yield* Fiber.join(sendTurnFiber).pipe(Effect.timeout("2 seconds"));
      const cancelledSnapshot = yield* adapter.readThread(threadId);
      assert.isAtLeast(cancelledSnapshot.turns.length, 1);
      const followUpFiber = yield* adapter
        .sendTurn({ threadId, input: "follow up before detached update", attachments: [] })
        .pipe(Effect.forkChild);
      yield* Effect.sleep("50 millis");
      const restartingSnapshot = yield* adapter
        .readThread(threadId)
        .pipe(Effect.timeout("1 second"));
      assert.deepEqual(
        restartingSnapshot.turns.map((turn) => String(turn.id)),
        cancelledSnapshot.turns.map((turn) => String(turn.id)),
      );
      yield* Deferred.await(followUpTurnStarted).pipe(Effect.timeout("3 seconds"));
      const restartedSnapshot = yield* adapter.readThread(threadId);
      assert.deepEqual(
        restartedSnapshot.turns
          .slice(0, cancelledSnapshot.turns.length)
          .map((turn) => String(turn.id)),
        cancelledSnapshot.turns.map((turn) => String(turn.id)),
      );
      yield* Effect.sleep("300 millis");

      assert.equal(completed.type, "turn.completed");
      if (completed.type === "turn.completed") {
        assert.equal(completed.payload.state, "cancelled");
      }
      const leakedDetachedDeltas = runtimeEvents.flatMap((event) =>
        event.type === "content.delta" &&
        (event.payload.delta === "detached late after cancel" ||
          event.payload.delta === " live continuation")
          ? [`${event.payload.delta}:${String(event.turnId)}`]
          : [],
      );
      assert.deepEqual(
        leakedDetachedDeltas,
        [],
        `detached cancelled prompt deltas leaked: ${leakedDetachedDeltas.join(", ")}`,
      );
      assert.isFalse(
        runtimeEvents.some((event) => event.type === "session.exited"),
        "internal Cursor restart must not publish provider session exit",
      );

      yield* adapter.stopSession(threadId);
      yield* Fiber.await(followUpFiber);
      yield* Fiber.interrupt(runtimeEventsFiber);
    }).pipe(TestClock.withLive),
  );

  it.effect("unblocks follow-up sends when Cursor cancel does not respond", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-wedged-cancel-unblocks-follow-up");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({
          T3_ACP_PROMPT_DELAY_MS: "1000",
          T3_ACP_HANG_CANCEL: "1",
        }),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const firstTurnStarted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.started" }>>();
      const followUpTurnStarted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.started" }>>();
      const cancelledTurnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      let turnStartedCount = 0;
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          if (event.type === "turn.started") {
            turnStartedCount += 1;
            yield* Deferred.succeed(
              turnStartedCount === 1 ? firstTurnStarted : followUpTurnStarted,
              event,
            ).pipe(Effect.ignore);
            return;
          }
          if (event.type === "turn.completed" && event.payload.state === "cancelled") {
            yield* Deferred.succeed(cancelledTurnCompleted, event).pipe(Effect.ignore);
          }
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "cancel against wedged cursor", attachments: [] })
        .pipe(Effect.forkChild);
      const firstStarted = yield* Deferred.await(firstTurnStarted).pipe(
        Effect.timeout("2 seconds"),
      );
      yield* adapter.interruptTurn(threadId).pipe(Effect.timeout("2 seconds"));
      const cancelled = yield* Deferred.await(cancelledTurnCompleted).pipe(
        Effect.timeout("2 seconds"),
      );
      const preJoinSnapshot = yield* adapter.readThread(threadId);
      assert.include(
        preJoinSnapshot.turns.map((turn) => String(turn.id)),
        String(firstStarted.turnId),
      );
      const cancelledTurn = yield* Fiber.join(sendTurnFiber).pipe(Effect.timeout("2 seconds"));

      const followUpFiber = yield* adapter
        .sendTurn({ threadId, input: "follow up after wedged cancel", attachments: [] })
        .pipe(Effect.forkChild);
      const followUpStarted = yield* Deferred.await(followUpTurnStarted).pipe(
        Effect.timeout("4 seconds"),
      );

      assert.equal(String(cancelled.turnId), String(firstStarted.turnId));
      assert.equal(String(cancelledTurn.turnId), String(firstStarted.turnId));
      assert.notEqual(String(followUpStarted.turnId), String(firstStarted.turnId));

      yield* adapter.stopSession(threadId);
      yield* Fiber.interrupt(followUpFiber);
      yield* Fiber.interrupt(runtimeEventsFiber);
    }).pipe(TestClock.withLive),
  );

  it.effect("retries the internal restart when Cursor session load fails", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-retry-failed-internal-restart");
      const failingWrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({
          T3_ACP_PROMPT_DELAY_MS: "1000",
          T3_ACP_FAIL_FIRST_LOAD_SESSION_AFTER_REPLAY: "1",
        }),
      );
      const workingWrapperPath = yield* Effect.promise(() => makeMockAgentWrapper());
      yield* serverSettings.updateSettings({
        providers: { cursor: { binaryPath: failingWrapperPath } },
      });

      const turnStarted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.started" }>>();
      const cancelledTurnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const restartedSessionStarted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "session.started" }>>();
      let sessionStartedCount = 0;
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          if (event.type === "session.started") {
            sessionStartedCount += 1;
            if (sessionStartedCount === 2) {
              yield* Deferred.succeed(restartedSessionStarted, event).pipe(Effect.ignore);
            }
            return;
          }
          if (event.type === "turn.started") {
            yield* Deferred.succeed(turnStarted, event).pipe(Effect.ignore);
            return;
          }
          if (event.type === "turn.completed" && event.payload.state === "cancelled") {
            yield* Deferred.succeed(cancelledTurnCompleted, event).pipe(Effect.ignore);
          }
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "cancel before failed restart", attachments: [] })
        .pipe(Effect.forkChild);
      yield* Deferred.await(turnStarted).pipe(Effect.timeout("2 seconds"));
      yield* Effect.sleep("50 millis");
      yield* adapter.interruptTurn(threadId).pipe(Effect.timeout("2 seconds"));
      yield* Deferred.await(cancelledTurnCompleted).pipe(Effect.timeout("2 seconds"));
      yield* Fiber.join(sendTurnFiber).pipe(Effect.timeout("2 seconds"));

      const failedRestart = yield* adapter
        .sendTurn({ threadId, input: "first retry should fail loading Cursor", attachments: [] })
        .pipe(Effect.exit);
      assert.equal(failedRestart._tag, "Failure");

      yield* serverSettings.updateSettings({
        providers: { cursor: { binaryPath: workingWrapperPath } },
      });
      const retryTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "retry after failed restart", attachments: [] })
        .pipe(Effect.forkChild);
      yield* Deferred.await(restartedSessionStarted).pipe(Effect.timeout("3 seconds"));
      yield* Fiber.join(retryTurnFiber).pipe(Effect.timeout("3 seconds"));

      yield* adapter.stopSession(threadId);
      yield* Fiber.interrupt(runtimeEventsFiber);
    }).pipe(TestClock.withLive),
  );

  it.effect("keeps cancelled prompt output suppressed when a follow-up send starts", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-drop-cancelled-output-before-follow-up");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({
          T3_ACP_EMIT_TOOL_CALLS: "1",
          T3_ACP_EMIT_LATE_UPDATE_AFTER_CANCEL: "1",
          T3_ACP_TOOL_CALL_AFTER_PERMISSION_DELAY_MS: "400",
        }),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const requestOpened =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "request.opened" }>>();
      const followUpRequestOpened =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "request.opened" }>>();
      const cancelledTurnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      let requestOpenedCount = 0;
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          runtimeEvents.push(event);
          if (event.type === "request.opened") {
            requestOpenedCount += 1;
            if (requestOpenedCount === 1) {
              yield* Deferred.succeed(requestOpened, event).pipe(Effect.ignore);
              return;
            }
            if (requestOpenedCount === 2) {
              yield* Deferred.succeed(followUpRequestOpened, event).pipe(Effect.ignore);
            }
          }
          if (event.type === "turn.completed" && event.payload.state === "cancelled") {
            yield* Deferred.succeed(cancelledTurnCompleted, event).pipe(Effect.ignore);
          }
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const firstSendFiber = yield* adapter
        .sendTurn({ threadId, input: "cancel before follow-up", attachments: [] })
        .pipe(Effect.forkChild);
      const opened = yield* Deferred.await(requestOpened).pipe(Effect.timeout("2 seconds"));
      const interruptFiber = yield* adapter.interruptTurn(threadId).pipe(Effect.forkChild);
      yield* Effect.sleep("10 millis");
      const followUpFiber = yield* adapter
        .sendTurn({ threadId, input: "follow up while cancel unwinds", attachments: [] })
        .pipe(Effect.forkChild);
      const secondFollowUpFiber = yield* adapter
        .sendTurn({ threadId, input: "second follow up while cancel unwinds", attachments: [] })
        .pipe(Effect.forkChild);

      yield* Fiber.join(interruptFiber).pipe(Effect.timeout("2 seconds"));
      yield* Deferred.await(cancelledTurnCompleted).pipe(Effect.timeout("2 seconds"));
      yield* Fiber.join(firstSendFiber).pipe(Effect.timeout("2 seconds"));

      const requestOpenedIndex = runtimeEvents.indexOf(opened);
      assert.isAtLeast(requestOpenedIndex, 0);
      const leakedCancelledDeltas = runtimeEvents
        .slice(requestOpenedIndex + 1)
        .flatMap((event) =>
          event.type === "content.delta" &&
          (event.payload.delta === "hello from mock" || event.payload.delta === "late after cancel")
            ? [`${event.payload.delta}:${String(event.turnId)}`]
            : [],
        );
      assert.deepEqual(
        leakedCancelledDeltas,
        [],
        `cancelled prompt deltas leaked: ${leakedCancelledDeltas.join(", ")}`,
      );

      yield* Deferred.await(followUpRequestOpened).pipe(Effect.timeout("3 seconds"));
      yield* Effect.sleep("100 millis");
      assert.isFalse(
        runtimeEvents.some((event) => event.type === "session.exited"),
        "concurrent internal Cursor restart must not publish provider session exit",
      );
      yield* adapter.stopSession(threadId);
      yield* Fiber.await(followUpFiber);
      yield* Fiber.await(secondFollowUpFiber);
      yield* Fiber.interrupt(runtimeEventsFiber);
    }).pipe(TestClock.withLive),
  );

  it.effect("stopping a session settles pending approval waits", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-stop-pending-approval");
      const approvalRequested = yield* Deferred.make<void>();

      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({ T3_ACP_EMIT_TOOL_CALLS: "1" }),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      yield* Stream.runForEach(adapter.streamEvents, (event) => {
        if (String(event.threadId) !== String(threadId) || event.type !== "request.opened") {
          return Effect.void;
        }
        return Deferred.succeed(approvalRequested, undefined).pipe(Effect.ignore);
      }).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({
          threadId,
          input: "run a tool call and then stop",
          attachments: [],
        })
        .pipe(Effect.forkChild);

      yield* Deferred.await(approvalRequested);
      yield* adapter.stopSession(threadId);
      yield* Fiber.await(sendTurnFiber);

      assert.equal(yield* adapter.hasSession(threadId), false);
    }),
  );

  it.effect("stopping a session settles pending user-input waits", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-stop-pending-user-input");
      const userInputRequested = yield* Deferred.make<void>();

      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({ T3_ACP_EMIT_ASK_QUESTION: "1" }),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      yield* Stream.runForEach(adapter.streamEvents, (event) => {
        if (String(event.threadId) !== String(threadId) || event.type !== "user-input.requested") {
          return Effect.void;
        }
        return Deferred.succeed(userInputRequested, undefined).pipe(Effect.ignore);
      }).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({
          threadId,
          input: "ask me a question and then stop",
          attachments: [],
        })
        .pipe(Effect.forkChild);

      yield* Deferred.await(userInputRequested);
      yield* adapter.stopSession(threadId);
      yield* Fiber.await(sendTurnFiber);

      assert.equal(yield* adapter.hasSession(threadId), false);
    }),
  );

  it.effect("interrupting a session settles pending user-input waits", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-interrupt-pending-user-input");
      const userInputRequested = yield* Deferred.make<void>();
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();

      const wrapperPath = yield* Effect.promise(() =>
        makeMockAgentWrapper({ T3_ACP_EMIT_ASK_QUESTION: "1" }),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          if (event.type === "user-input.requested") {
            yield* Deferred.succeed(userInputRequested, undefined).pipe(Effect.ignore);
            return;
          }
          if (event.type === "turn.completed") {
            yield* Deferred.succeed(turnCompleted, event).pipe(Effect.ignore);
          }
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({
          threadId,
          input: "ask me a question and then interrupt",
          attachments: [],
        })
        .pipe(Effect.forkChild);

      yield* Deferred.await(userInputRequested);
      yield* adapter.interruptTurn(threadId);
      const completed = yield* Deferred.await(turnCompleted).pipe(Effect.timeout("2 seconds"));
      yield* Fiber.await(sendTurnFiber);

      assert.equal(completed.payload.state, "cancelled");
      assert.equal(completed.payload.stopReason, "cancelled");
      assert.equal(yield* adapter.hasSession(threadId), true);
      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("broadcasts runtime events to multiple stream consumers", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-runtime-event-broadcast");

      const wrapperPath = yield* Effect.promise(() => makeMockAgentWrapper());
      yield* settings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const firstConsumer = yield* Stream.take(adapter.streamEvents, 3).pipe(
        Stream.runCollect,
        Effect.forkChild,
      );
      const secondConsumer = yield* Stream.take(adapter.streamEvents, 3).pipe(
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "default" },
      });

      const firstEvents = Array.from(yield* Fiber.join(firstConsumer));
      const secondEvents = Array.from(yield* Fiber.join(secondConsumer));

      assert.deepStrictEqual(
        firstEvents.map((event) => event.type),
        ["session.started", "session.state.changed", "thread.started"],
      );
      assert.deepStrictEqual(
        secondEvents.map((event) => event.type),
        ["session.started", "session.state.changed", "thread.started"],
      );

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("switches model in-session via session/set_config_option", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-model-switch");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "cursor-acp-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const argvLogPath = NodePath.join(tempDir, "argv.txt");
      yield* Effect.promise(() => NodeFSP.writeFile(requestLogPath, "", "utf8"));
      const wrapperPath = yield* Effect.promise(() =>
        makeProbeWrapper(requestLogPath, argvLogPath),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "composer-2" },
      });

      yield* adapter.sendTurn({
        threadId,
        input: "first turn",
        attachments: [],
      });

      yield* adapter.sendTurn({
        threadId,
        input: "second turn after switching model",
        attachments: [],
        modelSelection: createModelSelection(ProviderInstanceId.make("cursor"), "composer-2", [
          { id: "fastMode", value: true },
        ]),
      });

      const argvRuns = yield* Effect.promise(() => readArgvLog(argvLogPath));
      assert.lengthOf(argvRuns, 1, "session should not restart — only one spawn");
      assert.deepStrictEqual(argvRuns[0], ["acp"]);

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const setConfigRequests = requests.filter(
        (entry) =>
          entry.method === "session/set_config_option" &&
          (entry.params as Record<string, unknown> | undefined)?.configId === "model",
      );
      assert.isAbove(setConfigRequests.length, 0, "should call session/set_config_option");
      assert.equal((setConfigRequests[0]?.params as Record<string, unknown>)?.value, "composer-2");

      const fastConfigRequests = requests.filter(
        (entry) =>
          entry.method === "session/set_config_option" &&
          (entry.params as Record<string, unknown> | undefined)?.configId === "fast",
      );
      assert.isAbove(fastConfigRequests.length, 0, "should apply fast mode as a separate config");
      const lastFastConfig = fastConfigRequests[fastConfigRequests.length - 1];
      assert.equal((lastFastConfig?.params as Record<string, unknown>)?.value, "true");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("clears prior fast mode in-session when the next turn sets fastMode: false", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-fast-mode-reset");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "cursor-acp-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const argvLogPath = NodePath.join(tempDir, "argv.txt");
      yield* Effect.promise(() => NodeFSP.writeFile(requestLogPath, "", "utf8"));
      const wrapperPath = yield* Effect.promise(() =>
        makeProbeWrapper(requestLogPath, argvLogPath),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "composer-2" },
      });

      yield* adapter.sendTurn({
        threadId,
        input: "first turn with fast mode",
        attachments: [],
        modelSelection: createModelSelection(ProviderInstanceId.make("cursor"), "composer-2", [
          { id: "fastMode", value: true },
        ]),
      });

      yield* adapter.sendTurn({
        threadId,
        input: "second turn without fast mode",
        attachments: [],
        modelSelection: createModelSelection(ProviderInstanceId.make("cursor"), "composer-2", [
          { id: "fastMode", value: false },
        ]),
      });

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const fastConfigRequests = requests.filter(
        (entry) =>
          entry.method === "session/set_config_option" &&
          (entry.params as Record<string, unknown> | undefined)?.configId === "fast",
      );
      assert.isAtLeast(fastConfigRequests.length, 2, "should set fast mode on and then off");

      const lastFastConfig = fastConfigRequests[fastConfigRequests.length - 1];
      assert.equal((lastFastConfig?.params as Record<string, unknown>)?.value, "false");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("preserves Cursor model options across internal restart after cancellation", () =>
    Effect.gen(function* () {
      const adapter = yield* CursorAdapter;
      const serverSettings = yield* ServerSettingsService;
      const threadId = ThreadId.make("cursor-restart-preserves-model-options");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "cursor-acp-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const argvLogPath = NodePath.join(tempDir, "argv.txt");
      yield* Effect.promise(() => NodeFSP.writeFile(requestLogPath, "", "utf8"));
      const wrapperPath = yield* Effect.promise(() =>
        makeProbeWrapper(requestLogPath, argvLogPath, {
          T3_ACP_PROMPT_DELAY_MS: "1000",
        }),
      );
      yield* serverSettings.updateSettings({ providers: { cursor: { binaryPath: wrapperPath } } });

      const turnStarted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.started" }>>();
      const cancelledTurnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const followUpTurnStarted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.started" }>>();
      let turnStartedCount = 0;
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          if (event.type === "turn.started") {
            turnStartedCount += 1;
            yield* Deferred.succeed(
              turnStartedCount === 1 ? turnStarted : followUpTurnStarted,
              event,
            ).pipe(Effect.ignore);
          }
          if (event.type === "turn.completed" && event.payload.state === "cancelled") {
            yield* Deferred.succeed(cancelledTurnCompleted, event).pipe(Effect.ignore);
          }
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("cursor"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: createModelSelection(ProviderInstanceId.make("cursor"), "composer-2", [
          { id: "fastMode", value: true },
        ]),
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "cancel before restart", attachments: [] })
        .pipe(Effect.forkChild);
      yield* Deferred.await(turnStarted).pipe(Effect.timeout("2 seconds"));
      yield* Effect.sleep("50 millis");
      yield* adapter.interruptTurn(threadId).pipe(Effect.timeout("2 seconds"));
      yield* Deferred.await(cancelledTurnCompleted).pipe(Effect.timeout("2 seconds"));
      yield* Fiber.join(sendTurnFiber).pipe(Effect.timeout("2 seconds"));

      const followUpFiber = yield* adapter
        .sendTurn({ threadId, input: "follow up without model selection", attachments: [] })
        .pipe(Effect.forkChild);
      yield* Deferred.await(followUpTurnStarted).pipe(Effect.timeout("3 seconds"));
      yield* adapter.stopSession(threadId);
      yield* Fiber.await(followUpFiber);
      yield* Fiber.interrupt(runtimeEventsFiber);

      const argvRuns = yield* Effect.promise(() => readArgvLog(argvLogPath));
      assert.isAtLeast(
        argvRuns.length,
        2,
        "internal restart should spawn a replacement ACP process",
      );

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const fastConfigRequests = requests.filter(
        (entry) =>
          entry.method === "session/set_config_option" &&
          (entry.params as Record<string, unknown> | undefined)?.configId === "fast",
      );
      assert.isAtLeast(
        fastConfigRequests.length,
        2,
        "fast mode should be applied on initial session and internal restart",
      );
      const lastFastConfig = fastConfigRequests[fastConfigRequests.length - 1];
      assert.equal((lastFastConfig?.params as Record<string, unknown>)?.value, "true");
    }).pipe(TestClock.withLive),
  );

  it.effect(
    "applies fast mode on the first turn when modelSelection uses a non-default instance id",
    () => {
      const customInstanceId = ProviderInstanceId.make("cursor_secondary");
      // Custom-instance cases can't share the suite-level `CursorAdapter`
      // layer because that one binds `instanceId: "cursor"`. We build a
      // fresh layer graph — including a fresh `ServerSettingsService` — so
      // mid-test `updateSettings` calls target the same service instance the
      // adapter's `resolveSettings` reads from, and so the outer
      // `yield* ServerSettingsService` sees the same snapshot as well.
      const customAdapterLayer = Layer.effect(
        CursorAdapter,
        Effect.gen(function* () {
          const cursorConfig = decodeCursorSettings({});
          const resolveSettings = yield* makeResolveCursorSettings;
          return yield* makeCursorAdapter(cursorConfig, {
            instanceId: customInstanceId,
            resolveSettings,
          });
        }),
      ).pipe(
        Layer.provideMerge(ServerSettingsService.layerTest()),
        Layer.provideMerge(
          ServerConfig.layerTest(process.cwd(), {
            prefix: "t3code-cursor-adapter-custom-instance-",
          }),
        ),
        Layer.provideMerge(NodeServices.layer),
      );

      return Effect.gen(function* () {
        const adapter = yield* CursorAdapter;
        const serverSettings = yield* ServerSettingsService;
        const threadId = ThreadId.make("cursor-fast-mode-custom-instance");
        const tempDir = yield* Effect.promise(() =>
          NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "cursor-acp-")),
        );
        const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
        const argvLogPath = NodePath.join(tempDir, "argv.txt");
        yield* Effect.promise(() => NodeFSP.writeFile(requestLogPath, "", "utf8"));
        const wrapperPath = yield* Effect.promise(() =>
          makeProbeWrapper(requestLogPath, argvLogPath),
        );
        yield* serverSettings.updateSettings({
          providers: { cursor: { binaryPath: wrapperPath } },
        });

        yield* adapter.startSession({
          threadId,
          provider: ProviderDriverKind.make("cursor"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
          modelSelection: {
            instanceId: customInstanceId,
            model: "composer-2",
          },
        });

        yield* adapter.sendTurn({
          threadId,
          input: "first turn with fast mode",
          attachments: [],
          modelSelection: {
            ...createModelSelection(ProviderInstanceId.make("cursor"), "composer-2", [
              { id: "fastMode", value: true },
            ]),
            instanceId: customInstanceId,
          },
        });

        const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
        const fastConfigRequests = requests.filter(
          (entry) =>
            entry.method === "session/set_config_option" &&
            (entry.params as Record<string, unknown> | undefined)?.configId === "fast",
        );
        assert.isAbove(
          fastConfigRequests.length,
          0,
          "fast mode should apply when instance id matches the adapter binding",
        );
        const lastFastConfig = fastConfigRequests[fastConfigRequests.length - 1];
        assert.equal((lastFastConfig?.params as Record<string, unknown>)?.value, "true");

        yield* adapter.stopSession(threadId);
      }).pipe(Effect.provide(customAdapterLayer));
    },
  );
});
