// @effect-diagnostics nodeBuiltinImport:off
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  ApprovalRequestId,
  CodexSettings,
  EnvironmentId,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderItemId,
  type ProviderApprovalDecision,
  type ProviderEvent,
  type ProviderSession,
  type ProviderTurnStartResult,
  type ProviderUserInputAnswers,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it, vi } from "@effect/vitest";

import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as CodexErrors from "effect-codex-app-server/errors";

import { ServerConfig } from "../../config.ts";
import { makeCodexMcpRuntimeConfig } from "../../mcp/McpProviderInjection.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { ProviderAdapterValidationError } from "../Errors.ts";
import type { CodexAdapterShape } from "../Services/CodexAdapter.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import {
  type CodexAdapterLiveOptions,
  deleteSessionIfCurrent,
  makeCodexAdapter,
} from "./CodexAdapter.ts";
import {
  type CodexSessionRuntimeOptions,
  type CodexSessionRuntimeSendTurnInput,
  type CodexSessionRuntimeShape,
  type CodexThreadSnapshot,
} from "./CodexSessionRuntime.ts";
const decodeCodexSettings = Schema.decodeSync(CodexSettings);

// Test-local service tag so the rest of the file can keep using `yield* CodexAdapter`.
class CodexAdapter extends Context.Service<CodexAdapter, CodexAdapterShape>()(
  "t3/provider/Layers/CodexAdapter.test/CodexAdapter",
) {}

const asThreadId = (value: string): ThreadId => ThreadId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);
const asEventId = (value: string): EventId => EventId.make(value);
const asItemId = (value: string): ProviderItemId => ProviderItemId.make(value);

class FakeCodexRuntime implements CodexSessionRuntimeShape {
  private readonly eventQueue = Effect.runSync(Queue.unbounded<ProviderEvent>());
  private readonly now = "2026-01-01T00:00:00.000Z";

  public readonly startImpl = vi.fn(() =>
    Promise.resolve({
      provider: ProviderDriverKind.make("codex"),
      status: "ready" as const,
      runtimeMode: this.options.runtimeMode,
      threadId: this.options.threadId,
      cwd: this.options.cwd,
      ...(this.options.model ? { model: this.options.model } : {}),
      createdAt: this.now,
      updatedAt: this.now,
    } satisfies ProviderSession),
  );

  public readonly sendTurnImpl = vi.fn(
    (_input: CodexSessionRuntimeSendTurnInput): Promise<ProviderTurnStartResult> =>
      Promise.resolve({
        threadId: this.options.threadId,
        turnId: asTurnId("turn-1"),
      }),
  );

  public readonly interruptTurnImpl = vi.fn(
    (_turnId?: TurnId): Promise<void> => Promise.resolve(undefined),
  );

  public readonly readThreadImpl = vi.fn(
    (): Promise<CodexThreadSnapshot> =>
      Promise.resolve({
        threadId: "provider-thread-1",
        turns: [],
      }),
  );

  public readonly rollbackThreadImpl = vi.fn(
    (_numTurns: number): Promise<CodexThreadSnapshot> =>
      Promise.resolve({
        threadId: "provider-thread-1",
        turns: [],
      }),
  );

  public readonly respondToRequestImpl = vi.fn(
    (_requestId: ApprovalRequestId, _decision: ProviderApprovalDecision): Promise<void> =>
      Promise.resolve(undefined),
  );

  public readonly respondToUserInputImpl = vi.fn(
    (_requestId: ApprovalRequestId, _answers: ProviderUserInputAnswers): Promise<void> =>
      Promise.resolve(undefined),
  );

  public readonly closeImpl = vi.fn(() => Promise.resolve(undefined));

  public startOverride: (() => Effect.Effect<ProviderSession>) | undefined;
  public closeOverride: (() => Effect.Effect<void>) | undefined;

  readonly options: CodexSessionRuntimeOptions;

  constructor(options: CodexSessionRuntimeOptions) {
    this.options = options;
  }

  makeSession(): ProviderSession {
    return {
      provider: ProviderDriverKind.make("codex"),
      status: "ready",
      runtimeMode: this.options.runtimeMode,
      threadId: this.options.threadId,
      cwd: this.options.cwd,
      ...(this.options.model ? { model: this.options.model } : {}),
      createdAt: this.now,
      updatedAt: this.now,
    };
  }

  start() {
    if (this.startOverride) return this.startOverride();
    return Effect.promise(() => this.startImpl());
  }

  getSession = Effect.promise(() => this.startImpl());

  sendTurn(input: CodexSessionRuntimeSendTurnInput) {
    return Effect.promise(() => this.sendTurnImpl(input));
  }

  interruptTurn(turnId?: TurnId) {
    return Effect.promise(() => this.interruptTurnImpl(turnId));
  }

  readThread = Effect.promise(() => this.readThreadImpl());

  rollbackThread(numTurns: number) {
    return Effect.promise(() => this.rollbackThreadImpl(numTurns));
  }

  respondToRequest(requestId: ApprovalRequestId, decision: ProviderApprovalDecision) {
    return Effect.promise(() => this.respondToRequestImpl(requestId, decision));
  }

  respondToUserInput(requestId: ApprovalRequestId, answers: ProviderUserInputAnswers) {
    return Effect.promise(() => this.respondToUserInputImpl(requestId, answers));
  }

  get events() {
    return Stream.fromQueue(this.eventQueue);
  }

  close = Effect.suspend(() =>
    this.closeOverride ? this.closeOverride() : Effect.promise(() => this.closeImpl()),
  );

  emit(event: ProviderEvent) {
    return Queue.offer(this.eventQueue, event).pipe(Effect.asVoid);
  }
}

function makeRuntimeFactory() {
  const runtimes: Array<FakeCodexRuntime> = [];
  const factory = vi.fn((options: CodexSessionRuntimeOptions) => {
    const runtime = new FakeCodexRuntime(options);
    runtimes.push(runtime);
    return Effect.succeed(runtime);
  });

  return {
    factory,
    get runtimes(): ReadonlyArray<FakeCodexRuntime> {
      return runtimes;
    },
    get lastRuntime(): FakeCodexRuntime | undefined {
      return runtimes.at(-1);
    },
  };
}

type TestRuntimeFactory = NonNullable<CodexAdapterLiveOptions["makeRuntime"]>;

const makeAdapterTestLayer = (factory: TestRuntimeFactory) =>
  Layer.effect(
    CodexAdapter,
    Effect.gen(function* () {
      const codexConfig = decodeCodexSettings({});
      return yield* makeCodexAdapter(codexConfig, { makeRuntime: factory });
    }),
  ).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  );

const buildAdapterWithScope = Effect.fn("CodexAdapter.test.buildAdapterWithScope")(function* (
  factory: TestRuntimeFactory,
  scope: Scope.Closeable,
) {
  const context = yield* Layer.buildWithScope(makeAdapterTestLayer(factory), scope);
  return yield* Effect.service(CodexAdapter).pipe(Effect.provide(context));
});

function makeScopedRuntimeFactory(options?: { readonly failConstruction?: boolean }) {
  const runtimes: Array<FakeCodexRuntime> = [];
  const releasedThreadIds: Array<ThreadId> = [];

  const factory = vi.fn((runtimeOptions: CodexSessionRuntimeOptions) =>
    Effect.gen(function* () {
      yield* Scope.Scope;
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          releasedThreadIds.push(runtimeOptions.threadId);
        }),
      );

      if (options?.failConstruction) {
        return yield* new CodexErrors.CodexAppServerSpawnError({
          command: `${runtimeOptions.binaryPath} app-server`,
          cause: new Error("runtime construction failed"),
        });
      }

      const runtime = new FakeCodexRuntime(runtimeOptions);
      runtimes.push(runtime);
      return runtime;
    }),
  );

  return {
    factory,
    releasedThreadIds,
    get lastRuntime(): FakeCodexRuntime | undefined {
      return runtimes.at(-1);
    },
  };
}

const providerSessionDirectoryTestLayer = Layer.succeed(ProviderSessionDirectory, {
  upsert: () => Effect.void,
  getProvider: () =>
    Effect.die(new Error("ProviderSessionDirectory.getProvider is not used in test")),
  getBinding: () => Effect.succeed(Option.none()),
  listThreadIds: () => Effect.succeed([]),
  listBindings: () => Effect.succeed([]),
});

const validationRuntimeFactory = makeRuntimeFactory();
const validationLayer = it.layer(
  Layer.effect(
    CodexAdapter,
    Effect.gen(function* () {
      const codexConfig = decodeCodexSettings({});
      return yield* makeCodexAdapter(codexConfig, {
        makeRuntime: validationRuntimeFactory.factory,
      });
    }),
  ).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  ),
);

validationLayer("CodexAdapterLive validation", (it) => {
  it.effect("returns validation error for non-codex provider on startSession", () =>
    Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const result = yield* adapter
        .startSession({
          provider: ProviderDriverKind.make("claudeAgent"),
          threadId: asThreadId("thread-1"),
          runtimeMode: "full-access",
        })
        .pipe(Effect.result);

      NodeAssert.equal(result._tag, "Failure");
      NodeAssert.deepStrictEqual(
        result.failure,
        new ProviderAdapterValidationError({
          provider: ProviderDriverKind.make("codex"),
          operation: "startSession",
          issue: "Expected provider 'codex' but received 'claudeAgent'.",
        }),
      );
      NodeAssert.equal(validationRuntimeFactory.factory.mock.calls.length, 0);
    }),
  );
  it.effect("maps codex model options before starting a session", () =>
    Effect.gen(function* () {
      validationRuntimeFactory.factory.mockClear();
      const adapter = yield* CodexAdapter;

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.3-codex", [
          { id: "serviceTier", value: "priority" },
        ]),
        runtimeMode: "full-access",
      });

      NodeAssert.deepStrictEqual(validationRuntimeFactory.factory.mock.calls[0]?.[0], {
        binaryPath: "codex",
        cwd: process.cwd(),
        launchArgs: "",
        model: "gpt-5.3-codex",
        providerInstanceId: ProviderInstanceId.make("codex"),
        serviceTier: "priority",
        threadId: asThreadId("thread-1"),
        runtimeMode: "full-access",
      });
    }),
  );

  it.effect("injects the canonical MCP server into Codex app-server sessions", () => {
    const runtimeFactory = makeRuntimeFactory();
    const baseEnvironment = { KEEP_ME: "1" };
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          environment: baseEnvironment,
          makeRuntime: runtimeFactory.factory,
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );
    const threadId = asThreadId("thread-codex-mcp-injection");
    const mcpSession: McpProviderSession.McpProviderSessionConfig = {
      environmentId: EnvironmentId.make("environment-codex-mcp-injection"),
      threadId,
      providerSessionId: "provider-session-codex-mcp-injection",
      providerInstanceId: ProviderInstanceId.make("codex"),
      endpoint: "http://127.0.0.1:3773/mcp",
      authorizationHeader: "Bearer codex-mcp-token",
    };

    return Effect.gen(function* () {
      McpProviderSession.setMcpProviderSession(mcpSession);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => McpProviderSession.clearMcpProviderSession(threadId)),
      );

      const adapter = yield* CodexAdapter;
      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("codex"),
        runtimeMode: "full-access",
      });

      const runtime = runtimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      const expected = makeCodexMcpRuntimeConfig(mcpSession, baseEnvironment);
      NodeAssert.deepStrictEqual(runtime.options.environment, expected.environment);
      NodeAssert.deepStrictEqual(runtime.options.appServerArgs, expected.appServerArgs);
    }).pipe(Effect.scoped, Effect.provide(layer));
  });
});

const sessionRuntimeFactory = makeRuntimeFactory();
const sessionErrorLayer = it.layer(
  Layer.effect(
    CodexAdapter,
    Effect.gen(function* () {
      const codexConfig = decodeCodexSettings({});
      return yield* makeCodexAdapter(codexConfig, {
        makeRuntime: sessionRuntimeFactory.factory,
      });
    }),
  ).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  ),
);

sessionErrorLayer("CodexAdapterLive session errors", (it) => {
  it.effect("maps missing adapter sessions to ProviderAdapterSessionNotFoundError", () =>
    Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const result = yield* adapter
        .sendTurn({
          threadId: asThreadId("sess-missing"),
          input: "hello",
          attachments: [],
        })
        .pipe(Effect.result);

      NodeAssert.equal(result._tag, "Failure");
      NodeAssert.equal(result.failure._tag, "ProviderAdapterSessionNotFoundError");
      NodeAssert.equal(result.failure.provider, "codex");
      NodeAssert.equal(result.failure.threadId, "sess-missing");
    }),
  );

  it.effect("maps codex model options before sending a turn", () =>
    Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("sess-missing"),
        runtimeMode: "full-access",
      });
      const runtime = sessionRuntimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      runtime.sendTurnImpl.mockClear();

      yield* Effect.ignore(
        adapter.sendTurn({
          threadId: asThreadId("sess-missing"),
          input: "hello",
          modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.3-codex", [
            { id: "reasoningEffort", value: "high" },
            { id: "serviceTier", value: "priority" },
          ]),
          attachments: [],
        }),
      );

      NodeAssert.deepStrictEqual(runtime.sendTurnImpl.mock.calls[0]?.[0], {
        input: "hello",
        model: "gpt-5.3-codex",
        effort: "high",
        serviceTier: "priority",
      });
    }),
  );

  it.effect("passes configured launch args into the session runtime", () => {
    const runtimeFactory = makeRuntimeFactory();
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({ launchArgs: "--strict-config --enable foo" });
        return yield* makeCodexAdapter(codexConfig, {
          makeRuntime: runtimeFactory.factory,
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("sess-launch-args"),
        runtimeMode: "full-access",
      });

      const runtime = runtimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      NodeAssert.equal(runtime.options.launchArgs, "--strict-config --enable foo");
    }).pipe(Effect.provide(layer));
  });

  it.effect("uses T3CODE_CODEX_LAUNCH_ARGS for the session runtime", () => {
    const runtimeFactory = makeRuntimeFactory();
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({ launchArgs: "--enable settings-feature" });
        return yield* makeCodexAdapter(codexConfig, {
          environment: { T3CODE_CODEX_LAUNCH_ARGS: " --strict-config --enable env-feature " },
          makeRuntime: runtimeFactory.factory,
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("sess-launch-args-env"),
        runtimeMode: "full-access",
      });

      const runtime = runtimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      NodeAssert.equal(runtime.options.launchArgs, "--strict-config --enable env-feature");
    }).pipe(Effect.provide(layer));
  });

  it.effect("maps codex model options for the adapter's bound custom instance id", () => {
    const customInstanceId = ProviderInstanceId.make("codex_personal");
    const customRuntimeFactory = makeRuntimeFactory();
    const customLayer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          instanceId: customInstanceId,
          makeRuntime: customRuntimeFactory.factory,
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("sess-custom-instance"),
        runtimeMode: "full-access",
      });
      const runtime = customRuntimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      runtime.sendTurnImpl.mockClear();

      yield* Effect.ignore(
        adapter.sendTurn({
          threadId: asThreadId("sess-custom-instance"),
          input: "hello",
          modelSelection: createModelSelection(
            ProviderInstanceId.make("codex_personal"),
            "gpt-5.3-codex",
            [
              { id: "reasoningEffort", value: "high" },
              { id: "serviceTier", value: "flex" },
            ],
          ),
          attachments: [],
        }),
      );

      NodeAssert.deepStrictEqual(runtime.sendTurnImpl.mock.calls[0]?.[0], {
        input: "hello",
        model: "gpt-5.3-codex",
        effort: "high",
        serviceTier: "flex",
      });
    }).pipe(Effect.provide(customLayer));
  });
});

const lifecycleRuntimeFactory = makeRuntimeFactory();
const lifecycleLayer = it.layer(
  Layer.effect(
    CodexAdapter,
    Effect.gen(function* () {
      const codexConfig = decodeCodexSettings({});
      return yield* makeCodexAdapter(codexConfig, {
        makeRuntime: lifecycleRuntimeFactory.factory,
      });
    }),
  ).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  ),
);

function startLifecycleRuntime() {
  return Effect.gen(function* () {
    const adapter = yield* CodexAdapter;
    yield* adapter.startSession({
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      runtimeMode: "full-access",
    });
    const runtime = lifecycleRuntimeFactory.lastRuntime;
    NodeAssert.ok(runtime);
    return { adapter, runtime };
  });
}

lifecycleLayer("CodexAdapterLive lifecycle", (it) => {
  it.effect("preserves detached-child routing across the real completion sequence", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 3)).pipe(
        Effect.forkChild,
      );
      const threadId = asThreadId("thread-1");
      const turnId = asTurnId("turn-detached-child");
      const itemId = asItemId("message-detached-child");

      yield* runtime.emit({
        id: asEventId("evt-detached-child-turn-started"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:01.000Z",
        method: "turn/started",
        threadId,
        turnId,
        payload: {
          threadId,
          turn: { id: turnId, status: "inProgress", items: [] },
        },
      } satisfies ProviderEvent);
      yield* runtime.emit({
        id: asEventId("evt-detached-child-item-completed"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:02.000Z",
        method: "item/completed",
        threadId,
        turnId,
        itemId,
        payload: {
          completedAtMs: 1_778_000_000_000,
          threadId,
          turnId,
          item: {
            type: "agentMessage",
            id: itemId,
            text: "detached child completed",
          },
        },
      } satisfies ProviderEvent);
      yield* runtime.emit({
        id: asEventId("evt-detached-child-turn-completed"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:03.000Z",
        method: "turn/completed",
        threadId,
        turnId,
        payload: {
          threadId,
          turn: { id: turnId, status: "completed", items: [] },
        },
      } satisfies ProviderEvent);

      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.deepStrictEqual(
        events.map((event) => ({
          type: event.type,
          threadId: event.threadId,
          turnId: event.turnId,
        })),
        [
          { type: "turn.started", threadId, turnId },
          { type: "item.completed", threadId, turnId },
          { type: "turn.completed", threadId, turnId },
        ],
      );
    }),
  );

  it.effect("maps completed agent message items to canonical item.completed events", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      const event: ProviderEvent = {
        id: asEventId("evt-msg-complete"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/completed",
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        itemId: asItemId("msg_1"),
        payload: {
          completedAtMs: 1_778_000_000_000,
          threadId: "thread-1",
          turnId: "turn-1",
          item: {
            type: "agentMessage",
            id: "msg_1",
            text: "done",
          },
        },
      };

      yield* runtime.emit(event);
      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "item.completed");
      if (firstEvent.value.type !== "item.completed") {
        return;
      }
      NodeAssert.equal(firstEvent.value.itemId, "msg_1");
      NodeAssert.equal(firstEvent.value.turnId, "turn-1");
      NodeAssert.equal(firstEvent.value.payload.itemType, "assistant_message");
    }),
  );

  it.effect("labels MCP lifecycle entries with server and tool names", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-mcp-complete"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/completed",
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        itemId: asItemId("mcp_1"),
        payload: {
          completedAtMs: 1_778_000_000_000,
          threadId: "thread-1",
          turnId: "turn-1",
          item: {
            type: "mcpToolCall",
            id: "mcp_1",
            server: "t3-code",
            tool: "preview_status",
            arguments: {},
            durationMs: 12,
            error: null,
            result: { content: [{ type: "text", text: "attached" }] },
            status: "completed",
          },
        },
      });
      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some" || firstEvent.value.type !== "item.completed") {
        return;
      }
      NodeAssert.equal(firstEvent.value.payload.itemType, "mcp_tool_call");
      NodeAssert.equal(firstEvent.value.payload.title, "t3-code · preview_status");
      NodeAssert.deepStrictEqual(firstEvent.value.payload.data, {
        completedAtMs: 1_778_000_000_000,
        threadId: "thread-1",
        turnId: "turn-1",
        item: {
          type: "mcpToolCall",
          id: "mcp_1",
          server: "t3-code",
          tool: "preview_status",
          arguments: {},
          durationMs: 12,
          error: null,
          result: { content: [{ type: "text", text: "attached" }] },
          status: "completed",
        },
      });
    }),
  );

  it.effect("maps completed plan items to canonical proposed-plan completion events", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      const event: ProviderEvent = {
        id: asEventId("evt-plan-complete"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/completed",
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        itemId: asItemId("plan_1"),
        payload: {
          completedAtMs: 1_778_000_000_000,
          threadId: "thread-1",
          turnId: "turn-1",
          item: {
            type: "plan",
            id: "plan_1",
            text: "## Final plan\n\n- one\n- two",
          },
        },
      };

      yield* runtime.emit(event);
      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "turn.proposed.completed");
      if (firstEvent.value.type !== "turn.proposed.completed") {
        return;
      }
      NodeAssert.equal(firstEvent.value.turnId, "turn-1");
      NodeAssert.equal(firstEvent.value.payload.planMarkdown, "## Final plan\n\n- one\n- two");
    }),
  );

  it.effect("maps plan deltas to canonical proposed-plan delta events", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-plan-delta"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/plan/delta",
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        itemId: asItemId("plan_1"),
        payload: {
          threadId: "thread-1",
          turnId: "turn-1",
          itemId: "plan_1",
          delta: "## Final plan",
        },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "turn.proposed.delta");
      if (firstEvent.value.type !== "turn.proposed.delta") {
        return;
      }
      NodeAssert.equal(firstEvent.value.turnId, "turn-1");
      NodeAssert.equal(firstEvent.value.payload.delta, "## Final plan");
    }),
  );

  it.effect("maps session/closed lifecycle events to canonical session.exited runtime events", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      const event: ProviderEvent = {
        id: asEventId("evt-session-closed"),
        kind: "session",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "session/closed",
        message: "Session stopped",
      };

      yield* runtime.emit(event);
      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "session.exited");
      if (firstEvent.value.type !== "session.exited") {
        return;
      }
      NodeAssert.equal(firstEvent.value.threadId, "thread-1");
      NodeAssert.equal(firstEvent.value.payload.reason, "Session stopped");
    }),
  );

  it.effect("maps retryable Codex error notifications to runtime.warning", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-retryable-error"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "error",
        turnId: asTurnId("turn-1"),
        payload: {
          threadId: "thread-1",
          turnId: "turn-1",
          error: {
            message: "Reconnecting... 2/5",
          },
          willRetry: true,
        },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "runtime.warning");
      if (firstEvent.value.type !== "runtime.warning") {
        return;
      }
      NodeAssert.equal(firstEvent.value.turnId, "turn-1");
      NodeAssert.equal(firstEvent.value.payload.message, "Reconnecting... 2/5");
    }),
  );

  it.effect("maps process stderr notifications to runtime.warning", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-process-stderr"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "process/stderr",
        turnId: asTurnId("turn-1"),
        message: "The filename or extension is too long. (os error 206)",
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "runtime.warning");
      if (firstEvent.value.type !== "runtime.warning") {
        return;
      }
      NodeAssert.equal(firstEvent.value.turnId, "turn-1");
      NodeAssert.equal(
        firstEvent.value.payload.message,
        "The filename or extension is too long. (os error 206)",
      );
    }),
  );

  it.effect("maps realtime started notifications with upstream realtime session ids", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-realtime-started"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "thread/realtime/started",
        payload: {
          threadId: "thread-1",
          realtimeSessionId: "realtime-session-1",
          version: "v2",
        },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "thread.realtime.started");
      if (firstEvent.value.type !== "thread.realtime.started") {
        return;
      }
      NodeAssert.equal(firstEvent.value.threadId, "thread-1");
      NodeAssert.equal(firstEvent.value.payload.realtimeSessionId, "realtime-session-1");
    }),
  );

  it.effect("maps fatal websocket stderr notifications to runtime.error", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-process-stderr-websocket"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "process/stderr",
        turnId: asTurnId("turn-1"),
        message:
          "2026-03-31T18:14:06.833399Z ERROR codex_api::endpoint::responses_websocket: failed to connect to websocket: HTTP error: 503 Service Unavailable, url: wss://chatgpt.com/backend-api/codex/responses",
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "runtime.error");
      if (firstEvent.value.type !== "runtime.error") {
        return;
      }
      NodeAssert.equal(firstEvent.value.turnId, "turn-1");
      NodeAssert.equal(firstEvent.value.payload.class, "provider_error");
      NodeAssert.equal(
        firstEvent.value.payload.message,
        "2026-03-31T18:14:06.833399Z ERROR codex_api::endpoint::responses_websocket: failed to connect to websocket: HTTP error: 503 Service Unavailable, url: wss://chatgpt.com/backend-api/codex/responses",
      );
    }),
  );

  it.effect("preserves request type when mapping serverRequest/resolved", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      const event: ProviderEvent = {
        id: asEventId("evt-request-resolved"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "serverRequest/resolved",
        requestKind: "command",
        requestId: ApprovalRequestId.make("req-1"),
        payload: {
          threadId: "thread-1",
          requestId: "req-1",
        },
      };

      yield* runtime.emit(event);
      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "request.resolved");
      if (firstEvent.value.type !== "request.resolved") {
        return;
      }
      NodeAssert.equal(firstEvent.value.payload.requestType, "command_execution_approval");
    }),
  );

  it.effect("preserves file-read request type when mapping serverRequest/resolved", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      const event: ProviderEvent = {
        id: asEventId("evt-file-read-request-resolved"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "serverRequest/resolved",
        requestKind: "file-read",
        requestId: ApprovalRequestId.make("req-file-read-1"),
        payload: {
          threadId: "thread-1",
          requestId: "req-file-read-1",
        },
      };

      yield* runtime.emit(event);
      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "request.resolved");
      if (firstEvent.value.type !== "request.resolved") {
        return;
      }
      NodeAssert.equal(firstEvent.value.payload.requestType, "file_read_approval");
    }),
  );

  it.effect("preserves explicit empty multi-select user-input answers", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      const event: ProviderEvent = {
        id: asEventId("evt-user-input-empty"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/tool/requestUserInput/answered",
        payload: {
          answers: {
            scope: {
              answers: [],
            },
          },
        },
      };

      yield* runtime.emit(event);
      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "user-input.resolved");
      if (firstEvent.value.type !== "user-input.resolved") {
        return;
      }
      NodeAssert.deepEqual(firstEvent.value.payload.answers, {
        scope: [],
      });
    }),
  );

  it.effect("maps windowsSandbox/setupCompleted to session state and warning on failure", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 2)).pipe(
        Effect.forkChild,
      );

      const event: ProviderEvent = {
        id: asEventId("evt-windows-sandbox-failed"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "windowsSandbox/setupCompleted",
        message: "Sandbox setup failed",
        payload: {
          mode: "unelevated",
          success: false,
          error: "unsupported environment",
        },
      };

      yield* runtime.emit(event);
      const events = Array.from(yield* Fiber.join(eventsFiber));

      NodeAssert.equal(events.length, 2);

      const firstEvent = events[0];
      const secondEvent = events[1];

      NodeAssert.equal(firstEvent?.type, "session.state.changed");
      if (firstEvent?.type === "session.state.changed") {
        NodeAssert.equal(firstEvent.payload.state, "error");
        NodeAssert.equal(firstEvent.payload.reason, "Sandbox setup failed");
      }

      NodeAssert.equal(secondEvent?.type, "runtime.warning");
      if (secondEvent?.type === "runtime.warning") {
        NodeAssert.equal(secondEvent.payload.message, "Sandbox setup failed");
      }
    }),
  );

  it.effect(
    "maps requestUserInput requests and answered notifications to canonical user-input events",
    () =>
      Effect.gen(function* () {
        const { adapter, runtime } = yield* startLifecycleRuntime();
        const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 2)).pipe(
          Effect.forkChild,
        );

        yield* runtime.emit({
          id: asEventId("evt-user-input-requested"),
          kind: "request",
          provider: ProviderDriverKind.make("codex"),
          threadId: asThreadId("thread-1"),
          createdAt: "2026-01-01T00:00:00.000Z",
          method: "item/tool/requestUserInput",
          requestId: ApprovalRequestId.make("req-user-input-1"),
          payload: {
            itemId: "item-user-input-1",
            threadId: "thread-1",
            turnId: "turn-1",
            questions: [
              {
                id: "sandbox_mode",
                header: "Sandbox",
                question: "Which mode should be used?",
                options: [
                  {
                    label: "workspace-write",
                    description: "Allow workspace writes only",
                  },
                ],
              },
            ],
          },
        } satisfies ProviderEvent);
        yield* runtime.emit({
          id: asEventId("evt-user-input-resolved"),
          kind: "notification",
          provider: ProviderDriverKind.make("codex"),
          threadId: asThreadId("thread-1"),
          createdAt: "2026-01-01T00:00:00.000Z",
          method: "item/tool/requestUserInput/answered",
          requestId: ApprovalRequestId.make("req-user-input-1"),
          payload: {
            answers: {
              sandbox_mode: {
                answers: ["workspace-write"],
              },
            },
          },
        } satisfies ProviderEvent);

        const events = Array.from(yield* Fiber.join(eventsFiber));
        NodeAssert.equal(events[0]?.type, "user-input.requested");
        if (events[0]?.type === "user-input.requested") {
          NodeAssert.equal(events[0].requestId, "req-user-input-1");
          NodeAssert.equal(events[0].payload.questions[0]?.id, "sandbox_mode");
          NodeAssert.equal(events[0].payload.questions[0]?.multiSelect, false);
        }

        NodeAssert.equal(events[1]?.type, "user-input.resolved");
        if (events[1]?.type === "user-input.resolved") {
          NodeAssert.equal(events[1].requestId, "req-user-input-1");
          NodeAssert.deepEqual(events[1].payload.answers, {
            sandbox_mode: "workspace-write",
          });
        }
      }),
  );

  it.effect("unwraps Codex token usage payloads for context window events", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-codex-thread-token-usage-updated"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "thread/tokenUsage/updated",
        payload: {
          threadId: "thread-1",
          turnId: "turn-1",
          tokenUsage: {
            total: {
              inputTokens: 11_833,
              cachedInputTokens: 3456,
              outputTokens: 6,
              reasoningOutputTokens: 0,
              totalTokens: 11_839,
            },
            last: {
              inputTokens: 120,
              cachedInputTokens: 0,
              outputTokens: 6,
              reasoningOutputTokens: 0,
              totalTokens: 126,
            },
            modelContextWindow: 258_400,
          },
        },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);
      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "thread.token-usage.updated");
      if (firstEvent.value.type !== "thread.token-usage.updated") {
        return;
      }

      NodeAssert.deepEqual(firstEvent.value.payload.usage, {
        usedTokens: 126,
        totalProcessedTokens: 11_839,
        maxTokens: 258_400,
        inputTokens: 120,
        cachedInputTokens: 0,
        outputTokens: 6,
        reasoningOutputTokens: 0,
        lastUsedTokens: 126,
        lastInputTokens: 120,
        lastCachedInputTokens: 0,
        lastOutputTokens: 6,
        lastReasoningOutputTokens: 0,
        compactsAutomatically: true,
      });
    }),
  );
});

const scopedLifecycleRuntimeFactory = makeScopedRuntimeFactory();
const scopedLifecycleLayer = it.layer(
  Layer.effect(
    CodexAdapter,
    Effect.gen(function* () {
      const codexConfig = decodeCodexSettings({});
      return yield* makeCodexAdapter(codexConfig, {
        makeRuntime: scopedLifecycleRuntimeFactory.factory,
      });
    }),
  ).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  ),
);

scopedLifecycleLayer("CodexAdapterLive scoped lifecycle", (it) => {
  it.effect("closes the externally owned session scope on stopSession", () =>
    Effect.gen(function* () {
      scopedLifecycleRuntimeFactory.releasedThreadIds.length = 0;
      const adapter = yield* CodexAdapter;

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-stop"),
        runtimeMode: "full-access",
      });

      const runtime = scopedLifecycleRuntimeFactory.lastRuntime;
      NodeAssert.ok(runtime);

      yield* adapter.stopSession(asThreadId("thread-stop"));

      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 1);
      NodeAssert.deepStrictEqual(scopedLifecycleRuntimeFactory.releasedThreadIds, [
        asThreadId("thread-stop"),
      ]);
      NodeAssert.equal(yield* adapter.hasSession(asThreadId("thread-stop")), false);
    }),
  );
});

const scopedFailureRuntimeFactory = makeScopedRuntimeFactory({ failConstruction: true });
const scopedFailureLayer = it.layer(
  Layer.effect(
    CodexAdapter,
    Effect.gen(function* () {
      const codexConfig = decodeCodexSettings({});
      return yield* makeCodexAdapter(codexConfig, {
        makeRuntime: scopedFailureRuntimeFactory.factory,
      });
    }),
  ).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  ),
);

scopedFailureLayer("CodexAdapterLive scoped startup failure", (it) => {
  it.effect("closes the externally owned session scope when startSession fails", () =>
    Effect.gen(function* () {
      scopedFailureRuntimeFactory.releasedThreadIds.length = 0;
      const adapter = yield* CodexAdapter;

      const result = yield* adapter
        .startSession({
          provider: ProviderDriverKind.make("codex"),
          threadId: asThreadId("thread-fail"),
          runtimeMode: "full-access",
        })
        .pipe(Effect.result);

      NodeAssert.equal(result._tag, "Failure");
      NodeAssert.equal(result.failure._tag, "ProviderAdapterProcessError");
      NodeAssert.deepStrictEqual(scopedFailureRuntimeFactory.releasedThreadIds, [
        asThreadId("thread-fail"),
      ]);
      NodeAssert.equal(yield* adapter.hasSession(asThreadId("thread-fail")), false);
    }),
  );
});

it.effect("single-flights compatible starts for the same thread", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const startEntered = yield* Deferred.make<void>();
      const releaseStart = yield* Deferred.make<void>();
      const runtimes: FakeCodexRuntime[] = [];
      const factory: TestRuntimeFactory = (options) =>
        Effect.sync(() => {
          const runtime = new FakeCodexRuntime(options);
          runtime.startOverride = () =>
            Deferred.succeed(startEntered, undefined).pipe(
              Effect.andThen(Deferred.await(releaseStart)),
              Effect.as(runtime.makeSession()),
            );
          runtimes.push(runtime);
          return runtime;
        });
      const adapterScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(adapterScope, Exit.void));
      const adapter = yield* buildAdapterWithScope(factory, adapterScope);
      const input = {
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-compatible-single-flight"),
        cwd: "/workspace/compatible",
        modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.4-mini"),
        runtimeMode: "full-access" as const,
        detached: true,
      };

      const firstFiber = yield* adapter.startSession(input).pipe(Effect.forkChild);
      yield* Deferred.await(startEntered);
      const secondFiber = yield* adapter.startSession(input).pipe(Effect.forkChild);
      yield* Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
        discard: true,
      });

      NodeAssert.equal(runtimes.length, 1);
      yield* Deferred.succeed(releaseStart, undefined);
      const first = yield* Fiber.join(firstFiber);
      const second = yield* Fiber.join(secondFiber);

      NodeAssert.strictEqual(first, second);
      NodeAssert.equal(runtimes.length, 1);
      NodeAssert.equal(yield* adapter.hasSession(input.threadId), true);
    }),
  ),
);

it.effect("keeps a shared start alive when its original caller is interrupted", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const startEntered = yield* Deferred.make<void>();
      const releaseStart = yield* Deferred.make<void>();
      let runtime: FakeCodexRuntime | undefined;
      const factory: TestRuntimeFactory = (options) =>
        Effect.sync(() => {
          runtime = new FakeCodexRuntime(options);
          runtime.startOverride = () =>
            Deferred.succeed(startEntered, undefined).pipe(
              Effect.andThen(Deferred.await(releaseStart)),
              Effect.as(runtime!.makeSession()),
            );
          return runtime;
        });
      const adapterScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(adapterScope, Exit.void));
      const adapter = yield* buildAdapterWithScope(factory, adapterScope);
      const input = {
        threadId: asThreadId("thread-shared-start-caller-cancelled"),
        runtimeMode: "full-access" as const,
      };

      const firstFiber = yield* adapter.startSession(input).pipe(Effect.forkChild);
      yield* Deferred.await(startEntered);
      const joinedFiber = yield* adapter.startSession(input).pipe(Effect.forkChild);
      yield* Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
        discard: true,
      });

      yield* Fiber.interrupt(firstFiber);
      NodeAssert.ok(runtime);
      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 0);
      yield* Deferred.succeed(releaseStart, undefined);
      yield* Fiber.join(joinedFiber);

      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 0);
      NodeAssert.equal(yield* adapter.hasSession(input.threadId), true);
    }),
  ),
);

it.effect("serializes incompatible same-thread starts until the old scope is closed", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const firstStartEntered = yield* Deferred.make<void>();
      const releaseFirstStart = yield* Deferred.make<void>();
      const runtimes: FakeCodexRuntime[] = [];
      const released: ThreadId[] = [];
      let liveRuntimes = 0;
      let maxLiveRuntimes = 0;
      const factory: TestRuntimeFactory = (options) =>
        Effect.gen(function* () {
          yield* Scope.Scope;
          const runtime = new FakeCodexRuntime(options);
          const index = runtimes.length;
          runtimes.push(runtime);
          liveRuntimes += 1;
          maxLiveRuntimes = Math.max(maxLiveRuntimes, liveRuntimes);
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              liveRuntimes -= 1;
              released.push(options.threadId);
            }),
          );
          if (index === 0) {
            runtime.startOverride = () =>
              Deferred.succeed(firstStartEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releaseFirstStart)),
                Effect.as(runtime.makeSession()),
              );
          }
          return runtime;
        });
      const adapterScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(adapterScope, Exit.void));
      const adapter = yield* buildAdapterWithScope(factory, adapterScope);
      const threadId = asThreadId("thread-incompatible-serialization");
      const baseInput = {
        provider: ProviderDriverKind.make("codex"),
        threadId,
        cwd: "/workspace/serialized",
        runtimeMode: "full-access" as const,
        detached: true,
      };

      const firstFiber = yield* adapter
        .startSession({
          ...baseInput,
          modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "model-a"),
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(firstStartEntered);
      const secondFiber = yield* adapter
        .startSession({
          ...baseInput,
          modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "model-b"),
        })
        .pipe(Effect.forkChild);
      yield* Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
        discard: true,
      });

      NodeAssert.equal(runtimes.length, 1);
      NodeAssert.equal(liveRuntimes, 1);
      const replacementExitFiber = yield* Stream.runHead(adapter.streamEvents).pipe(
        Effect.forkChild,
      );
      yield* Deferred.succeed(releaseFirstStart, undefined);
      yield* Fiber.join(firstFiber);
      const second = yield* Fiber.join(secondFiber);

      NodeAssert.equal(second.model, "model-b");
      NodeAssert.equal(runtimes.length, 2);
      NodeAssert.equal(maxLiveRuntimes, 1);
      NodeAssert.deepStrictEqual(released, [threadId]);
      NodeAssert.equal(runtimes[0]?.closeImpl.mock.calls.length, 1);
      yield* Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
        discard: true,
      });
      NodeAssert.equal(replacementExitFiber.pollUnsafe(), undefined);
      yield* Fiber.interrupt(replacementExitFiber);
    }),
  ),
);

it.effect("finishes replacement teardown when the replacement start is cancelled", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const closeEntered = yield* Deferred.make<void>();
      const releaseClose = yield* Deferred.make<void>();
      const released: ThreadId[] = [];
      const runtimes: FakeCodexRuntime[] = [];
      const factory: TestRuntimeFactory = (options) =>
        Effect.gen(function* () {
          yield* Scope.Scope;
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              released.push(options.threadId);
            }),
          );
          const runtime = new FakeCodexRuntime(options);
          runtimes.push(runtime);
          return runtime;
        });
      const adapterScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(adapterScope, Exit.void));
      const adapter = yield* buildAdapterWithScope(factory, adapterScope);
      const threadId = asThreadId("thread-cancelled-replacement-close");
      const baseInput = { threadId, runtimeMode: "full-access" as const };

      yield* adapter.startSession({
        ...baseInput,
        modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "model-a"),
      });
      const oldRuntime = runtimes[0];
      NodeAssert.ok(oldRuntime);
      oldRuntime.closeOverride = () =>
        Effect.promise(() => oldRuntime.closeImpl()).pipe(
          Effect.andThen(Deferred.succeed(closeEntered, undefined)),
          Effect.andThen(Deferred.await(releaseClose)),
        );
      const exitEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);
      const replacementFiber = yield* adapter
        .startSession({
          ...baseInput,
          modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "model-b"),
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(closeEntered);

      const interruptFiber = yield* Fiber.interrupt(replacementFiber).pipe(Effect.forkChild);
      yield* Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
        discard: true,
      });
      NodeAssert.equal(runtimes.length, 1);
      NodeAssert.deepStrictEqual(released, []);
      yield* Deferred.succeed(releaseClose, undefined);
      yield* Fiber.join(interruptFiber);
      const exitEvent = yield* Fiber.join(exitEventFiber);

      NodeAssert.equal(oldRuntime.closeImpl.mock.calls.length, 1);
      NodeAssert.equal(exitEvent._tag, "Some");
      if (exitEvent._tag === "Some") {
        NodeAssert.equal(exitEvent.value.type, "session.exited");
      }
      NodeAssert.deepStrictEqual(released, [threadId]);
      NodeAssert.equal(runtimes.length, 1);
      NodeAssert.equal(yield* adapter.hasSession(threadId), false);
    }),
  ),
);

it.effect("emits the old session exit when replacement construction fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const runtimes: FakeCodexRuntime[] = [];
      const factory: TestRuntimeFactory = (options) => {
        if (runtimes.length > 0) {
          return Effect.fail(
            new CodexErrors.CodexAppServerSpawnError({
              command: `${options.binaryPath} app-server`,
              cause: new Error("replacement construction failed"),
            }),
          );
        }
        return Effect.sync(() => {
          const runtime = new FakeCodexRuntime(options);
          runtimes.push(runtime);
          return runtime;
        });
      };
      const adapterScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(adapterScope, Exit.void));
      const adapter = yield* buildAdapterWithScope(factory, adapterScope);
      const threadId = asThreadId("thread-failed-replacement-exit");
      const baseInput = { threadId, runtimeMode: "full-access" as const };
      yield* adapter.startSession({
        ...baseInput,
        modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "model-a"),
      });
      const oldRuntime = runtimes[0];
      NodeAssert.ok(oldRuntime);
      const exitEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      const replacementExit = yield* adapter
        .startSession({
          ...baseInput,
          modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "model-b"),
        })
        .pipe(Effect.exit);
      const exitEvent = yield* Fiber.join(exitEventFiber);

      NodeAssert.equal(Exit.isFailure(replacementExit), true);
      NodeAssert.equal(exitEvent._tag, "Some");
      if (exitEvent._tag === "Some") {
        NodeAssert.equal(exitEvent.value.type, "session.exited");
        NodeAssert.equal(exitEvent.value.threadId, threadId);
      }
      NodeAssert.equal(oldRuntime.closeImpl.mock.calls.length, 1);
      NodeAssert.equal(yield* adapter.hasSession(threadId), false);
    }),
  ),
);

for (const scenario of [
  {
    name: "resume cursors",
    first: { resumeCursor: { threadId: "provider-thread-a" } },
    second: { resumeCursor: { threadId: "provider-thread-b" } },
  },
  {
    name: "service tiers",
    first: {
      modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "model-a", [
        { id: "serviceTier", value: "priority" },
      ]),
    },
    second: {
      modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "model-a", [
        { id: "serviceTier", value: "flex" },
      ]),
    },
  },
] as const) {
  it.effect(`serializes same-thread starts with different ${scenario.name}`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const firstStartEntered = yield* Deferred.make<void>();
        const releaseFirstStart = yield* Deferred.make<void>();
        const runtimes: FakeCodexRuntime[] = [];
        const factory: TestRuntimeFactory = (options) =>
          Effect.sync(() => {
            const runtime = new FakeCodexRuntime(options);
            if (runtimes.length === 0) {
              runtime.startOverride = () =>
                Deferred.succeed(firstStartEntered, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseFirstStart)),
                  Effect.as(runtime.makeSession()),
                );
            }
            runtimes.push(runtime);
            return runtime;
          });
        const adapterScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(adapterScope, Exit.void));
        const adapter = yield* buildAdapterWithScope(factory, adapterScope);
        const baseInput = {
          threadId: asThreadId(`thread-incompatible-${scenario.name.replace(" ", "-")}`),
          runtimeMode: "full-access" as const,
        };

        const firstFiber = yield* adapter
          .startSession({ ...baseInput, ...scenario.first })
          .pipe(Effect.forkChild);
        yield* Deferred.await(firstStartEntered);
        const secondFiber = yield* adapter
          .startSession({ ...baseInput, ...scenario.second })
          .pipe(Effect.forkChild);
        yield* Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
          discard: true,
        });

        NodeAssert.equal(runtimes.length, 1);
        yield* Deferred.succeed(releaseFirstStart, undefined);
        yield* Fiber.join(firstFiber);
        yield* Fiber.join(secondFiber);

        NodeAssert.equal(runtimes.length, 2);
        NodeAssert.equal(runtimes[0]?.closeImpl.mock.calls.length, 1);
      }),
    ),
  );
}

it.effect("serializes same-thread starts from different MCP provider sessions", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const firstStartEntered = yield* Deferred.make<void>();
      const releaseFirstStart = yield* Deferred.make<void>();
      const runtimes: FakeCodexRuntime[] = [];
      const factory: TestRuntimeFactory = (options) =>
        Effect.sync(() => {
          const runtime = new FakeCodexRuntime(options);
          if (runtimes.length === 0) {
            runtime.startOverride = () =>
              Deferred.succeed(firstStartEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releaseFirstStart)),
                Effect.as(runtime.makeSession()),
              );
          }
          runtimes.push(runtime);
          return runtime;
        });
      const adapterScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(adapterScope, Exit.void));
      const adapter = yield* buildAdapterWithScope(factory, adapterScope);
      const threadId = asThreadId("thread-incompatible-mcp-session");
      const firstMcpSession: McpProviderSession.McpProviderSessionConfig = {
        environmentId: EnvironmentId.make("environment-mcp-a"),
        threadId,
        providerSessionId: "provider-session-mcp-a",
        providerInstanceId: ProviderInstanceId.make("codex"),
        endpoint: "http://127.0.0.1:3773/mcp-a",
        authorizationHeader: "Bearer mcp-token-a",
      };
      const secondMcpSession: McpProviderSession.McpProviderSessionConfig = {
        ...firstMcpSession,
        environmentId: EnvironmentId.make("environment-mcp-b"),
        providerSessionId: "provider-session-mcp-b",
        endpoint: "http://127.0.0.1:3773/mcp-b",
        authorizationHeader: "Bearer mcp-token-b",
      };
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => McpProviderSession.clearMcpProviderSession(threadId)),
      );
      const input = { threadId, runtimeMode: "full-access" as const };

      McpProviderSession.setMcpProviderSession(firstMcpSession);
      const firstFiber = yield* adapter.startSession(input).pipe(Effect.forkChild);
      yield* Deferred.await(firstStartEntered);
      McpProviderSession.setMcpProviderSession(secondMcpSession);
      const secondFiber = yield* adapter.startSession(input).pipe(Effect.forkChild);
      yield* Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
        discard: true,
      });

      NodeAssert.equal(runtimes.length, 1);
      yield* Deferred.succeed(releaseFirstStart, undefined);
      yield* Fiber.join(firstFiber);
      yield* Fiber.join(secondFiber);

      NodeAssert.equal(runtimes.length, 2);
      NodeAssert.deepStrictEqual(
        runtimes[0]?.options.appServerArgs,
        makeCodexMcpRuntimeConfig(firstMcpSession, process.env).appServerArgs,
      );
      NodeAssert.deepStrictEqual(
        runtimes[1]?.options.appServerArgs,
        makeCodexMcpRuntimeConfig(secondMcpSession, process.env).appServerArgs,
      );
    }),
  ),
);

it.effect("starts different thread ids concurrently", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const releaseStarts = yield* Deferred.make<void>();
      const enteredByThread = new Map<ThreadId, Deferred.Deferred<void>>();
      const runtimes: FakeCodexRuntime[] = [];
      const factory: TestRuntimeFactory = (options) =>
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          enteredByThread.set(options.threadId, entered);
          const runtime = new FakeCodexRuntime(options);
          runtime.startOverride = () =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(releaseStarts)),
              Effect.as(runtime.makeSession()),
            );
          runtimes.push(runtime);
          return runtime;
        });
      const adapterScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(adapterScope, Exit.void));
      const adapter = yield* buildAdapterWithScope(factory, adapterScope);
      const firstThreadId = asThreadId("thread-parallel-a");
      const secondThreadId = asThreadId("thread-parallel-b");

      const firstFiber = yield* adapter
        .startSession({ threadId: firstThreadId, runtimeMode: "full-access" })
        .pipe(Effect.forkChild);
      while (!enteredByThread.has(firstThreadId)) yield* Effect.yieldNow;
      yield* Deferred.await(enteredByThread.get(firstThreadId)!);
      const secondFiber = yield* adapter
        .startSession({ threadId: secondThreadId, runtimeMode: "full-access" })
        .pipe(Effect.forkChild);
      while (!enteredByThread.has(secondThreadId)) yield* Effect.yieldNow;
      yield* Deferred.await(enteredByThread.get(secondThreadId)!);

      NodeAssert.equal(runtimes.length, 2);
      yield* Deferred.succeed(releaseStarts, undefined);
      yield* Fiber.join(firstFiber);
      yield* Fiber.join(secondFiber);
    }),
  ),
);

it.effect("closes an interrupted in-flight start scope", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const startEntered = yield* Deferred.make<void>();
      const neverRelease = yield* Deferred.make<void>();
      const released: ThreadId[] = [];
      let runtime: FakeCodexRuntime | undefined;
      const factory: TestRuntimeFactory = (options) =>
        Effect.gen(function* () {
          yield* Scope.Scope;
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              released.push(options.threadId);
            }),
          );
          runtime = new FakeCodexRuntime(options);
          runtime.startOverride = () =>
            Deferred.succeed(startEntered, undefined).pipe(
              Effect.andThen(Deferred.await(neverRelease)),
              Effect.as(runtime!.makeSession()),
            );
          return runtime;
        });
      const adapterScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(adapterScope, Exit.void));
      const adapter = yield* buildAdapterWithScope(factory, adapterScope);
      const threadId = asThreadId("thread-interrupted-start");
      const startFiber = yield* adapter
        .startSession({ threadId, runtimeMode: "full-access" })
        .pipe(Effect.forkChild);
      yield* Deferred.await(startEntered);

      yield* Fiber.interrupt(startFiber);

      NodeAssert.ok(runtime);
      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 1);
      NodeAssert.deepStrictEqual(released, [threadId]);
      NodeAssert.equal(yield* adapter.hasSession(threadId), false);
    }),
  ),
);

for (const stopOperation of ["stopSession", "stopAll"] as const) {
  it.effect(`${stopOperation} cancels and drains an in-flight start`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const startEntered = yield* Deferred.make<void>();
        const neverRelease = yield* Deferred.make<void>();
        const released: ThreadId[] = [];
        let runtime: FakeCodexRuntime | undefined;
        const factory: TestRuntimeFactory = (options) =>
          Effect.gen(function* () {
            yield* Scope.Scope;
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                released.push(options.threadId);
              }),
            );
            runtime = new FakeCodexRuntime(options);
            runtime.startOverride = () =>
              Deferred.succeed(startEntered, undefined).pipe(
                Effect.andThen(Deferred.await(neverRelease)),
                Effect.as(runtime!.makeSession()),
              );
            return runtime;
          });
        const adapterScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(adapterScope, Exit.void));
        const adapter = yield* buildAdapterWithScope(factory, adapterScope);
        const threadId = asThreadId(`thread-${stopOperation}-during-start`);
        const startFiber = yield* adapter
          .startSession({ threadId, runtimeMode: "full-access" })
          .pipe(Effect.forkChild);
        yield* Deferred.await(startEntered);

        if (stopOperation === "stopSession") {
          yield* adapter.stopSession(threadId);
        } else {
          yield* adapter.stopAll();
        }
        const startExit = yield* Fiber.await(startFiber);

        NodeAssert.equal(Exit.isFailure(startExit), true);
        NodeAssert.ok(runtime);
        NodeAssert.equal(runtime.closeImpl.mock.calls.length, 1);
        NodeAssert.deepStrictEqual(released, [threadId]);
        NodeAssert.equal(yield* adapter.hasSession(threadId), false);
      }),
    ),
  );
}

it.effect("stopSession cancels an incompatible start waiting behind another start", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const startEntered = yield* Deferred.make<void>();
      const neverRelease = yield* Deferred.make<void>();
      const runtimes: FakeCodexRuntime[] = [];
      const factory: TestRuntimeFactory = (options) =>
        Effect.sync(() => {
          const runtime = new FakeCodexRuntime(options);
          runtime.startOverride = () =>
            Deferred.succeed(startEntered, undefined).pipe(
              Effect.andThen(Deferred.await(neverRelease)),
              Effect.as(runtime.makeSession()),
            );
          runtimes.push(runtime);
          return runtime;
        });
      const adapterScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(adapterScope, Exit.void));
      const adapter = yield* buildAdapterWithScope(factory, adapterScope);
      const threadId = asThreadId("thread-stop-cancels-serialized-start");
      const baseInput = { threadId, runtimeMode: "full-access" as const };
      const firstFiber = yield* adapter
        .startSession({
          ...baseInput,
          modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "model-a"),
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(startEntered);
      const waitingFiber = yield* adapter
        .startSession({
          ...baseInput,
          modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "model-b"),
        })
        .pipe(Effect.forkChild);
      yield* Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
        discard: true,
      });

      yield* adapter.stopSession(threadId);
      const firstExit = yield* Fiber.await(firstFiber);
      const waitingExit = yield* Fiber.await(waitingFiber);

      NodeAssert.equal(Exit.isFailure(firstExit), true);
      NodeAssert.equal(Exit.isFailure(waitingExit), true);
      NodeAssert.equal(runtimes.length, 1);
      NodeAssert.equal(yield* adapter.hasSession(threadId), false);
    }),
  ),
);

it.effect("stale cleanup cannot remove or close a successor session", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const oldCloseEntered = yield* Deferred.make<void>();
      const releaseOldClose = yield* Deferred.make<void>();
      const runtimes: FakeCodexRuntime[] = [];
      const factory: TestRuntimeFactory = (options) =>
        Effect.sync(() => {
          const runtime = new FakeCodexRuntime(options);
          runtimes.push(runtime);
          return runtime;
        });
      const adapterScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(adapterScope, Exit.void));
      const adapter = yield* buildAdapterWithScope(factory, adapterScope);
      const threadId = asThreadId("thread-stale-cleanup");
      const input = { threadId, runtimeMode: "full-access" as const };
      const stale = { generation: 1 };
      const current = { generation: 2 };
      const sessionRegistry = new Map([[threadId, current]]);

      NodeAssert.equal(deleteSessionIfCurrent(sessionRegistry, threadId, stale), false);
      NodeAssert.strictEqual(sessionRegistry.get(threadId), current);

      yield* adapter.startSession(input);
      const oldRuntime = runtimes[0];
      NodeAssert.ok(oldRuntime);
      oldRuntime.closeOverride = () =>
        Deferred.succeed(oldCloseEntered, undefined).pipe(
          Effect.andThen(Deferred.await(releaseOldClose)),
        );
      const staleStopFiber = yield* adapter.stopSession(threadId).pipe(Effect.forkChild);
      yield* Deferred.await(oldCloseEntered);

      const successorFiber = yield* adapter.startSession(input).pipe(Effect.forkChild);
      yield* Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
        discard: true,
      });
      NodeAssert.equal(runtimes.length, 1);
      yield* Deferred.succeed(releaseOldClose, undefined);
      yield* Fiber.join(staleStopFiber);
      const successor = yield* Fiber.join(successorFiber);
      const successorRuntime = runtimes[1];
      NodeAssert.ok(successorRuntime);

      NodeAssert.equal(successor.model, undefined);
      NodeAssert.equal(yield* adapter.hasSession(threadId), true);
      NodeAssert.equal(successorRuntime.closeImpl.mock.calls.length, 0);
      yield* adapter.sendTurn({ threadId, input: "still alive" });
      NodeAssert.equal(successorRuntime.sendTurnImpl.mock.calls.length, 1);
    }),
  ),
);

it.effect("does not forward a stale session-closed event while starting a successor", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const oldCloseEntered = yield* Deferred.make<void>();
      const releaseOldClose = yield* Deferred.make<void>();
      const runtimes: FakeCodexRuntime[] = [];
      const factory: TestRuntimeFactory = (options) =>
        Effect.sync(() => {
          const runtime = new FakeCodexRuntime(options);
          runtimes.push(runtime);
          return runtime;
        });
      const adapterScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(adapterScope, Exit.void));
      const adapter = yield* buildAdapterWithScope(factory, adapterScope);
      const threadId = asThreadId("thread-stale-close-event");
      const input = { threadId, runtimeMode: "full-access" as const };

      yield* adapter.startSession(input);
      const oldRuntime = runtimes[0];
      NodeAssert.ok(oldRuntime);
      oldRuntime.closeOverride = () =>
        oldRuntime
          .emit({
            id: asEventId("evt-stale-session-closed"),
            kind: "session",
            provider: ProviderDriverKind.make("codex"),
            threadId,
            createdAt: "2026-01-01T00:00:00.000Z",
            method: "session/closed",
            message: "Old session stopped",
          })
          .pipe(
            Effect.andThen(Deferred.succeed(oldCloseEntered, undefined)),
            Effect.andThen(Deferred.await(releaseOldClose)),
          );
      const forwardedEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(
        Effect.forkChild,
      );
      const staleStopFiber = yield* adapter.stopSession(threadId).pipe(Effect.forkChild);
      yield* Deferred.await(oldCloseEntered);
      const successorFiber = yield* adapter.startSession(input).pipe(Effect.forkChild);
      yield* Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
        discard: true,
      });

      NodeAssert.equal(runtimes.length, 1);
      yield* Deferred.succeed(releaseOldClose, undefined);
      yield* Fiber.join(staleStopFiber);
      yield* Fiber.join(successorFiber);
      const forwardedEvent = yield* Fiber.join(forwardedEventFiber);
      NodeAssert.equal(forwardedEvent._tag, "Some");
      if (forwardedEvent._tag === "Some") {
        NodeAssert.equal(forwardedEvent.value.type, "session.exited");
        NodeAssert.equal(forwardedEvent.value.threadId, threadId);
      }
      const duplicateEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(
        Effect.forkChild,
      );
      yield* Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
        discard: true,
      });

      NodeAssert.equal(duplicateEventFiber.pollUnsafe(), undefined);
      yield* Fiber.interrupt(duplicateEventFiber);
      NodeAssert.equal(yield* adapter.hasSession(threadId), true);
    }),
  ),
);

it.effect("does not duplicate a session exit already forwarded by the runtime", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const runtimeFactory = makeRuntimeFactory();
      const adapterScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(adapterScope, Exit.void));
      const adapter = yield* buildAdapterWithScope(runtimeFactory.factory, adapterScope);
      const threadId = asThreadId("thread-runtime-exit-deduplication");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      const runtime = runtimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      const runtimeExitFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-runtime-session-closed"),
        kind: "session",
        provider: ProviderDriverKind.make("codex"),
        threadId,
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "session/closed",
        message: "Runtime session stopped",
      });
      const runtimeExit = yield* Fiber.join(runtimeExitFiber);
      NodeAssert.equal(runtimeExit._tag, "Some");
      if (runtimeExit._tag === "Some") {
        NodeAssert.equal(runtimeExit.value.type, "session.exited");
      }
      const duplicateExitFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* adapter.stopSession(threadId);
      yield* Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
        discard: true,
      });

      NodeAssert.equal(duplicateExitFiber.pollUnsafe(), undefined);
      yield* Fiber.interrupt(duplicateExitFiber);
    }),
  ),
);

it.effect("adapter release cancels and drains an in-flight start", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const startEntered = yield* Deferred.make<void>();
      const neverRelease = yield* Deferred.make<void>();
      const released: ThreadId[] = [];
      let runtime: FakeCodexRuntime | undefined;
      const factory: TestRuntimeFactory = (options) =>
        Effect.gen(function* () {
          yield* Scope.Scope;
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              released.push(options.threadId);
            }),
          );
          runtime = new FakeCodexRuntime(options);
          runtime.startOverride = () =>
            Deferred.succeed(startEntered, undefined).pipe(
              Effect.andThen(Deferred.await(neverRelease)),
              Effect.as(runtime!.makeSession()),
            );
          return runtime;
        });
      const adapterScope = yield* Scope.make("sequential");
      const adapter = yield* buildAdapterWithScope(factory, adapterScope);
      const threadId = asThreadId("thread-adapter-release-start");
      const startFiber = yield* adapter
        .startSession({ threadId, runtimeMode: "full-access" })
        .pipe(Effect.forkChild);
      yield* Deferred.await(startEntered);

      yield* Scope.close(adapterScope, Exit.void);
      yield* Fiber.await(startFiber);

      NodeAssert.ok(runtime);
      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 1);
      NodeAssert.deepStrictEqual(released, [threadId]);
    }),
  ),
);

it.effect("flushes managed native logs when the adapter layer shuts down", () =>
  Effect.gen(function* () {
    const tempDir = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "t3-codex-adapter-native-log-"),
    );
    const basePath = NodePath.join(tempDir, "provider-native.ndjson");
    const runtimeFactory = makeRuntimeFactory();
    const scope = yield* Scope.make("sequential");
    let scopeClosed = false;

    try {
      const layer = Layer.effect(
        CodexAdapter,
        Effect.gen(function* () {
          const codexConfig = decodeCodexSettings({});
          return yield* makeCodexAdapter(codexConfig, {
            makeRuntime: runtimeFactory.factory,
            nativeEventLogPath: basePath,
          });
        }),
      ).pipe(
        Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
        Layer.provideMerge(ServerSettingsService.layerTest()),
        Layer.provideMerge(providerSessionDirectoryTestLayer),
        Layer.provideMerge(NodeServices.layer),
      );
      const context = yield* Layer.buildWithScope(layer, scope);
      const adapter = yield* Effect.service(CodexAdapter).pipe(Effect.provide(context));

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-logger"),
        runtimeMode: "full-access",
      });

      const runtime = runtimeFactory.lastRuntime;
      NodeAssert.ok(runtime);

      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);
      yield* runtime.emit({
        id: asEventId("evt-native-log"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-logger"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "process/stderr",
        message: "native flush test",
      } satisfies ProviderEvent);
      yield* Fiber.join(firstEventFiber);

      yield* Scope.close(scope, Exit.void);
      scopeClosed = true;

      const threadLogPath = NodePath.join(tempDir, "thread-logger.log");
      NodeAssert.equal(NodeFS.existsSync(threadLogPath), true);
      const contents = NodeFS.readFileSync(threadLogPath, "utf8");
      NodeAssert.match(contents, /NTIVE: .*"message":"native flush test"/);
    } finally {
      if (!scopeClosed) {
        yield* Scope.close(scope, Exit.void);
      }
      NodeFS.rmSync(tempDir, { recursive: true, force: true });
    }
  }),
);
