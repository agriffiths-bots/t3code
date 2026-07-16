import {
  CommandId,
  EnvironmentId,
  MessageId,
  ORCHESTRATION_WS_METHODS,
  ThreadId,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "../connection/model.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import type * as RpcSession from "../rpc/session.ts";
import {
  ThreadReconciliationActivity,
  type ThreadReconciliationActivityEvent,
} from "./threadReconciliationActivity.ts";
import { startThreadTurnWithReconciliation } from "./threadCommands.ts";

const TARGET = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("environment-thread-command-test"),
  label: "Thread command test",
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
});
const TEST_CRYPTO_LAYER = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) => new Uint8Array(size),
    digest: (_algorithm, data) => Effect.succeed(data),
  }),
);

it.effect("publishes local turn activity before an ambiguously failed dispatch", () =>
  Effect.gen(function* () {
    const order: string[] = [];
    const dispatchFailure = new Error("response lost after server acceptance");
    const client = {
      [ORCHESTRATION_WS_METHODS.dispatchCommand]: () =>
        Effect.sync(() => order.push("dispatch")).pipe(
          Effect.andThen(Effect.fail(dispatchFailure)),
        ),
    } as unknown as WsRpcProtocolClient;
    const session: RpcSession.RpcSession = {
      client,
      initialConfig: Effect.never,
      ready: Effect.void,
      probe: Effect.void,
      closed: Effect.never,
    };
    const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
      target: TARGET,
      state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
      session: yield* SubscriptionRef.make(Option.some(session)),
      prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
      connect: Effect.void,
      disconnect: Effect.void,
      retryNow: Effect.void,
    } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
    const activity = ThreadReconciliationActivity.of({
      publish: (_event: ThreadReconciliationActivityEvent) =>
        Effect.sync(() => order.push("activity")).pipe(Effect.asVoid),
      events: Stream.empty,
    });

    const failure = yield* startThreadTurnWithReconciliation({
      commandId: CommandId.make("command-turn-response-lost"),
      threadId: ThreadId.make("thread-turn-response-lost"),
      message: {
        messageId: MessageId.make("message-turn-response-lost"),
        role: "user",
        text: "Start despite an ambiguous response.",
        attachments: [],
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      createdAt: "2026-07-16T00:00:00.000Z",
    }).pipe(
      Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
      Effect.provideService(ThreadReconciliationActivity, activity),
      Effect.flip,
    );

    expect(failure).toBe(dispatchFailure);
    expect(order).toEqual(["activity", "dispatch"]);
  }).pipe(Effect.provide(TEST_CRYPTO_LAYER)),
);
