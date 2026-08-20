import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EnvironmentId,
  EventId,
  MessageId,
  NonNegativeInt,
  ProjectId,
  ProviderDriverKind,
  OrchestrationDispatchCommandError,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type MatrixBridgeStatus,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import type { TestTurnResponse } from "./TestProviderAdapter.integration.ts";
import {
  makeOrchestrationIntegrationHarness,
  type OrchestrationIntegrationHarness,
} from "./OrchestrationEngineHarness.integration.ts";
import { EnvironmentAuth } from "../src/auth/EnvironmentAuth.ts";
import * as ServerEnvironment from "../src/environment/ServerEnvironment.ts";
import {
  makeFakeMatrixBridgeClient,
  type FakeMatrixBridgeClient,
} from "../src/matrix/MatrixBridgeClient.ts";
import {
  MatrixBridgeReactor,
  layer as MatrixBridgeReactorLive,
} from "../src/matrix/MatrixBridgeReactor.ts";
import { MatrixBridgeConfig, type MatrixBridgeConfigV1 } from "../src/matrix/MatrixBridgeConfig.ts";
import { trustedSystemDispatchAuthority } from "../src/orchestration/commandAudienceGuard.ts";
import { BootstrapTurnStartDispatcher } from "../src/orchestration/Services/BootstrapTurnStartDispatcher.ts";
import { OrchestrationEngineService } from "../src/orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../src/orchestration/Services/ProjectionSnapshotQuery.ts";

const authority = trustedSystemDispatchAuthority("matrix-bridge-integration");
const projectId = ProjectId.make("matrix-integration-project");
const threadId = ThreadId.make("matrix-integration-thread");
const allowedUserId = "@adam:example";
const roomId = "!integration:example";
const provider = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const createdAt = "2026-08-19T11:00:00.000Z";
const finalText = "Projected integration final.";

const botUserId = "@t3bot:example";

/**
 * The real adapter applies the connection's joined membership before anything
 * can be sent, and the bridge refuses to deliver until it has.
 */
const applyRoomMembership = (fake: FakeMatrixBridgeClient) =>
  fake.awaitListening.pipe(
    Effect.andThen(
      fake.emitInbound({
        kind: "membership",
        roomId,
        botUserId,
        joined: [botUserId, allowedUserId],
      }),
    ),
    Effect.asVoid,
  );

const bridgeStatus: MatrixBridgeStatus = {
  state: "active",
  ownerThreadId: threadId,
  encryptionReady: true,
  reason: null,
};

function runtimeBase(eventId: string, at: string) {
  return {
    eventId: EventId.make(eventId),
    provider,
    createdAt: at,
  };
}

function withHarness<A, E>(
  use: (harness: OrchestrationIntegrationHarness) => Effect.Effect<A, E, Crypto.Crypto>,
) {
  return Effect.acquireUseRelease(
    makeOrchestrationIntegrationHarness({ provider }),
    use,
    (harness) => harness.dispose,
  ).pipe(Effect.provide(NodeServices.layer));
}

const bridgeConfig: MatrixBridgeConfigV1 = {
  version: 1,
  homeserverUrl: "https://matrix.example",
  accessToken: "integration-secret-token",
  allowedUserIds: [allowedUserId],
  roomId,
  pairing: {
    state: "paired",
    userId: allowedUserId,
    pairedAt: createdAt,
  },
  ownerThreadId: threadId,
  ownershipEpoch: NonNegativeInt.make(1),
  cryptoStoreGeneration: "integration-generation",
  configuredAt: createdAt,
  lastDeliveredTurnId: null,
  deliveryBaselineSequence: NonNegativeInt.make(0),
  deliveryCheckpointInitialized: true,
};

/**
 * The bridge runs against the real engine and projection; only the Matrix
 * transport and the stored connection are faked.
 */
function makeReactorLayer(
  harness: OrchestrationIntegrationHarness,
  fake: FakeMatrixBridgeClient,
): Layer.Layer<MatrixBridgeReactor, never, Crypto.Crypto> {
  const configLayer = Layer.succeed(
    MatrixBridgeConfig,
    MatrixBridgeConfig.of({
      currentConfig: Effect.succeed(Option.some(bridgeConfig)),
      configView: Effect.die("configView is not used by this integration test"),
      status: Effect.succeed(bridgeStatus),
      statusChanges: Stream.empty,
      configure: () => Effect.die("configure is not used by this integration test"),
      disconnect: Effect.die("disconnect is not used by this integration test"),
      setOwner: () => Effect.die("setOwner is not used by this integration test"),
      clearOwnerIfMatches: () =>
        Effect.die("clearOwnerIfMatches is not used by this integration test"),
      recordRoomIfMatches: () =>
        Effect.die("recordRoomIfMatches is not used by this integration test"),
      reportTransportStateIfMatches: () =>
        Effect.die("reportTransportStateIfMatches is not used by this integration test"),
      markPairedIfMatches: () => Effect.succeed(true),
      reportRoomMembershipIfMatches: () => Effect.succeed(true),
      reportDegradedIfMatches: () => Effect.succeed(true),
      initializeDeliveryCheckpointIfMissing: () => Effect.succeed(true),
      markDeliveredIfMatches: () => Effect.succeed(true),
      reportPermanentSendFailureIfMatches: () => Effect.succeed(true),
    }),
  );
  const dependencies = Layer.mergeAll(
    fake.layer,
    configLayer,
    Layer.succeed(OrchestrationEngineService, harness.engine),
    Layer.succeed(ProjectionSnapshotQuery, harness.snapshotQuery),
    Layer.mock(ServerEnvironment.ServerEnvironment)({
      getEnvironmentId: Effect.succeed(EnvironmentId.make("matrix-integration-env")),
    }),
    Layer.mock(EnvironmentAuth)({
      consumePairingCredentialForProof: () =>
        Effect.die("pairing is already complete in this integration test"),
      isLivePairingCredential: () => Effect.succeed(false),
    }),
    // A bridged turn start reaches the same engine command the composer
    // dispatches, without the worktree bootstrap this fixture does not need.
    Layer.succeed(BootstrapTurnStartDispatcher, {
      dispatch: (command, authority) =>
        harness.engine.dispatch(command, authority).pipe(
          Effect.mapError(
            (cause) =>
              new OrchestrationDispatchCommandError({
                message: "Failed to dispatch a bridged turn start.",
                cause,
              }),
          ),
        ),
    }),
  );
  return MatrixBridgeReactorLive.pipe(Layer.provide(dependencies));
}

it.live("observes a real projected final through the fake Matrix client", () =>
  withHarness((harness) =>
    Effect.scoped(
      Effect.gen(function* () {
        const fake = yield* makeFakeMatrixBridgeClient;
        const reactorLayer = makeReactorLayer(harness, fake);

        return yield* Effect.gen(function* () {
          const reactor = yield* MatrixBridgeReactor;

          yield* harness.engine.dispatch(
            {
              type: "project.create",
              commandId: CommandId.make("matrix-project-create"),
              projectId,
              title: "Matrix Integration Project",
              workspaceRoot: harness.workspaceDir,
              defaultModelSelection: {
                instanceId,
                model: "gpt-5.5",
              },
              createdAt,
            },
            authority,
          );
          yield* harness.engine.dispatch(
            {
              type: "thread.create",
              commandId: CommandId.make("matrix-thread-create"),
              threadId,
              projectId,
              title: "Matrix Integration Thread",
              modelSelection: {
                instanceId,
                model: "gpt-5.5",
              },
              interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
              runtimeMode: "approval-required",
              branch: null,
              worktreePath: harness.workspaceDir,
              createdAt,
            },
            authority,
          );

          yield* reactor.start();
          yield* applyRoomMembership(fake);

          const turnResponse: TestTurnResponse = {
            events: [
              {
                type: "turn.started",
                ...runtimeBase("matrix-provider-started", "2026-08-19T11:00:01.000Z"),
                threadId,
                turnId: "fixture-turn",
              },
              {
                type: "message.delta",
                ...runtimeBase("matrix-provider-delta", "2026-08-19T11:00:02.000Z"),
                threadId,
                turnId: "fixture-turn",
                delta: finalText,
              },
              {
                type: "turn.completed",
                ...runtimeBase("matrix-provider-completed", "2026-08-19T11:00:03.000Z"),
                threadId,
                turnId: "fixture-turn",
                status: "completed",
              },
            ],
          };
          yield* harness.adapterHarness!.queueTurnResponseForNextSession(turnResponse);
          yield* harness.engine.dispatch(
            {
              type: "thread.turn.start",
              commandId: CommandId.make("matrix-turn-start"),
              threadId,
              message: {
                messageId: MessageId.make("matrix-user-message"),
                role: "user",
                text: "Produce the integration final",
                attachments: [],
              },
              interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
              runtimeMode: "approval-required",
              createdAt,
            },
            authority,
          );

          const sent = yield* fake.awaitSentCount(1);
          assert.strictEqual(sent.length, 1);
          assert.deepStrictEqual(sent[0]?.content, {
            msgtype: "m.text",
            body: finalText,
          });

          const projected = yield* harness.snapshotQuery.getThreadDetailById(threadId);
          assert.isTrue(Option.isSome(projected));
          if (Option.isSome(projected)) {
            const final = projected.value.messages.findLast(
              (message) =>
                message.role === "assistant" &&
                message.streaming === false &&
                message.turnId === TurnId.make("turn-1"),
            );
            assert.strictEqual(final?.text, finalText);
            assert.strictEqual(sent[0]?.content.body, final?.text);
          }
        }).pipe(Effect.provide(reactorLayer));
      }),
    ),
  ),
);

it.live("starts a real turn from an inbound Matrix message and ignores the room's noise", () =>
  withHarness((harness) =>
    Effect.scoped(
      Effect.gen(function* () {
        const fake = yield* makeFakeMatrixBridgeClient;
        const reactorLayer = makeReactorLayer(harness, fake);

        return yield* Effect.gen(function* () {
          const reactor = yield* MatrixBridgeReactor;

          yield* harness.engine.dispatch(
            {
              type: "project.create",
              commandId: CommandId.make("matrix-inbound-project-create"),
              projectId,
              title: "Matrix Integration Project",
              workspaceRoot: harness.workspaceDir,
              defaultModelSelection: { instanceId, model: "gpt-5.5" },
              createdAt,
            },
            authority,
          );
          yield* harness.engine.dispatch(
            {
              type: "thread.create",
              commandId: CommandId.make("matrix-inbound-thread-create"),
              threadId,
              projectId,
              title: "Matrix Integration Thread",
              modelSelection: { instanceId, model: "gpt-5.5" },
              interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
              runtimeMode: "approval-required",
              branch: null,
              worktreePath: harness.workspaceDir,
              createdAt,
            },
            authority,
          );

          yield* reactor.start();
          yield* applyRoomMembership(fake);
          yield* harness.adapterHarness!.queueTurnResponseForNextSession({
            events: [
              {
                type: "turn.started",
                ...runtimeBase("matrix-inbound-started", "2026-08-19T11:10:01.000Z"),
                threadId,
                turnId: "inbound-fixture-turn",
              },
              {
                type: "message.delta",
                ...runtimeBase("matrix-inbound-delta", "2026-08-19T11:10:02.000Z"),
                threadId,
                turnId: "inbound-fixture-turn",
                delta: finalText,
              },
              {
                type: "turn.completed",
                ...runtimeBase("matrix-inbound-completed", "2026-08-19T11:10:03.000Z"),
                threadId,
                turnId: "inbound-fixture-turn",
                status: "completed",
              },
            ],
          } satisfies TestTurnResponse);

          // The bot's own echo, another room, and a sender outside the allowed
          // list all reach the reactor first and must produce nothing.
          yield* fake.emitInbound({
            kind: "text",
            eventId: "$echo",
            roomId,
            sender: botUserId,
            body: "bridge output",
            isEdit: false,
            roomAllowedOnly: true,
            ownershipEpoch: 1,
          });
          yield* fake.emitInbound({
            kind: "text",
            eventId: "$elsewhere",
            roomId: "!other:example",
            sender: allowedUserId,
            body: "wrong room",
            isEdit: false,
            roomAllowedOnly: true,
            ownershipEpoch: 1,
          });
          yield* fake.emitInbound({
            kind: "text",
            eventId: "$stranger",
            roomId,
            sender: "@stranger:example",
            body: "not allowed",
            isEdit: false,
            roomAllowedOnly: true,
            ownershipEpoch: 1,
          });
          const delivered = yield* fake.emitInbound({
            kind: "text",
            eventId: "$adam-1",
            roomId,
            sender: allowedUserId,
            body: "Run the integration turn",
            isEdit: false,
            roomAllowedOnly: true,
            ownershipEpoch: 1,
          });
          assert.isTrue(delivered);

          const thread = yield* harness.waitForThread(
            threadId,
            (current) => current.latestTurn?.state === "completed",
          );
          const userMessages = thread.messages.filter((message) => message.role === "user");
          assert.deepStrictEqual(
            userMessages.map((message) => message.text),
            ["Run the integration turn"],
          );
          assert.strictEqual(thread.turns.length, 1);

          const sent = yield* fake.awaitSentCount(1);
          assert.strictEqual(sent.length, 1);
          assert.strictEqual(sent[0]?.content.body, finalText);
        }).pipe(Effect.provide(reactorLayer));
      }),
    ),
  ),
);
