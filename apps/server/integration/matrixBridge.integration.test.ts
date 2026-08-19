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
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type MatrixBridgeStatus,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import type { TestTurnResponse } from "./TestProviderAdapter.integration.ts";
import {
  makeOrchestrationIntegrationHarness,
  type OrchestrationIntegrationHarness,
} from "./OrchestrationEngineHarness.integration.ts";
import * as ServerEnvironment from "../src/environment/ServerEnvironment.ts";
import { makeFakeMatrixBridgeClient } from "../src/matrix/MatrixBridgeClient.ts";
import {
  MatrixBridgeReactor,
  layer as MatrixBridgeReactorLive,
} from "../src/matrix/MatrixBridgeReactor.ts";
import { MatrixBridgeConfig, type MatrixBridgeConfigV1 } from "../src/matrix/MatrixBridgeConfig.ts";
import { trustedSystemDispatchAuthority } from "../src/orchestration/commandAudienceGuard.ts";
import { OrchestrationEngineService } from "../src/orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../src/orchestration/Services/ProjectionSnapshotQuery.ts";

const authority = trustedSystemDispatchAuthority("matrix-bridge-integration");
const projectId = ProjectId.make("matrix-integration-project");
const threadId = ThreadId.make("matrix-integration-thread");
const provider = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const createdAt = "2026-08-19T11:00:00.000Z";
const finalText = "Projected integration final.";

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

function withHarness<A, E>(use: (harness: OrchestrationIntegrationHarness) => Effect.Effect<A, E>) {
  return Effect.acquireUseRelease(
    makeOrchestrationIntegrationHarness({ provider }),
    use,
    (harness) => harness.dispose,
  ).pipe(Effect.provide(NodeServices.layer));
}

it.live("observes a real projected final through the fake Matrix client", () =>
  withHarness((harness) =>
    Effect.scoped(
      Effect.gen(function* () {
        const fake = yield* makeFakeMatrixBridgeClient;
        const config: MatrixBridgeConfigV1 = {
          version: 1,
          homeserverUrl: "https://matrix.example",
          accessToken: "integration-secret-token",
          allowedUserIds: ["@adam:example"],
          roomId: "!integration:example",
          pairing: {
            state: "paired",
            userId: "@adam:example",
            pairedAt: createdAt,
          },
          ownerThreadId: threadId,
          ownershipEpoch: NonNegativeInt.make(1),
          cryptoStoreGeneration: "integration-generation",
        };
        const configLayer = Layer.succeed(
          MatrixBridgeConfig,
          MatrixBridgeConfig.of({
            currentConfig: Effect.succeed(Option.some(config)),
            status: Effect.succeed(bridgeStatus),
            statusChanges: Stream.empty,
            configure: () => Effect.die("configure is not used by this integration test"),
            disconnect: Effect.die("disconnect is not used by this integration test"),
            setOwner: () => Effect.die("setOwner is not used by this integration test"),
            clearOwnerIfMatches: () =>
              Effect.die("clearOwnerIfMatches is not used by this integration test"),
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
        );
        const reactorLayer = MatrixBridgeReactorLive.pipe(Layer.provide(dependencies));

        return yield* Effect.gen(function* () {
          const reactor = yield* MatrixBridgeReactor;
          yield* reactor.start();

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
