import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { ChildThreadCoordinator } from "../Services/ChildThreadCoordinator.ts";
import { CheckpointReactor } from "../Services/CheckpointReactor.ts";
import { OrchestrationReactor } from "../Services/OrchestrationReactor.ts";
import { ProviderCommandReactor } from "../Services/ProviderCommandReactor.ts";
import { ProviderRuntimeIngestionService } from "../Services/ProviderRuntimeIngestion.ts";
import { ScheduledTasksReactor } from "../Services/ScheduledTasksReactor.ts";
import { ThreadDeletionReactor } from "../Services/ThreadDeletionReactor.ts";
import { makeOrchestrationReactor } from "./OrchestrationReactor.ts";
import * as AgentAwarenessRelay from "../../relay/AgentAwarenessRelay.ts";
import { MatrixBridgeReactor } from "../../matrix/MatrixBridgeReactor.ts";
import { ServerActivation } from "../../serverActivation.ts";
import * as VcsMaintenanceReactor from "../../vcs/VcsMaintenanceReactor.ts";

const inertDependencies = Layer.mergeAll(
  Layer.succeed(ProviderRuntimeIngestionService, {
    start: () => Effect.void,
    drain: Effect.void,
  }),
  Layer.succeed(ProviderCommandReactor, {
    start: () => Effect.void,
    drain: Effect.void,
  }),
  Layer.succeed(CheckpointReactor, {
    start: () => Effect.void,
    drain: Effect.void,
  }),
  Layer.succeed(ThreadDeletionReactor, {
    start: () => Effect.void,
    drain: Effect.void,
  }),
  Layer.succeed(AgentAwarenessRelay.AgentAwarenessRelay, {
    publishThread: () => Effect.void,
    start: () => Effect.void,
  }),
  Layer.succeed(VcsMaintenanceReactor.VcsMaintenanceReactor, {
    start: () => Effect.void,
    sweep: () => Effect.void,
  }),
);

it.effect("does not start activation-sensitive reactors before activation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const activation = yield* Deferred.make<void>();
      const childCoordinatorStarted = yield* Deferred.make<void>();
      const schedulerTicked = yield* Deferred.make<void>();
      const matrixBridgeStarted = yield* Deferred.make<void>();
      const activationStartupReady = yield* Deferred.make<void>();
      const startOrder: string[] = [];

      const layer = Layer.effect(OrchestrationReactor, makeOrchestrationReactor).pipe(
        Layer.provideMerge(inertDependencies),
        Layer.provideMerge(
          Layer.mock(ChildThreadCoordinator)({
            start: () =>
              Effect.sync(() => {
                startOrder.push("child-coordinator");
              }).pipe(Effect.andThen(Deferred.succeed(childCoordinatorStarted, undefined))),
          }),
        ),
        Layer.provideMerge(
          Layer.succeed(ScheduledTasksReactor, {
            start: () =>
              Effect.sync(() => {
                startOrder.push("scheduler");
              }).pipe(Effect.andThen(Deferred.succeed(schedulerTicked, undefined))),
          }),
        ),
        Layer.provideMerge(
          Layer.succeed(MatrixBridgeReactor, {
            start: () =>
              Effect.sync(() => {
                startOrder.push("matrix-bridge");
              }).pipe(Effect.andThen(Deferred.succeed(matrixBridgeStarted, undefined))),
            drain: Effect.void,
          }),
        ),
        Layer.provideMerge(Layer.succeed(ServerActivation, Deferred.await(activation))),
      );

      yield* Effect.gen(function* () {
        const reactor = yield* OrchestrationReactor;
        yield* reactor.start();

        yield* reactor.activationReady.pipe(
          Effect.andThen(Deferred.succeed(activationStartupReady, undefined)),
          Effect.forkScoped,
        );
        yield* Effect.yieldNow;

        expect(yield* Deferred.isDone(childCoordinatorStarted)).toBe(false);
        expect(yield* Deferred.isDone(schedulerTicked)).toBe(false);
        expect(yield* Deferred.isDone(activationStartupReady)).toBe(false);
        expect(yield* Deferred.isDone(matrixBridgeStarted)).toBe(false);

        yield* Deferred.succeed(activation, undefined);
        yield* Deferred.await(activationStartupReady);

        expect(yield* Deferred.isDone(childCoordinatorStarted)).toBe(true);
        expect(yield* Deferred.isDone(schedulerTicked)).toBe(true);
        expect(yield* Deferred.isDone(matrixBridgeStarted)).toBe(true);
        expect(startOrder).toEqual(["child-coordinator", "scheduler", "matrix-bridge"]);
      }).pipe(Effect.provide(layer));
    }),
  ),
);
