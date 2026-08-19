import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { ChildThreadCoordinator } from "../Services/ChildThreadCoordinator.ts";
import { CheckpointReactor } from "../Services/CheckpointReactor.ts";
import {
  OrchestrationReactor,
  type OrchestrationReactorShape,
} from "../Services/OrchestrationReactor.ts";
import { ProviderCommandReactor } from "../Services/ProviderCommandReactor.ts";
import { ProviderRuntimeIngestionService } from "../Services/ProviderRuntimeIngestion.ts";
import { ScheduledTasksReactor } from "../Services/ScheduledTasksReactor.ts";
import { ThreadDeletionReactor } from "../Services/ThreadDeletionReactor.ts";
import * as AgentAwarenessRelay from "../../relay/AgentAwarenessRelay.ts";
import * as VcsMaintenanceReactor from "../../vcs/VcsMaintenanceReactor.ts";

export const makeOrchestrationReactor = Effect.gen(function* () {
  const providerRuntimeIngestion = yield* ProviderRuntimeIngestionService;
  const providerCommandReactor = yield* ProviderCommandReactor;
  const checkpointReactor = yield* CheckpointReactor;
  const threadDeletionReactor = yield* ThreadDeletionReactor;
  const agentAwarenessRelay = yield* AgentAwarenessRelay.AgentAwarenessRelay;
  const vcsMaintenanceReactor = yield* VcsMaintenanceReactor.VcsMaintenanceReactor;
  const childThreadCoordinator = yield* ChildThreadCoordinator;
  const scheduledTasksReactor = yield* ScheduledTasksReactor;

  const start: OrchestrationReactorShape["start"] = Effect.fn("start")(function* () {
    yield* providerRuntimeIngestion.start();
    yield* providerCommandReactor.start();
    yield* checkpointReactor.start();
    yield* threadDeletionReactor.start();
    yield* agentAwarenessRelay.start();
    yield* vcsMaintenanceReactor.start();
    yield* childThreadCoordinator.start();
    yield* scheduledTasksReactor.start();
  });

  return {
    start,
  } satisfies OrchestrationReactorShape;
});

export const OrchestrationReactorLive = Layer.effect(
  OrchestrationReactor,
  makeOrchestrationReactor,
);
