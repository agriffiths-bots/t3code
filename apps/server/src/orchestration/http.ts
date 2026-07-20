import {
  AuthProjectAudienceAdminScope,
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
  OrchestrationDispatchCommandError,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";

import {
  cleanupPersistedCommandAttachments,
  normalizeAuthorizedDispatchCommand,
} from "./Normalizer.ts";
import { sessionDispatchAuthority } from "./commandAudienceGuard.ts";
import {
  annotateEnvironmentRequest,
  failEnvironmentInternal,
  failEnvironmentInvalidRequest,
  failEnvironmentNotFound,
  failEnvironmentScopeRequired,
  requireEnvironmentScope,
} from "../auth/http.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import { coveredThreadRevision } from "./threadRevision.ts";
import { OrchestrationEventStore } from "../persistence/Services/OrchestrationEventStore.ts";
import {
  ProjectAudienceAdministrationError,
  setProjectAudienceToFactory,
} from "../project/ProjectAudienceAdministration.ts";
import {
  OrchestrationCommandAudienceAuthorizationError,
  OrchestrationCommandInvariantError,
  OrchestrationCommandPreviouslyRejectedError,
} from "./Errors.ts";

const isClientCommandDispatchError = (cause: unknown) =>
  isOrchestrationCommandInvariantError(cause) ||
  isOrchestrationCommandAudienceAuthorizationError(cause) ||
  isOrchestrationCommandPreviouslyRejectedError(cause);

const isOrchestrationDispatchCommandError = Schema.is(OrchestrationDispatchCommandError);
const isOrchestrationCommandInvariantError = Schema.is(OrchestrationCommandInvariantError);
const isOrchestrationCommandAudienceAuthorizationError = Schema.is(
  OrchestrationCommandAudienceAuthorizationError,
);
const isOrchestrationCommandPreviouslyRejectedError = Schema.is(
  OrchestrationCommandPreviouslyRejectedError,
);
const isProjectAudienceAdministrationError = Schema.is(ProjectAudienceAdministrationError);

export const orchestrationHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "orchestration",
  Effect.fnUntraced(function* (handlers) {
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const orchestrationEngine = yield* OrchestrationEngineService;
    const orchestrationEventStore = yield* OrchestrationEventStore;

    return handlers
      .handle(
        "snapshot",
        Effect.fn("environment.orchestration.snapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* projectionSnapshotQuery
            .getSnapshot()
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_snapshot_failed", cause),
              ),
            );
        }),
      )
      .handle(
        "shellSnapshot",
        Effect.fn("environment.orchestration.shellSnapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* projectionSnapshotQuery
            .getShellSnapshot()
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_snapshot_failed", cause),
              ),
            );
        }),
      )
      .handle(
        "threadSnapshot",
        Effect.fn("environment.orchestration.threadSnapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          const [snapshot, latestRevision] = yield* Effect.all([
            projectionSnapshotQuery.getThreadDetailSnapshot(args.params.threadId),
            orchestrationEventStore.getLatestThreadRevision(args.params.threadId),
          ]).pipe(
            Effect.catch((cause) =>
              failEnvironmentInternal("orchestration_thread_snapshot_failed", cause),
            ),
          );
          if (Option.isNone(snapshot)) {
            return yield* failEnvironmentNotFound("thread_not_found");
          }
          return {
            ...snapshot.value,
            storageEpoch: orchestrationEventStore.storageEpoch,
            ...coveredThreadRevision(snapshot.value.snapshotSequence, latestRevision),
          };
        }),
      )
      .handle(
        "threadRevision",
        Effect.fn("environment.orchestration.threadRevision")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          const session = yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          if (session.audienceCeiling === "factory") {
            return yield* failEnvironmentScopeRequired(AuthOrchestrationReadScope);
          }
          const threadSnapshot = yield* projectionSnapshotQuery
            .getThreadShellSnapshotByIdIncludingArchived(args.params.threadId)
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_thread_revision_failed", cause),
              ),
            );
          if (Option.isNone(threadSnapshot.thread)) {
            const latestStoreSequence = yield* orchestrationEventStore
              .getLatestSequence()
              .pipe(
                Effect.catch((cause) =>
                  failEnvironmentInternal("orchestration_thread_revision_failed", cause),
                ),
              );
            if (threadSnapshot.snapshotSequence < latestStoreSequence) {
              return yield* failEnvironmentInternal("orchestration_thread_revision_failed");
            }
            return yield* failEnvironmentNotFound("thread_not_found");
          }
          const revision = yield* Effect.all({
            latest: orchestrationEventStore.getLatestThreadRevision(args.params.threadId),
            projection: projectionSnapshotQuery.getSnapshotSequence(),
          }).pipe(
            Effect.catch((cause) =>
              failEnvironmentInternal("orchestration_thread_revision_failed", cause),
            ),
          );
          return {
            storageEpoch: orchestrationEventStore.storageEpoch,
            ...revision.latest,
            projectionSequence: revision.projection.snapshotSequence,
          };
        }),
      )
      .handle(
        "dispatch",
        Effect.fn("environment.orchestration.dispatch")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          const session = yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          const authority = sessionDispatchAuthority(session);
          const normalizedCommandExit = yield* Effect.exit(
            normalizeAuthorizedDispatchCommand(args.payload, authority),
          );
          if (Exit.isFailure(normalizedCommandExit)) {
            const cause = Cause.squash(normalizedCommandExit.cause);
            if (isOrchestrationDispatchCommandError(cause) || isClientCommandDispatchError(cause)) {
              return yield* failEnvironmentInvalidRequest("invalid_command");
            }
            return yield* failEnvironmentInternal("orchestration_dispatch_failed", cause);
          }
          const normalizedCommand = normalizedCommandExit.value;
          return yield* orchestrationEngine.dispatch(normalizedCommand, authority).pipe(
            Effect.catch((cause) =>
              Effect.gen(function* () {
                yield* cleanupPersistedCommandAttachments(normalizedCommand);
                if (isClientCommandDispatchError(cause)) {
                  return yield* failEnvironmentInvalidRequest("invalid_command");
                }
                return yield* failEnvironmentInternal("orchestration_dispatch_failed", cause);
              }),
            ),
          );
        }),
      )
      .handle(
        "setProjectAudienceToFactory",
        Effect.fn("environment.orchestration.setProjectAudienceToFactory")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          const session = yield* requireEnvironmentScope(AuthProjectAudienceAdminScope);
          return yield* setProjectAudienceToFactory({
            projectId: args.payload.projectId,
            actor: session.subject,
          }).pipe(
            Effect.catch((cause) =>
              Effect.gen(function* () {
                if (
                  isProjectAudienceAdministrationError(cause) ||
                  isClientCommandDispatchError(cause)
                ) {
                  return yield* failEnvironmentInvalidRequest("invalid_command");
                }
                return yield* failEnvironmentInternal("orchestration_dispatch_failed", cause);
              }),
            ),
          );
        }),
      );
  }),
);
