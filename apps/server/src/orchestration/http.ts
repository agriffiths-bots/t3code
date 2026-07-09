import {
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

import { cleanupPersistedCommandAttachments, normalizeDispatchCommand } from "./Normalizer.ts";
import {
  annotateEnvironmentRequest,
  failEnvironmentInternal,
  failEnvironmentInvalidRequest,
  failEnvironmentNotFound,
  requireEnvironmentScope,
} from "../auth/http.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import {
  OrchestrationCommandInvariantError,
  OrchestrationCommandPreviouslyRejectedError,
} from "./Errors.ts";

const isClientCommandDispatchError = (cause: unknown) =>
  isOrchestrationCommandInvariantError(cause) ||
  isOrchestrationCommandPreviouslyRejectedError(cause);

const isOrchestrationDispatchCommandError = Schema.is(OrchestrationDispatchCommandError);
const isOrchestrationCommandInvariantError = Schema.is(OrchestrationCommandInvariantError);
const isOrchestrationCommandPreviouslyRejectedError = Schema.is(
  OrchestrationCommandPreviouslyRejectedError,
);

export const orchestrationHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "orchestration",
  Effect.fnUntraced(function* (handlers) {
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const orchestrationEngine = yield* OrchestrationEngineService;

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
          const snapshot = yield* projectionSnapshotQuery
            .getThreadDetailSnapshot(args.params.threadId)
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_thread_snapshot_failed", cause),
              ),
            );
          if (Option.isNone(snapshot)) {
            return yield* failEnvironmentNotFound("thread_not_found");
          }
          return snapshot.value;
        }),
      )
      .handle(
        "dispatch",
        Effect.fn("environment.orchestration.dispatch")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          const normalizedCommandExit = yield* Effect.exit(normalizeDispatchCommand(args.payload));
          if (Exit.isFailure(normalizedCommandExit)) {
            const cause = Cause.squash(normalizedCommandExit.cause);
            if (isOrchestrationDispatchCommandError(cause)) {
              return yield* failEnvironmentInvalidRequest("invalid_command");
            }
            return yield* failEnvironmentInternal("orchestration_dispatch_failed", cause);
          }
          const normalizedCommand = normalizedCommandExit.value;
          return yield* orchestrationEngine.dispatch(normalizedCommand).pipe(
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
      );
  }),
);
