import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as DeviceNotifications from "../../../notifications/DeviceNotifications.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ThreadStartToolError } from "../thread/tools.ts";
import { NotifyToolkit, type NotifyInput } from "./tools.ts";

const isThreadStartToolError = Schema.is(ThreadStartToolError);

const fail = (message: string) => new ThreadStartToolError({ message });

const toToolError = (error: unknown, fallback: string): ThreadStartToolError =>
  isThreadStartToolError(error) ? error : fail(error instanceof Error ? error.message : fallback);

const requireInvocation = McpInvocationContext.requireProviderMcpCapability("notification").pipe(
  Effect.mapError((error) => fail(error.message)),
);

function threadDeepLink(invocation: McpInvocationContext.ProviderMcpInvocationScope): string {
  return `/${encodeURIComponent(invocation.environmentId)}/${encodeURIComponent(
    invocation.threadId,
  )}`;
}

const makeHandlers = Effect.fn("NotifyToolkit.makeHandlers")(function* () {
  const notifications = yield* DeviceNotifications.DeviceNotifications;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;

  const notify = Effect.fn("NotifyToolkit.notify")(function* (input: NotifyInput) {
    const invocation = yield* requireInvocation;
    const thread = yield* projectionSnapshotQuery
      .getThreadShellByIdIncludingArchived(invocation.threadId)
      .pipe(
        Effect.mapError((error) => toToolError(error, "Failed to resolve notification scope.")),
      );
    if (Option.isNone(thread)) {
      return yield* fail(`Thread ${invocation.threadId} was not found.`);
    }
    const deepLink = input.deepLink ?? threadDeepLink(invocation);
    const result = yield* notifications
      .notify(
        {
          ...input,
          deepLink,
        },
        {
          dataAudience: thread.value.dataAudience,
          resultAudienceCeiling: thread.value.dataAudience,
        },
      )
      .pipe(Effect.mapError((error) => toToolError(error, "Failed to send notification.")));

    return {
      ...result,
      threadId: invocation.threadId,
      deepLink,
    };
  });

  return {
    t3_notify: notify,
  } satisfies Parameters<typeof NotifyToolkit.toLayer>[0];
});

export const NotifyToolkitHandlersLive = Layer.unwrap(
  makeHandlers().pipe(Effect.map((handlers) => NotifyToolkit.toLayer(handlers))),
);
