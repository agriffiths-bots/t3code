import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";

export interface ThreadReconciliationActivityEvent {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly reason: "locally-initiated-turn";
}

/**
 * In-process bridge between command atoms and mounted thread state. The live
 * event stream normally supplies the same activity signal, but publishing the
 * local intent ensures reconciliation starts promptly even when that live
 * frame is precisely the one that was missed.
 */
export class ThreadReconciliationActivity extends Context.Service<
  ThreadReconciliationActivity,
  {
    readonly publish: (event: ThreadReconciliationActivityEvent) => Effect.Effect<void>;
    readonly events: Stream.Stream<ThreadReconciliationActivityEvent>;
  }
>()("@t3tools/client-runtime/state/threadReconciliationActivity") {}

export const threadReconciliationActivityLayer = Layer.effect(
  ThreadReconciliationActivity,
  Effect.gen(function* () {
    const events = yield* PubSub.unbounded<ThreadReconciliationActivityEvent>();
    yield* Effect.addFinalizer(() => PubSub.shutdown(events));
    return ThreadReconciliationActivity.of({
      publish: (event) => PubSub.publish(events, event).pipe(Effect.asVoid),
      events: Stream.fromPubSub(events),
    });
  }),
);
