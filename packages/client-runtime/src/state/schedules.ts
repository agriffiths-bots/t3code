import {
  ORCHESTRATION_WS_METHODS,
  type EnvironmentId,
  type ScheduledTaskEntry,
  type ScheduledTasksSnapshot,
  type ScheduledTasksStreamItem,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom } from "effect/unstable/reactivity";

import { EnvironmentRegistry } from "../connection/registry.ts";
import { connectionProjectionPhase } from "../connection/model.ts";
import { EnvironmentSupervisor } from "../connection/supervisor.ts";
import { subscribe } from "../rpc/client.ts";
import { createEnvironmentRpcCommand, followStreamInEnvironment } from "./runtime.ts";

/**
 * Environment-scoped scheduled-tasks state, modelled 1:1 on `state/shell.ts`.
 *
 * One `subscribeScheduledTasks` subscription per environment folds a snapshot
 * (then upsert/remove deltas) into a `SubscriptionRef`, guarded by the
 * monotonic `sequence` exactly like the shell stream (state/shell.ts:119).
 * Unlike the shell there is no local cache layer — schedules stream fresh on
 * connect, which is acceptable for a small list that the server keeps live.
 *
 * The single subscription feeds every web surface (panel, thread-row icon,
 * composer banner) via `schedulesByThreadIdAtom` in the web package.
 */
export type EnvironmentScheduledTasksStatus = "empty" | "synchronizing" | "live";

export interface EnvironmentScheduledTasksState {
  readonly snapshot: Option.Option<ScheduledTasksSnapshot>;
  readonly status: EnvironmentScheduledTasksStatus;
  readonly error: Option.Option<string>;
}

export const EMPTY_SCHEDULED_TASKS_STATE: EnvironmentScheduledTasksState = {
  snapshot: Option.none(),
  status: "empty",
  error: Option.none(),
};

function statusForSnapshot(
  snapshot: Option.Option<ScheduledTasksSnapshot>,
): EnvironmentScheduledTasksStatus {
  return Option.isSome(snapshot) ? "live" : "empty";
}

function formatError(error: unknown): string {
  return error instanceof Error && error.message.trim().length > 0
    ? error.message
    : "Could not synchronize scheduled tasks.";
}

// Fold one stream item into the current snapshot, honouring the monotonic
// sequence guard (deltas older than the applied snapshot are ignored).
function applyScheduledTasksStreamItem(
  current: Option.Option<ScheduledTasksSnapshot>,
  item: ScheduledTasksStreamItem,
): ScheduledTasksSnapshot | null {
  if (item.kind === "snapshot") {
    return item.snapshot;
  }
  return Option.match(current, {
    onNone: () => null,
    onSome: (snapshot) => {
      if (item.sequence <= snapshot.sequence) {
        return snapshot;
      }
      if (item.kind === "task-upserted") {
        const tasks = snapshot.tasks.filter((task) => task.taskId !== item.task.taskId);
        return { sequence: item.sequence, tasks: [...tasks, item.task] };
      }
      return {
        sequence: item.sequence,
        tasks: snapshot.tasks.filter((task) => task.taskId !== item.taskId),
      };
    },
  });
}

export const makeEnvironmentScheduledTasksState = Effect.fn("EnvironmentScheduledTasksState.make")(
  function* () {
    const supervisor = yield* EnvironmentSupervisor;
    const state = yield* SubscriptionRef.make<EnvironmentScheduledTasksState>(
      EMPTY_SCHEDULED_TASKS_STATE,
    );

    const setSynchronizing = SubscriptionRef.update(state, (current) => ({
      ...current,
      status: current.status === "live" ? ("live" as const) : ("synchronizing" as const),
      error: Option.none(),
    }));
    const setDisconnected = SubscriptionRef.update(state, (current) => ({
      ...current,
      status: statusForSnapshot(current.snapshot),
    }));
    const setStreamError = (error: unknown) =>
      SubscriptionRef.update(state, (current) => ({
        ...current,
        status: statusForSnapshot(current.snapshot),
        error: Option.some(formatError(error)),
      }));

    const applyItem = Effect.fn("EnvironmentScheduledTasksState.applyItem")(function* (
      item: ScheduledTasksStreamItem,
    ) {
      const current = yield* SubscriptionRef.get(state);
      const nextSnapshot = applyScheduledTasksStreamItem(current.snapshot, item);
      if (nextSnapshot === null) {
        return;
      }
      yield* SubscriptionRef.set(state, {
        snapshot: Option.some(nextSnapshot),
        status: "live",
        error: Option.none(),
      });
    });

    yield* subscribe(
      ORCHESTRATION_WS_METHODS.subscribeScheduledTasks,
      {},
      {
        onExpectedFailure: (cause) => setStreamError(Cause.squash(cause)),
      },
    ).pipe(Stream.runForEach(applyItem), Effect.forkScoped);

    yield* SubscriptionRef.changes(supervisor.state).pipe(
      Stream.runForEach((connectionState) => {
        switch (connectionProjectionPhase(connectionState)) {
          case "synchronizing":
            return setSynchronizing;
          case "disconnected":
            return setDisconnected;
          case "ready":
            return setSynchronizing;
        }
      }),
      Effect.forkScoped,
    );

    return state;
  },
);

export function scheduledTasksStateChanges(environmentId: EnvironmentId) {
  return followStreamInEnvironment(
    environmentId,
    Stream.unwrap(makeEnvironmentScheduledTasksState().pipe(Effect.map(SubscriptionRef.changes))),
  );
}

export function createEnvironmentScheduledTasksAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  const stateAtom = Atom.family((environmentId: EnvironmentId) =>
    runtime.atom(scheduledTasksStateChanges(environmentId), {
      initialValue: EMPTY_SCHEDULED_TASKS_STATE,
    }),
  );

  const stateValueAtom = Atom.family((environmentId: EnvironmentId) =>
    Atom.make((get) =>
      Option.getOrElse(
        AsyncResult.value(get(stateAtom(environmentId))),
        () => EMPTY_SCHEDULED_TASKS_STATE,
      ),
    ).pipe(Atom.withLabel(`environment-scheduled-tasks-state-value:${environmentId}`)),
  );

  return {
    stateAtom,
    stateValueAtom,
  };
}

export type EnvironmentScheduledTaskEntry = ScheduledTaskEntry;

/**
 * Interactive scheduled-task mutations, modelled on `createShellEnvironmentAtoms`
 * (state/shellCommands.ts). Each command targets an environment and reuses the
 * server repo update/delete the MCP `t3_schedule_update`/`t3_schedule_delete`
 * handlers call; the server bumps its liveness `SubscriptionRef`, so the
 * subscription folded above re-surfaces the change on every web surface.
 * Create stays agent-only — no command is exposed for it.
 */
export function createScheduledTasksEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  return {
    setEnabled: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:scheduled-tasks:set-enabled",
      tag: ORCHESTRATION_WS_METHODS.setScheduledTaskEnabled,
    }),
    delete: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:scheduled-tasks:delete",
      tag: ORCHESTRATION_WS_METHODS.deleteScheduledTask,
    }),
  };
}
