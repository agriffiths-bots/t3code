import { describe, expect, it } from "@effect/vitest";
import { ThreadId, type OrchestrationCommand } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";

import {
  commandRequiresWorktreeLifecycle,
  WorktreeLifecycleCoordinator,
  WorktreeLifecycleCoordinatorLive,
} from "./WorktreeLifecycleCoordinator.ts";

describe("WorktreeLifecycleCoordinator", () => {
  it.effect("serializes competing lifecycle operations", () =>
    Effect.gen(function* () {
      const coordinator = yield* WorktreeLifecycleCoordinator;
      const firstEntered = yield* Deferred.make<void>();
      const releaseFirst = yield* Deferred.make<void>();
      const secondEntered = yield* Deferred.make<void>();

      const first = yield* coordinator
        .withPermit(
          Deferred.succeed(firstEntered, undefined).pipe(
            Effect.andThen(Deferred.await(releaseFirst)),
          ),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(firstEntered);

      const second = yield* coordinator
        .withPermit(Deferred.succeed(secondEntered, undefined))
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      expect(Option.isNone(yield* Deferred.poll(secondEntered))).toBe(true);

      yield* Deferred.succeed(releaseFirst, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(second);
      expect(Option.isSome(yield* Deferred.poll(secondEntered))).toBe(true);
    }).pipe(Effect.provide(WorktreeLifecycleCoordinatorLive)),
  );

  it("covers lifecycle, activation, project-root, branch, and worktree metadata commands", () => {
    const coordinated = [
      { type: "thread.archive" },
      { type: "thread.delete" },
      { type: "thread.unarchive" },
      { type: "thread.turn.start" },
      { type: "project.delete" },
      { type: "project.meta.update", workspaceRoot: "/retargeted" },
      { type: "thread.meta.update", branch: "feature/renamed" },
      { type: "thread.meta.update", worktreePath: "/worktree" },
    ] as unknown as ReadonlyArray<OrchestrationCommand>;
    for (const command of coordinated) {
      expect(commandRequiresWorktreeLifecycle(command)).toBe(true);
    }

    expect(
      commandRequiresWorktreeLifecycle({
        type: "project.meta.update",
        title: "Rename only",
      } as unknown as OrchestrationCommand),
    ).toBe(false);
    expect(
      commandRequiresWorktreeLifecycle({
        type: "thread.meta.update",
        title: "Rename only",
      } as unknown as OrchestrationCommand),
    ).toBe(false);
  });

  it.effect("tracks pending teardown until ownership metadata is cleared", () =>
    Effect.gen(function* () {
      const coordinator = yield* WorktreeLifecycleCoordinator;
      const threadId = ThreadId.make("thread-pending-teardown");

      expect(yield* coordinator.isTeardownPending(threadId)).toBe(false);
      yield* coordinator.markTeardownPending(threadId);
      expect(yield* coordinator.isTeardownPending(threadId)).toBe(true);
      yield* coordinator.clearTeardownPending(threadId);
      expect(yield* coordinator.isTeardownPending(threadId)).toBe(false);
    }).pipe(Effect.provide(WorktreeLifecycleCoordinatorLive)),
  );
});
