import { EnvironmentId, IsoDateTime, ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { RemoteChild, RemoteChildRepository } from "../Services/RemoteChildren.ts";
import { RemoteChildRepositoryLive } from "./RemoteChildren.ts";
import { SqlitePersistenceMemory } from "./Sqlite.ts";

const layer = it.layer(RemoteChildRepositoryLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)));

const parentThreadId = ThreadId.make("remote-parent");
const childEnvironmentId = EnvironmentId.make("remote-env");
const childThreadId = ThreadId.make("remote-child");

const makeRow = (overrides: Partial<RemoteChild> = {}): RemoteChild =>
  ({
    parentThreadId,
    childEnvironmentId,
    childThreadId,
    alias: "backend-b",
    spawnParams: { prompt: "remote", directory: "/remote/repo", detached: true },
    status: "running",
    lastPolledAt: null,
    createdAt: IsoDateTime.make("2026-07-08T09:00:00.000Z"),
    updatedAt: IsoDateTime.make("2026-07-08T09:00:00.000Z"),
    ...overrides,
  }) satisfies RemoteChild;

layer("RemoteChildRepository", (it) => {
  it.effect("does not let stale non-terminal updates overwrite terminal status", () =>
    Effect.gen(function* () {
      const repository = yield* RemoteChildRepository;

      yield* repository.upsert(makeRow());
      const claimed = yield* repository.claimTerminalDelivery({
        parentThreadId,
        childEnvironmentId,
        childThreadId,
        claimId: "terminal-claim-1",
        claimedAt: IsoDateTime.make("2026-07-08T09:01:00.000Z"),
        claimStaleBefore: IsoDateTime.make("2026-07-08T08:56:00.000Z"),
        lastPolledAt: IsoDateTime.make("2026-07-08T09:01:00.000Z"),
        updatedAt: IsoDateTime.make("2026-07-08T09:01:00.000Z"),
      });
      assert.isTrue(Option.isSome(claimed));

      const marked = yield* repository.markTerminalStatus({
        parentThreadId,
        childEnvironmentId,
        childThreadId,
        claimId: "terminal-claim-1",
        status: "completed",
        lastPolledAt: IsoDateTime.make("2026-07-08T09:02:00.000Z"),
        updatedAt: IsoDateTime.make("2026-07-08T09:02:00.000Z"),
      });
      assert.equal(Option.getOrThrow(marked).status, "completed");

      yield* repository.updateStatus({
        parentThreadId,
        childEnvironmentId,
        childThreadId,
        status: "running",
        lastPolledAt: IsoDateTime.make("2026-07-08T09:03:00.000Z"),
        updatedAt: IsoDateTime.make("2026-07-08T09:03:00.000Z"),
      });

      const [row] = yield* repository.listByParent({ parentThreadId });
      assert.equal(row?.status, "completed");
      assert.equal(row?.updatedAt, "2026-07-08T09:02:00.000Z");
    }),
  );
});
