import { ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import {
  PendingDispatch,
  PendingDispatchId,
  PendingDispatchRepository,
} from "../Services/PendingDispatches.ts";
import { PendingDispatchRepositoryLive } from "./PendingDispatches.ts";
import { SqlitePersistenceMemory } from "./Sqlite.ts";

const layer = it.layer(
  PendingDispatchRepositoryLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);

const makeRow = (overrides: Partial<PendingDispatch> = {}): PendingDispatch =>
  ({
    id: PendingDispatchId.make("dispatch-1"),
    kind: "parent_injection",
    targetThreadId: ThreadId.make("parent-1"),
    sourceChildId: ThreadId.make("child-1"),
    text: "child completed",
    error: null,
    status: null,
    commandId: null,
    createdAt: "2026-06-17T09:00:00.000Z",
    ...overrides,
  }) satisfies PendingDispatch;

layer("PendingDispatchRepository", (it) => {
  it.effect("insert, listByTarget, and deleteByIds round-trip", () =>
    Effect.gen(function* () {
      const repository = yield* PendingDispatchRepository;
      const parent = ThreadId.make("parent-roundtrip");
      const child = ThreadId.make("child-roundtrip");

      yield* repository.insert(
        makeRow({
          id: PendingDispatchId.make("rt-inject-a"),
          targetThreadId: parent,
          sourceChildId: child,
          createdAt: "2026-06-17T09:00:00.000Z",
        }),
      );
      yield* repository.insert(
        makeRow({
          id: PendingDispatchId.make("rt-inject-b"),
          targetThreadId: parent,
          sourceChildId: child,
          createdAt: "2026-06-17T09:01:00.000Z",
        }),
      );
      // Different kind for the same target must not leak into the injection list.
      yield* repository.insert(
        makeRow({
          id: PendingDispatchId.make("rt-steer"),
          kind: "child_steer",
          targetThreadId: parent,
          sourceChildId: null,
          text: "please retry",
        }),
      );

      const injections = yield* repository.listByTarget({
        kind: "parent_injection",
        targetThreadId: parent,
      });
      assert.deepEqual(
        injections.map((row) => row.id),
        ["rt-inject-a", "rt-inject-b"],
      );
      assert.equal(injections[0]?.text, "child completed");
      assert.equal(injections[0]?.sourceChildId, child);

      const steers = yield* repository.listByTarget({
        kind: "child_steer",
        targetThreadId: parent,
      });
      assert.equal(steers.length, 1);
      assert.equal(steers[0]?.sourceChildId, null);
      assert.equal(steers[0]?.text, "please retry");

      yield* repository.deleteByIds([
        PendingDispatchId.make("rt-inject-a"),
        PendingDispatchId.make("rt-inject-b"),
      ]);

      const afterDelete = yield* repository.listByTarget({
        kind: "parent_injection",
        targetThreadId: parent,
      });
      assert.equal(afterDelete.length, 0);

      // Steer row survives the targeted delete.
      const steersAfter = yield* repository.listByTarget({
        kind: "child_steer",
        targetThreadId: parent,
      });
      assert.equal(steersAfter.length, 1);
    }),
  );

  it.effect("deleteByIds is a no-op for an empty id list", () =>
    Effect.gen(function* () {
      const repository = yield* PendingDispatchRepository;
      const parent = ThreadId.make("parent-empty-delete");

      yield* repository.insert(
        makeRow({ id: PendingDispatchId.make("empty-delete-keep"), targetThreadId: parent }),
      );
      yield* repository.deleteByIds([]);

      const remaining = yield* repository.listByTarget({
        kind: "parent_injection",
        targetThreadId: parent,
      });
      assert.equal(remaining.length, 1);
    }),
  );

  it.effect("claim stamps command_id and is a no-op for an empty id list", () =>
    Effect.gen(function* () {
      const repository = yield* PendingDispatchRepository;
      const parent = ThreadId.make("parent-claim");

      yield* repository.insert(
        makeRow({ id: PendingDispatchId.make("claim-a"), targetThreadId: parent }),
      );
      yield* repository.insert(
        makeRow({ id: PendingDispatchId.make("claim-b"), targetThreadId: parent }),
      );

      // Empty claim is a no-op (no rows touched).
      yield* repository.claim({ ids: [], commandId: "server:subagent-wake:noop" });
      yield* repository.claim({
        ids: [PendingDispatchId.make("claim-a")],
        commandId: "server:subagent-wake:claim-a",
      });

      const rows = yield* repository.listByTarget({
        kind: "parent_injection",
        targetThreadId: parent,
      });
      const byId = new Map(rows.map((row) => [row.id as string, row]));
      assert.equal(byId.get("claim-a")?.commandId, "server:subagent-wake:claim-a");
      assert.equal(byId.get("claim-b")?.commandId, null);
    }),
  );

  it.effect("listAll returns every row across targets, oldest first", () =>
    Effect.gen(function* () {
      const repository = yield* PendingDispatchRepository;

      yield* repository.insert(
        makeRow({
          id: PendingDispatchId.make("all-2"),
          targetThreadId: ThreadId.make("parent-all-x"),
          createdAt: "2026-06-17T10:00:00.000Z",
        }),
      );
      yield* repository.insert(
        makeRow({
          id: PendingDispatchId.make("all-1"),
          targetThreadId: ThreadId.make("parent-all-y"),
          createdAt: "2026-06-17T09:00:00.000Z",
        }),
      );

      const all = yield* repository.listAll();
      const ids = all.map((row) => row.id as string);
      assert.isTrue(ids.includes("all-1"));
      assert.isTrue(ids.includes("all-2"));
      assert.isTrue(ids.indexOf("all-1") < ids.indexOf("all-2"));
    }),
  );
});
