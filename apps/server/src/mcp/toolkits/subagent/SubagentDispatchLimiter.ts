import type { ThreadId } from "@t3tools/contracts";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

const DEFAULT_MAX_CONCURRENT_DISPATCHES = 6;

export interface SubagentDispatchLease {
  readonly id: symbol;
}

export interface SubagentDispatchLimiterShape {
  readonly maxConcurrentDispatches: number;
  readonly acquire: Effect.Effect<SubagentDispatchLease>;
  readonly bindChild: (
    lease: SubagentDispatchLease,
    childThreadId: ThreadId,
  ) => Effect.Effect<void>;
  readonly seedChild: (childThreadId: ThreadId) => Effect.Effect<void>;
  readonly release: (lease: SubagentDispatchLease) => Effect.Effect<void>;
  readonly releaseForChild: (childThreadId: ThreadId) => Effect.Effect<void>;
}

export class SubagentDispatchLimiter extends Context.Service<
  SubagentDispatchLimiter,
  SubagentDispatchLimiterShape
>()("t3/mcp/toolkits/subagent/SubagentDispatchLimiter") {}

const parseMax = (value: string): number => {
  const parsed = Number.parseInt(value.trim(), 10);
  if (!Number.isFinite(parsed)) return DEFAULT_MAX_CONCURRENT_DISPATCHES;
  return Math.max(1, parsed);
};

const EnvConfig = Config.string("T3CODE_SUBAGENT_MAX_CONCURRENT_DISPATCHES").pipe(
  Config.withDefault(String(DEFAULT_MAX_CONCURRENT_DISPATCHES)),
  Config.map(parseMax),
);

interface State {
  readonly activeLeases: ReadonlySet<symbol>;
  readonly childLeases: ReadonlyMap<ThreadId, symbol>;
  readonly waiters: ReadonlyArray<Waiter>;
  readonly handoffs: ReadonlyMap<symbol, symbol>;
}

interface Waiter {
  readonly id: symbol;
  readonly deferred: Deferred.Deferred<SubagentDispatchLease>;
}

const emptyState: State = {
  activeLeases: new Set(),
  childLeases: new Map(),
  waiters: [],
  handoffs: new Map(),
};

const makeWithMax = (maxConcurrentDispatches: number) =>
  Effect.gen(function* () {
    const state = yield* Ref.make<State>(emptyState);

    const makeLease = (): SubagentDispatchLease => ({
      id: Symbol("subagent-dispatch-lease"),
    });

    const releaseId = (id: symbol) =>
      Ref.modify(state, (current) => {
        if (!current.activeLeases.has(id)) return [Effect.void, current] as const;
        const activeLeases = new Set(current.activeLeases);
        activeLeases.delete(id);
        const childLeases = new Map(current.childLeases);
        for (const [childThreadId, leaseId] of childLeases) {
          if (leaseId === id) childLeases.delete(childThreadId);
        }
        const [nextWaiter, ...remainingWaiters] = current.waiters;
        const handoffs = new Map(current.handoffs);
        if (nextWaiter !== undefined && activeLeases.size < maxConcurrentDispatches) {
          const nextLease = makeLease();
          activeLeases.add(nextLease.id);
          handoffs.set(nextWaiter.id, nextLease.id);
          return [
            Deferred.succeed(nextWaiter.deferred, nextLease).pipe(Effect.asVoid),
            { activeLeases, childLeases, waiters: remainingWaiters, handoffs },
          ] as const;
        }
        return [
          Effect.void,
          { activeLeases, childLeases, waiters: current.waiters, handoffs },
        ] as const;
      }).pipe(Effect.flatten);

    const clearWaiterHandoff = (waiterId: symbol) =>
      Ref.update(state, (current) => {
        if (!current.handoffs.has(waiterId)) return current;
        const handoffs = new Map(current.handoffs);
        handoffs.delete(waiterId);
        return {
          ...current,
          handoffs,
        };
      });

    const cancelWaiter = (waiter: Waiter) =>
      Ref.modify(state, (current) => {
        const waiters = current.waiters.filter((candidate) => candidate.id !== waiter.id);
        const handedLeaseId = current.handoffs.get(waiter.id);
        if (handedLeaseId === undefined) {
          return [Effect.void, { ...current, waiters }] as const;
        }
        const handoffs = new Map(current.handoffs);
        handoffs.delete(waiter.id);
        return [releaseId(handedLeaseId), { ...current, waiters, handoffs }] as const;
      }).pipe(Effect.flatten);

    const acquire = Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const waiter: Waiter = {
          id: Symbol("subagent-dispatch-waiter"),
          deferred: yield* Deferred.make<SubagentDispatchLease>(),
        };
        const granted = yield* Ref.modify(state, (current) => {
          if (current.activeLeases.size < maxConcurrentDispatches && current.waiters.length === 0) {
            const lease = makeLease();
            return [
              lease,
              {
                activeLeases: new Set(current.activeLeases).add(lease.id),
                childLeases: current.childLeases,
                waiters: current.waiters,
                handoffs: current.handoffs,
              },
            ] as const;
          }
          return [
            null,
            {
              activeLeases: current.activeLeases,
              childLeases: current.childLeases,
              waiters: [...current.waiters, waiter],
              handoffs: current.handoffs,
            },
          ] as const;
        });
        if (granted !== null) return granted;
        return yield* restore(Deferred.await(waiter.deferred)).pipe(
          Effect.tap(() => clearWaiterHandoff(waiter.id)),
          Effect.onInterrupt(() => cancelWaiter(waiter)),
        );
      }),
    );

    const bindChild: SubagentDispatchLimiterShape["bindChild"] = (lease, childThreadId) =>
      Ref.update(state, (current) => {
        if (!current.activeLeases.has(lease.id)) return current;
        const childLeases = new Map(current.childLeases);
        childLeases.set(childThreadId, lease.id);
        return {
          activeLeases: current.activeLeases,
          childLeases,
          waiters: current.waiters,
          handoffs: current.handoffs,
        };
      });

    const seedChild: SubagentDispatchLimiterShape["seedChild"] = (childThreadId) =>
      Ref.update(state, (current) => {
        if (current.childLeases.has(childThreadId)) return current;
        const lease = makeLease();
        const activeLeases = new Set(current.activeLeases);
        activeLeases.add(lease.id);
        const childLeases = new Map(current.childLeases);
        childLeases.set(childThreadId, lease.id);
        return {
          activeLeases,
          childLeases,
          waiters: current.waiters,
          handoffs: current.handoffs,
        };
      });

    const releaseForChild: SubagentDispatchLimiterShape["releaseForChild"] = (childThreadId) =>
      Ref.get(state).pipe(
        Effect.flatMap((current) => {
          const id = current.childLeases.get(childThreadId);
          return id === undefined ? Effect.void : releaseId(id);
        }),
      );

    const release: SubagentDispatchLimiterShape["release"] = (lease) => releaseId(lease.id);

    return SubagentDispatchLimiter.of({
      maxConcurrentDispatches,
      acquire,
      bindChild,
      seedChild,
      release,
      releaseForChild,
    });
  });

export const make = makeWithMax;

export const layer = Layer.effect(SubagentDispatchLimiter, EnvConfig.pipe(Effect.flatMap(make)));

export const layerTest = (maxConcurrentDispatches = DEFAULT_MAX_CONCURRENT_DISPATCHES) =>
  Layer.effect(SubagentDispatchLimiter, makeWithMax(maxConcurrentDispatches));
