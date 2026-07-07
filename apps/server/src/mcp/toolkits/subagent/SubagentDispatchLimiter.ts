import type { ThreadId } from "@t3tools/contracts";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";

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
}

const emptyState: State = {
  activeLeases: new Set(),
  childLeases: new Map(),
};

const makeWithMax = (maxConcurrentDispatches: number) =>
  Effect.gen(function* () {
    const semaphore = yield* Semaphore.make(maxConcurrentDispatches);
    const state = yield* Ref.make<State>(emptyState);

    const addLease = (id: symbol) =>
      Ref.update(state, (current) => ({
        activeLeases: new Set(current.activeLeases).add(id),
        childLeases: current.childLeases,
      }));

    const releaseId = (id: symbol) =>
      Ref.modify(state, (current) => {
        if (!current.activeLeases.has(id)) return [false, current] as const;
        const activeLeases = new Set(current.activeLeases);
        activeLeases.delete(id);
        const childLeases = new Map(current.childLeases);
        for (const [childThreadId, leaseId] of childLeases) {
          if (leaseId === id) childLeases.delete(childThreadId);
        }
        return [true, { activeLeases, childLeases }] as const;
      }).pipe(Effect.flatMap((released) => (released ? semaphore.release(1) : Effect.void)));

    const acquire = Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* restore(semaphore.take(1));
        const lease = { id: Symbol("subagent-dispatch-lease") } satisfies SubagentDispatchLease;
        yield* addLease(lease.id);
        return lease;
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
      release,
      releaseForChild,
    });
  });

export const make = makeWithMax;

export const layer = Layer.effect(SubagentDispatchLimiter, EnvConfig.pipe(Effect.flatMap(make)));

export const layerTest = (maxConcurrentDispatches = DEFAULT_MAX_CONCURRENT_DISPATCHES) =>
  Layer.effect(SubagentDispatchLimiter, makeWithMax(maxConcurrentDispatches));
