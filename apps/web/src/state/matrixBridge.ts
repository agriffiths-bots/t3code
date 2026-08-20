import { useAtomValue } from "@effect/atom-react";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
  createEnvironmentRpcSubscriptionAtomFamily,
} from "@t3tools/client-runtime/state/runtime";
import {
  AuthOrchestrationOperateScope,
  WS_METHODS,
  type AuthSessionState,
  type EnvironmentId,
  type MatrixBridgeConfigView,
  type MatrixBridgeStatus,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { AsyncResult, Atom, AtomRegistry } from "effect/unstable/reactivity";

import { environmentCatalog } from "../connection/catalog";
import { connectionAtomRuntime } from "../connection/runtime";
import type {
  MatrixBridgeMenuState,
  MatrixBridgeOwnership,
} from "../components/threadActionMenu.logic";
import { environmentServerConfigsAtom } from "./server";
import { environmentSession } from "./session";

/**
 * Matrix bridge controls for the sidebar menus and the Connections panel.
 *
 * The server is the only authority for the bridge: one configuration and at
 * most one owner thread per environment. Everything here is a thin view over
 * `matrixBridge.*` RPCs, so a stale client view can mislabel a control but can
 * never invent bridge state.
 *
 * `configure` and `disconnect` need `access:write`, `getConfig` needs
 * `access:read`, `setOwner` needs `orchestration:operate`, and the status
 * subscription needs `orchestration:read`. The bot access token is write-only:
 * it goes out with `configure` and is never returned by any of these calls.
 */
/**
 * The saved connection, for repopulating the Connections form after a reload.
 * A session without `access:read` simply fails this query and keeps the blank
 * form it had before, which is why nothing else depends on it.
 */
export const matrixBridgeSavedConfig = createEnvironmentRpcQueryAtomFamily(connectionAtomRuntime, {
  label: "environment-data:matrix-bridge:get-config",
  tag: WS_METHODS.matrixBridgeGetConfig,
  staleTimeMs: 5_000,
  idleTtlMs: 60_000,
});

/**
 * Invalidates one environment's saved connection.
 *
 * The query is read through `matrixBridgeStatesAtom`, which every bridge
 * control subscribes to, so it stays mounted for the life of the app and its
 * cached answer would otherwise outlive the change that made it wrong.
 */
const refreshSavedConfig = (
  environmentId: EnvironmentId,
  registry: AtomRegistry.AtomRegistry,
): Effect.Effect<void> =>
  Effect.sync(() => {
    registry.refresh(matrixBridgeSavedConfig({ environmentId, input: {} }));
  });

export const matrixBridgeEnvironment = {
  // Both writes change what `getConfig` would answer, so both refetch it here
  // rather than in a caller: a form opened later must not hydrate from the
  // connection this replaced or removed.
  configure: createEnvironmentRpcCommand(connectionAtomRuntime, {
    label: "environment-data:matrix-bridge:configure",
    tag: WS_METHODS.matrixBridgeConfigure,
    onSuccess: ({ environmentId }, registry) => refreshSavedConfig(environmentId, registry),
  }),
  disconnect: createEnvironmentRpcCommand(connectionAtomRuntime, {
    label: "environment-data:matrix-bridge:disconnect",
    tag: WS_METHODS.matrixBridgeDisconnect,
    onSuccess: ({ environmentId }, registry) => refreshSavedConfig(environmentId, registry),
  }),
  setOwner: createEnvironmentRpcCommand(connectionAtomRuntime, {
    label: "environment-data:matrix-bridge:set-owner",
    tag: WS_METHODS.matrixBridgeSetOwner,
  }),
};

/** The saved connection when one is readable, and nothing otherwise. */
export function matrixBridgeSavedConfigView(
  result: AsyncResult.AsyncResult<{ readonly config: MatrixBridgeConfigView | null }, unknown>,
): MatrixBridgeConfigView | null {
  return AsyncResult.isSuccess(result) ? result.value.config : null;
}

const matrixBridgeStatusAtom = createEnvironmentRpcSubscriptionAtomFamily(connectionAtomRuntime, {
  label: "environment-data:matrix-bridge:status",
  tag: WS_METHODS.matrixBridgeSubscribeStatus,
});

/**
 * What this client knows about one environment's bridge. `pending` and
 * `unavailable` are kept apart on purpose: the status stream needs
 * `orchestration:read`, which a client holding only `access:write` or
 * `orchestration:operate` may lack, and collapsing its failure into "no
 * bridge" would hide management controls that session is entitled to use.
 */
export type MatrixBridgeStatusView =
  | { readonly kind: "pending" }
  | { readonly kind: "unavailable" }
  | { readonly kind: "status"; readonly status: MatrixBridgeStatus };

const PENDING_STATUS_VIEW: MatrixBridgeStatusView = { kind: "pending" };
const UNAVAILABLE_STATUS_VIEW: MatrixBridgeStatusView = { kind: "unavailable" };

/** What one environment's server allows this client to see and do. */
export interface MatrixBridgeEnvironmentState {
  readonly statusView: MatrixBridgeStatusView;
  /** The saved connection when this session may read it, and null otherwise. */
  readonly savedConfig: MatrixBridgeConfigView | null;
  readonly canOperate: boolean;
}

export function matrixBridgeStatusView(
  result: AsyncResult.AsyncResult<MatrixBridgeStatus, unknown>,
): MatrixBridgeStatusView {
  if (AsyncResult.isFailure(result)) {
    return UNAVAILABLE_STATUS_VIEW;
  }
  return Option.match(AsyncResult.value(result), {
    onNone: () => PENDING_STATUS_VIEW,
    onSome: (status) => ({ kind: "status", status }) as const,
  });
}

/**
 * Whether this client may move the bridge. `matrixBridge.setOwner` needs
 * `orchestration:operate` while the status stream needs only
 * `orchestration:read`, so a read-scoped client can watch the bridge and must
 * not be offered controls that would always be rejected.
 *
 * Optimistic unless the environment positively reports scopes without operate:
 * an unfetched session or a server too old to report scopes must not hide a
 * control the RPC layer would accept, and the RPC layer stays authoritative
 * either way. Matches how provider settings treat remote sessions.
 */
export function canOperateMatrixBridge(session: Pick<AuthSessionState, "scopes"> | null): boolean {
  if (session === null || session.scopes === undefined) {
    return true;
  }
  return session.scopes.includes(AuthOrchestrationOperateScope);
}

/**
 * One status subscription per capable environment. Environments whose server
 * does not advertise `matrixBridge` get no entry at all, so older backends
 * never see an unknown RPC method and their menus stay unchanged.
 */
export const matrixBridgeStatesAtom = Atom.make((get) => {
  const serverConfigs = get(environmentServerConfigsAtom);
  const states = new Map<EnvironmentId, MatrixBridgeEnvironmentState>();
  for (const environmentId of get(environmentCatalog.catalogValueAtom).entries.keys()) {
    if (serverConfigs.get(environmentId)?.environment.capabilities.matrixBridge !== true) {
      continue;
    }
    states.set(environmentId, {
      statusView: matrixBridgeStatusView(get(matrixBridgeStatusAtom({ environmentId, input: {} }))),
      savedConfig: matrixBridgeSavedConfigView(
        get(matrixBridgeSavedConfig({ environmentId, input: {} })),
      ),
      canOperate: canOperateMatrixBridge(
        get(environmentSession.sessionStateValueAtom(environmentId)),
      ),
    });
  }
  return states as ReadonlyMap<EnvironmentId, MatrixBridgeEnvironmentState>;
}).pipe(Atom.withLabel("matrix-bridge-states"));

/**
 * Bridge state is per environment: a thread only ever reads the bridge of the
 * server that owns it, never another connected backend's. A missing entry is
 * an environment without the capability, which renders no bridge controls.
 */
/**
 * `disabled` is the one lifecycle state that means the server holds no bridge
 * configuration at all, so `setOwner` would reject it with `notConfigured`.
 * Every other state has a stored configuration and a meaningful owner pointer.
 */
export function matrixBridgeOwnership(view: MatrixBridgeStatusView): MatrixBridgeOwnership {
  if (view.kind !== "status") {
    return { kind: "unknown" };
  }
  if (view.status.state === "disabled") {
    return { kind: "unconfigured" };
  }
  return { kind: "owner", ownerThreadId: view.status.ownerThreadId };
}

export function selectMatrixBridgeMenuState(
  states: ReadonlyMap<EnvironmentId, MatrixBridgeEnvironmentState>,
  threadRef: ScopedThreadRef,
): MatrixBridgeMenuState {
  const state = states.get(threadRef.environmentId);
  return {
    supported: state !== undefined,
    canOperate: state?.canOperate ?? false,
    ownership: matrixBridgeOwnership(state?.statusView ?? PENDING_STATUS_VIEW),
    threadId: threadRef.threadId,
  };
}

export function selectMatrixBridgeStatusView(
  states: ReadonlyMap<EnvironmentId, MatrixBridgeEnvironmentState>,
  environmentId: EnvironmentId | null,
): MatrixBridgeStatusView {
  if (environmentId === null) {
    return PENDING_STATUS_VIEW;
  }
  return states.get(environmentId)?.statusView ?? PENDING_STATUS_VIEW;
}

function errorText(error: unknown): string {
  if (typeof error === "string") {
    return error;
  }
  if (typeof error === "object" && error !== null) {
    const message = (error as { readonly message?: unknown }).message;
    if (typeof message === "string") {
      return message;
    }
  }
  return "";
}

/**
 * Failure text for a bridge RPC rejection. `MatrixBridgeOperationError`
 * messages are written by the server to be operator-safe (no token, room id,
 * or Matrix payload), so they can be surfaced verbatim; anything else falls
 * back rather than leaking an unshaped cause into a toast.
 */
export function matrixBridgeFailureMessage(error: unknown, fallback: string): string {
  const message = errorText(error).trim();
  return message.length > 0 ? message : fallback;
}

export function useMatrixBridgeStates(): ReadonlyMap<EnvironmentId, MatrixBridgeEnvironmentState> {
  return useAtomValue(matrixBridgeStatesAtom);
}

export function useMatrixBridgeStatusView(
  environmentId: EnvironmentId | null,
): MatrixBridgeStatusView {
  return selectMatrixBridgeStatusView(useMatrixBridgeStates(), environmentId);
}

export function selectMatrixBridgeSavedConfig(
  states: ReadonlyMap<EnvironmentId, MatrixBridgeEnvironmentState>,
  environmentId: EnvironmentId | null,
): MatrixBridgeConfigView | null {
  return environmentId === null ? null : (states.get(environmentId)?.savedConfig ?? null);
}

export function useMatrixBridgeSavedConfig(
  environmentId: EnvironmentId | null,
): MatrixBridgeConfigView | null {
  return selectMatrixBridgeSavedConfig(useMatrixBridgeStates(), environmentId);
}
