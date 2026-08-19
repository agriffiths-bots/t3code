import type { ContextMenuItem, ThreadId } from "@t3tools/contracts";
import type { SnoozePreset } from "@t3tools/client-runtime/state/thread-settled";

/**
 * Ids for the per-thread action menu. Snooze presets are dispatched as
 * `snooze:<presetId>` so the union stays closed while the preset list
 * remains data-driven.
 */
export type ThreadActionMenuId =
  | "new-thread-on-branch"
  | "pin"
  | "unpin"
  | "settle"
  | "unsettle"
  | "snooze"
  | `snooze:${string}`
  | "unsnooze"
  | MatrixBridgeMenuId
  | "rename"
  | "regenerate-title"
  | "mark-unread"
  | "copy-path"
  | "copy-branch"
  | "delete";

/**
 * Claiming and releasing the single Matrix bridge owner. Claiming is one
 * server call whether or not another thread holds the bridge, so both labels
 * dispatch the same id.
 */
export type MatrixBridgeMenuId = "bridge-to-matrix" | "unbridge-matrix";

/**
 * What this client knows about the bridge owner, which is not the same as
 * "there is no owner":
 *
 * - `unconfigured` — the server holds no bridge configuration, so `setOwner`
 *   would reject with `notConfigured`. There is nothing to offer.
 * - `unknown` — this client cannot read the status stream (it needs
 *   `orchestration:read`, which an operate-only session may lack), or the
 *   first snapshot has not arrived. Guessing "no owner" here would offer
 *   "Bridge to Matrix" on the thread that already owns the bridge and would
 *   hide the only way to stop it.
 * - `owner` — the status stream is readable and this is its owner pointer.
 */
export type MatrixBridgeOwnership =
  | { readonly kind: "unconfigured" }
  | { readonly kind: "unknown" }
  | { readonly kind: "owner"; readonly ownerThreadId: ThreadId | null };

export interface MatrixBridgeMenuState {
  /** `environment.capabilities.matrixBridge`. Absent or false hides the item. */
  readonly supported: boolean;
  /**
   * Whether this client's session on that environment may call
   * `matrixBridge.setOwner`, which needs `orchestration:operate`. A read-scoped
   * client can watch the bridge, so capability alone is not enough to offer a
   * control that would always be rejected.
   */
  readonly canOperate: boolean;
  readonly ownership: MatrixBridgeOwnership;
  /** The thread this menu was opened on. */
  readonly threadId: ThreadId;
}

/**
 * The Matrix bridge item on its own, so the legacy sidebar's separate menu
 * renders exactly what the shared menu renders instead of a second copy that
 * can drift.
 *
 * Every item this returns is one the server would accept. An action that is
 * certain to fail is not offered, and an action whose correctness cannot be
 * established is shown disabled rather than guessed at.
 */
export function buildMatrixBridgeMenuItems(
  state: MatrixBridgeMenuState,
): ReadonlyArray<ContextMenuItem<MatrixBridgeMenuId>> {
  if (!state.supported || !state.canOperate || state.ownership.kind === "unconfigured") {
    return [];
  }
  if (state.ownership.kind === "unknown") {
    return [
      {
        id: "bridge-to-matrix",
        label: "Matrix bridge status unavailable",
        disabled: true,
      },
    ];
  }
  if (state.ownership.ownerThreadId === state.threadId) {
    return [{ id: "unbridge-matrix", label: "Stop Matrix bridge" }];
  }
  return [
    {
      id: "bridge-to-matrix",
      label:
        state.ownership.ownerThreadId === null ? "Bridge to Matrix" : "Move Matrix bridge here",
    },
  ];
}

export interface ThreadActionMenuState {
  readonly branch: string | null;
  readonly isPinned: boolean;
  readonly isSettled: boolean;
  readonly isSnoozed: boolean;
  readonly canSnoozeNow: boolean;
  readonly isRegeneratingTitle: boolean;
  readonly supports: {
    readonly settlement: boolean;
    readonly snooze: boolean;
    readonly pinning: boolean;
    readonly titleRegeneration: boolean;
  };
  readonly snoozePresets: ReadonlyArray<SnoozePreset>;
  readonly matrixBridge: MatrixBridgeMenuState;
}

/**
 * Single source for the per-thread action menu: the sidebar row's right-click
 * menu and the chat header menu both render exactly this list, so labels,
 * ordering, and capability gating cannot drift between the two surfaces.
 */
export function buildThreadActionMenuItems(
  state: ThreadActionMenuState,
): ReadonlyArray<ContextMenuItem<ThreadActionMenuId>> {
  return [
    ...(state.branch
      ? [
          {
            id: "new-thread-on-branch" as const,
            label: `New thread on ${state.branch}`,
          },
        ]
      : []),
    ...(state.supports.pinning
      ? [
          state.isPinned
            ? { id: "unpin" as const, label: "Unpin thread" }
            : { id: "pin" as const, label: "Pin thread" },
        ]
      : []),
    // Both lifecycle actions stay available on pinned threads: settling
    // clears the pin ("done" beats "keep on top"), and snoozing hides the
    // card until wake with the pin intact.
    ...(state.supports.settlement
      ? [
          state.isSettled
            ? { id: "unsettle" as const, label: "Un-settle thread" }
            : { id: "settle" as const, label: "Settle thread" },
        ]
      : []),
    ...(state.supports.snooze
      ? [
          state.isSnoozed
            ? { id: "unsnooze" as const, label: "Wake thread" }
            : {
                id: "snooze" as const,
                label: "Snooze",
                disabled: !state.canSnoozeNow,
                children: state.snoozePresets.map((preset) => ({
                  id: `snooze:${preset.id}` as const,
                  label: `${preset.label} (${preset.whenLabel})`,
                })),
              },
        ]
      : []),
    // Bridge ownership sits with the other lifecycle actions: it changes what
    // the thread is for, not what you can copy out of it.
    ...buildMatrixBridgeMenuItems(state.matrixBridge),
    { id: "rename", label: "Rename thread" },
    ...(state.supports.titleRegeneration
      ? [
          {
            id: "regenerate-title" as const,
            label: state.isRegeneratingTitle ? "Regenerating…" : "Regenerate title",
            disabled: state.isRegeneratingTitle,
          },
        ]
      : []),
    { id: "mark-unread", label: "Mark unread" },
    { id: "copy-path", label: "Copy path", icon: "copy" },
    ...(state.branch ? [{ id: "copy-branch" as const, label: "Copy branch", icon: "copy" }] : []),
    { id: "delete", label: "Delete", destructive: true, icon: "trash" },
  ];
}
