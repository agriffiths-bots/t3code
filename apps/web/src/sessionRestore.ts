import type { EnvironmentConnectionPhase } from "@t3tools/client-runtime/connection";

export const SESSION_RESTORE_TIMEOUT_MS = 10_000;
export const SESSION_CONNECT_TIMEOUT_MS = 15_000;

export type SessionRestoreResolution =
  | { readonly kind: "connecting" }
  | { readonly kind: "connection-error" }
  | { readonly kind: "restoring" }
  | { readonly kind: "restore-error" }
  | { readonly kind: "stale" }
  | { readonly kind: "ready" };

export type SessionRestoreWaitingStage = "connecting" | "restoring";

export function sessionRestoreWaitingStage(
  resolution: SessionRestoreResolution,
): SessionRestoreWaitingStage | null {
  return resolution.kind === "connecting" || resolution.kind === "restoring"
    ? resolution.kind
    : null;
}

export function resolveSessionDetailStatus(input: {
  readonly deleted: boolean;
  readonly hasDetail: boolean;
  readonly hasError: boolean;
}): "pending" | "ready" | "deleted" | "error" {
  if (input.deleted) return "deleted";
  if (input.hasDetail) return "ready";
  return input.hasError ? "error" : "pending";
}

export function shouldSubscribeToServerThread(input: {
  readonly draftExists: boolean;
  readonly draftPromoted: boolean;
  readonly shellPresent: boolean;
}): boolean {
  return !input.draftExists || input.draftPromoted || input.shellPresent;
}

export function resolveSessionRestore(input: {
  readonly catalogReady: boolean;
  readonly environmentPresent: boolean;
  readonly connectionPhase: EnvironmentConnectionPhase | null;
  readonly shellAuthoritative: boolean;
  readonly shellHasThread: boolean;
  readonly draftExists: boolean;
  readonly detailStatus?: "pending" | "ready" | "deleted" | "error";
  readonly timedOut: boolean;
}): SessionRestoreResolution {
  if (!input.catalogReady) {
    return input.timedOut ? { kind: "connection-error" } : { kind: "connecting" };
  }
  if (!input.environmentPresent) {
    return { kind: "stale" };
  }
  if (input.connectionPhase !== "connected") {
    if (input.connectionPhase === "error" || input.timedOut) {
      return { kind: "connection-error" };
    }
    return { kind: "connecting" };
  }
  if (input.draftExists) {
    return { kind: "ready" };
  }
  if (!input.shellHasThread) {
    if (!input.shellAuthoritative) {
      return input.timedOut ? { kind: "restore-error" } : { kind: "restoring" };
    }
    return { kind: "stale" };
  }
  switch (input.detailStatus) {
    case "ready":
      return { kind: "ready" };
    case "deleted":
      return { kind: "stale" };
    case "error":
      return input.timedOut ? { kind: "restore-error" } : { kind: "restoring" };
    case "pending":
    case undefined:
      return input.timedOut ? { kind: "restore-error" } : { kind: "restoring" };
  }

  return { kind: "restore-error" };
}
