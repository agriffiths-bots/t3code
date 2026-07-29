export type ThreadAttentionBlocker = "approval" | "input" | "failed" | "working" | "plan";

type ThreadSessionLike = {
  readonly status?: string | null;
  readonly activeTurnId?: unknown | null;
} | null;

type ThreadLatestTurnLike = {
  readonly state?: string | null;
} | null;

export type ThreadAttentionInput = {
  readonly hasPendingApprovals?: boolean;
  readonly hasPendingUserInput?: boolean;
  readonly hasActionableProposedPlan?: boolean;
  readonly session?: ThreadSessionLike;
  readonly latestTurn?: ThreadLatestTurnLike;
};

export function hasActiveThreadSession(session: ThreadSessionLike): boolean {
  return (
    session?.status === "starting" ||
    session?.status === "running" ||
    (session?.status === "waiting" && session.activeTurnId != null)
  );
}

export function resolveThreadAttentionBlocker(
  thread: ThreadAttentionInput,
): ThreadAttentionBlocker | null {
  if (thread.hasPendingApprovals === true) return "approval";
  if (thread.hasPendingUserInput === true) return "input";
  if (thread.session?.status === "error" || thread.latestTurn?.state === "error") {
    return "failed";
  }
  if (hasActiveThreadSession(thread.session ?? null) || thread.latestTurn?.state === "running") {
    return "working";
  }
  if (thread.hasActionableProposedPlan === true) return "plan";
  return null;
}

export function hasThreadAttentionBlocker(thread: ThreadAttentionInput): boolean {
  return resolveThreadAttentionBlocker(thread) !== null;
}
