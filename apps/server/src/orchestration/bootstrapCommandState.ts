import type { OrchestrationCommand, OrchestrationThread } from "@t3tools/contracts";

type ThreadTurnStartCommand = Extract<OrchestrationCommand, { type: "thread.turn.start" }>;
export type BootstrapCreateThreadCommand = NonNullable<
  NonNullable<ThreadTurnStartCommand["bootstrap"]>["createThread"]
>;
export type BootstrapPrepareWorktreeCommand = NonNullable<
  NonNullable<ThreadTurnStartCommand["bootstrap"]>["prepareWorktree"]
>;

function toCanonicalJson(value: unknown): string {
  return JSON.stringify(toCanonicalJsonValue(value));
}

function toCanonicalJsonValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(toCanonicalJsonValue);
  }
  if (typeof value !== "object" || value === null) {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value)
      .filter(([, entryValue]) => entryValue !== undefined)
      .sort(([leftKey], [rightKey]) => leftKey.localeCompare(rightKey))
      .map(([entryKey, entryValue]) => [entryKey, toCanonicalJsonValue(entryValue)]),
  );
}

export function threadMatchesBootstrapCreate(
  thread: OrchestrationThread,
  createThread: BootstrapCreateThreadCommand,
  hasPrepareWorktree: boolean,
): boolean {
  if (thread.archivedAt !== null || thread.deletedAt !== null) return false;
  if (thread.messages.length > 0 || thread.latestTurn !== null) return false;
  if (thread.projectId !== createThread.projectId) return false;
  if (thread.createdAt !== createThread.createdAt) return false;
  if (toCanonicalJson(thread.modelSelection) !== toCanonicalJson(createThread.modelSelection)) {
    return false;
  }
  if (thread.runtimeMode !== createThread.runtimeMode) return false;
  if (thread.interactionMode !== createThread.interactionMode) return false;
  if (!hasPrepareWorktree && thread.branch !== createThread.branch) return false;
  if (!hasPrepareWorktree && thread.worktreePath !== createThread.worktreePath) return false;
  return true;
}

export function threadHasPreparedBootstrapWorktree(
  thread: OrchestrationThread,
  createThread: BootstrapCreateThreadCommand | undefined,
  prepareWorktree: BootstrapPrepareWorktreeCommand,
): boolean {
  if (thread.archivedAt !== null || thread.deletedAt !== null || thread.worktreePath === null) {
    return false;
  }
  if (prepareWorktree.branch !== undefined) {
    if (thread.branch !== prepareWorktree.branch) {
      return false;
    }
  } else if (!createThread) {
    return false;
  }
  if (
    createThread &&
    thread.branch === createThread.branch &&
    thread.worktreePath === createThread.worktreePath
  ) {
    return false;
  }
  return true;
}
