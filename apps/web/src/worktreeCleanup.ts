import type { ThreadShell } from "./types";

function normalizeWorktreePath(path: string | null): string | null {
  const trimmed = path?.trim();
  if (!trimmed) {
    return null;
  }
  return trimmed;
}

function isSameOrNestedPath(candidate: string | null, root: string): boolean {
  if (!candidate) {
    return false;
  }
  const normalizedCandidate = candidate.replace(/\\/g, "/").replace(/\/+$/, "");
  const normalizedRoot = root.replace(/\\/g, "/").replace(/\/+$/, "");
  return (
    normalizedCandidate === normalizedRoot ||
    normalizedCandidate.startsWith(
      normalizedRoot.endsWith("/") ? normalizedRoot : `${normalizedRoot}/`,
    )
  );
}

export function getOrphanedWorktreePathForThread(
  threads: ReadonlyArray<
    Pick<ThreadShell, "id" | "worktreePath" | "worktreeRemovable" | "worktreeRemovalPath">
  >,
  threadId: ThreadShell["id"],
): string | null {
  const targetThread = threads.find((thread) => thread.id === threadId);
  if (!targetThread) {
    return null;
  }

  const targetWorktreePath = normalizeWorktreePath(targetThread.worktreePath);
  if (!targetWorktreePath) {
    return null;
  }
  if (targetThread.worktreeRemovable !== true) {
    return null;
  }
  const targetRemovalPath = normalizeWorktreePath(
    targetThread.worktreeRemovalPath ?? targetWorktreePath,
  );
  if (!targetRemovalPath) {
    return null;
  }

  const isShared = threads.some((thread) => {
    if (thread.id === threadId) {
      return false;
    }
    const threadWorktreePath = normalizeWorktreePath(thread.worktreePath);
    return (
      threadWorktreePath === targetWorktreePath ||
      normalizeWorktreePath(thread.worktreeRemovalPath ?? thread.worktreePath) ===
        targetRemovalPath ||
      isSameOrNestedPath(threadWorktreePath, targetRemovalPath)
    );
  });

  return isShared ? null : targetRemovalPath;
}

export function formatWorktreePathForDisplay(worktreePath: string): string {
  const trimmed = worktreePath.trim();
  if (!trimmed) {
    return worktreePath;
  }

  const normalized = trimmed.replace(/\\/g, "/").replace(/\/+$/, "");
  const parts = normalized.split("/");
  const lastPart = parts[parts.length - 1]?.trim() ?? "";
  return lastPart.length > 0 ? lastPart : trimmed;
}
