import {
  parseScopedThreadKey,
  scopeProjectRef,
  scopeThreadRef,
} from "@t3tools/client-runtime/environment";
import { settlePromise } from "@t3tools/client-runtime/state/runtime";
import { EnvironmentId, type ScopedThreadRef, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Schema from "effect/Schema";
import { AsyncResult } from "effect/unstable/reactivity";
import { useRouter } from "@tanstack/react-router";
import { useCallback, useMemo, useRef } from "react";

import { getFallbackThreadIdAfterDelete } from "../components/Sidebar.logic";
import { useComposerDraftStore } from "../composerDraftStore";
import { terminalEnvironment } from "../state/terminal";
import { threadEnvironment } from "../state/threads";
import { useNewThreadHandler } from "./useHandleNewThread";
import { refreshArchivedThreadsForEnvironment } from "../lib/archivedThreadsState";
import { readLocalApi } from "../localApi";
import { readEnvironmentThreadRefs, readThreadShell } from "../state/entities";
import { useTerminalUiStateStore } from "../terminalUiStateStore";
import { buildThreadRouteParams, resolveThreadRouteRef } from "../threadRoutes";
import { useClientSettings } from "./useSettings";
import { useAtomCommand } from "../state/use-atom-command";
import { isThreadSessionActive } from "../session-logic";
import { formatWorktreePathForDisplay } from "../worktreeCleanup";

export class ThreadArchiveBlockedError extends Schema.TaggedErrorClass<ThreadArchiveBlockedError>()(
  "ThreadArchiveBlockedError",
  {
    environmentId: EnvironmentId,
    threadId: ThreadId,
  },
) {
  override get message(): string {
    return "Cannot archive a running thread.";
  }
}

export interface ThreadDeleteConfirmationSubject {
  readonly title: string;
  readonly worktreePath: string | null;
  readonly worktreeRemovable?: boolean | undefined;
  readonly worktreeRemovalPath?: string | null | undefined;
}

export function ownedWorktreePathForThreadDelete(
  input: ThreadDeleteConfirmationSubject,
): string | null {
  return input.worktreeRemovable === true
    ? (input.worktreeRemovalPath ?? input.worktreePath)
    : null;
}

export function isThreadDeleteConfirmationCurrent(
  confirmedOwnedWorktreePath: string | null | undefined,
  subject: ThreadDeleteConfirmationSubject,
): boolean {
  return confirmedOwnedWorktreePath === ownedWorktreePathForThreadDelete(subject);
}

export function threadDeleteRequiresConfirmation(
  confirmHistoryDeletion: boolean,
  subject: ThreadDeleteConfirmationSubject,
): boolean {
  return confirmHistoryDeletion || ownedWorktreePathForThreadDelete(subject) !== null;
}

export function buildThreadDeleteConfirmationMessage(
  input: ThreadDeleteConfirmationSubject,
): string {
  const lines = [
    `Delete thread "${input.title}"?`,
    "This permanently clears conversation history for this thread.",
  ];
  const ownedWorktreePath = ownedWorktreePathForThreadDelete(input);
  if (ownedWorktreePath) {
    lines.push(
      "",
      "This also permanently deletes its T3-created worktree when no other thread or project uses it:",
      formatWorktreePathForDisplay(ownedWorktreePath),
      "Uncommitted and untracked files in that worktree will be lost.",
    );
  }
  return lines.join("\n");
}

export function buildThreadsDeleteConfirmationMessage(
  inputs: ReadonlyArray<{
    readonly worktreePath: string | null;
    readonly worktreeRemovable?: boolean | undefined;
    readonly worktreeRemovalPath?: string | null | undefined;
  }>,
): string {
  const lines = [
    `Delete ${inputs.length} thread${inputs.length === 1 ? "" : "s"}?`,
    "This permanently clears conversation history for these threads.",
  ];
  const ownedWorktreePaths = [
    ...new Set(
      inputs.flatMap((input) => {
        const path = ownedWorktreePathForThreadDelete({ title: "", ...input });
        return path ? [path] : [];
      }),
    ),
  ];
  if (ownedWorktreePaths.length > 0) {
    lines.push(
      "",
      "This also permanently deletes these T3-created worktrees when no other thread or project uses them:",
      ...ownedWorktreePaths.map((path) => `- ${formatWorktreePathForDisplay(path)}`),
      "Uncommitted and untracked files in those worktrees will be lost.",
    );
  }
  return lines.join("\n");
}

export function useThreadActions() {
  const closeTerminal = useAtomCommand(terminalEnvironment.close);
  const archiveThreadMutation = useAtomCommand(threadEnvironment.archive, {
    reportFailure: false,
  });
  const unarchiveThreadMutation = useAtomCommand(threadEnvironment.unarchive, {
    reportFailure: false,
  });
  const deleteThreadMutation = useAtomCommand(threadEnvironment.delete, {
    reportFailure: false,
  });
  const stopThreadSession = useAtomCommand(threadEnvironment.stopSession);
  const sidebarThreadSortOrder = useClientSettings((settings) => settings.sidebarThreadSortOrder);
  const confirmThreadDelete = useClientSettings((settings) => settings.confirmThreadDelete);
  const clearComposerDraftForThread = useComposerDraftStore((store) => store.clearDraftThread);
  const clearProjectDraftThreadById = useComposerDraftStore(
    (store) => store.clearProjectDraftThreadById,
  );
  const clearTerminalUiState = useTerminalUiStateStore((state) => state.clearTerminalUiState);
  const router = useRouter();
  const handleNewThread = useNewThreadHandler();
  // Keep a ref so archiveThread can call handleNewThread without appearing in
  // its dependency array — handleNewThread is inherently unstable (depends on
  // the projects list) and would otherwise cascade new references into every
  // sidebar row via archiveThread → attemptArchiveThread.
  const handleNewThreadRef = useRef(handleNewThread);
  handleNewThreadRef.current = handleNewThread;

  const resolveThreadTarget = useCallback((target: ScopedThreadRef) => {
    const thread = readThreadShell(target);
    if (!thread) {
      return null;
    }
    return {
      thread,
      threadRef: target,
    };
  }, []);
  const getCurrentRouteThreadRef = useCallback(() => {
    const currentRouteParams = router.state.matches[router.state.matches.length - 1]?.params ?? {};
    return resolveThreadRouteRef(currentRouteParams);
  }, [router]);

  const archiveThread = useCallback(
    async (target: ScopedThreadRef) => {
      const resolved = resolveThreadTarget(target);
      if (!resolved) return AsyncResult.success(undefined);
      const { thread, threadRef } = resolved;
      if (isThreadSessionActive(thread.session)) {
        return AsyncResult.failure(
          Cause.fail(
            new ThreadArchiveBlockedError({
              environmentId: threadRef.environmentId,
              threadId: threadRef.threadId,
            }),
          ),
        );
      }

      const currentRouteThreadRef = getCurrentRouteThreadRef();
      const shouldNavigateToDraft =
        currentRouteThreadRef?.threadId === threadRef.threadId &&
        currentRouteThreadRef.environmentId === threadRef.environmentId;
      const archiveResult = await archiveThreadMutation({
        environmentId: threadRef.environmentId,
        input: { threadId: threadRef.threadId },
      });
      if (archiveResult._tag === "Failure") {
        return archiveResult;
      }

      if (shouldNavigateToDraft) {
        const navigationResult = await settlePromise(() =>
          handleNewThreadRef.current(scopeProjectRef(thread.environmentId, thread.projectId)),
        );
        if (navigationResult._tag === "Failure") {
          return navigationResult;
        }
        refreshArchivedThreadsForEnvironment(threadRef.environmentId);
        return archiveResult;
      }

      refreshArchivedThreadsForEnvironment(threadRef.environmentId);
      return archiveResult;
    },
    [archiveThreadMutation, getCurrentRouteThreadRef, resolveThreadTarget],
  );

  const unarchiveThread = useCallback(
    async (target: ScopedThreadRef) => {
      const result = await unarchiveThreadMutation({
        environmentId: target.environmentId,
        input: { threadId: target.threadId },
      });
      if (result._tag === "Success") {
        refreshArchivedThreadsForEnvironment(target.environmentId);
      }
      return result;
    },
    [unarchiveThreadMutation],
  );

  const deleteThread = useCallback(
    async (
      target: ScopedThreadRef,
      opts: {
        deletedThreadKeys?: ReadonlySet<string>;
        confirmedOwnedWorktreePath?: string | null;
        confirmationSubject?: ThreadDeleteConfirmationSubject;
      } = {},
    ) => {
      let resolved = resolveThreadTarget(target);
      const localApi = readLocalApi();
      if (localApi) {
        const fallbackSubject = opts.confirmationSubject ?? {
          title: "this thread",
          worktreePath: null,
          worktreeRemovable: false,
          worktreeRemovalPath: null,
        };
        let confirmedOwnedWorktreePath = opts.confirmedOwnedWorktreePath;
        while (true) {
          const confirmationSubject = resolved?.thread ?? fallbackSubject;
          const currentOwnedWorktreePath = ownedWorktreePathForThreadDelete(confirmationSubject);
          if (
            threadDeleteRequiresConfirmation(confirmThreadDelete, confirmationSubject) &&
            !isThreadDeleteConfirmationCurrent(confirmedOwnedWorktreePath, confirmationSubject)
          ) {
            const confirmationResult = await settlePromise(() =>
              localApi.dialogs.confirm(buildThreadDeleteConfirmationMessage(confirmationSubject)),
            );
            if (confirmationResult._tag === "Failure") {
              return confirmationResult;
            }
            if (!confirmationResult.value) {
              return AsyncResult.success({ deleted: false as const });
            }
            confirmedOwnedWorktreePath = currentOwnedWorktreePath;
          }

          const refreshed = resolveThreadTarget(target);
          const refreshedOwnedWorktreePath = ownedWorktreePathForThreadDelete(
            refreshed?.thread ?? fallbackSubject,
          );
          if (refreshedOwnedWorktreePath === currentOwnedWorktreePath) {
            resolved = refreshed;
            break;
          }
          resolved = refreshed;
        }
      }
      if (!resolved) {
        // Thread not in main store (e.g. archived thread) — dispatch delete directly.
        const result = await deleteThreadMutation({
          environmentId: target.environmentId,
          input: { threadId: target.threadId },
        });
        if (result._tag === "Success") {
          refreshArchivedThreadsForEnvironment(target.environmentId);
          return AsyncResult.success({ deleted: true as const });
        }
        return result;
      }
      const { thread, threadRef } = resolved;
      const threads = readEnvironmentThreadRefs(threadRef.environmentId).flatMap((ref) => {
        const shell = readThreadShell(ref);
        return shell === null ? [] : [shell];
      });
      const deletedThreadIds =
        opts.deletedThreadKeys && opts.deletedThreadKeys.size > 0
          ? new Set<ThreadId>(
              [...opts.deletedThreadKeys].flatMap((threadKey) => {
                const ref = parseScopedThreadKey(threadKey);
                return ref && ref.environmentId === threadRef.environmentId ? [ref.threadId] : [];
              }),
            )
          : new Set<ThreadId>();
      if (thread.session && thread.session.status !== "stopped") {
        await stopThreadSession({
          environmentId: threadRef.environmentId,
          input: { threadId: threadRef.threadId },
        });
      }

      await closeTerminal({
        environmentId: threadRef.environmentId,
        input: { threadId: threadRef.threadId, deleteHistory: true },
      });

      const currentRouteThreadRef = getCurrentRouteThreadRef();
      const shouldNavigateToFallback =
        currentRouteThreadRef?.threadId === threadRef.threadId &&
        currentRouteThreadRef.environmentId === threadRef.environmentId;
      const fallbackThreadId = getFallbackThreadIdAfterDelete({
        threads,
        deletedThreadId: threadRef.threadId,
        deletedThreadIds,
        sortOrder: sidebarThreadSortOrder,
      });
      const deleteResult = await deleteThreadMutation({
        environmentId: threadRef.environmentId,
        input: { threadId: threadRef.threadId },
      });
      if (deleteResult._tag === "Failure") {
        return deleteResult;
      }
      refreshArchivedThreadsForEnvironment(threadRef.environmentId);
      clearComposerDraftForThread(threadRef);
      clearProjectDraftThreadById(
        scopeProjectRef(threadRef.environmentId, thread.projectId),
        threadRef,
      );
      clearTerminalUiState(threadRef);

      if (shouldNavigateToFallback) {
        if (fallbackThreadId) {
          const fallbackThread = readThreadShell(
            scopeThreadRef(threadRef.environmentId, fallbackThreadId),
          );
          if (fallbackThread) {
            const navigationResult = await settlePromise(() =>
              router.navigate({
                to: "/$environmentId/$threadId",
                params: buildThreadRouteParams(
                  scopeThreadRef(fallbackThread.environmentId, fallbackThread.id),
                ),
                replace: true,
              }),
            );
            if (navigationResult._tag === "Failure") {
              return navigationResult;
            }
          } else {
            const navigationResult = await settlePromise(() =>
              router.navigate({ to: "/", replace: true }),
            );
            if (navigationResult._tag === "Failure") {
              return navigationResult;
            }
          }
        } else {
          const navigationResult = await settlePromise(() =>
            router.navigate({ to: "/", replace: true }),
          );
          if (navigationResult._tag === "Failure") {
            return navigationResult;
          }
        }
      }

      return AsyncResult.success({ deleted: true as const });
    },
    [
      clearComposerDraftForThread,
      clearProjectDraftThreadById,
      clearTerminalUiState,
      closeTerminal,
      confirmThreadDelete,
      deleteThreadMutation,
      getCurrentRouteThreadRef,
      router,
      resolveThreadTarget,
      sidebarThreadSortOrder,
      stopThreadSession,
    ],
  );

  const confirmAndDeleteThread = useCallback(
    (target: ScopedThreadRef, confirmationSubject?: ThreadDeleteConfirmationSubject) =>
      deleteThread(target, confirmationSubject ? { confirmationSubject } : {}),
    [deleteThread],
  );

  return useMemo(
    () => ({
      archiveThread,
      unarchiveThread,
      deleteThread,
      confirmAndDeleteThread,
    }),
    [archiveThread, confirmAndDeleteThread, deleteThread, unarchiveThread],
  );
}
