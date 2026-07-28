import {
  EnvironmentId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import type { Thread } from "../types";
import {
  ENVIRONMENT_UNAVAILABLE_BANNER_DEBOUNCE_MS,
  MAX_HIDDEN_MOUNTED_PREVIEW_THREADS,
  MAX_HIDDEN_MOUNTED_TERMINAL_THREADS,
  buildThreadErrorDismissKey,
  buildExpiredTerminalContextToastCopy,
  buildThreadTurnInterruptInput,
  createLocalDispatchSnapshot,
  deriveComposerSendState,
  deriveLockedProvider,
  getStartedThreadModelChangeBlockReason,
  hasQueuedSubmissionBeenObservedByShell,
  hasServerAcknowledgedLocalDispatch,
  reconcileMountedTerminalThreadIds,
  reconcilePendingLocalTerminalIds,
  reconcileTerminalIdsFromServerMetadata,
  reconcileRetainedMountedThreadIds,
  resolveVisibleServerThreadError,
  resolveSendEnvMode,
  shouldShowEnvironmentUnavailableBanner,
  shouldWriteThreadErrorToCurrentServerThread,
  threadHasEstablishedProviderBinding,
  workspaceRelativePathFromRepositoryRoot,
} from "./ChatView.logic";

const environmentId = EnvironmentId.make("environment-local");
const projectId = ProjectId.make("project-1");
const threadId = ThreadId.make("thread-1");
const now = "2026-03-29T00:00:00.000Z";

function makeThread(overrides: Partial<Thread> = {}): Thread {
  return {
    id: threadId,
    environmentId,
    projectId,
    dataAudience: "private",
    title: "Thread",
    modelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.4",
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    session: null,
    messages: [],
    turns: [],
    proposedPlans: [],
    activities: [],
    checkpoints: [],
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    deletedAt: null,
    latestTurn: null,
    branch: null,
    worktreePath: null,
    ...overrides,
  };
}

const completedTurn = {
  turnId: TurnId.make("turn-1"),
  state: "completed" as const,
  requestedAt: now,
  startedAt: "2026-03-29T00:00:01.000Z",
  completedAt: "2026-03-29T00:00:10.000Z",
  assistantMessageId: null,
};

const readySession = {
  threadId,
  status: "ready" as const,
  providerName: "codex",
  providerInstanceId: ProviderInstanceId.make("codex"),
  runtimeMode: "full-access" as const,
  activeTurnId: null,
  lastError: null,
  updatedAt: "2026-03-29T00:00:10.000Z",
};

describe("reconcileTerminalIdsFromServerMetadata", () => {
  it("preserves optimistic local opens that have not appeared in server metadata yet", () => {
    expect(
      reconcileTerminalIdsFromServerMetadata({
        serverIds: ["terminal-1"],
        clientIds: ["terminal-1", "terminal-2"],
        seenServerIds: new Set(["terminal-1"]),
      }),
    ).toBeNull();
  });

  it("removes terminals that disappear after being observed in server metadata", () => {
    expect(
      reconcileTerminalIdsFromServerMetadata({
        serverIds: ["terminal-1"],
        clientIds: ["terminal-1", "terminal-2"],
        seenServerIds: new Set(["terminal-1", "terminal-2"]),
      }),
    ).toEqual(["terminal-1"]);
  });

  it("removes observed missing terminals while preserving unseen local opens", () => {
    expect(
      reconcileTerminalIdsFromServerMetadata({
        serverIds: ["terminal-1"],
        clientIds: ["terminal-1", "terminal-2", "terminal-3"],
        seenServerIds: new Set(["terminal-1", "terminal-2"]),
      }),
    ).toEqual(["terminal-1", "terminal-3"]);
  });

  it("preserves a reused terminal id while its fresh local open is pending", () => {
    expect(
      reconcileTerminalIdsFromServerMetadata({
        serverIds: [],
        clientIds: ["terminal-1"],
        seenServerIds: new Set(["terminal-1"]),
        pendingLocalIds: new Set(["terminal-1"]),
      }),
    ).toBeNull();
  });
});

describe("reconcilePendingLocalTerminalIds", () => {
  it("tracks local additions until the server acknowledges them", () => {
    const pending = reconcilePendingLocalTerminalIds({
      pendingLocalIds: new Set(),
      previousClientIds: [],
      clientIds: ["terminal-1"],
      serverIds: new Set(),
    });
    expect([...pending]).toEqual(["terminal-1"]);

    expect(
      reconcilePendingLocalTerminalIds({
        pendingLocalIds: pending,
        previousClientIds: ["terminal-1"],
        clientIds: ["terminal-1"],
        serverIds: new Set(["terminal-1"]),
      }).has("terminal-1"),
    ).toBe(false);
  });

  it("clears pending ids that the client removes before server acknowledgement", () => {
    expect(
      reconcilePendingLocalTerminalIds({
        pendingLocalIds: new Set(["terminal-1"]),
        previousClientIds: ["terminal-1"],
        clientIds: [],
        serverIds: new Set(),
      }).has("terminal-1"),
    ).toBe(false);
  });
});

describe("workspaceRelativePathFromRepositoryRoot", () => {
  it("returns the workspace path relative to a repository root", () => {
    expect(workspaceRelativePathFromRepositoryRoot("/repo", "/repo/packages/app")).toBe(
      "packages/app",
    );
  });

  it("returns null when no repository root is known", () => {
    expect(workspaceRelativePathFromRepositoryRoot(null, "/repo/packages/app")).toBeNull();
  });
});

describe("threadHasEstablishedProviderBinding", () => {
  it("does not treat synthetic first-start errors as established provider bindings", () => {
    const thread = makeThread({
      session: {
        ...readySession,
        status: "error",
        activeTurnId: null,
        lastError: "Provider failed before the turn started.",
      },
      latestTurn: {
        ...completedTurn,
        state: "running",
        startedAt: null,
        completedAt: null,
      },
    });

    expect(threadHasEstablishedProviderBinding(thread)).toBe(false);
    expect(
      deriveLockedProvider({
        thread,
        selectedProvider: "grok",
        threadProvider: thread.session?.providerName ?? null,
      }),
    ).toBeNull();
  });

  it("treats active sessions and started turns as established provider bindings", () => {
    expect(
      threadHasEstablishedProviderBinding(
        makeThread({
          session: {
            ...readySession,
            status: "running",
            activeTurnId: TurnId.make("turn-running"),
          },
        }),
      ),
    ).toBe(true);

    expect(
      threadHasEstablishedProviderBinding(
        makeThread({
          session: {
            ...readySession,
            status: "error",
            lastError: "Provider failed after the turn started.",
          },
          latestTurn: completedTurn,
        }),
      ),
    ).toBe(true);
  });
});

describe("hasQueuedSubmissionBeenObservedByShell", () => {
  it("waits when the shell has not observed a turn at or after the queued submission", () => {
    expect(
      hasQueuedSubmissionBeenObservedByShell({
        submissionCreatedAt: "2026-03-29T00:00:05.000Z",
        latestTurn: null,
      }),
    ).toBe(false);
    expect(
      hasQueuedSubmissionBeenObservedByShell({
        submissionCreatedAt: "2026-03-29T00:00:05.000Z",
        latestTurn: {
          ...completedTurn,
          requestedAt: "2026-03-29T00:00:04.000Z",
        },
      }),
    ).toBe(false);
  });

  it("treats a shell turn requested at or after the queued submission as observed", () => {
    expect(
      hasQueuedSubmissionBeenObservedByShell({
        submissionCreatedAt: "2026-03-29T00:00:05.000Z",
        latestTurn: {
          ...completedTurn,
          requestedAt: "2026-03-29T00:00:05.000Z",
        },
      }),
    ).toBe(true);
    expect(
      hasQueuedSubmissionBeenObservedByShell({
        submissionCreatedAt: "2026-03-29T00:00:05.000Z",
        latestTurn: {
          ...completedTurn,
          requestedAt: "2026-03-29T00:00:06.000Z",
        },
      }),
    ).toBe(true);
  });
});

describe("buildThreadTurnInterruptInput", () => {
  it("targets the session's active running turn", () => {
    const activeTurnId = TurnId.make("turn-running");

    expect(
      buildThreadTurnInterruptInput(
        makeThread({
          session: {
            ...readySession,
            status: "running",
            activeTurnId,
          },
        }),
      ),
    ).toEqual({ threadId, turnId: activeTurnId });
  });

  it("targets the session's active waiting turn", () => {
    const activeTurnId = TurnId.make("turn-waiting");

    expect(
      buildThreadTurnInterruptInput(
        makeThread({
          session: {
            ...readySession,
            status: "waiting",
            activeTurnId,
          },
        }),
      ),
    ).toEqual({ threadId, turnId: activeTurnId });
  });

  it("omits a turn id when the session is not running", () => {
    expect(buildThreadTurnInterruptInput(makeThread({ session: readySession }))).toEqual({
      threadId,
    });
  });
});

describe("shouldShowEnvironmentUnavailableBanner", () => {
  it("suppresses short reconnect blips", () => {
    expect(
      shouldShowEnvironmentUnavailableBanner({
        connectionPhase: "reconnecting",
        unavailableSinceMs: 1_000,
        nowMs: 1_000 + ENVIRONMENT_UNAVAILABLE_BANNER_DEBOUNCE_MS - 1,
      }),
    ).toBe(false);
  });

  it("shows the banner once the connection has stayed unavailable", () => {
    expect(
      shouldShowEnvironmentUnavailableBanner({
        connectionPhase: "reconnecting",
        unavailableSinceMs: 1_000,
        nowMs: 1_000 + ENVIRONMENT_UNAVAILABLE_BANNER_DEBOUNCE_MS,
      }),
    ).toBe(true);
  });

  it("clears immediately when the connection is back", () => {
    expect(
      shouldShowEnvironmentUnavailableBanner({
        connectionPhase: "connected",
        unavailableSinceMs: 1_000,
        nowMs: 1_000 + ENVIRONMENT_UNAVAILABLE_BANNER_DEBOUNCE_MS,
      }),
    ).toBe(false);
  });
});

describe("resolveVisibleServerThreadError", () => {
  it("uses a dismissed key to hide a persisted server error for that turn", () => {
    const turnId = TurnId.make("turn-dismissed");
    const dismissKey = buildThreadErrorDismissKey({
      threadKey: "environment-local:thread-1",
      turnId,
      error: "The turn was interrupted. Send your message again to retry.",
    });

    expect(dismissKey).not.toBeNull();
    expect(
      resolveVisibleServerThreadError({
        localError: null,
        sessionError: "The turn was interrupted. Send your message again to retry.",
        dismissedSessionErrorKeys: { [dismissKey!]: true },
        threadKey: "environment-local:thread-1",
        turnId,
      }),
    ).toBeNull();
  });

  it("allows the same friendly interruption message to appear on a later turn", () => {
    const dismissedTurnId = TurnId.make("turn-dismissed");
    const laterTurnId = TurnId.make("turn-later");
    const dismissKey = buildThreadErrorDismissKey({
      threadKey: "environment-local:thread-1",
      turnId: dismissedTurnId,
      error: "The turn was interrupted. Send your message again to retry.",
    });

    expect(
      resolveVisibleServerThreadError({
        localError: null,
        sessionError: "The turn was interrupted. Send your message again to retry.",
        dismissedSessionErrorKeys: { [dismissKey!]: true },
        threadKey: "environment-local:thread-1",
        turnId: laterTurnId,
      }),
    ).toBe("The turn was interrupted. Send your message again to retry.");
  });

  it("keeps local errors visible even when a persisted session error was dismissed", () => {
    const dismissKey = buildThreadErrorDismissKey({
      threadKey: "environment-local:thread-1",
      turnId: null,
      error: "Persisted error",
    });

    expect(
      resolveVisibleServerThreadError({
        localError: "Select a base branch before sending.",
        sessionError: "Persisted error",
        dismissedSessionErrorKeys: { [dismissKey!]: true },
        threadKey: "environment-local:thread-1",
        turnId: null,
      }),
    ).toBe("Select a base branch before sending.");
  });
});

describe("deriveComposerSendState", () => {
  it("treats expired terminal pills as non-sendable content", () => {
    const state = deriveComposerSendState({
      prompt: "\uFFFC",
      imageCount: 0,
      terminalContexts: [
        {
          id: "ctx-expired",
          threadId,
          terminalId: "default",
          terminalLabel: "Terminal 1",
          lineStart: 4,
          lineEnd: 4,
          text: "",
          createdAt: now,
        },
      ],
    });

    expect(state.trimmedPrompt).toBe("");
    expect(state.sendableTerminalContexts).toEqual([]);
    expect(state.expiredTerminalContextCount).toBe(1);
    expect(state.hasSendableContent).toBe(false);
  });

  it("keeps text sendable while excluding expired terminal pills", () => {
    const state = deriveComposerSendState({
      prompt: `yoo \uFFFC waddup`,
      imageCount: 0,
      terminalContexts: [
        {
          id: "ctx-expired",
          threadId,
          terminalId: "default",
          terminalLabel: "Terminal 1",
          lineStart: 4,
          lineEnd: 4,
          text: "",
          createdAt: now,
        },
      ],
    });

    expect(state.trimmedPrompt).toBe("yoo  waddup");
    expect(state.expiredTerminalContextCount).toBe(1);
    expect(state.hasSendableContent).toBe(true);
  });

  it("treats element contexts as sendable content (no text, no images, no terminals)", () => {
    const state = deriveComposerSendState({
      prompt: "",
      imageCount: 0,
      terminalContexts: [],
      elementContextCount: 1,
    });

    expect(state.trimmedPrompt).toBe("");
    expect(state.expiredTerminalContextCount).toBe(0);
    expect(state.hasSendableContent).toBe(true);
  });

  it("does NOT treat zero element contexts as sendable", () => {
    expect(
      deriveComposerSendState({
        prompt: "",
        imageCount: 0,
        terminalContexts: [],
        elementContextCount: 0,
      }).hasSendableContent,
    ).toBe(false);
  });
});

describe("buildExpiredTerminalContextToastCopy", () => {
  it("formats empty and omission guidance", () => {
    expect(buildExpiredTerminalContextToastCopy(1, "empty")).toEqual({
      title: "Expired terminal context won't be sent",
      description: "Remove it or re-add it to include terminal output.",
    });
    expect(buildExpiredTerminalContextToastCopy(2, "omitted")).toEqual({
      title: "Expired terminal contexts omitted from message",
      description: "Re-add it if you want that terminal output included.",
    });
  });
});

describe("getStartedThreadModelChangeBlockReason", () => {
  const providers = [
    {
      instanceId: ProviderInstanceId.make("codex"),
    },
    {
      instanceId: ProviderInstanceId.make("grok"),
      requiresNewThreadForModelChange: true,
    },
  ];

  it("allows model changes before a provider session has started", () => {
    expect(
      getStartedThreadModelChangeBlockReason({
        providers,
        hasStartedSession: false,
        currentModelSelection: {
          instanceId: ProviderInstanceId.make("grok"),
          model: "grok-build",
        },
        nextModelSelection: {
          instanceId: ProviderInstanceId.make("grok"),
          model: "grok-other",
        },
      }),
    ).toBeNull();
  });

  it("allows unchanged model selections for restricted providers", () => {
    expect(
      getStartedThreadModelChangeBlockReason({
        providers,
        hasStartedSession: true,
        currentModelSelection: {
          instanceId: ProviderInstanceId.make("grok"),
          model: "grok-build",
        },
        nextModelSelection: {
          instanceId: ProviderInstanceId.make("grok"),
          model: "grok-build",
        },
      }),
    ).toBeNull();
  });

  it("blocks started-session model changes when either provider requires a new thread", () => {
    expect(
      getStartedThreadModelChangeBlockReason({
        providers,
        hasStartedSession: true,
        currentModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
        },
        nextModelSelection: {
          instanceId: ProviderInstanceId.make("grok"),
          model: "grok-build",
        },
      }),
    ).toEqual({
      title: "Start a new chat to change models",
      description:
        "This provider does not allow switching models after a conversation has started.",
    });
  });
});

describe("resolveSendEnvMode", () => {
  it("keeps worktree mode only for git repositories", () => {
    expect(resolveSendEnvMode({ requestedEnvMode: "worktree", isGitRepo: true })).toBe("worktree");
    expect(resolveSendEnvMode({ requestedEnvMode: "worktree", isGitRepo: false })).toBe("local");
  });
});

describe("reconcileMountedTerminalThreadIds", () => {
  it("keeps open threads and makes the active thread most recent", () => {
    expect(
      reconcileMountedTerminalThreadIds({
        currentThreadIds: ["thread-a", "thread-b", "thread-c"],
        openThreadIds: ["thread-a", "thread-b", "thread-c"],
        activeThreadId: "thread-a",
        activeThreadTerminalOpen: true,
        maxHiddenThreadCount: 2,
      }),
    ).toEqual(["thread-b", "thread-c", "thread-a"]);
  });

  it("drops closed threads and enforces the hidden mounted cap", () => {
    const ids = Array.from(
      { length: MAX_HIDDEN_MOUNTED_TERMINAL_THREADS + 2 },
      (_, index) => `thread-${index}`,
    );
    expect(
      reconcileMountedTerminalThreadIds({
        currentThreadIds: ids,
        openThreadIds: ids.slice(1),
        activeThreadId: null,
        activeThreadTerminalOpen: false,
      }),
    ).toEqual(ids.slice(-MAX_HIDDEN_MOUNTED_TERMINAL_THREADS));
  });
});

describe("reconcileRetainedMountedThreadIds", () => {
  it("retains hidden open threads and adds the active open thread", () => {
    expect(
      reconcileRetainedMountedThreadIds({
        currentThreadIds: [ThreadId.make("thread-hidden")],
        openThreadIds: [ThreadId.make("thread-hidden")],
        activeThreadId: ThreadId.make("thread-active"),
        activeThreadOpen: true,
        maxHiddenThreadCount: MAX_HIDDEN_MOUNTED_PREVIEW_THREADS,
      }),
    ).toEqual([ThreadId.make("thread-hidden"), ThreadId.make("thread-active")]);
  });

  it("can retain the active thread as hidden when it is inactive", () => {
    expect(
      reconcileRetainedMountedThreadIds({
        currentThreadIds: [ThreadId.make("thread-active")],
        openThreadIds: [ThreadId.make("thread-active")],
        activeThreadId: ThreadId.make("thread-active"),
        activeThreadOpen: false,
        maxHiddenThreadCount: MAX_HIDDEN_MOUNTED_PREVIEW_THREADS,
        retainInactiveActiveThread: true,
      }),
    ).toEqual([ThreadId.make("thread-active")]);
  });

  it("evicts the oldest hidden threads beyond the configured cap", () => {
    const currentThreadIds = Array.from(
      { length: MAX_HIDDEN_MOUNTED_PREVIEW_THREADS + 2 },
      (_, index) => ThreadId.make(`thread-${index + 1}`),
    );

    expect(
      reconcileRetainedMountedThreadIds({
        currentThreadIds,
        openThreadIds: currentThreadIds,
        activeThreadId: null,
        activeThreadOpen: false,
        maxHiddenThreadCount: MAX_HIDDEN_MOUNTED_PREVIEW_THREADS,
      }),
    ).toEqual(currentThreadIds.slice(-MAX_HIDDEN_MOUNTED_PREVIEW_THREADS));
  });
});

describe("shouldWriteThreadErrorToCurrentServerThread", () => {
  it("requires the environment, route thread, and target thread to match", () => {
    const routeThreadRef = { environmentId, threadId };

    expect(
      shouldWriteThreadErrorToCurrentServerThread({
        serverThread: { environmentId, id: threadId },
        routeThreadRef,
        targetThreadId: threadId,
      }),
    ).toBe(true);
    expect(
      shouldWriteThreadErrorToCurrentServerThread({
        serverThread: null,
        routeThreadRef,
        targetThreadId: threadId,
      }),
    ).toBe(false);
  });
});

describe("hasServerAcknowledgedLocalDispatch", () => {
  it("does not acknowledge unchanged server state", () => {
    const localDispatch = createLocalDispatchSnapshot(
      makeThread({ latestTurn: completedTurn, session: readySession }),
    );

    expect(
      hasServerAcknowledgedLocalDispatch({
        localDispatch,
        phase: "ready",
        latestTurn: completedTurn,
        latestUserMessageId: localDispatch.latestUserMessageId,
        session: readySession,
        hasPendingApproval: false,
        hasPendingUserInput: false,
        threadError: null,
      }),
    ).toBe(false);
  });

  it("acknowledges a settled newer turn", () => {
    const localDispatch = createLocalDispatchSnapshot(
      makeThread({ latestTurn: completedTurn, session: readySession }),
    );
    const newerTurn = {
      ...completedTurn,
      turnId: TurnId.make("turn-2"),
      requestedAt: "2026-03-29T00:01:00.000Z",
      startedAt: "2026-03-29T00:01:01.000Z",
      completedAt: "2026-03-29T00:01:30.000Z",
    };

    expect(
      hasServerAcknowledgedLocalDispatch({
        localDispatch,
        phase: "ready",
        latestTurn: newerTurn,
        latestUserMessageId: localDispatch.latestUserMessageId,
        session: { ...readySession, updatedAt: newerTurn.completedAt },
        hasPendingApproval: false,
        hasPendingUserInput: false,
        threadError: null,
      }),
    ).toBe(true);
  });

  it("waits for the matching running turn before acknowledging", () => {
    const localDispatch = createLocalDispatchSnapshot(
      makeThread({ latestTurn: completedTurn, session: readySession }),
    );
    const runningTurn = {
      ...completedTurn,
      turnId: TurnId.make("turn-2"),
      state: "running" as const,
      requestedAt: "2026-03-29T00:01:00.000Z",
      startedAt: "2026-03-29T00:01:01.000Z",
      completedAt: null,
    };

    expect(
      hasServerAcknowledgedLocalDispatch({
        localDispatch,
        phase: "running",
        latestTurn: runningTurn,
        latestUserMessageId: localDispatch.latestUserMessageId,
        session: {
          ...readySession,
          status: "running",
          activeTurnId: TurnId.make("turn-other"),
        },
        hasPendingApproval: false,
        hasPendingUserInput: false,
        threadError: null,
      }),
    ).toBe(false);
    expect(
      hasServerAcknowledgedLocalDispatch({
        localDispatch,
        phase: "running",
        latestTurn: runningTurn,
        latestUserMessageId: localDispatch.latestUserMessageId,
        session: {
          ...readySession,
          status: "running",
          activeTurnId: runningTurn.turnId,
        },
        hasPendingApproval: false,
        hasPendingUserInput: false,
        threadError: null,
      }),
    ).toBe(true);
  });

  it("acknowledges a steering message projected onto the current running turn", () => {
    const runningTurn = {
      ...completedTurn,
      state: "running" as const,
      completedAt: null,
    };
    const runningSession = {
      ...readySession,
      status: "running" as const,
      activeTurnId: runningTurn.turnId,
    };
    const localDispatch = createLocalDispatchSnapshot(
      makeThread({
        latestTurn: runningTurn,
        session: runningSession,
        messages: [
          {
            id: MessageId.make("message-before-steer"),
            role: "user",
            text: "Initial prompt",
            turnId: runningTurn.turnId,
            createdAt: runningTurn.requestedAt,
            updatedAt: runningTurn.requestedAt,
            streaming: false,
          },
        ],
      }),
    );

    expect(
      hasServerAcknowledgedLocalDispatch({
        localDispatch,
        phase: "running",
        latestTurn: runningTurn,
        latestUserMessageId: MessageId.make("message-steer"),
        session: runningSession,
        hasPendingApproval: false,
        hasPendingUserInput: false,
        threadError: null,
      }),
    ).toBe(true);
  });

  it("acknowledges pending user interaction and errors immediately", () => {
    const localDispatch = createLocalDispatchSnapshot(makeThread());
    const common = {
      localDispatch,
      phase: "ready" as const,
      latestTurn: null,
      latestUserMessageId: localDispatch.latestUserMessageId,
      session: null,
      hasPendingApproval: false,
      hasPendingUserInput: false,
      threadError: null,
    };

    expect(hasServerAcknowledgedLocalDispatch({ ...common, hasPendingApproval: true })).toBe(true);
    expect(hasServerAcknowledgedLocalDispatch({ ...common, hasPendingUserInput: true })).toBe(true);
    expect(hasServerAcknowledgedLocalDispatch({ ...common, threadError: "failed" })).toBe(true);
  });
});
