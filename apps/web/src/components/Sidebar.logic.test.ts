import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  buildSidebarThreadTreeRows,
  createThreadJumpHintVisibilityController,
  filterSidebarThreadTreeRowsByExpansion,
  getSidebarThreadIdsToPrewarm,
  getVisibleSidebarThreadIds,
  getVisibleSidebarThreadTreeRowsForPreview,
  resolveAdjacentThreadId,
  getFallbackThreadIdAfterDelete,
  getVisibleThreadsForProject,
  getProjectSortTimestamp,
  hasUnseenCompletion,
  isContextMenuPointerDown,
  isTrailingDoubleClick,
  orderItemsByPreferredIds,
  resolveProjectStatusIndicator,
  resolveSidebarNewThreadSeedContext,
  resolveSidebarNewThreadEnvMode,
  resolveSidebarStageBadgeLabel,
  resolveThreadRowClassName,
  resolveThreadStatusPill,
  sidebarThreadExpansionKey,
  shouldClearThreadSelectionOnMouseDown,
  sortSidebarThreadsByActivity,
  sortProjectsForSidebar,
  THREAD_JUMP_HINT_SHOW_DELAY_MS,
  type SidebarThreadActivityInput,
  type SidebarThreadTreeInput,
} from "./Sidebar.logic";
import {
  EnvironmentId,
  OrchestrationLatestTurn,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import {
  DEFAULT_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  type Project,
  type Thread,
} from "../types";

const localEnvironmentId = EnvironmentId.make("environment-local");

describe("resolveSidebarStageBadgeLabel", () => {
  it("returns Nightly for nightly primary server versions", () => {
    expect(
      resolveSidebarStageBadgeLabel({
        primaryServerVersion: "0.0.28-nightly.20260616.12",
        fallbackStageLabel: "Alpha",
      }),
    ).toBe("Nightly");
  });

  it("returns the fallback label for stable primary server versions", () => {
    expect(
      resolveSidebarStageBadgeLabel({
        primaryServerVersion: "0.0.27",
        fallbackStageLabel: "Alpha",
      }),
    ).toBe("Alpha");
  });

  it("returns the fallback label when the primary server version is missing", () => {
    expect(
      resolveSidebarStageBadgeLabel({
        primaryServerVersion: null,
        fallbackStageLabel: "Dev",
      }),
    ).toBe("Dev");
  });

  it("returns the fallback label for malformed nightly prerelease versions", () => {
    expect(
      resolveSidebarStageBadgeLabel({
        primaryServerVersion: "0.0.28-nightly.20260616",
        fallbackStageLabel: "Alpha",
      }),
    ).toBe("Alpha");
  });
});

function makeLatestTurn(overrides?: {
  completedAt?: string | null;
  startedAt?: string | null;
}): OrchestrationLatestTurn {
  return {
    turnId: "turn-1" as never,
    state: "completed",
    assistantMessageId: null,
    requestedAt: "2026-03-09T10:00:00.000Z",
    startedAt: overrides?.startedAt ?? "2026-03-09T10:00:00.000Z",
    completedAt: overrides?.completedAt ?? "2026-03-09T10:05:00.000Z",
  };
}

describe("hasUnseenCompletion", () => {
  it("returns true when a thread completed after its last visit", () => {
    expect(
      hasUnseenCompletion({
        hasActionableProposedPlan: false,
        hasPendingApprovals: false,
        hasPendingUserInput: false,
        interactionMode: "default",
        latestTurn: makeLatestTurn(),
        lastVisitedAt: "2026-03-09T10:04:00.000Z",
        session: null,
      }),
    ).toBe(true);
  });

  it("treats a missing client visit marker as read", () => {
    expect(
      hasUnseenCompletion({
        hasActionableProposedPlan: false,
        hasPendingApprovals: false,
        hasPendingUserInput: false,
        interactionMode: "default",
        latestTurn: makeLatestTurn(),
        lastVisitedAt: undefined,
        session: null,
      }),
    ).toBe(false);
  });
});

describe("createThreadJumpHintVisibilityController", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("delays showing jump hints until the configured delay elapses", () => {
    const visibilityChanges: boolean[] = [];
    const controller = createThreadJumpHintVisibilityController({
      delayMs: THREAD_JUMP_HINT_SHOW_DELAY_MS,
      onVisibilityChange: (visible) => {
        visibilityChanges.push(visible);
      },
    });

    controller.sync(true);
    vi.advanceTimersByTime(THREAD_JUMP_HINT_SHOW_DELAY_MS - 1);

    expect(visibilityChanges).toEqual([]);

    vi.advanceTimersByTime(1);

    expect(visibilityChanges).toEqual([true]);
  });

  it("hides immediately when the modifiers are released", () => {
    const visibilityChanges: boolean[] = [];
    const controller = createThreadJumpHintVisibilityController({
      delayMs: THREAD_JUMP_HINT_SHOW_DELAY_MS,
      onVisibilityChange: (visible) => {
        visibilityChanges.push(visible);
      },
    });

    controller.sync(true);
    vi.advanceTimersByTime(THREAD_JUMP_HINT_SHOW_DELAY_MS);
    controller.sync(false);

    expect(visibilityChanges).toEqual([true, false]);
  });

  it("cancels a pending reveal when the modifier is released early", () => {
    const visibilityChanges: boolean[] = [];
    const controller = createThreadJumpHintVisibilityController({
      delayMs: THREAD_JUMP_HINT_SHOW_DELAY_MS,
      onVisibilityChange: (visible) => {
        visibilityChanges.push(visible);
      },
    });

    controller.sync(true);
    vi.advanceTimersByTime(Math.floor(THREAD_JUMP_HINT_SHOW_DELAY_MS / 2));
    controller.sync(false);
    vi.advanceTimersByTime(THREAD_JUMP_HINT_SHOW_DELAY_MS);

    expect(visibilityChanges).toEqual([]);
  });
});

describe("getSidebarThreadIdsToPrewarm", () => {
  it("returns only the first visible thread ids up to the prewarm limit", () => {
    expect(getSidebarThreadIdsToPrewarm(["t1", "t2", "t3"], 2)).toEqual(["t1", "t2"]);
  });

  it("returns all visible thread ids when they fit within the limit", () => {
    expect(getSidebarThreadIdsToPrewarm(["t1", "t2"], 10)).toEqual(["t1", "t2"]);
  });

  it("returns no thread ids when the limit is zero", () => {
    expect(getSidebarThreadIdsToPrewarm(["t1", "t2"], 0)).toEqual([]);
  });
});

describe("shouldClearThreadSelectionOnMouseDown", () => {
  it("preserves selection for thread items", () => {
    const child = {
      closest: (selector: string) =>
        selector.includes("[data-thread-item]") ? ({} as Element) : null,
    } as unknown as HTMLElement;

    expect(shouldClearThreadSelectionOnMouseDown(child)).toBe(false);
  });

  it("preserves selection for thread list toggle controls", () => {
    const selectionSafe = {
      closest: (selector: string) =>
        selector.includes("[data-thread-selection-safe]") ? ({} as Element) : null,
    } as unknown as HTMLElement;

    expect(shouldClearThreadSelectionOnMouseDown(selectionSafe)).toBe(false);
  });

  it("clears selection for unrelated sidebar clicks", () => {
    const unrelated = {
      closest: () => null,
    } as unknown as HTMLElement;

    expect(shouldClearThreadSelectionOnMouseDown(unrelated)).toBe(true);
  });
});

describe("isTrailingDoubleClick", () => {
  it("treats a single click as a normal activation", () => {
    expect(isTrailingDoubleClick(1)).toBe(false);
  });

  it("treats synthetic/keyboard activations (detail 0) as a normal activation", () => {
    expect(isTrailingDoubleClick(0)).toBe(false);
  });

  it("ignores the second click of a double-click so it does not navigate", () => {
    expect(isTrailingDoubleClick(2)).toBe(true);
  });

  it("ignores further clicks of a triple-click", () => {
    expect(isTrailingDoubleClick(3)).toBe(true);
  });
});

describe("resolveSidebarNewThreadEnvMode", () => {
  it("uses the app default when the caller does not request a specific mode", () => {
    expect(
      resolveSidebarNewThreadEnvMode({
        defaultEnvMode: "worktree",
      }),
    ).toBe("worktree");
  });

  it("preserves an explicit requested mode over the app default", () => {
    expect(
      resolveSidebarNewThreadEnvMode({
        requestedEnvMode: "local",
        defaultEnvMode: "worktree",
      }),
    ).toBe("local");
  });
});

describe("resolveSidebarNewThreadSeedContext", () => {
  it("prefers the default worktree mode over active thread context", () => {
    expect(
      resolveSidebarNewThreadSeedContext({
        projectId: "project-1",
        defaultEnvMode: "worktree",
        activeThread: {
          projectId: "project-1",
          branch: "feature/existing",
          worktreePath: "/repo/.t3/worktrees/existing",
        },
        activeDraftThread: {
          projectId: "project-1",
          branch: "feature/draft",
          worktreePath: "/repo/.t3/worktrees/draft",
          envMode: "worktree",
          startFromOrigin: true,
        },
      }),
    ).toEqual({
      envMode: "worktree",
    });
  });

  it("inherits the active server thread context when creating a new thread in the same project", () => {
    expect(
      resolveSidebarNewThreadSeedContext({
        projectId: "project-1",
        defaultEnvMode: "local",
        activeThread: {
          projectId: "project-1",
          branch: "effect-atom",
          worktreePath: null,
        },
        activeDraftThread: null,
      }),
    ).toEqual({
      branch: "effect-atom",
      worktreePath: null,
      envMode: "local",
    });
  });

  it("prefers the active draft thread context when it matches the target project", () => {
    expect(
      resolveSidebarNewThreadSeedContext({
        projectId: "project-1",
        defaultEnvMode: "local",
        activeThread: {
          projectId: "project-1",
          branch: "effect-atom",
          worktreePath: null,
        },
        activeDraftThread: {
          projectId: "project-1",
          branch: "feature/new-draft",
          worktreePath: "/repo/worktree",
          envMode: "worktree",
          startFromOrigin: true,
        },
      }),
    ).toEqual({
      branch: "feature/new-draft",
      worktreePath: "/repo/worktree",
      envMode: "worktree",
      startFromOrigin: true,
    });
  });

  it("falls back to the default env mode when there is no matching active thread context", () => {
    expect(
      resolveSidebarNewThreadSeedContext({
        projectId: "project-2",
        defaultEnvMode: "worktree",
        activeThread: {
          projectId: "project-1",
          branch: "effect-atom",
          worktreePath: null,
        },
        activeDraftThread: null,
      }),
    ).toEqual({
      envMode: "worktree",
    });
  });
});

describe("orderItemsByPreferredIds", () => {
  it("keeps preferred ids first, skips stale ids, and preserves the relative order of remaining items", () => {
    const ordered = orderItemsByPreferredIds({
      items: [
        { id: ProjectId.make("project-1"), name: "One" },
        { id: ProjectId.make("project-2"), name: "Two" },
        { id: ProjectId.make("project-3"), name: "Three" },
      ],
      preferredIds: [
        ProjectId.make("project-3"),
        ProjectId.make("project-missing"),
        ProjectId.make("project-1"),
      ],
      getId: (project) => project.id,
    });

    expect(ordered.map((project) => project.id)).toEqual([
      ProjectId.make("project-3"),
      ProjectId.make("project-1"),
      ProjectId.make("project-2"),
    ]);
  });

  it("does not duplicate items when preferred ids repeat", () => {
    const ordered = orderItemsByPreferredIds({
      items: [
        { id: ProjectId.make("project-1"), name: "One" },
        { id: ProjectId.make("project-2"), name: "Two" },
      ],
      preferredIds: [
        ProjectId.make("project-2"),
        ProjectId.make("project-1"),
        ProjectId.make("project-2"),
      ],
      getId: (project) => project.id,
    });

    expect(ordered.map((project) => project.id)).toEqual([
      ProjectId.make("project-2"),
      ProjectId.make("project-1"),
    ]);
  });

  it("honors projectOrder physical keys via getProjectOrderKey", async () => {
    // Regression guard for #1904 / the regression introduced by #2055:
    // `projectOrder` is populated with physical keys (envId + cwd-derived)
    // by the store and by drag-end handlers. Readers must identify projects
    // with the same key format, or manual sort silently snaps back.
    const { getProjectOrderKey } = await import("../logicalProject");
    const projects = [
      {
        environmentId: EnvironmentId.make("environment-local"),
        id: ProjectId.make("id-alpha"),
        workspaceRoot: "/work/alpha",
      },
      {
        environmentId: EnvironmentId.make("environment-local"),
        id: ProjectId.make("id-beta"),
        workspaceRoot: "/work/beta",
      },
      {
        environmentId: EnvironmentId.make("environment-local"),
        id: ProjectId.make("id-gamma"),
        workspaceRoot: "/work/gamma",
      },
    ];
    const ordered = orderItemsByPreferredIds({
      items: projects,
      preferredIds: [getProjectOrderKey(projects[2]!), getProjectOrderKey(projects[0]!)],
      getId: getProjectOrderKey,
    });

    expect(ordered.map((project) => project.workspaceRoot)).toEqual([
      "/work/gamma",
      "/work/alpha",
      "/work/beta",
    ]);
  });

  it("resolves legacy preference aliases without materializing project state", () => {
    const ordered = orderItemsByPreferredIds({
      items: [
        { id: "physical-a", cwd: "/work/a" },
        { id: "physical-b", cwd: "/work/b" },
        { id: "physical-c", cwd: "/work/c" },
      ],
      preferredIds: ["legacy:/work/c", "legacy:/work/a"],
      getId: (project) => project.id,
      getPreferenceIds: (project) => [project.id, `legacy:${project.cwd}`],
    });

    expect(ordered.map((project) => project.id)).toEqual([
      "physical-c",
      "physical-a",
      "physical-b",
    ]);
  });
});

describe("resolveAdjacentThreadId", () => {
  it("resolves adjacent thread ids in ordered sidebar traversal", () => {
    const threads = [
      ThreadId.make("thread-1"),
      ThreadId.make("thread-2"),
      ThreadId.make("thread-3"),
    ];

    expect(
      resolveAdjacentThreadId({
        threadIds: threads,
        currentThreadId: threads[1] ?? null,
        direction: "previous",
      }),
    ).toBe(threads[0]);
    expect(
      resolveAdjacentThreadId({
        threadIds: threads,
        currentThreadId: threads[1] ?? null,
        direction: "next",
      }),
    ).toBe(threads[2]);
    expect(
      resolveAdjacentThreadId({
        threadIds: threads,
        currentThreadId: null,
        direction: "next",
      }),
    ).toBe(threads[0]);
    expect(
      resolveAdjacentThreadId({
        threadIds: threads,
        currentThreadId: null,
        direction: "previous",
      }),
    ).toBe(threads[2]);
    expect(
      resolveAdjacentThreadId({
        threadIds: threads,
        currentThreadId: threads[0] ?? null,
        direction: "previous",
      }),
    ).toBeNull();
  });
});

describe("getVisibleSidebarThreadIds", () => {
  it("returns only the rendered visible thread order across projects", () => {
    expect(
      getVisibleSidebarThreadIds([
        {
          renderedThreadIds: [
            ThreadId.make("thread-12"),
            ThreadId.make("thread-11"),
            ThreadId.make("thread-10"),
          ],
        },
        {
          renderedThreadIds: [ThreadId.make("thread-8"), ThreadId.make("thread-6")],
        },
      ]),
    ).toEqual([
      ThreadId.make("thread-12"),
      ThreadId.make("thread-11"),
      ThreadId.make("thread-10"),
      ThreadId.make("thread-8"),
      ThreadId.make("thread-6"),
    ]);
  });

  it("skips threads from collapsed projects whose thread panels are not shown", () => {
    expect(
      getVisibleSidebarThreadIds([
        {
          shouldShowThreadPanel: false,
          renderedThreadIds: [ThreadId.make("thread-hidden-2"), ThreadId.make("thread-hidden-1")],
        },
        {
          shouldShowThreadPanel: true,
          renderedThreadIds: [ThreadId.make("thread-12"), ThreadId.make("thread-11")],
        },
      ]),
    ).toEqual([ThreadId.make("thread-12"), ThreadId.make("thread-11")]);
  });
});

describe("isContextMenuPointerDown", () => {
  it("treats secondary-button presses as context menu gestures on all platforms", () => {
    expect(
      isContextMenuPointerDown({
        button: 2,
        ctrlKey: false,
        isMac: false,
      }),
    ).toBe(true);
  });

  it("treats ctrl+primary-click as a context menu gesture on macOS", () => {
    expect(
      isContextMenuPointerDown({
        button: 0,
        ctrlKey: true,
        isMac: true,
      }),
    ).toBe(true);
  });

  it("does not treat ctrl+primary-click as a context menu gesture off macOS", () => {
    expect(
      isContextMenuPointerDown({
        button: 0,
        ctrlKey: true,
        isMac: false,
      }),
    ).toBe(false);
  });
});

describe("resolveThreadStatusPill", () => {
  const baseThread = {
    hasActionableProposedPlan: false,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    interactionMode: "plan" as const,
    latestTurn: null,
    lastVisitedAt: undefined,
    session: {
      threadId: ThreadId.make("thread-1"),
      status: "running" as const,
      providerName: "Codex",
      providerInstanceId: ProviderInstanceId.make("codex"),
      runtimeMode: DEFAULT_RUNTIME_MODE,
      activeTurnId: "turn-1" as never,
      lastError: null,
      updatedAt: "2026-03-09T10:00:00.000Z",
    },
  };

  it("shows pending approval before all other statuses", () => {
    expect(
      resolveThreadStatusPill({
        thread: {
          ...baseThread,
          hasPendingApprovals: true,
          hasPendingUserInput: true,
        },
      }),
    ).toMatchObject({ label: "Pending Approval", pulse: false });
  });

  it("shows awaiting input when plan mode is blocked on user answers", () => {
    expect(
      resolveThreadStatusPill({
        thread: {
          ...baseThread,
          hasPendingUserInput: true,
        },
      }),
    ).toMatchObject({ label: "Awaiting Input", pulse: false });
  });

  it("falls back to working when the thread is actively running without blockers", () => {
    expect(
      resolveThreadStatusPill({
        thread: baseThread,
      }),
    ).toMatchObject({ label: "Working", pulse: true });
  });

  it("shows plan ready when a settled plan turn has a proposed plan ready for follow-up", () => {
    expect(
      resolveThreadStatusPill({
        thread: {
          ...baseThread,
          hasActionableProposedPlan: true,
          latestTurn: makeLatestTurn(),
          session: {
            ...baseThread.session,
            status: "ready",
            activeTurnId: null,
          },
        },
      }),
    ).toMatchObject({ label: "Plan Ready", pulse: false });
  });

  it("does not manufacture completed state without a client visit marker", () => {
    expect(
      resolveThreadStatusPill({
        thread: {
          ...baseThread,
          latestTurn: makeLatestTurn(),
          session: {
            ...baseThread.session,
            status: "ready",
            activeTurnId: null,
          },
        },
      }),
    ).toBeNull();
  });

  it("shows completed when there is an unseen completion and no active blocker", () => {
    expect(
      resolveThreadStatusPill({
        thread: {
          ...baseThread,
          interactionMode: "default",
          latestTurn: makeLatestTurn(),
          lastVisitedAt: "2026-03-09T10:04:00.000Z",
          session: {
            ...baseThread.session,
            status: "ready",
            activeTurnId: null,
          },
        },
      }),
    ).toMatchObject({ label: "Completed", pulse: false });
  });
});

describe("resolveThreadRowClassName", () => {
  it("uses the darker selected palette when a thread is both selected and active", () => {
    const className = resolveThreadRowClassName({ isActive: true, isSelected: true });
    expect(className).toContain("bg-primary/22");
    expect(className).toContain("hover:bg-primary/26");
    expect(className).toContain("dark:bg-primary/30");
    expect(className).not.toContain("bg-accent/85");
  });

  it("uses selected hover colors for selected threads", () => {
    const className = resolveThreadRowClassName({ isActive: false, isSelected: true });
    expect(className).toContain("bg-primary/15");
    expect(className).toContain("hover:bg-primary/19");
    expect(className).toContain("dark:bg-primary/22");
    expect(className).not.toContain("hover:bg-accent");
  });

  it("keeps the accent palette for active-only threads", () => {
    const className = resolveThreadRowClassName({ isActive: true, isSelected: false });
    expect(className).toContain("bg-accent/85");
    expect(className).toContain("hover:bg-accent");
  });
});

describe("resolveProjectStatusIndicator", () => {
  it("returns null when no threads have a notable status", () => {
    expect(resolveProjectStatusIndicator([null, null])).toBeNull();
  });

  it("surfaces the highest-priority actionable state across project threads", () => {
    expect(
      resolveProjectStatusIndicator([
        {
          label: "Completed",
          colorClass: "text-emerald-600",
          dotClass: "bg-emerald-500",
          pulse: false,
        },
        {
          label: "Pending Approval",
          colorClass: "text-amber-600",
          dotClass: "bg-amber-500",
          pulse: false,
        },
        {
          label: "Working",
          colorClass: "text-sky-600",
          dotClass: "bg-sky-500",
          pulse: true,
        },
      ]),
    ).toMatchObject({ label: "Pending Approval", dotClass: "bg-amber-500" });
  });

  it("prefers plan-ready over completed when no stronger action is needed", () => {
    expect(
      resolveProjectStatusIndicator([
        {
          label: "Completed",
          colorClass: "text-emerald-600",
          dotClass: "bg-emerald-500",
          pulse: false,
        },
        {
          label: "Plan Ready",
          colorClass: "text-violet-600",
          dotClass: "bg-violet-500",
          pulse: false,
        },
      ]),
    ).toMatchObject({ label: "Plan Ready", dotClass: "bg-violet-500" });
  });
});

function makeTreeThread(
  overrides: Partial<SidebarThreadTreeInput> & Pick<SidebarThreadTreeInput, "id">,
): SidebarThreadTreeInput {
  return {
    id: overrides.id,
    environmentId: overrides.environmentId ?? localEnvironmentId,
    hasActionableProposedPlan: overrides.hasActionableProposedPlan ?? false,
    hasPendingApprovals: overrides.hasPendingApprovals ?? false,
    hasPendingUserInput: overrides.hasPendingUserInput ?? false,
    interactionMode: overrides.interactionMode ?? "default",
    latestTurn: overrides.latestTurn ?? null,
    parentThreadId: overrides.parentThreadId ?? null,
    session: overrides.session ?? null,
  };
}

function makeActivityTreeThread(
  overrides: Partial<SidebarThreadActivityInput> & Pick<SidebarThreadActivityInput, "id">,
): SidebarThreadActivityInput {
  return {
    ...makeTreeThread(overrides),
    createdAt: overrides.createdAt ?? "2026-03-09T10:00:00.000Z",
    updatedAt: overrides.updatedAt ?? "2026-03-09T10:00:00.000Z",
    latestUserMessageAt: overrides.latestUserMessageAt ?? null,
  };
}

describe("buildSidebarThreadTreeRows", () => {
  it("places child threads directly under their parent with indentation metadata", () => {
    const parent = makeTreeThread({ id: ThreadId.make("parent") });
    const unrelated = makeTreeThread({ id: ThreadId.make("unrelated") });
    const child = makeTreeThread({
      id: ThreadId.make("child"),
      parentThreadId: parent.id,
    });

    const rows = buildSidebarThreadTreeRows([parent, unrelated, child]);

    expect(rows.map((row) => row.thread.id)).toEqual([parent.id, child.id, unrelated.id]);
    expect(rows.map((row) => row.depth)).toEqual([0, 1, 0]);
    expect(rows[0]).toMatchObject({
      directChildCount: 1,
      descendantCount: 1,
    });
  });

  it("orders parent groups by the best sorted position among parent and descendants", () => {
    const parent = makeTreeThread({ id: ThreadId.make("old-parent") });
    const newerChild = makeTreeThread({
      id: ThreadId.make("newer-child"),
      parentThreadId: parent.id,
    });
    const middleThread = makeTreeThread({ id: ThreadId.make("middle-thread") });

    const rows = buildSidebarThreadTreeRows([newerChild, middleThread, parent]);

    expect(rows.map((row) => row.thread.id)).toEqual([parent.id, newerChild.id, middleThread.id]);
  });

  it("sorts sidebar threads active-first, then by latest activity", () => {
    const oldActive = makeActivityTreeThread({
      id: ThreadId.make("old-active"),
      updatedAt: "2026-03-09T10:01:00.000Z",
      session: {
        threadId: ThreadId.make("old-active"),
        status: "running",
        providerName: "Codex",
        providerInstanceId: ProviderInstanceId.make("codex"),
        runtimeMode: DEFAULT_RUNTIME_MODE,
        activeTurnId: "turn-running" as never,
        lastError: null,
        updatedAt: "2026-03-09T10:01:00.000Z",
      },
    });
    const recentIdle = makeActivityTreeThread({
      id: ThreadId.make("recent-idle"),
      updatedAt: "2026-03-09T10:10:00.000Z",
    });
    const olderIdle = makeActivityTreeThread({
      id: ThreadId.make("older-idle"),
      updatedAt: "2026-03-09T10:05:00.000Z",
    });

    const sorted = sortSidebarThreadsByActivity([recentIdle, olderIdle, oldActive]);

    expect(sorted.map((thread) => thread.id)).toEqual([oldActive.id, recentIdle.id, olderIdle.id]);
  });

  it("uses the configured thread sort order after active threads", () => {
    const newerCreated = makeActivityTreeThread({
      id: ThreadId.make("newer-created"),
      createdAt: "2026-03-09T10:10:00.000Z",
      updatedAt: "2026-03-09T10:01:00.000Z",
    });
    const olderCreated = makeActivityTreeThread({
      id: ThreadId.make("older-created"),
      createdAt: "2026-03-09T10:05:00.000Z",
      updatedAt: "2026-03-09T10:20:00.000Z",
    });

    const sorted = sortSidebarThreadsByActivity([olderCreated, newerCreated], "created_at");

    expect(sorted.map((thread) => thread.id)).toEqual([newerCreated.id, olderCreated.id]);
  });

  it("positions parent groups by the most active descendant and sorts children by activity", () => {
    const oldParent = makeActivityTreeThread({
      id: ThreadId.make("old-parent"),
      updatedAt: "2026-03-09T10:00:00.000Z",
    });
    const activeChild = makeActivityTreeThread({
      id: ThreadId.make("active-child"),
      parentThreadId: oldParent.id,
      updatedAt: "2026-03-09T10:01:00.000Z",
      latestTurn: {
        ...makeLatestTurn(),
        state: "running",
        completedAt: null,
      },
    });
    const recentChild = makeActivityTreeThread({
      id: ThreadId.make("recent-child"),
      parentThreadId: oldParent.id,
      updatedAt: "2026-03-09T10:20:00.000Z",
    });
    const recentRoot = makeActivityTreeThread({
      id: ThreadId.make("recent-root"),
      updatedAt: "2026-03-09T10:30:00.000Z",
    });

    const rows = buildSidebarThreadTreeRows(
      sortSidebarThreadsByActivity([oldParent, recentRoot, recentChild, activeChild]),
    );

    expect(rows.map((row) => row.thread.id)).toEqual([
      oldParent.id,
      activeChild.id,
      recentChild.id,
      recentRoot.id,
    ]);
  });

  it("rolls descendant running, done, and failed counts onto parent rows", () => {
    const parent = makeTreeThread({ id: ThreadId.make("parent") });
    const running = makeTreeThread({
      id: ThreadId.make("running"),
      parentThreadId: parent.id,
      session: {
        threadId: ThreadId.make("running"),
        status: "running",
        providerName: "Codex",
        providerInstanceId: ProviderInstanceId.make("codex"),
        runtimeMode: DEFAULT_RUNTIME_MODE,
        activeTurnId: "turn-running" as never,
        lastError: null,
        updatedAt: "2026-03-09T10:00:00.000Z",
      },
    });
    const done = makeTreeThread({
      id: ThreadId.make("done"),
      parentThreadId: parent.id,
      latestTurn: makeLatestTurn(),
    });
    const failed = makeTreeThread({
      id: ThreadId.make("failed"),
      parentThreadId: parent.id,
      latestTurn: {
        ...makeLatestTurn(),
        state: "error",
      },
    });

    const [parentRow] = buildSidebarThreadTreeRows([parent, running, done, failed]);

    expect(parentRow?.rollup).toEqual({
      needsYou: 0,
      running: 1,
      done: 1,
      failed: 1,
    });
  });

  it("keeps needs-you rollup metadata without overriding activity order", () => {
    const passiveParent = makeTreeThread({ id: ThreadId.make("passive-parent") });
    const activeParent = makeTreeThread({ id: ThreadId.make("active-parent") });
    const passiveChild = makeTreeThread({
      id: ThreadId.make("passive-child"),
      parentThreadId: activeParent.id,
    });
    const needsYouChild = makeTreeThread({
      id: ThreadId.make("needs-you-child"),
      parentThreadId: activeParent.id,
      hasPendingUserInput: true,
    });

    const rows = buildSidebarThreadTreeRows([
      passiveParent,
      activeParent,
      passiveChild,
      needsYouChild,
    ]);

    expect(rows.map((row) => row.thread.id)).toEqual([
      passiveParent.id,
      activeParent.id,
      passiveChild.id,
      needsYouChild.id,
    ]);
    expect(rows[1]).toMatchObject({
      hasNeedsYou: false,
      hasDescendantNeedsYou: true,
      rollup: {
        needsYou: 1,
        running: 0,
      },
    });
    expect(rows[3]).toMatchObject({ hasNeedsYou: true });
  });

  it("filters descendants of collapsed parent thread groups", () => {
    const parent = makeTreeThread({ id: ThreadId.make("parent") });
    const child = makeTreeThread({
      id: ThreadId.make("child"),
      parentThreadId: parent.id,
    });
    const grandchild = makeTreeThread({
      id: ThreadId.make("grandchild"),
      parentThreadId: child.id,
    });
    const sibling = makeTreeThread({ id: ThreadId.make("sibling") });
    const rows = buildSidebarThreadTreeRows([parent, child, grandchild, sibling]);

    const filtered = filterSidebarThreadTreeRowsByExpansion(rows, {
      [sidebarThreadExpansionKey(parent)]: false,
    });

    expect(filtered.map((row) => row.thread.id)).toEqual([parent.id, sibling.id]);
  });

  it("keeps the active descendant and its ancestors visible through collapsed parent groups", () => {
    const parent = makeTreeThread({ id: ThreadId.make("parent") });
    const child = makeTreeThread({
      id: ThreadId.make("child"),
      parentThreadId: parent.id,
    });
    const grandchild = makeTreeThread({
      id: ThreadId.make("grandchild"),
      parentThreadId: child.id,
    });
    const siblingChild = makeTreeThread({
      id: ThreadId.make("sibling-child"),
      parentThreadId: parent.id,
    });
    const rows = buildSidebarThreadTreeRows([parent, child, grandchild, siblingChild]);

    const filtered = filterSidebarThreadTreeRowsByExpansion(
      rows,
      {
        [sidebarThreadExpansionKey(parent)]: false,
        [sidebarThreadExpansionKey(child)]: false,
      },
      { activeThreadKey: sidebarThreadExpansionKey(grandchild) },
    );

    expect(filtered.map((row) => row.thread.id)).toEqual([parent.id, child.id, grandchild.id]);
  });

  it("keeps the active tree path visible when the folded preview would slice it out", () => {
    const earlyRows = Array.from({ length: 5 }, (_, index) =>
      makeTreeThread({ id: ThreadId.make(`early-${index + 1}`) }),
    );
    const parent = makeTreeThread({ id: ThreadId.make("parent") });
    const child = makeTreeThread({
      id: ThreadId.make("child"),
      parentThreadId: parent.id,
    });
    const grandchild = makeTreeThread({
      id: ThreadId.make("grandchild"),
      parentThreadId: child.id,
    });
    const rows = buildSidebarThreadTreeRows([...earlyRows, parent, child, grandchild]);
    const filtered = filterSidebarThreadTreeRowsByExpansion(
      rows,
      {
        [sidebarThreadExpansionKey(parent)]: false,
        [sidebarThreadExpansionKey(child)]: false,
      },
      { activeThreadKey: sidebarThreadExpansionKey(grandchild) },
    );

    const preview = getVisibleSidebarThreadTreeRowsForPreview({
      activeThreadKey: sidebarThreadExpansionKey(grandchild),
      isThreadListExpanded: false,
      previewLimit: 3,
      rows: filtered,
    });

    expect(preview.hasOverflowingThreads).toBe(true);
    expect(preview.visibleRows.map((row) => row.thread.id)).toEqual([
      ThreadId.make("early-1"),
      ThreadId.make("early-2"),
      ThreadId.make("early-3"),
      parent.id,
      child.id,
      grandchild.id,
    ]);
    expect(preview.hiddenRows.map((row) => row.thread.id)).toEqual([
      ThreadId.make("early-4"),
      ThreadId.make("early-5"),
    ]);
  });

  it("does not report folded preview overflow when the active path leaves no hidden rows", () => {
    const threads = Array.from({ length: 7 }, (_, index) =>
      makeTreeThread({ id: ThreadId.make(`thread-${index + 1}`) }),
    );
    const rows = buildSidebarThreadTreeRows(threads);

    const preview = getVisibleSidebarThreadTreeRowsForPreview({
      activeThreadKey: sidebarThreadExpansionKey(threads[6]!),
      isThreadListExpanded: false,
      previewLimit: 6,
      rows,
    });

    expect(preview.hasOverflowingThreads).toBe(false);
    expect(preview.visibleRows.map((row) => row.thread.id)).toEqual(
      threads.map((thread) => thread.id),
    );
    expect(preview.hiddenRows).toEqual([]);
  });

  it("keeps child siblings visible when only one nested parent is collapsed", () => {
    const parent = makeTreeThread({ id: ThreadId.make("parent") });
    const child = makeTreeThread({
      id: ThreadId.make("child"),
      parentThreadId: parent.id,
    });
    const grandchild = makeTreeThread({
      id: ThreadId.make("grandchild"),
      parentThreadId: child.id,
    });
    const siblingChild = makeTreeThread({
      id: ThreadId.make("sibling-child"),
      parentThreadId: parent.id,
    });
    const rows = buildSidebarThreadTreeRows([parent, child, grandchild, siblingChild]);

    const filtered = filterSidebarThreadTreeRowsByExpansion(rows, {
      [sidebarThreadExpansionKey(child)]: false,
    });

    expect(filtered.map((row) => row.thread.id)).toEqual([parent.id, child.id, siblingChild.id]);
  });

  it("keeps orphaned and cyclic child links visible as root rows", () => {
    const orphan = makeTreeThread({
      id: ThreadId.make("orphan"),
      parentThreadId: ThreadId.make("missing-parent"),
    });
    const cycleA = makeTreeThread({
      id: ThreadId.make("cycle-a"),
      parentThreadId: ThreadId.make("cycle-b"),
    });
    const cycleB = makeTreeThread({
      id: ThreadId.make("cycle-b"),
      parentThreadId: ThreadId.make("cycle-a"),
    });

    const rows = buildSidebarThreadTreeRows([orphan, cycleA, cycleB]);

    expect(rows.map((row) => row.thread.id)).toEqual([orphan.id, cycleA.id, cycleB.id]);
    expect(rows.map((row) => row.depth)).toEqual([0, 0, 1]);
  });
});

describe("getVisibleThreadsForProject", () => {
  it("includes the active thread even when it falls below the folded preview", () => {
    const threads = Array.from({ length: 8 }, (_, index) =>
      makeThread({
        id: ThreadId.make(`thread-${index + 1}`),
        title: `Thread ${index + 1}`,
      }),
    );

    const result = getVisibleThreadsForProject({
      threads,
      activeThreadId: ThreadId.make("thread-8"),
      isThreadListExpanded: false,
      previewLimit: 6,
    });

    expect(result.hasHiddenThreads).toBe(true);
    expect(result.visibleThreads.map((thread) => thread.id)).toEqual([
      ThreadId.make("thread-1"),
      ThreadId.make("thread-2"),
      ThreadId.make("thread-3"),
      ThreadId.make("thread-4"),
      ThreadId.make("thread-5"),
      ThreadId.make("thread-6"),
      ThreadId.make("thread-8"),
    ]);
    expect(result.hiddenThreads.map((thread) => thread.id)).toEqual([ThreadId.make("thread-7")]);
  });

  it("returns all threads when the list is expanded", () => {
    const threads = Array.from({ length: 8 }, (_, index) =>
      makeThread({
        id: ThreadId.make(`thread-${index + 1}`),
      }),
    );

    const result = getVisibleThreadsForProject({
      threads,
      activeThreadId: ThreadId.make("thread-8"),
      isThreadListExpanded: true,
      previewLimit: 6,
    });

    expect(result.hasHiddenThreads).toBe(true);
    expect(result.visibleThreads.map((thread) => thread.id)).toEqual(
      threads.map((thread) => thread.id),
    );
    expect(result.hiddenThreads).toEqual([]);
  });
});

function makeProject(overrides: Partial<Project> = {}): Project {
  const { defaultModelSelection, ...rest } = overrides;
  return {
    id: ProjectId.make("project-1"),
    environmentId: localEnvironmentId,
    title: "Project",
    workspaceRoot: "/tmp/project",
    repositoryIdentity: null,
    defaultModelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.4",
      ...defaultModelSelection,
    },
    createdAt: "2026-03-09T10:00:00.000Z",
    updatedAt: "2026-03-09T10:00:00.000Z",
    scripts: [],
    ...rest,
  };
}

function makeThread(overrides: Partial<Thread> = {}): Thread {
  return {
    id: ThreadId.make("thread-1"),
    environmentId: localEnvironmentId,
    projectId: ProjectId.make("project-1"),
    title: "Thread",
    modelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.4",
      ...overrides?.modelSelection,
    },
    runtimeMode: DEFAULT_RUNTIME_MODE,
    interactionMode: DEFAULT_INTERACTION_MODE,
    session: null,
    messages: [],
    proposedPlans: [],
    createdAt: "2026-03-09T10:00:00.000Z",
    archivedAt: null,
    deletedAt: null,
    updatedAt: "2026-03-09T10:00:00.000Z",
    latestTurn: null,
    branch: null,
    worktreePath: null,
    checkpoints: [],
    activities: [],
    ...overrides,
  };
}

describe("getFallbackThreadIdAfterDelete", () => {
  it("returns the top remaining thread in the deleted thread's project sidebar order", () => {
    const fallbackThreadId = getFallbackThreadIdAfterDelete({
      threads: [
        makeThread({
          id: ThreadId.make("thread-oldest"),
          projectId: ProjectId.make("project-1"),
          createdAt: "2026-03-09T10:00:00.000Z",
          messages: [],
        }),
        makeThread({
          id: ThreadId.make("thread-active"),
          projectId: ProjectId.make("project-1"),
          createdAt: "2026-03-09T10:05:00.000Z",
          messages: [],
        }),
        makeThread({
          id: ThreadId.make("thread-newest"),
          projectId: ProjectId.make("project-1"),
          createdAt: "2026-03-09T10:10:00.000Z",
          messages: [],
        }),
        makeThread({
          id: ThreadId.make("thread-other-project"),
          projectId: ProjectId.make("project-2"),
          createdAt: "2026-03-09T10:20:00.000Z",
          messages: [],
        }),
      ],
      deletedThreadId: ThreadId.make("thread-active"),
      sortOrder: "created_at",
    });

    expect(fallbackThreadId).toBe(ThreadId.make("thread-newest"));
  });

  it("skips other threads being deleted in the same action", () => {
    const fallbackThreadId = getFallbackThreadIdAfterDelete({
      threads: [
        makeThread({
          id: ThreadId.make("thread-active"),
          projectId: ProjectId.make("project-1"),
          createdAt: "2026-03-09T10:05:00.000Z",
          messages: [],
        }),
        makeThread({
          id: ThreadId.make("thread-newest"),
          projectId: ProjectId.make("project-1"),
          createdAt: "2026-03-09T10:10:00.000Z",
          messages: [],
        }),
        makeThread({
          id: ThreadId.make("thread-next"),
          projectId: ProjectId.make("project-1"),
          createdAt: "2026-03-09T10:07:00.000Z",
          messages: [],
        }),
      ],
      deletedThreadId: ThreadId.make("thread-active"),
      deletedThreadIds: new Set([ThreadId.make("thread-active"), ThreadId.make("thread-newest")]),
      sortOrder: "created_at",
    });

    expect(fallbackThreadId).toBe(ThreadId.make("thread-next"));
  });
});
describe("sortProjectsForSidebar", () => {
  it("sorts projects by the most recent user message across their threads", () => {
    const projects = [
      makeProject({ id: ProjectId.make("project-1"), title: "Older project" }),
      makeProject({ id: ProjectId.make("project-2"), title: "Newer project" }),
    ];
    const threads = [
      makeThread({
        projectId: ProjectId.make("project-1"),
        updatedAt: "2026-03-09T10:20:00.000Z",
        messages: [
          {
            id: "message-1" as never,
            role: "user",
            text: "older project user message",
            turnId: null,
            createdAt: "2026-03-09T10:01:00.000Z",
            updatedAt: "2026-03-09T10:01:00.000Z",
            streaming: false,
          },
        ],
      }),
      makeThread({
        id: ThreadId.make("thread-2"),
        projectId: ProjectId.make("project-2"),
        updatedAt: "2026-03-09T10:05:00.000Z",
        messages: [
          {
            id: "message-2" as never,
            role: "user",
            text: "newer project user message",
            turnId: null,
            createdAt: "2026-03-09T10:05:00.000Z",
            updatedAt: "2026-03-09T10:05:00.000Z",
            streaming: false,
          },
        ],
      }),
    ];

    const sorted = sortProjectsForSidebar(projects, threads, "updated_at");

    expect(sorted.map((project) => project.id)).toEqual([
      ProjectId.make("project-2"),
      ProjectId.make("project-1"),
    ]);
  });

  it("falls back to project timestamps when a project has no threads", () => {
    const sorted = sortProjectsForSidebar(
      [
        makeProject({
          id: ProjectId.make("project-1"),
          title: "Older project",
          updatedAt: "2026-03-09T10:01:00.000Z",
        }),
        makeProject({
          id: ProjectId.make("project-2"),
          title: "Newer project",
          updatedAt: "2026-03-09T10:05:00.000Z",
        }),
      ],
      [],
      "updated_at",
    );

    expect(sorted.map((project) => project.id)).toEqual([
      ProjectId.make("project-2"),
      ProjectId.make("project-1"),
    ]);
  });

  it("falls back to name and id ordering when projects have no sortable timestamps", () => {
    const sorted = sortProjectsForSidebar(
      [
        makeProject({
          id: ProjectId.make("project-2"),
          title: "Beta",
          createdAt: "invalid-created-at" as never,
          updatedAt: "invalid-updated-at" as never,
        }),
        makeProject({
          id: ProjectId.make("project-1"),
          title: "Alpha",
          createdAt: "invalid-created-at" as never,
          updatedAt: "invalid-updated-at" as never,
        }),
      ],
      [],
      "updated_at",
    );

    expect(sorted.map((project) => project.id)).toEqual([
      ProjectId.make("project-1"),
      ProjectId.make("project-2"),
    ]);
  });

  it("preserves manual project ordering", () => {
    const projects = [
      makeProject({ id: ProjectId.make("project-2"), title: "Second" }),
      makeProject({ id: ProjectId.make("project-1"), title: "First" }),
    ];

    const sorted = sortProjectsForSidebar(projects, [], "manual");

    expect(sorted.map((project) => project.id)).toEqual([
      ProjectId.make("project-2"),
      ProjectId.make("project-1"),
    ]);
  });

  it("ignores archived threads when sorting projects", () => {
    const sorted = sortProjectsForSidebar(
      [
        makeProject({
          id: ProjectId.make("project-1"),
          title: "Visible project",
          updatedAt: "2026-03-09T10:01:00.000Z",
        }),
        makeProject({
          id: ProjectId.make("project-2"),
          title: "Archived-only project",
          updatedAt: "2026-03-09T10:00:00.000Z",
        }),
      ],
      [
        makeThread({
          id: ThreadId.make("thread-visible"),
          projectId: ProjectId.make("project-1"),
          updatedAt: "2026-03-09T10:02:00.000Z",
          archivedAt: null,
        }),
        makeThread({
          id: ThreadId.make("thread-archived"),
          projectId: ProjectId.make("project-2"),
          updatedAt: "2026-03-09T10:10:00.000Z",
          archivedAt: "2026-03-09T10:11:00.000Z",
        }),
      ].filter((thread) => thread.archivedAt === null),
      "updated_at",
    );

    expect(sorted.map((project) => project.id)).toEqual([
      ProjectId.make("project-1"),
      ProjectId.make("project-2"),
    ]);
  });

  it("returns the project timestamp when no threads are present", () => {
    const timestamp = getProjectSortTimestamp(
      makeProject({ updatedAt: "2026-03-09T10:10:00.000Z" }),
      [],
      "updated_at",
    );

    expect(timestamp).toBe(Date.parse("2026-03-09T10:10:00.000Z"));
  });
});
