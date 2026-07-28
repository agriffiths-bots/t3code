import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { EnvironmentId, ProjectId, ProviderInstanceId, ThreadId, TurnId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  buildThreadListV2Items,
  resolveThreadListV2Status,
  selectThreadListV2PrSettlementCandidates,
  sortThreadsForListV2,
} from "./threadListV2";

const environmentId = EnvironmentId.make("environment-1");

function makeThread(
  input: Partial<EnvironmentThreadShell> & Pick<EnvironmentThreadShell, "id" | "title">,
): EnvironmentThreadShell {
  return {
    environmentId,
    projectId: ProjectId.make("project-1"),
    dataAudience: "private",
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    latestTurn: null,
    createdAt: "2026-06-01T00:00:00.000Z",
    updatedAt: "2026-06-01T00:00:00.000Z",
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    session: null,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    parentThreadId: null,
    ...input,
  };
}

const NOW = "2026-06-02T00:00:00.000Z";

describe("resolveThreadListV2Status", () => {
  it("prioritizes approval over a running session", () => {
    const thread = makeThread({
      id: ThreadId.make("t"),
      title: "t",
      hasPendingApprovals: true,
      session: {
        threadId: ThreadId.make("t"),
        status: "running",
        providerName: "Codex",
        providerInstanceId: ProviderInstanceId.make("codex"),
        runtimeMode: "full-access",
        activeTurnId: null,
        lastError: null,
        updatedAt: NOW,
      },
    });
    expect(resolveThreadListV2Status(thread)).toBe("approval");
  });

  it("resolves ready for quiescent threads", () => {
    expect(resolveThreadListV2Status(makeThread({ id: ThreadId.make("t"), title: "t" }))).toBe(
      "ready",
    );
  });

  it("keeps an actionable proposed plan visible after the session settles", () => {
    expect(
      resolveThreadListV2Status(
        makeThread({
          id: ThreadId.make("plan-ready"),
          title: "Plan ready",
          hasActionableProposedPlan: true,
          interactionMode: "plan",
          latestTurn: {
            turnId: TurnId.make("plan-turn"),
            state: "completed",
            assistantMessageId: null,
            requestedAt: NOW,
            startedAt: NOW,
            completedAt: NOW,
          },
        }),
      ),
    ).toBe("plan");
  });

  it("treats waiting as working only while it carries an active turn", () => {
    const waiting = makeThread({
      id: ThreadId.make("waiting"),
      title: "Waiting",
      session: {
        threadId: ThreadId.make("waiting"),
        status: "waiting",
        providerName: "Codex",
        providerInstanceId: ProviderInstanceId.make("codex"),
        runtimeMode: "full-access",
        activeTurnId: TurnId.make("turn-waiting"),
        lastError: null,
        updatedAt: NOW,
      },
    });
    expect(resolveThreadListV2Status(waiting)).toBe("working");
    expect(
      resolveThreadListV2Status({
        ...waiting,
        session: waiting.session ? { ...waiting.session, activeTurnId: null } : null,
      }),
    ).toBe("ready");
  });

  it("reports working for a running latest turn before session state arrives", () => {
    expect(
      resolveThreadListV2Status(
        makeThread({
          id: ThreadId.make("latest-running"),
          title: "Latest running",
          latestTurn: {
            turnId: TurnId.make("turn-latest-running"),
            state: "running",
            assistantMessageId: null,
            requestedAt: NOW,
            startedAt: NOW,
            completedAt: null,
          },
        }),
      ),
    ).toBe("working");
  });
});

describe("sortThreadsForListV2", () => {
  it("orders by creation time, newest first, ignoring activity", () => {
    const sorted = sortThreadsForListV2([
      { id: "oldest", createdAt: "2026-06-01T08:00:00.000Z" },
      { id: "newest", createdAt: "2026-06-01T12:00:00.000Z" },
      { id: "middle", createdAt: "2026-06-01T10:00:00.000Z" },
    ]);
    expect(sorted.map((thread) => thread.id)).toEqual(["newest", "middle", "oldest"]);
  });
});

describe("buildThreadListV2Items", () => {
  it("partitions settled threads into a slim tail with one divider", () => {
    const { items } = buildThreadListV2Items({
      threads: [
        makeThread({ id: ThreadId.make("active"), title: "Active" }),
        makeThread({
          id: ThreadId.make("settled"),
          title: "Settled",
          settledOverride: "settled",
          settledAt: NOW,
        }),
        makeThread({
          id: ThreadId.make("settled-2"),
          title: "Settled 2",
          settledOverride: "settled",
          settledAt: NOW,
        }),
      ],
      environmentId: null,
      searchQuery: "",
      now: NOW,
    });

    expect(items.map((item) => [item.thread.id, item.variant])).toEqual([
      ["active", "card"],
      ["settled", "slim"],
      ["settled-2", "slim"],
    ]);
    expect(items.map((item) => item.showSettledDivider)).toEqual([false, true, false]);
    expect(items.map((item) => item.isLast)).toEqual([false, false, true]);
  });

  it("keeps cards in creation order while settled sorts by recency", () => {
    const { items } = buildThreadListV2Items({
      threads: [
        makeThread({
          id: ThreadId.make("older-created"),
          title: "Older",
          createdAt: "2026-06-01T08:00:00.000Z",
          updatedAt: NOW, // recent activity must NOT promote it
        }),
        makeThread({
          id: ThreadId.make("newer-created"),
          title: "Newer",
          createdAt: "2026-06-01T12:00:00.000Z",
        }),
      ],
      environmentId: null,
      searchQuery: "",
      now: NOW,
    });

    expect(items.map((item) => item.thread.id)).toEqual(["newer-created", "older-created"]);
  });

  it("keeps settled threads in the tail and filters by search query", () => {
    const { items } = buildThreadListV2Items({
      threads: [
        makeThread({ id: ThreadId.make("match"), title: "Fix login bug" }),
        makeThread({ id: ThreadId.make("miss"), title: "Greeting" }),
        makeThread({
          id: ThreadId.make("settled"),
          title: "Fix login again",
          settledOverride: "settled",
          settledAt: NOW,
        }),
      ],
      environmentId: null,
      searchQuery: "login",
      now: NOW,
    });

    expect(items.map((item) => [item.thread.id, item.variant])).toEqual([
      ["match", "card"],
      ["settled", "slim"],
    ]);
  });

  it("scopes the flat list to one project", () => {
    const otherProjectId = ProjectId.make("project-2");
    const { items } = buildThreadListV2Items({
      threads: [
        makeThread({ id: ThreadId.make("included"), title: "Included" }),
        makeThread({
          id: ThreadId.make("excluded"),
          projectId: otherProjectId,
          title: "Excluded",
        }),
      ],
      environmentId: null,
      projectRef: { environmentId, projectId: ProjectId.make("project-1") },
      searchQuery: "",
      now: NOW,
    });

    expect(items.map((item) => item.thread.id)).toEqual(["included"]);
  });

  it("keeps a mixed parent/child tree together while settling rows independently", () => {
    const parentId = ThreadId.make("parent");
    const { items } = buildThreadListV2Items({
      threads: [
        makeThread({
          id: parentId,
          title: "Settled parent",
          settledOverride: "settled",
          settledAt: NOW,
        }),
        makeThread({
          id: ThreadId.make("active-child"),
          title: "Active child",
          parentThreadId: parentId,
        }),
      ],
      environmentId: null,
      searchQuery: "",
      now: NOW,
    });

    expect(
      items.map((item) => ({
        id: item.thread.id,
        variant: item.variant,
        depth: item.treeDepth,
        open: item.unsettledDescendantCount,
        divider: item.showSettledDivider,
      })),
    ).toEqual([
      { id: "parent", variant: "slim", depth: 0, open: 1, divider: false },
      { id: "active-child", variant: "card", depth: 1, open: 0, divider: false },
    ]);
  });
});

describe("buildThreadListV2Items settled paging", () => {
  it("does not auto-settle inactive threads when no mobile threshold is configured", () => {
    const inactive = makeThread({
      id: ThreadId.make("inactive"),
      title: "Inactive but open",
      createdAt: "2026-01-01T00:00:00.000Z",
      updatedAt: "2026-01-01T00:00:00.000Z",
    });

    const layout = buildThreadListV2Items({
      threads: [inactive],
      environmentId: null,
      searchQuery: "",
      now: NOW,
    });

    expect(layout.items).toHaveLength(1);
    expect(layout.items[0]?.variant).toBe("card");
  });

  it("caps the settled tail at settledLimit and reports the hidden count", () => {
    const threads = [
      makeThread({ id: ThreadId.make("active"), title: "Active" }),
      ...Array.from({ length: 4 }, (_, index) =>
        makeThread({
          id: ThreadId.make(`settled-${index}`),
          title: `Settled ${index}`,
          settledOverride: "settled",
          settledAt: NOW,
          latestUserMessageAt: `2026-06-01T0${index}:00:00.000Z`,
          // A turn adopted the message (same requestedAt): without it the
          // thread reads as a queued turn start, which never settles.
          latestTurn: {
            turnId: TurnId.make(`turn-${index}`),
            state: "completed",
            requestedAt: `2026-06-01T0${index}:00:00.000Z`,
            startedAt: `2026-06-01T0${index}:00:00.000Z`,
            completedAt: `2026-06-01T0${index}:10:00.000Z`,
            assistantMessageId: null,
          },
        }),
      ),
    ];

    const layout = buildThreadListV2Items({
      threads,
      environmentId: null,
      searchQuery: "",
      settledLimit: 2,
      now: NOW,
    });

    expect(layout.hiddenSettledCount).toBe(2);
    expect(layout.items.filter((item) => item.variant === "slim")).toHaveLength(2);
    // Most recent settled first — the hidden ones are the oldest.
    expect(layout.items.map((item) => item.thread.id)).toEqual([
      "active",
      "settled-3",
      "settled-2",
    ]);
  });

  it("keeps the selected settled tree visible beyond the first page", () => {
    const selectedRootId = ThreadId.make("selected-page-0");
    const threads = [
      ...Array.from({ length: 3 }, (_, index) =>
        makeThread({
          id: ThreadId.make(`selected-page-${index}`),
          title: `Settled ${index}`,
          settledOverride: "settled",
          settledAt: NOW,
          latestUserMessageAt: `2026-06-01T0${index}:00:00.000Z`,
          latestTurn: {
            turnId: TurnId.make(`selected-turn-${index}`),
            state: "completed",
            requestedAt: `2026-06-01T0${index}:00:00.000Z`,
            startedAt: `2026-06-01T0${index}:00:00.000Z`,
            completedAt: `2026-06-01T0${index}:10:00.000Z`,
            assistantMessageId: null,
          },
        }),
      ),
      makeThread({
        id: ThreadId.make("selected-page-child"),
        title: "Selected settled child",
        parentThreadId: selectedRootId,
        settledOverride: "settled",
        settledAt: NOW,
      }),
    ];

    const layout = buildThreadListV2Items({
      threads,
      environmentId: null,
      searchQuery: "",
      settledLimit: 1,
      selectedThreadKey: `${environmentId}:selected-page-child`,
      now: NOW,
    });

    expect(layout.items.map((item) => item.thread.id)).toEqual([
      "selected-page-2",
      "selected-page-0",
      "selected-page-child",
    ]);
    expect(layout.items.map((item) => item.treeDepth)).toEqual([0, 0, 1]);
    expect(layout.hiddenSettledCount).toBe(1);
  });
});

describe("selectThreadListV2PrSettlementCandidates", () => {
  it("keeps PR-derived settlement observed while excluding explicit lifecycle states", () => {
    const candidate = makeThread({
      id: ThreadId.make("candidate"),
      title: "Candidate",
      branch: "feature/candidate",
    });
    const candidates = selectThreadListV2PrSettlementCandidates({
      threads: [
        candidate,
        { ...candidate, id: ThreadId.make("explicit-settled"), settledOverride: "settled" },
        { ...candidate, id: ThreadId.make("pinned-active"), settledOverride: "active" },
        { ...candidate, id: ThreadId.make("pending"), hasPendingApprovals: true },
      ],
      environmentId: null,
      searchQuery: "",
      now: NOW,
    });

    expect(candidates.map((thread) => thread.id)).toEqual(["candidate"]);
  });
});
