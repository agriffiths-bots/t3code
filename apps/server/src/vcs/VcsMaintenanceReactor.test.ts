import { describe, expect, it } from "vite-plus/test";

import { MAX_THREAD_CHECKPOINTS } from "../orchestration/checkpointRetention.ts";
import {
  CHECKPOINT_REFS_KEEP_PER_THREAD,
  selectStaleWorktreeReapCandidates,
  type WorktreeMaintenanceRow,
} from "./VcsMaintenanceReactor.ts";

const NOW = Date.parse("2026-07-06T12:00:00.000Z");

function row(
  overrides: Partial<WorktreeMaintenanceRow> & Pick<WorktreeMaintenanceRow, "threadId">,
): WorktreeMaintenanceRow {
  return {
    threadId: overrides.threadId,
    projectCwd: overrides.projectCwd ?? "/repo",
    worktreePath: overrides.worktreePath ?? `/worktrees/${overrides.threadId}`,
    worktreeRemovable: overrides.worktreeRemovable ?? true,
    worktreeRemovalPath: overrides.worktreeRemovalPath ?? null,
    updatedAt: overrides.updatedAt ?? "2026-07-05T00:00:00.000Z",
    archivedAt: overrides.archivedAt ?? null,
    deletedAt: overrides.deletedAt ?? null,
    sessionStatus: overrides.sessionStatus ?? "stopped",
    runtimeStatus: overrides.runtimeStatus ?? null,
    pendingApprovalCount: overrides.pendingApprovalCount ?? 0,
    pendingUserInputCount: overrides.pendingUserInputCount ?? 0,
  };
}

describe("selectStaleWorktreeReapCandidates", () => {
  it("keeps one extra physical checkpoint ref for the turn zero baseline", () => {
    expect(CHECKPOINT_REFS_KEEP_PER_THREAD).toBe(MAX_THREAD_CHECKPOINTS + 1);
  });

  it("selects old removable stopped worktrees", () => {
    expect(
      selectStaleWorktreeReapCandidates([row({ threadId: "thread-old" })], ["/repo"], NOW, {
        stoppedAgeMs: 60_000,
      }),
    ).toEqual([
      {
        threadId: "thread-old",
        threadIds: ["thread-old"],
        projectCwd: "/repo",
        path: "/worktrees/thread-old",
      },
    ]);
  });

  it("keeps active, pending, young, shared, and project-root paths", () => {
    const rows = [
      row({ threadId: "running", sessionStatus: "running" }),
      row({ threadId: "starting", sessionStatus: "starting" }),
      row({ threadId: "waiting", sessionStatus: "waiting" }),
      row({ threadId: "idle", sessionStatus: "idle" }),
      row({ threadId: "interrupted", sessionStatus: "interrupted" }),
      row({ threadId: "ready-runtime", runtimeStatus: "ready" }),
      row({ threadId: "waiting-runtime", runtimeStatus: "waiting" }),
      row({ threadId: "starting-runtime", runtimeStatus: "starting" }),
      row({ threadId: "approval", pendingApprovalCount: 1 }),
      row({ threadId: "input", pendingUserInputCount: 1 }),
      row({ threadId: "young", updatedAt: "2026-07-06T11:59:30.000Z" }),
      row({ threadId: "not-removable", worktreeRemovable: false }),
      row({ threadId: "project-root", worktreePath: "/repo" }),
      row({ threadId: "nested-project-path", worktreePath: "/repo/worktrees/old" }),
      row({ threadId: "shared-a", worktreePath: "/worktrees/shared" }),
      row({
        threadId: "shared-b",
        worktreePath: "/worktrees/shared/nested",
        worktreeRemovable: false,
      }),
    ];

    expect(
      selectStaleWorktreeReapCandidates(rows, ["/repo"], NOW, {
        stoppedAgeMs: 60_000,
      }),
    ).toEqual([]);
  });

  it("keeps stale paths that overlap a live thread path", () => {
    const rows = [
      row({
        threadId: "stale-parent",
        worktreePath: "/worktrees/shared",
      }),
      row({
        threadId: "live-child",
        worktreePath: "/worktrees/shared/nested",
        worktreeRemovable: false,
      }),
      row({
        threadId: "stale-child",
        worktreePath: "/worktrees/live/nested",
      }),
      row({
        threadId: "live-parent",
        worktreePath: "/worktrees/live",
        worktreeRemovable: false,
      }),
    ];

    expect(
      selectStaleWorktreeReapCandidates(rows, ["/repo"], NOW, {
        stoppedAgeMs: 60_000,
      }),
    ).toEqual([]);
  });

  it("selects shared stale removal roots once and clears every stale owner", () => {
    const rows = [
      row({
        threadId: "shared-a",
        worktreePath: "/worktrees/shared/a",
        worktreeRemovalPath: "/worktrees/shared",
      }),
      row({
        threadId: "shared-b",
        worktreePath: "/worktrees/shared/b",
        worktreeRemovalPath: "/worktrees/shared",
        archivedAt: "2026-07-05T00:00:00.000Z",
      }),
    ];

    expect(
      selectStaleWorktreeReapCandidates(rows, ["/repo"], NOW, {
        stoppedAgeMs: 60_000,
      }),
    ).toEqual([
      {
        threadId: "shared-a",
        threadIds: ["shared-a", "shared-b"],
        projectCwd: "/repo",
        path: "/worktrees/shared",
      },
    ]);
  });

  it("prefers a stale parent removal root over stale nested roots", () => {
    const rows = [
      row({
        threadId: "stale-child",
        worktreePath: "/worktrees/shared/child",
      }),
      row({
        threadId: "stale-parent",
        worktreePath: "/worktrees/shared",
      }),
    ];

    expect(
      selectStaleWorktreeReapCandidates(rows, ["/repo"], NOW, {
        stoppedAgeMs: 60_000,
      }),
    ).toEqual([
      {
        threadId: "stale-parent",
        threadIds: ["stale-child", "stale-parent"],
        projectCwd: "/repo",
        path: "/worktrees/shared",
      },
    ]);
  });

  it("uses the shorter archived age for archived and deleted threads", () => {
    const rows = [
      row({
        threadId: "archived",
        archivedAt: "2026-07-06T11:30:00.000Z",
        updatedAt: "2026-07-06T11:30:00.000Z",
      }),
      row({
        threadId: "deleted",
        deletedAt: "2026-07-06T11:30:00.000Z",
        updatedAt: "2026-07-06T11:30:00.000Z",
      }),
      row({
        threadId: "stopped",
        updatedAt: "2026-07-06T11:30:00.000Z",
      }),
    ];

    expect(
      selectStaleWorktreeReapCandidates(rows, ["/repo"], NOW, {
        archivedAgeMs: 20 * 60_000,
        stoppedAgeMs: 60 * 60_000,
      }).map((candidate) => candidate.threadId),
    ).toEqual(["archived", "deleted"]);
  });
});
