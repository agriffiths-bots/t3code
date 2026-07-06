import { describe, expect, it } from "vite-plus/test";

import { MAX_THREAD_CHECKPOINTS } from "../orchestration/checkpointRetention.ts";
import {
  CHECKPOINT_REFS_KEEP_PER_THREAD,
  isWorktreePathListed,
  selectStaleWorktreeReapCandidates,
  shouldRetainWorktreeMetadataAfterListFailure,
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
  it("keeps physical refs for the turn zero baseline and boundary predecessor", () => {
    expect(CHECKPOINT_REFS_KEEP_PER_THREAD).toBe(MAX_THREAD_CHECKPOINTS + 2);
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

  it("coalesces shared stale archived worktree paths", () => {
    expect(
      selectStaleWorktreeReapCandidates(
        [
          row({
            threadId: "archived-a",
            worktreePath: "/worktrees/shared-archived",
            archivedAt: "2026-07-06T11:30:00.000Z",
            updatedAt: "2026-07-06T11:30:00.000Z",
          }),
          row({
            threadId: "archived-b",
            worktreePath: "/worktrees/shared-archived",
            archivedAt: "2026-07-06T11:30:00.000Z",
            updatedAt: "2026-07-06T11:30:00.000Z",
          }),
        ],
        ["/repo"],
        NOW,
        {
          archivedAgeMs: 20 * 60_000,
          stoppedAgeMs: 60 * 60_000,
        },
      ),
    ).toEqual([
      {
        threadId: "archived-a",
        threadIds: ["archived-a", "archived-b"],
        projectCwd: "/repo",
        path: "/worktrees/shared-archived",
      },
    ]);
  });

  it("does not let deleted metadata rows retain a stale worktree path", () => {
    expect(
      selectStaleWorktreeReapCandidates(
        [
          row({
            threadId: "stale",
            worktreePath: "/worktrees/deleted-row-overlap",
          }),
          row({
            threadId: "deleted-metadata",
            worktreePath: "/worktrees/deleted-row-overlap",
            worktreeRemovable: false,
            deletedAt: "2026-07-06T11:30:00.000Z",
          }),
        ],
        ["/repo"],
        NOW,
        {
          stoppedAgeMs: 60_000,
        },
      ),
    ).toEqual([
      {
        threadId: "stale",
        threadIds: ["stale"],
        projectCwd: "/repo",
        path: "/worktrees/deleted-row-overlap",
      },
    ]);
  });

  it("ignores stale pending counters on deleted threads", () => {
    expect(
      selectStaleWorktreeReapCandidates(
        [
          row({
            threadId: "deleted-pending",
            deletedAt: "2026-07-06T11:30:00.000Z",
            updatedAt: "2026-07-06T11:30:00.000Z",
            pendingApprovalCount: 1,
            pendingUserInputCount: 1,
          }),
        ],
        ["/repo"],
        NOW,
        {
          archivedAgeMs: 20 * 60_000,
        },
      ),
    ).toEqual([
      {
        threadId: "deleted-pending",
        threadIds: ["deleted-pending"],
        projectCwd: "/repo",
        path: "/worktrees/deleted-pending",
      },
    ]);
  });

  it("ignores stale pending counters on archived threads", () => {
    expect(
      selectStaleWorktreeReapCandidates(
        [
          row({
            threadId: "archived-pending",
            archivedAt: "2026-07-06T11:30:00.000Z",
            updatedAt: "2026-07-06T11:30:00.000Z",
            pendingApprovalCount: 1,
            pendingUserInputCount: 1,
          }),
        ],
        ["/repo"],
        NOW,
        {
          archivedAgeMs: 20 * 60_000,
        },
      ),
    ).toEqual([
      {
        threadId: "archived-pending",
        threadIds: ["archived-pending"],
        projectCwd: "/repo",
        path: "/worktrees/archived-pending",
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

describe("worktree registration helpers", () => {
  it("matches exact normalized paths in git worktree porcelain output", () => {
    expect(
      isWorktreePathListed(
        [
          "worktree /repo",
          "HEAD abc",
          "branch refs/heads/main",
          "",
          "worktree /tmp/t3-worktree",
          "HEAD def",
          "branch refs/heads/feature",
          "",
        ].join("\n"),
        "/tmp/t3-worktree",
      ),
    ).toBe(true);

    expect(isWorktreePathListed("worktree /tmp/t3-worktree-child\n", "/tmp/t3-worktree")).toBe(
      false,
    );
  });

  it("only clears metadata after a failed worktree listing when both paths are gone", () => {
    expect(
      shouldRetainWorktreeMetadataAfterListFailure({
        projectRootExists: false,
        worktreePathExists: false,
      }),
    ).toBe(false);
    expect(
      shouldRetainWorktreeMetadataAfterListFailure({
        projectRootExists: true,
        worktreePathExists: false,
      }),
    ).toBe(true);
    expect(
      shouldRetainWorktreeMetadataAfterListFailure({
        projectRootExists: false,
        worktreePathExists: true,
      }),
    ).toBe(true);
    expect(
      shouldRetainWorktreeMetadataAfterListFailure({
        projectRootExists: true,
        worktreePathExists: false,
        detail: "fatal: not a git repository",
      }),
    ).toBe(false);
    expect(
      shouldRetainWorktreeMetadataAfterListFailure({
        projectRootExists: true,
        worktreePathExists: false,
        detail: "permission denied",
      }),
    ).toBe(true);
  });
});
