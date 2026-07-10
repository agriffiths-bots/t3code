import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  buildThreadDeleteConfirmationMessage,
  buildThreadsDeleteConfirmationMessage,
  isThreadDeleteConfirmationCurrent,
  ThreadArchiveBlockedError,
} from "./useThreadActions";

describe("ThreadArchiveBlockedError", () => {
  it("keeps the blocked thread context with the fixed message", () => {
    const error = new ThreadArchiveBlockedError({
      environmentId: EnvironmentId.make("environment-1"),
      threadId: ThreadId.make("thread-1"),
    });

    expect(error).toMatchObject({
      environmentId: "environment-1",
      threadId: "thread-1",
    });
    expect(error.message).toBe("Cannot archive a running thread.");
  });
});

describe("buildThreadDeleteConfirmationMessage", () => {
  it("warns that deleting an owned worktree loses uncommitted files", () => {
    expect(
      buildThreadDeleteConfirmationMessage({
        title: "Lifecycle work",
        worktreePath: "/tmp/t3-worktrees/lifecycle-work/packages/app",
        worktreeRemovable: true,
        worktreeRemovalPath: "/tmp/t3-worktrees/lifecycle-work",
      }),
    ).toContain(
      "This also permanently deletes its T3-created worktree when no other thread or project uses it:\nlifecycle-work\nUncommitted and untracked files in that worktree will be lost.",
    );
  });

  it("warns about every owned worktree in a multi-thread deletion", () => {
    expect(
      buildThreadsDeleteConfirmationMessage([
        {
          worktreePath: "/tmp/t3-worktrees/first",
          worktreeRemovable: true,
          worktreeRemovalPath: "/tmp/t3-worktrees/first",
        },
        {
          worktreePath: "/tmp/t3-worktrees/second/packages/app",
          worktreeRemovable: true,
          worktreeRemovalPath: "/tmp/t3-worktrees/second",
        },
      ]),
    ).toContain(
      "T3-created worktrees when no other thread or project uses them:\n- first\n- second\nUncommitted and untracked files in those worktrees will be lost.",
    );
  });

  it("invalidates a confirmation when worktree ownership appears while the dialog is open", () => {
    expect(
      isThreadDeleteConfirmationCurrent(null, {
        title: "Lifecycle work",
        worktreePath: "/tmp/t3-worktrees/lifecycle-work",
        worktreeRemovable: true,
        worktreeRemovalPath: "/tmp/t3-worktrees/lifecycle-work",
      }),
    ).toBe(false);
  });
});
