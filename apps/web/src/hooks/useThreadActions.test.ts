import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  buildThreadDeleteConfirmationMessage,
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
});
