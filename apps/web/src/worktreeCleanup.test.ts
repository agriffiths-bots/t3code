import { describe, expect, it } from "vite-plus/test";

import { formatWorktreePathForDisplay } from "./worktreeCleanup";

describe("formatWorktreePathForDisplay", () => {
  it("shows the final normalized path segment", () => {
    expect(formatWorktreePathForDisplay("/tmp/custom-worktrees/my-worktree/")).toBe("my-worktree");
    expect(formatWorktreePathForDisplay("C:\\Users\\julius\\.t3\\worktrees\\my-worktree")).toBe(
      "my-worktree",
    );
  });
});
