import { expect, it } from "vite-plus/test";

import { MAX_THREAD_CHECKPOINTS } from "../orchestration/checkpointRetention.ts";
import { CHECKPOINT_REF_LIST_MAX_OUTPUT_BYTES } from "./GitVcsDriver.ts";
import { CHECKPOINT_REFS_KEEP_PER_THREAD } from "./VcsMaintenanceReactor.ts";

it("retains checkpoint reference maintenance after worktree teardown moved to lifecycle events", () => {
  expect(CHECKPOINT_REFS_KEEP_PER_THREAD).toBe(MAX_THREAD_CHECKPOINTS + 2);
  expect(CHECKPOINT_REF_LIST_MAX_OUTPUT_BYTES).toBeGreaterThanOrEqual(64 * 1024 * 1024);
});
