import { describe, expect, it } from "vite-plus/test";

import {
  hasActiveThreadSession,
  hasThreadAttentionBlocker,
  resolveThreadAttentionBlocker,
} from "./threadAttention.ts";

describe("thread attention blockers", () => {
  it("classifies the attention states in status priority order", () => {
    expect(
      resolveThreadAttentionBlocker({
        hasPendingApprovals: true,
        hasPendingUserInput: true,
        session: { status: "running", activeTurnId: "turn-1" },
      }),
    ).toBe("approval");
    expect(resolveThreadAttentionBlocker({ hasPendingUserInput: true })).toBe("input");
    expect(resolveThreadAttentionBlocker({ session: { status: "error" } })).toBe("failed");
    expect(resolveThreadAttentionBlocker({ latestTurn: { state: "error" } })).toBe("failed");
    expect(resolveThreadAttentionBlocker({ session: { status: "running" } })).toBe("working");
    expect(resolveThreadAttentionBlocker({ latestTurn: { state: "running" } })).toBe("working");
    expect(resolveThreadAttentionBlocker({ hasActionableProposedPlan: true })).toBe("plan");
    expect(resolveThreadAttentionBlocker({})).toBeNull();
  });

  it("treats only live waiting sessions as active work", () => {
    expect(hasActiveThreadSession({ status: "waiting", activeTurnId: "turn-1" })).toBe(true);
    expect(hasActiveThreadSession({ status: "waiting", activeTurnId: null })).toBe(false);
    expect(hasActiveThreadSession({ status: "stopped", activeTurnId: "turn-1" })).toBe(false);
  });

  it("exposes a boolean predicate for settle guards", () => {
    expect(hasThreadAttentionBlocker({ hasActionableProposedPlan: true })).toBe(true);
    expect(hasThreadAttentionBlocker({ session: { status: "ready" } })).toBe(false);
  });
});
