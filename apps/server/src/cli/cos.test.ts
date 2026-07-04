import { assert, describe, it } from "@effect/vitest";

import type { OrchestrationThread } from "@t3tools/contracts";
import { resolveCosWakeRuntimeMode } from "./cos.ts";

describe("resolveCosWakeRuntimeMode", () => {
  it("prefers the active session runtime over the thread default", () => {
    const thread = {
      runtimeMode: "full-access",
      session: {
        runtimeMode: "approval-required",
      },
    } as OrchestrationThread;

    assert.equal(resolveCosWakeRuntimeMode(thread), "approval-required");
  });

  it("falls back to the thread runtime when there is no active session", () => {
    const thread = {
      runtimeMode: "auto-accept-edits",
      session: null,
    } as OrchestrationThread;

    assert.equal(resolveCosWakeRuntimeMode(thread), "auto-accept-edits");
  });

  it("ignores stopped and unstarted error session runtimes because they can be stale", () => {
    assert.equal(
      resolveCosWakeRuntimeMode({
        runtimeMode: "approval-required",
        session: {
          status: "stopped",
          activeTurnId: "turn-1",
          runtimeMode: "full-access",
        },
      } as OrchestrationThread),
      "approval-required",
    );
    assert.equal(
      resolveCosWakeRuntimeMode({
        runtimeMode: "approval-required",
        session: {
          status: "error",
          activeTurnId: null,
          runtimeMode: "full-access",
        },
      } as OrchestrationThread),
      "approval-required",
    );
  });

  it("keeps errored started session runtime for wake retries", () => {
    assert.equal(
      resolveCosWakeRuntimeMode({
        runtimeMode: "full-access",
        session: {
          status: "error",
          activeTurnId: "turn-1",
          runtimeMode: "approval-required",
        },
      } as OrchestrationThread),
      "approval-required",
    );
  });
});
