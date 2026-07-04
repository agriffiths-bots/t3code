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

  it("ignores stopped or error session runtimes because they can be stale", () => {
    assert.equal(
      resolveCosWakeRuntimeMode({
        runtimeMode: "approval-required",
        session: {
          status: "stopped",
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
          runtimeMode: "full-access",
        },
      } as OrchestrationThread),
      "approval-required",
    );
  });
});
