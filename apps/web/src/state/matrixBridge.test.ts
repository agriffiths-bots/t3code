import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentId,
  MatrixBridgeOperationError,
  ThreadId,
  type MatrixBridgeStatus,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/unstable/reactivity";
import { describe, expect, it } from "vite-plus/test";

import {
  canOperateMatrixBridge,
  matrixBridgeFailureMessage,
  matrixBridgeStatusView,
  selectMatrixBridgeMenuState,
  selectMatrixBridgeStatusView,
  type MatrixBridgeEnvironmentState,
} from "./matrixBridge";

const environmentA = EnvironmentId.make("environment-a");
const environmentB = EnvironmentId.make("environment-b");
const ownerThread = ThreadId.make("thread-owner");
const otherThread = ThreadId.make("thread-other");

const activeStatus: MatrixBridgeStatus = {
  state: "active",
  ownerThreadId: ownerThread,
  encryptionReady: true,
  reason: null,
};

const states: ReadonlyMap<EnvironmentId, MatrixBridgeEnvironmentState> = new Map([
  [environmentA, { statusView: { kind: "status", status: activeStatus }, canOperate: true }],
]);

describe("matrixBridgeStatusView", () => {
  it("is pending until the subscription delivers a snapshot", () => {
    expect(matrixBridgeStatusView(AsyncResult.initial())).toEqual({ kind: "pending" });
  });

  it("separates a failed subscription from an unloaded one, so write controls survive it", () => {
    expect(
      matrixBridgeStatusView(
        AsyncResult.failure<MatrixBridgeStatus, Error>(Cause.fail(new Error("not authorized"))),
      ),
    ).toEqual({ kind: "unavailable" });
  });

  it("reports the delivered status", () => {
    expect(matrixBridgeStatusView(AsyncResult.success(activeStatus))).toEqual({
      kind: "status",
      status: activeStatus,
    });
  });
});

describe("canOperateMatrixBridge", () => {
  it("stays optimistic when the environment has not reported scopes", () => {
    expect(canOperateMatrixBridge(null)).toBe(true);
    expect(canOperateMatrixBridge({})).toBe(true);
  });

  it("denies a read-scoped client, which cannot call setOwner", () => {
    expect(canOperateMatrixBridge({ scopes: [AuthOrchestrationReadScope] })).toBe(false);
  });

  it("allows a client granted orchestration:operate", () => {
    expect(
      canOperateMatrixBridge({
        scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
      }),
    ).toBe(true);
  });
});

describe("selectMatrixBridgeMenuState", () => {
  it("routes each thread to its own environment's bridge", () => {
    expect(selectMatrixBridgeMenuState(states, scopeThreadRef(environmentA, otherThread))).toEqual({
      supported: true,
      canOperate: true,
      ownerThreadId: ownerThread,
      threadId: otherThread,
    });
  });

  it("treats an environment without an entry as one without the capability", () => {
    expect(selectMatrixBridgeMenuState(states, scopeThreadRef(environmentB, otherThread))).toEqual({
      supported: false,
      canOperate: false,
      ownerThreadId: null,
      threadId: otherThread,
    });
  });

  it("has no owner before the status subscription delivers one", () => {
    const pending = new Map([
      [environmentA, { statusView: { kind: "pending" } as const, canOperate: true }],
    ]);
    expect(
      selectMatrixBridgeMenuState(pending, scopeThreadRef(environmentA, otherThread)),
    ).toMatchObject({ supported: true, ownerThreadId: null });
  });
});

describe("selectMatrixBridgeStatusView", () => {
  it("returns the status of that environment only", () => {
    expect(selectMatrixBridgeStatusView(states, environmentA)).toEqual({
      kind: "status",
      status: activeStatus,
    });
    expect(selectMatrixBridgeStatusView(states, environmentB)).toEqual({ kind: "pending" });
    expect(selectMatrixBridgeStatusView(states, null)).toEqual({ kind: "pending" });
  });
});

describe("matrixBridgeFailureMessage", () => {
  it("surfaces the server's sanitized operation message", () => {
    expect(
      matrixBridgeFailureMessage(
        new MatrixBridgeOperationError({
          reason: "threadArchived",
          message: "An archived thread cannot own the Matrix bridge.",
        }),
        "An error occurred.",
      ),
    ).toBe("An archived thread cannot own the Matrix bridge.");
  });

  it("surfaces transport errors", () => {
    expect(matrixBridgeFailureMessage(new Error("Backend is not connected."), "fallback")).toBe(
      "Backend is not connected.",
    );
  });

  it("falls back rather than rendering an unshaped cause", () => {
    expect(matrixBridgeFailureMessage({ cause: "opaque" }, "An error occurred.")).toBe(
      "An error occurred.",
    );
    expect(matrixBridgeFailureMessage(new Error("   "), "An error occurred.")).toBe(
      "An error occurred.",
    );
  });
});
