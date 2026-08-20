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
  matrixBridgeOwnership,
  matrixBridgeStatusView,
  matrixBridgeSavedConfigView,
  selectMatrixBridgeMenuState,
  selectMatrixBridgeSavedConfig,
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
  [
    environmentA,
    { statusView: { kind: "status", status: activeStatus }, savedConfig: null, canOperate: true },
  ],
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

describe("matrixBridgeOwnership", () => {
  it("reports no configuration rather than an empty owner when the bridge is off", () => {
    expect(
      matrixBridgeOwnership({
        kind: "status",
        status: { ...activeStatus, state: "disabled", ownerThreadId: null },
      }),
    ).toEqual({ kind: "unconfigured" });
  });

  it("keeps an unreadable status unknown, so an operate-only session is not told there is no owner", () => {
    expect(matrixBridgeOwnership({ kind: "unavailable" })).toEqual({ kind: "unknown" });
    expect(matrixBridgeOwnership({ kind: "pending" })).toEqual({ kind: "unknown" });
  });

  it("reports the owner pointer once the status is readable", () => {
    expect(matrixBridgeOwnership({ kind: "status", status: activeStatus })).toEqual({
      kind: "owner",
      ownerThreadId: ownerThread,
    });
  });
});

describe("selectMatrixBridgeMenuState", () => {
  it("routes each thread to its own environment's bridge", () => {
    expect(selectMatrixBridgeMenuState(states, scopeThreadRef(environmentA, otherThread))).toEqual({
      supported: true,
      canOperate: true,
      ownership: { kind: "owner", ownerThreadId: ownerThread },
      threadId: otherThread,
    });
  });

  it("treats an environment without an entry as one without the capability", () => {
    expect(selectMatrixBridgeMenuState(states, scopeThreadRef(environmentB, otherThread))).toEqual({
      supported: false,
      canOperate: false,
      ownership: { kind: "unknown" },
      threadId: otherThread,
    });
  });

  it("has unknown ownership before the status subscription delivers one", () => {
    const pending = new Map([
      [
        environmentA,
        { statusView: { kind: "pending" } as const, savedConfig: null, canOperate: true },
      ],
    ]);
    expect(
      selectMatrixBridgeMenuState(pending, scopeThreadRef(environmentA, otherThread)),
    ).toMatchObject({ supported: true, ownership: { kind: "unknown" } });
  });
});

describe("selectMatrixBridgeSavedConfig", () => {
  const saved = {
    homeserverUrl: "https://matrix.example.test/",
    allowedUserIds: ["@adam:beeper.com"],
    roomId: "!room:matrix.example.test",
  } as const;

  it("returns the saved connection for the environment that owns it", () => {
    const withConfig = new Map([
      [
        environmentA,
        { statusView: { kind: "pending" } as const, savedConfig: saved, canOperate: true },
      ],
    ]);
    expect(selectMatrixBridgeSavedConfig(withConfig, environmentA)).toEqual(saved);
  });

  it("has nothing for an environment with no bridge, and nothing without one selected", () => {
    expect(selectMatrixBridgeSavedConfig(states, environmentB)).toBeNull();
    expect(selectMatrixBridgeSavedConfig(states, null)).toBeNull();
  });
});

describe("matrixBridgeSavedConfigView", () => {
  it("is nothing until the query answers, so the form stays as the operator left it", () => {
    expect(matrixBridgeSavedConfigView(AsyncResult.initial())).toBeNull();
    expect(
      matrixBridgeSavedConfigView(AsyncResult.failure(Cause.fail(new Error("no access:read")))),
    ).toBeNull();
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
