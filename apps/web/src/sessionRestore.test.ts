import { describe, expect, it } from "vite-plus/test";

import {
  resolveSessionDetailStatus,
  resolveSessionRestore,
  sessionRestoreWaitingStage,
  shouldSubscribeToServerThread,
} from "./sessionRestore";

const connectedInput = {
  catalogReady: true,
  environmentPresent: true,
  connectionPhase: "connected" as const,
  shellAuthoritative: true,
  shellHasThread: true,
  draftExists: false,
  timedOut: false,
};

describe("session restore launch smoke", () => {
  it("clears a missing persisted thread without mounting its detail subscription", () => {
    const result = resolveSessionRestore({
      ...connectedInput,
      shellHasThread: false,
    });

    expect(result).toEqual({ kind: "stale" });
  });

  it("does not reuse a persisted Windows route for a WSL-only catalog", () => {
    const result = resolveSessionRestore({
      ...connectedInput,
      environmentPresent: false,
    });

    expect(result).toEqual({ kind: "stale" });
  });

  it("distinguishes backend connection from session restoration", () => {
    expect(
      resolveSessionRestore({
        ...connectedInput,
        connectionPhase: "connecting",
        shellAuthoritative: false,
      }),
    ).toEqual({ kind: "connecting" });
    expect(
      resolveSessionRestore({
        ...connectedInput,
        shellAuthoritative: false,
      }),
    ).toEqual({ kind: "restoring" });
  });

  it("turns both indefinite wait states into bounded actionable errors", () => {
    expect(
      resolveSessionRestore({
        ...connectedInput,
        connectionPhase: "reconnecting",
        shellAuthoritative: false,
        timedOut: true,
      }),
    ).toEqual({ kind: "connection-error" });
    expect(
      resolveSessionRestore({
        ...connectedInput,
        shellAuthoritative: false,
        timedOut: true,
      }),
    ).toEqual({ kind: "restore-error" });
  });

  it("waits for a live shell before rejecting a thread absent from cache", () => {
    expect(
      resolveSessionRestore({
        ...connectedInput,
        shellAuthoritative: false,
        shellHasThread: false,
      }),
    ).toEqual({ kind: "restoring" });
    expect(
      resolveSessionRestore({
        ...connectedInput,
        shellAuthoritative: true,
        shellHasThread: false,
      }),
    ).toEqual({ kind: "stale" });
  });

  it("keeps a known restored thread mounted while the live shell resynchronizes", () => {
    expect(
      resolveSessionRestore({
        ...connectedInput,
        shellAuthoritative: false,
        detailStatus: "ready",
        timedOut: true,
      }),
    ).toEqual({ kind: "ready" });
  });

  it("arms a timeout only for a currently waiting resolution", () => {
    expect(sessionRestoreWaitingStage({ kind: "connecting" })).toBe("connecting");
    expect(sessionRestoreWaitingStage({ kind: "restoring" })).toBe("restoring");
    expect(sessionRestoreWaitingStage({ kind: "ready" })).toBeNull();
  });

  it("keeps cached detail usable after a transient stream error", () => {
    expect(resolveSessionDetailStatus({ deleted: false, hasDetail: true, hasError: true })).toBe(
      "ready",
    );
    expect(resolveSessionDetailStatus({ deleted: true, hasDetail: true, hasError: true })).toBe(
      "deleted",
    );
  });

  it("defers draft detail subscriptions until the server thread is discoverable", () => {
    expect(
      shouldSubscribeToServerThread({
        draftExists: true,
        draftPromoted: false,
        shellPresent: false,
      }),
    ).toBe(false);
    expect(
      shouldSubscribeToServerThread({
        draftExists: true,
        draftPromoted: false,
        shellPresent: true,
      }),
    ).toBe(true);
    expect(
      shouldSubscribeToServerThread({
        draftExists: true,
        draftPromoted: true,
        shellPresent: false,
      }),
    ).toBe(true);
  });
});
