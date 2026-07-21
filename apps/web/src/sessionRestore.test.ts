import { describe, expect, it } from "vite-plus/test";

import { resolveSessionRestore } from "./sessionRestore";

const connectedInput = {
  catalogReady: true,
  environmentPresent: true,
  connectionPhase: "connected" as const,
  shellBootstrapped: true,
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
        shellBootstrapped: false,
      }),
    ).toEqual({ kind: "connecting" });
    expect(
      resolveSessionRestore({
        ...connectedInput,
        shellBootstrapped: false,
      }),
    ).toEqual({ kind: "restoring" });
  });

  it("turns both indefinite wait states into bounded actionable errors", () => {
    expect(
      resolveSessionRestore({
        ...connectedInput,
        connectionPhase: "reconnecting",
        shellBootstrapped: false,
        timedOut: true,
      }),
    ).toEqual({ kind: "connection-error" });
    expect(
      resolveSessionRestore({
        ...connectedInput,
        shellBootstrapped: false,
        timedOut: true,
      }),
    ).toEqual({ kind: "restore-error" });
  });
});
