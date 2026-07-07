// Regression guard for the post-pair blocked-connection bug: the primary
// environment's supervisor can park in the "blocked" phase before the browser
// session exists (its first connect attempt fails authentication, and
// blocked-phase supervisors wait for an explicit signal instead of retrying).
// A successful pairing submit must therefore kick the supervisor via
// environmentCatalog.retryNow before the app navigates off /pair. Without the
// kick the freshly paired app renders an empty shell (no projects, no
// websocket) until the user manually reloads the page.
import { describe, expect, it } from "vite-plus/test";
import type { EnvironmentId } from "@t3tools/contracts";

import {
  errorMessageFromUnknown,
  submitPairingCredentialAndUnblock,
} from "./PairingRouteSurface.logic";

const PRIMARY_ID = "env-primary" as EnvironmentId;

function makeDeps(overrides?: {
  primaryEnvironmentId?: EnvironmentId | null;
  submitError?: unknown;
}) {
  const calls: { submitted: string[]; retried: EnvironmentId[] } = { submitted: [], retried: [] };
  const deps = {
    submitServerAuthCredential: (credential: string) => {
      calls.submitted.push(credential);
      return overrides?.submitError !== undefined
        ? Promise.reject(overrides.submitError)
        : Promise.resolve();
    },
    retryPrimaryEnvironment: (environmentId: EnvironmentId) => {
      calls.retried.push(environmentId);
      return Promise.resolve();
    },
    getPrimaryEnvironmentId: () =>
      overrides && "primaryEnvironmentId" in overrides
        ? (overrides.primaryEnvironmentId ?? null)
        : PRIMARY_ID,
    errorMessageFromUnknown,
  };
  return { deps, calls };
}

describe("submitPairingCredentialAndUnblock", () => {
  it("kicks the parked primary environment supervisor after a successful submit", async () => {
    const { deps, calls } = makeDeps();

    const error = await submitPairingCredentialAndUnblock(deps, "PAIRME12345");

    expect(error).toBeNull();
    expect(calls.submitted).toEqual(["PAIRME12345"]);
    expect(calls.retried).toEqual([PRIMARY_ID]);
  });

  it("succeeds without a retry when the primary environment is not registered yet", async () => {
    const { deps, calls } = makeDeps({ primaryEnvironmentId: null });

    const error = await submitPairingCredentialAndUnblock(deps, "PAIRME12345");

    expect(error).toBeNull();
    expect(calls.retried).toEqual([]);
  });

  it("re-reads the primary id after the exchange, retrying one registered mid-flight", async () => {
    // Primary is null when the submit starts, then the platform poll registers
    // it while submitServerAuthCredential is in flight. The getter must observe
    // the late value and still kick the parked supervisor.
    const calls = { submitted: [] as string[], retried: [] as EnvironmentId[] };
    let currentPrimaryId: EnvironmentId | null = null;
    const error = await submitPairingCredentialAndUnblock(
      {
        submitServerAuthCredential: (credential) => {
          calls.submitted.push(credential);
          currentPrimaryId = PRIMARY_ID; // registered mid-exchange
          return Promise.resolve();
        },
        retryPrimaryEnvironment: (environmentId) => {
          calls.retried.push(environmentId);
          return Promise.resolve();
        },
        getPrimaryEnvironmentId: () => currentPrimaryId,
        errorMessageFromUnknown,
      },
      "PAIRME12345",
    );

    expect(error).toBeNull();
    expect(calls.retried).toEqual([PRIMARY_ID]);
  });

  it("returns the credential error and does not retry when the submit is rejected", async () => {
    const { deps, calls } = makeDeps({ submitError: new Error("Invalid pairing token.") });

    const error = await submitPairingCredentialAndUnblock(deps, "BADCODE");

    expect(error).toBe("Invalid pairing token.");
    expect(calls.retried).toEqual([]);
  });

  it("falls back to a generic message for non-Error rejections", async () => {
    const { deps } = makeDeps({ submitError: { odd: true } });

    const error = await submitPairingCredentialAndUnblock(deps, "BADCODE");

    expect(error).toBe("Authentication failed.");
  });
});
