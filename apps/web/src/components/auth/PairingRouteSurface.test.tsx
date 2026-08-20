// Regression guard for the post-pair blocked-connection bug: the primary
// environment's supervisor can park in the "blocked" phase before the browser
// session exists (its first connect attempt fails authentication, and
// blocked-phase supervisors wait for an explicit signal instead of retrying).
// A successful pairing submit must therefore kick the supervisor via
// environmentCatalog.retryNow before the app navigates off /pair. Without the
// kick the freshly paired app renders an empty shell (no projects, no
// websocket) until the user manually reloads the page.
import { AuthAdministrativeScopes, AuthStandardClientScopes } from "@t3tools/contracts";
import type { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  describeAuthenticatedPairingApply,
  describeAuthenticatedPairingFailure,
  errorMessageFromUnknown,
  incomingGrantFromPairingLinks,
  incomingDropsCurrentScopes,
  isNarrowerAudienceCeiling,
  pairingApplyFailureKindFromUnknown,
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

    expect(error).toEqual({
      message: "Invalid pairing token.",
      kind: "generic",
    });
    expect(calls.retried).toEqual([]);
  });

  it("falls back to a generic message for non-Error rejections", async () => {
    const { deps } = makeDeps({ submitError: { odd: true } });

    const error = await submitPairingCredentialAndUnblock(deps, "BADCODE");

    expect(error).toEqual({
      message: "Authentication failed.",
      kind: "generic",
    });
  });

  it("classifies tagged pairing failures so the apply surface can match copy to the outcome", async () => {
    const { deps } = makeDeps({
      submitError: Object.assign(new Error("used"), {
        _tag: "PrimaryEnvironmentPairingCredentialConsumedError",
      }),
    });

    const error = await submitPairingCredentialAndUnblock(deps, "USEDCODE");

    expect(error).toEqual({
      message: "used",
      kind: "consumed",
    });
  });
});

describe("incomingDropsCurrentScopes", () => {
  it("is true when incoming omits any current scope, even if it also adds scopes", () => {
    expect(
      incomingDropsCurrentScopes([...AuthStandardClientScopes], [...AuthAdministrativeScopes]),
    ).toBe(true);
    expect(incomingDropsCurrentScopes(["relay:write"], ["access:write"])).toBe(true);
  });

  it("is false when incoming keeps every current scope", () => {
    expect(
      incomingDropsCurrentScopes([...AuthStandardClientScopes], [...AuthStandardClientScopes]),
    ).toBe(false);
    expect(
      incomingDropsCurrentScopes([...AuthAdministrativeScopes], [...AuthStandardClientScopes]),
    ).toBe(false);
  });
});

describe("incomingGrantFromPairingLinks", () => {
  it("returns the matching grant without consuming it", () => {
    expect(
      incomingGrantFromPairingLinks("PAIRME12345", [
        {
          credential: "OTHER",
          scopes: [...AuthAdministrativeScopes],
          audienceCeiling: "private",
        },
        {
          credential: "PAIRME12345",
          scopes: [...AuthStandardClientScopes],
          audienceCeiling: "factory",
        },
      ]),
    ).toEqual({
      scopes: [...AuthStandardClientScopes],
      audienceCeiling: "factory",
    });
  });

  it("returns null when the credential is not a listed pairing link", () => {
    expect(incomingGrantFromPairingLinks("PAIRME12345", [])).toBeNull();
  });
});

describe("isNarrowerAudienceCeiling", () => {
  it("treats factory as narrower than private", () => {
    expect(isNarrowerAudienceCeiling("factory", "private")).toBe(true);
    expect(isNarrowerAudienceCeiling("private", "private")).toBe(false);
    expect(isNarrowerAudienceCeiling("factory", "factory")).toBe(false);
    expect(isNarrowerAudienceCeiling("private", "factory")).toBe(false);
  });
});

describe("describeAuthenticatedPairingApply", () => {
  it("warns plainly when the incoming grant is a strict subset of the current session", () => {
    const copy = describeAuthenticatedPairingApply({
      current: { scopes: [...AuthAdministrativeScopes], audienceCeiling: "private" },
      incoming: { scopes: [...AuthStandardClientScopes], audienceCeiling: "private" },
    });

    expect(copy.title).toBe("Apply this pairing link?");
    expect(copy.downgradeWarning).toMatch(/less access/i);
    expect(copy.downgradeWarning).toMatch(/startup pairing URL/i);
  });

  it("warns when an incoming grant adds scopes but still drops existing ones", () => {
    const copy = describeAuthenticatedPairingApply({
      current: { scopes: ["access:read", "access:write"], audienceCeiling: "private" },
      incoming: { scopes: ["access:read", "relay:write"], audienceCeiling: "private" },
    });

    expect(copy.downgradeWarning).toMatch(/less access/i);
  });

  it("warns when the incoming grant narrows data access even if scopes match", () => {
    const copy = describeAuthenticatedPairingApply({
      current: { scopes: [...AuthStandardClientScopes], audienceCeiling: "private" },
      incoming: { scopes: [...AuthStandardClientScopes], audienceCeiling: "factory" },
    });

    expect(copy.downgradeWarning).toMatch(/less access/i);
  });

  it("warns any authenticated session when the incoming grant cannot be inspected", () => {
    const copy = describeAuthenticatedPairingApply({
      current: { scopes: [...AuthStandardClientScopes], audienceCeiling: "private" },
      incoming: null,
    });

    expect(copy.downgradeWarning).toMatch(/could not confirm/i);
    expect(copy.downgradeWarning).toMatch(/startup pairing URL/i);
  });

  it("warns when the current grant cannot be read, including omitted scopes", () => {
    const copy = describeAuthenticatedPairingApply({
      current: null,
      incoming: { scopes: [...AuthStandardClientScopes], audienceCeiling: "factory" },
    });

    expect(copy.downgradeWarning).toMatch(/could not confirm/i);
  });

  it("does not warn when both grants are known and the incoming grant is not a downgrade", () => {
    expect(
      describeAuthenticatedPairingApply({
        current: { scopes: [...AuthStandardClientScopes], audienceCeiling: "private" },
        incoming: { scopes: [...AuthAdministrativeScopes], audienceCeiling: "private" },
      }).downgradeWarning,
    ).toBeNull();
    expect(
      describeAuthenticatedPairingApply({
        current: { scopes: [...AuthStandardClientScopes], audienceCeiling: "factory" },
        incoming: { scopes: [...AuthStandardClientScopes], audienceCeiling: "factory" },
      }).downgradeWarning,
    ).toBeNull();
  });
});

describe("pairingApplyFailureKindFromUnknown", () => {
  it("maps tagged primary pairing errors", () => {
    expect(
      pairingApplyFailureKindFromUnknown({
        _tag: "PrimaryEnvironmentSessionReplacementError",
      }),
    ).toBe("replacement-failed");
    expect(
      pairingApplyFailureKindFromUnknown({
        _tag: "PrimaryEnvironmentSessionReplacementRevertedError",
      }),
    ).toBe("consumed");
    expect(
      pairingApplyFailureKindFromUnknown({
        _tag: "PrimaryEnvironmentPairingCredentialConsumedError",
      }),
    ).toBe("consumed");
    expect(
      pairingApplyFailureKindFromUnknown({
        _tag: "PrimaryEnvironmentPairingCredentialRejectedError",
      }),
    ).toBe("rejected");
    expect(pairingApplyFailureKindFromUnknown(new Error("nope"))).toBe("generic");
  });
});

describe("describeAuthenticatedPairingFailure", () => {
  it("keeps retry and current-session copy when replacement failed and the browser is still signed in", () => {
    const copy = describeAuthenticatedPairingFailure({
      kind: "replacement-failed",
      stillAuthenticated: true,
    });

    expect(copy.title).toBe("Pairing link was not applied");
    expect(copy.explanation).toMatch(/kept its current session/i);
    expect(copy.explanation).toMatch(/retry with this link/i);
    expect(copy.retryLabel).toBe("Retry");
    expect(copy.continueLabel).toBe("Continue with current session");
  });

  it("does not offer retry when the link was already used", () => {
    const signedIn = describeAuthenticatedPairingFailure({
      kind: "consumed",
      stillAuthenticated: true,
    });
    const signedOut = describeAuthenticatedPairingFailure({
      kind: "consumed",
      stillAuthenticated: false,
    });

    expect(signedIn.explanation).toMatch(/already used/i);
    expect(signedIn.explanation).toMatch(/kept its current session/i);
    expect(signedIn.retryLabel).toBeNull();
    expect(signedOut.explanation).toMatch(/already used/i);
    expect(signedOut.explanation).toMatch(/no longer signed in/i);
    expect(signedOut.retryLabel).toBeNull();
    expect(signedOut.continueLabel).toBe("Continue");
  });

  it("tells the truth when displacement ended the current session", () => {
    const copy = describeAuthenticatedPairingFailure({
      kind: "generic",
      stillAuthenticated: false,
    });

    expect(copy.explanation).toMatch(/no longer signed in/i);
    expect(copy.explanation).toMatch(/retry with this link/i);
    expect(copy.retryLabel).toBe("Retry");
    expect(copy.continueLabel).toBe("Continue");
    expect(copy.explanation).not.toMatch(/kept its current session/i);
  });
});
