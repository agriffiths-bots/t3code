import type { AuthAudienceCeiling, AuthEnvironmentScope, EnvironmentId } from "@t3tools/contracts";

export interface PairingCredentialSubmitDependencies {
  readonly submitServerAuthCredential: (credential: string) => Promise<void>;
  readonly retryPrimaryEnvironment: (environmentId: EnvironmentId) => Promise<unknown>;
  // Read as a getter, not a captured value: a pairing submit can begin before
  // the platform poll has registered the primary environment, so the id must be
  // re-read AFTER the credential exchange to catch a primary that parked
  // (blocked) while the exchange was in flight.
  readonly getPrimaryEnvironmentId: () => EnvironmentId | null;
  readonly errorMessageFromUnknown: (error: unknown) => string;
}

export type PairingApplyFailureKind = "replacement-failed" | "consumed" | "rejected" | "generic";

export type PairingSubmitFailure = {
  readonly message: string;
  readonly kind: PairingApplyFailureKind;
};

export function pairingApplyFailureKindFromUnknown(error: unknown): PairingApplyFailureKind {
  if (typeof error !== "object" || error === null || !("_tag" in error)) {
    return "generic";
  }

  switch (error._tag) {
    case "PrimaryEnvironmentSessionReplacementError":
      return "replacement-failed";
    case "PrimaryEnvironmentSessionReplacementRevertedError":
    case "PrimaryEnvironmentPairingCredentialConsumedError":
      return "consumed";
    case "PrimaryEnvironmentPairingCredentialRejectedError":
      return "rejected";
    default:
      return "generic";
  }
}

// Submit a pairing credential and, on success, kick the primary environment's
// connection supervisor. The supervisor may already be parked in the "blocked"
// phase: its first connection attempt ran before this session cookie existed,
// failed authentication, and blocked-phase supervisors wait for an explicit
// signal instead of retrying. Without the kick the freshly paired app stays on
// an empty shell (no projects, no websocket) until a manual page reload.
// Returns the user-facing failure, or null when authentication succeeded.
export async function submitPairingCredentialAndUnblock(
  deps: PairingCredentialSubmitDependencies,
  credential: string,
): Promise<PairingSubmitFailure | null> {
  try {
    await deps.submitServerAuthCredential(credential);
  } catch (error) {
    return {
      message: deps.errorMessageFromUnknown(error),
      kind: pairingApplyFailureKindFromUnknown(error),
    };
  }

  // Re-read after the exchange: the poll may have registered (and parked) the
  // primary supervisor while submitServerAuthCredential was in flight.
  const primaryEnvironmentId = deps.getPrimaryEnvironmentId();
  if (primaryEnvironmentId !== null) {
    await deps.retryPrimaryEnvironment(primaryEnvironmentId);
  }

  return null;
}

export function errorMessageFromUnknown(error: unknown): string {
  if (error instanceof Error && error.message.trim().length > 0) {
    return error.message;
  }

  if (typeof error === "string" && error.trim().length > 0) {
    return error;
  }

  return "Authentication failed.";
}

export function incomingDropsCurrentScopes(
  incoming: ReadonlyArray<string>,
  current: ReadonlyArray<string>,
): boolean {
  const incomingSet = new Set(incoming);
  return current.some((scope) => !incomingSet.has(scope));
}

export type PairingGrantView = {
  readonly scopes: ReadonlyArray<AuthEnvironmentScope> | ReadonlyArray<string>;
  readonly audienceCeiling: AuthAudienceCeiling;
};

export function incomingGrantFromPairingLinks(
  credential: string,
  links: ReadonlyArray<{
    readonly credential: string;
    readonly scopes: ReadonlyArray<AuthEnvironmentScope>;
    readonly audienceCeiling: AuthAudienceCeiling;
  }>,
): PairingGrantView | null {
  const match = links.find((link) => link.credential === credential);
  return match === undefined
    ? null
    : { scopes: match.scopes, audienceCeiling: match.audienceCeiling };
}

export function isNarrowerAudienceCeiling(
  incoming: AuthAudienceCeiling,
  current: AuthAudienceCeiling,
): boolean {
  return current === "private" && incoming === "factory";
}

const AUTHENTICATED_PAIRING_APPLY_EXPLANATION =
  "This one-time link replaces the session on this browser with the permissions it grants.";

const KNOWN_DOWNGRADE_WARNING =
  "This link grants less access than this browser has now. Applying it replaces this session with the weaker grant. If you lose administrative access, on a headless server it only comes back from the startup pairing URL after a restart.";

const UNKNOWN_GRANT_WARNING =
  "This browser could not confirm what this link grants before applying. If the link is weaker than the current session, those permissions will be lost. If you lose administrative access, on a headless server it only comes back from the startup pairing URL after a restart.";

export function describeAuthenticatedPairingApply(input: {
  readonly current: PairingGrantView | null;
  readonly incoming: PairingGrantView | null;
}): {
  readonly title: string;
  readonly explanation: string;
  readonly downgradeWarning: string | null;
} {
  if (input.current !== null && input.incoming !== null) {
    const weaker =
      incomingDropsCurrentScopes(input.incoming.scopes, input.current.scopes) ||
      isNarrowerAudienceCeiling(input.incoming.audienceCeiling, input.current.audienceCeiling);
    return {
      title: "Apply this pairing link?",
      explanation: AUTHENTICATED_PAIRING_APPLY_EXPLANATION,
      downgradeWarning: weaker ? KNOWN_DOWNGRADE_WARNING : null,
    };
  }

  return {
    title: "Apply this pairing link?",
    explanation: AUTHENTICATED_PAIRING_APPLY_EXPLANATION,
    downgradeWarning: UNKNOWN_GRANT_WARNING,
  };
}

export function describeAuthenticatedPairingFailure(input: {
  readonly kind: PairingApplyFailureKind;
  readonly stillAuthenticated: boolean;
}): {
  readonly title: string;
  readonly explanation: string;
  readonly retryLabel: string | null;
  readonly continueLabel: string;
} {
  const title = "Pairing link was not applied";

  if (input.kind === "consumed") {
    return {
      title,
      explanation: input.stillAuthenticated
        ? "This link was already used. This browser kept its current session. Get a new pairing link if you still need to replace it."
        : "This link was already used, and this browser is no longer signed in. Get a new pairing link to sign in again.",
      retryLabel: null,
      continueLabel: input.stillAuthenticated ? "Continue with current session" : "Continue",
    };
  }

  if (input.stillAuthenticated) {
    return {
      title,
      explanation:
        input.kind === "rejected"
          ? "This pairing token was rejected. This browser kept its current session. Check the link and try again, or continue as you are."
          : "This browser kept its current session. You can retry with this link or continue as you are.",
      retryLabel: "Retry",
      continueLabel: "Continue with current session",
    };
  }

  return {
    title,
    explanation:
      "This browser is no longer signed in. You can retry with this link to sign in again.",
    retryLabel: "Retry",
    continueLabel: "Continue",
  };
}
