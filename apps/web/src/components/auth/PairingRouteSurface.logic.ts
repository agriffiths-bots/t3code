import type { EnvironmentId } from "@t3tools/contracts";

export interface PairingCredentialSubmitDependencies {
  readonly submitServerAuthCredential: (credential: string) => Promise<void>;
  readonly retryPrimaryEnvironment: (environmentId: EnvironmentId) => Promise<unknown>;
  readonly primaryEnvironmentId: EnvironmentId | null;
  readonly errorMessageFromUnknown: (error: unknown) => string;
}

// Submit a pairing credential and, on success, kick the primary environment's
// connection supervisor. The supervisor may already be parked in the "blocked"
// phase: its first connection attempt ran before this session cookie existed,
// failed authentication, and blocked-phase supervisors wait for an explicit
// signal instead of retrying. Without the kick the freshly paired app stays on
// an empty shell (no projects, no websocket) until a manual page reload.
// Returns the user-facing error message, or null when authentication succeeded.
export async function submitPairingCredentialAndUnblock(
  deps: PairingCredentialSubmitDependencies,
  credential: string,
): Promise<string | null> {
  try {
    await deps.submitServerAuthCredential(credential);
  } catch (error) {
    return deps.errorMessageFromUnknown(error);
  }

  if (deps.primaryEnvironmentId !== null) {
    await deps.retryPrimaryEnvironment(deps.primaryEnvironmentId);
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
