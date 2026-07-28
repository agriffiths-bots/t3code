import type { TurnId } from "@t3tools/contracts";

export const PROVIDER_EMPTY_RESPONSE_ERROR =
  "Provider completed the turn without emitting an assistant response.";

export const PROVIDER_SESSION_FAILED_DURING_TURN_ERROR =
  "Provider session failed while the turn was running.";

export const PROVIDER_SESSION_CLOSED_DURING_TURN_ERROR =
  "Provider session closed while the turn was running.";

export const providerSessionDisappearedDuringTurnError = (turnId: TurnId): string =>
  `Provider session disappeared while turn '${turnId}' was running.`;
