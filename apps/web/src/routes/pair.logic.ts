export type PairRouteAuthStatus =
  | "hosted-pairing"
  | "hosted-static"
  | "authenticated"
  | "requires-auth"
  | "pending";

export type PairRouteDisposition =
  | "hosted-pairing"
  | "apply-pairing-credential"
  | "pairing-form"
  | "redirect-home";

/**
 * Decide what `/pair` should do for the current auth gate and URL credential.
 *
 * An already-signed-in browser that just opened a same-origin pairing link
 * reaches the apply surface. That surface requires an explicit click before
 * the grant replaces the current session. Never discard the link silently.
 */
export function pairRouteDisposition(input: {
  readonly authStatus: PairRouteAuthStatus;
  readonly pairingToken: string | null;
}): PairRouteDisposition {
  if (input.authStatus === "hosted-pairing") {
    return "hosted-pairing";
  }

  if (input.authStatus === "hosted-static") {
    return "redirect-home";
  }

  if (input.authStatus === "pending") {
    return "pairing-form";
  }

  const pairingToken = input.pairingToken?.trim() ?? "";
  if (input.authStatus === "authenticated") {
    return pairingToken.length > 0 ? "apply-pairing-credential" : "redirect-home";
  }

  return "pairing-form";
}
