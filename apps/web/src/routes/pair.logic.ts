export type PairRouteAuthStatus =
  | "hosted-pairing"
  | "hosted-static"
  | "authenticated"
  | "requires-auth"
  | "pending";

export type PairRouteDisposition =
  | "hosted-pairing"
  | "apply-pairing-credential"
  | "desktop-local-session"
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
  readonly isDesktop?: boolean;
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
    if (pairingToken.length === 0) {
      return "redirect-home";
    }
    // Desktop's primary connection is the main-process bearer, not the cookie
    // pairing replace would install. Applying here would consume the link and
    // report success while the administrative session stayed in place.
    return input.isDesktop === true ? "desktop-local-session" : "apply-pairing-credential";
  }

  return "pairing-form";
}
