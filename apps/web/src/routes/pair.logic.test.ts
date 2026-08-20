import { describe, expect, it } from "vite-plus/test";

import { pairRouteDisposition } from "./pair.logic";

describe("pairRouteDisposition", () => {
  it("keeps hosted pairing on the hosted surface", () => {
    expect(
      pairRouteDisposition({
        authStatus: "hosted-pairing",
        pairingToken: "PAIRME12345",
      }),
    ).toBe("hosted-pairing");
  });

  it("redeems a pairing credential presented to an already-signed-in browser", () => {
    expect(
      pairRouteDisposition({
        authStatus: "authenticated",
        pairingToken: "PAIRME12345",
      }),
    ).toBe("apply-pairing-credential");
  });

  it("does not apply a pairing link against the desktop app's local session", () => {
    expect(
      pairRouteDisposition({
        authStatus: "authenticated",
        pairingToken: "PAIRME12345",
        isDesktop: true,
      }),
    ).toBe("desktop-local-session");
  });

  it("treats whitespace-only pairing credentials as absent", () => {
    expect(
      pairRouteDisposition({
        authStatus: "authenticated",
        pairingToken: "   ",
      }),
    ).toBe("redirect-home");
  });

  it("redirects a signed-in browser that opened /pair with no credential", () => {
    expect(
      pairRouteDisposition({
        authStatus: "authenticated",
        pairingToken: null,
      }),
    ).toBe("redirect-home");
  });

  it("does not redeem a local pairing token on the hosted static app", () => {
    expect(
      pairRouteDisposition({
        authStatus: "hosted-static",
        pairingToken: "PAIRME12345",
      }),
    ).toBe("redirect-home");
  });

  it("keeps the pairing route up while auth is still pending", () => {
    expect(
      pairRouteDisposition({
        authStatus: "pending",
        pairingToken: "PAIRME12345",
      }),
    ).toBe("pairing-form");
  });

  it("shows the pairing form when the browser still needs a session", () => {
    expect(
      pairRouteDisposition({
        authStatus: "requires-auth",
        pairingToken: "PAIRME12345",
      }),
    ).toBe("pairing-form");
    expect(
      pairRouteDisposition({
        authStatus: "requires-auth",
        pairingToken: null,
      }),
    ).toBe("pairing-form");
  });
});
