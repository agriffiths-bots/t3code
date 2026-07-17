import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";

import {
  AuthAccessTokenResult,
  AuthBrowserSessionResult,
  AuthClientSession,
  AuthPairingCredentialResult,
  AuthPairingLink,
} from "./auth.ts";

const decodeAccessToken = Schema.decodeUnknownSync(AuthAccessTokenResult);
const decodeBrowserSession = Schema.decodeUnknownSync(AuthBrowserSessionResult);
const decodeClientSession = Schema.decodeUnknownSync(AuthClientSession);
const decodePairingCredential = Schema.decodeUnknownSync(AuthPairingCredentialResult);
const decodePairingLink = Schema.decodeUnknownSync(AuthPairingLink);

describe("legacy auth response compatibility", () => {
  it("defaults an absent response audience ceiling to private", () => {
    const issuedAt = DateTime.makeUnsafe("2026-07-17T11:00:00.000Z");
    const createdAt = DateTime.makeUnsafe("2026-07-17T11:55:00.000Z");
    const expiresAt = DateTime.makeUnsafe("2026-07-17T12:00:00.000Z");
    const accessToken = decodeAccessToken({
      access_token: "legacy-access-token",
      issued_token_type: "urn:ietf:params:oauth:token-type:access_token",
      token_type: "Bearer",
      expires_in: 3600,
      scope: "orchestration:read",
    });
    const browserSession = decodeBrowserSession({
      authenticated: true,
      scopes: ["orchestration:read"],
      sessionMethod: "browser-session-cookie",
      expiresAt,
    });
    const pairingCredential = decodePairingCredential({
      id: "legacy-pairing-credential",
      credential: "LEGACYPAIRING",
      expiresAt,
    });
    const pairingLink = decodePairingLink({
      id: "legacy-pairing-link",
      credential: "LEGACYPAIRING",
      scopes: ["orchestration:read"],
      subject: "legacy-client",
      createdAt,
      expiresAt,
    });
    const clientSession = decodeClientSession({
      sessionId: "legacy-session",
      subject: "legacy-client",
      scopes: ["orchestration:read"],
      method: "bearer-access-token",
      client: { deviceType: "unknown" },
      issuedAt,
      expiresAt,
      lastConnectedAt: null,
      connected: false,
      current: false,
    });

    expect(accessToken.audienceCeiling).toBe("private");
    expect(browserSession.audienceCeiling).toBe("private");
    expect(pairingCredential.audienceCeiling).toBe("private");
    expect(pairingLink.audienceCeiling).toBe("private");
    expect(clientSession.audienceCeiling).toBe("private");
  });
});
