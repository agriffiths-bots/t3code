import { describe, expect, it } from "vite-plus/test";

import {
  buildPairingUrl,
  extractPairingUrlFromQrPayload,
  PairingQrPayloadEmptyError,
  parsePairingUrl,
} from "./pairing";

describe("buildPairingUrl", () => {
  it("uses HTTP for a schemeless IP address", () => {
    expect(buildPairingUrl("192.168.1.100:3773", "pairing-token")).toBe(
      "http://192.168.1.100:3773/#token=pairing-token",
    );
  });

  it("keeps HTTPS as the default for a schemeless hostname", () => {
    expect(buildPairingUrl("remote.example.com", "pairing-token")).toBe(
      "https://remote.example.com/#token=pairing-token",
    );
  });

  it("preserves an explicit scheme for an IP address", () => {
    expect(buildPairingUrl("https://192.168.1.100:3773", "pairing-token")).toBe(
      "https://192.168.1.100:3773/#token=pairing-token",
    );
  });
});

describe("extractPairingUrlFromQrPayload", () => {
  it("trims raw pairing urls from qr payloads", () => {
    expect(
      extractPairingUrlFromQrPayload("  https://remote.example.com/pair#token=pairing-token  "),
    ).toBe("https://remote.example.com/pair#token=pairing-token");
  });

  it("unwraps mobile deep links that carry an encoded pairing url", () => {
    expect(
      extractPairingUrlFromQrPayload(
        "t3code://pair?pairingUrl=https%3A%2F%2Fremote.example.com%2Fpair%23token%3Dpairing-token",
      ),
    ).toBe("https://remote.example.com/pair#token=pairing-token");
  });

  it("rejects empty qr payloads", () => {
    expect(() => extractPairingUrlFromQrPayload("   ")).toThrowError(PairingQrPayloadEmptyError);
    expect(() => extractPairingUrlFromQrPayload("   ")).toThrowError(
      "Scanned QR code did not contain a pairing URL.",
    );
  });
});

describe("parsePairingUrl", () => {
  it("reads hosted pairing links into backend host fields", () => {
    expect(
      parsePairingUrl(
        "https://app.t3.codes/pair?host=https%3A%2F%2Fdesktop.tailnet.ts.net%2F#token=pairing-token",
      ),
    ).toEqual({
      host: "https://desktop.tailnet.ts.net",
      code: "pairing-token",
      cloudflareAccessToken: "",
      cloudflareAccessClientId: "",
      cloudflareAccessClientSecret: "",
    });
  });

  it("keeps Cloudflare Access tokens separate from pairing codes", () => {
    expect(
      parsePairingUrl(
        "https://remote.example.com/pair#token=pairing-token&cf_access_token=cf-access-jwt",
      ),
    ).toEqual({
      host: "https://remote.example.com",
      code: "pairing-token",
      cloudflareAccessToken: "cf-access-jwt",
      cloudflareAccessClientId: "",
      cloudflareAccessClientSecret: "",
    });
  });

  it("keeps Cloudflare Access service-token credentials from pairing urls", () => {
    expect(
      parsePairingUrl(
        "https://remote.example.com/pair#token=pairing-token&cf_access_client_id=client-id&cf_access_client_secret=client-secret",
      ),
    ).toEqual({
      host: "https://remote.example.com",
      code: "pairing-token",
      cloudflareAccessToken: "",
      cloudflareAccessClientId: "client-id",
      cloudflareAccessClientSecret: "client-secret",
    });
  });
});

describe("buildPairingUrl", () => {
  it("adds Cloudflare Access tokens as hidden fragment state", () => {
    expect(buildPairingUrl("remote.example.com", "pairing-token", "cf-access-jwt")).toBe(
      "https://remote.example.com/#token=pairing-token&cf_access_token=cf-access-jwt",
    );
  });

  it("adds Cloudflare Access service-token fields as hidden fragment state", () => {
    expect(
      buildPairingUrl("remote.example.com", "pairing-token", {
        clientId: "client-id",
        clientSecret: "client-secret",
      }),
    ).toBe(
      "https://remote.example.com/#token=pairing-token&cf_access_client_id=client-id&cf_access_client_secret=client-secret",
    );
  });
});
