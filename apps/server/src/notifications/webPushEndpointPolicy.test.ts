import { describe, expect, it } from "@effect/vitest";

import {
  getWebPushEndpointValidationError,
  isPublicWebPushIpAddress,
} from "./webPushEndpointPolicy.ts";

describe("getWebPushEndpointValidationError", () => {
  it("accepts public HTTPS push-service endpoints", () => {
    expect(
      getWebPushEndpointValidationError("https://updates.push.services.mozilla.com/wpush/v2/test"),
    ).toBeNull();
    expect(
      getWebPushEndpointValidationError("https://fcm.googleapis.com/fcm/send/test"),
    ).toBeNull();
    expect(
      getWebPushEndpointValidationError("https://wns2-ln2p.notify.windows.com/w/?token=test"),
    ).toBeNull();
  });

  it("rejects non-HTTPS and non-public endpoints", () => {
    const blockedEndpoints = [
      "http://updates.push.services.mozilla.com/wpush/v2/test",
      "https://user:pass@updates.push.services.mozilla.com/wpush/v2/test",
      "https://localhost/wpush/v2/test",
      "https://example.internal/wpush/v2/test",
      "https://127.0.0.1/wpush/v2/test",
      "https://10.0.0.1/wpush/v2/test",
      "https://172.16.0.1/wpush/v2/test",
      "https://192.168.0.1/wpush/v2/test",
      "https://169.254.169.254/latest/meta-data",
      "https://[::1]/wpush/v2/test",
      "https://[fc00::1]/wpush/v2/test",
      "https://[fe80::1]/wpush/v2/test",
      "not-a-url",
    ];

    for (const endpoint of blockedEndpoints) {
      expect(getWebPushEndpointValidationError(endpoint), endpoint).not.toBeNull();
    }
  });
});

describe("isPublicWebPushIpAddress", () => {
  it("treats routable addresses as public", () => {
    expect(isPublicWebPushIpAddress("8.8.8.8")).toBe(true);
    expect(isPublicWebPushIpAddress("2606:4700:4700::1111")).toBe(true);
  });

  it("treats loopback, private, link-local and unique-local addresses as non-public", () => {
    const nonPublic = [
      "127.0.0.1",
      "10.0.0.1",
      "172.16.0.1",
      "192.168.0.1",
      "169.254.169.254",
      "::1",
      "fc00::1",
      "fe80::1",
      "::ffff:127.0.0.1",
    ];

    for (const address of nonPublic) {
      expect(isPublicWebPushIpAddress(address), address).toBe(false);
    }
  });
});
