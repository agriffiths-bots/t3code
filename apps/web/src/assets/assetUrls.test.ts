import { describe, expect, it } from "vite-plus/test";

import {
  resolveAssetUrl,
  resolveBoundAssetUrl,
  resolveBrowserAssetSurfaceBaseUrl,
} from "./assetUrls";

describe("resolveAssetUrl", () => {
  it("resolves an environment-relative asset URL", () => {
    expect(
      resolveAssetUrl("https://environment.example/base/", "/api/assets/signed-token/favicon.png"),
    ).toBe("https://environment.example/api/assets/signed-token/favicon.png");
  });

  it("rejects an invalid environment base URL", () => {
    expect(resolveAssetUrl("not a URL", "/api/assets/signed-token/favicon.png")).toBeNull();
  });

  it("keeps credential-less assets on the environment origin", async () => {
    await expect(
      resolveBoundAssetUrl("https://environment.example/", "t3code://app/", {
        relativeUrl: "/api/assets/signed-token/report.html",
        expiresAt: Date.now() + 60_000,
        surfaceCredential: null,
      }),
    ).resolves.toBe("https://environment.example/api/assets/signed-token/report.html");
  });

  it("uses the app origin for surface-bound relay URLs", () => {
    expect(
      resolveBrowserAssetSurfaceBaseUrl("https://private-app.example/settings?tab=connections"),
    ).toBe("https://private-app.example/");
    expect(resolveBrowserAssetSurfaceBaseUrl("t3code://app/settings")).toBe("t3code://app/");
  });

  it("rejects non-origin browser locations", () => {
    expect(resolveBrowserAssetSurfaceBaseUrl("not a URL")).toBeNull();
    expect(resolveBrowserAssetSurfaceBaseUrl("file:///tmp/index.html")).toBeNull();
  });
});
