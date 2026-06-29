import { describe, expect, it, vi } from "vite-plus/test";

vi.mock("expo-crypto", () => ({
  getRandomBytes: (byteCount: number) => new Uint8Array(byteCount),
}));

vi.mock("expo-linking", () => ({
  createURL: (path: string) => `t3code-preview://${path}`,
}));

vi.mock("expo-web-browser", () => ({
  openAuthSessionAsync: vi.fn(),
}));

import { parseAccessAudienceFromLocation } from "./cloudflareAccess";

describe("parseAccessAudienceFromLocation", () => {
  it("reads the Cloudflare Access audience from the standard redirect location", () => {
    expect(
      parseAccessAudienceFromLocation(
        "https://example.cloudflareaccess.com/cdn-cgi/access/login/app.example.test?kid=app-aud&redirect_url=%2F",
        "https://app.example.test/",
      ),
    ).toBe("app-aud");
  });

  it("accepts aud as a fallback query parameter", () => {
    expect(
      parseAccessAudienceFromLocation(
        "/cdn-cgi/access/cli?aud=app-aud",
        "https://app.example.test/",
      ),
    ).toBe("app-aud");
  });

  it("returns null when no audience is present", () => {
    expect(
      parseAccessAudienceFromLocation(
        "https://example.cloudflareaccess.com/cdn-cgi/access/login/app.example.test",
        "https://app.example.test/",
      ),
    ).toBeNull();
  });
});
