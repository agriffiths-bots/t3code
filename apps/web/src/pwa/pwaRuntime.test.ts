import { describe, expect, it } from "vite-plus/test";

import { resolveDeepLinkTarget } from "./pwaRuntime";

describe("resolveDeepLinkTarget", () => {
  it("keeps notification deep links as pathnames for browser history", () => {
    expect(resolveDeepLinkTarget("/env-1/thread-1", "browser")).toBe("/env-1/thread-1");
  });

  it("routes notification deep links through hash history for Electron", () => {
    expect(resolveDeepLinkTarget("/env-1/thread-1", "hash")).toBe("#/env-1/thread-1");
  });

  it("rejects missing, non-path, and protocol-relative deep links", () => {
    expect(resolveDeepLinkTarget(undefined, "hash")).toBeNull();
    expect(resolveDeepLinkTarget("https://example.com/env-1/thread-1", "hash")).toBeNull();
    expect(resolveDeepLinkTarget("//example.com/env-1/thread-1", "hash")).toBeNull();
  });
});
