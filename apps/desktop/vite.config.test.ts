import { describe, expect, it } from "vite-plus/test";

import { shouldBundleDesktopMainDependency } from "./vite.config";

describe("desktop Vite packaging", () => {
  it("bundles Effect runtime packages into the Electron main process", () => {
    expect(shouldBundleDesktopMainDependency("@effect/platform-node/NodeRuntime")).toBe(true);
    expect(shouldBundleDesktopMainDependency("@effect/platform-node/NodeServices")).toBe(true);
    expect(shouldBundleDesktopMainDependency("effect/Effect")).toBe(true);
  });

  it("leaves Electron and file-backed desktop runtime packages external", () => {
    expect(shouldBundleDesktopMainDependency("electron")).toBe(false);
    expect(shouldBundleDesktopMainDependency("electron-updater")).toBe(false);
    expect(shouldBundleDesktopMainDependency("@clerk/electron")).toBe(false);
    expect(shouldBundleDesktopMainDependency("playwright-core")).toBe(false);
  });
});
