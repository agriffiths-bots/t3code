import { describe, expect, it } from "vite-plus/test";

import { shouldBundleDesktopMainDependency } from "./vite.config";

describe("desktop Vite packaging", () => {
  it("bundles Effect runtime packages into the Electron main process", () => {
    expect(shouldBundleDesktopMainDependency("@effect/platform-node/NodeRuntime")).toBe(true);
    expect(shouldBundleDesktopMainDependency("@effect/platform-node/NodeServices")).toBe(true);
    expect(shouldBundleDesktopMainDependency("effect/Effect")).toBe(true);
  });

  it("bundles JavaScript desktop runtime dependencies into the Electron main process", () => {
    expect(shouldBundleDesktopMainDependency("electron-updater")).toBe(true);
    expect(shouldBundleDesktopMainDependency("@clerk/electron")).toBe(true);
    expect(shouldBundleDesktopMainDependency("@clerk/electron/storage")).toBe(true);
    expect(shouldBundleDesktopMainDependency("electron-store")).toBe(true);
    expect(shouldBundleDesktopMainDependency("playwright-core")).toBe(true);
  });

  it("leaves Electron and native runtime packages external", () => {
    expect(shouldBundleDesktopMainDependency("electron")).toBe(false);
    expect(shouldBundleDesktopMainDependency("node:fs")).toBe(false);
    expect(shouldBundleDesktopMainDependency("@clerk/electron-passkeys")).toBe(false);
    expect(shouldBundleDesktopMainDependency("@clerk/electron-passkeys-win32-x64-msvc")).toBe(
      false,
    );
  });
});
