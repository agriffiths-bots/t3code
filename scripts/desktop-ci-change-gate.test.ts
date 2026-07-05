import { describe, expect, it } from "vite-plus/test";

import { shouldRunDesktopLaunchSmoke } from "./desktop-ci-change-gate.ts";

describe("desktop CI change gate", () => {
  it("skips web-only and documentation-only changes", () => {
    expect(
      shouldRunDesktopLaunchSmoke([
        "apps/web/src/components/ChatView.tsx",
        "docs/desktop.md",
        "README.md",
      ]),
    ).toBe(false);
  });

  it("runs for desktop, build, workflow, and package dependency changes", () => {
    expect(shouldRunDesktopLaunchSmoke(["apps/desktop/src/main.ts"])).toBe(true);
    expect(shouldRunDesktopLaunchSmoke(["scripts/build-desktop-artifact.ts"])).toBe(true);
    expect(shouldRunDesktopLaunchSmoke([".github/workflows/ci.yml"])).toBe(true);
    expect(shouldRunDesktopLaunchSmoke(["pnpm-lock.yaml"])).toBe(true);
  });
});
