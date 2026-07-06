import { describe, expect, it } from "vite-plus/test";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { readReadyMarker, validateReadyMarker } from "./desktop-launch-smoke.mjs";

describe("desktop-launch-smoke ready marker validation", () => {
  it("accepts a loaded app window even when the CI runner reports it hidden", () => {
    expect(
      validateReadyMarker(
        {
          status: "main-window-ready",
          windowId: 1,
          title: "T3 Code (Alpha)",
          visible: false,
          url: "t3code://app/",
        },
        "main-window-ready.json",
      ),
    ).toMatchObject({
      windowId: 1,
      visible: false,
      url: "t3code://app/",
    });
  });

  it("rejects a marker that has not loaded the app URL", () => {
    expect(() =>
      validateReadyMarker(
        {
          status: "main-window-ready",
          windowId: 1,
          title: "T3 Code (Alpha)",
          visible: true,
          url: "",
        },
        "main-window-ready.json",
      ),
    ).toThrow(/loaded window URL/);
  });

  it("rejects a marker for a non-app URL", () => {
    expect(() =>
      validateReadyMarker(
        {
          status: "main-window-ready",
          windowId: 1,
          title: "T3 Code (Alpha)",
          visible: true,
          url: "about:blank",
        },
        "main-window-ready.json",
      ),
    ).toThrow(/unexpected window URL/);
  });

  it("treats a partially written marker as not ready yet", async () => {
    const tempDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "desktop-smoke-test-"));
    const markerPath = NodePath.join(tempDir, "main-window-ready.json");
    await NodeFSP.writeFile(markerPath, '{"status":');

    await expect(readReadyMarker(markerPath)).resolves.toBeUndefined();
  });
});
