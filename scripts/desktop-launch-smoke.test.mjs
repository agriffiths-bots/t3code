import { describe, expect, it } from "vite-plus/test";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  appendOutput,
  captureFatalOutput,
  readReadyMarker,
  validateReadyMarker,
} from "./desktop-launch-smoke.mjs";

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

describe("desktop-launch-smoke output diagnostics", () => {
  it("keeps stream entries separated while appending output", () => {
    expect(appendOutput("stdout: first", "stderr", "second", 1_000)).toBe(
      "stdout: first\nstderr: second",
    );
  });

  it("captures a bounded fatal snapshot before later output can displace it", () => {
    const snapshot = captureFatalOutput(
      `stdout: booting\nstderr: fatal startup error: missing runtime file\n${"tail\n".repeat(200)}`,
      220,
    );

    expect(snapshot).toContain("fatal startup error");
    expect(snapshot).toContain("[output truncated after fatal]");
    expect(snapshot.length).toBeLessThanOrEqual(220);
  });

  it("keeps context around a fatal pattern after earlier output is truncated", () => {
    const snapshot = captureFatalOutput(
      `${"prefix\n".repeat(200)}stderr: ERR_MODULE_NOT_FOUND: missing package\nstdout: done`,
      220,
    );

    expect(snapshot).toContain("[output truncated before fatal]");
    expect(snapshot).toContain("ERR_MODULE_NOT_FOUND");
    expect(snapshot.length).toBeLessThanOrEqual(220);
  });

  it("does not create a fatal snapshot for ordinary output", () => {
    expect(captureFatalOutput("stdout: ready")).toBeUndefined();
  });
});
