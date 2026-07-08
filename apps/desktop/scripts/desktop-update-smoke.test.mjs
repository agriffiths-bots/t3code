import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it } from "vite-plus/test";

import {
  buildPosixExecutablePattern,
  listLinuxSmokeProcessIds,
  makeDesktopEnv,
  readyMarkerMatchesExpectedVersion,
  resolveUpdateChannelFiles,
} from "./desktop-update-smoke.mjs";

describe("desktop-update-smoke process cleanup", () => {
  it("uses an anchored escaped executable pattern for POSIX cleanup", async () => {
    const tempDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "desktop-update-smoke-"));
    const executablePath = NodePath.join(tempDir, "T3 Code (Alpha)+.AppImage");
    await NodeFSP.writeFile(executablePath, "");

    const realExecutablePath = await NodeFSP.realpath(executablePath);
    const pattern = buildPosixExecutablePattern(executablePath);

    expect(pattern).toBe(
      `^${realExecutablePath.replace(/[\\^$.*+?()[\]{}|]/g, "\\$&")}([[:space:]]|$)`,
    );
    expect(pattern).toContain("\\(");
    expect(pattern).toContain("\\+");
    expect(pattern).not.toContain("?:");
    expect(pattern).not.toContain("\\s");
  });

  it("finds Linux smoke children by the isolated T3CODE_HOME", async () => {
    const tempDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "desktop-update-proc-"));
    const procRoot = NodePath.join(tempDir, "proc");
    const smokeHome = NodePath.join(tempDir, "t3-home");
    await Promise.all([
      NodeFSP.mkdir(NodePath.join(procRoot, "123"), { recursive: true }),
      NodeFSP.mkdir(NodePath.join(procRoot, "456"), { recursive: true }),
      NodeFSP.mkdir(NodePath.join(procRoot, "not-a-pid"), { recursive: true }),
    ]);
    await Promise.all([
      NodeFSP.writeFile(
        NodePath.join(procRoot, "123", "environ"),
        `T3CODE_HOME=${smokeHome}\0T3CODE_DESKTOP_MOCK_UPDATES=1\0`,
      ),
      NodeFSP.writeFile(NodePath.join(procRoot, "456", "environ"), `T3CODE_HOME=${smokeHome}\0`),
    ]);

    expect(listLinuxSmokeProcessIds({ procRoot, t3Home: smokeHome })).toEqual([123]);
  });

  it("probes platform-specific update channel files before legacy names", () => {
    expect(resolveUpdateChannelFiles("darwin").slice(0, 2)).toEqual([
      "/latest-mac.yml",
      "/nightly-mac.yml",
    ]);
    expect(resolveUpdateChannelFiles("linux").slice(0, 2)).toEqual([
      "/latest-linux.yml",
      "/nightly-linux.yml",
    ]);
    expect(resolveUpdateChannelFiles("linux", "arm64").slice(0, 4)).toEqual([
      "/latest-linux-arm64.yml",
      "/nightly-linux-arm64.yml",
      "/latest-linux.yml",
      "/nightly-linux.yml",
    ]);
    expect(resolveUpdateChannelFiles("win32")).toEqual(["/latest.yml", "/nightly.yml"]);
  });

  it("isolates packaged app home and preserves AppImage extraction in child env", () => {
    const env = makeDesktopEnv({
      appData: "/tmp/app-data",
      backendPort: 3001,
      homeDir: "/tmp/home",
      readyMarkerPath: "/tmp/ready.json",
      t3Home: "/tmp/t3-home",
      updateServerPort: 3002,
    });

    expect(env.APPIMAGE_EXTRACT_AND_RUN).toBe("1");
    expect(env.HOME).toBe("/tmp/home");
    expect(env.XDG_CONFIG_HOME).toBe("/tmp/app-data");
    expect(env.T3CODE_HOME).toBe("/tmp/t3-home");
  });

  it("allows legacy pre-update ready markers without appVersion", () => {
    const marker = {
      status: "main-window-ready",
      windowId: 1,
      url: "t3code://app/",
    };

    expect(readyMarkerMatchesExpectedVersion(marker, "0.0.900")).toBe(false);
    expect(
      readyMarkerMatchesExpectedVersion(marker, "0.0.900", {
        allowMissingVersion: true,
      }),
    ).toBe(true);
  });
});
