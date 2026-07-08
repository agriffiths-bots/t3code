import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it } from "vite-plus/test";

import {
  buildPosixExecutablePattern,
  listLinuxSmokeProcessIds,
  makeDesktopEnv,
  prepareInstalledExecutableForLaunch,
  readyMarkerMatchesExpectedVersion,
  resolveUpdateChannelFiles,
  waitForUpdateServer,
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

  it("keeps probing until a channel manifest advertises the expected version", async () => {
    const server = NodeHttp.createServer((request, response) => {
      switch (request.url) {
        case "/latest.yml":
          response.writeHead(200, { "Content-Type": "text/yaml" });
          response.end("version: 0.0.1\n");
          return;
        case "/nightly.yml":
          response.writeHead(200, { "Content-Type": "text/yaml" });
          response.end("version: 0.0.2\n");
          return;
        default:
          response.writeHead(404, { "Content-Type": "text/plain" });
          response.end("not found");
      }
    });

    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    try {
      const address = server.address();
      if (!address || typeof address === "string") {
        throw new Error("Test server did not bind to a TCP port");
      }

      const result = await waitForUpdateServer(
        "127.0.0.1",
        address.port,
        Date.now() + 2_000,
        "0.0.2",
      );

      expect(result.channelFile).toBe("/nightly.yml");
      expect(result.text).toContain("version: 0.0.2");
    } finally {
      await new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    }
  });

  it("isolates packaged app home and preserves AppImage extraction in child env", () => {
    const env = makeDesktopEnv({
      appData: "/tmp/app-data",
      backendPort: 3001,
      homeDir: "/tmp/home",
      localAppData: "/tmp/local-app-data",
      readyMarkerPath: "/tmp/ready.json",
      t3Home: "/tmp/t3-home",
      updateServerPort: 3002,
    });

    expect(env.APPIMAGE_EXTRACT_AND_RUN).toBe("1");
    expect(env.HOME).toBe("/tmp/home");
    expect(env.LOCALAPPDATA).toBe("/tmp/local-app-data");
    expect(env.XDG_CONFIG_HOME).toBe("/tmp/app-data");
    expect(env.T3CODE_HOME).toBe("/tmp/t3-home");
  });

  it("makes POSIX installed executables launchable without changing Windows paths", async () => {
    const tempDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "desktop-update-mode-"));
    const executablePath = NodePath.join(tempDir, "T3 Code.AppImage");
    await NodeFSP.writeFile(executablePath, "");
    await NodeFSP.chmod(executablePath, 0o644);

    prepareInstalledExecutableForLaunch(executablePath, "linux");
    expect((await NodeFSP.stat(executablePath)).mode & 0o111).not.toBe(0);

    await NodeFSP.chmod(executablePath, 0o644);
    prepareInstalledExecutableForLaunch(executablePath, "win32");
    expect((await NodeFSP.stat(executablePath)).mode & 0o111).toBe(0);
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
