import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it } from "vite-plus/test";

import { buildPosixExecutablePattern, listLinuxSmokeProcessIds } from "./desktop-update-smoke.mjs";

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
});
