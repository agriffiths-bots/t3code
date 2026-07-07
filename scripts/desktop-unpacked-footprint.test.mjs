import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it } from "vite-plus/test";

import { assertUnpackedFootprint, countFiles } from "./desktop-unpacked-footprint.mjs";

describe("desktop-unpacked-footprint", () => {
  it("counts nested loose files", async () => {
    const tempDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "desktop-footprint-"));
    await NodeFSP.mkdir(NodePath.join(tempDir, "apps/server/dist"), { recursive: true });
    await NodeFSP.mkdir(NodePath.join(tempDir, "node_modules/node-pty/lib"), { recursive: true });
    await NodeFSP.writeFile(NodePath.join(tempDir, "apps/server/dist/bin.mjs"), "");
    await NodeFSP.writeFile(NodePath.join(tempDir, "node_modules/node-pty/package.json"), "{}");
    await NodeFSP.writeFile(NodePath.join(tempDir, "node_modules/node-pty/lib/index.js"), "");

    await expect(countFiles(tempDir)).resolves.toBe(3);
    await expect(assertUnpackedFootprint(tempDir, 3)).resolves.toBe(3);
    await expect(assertUnpackedFootprint(tempDir, 2)).rejects.toThrow(/exceeding limit 2/);
  });
});
