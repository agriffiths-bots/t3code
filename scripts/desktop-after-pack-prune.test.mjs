import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it } from "vite-plus/test";

import afterPack from "./desktop-after-pack-prune.mjs";

async function touch(path) {
  await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true });
  await NodeFSP.writeFile(path, "");
}

async function exists(path) {
  try {
    await NodeFSP.lstat(path);
    return true;
  } catch {
    return false;
  }
}

describe("desktop-after-pack-prune", () => {
  it("keeps only Windows x64 and WSL Linux glibc x64 native sidecars", async () => {
    const tempDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "desktop-prune-"));
    const root = NodePath.join(tempDir, "resources", "app.asar.unpacked", "node_modules");

    await Promise.all([
      touch(NodePath.join(root, "@anthropic-ai/claude-agent-sdk-win32-x64/claude.exe")),
      touch(NodePath.join(root, "@anthropic-ai/claude-agent-sdk-linux-x64/claude")),
      touch(NodePath.join(root, "@anthropic-ai/claude-agent-sdk-linux-x64-musl/claude")),
      touch(NodePath.join(root, "@anthropic-ai/claude-agent-sdk-win32-arm64/claude.exe")),
      touch(NodePath.join(root, "@ff-labs/fff-bin-win32-x64/fff_c.dll")),
      touch(NodePath.join(root, "@ff-labs/fff-bin-linux-x64-gnu/libfff_c.so")),
      touch(NodePath.join(root, "@ff-labs/fff-bin-linux-x64-musl/libfff_c.so")),
      touch(NodePath.join(root, "@yuuang/ffi-rs-win32-x64-msvc/ffi-rs.win32-x64-msvc.node")),
      touch(NodePath.join(root, "@yuuang/ffi-rs-win32-ia32-msvc/ffi-rs.win32-ia32-msvc.node")),
      touch(NodePath.join(root, "@yuuang/ffi-rs-linux-x64-gnu/ffi-rs.linux-x64-gnu.node")),
      touch(NodePath.join(root, "@yuuang/ffi-rs-linux-x64-musl/ffi-rs.linux-x64-musl.node")),
      touch(NodePath.join(root, "node-pty/package.json")),
      touch(NodePath.join(root, "node-pty/lib/index.js")),
      touch(NodePath.join(root, "node-pty/lib/index.js.map")),
      touch(NodePath.join(root, "node-pty/prebuilds/win32-x64/pty.node")),
      touch(NodePath.join(root, "node-pty/prebuilds/win32-arm64/pty.node")),
      touch(NodePath.join(root, "node-pty/prebuilds/linux-x64/pty.node")),
      touch(NodePath.join(root, "node-pty/prebuilds/linux-arm64/pty.node")),
      touch(NodePath.join(root, "node-pty/third_party/conpty/1.23/win10-x64/OpenConsole.exe")),
      touch(NodePath.join(root, "node-pty/third_party/conpty/1.23/win10-arm64/OpenConsole.exe")),
    ]);

    await afterPack({
      appOutDir: tempDir,
      electronPlatformName: "win32",
      arch: 1,
    });

    await expect(
      exists(NodePath.join(root, "@anthropic-ai/claude-agent-sdk-win32-x64")),
    ).resolves.toBe(true);
    await expect(
      exists(NodePath.join(root, "@anthropic-ai/claude-agent-sdk-linux-x64")),
    ).resolves.toBe(true);
    await expect(
      exists(NodePath.join(root, "@anthropic-ai/claude-agent-sdk-linux-x64-musl")),
    ).resolves.toBe(false);
    await expect(
      exists(NodePath.join(root, "@anthropic-ai/claude-agent-sdk-win32-arm64")),
    ).resolves.toBe(false);

    await expect(exists(NodePath.join(root, "@ff-labs/fff-bin-win32-x64"))).resolves.toBe(true);
    await expect(exists(NodePath.join(root, "@ff-labs/fff-bin-linux-x64-gnu"))).resolves.toBe(true);
    await expect(exists(NodePath.join(root, "@ff-labs/fff-bin-linux-x64-musl"))).resolves.toBe(
      false,
    );

    await expect(exists(NodePath.join(root, "@yuuang/ffi-rs-win32-x64-msvc"))).resolves.toBe(true);
    await expect(exists(NodePath.join(root, "@yuuang/ffi-rs-linux-x64-gnu"))).resolves.toBe(true);
    await expect(exists(NodePath.join(root, "@yuuang/ffi-rs-win32-ia32-msvc"))).resolves.toBe(
      false,
    );
    await expect(exists(NodePath.join(root, "@yuuang/ffi-rs-linux-x64-musl"))).resolves.toBe(false);

    await expect(exists(NodePath.join(root, "node-pty/lib/index.js"))).resolves.toBe(true);
    await expect(exists(NodePath.join(root, "node-pty/lib/index.js.map"))).resolves.toBe(false);
    await expect(exists(NodePath.join(root, "node-pty/prebuilds/win32-x64"))).resolves.toBe(true);
    await expect(exists(NodePath.join(root, "node-pty/prebuilds/linux-x64"))).resolves.toBe(true);
    await expect(exists(NodePath.join(root, "node-pty/prebuilds/win32-arm64"))).resolves.toBe(
      false,
    );
    await expect(exists(NodePath.join(root, "node-pty/prebuilds/linux-arm64"))).resolves.toBe(
      false,
    );
    await expect(
      exists(NodePath.join(root, "node-pty/third_party/conpty/1.23/win10-x64")),
    ).resolves.toBe(true);
    await expect(
      exists(NodePath.join(root, "node-pty/third_party/conpty/1.23/win10-arm64")),
    ).resolves.toBe(false);
  });
});
