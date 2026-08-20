import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it } from "vite-plus/test";

import afterPack, {
  createPackagedIntegrityManifest,
  matrixCryptoBindingPrefixes,
  PACKAGED_INTEGRITY_MANIFEST_FILE_NAME,
  resolveAppAsarUnpackedRoots,
} from "./desktop-after-pack-prune.mjs";

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
    const badFfiTarget = NodePath.join(
      root,
      ".pnpm/@yuuang+ffi-rs-win32-ia32-msvc@1.3.2/node_modules/@yuuang/ffi-rs-win32-ia32-msvc",
    );

    await Promise.all([
      touch(NodePath.join(root, "@anthropic-ai/claude-agent-sdk-win32-x64/claude.exe")),
      touch(NodePath.join(root, "@anthropic-ai/claude-agent-sdk-linux-x64/claude")),
      touch(NodePath.join(root, "@anthropic-ai/claude-agent-sdk-linux-x64-musl/claude")),
      touch(NodePath.join(root, "@anthropic-ai/claude-agent-sdk-win32-arm64/claude.exe")),
      touch(NodePath.join(root, "@ff-labs/fff-bin-win32-x64/fff_c.dll")),
      touch(NodePath.join(root, "@ff-labs/fff-bin-linux-x64-gnu/libfff_c.so")),
      touch(NodePath.join(root, "@ff-labs/fff-bin-linux-x64-musl/libfff_c.so")),
      touch(NodePath.join(root, "@yuuang/ffi-rs-win32-x64-msvc/ffi-rs.win32-x64-msvc.node")),
      touch(NodePath.join(badFfiTarget, "ffi-rs.win32-ia32-msvc.node")),
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
    await NodeFSP.mkdir(NodePath.join(root, "@yuuang"), { recursive: true });
    await NodeFSP.symlink(
      badFfiTarget,
      NodePath.join(root, "@yuuang/ffi-rs-win32-ia32-msvc"),
      "dir",
    );

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
    await expect(exists(badFfiTarget)).resolves.toBe(false);
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

    const manifestPath = NodePath.join(tempDir, "resources", PACKAGED_INTEGRITY_MANIFEST_FILE_NAME);
    await expect(exists(manifestPath)).resolves.toBe(true);
    const manifest = JSON.parse(await NodeFSP.readFile(manifestPath, "utf8"));
    expect(manifest.version).toBe(1);
    expect(manifest.requiredFiles).toContain("node_modules/node-pty/package.json");
    expect(manifest.requiredFiles).toContain("node_modules/node-pty/prebuilds/win32-x64/pty.node");
    expect(manifest.requiredFiles).toContain(
      "node_modules/@ff-labs/fff-bin-linux-x64-gnu/libfff_c.so",
    );
    expect(manifest.requiredFiles).not.toContain(
      "node_modules/node-pty/prebuilds/win32-arm64/pty.node",
    );
    expect(manifest.requiredFiles).not.toContain(
      "node_modules/@ff-labs/fff-bin-linux-x64-musl/libfff_c.so",
    );
  });

  it("keeps only the target Matrix crypto binding and drops its installer", async () => {
    const tempDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "desktop-prune-matrix-"));
    const root = NodePath.join(tempDir, "resources", "app.asar.unpacked", "node_modules");
    const cryptoDir = NodePath.join(root, "@matrix-org", "matrix-sdk-crypto-nodejs");

    await Promise.all([
      touch(NodePath.join(cryptoDir, "package.json")),
      touch(NodePath.join(cryptoDir, "index.js")),
      touch(NodePath.join(cryptoDir, "index.d.ts")),
      touch(NodePath.join(cryptoDir, "download-lib.js")),
      touch(NodePath.join(cryptoDir, "README.md")),
      touch(NodePath.join(cryptoDir, ".node-version")),
      touch(NodePath.join(cryptoDir, "node_modules/node-downloader-helper/package.json")),
      touch(NodePath.join(cryptoDir, "matrix-sdk-crypto.linux-x64-gnu.node")),
      touch(NodePath.join(cryptoDir, "matrix-sdk-crypto.darwin-arm64.node")),
      touch(NodePath.join(cryptoDir, "matrix-sdk-crypto.win32-x64-msvc.node")),
    ]);

    await afterPack({
      appOutDir: tempDir,
      electronPlatformName: "linux",
      arch: 1,
    });

    await expect(
      exists(NodePath.join(cryptoDir, "matrix-sdk-crypto.linux-x64-gnu.node")),
    ).resolves.toBe(true);
    await expect(
      exists(NodePath.join(cryptoDir, "matrix-sdk-crypto.darwin-arm64.node")),
    ).resolves.toBe(false);
    await expect(
      exists(NodePath.join(cryptoDir, "matrix-sdk-crypto.win32-x64-msvc.node")),
    ).resolves.toBe(false);
    await expect(exists(NodePath.join(cryptoDir, "index.js"))).resolves.toBe(true);
    await expect(exists(NodePath.join(cryptoDir, "package.json"))).resolves.toBe(true);
    await expect(exists(NodePath.join(cryptoDir, "download-lib.js"))).resolves.toBe(false);
    await expect(exists(NodePath.join(cryptoDir, "index.d.ts"))).resolves.toBe(false);
    await expect(exists(NodePath.join(cryptoDir, "README.md"))).resolves.toBe(false);
    await expect(exists(NodePath.join(cryptoDir, ".node-version"))).resolves.toBe(false);
    await expect(exists(NodePath.join(cryptoDir, "node_modules"))).resolves.toBe(false);

    const manifestPath = NodePath.join(tempDir, "resources", PACKAGED_INTEGRITY_MANIFEST_FILE_NAME);
    const manifest = JSON.parse(await NodeFSP.readFile(manifestPath, "utf8"));
    expect(manifest.requiredFiles).toContain(
      "node_modules/@matrix-org/matrix-sdk-crypto-nodejs/matrix-sdk-crypto.linux-x64-gnu.node",
    );
  });

  it("names the Matrix crypto bindings each packaged target can load", () => {
    expect(matrixCryptoBindingPrefixes("linux", "x64")).toEqual(["matrix-sdk-crypto.linux-x64"]);
    expect(matrixCryptoBindingPrefixes("linux", "arm64")).toEqual([
      "matrix-sdk-crypto.linux-arm64",
    ]);
    expect(matrixCryptoBindingPrefixes("darwin", "arm64")).toEqual([
      "matrix-sdk-crypto.darwin-universal",
      "matrix-sdk-crypto.darwin-arm64",
    ]);
    expect(matrixCryptoBindingPrefixes("darwin", "universal")).toEqual([
      "matrix-sdk-crypto.darwin-universal",
      "matrix-sdk-crypto.darwin-arm64",
      "matrix-sdk-crypto.darwin-x64",
    ]);
    // Windows keeps the Linux binding its WSL backend loads.
    expect(matrixCryptoBindingPrefixes("win32", "x64")).toEqual([
      "matrix-sdk-crypto.win32-x64",
      "matrix-sdk-crypto.linux-x64",
    ]);
    expect(matrixCryptoBindingPrefixes("linux", "ia32")).toEqual([]);
  });

  it("keeps the WSL Linux crypto binding inside Windows packages", async () => {
    const tempDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "desktop-prune-wsl-"));
    const root = NodePath.join(tempDir, "resources", "app.asar.unpacked", "node_modules");
    const cryptoDir = NodePath.join(root, "@matrix-org", "matrix-sdk-crypto-nodejs");

    await Promise.all([
      touch(NodePath.join(cryptoDir, "package.json")),
      touch(NodePath.join(cryptoDir, "index.js")),
      touch(NodePath.join(cryptoDir, "matrix-sdk-crypto.win32-x64-msvc.node")),
      touch(NodePath.join(cryptoDir, "matrix-sdk-crypto.linux-x64-gnu.node")),
      touch(NodePath.join(cryptoDir, "matrix-sdk-crypto.linux-arm64-gnu.node")),
      touch(NodePath.join(cryptoDir, "matrix-sdk-crypto.darwin-arm64.node")),
    ]);

    await afterPack({ appOutDir: tempDir, electronPlatformName: "win32", arch: 1 });

    await expect(
      exists(NodePath.join(cryptoDir, "matrix-sdk-crypto.win32-x64-msvc.node")),
    ).resolves.toBe(true);
    await expect(
      exists(NodePath.join(cryptoDir, "matrix-sdk-crypto.linux-x64-gnu.node")),
    ).resolves.toBe(true);
    await expect(
      exists(NodePath.join(cryptoDir, "matrix-sdk-crypto.linux-arm64-gnu.node")),
    ).resolves.toBe(false);
    await expect(
      exists(NodePath.join(cryptoDir, "matrix-sdk-crypto.darwin-arm64.node")),
    ).resolves.toBe(false);
  });

  it("keeps both architecture bindings in a universal macOS package", async () => {
    const tempDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "desktop-prune-univ-"));
    const root = NodePath.join(tempDir, "resources", "app.asar.unpacked", "node_modules");
    const cryptoDir = NodePath.join(root, "@matrix-org", "matrix-sdk-crypto-nodejs");

    await Promise.all([
      touch(NodePath.join(cryptoDir, "package.json")),
      touch(NodePath.join(cryptoDir, "index.js")),
      touch(NodePath.join(cryptoDir, "download-lib.js")),
      touch(NodePath.join(cryptoDir, "matrix-sdk-crypto.darwin-arm64.node")),
      touch(NodePath.join(cryptoDir, "matrix-sdk-crypto.darwin-x64.node")),
      touch(NodePath.join(cryptoDir, "matrix-sdk-crypto.linux-x64-gnu.node")),
    ]);

    // electron-builder reports the universal architecture on its final hook.
    await afterPack({ appOutDir: tempDir, electronPlatformName: "darwin", arch: 4 });

    await expect(
      exists(NodePath.join(cryptoDir, "matrix-sdk-crypto.darwin-arm64.node")),
    ).resolves.toBe(true);
    await expect(
      exists(NodePath.join(cryptoDir, "matrix-sdk-crypto.darwin-x64.node")),
    ).resolves.toBe(true);
    await expect(
      exists(NodePath.join(cryptoDir, "matrix-sdk-crypto.linux-x64-gnu.node")),
    ).resolves.toBe(false);
    await expect(exists(NodePath.join(cryptoDir, "download-lib.js"))).resolves.toBe(false);
  });

  it("breaks directory symlink cycles while collecting manifest entries", async () => {
    const tempDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "desktop-prune-cycle-"));
    const root = NodePath.join(tempDir, "resources", "app.asar.unpacked");
    const packageRoot = NodePath.join(root, "node_modules", "cycle-package");
    await touch(NodePath.join(packageRoot, "package.json"));
    await NodeFSP.symlink(packageRoot, NodePath.join(packageRoot, "loop"), "dir");

    const manifest = await createPackagedIntegrityManifest(root);

    expect(manifest.requiredFiles).toContain("node_modules/cycle-package/package.json");
    expect(manifest.requiredFiles.some((file) => file.includes("/loop/loop/"))).toBe(false);
  });

  it("writes the packaged integrity manifest inside macOS app bundles", async () => {
    const tempDir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "desktop-prune-mac-"));
    const unpackedRoot = NodePath.join(
      tempDir,
      "T3 Code.app",
      "Contents",
      "Resources",
      "app.asar.unpacked",
    );
    await touch(NodePath.join(unpackedRoot, "node_modules/runtime/package.json"));

    await expect(resolveAppAsarUnpackedRoots(tempDir)).resolves.toContain(unpackedRoot);
    await afterPack({
      appOutDir: tempDir,
      electronPlatformName: "darwin",
      arch: 1,
    });

    const manifestPath = NodePath.join(
      tempDir,
      "T3 Code.app",
      "Contents",
      "Resources",
      PACKAGED_INTEGRITY_MANIFEST_FILE_NAME,
    );
    await expect(exists(manifestPath)).resolves.toBe(true);
    const manifest = JSON.parse(await NodeFSP.readFile(manifestPath, "utf8"));
    expect(manifest.requiredFiles).toContain("node_modules/runtime/package.json");
  });
});
