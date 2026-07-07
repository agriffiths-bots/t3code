import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

const RUNTIME_NODE_PTY_BUILD_FILE = NodePath.normalize("Release/pty.node");
const NODE_PTY_KEEP_PREBUILD_FILE_NAMES = new Set([
  "pty.node",
  "conpty.node",
  "conpty_console_list.node",
  "winpty.dll",
  "winpty-agent.exe",
  "spawn-helper",
  "t3code-wsl-node-pty.json",
]);

async function pathExists(path) {
  try {
    await NodeFSP.lstat(path);
    return true;
  } catch {
    return false;
  }
}

async function removePath(path) {
  await NodeFSP.rm(path, { recursive: true, force: true });
}

async function listEntries(path) {
  try {
    return await NodeFSP.readdir(path, { withFileTypes: true });
  } catch {
    return [];
  }
}

async function removeEmptyDirectories(path) {
  for (const entry of await listEntries(path)) {
    if (entry.isDirectory()) {
      await removeEmptyDirectories(NodePath.join(path, entry.name));
    }
  }
  const remaining = await listEntries(path);
  if (remaining.length === 0) {
    await removePath(path);
  }
}

async function pruneNodePtyLibDirectory(directory) {
  for (const entry of await listEntries(directory)) {
    const entryPath = NodePath.join(directory, entry.name);
    if (entry.isDirectory()) {
      await pruneNodePtyLibDirectory(entryPath);
      await removeEmptyDirectories(entryPath);
      continue;
    }
    if (entry.name.endsWith(".map") || entry.name.endsWith(".test.js")) {
      await removePath(entryPath);
    }
  }
}

async function pruneNodePtyLib(nodePtyDir) {
  await pruneNodePtyLibDirectory(NodePath.join(nodePtyDir, "lib"));
}

async function pruneNodePtyBuild(nodePtyDir) {
  const buildDir = NodePath.join(nodePtyDir, "build");
  const visit = async (directory) => {
    for (const entry of await listEntries(directory)) {
      const entryPath = NodePath.join(directory, entry.name);
      if (entry.isDirectory()) {
        await visit(entryPath);
        await removeEmptyDirectories(entryPath);
        continue;
      }
      const relative = NodePath.normalize(NodePath.relative(buildDir, entryPath));
      if (relative !== RUNTIME_NODE_PTY_BUILD_FILE) {
        await removePath(entryPath);
      }
    }
  };
  await visit(buildDir);
}

function shouldKeepPrebuildDirectory(platform, directoryName) {
  if (platform === "darwin") {
    return directoryName.startsWith("darwin-");
  }
  if (platform === "win32") {
    return directoryName.startsWith("win32-") || directoryName.startsWith("linux-");
  }
  return directoryName.startsWith("linux-");
}

async function pruneNodePtyPrebuilds(nodePtyDir, platform) {
  const prebuildsDir = NodePath.join(nodePtyDir, "prebuilds");
  for (const entry of await listEntries(prebuildsDir)) {
    const entryPath = NodePath.join(prebuildsDir, entry.name);
    if (!entry.isDirectory()) {
      await removePath(entryPath);
      continue;
    }
    if (!shouldKeepPrebuildDirectory(platform, entry.name)) {
      await removePath(entryPath);
      continue;
    }
    for (const child of await listEntries(entryPath)) {
      if (!NODE_PTY_KEEP_PREBUILD_FILE_NAMES.has(child.name)) {
        await removePath(NodePath.join(entryPath, child.name));
      }
    }
    await removeEmptyDirectories(entryPath);
  }
  await removeEmptyDirectories(prebuildsDir);
}

async function pruneNodePty(nodePtyDir, platform) {
  if (!(await pathExists(nodePtyDir))) {
    return;
  }

  await Promise.all([
    removePath(NodePath.join(nodePtyDir, "binding.gyp")),
    removePath(NodePath.join(nodePtyDir, "README.md")),
    removePath(NodePath.join(nodePtyDir, "deps")),
    removePath(NodePath.join(nodePtyDir, "scripts")),
    removePath(NodePath.join(nodePtyDir, "src")),
    removePath(NodePath.join(nodePtyDir, "typings")),
  ]);
  await pruneNodePtyLib(nodePtyDir);
  await pruneNodePtyBuild(nodePtyDir);
  await pruneNodePtyPrebuilds(nodePtyDir, platform);
}

function resolveAppAsarUnpackedRoots(appOutDir) {
  return [
    NodePath.join(appOutDir, "resources", "app.asar.unpacked"),
    NodePath.join(appOutDir, "Contents", "Resources", "app.asar.unpacked"),
  ];
}

export default async function afterPack(context) {
  for (const root of resolveAppAsarUnpackedRoots(context.appOutDir)) {
    await pruneNodePty(
      NodePath.join(root, "node_modules", "node-pty"),
      context.electronPlatformName,
    );
  }
}
