import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

export const PACKAGED_INTEGRITY_MANIFEST_FILE_NAME = "packaged-integrity-manifest.json";
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
const ELECTRON_BUILDER_ARCH_NAMES = new Map([
  [0, "ia32"],
  [1, "x64"],
  [2, "armv7l"],
  [3, "arm64"],
  [4, "universal"],
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

function isPathInside(parentPath, childPath) {
  const relative = NodePath.relative(parentPath, childPath);
  return relative !== "" && !relative.startsWith("..") && !NodePath.isAbsolute(relative);
}

async function isDirectoryLike(path, entry) {
  if (entry.isDirectory()) {
    return true;
  }
  if (!entry.isSymbolicLink()) {
    return false;
  }
  try {
    return (await NodeFSP.stat(path)).isDirectory();
  } catch {
    return false;
  }
}

async function removePackageDirectory(path, unpackedRoot) {
  const stat = await NodeFSP.lstat(path).catch(() => null);
  if (stat === null) {
    return;
  }

  const linkedTarget = stat.isSymbolicLink()
    ? await NodeFSP.realpath(path).catch(() => null)
    : null;
  await removePath(path);
  if (linkedTarget !== null && isPathInside(unpackedRoot, linkedTarget)) {
    await removePath(linkedTarget);
  }
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

function toPosixPath(path) {
  return path.split(NodePath.sep).join("/");
}

async function collectRelativeFiles(input) {
  const { root, current, logicalRoot, files, activeDirectories } = input;
  const realCurrent = await NodeFSP.realpath(current).catch(() => current);
  if (activeDirectories.has(realCurrent)) {
    return;
  }
  activeDirectories.add(realCurrent);

  try {
    for (const entry of await listEntries(current)) {
      const entryPath = NodePath.join(current, entry.name);
      const logicalPath = logicalRoot ? `${logicalRoot}/${entry.name}` : entry.name;
      const targetStat = entry.isSymbolicLink()
        ? await NodeFSP.stat(entryPath).catch(() => null)
        : null;

      if (entry.isDirectory() || targetStat?.isDirectory()) {
        await collectRelativeFiles({
          root,
          current: entryPath,
          logicalRoot: logicalPath,
          files,
          activeDirectories,
        });
        continue;
      }

      if (entry.isFile() || targetStat?.isFile()) {
        files.add(toPosixPath(NodePath.relative(root, entryPath)));
      }
    }
  } finally {
    activeDirectories.delete(realCurrent);
  }
}

export async function createPackagedIntegrityManifest(unpackedRoot) {
  const requiredFiles = new Set();
  await collectRelativeFiles({
    root: unpackedRoot,
    current: unpackedRoot,
    logicalRoot: "",
    files: requiredFiles,
    activeDirectories: new Set(),
  });

  return {
    version: 1,
    generatedAt: new Date().toISOString(),
    requiredFiles: [...requiredFiles].sort(),
  };
}

async function writePackagedIntegrityManifest(resourcesDir, unpackedRoot) {
  const manifest = await createPackagedIntegrityManifest(unpackedRoot);
  await NodeFSP.writeFile(
    NodePath.join(resourcesDir, PACKAGED_INTEGRITY_MANIFEST_FILE_NAME),
    `${JSON.stringify(manifest, null, 2)}\n`,
  );
  console.log(
    `[desktop-after-pack-prune] wrote ${manifest.requiredFiles.length} packaged integrity entries`,
  );
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

function shouldKeepPrebuildDirectory(platform, archName, directoryName) {
  const linuxArch = linuxArchFor(archName);
  if (platform === "darwin") {
    return directoryName === `darwin-${archName}`;
  }
  if (platform === "win32") {
    return (
      directoryName === `win32-${archName}` ||
      (linuxArch !== null && directoryName === `linux-${linuxArch}`)
    );
  }
  return linuxArch !== null && directoryName === `linux-${linuxArch}`;
}

function resolveArchName(arch) {
  if (typeof arch === "string") {
    return arch;
  }
  return ELECTRON_BUILDER_ARCH_NAMES.get(arch) ?? null;
}

function linuxArchFor(archName) {
  return archName === "arm64" ? "arm64" : archName === "x64" ? "x64" : null;
}

function shouldKeepNativePackageDirectory(platform, archName, packageName) {
  if (packageName.startsWith("claude-agent-sdk-")) {
    const linuxArch = linuxArchFor(archName);
    if (platform === "win32") {
      return (
        packageName === `claude-agent-sdk-win32-${archName}` ||
        (linuxArch !== null && packageName === `claude-agent-sdk-linux-${linuxArch}`)
      );
    }
    if (platform === "darwin") {
      return packageName === `claude-agent-sdk-darwin-${archName}`;
    }
    return linuxArch !== null && packageName === `claude-agent-sdk-linux-${linuxArch}`;
  }

  if (packageName.startsWith("fff-bin-")) {
    const linuxArch = linuxArchFor(archName);
    if (platform === "win32") {
      return (
        packageName === `fff-bin-win32-${archName}` ||
        (linuxArch !== null && packageName === `fff-bin-linux-${linuxArch}-gnu`)
      );
    }
    if (platform === "darwin") {
      return packageName === `fff-bin-darwin-${archName}`;
    }
    return linuxArch !== null && packageName === `fff-bin-linux-${linuxArch}-gnu`;
  }

  if (packageName.startsWith("ffi-rs-")) {
    const linuxArch = linuxArchFor(archName);
    if (platform === "win32") {
      return (
        packageName === `ffi-rs-win32-${archName}-msvc` ||
        (linuxArch !== null && packageName === `ffi-rs-linux-${linuxArch}-gnu`)
      );
    }
    if (platform === "darwin") {
      return packageName === `ffi-rs-darwin-${archName}`;
    }
    return linuxArch !== null && packageName === `ffi-rs-linux-${linuxArch}-gnu`;
  }

  return true;
}

async function pruneScopedNativePackages(unpackedRoot, scopeDir, platform, archName) {
  for (const entry of await listEntries(scopeDir)) {
    const entryPath = NodePath.join(scopeDir, entry.name);
    if (!(await isDirectoryLike(entryPath, entry))) {
      continue;
    }
    if (!shouldKeepNativePackageDirectory(platform, archName, entry.name)) {
      await removePackageDirectory(entryPath, unpackedRoot);
    }
  }
  await removeEmptyDirectories(scopeDir);
}

async function pruneNativeSidecars(root, platform, arch) {
  const archName = resolveArchName(arch);
  if (archName === null || archName === "universal") {
    return;
  }

  const nodeModulesDir = NodePath.join(root, "node_modules");
  await Promise.all([
    pruneScopedNativePackages(
      root,
      NodePath.join(nodeModulesDir, "@anthropic-ai"),
      platform,
      archName,
    ),
    pruneScopedNativePackages(root, NodePath.join(nodeModulesDir, "@ff-labs"), platform, archName),
    pruneScopedNativePackages(root, NodePath.join(nodeModulesDir, "@yuuang"), platform, archName),
  ]);
}

async function pruneNodePtyPrebuilds(nodePtyDir, platform, archName) {
  const prebuildsDir = NodePath.join(nodePtyDir, "prebuilds");
  for (const entry of await listEntries(prebuildsDir)) {
    const entryPath = NodePath.join(prebuildsDir, entry.name);
    if (!entry.isDirectory()) {
      await removePath(entryPath);
      continue;
    }
    if (!shouldKeepPrebuildDirectory(platform, archName, entry.name)) {
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

async function pruneNodePtyThirdParty(nodePtyDir, platform, archName) {
  const thirdPartyDir = NodePath.join(nodePtyDir, "third_party");
  if (platform !== "win32") {
    await removePath(thirdPartyDir);
    return;
  }

  const conptyDir = NodePath.join(thirdPartyDir, "conpty");
  for (const version of await listEntries(conptyDir)) {
    const versionPath = NodePath.join(conptyDir, version.name);
    if (!version.isDirectory()) {
      await removePath(versionPath);
      continue;
    }
    for (const entry of await listEntries(versionPath)) {
      const entryPath = NodePath.join(versionPath, entry.name);
      if (!entry.isDirectory()) {
        await removePath(entryPath);
        continue;
      }
      if (entry.name !== `win10-${archName}`) {
        await removePath(entryPath);
      }
    }
    await removeEmptyDirectories(versionPath);
  }
  await removeEmptyDirectories(conptyDir);
  await removeEmptyDirectories(thirdPartyDir);
}

async function pruneNodePty(nodePtyDir, platform, arch) {
  if (!(await pathExists(nodePtyDir))) {
    return;
  }
  const archName = resolveArchName(arch);
  if (archName === null || archName === "universal") {
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
  await pruneNodePtyPrebuilds(nodePtyDir, platform, archName);
  await pruneNodePtyThirdParty(nodePtyDir, platform, archName);
}

export async function resolveAppAsarUnpackedRoots(appOutDir) {
  const roots = [
    NodePath.join(appOutDir, "resources", "app.asar.unpacked"),
    NodePath.join(appOutDir, "Contents", "Resources", "app.asar.unpacked"),
  ];
  for (const entry of await listEntries(appOutDir)) {
    if (!entry.isDirectory() || !entry.name.endsWith(".app")) {
      continue;
    }
    roots.push(NodePath.join(appOutDir, entry.name, "Contents", "Resources", "app.asar.unpacked"));
  }
  return [...new Set(roots)];
}

export default async function afterPack(context) {
  for (const root of await resolveAppAsarUnpackedRoots(context.appOutDir)) {
    if (!(await pathExists(root))) {
      continue;
    }
    await pruneNativeSidecars(root, context.electronPlatformName, context.arch);
    await pruneNodePty(
      NodePath.join(root, "node_modules", "node-pty"),
      context.electronPlatformName,
      context.arch,
    );
    await writePackagedIntegrityManifest(NodePath.dirname(root), root);
  }
}
