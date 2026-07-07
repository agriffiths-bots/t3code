#!/usr/bin/env node
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

function usage() {
  return [
    "usage: node scripts/desktop-unpacked-footprint.mjs --root <app.asar.unpacked> --max-files <n>",
    "",
    "Fails when the packaged desktop app leaves too many loose files in app.asar.unpacked.",
  ].join("\n");
}

function parseArgs(argv) {
  const options = { root: undefined, maxFiles: undefined };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const value = argv[index + 1];
    switch (arg) {
      case "--root":
        if (!value) throw new Error("--root requires a value");
        options.root = NodePath.resolve(value);
        index += 1;
        break;
      case "--max-files":
        if (!value) throw new Error("--max-files requires a value");
        options.maxFiles = Number.parseInt(value, 10);
        if (!Number.isInteger(options.maxFiles) || options.maxFiles < 1) {
          throw new Error("--max-files must be a positive integer");
        }
        index += 1;
        break;
      case "--help":
      case "-h":
        console.log(usage());
        process.exit(0);
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }
  if (!options.root || options.maxFiles === undefined) {
    throw new Error("Both --root and --max-files are required");
  }
  return options;
}

export async function countFiles(root) {
  const stat = await NodeFSP.lstat(root);
  if (!stat.isDirectory()) {
    return 1;
  }

  let count = 0;
  const entries = await NodeFSP.readdir(root, { withFileTypes: true });
  for (const entry of entries) {
    const entryPath = NodePath.join(root, entry.name);
    if (entry.isDirectory()) {
      count += await countFiles(entryPath);
    } else {
      count += 1;
    }
  }
  return count;
}

export async function assertUnpackedFootprint(root, maxFiles) {
  if (!NodeFS.existsSync(root)) {
    throw new Error(`Desktop unpacked root does not exist: ${root}`);
  }
  const fileCount = await countFiles(root);
  if (fileCount > maxFiles) {
    throw new Error(
      `Desktop unpacked footprint contains ${fileCount} files, exceeding limit ${maxFiles}: ${root}`,
    );
  }
  return fileCount;
}

async function main() {
  const { root, maxFiles } = parseArgs(process.argv.slice(2));
  const fileCount = await assertUnpackedFootprint(root, maxFiles);
  console.log(`[desktop-unpacked-footprint] ${fileCount} files in ${root} (limit ${maxFiles})`);
}

if (process.argv[1] && import.meta.url === NodeURL.pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.stack || error.message : String(error));
    process.exit(1);
  });
}
