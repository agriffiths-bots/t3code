#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off - Standalone GitHub Actions helper reads a changed-file list before an Effect runtime exists.
// @effect-diagnostics globalConsole:off - GitHub Actions step prints the boolean gate result directly.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

export function isPureDocumentationPath(filePath: string): boolean {
  const normalized = filePath.replaceAll("\\", "/").replace(/^\.\/+/u, "");
  const basename = NodePath.posix.basename(normalized);
  return (
    normalized.startsWith("docs/") ||
    normalized.startsWith(".github/ISSUE_TEMPLATE/") ||
    normalized === ".github/pull_request_template.md" ||
    basename === "LICENSE" ||
    basename === "NOTICE" ||
    basename === "README" ||
    basename === "CHANGELOG" ||
    basename === "CONTRIBUTING" ||
    /\.(?:md|mdx|txt)$/iu.test(basename)
  );
}

export function shouldRunDesktopLaunchSmoke(files: readonly string[]): boolean {
  return files.some((filePath) => {
    const normalized = filePath
      .trim()
      .replaceAll("\\", "/")
      .replace(/^\.\/+/u, "");
    if (!normalized) return false;
    if (normalized.startsWith("apps/web/")) return false;
    return !isPureDocumentationPath(normalized);
  });
}

function readFilesFromArgs(argv: readonly string[]): string[] {
  const fileListPathIndex = argv.indexOf("--files");
  if (fileListPathIndex >= 0) {
    const fileListPath = argv[fileListPathIndex + 1];
    if (!fileListPath) {
      throw new Error("--files requires a path.");
    }
    return NodeFS.readFileSync(fileListPath, "utf8").split(/\r?\n/u);
  }

  return argv.filter((arg) => !arg.startsWith("--"));
}

function appendGitHubOutput(values: Record<string, string>): void {
  const outputPath = process.env.GITHUB_OUTPUT;
  if (!outputPath) return;

  const body = Object.entries(values)
    .map(([key, value]) => `${key}=${value}`)
    .join("\n");
  NodeFS.appendFileSync(outputPath, `${body}\n`);
}

function main() {
  const files = readFilesFromArgs(process.argv.slice(2));
  const run = shouldRunDesktopLaunchSmoke(files);
  const value = run ? "true" : "false";
  appendGitHubOutput({ run_desktop_launch_smoke: value });
  console.log(value);
}

if (
  process.argv[1] &&
  NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)
) {
  main();
}
