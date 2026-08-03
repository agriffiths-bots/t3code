// @effect-diagnostics nodeBuiltinImport:off - Standalone build-output scanner walks the local dist before an Effect runtime exists.
// @effect-diagnostics globalConsole:off - Build script prints a one-line result and a findings report.
//
// assert-web-no-dev-endpoints — scan EVERY emitted asset of a hosted web build
// for dev/loopback backend endpoints and FAIL the build if any survive.
//
// Root cause it guards (2026-07-22 outage): the auth client executed from a
// NON-ENTRY chunk (textarea-*.js) that had VITE_HTTP_URL=http://127.0.0.1:15773
// / VITE_WS_URL=ws://127.0.0.1:15773 inlined into it. Earlier hotfixes patched
// only the main entry bundle, so the loopback endpoint kept shipping. This
// scanner reads every JS/CSS/HTML asset in the graph — not just the entry — so
// a loopback backend cannot hide in a lazily-loaded chunk.
//
// Precision: the app legitimately renders a few loopback URLs as UI *placeholders*
// (e.g. "http://localhost:5173", "http://127.0.0.1:4096"). Those exact origins
// are allow-listed. Anything else that looks like a loopback backend — any
// ws/wss loopback URL, any non-allowlisted http/https loopback URL, or the
// desktop dev backend port :15773 — is a finding. Bare loopback hostnames with
// no scheme (e.g. the LOOPBACK_HOSTNAMES set literal) are intentionally NOT
// matched, because a hosted build compares against them at runtime.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import { isHostedBuild } from "./lib/hosted-build.ts";

/**
 * Exact loopback origins the web UI renders as harmless input placeholders.
 * Verified against source (apps/web/src/components/ProjectScriptsControl.tsx,
 * packages/contracts/src/settings.ts). Extend ONLY with a source citation.
 */
export const DEFAULT_DEV_ENDPOINT_ALLOWLIST: ReadonlyArray<string> = [
  "http://localhost:5173",
  "http://127.0.0.1:4096",
];

/** Desktop dev backend ports that must never appear in a hosted bundle. */
export const FORBIDDEN_DEV_PORTS: ReadonlyArray<number> = [15773];

const LOOPBACK_HOST_PATTERN = "(?:127\\.0\\.0\\.1|0\\.0\\.0\\.0|localhost|\\[::1\\]|::1)";

// Scheme + loopback host (+ optional :port). Global + case-insensitive.
function loopbackUrlRegex(): RegExp {
  return new RegExp(`(https?|wss?):\\/\\/${LOOPBACK_HOST_PATTERN}(?::(\\d+))?`, "gi");
}

export interface DevEndpointFinding {
  readonly match: string;
  readonly index: number;
  readonly reason: string;
}

/**
 * Scan a single asset's text for dev/loopback backend endpoints. Pure and
 * deterministic; the CLI wraps this over every asset in the graph.
 */
export function scanTextForDevEndpoints(
  text: string,
  options: { readonly allowlist?: ReadonlyArray<string> } = {},
): DevEndpointFinding[] {
  const allowlist = new Set(
    (options.allowlist ?? DEFAULT_DEV_ENDPOINT_ALLOWLIST).map((origin) => origin.toLowerCase()),
  );
  const findings: DevEndpointFinding[] = [];

  const regex = loopbackUrlRegex();
  let urlMatch: RegExpExecArray | null;
  while ((urlMatch = regex.exec(text)) !== null) {
    const full = urlMatch[0];
    const scheme = urlMatch[1] ?? "";
    const port = urlMatch[2];
    // Only a loopback URL with an EXPLICIT PORT is a backend endpoint. Bare
    // loopback origins (e.g. `http://localhost`, no port) are ubiquitous
    // library defaults for URL base resolution and are never the contamination
    // signal — the desktop/dev backend always carries a port (e.g. :15773).
    if (!port) {
      continue;
    }
    const normalizedOrigin = full.toLowerCase();
    const isWebSocket = scheme.toLowerCase().startsWith("ws");
    const nextCharacter = text[urlMatch.index + full.length];
    const hasUrlSuffix = nextCharacter === "/" || nextCharacter === "?" || nextCharacter === "#";
    // Only the exact placeholder origin is harmless. An allowlisted origin
    // followed by a path/query/fragment is an actual endpoint and must fail.
    if (!isWebSocket && allowlist.has(normalizedOrigin) && !hasUrlSuffix) {
      continue;
    }
    findings.push({
      match: full,
      index: urlMatch.index,
      reason: isWebSocket
        ? "loopback WebSocket backend URL"
        : "non-allowlisted loopback backend URL",
    });
  }

  for (const port of FORBIDDEN_DEV_PORTS) {
    const portRegex = new RegExp(`:${port}(?!\\d)`, "g");
    let portMatch: RegExpExecArray | null;
    while ((portMatch = portRegex.exec(text)) !== null) {
      // Avoid double-reporting a hit already captured as a loopback URL above.
      if (
        findings.some(
          (finding) =>
            portMatch!.index >= finding.index &&
            portMatch!.index < finding.index + finding.match.length,
        )
      ) {
        continue;
      }
      findings.push({
        match: `:${port}`,
        index: portMatch.index,
        reason: `desktop dev backend port :${port}`,
      });
    }
  }

  findings.sort((a, b) => a.index - b.index);
  return findings;
}

const SCANNED_EXTENSIONS = new Set([".js", ".mjs", ".cjs", ".css", ".html"]);

async function collectAssetFiles(distDir: string): Promise<string[]> {
  const results: string[] = [];
  async function walk(dir: string): Promise<void> {
    const entries = await NodeFSP.readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      const full = NodePath.join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(full);
      } else if (
        entry.isFile() &&
        SCANNED_EXTENSIONS.has(NodePath.extname(entry.name).toLowerCase())
      ) {
        results.push(full);
      }
    }
  }
  await walk(distDir);
  results.sort();
  return results;
}

export interface AssetScanResult {
  readonly file: string;
  readonly findings: ReadonlyArray<DevEndpointFinding>;
}

/** Scan every JS/CSS/HTML asset under `distDir`. Skips sourcemaps and binaries. */
export async function scanDistForDevEndpoints(
  distDir: string,
  options: { readonly allowlist?: ReadonlyArray<string> } = {},
): Promise<AssetScanResult[]> {
  const files = await collectAssetFiles(distDir);
  const results: AssetScanResult[] = [];
  for (const file of files) {
    const text = await NodeFSP.readFile(file, "utf8");
    const findings = scanTextForDevEndpoints(text, options);
    if (findings.length > 0) {
      results.push({ file, findings });
    }
  }
  return results;
}

async function main(): Promise<void> {
  const distDir = NodePath.resolve(process.argv[2] ?? "apps/web/dist");
  const forced =
    process.env.T3CODE_ASSERT_DEV_ENDPOINTS?.trim() === "1" || process.argv.includes("--force");

  if (!isHostedBuild(process.env) && !forced) {
    console.log(
      "assert-web-no-dev-endpoints: skipped (not a hosted build; set T3CODE_HOSTED_BUILD=1 " +
        "or pass --force to enforce). Desktop/dev builds legitimately pin a loopback backend.",
    );
    return;
  }

  const info = await NodeFSP.stat(distDir).catch(() => null);
  if (!info?.isDirectory()) {
    throw new Error(`web dev-endpoint assertion failed: ${distDir} is not a directory`);
  }

  const results = await scanDistForDevEndpoints(distDir);
  if (results.length > 0) {
    const report = results
      .map((result) => {
        const rel = NodePath.relative(distDir, result.file) || result.file;
        const lines = result.findings
          .map((finding) => `    - ${finding.reason}: …${finding.match}…`)
          .join("\n");
        return `  ${rel}\n${lines}`;
      })
      .join("\n");
    throw new Error(
      `web dev-endpoint assertion failed: hosted bundle contains dev/loopback backend endpoints ` +
        `in ${results.length} asset(s):\n${report}\n` +
        `A hosted build must derive its backend from window.location.origin. This is the ` +
        `2026-07-22 contamination class — fix the build environment, do not allowlist.`,
    );
  }

  console.log("web dev-endpoint assertion passed (no loopback/dev backend endpoints in any asset)");
}

/**
 * True when this file is executed as a script (`node assert-web-no-dev-endpoints.ts …`)
 * rather than imported. Compares fully-resolved REAL paths so a symlinked,
 * relative, or oddly-cased `argv[1]` can never silently turn the deploy-time
 * validator into a no-op (the failure mode of a `file://`-string comparison).
 */
function isDirectRun(): boolean {
  const entry = process.argv[1];
  if (!entry) {
    return false;
  }
  try {
    return (
      NodeFS.realpathSync(entry) === NodeFS.realpathSync(NodeURL.fileURLToPath(import.meta.url))
    );
  } catch {
    return false;
  }
}

if (isDirectRun()) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
