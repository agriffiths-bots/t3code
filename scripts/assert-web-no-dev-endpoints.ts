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
// Anything that looks like a loopback backend — any ported http(s)/ws(s)
// loopback URL or the desktop dev backend port :15773 — is a finding. There is
// deliberately no origin allowlist: emitted text cannot prove whether a literal
// came from harmless UI copy or a configured backend. Bare loopback hostnames
// with no scheme (e.g. the LOOPBACK_HOSTNAMES set literal) are intentionally
// NOT matched, because a hosted build compares against them at runtime.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import { isHostedBuild } from "./lib/hosted-build.ts";

/** Desktop dev backend ports that must never appear in a hosted bundle. */
export const FORBIDDEN_DEV_PORTS: ReadonlyArray<number> = [15773];

// Explicit or protocol-relative URL + optional userinfo + host + explicit port.
// Userinfo is matched greedily through the final @ so it cannot conceal the
// host, but is never copied into findings. URL parsing below normalizes IPv4
// shorthand/integer spellings before loopback classification. WHATWG special
// schemes also normalize missing or backslash separators, so accept those raw
// forms here and classify their host exactly as the client does.
function loopbackUrlRegex(): RegExp {
  const userinfo = "(?:[^\\s/?#'\"<>\\\\]*@)?";
  const host = "(\\[[^\\]\\s/?#'\"<>\\\\]+\\]|[^\\s:/?#'\"<>\\\\]+)";
  return new RegExp(
    `(?:(https?|wss?):[\\\\/]*|(?<![:\\\\/])[\\\\/]{2,})${userinfo}${host}:(\\d+)`,
    "gi",
  );
}

function isLoopbackUrlHost(scheme: string, host: string, port: string): boolean {
  try {
    const parsedHost = new URL(`${scheme || "http"}://${host}:${port}`).hostname
      .toLowerCase()
      .replace(/^\[|\]$/g, "")
      .replace(/\.$/, "");
    if (
      parsedHost === "localhost" ||
      parsedHost.endsWith(".localhost") ||
      parsedHost === "::" ||
      parsedHost === "::1" ||
      parsedHost === "0.0.0.0"
    ) {
      return true;
    }
    const mappedIpv4 = /^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/.exec(parsedHost);
    if (mappedIpv4 !== null) {
      const high = Number.parseInt(mappedIpv4[1] ?? "", 16);
      const low = Number.parseInt(mappedIpv4[2] ?? "", 16);
      if ((high === 0 && low === 0) || (high & 0xff00) === 0x7f00) {
        return true;
      }
    }
    return /^\d+\.\d+\.\d+\.\d+$/.test(parsedHost) && parsedHost.split(".")[0] === "127";
  } catch {
    return false;
  }
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
export function scanTextForDevEndpoints(text: string): DevEndpointFinding[] {
  const findings: DevEndpointFinding[] = [];
  const matchedUrlRanges: Array<{ readonly start: number; readonly end: number }> = [];

  const regex = loopbackUrlRegex();
  let urlMatch: RegExpExecArray | null;
  while ((urlMatch = regex.exec(text)) !== null) {
    const full = urlMatch[0];
    const scheme = urlMatch[1] ?? "";
    const host = urlMatch[2] ?? "";
    const port = urlMatch[3];
    if (!isLoopbackUrlHost(scheme, host, port ?? "")) {
      continue;
    }
    const isWebSocket = scheme.toLowerCase().startsWith("ws");
    matchedUrlRanges.push({ start: urlMatch.index, end: urlMatch.index + full.length });
    findings.push({
      // Do not expose optional URL credentials in logs or review artifacts.
      match: scheme ? `${scheme}://${host}:${port}` : `//${host}:${port}`,
      index: urlMatch.index,
      reason: isWebSocket ? "loopback WebSocket backend URL" : "loopback backend URL",
    });
  }

  for (const port of FORBIDDEN_DEV_PORTS) {
    const portRegex = new RegExp(`:${port}(?!\\d)`, "g");
    let portMatch: RegExpExecArray | null;
    while ((portMatch = portRegex.exec(text)) !== null) {
      // Avoid double-reporting a hit already captured as a loopback URL above.
      if (
        matchedUrlRanges.some(
          (range) => portMatch!.index >= range.start && portMatch!.index < range.end,
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
export async function scanDistForDevEndpoints(distDir: string): Promise<AssetScanResult[]> {
  const files = await collectAssetFiles(distDir);
  const results: AssetScanResult[] = [];
  for (const file of files) {
    const text = await NodeFSP.readFile(file, "utf8");
    const findings = scanTextForDevEndpoints(text);
    if (findings.length > 0) {
      results.push({ file, findings });
    }
  }
  return results;
}

export function parseScannerCliArgs(args: ReadonlyArray<string>): {
  readonly distDir: string;
  readonly forced: boolean;
} {
  const positional: string[] = [];
  let forced = false;
  for (const arg of args) {
    if (arg === "--force") {
      forced = true;
    } else if (arg.startsWith("--")) {
      throw new Error(`web dev-endpoint assertion failed: unknown option ${arg}`);
    } else {
      positional.push(arg);
    }
  }
  if (positional.length > 1) {
    throw new Error("web dev-endpoint assertion failed: expected at most one dist directory");
  }
  return {
    distDir: NodePath.resolve(positional[0] ?? "apps/web/dist"),
    forced,
  };
}

async function main(): Promise<void> {
  const parsed = parseScannerCliArgs(process.argv.slice(2));
  const distDir = parsed.distDir;
  const forced = process.env.T3CODE_ASSERT_DEV_ENDPOINTS?.trim() === "1" || parsed.forced;

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
