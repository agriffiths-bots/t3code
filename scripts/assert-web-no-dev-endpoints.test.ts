// @effect-diagnostics nodeBuiltinImport:off - Test drives the standalone scanner over a real temp dist tree, mirroring its node:fs CLI.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { assert, expect, it } from "@effect/vitest";
import {
  assertDistHasNoDevEndpoints,
  parseScannerCliArgs,
  scanDistForDevEndpoints,
  scanTextForDevEndpoints,
} from "./assert-web-no-dev-endpoints.ts";
import {
  HOSTED_DISPLAY_URL_ALLOWLIST,
  hostedDisplayUrlMarker,
} from "./hosted-display-url-allowlist.ts";

const HOSTED_DISPLAY_URL_MARKERS = HOSTED_DISPLAY_URL_ALLOWLIST.map((entry) =>
  hostedDisplayUrlMarker(entry.sourceFile, entry.url),
);

async function createHostedBuildFixture(
  assetText: string,
  missingProvenanceUrl?: string,
): Promise<{ readonly repoRoot: string; readonly distDir: string }> {
  const repoRoot = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-hosted-build-"));
  const distDir = NodePath.join(repoRoot, "apps", "web", "dist");
  const assetFile = NodePath.join(distDir, "assets", "index.js");
  await NodeFSP.mkdir(NodePath.dirname(assetFile), { recursive: true });
  await NodeFSP.writeFile(assetFile, assetText, "utf8");

  for (const entry of HOSTED_DISPLAY_URL_ALLOWLIST) {
    const sourceFile = NodePath.join(repoRoot, entry.sourceFile);
    const sourceText =
      entry.url === missingProvenanceUrl
        ? "display copy removed"
        : JSON.stringify(hostedDisplayUrlMarker(entry.sourceFile, entry.url));
    await NodeFSP.mkdir(NodePath.dirname(sourceFile), { recursive: true });
    await NodeFSP.writeFile(sourceFile, sourceText, "utf8");
  }

  return { repoRoot, distDir };
}

it("flags the 2026-07-22 contamination in any chunk", () => {
  const findings = scanTextForDevEndpoints(
    `fetch("http://127.0.0.1:15773/api/auth/session");new WebSocket("ws://127.0.0.1:15773/ws")`,
  );
  assert.isAtLeast(findings.length, 2);
  assert.isTrue(findings.some((finding) => finding.reason.includes("WebSocket")));
  assert.isTrue(findings.some((finding) => finding.reason.includes("loopback backend URL")));
});

it("flags ported loopback backend URLs across schemes and host forms", () => {
  for (const url of [
    "https://localhost:3773/api",
    "backend:http://localhost:3773/api",
    "prefix:wss://127.0.0.2:3773/ws",
    "ws://localhost:15773/ws",
    "wss://127.0.0.1:15773/ws",
    "http://127.0.0.0:3773/api",
    "http://127.0.0.2:3773/api",
    "ws://127.1.2.3:3773/ws",
    "http://127.255.255.255:3773/api",
    "http://127.1:3773/api",
    "http://2130706433:3773/api",
    "http://[::ffff:127.0.0.1]:3773/api",
    "ws://[::ffff:127.255.255.255]:3773/ws",
    "http://[::ffff:0.0.0.0]:3773/api",
    "http://[::]:3773/api",
    "http://[::1]:9229/foo",
    "http://0.0.0.0:8080/bar",
    "http://dev.localhost:8080/bar",
    "//localhost:3773/api",
    "//127.0.0.2:3773/api",
    "//[::1]:8443/session",
    String.raw`http:localhost:3773/api`,
    String.raw`http:/localhost:3773/api`,
    String.raw`http:\\localhost:3773/api`,
    String.raw`wss:\\\\127.0.0.2:3773/ws`,
    String.raw`\\\\localhost:3773/api`,
    String.raw`/\\localhost:3773/api`,
  ]) {
    const findings = scanTextForDevEndpoints(`x=${JSON.stringify(url)}`);
    assert.isAtLeast(findings.length, 1, `expected a finding for ${url}`);
  }
});

it("flags loopback URLs hidden behind userinfo without reporting credentials", () => {
  for (const url of [
    "http://user:pass@localhost:3773/api",
    "http://user:p@ss@localhost:3773/api",
    "//user:p@ss@localhost:3773/api",
    "https://token@[::1]:8443/session",
  ]) {
    const findings = scanTextForDevEndpoints(`x=${JSON.stringify(url)}`);
    assert.isAtLeast(findings.length, 1, `expected a finding for ${url}`);
    assert.notInclude(findings[0]?.match ?? "", "@");
    assert.notInclude(findings[0]?.match ?? "", "user");
    assert.notInclude(findings[0]?.match ?? "", "pass");
    assert.notInclude(findings[0]?.match ?? "", "token");
  }
});

it("does NOT flag bare loopback origins (library URL-base defaults)", () => {
  // Regression: a vendored library ships `this.origin=\`http://localhost\``
  // as a default base. A hosted build legitimately contains this; the
  // contamination signal is a loopback backend WITH a port.
  for (const text of [
    "this.origin=`http://localhost`",
    'new URL(path, "http://localhost")',
    'base="https://127.0.0.1"',
    'ws="ws://localhost"',
  ]) {
    assert.deepEqual(scanTextForDevEndpoints(text), [], `unexpected finding in: ${text}`);
  }
});

it("does not globally allowlist loopback origins", () => {
  for (const url of [
    "http://localhost:5173",
    "http://localhost:5173/api",
    "http://localhost:5173?token=secret",
    "http://127.0.0.1:4096",
    "http://127.0.0.1:4096#backend",
  ]) {
    const findings = scanTextForDevEndpoints(`fetch(${JSON.stringify(url)})`);
    assert.isAtLeast(findings.length, 1, `expected a finding for ${url}`);
  }
});

it("parses --force independently of the positional dist directory", () => {
  const conventional = parseScannerCliArgs(["--force", "apps/web/dist"]);
  const trailing = parseScannerCliArgs(["apps/web/dist", "--force"]);
  assert.isTrue(conventional.forced);
  assert.equal(conventional.distDir, trailing.distDir);
  const staged = parseScannerCliArgs([
    "--force",
    "apps/web/dist",
    "--repo-root",
    "/tmp/staged-checkout",
  ]);
  assert.equal(staged.repoRoot, NodePath.resolve("/tmp/staged-checkout"));
  assert.throws(() => parseScannerCliArgs(["--repo-root"]));
  assert.throws(() => parseScannerCliArgs(["--unknown", "apps/web/dist"]));
  assert.throws(() => parseScannerCliArgs(["first", "second"]));
});

it("does not expose surrounding bundle text in scanner findings", () => {
  const secret = "adjacent-bundle-secret";
  const findings = scanTextForDevEndpoints(
    `const token="${secret}";fetch("http://localhost:9999/api")`,
  );
  assert.equal(findings[0]?.match, "http://localhost:9999");
  assert.notInclude(findings[0]?.match ?? "", secret);
});

it("does not flag bare loopback hostnames used for runtime comparison", () => {
  // e.g. LOOPBACK_HOSTNAMES = new Set(["127.0.0.1","::1","localhost"])
  const text = `const LOOPBACK=new Set(["127.0.0.1","::1","localhost","0.0.0.0"]);`;
  assert.deepEqual(scanTextForDevEndpoints(text), []);
});

it("flags a loopback WebSocket backend", () => {
  const findings = scanTextForDevEndpoints(`new WebSocket("ws://localhost:5173/ws")`);
  assert.isAtLeast(findings.length, 1);
});

it("scans every asset in a dist tree and reports contaminated files", async () => {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devendpoint-scan-"));
  try {
    await NodeFSP.writeFile(NodePath.join(dir, "index-CLEAN.js"), 'const x="ok";', "utf8");
    await NodeFSP.mkdir(NodePath.join(dir, "assets"), { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(dir, "assets", "textarea-BAD.js"),
      'fetch("http://127.0.0.1:15773/api/auth/session")',
      "utf8",
    );
    // Sourcemaps and images must be skipped even if they contain the string.
    await NodeFSP.writeFile(
      NodePath.join(dir, "assets", "textarea-BAD.js.map"),
      '{"sources":["http://127.0.0.1:15773"]}',
      "utf8",
    );

    const results = await scanDistForDevEndpoints(dir);
    assert.equal(results.length, 1);
    assert.isTrue(results[0]!.file.endsWith("textarea-BAD.js"));
  } finally {
    await NodeFSP.rm(dir, { recursive: true, force: true });
  }
});

it("does not flag ported public backend URLs", () => {
  for (const url of [
    "https://example.com:3773/api",
    "ftp://localhost:3773/api",
    String.raw`ftp:\\\\localhost:3773/api`,
    "http://128.0.0.1:3773/api",
    "http://0.0.1.1:3773/api",
    "http://[::ffff:0.0.1.1]:3773/api",
    "http://[::ffff:128.0.0.1]:3773/api",
    "http://127.example.com:3773/api",
  ]) {
    assert.deepEqual(scanTextForDevEndpoints(url), []);
  }
});

it("passes a scrubbed hosted build containing the audited display copy", async () => {
  const assetText = `const displayCopy=${JSON.stringify(HOSTED_DISPLAY_URL_MARKERS)};`;
  const fixture = await createHostedBuildFixture(assetText);
  try {
    await assertDistHasNoDevEndpoints(fixture.distDir, fixture.repoRoot);
  } finally {
    await NodeFSP.rm(fixture.repoRoot, { recursive: true, force: true });
  }
});

it("still rejects a VITE_HTTP_URL-style backend endpoint", async () => {
  const assetText = [
    `const displayCopy=${JSON.stringify(HOSTED_DISPLAY_URL_MARKERS)};`,
    'const VITE_HTTP_URL="http://127.0.0.1:15773";fetch(VITE_HTTP_URL);',
  ].join("\n");
  const fixture = await createHostedBuildFixture(assetText);
  try {
    await expect(assertDistHasNoDevEndpoints(fixture.distDir, fixture.repoRoot)).rejects.toThrow(
      /http:\/\/127\.0\.0\.1:15773/,
    );
  } finally {
    await NodeFSP.rm(fixture.repoRoot, { recursive: true, force: true });
  }
});

it.each(HOSTED_DISPLAY_URL_ALLOWLIST.map((entry) => [entry.url] as const))(
  "rejects a VITE_HTTP_URL endpoint identical to audited display copy: %s",
  async (url) => {
    const fixture = await createHostedBuildFixture(
      [
        `const displayCopy=${JSON.stringify(HOSTED_DISPLAY_URL_MARKERS)};`,
        `const VITE_HTTP_URL=${JSON.stringify(url)};fetch(VITE_HTTP_URL);`,
      ].join("\n"),
    );
    try {
      await expect(assertDistHasNoDevEndpoints(fixture.distDir, fixture.repoRoot)).rejects.toThrow(
        /hosted bundle contains dev\/loopback backend endpoints/,
      );
    } finally {
      await NodeFSP.rm(fixture.repoRoot, { recursive: true, force: true });
    }
  },
);

it("fails closed when emitted display literals are not provenance-marked", async () => {
  const assetText = `const displayCopy=${JSON.stringify(
    HOSTED_DISPLAY_URL_ALLOWLIST.map((entry) => entry.url),
  )};`;
  const fixture = await createHostedBuildFixture(assetText);
  try {
    await expect(assertDistHasNoDevEndpoints(fixture.distDir, fixture.repoRoot)).rejects.toThrow(
      /hosted bundle contains dev\/loopback backend endpoints/,
    );
  } finally {
    await NodeFSP.rm(fixture.repoRoot, { recursive: true, force: true });
  }
});

it.each([
  ["path", "http://localhost:5173/api"],
  ["query", "http://localhost:5173?token=secret"],
  ["missing audited slash", "http://127.0.0.1:5173"],
  ["path after audited slash", "http://127.0.0.1:5173/api"],
] as const)("does not let the display allowlist hide a %s endpoint", async (_case, url) => {
  const fixture = await createHostedBuildFixture(
    [
      `const displayCopy=${JSON.stringify(HOSTED_DISPLAY_URL_MARKERS)};`,
      `const VITE_HTTP_URL=${JSON.stringify(url)};`,
    ].join("\n"),
  );
  try {
    await expect(assertDistHasNoDevEndpoints(fixture.distDir, fixture.repoRoot)).rejects.toThrow(
      /hosted bundle contains dev\/loopback backend endpoints/,
    );
  } finally {
    await NodeFSP.rm(fixture.repoRoot, { recursive: true, force: true });
  }
});

it.each(HOSTED_DISPLAY_URL_ALLOWLIST.map((entry) => [entry.url] as const))(
  "fails closed when provenance for %s is missing",
  async (url) => {
    const fixture = await createHostedBuildFixture('const clean="asset";', url);
    try {
      await expect(assertDistHasNoDevEndpoints(fixture.distDir, fixture.repoRoot)).rejects.toThrow(
        /display URL allowlist provenance requires exactly one marked occurrence/,
      );
    } finally {
      await NodeFSP.rm(fixture.repoRoot, { recursive: true, force: true });
    }
  },
);
