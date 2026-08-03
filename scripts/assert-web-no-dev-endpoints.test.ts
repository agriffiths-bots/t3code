// @effect-diagnostics nodeBuiltinImport:off - Test drives the standalone scanner over a real temp dist tree, mirroring its node:fs CLI.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { assert, it } from "@effect/vitest";

import {
  DEFAULT_DEV_ENDPOINT_ALLOWLIST,
  scanDistForDevEndpoints,
  scanTextForDevEndpoints,
} from "./assert-web-no-dev-endpoints.ts";

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
    "ws://localhost:15773/ws",
    "wss://127.0.0.1:15773/ws",
    "http://[::1]:9229/foo",
    "http://0.0.0.0:8080/bar",
  ]) {
    const findings = scanTextForDevEndpoints(`x=${JSON.stringify(url)}`);
    assert.isAtLeast(findings.length, 1, `expected a finding for ${url}`);
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

it("allows the known UI placeholder loopback origins", () => {
  const text = `placeholder="http://localhost:5173" other="http://127.0.0.1:4096"`;
  assert.deepEqual(scanTextForDevEndpoints(text), []);
  // Sanity: the allowlist is exactly those two origins.
  assert.deepEqual([...DEFAULT_DEV_ENDPOINT_ALLOWLIST].sort(), [
    "http://127.0.0.1:4096",
    "http://localhost:5173",
  ]);
});

it("flags backend paths, queries, and fragments on allowlisted placeholder origins", () => {
  for (const url of [
    "http://localhost:5173/api",
    "http://localhost:5173?token=secret",
    "http://127.0.0.1:4096#backend",
  ]) {
    const findings = scanTextForDevEndpoints(`fetch(${JSON.stringify(url)})`);
    assert.isAtLeast(findings.length, 1, `expected a finding for ${url}`);
  }
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

it("still flags an allowlisted origin when used over ws", () => {
  // A placeholder host is only allowlisted for http/https; a ws backend to it
  // is never legitimate.
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
