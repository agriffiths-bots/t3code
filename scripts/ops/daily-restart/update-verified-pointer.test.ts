// @effect-diagnostics nodeBuiltinImport:off - Tests assert local file writes and non-regression behavior.
// @effect-diagnostics globalDate:off - Tests pin pointer timestamps deterministically.
import { assert, it } from "@effect/vitest";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  updateVerifiedPointer,
  verifyClientArtifacts,
  type GitHubCheckRun,
  type GitHubRelease,
  type PublishClient,
  type VerificationClient,
} from "./update-verified-pointer.ts";

const sha = "0123456789abcdef0123456789abcdef01234567";
const asset = (name: string) => ({
  name,
  browser_download_url: `https://downloads.example/${name}`,
});
const fullAssets = [
  asset("T3-Code-1.0.0-x64.exe"),
  asset("T3-Code-1.0.0-x64.exe.blockmap"),
  asset("latest.yml"),
];
const release = (assets = fullAssets): GitHubRelease => ({
  tag_name: "main-20260703010101-0123456",
  html_url: "https://github.com/agriffiths-bots/t3code/releases/tag/main-20260703010101-0123456",
  target_commitish: sha,
  assets,
});
const checks = (
  overrides: Partial<Record<string, string | null>> = {},
): ReadonlyArray<GitHubCheckRun> => [
  {
    name: "Build and publish artifacts / Windows Launch Smoke",
    conclusion: overrides["Windows Launch Smoke"] ?? "success",
    html_url: "https://github.com/checks/windows",
  },
  {
    name: "Desktop Linux Package Smoke",
    conclusion: overrides["Desktop Linux Package Smoke"] ?? "skipped",
    html_url: "https://github.com/checks/linux",
  },
];
const client = (options?: {
  readonly releases?: ReadonlyArray<GitHubRelease>;
  readonly checkRuns?: ReadonlyArray<GitHubCheckRun>;
}): VerificationClient => ({
  listReleases: async () => options?.releases ?? [release()],
  listCheckRuns: async () => options?.checkRuns ?? checks(),
});

it("verifies a sha with desktop assets and launch smoke, ignoring the pointer release", async () => {
  const result = await verifyClientArtifacts({
    sha,
    client: client({
      releases: [
        { ...release([asset("client-verified-latest.json")]), tag_name: "client-verified-latest" },
        release(),
      ],
    }),
    now: new Date("2026-07-03T12:00:00.000Z"),
  });

  assert.equal(result.verified, true);
  if (!result.verified) return;
  assert.equal(result.pointer.sha, sha);
  assert.equal(result.pointer.desktop.launch_smoke, "success");
  assert.equal(
    result.pointer.desktop.artifacts.windows.installer,
    "https://downloads.example/T3-Code-1.0.0-x64.exe",
  );
  assert.equal(result.pointer.desktop.linux.ready, "unknown");
  assert.equal(result.pointer.mobile.ready, "unknown");
  assert.equal(result.pointer.mobile.ota, "unknown");
  assert.equal(result.pointer.mobile.apk, undefined);
  assert.match(result.pointer.mobile.reason, /No Expo OTA publish workflow/);
});

it("rejects smoke-failed and artifact-missing shas", async () => {
  const smokeFailed = await verifyClientArtifacts({
    sha,
    client: client({ checkRuns: checks({ "Windows Launch Smoke": "failure" }) }),
  });
  const artifactsMissing = await verifyClientArtifacts({
    sha,
    client: client({
      releases: [release([asset("T3-Code-1.0.0-x64.exe"), asset("latest.yml")])],
    }),
  });

  assert.deepStrictEqual(smokeFailed, {
    verified: false,
    reason: "Windows Launch Smoke concluded failure",
  });
  assert.deepStrictEqual(artifactsMissing, {
    verified: false,
    reason: "missing desktop artifacts: windows blockmap",
  });
});

it("does not move or overwrite the pointer on verification failure", async () => {
  const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "verified-pointer-test-"));
  const outFile = NodePath.join(tempDir, "pointer.json");
  NodeFS.writeFileSync(outFile, "previous pointer\n");
  let published = false;
  const publishClient: PublishClient = {
    assertWriteIdentity: async () => {
      published = true;
    },
    publishPointer: async () => {
      published = true;
    },
  };

  try {
    const result = await updateVerifiedPointer({
      sha,
      verificationClient: client({
        checkRuns: checks({ "Windows Launch Smoke": "cancelled" }),
      }),
      publishClient,
      outFile,
    });

    assert.deepStrictEqual(result, {
      verified: false,
      reason: "Windows Launch Smoke concluded cancelled",
    });
    assert.equal(NodeFS.readFileSync(outFile, "utf8"), "previous pointer\n");
    assert.equal(published, false);
  } finally {
    NodeFS.rmSync(tempDir, { recursive: true, force: true });
  }
});
