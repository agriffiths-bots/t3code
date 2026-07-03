#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off - GitHub release pointer publishing is a gh/git CLI adapter.
// @effect-diagnostics globalDate:off - The pointer schema stores an ISO verification timestamp.

import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";

const execFile = NodeUtil.promisify(NodeChildProcess.execFile);

export const POINTER_TAG = "client-verified-latest";
export const POINTER_ASSET = "client-verified-latest.json";
export const REQUIRED_SMOKE_CHECKS = ["Windows Launch Smoke"] as const;

const SHA_PATTERN = /^[0-9a-f]{7,40}$/i;

export interface GitHubAsset {
  readonly name: string;
  readonly browser_download_url: string;
}

export interface GitHubRelease {
  readonly tag_name: string;
  readonly html_url: string;
  readonly target_commitish: string;
  readonly assets: ReadonlyArray<GitHubAsset>;
}

export interface GitHubCheckRun {
  readonly name: string;
  readonly conclusion: string | null;
  readonly html_url?: string;
}

export interface VerifiedPointer {
  readonly version: 1;
  readonly sha: string;
  readonly verified_at: string;
  readonly desktop: {
    readonly ready: boolean;
    readonly artifacts: {
      readonly release: string;
      readonly windows: {
        readonly installer: string;
        readonly blockmap: string;
        readonly manifest: string;
      };
    };
    readonly launch_smoke: "success";
    readonly checks: Record<string, string>;
    readonly linux: {
      readonly ready: "unknown";
      readonly launch_smoke: "unknown";
      readonly artifact: "unknown";
      readonly reason: string;
    };
  };
  readonly mobile: {
    readonly ready: "unknown";
    readonly ota: "unknown";
    readonly apk?: string;
    readonly reason: string;
  };
}

export type VerificationResult =
  | { readonly verified: true; readonly pointer: VerifiedPointer }
  | { readonly verified: false; readonly reason: string };

export interface VerificationClient {
  readonly listReleases: () => Promise<ReadonlyArray<GitHubRelease>>;
  readonly listCheckRuns: (sha: string) => Promise<ReadonlyArray<GitHubCheckRun>>;
}

export interface PublishClient {
  readonly assertWriteIdentity: () => Promise<void>;
  readonly publishPointer: (pointer: VerifiedPointer) => Promise<void>;
}

function findAsset(release: GitHubRelease, pattern: RegExp): GitHubAsset | undefined {
  return release.assets.find((asset) => pattern.test(asset.name));
}

function findCheck(
  checkRuns: ReadonlyArray<GitHubCheckRun>,
  expectedName: (typeof REQUIRED_SMOKE_CHECKS)[number],
): GitHubCheckRun | undefined {
  return checkRuns.find(
    (check) => check.name === expectedName || check.name.endsWith(` / ${expectedName}`),
  );
}

function describeMissingChecks(checkRuns: ReadonlyArray<GitHubCheckRun>) {
  const missingOrFailed = REQUIRED_SMOKE_CHECKS.flatMap((name) => {
    const check = findCheck(checkRuns, name);
    if (!check) return [`missing ${name}`];
    if (check.conclusion !== "success") return [`${name} concluded ${check.conclusion ?? "null"}`];
    return [];
  });
  return missingOrFailed.length === 0 ? undefined : missingOrFailed.join("; ");
}

export async function verifyClientArtifacts(options: {
  readonly sha: string;
  readonly client: VerificationClient;
  readonly now?: Date;
}): Promise<VerificationResult> {
  const sha = options.sha.trim();
  if (!SHA_PATTERN.test(sha)) {
    return { verified: false, reason: `invalid sha ${sha}` };
  }

  const [releases, checkRuns] = await Promise.all([
    options.client.listReleases(),
    options.client.listCheckRuns(sha),
  ]);

  const checkFailure = describeMissingChecks(checkRuns);
  if (checkFailure) {
    return { verified: false, reason: checkFailure };
  }

  const release = releases
    .filter((candidate) => candidate.tag_name !== POINTER_TAG)
    .find((candidate) => candidate.target_commitish.toLowerCase() === sha.toLowerCase());
  if (!release) {
    return { verified: false, reason: `missing release targeting ${sha}` };
  }

  const windowsInstaller = findAsset(release, /\.exe$/i);
  const windowsBlockmap = findAsset(release, /\.blockmap$/i);
  const windowsManifest = findAsset(release, /\.ya?ml$/i);
  const missingAssets = [
    ["windows installer", windowsInstaller],
    ["windows blockmap", windowsBlockmap],
    ["windows update manifest", windowsManifest],
  ].flatMap(([label, asset]) => (asset ? [] : [label]));
  if (missingAssets.length > 0) {
    return { verified: false, reason: `missing desktop artifacts: ${missingAssets.join(", ")}` };
  }

  const apk = findAsset(release, /\.apk$/i);
  const checks = Object.fromEntries(
    REQUIRED_SMOKE_CHECKS.map((name) => [name, findCheck(checkRuns, name)?.html_url ?? "success"]),
  );

  return {
    verified: true,
    pointer: {
      version: 1,
      sha,
      verified_at: (options.now ?? new Date()).toISOString(),
      desktop: {
        ready: true,
        artifacts: {
          release: release.html_url,
          windows: {
            installer: windowsInstaller!.browser_download_url,
            blockmap: windowsBlockmap!.browser_download_url,
            manifest: windowsManifest!.browser_download_url,
          },
        },
        launch_smoke: "success",
        checks,
        linux: {
          ready: "unknown",
          launch_smoke: "unknown",
          artifact: "unknown",
          reason:
            "No main-queryable Linux launch-smoke check or Linux release artifact is currently published by this repository.",
        },
      },
      mobile: {
        ready: "unknown",
        ota: "unknown",
        ...(apk ? { apk: apk.browser_download_url } : {}),
        reason:
          "No Expo OTA publish workflow is currently queryable from this repository; APK release assets are reported when present.",
      },
    },
  };
}

export async function updateVerifiedPointer(options: {
  readonly sha: string;
  readonly verificationClient: VerificationClient;
  readonly publishClient?: PublishClient;
  readonly outFile?: string;
  readonly dryRun?: boolean;
  readonly now?: Date;
}): Promise<VerificationResult> {
  const result = await verifyClientArtifacts({
    sha: options.sha,
    client: options.verificationClient,
    ...(options.now ? { now: options.now } : {}),
  });
  if (!result.verified) return result;

  const serialized = `${JSON.stringify(result.pointer, null, 2)}\n`;
  if (options.outFile) {
    const tempOutFile = `${options.outFile}.${process.pid}.tmp`;
    NodeFS.writeFileSync(tempOutFile, serialized);
    NodeFS.renameSync(tempOutFile, options.outFile);
  } else if (!options.dryRun) {
    if (!options.publishClient) {
      throw new Error("publishClient is required when --out and --dry-run are not set");
    }
    await options.publishClient.assertWriteIdentity();
    await options.publishClient.publishPointer(result.pointer);
  }

  return result;
}

async function run(command: string, args: ReadonlyArray<string>) {
  const { stdout } = await execFile(command, [...args], {
    maxBuffer: 20 * 1024 * 1024,
  });
  return stdout.trim();
}

async function ghJson<T>(repo: string, path: string): Promise<T> {
  return JSON.parse(await run("gh", ["api", `/repos/${repo}${path}`])) as T;
}

async function resolveRepo() {
  return run("gh", ["repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"]);
}

async function resolveOriginMainSha() {
  return run("git", ["rev-parse", "origin/main"]);
}

function makeGhVerificationClient(repo: string): VerificationClient {
  return {
    listReleases: () => ghJson<ReadonlyArray<GitHubRelease>>(repo, "/releases?per_page=100"),
    listCheckRuns: async (sha) => {
      const response = await ghJson<{ readonly check_runs: ReadonlyArray<GitHubCheckRun> }>(
        repo,
        `/commits/${sha}/check-runs?per_page=100`,
      );
      return response.check_runs;
    },
  };
}

function makeGhPublishClient(repo: string): PublishClient {
  return {
    assertWriteIdentity: async () => {
      const status = JSON.parse(
        await run("gh", [
          "auth",
          "status",
          "--active",
          "--hostname",
          "github.com",
          "--json",
          "hosts",
        ]),
      ) as {
        readonly hosts?: Record<
          string,
          ReadonlyArray<{ readonly active?: boolean; readonly login?: string }>
        >;
      };
      const activeLogin = status.hosts?.["github.com"]?.find((entry) => entry.active)?.login;
      if (activeLogin !== "wizzoapp[bot]") {
        throw new Error("GitHub writes require gh authenticated as wizzoapp[bot]");
      }
    },
    publishPointer: async (pointer) => {
      const tempDir = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "client-verified-pointer-"),
      );
      const newAssetDir = NodePath.join(tempDir, "new");
      const previousAssetDir = NodePath.join(tempDir, "previous");
      NodeFS.mkdirSync(newAssetDir);
      NodeFS.mkdirSync(previousAssetDir);
      const assetPath = NodePath.join(newAssetDir, POINTER_ASSET);
      NodeFS.writeFileSync(assetPath, `${JSON.stringify(pointer, null, 2)}\n`);
      try {
        const releaseExists = await execFile(
          "gh",
          ["release", "view", POINTER_TAG, "--repo", repo],
          {
            maxBuffer: 1024 * 1024,
          },
        ).then(
          () => true,
          () => false,
        );
        if (!releaseExists) {
          await run("gh", [
            "release",
            "create",
            POINTER_TAG,
            assetPath,
            "--repo",
            repo,
            "--target",
            pointer.sha,
            "--prerelease",
            "--latest=false",
            "--title",
            "Latest verified client pointer",
            "--notes",
            "Machine-readable pointer for the latest launch-verified client artifacts.",
          ]);
          return;
        }
        const hadPreviousAsset = await execFile(
          "gh",
          [
            "release",
            "download",
            POINTER_TAG,
            "--repo",
            repo,
            "--pattern",
            POINTER_ASSET,
            "--dir",
            previousAssetDir,
            "--clobber",
          ],
          { maxBuffer: 1024 * 1024 },
        ).then(
          () => true,
          () => false,
        );
        const previousAssetPath = NodePath.join(previousAssetDir, POINTER_ASSET);
        try {
          await run("gh", [
            "release",
            "upload",
            POINTER_TAG,
            assetPath,
            "--repo",
            repo,
            "--clobber",
          ]);
        } catch (error) {
          if (hadPreviousAsset && NodeFS.existsSync(previousAssetPath)) {
            await run("gh", [
              "release",
              "upload",
              POINTER_TAG,
              previousAssetPath,
              "--repo",
              repo,
              "--clobber",
            ]);
          }
          throw error;
        }
      } finally {
        NodeFS.rmSync(tempDir, { recursive: true, force: true });
      }
    },
  };
}

function parseArgs(argv: ReadonlyArray<string>) {
  const parsed: { sha?: string; dryRun: boolean; outFile?: string } = { dryRun: false };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--dry-run") {
      parsed.dryRun = true;
    } else if (arg === "--sha") {
      const value = argv[++index];
      if (!value) throw new Error("--sha requires a value");
      parsed.sha = value;
    } else if (arg === "--out") {
      const value = argv[++index];
      if (!value) throw new Error("--out requires a value");
      parsed.outFile = value;
    } else {
      throw new Error(`Unknown argument ${arg}`);
    }
  }
  return parsed;
}

export async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  const repo = await resolveRepo();
  const sha = args.sha ?? (await resolveOriginMainSha());
  const result = await updateVerifiedPointer({
    sha,
    verificationClient: makeGhVerificationClient(repo),
    publishClient: makeGhPublishClient(repo),
    dryRun: args.dryRun,
    ...(args.outFile ? { outFile: args.outFile } : {}),
  });

  if (result.verified) {
    process.stdout.write(`VERIFIED ${result.pointer.sha}\n`);
    return;
  }
  process.stdout.write(`NOT-VERIFIED ${result.reason}\n`);
  process.exitCode = 1;
}

if (import.meta.main) {
  main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    process.stdout.write(`NOT-VERIFIED ${message}\n`);
    process.exitCode = 1;
  });
}
