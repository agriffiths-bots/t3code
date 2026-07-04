#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off - GitHub release pointer publishing is a gh/git CLI adapter.
// @effect-diagnostics globalDate:off - The pointer schema stores an ISO verification timestamp.

import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeUtil from "node:util";

const execFile = NodeUtil.promisify(NodeChildProcess.execFile);

export const POINTER_BRANCH = "client-verified-latest";
export const POINTER_PATH = "client-verified-latest.json";
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
  readonly draft?: boolean;
  readonly assets: ReadonlyArray<GitHubAsset>;
}

export interface GitHubCheckRun {
  readonly name: string;
  readonly conclusion: string | null;
  readonly completed_at?: string | null;
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

function findWindowsAssets(release: GitHubRelease) {
  return {
    installer: findAsset(release, /\.exe$/i),
    blockmap: findAsset(release, /\.blockmap$/i),
    manifest: findAsset(release, /^(?:latest|nightly)\.ya?ml$/i),
  };
}

function releaseMatchesSha(release: GitHubRelease, sha: string) {
  return release.target_commitish.toLowerCase().startsWith(sha.toLowerCase());
}

function checkCompletedTime(check: GitHubCheckRun) {
  const time = Date.parse(check.completed_at ?? "");
  return Number.isFinite(time) ? time : 0;
}

function findCheck(
  checkRuns: ReadonlyArray<GitHubCheckRun>,
  expectedName: (typeof REQUIRED_SMOKE_CHECKS)[number],
): GitHubCheckRun | undefined {
  const matches = checkRuns.filter(
    (check) => check.name === expectedName || check.name.endsWith(` / ${expectedName}`),
  );
  return matches
    .filter((check) => check.conclusion !== "skipped")
    .toSorted((left, right) => checkCompletedTime(right) - checkCompletedTime(left))[0];
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

  const matchingReleases = releases.filter(
    (candidate) =>
      !candidate.draft &&
      candidate.tag_name !== POINTER_BRANCH &&
      releaseMatchesSha(candidate, sha),
  );
  const release = matchingReleases.find((candidate) => {
    const assets = findWindowsAssets(candidate);
    return assets.installer && assets.blockmap && assets.manifest;
  });
  if (!release) {
    if (matchingReleases.length === 0) {
      return { verified: false, reason: `missing release targeting ${sha}` };
    }
    const assets = findWindowsAssets(matchingReleases[0]!);
    const missingAssets = [
      ["windows installer", assets.installer],
      ["windows blockmap", assets.blockmap],
      ["windows update manifest", assets.manifest],
    ].flatMap(([label, asset]) => (asset ? [] : [label]));
    return { verified: false, reason: `missing desktop artifacts: ${missingAssets.join(", ")}` };
  }

  const {
    installer: windowsInstaller,
    blockmap: windowsBlockmap,
    manifest: windowsManifest,
  } = findWindowsAssets(release);
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
      sha: release.target_commitish,
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

async function ghJsonPages<T>(repo: string, path: string): Promise<ReadonlyArray<T>> {
  return JSON.parse(
    await run("gh", ["api", "--paginate", "--slurp", `/repos/${repo}${path}`]),
  ) as ReadonlyArray<T>;
}

async function ghJsonOptional<T>(repo: string, path: string): Promise<T | undefined> {
  try {
    return await ghJson<T>(repo, path);
  } catch {
    return undefined;
  }
}

async function resolveRepo() {
  return run("gh", ["repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"]);
}

async function resolveOriginMainSha() {
  await run("git", ["fetch", "--quiet", "origin", "main:refs/remotes/origin/main"]);
  return run("git", ["rev-parse", "origin/main"]);
}

async function resolveShaArg(shaArg: string) {
  const sha = shaArg.trim();
  if (SHA_PATTERN.test(sha)) {
    return sha;
  }
  return run("git", ["rev-parse", shaArg]);
}

function makeGhVerificationClient(repo: string): VerificationClient {
  return {
    listReleases: () => ghJson<ReadonlyArray<GitHubRelease>>(repo, "/releases?per_page=100"),
    listCheckRuns: async (sha) => {
      const pages = await ghJsonPages<{ readonly check_runs: ReadonlyArray<GitHubCheckRun> }>(
        repo,
        `/commits/${sha}/check-runs?per_page=100&filter=all`,
      );
      return pages.flatMap((page) => page.check_runs);
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
      const branch = await ghJsonOptional<{ readonly ref: string }>(
        repo,
        `/git/ref/heads/${POINTER_BRANCH}`,
      );
      if (!branch) {
        await run("gh", [
          "api",
          `/repos/${repo}/git/refs`,
          "-X",
          "POST",
          "-f",
          `ref=refs/heads/${POINTER_BRANCH}`,
          "-f",
          `sha=${pointer.sha}`,
        ]);
      }
      const existing = await ghJsonOptional<{ readonly sha: string }>(
        repo,
        `/contents/${POINTER_PATH}?ref=${POINTER_BRANCH}`,
      );
      const args = [
        "api",
        `/repos/${repo}/contents/${POINTER_PATH}`,
        "-X",
        "PUT",
        "-f",
        `message=Update verified client pointer to ${pointer.sha}`,
        "-f",
        `content=${Buffer.from(`${JSON.stringify(pointer, null, 2)}\n`).toString("base64")}`,
        "-f",
        `branch=${POINTER_BRANCH}`,
      ];
      if (existing) {
        args.push("-f", `sha=${existing.sha}`);
      }
      await run("gh", args);
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
  const sha = args.sha ? await resolveShaArg(args.sha) : await resolveOriginMainSha();
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
