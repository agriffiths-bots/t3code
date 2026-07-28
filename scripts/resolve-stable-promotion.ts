#!/usr/bin/env node

// @effect-diagnostics nodeBuiltinImport:off - Standalone release metadata bootstrap runs before an Effect runtime exists.
import * as NodeFS from "node:fs";

export interface StableVersion {
  readonly major: bigint;
  readonly minor: bigint;
  readonly patch: bigint;
}

export interface StablePromotionPlan {
  readonly latestTag: string;
  readonly nextTag: string;
  readonly nextVersion: string;
}

const StableTagPattern = /^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;

export function parseStableTag(tag: string): StableVersion | undefined {
  const match = StableTagPattern.exec(tag);
  if (!match) {
    return undefined;
  }
  const [, major, minor, patch] = match;
  if (major === undefined || minor === undefined || patch === undefined) {
    return undefined;
  }

  return {
    major: BigInt(major),
    minor: BigInt(minor),
    patch: BigInt(patch),
  };
}

function compareStableVersions(left: StableVersion, right: StableVersion): number {
  for (const key of ["major", "minor", "patch"] as const) {
    if (left[key] < right[key]) return -1;
    if (left[key] > right[key]) return 1;
  }
  return 0;
}

export function resolveStablePromotionPlan(
  tags: ReadonlyArray<string>,
  expectedLatestTag?: string,
): StablePromotionPlan {
  const stableTags = tags.flatMap((tag) => {
    const trimmed = tag.trim();
    const version = parseStableTag(trimmed);
    return version ? [{ tag: trimmed, version }] : [];
  });
  stableTags.sort((left, right) => compareStableVersions(right.version, left.version));

  const latest = stableTags[0];
  if (!latest) {
    throw new Error(
      "No plain-semver stable tag exists; refusing to invent an initial release line.",
    );
  }
  if (expectedLatestTag !== undefined && parseStableTag(expectedLatestTag) === undefined) {
    throw new Error(`Expected latest stable tag '${expectedLatestTag}' is not plain semver.`);
  }
  if (expectedLatestTag !== undefined && latest.tag !== expectedLatestTag) {
    throw new Error(
      `Stable tag changed during nightly verification: expected ${expectedLatestTag}, found ${latest.tag}. Refusing to auto-tag over a manual release.`,
    );
  }

  const nextVersion = `${latest.version.major}.${latest.version.minor}.${latest.version.patch + 1n}`;
  return {
    latestTag: latest.tag,
    nextTag: `v${nextVersion}`,
    nextVersion,
  };
}

function parseArguments(args: ReadonlyArray<string>) {
  let tagsFile: string | undefined;
  let expectedLatestTag: string | undefined;
  let githubOutput = false;

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--tags-file") {
      tagsFile = args[index + 1];
      index += 1;
    } else if (argument === "--expected-latest") {
      expectedLatestTag = args[index + 1];
      index += 1;
    } else if (argument === "--github-output") {
      githubOutput = true;
    } else {
      throw new Error(`Unknown argument '${argument}'.`);
    }
  }

  if (!tagsFile) {
    throw new Error("--tags-file is required.");
  }
  return { tagsFile, expectedLatestTag, githubOutput };
}

function serializePlan(plan: StablePromotionPlan) {
  return [
    `latest_tag=${plan.latestTag}`,
    `next_tag=${plan.nextTag}`,
    `next_version=${plan.nextVersion}`,
    "",
  ].join("\n");
}

function main() {
  const options = parseArguments(process.argv.slice(2));
  const tags = NodeFS.readFileSync(options.tagsFile, "utf8").split(/\r?\n/);
  const plan = resolveStablePromotionPlan(tags, options.expectedLatestTag);
  const output = serializePlan(plan);

  if (options.githubOutput) {
    const outputPath = process.env.GITHUB_OUTPUT;
    if (!outputPath) {
      throw new Error("GITHUB_OUTPUT is required with --github-output.");
    }
    NodeFS.appendFileSync(outputPath, output, "utf8");
  } else {
    process.stdout.write(output);
  }
}

if (import.meta.main) {
  main();
}
