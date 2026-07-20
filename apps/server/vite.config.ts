import "vite-plus/test/config";
import { normalizeBuildVersion } from "@t3tools/shared/semver";
import { defineConfig, mergeConfig } from "vite-plus";

import baseConfig from "../../vite.config.ts";
import {
  packageNameMatchesPrefix,
  resolveBarePackageName,
} from "../../scripts/lib/package-names.ts";
import { loadRepoEnv } from "../../scripts/lib/public-config.ts";
import packageJson from "./package.json" with { type: "json" };

const externalRuntimePackageNames = new Set([
  "@effect/platform-bun",
  "@effect/sql-sqlite-bun",
  "@ff-labs/fff-node",
  "ffi-rs",
  "node-pty",
]);
const externalRuntimePackagePrefixes = [
  "@anthropic-ai/claude-agent-sdk-",
  "@ff-labs/fff-bin-",
  "@msgpackr-extract/",
  "@yuuang/ffi-rs-",
] as const;

export function shouldBundleCliDependency(id: string): boolean {
  const packageName = resolveBarePackageName(id);
  if (packageName === undefined) return false;
  if (externalRuntimePackageNames.has(packageName)) return false;
  if (packageNameMatchesPrefix(packageName, externalRuntimePackagePrefixes)) {
    return false;
  }
  return true;
}

const repoEnv = loadRepoEnv();
const cliBuildChannel = packageJson.version.includes("-nightly.") ? "nightly" : "latest";

function resolveBuildSha(): string {
  const envSha = process.env.T3CODE_BUILD_SHA?.trim() ?? "";
  return /^[0-9a-f]{40}$/i.test(envSha) ? envSha.toLowerCase() : "";
}

const buildSha = resolveBuildSha();
const buildVersion = normalizeBuildVersion(process.env.T3CODE_BUILD_VERSION) ?? "";

export default mergeConfig(
  baseConfig,
  defineConfig({
    run: {
      tasks: {
        build: {
          command: "node scripts/cli.ts build",
          dependsOn: ["@t3tools/web#build"],
          cache: false,
        },
      },
    },
    pack: {
      entry: ["src/bin.ts"],
      outDir: "dist",
      sourcemap: true,
      clean: true,
      deps: {
        alwaysBundle: shouldBundleCliDependency,
        onlyBundle: false,
      },
      banner: {
        js: "#!/usr/bin/env node\n",
      },
      define: {
        __T3CODE_BUILD_SHA__: JSON.stringify(buildSha),
        __T3CODE_BUILD_VERSION__: JSON.stringify(buildVersion),
        __T3CODE_BUILD_CHANNEL__: JSON.stringify(cliBuildChannel),
        __T3CODE_BUILD_RELAY_URL__: JSON.stringify(repoEnv.T3CODE_RELAY_URL?.trim() ?? ""),
        __T3CODE_BUILD_CLERK_PUBLISHABLE_KEY__: JSON.stringify(
          repoEnv.T3CODE_CLERK_PUBLISHABLE_KEY?.trim() ?? "",
        ),
        __T3CODE_BUILD_CLERK_CLI_OAUTH_CLIENT_ID__: JSON.stringify(
          repoEnv.T3CODE_CLERK_CLI_OAUTH_CLIENT_ID?.trim() ?? "",
        ),
        __T3CODE_BUILD_RELAY_CLIENT_OTLP_TRACES_URL__: JSON.stringify(
          repoEnv.T3CODE_RELAY_CLIENT_OTLP_TRACES_URL?.trim() ?? "",
        ),
        __T3CODE_BUILD_RELAY_CLIENT_OTLP_TRACES_DATASET__: JSON.stringify(
          repoEnv.T3CODE_RELAY_CLIENT_OTLP_TRACES_DATASET?.trim() ?? "",
        ),
        __T3CODE_BUILD_RELAY_CLIENT_OTLP_TRACES_TOKEN__: JSON.stringify(
          repoEnv.T3CODE_RELAY_CLIENT_OTLP_TRACES_TOKEN?.trim() ?? "",
        ),
      },
    },
    test: {
      // The server suite exercises sqlite, git, temp worktrees, and orchestration
      // runtimes heavily. Running files in parallel introduces load-sensitive flakes.
      fileParallelism: false,
      // Server integration tests exercise sqlite, git, and orchestration together.
      // Under package-wide runs they can exceed the default budget on loaded CI hosts.
      hookTimeout: 120_000,
      testTimeout: 120_000,
    },
  }),
);
