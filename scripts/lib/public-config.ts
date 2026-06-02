// @effect-diagnostics nodeBuiltinImport:off - Build bootstrap reads optional root env files before an Effect runtime exists.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeUtil from "node:util";

import { DEFAULT_T3_CLERK_PUBLISHABLE_KEY, DEFAULT_T3_RELAY_URL } from "@t3tools/shared/relayAuth";

export interface T3CodePublicConfig {
  readonly clerkPublishableKey: string;
  readonly relayUrl: string;
}

type Environment = Readonly<Record<string, string | undefined>>;

const REPO_ROOT = NodePath.dirname(
  NodePath.dirname(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url))),
);

// These values are intentionally public. Client builds embed them so a fresh clone can use
// T3 Cloud without local setup. Use root .env files or CI environment variables to override them.
export const DEFAULT_PUBLIC_CONFIG: T3CodePublicConfig = {
  clerkPublishableKey: DEFAULT_T3_CLERK_PUBLISHABLE_KEY,
  relayUrl: DEFAULT_T3_RELAY_URL,
};

export function loadRepoEnv({
  baseEnv = process.env,
  repoRoot = REPO_ROOT,
}: {
  readonly baseEnv?: Environment;
  readonly repoRoot?: string;
} = {}): Record<string, string | undefined> {
  const rootEnv = readEnvFile(NodePath.join(repoRoot, ".env"));
  const localEnv = readEnvFile(NodePath.join(repoRoot, ".env.local"));
  const config = resolvePublicConfig(baseEnv, localEnv, rootEnv);

  return {
    ...rootEnv,
    ...localEnv,
    ...baseEnv,
    T3CODE_CLERK_PUBLISHABLE_KEY: config.clerkPublishableKey,
    T3_RELAY_URL: config.relayUrl,
    VITE_CLERK_PUBLISHABLE_KEY: config.clerkPublishableKey,
    VITE_T3_RELAY_URL: config.relayUrl,
    EXPO_PUBLIC_CLERK_PUBLISHABLE_KEY: config.clerkPublishableKey,
  };
}

export function resolvePublicConfig(...sources: readonly Environment[]): T3CodePublicConfig {
  return {
    clerkPublishableKey:
      firstNonEmpty(
        sources,
        "T3CODE_CLERK_PUBLISHABLE_KEY",
        "VITE_CLERK_PUBLISHABLE_KEY",
        "EXPO_PUBLIC_CLERK_PUBLISHABLE_KEY",
      ) ?? DEFAULT_PUBLIC_CONFIG.clerkPublishableKey,
    relayUrl:
      firstNonEmpty(sources, "T3_RELAY_URL", "VITE_T3_RELAY_URL") ?? DEFAULT_PUBLIC_CONFIG.relayUrl,
  };
}

function firstNonEmpty(sources: readonly Environment[], ...names: readonly string[]) {
  for (const source of sources) {
    for (const name of names) {
      const value = source[name]?.trim();
      if (value) {
        return value;
      }
    }
  }
  return undefined;
}

function readEnvFile(path: string): Record<string, string | undefined> {
  return NodeFS.existsSync(path) ? NodeUtil.parseEnv(NodeFS.readFileSync(path, "utf8")) : {};
}
