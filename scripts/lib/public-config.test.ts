// @effect-diagnostics nodeBuiltinImport:off - Tests exercise root env file precedence directly.
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import { DEFAULT_PUBLIC_CONFIG, loadRepoEnv, resolvePublicConfig } from "./public-config.ts";

const temporaryDirectories: string[] = [];

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe("loadRepoEnv", () => {
  it("projects checked-in public defaults into Vite and Expo aliases", () => {
    const env = loadRepoEnv({ baseEnv: {}, repoRoot: makeTemporaryDirectory() });

    expect(env.T3CODE_CLERK_PUBLISHABLE_KEY).toBe(DEFAULT_PUBLIC_CONFIG.clerkPublishableKey);
    expect(env.VITE_CLERK_PUBLISHABLE_KEY).toBe(DEFAULT_PUBLIC_CONFIG.clerkPublishableKey);
    expect(env.EXPO_PUBLIC_CLERK_PUBLISHABLE_KEY).toBe(DEFAULT_PUBLIC_CONFIG.clerkPublishableKey);
    expect(env.T3_RELAY_URL).toBe(DEFAULT_PUBLIC_CONFIG.relayUrl);
    expect(env.VITE_T3_RELAY_URL).toBe(DEFAULT_PUBLIC_CONFIG.relayUrl);
  });

  it("applies process, root local, root, and checked-in precedence in that order", () => {
    const repoRoot = makeTemporaryDirectory();
    writeFileSync(
      join(repoRoot, ".env"),
      "T3CODE_CLERK_PUBLISHABLE_KEY=pk_root\nT3_RELAY_URL=https://root.example.test\n",
    );
    writeFileSync(
      join(repoRoot, ".env.local"),
      "T3CODE_CLERK_PUBLISHABLE_KEY=pk_local\nT3_RELAY_URL=https://local.example.test\n",
    );

    expect(loadRepoEnv({ baseEnv: {}, repoRoot }).T3_RELAY_URL).toBe("https://local.example.test");
    expect(
      loadRepoEnv({
        baseEnv: {
          T3CODE_CLERK_PUBLISHABLE_KEY: "pk_ci",
          T3_RELAY_URL: "https://ci.example.test",
        },
        repoRoot,
      }),
    ).toMatchObject({
      T3CODE_CLERK_PUBLISHABLE_KEY: "pk_ci",
      VITE_CLERK_PUBLISHABLE_KEY: "pk_ci",
      EXPO_PUBLIC_CLERK_PUBLISHABLE_KEY: "pk_ci",
      T3_RELAY_URL: "https://ci.example.test",
      VITE_T3_RELAY_URL: "https://ci.example.test",
    });
  });

  it("accepts legacy framework aliases as root overrides", () => {
    expect(
      resolvePublicConfig({
        VITE_CLERK_PUBLISHABLE_KEY: "pk_legacy",
        VITE_T3_RELAY_URL: "https://legacy.example.test",
      }),
    ).toEqual({
      clerkPublishableKey: "pk_legacy",
      relayUrl: "https://legacy.example.test",
    });
  });
});

function makeTemporaryDirectory() {
  const directory = mkdtempSync(join(tmpdir(), "t3code-public-config-"));
  temporaryDirectories.push(directory);
  return directory;
}
