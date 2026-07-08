import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { APP_VERSION } from "./branding";
import {
  appendVersionMismatchHint,
  buildVersionMismatchDismissalKey,
  dismissVersionMismatch,
  formatVersionWithBuildSha,
  isVersionMismatchDismissed,
  resolveServerConfigVersionMismatch,
  resolveVersionMismatch,
} from "./versionSkew";

describe("versionSkew", () => {
  it("does not warn when versions match", () => {
    expect(resolveVersionMismatch(APP_VERSION)).toBeNull();
  });

  it("returns a mismatch when the server version differs from the client", () => {
    expect(resolveVersionMismatch("9.9.9")).toEqual({
      clientVersion: APP_VERSION,
      serverVersion: "9.9.9",
      hint: "Version mismatch. Try syncing the client and server to the same T3 Code version.",
    });
  });

  it("does not warn when stamped client and server builds share the same sha", () => {
    const buildSha = "d7b6e15ecd7b6e15ecd7b6e15ecd7b6e15ecd7b6";

    expect(
      resolveVersionMismatch("0.0.28", {
        clientVersion: "0.0.29-nightly.20260708.26",
        clientBuildSha: buildSha,
        serverBuildSha: buildSha,
      }),
    ).toBeNull();
  });

  it("warns when stamped client and server builds have different shas", () => {
    const clientBuildSha = "d7b6e15ecd7b6e15ecd7b6e15ecd7b6e15ecd7b6";
    const serverBuildSha = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

    expect(
      resolveVersionMismatch("0.0.29-nightly.20260708.26", {
        clientVersion: "0.0.29-nightly.20260708.26",
        clientBuildSha,
        serverBuildSha,
      }),
    ).toMatchObject({
      clientVersion: "0.0.29-nightly.20260708.26",
      serverVersion: "0.0.29-nightly.20260708.26",
      clientBuildSha,
      serverBuildSha,
    });
  });

  it("reads the server version from config descriptors", () => {
    expect(
      resolveServerConfigVersionMismatch({
        environment: {
          environmentId: EnvironmentId.make("environment-1"),
          label: "Remote",
          platform: {
            os: "darwin",
            arch: "arm64",
          },
          serverVersion: "9.9.9",
          capabilities: {
            repositoryIdentity: true,
          },
        },
      }),
    ).toMatchObject({
      serverVersion: "9.9.9",
    });
  });

  it("keys dismissals by environment, client version, and server version", () => {
    const environmentId = EnvironmentId.make("environment-dismissal");
    const key = buildVersionMismatchDismissalKey(environmentId, {
      clientVersion: APP_VERSION,
      serverVersion: "9.9.9",
    });

    expect(key).toBe(`${environmentId}:${APP_VERSION}:9.9.9`);
    expect(isVersionMismatchDismissed(key)).toBe(false);

    dismissVersionMismatch(key);

    expect(isVersionMismatchDismissed(key)).toBe(true);
    expect(
      isVersionMismatchDismissed(
        buildVersionMismatchDismissalKey(environmentId, {
          clientVersion: APP_VERSION,
          serverVersion: "9.9.10",
        }),
      ),
    ).toBe(false);
  });

  it("includes build shas in dismissal keys when present", () => {
    const environmentId = EnvironmentId.make("environment-build-dismissal");
    const clientBuildSha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const serverBuildSha = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    expect(
      buildVersionMismatchDismissalKey(environmentId, {
        clientVersion: "1.0.0",
        serverVersion: "1.0.0",
        clientBuildSha,
        serverBuildSha,
      }),
    ).toBe(`${environmentId}:1.0.0@${clientBuildSha}:1.0.0@${serverBuildSha}`);
  });

  it("formats versions with short build shas", () => {
    expect(
      formatVersionWithBuildSha(
        "0.0.29-nightly.20260708.26",
        "d7b6e15ecd7b6e15ecd7b6e15ecd7b6e15ecd7b6",
      ),
    ).toBe("0.0.29-nightly.20260708.26 (sha d7b6e15e)");
  });

  it("appends a hint to connection errors when versions differ", () => {
    const mismatch = resolveVersionMismatch("9.9.9");

    expect(appendVersionMismatchHint("Socket closed.", mismatch)).toBe(
      "Socket closed. Hint: Version mismatch. Try syncing the client and server to the same T3 Code version.",
    );
  });
});
