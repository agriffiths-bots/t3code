import { describe, expect, it } from "vite-plus/test";

import { shouldBundleCliDependency } from "./vite.config";

describe("server Vite packaging", () => {
  it("bundles JavaScript runtime dependencies into the CLI/server artifact", () => {
    expect(shouldBundleCliDependency("effect/Effect")).toBe(true);
    expect(shouldBundleCliDependency("@pierre/diffs")).toBe(true);
    expect(shouldBundleCliDependency("@t3tools/shared/Net")).toBe(true);
    expect(shouldBundleCliDependency("croner")).toBe(true);
    expect(shouldBundleCliDependency("web-push")).toBe(true);
    expect(shouldBundleCliDependency("@opencode-ai/sdk/v2")).toBe(true);
  });

  it("leaves native and Bun-only runtime packages external", () => {
    expect(shouldBundleCliDependency("@effect/platform-bun/BunHttpServer")).toBe(false);
    expect(shouldBundleCliDependency("@effect/sql-sqlite-bun")).toBe(false);
    expect(shouldBundleCliDependency("@ff-labs/fff-node")).toBe(false);
    expect(shouldBundleCliDependency("@ff-labs/fff-bin-linux-x64-gnu")).toBe(false);
    expect(shouldBundleCliDependency("@anthropic-ai/claude-agent-sdk-linux-x64")).toBe(false);
    expect(shouldBundleCliDependency("@msgpackr-extract/msgpackr-extract-linux-x64")).toBe(false);
    expect(shouldBundleCliDependency("@yuuang/ffi-rs-linux-x64-gnu")).toBe(false);
    expect(shouldBundleCliDependency("ffi-rs")).toBe(false);
    expect(shouldBundleCliDependency("node-pty")).toBe(false);
    expect(shouldBundleCliDependency("node:fs")).toBe(false);
  });

  it("leaves the optional Matrix bridge packages external", () => {
    expect(shouldBundleCliDependency("matrix-bot-sdk")).toBe(false);
    expect(shouldBundleCliDependency("@matrix-org/matrix-sdk-crypto-nodejs")).toBe(false);
  });
});
