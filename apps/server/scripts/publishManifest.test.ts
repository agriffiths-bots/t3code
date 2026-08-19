import { describe, expect, it } from "vite-plus/test";

import serverPackageJson from "../package.json" with { type: "json" };
import { createPublishManifest } from "./publishManifest.ts";

const source = {
  name: "t3",
  repository: { type: "git", url: "https://github.com/pingdotgg/t3code", directory: "apps/server" },
  bin: { t3: "./dist/bin.mjs" },
  type: "module",
  engines: { node: "^22.16 || ^23.11 || >=24.10" },
  files: ["dist"],
  dependencies: { croner: "^9.0.0", effect: "catalog:" },
  optionalDependencies: { "matrix-bot-sdk": "0.8.0", "some-catalog-package": "catalog:" },
};

const catalog = { effect: "4.0.0-beta.103", "some-catalog-package": "1.2.3" };

describe("publish manifest", () => {
  it("keeps optional dependencies and resolves their catalog specs", () => {
    const manifest = createPublishManifest({ source, version: "0.0.32", catalog, overrides: {} });

    expect(manifest.optionalDependencies).toEqual({
      "matrix-bot-sdk": "0.8.0",
      "some-catalog-package": "1.2.3",
    });
    expect(manifest.dependencies).toEqual({ croner: "^9.0.0", effect: "4.0.0-beta.103" });
    expect(manifest.version).toBe("0.0.32");
  });

  it("omits the optional dependency block when the package has none", () => {
    const { optionalDependencies: _optional, ...withoutOptional } = source;
    const manifest = createPublishManifest({
      source: withoutOptional,
      version: "0.0.32",
      catalog,
      overrides: {},
    });

    expect(manifest).not.toHaveProperty("optionalDependencies");
  });

  it("publishes the Matrix bridge packages so npm installs can resolve them", () => {
    // The CLI bundle externalizes both packages, so dropping them from the
    // published manifest would leave the bridge permanently unavailable for
    // every npm-installed release.
    const manifest = createPublishManifest({
      // Regular dependencies are emptied because several carry `catalog:`
      // specs; this case is about the real optional dependencies surviving.
      source: { ...serverPackageJson, dependencies: {} },
      version: serverPackageJson.version,
      catalog: {},
      overrides: {},
    });

    expect(manifest.optionalDependencies).toMatchObject({
      "@matrix-org/matrix-sdk-crypto-nodejs": "0.4.0",
      "matrix-bot-sdk": "0.8.0",
    });
    // Exact pins: the crypto binding above 0.4.0 requires Node 24, which this
    // package does not require of its users.
    for (const spec of Object.values(manifest.optionalDependencies ?? {})) {
      expect(spec).toMatch(/^\d+\.\d+\.\d+$/u);
    }
  });
});
