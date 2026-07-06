// @effect-diagnostics nodeBuiltinImport:off - Tests create incomplete packaged-install fixtures directly.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { assert, describe, it } from "@effect/vitest";

import {
  checkPackagedIntegrity,
  formatPackagedIntegrityFailure,
  PACKAGED_INTEGRITY_MANIFEST_FILE_NAME,
  resolveUnpackedRoot,
} from "./PackagedIntegrity.ts";

function makeTempInstall() {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3code-integrity-"));
  const resourcesPath = NodePath.join(root, "resources");
  const appPath = NodePath.join(resourcesPath, "app.asar");
  const unpackedRoot = `${appPath}.unpacked`;
  const manifestPath = NodePath.join(resourcesPath, PACKAGED_INTEGRITY_MANIFEST_FILE_NAME);
  NodeFS.mkdirSync(unpackedRoot, { recursive: true });
  NodeFS.mkdirSync(resourcesPath, { recursive: true });
  return { root, resourcesPath, appPath, unpackedRoot, manifestPath };
}

describe("PackagedIntegrity", () => {
  it("resolves the app.asar.unpacked sidecar beside app.asar", () => {
    assert.equal(
      resolveUnpackedRoot(
        "C:\\Users\\adam\\AppData\\Local\\Programs\\T3\\resources\\app.asar",
        "ignored",
      ),
      "C:\\Users\\adam\\AppData\\Local\\Programs\\T3\\resources\\app.asar.unpacked",
    );
  });

  it("passes when every manifest file exists", () => {
    const install = makeTempInstall();
    NodeFS.mkdirSync(NodePath.join(install.unpackedRoot, "node_modules/effect"), {
      recursive: true,
    });
    NodeFS.mkdirSync(NodePath.join(install.unpackedRoot, "apps/server/dist"), {
      recursive: true,
    });
    NodeFS.writeFileSync(NodePath.join(install.unpackedRoot, "apps/server/dist/bin.mjs"), "");
    NodeFS.writeFileSync(
      NodePath.join(install.unpackedRoot, "node_modules/effect/package.json"),
      "{}",
    );
    NodeFS.writeFileSync(
      install.manifestPath,
      JSON.stringify({
        version: 1,
        requiredFiles: ["apps/server/dist/bin.mjs", "node_modules/effect/package.json"],
      }),
    );

    const result = checkPackagedIntegrity({
      appPath: install.appPath,
      resourcesPath: install.resourcesPath,
      manifestPath: install.manifestPath,
      isPackaged: true,
    });

    assert.isTrue(result.ok);
  });

  it("reports missing unpacked files before runtime imports crash", () => {
    const install = makeTempInstall();
    NodeFS.mkdirSync(NodePath.join(install.unpackedRoot, "apps/server/dist"), {
      recursive: true,
    });
    NodeFS.writeFileSync(NodePath.join(install.unpackedRoot, "apps/server/dist/bin.mjs"), "");
    NodeFS.writeFileSync(
      install.manifestPath,
      JSON.stringify({
        version: 1,
        requiredFiles: ["apps/server/dist/bin.mjs", "node_modules/effect/index.js"],
      }),
    );

    const result = checkPackagedIntegrity({
      appPath: install.appPath,
      resourcesPath: install.resourcesPath,
      manifestPath: install.manifestPath,
      isPackaged: true,
    });

    assert.isFalse(result.ok);
    if (result.ok) return;
    assert.deepEqual(result.failure.missingFiles, ["node_modules/effect/index.js"]);
    assert.include(
      formatPackagedIntegrityFailure({
        failure: result.failure,
        appVersion: "1.2.3",
        updateChannel: "nightly",
      }),
      "Installation is incomplete",
    );
  });
});
