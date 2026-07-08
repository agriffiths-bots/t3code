import * as NodeModule from "node:module";
// @effect-diagnostics-next-line nodeBuiltinImport:off - The bootstrap resolves packaged files before the Effect app runtime exists.
import * as NodePath from "node:path";

import * as Electron from "electron";

import {
  checkPackagedIntegrity,
  formatPackagedIntegrityFailure,
  PACKAGED_INTEGRITY_MANIFEST_FILE_NAME,
} from "./app/PackagedIntegrity.ts";

function resolveUpdateChannel(appVersion: string): string {
  return /-nightly\.\d{8}\.\d+$/u.test(appVersion) ? "nightly" : "latest";
}

function loadMainProcess() {
  const runtimeRequire = NodeModule.createRequire(__filename);
  runtimeRequire(NodePath.join(__dirname, "main.cjs"));
}

const integrityResult = checkPackagedIntegrity({
  appPath: Electron.app.getAppPath(),
  resourcesPath: process.resourcesPath,
  manifestPath: NodePath.join(process.resourcesPath, PACKAGED_INTEGRITY_MANIFEST_FILE_NAME),
  isPackaged: Electron.app.isPackaged,
});

if (!integrityResult.ok) {
  const content = formatPackagedIntegrityFailure({
    failure: integrityResult.failure,
    appVersion: Electron.app.getVersion(),
    updateChannel: resolveUpdateChannel(Electron.app.getVersion()),
  });
  process.stderr.write(`fatal startup error: ${content}\n`);
  Electron.dialog.showErrorBox("T3 Code installation is incomplete", content);
  Electron.app.exit(1);
} else {
  loadMainProcess();
}
