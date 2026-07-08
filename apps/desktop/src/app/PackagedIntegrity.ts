// @effect-diagnostics nodeBuiltinImport:off - Packaged startup integrity runs synchronously before the Effect app runtime exists.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

export const PACKAGED_INTEGRITY_MANIFEST_FILE_NAME = "packaged-integrity-manifest.json";
export const REINSTALL_URL = "https://github.com/agriffiths-bots/t3code/releases/latest";

export interface PackagedIntegrityManifest {
  readonly version: 1;
  readonly requiredFiles: readonly string[];
  readonly unpackedPackages?: readonly string[];
}

export interface PackagedIntegrityCheckInput {
  readonly appPath: string;
  readonly resourcesPath: string;
  readonly manifestPath: string;
  readonly isPackaged: boolean;
}

export interface PackagedIntegrityFailure {
  readonly kind: "manifest-missing" | "manifest-invalid" | "files-missing";
  readonly appPath: string;
  readonly unpackedRoot: string;
  readonly manifestPath: string;
  readonly missingFiles: readonly string[];
  readonly detail: string;
}

export interface PackagedIntegritySuccess {
  readonly ok: true;
  readonly skipped: boolean;
  readonly unpackedRoot: string;
}

export type PackagedIntegrityResult =
  | PackagedIntegritySuccess
  | {
      readonly ok: false;
      readonly failure: PackagedIntegrityFailure;
    };

export function resolveUnpackedRoot(appPath: string, resourcesPath: string): string {
  return appPath.endsWith("app.asar")
    ? `${appPath}.unpacked`
    : NodePath.join(resourcesPath, "app.asar.unpacked");
}

function decodePackagedIntegrityManifest(raw: string): PackagedIntegrityManifest {
  const parsed = JSON.parse(raw) as Partial<PackagedIntegrityManifest>;
  if (
    parsed.version !== 1 ||
    !Array.isArray(parsed.requiredFiles) ||
    parsed.requiredFiles.some((entry) => typeof entry !== "string" || entry.length === 0)
  ) {
    throw new Error("manifest does not match version 1 packaged integrity schema");
  }
  return {
    version: 1,
    requiredFiles: parsed.requiredFiles,
    ...(Array.isArray(parsed.unpackedPackages)
      ? { unpackedPackages: parsed.unpackedPackages.filter((entry) => typeof entry === "string") }
      : {}),
  };
}

export function checkPackagedIntegrity(
  input: PackagedIntegrityCheckInput,
): PackagedIntegrityResult {
  const unpackedRoot = resolveUnpackedRoot(input.appPath, input.resourcesPath);
  if (!input.isPackaged) {
    return { ok: true, skipped: true, unpackedRoot };
  }

  let manifest: PackagedIntegrityManifest;
  try {
    manifest = decodePackagedIntegrityManifest(NodeFS.readFileSync(input.manifestPath, "utf8"));
  } catch (cause) {
    return {
      ok: false,
      failure: {
        kind: NodeFS.existsSync(input.manifestPath) ? "manifest-invalid" : "manifest-missing",
        appPath: input.appPath,
        unpackedRoot,
        manifestPath: input.manifestPath,
        missingFiles: [],
        detail: cause instanceof Error ? cause.message : String(cause),
      },
    };
  }

  const missingFiles = manifest.requiredFiles.filter(
    (relativePath) => !NodeFS.existsSync(NodePath.join(unpackedRoot, relativePath)),
  );
  if (missingFiles.length === 0) {
    return { ok: true, skipped: false, unpackedRoot };
  }

  return {
    ok: false,
    failure: {
      kind: "files-missing",
      appPath: input.appPath,
      unpackedRoot,
      manifestPath: input.manifestPath,
      missingFiles,
      detail: `${missingFiles.length} required unpacked file(s) are missing.`,
    },
  };
}

export function formatPackagedIntegrityFailure(input: {
  readonly failure: PackagedIntegrityFailure;
  readonly appVersion: string;
  readonly updateChannel: string;
}): string {
  const preview = input.failure.missingFiles.slice(0, 12);
  const missingList =
    preview.length > 0
      ? `\n\nMissing files:\n${preview.map((file) => `- ${file}`).join("\n")}${
          input.failure.missingFiles.length > preview.length
            ? `\n- ...and ${input.failure.missingFiles.length - preview.length} more`
            : ""
        }`
      : "";

  return [
    "Installation is incomplete. T3 Code could not verify its packaged runtime files.",
    "",
    `Version: ${input.appVersion}`,
    `Update channel: ${input.updateChannel}`,
    `Install path: ${input.failure.appPath}`,
    `Unpacked runtime path: ${input.failure.unpackedRoot}`,
    `Integrity manifest: ${input.failure.manifestPath}`,
    `Problem: ${input.failure.detail}`,
    missingList,
    "",
    `Please reinstall T3 Code from ${REINSTALL_URL}.`,
  ]
    .filter((part) => part.length > 0)
    .join("\n");
}
