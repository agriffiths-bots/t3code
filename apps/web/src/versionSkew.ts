import type { EnvironmentId, ServerConfig } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import { APP_BUILD_SHA, APP_VERSION } from "./branding";
import { getLocalStorageItem, setLocalStorageItem } from "./hooks/useLocalStorage";

export interface VersionMismatch {
  readonly clientVersion: string;
  readonly serverVersion: string;
  readonly clientBuildSha?: string;
  readonly serverBuildSha?: string;
  readonly hint: string;
}

export interface VersionMismatchOptions {
  readonly clientVersion?: string | null | undefined;
  readonly clientBuildSha?: string | null | undefined;
  readonly serverBuildSha?: string | null | undefined;
}

export const VERSION_MISMATCH_DISMISSALS_STORAGE_KEY = "t3code:version-mismatch-dismissals:v1";

const VersionMismatchDismissalsSchema = Schema.Struct({
  keys: Schema.Array(Schema.String),
});

type VersionMismatchDismissals = typeof VersionMismatchDismissalsSchema.Type;

function normalizeVersion(version: string | null | undefined): string | null {
  const trimmed = version?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : null;
}

function normalizeBuildSha(sha: string | null | undefined): string | null {
  const trimmed = sha?.trim();
  return trimmed && /^[0-9a-f]{40}$/i.test(trimmed) ? trimmed.toLowerCase() : null;
}

function makeVersionMismatch(
  clientVersion: string,
  serverVersion: string,
  clientBuildSha: string | null,
  serverBuildSha: string | null,
): VersionMismatch {
  return {
    clientVersion,
    serverVersion,
    ...(clientBuildSha !== null ? { clientBuildSha } : {}),
    ...(serverBuildSha !== null ? { serverBuildSha } : {}),
    hint: "Version mismatch. Try syncing the client and server to the same T3 Code version.",
  };
}

export function resolveVersionMismatch(
  serverVersion: string | null | undefined,
  options: VersionMismatchOptions = {},
): VersionMismatch | null {
  const normalizedClientVersion = normalizeVersion(options.clientVersion ?? APP_VERSION);
  const normalizedServerVersion = normalizeVersion(serverVersion);
  if (!normalizedClientVersion || !normalizedServerVersion) {
    return null;
  }

  const normalizedClientBuildSha = normalizeBuildSha(options.clientBuildSha ?? APP_BUILD_SHA);
  const normalizedServerBuildSha = normalizeBuildSha(options.serverBuildSha);
  if (normalizedClientBuildSha !== null && normalizedServerBuildSha !== null) {
    return normalizedClientBuildSha === normalizedServerBuildSha
      ? null
      : makeVersionMismatch(
          normalizedClientVersion,
          normalizedServerVersion,
          normalizedClientBuildSha,
          normalizedServerBuildSha,
        );
  }

  return normalizedClientVersion === normalizedServerVersion
    ? null
    : makeVersionMismatch(
        normalizedClientVersion,
        normalizedServerVersion,
        normalizedClientBuildSha,
        normalizedServerBuildSha,
      );
}

export function resolveServerConfigVersionMismatch(
  serverConfig: Pick<ServerConfig, "environment"> | null | undefined,
  options: Omit<VersionMismatchOptions, "serverBuildSha"> = {},
): VersionMismatch | null {
  return resolveVersionMismatch(serverConfig?.environment.serverVersion, {
    ...options,
    serverBuildSha: serverConfig?.environment.serverBuildSha,
  });
}

export function buildVersionMismatchDismissalKey(
  environmentId: EnvironmentId,
  mismatch: Pick<
    VersionMismatch,
    "clientVersion" | "serverVersion" | "clientBuildSha" | "serverBuildSha"
  >,
): string {
  const clientIdentity =
    mismatch.clientBuildSha !== undefined
      ? `${mismatch.clientVersion}@${mismatch.clientBuildSha}`
      : mismatch.clientVersion;
  const serverIdentity =
    mismatch.serverBuildSha !== undefined
      ? `${mismatch.serverVersion}@${mismatch.serverBuildSha}`
      : mismatch.serverVersion;
  return `${environmentId}:${clientIdentity}:${serverIdentity}`;
}

export function formatVersionWithBuildSha(version: string, buildSha: string | undefined): string {
  return buildSha === undefined ? version : `${version} (sha ${buildSha.slice(0, 8)})`;
}

function readVersionMismatchDismissals(): VersionMismatchDismissals {
  try {
    return (
      getLocalStorageItem(
        VERSION_MISMATCH_DISMISSALS_STORAGE_KEY,
        VersionMismatchDismissalsSchema,
      ) ?? { keys: [] }
    );
  } catch (error) {
    console.error("Could not read version-mismatch dismissals.", error);
    return { keys: [] };
  }
}

function writeVersionMismatchDismissals(document: VersionMismatchDismissals): void {
  try {
    setLocalStorageItem(
      VERSION_MISMATCH_DISMISSALS_STORAGE_KEY,
      document,
      VersionMismatchDismissalsSchema,
    );
  } catch (error) {
    console.error("Could not persist version-mismatch dismissals.", error);
  }
}

export function isVersionMismatchDismissed(dismissalKey: string | null | undefined): boolean {
  if (!dismissalKey) {
    return false;
  }
  return readVersionMismatchDismissals().keys.includes(dismissalKey);
}

export function dismissVersionMismatch(dismissalKey: string | null | undefined): void {
  if (!dismissalKey) {
    return;
  }
  const document = readVersionMismatchDismissals();
  if (document.keys.includes(dismissalKey)) {
    return;
  }
  writeVersionMismatchDismissals({
    keys: [...document.keys, dismissalKey],
  });
}

export function appendVersionMismatchHint(
  message: string | null | undefined,
  mismatch: VersionMismatch | null | undefined,
): string | null {
  const normalizedMessage = normalizeVersion(message);
  if (!normalizedMessage) {
    return mismatch?.hint ?? null;
  }
  if (!mismatch) {
    return normalizedMessage;
  }
  return `${normalizedMessage} Hint: ${mismatch.hint}`;
}
