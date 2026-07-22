import {
  ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY,
  AssetClaimJson,
  AssetAttachmentNotFoundError,
  AssetPreviewTypeValidationError,
  AssetProjectFaviconInspectionError,
  AssetProjectFaviconNotFoundError,
  AssetProjectFaviconResolutionError,
  AssetSigningKeyLoadError,
  AssetWorkspaceAssetInspectionError,
  AssetWorkspaceAssetNotFoundError,
  AssetWorkspaceContextNotFoundError,
  AssetWorkspacePathValidationError,
  AssetWorkspaceResolutionError,
  AssetWorkspaceRootNormalizationError,
  AssetSurfaceCredentialClaimJson,
  type AssetClaim,
  type AssetClientCapability,
  type AssetResource,
  type AuthAudienceCeiling,
  type AuthSessionId,
  type DataAudience,
} from "@t3tools/contracts";
import {
  isWorkspaceImagePreviewPath,
  isWorkspacePreviewEntryPath,
  WORKSPACE_BROWSER_PREVIEW_EXTENSIONS,
  WORKSPACE_IMAGE_PREVIEW_EXTENSIONS,
} from "@t3tools/shared/filePreview";
import { PROJECT_FAVICON_FALLBACK_MARKER } from "@t3tools/shared/projectFavicon";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";

import {
  base64UrlDecodeUtf8,
  base64UrlEncode,
  signPayload,
  timingSafeEqualBase64Url,
} from "../auth/utils.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import { canReadDataAudience } from "../auth/audienceDataPolicy.ts";
import { resolveAttachmentPathById } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectFaviconResolver from "../project/ProjectFaviconResolver.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";

export const ASSET_ROUTE_PREFIX = "/api/assets";
export const ASSET_SURFACE_RELAY_PREFIX = `${ASSET_ROUTE_PREFIX}/relay`;
export const ASSET_SURFACE_BIND_PATH = `${ASSET_SURFACE_RELAY_PREFIX}/surface`;
export const ASSET_SURFACE_CREDENTIAL_HEADER = "x-t3-asset-surface";
export const ASSET_APP_RELAY_PREFIX = "/_asset-relay";

export function assetSurfaceCookiePrefix(sessionCookieName: string): string {
  return `${sessionCookieName}_asset_surface_`;
}

export function assetSurfaceCookieName(
  sessionCookieName: string,
  surfaceBindingId: string,
): string {
  return `${assetSurfaceCookiePrefix(sessionCookieName)}${surfaceBindingId}`;
}

const SIGNING_SECRET_NAME = "asset-access-signing-key";
const ASSET_TOKEN_TTL_MS = 60 * 60 * 1000;
const PREVIEW_ASSET_EXTENSIONS = new Set([
  ...WORKSPACE_BROWSER_PREVIEW_EXTENSIONS,
  ...WORKSPACE_IMAGE_PREVIEW_EXTENSIONS,
  ".css",
  ".js",
  ".mjs",
  ".otf",
  ".ttf",
  ".woff",
  ".woff2",
]);

const decodeAssetClaims = Schema.decodeUnknownOption(AssetClaimJson);
const encodeAssetClaims = Schema.encodeSync(AssetClaimJson);
const decodeAssetSurfaceCredentialClaims = Schema.decodeUnknownOption(
  AssetSurfaceCredentialClaimJson,
);
const encodeAssetSurfaceCredentialClaims = Schema.encodeSync(AssetSurfaceCredentialClaimJson);

export type ResolvedAsset = { readonly kind: "file"; readonly path: string };

function decodeClaims(encodedPayload: string): AssetClaim | null {
  try {
    return Option.getOrNull(decodeAssetClaims(base64UrlDecodeUtf8(encodedPayload)));
  } catch {
    return null;
  }
}

function decodeSurfaceCredentialClaims(encodedPayload: string) {
  try {
    return Option.getOrNull(
      decodeAssetSurfaceCredentialClaims(base64UrlDecodeUtf8(encodedPayload)),
    );
  } catch {
    return null;
  }
}

function signToken(encodedPayload: string, signingSecret: Uint8Array): string {
  return `${encodedPayload}.${signPayload(encodedPayload, signingSecret)}`;
}

function splitSignedToken(token: string): readonly [string, string] | null {
  const parts = token.split(".");
  return parts.length === 2 && parts[0] && parts[1] ? [parts[0], parts[1]] : null;
}

export function decodeAssetRelayRoutingClaim(token: string): AssetClaim | null {
  const tokenParts = splitSignedToken(token);
  return tokenParts === null ? null : decodeClaims(tokenParts[0]);
}

export function effectiveAssetClaimAudience(claim: AssetClaim): DataAudience {
  return claim.dataAudience === "private" || claim.issuingAudience === "private"
    ? "private"
    : "factory";
}

const verifyAssetClaimToken = Effect.fn("AssetAccess.verifyAssetClaimToken")(function* (
  token: string,
) {
  const tokenParts = splitSignedToken(token);
  if (tokenParts === null) return null;
  const [encodedPayload, signature] = tokenParts;

  const secretStore = yield* ServerSecretStore.ServerSecretStore;
  const signingSecret = yield* secretStore.getOrCreateRandom(SIGNING_SECRET_NAME, 32).pipe(
    Effect.tapError((cause) => Effect.logError("Failed to load the asset signing key.", { cause })),
    Effect.orElseSucceed(() => null),
  );
  if (!signingSecret) return null;
  if (!timingSafeEqualBase64Url(signature, signPayload(encodedPayload, signingSecret))) return null;

  const claim = decodeClaims(encodedPayload);
  if (!claim || claim.expiresAt <= (yield* Clock.currentTimeMillis)) return null;
  return { claim, signingSecret };
});

function surfaceBindingIdForSession(sessionId: AuthSessionId, signingSecret: Uint8Array): string {
  return signPayload(`asset-surface:${sessionId}`, signingSecret);
}

function decodeRelativePath(value: string): string | null {
  try {
    return decodeURIComponent(value);
  } catch {
    return null;
  }
}

const optionOnNotFound = <A, R>(
  effect: Effect.Effect<A, PlatformError.PlatformError, R>,
): Effect.Effect<Option.Option<A>, PlatformError.PlatformError, R> =>
  effect.pipe(
    Effect.map(Option.some),
    Effect.catchTags({
      PlatformError: (error) =>
        error.reason._tag === "NotFound" ? Effect.succeed(Option.none<A>()) : Effect.fail(error),
    }),
  );

const resolveCanonicalWorkspaceFile = Effect.fn("AssetAccess.resolveCanonicalWorkspaceFile")(
  function* (input: { readonly workspaceRoot: string; readonly relativePath: string }) {
    const fileSystem = yield* FileSystem.FileSystem;
    const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
    const resolved = yield* workspacePaths.resolveRelativePathWithinRoot(input).pipe(
      Effect.map(Option.some),
      Effect.catchTags({
        WorkspacePathOutsideRootError: () => Effect.succeed(Option.none()),
      }),
    );
    if (Option.isNone(resolved)) return null;

    const [canonicalRoot, canonicalFile] = yield* Effect.all([
      optionOnNotFound(fileSystem.realPath(input.workspaceRoot)),
      optionOnNotFound(fileSystem.realPath(resolved.value.absolutePath)),
    ]);
    if (Option.isNone(canonicalRoot) || Option.isNone(canonicalFile)) return null;

    const path = yield* Path.Path;
    const relative = path.relative(canonicalRoot.value, canonicalFile.value);
    if (relative === "" || relative.startsWith("..") || path.isAbsolute(relative)) return null;

    const info = yield* optionOnNotFound(fileSystem.stat(canonicalFile.value));
    return Option.isSome(info) && info.value.type === "File" ? canonicalFile.value : null;
  },
);

const resolveCanonicalWorkspaceFileForRequest = (input: {
  readonly workspaceRoot: string;
  readonly relativePath: string;
}) =>
  resolveCanonicalWorkspaceFile(input).pipe(
    Effect.tapError((cause) =>
      Effect.logError("Failed to resolve canonical asset path.", {
        workspaceRoot: input.workspaceRoot,
        relativePath: input.relativePath,
        cause,
      }),
    ),
    Effect.orElseSucceed(() => null),
  );

export const issueAssetUrl = Effect.fn("AssetAccess.issueAssetUrl")(function* (input: {
  readonly resource: AssetResource;
  readonly workspaceRoot?: string;
  readonly dataAudience?: DataAudience;
  readonly issuingAudience?: DataAudience;
  readonly clientCapabilities?: ReadonlyArray<AssetClientCapability>;
  readonly surfaceSessionId?: AuthSessionId;
  readonly surfaceSessionExpiresAt?: DateTime.DateTime;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
  const now = yield* Clock.currentTimeMillis;
  let expiresAt = now + ASSET_TOKEN_TTL_MS;
  const serverEnvironment = yield* ServerEnvironment.ServerEnvironment;
  const issuingBackendId = yield* serverEnvironment.getEnvironmentId;
  const dataAudience = input.dataAudience ?? "private";
  const issuingAudience = input.issuingAudience ?? "private";
  if (!canReadDataAudience(issuingAudience, dataAudience)) {
    return yield* new AssetWorkspaceContextNotFoundError({ resource: input.resource });
  }
  const audienceClaims = { dataAudience, issuingAudience, issuingBackendId };
  let claims: AssetClaim;
  let fileName: string;

  switch (input.resource._tag) {
    case "workspace-file": {
      if (!input.workspaceRoot) {
        return yield* new AssetWorkspaceContextNotFoundError({
          resource: input.resource,
        });
      }
      const workspaceRoot = yield* workspacePaths.normalizeWorkspaceRoot(input.workspaceRoot).pipe(
        Effect.mapError(
          (cause) =>
            new AssetWorkspaceRootNormalizationError({
              resource: input.resource,
              cause,
            }),
        ),
      );
      const relativePath = path.isAbsolute(input.resource.path)
        ? path.relative(workspaceRoot, input.resource.path)
        : input.resource.path;
      const resolved = yield* workspacePaths
        .resolveRelativePathWithinRoot({ workspaceRoot, relativePath })
        .pipe(
          Effect.mapError(
            (cause) =>
              new AssetWorkspacePathValidationError({
                resource: input.resource,
                cause,
              }),
          ),
        );
      if (!isWorkspacePreviewEntryPath(resolved.relativePath)) {
        return yield* new AssetPreviewTypeValidationError({
          resource: input.resource,
        });
      }
      const canonicalFile = yield* resolveCanonicalWorkspaceFile({
        workspaceRoot,
        relativePath: resolved.relativePath,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new AssetWorkspaceAssetInspectionError({
              resource: input.resource,
              cause,
            }),
        ),
      );
      if (!canonicalFile) {
        return yield* new AssetWorkspaceAssetNotFoundError({
          resource: input.resource,
        });
      }
      const canonicalWorkspaceRoot = yield* fileSystem.realPath(workspaceRoot).pipe(
        Effect.mapError(
          (cause) =>
            new AssetWorkspaceResolutionError({
              resource: input.resource,
              cause,
            }),
        ),
      );
      claims = isWorkspaceImagePreviewPath(resolved.relativePath)
        ? {
            version: 1,
            kind: "workspace-file-exact",
            workspaceRoot: canonicalWorkspaceRoot,
            relativePath: resolved.relativePath,
            expiresAt,
            ...audienceClaims,
            surfaceBindingId: null,
          }
        : {
            version: 1,
            kind: "workspace-file",
            workspaceRoot: canonicalWorkspaceRoot,
            baseRelativePath: path.dirname(resolved.relativePath),
            expiresAt,
            ...audienceClaims,
            surfaceBindingId: null,
          };
      fileName = path.basename(resolved.relativePath);
      break;
    }
    case "attachment": {
      const config = yield* ServerConfig.ServerConfig;
      const attachmentPath = resolveAttachmentPathById({
        attachmentsDir: config.attachmentsDir,
        attachmentId: input.resource.attachmentId,
      });
      if (!attachmentPath) {
        return yield* new AssetAttachmentNotFoundError({
          resource: input.resource,
        });
      }
      claims = {
        version: 1,
        kind: "attachment",
        attachmentId: input.resource.attachmentId,
        expiresAt,
        ...audienceClaims,
        surfaceBindingId: null,
      };
      fileName = path.basename(attachmentPath);
      break;
    }
    case "project-favicon": {
      const workspaceRoot = yield* workspacePaths.normalizeWorkspaceRoot(input.resource.cwd).pipe(
        Effect.mapError(
          (cause) =>
            new AssetWorkspaceRootNormalizationError({
              resource: input.resource,
              cause,
            }),
        ),
      );
      const faviconResolver = yield* ProjectFaviconResolver.ProjectFaviconResolver;
      const faviconPath = yield* faviconResolver.resolvePath(workspaceRoot).pipe(
        Effect.mapError(
          (cause) =>
            new AssetProjectFaviconResolutionError({
              resource: input.resource,
              cause,
            }),
        ),
      );
      const relativePath = faviconPath ? path.relative(workspaceRoot, faviconPath) : null;
      if (
        relativePath &&
        !(yield* resolveCanonicalWorkspaceFile({ workspaceRoot, relativePath }).pipe(
          Effect.mapError(
            (cause) =>
              new AssetProjectFaviconInspectionError({
                resource: input.resource,
                cause,
              }),
          ),
        ))
      ) {
        return yield* new AssetProjectFaviconNotFoundError({
          resource: input.resource,
        });
      }
      claims = {
        version: 1,
        kind: "project-favicon",
        workspaceRoot: yield* fileSystem.realPath(workspaceRoot).pipe(
          Effect.mapError(
            (cause) =>
              new AssetWorkspaceResolutionError({
                resource: input.resource,
                cause,
              }),
          ),
        ),
        relativePath,
        expiresAt,
        ...audienceClaims,
        surfaceBindingId: null,
      };
      fileName = relativePath ? path.basename(relativePath) : PROJECT_FAVICON_FALLBACK_MARKER;
      break;
    }
  }

  const secretStore = yield* ServerSecretStore.ServerSecretStore;
  const signingSecret = yield* secretStore.getOrCreateRandom(SIGNING_SECRET_NAME, 32).pipe(
    Effect.mapError(
      (cause) =>
        new AssetSigningKeyLoadError({
          resource: input.resource,
          cause,
        }),
    ),
  );
  if (!input.clientCapabilities?.includes(ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY)) {
    const encodedPayload = base64UrlEncode(encodeAssetClaims(claims));
    return {
      relativeUrl: `${ASSET_ROUTE_PREFIX}/${signToken(encodedPayload, signingSecret)}/${encodeURIComponent(fileName)}`,
      expiresAt,
    };
  }
  if (input.surfaceSessionId === undefined) {
    return yield* new AssetWorkspaceContextNotFoundError({ resource: input.resource });
  }
  const surfaceBindingId = surfaceBindingIdForSession(input.surfaceSessionId, signingSecret);
  const surfaceCredentialExpiresAt = Math.min(
    expiresAt,
    input.surfaceSessionExpiresAt?.epochMilliseconds ?? expiresAt,
  );
  expiresAt = surfaceCredentialExpiresAt;
  if (expiresAt <= now) {
    return yield* new AssetWorkspaceContextNotFoundError({ resource: input.resource });
  }
  claims = { ...claims, surfaceBindingId, expiresAt };
  const encodedPayload = base64UrlEncode(encodeAssetClaims(claims));
  const token = signToken(encodedPayload, signingSecret);
  const surfaceCredential = signToken(
    base64UrlEncode(
      encodeAssetSurfaceCredentialClaims({
        version: 1,
        kind: "asset-surface",
        surfaceSessionId: input.surfaceSessionId,
        surfaceBindingId,
        expiresAt: surfaceCredentialExpiresAt,
      }),
    ),
    signingSecret,
  );
  return {
    relativeUrl: `${ASSET_SURFACE_RELAY_PREFIX}/${token}/${encodeURIComponent(fileName)}`,
    expiresAt,
    surfaceCredential,
  };
});

export const verifyAssetSurfaceCredential = Effect.fn("AssetAccess.verifyAssetSurfaceCredential")(
  function* (credential: string) {
    const tokenParts = splitSignedToken(credential);
    if (tokenParts === null) return null;
    const [encodedPayload, signature] = tokenParts;

    const secretStore = yield* ServerSecretStore.ServerSecretStore;
    const signingSecret = yield* secretStore.getOrCreateRandom(SIGNING_SECRET_NAME, 32).pipe(
      Effect.tapError((cause) =>
        Effect.logError("Failed to load the asset surface signing key.", { cause }),
      ),
      Effect.orElseSucceed(() => null),
    );
    if (!signingSecret) return null;
    if (!timingSafeEqualBase64Url(signature, signPayload(encodedPayload, signingSecret))) {
      return null;
    }

    const claims = decodeSurfaceCredentialClaims(encodedPayload);
    if (!claims || claims.expiresAt <= (yield* Clock.currentTimeMillis)) return null;
    const sessions = yield* SessionStore.SessionStore;
    const active = yield* sessions.isActive(claims.surfaceSessionId).pipe(
      Effect.tapError((cause) =>
        Effect.logWarning("Failed to verify the asset surface session.", {
          sessionId: claims.surfaceSessionId,
          cause,
        }),
      ),
      Effect.orElseSucceed(() => false),
    );
    return active ? claims : null;
  },
);

export const resolveAsset = Effect.fn("AssetAccess.resolveAsset")(function* (
  token: string,
  relativePath: string,
  requestProof: {
    readonly sessionId?: AuthSessionId;
    readonly surfaceCredentials?: ReadonlyArray<string>;
    readonly allowUnbound?: boolean;
  } = {},
) {
  const verified = yield* verifyAssetClaimToken(token);
  if (verified === null) return null;
  const { claim: claims, signingSecret } = verified;

  if (claims.surfaceBindingId === null) {
    if (requestProof.allowUnbound !== true) return null;
  } else {
    let matchedSurface =
      requestProof.sessionId !== undefined &&
      surfaceBindingIdForSession(requestProof.sessionId, signingSecret) === claims.surfaceBindingId;
    if (!matchedSurface) {
      for (const credential of requestProof.surfaceCredentials ?? []) {
        const surfaceCredential = yield* verifyAssetSurfaceCredential(credential);
        if (surfaceCredential?.surfaceBindingId === claims.surfaceBindingId) {
          matchedSurface = true;
          break;
        }
      }
    }
    if (!matchedSurface) return null;
  }

  if (claims.kind === "attachment") {
    const config = yield* ServerConfig.ServerConfig;
    const attachmentPath = resolveAttachmentPathById({
      attachmentsDir: config.attachmentsDir,
      attachmentId: claims.attachmentId,
    });
    if (!attachmentPath) return null;
    const fileSystem = yield* FileSystem.FileSystem;
    const info = yield* optionOnNotFound(fileSystem.stat(attachmentPath)).pipe(
      Effect.tapError((cause) =>
        Effect.logError("Failed to inspect attachment asset.", {
          attachmentId: claims.attachmentId,
          path: attachmentPath,
          cause,
        }),
      ),
      Effect.orElseSucceed(() => Option.none()),
    );
    return Option.isSome(info) && info.value.type === "File"
      ? ({ kind: "file", path: attachmentPath } satisfies ResolvedAsset)
      : null;
  }

  if (claims.kind === "project-favicon") {
    if (claims.relativePath === null) return null;
    const faviconPath = yield* resolveCanonicalWorkspaceFileForRequest({
      workspaceRoot: claims.workspaceRoot,
      relativePath: claims.relativePath,
    });
    return faviconPath ? ({ kind: "file", path: faviconPath } satisfies ResolvedAsset) : null;
  }

  const decodedPath = decodeRelativePath(relativePath);
  if (decodedPath === null) return null;
  const path = yield* Path.Path;
  if (claims.kind === "workspace-file-exact") {
    if (decodedPath !== path.basename(claims.relativePath)) return null;
    const exactWorkspaceFile = yield* resolveCanonicalWorkspaceFileForRequest({
      workspaceRoot: claims.workspaceRoot,
      relativePath: claims.relativePath,
    });
    return exactWorkspaceFile
      ? ({ kind: "file", path: exactWorkspaceFile } satisfies ResolvedAsset)
      : null;
  }
  const segments = decodedPath.split(/[\\/]/);
  if (
    decodedPath.length === 0 ||
    decodedPath.includes("\0") ||
    segments.some((segment) => segment === "." || segment === ".." || segment.startsWith(".")) ||
    !PREVIEW_ASSET_EXTENSIONS.has(path.extname(decodedPath).toLowerCase())
  ) {
    return null;
  }
  const joinedRelativePath =
    claims.baseRelativePath === "." ? decodedPath : path.join(claims.baseRelativePath, decodedPath);
  const workspaceFile = yield* resolveCanonicalWorkspaceFileForRequest({
    workspaceRoot: claims.workspaceRoot,
    relativePath: joinedRelativePath,
  });
  return workspaceFile ? ({ kind: "file", path: workspaceFile } satisfies ResolvedAsset) : null;
});

export const resolveLocalAssetRelay = Effect.fn("AssetAccess.resolveLocalAssetRelay")(
  function* (input: {
    readonly token: string;
    readonly relativePath: string;
    readonly viewerSessionId: AuthSessionId;
    readonly viewerAudienceCeiling: AuthAudienceCeiling;
    readonly viewerSessionExpiresAt?: DateTime.DateTime;
  }) {
    const verified = yield* verifyAssetClaimToken(input.token);
    if (verified === null) return null;
    const { claim, signingSecret } = verified;
    const environment = yield* ServerEnvironment.ServerEnvironment;
    const localBackendId = yield* environment.getEnvironmentId;
    if (claim.issuingBackendId !== null && claim.issuingBackendId !== localBackendId) return null;

    const audience = effectiveAssetClaimAudience(claim);
    if (!canReadDataAudience(input.viewerAudienceCeiling, audience)) return null;
    if (claim.surfaceBindingId === null) {
      return audience === "private"
        ? null
        : yield* resolveAsset(input.token, input.relativePath, { allowUnbound: true });
    }

    const now = yield* Clock.currentTimeMillis;
    const proofExpiresAt = Math.min(
      claim.expiresAt,
      input.viewerSessionExpiresAt?.epochMilliseconds ?? claim.expiresAt,
    );
    if (proofExpiresAt <= now) return null;
    const relayProof = signToken(
      base64UrlEncode(
        encodeAssetSurfaceCredentialClaims({
          version: 1,
          kind: "asset-surface",
          surfaceSessionId: input.viewerSessionId,
          surfaceBindingId: claim.surfaceBindingId,
          expiresAt: proofExpiresAt,
        }),
      ),
      signingSecret,
    );
    return yield* resolveAsset(input.token, input.relativePath, {
      surfaceCredentials: [relayProof],
    });
  },
);
