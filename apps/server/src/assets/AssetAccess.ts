// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";

import {
  ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY,
  AssetClientUpgradeRequiredError,
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
  AssetWorkspaceContextResolutionError,
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
import * as Exit from "effect/Exit";
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
import { canReadDataAudience, strictestDataAudience } from "../auth/audienceDataPolicy.ts";
import {
  parseThreadSegmentFromAttachmentId,
  resolveAttachmentPathById,
  toSafeThreadAttachmentSegment,
} from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProjectFilesystemAudienceGuard from "../project/ProjectFilesystemAudienceGuard.ts";
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

export type ResolvedAsset = {
  readonly kind: "file";
  readonly path: string;
  readonly contentLength: number;
  readonly stream: NodeFS.ReadStream;
};
export type AssetResolution = ResolvedAsset | { readonly kind: "forbidden" };

const audienceClaimsForIssue = Effect.fn("AssetAccess.audienceClaimsForIssue")(function* (input: {
  readonly resource: AssetResource;
  readonly claimedDataAudience: DataAudience;
  readonly issuingAudience: AuthAudienceCeiling;
  readonly issuingBackendId: AssetClaim["issuingBackendId"];
  readonly canonicalTargetPath?: string;
  readonly liveDataAudience?: DataAudience;
}) {
  const liveDataAudience = input.canonicalTargetPath
    ? ((yield* ProjectFilesystemAudienceGuard.classifyPathAudience(input.canonicalTargetPath).pipe(
        Effect.mapError(
          (cause) => new AssetWorkspaceContextResolutionError({ resource: input.resource, cause }),
        ),
      )) ?? "private")
    : (input.liveDataAudience ?? input.claimedDataAudience);
  const dataAudience = strictestDataAudience(input.claimedDataAudience, liveDataAudience);
  if (!canReadDataAudience(input.issuingAudience, dataAudience)) {
    return yield* new AssetWorkspaceContextNotFoundError({ resource: input.resource });
  }
  return {
    dataAudience,
    issuingAudience: input.issuingAudience,
    issuingBackendId: input.issuingBackendId,
  };
});

export const classifyAttachmentAudience = Effect.fn("AssetAccess.classifyAttachmentAudience")(
  function* (attachmentId: string) {
    const threadSegment = parseThreadSegmentFromAttachmentId(attachmentId);
    if (!threadSegment) return null;

    const snapshotQuery = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
    const snapshot = yield* snapshotQuery.getCommandReadModel();
    const projectById = new Map(snapshot.projects.map((project) => [project.id, project]));
    let audience: DataAudience | null = null;
    for (const thread of snapshot.threads) {
      if (toSafeThreadAttachmentSegment(thread.id) !== threadSegment) {
        continue;
      }
      const project = projectById.get(thread.projectId);
      const threadAudience = project
        ? strictestDataAudience(thread.dataAudience, project.dataAudience)
        : thread.dataAudience;
      audience =
        audience === null ? threadAudience : strictestDataAudience(audience, threadAudience);
    }
    return audience;
  },
);

type OpenedAssetFile = {
  readonly handle: NodeFSP.FileHandle;
  readonly canonicalPath: string;
  readonly contentLength: number;
};

const closeOpenedAssetFile = (opened: OpenedAssetFile) =>
  Effect.promise(() => opened.handle.close().catch(() => undefined));

const openResolvedAssetFile = Effect.fn("AssetAccess.openResolvedAssetFile")(function* (
  resolvedPath: string,
) {
  const handle = yield* Effect.tryPromise(() =>
    NodeFSP.open(resolvedPath, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW),
  ).pipe(Effect.orElseSucceed(() => null));
  if (!handle) return null;

  const opened = yield* Effect.tryPromise(async () => {
    const handleInfo = await handle.stat();
    if (!handleInfo.isFile()) return null;

    // Re-resolve and compare the inode after opening. This catches both final-component and
    // ancestor swaps while retaining the exact handle that was authorized for streaming.
    const canonicalPath = await NodeFSP.realpath(resolvedPath);
    const pathInfo = await NodeFSP.stat(canonicalPath);
    if (!pathInfo.isFile() || pathInfo.dev !== handleInfo.dev || pathInfo.ino !== handleInfo.ino) {
      return null;
    }
    return { handle, canonicalPath, contentLength: handleInfo.size } satisfies OpenedAssetFile;
  }).pipe(Effect.orElseSucceed(() => null));

  if (!opened) {
    yield* Effect.promise(() => handle.close().catch(() => undefined));
  }
  return opened;
});

const toResolvedAsset = (opened: OpenedAssetFile): ResolvedAsset => ({
  kind: "file",
  path: opened.canonicalPath,
  contentLength: opened.contentLength,
  stream: opened.handle.createReadStream({ autoClose: true }),
});

const withOpenedAssetFile = <E, R>(
  resolvedPath: string,
  use: (
    opened: OpenedAssetFile,
    transferToResponse: () => ResolvedAsset,
  ) => Effect.Effect<AssetResolution | null, E, R>,
): Effect.Effect<AssetResolution | null, E, R> => {
  let transferredToResponse = false;
  return Effect.acquireUseRelease(
    openResolvedAssetFile(resolvedPath),
    (opened) => {
      if (!opened) return Effect.succeed(null);
      return use(opened, () => {
        transferredToResponse = true;
        return toResolvedAsset(opened);
      });
    },
    (opened, exit) =>
      opened && !(transferredToResponse && Exit.isSuccess(exit))
        ? closeOpenedAssetFile(opened)
        : Effect.void,
  );
};

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
  return strictestDataAudience(claim.dataAudience, claim.issuingAudience);
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
  readonly issuingAudience?: AuthAudienceCeiling;
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
  const claimedDataAudience = input.dataAudience ?? ("private" as const);
  const issuingAudience = input.issuingAudience ?? ("private" as const);
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
      const audienceClaims = yield* audienceClaimsForIssue({
        resource: input.resource,
        claimedDataAudience,
        issuingAudience,
        issuingBackendId,
        canonicalTargetPath: canonicalFile,
      });
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
            surfaceBindingId: null,
            ...audienceClaims,
          }
        : {
            version: 1,
            kind: "workspace-file",
            workspaceRoot: canonicalWorkspaceRoot,
            baseRelativePath: path.dirname(resolved.relativePath),
            expiresAt,
            surfaceBindingId: null,
            ...audienceClaims,
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
      const audienceClaims = yield* audienceClaimsForIssue({
        resource: input.resource,
        claimedDataAudience,
        issuingAudience,
        issuingBackendId,
        liveDataAudience:
          (yield* classifyAttachmentAudience(input.resource.attachmentId).pipe(
            Effect.mapError(
              (cause) =>
                new AssetWorkspaceContextResolutionError({ resource: input.resource, cause }),
            ),
          )) ?? "private",
      });
      claims = {
        version: 1,
        kind: "attachment",
        attachmentId: input.resource.attachmentId,
        expiresAt,
        surfaceBindingId: null,
        ...audienceClaims,
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
      const canonicalFaviconPath = relativePath
        ? yield* resolveCanonicalWorkspaceFile({ workspaceRoot, relativePath }).pipe(
            Effect.mapError(
              (cause) =>
                new AssetProjectFaviconInspectionError({
                  resource: input.resource,
                  cause,
                }),
            ),
          )
        : null;
      if (relativePath && !canonicalFaviconPath) {
        return yield* new AssetProjectFaviconNotFoundError({
          resource: input.resource,
        });
      }
      const audienceClaims = yield* audienceClaimsForIssue({
        resource: input.resource,
        claimedDataAudience,
        issuingAudience,
        issuingBackendId,
        ...(canonicalFaviconPath ? { canonicalTargetPath: canonicalFaviconPath } : {}),
      });
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
        surfaceBindingId: null,
        ...audienceClaims,
      };
      fileName = relativePath ? path.basename(relativePath) : PROJECT_FAVICON_FALLBACK_MARKER;
      break;
    }
  }

  const requiresSurfaceBinding = effectiveAssetClaimAudience(claims) === "private";
  if (
    requiresSurfaceBinding &&
    !input.clientCapabilities?.includes(ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY)
  ) {
    yield* Effect.logWarning(
      "Private asset URL issuance requires a client upgrade for same-origin relay support.",
      {
        "asset.outcome": "upgrade_required",
        "asset.required_capability": ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY,
        "asset.resource_kind": input.resource._tag,
      },
    );
    return yield* new AssetClientUpgradeRequiredError({
      resource: input.resource,
      requiredCapability: ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY,
    });
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
  let surfaceSessionId: AuthSessionId | null = null;
  let surfaceBindingId: string | null = null;
  let surfaceCredentialExpiresAt: number | null = null;
  if (requiresSurfaceBinding) {
    if (input.surfaceSessionId === undefined || input.surfaceSessionExpiresAt === undefined) {
      return yield* new AssetWorkspaceContextNotFoundError({ resource: input.resource });
    }
    surfaceSessionId = input.surfaceSessionId;
    surfaceBindingId = surfaceBindingIdForSession(surfaceSessionId, signingSecret);
    surfaceCredentialExpiresAt = input.surfaceSessionExpiresAt.epochMilliseconds;
    expiresAt = Math.min(expiresAt, surfaceCredentialExpiresAt);
    if (expiresAt <= now) {
      return yield* new AssetWorkspaceContextNotFoundError({ resource: input.resource });
    }
  }
  claims = { ...claims, surfaceBindingId, expiresAt };
  const encodedPayload = base64UrlEncode(encodeAssetClaims(claims));
  const token = signToken(encodedPayload, signingSecret);
  const surfaceCredential =
    surfaceBindingId === null || surfaceSessionId === null || surfaceCredentialExpiresAt === null
      ? null
      : signToken(
          base64UrlEncode(
            encodeAssetSurfaceCredentialClaims({
              version: 1,
              kind: "asset-surface",
              surfaceSessionId,
              surfaceBindingId,
              expiresAt: surfaceCredentialExpiresAt,
            }),
          ),
          signingSecret,
        );
  return {
    relativeUrl: `${surfaceBindingId === null ? ASSET_ROUTE_PREFIX : ASSET_SURFACE_RELAY_PREFIX}/${token}/${encodeURIComponent(fileName)}`,
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

  if (!canReadDataAudience(claims.issuingAudience, claims.dataAudience)) {
    return null;
  }

  const effectiveAudience = effectiveAssetClaimAudience(claims);
  if (claims.surfaceBindingId === null) {
    if (effectiveAudience === "private" || requestProof.allowUnbound !== true) return null;
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

  const authorizeResolvedPath = Effect.fn("AssetAccess.resolveAsset.authorizeResolvedPath")(
    function* (resolvedPath: string, signedWorkspaceRoot: string) {
      return yield* withOpenedAssetFile(resolvedPath, (opened, transferToResponse) =>
        Effect.gen(function* () {
          const path = yield* Path.Path;
          const relativeToSignedRoot = path.relative(signedWorkspaceRoot, opened.canonicalPath);
          if (
            relativeToSignedRoot === ".." ||
            relativeToSignedRoot.startsWith(`..${path.sep}`) ||
            path.isAbsolute(relativeToSignedRoot)
          ) {
            return null;
          }
          const liveDataAudience =
            (yield* ProjectFilesystemAudienceGuard.classifyCanonicalPathAudience(
              opened.canonicalPath,
            ).pipe(Effect.orElseSucceed(() => null))) ?? "private";
          if (!canReadDataAudience(claims.dataAudience, liveDataAudience)) {
            return null;
          }
          return transferToResponse();
        }),
      );
    },
  );

  if (claims.kind === "attachment") {
    const config = yield* ServerConfig.ServerConfig;
    const attachmentPath = resolveAttachmentPathById({
      attachmentsDir: config.attachmentsDir,
      attachmentId: claims.attachmentId,
    });
    if (!attachmentPath) return null;
    return yield* withOpenedAssetFile(attachmentPath, (opened, transferToResponse) =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const canonicalAttachmentsDir = yield* Effect.tryPromise(() =>
          NodeFSP.realpath(config.attachmentsDir),
        ).pipe(Effect.orElseSucceed(() => null));
        const attachmentRelativePath = canonicalAttachmentsDir
          ? path.relative(canonicalAttachmentsDir, opened.canonicalPath)
          : null;
        if (
          attachmentRelativePath === null ||
          attachmentRelativePath === "" ||
          attachmentRelativePath === ".." ||
          attachmentRelativePath.startsWith(`..${path.sep}`) ||
          path.isAbsolute(attachmentRelativePath)
        ) {
          return null;
        }

        const liveDataAudience =
          (yield* classifyAttachmentAudience(claims.attachmentId).pipe(
            Effect.orElseSucceed(() => null),
          )) ?? "private";
        if (!canReadDataAudience(claims.dataAudience, liveDataAudience)) {
          return null;
        }
        return transferToResponse();
      }),
    );
  }

  if (claims.kind === "project-favicon") {
    if (claims.relativePath === null) return null;
    const faviconPath = yield* resolveCanonicalWorkspaceFileForRequest({
      workspaceRoot: claims.workspaceRoot,
      relativePath: claims.relativePath,
    });
    return faviconPath ? yield* authorizeResolvedPath(faviconPath, claims.workspaceRoot) : null;
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
      ? yield* authorizeResolvedPath(exactWorkspaceFile, claims.workspaceRoot)
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
  return workspaceFile ? yield* authorizeResolvedPath(workspaceFile, claims.workspaceRoot) : null;
});

export const resolveLocalAssetRelay = Effect.fn("AssetAccess.resolveLocalAssetRelay")(
  function* (input: {
    readonly token: string;
    readonly encodedRelativePath: string;
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
        : yield* resolveAsset(input.token, input.encodedRelativePath, { allowUnbound: true });
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
    return yield* resolveAsset(input.token, input.encodedRelativePath, {
      surfaceCredentials: [relayProof],
    });
  },
);
