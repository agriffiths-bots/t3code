import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY,
  AuthSessionId,
  EnvironmentId,
  ThreadId,
} from "@t3tools/contracts";
import { PROJECT_FAVICON_FALLBACK_MARKER } from "@t3tools/shared/projectFavicon";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectFaviconResolver from "../project/ProjectFaviconResolver.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import {
  ASSET_ROUTE_PREFIX,
  ASSET_SURFACE_RELAY_PREFIX,
  decodeAssetRelayRoutingClaim,
  issueAssetUrl as issueAssetUrlImpl,
  resolveAsset as resolveAssetImpl,
  resolveLocalAssetRelay,
} from "./AssetAccess.ts";

const TEST_SURFACE_SESSION_ID = AuthSessionId.make("asset-access-test-surface");
const OTHER_SURFACE_SESSION_ID = AuthSessionId.make("asset-access-other-surface");
const TEST_ENVIRONMENT_ID = EnvironmentId.make("asset-access-environment");

const assetRouteSuffix = (relativeUrl: string) => {
  const prefix = relativeUrl.startsWith(`${ASSET_SURFACE_RELAY_PREFIX}/`)
    ? ASSET_SURFACE_RELAY_PREFIX
    : ASSET_ROUTE_PREFIX;
  return relativeUrl.slice(`${prefix}/`.length);
};

const issueAssetUrl = (input: Parameters<typeof issueAssetUrlImpl>[0]) =>
  issueAssetUrlImpl({
    ...input,
    clientCapabilities: [ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY],
    surfaceSessionId: TEST_SURFACE_SESSION_ID,
  });

const resolveAsset = (
  token: string,
  relativePath: string,
  sessionId: AuthSessionId = TEST_SURFACE_SESSION_ID,
) => resolveAssetImpl(token, relativePath, { sessionId });

const configLayer = ServerConfig.ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-asset-access-test-",
});
const testLayer = Layer.mergeAll(
  configLayer,
  WorkspacePaths.layer,
  ProjectFaviconResolver.layer.pipe(Layer.provide(WorkspacePaths.layer)),
  ServerSecretStore.layer.pipe(Layer.provide(configLayer)),
  Layer.mock(SessionStore.SessionStore)({
    cookieName: "t3_asset_access_test",
    isActive: () => Effect.succeed(true),
  }),
  Layer.mock(ServerEnvironment.ServerEnvironment)({
    getEnvironmentId: Effect.succeed(TEST_ENVIRONMENT_ID),
    getDescriptor: Effect.die("unused test environment descriptor"),
  }),
).pipe(Layer.provideMerge(NodeServices.layer));

describe("AssetAccess", () => {
  it.effect("issues workspace URLs that resolve the entry file and sibling assets", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-asset-workspace-",
      });
      const htmlPath = path.join(root, "report.html");
      const cssPath = path.join(root, "report.css");
      yield* fileSystem.writeFileString(htmlPath, '<link rel="stylesheet" href="report.css">');
      yield* fileSystem.writeFileString(cssPath, "body { color: red; }");
      yield* fileSystem.writeFileString(path.join(root, ".env"), "SECRET=value");
      const canonicalHtmlPath = yield* fileSystem.realPath(htmlPath);
      const canonicalCssPath = yield* fileSystem.realPath(cssPath);

      const result = yield* issueAssetUrl({
        resource: {
          _tag: "workspace-file",
          threadId: ThreadId.make("thread-1"),
          path: htmlPath,
        },
        workspaceRoot: root,
        dataAudience: "factory",
        issuingAudience: "factory",
      });
      const suffix = assetRouteSuffix(result.relativeUrl);
      const separatorIndex = suffix.indexOf("/");
      const token = suffix.slice(0, separatorIndex);

      expect(decodeAssetRelayRoutingClaim(token)).toMatchObject({
        kind: "workspace-file",
        dataAudience: "factory",
        issuingAudience: "factory",
      });

      expect(yield* resolveAsset(token, "report.html")).toEqual({
        kind: "file",
        path: canonicalHtmlPath,
      });
      expect(yield* resolveAsset(token, "report.css")).toEqual({
        kind: "file",
        path: canonicalCssPath,
      });
      expect(yield* resolveAsset(token, "../secret.txt")).toBeNull();
      expect(yield* resolveAsset(token, ".env")).toBeNull();
      expect(yield* resolveAsset(`${token}tampered`, "report.html")).toBeNull();
      expect(
        yield* resolveLocalAssetRelay({
          token,
          relativePath: "report.html",
          viewerSessionId: OTHER_SURFACE_SESSION_ID,
          viewerAudienceCeiling: "factory",
        }),
      ).toEqual({ kind: "file", path: canonicalHtmlPath });
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("rejects workspace files outside the authorized root", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-asset-root-",
      });
      const outside = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-asset-outside-",
      });
      const htmlPath = path.join(outside, "report.html");
      yield* fileSystem.writeFileString(htmlPath, "<p>outside</p>");

      const error = yield* issueAssetUrl({
        resource: {
          _tag: "workspace-file",
          threadId: ThreadId.make("thread-1"),
          path: htmlPath,
        },
        workspaceRoot: root,
      }).pipe(Effect.flip);
      expect(error.message).toBe("Workspace file path must be relative to the project root.");
      expect(error).toMatchObject({
        _tag: "AssetWorkspacePathValidationError",
        resource: {
          _tag: "workspace-file",
          threadId: "thread-1",
          path: htmlPath,
        },
      });
      expect(error.cause).toBeInstanceOf(WorkspacePaths.WorkspacePathOutsideRootError);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("preserves non-missing canonical path failures when issuing asset URLs", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-asset-permission-root-",
      });
      const htmlPath = path.join(root, "report.html");
      yield* fileSystem.writeFileString(htmlPath, "<p>report</p>");
      const cause = PlatformError.systemError({
        _tag: "PermissionDenied",
        module: "FileSystem",
        method: "realPath",
        pathOrDescriptor: htmlPath,
      });
      const failingFileSystem = FileSystem.FileSystem.of({
        ...fileSystem,
        realPath: () => Effect.fail(cause),
      });

      const error = yield* issueAssetUrl({
        resource: {
          _tag: "workspace-file",
          threadId: ThreadId.make("thread-1"),
          path: htmlPath,
        },
        workspaceRoot: root,
      }).pipe(Effect.provideService(FileSystem.FileSystem, failingFileSystem), Effect.flip);

      expect(error.message).toBe("Failed to inspect the workspace asset.");
      expect(error).toMatchObject({
        _tag: "AssetWorkspaceAssetInspectionError",
        resource: {
          _tag: "workspace-file",
          threadId: "thread-1",
          path: htmlPath,
        },
      });
      expect(error.cause).toBe(cause);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("issues exact workspace URLs for image previews", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-asset-image-workspace-",
      });
      const assetsDirectory = path.join(root, "assets");
      const imagePath = path.join(assetsDirectory, "icon.png");
      const siblingPath = path.join(assetsDirectory, "other.png");
      yield* fileSystem.makeDirectory(assetsDirectory, { recursive: true });
      yield* fileSystem.writeFile(imagePath, new Uint8Array([137, 80, 78, 71]));
      yield* fileSystem.writeFile(siblingPath, new Uint8Array([137, 80, 78, 71]));
      const canonicalImagePath = yield* fileSystem.realPath(imagePath);

      const result = yield* issueAssetUrl({
        resource: {
          _tag: "workspace-file",
          threadId: ThreadId.make("thread-1"),
          path: imagePath,
        },
        workspaceRoot: root,
        dataAudience: "factory",
        issuingAudience: "factory",
      });
      const suffix = assetRouteSuffix(result.relativeUrl);
      const separatorIndex = suffix.indexOf("/");
      const token = suffix.slice(0, separatorIndex);

      expect(decodeAssetRelayRoutingClaim(token)).toMatchObject({
        kind: "workspace-file-exact",
        dataAudience: "factory",
        issuingAudience: "factory",
      });

      expect(yield* resolveAsset(token, "icon.png")).toEqual({
        kind: "file",
        path: canonicalImagePath,
      });
      expect(yield* resolveAsset(token, "other.png")).toBeNull();
      expect(yield* resolveAsset(token, "../icon.png")).toBeNull();
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("issues exact attachment capabilities by attachment id", () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const attachmentId = "thread-1-00000000-0000-4000-8000-000000000001";
      const attachmentPath = path.join(config.attachmentsDir, `${attachmentId}.png`);
      yield* fileSystem.makeDirectory(config.attachmentsDir, { recursive: true });
      yield* fileSystem.writeFile(attachmentPath, new Uint8Array([1, 2, 3]));

      const result = yield* issueAssetUrl({
        resource: { _tag: "attachment", attachmentId },
        dataAudience: "factory",
        issuingAudience: "factory",
      });
      const suffix = assetRouteSuffix(result.relativeUrl);
      const separatorIndex = suffix.indexOf("/");
      const token = suffix.slice(0, separatorIndex);

      expect(decodeAssetRelayRoutingClaim(token)).toMatchObject({
        dataAudience: "factory",
        issuingAudience: "factory",
        issuingBackendId: TEST_ENVIRONMENT_ID,
      });
      expect(yield* resolveAsset(token, "ignored.png")).toEqual({
        kind: "file",
        path: attachmentPath,
      });
      expect(yield* resolveAsset(token, "ignored.png", OTHER_SURFACE_SESSION_ID)).toBeNull();
      expect(yield* resolveAssetImpl(token, "ignored.png")).toBeNull();
      expect(
        yield* resolveLocalAssetRelay({
          token,
          relativePath: "ignored.png",
          viewerSessionId: OTHER_SURFACE_SESSION_ID,
          viewerAudienceCeiling: "factory",
        }),
      ).toEqual({ kind: "file", path: attachmentPath });
      expect(result.relativeUrl).not.toContain(TEST_SURFACE_SESSION_ID);
      expect(result.relativeUrl).not.toContain(result.surfaceCredential);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("preserves signed direct URLs for web clients without relay capability", () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const attachmentId = "thread-old-client-00000000-0000-4000-8000-000000000001";
      yield* fileSystem.makeDirectory(config.attachmentsDir, { recursive: true });
      yield* fileSystem.writeFile(
        path.join(config.attachmentsDir, `${attachmentId}.png`),
        new Uint8Array([1, 2, 3]),
      );

      const result = yield* issueAssetUrlImpl({
        resource: { _tag: "attachment", attachmentId },
        surfaceSessionId: TEST_SURFACE_SESSION_ID,
      });
      const suffix = assetRouteSuffix(result.relativeUrl);
      const separatorIndex = suffix.indexOf("/");
      const token = suffix.slice(0, separatorIndex);

      expect(result).toEqual({
        relativeUrl: expect.stringMatching(/^\/api\/assets\/(?!relay\/)/),
        expiresAt: expect.any(Number),
      });
      expect(result).not.toHaveProperty("surfaceCredential");
      expect(yield* resolveAssetImpl(token, "ignored.png")).toBeNull();
      expect(yield* resolveAssetImpl(token, "ignored.png", { allowUnbound: true })).toEqual({
        kind: "file",
        path: path.join(config.attachmentsDir, `${attachmentId}.png`),
      });
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("issues project favicon capabilities with a signed fallback", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-asset-favicon-",
      });
      const faviconPath = path.join(root, "favicon.svg");
      yield* fileSystem.writeFileString(faviconPath, "<svg />");
      const canonicalFaviconPath = yield* fileSystem.realPath(faviconPath);

      const faviconResult = yield* issueAssetUrl({
        resource: { _tag: "project-favicon", cwd: root },
        dataAudience: "factory",
        issuingAudience: "factory",
      });
      const faviconSuffix = assetRouteSuffix(faviconResult.relativeUrl);
      const faviconSeparatorIndex = faviconSuffix.indexOf("/");
      expect(
        decodeAssetRelayRoutingClaim(faviconSuffix.slice(0, faviconSeparatorIndex)),
      ).toMatchObject({
        kind: "project-favicon",
        dataAudience: "factory",
        issuingAudience: "factory",
      });
      expect(
        yield* resolveAsset(
          faviconSuffix.slice(0, faviconSeparatorIndex),
          faviconSuffix.slice(faviconSeparatorIndex + 1),
        ),
      ).toEqual({ kind: "file", path: canonicalFaviconPath });

      yield* fileSystem.remove(faviconPath);
      const fallbackResult = yield* issueAssetUrl({
        resource: { _tag: "project-favicon", cwd: root },
        dataAudience: "factory",
        issuingAudience: "factory",
      });
      expect(fallbackResult.relativeUrl.endsWith(`/${PROJECT_FAVICON_FALLBACK_MARKER}`)).toBe(true);
      const fallbackSuffix = assetRouteSuffix(fallbackResult.relativeUrl);
      const fallbackSeparatorIndex = fallbackSuffix.indexOf("/");
      expect(
        yield* resolveAsset(
          fallbackSuffix.slice(0, fallbackSeparatorIndex),
          fallbackSuffix.slice(fallbackSeparatorIndex + 1),
        ),
      ).toBeNull();
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("preserves structured project favicon resolution causes", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const root = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-asset-favicon-error-",
      });
      const platformCause = PlatformError.systemError({
        _tag: "PermissionDenied",
        module: "FileSystem",
        method: "stat",
      });
      const resolutionCause = new ProjectFaviconResolver.ProjectFaviconResolutionError({
        operation: "stat-candidate",
        workspaceRoot: root,
        relativePath: "favicon.svg",
        cause: platformCause,
      });
      const resolver = ProjectFaviconResolver.ProjectFaviconResolver.of({
        resolvePath: () => Effect.fail(resolutionCause),
      });

      const error = yield* issueAssetUrl({
        resource: { _tag: "project-favicon", cwd: root },
      }).pipe(
        Effect.provideService(ProjectFaviconResolver.ProjectFaviconResolver, resolver),
        Effect.flip,
      );

      expect(error.message).toBe("Failed to resolve project favicon.");
      expect(error._tag).toBe("AssetProjectFaviconResolutionError");
      expect(error.cause).toBe(resolutionCause);
    }).pipe(Effect.provide(testLayer)),
  );
});
