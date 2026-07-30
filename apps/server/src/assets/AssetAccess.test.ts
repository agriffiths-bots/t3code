import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY,
  AuthSessionId,
  EnvironmentId,
  ThreadId,
  type OrchestrationReadModel,
} from "@t3tools/contracts";
import { PROJECT_FAVICON_FALLBACK_MARKER } from "@t3tools/shared/projectFavicon";
import { describe, expect, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as TestClock from "effect/testing/TestClock";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectFaviconResolver from "../project/ProjectFaviconResolver.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import {
  ASSET_ROUTE_PREFIX,
  ASSET_SURFACE_RELAY_PREFIX,
  classifyAttachmentAudience,
  decodeAssetRelayRoutingClaim,
  issueAssetUrl as issueAssetUrlImpl,
  resolveAsset as resolveAssetImpl,
  resolveLocalAssetRelay,
  type AssetResolution,
} from "./AssetAccess.ts";

const TEST_SURFACE_SESSION_ID = AuthSessionId.make("asset-access-test-surface");
const TEST_SURFACE_SESSION_EXPIRES_AT = DateTime.makeUnsafe("2999-01-01T00:00:00.000Z");
const OTHER_SURFACE_SESSION_ID = AuthSessionId.make("asset-access-other-surface");
const TEST_ENVIRONMENT_ID = EnvironmentId.make("asset-access-environment");
const issuedSurfaceCredentialByToken = new Map<string, string>();

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
    surfaceSessionExpiresAt: TEST_SURFACE_SESSION_EXPIRES_AT,
  }).pipe(
    Effect.tap((result) =>
      Effect.sync(() => {
        const token = assetRouteSuffix(result.relativeUrl).split("/", 1)[0];
        if (token && result.surfaceCredential) {
          issuedSurfaceCredentialByToken.set(token, result.surfaceCredential);
        }
      }),
    ),
  );

const resolveAsset = (
  token: string,
  relativePath: string,
  requestProof: "private" | "factory" | string | null = null,
) =>
  resolveAssetImpl(
    token,
    relativePath,
    requestProof === "private"
      ? { surfaceCredentials: [issuedSurfaceCredentialByToken.get(token) ?? ""] }
      : requestProof === "factory"
        ? { allowUnbound: true }
        : typeof requestProof === "string"
          ? { surfaceCredentials: [requestProof] }
          : {},
  );

const summarizeAssetResolution = (resolution: AssetResolution | null) => {
  if (resolution?.kind !== "file") return resolution;
  resolution.stream.destroy();
  return { kind: resolution.kind, path: resolution.path };
};

const configLayer = ServerConfig.ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-asset-access-test-",
});
const emptyReadModel: OrchestrationReadModel = {
  snapshotSequence: 0,
  projects: [],
  threads: [],
  updatedAt: "1970-01-01T00:00:00.000Z",
};
const makeAssetAccessTestLayer = (
  getCommandReadModel: () => Effect.Effect<OrchestrationReadModel> = () =>
    Effect.succeed(emptyReadModel),
) =>
  Layer.mergeAll(
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
    Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
      getCommandReadModel,
    }),
  ).pipe(Layer.provideMerge(NodeServices.layer));
const testLayer = makeAssetAccessTestLayer();

const makeFactoryReadModel = (input: {
  readonly projectId: string;
  readonly threadId: string;
  readonly workspaceRoot: string;
}): OrchestrationReadModel =>
  ({
    snapshotSequence: 1,
    updatedAt: "2026-07-19T00:00:00.000Z",
    projects: [
      {
        id: input.projectId,
        workspaceRoot: input.workspaceRoot,
        dataAudience: "factory",
        deletedAt: null,
      },
    ],
    threads: [
      {
        id: input.threadId,
        projectId: input.projectId,
        dataAudience: "factory",
        deletedAt: null,
        worktreePath: null,
      },
    ],
  }) as unknown as OrchestrationReadModel;

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
      });
      const suffix = assetRouteSuffix(result.relativeUrl);
      const separatorIndex = suffix.indexOf("/");
      const token = suffix.slice(0, separatorIndex);

      expect(
        summarizeAssetResolution(yield* resolveAsset(token, "report.html", "private")),
      ).toEqual({ kind: "file", path: canonicalHtmlPath });
      expect(summarizeAssetResolution(yield* resolveAsset(token, "report.css", "private"))).toEqual(
        { kind: "file", path: canonicalCssPath },
      );
      expect(yield* resolveAsset(token, "../secret.txt", "private")).toBeNull();
      expect(yield* resolveAsset(token, ".env", "private")).toBeNull();
      expect(yield* resolveAsset(`${token}tampered`, "report.html", "private")).toBeNull();
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
      });
      const suffix = assetRouteSuffix(result.relativeUrl);
      const separatorIndex = suffix.indexOf("/");
      const token = suffix.slice(0, separatorIndex);

      expect(summarizeAssetResolution(yield* resolveAsset(token, "icon.png", "private"))).toEqual({
        kind: "file",
        path: canonicalImagePath,
      });
      expect(yield* resolveAsset(token, "other.png", "private")).toBeNull();
      expect(yield* resolveAsset(token, "../icon.png", "private")).toBeNull();
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("streams the same file handle that passed audience authorization", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-asset-handle-workspace-",
      });
      const outside = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-asset-handle-private-",
      });
      const entryPath = path.join(root, "report.html");
      const privatePath = path.join(outside, "private.html");
      yield* fileSystem.writeFileString(entryPath, "authorized contents");
      yield* fileSystem.writeFileString(privatePath, "private contents");

      const result = yield* issueAssetUrl({
        resource: {
          _tag: "workspace-file",
          threadId: ThreadId.make("thread-handle-race"),
          path: entryPath,
        },
        workspaceRoot: root,
      });
      const suffix = assetRouteSuffix(result.relativeUrl);
      const separatorIndex = suffix.indexOf("/");
      const resolution = yield* resolveAsset(
        suffix.slice(0, separatorIndex),
        suffix.slice(separatorIndex + 1),
        "private",
      );
      expect(resolution?.kind).toBe("file");
      if (!resolution || resolution.kind !== "file") return;

      yield* fileSystem.remove(entryPath);
      yield* fileSystem.symlink(privatePath, entryPath);
      const bytes = yield* Effect.tryPromise(async () => {
        const chunks: Buffer[] = [];
        for await (const chunk of resolution.stream) {
          chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        }
        return Buffer.concat(chunks).toString("utf8");
      });
      expect(bytes).toBe("authorized contents");
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("rejects assets after the signed workspace root is replaced", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const parent = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-asset-replaced-root-",
      });
      const root = path.join(parent, "workspace");
      const retiredRoot = path.join(parent, "retired-workspace");
      const replacementRoot = path.join(parent, "replacement");
      const entryPath = path.join(root, "report.html");
      yield* fileSystem.makeDirectory(root);
      yield* fileSystem.makeDirectory(replacementRoot);
      yield* fileSystem.writeFileString(entryPath, "authorized contents");
      yield* fileSystem.writeFileString(path.join(replacementRoot, "secret.css"), "private");

      const result = yield* issueAssetUrl({
        resource: {
          _tag: "workspace-file",
          threadId: ThreadId.make("thread-replaced-root"),
          path: entryPath,
        },
        workspaceRoot: root,
      });
      const suffix = assetRouteSuffix(result.relativeUrl);
      const separatorIndex = suffix.indexOf("/");

      yield* fileSystem.rename(root, retiredRoot);
      yield* fileSystem.symlink(replacementRoot, root);
      expect(
        yield* resolveAsset(suffix.slice(0, separatorIndex), "secret.css", "private"),
      ).toBeNull();
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("takes the strictest audience across lossy attachment thread id matches", () => {
    const readModel = {
      snapshotSequence: 1,
      updatedAt: "2026-07-19T00:00:00.000Z",
      projects: [
        { id: "project-factory", dataAudience: "factory", deletedAt: null },
        {
          id: "project-private",
          dataAudience: "private",
          deletedAt: "2026-07-19T00:00:00.000Z",
        },
      ],
      threads: [
        {
          id: "Thread.Foo",
          projectId: "project-private",
          dataAudience: "private",
          deletedAt: "2026-07-19T00:00:00.000Z",
        },
        {
          id: "thread-foo",
          projectId: "project-factory",
          dataAudience: "factory",
          deletedAt: null,
        },
      ],
    } as unknown as OrchestrationReadModel;
    const collisionProjectionLayer = Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
      getCommandReadModel: () => Effect.succeed(readModel),
    });

    return Effect.gen(function* () {
      expect(
        yield* classifyAttachmentAudience("thread-foo-00000000-0000-4000-8000-000000000003"),
      ).toBe("private");
    }).pipe(Effect.provide(collisionProjectionLayer));
  });

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
      });
      const suffix = assetRouteSuffix(result.relativeUrl);
      const separatorIndex = suffix.indexOf("/");
      const token = suffix.slice(0, separatorIndex);

      expect(
        summarizeAssetResolution(yield* resolveAsset(token, "ignored.png", "private")),
      ).toEqual({ kind: "file", path: attachmentPath });
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("rejects audience claims that exceed their issuing ceiling", () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const attachmentId = "thread-private-00000000-0000-4000-8000-000000000002";
      const attachmentPath = path.join(config.attachmentsDir, `${attachmentId}.png`);
      yield* fileSystem.makeDirectory(config.attachmentsDir, { recursive: true });
      yield* fileSystem.writeFile(attachmentPath, new Uint8Array([1, 2, 3]));

      const error = yield* issueAssetUrl({
        resource: { _tag: "attachment", attachmentId },
        dataAudience: "private",
        issuingAudience: "factory",
      }).pipe(Effect.flip);
      expect(error._tag).toBe("AssetWorkspaceContextNotFoundError");

      const privateResult = yield* issueAssetUrl({
        resource: { _tag: "attachment", attachmentId },
        dataAudience: "private",
        issuingAudience: "private",
      });
      const privateSuffix = assetRouteSuffix(privateResult.relativeUrl);
      const privateSeparatorIndex = privateSuffix.indexOf("/");
      const privateToken = privateSuffix.slice(0, privateSeparatorIndex);
      const privateFileName = privateSuffix.slice(privateSeparatorIndex + 1);
      expect(yield* resolveAsset(privateToken, privateFileName)).toBeNull();
      expect(yield* resolveAsset(privateToken, privateFileName, "factory")).toBeNull();
      expect(
        summarizeAssetResolution(yield* resolveAsset(privateToken, privateFileName, "private")),
      ).toEqual({ kind: "file", path: attachmentPath });
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("binds private capabilities to the surface session that minted them", () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const attachmentId = "thread-surface-00000000-0000-4000-8000-000000000004";
      const attachmentPath = path.join(config.attachmentsDir, `${attachmentId}.png`);
      yield* fileSystem.makeDirectory(config.attachmentsDir, { recursive: true });
      yield* fileSystem.writeFile(attachmentPath, new Uint8Array([1, 2, 3]));
      const sessionExpiresAt = DateTime.makeUnsafe((yield* Clock.currentTimeMillis) + 5 * 60_000);

      const owner = yield* issueAssetUrlImpl({
        resource: { _tag: "attachment", attachmentId },
        clientCapabilities: [ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY],
        surfaceSessionId: AuthSessionId.make("surface-owner"),
        surfaceSessionExpiresAt: sessionExpiresAt,
      });
      const other = yield* issueAssetUrlImpl({
        resource: { _tag: "attachment", attachmentId },
        clientCapabilities: [ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY],
        surfaceSessionId: AuthSessionId.make("surface-other"),
        surfaceSessionExpiresAt: sessionExpiresAt,
      });
      const suffix = assetRouteSuffix(owner.relativeUrl);
      const separatorIndex = suffix.indexOf("/");
      const token = suffix.slice(0, separatorIndex);
      const fileName = suffix.slice(separatorIndex + 1);

      expect(yield* resolveAssetImpl(token, fileName)).toBeNull();
      expect(
        yield* resolveAssetImpl(token, fileName, {
          surfaceCredentials: [other.surfaceCredential ?? ""],
        }),
      ).toBeNull();
      expect(
        summarizeAssetResolution(
          yield* resolveAssetImpl(token, fileName, {
            surfaceCredentials: [owner.surfaceCredential ?? ""],
          }),
        ),
      ).toEqual({ kind: "file", path: attachmentPath });
      expect(owner.expiresAt).toBe(sessionExpiresAt.epochMilliseconds);

      expect(
        yield* resolveAssetImpl(token, fileName, {
          surfaceCredentials: [owner.surfaceCredential ?? ""],
        }).pipe(
          Effect.provide(
            Layer.mock(SessionStore.SessionStore)({
              cookieName: "t3_asset_access_revoked_test",
              isActive: () => Effect.succeed(false),
            }),
          ),
        ),
      ).toBeNull();
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("issues a stable surface credential across asset refreshes in one session", () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const attachmentId = "thread-surface-stable-00000000-0000-4000-8000-000000000005";
      const attachmentPath = path.join(config.attachmentsDir, `${attachmentId}.png`);
      yield* fileSystem.makeDirectory(config.attachmentsDir, { recursive: true });
      yield* fileSystem.writeFile(attachmentPath, new Uint8Array([1, 2, 3]));
      const sessionId = AuthSessionId.make("surface-stable");
      const sessionExpiresAt = DateTime.makeUnsafe(
        (yield* Clock.currentTimeMillis) + 2 * 60 * 60_000,
      );

      const first = yield* issueAssetUrlImpl({
        resource: { _tag: "attachment", attachmentId },
        clientCapabilities: [ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY],
        surfaceSessionId: sessionId,
        surfaceSessionExpiresAt: sessionExpiresAt,
      });
      yield* TestClock.adjust("1 minute");
      const refreshed = yield* issueAssetUrlImpl({
        resource: { _tag: "attachment", attachmentId },
        clientCapabilities: [ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY],
        surfaceSessionId: sessionId,
        surfaceSessionExpiresAt: sessionExpiresAt,
      });

      expect(refreshed.expiresAt).toBeGreaterThan(first.expiresAt);
      expect(refreshed.surfaceCredential).toBe(first.surfaceCredential);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("issues factory workspace URLs with relay routing metadata", () => {
    let workspaceRoot = process.cwd();
    const factoryLayer = makeAssetAccessTestLayer(() =>
      Effect.sync(() =>
        makeFactoryReadModel({
          projectId: "project-factory-workspace",
          threadId: "thread-factory-workspace",
          workspaceRoot,
        }),
      ),
    );
    return Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-asset-factory-workspace-",
      });
      workspaceRoot = root;
      const htmlPath = path.join(root, "report.html");
      const cssPath = path.join(root, "report.css");
      const percentCssPath = path.join(root, "theme%20.css");
      yield* fileSystem.writeFileString(htmlPath, '<link rel="stylesheet" href="report.css">');
      yield* fileSystem.writeFileString(cssPath, "body { color: red; }");
      yield* fileSystem.writeFileString(percentCssPath, "body { color: blue; }");
      const canonicalHtmlPath = yield* fileSystem.realPath(htmlPath);
      const canonicalCssPath = yield* fileSystem.realPath(cssPath);
      const canonicalPercentCssPath = yield* fileSystem.realPath(percentCssPath);

      const result = yield* issueAssetUrl({
        resource: {
          _tag: "workspace-file",
          threadId: ThreadId.make("thread-factory-workspace"),
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
        issuingBackendId: TEST_ENVIRONMENT_ID,
      });
      expect(
        summarizeAssetResolution(yield* resolveAsset(token, "report.html", "factory")),
      ).toEqual({
        kind: "file",
        path: canonicalHtmlPath,
      });
      expect(summarizeAssetResolution(yield* resolveAsset(token, "report.css", "factory"))).toEqual(
        {
          kind: "file",
          path: canonicalCssPath,
        },
      );
      expect(
        summarizeAssetResolution(yield* resolveAsset(token, "theme%2520.css", "factory")),
      ).toEqual({ kind: "file", path: canonicalPercentCssPath });
      expect(yield* resolveAsset(token, "broken%ZZ.css", "factory")).toBeNull();
      expect(
        summarizeAssetResolution(
          yield* resolveLocalAssetRelay({
            token,
            encodedRelativePath: "report.html",
            viewerSessionId: OTHER_SURFACE_SESSION_ID,
            viewerAudienceCeiling: "factory",
          }),
        ),
      ).toEqual({ kind: "file", path: canonicalHtmlPath });
    }).pipe(Effect.provide(factoryLayer));
  });

  it.effect("masks nested private workspace assets for directory-scoped factory tokens", () => {
    let workspaceRoot = process.cwd();
    let nestedPrivateRoot = process.cwd();
    const factoryLayer = makeAssetAccessTestLayer(() =>
      Effect.succeed({
        snapshotSequence: 1,
        updatedAt: "2026-07-19T00:00:00.000Z",
        projects: [
          {
            id: "project-factory-nested",
            workspaceRoot,
            dataAudience: "factory",
            deletedAt: null,
          },
          {
            id: "project-private-nested",
            workspaceRoot: nestedPrivateRoot,
            dataAudience: "private",
            deletedAt: null,
          },
        ],
        threads: [
          {
            id: "thread-factory-nested-private",
            projectId: "project-factory-nested",
            dataAudience: "factory",
            deletedAt: null,
            worktreePath: null,
          },
        ],
      } as unknown as OrchestrationReadModel),
    );

    return Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-asset-nested-private-",
      });
      const privateRoot = path.join(root, "private-project");
      workspaceRoot = root;
      nestedPrivateRoot = privateRoot;
      yield* fileSystem.makeDirectory(privateRoot, { recursive: true });
      const htmlPath = path.join(root, "report.html");
      const secretCssPath = path.join(privateRoot, "secret.css");
      yield* fileSystem.writeFileString(htmlPath, "<html></html>");
      yield* fileSystem.writeFileString(secretCssPath, "body { color: black; }");
      const canonicalHtmlPath = yield* fileSystem.realPath(htmlPath);

      const result = yield* issueAssetUrl({
        resource: {
          _tag: "workspace-file",
          threadId: ThreadId.make("thread-factory-nested-private"),
          path: htmlPath,
        },
        workspaceRoot: root,
        dataAudience: "factory",
        issuingAudience: "factory",
      });
      const suffix = assetRouteSuffix(result.relativeUrl);
      const separatorIndex = suffix.indexOf("/");
      const token = suffix.slice(0, separatorIndex);

      expect(
        summarizeAssetResolution(yield* resolveAsset(token, "report.html", "factory")),
      ).toEqual({ kind: "file", path: canonicalHtmlPath });
      expect(yield* resolveAsset(token, "private-project/secret.css", "factory")).toBeNull();
      expect(yield* resolveAsset(token, "private-project/missing.css", "factory")).toBeNull();
    }).pipe(Effect.provide(factoryLayer));
  });

  it.effect("issues factory attachment capabilities with relay routing metadata", () => {
    const factoryLayer = makeAssetAccessTestLayer(() =>
      Effect.succeed(
        makeFactoryReadModel({
          projectId: "project-factory",
          threadId: "thread-factory",
          workspaceRoot: process.cwd(),
        }),
      ),
    );
    return Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const attachmentId = "thread-factory-00000000-0000-4000-8000-000000000006";
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
        kind: "attachment",
        dataAudience: "factory",
        issuingAudience: "factory",
        issuingBackendId: TEST_ENVIRONMENT_ID,
      });
      expect(
        summarizeAssetResolution(yield* resolveAsset(token, "ignored.png", "factory")),
      ).toEqual({
        kind: "file",
        path: attachmentPath,
      });
      expect(yield* resolveAssetImpl(token, "ignored.png")).toBeNull();
      expect(
        summarizeAssetResolution(
          yield* resolveLocalAssetRelay({
            token,
            encodedRelativePath: "ignored.png",
            viewerSessionId: OTHER_SURFACE_SESSION_ID,
            viewerAudienceCeiling: "factory",
          }),
        ),
      ).toEqual({ kind: "file", path: attachmentPath });
      expect(result.relativeUrl).not.toContain(TEST_SURFACE_SESSION_ID);
      expect(result.surfaceCredential ?? null).toBeNull();
    }).pipe(Effect.provide(factoryLayer));
  });

  it.effect("preserves signed direct URLs for factory web clients without relay capability", () => {
    const factoryLayer = makeAssetAccessTestLayer(() =>
      Effect.succeed(
        makeFactoryReadModel({
          projectId: "project-old-client",
          threadId: "thread-old-client",
          workspaceRoot: process.cwd(),
        }),
      ),
    );
    return Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const attachmentId = "thread-old-client-00000000-0000-4000-8000-000000000001";
      const attachmentPath = path.join(config.attachmentsDir, `${attachmentId}.png`);
      yield* fileSystem.makeDirectory(config.attachmentsDir, { recursive: true });
      yield* fileSystem.writeFile(attachmentPath, new Uint8Array([1, 2, 3]));

      const result = yield* issueAssetUrlImpl({
        resource: { _tag: "attachment", attachmentId },
        dataAudience: "factory",
        issuingAudience: "factory",
      });
      const suffix = assetRouteSuffix(result.relativeUrl);
      const separatorIndex = suffix.indexOf("/");
      const token = suffix.slice(0, separatorIndex);

      expect(result.relativeUrl).toMatch(/^\/api\/assets\/(?!relay\/)/);
      expect(result.surfaceCredential ?? null).toBeNull();
      expect(yield* resolveAssetImpl(token, "ignored.png")).toBeNull();
      expect(
        summarizeAssetResolution(
          yield* resolveAssetImpl(token, "ignored.png", { allowUnbound: true }),
        ),
      ).toEqual({ kind: "file", path: attachmentPath });
    }).pipe(Effect.provide(factoryLayer));
  });

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
      });
      const faviconSuffix = assetRouteSuffix(faviconResult.relativeUrl);
      const faviconSeparatorIndex = faviconSuffix.indexOf("/");
      expect(
        decodeAssetRelayRoutingClaim(faviconSuffix.slice(0, faviconSeparatorIndex)),
      ).toMatchObject({ kind: "project-favicon" });
      expect(
        summarizeAssetResolution(
          yield* resolveAsset(
            faviconSuffix.slice(0, faviconSeparatorIndex),
            faviconSuffix.slice(faviconSeparatorIndex + 1),
            "private",
          ),
        ),
      ).toEqual({ kind: "file", path: canonicalFaviconPath });

      yield* fileSystem.remove(faviconPath);
      const fallbackResult = yield* issueAssetUrl({
        resource: { _tag: "project-favicon", cwd: root },
      });
      expect(fallbackResult.relativeUrl.endsWith(`/${PROJECT_FAVICON_FALLBACK_MARKER}`)).toBe(true);
      const fallbackSuffix = assetRouteSuffix(fallbackResult.relativeUrl);
      const fallbackSeparatorIndex = fallbackSuffix.indexOf("/");
      expect(
        yield* resolveAsset(
          fallbackSuffix.slice(0, fallbackSeparatorIndex),
          fallbackSuffix.slice(fallbackSeparatorIndex + 1),
          "private",
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
