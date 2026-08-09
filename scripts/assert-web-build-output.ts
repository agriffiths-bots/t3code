// @effect-diagnostics nodeBuiltinImport:off - Standalone build-output assertion serves local dist before an Effect runtime exists.
// @effect-diagnostics globalFetch:off - Standalone assertion fetches its local loopback static server.
// @effect-diagnostics globalConsole:off - Build script prints a one-line CI success marker.
import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const repoRoot = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const distDir = NodePath.resolve(process.argv[2] ?? "apps/web/dist");
const requiredFiles = ["index.html", "manifest.webmanifest", "sw.js"] as const;
const manifestAuthCookieName = "t3_pwa_manifest_auth";
const manifestAuthCookieValue = "ok";
const chromeChannel = process.env.T3_UI_CHROME_CHANNEL?.trim() || "chrome";
const pwaInstallabilityCheckMode =
  process.env.T3CODE_PWA_INSTALLABILITY_CHECK?.trim().toLowerCase() ?? "auto";

type StaticServerOptions = {
  readonly requireManifestCookie?: boolean;
};

type BrowserCookie = {
  readonly name: string;
  readonly value: string;
  readonly url: string;
};

type ConsoleMessageLike = {
  readonly type: () => string;
  readonly text: () => string;
};

type PageLike = {
  readonly goto: (
    url: string,
    options?: {
      readonly timeout?: number;
      readonly waitUntil?: "commit" | "domcontentloaded" | "load" | "networkidle";
    },
  ) => Promise<unknown>;
  readonly evaluate: <T>(expression: string) => Promise<T>;
  readonly on: (event: "console", listener: (message: ConsoleMessageLike) => void) => void;
  readonly url: () => string;
};

type CdpSessionLike = {
  readonly send: <T>(method: string, params?: Record<string, unknown>) => Promise<T>;
};

type BrowserContextLike = {
  readonly addCookies: (cookies: ReadonlyArray<BrowserCookie>) => Promise<void>;
  readonly close: () => Promise<void>;
  readonly newCDPSession: (page: PageLike) => Promise<CdpSessionLike>;
  readonly newPage: () => Promise<PageLike>;
  readonly pages: () => ReadonlyArray<PageLike>;
};

type ChromiumLauncher = {
  readonly launchPersistentContext: (
    userDataDir: string,
    options: {
      readonly channel: string;
      readonly hasTouch: boolean;
      readonly headless: boolean;
      readonly isMobile: boolean;
      readonly viewport: {
        readonly height: number;
        readonly width: number;
      };
    },
  ) => Promise<BrowserContextLike>;
};

type ChromiumLoadResult =
  | {
      readonly _tag: "Loaded";
      readonly chromium: ChromiumLauncher;
      readonly packagePath: string;
    }
  | {
      readonly _tag: "Unavailable";
      readonly reason: string;
    };

type AppManifestResult = {
  readonly data?: string;
  readonly errors?: ReadonlyArray<unknown>;
  readonly manifest?: {
    readonly display?: string;
    readonly name?: string;
    readonly shortName?: string;
    readonly startUrl?: string;
  };
  readonly url?: string;
};

type InstallabilityError = {
  readonly errorArguments?: ReadonlyArray<unknown>;
  readonly errorId?: string;
};

type InstallabilityErrorsResult = {
  readonly installabilityErrors?: ReadonlyArray<InstallabilityError>;
};

const installableDisplayModes = new Set(["standalone", "minimal-ui"]);

function fail(message: string): never {
  throw new Error(`web build output assertion failed: ${message}`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isChromiumModule(value: unknown): value is { readonly chromium: ChromiumLauncher } {
  return (
    isRecord(value) &&
    isRecord(value.chromium) &&
    typeof value.chromium.launchPersistentContext === "function"
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function chromiumPackageCandidates(): string[] {
  const configuredPath = process.env.T3CODE_PLAYWRIGHT_CORE_PATH?.trim();
  return [
    ...(configuredPath ? [configuredPath] : []),
    NodePath.join(repoRoot, "apps/desktop/node_modules/playwright-core"),
    NodePath.join(repoRoot, "scripts/node_modules/playwright-core"),
    NodePath.join(repoRoot, "node_modules/playwright-core"),
  ];
}

function tryLoadChromium(): ChromiumLoadResult {
  const require = NodeModule.createRequire(import.meta.url);
  const failures: string[] = [];
  for (const packagePath of chromiumPackageCandidates()) {
    let loaded: unknown;
    try {
      loaded = require(packagePath);
    } catch (error) {
      failures.push(`${packagePath}: ${errorMessage(error)}`);
      continue;
    }
    if (!isChromiumModule(loaded)) {
      failures.push(`${packagePath}: did not expose a Chromium launcher`);
      continue;
    }
    return { _tag: "Loaded", chromium: loaded.chromium, packagePath };
  }
  return { _tag: "Unavailable", reason: failures.join("; ") };
}

async function readRequiredFile(relativePath: string): Promise<Buffer> {
  const filePath = NodePath.resolve(distDir, relativePath);
  if (!filePath.startsWith(`${distDir}${NodePath.sep}`) && filePath !== distDir) {
    fail(`invalid required path ${relativePath}`);
  }

  const info = await NodeFSP.stat(filePath).catch(() => null);
  if (!info?.isFile()) {
    fail(`missing ${relativePath}`);
  }
  return await NodeFSP.readFile(filePath);
}

function contentTypeFor(filePath: string): string {
  if (filePath.endsWith(".html")) return "text/html; charset=utf-8";
  if (filePath.endsWith(".js")) return "application/javascript; charset=utf-8";
  if (filePath.endsWith(".webmanifest")) return "application/manifest+json; charset=utf-8";
  if (filePath.endsWith(".png")) return "image/png";
  return "application/octet-stream";
}

async function resolveStaticFile(request: NodeHttp.IncomingMessage): Promise<string | null> {
  const requestUrl = new URL(request.url ?? "/", "http://127.0.0.1");
  const requestPath = requestUrl.pathname === "/" ? "/index.html" : requestUrl.pathname;
  const rawRelativePath = decodeURIComponent(requestPath).replace(/^[/\\]+/, "");
  const relativePath = NodePath.normalize(rawRelativePath).replace(/^[/\\]+/, "");
  if (
    relativePath.length === 0 ||
    rawRelativePath.startsWith("..") ||
    relativePath.startsWith("..") ||
    relativePath.includes("\0")
  ) {
    return null;
  }

  const filePath = NodePath.resolve(distDir, relativePath);
  const withinDist = filePath === distDir || filePath.startsWith(`${distDir}${NodePath.sep}`);
  if (!withinDist) {
    return null;
  }

  const info = await NodeFSP.stat(filePath).catch(() => null);
  return info?.isFile() ? filePath : NodePath.resolve(distDir, "index.html");
}

async function handleStaticRequest(
  request: NodeHttp.IncomingMessage,
  response: NodeHttp.ServerResponse,
  options: StaticServerOptions = {},
) {
  const requestUrl = new URL(request.url ?? "/", "http://127.0.0.1");
  if (
    options.requireManifestCookie &&
    requestUrl.pathname === "/manifest.webmanifest" &&
    !requestHasCookie(request, manifestAuthCookieName, manifestAuthCookieValue)
  ) {
    response.writeHead(401, { "content-type": "text/plain; charset=utf-8" });
    response.end("Manifest requires credentials");
    return;
  }

  const filePath = await resolveStaticFile(request);
  if (!filePath) {
    response.writeHead(400, { "content-type": "text/plain; charset=utf-8" });
    response.end("Bad Request");
    return;
  }

  const body = await NodeFSP.readFile(filePath);
  response.writeHead(200, { "content-type": contentTypeFor(filePath) });
  response.end(body);
}

function requestHasCookie(
  request: NodeHttp.IncomingMessage,
  cookieName: string,
  cookieValue: string,
): boolean {
  const cookieHeader = request.headers.cookie ?? "";
  for (const cookie of cookieHeader.split(";")) {
    const [rawName, ...rawValueParts] = cookie.trim().split("=");
    if (rawName === cookieName && rawValueParts.join("=") === cookieValue) {
      return true;
    }
  }
  return false;
}

async function withStaticServer<T>(
  run: (baseUrl: string) => Promise<T>,
  options: StaticServerOptions = {},
): Promise<T> {
  const server = NodeHttp.createServer((request, response) => {
    void handleStaticRequest(request, response, options).catch((error: unknown) => {
      response.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
      response.end(error instanceof Error ? error.message : "Internal Server Error");
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    fail("static assertion server did not bind a TCP port");
  }

  try {
    return await run(`http://127.0.0.1:${address.port}`);
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  }
}

function formatInstallabilityErrors(errors: ReadonlyArray<InstallabilityError>): string {
  return errors
    .map((error) => {
      const argumentsText =
        error.errorArguments && error.errorArguments.length > 0
          ? ` ${JSON.stringify(error.errorArguments)}`
          : "";
      return `${error.errorId ?? "unknown"}${argumentsText}`;
    })
    .join(", ");
}

function assertInstallableManifestData(manifestData: string, baseUrl: string): void {
  let manifest: unknown;
  try {
    manifest = JSON.parse(manifestData);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    fail(`Chrome loaded invalid manifest JSON from ${baseUrl}: ${message}`);
  }

  if (!isRecord(manifest)) {
    fail(`Chrome loaded a non-object manifest from ${baseUrl}`);
  }

  const name = manifest.name;
  const shortName = manifest.short_name;
  if (
    (typeof name !== "string" || name.trim().length === 0) &&
    (typeof shortName !== "string" || shortName.trim().length === 0)
  ) {
    fail(`Chrome loaded manifest without name or short_name from ${baseUrl}`);
  }

  const display = manifest.display;
  if (typeof display !== "string" || !installableDisplayModes.has(display)) {
    fail(`Chrome loaded manifest with non-installable display mode ${JSON.stringify(display)}`);
  }
}

function shouldRequireCredentialedManifestInstallability(): boolean {
  if (pwaInstallabilityCheckMode === "required") return true;
  if (pwaInstallabilityCheckMode === "skip") return false;
  return (
    (process.env.CI === "true" || process.env.GITHUB_ACTIONS === "true") && !process.env.VERCEL
  );
}

function isBuildAssertionFailure(error: unknown): boolean {
  return errorMessage(error).startsWith("web build output assertion failed:");
}

async function assertCredentialedManifestInstallability(
  baseUrl: string,
  chromium: ChromiumLauncher,
): Promise<void> {
  const userDataDir = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "t3-pwa-installability-"),
  );
  let context: BrowserContextLike | null = null;
  const consoleErrors: string[] = [];

  try {
    context = await chromium.launchPersistentContext(userDataDir, {
      channel: chromeChannel,
      hasTouch: true,
      headless: true,
      isMobile: true,
      viewport: { height: 915, width: 412 },
    });
    await context.addCookies([
      {
        name: manifestAuthCookieName,
        value: manifestAuthCookieValue,
        url: baseUrl,
      },
    ]);

    const page = context.pages()[0] ?? (await context.newPage());
    page.on("console", (message) => {
      if (message.type() === "error" || message.type() === "warning") {
        consoleErrors.push(message.text());
      }
    });
    const cdp = await context.newCDPSession(page);
    await cdp.send<unknown>("Page.enable");
    await page.goto(baseUrl, { timeout: 30_000, waitUntil: "domcontentloaded" });
    await page.evaluate<true>(`
      "serviceWorker" in navigator
        ? (async () => {
            await navigator.serviceWorker.register("/sw.js", { scope: "/" });
            return await Promise.race([
              navigator.serviceWorker.ready.then(() => true),
              new Promise((_, reject) =>
                setTimeout(() => reject(new Error("service worker ready timeout")), 15000),
              ),
            ]);
          })()
        : Promise.reject(new Error("service workers are unavailable"))
    `);

    const manifest = await cdp.send<AppManifestResult>("Page.getAppManifest");
    const installability = await cdp.send<InstallabilityErrorsResult>(
      "Page.getInstallabilityErrors",
    );
    const installabilityErrors = installability.installabilityErrors ?? [];
    if (installabilityErrors.length > 0) {
      fail(
        `Chrome reported PWA installability errors with a credential-protected manifest: ${formatInstallabilityErrors(installabilityErrors)}`,
      );
    }
    if (manifest.errors && manifest.errors.length > 0) {
      fail(`Chrome reported manifest errors: ${JSON.stringify(manifest.errors)}`);
    }
    if (!manifest.data || !manifest.data.includes('"name"')) {
      fail(
        `Chrome did not load the credential-protected manifest data from ${baseUrl}; console=${JSON.stringify(consoleErrors)}`,
      );
    }
    assertInstallableManifestData(manifest.data, baseUrl);
  } finally {
    if (context) {
      await context.close().catch(() => undefined);
    }
    await NodeFSP.rm(userDataDir, { force: true, recursive: true });
  }
}

async function runCredentialedManifestInstallabilityCheck(baseUrl: string): Promise<void> {
  if (pwaInstallabilityCheckMode === "skip") {
    console.warn("Skipping credential-protected PWA installability check by configuration.");
    return;
  }

  const required = shouldRequireCredentialedManifestInstallability();
  const loaded = tryLoadChromium();
  if (loaded._tag === "Unavailable") {
    if (required) {
      fail(
        `credential-protected PWA installability check could not load Chromium: ${loaded.reason}`,
      );
    }
    console.warn(
      `Skipping credential-protected PWA installability check because Playwright is unavailable: ${loaded.reason}`,
    );
    return;
  }

  try {
    await assertCredentialedManifestInstallability(baseUrl, loaded.chromium);
  } catch (error) {
    if (required || isBuildAssertionFailure(error)) {
      throw error;
    }
    console.warn(
      `Skipping credential-protected PWA installability check from ${loaded.packagePath} because Chrome could not run: ${errorMessage(error)}`,
    );
  }
}

function assertNotHtml(pathname: string, contentType: string, body: string) {
  if (contentType.toLowerCase().includes("text/html")) {
    fail(`${pathname} was served as HTML (${contentType})`);
  }
  if (/^\s*<!doctype html\b/i.test(body) || /^\s*<html\b/i.test(body)) {
    fail(`${pathname} body is the SPA HTML document`);
  }
}

for (const file of requiredFiles) {
  await readRequiredFile(file);
}

const indexSource = (await readRequiredFile("index.html")).toString("utf8");
if (
  !indexSource.includes('rel="manifest" href="/manifest.webmanifest" crossorigin="use-credentials"')
) {
  fail('index.html manifest link must use crossorigin="use-credentials"');
}

const workerSource = (await readRequiredFile("sw.js")).toString("utf8");
assertNotHtml("sw.js", "application/javascript", workerSource);
if (!workerSource.includes('self.addEventListener("fetch"')) {
  fail("sw.js does not contain the app-shell fetch handler");
}

const manifestSource = (await readRequiredFile("manifest.webmanifest")).toString("utf8");
assertNotHtml("manifest.webmanifest", "application/manifest+json", manifestSource);
JSON.parse(manifestSource);

await withStaticServer(async (baseUrl) => {
  for (const pathname of ["/sw.js", "/manifest.webmanifest"] as const) {
    const response = await fetch(`${baseUrl}${pathname}`);
    if (!response.ok) {
      fail(`${pathname} returned HTTP ${response.status}`);
    }
    const contentType = response.headers.get("content-type") ?? "";
    const body = await response.text();
    assertNotHtml(pathname, contentType, body);
  }
});

await withStaticServer(
  async (baseUrl) => {
    await runCredentialedManifestInstallabilityCheck(baseUrl);
  },
  { requireManifestCookie: true },
);

console.log("web build output assertion passed");
