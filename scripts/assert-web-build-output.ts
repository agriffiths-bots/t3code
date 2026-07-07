// @effect-diagnostics nodeBuiltinImport:off - Standalone build-output assertion serves local dist before an Effect runtime exists.
// @effect-diagnostics globalFetch:off - Standalone assertion fetches its local loopback static server.
// @effect-diagnostics globalConsole:off - Build script prints a one-line CI success marker.
import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodePath from "node:path";

const distDir = NodePath.resolve(process.argv[2] ?? "apps/web/dist");
const requiredFiles = ["index.html", "manifest.webmanifest", "sw.js"] as const;

function fail(message: string): never {
  throw new Error(`web build output assertion failed: ${message}`);
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
) {
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

async function withStaticServer<T>(run: (baseUrl: string) => Promise<T>): Promise<T> {
  const server = NodeHttp.createServer((request, response) => {
    void handleStaticRequest(request, response).catch((error: unknown) => {
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

console.log("web build output assertion passed");
