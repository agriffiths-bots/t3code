const CACHE_VERSION = "t3-code-v2";
const APP_SHELL_URLS = ["/", "/manifest.webmanifest", "/pwa-icon-192.png", "/pwa-icon-512.png"];
const BUILD_ASSET_PATH_PREFIX = "/assets/";

function isBuildAssetUrl(url) {
  return url.origin === self.location.origin && url.pathname.startsWith(BUILD_ASSET_PATH_PREFIX);
}

function isUsableBuildAssetResponse(response) {
  const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
  return response.ok && !contentType.includes("text/html");
}

function isUsableShellResponse(response) {
  const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
  return response.ok && contentType.includes("text/html");
}

function assetUnavailableResponse() {
  return new Response("T3 Code asset unavailable while offline.", {
    status: 503,
    headers: { "content-type": "text/plain; charset=utf-8" },
  });
}

function shellUnavailableResponse() {
  return new Response("T3 Code is offline. Reconnect to continue.", {
    status: 503,
    headers: { "content-type": "text/plain; charset=utf-8" },
  });
}

async function cachedShellOrUnavailable() {
  const cached = await caches.match("/");
  return cached ?? shellUnavailableResponse();
}

function extractBuildAssetUrls(html) {
  const urls = new Set();
  const attributePattern = /\b(?:href|src)="([^"]+)"/g;
  for (const match of html.matchAll(attributePattern)) {
    const assetUrl = new URL(match[1], self.location.origin);
    if (isBuildAssetUrl(assetUrl)) {
      urls.add(assetUrl.href);
    }
  }
  return [...urls];
}

async function cacheRequiredBuildAsset(cache, url) {
  const response = await fetch(url);
  if (!isUsableBuildAssetResponse(response)) {
    throw new Error(`Build asset did not return a cacheable asset response: ${url}`);
  }
  await cache.put(url, response);
}

async function cacheShellAndBuildAssets(cache, shellResponse) {
  const shellForCache = shellResponse.clone();
  const assetUrls = extractBuildAssetUrls(await shellResponse.text());
  await Promise.all(assetUrls.map((url) => cacheRequiredBuildAsset(cache, url)));
  await cache.put("/", shellForCache);
}

async function cacheBuildAssetsFromShell(cache) {
  const shell = await cache.match("/");
  if (!shell) return;

  const assetUrls = extractBuildAssetUrls(await shell.clone().text());
  await Promise.all(assetUrls.map((url) => cacheRequiredBuildAsset(cache, url)));
}

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches
      .open(CACHE_VERSION)
      .then(async (cache) => {
        await cache.addAll(APP_SHELL_URLS);
        await cacheBuildAssetsFromShell(cache);
      })
      .then(() => self.skipWaiting()),
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) =>
        Promise.all(keys.filter((key) => key !== CACHE_VERSION).map((key) => caches.delete(key))),
      )
      .then(() => self.clients.claim()),
  );
});

self.addEventListener("fetch", (event) => {
  const request = event.request;
  if (request.method !== "GET") return;

  const requestUrl = new URL(request.url);
  if (isBuildAssetUrl(requestUrl)) {
    const networkResponse = fetch(request);
    const cacheRefresh = networkResponse
      .then(async (response) => {
        if (!isUsableBuildAssetResponse(response)) return;

        const assetResponse = response.clone();
        const cache = await caches.open(CACHE_VERSION);
        await cache.put(request, assetResponse);
      })
      .catch(() => undefined);

    event.waitUntil(cacheRefresh);
    event.respondWith(
      networkResponse
        .then(async (response) => {
          if (isUsableBuildAssetResponse(response)) return response;
          const cached = await caches.match(request);
          return cached ?? assetUnavailableResponse();
        })
        .catch(async () => {
          const cached = await caches.match(request);
          return cached ?? assetUnavailableResponse();
        }),
    );
    return;
  }

  const acceptsHtml = request.headers.get("accept")?.includes("text/html") ?? false;
  if (!acceptsHtml || requestUrl.origin !== self.location.origin) return;
  const canRefreshShellCache = requestUrl.pathname === "/";

  const networkResponse = fetch(request);
  const shellRefresh = networkResponse
    .then(async (response) => {
      if (!canRefreshShellCache || !isUsableShellResponse(response)) return;

      const shellResponse = response.clone();
      const cache = await caches.open(CACHE_VERSION);
      await cacheShellAndBuildAssets(cache, shellResponse);
    })
    .catch(() => undefined);

  event.waitUntil(shellRefresh);
  event.respondWith(
    networkResponse
      .then(async (response) => {
        if (canRefreshShellCache && !isUsableShellResponse(response)) {
          return await cachedShellOrUnavailable();
        }
        return response;
      })
      .catch(cachedShellOrUnavailable),
  );
});
