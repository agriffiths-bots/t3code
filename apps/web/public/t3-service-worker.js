const CACHE_VERSION = "t3-code-v2";
const CACHE_PREFIX = "t3-code-";
const APP_SHELL_URLS = ["/", "/manifest.webmanifest", "/pwa-icon-192.png", "/pwa-icon-512.png"];
const DEFAULT_ACK_URL = "/api/notifications/ack";
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
  const cache = await caches.open(CACHE_VERSION);
  const cached = await cache.match("/");
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

async function deleteSupersededCaches() {
  const cacheKeys = await caches.keys();
  const clients = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
  // Open tabs can still request lazy chunks from the build that loaded them.
  const keepPriorAppCaches = clients.length > 0;
  await Promise.all(
    cacheKeys
      .filter((key) => key !== CACHE_VERSION)
      .filter((key) => !keepPriorAppCaches || !key.startsWith(CACHE_PREFIX))
      .map((key) => caches.delete(key)),
  );
}

self.addEventListener("activate", (event) => {
  event.waitUntil(deleteSupersededCaches().then(() => self.clients.claim()));
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

  const networkResponse = fetch(request);
  const shellRefresh = networkResponse
    .then(async (response) => {
      if (!isUsableShellResponse(response)) return;

      const shellResponse = response.clone();
      const cache = await caches.open(CACHE_VERSION);
      await cacheShellAndBuildAssets(cache, shellResponse);
    })
    .catch(() => undefined);
  const cacheCleanup = deleteSupersededCaches().catch(() => undefined);

  event.waitUntil(Promise.all([shellRefresh, cacheCleanup]));
  event.respondWith(networkResponse.then((response) => response).catch(cachedShellOrUnavailable));
});

function readPushPayload(event) {
  if (!event.data) return null;
  try {
    return event.data.json();
  } catch {
    return null;
  }
}

function notificationTag(notificationId) {
  return `t3:${notificationId}`;
}

async function acknowledge(notification, action) {
  const data = notification?.data;
  if (!data?.notificationId || !data?.ackToken) return;
  const ackUrl =
    typeof data.ackUrl === "string" && data.ackUrl.length > 0 ? data.ackUrl : DEFAULT_ACK_URL;
  await fetch(ackUrl, {
    method: "POST",
    mode: "cors",
    credentials: "omit",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      notificationId: data.notificationId,
      ackToken: data.ackToken,
      action,
    }),
    keepalive: true,
  }).catch(() => undefined);
}

async function closeNotification(notificationId) {
  const notifications = await self.registration.getNotifications({
    tag: notificationTag(notificationId),
  });
  for (const notification of notifications) {
    notification.close();
  }
}

self.addEventListener("push", (event) => {
  const payload = readPushPayload(event);
  if (!payload) return;

  if (payload.kind === "dismiss" && typeof payload.notificationId === "string") {
    event.waitUntil(closeNotification(payload.notificationId));
    return;
  }

  const notification = payload.notification;
  if (payload.kind !== "show" || !notification?.notificationId || !notification?.title) {
    return;
  }

  event.waitUntil(
    self.registration.showNotification(notification.title, {
      body: notification.body,
      tag: notificationTag(notification.notificationId),
      data: {
        notificationId: notification.notificationId,
        ackToken: notification.ackToken,
        ackUrl: typeof payload.ackUrl === "string" ? payload.ackUrl : DEFAULT_ACK_URL,
        deepLink: notification.deepLink ?? "/",
      },
      icon: "/pwa-icon-192.png",
      badge: "/pwa-icon-192.png",
      requireInteraction: notification.requireInteraction !== false,
      actions: [
        { action: "open", title: "Open" },
        { action: "dismiss", title: "Dismiss" },
      ],
    }),
  );
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  event.waitUntil(
    (async () => {
      const ackPromise = acknowledge(
        event.notification,
        event.action === "dismiss" ? "dismissed" : "opened",
      );
      if (event.action === "dismiss") {
        await ackPromise;
        return;
      }

      const rawDeepLink = event.notification.data?.deepLink;
      const deepLink =
        typeof rawDeepLink === "string" &&
        rawDeepLink.startsWith("/") &&
        !rawDeepLink.startsWith("//")
          ? rawDeepLink
          : "/";
      const allClients = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
      const sameOriginUrl = new URL(deepLink, self.location.origin).toString();
      for (const client of allClients) {
        if ("focus" in client && client.url.startsWith(self.location.origin)) {
          await client.focus();
          // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Service worker Client.postMessage does not accept a targetOrigin parameter.
          client.postMessage({ type: "t3-notification-open", deepLink });
          await ackPromise;
          return;
        }
      }
      await self.clients.openWindow(sameOriginUrl);
      await ackPromise;
    })(),
  );
});

self.addEventListener("notificationclose", (event) => {
  event.waitUntil(acknowledge(event.notification, "closed"));
});
