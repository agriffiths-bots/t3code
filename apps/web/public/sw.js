const CACHE_VERSION = "t3-code-v2";
const CACHE_PREFIX = "t3-code-";
const APP_SHELL_URLS = ["/", "/manifest.webmanifest", "/pwa-icon-192.png", "/pwa-icon-512.png"];
const DEFAULT_ACK_URL = "/api/notifications/ack";
const PUSH_RECOVERY_DB_NAME = "t3-push-recovery";
const PUSH_RECOVERY_DB_VERSION = 1;
const PUSH_RECOVERY_STORE_NAME = "configuration";
const PUSH_RECOVERY_CONFIG_KEY = "current";
const PUSH_RECOVERY_RETRY_DELAYS_MS = [0, 250, 1_000];
const BUILD_ASSET_PATH_PREFIX = "/assets/";
const BUILD_ASSET_CACHE_PREFIX = `${CACHE_PREFIX}assets-`;
const NON_SPA_ROUTE_PATH_PREFIXES = [
  "/.well-known",
  "/api",
  "/assets",
  "/attachments",
  "/download",
  "/downloads",
];
const STATIC_FILE_PATH_PATTERN = /\/[^/]+\.[^/]+$/;

function openPushRecoveryDatabase() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(PUSH_RECOVERY_DB_NAME, PUSH_RECOVERY_DB_VERSION);
    request.onupgradeneeded = () => {
      if (!request.result.objectStoreNames.contains(PUSH_RECOVERY_STORE_NAME)) {
        request.result.createObjectStore(PUSH_RECOVERY_STORE_NAME);
      }
    };
    request.onsuccess = () => resolve(request.result);
    // oxlint-disable-next-line unicorn/prefer-add-event-listener -- This request is owned by this one-shot promise.
    request.onerror = () =>
      reject(request.error ?? new Error("Failed to open push recovery store."));
  });
}

async function readPushRecoveryConfig() {
  const database = await openPushRecoveryDatabase();
  try {
    return await new Promise((resolve, reject) => {
      const request = database
        .transaction(PUSH_RECOVERY_STORE_NAME, "readonly")
        .objectStore(PUSH_RECOVERY_STORE_NAME)
        .get(PUSH_RECOVERY_CONFIG_KEY);
      request.onsuccess = () => resolve(request.result ?? null);
      // oxlint-disable-next-line unicorn/prefer-add-event-listener -- This request is owned by this one-shot promise.
      request.onerror = () =>
        reject(request.error ?? new Error("Failed to read push recovery configuration."));
    });
  } finally {
    database.close();
  }
}

async function writePushRecoveryConfig(config) {
  const database = await openPushRecoveryDatabase();
  try {
    await new Promise((resolve, reject) => {
      const transaction = database.transaction(PUSH_RECOVERY_STORE_NAME, "readwrite");
      transaction.oncomplete = () => resolve();
      // oxlint-disable-next-line unicorn/prefer-add-event-listener -- This transaction is owned by this one-shot promise.
      transaction.onerror = () =>
        reject(transaction.error ?? new Error("Failed to write push recovery configuration."));
      // oxlint-disable-next-line unicorn/prefer-add-event-listener -- This transaction is owned by this one-shot promise.
      transaction.onabort = () =>
        reject(transaction.error ?? new Error("Push recovery configuration write aborted."));
      transaction.objectStore(PUSH_RECOVERY_STORE_NAME).put(config, PUSH_RECOVERY_CONFIG_KEY);
    });
  } finally {
    database.close();
  }
}

function pushRecoveryConfigsMatch(left, right) {
  return (
    left.oldEndpoint === right.oldEndpoint &&
    left.recoveryToken === right.recoveryToken &&
    left.recoveryUrl === right.recoveryUrl &&
    left.vapidPublicKey === right.vapidPublicKey
  );
}

async function replacePushRecoveryConfig(expected, replacement) {
  const database = await openPushRecoveryDatabase();
  try {
    return await new Promise((resolve, reject) => {
      let replaced = false;
      const transaction = database.transaction(PUSH_RECOVERY_STORE_NAME, "readwrite");
      transaction.oncomplete = () => resolve(replaced);
      // oxlint-disable-next-line unicorn/prefer-add-event-listener -- This transaction is owned by this one-shot promise.
      transaction.onerror = () =>
        reject(transaction.error ?? new Error("Failed to replace push recovery configuration."));
      // oxlint-disable-next-line unicorn/prefer-add-event-listener -- This transaction is owned by this one-shot promise.
      transaction.onabort = () =>
        reject(transaction.error ?? new Error("Push recovery configuration replace aborted."));
      const store = transaction.objectStore(PUSH_RECOVERY_STORE_NAME);
      const request = store.get(PUSH_RECOVERY_CONFIG_KEY);
      request.onsuccess = () => {
        const current = parsePushRecoveryConfig(request.result);
        if (!current || !pushRecoveryConfigsMatch(current, expected)) return;
        replaced = true;
        store.put(replacement, PUSH_RECOVERY_CONFIG_KEY);
      };
    });
  } finally {
    database.close();
  }
}

function parsePushRecoveryConfig(value) {
  if (
    !value ||
    typeof value.oldEndpoint !== "string" ||
    typeof value.recoveryToken !== "string" ||
    typeof value.recoveryUrl !== "string" ||
    typeof value.vapidPublicKey !== "string"
  ) {
    return null;
  }
  try {
    const recoveryUrl = new URL(value.recoveryUrl);
    if (recoveryUrl.protocol !== "http:" && recoveryUrl.protocol !== "https:") {
      return null;
    }
  } catch {
    return null;
  }
  return {
    oldEndpoint: value.oldEndpoint,
    recoveryToken: value.recoveryToken,
    recoveryUrl: value.recoveryUrl,
    vapidPublicKey: value.vapidPublicKey,
  };
}

function urlBase64ToUint8Array(value) {
  const padding = "=".repeat((4 - (value.length % 4)) % 4);
  const raw = atob(`${value}${padding}`.replace(/-/g, "+").replace(/_/g, "/"));
  return Uint8Array.from(raw, (character) => character.charCodeAt(0));
}

function waitForPushRecoveryRetry(delayMillis) {
  return new Promise((resolve) => setTimeout(resolve, delayMillis));
}

function toServerPushSubscription(subscription) {
  const json = subscription?.toJSON();
  if (!json?.endpoint || !json.keys?.p256dh || !json.keys.auth) {
    return null;
  }
  return {
    endpoint: json.endpoint,
    expirationTime: json.expirationTime ?? null,
    keys: {
      p256dh: json.keys.p256dh,
      auth: json.keys.auth,
    },
  };
}

function isBuildAssetUrl(url) {
  return url.origin === self.location.origin && url.pathname.startsWith(BUILD_ASSET_PATH_PREFIX);
}

function requestAcceptsHtml(request) {
  return request.headers.get("accept")?.toLowerCase().includes("text/html") ?? false;
}

function isExcludedSpaRoutePath(pathname) {
  return NON_SPA_ROUTE_PATH_PREFIXES.some(
    (prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`),
  );
}

function isSpaRoutePath(pathname) {
  return !isExcludedSpaRoutePath(pathname) && !STATIC_FILE_PATH_PATTERN.test(pathname);
}

function isAppShellNavigationRequest(request, url) {
  return (
    request.mode === "navigate" &&
    url.origin === self.location.origin &&
    requestAcceptsHtml(request) &&
    isSpaRoutePath(url.pathname)
  );
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

function hashBuildAssetUrls(assetUrls) {
  let hash = 2166136261;
  for (const assetUrl of [...assetUrls].sort()) {
    for (let index = 0; index < assetUrl.length; index += 1) {
      hash ^= assetUrl.charCodeAt(index);
      hash = Math.imul(hash, 16777619);
    }
    hash ^= 10;
    hash = Math.imul(hash, 16777619);
  }

  return (hash >>> 0).toString(36);
}

function buildAssetCacheNameForAssetUrls(assetUrls) {
  return `${BUILD_ASSET_CACHE_PREFIX}${hashBuildAssetUrls(assetUrls)}`;
}

async function cacheShellAndBuildAssets(cache, shellResponse) {
  const shellForCache = shellResponse.clone();
  const assetUrls = extractBuildAssetUrls(await shellResponse.text());
  const assetCache = await caches.open(buildAssetCacheNameForAssetUrls(assetUrls));
  await Promise.all(assetUrls.map((url) => cacheRequiredBuildAsset(assetCache, url)));
  await cache.put("/", shellForCache);
}

async function cacheBuildAssetsFromShell(cache) {
  const shell = await cache.match("/");
  if (!shell) return;

  const assetUrls = extractBuildAssetUrls(await shell.clone().text());
  const assetCache = await caches.open(buildAssetCacheNameForAssetUrls(assetUrls));
  await Promise.all(assetUrls.map((url) => cacheRequiredBuildAsset(assetCache, url)));
}

async function extractCachedShellBuildAssetUrls(cache) {
  const shell = await cache.match("/");
  if (!shell) return [];
  return extractBuildAssetUrls(await shell.clone().text());
}

async function currentBuildAssetCacheName() {
  const cache = await caches.open(CACHE_VERSION);
  const assetUrls = await extractCachedShellBuildAssetUrls(cache);
  return buildAssetCacheNameForAssetUrls(assetUrls);
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
  const buildAssetCacheName = await currentBuildAssetCacheName();
  const clients = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
  // Open tabs can still request lazy chunks from the build that loaded them.
  const keepPriorAppCaches = clients.length > 0;
  const supersededCacheDeletes = cacheKeys
    .filter((key) => key !== CACHE_VERSION && key !== buildAssetCacheName)
    .filter((key) => !keepPriorAppCaches || !key.startsWith(CACHE_PREFIX))
    .map((key) => caches.delete(key));
  await Promise.all(supersededCacheDeletes);
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
        const retainedAssetUrls = await extractCachedShellBuildAssetUrls(cache);
        const assetCache = await caches.open(buildAssetCacheNameForAssetUrls(retainedAssetUrls));
        await assetCache.put(request, assetResponse);
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

  if (!isAppShellNavigationRequest(request, requestUrl)) return;

  const networkResponse = fetch(request);
  const shellRefresh = networkResponse
    .then(async (response) => {
      if (!isUsableShellResponse(response)) return;

      const shellResponse = response.clone();
      const cache = await caches.open(CACHE_VERSION);
      await cacheShellAndBuildAssets(cache, shellResponse);
    })
    .catch(() => undefined);
  const cacheCleanup = shellRefresh.then(() => deleteSupersededCaches()).catch(() => undefined);

  event.waitUntil(cacheCleanup);
  event.respondWith(networkResponse.then((response) => response).catch(cachedShellOrUnavailable));
});

self.addEventListener("message", (event) => {
  if (event.data?.type !== "t3-push-recovery-config") return;
  const config = parsePushRecoveryConfig(event.data);
  if (!config) return;
  event.waitUntil(writePushRecoveryConfig(config));
});

async function recoverPushSubscription(event) {
  const config = parsePushRecoveryConfig(await readPushRecoveryConfig());
  if (!config) return;

  const subscription =
    event.newSubscription ??
    (await self.registration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8Array(config.vapidPublicKey),
    }));
  const newSubscription = toServerPushSubscription(subscription);
  if (!newSubscription) return;
  const request = {
    method: "POST",
    mode: "cors",
    credentials: "omit",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      oldEndpoint: config.oldEndpoint,
      recoveryToken: config.recoveryToken,
      newSubscription,
    }),
  };

  for (const [attempt, delayMillis] of PUSH_RECOVERY_RETRY_DELAYS_MS.entries()) {
    if (delayMillis > 0) await waitForPushRecoveryRetry(delayMillis);
    try {
      const response = await fetch(config.recoveryUrl, request);
      if (!response.ok) {
        const retryable =
          response.status === 408 || response.status === 429 || response.status >= 500;
        if (!retryable) return;
        throw new Error(`Push recovery failed with status ${response.status}.`);
      }

      const result = await response.json();
      if (typeof result?.recoveryToken !== "string" || result.recoveryToken.length === 0) {
        throw new Error("Push recovery returned an invalid token response.");
      }
      await replacePushRecoveryConfig(config, {
        ...config,
        oldEndpoint: newSubscription.endpoint,
        recoveryToken: result.recoveryToken,
      });
      return;
    } catch (cause) {
      if (attempt === PUSH_RECOVERY_RETRY_DELAYS_MS.length - 1) throw cause;
    }
  }
}

self.addEventListener("pushsubscriptionchange", (event) => {
  event.waitUntil(recoverPushSubscription(event).catch(() => undefined));
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
